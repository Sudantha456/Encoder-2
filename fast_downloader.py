"""
fast_downloader.py — Telethon MULTI-CONNECTION parallel downloader
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• Connections කිහිපයක් (DL_CONNECTIONS) × හැම එකකම parallel requests (DL_WORKERS_PER_CONN)
• 1MB chunks, file descriptor එක එක පාරක් open (pwrite)
• FloodWait → හැම worker එකම pause; fail වුණොත් native download එකට fallback
"""
import contextlib
import os
import time
import logging
import asyncio
import random
import re
from dataclasses import dataclass
from typing import Any, Optional, Callable, Dict, List

from telethon import TelegramClient
from telethon.tl.functions.upload import GetFileRequest
from telethon.tl.types import InputDocumentFileLocation
from telethon.errors import FloodWaitError

import config as _cfg
from config import PROGRESS_UPDATE_INTERVAL
from fast_conn import SenderPool

logger = logging.getLogger("FastDL")

CHUNK_SIZE = 1024 * 1024                                   # 1MB (Telegram max)
DL_CONNECTIONS = getattr(_cfg, "DL_CONNECTIONS", 6)
DL_WORKERS_PER_CONN = getattr(_cfg, "DL_WORKERS_PER_CONN", 4)
CHUNK_TIMEOUT = 20.0                                       # chunk එකක් මේ තත්පර ගණනට වැඩි නම් retry (පරණ: 60)
HEDGE_AFTER = 3.0                                          # end-game: chunk එකක් තත්පර 3කට වඩා inflight නම් තව worker කෙනෙක් ඒක ඉල්ලනවා
HEDGE_MAX = 3                                              # එකම chunk එකට උපරිම එකවර requests
BACKOFF_SCALE = 1.0                                        # retry backoff (tests වලදී අඩු කරන්න)
SMALL_FILE = 4 * 1024 * 1024                               # මීට කුඩා නම් native

# ── 🚑 Stall protection: bytes එකක්වත් එන්නේ නැතිව හිර වුණොත් හැදෙන්න ඕනේ (100% වගේ පෙන්වා ඉන්න බෑ) ──
STALL_WARN = float(os.getenv("DL_STALL_WARN", "45"))       # මෙච්චර වෙලාවක් progress නැත්නම් flood clears + retry
STALL_FAIL = float(os.getenv("DL_STALL_FAIL", "150"))      # මෙච්චර වෙලාවක් නම් download එක abort (fallback)


@dataclass
class Lane:
    """එක userbot එකක් = එක 'lane' එකක්. Lanes ඔක්කොම එකම file එකේ chunks බෙදාගෙන එකවර download කරනවා."""
    name: str
    client: Any
    location: Any
    dc_id: Optional[int]
    connections: int
    pool: Any = None
    flood_until: float = 0.0
    dead: bool = False
    got: int = 0                                  # මේ lane එකෙන් download කළ bytes
    bad: int = 0                                  # දිගටම ආපු errors (සාර්ථක chunk එකකින් reset)
    on_flood: Optional[Callable[[int], None]] = None   # (seconds) → userbot pool එකට දැනුම් දෙන්න
    # ── weighted scheduler (වේගවත් userbot එකට වැඩි chunks) ──
    rate: float = 0.0                             # EWMA bytes/s (chunk success වලින්)
    chunks: int = 0                               # සාර්ථක chunks ගණන
    active: int = 0                               # දැන් in-flight requests
    quota: int = 4                                # මේ lane එකට එකවර අවසර ඇති requests (rebalancer වෙනස් කරනවා)


class FastDownloader:

    def __init__(self):
        self._cancel_flags: Dict[str, bool] = {}

    def cancel(self, download_id: str):
        self._cancel_flags[download_id] = True

    def is_cancelled(self, download_id: str) -> bool:
        return self._cancel_flags.get(download_id, False)

    async def cleanup(self, download_id: str):
        self._cancel_flags.pop(download_id, None)

    def force_delete_file(self, file_path: str) -> bool:
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
                return True
            except Exception:
                return False
        return False

    def _get_file_location(self, message):
        if getattr(message, 'document', None):
            return InputDocumentFileLocation(
                id=message.document.id,
                access_hash=message.document.access_hash,
                file_reference=message.document.file_reference,
                thumb_size=""
            ), message.document.size
        return None, 0

    async def download(
        self,
        client: TelegramClient,
        message,
        save_path: str,
        download_id: str,
        progress_callback: Optional[Callable] = None,
        max_retries: int = 10,
    ) -> str:
        self._cancel_flags[download_id] = False

        doc = getattr(message, 'document', None)
        doc_size = getattr(doc, 'size', 0) if doc else 0
        if getattr(message, 'photo', None) or doc_size < SMALL_FILE:
            return await self._download_native(client, message, save_path, download_id, progress_callback, max_retries)

        location, total_size = self._get_file_location(message)
        if not location or total_size == 0:
            return await self._download_native(client, message, save_path, download_id, progress_callback, max_retries)

        try:
            return await self._download_parallel(client, doc, location, total_size, save_path,
                                                 download_id, progress_callback, max_retries)
        except asyncio.CancelledError:
            self.force_delete_file(save_path)
            raise
        except Exception as e:
            if self.is_cancelled(download_id):
                self.force_delete_file(save_path)
                raise asyncio.CancelledError("Cancelled")
            logger.warning(f"Parallel download failed ({e!r}) — native fallback")
            self.force_delete_file(save_path)
            return await self._download_native(client, message, save_path, download_id, progress_callback, max_retries)

    async def _download_parallel(self, client, doc, location, total_size, save_path,
                                download_id, progress_callback, max_retries) -> str:
        pool = SenderPool(client, getattr(doc, 'dc_id', None), DL_CONNECTIONS)
        await asyncio.wait_for(pool.start(), timeout=60.0)

        flags = os.O_RDWR | os.O_CREAT | os.O_TRUNC | getattr(os, "O_BINARY", 0)
        fd = os.open(save_path, flags)
        os.ftruncate(fd, total_size)
        has_pwrite = hasattr(os, "pwrite")
        write_lock = asyncio.Lock()

        downloaded = 0
        last_update = [0.0]
        last_prog = [time.monotonic()]      # stall watchdog
        flood_until = [0.0]

        def _prog(n: int):
            nonlocal downloaded
            downloaded += n
            now = time.monotonic()
            last_prog[0] = now
            if now - last_update[0] >= PROGRESS_UPDATE_INTERVAL or downloaded >= total_size:
                last_update[0] = now
                if progress_callback:
                    asyncio.create_task(progress_callback(downloaded, total_size))

        async def write_at(off: int, data: bytes):
            if has_pwrite:
                os.pwrite(fd, data, off)
            else:
                async with write_lock:
                    os.lseek(fd, off, os.SEEK_SET)
                    os.write(fd, data)

        async def fetch(off: int, worker_idx: int):
            for attempt in range(max_retries):
                if self.is_cancelled(download_id):
                    raise asyncio.CancelledError("Cancelled")
                wait = flood_until[0] - time.monotonic()
                if wait > 0:
                    await asyncio.sleep(wait + random.uniform(0.0, 0.5))
                try:
                    req = GetFileRequest(location=location, offset=off, limit=CHUNK_SIZE)
                    # retry වෙනකොට වෙන connection එකකට මාරු වෙනවා
                    res = await asyncio.wait_for(pool.send(req, worker_idx + attempt), timeout=60.0)
                    data = getattr(res, "bytes", None)
                    if data is None:
                        raise RuntimeError(f"Unexpected response: {type(res).__name__}")
                    want = min(CHUNK_SIZE, total_size - off)
                    if not data or (len(data) < want and off + len(data) < total_size):
                        raise RuntimeError(f"bad chunk ({len(data) or 0}/{want})")
                    await write_at(off, data)
                    _prog(len(data))
                    return
                except asyncio.CancelledError:
                    raise
                except FloodWaitError as e:
                    flood_until[0] = max(flood_until[0], time.monotonic() + e.seconds + 1)
                    logger.warning(f"FloodWait {e.seconds}s — all workers paused")
                except asyncio.TimeoutError:
                    if attempt == max_retries - 1:
                        raise RuntimeError(f"Chunk {off} timed out {max_retries}×")
                    await asyncio.sleep(min(2 ** attempt, 8))
                except Exception as e:
                    err = str(e).lower()
                    if "wait of" in err or "premium" in err:
                        m = re.search(r'wait of (\d+)', err)
                        flood_until[0] = max(flood_until[0], time.monotonic() + (int(m.group(1)) if m else 12) + 1)
                        continue
                    if attempt == max_retries - 1:
                        raise
                    await asyncio.sleep(min(2 ** attempt, 8))

        queue: asyncio.Queue = asyncio.Queue()
        for off in range(0, total_size, CHUNK_SIZE):
            queue.put_nowait(off)

        async def worker(idx: int):
            while True:
                if self.is_cancelled(download_id):
                    raise asyncio.CancelledError("Cancelled")
                try:
                    off = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                await fetch(off, idx)

        n_workers = min(len(pool.senders) * DL_WORKERS_PER_CONN, queue.qsize())
        tasks = [asyncio.create_task(worker(i)) for i in range(n_workers)]

        async def guard():
            """bytes එකක්වත් එන්නේ නැතිව හිර වුණොත් (100% වගේ පෙන්වා ඉන්නේ නැතුව) abort කරනවා."""
            while True:
                await asyncio.sleep(5.0)
                idle = time.monotonic() - last_prog[0]
                if idle > STALL_WARN:
                    flood_until[0] = 0.0                     # FloodWait park එක ඉවත්
                if idle > STALL_FAIL:
                    raise RuntimeError(f"download stalled {idle:.0f}s "
                                       f"({downloaded / 1048576:.0f}/{total_size / 1048576:.0f}MB)")

        main = asyncio.create_task(asyncio.gather(*tasks))
        gtask = asyncio.create_task(guard())
        try:
            await asyncio.wait({main, gtask}, return_when=asyncio.FIRST_COMPLETED)
            if gtask.done() and not gtask.cancelled() and gtask.exception():
                raise gtask.exception()
            await main
            if downloaded < total_size:
                raise RuntimeError(f"Incomplete download {downloaded}/{total_size}")
            return save_path
        except BaseException:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        finally:
            gtask.cancel()
            with contextlib.suppress(BaseException):
                await gtask
            os.close(fd)
            await pool.close()


    # ═════════ Multi-userbot download: chunks lanes අතර බෙදනවා ═════════
    async def download_lanes(self, lanes: List[Lane], total_size: int, save_path: str, download_id: str,
                             progress_callback: Optional[Callable] = None, max_retries: int = 8,
                             max_flood_wait: float = 30.0, workers_per_conn: Optional[int] = None) -> str:
        """
        Userbots කිහිපයක් එකවර (lanes) — **smart, weighted** chunk scheduler:

        • Shared chunk queue. එක chunk එකක් එකවර **එක** තැනකින් විතරයි download වෙන්නේ —
          in-flight / finished / queue යන තුනෙන් එකක තියෙන chunk එකක් ආයෙ queue එකට දාන්නේ නෑ
          (පරණ hedge logic එකේ duplicate downloads වුණා → bandwidth නාස්ති).
        • ඒ ඒ lane එකේ මැනගත්ත throughput (EWMA) අනුව quota එක 2s වලට වරක් අලුත් වෙනවා
          → වේගවත් userbot එකට වැඩි chunks, මන්දගාමී එකට අඩුව (උපරිම speed).
        • Hedge (end-game duplicate) දැන් adaptive: chunk එකකට යන සාමාන්‍ය වෙලාව × 2.5 ට වඩා
          පරණ නම් විතරයි, ඒකත් queue එක හිස් වෙලා නම්. Timeout වුණොත් chunk එක queue එකට ආපහු
          යනවා → වෙන lane එකක් ඉක්මනින් ගන්නවා.
        • Lane එකක FloodWait: chunks වෙන lanes ගන්නවා; දිග FloodWait නම් ඒ lane එක dead.
        """
        wpc = workers_per_conn or DL_WORKERS_PER_CONN
        self._cancel_flags[download_id] = False

        # 1) pools (පොඩි stagger එකක් — එකවර connect කරද්දී flood වෙන එක අඩු කරන්න)
        async def start_lane(i: int, ln: Lane):
            await asyncio.sleep(i * 0.25)
            ln.pool = SenderPool(ln.client, ln.dc_id, ln.connections, use_main=False)
            await asyncio.wait_for(ln.pool.start(), timeout=60.0)

        res = await asyncio.gather(*[start_lane(i, ln) for i, ln in enumerate(lanes)], return_exceptions=True)
        for ln, r in zip(lanes, res):
            if isinstance(r, BaseException):
                ln.dead = True
                logger.warning(f"[{ln.name}] lane start failed: {r!r}")
                if isinstance(r, FloodWaitError) and ln.on_flood:
                    ln.on_flood(r.seconds)
        live = [ln for ln in lanes if not ln.dead]
        if not live:
            for ln in lanes:
                if ln.pool:
                    await ln.pool.close()
            raise RuntimeError("Userbot lane එකක්වත් start වුණේ නෑ")

        flags = os.O_RDWR | os.O_CREAT | os.O_TRUNC | getattr(os, "O_BINARY", 0)
        fd = os.open(save_path, flags)
        os.ftruncate(fd, total_size)
        has_pwrite = hasattr(os, "pwrite")
        write_lock = asyncio.Lock()

        n_chunks = (total_size + CHUNK_SIZE - 1) // CHUNK_SIZE
        queue: asyncio.Queue = asyncio.Queue()
        queued: set = set()                        # queue එකේ ඉන්න offsets
        inflight: Dict[int, list] = {}             # off → [started_at, copies]
        finished: set = set()
        fails: Dict[int, int] = {}
        remaining = [n_chunks]
        done_ev = asyncio.Event()
        fatal: asyncio.Future = asyncio.get_running_loop().create_future()
        fatal.add_done_callback(lambda f: f.cancelled() or f.exception())   # 'never retrieved' warning වළක්වන්න
        downloaded = [0]
        last_update = [0.0]
        last_prog = [time.monotonic()]             # අන්තිම bytes එක ආපු වෙලාව (stall watchdog)
        warned = [False]
        chunk_time = [1.0]                         # EWMA: එක chunk එකකට යන තත්පර

        def push(off: int):
            """Queue එකට දාන්නේ තවම කිසිම තැනක නැති chunk එකක් විතරයි (duplicate නෑ)."""
            if off in finished or off in queued or off in inflight:
                return
            queued.add(off)
            queue.put_nowait(off)

        for off in range(0, total_size, CHUNK_SIZE):
            push(off)

        def _fatal(exc: BaseException):
            if not fatal.done():
                fatal.set_exception(exc)

        def _prog(n: int):
            downloaded[0] += n
            now = time.monotonic()
            last_prog[0] = now
            warned[0] = False
            if now - last_update[0] >= PROGRESS_UPDATE_INTERVAL or downloaded[0] >= total_size:
                last_update[0] = now
                if progress_callback:
                    asyncio.create_task(progress_callback(downloaded[0], total_size))

        async def write_at(off: int, data: bytes):
            if has_pwrite:
                os.pwrite(fd, data, off)
            else:
                async with write_lock:
                    os.lseek(fd, off, os.SEEK_SET)
                    os.write(fd, data)

        def lane_died(ln: Lane, why: str):
            if not ln.dead:
                ln.dead = True
                logger.warning(f"[{ln.name}] lane dropped: {why}")
            if all(l.dead for l in lanes):
                _fatal(RuntimeError("සියලුම userbot lanes fail වුණා: " + why))

        tasks: list = []
        per: Dict[str, int] = {}

        def pick_hedge() -> Optional[int]:
            """Queue හිස් + chunk එකක් දිගටම inflight → වෙන connection එකකින් ආයෙ ඉල්ලනවා."""
            now = time.monotonic()
            limit = max(5.0, chunk_time[0] * 2.5)
            best = None
            for off, (t0, copies) in inflight.items():
                if off in finished or copies >= HEDGE_MAX:
                    continue
                if now - t0 < limit:
                    continue
                if best is None or t0 < inflight[best][0]:
                    best = off
            return best

        async def worker(ln: Lane, widx: int):
            while not done_ev.is_set() and not ln.dead:
                if self.is_cancelled(download_id):
                    raise asyncio.CancelledError("Cancelled")
                wait = ln.flood_until - time.monotonic()
                if wait > 0:                                   # මේ lane එක FloodWait pause එකක — chunk අල්ලන්නේ නෑ
                    await asyncio.sleep(min(wait, 1.0))
                    continue
                if ln.active >= ln.quota:                      # weighted gate: slow lane එකට වැඩි requests දෙන්නේ නෑ
                    await asyncio.sleep(0.02)
                    continue
                hedge = False
                try:
                    off = queue.get_nowait()
                    queued.discard(off)
                except asyncio.QueueEmpty:
                    off = pick_hedge()
                    if off is None:
                        await asyncio.sleep(0.05)
                        continue
                    hedge = True
                if off in finished:
                    continue
                rec = inflight.setdefault(off, [time.monotonic(), 0])
                rec[1] += 1
                ln.active += 1
                t_send = time.monotonic()
                want = min(CHUNK_SIZE, total_size - off)
                requeue, slow = False, False
                try:
                    req = GetFileRequest(location=ln.location, offset=off, limit=want)
                    r = await asyncio.wait_for(ln.pool.send(req, widx + (1 if hedge else 0)),
                                               timeout=CHUNK_TIMEOUT)
                    data = getattr(r, "bytes", None)
                    if data is None:
                        raise RuntimeError(f"Unexpected response: {type(r).__name__}")
                    if not data:
                        raise RuntimeError("empty chunk")
                    if len(data) < want and off + len(data) < total_size:
                        # අඩක් ආපු chunk එකක් — නැවත ඉල්ලන්න (missing bytes = corrupt file)
                        raise RuntimeError(f"short read {len(data)}/{want}")
                    ln.bad = 0
                    if off not in finished:
                        finished.add(off)                      # පළමුව ඉවර වූ copy එක විතරයි ගණන් වෙන්නේ
                        await write_at(off, data)
                        ln.got += len(data)
                        ln.chunks += 1
                        # per-lane throughput (EWMA) → rebalancer මේකෙන් quota හදනවා
                        dt = max(time.monotonic() - t_send, 0.01)
                        inst = len(data) / dt
                        ln.rate = inst if ln.chunks <= 2 else (0.7 * ln.rate + 0.3 * inst)
                        chunk_time[0] = dt if ln.chunks <= 2 else (0.8 * chunk_time[0] + 0.2 * dt)
                        _prog(len(data))
                        remaining[0] -= 1
                        if remaining[0] <= 0:
                            done_ev.set()
                except asyncio.CancelledError:
                    if not hedge and off not in finished:
                        requeue = True
                    raise
                except FloodWaitError as e:
                    if not hedge and off not in finished:
                        requeue = True
                    if ln.on_flood:
                        ln.on_flood(e.seconds)
                    if e.seconds > max_flood_wait:
                        # දිග FloodWait = මේ lane එකෙන් වැඩක් නෑ. අනිත් ඒවත් dead නම් fatal → fallback.
                        lane_died(ln, f"FloodWait {e.seconds}s (> {max_flood_wait:.0f}s)")
                        return
                    ln.flood_until = max(ln.flood_until, time.monotonic() + e.seconds + random.uniform(0.5, 1.5))
                    logger.warning(f"[{ln.name}] FloodWait {e.seconds}s — lane paused, others continue")
                except Exception as e:
                    if hedge:
                        continue                               # hedge copy එක fail වුණාට කමක් නෑ (මුල් එක තාම තියෙනවා)
                    if off not in finished:
                        requeue = True
                    n = fails[off] = fails.get(off, 0) + 1
                    msg = str(e).lower()
                    if "wait of" in msg:
                        m = re.search(r'wait of (\d+)', msg)
                        ln.flood_until = max(ln.flood_until, time.monotonic() + (int(m.group(1)) if m else 12) + 1)
                        continue
                    slow = True
                    if n >= max_retries:
                        _fatal(RuntimeError(f"Chunk {off} {n}× fail ({e!r})"))
                        return
                    ln.bad += 1
                    if ln.bad >= 5 and any(l is not ln and not l.dead for l in lanes):
                        lane_died(ln, f"දිගටම errors ({ln.bad}): {e!r}")      # වෙන lanes ඉතුරුයි → මේක අයින්
                        return
                    await asyncio.sleep(min(2 ** (n - 1), 8) * BACKOFF_SCALE)
                finally:
                    ln.active -= 1
                    if slow:
                        ln.rate = ln.rate * 0.7
                    rec[1] -= 1
                    if inflight.get(off) is rec and rec[1] <= 0:   # (stall-reset වුණා නම් අලුත් rec එක ඉතුරු කරනවා)
                        inflight.pop(off, None)
                    if requeue:
                        push(off)

        # workers lanes අතර interleave (A,B,C,A,B,C…) — නැත්නම් මුල් lane එකේ workers ම chunks ඔක්කොම අල්ලගන්නවා
        for ln in live:
            per[ln.name] = max(1, len(ln.pool.senders)) * wpc
        order = [(ln, i) for i in range(max(per.values())) for ln in live if i < per[ln.name]]
        tasks = [asyncio.create_task(worker(ln, i)) for ln, i in order]
        for ln in live:
            ln.quota = max(2, len(tasks) // max(len(live), 1))

        async def rebalance():
            """2s වලට වරක්: මනින ලද speed එකට අනුව lane quota (වේගවත් එකට වැඩි requests)."""
            while not done_ev.is_set():
                await asyncio.sleep(2.0)
                cur = [l for l in lanes if not l.dead]
                if not cur:
                    return
                known = [l.rate for l in cur if l.chunks >= 6 and l.rate > 0]
                base = (sum(known) / len(known)) if known else 0.0
                vals = {id(l): (l.rate if (l.chunks >= 6 and l.rate > 0) else base) for l in cur}
                tot = sum(vals.values())
                n = len(tasks)
                for l in cur:
                    share = (vals[id(l)] / tot) if tot > 0 else (1.0 / len(cur))
                    l.quota = max(2, min(per.get(l.name, n), max(2, int(round(n * share)))))

        done_task = asyncio.create_task(done_ev.wait())
        bal_task = asyncio.create_task(rebalance())
        try:
            while True:
                done, _ = await asyncio.wait({done_task, fatal}, return_when=asyncio.FIRST_COMPLETED, timeout=1.0)
                if self.is_cancelled(download_id):
                    raise asyncio.CancelledError("Cancelled")
                if fatal.done():
                    fatal.result()                              # raises
                for t in tasks:
                    if t.done() and not t.cancelled() and t.exception():
                        raise t.exception()
                if done_ev.is_set():
                    break

                # 🚑 Stall watchdog — bytes එකක්වත් එන්නේ නැතිව හිර වුණොත්:
                idle = time.monotonic() - last_prog[0]
                if idle > STALL_FAIL:
                    raise RuntimeError(
                        f"download stalled {idle:.0f}s ({downloaded[0] / 1048576:.0f}/"
                        f"{total_size / 1048576:.0f}MB, lanes dead: "
                        f"{', '.join(l.name for l in lanes if l.dead) or '-'})")
                if idle > STALL_WARN and not warned[0]:
                    warned[0] = True
                    logger.warning(f"⏸ no progress for {idle:.0f}s — flood waits clear කරලා stuck chunks retry")
                    for l in lanes:
                        l.flood_until = 0.0
                        l.bad = 0
                    for off in [o for o in list(inflight) if o not in finished]:
                        inflight.pop(off, None)                 # අලුත් copy එකකට ඉඩ දෙන්න
                        push(off)
            if downloaded[0] < total_size:
                raise RuntimeError(f"Incomplete download {downloaded[0]}/{total_size}")
            logger.info("lane summary: " + ", ".join(
                f"{l.name}={l.got / 1048576:.0f}MB@{l.rate / 1048576:.1f}MB/s(q{l.quota})" for l in lanes))
            return save_path
        except BaseException:
            self.force_delete_file(save_path)
            raise
        finally:
            done_task.cancel()
            bal_task.cancel()
            for t in tasks:
                t.cancel()
            await asyncio.gather(done_task, bal_task, *tasks, return_exceptions=True)
            os.close(fd)
            await asyncio.gather(*[ln.pool.close() for ln in lanes if ln.pool], return_exceptions=True)

    async def _download_native(self, client, message, save_path, download_id, progress_callback, max_retries=10):
        last_update = [0.0]

        def _prog(current: int, total: int):
            if self.is_cancelled(download_id):
                raise asyncio.CancelledError("Cancelled")
            now = time.monotonic()
            if now - last_update[0] < PROGRESS_UPDATE_INTERVAL and current < total:
                return
            last_update[0] = now
            if progress_callback:
                asyncio.create_task(progress_callback(current, total))

        for attempt in range(max_retries):
            if self.is_cancelled(download_id):
                raise asyncio.CancelledError("Cancelled")
            try:
                path = await client.download_media(message, file=save_path, progress_callback=_prog)
                if not path:
                    raise RuntimeError("Native download_media returned no path")
                return path
            except asyncio.CancelledError:
                self.force_delete_file(save_path)
                raise
            except Exception as e:
                err_str = str(e).lower()
                if isinstance(e, FloodWaitError) or "premium" in err_str or "wait of" in err_str:
                    wait_secs = 15
                    if isinstance(e, FloodWaitError):
                        wait_secs = e.seconds
                    else:
                        m = re.search(r'wait of (\d+)', err_str)
                        if m:
                            wait_secs = int(m.group(1))
                    wait_time = wait_secs + random.uniform(1.0, 3.0)
                    logger.warning(f"Native Rate Limit. Waiting {wait_time:.1f}s")
                    await asyncio.sleep(wait_time)
                    continue
                if attempt >= max_retries - 1:
                    raise
                await asyncio.sleep(2 ** attempt)

        raise RuntimeError(f"Native Download failed after {max_retries} attempts")


_downloader: Optional[FastDownloader] = None


def get_downloader() -> FastDownloader:
    global _downloader
    if _downloader is None:
        _downloader = FastDownloader()
    return _downloader


async def init_download_engine() -> FastDownloader:
    dl = get_downloader()
    logger.info(f"FastDownloader (multi-connection ×{DL_CONNECTIONS}) initialized!")
    return dl


async def close_download_engine():
    global _downloader
    _downloader = None
