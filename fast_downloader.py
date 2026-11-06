"""
fast_downloader.py — Telethon MULTI-CONNECTION parallel downloader
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• Connections කිහිපයක් (DL_CONNECTIONS) × හැම එකකම parallel requests (DL_WORKERS_PER_CONN)
• 1MB chunks, file descriptor එක එක පාරක් open (pwrite)
• FloodWait → හැම worker එකම pause; fail වුණොත් native download එකට fallback
"""
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
        flood_until = [0.0]

        def _prog(n: int):
            nonlocal downloaded
            downloaded += n
            now = time.monotonic()
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
        try:
            await asyncio.gather(*tasks)
            if downloaded < total_size:
                raise RuntimeError(f"Incomplete download {downloaded}/{total_size}")
            return save_path
        except BaseException:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        finally:
            os.close(fd)
            await pool.close()


    # ═════════ Multi-userbot download: chunks lanes අතර බෙදනවා ═════════
    async def download_lanes(self, lanes: List[Lane], total_size: int, save_path: str, download_id: str,
                             progress_callback: Optional[Callable] = None, max_retries: int = 8,
                             max_flood_wait: float = 30.0, workers_per_conn: Optional[int] = None,
                             offset: int = 0, length: Optional[int] = None) -> str:
        """
        • Shared chunk queue — හැම lane එකක්ම queue එකෙන් chunk අරන් ඒ ඒ userbot ගේ connections හරහා ගන්නවා.
        • Lane එකක FloodWait ආවොත්: chunk එක queue එකට ආපහු දානවා (වෙන lanes ගන්නවා), ඒ lane එක විතරක් නතර වෙලා ඉන්නවා.
          FloodWait > max_flood_wait නම් ඒ lane එක මේ job එකට dead (userbot එක cooldown).
        • Lane එකක් fail වුණාට අනිත් ඒවා ඉවර කරනවා. ඔක්කොම dead වුණොත් විතරයි error.
        """
        wpc = workers_per_conn or DL_WORKERS_PER_CONN
        self._cancel_flags[download_id] = False
        loop = asyncio.get_running_loop()
        expected = int(length) if length is not None else int(total_size)
        base = int(offset)

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
        os.ftruncate(fd, expected)
        has_pwrite = hasattr(os, "pwrite")
        write_lock = asyncio.Lock()

        queue: asyncio.Queue = asyncio.Queue()
        for off in range(base, base + expected, CHUNK_SIZE):
            queue.put_nowait(off)
        remaining = [queue.qsize()]
        fails: Dict[int, int] = {}
        done_ev = asyncio.Event()
        fatal: asyncio.Future = loop.create_future()
        fatal.add_done_callback(lambda f: f.cancelled() or f.exception())   # 'never retrieved' warning වළක්වන්න
        downloaded = [0]
        last_update = [0.0]

        def _fatal(exc: BaseException):
            if not fatal.done():
                fatal.set_exception(exc)

        def _prog(n: int):
            downloaded[0] += n
            now = time.monotonic()
            if now - last_update[0] >= PROGRESS_UPDATE_INTERVAL or downloaded[0] >= expected:
                last_update[0] = now
                if progress_callback:
                    asyncio.create_task(progress_callback(downloaded[0], expected))

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

        finished: set = set()
        inflight: Dict[int, list] = {}            # off → [පටන් ගත් වෙලාව, එකවර requests ගණන]

        def pick_hedge() -> Optional[int]:
            """End-game: queue හිස්, ඒත් chunks ටිකක් slow connections වල හිරවෙලා → නිදහස් workers ඒවා නැවත ඉල්ලනවා."""
            now = time.monotonic()
            c = [(t0, off) for off, (t0, n) in inflight.items()
                 if off not in finished and n < HEDGE_MAX and now - t0 >= HEDGE_AFTER]
            return min(c)[1] if c else None

        async def worker(ln: Lane, widx: int):
            while not done_ev.is_set() and not ln.dead:
                if self.is_cancelled(download_id):
                    raise asyncio.CancelledError("Cancelled")
                wait = ln.flood_until - time.monotonic()
                if wait > 0:                                   # මේ lane එක FloodWait pause එකක — chunk අල්ලන්නේ නෑ
                    await asyncio.sleep(min(wait, 1.0))
                    continue
                hedge = False
                try:
                    off = queue.get_nowait()
                except asyncio.QueueEmpty:
                    off = pick_hedge()
                    if off is None:
                        await asyncio.sleep(0.1)
                        continue
                    hedge = True
                if off in finished:
                    continue
                rec = inflight.setdefault(off, [time.monotonic(), 0])
                rec[1] += 1
                try:
                    lim = min(CHUNK_SIZE, base + expected - off)
                    req_lim = lim if lim % 4096 == 0 else -(-lim // 4096) * 4096   # Telegram 4096 rule — tail aligned-up, clamp කරනවා
                    req = GetFileRequest(location=ln.location, offset=off, limit=req_lim)
                    r = await asyncio.wait_for(ln.pool.send(req, widx + (1 if hedge else 0)), timeout=CHUNK_TIMEOUT)
                    data = getattr(r, "bytes", None)
                    if data is None:
                        raise RuntimeError(f"Unexpected response: {type(r).__name__}")
                    if len(data) > lim:
                        data = data[:lim]
                    ln.bad = 0
                    if off not in finished:                    # පළමුව ඉවර වූ copy එක විතරක් ගණන් වෙනවා
                        finished.add(off)
                        await write_at(off - base, data)
                        ln.got += len(data)
                        _prog(len(data))
                        remaining[0] -= 1
                        if remaining[0] <= 0:
                            done_ev.set()
                except asyncio.CancelledError:
                    if not hedge and off not in finished:
                        queue.put_nowait(off)
                    raise
                except FloodWaitError as e:
                    if not hedge and off not in finished:
                        queue.put_nowait(off)                  # chunk එක වෙන lane එකකට
                    if ln.on_flood:
                        ln.on_flood(e.seconds)
                    if e.seconds > max_flood_wait and any(l is not ln and not l.dead for l in lanes):
                        lane_died(ln, f"FloodWait {e.seconds}s (> {max_flood_wait:.0f}s)")
                        return
                    ln.flood_until = max(ln.flood_until, time.monotonic() + e.seconds + random.uniform(0.5, 1.5))
                    logger.warning(f"[{ln.name}] FloodWait {e.seconds}s — lane paused, others continue")
                except Exception as e:
                    if not hedge and off not in finished:
                        queue.put_nowait(off)
                    if hedge:
                        continue                               # hedge copy එක fail වුණාට කමක් නෑ (මුල් එක තාම තියෙනවා)
                    n = fails[off] = fails.get(off, 0) + 1
                    msg = str(e).lower()
                    if "wait of" in msg:
                        m = re.search(r'wait of (\d+)', msg)
                        ln.flood_until = max(ln.flood_until, time.monotonic() + (int(m.group(1)) if m else 12) + 1)
                        continue
                    if n >= max_retries:
                        _fatal(RuntimeError(f"Chunk {off} {n}× fail ({e!r})"))
                        return
                    ln.bad += 1
                    if ln.bad >= 5 and any(l is not ln and not l.dead for l in lanes):
                        lane_died(ln, f"දිගටම errors ({ln.bad}): {e!r}")      # වෙන lanes ඉතුරුයි → මේක අයින්
                        return
                    await asyncio.sleep(min(2 ** (n - 1), 8) * BACKOFF_SCALE)
                finally:
                    rec[1] -= 1
                    if rec[1] <= 0:
                        inflight.pop(off, None)

        # workers lanes අතර interleave (A,B,C,A,B,C…) — නැත්නම් මුල් lane එකේ workers ම chunks ඔක්කොම අල්ලගන්නවා
        per = {ln.name: max(1, len(ln.pool.senders)) * wpc for ln in live}
        order = [(ln, i) for i in range(max(per.values())) for ln in live if i < per[ln.name]]
        tasks = [asyncio.create_task(worker(ln, i)) for ln, i in order]
        done_task = asyncio.create_task(done_ev.wait())
        try:
            while True:
                finished, _ = await asyncio.wait({done_task, fatal}, return_when=asyncio.FIRST_COMPLETED, timeout=1.0)
                if self.is_cancelled(download_id):
                    raise asyncio.CancelledError("Cancelled")
                if fatal.done():
                    fatal.result()                              # raises
                for t in tasks:
                    if t.done() and not t.cancelled() and t.exception():
                        raise t.exception()
                if done_ev.is_set():
                    break
            if downloaded[0] < expected:
                raise RuntimeError(f"Incomplete download {downloaded[0]}/{expected}")
            logger.info("lane summary: " + ", ".join(f"{l.name}={l.got / 1048576:.0f}MB" for l in lanes))
            return save_path
        except BaseException:
            self.force_delete_file(save_path)
            raise
        finally:
            done_task.cancel()
            for t in tasks:
                t.cancel()
            await asyncio.gather(done_task, *tasks, return_exceptions=True)
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
