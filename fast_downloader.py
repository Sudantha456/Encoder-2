"""
fast_downloader.py — Next-Gen Multi-Userbot Streaming Parallel Download Engine
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• Persistent Warm Multi-IP Connection Pools (Abridged MTProto + 8MB TCP buffers + TCP_NODELAY)
• Streaming Lane & Socket Activation: පළමු socket එක connect වූ සැණින් download ආරම්භ වේ (0ms warm start)
• Multi-Thread AsyncDiskWriter (C pwrite): disk flushes නිසා network event loop එක කිසිවිටෙක block නොවේ
• Smart Least-Loaded Routing + Adaptive Cross-Lane End-Game Hedging (tail latency ඉවත් කරයි)
• Auto FileReference Refresh + Per-Socket Self-Healing + Sub-Chunk Completeness Verification
"""
import asyncio
import collections
import contextlib
import inspect
import logging
import math
import os
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Callable, Deque, Dict, List, Optional

from telethon import TelegramClient
from telethon.errors import (
    AuthKeyError,
    FileMigrateError,
    FileReferenceExpiredError,
    FilerefUpgradeNeededError,
    FloodWaitError,
)
from telethon.tl.functions.upload import GetFileRequest
from telethon.tl.types import InputDocumentFileLocation

import config as _cfg
from config import PROGRESS_UPDATE_INTERVAL
from fast_conn import SenderPool, acquire_pool, close_all_pools, invalidate_auth_cache

_DEFAULT_SENDER_POOL = SenderPool

logger = logging.getLogger("FastDL")

CHUNK_SIZE = 1024 * 1024                                            # 1MB (Telegram max aligned block)
DL_CONNECTIONS = getattr(_cfg, "DL_CONNECTIONS", 12)
DL_WORKERS_PER_CONN = getattr(_cfg, "DL_WORKERS_PER_CONN", 5)
DL_DISK_THREADS = getattr(_cfg, "DL_DISK_THREADS", 4)
DL_MAX_PENDING_WRITES = getattr(_cfg, "DL_MAX_PENDING_WRITES", 64)
CHUNK_TIMEOUT = 18.0                                                # fast stall detection
HEDGE_AFTER = 2.0                                                   # max cap for adaptive end-game hedge
HEDGE_MAX = 3                                                       # max concurrent copies of one chunk
BACKOFF_SCALE = 0.6                                                 # snappy retry backoff
SMALL_FILE = 2 * 1024 * 1024                                        # < 2MB → native single-shot


_DISK_POOL: Optional[ThreadPoolExecutor] = None
_DISK_POOL_LOCK = threading.Lock()


def _get_disk_executor() -> ThreadPoolExecutor:
    global _DISK_POOL
    if _DISK_POOL is None:
        with _DISK_POOL_LOCK:
            if _DISK_POOL is None:
                _DISK_POOL = ThreadPoolExecutor(
                    max_workers=max(2, int(DL_DISK_THREADS)),
                    thread_name_prefix="FastDL-Disk",
                )
    return _DISK_POOL


class AsyncDiskWriter:
    """
    Non-blocking random-access file writer backed by a multi-thread C pwrite pool.
    Releases the GIL during pwrite() and applies bounded backpressure so network
    coroutines stay 100% unblocked even during kernel page-cache flushes.
    """

    def __init__(self, save_path: str, total_size: int, max_pending: int = DL_MAX_PENDING_WRITES):
        self.save_path = save_path
        self.total_size = total_size
        self._fd: Optional[int] = None
        self._has_pwrite = hasattr(os, "pwrite")
        self._seek_lock = threading.Lock()
        self._sem = asyncio.Semaphore(max(8, int(max_pending)))
        self._exec = _get_disk_executor()
        self._closed = False

    def _open_sync(self) -> None:
        parent = os.path.dirname(os.path.abspath(self.save_path))
        if parent:
            os.makedirs(parent, exist_ok=True)
        flags = os.O_RDWR | os.O_CREAT | os.O_TRUNC | getattr(os, "O_BINARY", 0)
        fd = os.open(self.save_path, flags, 0o666)
        try:
            if self.total_size > 0:
                os.ftruncate(fd, self.total_size)
            fadvise = getattr(os, "posix_fadvise", None)
            seq_flag = getattr(os, "POSIX_FADV_SEQUENTIAL", None)
            if fadvise is not None and seq_flag is not None and self.total_size > 0:
                with contextlib.suppress(OSError):
                    fadvise(fd, 0, self.total_size, seq_flag)
        except BaseException:
            os.close(fd)
            raise
        self._fd = fd

    async def open(self) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._exec, self._open_sync)

    def _write_sync(self, off: int, data: bytes) -> None:
        fd = self._fd
        if fd is None:
            raise RuntimeError("Disk writer already closed")
        mv = memoryview(data)
        total = len(mv)
        written = 0
        if self._has_pwrite:
            while written < total:
                n = os.pwrite(fd, mv[written:], off + written)
                if n <= 0:
                    raise OSError(f"os.pwrite returned {n} at offset {off + written}")
                written += n
        else:
            with self._seek_lock:
                os.lseek(fd, off, os.SEEK_SET)
                while written < total:
                    n = os.write(fd, mv[written:])
                    if n <= 0:
                        raise OSError(f"os.write returned {n} at offset {off + written}")
                    written += n

    async def write(self, off: int, data: bytes) -> None:
        async with self._sem:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(self._exec, self._write_sync, off, data)

    def _close_sync(self) -> None:
        fd, self._fd = self._fd, None
        if fd is not None:
            with contextlib.suppress(OSError):
                os.close(fd)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._exec, self._close_sync)


async def _send_pool_req(
    pool: Any,
    req: Any,
    hint: int,
    timeout: float,
    slot_idx: Optional[int] = None,
    max_slot_inflight: int = 4,
) -> Any:
    if slot_idx is not None:
        send_pref = getattr(pool, "send_slot_preferred", None)
        if callable(send_pref):
            return await send_pref(
                slot_idx, req, timeout=timeout, max_slot_inflight=max_slot_inflight
            )
    send_smart = getattr(pool, "send_smart", None)
    if callable(send_smart):
        return await send_smart(req, hint=hint, timeout=timeout)
    return await asyncio.wait_for(pool.send(req, hint), timeout=timeout)


async def _fetch_chunk_bytes(
    pool: Any,
    location: Any,
    off: int,
    expected_len: int,
    hint: int,
    timeout: float,
    slot_idx: Optional[int] = None,
    max_slot_inflight: int = 4,
) -> bytes:
    """
    Fetch an exact [off, off + expected_len) chunk.
    Fast-paths single 1MB responses with zero copy; seamlessly stitches sub-blocks
    if a Telegram storage server ever returns smaller slices (e.g. 512KB).
    """
    req = GetFileRequest(location=location, offset=off, limit=CHUNK_SIZE)
    res = await _send_pool_req(
        pool, req, hint=hint, timeout=timeout,
        slot_idx=slot_idx, max_slot_inflight=max_slot_inflight,
    )
    data = getattr(res, "bytes", None)
    if data is None:
        raise RuntimeError(f"Unexpected GetFile response: {type(res).__name__}")
    got_len = len(data)
    if got_len == 0 and expected_len > 0:
        raise RuntimeError(f"Empty chunk returned at offset {off}")
    if got_len >= expected_len:
        return data if got_len == expected_len else data[:expected_len]

    buf = bytearray(data)
    while len(buf) < expected_len:
        sub_off = off + len(buf)
        need = expected_len - len(buf)
        rem_in_mb = CHUNK_SIZE - (sub_off % CHUNK_SIZE)
        aligned_limit = min(rem_in_mb, max(4096, ((need + 4095) // 4096) * 4096))
        sub_req = GetFileRequest(
            location=location,
            offset=sub_off,
            limit=aligned_limit,
            precise=True,
        )
        sub_res = await _send_pool_req(
            pool, sub_req, hint=hint + 1, timeout=timeout,
            slot_idx=slot_idx, max_slot_inflight=max_slot_inflight,
        )
        more = getattr(sub_res, "bytes", None)
        if not more:
            raise RuntimeError(f"Incomplete sub-chunk at offset {sub_off} ({len(buf)}/{expected_len})")
        buf.extend(more[:need])
    return bytes(buf)


def _is_fileref_error(exc: BaseException) -> bool:
    if isinstance(exc, (FileReferenceExpiredError, FilerefUpgradeNeededError)):
        return True
    msg = str(exc).lower()
    return "file_reference" in msg or "file reference" in msg or "fileref" in msg


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
    got: int = 0                                              # මේ lane එකෙන් download කළ bytes
    bad: int = 0                                              # දිගටම ආපු errors (සාර්ථක chunk එකකින් reset)
    on_flood: Optional[Callable[[int], None]] = None          # (seconds) → userbot pool එකට දැනුම් දෙන්න
    refresh_location: Optional[Callable[[], Any]] = None      # async () -> updated InputDocumentFileLocation
    persistent_pool: bool = True                              # warm SenderPool reuse across jobs


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
        if getattr(message, "document", None):
            return InputDocumentFileLocation(
                id=message.document.id,
                access_hash=message.document.access_hash,
                file_reference=message.document.file_reference,
                thumb_size="",
            ), message.document.size
        return None, 0

    async def _obtain_lane_pool(self, client: Any, dc_id: Optional[int], connections: int,
                                persistent: bool = True, use_main: bool = False) -> Any:
        # Respect monkeypatched SenderPool in unit tests; use persistent warm pools in production
        if persistent and SenderPool is _DEFAULT_SENDER_POOL:
            return await acquire_pool(client, dc_id, size=connections, use_main=use_main)
        return SenderPool(client, dc_id, connections, use_main=use_main)

    async def _start_pool_streaming(self, pool: Any, size: int, on_slot_ready: Callable[[Any], None]) -> None:
        start_fn = pool.start
        try:
            sig = inspect.signature(start_fn)
            supports_cb = "on_slot_ready" in sig.parameters
        except (TypeError, ValueError):
            supports_cb = False

        if supports_cb:
            await asyncio.wait_for(
                start_fn(target_size=size, on_slot_ready=on_slot_ready),
                timeout=60.0,
            )
        else:
            await asyncio.wait_for(start_fn(), timeout=60.0)
            senders = getattr(pool, "slots", None) or getattr(pool, "senders", [])
            for s in senders:
                on_slot_ready(s)

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

        doc = getattr(message, "document", None)
        doc_size = getattr(doc, "size", 0) if doc else 0
        if getattr(message, "photo", None) or doc_size < SMALL_FILE:
            return await self._download_native(client, message, save_path, download_id, progress_callback, max_retries)

        location, total_size = self._get_file_location(message)
        if not location or total_size == 0:
            return await self._download_native(client, message, save_path, download_id, progress_callback, max_retries)

        async def _refresh_msg_loc():
            chat_id = getattr(message, "chat_id", None) or getattr(message, "peer_id", None)
            if chat_id is None or not getattr(message, "id", None):
                return None
            fresh = await client.get_messages(chat_id, ids=message.id)
            fdoc = getattr(fresh, "document", None) if fresh else None
            if fdoc and fdoc.id == location.id:
                return InputDocumentFileLocation(
                    id=fdoc.id,
                    access_hash=fdoc.access_hash,
                    file_reference=fdoc.file_reference,
                    thumb_size="",
                )
            return None

        single_lane = Lane(
            name="BOT",
            client=client,
            location=location,
            dc_id=getattr(doc, "dc_id", None),
            connections=DL_CONNECTIONS,
            refresh_location=_refresh_msg_loc,
            persistent_pool=True,
        )

        try:
            return await self.download_lanes(
                [single_lane],
                total_size=total_size,
                save_path=save_path,
                download_id=download_id,
                progress_callback=progress_callback,
                max_retries=max_retries,
                max_flood_wait=120.0,
                workers_per_conn=DL_WORKERS_PER_CONN,
            )
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

    # ═════════ Multi-userbot Turbo Swarm Download Engine ═════════
    async def download_lanes(
        self,
        lanes: List[Lane],
        total_size: int,
        save_path: str,
        download_id: str,
        progress_callback: Optional[Callable] = None,
        max_retries: int = 8,
        max_flood_wait: float = 30.0,
        workers_per_conn: Optional[int] = None,
        extra_lane_coros: Optional[List[Any]] = None,
    ) -> str:
        """
        • Streaming multi-userbot swarm: warm pools start in ~0ms; cold sockets spawn workers as each connects.
        • Lock-free O(1) cursor + priority retry deque + bytearray completion bitmap.
        • Adaptive cross-lane end-game hedging eliminates slow tail chunks.
        • Non-blocking multi-thread pwrite disk engine.
        """
        if total_size <= 0:
            raise ValueError(f"Invalid total_size: {total_size}")

        wpc = max(1, int(workers_per_conn or DL_WORKERS_PER_CONN))
        self._cancel_flags[download_id] = False
        loop = asyncio.get_running_loop()
        t_dl_start = time.monotonic()

        n_chunks = (total_size + CHUNK_SIZE - 1) // CHUNK_SIZE
        done_bitmap = bytearray(n_chunks)              # 0 = pending, 1 = finished (no set shadowing!)
        main_cursor = [0]
        retry_deque: Deque[int] = collections.deque()
        remaining = [n_chunks]
        fails: Dict[int, int] = {}
        inflight: Dict[int, list] = {}                 # cidx -> [t0, count, first_lane_name]
        ewma_chunk = [0.5]

        done_ev = asyncio.Event()
        wake_ev = asyncio.Event()
        fatal: asyncio.Future = loop.create_future()
        fatal.add_done_callback(lambda f: f.cancelled() or f.exception())

        downloaded = [0]
        last_update = [0.0]

        all_lanes: List[Lane] = list(lanes)
        pending_lane_discoveries = [len(extra_lane_coros or [])]
        pending_lane_starts = [len(lanes) + pending_lane_discoveries[0]]
        expected_total_lanes = max(1, pending_lane_starts[0])

        writer = AsyncDiskWriter(save_path, total_size)
        await writer.open()

        worker_tasks: List[asyncio.Task] = []
        startup_tasks: List[asyncio.Task] = []
        lane_worker_counts: Dict[int, int] = {}
        lane_refresh_locks: Dict[int, asyncio.Lock] = {}
        lane_last_refresh: Dict[int, float] = {}

        def _fatal(exc: BaseException) -> None:
            if not fatal.done():
                fatal.set_exception(exc)

        def _prog(n: int) -> None:
            downloaded[0] += n

        async def _progress_loop() -> None:
            if not progress_callback:
                return
            tick_iv = min(1.0, max(0.25, PROGRESS_UPDATE_INTERVAL * 0.5))
            last_sent = -1
            while not done_ev.is_set():
                await asyncio.sleep(tick_iv)
                cur = downloaded[0]
                if cur != last_sent:
                    last_sent = cur
                    with contextlib.suppress(Exception):
                        res = progress_callback(cur, total_size)
                        if inspect.isawaitable(res):
                            await res

        def _check_all_dead(why: str) -> None:
            if pending_lane_starts[0] <= 0 and (not all_lanes or all(l.dead for l in all_lanes)):
                _fatal(RuntimeError("සියලුම userbot lanes fail වුණා: " + why))

        def lane_died(ln: Lane, why: str) -> None:
            if not ln.dead:
                ln.dead = True
                logger.warning(f"[{ln.name}] lane dropped: {why}")
                wake_ev.set()
            _check_all_dead(why)

        def _fair_lane_worker_cap(ln: Lane) -> int:
            live_lanes = sum(1 for l in all_lanes if not l.dead) + pending_lane_discoveries[0]
            share_lanes = max(1, live_lanes if live_lanes > 0 else expected_total_lanes)
            full_cap = max(1, ln.connections) * wpc
            if n_chunks >= share_lanes * full_cap:
                return full_cap
            # Fair cap on smaller files so Lane #1 never hoards the entire queue before Lanes #2..#N pull
            return max(2, min(full_cap, math.ceil(n_chunks / share_lanes)))

        def _next_chunk() -> Optional[int]:
            while retry_deque:
                cidx = retry_deque.popleft()
                if not done_bitmap[cidx]:
                    return cidx
            if main_cursor[0] < n_chunks:
                cidx = main_cursor[0]
                main_cursor[0] += 1
                return cidx
            return None

        def _pick_hedge(my_lane: str) -> Optional[int]:
            """
            Adaptive cross-lane end-game hedging:
            • Dynamic threshold scales with observed EWMA chunk latency (0.3s..2.0s)
            • Strongly prefers hedging chunks stuck on a DIFFERENT userbot lane!
            """
            if not inflight:
                return None
            now = time.monotonic()
            thresh = min(max(ewma_chunk[0] * 1.6, 0.30), HEDGE_AFTER)
            fast_tail = remaining[0] <= max(8, expected_total_lanes * 2)
            tail_thresh = max(0.22, ewma_chunk[0] * 1.15)

            best_key = None
            best_cidx = None
            for cidx, rec in inflight.items():
                if done_bitmap[cidx] or rec[1] >= HEDGE_MAX:
                    continue
                age = now - rec[0]
                diff_lane = rec[2] != my_lane
                eligible = (age >= thresh) or (fast_tail and diff_lane and rec[1] == 1 and age >= tail_thresh)
                if not eligible:
                    continue
                # Prioritize cross-lane hedges first, fewer active copies second, oldest chunk third
                cand_key = (0 if diff_lane else 1, rec[1], -age)
                if best_key is None or cand_key < best_key:
                    best_key = cand_key
                    best_cidx = cidx
            return best_cidx

        async def _try_refresh_loc(ln: Lane) -> bool:
            if not ln.refresh_location:
                return False
            lid = id(ln)
            lk = lane_refresh_locks.setdefault(lid, asyncio.Lock())
            async with lk:
                if time.monotonic() - lane_last_refresh.get(lid, 0.0) < 2.0:
                    return True
                try:
                    res = ln.refresh_location()
                    new_loc = await res if inspect.isawaitable(res) else res
                    if new_loc is not None:
                        ln.location = new_loc
                        lane_last_refresh[lid] = time.monotonic()
                        logger.info(f"[{ln.name}] refreshed expired file_reference")
                        return True
                except Exception as e:
                    logger.warning(f"[{ln.name}] file_reference refresh failed: {e!r}")
                return False

        async def worker(ln: Lane, widx: int, slot_idx: int = 0, pace_delay: float = 0.0) -> None:
            if pace_delay > 0 and n_chunks >= 8 and not done_ev.is_set():
                await asyncio.sleep(pace_delay)
            lane_fail_limit = max(32, ln.connections * wpc * 2)
            while not done_ev.is_set() and not ln.dead:
                if self.is_cancelled(download_id):
                    raise asyncio.CancelledError("Cancelled")

                wait = ln.flood_until - time.monotonic()
                if wait > 0:
                    await asyncio.sleep(min(wait, 0.5))
                    continue

                hedge = False
                cidx = _next_chunk()
                if cidx is None:
                    cidx = _pick_hedge(ln.name)
                    if cidx is None:
                        wake_ev.clear()
                        if done_ev.is_set():
                            return
                        try:
                            await asyncio.wait_for(wake_ev.wait(), timeout=0.04)
                        except asyncio.TimeoutError:
                            pass
                        continue
                    hedge = True

                if done_bitmap[cidx]:
                    continue

                off = cidx * CHUNK_SIZE
                expected_len = min(CHUNK_SIZE, total_size - off)
                t_req = time.monotonic()
                rec = inflight.get(cidx)
                if rec is None:
                    rec = [t_req, 1, ln.name]
                    inflight[cidx] = rec
                else:
                    rec[1] += 1

                try:
                    data = await _fetch_chunk_bytes(
                        ln.pool,
                        ln.location,
                        off,
                        expected_len,
                        hint=widx + (3 if hedge else 0),
                        timeout=CHUNK_TIMEOUT,
                        slot_idx=None if hedge else slot_idx,
                        max_slot_inflight=wpc + 1,
                    )
                    dt = max(0.01, time.monotonic() - t_req)
                    ewma_chunk[0] = ewma_chunk[0] * 0.85 + dt * 0.15
                    ln.bad = 0

                    if not done_bitmap[cidx]:
                        done_bitmap[cidx] = 1
                        await writer.write(off, data)
                        ln.got += len(data)
                        _prog(len(data))
                        remaining[0] -= 1
                        if remaining[0] <= 0:
                            done_ev.set()
                            wake_ev.set()
                            return
                except asyncio.CancelledError:
                    if not hedge and not done_bitmap[cidx]:
                        retry_deque.appendleft(cidx)
                        wake_ev.set()
                    raise
                except FileMigrateError as e:
                    if not hedge and not done_bitmap[cidx]:
                        retry_deque.appendleft(cidx)
                        wake_ev.set()
                    try:
                        old_pool = ln.pool
                        ln.dc_id = e.new_dc
                        ln.pool = await self._obtain_lane_pool(
                            ln.client, ln.dc_id, ln.connections, persistent=ln.persistent_pool
                        )
                        await asyncio.wait_for(ln.pool.start(target_size=ln.connections), timeout=45.0)
                        if old_pool:
                            await old_pool.close()
                    except Exception as mig_err:
                        lane_died(ln, f"DC migrate failed: {mig_err!r}")
                        return
                except FloodWaitError as e:
                    if not hedge and not done_bitmap[cidx]:
                        retry_deque.appendleft(cidx)
                        wake_ev.set()
                    now = time.monotonic()
                    # Deduplicate burst notifications from simultaneous workers on the same lane
                    first_in_burst = now >= ln.flood_until
                    if first_in_burst and ln.on_flood and (e.seconds > 5 or e.seconds > max_flood_wait):
                        ln.on_flood(e.seconds)
                    if e.seconds > max_flood_wait and any(l is not ln and not l.dead for l in all_lanes):
                        lane_died(ln, f"FloodWait {e.seconds}s (> {max_flood_wait:.0f}s)")
                        return
                    ln.flood_until = max(ln.flood_until, now + e.seconds + random.uniform(0.3, 1.0))
                    if first_in_burst:
                        logger.warning(f"[{ln.name}] FloodWait {e.seconds}s — lane paused, other lanes continue")
                except Exception as e:
                    if _is_fileref_error(e):
                        refreshed = await _try_refresh_loc(ln)
                        if not hedge and not done_bitmap[cidx]:
                            retry_deque.appendleft(cidx)
                            wake_ev.set()
                        if refreshed:
                            continue

                    if isinstance(e, AuthKeyError) and ln.dc_id:
                        invalidate_auth_cache(ln.client, ln.dc_id)

                    if not hedge and not done_bitmap[cidx]:
                        retry_deque.appendleft(cidx)
                        wake_ev.set()
                    if hedge:
                        continue

                    msg = str(e).lower()
                    if "wait of" in msg or "flood" in msg:
                        m = re.search(r"wait of (\d+)", msg)
                        secs = int(m.group(1)) if m else 10
                        ln.flood_until = max(ln.flood_until, time.monotonic() + secs + 1.0)
                        continue

                    n = fails[cidx] = fails.get(cidx, 0) + 1
                    if n >= max_retries:
                        _fatal(RuntimeError(f"Chunk {off} failed {n}× ({e!r})"))
                        return

                    ln.bad += 1
                    other_alive = any(l is not ln and not l.dead for l in all_lanes) or pending_lane_starts[0] > 0
                    if ln.bad >= lane_fail_limit and other_alive:
                        lane_died(ln, f"දිගටම errors ({ln.bad}): {e!r}")
                        return
                    await asyncio.sleep(0.05 * (2 ** min(n - 1, 5)) * BACKOFF_SCALE)
                finally:
                    rec[1] -= 1
                    if rec[1] <= 0:
                        inflight.pop(cidx, None)
                    if main_cursor[0] >= n_chunks:
                        wake_ev.set()

        def _spawn_workers_for_slot(ln: Lane, slot_obj: Any = None) -> None:
            if done_ev.is_set() or ln.dead:
                return
            lid = id(ln)
            cur = lane_worker_counts.get(lid, 0)
            cap = _fair_lane_worker_cap(ln)
            to_spawn = min(wpc, max(0, cap - cur))
            slot_idx = int(getattr(slot_obj, "idx", cur // wpc) or 0)
            for k in range(to_spawn):
                idx = cur + k
                pace = (k * 0.012) + ((slot_idx % 8) * 0.003)
                worker_tasks.append(asyncio.create_task(worker(ln, idx, slot_idx=slot_idx, pace_delay=pace)))
            if to_spawn > 0:
                lane_worker_counts[lid] = cur + to_spawn

        async def _activate_lane(stagger_idx: int, ln: Lane) -> None:
            try:
                if ln.pool is None:
                    ln.pool = await self._obtain_lane_pool(
                        ln.client, ln.dc_id, ln.connections, persistent=ln.persistent_pool
                    )
                # Warm pools have slots ready already → 0ms stagger; cold pools use tiny 30ms stagger
                has_warm = bool(getattr(ln.pool, "slots", None))
                if not has_warm and stagger_idx > 0:
                    await asyncio.sleep(min(stagger_idx * 0.03, 0.15))

                await self._start_pool_streaming(
                    ln.pool,
                    size=ln.connections,
                    on_slot_ready=lambda slot_obj: _spawn_workers_for_slot(ln, slot_obj),
                )
                # Ensure at least 1 worker spawned if pool started with slots
                if lane_worker_counts.get(id(ln), 0) == 0 and getattr(ln.pool, "senders", None):
                    _spawn_workers_for_slot(ln)
            except FloodWaitError as e:
                ln.dead = True
                if ln.on_flood:
                    ln.on_flood(e.seconds)
                logger.warning(f"[{ln.name}] lane start FloodWait: {e!r}")
            except BaseException as e:
                ln.dead = True
                logger.warning(f"[{ln.name}] lane start failed: {e!r}")
            finally:
                pending_lane_starts[0] = max(0, pending_lane_starts[0] - 1)
                _check_all_dead("Userbot lane එකක්වත් start වුණේ නෑ")

        async def _discover_and_activate(idx: int, coro: Any) -> None:
            ln: Optional[Lane] = None
            try:
                pair = await coro
                if pair:
                    ln = pair[0] if isinstance(pair, tuple) else pair
                    all_lanes.append(ln)
                    lanes.append(ln)
            except Exception as e:
                logger.warning(f"Extra lane discovery #{idx} failed: {e!r}")
            finally:
                pending_lane_discoveries[0] = max(0, pending_lane_discoveries[0] - 1)

            if ln is not None:
                await _activate_lane(idx, ln)
            else:
                pending_lane_starts[0] = max(0, pending_lane_starts[0] - 1)
                _check_all_dead("Userbot lane එකක්වත් start වුණේ නෑ")

        for i, ln in enumerate(lanes):
            startup_tasks.append(asyncio.create_task(_activate_lane(i, ln)))
        for j, coro in enumerate(extra_lane_coros or [], start=len(lanes)):
            startup_tasks.append(asyncio.create_task(_discover_and_activate(j, coro)))

        if not startup_tasks:
            await writer.close()
            raise RuntimeError("Userbot lane එකක්වත් ලබා දී නෑ")

        done_task = asyncio.create_task(done_ev.wait())
        prog_task = asyncio.create_task(_progress_loop())
        try:
            while True:
                await asyncio.wait({done_task, fatal}, return_when=asyncio.FIRST_COMPLETED, timeout=0.5)
                if self.is_cancelled(download_id):
                    raise asyncio.CancelledError("Cancelled")
                if fatal.done():
                    fatal.result()
                for t in worker_tasks:
                    if t.done() and not t.cancelled() and t.exception():
                        raise t.exception()
                if done_ev.is_set():
                    break
                # Safety check if all startup + worker tasks exited unexpectedly before completion
                if (
                    pending_lane_starts[0] <= 0
                    and all(t.done() for t in startup_tasks)
                    and (not worker_tasks or all(t.done() for t in worker_tasks))
                    and not done_ev.is_set()
                ):
                    raise RuntimeError("Userbot lane එකක්වත් සක්‍රියව නැත (all workers exited)")

            if downloaded[0] < total_size:
                raise RuntimeError(f"Incomplete download {downloaded[0]}/{total_size}")

            await writer.close()
            if progress_callback:
                with contextlib.suppress(Exception):
                    res = progress_callback(total_size, total_size)
                    if inspect.isawaitable(res):
                        await res
            dt_total = max(0.001, time.monotonic() - t_dl_start)
            mb_s = (total_size / 1048576) / dt_total
            logger.info(
                f"⚡ FastDL complete: {total_size / 1048576:.1f}MB in {dt_total:.2f}s ({mb_s:.1f} MB/s) | "
                + ", ".join(f"{l.name}={l.got / 1048576:.1f}MB" for l in all_lanes)
            )
            return save_path
        except BaseException:
            await writer.close()
            self.force_delete_file(save_path)
            raise
        finally:
            done_task.cancel()
            prog_task.cancel()
            for t in startup_tasks:
                if not t.done():
                    t.cancel()
            for t in worker_tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(done_task, prog_task, *startup_tasks, *worker_tasks, return_exceptions=True)
            await writer.close()
            # Note: managed pools merely release their borrow count & stay warm for the next download!
            await asyncio.gather(*[ln.pool.close() for ln in all_lanes if ln.pool], return_exceptions=True)

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
                        m = re.search(r"wait of (\d+)", err_str)
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
    _get_disk_executor()
    logger.info(
        f"⚡ FastDownloader Turbo Engine initialized "
        f"(conns={DL_CONNECTIONS}, ub_conns={getattr(_cfg, 'UB_DL_CONNECTIONS', 16)}, "
        f"workers/conn={DL_WORKERS_PER_CONN}, disk_threads={DL_DISK_THREADS})"
    )
    return dl


async def close_download_engine():
    global _downloader, _DISK_POOL
    await close_all_pools()
    pool, _DISK_POOL = _DISK_POOL, None
    if pool is not None:
        pool.shutdown(wait=False)
    _downloader = None
