"""
fast_uploader.py — Next-Gen Turbo Parallel MTProto Uploader (Sustained Peak Speed Edition)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• Warm Persistent Home-DC Pool (acquire_pool): cold handshake / slow-start නැතිව තත්පර 0 න් උපරිම වේගය.
• Slot-Pinned Constant-Depth Pipelining + Micro-Pacing: හැම TCP socket එකකම නියත pipeline depth (3×512KB)
  පවත්වා ගනිමින් burst-and-stall speed බැසීම් (sawtooth drops) 100% ඉවත් කරයි.
• Non-Blocking AsyncDiskReader (os.pread + Linux POSIX_FADV_SEQUENTIAL read-ahead):
  event loop එක කිසිසේත් block නොකර sequential 512KB කොටස් RAM cache එකෙන් කෙළින්ම කියවයි.
• Idempotent End-Game Tail Hedging + Adaptive Timeout: 98%→100% අවසාන කොටස් හිරවීම සම්පූර්ණයෙන් නැති කරයි.
"""
import asyncio
import collections
import concurrent.futures
import contextlib
import inspect
import json
import logging
import os
import random
import re
import subprocess
import threading
import time
from typing import Any, Callable, Deque, Dict, List, Optional

from telethon import TelegramClient
from telethon.errors import AuthKeyError, FloodWaitError
from telethon.helpers import generate_random_long
from telethon.tl.functions.upload import SaveBigFilePartRequest, SaveFilePartRequest
from telethon.tl.types import (
    DocumentAttributeAudio,
    DocumentAttributeFilename,
    DocumentAttributeVideo,
    InputFile,
    InputFileBig,
)

import config as _cfg
from ffenv import ffmpeg_env
from config import PROGRESS_UPDATE_INTERVAL, UL_CONNECTIONS, UL_WORKERS_PER_CONN
from fast_conn import SenderPool, acquire_pool

logger = logging.getLogger("FastUploader")

CHUNK_SIZE = 512 * 1024               # 512 KB — Telegram SaveBigFilePart / SaveFilePart maximum
BIG_FILE_THRESHOLD = 10 * 1024 * 1024 # > 10 MB → SaveBigFilePartRequest / InputFileBig
PART_TIMEOUT = 22.0                   # Per-part RPC timeout before automatic healthy-slot retry
HEDGE_AFTER = 2.2                     # End-game tail hedge ceiling (s)
HEDGE_MAX = 2                         # Original + 1 backup copy for tail stragglers
UL_DISK_THREADS = int(getattr(_cfg, "UL_DISK_THREADS", 4))

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".bmp"}
VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv", ".m4v", ".ts"}
AUDIO_EXTS = {".mp3", ".m4a", ".aac", ".ogg", ".opus", ".flac", ".wav"}

_DEFAULT_SENDER_POOL = SenderPool
_READ_POOL: Optional[concurrent.futures.ThreadPoolExecutor] = None


def _get_read_executor() -> concurrent.futures.ThreadPoolExecutor:
    global _READ_POOL
    if _READ_POOL is None:
        _READ_POOL = concurrent.futures.ThreadPoolExecutor(
            max_workers=max(2, UL_DISK_THREADS),
            thread_name_prefix="fastul-disk",
        )
    return _READ_POOL


class LongFloodWait(Exception):
    """FloodWait > max_flood_wait වුණොත් raise වෙනවා (caller ට වෙන userbot එකකට මාරු වෙන්න)."""

    def __init__(self, seconds: int):
        self.seconds = seconds
        super().__init__(f"FloodWait {seconds}s")


class AsyncDiskReader:
    """
    Thread-pool backed positional file reader using os.pread() + POSIX_FADV_SEQUENTIAL.
    Eliminates event-loop blocking and seek-lock contention across parallel upload workers.
    """

    def __init__(self, path: str, total_size: int):
        self.path = path
        self.total_size = total_size
        self._fd: Optional[int] = None
        self._has_pread = hasattr(os, "pread")
        self._fallback_lock = threading.Lock()
        self._closed = False
        self._exec = _get_read_executor()

    def _open_sync(self) -> None:
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
        fd = os.open(self.path, flags)
        fadvise = getattr(os, "posix_fadvise", None)
        seq_flag = getattr(os, "POSIX_FADV_SEQUENTIAL", None)
        if callable(fadvise) and seq_flag is not None and self.total_size > 0:
            with contextlib.suppress(OSError):
                fadvise(fd, 0, self.total_size, seq_flag)
        self._fd = fd

    async def open(self) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._exec, self._open_sync)

    def _pread_sync(self, offset: int, length: int) -> bytes:
        fd = self._fd
        if fd is None:
            raise RuntimeError("AsyncDiskReader already closed")
        out = bytearray()
        while len(out) < length:
            need = length - len(out)
            cur_off = offset + len(out)
            if self._has_pread:
                chunk = os.pread(fd, need, cur_off)
            else:
                with self._fallback_lock:
                    os.lseek(fd, cur_off, os.SEEK_SET)
                    chunk = os.read(fd, need)
            if not chunk:
                break
            out.extend(chunk)
        return bytes(out)

    async def read_at(self, offset: int, length: int) -> bytes:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._exec, self._pread_sync, offset, length)

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


async def _send_upload_req(
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


class FastUploader:

    def __init__(self):
        self._cancel_flags: Dict[str, bool] = {}

    def cancel(self, upload_id: str):
        self._cancel_flags[upload_id] = True

    def is_cancelled(self, upload_id: str) -> bool:
        return self._cancel_flags.get(upload_id, False)

    async def cleanup(self, upload_id: str):
        self._cancel_flags.pop(upload_id, None)

    async def get_video_info(self, filepath: str, hint: Optional[Dict] = None) -> Dict:
        """Probe output dimensions for the Telegram video attributes.

        ffprobe is not part of every static ffmpeg download, so fall back to
        `ffmpeg -i` output; without correct width/height Telegram sizes the
        player box wrongly (`hint` carries the bot's own known dimensions).
        """
        info = await self._probe(filepath)
        video_stream = next(
            (s for s in info.get("streams", []) if s.get("codec_type") == "video"), None
        )
        if not video_stream:
            try:
                video_stream = await asyncio.to_thread(_probe_with_ffmpeg, filepath)
            except Exception as e:
                logger.warning(f"ffmpeg fallback probe failed: {e!r}")
                video_stream = None
        try:
            duration = float(info.get("format", {}).get("duration", 0))
        except (TypeError, ValueError):
            duration = 0.0
        width = int(video_stream.get("width", 0)) if video_stream else 0
        height = int(video_stream.get("height", 0)) if video_stream else 0
        # Anamorphic video (SAR != 1:1) must be reported at display size,
        # otherwise Telegram draws the player in the wrong aspect box.
        sar = (video_stream or {}).get("sample_aspect_ratio") or "1:1"
        try:
            num, den = (int(x) for x in str(sar).split(":"))
            if num and den and num != den:
                width = int(round(width * num / den))
        except (ValueError, TypeError):
            pass
        hint = hint or {}
        if not width or not height:
            width, height = int(hint.get("width") or 0), int(hint.get("height") or 0)
        if not duration:
            duration = float(hint.get("duration") or 0)
        if not width or not height:
            logger.warning(f"video dimensions unknown for {filepath} — Telegram metadata may be wrong")
        return {"duration": int(duration), "width": width, "height": height}

    async def _probe(self, filepath: str) -> Dict:
        cmd = [
            "ffprobe", "-v", "quiet", "-print_format", "json",
            "-show_format", "-show_streams", filepath,
        ]
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=ffmpeg_env(),
            )
        except FileNotFoundError:
            return {}
        try:
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=20.0)
        except Exception:
            with contextlib.suppress(Exception):
                proc.kill()
            return {}
        try:
            return json.loads(stdout.decode("utf-8", errors="replace") or "{}")
        except ValueError:
            return {}

    async def generate_thumbnail(self, filepath: str) -> Optional[str]:
        thumb_path = filepath + "_thumb.jpg"
        for ts in ("00:00:03", "00:00:01", "00:00:00"):
            cmd = [
                "ffmpeg", "-y", "-ss", ts, "-i", filepath,
                "-vframes", "1",
                "-vf", "scale='min(320,iw)':-2",
                "-q:v", "4", thumb_path,
            ]
            try:
                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                    env=ffmpeg_env(),
                )
                await asyncio.wait_for(proc.wait(), timeout=15.0)
                if os.path.exists(thumb_path) and os.path.getsize(thumb_path) > 100:
                    return thumb_path
            except Exception:
                pass
        return None

    async def _obtain_pool(self, client: Any, connections: int, use_main: bool = True) -> Any:
        sess = getattr(client, "session", None)
        home_dc = getattr(sess, "dc_id", None)
        if SenderPool is _DEFAULT_SENDER_POOL:
            return await acquire_pool(client, home_dc, size=connections, use_main=use_main)
        return SenderPool(client, home_dc, connections, use_main=use_main)

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

    # ═════════ Next-Gen Turbo Parallel Upload Engine ═════════
    async def _parallel_upload(
        self,
        client: TelegramClient,
        filepath: str,
        filename: str,
        file_size: int,
        upload_id: str,
        progress_callback: Optional[Callable] = None,
        connections: Optional[int] = None,
        max_flood_wait: Optional[float] = None,
        use_main: bool = True,
        workers_per_conn: Optional[int] = None,
        max_retries: int = 8,
    ):
        if file_size == 0:
            return filepath

        conns = max(1, int(connections or UL_CONNECTIONS))
        wpc = max(1, int(workers_per_conn or UL_WORKERS_PER_CONN))
        file_id = generate_random_long()
        is_big = file_size > BIG_FILE_THRESHOLD
        total_parts = (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE
        t_ul_start = time.monotonic()

        done_bitmap = bytearray(total_parts)
        main_cursor = [0]
        retry_deque: Deque[int] = collections.deque()
        remaining = [total_parts]
        fails: Dict[int, int] = {}
        inflight: Dict[int, list] = {}          # pidx -> [t0, active_copies, slot_idx]
        ewma_part = [0.35]

        uploaded = [0]
        flood_until = [0.0]

        loop = asyncio.get_running_loop()
        done_ev = asyncio.Event()
        wake_ev = asyncio.Event()
        fatal: asyncio.Future = loop.create_future()
        fatal.add_done_callback(lambda f: f.cancelled() or f.exception())

        reader = AsyncDiskReader(filepath, file_size)
        await reader.open()
        pool = await self._obtain_pool(client, connections=conns, use_main=use_main)

        worker_tasks: List[asyncio.Task] = []
        spawned_count = [0]

        def _fatal(exc: BaseException) -> None:
            if not fatal.done():
                fatal.set_exception(exc)

        def _next_part() -> Optional[int]:
            while retry_deque:
                pidx = retry_deque.popleft()
                if not done_bitmap[pidx]:
                    return pidx
            if main_cursor[0] < total_parts:
                pidx = main_cursor[0]
                main_cursor[0] += 1
                return pidx
            return None

        def _pick_tail_hedge(my_slot: int) -> Optional[int]:
            """SaveBigFilePart / SaveFilePart are idempotent: rescue stalled tail parts on an alternate socket."""
            if not inflight or remaining[0] > max(12, conns * 2):
                return None
            now = time.monotonic()
            thresh = min(max(ewma_part[0] * 2.2, 0.45), HEDGE_AFTER)
            best_pidx = None
            best_age = 0.0
            for pidx, rec in inflight.items():
                if done_bitmap[pidx] or rec[1] >= HEDGE_MAX:
                    continue
                age = now - rec[0]
                diff_slot = rec[2] != my_slot
                if age >= (thresh * 0.75 if diff_slot else thresh) and age > best_age:
                    best_age = age
                    best_pidx = pidx
            return best_pidx

        async def _progress_loop() -> None:
            if not progress_callback:
                return
            tick_iv = min(1.0, max(0.25, PROGRESS_UPDATE_INTERVAL * 0.5))
            last_sent = -1
            while not done_ev.is_set():
                await asyncio.sleep(tick_iv)
                cur = min(uploaded[0], file_size)
                if cur != last_sent:
                    last_sent = cur
                    with contextlib.suppress(Exception):
                        res = progress_callback(cur, file_size)
                        if inspect.isawaitable(res):
                            await res

        async def worker(widx: int, slot_idx: int = 0, pace_delay: float = 0.0) -> None:
            if pace_delay > 0 and total_parts >= 12 and not done_ev.is_set():
                await asyncio.sleep(pace_delay)

            while not done_ev.is_set():
                if self.is_cancelled(upload_id):
                    raise asyncio.CancelledError("Upload cancelled")

                wait = flood_until[0] - time.monotonic()
                if wait > 0:
                    await asyncio.sleep(min(wait, 0.5))
                    continue

                hedge = False
                pidx = _next_part()
                if pidx is None:
                    pidx = _pick_tail_hedge(slot_idx)
                    if pidx is None:
                        wake_ev.clear()
                        if done_ev.is_set():
                            return
                        try:
                            await asyncio.wait_for(wake_ev.wait(), timeout=0.04)
                        except asyncio.TimeoutError:
                            pass
                        continue
                    hedge = True

                if done_bitmap[pidx]:
                    continue

                off = pidx * CHUNK_SIZE
                part_len = min(CHUNK_SIZE, file_size - off)
                t_req = time.monotonic()
                rec = inflight.get(pidx)
                if rec is None:
                    rec = [t_req, 1, slot_idx]
                    inflight[pidx] = rec
                else:
                    rec[1] += 1

                try:
                    chunk_data = await reader.read_at(off, part_len)
                    if not chunk_data:
                        raise RuntimeError(f"Unexpected empty read at offset {off}")

                    if is_big:
                        req = SaveBigFilePartRequest(
                            file_id=file_id,
                            file_part=pidx,
                            file_total_parts=total_parts,
                            bytes=chunk_data,
                        )
                    else:
                        req = SaveFilePartRequest(
                            file_id=file_id,
                            file_part=pidx,
                            bytes=chunk_data,
                        )

                    dyn_timeout = max(14.0, min(PART_TIMEOUT, ewma_part[0] * 10.0 + 8.0))
                    ok = await _send_upload_req(
                        pool,
                        req,
                        hint=widx + (5 if hedge else 0),
                        timeout=dyn_timeout,
                        slot_idx=None if hedge else slot_idx,
                        max_slot_inflight=wpc + 1,
                    )
                    if ok is False:
                        raise RuntimeError(f"SaveFilePart returned False for part {pidx}")

                    dt = max(0.01, time.monotonic() - t_req)
                    ewma_part[0] = ewma_part[0] * 0.85 + dt * 0.15

                    if not done_bitmap[pidx]:
                        done_bitmap[pidx] = 1
                        uploaded[0] += len(chunk_data)
                        remaining[0] -= 1
                        if remaining[0] <= 0:
                            done_ev.set()
                            wake_ev.set()
                            return
                except asyncio.CancelledError:
                    if not hedge and not done_bitmap[pidx]:
                        retry_deque.appendleft(pidx)
                        wake_ev.set()
                    raise
                except FloodWaitError as e:
                    if not hedge and not done_bitmap[pidx]:
                        retry_deque.appendleft(pidx)
                        wake_ev.set()
                    if max_flood_wait is not None and e.seconds > max_flood_wait:
                        _fatal(LongFloodWait(e.seconds))
                        return
                    now = time.monotonic()
                    if now >= flood_until[0]:
                        logger.warning(f"Upload FloodWait ({e.seconds}s) — pausing smoothly")
                    flood_until[0] = max(flood_until[0], now + e.seconds + random.uniform(0.3, 1.0))
                except AuthKeyError as e:
                    if not hedge and not done_bitmap[pidx]:
                        retry_deque.appendleft(pidx)
                        wake_ev.set()
                    n = fails[pidx] = fails.get(pidx, 0) + 1
                    if n >= max_retries:
                        _fatal(RuntimeError(f"Upload part {pidx} auth failed {n}× ({e!r})"))
                        return
                    await asyncio.sleep(0.2)
                except Exception as e:
                    if not hedge and not done_bitmap[pidx]:
                        retry_deque.appendleft(pidx)
                        wake_ev.set()
                    if hedge:
                        continue

                    msg = str(e).lower()
                    if "wait of" in msg or "flood" in msg:
                        m = re.search(r"wait of (\d+)", msg)
                        secs = int(m.group(1)) if m else 10
                        if max_flood_wait is not None and secs > max_flood_wait:
                            _fatal(LongFloodWait(secs))
                            return
                        flood_until[0] = max(flood_until[0], time.monotonic() + secs + 1.0)
                        continue

                    n = fails[pidx] = fails.get(pidx, 0) + 1
                    if n >= max_retries:
                        _fatal(RuntimeError(f"Part {pidx} upload failed {n}× ({e!r})"))
                        return
                    await asyncio.sleep(min(0.15 * (2 ** min(n - 1, 4)), 3.0))
                finally:
                    rec[1] -= 1
                    if rec[1] <= 0:
                        inflight.pop(pidx, None)
                    if main_cursor[0] >= total_parts:
                        wake_ev.set()

        def _spawn_for_slot(slot_obj: Any = None) -> None:
            if done_ev.is_set():
                return
            cur = spawned_count[0]
            max_workers = min(total_parts, conns * wpc)
            to_spawn = min(wpc, max(0, max_workers - cur))
            slot_idx = int(getattr(slot_obj, "idx", cur // wpc) or 0)
            for k in range(to_spawn):
                widx = cur + k
                pace = (k * 0.012) + ((slot_idx % 8) * 0.003)
                worker_tasks.append(asyncio.create_task(worker(widx, slot_idx=slot_idx, pace_delay=pace)))
            if to_spawn > 0:
                spawned_count[0] = cur + to_spawn

        done_task = asyncio.create_task(done_ev.wait())
        prog_task = asyncio.create_task(_progress_loop())
        try:
            try:
                await self._start_pool_streaming(pool, size=conns, on_slot_ready=_spawn_for_slot)
            except Exception as e:
                if spawned_count[0] == 0 and use_main:
                    logger.warning(f"Upload pool start failed ({e!r}) — single main connection fallback")
                    await pool.close()
                    pool = SenderPool(client, None, 1, use_main=True)
                    await self._start_pool_streaming(pool, size=1, on_slot_ready=_spawn_for_slot)
                elif spawned_count[0] == 0:
                    raise

            if spawned_count[0] == 0 and getattr(pool, "senders", None):
                _spawn_for_slot()
            if not worker_tasks:
                raise RuntimeError("Upload connection pool started 0 workers")

            while True:
                await asyncio.wait({done_task, fatal}, return_when=asyncio.FIRST_COMPLETED, timeout=0.5)
                if self.is_cancelled(upload_id):
                    raise asyncio.CancelledError("Upload cancelled")
                if fatal.done():
                    fatal.result()
                for t in worker_tasks:
                    if t.done() and not t.cancelled() and t.exception():
                        raise t.exception()
                if done_ev.is_set():
                    break
                if all(t.done() for t in worker_tasks) and not done_ev.is_set():
                    raise RuntimeError("All upload workers exited before completion")

            if uploaded[0] < file_size:
                raise RuntimeError(f"Incomplete upload {uploaded[0]}/{file_size}")

            if progress_callback:
                with contextlib.suppress(Exception):
                    res = progress_callback(file_size, file_size)
                    if inspect.isawaitable(res):
                        await res

            dt_total = max(0.001, time.monotonic() - t_ul_start)
            mb_s = (file_size / 1048576) / dt_total
            logger.info(
                f"⚡ FastUL complete: {file_size / 1048576:.1f}MB ({total_parts} parts) "
                f"in {dt_total:.2f}s ({mb_s:.1f} MB/s)"
            )
            if is_big:
                return InputFileBig(id=file_id, parts=total_parts, name=filename)
            return InputFile(id=file_id, parts=total_parts, name=filename, md5_checksum="")
        finally:
            done_task.cancel()
            prog_task.cancel()
            for t in worker_tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(done_task, prog_task, *worker_tasks, return_exceptions=True)
            await reader.close()
            await pool.close()

    async def upload_file(
        self,
        client: TelegramClient,
        chat_id: Any,
        filepath: str,
        upload_id: str,
        progress_callback: Optional[Callable] = None,
        connections: Optional[int] = None,
        max_flood_wait: Optional[float] = None,
        use_main: bool = True,
        caption: str = "",
        workers_per_conn: Optional[int] = None,
        video_hint: Optional[Dict] = None,
    ):
        self._cancel_flags[upload_id] = False
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File not found: {filepath}")

        filename = os.path.basename(filepath)
        ext = os.path.splitext(filename)[1].lower()
        file_size = os.path.getsize(filepath)

        is_image = ext in IMAGE_EXTS
        is_video = ext in VIDEO_EXTS
        is_audio = ext in AUDIO_EXTS

        logger.info(f"📤 Uploading: {filename} ({file_size / 1048576:.1f}MB)")

        video_info = None
        thumb_path = None
        if is_video:
            # Probe metadata & extract thumbnail in parallel!
            video_info, thumb_path = await asyncio.gather(
                self.get_video_info(filepath, hint=video_hint),
                self.generate_thumbnail(filepath),
            )

        try:
            return await self._do_upload(
                client=client,
                chat_id=chat_id,
                filepath=filepath,
                filename=filename,
                file_size=file_size,
                is_image=is_image,
                is_video=is_video,
                is_audio=is_audio,
                video_info=video_info,
                thumb_path=thumb_path,
                upload_id=upload_id,
                progress_callback=progress_callback,
                connections=connections,
                max_flood_wait=max_flood_wait,
                use_main=use_main,
                caption=caption,
                workers_per_conn=workers_per_conn,
            )
        finally:
            if thumb_path and os.path.exists(thumb_path):
                with contextlib.suppress(OSError):
                    os.remove(thumb_path)

    # Alias for callers expecting .upload(...)
    upload = upload_file

    async def _do_upload(
        self,
        client,
        chat_id: Any,
        filepath: str,
        filename: str,
        file_size: int,
        is_image: bool,
        is_video: bool,
        is_audio: bool,
        video_info: Optional[Dict],
        thumb_path: Optional[str],
        upload_id: str,
        progress_callback: Optional[Callable] = None,
        connections: Optional[int] = None,
        max_flood_wait: Optional[float] = None,
        use_main: bool = True,
        caption: str = "",
        workers_per_conn: Optional[int] = None,
    ):
        uploaded_file = await self._parallel_upload(
            client=client,
            filepath=filepath,
            filename=filename,
            file_size=file_size,
            upload_id=upload_id,
            progress_callback=progress_callback,
            connections=connections,
            max_flood_wait=max_flood_wait,
            use_main=use_main,
            workers_per_conn=workers_per_conn,
        )

        # Upload tiny thumbnail once before the commit loop so retries don't re-upload it
        uploaded_thumb = None
        if is_video and thumb_path and os.path.exists(thumb_path):
            with contextlib.suppress(Exception):
                uploaded_thumb = await client.upload_file(thumb_path)

        max_commit_retries = 4
        for attempt in range(max_commit_retries):
            if self.is_cancelled(upload_id):
                raise asyncio.CancelledError("Upload cancelled")
            try:
                if is_image:
                    send_coro = client.send_file(
                        chat_id, file=uploaded_file, caption=caption, force_document=False
                    )
                elif is_video:
                    vi = video_info or {}
                    attrs = [
                        DocumentAttributeVideo(
                            duration=vi.get("duration", 0),
                            w=vi.get("width", 0),
                            h=vi.get("height", 0),
                            supports_streaming=True,
                        )
                    ]
                    send_coro = client.send_file(
                        chat_id,
                        file=uploaded_file,
                        caption=caption,
                        thumb=uploaded_thumb or thumb_path,
                        attributes=attrs,
                        force_document=False,
                        supports_streaming=True,
                    )
                elif is_audio:
                    attrs = [
                        DocumentAttributeAudio(duration=0),
                        DocumentAttributeFilename(file_name=filename),
                    ]
                    send_coro = client.send_file(
                        chat_id, file=uploaded_file, caption=caption, attributes=attrs, force_document=False
                    )
                else:
                    send_coro = client.send_file(
                        chat_id,
                        file=uploaded_file,
                        caption=caption,
                        attributes=[DocumentAttributeFilename(file_name=filename)],
                        force_document=True,
                    )

                return await asyncio.wait_for(send_coro, timeout=120.0)
            except asyncio.CancelledError:
                raise
            except asyncio.TimeoutError:
                logger.warning("Upload Commit timed out. Retrying...")
            except FloodWaitError as e:
                if max_flood_wait is not None and e.seconds > max_flood_wait:
                    raise LongFloodWait(e.seconds)
                wait = e.seconds + 1
                logger.warning(f"Upload Commit FloodWait: {wait:.1f}s")
                await asyncio.sleep(wait)
                continue
            except Exception as e:
                msg_str = str(e).lower()
                if not client.is_connected() or "size equal" in msg_str or "0 b" in msg_str:
                    logger.warning(f"Connection dead, reconnecting before retry: {e}")
                    try:
                        if client.is_connected():
                            await client.disconnect()
                        await asyncio.sleep(1.5)
                        await client.connect()
                    except Exception as reconnect_err:
                        logger.error(f"Reconnect failed: {reconnect_err}")

                if attempt >= max_commit_retries - 1:
                    raise
                logger.warning(f"Upload Commit error ({attempt + 1}): {e} — retry in 2s")
                await asyncio.sleep(2.0)

        raise RuntimeError(f"Upload failed after {max_commit_retries} attempts")


_uploader: Optional[FastUploader] = None


def get_uploader() -> FastUploader:
    global _uploader
    if _uploader is None:
        _uploader = FastUploader()
    return _uploader


async def init_upload_engine() -> FastUploader:
    ul = get_uploader()
    _get_read_executor()
    logger.info(
        f"⚡ FastUploader Turbo Engine initialized "
        f"(conns={UL_CONNECTIONS}, ub_conns={getattr(_cfg, 'UB_UL_CONNECTIONS', 16)}, "
        f"workers/conn={UL_WORKERS_PER_CONN}, disk_threads={UL_DISK_THREADS})"
    )
    return ul


async def close_upload_engine():
    global _uploader, _READ_POOL
    pool, _READ_POOL = _READ_POOL, None
    if pool is not None:
        pool.shutdown(wait=False)
    _uploader = None


def _probe_with_ffmpeg(path: str) -> Dict:
    """Minimal video-stream probe using `ffmpeg -i` (no ffprobe on the host)."""
    r = subprocess.run([ "ffmpeg", "-hide_banner", "-i", path],
                       capture_output=True, text=True, errors="replace", env=ffmpeg_env())
    for line in r.stderr.splitlines():
        line = line.strip()
        if not (line.startswith("Stream #") and "Video:" in line):
            continue
        codec = re.search(r"Video: (\w+)", line)
        dims = re.findall(r"(\d{2,5})x(\d{2,5})", line)
        if not codec or not dims:
            continue
        w, h = dims[-1]
        stream = {"codec_type": "video", "codec_name": codec.group(1),
                  "width": int(w), "height": int(h)}
        sar = re.search(r"SAR (\d+:\d+)", line)
        if sar:
            stream["sample_aspect_ratio"] = sar.group(1)
        return stream
    return {}
