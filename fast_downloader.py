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
from typing import Optional, Callable, Dict

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
SMALL_FILE = 4 * 1024 * 1024                               # මීට කුඩා නම් native


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
