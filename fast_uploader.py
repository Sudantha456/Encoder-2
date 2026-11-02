"""
fast_uploader.py — Telethon CONTROLLED PARALLEL Uploader (Anti-Freeze Edition)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Silent Network Hangs (Freezes) වළක්වා ගැනීමට asyncio.wait_for Timeouts ඇතුළත් කර ඇත.
"""
import os
import asyncio
import time
import json
import logging
from typing import Optional, Callable, Dict

from telethon import TelegramClient
from telethon.tl.types import (
    DocumentAttributeVideo, DocumentAttributeAudio,
    DocumentAttributeFilename, InputFileBig, InputFile
)
from telethon.tl.functions.upload import SaveBigFilePartRequest, SaveFilePartRequest
from telethon.errors import FloodWaitError
from telethon.helpers import generate_random_long

import config as _cfg
from config import PROGRESS_UPDATE_INTERVAL
from fast_conn import SenderPool

logger = logging.getLogger("FastUL")

VIDEO_EXTS = {
    ".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv",
    ".wmv", ".m4v", ".3gp", ".ts", ".m2ts", ".mpg",
    ".mpeg", ".divx", ".xvid", ".rm", ".rmvb",
}
AUDIO_EXTS = {
    ".mp3", ".flac", ".ogg", ".m4a", ".aac", ".wav",
    ".opus", ".wma", ".aiff", ".ape",
}
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp"}

CHUNK_SIZE = 512 * 1024  
UL_CONNECTIONS = getattr(_cfg, "UL_CONNECTIONS", 6)
UL_WORKERS_PER_CONN = getattr(_cfg, "UL_WORKERS_PER_CONN", 4)

class FastUploader:

    def __init__(self):
        self._cancel_flags: Dict[str, bool] = {}

    def cancel(self, upload_id: str):
        self._cancel_flags[upload_id] = True

    def is_cancelled(self, upload_id: str) -> bool:
        return self._cancel_flags.get(upload_id, False)

    async def cleanup(self, upload_id: str):
        self._cancel_flags.pop(upload_id, None)

    async def get_video_info(self, filepath: str) -> Dict:
        cmd = [
            "ffprobe", "-v", "quiet", "-print_format", "json",
            "-show_format", "-show_streams", filepath,
        ]
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            # Timeout included for subprocess
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=30.0)
            info = json.loads(stdout.decode())
            video_stream = next(
                (s for s in info.get("streams", []) if s.get("codec_type") == "video"), None
            )
            duration = float(info.get("format", {}).get("duration", 0))
            width    = int(video_stream.get("width",  0)) if video_stream else 0
            height   = int(video_stream.get("height", 0)) if video_stream else 0
            return {
                "duration": int(duration),
                "width":    width,
                "height":   height,
            }
        except Exception as e:
            logger.warning(f"ffprobe failed: {e}")
            return {"duration": 0, "width": 0, "height": 0}

    async def generate_thumbnail(self, filepath: str) -> Optional[str]:
        thumb_path = filepath + "_thumb.jpg"
        for ts in ["00:00:03", "00:00:01", "00:00:00"]:
            cmd = [
                "ffmpeg", "-y", "-i", filepath,
                "-ss", ts, "-vframes", "1",
                "-vf", "scale='min(320,iw)':-2",
                "-q:v", "4", thumb_path,
            ]
            try:
                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                await asyncio.wait_for(proc.communicate(), timeout=30.0)
                if os.path.exists(thumb_path) and os.path.getsize(thumb_path) > 100:
                    return thumb_path
            except Exception:
                pass
        return None

    async def _parallel_upload(self, client: TelegramClient, filepath: str, filename: str, file_size: int, upload_id: str, progress_cb: Callable):
        if file_size == 0:
            return filepath

        file_id = generate_random_long()
        is_big = file_size > 10 * 1024 * 1024
        total_parts = (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE

        # Multi-connection pool (home DC). Fail වුණොත් main client එක විතරක්.
        pool = SenderPool(client, None, UL_CONNECTIONS)
        try:
            await asyncio.wait_for(pool.start(), timeout=60.0)
        except Exception as e:
            logger.warning(f"Pool start failed ({e!r}) — single connection")
            await pool.close()
            pool = SenderPool(client, None, 1)
            await pool.start()

        uploaded_bytes = 0
        queue: asyncio.Queue = asyncio.Queue()
        for i in range(total_parts):
            queue.put_nowait(i)

        async def worker(w: int):
            nonlocal uploaded_bytes
            with open(filepath, "rb") as f:
                while True:
                    if self.is_cancelled(upload_id):
                        raise asyncio.CancelledError("Upload cancelled")
                    try:
                        part_idx = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        return
                    f.seek(part_idx * CHUNK_SIZE)
                    chunk_data = f.read(CHUNK_SIZE)

                    for attempt in range(6):
                        if self.is_cancelled(upload_id):
                            raise asyncio.CancelledError("Upload cancelled")
                        try:
                            if is_big:
                                req = SaveBigFilePartRequest(file_id=file_id, file_part=part_idx,
                                                             file_total_parts=total_parts, bytes=chunk_data)
                            else:
                                req = SaveFilePartRequest(file_id=file_id, file_part=part_idx, bytes=chunk_data)
                            await asyncio.wait_for(pool.send(req, w + attempt), timeout=40.0)
                            uploaded_bytes += len(chunk_data)
                            progress_cb(min(uploaded_bytes, file_size), file_size)
                            break
                        except asyncio.CancelledError:
                            raise
                        except FloodWaitError as e:
                            logger.warning(f"Upload FloodWait ({e.seconds}s)")
                            await asyncio.sleep(e.seconds + 1)
                        except asyncio.TimeoutError:
                            logger.warning(f"Part {part_idx} timed out (attempt {attempt+1})")
                            if attempt == 5:
                                raise RuntimeError(f"Part {part_idx} failed after 6 timeouts")
                            await asyncio.sleep(min(2 ** attempt, 8))
                        except Exception as e:
                            if attempt == 5:
                                logger.error(f"Failed to upload part {part_idx}: {e}")
                                raise
                            await asyncio.sleep(min(2 ** attempt, 8))

        n_workers = min(len(pool.senders) * UL_WORKERS_PER_CONN, total_parts)
        tasks = [asyncio.create_task(worker(i)) for i in range(n_workers)]
        try:
            await asyncio.gather(*tasks)
        except BaseException:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        finally:
            await pool.close()

        if is_big:
            return InputFileBig(id=file_id, parts=total_parts, name=filename)
        return InputFile(id=file_id, parts=total_parts, name=filename, md5_checksum="")

    async def upload_file(
        self,
        client:            TelegramClient,
        chat_id:           int,
        filepath:          str,
        upload_id:         str,
        progress_callback: Optional[Callable] = None,
    ):
        self._cancel_flags[upload_id] = False

        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File not found: {filepath}")

        filename  = os.path.basename(filepath)
        ext       = os.path.splitext(filename)[1].lower()
        file_size = os.path.getsize(filepath)

        is_image = ext in IMAGE_EXTS
        is_video = ext in VIDEO_EXTS
        is_audio = ext in AUDIO_EXTS

        logger.info(f"📤 Uploading: {filename} ({file_size/1024/1024:.1f}MB)")

        video_info = None
        thumb_path = None
        if is_video:
            video_info = await self.get_video_info(filepath)
            thumb_path = await self.generate_thumbnail(filepath)

        _last_update = [0.0]

        def _prog(current: int, total: int):
            if self.is_cancelled(upload_id):
                raise asyncio.CancelledError("Upload cancelled")
            now = time.monotonic()
            if now - _last_update[0] < PROGRESS_UPDATE_INTERVAL and current < total:
                return
            _last_update[0] = now
            if progress_callback:
                asyncio.create_task(progress_callback(current, total))

        try:
            msg = await self._do_upload(
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
                progress_cb=_prog,
            )
            return msg
        finally:
            if thumb_path and os.path.exists(thumb_path):
                try:
                    os.remove(thumb_path)
                except Exception:
                    pass

    async def _do_upload(
        self,
        client,
        chat_id:    int,
        filepath:   str,
        filename:   str,
        file_size:  int,
        is_image:   bool,
        is_video:   bool,
        is_audio:   bool,
        video_info: Optional[Dict],
        thumb_path: Optional[str],
        upload_id:  str,
        progress_cb: Callable,
    ):
        
        uploaded_file = await self._parallel_upload(client, filepath, filename, file_size, upload_id, progress_cb)

        max_retries = 3
        for attempt in range(max_retries):
            if self.is_cancelled(upload_id):
                raise asyncio.CancelledError("Upload cancelled")
            try:
                # Commit Phase Timeout 
                send_coro = None
                if is_image:
                    send_coro = client.send_file(chat_id, file=uploaded_file, caption="", force_document=False)
                elif is_video:
                    vi = video_info or {}
                    attrs = [DocumentAttributeVideo(duration=vi.get("duration", 0), w=vi.get("width", 0), h=vi.get("height", 0), supports_streaming=True)]
                    send_coro = client.send_file(chat_id, file=uploaded_file, caption="", thumb=thumb_path, attributes=attrs, force_document=False, supports_streaming=True)
                elif is_audio:
                    attrs = [DocumentAttributeAudio(duration=0), DocumentAttributeFilename(file_name=filename)]
                    send_coro = client.send_file(chat_id, file=uploaded_file, caption="", attributes=attrs, force_document=False)
                else:
                    send_coro = client.send_file(chat_id, file=uploaded_file, caption="", attributes=[DocumentAttributeFilename(file_name=filename)], force_document=True)

                # Send File Commit Timeout (2 Mins maximum)
                return await asyncio.wait_for(send_coro, timeout=120.0)

            except asyncio.CancelledError:
                raise
            except asyncio.TimeoutError:
                logger.warning(f"Upload Commit Timed out. Retrying...")
            except FloodWaitError as e:
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
                        await asyncio.sleep(2)
                        await client.connect()
                    except Exception as reconnect_err:
                        logger.error(f"Reconnect failed: {reconnect_err}")

                if attempt >= max_retries - 1:
                    raise
                logger.warning(f"Upload Commit error ({attempt+1}): {e} — retry in 3s")
                await asyncio.sleep(3)

        raise RuntimeError(f"Upload failed after {max_retries} attempts")


_uploader: Optional[FastUploader] = None

def get_uploader() -> FastUploader:
    global _uploader
    if _uploader is None:
        _uploader = FastUploader()
    return _uploader

