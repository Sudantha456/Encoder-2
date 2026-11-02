"""FFmpeg runner with live -progress parsing."""
import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger("Transcoder")


@dataclass
class EncState:
    duration: float = 0.0
    src_fps: float = 0.0
    frame: int = 0
    fps: float = 0.0
    speed: float = 0.0
    out_time: float = 0.0
    out_size: int = 0
    started: float = field(default_factory=time.monotonic)
    finished: bool = False

    @property
    def elapsed(self) -> float:
        return time.monotonic() - self.started

    @property
    def percent(self) -> float:
        if self.finished:
            return 100.0
        return min(self.out_time / self.duration * 100, 100.0) if self.duration else 0.0

    @property
    def avg_speed(self) -> float:
        return self.out_time / self.elapsed if self.elapsed > 0 else 0.0

    @property
    def eta(self) -> float:
        sp = self.speed or self.avg_speed
        return (self.duration - self.out_time) / sp if sp > 0 and self.duration else 0.0

    @property
    def est_total_frames(self) -> int:
        return int(self.duration * self.src_fps) if self.duration and self.src_fps else 0

    @property
    def est_final_size(self) -> int:
        return int(self.out_size / self.percent * 100) if self.percent >= 3 else 0


def build_cmd(src, dst, *, target, scale, portrait, preset, codec, crf, abitrate=0,
              maxrate_kbps=0, audio_mode="aac", container="mp4"):
    """audio_mode: aac | copy | none     container: mp4 | mkv"""
    vf = []
    if scale:
        vf.append(f"scale={target}:-2" if portrait else f"scale=-2:{target}")
    vf.append("format=yuv420p")
    cmd = ["ffmpeg", "-hide_banner", "-nostdin", "-y", "-i", src, "-map", "0:v:0"]
    if audio_mode != "none":
        cmd += ["-map", "0:a?"]
    cmd += ["-sn", "-dn", "-vf", ",".join(vf), "-c:v", codec, "-preset", preset, "-crf", str(crf)]
    if maxrate_kbps:
        cmd += ["-maxrate", f"{maxrate_kbps}k", "-bufsize", f"{maxrate_kbps * 2}k"]
    if codec == "libx265":
        cmd += ["-x265-params", "log-level=error"]
        if container == "mp4":
            cmd += ["-tag:v", "hvc1"]
    if audio_mode == "none":
        cmd += ["-an"]
    elif audio_mode == "copy":
        cmd += ["-c:a", "copy"]
    else:
        cmd += ["-c:a", "aac", "-b:a", f"{abitrate}k"]
    cmd += ["-max_muxing_queue_size", "1024"]
    if container == "mp4":
        cmd += ["-movflags", "+faststart"]
    cmd += ["-progress", "pipe:1", "-nostats", dst]
    return cmd


def _num(v, cast=float):
    try:
        return cast(v)
    except Exception:
        return 0


async def transcode(src: str, dst: str, state: EncState, job, **opts):
    """job.proc / job.cancelled භාවිතා කරනවා (cancel කිරීමට)."""
    cmd = build_cmd(src, dst, **opts)
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    job.proc = proc
    err_tail = deque(maxlen=12)

    async def drain():
        async for line in proc.stderr:
            err_tail.append(line.decode(errors="ignore").rstrip())

    drain_task = asyncio.create_task(drain())
    state.started = time.monotonic()
    try:
        while True:
            raw = await proc.stdout.readline()
            if not raw:
                break
            k, _, v = raw.decode(errors="ignore").strip().partition("=")
            if k == "frame":
                state.frame = _num(v, int)
            elif k == "fps":
                state.fps = _num(v)
            elif k == "speed":
                state.speed = _num(v.strip().rstrip("x"))
            elif k in ("out_time_us", "out_time_ms"):
                t = _num(v, int)
                if t > 0:
                    state.out_time = t / 1e6
            elif k == "total_size":
                state.out_size = _num(v, int)
            elif k == "progress" and v == "end":
                state.finished = True
        rc = await proc.wait()
        await drain_task
    finally:
        if proc.returncode is None:
            proc.kill()
        job.proc = None

    if job.cancelled:
        raise asyncio.CancelledError("cancelled")
    if rc != 0:
        raise RuntimeError("FFmpeg failed:\n" + "\n".join(err_tail))
