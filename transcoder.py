"""FFmpeg runner with live -progress parsing (stdlib-only — runs on main & GitHub workers)."""
import asyncio
import contextlib
import logging
import subprocess
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional, Set

from ffenv import ffmpeg_env

logger = logging.getLogger("Transcoder")

_X265_PIX_FMTS_CACHE: Optional[Set[str]] = None


def get_x265_supported_pix_fmts() -> Set[str]:
    """Detect pixel formats supported by the installed ffmpeg's libx265 encoder (cached)."""
    global _X265_PIX_FMTS_CACHE
    if _X265_PIX_FMTS_CACHE is not None:
        return _X265_PIX_FMTS_CACHE
    sup: Set[str] = set()
    try:
        r = subprocess.run(
            ["ffmpeg", "-hide_banner", "-h", "encoder=libx265"],
            capture_output=True,
            text=True,
            timeout=10,
            env=ffmpeg_env(),
        )
        for line in (r.stdout or "").splitlines():
            if "Supported pixel formats:" in line:
                sup.update(line.split(":", 1)[1].split())
    except Exception:
        pass
    _X265_PIX_FMTS_CACHE = sup
    return sup


def resolve_x265_pix_fmt(requested: str, supported: Optional[Set[str]] = None) -> str:
    """
    Map requested pixel format to the closest valid libx265 output pixel format.
    • 8/10/12-bit formats (yuv420p..yuv444p12le) map 1:1 on standard multilib libx265 builds.
    • 16-bit formats (yuv420p16le..yuv444p16le) keep 16-bit precision in -vf format=... and
      encode at the highest supported HEVC bit-depth (12-bit Main12 / 4:2:2 12 / 4:4:4 12).
    """
    req = (requested or "yuv420p").strip()
    sup = get_x265_supported_pix_fmts() if supported is None else supported
    if not sup:
        if req.endswith("16le"):
            return req[:-4] + "12le"
        return req
    if req in sup:
        return req
    chroma = "444" if "444" in req else ("422" if "422" in req else "420")
    for cand in (f"yuv{chroma}p12le", f"yuv{chroma}p10le", f"yuv{chroma}p", "yuv420p10le", "yuv420p"):
        if cand in sup:
            return cand
    return "yuv420p"


def build_x265_extra(
    *,
    bframes: Optional[bool] = None,
    sao: Optional[bool] = None,
    lookahead: Optional[bool] = None,
    aq: Optional[bool] = None,
) -> str:
    """Build safe colon-separated x265 tuning parameters (respecting rc-lookahead > bframes)."""
    parts = []
    bf_count: Optional[int] = None
    if bframes is not None:
        bf_count = 4 if bframes else 0
        parts.append(f"bframes={bf_count}:b-adapt={1 if bframes else 0}")
    if sao is not None:
        parts.append("sao=1" if sao else "no-sao=1")
    if lookahead is not None:
        if lookahead:
            parts.append("rc-lookahead=25:cutree=1")
        else:
            # x265 requires lookahead depth > consecutive bframes count
            min_la = (bf_count + 1) if (bf_count is not None and bf_count > 0) else (0 if bf_count == 0 else 9)
            parts.append(f"rc-lookahead={min_la}:no-cutree=1")
    if aq is not None:
        parts.append("aq-mode=2:aq-strength=1.0" if aq else "aq-mode=0")
    return ":".join(parts)


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


def build_cmd(
    src,
    dst,
    *,
    target=0,
    scale=False,
    portrait=False,
    preset="ultrafast",
    codec="libx264",
    crf=23,
    abitrate=0,
    maxrate_kbps=0,
    audio_mode="aac",
    container="mp4",
    pix_fmt="yuv420p",
    x265_extra="",
):
    """audio_mode: aac | copy | none     container: mp4 | mkv"""
    req_pix = (pix_fmt or "yuv420p").strip()
    vf = []
    if scale and target:
        vf.append(f"scale={target}:-2" if portrait else f"scale=-2:{target}")
    elif "444" not in req_pix:
        # Ensure even dimensions for 4:2:0 / 4:2:2 chroma subsampling without altering even resolutions
        vf.append("scale=trunc(iw/2)*2:trunc(ih/2)*2")
    vf.append(f"format={req_pix if codec == 'libx265' else 'yuv420p'}")

    enc_pix = resolve_x265_pix_fmt(req_pix) if codec == "libx265" else "yuv420p"

    cmd = ["ffmpeg", "-hide_banner", "-nostdin", "-y", "-i", src, "-map", "0:v:0"]
    if audio_mode != "none":
        cmd += ["-map", "0:a?"]
    cmd += [
        "-sn", "-dn",
        "-vf", ",".join(vf),
        "-c:v", codec,
        "-pix_fmt", enc_pix,
        "-preset", preset,
        "-crf", str(crf),
    ]
    if maxrate_kbps:
        cmd += ["-maxrate", f"{maxrate_kbps}k", "-bufsize", f"{maxrate_kbps * 2}k"]
    if codec == "libx265":
        xp = f"log-level=error:{x265_extra}" if x265_extra else "log-level=error"
        cmd += ["-x265-params", xp]
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


async def verify_output(path: str, job):
    """Decode the entire output before upload; file size alone cannot prove integrity."""
    if job.cancelled:
        raise asyncio.CancelledError("cancelled")
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg", "-hide_banner", "-v", "error", "-nostdin", "-xerror",
        "-err_detect", "explode", "-i", path, "-map", "0:v:0", "-map", "0:a?",
        "-f", "null", "-", stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE, env=ffmpeg_env(),
    )
    job.proc = proc
    try:
        _, err = await proc.communicate()
    finally:
        if proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
            await proc.wait()
        job.proc = None
    if job.cancelled:
        raise asyncio.CancelledError("cancelled")
    if proc.returncode:
        raise RuntimeError("Encoded video failed playback verification: " +
                           err.decode(errors="replace")[-600:].strip())


def _num(v, cast=float):
    try:
        return cast(v)
    except Exception:
        return 0


async def transcode(src: str, dst: str, state: EncState, job, **opts):
    """job.proc / job.cancelled භාවිතා කරනවා (cancel කිරීමට)."""
    cmd = build_cmd(src, dst, **opts)
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        env=ffmpeg_env()
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
