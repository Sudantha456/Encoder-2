"""Metadata: Telegram attrs + ffprobe (head/tail partial probe), size estimation."""
import asyncio
import json
import logging
import os
import tempfile
from dataclasses import dataclass
from typing import Optional

from telethon.tl.types import DocumentAttributeVideo, DocumentAttributeFilename

from config import (RESOLUTIONS, AUDIO_BITRATE, PROBE_HEAD_CHUNKS, PROBE_TAIL_CHUNKS,
                    SIZE_GUARD, SIZE_GUARD_RATIO, MIN_GUARD_KBPS)
from ui import human_size, human_size_short, human_time, esc

logger = logging.getLogger("Media")

DC_NAMES = {1: "Miami", 2: "Amsterdam", 3: "Miami", 4: "Amsterdam", 5: "Singapore"}
CODEC_NAMES = {
    "h264": "H.264 / AVC", "hevc": "H.265 / HEVC", "vp9": "VP9", "vp8": "VP8",
    "av1": "AV1", "mpeg4": "MPEG-4", "mpeg2video": "MPEG-2", "wmv3": "WMV",
}
CHUNK = 512 * 1024


@dataclass
class VideoMeta:
    name: str = "video.mp4"
    size: int = 0
    mime: str = ""
    dc: Optional[int] = None
    duration: float = 0.0
    width: int = 0
    height: int = 0
    fps: float = 0.0
    vcodec: str = ""
    vprofile: str = ""
    vbitrate: int = 0
    acodec: str = ""
    achannels: int = 0
    abitrate: int = 0
    container: str = ""
    probed: bool = False


# ───────────────────────── Telegram attrs ─────────────────────────
def telegram_meta(message) -> VideoMeta:
    doc = message.document
    m = VideoMeta(size=doc.size, mime=doc.mime_type or "", dc=getattr(doc, "dc_id", None))
    m.name = f"video_{message.id}.mp4"
    for a in doc.attributes:
        if isinstance(a, DocumentAttributeFilename):
            m.name = a.file_name
        elif isinstance(a, DocumentAttributeVideo):
            m.duration, m.width, m.height = float(a.duration), a.w, a.h
    m.container = (os.path.splitext(m.name)[1].lstrip(".") or m.mime.split("/")[-1]).lower()
    return m


# ───────────────────────── ffprobe ─────────────────────────
async def ffprobe(path: str, timeout: float = 30.0) -> dict:
    proc = await asyncio.create_subprocess_exec(
        "ffprobe", "-v", "error", "-print_format", "json",
        "-show_format", "-show_streams", path,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    try:
        out, _ = await asyncio.wait_for(proc.communicate(), timeout)
    except Exception:
        proc.kill()
        raise
    return json.loads(out.decode() or "{}")


def _fps(v: dict) -> float:
    for key in ("avg_frame_rate", "r_frame_rate"):
        try:
            n, d = v.get(key, "0/0").split("/")
            if float(d) and float(n):
                return float(n) / float(d)
        except Exception:
            pass
    return 0.0


def apply_probe(m: VideoMeta, info: dict) -> bool:
    """ffprobe JSON → meta. Video stream නැත්නම් False."""
    streams = info.get("streams", [])
    v = next((s for s in streams if s.get("codec_type") == "video"
              and not s.get("disposition", {}).get("attached_pic")), None)
    a = next((s for s in streams if s.get("codec_type") == "audio"), None)
    if not v:
        return False
    fmt = info.get("format", {})

    w, h = int(v.get("width", 0)), int(v.get("height", 0))
    rot = 0
    try:
        rot = int(float(v.get("tags", {}).get("rotate", 0)))
    except Exception:
        pass
    for sd in v.get("side_data_list", []):
        if "rotation" in sd:
            rot = int(float(sd["rotation"]))
    if abs(rot) % 180 == 90:
        w, h = h, w
    if w and h:
        m.width, m.height = w, h

    try:
        dur = float(fmt.get("duration") or v.get("duration") or 0)
        if dur > 0:
            m.duration = dur
    except Exception:
        pass
    m.fps = _fps(v) or m.fps
    m.vcodec = v.get("codec_name", "") or m.vcodec
    m.vprofile = v.get("profile", "") or ""
    try:
        m.vbitrate = int(v.get("bit_rate") or 0)
    except Exception:
        m.vbitrate = 0
    if not m.vbitrate:
        try:
            tot = int(fmt.get("bit_rate") or 0)
            m.vbitrate = max(tot - (int(a.get("bit_rate") or 0) if a else 0), 0)
        except Exception:
            pass
    if a:
        m.acodec = a.get("codec_name", "")
        m.achannels = int(a.get("channels", 0) or 0)
        try:
            m.abitrate = int(a.get("bit_rate") or 0)
        except Exception:
            m.abitrate = 0
    else:
        m.acodec, m.achannels, m.abitrate = "", 0, 0
    fn = fmt.get("format_name", "")
    if fn:
        m.container = fn.split(",")[0]
    m.probed = True
    return True


async def probe_remote(client, message, meta: VideoMeta, workdir: str) -> bool:
    """
    Download නොකර metadata ගන්නවා: file එකේ head + tail පමණක් අරන් sparse file එකක
    හදලා ffprobe කරනවා (moov atom අගට තියෙන mp4 වලටත් වැඩ කරනවා).
    """
    path = os.path.join(workdir, "probe.bin")
    size = meta.size
    try:
        async def _fetch():
            with open(path, "wb") as f:
                f.truncate(size)
                head_bytes = PROBE_HEAD_CHUNKS * CHUNK
                tail_bytes = PROBE_TAIL_CHUNKS * CHUNK
                if size <= head_bytes + tail_bytes:
                    ranges = [(0, None)]
                else:
                    tail_off = ((size - tail_bytes) // CHUNK) * CHUNK
                    ranges = [(0, PROBE_HEAD_CHUNKS), (tail_off, None)]
                for off, limit in ranges:
                    pos = off
                    async for chunk in client.iter_download(
                        message.media, offset=off, request_size=CHUNK, limit=limit
                    ):
                        f.seek(pos)
                        f.write(chunk)
                        pos += len(chunk)
        await asyncio.wait_for(_fetch(), timeout=45.0)
        info = await ffprobe(path, timeout=20.0)
        return apply_probe(meta, info)
    except Exception as e:
        logger.warning(f"remote probe failed: {e}")
        return False
    finally:
        try:
            os.remove(path)
        except OSError:
            pass


# ───────────────────────── resolution + size estimate ─────────────────────────
PRESET_FACTOR = {"ultrafast": 1.55, "superfast": 1.35, "veryfast": 1.2, "faster": 1.1,
                 "fast": 1.05, "medium": 1.0, "slow": 0.95, "slower": 0.9, "veryslow": 0.87}
CODEC_FACTOR = {"libx264": 1.0, "libx265": 0.62}
RES_BPP = {720: 1.0, 480: 1.2, 240: 1.5}
BASE_BPP = 0.075   # bits / pixel / frame  (x264, CRF 23, medium)


def short_side(m: VideoMeta) -> int:
    return min(m.width, m.height)


def res_label(m: VideoMeta) -> str:
    s = short_side(m)
    if not s:
        return "—"
    for lim, lab in ((2160, "4K"), (1440, "1440p"), (1080, "1080p"), (720, "720p"), (480, "480p"), (360, "360p")):
        if s >= lim * 0.9:
            return lab
    return f"{s}p"


def offered_targets(m: VideoMeta):
    s = short_side(m)
    t = [r for r in RESOLUTIONS if s >= r * 0.9]
    return t or [RESOLUTIONS[-1]]


def target_dims(m: VideoMeta, t: int):
    w, h = m.width, m.height
    s = min(w, h)
    if s <= t or not s:
        return w - w % 2, h - h % 2
    r = t / s
    return int(round(w * r / 2) * 2), int(round(h * r / 2) * 2)


def guard_kbps(m: VideoMeta, t: int) -> int:
    """Source video bitrate (resolution අනුව scale කරපු) — 0 නම් guard නෑ."""
    if not SIZE_GUARD:
        return 0
    dur = m.duration
    src_v = m.vbitrate or (max(m.size * 8 / dur - (m.abitrate or 128_000), 0) if dur > 0 else 0)
    if src_v <= 0 or not m.width:
        return 0
    tw, th = target_dims(m, t)
    ratio = (tw * th) / max(m.width * m.height, 1)
    return max(int(src_v * (ratio ** 0.75) * SIZE_GUARD_RATIO / 1000), MIN_GUARD_KBPS)


def estimate_size(m: VideoMeta, t: int, preset: str, codec: str) -> int:
    dur = m.duration
    if dur <= 0 or not m.width:
        return 0
    tw, th = target_dims(m, t)
    fps = min(m.fps or 30, 60)
    bpp = BASE_BPP * RES_BPP.get(t, 1.0) * PRESET_FACTOR.get(preset, 1.0) * CODEC_FACTOR.get(codec, 1.0)
    vbps = tw * th * fps * bpp

    g = guard_kbps(m, t)
    if g:
        vbps = min(vbps, g * 1000)

    abps = AUDIO_BITRATE.get(t, 96) * 1000
    if m.probed and not m.acodec:
        abps = 0
    elif m.abitrate:
        abps = min(abps, m.abitrate)
    return int((vbps + abps) * dur / 8 * 1.01)


# ───────────────────────── preview card ─────────────────────────
def render_preview(m: VideoMeta, preset: str, codec: str) -> str:
    unk = "—"
    codec_txt = CODEC_NAMES.get(m.vcodec, m.vcodec.upper()) if m.vcodec else "unknown (download පසු පෙන්වයි)"
    if m.vprofile:
        codec_txt += f" ({m.vprofile})"
    res_txt = f"{m.width}×{m.height} • {res_label(m)}" if m.width else unk
    if m.fps:
        res_txt += f" • {m.fps:.2f} fps".replace(".00", "")
    br = f" • 🎚 {m.vbitrate / 1e6:.2f} Mbps" if m.vbitrate else ""
    if m.acodec:
        ch = {1: "mono", 2: "stereo"}.get(m.achannels, f"{m.achannels}ch" if m.achannels else "")
        ab = f" • {m.abitrate // 1000} kbps" if m.abitrate else ""
        audio = f"{m.acodec}{' • ' + ch if ch else ''}{ab}"
    else:
        audio = "none" if m.probed else unk
    dc = f"DC{m.dc} ({DC_NAMES.get(m.dc, '?')})" if m.dc else unk

    return "\n".join([
        f"🎬 <b>{esc(m.name)}</b>",
        "━━━━━━━━━━━━━━━━━━━━",
        f"🎞 Codec: <code>{esc(codec_txt)}</code>",
        f"📐 Resolution: <code>{esc(res_txt)}</code>",
        f"⏱ Duration: <code>{human_time(m.duration) if m.duration else unk}</code>",
        f"📦 Size: <code>{human_size(m.size)}</code>{br}",
        f"🔊 Audio: <code>{esc(audio)}</code>",
        f"📁 Container: <code>{esc(m.container or unk)}</code>",
        f"🌐 Saved DC: <code>{esc(dc)}</code>",
        "━━━━━━━━━━━━━━━━━━━━",
        f"⚙️ Preset: <b>{preset}</b> • Codec: <b>{codec}</b>",
        "👇 <b>Output resolution එක තෝරන්න</b> (size එක ආසන්න ≈ අගයක්)",
    ])
