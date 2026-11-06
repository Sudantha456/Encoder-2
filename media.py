"""Metadata: Telegram attrs + ffprobe (head/tail partial probe), size estimation."""
import asyncio
import contextlib
import json
import logging
import os
from dataclasses import dataclass
from typing import Optional

from telethon.tl.types import DocumentAttributeVideo, DocumentAttributeFilename

from config import (RESOLUTIONS, AUDIO_BITRATE, PROBE_HEAD_CHUNKS, PROBE_TAIL_CHUNKS,
                    PROBE_CONNECTIONS, PROBE_TIMEOUT, PROBE_CACHE, PROBE_CACHE_FILE,
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


# ───────────────────────── ⚡ fast metadata (probe) ─────────────────────────
# Preview එක instant කරන්න:
#   • head + tail chunks හැම එකක්ම connections කිහිපයක් හරහා PARALLEL ගන්නවා (sequential නෑ)
#   • එකම video එක ආයෙ එව්වොත් cached metadata එකෙන් ක්ෂණිකව (network නෑ)
_cache: Optional[dict] = None


def _cache_load() -> dict:
    global _cache
    if _cache is not None:
        return _cache
    try:
        with open(PROBE_CACHE_FILE, "r", encoding="utf-8") as f:
            _cache = json.load(f) or {}
    except Exception:
        _cache = {}
    return _cache


def _cache_save() -> None:
    try:
        tmp = PROBE_CACHE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_cache or {}, f, separators=(",", ":"))
        os.replace(tmp, PROBE_CACHE_FILE)
    except Exception as e:
        logger.debug(f"probe cache save failed: {e}")


def _doc_key(message) -> str:
    doc = getattr(message, "document", None)
    if not doc:
        return ""
    return f"{doc.id}:{getattr(doc, 'size', 0)}"


CACHE_FIELDS = ("width", "height", "duration", "fps", "vcodec", "vprofile", "vbitrate",
                "acodec", "achannels", "abitrate", "container")


def cache_get(message) -> Optional[dict]:
    if not PROBE_CACHE:
        return None
    key = _doc_key(message)
    return _cache_load().get(key) if key else None


def cache_put(message, m: VideoMeta) -> None:
    if not PROBE_CACHE:
        return
    key = _doc_key(message)
    if not key:
        return
    data = _cache_load()
    data[key] = {f: getattr(m, f) for f in CACHE_FIELDS}
    if len(data) > 400:                      # පරණ ඒවා අයින් (LRU-ish)
        for k in list(data)[:len(data) - 400]:
            data.pop(k, None)
    _cache_save()


def apply_cached(m: VideoMeta, data: dict) -> bool:
    if not data or not data.get("width"):
        return False
    for f in CACHE_FIELDS:
        if f in data:
            setattr(m, f, data[f])
    m.probed = True
    return True


def _probe_offsets(size: int):
    """(offset, length) — head + tail chunks (512KB aligned, parallel)."""
    head_bytes = PROBE_HEAD_CHUNKS * CHUNK
    tail_bytes = PROBE_TAIL_CHUNKS * CHUNK
    if size <= head_bytes + tail_bytes:
        offs = [(off, min(CHUNK, size - off)) for off in range(0, size, CHUNK)]
        return offs
    tail_off = ((size - tail_bytes) // CHUNK) * CHUNK
    offs = [(off, CHUNK) for off in range(0, head_bytes, CHUNK)]
    offs += [(off, min(CHUNK, size - off)) for off in range(tail_off, size, CHUNK)]
    return [(o, n) for o, n in offs if n > 0]


async def _probe_fetch(client, message, path: str, size: int) -> None:
    """head/tail chunks ඔක්කොම එකවර (multi-connection) — 3–8× වේගවත් sequential එකට වඩා."""
    doc = getattr(message, "document", None)
    offs = _probe_offsets(size)
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_TRUNC | getattr(os, "O_BINARY", 0))
    os.ftruncate(fd, size)
    has_pwrite = hasattr(os, "pwrite")
    write_lock = asyncio.Lock()

    async def write_at(off: int, data: bytes):
        if has_pwrite:
            os.pwrite(fd, data, off)
        else:
            async with write_lock:
                os.lseek(fd, off, os.SEEK_SET)
                os.write(fd, data)

    pool = None
    try:
        dc = getattr(doc, "dc_id", None) if doc else None
        home = getattr(getattr(client, "session", None), "dc_id", None)
        if doc and dc in (None, home):        # same DC → extra connections සුරක්ෂිතයි
            from fast_conn import SenderPool
            from telethon.tl.functions.upload import GetFileRequest
            from telethon.tl.types import InputDocumentFileLocation
            loc = InputDocumentFileLocation(id=doc.id, access_hash=doc.access_hash,
                                            file_reference=doc.file_reference, thumb_size="")
            pool = SenderPool(client, dc, max(2, min(PROBE_CONNECTIONS, len(offs))))
            await asyncio.wait_for(pool.start(), timeout=12.0)

            async def one(i: int, off: int, n: int):
                req = GetFileRequest(location=loc, offset=off, limit=n)
                r = await asyncio.wait_for(pool.send(req, i), timeout=20.0)
                data = getattr(r, "bytes", None)
                if data:
                    await write_at(off, data)

            await asyncio.gather(*[one(i, o, n) for i, (o, n) in enumerate(offs)])
            return

        # වෙන DC එකක් / pool fail → Telethon ගේ native path (head + tail එකවර)
        head = [(o, n) for o, n in offs if o < PROBE_HEAD_CHUNKS * CHUNK]
        tail = [(o, n) for o, n in offs if o >= PROBE_HEAD_CHUNKS * CHUNK]

        async def native(group):
            for off, n in group:
                pos = off
                limit = max(1, (n + CHUNK - 1) // CHUNK)
                async for chunk in client.iter_download(message.media, offset=off,
                                                        request_size=CHUNK, limit=limit):
                    await write_at(pos, chunk[:CHUNK])
                    pos += len(chunk)
                    if pos >= off + n:
                        break

        await asyncio.gather(native(head), native(tail))
    finally:
        with contextlib.suppress(Exception):
            os.close(fd)
        if pool is not None:
            # close එක hang වුණොත් preview එක කවදාවත් එන්නේ නෑ → timeout
            with contextlib.suppress(BaseException):
                await asyncio.wait_for(pool.close(), timeout=10)


async def probe_remote(client, message, meta: VideoMeta, workdir: str,
                       timeout: Optional[float] = None) -> bool:
    """
    Download නොකර metadata ගන්නවා: file එකේ head + tail පමණක් (parallel) අරන් sparse file
    එකක හදලා ffprobe කරනවා (moov atom අගට තියෙන mp4 වලටත් වැඩ කරනවා).
    Cache එකේ තිබ්බොත් network එකට යන්නේ නෑ → instant.
    """
    if not meta.size:
        return False
    cached = cache_get(message)
    if cached and apply_cached(meta, cached):
        logger.info("⚡ preview: cached metadata")
        return True

    path = os.path.join(workdir, "probe.bin")
    try:
        await asyncio.wait_for(_probe_fetch(client, message, path, meta.size),
                               timeout=timeout or PROBE_TIMEOUT)
    except Exception as e:      # partial data වුණත් ffprobe try කරනවා
        logger.warning(f"remote probe fetch incomplete: {e}")
    try:
        info = await ffprobe(path, timeout=20.0)
        ok = apply_probe(meta, info)
        if ok:
            cache_put(message, meta)
        return ok
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
RES_BPP = {1080: 0.9, 720: 1.0, 540: 1.1, 480: 1.2, 360: 1.35, 240: 1.5, 144: 1.8}
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
    """
    User ට තෝරන්න පුළුවන් output resolutions **සියල්ල** — source එකේ resolution එකත් ඇතුළුව,
    ලොකුම එක මුලින්. (upscale කරන්නේ නෑ: source එකට වඩා ලොකු ඒවා පෙන්වන්නේ නෑ.)
      • 1080p source → 1080p, 720p, 540p, 480p, 360p, 240p, 144p  (7ක්)
      • 576p source  → 576p, 540p, 480p, 360p, 240p, 144p
      • 240p source  → 240p, 144p
      • එකක් විතරයි නම් → [ඒක]  (bot එක ඒකෙන්ම ඉදිරියට යනවා, බොත්තම් අවශ්‍ය නෑ)
    """
    s = short_side(m)
    if not s:
        return [720]                                   # probe fail → sensible default
    out = {r for r in RESOLUTIONS if r <= s}
    out.add(int(s))                                    # source resolution එකම (standard නොවුණත්)
    return sorted(out, reverse=True)


def is_source_res(m: VideoMeta, t: int) -> bool:
    return abs(int(t) - int(short_side(m))) <= 2


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
    targets = offered_targets(m)
    if len(targets) > 1:
        choices = ("🎚 Qualities: <code>" +
                   " • ".join(f"{t}p" + (" (original)" if is_source_res(m, t) else "") for t in targets)
                   + "</code>")
        tail = "👇 <b>Output resolution එක තෝරන්න</b> (size එක ආසන්න ≈ අගයක්)"
    else:
        choices = f"🎚 Quality: <code>{targets[0]}p (original)</code>"
        tail = "👇 <b>එකම quality එකෙන් ඉදිරියට යනවා…</b>"

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
        choices,
        tail,
    ])
