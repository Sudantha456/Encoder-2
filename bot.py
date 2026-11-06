"""
bot.py — Telethon Video Encoder Bot
Video එකක් දාන්න → metadata preview → resolution (1080p…144p) තෝරන්න →
fast_downloader → FFmpeg (live progress) → fast_uploader
"""
import asyncio
import contextlib
import logging
import os
import re
import shutil
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

try:
    import uvloop
    uvloop.install()
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())
except ImportError:
    pass

from telethon import TelegramClient, events, Button
from telethon.errors import MessageNotModifiedError, FloodWaitError, QueryIdInvalidError

import config
from config import (API_ID, API_HASH, BOT_TOKEN, SESSION_NAME, ALLOWED_USERS,
                    PROGRESS_UPDATE_INTERVAL, DOWNLOAD_DIR, PRESETS, CODECS,
                    PRESET_INFO, CRF, AUDIO_BITRATE, MAX_UPLOAD_BYTES,
                    WORKER_CHOICES, QUEUE_MAX, QUEUE_RES_CHOICES,
                    ENCODE_MODES, CRF_CHOICES, CRF_INFO, PIX_FMT_CHOICES,
                    PIX_FMT_INFO, X265_TOGGLE_INFO)
from fast_conn import FastTcpConnection
from fast_downloader import close_download_engine, get_downloader, init_download_engine
from fast_uploader import get_uploader
from userbots import Relay
from media import (VideoMeta, telegram_meta, probe_remote, ffprobe, apply_probe,
                   offered_targets, estimate_size, estimate_compress_size,
                   render_preview, render_compress_preview, res_label,
                   short_side, guard_kbps, compress_guard_kbps)
from settings_store import SettingsStore
from transcoder import EncState, build_x265_extra, resolve_x265_pix_fmt, transcode
import dist
from ui import (SpeedTracker, render_transfer, render_encode, human_size,
                human_size_short, human_time, human_speed, esc, render_dist)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s: %(message)s")
logger = logging.getLogger("EncoderBot")

client = TelegramClient(SESSION_NAME, API_ID, API_HASH, connection=FastTcpConnection)
client.parse_mode = "html"
store = SettingsStore()
pool = dist.WorkerPool()
relay = Relay()                         # userbots ↔ private channel
queue: deque = deque()                  # බලාගෙන ඉන්න jobs (FIFO)
current_job = [None]                    # දැන් run වෙන job එක
queue_task: Optional[asyncio.Task] = None
probe_sem = asyncio.Semaphore(3)        # videos 20ක් එකවර දැම්මොත් probe flood වෙන එක වළක්වන්න


@dataclass
class Job:
    id: str
    user_id: int
    chat_id: int
    message: object
    meta: VideoMeta
    status: object
    cancelled: bool = False
    started: bool = False
    proc: object = None
    height: int = 0
    mode: str = "res"
    job_cfg: Optional[dict] = None
    queued: bool = False
    live: object = None


jobs = {}


# ───────────────────────── helpers ─────────────────────────
def allowed(uid: int) -> bool:
    return not ALLOWED_USERS or uid in ALLOWED_USERS


def cb(*parts) -> bytes:
    return ":".join(str(p) for p in parts).encode()


def safe_stem(name: str) -> str:
    stem = re.sub(r"[^\w\-. ]+", "_", os.path.splitext(name)[0]).strip(" ._") or "video"
    return stem[:80]


class Live:
    """Message එක throttle කරලා edit කරන helper (FloodWait / same-text safe)."""

    def __init__(self, msg, buttons=None):
        self.msg, self.buttons = msg, buttons
        self._busy, self._last = False, None

    async def set(self, text: str, force: bool = False):
        if text == self._last or (self._busy and not force):
            return
        self._busy = True
        try:
            await self.msg.edit(text, buttons=self.buttons)
            self._last = text
        except MessageNotModifiedError:
            self._last = text
        except FloodWaitError as e:
            logger.warning(f"edit FloodWait {e.seconds}s — skipped")
        except Exception as e:
            logger.warning(f"edit failed: {e}")
        finally:
            self._busy = False


# ───────────────────────── preview / settings panels ─────────────────────────
def job_target_badge(job: Job) -> str:
    if job.mode == "compress" or job.height <= 0:
        c = (job.job_cfg or {}).get("c_crf", 18)
        pf = (job.job_cfg or {}).get("c_pix_fmt", "yuv420p")
        return f"🗜 Size-Reduce (H.265 • CRF {c} • {pf})"
    return f"{job.height}p"


def preview_buttons(job: Job):
    cfg = store.get(job.user_id)
    if cfg["mode"] == "compress":
        est = estimate_compress_size(
            job.meta,
            crf=cfg["c_crf"],
            preset=cfg["c_preset"],
            pix_fmt=cfg["c_pix_fmt"],
            bframes=cfg["c_bframes"],
        )
        est_lbl = f" ≈{human_size_short(est)}" if est else ""
        start_btn = [Button.inline(
            f"🚀 H.265 Compress කරන්න (CRF {cfg['c_crf']}{est_lbl})",
            cb("cmp", job.id, "go"),
        )]
        qcrf_btns = [
            Button.inline(
                ("✅ " if cfg["c_crf"] == c else "") + f"CRF {c}" + ("⭐" if c == 18 else ""),
                cb("pcrf", job.id, c),
            )
            for c in (16, 18, 20, 22, 24)
        ]
        return [
            start_btn,
            qcrf_btns,
            [Button.inline("⚙️ Settings", cb("cfg", job.id)),
             Button.inline("❌ Cancel", cb("x", job.id))],
        ]

    btns = []
    for t in offered_targets(job.meta):
        est = estimate_size(job.meta, t, cfg["preset"], cfg["codec"])
        label = f"{t}p ≈{human_size_short(est)}" if est else f"{t}p"
        btns.append(Button.inline(label, cb("res", job.id, t)))
    rows = [btns[i:i + 3] for i in range(0, len(btns), 3)]
    rows.append([Button.inline("⚙️ Settings", cb("cfg", job.id)),
                 Button.inline("❌ Cancel", cb("x", job.id))])
    return rows


async def show_preview(job: Job):
    cfg = store.get(job.user_id)
    txt = (
        render_compress_preview(job.meta, cfg)
        if cfg["mode"] == "compress"
        else render_preview(job.meta, cfg["preset"], cfg["codec"])
    )
    await job.status.edit(txt, buttons=preview_buttons(job))


def _mode_switch_row(active_mode: str, job_id: str) -> list:
    return [
        Button.inline(
            ("✅ " if active_mode == "res" else "") + "📐 Resolution වෙනස් කිරීම",
            cb("mod", job_id, "res"),
        ),
        Button.inline(
            ("✅ " if active_mode == "compress" else "") + "🗜 Size-Reduce (H.265)",
            cb("mod", job_id, "compress"),
        ),
    ]


def settings_text(uid: int, sub: str = "") -> str:
    s = store.get(uid)
    if sub == "crf":
        lines = [
            "🎯 <b>CRF (Constant Rate Factor) — H.265 Quality vs Size</b>",
            "━━━━━━━━━━━━━━━━━━━━",
            f"දැනට තෝරා ඇත්තේ: <b>CRF {s['c_crf']} ({CRF_INFO[s['c_crf']][0]})</b>",
            "",
        ]
        for c in CRF_CHOICES:
            tier, desc = CRF_INFO[c]
            mark = "✅ " if s["c_crf"] == c else "• "
            lines.append(f"{mark}<b>{c} — {tier}</b>: {desc}")
        lines += [
            "",
            "━━━━━━━━━━━━━━━━━━━━",
            "💡 <b>Visually Lossless සඳහා නිර්දේශය:</b> <code>CRF 18 – 20</code>",
            "<i>අඩු CRF අගය = ඉහළම quality • වැඩි CRF අගය = තවත් කුඩා file size</i>",
        ]
        return "\n".join(lines)

    if sub == "pix":
        cur_pf = s["c_pix_fmt"]
        enc_pf = resolve_x265_pix_fmt(cur_pf)
        note_16 = (
            f"\n<i>ℹ️ {cur_pf}: 16-bit precision filter සැකසුමෙන් පසු HEVC උපරිම {enc_pf} ලෙස encode වේ.</i>"
            if cur_pf != enc_pf else ""
        )
        lines = [
            "🎨 <b>Pixel Format තේරීම (Bit-Depth &amp; Chroma)</b>",
            "━━━━━━━━━━━━━━━━━━━━",
            f"දැනට තෝරා ඇත්තේ: <code>{cur_pf}</code> ({PIX_FMT_INFO[cur_pf][0]}){note_16}",
            "",
        ]
        for pf in PIX_FMT_CHOICES:
            spec, desc = PIX_FMT_INFO[pf]
            mark = "✅ " if cur_pf == pf else "• "
            lines.append(f"{mark}<code>{pf}</code> <b>({spec})</b> — {desc}")
        lines += [
            "",
            "━━━━━━━━━━━━━━━━━━━━",
            "💡 සාමාන්‍ය phones/TV සියල්ලටම <code>yuv420p</code> හෝ <code>yuv420p10le</code> වඩාත්ම ගැලපේ.",
        ]
        return "\n".join(lines)

    if sub == "cpre":
        lines = [
            "🎚 <b>Size-Reduce Preset &amp; x265 Options</b>",
            "━━━━━━━━━━━━━━━━━━━━",
            f"🎚 Preset: <b>{s['c_preset']}</b> — <i>{PRESET_INFO[s['c_preset']]}</i>",
            "",
            "🧬 <b>x265 Advanced Options:</b>",
        ]
        for k, (title, desc) in X265_TOGGLE_INFO.items():
            st_txt = "ON ✅" if s[k] else "OFF ❌"
            lines.append(f"• <b>{title} ({st_txt})</b>: {desc}")
        lines += [
            "",
            "━━━━━━━━━━━━━━━━━━━━",
            "💡 <code>fast</code> / <code>medium</code> / <code>slow</code> preset එකකින් Visually Lossless H.265 හොඳම compression ලැබේ.",
        ]
        return "\n".join(lines)

    if s["mode"] == "compress":
        crf = s["c_crf"]
        crf_tier, crf_desc = CRF_INFO[crf]
        pf = s["c_pix_fmt"]
        pf_spec, pf_desc = PIX_FMT_INFO[pf]
        return "\n".join([
            "⚙️ <b>Encoding Settings — 🗜 Size-Reduce Mode</b>",
            "━━━━━━━━━━━━━━━━━━━━",
            "🔀 Mode: <b>🗜 Quality/Resolution නොවෙනස්ව Size අඩු කිරීම (H.265)</b>",
            f"🎯 CRF: <b>{crf} ({crf_tier})</b> — <i>{crf_desc}</i>",
            f"🎨 Pixel Format: <code>{pf}</code> <b>({pf_spec})</b>",
            f"   <i>{pf_desc}</i>",
            f"🎚 Preset: <b>{s['c_preset']}</b> — <i>{PRESET_INFO[s['c_preset']]}</i>",
            (
                f"🧬 x265: <b>B-Frames {'ON' if s['c_bframes'] else 'OFF'}</b> • "
                f"<b>SAO {'ON' if s['c_sao'] else 'OFF'}</b> • "
                f"<b>Lookahead {'ON' if s['c_lookahead'] else 'OFF'}</b> • "
                f"<b>AQ {'ON' if s['c_aq'] else 'OFF'}</b>"
            ),
            f"🌐 Distributed (GitHub workers): <b>{s['c_dist']}</b>"
            + ("" if dist.available() else "  <i>(මේ environment එකේ නෑ)</i>"),
            f"👷 Workers: <b>{s['c_workers']}</b>  <i>({min(WORKER_CHOICES)} – {max(WORKER_CHOICES)}, accounts {1 + len(dist.HELPERS)} × {config.WORKERS_PER_ACCOUNT})</i>",
            f"📋 Auto-queue: <b>{'📋 Ask (Preview + Button)' if s['c_auto'] == 'ask' else '⚡ Auto (කෙළින්ම Queue)'}</b>",
            f"👤 Userbot relay: <b>{('ON ×' + str(len(relay.ready))) if relay.enabled else 'OFF'}</b>",
            f"💾 Settings Save: {store.cloud_status_text()}",
            "━━━━━━━━━━━━━━━━━━━━",
            "<i>Original resolution එලෙසම තබා H.264 ➜ H.265 Visually Lossless ලෙස size අඩු කරයි.</i>",
        ])

    return "\n".join([
        "⚙️ <b>Encoding Settings — 📐 Resolution Mode</b>",
        "━━━━━━━━━━━━━━━━━━━━",
        "🔀 Mode: <b>📐 Resolution වෙනස් කිරීම (1080p … 144p)</b>",
        f"🎚 Preset: <b>{s['preset']}</b>",
        f"   <i>{PRESET_INFO[s['preset']]}</i>",
        f"🎞 Codec: <b>{s['codec']}</b>",
        f"🌐 Distributed (GitHub workers): <b>{s['dist']}</b>"
        + ("" if dist.available() else "  <i>(මේ environment එකේ නෑ)</i>"),
        f"👷 Workers: <b>{s['workers']}</b>  <i>({min(WORKER_CHOICES)} – {max(WORKER_CHOICES)}, accounts {1 + len(dist.HELPERS)} × {config.WORKERS_PER_ACCOUNT}; download එක යන අතරේත් වෙනස් කරන්න පුළුවන්)</i>",
        f"👤 Userbot relay: <b>{('ON ×' + str(len(relay.ready))) if relay.enabled else 'OFF'}</b>",
        f"📋 Auto-queue: <b>{'ask (බොත්තම්)' if s['qres'] == 'ask' else '🧠 smart' if s['qres'] == 'smart' else s['qres'] + 'p'}</b>",
        f"💾 Settings Save: {store.cloud_status_text()}",
        "━━━━━━━━━━━━━━━━━━━━",
        "ultrafast → වේගවත්ම  |  veryslow → අඩුම size",
        "H.265 → size අඩුයි, encode වෙන්න වැඩි වෙලාවක් යනවා",
    ])


def _short_pf_btn(pf: str, selected: bool) -> str:
    spec = PIX_FMT_INFO[pf][0].replace("-bit, ", "b ")
    return ("✅ " if selected else "") + f"{pf} ({spec})"


def settings_buttons(uid: int, job_id: str, sub: str = ""):
    s = store.get(uid)

    if sub == "crf":
        btns = [
            Button.inline(
                ("✅ " if s["c_crf"] == c else "") + f"{c} • {CRF_INFO[c][0]}" + (" ⭐" if c == 18 else ""),
                cb("ccrf", job_id, c),
            )
            for c in CRF_CHOICES
        ]
        rows = [btns[i:i + 2] for i in range(0, len(btns), 2)]
        rows.append([Button.inline("🔙 Settings වෙත", cb("cfg", job_id))])
        return rows

    if sub == "pix":
        btns = [
            Button.inline(_short_pf_btn(pf, s["c_pix_fmt"] == pf), cb("cpix", job_id, pf))
            for pf in PIX_FMT_CHOICES
        ]
        rows = [btns[i:i + 2] for i in range(0, len(btns), 2)]
        rows.append([Button.inline("🔙 Settings වෙත", cb("cfg", job_id))])
        return rows

    if sub == "cpre":
        rows, row = [], []
        for p in PRESETS:
            row.append(Button.inline(("✅ " if p == s["c_preset"] else "") + p, cb("cpre", job_id, p)))
            if len(row) == 3:
                rows.append(row)
                row = []
        rows.append([
            Button.inline(f"{'✅' if s['c_bframes'] else '❌'} B-Frames: {'ON' if s['c_bframes'] else 'OFF'}", cb("ctog", job_id, "bframes_s")),
            Button.inline(f"{'✅' if s['c_sao'] else '❌'} SAO: {'ON' if s['c_sao'] else 'OFF'}", cb("ctog", job_id, "sao_s")),
        ])
        rows.append([
            Button.inline(f"{'✅' if s['c_lookahead'] else '❌'} Lookahead: {'ON' if s['c_lookahead'] else 'OFF'}", cb("ctog", job_id, "lookahead_s")),
            Button.inline(f"{'✅' if s['c_aq'] else '❌'} AQ-Mode: {'ON' if s['c_aq'] else 'OFF'}", cb("ctog", job_id, "aq_s")),
        ])
        rows.append([Button.inline("🔙 Settings වෙත", cb("cfg", job_id))])
        return rows

    if s["mode"] == "compress":
        rows = [_mode_switch_row("compress", job_id)]
        rows.append([
            Button.inline(f"🎯 CRF: {s['c_crf']} ({CRF_INFO[s['c_crf']][0]}) ▸", cb("sub", job_id, "crf")),
            Button.inline(f"🎨 PixFmt: {s['c_pix_fmt']} ▸", cb("sub", job_id, "pix")),
        ])
        rows.append([
            Button.inline(f"🎚 Preset: {s['c_preset']} ▸", cb("sub", job_id, "cpre")),
            Button.inline(
                "📋 Queue: Ask" if s["c_auto"] == "ask" else "⚡ Queue: Auto",
                cb("caut", job_id, "auto" if s["c_auto"] == "ask" else "ask"),
            ),
        ])
        rows.append([
            Button.inline(f"{'✅' if s['c_bframes'] else '❌'} B-Frames: {'ON' if s['c_bframes'] else 'OFF'}", cb("ctog", job_id, "bframes")),
            Button.inline(f"{'✅' if s['c_sao'] else '❌'} SAO: {'ON' if s['c_sao'] else 'OFF'}", cb("ctog", job_id, "sao")),
        ])
        rows.append([
            Button.inline(f"{'✅' if s['c_lookahead'] else '❌'} Lookahead: {'ON' if s['c_lookahead'] else 'OFF'}", cb("ctog", job_id, "lookahead")),
            Button.inline(f"{'✅' if s['c_aq'] else '❌'} AQ-Mode: {'ON' if s['c_aq'] else 'OFF'}", cb("ctog", job_id, "aq")),
        ])
        rows.append([
            Button.inline(("✅ " if s["c_dist"] == m else "") + f"🌐 {m.title()}", cb("cdst", job_id, m))
            for m in ("auto", "on", "off")
        ])
        wbtns = [
            Button.inline(("✅ " if s["c_workers"] == n else "") + f"👷{n}", cb("cwrk", job_id, n))
            for n in WORKER_CHOICES
        ]
        rows += [wbtns[i:i + 5] for i in range(0, len(wbtns), 5)]
        rows.append([Button.inline("🔙 Back" if job_id != "0" else "✖ Close", cb("back", job_id))])
        return rows

    rows = [_mode_switch_row("res", job_id)]
    row = []
    for p in PRESETS:
        row.append(Button.inline(("✅ " if p == s["preset"] else "") + p, cb("pre", job_id, p)))
        if len(row) == 3:
            rows.append(row)
            row = []
    rows.append([Button.inline(("✅ " if c == s["codec"] else "") + n, cb("cod", job_id, c))
                 for c, n in CODECS.items()])
    rows.append([Button.inline(("✅ " if s["dist"] == m else "") + f"🌐 {m.title()}", cb("dst", job_id, m))
                 for m in ("auto", "on", "off")])
    wbtns = [Button.inline(("✅ " if s["workers"] == n else "") + f"👷{n}", cb("wrk", job_id, n))
             for n in WORKER_CHOICES]
    rows += [wbtns[i:i + 5] for i in range(0, len(wbtns), 5)]          # 5..25 / 30..50
    qb = [Button.inline(("✅ " if s["qres"] == q else "") + ("📋 Ask" if q == "ask" else "🧠 Smart" if q == "smart" else f"📋 {q}p"),
                        cb("qres", job_id, q)) for q in QUEUE_RES_CHOICES]
    rows += [qb[i:i + 3] for i in range(0, len(qb), 3)]
    rows.append([Button.inline("🔙 Back" if job_id != "0" else "✖ Close", cb("back", job_id))])
    return rows


# ───────────────────────── commands ─────────────────────────
@client.on(events.NewMessage(pattern=r"^/(start|help)", incoming=True))
async def cmd_start(event):
    if not allowed(event.sender_id):
        return
    await event.reply(
        "👋 <b>Video Encoder Bot</b>\n\n"
        "🔀 <b>Modes දෙකක් ඇත (/settings වලින් මාරු කරන්න):</b>\n"
        "  1️⃣ <b>📐 Resolution Mode</b> — 1080p…144p ලෙස resolution වෙනස් කිරීම\n"
        "  2️⃣ <b>🗜 Size-Reduce Mode (H.265)</b> — Resolution සහ Quality වෙනස් නොවී Visually Lossless ලෙස size අඩු කිරීම (CRF, Pixel Format, B-Frames/SAO/Lookahead, Workers)\n\n"
        "⚡ High-speed download → FFmpeg / GitHub Workers encode → High-speed upload\n"
        "☁️ ඔබගේ සියලුම /settings තේරීම් GitHub JSON හි ස්වයංක්‍රීයව save වේ\n\n"
        "⚙️ Settings: /settings   •   📋 /queue   •   🧹 /clear\n"
        "🌐 Worker workflows: /workflow   •   🛑 /End_Work (සේරම workers kill)"
    )


@client.on(events.NewMessage(pattern=r"^/settings", incoming=True))
async def cmd_settings(event):
    if not allowed(event.sender_id):
        return
    await event.reply(settings_text(event.sender_id), buttons=settings_buttons(event.sender_id, "0"))


async def init_helpers(notify: bool = True):
    """Helper GitHub accounts bootstrap (repo/files/secret) — bot start එකේදීත්, /check එකේදීත්."""
    if not (dist.available() and config.HELPER_TOKENS):
        return
    await dist.setup_helpers()
    logger.info(f"🤝 Helper accounts ready: {len(dist.HELPERS)}/{len(config.HELPER_TOKENS)} • workers capacity {dist.capacity()}")
    if notify and dist.HELPER_PROBLEMS:
        for uid in ALLOWED_USERS:
            with contextlib.suppress(Exception):
                await client.send_message(uid, dist.helpers_status())


@client.on(events.NewMessage(pattern=r"^/check", incoming=True))
async def cmd_check(event):
    """Userbots + private channel setup එක ආයෙ පරීක්ෂා කරනවා (job එකක් run වෙන්නේ නැති වෙලාවක)."""
    if not allowed(event.sender_id):
        return
    if pending_count():
        await event.reply(relay.status_text() + "\n\n<i>Jobs run වෙනවා — re-check කරන්නේ queue එක හිස් වුණාම.</i>")
        return
    m = await event.reply("🔍 <b>Userbots / channel setup බලනවා…</b>")
    await relay.stop()
    relay.userbots = []
    await relay.setup(client)
    await init_helpers(notify=False)
    await m.edit(relay.status_text() + "\n\n" + dist.helpers_status())


# ───────────────────────── 🌐 /workflow  &  🛑 /End_Work ─────────────────────────
_STATUS_ICON = {"in_progress": "▶️", "queued": "⏳", "waiting": "⏳", "requested": "⏳", "pending": "⏳"}
end_work_lock = asyncio.Lock()


def _age(iso: str) -> str:
    try:
        t = datetime.fromisoformat((iso or "").replace("Z", "+00:00"))
        sec = max(0, int((datetime.now(timezone.utc) - t).total_seconds()))
    except Exception:
        return "?"
    h, rem = divmod(sec, 3600)
    m, s = divmod(rem, 60)
    return f"{h}h {m:02d}m" if h else (f"{m}m {s:02d}s" if m else f"{s}s")


def _parts_str(idx: list) -> str:
    idx = sorted(set(idx))
    if not idx:
        return "-"
    out, start, prev = [], idx[0], idx[0]
    for i in idx[1:] + [None]:
        if i is not None and i == prev + 1:
            prev = i
            continue
        out.append(str(start) if start == prev else f"{start}-{prev}")
        if i is not None:
            start = prev = i
    return ",".join(out)


def _acct_label(i: int) -> str:
    return "🏠 Main" if i == 0 else f"🤝 Helper #{i}"


@client.on(events.NewMessage(pattern=r"(?i)^/workflows?(@\w+)?(\s|$)", incoming=True))
async def cmd_workflow(event):
    """Worker workflows ගැන සම්පූර්ණ විස්තර (account එකින් එක: session, runs, workers)."""
    if not allowed(event.sender_id):
        return
    if not dist.available():
        await event.reply("🌐 <b>Distributed workers unavailable</b>\n<i>gh CLI / GH_TOKEN / GITHUB_REPOSITORY නෑ.</i>")
        return
    m = await event.reply("🔍 <b>Worker workflows බලනවා…</b>")
    cur = current_job[0]
    cfg = store.get(event.sender_id)
    act_wrk = cfg["c_workers"] if cfg["mode"] == "compress" else cfg["workers"]
    act_dst = cfg["c_dist"] if cfg["mode"] == "compress" else cfg["dist"]
    head = [
        "🌐 <b>Worker Workflows</b>",
        "━━━━━━━━━━━━━━━━━━━━",
        f"⚙️ Mode: <b>{'🗜 Size-Reduce' if cfg['mode'] == 'compress' else '📐 Resolution'}</b> • Workers: <b>{act_wrk}</b> • Dist: <b>{esc(str(act_dst))}</b> • Capacity: <b>{dist.capacity()}</b>",
        f"📋 Queue: {'▶️ ' + esc(cur.meta.name[:30]) + ' + ' if cur else ''}<b>{len(queue)}</b> waiting",
        f"🧠 Pool: <b>{'ACTIVE' if pool.active else 'idle'}</b>{' • 🛑 halted (/End_Work)' if pool.halted else ''}",
    ]
    try:
        info = await pool.overview()
    except Exception as e:
        await m.edit("\n".join(head) + f"\n\n❌ GitHub query fail: <code>{esc(str(e)[:150])}</code>")
        return
    total_runs = total_workers = 0
    body = []
    for a in info:
        runs = a["runs"]
        total_runs += len(runs)
        sess = f"session <code>{a['session']}</code>" if a["session"] else "session -"
        flag = " • ❌ dispatch failed" if a["failed"] else ""
        body.append(f"\n{_acct_label(a['idx'])} — <code>{esc(a['repo'])}</code>")
        body.append(f"   {sess} • dispatched <b>{len(a['dispatched'])}</b> [{_parts_str(a['dispatched'])}]{flag}")
        if a["error"]:
            body.append(f"   ⚠️ {esc(a['error'])}")
            continue
        if not runs:
            body.append("   💤 Active workflow runs නෑ")
        for r in runs[:6]:
            jobs = r.get("jobs", [])
            run_n = sum(1 for j in jobs if j.get("status") == "in_progress")
            que_n = sum(1 for j in jobs if j.get("status") in ("queued", "waiting", "pending", "requested"))
            total_workers += run_n
            idx = [int(x) for j in jobs for x in re.findall(r"\((\d+)\)", j.get("name", ""))]
            body.append(f"   {_STATUS_ICON.get(r['status'], '•')} run <code>#{r['databaseId']}</code> • {r['status']} • {_age(r.get('createdAt'))}"
                        f"\n      👷 running <b>{run_n}</b> • queued <b>{que_n}</b> • parts [{_parts_str(idx)}]")
        if len(runs) > 6:
            body.append(f"   … තව runs {len(runs) - 6} ක්")
    summary = f"\n━━━━━━━━━━━━━━━━━━━━\n📊 Active runs: <b>{total_runs}</b> • Running workers: <b>{total_workers}</b>"
    tail = "\n🛑 සේරම kill කරන්න: /End_Work" if total_runs else ""
    text = "\n".join(head + body) + summary + tail
    await m.edit(text[:4000])


@client.on(events.NewMessage(pattern=r"(?i)^/end_?work(@\w+)?(\s|$)", incoming=True))
async def cmd_end_work(event):
    """Main + helper accounts වල run වෙන සේරම worker workflows kill (kill request 5 ක් දක්වා, kill වෙනකන්)."""
    if not allowed(event.sender_id):
        return
    if not dist.available():
        await event.reply("🌐 <b>Distributed workers unavailable</b> — kill කරන්න දෙයක් නෑ.")
        return
    if end_work_lock.locked():
        await event.reply("⏳ <i>/End_Work දැනටමත් run වෙනවා…</i>")
        return
    async with end_work_lock:
        m = await event.reply("🛑 <b>End Work</b> — workers kill කරනවා…")
        live = Live(m)
        busy = pending_count() > 0

        async def prog(rnd, total, force, counts):
            rows = "\n".join(f"   {_acct_label(i)}: <b>{n}</b> run" for i, n in counts.items() if n)
            await live.set(f"🛑 <b>Kill request {rnd}/{total}</b>{' (force)' if force else ''}\n{rows}", force=True)

        try:
            res = await pool.kill_all(rounds=5, on_progress=prog)
        except Exception as e:
            logger.exception("end_work failed")
            await m.edit(f"❌ <b>End Work fail</b>: <code>{esc(str(e)[:200])}</code>")
            return
        if pending_count() == 0:
            pool.halted = False            # job එකක් නැත්නම් ඊළඟ queue එකට workers ආයෙ allow
        left = sum(len(v) for v in res["left"].values())
        lines = ["🛑 <b>End Work</b>", "━━━━━━━━━━━━━━━━━━━━"]
        for i, repo in res["accts"]:
            n, rem = res["cancelled"].get(i, 0), len(res["left"].get(i, []))
            lines.append(f"{_acct_label(i)} <code>{esc(repo)}</code>\n   🔪 killed <b>{n}</b>" + (f" • ⚠️ තව <b>{rem}</b> ඉතුරුයි" if rem else " • ✅ clean"))
        lines.append("━━━━━━━━━━━━━━━━━━━━")
        if left:
            lines.append(f"⚠️ Kill requests {res['rounds']} ක් යැව්වත් runs <b>{left}</b> ඉතුරුයි — තත්පර කීපයකින් /workflow බලන්න, නැත්නම් ආයෙ /End_Work.")
        else:
            lines.append(f"✅ සේරම worker workflows kill වුණා (kill requests: {res['rounds']})")
        if busy:
            lines.append("<i>දැනට run වෙන distributed job එක worker pool එක නවත්වපු නිසා main runner එකට fallback නොවී stop/fail වෙයි.</i>")
        await m.edit("\n".join(lines))


def queue_text() -> str:
    rows = []
    cur = current_job[0]
    if cur:
        rows.append(f"▶️ <b>Running</b>: <code>{esc(cur.meta.name[:40])}</code> → {esc(job_target_badge(cur))}")
    for i, j in enumerate(queue, 1):
        rows.append(f"{i}. <code>{esc(j.meta.name[:40])}</code> → {esc(job_target_badge(j))}")
    if not rows:
        return "📭 Queue හිස්."
    return f"📋 <b>Queue</b> ({len(queue) + (1 if cur else 0)}/{QUEUE_MAX})\n━━━━━━━━━━━━━━━━━━━━\n" + "\n".join(rows)


@client.on(events.NewMessage(pattern=r"^/queue", incoming=True))
async def cmd_queue(event):
    if allowed(event.sender_id):
        await event.reply(queue_text())


@client.on(events.NewMessage(pattern=r"^/clear", incoming=True))
async def cmd_clear(event):
    if not allowed(event.sender_id):
        return
    mine = [j for j in queue if j.user_id == event.sender_id]
    for j in mine:
        await cancel_waiting(j)
    await event.reply(f"🧹 Queue එකෙන් {len(mine)} ක් ඉවත් කළා. (දැන් run වෙන එක නවත්තන්න ඒකේ ❌ Cancel)")


# ───────────────────────── queue ─────────────────────────
def pending_count() -> int:
    return len(queue) + (1 if current_job[0] else 0)


async def cancel_waiting(job: Job):
    job.cancelled = True
    with contextlib.suppress(ValueError):
        queue.remove(job)
    jobs.pop(job.id, None)
    with contextlib.suppress(Exception):
        await job.status.edit("❌ <b>Cancelled</b>", buttons=None)
    await refresh_positions()


async def refresh_positions():
    for i, j in enumerate(list(queue), 1):
        if j.live:
            await j.live.set(
                f"⏳ <b>Queued #{i}</b> / {len(queue)}\n"
                f"<code>{esc(j.meta.name[:50])}</code> → {esc(job_target_badge(j))}\n"
                "ඉස්සරහ jobs ඉවර වුණාම ස්වයංක්‍රීයව පටන් ගන්නවා.",
                force=True,
            )


async def enqueue(job: Job, height: int, *, mode: str = "res") -> bool:
    global queue_task
    if pending_count() >= QUEUE_MAX:
        with contextlib.suppress(Exception):
            await job.status.edit(f"🚫 Queue පිරිලා ({QUEUE_MAX}/{QUEUE_MAX}). ටිකක් ඉඳලා ආයෙ එවන්න.", buttons=None)
        jobs.pop(job.id, None)
        return False
    job.height = 0 if mode == "compress" else height
    job.mode = mode
    job.job_cfg = store.get(job.user_id)
    job.queued, job.started = True, True
    job.live = Live(job.status, [[Button.inline("❌ Cancel", cb("x", job.id))]])
    queue.append(job)
    await refresh_positions()
    if queue_task is None or queue_task.done():
        queue_task = asyncio.create_task(queue_loop())
    return True


async def queue_loop():
    """Jobs එකින් එක. Queue හිස් වුණාම workers pool එක නවත්තනවා (hold වෙලා හිටපු workers exit)."""
    global queue_task
    try:
        while True:
            while queue:
                job = queue.popleft()
                if job.cancelled:
                    continue
                current_job[0] = job
                await refresh_positions()
                try:
                    await run_job(job, job.height)
                except Exception:
                    logger.exception("run_job crashed")
                finally:
                    current_job[0] = None
            with contextlib.suppress(Exception):
                await pool.stop()
            if not queue:           # stop අතරේ අලුත් job ආවොත් ආයෙ loop
                queue_task = None
                return
    finally:
        if current_job[0] is None and not queue:
            queue_task = None


# ───────────────────────── video received ─────────────────────────
def is_video(e) -> bool:
    m = e.message
    if not m.document or m.gif:
        return False
    return (m.document.mime_type or "").startswith("video/") or bool(m.video)


@client.on(events.NewMessage(incoming=True, func=is_video))
async def on_video(event):
    if not allowed(event.sender_id):
        return
    msg = event.message
    status = await event.reply("🔎 <b>Analyzing video…</b>")
    job = Job(id=uuid.uuid4().hex[:8], user_id=event.sender_id, chat_id=event.chat_id,
              message=msg, meta=telegram_meta(msg), status=status)
    jobs[job.id] = job
    if relay.enabled:
        relay.trigger_warmup(job.meta.dc)

    workdir = Path(DOWNLOAD_DIR) / ("probe_" + job.id)
    workdir.mkdir(parents=True, exist_ok=True)
    try:
        async with probe_sem:
            await probe_remote(client, msg, job.meta, str(workdir))
    finally:
        shutil.rmtree(workdir, ignore_errors=True)

    ucfg = store.get(job.user_id)
    if ucfg["mode"] == "compress":
        if ucfg["c_auto"] == "auto":
            await enqueue(job, 0, mode="compress")
        else:
            await show_preview(job)
    else:
        qres = ucfg["qres"]
        if qres != "ask":                       # auto-queue: source එකට වඩා ලොකු resolution නම් ළඟම එක
            opts = offered_targets(job.meta)
            if qres == "smart":                 # source එකට අඩුවෙන් තියෙන හොඳම (ලොකුම) resolution එක
                target = max(opts)
            else:
                want = int(qres)
                target = want if want in opts else min(opts, key=lambda t: abs(t - want))
            await enqueue(job, target, mode="res")
        else:
            await show_preview(job)


# ───────────────────────── callbacks ─────────────────────────
async def safe_answer(event, *args, **kwargs):
    """Callback query එක (තත්පර ~15කින්) expire වුණොත් QueryIdInvalidError ignore කරනවා."""
    try:
        await event.answer(*args, **kwargs)
    except QueryIdInvalidError:
        pass
    except Exception as e:
        logger.debug(f"callback answer failed: {e!r}")


@client.on(events.CallbackQuery())
async def on_callback(event):
    try:
        action, job_id, arg = (event.data.decode().split(":", 2) + [""])[:3]
    except Exception:
        return
    job = jobs.get(job_id)
    uid = event.sender_id

    if job_id != "0" and not job:
        await safe_answer(event, "Job එක අවසන් / expire වෙලා.", alert=True)
        return
    if job and uid != job.user_id:
        await safe_answer(event, "මේක ඔයාගේ job එකක් නෙවෙයි.", alert=True)
        return

    if action == "res":
        if job.started:
            await safe_answer(event, "Already in queue…")
            return
        await safe_answer(event, "Queued ✔")      # කලින්ම answer — enqueue එක slow වුණත් query expire වෙන්නේ නෑ
        await enqueue(job, int(arg), mode="res")  # queue පිරිලා නම් enqueue එකම status message එක edit කරනවා

    elif action == "cmp":
        if job.started:
            await safe_answer(event, "Already in queue…")
            return
        await safe_answer(event, "Queued (Size-Reduce H.265) ✔")
        await enqueue(job, 0, mode="compress")

    elif action == "pcrf":
        if job.started:
            await safe_answer(event, "Already in queue…")
            return
        c_val = int(arg)
        if c_val not in CRF_CHOICES:
            return
        store.set(uid, c_crf=c_val)
        await safe_answer(event, f"CRF {c_val} ({CRF_INFO[c_val][0]}) ✔")
        with contextlib.suppress(MessageNotModifiedError):
            await show_preview(job)

    elif action == "x":
        if job.queued and job in queue:          # තාම පටන් අරන් නෑ → queue එකෙන් අයින්
            await safe_answer(event, "Removed")
            await cancel_waiting(job)
            return
        job.cancelled = True
        get_downloader().cancel(job.id)
        get_uploader().cancel(job.id)
        if job.proc:
            with contextlib.suppress(Exception):
                job.proc.kill()
        await safe_answer(event, "Cancelling…")
        if not job.started:
            jobs.pop(job.id, None)
            with contextlib.suppress(Exception):
                await job.status.edit("❌ <b>Cancelled</b>", buttons=None)

    elif action == "cfg":
        await safe_answer(event)
        with contextlib.suppress(MessageNotModifiedError):
            await event.edit(settings_text(uid), buttons=settings_buttons(uid, job_id))

    elif action == "sub":
        if arg not in ("crf", "pix", "cpre"):
            return
        await safe_answer(event)
        with contextlib.suppress(MessageNotModifiedError):
            await event.edit(settings_text(uid, sub=arg), buttons=settings_buttons(uid, job_id, sub=arg))

    elif action == "mod":
        if arg not in ENCODE_MODES:
            return
        store.set(uid, mode=arg)
        lbl = "🗜 Size-Reduce (H.265)" if arg == "compress" else "📐 Resolution Mode"
        await safe_answer(event, f"{lbl} ✔")
        with contextlib.suppress(MessageNotModifiedError):
            await event.edit(settings_text(uid), buttons=settings_buttons(uid, job_id))

    elif action == "ccrf":
        c_val = int(arg)
        if c_val not in CRF_CHOICES:
            return
        store.set(uid, c_crf=c_val)
        await safe_answer(event, f"CRF {c_val} ({CRF_INFO[c_val][0]}) ✔")
        with contextlib.suppress(MessageNotModifiedError):
            await event.edit(settings_text(uid, sub="crf"), buttons=settings_buttons(uid, job_id, sub="crf"))

    elif action == "cpix":
        if arg not in PIX_FMT_CHOICES:
            return
        store.set(uid, c_pix_fmt=arg)
        await safe_answer(event, f"{arg} ✔")
        with contextlib.suppress(MessageNotModifiedError):
            await event.edit(settings_text(uid, sub="pix"), buttons=settings_buttons(uid, job_id, sub="pix"))

    elif action == "cpre":
        if arg not in PRESETS:
            return
        store.set(uid, c_preset=arg)
        await safe_answer(event, f"Preset: {arg} ✔")
        with contextlib.suppress(MessageNotModifiedError):
            await event.edit(settings_text(uid, sub="cpre"), buttons=settings_buttons(uid, job_id, sub="cpre"))

    elif action == "ctog":
        in_sub = arg.endswith("_s")
        base_arg = arg[:-2] if in_sub else arg
        tmap = {"bframes": "c_bframes", "sao": "c_sao", "lookahead": "c_lookahead", "aq": "c_aq"}
        if base_arg not in tmap:
            return
        k = tmap[base_arg]
        cur_s = store.get(uid)
        new_val = not bool(cur_s.get(k, True))
        store.set(uid, **{k: new_val})
        title = X265_TOGGLE_INFO[k][0]
        await safe_answer(event, f"{title}: {'ON' if new_val else 'OFF'} ✔")
        sub_name = "cpre" if in_sub else ""
        with contextlib.suppress(MessageNotModifiedError):
            await event.edit(settings_text(uid, sub=sub_name), buttons=settings_buttons(uid, job_id, sub=sub_name))

    elif action in ("caut", "cdst", "cwrk"):
        key = {"caut": "c_auto", "cdst": "c_dist", "cwrk": "c_workers"}[action]
        if action == "cwrk":
            if int(arg) not in WORKER_CHOICES:
                return
            arg = int(arg)
        store.set(uid, **{key: arg})
        await safe_answer(event, f"{arg} ✔")
        if action == "cwrk" and pool.active and dist.available():
            async def _grow_c():
                with contextlib.suppress(Exception):
                    await pool.ensure(min(arg, dist.capacity()))
            asyncio.create_task(_grow_c())
        with contextlib.suppress(MessageNotModifiedError):
            await event.edit(settings_text(uid), buttons=settings_buttons(uid, job_id))

    elif action in ("pre", "cod", "dst", "wrk", "qres"):
        key = {"pre": "preset", "cod": "codec", "dst": "dist", "wrk": "workers", "qres": "qres"}[action]
        if action == "wrk":
            if int(arg) not in WORKER_CHOICES:
                return
            arg = int(arg)
        store.set(uid, **{key: arg})
        await safe_answer(event, f"{arg} ✔")
        if action == "wrk" and pool.active and dist.available():
            # Download අතරේ වැඩි කළොත් අලුත් runners ඉක්මනින් start වෙන්න (අඩු කළොත් ඉතුරු ඒවා hold)
            async def _grow():
                with contextlib.suppress(Exception):
                    await pool.ensure(min(arg, dist.capacity()))
            asyncio.create_task(_grow())
        with contextlib.suppress(MessageNotModifiedError):
            await event.edit(settings_text(uid), buttons=settings_buttons(uid, job_id))

    elif action == "back":
        await safe_answer(event)
        if job and not job.started:
            await show_preview(job)
        else:
            with contextlib.suppress(Exception):
                await event.delete()


# ───────────────────────── main pipeline ─────────────────────────
async def run_job(job: Job, height: int):
    cancel_btn = [[Button.inline("❌ Cancel", cb("x", job.id))]]
    live = job.live or Live(job.status, cancel_btn)
    live.buttons = cancel_btn
    meta = job.meta
    jcfg = job.job_cfg or store.get(job.user_id)
    live_cfg = store.get(job.user_id)
    is_compress = job.mode == "compress" or height <= 0

    if is_compress:
        preset = jcfg["c_preset"]
        codec = "libx265"
        crf_val = jcfg["c_crf"]
        pix_fmt = jcfg["c_pix_fmt"]
        x265_extra = build_x265_extra(
            bframes=jcfg["c_bframes"],
            sao=jcfg["c_sao"],
            lookahead=jcfg["c_lookahead"],
            aq=jcfg["c_aq"],
        )
        dist_mode = live_cfg["c_dist"]
        est_size = estimate_compress_size(
            meta, crf=crf_val, preset=preset, pix_fmt=pix_fmt, bframes=jcfg["c_bframes"]
        )
    else:
        preset = jcfg["preset"]
        codec = jcfg["codec"]
        crf_val = CRF[codec]
        pix_fmt = "yuv420p"
        x265_extra = ""
        dist_mode = live_cfg["dist"]
        est_size = estimate_size(meta, height, preset, codec)

    workdir = Path(DOWNLOAD_DIR) / job.id
    workdir.mkdir(parents=True, exist_ok=True)
    stem = safe_stem(meta.name)
    ext = os.path.splitext(meta.name)[1] or ".mp4"
    src = workdir / f"in_{stem}{ext}"
    dst = (
        workdir / f"{stem}_H265_CRF{crf_val}.mp4"
        if is_compress
        else workdir / f"{stem}_{height}p.mp4"
    )

    phase = ["dl"]
    t_start = time.monotonic()
    stats = {}
    dl, ul = get_downloader(), get_uploader()
    ch_ids, ch_out = [], [None, None]       # channel messages (cleanup): bot ගේ ids, [userbot msg, userbot]

    try:
        if job.cancelled:
            raise asyncio.CancelledError()

        # 0) Workers ඉක්මන් start: download එක යන අතරේම runners ready කරන්න ─
        if dist.should_distribute(meta, 0 if is_compress else height, preset, codec, dist_mode, pix_fmt=pix_fmt):
            async def _prestart():
                with contextlib.suppress(Exception):
                    cur_c = store.get(job.user_id)
                    w_n = cur_c["c_workers"] if is_compress else cur_c["workers"]
                    await pool.ensure(min(w_n, dist.capacity()))
            prestart = asyncio.create_task(_prestart())

        # 1) DOWNLOAD ─────────────────────────────
        use_relay, ch_in = relay.enabled and bool(relay.ready), None
        dl_badge = f"⚡ {len(relay.ready)}×UB" if use_relay else ""
        tr = SpeedTracker(total=meta.size)

        async def dl_cb(cur, tot):
            if phase[0] != "dl":
                return
            tr.update(cur, tot)
            await live.set(render_transfer("dl", tr, meta.name, dl_badge))

        if use_relay:                                      # user ගේ video එක → private channel + parallel DC pool warmup
            try:
                await live.set("📨 <b>Channel එකට forward කරනවා…</b>", force=True)
                fwd_task = asyncio.create_task(relay.forward_in(client, job.message))
                warm_task = asyncio.create_task(relay.warmup(meta.dc))
                try:
                    ch_in = await fwd_task
                    ch_ids.append(ch_in)
                finally:
                    if not warm_task.done():
                        # Keep warmup running in background if not finished yet; download_lanes attaches seamlessly
                        relay._bg_tasks.add(warm_task)
                        warm_task.add_done_callback(relay._bg_tasks.discard)
                    else:
                        with contextlib.suppress(Exception):
                            warm_task.result()
            except Exception as e:
                logger.warning(f"relay forward failed ({e!r}) → bot download")
                use_relay = False
                dl_badge = ""

        tr.reset()
        await live.set(render_transfer("dl", tr, meta.name, dl_badge), force=True)

        if use_relay:
            try:
                await relay.download(dl, ch_in, str(src), job.id, dl_cb)
                stats["via"] = relay.last_used_lanes or [u.name for u in relay.ready]
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if job.cancelled:
                    raise asyncio.CancelledError()
                logger.warning(f"relay download failed ({e!r}) → bot download")
                await live.set("⚠️ Userbot download fail → bot එකෙන් download කරනවා…", force=True)
                src.unlink(missing_ok=True)
                use_relay = False
                dl_badge = ""
                stats.pop("via", None)
                tr.reset()
                await dl.download(client, job.message, str(src), job.id, dl_cb)
        else:
            await dl.download(client, job.message, str(src), job.id, dl_cb)
        stats["dl_avg"], stats["dl_time"] = tr.avg_speed, tr.elapsed
        if job.cancelled:
            raise asyncio.CancelledError()

        # 2) REAL PROBE ───────────────────────────
        phase[0] = "probe"
        await live.set("🔍 <b>Analyzing downloaded file…</b>", force=True)
        if not apply_probe(meta, await ffprobe(str(src))):
            raise RuntimeError("Valid video stream එකක් හමු වුණේ නෑ.")

        # 3) ENCODE ───────────────────────────────
        phase[0] = "enc"
        src_ab = meta.abitrate // 1000
        if is_compress:
            # Recompute estimate after real probe
            est_size = estimate_compress_size(
                meta, crf=crf_val, preset=preset, pix_fmt=pix_fmt, bframes=jcfg["c_bframes"]
            )
            if not meta.acodec:
                audio_mode, ab = "none", 0
            elif meta.acodec in ("aac", "mp3", "ac3", "eac3"):
                # Original audio quality 100% රැකගැනීමට lossless stream copy
                audio_mode, ab = "copy", src_ab or 192
            else:
                audio_mode, ab = "aac", min(max(src_ab or 192, 160), 256)

            enc_opts = dict(
                target=0,
                scale=False,
                portrait=meta.height > meta.width,
                preset=preset,
                codec="libx265",
                crf=crf_val,
                maxrate_kbps=compress_guard_kbps(meta, crf_val),
                abitrate=ab,
                audio_mode=audio_mode,
                pix_fmt=pix_fmt,
                x265_extra=x265_extra,
            )
            t_label = f"🗜 {meta.width}×{meta.height} (Original)" if meta.width else "🗜 Size-Reduce"
            extra_lbl = f"CRF {crf_val} • {pix_fmt}"
        else:
            est_size = estimate_size(meta, height, preset, codec)
            # Audio: source එකේ bitrate එකට වඩා වැඩි කරන්නේ නෑ; AAC & අඩු නම් copy
            ab = AUDIO_BITRATE.get(height, 96)
            ac = meta.acodec == "aac" and 0 < src_ab <= ab
            if src_ab and not ac:
                ab = min(ab, src_ab)
            audio_mode = "none" if (meta.probed and not meta.acodec) else ("copy" if ac else "aac")
            enc_opts = dict(
                target=height,
                scale=short_side(meta) > height,
                portrait=meta.height > meta.width,
                preset=preset,
                codec=codec,
                crf=crf_val,
                maxrate_kbps=guard_kbps(meta, height),
                abitrate=ab,
                audio_mode=audio_mode,
                pix_fmt="yuv420p",
                x265_extra="",
            )
            t_label = f"{height}p"
            extra_lbl = ""
        encoded = False

        # 3a) Distributed (GitHub Actions workers) ─
        cfg = store.get(job.user_id)         # download අතරේ settings (workers…) වෙනස් කළා නම් අලුත් එක
        cur_wrk = cfg["c_workers"] if is_compress else cfg["workers"]
        cur_dst = cfg["c_dist"] if is_compress else cfg["dist"]
        n_workers = min(cur_wrk, dist.capacity())       # accounts වලින් දෙන්න පුළුවන් උපරිමය
        if dist.should_distribute(meta, 0 if is_compress else height, preset, codec, cur_dst, pix_fmt=pix_fmt):
            dst_state = dist.DistState(meta.duration, meta.fps, n_workers)

            async def dist_ticker():
                while True:
                    await live.set(
                        render_dist(
                            dst_state, meta.name, height, preset, codec,
                            target_label=t_label, extra_info=extra_lbl,
                        )
                    )
                    await asyncio.sleep(PROGRESS_UPDATE_INTERVAL)

            dticker = asyncio.create_task(dist_ticker())
            try:
                run = dist.DistRun(client=client, job=job, src=src, dst=dst, meta=meta,
                                   state=dst_state, opts=enc_opts, workdir=workdir,
                                   pool=pool, n_workers=n_workers)
                dstats = await run.run()
                stats["enc_time"], stats["enc_speed"] = dst_state.elapsed, meta.duration / max(dst_state.elapsed, 1)
                stats["workers"] = dstats["workers"]
                stats["local_parts"] = dstats.get("local_parts", 0)
                stats["reassigned_parts"] = dstats.get("reassigned_parts", 0)
                encoded = True
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.exception(f"Distributed encode failed (local fallback disabled): {e!r}")
                await live.set(
                    f"❌ <b>Distributed encode failed</b> ({esc(str(e)[:160])})\n"
                    "Worker pool එකෙන් part එකක් complete කරන්න නොහැකි වුණා. Main runner fallback disabled.",
                    force=True,
                )
                raise
            finally:
                dticker.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await dticker

        # 3b) Single runner ───────────────────────
        if not encoded:
            st = EncState(duration=meta.duration, src_fps=meta.fps)

            async def enc_ticker():
                while True:
                    await live.set(
                        render_encode(
                            st, meta.name, height, preset, codec,
                            target_label=t_label, extra_info=extra_lbl,
                        )
                    )
                    await asyncio.sleep(PROGRESS_UPDATE_INTERVAL)

            ticker = asyncio.create_task(enc_ticker())
            try:
                await transcode(
                    str(src), str(dst), st, job,
                    target=enc_opts["target"], scale=enc_opts["scale"], portrait=enc_opts["portrait"],
                    preset=preset, codec=codec, crf=crf_val, abitrate=ab,
                    maxrate_kbps=enc_opts["maxrate_kbps"], audio_mode=enc_opts["audio_mode"],
                    pix_fmt=enc_opts["pix_fmt"], x265_extra=enc_opts["x265_extra"],
                )
            finally:
                ticker.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await ticker
            stats["enc_time"], stats["enc_speed"] = st.elapsed, st.avg_speed
        with contextlib.suppress(OSError):
            os.remove(src)

        out_size = os.path.getsize(dst)
        if out_size > MAX_UPLOAD_BYTES:
            raise RuntimeError(f"Output {human_size(out_size)} — Telegram bot limit (≈2GB) ඉක්මවනවා.")

        # 4) UPLOAD ───────────────────────────────
        phase[0] = "ul"
        tr2 = SpeedTracker(total=out_size)
        await live.set(render_transfer("ul", tr2, dst.name), force=True)

        async def ul_cb(cur, tot):
            if phase[0] != "ul":
                return
            tr2.update(cur, tot)
            await live.set(render_transfer("ul", tr2, dst.name))

        saved_pct = (1 - out_size / meta.size) * 100 if meta.size else 0
        if is_compress:
            res_txt = f"{meta.width}×{meta.height} ({res_label(meta)})" if meta.width else "Original Res"
            caption = (
                f"🎬 <b>{esc(stem)}</b>\n"
                f"🗜 {res_txt} • 🎞 H.265 CRF {crf_val} ({CRF_INFO[crf_val][0]}) • 🎨 {pix_fmt} • ⚙️ {preset}\n"
                f"📦 {human_size(out_size)} (original {human_size(meta.size)} • {'−' if saved_pct >= 0 else '+'}{abs(saved_pct):.0f}%)"
            )
        else:
            caption = (
                f"🎬 <b>{esc(stem)}</b>\n"
                f"📐 {height}p • 🎞 {codec} • ⚙️ {preset}\n"
                f"📦 {human_size(out_size)} (original {human_size(meta.size)})"
            )
        sent = None
        if use_relay:
            try:
                ub, ch_msg = await relay.upload(ul, str(dst), job.id, ul_cb)       # userbot → channel
                ch_out[0], ch_out[1] = ch_msg, ub
                stats["ul_via"] = ub.name
                stats["ul_avg"], stats["ul_time"] = tr2.avg_speed, tr2.elapsed
                await live.set("📨 <b>User ගේ chat එකට යවනවා…</b>", force=True)
                sent = await relay.deliver(client, job.chat_id, ch_msg.id, caption)  # main bot → user (re-upload නෑ)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if job.cancelled:
                    raise asyncio.CancelledError()
                logger.warning(f"relay upload/deliver failed ({e!r}) → bot upload")
                await live.set("⚠️ Userbot upload fail → bot එකෙන් upload කරනවා…", force=True)
                tr2 = SpeedTracker(total=out_size)
                sent = None
        if sent is None:
            sent = await ul.upload_file(client, job.chat_id, str(dst), job.id, ul_cb)
            stats["ul_avg"], stats["ul_time"] = tr2.avg_speed, tr2.elapsed
            with contextlib.suppress(Exception):
                await client.edit_message(sent, caption)
        phase[0] = "done"

        saved = (1 - out_size / meta.size) * 100 if meta.size else 0
        live.buttons = None
        await live.set("\n".join([
            "✅ <b>Done!</b>",
            f"<code>{esc(dst.name)}</code>",
            "━━━━━━━━━━━━━━━━━━━━",
            f"📦 <code>{human_size(meta.size)}</code> → <code>{human_size(out_size)}</code> "
            f"({'−' if saved >= 0 else '+'}{abs(saved):.0f}%)"
            + (f"  •  est ≈{human_size(est_size)}" if est_size else ""),
            f"📥 DL{' 👤×' + str(len(stats['via'])) if stats.get('via') else ''}: "
            f"<code>{human_speed(stats['dl_avg'])}</code> • <code>{human_time(stats['dl_time'])}</code>",
            f"🎬 Encode{' 🌐×' + str(stats['workers']) if 'workers' in stats else ''}: "
            f"<code>{stats['enc_speed']:.2f}x</code> • <code>{human_time(stats['enc_time'])}</code>"
            + (f"  (♻️ reassigned: {stats['reassigned_parts']})" if stats.get("reassigned_parts") else "")
            + (f"  (🏠 local parts: {stats['local_parts']})" if stats.get("local_parts") else ""),
            f"📤 UL{' 👤' + stats['ul_via'] if stats.get('ul_via') else ''}: "
            f"<code>{human_speed(stats['ul_avg'])}</code> • <code>{human_time(stats['ul_time'])}</code>",
            f"⏱ Total: <code>{human_time(time.monotonic() - t_start)}</code>",
        ]), force=True)

    except asyncio.CancelledError:
        phase[0] = "stop"
        live.buttons = None
        await live.set("❌ <b>Cancelled</b>", force=True)
    except Exception as e:
        phase[0] = "stop"
        logger.exception("job failed")
        live.buttons = None
        await live.set(f"⚠️ <b>Failed</b>\n<code>{esc(str(e)[-900:])}</code>", force=True)
    finally:
        if ch_ids or ch_out[0] is not None:
            with contextlib.suppress(Exception):
                await relay.cleanup(client, ch_ids, ch_out[0], ch_out[1])
        await dl.cleanup(job.id)
        await ul.cleanup(job.id)
        shutil.rmtree(workdir, ignore_errors=True)
        jobs.pop(job.id, None)


async def main():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    await client.start(bot_token=BOT_TOKEN)
    await init_download_engine()
    logger.info(f"🌐 Distributed workers: {'available (on-demand per queue)' if dist.available() else 'unavailable'}")
    me = await client.get_me()
    await relay.setup(client)
    await init_helpers()
    if relay.configured and not relay.enabled:           # setup වැරදි නම් admins ට කෙළින්ම කියනවා
        for uid in ALLOWED_USERS:
            with contextlib.suppress(Exception):
                await client.send_message(uid, relay.status_text() + "\n\n<i>Fallback: bot එකෙන්ම download/upload. "
                                          "හදලා /check කරන්න.</i>")
    logger.info(f"✅ @{me.username} running • update interval {PROGRESS_UPDATE_INTERVAL}s")
    try:
        await client.run_until_disconnected()
    finally:
        await asyncio.to_thread(store.flush, 8.0)
        await relay.stop()
        await close_download_engine()


if __name__ == "__main__":
    client.loop.run_until_complete(main())
