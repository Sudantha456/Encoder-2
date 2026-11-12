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
from fast_uploader import close_upload_engine, get_uploader, init_upload_engine
from userbots import Relay
from media import (VideoMeta, telegram_meta, probe_remote, ffprobe, apply_probe,
                   offered_targets, estimate_size, estimate_compress_size,
                   render_preview, render_compress_preview, res_label,
                   short_side, guard_kbps, compress_guard_kbps)
from settings_store import SettingsStore
from transcoder import EncState, build_x265_extra, resolve_x265_pix_fmt, transcode
import dist
from ui import SpeedTracker
from widgets import (Doc, cancelled_card, chips, clip, done_card, endwork_progress,
                     endwork_result, esc, event_card, failed_card, health_card,
                     home_card, human_age, human_size, human_size_short, human_time,
                     note, queue_card, queued_card, render_dist, render_encode,
                     render_transfer, speed_txt, toast_card, to_plain, workflow_account)

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


async def reply_card(event, text: str, buttons=None):
    """Reply with an HTML widget; degrade to plain text if entities are rejected."""
    try:
        return await event.reply(text, buttons=buttons)
    except FloodWaitError:
        raise                                   # no duplicate send while rate-limited
    except Exception as e:
        logger.warning(f"widget reply failed ({e!r}) → plain fallback")
        return await event.reply(to_plain(text), buttons=buttons, parse_mode=None)


async def edit_card(msg, text: str, buttons=None):
    """Edit a widget message; never raise on parse/entity problems."""
    try:
        await msg.edit(text, buttons=buttons)
    except (MessageNotModifiedError, FloodWaitError):
        pass                                    # nothing to do / back off quietly
    except Exception as e:
        logger.warning(f"widget edit failed ({e!r}) → plain fallback")
        with contextlib.suppress(Exception):
            await msg.edit(to_plain(text), buttons=buttons, parse_mode=None)


def safe_stem(name: str) -> str:
    stem = re.sub(r"[^\w\-. ]+", "_", os.path.splitext(name)[0]).strip(" ._") or "video"
    return stem[:80]


class Live:
    """
    Per-chat FloodWait-safe message editor.
    • Spaces edits per chat so Telegram's ~20 edits/min per-chat limit is never exceeded.
    • Coalesces rapid force=True transitions so final states ('Done!', 'Cancelled') always land cleanly.
    """

    _chat_next_edit: dict = {}
    _chat_locks: dict = {}

    def __init__(self, msg, buttons=None):
        self.msg, self.buttons = msg, buttons
        self._busy = False
        self._last = None
        self._next_own = 0.0
        self._seq = 0
        self._lock = asyncio.Lock()

    @property
    def _chat_id(self) -> int:
        return int(getattr(self.msg, "chat_id", 0) or 0)

    def _chat_lock(self) -> asyncio.Lock:
        cid = self._chat_id
        lk = Live._chat_locks.get(cid)
        if lk is None:
            lk = asyncio.Lock()
            Live._chat_locks[cid] = lk
        return lk

    async def set(self, text: str, force: bool = False):
        if text == self._last:
            return
        min_iv = max(PROGRESS_UPDATE_INTERVAL, getattr(config, "MIN_EDIT_INTERVAL", 3.8))
        cid = self._chat_id
        now = time.monotonic()

        if not force:
            if self._busy or now < self._next_own or now < Live._chat_next_edit.get(cid, 0.0):
                return
            if self._lock.locked() or self._chat_lock().locked():
                return

        self._seq += 1
        my_seq = self._seq

        async with self._lock:
            if my_seq < self._seq or text == self._last:
                return
            self._busy = True
            try:
                async with self._chat_lock():
                    if my_seq < self._seq or text == self._last:
                        return
                    wait_rem = Live._chat_next_edit.get(cid, 0.0) - time.monotonic()
                    if wait_rem > 0:
                        if not force:
                            return
                        await asyncio.sleep(min(wait_rem, 5.0))
                        if my_seq < self._seq or text == self._last:
                            return

                    await self.msg.edit(text, buttons=self.buttons)
                    t_done = time.monotonic()
                    self._last = text
                    self._next_own = t_done + (1.2 if force else min_iv)
                    Live._chat_next_edit[cid] = t_done + (1.4 if force else 2.2)
            except MessageNotModifiedError:
                self._last = text
            except FloodWaitError as e:
                wait_s = max(float(getattr(e, "seconds", 3) or 3), 1.0)
                Live._chat_next_edit[cid] = time.monotonic() + wait_s + 1.0
                self._next_own = Live._chat_next_edit[cid]
                logger.warning(f"edit FloodWait {e.seconds}s — backing off chat {cid}")
            except Exception as e:
                logger.warning(f"edit failed: {e}")
                # Never leave the user without progress: if the client/server rejected
                # the entities, push a parse-free version of the same widget.
                if any(k in str(e).lower() for k in ("parse", "entit", "bad request", "unsupported")):
                    with contextlib.suppress(Exception):
                        await self.msg.edit(to_plain(text), buttons=self.buttons, parse_mode=None)
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


def _crf_panel(s: dict) -> str:
    cur = int(s["c_crf"])
    tier, desc = CRF_INFO[cur]
    d = Doc()
    d.head("🎯", "CRF TUNING", "quality vs size")
    d.quote([f"දැනට <b>CRF {cur} ({esc(tier)})</b>", f"<i>{esc(desc)}</i>"])
    rows = []
    for c in CRF_CHOICES:
        t, dd = CRF_INFO[c]
        rows.append(f"{'✅' if c == cur else '•'} <b>{c} — {esc(t)}</b>: {esc(dd)}")
    d.sec("🎚", "CRF Choices")
    d.quote(rows, expandable=True)
    d.add("💡 <b>Visually Lossless:</b> <code>CRF 18 – 20</code>")
    d.add(note("අඩු CRF = ඉහළම quality · වැඩි CRF = තවත් කුඩා file size"))
    return d.build()


def _pix_panel(s: dict) -> str:
    cur = s["c_pix_fmt"]
    spec, desc = PIX_FMT_INFO[cur]
    enc_pf = resolve_x265_pix_fmt(cur)
    d = Doc()
    d.head("🎨", "PIXEL FORMAT", "bit-depth · chroma")
    top = [f"දැනට <code>{esc(cur)}</code> <b>({esc(spec)})</b>", f"<i>{esc(desc)}</i>"]
    if cur != enc_pf:
        top.append(f"ℹ️ HEVC උපරිම <code>{esc(enc_pf)}</code> ලෙස encode වේ")
    d.quote(top)
    rows = []
    for pf in PIX_FMT_CHOICES:
        pf_spec, pf_desc = PIX_FMT_INFO[pf]
        rows.append(f"{'✅' if pf == cur else '•'} <code>{esc(pf)}</code> "
                    f"<b>({esc(pf_spec)})</b>: {esc(pf_desc)}")
    d.sec("🧪", "Choices")
    d.quote(rows, expandable=True)
    d.add(note("සාමාන්‍ය phones/TV සියල්ලටම yuv420p හෝ yuv420p10le වඩාත්ම ගැලපේ."))
    return d.build()


def _cpre_panel(s: dict) -> str:
    d = Doc()
    d.head("🎚", "PRESET & x265", "advanced")
    d.quote([f"🎚 Preset <b>{esc(s['c_preset'])}</b>", f"<i>{esc(PRESET_INFO[s['c_preset']])}</i>"])
    d.sec("🧬", "x265 toggles")
    for k, (t, desc) in X265_TOGGLE_INFO.items():
        d.add(f"{'✅' if s[k] else '❌'} <b>{esc(t)}</b> — <i>{esc(desc)}</i>")
    d.add(note("fast / medium / slow preset එකකින් Visually Lossless H.265 හොඳම compression ලැබේ."))
    return d.build()


def _pipeline_rows(s: dict, *, compress: bool) -> list:
    dist_key = "c_dist" if compress else "dist"
    wrk_key = "c_workers" if compress else "workers"
    auto = s.get("c_auto") if compress else s.get("qres")
    auto_lbl = (("auto" if auto == "auto" else "ask (button)") if compress else _queue_lbl(auto))
    rows = [
        f"🌐 Distributed <b>{esc(str(s[dist_key]))}</b> · 👷 Workers <b>{s[wrk_key]}</b> "
        f"<i>({min(WORKER_CHOICES)}–{max(WORKER_CHOICES)})</i>",
        f"👤 Relay <b>{_relay_short()}</b> · 📋 Auto-queue <b>{auto_lbl}</b>"
        + ("" if dist.available() else " · <i>dist n/a</i>"),
    ]
    return rows


def settings_text(uid: int, sub: str = "") -> str:
    s = store.get(uid)

    if sub == "crf":
        return _crf_panel(s)
    if sub == "pix":
        return _pix_panel(s)
    if sub == "cpre":
        return _cpre_panel(s)

    if s["mode"] == "compress":
        crf = int(s["c_crf"])
        crf_tier, crf_desc = CRF_INFO[crf]
        pf = s["c_pix_fmt"]
        pf_spec, pf_desc = PIX_FMT_INFO[pf]
        d = Doc()
        d.head("⚙️", "ENCODE SETTINGS", "🗜 size-reduce · live")
        d.quote([
            "🗜 <b>Size-Reduce (H.265)</b> · visually lossless",
            chips(f"CRF {crf}", pf, s["c_preset"], f"{s['c_workers']}w"),
            f"🌐 dist <code>{esc(str(s['c_dist']))}</code> · 📋 queue "
            f"<code>{'auto' if s['c_auto'] == 'auto' else 'ask'}</code>",
        ])
        d.sec("🎯", "Quality")
        d.add(f"🎯 CRF <b>{crf} ({esc(crf_tier)})</b> — <i>{esc(crf_desc)}</i>")
        d.add(f"🎨 Pixel <code>{esc(pf)}</code> <b>({esc(pf_spec)})</b> — <i>{esc(pf_desc)}</i>")
        d.add(f"🎚 Preset <code>{esc(s['c_preset'])}</code> — <i>{esc(PRESET_INFO[s['c_preset']])}</i>")
        d.sec("🧬", "x265")
        d.add("🧬 <code>B-Frames:%s · SAO:%s · LA:%s · AQ:%s</code>" % (
            "ON" if s["c_bframes"] else "OFF", "ON" if s["c_sao"] else "OFF",
            "ON" if s["c_lookahead"] else "OFF", "ON" if s["c_aq"] else "OFF"))
        d.sec("🚀", "Pipeline")
        d.extend(_pipeline_rows(s, compress=True))
        d.add(_cloud_line())
        d.add(note("Original resolution එලෙසම තබා H.264 ➜ H.265 ලෙස size අඩු කරයි."))
        return d.build(tail=note("මාරු කරන්න mode / preset / CRF buttons භාවිත කරන්න."))

    d = Doc()
    d.head("⚙️", "ENCODE SETTINGS", "📐 resolution · live")
    d.quote([
        "📐 <b>Resolution Mode</b> · 1080p → 144p",
        chips(s["preset"], _codec_short(s["codec"]), f"{s['workers']}w"),
        f"🌐 dist <code>{esc(str(s['dist']))}</code> · 📋 queue <code>{esc(_queue_lbl(s['qres']))}</code>",
    ])
    d.sec("🎛", "Encoder")
    d.add(f"🎚 Preset <code>{esc(s['preset'])}</code> — <i>{esc(PRESET_INFO[s['preset']])}</i>")
    d.add(f"🎞 Codec <code>{esc(s['codec'])}</code> — <i>{esc(CODECS.get(s['codec'], ''))}</i>")
    d.sec("🚀", "Pipeline")
    d.extend(_pipeline_rows(s, compress=False))
    d.add(_cloud_line())
    d.sec("📝", "Notes")
    d.add(note("ultrafast → වේගවත්ම · veryslow → අඩුම size"))
    d.add(note("H.265 → size අඩුයි, encode වෙන්න වැඩි වෙලාවක් යනවා"))
    return d.build(tail=note("Download එක යන අතරේත් workers වෙනස් කරන්න පුළුවන්."))


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
def _relay_short() -> str:
    return f"ON ×{len(relay.ready)}" if relay.enabled else "OFF"


def _codec_short(codec: str) -> str:
    return {"libx264": "H.264", "libx265": "H.265"}.get(str(codec), str(codec))


def _queue_lbl(q) -> str:
    q = str(q)
    return "ask (buttons)" if q == "ask" else ("smart" if q == "smart" else f"{q}p")


@client.on(events.NewMessage(pattern=r"^/(start|help)", incoming=True))
async def cmd_start(event):
    if not allowed(event.sender_id):
        return
    await reply_card(event, home_card(
        name="VIDEO ENCODER",
        subtitle="premium HTML UI",
        headline=[
            "⚡ Turbo download → encode → auto upload",
            f"👤 Relay <b>{_relay_short()}</b> · 🌐 Runners <b>{dist.capacity()}</b>",
            f"🗂 Accounts <b>{1 + len(dist.HELPERS)}</b> · ☁️ Settings auto-save",
        ],
        modes=[
            ("📐 <b>Resolution Mode</b>", "1080p → 144p"),
            ("🗜 <b>Size-Reduce Mode</b>", "H.265 lossless"),
        ],
        commands=[
            ("⚙️ /settings", "mode · preset · CRF"),
            ("📋 /queue", "run + waiting"),
            ("🩺 /check", "setup health"),
            ("🌐 /workflow", "Actions runs"),
            ("🧹 /clear", "queue ඉවත් කිරීම"),
            ("🛑 /End_Work", "workers kill"),
        ],
        tip="Video එකක් එවන්න — preview එකෙන් තෝරන්න.",
        footer=note("HTML widgets · mobile + desktop · safe fallbacks"),
    ))


@client.on(events.NewMessage(pattern=r"^/settings", incoming=True))
async def cmd_settings(event):
    if not allowed(event.sender_id):
        return
    await reply_card(event, settings_text(event.sender_id), buttons=settings_buttons(event.sender_id, "0"))


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


def _cloud_line() -> str:
    """`💾 Settings save: …` without the duplicated cloud emoji."""
    txt = store.cloud_status_text()
    for emoji in ("💾", "☁️"):
        if txt.startswith(emoji):
            txt = txt[len(emoji):].lstrip()
            break
    return f"💾 Settings save: {txt}"


def check_text() -> str:
    """Premium health card: userbot lanes + helper accounts + runner pool."""
    st = relay.status_rows()
    h = dist.helpers_rows()
    helper_rows = [f"✅ <code>{esc(r)}</code>" for r in h["ok"]]
    helper_rows += [f"❌ <code>{esc(p[:110])}</code>" for p in h["problems"]]
    helper_summary = f"🤝 Helpers <b>{h['total']}/{h['configured']}</b> · 👷 capacity <b>{h['capacity']}</b>"
    if dist.available():
        extra = [f"🌐 Distributed <b>ON</b> · 👷 <b>{h['capacity']}</b> workers",
                 f"🗂 Accounts <b>{1 + h['total']}</b> · 🧩 {config.WORKERS_PER_ACCOUNT} workers / account"]
    else:
        extra = ["🌐 <b>Distributed OFF</b>",
                 "👷 Single runner mode — GitHub CLI / token / repo නෑ"]
    return health_card(
        relay_on=bool(st["on"]), relay_summary=st["summary"], relay_rows=st["rows"],
        helper_rows=helper_rows, helper_summary=helper_summary, extra=extra,
        footer=note("Setup වැරදි නම් fallback: bot එකෙන්ම download/upload — jobs fail වෙන්නේ නෑ."),
    )


@client.on(events.NewMessage(pattern=r"^/check", incoming=True))
async def cmd_check(event):
    """Userbots + private channel setup එක ආයෙ පරීක්ෂා කරනවා (job එකක් run වෙන්නේ නැති වෙලාවක)."""
    if not allowed(event.sender_id):
        return
    if pending_count():
        await reply_card(event, event_card(
            icon="⚠️", title_text="CHECK PAUSED",
            lines=[f"Jobs <b>{pending_count()}</b> ක් run / queue වෙනවා",
                   "Setup re-scan එකක් run කරන්නේ නෑ — jobs ට බලපෑමක් නෑ"],
            hint="Queue එක හිස් වුණාම /check කරන්න.",
        ))
        return
    m = await reply_card(event, event_card(
        icon="🔍", title_text="SYSTEM CHECK",
        lines=["Userbots · channel · runner pool පරීක්ෂා කරනවා…"],
    ))
    await relay.stop()
    relay.userbots = []
    await relay.setup(client)
    await init_helpers(notify=False)
    await edit_card(m, check_text())


# ───────────────────────── 🌐 /workflow  &  🛑 /End_Work ─────────────────────────
_STATUS_ICON = {"in_progress": "▶️", "queued": "⏳", "waiting": "⏳", "requested": "⏳", "pending": "⏳"}
end_work_lock = asyncio.Lock()


def _age(iso: str) -> str:
    try:
        t = datetime.fromisoformat((iso or "").replace("Z", "+00:00"))
        sec = max(0, int((datetime.now(timezone.utc) - t).total_seconds()))
    except Exception:
        return "?"
    return human_age(sec)


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
        await reply_card(event, toast_card(
            icon="🌐", title_text="WORKERS UNAVAILABLE",
            lines=["<code>gh</code> CLI · <code>GH_TOKEN</code> · <code>GITHUB_REPOSITORY</code> නෑ"],
            hint="Jobs single-runner mode එකෙන් run වෙයි — /check එකෙන් තහවුරු කරන්න.",
        ))
        return
    m = await reply_card(event, event_card(
        icon="🔍", title_text="WORKER WORKFLOWS",
        lines=["GitHub Actions runs + worker parts බලනවා…"],
    ))
    cur = current_job[0]
    cfg = store.get(event.sender_id)
    act_wrk = cfg["c_workers"] if cfg["mode"] == "compress" else cfg["workers"]
    act_dst = cfg["c_dist"] if cfg["mode"] == "compress" else cfg["dist"]
    try:
        info = await pool.overview()
    except Exception as e:
        await edit_card(m, failed_card(name="pool overview", error=str(e)[:180],
                                       hint="GitHub token / network අවුලක්. /End_Work එකෙන් workers kill කරන්න පුළුවන්."))
        return

    total_runs = total_workers = 0
    d = Doc()
    d.head("🌐", "WORKER WORKFLOWS", "live")
    d.quote([
        f"⚙️ Mode <b>{'🗜 Size-Reduce' if cfg['mode'] == 'compress' else '📐 Resolution'}</b> · "
        f"👷 <b>{act_wrk}</b> · 🌐 dist <b>{esc(str(act_dst))}</b>",
        f"🧠 Pool <b>{'ACTIVE' if pool.active else 'idle'}</b>"
        + (" · 🛑 halted (/End_Work)" if pool.halted else "") + f" · 🧮 capacity <b>{dist.capacity()}</b>",
        f"📋 Queue <b>{len(queue)}</b> waiting" + (f" · ▶️ <code>{esc(clip(cur.meta.name, 24))}</code>" if cur else ""),
    ])
    for pos, a in enumerate(info):
        runs = a["runs"]
        total_runs += len(runs)
        rows = []
        sess = f"<code>{esc(a['session'])}</code>" if a["session"] else "<i>no session</i>"
        rows.append(f"🔑 {sess} · dispatched <b>{len(a['dispatched'])}</b> [{_parts_str(a['dispatched'])}]")
        if a["error"]:
            rows.append(f"❌ <code>{esc(clip(a['error'], 110))}</code>")
        elif a["failed"]:
            rows.append("⚠️ dispatch failed — token / permission බලන්න")
        elif not runs:
            rows.append(note("Active workflow runs නෑ — pool idle"))
        for r in runs[:6]:
            jobs = r.get("jobs", [])
            run_n = sum(1 for j in jobs if j.get("status") == "in_progress")
            que_n = sum(1 for j in jobs if j.get("status") in ("queued", "waiting", "pending", "requested"))
            total_workers += run_n
            idx = [int(x) for j in jobs for x in re.findall(r"\((\d+)\)", j.get("name", ""))]
            rows.append(
                f"{_STATUS_ICON.get(r['status'], '•')} <code>#{r['databaseId']}</code> "
                f"<b>{esc(str(r['status']).replace('_', ' '))}</b> · {_age(r.get('createdAt'))}\n"
                f"    👷 run <b>{run_n}</b> · ⏳ queue <b>{que_n}</b> · 🧩 [{_parts_str(idx)}]"
            )
        if len(runs) > 6:
            rows.append(note(f"… තව runs {len(runs) - 6} ක්"))
        if pos:
            d.gap()
        d.extend(workflow_account(label=_acct_label(a["idx"]), repo=a["repo"], rows=rows))
    d.add(f"📊 Active runs <b>{total_runs}</b> · 👷 Running workers <b>{total_workers}</b>")
    d.add(note("🛑 සේරම kill කරන්න: /End_Work"))
    await edit_card(m, d.build())


@client.on(events.NewMessage(pattern=r"(?i)^/end_?work(@\w+)?(\s|$)", incoming=True))
async def cmd_end_work(event):
    """Main + helper accounts වල run වෙන සේරම worker workflows kill (kill request 5 ක් දක්වා, kill වෙනකන්)."""
    if not allowed(event.sender_id):
        return
    if not dist.available():
        await reply_card(event, toast_card(
            icon="🌐", title_text="WORKERS UNAVAILABLE",
            lines=["Kill කරන්න distributed workers නෑ"],
            hint="Jobs single-runner mode එකෙන් run වෙනවා.",
        ))
        return
    if end_work_lock.locked():
        await reply_card(event, event_card(
            icon="⏳", title_text="END WORK RUNNING",
            lines=["දැනටමත් kill request යවනවා…"],
            hint="තත්පර කීපයක් ඉඳලා /workflow බලන්න.",
        ))
        return
    async with end_work_lock:
        m = await reply_card(event, event_card(
            icon="🛑", title_text="END WORK",
            lines=["Main + helper accounts වල workers kill කරනවා…"],
        ))
        live = Live(m)
        busy = pending_count() > 0

        async def prog(rnd, total, force, counts):
            pairs = [(_acct_label(i), n) for i, n in counts.items() if n]
            await live.set(endwork_progress(round_no=rnd, total=total, counts=pairs), force=True)

        try:
            res = await pool.kill_all(rounds=5, on_progress=prog)
        except Exception as e:
            logger.exception("end_work failed")
            await edit_card(m, failed_card(name="End Work", error=str(e)[:180],
                                           hint="GitHub token permissions (actions: write) බලන්න."))
            return
        if pending_count() == 0:
            pool.halted = False            # job එකක් නැත්නම් ඊළඟ queue එකට workers ආයෙ allow
        left = sum(len(v) for v in res["left"].values())
        accounts = [{
            "label": _acct_label(i),
            "repo": repo,
            "killed": res["cancelled"].get(i, 0),
            "left": len(res["left"].get(i, [])),
        } for i, repo in res["accts"]]
        await edit_card(m, endwork_result(accounts=accounts, left_total=left,
                                          rounds=res["rounds"], busy=busy))


def queue_text() -> str:
    cur = current_job[0]
    running = None
    if cur:
        running = {"name": cur.meta.name, "badge": job_target_badge(cur)}
    items = [{"name": j.meta.name, "badge": job_target_badge(j)} for j in queue]
    return queue_card(running=running, items=items, cap=QUEUE_MAX)


@client.on(events.NewMessage(pattern=r"^/queue", incoming=True))
async def cmd_queue(event):
    if allowed(event.sender_id):
        await reply_card(event, queue_text())


@client.on(events.NewMessage(pattern=r"^/clear", incoming=True))
async def cmd_clear(event):
    if not allowed(event.sender_id):
        return
    mine = [j for j in queue if j.user_id == event.sender_id]
    for j in mine:
        await cancel_waiting(j)
    await reply_card(event, toast_card(
        icon="🧹", title_text="QUEUE CLEARED",
        lines=[f"Queue එකෙන් <b>{len(mine)}</b> ක් ඉවත් කළා",
               f"📋 ඉතුරු queue <b>{len(queue)}</b> · ▶️ running <b>{1 if current_job[0] else 0}</b>"],
        hint="Running job එක නවත්තන්න ඒකේ ❌ Cancel ඔබන්න.",
    ))


# ───────────────────────── queue ─────────────────────────
def pending_count() -> int:
    return len(queue) + (1 if current_job[0] else 0)


async def cancel_waiting(job: Job):
    job.cancelled = True
    with contextlib.suppress(ValueError):
        queue.remove(job)
    jobs.pop(job.id, None)
    with contextlib.suppress(Exception):
        await job.status.edit(cancelled_card(name=getattr(job.meta, "name", ""),
                                             note_text="Queue එකෙන් ඉවත් කළා"), buttons=None)
    await refresh_positions()


_refresh_pos_task: Optional[asyncio.Task] = None
_refresh_pos_dirty: bool = False


async def _refresh_positions_inner():
    global _refresh_pos_dirty
    while _refresh_pos_dirty:
        await asyncio.sleep(0.8)
        _refresh_pos_dirty = False
        snap = list(queue)
        total = len(snap)
        for i, j in enumerate(snap, 1):
            if j.live and not j.cancelled and j in queue:
                ahead = [(k, snap[k - 1].meta.name, job_target_badge(snap[k - 1]))
                         for k in range(1, i)]
                with contextlib.suppress(Exception):
                    await j.live.set(
                        queued_card(index=i, total=total, name=j.meta.name,
                                    badge=job_target_badge(j), ahead=ahead),
                        force=True,
                    )


async def refresh_positions():
    """Coalesce burst queue changes so batch-forwarding 10+ videos doesn't fire O(N²) edits."""
    global _refresh_pos_task, _refresh_pos_dirty
    _refresh_pos_dirty = True
    if _refresh_pos_task is None or _refresh_pos_task.done():
        _refresh_pos_task = asyncio.create_task(_refresh_positions_inner())


async def enqueue(job: Job, height: int, *, mode: str = "res") -> bool:
    global queue_task
    if pending_count() >= QUEUE_MAX:
        with contextlib.suppress(Exception):
            await job.status.edit(toast_card(
            icon="🚫", title_text="QUEUE FULL",
            lines=[f"Queue එකේ <b>{QUEUE_MAX}/{QUEUE_MAX}</b> jobs ඉන්නවා",
                   "ටිකක් ඉඳලා video එක ආයෙ එවන්න"],
            hint="Queue එක හිස් කරන්න /clear · ඉවර වෙනකන් /queue බලන්න.",
        ), buttons=None)
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
    status = await reply_card(event, event_card(
        icon="🔎", title_text="ANALYZING",
        lines=["Metadata + endpoint probe කරනවා…"],
        hint="Video එකේ codec, resolution, audio තොරතුරු ලබා ගන්නවා.",
    ))
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
                await job.status.edit(cancelled_card(
                    name=getattr(job.meta, "name", ""),
                    note_text="Preview එකෙන් cancel කළා — encode එකක් ආරම්භ වුණේ නෑ"), buttons=None)

    elif action == "cfg":
        await safe_answer(event)
        with contextlib.suppress(MessageNotModifiedError):
            await edit_card(event, settings_text(uid), buttons=settings_buttons(uid, job_id))

    elif action == "sub":
        if arg not in ("crf", "pix", "cpre"):
            return
        await safe_answer(event)
        with contextlib.suppress(MessageNotModifiedError):
            await edit_card(event, settings_text(uid, sub=arg), buttons=settings_buttons(uid, job_id, sub=arg))

    elif action == "mod":
        if arg not in ENCODE_MODES:
            return
        store.set(uid, mode=arg)
        lbl = "🗜 Size-Reduce (H.265)" if arg == "compress" else "📐 Resolution Mode"
        await safe_answer(event, f"{lbl} ✔")
        with contextlib.suppress(MessageNotModifiedError):
            await edit_card(event, settings_text(uid), buttons=settings_buttons(uid, job_id))

    elif action == "ccrf":
        c_val = int(arg)
        if c_val not in CRF_CHOICES:
            return
        store.set(uid, c_crf=c_val)
        await safe_answer(event, f"CRF {c_val} ({CRF_INFO[c_val][0]}) ✔")
        with contextlib.suppress(MessageNotModifiedError):
            await edit_card(event, settings_text(uid, sub="crf"), buttons=settings_buttons(uid, job_id, sub="crf"))

    elif action == "cpix":
        if arg not in PIX_FMT_CHOICES:
            return
        store.set(uid, c_pix_fmt=arg)
        await safe_answer(event, f"{arg} ✔")
        with contextlib.suppress(MessageNotModifiedError):
            await edit_card(event, settings_text(uid, sub="pix"), buttons=settings_buttons(uid, job_id, sub="pix"))

    elif action == "cpre":
        if arg not in PRESETS:
            return
        store.set(uid, c_preset=arg)
        await safe_answer(event, f"Preset: {arg} ✔")
        with contextlib.suppress(MessageNotModifiedError):
            await edit_card(event, settings_text(uid, sub="cpre"), buttons=settings_buttons(uid, job_id, sub="cpre"))

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
            await edit_card(event, settings_text(uid, sub=sub_name), buttons=settings_buttons(uid, job_id, sub=sub_name))

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
            await edit_card(event, settings_text(uid), buttons=settings_buttons(uid, job_id))

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
            await edit_card(event, settings_text(uid), buttons=settings_buttons(uid, job_id))

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
                await live.set(event_card(
                    icon="📨", title_text="RELAY",
                    lines=["Video එක private channel එකට forward කරනවා…",
                           f"👤 Lanes <b>{len(relay.ready)}</b> · ⚡ warm pools සූදානම් කරනවා"],
                    hint="Userbots එකවර download කරන නිසා speed වැඩියි.",
                ), force=True)
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
                await live.set(event_card(
                    icon="⚠️", title_text="RELAY FALLBACK",
                    lines=["Userbot download fail — <b>bot</b> එකෙන් download කරනවා",
                           "Job එක fail වෙන්නේ නෑ"],
                ), force=True)
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
        if use_relay:
            relay.trigger_upload_warmup()
        await live.set(event_card(
            icon="🔍", title_text="PROBING FILE",
            lines=[f"📄 <code>{esc(clip(meta.name, 40))}</code>",
                   "Real resolution · fps · audio stream තහවුරු කරනවා…"],
        ), force=True)
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

            tick_iv = max(PROGRESS_UPDATE_INTERVAL, getattr(config, "MIN_EDIT_INTERVAL", 3.8))

            async def dist_ticker():
                while True:
                    await live.set(
                        render_dist(
                            dst_state, meta.name, height, preset, codec,
                            target_label=t_label, extra_info=extra_lbl,
                        )
                    )
                    await asyncio.sleep(tick_iv)

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
                await live.set(failed_card(
                    name=meta.name, error=str(e),
                    hint="Worker pool එකෙන් part එකක් complete කරන්න බැරි වුණා · main-runner fallback disabled. /workflow එකෙන් runners බලන්න.",
                ), force=True)
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
        if use_relay:
            relay.trigger_upload_warmup()
        ul_badge = "⚡ UB" if use_relay else ""
        tr2 = SpeedTracker(total=out_size)
        await live.set(render_transfer("ul", tr2, dst.name, ul_badge), force=True)

        async def ul_cb(cur, tot):
            if phase[0] != "ul":
                return
            tr2.update(cur, tot)
            await live.set(render_transfer("ul", tr2, dst.name, ul_badge))

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
                await live.set(event_card(
                    icon="📨", title_text="DELIVERING",
                    lines=["Encoded video එක chat එකට යවනවා…",
                           "Re-upload නෑ — channel message එකම forward වේ"],
                ), force=True)
                sent = await relay.deliver(client, job.chat_id, ch_msg.id, caption)  # main bot → user (re-upload නෑ)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if job.cancelled:
                    raise asyncio.CancelledError()
                logger.warning(f"relay upload/deliver failed ({e!r}) → bot upload")
                await live.set(event_card(
                    icon="⚠️", title_text="RELAY FALLBACK",
                    lines=["Userbot upload fail — <b>bot</b> එකෙන් upload කරනවා",
                           "Job එක fail වෙන්නේ නෑ"],
                ), force=True)
                ul_badge = ""
                tr2 = SpeedTracker(total=out_size)
                sent = None
        if sent is None:
            sent = await ul.upload_file(client, job.chat_id, str(dst), job.id, ul_cb)
            stats["ul_avg"], stats["ul_time"] = tr2.avg_speed, tr2.elapsed
            with contextlib.suppress(Exception):
                await client.edit_message(sent, caption)
        phase[0] = "done"

        saved = (1 - out_size / meta.size) * 100 if meta.size else 0
        via = stats.get("via") or []
        stat_rows = [
            ("DL", f"{human_size_short(stats.get('dl_avg', 0))}/s",
             human_time(stats.get("dl_time", 0)), f"x{len(via)}" if via else ""),
            ("ENC", speed_txt(stats.get("enc_speed", 0)),
             human_time(stats.get("enc_time", 0)),
             f"x{stats['workers']}" if "workers" in stats else ""),
            ("UP", f"{human_size_short(stats.get('ul_avg', 0))}/s",
             human_time(stats.get("ul_time", 0)), stats.get("ul_via", "")),
            ("TOTAL", "", human_time(time.monotonic() - t_start), ""),
        ]
        extras = []
        if "workers" in stats:
            extras.append(f"👷 workers <b>{stats['workers']}</b>")
        if stats.get("reassigned_parts"):
            extras.append(f"♻️ reassigned <b>{stats['reassigned_parts']}</b>")
        if stats.get("local_parts"):
            extras.append(f"🏠 local parts <b>{stats['local_parts']}</b>")
        if est_size:
            extras.append(f"🎯 est <code>≈{human_size_short(est_size)}</code>")
        live.buttons = None
        await live.set(done_card(
            name=dst.name,
            before=human_size(meta.size), after=human_size(out_size),
            saved_pct=saved if meta.size else None,
            stats=stat_rows, extras=extras,
            footer=note("Video එක upload වුණා · /queue ඊළඟ job එක බලන්න."),
        ), force=True)

    except asyncio.CancelledError:
        phase[0] = "stop"
        live.buttons = None
        await live.set(cancelled_card(name=meta.name), force=True)
    except Exception as e:
        phase[0] = "stop"
        logger.exception("job failed")
        live.buttons = None
        await live.set(failed_card(name=meta.name, error=str(e)[-700:]), force=True)
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
    await init_upload_engine()
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
        await close_upload_engine()
        await close_download_engine()


if __name__ == "__main__":
    client.loop.run_until_complete(main())
