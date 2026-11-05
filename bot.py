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

from telethon import TelegramClient, events, Button
from telethon.errors import MessageNotModifiedError, FloodWaitError, QueryIdInvalidError

import config
from config import (API_ID, API_HASH, BOT_TOKEN, SESSION_NAME, ALLOWED_USERS,
                    PROGRESS_UPDATE_INTERVAL, DOWNLOAD_DIR, PRESETS, CODECS,
                    PRESET_INFO, CRF, AUDIO_BITRATE, MAX_UPLOAD_BYTES,
                    WORKER_CHOICES, QUEUE_MAX, QUEUE_RES_CHOICES)
from fast_downloader import get_downloader, init_download_engine
from fast_uploader import get_uploader
from userbots import Relay
from media import (VideoMeta, telegram_meta, probe_remote, ffprobe, apply_probe,
                   offered_targets, estimate_size, render_preview, short_side, guard_kbps,
                   resolve_preset, resolve_crf)
from settings_store import SettingsStore
from transcoder import EncState, transcode
import dist
from ui import (SpeedTracker, render_transfer, render_encode, human_size,
                human_size_short, human_time, human_speed, esc, render_dist)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s: %(message)s")
logger = logging.getLogger("EncoderBot")

client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
client.parse_mode = "html"
store = SettingsStore()
pool = dist.WorkerPool()
relay = Relay()                         # userbots ↔ private channel
queue: deque = deque()                  # බලාගෙන ඉන්න jobs (FIFO)
current_job = [None]                    # දැන් run වෙන job එක
queue_task: Optional[asyncio.Task] = None
queue_paused = False
queue_resume = asyncio.Event()
queue_resume.set()
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
    queued: bool = False
    priority: int = 0
    live: object = None
    resume_checkpoint: object = None
    completed: bool = False
    dist_state: object = None
    dist_pool: object = None


jobs = {}
queue_state_lock = asyncio.Lock()
_restoring_queue = False


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
def preview_buttons(job: Job):
    cfg = store.get(job.user_id)
    row = []
    for t in offered_targets(job.meta):
        est = estimate_size(job.meta, t, cfg["preset"], cfg["codec"])
        label = f"{t}p ≈{human_size_short(est)}" if est else f"{t}p"
        row.append(Button.inline(label, cb("res", job.id, t)))
    return [row, [Button.inline("⚙️ Settings", cb("cfg", job.id)),
                  Button.inline("❌ Cancel", cb("x", job.id))]]


async def show_preview(job: Job):
    cfg = store.get(job.user_id)
    await job.status.edit(render_preview(job.meta, cfg["preset"], cfg["codec"]),
                          buttons=preview_buttons(job))


def settings_text(uid: int) -> str:
    s = store.get(uid)
    return "\n".join([
        "⚙️ <b>Encoding Settings</b>",
        "━━━━━━━━━━━━━━━━━━━━",
        f"🎚 Preset: <b>{s['preset']}</b>",
        f"   <i>{PRESET_INFO[s['preset']]}</i>",
        f"🎞 Codec: <b>{s['codec']}</b>",
        f"🌐 Distributed (GitHub workers): <b>{s['dist']}</b>"
        + ("" if dist.available() else "  <i>(මේ environment එකේ නෑ)</i>"),
        f"👷 Workers: <b>{s['workers']}</b>  <i>({min(WORKER_CHOICES)} – {max(WORKER_CHOICES)}, accounts {1 + len(dist.HELPERS)} × {config.WORKERS_PER_ACCOUNT}; download එක යන අතරේත් වෙනස් කරන්න පුළුවන්)</i>",
        f"👤 Userbot relay: <b>{('ON ×' + str(len(relay.ready))) if relay.enabled else 'OFF'}</b>",
        f"📋 Auto-queue: <b>{'ask (බොත්තම්)' if s['qres'] == 'ask' else '🧠 smart' if s['qres'] == 'smart' else s['qres'] + 'p'}</b>",
        "━━━━━━━━━━━━━━━━━━━━",
        "ultrafast → වේගවත්ම  |  veryslow → අඩුම size",
        "H.265 → size අඩුයි, encode වෙන්න වැඩි වෙලාවක් යනවා",
    ])


def settings_buttons(uid: int, job_id: str):
    s = store.get(uid)
    rows, row = [], []
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
        "🎬 Video එකක් එවන්න → metadata preview එක පෙනෙයි\n"
        "📐 Source එකට අඩු resolutions (1080p…144p) තෝරන්න (≈ size පෙන්වනවා) • Settings → 🧠 Smart = auto හොඳම එක\n"
        "⚡ High-speed download → FFmpeg encode → High-speed upload\n\n"
        "📋 Videos 20 ක් දක්වා queue කරන්න පුළුවන් — එකින් එක download → encode → upload\n"
        "⚙️ Preset / Codec / Workers: /settings   •   📋 /queue   •   🧹 /clear\n"
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


@client.on(events.NewMessage(pattern=r"(?i)^/(workflows?|workers)(@\w+)?(\s|$)", incoming=True))
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
    head = [
        "🌐 <b>Worker Workflows</b>",
        "━━━━━━━━━━━━━━━━━━━━",
        f"⚙️ Settings workers: <b>{cfg['workers']}</b> • Dist mode: <b>{esc(str(cfg['dist']))}</b> • Capacity: <b>{dist.capacity()}</b>",
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


@client.on(events.NewMessage(pattern=r"(?i)^/workerstats(@\w+)?(\s|$)", incoming=True))
async def cmd_workerstats(event):
    if not allowed(event.sender_id):
        return
    job = current_job[0]
    if not job or not job.dist_state:
        await event.reply("📊 <b>Worker stats</b>\nදැනට distributed encode job එකක් run වෙන්නේ නෑ.")
        return
    st = job.dist_state
    active = sum(1 for p in st.parts if p.status in ("running", "uploading"))
    held = sum(1 for p in st.parts if p.status == "held")
    done = sum(1 for p in st.parts if p.ready)
    lines = ["📊 <b>Live Worker Statistics</b>", f"🎬 <code>{esc(job.meta.name[:45])}</code>",
             f"👷 Active <b>{active}</b> • Held <b>{held}</b> • Completed <b>{done}/{len(st.parts)}</b>",
             f"🚀 Aggregate <b>{st.fps:.0f} FPS</b> • <b>{st.speed:.2f}x</b>",
             f"♻️ Reassigned <b>{getattr(st, 'reassigned_parts', 0)}</b>", ""]
    for p in st.parts[:24]:
        state = "✅ done" if p.ready else p.status
        lines.append(f"Part {p.idx + 1}: <b>{esc(state)}</b> • worker {p.runner_idx} / acct {p.acct}"
                     + (f" • {p.fps:.0f} FPS • {p.speed:.2f}x" if p.fps or p.speed else ""))
    if len(st.parts) > 24:
        lines.append(f"… තවත් parts {len(st.parts) - 24} ක්")
    if job.dist_pool:
        scores = [(a.idx, wi, score) for a in job.dist_pool.accts
                  for wi, score in a.worker_scores.items()]
        if scores:
            lines += ["", "<b>Best observed workers</b>"]
            for ai, wi, score in sorted(scores, key=lambda x: x[2], reverse=True)[:8]:
                lines.append(f"• Account {ai} / worker {wi}: <b>{score:.0f}</b> score")
    await event.reply("\n".join(lines)[:4000])


@client.on(events.NewMessage(pattern=r"(?i)^/health(@\w+)?(\s|$)", incoming=True))
async def cmd_health(event):
    if not allowed(event.sender_id):
        return
    job = current_job[0]
    lines = ["🩺 <b>Encoder System Health</b>",
             f"📋 Queue: <b>{len(queue)}</b> waiting • {'⏸ paused' if queue_paused else '▶️ active'}",
             f"🎬 Current job: <b>{'yes' if job else 'no'}</b>",
             f"🌐 GitHub workers: <b>{'available' if dist.available() else 'unavailable'}</b> • capacity <b>{dist.capacity() if dist.available() else 0}</b>",
             f"👷 Pool: <b>{'active' if pool.active else 'idle'}</b>{' • halted' if pool.halted else ''}",
             f"👤 Userbot relay: <b>{'ready' if relay.enabled and relay.ready else 'off / fallback'}</b>",
             f"💾 JSON queue persistence: <b>{'available' if dist.available() else 'unavailable'}</b>"]
    if job and job.dist_state:
        st = job.dist_state
        lines.append(f"📈 Encode: <b>{st.percent:.1f}%</b> • <b>{st.fps:.0f} FPS</b> • <b>{st.speed:.2f}x</b>")
    await event.reply("\n".join(lines))


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
        if current_job[0]:
            current_job[0].cancelled = True
            get_downloader().cancel(current_job[0].id)
            get_uploader().cancel(current_job[0].id)
            if current_job[0].proc:
                with contextlib.suppress(Exception):
                    current_job[0].proc.kill()
        await persist_queue_state()

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
        rows.append(f"▶️ <b>Running</b>: <code>{esc(cur.meta.name[:40])}</code> → {cur.height}p")
    for i, j in enumerate(queue, 1):
        rows.append(f"{i}. <code>{esc(j.meta.name[:40])}</code> → {j.height}p")
    if queue_paused:
        rows.insert(0, "⏸ <b>Queue paused</b>")
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


@client.on(events.NewMessage(pattern=r"(?i)^/pause(@\w+)?(\s|$)", incoming=True))
async def cmd_pause(event):
    global queue_paused
    if not allowed(event.sender_id):
        return
    queue_paused = True
    queue_resume.clear()
    await event.reply("⏸ <b>Queue paused</b> — දැනට run වෙන video එක ඉවර වෙනවා; ඊළඟ job එක start වෙන්නේ /resume කළාම.")


@client.on(events.NewMessage(pattern=r"(?i)^/resume(@\w+)?(\s|$)", incoming=True))
async def cmd_resume(event):
    global queue_paused
    if not allowed(event.sender_id):
        return
    queue_paused = False
    queue_resume.set()
    await event.reply("▶️ <b>Queue resumed</b> — ඊළඟ job එක start කරනවා.")


@client.on(events.NewMessage(pattern=r"(?i)^/cancel(@\w+)?(\s|$)", incoming=True))
async def cmd_cancel(event):
    if not allowed(event.sender_id):
        return
    job = current_job[0]
    if not job or job.user_id != event.sender_id:
        await event.reply("දැනට cancel කළ හැකි running job එකක් නැහැ.")
        return
    job.cancelled = True
    get_downloader().cancel(job.id)
    get_uploader().cancel(job.id)
    if job.proc:
        with contextlib.suppress(Exception):
            job.proc.kill()
    await persist_queue_state()
    await event.reply("🛑 Current job එක cancel කරනවා. එයට assign කළ workers පමණක් stop/release කරනවා.")


# ───────────────────────── queue ─────────────────────────
def pending_count() -> int:
    return len(queue) + (1 if current_job[0] else 0)


async def _queue_record(job: Job) -> dict:
    return {
        "id": job.id, "user_id": int(job.user_id), "chat_id": int(job.chat_id),
        "message_id": int(getattr(job.message, "id", 0) or 0),
        "status_message_id": int(getattr(job.status, "id", 0) or 0),
        "height": int(job.height), "priority": int(getattr(job, "priority", 0) or 0),
        "cancelled": bool(job.cancelled),
        "completed": bool(job.completed), "source_name": getattr(job.meta, "name", ""),
        "queued_at": int(time.time()),
    }


async def persist_queue_state():
    """Persist the active and waiting queue as a small JSON Release asset (no DB)."""
    if _restoring_queue or not dist.available():
        return
    async with queue_state_lock:
        cur = current_job[0]
        current = await _queue_record(cur) if cur and not cur.cancelled and not cur.completed else None
        waiting = [await _queue_record(j) for j in list(queue)
                   if not j.cancelled and not j.completed]
        payload = {"version": 1, "updated_at": int(time.time()), "paused": bool(queue_paused),
                   "current": current, "queued": waiting}
        await dist.save_queue_snapshot(payload)


async def cancel_waiting(job: Job):
    job.cancelled = True
    with contextlib.suppress(ValueError):
        queue.remove(job)
    jobs.pop(job.id, None)
    with contextlib.suppress(Exception):
        await job.status.edit("❌ <b>Cancelled</b>", buttons=None)
    await refresh_positions()
    await persist_queue_state()


async def refresh_positions():
    for i, j in enumerate(list(queue), 1):
        if j.live:
            await j.live.set(f"⏳ <b>Queued #{i}</b> / {len(queue)}\n<code>{esc(j.meta.name[:50])}</code> → {j.height}p\n"
                             "ඉස්සරහ jobs ඉවර වුණාම ස්වයංක්‍රීයව පටන් ගන්නවා.", force=True)


def _rebalance_queue_fairness():
    """Priority first, then round-robin by sender while preserving per-user FIFO."""
    by_priority = {}
    for item in queue:
        priority = int(getattr(item, "priority", 0) or 0)
        per_user, order = by_priority.setdefault(priority, ({}, []))
        if item.user_id not in per_user:
            per_user[item.user_id] = deque()
            order.append(item.user_id)
        per_user[item.user_id].append(item)
    queue.clear()
    for priority in sorted(by_priority, reverse=True):
        per_user, order = by_priority[priority]
        while any(per_user.values()):
            for uid in order:
                if per_user[uid]:
                    queue.append(per_user[uid].popleft())


async def enqueue(job: Job, height: int) -> bool:
    global queue_task
    if pending_count() >= QUEUE_MAX:
        with contextlib.suppress(Exception):
            await job.status.edit(f"🚫 Queue පිරිලා ({QUEUE_MAX}/{QUEUE_MAX}). ටිකක් ඉඳලා ආයෙ එවන්න.", buttons=None)
        jobs.pop(job.id, None)
        return False
    user_pending = sum(1 for x in queue if x.user_id == job.user_id)
    if current_job[0] and current_job[0].user_id == job.user_id:
        user_pending += 1
    if user_pending >= config.QUEUE_USER_MAX:
        with contextlib.suppress(Exception):
            await job.status.edit(f"🚫 ඔයාගේ queue limit එක {config.QUEUE_USER_MAX} jobs. පරණ job එකක් ඉවර වූ පසු නැවත එවන්න.", buttons=None)
        jobs.pop(job.id, None)
        return False
    job.height, job.queued, job.started = height, True, True
    job.priority = 1 if job.user_id in config.QUEUE_PRIORITY_USERS else 0
    job.live = Live(job.status, [[Button.inline("❌ Cancel", cb("x", job.id))]])
    queue.append(job)
    _rebalance_queue_fairness()
    await refresh_positions()
    await persist_queue_state()
    if queue_task is None or queue_task.done():
        queue_task = asyncio.create_task(queue_loop())
    return True


async def queue_loop():
    """Jobs එකින් එක. Queue හිස් වුණාම workers pool එක නවත්තනවා (hold වෙලා හිටපු workers exit)."""
    global queue_task
    try:
        while True:
            while queue:
                await queue_resume.wait()
                job = queue.popleft()
                if job.cancelled:
                    continue
                current_job[0] = job
                await refresh_positions()
                await persist_queue_state()
                try:
                    await run_job(job, job.height)
                except Exception:
                    logger.exception("run_job crashed")
                finally:
                    current_job[0] = None
                    await persist_queue_state()
                # /End_Work cancels the active worker pool. The next queued job
                # starts a fresh session instead of inheriting the halted flag.
                if pool.halted and queue:
                    pool.halted = False
            with contextlib.suppress(Exception):
                await pool.stop()
            if not queue:           # stop අතරේ අලුත් job ආවොත් ආයෙ loop
                with contextlib.suppress(Exception):
                    await dist.clear_queue_snapshot()
                if not queue:       # clear_queue_snapshot() අතරතුර enqueue වුණාද බලන්න
                    queue_task = None
                    return
            await persist_queue_state()
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

    workdir = Path(DOWNLOAD_DIR) / ("probe_" + job.id)
    workdir.mkdir(parents=True, exist_ok=True)
    try:
        async with probe_sem:
            await probe_remote(client, msg, job.meta, str(workdir))
    finally:
        shutil.rmtree(workdir, ignore_errors=True)

    qres = store.get(job.user_id)["qres"]
    if qres != "ask":                       # auto-queue: source එකට වඩා ලොකු resolution නම් ළඟම එක
        opts = offered_targets(job.meta)
        if qres == "smart":                 # source එකට අඩුවෙන් තියෙන හොඳම (ලොකුම) resolution එක
            target = max(opts)
        else:
            want = int(qres)
            target = want if want in opts else min(opts, key=lambda t: abs(t - want))
        await enqueue(job, target)
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
        await enqueue(job, int(arg))              # queue පිරිලා නම් enqueue එකම status message එක edit කරනවා

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
        await persist_queue_state()
        await safe_answer(event, "Cancelling…")
        if not job.started:
            jobs.pop(job.id, None)
            with contextlib.suppress(Exception):
                await job.status.edit("❌ <b>Cancelled</b>", buttons=None)

    elif action == "cfg":
        await safe_answer(event, )
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
        await safe_answer(event, )
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
    cfg = store.get(job.user_id)
    preset, codec = cfg["preset"], cfg["codec"]
    resume = job.resume_checkpoint if isinstance(job.resume_checkpoint, dict) else {}
    saved_run_opts = resume.get("run_opts") if resume else None
    if isinstance(saved_run_opts, dict) and int(saved_run_opts.get("target", -1)) == int(height):
        preset = str(saved_run_opts.get("preset", preset))
        codec = str(saved_run_opts.get("codec", codec))
    est_size = estimate_size(meta, height, preset, codec)

    workdir = Path(DOWNLOAD_DIR) / job.id
    workdir.mkdir(parents=True, exist_ok=True)
    stem = safe_stem(meta.name)
    ext = os.path.splitext(meta.name)[1] or ".mp4"
    src, dst = workdir / f"in_{stem}{ext}", workdir / f"{stem}_{height}p.mp4"

    phase = ["dl"]
    t_start = time.monotonic()
    stats = {}
    dl, ul = get_downloader(), get_uploader()
    ch_ids, ch_out = [], [None, None]       # channel messages (cleanup): bot ගේ ids, [userbot msg, userbot]

    try:
        if job.cancelled:
            raise asyncio.CancelledError()

        # 0) Workers ඉක්මන් start: download එක යන අතරේම runners ready කරන්න ─
        # On resume, revoke the previous session before dispatching any new
        # runners. DistRun performs that revocation before its own ensure().
        if (not resume and dist.should_distribute(meta, height, preset, codec, cfg["dist"])):
            async def _prestart():
                with contextlib.suppress(Exception):
                    requested = min(store.get(job.user_id)["workers"], dist.capacity())
                    await pool.ensure(dist.worker_budget(meta.duration, requested))
            prestart = asyncio.create_task(_prestart())

        # 1) DOWNLOAD ─────────────────────────────
        tr = SpeedTracker(total=meta.size)
        await live.set(render_transfer("dl", tr, meta.name), force=True)

        async def dl_cb(cur, tot):
            if phase[0] != "dl":
                return
            tr.update(cur, tot)
            await live.set(render_transfer("dl", tr, meta.name))

        use_relay, ch_in = relay.enabled and bool(relay.ready), None
        if use_relay:                                      # user ගේ video එක → private channel (userbots එතනින් ගන්නවා)
            try:
                await live.set("📨 <b>Channel එකට forward කරනවා…</b>", force=True)
                ch_in = await relay.forward_in(client, job.message)
                ch_ids.append(ch_in)
            except Exception as e:
                logger.warning(f"relay forward failed ({e!r}) → bot download")
                use_relay = False
            await live.set(render_transfer("dl", tr, meta.name), force=True)
        if use_relay:
            try:
                await relay.download(dl, ch_in, str(src), job.id, dl_cb)
                stats["via"] = [u.name for u in relay.ready]
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if job.cancelled:
                    raise asyncio.CancelledError()
                logger.warning(f"relay download failed ({e!r}) → bot download")
                await live.set("⚠️ Userbot download fail → bot එකෙන් download කරනවා…", force=True)
                src.unlink(missing_ok=True)
                tr = SpeedTracker(total=meta.size)
                use_relay = False
                stats.pop("via", None)
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

        # Auto profile uses the actual downloaded-file metadata, not Telegram's
        # approximate attributes, to choose a suitable preset and CRF.
        preset = resolve_preset(meta, height, codec, preset)
        crf = resolve_crf(meta, height, codec, CRF[codec])

        # 3) ENCODE ───────────────────────────────
        phase[0] = "enc"
        # Audio: source එකේ bitrate එකට වඩා වැඩි කරන්නේ නෑ; AAC & අඩු නම් copy
        ab = AUDIO_BITRATE.get(height, 96)
        src_ab = meta.abitrate // 1000
        ac = meta.acodec == "aac" and 0 < src_ab <= ab
        if src_ab and not ac:
            ab = min(ab, src_ab)
        enc_opts = dict(
            target=height, scale=short_side(meta) > height, portrait=meta.height > meta.width,
            preset=preset, codec=codec, crf=crf, maxrate_kbps=guard_kbps(meta, height),
            abitrate=ab, audio_mode="copy" if ac else "aac",
            subtitle_indexes=[s["index"] for s in meta.subtitle_streams
                              if s.get("codec") in ("subrip", "ass", "ssa", "mov_text", "webvtt", "text")],
        )
        if isinstance(saved_run_opts, dict) and int(saved_run_opts.get("target", -1)) == int(height):
            allowed_opt_keys = set(enc_opts)
            restored_opts = {k: v for k, v in saved_run_opts.items() if k in allowed_opt_keys}
            enc_opts.update(restored_opts)
            preset, codec = enc_opts["preset"], enc_opts["codec"]
            crf = enc_opts["crf"]

        encoded = False

        # 3a) Distributed (GitHub Actions workers) ─
        cfg = store.get(job.user_id)         # download අතරේ settings (workers…) වෙනස් කළා නම් අලුත් එක
        requested_workers = min(cfg["workers"], dist.capacity())
        if resume and isinstance(resume.get("stats"), dict):
            requested_workers = min(int(resume["stats"].get("workers", requested_workers) or requested_workers),
                                    max(1, dist.capacity()))
        n_workers = dist.worker_budget(meta.duration, requested_workers)
        resume_distributed = bool(resume and (isinstance(saved_run_opts, dict) or resume.get("parts")))
        if resume_distributed or dist.should_distribute(meta, height, preset, codec, cfg["dist"]):
            dst_state = dist.DistState(meta.duration, meta.fps, n_workers)

            async def dist_ticker():
                while True:
                    await live.set(render_dist(dst_state, meta.name, height, preset, codec))
                    await asyncio.sleep(PROGRESS_UPDATE_INTERVAL)

            dticker = asyncio.create_task(dist_ticker())
            try:
                run = dist.DistRun(client=client, job=job, src=src, dst=dst, meta=meta,
                                   state=dst_state, opts=enc_opts, workdir=workdir,
                                   pool=pool, n_workers=n_workers, resume_manifest=resume or None)
                job.dist_state, job.dist_pool = dst_state, pool
                dstats = await run.run()
                stats["enc_time"], stats["enc_speed"] = dst_state.elapsed, meta.duration / max(dst_state.elapsed, 1)
                stats["workers"] = dstats["workers"]
                stats["local_parts"] = dstats["local_parts"]
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
                    await live.set(render_encode(st, meta.name, height, preset, codec))
                    await asyncio.sleep(PROGRESS_UPDATE_INTERVAL)

            ticker = asyncio.create_task(enc_ticker())
            try:
                await transcode(
                    str(src), str(dst), st, job,
                    target=height, scale=enc_opts["scale"], portrait=enc_opts["portrait"],
                    preset=preset, codec=codec, crf=crf, abitrate=ab,
                    maxrate_kbps=enc_opts["maxrate_kbps"], audio_mode=enc_opts["audio_mode"],
                    subtitle_indexes=enc_opts["subtitle_indexes"],
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

        caption = (f"🎬 <b>{esc(stem)}</b>\n"
                   f"📐 {height}p • 🎞 {codec} • ⚙️ {preset}\n"
                   f"📦 {human_size(out_size)} (original {human_size(meta.size)})")
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
        job.completed = True
        await persist_queue_state()

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
        job.cancelled = True
        live.buttons = None
        await live.set("❌ <b>Cancelled</b>", force=True)
        await persist_queue_state()
    except Exception as e: