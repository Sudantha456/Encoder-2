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
                    PROGRESS_UPDATE_INTERVAL, PROGRESS_TICK, PROGRESS_MIN_EDIT_GAP,
                    PROBE_TIMEOUT, DOWNLOAD_DIR, PRESETS, CODECS,
                    PRESET_INFO, CRF, AUDIO_BITRATE, MAX_UPLOAD_BYTES,
                    WORKER_CHOICES, QUEUE_MAX, QUEUE_RES_CHOICES)
from fast_downloader import get_downloader, init_download_engine
from fast_uploader import get_uploader
from userbots import Relay
from media import (VideoMeta, telegram_meta, probe_remote, ffprobe, apply_probe,
                   offered_targets, estimate_size, render_preview, short_side, guard_kbps,
                   is_source_res, cache_get as meta_cache_get, apply_cached as meta_apply_cached)
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
probe_sem = asyncio.Semaphore(2)        # videos 20ක් එකවර දැම්මොත් probe flood වෙන එක වළක්වන්න
bg_tasks: set = set()                   # background tasks (GC නොවෙන්න reference තියාගන්න)


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
    """
    Progress message එක edit කරන helper — **smooth** සහ FloodWait-safe.

    පරණ එකේ අවුල: edit එකක් යන අතරේ (ටිකක් slow) එන updates ඔක්කොම drop වුණා →
    progress එක "හිර වෙලා" වගේ පේනවා, ඊට පස්සේ එකපාරට පනිනවා.
    දැන්:
      • set() කොච්චර වේගයෙන් call කළත් edit එක යන්නේ PROGRESS_MIN_EDIT_GAP (1.2s) ට වඩා ඉක්මනින් නෑ
      • අතරමැද ඒවා coalesce — හැම වෙලාවෙම edit වෙන්නේ **අලුත්ම state** එක
      • edit එක යන අතරේ අලුත් state එකක් ආවොත් ඒකට පස්සේ තව edit එකක් (trailing update නැති වෙන්නේ නෑ)
    """

    def __init__(self, msg, buttons=None, gap: Optional[float] = None):
        self.msg, self.buttons = msg, buttons
        self.gap = PROGRESS_MIN_EDIT_GAP if gap is None else gap
        self._latest = None
        self._shown = None
        self._shown_buttons = buttons
        self._force = False
        self._task: Optional[asyncio.Task] = None
        self._last_edit = 0.0
        self._closed = False
        self._flood_until = 0.0

    # ── public ──
    async def set(self, text: str, force: bool = False):
        if self._closed:
            return
        self._latest = text
        if force:
            self._force = True
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run())

    async def flush(self, timeout: float = 8.0):
        """අන්තිම state එකත් edit වෙනකම් ඉන්නවා (job එක ඉවර වෙද්දී)."""
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if self._task is None or self._task.done():
                if self._latest == self._shown and self.buttons is self._shown_buttons:
                    return
            await asyncio.sleep(0.1)

    def close(self):
        self._closed = True
        if self._task and not self._task.done():
            self._task.cancel()

    # ── internals ──
    def _dirty(self) -> bool:
        return (self._latest is not None
                and (self._force or self._latest != self._shown
                     or self.buttons is not self._shown_buttons))

    async def _run(self):
        while not self._closed and self._dirty():
            now = time.monotonic()
            wait = max(self.gap - (now - self._last_edit), self._flood_until - now)
            if wait > 0:
                await asyncio.sleep(min(wait, self.gap))
                continue
            text = self._latest
            self._force = False
            try:
                await self.msg.edit(text, buttons=self.buttons)
            except MessageNotModifiedError:
                pass
            except FloodWaitError as e:
                logger.warning(f"edit FloodWait {e.seconds}s — ටිකක් ඉඳලා ආයෙ")
                self._flood_until = time.monotonic() + min(e.seconds, 15)
                continue
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"edit failed: {e}")
            self._shown = text
            self._shown_buttons = self.buttons
            self._last_edit = time.monotonic()


# ───────────────────────── preview / settings panels ─────────────────────────
def preview_buttons(job: Job):
    """Video එකේ තියෙන **සියලුම** qualities බොත්තම් විදිහට (ලොකුම එක මුලින්)."""
    cfg = store.get(job.user_id)
    btns = []
    for t in offered_targets(job.meta):
        est = estimate_size(job.meta, t, cfg["preset"], cfg["codec"])
        label = f"{t}p ≈{human_size_short(est)}" if est else f"{t}p"
        if is_source_res(job.meta, t):
            label = "🎯 " + label                    # source එකේම resolution එක
        btns.append(Button.inline(label, cb("res", job.id, t)))
    rows = [btns[i:i + 3] for i in range(0, len(btns), 3)] or [[]]
    rows.append([Button.inline("⚙️ Settings", cb("cfg", job.id)),
                 Button.inline("❌ Cancel", cb("x", job.id))])
    return rows


def pick_target(meta: VideoMeta, qres: str) -> int:
    """Auto-queue: settings එකේ resolution එකට ළඟම එක (source එකට වඩා ලොකු නෑ)."""
    opts = offered_targets(meta)
    if qres == "smart" or not opts:
        return max(opts) if opts else 720
    try:
        want = int(qres)
    except (TypeError, ValueError):
        return max(opts)
    return want if want in opts else min(opts, key=lambda t: abs(t - want))


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
        rows.append(f"▶️ <b>Running</b>: <code>{esc(cur.meta.name[:40])}</code> → {cur.height}p")
    for i, j in enumerate(queue, 1):
        rows.append(f"{i}. <code>{esc(j.meta.name[:40])}</code> → {j.height}p")
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
    if job.live:                      # queue එකේ progress edits නවත්තලා "Cancelled" එක ලියනවා
        job.live.close()
    with contextlib.suppress(Exception):
        await job.status.edit("❌ <b>Cancelled</b>", buttons=None)
    await refresh_positions()


async def refresh_positions():
    for i, j in enumerate(list(queue), 1):
        if j.live:
            await j.live.set(f"⏳ <b>Queued #{i}</b> / {len(queue)}\n<code>{esc(j.meta.name[:50])}</code> → {j.height}p\n"
                             "ඉස්සරහ jobs ඉවර වුණාම ස්වයංක්‍රීයව පටන් ගන්නවා.", force=True)


async def enqueue(job: Job, height: int) -> bool:
    global queue_task
    if pending_count() >= QUEUE_MAX:
        with contextlib.suppress(Exception):
            await job.status.edit(f"🚫 Queue පිරිලා ({QUEUE_MAX}/{QUEUE_MAX}). ටිකක් ඉඳලා ආයෙ එවන්න.", buttons=None)
        jobs.pop(job.id, None)
        return False
    job.height, job.queued, job.started = height, True, True
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


def _spawn(coro):
    """Background task එකක් — reference තියාගෙන (GC නොවෙන්න)."""
    t = asyncio.create_task(coro)
    bg_tasks.add(t)
    t.add_done_callback(bg_tasks.discard)
    return t


async def _preview_probe(job: Job) -> bool:
    """Metadata probe (multi-connection, cached) — preview එක block නොකර background එකේ."""
    workdir = Path(DOWNLOAD_DIR) / ("probe_" + job.id)
    workdir.mkdir(parents=True, exist_ok=True)
    try:
        async with probe_sem:
            return await probe_remote(client, job.message, job.meta, str(workdir),
                                      timeout=PROBE_TIMEOUT)
    except Exception as e:                      # noqa: BLE001
        logger.warning(f"probe failed: {e!r}")
        return False
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


async def _refresh_preview(job: Job, probe: asyncio.Task):
    """Probe ඉවර වුණාම preview එක (codec/audio/qualities) අලුත් කරනවා."""
    try:
        await asyncio.wait_for(asyncio.shield(probe), timeout=PROBE_TIMEOUT + 15)
    except Exception:                           # noqa: BLE001
        pass
    if job.started or jobs.get(job.id) is not job:
        return
    try:
        await show_preview(job)
    except MessageNotModifiedError:
        pass
    except Exception as e:                      # noqa: BLE001
        logger.debug(f"preview refresh failed: {e!r}")


@client.on(events.NewMessage(incoming=True, func=is_video))
async def on_video(event):
    if not allowed(event.sender_id):
        return
    msg = event.message
    meta = telegram_meta(msg)

    # ⚡ Cache: එකම video එක කලින් ආවා නම් network එකට යන්නේ නෑ (instant)
    cached = meta_cache_get(msg)
    if cached:
        meta_apply_cached(meta, cached)

    cfg = store.get(event.sender_id)
    ask = cfg["qres"] == "ask"
    opts = offered_targets(meta)
    # Telegram attributes වලින් resolution දැනටමත් තියෙනවා → preview එක **පළමු message එකෙන්ම**
    # (කලින් "Analyzing…" එකක් දාලා edit කළා → extra roundtrip)
    direct = ask and len(opts) > 1
    job = Job(id=uuid.uuid4().hex[:8], user_id=event.sender_id, chat_id=event.chat_id,
              message=msg, meta=meta, status=None)
    text = render_preview(meta, cfg["preset"], cfg["codec"]) if direct else "🔎 <b>Analyzing video…</b>"
    job.status = await event.reply(text, buttons=preview_buttons(job) if direct else None)
    jobs[job.id] = job

    probe = _spawn(_preview_probe(job)) if not cached else None

    if not ask:                                 # auto-queue: probe එක එනකම් ඉන්නේ නෑ
        await enqueue(job, pick_target(meta, cfg["qres"]))
        return

    if not short_side(meta) and probe:          # dimensions නෑ → probe එක ඉවර වෙනකම් ඉන්නවා
        with contextlib.suppress(Exception):
            await asyncio.wait_for(asyncio.shield(probe), timeout=PROBE_TIMEOUT + 10)
        opts = offered_targets(job.meta)

    if len(opts) <= 1:                          # quality එකක් විතරයි → බොත්තම් නැතුව ඉදිරියට
        await enqueue(job, opts[0])
        return

    if not direct:
        await show_preview(job)                 # ⚡ instant preview (probe නැතුව)
    if probe is not None:
        _spawn(_refresh_preview(job, probe))


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
    est_size = estimate_size(meta, height, preset, codec)

    workdir = Path(DOWNLOAD_DIR) / job.id
    workdir.mkdir(parents=True, exist_ok=True)
    stem = safe_stem(meta.name)
    ext = os.path.splitext(meta.name)[1] or ".mp4"
    src, dst = workdir / f"in_{stem}{ext}", workdir / f"{stem}_{height}p.mp4"

    phase = ["dl"]
    t_start = time.monotonic()
    stats = {}
    xfer: Optional[asyncio.Task] = None
    dl, ul = get_downloader(), get_uploader()
    ch_ids, ch_out = [], [None, None]       # channel messages (cleanup): bot ගේ ids, [userbot msg, userbot]

    try:
        if job.cancelled:
            raise asyncio.CancelledError()

        # 0) Workers ඉක්මන් start: download එක යන අතරේම runners ready කරන්න ─
        if dist.should_distribute(meta, height, preset, codec, cfg["dist"]):
            async def _prestart():
                with contextlib.suppress(Exception):
                    await pool.ensure(min(store.get(job.user_id)["workers"], dist.capacity()))
            prestart = asyncio.create_task(_prestart())

        # 1) DOWNLOAD ─────────────────────────────
        tr = SpeedTracker(total=meta.size)
        await live.set(render_transfer("dl", tr, meta.name), force=True)
        # smooth progress: state එක PROGRESS_TICK වලට කියවලා render (edit එක Live එකෙන් throttle)
        view = {"kind": "dl", "tr": tr, "name": meta.name}

        async def transfer_ticker():
            # job එක ඉවර වෙනකම් ජීවත් වෙනවා (dl → probe → enc → ul; upload වෙද්දීත් smooth)
            while True:
                if phase[0] in ("dl", "ul"):
                    await live.set(render_transfer(view["kind"], view["tr"], view["name"]))
                await asyncio.sleep(PROGRESS_TICK)

        xfer = asyncio.create_task(transfer_ticker())

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
                view.update(kind="dl", tr=tr, name=meta.name)
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
            preset=preset, codec=codec, crf=CRF[codec], maxrate_kbps=guard_kbps(meta, height),
            abitrate=ab, audio_mode="copy" if ac else "aac",
        )
        encoded = False

        # 3a) Distributed (GitHub Actions workers) ─
        cfg = store.get(job.user_id)         # download අතරේ settings (workers…) වෙනස් කළා නම් අලුත් එක
        n_workers = min(cfg["workers"], dist.capacity())       # accounts වලින් දෙන්න පුළුවන් උපරිමය
        if dist.should_distribute(meta, height, preset, codec, cfg["dist"]):
            dst_state = dist.DistState(meta.duration, meta.fps, n_workers)

            async def dist_ticker():
                while True:
                    await live.set(render_dist(dst_state, meta.name, height, preset, codec))
                    await asyncio.sleep(PROGRESS_TICK)

            dticker = asyncio.create_task(dist_ticker())
            try:
                run = dist.DistRun(client=client, job=job, src=src, dst=dst, meta=meta,
                                   state=dst_state, opts=enc_opts, workdir=workdir,
                                   pool=pool, n_workers=n_workers)
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
                    await asyncio.sleep(PROGRESS_TICK)

            ticker = asyncio.create_task(enc_ticker())
            try:
                await transcode(
                    str(src), str(dst), st, job,
                    target=height, scale=enc_opts["scale"], portrait=enc_opts["portrait"],
                    preset=preset, codec=codec, crf=CRF[codec], abitrate=ab,
                    maxrate_kbps=enc_opts["maxrate_kbps"], audio_mode=enc_opts["audio_mode"],
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
        view.update(kind="ul", tr=tr2, name=dst.name)
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
                view.update(kind="ul", tr=tr2, name=dst.name)
                sent = None
        if sent is None:
            sent = await ul.upload_file(client, job.chat_id, str(dst), job.id, ul_cb)
            stats["ul_avg"], stats["ul_time"] = tr2.avg_speed, tr2.elapsed
            with contextlib.suppress(Exception):
                await client.edit_message(sent, caption)
        phase[0] = "done"
        if xfer:
            xfer.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await xfer
            xfer = None

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
        if xfer and not xfer.done():
            xfer.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await xfer
        # අන්තිම progress එක (✅ Done / ❌ Cancel) edit වෙනකම් ඉන්නවා → message එක හිර වෙන්නේ නෑ
        with contextlib.suppress(Exception):
            await live.flush(timeout=10)
        live.close()
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
        await relay.stop()


if __name__ == "__main__":
    client.loop.run_until_complete(main())
