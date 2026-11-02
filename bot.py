"""
bot.py — Telethon Video Encoder Bot
Video එකක් දාන්න → metadata preview → resolution (720p/480p/240p) තෝරන්න →
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
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from telethon import TelegramClient, events, Button
from telethon.errors import MessageNotModifiedError, FloodWaitError

import config
from config import (API_ID, API_HASH, BOT_TOKEN, SESSION_NAME, ALLOWED_USERS,
                    PROGRESS_UPDATE_INTERVAL, DOWNLOAD_DIR, PRESETS, CODECS,
                    PRESET_INFO, CRF, AUDIO_BITRATE, MAX_UPLOAD_BYTES,
                    WORKER_CHOICES, QUEUE_MAX, QUEUE_RES_CHOICES)
from fast_downloader import get_downloader, init_download_engine
from fast_uploader import get_uploader
from media import (VideoMeta, telegram_meta, probe_remote, ffprobe, apply_probe,
                   offered_targets, estimate_size, render_preview, short_side, guard_kbps)
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
        f"👷 Workers: <b>{s['workers']}</b>  <i>(download එක යන අතරේත් වෙනස් කරන්න පුළුවන්)</i>",
        f"📋 Auto-queue: <b>{'ask (බොත්තම්)' if s['qres'] == 'ask' else s['qres'] + 'p'}</b>",
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
    rows.append([Button.inline(("✅ " if s["workers"] == n else "") + f"👷{n}", cb("wrk", job_id, n))
                 for n in WORKER_CHOICES])
    rows.append([Button.inline(("✅ " if s["qres"] == q else "") + ("📋 Ask" if q == "ask" else f"📋 {q}p"),
                               cb("qres", job_id, q)) for q in QUEUE_RES_CHOICES])
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
        "📐 720p / 480p / 240p තෝරන්න (≈ size පෙන්වනවා)\n"
        "⚡ High-speed download → FFmpeg encode → High-speed upload\n\n"
        "📋 Videos 20 ක් දක්වා queue කරන්න පුළුවන් — එකින් එක download → encode → upload\n"
        "⚙️ Preset / Codec / Workers: /settings   •   📋 /queue   •   🧹 /clear"
    )


@client.on(events.NewMessage(pattern=r"^/settings", incoming=True))
async def cmd_settings(event):
    if not allowed(event.sender_id):
        return
    await event.reply(settings_text(event.sender_id), buttons=settings_buttons(event.sender_id, "0"))


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
        want = int(qres)
        await enqueue(job, want if want in opts else min(opts, key=lambda t: abs(t - want)))
    else:
        await show_preview(job)


# ───────────────────────── callbacks ─────────────────────────
@client.on(events.CallbackQuery())
async def on_callback(event):
    try:
        action, job_id, arg = (event.data.decode().split(":", 2) + [""])[:3]
    except Exception:
        return
    job = jobs.get(job_id)
    uid = event.sender_id

    if job_id != "0" and not job:
        await event.answer("Job එක අවසන් / expire වෙලා.", alert=True)
        return
    if job and uid != job.user_id:
        await event.answer("මේක ඔයාගේ job එකක් නෙවෙයි.", alert=True)
        return

    if action == "res":
        if job.started:
            await event.answer("Already in queue…")
            return
        ok = await enqueue(job, int(arg))
        await event.answer(f"Queued #{pending_count()}" if ok else "Queue පිරිලා", alert=not ok)

    elif action == "x":
        if job.queued and job in queue:          # තාම පටන් අරන් නෑ → queue එකෙන් අයින්
            await event.answer("Removed")
            await cancel_waiting(job)
            return
        job.cancelled = True
        get_downloader().cancel(job.id)
        get_uploader().cancel(job.id)
        if job.proc:
            with contextlib.suppress(Exception):
                job.proc.kill()
        await event.answer("Cancelling…")
        if not job.started:
            jobs.pop(job.id, None)
            with contextlib.suppress(Exception):
                await job.status.edit("❌ <b>Cancelled</b>", buttons=None)

    elif action == "cfg":
        await event.answer()
        await event.edit(settings_text(uid), buttons=settings_buttons(uid, job_id))

    elif action in ("pre", "cod", "dst", "wrk", "qres"):
        key = {"pre": "preset", "cod": "codec", "dst": "dist", "wrk": "workers", "qres": "qres"}[action]
        if action == "wrk":
            if int(arg) not in WORKER_CHOICES:
                return
            arg = int(arg)
        store.set(uid, **{key: arg})
        await event.answer(f"{arg} ✔")
        if action == "wrk" and pool.active and dist.available():
            # Download අතරේ වැඩි කළොත් අලුත් runners ඉක්මනින් start වෙන්න (අඩු කළොත් ඉතුරු ඒවා hold)
            async def _grow():
                with contextlib.suppress(Exception):
                    await pool.ensure(arg)
            asyncio.create_task(_grow())
        with contextlib.suppress(MessageNotModifiedError):
            await event.edit(settings_text(uid), buttons=settings_buttons(uid, job_id))

    elif action == "back":
        await event.answer()
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
    dl, ul = get_downloader(), get_uploader()

    try:
        if job.cancelled:
            raise asyncio.CancelledError()

        # 0) Workers ඉක්මන් start: download එක යන අතරේම runners ready කරන්න ─
        if dist.should_distribute(meta, height, preset, codec, cfg["dist"]):
            async def _prestart():
                with contextlib.suppress(Exception):
                    await pool.ensure(store.get(job.user_id)["workers"])
            prestart = asyncio.create_task(_prestart())

        # 1) DOWNLOAD ─────────────────────────────
        tr = SpeedTracker(total=meta.size)
        await live.set(render_transfer("dl", tr, meta.name), force=True)

        async def dl_cb(cur, tot):
            if phase[0] != "dl":
                return
            tr.update(cur, tot)
            await live.set(render_transfer("dl", tr, meta.name))

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
        n_workers = cfg["workers"]
        if dist.should_distribute(meta, height, preset, codec, cfg["dist"]):
            dst_state = dist.DistState(meta.duration, meta.fps, n_workers)

            async def dist_ticker():
                while True:
                    await live.set(render_dist(dst_state, meta.name, height, preset, codec))
                    await asyncio.sleep(PROGRESS_UPDATE_INTERVAL)

            dticker = asyncio.create_task(dist_ticker())
            try:
                run = dist.DistRun(client=client, job=job, src=src, dst=dst, meta=meta,
                                   state=dst_state, opts=enc_opts, workdir=workdir,
                                   pool=pool, n_workers=n_workers)
                dstats = await run.run()
                stats["enc_time"], stats["enc_speed"] = dst_state.elapsed, meta.duration / max(dst_state.elapsed, 1)
                stats["workers"] = dstats["workers"]
                stats["local_parts"] = dstats["local_parts"]
                encoded = True
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"Distributed encode failed → local: {e!r}")
                await live.set(f"⚠️ Distributed failed ({esc(str(e)[:120])})\nmain runner එකෙන්ම encode කරනවා…", force=True)
                for pat in ("raw_part_*", "encoded_part_*", "audio.mka", "concat.txt"):
                    for f in workdir.glob(pat):
                        f.unlink(missing_ok=True)
                dst.unlink(missing_ok=True)
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
        await live.set(render_transfer("ul", tr2, dst.name), force=True)

        async def ul_cb(cur, tot):
            if phase[0] != "ul":
                return
            tr2.update(cur, tot)
            await live.set(render_transfer("ul", tr2, dst.name))

        sent = await ul.upload_file(client, job.chat_id, str(dst), job.id, ul_cb)
        stats["ul_avg"], stats["ul_time"] = tr2.avg_speed, tr2.elapsed
        phase[0] = "done"

        caption = (f"🎬 <b>{esc(stem)}</b>\n"
                   f"📐 {height}p • 🎞 {codec} • ⚙️ {preset}\n"
                   f"📦 {human_size(out_size)} (original {human_size(meta.size)})")
        with contextlib.suppress(Exception):
            await client.edit_message(sent, caption)

        saved = (1 - out_size / meta.size) * 100 if meta.size else 0
        live.buttons = None
        await live.set("\n".join([
            "✅ <b>Done!</b>",
            f"<code>{esc(dst.name)}</code>",
            "━━━━━━━━━━━━━━━━━━━━",
            f"📦 <code>{human_size(meta.size)}</code> → <code>{human_size(out_size)}</code> "
            f"({'−' if saved >= 0 else '+'}{abs(saved):.0f}%)"
            + (f"  •  est ≈{human_size(est_size)}" if est_size else ""),
            f"📥 DL: <code>{human_speed(stats['dl_avg'])}</code> • <code>{human_time(stats['dl_time'])}</code>",
            f"🎬 Encode{' 🌐×' + str(stats['workers']) if 'workers' in stats else ''}: "
            f"<code>{stats['enc_speed']:.2f}x</code> • <code>{human_time(stats['enc_time'])}</code>"
            + (f"  (🏠 local parts: {stats['local_parts']})" if stats.get("local_parts") else ""),
            f"📤 UL: <code>{human_speed(stats['ul_avg'])}</code> • <code>{human_time(stats['ul_time'])}</code>",
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
    logger.info(f"✅ @{me.username} running • update interval {PROGRESS_UPDATE_INTERVAL}s")
    await client.run_until_disconnected()


if __name__ == "__main__":
    client.loop.run_until_complete(main())
