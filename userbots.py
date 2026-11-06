"""
userbots.py — Userbot relay (private channel හරහා උපරිම download/upload speed)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  user ──video──▶ main BOT ──forward──▶ private CHANNEL
                                           │  (userbot 1..N එකවර chunks බෙදාගෙන download — bot limits නෑ)
                                           ▼
                                    FFmpeg encode (main runner / GitHub workers)
                                           │
                       userbot (වඩාම නිදහස් එක) ──upload──▶ CHANNEL ──▶ main BOT ──▶ user ගේ private chat
  Progress updates: main bot, user ගේ chat එකේ message එක edit කරනවා.

Flood protection:
  • Download: userbots ගාණ lanes. එක lane එකකට FloodWait ආවොත් ඒ lane එක විතරක් pause (chunks වෙන lanes ගන්නවා);
    FloodWait > UB_MAX_FLOOD_WAIT නම් ඒ userbot එක cooldown එකකට (UB_FLOOD_COOLDOWN) යනවා.
  • Upload: නිදහස් / flood අඩුම userbot එක. Long FloodWait එකක් ආවොත් ස්වයංක්‍රීයව ඊළඟ userbot එකෙන් ආයෙ.
"""
import asyncio
import contextlib
import logging
import time
from typing import List, Optional

from telethon import TelegramClient
from telethon.errors import FloodWaitError, AuthKeyError, RPCError
from telethon.sessions import StringSession
from telethon.tl.types import InputDocumentFileLocation

import config
from channel import (SetupError, parse_channel_ref, resolve_for_user, resolve_for_bot,
                     check_user_perms, check_bot_perms)
from fast_downloader import Lane
from fast_uploader import LongFloodWait

logger = logging.getLogger("UserBots")


class UserBot:
    def __init__(self, idx: int, session: str):
        self.idx = idx
        self.name = f"UB{idx}"
        self.session = session
        self.client: Optional[TelegramClient] = None
        self.me = None
        self.entity = None                 # channel entity (මේ userbot ගේ access_hash එක්ක)
        self.ok = False
        self.error = ""
        self.cooldown_until = 0.0
        self.floods = 0                    # මේ run එකේ ආපු FloodWait ගණන

    @property
    def label(self) -> str:
        who = f"@{self.me.username}" if self.me and getattr(self.me, "username", None) else (
            self.me.first_name if self.me else "?")
        return f"{self.name} ({who})"

    @property
    def cooling(self) -> bool:
        return time.monotonic() < self.cooldown_until

    def flood(self, seconds: int):
        self.floods += 1
        self.cooldown_until = max(self.cooldown_until, time.monotonic() + max(int(seconds), config.UB_FLOOD_COOLDOWN))
        logger.warning(f"[{self.name}] FloodWait {seconds}s → cooldown {config.UB_FLOOD_COOLDOWN}s")


def _plan_main_sessions() -> list:
    """Main relay එකේ පාවිච්චි කරන sessions plan කරනවා (sessionplan.py — helpers bootstrap ඉවර වුණාට පස්සේම)."""
    try:
        import dist as _dist
        dl, sess = list(_dist.HELPER_DL), list(_dist.HELPER_SESS)
        done = bool(_dist.HELPERS_SETUP_DONE)
    except Exception:
        dl, sess, done = [], [], False
    from sessionplan import plan_main_sessions
    return plan_main_sessions(config.USER_STRINGS, config.DIST_DOWNLOAD, config.HELPER_TOKENS, dl, sess, done)


class Relay:
    def __init__(self):
        self.userbots: List[UserBot] = []
        self.bot_peer = None               # main bot ගේ InputPeerChannel
        self.enabled = False
        self.problems: List[str] = []      # user ට පෙන්වන්න setup errors
        self.channel_num: int = 0          # private channel ගේ numeric id (helper dl lanes resolve කරන්න)

    # ───────────────────────── setup ─────────────────────────
    @property
    def configured(self) -> bool:
        return bool(config.USER_STRINGS and config.CHANNEL_ID)

    @property
    def ready(self) -> List[UserBot]:
        return [u for u in self.userbots if u.ok]

    async def _start_userbot(self, ub: UserBot, ref):
        try:
            ub.client = TelegramClient(StringSession(ub.session), config.API_ID, config.API_HASH,
                                       flood_sleep_threshold=0)       # FloodWait අපි ම handle කරනවා (අහඹු sleeps නෑ)
            ub.client.parse_mode = "html"
            await ub.client.connect()
            if not await ub.client.is_user_authorized():
                raise SetupError(f"{ub.name}: session string එක invalid / log out වෙලා (ආයෙ generate කරන්න).")
            ub.me = await ub.client.get_me()
            if getattr(ub.me, "bot", False):
                raise SetupError(f"{ub.name}: මේක BOT token session එකක්. USER account session string එකක් ඕනේ.")
            ub.entity = await resolve_for_user(ub.client, ref, ub.label)
            await check_user_perms(ub.client, ub.entity, ub.label, need_post=True)
            ub.ok = True
        except SetupError as e:
            ub.error = str(e)
        except AuthKeyError:
            ub.error = f"{ub.name}: session string එක invalid (auth key)."
        except Exception as e:
            ub.error = f"{ub.name}: {type(e).__name__}: {e}"
        if not ub.ok:
            logger.error(ub.error)
            self.problems.append(ub.error)

    async def setup(self, bot) -> bool:
        """Userbots connect → channel resolve → permissions → bot resolve → round-trip self-test."""
        self.enabled, self.problems = False, []
        if not self.configured:
            logger.info("👤 Userbot relay off (USER_STRINGS / CHANNEL_ID set කරලා නෑ) — bot එකෙන්ම download/upload.")
            return False
        try:
            ref = parse_channel_ref(config.CHANNEL_ID)
        except SetupError as e:
            self.problems.append(str(e))
            logger.error(e)
            return False

        main_sessions = _plan_main_sessions()   # distributed dl දී: sessions බෙදනවා (dead helper lanes එම sessions main එකේ)
        if len(main_sessions) != len(config.USER_STRINGS):
            logger.info(f"👤 userbot relay: main runner එකට session(s) {len(main_sessions)}/{len(config.USER_STRINGS)} "
                        "(ඉතිරි ඒවා helper dl lanes)")
        self.userbots = [UserBot(i + 1, s) for i, s in enumerate(main_sessions)]
        for i, ub in enumerate(self.userbots):                       # invite-join / dialogs load එකවර flood නොවෙන්න stagger
            await self._start_userbot(ub, ref)
        good = self.ready
        if not good:
            logger.error("👤 Userbot එකක්වත් ready නෑ — relay off.")
            return False

        ids = {u.entity.id for u in good}
        if len(ids) > 1:
            self.problems.append(f"Userbots වෙනස් channels වලට resolve වුණා: {ids}")
            return False

        first = good[0]
        self.channel_num = int(getattr(first.entity, "id", 0) or 0)

        async def pinger():
            m = await first.client.send_message(first.entity, "🔧 encoder setup ping")
            return m.id

        try:
            self.bot_peer = await resolve_for_bot(bot, ref, first.entity, pinger)
            await check_bot_perms(bot, self.bot_peer, first.entity)
            await self._selftest(bot)
        except SetupError as e:
            self.problems.append(str(e))
            logger.error(f"👤 {e}")
            return False
        except Exception as e:
            self.problems.append(f"Self-test error: {type(e).__name__}: {e}")
            logger.exception("relay self-test")
            return False

        self.enabled = True
        logger.info("👤 Userbot relay READY — " + ", ".join(u.label for u in self.ready)
                    + f" • channel -100{first.entity.id}")
        return True

    async def _selftest(self, bot):
        """Bot channel එකට message එකක් දානවා → හැම userbot එකටම ඒක පේනවාද (එකම channel එකද) බලනවා."""
        m = await bot.send_message(self.bot_peer, "🔧 encoder self-test", parse_mode=None)
        try:
            for ub in list(self.ready):
                seen = None
                for _ in range(5):
                    seen = await ub.client.get_messages(ub.entity, ids=m.id)
                    if seen:
                        break
                    await asyncio.sleep(1)
                if not seen:
                    ub.ok = False
                    ub.error = f"{ub.name}: bot දාපු message එක userbot ට පේන්නේ නෑ (channel එකම නෙවෙයිද / membership?)."
                    self.problems.append(ub.error)
        finally:
            with contextlib.suppress(Exception):
                await bot.delete_messages(self.bot_peer, m.id)
        if not self.ready:
            raise SetupError("Self-test fail: කිසිම userbot එකකට bot ගේ messages පේන්නේ නෑ.")

    async def stop(self):
        for ub in self.userbots:
            if ub.client:
                with contextlib.suppress(Exception):
                    await ub.client.disconnect()

    # ───────────────────────── main bot side ─────────────────────────
    async def forward_in(self, bot, message) -> int:
        """User ගේ video message එක channel එකට forward කරනවා → channel message id."""
        res = await bot.forward_messages(self.bot_peer, message)
        res = res[0] if isinstance(res, (list, tuple)) else res
        if not res:
            raise RuntimeError("Channel එකට forward කරන්න බැරි වුණා (protected content ද?)")
        return res.id

    async def deliver(self, bot, chat_id: int, ch_msg_id: int, caption: str):
        """Channel එකේ ඇති encoded file එක (re-upload නැතුව) user ගේ private chat එකට."""
        m = None
        for i in range(6):
            m = await bot.get_messages(self.bot_peer, ids=ch_msg_id)
            if m and m.media:
                break
            await asyncio.sleep(1 + i)
        if not (m and m.media):
            raise RuntimeError("Channel එකේ encoded file එක bot ට හොයාගන්න බැරි වුණා")
        try:
            return await bot.send_file(chat_id, m.media, caption=caption, supports_streaming=True)
        except Exception as e:
            logger.warning(f"send_file by reference failed ({e!r}) → forward")
            res = await bot.forward_messages(chat_id, ch_msg_id, from_peer=self.bot_peer)
            return res[0] if isinstance(res, (list, tuple)) else res

    async def cleanup(self, bot, bot_ids: List[int], ub_msg=None, ub: Optional[UserBot] = None):
        if not config.CHANNEL_CLEANUP:
            return
        if bot_ids:
            with contextlib.suppress(Exception):
                await bot.delete_messages(self.bot_peer, bot_ids)
        if ub_msg is not None and ub is not None:
            with contextlib.suppress(Exception):
                await ub.client.delete_messages(ub.entity, ub_msg.id)

    # ───────────────────────── userbot side ─────────────────────────
    def _order(self) -> List[UserBot]:
        """Flood අඩු / cooldown නැති userbots මුලින්."""
        return sorted(self.ready, key=lambda u: (u.cooling, u.floods, u.idx))

    async def _lane_for(self, ub: UserBot, ch_msg_id: int, connections: int):
        """(Lane, file_size) හෝ None"""
        for _ in range(6):                                           # forward එක ලැබෙනකම් ටිකක් බලනවා
            try:
                m = await ub.client.get_messages(ub.entity, ids=ch_msg_id)
            except FloodWaitError as e:
                ub.flood(e.seconds)
                return None
            except RPCError as e:
                logger.warning(f"[{ub.name}] get_messages: {e!r}")
                return None
            doc = getattr(m, "document", None) if m else None
            if doc:
                loc = InputDocumentFileLocation(id=doc.id, access_hash=doc.access_hash,
                                                file_reference=doc.file_reference, thumb_size="")
                return Lane(name=ub.name, client=ub.client, location=loc, dc_id=doc.dc_id,
                            connections=connections, on_flood=ub.flood), doc.size
            await asyncio.sleep(1)
        return None

    async def download(self, dl, ch_msg_id: int, save_path: str, job_id: str, progress_cb,
                       offset: int = 0, length: Optional[int] = None) -> str:
        """සියලුම ready userbots (cooldown නැති) එකවර — chunks ඒ අතර බෙදෙනවා.
        offset/length: [offset, offset+length) byte range විතරක් (distributed download slice)."""
        order = [u for u in self._order() if not u.cooling] or self._order()
        got = await asyncio.gather(*[self._lane_for(u, ch_msg_id, config.UB_DL_CONNECTIONS) for u in order])
        pairs = [g for g in got if g]
        if not pairs:
            raise RuntimeError("Userbot කිසිවෙකුට channel එකේ file එක පේන්නේ නෑ")
        total = pairs[0][1]
        if length is not None and (offset < 0 or length <= 0 or offset + length > total):
            raise RuntimeError(f"Invalid byte range [{offset}, {offset + length}) vs {total}")
        full = length is None
        eff = total if full else length
        lanes = [p[0] for p in pairs]
        if eff < 8 * 1024 * 1024:                 # පොඩි range → userbot එකක් ඇති
            lanes = lanes[:1]
        what = f"{eff / 1048576:.1f}MB" + ("" if full else f" (range @{offset})")
        logger.info(f"📥 relay download: {what} via {', '.join(l.name for l in lanes)}")
        return await dl.download_lanes(lanes, eff, save_path, job_id, progress_cb,
                                       max_flood_wait=config.UB_MAX_FLOOD_WAIT,
                                       offset=offset, length=length)

    async def upload(self, up, path: str, job_id: str, progress_cb):
        """(userbot, channel message). Flood වුණොත් ඊළඟ userbot එකෙන්."""
        last: Optional[BaseException] = None
        cands = self._order()
        for n, ub in enumerate(cands):
            if ub.cooling and n < len(cands) - 1:
                continue
            try:
                logger.info(f"📤 relay upload via {ub.name}")
                msg = await up.upload_file(ub.client, ub.entity, path, job_id, progress_cb,
                                           connections=config.UB_UL_CONNECTIONS,
                                           max_flood_wait=config.UB_MAX_FLOOD_WAIT, use_main=False)
                return ub, msg
            except LongFloodWait as e:
                ub.flood(e.seconds)
                last = e
                logger.warning(f"[{ub.name}] upload FloodWait {e.seconds}s → ඊළඟ userbot එක")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                last = e
                logger.warning(f"[{ub.name}] upload failed: {e!r} → ඊළඟ userbot එක")
        raise RuntimeError(f"සියලුම userbots වලින් upload fail: {last!r}")

    # ───────────────────────── status ─────────────────────────
    def status_text(self) -> str:
        if not self.configured:
            return ("👤 <b>Userbot relay: OFF</b>\n<code>USER_STRINGS</code> සහ <code>CHANNEL_ID</code> set කරලා නෑ — "
                    "bot එකෙන්ම download/upload වෙනවා.")
        lines = [f"👤 <b>Userbot relay: {'ON ✅' if self.enabled else 'OFF ❌'}</b>"]
        for u in self.userbots:
            if u.ok:
                st = f"⏸ cooldown {max(0, u.cooldown_until - time.monotonic()):.0f}s" if u.cooling else "🟢 ready"
                lines.append(f"• {u.label} — {st} • floods {u.floods}")
            else:
                lines.append(f"• ❌ {u.error}")
        for p in self.problems:
            if not any(p == u.error for u in self.userbots):
                lines.append(f"⚠️ {p}")
        return "\n".join(lines)
