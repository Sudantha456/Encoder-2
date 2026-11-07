"""
userbots.py — Userbot relay (private channel හරහා උපරිම download/upload speed)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  user ──video──▶ main BOT ──forward──▶ private CHANNEL
                                           │  (userbot 1..N එකවර warm multi-IP pools හරහා download)
                                           ▼
                                    FFmpeg encode (main runner / GitHub workers)
                                           │
                       userbot (වඩාම නිදහස් එක) ──upload──▶ CHANNEL ──▶ main BOT ──▶ user ගේ private chat
  Progress updates: main bot, user ගේ chat එකේ message එක edit කරනවා.

Turbo Download Upgrades:
  • FastTcpConnection (Abridged MTProto + 8MB buffers + TCP_NODELAY) on all userbot clients & pools.
  • Live channel message push-cache (0ms channel message lookup without polling delay).
  • Background DC pool pre-warming (on_video + parallel with forward_in → 0ms warm start).
  • Streaming lane activation: පළමු userbot එකට message ලැබුණු සැණින් download ඇරඹේ; අනිත් userbots එසැණින් එක්වේ.
  • Auto FileReference refresh callback for multi-GB downloads.
"""
import asyncio
import collections
import contextlib
import logging
import math
import time
from typing import Any, List, Optional

from telethon import TelegramClient, events
from telethon.errors import AuthKeyError, FloodWaitError, RPCError
from telethon.sessions import StringSession
from telethon.tl.types import InputDocumentFileLocation

import config
from channel import (
    SetupError,
    check_bot_perms,
    check_user_perms,
    parse_channel_ref,
    resolve_for_bot,
    resolve_for_user,
)
from fast_conn import FastTcpConnection, close_client_pools, warmup_pool
from fast_downloader import Lane
from fast_uploader import LongFloodWait

logger = logging.getLogger("UserBots")

MSG_CACHE_MAX = 64


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
        self.msg_cache: "collections.OrderedDict[int, Any]" = collections.OrderedDict()

    @property
    def label(self) -> str:
        who = f"@{self.me.username}" if self.me and getattr(self.me, "username", None) else (
            self.me.first_name if self.me else "?")
        return f"{self.name} ({who})"

    @property
    def cooling(self) -> bool:
        return time.monotonic() < self.cooldown_until

    def cache_message(self, msg: Any) -> None:
        mid = getattr(msg, "id", None)
        if not mid:
            return
        self.msg_cache[mid] = msg
        self.msg_cache.move_to_end(mid)
        while len(self.msg_cache) > MSG_CACHE_MAX:
            self.msg_cache.popitem(last=False)

    def flood(self, seconds: int):
        secs = int(seconds)
        self.floods += 1
        # Short transient waits (<= 5s) don't exile the userbot for 60s across subsequent jobs
        cd = secs + 2 if secs <= 5 else max(secs, config.UB_FLOOD_COOLDOWN)
        self.cooldown_until = max(self.cooldown_until, time.monotonic() + cd)
        logger.warning(f"[{self.name}] FloodWait {secs}s → cooldown {cd}s")


class Relay:
    def __init__(self):
        self.userbots: List[UserBot] = []
        self.bot_peer = None               # main bot ගේ InputPeerChannel
        self.enabled = False
        self.problems: List[str] = []      # user ට පෙන්වන්න setup errors
        self.last_used_lanes: List[str] = []
        self._bg_tasks: set = set()

    # ───────────────────────── setup ─────────────────────────
    @property
    def configured(self) -> bool:
        return bool(config.USER_STRINGS and config.CHANNEL_ID)

    @property
    def ready(self) -> List[UserBot]:
        return [u for u in self.userbots if u.ok]

    def connections_per_ub(self, active_count: Optional[int] = None) -> int:
        n = max(1, active_count if active_count is not None else len(self.ready))
        base = max(1, int(getattr(config, "UB_DL_CONNECTIONS", 16)))
        max_per = max(base, int(getattr(config, "UB_MAX_CONNECTIONS", 20)))
        target_tot = int(getattr(config, "UB_TARGET_TOTAL_CONNECTIONS", 80))
        scaled = math.ceil(target_tot / n)
        return min(max_per, max(base, scaled))

    async def _start_userbot(self, ub: UserBot, ref, delay: float = 0.0):
        if delay > 0:
            await asyncio.sleep(delay)
        try:
            ub.client = TelegramClient(
                StringSession(ub.session),
                config.API_ID,
                config.API_HASH,
                connection=FastTcpConnection,
                flood_sleep_threshold=0,       # FloodWait අපි ම handle කරනවා (අහඹු sleeps නෑ)
            )
            ub.client.parse_mode = "html"
            await ub.client.connect()
            if not await ub.client.is_user_authorized():
                raise SetupError(f"{ub.name}: session string එක invalid / log out වෙලා (ආයෙ generate කරන්න).")
            ub.me = await ub.client.get_me()
            if getattr(ub.me, "bot", False):
                raise SetupError(f"{ub.name}: මේක BOT token session එකක්. USER account session string එකක් ඕනේ.")
            ub.entity = await resolve_for_user(ub.client, ref, ub.label)
            await check_user_perms(ub.client, ub.entity, ub.label, need_post=True)

            # Live push cache: bot විසින් channel එකට forward කරන messages 0ms තුළ memory එකෙන් ගන්නවා
            @ub.client.on(events.NewMessage(chats=ub.entity))
            async def _on_channel_msg(event, _ub=ub):
                if getattr(event, "message", None):
                    _ub.cache_message(event.message)

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

        self.userbots = [UserBot(i + 1, s) for i, s in enumerate(config.USER_STRINGS)]
        await asyncio.gather(*[
            self._start_userbot(ub, ref, delay=i * 0.15)
            for i, ub in enumerate(self.userbots)
        ])
        good = self.ready
        if not good:
            logger.error("👤 Userbot එකක්වත් ready නෑ — relay off.")
            return False

        ids = {u.entity.id for u in good}
        if len(ids) > 1:
            self.problems.append(f"Userbots වෙනස් channels වලට resolve වුණා: {ids}")
            return False

        first = good[0]

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
        conns = self.connections_per_ub(len(self.ready))
        logger.info(
            "👤 Userbot relay READY — "
            + ", ".join(u.label for u in self.ready)
            + f" • {len(self.ready)}×{conns}={len(self.ready) * conns} turbo conns • channel -100{first.entity.id}"
        )
        # Pre-warm home DC pools in background right after setup
        self.trigger_warmup(None)
        return True

    async def _selftest(self, bot):
        """Bot channel එකට message එකක් දානවා → හැම userbot එකටම ඒක පේනවාද (එකම channel එකද) බලනවා."""
        m = await bot.send_message(self.bot_peer, "🔧 encoder self-test", parse_mode=None)
        try:
            for ub in list(self.ready):
                seen = ub.msg_cache.get(m.id)
                for attempt in range(6):
                    if seen:
                        break
                    seen = await ub.client.get_messages(ub.entity, ids=m.id)
                    if seen:
                        break
                    await asyncio.sleep(0.25 + attempt * 0.2)
                if not seen:
                    ub.ok = False
                    ub.error = f"{ub.name}: bot දාපු message එක userbot ට පේන්නේ නෑ (channel එකම නෙවෙයිද / membership?)."
                    self.problems.append(ub.error)
        finally:
            with contextlib.suppress(Exception):
                await bot.delete_messages(self.bot_peer, m.id)
        if not self.ready:
            raise SetupError("Self-test fail: කිසිම userbot එකකට bot ගේ messages පේන්නේ නෑ.")

    async def warmup(self, dc_id: Optional[int] = None) -> None:
        """Pre-warm persistent multi-connection pools for all ready userbots on target DC."""
        if not self.enabled or not self.ready:
            return
        ubs = [u for u in self._order() if not u.cooling] or self._order()
        conns = self.connections_per_ub(len(ubs))
        await asyncio.gather(
            *[warmup_pool(u.client, dc_id, size=conns, use_main=False) for u in ubs if u.client],
            return_exceptions=True,
        )

    def trigger_warmup(self, dc_id: Optional[int] = None) -> None:
        """Fire-and-forget background pool warmup."""
        if not self.enabled or not self.ready:
            return
        t = asyncio.create_task(self.warmup(dc_id))
        self._bg_tasks.add(t)
        t.add_done_callback(self._bg_tasks.discard)

    async def warmup_upload(self) -> None:
        """Pre-warm persistent Home-DC upload pool on primary ready userbot(s)."""
        if not self.enabled or not self.ready:
            return
        cands = [u for u in self._order() if not u.cooling] or self._order()
        targets = cands[:2]
        await asyncio.gather(
            *[
                warmup_pool(u.client, None, size=config.UB_UL_CONNECTIONS, use_main=False)
                for u in targets if u.client
            ],
            return_exceptions=True,
        )

    def trigger_upload_warmup(self) -> None:
        if not self.enabled or not self.ready:
            return
        t = asyncio.create_task(self.warmup_upload())
        self._bg_tasks.add(t)
        t.add_done_callback(self._bg_tasks.discard)

    async def stop(self):
        for t in list(self._bg_tasks):
            t.cancel()
        if self._bg_tasks:
            await asyncio.gather(*self._bg_tasks, return_exceptions=True)
        self._bg_tasks.clear()
        for ub in self.userbots:
            if ub.client:
                with contextlib.suppress(Exception):
                    await close_client_pools(ub.client)
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
            await asyncio.sleep(0.3 + i * 0.5)
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

    async def _fetch_channel_doc(self, ub: UserBot, ch_msg_id: int, bypass_cache: bool = False):
        if not bypass_cache:
            cached = ub.msg_cache.get(ch_msg_id)
            doc = getattr(cached, "document", None) if cached else None
            if doc:
                return doc

        delays = (0.08, 0.16, 0.30, 0.50, 0.80, 1.20)
        for d in delays:
            if not bypass_cache:
                cached = ub.msg_cache.get(ch_msg_id)
                doc = getattr(cached, "document", None) if cached else None
                if doc:
                    return doc
            try:
                m = await ub.client.get_messages(ub.entity, ids=ch_msg_id)
            except FloodWaitError as e:
                ub.flood(e.seconds)
                return None
            except RPCError as e:
                logger.warning(f"[{ub.name}] get_messages: {e!r}")
                return None
            if m:
                ub.cache_message(m)
                doc = getattr(m, "document", None)
                if doc:
                    return doc
            await asyncio.sleep(d)
        return None

    async def _lane_for(self, ub: UserBot, ch_msg_id: int, connections: int):
        """(Lane, file_size) හෝ None"""
        doc = await self._fetch_channel_doc(ub, ch_msg_id, bypass_cache=False)
        if not doc:
            return None

        doc_id = doc.id

        async def _refresh_loc():
            fresh_doc = await self._fetch_channel_doc(ub, ch_msg_id, bypass_cache=True)
            if fresh_doc and fresh_doc.id == doc_id:
                return InputDocumentFileLocation(
                    id=fresh_doc.id,
                    access_hash=fresh_doc.access_hash,
                    file_reference=fresh_doc.file_reference,
                    thumb_size="",
                )
            return None

        loc = InputDocumentFileLocation(
            id=doc.id,
            access_hash=doc.access_hash,
            file_reference=doc.file_reference,
            thumb_size="",
        )
        lane = Lane(
            name=ub.name,
            client=ub.client,
            location=loc,
            dc_id=doc.dc_id,
            connections=connections,
            on_flood=ub.flood,
            refresh_location=_refresh_loc,
            persistent_pool=True,
        )
        return lane, doc.size

    async def download(self, dl, ch_msg_id: int, save_path: str, job_id: str, progress_cb) -> str:
        """
        Streaming multi-userbot download:
        පළමු userbot එකෙන් file location ලැබුණු සැණින් download_lanes ආරම්භ කරන අතර,
        අනිත් userbots තත්පරයෙන් දහසෙන් පංගු කිහිපයකින් එම swarm එකටම එක් වේ.
        """
        self.last_used_lanes = []
        order = [u for u in self._order() if not u.cooling] or self._order()
        if not order:
            raise RuntimeError("Ready userbots කිසිවක් නෑ")

        conns = self.connections_per_ub(len(order))
        tasks = [asyncio.create_task(self._lane_for(u, ch_msg_id, conns)) for u in order]

        ready_lanes: List[Lane] = []
        total_size = 0
        pending = set(tasks)

        try:
            while pending and not ready_lanes:
                done_batch, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for fut in done_batch:
                    try:
                        res = fut.result()
                    except Exception as e:
                        logger.warning(f"lane resolve error: {e!r}")
                        res = None
                    if res:
                        ln, sz = res
                        ready_lanes.append(ln)
                        total_size = sz

            if not ready_lanes:
                raise RuntimeError("Userbot කිසිවෙකුට channel එකේ file එක පේන්නේ නෑ")

            # Small files (< 2MB) need just 1 warm lane; cancel remaining lookups
            if total_size < 2 * 1024 * 1024:
                for p in pending:
                    p.cancel()
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)
                pending = set()

            extra_coros = list(pending)
            logger.info(
                f"📥 relay turbo download: {total_size / 1048576:.1f}MB via "
                f"{len(ready_lanes)}+{len(extra_coros)} userbot(s) × {conns} conns/bot"
            )
            out = await dl.download_lanes(
                ready_lanes,
                total_size,
                save_path,
                job_id,
                progress_cb,
                max_flood_wait=config.UB_MAX_FLOOD_WAIT,
                extra_lane_coros=extra_coros,
            )
            self.last_used_lanes = [l.name for l in ready_lanes if l.got > 0] or [l.name for l in ready_lanes]
            return out
        except BaseException:
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

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
        conns = self.connections_per_ub(len(self.ready)) if self.ready else config.UB_DL_CONNECTIONS
        lines = [
            f"👤 <b>Userbot relay: {'ON ✅' if self.enabled else 'OFF ❌'}</b>"
            + (f"  (⚡ <code>{len(self.ready)}×{conns} = {len(self.ready) * conns}</code> conns)" if self.enabled else "")
        ]
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
