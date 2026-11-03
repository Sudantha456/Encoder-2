"""
channel.py — Private channel හරියටම අඳුරගන්නා helpers
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"Could not find the input entity for PeerChannel(...)" error එකට හේතුව: Telethon session එකේ ඒ channel එකේ
access_hash එක cache වෙලා නැති එක (GitHub Actions වල හැම run එකම අලුත් session). මෙතන:

  Userbot → channel id / @username / invite link ඕනෑම එකකින් resolve කරනවා (invite link නම් join වෙනවා,
            id නම් dialogs load කරලා cache කරනවා).
  Main bot → bot ට dialogs නෑ. ඒ නිසා: cache → GetChannels → (අවසාන) userbot ගෙන් channel එකට ping message එකක්
            දාලා bot ට update එකක් ලැබෙනකම් බලනවා (bot admin නම් ලැබෙනවා) → access_hash cache වෙනවා.
  ඒ සියල්ලෙන් පස්සේ permissions validate කරලා, වැරදි තියෙනවා නම් නිශ්චිත හේතුව සහිත SetupError එකක් දෙනවා.
"""
import asyncio
import logging
import re
from dataclasses import dataclass
from typing import Optional

from telethon import events
from telethon.errors import (UserAlreadyParticipantError, InviteHashExpiredError, InviteHashInvalidError,
                             ChannelPrivateError, ChannelInvalidError, RPCError)
from telethon.tl import types
from telethon.tl.functions.channels import GetChannelsRequest
from telethon.tl.functions.messages import CheckChatInviteRequest, ImportChatInviteRequest

logger = logging.getLogger("Channel")


class SetupError(Exception):
    """Channel / userbot setup එකේ වැරැද්දක් — message එක කෙළින්ම user ට පෙන්වන්න පුළුවන්."""


@dataclass
class ChannelRef:
    kind: str            # "id" | "username" | "invite"
    value: str           # id (marked නැති, ධන int string) | username | invite hash

    @property
    def channel_id(self) -> Optional[int]:
        return int(self.value) if self.kind == "id" else None


_INVITE_RE = re.compile(r"(?:https?://)?(?:t|telegram)\.me/(?:joinchat/|\+)([\w-]+)", re.I)
_USERNAME_RE = re.compile(r"(?:https?://)?(?:t|telegram)\.me/([A-Za-z]\w{3,})/?$|^@?([A-Za-z]\w{3,})$")


def parse_channel_ref(raw: str) -> ChannelRef:
    raw = (raw or "").strip()
    if not raw:
        raise SetupError("CHANNEL_ID හිස්. -100… id එකක්, @username එකක් හෝ invite link එකක් දෙන්න.")
    m = _INVITE_RE.search(raw)
    if m:
        return ChannelRef("invite", m.group(1))
    if re.fullmatch(r"-?\d+", raw):
        n = raw.lstrip("-")
        if raw.startswith("-100"):
            n = raw[4:]
        elif raw.startswith("-"):
            raise SetupError(
                f"CHANNEL_ID '{raw}' සාමාන්‍ය group එකක id එකක් (channel/supergroup නෙවෙයි). "
                "Private CHANNEL එකක් (id එක -100 වලින් පටන් ගන්නවා) හෝ supergroup එකක් භාවිතා කරන්න.")
        if not n or int(n) == 0:
            raise SetupError(f"CHANNEL_ID '{raw}' වැරදියි.")
        return ChannelRef("id", str(int(n)))
    m = _USERNAME_RE.match(raw)
    if m:
        return ChannelRef("username", m.group(1) or m.group(2))
    raise SetupError(f"CHANNEL_ID '{raw}' තේරුම් ගන්න බැහැ. -100… id / @username / https://t.me/+… link එකක් දෙන්න.")


def _as_channel(entity, source: str):
    if not isinstance(entity, types.Channel):
        raise SetupError(f"{source}: ලැබුණේ channel/supergroup එකක් නෙවෙයි ({type(entity).__name__}).")
    return entity


async def resolve_for_user(client, ref: ChannelRef, who: str):
    """Userbot එකකට channel entity එක (access_hash සහිතව) ගන්නවා. Invite link නම් join වෙනවා."""
    try:
        if ref.kind == "invite":
            try:
                info = await client(CheckChatInviteRequest(ref.value))
            except (InviteHashExpiredError, InviteHashInvalidError):
                raise SetupError(f"{who}: invite link එක expire / invalid.")
            if isinstance(info, types.ChatInviteAlready):
                return _as_channel(info.chat, who)
            try:
                upd = await client(ImportChatInviteRequest(ref.value))
            except UserAlreadyParticipantError:
                pass
            else:
                chats = [c for c in getattr(upd, "chats", []) if isinstance(c, types.Channel)]
                if chats:
                    return chats[0]
            info = await client(CheckChatInviteRequest(ref.value))
            if isinstance(info, types.ChatInviteAlready):
                return _as_channel(info.chat, who)
            raise SetupError(f"{who}: invite link එකෙන් join වෙන්න බැරි වුණා (admin approval ඕනෑ වෙන්න පුළුවන්).")

        if ref.kind == "username":
            return _as_channel(await client.get_entity(ref.value), who)

        # id: පළමුව cache, එහෙම නැත්නම් dialogs load කරලා cache කරනවා
        peer = types.PeerChannel(ref.channel_id)
        try:
            return _as_channel(await client.get_entity(peer), who)
        except (ValueError, ChannelPrivateError, ChannelInvalidError):
            pass
        async for _ in client.iter_dialogs():          # සියලු dialogs → entity cache
            pass
        try:
            return _as_channel(await client.get_entity(peer), who)
        except (ValueError, ChannelPrivateError, ChannelInvalidError):
            raise SetupError(
                f"{who}: channel -100{ref.value} හොයාගන්න බැහැ. මේ userbot account එක channel එකේ member නෙවෙයි. "
                "Account එක channel එකට add කරන්න (හෝ CHANNEL_ID එක invite link එකක් විදිහට දෙන්න — එතකොට auto-join වෙනවා).")
    except SetupError:
        raise
    except RPCError as e:
        raise SetupError(f"{who}: channel resolve error — {type(e).__name__}: {e}")


async def check_user_perms(client, entity, who: str, need_post: bool = True):
    """Userbot ට channel එකට upload කරන්න (post) පුළුවන්ද?"""
    try:
        perms = await client.get_permissions(entity, "me")
    except RPCError as e:
        raise SetupError(f"{who}: channel permissions බලන්න බැහැ — {type(e).__name__}")
    if not need_post:
        return
    if entity.broadcast and not (perms.is_admin and perms.post_messages):
        raise SetupError(
            f"{who}: channel එකේ Admin නෑ (Post Messages permission ඕනේ). Broadcast channel එකක userbot එකට upload කරන්න "
            "admin කරන්න ඕනේ — නැත්නම් channel එක වෙනුවට private SUPERGROUP එකක් භාවිතා කරන්න.")
    if not entity.broadcast and perms.send_messages is False:
        raise SetupError(f"{who}: group එකේ message යවන්න අවසර නෑ.")


async def resolve_for_bot(bot, ref: ChannelRef, ent_from_user, pinger, timeout: float = 20.0):
    """
    Main bot ට channel එකේ InputPeer එක ලබාගන්නවා.
    ent_from_user = userbot එකකින් ගත්ත Channel entity (id සඳහා), pinger = async fn(): userbot එකෙන් channel එකට ping.
    """
    cid = ent_from_user.id
    peer = types.PeerChannel(cid)

    async def cached():
        try:
            return await bot.get_input_entity(peer)
        except (ValueError, TypeError):
            return None

    # 1) දැනටමත් cache වෙලාද?
    got = await cached()
    # 2) public username
    if got is None and getattr(ent_from_user, "username", None):
        try:
            await bot.get_entity(ent_from_user.username)
            got = await cached()
        except Exception as e:
            logger.debug(f"bot username resolve failed: {e!r}")
    # 3) raw GetChannels (bot admin නම් සමහර විට hash 0 වලට වුණත් response එක එනවා; response එකෙන් cache වෙනවා)
    if got is None:
        try:
            await bot(GetChannelsRequest([types.InputChannel(cid, 0)]))
            got = await cached()
        except Exception as e:
            logger.debug(f"bot GetChannels(hash=0) failed: {e!r}")
    # 4) Userbot ping → bot ට update එකක් ලැබෙනවා (bot admin නම්) → entity cache වෙනවා
    if got is None:
        seen = asyncio.Event()

        async def on_raw(update):
            msg = getattr(update, "message", None)
            pc = getattr(msg, "peer_id", None)
            if isinstance(pc, types.PeerChannel) and pc.channel_id == cid:
                seen.set()

        bot.add_event_handler(on_raw, events.Raw(types.UpdateNewChannelMessage))
        try:
            ping_id = await pinger()
            try:
                await asyncio.wait_for(seen.wait(), timeout)
            except asyncio.TimeoutError:
                pass
            got = await cached()
            if ping_id:
                try:
                    await bot.delete_messages(got, ping_id) if got else None
                except Exception:
                    pass
        finally:
            bot.remove_event_handler(on_raw)

    if got is None:
        raise SetupError(
            "Main BOT ට channel එක අඳුරගන්න බැහැ. විසඳුම: (1) bot එක channel එකට ADMIN විදිහට add කරන්න "
            "(Post Messages + Delete Messages permissions), (2) CHANNEL_ID එක නිවැරදිද බලන්න (-100…), "
            "(3) වෙනත් bot token එකක් නම් ඒ bot එකම add කළාද බලන්න. පස්සේ bot එක restart කරන්න.")
    return got


async def check_bot_perms(bot, input_peer, entity_from_user):
    """Bot ට forward (post) + read කරන්න පුළුවන්ද? (admin ඕනේ)"""
    try:
        perms = await bot.get_permissions(input_peer, "me")
    except Exception as e:
        raise SetupError(f"Main bot: channel permissions බලන්න බැහැ ({type(e).__name__}). Bot එක channel එකේ admin කරන්න.")
    if not perms.is_admin:
        raise SetupError("Main bot channel එකේ ADMIN නෙවෙයි. Admin කරන්න (Post Messages + Delete Messages).")
    if entity_from_user.broadcast and not perms.post_messages:
        raise SetupError("Main bot ට 'Post Messages' permission නෑ. Channel admin settings වල ඒක on කරන්න.")
