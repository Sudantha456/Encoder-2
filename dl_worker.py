"""
dl_worker.py — Helper GitHub runner: download ONE exact byte range of the source video
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Main runner file එක analyze කරලා bytes range හරියටම බෙදනවා — මේ runner එකට දෙන range එක
[RANGE_START, RANGE_END) — එක byte එකක්වත් වෙනත් runner එකක් ගන්නේ නෑ, miss වෙන්නෙත් නෑ.
  1. Telegram GetFileRequest 1MB chunks (ranges 1MB aligned — main-side dlparts.byte_ranges)
  2. Private channel එකෙන්ම forwarded video message එක read කරනවා (userbot member යි — file_reference fresh)
  3. BOT_TOKEN secret එකෙන් Telegram message එක edit කරලා progress (PROGD line) → main runner aggregate
  4. අවසන් කළාම range එක own repo එකේ release `enc-dl-<job>` එකට `dl_slice_<n>` ලෙස upload

Env (dispatch inputs): JOB, SLICE, RANGE_START, RANGE_END, TOTAL_SIZE, CH_MSG, CONNECTIONS, CHAT, MSG_ID
Secrets: DL_SESSION (මේ helper account එකේ Telegram userbot), API_ID, API_HASH, BOT_TOKEN, CHANNEL_NUM
"""
import asyncio
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request

CHUNK_SIZE = 1024 * 1024                       # ranges 1MB aligned (main-side dlparts.byte_ranges)
SPEED_WIN = 10.0
HB_CHECK = 30


def env_int(name, default=0):
    try:
        return int(os.environ.get(name, default))
    except Exception:
        return default


JOB = os.environ.get("JOB", "")
SLICE = env_int("SLICE")
START = env_int("RANGE_START")
END = env_int("RANGE_END")
TOTAL = env_int("TOTAL_SIZE")
CH_MSG = env_int("CH_MSG")
CONNECTIONS = max(1, env_int("CONNECTIONS", 12))
CHAT = os.environ.get("CHAT", "")
MSG_ID = env_int("MSG_ID")
CHANNEL_NUM = env_int("CHANNEL_NUM")
CHANNEL_REF = os.environ.get("CHANNEL_REF", "").strip()   # raw CHANNEL_ID (id/@username/invite)
SLICE_LEN = max(0, END - START)
TAG = f"enc-dl-{JOB}"
NAME = f"dl_slice_{SLICE}"


def gh(*args, check=True):
    r = subprocess.run(["gh", *args], capture_output=True, text=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"gh {' '.join(args[:2])} failed: {r.stderr[-300:]}")
    return r


class Reporter:
    def __init__(self):
        self.token = os.environ.get("BOT_TOKEN", "")

    def send(self, state, done, speed, note=""):
        text = (f"🧩 Slice {SLICE + 1} • {state} • {done / max(SLICE_LEN, 1) * 100:.0f}%{(' • ' + note) if note else ''}\n"
                f"📦 {done / 1048576:.1f} / {SLICE_LEN / 1048576:.1f} MB • {speed / 1048576:.1f} MB/s\n"
                f"PROGD|{JOB}|{SLICE}|{state}|{done}|{SLICE_LEN}|{speed:.1f}")
        if self.token and CHAT and MSG_ID:
            data = urllib.parse.urlencode(
                {"chat_id": CHAT, "message_id": MSG_ID, "text": text}).encode()
            try:
                urllib.request.urlopen(f"https://api.telegram.org/bot{self.token}/editMessageText",
                                       data, timeout=15).read()
            except Exception as e:
                print(f"[report] {e}", flush=True)
        else:
            print(text, flush=True)


def speed_tracker():
    samples = [[time.monotonic(), 0]]

    def push(done):
        now = time.monotonic()
        samples.append([now, done])
        while len(samples) > 2 and now - samples[0][0] > SPEED_WIN:
            samples.pop(0)

    def speed():
        (t0, c0), (t1, c1) = samples[0], samples[-1]
        if t1 - t0 < 0.5:
            return 0.0
        return max(0.0, (c1 - c0) / (t1 - t0))

    return push, speed


async def resolve_channel(client):
    """Channel entity resolve.

    පළමුව CHANNEL_REF (raw CHANNEL_ID — id / @username / invite link) — channel.py එකේ battle-tested logic
    (invite link නම් මේ account එක auto-JOIN වෙනවා). නැතිනම් CHANNEL_NUM (numeric id) dialogs scan."""
    if CHANNEL_REF:
        from channel import parse_channel_ref, resolve_for_user
        return await resolve_for_user(client, parse_channel_ref(CHANNEL_REF), f"slice {SLICE}")
    want = CHANNEL_NUM
    if want > 1_000_000_000_000:          # full -100… form → ඇතුළේ channel id එක විතරක්
        want = int(str(want)[4:])
    n_dialogs, seen, matched = 0, [], None
    async for dialog in client.iter_dialogs(limit=None):
        n_dialogs += 1
        peer = getattr(dialog, "peer", None)
        is_chan = getattr(peer, "channel_id", None) is not None
        pid = getattr(peer, "channel_id", None) or getattr(peer, "chat_id", None) or getattr(peer, "user_id", None)
        if is_chan and pid is not None:
            seen.append(pid)
        if pid == want or pid == CHANNEL_NUM:
            matched = dialog.entity
            break
    if matched is not None:
        return matched
    print(f"[diag] dialogs scanned={n_dialogs} channels seen={len(seen)} first={seen[:12]}", flush=True)
    raise RuntimeError(f"channel {CHANNEL_NUM} dialogs එකේ නෑ — userbot මේ channel එකේ member නෙවෙයි "
                       f"(channels seen: {len(seen)})")


async def main():
    if not JOB or SLICE_LEN <= 0 or TOTAL <= 0 or not CH_MSG:
        print("bad config", file=sys.stderr, flush=True)
        sys.exit(2)
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    from telethon.tl.functions.upload import GetFileRequest
    from telethon.tl.types import InputDocumentFileLocation
    from telethon.errors import FloodWaitError
    from fast_conn import SenderPool

    rep = Reporter()
    push, speed = speed_tracker()
    done_bytes = 0
    t0 = time.monotonic()
    rep.send("running", 0, 0.0, "connecting")

    session = os.environ.get("DL_SESSION", "").strip()
    api_id = env_int("API_ID")
    api_hash = os.environ.get("API_HASH", "").strip()
    if not session:
        rep.send("failed", 0, 0.0, "DL_SESSION secret නෑ")
        sys.exit(3)
    if not api_id or not api_hash:
        rep.send("failed", 0, 0.0, "API_ID / API_HASH secret නෑ")
        sys.exit(3)
    if not CHANNEL_NUM and not CHANNEL_REF:
        rep.send("failed", 0, 0.0, "CHANNEL_NUM / CHANNEL_REF secret දෙකම නෑ")
        sys.exit(3)

    client = TelegramClient(StringSession(session), api_id, api_hash, flood_sleep_threshold=0)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            rep.send("failed", 0, 0.0, "DL_SESSION invalid")
            sys.exit(3)
        entity = await resolve_channel(client)
        msg = await client.get_messages(entity, ids=CH_MSG)
        doc = getattr(msg, "document", None) if msg else None
        if not doc:
            rep.send("failed", 0, 0.0, "channel message එකේ video නෑ")
            sys.exit(4)
        if int(doc.size) != TOTAL:
            rep.send("failed", 0, 0.0, f"size mismatch {doc.size} != {TOTAL}")
            sys.exit(4)
        dc_id = getattr(doc, "dc_id", 0)
        location = InputDocumentFileLocation(id=doc.id, access_hash=doc.access_hash,
                                             file_reference=doc.file_reference, thumb_size="")
    except SystemExit:
        raise
    except Exception as e:
        rep.send("failed", 0, 0.0, f"setup: {str(e)[:80]}")
        sys.exit(3)

    pool = SenderPool(client, dc_id, CONNECTIONS, use_main=False)
    await asyncio.wait_for(pool.start(), timeout=60.0)

    path = f"dl_slice_{SLICE}.bin"
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
    os.ftruncate(fd, SLICE_LEN)

    queue: asyncio.Queue = asyncio.Queue()
    for off in range(START, END, CHUNK_SIZE):
        queue.put_nowait((off, min(CHUNK_SIZE, END - off)))   # slice මායිමෙන් පිටතට කවදාවත් යන්නේ නෑ

    stop = asyncio.Event()
    fail = asyncio.get_event_loop().create_future()

    async def watchdog():
        while not stop.is_set():
            await asyncio.sleep(HB_CHECK)
            r = await asyncio.to_thread(gh, "release", "view", TAG, "--json", "id", check=False)
            if r.returncode != 0 and "not found" in (r.stderr or "").lower():
                print("release gone — aborting", flush=True)
                stop.set()
                return

    async def ticker():
        while not stop.is_set():
            await asyncio.sleep(3)
            push(done_bytes)
            rep.send("running", done_bytes, speed())

    flush_lock = asyncio.Lock()

    async def worker(widx: int):
        nonlocal done_bytes
        while not stop.is_set():
            try:
                off, lim = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            req = GetFileRequest(location=location, offset=off, limit=lim)
            for attempt in range(8):
                if stop.is_set():
                    return
                try:
                    res = await asyncio.wait_for(pool.send(req, widx + attempt), timeout=120.0)
                    data = getattr(res, "bytes", None)
                    if data is None:
                        raise RuntimeError(f"Unexpected response {type(res).__name__}")
                    if len(data) != lim:
                        raise RuntimeError(f"Short read {len(data)}/{lim} @ {off}")
                    async with flush_lock:
                        os.pwrite(fd, data, off - START)
                        done_bytes += len(data)
                    push(done_bytes)
                    break
                except FloodWaitError as e:
                    print(f"FloodWait {e.seconds}s — lane paused", flush=True)
                    await asyncio.sleep(min(e.seconds, 90) + 1)
                except Exception as e:
                    if attempt == 7:
                        if not fail.done():
                            fail.set_exception(RuntimeError(f"chunk @{off}: {e!r}"))
                        return
                    await asyncio.sleep(min(2 ** attempt, 8))
            else:
                return

    wd = asyncio.create_task(watchdog())
    tk = asyncio.create_task(ticker())
    tasks = [asyncio.create_task(worker(i)) for i in range(min(CONNECTIONS * 2, queue.qsize()))]
    try:
        await asyncio.gather(*tasks)
        if done_bytes != SLICE_LEN:
            raise RuntimeError(f"Incomplete slice {done_bytes}/{SLICE_LEN}")
        stop.set()
    except BaseException as e:
        stop.set()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        wd.cancel(); tk.cancel()
        await pool.close()
        os.close(fd)
        await client.disconnect()
        rep.send("failed", done_bytes, 0.0, str(e)[:80])
        raise

    wd.cancel(); tk.cancel()
    await asyncio.gather(wd, tk, return_exceptions=True)
    os.close(fd)
    await pool.close()
    await client.disconnect()

    # අවසන්: release එක හදාගෙන slice එක upload (byte-exact size verify main runner එකෙන් වෙනවා)
    if os.path.exists(path):
        os.replace(path, NAME)      # asset name = dl_slice_<n> (main & asm අපේක්ෂා කරන්නේ මේ නම)
        path = NAME
    r = gh("release", "view", TAG, "--json", "id", check=False)
    if r.returncode != 0:
        gh("release", "create", TAG, "--prerelease", "--title", f"Encoder download {TAG}",
           "--notes", f"encoder dl slice {SLICE}")
    gh("release", "upload", TAG, path, "--clobber")
    rep.send("done", done_bytes, 0.0, f"{time.monotonic() - t0:.0f}s")
    print(f"slice {SLICE} done: {done_bytes} bytes in {time.monotonic() - t0:.1f}s", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
