"""
worker.py — GitHub Actions worker
Modes:
  • serve: queue session එකට bot විසින් dispatch කරනවා → ready වෙලා hold → job එන්නම encode → queue ඉවර වුණාම exit.
      Job signal = Release "enc-q-<SESSION>" හි body (ETag conditional polling → rate limit එකට ගණන් වෙන්නේ නෑ)
  • single (dispatch): CONFIG env එකෙන් එක කොටසක් encode කරලා ඉවර වෙනවා
Progress: Telegram Bot API editMessageText (main bot හදපු message එක edit කරනවා).
Env: PART, SESSION, BOT_TOKEN, GH_TOKEN, GH_REPO  (single mode: CONFIG)
"""
import asyncio
import json
import os
import shutil
import subprocess
import sys
import time
import types
import urllib.error
import urllib.parse
import urllib.request

from transcoder import EncState, transcode

QUEUE_TAG = "enc-queue"
POLL = float(os.getenv("WORKER_POLL", "3"))
HB_STALE = 180        # main heartbeat මේ තත්පර ගණනට වඩා පරණ නම් worker exit
JOB_MAX_AGE = 300
STANDBY_BASE = 100    # idx >= 100 → hot standby worker (කොටසක් නැතුව ready වෙලා ඉඳලා main assign කරන part එක encode කරනවා)


class Reporter:
    def __init__(self, cfg, idx):
        self.cfg, self.idx = cfg, idx
        self.token = os.getenv("BOT_TOKEN", "")
        self.msg_id = cfg["ids"][idx]
        self.test_dir = os.getenv("PROGRESS_DIR")

    def _send_blocking(self, text):
        if self.test_dir:
            with open(os.path.join(self.test_dir, f"{self.idx}.txt"), "w") as f:
                f.write(text)
            return
        if not self.token:
            print(text)
            return
        data = urllib.parse.urlencode({"chat_id": self.cfg["chat"], "message_id": self.msg_id, "text": text}).encode()
        try:
            urllib.request.urlopen(f"https://api.telegram.org/bot{self.token}/editMessageText", data, timeout=15).read()
        except Exception as e:  # not modified / flood — ignore
            print(f"[report] {e}", flush=True)

    async def send(self, status, st: EncState, note=""):
        text = (f"🧩 Part {self.idx + 1} • {status} • {st.percent:.0f}%{(' • ' + note) if note else ''}\n"
                f"PROG|{self.cfg['job']}|{self.idx}|{status}|{st.out_time:.2f}|{st.fps:.1f}|{st.speed:.2f}|{st.out_size}|{st.frame}")
        await asyncio.to_thread(self._send_blocking, text)


def gh(*args, check=True):
    r = subprocess.run(["gh", *args], capture_output=True, text=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"gh {' '.join(args[:2])} failed: {r.stderr[-300:]}")
    return r


async def run_part(cfg: dict, idx: int, workdir: str = "."):
    """එක කොටසක් encode + upload. Fail වුණොත් 'failed' report කරලා exception raise."""
    tag = f"enc-{cfg['job']}"
    raw = os.path.join(workdir, f"raw_part_{idx}.mp4")
    dst = os.path.join(workdir, f"encoded_part_{idx}.mkv")
    st = EncState(duration=cfg["durations"][idx], src_fps=cfg["fps"])
    rep = Reporter(cfg, idx)
    ctl = types.SimpleNamespace(cancelled=False, proc=None)
    interval = float(cfg.get("interval", 3))

    async def ticker():
        while True:
            await rep.send("running", st)
            await asyncio.sleep(interval)

    async def watchdog():  # Release delete වුණොත් (cancel / main ඉවර) නවතිනවා
        while True:
            await asyncio.sleep(30)
            r = await asyncio.to_thread(gh, "release", "view", tag, "--json", "id", check=False)
            if r.returncode != 0 and "not found" in r.stderr.lower():
                ctl.cancelled = True
                if ctl.proc:
                    ctl.proc.kill()
                return

    bg = []
    try:
        await rep.send("running", st)
        if not os.path.exists(raw):
            await asyncio.to_thread(gh, "release", "download", tag, "-p", f"raw_part_{idx}.mp4",
                                    "-D", workdir, "--clobber")
        if not os.path.exists(raw):
            raise RuntimeError("raw part not found")
        bg = [asyncio.create_task(ticker()), asyncio.create_task(watchdog())]
        await transcode(raw, dst, st, ctl, target=cfg["target"], scale=cfg["scale"], portrait=cfg["portrait"],
                        preset=cfg["preset"], codec=cfg["codec"], crf=cfg["crf"], maxrate_kbps=cfg["maxrate"],
                        audio_mode="none", container="mkv")
        for t in bg:
            t.cancel()
        await rep.send("uploading", st)
        await asyncio.to_thread(gh, "release", "upload", tag, dst, "--clobber")
        st.finished = True
        await rep.send("done", st)
    except BaseException as e:
        for t in bg:
            t.cancel()
        await rep.send("failed", st, str(e)[:60].replace("\n", " "))
        raise


# ───────────────────────── persistent (serve) mode ─────────────────────────
def poll_queue(tag, etag):
    """(status, etag, release_json). 304 = වෙනසක් නෑ (rate limit ගණන් වෙන්නේ නෑ)."""
    qf = os.getenv("QUEUE_FILE")          # local test hook
    if qf:
        if not os.path.exists(qf):
            return 404, None, None
        m = str(os.path.getmtime(qf))
        if m == etag:
            return 304, etag, None
        return 200, m, json.load(open(qf))
    repo, token = os.environ["GH_REPO"], os.environ["GH_TOKEN"]
    req = urllib.request.Request(
        f"https://api.github.com/repos/{repo}/releases/tags/{tag}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
                 "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "encoder-worker"})
    if etag:
        req.add_header("If-None-Match", etag)
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return 200, r.headers.get("ETag"), json.loads(r.read())
    except urllib.error.HTTPError as e:
        if e.code == 304:
            return 304, etag, None
        if e.code == 404:
            return 404, None, None
        raise


def parse_hb(name) -> float:
    try:
        return float(str(name).split("hb", 1)[1].strip())
    except Exception:
        return 0.0


def announce_ready(tag, idx):
    """Standby worker එක ready බව main ට කියනවා: mailbox release එකට ready_<idx> asset එකක්."""
    if os.getenv("QUEUE_FILE"):
        return
    try:
        p = f"ready_{idx}"
        with open(p, "w") as f:
            f.write("1")
        gh("release", "upload", tag, p, "--clobber", check=False)
    except Exception as e:
        print(f"[worker {idx}] ready announce failed: {e}", flush=True)


async def serve():
    """Queue session එක පුරා hold වෙලා ඉඳලා jobs encode කරනවා. Queue හිස් වුණාම (state=end / release මැකුණාම) exit."""
    idx = int(os.environ["PART"])
    session = os.environ.get("SESSION", "")
    tag = f"enc-q-{session}" if session else QUEUE_TAG
    etag, hb = None, 0.0
    done: set = set()                     # (job, part) — එකම part එක දෙපාරක් encode නොකරන්න
    standby = idx >= STANDBY_BASE
    poll = min(POLL, 2.0) if standby else POLL
    start, seen = time.time(), False
    print(f"[worker {idx}] ready — session {session or '-'}; holding until queue ends (poll {poll}s)"
          f"{' [standby]' if standby else ''}", flush=True)
    if standby:
        await asyncio.to_thread(announce_ready, tag, idx)
    while True:
        try:
            status, new_etag, rel = await asyncio.to_thread(poll_queue, tag, etag)
        except Exception as e:
            print(f"[worker {idx}] poll error: {e}", flush=True)
            await asyncio.sleep(10)
            continue

        if status == 404 and seen:
            print(f"[worker {idx}] queue finished (session closed) — exiting", flush=True)
            return

        if status == 200 and rel:
            etag, seen = new_etag, True
            hb = parse_hb(rel.get("name")) or hb
            try:
                job = json.loads(rel.get("body") or "{}")
            except Exception:
                job = {}
            if job.get("state") == "end":
                print(f"[worker {idx}] queue empty — exiting", flush=True)
                return
            cfg = job.get("cfg")
            if job.get("state") == "job" and cfg and time.time() - job.get("ts", 0) < JOB_MAX_AGE:
                asg = job.get("assign") or {}
                part = None
                if str(idx) in asg:                       # main විසින් මේ worker එකට කෙලින්ම දුන්න part (hot standby / rescue)
                    part = int(asg[str(idx)])
                elif idx < cfg["n"] and idx not in (job.get("skip") or []):   # සාමාන්‍ය: worker idx = part idx
                    part = idx
                key = (job.get("job"), part)
                if part is not None and part < cfg["n"] and key not in done:
                    done.add(key)
                    print(f"[worker {idx}] job {key[0]} → part {part}", flush=True)
                    wd = os.path.join("work", f"{key[0]}_{part}")
                    os.makedirs(wd, exist_ok=True)
                    try:
                        await run_part(cfg, part, wd)
                    except BaseException as e:
                        print(f"[worker {idx}] job failed: {e}", file=sys.stderr, flush=True)
                    finally:
                        shutil.rmtree(wd, ignore_errors=True)
                    etag = None                           # ඊළඟ poll එකේ අලුත්ම mailbox state එක ගන්න
                    continue                              # ඉවර → ආයෙත් hold (queue එකේ ඊළඟ job / assign එකට)

        now = time.time()
        if seen and hb and now - hb > HB_STALE:
            print(f"[worker {idx}] main heartbeat lost — exiting", flush=True)
            return
        if not seen and now - start > 600:
            print(f"[worker {idx}] session never appeared — exiting", flush=True)
            return
        await asyncio.sleep(poll)


async def single():
    cfg = json.loads(os.environ["CONFIG"])
    idx = int(os.environ["PART"])
    if idx >= cfg["n"]:
        print("no part for this worker")
        return
    try:
        await run_part(cfg, idx)
    except BaseException as e:
        print(f"worker failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(single() if os.getenv("CONFIG") and os.getenv("WORKER_MODE") != "serve" else serve())
