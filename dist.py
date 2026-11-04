"""
dist.py — Distributed encoding: main runner ↔ GitHub Actions workers (queue-aware pool)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• Queue එකේ පළමු job එක download වෙන්න පටන් ගන්නකොටම WorkerPool එක settings වල workers ගාණට
  runners dispatch කරනවා → download එක ඉවර වෙද්දී runners ready.
• Workers queue එක ඉවර වෙනකම් hold වෙනවා (job එකෙන් job එකට). Queue හිස් වුණාම pool එක stop → workers exit.
• Download අතරතුර settings වල workers ගාන වැඩි කළොත් අලුත් runners ඉක්මනින් dispatch; අඩු කළොත්
  ඉතුරු workers hold (ඊළඟ job වලට ආයෙ වැඩි කළොත් ready).
Job එකකට:
  1. keyframe boundaries වල ffmpeg -c copy වලින් කොටස් N කට කඩනවා  (N = ඒ මොහොතේ settings workers)
  2. කොටස් temp Release එකකට upload → pool mailbox එකට job post
  3. Workers encode කරලා Release එකටම දානවා; progress Telegram message edit වලින්
  4. Main: කොටස් download කරලා concat + audio mux;  Worker fail / start නොවුණොත් ඒ කොටස main එකම encode (fallback)
"""
import asyncio
import base64
import contextlib
import json
import logging
import os
import re
import shutil
import time
import types
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import config
from config import PROGRESS_UPDATE_INTERVAL
from pathlib import Path as _P
from transcoder import EncState, transcode

logger = logging.getLogger("Dist")


class DistError(Exception):
    pass




def available() -> bool:
    return bool(shutil.which("gh") and os.getenv("GH_TOKEN")
                and os.getenv("GITHUB_REPOSITORY"))


# ───────────── 🤝 Helper GitHub accounts ─────────────
HELPERS: List[tuple] = []          # (token, "owner/repo") — bootstrap සාර්ථක වූ ඒවා
HELPER_PROBLEMS: List[str] = []
PER_ACCT = config.WORKERS_PER_ACCOUNT


def capacity() -> int:
    """Main account (10) + helper accounts (10 බැගින්) — settings workers මීට වැඩි වෙන්නේ නෑ."""
    return PER_ACCT * (1 + len(HELPERS))


async def _helper_gh(token: str, repo: str, *args, check=True) -> str:
    env = {**os.environ, "GH_TOKEN": token}
    env.pop("GH_REPO", None)
    proc = await asyncio.create_subprocess_exec("gh", *args, env=env, stdout=asyncio.subprocess.PIPE,
                                                stderr=asyncio.subprocess.PIPE)
    out, err = await proc.communicate()
    if check and proc.returncode != 0:
        raise DistError(f"gh {' '.join(args[:2])}: {err.decode(errors='ignore')[-200:].strip()}")
    return out.decode(errors="ignore")


async def _bootstrap_helper(token: str) -> tuple:
    st = ["login"]
    try:
        return await _bootstrap_inner(token, st)
    except Exception as e:                       # කුමන පියවරේද fail වුණේ කියලා පෙන්නනවා
        raise DistError(f"පියවර '{st[0]}' fail: {e}") from e


async def _bootstrap_inner(token: str, st: list) -> tuple:
    """Helper account එකේ public repo එක හදලා worker files + BOT_TOKEN secret එක දානවා (idempotent)."""
    login = (await _helper_gh(token, "", "api", "user", "-q", ".login")).strip()
    repo = f"{login}/{config.HELPER_REPO}"
    st[0] = "repo create/check"
    # repo එක දැනටමත් තියෙනවද බලනවා — තියෙනවා නම් අලුත් එකක් හදන්නේ නෑ (එකම නමින් repo එකම යළි භාවිතා කරනවා)
    got = (await _helper_gh(token, repo, "api", f"repos/{repo}", "-q", ".full_name", check=False)).strip()
    if got.lower() != repo.lower():          # 404 වුණාම gh error body එකත් stdout එකට print කරනවා → නමෙන්ම සසඳනවා
        try:
            await _helper_gh(token, repo, "api", "-X", "POST", "user/repos", "-f", f"name={config.HELPER_REPO}",
                             "-F", "private=false", "-F", "auto_init=true", "-f", "description=encoder worker")
            logger.info(f"🤝 repo created: {repo}")
            await asyncio.sleep(3)
        except DistError as e:
            if "already exists" not in str(e).lower():       # check එක අසාර්ථක වුණත් repo තියෙනවා නම් ඉදිරියට
                raise
    else:
        logger.info(f"🤝 repo exists: {repo} — නැවත හදන්නේ නෑ")
    root = _P(__file__).resolve().parent
    files = {"worker.py": root / "worker.py", "transcoder.py": root / "transcoder.py",
             ".github/workflows/encode-worker.yml": root / ".github/workflows/encode-worker.yml"}
    for path, src in files.items():
        st[0] = f"push {path} (token එකට 'workflow' scope ඕනේ)"
        raw = src.read_bytes()
        cur = await _helper_gh(token, repo, "api", f"repos/{repo}/contents/{path}", check=False)
        sha = ""
        try:
            j = json.loads(cur or "{}")
            if base64.b64decode(j.get("content", "")) == raw:
                continue                                  # වෙනසක් නෑ
            sha = j.get("sha", "")
        except Exception:
            pass
        args = ["api", "-X", "PUT", f"repos/{repo}/contents/{path}", "-f", f"message=update {path}",
                "-f", "content=" + base64.b64encode(raw).decode()]
        if sha:
            args += ["-f", f"sha={sha}"]
        await _helper_gh(token, repo, *args)
    st[0] = "secret BOT_TOKEN"
    try:
        await _helper_gh(token, repo, "secret", "set", "BOT_TOKEN", "--repo", repo, "--body", config.BOT_TOKEN)
    except Exception as e:                       # fatal නෑ: workers වැඩ කරනවා, progress edits විතරක් නැති වෙනවා
        HELPER_PROBLEMS.append(f"{repo}: BOT_TOKEN secret set වුණේ නෑ ({e}) — progress පෙන්වන්නේ නෑ")
    return token, repo


async def setup_helpers():
    HELPERS.clear()
    HELPER_PROBLEMS.clear()
    res = await asyncio.gather(*[_bootstrap_helper(t) for t in config.HELPER_TOKENS], return_exceptions=True)
    for i, r in enumerate(res, 1):
        if isinstance(r, BaseException):
            t = config.HELPER_TOKENS[i - 1]
            HELPER_PROBLEMS.append(f"Token #{i} (…{t[-4:]}): {r}")
            logger.error(HELPER_PROBLEMS[-1])
        else:
            HELPERS.append(r)
            logger.info(f"🤝 token #{i} ready: {r[1]}")


# ───────────── single-runner encode time estimate (auto mode) ─────────────
ENC_SPEED_720P = {"ultrafast": 6.5, "superfast": 5.0, "veryfast": 3.5, "faster": 2.8, "fast": 2.2,
                  "medium": 1.6, "slow": 0.9, "slower": 0.5, "veryslow": 0.25}


def estimate_encode_seconds(meta, target, preset, codec) -> float:
    from media import target_dims
    tw, th = target_dims(meta, target)
    sp = ENC_SPEED_720P.get(preset, 1.0) * (921600 / max(tw * th, 1)) * (0.4 if codec == "libx265" else 1.0)
    return meta.duration / max(sp, 0.05)


def should_distribute(meta, target, preset, codec, mode: str) -> bool:
    if mode == "off" or not available() or meta.duration < 30:
        return False
    if mode == "on":
        return True
    return estimate_encode_seconds(meta, target, preset, codec) >= config.DIST_AUTO_MIN_SECONDS


# ───────────────────────── state (UI render කරන්නේ මේකෙන්) ─────────────────────────
@dataclass
class Part:
    idx: int
    start: float
    duration: float
    status: str = "waiting"      # waiting|running|uploading|remote_done|failed|local|ready
    out_time: float = 0.0
    fps: float = 0.0
    speed: float = 0.0
    size: int = 0
    frame: int = 0
    ready: bool = False
    fetching: bool = False
    local_started: bool = False
    local_state: Optional[EncState] = None
    path: str = ""
    last_raw: str = ""
    last_change: float = field(default_factory=time.monotonic)
    acct: int = 0                 # ඒ කොටස encode වෙන GitHub account index (0 = main)
    spilled: bool = False
    spill_t: float = 0.0

    @property
    def pct(self) -> float:
        return 100.0 if self.ready else min(self.out_time / self.duration * 100, 99.9) if self.duration else 0.0


class DistState:
    def __init__(self, duration: float, src_fps: float, n_workers: int):
        self.duration, self.src_fps, self.n_workers = duration, src_fps, n_workers
        self.phase = "split"
        self.parts: List[Part] = []
        self.t0 = time.monotonic()
        self.enc_t0: Optional[float] = None
        self.uploaded = 0

    @property
    def elapsed(self):
        return time.monotonic() - self.t0

    @property
    def enc_elapsed(self):
        return time.monotonic() - self.enc_t0 if self.enc_t0 else 0.0

    @property
    def out_time(self):
        return sum(p.duration if p.ready else min(p.out_time, p.duration) for p in self.parts)

    @property
    def percent(self):
        return min(self.out_time / self.duration * 100, 100.0) if self.duration else 0.0

    @property
    def speed(self):
        return sum(p.speed for p in self.parts if p.status in ("running", "local") and not p.ready)

    @property
    def fps(self):
        return sum(p.fps for p in self.parts if p.status in ("running", "local") and not p.ready)

    @property
    def frame(self):
        return sum(p.frame for p in self.parts)

    @property
    def out_size(self):
        return sum(p.size for p in self.parts)

    @property
    def avg_speed(self):
        return self.out_time / self.enc_elapsed if self.enc_elapsed > 0 else 0.0

    @property
    def eta(self):
        sp = self.speed or self.avg_speed
        return (self.duration - self.out_time) / sp if sp > 0 else 0.0

    @property
    def est_final_size(self):
        return int(self.out_size / self.percent * 100) if self.percent >= 3 else 0

    @property
    def est_total_frames(self):
        return int(self.duration * self.src_fps) if self.src_fps else 0


# ───────────────────────── GitHub (gh CLI) ─────────────────────────
class GhBackend:
    def __init__(self, token: Optional[str] = None, repo: Optional[str] = None):
        self.env = {**os.environ, "GH_REPO": repo or os.environ["GITHUB_REPOSITORY"]}
        if token:                                  # helper account
            self.env["GH_TOKEN"] = token
        self.ref = "main" if token else os.getenv("GITHUB_REF_NAME", "main")

    async def _gh(self, *args, check=True) -> str:
        proc = await asyncio.create_subprocess_exec(
            "gh", *args, env=self.env,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        out, err = await proc.communicate()
        if check and proc.returncode != 0:
            raise DistError(f"gh {' '.join(args[:2])} failed: {err.decode(errors='ignore')[-300:]}")
        return out.decode(errors="ignore")

    async def release_create(self, tag):
        await self._gh("release", "create", tag, "--prerelease", "--title", f"Temp {tag}", "--notes", "encoder temp")

    async def upload(self, tag, paths):
        await self._gh("release", "upload", tag, *[str(p) for p in paths], "--clobber")

    async def download(self, tag, pattern, dest):
        await self._gh("release", "download", tag, "-p", pattern, "-D", str(dest), "--clobber")

    async def assets(self, tag) -> dict:
        data = json.loads(await self._gh("release", "view", tag, "--json", "assets") or "{}")
        return {a["name"]: a.get("state", "uploaded") for a in data.get("assets", [])}

    async def dispatch_workers(self, session: str, indices):
        await self._gh("workflow", "run", config.DIST_WORKFLOW, "--ref", self.ref,
                       "-f", f"session={session}", "-f", f"indices={json.dumps(list(indices), separators=(',', ':'))}")

    # ── worker pool mailbox: Release "enc-q-<session>"  (body = job signal, title = heartbeat) ──
    async def mailbox_create(self, tag, body):
        await self._gh("release", "create", tag, "--prerelease",
                       "--title", f"Encoder queue hb {int(time.time())}", "--notes", body)

    async def mailbox_set(self, tag, body):
        await self._gh("release", "edit", tag, "--notes", body, check=False)

    async def heartbeat(self, tag):
        await self._gh("release", "edit", tag, "--title", f"Encoder queue hb {int(time.time())}", check=False)

    async def release_delete(self, tag):
        await self._gh("release", "delete", tag, "--cleanup-tag", "-y", check=False)


# ───────────────────────── Worker pool (queue session) ─────────────────────────
class Acct:
    """GitHub account එකක් (0 = main, 1.. = helpers) — තමන්ගේම mailbox session + workers."""

    def __init__(self, idx: int, gh):
        self.idx, self.gh = idx, gh
        self.session = self.tag = ""
        self.dispatched: set = set()      # dispatch කරපු worker indices
        self.active = False
        self.failed = False
        self.hb: Optional[asyncio.Task] = None


class WorkerPool:
    """Queue session එකක් පුරා ජීවත් වෙන workers (main + helper accounts, එකකට PER_ACCT බැගින්)."""

    def __init__(self, gh=None):
        self.accts: List[Acct] = []
        self._lock = asyncio.Lock()

    def build(self):
        want = 1 + len(HELPERS)
        if len(self.accts) != want:
            self.accts = [Acct(0, GhBackend())] + [Acct(i + 1, GhBackend(t, r)) for i, (t, r) in enumerate(HELPERS)]

    @property
    def active(self) -> bool:
        return any(a.active for a in self.accts)

    def _body(self, a: Acct, state="idle", **extra) -> str:
        return json.dumps({"session": a.session, "state": state, "ts": time.time(), **extra}, separators=(",", ":"))

    async def _start(self, a: Acct):
        if not a.active:
            a.session = os.urandom(4).hex()
            a.tag = f"enc-q-{a.session}"
            a.dispatched = set()
            await a.gh.mailbox_create(a.tag, self._body(a))
            a.active, a.failed = True, False
            a.hb = asyncio.create_task(self._heartbeat(a))
            logger.info(f"🌐 [acct {a.idx}] session {a.session} started")

    async def ensure(self, n: int):
        """Workers n ක් ready කරනවා: index i → account i // PER_ACCT. Account එකක් fail වුණොත් ඒ කොටස් පස්සේ spill/local."""
        async with self._lock:
            self.build()
            n = min(n, PER_ACCT * len(self.accts))
            errors = []

            async def one(a: Acct):
                need = {i for i in range(n) if i // PER_ACCT == a.idx}
                if not need:
                    return
                try:
                    await self._start(a)
                    new = sorted(need - a.dispatched)
                    if new:
                        await a.gh.dispatch_workers(a.session, new)
                        a.dispatched |= set(new)
                        logger.info(f"🌐 [acct {a.idx}] dispatched workers {new}")
                except Exception as e:
                    a.failed = True
                    errors.append(e)
                    logger.warning(f"🌐 [acct {a.idx}] dispatch failed: {e}")

            await asyncio.gather(*[one(a) for a in self.accts])
            if errors and self.accts[0].failed:
                raise DistError(f"main account dispatch failed: {errors[0]}")

    async def _heartbeat(self, a: Acct):
        while True:
            await asyncio.sleep(60)
            await a.gh.heartbeat(a.tag)

    async def post_job(self, job_id: str, cfg: dict):
        await asyncio.gather(*[a.gh.mailbox_set(a.tag, self._body(a, "job", job=job_id, cfg=cfg))
                               for a in self.accts if a.active and not a.failed])

    async def clear_job(self):
        await asyncio.gather(*[a.gh.mailbox_set(a.tag, self._body(a)) for a in self.accts if a.active])

    def pick_spare(self, exclude: set) -> Optional[Acct]:
        """Busy/ start නොවුණ part එකකට: තවම free slots තියෙන වෙන account එකක්."""
        c = [a for a in self.accts if a.idx not in exclude and not a.failed and len(a.dispatched) < PER_ACCT]
        return min(c, key=lambda a: len(a.dispatched)) if c else None

    async def stop(self):
        """Queue හිස් → හැම account එකකම workers ට 'end' කියලා release මකනවා (spill කරපු ඒවත් වැහෙනවා)."""
        async with self._lock:
            act = [a for a in self.accts if a.active]
            if not act:
                return
            for a in act:
                a.active = False
                if a.hb:
                    a.hb.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await a.hb
            await asyncio.gather(*[a.gh.mailbox_set(a.tag, self._body(a, "end")) for a in act])
            await asyncio.sleep(6)                      # workers poll කරලා 'end' දකින්න
            await asyncio.gather(*[a.gh.release_delete(a.tag) for a in act])
            for a in act:
                a.dispatched = set()
            logger.info(f"🌐 pool stopped ({len(act)} accounts)")


# ───────────────────────── Telegram progress board ─────────────────────────
PROG_RE = re.compile(r"PROG\|([^|]*)\|(\d+)\|(\w+)\|([\d.]+)\|([\d.]+)\|([\d.]+)\|(\d+)\|(\d+)")


def parse_prog(text: str, job_id: str, idx: int):
    m = PROG_RE.search(text or "")
    if not m or m.group(1) != job_id or int(m.group(2)) != idx:
        return None
    return dict(state=m.group(3), out_time=float(m.group(4)), fps=float(m.group(5)),
                speed=float(m.group(6)), size=int(m.group(7)), frame=int(m.group(8)))


class TgBoard:
    """Workers progress edit කරන messages N ක් (Telethon වලින් හදලා, ids workers ට දෙනවා)."""

    def __init__(self, client, chat_id, job_id, n):
        self.client, self.chat_id, self.job_id, self.n = client, chat_id, job_id, n
        self.ids: List[int] = []

    async def create(self):
        for i in range(self.n):
            m = await self.client.send_message(
                self.chat_id, f"🧩 Part {i + 1} • waiting\nPROG|{self.job_id}|{i}|waiting|0|0|0|0|0",
                parse_mode=None, silent=True)
            self.ids.append(m.id)

    async def read(self) -> List[str]:
        msgs = await self.client.get_messages(self.chat_id, ids=self.ids)
        return [(m.message if m else "") for m in msgs]

    async def delete(self):
        if self.ids:
            with contextlib.suppress(Exception):
                await self.client.delete_messages(self.chat_id, self.ids)


# ───────────────────────── ffmpeg helpers ─────────────────────────
async def keyframes(path: str) -> List[float]:
    proc = await asyncio.create_subprocess_exec(
        "ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "packet=pts_time,flags",
        "-of", "csv=p=0", path, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    out, _ = await proc.communicate()
    res = []
    for line in out.decode(errors="ignore").splitlines():
        parts = line.strip().split(",")
        if len(parts) >= 2 and "K" in parts[1]:
            try:
                res.append(float(parts[0]))
            except ValueError:
                pass
    return sorted(res)


def pick_splits(kfs: List[float], duration: float, n: int, min_len: float = 3.0) -> List[float]:
    splits, last = [], 0.0
    for i in range(1, n):
        target = duration * i / n
        cands = [k for k in kfs if k >= last + min_len and k <= duration - min_len]
        if not cands:
            break
        k = min(cands, key=lambda x: abs(x - target))
        if splits and k <= splits[-1]:
            continue
        splits.append(k)
        last = k
    return splits


# ───────────────────────── orchestrator ─────────────────────────
class DistRun:
    def __init__(self, *, client, job, src, dst, meta, state: DistState, opts: dict, workdir: Path,
                 pool: "WorkerPool", n_workers: int, gh=None, board_cls=None):
        self.pool, self.n_workers = pool, n_workers
        self.client, self.job, self.src, self.dst = client, job, str(src), str(dst)
        self.meta, self.state, self.opts, self.work = meta, state, opts, Path(workdir)
        self.used: set = set()                 # parts release එකක් හදපු accounts
        self.job_cfg: dict = {}
        self.board_cls = board_cls or TgBoard
        self.tag = f"enc-{job.id}"
        self.board = None
        self.tasks: List[asyncio.Task] = []
        self.ctls: List[types.SimpleNamespace] = []
        self.stats = {"workers": 0, "local_parts": 0}

    async def _ff(self, *args):
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-hide_banner", "-nostdin", "-y", *args,
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
        self.job.proc = proc
        _, err = await proc.communicate()
        self.job.proc = None
        if self.job.cancelled:
            raise asyncio.CancelledError()
        if proc.returncode != 0:
            raise DistError("ffmpeg failed: " + err.decode(errors="ignore")[-400:])

    async def run(self) -> dict:
        return await self._run()

    def _g(self, ai: int):
        return self.pool.accts[ai].gh

    async def _run(self) -> dict:
        s, meta = self.state, self.meta
        self.pool.build()
        self._ensure = None
        try:
            # 1) SPLIT
            s.phase = "split"
            # pool එක ready කරන්න (download එකේදී කලින්ම පටන් අරන් නම් මේක no-op / අලුත් workers විතරක් dispatch)
            ensure = self._ensure = asyncio.create_task(self.pool.ensure(self.n_workers))
            splits = pick_splits(await keyframes(self.src), meta.duration, self.n_workers)
            if not splits:
                raise DistError("Split කරන්න keyframes මදි")
            bounds = [0.0] + splits + [meta.duration]
            await self._ff("-i", self.src, "-map", "0:v:0", "-c", "copy", "-an", "-sn", "-dn",
                           "-f", "segment", "-segment_times", ",".join(f"{t:.3f}" for t in splits),
                           "-reset_timestamps", "1", "-segment_format", "mp4",
                           str(self.work / "raw_part_%d.mp4"))
            files = sorted(self.work.glob("raw_part_*.mp4"), key=lambda p: int(re.findall(r"\d+", p.stem)[0]))
            if len(files) != len(bounds) - 1:
                raise DistError(f"Split parts mismatch ({len(files)} vs {len(bounds) - 1})")
            s.parts = [Part(i, bounds[i], bounds[i + 1] - bounds[i], acct=i // PER_ACCT) for i in range(len(files))]
            self.stats["workers"] = len(files)
            await ensure                               # pool ready (fail → DistError → local fallback)

            # 2) UPLOAD → Release
            s.phase = "upload"
            groups: dict = {}
            for p, f in zip(s.parts, files):
                groups.setdefault(p.acct, []).append(f)

            async def up(ai, fs):
                await self._g(ai).release_create(self.tag)
                self.used.add(ai)
                await self._g(ai).upload(self.tag, fs)

            await asyncio.gather(*[up(ai, fs) for ai, fs in groups.items()])   # account එකකට එහි කොටස් විතරයි

            # 3) progress board + audio + dispatch
            chat = int(config.DIST_PROGRESS_CHAT) if config.DIST_PROGRESS_CHAT else self.job.chat_id
            self.board = self.board_cls(self.client, chat, self.job.id, len(files))
            await self.board.create()
            audio_task = None
            if self.meta.acodec and self.opts["audio_mode"] != "none":
                audio_task = asyncio.create_task(self._extract_audio())
                self.tasks.append(audio_task)
            o = self.opts
            job_cfg = {
                "job": self.job.id, "n": len(files), "chat": chat, "ids": self.board.ids,
                "durations": [p.duration for p in s.parts], "fps": meta.fps,
                "target": o["target"], "scale": o["scale"], "portrait": o["portrait"],
                "preset": o["preset"], "codec": o["codec"], "crf": o["crf"],
                "maxrate": o["maxrate_kbps"], "interval": PROGRESS_UPDATE_INTERVAL,
            }
            self.job_cfg = job_cfg
            await self.pool.post_job(self.job.id, job_cfg)        # hold වෙලා ඉන්න workers තත්පර 3කින් අල්ලගන්නවා

            # 4) ENCODE (monitor)
            s.phase = "encode"
            s.enc_t0 = time.monotonic()
            for p in s.parts:
                p.last_change = time.monotonic()
            await self._monitor()

            # 5) MERGE
            s.phase = "merge"
            audio = await audio_task if audio_task else None
            await self._merge(audio)
            return self.stats
        finally:
            if self._ensure:
                with contextlib.suppress(BaseException):
                    await self._ensure
            for c in self.ctls:
                c.cancelled = True
                if c.proc:
                    with contextlib.suppress(Exception):
                        c.proc.kill()
            for t in self.tasks:
                t.cancel()
            await asyncio.gather(*self.tasks, return_exceptions=True)
            if self.board:
                await self.board.delete()
            await asyncio.gather(*[self._g(ai).release_delete(self.tag) for ai in self.used], return_exceptions=True)
            await self.pool.clear_job()

    # ── audio (main runner, workers encode කරන අතරේ) ──
    async def _extract_audio(self) -> str:
        out = str(self.work / "audio.mka")
        o = self.opts
        a = ["-c:a", "copy"] if o["audio_mode"] == "copy" else ["-c:a", "aac", "-b:a", f"{o['abitrate']}k"]
        await self._ff("-i", self.src, "-vn", "-sn", "-dn", "-map", "0:a?", *a, out)
        return out

    # ── monitor loop ──
    def _apply(self, p: Part, txt: str):
        if p.ready or p.local_started:
            return
        if txt != p.last_raw:
            p.last_raw, p.last_change = txt, time.monotonic()
        d = parse_prog(txt, self.job.id, p.idx)
        if not d:
            return
        st = d["state"]
        if st in ("running", "uploading"):
            p.status = st
            p.out_time, p.fps, p.speed, p.size, p.frame = d["out_time"], d["fps"], d["speed"], d["size"], d["frame"]
            if st == "uploading":
                p.out_time = p.duration
        elif st == "done":
            p.status = "remote_done"
        elif st == "failed":
            p.status = "failed"

    async def _fetch(self, p: Part):
        p.fetching = True
        try:
            await self._g(p.acct).download(self.tag, f"encoded_part_{p.idx}.mkv", self.work)
            path = self.work / f"encoded_part_{p.idx}.mkv"
            if not path.exists() or path.stat().st_size == 0:
                raise DistError("empty encoded part")
            p.path, p.size = str(path), path.stat().st_size
            p.ready, p.status, p.speed, p.fps = True, "ready", 0.0, 0.0
        except Exception as e:
            logger.warning(f"fetch part {p.idx} failed: {e}")
            p.fetching, p.status = False, "failed"

    async def _local(self, p: Part):
        p.local_started, p.status = True, "local"
        self.stats["local_parts"] += 1
        st = EncState(duration=p.duration, src_fps=self.meta.fps)
        p.local_state = st
        ctl = types.SimpleNamespace(cancelled=False, proc=None)
        self.ctls.append(ctl)
        dst = self.work / f"encoded_part_{p.idx}.mkv"
        o = self.opts
        await transcode(str(self.work / f"raw_part_{p.idx}.mp4"), str(dst), st, ctl,
                        target=o["target"], scale=o["scale"], portrait=o["portrait"], preset=o["preset"],
                        codec=o["codec"], crf=o["crf"], maxrate_kbps=o["maxrate_kbps"],
                        audio_mode="none", container="mkv")
        p.path, p.size, p.ready, p.status = str(dst), dst.stat().st_size, True, "ready"
        p.speed = p.fps = 0.0

    async def _monitor(self):
        s = self.state
        last_assets = time.monotonic()
        while not all(p.ready for p in s.parts):
            if self.job.cancelled:
                raise asyncio.CancelledError()
            now = time.monotonic()
            try:
                texts = await self.board.read()
                for p, t in zip(s.parts, texts):
                    self._apply(p, t)
            except Exception as e:
                logger.debug(f"board read failed: {e}")

            # secondary: Release assets (progress message ආවේ නැතිනම් වුණත් අල්ලගන්න)
            if now - last_assets > 30 and any(not p.ready and not p.local_started and not p.fetching for p in s.parts):
                last_assets = now
                with contextlib.suppress(Exception):
                    for ai in {p.acct for p in s.parts if not p.ready}:
                        assets = await self._g(ai).assets(self.tag)
                        for p in s.parts:
                            if (p.acct == ai and not p.ready and not p.local_started and not p.fetching
                                    and assets.get(f"encoded_part_{p.idx}.mkv") == "uploaded"):
                                p.status = "remote_done"

            for p in s.parts:
                if p.ready or p.fetching:
                    continue
                if p.local_started:
                    if p.local_state:
                        ls = p.local_state
                        p.out_time, p.fps, p.speed, p.frame, p.size = ls.out_time, ls.fps, ls.speed, ls.frame, ls.out_size
                    continue
                idle = now - p.last_change
                if p.status == "remote_done":
                    self.tasks.append(asyncio.create_task(self._fetch(p)))
                elif (p.status == "waiting" and not p.spilled
                      and now - s.enc_t0 > config.DIST_START_TIMEOUT and await self._spill(p)):
                    continue                           # busy account → වෙන account එකේ free runner එකකට යැව්වා
                elif (p.status == "failed"
                      or (p.status == "waiting" and now - max(s.enc_t0, p.spill_t) > config.DIST_START_TIMEOUT)
                      or (p.status in ("running", "uploading") and idle > config.DIST_STALE_SECONDS)):
                    logger.warning(f"part {p.idx}: {p.status} → local fallback")
                    self.tasks.append(asyncio.create_task(self._local(p)))
            # local task errors → ඉක්මනින් surface කරන්න
            for t in self.tasks:
                if t.done() and not t.cancelled() and t.exception():
                    raise t.exception()
            await asyncio.sleep(max(1.0, PROGRESS_UPDATE_INTERVAL))

    # ── runner busy / start නොවුණොත්: වෙන account එකක free slot එකකට (එකම part index එකෙන්) ──
    async def _spill(self, p: Part) -> bool:
        p.spilled = True
        a = self.pool.pick_spare({p.acct})
        if not a:
            return False
        try:
            async with self.pool._lock:
                await self.pool._start(a)
            g = a.gh
            if a.idx not in self.used:
                await g.release_create(self.tag)
                self.used.add(a.idx)
            await g.upload(self.tag, [self.work / f"raw_part_{p.idx}.mp4"])
            await g.mailbox_set(a.tag, self.pool._body(a, "job", job=self.job.id, cfg=self.job_cfg))
            await g.dispatch_workers(a.session, [p.idx])
            a.dispatched.add(p.idx)
            logger.warning(f"part {p.idx}: acct {p.acct} busy → spill to acct {a.idx}")
            p.acct, p.spill_t, p.last_change = a.idx, time.monotonic(), time.monotonic()
            return True
        except Exception as e:
            logger.warning(f"spill failed: {e}")
            return False

    # ── merge ──
    async def _merge(self, audio: Optional[str]):
        lst = self.work / "concat.txt"
        lst.write_text("".join(f"file '{Path(p.path).resolve()}'\n" for p in self.state.parts))
        args = ["-f", "concat", "-safe", "0", "-i", str(lst)]
        if audio:
            args += ["-i", audio]
        args += ["-map", "0:v:0"]
        if audio:
            args += ["-map", "1:a"]
        args += ["-c", "copy"]
        if self.opts["codec"] == "libx265":
            args += ["-tag:v", "hvc1"]
        args += ["-movflags", "+faststart", self.dst]
        await self._ff(*args)


def helpers_status() -> str:
    lines = [f"🤝 <b>Helper accounts: {len(HELPERS)}/{len(config.HELPER_TOKENS)}</b> • workers capacity <b>{capacity()}</b>"]
    lines += [f"• ✅ {r}" for _, r in HELPERS]
    lines += [f"• ❌ {p}" for p in HELPER_PROBLEMS]
    if not config.HELPER_TOKENS:
        lines.append("HELPER_TOKENS set කරලා නෑ.")
    return "\n".join(lines)
