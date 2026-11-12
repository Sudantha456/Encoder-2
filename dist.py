"""
dist.py — Distributed encoding: main runner ↔ GitHub Actions workers (queue-aware pool)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• Queue එකේ පළමු job එක download වෙන්න පටන් ගන්නකොටම WorkerPool එක settings වල workers ගාණට
  runners dispatch කරනවා → download එක ඉවර වෙද්දී runners ready.
• Workers queue එක ඉවර වෙනකම් hold වෙනවා (job එකෙන් job එකට). Queue හිස් වුණාම pool එක stop → workers exit.
• Download අතරතුර settings වල workers ගාන වැඩි කළොත් අලුත් runners ඉක්මනින් dispatch; අඩු කළොත්
  ඉතුරු workers hold (ඊළඟ job වලට ආයෙ වැඩි කළොත් ready).
Job එකකට:
  1. keyframe boundaries වල ffmpeg -c copy වලින් parts කඩනවා
  2. parts temp Releases වලට upload කරලා worker mailbox එකට assign කරනවා
  3. runner එකක් start නොවුණොත් part එක held; completed worker එකක් idle lease දුන්නාම ඒකට reassign කරනවා
  4. workers encode කරලා Release එකට upload කරනවා; main runner එක fetch + concat + audio mux විතරයි
  5. queue හිස් වුණාම active සහ queued encode-worker workflows සියල්ල cancel කරනවා
"""
import asyncio
import base64
import contextlib
import hmac
import json
import logging
import os
import re
import shutil
import signal
import time
import types
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional

import config
from config import PROGRESS_UPDATE_INTERVAL
from ffenv import ffmpeg_env
from pathlib import Path as _P

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
    # Every module the worker imports must be listed here: the helper repo is
    # populated only from this mapping, and worker.py/transcoder.py import
    # these files directly.
    files = {"worker.py": root / "worker.py", "transcoder.py": root / "transcoder.py",
             "ffenv.py": root / "ffenv.py",
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


def estimate_encode_seconds(meta, target, preset, codec, pix_fmt: str = "yuv420p") -> float:
    from media import target_dims
    if target and target > 0:
        tw, th = target_dims(meta, target)
    else:
        tw = max(meta.width - (meta.width % 2), 2) if meta.width else 1280
        th = max(meta.height - (meta.height % 2), 2) if meta.height else 720
    pf_factor = 0.8 if any(x in (pix_fmt or "") for x in ("10", "12", "16", "422", "444")) else 1.0
    sp = (
        ENC_SPEED_720P.get(preset, 1.0)
        * (921600 / max(tw * th, 1))
        * (0.4 if codec == "libx265" else 1.0)
        * pf_factor
    )
    return meta.duration / max(sp, 0.05)


def should_distribute(meta, target, preset, codec, mode: str, pix_fmt: str = "yuv420p") -> bool:
    if mode == "off" or not available() or meta.duration < 30:
        return False
    if mode == "on":
        return True
    return estimate_encode_seconds(meta, target, preset, codec, pix_fmt=pix_fmt) >= config.DIST_AUTO_MIN_SECONDS


_BG: set = set()      # background cleanup tasks (GC වෙන්නේ නැති වෙන්න)


# ───────────────────────── state (UI render කරන්නේ මේකෙන්) ─────────────────────────
@dataclass
class Part:
    idx: int
    start: float
    duration: float
    status: str = "waiting"      # waiting|held|running|uploading|remote_done|failed|ready
    out_time: float = 0.0
    fps: float = 0.0
    speed: float = 0.0
    size: int = 0
    frame: int = 0
    ready: bool = False
    fetching: bool = False
    path: str = ""
    last_raw: str = ""
    last_change: float = field(default_factory=time.monotonic)
    acct: int = 0                 # ඒ කොටස encode වෙන GitHub account index (0 = main)
    runner_idx: int = -1          # මේ part එකට දැනට assign කර ඇති worker index (global within account)
    attempted_runners: set = field(default_factory=set)  # (account index, runner index) retries avoid
    attempt: int = 0              # unique lease generation for worker-side de-duplication
    orphan: bool = False          # owner account එක fail → raw part එක තවම upload වෙලා නෑ
    last_try: float = 0.0
    via_http: bool = False        # HTTP ProgressHub හරහා live updates ලැබෙනවාද යන්න

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
        # merge phase progress (UI)
        self.merge_step = ""            # audio | mux
        self.merge_t0 = 0.0
        self.merge_out = 0.0            # seconds of output written
        self.merge_size = 0
        self.merge_speed = 0.0

    @property
    def merge_pct(self):
        return min(self.merge_out / self.duration * 100, 99.9) if self.duration else 0.0

    @property
    def merge_elapsed(self):
        return time.monotonic() - self.merge_t0 if self.merge_t0 else 0.0

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
        return sum(p.speed for p in self.parts if p.status == "running" and not p.ready)

    @property
    def fps(self):
        return sum(p.fps for p in self.parts if p.status == "running" and not p.ready)

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
ACTIVE_STATUSES = ("in_progress", "queued", "waiting", "requested", "pending")


class GhBackend:
    def __init__(self, token: Optional[str] = None, repo: Optional[str] = None):
        self.repo = repo or os.environ["GITHUB_REPOSITORY"]
        self.env = {**os.environ, "GH_REPO": self.repo}
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

    @staticmethod
    def _upload_retryable(exc: Exception) -> bool:
        """Return True only for transient GitHub/network upload failures."""
        msg = str(exc).lower()
        markers = (
            "http 408", "http 409", "http 425", "http 429",
            "http 500", "http 502", "http 503", "http 504",
            "timed out", "timeout", "temporarily unavailable",
            "connection reset", "connection aborted", "connection closed",
            "remote end closed", "unexpected eof", "network is unreachable",
            "temporary failure", "tls handshake", "tcpfull",
        )
        return any(marker in msg for marker in markers)

    async def _verify_asset(self, tag: str, name: str, expected_size: int) -> bool:
        """Check that GitHub has the completed asset with the expected byte size."""
        try:
            raw = await self._gh("release", "view", tag, "--json", "assets")
            data = json.loads(raw or "{}")
            for asset in data.get("assets", []):
                if asset.get("name") == name:
                    return (asset.get("state", "uploaded") == "uploaded"
                            and int(asset.get("size", -1)) == expected_size)
            return False
        except Exception as exc:
            logger.warning("Release asset verification failed for %s: %s", name, exc)
            return False

    async def _upload_one(self, tag: str, path: Path, *, attempts: int = 5):
        """Upload one asset at a time, retry transient failures and verify completion.

        A timeout can happen after GitHub has actually received the complete asset,
        so verification is performed before every retry to avoid needless re-upload.
        """
        path = Path(path)
        if not path.is_file() or path.stat().st_size <= 0:
            raise DistError(f"Upload source is missing or empty: {path}")

        name = path.name
        last_error = None
        for attempt in range(1, attempts + 1):
            # A previous request may have completed even if its client saw a timeout.
            if await self._verify_asset(tag, name, path.stat().st_size):
                logger.info("GitHub asset already verified: %s", name)
                return

            try:
                await self._gh("release", "upload", tag, str(path), "--clobber")
            except Exception as exc:
                last_error = exc
                if not self._upload_retryable(exc) or attempt >= attempts:
                    raise DistError(
                        f"GitHub upload failed for {name} (attempt {attempt}/{attempts}): {exc}"
                    ) from exc
                delay = min(5 * (2 ** (attempt - 1)), 60)
                logger.warning(
                    "Transient GitHub upload error for %s (attempt %s/%s); retrying in %ss: %s",
                    name, attempt, attempts, delay, exc,
                )
                await asyncio.sleep(delay)

            # Poll briefly because Release asset metadata can lag behind the upload.
            for check_no in range(4):
                if await self._verify_asset(tag, name, path.stat().st_size):
                    logger.info("GitHub asset upload verified: %s (%s bytes)", name, path.stat().st_size)
                    return
                if check_no < 3:
                    await asyncio.sleep(2 + check_no * 2)

            if attempt < attempts:
                delay = min(5 * (2 ** (attempt - 1)), 60)
                logger.warning(
                    "GitHub did not confirm asset %s after upload (attempt %s/%s); retrying in %ss",
                    name, attempt, attempts, delay,
                )
                await asyncio.sleep(delay)

        raise DistError(
            f"GitHub upload could not be verified for {name} after {attempts} attempts"
            + (f": {last_error}" if last_error else "")
        )

    async def upload(self, tag, paths):
        """Upload each release asset independently to isolate failures and enable retries."""
        for path in paths:
            await self._upload_one(tag, Path(path))

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

    async def release_delete_asset(self, tag, name):
        # An idle marker is a one-use lease. Removing it prevents a stale marker
        # from being scheduled to a second part before that runner reports idle again.
        await self._gh("release", "delete-asset", tag, name, "-y", check=False)

    # ── workflow runs (encode-worker.yml විතරයි — run-bot.yml එකට කවදාවත් අත ගහන්නේ නෑ) ──
    async def runs(self, statuses=ACTIVE_STATUSES) -> List[dict]:
        async def one(st):
            # Read enough history to cover multiple queued matrix batches, not
            # just the first 100 active runs returned by GitHub CLI.
            raw = await self._gh("run", "list", "--workflow", config.DIST_WORKFLOW, "--status", st,
                                 "--limit", "1000", "--json",
                                 "databaseId,status,createdAt,startedAt,displayTitle,event", check=False)
            try:
                return json.loads(raw or "[]")
            except Exception:
                return []
        seen: dict = {}
        for lst in await asyncio.gather(*[one(st) for st in statuses]):
            for r in lst:
                seen[r["databaseId"]] = r
        return sorted(seen.values(), key=lambda r: r.get("createdAt", ""))

    async def run_jobs(self, run_id) -> List[dict]:
        raw = await self._gh("run", "view", str(run_id), "--json", "jobs", check=False)
        try:
            return json.loads(raw or "{}").get("jobs", [])
        except Exception:
            return []

    async def run_cancel(self, run_id, force=False):
        if force:       # normal cancel එකට නොනවතින (hang වුණ) runs වලට
            await self._gh("api", "-X", "POST", f"repos/{self.repo}/actions/runs/{run_id}/force-cancel", check=False)
        else:
            await self._gh("run", "cancel", str(run_id), check=False)

    async def release_tags(self) -> List[str]:
        raw = await self._gh("release", "list", "--limit", "100", "--json", "tagName", check=False)
        try:
            return [r["tagName"] for r in json.loads(raw or "[]")]
        except Exception:
            return []


# ───────────────────────── Direct HTTP Progress Hub ─────────────────────────
class ProgressHub:
    """
    Lightweight async HTTP receiver + automatic Quick-Tunnel (cloudflared / ssh localhost.run).
    Workers POST JSON progress directly to main bot memory → 0 Telegram messages, 0 EditMessageRequest
    FloodWaits even with 50 concurrent workers!
    """

    def __init__(self):
        self.token: str = os.urandom(16).hex()
        self.port: int = 0
        self.public_url: str = ""
        self._server: Optional[asyncio.AbstractServer] = None
        self._tunnel_proc: Optional[asyncio.subprocess.Process] = None
        self._drain_task: Optional[asyncio.Task] = None
        self._jobs: Dict[str, Callable[[dict], None]] = {}
        self._lock = asyncio.Lock()

    def register(self, job_id: str, cb: Callable[[dict], None]) -> None:
        self._jobs[str(job_id)] = cb

    def unregister(self, job_id: str) -> None:
        self._jobs.pop(str(job_id), None)

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        status_line = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok"
        try:
            head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=5.0)
            lines = head.decode("latin-1", errors="ignore").split("\r\n")
            req_line = lines[0] if lines else ""
            headers: Dict[str, str] = {}
            for ln in lines[1:]:
                if ":" in ln:
                    k, v = ln.split(":", 1)
                    headers[k.strip().lower()] = v.strip()

            if req_line.startswith("POST "):
                got_tok = headers.get("x-auth-token", "")
                if not hmac.compare_digest(got_tok, self.token):
                    status_line = b"HTTP/1.1 403 Forbidden\r\nContent-Length: 9\r\nConnection: close\r\n\r\nforbidden"
                else:
                    clen = min(max(int(headers.get("content-length", "0") or 0), 0), 32768)
                    body = await asyncio.wait_for(reader.readexactly(clen), timeout=5.0) if clen else b"{}"
                    data = json.loads(body.decode("utf-8", errors="ignore") or "{}")
                    if isinstance(data, dict):
                        cb = self._jobs.get(str(data.get("job", "")))
                        if cb:
                            cb(data)
        except Exception:
            status_line = b"HTTP/1.1 400 Bad Request\r\nContent-Length: 3\r\nConnection: close\r\n\r\nbad"
        finally:
            with contextlib.suppress(Exception):
                writer.write(status_line)
                await writer.drain()
                writer.close()
                await writer.wait_closed()

    async def _start_server_unlocked(self) -> None:
        if self._server is not None:
            return
        manual_url = getattr(config, "DIST_REPORT_URL", "").strip()
        bind_host = "0.0.0.0" if manual_url else "127.0.0.1"
        bind_port = int(os.getenv("DIST_REPORT_PORT", "0") or 0)
        self._server = await asyncio.start_server(self._handle_client, bind_host, bind_port)
        socks = self._server.sockets or []
        if socks:
            self.port = int(socks[0].getsockname()[1])

    async def _spawn_tunnel(self, cmd: List[str], url_regex: str, timeout: float) -> str:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            stdin=asyncio.subprocess.DEVNULL,
        )
        pat = re.compile(url_regex)
        found_url = ""
        deadline = time.monotonic() + timeout
        try:
            while time.monotonic() < deadline:
                rem = max(0.2, deadline - time.monotonic())
                line = await asyncio.wait_for(proc.stdout.readline(), timeout=rem)
                if not line:
                    break
                txt = line.decode("utf-8", errors="ignore")
                m = pat.search(txt)
                if m:
                    found_url = m.group(0).rstrip("/")
                    break
        except asyncio.TimeoutError:
            pass

        if not found_url:
            with contextlib.suppress(Exception):
                proc.kill()
                await proc.wait()
            return ""

        self._tunnel_proc = proc

        async def _drain():
            with contextlib.suppress(Exception):
                while True:
                    ln = await proc.stdout.readline()
                    if not ln:
                        break

        self._drain_task = asyncio.create_task(_drain())
        return found_url

    async def ensure(self) -> str:
        async with self._lock:
            await self._start_server_unlocked()
            manual_url = getattr(config, "DIST_REPORT_URL", "").strip()
            if manual_url:
                self.public_url = manual_url if manual_url.endswith("/prog") else manual_url.rstrip("/") + "/prog"
                return self.public_url

            if self._tunnel_proc is not None and self._tunnel_proc.returncode is None and self.public_url:
                return self.public_url

            await self._stop_tunnel_unlocked()

            if shutil.which("cloudflared"):
                base = await self._spawn_tunnel(
                    ["cloudflared", "tunnel", "--no-autoupdate", "--url", f"http://127.0.0.1:{self.port}"],
                    r"https://[a-zA-Z0-9-]+\.trycloudflare\.com",
                    timeout=15.0,
                )
                if base:
                    self.public_url = f"{base}/prog"
                    logger.info(f"🌐 ProgressHub tunnel active (cloudflared): {self.public_url}")
                    return self.public_url

            if shutil.which("ssh"):
                base = await self._spawn_tunnel(
                    [
                        "ssh", "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
                        "-o", "BatchMode=yes", "-o", "ExitOnForwardFailure=yes",
                        "-o", "ServerAliveInterval=30",
                        "-R", f"80:127.0.0.1:{self.port}", "nokey@localhost.run",
                    ],
                    r"https://[a-zA-Z0-9-]+\.lhr\.life",
                    timeout=12.0,
                )
                if base:
                    self.public_url = f"{base}/prog"
                    logger.info(f"🌐 ProgressHub tunnel active (localhost.run): {self.public_url}")
                    return self.public_url

            logger.warning("🌐 ProgressHub tunnel unavailable — using GitHub Release asset liveness fallback")
            self.public_url = ""
            return ""

    async def _stop_tunnel_unlocked(self) -> None:
        if self._drain_task:
            self._drain_task.cancel()
            with contextlib.suppress(BaseException):
                await self._drain_task
            self._drain_task = None
        if self._tunnel_proc is not None:
            if self._tunnel_proc.returncode is None:
                with contextlib.suppress(Exception):
                    self._tunnel_proc.terminate()
                try:
                    await asyncio.wait_for(self._tunnel_proc.wait(), timeout=2.0)
                except Exception:
                    with contextlib.suppress(Exception):
                        self._tunnel_proc.kill()
            self._tunnel_proc = None
        self.public_url = ""

    async def stop(self) -> None:
        async with self._lock:
            await self._stop_tunnel_unlocked()
            if self._server is not None:
                self._server.close()
                with contextlib.suppress(Exception):
                    await self._server.wait_closed()
                self._server = None
            self.port = 0
            self._jobs.clear()


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
        self.standby: set = set()          # 🔥 hot standby worker indices (>= STANDBY_BASE)
        self.sb_busy: dict = {}            # standby idx -> Part
        self.assign: dict = {}             # worker idx -> {part, attempt} (mailbox 'assign')
        self.skip: set = set()             # වෙන account එකකට මාරු කළ parts (මෙහි workers ට start නොකර skip)
        self.claimed_idle: set = set()     # one-use idle marker asset names already consumed


class WorkerPool:
    """Queue session එකක් පුරා ජීවත් වෙන workers (main + helper accounts, එකකට PER_ACCT බැගින්)."""

    def __init__(self, gh=None):
        self.accts: List[Acct] = []
        self._lock = asyncio.Lock()
        self.halted = False            # /End_Work පස්සේ අලුත් workers dispatch වෙන එක නවත්තනවා
        self.hub = ProgressHub()

    def build(self):
        want = 1 + len(HELPERS)
        if len(self.accts) != want:
            self.accts = [Acct(0, GhBackend())] + [Acct(i + 1, GhBackend(t, r)) for i, (t, r) in enumerate(HELPERS)]

    @property
    def active(self) -> bool:
        return any(a.active and not a.failed for a in self.accts)

    def _body(self, a: Acct, state="idle", **extra) -> str:
        if state == "job":
            extra.setdefault("assign", {str(k): v for k, v in a.assign.items()})
            extra.setdefault("skip", sorted(a.skip))
        return json.dumps({"session": a.session, "state": state, "ts": time.time(), **extra}, separators=(",", ":"))

    async def _start(self, a: Acct):
        if not a.active:
            a.session = os.urandom(4).hex()
            a.tag = f"enc-q-{a.session}"
            a.dispatched, a.standby, a.sb_busy, a.assign, a.skip = set(), set(), {}, {}, set()
            a.claimed_idle = set()
            await a.gh.mailbox_create(a.tag, self._body(a))
            a.active, a.failed = True, False
            a.hb = asyncio.create_task(self._heartbeat(a))
            logger.info(f"🌐 [acct {a.idx}] session {a.session} started")

    async def ensure(self, n: int):
        """Dispatch available accounts; failed-account parts stay held for reassignment."""
        if self.halted:
            raise DistError("Worker pool stopped (/End_Work)")
        async with self._lock:
            self.build()
            n = min(n, PER_ACCT * len(self.accts))
            errors = []

            async def one(a: Acct):
                need = {i for i in range(n) if i % len(self.accts) == a.idx}     # round-robin: හැම account එකටම සමානව
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
                    a.active = False
                    if a.hb:
                        a.hb.cancel()
                        with contextlib.suppress(BaseException):
                            await a.hb
                        a.hb = None
                    errors.append(e)
                    logger.warning(f"🌐 [acct {a.idx}] dispatch failed: {e}")

            hub_task = asyncio.create_task(self.hub.ensure())
            await asyncio.gather(*[one(a) for a in self.accts])
            with contextlib.suppress(Exception):
                await hub_task
            # A single account failure must not collapse the distributed job.
            # Its parts are held and will be assigned to an idle runner elsewhere.
            if not any(a.active and not a.failed for a in self.accts):
                detail = str(errors[0]) if errors else "no worker account available"
                raise DistError(f"all worker dispatches failed: {detail}")
            await self._plan_standby(n)

    async def _plan_standby(self, n: int):
        """Optional warm spare runners; they join the same idle-lease scheduler."""
        want = config.DIST_STANDBY
        k = len(self.accts)
        if want <= 0 or k == 0:
            return
        cands = []
        for a in self.accts:
            if a.failed:
                continue
            normal = sum(1 for i in range(n) if i % k == a.idx)
            spare = PER_ACCT - max(normal, len(a.dispatched)) - len(a.standby)
            if spare > 0:
                cands.append([a, spare, []])
        todo = want - sum(len(a.standby) for a in self.accts)
        cands.sort(key=lambda c: -c[1])
        while todo > 0 and any(c[1] > 0 for c in cands):
            for c in cands:
                if todo <= 0:
                    break
                if c[1] > 0:
                    c[2].append(max(c[0].standby, default=config.STANDBY_BASE - 1) + 1 + len(c[2]))
                    c[1] -= 1
                    todo -= 1

        async def disp(a: Acct, idxs: list):
            try:
                await self._start(a)
                await a.gh.dispatch_workers(a.session, idxs)
                a.standby |= set(idxs)
                logger.info(f"🔥 [acct {a.idx}] standby workers {idxs}")
            except Exception as e:
                logger.warning(f"🔥 [acct {a.idx}] standby dispatch failed: {e}")

        await asyncio.gather(*[disp(a, ix) for a, _, ix in cands if ix])

    async def _heartbeat(self, a: Acct):
        while True:
            await asyncio.sleep(60)
            await a.gh.heartbeat(a.tag)

    async def post_job(self, job_id: str, cfg: dict):
        await asyncio.gather(*[a.gh.mailbox_set(a.tag, self._body(a, "job", job=job_id, cfg=cfg))
                               for a in self.accts if a.active and not a.failed])

    async def clear_job(self):
        for a in self.accts:
            a.assign, a.skip, a.sb_busy = {}, set(), {}
        await asyncio.gather(*[a.gh.mailbox_set(a.tag, self._body(a)) for a in self.accts if a.active])

    def pick_spare(self, exclude: set) -> Optional[Acct]:
        """Busy/ start නොවුණ part එකකට: තවම free slots තියෙන වෙන account එකක්."""
        c = [a for a in self.accts if a.idx not in exclude and not a.failed and len(a.dispatched) + len(a.standby) < PER_ACCT]
        return min(c, key=lambda a: len(a.dispatched)) if c else None

    async def _cancel_workflow_runs(self, rounds: int = 6, on_progress=None) -> dict:
        """Cancel every encode-worker workflow run, including queued matrix runs."""
        cancelled = {a.idx: set() for a in self.accts}
        remaining = {a.idx: [] for a in self.accts}
        used = 0
        for rnd in range(1, rounds + 1):
            lists = await asyncio.gather(*[a.gh.runs() for a in self.accts], return_exceptions=True)
            remaining = {a.idx: (lst if isinstance(lst, list) else [])
                         for a, lst in zip(self.accts, lists)}
            if not any(remaining.values()):
                break
            used = rnd
            force = rnd >= 3
            if on_progress:
                with contextlib.suppress(Exception):
                    await on_progress(rnd, rounds, force, {i: len(v) for i, v in remaining.items()})
            await asyncio.gather(*[
                a.gh.run_cancel(run["databaseId"], force)
                for a in self.accts for run in remaining[a.idx]
            ], return_exceptions=True)
            for a in self.accts:
                cancelled[a.idx] |= {r["databaseId"] for r in remaining[a.idx]}
            await asyncio.sleep(3)

        lists = await asyncio.gather(*[a.gh.runs() for a in self.accts], return_exceptions=True)
        left = {a.idx: (lst if isinstance(lst, list) else []) for a, lst in zip(self.accts, lists)}
        return {"rounds": used, "cancelled": {i: len(v) for i, v in cancelled.items()},
                "left": left, "accts": [(a.idx, a.gh.repo) for a in self.accts]}

    async def stop(self):
        """Queue empty: close worker mailboxes and cancel every worker workflow run."""
        async with self._lock:
            # Build even when this queue contained only short/local jobs; that
            # lets cleanup discover stale runs from older sessions. Avoid
            # requiring GitHub environment variables for local-only installs.
            if not self.accts and available():
                self.build()
            act = [a for a in self.accts if a.active]
            for a in act:
                a.active = False
                if a.hb:
                    a.hb.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await a.hb
                    a.hb = None
            if act:
                await asyncio.gather(*[
                    a.gh.mailbox_set(a.tag, self._body(a, "end")) for a in act
                ], return_exceptions=True)
                # Give active runners a brief opportunity to exit cleanly. Queued
                # matrix jobs are explicitly cancelled by _cancel_workflow_runs.
                await asyncio.sleep(4)

            res = await self._cancel_workflow_runs(rounds=6)

            async def purge(a: Acct):
                tags = [tag for tag in await a.gh.release_tags() if tag.startswith("enc-")]
                await asyncio.gather(*[a.gh.release_delete(tag) for tag in tags], return_exceptions=True)

            await asyncio.gather(*[purge(a) for a in self.accts], return_exceptions=True)
            for a in self.accts:
                a.dispatched, a.standby, a.sb_busy, a.assign, a.skip = set(), set(), {}, {}, set()
                a.claimed_idle = set()
                a.session = a.tag = ""
            await self.hub.stop()
            self.halted = False
            left = sum(len(v) for v in res["left"].values())
            if left:
                logger.warning(f"🌐 pool stop: {left} worker workflow runs remain after cancellation")
            else:
                logger.info(f"🌐 pool stopped; all worker workflows cancelled ({len(act)} active accounts)")

    # ───────────── /End_Work : සේරම worker workflows kill ─────────────
    async def kill_all(self, rounds: int = 6, on_progress=None) -> dict:
        """Stop the worker pool and cancel active + queued encode-worker runs only."""
        self.build()
        self.halted = True
        await self.hub.stop()
        for a in self.accts:
            a.active = False
            if a.hb:
                a.hb.cancel()
                with contextlib.suppress(BaseException):
                    await a.hb
                a.hb = None
            if a.tag:
                await a.gh.mailbox_set(a.tag, self._body(a, "end"))

        async def purge(a: Acct):
            tags = [tag for tag in await a.gh.release_tags() if tag.startswith("enc-")]
            await asyncio.gather(*[a.gh.release_delete(tag) for tag in tags], return_exceptions=True)

        await asyncio.gather(*[purge(a) for a in self.accts], return_exceptions=True)
        result = await self._cancel_workflow_runs(rounds=rounds, on_progress=on_progress)
        for a in self.accts:
            a.dispatched, a.session, a.tag = set(), "", ""
            a.standby, a.sb_busy, a.assign, a.skip = set(), {}, {}, set()
            a.claimed_idle = set()
        return result

    # ───────────── /workflow : worker workflows විස්තර ─────────────
    async def overview(self) -> List[dict]:
        self.build()

        async def one(a: Acct):
            info = {"idx": a.idx, "repo": a.gh.repo, "session": a.session, "active": a.active,
                    "failed": a.failed, "dispatched": sorted(a.dispatched), "runs": [], "error": ""}
            try:
                runs = await a.gh.runs()
                jobs = await asyncio.gather(*[a.gh.run_jobs(r["databaseId"]) for r in runs[:15]])
                for r, js in zip(runs, jobs):
                    r["jobs"] = js
                info["runs"] = runs
            except Exception as e:
                info["error"] = str(e)[:120]
            return info

        return await asyncio.gather(*[one(a) for a in self.accts])


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

    async def reset_part(self, idx: int):
        if idx < len(self.ids):
            with contextlib.suppress(Exception):
                await self.client.edit_message(
                    self.chat_id, self.ids[idx],
                    f"🧩 Part {idx + 1} • held / waiting for runner\n"
                    f"PROG|{self.job_id}|{idx}|waiting|0|0|0|0|0",
                    parse_mode=None,
                )


# ───────────────────────── ffmpeg helpers ─────────────────────────
async def keyframes(path: str) -> List[float]:
    proc = await asyncio.create_subprocess_exec(
        "ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "packet=pts_time,flags",
        "-of", "csv=p=0", path, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        env=ffmpeg_env())
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
        self.board_cls = board_cls             # None in production (0 Telegram messages; ProgressHub HTTP instead)
        self.tag = f"enc-{job.id}"
        self.board = None
        self.tasks: List[asyncio.Task] = []
        self.ctls: List[types.SimpleNamespace] = []
        self.stats = {"workers": 0, "local_parts": 0, "reassigned_parts": 0}
        self._rlock = asyncio.Lock()

    async def _ff(self, *args, on_progress=None, track=True, stage="processing"):
        if self.job.cancelled:
            raise asyncio.CancelledError()
        cmd = ["ffmpeg", "-hide_banner", "-nostdin", "-y"]
        if on_progress:
            cmd += ["-progress", "pipe:1", "-nostats"]
        proc = await asyncio.create_subprocess_exec(
            *cmd, *args,
            stdout=asyncio.subprocess.PIPE if on_progress else asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE, env=ffmpeg_env())
        if track:                       # split/merge → Cancel button එකෙන් kill වෙන්න; audio (සමාන්තරව) වෙනම handle කරනවා
            self.job.proc = proc
        async def stderr_tail():
            # Timestamp warnings can be numerous on long files. Keep a bounded
            # diagnostic tail while continuously draining the pipe.
            tail = bytearray()
            while True:
                chunk = await proc.stderr.read(8192)
                if not chunk:
                    return bytes(tail)
                tail.extend(chunk)
                if len(tail) > 16000:
                    del tail[:-16000]

        err_task = asyncio.create_task(stderr_tail())

        async def pump():
            t = size = sp = 0.0
            async for raw in proc.stdout:
                k, _, v = raw.decode(errors="ignore").strip().partition("=")
                try:
                    if k in ("out_time_us", "out_time_ms"):      # දෙකම microseconds
                        t = int(v) / 1e6
                    elif k == "total_size":
                        size = int(v)
                    elif k == "speed":
                        sp = float(v.strip().rstrip("x") or 0)
                    elif k == "progress":
                        on_progress(max(t, 0.0), int(size), sp)
                except ValueError:
                    pass

        try:
            if on_progress:
                await pump()
            await proc.wait()
            err = await err_task
        except BaseException:
            if proc.returncode is None:
                with contextlib.suppress(ProcessLookupError):
                    proc.kill()
            await proc.wait()
            await err_task
            raise
        finally:
            if track:
                self.job.proc = None
        if self.job.cancelled:
            raise asyncio.CancelledError()
        if proc.returncode != 0:
            code = proc.returncode
            status = f"exit code {code}"
            if code < 0:
                try:
                    status += f" ({signal.Signals(-code).name})"
                except ValueError:
                    pass
            detail = err.decode(errors="replace").strip()
            try:
                free = shutil.disk_usage(self.work).free
                disk = f", free disk {free // (1024 * 1024)} MiB"
            except OSError:
                disk = ""
            logger.error("FFmpeg %s failed (%s%s). stderr:\n%s",
                         stage, status, disk, detail or "<empty>")
            raise DistError(f"ffmpeg {stage} failed ({status}{disk}): " +
                            (detail[-600:] if detail else "no stderr output"))

    async def run(self) -> dict:
        return await self._run()

    def _g(self, ai: int):
        return self.pool.accts[ai].gh

    async def _run(self) -> dict:
        s, meta = self.state, self.meta
        self.pool.build()
        self._ensure = None
        audio_task = None
        try:
            # 0) AUDIO — encoding runners run කරන අතරේ background එකේ extract වෙනවා
            if self.meta.acodec and self.opts["audio_mode"] != "none":
                audio_task = asyncio.create_task(self._extract_audio())
                self.tasks.append(audio_task)

            # 1) SPLIT
            s.phase = "split"
            # pool එක ready කරන්න (download එකේදී කලින්ම පටන් අරන් නම් මේක no-op / අලුත් workers විතරක් dispatch)
            ensure = self._ensure = asyncio.create_task(self.pool.ensure(self.n_workers))
            splits = pick_splits(await keyframes(self.src), meta.duration, self.n_workers)
            if splits:
                bounds = [0.0] + splits + [meta.duration]
                await self._ff("-i", self.src, "-map", "0:v:0", "-c", "copy", "-an", "-sn", "-dn",
                               "-f", "segment", "-segment_times", ",".join(f"{t:.3f}" for t in splits),
                               "-reset_timestamps", "1", "-segment_format", "mp4",
                               str(self.work / "raw_part_%d.mp4"))
            else:
                # A video without enough keyframes is still sent to one GitHub
                # worker instead of silently falling back to the main runner.
                bounds = [0.0, meta.duration]
                await self._ff("-i", self.src, "-map", "0:v:0", "-c", "copy", "-an", "-sn", "-dn",
                               str(self.work / "raw_part_0.mp4"))
            files = sorted(self.work.glob("raw_part_*.mp4"), key=lambda p: int(re.findall(r"\d+", p.stem)[0]))
            if len(files) != len(bounds) - 1:
                raise DistError(f"Split parts mismatch ({len(files)} vs {len(bounds) - 1})")
            k = len(self.pool.accts)
            s.parts = [Part(i, bounds[i], bounds[i + 1] - bounds[i], acct=i % k, runner_idx=i)
                       for i in range(len(files))]
            self.stats["workers"] = len(files)
            await ensure                               # partial account failures are tolerated; all-fail raises
            for p in s.parts:                           # failed account parts remain held for idle-runner scheduling
                if self.pool.accts[p.acct].failed:
                    p.orphan = True

            # 2) UPLOAD → Release
            s.phase = "upload"
            groups: dict = {}
            for p, f in zip(s.parts, files):
                if not p.orphan:
                    groups.setdefault(p.acct, []).append(f)

            async def up(ai, fs):
                await self._ensure_release(ai)
                # Account-level isolation: each account uploads only its own parts;
                # GhBackend retries and verifies every asset independently.
                try:
                    await self._g(ai).upload(self.tag, fs)
                except Exception as exc:
                    raise DistError(f"raw-part upload failed for account #{ai}: {exc}") from exc

            # Different GitHub accounts can upload concurrently, while each account
            # uploads its own assets sequentially to avoid saturating one connection.
            upload_results = await asyncio.gather(
                *[up(ai, fs) for ai, fs in groups.items()], return_exceptions=True
            )
            upload_errors = [str(r) for r in upload_results if isinstance(r, BaseException)]
            if upload_errors:
                raise DistError("One or more account raw-part uploads failed: " + " | ".join(upload_errors))

            # 3) ProgressHub HTTP registration + dispatch (0 Telegram messages created!)
            chat = int(config.DIST_PROGRESS_CHAT) if config.DIST_PROGRESS_CHAT else self.job.chat_id
            if self.board_cls is not None:
                self.board = self.board_cls(self.client, chat, self.job.id, len(files))
                await self.board.create()

            report_url = ""
            report_tok = ""
            if hasattr(self.pool, "hub") and self.pool.hub is not None:
                self.pool.hub.register(self.job.id, self._on_http_report)
                report_url = await self.pool.hub.ensure()
                report_tok = self.pool.hub.token

            o = self.opts
            job_cfg = {
                "job": self.job.id, "n": len(files), "chat": chat,
                "ids": self.board.ids if self.board else [],
                "report_url": report_url, "report_token": report_tok,
                "durations": [p.duration for p in s.parts], "fps": meta.fps,
                "target": o["target"], "scale": o["scale"], "portrait": o["portrait"],
                "preset": o["preset"], "codec": o["codec"], "crf": o["crf"],
                "maxrate": o["maxrate_kbps"], "interval": PROGRESS_UPDATE_INTERVAL,
                "pix_fmt": o.get("pix_fmt", "yuv420p"),
                "x265_extra": o.get("x265_extra", ""),
            }
            self.job_cfg = job_cfg
            await self.pool.post_job(self.job.id, job_cfg)        # hold වෙලා ඉන්න workers තත්පර 3කින් අල්ලගන්නවා

            # 4) ENCODE (monitor)
            s.phase = "encode"
            s.enc_t0 = time.monotonic()
            for p in s.parts:
                p.last_change = time.monotonic()
            await self._monitor()

            # The original source is still available, and every encoded part
            # is now local. Free the (often very large) split input files before
            # creating temporary transport streams and the final MP4.
            for f in files:
                with contextlib.suppress(OSError):
                    f.unlink()

            # 5) MERGE
            s.phase = "merge"
            await self._merge(audio_task)
            return self.stats
        finally:
            if hasattr(self.pool, "hub") and self.pool.hub is not None:
                self.pool.hub.unregister(self.job.id)
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
            # clear_job ඉක්මනින් (parallel, තත්පර 1-2) — workers ට ඊළඟ job එකට සූදානම් වෙන්න
            with contextlib.suppress(BaseException):
                await self.pool.clear_job()
            # board messages (workers×) + Release assets මකන එක slow → merge/upload block නොකර background එකේ
            t = asyncio.create_task(self._cleanup_bg())
            _BG.add(t)
            t.add_done_callback(_BG.discard)

    async def _cleanup_bg(self):
        with contextlib.suppress(BaseException):
            if self.board:
                await self.board.delete()
        await asyncio.gather(*[self._g(ai).release_delete(self.tag) for ai in self.used], return_exceptions=True)

    # ── audio (main runner, workers encode කරන අතරේ) ──
    async def _extract_audio(self) -> str:
        out = str(self.work / "audio.mka")
        o = self.opts
        a = ["-c:a", "copy"] if o["audio_mode"] == "copy" else ["-c:a", "aac", "-b:a", f"{o['abitrate']}k"]
        await self._ff("-i", self.src, "-vn", "-sn", "-dn", "-map", "0:a?", *a, out, track=False)
        return out

    # ── monitor + idle-runner scheduler ──
    def _on_http_report(self, d: dict) -> None:
        try:
            idx = int(d.get("idx", -1))
        except (TypeError, ValueError):
            return
        if idx < 0 or idx >= len(self.state.parts):
            return
        p = self.state.parts[idx]
        if p.ready:
            return
        try:
            rep_attempt = int(d.get("attempt", 0))
        except (TypeError, ValueError):
            rep_attempt = 0
        if rep_attempt < p.attempt:
            return

        p.last_change = time.monotonic()
        p.via_http = True
        st = str(d.get("state", ""))
        if st == "downloading":
            if p.status in ("waiting", "held", "downloading"):
                p.status = "downloading"
        elif st in ("running", "uploading"):
            p.status = st
            p.out_time = float(d.get("out_time") or 0.0)
            p.fps = float(d.get("fps") or 0.0)
            p.speed = float(d.get("speed") or 0.0)
            p.size = int(d.get("size") or 0)
            p.frame = int(d.get("frame") or 0)
            if st == "uploading":
                p.out_time = p.duration
        elif st == "done":
            p.out_time = p.duration
            p.status = "remote_done"
        elif st == "failed":
            p.status = "failed"

    def _apply(self, p: Part, txt: str):
        if p.ready:
            return
        if txt != p.last_raw:
            p.last_raw, p.last_change = txt, time.monotonic()
        d = parse_prog(txt, self.job.id, p.idx)
        if not d:
            return
        st = d["state"]
        if st == "downloading":
            p.status = "downloading"
        elif st in ("running", "uploading"):
            p.status = st
            p.out_time, p.fps, p.speed, p.size, p.frame = (
                d["out_time"], d["fps"], d["speed"], d["size"], d["frame"]
            )
            if st == "uploading":
                p.out_time = p.duration
        elif st == "done":
            p.status = "remote_done"
        elif st == "failed":
            p.status = "failed"

    async def _ensure_release(self, ai: int):
        async with self._rlock:
            if ai not in self.used:
                await self._g(ai).release_create(self.tag)
                self.used.add(ai)

    async def _fetch(self, p: Part):
        p.fetching = True
        p.last_try = time.monotonic()
        try:
            path = self.work / f"encoded_part_{p.idx}.mkv"
            err = None
            # A part can finish on its original account after it was moved. Search
            # every release used by this job, not only the latest assigned account.
            order = [p.acct] + [x for x in sorted(self.used) if x != p.acct]
            for ai in order:
                try:
                    await self._g(ai).download(self.tag, f"encoded_part_{p.idx}.mkv", self.work)
                    if path.exists() and path.stat().st_size > 0:
                        break
                except Exception as e:
                    err = e
            else:
                raise DistError(f"encoded part not found ({err})")
            p.path, p.size = str(path), path.stat().st_size
            p.ready, p.status, p.speed, p.fps = True, "ready", 0.0, 0.0
            p.fetching = False
        except Exception as e:
            logger.warning(f"fetch part {p.idx} failed; retrying remote fetch: {e}")
            p.fetching, p.status = False, "remote_done"
            p.last_try = time.monotonic()

    def _hold(self, p: Part, reason: str):
        if p.ready:
            return
        if p.runner_idx >= 0:
            p.attempted_runners.add((p.acct, p.runner_idx))
        p.status = "held"
        p.speed = p.fps = 0.0
        p.last_try = time.monotonic()
        p.last_change = p.last_try
        logger.warning(f"part {p.idx}: held for an idle worker ({reason})")

    async def _idle_workers(self):
        """Return idle worker leases reported by workers after completing a part."""
        found = []
        async def read(a: Acct):
            if not a.active or a.failed or not a.tag:
                return []
            try:
                assets = await a.gh.assets(a.tag)
            except Exception as e:
                logger.debug(f"idle scan acct {a.idx} failed: {e}")
                return []
            known = a.dispatched | a.standby
            markers = {}
            for name, state in assets.items():
                if state != "uploaded":
                    continue
                bits = name.split("_")
                if len(bits) >= 2 and bits[0] in ("idle", "ready") and bits[1].isdigit():
                    wi = int(bits[1])
                    if wi not in known or name in a.claimed_idle:
                        continue
                    key = (a.idx, wi)
                    stamp = int(bits[2]) if len(bits) > 2 and bits[2].isdigit() else 0
                    old = markers.get(key)
                    if old is None or (bits[0] == "idle" and old[1].startswith("ready_")) or stamp > old[0]:
                        if old:
                            a.claimed_idle.add(old[1])
                        markers[key] = (stamp, name)
                    else:
                        a.claimed_idle.add(name)
            return [(a.idx, wi, data[1]) for (_, wi), data in markers.items()]
        rows = await asyncio.gather(*[read(a) for a in self.pool.accts])
        for row in rows:
            found.extend(row)
        return found

    async def _assign_part(self, ai: int, worker_idx: int, marker: str, p: Part):
        target = self.pool.accts[ai]
        old_ai = p.acct
        old = self.pool.accts[old_ai]
        raw = self.work / f"raw_part_{p.idx}.mp4"
        if not raw.exists() or raw.stat().st_size <= 0:
            raise DistError(f"raw part {p.idx} is missing; cannot reassign")

        await self._ensure_release(ai)
        if p.orphan or old_ai != ai:
            await target.gh.upload(self.tag, [raw])

        # Mark the original worker's default part as skipped before publishing the
        # new lease. A late-starting queued matrix job must not encode it twice.
        old.skip.add(p.idx)
        p.attempt += 1
        target.assign[worker_idx] = {"part": p.idx, "attempt": p.attempt}
        if worker_idx in target.standby:
            target.sb_busy[worker_idx] = p

        body = self.pool._body(target, "job", job=self.job.id, cfg=self.job_cfg)
        if self.board:
            await self.board.reset_part(p.idx)
        if old_ai != ai and old.active and not old.failed:
            await old.gh.mailbox_set(old.tag, self.pool._body(
                old, "job", job=self.job.id, cfg=self.job_cfg
            ))
        await target.gh.mailbox_set(target.tag, body)

        # Unique marker names mean deleting this consumed lease cannot delete a
        # fresh idle marker uploaded by the same worker after its next part.
        target.claimed_idle.add(marker)
        await target.gh.release_delete_asset(target.tag, marker)

        p.acct, p.runner_idx, p.orphan = ai, worker_idx, False
        p.status = "waiting"
        p.out_time = p.fps = p.speed = 0.0
        p.frame = p.size = 0
        p.last_raw = ""
        p.via_http = False
        p.last_change = time.monotonic()
        self.stats["reassigned_parts"] += 1
        logger.info(f"♻️ part {p.idx} reassigned → acct {ai}, worker {worker_idx}, attempt {p.attempt}")

    async def _schedule_held(self):
        """Assign held parts only to runners that have explicitly announced idle."""
        held = [p for p in self.state.parts
                if not p.ready and not p.fetching and p.status == "held"]
        if not held or self.pool.halted:
            return
        workers = await self._idle_workers()
        if not workers:
            return

        # Oldest held part first; one marker is a one-part lease and is consumed once.
        held.sort(key=lambda p: (p.last_try or p.last_change, p.idx))
        used_workers = set()
        for ai, worker_idx, marker in workers:
            if not held:
                break
            if (ai, worker_idx) in used_workers:
                continue
            eligible = [p for p in held
                        if (ai, worker_idx) not in p.attempted_runners
                        and not (p.acct == ai and p.runner_idx == worker_idx)]
            if not eligible:
                continue
            p = eligible[0]
            try:
                await self._assign_part(ai, worker_idx, marker, p)
                held.remove(p)
                used_workers.add((ai, worker_idx))
            except Exception as e:
                logger.warning(f"reassign part {p.idx} to acct {ai}/worker {worker_idx} failed: {e}")

    async def _monitor(self):
        s = self.state
        last_assets = 0.0
        last_sched = 0.0
        while not all(p.ready for p in s.parts):
            if self.job.cancelled:
                raise asyncio.CancelledError()
            if self.pool.halted:
                raise DistError("worker pool stopped (/End_Work); distributed job cannot continue")
            now = time.monotonic()
            if self.board is not None:
                try:
                    texts = await self.board.read()
                    for p, t in zip(s.parts, texts):
                        self._apply(p, t)
                except Exception as e:
                    logger.debug(f"board read failed: {e}")

            # Self-heal tunnel if cloudflared/ssh ever exited mid-job
            hub = getattr(self.pool, "hub", None)
            if hub is not None and hub._tunnel_proc is not None and hub._tunnel_proc.returncode is not None:
                new_url = await hub.ensure()
                if new_url and new_url != self.job_cfg.get("report_url"):
                    self.job_cfg["report_url"] = new_url
                    with contextlib.suppress(Exception):
                        await self.pool.post_job(self.job.id, self.job_cfg)

            # Check all releases used by this job for completed outputs & fallback started markers.
            if now - last_assets > 15 and self.used:
                last_assets = now
                async def assets_of(ai):
                    try:
                        return ai, await self._g(ai).assets(self.tag)
                    except Exception:
                        return ai, {}
                all_assets = await asyncio.gather(*[assets_of(ai) for ai in sorted(self.used)])
                uploaded = set()
                for ai, assets in all_assets:
                    uploaded.update(name for name, st in assets.items() if st == "uploaded")
                for p in s.parts:
                    if p.ready or p.fetching:
                        continue
                    if f"encoded_part_{p.idx}.mkv" in uploaded:
                        p.status = "remote_done"
                    elif p.status == "waiting" and f"started_{p.idx}_{p.attempt}" in uploaded:
                        p.status = "running"
                        p.last_change = now

            for p in s.parts:
                if p.ready or p.fetching:
                    continue
                idle = now - p.last_change
                stale_lim = config.DIST_STALE_SECONDS if p.via_http else max(config.DIST_STALE_SECONDS * 4, 900)
                if p.status == "remote_done":
                    if now - p.last_try >= 2.0:
                        self.tasks.append(asyncio.create_task(self._fetch(p)))
                elif p.status == "failed":
                    self._hold(p, "worker reported failure")
                elif p.status == "waiting" and idle > config.DIST_START_TIMEOUT:
                    self._hold(p, "runner has not started")
                elif p.status == "downloading" and idle > max(config.DIST_START_TIMEOUT * 3, 120):
                    self._hold(p, "raw download stalled")
                elif p.status in ("running", "uploading") and idle > stale_lim:
                    self._hold(p, "runner stopped reporting progress")

            if now - last_sched >= 6.0:
                last_sched = now
                await self._schedule_held()

            # Surface fetch errors promptly; held parts deliberately stay in the
            # scheduler queue instead of being encoded by the main bot runner.
            for task in self.tasks:
                if task.done() and not task.cancelled() and task.exception():
                    raise task.exception()
            await asyncio.sleep(max(1.0, min(PROGRESS_UPDATE_INTERVAL, 2.5)))

    # ── merge ──
    async def _merge(self, audio_task):
        s = self.state
        s.merge_t0 = time.monotonic()
        s.merge_step = "audio"                      # audio extract තාම ඉවර නැත්නම් මෙතන තමයි බලාගෙන ඉන්නේ
        audio = await audio_task if audio_task else None
        s.merge_step, s.merge_out, s.merge_size = "mux", 0.0, 0
        # Each worker starts a new encoder with its own codec parameter sets.
        # A straight MKV -> MP4 concat only keeps the first part's extradata;
        # later parts can then freeze in players even though the file is complete.
        # Remux each part to Annex B in MPEG-TS first, so its own headers are
        # present in-band at the join. No video is re-encoded here.
        segments = []
        bsf = "hevc_mp4toannexb" if self.opts["codec"] == "libx265" else "h264_mp4toannexb"
        lst = self.work / "concat.txt"
        try:
            for p in s.parts:
                segment = self.work / f"merge_part_{p.idx}.ts"
                segments.append(segment)  # also clean up a partially written segment
                await self._ff("-i", str(p.path), "-map", "0:v:0", "-c:v", "copy",
                               "-bsf:v", bsf, "-f", "mpegts", str(segment),
                               stage=f"prepare part {p.idx}")
            # concat demuxer uses single quotes and backslash escapes, not shell quoting.
            def concat_path(path):
                return str(path.resolve()).replace("'", "'\\''")
            lst.write_text("".join(f"file '{concat_path(p)}'\n" for p in segments))
            args = ["-f", "concat", "-safe", "0", "-i", str(lst)]
            if audio:
                args += ["-i", audio]
            args += ["-map", "0:v:0"]
            if audio:
                args += ["-map", "1:a"]
            args += ["-c", "copy"]
            if self.opts["codec"] == "libx265":
                # hev1 permits in-band VPS/SPS/PPS changes at part boundaries;
                # hvc1 requires all parameter sets in the MP4 sample entry.
                args += ["-tag:v", "hev1"]
            args += ["-movflags", "+faststart", self.dst]

            def cb(t, size, sp):
                s.merge_out, s.merge_size, s.merge_speed = t, size, sp

            await self._ff(*args, on_progress=cb, stage="final merge")
            s.merge_out = s.duration
        finally:
            for path in [*segments, lst]:
                with contextlib.suppress(OSError):
                    path.unlink()


def helpers_status() -> str:
    lines = [f"🤝 <b>Helper accounts: {len(HELPERS)}/{len(config.HELPER_TOKENS)}</b> • workers capacity <b>{capacity()}</b>"]
    lines += [f"• ✅ {r}" for _, r in HELPERS]
    lines += [f"• ❌ {p}" for p in HELPER_PROBLEMS]
    if not config.HELPER_TOKENS:
        lines.append("HELPER_TOKENS set කරලා නෑ.")
    return "\n".join(lines)
