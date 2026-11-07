"""
settings_store.py — Per-user settings store (Resolution Mode + Size-Reduce Mode)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• Resolution වෙනස් කරන mode එකේ settings සහ Size-Reduce (H.265 Visually Lossless) mode එකේ
  settings දෙකම එකිනෙකට වෙනම user_settings.json තුළ save වේ.
• GitHub Cloud Sync: GH_TOKEN + GITHUB_REPOSITORY ඇති විට (GitHub Actions ඇතුළුව) ස්වයංක්‍රීයව
  GitHub repo එකේ user_settings.json වෙත sync කරන නිසා bot restart වුණත් user selections කිසිවක් reset නොවේ.
"""
import base64
import json
import logging
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Optional

import config
from config import (
    CODECS,
    COMPRESS_AUTO_CHOICES,
    CRF_CHOICES,
    DEFAULT_CODEC,
    DEFAULT_COMPRESS_CRF,
    DEFAULT_COMPRESS_PIX_FMT,
    DEFAULT_COMPRESS_PRESET,
    DEFAULT_MODE,
    DEFAULT_PRESET,
    DEFAULT_WORKERS,
    ENCODE_MODES,
    PIX_FMT_CHOICES,
    PRESETS,
    QUEUE_RES_CHOICES,
    SETTINGS_FILE,
    WORKER_CHOICES,
)

logger = logging.getLogger("SettingsStore")


def _to_bool(val: Any, default: bool = True) -> bool:
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return bool(val)
    if isinstance(val, str):
        low = val.strip().lower()
        if low in ("1", "true", "on", "yes"):
            return True
        if low in ("0", "false", "off", "no"):
            return False
    return default


def _normalize_user_cfg(s: Optional[dict]) -> dict:
    s = s if isinstance(s, dict) else {}
    mode = s.get("mode", DEFAULT_MODE)
    preset = s.get("preset", DEFAULT_PRESET)
    codec = s.get("codec", DEFAULT_CODEC)
    dist_m = s.get("dist", "auto")
    qres = str(s.get("qres", "ask"))

    try:
        workers = int(s.get("workers", DEFAULT_WORKERS))
    except (TypeError, ValueError):
        workers = DEFAULT_WORKERS

    c_preset = s.get("c_preset", DEFAULT_COMPRESS_PRESET)
    c_dist = s.get("c_dist", "auto")
    c_auto = s.get("c_auto", "ask")
    c_pix = s.get("c_pix_fmt", DEFAULT_COMPRESS_PIX_FMT)

    try:
        c_workers = int(s.get("c_workers", DEFAULT_WORKERS))
    except (TypeError, ValueError):
        c_workers = DEFAULT_WORKERS

    try:
        c_crf = int(s.get("c_crf", DEFAULT_COMPRESS_CRF))
    except (TypeError, ValueError):
        c_crf = DEFAULT_COMPRESS_CRF

    return {
        # Active mode ("res" | "compress")
        "mode": mode if mode in ENCODE_MODES else DEFAULT_MODE,
        # ── Mode 1: Resolution Change Mode ──
        "preset": preset if preset in PRESETS else DEFAULT_PRESET,
        "codec": codec if codec in CODECS else DEFAULT_CODEC,
        "dist": dist_m if dist_m in ("auto", "on", "off") else "auto",
        "workers": workers if workers in WORKER_CHOICES else DEFAULT_WORKERS,
        "qres": qres if qres in QUEUE_RES_CHOICES else "ask",
        # ── Mode 2: Size-Reduce / Visually Lossless H.265 Mode ──
        "c_preset": c_preset if c_preset in PRESETS else DEFAULT_COMPRESS_PRESET,
        "c_dist": c_dist if c_dist in ("auto", "on", "off") else "auto",
        "c_workers": c_workers if c_workers in WORKER_CHOICES else DEFAULT_WORKERS,
        "c_auto": c_auto if c_auto in COMPRESS_AUTO_CHOICES else "ask",
        "c_crf": c_crf if c_crf in CRF_CHOICES else DEFAULT_COMPRESS_CRF,
        "c_pix_fmt": c_pix if c_pix in PIX_FMT_CHOICES else DEFAULT_COMPRESS_PIX_FMT,
        "c_bframes": _to_bool(s.get("c_bframes"), True),
        "c_sao": _to_bool(s.get("c_sao"), True),
        "c_lookahead": _to_bool(s.get("c_lookahead"), True),
        "c_aq": _to_bool(s.get("c_aq"), True),
    }


class SettingsStore:
    def __init__(
        self,
        path: str = SETTINGS_FILE,
        *,
        gh_token: Optional[str] = None,
        gh_repo: Optional[str] = None,
        gh_path: Optional[str] = None,
        gh_branch: Optional[str] = None,
        debounce_sec: float = 2.5,
    ):
        self.path = path
        self.gh_token = (
            gh_token
            if gh_token is not None
            else (os.getenv("SETTINGS_GH_TOKEN") or os.getenv("GH_TOKEN") or os.getenv("GITHUB_TOKEN") or "")
        ).strip()
        self.gh_repo = (
            gh_repo
            if gh_repo is not None
            else (os.getenv("SETTINGS_GH_REPO") or os.getenv("GITHUB_REPOSITORY") or "")
        ).strip()
        self.gh_path = (
            gh_path
            if gh_path is not None
            else getattr(config, "SETTINGS_GH_PATH", SETTINGS_FILE)
        ).strip("/")
        self.gh_branch = (
            gh_branch
            if gh_branch is not None
            else (getattr(config, "SETTINGS_GH_BRANCH", "") or os.getenv("GITHUB_REF_NAME") or "")
        ).strip()

        self._debounce = max(0.05, float(debounce_sec))
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._data: Dict[str, dict] = {}
        self._gh_sha: Optional[str] = None
        self._resolved_branch: Optional[str] = self.gh_branch or None
        self._dirty_seq: int = 0
        self._pushed_seq: int = 0
        self._last_sync_ok: Optional[bool] = None
        self._last_sync_err: str = ""
        self._closed = False
        self._worker_thread: Optional[threading.Thread] = None

        # 1) Load local file first
        self._load_local()

        # 2) If GitHub Cloud Sync is available, pull latest JSON from GitHub right at startup
        if self.gh_enabled:
            self._pull_from_github_startup()

    @property
    def gh_enabled(self) -> bool:
        return bool(self.gh_token and self.gh_repo and "/" in self.gh_repo)

    def _load_local(self) -> None:
        if os.path.exists(self.path):
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    raw = json.load(f)
                if isinstance(raw, dict):
                    self._data = {str(k): _normalize_user_cfg(v) for k, v in raw.items()}
            except Exception as e:
                logger.warning(f"Local settings read failed ({self.path}): {e}")
                self._data = {}

    def _write_local_unlocked(self) -> None:
        parent = os.path.dirname(os.path.abspath(self.path))
        if parent:
            os.makedirs(parent, exist_ok=True)
        tmp = f"{self.path}.tmp.{threading.get_ident()}"
        payload = json.dumps(self._data, indent=2, ensure_ascii=False, sort_keys=True) + "\n"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(payload)
        os.replace(tmp, self.path)

    # ───────────────────────── GitHub REST API Sync ─────────────────────────
    def _gh_request(self, method: str, url: str, body: Optional[dict] = None) -> tuple:
        headers = {
            "Authorization": f"Bearer {self.gh_token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "encoder-bot-settings-sync",
        }
        data_bytes = None
        if body is not None:
            headers["Content-Type"] = "application/json; charset=utf-8"
            data_bytes = json.dumps(body).encode("utf-8")

        req = urllib.request.Request(url, data=data_bytes, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                raw = resp.read().decode("utf-8")
                return resp.status, json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            err_body = ""
            try:
                err_body = e.read().decode("utf-8", errors="ignore")
            except Exception:
                pass
            return e.code, err_body

    def _ensure_branch(self) -> str:
        if self._resolved_branch:
            return self._resolved_branch
        status, data = self._gh_request("GET", f"https://api.github.com/repos/{self.gh_repo}")
        if status == 200 and isinstance(data, dict) and data.get("default_branch"):
            self._resolved_branch = str(data["default_branch"])
        else:
            self._resolved_branch = "main"
        return self._resolved_branch

    def _fetch_remote_file(self, branch: str) -> tuple:
        """Returns (status_code, sha_or_None, parsed_dict_or_None)."""
        quoted_path = urllib.parse.quote(self.gh_path, safe="/")
        qs = urllib.parse.urlencode({"ref": branch}) if branch else ""
        url = f"https://api.github.com/repos/{self.gh_repo}/contents/{quoted_path}" + (f"?{qs}" if qs else "")
        status, data = self._gh_request("GET", url)
        if status == 200 and isinstance(data, dict):
            sha = data.get("sha")
            b64 = data.get("content", "")
            try:
                decoded = base64.b64decode(b64).decode("utf-8")
                parsed = json.loads(decoded or "{}")
                if isinstance(parsed, dict):
                    return 200, sha, parsed
            except Exception as e:
                logger.warning(f"GitHub {self.gh_path} JSON parse warning: {e}")
                return 200, sha, {}
        return status, None, None

    def _pull_from_github_startup(self) -> None:
        try:
            branch = self._ensure_branch()
            status, sha, remote_data = self._fetch_remote_file(branch)
            if status == 200 and remote_data is not None:
                with self._lock:
                    merged = {str(k): _normalize_user_cfg(v) for k, v in remote_data.items()}
                    # Preserve any local keys not yet in remote while remote takes precedence on startup
                    for uk, uv in self._data.items():
                        merged.setdefault(uk, uv)
                    self._data = merged
                    self._gh_sha = sha
                    self._last_sync_ok = True
                    self._write_local_unlocked()
                logger.info(
                    f"☁️ Loaded user settings from GitHub ({self.gh_repo}:{branch}/{self.gh_path}, "
                    f"{len(self._data)} user(s))"
                )
            elif status == 404:
                logger.info(
                    f"☁️ GitHub {self.gh_repo}:{branch}/{self.gh_path} not created yet — will create on first change"
                )
            else:
                logger.warning(f"☁️ GitHub settings fetch returned HTTP {status}")
        except Exception as e:
            logger.warning(f"☁️ GitHub settings startup load skipped: {e}")

    def _ensure_sync_worker_unlocked(self) -> None:
        if not self.gh_enabled or self._closed:
            return
        if self._worker_thread is None or not self._worker_thread.is_alive():
            self._worker_thread = threading.Thread(
                target=self._sync_loop,
                name="SettingsGitHubSync",
                daemon=True,
            )
            self._worker_thread.start()

    def _push_snapshot_to_github(self, snapshot: Dict[str, dict]) -> bool:
        branch = self._ensure_branch()
        quoted_path = urllib.parse.quote(self.gh_path, safe="/")
        url = f"https://api.github.com/repos/{self.gh_repo}/contents/{quoted_path}"

        for attempt in range(3):
            with self._lock:
                sha = self._gh_sha

            if sha is None and attempt == 0:
                st0, remote_sha0, remote_dict0 = self._fetch_remote_file(branch)
                if st0 == 200:
                    sha = remote_sha0
                    with self._lock:
                        self._gh_sha = sha
                    if isinstance(remote_dict0, dict):
                        merged = {str(k): _normalize_user_cfg(v) for k, v in remote_dict0.items()}
                        merged.update(snapshot)
                        snapshot = merged

            raw_json = (json.dumps(snapshot, indent=2, ensure_ascii=False, sort_keys=True) + "\n").encode("utf-8")
            body: Dict[str, Any] = {
                "message": f"chore: update {self.gh_path} [skip ci]",
                "content": base64.b64encode(raw_json).decode("ascii"),
            }
            if branch:
                body["branch"] = branch
            if sha:
                body["sha"] = sha

            status, resp = self._gh_request("PUT", url, body)
            if status in (200, 201) and isinstance(resp, dict):
                new_sha = (resp.get("content") or {}).get("sha")
                with self._lock:
                    if new_sha:
                        self._gh_sha = new_sha
                    self._last_sync_ok = True
                    self._last_sync_err = ""
                return True

            # Handle SHA conflict (409 / 422): pull fresh SHA + merge non-overlapping users, then retry
            if status in (409, 422) and attempt < 2:
                st_r, fresh_sha, remote_dict = self._fetch_remote_file(branch)
                if st_r == 200:
                    with self._lock:
                        self._gh_sha = fresh_sha
                        if isinstance(remote_dict, dict):
                            base_map = {str(k): _normalize_user_cfg(v) for k, v in remote_dict.items()}
                            base_map.update(snapshot)
                            base_map.update(self._data)
                            self._data = base_map
                            snapshot = json.loads(json.dumps(self._data))
                            self._write_local_unlocked()
                    continue

            with self._lock:
                self._last_sync_ok = False
                self._last_sync_err = f"HTTP {status}"
            logger.warning(f"☁️ GitHub settings push failed (HTTP {status}): {str(resp)[:180]}")
            return False
        return False

    def _sync_loop(self) -> None:
        while True:
            with self._cond:
                while not self._closed and self._dirty_seq <= self._pushed_seq:
                    self._cond.wait(timeout=30.0)
                if self._closed and self._dirty_seq <= self._pushed_seq:
                    return

                # Debounce rapid button taps so 5 clicks coalesce into 1 clean GitHub commit
                while not self._closed:
                    target_seq = self._dirty_seq
                    self._cond.wait(timeout=self._debounce)
                    if self._dirty_seq == target_seq:
                        break

                target_seq = self._dirty_seq
                snapshot = json.loads(json.dumps(self._data))

            try:
                self._push_snapshot_to_github(snapshot)
            except Exception as e:
                with self._lock:
                    self._last_sync_ok = False
                    self._last_sync_err = str(e)[:80]
                logger.warning(f"☁️ GitHub settings sync error: {e}")

            with self._cond:
                self._pushed_seq = max(self._pushed_seq, target_seq)
                self._cond.notify_all()

    def flush(self, timeout: float = 8.0) -> bool:
        """Wait for any pending GitHub settings sync to finish (used on graceful shutdown)."""
        if not self.gh_enabled:
            return True
        deadline = time.monotonic() + max(0.1, timeout)
        with self._cond:
            self._cond.notify_all()
            while self._pushed_seq < self._dirty_seq:
                rem = deadline - time.monotonic()
                if rem <= 0:
                    return False
                self._cond.wait(timeout=rem)
        return True

    def cloud_status_text(self) -> str:
        if not self.gh_enabled:
            return "💾 <code>Local JSON</code> <i>(GH_TOKEN නැති නිසා local පමණයි)</i>"
        if self._dirty_seq > self._pushed_seq:
            return f"☁️ <code>GitHub ({self.gh_path})</code> • 🔄 <i>Syncing…</i>"
        if self._last_sync_ok is False:
            return f"☁️ <code>GitHub ({self.gh_path})</code> • ⚠️ <i>{self._last_sync_err or 'Sync retry'}</i>"
        return f"☁️ <code>GitHub ({self.gh_path})</code> ✅ <i>(Restart වුණත් reset නොවේ)</i>"

    # ───────────────────────── Public Get / Set API ─────────────────────────
    def get(self, user_id: int) -> dict:
        with self._lock:
            s = self._data.get(str(user_id), {})
            return _normalize_user_cfg(s)

    def set(self, user_id: int, **kwargs) -> dict:
        with self._cond:
            cur = _normalize_user_cfg(self._data.get(str(user_id), {}))
            for k, v in kwargs.items():
                if v is not None:
                    cur[k] = v
            norm = _normalize_user_cfg(cur)
            self._data[str(user_id)] = norm
            self._write_local_unlocked()
            if self.gh_enabled:
                self._dirty_seq += 1
                self._ensure_sync_worker_unlocked()
                self._cond.notify_all()
            return dict(norm)
