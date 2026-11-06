"""
ghassets.py — GitHub Release asset transfer that survives flaky runner links
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ප්‍රශ්නය: `gh release upload` එක file එකම එක HTTP request එකකින් යවනවා. 200–400MB
part එකක් Actions runner එකකින් (හෝ slow link එකකින්) යද්දී body එක තත්පර
ගාණක් නිශ්චල වුණොත් GitHub: `HTTP 408: Upload body timed out due to inactivity`
කියලා request එක කපලා දානවා → මුළු job එකම fail.

විසඳුම (මේ module එකේ):
  • විශාල file එකක් කුඩා chunks වලට කඩලා එකින් එක (parallel 3ක්) upload කරනවා
    → stall එකක් වුණොත් යන්නේ ඒ chunk එක විතරයි, ආයෙ ඒකම retry වෙනවා.
  • හැම attempt එකකටත් timeout එකක් — hang වුණ upload එක kill කරලා ආයෙ try.
  • දැනටමත් සම්පූර්ණයෙන් upload වෙච්ච chunks skip → resume (ආයෙ එකම දේ upload නෑ).
  • අඩක් ගිය (state != uploaded) assets මකලා ආයෙ යවනවා (partial asset වළක්වන්න).
  • download පැත්තේද chunks තිබ්බොත් ඒවා අරගෙන පිළිවෙලට concat කරනවා.

Chunk name: "<original>.chunk000", ".chunk001" …  (assets 1000ක් දක්වා තියෙන්න පුළුවන්)
Worker (worker.py) සහ main runner (dist.py) දෙකම මේක import කරනවා — stdlib විතරයි.
"""
from __future__ import annotations

import contextlib
import json
import logging
import os
import random
import shutil
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

logger = logging.getLogger("GhAssets")

CHUNK_MARK = ".chunk"
MANIFEST_MARK = ".manifest"      # chunked file එකක් සම්පූර්ණද කියලා verify කරන්න

# ── knobs (env වලින් tune කරන්න පුළුවන්) ──
CHUNK_BYTES = max(4, int(os.getenv("GH_UPLOAD_CHUNK_MB", "24"))) * 1024 * 1024
SINGLE_BYTES = 12 * 1024 * 1024          # මීට කුඩා file → එකම request එකකින්
UPLOAD_PARALLEL = max(1, int(os.getenv("GH_UPLOAD_PARALLEL", "3")))
UPLOAD_ATTEMPTS = max(1, int(os.getenv("GH_UPLOAD_ATTEMPTS", "6")))
UPLOAD_TIMEOUT = float(os.getenv("GH_UPLOAD_TIMEOUT", "300"))      # එක request එකකට
DOWNLOAD_ATTEMPTS = max(1, int(os.getenv("GH_DOWNLOAD_ATTEMPTS", "5")))
DOWNLOAD_TIMEOUT = float(os.getenv("GH_DOWNLOAD_TIMEOUT", "900"))


class GhAssetError(Exception):
    """Asset transfer එකක් (retries ඔක්කොම ඉවර වෙලාත්) fail වුණා."""


# ───────────────────────────── low level ─────────────────────────────
def _gh(args: Sequence[str], env: Optional[dict] = None, timeout: Optional[float] = None):
    """(returncode, stdout, stderr). Timeout වුණොත් returncode = -9."""
    try:
        p = subprocess.run(["gh", *args], capture_output=True, text=True, env=env, timeout=timeout)
    except subprocess.TimeoutExpired:
        return -9, "", f"gh {args[0]} {args[1] if len(args) > 1 else ''}: timed out ({timeout:.0f}s)"
    except FileNotFoundError:
        raise GhAssetError("gh CLI not found")
    return p.returncode, p.stdout, p.stderr


def _short(txt: str, n: int = 240) -> str:
    return " ".join((txt or "").split())[-n:]


def assets(tag: str, repo: str, env: Optional[dict] = None) -> Dict[str, str]:
    """{asset name → state('uploaded' | 'starter' …)}. Release එක නැත්නම් {} ."""
    rc, out, err = _gh(["release", "view", tag, "--repo", repo, "--json", "assets"], env, 120)
    if rc != 0:
        logger.debug(f"assets({tag}) failed: {_short(err or out)}")
        return {}
    try:
        data = json.loads(out or "{}")
    except Exception:
        return {}
    return {a.get("name", ""): a.get("state", "uploaded") for a in data.get("assets", [])}


def _delete_asset(tag: str, name: str, repo: str, env: Optional[dict]) -> None:
    _gh(["release", "delete-asset", tag, name, "-y", "--repo", repo], env, 120)


def _upload_once(tag: str, path: Path, name: str, repo: str, env: Optional[dict]) -> None:
    rc, out, err = _gh(["release", "upload", tag, str(path), "--clobber", "--repo", repo],
                       env, UPLOAD_TIMEOUT)
    if rc != 0:
        raise GhAssetError(_short(err or out) or f"upload {name} failed (rc={rc})")


def _upload_retry(tag: str, path: Path, name: str, repo: str, env: Optional[dict],
                  attempts: int = UPLOAD_ATTEMPTS) -> None:
    """408 / timeout / network නිසා fail වුණොත් partial asset එක මකලා ආයෙ try."""
    last = ""
    for k in range(attempts):
        try:
            _upload_once(tag, path, name, repo, env)
            return
        except GhAssetError as e:
            last = str(e)
            logger.warning(f"⬆️ {name} attempt {k + 1}/{attempts} failed: {last}")
            # අඩක් ගිය asset එකක් ඉතුරු වුණොත් ඊළඟ attempt එකේදී 'already exists'
            # හෝ corrupt asset එකක් වෙන්න පුළුවන් → ඉවත් කරනවා.
            if k < attempts - 1:
                _delete_asset(tag, name, repo, env)
                time.sleep(min(2 ** k, 20) * (0.6 + random.random()))
    raise GhAssetError(f"{name}: {last}")


def _write_chunk(src: Path, dst: Path, offset: int, size: int) -> None:
    with open(src, "rb") as f, open(dst, "wb") as o:
        f.seek(offset)
        left = size
        while left > 0:
            buf = f.read(min(1 << 20, left))
            if not buf:
                break
            o.write(buf)
            left -= len(buf)


def _concat(parts: Iterable[Path], out: Path) -> None:
    with open(out, "wb") as o:
        for p in parts:
            with open(p, "rb") as i:
                shutil.copyfileobj(i, o, 1 << 20)


# ───────────────────────────── upload ─────────────────────────────
def upload(tag: str, paths: Sequence, repo: str, env: Optional[dict] = None,
           chunk: int = CHUNK_BYTES, parallel: int = UPLOAD_PARALLEL) -> List[str]:
    """
    File(s) → Release assets. විශාල ඒවා chunked (resume-capable). Fail වුණොත් GhAssetError.
    Return: upload කළ asset නම් වල ලැයිස්තුවක් (chunked නම් chunk නම්).
    """
    files = [Path(p) for p in paths]
    if not files:
        return []
    have = assets(tag, repo, env)
    tmpdir = Path(tempfile.mkdtemp(prefix="ghassets_"))
    made: List[str] = []

    class _Job:
        __slots__ = ("path", "name", "tmp", "off", "size")

        def __init__(self, path: Path, name: str, tmp: Path, off: int, size: int):
            self.path, self.name, self.tmp, self.off, self.size = path, name, tmp, off, size

    plan: List[_Job] = []
    try:
        for f in files:
            size = f.stat().st_size
            if size <= 0:
                raise GhAssetError(f"{f.name}: empty file")
            if size <= SINGLE_BYTES:
                if have.get(f.name) == "uploaded" and not _force():
                    logger.info(f"⬆️ {f.name}: already uploaded — skip")
                    continue
                plan.append(_Job(f, f.name, Path(), 0, 0))       # tmp එකක් ඕන නෑ
                continue

            n = (size + chunk - 1) // chunk
            names = [f"{f.name}{CHUNK_MARK}{i:03d}" for i in range(n)]
            # පරණ direct asset / manifest එකක් තිබ්බොත් (partial) මකනවා — downloader එක
            # අසම්පූර්ණ දෙයක් සම්පූර්ණ කියලා හිතන එක වළක්වන්න.
            for stale in (f.name, f.name + MANIFEST_MARK):
                if stale in have:
                    _delete_asset(tag, stale, repo, env)
            todo = 0
            for i, name in enumerate(names):
                st = have.get(name)
                if st == "uploaded":
                    continue                                    # resume: දැනටමත් තියෙනවා
                if st:
                    _delete_asset(tag, name, repo, env)         # partial → අයින්
                # temp file එකේ නම = asset නම (gh upload කරන්නේ basename එකෙන්)
                plan.append(_Job(f, name, tmpdir / name, i * chunk,
                                 min(chunk, size - i * chunk)))
                todo += 1
            if todo:
                logger.info(f"⬆️ {f.name}: {size / 1048576:.0f}MB → {todo}/{n} chunks "
                            f"({chunk / 1048576:.0f}MB each)")
            else:
                logger.info(f"⬆️ {f.name}: all {n} chunks already uploaded — skip")

        def work(j: _Job):
            if j.off or j.size:
                _write_chunk(j.path, j.tmp, j.off, j.size)
                try:
                    _upload_retry(tag, j.tmp, j.name, repo, env)
                finally:
                    j.tmp.unlink(missing_ok=True)
            else:
                _upload_retry(tag, j.path, j.name, repo, env)

        if plan:
            with ThreadPoolExecutor(max_workers=max(1, min(parallel, len(plan)))) as ex:
                for fut in [ex.submit(work, j) for j in plan]:
                    fut.result()                            # මුල් exception එකම උඩට

        # chunks ඔක්කොම සාර්ථක වුණාට පස්සේ විතරයි manifest එක (size verification).
        for f in files:
            size = f.stat().st_size
            if size <= SINGLE_BYTES:
                continue
            n = (size + chunk - 1) // chunk
            if f"{f.name}{CHUNK_MARK}000" not in have and not any(
                    j.name.startswith(f.name + CHUNK_MARK) for j in plan):
                continue
            mf = tmpdir / (f.name + MANIFEST_MARK)
            mf.write_text(json.dumps({"name": f.name, "size": size, "chunk": chunk, "parts": n}))
            _upload_retry(tag, mf, mf.name, repo, env)
            made.append(mf.name)

        made = [j.name for j in plan] + [m for m in made if m.endswith(MANIFEST_MARK)]
        return made
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def _force() -> bool:
    return os.getenv("GH_UPLOAD_FORCE", "") == "1"


# ───────────────────────────── download ─────────────────────────────
def _download_pattern(tag: str, pattern: str, dest: Path, repo: str, env: Optional[dict],
                      attempts: int = DOWNLOAD_ATTEMPTS) -> None:
    last = ""
    for k in range(attempts):
        rc, out, err = _gh(["release", "download", tag, "-p", pattern, "-D", str(dest), "--clobber",
                            "--repo", repo], env, DOWNLOAD_TIMEOUT)
        if rc == 0:
            return
        last = _short(err or out)
        logger.warning(f"⬇️ {pattern} attempt {k + 1}/{attempts} failed: {last}")
        if k < attempts - 1:
            time.sleep(min(2 ** k, 15) * (0.5 + random.random()))
    raise GhAssetError(f"download {pattern}: {last}")


def download(tag: str, name: str, dest_dir, repo: str, env: Optional[dict] = None) -> Path:
    """
    Release asset එකක් (chunked නම් chunks ටික concat කරලා) dest_dir/name එකට.
    Return: ලැබුණු file path (size > 0 නම් විතරයි).
    """
    dest = Path(dest_dir)
    dest.mkdir(parents=True, exist_ok=True)
    out = dest / name
    have = assets(tag, repo, env)

    # 1) සාමාන්‍ය (chunk නොකළ) asset එක
    if have.get(name) == "uploaded":
        _download_pattern(tag, name, dest, repo, env)
        if out.exists() and out.stat().st_size > 0:
            return out
        out.unlink(missing_ok=True)

    # 2) chunked asset එකක්
    chunks = sorted(n for n in have if n.startswith(name + CHUNK_MARK))
    if not chunks:
        raise GhAssetError(f"{name}: asset එකක් හමු වුණේ නෑ ({tag})")

    tmp = Path(tempfile.mkdtemp(prefix="ghdl_"))
    try:
        expected = None
        if have.get(name + MANIFEST_MARK) == "uploaded":
            with contextlib.suppress(Exception):
                _download_pattern(tag, name + MANIFEST_MARK, tmp, repo, env)
                expected = int(json.loads((tmp / (name + MANIFEST_MARK)).read_text()).get("size") or 0) or None
        ordered = []
        for c in chunks:
            _download_pattern(tag, c, tmp, repo, env)
            p = tmp / c
            if not p.exists() or p.stat().st_size <= 0:
                raise GhAssetError(f"chunk {c} download වුණේ නෑ / හිස්")
            ordered.append(p)
        _concat(ordered, out)
        size = out.stat().st_size if out.exists() else 0
        if size <= 0:
            raise GhAssetError(f"{name}: concat failed")
        if expected and size != expected:
            out.unlink(missing_ok=True)
            raise GhAssetError(f"{name}: size mismatch {size} != {expected} (chunk korrupt)")
        logger.info(f"⬇️ {name}: {len(ordered)} chunks → {size / 1048576:.1f}MB")
        return out
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def download_pattern(tag: str, pattern: str, dest_dir, repo: str, env: Optional[dict] = None) -> None:
    """සාමාන්‍ය glob download (retries සමඟ) — කුඩා/normal assets වලට."""
    dest = Path(dest_dir)
    dest.mkdir(parents=True, exist_ok=True)
    _download_pattern(tag, pattern, dest, repo, env)


def purge_chunks(tag: str, name: str, repo: str, env: Optional[dict] = None) -> None:
    """file එකක chunk assets ඔක්කොම මකනවා (release cleanup වලට)."""
    for n in [n for n in assets(tag, repo, env)
              if n.startswith(name + CHUNK_MARK) or n == name + MANIFEST_MARK]:
        _delete_asset(tag, n, repo, env)
