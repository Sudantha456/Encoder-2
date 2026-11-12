"""Environment overrides for ffmpeg/ffprobe subprocesses.

The static Linux builds downloaded in the GitHub workflows link glibc
statically. Their MPEG-TS demuxer converts the ISO-6937 service name with
iconv, which makes glibc ``dlopen()`` the *host* gconv module. That module is
built against a different glibc than the statically linked one, so loading it
crashes the process with SIGSEGV (exit code -11) before it can print any
error. Pointing ``GCONV_PATH`` at a directory without gconv modules stops the
load; the affected conversions then report failure instead of crashing.

Only Linux is touched; other platforms get the inherited environment.
"""
import os
import sys
import tempfile

_EMPTY_GCONV: str | None = None


def ffmpeg_env() -> dict | None:
    """Return a subprocess env for ffmpeg/ffprobe, or None to inherit."""
    global _EMPTY_GCONV
    if not sys.platform.startswith("linux"):
        return None
    if _EMPTY_GCONV is None:
        path = os.path.join(tempfile.gettempdir(), "encoder-empty-gconv")
        os.makedirs(path, exist_ok=True)
        _EMPTY_GCONV = path
    env = os.environ.copy()
    env["GCONV_PATH"] = _EMPTY_GCONV
    return env
