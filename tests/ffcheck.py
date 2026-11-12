"""Shared helpers for playback/merge tests (no production dependency)."""
import json
import re
import shutil
import subprocess

FFMPEG = shutil.which("ffmpeg")
FFPROBE = shutil.which("ffprobe")

from ffenv import ffmpeg_env


def ff(*args):
    return subprocess.run([FFMPEG, "-hide_banner", "-loglevel", "error", "-y", *map(str, args)],
                          check=True, capture_output=True, env=ffmpeg_env())


def probe(path):
    """ffprobe JSON when available; otherwise parse `ffmpeg -i` output."""
    if FFPROBE:
        r = subprocess.run(["ffprobe", "-v", "error", "-print_format", "json",
                            "-show_format", "-show_streams", str(path)],
                           capture_output=True, text=True, env=ffmpeg_env())
        try:
            return json.loads(r.stdout or "{}")
        except ValueError:
            return {}
    r = subprocess.run([FFMPEG, "-hide_banner", "-i", str(path)],
                       capture_output=True, text=True, errors="replace", env=ffmpeg_env())
    streams, audio = [], False
    for line in r.stderr.splitlines():
        line = line.strip()
        if line.startswith("Stream #") and "Video:" in line:
            codec = re.search(r"Video: (\w+)", line)
            dims = re.findall(r"(\d{2,5})x(\d{2,5})", line)
            if codec and dims:
                w, h = dims[-1]
                stream = {"codec_type": "video", "codec_name": codec.group(1),
                          "width": int(w), "height": int(h)}
                sar = re.search(r"SAR (\d+:\d+|\([^)]*\)) DAR (\d+:\d+)", line)
                if sar:
                    stream["sample_aspect_ratio"] = sar.group(1)
                    stream["display_aspect_ratio"] = sar.group(2)
                streams.append(stream)
        elif line.startswith("Stream #") and "Audio:" in line:
            audio = True
    if audio:
        streams.append({"codec_type": "audio"})
    return {"streams": streams}


def full_decode_ok(path):
    r = subprocess.run([FFMPEG, "-v", "error", "-xerror", "-err_detect", "explode",
                        "-i", str(path), "-f", "null", "-"],
                       capture_output=True, env=ffmpeg_env())
    return r.returncode == 0


def stream_info(path):
    info = probe(path)
    return next((s for s in info.get("streams", []) if s.get("codec_type") == "video"), {})


def frames(path):
    out = ff("-i", path, "-map", "0:v:0", "-f", "framemd5", "-").stdout
    return [line.split(b",")[-1].strip() for line in out.splitlines() if line and not line.startswith(b"#")]
