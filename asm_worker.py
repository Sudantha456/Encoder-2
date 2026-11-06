"""
asm_worker.py — Helper GitHub runner: assemble slices → split into encode parts (byte-exact)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  1. Main runner verified bytes; මෙතන ආපහු size එකෙන් verify කරලා cat කරනවා (byte-identical source)
  2. ffprobe sanity check — valid video stream?
  3. keyframes හොයලා SPLITS ගාණට parts කඩනවා (ffmpeg -c copy — quality loss නෑ)
  4. audio extract (audio.mka) — main runner එක merge එකේදී use කරනවා
  5. OWN repo release `enc-<job>` එකට raw parts + audio.mka + asm_done.json upload

Env: JOB, SOURCES (JSON: [{"repo","tag","name","size"}...] — slice index order irrelevant, name එකේ idx තියෙනවා),
     TOTAL_SIZE, DURATION, SPLITS, AUDIO_MODE (copy|aac|none), ABITRATE (kbps)
Secrets: GH_TOKEN (GITHUB_TOKEN — own repo releases), BOT_TOKEN
"""
import json
import os
import re
import subprocess
import sys

JOB = os.environ.get("JOB", "")
TAG = f"enc-{JOB}"
TOTAL = int(os.environ.get("TOTAL_SIZE", "0") or 0)
DURATION = float(os.environ.get("DURATION", "0") or 0)
SPLITS = max(1, int(os.environ.get("SPLITS", "1") or 1))
AUDIO_MODE = os.environ.get("AUDIO_MODE", "aac")
ABITRATE = int(os.environ.get("ABITRATE", "128") or 128)


def gh(*args, check=True):
    r = subprocess.run(["gh", *args], capture_output=True, text=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"gh {' '.join(args[:2])} failed: {r.stderr[-300:]}")
    return r


def run(cmd, **kw):
    r = subprocess.run(cmd, capture_output=True, text=True, **kw)
    if r.returncode != 0:
        raise RuntimeError(f"{cmd[0]} failed: {(r.stderr or r.stdout)[-400:]}")
    return r


def keyframes(path):
    r = subprocess.run(["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries",
                        "packet=pts_time,flags", "-of", "csv=p=0", path],
                       capture_output=True, text=True)
    kfs = []
    for line in (r.stdout or "").splitlines():
        parts = line.strip().split(",")
        if len(parts) >= 2 and "K" in parts[1]:
            try:
                kfs.append(float(parts[0]))
            except ValueError:
                pass
    return sorted(kfs)


def pick_splits(kfs, duration, n, min_len=3.0):
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


def main():
    sources = json.loads(os.environ.get("SOURCES", "[]"))
    if not JOB or not sources or TOTAL <= 0:
        print("bad config", file=sys.stderr, flush=True)
        sys.exit(2)
    print(f"assembling {len(sources)} slices → source ({TOTAL} bytes)", flush=True)

    os.makedirs("slices", exist_ok=True)
    ordered = sorted(sources, key=lambda s: int(re.findall(r"\d+", s["name"])[-1]))

    # 1+2) download & EXACT size verify — bytes miss වුණොත් මෙතනම නවතිනවා
    total_got = 0
    for s in ordered:
        r = gh("release", "download", s["tag"], "-R", s["repo"], "-p", s["name"],
               "-D", "slices", "--clobber", check=False)
        p = os.path.join("slices", s["name"])
        if r.returncode != 0 or not os.path.exists(p):
            raise RuntimeError(f"slice download failed: {s['repo']}/{s['tag']}/{s['name']}: {r.stderr[-200:]}")
        got = os.path.getsize(p)
        if got != int(s["size"]):
            raise RuntimeError(f"slice {s['name']} size mismatch: {got} != {s['size']} (byte-exact නෑ)")
        total_got += got
        print(f"  ✓ {s['name']}: {got} bytes from {s['repo']}", flush=True)
    if total_got != TOTAL:
        raise RuntimeError(f"total size mismatch: {total_got} != {TOTAL}")

    with open("src.mp4", "wb") as out:
        for s in ordered:
            with open(os.path.join("slices", s["name"]), "rb") as f:
                while True:
                    chunk = f.read(8 * 1024 * 1024)
                    if not chunk:
                        break
                    out.write(chunk)
    with open("src.mp4", "rb") as f:               # re-open & verify — full reassembly check
        f.seek(0, os.SEEK_END)
        if f.tell() != TOTAL:
            raise RuntimeError("reassembled size mismatch")
    print(f"source ready: {TOTAL} bytes", flush=True)

    # 3) ffprobe sanity — video stream එකක් ඕනේ
    info = json.loads(run(["ffprobe", "-v", "error", "-print_format", "json",
                           "-show_format", "-show_streams", "src.mp4"]).stdout or "{}")
    vstreams = [s for s in info.get("streams", []) if s.get("codec_type") == "video"]
    if not vstreams:
        raise RuntimeError("reassembled file එකේ video stream එකක් නෑ")
    dur = float((info.get("format") or {}).get("duration") or DURATION or 0)
    if dur <= 0:
        dur = DURATION

    # 4) keyframes → SPLITS parts
    kfs = keyframes("src.mp4")
    splits = pick_splits(kfs, dur, SPLITS)
    bounds = [0.0] + splits + [dur]
    print(f"keyframes: {len(kfs)} • split points: {len(splits)}", flush=True)
    if splits:
        run(["ffmpeg", "-hide_banner", "-nostdin", "-y", "-i", "src.mp4",
             "-map", "0:v:0", "-c", "copy", "-an", "-sn", "-dn",
             "-f", "segment", "-segment_times", ",".join(f"{t:.3f}" for t in splits),
             "-reset_timestamps", "1", "-segment_format", "mp4", "raw_part_%d.mp4"])
    else:
        run(["ffmpeg", "-hide_banner", "-nostdin", "-y", "-i", "src.mp4",
             "-map", "0:v:0", "-c", "copy", "-an", "-sn", "-dn", "raw_part_0.mp4"])

    parts = sorted((p for p in os.listdir(".") if re.fullmatch(r"raw_part_\d+\.mp4", p)),
                   key=lambda p: int(re.findall(r"\d+", p)[0]))
    if len(parts) != len(bounds) - 1:
        raise RuntimeError(f"split mismatch: {len(parts)} files vs {len(bounds) - 1} bounds")

    # 5) audio
    aconf = ["-c:a", "copy"] if AUDIO_MODE == "copy" else (["-c:a", "aac", "-b:a", f"{ABITRATE}k"]
                                                          if AUDIO_MODE == "aac" else None)
    audio_name = ""
    if aconf and any(s.get("codec_type") == "audio" for s in info.get("streams", [])):
        run(["ffmpeg", "-hide_banner", "-nostdin", "-y", "-i", "src.mp4", "-vn", "-sn", "-dn",
             "-map", "0:a?", *aconf, "audio.mka"])
        if os.path.exists("audio.mka") and os.path.getsize("audio.mka") > 0:
            audio_name = "audio.mka"
        else:
            audio_name = ""

    # 6) upload → own repo release enc-<job>
    r = gh("release", "view", TAG, "--json", "id", check=False)
    if r.returncode != 0:
        gh("release", "create", TAG, "--prerelease", "--title", f"Encoder job {TAG}",
           "--notes", "encoder parts")
    uploads = list(parts)
    if audio_name:
        uploads.append(audio_name)
    for p in uploads:
        gh("release", "upload", TAG, p, "--clobber")
        print(f"  ✓ uploaded {p}", flush=True)

    marker = {"ok": True, "bounds": bounds, "parts": len(parts), "audio": audio_name,
              "duration": dur, "total_size": TOTAL}
    with open("asm_done.json", "w") as f:
        json.dump(marker, f, separators=(",", ":"))
    gh("release", "upload", TAG, "asm_done.json", "--clobber")   # marker අවසන් — main මේකෙන් දැනගන්නවා
    print(f"assembly done: {len(parts)} parts → {TAG}", flush=True)


if __name__ == "__main__":
    try:
        main()
    except BaseException as e:
        print(f"ASM FAILED: {e}", file=sys.stderr, flush=True)
        try:
            with open("asm_done.json", "w") as f:
                json.dump({"ok": False, "error": str(e)[-300:]}, f)
            r = gh("release", "view", TAG, "--json", "id", check=False)
            if r.returncode != 0:
                gh("release", "create", TAG, "--prerelease", "--title", f"Encoder job {TAG}",
                   "--notes", "encoder parts", check=False)
            gh("release", "upload", TAG, "asm_done.json", "--clobber", check=False)
        except Exception:
            pass
        sys.exit(1)
