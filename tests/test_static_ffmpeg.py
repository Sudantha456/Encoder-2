"""Regression test for the static-FFmpeg MPEG-TS demuxer crash.

John Van Sickle / BtbN static Linux builds link glibc statically, and their
MPEG-TS demuxer asks glibc to convert the ISO-6937 service name. glibc then
dlopen()s the *host* gconv module, which SIGSEGVs the process (exit -11, no
stderr) on any .ts input. ``ffenv.ffmpeg_env`` blocks that load, so the merge
stages must hand it to every ffmpeg subprocess.
"""
from pathlib import Path
import shutil
import subprocess
import tempfile
import types
import unittest

import dist
from ffenv import ffmpeg_env
from transcoder import verify_output

FFMPEG = shutil.which("ffmpeg")
ENV = ffmpeg_env()


def ff(*args):
    return subprocess.run([FFMPEG, "-hide_banner", "-loglevel", "error", "-y", *map(str, args)],
                          capture_output=True, env=ENV)


def frames(path):
    out = ff("-i", path, "-map", "0:v:0", "-f", "framemd5", "-").stdout
    return [line.split(b",")[-1].strip() for line in out.splitlines() if line and not line.startswith(b"#")]


@unittest.skipUnless(FFMPEG, "ffmpeg is required")
class StaticTsDemuxerTests(unittest.IsolatedAsyncioTestCase):
    async def test_merge_reads_transport_streams(self):
        with tempfile.TemporaryDirectory() as d:
            work = Path(d)
            parts, expected = [], []
            for i in range(4):
                part = work / f"part{i}.mkv"
                ff("-f", "lavfi", "-i", "testsrc2=size=96x64:rate=10:duration=1",
                   "-c:v", "libx265", "-preset", "ultrafast" if i % 2 else "medium",
                   "-pix_fmt", "yuv420p", "-x265-params", f"log-level=error:bframes={i % 2}", part)
                expected += frames(part)
                parts.append(dist.Part(i, i, 1, path=str(part)))
            state = dist.DistState(4, 10, 4)
            state.parts = parts
            out = work / "merged.mp4"
            job = types.SimpleNamespace(proc=None, cancelled=False, id="test")
            run = dist.DistRun(client=None, job=job, src="", dst=out, meta=None,
                               state=state, opts={"codec": "libx265"}, workdir=work,
                               pool=None, n_workers=4)
            await run._merge(None)
            await verify_output(str(out), job)
            self.assertEqual(frames(out), expected)
            # Transport streams are the input the demuxer used to crash on.
            await run._ff("-i", str(work / "merged.mp4"), "-map", "0:v:0", "-c", "copy",
                          "-f", "mpegts", str(work / "probe.ts"), stage="ts probe")
            self.assertGreater((work / "probe.ts").stat().st_size, 0)


if __name__ == "__main__":
    unittest.main()
