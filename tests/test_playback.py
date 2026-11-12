"""Playback regression tests. Run with: python -m unittest discover -s tests."""
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import types
import unittest

from transcoder import verify_output

FFMPEG = shutil.which("ffmpeg")


def ff(*args):
    return subprocess.run([FFMPEG, "-hide_banner", "-loglevel", "error", "-y", *map(str, args)],
                          check=True, capture_output=True)


def count_frames(path):
    out = ff("-i", path, "-map", "0:v:0", "-f", "framemd5", "-").stdout
    return sum(1 for line in out.splitlines() if line and not line.startswith(b"#"))


def parameter_sets(path, codec):
    name = "hevc" if codec == "libx265" else "h264"
    raw = ff("-i", path, "-map", "0:v:0", "-c:v", "copy", "-bsf:v",
             f"{name}_mp4toannexb", "-f", name, "-").stdout
    chunks = re.split(b"\\x00\\x00\\x00?\\x01", raw)
    if codec == "libx265":
        return {c for c in chunks if c and (c[0] >> 1) & 63 in (32, 33, 34)}
    return {c for c in chunks if c and c[0] & 31 in (7, 8)}


@unittest.skipUnless(FFMPEG, "ffmpeg is required")
class PlaybackTests(unittest.IsolatedAsyncioTestCase):
    async def test_distributed_merge_preserves_frames_and_headers(self):
        # Differing encoder settings deliberately create distinct parameter sets.
        import dist
        with tempfile.TemporaryDirectory(prefix="merge's test ") as d:
            work = Path(d)
            for codec in ("libx264", "libx265"):
                parts = []
                for i, preset in enumerate(("ultrafast", "medium")):
                    path = work / f"{codec}_{i}.mkv"
                    args = ["-f", "lavfi", "-i", f"testsrc2=size=96x64:rate=12:duration=1",
                            "-c:v", codec, "-preset", preset, "-pix_fmt", "yuv420p"]
                    if codec == "libx265":
                        args += ["-x265-params", "log-level=error:pools=1:frame-threads=1"]
                    ff(*args, path)
                    parts.append(dist.Part(i, i, 1, path=str(path)))
                state = dist.DistState(2, 12, 2)
                state.parts = parts
                out = work / f"{codec}.mp4"
                job = types.SimpleNamespace(proc=None, cancelled=False, id="test")
                run = dist.DistRun(client=None, job=job, src="", dst=out, meta=None,
                                   state=state, opts={"codec": codec}, workdir=work,
                                   pool=None, n_workers=2)
                # Exercise the separate audio mux path for HEVC, and the
                # no-audio path for AVC.
                audio_task = None
                if codec == "libx265":
                    audio = work / "audio.mka"
                    ff("-f", "lavfi", "-i", "sine=frequency=440:duration=2",
                       "-c:a", "aac", audio)
                    async def audio_ready():
                        return str(audio)
                    audio_task = audio_ready()
                await run._merge(audio_task)
                await verify_output(str(out), job)
                if audio_task:
                    self.assertTrue(ff("-i", out, "-map", "0:a:0", "-f", "s16le", "-").stdout)
                self.assertEqual(count_frames(out), sum(count_frames(p.path) for p in parts))
                for part in parts:
                    headers = parameter_sets(part.path, codec)
                    self.assertTrue(headers)
                    self.assertTrue(headers <= parameter_sets(out, codec))
                if codec == "libx265":
                    self.assertIn(b"hev1", out.read_bytes())

    async def test_validation_rejects_corruption_without_size_change(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "broken.mp4"
            ff("-f", "lavfi", "-i", "testsrc2=size=128x96:rate=20:duration=3",
               "-c:v", "libx264", "-movflags", "+faststart", path)
            original = path.read_bytes()
            pos = original.index(b"mdat") + 8
            data = bytearray(original)
            start = pos + (len(data) - pos) // 3
            end = pos + (len(data) - pos) * 2 // 3
            data[start:end] = b"\0" * (end - start)
            path.write_bytes(data)
            self.assertEqual(len(data), len(original))
            job = types.SimpleNamespace(proc=None, cancelled=False)
            with self.assertRaisesRegex(RuntimeError, "playback verification"):
                await verify_output(str(path), job)

    async def test_validation_rejects_invalid_file(self):
        with tempfile.TemporaryDirectory() as d:
            bad = Path(d) / "bad.mp4"
            bad.write_bytes(b"not a video")
            job = types.SimpleNamespace(proc=None, cancelled=False)
            with self.assertRaisesRegex(RuntimeError, "playback verification"):
                await verify_output(str(bad), job)
            self.assertIsNone(job.proc)


if __name__ == "__main__":
    unittest.main()
