"""Every distributed part must share one output frame size.

A part whose aspect ratio differs from the main feature (promo clips,
letterboxed intros) used to be scaled to a different width, so the merged
video changed frame size mid-playback.
"""
import subprocess
import tempfile
import types
import unittest
from pathlib import Path

import dist
from ffcheck import FFMPEG, ff, stream_info
from transcoder import build_cmd


def frame_sizes(path):
    """Decoded frame byte size (constant only when the geometry is constant)."""
    r = ff("-i", path, "-map", "0:v:0", "-f", "framecrc", "-")
    sizes = []
    for line in r.stdout.decode().splitlines():
        if not line or line.startswith("#"):
            continue
        cols = [c.strip() for c in line.split(",")]
        if len(cols) > 4:
            sizes.append(cols[4])
    return sizes


def decoded_frame_at(path, timestamp):
    """(width, height) of the frame decoded at `timestamp`."""
    raw = subprocess.run([FFMPEG, "-v", "error", "-ss", str(timestamp), "-i", str(path),
                          "-frames:v", "1", "-f", "rawvideo", "-pix_fmt", "gray", "-"],
                         capture_output=True).stdout
    for w, h in ((854, 480), (640, 640), (1920, 1080), (640, 480), (480, 854)):
        if len(raw) == w * h:
            return w, h
    return None, len(raw)


@unittest.skipUnless(FFMPEG, "ffmpeg is required")
class FixedFrameTests(unittest.IsolatedAsyncioTestCase):
    async def test_parts_with_different_aspect_share_frame_size(self):
        with tempfile.TemporaryDirectory() as d:
            work = Path(d)
            ff("-f", "lavfi", "-i", "testsrc2=size=640x640:rate=25:duration=1",
               "-c:v", "libx264", "-pix_fmt", "yuv420p", "-g", "25", work / "square.mp4")
            ff("-f", "lavfi", "-i", "testsrc2=size=1920x1080:rate=25:duration=3",
               "-c:v", "libx264", "-pix_fmt", "yuv420p", "-g", "25", work / "wide.mp4")
            (work / "list.txt").write_text(
                f"file '{work / 'square.mp4'}'\nfile '{work / 'wide.mp4'}'\n")
            ff("-f", "concat", "-safe", "0", "-i", work / "list.txt", "-c", "copy", work / "src.mp4")
            ff("-i", work / "src.mp4", "-map", "0:v:0", "-c", "copy", "-an", "-f", "segment",
               "-segment_times", "1,2", "-reset_timestamps", "1", "-segment_format", "mp4",
               work / "raw_part_%d.mp4")

            parts = []
            for i, raw in enumerate(sorted(work.glob("raw_part_*.mp4"))):
                out = work / f"enc_{i}.mkv"
                cmd = build_cmd(str(raw), str(out), size=(854, 480), preset="ultrafast",
                                codec="libx265", crf=24, audio_mode="none", container="mkv")
                subprocess.run(cmd[:-1] + [str(out)], check=True, capture_output=True)
                info = stream_info(out)
                self.assertEqual((info["width"], info["height"]), (854, 480))
                parts.append(dist.Part(i, i, 1, path=str(out)))

            state = dist.DistState(3, 30, 3)
            state.parts = parts
            job = types.SimpleNamespace(proc=None, cancelled=False, id="test")
            final = work / "final.mp4"
            run = dist.DistRun(client=None, job=job, src="", dst=final, meta=None,
                               state=state, opts={"codec": "libx265"}, workdir=work,
                               pool=None, n_workers=3)
            await run._merge(None)

            info = stream_info(final)
            self.assertEqual((info["width"], info["height"]), (854, 480))
            # Same number of bytes per frame => the frame size never changes.
            self.assertEqual(len(set(frame_sizes(final))), 1)
            self.assertEqual(decoded_frame_at(final, 0.2)[0], 854)
            self.assertEqual(decoded_frame_at(final, 3.4)[0], 854)


if __name__ == "__main__":
    unittest.main()
