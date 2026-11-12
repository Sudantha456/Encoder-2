"""Output frame geometry must match the source aspect ratio exactly."""
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from ffcheck import FFMPEG, ff, stream_info
from transcoder import build_cmd


def sar_of(path):
    s = stream_info(path)
    return s.get("sample_aspect_ratio"), s.get("display_aspect_ratio"), s.get("width"), s.get("height")


@unittest.skipUnless(FFMPEG, "ffmpeg is required")
class AspectTests(unittest.TestCase):
    def test_scaled_output_has_square_pixels(self):
        with tempfile.TemporaryDirectory() as d:
            work = Path(d)
            src = work / "src.mp4"
            ff("-f", "lavfi", "-i", "testsrc2=size=1920x1080:rate=10:duration=1",
               "-c:v", "libx264", "-pix_fmt", "yuv420p", src)
            out = work / "out.mp4"
            cmd = build_cmd(str(src), str(out), target=480, scale=True, portrait=False,
                            preset="ultrafast", codec="libx265", crf=24)
            subprocess.run(cmd[:1] + cmd[1:-1] + [str(out)], check=True,
                           capture_output=True, env=None)
            sar, dar, w, h = sar_of(out)
            self.assertEqual((w, h), (854, 480))
            self.assertEqual(sar, "1:1")
            self.assertEqual(dar, "427:240")       # 16:9 within even-pixel rounding

    def test_anamorphic_source_is_squared(self):
        with tempfile.TemporaryDirectory() as d:
            work = Path(d)
            src = work / "src.mp4"
            ff("-f", "lavfi", "-i", "testsrc2=size=720x576:rate=10:duration=1",
               "-vf", "setsar=16/15", "-c:v", "libx264", "-pix_fmt", "yuv420p", src)
            out = work / "out.mp4"
            cmd = build_cmd(str(src), str(out), target=480, scale=True, portrait=False,
                            preset="ultrafast", codec="libx265", crf=24)
            subprocess.run(cmd[:1] + cmd[1:-1] + [str(out)], check=True, capture_output=True)
            sar, dar, w, h = sar_of(out)
            self.assertEqual(sar, "1:1")
            # 720x576 at SAR 16:15 displays as 768x576 (4:3) -> 640x480.
            self.assertEqual((w, h), (640, 480))

    def test_odd_source_keeps_display_ratio(self):
        with tempfile.TemporaryDirectory() as d:
            work = Path(d)
            src = work / "src.mp4"
            ff("-f", "lavfi", "-i", "testsrc2=size=1280x718:rate=10:duration=1",
               "-c:v", "libx264", "-pix_fmt", "yuv420p", src)
            out = work / "out.mp4"
            cmd = build_cmd(str(src), str(out), target=360, scale=True, portrait=False,
                            preset="ultrafast", codec="libx265", crf=24)
            subprocess.run(cmd[:1] + cmd[1:-1] + [str(out)], check=True, capture_output=True)
            sar, dar, w, h = sar_of(out)
            self.assertEqual(sar, "1:1")
            self.assertEqual(w % 2, 0)
            self.assertEqual(h % 2, 0)


if __name__ == "__main__":
    unittest.main()
