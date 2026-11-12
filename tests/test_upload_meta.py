"""Telegram video attributes must carry the real output geometry."""
import asyncio
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import fast_uploader
from fast_uploader import FastUploader, _probe_with_ffmpeg
from ffcheck import FFMPEG, ff, probe, stream_info


class UploadMetaTests(unittest.IsolatedAsyncioTestCase):
    @unittest.skipUnless(FFMPEG, "ffmpeg is required")
    async def test_falls_back_to_ffmpeg_without_ffprobe(self):
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "out.mp4"
            ff("-f", "lavfi", "-i", "testsrc2=size=854x480:rate=10:duration=1",
               "-c:v", "libx265", "-preset", "ultrafast", "-pix_fmt", "yuv420p",
               "-x265-params", "log-level=error", out)
            up = FastUploader.__new__(FastUploader)
            with patch.object(FastUploader, "_probe", return_value={}):
                info = await up.get_video_info(str(out))
            self.assertEqual((info["width"], info["height"]), (854, 480))

    @unittest.skipUnless(FFMPEG, "ffmpeg is required")
    async def test_hint_used_when_probe_fails(self):
        up = FastUploader.__new__(FastUploader)
        with patch.object(FastUploader, "_probe", return_value={}), \
             patch("fast_uploader._probe_with_ffmpeg", return_value={}):
            info = await up.get_video_info("missing.mp4", hint={"width": 854, "height": 480,
                                                               "duration": 90})
        self.assertEqual((info["width"], info["height"], info["duration"]), (854, 480, 90))

    def test_anamorphic_probe_reports_display_width(self):
        with tempfile.TemporaryDirectory() as d:
            src = Path(d) / "a.mp4"
            ff("-f", "lavfi", "-i", "testsrc2=size=720x576:rate=10:duration=1",
               "-vf", "setsar=16/15", "-c:v", "libx264", "-pix_fmt", "yuv420p", src)
            stream = _probe_with_ffmpeg(str(src))
            self.assertEqual(stream.get("sample_aspect_ratio"), "16:15")
            self.assertEqual((stream["width"], stream["height"]), (720, 576))


if __name__ == "__main__":
    unittest.main()
