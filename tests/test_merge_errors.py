"""Regression tests for distributed merge failure diagnostics and cleanup."""
import os
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

import dist


def make_run(work, parts=()):
    state = dist.DistState(2, 24, len(parts))
    state.parts = list(parts)
    job = types.SimpleNamespace(proc=None, cancelled=False, id="test")
    return dist.DistRun(client=None, job=job, src="", dst=work / "out.mp4", meta=None,
                        state=state, opts={"codec": "libx265"}, workdir=work,
                        pool=None, n_workers=len(parts))


class MergeErrorTests(unittest.IsolatedAsyncioTestCase):
    async def test_empty_stderr_reports_signal_and_stage(self):
        with tempfile.TemporaryDirectory() as d:
            work = Path(d)
            ff = work / "ffmpeg"
            ff.write_text("#!/bin/sh\nkill -9 $$\n")
            ff.chmod(0o755)
            run = make_run(work)
            with patch.dict(os.environ, {"PATH": str(work)}):
                with self.assertRaisesRegex(dist.DistError, r"final merge failed .*SIGKILL.*no stderr output"):
                    await run._ff("ignored", on_progress=lambda *args: None, stage="final merge")
            self.assertIsNone(run.job.proc)

    async def test_stderr_and_exit_code_are_reported(self):
        with tempfile.TemporaryDirectory() as d:
            work = Path(d)
            ff = work / "ffmpeg"
            ff.write_text("#!/bin/sh\necho 'No space left on device' >&2\nexit 1\n")
            ff.chmod(0o755)
            with patch.dict(os.environ, {"PATH": str(work)}):
                with self.assertRaisesRegex(dist.DistError, r"exit code 1.*No space left on device"):
                    await make_run(work)._ff("ignored", stage="prepare part 0")

    async def test_partial_segments_removed_on_failure(self):
        with tempfile.TemporaryDirectory() as d:
            work = Path(d)
            run = make_run(work, [dist.Part(0, 0, 1, path="encoded.mkv")])

            async def fail_prepare(*args, **kwargs):
                (work / "merge_part_0.ts").write_bytes(b"partial")
                raise dist.DistError("prepare failed")

            run._ff = fail_prepare
            with self.assertRaises(dist.DistError):
                await run._merge(None)
            self.assertFalse((work / "merge_part_0.ts").exists())

    async def test_final_merge_failure_cleans_up(self):
        with tempfile.TemporaryDirectory() as d:
            work = Path(d)
            run = make_run(work, [dist.Part(0, 0, 1, path="encoded.mkv")])

            async def fail_merge(*args, **kwargs):
                if kwargs["stage"] == "final merge":
                    raise dist.DistError("merge failed")
                (work / "merge_part_0.ts").write_bytes(b"data")

            run._ff = fail_merge
            with self.assertRaises(dist.DistError):
                await run._merge(None)
            self.assertFalse((work / "merge_part_0.ts").exists())
            self.assertFalse((work / "concat.txt").exists())
