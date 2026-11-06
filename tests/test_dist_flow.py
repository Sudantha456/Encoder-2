"""
tests/test_dist_flow.py — Distributed download / assembly pipeline tests
(stdlib unittest + ffmpeg/ffprobe on PATH). Run: python3 -m unittest discover -s tests -v
"""
import asyncio
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dlparts import byte_ranges, verify_ranges, range_requests       # noqa: E402

CHUNK = 1024 * 1024
ASSET = 1900 * 1024 * 1024


class TestRanges(unittest.TestCase):
    def test_exact_coverage(self):
        sizes = [0, 1, 999, CHUNK - 1, CHUNK, CHUNK + 1, 5 * CHUNK + 7, 50 * CHUNK + 3,
                 12 * CHUNK + 5, 11 * CHUNK + 13, 250 * CHUNK + 9]
        for size in sizes:
            for n in range(1, 9):
                for align in (1, 4096, CHUNK):
                    ranges = byte_ranges(size, n, align=align)
                    self.assertTrue(verify_ranges(ranges, size),
                                    f"size={size} n={n} align={align} ranges={ranges}")

    def test_chunk_limits_never_cross_range(self):
        # dl_worker / download_lanes ග්‍රහනය අනුකරණය: request limit කිසිම වෙලාවක range එකෙන් පිටතට යන්නේ නෑ
        for size in (1, 5 * CHUNK + 123, 12 * CHUNK + 1, 50 * CHUNK):
            ranges = byte_ranges(size, 5, align=CHUNK)
            self.assertTrue(verify_ranges(ranges, size))
            for start, end in ranges:
                got = 0
                for off in range(start, end, CHUNK):
                    lim = min(CHUNK, end - off)
                    self.assertLessEqual(off + lim, end)
                    got += lim
                self.assertEqual(got, end - start)
        total = sum(e - s for s, e in byte_ranges(12 * CHUNK + 1, 5, align=CHUNK))
        self.assertEqual(total, 12 * CHUNK + 1)

    def test_zero_length_lanes_possible_but_handled(self):
        # file කුඩා අවස්ථාවලදී zero-block lanes ඇතිවෙන්න පුළුවන් — orchestrator ඒවා drop කරනවා
        ranges = byte_ranges(2 * CHUNK, 5, align=CHUNK)
        self.assertTrue(verify_ranges(ranges, 2 * CHUNK))
        self.assertTrue(any(e <= s for s, e in ranges))


class TestParseProgDl(unittest.TestCase):
    def test_parse(self):
        import dist
        d = dist.parse_prog_dl("blah\nPROGD|job1|2|running|1234|5678|99.5\n", "job1", 2)
        self.assertEqual(d, dict(state="running", done=1234, total=5678, speed=99.5))
        self.assertIsNone(dist.parse_prog_dl("PROGD|job1|3|running|1|2|3.0", "job1", 2))
        self.assertIsNone(dist.parse_prog_dl("PROGD|other|2|running|1|2|3.0", "job1", 2))


@unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "ffmpeg/ffprobe not on PATH")
class TestAssemblyE2E(unittest.TestCase):
    """ගැබ් ගැනුම් ප්‍රවාහය මුලුමනින්ම දේශීයව: slices → fake releases → asm_worker → parts → concat → play."""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="dist_e2e_"))
        self.work = self.tmp / "work"
        self.work.mkdir()
        self.fake = self.tmp / "releases"
        self.fake.mkdir()
        self.fake_bin = self.tmp / "bin"
        self.fake_bin.mkdir()
        gh_link = self.fake_bin / "gh"
        gh_link.symlink_to(ROOT / "tests" / "fake_gh.py")
        gh_link.chmod(0o755)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _run(self, cmd, **kw):
        return subprocess.run(cmd, cwd=self.work, capture_output=True, text=True, **kw)

    def test_full_pipeline(self):
        src = self.work / "sample.mp4"
        r = self._run(["ffmpeg", "-hide_banner", "-v", "error", "-y",
                       "-f", "lavfi", "-i", "testsrc2=size=640x360:rate=24:duration=10",
                       "-f", "lavfi", "-i", "sine=frequency=440:duration=10",
                       "-c:v", "libx264", "-preset", "ultrafast", "-b:v", "3500k", "-maxrate", "3500k",
                       "-bufsize", "7M", "-g", "24", "-keyint_min", "24", "-sc_threshold", "0",
                       "-c:a", "aac", "-shortest", str(src)])
        self.assertEqual(r.returncode, 0, r.stderr)
        size = src.stat().st_size
        self.assertGreaterEqual(size, 4 * CHUNK)
        print(f"\nsample: {size} bytes ({size / CHUNK:.1f} MB)")

        # 1) main runner එක analyze කරලා byte-exact ranges බෙදාගන්නා ආකාරය
        ranges = byte_ranges(size, 5, align=CHUNK)
        self.assertTrue(verify_ranges(ranges, size))
        original_sha = hashlib.sha256(src.read_bytes()).hexdigest()

        # lane 0 = main, lanes 1.. = helpers; assembler = helper #1 ("acct1/encoder-workers")
        repos = {0: "acct1/encoder-workers", 1: "acct1/encoder-workers",
                 2: "acct2/encoder-workers", 3: "acct3/encoder-workers", 4: "acct4/encoder-workers"}
        dl_tag = "enc-dl-testjob"
        for i, (s, e) in enumerate(ranges):
            repo_dir = self.fake / repos[i] / dl_tag
            repo_dir.mkdir(parents=True, exist_ok=True)
            (repo_dir / f"dl_slice_{i}").write_bytes(src.read_bytes()[s:e])

        # 2) assembler (asm_worker.py) run කරනවා — fake gh + actual ffmpeg
        asm_work = self.work / "asm"
        asm_work.mkdir()
        env = dict(os.environ)
        env.update({
            "PATH": f"{self.fake_bin}:{os.environ['PATH']}",
            "FAKE_REL": str(self.fake),
            "GH_REPO": "acct1/encoder-workers",
            "GH_TOKEN": "fake",
            "JOB": "testjob",
            "SOURCES": json.dumps([{"repo": repos[i], "tag": dl_tag,
                                    "name": f"dl_slice_{i}", "size": e - s}
                                   for i, (s, e) in enumerate(ranges)]),
            "TOTAL_SIZE": str(size),
            "DURATION": "10",
            "SPLITS": "4",
            "AUDIO_MODE": "aac",
            "ABITRATE": "96",
            "BOT_TOKEN": "",
        })
        r = subprocess.run([sys.executable, str(ROOT / "asm_worker.py")], cwd=asm_work,
                           env=env, capture_output=True, text=True, timeout=300)
        print(r.stdout[-1500:])
        if r.returncode != 0:
            print("STDERR:", r.stderr[-2000:])
        self.assertEqual(r.returncode, 0)

        # 3) asm_done.json marker + parts release එකට upload වෙලාද
        enc_dir = self.fake / "acct1" / "encoder-workers" / "enc-testjob"
        marker = json.loads((enc_dir / "asm_done.json").read_text())
        self.assertTrue(marker["ok"])
        bounds = marker["bounds"]
        parts = marker["parts"]
        self.assertEqual(len(bounds), parts + 1)
        self.assertEqual(bounds[0], 0)
        self.assertTrue(all(b2 >= b1 for b1, b2 in zip(bounds, bounds[1:])))
        raw_parts = sorted(enc_dir.glob("raw_part_*.mp4"))
        self.assertEqual(len(raw_parts), parts)
        self.assertTrue((enc_dir / "audio.mka").exists())

        # 4) හැම raw part එකක්ම decodable ද (workers independent encode)
        for p in raw_parts:
            rr = self._run(["ffmpeg", "-v", "error", "-i", str(p), "-f", "null", "-"])
            self.assertEqual(rr.returncode, 0, f"{p}: {rr.stderr}")

        # 5) concat + audio mux → final output — playable & duration ≈ source
        lst = self.work / "concat.txt"
        lst.write_text("".join(f"file '{p.resolve()}'\n" for p in raw_parts))
        final = self.work / "final.mp4"
        rr = self._run(["ffmpeg", "-hide_banner", "-v", "error", "-y",
                        "-f", "concat", "-safe", "0", "-i", str(lst),
                        "-i", str(enc_dir / "audio.mka"),
                        "-map", "0:v:0", "-map", "1:a", "-c", "copy",
                        "-movflags", "+faststart", str(final)])
        self.assertEqual(rr.returncode, 0, rr.stderr)
        out = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration",
                              "-of", "csv=p=0", str(final)], capture_output=True, text=True).stdout.strip()
        print(f"final duration: {out}s")
        self.assertAlmostEqual(float(out), 10.0, delta=0.6)

    def test_assembly_rejects_corrupt_slice(self):
        src = self.work / "sample.mp4"
        r = self._run(["ffmpeg", "-hide_banner", "-v", "error", "-y",
                       "-f", "lavfi", "-i", "testsrc2=size=320x240:rate=24:duration=10",
                       "-c:v", "libx264", "-preset", "ultrafast", "-b:v", "2500k", "-maxrate", "2500k",
                       "-bufsize", "5M", "-g", "24", str(src)])
        self.assertEqual(r.returncode, 0, r.stderr)
        size = src.stat().st_size
        ranges = byte_ranges(size, 2, align=CHUNK)
        self.assertTrue(all(e > s for s, e in ranges), f"empty lane in ranges {ranges}; grow sample")
        dl_tag = "enc-dl-badjob"
        repos = {0: "acct1/encoder-workers", 1: "acct1/encoder-workers"}
        for i, (s, e) in enumerate(ranges):
            d = self.fake / repos[i] / dl_tag
            d.mkdir(parents=True, exist_ok=True)
            data = bytearray(src.read_bytes()[s:e])
            if i == 1:
                data[0] ^= 0xFF          # byte එකක් වත් වෙනස් වුණොත්…
                data = data[:-3]          # size mismatch — verify අල්ලගන්න ඕනේ
            (d / f"dl_slice_{i}").write_bytes(bytes(data))
        asm_work = self.work / "asm2"
        asm_work.mkdir()
        env = dict(os.environ)
        env.update({"PATH": f"{self.fake_bin}:{os.environ['PATH']}", "FAKE_REL": str(self.fake),
                    "GH_REPO": "acct1/encoder-workers", "GH_TOKEN": "fake", "JOB": "badjob",
                    "SOURCES": json.dumps([{"repo": repos[i], "tag": dl_tag, "name": f"dl_slice_{i}",
                                            "size": e - s} for i, (s, e) in enumerate(ranges)]),
                    "TOTAL_SIZE": str(size), "DURATION": "10", "SPLITS": "2",
                    "AUDIO_MODE": "none", "ABITRATE": "96", "BOT_TOKEN": ""})
        r = subprocess.run([sys.executable, str(ROOT / "asm_worker.py")], cwd=asm_work,
                           env=env, capture_output=True, text=True, timeout=120)
        self.assertNotEqual(r.returncode, 0)
        marker = json.loads((self.fake / "acct1" / "encoder-workers" / "enc-badjob" / "asm_done.json").read_text())
        self.assertFalse(marker["ok"])


if __name__ == "__main__":
    unittest.main(verbosity=2)

class _FakePeer:
    def __init__(self, kind, pid):
        self._kind, self._pid = kind, pid

    @property
    def channel_id(self):
        return self._pid if self._kind == "channel" else None

    @property
    def chat_id(self):
        return self._pid if self._kind == "chat" else None

    @property
    def user_id(self):
        return self._pid if self._kind == "user" else None


class _FakeDialog:
    def __init__(self, peer, entity):
        self.peer, self.entity = peer, entity


class _FakeClient:
    def __init__(self, dialogs):
        self._dialogs = dialogs

    async def iter_dialogs(self, limit=None):
        for d in self._dialogs:
            yield d


class TestResolveChannel(unittest.TestCase):
    def setUp(self):
        import dl_worker
        self.dl = dl_worker
        self._old_ref, self._old_num = dl_worker.CHANNEL_REF, dl_worker.CHANNEL_NUM

    def tearDown(self):
        self.dl.CHANNEL_REF, self.dl.CHANNEL_NUM = self._old_ref, self._old_num

    def test_fallback_scan_finds_channel(self):
        self.dl.CHANNEL_REF, self.dl.CHANNEL_NUM = "", 4340204571
        chan = object()
        client = _FakeClient([_FakeDialog(_FakePeer("user", 111), "user-x"),
                              _FakeDialog(_FakePeer("channel", 999000111), "other-chan"),
                              _FakeDialog(_FakePeer("channel", 4340204571), chan)])
        got = asyncio.run(self.dl.resolve_channel(client))
        self.assertIs(got, chan)

    def test_fallback_scan_missing_raises_diagnostic(self):
        self.dl.CHANNEL_REF, self.dl.CHANNEL_NUM = "", 4340204571
        client = _FakeClient([_FakeDialog(_FakePeer("user", 111), "user-x"),
                              _FakeDialog(_FakePeer("channel", 123), "other")])
        with self.assertRaises(RuntimeError) as cm:
            asyncio.run(self.dl.resolve_channel(client))
        self.assertIn("4340204571", str(cm.exception))
        self.assertIn("channels seen", str(cm.exception))


class TestPreflight(unittest.TestCase):
    def test_preflight_bad_channel_ref(self):
        import dist
        import config
        old = config.CHANNEL_ID
        config.CHANNEL_ID = "%%%garbage%%%"
        try:
            err = asyncio.run(dist._preflight_dl("fake-session", "Helper #1"))
            try:
                import telethon  # noqa: F401
                self.assertTrue(err and "CHANNEL_ID parse" in err)
            except ImportError:
                self.assertIsNone(err)   # telethon නැති local env එකේ pre-flight skip වෙනවා (runner එකේ telethon තියෙනවා)
        finally:
            config.CHANNEL_ID = old

    def test_preflight_empty_inputs(self):
        import dist
        import config
        old = config.CHANNEL_ID
        config.CHANNEL_ID = ""
        try:
            self.assertIsNone(asyncio.run(dist._preflight_dl("fake-session", "Helper #1")))
            self.assertIsNone(asyncio.run(dist._preflight_dl("", "Helper #1")))
        finally:
            config.CHANNEL_ID = old

class TestSessionPlan(unittest.TestCase):
    def _plan(self, **kw):
        from sessionplan import plan_main_sessions
        return plan_main_sessions(**kw)

    def test_all_dl_ready_split(self):
        # 5 sessions, 4 healthy helpers holding s2..s5 → main = [s1]
        got = self._plan(sessions=["s1","s2","s3","s4","s5"], dist_download=True,
                         helper_tokens=["t1","t2","t3","t4"],
                         helper_dl=[True, True, True, True],
                         helper_sess=["s2","s3","s4","s5"], helpers_setup_done=True)
        self.assertEqual(got, ["s1"])

    def test_failed_bootstrap_session_comes_home(self):
        # token #3 login 403 → bootstrap fail → s4 free → main = [s1, s4] + extras
        got = self._plan(sessions=["s1","s2","s3","s4","s5"], dist_download=True,
                         helper_tokens=["t1","t2","t3","t4"],
                         helper_dl=[True, True, True],          # successful bootstraps only
                         helper_sess=["s2","s3","s5"], helpers_setup_done=True)
        self.assertEqual(got, ["s1","s4"])                        # s5 dl-ok helper ගන්නවා; s4 (t3 fail) main ට එනවා

    def test_dl_dead_helpers_sessions_return_to_main(self):
        # pre-flight fail ඔක්කොම → කිසිම helper session claim කරේ නෑ → main = 5 sessionsම
        got = self._plan(sessions=["s1","s2","s3","s4","s5"], dist_download=True,
                         helper_tokens=["t1","t2","t3","t4"],
                         helper_dl=[False, False, False],
                         helper_sess=["s2","s3","s5"], helpers_setup_done=True)
        self.assertEqual(sorted(got), ["s1","s2","s3","s4","s5"])
        self.assertEqual(got[0], "s1")

    def test_partial_dl_dead(self):
        # helper 4 dl dead → s5 main ට; helpers 1,2 → s2,s3
        got = self._plan(sessions=["s1","s2","s3","s4","s5"], dist_download=True,
                         helper_tokens=["t1","t2","t3","t4"],
                         helper_dl=[True, True, False],
                         helper_sess=["s2","s3","s5"], helpers_setup_done=True)
        self.assertEqual(got, ["s1","s5","s4"])

    def test_before_bootstrap_safe_default(self):
        got = self._plan(sessions=["s1","s2","s3","s4","s5"], dist_download=True,
                         helper_tokens=["t1"], helper_dl=[], helper_sess=[],
                         helpers_setup_done=False)
        self.assertEqual(got, ["s1"])

    def test_dist_off_or_no_tokens_all_on_main(self):
        got = self._plan(sessions=["s1","s2","s3","s4","s5"], dist_download=False,
                         helper_tokens=["t1"], helper_dl=[], helper_sess=[],
                         helpers_setup_done=True)
        self.assertEqual(got, ["s1","s2","s3","s4","s5"])
        got = self._plan(sessions=["s1","s2","s3","s4","s5"], dist_download=True,
                         helper_tokens=[], helper_dl=[], helper_sess=[],
                         helpers_setup_done=True)
        self.assertEqual(got, ["s1","s2","s3","s4","s5"])

    def test_no_duplicate_sessions(self):
        got = self._plan(sessions=["s1","s1","s2","s3"], dist_download=True,
                         helper_tokens=["t1","t2"], helper_dl=[True, False],
                         helper_sess=["s1x-unused","s2"], helpers_setup_done=True)
        self.assertEqual(len(got), len(set(got)))


class TestRangeRequests(unittest.TestCase):
    def test_all_requests_satisfy_telegram_rules(self):
        # Telegram getFile: offset%4096==0, limit%4096==0, 1048576%limit==0, same 1MB fragment
        for start, size, total in [(0, 1, 1), (0, 2138, 2138), (0, 1048576, 1048576),
                                   (0, 1048576 * 3 + 7, 1048576 * 3 + 7),
                                   (547356672, 135792708, 683149380),
                                   (0, 683149380, 683149380),
                                   (131072, 7 * 1048576 + 123, 9 * 1048576 + 123)]:
            reqs = list(range_requests(start, start + size, total, CHUNK))
            got = set()
            for off, req_lim, want in reqs:
                self.assertEqual(off % 4096, 0, f"offset not aligned: {hex(off)}")
                self.assertEqual(req_lim % 4096, 0)
                self.assertEqual(1048576 % req_lim, 0, f"limit {req_lim} not a 1MB divisor")
                self.assertEqual(off // 1048576, (off + req_lim - 1) // 1048576,
                                 f"crosses 1MB fragment: {hex(off)}+{req_lim}")
                self.assertLessEqual(want, req_lim)
                got.add((off, off + want))
            self.assertEqual(sum(w for *_, w in reqs), size)
            # no overlaps, no gaps, exact cover of [start, start+size)
            pos = start
            for off, end in sorted(got):
                self.assertEqual(off, pos)
                pos = end
            self.assertEqual(pos, start + size)

    def test_eof_tail_decomposes_correctly(self):
        start, sln, total = 547356672, 135792708, 683149380
        reqs = list(range_requests(start, start + sln, total, CHUNK))
        self.assertEqual(sum(w for *_, w in reqs), sln)
        self.assertEqual(reqs[-1], (683147264, 4096, 2116))
        self.assertEqual(reqs[-2], (682622976, 524288, 524288))

    def test_interior_chunks_exact(self):
        reqs = list(range_requests(0, 3 * CHUNK, 100 * CHUNK, CHUNK))
        for off, req_lim, want in reqs:
            self.assertEqual(req_lim, want)
            self.assertEqual(req_lim % 4096, 0)
