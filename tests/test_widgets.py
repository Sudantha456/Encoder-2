#!/usr/bin/env python3
"""
tests/test_widgets.py — zero-network verification for the Telegram HTML widget kit.

Run:  python3 tests/test_widgets.py

Checks, for every widget and for hostile/extreme inputs:
  1. output is well-formed, tag-balanced Telegram HTML
  2. Telethon's real HTML→entity parser accepts it (no ValueError)
  3. entity offsets/lengths stay inside the message (no ENTITY_BOUNDS_INVALID)
  4. message length never exceeds the Bot API 4096 UTF-16 limit
  5. user content (file names, errors) can never inject markup
  6. every <pre> block is ≤ 32 cells wide and emoji-free (mobile/desktop safe)
  7. 0…50 workers and every part state render without raising
"""
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import widgets as W  # noqa: E402

try:                                              # real Telegram parser
    from telethon.extensions.html import parse as tg_parse
except Exception:                                 # pragma: no cover
    tg_parse = None

FAILS = []
CHECKS = 0


def check(name, cond, detail=""):
    global CHECKS
    CHECKS += 1
    if not cond:
        FAILS.append(f"{name}: {detail}")


_TAG = W._TAG_RE


def validate(name, text):
    """Structural + (when available) Telethon parser validation."""
    check(f"{name} not empty", bool(text and text.strip()), repr(text)[:120])
    clean = W.sanitize(text)

    # tag balance
    stack = []
    for m in _TAG.finditer(clean):
        closing, tag = m.group(1), m.group(2).lower()
        if tag not in W._ALLOWED_TAGS:
            check(f"{name} tag whitelist", False, tag)
            continue
        if closing:
            check(f"{name} close order", bool(stack) and stack[-1] == tag, f"{tag} / {stack[-3:]}")
            if stack:
                stack.pop()
        else:
            stack.append(tag)
    check(f"{name} balanced", not stack, str(stack))

    # no unescaped angle brackets in text segments
    pos = 0
    for m in _TAG.finditer(clean):
        seg = clean[pos:m.start()]
        pos = m.end()
        if "<" in seg or ">" in seg:
            check(f"{name} escaped text", False, seg[:80])
    check(f"{name} escaped tail", "<" not in clean[pos:] and ">" not in clean[pos:], clean[pos:][:80])

    # length (UTF-16 code units, Telegram's unit)
    check(f"{name} length", W.text_len(clean) <= W.TG_TEXT_LIMIT, str(W.text_len(clean)))

    # <pre> hygiene: ≤32 cells, no emoji (they are double-width and break alignment)
    for block in _pre_blocks(clean):
        for line in block.split("\n"):
            check(f"{name} pre width", len(line) <= 32, f"{len(line)}: {line!r}")
            check(f"{name} pre no-emoji", not _has_emoji(line), line)

    if tg_parse is not None:
        try:
            plain, entities = tg_parse(clean)
        except Exception as exc:                  # parser blow-up == broken message
            check(f"{name} telethon parse", False, repr(exc))
            return
        total = W.text_len(plain)
        check(f"{name} telethon length", total <= W.TG_TEXT_LIMIT, str(total))
        for ent in entities:
            check(f"{name} entity bounds",
                  0 <= ent.offset and ent.length >= 0 and ent.offset + ent.length <= total,
                  f"{type(ent).__name__} {ent.offset}+{ent.length} > {total}")
        check(f"{name} telethon text", isinstance(plain, str) and len(plain) > 0, plain[:60])


def _has_emoji(text):
    """Emoji / pictographs are double-width in <pre> on most clients."""
    for ch in text:
        cp = ord(ch)
        if cp >= 0x1F000 or 0x2600 <= cp <= 0x27BF or 0x2B00 <= cp <= 0x2BFF or cp == 0xFE0F:
            return True
    return False


def _pre_blocks(html_text):
    out, pos = [], 0
    while True:
        start = html_text.find("<pre>", pos)
        if start < 0:
            return out
        end = html_text.find("</pre>", start)
        if end < 0:
            return out
        out.append(html_text[start + 5:end])
        pos = end + 6


# ───────────────────────── fixtures ─────────────────────────
class Tr:
    def __init__(self, cur=0, total=0, speed=0.0, avg=0.0, peak=0.0, eta=0.0, elapsed=0.0):
        self.current, self.total = cur, total
        self.speed, self.avg_speed, self.peak_speed = speed, avg, peak
        self.eta, self.elapsed = eta, elapsed

    @property
    def percent(self):
        return self.current / self.total * 100 if self.total else 0.0


def enc_state(pct=63.4, **kw):
    st = SimpleNamespace(
        percent=pct, out_time=1245.2, duration=3600.0, speed=4.72, fps=118.0,
        eta=232.0, elapsed=448.0, out_size=412 * 1024 * 1024,
        est_total_frames=108000, frame=68400, est_final_size=661 * 1024 * 1024,
    )
    for k, v in kw.items():
        setattr(st, k, v)
    return st


ALL_STATES = ["waiting", "held", "downloading", "uploading", "remote_done", "running", "ready", "failed", "brand_new"]


def part(i, status):
    return SimpleNamespace(idx=i, status=status, pct=(i * 7) % 101, speed=1.5 + i / 10,
                           ready=status == "ready")


def dist_state(n_workers=10, phase="enc", statuses=None, **kw):
    statuses = statuses or ALL_STATES
    parts = [part(i, statuses[i % len(statuses)]) for i in range(n_workers)]
    st = SimpleNamespace(
        n_workers=n_workers, phase=phase, parts=parts, percent=52.3, out_time=1882.8,
        duration=3600.0, speed=8.42, fps=240.0, eta=88.0, elapsed=179.0,
        out_size=402 * 1024 * 1024, est_final_size=661 * 1024 * 1024,
        merge_pct=88.0, merge_step="", merge_out=3100.0, merge_size=600 * 1024 * 1024,
        merge_speed=3.2, merge_eta=90.0, merge_elapsed=95.0,
    )
    for k, v in kw.items():
        setattr(st, k, v)
    return st


HOSTILE = '<b>Movie & "Friends"</b> <script>alert(1)</script> → 1080p.mkv'
SHORT_HOSTILE = '<script>alert(1)</script>.mkv'
LONG = "🎬" + "A" * 900 + ".mkv"

TRACKERS = [
    Tr(), Tr(0, 100), Tr(50, 100, 1e6, 9e5, 2e6, 42, 13),
    Tr(100, 100, 0, 0, 0, 0, 0), Tr(10 ** 12, 2 * 10 ** 12, 3e8, 2e8, 4e8, 3600, 120),
]

# ───────────────────────── 1. progress widgets ─────────────────────────
for i, tr in enumerate(TRACKERS):
    validate(f"transfer-dl-{i}", W.render_transfer("dl", tr, HOSTILE, "⚡ 3×UB"))
    validate(f"transfer-ul-{i}", W.render_transfer("ul", tr, LONG, ""))

for pct in (0, 0.4, 50, 99.99, 100, -5, 150, None):
    validate(f"encode-{pct}", W.render_encode(enc_state(pct if pct is not None else 0),
                                              HOSTILE, 720, "veryslow", "libx265",
                                              target_label="🗜 1920×1080 (Original)",
                                              extra_info="CRF 18 • yuv420p10le"))

validate("encode-empty-state", W.render_encode(SimpleNamespace(), "", 0, "", ""))
validate("encode-missing-codec", W.render_encode(enc_state(), "x.mp4", 0, "fast", "libx264"))

for n in (0, 1, 2, 10, 11, 25, 50):
    validate(f"dist-{n}", W.render_dist(dist_state(n_workers=n), HOSTILE, 720, "fast", "libx265",
                                        target_label="720p", extra_info="CRF 18 • yuv420p"))
for phase in ("split", "upload", "merge"):
    validate(f"dist-phase-{phase}", W.render_dist(dist_state(10, phase=phase), "x.mp4", 720, "fast", "libx265"))
validate("dist-merge-audio", W.render_dist(dist_state(6, phase="merge", merge_step="audio"), "x.mp4", 720, "fast", "libx265"))
validate("dist-empty", W.render_dist(SimpleNamespace(phase="enc", parts=[], n_workers=0), "x.mp4", 720, "", ""))
validate("dist-zero-duration", W.render_dist(dist_state(4, duration=0), "x.mp4", 720, "fast", "libx265"))

# ───────────────────────── 2. command widgets ─────────────────────────
validate("home", W.home_card(
    name="VIDEO ENCODER", subtitle="premium HTML UI",
    headline=["⚡ Turbo download → encode → upload", "👤 Relay <b>ON ×3</b>"],
    modes=[("📐 <b>Resolution</b>", "1080p → 144p"), ("🗜 <b>Size-Reduce</b>", "H.265")],
    commands=[(f"/cmd{i}", f"desc {i}") for i in range(10)],
    tip="Video එකක් එවන්න — preview එකෙන් තෝරන්න.",
    footer="<i>HTML widgets</i>",
))

validate("settings", W.settings_card(
    mode_label="📐 Resolution Mode", mode_icon="📐",
    summary=["🎚 <code>ultrafast</code> · 🎞 <code>H.264</code>", "🌐 dist <code>auto</code>"],
    sections=[("🎛", "Encoder", ["🎚 Preset <code>ultrafast</code>"]),
              ("🚀", "Pipeline", ["🌐 <b>auto</b>", "👷 <b>15</b>"]),
              ("📝", "Notes", ["<i>note</i>"])],
    footer="<i>tail</i>",
))

validate("health", W.health_card(
    relay_on=True, relay_summary="3×16 = 48 conns",
    relay_rows=["🟢 <b>UB1</b> · ready", "⏸ <b>UB2</b> · cooldown <code>42s</code>"],
    helper_summary="🤝 Helpers <b>2/3</b>",
    helper_rows=[f"✅ <code>repo-{i}</code>" for i in range(8)],
    extra=["🌐 Distributed <b>ON</b>"], footer="<i>fallback safe</i>",
))

validate("queued", W.queued_card(index=7, total=12, name=HOSTILE, badge="🗜 Size-Reduce (H.265 • CRF 18)",
                                 ahead=[(1, "a.mkv", "720p"), (2, "b.mkv", "480p"), (3, "c.mkv", "360p")]))
validate("queue-empty", W.queue_card(cap=20))
validate("queue-full", W.queue_card(
    running={"name": HOSTILE, "badge": "720p", "pct": "42%"},
    items=[{"name": f"movie_{i}.mkv", "badge": "1080p"} for i in range(20)], cap=20,
))

validate("endwork-progress", W.endwork_progress(round_no=2, total=5,
                                                counts=[("🏠 Main", 3), ("🤝 Helper #1", 2)]))
validate("endwork-ok", W.endwork_result(accounts=[
    {"label": "🏠 Main", "repo": "user/repo", "killed": 3, "left": 0},
    {"label": "🤝 Helper #1", "repo": "user/helper", "killed": 2, "left": 0},
], left_total=0, rounds=2))
validate("endwork-left", W.endwork_result(accounts=[
    {"label": "🏠 Main", "repo": "user/repo", "killed": 1, "left": 4},
], left_total=4, rounds=5, busy=True))

validate("toast", W.toast_card(icon="🧹", title_text="QUEUE CLEARED",
                               lines=["Queue එකෙන් <b>2</b> ක් ඉවත් කළා"], hint="hint"))
validate("event", W.event_card(icon="📨", title_text="RELAY", lines=["forwarding…"],
                               hint="hint", gauge_pct=42))
validate("preview", W.preview_card(
    title_text="VIDEO PREVIEW", filename=HOSTILE,
    sections=[("🎞", "Source", ["🎞 Codec <code>H.264</code>",
                                "📐 Resolution <code>1920×1080</code>"]),
              ("⚙️", "Encode", ["🎚 Preset <code>fast</code>"])],
    footer="👇 <b>Output resolution තෝරන්න</b>",
))

validate("done", W.done_card(
    name=HOSTILE, before="661.3 MB", after="412.6 MB", saved_pct=37.6,
    stats=[("DL", "84.2MB/s", "00:01:12", "x3"), ("ENC", "4.72x", "00:07:28", "x6"),
           ("UP", "32.1MB/s", "00:00:54", "UB1"), ("TOTAL", "", "00:09:41", "")],
    extras=["👷 workers <b>6</b>", "♻️ reassigned <b>1</b>"],
    footer="<i>done</i>",
))
validate("done-negative", W.done_card(name="x.mp4", before="1.0 MB", after="2.0 MB",
                                      saved_pct=-100, stats=[], extras=[]))
validate("failed", W.failed_card(name=HOSTILE, error="ffmpeg: <No such file> & broken 'quote'"))
validate("failed-huge", W.failed_card(name=LONG, error="E" * 5000))
validate("cancelled", W.cancelled_card(name=HOSTILE))
validate("cancelled-bare", W.cancelled_card())

# ───────────────────────── 2b. coloured meters & palette ─────────────────────────
check("meter starts red", W.meter(10).count(W.RED) > 0, W.meter(10))
check("meter mid yellow", W.meter(50).count(W.YELLOW) > 0, W.meter(50))
check("meter end green", W.meter(90).count(W.GREEN) > 0, W.meter(90))
check("meter 0% empty", W.meter(0) == W.WHITE * 10, W.meter(0))
check("meter 100% full", W.meter(100) == W.GREEN * 10, W.meter(100))

for pct in (0, 0.4, 12.5, 33, 49.9, 66, 88.2, 99.99, 100, -5, 150, None):
    line = W.meter_line(pct if pct is not None else 0)
    check(f"meter-line-{pct} width", W.cells(line) <= 30, f"{W.cells(line)} cells: {line}")
    validate(f"meter-{pct}", line)

check("pack_parts budget", all(W.cells(l) <= 30 for l in
                               W.pack_parts(["🎞 <code>H.265</code>", "⚙️ <code>veryfast</code>",
                                             "👷 <code>35w</code>", "🎯 <code>CRF24</code>",
                                             "🎨 <code>8b</code>"])))
check("keycaps", W.num_badge(1) == "1️⃣" and W.num_badge(10) == "🔟" and W.num_badge(11) == "11.")
check("badge colour", W.badge_icon("Size-Reduce (H.265 • CRF 18)") == W.PURPLE
      and W.badge_icon("720p") == W.BLUE)
check("onoff colour", W.RED in W.onoff(False) and W.GREEN in W.onoff(True))

# ───────────────────────── 3. hardening ─────────────────────────
# hostile content cannot inject markup
out = W.done_card(name=SHORT_HOSTILE, before="1 MB", after="900 KB", saved_pct=10,
                  stats=[], extras=[])
plain = W.sanitize(out)
check("injection escaped", "&lt;script&gt;" in plain and "<script>" not in plain, plain[:200])
if tg_parse is not None:
    text_out, ents = tg_parse(out)
    check("injection stays text", "<script>alert(1)</script>" in text_out, text_out[:160])
    check("injection no bold injection",
          not any(type(e).__name__ == "MessageEntityBold" and "alert" in text_out[e.offset:e.offset + e.length]
                  for e in ents))

# already-valid HTML must survive sanitize untouched
src = "<b>a</b> <blockquote expandable>x &amp; y</blockquote> <pre>1 2</pre>"
check("sanitize idempotent", W.sanitize(W.sanitize(src)) == W.sanitize(src))
check("expandable kept", "<blockquote expandable>" in W.sanitize(src))

# broken nesting is repaired
check("repair nesting", W.sanitize("<b><i>x</b></i>") == "<b><i>x</i></b>", W.sanitize("<b><i>x</b></i>"))

# unknown tags dropped, text kept
check("drop unknown", W.sanitize("<div>hi<u>x</u></div>") == "hi<u>x</u>", W.sanitize("<div>hi<u>x</u></div>"))

# bare ampersands / stray '<' are escaped
check("bare amp", "&amp;" in W.sanitize("a & b"), W.sanitize("a & b"))
check("stray lt", "&lt;" in W.sanitize("2 < 3 <b>ok</b>"), W.sanitize("2 < 3 <b>ok</b>"))

# truncation keeps tags balanced + entity-safe
huge = "<b>" + ("x" * 5000) + "</b><i>tail</i>"
trunc = W.truncate_html(huge, 500)
if tg_parse is not None:
    _tl = W.text_len(tg_parse(trunc)[0])
else:
    import re as _re
    _tl = W.text_len(_re.sub(r"<[^>]+>", "", trunc))
check("truncate length", _tl <= 500, str(_tl))
check("truncate balanced", trunc.count("<b>") == trunc.count("</b>"))
check("truncate entity", not trunc.rstrip("</b>").endswith("&"), trunc[-20:])
validate("truncate-parses", trunc)

# doc overflow drops whole trailing blocks
d = W.Doc()
for i in range(200):
    d.add(f"<b>line {i}</b> " + "y" * 40)
built = d.build()
check("doc overflow", W.text_len(built) <= W.SAFE_LIMIT, str(W.text_len(built)))
validate("doc-parses", built)

# hostile object graphs must never raise
validate("none-transfer", W.render_transfer("dl", SimpleNamespace(), None, None))
validate("none-dist", W.render_dist(SimpleNamespace(), None, None, None, None))
validate("none-encode", W.render_encode(SimpleNamespace(), None, None, None, None))
validate("queue-none", W.queue_card(running={"name": None, "badge": None},
                                    items=[{"name": None}], cap=0))

# formatting helpers are division-safe
check("bar bounds", W.bar(-10) == "░" * 14 and W.bar(1000) == "█" * 14, W.bar(-10))
check("human_size", W.human_size(0) == "0 B" and W.human_size(1536) == "1.5 KB", W.human_size(1536))
check("human_time", W.human_time(-5) == "00:00:00" and W.human_time(3661) == "01:01:01")

# ───────────────────────── report ─────────────────────────
if FAILS:
    print(f"\n❌ {len(FAILS)} failure(s) / {CHECKS} checks\n")
    for f in FAILS[:40]:
        print("  •", f)
    sys.exit(1)
print(f"✅ widgets: {CHECKS} checks passed"
      + ("" if tg_parse else "  (Telethon parser unavailable — structural checks only)"))
