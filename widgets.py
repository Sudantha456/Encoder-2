"""
widgets.py — Premium Telegram HTML widget kit.

Design goals
────────────
• **One visual language** for every command (/start, /settings, /check, /queue,
  /workflow, /clear, /End_Work) *and* every live job update (queue → download →
  probe → encode → distributed encode → upload → done/failed/cancelled).
• **Mobile first, desktop friendly** — every line is kept inside the ~34-cell
  Telegram mobile gutter, so nothing wraps or scrolls sideways. Wide/column data
  lives only inside ``<pre>`` blocks, and those blocks never contain emoji
  (emoji render double-width on some clients and would break the alignment).
• **Premium look with native HTML entities** — bold titles, native
  ``<blockquote>`` cards, collapsible ``<blockquote expandable>`` panels,
  monospace gauges/boards, ``<code>`` values. No custom-emoji ids, no colours,
  no exotic tags: only what Telegram's HTML parse mode supports on every client
  (Android / iOS / Desktop / Web).
• **Cannot break the bot** — every renderer returns tag-balanced, entity-safe,
  length-safe HTML (≤ 3800 UTF-16 units). ``sanitize()`` drops unsupported tags,
  repairs nesting and escapes stray ``&``/``<``; ``truncate_html()`` never splits
  a tag, an entity or a surrogate pair. All renderers tolerate ``None``/missing
  fields, so a failed GitHub Actions probe can never raise while rendering.
• **Action-runner safe** — the distributed-encode board handles 0…50 workers,
  every part state (waiting/held/downloading/running/remote_done/ready/failed)
  and unknown future states without ever producing an unparsable message.

Everything here is pure stdlib (no Telethon import), so it can be unit-tested
without network access.
"""
from __future__ import annotations

import html as _html
import re
from typing import Iterable, List, Optional, Sequence, Tuple

# ───────────────────────── limits ─────────────────────────
TG_TEXT_LIMIT = 4096      # Bot API hard limit (UTF-16 code units)
SAFE_LIMIT = 3800         # our budget: leaves room for the final "…" + edits

# ───────────────────────── text helpers ─────────────────────────
def esc(value) -> str:
    """Escape everything that could be mistaken for HTML."""
    if value is None:
        return ""
    return _html.escape(str(value), quote=False)


def clip(value, limit: int) -> str:
    """Clip *raw* text (before escaping) and add an ellipsis."""
    s = "" if value is None else str(value)
    if limit <= 0:
        return ""
    return s if len(s) <= limit else s[: max(0, limit - 1)].rstrip() + "…"


def text_len(value: str) -> int:
    """Telegram counts message length in UTF-16 code units, not code points."""
    return len(value.encode("utf-16-le")) // 2


def human_size(n) -> str:
    try:
        n = float(n or 0)
    except (TypeError, ValueError):
        n = 0.0
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024 or unit == "TB":
            return f"{n:.0f} B" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024.0
    return f"{n:.1f} TB"


def human_size_short(n) -> str:
    return human_size(n).replace(" ", "")


def human_speed(bps) -> str:
    try:
        if float(bps or 0) <= 0:
            return "—"
    except (TypeError, ValueError):
        return "—"
    return f"{human_size(bps)}/s"


def human_time(sec) -> str:
    try:
        sec = max(0, int(sec or 0))
    except (TypeError, ValueError):
        sec = 0
    h, r = divmod(sec, 3600)
    m, s = divmod(r, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def human_age(sec) -> str:
    """Short age: ``12s``, ``4m 05s``, ``3h 12m``."""
    try:
        sec = max(0, int(sec or 0))
    except (TypeError, ValueError):
        sec = 0
    h, rem = divmod(sec, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m:02d}m"
    return f"{m}m {s:02d}s" if m else f"{s}s"


def speed_txt(sp) -> str:
    """``4.72x`` / ``12.4x`` — stable, 4-5 chars."""
    try:
        sp = max(0.0, float(sp or 0.0))
    except (TypeError, ValueError):
        sp = 0.0
    return f"{sp:.1f}x" if sp < 100 else f"{sp:.0f}x"


# ───────────────────────── gauges ─────────────────────────
def bar(pct, width: int = 14) -> str:
    """`████████░░░░░░` — pure single-cell glyphs, safe inside <pre>."""
    try:
        pct = float(pct or 0.0)
    except (TypeError, ValueError):
        pct = 0.0
    width = max(3, int(width))
    pct = min(100.0, max(0.0, pct))
    filled = int(round(width * pct / 100.0))
    return "█" * filled + "░" * (width - filled)


def pct_txt(pct) -> str:
    try:
        pct = float(pct or 0.0)
    except (TypeError, ValueError):
        pct = 0.0
    return f"{min(100.0, max(0.0, pct)):5.1f}%"


def gauge(pct, width: int = 14, suffix: str = "") -> str:
    """Single-line monospace gauge used as a widget 'meter'."""
    return f"[{bar(pct, width)}] {pct_txt(pct)}{(' ' + suffix) if suffix else ''}"


# ───────────────────────── HTML sanitiser ─────────────────────────
# Tags Telegram's HTML parse mode understands (and that Telethon maps to entities).
_ALLOWED_TAGS = {
    "b", "strong", "i", "em", "u", "ins", "s", "strike", "del",
    "code", "pre", "blockquote", "tg-spoiler", "a",
}

_TAG_RE = re.compile(r"<\s*(/?)\s*([a-zA-Z][a-zA-Z0-9-]*)((?:\s[^<>]*?)?)(/?)\s*>")
_ENTITY_TAIL_RE = re.compile(r"&[a-zA-Z#0-9]{0,31}$")
_BARE_AMP_RE = re.compile(r"&(?!(?:[a-zA-Z][a-zA-Z0-9]{1,31}|#\d{1,7}|#x[0-9a-fA-F]{1,6});)")


def _escape_segment(segment: str) -> str:
    """Only touch *text* segments: stray ``<`` and non-entity ``&``."""
    if not segment:
        return ""
    return _BARE_AMP_RE.sub("&amp;", segment.replace("<", "&lt;"))


def sanitize(text: str) -> str:
    """
    Return guaranteed-parsable Telegram HTML.

    • drops unsupported/void tags (keeps their text)
    • repairs mis-nested tags by closing/opening in the right order
    • escapes stray ``<`` and bare ``&`` inside text segments
    • keeps ``<blockquote expandable>`` (collapsible panel support)
    """
    if not text:
        return ""
    out: List[str] = []
    stack: List[str] = []
    pos = 0
    for m in _TAG_RE.finditer(text):
        out.append(_escape_segment(text[pos:m.start()]))
        pos = m.end()
        closing, name = m.group(1), (m.group(2) or "").lower()
        attrs = m.group(3) or ""
        self_closing = m.group(4)
        if name not in _ALLOWED_TAGS or self_closing:
            continue                                  # unknown tag → keep text only
        if closing:
            if name in stack:
                while stack and stack[-1] != name:
                    out.append(f"</{stack.pop()}>")
                stack.pop()
                out.append(f"</{name}>")
            continue                                  # stray close → ignore
        if name == "blockquote" and "expandable" in attrs:
            out.append("<blockquote expandable>")
        elif name == "a":
            href = re.search(r'href\s*=\s*"([^"]*)"', attrs) or re.search(r"href\s*=\s*'([^']*)'", attrs)
            if not href:
                continue
            out.append(f'<a href="{_html.escape(href.group(1), quote=True)}">')
        else:
            out.append(f"<{name}>")
        stack.append(name)
    out.append(_escape_segment(text[pos:]))
    while stack:
        out.append(f"</{stack.pop()}>")
    return "".join(out)


def _cut_escaped(segment: str, room: int) -> str:
    """Cut an already-escaped text segment to *room* UTF-16 units, entity-safe."""
    if room <= 0:
        return ""
    used = 0
    idx = 0
    for ch in segment:
        cost = 2 if ord(ch) > 0xFFFF else 1
        if used + cost > room:
            break
        used += cost
        idx += 1
    cut = segment[:idx]
    return _ENTITY_TAIL_RE.sub("", cut)              # never leave `&am` behind


def to_plain(text: str) -> str:
    """
    Degraded, parse-free fallback for the (theoretical) case where a client or
    server rejects the entity set. Always returns something printable.
    """
    if not text:
        return ""
    try:
        return _html.unescape(_TAG_RE.sub("", sanitize(text)))
    except Exception:                              # never let the UI die
        return str(text)


def truncate_html(text: str, limit: int = SAFE_LIMIT, ellipsis: str = "…") -> str:
    """Sanitize + hard-truncate while keeping every tag balanced."""
    text = sanitize(text)
    if text_len(text) <= limit:
        return text
    budget = max(0, limit - text_len(ellipsis))
    out: List[str] = []
    stack: List[str] = []
    used = 0
    stopped = False
    pos = 0
    for m in _TAG_RE.finditer(text):
        segment = text[pos:m.start()]
        pos = m.end()
        closing, name = m.group(1), (m.group(2) or "").lower()
        if not stopped:
            if segment:
                room = budget - used
                if text_len(segment) <= room:
                    out.append(segment)
                    used += text_len(segment)
                else:
                    out.append(_cut_escaped(segment, room))
                    used = budget
                    stopped = True
            if not stopped:
                if closing:
                    if stack and stack[-1] == name:
                        stack.pop()
                        out.append(f"</{name}>")
                    elif name in stack:
                        while stack and stack[-1] != name:
                            out.append(f"</{stack.pop()}>")
                        stack.pop()
                        out.append(f"</{name}>")
                else:
                    stack.append(name)
                    out.append(m.group(0) if name != "blockquote" else (
                        "<blockquote expandable>" if "expandable" in (m.group(3) or "") else "<blockquote>"))
    if not stopped:
        segment = text[pos:]
        room = budget - used
        out.append(segment if text_len(segment) <= room else _cut_escaped(segment, room))
    out.append(ellipsis)
    while stack:
        out.append(f"</{stack.pop()}>")
    return "".join(out)


# ───────────────────────── building blocks ─────────────────────────
RULE_CHAR = "─"
SECTION_MARK = "▎"


def title(icon: str, text: str, hint: str = "") -> str:
    head = f"{icon} <b>{esc(text)}</b>" if icon else f"<b>{esc(text)}</b>"
    return f"{head} · <i>{esc(hint)}</i>" if hint else head


def section(icon: str, label: str, hint: str = "") -> str:
    head = f"{SECTION_MARK}{icon} <b>{esc(label)}</b>" if icon else f"{SECTION_MARK}<b>{esc(label)}</b>"
    return f"{head} · <i>{esc(hint)}</i>" if hint else head


def kv(icon: str, label: str, value, mono: bool = True) -> str:
    """`🎚 Preset  <code>ultrafast</code>` — one fact per line."""
    v = f"<code>{esc(value)}</code>" if mono else f"<b>{esc(value)}</b>"
    return f"{icon} {esc(label)} {v}".replace("  ", " ")


def bullet(icon: str, text: str) -> str:
    return f"{icon} {esc(text)}" if icon else f"• {esc(text)}"


def rule(width: int = 18) -> str:
    return RULE_CHAR * max(3, width)


def quote(lines, expandable: bool = False) -> str:
    body = "\n".join(lines) if not isinstance(lines, str) else lines
    return f"<blockquote expandable>{body}</blockquote>" if expandable else f"<blockquote>{body}</blockquote>"


def mono(lines) -> str:
    body = "\n".join(lines) if not isinstance(lines, str) else lines
    return f"<pre>{esc(body)}</pre>"


def chips(*items) -> str:
    return " · ".join(f"<code>{esc(i)}</code>" for i in items if i not in (None, ""))


def onoff(flag, on: str = "ON", off: str = "OFF") -> str:
    return f"{'✅' if flag else '❌'} <b>{esc(on if flag else off)}</b>"


def status_dot(ok: bool) -> str:
    return "🟢" if ok else "🔴"


def note(text: str) -> str:
    return f"<i>{esc(text)}</i>"


class Doc:
    """
    Compose a message out of self-contained, tag-balanced blocks.

    ``build()`` sanitizes the result and, if it would exceed the Telegram limit,
    drops trailing blocks (never splitting one) so the HTML always stays valid.
    """

    def __init__(self, limit: int = SAFE_LIMIT):
        self.limit = limit
        self.blocks: List[str] = []

    # ── composition ──
    def add(self, block: Optional[str]) -> "Doc":
        if block:
            self.blocks.append(block)
        return self

    def extend(self, blocks: Iterable[Optional[str]]) -> "Doc":
        for b in blocks:
            self.add(b)
        return self

    def line(self, text: str) -> "Doc":
        return self.add(text)

    def gap(self) -> "Doc":
        return self.add("")

    def head(self, icon: str, text: str, hint: str = "") -> "Doc":
        return self.add(title(icon, text, hint))

    def sec(self, icon: str, label: str, hint: str = "") -> "Doc":
        return self.add(section(icon, label, hint))

    def kv(self, icon: str, label: str, value, mono: bool = True) -> "Doc":
        return self.add(kv(icon, label, value, mono))

    def rule(self, width: int = 18) -> "Doc":
        return self.add(rule(width))

    def quote(self, lines, expandable: bool = False) -> "Doc":
        return self.add(quote(lines, expandable))

    def mono(self, lines) -> "Doc":
        return self.add(mono(lines))

    def gauge(self, pct, width: int = 14) -> "Doc":
        return self.add(mono([gauge(pct, width)]))

    def build(self, tail: Optional[str] = None) -> str:
        blocks = list(self.blocks)
        if tail:
            blocks.append(tail)
        if not blocks:
            return ""
        text = sanitize("\n".join(blocks))
        if text_len(text) <= self.limit:
            return text
        while blocks:
            blocks.pop()
            candidate = sanitize("\n".join(blocks + [note("…")]))
            if text_len(candidate) <= self.limit:
                return candidate
        return note("…")


# ───────────────────────── job progress widgets ─────────────────────────
def render_transfer(kind: str, tr, name: str, badge: str = "") -> str:
    """Live download / upload card (drop-in replacement for the old renderer)."""
    up = str(kind).lower().startswith("u")
    icon, word = ("📤", "UPLOAD") if up else ("📥", "DOWNLOAD")
    pct = getattr(tr, "percent", 0.0) or 0.0
    cur = getattr(tr, "current", 0) or 0
    tot = getattr(tr, "total", 0) or 0
    d = Doc()
    d.head(icon, word, badge)
    d.quote(f"📄 <code>{esc(clip(name, 44))}</code>")
    d.mono([gauge(pct, 14)])
    d.add(f"📦 <code>{human_size(cur)}</code> / <code>{human_size(tot)}</code>")
    d.add(f"⚡ <code>{human_speed(getattr(tr, 'speed', 0))}</code> · avg <code>{human_speed(getattr(tr, 'avg_speed', 0))}</code>")
    peak = getattr(tr, "peak_speed", 0.0) or 0.0
    if peak > 0 and (getattr(tr, "elapsed", 0.0) or 0) >= 1.5:
        d.add(f"📈 peak <code>{human_speed(peak)}</code>")
    d.add(f"⏳ <code>{human_time(getattr(tr, 'eta', 0))}</code> · ⏱ <code>{human_time(getattr(tr, 'elapsed', 0))}</code>")
    return d.build()


_CODEC_SHORT = {"libx265": "H.265", "libx264": "H.264"}


def _codec_txt(codec: str) -> str:
    c = str(codec or "")
    return _CODEC_SHORT.get(c, c)


def _extra_short(extra_info: str) -> str:
    if not extra_info:
        return ""
    s = str(extra_info).replace("CRF ", "CRF")
    for full, short in (
        ("yuv420p10le", "10b"),
        ("yuv422p10le", "422-10b"),
        ("yuv444p10le", "444-10b"),
        ("yuv420p12le", "12b"),
        ("yuv422p12le", "422-12b"),
        ("yuv444p12le", "444-12b"),
        ("yuv420p16le", "16b"),
        ("yuv422p16le", "422-16b"),
        ("yuv444p16le", "444-16b"),
        ("yuv420p", "8b"),
        ("yuv422p", "422-8b"),
        ("yuv444p", "444-8b"),
    ):
        s = s.replace(full, short)
    return s


def _cfg_line(preset: str, codec: str, extra: str, workers: Optional[int] = None) -> str:
    parts = [_codec_txt(codec), preset]
    ex = _extra_short(extra)
    if ex:
        parts.append(ex)
    if workers:
        parts.append(f"{workers}w")
    return " · ".join(p for p in parts if p)


def render_encode(st, name: str, height: int, preset: str, codec: str, *,
                  target_label: str = "", extra_info: str = "") -> str:
    """Live single-runner encode card."""
    pct = getattr(st, "percent", 0.0) or 0.0
    t_lbl = (target_label or (f"{height}p" if height else "Original")).replace(" (Original)", "")
    d = Doc()
    d.head("🎬", "ENCODE", t_lbl)
    d.quote([f"📄 <code>{esc(clip(name, 40))}</code>",
             f"⚙️ <code>{esc(_cfg_line(preset, codec, extra_info))}</code>"])
    d.mono([gauge(pct, 14)])
    d.add(f"🎞 <code>{human_time(getattr(st, 'out_time', 0))}</code> / <code>{human_time(getattr(st, 'duration', 0))}</code>")
    d.add(f"🚀 <code>{speed_txt(getattr(st, 'speed', 0))}</code> · <code>{int(getattr(st, 'fps', 0) or 0)} fps</code>")
    d.add(f"⏳ <code>{human_time(getattr(st, 'eta', 0))}</code> · ⏱ <code>{human_time(getattr(st, 'elapsed', 0))}</code>")
    size = f"📦 <code>{human_size(getattr(st, 'out_size', 0))}</code>"
    est = getattr(st, "est_final_size", 0) or 0
    if est:
        size += f" / ≈<code>{human_size(est)}</code>"
    d.add(size)
    return d.build()


_PART_STATE_ROWS = {
    "ready":       ("100%", "DONE"),
    "uploading":   (" 99%", "save"),
    "remote_done": (" 99%", "pull"),
    "downloading": ("  0%", "load"),
    "waiting":     (" ..", "wait"),
    "held":        (" --", "held"),
    "failed":      (" !!", "retry"),
}


def _part_cell(p, compact: bool = False) -> str:
    """ASCII-only cell for the <pre> worker board (never wider than 13 chars)."""
    try:
        num = f"{(int(getattr(p, 'idx', 0)) + 1) % 100:02d}"
    except (TypeError, ValueError):
        num = "??"
    status = str(getattr(p, "status", "") or "")
    pct_v = getattr(p, "pct", 0) or 0
    if status == "running":
        if compact:
            return f"{num} {pct_txt(pct_v)} {speed_txt(getattr(p, 'speed', 0))}"
        return f"{num} [{bar(pct_v, 8)}] {pct_txt(pct_v)} {speed_txt(getattr(p, 'speed', 0))}"
    if status in _PART_STATE_ROWS:
        mark, word = _PART_STATE_ROWS[status]
        if compact:
            return f"{num} {mark}  {word}"
        return f"{num} [{'█' * 8 if word in ('DONE', 'save', 'pull') else '░' * 8}] {mark}  {word}"
    word = status[:10] or "?"
    if compact:                                  # keep the 2-column board ≤29 cells
        return f"{num} {word[:8]:>8s}"
    return f"{num} [░░░░░░░░] {word:>10s}"


def _worker_board(parts: Sequence) -> List[str]:
    """1-column for ≤10 workers, 2-column for 11…50 (both ≤29 cells wide)."""
    parts = list(parts or [])
    if not parts:
        return ["no parts yet"]
    if len(parts) <= 10:
        return [_part_cell(p) for p in parts]
    cells = [_part_cell(p, compact=True) for p in parts]
    rows = []
    for i in range(0, len(cells), 2):
        if i + 1 < len(cells):
            rows.append(f"{cells[i]} │ {cells[i + 1]}")
        else:
            rows.append(cells[i])
    return rows


def render_dist(st, name: str, height: int, preset: str, codec: str, *,
                target_label: str = "", extra_info: str = "") -> str:
    """Live distributed (GitHub Actions) encode card + worker board."""
    t_lbl = (target_label or (f"{height}p" if height else "Original")).replace(" (Original)", "")
    try:
        n_workers = int(getattr(st, "n_workers", 0) or 0)
    except (TypeError, ValueError):
        n_workers = 0
    d = Doc()
    d.head("🌐", "DIST ENCODE", t_lbl)
    d.quote([f"📄 <code>{esc(clip(name, 40))}</code>",
             f"⚙️ <code>{esc(_cfg_line(preset, codec, extra_info, n_workers))}</code>"])
    phase = str(getattr(st, "phase", "") or "")

    parts = list(getattr(st, "parts", []) or [])
    if phase == "split":
        d.mono([gauge(0, 14)])
        d.add("✂️ Keyframes අනුව video එක කොටස් වලට කඩනවා…")
        d.add(f"🧩 parts <b>{len(parts)}</b> · 👷 workers <b>{n_workers}</b>")
        return d.build()

    if phase == "upload":
        d.mono([gauge(0, 14)])
        d.add(f"☁️ කොටස් <b>{len(parts)}</b> Release එකට upload කරනවා…")
        return d.build()

    if phase == "merge":
        d.mono([gauge(getattr(st, "merge_pct", 0), 14)])
        step = str(getattr(st, "merge_step", "") or "")
        if step == "audio":
            d.add(f"🔊 Audio track සකසනවා… <code>{human_time(getattr(st, 'merge_elapsed', 0))}</code>")
            return d.build()
        d.add(f"🎞 <code>{human_time(getattr(st, 'merge_out', 0))}</code> / <code>{human_time(getattr(st, 'duration', 0))}</code>")
        d.add(f"🚀 <code>{speed_txt(getattr(st, 'merge_speed', 0))}</code> · 📦 <code>{human_size(getattr(st, 'merge_size', 0))}</code>")
        m_left = 0.0
        if getattr(st, "merge_speed", 0) and getattr(st, "duration", 0):
            m_left = max(0.0, st.duration - (getattr(st, "merge_out", 0) or 0)) / st.merge_speed
        d.add(f"⏳ <code>{human_time(m_left)}</code> · ⏱ <code>{human_time(getattr(st, 'merge_elapsed', 0))}</code>")
        d.add(f"🔗 parts <b>{len(parts)}</b> merge වෙනවා…")
        return d.build()

    n_done = sum(1 for p in parts if getattr(p, "ready", False))
    n_run = sum(1 for p in parts if getattr(p, "status", "") == "running" and not getattr(p, "ready", False))
    n_cloud = sum(1 for p in parts if getattr(p, "status", "") in ("downloading", "uploading", "remote_done")
                  and not getattr(p, "ready", False))
    n_wait = max(0, len(parts) - n_done - n_run - n_cloud)

    d.mono([gauge(getattr(st, "percent", 0), 14)])
    d.add(f"🎞 <code>{human_time(getattr(st, 'out_time', 0))}</code> / <code>{human_time(getattr(st, 'duration', 0))}</code>")
    d.add(f"🚀 <code>{speed_txt(getattr(st, 'speed', 0))}</code> · <code>{int(getattr(st, 'fps', 0) or 0)} fps</code>")
    d.add(f"⏳ <code>{human_time(getattr(st, 'eta', 0))}</code> · ⏱ <code>{human_time(getattr(st, 'elapsed', 0))}</code>")
    size = f"📦 <code>{human_size(getattr(st, 'out_size', 0))}</code>"
    est = getattr(st, "est_final_size", 0) or 0
    if est:
        size += f" / ≈<code>{human_size(est)}</code>"
    d.add(size)
    bits = [f"✅<b>{n_done}/{len(parts)}</b>"]
    for icon, n in (("▶️", n_run), ("☁️", n_cloud), ("⏳", n_wait)):
        if n:
            bits.append(f"{icon}{n}")
    d.add("👷 " + " · ".join(bits))
    d.mono(_worker_board(parts))
    return d.build()


# ───────────────────────── command widgets ─────────────────────────
def home_card(*, name: str = "VIDEO ENCODER", subtitle: str = "premium HTML UI",
              headline: Sequence[str] = (), modes: Sequence[Tuple[str, str]] = (),
              commands: Sequence[Tuple[str, str]] = (), tip: str = "",
              footer: str = "") -> str:
    d = Doc()
    d.head("🎬", name, subtitle)
    if headline:
        d.quote(list(headline))
    if modes:
        d.sec("🔀", "Modes")
        for label, desc in modes:
            d.add(f"• {label} — {desc}")
    if commands:
        d.sec("⌨️", "Commands")
        d.quote([f"{cmd} — {desc}" for cmd, desc in commands], expandable=len(commands) > 4)
    if tip:
        d.sec("💡", "Tip")
        d.add(note(tip))
    return d.build(tail=footer or None)


def settings_card(*, mode_label: str, mode_icon: str, summary: Sequence[str],
                  sections: Sequence[Tuple[str, str, Sequence[str]]],
                  footer: str = "", hint: str = "live") -> str:
    d = Doc()
    d.head("⚙️", "ENCODE SETTINGS", hint)
    d.quote([f"{mode_icon} <b>{esc(mode_label)}</b>"] + list(summary))
    for icon, label, rows in sections:
        if not rows:
            continue
        d.sec(icon, label)
        d.extend(rows)
    return d.build(tail=footer or None)


def health_card(*, relay_on: bool, relay_summary: str, relay_rows: Sequence[str],
                helper_summary: str = "", helper_rows: Sequence[str] = (),
                extra: Sequence[str] = (), footer: str = "",
                hint: str = "userbots · runners") -> str:
    """Diagnostics card for /check (and start-up notices)."""
    d = Doc()
    d.head("🩺", "SYSTEM CHECK", hint)
    top = [f"👤 Userbot relay {onoff(relay_on)} · <code>{esc(relay_summary)}</code>"]
    if helper_summary:
        top.append(helper_summary)
    d.quote(top)
    d.sec("👤", "Userbot lanes")
    d.extend(list(relay_rows) or [note("userbots configure කරලා නෑ")])
    if helper_rows:
        d.sec("🤝", "Helper accounts")
        d.quote(list(helper_rows), expandable=len(helper_rows) > 4)
    if extra:
        d.sec("🌐", "Runner pool")
        d.extend(extra)
    return d.build(tail=footer or None)


def queued_card(*, index: int, total: int, name: str, badge: str,
                ahead: Sequence[Tuple[int, str, str]] = ()) -> str:
    """Queue position card with a mini pipeline of the jobs ahead."""
    d = Doc()
    d.head("⏳", "QUEUED", f"#{index}/{total}")
    d.quote([f"🎬 <code>{esc(clip(name, 40))}</code>",
             f"🎯 <b>{esc(badge)}</b>"])
    if ahead:
        rows = [f"<b>{i:02d}</b> · <code>{esc(clip(n, 30))}</code>\n"
                f"      🎯 {esc(clip(b, 20))}" for i, n, b in ahead[:3]]
        d.sec("⏫", "ඉස්සරහ", f"{len(ahead)} jobs")
        d.quote(rows)
    d.add("⚡ Jobs ඉවර වුණාම auto start වෙයි")
    d.sec("💡", "Tip")
    d.add(note("❌ Cancel එකෙන් job එක queue එකෙන් අයින් කරන්න."))
    return d.build()


def queue_card(*, running: Optional[dict] = None,
               items: Sequence[dict] = (), cap: int = 0) -> str:
    total = len(items) + (1 if running else 0)
    d = Doc()
    d.head("📋", "QUEUE", f"{total}/{cap}" if cap else f"{total}")
    if not running and not items:
        d.quote(["📭 Queue එක හිස්", "Video එකක් එවන්න — වහාම queue වෙයි."])
        d.sec("💡", "Tip")
        d.add(note("/settings වලින් mode, preset, workers තෝරන්න."))
        return d.build()
    if running:
        d.quote(["▶️ <b>Running</b>",
                 f"🎬 <code>{esc(clip(running.get('name'), 40))}</code>",
                 f"🎯 <b>{esc(running.get('badge') or '—')}</b>"
                 + (f" · <code>{esc(running.get('pct'))}</code>" if running.get("pct") else "")])
    if items:
        rows = []
        for i, it in enumerate(items, 1):
            rows.append(f"<b>{i:02d}</b> · <code>{esc(clip(it.get('name'), 34))}</code>\n"
                        f"      🎯 <b>{esc(it.get('badge') or '—')}</b>")
        d.sec("⏳", "Waiting", f"{len(items)}")
        d.quote(rows, expandable=len(rows) > 3)
    return d.build(tail=note("Queue එකේ job එකක් ඉවත් කරන්න: /clear"))


def workflow_account(*, label: str, repo: str, rows: Sequence[str]) -> List[str]:
    """Blocks for one GitHub account inside /workflow."""
    blocks = [section("", label, repo)]
    blocks.extend(rows)
    return blocks


def endwork_progress(*, round_no: int, total: int, counts: Sequence[Tuple[str, int]]) -> str:
    d = Doc()
    d.head("🛑", "END WORK", f"kill {round_no}/{total}")
    rows = [f"{label} · <b>{n}</b> run" for label, n in counts if n]
    if rows:
        d.quote(rows)
    d.mono([gauge((round_no / max(total, 1)) * 100, 14, "sent")])
    return d.build(tail=note("Workers kill වෙනකන් තත්පර කීපයක් ඉන්න…"))


def endwork_result(*, accounts: Sequence[dict], left_total: int, rounds: int,
                   busy: bool = False) -> str:
    d = Doc()
    d.head("🛑", "END WORK", "complete" if not left_total else "partial")
    killed = sum(int(a.get("killed") or 0) for a in accounts)
    if left_total:
        d.quote(["⚠️ <b>සම්පූර්ණයෙන් kill වුණේ නෑ</b>",
                 f"🔪 killed <b>{killed}</b> · ⏳ ඉතුරු <b>{left_total}</b>",
                 f"🔁 kill requests <b>{rounds}</b>"])
    else:
        d.quote(["✅ <b>සේරම worker workflows kill වුණා</b>",
                 f"🔪 killed <b>{killed}</b> · 🔁 requests <b>{rounds}</b>"])
    for a in accounts:
        left = int(a.get("left") or 0)
        d.sec("", str(a.get("label") or "account"), str(a.get("repo") or ""))
        d.add(f"🔪 killed <b>{int(a.get('killed') or 0)}</b> · "
              + ("⚠️ තව <b>%d</b> ඉතුරුයි" % left if left else "✅ clean"))
    if busy:
        d.add(note("Distributed job එක worker pool නැවැත්තුව නිසා fallback නොවී stop වෙයි."))
    if left_total:
        d.add(note("තත්පර කීපයකින් /workflow බලන්න — අවශ්‍ය නම් ආයෙ /End_Work."))
    return d.build()


def toast_card(*, icon: str, title_text: str, lines: Sequence[str] = (),
               hint: str = "") -> str:
    d = Doc()
    d.head(icon, title_text)
    if lines:
        d.quote(list(lines))
    if hint:
        d.add(note(hint))
    return d.build()


def event_card(*, icon: str, title_text: str, lines: Sequence[str] = (),
               hint: str = "", gauge_pct: Optional[float] = None) -> str:
    """Transient job notices (forwarding, probing, fallback, …)."""
    d = Doc()
    d.head(icon, title_text)
    if lines:
        d.quote(list(lines))
    if gauge_pct is not None:
        d.mono([gauge(gauge_pct, 14)])
    if hint:
        d.add(note(hint))
    return d.build()


# ───────────────────────── job result widgets ─────────────────────────
def _stat_grid(rows: Sequence[Tuple[str, str, str, str]]) -> List[str]:
    """ASCII-only aligned stat table: label · value · time · note."""
    out = []
    for label, value, time_s, note_s in rows:
        line = f"{str(label)[:5]:<5}{str(value)[:9]:>9}  {str(time_s)[:8]:>8}"
        if note_s:
            line += f"  {note_s}"
        out.append(line.rstrip())
    return out


def done_card(*, name: str, before: str, after: str, saved_pct: Optional[float],
              stats: Sequence[Tuple[str, str, str, str]] = (),
              extras: Sequence[str] = (), footer: str = "") -> str:
    d = Doc()
    d.head("✅", "JOB COMPLETE", "uploaded")
    delta = ""
    if saved_pct is not None:
        delta = f" · <b>{'−' if saved_pct >= 0 else '+'}{abs(saved_pct):.0f}%</b>"
    d.quote([f"🎬 <code>{esc(clip(name, 42))}</code>",
             f"📦 <code>{esc(before)}</code> → <code>{esc(after)}</code>{delta}"])
    if stats:
        d.mono(_stat_grid(stats))
    if extras:
        d.add(" · ".join(extras))
    return d.build(tail=footer or None)


def failed_card(*, name: str, error: str, hint: str = "") -> str:
    d = Doc()
    d.head("⚠️", "JOB FAILED")
    d.quote([f"🎬 <code>{esc(clip(name, 40))}</code>",
             f"❌ <code>{esc(clip(str(error or 'unknown error'), 280))}</code>"])
    d.add(note(hint or "Settings අඩු කරලා ආයෙ try කරන්න · /check බලන්න."))
    return d.build()


def cancelled_card(*, name: str = "", note_text: str = "Job එක නතර කළා · queue එක ඊළඟ එකට ගියා") -> str:
    d = Doc()
    d.head("❌", "CANCELLED")
    rows = [f"🎬 <code>{esc(clip(name, 40))}</code>"] if name else []
    rows.append(esc(note_text))
    d.quote(rows)
    return d.build()


def preview_card(*, title_text: str, filename: str, sections: Sequence[Tuple[str, str, Sequence[str]]],
                 footer: str = "", hint: str = "preview") -> str:
    """Metadata preview before the user picks a target."""
    d = Doc()
    d.head("🎬", title_text, hint)
    if filename:
        d.quote(f"📄 <code>{esc(clip(filename, 46))}</code>")
    for icon, label, rows in sections:
        if not rows:
            continue
        d.sec(icon, label)
        d.extend(rows)
    return d.build(tail=footer or None)


__all__ = [
    # helpers
    "esc", "clip", "text_len", "human_size", "human_size_short", "human_speed",
    "human_time", "human_age", "speed_txt", "bar", "pct_txt", "gauge",
    # primitives
    "sanitize", "truncate_html", "to_plain", "title", "section", "kv", "bullet", "rule",
    "quote", "mono", "chips", "onoff", "status_dot", "note", "Doc",
    # widgets
    "render_transfer", "render_encode", "render_dist",
    "home_card", "settings_card", "health_card", "queued_card", "queue_card",
    "workflow_account", "endwork_progress", "endwork_result", "toast_card",
    "event_card", "done_card", "failed_card", "cancelled_card", "preview_card",
]
