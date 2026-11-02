"""Formatting helpers + progress panels (HTML parse mode)."""
import html
import time
from collections import deque


def esc(s) -> str:
    return html.escape(str(s))


def human_size(n: float) -> str:
    n = float(n or 0)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024 or unit == "TB":
            return f"{n:.0f} B" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024


def human_size_short(n: float) -> str:
    return human_size(n).replace(" ", "")


def human_speed(bps: float) -> str:
    return f"{human_size(bps)}/s"


def human_time(sec: float) -> str:
    sec = max(0, int(sec or 0))
    h, r = divmod(sec, 3600)
    m, s = divmod(r, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def bar(pct: float, width: int = 20) -> str:
    pct = min(max(pct, 0.0), 100.0)
    filled = round(width * pct / 100)
    return "█" * filled + "░" * (width - filled)


def short_name(name: str, limit: int = 40) -> str:
    return name if len(name) <= limit else name[: limit - 1] + "…"


class SpeedTracker:
    """Sliding-window speed + ETA for download/upload."""

    def __init__(self, total: int = 0, window: float = 10.0):
        self.start = time.monotonic()
        self.total = total
        self.current = 0
        self.window = window
        self.samples = deque([(self.start, 0)])

    def update(self, current: int, total: int = 0):
        now = time.monotonic()
        self.current = current
        if total:
            self.total = total
        self.samples.append((now, current))
        while len(self.samples) > 2 and now - self.samples[0][0] > self.window:
            self.samples.popleft()

    @property
    def elapsed(self) -> float:
        return time.monotonic() - self.start

    @property
    def avg_speed(self) -> float:
        return self.current / self.elapsed if self.elapsed > 0 else 0.0

    @property
    def speed(self) -> float:
        (t0, c0), (t1, c1) = self.samples[0], self.samples[-1]
        if t1 - t0 < 0.5:
            return self.avg_speed
        return max(0.0, (c1 - c0) / (t1 - t0))

    @property
    def eta(self) -> float:
        sp = self.speed
        return (self.total - self.current) / sp if sp > 0 and self.total else 0.0

    @property
    def percent(self) -> float:
        return self.current / self.total * 100 if self.total else 0.0


def render_transfer(kind: str, tr: SpeedTracker, name: str) -> str:
    head = "📥 <b>Downloading</b>" if kind == "dl" else "📤 <b>Uploading</b>"
    return "\n".join([
        f"{head}\n<code>{esc(short_name(name))}</code>",
        "",
        f"<code>[{bar(tr.percent)}] {tr.percent:5.1f}%</code>",
        f"📦 <code>{human_size(tr.current)} / {human_size(tr.total)}</code>",
        f"⚡ Speed: <code>{human_speed(tr.speed)}</code>  (avg <code>{human_speed(tr.avg_speed)}</code>)",
        f"⏳ ETA: <code>{human_time(tr.eta)}</code>   ⏱ Elapsed: <code>{human_time(tr.elapsed)}</code>",
    ])


def render_encode(st, name: str, height: int, preset: str, codec: str) -> str:
    pct = st.percent
    lines = [
        f"🎬 <b>Encoding</b> → <b>{height}p</b>\n<code>{esc(short_name(name))}</code>",
        f"⚙️ <code>{codec} • {preset}</code>",
        "",
        f"<code>[{bar(pct)}] {pct:5.1f}%</code>",
        f"🎞 Video: <code>{human_time(st.out_time)} / {human_time(st.duration)}</code>",
        f"🚀 Speed: <code>{st.speed:.2f}x</code> realtime  •  <code>{st.fps:.0f} fps</code>",
    ]
    if st.est_total_frames:
        lines.append(f"🖼 Frames: <code>{st.frame:,} / ≈{st.est_total_frames:,}</code>")
    lines.append(f"⏳ ETA: <code>{human_time(st.eta)}</code>   ⏱ Elapsed: <code>{human_time(st.elapsed)}</code>")
    size_line = f"📦 Output so far: <code>{human_size(st.out_size)}</code>"
    if st.est_final_size:
        size_line += f"  (≈ final <code>{human_size(st.est_final_size)}</code>)"
    lines.append(size_line)
    return "\n".join(lines)


def _mini(pct: float, width: int = 8) -> str:
    filled = round(width * min(max(pct, 0), 100) / 100)
    return "▰" * filled + "▱" * (width - filled)


def render_dist(st, name: str, height: int, preset: str, codec: str) -> str:
    head = (f"🌐 <b>Distributed Encode</b> → <b>{height}p</b>\n<code>{esc(short_name(name))}</code>\n"
            f"⚙️ <code>{codec} • {preset} • {st.n_workers} workers</code>\n")
    if st.phase == "split":
        return head + "\n✂️ Video එක keyframes අනුව කොටස් වලට කඩනවා…"
    if st.phase == "upload":
        return head + f"\n☁️ කොටස් {len(st.parts)} Release එකට upload කරනවා…"
    if st.phase == "merge":
        return head + "\n🔗 <b>කොටස් merge කරනවා…</b>"
    labels = {
        "waiting": "⏳ runner එකක් බලාගෙන", "uploading": "☁️ result upload",
        "remote_done": "📥 fetching", "failed": "⚠️ main එකෙන් retry", "ready": "✅ done",
    }
    lines = [head, f"<code>[{bar(st.percent)}] {st.percent:5.1f}%</code>",
             f"🎞 Video: <code>{human_time(st.out_time)} / {human_time(st.duration)}</code>",
             f"🚀 Speed: <code>{st.speed:.2f}x</code> realtime (මුළු එකතුව) • <code>{st.fps:.0f} fps</code>"]
    if st.est_total_frames:
        lines.append(f"🖼 Frames: <code>{st.frame:,} / ≈{st.est_total_frames:,}</code>")
    lines.append(f"⏳ ETA: <code>{human_time(st.eta)}</code>   ⏱ Elapsed: <code>{human_time(st.elapsed)}</code>")
    size = f"📦 Output so far: <code>{human_size(st.out_size)}</code>"
    if st.est_final_size:
        size += f"  (≈ final <code>{human_size(st.est_final_size)}</code>)"
    lines += [size, "", "<b>Workers</b>"]
    for p in st.parts:
        if p.status == "running":
            txt = f"{_mini(p.pct)} {p.pct:3.0f}% • {p.speed:.1f}x"
        elif p.status == "local":
            txt = f"🏠 {_mini(p.pct)} {p.pct:3.0f}% • {p.speed:.1f}x"
        else:
            txt = labels.get(p.status, p.status)
        lines.append(f"<code>{p.idx + 1}</code> {txt}")
    return "\n".join(lines)
