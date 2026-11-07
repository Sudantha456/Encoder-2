"""Formatting helpers + responsive mobile/desktop progress panels (HTML parse mode)."""
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


def bar(pct: float, width: int = 16) -> str:
    """16-cell block bar: `[████████░░░░░░░░] 100.0%` == 25 chars (never wraps on mobile)."""
    pct = min(max(pct, 0.0), 100.0)
    filled = round(width * pct / 100)
    return "█" * filled + "░" * (width - filled)


def short_name(name: str, limit: int = 28) -> str:
    return name if len(name) <= limit else name[: limit - 1] + "…"


def _fmt_pct(pct: float) -> str:
    """Exact 4-char percentage (`  0%` .. `100%`)."""
    p = min(100, max(0, int(round(pct or 0.0))))
    return f"{p:3d}%"


def _fmt_speed(sp: float) -> str:
    """Exact 5-char speed (` 4.7x`, `12.4x`, ` 115x`)."""
    sp = max(0.0, float(sp or 0.0))
    if sp >= 100.0:
        return f"{min(sp, 9999.0):4.0f}x"
    return f"{sp:4.1f}x"


class SpeedTracker:
    """
    Outlier-resistant sliding-window + EWMA speed tracker for download/upload.
    Eliminates momentary RPC/socket batch jitter while accurately tracking steady throughput.
    """

    def __init__(self, total: int = 0, window: float = 8.0):
        self.total = total
        self.window = window
        self.peak_speed = 0.0
        self._ewma_speed = 0.0
        self.reset()

    def reset(self):
        self.start = time.monotonic()
        self.current = 0
        self.peak_speed = 0.0
        self._ewma_speed = 0.0
        self.samples = deque([(self.start, 0)])

    def update(self, current: int, total: int = 0):
        now = time.monotonic()
        if current < self.current:
            self.current = current
            self.samples.clear()
            self.samples.append((now, current))
            return
        self.current = current
        if total:
            self.total = total
        # Coalesce ultra-close samples (<120ms apart) so burst callbacks don't skew dt
        if len(self.samples) >= 2 and (now - self.samples[-1][0]) < 0.12:
            self.samples[-1] = (now, current)
        else:
            self.samples.append((now, current))
        while len(self.samples) > 2 and (now - self.samples[0][0]) > self.window:
            self.samples.popleft()

        (t0, c0), (t1, c1) = self.samples[0], self.samples[-1]
        dt = t1 - t0
        if dt >= 0.35:
            raw = max(0.0, (c1 - c0) / dt)
            if self._ewma_speed <= 0.0:
                self._ewma_speed = raw
            else:
                self._ewma_speed = self._ewma_speed * 0.62 + raw * 0.38

        sp = self.speed
        if (now - self.start) >= 1.0 and sp > self.peak_speed:
            self.peak_speed = sp

    @property
    def elapsed(self) -> float:
        return time.monotonic() - self.start

    @property
    def avg_speed(self) -> float:
        return self.current / self.elapsed if self.elapsed > 0 else 0.0

    @property
    def speed(self) -> float:
        if self._ewma_speed > 0.0:
            return self._ewma_speed
        (t0, c0), (t1, c1) = self.samples[0], self.samples[-1]
        if t1 - t0 < 0.35:
            return self.avg_speed
        return max(0.0, (c1 - c0) / (t1 - t0))

    @property
    def eta(self) -> float:
        sp = self.speed
        return (self.total - self.current) / sp if sp > 0 and self.total else 0.0

    @property
    def percent(self) -> float:
        return self.current / self.total * 100 if self.total else 0.0


def render_transfer(kind: str, tr: SpeedTracker, name: str, badge: str = "") -> str:
    head = "📥 <b>Downloading</b>" if kind == "dl" else "📤 <b>Uploading</b>"
    if badge:
        head += f" • <i>{esc(badge)}</i>"
    lines = [
        f"{head}\n<code>{esc(short_name(name, 28))}</code>",
        "",
        f"<code>[{bar(tr.percent)}] {tr.percent:5.1f}%</code>",
        f"📦 <code>{human_size(tr.current)}</code> / <code>{human_size(tr.total)}</code>",
        f"⚡ <code>{human_speed(tr.speed)}</code> • avg <code>{human_speed(tr.avg_speed)}</code>",
    ]
    if tr.peak_speed > 0 and tr.elapsed >= 1.5:
        lines.append(f"📈 Peak <code>{human_speed(tr.peak_speed)}</code>")
    lines.append(f"⏳ <code>{human_time(tr.eta)}</code> • ⏱ <code>{human_time(tr.elapsed)}</code>")
    return "\n".join(lines)


def _compact_codec(codec: str) -> str:
    c = str(codec or "")
    if c == "libx265":
        return "H.265"
    if c == "libx264":
        return "H.264"
    return c


def _compact_extra(extra_info: str) -> str:
    if not extra_info:
        return ""
    s = extra_info.replace("CRF ", "CRF")
    for full, short in (
        ("yuv420p10le", "10b"),
        ("yuv422p10le", "422-10b"),
        ("yuv444p10le", "444-10b"),
        ("yuv420p12le", "12b"),
        ("yuv420p16le", "16b"),
        ("yuv420p", "8b"),
    ):
        s = s.replace(full, short)
    return s


def render_encode(
    st,
    name: str,
    height: int,
    preset: str,
    codec: str,
    *,
    target_label: str = "",
    extra_info: str = "",
) -> str:
    pct = st.percent
    t_lbl = (target_label or (f"{height}p" if height else "Original")).replace(" (Original)", "")
    ext_s = _compact_extra(extra_info)
    cfg_txt = f"{_compact_codec(codec)} • {preset}" + (f" • {ext_s}" if ext_s else "")
    lines = [
        f"🎬 <b>Encoding</b> → <b>{esc(t_lbl)}</b>\n<code>{esc(short_name(name, 27))}</code>",
        f"⚙️ <code>{esc(short_name(cfg_txt, 26))}</code>",
        "",
        f"<code>[{bar(pct)}] {pct:5.1f}%</code>",
        f"🎞 <code>{human_time(st.out_time)}</code> / <code>{human_time(st.duration)}</code>",
        f"🚀 <code>{st.speed:.2f}x</code> • <code>{st.fps:.0f} fps</code>",
    ]
    if st.est_total_frames:
        lines.append(f"🖼 <code>{st.frame:,}</code> / ≈<code>{st.est_total_frames:,}</code>")
    lines.append(f"⏳ <code>{human_time(st.eta)}</code> • ⏱ <code>{human_time(st.elapsed)}</code>")
    size_line = f"📦 <code>{human_size(st.out_size)}</code>"
    if st.est_final_size:
        size_line += f" / ≈<code>{human_size(st.est_final_size)}</code>"
    lines.append(size_line)
    return "\n".join(lines)


def _render_merge(st) -> str:
    n = len(st.parts)
    lines = [f"✅ කොටස් <b>{n}/{n}</b> හරි • 🔗 <b>Merge වෙනවා…</b>", ""]
    if getattr(st, "merge_step", "") == "audio":
        lines.append(f"🔊 Audio track සකසනවා… <code>{human_time(st.merge_elapsed)}</code>")
        return "\n".join(lines)
    pct = st.merge_pct
    sp = st.merge_speed
    left = (st.duration - st.merge_out) / sp if sp > 0 else 0.0
    lines += [
        f"<code>[{bar(pct)}] {pct:5.1f}%</code>",
        f"🎞 <code>{human_time(st.merge_out)}</code> / <code>{human_time(st.duration)}</code>",
        f"🚀 <code>{sp:.1f}x</code> • 📦 <code>{human_size(st.merge_size)}</code>",
        f"⏳ <code>{human_time(left)}</code> • ⏱ <code>{human_time(st.merge_elapsed)}</code>",
    ]
    return "\n".join(lines)


def _part_row_single(p) -> str:
    """Exact 24-char 1-column row for <=10 workers using strictly 1-cell glyphs."""
    num = f"{(p.idx + 1) % 100:02d}"
    if p.status == "running":
        return f"{num} [{bar(p.pct, 8)}] {_fmt_pct(p.pct)} {_fmt_speed(p.speed)}"
    state_rows = {
        "ready":       f"{num} [████████] 100%  DONE",
        "uploading":   f"{num} [████████]  99%  save",
        "remote_done": f"{num} [████████]  99%  pull",
        "downloading": f"{num} [░░░░░░░░]   0%  load",
        "waiting":     f"{num} [░░░░░░░░]   ..  wait",
        "held":        f"{num} [░░░░░░░░]   --  held",
        "failed":      f"{num} [░░░░░░░░]   !! retry",
    }
    if p.status in state_rows:
        return state_rows[p.status]
    st_txt = str(p.status)[:10]
    return f"{num} [░░░░░░░░] {st_txt:>10s}"


def _part_cell_compact(p) -> str:
    """
    Exact 13-char compact cell for 2-column 11..50 worker board.
    `cell_left (13) + ' │ ' (3) + cell_right (13) == 29` pure 1-cell monospace chars:
    fits 100% single-line without wrapping or scrolling on all mobile & desktop clients.
    """
    num = f"{(p.idx + 1) % 100:02d}"
    if p.status == "running":
        return f"{num} {_fmt_pct(p.pct)} {_fmt_speed(p.speed)}"
    compact_states = {
        "ready":       f"{num} 100%  DONE",
        "uploading":   f"{num}  99%  save",
        "remote_done": f"{num}  99%  pull",
        "downloading": f"{num}   0%  load",
        "waiting":     f"{num}   ..  wait",
        "held":        f"{num}   --  held",
        "failed":      f"{num}   !! retry",
    }
    if p.status in compact_states:
        return compact_states[p.status]
    return f"{num} {str(p.status)[:10]:>10s}"


def render_dist(
    st,
    name: str,
    height: int,
    preset: str,
    codec: str,
    *,
    target_label: str = "",
    extra_info: str = "",
) -> str:
    t_lbl = target_label or (f"{height}p" if height else "Original")
    t_lbl_short = t_lbl.replace(" (Original)", "")
    ext_s = _compact_extra(extra_info)
    cfg_parts = [_compact_codec(codec), preset]
    if ext_s:
        cfg_parts.append(ext_s)
    cfg_parts.append(f"{st.n_workers}w")
    cfg_txt = "•".join(cfg_parts) if len(" • ".join(cfg_parts)) > 26 else " • ".join(cfg_parts)

    head = (
        f"🌐 <b>Dist Encode</b> → <b>{esc(t_lbl_short)}</b>\n"
        f"<code>{esc(short_name(name, 27))}</code>\n"
        f"⚙️ <code>{esc(short_name(cfg_txt, 26))}</code>\n"
    )
    if st.phase == "split":
        return head + "\n✂️ Video එක keyframes අනුව කොටස් වලට කඩනවා…"
    if st.phase == "upload":
        return head + f"\n☁️ කොටස් {len(st.parts)} Release එකට upload කරනවා…"
    if st.phase == "merge":
        return head + "\n" + _render_merge(st)

    n_total = len(st.parts)
    n_done = sum(1 for p in st.parts if p.ready)
    n_run = sum(1 for p in st.parts if p.status == "running" and not p.ready)
    n_xfer = sum(1 for p in st.parts if p.status in ("downloading", "uploading", "remote_done") and not p.ready)
    n_wait = n_total - n_done - n_run - n_xfer

    summary_bits = [f"<b>{n_done}/{n_total}</b>"]
    if n_run:
        summary_bits.append(f"▶️{n_run}")
    if n_xfer:
        summary_bits.append(f"☁️{n_xfer}")
    if n_wait:
        summary_bits.append(f"⏳{n_wait}")

    sp_txt = f"{st.speed:.1f}x" if st.speed >= 10 else f"{st.speed:.2f}x"
    lines = [
        head,
        f"<code>[{bar(st.percent)}] {st.percent:5.1f}%</code>",
        f"🎞 <code>{human_time(st.out_time)}</code> / <code>{human_time(st.duration)}</code>",
        f"🚀 <code>{sp_txt}</code> • <code>{st.fps:.0f} fps</code>",
    ]
    if st.est_total_frames:
        lines.append(f"🖼 <code>{st.frame:,}</code> / ≈<code>{st.est_total_frames:,}</code>")
    lines.append(f"⏳ <code>{human_time(st.eta)}</code> • ⏱ <code>{human_time(st.elapsed)}</code>")
    size = f"📦 <code>{human_size(st.out_size)}</code>"
    if st.est_final_size:
        size += f" / ≈<code>{human_size(st.est_final_size)}</code>"
    lines += [size, "", f"👷 {' • '.join(summary_bits)}"]

    if n_total <= 10:
        w_rows = [_part_row_single(p) for p in st.parts]
        lines.append("<pre>" + esc("\n".join(w_rows)) + "</pre>")
    else:
        cells = [_part_cell_compact(p) for p in st.parts]
        grid_rows = []
        for i in range(0, len(cells), 2):
            if i + 1 < len(cells):
                grid_rows.append(f"{cells[i]} │ {cells[i + 1]}")
            else:
                grid_rows.append(cells[i])
        lines.append("<pre>" + esc("\n".join(grid_rows)) + "</pre>")
    return "\n".join(lines)
