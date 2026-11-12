"""
ui.py — live-progress engine + re-exported premium widget renderers.

All message *formatting* now lives in :mod:`widgets` (one visual language for
commands and job updates). This module keeps the speed tracker used by the
download/upload engines and re-exports the old public API so existing imports
(``from ui import render_transfer, human_size, …``) keep working unchanged.
"""
from __future__ import annotations

import time
from collections import deque

# Re-exports — old names still resolve here (bot.py / media.py rely on them).
from widgets import (  # noqa: F401
    Doc,
    bar,
    bullet,
    chips,
    clip,
    esc,
    gauge,
    human_age,
    human_size,
    human_size_short,
    human_speed,
    human_time,
    kv,
    mono,
    note,
    onoff,
    pct_txt,
    quote,
    render_dist,
    render_encode,
    render_transfer,
    rule,
    section,
    speed_txt,
    title,
)


def short_name(name: str, limit: int = 28) -> str:
    """Backwards-compatible short filename helper."""
    name = str(name or "")
    return name if len(name) <= limit else name[: max(0, limit - 1)] + "…"


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


__all__ = [
    "SpeedTracker", "short_name",
    "esc", "human_size", "human_size_short", "human_speed", "human_time",
    "human_age", "bar", "pct_txt", "gauge", "speed_txt", "clip", "Doc",
    "render_transfer", "render_encode", "render_dist",
    "title", "section", "kv", "bullet", "rule", "quote", "mono", "chips",
    "onoff", "note",
]
