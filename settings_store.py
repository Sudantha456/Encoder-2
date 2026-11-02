"""Per-user preset/codec settings (JSON file)."""
import json
import os
import threading

from config import (SETTINGS_FILE, DEFAULT_PRESET, DEFAULT_CODEC, PRESETS, CODECS,
                    WORKER_CHOICES, DEFAULT_WORKERS, QUEUE_RES_CHOICES)


class SettingsStore:
    def __init__(self, path: str = SETTINGS_FILE):
        self.path = path
        self._lock = threading.Lock()
        self._data = {}
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    self._data = json.load(f)
            except Exception:
                self._data = {}

    def get(self, user_id: int) -> dict:
        s = self._data.get(str(user_id), {})
        preset = s.get("preset", DEFAULT_PRESET)
        codec = s.get("codec", DEFAULT_CODEC)
        try:
            workers = int(s.get("workers", DEFAULT_WORKERS))
        except (TypeError, ValueError):
            workers = DEFAULT_WORKERS
        return {
            "preset": preset if preset in PRESETS else DEFAULT_PRESET,
            "codec": codec if codec in CODECS else DEFAULT_CODEC,
            "dist": s.get("dist", "auto") if s.get("dist", "auto") in ("auto", "on", "off") else "auto",
            "workers": workers if workers in WORKER_CHOICES else DEFAULT_WORKERS,
            "qres": s.get("qres", "ask") if s.get("qres", "ask") in QUEUE_RES_CHOICES else "ask",
        }

    def set(self, user_id: int, **kwargs):
        with self._lock:
            cur = self.get(user_id)
            cur.update({k: v for k, v in kwargs.items() if v})
            self._data[str(user_id)] = cur
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self._data, f, indent=2)
            os.replace(tmp, self.path)
