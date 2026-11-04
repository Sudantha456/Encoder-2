"""
config.py — Encoder bot settings
fast_downloader.py / fast_uploader.py දෙකත් මේ PROGRESS_UPDATE_INTERVAL එකම import කරනවා.
"""
import os
import re

# ── Telegram ──────────────────────────────────────────────
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
SESSION_NAME = os.getenv("SESSION_NAME", "encoder_bot")

# හිස්නම් හැමෝටම, නැත්නම් "123,456" වගේ user id (comma separated)
ALLOWED_USERS = [int(x) for x in os.getenv("ALLOWED_USERS", "").split(",") if x.strip()]

# ── 🔁 Progress update interval (තත්පර) — මෙතනින් වෙනස් කරන්න ──
PROGRESS_UPDATE_INTERVAL = float(os.getenv("PROGRESS_UPDATE_INTERVAL", "3"))

# ── Paths ─────────────────────────────────────────────────
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "downloads")
SETTINGS_FILE = os.getenv("SETTINGS_FILE", "user_settings.json")

# ── Encoding ──────────────────────────────────────────────
DEFAULT_PRESET = "ultrafast"
DEFAULT_CODEC = "libx264"

PRESETS = ["ultrafast", "superfast", "veryfast", "faster", "fast",
           "medium", "slow", "slower", "veryslow"]
CODECS = {"libx264": "H.264 (libx264)", "libx265": "H.265 (libx265)"}

PRESET_INFO = {
