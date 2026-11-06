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
    "ultrafast": "⚡ වේගවත්ම • Quality අඩු • File size ඉතා විශාලයි",
    "superfast": "🚀 ඉතා වේගවත් • size විශාලයි",
    "veryfast":  "🏃 වේගවත් • size ටිකක් විශාලයි",
    "faster":    "💨 වේගවත් • සාමාන්‍ය ට වඩා හොඳයි",
    "fast":      "👍 සාමාන්‍ය ට වඩා වේගවත්",
    "medium":    "⚖️ FFmpeg default setting එක",
    "slow":      "🐌 මන්දගාමී • size අඩුයි • quality හොඳයි",
    "slower":    "🐢 ඉතා මන්දගාමී • size තව අඩුයි",
    "veryslow":  "🦥 මන්දගාමීම • උපරිම quality • අඩුම size",
}

RESOLUTIONS = [1080, 720, 540, 480, 360, 240, 144]            # buttons වල පෙන්වන්නේ (short side)
CRF = {"libx264": 23, "libx265": 28}     # quality (අඩු = හොඳයි)
AUDIO_BITRATE = {1080: 160, 720: 128, 540: 112, 480: 96, 360: 80, 240: 64, 144: 48}   # kbps

# 🛡 Size guard: output bitrate එක source එකේ bitrate එක (resolution අනුව scale කරලා) ඉක්මවන්නේ නෑ.
# සමහර වෙලාවට (ultrafast වගේ) 20MB file එක 70MB වෙන එක වළක්වනවා. False කළොත් pure CRF.
SIZE_GUARD = os.getenv("SIZE_GUARD", "1") == "1"
SIZE_GUARD_RATIO = float(os.getenv("SIZE_GUARD_RATIO", "0.7"))   # 0.7 = source bitrate එකෙන් 70% (≈30% කුඩා). අඩු කළොත් තවත් කුඩා
MIN_GUARD_KBPS = 120

# ── 🚀 Speed: connections × workers (වැඩි = වේගවත්, නමුත් FloodWait වැඩි වෙන්න පුළුවන්) ──
DL_CONNECTIONS = int(os.getenv("DL_CONNECTIONS", "6"))
DL_WORKERS_PER_CONN = int(os.getenv("DL_WORKERS_PER_CONN", "4"))
UL_CONNECTIONS = int(os.getenv("UL_CONNECTIONS", "6"))
UL_WORKERS_PER_CONN = int(os.getenv("UL_WORKERS_PER_CONN", "4"))

# ── 🌐 Distributed encoding (GitHub Actions workers) ──
# Workers ගාන දැන් settings (/settings → 👷 Workers) වලින් — 2…16. මේ default එක විතරයි.
WORKERS_PER_ACCOUNT = 10                      # GitHub account එකකින් workers 10 යි (main + helpers)
WORKER_CHOICES = list(range(5, 51, 5))      # 5,10,…,50  (/settings → 👷 Workers)
MAX_WORKERS = max(WORKER_CHOICES)
DEFAULT_WORKERS = int(os.getenv("DIST_WORKERS", "10"))
if DEFAULT_WORKERS not in WORKER_CHOICES:
    DEFAULT_WORKERS = 10

# 🤝 Helper GitHub accounts (වෙනම accounts වල free runners). HELPER_TOKENS = tokenA,tokenB,tokenC,tokenD
# Token = classic PAT (scopes: repo + workflow). Bot එක ඒ account එකේ public repo එකක් තනියම හදලා worker files + BOT_TOKEN secret දානවා.
HELPER_TOKENS = [t for t in re.split(r"[,\s]+", os.getenv("HELPER_TOKENS", "").strip()) if t]
HELPER_REPO = os.getenv("HELPER_REPO", "encoder-workers")
DIST_WORKFLOW = os.getenv("DIST_WORKFLOW", "encode-worker.yml")
DIST_AUTO_MIN_SECONDS = int(os.getenv("DIST_AUTO_MIN_SECONDS", "240"))  # auto mode: single-runner encode මීට වැඩි නම් විතරයි
DIST_START_TIMEOUT = int(os.getenv("DIST_START_TIMEOUT", "10"))    # runner එක start නොවුණොත් part එක held queue එකට යනවා
DIST_STANDBY = int(os.getenv("DIST_STANDBY", "0"))                # optional extra idle runners — 0 = off
STANDBY_BASE = 100                                                 # standby worker index (worker.py එකේත් 100)
DIST_STALE_SECONDS = int(os.getenv("DIST_STALE_SECONDS", "180"))   # runner progress නතර වුණොත් part එක held queue එකට
DIST_PROGRESS_CHAT = os.getenv("DIST_PROGRESS_CHAT", "")           # හිස් = user ගේ chat එකම (messages පස්සේ delete වෙනවා)

# ── 🌐 Distributed DOWNLOAD (main runner + helper runners, byte-exact range split) ──
# File එක main runner එක analyze කරලා — bytes range හරියටම account/runner 5ට බෙදනවා (ගැප්/අතිපිහිටුමක් නෑ).
# Helper runner එකක් final කළාම ඒ slice එක own repo release `enc-dl-<job>` එකට upload →
# helper assembler එකක් (හෝ main fallback) reassemble (cat) + verify → keyframe parts → encode workers.
DIST_DOWNLOAD = os.getenv("DIST_DOWNLOAD", "1") == "1"
DIST_DL_MIN_BYTES = int(os.getenv("DIST_DL_MIN_BYTES", str(50 * 1024 * 1024)))   # මීට කුඩා නම් සාමාන්‍ය download
DIST_DL_CONNECTIONS = int(os.getenv("DIST_DL_CONNECTIONS", "12"))   # එක helper runner එකකට connections
DIST_DL_TIMEOUT = int(os.getenv("DIST_DL_TIMEOUT", "2700"))    # download phase උපරිම කාලය (s)
DIST_DL_STALE = int(os.getenv("DIST_DL_STALE", "180"))         # lane එකක් progress නතර කළොත් dead
DIST_ASM_TIMEOUT = int(os.getenv("DIST_ASM_TIMEOUT", "1500"))  # helper assembler බලාගෙන ඉන්න උපරිම (s)

# ── 📋 Queue ──
QUEUE_MAX = int(os.getenv("QUEUE_MAX", "20"))      # එකවර queue එකේ තියෙන්න පුළුවන් උපරිම videos
QUEUE_RES_CHOICES = ["ask", "smart"] + [str(r) for r in RESOLUTIONS]   # auto-queue: "ask" = බොත්තම්, "smart" = source එකට අඩුවෙන් තියෙන හොඳම resolution එක

MAX_UPLOAD_BYTES = 2000 * 1024 * 1024    # bot upload limit ≈ 2GB

# Preview එකට metadata probe කරන bytes (head + tail)
PROBE_HEAD_CHUNKS = 6      # 6 × 512KB = 3MB
PROBE_TAIL_CHUNKS = 12     # 12 × 512KB = 6MB


# ═════════════════ 👤 Userbot relay (private channel හරහා වේගවත් download/upload) ═════════════════
# Flow: user → main bot → (forward) private channel → userbot(s) download → encode → userbot upload
#       → channel → main bot → user ගේ private chat එකට.
#
# USER_STRINGS = Telethon StringSession එකක් හෝ කිහිපයක් (comma / newline වලින් වෙන් කරන්න):
#     USER_STRINGS=session1,session2,session3
# පරණ විදිහටත් වැඩ:  USER_STRING_1, USER_STRING_2, USER_STRING_3 …  (හෝ USER_STRING)
def _collect_user_strings() -> list:
    raw = [os.getenv("USER_STRINGS", ""), os.getenv("USER_STRING", "")]
    numbered = sorted(((int(k.rsplit("_", 1)[1]), v) for k, v in os.environ.items()
                       if re.fullmatch(r"USER_STR?ING_\d+", k) and v.strip()), key=lambda x: x[0])
    raw += [v for _, v in numbered]
    out = []
    for blob in raw:
        for part in re.split(r"[,\s]+", blob.strip()):
            if part and part not in out:
                out.append(part)
    return out


USER_STRINGS = _collect_user_strings()

# Private channel: "-1001234567890" (හොඳම)  |  "@username"  |  "https://t.me/+AbCdEf…" invite link
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()
CHANNEL_CLEANUP = os.getenv("CHANNEL_CLEANUP", "1") == "1"      # delivery ඉවර වුණාම channel එකේ messages මකනවා

UB_DL_CONNECTIONS = int(os.getenv("UB_DL_CONNECTIONS", str(DL_CONNECTIONS)))   # එක userbot එකකට connections
UB_UL_CONNECTIONS = int(os.getenv("UB_UL_CONNECTIONS", str(UL_CONNECTIONS)))
UB_MAX_FLOOD_WAIT = int(os.getenv("UB_MAX_FLOOD_WAIT", "30"))   # මීට වැඩි FloodWait එකක් ආවොත් ඒ userbot එක පැත්තකට දාලා තව එකක් use කරනවා
UB_FLOOD_COOLDOWN = int(os.getenv("UB_FLOOD_COOLDOWN", "60"))   # FloodWait ආපු userbot එක ඊළඟ jobs වලට මෙච්චර වෙලාවක් skip
