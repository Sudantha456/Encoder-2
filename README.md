# Encoder-2

# 👤 Userbot relay setup

**Flow:** user → main bot → (forward) private channel → userbot(s) download (multi-lane) → FFmpeg encode (runner / GitHub workers) → userbot upload → channel → main bot → user ගේ private chat (progress updates main bot එකෙන්).

## 1. Private channel
- Private **channel** (හෝ supergroup) එකක් හදන්න. ID එක `-100…` (උදා `-1001234567890`) — @userinfobot / @getidsbot වලින් ගන්න.
- **Main bot** → channel එකේ **Admin** (Post Messages + Delete Messages).
- **හැම userbot account එකක්ම** → channel එකේ member. Broadcast **channel** එකක නම් userbots ටත් **Admin (Post Messages)** ඕනේ (upload කරන්න). Supergroup එකක් නම් admin ඕනේ නෑ.
- Invite link එක `CHANNEL_ID` එකට දුන්නොත් userbots තනියම join වෙනවා.

## 2. Userbot sessions
`python gen_session.py` (හැම account එකකටම එක සැරයක්) → strings ටික:
```
USER_STRINGS=string1,string2,string3      # GitHub → Settings → Secrets → USER_STRINGS
CHANNEL_ID=-1001234567890                 # secret CHANNEL_ID
```
(`USER_STRING_1`, `USER_STRING_2`… විදිහටත් දෙන්න පුළුවන්.) Userbots වැඩි = download speed වැඩි + FloodWait අඩු.

## 3. පරීක්ෂා කිරීම
Bot start වෙද්දී self-test එකක් run වෙනවා (userbot connect → channel resolve → permissions → bot↔userbot round-trip).
Setup වැරදි නම් `ALLOWED_USERS` ට හේතුව සහිත message එකක් යනවා, bot එක **පරණ විදිහටම** (bot එකෙන් download/upload) වැඩ කරනවා. හදලා `/check` එවන්න.

## ⚡ Turbo Download Engine
- **Persistent Warm Pools & AuthKey Cache:** හැම userbot එකකටම DC අනුව warm Abridged-TCP connections (`FastTcpConnection` + 8MB buffers + `TCP_NODELAY` + `uvloop`) jobs අතරේත් සූදානම්ව තබා ගනී — cold handshake / `ExportAuthorization` ප්‍රමාදය ඉවත් කරයි.
- **Multi-IP & `media_only` DC Routing:** එකම DC එකේ dedicated `media_only` සහ සාමාන්‍ය load-balancer IPs සියල්ල හරහා connections ස්වයංක්‍රීයව බෙදා හරිනවා.
- **Streaming Swarm + Async `pwrite` Disk Engine:** පළමු userbot socket එක ready වූ සැණින් download ආරම්භ වන අතර, multi-thread `pwrite` disk pool එක නිසා network I/O කිසිවිටෙක block නොවේ.
- Speed tuning variables (optional): `UB_DL_CONNECTIONS=16`, `UB_TARGET_TOTAL_CONNECTIONS=80`, `DL_WORKERS_PER_CONN=5`.

## Flood protection
- Download: userbot එකකට ඒකේම connections; එක userbot එකකට FloodWait ආවොත් ඒ lane එක විතරක් pause — chunks අනිත් userbots ගන්නවා. `UB_MAX_FLOOD_WAIT` (30s) ඉක්මවුවොත් ඒ userbot එක cooldown (`UB_FLOOD_COOLDOWN` 60s).
- Upload: flood අඩුම / cooldown නැති userbot එක; flood වුණොත් ඊළඟ userbot එකෙන් auto retry.
- Userbot path fail වුණොත් (forward / download / upload) job එක fail වෙන්නේ නෑ — bot එකෙන්ම කරනවා.

## 🔀 Encoding Modes (`/settings` වලින් මාරු කළ හැක)

1. **📐 Resolution Mode (`res`)** — 1080p, 720p, 540p, 480p, 360p, 240p, 144p ලෙස resolution වෙනස් කිරීම (`H.264` හෝ `H.265`, Preset, Workers `5..50`, Smart Auto-Queue).
2. **🗜 Size-Reduce / Visually Lossless Mode (`compress`)** — Original resolution සහ visual quality වෙනස් නොවී **H.264 ➜ H.265 (`libx265`)** ලෙස file size එක පමණක් විශාල වශයෙන් අඩු කිරීම:
   - **🎯 CRF තේරීම (`14` Ultra … `32` Extreme):** සිංහල කෙටි විස්තර සහිතව (`CRF 18–20` Visually Lossless නිර්දේශය).
   - **🎨 Pixel Format තේරීම (`yuv420p` … `yuv444p16le`):** 8-bit / 10-bit / 12-bit / 16-bit සහ 4:2:0 / 4:2:2 / 4:4:4 formats සිංහල විස්තර සහිතව.
   - **🧬 x265 ON/OFF Toggles:** `B-Frames`, `SAO`, `Lookahead`, `AQ-Mode` කෙළින්ම ON/OFF කළ හැක.
   - **🎚 Preset & 👷 Distributed Workers (`5..50`):** Resolution Mode එකේ මෙන්ම single runner සහ GitHub Actions multi-worker distributed encoding දෙකම සහය දක්වයි.

## ☁️ GitHub JSON Persistent Settings (`user_settings.json`)
- `/settings` හි Resolution Mode සහ Size-Reduce Mode දෙකටම අදාළ සියලුම user selections `user_settings.json` ගොනුවේ වෙන වෙනම තැන්පත් වන අතර GitHub REST API හරහා repo එකේ `user_settings.json` වෙත ස්වයංක්‍රීයව sync වේ — bot එක restart වුණත් settings කිසිවක් reset නොවේ.

## Playback reliability
- Distributed workers encode parts independently, so each part can have different codec headers. Before MP4 assembly, parts are remuxed to MPEG-TS with their own in-band H.264/H.265 headers; distributed HEVC uses the `hev1` MP4 tag. This does not re-encode the video, but needs extra temporary disk space. Some players support `hvc1` but not `hev1`; try another player or single-runner encoding if needed.
- A whole-file decode check is no longer run after encoding: on long videos it took almost as long as the encode itself. The output is instead probed for a valid video stream (cheap) before the source is deleted and the file is uploaded.
- The static FFmpeg builds used by the workflows crash with `SIGSEGV` (exit `-11`, empty stderr) on *any* MPEG-TS input on some hosts: their statically linked glibc `dlopen()`s the host gconv module for the ISO-6937 service name. `ffenv.py` points `GCONV_PATH` at an empty directory for every ffmpeg/ffprobe subprocess, which avoids the crash. This affects the whole bot (metadata probing, thumbnails, verification and merge), not only distributed mode.
- Once all distributed parts are ready, their temporary raw splits are removed before merging to reduce peak disk use. If FFmpeg fails, the bot reports the merge stage, exit code/signal and free disk space; the Actions log also contains the stderr tail. A `SIGKILL` can indicate a resource limit, but is not by itself proof of an out-of-memory failure.
- Parts are scaled to the chosen resolution only (`-2:<height>`), so a part with a different aspect ratio keeps its own natural width instead of being padded or cropped. Telegram then adapts the player when the frame size changes.
- Telegram's video attributes carry the real output geometry: the uploader probes the finished file (falling back to `ffmpeg -i` when `ffprobe` is missing, then to the bot's computed target size), because `0x0` dimensions make Telegram draw the player in the wrong box. Scaled output forces square pixels (`setsar=1`) with Lanczos resampling, so the frame geometry matches the source instead of drifting (e.g. 1920×1080 → 854×480 would otherwise carry `SAR 1280:1281`). The Telegram video attributes use the real output dimensions, so the player box matches the file.
- Playback also depends on the chosen HEVC pixel format and the device. For widest compatibility use H.264 or H.265 `yuv420p` (8-bit, 4:2:0).

## Workers
`/settings → 👷 Workers`: 5, 10, 15, 20, 25, 30, 35, 40, 45, 50.
