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

## Workers
`/settings → 👷 Workers`: 5, 10, 15, 20, 25, 30, 35, 40, 45, 50.

## 🎨 Premium HTML Widgets (`widgets.py`)

Telegram **HTML parse mode** එකෙන් හදපු එකම widget language එකක් — commands වලටත්, live job updates වලටත්.

| Where | Widget |
|---|---|
| `/start`, `/help` | hero card + modes + collapsible command list |
| `/settings` (+ CRF / Pixel / Preset panels) | metric cards, `blockquote expandable` choice lists, live status chips |
| `/check` | health card: relay lanes, helper accounts, runner pool |
| `/workflow` | per-account cards, run status board (status icons + age + parts) |
| `/queue` | running card + numbered waiting list |
| `/clear` | toast card |
| `/End_Work` | kill progress meter → per-account result report |
| job updates | analyzing · queued (#pos) · relay forward · download · probing · encode · distributed board · upload · delivering · done · failed · cancelled |

**Design rules**

- **Mobile first:** normal text is kept ~34 cells wide; wide/columnar data only lives inside `<pre>` and those blocks are ≤ 32 cells so nothing scrolls sideways on Android/iOS.
- **Desktop friendly:** native `<blockquote>` cards + collapsible panels render the same on Desktop/Web; no custom emoji ids, no colours (Telegram has none), only real HTML entities.
- **Premium look:** bold titles, `<code>` values, monospace gauges (`[██████░░░░░]  52.3%`), aligned stat grids (`DL/ENC/UP/TOTAL`), status pills (🟢/⏸/🔴).
- **Zero-error guarantee** (`widgets.sanitize()` → `truncate_html()` → `Doc.build()`):
  - only Telegram-supported tags are emitted; unknown tags are dropped, mis-nesting repaired, stray `&`/`<` escaped
  - every message is ≤ 3800 UTF-16 units (4096 hard limit), truncated at tag boundaries — never inside a tag, entity or surrogate pair
  - user content (file names, ffmpeg/GitHub errors) is always escaped → markup injection impossible
  - renderers tolerate `None`/missing fields, 0s, negative/NaN-ish values and 0…50 workers with all part states
  - `bot.reply_card()` / `bot.edit_card()` / `Live.set()` degrade to parse-free plain text if a client ever rejects the entities, so progress messages never go missing
- **Action-runner safe:** the distributed board reads `DistState`/`Part` defensively; GitHub failures render as a widget error card instead of raising, and `/workflow` + `/End_Work` keep working with 1…N helper accounts.

**Verify (no network / no Telegram login needed):**

```bash
python3 tests/test_widgets.py     # 2000+ checks incl. Telethon's real HTML parser
```
