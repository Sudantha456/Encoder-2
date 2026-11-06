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

## Flood protection
- Download: userbot එකකට ඒකේම connections; එක userbot එකකට FloodWait ආවොත් ඒ lane එක විතරක් pause — chunks අනිත් userbots ගන්නවා. `UB_MAX_FLOOD_WAIT` (30s) ඉක්මවුවොත් ඒ userbot එක cooldown (`UB_FLOOD_COOLDOWN` 60s).
- Upload: flood අඩුම / cooldown නැති userbot එක; flood වුණොත් ඊළඟ userbot එකෙන් auto retry.
- Userbot path fail වුණොත් (forward / download / upload) job එක fail වෙන්නේ නෑ — bot එකෙන්ම කරනවා.

## Workers
`/settings → 👷 Workers`: 2, 4, 6, 8, 10, 12, 14, 16.

# 🌐 Distributed download (main + helper runners)

**Flow:** user → main bot → (forward) private channel → main runner file එක **analyze** කරලා bytes range හරියටම බෙදනවා (main + 4 helpers, එක account එකකට userbot එකක් බැගින්) → හැම runner එකක්ම තමන්ගේ range එක parallel download කරනවා → slices GitHub releases → helper assembler (හෝ main fallback) reassemble → keyframe parts → encode workers → encoded parts → **main account release** → main runner merge → user ගේ chat.

## Session බෙදීම (5 accounts)
`USER_STRINGS` එකේ 5 sessions: **#1 → main runner** relay, **#2..#5 → helper #1..#4** (DL_SESSION secret එක විදිහට bot එකෙන්ම auto-දෙනවා). එක session එක රනිං යන්ත දෙකක කවදාවත් පාවිච්චි වෙන්නේ නෑ (Telethon "session already in use" වලින් disconnect වෙනවා). 5ට වඩා sessions දුන්නොත් අතිරේක ඒවා main runner එකෙන් ගන්නවා. `DIST_DOWNLOAD=1` + `HELPER_TOKENS` තිබේ නම් විතරයි split වෙන්නේ — නැත්නම් පරණ විදිහටම (සියල්ල main runner එකේ).

**Dead-lane fallback:** helper එකක් bootstrap / dl pre-flight එකෙන් fail වුණොත් ඒකේ session එක **main runner එකට ආපහු ලැබෙනවා** (sessionplan.py — helpers bootstrap ඉවර වුණාට පස්සේ relay ගණන් ගන්නවා). Dead lane එකක් නිසා session එකක් නිකම් තියෙන්නේ නෑ. බෝට් start ordering: **helpers (bootstrap + pre-flight) → relay** — ඒ නිසා /check එකේ "Helper …: dl lane OFF — <හේතුව>" පේනවා නම්, ඒ session එක main relay එකේ on.

## Helper accounts (auto-setup on bot start)
හැම helper account එකකටම bot එකෙන්ම (`setup_helpers`):
- public repo `<login>/encoder-workers` (idempotent — තියෙනවා නම් නැවත හදන්නේ නෑ)
- worker files + workflows push: `encode-worker.yml` (encode), `dl-worker.yml` (download slices), `asm-worker.yml` (assemble/split)
- secrets auto: `BOT_TOKEN`, `API_ID`, `API_HASH`, `DL_SESSION`, `CHANNEL_NUM` (job එකකට එකවර)

Helper එකක් connect වෙන්නේ නැත්නම් (token/repo fail) **ඒක drop කරලා** ඉතිරි lanes එක්කම distributed download දිගටම යනවා. Helpers සියල්ල fail නම් — සාමාන්‍ය download (userbot relay / bot) ට fall back වෙනවා. Helper ට DL_SESSION දෙන්න බැරි වුණොත්, හෝ **dl pre-flight** එකේ channel resolve fail වුණොත් (මේ account එක channel එකේ member නෙවෙයි නම් වැඩේ මේකයි) ඒ helper එකේ **dl lane off** + හේතුව `/check` එකේ පේනවා — ඒ session එක main runner එකේ පාවිච්චි වෙනවා (encode worker විදිහට නම් වැඩ කරනවා).

**Pre-flight:** bot start වෙද්දී (helpers bootstrap වෙද්දී) හැම helper dl session එකක් සමඟම channel resolve පරීක්ෂාවක් කරනවා — invite link නම් auto-join වෙනවා. Fail නම් job එක run වෙන වෙලාවට කලින්ම **dl lane OFF** කරලා හේතුව `HELPER_PROBLEMS` වලට දමනවා — `/check` එකේ **Helper #N (…): dl lane OFF — …** විදිහට පේනවා. නිවැරදි කළාට පස්සේ `/check` (queue එක හිස් වෙලාවක) එවන්න — ආයෙමත් test වෙලා, සාර්ථක නම් dl lane on කරලා, session එකs ආයෙ බෙදෙනවා.

## Integrity — එක byte එකක්වත් miss වෙන්නේ නෑ
- `dlparts.byte_ranges(size, n, align=1MB)`: ranges gap/overlap නැතිව **[0, size)** සම්පූර්ණ cover කරනවා; `verify_ranges()` double-check; slice කිසිම වෙලාවක range මායිමෙන් පිටතට යන්නේ නෑ (GetFileRequest limit = `min(1MB, slice_remaining)`).
- Helper lane: 1MB chunks → slice file (`ftruncate` exact) → `dl_slice_<n>` asset (size-verify).
- Main එකේ settle loop: හැම slice එකකම release asset **size == expected** (byte-exact); හරි නැත්නම් main runner එක ඒ range එක Telegram එකෙන්ම re-download → verify → helper release එකට upload → ආයේ verify.
- Assembler (`asm_worker.py`, helper #k): slices download → **cat order-verify** (concatenated size == `TOTAL_SIZE`) → ffprobe → keyframe parts (`raw_part_N.mp4`) + `audio.mka` → own release `enc-<job>` → marker `asm_done.json` (bounds/parts/audio).
- Main: marker validate → **main එක full file එක hold කරන්නේ නෑ** (main slice upload කළාට පස්සේ delete) → workers raw parts assembler repo එකෙන් කෙළින්ම ගන්නවා → encode → encoded parts **worker own releases** → main fetch (+ **main repo release `enc-<job>` එකට upload**, merge ඉන්පස්සේ) → concat demuxer + audio mux → delivery.
- GitHub release asset ≈2GB limit: slice ≥ ~1.9GB නම් distributed download auto-disable (සාමාන්‍ය path).
- Test: `PATH=<ffmpeg+ffprobe> python3 -m unittest discover -s tests` — range-tiling asserts + **e2e** (fake `gh` releases → asm_worker → raw parts → concat → 10.00s output) + corrupt-slice rejection.

## Progress UI (Telegram)
`render_dl` — හැම account එකේම slice %/speed/done + **TOTAL speed** (ඔක්කොම lanes එකතුව, live), ETA, elapsed. Helper lanes "🧩 Slice N" messages (silent, parse_mode off) — ඔක්කොම finish/fail වුණාම delete.

## Env knobs
```
DIST_DOWNLOAD=1          # distributed download on/off
DIST_DL_MIN_BYTES=52428800   # මීට කුඩා file → සාමාන්‍ය download
DIST_DL_CONNECTIONS=12   # එක helper runner එකකට Telegram connections
DIST_DL_TIMEOUT=2700     # download phase උපරිම තත්පර
DIST_DL_STALE=180        # lane progress නතර වුණොත් (s) → failed
DIST_ASM_TIMEOUT=1500    # assembler marker බලාගෙන ඉන්න උපරිම (s)
```

## Notes / caveats
- Helper repos **public** — slices/parts GitHub එකේ visible වෙනවා (මේ project එකේ encode partsත් එහෙමයි). Private/sensitive content නම් helpers ඔබේ trusted accounts වෙන්න ඕනේ.
- Helpers file_reference එකකින් නොව, **channel message එක තමන්ගේ userbot එකෙන් කියවලා** පාවිච්චි කරනවා (stale file_reference ගැටලුව නෑ).
- Helper dl lanes **channel member (හෝ admin)** වෙන්න ඕනේ — admin දැම්ම කියලා ඔබ දන්නා account ටිකම නොවේ, sessions[1..5] වල අයත් channel එකේ ඉන්න ඕනේ. `CHANNEL_ID` invite link එකක් දුන්නොත් pre-flight එකෙන් helpers auto-join වෙනවා — id එකක් නම්, ඒ accounts channel එකට add කරන්න.
- එක session string එකෙන් clients දෙකක් ධාවනය වෙන්නේ නෑ — split logic implement කරලා තියෙනවා (උඩ බලන්න).
- Assembler fail/timeout → main runner slices reassemble කරලා (local) පරණ dist-encode path එකෙන් යනවා; upload/size වැරදි නම් job fail — නැවත retry කරන්න පුළුවන්.
