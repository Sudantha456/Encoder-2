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

---

## ⚡ Speed & reliability (v2)

### 1. GitHub upload failures (`HTTP 408: Upload body timed out due to inactivity`) — fixed
`ghassets.py` (new) now transfers every Release asset (raw parts + encoded parts) in **chunks**:

| what | before | now |
|---|---|---|
| upload | whole file in 1 request | chunks (`GH_UPLOAD_CHUNK_MB`, default 24MB) uploaded 3 at a time |
| a stall | whole part = job fail | only that chunk retries (per-request timeout `GH_UPLOAD_TIMEOUT`, 6 attempts) |
| retry | uploads everything again | finished chunks are skipped (**resume**) |
| verify | none | `<file>.manifest` → download වුණු size එක verify කරනවා |
| last resort | job fail | part එක **main runner එකේම** encode කරනවා (`DIST_LOCAL_RESCUE=1`) |

Worker එකට `ghassets.py` copy වෙන්නේ bot start වෙද්දීම (helper repos auto-update).

### 2. Smooth progress
* `Live` progress editor: හැම update එකක්ම coalesce කරලා, `PROGRESS_MIN_EDIT_GAP` (1.2s) ට වඩා ඉක්මනින් edit නෑ → FloodWait නෑ, delay stack වෙන්නේ නෑ, අන්තිම state එක නැති වෙන්නේ නෑ.
* `PROGRESS_TICK` (0.5s): panel එකට state එක කියවන rate — bar එක smooth වෙන්න.
* Download/upload: `SpeedTracker.current_live` (extrapolation).
* Distributed: part එකක අන්තිම report එකෙන් පස්සේ `speed × elapsed` වලින් `live_out` extrapolate.

### 3. All qualities + single quality
* Preview එකේ **source resolution එක ඇතුළුව** තියෙන හැම quality එකක්ම බොත්තම් විදිහට (`🎯` = original). Upscale නෑ.
* Quality එකක් විතරයි තියෙනවා නම් බොත්තම් නැතුව ඒකෙන්ම ඉදිරියට යනවා.
* තෝරපු quality එකෙන්ම encode වෙනවා (`scale=…:H`, source එකට සමාන නම් re-scale නෑ).

### 4. Instant preview
* Telegram attributes (resolution/duration/size) වලින් **ක්ෂණිකව** preview; metadata probe එක background එකේ.
* Probe: head+tail chunks `PROBE_CONNECTIONS` (10) ක් හරහා **parallel** (sequential නෑ) + `PROBE_TIMEOUT` (20s).
* එකම video එක ආයෙ එව්වොත් `media_cache.json` එකෙන් **network නැතුව**.

### 5. Download speed
* `UB_DL_CONNECTIONS` default 6 → **8** (Telegram limit එක connection එකකට — connections වැඩි = speed වැඩි).
* **Duplicate chunks නෑ**: chunk එකක් queue/in-flight/finished — තුනෙන් එකක තියෙනවා නම් ආයෙ ඉල්ලන්නේ නෑ. Hedge එක දැන් adaptive (chunk time × 2.5) + queue හිස් වෙලා නම් විතරයි.
* **Weighted scheduler**: හැම lane එකකම throughput (EWMA) මැනලා 2s වලට වරක් `quota` අලුත් කරනවා → වේගවත් userbot එකට වැඩි chunks.
* FloodWait/error ආපු chunk එක queue එකට ආපහු → වෙන lane එකක් ඉක්මනින් ගන්නවා.

### Tunables (env / GitHub → Settings → Variables)
```
GH_UPLOAD_CHUNK_MB=24      GH_UPLOAD_PARALLEL=3     GH_UPLOAD_TIMEOUT=300
PROGRESS_UPDATE_INTERVAL=2 PROGRESS_TICK=0.5         PROGRESS_MIN_EDIT_GAP=1.2
UB_DL_CONNECTIONS=8        UB_UL_CONNECTIONS=8       PROBE_CONNECTIONS=10
DIST_LOCAL_RESCUE=1        DIST_RESCUE_AFTER=180     PROBE_CACHE=1
```
