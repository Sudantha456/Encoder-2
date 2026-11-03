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
