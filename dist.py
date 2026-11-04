"""
dist.py — Distributed encoding: main runner ↔ GitHub Actions workers (queue-aware pool)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• Queue එකේ පළමු job එක download වෙන්න පටන් ගන්නකොටම WorkerPool එක settings වල workers ගාණට
  runners dispatch කරනවා → download එක ඉවර වෙද්දී runners ready.
• Workers queue එක ඉවර වෙනකම් hold වෙනවා (job එකෙන් job එකට). Queue හිස් වුණාම pool එක stop → workers exit.
• Download අතරතුර settings වල workers ගාන වැඩි කළොත් අලුත් runners ඉක්මනින් dispatch; අඩු කළොත්
  ඉතුරු workers hold (ඊළඟ job වලට ආයෙ වැඩි කළොත් ready).
Job එකකට:
  1. keyframe boundaries වල ffmpeg -c copy වලින් කොටස් N කට කඩනවා  (N = ඒ මොහොතේ settings workers)
  2. කොටස් temp Release එකකට upload → pool mailbox එකට job post
  3. Workers encode කරලා Release එකටම දානවා; progress Telegram message edit වලින්
  4. Main: කොටස් download කරලා concat + audio mux;  Worker fail / start නොවුණොත් ඒ කොටස main එකම encode (fallback)
"""
import asyncio
import base64
import contextlib
import json
import logging
import os
import re
import shutil
import time
import types
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import config
from config import PROGRESS_UPDATE_INTERVAL
from pathlib import Path as _P
