"""
sessionplan.py — Userbot sessions  බෙදීම (pure logic, telethon නැත)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
5 Telegram user accounts බෙදන ක්‍රමය:
  • session[0] → main runner (relay) — හැම විටම
  • session[1..N] → helper runners (DL_SESSION) — එක account එකක් එක runner එකකට
  • dead helper lane (bootstrap / pre-flight fail) නම් ඒ session එක main runner එකේ භාවිතා වෙනවා
    — කිසිම session එකක් අයාළේ යන්නේ නෑ, කිසිවෙකුත් දෙතැනකට බෙදී යන්නේ නෑ
"""
from typing import List, Sequence


def plan_main_sessions(sessions: Sequence[str], dist_download: bool, helper_tokens: Sequence[str],
                       helper_dl: Sequence[bool], helper_sess: Sequence[str],
                       helpers_setup_done: bool) -> List[str]:
    sessions = list(sessions)
    if not (dist_download and helper_tokens) or len(sessions) <= 1:
        return sessions
    if not helpers_setup_done:                         # bootstrap තාම නෑ — safe default: split
        return sessions[:1] + sessions[5:]
    claimed = {s for s, ok in zip(helper_sess, helper_dl) if s and ok}
    pool = [sessions[0]]
    pool += [s for s, ok in zip(helper_sess, helper_dl) if s and not ok]   # dead helper sessions → main
    pool += [s for s in sessions if s not in claimed and s not in pool]    # ඉතිරි (extra) sessions → main
    return pool
