"""
dlparts.py — Download byte-range planning (SHARED, pure logic)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Distributed download එකේදී file එකේ bytes EXACT ලෙස බෙදනවා:
  • gap නැත — හැම byte එකක්ම හරියටම එක lane එකකට
  • overlap නැත — එක byte එකවත් දෙතැනකට බෙදෙන්නේ නෑ
  • අවසන් byte එක දක්වා සම්පූර්ණයි

භාවිතා කරන align එක (1MB) නිසා Telegram GetFileRequest ආරක්ෂිතයි;
align=1 power-of-two නොවේ නම් සාමාන්‍ය exact byte split (testing).
"""
from typing import List, Tuple


def byte_ranges(total_size: int, n: int, align: int = 1024 * 1024) -> List[Tuple[int, int]]:
    """total_size bytes → [start, end) කැබලි n ක්. යුගල එකවර 0 නැත (හැම lane එකකටම > 0 හෝ 0-ලંબાයි range)."""
    if total_size < 0:
        raise ValueError("total_size negative")
    n = max(1, int(n))
    if align <= 1:
        base, rem = divmod(total_size, n)
        out, pos = [], 0
        for i in range(n):
            ln = base + (1 if i < rem else 0)
            out.append((pos, pos + ln))
            pos += ln
        return out
    blocks = -(-total_size // align)            # ceil — අවසන් (partial) block එක ගණනට ඇතුළත්
    base, rem = divmod(blocks, n)
    out, pos = [], 0
    for i in range(n):
        b = base + (1 if i < rem else 0)
        start = pos
        pos = min(pos + b * align, total_size)
        out.append((start, pos))
    return out


def verify_ranges(ranges: List[Tuple[int, int]], total_size: int) -> bool:
    """Ranges list එක [0, total_size) හරියටම cover කරනවාද — ආරක්ෂිත double-check."""
    if not ranges:
        return total_size == 0
    pos = 0
    for s, e in ranges:
        if s != pos or e < s:
            return False
        pos = e
    return pos == total_size


def range_requests(start: int, end: int, file_end: int, chunk_size: int = 1024 * 1024):
    """(offset, request_limit, expected_bytes) ටික yield කරනවා — 4096-aligned requests.

    Telegram getFileRequest limits 4096 ගුණාකාර විය යුතුයි; interior chunks ඒවා 1MB-aligned වගේම
    4096-divisible. absolute EOF එකේ පමණක් 1MB නොගියයි — එතන request එක 4096-පේළියට උඩ ඉල්ලලා
    (clamped) expected bytes ගන්නවා. දත්ත කැපුවේ caller එකට — ලැබුණු bytes want ට trim කරන්න.
    """
    for off in range(start, end, chunk_size):
        lim = min(chunk_size, end - off)
        if lim % 4096 == 0:
            yield off, lim, lim
            continue
        if off + lim == file_end:                    # absolute EOF chunk — aligned-up request, clamp on write
            yield off, -(-lim // 4096) * 4096, lim
        else:                                        # 1MB-aligned plans වල මේ play එකට එන්නේ නෑ
            yield off, lim, lim
