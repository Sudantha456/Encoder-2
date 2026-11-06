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


_FRAG = 1024 * 1024


def _largest_pow2_le(x: int) -> int:
    return 1 << (x.bit_length() - 1)


def range_requests(start: int, end: int, file_end: int, chunk_size: int = 1024 * 1024):
    """(offset, request_limit, expected_bytes) — Telegram getFile නීති 4ම සපුරන requests:

      1) offset % 4096 == 0        2) limit % 4096 == 0        3) 1048576 % limit == 0
      4) [offset, offset+limit) එකම 1MB fragment එකේ රැඳිය යුතුයි

    Interior 1MB chunks හැමදාම හරි. Partial/tail chunk එක power-of-two divisors වලට කඩලා
    fragment මතනවා; EOF එකේ <4096 ඉතුරු උනොත් 4096 overshoot + caller clamp (ලැබුණු bytes want ට trim).
    """
    for off in range(start, end, chunk_size):
        lim = min(chunk_size, end - off)
        pos, remaining = off, lim
        while remaining > 0:
            space = min(remaining, (pos // _FRAG + 1) * _FRAG - pos)   # fragment එක මතනවා, file_end ඉක්මෙන්නේ නෑ
            if space >= 4096:
                req = _largest_pow2_le(space)      # 4096 ≤ req ≤ 1MB, power of two ⇒ 1MB % req == 0
                yield pos, req, min(req, remaining)
                pos += req
                remaining -= req
            else:                                   # <4096 ඉතුරු = absolute EOF — overshoot (clamp on write)
                yield pos, 4096, remaining
                remaining = 0
