"""
fast_conn.py — High-Throughput Multi-IP Persistent MTProto Connection Pool Engine
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• FastTcpConnection (Abridged MTProto): CRC32 + 1MB buffer copy overhead ඉවත් කර,
  8MB StreamReader window + TCP_NODELAY + auto-tuned TCP RCVBUF මඟින් full BDP speed ලබා දෙයි.
• Multi-IP & media_only DC routing: එකම DC එකේ media_only සහ සාමාන්‍ය load-balancer IPs සියල්ල හරහා
  connections බෙදා හරිනවා (තනි IP bottleneck / rate-limit වළක්වනවා).
• Proper MTProto session init: හැම extra sender එකක්ම InvokeWithLayer + InvokeWithoutUpdates + InitConnection
  මඟින් initialize කරනවා (server update spam නැහැ, BadServerSalt / CONNECTION_NOT_INITED නැහැ).
• Persistent Warm Pool + AuthKey Cache: userbot × DC auth keys සහ warm TCP connections jobs අතරේ
  රඳවා තබා ගන්නා නිසා cold handshake / ExportAuthorization FloodWait නැතිව ක්ෂණිකව උපරිම වේගය ලැබේ.
"""
import asyncio
import contextlib
import logging
import socket
import sys
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

from telethon.errors import AuthKeyError, FloodWaitError
from telethon.network.connection.tcpabridged import ConnectionTcpAbridged
from telethon.network.connection.tcpfull import ConnectionTcpFull

import config as _cfg

logger = logging.getLogger("FastConn")

# 8 MB StreamReader high-water mark: prevents asyncio from calling pause_reading()
# every 128 KB during 1 MB pipelined chunk downloads, allowing OS TCP rmem auto-tuning
# to open multi-megabyte receive windows across high-RTT links.
STREAM_READER_LIMIT = 8 * 1024 * 1024
WRITE_BUF_HIGH = 2 * 1024 * 1024
WRITE_BUF_LOW = 512 * 1024
NON_LINUX_SOCK_BUF = 2 * 1024 * 1024
LINUX_NOTSENT_LOWAT = 512 * 1024
CONNECT_TIMEOUT = 10.0
INIT_TIMEOUT = 12.0


def _tune_socket(writer: Optional[asyncio.StreamWriter]) -> None:
    if writer is None:
        return
    transport = getattr(writer, "transport", None)
    if transport is not None:
        with contextlib.suppress(Exception):
            transport.set_write_buffer_limits(high=WRITE_BUF_HIGH, low=WRITE_BUF_LOW)
    sock = writer.get_extra_info("socket")
    if sock is None:
        return
    with contextlib.suppress(Exception):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    with contextlib.suppress(Exception):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    # Linux TCP keepalive probes so warm pooled connections stay alive across NAT
    for opt_name, val in (("TCP_KEEPIDLE", 30), ("TCP_KEEPINTVL", 10), ("TCP_KEEPCNT", 3)):
        opt = getattr(socket, opt_name, None)
        if opt is not None:
            with contextlib.suppress(Exception):
                sock.setsockopt(socket.IPPROTO_TCP, opt, val)
    if sys.platform.startswith("linux"):
        # CRITICAL on Linux: never set SO_RCVBUF / SO_SNDBUF manually — doing so sets
        # SOCK_RCVBUF_LOCK / SOCK_SNDBUF_LOCK, disabling kernel tcp_rmem/tcp_wmem + BBR
        # dynamic BDP auto-tuning and causing upload bufferbloat spikes & tcp_mem window drops.
        quickack = getattr(socket, "TCP_QUICKACK", 12)
        notsent_lowat = getattr(socket, "TCP_NOTSENT_LOWAT", 25)
        with contextlib.suppress(Exception):
            sock.setsockopt(socket.IPPROTO_TCP, quickack, 1)
        with contextlib.suppress(Exception):
            sock.setsockopt(socket.IPPROTO_TCP, notsent_lowat, LINUX_NOTSENT_LOWAT)
    else:
        for opt in (socket.SO_RCVBUF, socket.SO_SNDBUF):
            with contextlib.suppress(Exception):
                sock.setsockopt(socket.SOL_SOCKET, opt, NON_LINUX_SOCK_BUF)


class FastTcpConnection(ConnectionTcpAbridged):
    """
    Ultra-low-overhead Abridged MTProto TCP connection:
    • No per-packet 1 MB buffer slicing/concatenation or CRC32 overhead (unlike ConnectionTcpFull)
    • 8 MB StreamReader limit (no epoll pause/resume thrashing on 1 MB chunks)
    • Deep async send/recv packet queues + TCP_NODELAY
    """

    def __init__(self, ip, port, dc_id, *, loggers, proxy=None, local_addr=None):
        super().__init__(ip, port, dc_id, loggers=loggers, proxy=proxy, local_addr=local_addr)
        self._send_queue = asyncio.Queue(64)
        self._recv_queue = asyncio.Queue(32)

    async def _connect(self, timeout=None, ssl=None):
        if not self._proxy:
            local_addr = None
            if self._local_addr is not None:
                if isinstance(self._local_addr, tuple) and len(self._local_addr) == 2:
                    local_addr = self._local_addr
                elif isinstance(self._local_addr, str):
                    local_addr = (self._local_addr, 0)
                else:
                    raise ValueError(f"Unknown local address format: {self._local_addr}")

            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(
                    host=self._ip,
                    port=self._port,
                    ssl=ssl,
                    local_addr=local_addr,
                    limit=STREAM_READER_LIMIT,
                ),
                timeout=timeout,
            )
            _tune_socket(self._writer)
            self._codec = self.packet_codec(self)
            self._init_conn()
            await self._writer.drain()
        else:
            await super()._connect(timeout=timeout, ssl=ssl)
            if self._reader is not None:
                self._reader._limit = STREAM_READER_LIMIT
            _tune_socket(self._writer)


def _pick_connection_factory(client):
    conn_cls = getattr(client, "_connection", None)
    if isinstance(conn_cls, type) and issubclass(conn_cls, (ConnectionTcpFull, ConnectionTcpAbridged)):
        return FastTcpConnection
    return conn_cls or FastTcpConnection


def _client_key(client) -> int:
    sess = getattr(client, "session", None)
    return id(sess) if sess is not None else id(client)


# ───────────────── Foreign-DC AuthKey Cache (per userbot × DC) ─────────────────
_AUTH_CACHE: Dict[Tuple[int, int], Any] = {}
_AUTH_LOCKS: Dict[Tuple[int, int], asyncio.Lock] = {}


def _get_auth_lock(key: Tuple[int, int]) -> asyncio.Lock:
    lk = _AUTH_LOCKS.get(key)
    if lk is None:
        lk = asyncio.Lock()
        _AUTH_LOCKS[key] = lk
    return lk


def invalidate_auth_cache(client, dc_id: Optional[int]) -> None:
    if dc_id is not None:
        _AUTH_CACHE.pop((_client_key(client), int(dc_id)), None)


async def _resolve_dc_endpoints(client, dc_id: int) -> List[Any]:
    """
    එකම DC එකට අයත් සියලුම වලංගු (non-CDN, non-tcpo_only) DcOption endpoints ලබා ගන්නවා.
    වැදගත්: Telegram MTProto වල media_only=True endpoints වෙනම server AuthKey table එකක් භාවිතා කරන නිසා,
    primary endpoint එකේ media_only flag එකට ගැළපෙන endpoints පමණක් තෝරා ගනී (-404 AuthKey ගැටලු 100% වළක්වයි).
    """
    primary = await client._get_dc(dc_id)
    cls = client.__class__
    cfg = getattr(cls, "_config", None) or getattr(client, "_config", None)
    dc_options = getattr(cfg, "dc_options", None)
    if not dc_options:
        return [primary]

    use_ipv6 = bool(getattr(client, "_use_ipv6", False))
    want_media_only = bool(getattr(primary, "media_only", False))
    candidates = [
        opt for opt in dc_options
        if getattr(opt, "id", None) == dc_id
        and not bool(getattr(opt, "cdn", False))
        and not bool(getattr(opt, "tcpo_only", False))
        and bool(getattr(opt, "ipv6", False)) == use_ipv6
        and bool(getattr(opt, "media_only", False)) == want_media_only
    ]
    if not candidates:
        return [primary]

    candidates.sort(key=lambda o: (0 if getattr(o, "port", 443) == 443 else 1))

    seen = {(primary.ip_address, primary.port)}
    out = [primary]
    for opt in candidates:
        ep = (opt.ip_address, opt.port)
        if ep not in seen:
            seen.add(ep)
            out.append(opt)
    return out


def _build_init_request(client, query):
    """Clone client._init_request safely without mutating shared state, wrapped with WithoutUpdates + Layer."""
    from telethon.tl.alltlobjects import LAYER
    from telethon.tl.functions import InitConnectionRequest, InvokeWithLayerRequest, InvokeWithoutUpdatesRequest

    ir = getattr(client, "_init_request", None)
    if ir is None:
        return None
    cloned = InitConnectionRequest(
        api_id=ir.api_id,
        device_model=ir.device_model,
        system_version=ir.system_version,
        app_version=ir.app_version,
        system_lang_code=ir.system_lang_code,
        lang_pack=ir.lang_pack,
        lang_code=ir.lang_code,
        query=query,
        proxy=getattr(ir, "proxy", None),
        params=getattr(ir, "params", None),
    )
    return InvokeWithLayerRequest(LAYER, InvokeWithoutUpdatesRequest(cloned))


async def _init_media_session(client, sender, query=None) -> None:
    """Negotiate MTProto layer, server salt, and disable server-pushed chat updates on a media sender."""
    from telethon.tl.functions.help import GetConfigRequest

    req = _build_init_request(client, query or GetConfigRequest())
    if req is not None:
        await asyncio.wait_for(sender.send(req), timeout=INIT_TIMEOUT)


@dataclass
class SenderSlot:
    idx: int
    sender: Any                       # None == main TelegramClient
    dc_opt: Any = None
    inflight: int = 0
    ewma_rtt: float = 0.25
    bad: int = 0
    last_ok: float = 0.0
    reconnecting: bool = False
    dead: bool = False

    def is_healthy(self, client) -> bool:
        if self.dead or self.reconnecting:
            return False
        if self.sender is None:
            is_conn = getattr(client, "is_connected", None)
            return bool(is_conn() if callable(is_conn) else True)
        if not self.sender.is_connected():
            return False
        conn = getattr(self.sender, "_connection", None)
        if conn is not None and hasattr(conn, "_connected") and not conn._connected:
            return False
        return True

    @property
    def load_score(self) -> float:
        return (self.inflight + 1) * max(self.ewma_rtt, 0.02) + (self.bad * 0.5)


class SenderPool:
    """
    Multi-connection MTProto sender pool with:
    • Multi-IP / media_only DC load spreading
    • Least-loaded latency-aware smart dispatch (send_smart)
    • Per-socket background self-healing (_heal_slot)
    • Streaming slot activation (on_slot_ready callback)
    • Full backward compatibility (.start(), .send(req, idx), .senders, .close())
    """

    def __init__(self, client, dc_id: Optional[int], size: int, use_main: bool = True, managed: bool = False):
        self.client = client
        self.dc_id = dc_id
        self.size = max(1, int(size))
        self.use_main = use_main
        self.managed = managed
        self.slots: List[SenderSlot] = []
        self.senders: List = []           # backward-compat list: None or MTProtoSender
        self._extras: List = []
        self._endpoints: List = []
        self._auth_key: Any = None
        self._start_lock = asyncio.Lock()
        self._heal_tasks: set = set()
        self.last_used: float = time.monotonic()
        self.active_borrows: int = 0

    def _sync_senders_list(self) -> None:
        self.senders = [s.sender for s in self.slots if not s.dead]

    async def _connect_raw_sender(self, auth_key, dc_opt, primary_dc) -> Any:
        from telethon.network import MTProtoSender

        c = self.client
        conn_cls = _pick_connection_factory(c)
        local_addr = getattr(c, "_local_addr", None)

        async def _reinit():
            with contextlib.suppress(Exception):
                await _init_media_session(c, sender)

        sender = MTProtoSender(
            auth_key,
            loggers=c._log,
            retries=2,
            delay=0.4,
            connect_timeout=CONNECT_TIMEOUT,
            auto_reconnect=True,
            auto_reconnect_callback=_reinit if auth_key is not None else None,
        )

        # Try chosen DC option first; if secondary IP fails, immediately fall back to primary DC IP
        targets = [dc_opt] if dc_opt is primary_dc else [dc_opt, primary_dc]
        last_err: Optional[BaseException] = None
        for target in targets:
            try:
                kwargs = dict(loggers=c._log, proxy=c._proxy)
                if local_addr is not None:
                    kwargs["local_addr"] = local_addr
                conn = conn_cls(target.ip_address, target.port, target.id, **kwargs)
                await asyncio.wait_for(sender.connect(conn), timeout=CONNECT_TIMEOUT + 4.0)
                return sender
            except Exception as e:
                last_err = e
                with contextlib.suppress(Exception):
                    await sender.disconnect()
                sender = MTProtoSender(
                    auth_key,
                    loggers=c._log,
                    retries=2,
                    delay=0.4,
                    connect_timeout=CONNECT_TIMEOUT,
                    auto_reconnect=True,
                    auto_reconnect_callback=_reinit if auth_key is not None else None,
                )
        raise last_err or RuntimeError(f"DC{self.dc_id} connect failed")

    async def _make(self, auth_key, ep_idx: int = 0):
        from telethon.tl.functions.auth import ExportAuthorizationRequest, ImportAuthorizationRequest

        c = self.client
        if not self._endpoints:
            self._endpoints = await _resolve_dc_endpoints(c, self.dc_id)
        primary = self._endpoints[0]
        # Use primary endpoint for fresh DH + auth import; spread subsequent connections across all endpoints
        dc_opt = primary if auth_key is None else self._endpoints[ep_idx % len(self._endpoints)]

        if auth_key is None:
            # Overlap foreign-DC TCP+DH handshake with Home-DC ExportAuthorizationRequest in parallel!
            sender_task = asyncio.create_task(self._connect_raw_sender(None, dc_opt, primary))
            export_task = asyncio.create_task(c(ExportAuthorizationRequest(self.dc_id)))
            try:
                sender, auth = await asyncio.gather(sender_task, export_task)
            except BaseException:
                for t in (sender_task, export_task):
                    if not t.done():
                        t.cancel()
                res = await asyncio.gather(sender_task, export_task, return_exceptions=True)
                for r in res:
                    if hasattr(r, "disconnect"):
                        with contextlib.suppress(Exception):
                            await r.disconnect()
                raise

            try:
                imp = ImportAuthorizationRequest(id=auth.id, bytes=auth.bytes)
                await _init_media_session(c, sender, query=imp)

                async def _reinit_imported():
                    with contextlib.suppress(Exception):
                        await _init_media_session(c, sender)

                sender._auto_reconnect_callback = _reinit_imported
            except BaseException:
                with contextlib.suppress(Exception):
                    await sender.disconnect()
                raise
            return sender

        sender = await self._connect_raw_sender(auth_key, dc_opt, primary)
        try:
            await _init_media_session(c, sender)
        except BaseException:
            with contextlib.suppress(Exception):
                await sender.disconnect()
            raise
        return sender

    async def _ensure_foreign_auth(self) -> Tuple[Any, Optional[Any]]:
        """Obtain or create cached foreign-DC auth_key. Returns (auth_key, optional_first_sender)."""
        c = self.client
        cache_key = (_client_key(c), int(self.dc_id))
        cached = _AUTH_CACHE.get(cache_key)
        if cached is not None:
            return cached, None

        async with _get_auth_lock(cache_key):
            cached = _AUTH_CACHE.get(cache_key)
            if cached is not None:
                return cached, None
            first = await self._make(None, ep_idx=0)
            _AUTH_CACHE[cache_key] = first.auth_key
            return first.auth_key, first

    async def start(self, target_size: Optional[int] = None,
                    on_slot_ready: Optional[Callable[[SenderSlot], None]] = None) -> None:
        if target_size is not None and target_size > self.size:
            self.size = int(target_size)
        self.last_used = time.monotonic()

        async with self._start_lock:
            c = self.client
            home = c.session.dc_id
            self.dc_id = self.dc_id or home
            same = self.dc_id == home

            if not self._endpoints:
                self._endpoints = await _resolve_dc_endpoints(c, self.dc_id)

            # 1) Reuse already-healthy warm slots immediately (0 ms startup!)
            healthy_slots: List[SenderSlot] = []
            dead_senders: List[Any] = []
            for sl in self.slots:
                if sl.is_healthy(c):
                    sl.dead = False
                    sl.bad = 0
                    healthy_slots.append(sl)
                    if on_slot_ready:
                        on_slot_ready(sl)
                else:
                    if sl.sender is not None:
                        dead_senders.append(sl.sender)

            for ds in dead_senders:
                with contextlib.suppress(Exception):
                    await ds.disconnect()
                with contextlib.suppress(ValueError):
                    self._extras.remove(ds)

            self.slots = healthy_slots
            for idx_i, sl in enumerate(self.slots):
                sl.idx = idx_i

            has_main = any(sl.sender is None for sl in self.slots)
            if same and self.use_main and not has_main and len(self.slots) < self.size:
                main_slot = SenderSlot(idx=len(self.slots), sender=None, dc_opt=self._endpoints[0])
                self.slots.append(main_slot)
                if on_slot_ready:
                    on_slot_ready(main_slot)

            need = self.size - len(self.slots)
            if need <= 0:
                self._sync_senders_list()
                return

            # 2) Resolve auth_key (home session key or cached/exported foreign DC key)
            auth_key = c.session.auth_key if same else None
            if not same:
                auth_key, first_sender = await self._ensure_foreign_auth()
                if first_sender is not None:
                    self._extras.append(first_sender)
                    sl = SenderSlot(idx=len(self.slots), sender=first_sender, dc_opt=self._endpoints[0])
                    self.slots.append(sl)
                    self._sync_senders_list()
                    if on_slot_ready:
                        on_slot_ready(sl)
                    need -= 1
            self._auth_key = auth_key

            # 3) Open remaining connections concurrently & stream each slot the instant it connects!
            if need > 0:
                base_ep = len(self.slots)

                async def _open_one(k: int):
                    try:
                        return k, await self._make(auth_key, ep_idx=base_ep + k)
                    except AuthKeyError:
                        if not same:
                            invalidate_auth_cache(c, self.dc_id)
                        raise

                tasks = [asyncio.create_task(_open_one(k)) for k in range(need)]
                auth_failed = False
                for fut in asyncio.as_completed(tasks):
                    try:
                        k, sender = await fut
                        self._extras.append(sender)
                        ep = self._endpoints[(base_ep + k) % len(self._endpoints)]
                        sl = SenderSlot(idx=len(self.slots), sender=sender, dc_opt=ep)
                        self.slots.append(sl)
                        self._sync_senders_list()
                        if on_slot_ready:
                            on_slot_ready(sl)
                    except AuthKeyError as e:
                        auth_failed = True
                        logger.warning(f"DC{self.dc_id} cached auth_key rejected ({e!r}); will refresh")
                    except Exception as e:
                        logger.warning(f"extra connection failed on DC{self.dc_id}: {e!r}")

                # If a cached foreign auth_key expired between jobs, re-export once cleanly
                if auth_failed and not same and not self.slots:
                    invalidate_auth_cache(c, self.dc_id)
                    auth_key, first_sender = await self._ensure_foreign_auth()
                    self._auth_key = auth_key
                    if first_sender is not None:
                        self._extras.append(first_sender)
                        sl = SenderSlot(idx=len(self.slots), sender=first_sender, dc_opt=self._endpoints[0])
                        self.slots.append(sl)
                        if on_slot_ready:
                            on_slot_ready(sl)

            self._sync_senders_list()
            if not self.slots:
                raise RuntimeError(f"No connection available for DC{self.dc_id}")
            logger.info(
                f"SenderPool ready: DC{self.dc_id} × {len(self.slots)} connections "
                f"across {min(len(self._endpoints), len(self.slots))} endpoint(s)"
            )

    def pick_slot(self, hint: int = 0) -> SenderSlot:
        """Select healthy slot with lowest latency-weighted in-flight load."""
        c = self.client
        healthy = [s for s in self.slots if s.is_healthy(c)]
        if not healthy:
            healthy = [s for s in self.slots if not s.dead]
        if not healthy:
            raise ConnectionError(f"All connections in DC{self.dc_id} pool are down")
        # Tie-break with hint rotation so equal-load slots spread evenly
        n = len(healthy)
        best = healthy[hint % n]
        best_score = best.load_score
        for off in range(1, n):
            cand = healthy[(hint + off) % n]
            sc = cand.load_score
            if sc < best_score:
                best, best_score = cand, sc
        return best

    def _schedule_heal(self, slot: SenderSlot, reset_auth: bool = False) -> None:
        if slot.sender is None or slot.reconnecting or slot.dead:
            return
        slot.reconnecting = True

        async def _heal():
            old = slot.sender
            try:
                if old is not None:
                    with contextlib.suppress(Exception):
                        await old.disconnect()
                    with contextlib.suppress(ValueError):
                        self._extras.remove(old)
                c = self.client
                same = self.dc_id == c.session.dc_id
                # Only refresh cached foreign auth_key if no sibling slot succeeded recently;
                # prevents a single socket glitch from invalidating a healthy shared auth_key!
                now = time.monotonic()
                sibling_ok = any(
                    s is not slot and not s.dead and s.last_ok > 0 and (now - s.last_ok) < 15.0
                    for s in self.slots
                )
                if reset_auth and not same and not sibling_ok:
                    invalidate_auth_cache(c, self.dc_id)
                    auth_key, new_sender = await self._ensure_foreign_auth()
                    self._auth_key = auth_key
                    if new_sender is None:
                        new_sender = await self._make(auth_key, ep_idx=slot.idx)
                else:
                    auth_key = c.session.auth_key if same else (self._auth_key or (await self._ensure_foreign_auth())[0])
                    self._auth_key = auth_key
                    new_sender = await self._make(auth_key, ep_idx=slot.idx + 1)
                self._extras.append(new_sender)
                slot.sender = new_sender
                slot.bad = 0
                slot.ewma_rtt = 0.25
                self._sync_senders_list()
            except Exception as e:
                logger.debug(f"Slot #{slot.idx} heal failed on DC{self.dc_id}: {e!r}")
                slot.dead = True
                self._sync_senders_list()
            finally:
                slot.reconnecting = False

        t = asyncio.create_task(_heal())
        self._heal_tasks.add(t)
        t.add_done_callback(self._heal_tasks.discard)

    async def send_on_slot(self, slot: SenderSlot, req, timeout: Optional[float] = None):
        self.last_used = time.monotonic()
        slot.inflight += 1
        t0 = time.monotonic()
        try:
            coro = self.client(req) if slot.sender is None else slot.sender.send(req)
            res = await (asyncio.wait_for(coro, timeout=timeout) if timeout else coro)
            now = time.monotonic()
            dt = max(0.005, now - t0)
            slot.ewma_rtt = slot.ewma_rtt * 0.75 + dt * 0.25
            slot.bad = 0
            slot.last_ok = now
            return res
        except FloodWaitError:
            raise
        except AuthKeyError:
            slot.bad += 1
            self._schedule_heal(slot, reset_auth=True)
            raise
        except (ConnectionError, OSError, asyncio.IncompleteReadError):
            slot.bad += 1
            self._schedule_heal(slot)
            raise
        except asyncio.TimeoutError:
            slot.bad += 1
            slot.ewma_rtt = min(slot.ewma_rtt * 1.6, 10.0)
            if slot.bad >= 2 and sum(1 for s in self.slots if not s.dead) > 1:
                self._schedule_heal(slot)
            raise
        except Exception as e:
            msg = str(e).lower()
            if "auth_key" in msg or "404" in msg or "connection_not_inited" in msg:
                slot.bad += 1
                self._schedule_heal(slot, reset_auth=("auth_key" in msg or "404" in msg))
            raise
        finally:
            slot.inflight = max(0, slot.inflight - 1)

    async def send_slot_preferred(
        self,
        preferred_idx: int,
        req,
        timeout: Optional[float] = None,
        max_slot_inflight: int = 4,
    ):
        """
        Keep constant pipeline depth pinned to `slots[preferred_idx]` when healthy;
        automatically spill over to `pick_slot` if that socket is busy or reconnecting.
        """
        if 0 <= preferred_idx < len(self.slots):
            sl = self.slots[preferred_idx]
            if sl.is_healthy(self.client) and sl.inflight < max_slot_inflight:
                return await self.send_on_slot(sl, req, timeout=timeout)
        slot = self.pick_slot(preferred_idx)
        return await self.send_on_slot(slot, req, timeout=timeout)

    async def send_smart(self, req, hint: int = 0, timeout: Optional[float] = None):
        slot = self.pick_slot(hint)
        return await self.send_on_slot(slot, req, timeout=timeout)

    async def send(self, req, idx: int = 0):
        """Backward-compatible send method used by fast_uploader and legacy callers."""
        if self.slots:
            return await self.send_smart(req, hint=idx)
        s = self.senders[idx % len(self.senders)]
        if s is None:
            return await self.client(req)
        return await s.send(req)

    def release(self) -> None:
        """Release a borrow on a managed pool while keeping warm connections alive."""
        self.active_borrows = max(0, self.active_borrows - 1)
        self.last_used = time.monotonic()

    async def close(self, force: bool = False) -> None:
        if self.managed and not force:
            self.release()
            return
        for t in list(self._heal_tasks):
            t.cancel()
        if self._heal_tasks:
            await asyncio.gather(*self._heal_tasks, return_exceptions=True)
        self._heal_tasks.clear()
        for s in self._extras:
            with contextlib.suppress(Exception):
                await s.disconnect()
        self._extras.clear()
        self.slots.clear()
        self.senders.clear()


# ───────────────── Global Persistent Warm Pool Registry ─────────────────
_POOLS: Dict[Tuple[int, int, bool], SenderPool] = {}
_POOLS_LOCK = asyncio.Lock()


async def _reap_idle_pools_unlocked() -> None:
    ttl = getattr(_cfg, "DL_POOL_IDLE_TTL", 900)
    now = time.monotonic()
    stale_keys = [
        k for k, p in _POOLS.items()
        if p.active_borrows <= 0 and (now - p.last_used) > ttl
    ]
    for k in stale_keys:
        pool = _POOLS.pop(k, None)
        if pool:
            asyncio.create_task(pool.close(force=True))


async def acquire_pool(client, dc_id: Optional[int], size: int, use_main: bool = False) -> SenderPool:
    """Get or create a persistent warm SenderPool for (client, dc_id, use_main)."""
    sess = getattr(client, "session", None)
    resolved_dc = int(dc_id or (getattr(sess, "dc_id", None) or 1))
    key = (_client_key(client), resolved_dc, bool(use_main))
    async with _POOLS_LOCK:
        await _reap_idle_pools_unlocked()
        pool = _POOLS.get(key)
        if pool is None:
            pool = SenderPool(client, resolved_dc, size=size, use_main=use_main, managed=True)
            _POOLS[key] = pool
        elif size > pool.size:
            pool.size = int(size)
        pool.active_borrows += 1
        pool.last_used = time.monotonic()
        return pool


async def warmup_pool(client, dc_id: Optional[int], size: int, use_main: bool = False) -> Optional[SenderPool]:
    """Pre-warm a persistent SenderPool in the background so the upcoming download starts in 0 ms."""
    pool = await acquire_pool(client, dc_id, size=size, use_main=use_main)
    try:
        await asyncio.wait_for(pool.start(target_size=size), timeout=45.0)
        return pool
    except Exception as e:
        logger.debug(f"Pool warmup DC{dc_id} skipped: {e!r}")
        return None
    finally:
        pool.release()


async def close_client_pools(client) -> None:
    ck = _client_key(client)
    to_close: List[SenderPool] = []
    async with _POOLS_LOCK:
        for k in [k for k in _POOLS if k[0] == ck]:
            to_close.append(_POOLS.pop(k))
    for k in [k for k in list(_AUTH_CACHE) if k[0] == ck]:
        _AUTH_CACHE.pop(k, None)
    if to_close:
        await asyncio.gather(*[p.close(force=True) for p in to_close], return_exceptions=True)


async def close_all_pools() -> None:
    async with _POOLS_LOCK:
        pools = list(_POOLS.values())
        _POOLS.clear()
    _AUTH_CACHE.clear()
    if pools:
        await asyncio.gather(*[p.close(force=True) for p in pools], return_exceptions=True)
