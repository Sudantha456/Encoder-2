"""
fast_conn.py — Multi-connection sender pool (FastTelethon style)
එකම DC එකට MTProto connections කිහිපයක් open කරලා requests ඒවා හරහා බෙදනවා.
Telegram එකේ speed limit එක connection එකකට, ඒ නිසා connections වැඩි = speed වැඩි.
"""
import asyncio
import logging
from typing import List, Optional

logger = logging.getLogger("FastConn")


class SenderPool:
    def __init__(self, client, dc_id: Optional[int], size: int):
        self.client = client
        self.dc_id = dc_id
        self.size = max(1, size)
        self.senders: List = []      # None == main client
        self._extras: List = []      # අපි හදපු MTProtoSender (close කරන්න ඕන)

    async def _make(self, auth_key):
        # Telethon internals — lazy import (පරණ/අලුත් version වල fail වුණොත් fallback වෙනවා)
        from telethon.network import MTProtoSender
        from telethon.tl.alltlobjects import LAYER
        from telethon.tl.functions import InvokeWithLayerRequest
        from telethon.tl.functions.auth import ExportAuthorizationRequest, ImportAuthorizationRequest

        c = self.client
        dc = await c._get_dc(self.dc_id)
        sender = MTProtoSender(auth_key, loggers=c._log)
        await sender.connect(c._connection(dc.ip_address, dc.port, dc.id,
                                           loggers=c._log, proxy=c._proxy))
        if auth_key is None:   # වෙන DC එකක් → auth export/import
            auth = await c(ExportAuthorizationRequest(self.dc_id))
            c._init_request.query = ImportAuthorizationRequest(id=auth.id, bytes=auth.bytes)
            await sender.send(InvokeWithLayerRequest(LAYER, c._init_request))
        return sender

    async def start(self):
        c = self.client
        home = c.session.dc_id
        self.dc_id = self.dc_id or home
        same = self.dc_id == home
        auth_key = c.session.auth_key if same else None
        need = self.size - 1 if same else self.size

        if same:
            self.senders.append(None)          # main client = connection #1
        if need > 0 and auth_key is None:      # වෙන DC: පළමු එකෙන් auth key ගන්නවා
            first = await self._make(None)
            self._extras.append(first)
            self.senders.append(first)
            auth_key = first.auth_key
            need -= 1
        if need > 0:
            res = await asyncio.gather(*[self._make(auth_key) for _ in range(need)],
                                       return_exceptions=True)
            for r in res:
                if isinstance(r, Exception):
                    logger.warning(f"extra connection failed: {r!r}")
                else:
                    self._extras.append(r)
                    self.senders.append(r)
        if not self.senders:
            raise RuntimeError("No connection available")
        logger.info(f"SenderPool ready: DC{self.dc_id} × {len(self.senders)} connections")

    async def send(self, req, idx: int = 0):
        s = self.senders[idx % len(self.senders)]
        if s is None:
            return await self.client(req)
        return await s.send(req)

    async def close(self):
        for s in self._extras:
            try:
                await s.disconnect()
            except Exception:
                pass
        self._extras.clear()
        self.senders.clear()
