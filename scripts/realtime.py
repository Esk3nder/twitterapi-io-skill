#!/usr/bin/env python3
"""Real-time tweet delivery: filter rules + webhook + websocket. Stdlib only.

    python3 realtime.py rules list
    python3 realtime.py rules add --tag ai --value "from:openai OR #AI" --confirm
    python3 realtime.py rules delete --rule-id RID --confirm
    python3 realtime.py stream --seconds 60

BILLING WARNING — the single most important fact about this subsystem:
a filter rule bills from the moment it is ACTIVE, whether or not anything ever
connects to consume it. A rule created and forgotten bills continuously. Every
command here prints the active-rule count so a leak cannot hide, and creating
or deleting a rule requires --confirm because both change server-side state.

Verified live 2026-08-10: GET /oapi/tweet_filter/get_rules -> {rules, msg, status}.
Rule CREATION was deliberately not exercised without operator consent, so the
add/update paths below are written from vendor documentation and are marked
UNVERIFIED. Do not present them as tested.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import socket
import ssl
import struct
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from twitterapi import Client, APIError  # noqa: E402

WS_HOST = "ws.twitterapi.io"
WS_PATH = "/twitter/tweet/websocket"
RECONNECT_MIN_SECONDS = 90      # vendor guidance: wait >=90s before reconnecting
CAPACITY_CLOSE_CODE = 1013      # vendor guidance: wait 60s+ on this code


# ------------------------------------------------------------------ rules --
def list_rules(c: Client) -> list:
    return (c._raw("GET", "/oapi/tweet_filter/get_rules") or {}).get("rules") or []


def active_rule_warning(c: Client) -> str:
    rules = list_rules(c)
    live = [r for r in rules if r.get("is_effect") in (1, True, "1")]
    if not live:
        return f"{len(rules)} rule(s), none active — nothing is billing."
    tags = ", ".join(str(r.get("tag")) for r in live[:5])
    return (f"WARNING: {len(live)} ACTIVE rule(s) [{tags}] are billing right now, "
            f"whether or not anything is connected. Delete unused rules.")


def add_rule(c: Client, tag, value, interval_seconds=60, *, confirm=False):
    """UNVERIFIED — creates billable server-side state. Requires confirm=True.

    Vendor sources disagree on the interval floor (0.1s vs 0.05s); 0.1 is
    enforced here as the safer bound. The range itself is untested."""
    if not confirm:
        raise PermissionError(
            "add_rule creates a rule that bills continuously while active. "
            "Pass --confirm to proceed, and delete it when finished.")
    if not (0.1 <= float(interval_seconds) <= 86400):
        raise ValueError("interval_seconds must be between 0.1 and 86400")
    body = {"tag": tag, "value": value, "interval_seconds": interval_seconds}
    return c._raw_json("POST", "/oapi/tweet_filter/add_rule", body)


def update_rule(c: Client, rule_id, tag, value, interval_seconds=60,
                is_effect=None, *, confirm=False):
    """Activate/deactivate or edit a rule.

    VERIFIED 2026-08-10: add_rule creates a rule with is_effect=0 — INACTIVE,
    and therefore not billing. Only update_rule with is_effect=1 starts
    delivery and starts the meter. Neither the vendor skill nor their blog
    documents this; both imply a new rule is live immediately.

    update_rule REPLACES the whole rule, so tag/value must be resent even when
    you only mean to toggle is_effect."""
    if not confirm:
        raise PermissionError(
            "update_rule can ACTIVATE a rule, which starts continuous billing. "
            "Pass --confirm.")
    body = {"rule_id": rule_id, "tag": tag, "value": value,
            "interval_seconds": interval_seconds}
    if is_effect is not None:
        body["is_effect"] = int(is_effect)
    return c._raw_json("POST", "/oapi/tweet_filter/update_rule", body)


def delete_rule(c: Client, rule_id, *, confirm=False):
    """Body-JSON DELETE (not a query string) — verified shape, untested call."""
    if not confirm:
        raise PermissionError("delete_rule changes server state; pass --confirm")
    return c._raw_json("DELETE", "/oapi/tweet_filter/delete_rule",
                       {"rule_id": rule_id})


# -------------------------------------------------------------- websocket --
class WebSocketClient:
    """Minimal RFC6455 client over stdlib ssl. Deliberately not a dependency:
    the whole point of this skill is zero install friction.

    Handles text/binary frames, fragmentation (continuation frames), and
    interleaved control frames (ping/pong/close) per RFC6455.
    """

    def __init__(self, api_key, host=WS_HOST, path=WS_PATH, timeout=70):
        self.api_key, self.host, self.path, self.timeout = api_key, host, path, timeout
        self.sock = None
        # Bytes read past the end of the HTTP handshake. The server's first
        # frame commonly arrives in the SAME TCP segment as the 101 response;
        # discarding the remainder loses that event and can desync every frame
        # after it (a stream that "connects" and then drops everything).
        self._buf = b""

    def connect(self):
        raw = socket.create_connection((self.host, 443), timeout=self.timeout)
        self.sock = ssl.create_default_context().wrap_socket(
            raw, server_hostname=self.host)
        key = base64.b64encode(os.urandom(16)).decode()
        req = (f"GET {self.path} HTTP/1.1\r\nHost: {self.host}\r\n"
               f"Upgrade: websocket\r\nConnection: Upgrade\r\n"
               f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n"
               f"x-api-key: {self.api_key}\r\n\r\n")
        self.sock.sendall(req.encode())
        resp = b""
        while b"\r\n\r\n" not in resp:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("server closed during handshake")
            resp += chunk
        head_bytes, sep, rest = resp.partition(b"\r\n\r\n")
        self._buf = rest                # keep frame bytes that rode along
        head = head_bytes.decode("latin-1")
        status = head.split("\r\n")[0]
        if "101" not in status:
            raise ConnectionError(f"handshake refused: {status} :: {head[:300]}")
        return self

    def _recv_exact(self, n):
        buf, self._buf = self._buf[:n], self._buf[n:]   # drain the handshake tail first
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("connection closed")
            buf += chunk
        return buf

    def recv(self):
        """Return (opcode, payload) for one COMPLETE application message.

        Reassembles fragmented messages (a large tweet batch may arrive split
        across continuation frames) and transparently answers ping frames,
        which RFC6455 permits to be interleaved between fragments. Returning a
        fragment as if it were a whole message yields truncated JSON that the
        caller silently drops.
        """
        frags, first_op = [], None
        while True:
            b1, b2 = self._recv_exact(2)
            fin, opcode = bool(b1 & 0x80), b1 & 0x0F
            masked, length = bool(b2 & 0x80), b2 & 0x7F
            if length == 126:
                length = struct.unpack(">H", self._recv_exact(2))[0]
            elif length == 127:
                length = struct.unpack(">Q", self._recv_exact(8))[0]
            mask = self._recv_exact(4) if masked else b""
            payload = self._recv_exact(length) if length else b""
            if masked:
                payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))

            if opcode == 0x9:                   # ping -> pong, keep waiting
                self._send(0xA, payload)
                continue
            if opcode == 0xA:                   # pong, ignore
                continue
            if opcode == 0x8:                   # close
                code = struct.unpack(">H", payload[:2])[0] if len(payload) >= 2 else None
                raise ConnectionError(f"server closed, code={code}")
            if opcode == 0x0:                   # continuation of the message
                frags.append(payload)
            else:
                first_op, frags = opcode, [payload]
            if fin:
                return first_op, b"".join(frags)

    def _send(self, opcode, payload=b""):
        mask = os.urandom(4)
        n = len(payload)
        header = bytes([0x80 | opcode])
        if n < 126:
            header += bytes([0x80 | n])
        elif n < 65536:
            header += bytes([0x80 | 126]) + struct.pack(">H", n)
        else:
            header += bytes([0x80 | 127]) + struct.pack(">Q", n)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        self.sock.sendall(header + mask + masked)

    def close(self):
        try:
            if self.sock:
                self._send(0x8, struct.pack(">H", 1000))
                self.sock.close()
        except Exception:
            pass


def stream(c: Client, *, seconds=None, on_event=None):
    """Consume the live stream. Yields decoded event dicts.

    Events observed per vendor docs: connected, ping, tweet, fast_tweet.
    Matched tweets arrive batched in `tweets[]` with rule_id and rule_tag.
    """
    warning = active_rule_warning(c)
    print(f"[realtime] {warning}", file=sys.stderr)
    if "none active" in warning or warning.startswith("0 rule"):
        # Connecting with no active rule succeeds and then delivers nothing,
        # which reads as a broken stream rather than an empty one.
        print("[realtime] NOTE: with no ACTIVE rule the stream will connect and "
              "stay empty. Create one with `rules add --tag T --value 'QUERY' "
              "--confirm`, then `rules activate --rule-id ID --tag T --value "
              "'QUERY' --confirm`.", file=sys.stderr)
    ws = WebSocketClient(c.key).connect()
    print(f"[realtime] connected to wss://{WS_HOST}{WS_PATH}", file=sys.stderr)
    started = time.time()
    try:
        while True:
            if seconds and time.time() - started > seconds:
                return
            opcode, payload = ws.recv()
            if opcode is None:
                continue
            try:
                evt = json.loads(payload.decode("utf-8", "replace"))
            except json.JSONDecodeError:
                continue
            (on_event or _print_event)(evt)
            yield evt
    finally:
        ws.close()


def _print_event(evt):
    et = evt.get("event_type")
    if et in ("connected", "ping"):
        print(f"  [{et}]", file=sys.stderr)
        return
    for t in evt.get("tweets") or []:
        print(json.dumps({"rule": evt.get("rule_tag"), "id": t.get("id"),
                          "author": (t.get("author") or {}).get("userName")
                                    or t.get("screen_name"),
                          "text": t.get("text")}, ensure_ascii=False))


# -------------------------------------------------------------------- cli --
def main(argv=None):
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("rules", help="manage filter rules (these BILL while active)")
    r.add_argument("action",
                   choices=["list", "add", "activate", "deactivate", "delete"])
    r.add_argument("--tag")
    r.add_argument("--value", help="X search query, <=255 chars")
    r.add_argument("--interval", type=float, default=60)
    r.add_argument("--rule-id")
    r.add_argument("--confirm", action="store_true",
                   help="required for add/activate/deactivate/delete: all change billable state")

    s = sub.add_parser("stream", help="consume the websocket stream")
    s.add_argument("--seconds", type=int, help="stop after N seconds")

    a = p.parse_args(argv)
    try:
        c = Client(verbose=False)
    except RuntimeError as e:
        print(str(e), file=sys.stderr)
        return 1

    if a.cmd == "rules":
        try:
            if a.action == "list":
                rules = list_rules(c)
                print(json.dumps(rules, indent=1))
                print(active_rule_warning(c), file=sys.stderr)
            elif a.action == "add":
                if not (a.tag and a.value):
                    p.error("add needs --tag and --value")
                print(json.dumps(add_rule(c, a.tag, a.value, a.interval,
                                          confirm=a.confirm), indent=1))
            elif a.action in ("activate", "deactivate"):
                # add_rule creates a rule INACTIVE (is_effect=0); it only bills
                # and streams once activated. Without this the CLI could create
                # a rule it could never turn on, so a user's stream stayed empty.
                if not (a.rule_id and a.tag and a.value):
                    p.error(f"{a.action} needs --rule-id, --tag and --value "
                            f"(update_rule replaces the whole rule)")
                print(json.dumps(update_rule(
                    c, a.rule_id, a.tag, a.value, a.interval,
                    is_effect=1 if a.action == "activate" else 0,
                    confirm=a.confirm), indent=1))
            else:
                if not a.rule_id:
                    p.error("delete needs --rule-id")
                print(json.dumps(delete_rule(c, a.rule_id, confirm=a.confirm),
                                 indent=1))
        except PermissionError as e:
            print(f"REFUSED: {e}", file=sys.stderr)
            return 2
        except APIError as e:
            print(f"API ERROR: {e}", file=sys.stderr)
            return 1
        return 0

    if a.cmd == "stream":
        try:
            for _ in stream(c, seconds=a.seconds):
                pass
        except KeyboardInterrupt:
            pass
        except TimeoutError:
            # A quiet stream is normal (no rule matched anything yet); it must
            # not surface as a traceback.
            print(f"[realtime] no events for {c.timeout}s — the stream is idle. "
                  f"Check `rules list`: a rule must exist AND be activated "
                  f"(is_effect=1) before anything is delivered.", file=sys.stderr)
            return 0
        except ConnectionError as e:
            print(f"[realtime] {e}", file=sys.stderr)
            print(f"[realtime] reconnect guidance: wait >={RECONNECT_MIN_SECONDS}s "
                  f"(>=60s on close code {CAPACITY_CLOSE_CODE})", file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
