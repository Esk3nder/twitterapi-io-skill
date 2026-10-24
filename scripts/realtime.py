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

import base64
import json
import os
import socket
import ssl
import struct
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from twitterapi import Client, APIError, IncompleteDataError  # noqa: E402
from axi import (RULE_FIELDS, TWEET_FIELDS, ArgumentParser, HelpFormatter, add_common_output_flags,
                 emit, emit_line, error, fast_version, format_record, prepare_output,
                 parse_fields, run_cli, success, UsageError)  # noqa: E402

WS_HOST = "ws.twitterapi.io"
WS_PATH = "/twitter/tweet/websocket"
WS_IDLE_TIMEOUT = 70            # seconds without a frame before recv times out
RECONNECT_MIN_SECONDS = 90      # vendor guidance: wait >=90s before reconnecting
CAPACITY_CLOSE_CODE = 1013      # vendor guidance: wait 60s+ on this code
INTERVAL_MIN, INTERVAL_MAX = 0.1, 86400


class StreamConnectError(ConnectionError):
    """The websocket could not be opened. Distinct from a stream that opened
    and later went idle or closed: no observation window ever started."""


# ------------------------------------------------------------------ rules --
def list_rules(c: Client) -> list:
    """Current filter rules. A response without a `rules` list is contract
    drift and raises: reading it as zero rules would print "nothing is
    billing" while rules may be billing."""
    resp = c._raw("GET", "/oapi/tweet_filter/get_rules")
    rules = resp.get("rules") if isinstance(resp, dict) else None
    if not isinstance(rules, list):
        raise IncompleteDataError(
            "get_rules response has no 'rules' list; rule state and billing "
            "are UNKNOWN, not zero")
    return rules


def _rule_state(rule):
    """'active', 'inactive', or 'unknown' from is_effect. Only the documented
    encodings count as known; anything else may be billing."""
    if not isinstance(rule, dict) or "is_effect" not in rule:
        return "unknown"
    value = rule["is_effect"]
    if value is True or (type(value) in (int, float) and value == 1) or value == "1":
        return "active"
    if value is False or (type(value) in (int, float) and value == 0) or value == "0":
        return "inactive"
    return "unknown"


def active_rule_warning(c: Client, rules=None) -> str:
    rules = list_rules(c) if rules is None else rules
    live = [r for r in rules if _rule_state(r) == "active"]
    unknown = [r for r in rules if _rule_state(r) == "unknown"]
    if not live and not unknown:
        return f"{len(rules)} rule(s), none active — nothing is billing."

    def tags(group):
        return ", ".join(str(r.get("tag") if isinstance(r, dict) else r)
                         for r in group[:5])
    parts = []
    if live:
        parts.append(f"WARNING: {len(live)} ACTIVE rule(s) [{tags(live)}] are "
                     f"billing right now, whether or not anything is connected. "
                     f"Delete unused rules.")
    if unknown:
        values = ", ".join(sorted({repr(r.get("is_effect", "<missing>"))
                                   if isinstance(r, dict) else "<not a rule>"
                                   for r in unknown}))
        parts.append(f"WARNING: {len(unknown)} rule(s) [{tags(unknown)}] have an "
                     f"unrecognized is_effect ({values}): active state UNKNOWN — "
                     f"they may be billing. Inspect them with `realtime.py rules list`.")
    return " ".join(parts)


def _post_mutation_warning(c: Client) -> str:
    """Active-rule count after a mutation. The mutation already happened, so a
    failed count is reported as unknown rather than failing the command."""
    try:
        return active_rule_warning(c)
    except (APIError, IncompleteDataError, OSError) as exc:
        return (f"WARNING: active-rule count UNKNOWN after this change ({exc}); "
                f"rules may be billing. Run `realtime.py rules list`.")


def _check_interval(interval_seconds):
    try:
        value = float(interval_seconds)
    except (TypeError, ValueError):
        raise UsageError(f"interval_seconds must be a number, got "
                         f"{interval_seconds!r}") from None
    if not (INTERVAL_MIN <= value <= INTERVAL_MAX):
        raise UsageError(f"interval_seconds must be between {INTERVAL_MIN} and "
                         f"{INTERVAL_MAX}, got {interval_seconds!r}")


def add_rule(c: Client, tag, value, interval_seconds=60, *, confirm=False):
    """UNVERIFIED — creates billable server-side state. Requires confirm=True.

    Vendor sources disagree on the interval floor (0.1s vs 0.05s); 0.1 is
    enforced here as the safer bound. The range itself is untested."""
    if not confirm:
        raise PermissionError(
            "add_rule creates a rule that bills continuously while active. "
            "Pass --confirm to proceed, and delete it when finished.")
    _check_interval(interval_seconds)
    body = {"tag": tag, "value": value, "interval_seconds": interval_seconds}
    return c._raw_json("POST", "/oapi/tweet_filter/add_rule", body)


def find_rule(rules, rule_id):
    for rule in rules:
        if isinstance(rule, dict) and str(rule.get("rule_id", rule.get("id"))) == str(rule_id):
            return rule
    raise UsageError(f"rule {rule_id!r} is not in `rules list` for this account")


def update_rule(c: Client, rule_id, tag, value, interval_seconds,
                is_effect=None, *, confirm=False):
    """Activate/deactivate or edit a rule.

    VERIFIED 2026-08-10: add_rule creates a rule with is_effect=0 — INACTIVE,
    and therefore not billing. Only update_rule with is_effect=1 starts
    delivery and starts the meter. Neither the vendor skill nor their blog
    documents this; both imply a new rule is live immediately.

    update_rule REPLACES the whole rule, so tag, value AND interval must be
    resent even when you only mean to toggle is_effect. There is no default
    interval: a default would silently reset the rule's delivery rate. Use
    toggle_rule() to keep the current fields."""
    if not confirm:
        raise PermissionError(
            "update_rule can ACTIVATE a rule, which starts continuous billing. "
            "Pass --confirm.")
    _check_interval(interval_seconds)
    body = {"rule_id": rule_id, "tag": tag, "value": value,
            "interval_seconds": interval_seconds}
    if is_effect is not None:
        body["is_effect"] = int(is_effect)
    return c._raw_json("POST", "/oapi/tweet_filter/update_rule", body)


def toggle_rule(c: Client, rule_id, active, *, tag=None, value=None,
                interval_seconds=None, confirm=False):
    """Activate or deactivate a rule while keeping its current fields.

    Reads the rule from `rules list` and resends its tag, value, and interval,
    replacing only the fields passed explicitly. Without this, a toggle would
    reset a 0.1s rule to some default interval."""
    if not confirm:
        raise PermissionError(
            "toggling a rule changes server-side state and activation starts "
            "continuous billing. Pass --confirm.")
    current = find_rule(list_rules(c), rule_id)
    fields = {
        "tag": tag if tag is not None else current.get("tag"),
        "value": value if value is not None else current.get("value"),
        "interval": (interval_seconds if interval_seconds is not None
                     else current.get("interval_seconds", current.get("interval"))),
    }
    missing = [name for name, field in fields.items() if field in (None, "")]
    if missing:
        raise UsageError(
            f"rule {rule_id!r} reports no current {', '.join(missing)}; pass "
            + ", ".join(f"--{name}" for name in missing) + " explicitly")
    return update_rule(c, rule_id, fields["tag"], fields["value"],
                       fields["interval"], is_effect=1 if active else 0,
                       confirm=True)


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

    def __init__(self, api_key, host=WS_HOST, path=WS_PATH, timeout=WS_IDLE_TIMEOUT):
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


def stream(c: Client, *, seconds=None, on_event=None, status=None):
    """Consume the live stream. Yields decoded event dicts.

    Events observed per vendor docs: connected, ping, tweet, fast_tweet.
    Matched tweets arrive batched in `tweets[]` with rule_id and rule_tag.

    Returns normally only when `seconds` have elapsed. Raises
    StreamConnectError when the socket never opened; after it opened, an idle
    socket raises TimeoutError and a server close raises ConnectionError.
    `status["connected_at"]` (when a dict is passed) records when the
    observation window started.
    """
    warning = active_rule_warning(c)
    print(f"[realtime] {warning}", file=sys.stderr)
    if "none active" in warning or warning.startswith("0 rule"):
        # Connecting with no active rule succeeds and then delivers nothing,
        # which reads as a broken stream rather than an empty one.
        print("[realtime] NOTE: with no ACTIVE rule the stream will connect and "
              "stay empty. Create one with `rules add --tag T --value 'QUERY' "
              "--confirm`, then `rules activate --rule-id ID --confirm`.",
              file=sys.stderr)
    try:
        ws = WebSocketClient(c.key).connect()
    except OSError as exc:              # includes TimeoutError, ssl errors, refusals
        raise StreamConnectError(
            f"could not open wss://{WS_HOST}{WS_PATH}: "
            f"{type(exc).__name__}: {exc}") from exc
    print(f"[realtime] connected to wss://{WS_HOST}{WS_PATH}", file=sys.stderr)
    started = time.time()
    if status is not None:
        status["connected_at"] = started
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


def _print_event(evt, *, fields=None, full=False, state=None):
    et = evt.get("event_type")
    if et in ("connected", "ping"):
        print(f"  [{et}]", file=sys.stderr)
        return
    for t in evt.get("tweets") or []:
        raw = {"id": t.get("id"), "createdAt": t.get("createdAt"),
               "author": (t.get("author") or {}).get("userName")
                         or t.get("screen_name"),
               "text": t.get("text"), "rule": evt.get("rule_tag")}
        schema = dict(TWEET_FIELDS)
        schema["rule"] = ("rule",)
        rendered, clipped = format_record(
            raw, command="realtime.py stream", schema=schema, fields=fields,
            defaults=("id", "created_at", "text", "rule"), full=full)
        if state is not None:
            state["records"] += 1
            state["truncated"] = state["truncated"] or clipped
        print(json.dumps(rendered, ensure_ascii=False))


# -------------------------------------------------------------------- cli --
def _parser():
    p = ArgumentParser(description=__doc__, formatter_class=HelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("rules", help="manage filter rules (these BILL while active)",
                       formatter_class=HelpFormatter)
    actions = r.add_subparsers(dest="action", required=True)
    listing = actions.add_parser("list", help="list current filter rules",
                                 formatter_class=HelpFormatter,
                                 epilog="Examples:\n  realtime.py rules list\n  realtime.py rules list --fields id,tag,active")
    add_common_output_flags(listing)
    add = actions.add_parser("add", help="create an inactive rule",
                             formatter_class=HelpFormatter,
                             epilog="Examples:\n  realtime.py rules add --tag <tag> --value \"<query>\" --confirm\n  realtime.py rules list")
    add.add_argument("--tag", required=True, help="rule label")
    add.add_argument("--value", required=True, help="X search query, <=255 chars")
    add.add_argument("--interval", type=float, default=60,
                     help="delivery interval in seconds")
    add.add_argument("--confirm", action="store_true",
                     help="required: acknowledges server-side mutation")
    for action in ("activate", "deactivate"):
        q = actions.add_parser(action, help=f"{action} an existing rule",
                               formatter_class=HelpFormatter,
                               epilog=f"Examples:\n  realtime.py rules {action} --rule-id <id> --confirm\n  realtime.py rules list")
        q.add_argument("--rule-id", required=True, help="existing rule ID")
        q.add_argument("--tag", help="replacement tag; omit to keep the current tag")
        q.add_argument("--value", help="replacement query; omit to keep the current query")
        q.add_argument("--interval", type=float,
                       help="delivery interval in seconds, 0.1-86400; omit to "
                            "keep the current interval")
        q.add_argument("--confirm", action="store_true",
                       help="acknowledge server-side mutation and billing risk")
    delete = actions.add_parser("delete", help="delete an existing rule",
                                formatter_class=HelpFormatter,
                                epilog="Examples:\n  realtime.py rules delete --rule-id <id> --confirm\n  realtime.py rules list")
    delete.add_argument("--rule-id", required=True, help="existing rule ID")
    delete.add_argument("--confirm", action="store_true",
                        help="acknowledge server-side mutation")

    s = sub.add_parser("stream", help="consume the websocket stream",
                       formatter_class=HelpFormatter,
                       epilog="Examples:\n  realtime.py stream --seconds <seconds>\n  realtime.py stream --fields id,created_at,text,rule")
    s.add_argument("--seconds", type=int, help="stop after this many seconds")
    add_common_output_flags(s)
    return p


def _main(argv=None):
    if fast_version(argv):
        return 0
    a = _parser().parse_args(argv)
    if a.cmd == "rules" and a.action == "list":
        parse_fields(a.fields, RULE_FIELDS, "realtime.py rules list")
    if a.cmd == "stream":
        parse_fields(a.fields, dict(TWEET_FIELDS, rule=("rule",)), "realtime.py stream")
    if a.cmd == "rules" and a.action != "list" and not a.confirm:
        emit(error("refusal", f"rules {a.action} changes server-side rule state; "
                   "--confirm is required", f"realtime.py rules {a.action}",
                   f"Run `realtime.py rules {a.action} --help` and review the billing warning"))
        return 2
    command = (f"realtime.py rules {a.action}" if a.cmd == "rules"
               else "realtime.py stream")
    if a.cmd == "rules" and getattr(a, "interval", None) is not None:
        _check_interval(a.interval)
    if a.cmd == "stream" and a.seconds is not None and a.seconds < 1:
        raise UsageError(f"--seconds must be at least 1, got {a.seconds}; "
                         f"omit it to stream until stopped")
    c = Client(verbose=False)

    if a.cmd == "rules":
        if a.action == "list":
            rules = list_rules(c)
            payload = prepare_output(
                {"rules": rules}, command="realtime.py rules list",
                fields=a.fields, full=a.full, collections={"rules": RULE_FIELDS},
                defaults={"rules": ("id", "tag", "value", "active")},
                context="for this twitterapi.io account",
                help_lines=("Run `realtime.py rules <add|activate|deactivate|delete> --help` for mutations",))
            emit(success("realtime.py rules list", payload))
            print(active_rule_warning(c, rules), file=sys.stderr)
            return 0
        if a.action == "add":
            result = {
                "rule": add_rule(c, a.tag, a.value, a.interval, confirm=True),
                "help": ["Run `realtime.py rules list` to inspect rule state",
                         "Run `realtime.py rules activate --rule-id <id> --confirm` to activate"]}
        elif a.action in ("activate", "deactivate"):
            # add_rule creates a rule INACTIVE (is_effect=0); it only bills
            # and streams once activated. Without this the CLI could create
            # a rule it could never turn on, so a user's stream stayed empty.
            result = {"rule": toggle_rule(
                c, a.rule_id, a.action == "activate", tag=a.tag,
                value=a.value, interval_seconds=a.interval, confirm=True),
                "help": ["Run `realtime.py rules list` to verify current state"]}
        else:
            result = {"rule": delete_rule(c, a.rule_id, confirm=True),
                      "help": ["Run `realtime.py rules list` to verify current state"]}
        # Every mutation reports the active-rule count: a rule left active
        # bills whether or not anything consumes it.
        warning = _post_mutation_warning(c)
        emit(success(command, result))
        print(f"[realtime] {warning}", file=sys.stderr)
        return 0

    if a.cmd == "stream":
        return _stream_cli(c, a)
    return 0


def _stream_cli(c, a):
    state = {"records": 0, "truncated": False}
    status = {}
    ended_by, detail = None, None   # None = the requested --seconds elapsed
    reconnect = (f"wait >={RECONNECT_MIN_SECONDS}s before reconnecting "
                 f"(>=60s on close code {CAPACITY_CLOSE_CODE})")
    try:
        callback = lambda evt: _print_event(
            evt, fields=a.fields, full=a.full, state=state)
        for _ in stream(c, seconds=a.seconds, on_event=callback, status=status):
            pass
    except KeyboardInterrupt:
        ended_by = "interrupted"
    except (APIError, IncompleteDataError) as e:
        # The active-rule check before connecting failed; nothing streamed.
        emit_line(error("api", str(e), "realtime.py stream",
                        "Run `realtime.py rules list`, then rerun the stream"))
        return 1
    except OSError as e:
        if "connected_at" not in status:
            # StreamConnectError, or a transport failure in the rule check:
            # no observation window ever started, so this is a failure.
            print(f"[realtime] {e}; {reconnect}", file=sys.stderr)
            emit_line(error("runtime", str(e), "realtime.py stream",
                            f"Wait at least {RECONNECT_MIN_SECONDS} seconds, then rerun the stream"))
            return 1
        if isinstance(e, TimeoutError):
            # A quiet stream is normal (no rule matched anything yet); it ends
            # the observation but must not surface as a traceback.
            ended_by = "idle"
            print(f"[realtime] no events for {WS_IDLE_TIMEOUT}s — the stream is idle. "
                  f"Check `rules list`: a rule must exist AND be activated "
                  f"(is_effect=1) before anything is delivered.", file=sys.stderr)
        else:                           # server close or reset
            print(f"[realtime] {e}; {reconnect}", file=sys.stderr)
            if not state["records"]:
                emit_line(error("runtime", str(e), "realtime.py stream",
                                f"Wait at least {RECONNECT_MIN_SECONDS} seconds, then rerun the stream"))
                return 1
            ended_by, detail = "closed", str(e)

    n = state["records"]
    observed = time.time() - status.get("connected_at", time.time())
    # Complete only when the requested window was observed in full. An
    # unbounded stream that ends on its own, or a bounded one that ends early,
    # is a partial observation.
    complete = ended_by is None or (ended_by == "idle" and a.seconds is not None
                                    and observed >= a.seconds)
    reason = None
    if not complete:
        reason = {
            "interrupted": "stream stopped by the user",
            "idle": (f"the stream went idle for {WS_IDLE_TIMEOUT}s and ended "
                     + (f"after {observed:.0f}s of the requested {a.seconds}s"
                        if a.seconds else "with no --seconds window requested")),
            "closed": f"the server closed the stream: {detail}",
        }[ended_by]
    emit_line(success("realtime.py stream", {
        "events_count": f"{n} of {n if complete else 'unknown'} total",
        "events_state": (None if n else
                         ("0 matching posts received during the requested stream window"
                          if complete else
                          "0 matching posts received before the stream ended")),
        "help": ((["Run `realtime.py stream --full` for complete text"]
                  if state["truncated"] else [])
                 + ([] if n else
                    ["Run `realtime.py rules list` to confirm an active rule exists"]))},
        complete=complete, reason=reason,
        resume=(None if complete else
                (f"Rerun `realtime.py stream` to continue; {reconnect}"
                 if ended_by == "closed" else "Rerun `realtime.py stream` to continue")),
        records_returned=n))
    return 0


def main(argv=None):
    return run_cli("realtime.py", _main, argv, parser=_parser)


if __name__ == "__main__":
    sys.exit(main())
