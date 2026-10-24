"""Shared Agent eXperience Interface boundary for the repository CLIs.

Domain functions continue to exchange ordinary JSON-compatible values.  This
module owns only command-boundary concerns: JSON emission, concise projections,
text previews, completeness, structured usage errors, and home views.
"""
from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import sys
from pathlib import Path

from twitterapi import UsageError  # noqa: F401  (re-exported for the CLIs)

VERSION = "1.0.0"
TEXT_LIMIT = 800
TEXT_FIELDS = frozenset({"text", "description", "bio", "alt_text"})

# Public name -> accepted source names, first match wins.
TWEET_FIELDS = {
    "id": ("id",), "created_at": ("created_at", "createdAt"),
    "text": ("text",), "likes": ("likes", "likeCount"),
    "retweets": ("retweets", "retweetCount"),
    "replies": ("replies", "replyCount"),
    "quotes": ("quotes", "quoteCount"), "views": ("views", "viewCount"),
    "author": ("author",), "media": ("media",),
}
ACCOUNT_FIELDS = {
    "id": ("id", "user_id", "account_id"),
    "user_name": ("user_name", "userName", "screen_name", "handle"),
    "name": ("name",), "description": ("description", "bio"),
    "followers": ("followers", "followers_count"),
    "following": ("following", "following_count", "friends_count"),
    "verified": ("verified", "isBlueVerified", "is_blue"),
}
MEDIA_FIELDS = {
    "type": ("type",), "url": ("url", "media_url_https"),
    "alt_text": ("alt_text", "ext_alt_text"),
    "full_resolution_url": ("full_resolution_url",), "tweet_id": ("tweet_id",),
}
RULE_FIELDS = {
    "id": ("id", "rule_id"), "tag": ("tag",), "value": ("value",),
    "active": ("active", "is_effect"), "interval": ("interval", "interval_seconds"),
}




class HelpFormatter(argparse.ArgumentDefaultsHelpFormatter,
                    argparse.RawDescriptionHelpFormatter):
    """Concise examples plus visible defaults."""


def emit(value) -> None:
    """Write exactly one JSON document to stdout."""
    print(json.dumps(value, indent=2, ensure_ascii=False, sort_keys=False))


def error(error_type: str, message: str, command: str, *help_lines: str) -> dict:
    return {
        "error": {"type": error_type, "message": str(message), "command": command},
        "help": list(help_lines) or [f"Run `{command} --help` for usage"],
    }


def emit_line(value) -> None:
    """One single-line JSON document. Streaming commands print JSON Lines, so
    a summary or error that follows records must not span several lines."""
    print(json.dumps(value, ensure_ascii=False))


def classify(exc, command: str):
    """The exit-code contract, in one place: (error type, exit code, help).

    2 = usage, refusal, or setup; 1 = API, runtime, or internal failure."""
    from twitterapi import (APIError, CostLimitExceeded, IncompleteDataError,
                            SetupError)
    if isinstance(exc, SetupError):
        # The message names the cause: missing or invalid key, bad
        # `TWITTERAPI_IO_QPS`, no credits, rate limit, or an outage.
        return ("setup", 2,
                "Fix the key, QPS, or account problem the message names, then rerun")
    if isinstance(exc, UsageError):
        return "usage", 2, f"Run `{command} --help` for valid arguments"
    if isinstance(exc, CostLimitExceeded):
        return ("refusal", 2,
                "Raise `--max-usd <amount>` or narrow the request, then rerun")
    if isinstance(exc, (APIError, IncompleteDataError)):
        # IncompleteDataError reaching the boundary: the upstream response
        # could not be proved complete or well-formed, an API contract failure.
        return "api", 1, "Check the handle or ID and `TWITTERAPI_IO_KEY`, then rerun"
    if isinstance(exc, (OSError, RuntimeError)):
        return "runtime", 1, "Check network access and the API status, then rerun"
    return ("internal", 1,
            f"Rerun `{command} --help`; if the failure persists, report this error")


def subcommand(parser, args, dests):
    """The command path the parser reads from `args`, e.g. "rules add", or
    None. `dests` names each subparser level's attribute, outermost first.
    Uses only the public parse_known_args. Called from the error path, so a
    parse error here is silenced: the parser's own error() writes to stdout,
    and that must not add a second JSON line."""
    try:
        with contextlib.redirect_stdout(io.StringIO()), \
                contextlib.redirect_stderr(io.StringIO()):
            namespace, _ = parser.parse_known_args(args)
    except SystemExit:
        return None
    return " ".join(str(part) for part in
                    (getattr(namespace, dest, None) for dest in dests)
                    if part) or None


def run_cli(command: str, function, argv=None, parser=None, dests=()) -> int:
    """The one structured-output boundary for every CLI: any exception that
    escapes a command becomes a single-line JSON error with its contract exit
    code. A streaming command may already have printed JSON Lines records, so
    the error is one line. An exception carrying `spend` reports it.

    `parser` builds the CLI's parser and `dests` names the attribute at each
    subcommand level; a CLI with subcommands passes both so the error names
    the subcommand path."""
    args = list(sys.argv[1:] if argv is None else argv)
    try:
        return function(argv)
    except Exception as exc:
        name = parser and dests and subcommand(parser(), args, dests)
        if name:
            command = f"{command} {name}"
        kind, code, help_line = classify(exc, command)
        body = error(kind, str(exc) or repr(exc), command, help_line)
        if getattr(exc, "spend", None):
            body["spend"] = exc.spend
        emit_line(body)
        return code


def parse_fields(requested: str | None, schema: dict, command: str,
                 defaults: tuple[str, ...] | None = None) -> tuple[str, ...]:
    fields = tuple(part.strip() for part in requested.split(",") if part.strip()) \
        if requested else tuple(defaults or schema.keys())
    unknown = [field for field in fields if field not in schema]
    if unknown:
        valid = ", ".join(schema)
        raise UsageError(
            f"unknown field {unknown[0]} for `{command}`; valid fields: {valid}")
    if not fields:
        raise UsageError(f"--fields for `{command}` must name at least one field")
    return fields


def project(record: dict, fields: tuple[str, ...], schema: dict) -> dict:
    out = {}
    for field in fields:
        for source in schema[field]:
            if source in record:
                out[field] = record[source]
                break
        else:
            out[field] = None
    return out


def format_record(record: dict, *, command: str, schema: dict,
                  fields: str | None = None, defaults=None,
                  full: bool = False) -> tuple[dict, bool]:
    selected = parse_fields(fields, schema, command, defaults)
    state = {"truncated": False}
    value = _truncate(project(record, selected, schema), full=full,
                      state=state)
    return value, state["truncated"]


def _truncate(value, *, full: bool, state: dict, key: str | None = None):
    if isinstance(value, dict):
        return {k: _truncate(v, full=full, state=state, key=k)
                for k, v in value.items()}
    if isinstance(value, list):
        return [_truncate(v, full=full, state=state, key=key) for v in value]
    if (not full and key in TEXT_FIELDS and isinstance(value, str)
            and len(value) > TEXT_LIMIT):
        state["truncated"] = True
        return value[:TEXT_LIMIT] + f"... (truncated, {len(value)} chars total)"
    return value


def prepare_output(payload: dict, *, command: str, fields: str | None = None,
                   full: bool = False, collections: dict[str, dict] | None = None,
                   defaults: dict[str, tuple[str, ...]] | None = None,
                   context: str = "for this command", help_lines=()) -> dict:
    """Copy and decorate a payload without mutating domain objects."""
    out = dict(payload)
    collections = collections or {}
    defaults = defaults or {}
    for name, schema in collections.items():
        records = list(out.get(name) or [])
        total_key = f"{name}_total"
        total = int(out.pop(total_key, len(records)))
        if records and all(isinstance(item, dict) for item in records):
            selected = parse_fields(fields, schema, command, defaults.get(name))
            records = [project(item, selected, schema) for item in records]
        out[name] = records
        out[f"{name}_count"] = f"{len(records)} of {total} total"
        if not records:
            out[f"{name}_state"] = f"0 {name.replace('_', ' ')} found {context}"
    state = {"truncated": False}
    out = _truncate(out, full=full, state=state)
    hints = list(out.get("help") or []) + list(help_lines)
    if state["truncated"]:
        hints.append(f"Run `{command} --full` to see complete text")
    if hints:
        out["help"] = list(dict.fromkeys(hints))
    return out


def _record_count(payload: dict) -> int:
    """Records across ALL top-level lists, not the first one found.

    Returning only the first list's length made completeness.records_returned
    report 0 for a drift payload whose `joined` was empty while `left` held
    departures — an agent keying on records_returned as the did-anything-come-
    back signal would read a members-only-left diff as an empty result."""
    return sum(len(value) for key, value in payload.items()
               if isinstance(value, list) and not key.startswith("help"))


def success(command: str, payload: dict | None = None, *, complete: bool = True,
            reason: str | None = None, resume: str | None = None,
            records_returned: int | None = None) -> dict:
    body = {"command": command, "complete": bool(complete)}
    body.update(payload or {})
    body["completeness"] = {
        "status": "complete" if complete else "partial",
        "reason": reason or ("requested scope fully evaluated" if complete
                             else "the requested scope was not fully evaluated"),
        "records_returned": (_record_count(payload or {}) if records_returned is None
                             else records_returned),
        "resume": resume or ("not needed" if complete else
                             f"Run `{command} --help` for completion options"),
    }
    return body


def fast_version(argv=None) -> bool:
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) == 1 and args[0] in {"-v", "-V", "--version"}:
        print(VERSION)
        return True
    return False


class ArgumentParser(argparse.ArgumentParser):
    """argparse with JSON usage failures on stdout."""

    _current_args = []

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("allow_abbrev", False)
        super().__init__(*args, **kwargs)

    def parse_args(self, args=None, namespace=None):
        ArgumentParser._current_args = list(sys.argv[1:] if args is None else args)
        return super().parse_args(args, namespace)

    def error(self, message):
        # argparse calls this on the parser that failed, so `self` is the
        # subcommand's parser when the subcommand's own arguments are wrong.
        if "unrecognized arguments:" in message:
            # An empty argument ("") has no token to name.
            tokens = message.split("unrecognized arguments:", 1)[1].split()
            token = tokens[0] if tokens else "(empty)"
            message = f"unknown flag {token}" if token.startswith("-") else \
                f"unknown argument {token}"
        # format_usage() is argparse's public usage line; it lists this
        # parser's own valid flags.
        emit_line(error("usage", message, self.prog,
                        self.format_usage().strip(),
                        f"Run `{self.prog} --help` for examples"))
        raise SystemExit(2)


def add_common_output_flags(parser, *, fields=True):
    if fields:
        parser.add_argument("--fields", help="comma-separated output fields")
    parser.add_argument("--full", action="store_true",
                        help="do not truncate long text fields")


def display_path(path) -> str:
    text = str(Path(path).expanduser().resolve())
    home = str(Path.home())
    return "~" + text[len(home):] if text == home or text.startswith(home + os.sep) else text


def dashboard(script: str, description: str, *, store_path=None,
              cohort_limit: int = 5) -> dict:
    import sqlite3
    from store import DEFAULT_DB, table_stats
    path = os.path.abspath(os.path.expanduser(store_path or DEFAULT_DB))
    stats = {"pages": 0, "accounts": 0, "tweets": 0, "cohorts": 0,
             "cohort_members": 0, "db_bytes": 0,
             "credits_cached": 0, "usd_saved_on_rerun": 0.0}   # no file yet
    cohorts = []
    if os.path.exists(path):
        # mode=ro, NOT immutable=1. Read-only is what the migration-safety
        # comment below needs; immutable additionally promises SQLite the file
        # cannot change, which skips locking AND never consults the -wal.
        # Every writer in this repo runs WAL mode, so with a crawl in flight
        # the immutable dashboard read the pre-WAL file and reported committed
        # rows as absent — measured: a store holding 25 committed tweets
        # dashboarded as tweets=0. Stale-as-definite is this project's core
        # bug class; a plain ro connection sees the WAL and honours locks.
        db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        db.row_factory = sqlite3.Row
        try:
            stats = table_stats(db)
            if stats["cohorts"]:            # 0 when cohort_meta is absent
                cohorts = [dict(row) for row in db.execute(
                    "SELECT name, version, size, created_at FROM cohort_meta "
                    "ORDER BY created_at DESC")]
        finally:
            db.close()
    shown = cohorts[:cohort_limit]
    payload = {
        "bin": display_path(script), "description": description,
        "version": VERSION, "store": display_path(path),
        "store_state": stats,
        "cohorts": shown,
        "cohorts_count": f"{len(shown)} of {len(cohorts)} total",
        "cohorts_state": (None if shown else
                          f"0 saved cohorts found in local store {display_path(path)}"),
        "help": [
            f"Run `{Path(script).name} --help` to see commands",
        ],
    }
    return success(Path(script).name, payload, records_returned=len(shown))
