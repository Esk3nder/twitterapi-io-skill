"""Shared Agent eXperience Interface boundary for the repository CLIs.

Domain functions continue to exchange ordinary JSON-compatible values.  This
module owns only command-boundary concerns: JSON emission, concise projections,
text previews, completeness, structured usage errors, home views, and explicit
session-hook setup.
"""
from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
import sys
from pathlib import Path

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


class UsageError(ValueError):
    """Invalid invocation detected before any dependency call."""


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


def run_cli(command: str, function, argv=None) -> int:
    """Catch unexpected CLI failures at one shared structured-output boundary."""
    try:
        return function(argv)
    except Exception as exc:
        kind = "runtime" if isinstance(exc, OSError) else "internal"
        message = str(exc) or repr(exc)
        help_line = (
            "Check network access, credentials, and filesystem permissions, then rerun"
            if kind == "runtime" else
            f"Rerun `{command} --help`; if the failure persists, report this error"
        )
        emit(error(kind, message, command, help_line))
        return 1


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
    for key, value in payload.items():
        if isinstance(value, list) and not key.startswith("help"):
            return len(value)
    return 0


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

    @classmethod
    def _active_chain(cls, parser):
        chain = [parser]
        remaining = cls._current_args
        current = parser
        while True:
            action = next((item for item in current._actions
                           if isinstance(item, argparse._SubParsersAction)), None)
            if not action:
                break
            choice = next((token for token in remaining if token in action.choices), None)
            if not choice:
                break
            current = action.choices[choice]
            chain.append(current)
            remaining = remaining[remaining.index(choice) + 1:]
        return chain

    def error(self, message):
        token = None
        if "unrecognized arguments:" in message:
            token = message.split("unrecognized arguments:", 1)[1].strip().split()[0]
            message = f"unknown flag {token}" if token.startswith("-") else \
                f"unknown argument {token}"
        chain = self._active_chain(self)
        active = chain[-1]
        flags = []
        for parser in chain:
            for action in parser._actions:
                flags.extend(action.option_strings)
        valid = ", ".join(dict.fromkeys(flags)) or "no optional flags"
        emit(error("usage", message, active.prog,
                   f"Valid flags for `{active.prog}`: {valid}",
                   f"Run `{active.prog} --help` for examples"))
        raise SystemExit(2)


def add_common_output_flags(parser, *, fields=True):
    if fields:
        parser.add_argument("--fields", help="comma-separated output fields")
    parser.add_argument("--full", action="store_true",
                        help="do not truncate long text fields")


def script_command(script: str, suffix: str = "") -> str:
    path = str(Path(script).resolve())
    found = shutil.which(Path(script).name)
    executable = Path(found).resolve() if found else None
    if executable == Path(path):
        base = shlex.quote(Path(script).name)
    else:
        base = f"{shlex.quote(sys.executable)} {shlex.quote(path)}"
    return f"{base} {suffix}".strip()


def display_path(path) -> str:
    text = str(Path(path).expanduser().resolve())
    home = str(Path.home())
    return "~" + text[len(home):] if text == home or text.startswith(home + os.sep) else text


def dashboard(script: str, description: str, *, store_path=None,
              cohort_limit: int = 5) -> dict:
    import sqlite3
    from store import DEFAULT_DB
    path = os.path.abspath(os.path.expanduser(store_path or DEFAULT_DB))
    stats = {"pages": 0, "accounts": 0, "tweets": 0, "cohorts": 0,
             "cohort_members": 0, "db_bytes": 0,
             "credits_cached": 0, "usd_saved_on_rerun": 0.0}
    cohorts = []
    if os.path.exists(path):
        db = sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True)
        db.row_factory = sqlite3.Row
        try:
            tables = {row[0] for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            # "cohorts" counts saved VERSIONS; "cohort_members" counts the rows
            # that actually make a store big. A real store reported cohorts=2
            # while holding 421,578 member rows, so growth was invisible in
            # this dashboard — the one place a user looks. db_bytes answers
            # the underlying question directly.
            mapping = {"pages": "pages", "accounts": "accounts", "tweets": "tweets",
                       "cohorts": "cohort_meta", "cohort_members": "cohorts"}
            for key, table in mapping.items():
                if table in tables:
                    stats[key] = db.execute(
                        f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            stats["db_bytes"] = (db.execute("PRAGMA page_count").fetchone()[0]
                                 * db.execute("PRAGMA page_size").fetchone()[0])
            if "pages" in tables:
                stats["credits_cached"] = db.execute(
                    "SELECT COALESCE(SUM(credits),0) FROM pages").fetchone()[0]
                stats["usd_saved_on_rerun"] = round(
                    stats["credits_cached"] / 100_000, 4)
            if "cohort_meta" in tables:
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
            "Run `jobs.py setup --help` to install session context",
        ],
    }
    return success(Path(script).name, payload, records_returned=len(shown))


def _merge_json_hook(path: Path, command: str) -> str:
    data = json.loads(path.read_text()) if path.exists() else {}
    events = data.setdefault("hooks", {})
    desired = {
        "SessionStart": command,
        "SessionEnd": command.replace("session-context", "session-capture"),
    }
    prior = []
    same = True
    for event, event_command in desired.items():
        hooks = events.setdefault(event, [])
        entry = {"matcher": "", "hooks": [{"type": "command", "command": event_command,
                                             "timeout": 10,
                                             "statusMessage": "Loading twitterapi.io state"}],
                 "_twitterapi_io": True}
        old = next((item for item in hooks if item.get("_twitterapi_io")), None)
        prior.append(old)
        same = same and old == entry
        hooks[:] = [item for item in hooks if not item.get("_twitterapi_io")]
        hooks.append(entry)
    status = "unchanged" if same else "repaired" if any(prior) else "installed"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2) + "\n")
    return status


def _merge_codex_feature(path: Path) -> None:
    text = path.read_text() if path.exists() else ""
    lines = text.splitlines()
    section_start = next((i for i, line in enumerate(lines)
                          if line.strip() == "[features]"), None)
    if section_start is not None:
        section_end = next((i for i in range(section_start + 1, len(lines))
                            if lines[i].lstrip().startswith("[")), len(lines))
        hook_line = next((i for i in range(section_start + 1, section_end)
                          if lines[i].split("#", 1)[0].strip().startswith("hooks")), None)
        if hook_line is not None and lines[hook_line].split("#", 1)[0].strip() == "hooks = true":
            return
        if hook_line is not None:
            lines[hook_line] = "hooks = true"
        else:
            lines.insert(section_end, "hooks = true")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(lines).rstrip() + "\n")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n\n[features]\nhooks = true\n")


def _write_opencode_plugin(path: Path, command: str) -> str:
    end_command = command.replace("session-context", "session-capture")
    content = """// Managed by twitterapi-io setup. Re-run setup to repair.\n""" + \
        "export const TwitterApiContext = async ({ $ }) => ({\n" + \
        "  'experimental.chat.system.transform': async (_input, output) => {\n" + \
        f"    const result = await $`{command}`.quiet().nothrow();\n" + \
        "    if (result.exitCode === 0) output.system.push(result.stdout.toString());\n" + \
        "  },\n" + \
        "  'session.idle': async () => {\n" + \
        f"    await $`{end_command}`.quiet().nothrow();\n" + \
        "  },\n});\n"
    old = path.read_text() if path.exists() else None
    status = "unchanged" if old == content else "repaired" if old is not None else "installed"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return status


def setup_hooks(root, *, agents, command: str, scope: str = "project") -> dict:
    """Install/repair managed project-scoped session integrations."""
    root = Path(root).resolve()
    result = {"scope": scope, "root": str(root), "installed": [], "repaired": [],
              "unchanged": []}
    for agent in agents:
        if agent == "claude":
            status = _merge_json_hook(root / ".claude/settings.json", command)
        elif agent == "codex":
            status = _merge_json_hook(root / ".codex/hooks.json", command)
            _merge_codex_feature(root / ".codex/config.toml")
        elif agent == "opencode":
            plugin = (root / ".config/opencode/plugins/twitterapi-io.js"
                      if scope == "user" else
                      root / ".opencode/plugins/twitterapi-io.js")
            status = _write_opencode_plugin(plugin, command)
        else:
            raise UsageError(f"unknown agent {agent}; valid agents: claude, codex, opencode")
        result[status].append(agent)
    result["help"] = [
        "Re-run setup after moving this repository to repair executable paths",
        "Remove the managed twitterapi-io entries/files to uninstall session context",
    ]
    return result


def check_hooks(root, *, agents, command: str, scope: str = "project") -> dict:
    """Read-only drift check for managed integrations and static guidance."""
    root = Path(root).resolve()
    checks = {}
    for agent in agents:
        if agent in {"claude", "codex"}:
            path = root / (".claude/settings.json" if agent == "claude"
                           else ".codex/hooks.json")
            try:
                data = json.loads(path.read_text())
                managed = []
                for event in ("SessionStart", "SessionEnd"):
                    managed.extend(item for item in data.get("hooks", {}).get(event, [])
                                   if item.get("_twitterapi_io"))
                text = json.dumps(managed)
                okay = (len(managed) == 2 and command in text and
                        command.replace("session-context", "session-capture") in text)
                if agent == "codex":
                    okay = okay and "hooks = true" in (
                        root / ".codex/config.toml").read_text()
            except (OSError, ValueError):
                okay = False
        elif agent == "opencode":
            path = (root / ".config/opencode/plugins/twitterapi-io.js"
                    if scope == "user" else
                    root / ".opencode/plugins/twitterapi-io.js")
            try:
                text = path.read_text()
                okay = (command in text and
                        command.replace("session-context", "session-capture") in text)
            except OSError:
                okay = False
        else:
            raise UsageError(f"unknown agent {agent}; valid agents: claude, codex, opencode")
        checks[agent] = "current" if okay else "missing_or_stale"
    repo = Path(__file__).resolve().parent.parent
    docs = "\n".join((repo / name).read_text() for name in ("README.md", "SKILL.md"))
    checks["static_guidance"] = ("current" if "completeness.status" in docs
                                 and "exits `3`" not in docs else "stale")
    return {"scope": scope, "root": str(root), "checks": checks,
            "ok": all(value == "current" for value in checks.values()),
            "help": ["Run `jobs.py setup` to install or repair stale integrations"]}
