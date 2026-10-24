"""Negative tests for the 2026-10-23 adversarial review of workflows/realtime.

Each test scripts only the HTTP/websocket boundary and runs the real CLI or
library decision logic offline. No network, no API key, no spend. The shared
root cause under test: ABSENCE read as a confident zero, empty, or complete.
"""
from __future__ import annotations

import contextlib
import io
import json
import os
import re
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPTS = os.path.join(os.path.dirname(HERE), "scripts")
sys.path.insert(0, SCRIPTS)

import realtime  # noqa: E402
import workflows  # noqa: E402
from twitterapi import APIError, IncompleteDataError  # noqa: E402
from tests.fakes import ScriptedClient, client_class  # noqa: E402

NOW = 1_800_000_000
FMT = "%a %b %d %H:%M:%S %z %Y"


def tw(tweet_id, ts, handle="alpha", **extra):
    record = {"id": str(tweet_id),
              "createdAt": datetime.fromtimestamp(ts, timezone.utc).strftime(FMT),
              "author": {"userName": handle}, "likeCount": 0, "text": "t"}
    record.update(extra)
    return record


def search_calls(client):
    return [p for _, path, p in client.raw_calls if "advanced_search" in path]


class CliCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.clients = []

    def path(self, name):
        return os.path.join(self.tmp.name, name)

    def run_cli(self, module, argv, handler=None, json_handler=None):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.ExitStack() as stack:
            stack.enter_context(mock.patch.object(module, "Client", client_class(
                raw=handler or self.no_request,
                raw_json=json_handler or (lambda path, body: {"status": "success"}),
                created=self.clients)))
            stack.enter_context(contextlib.redirect_stdout(out))
            stack.enter_context(contextlib.redirect_stderr(err))
            rc = module.main(argv)
        return rc, out.getvalue(), err.getvalue()

    @staticmethod
    def no_request(path, params):
        raise AssertionError(f"unexpected paid request to {path}")

    def lines(self, stdout):
        return [json.loads(line) for line in stdout.splitlines() if line.strip()]


# --------------------------------------------------------------- defect 1 --
class TestCeilingNotTightenedByLibraryDefault(CliCase):
    def test_history_cli_keeps_the_cli_ceiling(self):
        """`--max-usd 50` must not be clamped to the library's $5 default."""
        rc, out, _ = self.run_cli(
            workflows, ["history", "alpha", "--max-usd", "50",
                        "--store", self.path("h.db")],
            lambda path, p: {"tweets": [tw(1, NOW)], "has_next_page": False})
        self.assertEqual(rc, 0)
        self.assertEqual(self.clients[0].max_usd, 50)

    def test_monitor_library_keeps_a_supplied_client_ceiling(self):
        client = ScriptedClient(raw=lambda path, p: {"tweets": [], "has_next_page": False},
                      max_usd=50)
        with mock.patch.object(workflows.time, "time", return_value=NOW), \
             contextlib.redirect_stderr(io.StringIO()):
            workflows.monitor(["alpha"], client=client, once=True,
                              on_tweet=lambda t: None)
        self.assertEqual(client.max_usd, 50)


# --------------------------------------------------------------- defect 2 --
class TestSearchDrift(CliCase):
    def test_missing_tweets_field_is_partial_not_complete_zero(self):
        rc, out, _ = self.run_cli(
            workflows, ["history", "alpha", "--since", "2026-01-01",
                        "--until", "2026-02-01", "--store", self.path("d.db")],
            lambda path, p: {"status": "success", "data": {"tweets": [tw(1, NOW)]}})
        self.assertEqual(rc, 0)
        summary = self.lines(out)[-1]
        self.assertFalse(summary["complete"])
        self.assertEqual(summary["completeness"]["status"], "partial")
        self.assertIn("tweets", summary["completeness"]["reason"])

    def test_walker_raises_when_tweets_is_not_a_list(self):
        client = ScriptedClient(raw=lambda path, p: {"tweets": None, "has_next_page": False},
                      max_usd=1)
        with self.assertRaises(IncompleteDataError):
            list(workflows.search_window(client, "from:alpha", 0, NOW))

    def test_short_page_with_has_next_page_keeps_walking(self):
        pages = [
            {"tweets": [tw(f"n{k}", NOW - 10 - k) for k in range(5)],
             "has_next_page": True},
            {"tweets": [tw(f"o{k}", NOW - 500 - k) for k in range(3)],
             "has_next_page": False},
        ]
        client = ScriptedClient(raw=lambda path, p: pages.pop(0), max_usd=1)
        got = list(workflows.search_window(client, "from:alpha", None, NOW))
        self.assertEqual(len(got), 8, "a short page that asserts more is not the end")
        self.assertEqual(len(search_calls(client)), 2)
        self.assertIn(f"until_time:{NOW - 14 + 1}", search_calls(client)[1]["query"])


# --------------------------------------------------------------- defect 3 --
class TestNonexistentHandle(CliCase):
    def test_unknown_handle_is_an_error_not_a_complete_zero(self):
        def handler(path, params):
            if "advanced_search" in path:
                return {"tweets": [], "has_next_page": False}
            if path == "/twitter/user/info":
                raise APIError(200, "user not found", path)
            raise AssertionError(path)
        rc, out, _ = self.run_cli(
            workflows, ["history", "no_such_user_zz", "--store", self.path("u.db")],
            handler)
        self.assertEqual(rc, 1)
        body = json.loads(out)
        self.assertEqual(body["error"]["type"], "api")
        self.assertNotIn("complete", body)


# --------------------------------------------------------------- defect 4 --
class TestHistoryArgumentRefusals(CliCase):
    def assert_usage_before_client(self, argv):
        rc, out, _ = self.run_cli(workflows, argv)
        self.assertEqual(rc, 2, out)
        self.assertEqual(json.loads(out)["error"]["type"], "usage")
        self.assertEqual(self.clients, [], "refusal must precede client and spend")

    def test_missing_user_and_query_names_the_subcommand(self):
        """Codex review of PR 3: this post-parse refusal went through the
        top-level parser, which named `workflows.py` and pointed to its help
        rather than `workflows.py history --help`."""
        rc, out, _ = self.run_cli(workflows, ["history"])
        self.assertEqual(rc, 2, out)
        body = json.loads(out)
        self.assertEqual(body["error"]["type"], "usage")
        self.assertEqual(body["error"]["command"], "workflows.py history")
        self.assertIn("workflows.py history --help", body["help"][0])
        self.assertEqual(self.clients, [], "refusal must precede client and spend")

    def test_inverted_window_refused(self):
        self.assert_usage_before_client(
            ["history", "alpha", "--since", "2026-03-01", "--until", "2026-01-01"])

    def test_empty_window_refused(self):
        self.assert_usage_before_client(
            ["history", "alpha", "--since", "2026-01-01", "--until", "2026-01-01"])

    def test_malformed_date_is_usage_not_internal(self):
        self.assert_usage_before_client(["history", "alpha", "--since", "2026/01/01"])

    def test_user_with_query_refused(self):
        self.assert_usage_before_client(["history", "alpha", "--query", "bitcoin"])

    def test_include_retweets_with_query_is_refused_before_client(self):
        """This refusal ran after the client was built, so without a key it
        reported a setup error instead of the refusal."""
        rc, out, _ = self.run_cli(
            workflows, ["history", "--query", "x", "--include-retweets"])
        self.assertEqual(rc, 2, out)
        self.assertEqual(json.loads(out)["error"]["type"], "usage")
        self.assertEqual(self.clients, [], "the error must precede client and spend")

    def test_unenforceable_min_faves_query_is_usage_not_internal(self):
        """prepare_search_query's ValueError escaped as an internal error
        (exit 1) for a query the user must rewrite."""
        self.assert_usage_before_client(
            ["history", "--query", "from:a OR from:b min_faves:10"])

    def test_failure_after_streamed_records_stays_one_json_line(self):
        """An unexpected failure after records were streamed printed an
        indented error document and broke the JSON Lines stream."""
        pages = [{"tweets": [tw(k, NOW - k) for k in range(20)],
                  "has_next_page": True}, OSError("connection reset")]

        def handler(path, params):
            item = pages.pop(0)
            if isinstance(item, BaseException):
                raise item
            return item
        rc, out, _ = self.run_cli(
            workflows, ["history", "--query", "foo", "--store", self.path("n.db")],
            handler)
        self.assertEqual(rc, 1)
        lines = self.lines(out)            # every line must parse on its own
        self.assertEqual(len(lines), 21)
        self.assertEqual(lines[-1]["error"]["type"], "runtime")

    def test_library_rejects_inverted_window_before_request(self):
        client = ScriptedClient(max_usd=1)
        with self.assertRaises(ValueError):
            workflows.history_search("from:alpha", "2026-03-01", "2026-01-01",
                                     client=client)


# --------------------------------------------------------------- defect 5 --
class TestSinceCoverage(CliCase):
    """A dated window that the walk exhausted is complete, even when the
    account was quiet at its start. A --since variant of the lifetime
    index-depth check labelled @karpathy's Oct 1-24 history partial on
    2026-10-23 because his oldest post in the window was the next day; that
    gap is the ordinary case, not evidence of missing data."""

    def test_quiet_start_of_a_dated_window_is_not_partial(self):
        oldest = int(datetime(2026, 3, 10, tzinfo=timezone.utc).timestamp())
        rec = tw("only", oldest)
        rec["author"].update({"createdAt": "Mon Apr 06 16:54:47 +0000 2009",
                              "statusesCount": 20_000})
        rc, out, err = self.run_cli(
            workflows, ["history", "alpha", "--since", "2026-01-01",
                        "--store", self.path("c.db")],
            lambda path, p: {"tweets": [rec], "has_next_page": False})
        self.assertEqual(rc, 0)
        summary = self.lines(out)[-1]
        self.assertTrue(summary["complete"], summary)
        self.assertNotIn("index coverage: partial", err.lower())

    def test_open_ended_history_short_of_account_creation_is_partial(self):
        oldest = int(datetime(2026, 3, 10, tzinfo=timezone.utc).timestamp())
        rec = tw("only", oldest)
        rec["author"].update({"createdAt": "Mon Apr 06 16:54:47 +0000 2009",
                              "statusesCount": 20_000})
        rc, out, err = self.run_cli(
            workflows, ["history", "alpha", "--store", self.path("c.db")],
            lambda path, p: {"tweets": [rec], "has_next_page": False})
        self.assertEqual(rc, 0)
        summary = self.lines(out)[-1]
        self.assertFalse(summary["complete"])
        self.assertIn("index coverage: partial", err.lower())


# ------------------------------------------------------------ defects 6-8 --
class TestMonitorResume(CliCase):
    @staticmethod
    def busy(path, params):
        until = int(re.search(r"until_time:(\d+)", params["query"]).group(1))
        return {"tweets": [tw(f"t{until - 1 - k}", until - 1 - k) for k in range(20)],
                "has_next_page": True}

    def run_monitor(self, state):
        with mock.patch.object(workflows.time, "time", return_value=NOW):
            return self.run_cli(
                workflows, ["monitor", "alpha", "--once", "--state", state,
                            "--max-usd", "0.006", "--lookback", "100"], self.busy)

    def test_ceiling_stopped_poll_does_not_reemit_on_restart(self):
        state = self.path("mon.json")
        rc1, out1, _ = self.run_monitor(state)
        rc2, out2, _ = self.run_monitor(state)
        self.assertEqual((rc1, rc2), (0, 0))
        first = [r["id"] for r in self.lines(out1) if "id" in r]
        second = [r["id"] for r in self.lines(out2) if "id" in r]
        self.assertTrue(first)
        self.assertEqual(set(first) & set(second), set(),
                         "restart re-emitted tweets already delivered")
        with open(state) as fh:
            self.assertEqual(json.load(fh)["last_ts"], NOW - 100,
                             "an unfinished window must not be checkpointed")

    def test_stop_summary_is_single_line_json_lines(self):
        rc, out, _ = self.run_monitor(self.path("jsonl.json"))
        self.assertEqual(rc, 0)
        rows = self.lines(out)          # raises on any non-JSON line
        self.assertFalse(rows[-1]["complete"])
        self.assertEqual(len(out.strip().splitlines()), len(rows))

    def test_invalid_monitor_arguments_refused_before_client(self):
        for argv in (["monitor", "alpha", "--interval", "0", "--once"],
                     ["monitor", "alpha", "--interval", "-5"],
                     ["monitor", ",", "--once"]):
            with self.subTest(argv=argv):
                rc, out, _ = self.run_cli(workflows, argv)
                self.assertEqual(rc, 2)
                self.assertEqual(json.loads(out)["error"]["type"], "usage")
        self.assertEqual(self.clients, [])

    def test_poll_window_overlaps_previous_window(self):
        """Late-indexed posts older than last_ts must still be reachable."""
        state = self.path("overlap.json")
        with open(state, "w") as fh:
            json.dump({"last_ts": NOW - 60, "seen_ids": ["old"]}, fh)
        client = ScriptedClient(raw=lambda path, p: {
            "tweets": [tw("late", NOW - 90), tw("old", NOW - 100)],
            "has_next_page": False}, max_usd=1)
        emitted = []
        with mock.patch.object(workflows.time, "time", return_value=NOW), \
             contextlib.redirect_stderr(io.StringIO()):
            workflows.monitor(["alpha"], client=client, state_file=state,
                              once=True, on_tweet=lambda t: emitted.append(t["id"]),
                              overlap=120)
        self.assertIn(f"since_time:{NOW - 60 - 120}", search_calls(client)[0]["query"])
        self.assertEqual(emitted, ["late"], "seen ids must drop overlap repeats")


# --------------------------------------------------------------- defect 9 --
class TestAudienceDenominator(CliCase):
    @staticmethod
    def handler(path, params):
        if path == "/twitter/user/info":
            return {"data": {"id": "42", "userName": "alpha", "followers": 10_000_000}}
        if "verifiedFollowers" in path:
            return {"followers": [{"id": "1", "userName": "v"}],
                    "has_next_page": True, "next_cursor": "c1"}
        raise AssertionError(path)

    def test_verified_only_is_not_priced_from_total_followers(self):
        rc, out, _ = self.run_cli(
            workflows, ["audience", "alpha", "--verified-only", "--limit", "1"],
            self.handler)
        self.assertEqual(rc, 0, out)
        summary = self.lines(out)[-1]
        self.assertEqual(summary["records_count"], "1 of unknown total")

    def test_verified_only_unbounded_walk_is_not_refused_up_front(self):
        def handler(path, params):
            if "verifiedFollowers" in path:
                return {"followers": [{"id": "1"}], "has_next_page": False}
            return self.handler(path, params)
        rc, out, err = self.run_cli(
            workflows, ["audience", "alpha", "--verified-only"], handler)
        self.assertEqual(rc, 0, out)
        self.assertNotIn("1,800", err)
        self.assertEqual(self.lines(out)[-1]["records_count"], "1 of 1 total")

    def test_limit_zero_is_usage_not_unlimited(self):
        rc, out, _ = self.run_cli(workflows, ["audience", "alpha", "--limit", "0"])
        self.assertEqual(rc, 2)
        self.assertEqual(json.loads(out)["error"]["type"], "usage")
        self.assertEqual(self.clients, [])


# -------------------------------------------------------------- defect 11 --
RULE = {"rule_id": "r1", "tag": "ai", "value": "from:openai",
        "interval_seconds": 0.1, "is_effect": 0}


class TestRealtimeRules(CliCase):
    def test_drifted_rules_response_is_not_zero_rules(self):
        rc, out, err = self.run_cli(
            realtime, ["rules", "list"],
            lambda path, p: {"status": "success", "data": {"rules": [dict(RULE)]}})
        self.assertEqual(rc, 1)
        self.assertEqual(json.loads(out)["error"]["type"], "api")
        self.assertNotIn("nothing is billing", err)

    def test_unrecognized_is_effect_may_be_billing(self):
        client = ScriptedClient(raw=lambda path, p: {"rules": [dict(RULE, is_effect="true")]})
        warning = realtime.active_rule_warning(client)
        self.assertNotIn("nothing is billing", warning)
        self.assertIn("UNKNOWN", warning)
        self.assertIn("may be billing", warning)

    def test_every_mutation_prints_the_active_rule_count(self):
        rules = lambda path, p: {"rules": [dict(RULE, is_effect=1)]}
        for argv in (["rules", "add", "--tag", "ai", "--value", "x", "--confirm"],
                     ["rules", "activate", "--rule-id", "r1", "--confirm"],
                     ["rules", "deactivate", "--rule-id", "r1", "--confirm"],
                     ["rules", "delete", "--rule-id", "r1", "--confirm"]):
            with self.subTest(argv=argv):
                rc, out, err = self.run_cli(realtime, argv, rules)
                self.assertEqual(rc, 0, out)
                self.assertIn("ACTIVE rule(s)", err)


# -------------------------------------------------------------- defect 13 --
class TestRuleToggle(CliCase):
    def test_activate_keeps_current_interval_tag_and_value(self):
        rc, out, _ = self.run_cli(realtime,
                                  ["rules", "activate", "--rule-id", "r1", "--confirm"],
                                  lambda path, p: {"rules": [dict(RULE)]})
        self.assertEqual(rc, 0, out)
        updates = [b for _, path, b in self.clients[0].json_calls
                   if path.endswith("update_rule")]
        self.assertEqual(updates, [{"rule_id": "r1", "tag": "ai",
                                    "value": "from:openai",
                                    "interval_seconds": 0.1, "is_effect": 1}])

    def test_out_of_bounds_interval_refused_before_request(self):
        rc, out, _ = self.run_cli(realtime, ["rules", "activate", "--rule-id", "r1",
                                             "--interval", "0.01", "--confirm"])
        self.assertEqual(rc, 2)
        self.assertEqual(json.loads(out)["error"]["type"], "usage")
        self.assertEqual(self.clients, [])

    def test_update_rule_enforces_bounds(self):
        client = ScriptedClient()
        with self.assertRaises(ValueError):
            realtime.update_rule(client, "r1", "ai", "x", 100_000, is_effect=1,
                                 confirm=True)
        self.assertEqual(client.json_calls, [])


# -------------------------------------------------------------- defect 12 --
class FakeSocket:
    script = []

    def __init__(self, *args, **kwargs):
        self.events = list(FakeSocket.script)

    def connect(self):
        # An exception CLASS first in the script fails the handshake itself.
        if self.events and isinstance(self.events[0], type):
            raise self.events.pop(0)("connect failed")
        return self

    def recv(self):
        item = self.events.pop(0)
        if isinstance(item, BaseException):
            raise item
        return 1, json.dumps(item).encode()

    def close(self):
        pass


def tweet_event(i):
    return {"event_type": "tweet", "rule_tag": "ai",
            "tweets": [{"id": str(i), "text": "x"}]}


class TestRealtimeStream(CliCase):
    active = staticmethod(lambda path, p: {"rules": [dict(RULE, is_effect=1)]})

    def stream(self, script, argv=("stream",)):
        FakeSocket.script = script
        with mock.patch.object(realtime, "WebSocketClient", FakeSocket):
            return self.run_cli(realtime, list(argv), self.active)

    def test_connect_timeout_is_an_error_not_complete_zero(self):
        rc, out, _ = self.stream([TimeoutError], ("stream", "--seconds", "30"))
        self.assertEqual(rc, 1)
        self.assertEqual(json.loads(out)["error"]["type"], "runtime")

    def test_idle_after_records_reports_real_count_and_partial(self):
        rc, out, _ = self.stream([tweet_event(1), tweet_event(2), tweet_event(3),
                                  TimeoutError("timed out")])
        self.assertEqual(rc, 0)
        rows = self.lines(out)
        summary = rows[-1]
        self.assertEqual(len(rows) - 1, 3)
        self.assertEqual(summary["completeness"]["records_returned"], 3)
        self.assertEqual(summary["events_count"], "3 of unknown total")
        self.assertFalse(summary["complete"],
                         "an unbounded stream that ends on its own is partial")

    def test_server_close_after_records_keeps_json_lines(self):
        rc, out, _ = self.stream([tweet_event(1),
                                  ConnectionError("server closed, code=1013")])
        self.assertEqual(rc, 0)
        rows = self.lines(out)          # raises on an indented document
        self.assertEqual(len(out.strip().splitlines()), len(rows))
        self.assertFalse(rows[-1]["complete"])
        self.assertEqual(rows[-1]["completeness"]["records_returned"], 1)


if __name__ == "__main__":
    unittest.main()
