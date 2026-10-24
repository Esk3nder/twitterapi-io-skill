"""Offline contract tests for the shared JSON AXI command boundary."""
from __future__ import annotations

import contextlib
import io
import json
import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

from tests.e2e_base import SCRIPTS  # noqa: F401

import axi


class TestJsonBoundary(unittest.TestCase):
    def test_truncation_fields_and_full_hint(self):
        payload = {"tweets": [{"id": "1", "createdAt": "now",
                                "text": "x" * 805, "likeCount": 7}]}
        out = axi.prepare_output(
            payload, command="history", fields="id,text", full=False,
            collections={"tweets": axi.TWEET_FIELDS}, context="for @alice")

        self.assertEqual(set(out["tweets"][0]), {"id", "text"})
        self.assertEqual(
            out["tweets"][0]["text"],
            "x" * 800 + "... (truncated, 805 chars total)")
        self.assertEqual(out["tweets_count"], "1 of 1 total")
        self.assertTrue(any("--full" in hint for hint in out["help"]))

    def test_partial_signal_is_explicit_and_repeated(self):
        out = axi.success("history", {"tweets": [{"id": "1"}]},
                          complete=False, reason="spend ceiling reached",
                          resume="Run with a higher --max-usd")

        self.assertFalse(out["complete"])
        self.assertEqual(out["completeness"]["status"], "partial")
        self.assertEqual(out["completeness"]["records_returned"], 1)
        self.assertIn("spend ceiling", out["completeness"]["reason"])
        self.assertIn("--max-usd", out["completeness"]["resume"])

    def test_empty_collection_has_context(self):
        out = axi.prepare_output(
            {"rules": []}, command="rules list",
            collections={"rules": axi.RULE_FIELDS},
            context="for this twitterapi.io account")
        self.assertEqual(out["rules"], [])
        self.assertEqual(
            out["rules_state"],
            "0 rules found for this twitterapi.io account")

    def test_unknown_field_lists_valid_set(self):
        with self.assertRaises(axi.UsageError) as cm:
            axi.parse_fields("id,secret", axi.TWEET_FIELDS, "history")
        self.assertIn("unknown field secret", str(cm.exception))
        self.assertIn("valid fields", str(cm.exception))



class TestExitContract(unittest.TestCase):
    """The one CLI boundary: 2 = usage, refusal, or setup; 1 = API, runtime,
    or internal. Every failure is ONE JSON line on stdout with a help line."""

    def test_every_failure_kind_maps_to_its_type_and_exit_code(self):
        from twitterapi import (APIError, CostLimitExceeded,
                                IncompleteDataError, SetupError)
        cases = [(SetupError("no key"), "setup", 2),
                 (axi.UsageError("bad flag"), "usage", 2),
                 (CostLimitExceeded("over ceiling"), "refusal", 2),
                 (APIError(500, "boom", "/p"), "api", 1),
                 (IncompleteDataError("drifted"), "api", 1),
                 (OSError("DNS unavailable"), "runtime", 1),
                 (KeyError("bug"), "internal", 1)]
        for exc, kind, code in cases:
            def fail(_argv, exc=exc):
                raise exc
            output = io.StringIO()
            with self.subTest(kind=kind, exc=type(exc).__name__), \
                    contextlib.redirect_stdout(output):
                rc = axi.run_cli("tool.py", fail, ["sub"])
            self.assertEqual(rc, code)
            self.assertEqual(output.getvalue().count("\n"), 1, "one JSON line")
            body = json.loads(output.getvalue())
            self.assertEqual(body["error"]["type"], kind)
            self.assertEqual(body["error"]["command"], "tool.py sub")
            self.assertTrue(body["help"])

    def test_missing_key_is_setup_exit_2_for_every_paid_command(self):
        """Run as real scripts: a class raised from a script's own __main__
        module is not the class axi.classify imports, so only a subprocess
        catches that (twitterapi.py --balance exited 1 that way)."""
        import subprocess
        import sys
        with tempfile.TemporaryDirectory() as home:
            store = os.path.join(home, "x.db")
            cases = [["jobs.py", "brief", "alpha", "--store", store],
                     ["workflows.py", "history", "alpha", "--store", store],
                     ["workflows.py", "audience", "alpha", "--limit", "1"],
                     ["workflows.py", "monitor", "alpha", "--once"],
                     ["realtime.py", "rules", "list"],
                     ["realtime.py", "stream", "--seconds", "5"],
                     ["twitterapi.py", "--balance"],
                     ["verify.py"]]
            env = {k: v for k, v in os.environ.items() if k != "TWITTERAPI_IO_KEY"}
            env["HOME"] = home
            for script, *argv in cases:
                with self.subTest(argv=[script, *argv]):
                    run = subprocess.run(
                        [sys.executable, os.path.join(SCRIPTS, script), *argv],
                        capture_output=True, text=True, timeout=30, env=env)
                    self.assertEqual(run.returncode, 2, run.stdout + run.stderr)
                    self.assertEqual(json.loads(run.stdout)["error"]["type"], "setup")
                    self.assertNotIn("Traceback", run.stderr)

    def test_help_works_without_a_key(self):
        import subprocess
        import sys
        env = {k: v for k, v in os.environ.items() if k != "TWITTERAPI_IO_KEY"}
        for argv in (["workflows.py", "--help"], ["workflows.py", "history", "--help"],
                     ["workflows.py", "monitor", "--help"],
                     ["realtime.py", "rules", "activate", "--help"]):
            run = subprocess.run([sys.executable, os.path.join(SCRIPTS, argv[0]),
                                  *argv[1:]], capture_output=True, text=True,
                                 timeout=20, env=env)
            self.assertEqual(run.returncode, 0, (argv, run.stdout, run.stderr))
            self.assertIn("usage:", run.stdout)


class TestArgumentParser(unittest.TestCase):
    def test_unknown_flag_is_structured_with_valid_flags(self):
        parser = axi.ArgumentParser(prog="tool")
        parser.add_argument("--state")
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            with self.assertRaises(SystemExit) as cm:
                parser.parse_args(["--stat", "closed"])
        self.assertEqual(cm.exception.code, 2)
        body = json.loads(output.getvalue())
        self.assertEqual(body["error"]["type"], "usage")
        self.assertIn("--stat", body["error"]["message"])
        self.assertIn("--state", body["help"][0])


class TestFastPaths(unittest.TestCase):
    def test_all_entry_points_accept_version_aliases(self):
        import subprocess
        import sys

        for script in ("jobs.py", "workflows.py", "realtime.py", "verify.py",
                       "twitterapi.py", "store.py", "cohort.py"):
            for flag in ("-v", "-V", "--version"):
                run = subprocess.run(
                    [sys.executable, os.path.join(SCRIPTS, script), flag],
                    capture_output=True, text=True, timeout=10,
                    env={**os.environ, "TWITTERAPI_IO_KEY": ""})
                self.assertEqual(run.returncode, 0, (script, flag, run.stderr))
                self.assertEqual(run.stdout, axi.VERSION + "\n")
                self.assertEqual(run.stderr, "")

    def test_no_args_jobs_and_workflows_are_content_first_json(self):
        import subprocess
        import sys

        with tempfile.TemporaryDirectory() as td:
            for script in ("jobs.py", "workflows.py"):
                run = subprocess.run(
                    [sys.executable, os.path.join(SCRIPTS, script)],
                    capture_output=True, text=True, timeout=10,
                    env={**os.environ, "HOME": td, "TWITTERAPI_IO_KEY": ""})
                self.assertEqual(run.returncode, 0, run.stderr)
                body = json.loads(run.stdout)
                self.assertIn("bin", body)
                self.assertIn("description", body)
                self.assertIn("store_state", body)
                self.assertIn("cohorts_count", body)
                self.assertTrue(body["complete"])
                self.assertEqual(body["completeness"]["status"], "complete")
                self.assertNotIn("usage:", run.stdout.lower())

    def test_subcommand_help_has_flags_defaults_and_examples(self):
        import subprocess
        import sys

        commands = [
            ("jobs.py", "brief"), ("jobs.py", "authority"),
            ("jobs.py", "drift"), ("jobs.py", "diffusion"),
            ("workflows.py", "audience"), ("workflows.py", "history"),
            ("workflows.py", "monitor"), ("realtime.py", "stream"),
        ]
        for script, command in commands:
            run = subprocess.run(
                [sys.executable, os.path.join(SCRIPTS, script), command, "--help"],
                capture_output=True, text=True, timeout=10)
            self.assertEqual(run.returncode, 0, (script, command, run.stderr))
            self.assertIn("options:", run.stdout)
            self.assertIn("Example", run.stdout)

    def test_invalid_fields_fail_before_api_configuration(self):
        import subprocess
        import sys

        commands = [
            ["jobs.py", "brief", "alice", "--fields", "secret"],
            ["workflows.py", "history", "alice", "--fields", "secret"],
            ["realtime.py", "rules", "list", "--fields", "secret"],
        ]
        for args in commands:
            run = subprocess.run(
                [sys.executable, os.path.join(SCRIPTS, args[0]), *args[1:]],
                capture_output=True, text=True, timeout=10,
                env={**os.environ, "TWITTERAPI_IO_KEY": ""})
            self.assertEqual(run.returncode, 2, (args, run.stderr))
            body = json.loads(run.stdout)
            self.assertEqual(body["error"]["type"], "usage")
            self.assertIn("valid fields", body["error"]["message"])
            self.assertNotIn("TWITTERAPI_IO_KEY", run.stdout)


class TestJobsPresentation(unittest.TestCase):
    def test_entity_brief_is_minimal_truncated_and_counted(self):
        import jobs

        tweets = [{"id": str(i), "createdAt": "now", "text": "x" * 900,
                   "likeCount": 100 - i, "retweetCount": i}
                  for i in range(16)]
        client = SimpleNamespace(
            user_info=lambda _handle: {"name": "Alice", "followers": 2,
                                       "description": "y" * 900},
            spend_report=lambda: "$0", store=None)
        args = SimpleNamespace(job="brief", target="alice", fields=None,
                               full=False, store=None)
        with mock.patch.object(jobs, "corpus", return_value=tweets):
            raw = jobs.entity_brief("alice", client=client)
        body = jobs._format_job(args, raw)

        self.assertEqual(body["top_tweets_count"], "15 of 16 total")
        self.assertEqual(set(body["top_tweets"][0]),
                         {"id", "created_at", "likes", "text"})
        self.assertTrue(body["top_tweets"][0]["text"].endswith(
            "... (truncated, 900 chars total)"))
        self.assertTrue(any("--full" in hint for hint in body["help"]))

    def test_drift_totals_survive_sixty_row_display_cap(self):
        import jobs
        from store import Store

        # drift is a local job now: it reads saved versions from the store
        # and needs no Client (contract change, see test_07_review_jobs).
        path = os.path.join(tempfile.mkdtemp(prefix="axi-drift-"), "s.db")
        with Store(path) as store:
            store.save_cohort("scene", [(str(i), f"old{i}", 1.0, "t")
                                        for i in range(70)])
            store.save_cohort("scene", [(str(i + 100), f"new{i}", 1.0, "t")
                                        for i in range(75)])
            body = jobs.cohort_drift("scene", store=store)

        self.assertEqual(len(body["joined"]), 60)
        self.assertEqual(body["joined_total"], 75)
        self.assertEqual(len(body["left"]), 60)
        self.assertEqual(body["left_total"], 70)


if __name__ == "__main__":
    unittest.main()

class TestRecordCountAndDashboardHonesty(unittest.TestCase):
    def test_records_returned_sums_all_record_lists(self):
        """A drift payload whose joined is empty but left holds departures must
        not report records_returned=0 — counting only the FIRST list made a
        members-only-left diff read as an empty result."""
        from axi import _record_count
        self.assertEqual(_record_count({"joined": [], "left": ["a", "b", "c"]}), 3)
        self.assertEqual(_record_count({"tweets": ["x", "y"]}), 2)
        self.assertEqual(_record_count({"help": ["h"], "left": ["a"]}), 1,
                         "help lists stay excluded from the record count")

    def test_dashboard_sees_rows_committed_to_the_wal(self):
        """The dashboard's read-only connection must consult the -wal.
        immutable=1 skipped it: a store holding 25 committed tweets (writer
        open, WAL uncheckpointed) dashboarded as tweets=0 — stale data
        presented as definite, this project's core bug class."""
        from axi import dashboard
        from store import Store
        path = os.path.join(tempfile.mkdtemp(prefix="axi-wal-"), "s.db")
        s = Store(path)          # writer stays open -> nothing checkpointed
        s.put_tweets([{"id": str(i),
                       "createdAt": "Mon Aug 10 17:16:53 +0000 2026",
                       "author": {"id": "1", "userName": "a"}}
                      for i in range(25)])
        d = dashboard("x.py", "d", store_path=path)
        self.assertEqual(d["store_state"]["tweets"], 25)
        s.close()

