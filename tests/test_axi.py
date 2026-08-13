"""Offline contract tests for the shared JSON AXI command boundary."""
from __future__ import annotations

import contextlib
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
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

    def test_emit_is_json_on_stdout(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            axi.emit({"ok": True})
        self.assertEqual(json.loads(output.getvalue()), {"ok": True})

    def test_unexpected_runtime_failure_is_structured(self):
        def fail(_argv):
            raise OSError("DNS unavailable")

        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            rc = axi.run_cli("tool.py", fail, [])
        body = json.loads(output.getvalue())
        self.assertEqual(rc, 1)
        self.assertEqual(body["error"]["type"], "runtime")
        self.assertIn("DNS unavailable", body["error"]["message"])
        self.assertTrue(body["help"])


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


class TestHookSetup(unittest.TestCase):
    def test_project_setup_is_explicit_idempotent_and_repairs_path(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".codex").mkdir()
            (root / ".codex/config.toml").write_text(
                "model = \"gpt-5\"\n\n[features]\nhooks = false\nweb = true\n")
            first = axi.setup_hooks(root, agents=["claude", "codex", "opencode"],
                                    command="/old/jobs.py session-context")
            second = axi.setup_hooks(root, agents=["claude", "codex", "opencode"],
                                     command="/new/jobs.py session-context")

            self.assertEqual(set(first["installed"]), {"claude", "codex", "opencode"})
            self.assertEqual(set(second["repaired"]), {"claude", "codex", "opencode"})
            claude = json.loads((root / ".claude/settings.json").read_text())
            codex = json.loads((root / ".codex/hooks.json").read_text())
            self.assertIn("/new/jobs.py", json.dumps(claude))
            self.assertIn("/new/jobs.py", json.dumps(codex))
            self.assertTrue((root / ".opencode/plugins/twitterapi-io.js").exists())
            config = (root / ".codex/config.toml").read_text()
            self.assertEqual(config.count("[features]"), 1)
            self.assertIn("hooks = true", config)
            self.assertIn("web = true", config)

            third = axi.setup_hooks(root, agents=["claude", "codex", "opencode"],
                                    command="/new/jobs.py session-context")
            self.assertEqual(set(third["unchanged"]), {"claude", "codex", "opencode"})
            checked = axi.check_hooks(
                root, agents=["claude", "codex", "opencode"],
                command="/new/jobs.py session-context")
            self.assertTrue(checked["ok"])


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
            ("jobs.py", "drift"), ("jobs.py", "setup"),
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

        class FakeCohort:
            def __init__(self, rows): self.rows = rows
            def __len__(self): return len(self.rows)
            def __iter__(self): return iter(self.rows)
            def minus(self, other):
                other_ids = {row["account_id"] for row in other.rows}
                return FakeCohort([row for row in self.rows
                                   if row["account_id"] not in other_ids])

        old = FakeCohort([{"account_id": str(i), "user_name": f"old{i}"}
                          for i in range(70)])
        new = FakeCohort([{"account_id": str(i + 100), "user_name": f"new{i}"}
                          for i in range(75)])
        client = SimpleNamespace(store=SimpleNamespace(
            cohort_versions=lambda _name: [1, 2]))
        with mock.patch.object(jobs.Cohort, "load", side_effect=[old, new]):
            body = jobs.cohort_drift("scene", client=client)

        self.assertEqual(len(body["joined"]), 60)
        self.assertEqual(body["joined_total"], 75)
        self.assertEqual(len(body["left"]), 60)
        self.assertEqual(body["left_total"], 70)


if __name__ == "__main__":
    unittest.main()
