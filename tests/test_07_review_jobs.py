"""Negative tests for the 2026-10-23 adversarial review of jobs.py / axi.py.

Each test pins one defect: before the fix it failed (wrong exit code, a
confident zero/False/complete read from ABSENT data, or a feature that should
not exist). No network: Client._raw/_raw_json are replaced by fakes that still
run the real budget guard and billing floor, so spend accounting is real.
"""
from __future__ import annotations

import contextlib
import io
import json
import os
import tempfile
import time
import unittest
from datetime import datetime, timezone
from unittest import mock

from tests.e2e_base import SCRIPTS  # noqa: F401  (SCRIPTS puts scripts/ on sys.path)
from tests.fakes import client_class

import jobs
import twitterapi
from store import Store


def fmt(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime(
        "%a %b %d %H:%M:%S +0000 %Y")


class JobsCase(unittest.TestCase):
    """Runs jobs.main with a fake transport and an isolated store."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="jobs-review-")
        self.db = os.path.join(self.tmp, "store.db")
        self.requests = []

    def run_jobs(self, argv, raw=None, raw_json=None, *, key="test"):
        created = []
        env = {"HOME": self.tmp}
        if key:
            env["TWITTERAPI_IO_KEY"] = key
        out, err = io.StringIO(), io.StringIO()
        with mock.patch.dict(os.environ, env), \
             mock.patch.object(jobs, "Client", client_class(
                 raw=raw or (), raw_json=raw_json or (), created=created)), \
             contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            if not key:
                os.environ.pop("TWITTERAPI_IO_KEY", None)
            try:
                code = jobs.main(argv)
            except SystemExit as exc:          # argparse usage errors
                code = exc.code
        self.requests += [(path, payload) for c in created
                          for _, path, payload in c.raw_calls + c.json_calls]
        return code, json.loads(out.getvalue())


# ------------------------------------------- 1. ceiling mid-job = partial --
class TestCeilingMidJobIsPartial(JobsCase):
    def busy_raw(self):
        now = int(time.time())

        def page(ut):
            return [{"id": f"t{ut - k}", "createdAt": fmt(ut - 1 - k * 60),
                     "likeCount": 1, "author": {"userName": "busy"}}
                    for k in range(20)]

        def raw(path, params):
            if path.endswith("user/info"):
                return {"data": {"userName": "busy", "followers": 10}}
            if path.endswith("advanced_search"):
                ut = int(params["query"].split("until_time:")[1])
                return {"tweets": page(ut), "has_next_page": True}
            raise AssertionError(path)

        def raw_json(path, body):
            return {"results": {"query_0": {"tweets": page(now),
                                            "has_next_page": True}}}
        return raw, raw_json

    def test_brief_ceiling_after_spend_returns_partial_contract(self):
        raw, raw_json = self.busy_raw()
        code, body = self.run_jobs(
            ["brief", "busy", "--store", self.db, "--max-usd", "0.02"], raw, raw_json)
        self.assertEqual(code, 0, body)
        self.assertFalse(body["complete"])
        self.assertEqual(body["completeness"]["status"], "partial")
        self.assertIn("ceiling", body["completeness"]["reason"])
        self.assertIn("--max-usd", body["completeness"]["resume"])
        self.assertIn("$", body["spend"])

    def test_brief_ceiling_mid_walk_keeps_the_tweets_it_bought(self):
        """Integration of corpus() with the shared walker: the first bulk page
        and every walked page before the ceiling were paid for and stored, so
        the partial must carry them rather than 0 tweets."""
        raw, raw_json = self.busy_raw()
        code, body = self.run_jobs(
            ["brief", "busy", "--store", self.db, "--max-usd", "0.02"], raw, raw_json)
        self.assertEqual(code, 0, body)
        self.assertFalse(body["complete"])
        self.assertGreaterEqual(body["tweet_count"], 20, body)

    def test_true_pre_spend_refusal_stays_exit_two(self):
        raw, raw_json = self.busy_raw()
        code, body = self.run_jobs(
            ["brief", "busy", "--store", self.db, "--max-usd", "0.00001"],
            raw, raw_json)
        self.assertEqual(code, 2)
        self.assertEqual(body["error"]["type"], "refusal")
        self.assertEqual(self.requests, [])

    def test_records_a_walker_attached_to_its_stop_are_returned(self):
        exc = twitterapi.CostLimitExceeded("Spend ceiling hit mid-window")
        exc.records = [{"id": "kept", "text": "paid", "likeCount": 3,
                        "createdAt": "now"}]

        class C:
            spent_usd = 0.01
            def user_info(self, _): return {"name": "n", "followers": 1}
            def spend_report(self): return "1 request"

        with mock.patch.object(jobs, "corpus", side_effect=exc):
            result = jobs.entity_brief("alpha", client=C())
        self.assertFalse(result["complete"])
        self.assertEqual([t["id"] for t in result["top_tweets"]], ["kept"])

    def test_diffusion_ceiling_keeps_paid_legs(self):
        def raw(path, params):
            kind = "r" if path.endswith("replies/v2") else "q"
            if path.endswith(("replies/v2", "quotes")):
                return {"tweets": [{"id": f"{kind}{i}",
                                    "createdAt": fmt(1_700_000_000 + i),
                                    "author": {"userName": f"{kind}{i}"}}
                                   for i in range(20)],
                        "has_next_page": False, "next_cursor": ""}
            raise AssertionError(path)       # retweeters must be refused
        # Replies + quotes cost 600 credits; the retweeters leg's reservation
        # (+360) crosses an 800-credit ($0.008) ceiling after money was spent.
        code, body = self.run_jobs(["diffusion", "1", "--store", self.db,
                                    "--limit", "20", "--max-usd", "0.008"], raw)
        self.assertEqual(code, 0, body)
        self.assertFalse(body["complete"])
        self.assertIn("ceiling", body["completeness"]["reason"])
        self.assertEqual(body["timed_engagers"], 40, "paid legs are kept")
        self.assertEqual(body["earliest_movers"][0]["handle"], "r0")


# ------------------------------------------------------------ 2. diffusion --
class TestDiffusionLimit(JobsCase):
    def test_counts_below_one_are_usage_errors_before_any_request(self):
        """--limit 0 returned an empty "partial" and --sample 0 an empty
        audit; neither is a real request."""
        for argv in (["diffusion", "123", "--limit", "0"],
                     ["authenticity", "a", "--sample", "0"],
                     ["authority", "q", "--sample", "-5"],
                     ["brief", "a", "--days", "0"]):
            code, body = self.run_jobs(argv + ["--store", self.db])
            self.assertEqual(code, 2, argv)
            self.assertEqual(body["error"]["type"], "usage", argv)
            self.assertIn("must be at least 1", body["error"]["message"])
        self.assertEqual(self.requests, [])

    def raw(self, path, params):
        now = int(time.time())
        cursor = int(params.get("cursor") or 0)
        if path.endswith("replies/v2"):
            start = cursor * 38
            tweets = [{"id": f"r{i}", "createdAt": fmt(now - i * 10),
                       "author": {"userName": f"u{i}", "followers": 1}}
                      for i in range(start, min(start + 38, 200))]
            return {"tweets": tweets, "has_next_page": start + 38 < 200,
                    "next_cursor": str(cursor + 1)}
        if path.endswith("quotes"):
            return {"tweets": [], "has_next_page": False, "next_cursor": ""}
        if path.endswith("retweeters"):
            return {"users": [{"id": "rt1"}], "has_next_page": False,
                    "next_cursor": ""}
        raise AssertionError(path)

    def test_limit_stop_is_partial_and_sends_latest(self):
        code, body = self.run_jobs(["diffusion", "123", "--store", self.db,
                                    "--limit", "50"], self.raw)
        self.assertEqual(code, 0, body)
        self.assertFalse(body["complete"])
        self.assertIn("--limit", body["completeness"]["resume"])
        replies = [p for path, p in self.requests if path.endswith("replies/v2")]
        self.assertEqual(replies[0].get("queryType"), "Latest")

    def test_raised_limit_finds_true_earliest_and_is_complete(self):
        code, body = self.run_jobs(["diffusion", "123", "--store", self.db,
                                    "--limit", "1000"], self.raw)
        self.assertEqual(code, 0, body)
        self.assertTrue(body["complete"])
        self.assertEqual(body["earliest_movers"][0]["handle"], "u199")


# -------------------------------------------------------------- 3. overlap --
class TestOverlapEstimate(JobsCase):
    def raw(self, path, params):
        if path.endswith("user/info"):
            return {"data": {"userName": params["userName"],
                             "followers": 1_200_000}}
        if path.endswith("followers_ids"):
            base = 0 if params["userName"] == "a" else 5
            return {"ids": [str(i) for i in range(base, base + 10)],
                    "has_next_page": False, "next_cursor": ""}
        raise AssertionError(path)

    def test_fully_cached_rerun_is_not_refused_and_costs_nothing(self):
        code, _ = self.run_jobs(["overlap", "a", "b", "--store", self.db,
                                 "--max-usd", "20"], self.raw)
        self.assertEqual(code, 0)
        self.requests.clear()
        code, body = self.run_jobs(["overlap", "a", "b", "--store", self.db],
                                   self.raw)
        self.assertEqual(code, 0, body)
        self.assertEqual(body["overlap"], 5)
        self.assertEqual(self.requests, [], "a cached rerun must be free")

    def test_cached_crawl_counts_for_any_handle_casing(self):
        """overlap() decided "cached" case-insensitively but the crawl looked
        pages up by a case-sensitive key: "A" skipped the estimate as cached,
        then paid to re-buy @a's followers."""
        code, _ = self.run_jobs(["overlap", "a", "b", "--store", self.db,
                                 "--max-usd", "20"], self.raw)
        self.assertEqual(code, 0)
        self.requests.clear()
        code, body = self.run_jobs(["overlap", "A", "@B", "--store", self.db],
                                   self.raw)
        self.assertEqual(code, 0, body)
        self.assertEqual(body["overlap"], 5)
        self.assertEqual(self.requests, [], "a re-cased rerun must be free")

    def test_unknown_follower_count_refuses(self):
        def raw(path, params):
            if path.endswith("user/info"):
                return {"data": {"userName": params["userName"]}}
            raise AssertionError(path)
        code, body = self.run_jobs(["overlap", "a", "b", "--store", self.db], raw)
        self.assertEqual(code, 2)
        self.assertEqual(body["error"]["type"], "refusal")
        self.assertIn("unknown", body["error"]["message"])


# ------------------------------------------------------------ 4. benchmark --
class TestBenchmarkHandles(JobsCase):
    def test_spaces_case_and_empties_are_normalized(self):
        now = int(time.time())

        def raw(path, params):
            if path.endswith("user/info"):
                return {"data": {"userName": params["userName"], "followers": 10}}
            raise AssertionError(path)

        def raw_json(path, body):
            res = {}
            for i, q in enumerate(body["queries"]):
                h = q["query"].split("from:")[1].split()[0]
                res[f"query_{i}"] = {"tweets": [{
                    "id": f"{h}{k}", "createdAt": fmt(now - 100 - k),
                    "likeCount": 5, "author": {"userName": h}} for k in range(3)],
                    "has_next_page": False}
            return {"results": res}

        code, body = self.run_jobs(["benchmark", "stripe, vercel,,@Stripe ",
                                    "--store", self.db], raw, raw_json)
        self.assertEqual(code, 0, body)
        self.assertEqual([e["handle"] for e in body["entities"]],
                         ["stripe", "vercel"])
        self.assertEqual([e["tweets_in_window"] for e in body["entities"]], [3, 3])
        queries = [q["query"] for path, b in self.requests
                   if path.endswith("bulk_advanced_search") for q in b["queries"]]
        self.assertTrue(all("from: " not in q for q in queries), queries)

    def test_no_handles_is_a_usage_error(self):
        code, body = self.run_jobs(["benchmark", " , ", "--store", self.db])
        self.assertEqual(code, 2)
        self.assertEqual(body["error"]["type"], "usage")


# ------------------------------------------------------------ 5. catalogue --
class TestCatalogueUnknownCounts(JobsCase):
    def test_absent_counts_are_not_averaged_as_zero(self):
        author = {"userName": "acct", "id": "1"}
        with Store(self.db) as s:
            s.put_tweets([
                {"id": "1", "createdAt": fmt(1_780_000_000), "likeCount": 10,
                 "viewCount": 1000, "bookmarkCount": 0, "author": author},
                {"id": "2", "createdAt": fmt(1_780_000_100), "likeCount": 0,
                 "author": author},
            ])
        code, body = self.run_jobs(["catalogue", "acct", "--store", self.db],
                                   key=None)
        self.assertEqual(code, 0, body)
        views = body["engagement"]["views"]
        self.assertEqual((views["average"], views["known"], views["unknown"]),
                         (1000.0, 1, 1))
        bookmarks = body["engagement"]["bookmarks"]
        self.assertEqual((bookmarks["average"], bookmarks["known"]), (0.0, 1),
                         "a reported zero is a known zero")
        self.assertEqual(body["engagement"]["likes"]["known"], 2)


# ---------------------------------------------------------------- 6. brief --
class TestBriefHonesty(JobsCase):
    def raw(self, path, params):
        if path.endswith("user/info"):
            return {"data": {"userName": "alpha", "followers": 3}}
        raise AssertionError(path)

    def raw_json(self, path, body):
        return {"results": {"query_0": {"tweets": [{
            "id": "1", "createdAt": fmt(int(time.time()) - 60), "likeCount": 4,
            "replyCount": 2, "quoteCount": 1, "viewCount": 90,
            "author": {"userName": "alpha"}}], "has_next_page": False}}}

    def test_absent_verification_is_unknown_and_requested_fields_are_filled(self):
        code, body = self.run_jobs(["brief", "alpha", "--store", self.db,
                                    "--fields", "views,replies,quotes"],
                                   self.raw, self.raw_json)
        self.assertEqual(code, 0, body)
        self.assertIsNone(body["profile"]["verified"])
        self.assertEqual(body["top_tweets"],
                         [{"views": 90, "replies": 2, "quotes": 1}])


# ------------------------------------------------------- 7/8. key handling --
class TestKeyHandling(JobsCase):
    def test_drift_runs_without_a_key(self):
        with Store(self.db) as s:
            s.save_cohort("scene", [("1", "alice", 1.0, "t")])
            s.save_cohort("scene", [("1", "Alice", 1.0, "t"), ("2", "bob", 1.0, "t")])
        code, body = self.run_jobs(["drift", "scene", "--store", self.db], key=None)
        self.assertEqual(code, 0, body)
        self.assertEqual((body["joined"], body["left"]), (["bob"], []))



# ------------------------------------------------------- 9. common flags ----
class TestCommonFlagsEitherSide(JobsCase):
    def test_max_usd_and_store_before_the_job_name(self):
        seen = {}

        def fake_job(a, c):
            seen.update(max_usd=c.max_usd, store=c.store.path
                        if hasattr(c.store, "path") else None)
            return {"ok": True}

        with mock.patch.dict(jobs.JOBS, {"brief": fake_job}):
            code, body = self.run_jobs(
                ["--max-usd", "0.5", "--store", self.db, "brief", "X"])
        self.assertEqual(code, 0, body)
        self.assertEqual(seen["max_usd"], 0.5)
        self.assertTrue(os.path.exists(self.db), "--store before the name binds")

    def test_after_the_name_still_works_and_wins(self):
        seen = {}
        with mock.patch.dict(jobs.JOBS, {
                "brief": lambda a, c: seen.setdefault("max", c.max_usd) and {}}):
            code, _ = self.run_jobs(["--max-usd", "9", "brief", "X",
                                     "--max-usd", "0.25", "--store", self.db])
        self.assertEqual(code, 0)
        self.assertEqual(seen["max"], 0.25)

    def test_max_usd_on_a_local_job_is_rejected_not_ignored(self):
        code, body = self.run_jobs(["--max-usd", "1", "catalogue", "x",
                                    "--store", self.db], key=None)
        self.assertEqual(code, 2)
        self.assertEqual(body["error"]["type"], "usage")


# ------------------------------------------------ 10/11. authority -----------
class TestAuthority(JobsCase):
    def test_sample_help_states_what_it_limits(self):
        out = io.StringIO()
        for job, phrase in (("authority", "seed-search tweets"),
                            ("authenticity", "follower profiles")):
            with contextlib.redirect_stdout(out), self.assertRaises(SystemExit):
                jobs._job_parser().parse_args([job, "--help"])
            self.assertIn(phrase, " ".join(out.getvalue().split()))

    def test_one_failed_member_is_the_partial_contract(self):
        now = int(time.time())
        names = ["m0", "m1", "locked"]

        def raw(path, params):
            if path.endswith("advanced_search"):
                return {"tweets": [{"id": f"t{i}", "createdAt": fmt(now - i),
                                    "author": {"id": str(i), "userName": n}}
                                   for i, n in enumerate(names)],
                        "has_next_page": False}
            if path.endswith("followings"):
                if params["userName"] == "locked":
                    raise twitterapi.APIError(403, "protected", path)
                return {"followings": [{"id": "0", "userName": "m0"}],
                        "has_next_page": False}
            raise AssertionError(path)

        code, body = self.run_jobs(["authority", "x", "--store", self.db,
                                    "--max-usd", "1"], raw)
        self.assertEqual(code, 0, body)
        self.assertFalse(body["complete"])
        self.assertEqual(body["completeness"]["status"], "partial")
        self.assertIn("locked", body["completeness"]["reason"])
        self.assertEqual(body["frontier"][0]["handle"], "m0")


# ------------------------------------------------------------- narrative ----
class TestNarrativePartialPrior(unittest.TestCase):
    def test_partial_prior_window_does_not_invent_new_authors(self):
        cur = [{"id": "1", "author": {"userName": "old_voice"}}]
        calls = []

        def fake_window(c, query, since, until):
            calls.append(since)
            if len(calls) == 2:
                raise twitterapi.CostLimitExceeded("Spend ceiling hit mid-window")
            return cur

        class C:
            spent_usd = 0.01
            def spend_report(self): return "2 requests"

        with mock.patch.object(jobs, "search_window", fake_window):
            result = jobs.narrative_tracker("topic", client=C())
        self.assertFalse(result["complete"])
        self.assertEqual(result["new_authors"], [],
                         "authors absent from a partial prior are not 'new'")


if __name__ == "__main__":
    unittest.main()


class TestDriftUsesCohortIdentity(unittest.TestCase):
    """drift had its own copy of Cohort.minus's matching rule because Cohort
    always built a Client (and so needed a key). The Client is lazy now; drift
    goes through Cohort and carries a partial saved version's status."""

    def test_partial_saved_version_makes_drift_partial_without_a_key(self):
        tmp = tempfile.mkdtemp(prefix="drift-partial-")
        s = Store(os.path.join(tmp, "s.db"))
        s.save_cohort("w", [("1", "alice", 1.0, "t")])
        s.save_cohort("w", [("2", "Bob", 1.0, "t")], spec={
            "complete": False, "incomplete_reasons": ["search stopped at limit 200"]})
        with mock.patch.dict(os.environ, {}, clear=False), \
                contextlib.redirect_stderr(io.StringIO()):
            os.environ.pop("TWITTERAPI_IO_KEY", None)
            out = jobs.cohort_drift("w", store=s)
        self.assertFalse(out["complete"])
        self.assertIn("limit 200", out["_warning"])
        self.assertEqual((out["joined"], out["left"]), (["Bob"], ["alice"]))

    def test_handle_match_is_case_insensitive_like_cohort_minus(self):
        tmp = tempfile.mkdtemp(prefix="drift-case-")
        s = Store(os.path.join(tmp, "s.db"))
        s.save_cohort("w", [("", "Alice", 1.0, "t")])
        s.save_cohort("w", [("", "alice", 1.0, "t")])
        out = jobs.cohort_drift("w", store=s)
        self.assertEqual((out["joined"], out["left"]), ([], []))
        self.assertTrue(out["complete"])


class TestAuthenticityUnknownFields(unittest.TestCase):
    """A follower profile with no bio key or no counts was scored as "no bio"
    and as a 0-follower account. Absent fields are unknown and stay out of
    the denominators."""

    def test_absent_bio_and_counts_are_not_scored(self):
        profiles = [
            {"id": "1", "description": "", "followers": 0, "following": 900},
            {"id": "2", "description": "builder", "followers": 50, "following": 10},
            {"id": "3"},                                   # thin profile
        ]

        class C:
            def user_info(self, _): return {"followers": 3}
            def paginate(self, *a, **k): return iter(profiles)
            def spend_report(self): return "1 request"

        with mock.patch.object(jobs, "_drain", lambda c, stream, label: (list(stream), None)):
            out = jobs.authenticity_audit("alpha", client=C())
        self.assertEqual(out["pct_no_bio"], 50.0)       # 1 of the 2 that reported
        self.assertEqual(out["bio_unknown"], 1)
        self.assertEqual(out["pct_egg_like"], 50.0)     # 1 of the 2 with counts
        self.assertEqual(out["counts_unknown"], 1)

    def test_no_reported_bios_is_null_not_zero_percent(self):
        class C:
            def user_info(self, _): return {"followers": 1}
            def paginate(self, *a, **k): return iter([{"id": "1"}])
            def spend_report(self): return "1 request"

        with mock.patch.object(jobs, "_drain", lambda c, stream, label: (list(stream), None)):
            out = jobs.authenticity_audit("alpha", client=C())
        self.assertIsNone(out["pct_no_bio"])
        self.assertIsNone(out["pct_egg_like"])
