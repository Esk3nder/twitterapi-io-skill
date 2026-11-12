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
import signal
import tempfile
import time
import unittest
from datetime import UTC, datetime
from unittest import mock

import jobs
import twitterapi
from store import Store

from tests.e2e_base import SCRIPTS  # noqa: F401  (SCRIPTS puts scripts/ on sys.path)
from tests.fakes import client_class


def fmt(ts):
    return datetime.fromtimestamp(ts, UTC).strftime(
        "%a %b %d %H:%M:%S +0000 %Y")


def window(params):
    """(since, until) of a search request; the walk always sends both."""
    query = params["query"]
    return (int(query.split("since_time:")[1].split()[0]),
            int(query.split("until_time:")[1].split()[0]))


def search_index(posts):
    """A fake advanced_search over `posts` ({id, ts, ...wire fields}): the
    newest 20 inside the requested window, and whether more remain."""
    def answer(params):
        since, until = window(params)
        handle = params["query"].split("from:")[1].split()[0] \
            if "from:" in params["query"] else None
        hits = sorted((p for p in posts if since <= p["ts"] < until
                       and handle in (None, p["author"]["userName"])),
                      key=lambda p: -p["ts"])
        return {"tweets": [{**p, "createdAt": fmt(p["ts"])} for p in hits[:20]],
                "has_next_page": len(hits) > 20}
    return answer


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
        def raw(path, params):
            if path.endswith("user/info"):
                return {"data": {"userName": "busy", "followers": 10}}
            if path.endswith("advanced_search"):
                _, ut = window(params)
                return {"tweets": [
                    {"id": f"t{ut - k}", "createdAt": fmt(ut - 1 - k * 60),
                     "likeCount": 1, "author": {"userName": "busy"}}
                    for k in range(20)], "has_next_page": True}
            raise AssertionError(path)
        return raw

    def test_brief_ceiling_after_spend_returns_partial_contract(self):
        code, body = self.run_jobs(
            ["brief", "busy", "--store", self.db, "--max-usd", "0.02"], self.busy_raw())
        self.assertEqual(code, 0, body)
        self.assertFalse(body["complete"])
        self.assertEqual(body["completeness"]["status"], "partial")
        self.assertIn("ceiling", body["completeness"]["reason"])
        self.assertIn("--max-usd", body["completeness"]["resume"])
        self.assertIn("$", body["spend"])

    def test_brief_ceiling_mid_walk_keeps_the_tweets_it_bought(self):
        """Every page walked before the ceiling was paid for and stored, so
        the partial must carry those rows rather than 0 tweets."""
        code, body = self.run_jobs(
            ["brief", "busy", "--store", self.db, "--max-usd", "0.02"], self.busy_raw())
        self.assertEqual(code, 0, body)
        self.assertFalse(body["complete"])
        self.assertGreaterEqual(body["tweet_count"], 20, body)

    def test_true_pre_spend_refusal_stays_exit_two(self):
        code, body = self.run_jobs(
            ["brief", "busy", "--store", self.db, "--max-usd", "0.00001"],
            self.busy_raw())
        self.assertEqual(code, 2)
        self.assertEqual(body["error"]["type"], "refusal")
        self.assertEqual(self.requests, [])

    def test_rows_yielded_before_a_stop_are_returned(self):
        def stopped_corpus(*args, **kwargs):
            yield {"id": "kept", "text": "paid", "likeCount": 3, "createdAt": "now"}
            raise twitterapi.CostLimitExceeded("Spend ceiling hit mid-window")

        class C:
            spent_usd = 0.01
            def user_info(self, _): return {"name": "n", "followers": 1}
            def spend_report(self): return "1 request"

        with mock.patch.object(jobs, "corpus", stopped_corpus):
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
            code, body = self.run_jobs([*argv, "--store", self.db])
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
        index = search_index([
            {"id": f"{h}{k}", "ts": now - 100 - k, "likeCount": 5,
             "author": {"userName": h}} for h in ("stripe", "vercel") for k in range(3)])

        def raw(path, params):
            if path.endswith("user/info"):
                return {"data": {"userName": params["userName"], "followers": 10}}
            return index(params)

        code, body = self.run_jobs(["benchmark", "stripe, vercel,,@Stripe ",
                                    "--store", self.db], raw)
        self.assertEqual(code, 0, body)
        self.assertEqual([e["handle"] for e in body["entities"]],
                         ["stripe", "vercel"])
        self.assertEqual([e["tweets_in_window"] for e in body["entities"]], [3, 3])
        queries = [b["query"] for path, b in self.requests
                   if path.endswith("advanced_search")]
        self.assertEqual({q.split()[0] for q in queries},
                         {"from:stripe", "from:vercel"}, queries)

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
        return search_index([{
            "id": "1", "ts": int(time.time()) - 60, "likeCount": 4,
            "replyCount": 2, "quoteCount": 1, "viewCount": 90,
            "author": {"userName": "alpha"}}])(params)

    def test_absent_verification_is_unknown_and_requested_fields_are_filled(self):
        code, body = self.run_jobs(["brief", "alpha", "--store", self.db,
                                    "--fields", "views,replies,quotes"], self.raw)
        self.assertEqual(code, 0, body)
        self.assertIsNone(body["profile"]["verified"])
        self.assertEqual(body["top_tweets"],
                         [{"views": 90, "replies": 2, "quotes": 1}])

    def test_a_bare_handle_looks_back_a_week_by_default(self):
        """Thirty days of a busy account is real money nobody asked for."""
        code, body = self.run_jobs(["brief", "alpha", "--store", self.db], self.raw)
        self.assertEqual(code, 0, body)
        self.assertEqual(body["window_days"], 7)
        self.assertEqual(body["top_tweets"][0]["handle"], "alpha")
        spans = {until - since for since, until in (
            window(b) for path, b in self.requests if path.endswith("advanced_search"))}
        self.assertEqual(spans, {7 * 86400 // 8}, "eight equal shards of one week")


# ------------------------------------------------ stop and resume (D1, D2) --
class TestStopAndResume(JobsCase):
    def setUp(self):
        super().setUp()
        base = int(time.time()) - 600
        # One post every 30 minutes for 100 hours: several pages per shard.
        self.index = search_index([
            {"id": f"p{k}", "ts": base - k * 1800, "likeCount": k,
             "author": {"userName": "alpha"}} for k in range(200)])

    def raw(self, path, params):
        if path.endswith("user/info"):
            return {"data": {"userName": "alpha", "followers": 3}}
        return self.index(params)

    def searches(self):
        return [b["query"] for path, b in self.requests
                if path.endswith("advanced_search")]

    def test_a_rerun_after_the_ceiling_replays_the_pages_already_bought(self):
        argv = ["brief", "alpha", "--store", self.db, "--max-usd"]
        code, first = self.run_jobs([*argv, "0.012"], self.raw)
        self.assertEqual(code, 0, first)
        self.assertFalse(first["complete"])
        self.assertIn("within 24 hours", first["completeness"]["resume"])
        bought = set(self.searches())
        self.assertTrue(bought)

        self.requests.clear()
        code, second = self.run_jobs([*argv, "1"], self.raw)
        self.assertEqual(code, 0, second)
        self.assertTrue(second["complete"], second)
        self.assertEqual(second["tweet_count"], 200)
        self.assertEqual(bought & set(self.searches()), set(),
                         "no page from the first run was paid for again")
        self.assertIn("cache hit", second["spend"])

        # The walk finished, so the next run is about the real present again.
        with Store(self.db) as s:
            self.assertEqual(s.get_page("walk", {"key": "brief:alpha:7:None"}), {})

    def test_ctrl_c_mid_walk_returns_the_rows_and_the_spend(self):
        seen = []

        def raw(path, params):
            if path.endswith("advanced_search"):
                seen.append(path)
                if len(seen) == 9:      # some shard is on its second page
                    os.kill(os.getpid(), signal.SIGINT)     # a real Ctrl-C
                    time.sleep(0.2)     # let the main thread take the signal
            return self.raw(path, params)

        code, body = self.run_jobs(["brief", "alpha", "--store", self.db], raw)
        self.assertEqual(code, 0, body)
        self.assertFalse(body["complete"])
        self.assertIn("stopped by the user", body["completeness"]["reason"])
        self.assertGreaterEqual(body["tweet_count"], 20)
        self.assertIn("$", body["spend"])

    def test_a_halted_client_sends_nothing_more(self):
        from tests.fakes import ScriptedClient
        c = ScriptedClient(raw=[])
        c.halt("stopped by the user")
        with self.assertRaises(twitterapi.CostLimitExceeded) as cm:
            c.user_info("alpha")
        self.assertIn("stopped by the user", str(cm.exception))
        self.assertEqual(c.raw_calls, [])
        self.assertEqual(c.spent_credits, 0.0)


# ------------------------------------------------------------ tweet lookup --
class TestTweetLookup(JobsCase):
    def raw(self, path, params):
        post = {"id": "1900000000000000001", "createdAt": fmt(1_700_000_100),
                "text": "the post", "likeCount": 7,
                "author": {"userName": "alpha"},
                "quoted_tweet": {"id": "5", "text": "quoted",
                                 "author": {"userName": "beta"}}}
        if path.endswith("/twitter/tweets"):
            return {"tweets": [post] if params["tweet_ids"] == post["id"] else []}
        if path.endswith("thread_context"):
            return {"tweets": [post, {"id": "0", "createdAt": fmt(1_700_000_000),
                                      "text": "root", "author": {"userName": "alpha"}}],
                    "has_next_page": False}
        raise AssertionError(path)

    def test_a_link_resolves_to_the_post_its_quote_and_its_thread(self):
        for ref in ("https://x.com/alpha/status/1900000000000000001?s=20",
                    "twitter.com/alpha/statuses/1900000000000000001",
                    "1900000000000000001"):
            with self.subTest(ref=ref):
                code, body = self.run_jobs(["tweet", ref, "--store", self.db], self.raw)
                self.assertEqual(code, 0, body)
                self.assertTrue(body["complete"])
                self.assertEqual(body["tweet"]["text"], "the post")
                self.assertEqual(body["quoted"]["handle"], "beta")
                self.assertEqual([t["id"] for t in body["thread"]],
                                 ["0", "1900000000000000001"], "oldest first")

    def test_a_handle_or_garbage_is_a_usage_error_before_any_request(self):
        for ref in ("alpha", "https://x.com/alpha", "status/abc"):
            with self.subTest(ref=ref):
                code, body = self.run_jobs(["tweet", ref, "--store", self.db], self.raw)
                self.assertEqual(code, 2, body)
                self.assertEqual(body["error"]["type"], "usage")
        self.assertEqual(self.requests, [])

    def test_a_post_the_api_does_not_return_is_stated_not_invented(self):
        code, body = self.run_jobs(["tweet", "42424242", "--store", self.db], self.raw)
        self.assertEqual(code, 0, body)
        self.assertIsNone(body["tweet"])
        self.assertIn("was not returned", body["tweet_state"])


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

    def test_accounts_outside_the_seed_that_members_follow_are_ranked(self):
        """The discovery is who the scene follows that was NOT in the seed."""
        now = int(time.time())
        follows = {"m0": ["hub", "m1", "solo"], "m1": ["Hub"], "m2": ["hub"]}

        def raw(path, params):
            if path.endswith("advanced_search"):
                return {"tweets": [{"id": f"t{i}", "createdAt": fmt(now - i),
                                    "author": {"id": str(i), "userName": n}}
                                   for i, n in enumerate(follows)],
                        "has_next_page": False}
            if path.endswith("followings"):
                return {"followings": [{"id": f"x-{n.lower()}", "userName": n}
                                       for n in follows[params["userName"]]],
                        "has_next_page": False}
            raise AssertionError(path)

        code, body = self.run_jobs(["authority", "x", "--store", self.db,
                                    "--max-usd", "1"], raw)
        self.assertEqual(code, 0, body)
        self.assertTrue(body["complete"], body)
        self.assertEqual(body["frontier_external"],
                         [{"handle": "hub", "in_degree": 3}],
                         "followed by one member (solo) is not a frontier")
        self.assertEqual(body["frontier_external_count"], "1 of 1 total")


# ------------------------------------------------------------- narrative ----
class TestNarrative(unittest.TestCase):
    NOW = 1_700_000_000
    DAY = 86400

    class C:
        spent_usd = 0.01
        def spend_report(self): return "2 requests"

    def post(self, post_id, days_ago, handle, likes=0, **entities):
        return {"id": post_id, "createdAt": fmt(self.NOW - days_ago * self.DAY),
                "likeCount": likes, "text": post_id,
                "author": {"userName": handle, "followers": 9} if handle else {},
                "entities": entities}

    def run_narrative(self, posts, stop=None, query="topic"):
        def fake_shards(c, q, since, until, index_faves=False):
            self.assertEqual((since, until), (self.NOW - 60 * self.DAY, self.NOW))
            yield from posts
            if stop:
                raise stop
        with mock.patch.object(jobs, "search_shards", fake_shards):
            return jobs.narrative_tracker(query, days=30, client=self.C(), now=self.NOW)

    def test_windows_are_split_at_the_middle_and_terms_are_diffed(self):
        result = self.run_narrative([
            self.post("new-big", 1, "Fresh", 50, symbols=[{"text": "ABC"}],
                      urls=[{"expanded_url": "https://www.Example.com/a"}],
                      user_mentions=[{"screen_name": "Hub"}]),
            self.post("new-small", 2, "veteran", 5, symbols=[{"text": "abc"}],
                      hashtags=[{"text": "Tag"}, {"text": "tag"}]),
            self.post("old", 40, "Veteran", 9, symbols=[{"text": "ABC"}]),
        ])
        self.assertTrue(result["complete"])
        self.assertEqual((result["current_tweet_count"], result["prior_tweet_count"]), (2, 1))
        self.assertEqual(result["new_authors"], ["fresh"])
        self.assertEqual([t["id"] for t in result["top_tweets"]], ["new-big", "new-small"])
        self.assertEqual(result["top_tweets"][0]["handle"], "Fresh")
        self.assertEqual(result["top_authors"][0],
                         {"handle": "Fresh", "tweets": 1, "likes": 50, "followers": 9})
        self.assertEqual(result["rising_cashtags"],
                         [{"term": "abc", "current": 2, "prior": 1}])
        self.assertEqual(result["rising_hashtags"],
                         [{"term": "tag", "current": 1, "prior": 0}],
                         "one tweet repeating a tag counts once")
        self.assertEqual(result["rising_mentions"][0]["term"], "hub")
        self.assertEqual(result["rising_domains"][0]["term"], "example.com")

    def test_an_author_without_a_handle_is_not_an_author_named_empty(self):
        """D13: a null userName crashed the job and counted as author ''."""
        nameless = self.post("ghost", 1, None, 3)
        nameless["author"] = {"userName": None}
        result = self.run_narrative([nameless, self.post("no-author", 2, None)])
        self.assertEqual(result["new_authors"], [])
        self.assertEqual(result["top_authors"], [])
        self.assertIsNone(result["top_tweets"][0]["handle"])

    def test_a_partial_walk_does_not_invent_new_authors_or_rising_terms(self):
        result = self.run_narrative(
            [self.post("seen", 1, "old_voice", 4, hashtags=[{"text": "x"}])],
            stop=twitterapi.CostLimitExceeded("Spend ceiling hit mid-window"))
        self.assertFalse(result["complete"])
        self.assertEqual(result["new_authors"], [],
                         "authors absent from a partial prior are not 'new'")
        self.assertEqual(result["rising_hashtags"], [])
        self.assertEqual([t["id"] for t in result["top_tweets"]], ["seen"],
                         "the rows that were bought are still returned")

    def test_min_faves_across_windows_is_flagged_as_not_like_for_like(self):
        self.assertIn("not like for like",
                      self.run_narrative([], query="topic min_faves:50")["_note"])
        self.assertNotIn("not like for like", self.run_narrative([])["_note"])


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
