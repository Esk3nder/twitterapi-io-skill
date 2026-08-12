"""First-principles invariants for a read-only client of a paid API.

Deterministic tests script only the transport boundary while exercising real
pagination, accounting, corpus, window, history, catalogue, thread, benchmark,
and monitor decisions.  The final test is one small read-only bulk search
against kaitoinfra.
"""
from __future__ import annotations

import contextlib
import io
import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest import mock

from tests.e2e_base import E2ETest, require_key


def tweet(tweet_id, ts, handle="alpha"):
    return {
        "id": str(tweet_id),
        "createdAt": datetime.fromtimestamp(ts, timezone.utc).strftime(
            "%a %b %d %H:%M:%S %z %Y"),
        "author": {"id": f"u-{handle}", "userName": handle},
        "likeCount": 0,
    }


class MemoryStore:
    def __init__(self):
        self.tweets = []

    def put_tweets(self, records):
        self.tweets.extend(records)


class StoreWriteError(RuntimeError):
    pass


class FailingStore:
    def put_tweets(self, records):
        raise StoreWriteError("sqlite write failed")


class ScriptedClientMixin:
    """Script only HTTP responses; retain the real client decision logic."""

    def _script_init(self, raw=(), raw_json=()):
        self.raw_responses = list(raw)
        self.json_responses = list(raw_json)
        self.raw_calls = []
        self.json_calls = []

    @staticmethod
    def _take(queue):
        if not queue:
            raise AssertionError("scripted transport received an extra paid request")
        value = queue.pop(0)
        if isinstance(value, BaseException):
            raise value
        return value

    def _raw(self, method, path, params=None, **kwargs):
        self.raw_calls.append((method, path, dict(params or {})))
        self._bill_request(path)
        return self._take(self.raw_responses)

    def _raw_json(self, method, path, body, **kwargs):
        self.json_calls.append((method, path, body))
        self._bill_request(path)
        return self._take(self.json_responses)


def scripted_client(*, raw=(), raw_json=(), max_usd=1.0, store=None):
    from twitterapi import Client

    class ScriptedClient(ScriptedClientMixin, Client):
        pass

    c = ScriptedClient(api_key="test", verbose=False, max_usd=max_usd,
                       store=store)
    c._script_init(raw=raw, raw_json=raw_json)
    return c


class TestCohortHydrationMoney(E2ETest):
    @staticmethod
    def _members(count):
        return {
            str(i): {
                "account_id": str(i), "user_name": "", "weight": 1.0,
                "provenance": "test",
            }
            for i in range(count)
        }

    @staticmethod
    def _page(start, count):
        return {"users": [
            {"id": str(i), "userName": f"user{i}"}
            for i in range(start, start + count)
        ]}

    def test_hydrate_method_ceiling_refuses_before_transport(self):
        """A hydrate-only ceiling must protect a client with no global ceiling."""
        from cohort import Cohort
        from twitterapi import CostLimitExceeded

        c = scripted_client(
            raw=[self._page(0, 100), self._page(100, 100)],
            max_usd=None, store=self.fresh_store("hydrate-ceiling"))
        self.addCleanup(c.store.close)
        co = Cohort(self._members(200), client=c, store=c.store)

        with self.assertRaises(CostLimitExceeded):
            co.hydrate(max_usd=0.001)
        self.assertEqual(c.raw_calls, [],
                         "the method ceiling must refuse before a paid request")
        self.assertEqual(c.spent_credits, 0.0)

    def test_hydrate_reuses_cached_batch_pages(self):
        """Hydrating the same IDs twice must not buy the same profiles twice."""
        from cohort import Cohort

        pages = [self._page(0, 100), self._page(100, 100)]
        c = scripted_client(raw=pages + pages, max_usd=None,
                            store=self.fresh_store("hydrate-cache"))
        self.addCleanup(c.store.close)
        co = Cohort(self._members(200), client=c, store=c.store)

        co.hydrate(max_usd=1.0)
        first_spend = c.spent_credits
        co.hydrate(max_usd=1.0)

        self.assertEqual(len(c.raw_calls), 2,
                         "the second hydration must make no paid requests")
        self.assertEqual(c.spent_credits, first_spend)
        self.assertEqual(c.cache_hits, 2)

    def test_cached_hydrate_is_allowed_with_zero_new_spend_budget(self):
        """A zero method budget must allow cache hits while blocking misses."""
        from cohort import Cohort

        c = scripted_client(raw=[self._page(0, 100)], max_usd=None,
                            store=self.fresh_store("hydrate-zero-cache"))
        self.addCleanup(c.store.close)
        co = Cohort(self._members(100), client=c, store=c.store)
        co.hydrate(max_usd=1.0)
        first_spend = c.spent_credits

        profiles = co.hydrate(max_usd=0.0)

        self.assertEqual(len(profiles), 100)
        self.assertEqual(len(c.raw_calls), 1)
        self.assertEqual(c.spent_credits, first_spend)
        self.assertEqual(c.cache_hits, 1)

    def test_hydrate_returns_profiles_and_fills_member_handles(self):
        """The purchased profiles must be directly usable and enrich members."""
        from cohort import Cohort

        c = scripted_client(raw=[self._page(0, 2)], max_usd=None,
                            store=self.fresh_store("hydrate-result"))
        self.addCleanup(c.store.close)
        co = Cohort(self._members(2), client=c, store=c.store)

        profiles = co.hydrate(max_usd=1.0)

        self.assertEqual([u["userName"] for u in profiles], ["user0", "user1"])
        self.assertEqual(co.handles(), ["user0", "user1"])


class TestBulkSearchInvariants(E2ETest):
    def test_empty_bulk_search_makes_no_paid_request(self):
        """No work must not incur even the API's per-request billing floor."""
        c = scripted_client(raw_json=[{"results": {}}])
        self.assertEqual(c.bulk_search([]), [])
        self.assertEqual(c.json_calls, [], "empty work must not reach transport")
        self.assertEqual(c.spent_credits, 0.0)

    def test_missing_bulk_subresult_is_not_reported_as_complete_empty_data(self):
        """A malformed batch cannot turn a missing account into a zero-tweet one."""
        c = scripted_client(raw_json=[{
            "results": {"query_0": {"tweets": [], "has_next_page": False}}
        }])
        with self.assertRaises(RuntimeError):
            c.bulk_search(["from:alpha", "from:beta"])

    def test_bulk_batch_is_refused_before_its_max_charge_can_cross_ceiling(self):
        """One POST must not overspend merely because its subqueries bill together."""
        c = scripted_client(
            raw_json=[{"results": {}}], max_usd=0.005)
        with self.assertRaises(Exception) as cm:
            c.bulk_search(["from:alpha", "from:beta"])
        from twitterapi import CostLimitExceeded
        self.assertIsInstance(cm.exception, CostLimitExceeded)
        self.assertEqual(c.json_calls, [], "refusal must happen before transport")
        self.assertEqual(c.spent_credits, 0.0)


class TestPaginationInvariants(E2ETest):
    def test_missing_required_items_field_is_incomplete_not_empty(self):
        """Contract drift must not turn an omitted array into a confident zero."""
        from twitterapi import Client, ENDPOINTS
        with self.assertRaises(RuntimeError):
            Client._unpack({"status": "success"}, ENDPOINTS["search"])

    def test_next_page_is_refused_when_one_bounded_page_can_cross_ceiling(self):
        """A terminal page cannot overspend and then look successful."""
        page = {"tweets": [tweet(i, 1_700_000_000 - i) for i in range(20)],
                "has_next_page": False, "next_cursor": ""}
        c = scripted_client(raw=[page], max_usd=0.002)
        from twitterapi import CostLimitExceeded
        with self.assertRaises(CostLimitExceeded):
            list(c.paginate("search", "from:alpha"))
        self.assertEqual(c.raw_calls, [], "known page maximum must be reserved pre-request")
        self.assertEqual(c.spent_credits, 0.0)

    def test_boundary_duplicates_do_not_inflate_count_or_consume_unique_limit(self):
        """Overlapping pages must still return the requested number of unique ids."""
        pages = [
            {"tweets": [tweet("1", 104), tweet("2", 103)],
             "has_next_page": True, "next_cursor": "c1"},
            {"tweets": [tweet("2", 103), tweet("3", 102)],
             "has_next_page": True, "next_cursor": "c2"},
            {"tweets": [tweet("4", 101)],
             "has_next_page": False, "next_cursor": ""},
        ]
        c = scripted_client(raw=pages)
        got = [t["id"] for t in c.paginate("search", "from:alpha", limit=4)]
        self.assertEqual(got, ["1", "2", "3", "4"])
        self.assertEqual(len(c.raw_calls), 3,
                         "a duplicate must not satisfy the caller's unique limit")

    def test_nonadvancing_pagination_is_incomplete_not_success(self):
        """Missing or repeated cursors cannot silently terminate a paid crawl."""
        cases = {
            "missing": [
                {"tweets": [tweet("1", 104)], "has_next_page": True,
                 "next_cursor": ""},
            ],
            "repeated": [
                {"tweets": [tweet("1", 104)], "has_next_page": True,
                 "next_cursor": "same"},
                {"tweets": [tweet("2", 103)], "has_next_page": True,
                 "next_cursor": "same"},
            ],
        }
        for label, pages in cases.items():
            with self.subTest(label=label):
                c = scripted_client(raw=pages)
                with self.assertRaises(RuntimeError):
                    list(c.paginate("search", "from:alpha"))


class TestCorpusInvariants(E2ETest):
    def test_corpus_batches_unique_handles_and_resumes_without_boundary_loss(self):
        """Duplicate handles must not rebill, and the first batched page must join its walk."""
        from jobs import corpus
        base = 1_700_000_000
        first = [tweet(f"a{i}", base - i, "Alpha") for i in range(20)]
        boundary_walk = [tweet("a19", base - 19, "Alpha")] + [
            tweet(f"a{i}", base - i, "Alpha") for i in range(20, 39)]
        tail = [tweet("a39", base - 39, "Alpha")]
        bulk = {"results": {
            "query_0": {"tweets": first, "has_next_page": True,
                        "next_cursor": "ignored"},
            "query_1": {"tweets": [tweet("b0", base - 5, "beta")],
                        "has_next_page": False, "next_cursor": ""},
            # Present only so the unmodified duplicate-query implementation can finish.
            "query_2": {"tweets": first, "has_next_page": False,
                        "next_cursor": ""},
        }}
        c = scripted_client(
            raw_json=[bulk],
            raw=[
                {"tweets": boundary_walk, "has_next_page": True,
                 "next_cursor": "ignored"},
                {"tweets": tail, "has_next_page": False, "next_cursor": ""},
            ],
            store=MemoryStore())
        got = corpus(["@Alpha", "beta", "alpha"], since_ts=base - 100,
                     until_ts=base + 1, client=c)
        self.assertEqual(len({t["id"] for t in got}), 41)
        sent = c.json_calls[0][2]["queries"]
        self.assertEqual([q["query"] for q in sent], [
            f"from:Alpha since_time:{base - 100} until_time:{base + 1}",
            f"from:beta since_time:{base - 100} until_time:{base + 1}",
        ], "case-insensitive duplicate handles must not create a paid subquery")
        self.assertIn(f"until_time:{base - 18}", c.raw_calls[0][2]["query"],
                      "resume must re-include the oldest batched second")

    def test_bulk_transport_failure_falls_back_and_retrieves_every_handle(self):
        """Batching is an optimization; its failure must not omit an account."""
        from jobs import corpus
        from twitterapi import APIError
        base = 1_700_000_000
        c = scripted_client(
            raw_json=[APIError(503, "temporary", "/bulk")],
            raw=[
                {"tweets": [tweet("a", base, "alpha")],
                 "has_next_page": False, "next_cursor": ""},
                {"tweets": [tweet("b", base, "beta")],
                 "has_next_page": False, "next_cursor": ""},
            ], store=MemoryStore())
        got = corpus(["alpha", "beta"], since_ts=base - 5,
                     until_ts=base + 5, client=c)
        self.assertEqual({t["id"] for t in got}, {"a", "b"})
        walked = [call[2]["query"].split()[0] for call in c.raw_calls]
        self.assertCountEqual(walked, ["from:alpha", "from:beta"])

    def test_local_store_failure_does_not_trigger_more_paid_searches(self):
        """A SQLite failure after a successful batch must not cause surprise API spend."""
        from jobs import corpus
        base = 1_700_000_000
        bulk = {"results": {"query_0": {
            "tweets": [tweet("a", base, "alpha")],
            "has_next_page": False, "next_cursor": ""}}}
        c = scripted_client(
            raw_json=[bulk],
            raw=[{"tweets": [tweet("a", base, "alpha")],
                  "has_next_page": False, "next_cursor": ""}],
            store=FailingStore())
        with self.assertRaises(StoreWriteError):
            corpus(["alpha"], since_ts=base - 5, until_ts=base + 5, client=c)
        self.assertEqual(c.raw_calls, [],
                         "persistence errors are not authorization for paid fallback")


class TestWindowCompleteness(E2ETest):
    def test_full_page_inside_one_second_raises_incomplete(self):
        """An unsplittable second cannot produce a confidently complete count."""
        from jobs import _search_window
        base = 1_700_000_000
        c = scripted_client(raw=[
            {"tweets": [tweet(i, base) for i in range(20)],
             "has_next_page": True, "next_cursor": "ignored"},
            {"tweets": [], "has_next_page": False, "next_cursor": ""},
        ], store=MemoryStore())
        with self.assertRaises(RuntimeError):
            _search_window(c, "from:alpha", base - 5, base + 5, max_pages=2)

    def test_max_pages_exhaustion_raises_incomplete(self):
        """A safety cap is not evidence that the requested time range ended."""
        from jobs import _search_window
        base = 1_700_000_000
        page = {"tweets": [tweet(i, base - i) for i in range(20)],
                "has_next_page": True, "next_cursor": "ignored"}
        c = scripted_client(raw=[page], store=MemoryStore())
        with self.assertRaises(RuntimeError):
            _search_window(c, "from:alpha", base - 100, base + 5, max_pages=1)

    def test_full_page_without_parseable_timestamps_raises_incomplete(self):
        """A full page with no resume boundary cannot be called exhaustive."""
        from jobs import _search_window
        records = [{"id": str(i), "createdAt": "not-a-date",
                    "author": {"userName": "alpha"}} for i in range(20)]
        c = scripted_client(raw=[{
            "tweets": records, "has_next_page": True, "next_cursor": "ignored",
        }], store=MemoryStore())
        with self.assertRaises(RuntimeError):
            _search_window(c, "from:alpha", 1_699_999_000, 1_700_000_000)


class TestJobAndWorkflowOrchestration(E2ETest):
    def test_diffusion_transport_failure_is_not_reported_as_zero_engagement(self):
        """Failed engagement crawls must not become a confident all-zero trace."""
        import urllib.error
        from jobs import diffusion_trace

        class FailingClient:
            def paginate(self, *args, **kwargs):
                raise urllib.error.URLError("network unavailable")

            def spend_report(self):
                return "0 requests"

        with self.assertRaises(urllib.error.URLError):
            diffusion_trace("tweet", limit=20, client=FailingClient())

    def test_competitive_benchmark_uses_one_shared_corpus(self):
        """Per-handle corpus calls would defeat batching and spend N requests."""
        import jobs
        calls = []

        def fake_corpus(handles, since_ts=None, until_ts=None, client=None):
            calls.append((list(handles), since_ts, client))
            return [tweet("a", 1_700_000_000, "alpha"),
                    tweet("b", 1_700_000_000, "beta")]

        class C:
            def user_info(self, handle):
                return {"followers": 10 if handle == "alpha" else 20,
                        "isBlueVerified": False}

            def spend_report(self):
                return "0 requests"

        with mock.patch.object(jobs, "corpus", fake_corpus):
            out = jobs.competitive_benchmark(["alpha", "beta"], days=7,
                                             client=C())
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0], ["alpha", "beta"])
        self.assertEqual([r["tweets_in_window"] for r in out["entities"]], [1, 1])

    def test_monitor_does_not_advance_checkpoint_after_incomplete_window(self):
        """A partial poll must be retried, not checkpointed past missing tweets."""
        import workflows

        def incomplete_history(*args, **kwargs):
            yield tweet("seen-before-gap", 1_700_000_000)
            raise RuntimeError("window incomplete")

        state_path = self.store_path("monitor-state.json")
        c = SimpleNamespace(max_usd=1.0, spent_usd=0.0,
                            _over_ceiling=lambda: False)
        with mock.patch.object(workflows, "history_search", incomplete_history):
            with self.assertRaises(RuntimeError):
                workflows.monitor(["alpha"], client=c, state_file=state_path,
                                  once=True, on_tweet=lambda _: None)
        self.assertFalse(os.path.exists(state_path),
                         "checkpoint must not advance after a partial window")

    def test_unreadable_facts_emit_warning_instead_of_silently_disabling_it(self):
        """Unknown verification age must not let stale prices look trustworthy."""
        import twitterapi
        with tempfile.NamedTemporaryFile("w", delete=False) as fh:
            fh.write("not-json")
            facts_path = fh.name
        self.addCleanup(lambda: os.path.exists(facts_path) and os.unlink(facts_path))
        err = io.StringIO()
        with mock.patch.object(twitterapi, "_FACTS_PATH", facts_path), \
             mock.patch.object(twitterapi, "_stale_warned", False), \
             contextlib.redirect_stderr(err):
            twitterapi.Client(api_key="test", verbose=False)
        self.assertIn("cannot verify", err.getvalue().lower())


class TestHistoryHonesty(E2ETest):
    @staticmethod
    def _args(**overrides):
        values = {
            "user": "alpha", "query": None, "since": None, "until": None,
            "exclude_replies": False, "exclude_retweets": False,
            "include_retweets": False, "max_pages": None, "max_usd": 1.0,
            "out": None,
        }
        values.update(overrides)
        return SimpleNamespace(**values)

    def test_history_declares_scope_and_include_retweets_runs_second_pass(self):
        """A parsed flag cannot silently do nothing or leave omissions unstated."""
        import workflows

        records = {
            "from:alpha": [tweet("original", 1_700_000_003, "alpha"),
                           tweet("shared", 1_700_000_002, "alpha")],
            "from:alpha filter:nativeretweets": [
                tweet("shared", 1_700_000_002, "alpha"),
                dict(tweet("retweet", 1_700_000_001, "alpha"),
                     retweeted_tweet={"id": "source"}),
            ],
        }
        calls = []

        def fake_history(query, *args, **kwargs):
            calls.append((query, kwargs.get("max_pages")))
            yield from records[query]

        client = SimpleNamespace(max_usd=1.0, spent_usd=0.0,
                                 spend_report=lambda: "0 requests, ~$0.0000")
        out, err = io.StringIO(), io.StringIO()
        with mock.patch.object(workflows, "Client", return_value=client), \
             mock.patch.object(workflows, "history_search", fake_history), \
             contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            rc = workflows._history_cli(
                self._args(include_retweets=True, max_pages=2))

        self.assertEqual(rc, 0)
        self.assertEqual(calls, [
            ("from:alpha", 2),
            ("from:alpha filter:nativeretweets", 2),
        ], "each history pass must receive its own full page budget")
        self.assertEqual([json.loads(line)["id"] for line in out.getvalue().splitlines()],
                         ["original", "shared", "retweet"],
                         "the second pass must deduplicate ids across both searches")
        banner = err.getvalue().lower()
        self.assertIn("scope", banner)
        self.assertIn("native retweets included", banner)

    def test_default_history_explicitly_declares_native_retweets_excluded(self):
        """A default history cannot make no-retweets look like user behavior."""
        import workflows

        calls = []

        def fake_history(query, *args, **kwargs):
            calls.append(query)
            return iter(())

        client = SimpleNamespace(max_usd=1.0, spent_usd=0.0,
                                 spend_report=lambda: "0 requests, ~$0.0000")
        out, err = io.StringIO(), io.StringIO()
        with mock.patch.object(workflows, "Client", return_value=client), \
             mock.patch.object(workflows, "history_search", fake_history), \
             contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            rc = workflows._history_cli(self._args())

        self.assertEqual(rc, 0)
        self.assertEqual(calls, ["from:alpha"])
        scope = err.getvalue().lower()
        self.assertIn("scope", scope)
        self.assertIn("native retweets excluded", scope)

    def test_history_index_gap_is_reported_partial_with_cost_ceiling_wording(self):
        """Index depth before account creation must not exit 0 as a full archive."""
        import workflows

        rec = tweet("oldest-indexed", 1_531_008_000, "alpha")
        rec["author"].update({
            "createdAt": "Mon Apr 06 16:54:47 +0000 2009",
            "statusesCount": 20_926,
        })

        def fake_history(*args, **kwargs):
            yield rec

        client = SimpleNamespace(max_usd=1.0, spent_usd=0.0,
                                 spend_report=lambda: "1 request, ~$0.0030")
        err = io.StringIO()
        with tempfile.NamedTemporaryFile() as output, \
             mock.patch.object(workflows, "Client", return_value=client), \
             mock.patch.object(workflows, "history_search", fake_history), \
             contextlib.redirect_stderr(err):
            rc = workflows._history_cli(self._args(out=output.name))

        report = err.getvalue().lower()
        self.assertEqual(rc, 3, "an index-limited archive is partial, not complete")
        self.assertIn("index coverage: partial", report)
        self.assertIn("2009", report)
        self.assertIn("2018", report)
        self.assertIn("upper bound", report,
                      "stated-post-count pricing is not a point estimate")
        self.assertIn("$3.14", report)


class TestLocalCorpusCapabilities(E2ETest):
    def _cached_corpus(self):
        s = self.fresh_store("catalogue")
        jan = 1_704_067_200
        feb = 1_706_745_600
        records = [
            dict(tweet("root", jan, "alpha"), text="root", likeCount=2,
                 retweetCount=1, replyCount=2, quoteCount=3, viewCount=10,
                 bookmarkCount=4, conversationId="root"),
            dict(tweet("self-reply", jan + 1, "alpha"), text="continued",
                 likeCount=8, retweetCount=2, replyCount=0, quoteCount=0,
                 viewCount=20, bookmarkCount=6, isReply=True,
                 inReplyToId="root", inReplyToUserId="u-alpha",
                 inReplyToUsername="alpha", conversationId="root"),
            dict(tweet("quote", feb, "alpha"), text="quote", likeCount=4,
                 retweetCount=0, replyCount=1, quoteCount=0, viewCount=30,
                 bookmarkCount=2, quoted_tweet={"id": "quoted"},
                 conversationId="quote"),
            dict(tweet("rt", feb + 1, "alpha"), text="RT", likeCount=0,
                 retweetCount=0, replyCount=0, quoteCount=0, viewCount=0,
                 bookmarkCount=0, retweeted_tweet={"id": "source"},
                 conversationId="rt"),
        ]
        s.put_tweets(records)
        return s

    def test_catalogue_summarises_cached_corpus_without_a_client(self):
        """Free repeat analysis must not require a paid API client or ad-hoc code."""
        from jobs import catalogue

        result = catalogue("@Alpha", store=self._cached_corpus())
        self.assertEqual(result["counts"], {
            "total": 4, "unique": 4, "originals": 1, "replies": 1,
            "self_thread_replies": 1, "quotes": 1, "native_retweets": 1,
        })
        self.assertEqual(result["engagement"]["likes"],
                         {"total": 14, "average": 3.5})
        self.assertEqual(set(result["engagement"]), {
            "likes", "retweets", "replies", "quotes", "views", "bookmarks",
        })
        self.assertEqual(result["monthly"], [
            {"month": "2024-01", "volume": 2, "median_likes": 5.0},
            {"month": "2024-02", "volume": 2, "median_likes": 2.0},
        ])
        self.assertEqual(result["spend"], "$0.00 (local cache only)")

    def test_threads_reconstruct_cached_conversations_and_state_limits(self):
        """conversationId must become usable structure without claiming missing context."""
        from jobs import reconstruct_threads

        result = reconstruct_threads("alpha", store=self._cached_corpus())
        self.assertEqual(result["thread_count"], 1)
        thread = result["threads"][0]
        self.assertEqual(thread["conversation_id"], "root")
        self.assertEqual([t["id"] for t in thread["tweets"]],
                         ["root", "self-reply"])
        self.assertTrue(thread["cached_root_present"])
        self.assertIn("cached", result["_scope"].lower())
        self.assertIn("may be absent", result["_scope"].lower())
        self.assertEqual(result["spend"], "$0.00 (local cache only)")

    def test_catalogue_cli_never_constructs_a_paid_client(self):
        """A $0 local job must remain usable without API initialization."""
        import jobs
        from store import Store

        path = self.store_path("catalogue-cli")
        with Store(path) as s:
            s.put_tweets([tweet("one", 1_704_067_200, "alpha")])
        output = io.StringIO()
        with mock.patch.object(jobs, "_client",
                               side_effect=AssertionError("paid client created")), \
             contextlib.redirect_stdout(output):
            rc = jobs.main(["catalogue", "alpha", "--store", path])
        self.assertEqual(rc, 0)
        self.assertEqual(json.loads(output.getvalue())["counts"]["total"], 1)


class TestSpendReporting(E2ETest):
    def test_spend_report_surfaces_cache_savings(self):
        """A near-zero rerun must look cached, not like broken accounting."""
        from twitterapi import Client

        c = Client(api_key="test", verbose=False)
        c.cache_hits = 2
        c.saved_credits = 4_500
        report = c.spend_report()
        self.assertIn("2 cache hits", report)
        self.assertIn("saved $0.0450", report)

    def test_spend_report_surfaces_oldest_cached_data_age(self):
        """Verification for F2: an old free snapshot must not look fresh."""
        from twitterapi import Client

        c = Client(api_key="test", verbose=False)
        c.oldest_cache_age = 9.5 * 3600
        report = c.spend_report()
        self.assertIn("oldest cached data served: 9.5 hours old", report)


class TestLiveBulkSearch(E2ETest):
    def test_live_bulk_search_shape_and_client_accounting(self):
        """The real POST path must match the verified envelope and exact charge identity."""
        require_key()
        from twitterapi import Client
        c = self.track(Client(verbose=False, max_usd=0.01,
                              store=self.fresh_store("live_bulk")))
        self.repro("Client().bulk_search(['from:kaitoinfra'])")
        result = c.bulk_search(["from:kaitoinfra"])
        self.assertEqual(len(result), 1)
        self.assertEqual(set(result[0]), {"tweets", "has_next_page", "next_cursor"})
        self.assertIsInstance(result[0]["tweets"], list)
        expected = Client.page_credits("search", len(result[0]["tweets"]), 20)
        self.assertEqual(c.spent_credits, expected,
                         "one-query batch spend must equal its one search page")


if __name__ == "__main__":
    unittest.main()
