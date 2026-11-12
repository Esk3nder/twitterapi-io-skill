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
import sqlite3
import tempfile
import unittest
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest import mock

from tests.e2e_base import E2ETest
from tests.fakes import ScriptedClient, client_class


def tweet(tweet_id, ts, handle="alpha"):
    return {
        "id": str(tweet_id),
        "createdAt": datetime.fromtimestamp(ts, UTC).strftime(
            "%a %b %d %H:%M:%S %z %Y"),
        "author": {"id": f"u-{handle}", "userName": handle},
        "likeCount": 0,
    }


class MemoryStore:
    """The store surface the search walk uses, held in memory."""

    def __init__(self):
        self.tweets, self.pages = [], {}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def put_tweets(self, records):
        self.tweets.extend(records)

    def raw_tweets(self, ids):
        held = {t["id"]: t for t in self.tweets}
        return [held[i] for i in ids] if all(i in held for i in ids) else None

    def get_page(self, path, params, max_age=None):
        return self.pages.get((path, json.dumps(params, sort_keys=True)))

    def put_page(self, path, params, body, credits=0):
        self.pages[(path, json.dumps(params, sort_keys=True))] = body

    def drop_pages(self, path, older_than):
        pass

    def record_run(self, *args):
        pass


class StoreWriteError(RuntimeError):
    pass


class FailingStore(MemoryStore):
    def put_tweets(self, records):
        raise StoreWriteError("sqlite write failed")


class TestVerifiedFollowerSemantics(E2ETest):
    def test_endpoint_membership_is_preserved_through_store_and_filter(self):
        """verifiedFollowers membership is ground truth even without flags."""
        from cohort import Cohort

        store = self.fresh_store("verified-follower-semantics")
        client = ScriptedClient(raw=[{
            "followers": [
                {"id": "1", "userName": "alice"},
                {"id": "2", "userName": "bob"},
            ],
            "has_next_page": False,
            "next_cursor": "",
        }], store=store)

        followers = list(client.verified_followers("seed-id"))
        cohort = Cohort.from_ids(
            [u["id"] for u in followers], client=client, store=store)

        self.assertEqual(
            (len(cohort.filter(verified=True)),
             len(cohort.filter(verified=False))),
            (2, 0),
            "endpoint membership must not become false when verification "
            "fields are absent",
        )

    def test_absent_verification_fields_remain_unknown_not_false(self):
        """A thin profile must not create a false-unverified ground truth."""
        from cohort import Cohort
        from store import normalize_account

        store = self.fresh_store("unknown-verification")
        store.put_accounts([{"id": "1", "userName": "alice"}])
        cohort = Cohort.from_ids(["1"], client=object(), store=store)

        normalized = normalize_account({"id": "1", "userName": "alice"})
        self.assertIsNone(normalized["is_blue"])
        self.assertIsNone(normalized["is_verified"])
        self.assertEqual(len(cohort.filter(verified=False)), 0)


class TestCohortFilterHonesty(E2ETest):
    def test_filter_warns_with_count_of_unhydrated_members_skipped(self):
        """A large unhydrated cohort must not collapse silently."""
        from cohort import Cohort

        store = self.fresh_store("filter-unhydrated-warning")
        store.put_accounts([{
            "id": "1", "userName": "alice", "followers": 1_000,
        }])
        cohort = Cohort.from_ids(["1", "2", "3"], client=object(), store=store)

        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            result = cohort.filter(min_followers=500)

        self.assertEqual(len(result), 1)
        self.assertIn("skipped 2", err.getvalue().lower())
        self.assertIn("hydrate", err.getvalue().lower())


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

        c = ScriptedClient(
            raw=[self._page(0, 100), self._page(100, 100)],
            max_usd=None, store=self.fresh_store("hydrate-ceiling"))
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
        c = ScriptedClient(raw=pages + pages, max_usd=None,
                            store=self.fresh_store("hydrate-cache"))
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

        c = ScriptedClient(raw=[self._page(0, 100)], max_usd=None,
                            store=self.fresh_store("hydrate-zero-cache"))
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

        c = ScriptedClient(raw=[self._page(0, 2)], max_usd=None,
                            store=self.fresh_store("hydrate-result"))
        co = Cohort(self._members(2), client=c, store=c.store)

        profiles = co.hydrate(max_usd=1.0)

        self.assertEqual([u["userName"] for u in profiles], ["user0", "user1"])
        self.assertEqual(co.handles(), ["user0", "user1"])


class TestPaginationInvariants(E2ETest):
    def test_paid_articles_survive_a_later_api_failure_as_flagged_partial(self):
        """F24: a broken continuation must not destroy already-paid articles."""
        from twitterapi import APIError

        first = [{"id": str(i)} for i in range(12)]
        second = [{"id": "12"}]
        c = ScriptedClient(raw=[
            {"data": {"articles": first}, "has_next_page": True,
             "next_cursor": "page-2"},
            {"data": {"articles": second}, "has_next_page": True,
             "next_cursor": "page-3"},
            APIError(200, "Failed to get user articles", "/twitter/user/articles"),
        ])
        err = io.StringIO()

        stream = c.paginate("articles", "trq212")
        with contextlib.redirect_stderr(err):
            articles = list(stream)

        self.assertEqual(len(articles), 13)
        self.assertFalse(stream.complete)
        self.assertIn("failed to get user articles", stream._warning.lower())
        self.assertIn("partial", stream._warning.lower())
        self.assertIn("stopped", err.getvalue().lower())

    def test_paid_articles_survive_repeated_empty_continuations(self):
        """F24: has_next_page lies after 13 rows end as partial, not an error."""
        first = [{"id": str(i)} for i in range(12)]
        pages = [
            {"data": {"articles": first}, "has_next_page": True,
             "next_cursor": "page-2"},
            {"data": {"articles": [{"id": "12"}]}, "has_next_page": True,
             "next_cursor": "page-3"},
            {"data": {"articles": []}, "has_next_page": True,
             "next_cursor": "page-4"},
            {"data": {"articles": []}, "has_next_page": True,
             "next_cursor": "page-5"},
        ]
        c = ScriptedClient(raw=pages)
        stream = c.paginate("articles", "trq212")

        self.assertEqual(len(list(stream)), 13)
        self.assertFalse(stream.complete)
        self.assertIn("2 empty pages", stream._warning)

    def test_first_page_api_failure_still_raises(self):
        """F24: no usable data means the underlying failure remains fatal."""
        from twitterapi import APIError

        c = ScriptedClient(raw=[
            APIError(200, "Failed to get user articles", "/twitter/user/articles"),
        ])
        stream = c.paginate("articles", "nobody")

        with self.assertRaises(APIError):
            list(stream)

    def test_tweet_timeline_resolves_handle_but_preserves_numeric_ids(self):
        """F25: the public paginator accepts handles like neighbouring endpoints."""
        timeline = {
            "data": {"tweets": [tweet("one", 1_700_000_000)]},
            "has_next_page": False,
            "next_cursor": "",
        }
        by_handle = ScriptedClient(raw=[
            {"data": {"id": "879696238953865217"}},
            timeline,
        ])

        self.assertEqual(len(list(by_handle.paginate(
            "tweet_timeline", "lydiahallie", limit=1))), 1)
        self.assertEqual(by_handle.raw_calls[0][1], "/twitter/user/info")
        self.assertEqual(by_handle.raw_calls[0][2]["userName"], "lydiahallie")
        self.assertEqual(
            by_handle.raw_calls[1][2]["userId"], "879696238953865217")

        by_id = ScriptedClient(raw=[timeline])
        self.assertEqual(len(list(by_id.paginate(
            "tweet_timeline", "879696238953865217", limit=1))), 1)
        self.assertEqual(len(by_id.raw_calls), 1)
        self.assertEqual(by_id.raw_calls[0][2]["userId"],
                         "879696238953865217")

    def test_store_path_is_coerced_before_nonempty_page_write_through(self):
        """A path-valued store must not crash only after a paid response."""
        path = self.store_path("client-path-store")
        page = {
            "tweets": [tweet("paid", 1_700_000_000)],
            "has_next_page": False,
            "next_cursor": "",
        }
        c = ScriptedClient(raw=[page], store=path)
        self.addCleanup(lambda: getattr(c.store, "close", lambda: None)())

        got = list(c.paginate("mentions", "alpha"))

        self.assertEqual([t["id"] for t in got], ["paid"])
        self.assertNotIsInstance(c.store, str)
        self.assertEqual(c.store.tweets_for(user_names=["alpha"])[0]["id"],
                         "paid")

    def test_paginate_surfaces_normalized_media_on_tweets(self):
        """Callers must not need to know the extendedEntities wire shape."""
        record = tweet("with-media", 1_700_000_002)
        record["extendedEntities"] = {"media": [{
            "type": "photo",
            "media_url_https": "https://pbs.twimg.com/media/example.png",
            "ext_alt_text": "Architecture diagram",
        }]}
        c = ScriptedClient(raw=[{
            "tweets": [record], "has_next_page": False, "next_cursor": "",
        }])

        got = list(c.paginate("mentions", "alpha"))

        self.assertEqual(got[0]["media"][0]["type"], "photo")
        self.assertEqual(got[0]["media"][0]["alt_text"],
                         "Architecture diagram")
        self.assertIn("name=4096x4096",
                      got[0]["media"][0]["full_resolution_url"])

    def test_search_is_not_served_by_cursor_pagination(self):
        """Cursors loop on old ranges; search has exactly one walker."""
        from twitterapi import UsageError
        c = ScriptedClient(raw=[])
        with self.assertRaises(UsageError) as cm:
            list(c.paginate("search", "from:alpha"))
        self.assertIn("search_window", str(cm.exception))
        self.assertEqual(c.raw_calls, [])

    def test_next_page_is_refused_when_one_bounded_page_can_cross_ceiling(self):
        """A terminal page cannot overspend and then look successful."""
        page = {"tweets": [tweet(i, 1_700_000_000 - i) for i in range(20)],
                "has_next_page": False, "next_cursor": ""}
        c = ScriptedClient(raw=[page], max_usd=0.002)
        from twitterapi import CostLimitExceeded
        with self.assertRaises(CostLimitExceeded):
            list(c.paginate("mentions", "alpha"))
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
        c = ScriptedClient(raw=pages)
        got = [t["id"] for t in c.paginate("mentions", "alpha", limit=4)]
        self.assertEqual(got, ["1", "2", "3", "4"])
        self.assertEqual(len(c.raw_calls), 3,
                         "a duplicate must not satisfy the caller's unique limit")

    def test_nonadvancing_pagination_returns_flagged_paid_frontier(self):
        """Missing/repeated cursors preserve rows without claiming completeness."""
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
                c = ScriptedClient(raw=pages)
                stream = c.paginate("mentions", "alpha")
                got = list(stream)
                self.assertGreater(len(got), 0)
                self.assertFalse(stream.complete)
                self.assertIn("partial", stream._warning.lower())


class TestCorpusInvariants(E2ETest):
    @staticmethod
    def _by_handle(base):
        def answer(path, params):
            handle = params["query"].split()[0].removeprefix("from:")
            return {"tweets": [tweet(f"{handle}-1", base, handle)],
                    "has_next_page": False}
        return answer

    def test_corpus_walks_each_unique_handle_once_inside_the_window(self):
        """Repeating @Name as name must not buy the same account twice."""
        from jobs import corpus
        base = 1_700_000_000
        c = ScriptedClient(raw=self._by_handle(base), store=MemoryStore())
        got = list(corpus(["@Alpha", "beta", "alpha", " "], base - 100, base + 1,
                          client=c))
        self.assertEqual([t["id"] for t in got], ["Alpha-1", "beta-1"])
        self.assertEqual([call[2]["query"] for call in c.raw_calls], [
            f"from:Alpha since_time:{base - 100} until_time:{base + 1}",
            f"from:beta since_time:{base - 100} until_time:{base + 1}"])

    def test_a_stop_in_a_later_handle_keeps_the_earlier_handles_rows(self):
        """A truncated corpus must surface as a stop, after the paid rows."""
        from jobs import corpus
        from twitterapi import IncompleteDataError
        base = 1_700_000_000
        c = ScriptedClient(raw=[
            {"tweets": [tweet("a", base, "alpha")], "has_next_page": False},
            {"tweets": [tweet("b", base, "beta")]},        # no has_next_page
        ], store=MemoryStore())
        got = []
        with self.assertRaises(IncompleteDataError):
            for t in corpus(["alpha", "beta"], base - 5, base + 5, client=c):
                got.append(t)
        self.assertEqual([t["id"] for t in got], ["a", "b"])

    def test_local_store_failure_does_not_trigger_more_paid_searches(self):
        """A SQLite failure after a paid page must not cause surprise API spend."""
        from jobs import corpus
        base = 1_700_000_000
        c = ScriptedClient(raw=self._by_handle(base), store=FailingStore())
        with self.assertRaises(StoreWriteError):
            list(corpus(["alpha", "beta"], base - 5, base + 5, client=c))
        self.assertEqual(len(c.raw_calls), 1,
                         "persistence errors are not authorization for more spend")


class TestWindowCompleteness(E2ETest):
    def test_a_full_second_is_split_by_reply_status_and_walked_whole(self):
        """Thirty tweets in one second: a time window shows 20; the two
        disjoint halves must recover all 30 and prove the window walked."""
        from workflows import search_window
        base = 1_700_000_000
        replies = [tweet(f"r{i}", base) for i in range(12)]
        others = [tweet(f"o{i}", base) for i in range(18)]
        c = ScriptedClient(raw=[
            {"tweets": (replies + others)[:20], "has_next_page": True},
            {"tweets": replies, "has_next_page": False},
            {"tweets": others, "has_next_page": False},
            {"tweets": [], "has_next_page": False},
        ], store=MemoryStore())
        got = list(search_window(c, "from:alpha", base - 5, base + 5))
        self.assertEqual(len({t["id"] for t in got}), 30)
        self.assertEqual(len(got), 30, "each id is yielded once")
        queries = [call[2]["query"] for call in c.raw_calls]
        self.assertEqual(queries[1], f"from:alpha filter:replies since_time:{base} "
                                     f"until_time:{base + 1}")
        self.assertEqual(queries[2], f"from:alpha -filter:replies since_time:{base} "
                                     f"until_time:{base + 1}")
        self.assertIn(f"until_time:{base}", queries[3],
                      "the walk resumes below the second it just covered")

    def test_a_second_that_is_still_full_after_the_split_raises_incomplete(self):
        """An unsplittable second cannot produce a confidently complete count."""
        from twitterapi import IncompleteDataError
        from workflows import search_window
        base = 1_700_000_000
        full = {"tweets": [tweet(i, base) for i in range(20)], "has_next_page": True}
        c = ScriptedClient(raw=[full, dict(full)], store=MemoryStore())
        got = []
        with self.assertRaises(IncompleteDataError) as cm:
            for t in search_window(c, "from:alpha", base - 5, base + 5):
                got.append(t)
        self.assertEqual(len(got), 20, "the paid rows are yielded before the stop")
        self.assertIn(str(base), str(cm.exception))

    def test_a_rerun_of_the_same_window_replays_what_was_bought(self):
        """A stopped walk must not pay for its first pages again."""
        from workflows import search_window
        base = 1_700_000_000
        pages = [
            {"tweets": [tweet(i, base - i) for i in range(20)], "has_next_page": True},
            {"tweets": [tweet(i, base - i) for i in range(19, 25)], "has_next_page": False},
        ]
        store = self.fresh_store("window-replay")
        first = ScriptedClient(raw=list(pages), store=store)
        bought = [t["id"] for t in search_window(first, "from:alpha", base - 100, base + 5)]
        rerun = ScriptedClient(raw=[], store=store)
        replayed = [t["id"] for t in search_window(rerun, "from:alpha", base - 100, base + 5)]
        self.assertEqual(replayed, bought)
        self.assertEqual(len(bought), 25)
        self.assertEqual(rerun.raw_calls, [], "every page came from the store")
        self.assertEqual(rerun.spent_credits, 0.0)
        self.assertEqual(rerun.cache_hits, 2)
        self.assertGreater(rerun.saved_credits, 0)

    def test_replay_buys_a_page_again_when_its_rows_are_gone(self):
        """An id list whose tweets are missing is not a page; never return
        fewer rows than were bought."""
        from workflows import search_window
        base = 1_700_000_000
        page = {"tweets": [tweet("a", base), tweet("b", base - 1)], "has_next_page": False}
        store = self.fresh_store("window-replay-gap")
        list(search_window(ScriptedClient(raw=[page], store=store),
                           "from:alpha", base - 100, base + 5))
        store.db.execute("DELETE FROM tweets WHERE id='b'")
        store.db.commit()
        rerun = ScriptedClient(raw=[page], store=store)
        got = [t["id"] for t in search_window(rerun, "from:alpha", base - 100, base + 5)]
        self.assertEqual(got, ["a", "b"])
        self.assertEqual(len(rerun.raw_calls), 1)

    def test_an_open_ended_walk_is_never_replayed(self):
        """Without a fixed end the same query means a different window."""
        from workflows import search_window
        page = {"tweets": [tweet("a", 1_700_000_000)], "has_next_page": False}
        store = self.fresh_store("window-open")
        for _ in range(2):
            c = ScriptedClient(raw=[page], store=store)
            list(search_window(c, "from:alpha"))
            self.assertEqual(len(c.raw_calls), 1)

    def test_min_faves_is_checked_locally_and_warns_of_the_full_scan(self):
        """A fresh likeCount is the ground truth, and the cost is disclosed."""
        from workflows import search_window
        qualifying = dict(tweet("target", 1_700_000_002), likeCount=109)
        below = dict(tweet("below", 1_700_000_001), likeCount=99)
        store = MemoryStore()
        c = ScriptedClient(raw=[{"tweets": [qualifying, below],
                                 "has_next_page": False}], store=store)
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            got = list(search_window(c, "from:alpha min_faves:100"))
        self.assertEqual([t["id"] for t in got], ["target"])
        self.assertNotIn("min_faves:", c.raw_calls[0][2]["query"])
        self.assertEqual({t["id"] for t in store.tweets}, {"target", "below"},
                         "all paid rows should remain available to the cache")
        warning = err.getvalue()
        self.assertIn("WARNING", warning)
        self.assertIn("--index-faves", warning)
        self.assertIn("$1.00 ceiling", warning)

    def test_index_faves_sends_the_threshold_and_filters_nothing(self):
        from workflows import search_window
        rows = [dict(tweet("stale-count", 1_700_000_002), likeCount=3)]
        c = ScriptedClient(raw=[{"tweets": rows, "has_next_page": False}])
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            got = list(search_window(c, "from:alpha min_faves:100", index_faves=True))
        self.assertIn("min_faves:100", c.raw_calls[0][2]["query"])
        self.assertEqual([t["id"] for t in got], ["stale-count"])
        self.assertEqual(err.getvalue(), "")

    def test_zero_min_faves_and_plain_queries_do_not_warn(self):
        from workflows import search_window
        for query in ("from:alpha min_faves:0", "from:alpha"):
            c = ScriptedClient(raw=[{"tweets": [], "has_next_page": False}])
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                list(search_window(c, query))
            self.assertEqual(err.getvalue(), "", query)

    def test_bare_dates_are_normalized_to_utc_day_boundaries(self):
        """Bare search dates must not inherit the index's shifted timezone."""
        from workflows import search_window
        c = ScriptedClient(raw=[{"tweets": [], "has_next_page": False}])
        list(search_window(c, "from:alpha since:2026-07-01 until:2026-08-01"))
        sent = c.raw_calls[0][2]["query"]
        self.assertIn("since:2026-07-01_00:00:00_UTC", sent)
        self.assertIn("until:2026-08-01_00:00:00_UTC", sent)

    def test_max_pages_exhaustion_raises_incomplete(self):
        """A safety cap is not evidence that the requested time range ended."""
        from workflows import search_window
        base = 1_700_000_000
        page = {"tweets": [tweet(i, base - i) for i in range(20)],
                "has_next_page": True, "next_cursor": "ignored"}
        c = ScriptedClient(raw=[page], store=MemoryStore())
        with self.assertRaises(RuntimeError):
            list(search_window(c, "from:alpha", base - 100, base + 5, max_pages=1))

    def test_full_page_without_parseable_timestamps_raises_incomplete(self):
        """A full page with no resume boundary cannot be called exhaustive."""
        from workflows import search_window
        records = [{"id": str(i), "createdAt": "not-a-date",
                    "author": {"userName": "alpha"}} for i in range(20)]
        c = ScriptedClient(raw=[{
            "tweets": records, "has_next_page": True, "next_cursor": "ignored",
        }], store=MemoryStore())
        with self.assertRaises(RuntimeError):
            list(search_window(c, "from:alpha", 1_699_999_000, 1_700_000_000))


class TestJobAndWorkflowOrchestration(E2ETest):
    def test_network_job_store_path_is_injected_into_shared_client(self):
        """--store must not be ignored by jobs that make network requests."""
        import jobs

        path = self.store_path("network-job-store")
        client = SimpleNamespace(store=mock.Mock())
        output = io.StringIO()
        with mock.patch.dict(jobs.os.environ, {"TWITTERAPI_IO_KEY": "test"}), \
             mock.patch.object(jobs, "Client", return_value=client) as client_type, \
             mock.patch.dict(jobs.JOBS, {
                 "brief": lambda args, shared: {"shared": shared is client},
             }), \
             contextlib.redirect_stdout(output):
            rc = jobs.main(["brief", "alpha", "--store", path])

        self.assertEqual(rc, 0)
        self.assertTrue(json.loads(output.getvalue())["shared"])
        # Client converts the path to a Store; the job closes it afterwards.
        client_type.assert_called_once_with(
            verbose=False, store=path, max_usd=5.0, cache_max_age=None)
        client.store.close.assert_called_once_with()

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

        def fake_corpus(handles, since_ts, until_ts, client=None):
            calls.append((list(handles), until_ts - since_ts, client))
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
        self.assertEqual(calls[0][:2], (["alpha", "beta"], 7 * 86400))
        self.assertEqual([r["tweets_in_window"] for r in out["entities"]], [1, 1])

    def test_monitor_does_not_advance_checkpoint_after_incomplete_window(self):
        """A partial poll must be retried, not checkpointed past missing tweets.

        Contract (2026-10-23 review): the checkpoint now records emitted ids
        incrementally, so the file exists; `last_ts` must stay at the start of
        the unfinished window."""
        import workflows

        def incomplete_window(*args, **kwargs):
            yield tweet("seen-before-gap", 1_700_000_000)
            raise RuntimeError("window incomplete")

        state_path = self.store_path("monitor-state.json")
        if os.path.exists(state_path):
            os.unlink(state_path)
        c = SimpleNamespace(max_usd=1.0, spent_usd=0.0,
                            _over_ceiling=lambda: False)
        now = 1_800_000_000
        with mock.patch.object(workflows, "search_window", incomplete_window), \
             mock.patch.object(workflows.time, "time", return_value=now):
            with self.assertRaises(RuntimeError):
                workflows.monitor(["alpha"], client=c, state_file=state_path,
                                  once=True, on_tweet=lambda _: None,
                                  lookback=900)
        with open(state_path) as fh:
            saved = json.load(fh)
        self.assertEqual(saved["last_ts"], now - 900,
                         "checkpoint must not advance after a partial window")
        self.assertEqual(saved["seen_ids"], ["seen-before-gap"])

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
            "exclude_replies": False,
            "include_retweets": False, "max_pages": None, "max_usd": 1.0,
            "out": None, "store": None, "index_faves": False,
        }
        values.update(overrides)
        return SimpleNamespace(**values)

    def test_history_acquisition_populates_catalogue_store(self):
        """Every paid history row must reach the documented $0 consumer."""
        import workflows
        from jobs import catalogue

        records = [
            tweet("one", 1_700_000_003, "alpha"),
            tweet("two", 1_700_000_002, "alpha"),
            tweet("three", 1_700_000_001, "alpha"),
        ]
        store = self.fresh_store("history-to-catalogue")
        client = ScriptedClient(raw=[{
            "tweets": records,
            "has_next_page": False,
            "next_cursor": "",
        }], store=store)

        acquired = list(workflows.history_search(
            "from:alpha", client=client, progress=False))
        summary = catalogue("alpha", store=store)

        self.assertEqual([row["id"] for row in acquired],
                         ["one", "two", "three"])
        self.assertEqual(
            summary["counts"]["total"], len(acquired),
            "catalogue must summarize the history corpus already paid for",
        )

    def test_history_cli_attaches_selected_store_for_local_consumers(self):
        """The documented CLI must not leave history persistence disabled,
        and must close the store it opened."""
        import workflows
        from jobs import catalogue

        created = []
        scripted = client_class(raw=[{
            "tweets": [tweet("paid", 1_700_000_001, "alpha")],
            "has_next_page": False,
            "next_cursor": "",
        }], created=created)
        store_path = self.store_path("history-cli-store")

        with mock.patch.object(workflows, "Client", scripted), \
             contextlib.redirect_stdout(io.StringIO()), \
             contextlib.redirect_stderr(io.StringIO()):
            rc = workflows._history_cli(self._args(
                out=self.store_path("history-cli-out.jsonl"), store=store_path))

        self.assertEqual(rc, 0)
        self.assertEqual(len(created), 1)
        with self.assertRaises(sqlite3.ProgrammingError):
            created[0].store.db.execute("SELECT 1")     # closed by the CLI
        store = self.fresh_store("history-cli-store")
        self.assertEqual(catalogue("alpha", store=store)["counts"]["total"], 1)

    def test_an_existing_out_file_is_refused_before_any_request(self):
        """A rerun must not empty the rows the first run wrote."""
        import workflows
        from twitterapi import UsageError

        created = []
        out_path = self.store_path("history-existing-out.jsonl")
        with open(out_path, "w") as fh:
            fh.write("first run\n")
        for cli, args in ((workflows._history_cli, self._args(out=out_path)),
                          (workflows._audience_cli, SimpleNamespace(
                              user="alpha", limit=None, out=out_path, max_usd=1.0))):
            with mock.patch.object(workflows, "Client",
                                   client_class(created=created)), self.assertRaises(UsageError):
                cli(args)
        self.assertEqual(created, [], "refused before a client or store exists")
        with open(out_path) as fh:
            self.assertEqual(fh.read(), "first run\n")

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
                                 store=MemoryStore(),
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
        lines = [json.loads(line) for line in out.getvalue().splitlines()]
        self.assertEqual([row["id"] for row in lines[:-1]],
                         ["original", "shared", "retweet"],
                         "the second pass must deduplicate ids across both searches")
        self.assertTrue(lines[-1]["complete"])
        self.assertEqual(lines[-1]["completeness"]["records_returned"], 3)
        banner = err.getvalue().lower()
        self.assertIn("scope", banner)
        self.assertIn("native retweets included", banner)

    def test_page_cap_in_first_pass_does_not_skip_native_retweet_pass(self):
        """Each search pass must receive its cap even when pass one hits it."""
        import workflows
        from twitterapi import IncompleteDataError

        calls = []

        def capped_history(query, *args, **kwargs):
            calls.append((query, kwargs.get("max_pages")))
            if "filter:nativeretweets" not in query:
                yield tweet("original", 1_700_000_003, "alpha")
                raise IncompleteDataError("stopped at --max-pages 1")
            yield dict(tweet("retweet", 1_700_000_001, "alpha"),
                       retweeted_tweet={"id": "source"})

        client = SimpleNamespace(max_usd=1.0, spent_usd=0.0,
                                 store=MemoryStore(),
                                 spend_report=lambda: "2 requests, ~$0.0060")
        out, err = io.StringIO(), io.StringIO()
        with mock.patch.object(workflows, "Client", return_value=client), \
             mock.patch.object(workflows, "history_search", capped_history), \
             contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            rc = workflows._history_cli(
                self._args(include_retweets=True, max_pages=1))

        self.assertEqual(rc, 0, "valid partial data uses exit 0 under AXI")
        self.assertEqual(calls, [
            ("from:alpha", 1),
            ("from:alpha filter:nativeretweets", 1),
        ])
        lines = [json.loads(line) for line in out.getvalue().splitlines()]
        self.assertEqual([row["id"] for row in lines[:-1]], ["original", "retweet"])
        self.assertFalse(lines[-1]["complete"])
        self.assertEqual(lines[-1]["completeness"]["status"], "partial")
        self.assertEqual(lines[-1]["completeness"]["records_returned"], 2)
        self.assertIn("native retweets 1", err.getvalue().lower())


    def test_default_history_explicitly_declares_native_retweets_excluded(self):
        """A default history cannot make no-retweets look like user behavior."""
        import workflows

        calls = []

        def fake_history(query, *args, **kwargs):
            calls.append(query)
            return iter(())

        # A zero-post USER history now resolves the handle (2026-10-23 review:
        # a nonexistent account must not read as an empty archive).
        client = SimpleNamespace(max_usd=1.0, spent_usd=0.0,
                                 store=MemoryStore(),
                                 user_info=lambda handle: {"id": "1", "userName": handle},
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

    def test_index_coverage_message_states_date_direction_and_distance(self):
        """Coverage diagnostics must not call a later crawl date 'before'."""
        import workflows

        created = datetime(2021, 5, 12, tzinfo=UTC)
        oldest = datetime(2021, 5, 14, tzinfo=UTC)
        rec = tweet("oldest-indexed", int(oldest.timestamp()), "alpha")
        rec["author"].update({
            "createdAt": created.strftime("%a %b %d %H:%M:%S %z %Y"),
            "statusesCount": 2,
        })

        def fake_history(*args, **kwargs):
            yield rec

        client = SimpleNamespace(max_usd=1.0, spent_usd=0.0,
                                 store=MemoryStore(),
                                 spend_report=lambda: "1 request, ~$0.0030")
        out, err = io.StringIO(), io.StringIO()
        with mock.patch.object(workflows, "Client", return_value=client), \
             mock.patch.object(workflows, "history_search", fake_history), \
             contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            rc = workflows._history_cli(self._args(out=self.store_path("history-out-coverage")))

        report = err.getvalue().lower()
        self.assertEqual(rc, 0)
        self.assertFalse(json.loads(out.getvalue())["complete"])
        self.assertIn(
            "reached back to 2021-05-14, 2 days after @alpha's "
            "2021-05-12 account creation",
            report,
        )
        self.assertNotIn("before @alpha", report)
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
                                 store=MemoryStore(),
                                 spend_report=lambda: "1 request, ~$0.0030")
        out, err = io.StringIO(), io.StringIO()
        with mock.patch.object(workflows, "Client", return_value=client), \
             mock.patch.object(workflows, "history_search", fake_history), \
             contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            rc = workflows._history_cli(self._args(out=self.store_path("history-out-gap")))

        report = err.getvalue().lower()
        self.assertEqual(rc, 0, "valid partial data uses exit 0 under AXI")
        payload = json.loads(out.getvalue())
        self.assertFalse(payload["complete"])
        self.assertEqual(payload["completeness"]["status"], "partial")
        self.assertIn("index coverage: partial", report)
        self.assertIn("2009", report)
        self.assertIn("2018", report)
        self.assertIn("upper bound", report,
                      "stated-post-count pricing is not a point estimate")
        self.assertIn("$3.14", report)


class TestJobsCliContracts(E2ETest):
    def test_sample_limits_authority_cohort(self):
        """The authority refusal advice must expose a working shrink knob."""
        import jobs

        client = SimpleNamespace(store=SimpleNamespace(close=lambda: None))
        result = {"complete": True, "frontier": []}
        with mock.patch.object(jobs, "_client", return_value=client), \
             mock.patch.object(jobs, "authority_map", return_value=result) as run, \
             contextlib.redirect_stdout(io.StringIO()):
            rc = jobs.main(["authority", "seed", "--sample", "30",
                            "--max-usd", "0.45"])

        self.assertEqual(rc, 0)
        run.assert_called_once_with("seed", cohort_limit=30, max_usd=0.45,
                                    client=client, progress=True)

    def test_sample_is_rejected_before_client_for_inapplicable_job(self):
        """A parsed global option must never be silently ignored."""
        import jobs

        with mock.patch.object(
                jobs, "_client", side_effect=AssertionError("client constructed")):
            with self.assertRaises(SystemExit) as cm, \
                 contextlib.redirect_stdout(io.StringIO()), \
                 contextlib.redirect_stderr(io.StringIO()):
                jobs.main(["brief", "alpha", "--sample", "30"])

        self.assertEqual(cm.exception.code, 2)

    def test_partial_authority_json_exits_zero_with_payload_signal(self):
        """Shell callers must distinguish usable partial data inside the payload."""
        import jobs

        client = SimpleNamespace(store=SimpleNamespace(close=lambda: None))
        result = {
            "complete": False, "members_crawled": 85,
            "frontier": [{"handle": "alice", "in_degree": 4}],
            "_warning": "PARTIAL: spend ceiling hit mid-crawl",
        }
        out = io.StringIO()
        with mock.patch.object(jobs, "_client", return_value=client), \
             mock.patch.object(jobs, "authority_map", return_value=result), \
             contextlib.redirect_stdout(out):
            rc = jobs.main(["authority", "seed", "--max-usd", "1.60"])

        self.assertEqual(rc, 0)
        payload = json.loads(out.getvalue())
        self.assertFalse(payload["complete"])
        self.assertEqual(payload["completeness"]["status"], "partial")
        self.assertEqual(payload["completeness"]["records_returned"], 1)
        self.assertEqual(payload["frontier"][0]["handle"], "alice")


class TestAuthorityContracts(E2ETest):
    def test_authority_reports_member_progress_and_running_spend(self):
        """A paid multi-member crawl must never look hung for minutes."""
        from cohort import Cohort

        class Client:
            max_usd = 5.0
            spent_usd = 0.0
            requests_made = 0

            def paginate(inner, name, handle):
                inner.requests_made += 1
                inner.spent_usd += 0.01
                return iter(())

            def spend_report(inner):
                return (f"{inner.requests_made} requests, "
                        f"~${inner.spent_usd:.4f}")

        c = Client()
        co = Cohort(client=c, store=MemoryStore())
        co._add("1", "alice", 1.0, "test")
        co._add("2", "bob", 1.0, "test")
        out, err = io.StringIO(), io.StringIO()

        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            co.authority(max_usd=1.0, progress=True)

        self.assertEqual(out.getvalue(), "", "progress must not corrupt JSON stdout")
        report = err.getvalue()
        self.assertIn("1/2", report)
        self.assertIn("2/2", report)
        self.assertIn("requests", report)
        self.assertIn("$0.0200", report)

    def test_authority_estimate_is_calibrated_above_observed_cost(self):
        """The guard's stated budget must cover the measured 1.88x crawl cost."""
        from cohort import Cohort
        from twitterapi import CostLimitExceeded

        class Client:
            max_usd = 5.0
            spent_usd = 0.0

            def paginate(inner, *args, **kwargs):
                raise AssertionError("paid crawl started below the safe estimate")

        co = Cohort(client=Client(), store=MemoryStore())
        for i in range(127):
            co._add(str(i + 1), f"member{i + 1}", 1.0, "test")

        with self.assertRaises(CostLimitExceeded) as cm:
            co.authority(max_usd=1.60, confirm=True)

        message = str(cm.exception)
        self.assertIn("$2.54", message)
        self.assertIn("2,000 followings/account", message)


class TestAbsentResourceSemantics(E2ETest):
    def test_null_community_identity_raises_instead_of_returning_zero_facts(self):
        """A missing community cannot truthfully have zero members or NSFW false."""
        from twitterapi import APIError

        c = ScriptedClient(raw=[{
            "community_info": {
                "id": None, "member_count": 0, "moderator_count": 0,
                "is_nsfw": False,
            },
        }])

        with self.assertRaises(APIError) as cm:
            c.community_info("999999999999999999")

        self.assertIn("not found", str(cm.exception).lower())

    def test_empty_community_collection_validates_resource_identity(self):
        """An empty page for a nonexistent community is not an empty community."""
        from twitterapi import APIError

        c = ScriptedClient(raw=[
            {"members": [], "has_next_page": False, "next_cursor": ""},
            {"community_info": {"id": None, "member_count": 0}},
        ])

        with self.assertRaises(APIError):
            list(c.paginate("community_members", "999999999999999999"))

        self.assertEqual([call[1] for call in c.raw_calls], [
            "/twitter/community/members", "/twitter/community/info",
        ])

    def test_empty_list_page_is_incomplete_not_confident_zero(self):
        """Empty, private, and nonexistent lists must remain distinguishable."""
        from twitterapi import IncompleteDataError

        c = ScriptedClient(raw=[{
            "members": [], "has_next_page": False, "next_cursor": "",
        }])

        with self.assertRaises(IncompleteDataError) as cm:
            list(c.paginate("list_members", "1493809298814746624"))

        message = str(cm.exception).lower()
        self.assertIn("empty", message)
        self.assertIn("private", message)
        self.assertIn("not exist", message)


class TestLocalCorpusCapabilities(E2ETest):
    def _cached_corpus(self):
        s = self.fresh_store("catalogue")
        jan = 1_704_067_200
        feb = 1_706_745_600
        records = [
            dict(tweet("root", jan, "alpha"), text="root", likeCount=2,
                 retweetCount=1, replyCount=2, quoteCount=3, viewCount=10,
                 bookmarkCount=4, conversationId="root",
                 extendedEntities={"media": [{
                     "type": "photo",
                     "media_url_https":
                         "https://pbs.twimg.com/media/catalogue.jpg",
                     "ext_alt_text": "Configuration screenshot",
                 }]}),
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
        # Contract change (2026-10-23 review): each metric also reports how
        # many rows carried a known value, so averages never fold in absences.
        self.assertEqual(result["engagement"]["likes"],
                         {"total": 14, "average": 3.5, "known": 4, "unknown": 0})
        self.assertEqual(set(result["engagement"]), {
            "likes", "retweets", "replies", "quotes", "views", "bookmarks",
        })
        self.assertEqual(result["monthly"], [
            {"month": "2024-01", "volume": 2, "median_likes": 5.0},
            {"month": "2024-02", "volume": 2, "median_likes": 2.0},
        ])
        self.assertEqual(result["spend"], "$0.00 (local cache only)")
        self.assertEqual(result["media"]["tweets_with_media"], 1)
        self.assertEqual(result["media"]["artifact_count"], 1)
        self.assertEqual(result["media"]["retrieved_count"], 0)
        self.assertEqual(result["media"]["undisplayed_count"], 1)
        self.assertIn("not downloaded", result["media"]["_warning"].lower())

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
        self.assertEqual(thread["tweets"][0]["media"][0]["type"], "photo")
        self.assertEqual(result["media"]["undisplayed_count"], 1)
        self.assertIn("not downloaded", result["media"]["_warning"].lower())
        self.assertEqual(result["spend"], "$0.00 (local cache only)")

    def test_media_local_job_lists_cached_artifacts_without_a_client(self):
        """The CLI must expose cached media URLs without a paid API client."""
        import jobs
        from store import Store

        path = self.store_path("media-cli")
        with Store(path) as store:
            with self._cached_corpus() as source_store:
                source = source_store.tweets_for(user_names=["alpha"])
            store.put_tweets([json.loads(row["raw"]) for row in source])
        output = io.StringIO()
        with mock.patch.object(jobs, "_client",
                               side_effect=AssertionError("paid client created")), \
             contextlib.redirect_stdout(output):
            rc = jobs.main(["media", "alpha", "--store", path])

        result = json.loads(output.getvalue())
        self.assertEqual(rc, 0)
        self.assertEqual(result["artifact_count"], 1)
        self.assertEqual(result["artifacts"][0]["type"], "photo")
        self.assertIn("pbs.twimg.com", result["artifacts"][0]["url"])

    def test_brief_exposes_media_and_discloses_unretrieved_bytes(self):
        """A text brief must state when image evidence was not interpreted."""
        import jobs

        record = dict(tweet("image", 1_700_000_000, "alpha"),
                      text="configuration is in the screenshot", likeCount=10,
                      media=[{
                          "type": "photo",
                          "url": "https://pbs.twimg.com/media/brief.jpg",
                          "alt_text": None,
                          "full_resolution_url": (
                              "https://pbs.twimg.com/media/brief.jpg?"
                              "format=jpg&name=4096x4096"),
                      }])

        class Client:
            def user_info(self, handle):
                return {"name": handle, "followers": 1,
                        "description": "", "isBlueVerified": False}

            def spend_report(self):
                return "0 requests"

        with mock.patch.object(jobs, "corpus", return_value=[record]):
            result = jobs.entity_brief("alpha", client=Client())

        self.assertEqual(result["top_tweets"][0]["media"], record["media"])
        self.assertEqual(result["media"]["undisplayed_count"], 1)
        self.assertIn("not downloaded", result["media"]["_warning"].lower())

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




if __name__ == "__main__":
    unittest.main()
