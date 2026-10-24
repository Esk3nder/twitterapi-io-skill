"""Negative tests for the 2026-10-23 adversarial review of the client and
cohort layer. Zero network, $0: transport is scripted at Client._raw or at
urllib.request.urlopen, so the real budget, pagination and cohort logic run.

Each test names the review defect it pins. Every one failed against the code
before the fix.
"""
from __future__ import annotations

import contextlib
import io
import json
import threading
import unittest
from unittest import mock

from tests.e2e_base import E2ETest
from tests.fakes import ScriptedClient
from tests.test_05_invariants import tweet


def _page(items, key="tweets", has_next=False, cursor=""):
    return {key: items, "has_next_page": has_next, "next_cursor": cursor}


class _Response:
    def __init__(self, body):
        self._body = json.dumps(body).encode()

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _live_client(max_usd):
    """Real Client and real transport; only urlopen is replaced per test."""
    from twitterapi import Client
    return Client(api_key="test", verbose=False, max_usd=max_usd, qps=1000)


class TestBudgetReservation(E2ETest):
    def test_fixed_page_endpoint_budgets_its_real_page_not_page_size(self):
        """#1: page_size=1 on search must not shrink the reserved 20-tweet page."""
        from twitterapi import CostLimitExceeded
        page = _page([tweet(i, 1_700_000_000 - i) for i in range(20)])
        c = ScriptedClient(raw=[page], max_usd=0.00015)

        with self.assertRaises(CostLimitExceeded):
            list(c.paginate("search", "from:alpha", page_size=1, limit=1))
        self.assertEqual(c.raw_calls, [], "refusal must precede transport")
        self.assertEqual(c.spent_credits, 0.0)

    def test_concurrent_requests_cannot_both_pass_the_ceiling(self):
        """#2: the second in-flight search must see the first one's reservation."""
        from twitterapi import CostLimitExceeded
        c = _live_client(max_usd=0.00315)       # one 300-credit page + 15
        page = _page([tweet(i, 1_700_000_000 - i) for i in range(20)])
        both_in_flight = threading.Barrier(2, timeout=1.0)
        calls, outcomes = [], []

        def fake_urlopen(req, timeout=None):
            calls.append(req.full_url)
            with contextlib.suppress(threading.BrokenBarrierError):
                both_in_flight.wait()
            return _Response(page)

        def search(query):
            try:
                outcomes.append(len(list(c.paginate("search", query))))
            except CostLimitExceeded:
                outcomes.append("refused")

        with mock.patch("urllib.request.urlopen", fake_urlopen):
            threads = [threading.Thread(target=search, args=(q,))
                       for q in ("from:alpha", "from:beta")]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

        self.assertEqual(len(calls), 1, "only one page fits under the ceiling")
        self.assertEqual(sorted(outcomes, key=str), [20, "refused"])
        self.assertLessEqual(c.spent_usd, c.max_usd)

    def test_retry_after_lost_response_books_the_worst_case(self):
        """#3: a timed-out page may have been billed in full; a retry that
        could double-bill past the ceiling is refused, not sent."""
        from twitterapi import CostLimitExceeded
        c = _live_client(max_usd=0.00315)
        page = _page([tweet(i, 1_700_000_000 - i) for i in range(20)])
        replies = [TimeoutError("read timed out"), _Response(page)]
        calls = []

        def fake_urlopen(req, timeout=None):
            calls.append(req.full_url)
            reply = replies.pop(0)
            if isinstance(reply, BaseException):
                raise reply
            return reply

        with mock.patch("urllib.request.urlopen", fake_urlopen), \
                mock.patch("twitterapi.time.sleep"):
            with self.assertRaises(CostLimitExceeded) as cm:
                list(c.paginate("search", "from:alpha"))

        self.assertEqual(len(calls), 1, "the retry must not be sent")
        self.assertIsInstance(cm.exception.__cause__, TimeoutError)
        self.assertEqual(c.spent_credits, 300.0,
                         "the lost attempt stays booked at its worst case")

    def test_http_error_releases_the_reservation_to_the_floor(self):
        """#3 boundary: an answered error bills the floor, not a page."""
        import urllib.error
        from twitterapi import APIError
        c = _live_client(max_usd=1.0)

        def fake_urlopen(req, timeout=None):
            raise urllib.error.HTTPError(req.full_url, 400, "bad", {},
                                         io.BytesIO(b'{"detail": "bad"}'))

        with mock.patch("urllib.request.urlopen", fake_urlopen):
            with self.assertRaises(APIError):
                list(c.paginate("search", "from:alpha"))
        self.assertEqual(c.spent_credits, 15.0)

    def test_full_page_estimate_is_not_the_spend_bound(self):
        """#7: short pages cost more than estimate(); max_usd still binds."""
        from twitterapi import CostLimitExceeded
        pages = [_page([str(i)], key="ids", has_next=True, cursor=f"c{i}")
                 for i in range(200)]
        c = ScriptedClient(raw=pages, max_usd=None)
        c.max_usd = c.estimate("follower_ids", 200) + 0.00015 * 5
        self.assertIn("not an upper bound", c.estimate.__doc__.lower())

        got = []
        with self.assertRaises(CostLimitExceeded):
            for uid in c.paginate("follower_ids", "alice", limit=200):
                got.append(uid)
        self.assertLessEqual(c.spent_usd, c.max_usd)
        self.assertLess(len(got), 200)


class TestPaginationMetadata(E2ETest):
    def test_absent_has_next_page_is_not_complete(self):
        """#4: a page with a cursor but no has_next_page is unknown, not done."""
        from twitterapi import IncompleteDataError
        first = {"tweets": [tweet("1", 104)], "next_cursor": "c2"}
        c = ScriptedClient(raw=[first])
        with self.assertRaises(IncompleteDataError):
            list(c.paginate("search", "from:alpha"))

        later = [_page([tweet("1", 104)], has_next=True, cursor="c2"),
                 {"tweets": [tweet("2", 103)], "next_cursor": "c3"}]
        c = ScriptedClient(raw=later)
        stream = c.paginate("search", "from:alpha")
        with contextlib.redirect_stderr(io.StringIO()):
            got = list(stream)
        self.assertEqual(len(got), 1, "only rows from readable pages are kept")
        self.assertFalse(stream.complete)

    def test_limit_stop_with_more_data_is_flagged_truncated(self):
        """#9: "first N" must be distinguishable from "the whole population"."""
        c = ScriptedClient(raw=[_page([tweet(i, 200 - i) for i in range(3)],
                                       has_next=True, cursor="c2")])
        stream = c.paginate("search", "from:alpha", limit=2)
        self.assertEqual(len(list(stream)), 2)
        self.assertTrue(stream.complete)
        self.assertTrue(stream.truncated_by_limit)

        c = ScriptedClient(raw=[_page([tweet(i, 200 - i) for i in range(2)])])
        stream = c.paginate("search", "from:alpha", limit=2)
        self.assertEqual(len(list(stream)), 2)
        self.assertFalse(stream.truncated_by_limit,
                         "an exact final page is the whole population")

    def test_truncation_is_known_to_a_consumer_that_stops_at_n(self):
        """#9: the flag must not depend on resuming the generator past N."""
        c = ScriptedClient(raw=[_page([tweet(i, 200 - i) for i in range(3)])])
        stream = c.paginate("search", "from:alpha", limit=2)
        it = iter(stream)
        next(it), next(it)
        self.assertTrue(stream.truncated_by_limit)

    def test_cache_hit_does_not_repersist_stale_records(self):
        """#8: a year-old cached batch page must not overwrite newer rows."""
        store = self.fresh_store("stale-cache-repersist")
        self.addCleanup(store.close)
        path = "/twitter/user/batch_info_by_ids"
        store.put_page(path, {"userIds": "1", "cursor": ""},
                       {"users": [{"id": "1", "userName": "alice",
                                   "followers": 100}]})
        store.db.execute("UPDATE pages SET fetched_at = fetched_at - 31536000")
        store.db.commit()
        store.put_accounts([{"id": "1", "userName": "alice", "followers": 500}])
        c = ScriptedClient(raw=[], max_usd=0.0, store=store)

        users = list(c.paginate("batch_users", "1"))

        self.assertEqual(users[0]["followers"], 100, "the cache hit is served")
        self.assertEqual(c.raw_calls, [])
        self.assertEqual(store.db.execute(
            "SELECT followers FROM accounts WHERE id='1'").fetchone()[0], 500)


class TestSearchQueryRewrite(E2ETest):
    def test_min_faves_under_or_is_refused_not_rewritten(self):
        """#5: one global threshold would drop an `a` tweet with 50 likes."""
        from twitterapi import prepare_search_query
        for query in ("from:a min_faves:10 OR from:b min_faves:100",
                      "from:a min_faves:10 OR from:b",
                      "-(from:a min_faves:5)",
                      "(from:a min_faves:5 lang:en) OR from:b"):
            with self.subTest(query=query):
                with self.assertRaises(ValueError):
                    prepare_search_query(query)
        c = ScriptedClient(raw=[_page([])], max_usd=1.0)
        with self.assertRaises(ValueError):
            list(c.paginate("search", "from:a min_faves:10 OR from:b min_faves:100"))
        self.assertEqual(c.raw_calls, [])

    def test_top_level_conjunction_is_still_rewritten_exactly(self):
        from twitterapi import prepare_search_query
        self.assertEqual(prepare_search_query("(from:a OR from:b) min_faves:10"),
                         ("(from:a OR from:b)", 10))
        self.assertEqual(prepare_search_query('"x OR y" min_faves:3'),
                         ('"x OR y"', 3))
        self.assertEqual(prepare_search_query("a min_faves:10 min_faves:20"),
                         ("a", 20))


class TestProfileAbsence(E2ETest):
    def test_null_user_data_is_an_error_not_an_empty_profile(self):
        """#6: a null `data` must not become a user with 0 followers."""
        from twitterapi import APIError
        c = ScriptedClient(raw=[{"data": None, "status": "success"}])
        with self.assertRaises(APIError):
            c.user_info("ghost")

    def test_missing_follower_count_is_not_zero(self):
        from twitterapi import IncompleteDataError
        c = ScriptedClient(raw=[{"data": {"id": "1", "userName": "alice"},
                                  "status": "success"}])
        with self.assertRaises(IncompleteDataError):
            c.follower_count("alice")

    def test_missing_relationship_is_unknown_not_unfollowed(self):
        """check_follow returned {} for a missing `data`, which reads as
        "neither account follows the other"."""
        from twitterapi import IncompleteDataError
        for body in ({"status": "success"}, {"status": "success", "data": None},
                     {"status": "success", "data": {}}):
            c = ScriptedClient(raw=[body])
            with self.assertRaises(IncompleteDataError, msg=body):
                c.check_follow("alice", "bob")
        c = ScriptedClient(raw=[{"status": "success",
                                  "data": {"following": True, "followed_by": False}}])
        self.assertEqual(c.check_follow("alice", "bob"),
                         {"following": True, "followed_by": False})


class _Stream(list):
    complete = True
    truncated_by_limit = False


class _StubClient:
    max_usd = None
    spent_usd = 0.0

    def __init__(self, plan):
        self.plan = plan

    def paginate(self, name, key, **kw):
        result = self.plan.get((name, str(key)), _Stream())
        if isinstance(result, BaseException):
            raise result
        return result

    def spend_report(self):
        return "stub"


class TestCohortPopulationHonesty(E2ETest):
    def test_limit_truncated_resolver_is_marked_partial(self):
        """#10: the first 2 of 3 community members are a sample."""
        from cohort import Cohort
        c = ScriptedClient(raw=[_page(
            [{"id": str(i), "userName": f"u{i}"} for i in range(3)],
            key="members", has_next=True, cursor="c2")])
        with contextlib.redirect_stderr(io.StringIO()):
            co = Cohort.from_community("77", limit=2, client=c,
                                       store=self.fresh_store("co-limit"))
        self.addCleanup(co.s.close)
        self.assertEqual(len(co), 2)
        self.assertFalse(co.complete)
        self.assertIn("limit=2", co.incomplete_reasons[0])

    def test_partial_cohort_stays_partial_through_save_load_and_algebra(self):
        from cohort import Cohort
        store = self.fresh_store("co-partial-roundtrip")
        self.addCleanup(store.close)
        co = Cohort.from_ids(["1", "2"], client=object(), store=store)
        with contextlib.redirect_stderr(io.StringIO()):
            co._mark_partial("stopped at limit")
            co.save("sample")
            loaded = Cohort.load("sample", client=object(), store=store)
        whole = Cohort.from_ids(["1"], client=object(), store=store)
        self.assertFalse(loaded.complete)
        self.assertFalse(whole.intersect(loaded).complete)
        self.assertFalse(whole.minus(loaded).complete)

    def test_failed_engager_leg_marks_cohort_partial(self):
        """#11: a reply-only cohort is not every engager."""
        from cohort import Cohort
        from twitterapi import APIError
        c = _StubClient({
            ("replies_v2", "9"): _Stream([{"author": {"id": "1", "userName": "a"}}]),
            ("quotes", "9"): APIError(500, "boom", "/twitter/tweet/quotes"),
        })
        with contextlib.redirect_stderr(io.StringIO()):
            co = Cohort.from_engagers("9", client=c,
                                      store=self.fresh_store("co-engagers"))
        self.addCleanup(co.s.close)
        self.assertEqual(len(co), 1)
        self.assertFalse(co.complete)
        self.assertIn("quote", co.incomplete_reasons[0])

    def test_load_of_unsaved_name_or_version_raises(self):
        """#15: a typo must not load as an empty cohort with overlap 0."""
        from cohort import Cohort
        store = self.fresh_store("co-load-missing")
        self.addCleanup(store.close)
        Cohort.from_ids(["1"], client=object(), store=store).save("real")
        with self.assertRaises(KeyError):
            Cohort.load("rael", client=object(), store=store)
        with self.assertRaises(KeyError):
            Cohort.load("real", 7, client=object(), store=store)

    def test_filter_counts_unknown_followers_and_bio(self):
        """#14: NULL followers / NULL bio are unknown, not 0 / empty."""
        from cohort import Cohort
        store = self.fresh_store("co-filter-unknown")
        self.addCleanup(store.close)
        store.put_accounts([
            {"id": "1", "userName": "known", "followers": 1000,
             "description": "ai founder"},
            {"id": "2", "userName": "nocount"},
        ])
        co = Cohort.from_ids(["1", "2"], client=object(), store=store)
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            by_followers = co.filter(min_followers=500)
            by_bio = co.filter(bio_matches="founder")
        self.assertEqual(by_followers.skipped_unknown_followers, 1)
        self.assertEqual(by_bio.skipped_unknown_bio, 1)
        self.assertFalse(by_followers.complete)
        self.assertIn("follower count", err.getvalue())


class TestAuthorityCompleteness(E2ETest):
    def test_unresolvable_member_makes_authority_partial(self):
        """#12: a member with no handle contributes no edges."""
        from cohort import Cohort
        c = _StubClient({
            ("batch_users", "2"): _Stream([]),
            ("followings", "alice"): _Stream([{"id": "3", "userName": "carol"}]),
        })
        co = Cohort(client=c, store=self.fresh_store("auth-unresolved"))
        self.addCleanup(co.s.close)
        co._add("1", "alice")
        co._add("2", None)
        co._add("3", "carol")
        co.authority(max_usd=100, confirm=False)
        self.assertIs(co.authority_complete, False)
        self.assertEqual(co.authority_crawled, 2)
        self.assertIn("no resolvable handle", co.authority_incomplete_reason)

    def test_handle_resolution_reserves_the_whole_batch(self):
        """#12: 100 id-only members must not spend $0.018 at max_usd=.001."""
        from cohort import Cohort
        c = ScriptedClient(raw=[{"users": [
            {"id": str(i), "userName": f"u{i}"} for i in range(100)]}],
            max_usd=None)
        co = Cohort.from_ids([str(i) for i in range(100)], client=c,
                             store=self.fresh_store("auth-resolution-budget"))
        self.addCleanup(co.s.close)
        co.authority(max_usd=0.001, confirm=False)
        self.assertEqual(c.raw_calls, [], "the batch must be refused up front")
        self.assertLessEqual(c.spent_usd, 0.001)
        self.assertIs(co.authority_complete, False)
        self.assertIn("ceiling", co.authority_incomplete_reason)

    def test_one_member_api_error_does_not_abort_the_crawl(self):
        """#13: a 403 on one member keeps every other member's paid edges."""
        from cohort import Cohort
        from twitterapi import APIError
        c = _StubClient({
            ("followings", "alice"): APIError(403, "protected",
                                              "/twitter/user/followings"),
            ("followings", "bob"): _Stream([{"id": "3", "userName": "carol"}]),
            ("followings", "carol"): _Stream([{"id": "2", "userName": "bob"}]),
        })
        co = Cohort(client=c, store=self.fresh_store("auth-member-error"))
        self.addCleanup(co.s.close)
        for aid, name in (("1", "alice"), ("2", "bob"), ("3", "carol")):
            co._add(aid, name)
        with contextlib.redirect_stderr(io.StringIO()):
            co.authority(max_usd=100, confirm=False)
        self.assertIs(co.authority_complete, False)
        self.assertEqual(co.authority_crawled, 2)
        self.assertIn("403", co.authority_incomplete_reason)
        self.assertEqual(co.members["3"]["weight"], 1)
        self.assertEqual(co.members["2"]["weight"], 1)


class TestNonJsonBody(unittest.TestCase):
    def test_html_body_is_an_api_error_not_a_value_error(self):
        """A 200 with an HTML body raised JSONDecodeError, a ValueError, which
        jobs.py reported as a refusal (exit 2) instead of an API failure."""
        import twitterapi

        class Body(io.BytesIO):
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False
        c = twitterapi.Client(api_key="test", max_usd=1, verbose=False)
        with mock.patch.object(twitterapi.urllib.request, "urlopen",
                               lambda req, timeout=None:
                               Body(b"<html>502 Bad Gateway</html>")), \
             mock.patch.object(twitterapi.Client, "_throttle", lambda self: None):
            with self.assertRaises(twitterapi.APIError) as cm:
                c.user_info("someone")
        self.assertNotIsInstance(cm.exception, ValueError)
        self.assertIn("not JSON", str(cm.exception))
        self.assertEqual(c.requests_made, 1, "no retry of an answered request")


class TestHydrateMatchesItsContract(E2ETest):
    def test_cached_full_profile_is_not_bought_again(self):
        """#16: a profiled member costs nothing to hydrate."""
        from cohort import Cohort
        store = self.fresh_store("hydrate-skip-cached")
        self.addCleanup(store.close)
        store.put_accounts([{"id": "1", "userName": "alice", "followers": 5,
                             "description": "bio", "isBlueVerified": False}])
        c = ScriptedClient(raw=[], max_usd=None, store=store)
        co = Cohort.from_ids(["1"], client=c, store=store)
        co.hydrate(max_usd=1.0)
        self.assertEqual(c.raw_calls, [])
        self.assertEqual(co.handles(), ["alice"])

    def test_handle_only_members_are_hydrated_or_reported(self):
        """#16: handle-only members were silently never hydrated."""
        from cohort import Cohort
        from twitterapi import APIError
        store = self.fresh_store("hydrate-handles")
        self.addCleanup(store.close)
        c = ScriptedClient(raw=[
            {"data": {"id": "7", "userName": "alice", "followers": 9,
                      "description": "", "isBlueVerified": True},
             "status": "success"},
            APIError(200, "user not found", "/twitter/user/info"),
        ], max_usd=None, store=store)
        co = Cohort(client=c, store=store)
        co._add(None, "alice")
        co._add(None, "ghost")
        with contextlib.redirect_stderr(io.StringIO()):
            co.hydrate(max_usd=1.0)
        self.assertIn("7", co.members, "alice is re-keyed under her id")
        self.assertEqual(co.unhydrated, ["@ghost"])
        self.assertEqual(len(co.filter(min_followers=1)), 1)

    def test_fully_profiled_cohort_needs_no_client_or_key(self):
        """hydrate() read self.c.store before deciding anything was needed, so
        a cohort profiled entirely from the local store still built a Client
        and failed without TWITTERAPI_IO_KEY."""
        import os
        from cohort import Cohort
        store = self.fresh_store("hydrate-no-client")
        self.addCleanup(store.close)
        store.put_accounts([
            {"id": "7", "userName": "a", "description": "bio", "followers": 3,
             "isBlueVerified": False},
            {"id": "8", "userName": "b", "description": "", "followers": 0,
             "isBlueVerified": True}])
        co = Cohort.from_ids(["7"], store=store)
        co._add(None, "b")
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("TWITTERAPI_IO_KEY", None)
            self.assertEqual(co.hydrate(), [])
        self.assertIsNone(co._client, "no request was needed, so no Client")
        self.assertEqual(sorted(co.members), ["7", "8"])
        self.assertEqual(co.unhydrated, [])


if __name__ == "__main__":
    unittest.main()


class TestSingleObjectEnvelopes(E2ETest):
    """Found by the live verify.py run on 2026-10-23: the strict has_next_page
    rule (#4) was applied to single-object endpoints, so user_info,
    user_about and community_info failed on their real envelopes, which carry
    no pagination fields at all."""

    def test_single_object_endpoints_need_no_pagination_fields(self):
        from twitterapi import Client, ENDPOINTS
        single = [n for n, spec in ENDPOINTS.items() if spec["items"] is None]
        self.assertIn("user_info", single)
        for name in single:
            items, has_next, cursor = Client._unpack(
                {"data": {"id": "1"}, "status": "success", "msg": "success"},
                ENDPOINTS[name])
            self.assertEqual((has_next, cursor), (False, ""), name)


class TestSearchPaginationAbsence(E2ETest):
    """Found by the diff review: _unpack refused a missing has_next_page, but
    bulk_search and the search window walk still read it as "no more pages",
    so a full first page with no pagination field made brief report a
    complete 20-tweet window."""

    def test_bulk_subresult_without_has_next_page_is_incomplete(self):
        from twitterapi import IncompleteDataError
        c = ScriptedClient(raw_json=[{"status": "success", "results": {
            "query_0": {"tweets": [tweet(i, 1_700_000_000 - i) for i in range(20)]}}}])
        with self.assertRaises(IncompleteDataError):
            c.bulk_search(["from:alpha"])

    def test_bulk_subresult_with_has_next_page_is_kept(self):
        c = ScriptedClient(raw_json=[{"status": "success", "results": {
            "query_0": {"tweets": [tweet(1, 1_700_000_000)], "has_next_page": False}}}])
        self.assertFalse(c.bulk_search(["from:alpha"])[0]["has_next_page"])

    def test_window_page_without_has_next_page_is_incomplete(self):
        from twitterapi import IncompleteDataError
        from workflows import search_window
        c = ScriptedClient(raw=[{"tweets": [tweet(i, 1_700_000_000 - i)
                                              for i in range(15)]}])
        got = []
        with self.assertRaises(IncompleteDataError):
            for t in search_window(c, "from:alpha", 1_699_990_000, 1_700_000_001):
                got.append(t)
        # The raise, not withholding valid rows, is what stops the window
        # reading as proven walked: every other partial stop yields first.
        self.assertEqual(len(got), 15, "the paid page's tweets are valid rows")
