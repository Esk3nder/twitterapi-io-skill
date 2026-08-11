"""Tier 0 — pure math and local sqlite. Zero network, $0.

Covers: the measured cost table (credits_for / page_credits), envelope
normalisation (_unpack, a pure function fed the verified envelope shapes),
store normalisers, member identity, cohort set algebra, cohort versioning +
drift, and store write concurrency. Cohort/Client construction needs the API
key in the environment but performs no HTTP here.
"""
import threading
import unittest

from tests.e2e_base import E2ETest, HAVE_KEY, require_key


class TestCostTable(E2ETest):
    """Client.credits_for is pure math — assert the measured table exactly."""

    def test_cost_identities(self):
        from twitterapi import Client, MIN_REQUEST_CREDITS
        self.repro("python3 -c \"from twitterapi import Client; "
                   "print(Client.credits_for('follower_ids', 5000))\"")
        cases = [
            # (endpoint, n_records, page_size, expected_credits, why)
            ("follower_ids", 5000, None, 2250.0, "one full ids page @0.45/rec"),
            ("follower_ids", 3000, None, 2250.0, "whole-page billing: partial pages cannot be bought"),
            ("follower_ids", 10000, None, 4500.0, "two full ids pages"),
            ("followers", 201, 200, 400.0, "201 profiles @200/page = 2 pages x 200cr"),
            ("follower_ids", 0, None, MIN_REQUEST_CREDITS, "empty request bills the 15cr floor"),
            ("followers", 0, None, MIN_REQUEST_CREDITS, "empty request bills the 15cr floor"),
        ]
        from twitterapi import Client as C
        for name, n, ps, want, why in cases:
            got = C.credits_for(name, n, ps)
            self.log(f"credits_for({name!r}, {n}, page_size={ps}) = {got} "
                     f"(want {want}: {why})")
            self.assertEqual(got, want, f"{name}/{n}/{ps}: {why}")

    def test_page_credits_floor_and_tiers(self):
        from twitterapi import Client as C
        self.assertEqual(C.page_credits("search", 0, 20), 15.0,
                         "empty page bills the 15-credit per-request floor")
        self.assertEqual(C.page_credits("follower_ids", 5000, 5000), 2250.0)
        self.assertEqual(C.page_credits("followers", 200, 200), 200.0)
        self.assertEqual(C.page_credits("followers", 20, 20), 60.0,
                         "default page size is the 3x tier")
        self.assertEqual(C.page_credits("search", 20, 20), 300.0,
                         "tweets bill flat 15cr/record")

    def test_estimate_conversion(self):
        from twitterapi import Client as C, credits_to_usd
        self.assertAlmostEqual(credits_to_usd(C.credits_for("follower_ids", 200_000_000)),
                               900.0, places=6,
                               msg="200M ids must estimate $900 (verified-facts worked example)")


class TestEnvelopeUnpack(E2ETest):
    """_unpack is a pure function; feed it the four verified envelope shapes.
    The live-envelope key sets are asserted separately in test_20."""

    def test_data_wrapped_with_root_pagination(self):
        from twitterapi import Client, ENDPOINTS
        resp = {"code": 0, "data": {"tweets": [{"id": "1"}, {"id": "2"}], "pin_tweet": None},
                "has_next_page": True, "next_cursor": "abc", "msg": "success", "status": "success"}
        items, has_next, cur = Client._unpack(resp, ENDPOINTS["last_tweets"])
        self.assertEqual([i["id"] for i in items], ["1", "2"])
        self.assertTrue(has_next)
        self.assertEqual(cur, "abc")

    def test_the_trap_pagination_inside_data_is_ignored(self):
        """Pagination keys nested inside `data` must NOT be honoured — the API
        puts them at the root; a nested copy would be a contract change."""
        from twitterapi import Client, ENDPOINTS
        resp = {"data": {"tweets": [{"id": "1"}], "has_next_page": True,
                         "next_cursor": "WRONG"}}
        items, has_next, cur = Client._unpack(resp, ENDPOINTS["last_tweets"])
        self.assertEqual(len(items), 1)
        self.assertFalse(has_next, "must read pagination from ROOT, not data")
        self.assertEqual(cur, "")

    def test_flat_root_envelope(self):
        from twitterapi import Client, ENDPOINTS
        resp = {"code": 0, "ids": ["9", "8"], "has_next_page": False,
                "next_cursor": "", "msg": "success", "status": "success"}
        items, has_next, cur = Client._unpack(resp, ENDPOINTS["follower_ids"])
        self.assertEqual(items, ["9", "8"])
        self.assertFalse(has_next)

    def test_missing_items_key_is_empty_not_crash(self):
        from twitterapi import Client, ENDPOINTS
        items, has_next, _ = Client._unpack({"status": "success"},
                                            ENDPOINTS["search"])
        self.assertEqual(items, [])
        self.assertFalse(has_next)


class TestNormalisers(E2ETest):
    def test_normalize_account_camel_and_snake(self):
        from store import normalize_account
        camel = normalize_account({"id": 7, "userName": "Ada", "followers": 5,
                                   "following": 2, "isBlueVerified": True})
        snake = normalize_account({"user_id": "7", "screen_name": "Ada",
                                   "followers_count": 5, "friends_count": 2,
                                   "is_blue_verified": 1})
        for k in ("id", "user_name", "followers", "following", "is_blue"):
            self.assertEqual(camel[k], snake[k], f"camel/snake disagree on {k}")

    def test_normalize_tweet_retweet_corroboration(self):
        from store import normalize_tweet
        t = normalize_tweet({"id": 1, "retweeted_tweet": {"id": "x"},
                             "createdAt": "Mon Aug 10 17:16:53 +0000 2026"})
        self.assertEqual(t["is_retweet"], 1,
                         "retweeted_tweet presence must corroborate isRetweet")
        self.assertGreater(t["created_ts"], 0)

    def test_parse_ts_malformed_returns_none(self):
        from store import parse_ts
        self.assertIsNone(parse_ts("not a date"))
        self.assertIsNone(parse_ts(""))

    def test_member_key_identity(self):
        from store import Store
        self.assertEqual(Store.member_key("123", "alice"), "123",
                         "id wins when present")
        self.assertEqual(Store.member_key("", "Alice"), "@alice")
        self.assertEqual(Store.member_key("", "@Alice"), "@alice")
        self.assertEqual(Store.member_key(None, None), "@",
                         "degenerate key is filtered by save_cohort")


@unittest.skipUnless(HAVE_KEY, "Cohort construction requires the API key "
                               "(no HTTP is made in these tests)")
class TestSetAlgebra(E2ETest):
    """Pure in-memory determinism of cohort set algebra. The Client instance
    is real but never used for network here."""

    def _client(self):
        from twitterapi import Client
        return Client(verbose=False)

    def _cohort(self, label, members):
        from cohort import Cohort
        co = Cohort(client=self._client(), store=self.fresh_store("algebra"),
                    label=label)
        for aid, name, w in members:
            co._add(aid, name, w, "test")
        return co

    def test_intersect_matches_on_split_identity(self):
        """The same account represented as id-only in A and as (id+handle) in
        B — and as handle-only in A vs (id+handle) in B — must be ONE account.

        Note the information-theoretic limit: id-only in A vs handle-only in B
        with NO shared field cannot match (nothing links '7' to 'ghost');
        asserted below as documented behaviour."""
        A = self._cohort("A", [("1", None, 1.0),        # id-only
                               (None, "carol", 1.0),    # handle-only
                               ("7", None, 1.0)])       # id-only, unlinkable
        B = self._cohort("B", [("1", "alice", 2.0),     # full identity
                               ("2", "carol", 2.0),     # full identity
                               (None, "ghost", 2.0)])   # handle-only, unlinkable
        both = A.intersect(B)
        keys = sorted(both.members.keys())
        self.log(f"intersection keys: {keys}")
        self.assertEqual(len(both), 2,
                         f"expected id-match + handle-match, got {keys}")
        self.assertIn("1", both.members, "id-only in A matched full identity in B")
        got_carol = [m for m in both if m["user_name"] == "carol"]
        self.assertEqual(len(got_carol), 1, "handle-only in A matched full identity in B")
        self.assertEqual(got_carol[0]["account_id"], "2",
                         "intersection must adopt the id learned from B")
        # weights merge
        self.assertEqual(both.members["1"]["weight"], 3.0)

    def test_unlinkable_split_identities_do_not_match(self):
        """id-only vs handle-only with no bridging field is unmatchable by
        construction — documenting this explicitly so nobody 'fixes' it into
        a false positive."""
        A = self._cohort("A", [("7", None, 1.0)])
        B = self._cohort("B", [(None, "ghost", 1.0)])
        self.assertEqual(len(A.intersect(B)), 0)

    def test_union_merges_weights_and_does_not_mutate_operands(self):
        A = self._cohort("A", [("1", "alice", 1.0), (None, "bob", 1.0)])
        B = self._cohort("B", [("1", None, 5.0), ("3", "bob", 2.0)])
        a_before = {k: dict(v) for k, v in A.members.items()}
        b_before = {k: dict(v) for k, v in B.members.items()}
        u = A.union(B)
        self.assertEqual(len(u), 2, f"got keys {sorted(u.members.keys())}")
        self.assertEqual(u.members["1"]["weight"], 6.0)
        bob = [m for m in u if m["user_name"] == "bob"][0]
        self.assertEqual(bob["weight"], 3.0,
                         "bob must be ONE member with merged weight, not two")
        # NOTE (observed, deliberate non-assertion): union's merge branch keeps
        # A's identity fields — it does not adopt bob's id from B the way
        # intersect() does. Reported as a quality observation, not enforced.
        self.assertEqual(A.members, a_before, "union must not mutate A")
        self.assertEqual(B.members, b_before, "union must not mutate B")

    def test_minus(self):
        A = self._cohort("A", [("1", "alice", 1.0), ("2", "bob", 1.0)])
        B = self._cohort("B", [(None, "ALICE", 9.0)])
        d = A.minus(B)
        self.assertEqual(len(d), 1)
        self.assertEqual(list(d)[0]["user_name"], "bob")

    def test_determinism(self):
        A = self._cohort("A", [("1", "alice", 1.0), ("2", "bob", 1.0),
                               (None, "eve", 1.0)])
        B = self._cohort("B", [("2", None, 1.0), (None, "eve", 2.0)])
        r1 = sorted(A.intersect(B).members.keys())
        r2 = sorted(A.intersect(B).members.keys())
        self.assertEqual(r1, r2, "same inputs must produce identical member sets")


@unittest.skipUnless(HAVE_KEY, "cohort_drift constructs a Client (no HTTP here)")
class TestCohortVersioningAndDrift(E2ETest):
    def test_save_load_drift(self):
        from twitterapi import Client
        from cohort import Cohort
        from jobs import cohort_drift
        s = self.fresh_store("drift")
        c = Client(verbose=False, store=s)
        v1 = s.save_cohort("scene", [("1", "alice", 1.0, "t"), ("2", "bob", 1.0, "t")])
        v2 = s.save_cohort("scene", [("2", "bob", 1.0, "t"), ("3", "carol", 1.0, "t")])
        self.assertEqual((v1, v2), (1, 2), "versions must auto-increment")
        out = cohort_drift("scene", client=c)
        self.log(f"drift: {out}")
        self.assertEqual(out["joined"], ["carol"])
        self.assertEqual(out["left"], ["alice"])
        self.assertEqual((out["old_size"], out["new_size"]), (2, 2))
        # load() round-trips
        got = Cohort.load("scene", client=c, store=s)
        self.assertEqual(len(got), 2)


class TestStoreConcurrency(E2ETest):
    def test_15_threads_x_50_put_tweets(self):
        """15 threads x 50 put_tweets of unique tweets -> exactly 750 rows,
        zero errors. (The pre-lock implementation lost 658/750 rows.)"""
        s = self.fresh_store("concurrency")
        errors = []

        def worker(tid):
            try:
                for i in range(50):
                    s.put_tweets([{
                        "id": f"{tid}-{i}",
                        "text": f"tweet {tid}-{i}",
                        "createdAt": "Mon Aug 10 17:16:53 +0000 2026",
                        "likeCount": i,
                        "author": {"id": f"a{tid}", "userName": f"user{tid}"},
                    }])
            except Exception as e:      # noqa: BLE001 — collecting all failures
                errors.append(f"thread {tid}: {type(e).__name__}: {e}")

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(15)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        stats = s.stats()
        self.log(f"errors={errors} stats={stats}")
        self.assertEqual(errors, [], "no thread may error")
        self.assertEqual(stats["tweets"], 750, "every row must survive")
        self.assertEqual(stats["accounts"], 15, "one author row per thread")


if __name__ == "__main__":
    unittest.main()
