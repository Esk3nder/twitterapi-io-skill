"""Tier 0 — pure math and local sqlite. Zero network, $0.

Covers: the measured cost table (credits_for / page_credits), envelope
normalisation (_unpack, a pure function fed the verified envelope shapes),
store normalisers, member identity, cohort set algebra, cohort versioning +
drift, and store write concurrency. Cohort/Client construction needs the API
key in the environment but performs no HTTP here.
"""
import concurrent.futures
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from tests.e2e_base import E2ETest, HAVE_KEY, SKILL, require_key


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

    def test_fixed_qps_never_reads_delayed_account_balance(self):
        """Release runs can throttle safely using client-side accounting only."""
        from twitterapi import Client

        c = Client(api_key="test", verbose=False, qps=5)
        with mock.patch.object(
                c, "balance", side_effect=AssertionError("balance read")):
            self.assertEqual(c.qps, 5)

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

    def test_missing_items_key_is_incomplete_not_empty(self):
        """An omitted required array is contract drift, not proof of zero rows."""
        from twitterapi import Client, ENDPOINTS, IncompleteDataError
        with self.assertRaises(IncompleteDataError):
            Client._unpack({"status": "success"}, ENDPOINTS["search"])

    def test_unknown_paginate_name_lists_the_valid_alias(self):
        """The API-path plural typo must not escape as an unexplained KeyError."""
        from twitterapi import Client
        c = Client(api_key="test", verbose=False)
        with self.assertRaises(ValueError) as cm:
            list(c.paginate("followers_ids", "alice"))
        message = str(cm.exception)
        self.assertIn("follower_ids", message)
        self.assertIn("unknown", message.lower())


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

    def test_tweet_media_is_first_class_and_round_trips_store(self):
        """Paid media metadata must survive without excavating opaque raw JSON."""
        from store import Store, normalize_tweet

        raw = {
            "id": "media-tweet",
            "createdAt": "Mon Aug 10 17:16:53 +0000 2026",
            "author": {"id": "1", "userName": "alice"},
            "extendedEntities": {"media": [{
                "type": "photo",
                "media_url_https": "https://pbs.twimg.com/media/example.jpg",
                "ext_alt_text": "Terminal configuration screenshot",
            }]},
        }
        expected = [{
            "type": "photo",
            "url": "https://pbs.twimg.com/media/example.jpg",
            "alt_text": "Terminal configuration screenshot",
            "full_resolution_url": (
                "https://pbs.twimg.com/media/example.jpg?"
                "format=jpg&name=4096x4096"),
        }]

        self.assertEqual(normalize_tweet(raw)["media"], expected)
        store = self.fresh_store("tweet-media-roundtrip")
        self.addCleanup(store.close)
        store.put_tweets([raw])

        self.assertIn(
            "media",
            [row["name"] for row in store.db.execute("PRAGMA table_info(tweets)")],
        )
        self.assertEqual(store.tweets_for(user_names=["alice"])[0]["media"],
                         expected)

    def test_parse_ts_malformed_returns_none(self):
        from store import parse_ts
        self.assertIsNone(parse_ts("not a date"))
        self.assertIsNone(parse_ts(""))

    def test_parse_ts_iso_z_is_stable_on_python_311(self):
        from store import parse_ts
        self.assertEqual(
            parse_ts("2017-06-27T13:41:42.000000Z"),
            1_498_570_902,
        )

    def test_member_key_identity(self):
        from store import Store
        self.assertEqual(Store.member_key("123", "alice"), "123",
                         "id wins when present")
        self.assertEqual(Store.member_key("", "Alice"), "@alice")
        self.assertEqual(Store.member_key("", "@Alice"), "@alice")
        self.assertEqual(Store.member_key(None, None), "@",
                         "degenerate key is filtered by save_cohort")

    def test_tweet_author_bio_fallback_cannot_destroy_rich_profile(self):
        """Verification for F1: thin tweet authors must not erase paid fields."""
        s = self.fresh_store("profile-preservation")
        self.addCleanup(s.close)
        rich_bio = "researcher building reliable social-data systems"
        s.put_accounts([{
            "id": "1", "userName": "alice", "description": rich_bio,
            "followers": 1234,
        }])
        s.put_tweets([{
            "id": "tweet", "text": "hello",
            "createdAt": "Mon Aug 10 17:16:53 +0000 2026",
            "author": {
                "id": "1", "userName": "alice", "description": "",
                "profile_bio": {"description": rich_bio},
            },
        }])
        row = s.db.execute(
            "SELECT description, followers FROM accounts WHERE id='1'").fetchone()
        self.assertEqual(row["description"], rich_bio)
        self.assertEqual(row["followers"], 1234)


class TestPaidGraphReadback(E2ETest):
    def test_follower_ids_for_ignores_crawl_cursor_and_page_size(self):
        """Paid follower pages must be readable without reproducing cache keys."""
        s = self.fresh_store("follower-readback")
        self.addCleanup(s.close)
        path = "/twitter/user/followers_ids"
        s.put_page(path, {"userName": "Alice", "count": 5000, "cursor": ""},
                   {"ids": ["1", "2"], "has_next_page": True,
                    "next_cursor": "next"}, credits=2250)
        s.put_page(path, {"userName": "Alice", "count": 5000,
                          "cursor": "next"},
                   {"ids": ["2", "3"], "has_next_page": False,
                    "next_cursor": ""}, credits=2250)
        s.put_page(path, {"userName": "alice", "count": 1000,
                          "cursor": "different-call"},
                   {"ids": ["3", "4"], "has_next_page": False,
                    "next_cursor": ""}, credits=450)
        s.put_page(path, {"userName": "bob", "count": 5000, "cursor": ""},
                   {"ids": ["not-alice"], "has_next_page": False,
                    "next_cursor": ""}, credits=2250)

        self.assertEqual(s.follower_ids_for("@ALICE"), ["1", "2", "3", "4"])


class TestConcurrentStoreReads(E2ETest):
    def test_cached_reads_are_safe_at_8_16_and_24_workers(self):
        """Regression: one shared sqlite connection must serialize every read."""
        s = self.fresh_store("concurrent-reads")
        self.addCleanup(s.close)
        path = "/twitter/user/followers_ids"
        params = {"userName": "alice", "count": 5000, "cursor": ""}
        page = {"ids": ["1", "2"], "has_next_page": False,
                "next_cursor": ""}
        s.put_page(path, params, page, credits=15)
        s.save_cohort("team", [("1", "alice", 1.0, "test")])

        def read_cached_state(_):
            for _ in range(50):
                self.assertEqual(s.get_page(path, params), page)
                self.assertIsNotNone(s.page_age(path, params))
                self.assertEqual(len(s.load_cohort("team")), 1)
                self.assertEqual(len(s.list_cohorts()), 1)
                self.assertEqual(s.cohort_versions("team"), [1])

        for workers in (8, 16, 24):
            with self.subTest(workers=workers):
                errors = []
                with concurrent.futures.ThreadPoolExecutor(
                        max_workers=workers) as pool:
                    futures = [pool.submit(read_cached_state, i)
                               for i in range(48)]
                    for future in futures:
                        try:
                            future.result()
                        except Exception as exc:
                            errors.append(exc)
                self.assertEqual(errors, [],
                                 f"{workers} workers raised {errors!r}")


class TestPublicCohortConstruction(E2ETest):
    def test_from_ids_builds_without_network_or_private_add(self):
        """Already-owned IDs must have a public, zero-request cohort path."""
        from cohort import Cohort
        client = object()
        s = self.fresh_store("from-ids")
        self.addCleanup(s.close)
        co = Cohort.from_ids(["1", 2, "1"], client=client, store=s,
                             label="paid followers")
        self.assertEqual(co.ids(), ["1", "2"])
        self.assertEqual(co.label, "paid followers")
        self.assertTrue(all(m["provenance"] == "provided_id" for m in co))

    def test_delete_cohort_removes_members_and_metadata_atomically(self):
        """Deleting a saved test cohort must not require orphan-prone raw SQL."""
        s = self.fresh_store("delete-cohort")
        self.addCleanup(s.close)
        s.save_cohort("test", [("1", "alice", 1.0, "test")])
        s.save_cohort("test", [("2", "bob", 1.0, "test")])
        s.save_cohort("keep", [("3", "carol", 1.0, "test")])

        self.assertEqual(s.delete_cohort("test"), 2)
        self.assertEqual(s.load_cohort("test"), [])
        self.assertEqual(s.cohort_versions("test"), [])
        self.assertEqual(s.cohort_versions("keep"), [1])

    def test_cohort_inherits_client_store_without_opening_default_cache(self):
        """Client store injection must not be lost at the Cohort boundary."""
        from cohort import Cohort
        s = self.fresh_store("inherited-store")
        self.addCleanup(s.close)
        client = SimpleNamespace(store=s)
        with mock.patch("cohort.Store",
                        side_effect=AssertionError("default Store() opened")):
            co = Cohort(client=client)
        self.assertIs(co.s, s)


class TestDocumentationContracts(E2ETest):
    @staticmethod
    def _read(name):
        return Path(SKILL, name).read_text()

    def test_skill_and_readme_present_one_library_story(self):
        """The skill must not forbid the library surface the README teaches."""
        skill = self._read("SKILL.md")
        self.assertNotIn("Run the workflows; do not hand-roll requests", skill)
        self.assertIn("library", skill.lower())

    def test_composition_example_preserves_the_injected_store(self):
        """Following the skill example must not silently open the home cache."""
        skill = self._read("SKILL.md")
        self.assertIn('Cohort.load("pm_talkers", client=c, store=s)', skill)
        self.assertIn('Cohort.load("crypto_ai", client=c, store=s)', skill)

    def test_readme_describes_only_paginated_endpoints(self):
        """Four single-object methods must not be advertised through paginate()."""
        readme = self._read("README.md")
        self.assertIn("one of the 27 paginated endpoints", readme)

    def test_readme_scopes_cached_tweets_and_two_pass_page_caps(self):
        """Cached authorship and per-pass caps must not imply broader coverage."""
        readme = self._read("README.md")
        self.assertIn("cached posts authored by these accounts", readme)
        self.assertRegex(readme, r"applies separately to each\s+search pass")

    def test_media_recovery_requirements_are_documented(self):
        """Users must be able to retrieve media bytes the skill leaves remote."""
        combined = self._read("README.md") + self._read("SKILL.md")
        self.assertIn("extendedEntities.media", combined)
        self.assertIn("media_url_https", combined)
        self.assertIn("format=jpg&name=4096x4096", combined)
        self.assertIn("User-Agent", combined)
        self.assertIn("pbs.twimg.com", combined)


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

    def test_drift_default_is_previous_step_not_whole_history(self):
        """Three versions, because with only two, first-ever and previous
        coincide — which is exactly how the versions[0] default survived the
        original two-version test while a 30-day watch would have reported a
        month of churn as one day's."""
        from twitterapi import Client
        from jobs import cohort_drift
        s = self.fresh_store("drift3")
        c = Client(verbose=False, store=s)
        s.save_cohort("w", [("1", "a", 1.0, "t")])
        s.save_cohort("w", [("1", "a", 1.0, "t"), ("2", "b", 1.0, "t")])
        s.save_cohort("w", [("2", "b", 1.0, "t"), ("3", "x", 1.0, "t")])
        out = cohort_drift("w", client=c)
        self.assertEqual((out["from_version"], out["to_version"]), (2, 3),
                         "default drift must diff the previous version, "
                         "not the first ever saved")
        self.assertEqual(out["joined"], ["x"],
                         "v1->v3 leakage: 'b' joined two versions ago and "
                         "must not appear in the default (v2->v3) diff")
        with self.assertRaises(ValueError, msg="an absent version loads as an "
                               "EMPTY cohort, so without validation a typo "
                               "reports every member as newly joined"):
            cohort_drift("w", v_old=1, v_new=99, client=c)


class TestFollowerIdsCrawlGenerations(E2ETest):
    PATH = "/twitter/user/followers_ids"

    def test_replacing_a_version_with_fewer_members_leaves_no_ghosts(self):
        s = self.fresh_store("ghosts")
        s.save_cohort("g", [("1", "a", 1.0, "t"), ("2", "b", 1.0, "t")])
        s.save_cohort("g", [("9", "z", 1.0, "t")], version=1)   # shrink 2 -> 1
        rows = s.load_cohort("g", version=1)
        self.assertEqual([m["user_name"] for m in rows], ["z"],
                         "INSERT OR REPLACE only touches supplied keys; the "
                         "omitted member must be deleted, not left as a ghost "
                         "that metadata denies but every load returns")
        self.assertEqual(s.stats()["cohort_members"], 1)

    def test_recrawl_replaces_rather_than_blends(self):
        """A crawl starts at a cursorless page; a re-crawl REPLACES that page
        (same cache key) but its continuation cursors differ, so the old
        crawl's cursor pages survive. Merging every page then reports people
        who unfollowed between crawls as current followers — observed on a
        real store as 39 pages spanning two crawls 3 hours apart."""
        s = self.fresh_store("gens")
        s.put_page(self.PATH, {"userName": "esk"}, {"ids": ["111", "222"]})
        s.put_page(self.PATH, {"userName": "esk", "cursor": "c1"},
                   {"ids": ["333", "555"]})          # 555 unfollows next
        time.sleep(0.02)                             # distinct fetched_at
        s.put_page(self.PATH, {"userName": "esk"}, {"ids": ["222", "444"]})
        s.put_page(self.PATH, {"userName": "esk", "cursor": "c9"},
                   {"ids": ["333"]})
        self.assertEqual(sorted(s.follower_ids_for("esk")),
                         ["222", "333", "444"],
                         "555 left before the second crawl and must not be "
                         "reported; 111 was replaced out of the start page")

    def test_cursor_only_cache_falls_back_to_all_pages(self):
        """No cursorless start page cached: cannot segment generations, so
        return everything rather than invent an empty answer for paid data."""
        s = self.fresh_store("gens2")
        s.put_page(self.PATH, {"userName": "esk", "cursor": "c1"},
                   {"ids": ["1"]})
        self.assertEqual(s.follower_ids_for("esk"), ["1"])


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
