"""Tier 0 — store.py defects confirmed by the 2026-10-23 adversarial review.

Zero network, $0. Every test here is a NEGATIVE test: it failed against the
pre-fix store and passes after. The shared root cause is ABSENCE read as a
confident zero / false / empty — a missing field must stay unknown (NULL), and
a present field is authoritative even when it is 0, False, or "".
"""
import concurrent.futures
import json
import os
import sqlite3
import threading
import unittest

from tests.e2e_base import E2ETest

PHOTO = {"type": "photo",
         "media_url_https": "https://pbs.twimg.com/media/X.jpg"}


def _tweet(**fields):
    base = {"id": "9", "author": {"id": "1", "userName": "a"}}
    base.update(fields)
    return base


class TestCohortWeightZero(E2ETest):
    def test_weight_zero_is_stored_as_zero_not_one(self):
        """`float(w or 1.0)` turned an authority weight of 0 into 1.0, so an
        account nobody in the cohort follows outranked real authorities."""
        s = self.fresh_store("r07-weight-zero")
        self.addCleanup(s.close)
        s.save_cohort("x", [("1", "a", 0.0, "authority_indegree"),
                            ("2", "b", 3.0, "authority_indegree"),
                            ("3", "c", None, "provided_id")])
        got = {r["account_id"]: r["weight"] for r in s.load_cohort("x")}
        self.assertEqual(got["1"], 0.0, "a real weight of 0 must survive")
        self.assertEqual(got["2"], 3.0)
        self.assertEqual(got["3"], 1.0, "only an ABSENT weight defaults to 1.0")


class TestCohortVersionRaceAcrossConnections(E2ETest):
    def test_two_connections_never_share_a_version(self):
        """Two processes saving one name both read MAX(version)+1 before either
        wrote, so both wrote version 1 and the second save deleted the first's
        members. `_wlock` is a thread lock and cannot see another connection.

        Connection A is paused right after it reads the next version. B must
        then WAIT (the version allocation holds the database write lock), not
        complete with the same version."""
        from store import Store
        path = self.store_path("r07-version-race")
        a, b = Store(path), Store(path)
        self.addCleanup(a.close)
        self.addCleanup(b.close)
        paused, release = threading.Event(), threading.Event()

        class PauseAfterVersionRead:
            def __init__(self, real):
                self.real = real

            def execute(self, sql, *args):
                cur = self.real.execute(sql, *args)
                if "MAX(version)" in sql:
                    row = cur.fetchone()
                    paused.set()
                    release.wait(10)

                    class Done:
                        def fetchone(self):
                            return row
                    return Done()
                return cur

            def __getattr__(self, name):
                return getattr(self.real, name)

        a.db = PauseAfterVersionRead(a.db)
        result = {}
        ta = threading.Thread(target=lambda: result.setdefault(
            "A", a.save_cohort("w", [("1", "a", 1, "p"), ("2", "b", 1, "p")])))
        tb = threading.Thread(target=lambda: result.setdefault(
            "B", b.save_cohort("w", [("3", "c", 1, "p")])))
        ta.start()
        self.assertTrue(paused.wait(10), "A never reached the version read")
        tb.start()
        tb.join(0.5)
        b_finished_while_a_held_version = not tb.is_alive()
        release.set()
        ta.join(30)
        tb.join(30)

        self.assertFalse(b_finished_while_a_held_version,
                         "B allocated a version while A's allocation was open")
        self.assertEqual(sorted(result.values()), [1, 2],
                         f"both savers must get distinct versions: {result}")
        members = {v: sorted(r["account_id"] for r in b.load_cohort("w", v))
                   for v in (1, 2)}
        self.assertEqual(members[result["A"]], ["1", "2"])
        self.assertEqual(members[result["B"]], ["3"])


class TestTweetAbsenceIsUnknown(E2ETest):
    def test_normalize_tweet_absent_counts_are_none_present_zero_is_zero(self):
        from store import normalize_tweet
        thin = normalize_tweet({"id": "1"})
        for k in ("likes", "retweets", "replies", "quotes", "views",
                  "created_ts", "is_reply", "is_quote", "is_retweet", "media"):
            self.assertIsNone(thin[k], f"absent source field -> {k} must be None")
        zero = normalize_tweet({"id": "1", "likeCount": 0, "viewCount": 0,
                                "isReply": False, "quoted_tweet": None,
                                "extendedEntities": {}})
        self.assertEqual((zero["likes"], zero["views"]), (0, 0))
        self.assertEqual((zero["is_reply"], zero["is_quote"]), (0, 0))
        self.assertEqual(zero["media"], [], "media keys present + empty = none")

    def test_thin_refetch_keeps_counts_media_and_raw_fields(self):
        """A refetch lacking viewCount/media used to INSERT OR REPLACE the row:
        views 12345 -> 0, one photo -> [], bookmarkCount gone from raw."""
        s = self.fresh_store("r07-thin-refetch")
        self.addCleanup(s.close)
        s.put_tweets([_tweet(viewCount=12345, likeCount=7, bookmarkCount=4,
                             createdAt="Mon Aug 10 17:16:53 +0000 2026",
                             isReply=True,
                             extendedEntities={"media": [PHOTO]})])
        # Thin record: likes present (authoritative), everything else absent.
        # `media: []` alone is what twitterapi.enrich_tweet_media attaches to
        # every record, so it is derived, not a source assertion.
        s.put_tweets([_tweet(likeCount=8, media=[])])
        row = s.tweets_for(account_ids=["1"])[0]
        self.assertEqual(row["likes"], 8, "a present count must win")
        self.assertEqual(row["views"], 12345, "absent viewCount must not zero")
        self.assertEqual(row["is_reply"], 1, "absent isReply must not flip")
        self.assertGreater(row["created_ts"], 0, "absent createdAt must not zero")
        self.assertEqual([m["type"] for m in row["media"]], ["photo"])
        raw = json.loads(row["raw"])
        self.assertEqual(raw["bookmarkCount"], 4,
                         "raw-only fields must survive a thin refetch")
        self.assertEqual(raw["likeCount"], 8)

    def test_present_zero_and_present_empty_media_do_overwrite(self):
        s = self.fresh_store("r07-present-wins")
        self.addCleanup(s.close)
        s.put_tweets([_tweet(viewCount=12345, extendedEntities={"media": [PHOTO]})])
        s.put_tweets([_tweet(viewCount=0, extendedEntities={})])
        row = s.tweets_for(account_ids=["1"])[0]
        self.assertEqual(row["views"], 0, "a present 0 is authoritative")
        self.assertEqual(row["media"], [], "media keys present + empty is real")

    def test_first_seen_thin_tweet_reads_back_unknown(self):
        s = self.fresh_store("r07-first-thin")
        self.addCleanup(s.close)
        s.put_tweets([_tweet()])
        row = s.tweets_for(account_ids=["1"])[0]
        self.assertIsNone(row["views"])
        self.assertIsNone(row["media"], "no media keys -> unknown, not []")


class TestTweetsMediaMigration(E2ETest):
    OLD_TWEETS = """CREATE TABLE tweets (
      id TEXT PRIMARY KEY, author_id TEXT, author_name TEXT,
      created_ts INTEGER, text TEXT, lang TEXT,
      likes INTEGER, retweets INTEGER, replies INTEGER, quotes INTEGER,
      views INTEGER, is_reply INTEGER, is_quote INTEGER, is_retweet INTEGER,
      conversation_id TEXT, in_reply_to TEXT, raw TEXT, seen_at REAL)"""

    def _old_db(self, name, with_media):
        path = self.store_path(name)
        db = sqlite3.connect(path)
        db.execute(self.OLD_TWEETS)
        if with_media:
            # Exactly how the production store got its column: appended by
            # ALTER, so it sits AFTER raw/seen_at, NOT NULL DEFAULT '[]'.
            db.execute("ALTER TABLE tweets ADD COLUMN media TEXT NOT NULL "
                       "DEFAULT '[]'")
        raw_media = json.dumps({"id": "m", "extendedEntities": {"media": [PHOTO]}})
        raw_none = json.dumps({"id": "n", "extendedEntities": {}})
        raw_thin = json.dumps({"id": "t"})
        rows = [("m", "1", "a", 100, "x", "en", 1, 2, 3, 4, 5, 0, 0, 0, "c", "",
                 raw_media, 1.5),
                ("n", "1", "a", 200, "y", "en", 0, 0, 0, 0, 0, 0, 0, 0, "c", "",
                 raw_none, 2.5),
                ("t", "1", "a", 300, "z", "en", 0, 0, 0, 0, 0, 0, 0, 0, "c", "",
                 raw_thin, 3.5)]
        db.executemany("INSERT INTO tweets (id, author_id, author_name, "
                       "created_ts, text, lang, likes, retweets, replies, "
                       "quotes, views, is_reply, is_quote, is_retweet, "
                       "conversation_id, in_reply_to, raw, seen_at) VALUES "
                       "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
        if with_media:
            db.execute("UPDATE tweets SET media=? WHERE id='m'",
                       (json.dumps([{"type": "photo"}]),))
        db.commit()
        before = {r[0]: r for r in db.execute("SELECT * FROM tweets")}
        db.close()
        return path, before

    def _media_col(self, s):
        return next(r for r in s.db.execute("PRAGMA table_info(tweets)")
                    if r["name"] == "media")

    def test_not_null_media_column_is_relaxed_preserving_every_row(self):
        from store import Store
        path, before = self._old_db("r07-migrate-notnull", with_media=True)
        s = Store(path)
        self.addCleanup(s.close)
        self.assertEqual(self._media_col(s)["notnull"], 0,
                         "media must be nullable so unknown can be stored")
        cols = [r["name"] for r in s.db.execute("PRAGMA table_info(tweets)")]
        after = {r["id"]: r for r in s.db.execute("SELECT * FROM tweets")}
        old_cols = ["id", "author_id", "author_name", "created_ts", "text",
                    "lang", "likes", "retweets", "replies", "quotes", "views",
                    "is_reply", "is_quote", "is_retweet", "conversation_id",
                    "in_reply_to", "raw", "seen_at", "media"]
        for tid, old in before.items():
            self.assertEqual({c: after[tid][c] for c in old_cols},
                             dict(zip(old_cols, old)),
                             f"row {tid} changed during migration")
        self.assertEqual(sorted(cols), sorted(old_cols))
        indexes = {r["name"] for r in s.db.execute(
            "SELECT name FROM sqlite_master WHERE type='index' "
            "AND tbl_name='tweets'")}
        self.assertTrue({"idx_tweets_author", "idx_tweets_ts"} <= indexes)
        # Idempotent: a second open is a no-op on the migrated table.
        s2 = Store(path)
        self.addCleanup(s2.close)
        self.assertEqual(s2.stats()["tweets"], 3)
        # And the store is writable with the new upsert afterwards.
        s2.put_tweets([{"id": "t", "author": {"id": "1"}, "likeCount": 9}])
        self.assertEqual(s2.tweets_for(account_ids=["1"])[0]["likes"], 9)

    def test_missing_media_column_recovers_media_from_raw_or_leaves_unknown(self):
        from store import Store
        path, _ = self._old_db("r07-migrate-add", with_media=False)
        s = Store(path)
        self.addCleanup(s.close)
        self.assertEqual(self._media_col(s)["notnull"], 0)
        got = {r["id"]: r for r in s.tweets_for(account_ids=["1"])}
        self.assertEqual([m["type"] for m in got["m"]["media"]], ["photo"])
        self.assertEqual(got["n"]["media"], [],
                         "raw asserted no media (extendedEntities: {})")
        self.assertIsNone(got["t"]["media"], "raw carried no media keys")


class TestAccountUpsertPresentValueWins(E2ETest):
    def test_lapsed_blue_and_cleared_bio_land(self):
        """MAX() made a lapsed subscription unwritable; NULLIF made a real
        empty bio unwritable."""
        s = self.fresh_store("r07-account-lapse")
        self.addCleanup(s.close)
        s.put_accounts([{"id": "1", "userName": "a", "isBlueVerified": True,
                         "isVerified": True, "description": "old bio"}])
        s.put_accounts([{"id": "1", "userName": "a", "isBlueVerified": False,
                         "isVerified": False, "description": ""}])
        row = s.db.execute("SELECT is_blue, is_verified, description "
                           "FROM accounts WHERE id='1'").fetchone()
        self.assertEqual(tuple(row), (0, 0, ""))

    def test_absent_fields_keep_the_stored_value(self):
        s = self.fresh_store("r07-account-absent")
        self.addCleanup(s.close)
        s.put_accounts([{"id": "1", "userName": "a", "isBlueVerified": True,
                         "description": "bio"}])
        s.put_accounts([{"id": "1", "userName": "a"}])
        row = s.db.execute("SELECT is_blue, description FROM accounts "
                           "WHERE id='1'").fetchone()
        self.assertEqual(tuple(row), (1, "bio"))

    def test_hollow_top_level_bio_defers_to_nested_profile_bio(self):
        """Author-shaped user records (description "" + profile_bio) reach
        put_accounts from endpoints other than tweets. The "" there is hollow,
        so it must not become an authoritative empty bio."""
        from store import normalize_account
        n = normalize_account({"id": "1", "description": "",
                               "profile_bio": {"description": "real bio"}})
        self.assertEqual(n["description"], "real bio")
        s = self.fresh_store("r07-hollow-bio")
        self.addCleanup(s.close)
        s.put_accounts([{"id": "1", "userName": "a", "description": "real bio"}])
        s.put_tweets([{"id": "t", "author": {
            "id": "1", "userName": "a", "description": "",
            "profile_bio": {"description": ""}}}])
        row = s.db.execute("SELECT description FROM accounts WHERE id='1'"
                           ).fetchone()
        self.assertEqual(row["description"], "real bio",
                         "a tweet author's empty bio fields are not evidence")


class TestEmptyFilterMeansNothing(E2ETest):
    def test_empty_id_or_name_list_returns_no_tweets(self):
        s = self.fresh_store("r07-empty-filter")
        self.addCleanup(s.close)
        s.put_tweets([_tweet(id="1"), _tweet(id="2", author={"id": "2",
                                                            "userName": "b"})])
        self.assertEqual(s.tweets_for(account_ids=[]), [],
                         "an empty cohort has no tweets, not the whole corpus")
        self.assertEqual(s.tweets_for(user_names=[]), [])
        self.assertEqual(len(s.tweets_for()), 2, "no filter still means all")


class TestLoadNeverSavedCohort(E2ETest):
    def test_saved_empty_cohort_still_loads_empty(self):
        s = self.fresh_store("r07-load-empty")
        self.addCleanup(s.close)
        s.save_cohort("empty", [])
        self.assertEqual(s.load_cohort("empty"), [])


class TestConcurrentUnlockedReads(E2ETest):
    def test_tweets_for_follower_ids_and_stats_are_safe_across_threads(self):
        """These readers bypassed _wlock on the shared connection. Measured
        before the fix at 16 threads: follower_ids_for silently returned 0 ids
        (plus a false PARTIAL warning), TypeError and InterfaceError."""
        s = self.fresh_store("r07-unlocked-reads")
        self.addCleanup(s.close)
        s.put_tweets([{"id": str(i), "author": {"id": "1", "userName": "a"},
                       "likeCount": i} for i in range(300)])
        s.put_page("/twitter/user/followers_ids",
                   {"userName": "a", "cursor": ""},
                   {"ids": [str(i) for i in range(2000)],
                    "has_next_page": False})

        def work(i):
            for _ in range(30):
                if i % 3 == 0:
                    assert len(s.tweets_for(account_ids=["1"])) == 300
                elif i % 3 == 1:
                    ids = s.follower_ids_for("a")
                    assert len(ids) == 2000 and ids.complete
                else:
                    assert s.stats()["tweets"] == 300

        errors = []
        with concurrent.futures.ThreadPoolExecutor(16) as pool:
            for fut in [pool.submit(work, i) for i in range(48)]:
                try:
                    fut.result()
                except Exception as exc:     # noqa: BLE001 — collect all
                    errors.append(f"{type(exc).__name__}: {exc}")
        self.assertEqual(errors, [])


if __name__ == "__main__":
    unittest.main()


class TestFollowerIdsNothingCached(unittest.TestCase):
    """overlap probes the cache on every first run; an empty cache printed a
    false PARTIAL warning with a reason about crawl generations."""

    def test_empty_cache_is_unknown_and_silent(self):
        import contextlib, io, os, tempfile
        from store import Store
        s = Store(os.path.join(tempfile.mkdtemp(prefix="fids-"), "s.db"))
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            got = s.follower_ids_for("nobody")
        self.assertEqual(list(got), [])
        self.assertFalse(got.complete)
        self.assertIn("no follower_ids pages are cached", got.reason)
        self.assertEqual(err.getvalue(), "")


class TestPutTweetsEmptyBatch(unittest.TestCase):
    """Found by the diff review: `rows` was bound only inside `if parsed:`,
    so an empty batch (jobs.corpus stores every chunk, including one for an
    account with no posts in the window) raised UnboundLocalError and turned
    `brief` on a quiet account into an internal error."""

    def test_empty_and_idless_batches_store_nothing(self):
        import tempfile
        from store import Store
        s = Store(os.path.join(tempfile.mkdtemp(prefix="empty-"), "s.db"))
        self.assertEqual(s.put_tweets([]), 0)
        self.assertEqual(s.put_tweets([{"text": "no id"}]), 0)


class TestHandleCaseInCacheKey(unittest.TestCase):
    def test_handle_casing_shares_one_cache_entry(self):
        import tempfile
        from store import Store
        s = Store(os.path.join(tempfile.mkdtemp(prefix="case-"), "s.db"))
        self.addCleanup(s.close)
        s.put_page("/twitter/user/followers_ids",
                   {"userName": "stripe", "cursor": ""}, {"ids": ["1"]})
        self.assertEqual(
            s.get_page("/twitter/user/followers_ids",
                       {"userName": "@Stripe", "cursor": ""}),
            {"ids": ["1"]})
        # A non-handle param keeps its case: search queries are not handles.
        s.put_page("/twitter/tweet/advanced_search", {"query": "AI"}, {"n": 1})
        self.assertIsNone(s.get_page("/twitter/tweet/advanced_search",
                                     {"query": "ai"}))

    def test_migration_rekeys_mixed_case_pages_and_keeps_newer(self):
        """Stores written before the fold hold paid pages under mixed-case
        keys (210 followings pages on the production store). Opening the
        store must make them reachable, not strand them as cache misses."""
        import tempfile
        from store import Store
        db = os.path.join(tempfile.mkdtemp(prefix="rekey-"), "s.db")
        Store(db).close()
        raw = sqlite3.connect(db)
        path = "/twitter/user/followings"

        def old_key(params):
            return path + "?" + json.dumps(
                {k: str(v) for k, v in params.items()}, sort_keys=True)
        rows = [({"userName": "Syynya", "cursor": ""}, {"v": "old"}, 100.0),
                ({"userName": "SYYNYA", "cursor": ""}, {"v": "new"}, 200.0),
                ({"userName": "Syynya", "cursor": "c1"}, {"v": "p2"}, 150.0)]
        for params, body, at in rows:
            raw.execute("INSERT INTO pages VALUES (?,?,?,?,?,?)",
                        (old_key(params), path,
                         json.dumps(params, sort_keys=True),
                         json.dumps(body), at, 0))
        raw.commit()
        raw.close()
        s = Store(db)
        self.addCleanup(s.close)
        self.assertEqual(s.get_page(path, {"userName": "syynya", "cursor": ""}),
                         {"v": "new"}, "the newer casing wins a collision")
        self.assertEqual(s.get_page(path, {"userName": "syynya", "cursor": "c1"}),
                         {"v": "p2"})
        self.assertEqual(
            s.db.execute("SELECT COUNT(*) FROM pages").fetchone()[0], 2)
