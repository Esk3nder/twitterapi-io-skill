"""Tier 6 — real pagination, cost accounting and the read-through cache.
The most expensive tier (~$0.055), so it runs last.

  - follower_ids('jack', limit=10000): exactly 10,000 unique ids across
    exactly 2 billed pages of 5,000; spent_credits equals both the pure-math
    prediction (2 x 2,250) and the per-page sum persisted in the store.
  - Cache determinism: an identical second crawl through the SAME Store makes
    0 API requests, spends $0.0000, and yields the IDENTICAL id sequence.
  - last_tweets('jack', limit=45): exactly 45 tweets across 3 pages — proves
    the data.tweets-with-ROOT-pagination trap is handled (mis-reading
    pagination from inside `data` would stop at 20 and look successful).
"""
import sqlite3
import unittest

from tests.e2e_base import E2ETest, record_charges, require_key


class TestFollowerCrawlAndCache(E2ETest):
    def test_50_profile_estimate_is_within_five_percent_of_actual(self):
        """A real tiered-price crawl guards estimate() against flat-rate drift."""
        require_key()
        from twitterapi import Client

        page_size = 20
        limit = 50
        c = self.track(Client(verbose=False, max_usd=0.01,
                              store=self.fresh_store("estimate_accuracy")))
        predicted = c.estimate("followers", limit, page_size=page_size)
        self.repro(
            "list(Client().paginate('followers', 'jack', limit=50, page_size=20))")

        profiles = list(c.paginate(
            "followers", "jack", limit=limit, page_size=page_size))
        actual = c.spent_usd
        relative_error = abs(predicted - actual) / actual
        self.log(f"page_size={page_size} predicted=${predicted:.5f} "
                 f"actual=${actual:.5f} error={relative_error:.2%}")

        self.assertEqual(len(profiles), limit)
        self.assertLessEqual(
            relative_error, 0.05,
            f"estimate error {relative_error:.2%} exceeds stated 5% tolerance")

    def test_10k_ids_two_pages_accounting_then_cache_replay(self):
        require_key()
        from twitterapi import Client, ENDPOINTS
        db_path = self.store_path("jack_crawl")
        store = self.fresh_store("jack_crawl")

        # ---- crawl 1: real API ------------------------------------------
        c1 = self.track(Client(verbose=False, store=store, max_usd=0.10))
        ev1 = record_charges(c1)
        self.repro("python3 workflows.py --max-usd 0.1 audience jack --limit 10000")
        ids1 = list(c1.follower_ids("jack", limit=10000))
        self.log(f"crawl1: {len(ids1)} ids, events={ev1}, "
                 f"requests={c1.requests_made} (incl. 1 QPS/balance probe), "
                 f"spent={c1.spent_credits}")
        self.dump_store(store)

        self.assertEqual(len(ids1), 10000, "must yield exactly the requested count")
        self.assertEqual(len(set(ids1)), 10000, "every id must be unique")
        self.assertEqual(ev1, [("follower_ids", 5000, 5000)] * 2,
                         "exactly 2 full pages of 5000 — no more, no fewer")
        # cost identity, three independent ways:
        predicted = sum(Client.page_credits(n, r, p) for n, r, p in ev1)
        self.assertEqual(c1.spent_credits, predicted,
                         "spent_credits must equal the page_credits sum for "
                         "the pages actually fetched")
        self.assertEqual(c1.spent_credits, 4500.0,
                         "2 pages x 5000 ids x 0.45cr = 4500 (the measured table)")
        path = ENDPOINTS["follower_ids"]["path"]
        rows = sqlite3.connect(db_path).execute(
            "SELECT COUNT(*), COALESCE(SUM(credits),0) FROM pages WHERE path=?",
            (path,)).fetchone()
        self.log(f"store: {rows[0]} cached pages, {rows[1]} credits recorded")
        self.assertEqual(rows[0], 2, "both pages must be cached")
        self.assertEqual(rows[1], 4500.0, "store must record what each page cost")

        # ---- crawl 2: must be a pure cache replay ------------------------
        c2 = self.track(Client(verbose=False, store=store, max_usd=0.10))
        ev2 = record_charges(c2)
        ids2 = list(c2.follower_ids("jack", limit=10000))
        self.log(f"crawl2: {len(ids2)} ids, requests={c2.requests_made}, "
                 f"spent={c2.spent_credits}, cache_hits={c2.cache_hits}, "
                 f"saved={c2.saved_credits}")

        self.assertEqual(c2.requests_made, 0,
                         "an identical cached crawl must make ZERO API requests")
        self.assertEqual(c2.spent_credits, 0.0,
                         "an identical cached crawl must spend $0.0000")
        self.assertEqual(ev2, [], "no page may be billed on the cached crawl")
        self.assertEqual(c2.cache_hits, 2)
        self.assertEqual(c2.saved_credits, 4500.0,
                         "saved_credits must mirror what crawl 1 paid")
        self.assertEqual(ids1, ids2,
                         "cached replay must yield the IDENTICAL id sequence")


class TestLiveEndpointRegressions(E2ETest):
    def test_articles_returns_all_13_paid_rows_with_honest_completeness(self):
        """F24 live: @trq212 has articles, unlike the old zero-row probe."""
        require_key()
        from twitterapi import Client

        c = self.track(Client(verbose=False, max_usd=0.02))
        self.repro("list(Client().paginate('articles', 'trq212'))")
        stream = c.paginate("articles", "trq212")
        articles = list(stream)
        self.log(f"articles={len(articles)} complete={stream.complete} "
                 f"warning={stream._warning!r} requests={c.requests_made} "
                 f"spent={c.spent_credits}")

        self.assertEqual(len(articles), 13)
        if not stream.complete:
            self.assertIn("partial", stream._warning.lower())

    def test_tweet_timeline_handle_matches_numeric_id(self):
        """F25 live: the public handle contract returns the numeric-ID timeline."""
        require_key()
        from twitterapi import Client

        handle_client = self.track(Client(verbose=False, max_usd=0.01))
        id_client = self.track(Client(verbose=False, max_usd=0.01))
        handle_rows = list(handle_client.paginate(
            "tweet_timeline", "lydiahallie", limit=20))
        id_rows = list(id_client.paginate(
            "tweet_timeline", "879696238953865217", limit=20))
        handle_ids = [str(tweet.get("id")) for tweet in handle_rows]
        numeric_ids = [str(tweet.get("id")) for tweet in id_rows]
        self.log(f"handle={len(handle_ids)} numeric={len(numeric_ids)} "
                 f"handle_requests={handle_client.requests_made} "
                 f"numeric_requests={id_client.requests_made}")

        self.assertEqual(len(handle_ids), 20)
        self.assertEqual(len(numeric_ids), 20)
        self.assertGreaterEqual(
            len(set(handle_ids) & set(numeric_ids)), 19,
            "sequential live calls may straddle one new tweet, but must resolve "
            "to the same account timeline",
        )


class TestLastTweetsPaginationTrap(E2ETest):
    def test_45_tweets_cross_three_pages(self):
        require_key()
        from twitterapi import Client
        c = self.track(Client(verbose=False, max_usd=0.02,
                              store=self.fresh_store("last_tweets")))
        events = record_charges(c)
        self.repro("Client().last_tweets('jack', limit=45)")
        tweets = list(c.last_tweets("jack", limit=45))
        ids = [t["id"] for t in tweets]
        self.log(f"{len(ids)} tweets, events={events}, requests={c.requests_made}, "
                 f"spent={c.spent_credits}")

        self.assertEqual(len(ids), 45,
                         "exactly 45 tweets: fewer (esp. exactly 20) means "
                         "pagination was read from inside `data` — the trap")
        self.assertEqual(len(set(ids)), 45, "no duplicate tweets across pages")
        page_names = {e[0] for e in events}
        self.assertEqual(page_names, {"last_tweets"})
        self.assertEqual(len(events), 3,
                         f"45 tweets at 20/page must take exactly 3 pages, "
                         f"got {len(events)}: {events}")
        # accounting identity for variable-size pages
        predicted = sum(Client.page_credits(n, r, p) for n, r, p in events)
        self.assertEqual(c.spent_credits, predicted,
                         "spent_credits must equal the per-page prediction")


if __name__ == "__main__":
    unittest.main()
