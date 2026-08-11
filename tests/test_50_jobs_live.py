"""Tier 5 — live analytical jobs on small/bounded targets. ~$0.035.

  - Self-overlap: jobs.overlap(kaitoinfra, kaitoinfra) must be an identity —
    jaccard exactly 1.0, overlap == a_followers == b_followers — and the
    second follower crawl must be served entirely from the shared Store
    (exactly ONE billed follower page, one cache hit).
  - Diffusion trace on tweet 2084352161404920316 (real OpenAI tweet, 584
    replies / 373 quotes / 1008 retweets at probe time): earliest_movers may
    contain ONLY replies and quotes (retweeters have no retweet timestamp —
    ordering them would fabricate an 'earliest mover'), mover timestamps are
    non-decreasing, and retweeters are a separate count off the timeline.
"""
import unittest
from datetime import datetime

from tests.e2e_base import E2ETest, record_charges, require_key

TW_FMT = "%a %b %d %H:%M:%S %z %Y"


class TestSelfOverlap(E2ETest):
    def test_overlap_with_self_is_identity_and_second_leg_is_cached(self):
        require_key()
        from twitterapi import Client
        from jobs import overlap
        store = self.fresh_store("self_overlap")
        c = self.track(Client(verbose=False, store=store, max_usd=1.0))
        events = record_charges(c)
        self.repro("python3 jobs.py overlap kaitoinfra kaitoinfra --max-usd 1")

        out = overlap("kaitoinfra", "kaitoinfra", max_usd=1.0, client=c)
        self.log(f"result: a={out['a_followers']} b={out['b_followers']} "
                 f"overlap={out['overlap']} jaccard={out['jaccard']}")
        self.log(f"charge events: {events}")
        self.log(f"cache_hits={c.cache_hits} saved={c.saved_credits} "
                 f"requests={c.requests_made} spent={c.spent_credits}")
        self.dump_store(store)

        self.assertGreater(out["a_followers"], 0,
                           "kaitoinfra returned no followers — suspended/renamed? "
                           "pick another small account (TRIAGE: boundary drift)")
        self.assertEqual(out["jaccard"], 1.0, "self-overlap must be exactly 1.0")
        self.assertEqual(out["overlap"], out["a_followers"])
        self.assertEqual(out["a_followers"], out["b_followers"])
        self.assertIsNone(out["_warning"])

        follower_pages = [e for e in events if e[0] == "follower_ids"]
        self.assertEqual(len(follower_pages), 1,
                         f"second leg must come from cache; billed pages: {events}")
        self.assertGreaterEqual(c.cache_hits, 1, "the cached page must register a hit")
        # accounting identity: 2 user_info lookups (18cr) + one small ids page
        # (10-ish records * 0.45cr, floored at the 15cr request minimum)
        from twitterapi import Client as C
        expected = 2 * 18 + C.page_credits("follower_ids", out["a_followers"], 5000)
        self.assertEqual(c.spent_credits, expected,
                         f"spent {c.spent_credits} != predicted {expected} "
                         f"(events={events})")
        self.assertEqual(c.saved_credits,
                         C.page_credits("follower_ids", out["a_followers"], 5000),
                         "saved_credits must equal the price of the cached page")


class TestDiffusionTrace(E2ETest):
    TWEET = "2084352161404920316"

    def test_diffusion_movers_are_only_replies_and_quotes_time_ordered(self):
        require_key()
        from twitterapi import Client
        from jobs import diffusion_trace
        c = self.track(Client(verbose=False, max_usd=0.06,
                              store=self.fresh_store("diffusion")))
        self.repro(f"jobs.diffusion_trace('{self.TWEET}', limit=60)")
        out = diffusion_trace(self.TWEET, limit=60, client=c)
        self.log(f"timed={out['timed_engagers']} retweeters={out['retweeter_count']} "
                 f"by_kind={out['by_kind']} movers={len(out['earliest_movers'])}")
        self.log(f"requests={c.requests_made} spent={c.spent_credits}")

        movers = out["earliest_movers"]
        self.assertGreater(len(movers), 0, "a tweet with hundreds of replies "
                                           "must produce timed movers")
        kinds = {m["kind"] for m in movers}
        self.assertLessEqual(kinds, {"reply", "quote"},
                             f"retweets must NEVER appear on the timeline "
                             f"(no retweet timestamp exists); got kinds={kinds}")
        for m in movers:
            self.assertNotIn("_ts", m, "internal sort key must not leak")
        stamps = [datetime.strptime(m["createdAt"], TW_FMT) for m in movers]
        self.assertEqual(stamps, sorted(stamps),
                         "earliest_movers must be in non-decreasing time order")

        self.assertGreater(out["retweeter_count"], 0,
                           "the probe tweet had 1000+ retweets; zero means the "
                           "retweeters crawl broke")
        self.assertEqual(out["by_kind"]["retweet"], out["retweeter_count"],
                         "retweeters are counted separately, never timed")
        self.assertEqual(out["timed_engagers"],
                         out["by_kind"]["reply"] + out["by_kind"]["quote"])
        self.assertLessEqual(len(movers), 25, "movers list is capped at 25")


if __name__ == "__main__":
    unittest.main()
