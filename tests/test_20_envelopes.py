"""Tier 2 — live envelope contracts. ~$0.01.

Live content changes; envelope KEY SETS do not (they were live-verified
2026-08-10 and recorded in references/verified-facts.md). Each test asserts
the exact top-level key set of a real response. A failure here most likely
means the vendor changed a contract: re-probe, then update verified-facts.md
AND the ENDPOINTS table together (see TRIAGE.md).

Small account (kaitoinfra, ~10 followers) everywhere — envelope shape does
not depend on account size, so there is no reason to pay for a big one.
"""
import unittest

from tests.e2e_base import E2ETest, require_key

# The curated contract list: path -> expected top-level key set, straight
# from references/verified-facts.md ("Response envelopes" section).
EXPECTED = {
    "user_info":     {"data", "msg", "status"},
    "follower_ids":  {"code", "ids", "has_next_page", "next_cursor", "msg", "status"},
    "followers":     {"code", "followers", "has_next_page", "next_cursor", "msg", "status"},
    "advanced_search": {"tweets", "has_next_page", "next_cursor"},
    "last_tweets":   {"code", "data", "has_next_page", "next_cursor", "msg", "status"},
    "tweets_by_ids": {"code", "tweets", "msg", "status"},
    "get_rules":     {"rules", "msg", "status"},
}


class TestEnvelopes(E2ETest):
    def setUp(self):
        super().setUp()
        require_key()
        from twitterapi import Client
        self.c = self.track(Client(verbose=False))

    def _assert_keys(self, label, resp, want):
        got = set(resp.keys())
        self.dump_resp(label, resp)
        self.assertEqual(
            got, want,
            f"{label}: top-level key set changed.\n  got:  {sorted(got)}\n"
            f"  want: {sorted(want)}\nIf the vendor changed the contract, "
            f"re-probe and update references/verified-facts.md + ENDPOINTS "
            f"(TRIAGE.md: 'API contract change').")

    def test_user_info(self):
        self.repro("GET /twitter/user/info?userName=kaitoinfra")
        r = self.c._raw("GET", "/twitter/user/info", {"userName": "kaitoinfra"})
        self._assert_keys("user_info", r, EXPECTED["user_info"])

    def test_follower_ids(self):
        self.repro("GET /twitter/user/followers_ids?userName=kaitoinfra")
        r = self.c._raw("GET", "/twitter/user/followers_ids",
                        {"userName": "kaitoinfra"})
        self._assert_keys("follower_ids", r, EXPECTED["follower_ids"])
        self.assertIsInstance(r["ids"], list)

    def test_followers(self):
        self.repro("GET /twitter/user/followers?userName=kaitoinfra&pageSize=200")
        r = self.c._raw("GET", "/twitter/user/followers",
                        {"userName": "kaitoinfra", "pageSize": 200})
        self._assert_keys("followers", r, EXPECTED["followers"])
        self.assertIsInstance(r["followers"], list)

    def test_advanced_search_has_no_status_envelope(self):
        self.repro("GET /twitter/tweet/advanced_search?query=from:kaitoinfra&queryType=Latest")
        r = self.c._raw("GET", "/twitter/tweet/advanced_search",
                        {"query": "from:kaitoinfra", "queryType": "Latest"})
        self._assert_keys("advanced_search", r, EXPECTED["advanced_search"])

    def test_last_tweets_trap_shape(self):
        """THE trap: tweets nest at data.tweets, pagination stays at ROOT.
        Uses jack (guaranteed non-empty timeline) so data.tweets is present."""
        self.repro("GET /twitter/user/last_tweets?userName=jack")
        r = self.c._raw("GET", "/twitter/user/last_tweets",
                        {"userName": "jack"})
        self._assert_keys("last_tweets", r, EXPECTED["last_tweets"])
        data = r["data"] or {}
        self.log(f"data keys: {sorted(data.keys())}")
        self.assertIn("tweets", data, "tweets must live INSIDE data")
        self.assertNotIn("has_next_page", data,
                         "pagination must NOT be inside data — root only")
        self.assertNotIn("next_cursor", data)

    def test_tweets_by_ids(self):
        self.repro("GET /twitter/tweets?tweet_ids=2084352161404920316")
        r = self.c._raw("GET", "/twitter/tweets",
                        {"tweet_ids": "2084352161404920316"})
        self._assert_keys("tweets_by_ids", r, EXPECTED["tweets_by_ids"])

    def test_filter_rules_list_read_only(self):
        """Rule LISTING is read-only and allowed; rule creation is deliberately
        untested (billable server-side state — see suite non-goals)."""
        self.repro("GET /oapi/tweet_filter/get_rules")
        r = self.c._raw("GET", "/oapi/tweet_filter/get_rules")
        self._assert_keys("get_rules", r, EXPECTED["get_rules"])
        from realtime import active_rule_warning
        self.log(active_rule_warning(self.c))


if __name__ == "__main__":
    unittest.main()
