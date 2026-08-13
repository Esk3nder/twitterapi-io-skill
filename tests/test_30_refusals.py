"""Tier 3 — refusal paths. ~$0.0006 (identity lookups only, never a crawl).

Every guard in the system must refuse BEFORE the expensive part:
  - jobs.py overlap of two multi-million-follower accounts under --max-usd 5
    exits 2 having fetched only the two profile lookups, zero follower pages;
  - workflows.py audience elonmusk at --max-usd 1 exits 2 with 0 records;
  - Cohort.authority over 150 members at $0.50 refuses with ZERO requests;
  - diffusion_trace with an over-ceiling client raises CostLimitExceeded with
    ZERO requests (regression: it used to swallow the refusal and return a
    fake '0 engagers' trace);
  - realtime.py rules add WITHOUT --confirm exits 2 before any HTTP.

Note on "spend exactly $0": computing a refusal estimate requires knowing the
follower count, which is itself a priced lookup (18 credits/profile). The
crawls are what refusal must prevent, and these tests pin the spend to
exactly the identity lookups and nothing else.
"""
import os
import json
import sqlite3
import unittest

from tests.e2e_base import E2ETest, STORE_DIR, require_key


class TestOverlapRefusal(E2ETest):
    def test_overlap_of_two_huge_accounts_refuses_before_any_crawl(self):
        require_key()
        p = self.run_cli(["jobs.py", "overlap", "elonmusk", "sama",
                          "--max-usd", "5"])
        self.assertEqual(p.returncode, 2,
                         f"expected refusal exit 2, got {p.returncode}; "
                         f"stderr: {p.stderr[-500:]}")
        payload = json.loads(p.stdout)
        self.assertEqual(payload["error"]["type"], "refusal")
        self.assertIn("$5.00", payload["error"]["message"],
                      "refusal must cite the ceiling it enforced")
        # Zero crawl pages may exist in the (HOME-isolated) store.
        db = os.path.join(STORE_DIR, ".twitterapi-cache", "store.db")
        pages = 0
        if os.path.exists(db):
            pages = sqlite3.connect(db).execute(
                "SELECT COUNT(*) FROM pages WHERE path LIKE '%followers%'"
            ).fetchone()[0]
        self.log(f"follower pages cached after refusal: {pages} (db={db})")
        self.assertEqual(pages, 0, "refusal must happen BEFORE any follower crawl")
        # Spend: exactly 2 user_info lookups (18cr each), derived — the CLI's
        # refusal path prints no spend report.
        self.charge_external(credits=36, requests=3, derived=True)


class TestAudienceRefusal(E2ETest):
    def test_audience_elonmusk_at_1usd_exits_2_with_zero_records(self):
        require_key()
        p = self.run_cli(["workflows.py", "--max-usd", "1", "audience",
                          "elonmusk"])
        self.assertEqual(p.returncode, 2,
                         f"expected exit 2, got {p.returncode}; stderr: {p.stderr[-500:]}")
        payload = json.loads(p.stdout)
        self.assertEqual(payload["error"]["type"], "refusal")
        self.assertIn("--limit", payload["help"][0])
        self.assertIn("0 records", p.stderr)
        spend = self.parse_cli_spend(p.stderr)
        self.log(f"parsed subprocess spend: {spend}")
        if spend:
            requests, credits = spend
            self.assertEqual(credits, 18.0,
                             "refusal must spend exactly the 18-credit profile lookup")
            self.charge_external(credits=credits, requests=requests)
        else:
            self.charge_external(credits=18, requests=2, derived=True)


class TestAuthorityRefusal(E2ETest):
    def test_authority_over_150_members_at_50_cents_refuses_with_zero_requests(self):
        require_key()
        from twitterapi import Client, CostLimitExceeded
        from cohort import Cohort
        c = self.track(Client(verbose=False))
        co = Cohort(client=c, store=self.fresh_store("authority_refusal"),
                    label="synthetic:150")
        for i in range(150):
            co._add(str(1000 + i), f"member{i}", 1.0, "synthetic")
        self.assertEqual(len(co), 150)
        self.repro("Cohort(150 members).authority(max_usd=0.50)")
        with self.assertRaises(CostLimitExceeded) as cm:
            co.authority(max_usd=0.50, confirm=True)
        self.log(f"refusal: {cm.exception}")
        self.assertEqual(c.requests_made, 0, "estimate gate must fire pre-network")
        self.assertEqual(c.spent_credits, 0.0, "authority refusal spends exactly $0")
        self.assertIn("150 members", str(cm.exception))


class TestDiffusionCeilingRegression(E2ETest):
    def test_diffusion_over_ceiling_raises_instead_of_fake_empty_trace(self):
        """Regression for the swallow-CostLimitExceeded fix: an over-ceiling
        client must raise, not return {'timed_engagers': 0, ...} as if the
        tweet simply had no engagement. Zero network: the preflight refuses
        before the first request."""
        require_key()
        from twitterapi import Client, CostLimitExceeded
        from jobs import diffusion_trace
        c = self.track(Client(verbose=False, max_usd=0.000001,
                              store=self.fresh_store("diffusion_refusal")))
        self.repro("diffusion_trace('2084352161404920316', limit=60, "
                   "client=Client(max_usd=1e-6))")
        with self.assertRaises(CostLimitExceeded) as cm:
            diffusion_trace("2084352161404920316", limit=60, client=c)
        self.log(f"refusal: {cm.exception}")
        self.assertEqual(c.requests_made, 0)
        self.assertEqual(c.spent_credits, 0.0)


class TestRealtimeGuard(E2ETest):
    def test_rules_add_without_confirm_refuses_exit_2(self):
        """Rule creation bills continuously while active; the CLI must refuse
        without --confirm, before any HTTP. The suite NEVER creates rules —
        a failed teardown would leave billing server-side state."""
        require_key()
        p = self.run_cli(["realtime.py", "rules", "add",
                          "--tag", "e2e_guard_probe",
                          "--value", "from:kaitoinfra"])
        self.assertEqual(p.returncode, 2,
                         f"expected refusal exit 2, got {p.returncode}; "
                         f"stderr: {p.stderr[-500:]}")
        payload = json.loads(p.stdout)
        self.assertEqual(payload["error"]["type"], "refusal")
        self.assertIn("--confirm", payload["error"]["message"])
        # Read-only verification that no rule leaked (listing is allowed).
        from twitterapi import Client
        from realtime import list_rules
        c = self.track(Client(verbose=False))
        rules = list_rules(c)
        tags = [r.get("tag") for r in rules]
        self.log(f"server-side rules after refusal: {tags}")
        self.assertNotIn("e2e_guard_probe", tags,
                         "the refused add must not have created a rule")


if __name__ == "__main__":
    unittest.main()
