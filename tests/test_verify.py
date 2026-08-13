"""Focused, offline regression tests for the F23 drift-detector sweep."""
from __future__ import annotations

import contextlib
import io
import unittest
from unittest import mock

from tests.e2e_base import SCRIPTS  # noqa: F401  (adds scripts/ to sys.path)

import verify
from twitterapi import APIError, ENDPOINTS


class FakeClient:
    def __init__(self, responses):
        self.responses = dict(responses)
        self.spent_credits = 0.0
        self.requests_made = 0

    @property
    def spent_usd(self):
        return self.spent_credits / 100_000

    def _raw(self, method, path, params):
        self.requests_made += 1
        name = next(name for name, spec in ENDPOINTS.items()
                    if spec["path"] == path)
        result = self.responses[name]
        if isinstance(result, Exception):
            raise result
        return result

    def _charge(self, name, records_in_page, page_size):
        from twitterapi import Client, MIN_REQUEST_CREDITS
        self.spent_credits += max(
            0.0,
            Client.page_credits(name, records_in_page, page_size)
            - MIN_REQUEST_CREDITS,
        )


class TestVerifyCoverage(unittest.TestCase):
    def test_default_partition_covers_every_endpoint(self):
        """F23: no ENDPOINTS entry may be silently absent from the sweep."""
        probed = set(verify.probe_endpoint_names(quick=False))
        unprobeable = set(verify.UNPROBEABLE)

        self.assertFalse(probed & unprobeable)
        self.assertEqual(
            set(ENDPOINTS),
            probed | unprobeable,
            "every endpoint must be probed by default or explicitly UNPROBEABLE",
        )
        self.assertIn("articles", probed)
        self.assertIn("tweet_timeline", probed)

    def test_new_endpoint_is_watched_by_default(self):
        with mock.patch.dict(
                verify.ENDPOINTS,
                {"future_endpoint": {
                    "path": "/future", "key_param": "userName",
                    "items": "users", "items_in": "root",
                    "page_param": None, "page_max": 20,
                }}):
            self.assertIn(
                "future_endpoint",
                verify.probe_endpoint_names(quick=False),
                "new table entries must not require a second allow-list edit",
            )

    def test_quick_mode_is_the_legacy_nine_endpoint_sample(self):
        self.assertEqual(
            verify.probe_endpoint_names(quick=True),
            list(verify.QUICK_ENDPOINTS),
        )
        self.assertEqual(len(verify.QUICK_ENDPOINTS), 9)

    def test_probe_params_use_minimum_pages_and_preserve_known_f25(self):
        fixtures = {"user_id": "123"}

        self.assertEqual(
            verify.probe_params("follower_ids", fixtures)["count"], 1)
        self.assertEqual(
            verify.probe_params("followers", fixtures)["pageSize"], 20)
        self.assertEqual(
            verify.probe_params("trends", fixtures)["count"], 1)
        self.assertEqual(
            verify.probe_params("verified_followers", fixtures)["user_id"],
            "123",
        )
        self.assertEqual(
            verify.probe_params("tweet_timeline", fixtures)["userId"],
            verify.TIMELINE_HANDLE,
            "F23 must expose F25 rather than silently substituting a numeric id",
        )


class TestVerifyFailures(unittest.TestCase):
    def test_contract_failure_is_counted_and_does_not_stop_later_probes(self):
        names = ["articles", "tweets_by_ids"]
        client = FakeClient({
            "articles": {
                "data": [{"id": str(i)} for i in range(13)],
                "has_next_page": False, "next_cursor": "",
            },
            "tweets_by_ids": {
                "code": 0, "tweets": [{"id": "1"}],
                "msg": "success", "status": "success",
            },
        })

        failures = verify.check_contracts(
            client, {"response_contracts": {}}, names, observed_out={})

        self.assertEqual([failure[0] for failure in failures], ["articles"])
        self.assertEqual(client.requests_made, 2)

    def test_environment_error_remains_exit_two_not_endpoint_drift(self):
        client = FakeClient({
            "articles": APIError(401, "bad key", ENDPOINTS["articles"]["path"]),
        })

        with self.assertRaises(EnvironmentError):
            verify.check_contracts(
                client, {"response_contracts": {}}, ["articles"])

    def test_main_exits_nonzero_and_prints_explicit_coverage(self):
        output = io.StringIO()
        fake = mock.Mock(spent_usd=0.0012)
        facts = {
            "verified_at": "2026-08-10",
            "verified_against": "test",
            "staleness_warn_days": 90,
            "response_contracts": {},
        }
        failure = [("articles", "request failed", "broken")]

        with mock.patch.dict(verify.os.environ, {"TWITTERAPI_IO_KEY": "test"}), \
             mock.patch.object(verify, "load_facts", return_value=facts), \
             mock.patch.object(verify, "check_internal_consistency", return_value=[]), \
             mock.patch.object(verify, "Client", return_value=fake), \
             mock.patch.object(verify, "check_contracts", return_value=failure), \
             contextlib.redirect_stdout(output):
            rc = verify.main([])

        rendered = output.getvalue()
        self.assertEqual(rc, 1)
        self.assertIn("probed 22/32", rendered)
        self.assertIn("10 unprobeable", rendered)
        self.assertIn("1 FAILING", rendered)
        self.assertIn("spent $0.00120", rendered)


if __name__ == "__main__":
    unittest.main()
