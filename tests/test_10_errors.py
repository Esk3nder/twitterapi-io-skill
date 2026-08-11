"""Tier 1 — live error semantics. A few requests, ~$0.0005.

Verifies the three documented error surfaces against the live API:
  - a nonexistent path is HTTP 404 (existence probing contract),
  - an existing path with no params is HTTP 400 with a `detail` naming the field,
  - the HTTP-200-with-body-error pattern ({"status":"error", ...}) is RAISED
    by the client, not returned as success. This is exercised on
    /oapi/x_user_stream/add_user_to_monitor_tweet, which on this
    subscription-less account is verified (2026-08-10) to return
    {"status":"error","msg":"No active monitoring subscription"} and create
    no server-side state. If it ever unexpectedly succeeds, the test attempts
    removal and fails loudly — that would mean a monitoring subscription
    appeared on the account.
"""
import json
import unittest

from tests.e2e_base import E2ETest, require_key


class TestErrorSemantics(E2ETest):
    def setUp(self):
        super().setUp()
        require_key()
        from twitterapi import Client
        self.c = self.track(Client(verbose=False))

    def test_nonexistent_path_is_404(self):
        from twitterapi import APIError
        self.repro("GET /twitter/no_such_endpoint_e2e_zz")
        with self.assertRaises(APIError) as cm:
            self.c._raw("GET", "/twitter/no_such_endpoint_e2e_zz")
        self.log(f"got status={cm.exception.status} detail={cm.exception.detail!r}")
        self.assertEqual(cm.exception.status, 404,
                         "existence probing relies on 404-for-missing-path")

    def test_existing_path_without_params_is_400_with_detail(self):
        from twitterapi import APIError
        self.repro("GET /twitter/user/followers (no params)")
        with self.assertRaises(APIError) as cm:
            self.c._raw("GET", "/twitter/user/followers")
        self.log(f"got status={cm.exception.status} detail={cm.exception.detail!r}")
        self.assertEqual(cm.exception.status, 400)
        self.assertIn("required", str(cm.exception.detail).lower(),
                      "400 detail must name the missing field")

    def test_http_200_body_error_is_raised_not_success(self):
        """Regression for the transport fix: the API signals failure in the
        body of an HTTP 200. Before the fix, this response flowed through as
        a 'successful' dict and paginate() would render it as an empty page."""
        from twitterapi import APIError
        path = "/oapi/x_user_stream/add_user_to_monitor_tweet"
        self.repro(f"POST {path} x_user_name=kaitoinfra (subscription-less account)")
        try:
            resp = self.c._raw_json("POST", path, {"x_user_name": "kaitoinfra"})
        except APIError as e:
            self.log(f"surfaced as APIError: status={e.status} detail={e.detail!r}")
            self.assertEqual(e.status, 200,
                             "must be the body-level error, not an HTTP error")
            self.assertIn("subscription", str(e.detail).lower(),
                          f"unexpected body error: {e.detail!r}")
            return
        # No exception: either the client regressed, or the account now HAS a
        # monitoring subscription and we just created server-side state.
        self.dump_resp("UNEXPECTED non-error response", resp)
        cleanup = None
        try:
            listing = self.c._raw("GET", "/oapi/x_user_stream/get_user_monitor_account")
            self.dump_resp("monitor accounts", listing)
            for acct in (listing.get("accounts") or listing.get("data") or []):
                if str(acct.get("x_user_name", "")).lower() == "kaitoinfra":
                    cleanup = self.c._raw_json(
                        "POST", "/oapi/x_user_stream/remove_user_from_monitor_tweet",
                        {"id_for_user": acct.get("id") or acct.get("id_for_user")})
                    self.dump_resp("cleanup attempt", cleanup)
        except Exception as e:  # noqa: BLE001 — best-effort cleanup, then fail
            self.log(f"cleanup attempt failed: {type(e).__name__}: {e}")
        self.fail(
            "add_user_to_monitor_tweet did NOT return the expected body error. "
            "Either the client stopped raising on status=='error' (regression) "
            "or the account now has an active monitoring subscription — verify "
            f"and remove any created monitor. Cleanup result: {cleanup!r}")

    def test_missing_user_is_body_error(self):
        """user/info on a missing account returns HTTP 200
        {"status":"error","msg":"user not found"} (probed 2026-08-10).
        Post-fix contract: user_info raises APIError(200) instead of silently
        returning {} — 'no such user' must not read as 'zero followers'."""
        from twitterapi import APIError
        self.repro("Client.user_info('zz_no_such_user_98q7x')")
        with self.assertRaises(APIError) as cm:
            self.c.user_info("zz_no_such_user_98q7x")
        self.log(f"status={cm.exception.status} detail={cm.exception.detail!r}")
        self.assertEqual(cm.exception.status, 200)
        self.assertIn("not found", str(cm.exception.detail).lower())


if __name__ == "__main__":
    unittest.main()
