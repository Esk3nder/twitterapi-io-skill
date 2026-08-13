#!/usr/bin/env python3
"""Re-verify this skill's recorded facts against the live API.

    python3 scripts/verify.py              # full read-surface sweep
    python3 scripts/verify.py --quick      # legacy 9-endpoint sample
    python3 scripts/verify.py --pricing    # + balance-delta price check, ~3 min
    python3 scripts/verify.py --update     # re-stamp facts.json from observation

WHY THIS EXISTS
    A skill that asserts "followers cost $0.01/1k" in prose cannot notice when
    that stops being true. Documentation rots silently and confidently — that
    is the exact failure this skill was built to replace. So the facts live in
    references/facts.json as evidence, and this command re-observes the API and
    diffs reality against that record.

    Exit 0 = the recorded facts still hold.
    Exit 1 = DRIFT. Something the skill asserts is no longer true; read the
             diff, re-probe, and update facts.json (and any prose that repeats
             the changed value) BEFORE trusting downstream results.

Contract checks are cheap and decisive: response key SETS are stable even when
content changes daily, so a changed key set means a real API change.
"""
from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from twitterapi import Client, ENDPOINTS, APIError  # noqa: E402

FACTS = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                     "..", "references", "facts.json")

# Stable, read-only fixtures already used by the suite or the old verifier.
HANDLE = "kaitoinfra"
TIMELINE_HANDLE = "jack"
ARTICLE_HANDLE = "elonmusk"
TWEET_ID = "2084352161404920316"
COMMUNITY_ID = "1493446837214187523"

# The compatibility sample is intentionally frozen. It is useful for a cheap
# smoke check, but its output must say that 23 endpoint names were not probed.
QUICK_ENDPOINTS = (
    "user_info", "follower_ids", "followers", "followings", "last_tweets",
    "search", "retweeters", "trends", "community_info",
)

# Exclusion is opt-in. Every ENDPOINTS entry not named here is probed by
# default, so adding an endpoint to the table cannot silently leave it unseen.
UNPROBEABLE = {
    "community_members": (
        "needs a Community id; /twitter/user/communities returns a user-profile "
        "payload instead of community data, so there is no discovery route"),
    "community_moderators": (
        "needs a Community id; /twitter/user/communities returns a user-profile "
        "payload instead of community data, so there is no discovery route"),
    "community_search": (
        "community discovery is unavailable; /twitter/user/communities returns "
        "a user-profile payload instead of community data"),
    "community_tweets": (
        "needs a Community id; /twitter/user/communities returns a user-profile "
        "payload instead of community data, so there is no discovery route"),
    "community_tweets_all": (
        "community discovery is unavailable; /twitter/user/communities returns "
        "a user-profile payload instead of community data"),
    "list_followers": (
        "needs a List id; every /twitter/user/lists parameter variant returns "
        "'user not found' for a known existing user, so there is no discovery "
        "route"),
    "list_members": (
        "needs a List id; every /twitter/user/lists parameter variant returns "
        "'user not found' for a known existing user, so there is no discovery "
        "route"),
    "list_tweets": (
        "needs a List id; every /twitter/user/lists parameter variant returns "
        "'user not found' for a known existing user, so there is no discovery "
        "route"),
    "list_tweets_filtered": (
        "needs a List id; every /twitter/user/lists parameter variant returns "
        "'user not found' for a known existing user, so there is no discovery "
        "route"),
    "space_detail": "needs a Space id; none is available to the suite",
}

_LAST_SETUP_ERROR = None


def probe_endpoint_names(*, quick=False):
    """Return the sweep in ENDPOINTS order; full coverage is the default."""
    if quick:
        return list(QUICK_ENDPOINTS)
    return [name for name in ENDPOINTS if name not in UNPROBEABLE]


def check_coverage(endpoint_names):
    """Fail closed if the default sweep ever loses an ENDPOINTS entry."""
    probed = set(endpoint_names)
    excluded = set(UNPROBEABLE)
    overlap = probed & excluded
    missing = set(ENDPOINTS) - probed - excluded
    unknown = (probed | excluded) - set(ENDPOINTS)
    if overlap or missing or unknown:
        raise AssertionError(
            "endpoint coverage partition is invalid: "
            f"missing={sorted(missing)} overlap={sorted(overlap)} "
            f"unknown={sorted(unknown)}")


def probe_params(name, fixtures):
    """Derive one minimum-cost read-only request from the endpoint contract."""
    spec = ENDPOINTS[name]
    key = spec["key_param"]

    if name == "community_info":
        value = COMMUNITY_ID
    elif name == "articles":
        value = ARTICLE_HANDLE
    elif name in ("followings", "last_tweets", "mentions"):
        value = TIMELINE_HANDLE
    elif name == "tweet_timeline":
        # F25: this handle-based probe must currently fail because the endpoint
        # accepts only a numeric userId. Do not hide that client-surface break.
        value = TIMELINE_HANDLE
    elif key in ("tweetId", "tweet_ids"):
        value = TWEET_ID
    elif key in ("userId", "userIds", "user_id"):
        value = fixtures.get("user_id")
        if not value:
            raise ValueError("numeric user id was not discoverable from user_info")
    elif key in ("userName", "username"):
        value = HANDLE
    elif key == "query":
        value = "from:kaitoinfra" if name == "search" else HANDLE
    elif key == "woeid":
        value = 1
    else:
        raise ValueError(
            f"no safe read-only fixture rule for key parameter {key!r}")

    params = {key: value}
    if name in ("search", "replies_v1", "replies_v2"):
        params["queryType"] = "Latest"
    if spec["page_param"]:
        # followers/followings clamp at 20; the other tunable endpoints accept 1.
        params[spec["page_param"]] = (
            20 if name in ("followers", "followings") else 1)
    return params


def _page_size(name, params):
    spec = ENDPOINTS[name]
    return int(params.get(spec["page_param"], spec["page_max"])) \
        if spec["page_param"] else spec["page_max"]


def _record_count(resp, spec):
    """Count billable records, conservatively, even for a malformed envelope."""
    if spec["items"] is None:
        return 1
    container = resp.get("data") if spec["items_in"] == "data" else resp
    if (isinstance(container, dict)
            and isinstance(container.get(spec["items"]), list)):
        return len(container[spec["items"]])
    if isinstance(container, list):
        return len(container)
    return spec["page_max"]


def load_facts(path=FACTS):
    with open(path) as f:
        return json.load(f)


def days_since(datestr):
    try:
        t = time.mktime(time.strptime(datestr, "%Y-%m-%d"))
        return int((time.time() - t) / 86400)
    except Exception:
        return None


def check_internal_consistency(facts):
    """facts.json (evidence) must agree with ENDPOINTS (behaviour).

    Without this the two records of the same truth can drift apart silently —
    the evidence file could say records arrive under `results` while the parser
    still reads `tweets`, and nothing would notice. Cheap, offline, no API."""
    drift = []
    for name, want in facts["response_contracts"].items():
        if name.startswith("_") or not isinstance(want, dict):
            continue                        # documentation keys, not endpoints
        spec = ENDPOINTS.get(name)
        if not spec:
            drift.append((name, "in facts.json but not in ENDPOINTS", "-"))
            print(f"  DRIFT  {name:16s} recorded but the client cannot call it")
            continue
        for field in ("items_key", "items_in"):
            recorded = want.get(field)
            actual = spec["items" if field == "items_key" else "items_in"]
            if recorded != actual:
                drift.append((name, f"{field} disagrees with ENDPOINTS",
                              f"facts.json={recorded!r} code={actual!r}"))
                print(f"  DRIFT  {name:16s} {field}: facts.json says {recorded!r}, "
                      f"client code says {actual!r}")
        if want.get("page_max") and want["page_max"] != spec["page_max"]:
            drift.append((name, "page_max disagrees with ENDPOINTS",
                          f"facts.json={want['page_max']} code={spec['page_max']}"))
            print(f"  DRIFT  {name:16s} page_max: facts.json "
                  f"{want['page_max']} vs code {spec['page_max']}")
    if not drift:
        print(f"  ok     facts.json agrees with the client's endpoint table "
              f"({len(facts['response_contracts'])} entries)")
    return drift


def check_contracts(c, facts, endpoint_names, observed_out=None):
    """Compare live response shapes against the recorded contracts."""
    failures = []
    contracts = facts["response_contracts"]
    fixtures = {}
    for name in endpoint_names:
        want = contracts.get(name)
        spec = ENDPOINTS[name]
        try:
            params = probe_params(name, fixtures)
        except Exception as e:
            failures.append((name, "probe configuration failed", str(e)[:90]))
            print(f"  FAIL   {name:22s} probe configuration: {str(e)[:60]}")
            continue
        try:
            if spec.get("key_resolver"):
                key_param = spec["key_param"]
                params[key_param] = c._resolve_endpoint_key(
                    name, params[key_param])
            resp = c._raw("GET", spec["path"], params)
        except APIError as e:
            # A failed request is NOT contract drift. Reporting "the API
            # changed" when the real cause is a bad key sends a first-time
            # user off editing facts.json for nothing.
            if e.status in (401, 402, 403, 429) or e.status >= 500:
                raise EnvironmentError(
                    {401: "TWITTERAPI_IO_KEY is invalid or expired.",
                     403: "TWITTERAPI_IO_KEY lacks access to this endpoint.",
                     402: "Account is out of credits — top up at "
                          "https://twitterapi.io/dashboard.",
                     429: "Rate limited. Wait and retry; QPS scales with balance.",
                     }.get(e.status, f"twitterapi.io returned {e.status} — "
                                     f"likely a transient outage, retry later.")
                    + f"  (on {name} -> {spec['path']})") from None
            failures.append((name, "request failed", str(e)[:90]))
            print(f"  FAIL   {name:22s} request failed: {str(e)[:60]}")
            continue

        c._charge(name, _record_count(resp, spec), _page_size(name, params))
        if name == "user_info":
            profile = resp.get("data") or {}
            if isinstance(profile, dict) and profile.get("id") not in (None, ""):
                fixtures["user_id"] = str(profile["id"])

        got_keys = sorted(resp.keys())
        if observed_out is not None:
            observed_out[name] = {"top_level_keys": got_keys}

        if want and got_keys != sorted(want["top_level_keys"]):
            missing = set(want["top_level_keys"]) - set(got_keys)
            extra = set(got_keys) - set(want["top_level_keys"])
            failures.append((name, "key set changed",
                             f"missing={sorted(missing)} new={sorted(extra)}"))
            print(f"  FAIL   {name:22s} keys {got_keys}")
            print(f"         expected {sorted(want['top_level_keys'])}"
                  f"  missing={sorted(missing)} new={sorted(extra)}")
            continue

        # The parser must still find records where the contract says they are.
        try:
            items, has_next, cursor = Client._unpack(resp, spec)
        except Exception as e:
            failures.append((name, "response contract failed", str(e)[:90]))
            print(f"  FAIL   {name:22s} response contract: {str(e)[:60]}")
            continue
        if spec["items"] is not None and not isinstance(items, list):
            failures.append((name, "items not a list", type(items).__name__))
            print(f"  FAIL   {name:22s} items_key '{spec['items']}' did not "
                  "yield a list")
            continue
        n = len(items) if isinstance(items, list) else "-"
        contract = "exact keys + envelope" if want else "ENDPOINTS envelope"
        print(f"  ok     {name:22s} {contract}, {n} records via "
              f"{spec['items_in']}.{spec.get('items')}")
    return failures


def check_pricing(c, facts):
    """Balance-delta price check. Slow: server billing settles 20-60s late."""
    drift = []
    print("\n  pricing (balance-delta; ~3 min of settle waits)")
    settle = 75

    def bal():
        return int(c._raw("GET", "/oapi/my/info")["recharge_credits"])

    time.sleep(settle)
    b0 = bal()
    n = len(list(c.follower_ids("kaitoinfra")))     # small page -> 15cr floor
    c.user_info("kaitoinfra")                       # profile -> 18cr
    time.sleep(settle)
    b1 = bal()
    got = b0 - b1
    want = facts["pricing"]["min_request_credits"] + facts["pricing"]["flat"]["profile"]
    if got != want:
        drift.append(("pricing", "credits per call changed", f"{got} vs {want}"))
        print(f"  DRIFT  small page + profile billed {got} credits, expected {want}")
    else:
        print(f"  ok     small page + profile billed exactly {got} credits")

    # /oapi/my/info must remain free or every qps lookup silently costs.
    time.sleep(settle)
    b2 = bal()
    for _ in range(10):
        bal()
    time.sleep(settle)
    b3 = bal()
    if b2 - b3 != 0:
        drift.append(("pricing", "/oapi/my/info is no longer free",
                      f"{b2 - b3} credits over 11 calls"))
        print(f"  DRIFT  /oapi/my/info now bills {b2 - b3} credits over 11 calls")
    else:
        print("  ok     /oapi/my/info still free (11 calls, 0 credits)")
    return drift


def _legacy_main(argv=None):
    global _LAST_SETUP_ERROR
    _LAST_SETUP_ERROR = None
    from axi import ArgumentParser
    p = ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--pricing", action="store_true",
                   help="also re-measure prices by balance delta (~3 min)")
    p.add_argument("--quick", action="store_true",
                   help="probe only the legacy 9-endpoint smoke sample")
    p.add_argument("--update", action="store_true",
                   help="rewrite facts.json from what was just observed")
    a = p.parse_args(argv)

    if not os.environ.get("TWITTERAPI_IO_KEY"):
        print("NOTICE: TWITTERAPI_IO_KEY not set — cannot verify. Skipping.")
        return 0

    facts = load_facts()
    age = days_since(facts.get("verified_at", ""))
    print(f"facts.json verified_at {facts.get('verified_at')} "
          f"({age} days ago)" if age is not None else "facts.json undated")
    endpoint_names = probe_endpoint_names(quick=a.quick)
    if not a.quick:
        check_coverage(endpoint_names)
    print(f"re-probing {len(endpoint_names)} of {len(ENDPOINTS)} endpoints "
          f"against {facts['verified_against']}"
          + (" (--quick: partial sweep)" if a.quick else "") + "\n")
    if not a.quick:
        for name, reason in UNPROBEABLE.items():
            print(f"  UNPROBEABLE  {name:22s} {reason}")
        print()

    drift = check_internal_consistency(facts)   # offline, free, runs first
    c = Client(verbose=False)
    observed = {}
    try:
        probe_failures = check_contracts(c, facts, endpoint_names, observed)
        if a.pricing:
            drift += check_pricing(c, facts)
    except EnvironmentError as e:
        # Setup/account problem, not a contract change. Exit 2 so callers and
        # CI can tell "fix your key" apart from "the API moved" (exit 1).
        print(f"\nCANNOT VERIFY — this is a setup problem, not API drift:\n  {e}\n"
              f"\nNothing about the recorded facts has been called into question. "
              f"Fix the above and re-run.", file=sys.stderr)
        _LAST_SETUP_ERROR = str(e)
        return 2

    skipped_quick = len(ENDPOINTS) - len(endpoint_names) if a.quick else 0
    coverage = f"probed {len(endpoint_names)}/{len(ENDPOINTS)}"
    if a.quick:
        coverage += f" · {skipped_quick} skipped by --quick"
    else:
        coverage += (f" · {len(UNPROBEABLE)} unprobeable "
                     "(no discovery route)")
    coverage += f" · {len(probe_failures)} FAILING · spent ${c.spent_usd:.5f}"
    print(f"\nCOVERAGE  {coverage}")

    drift += probe_failures

    if drift:
        print(f"\nVERIFY FAILED — {len(drift)} check(s) failed:\n")
        for name, kind, detail in drift:
            print(f"  {name}: {kind} — {detail}")
        print("\nThe probeable read surface is not fully healthy. Investigate each\n"
              "failure before trusting downstream results; update recorded facts\n"
              "only when the live contract, rather than the client, actually moved.")
        return 1

    if a.quick:
        print(f"\nQuick sweep passed for {len(endpoint_names)}/{len(ENDPOINTS)} "
              "endpoints; run without --quick for the full probeable surface.")
    else:
        print("\nAll probed endpoint contracts passed; endpoints marked "
              "UNPROBEABLE were not tested.")
    if a.update:
        facts["verified_at"] = time.strftime("%Y-%m-%d")
        for name, obs in observed.items():
            if name in facts["response_contracts"]:
                facts["response_contracts"][name]["top_level_keys"] = obs["top_level_keys"]
        with open(FACTS, "w") as f:
            json.dump(facts, f, indent=2)
            f.write("\n")
        print(f"facts.json re-stamped {facts['verified_at']}")
    elif age is not None and age >= facts.get("staleness_warn_days", 90):
        print(f"NOTE: the stamp is {age} days old. Re-run with --update to refresh it.")
    return 0


def _axi_main(argv=None):
    from axi import emit, error, fast_version, success
    if fast_version(argv):
        return 0
    if not os.environ.get("TWITTERAPI_IO_KEY"):
        emit(error("setup", "TWITTERAPI_IO_KEY not set; verification was not run",
                   "verify.py", "Set `TWITTERAPI_IO_KEY` in the environment and rerun"))
        return 2
    progress = io.StringIO()
    with contextlib.redirect_stdout(progress):
        rc = _legacy_main(argv)
    rendered = progress.getvalue()
    if rendered:
        print(rendered, file=sys.stderr, end="")
    lines = [line.strip() for line in rendered.splitlines() if line.strip()]
    coverage = next((line for line in lines if line.startswith("COVERAGE")), None)
    detail = (_LAST_SETUP_ERROR if rc == 2 and _LAST_SETUP_ERROR else
              coverage or (lines[-1] if lines else "verification produced no summary"))
    if rc == 0:
        emit(success("verify.py", {"verification": "passed", "summary": detail,
                                   "help": ["Run `verify.py --quick` for a lower-cost smoke sweep"]}))
    else:
        kind = "setup" if rc == 2 else "runtime"
        emit(error(kind, detail, "verify.py",
                   "Inspect stderr diagnostics, fix the reported condition, and rerun `verify.py`"))
    return rc


def main(argv=None):
    from axi import run_cli
    return run_cli("verify.py", _axi_main, argv)


if __name__ == "__main__":
    sys.exit(main())
