#!/usr/bin/env python3
"""Re-verify this skill's recorded facts against the live API.

    python3 scripts/verify.py              # contracts only, ~$0.002, ~15s
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
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from twitterapi import Client, ENDPOINTS, APIError  # noqa: E402

FACTS = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                     "..", "references", "facts.json")

# Small, stable targets. kaitoinfra has ~10 followers so contract probes stay
# in the 15-credit minimum rather than paying for a real crawl.
PROBES = {
    "user_info":      {"userName": "kaitoinfra"},
    "follower_ids":   {"userName": "kaitoinfra", "count": 5000},
    "followers":      {"userName": "kaitoinfra", "pageSize": 200},
    "followings":     {"userName": "jack", "pageSize": 200},
    "last_tweets":    {"userName": "jack"},
    "search":         {"query": "from:openai", "queryType": "Latest"},
    "replies_v2":     {"tweetId": "2084352161404920316", "queryType": "Latest"},
    "retweeters":     {"tweetId": "2084352161404920316"},
    "trends":         {"woeid": 1, "count": 5},
    "community_info": {"community_id": "1493446837214187523"},
}


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


def check_contracts(c, facts, observed_out=None):
    """Compare live response shapes against the recorded contracts."""
    drift = []
    contracts = facts["response_contracts"]
    for name, params in PROBES.items():
        want = contracts.get(name)
        if not want:
            continue
        spec = ENDPOINTS[name]
        try:
            resp = c._raw("GET", spec["path"], params)
        except APIError as e:
            drift.append((name, "request failed", str(e)[:90]))
            print(f"  DRIFT  {name:16s} request failed: {str(e)[:60]}")
            continue

        got_keys = sorted(resp.keys())
        if observed_out is not None:
            observed_out[name] = {"top_level_keys": got_keys}

        if got_keys != sorted(want["top_level_keys"]):
            missing = set(want["top_level_keys"]) - set(got_keys)
            extra = set(got_keys) - set(want["top_level_keys"])
            drift.append((name, "key set changed",
                          f"missing={sorted(missing)} new={sorted(extra)}"))
            print(f"  DRIFT  {name:16s} keys {got_keys}")
            print(f"         expected {sorted(want['top_level_keys'])}"
                  f"  missing={sorted(missing)} new={sorted(extra)}")
            continue

        # The parser must still find records where the contract says they are.
        items, has_next, cursor = Client._unpack(resp, spec)
        if want.get("items_key") and not isinstance(items, list):
            drift.append((name, "items not a list", type(items).__name__))
            print(f"  DRIFT  {name:16s} items_key '{want['items_key']}' "
                  f"did not yield a list")
            continue
        n = len(items) if isinstance(items, list) else "-"
        print(f"  ok     {name:16s} keys ok, {n} records via "
              f"{want['items_in']}.{want.get('items_key')}")
    return drift


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


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--pricing", action="store_true",
                   help="also re-measure prices by balance delta (~3 min)")
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
    print(f"re-probing {len(PROBES)} endpoints against {facts['verified_against']}\n")

    drift = check_internal_consistency(facts)   # offline, free, runs first
    c = Client(verbose=False)
    observed = {}
    drift += check_contracts(c, facts, observed)
    if a.pricing:
        drift += check_pricing(c, facts)

    print(f"\n  spent ${c.spent_usd:.4f} on verification")

    if drift:
        print(f"\nDRIFT DETECTED — {len(drift)} fact(s) no longer hold:\n")
        for name, kind, detail in drift:
            print(f"  {name}: {kind} — {detail}")
        print("\nThe skill now asserts things the API no longer does. Re-probe the\n"
              "affected endpoints, update references/facts.json (or run --update),\n"
              "and fix any prose in references/verified-facts.md that repeats the\n"
              "changed value. Do not trust downstream results until you do.")
        return 1

    print("\nAll recorded facts still hold.")
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


if __name__ == "__main__":
    sys.exit(main())
