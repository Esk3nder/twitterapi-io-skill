#!/usr/bin/env bash
# E2E suite for the twitterapi-io skill. PAID: hits the live API.
#
#   bash run_e2e.sh
#
# - Orders tests cheap -> expensive; a broken transport fails in the
#   sub-cent tiers before any crawl money is spent.
# - Isolates the store in a fresh temp dir. ~/.twitterapi-cache is never
#   touched (the harness hard-fails if a store would resolve there).
# - Prints a per-test and total cost report from CLIENT-SIDE accounting
#   (server billing settles 20-60s late; never assert via balance reads).
# - Hard-fails if a run exceeds $0.25, independent of test results.
# - Missing key: prints a notice and exits 0 (skip, not failure).
set -uo pipefail
cd "$(dirname "$0")"

BUDGET_USD="${E2E_BUDGET_USD:-0.25}"

if [ -z "${TWITTERAPI_IO_KEY:-}" ]; then
  echo "NOTICE: TWITTERAPI_IO_KEY is not set — the paid E2E suite was SKIPPED."
  echo "        Export the key (see ~/.zshenv) or set the CI secret to run it."
  exit 0
fi

TMP="$(mktemp -d "${TMPDIR:-/tmp}/twapi-e2e.XXXXXX")"
export E2E_STORE_DIR="$TMP/store"
export E2E_COST_LOG="$TMP/costs.jsonl"
mkdir -p "$E2E_STORE_DIR"
: > "$E2E_COST_LOG"
echo "store isolation: $E2E_STORE_DIR"
echo "budget ceiling : \$$BUDGET_USD"
echo

# Cheap -> expensive. Tier 0 is free; tiers 1-3 are sub-cent; 4-6 do the
# real crawls.
FILES=(
  test_00_pure
  test_10_errors
  test_20_envelopes
  test_30_refusals
  test_40_history
  test_50_jobs_live
  test_60_crawl_cache
)

suite_failed=0
for f in "${FILES[@]}"; do
  echo "==================== tests.$f ===================="
  if ! python3 -m unittest -v "tests.$f"; then
    suite_failed=1
  fi
  echo
done

echo "==================== cost report ===================="
python3 - "$E2E_COST_LOG" "$BUDGET_USD" <<'PY'
import json, sys

log, budget = sys.argv[1], float(sys.argv[2])
rows = [json.loads(l) for l in open(log) if l.strip()]
if not rows:
    print("no cost records written (all tests skipped?)")
    sys.exit(0)

w = max(len(r["test"]) for r in rows)
print(f"{'test':<{w}}  {'status':6} {'reqs':>5} {'credits':>9} {'usd':>9} "
      f"{'saved_cr':>9} {'secs':>6}")
for r in rows:
    print(f"{r['test']:<{w}}  {r['status']:6} {r['requests']:>5} "
          f"{r['credits']:>9,.1f} {r['usd']:>9.5f} {r['saved_credits']:>9,.1f} "
          f"{r['seconds']:>6.1f}" + ("  *derived" if r.get("derived") else ""))
tot_req = sum(r["requests"] for r in rows)
tot_usd = sum(r["usd"] for r in rows)
tot_cr = sum(r["credits"] for r in rows)
print("-" * (w + 52))
print(f"{'TOTAL':<{w}}  {'':6} {tot_req:>5} {tot_cr:>9,.1f} {tot_usd:>9.5f}")
print(f"\n(client-side accounting; server billing settles ~60s late. "
      f"'*derived' rows include\n subprocess spend computed from the measured "
      f"price table where no report was printed.)")
if tot_usd > budget:
    print(f"\nBUDGET EXCEEDED: ${tot_usd:.4f} > ${budget:.2f} — HARD FAIL. "
          f"See TRIAGE.md 'Budget hard-fail'.")
    sys.exit(1)
print(f"\nbudget ok: ${tot_usd:.4f} of ${budget:.2f}")
PY
budget_failed=$?

if [ "$suite_failed" -ne 0 ] || [ "$budget_failed" -ne 0 ]; then
  echo
  echo "E2E FAILED (tests=$suite_failed budget=$budget_failed). Artifacts kept: $TMP"
  echo "Triage: tests/TRIAGE.md"
  exit 1
fi
rm -rf "$TMP"
echo
echo "E2E PASSED"
