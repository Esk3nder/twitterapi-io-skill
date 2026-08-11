# E2E failure triage

The suite asserts **invariants against live data**, so a failure is one of a
small number of classes. Every test logs request counts, credits, response key
sets, store stats and an exact repro command to stderr — read the `[test_name]`
lines above the traceback first.

Run: `bash run_e2e.sh` from the skill root (temp-isolated store, per-test cost
report, hard budget $0.25). Single test:
`E2E_STORE_DIR=$(mktemp -d) python3 -m unittest tests.test_60_crawl_cache -v`

| Failure class | How it looks | Likely cause | Next step |
|---|---|---|---|
| Missing/invalid key | Tests report `skipped: TWITTERAPI_IO_KEY not set`; suite exits 0 with a NOTICE | Env var absent (CI secret unset, or non-login shell without `~/.zshenv`) | **Not a failure.** Export the key or set the `TWITTERAPI_IO_KEY` repo secret. An *invalid* key instead fails `test_10`/`test_20` with 401/403 APIErrors on every call — rotate the key. |
| Balance-tier QPS drift | Suite suddenly takes minutes; `[twitterapi] balance ... -> N QPS` lines show N < 20; timeouts on the big tiers | Account balance fell below a QPS ladder rung (20 QPS needs >= 50,000 credits ≈ $0.50) | Check `python3 scripts/twitterapi.py` (prints balance + QPS). Top up the account. Do not "fix" the client's derived rate. |
| API contract change | `test_20_envelopes`: "top-level key set changed", with got-vs-want sets in the log | Vendor added/removed/renamed an envelope key | Re-probe the endpoint live (GET with no params: 400 = exists), update `references/verified-facts.md` **and** the `ENDPOINTS` table together, then update `EXPECTED` in `test_20_envelopes.py`. Never update the test alone — the facts file is the source of truth. |
| Cache pollution / store not isolated | `test_60` crawl 2 shows requests > 0 with stale ids, or crawl 1 finds pages already cached; base harness raises "resolves inside the production cache" | `E2E_STORE_DIR` pointed at a reused/shared dir, or a test constructed `Store()` with no path | Always run via `run_e2e.sh` (fresh mktemp per run). If a test added a bare `Store()`, that is the bug — every store must go through `fresh_store()`; subprocesses get `HOME` overridden by `run_cli()`. |
| Rate-limit 429 bursts | APIError 429 in logs; the client retries 5x with backoff, so persistent 429s mean sustained pressure | Parallel suite runs against one key, or the tier dropped (see QPS drift) | Re-run alone. If it persists at healthy balance, capture the response and compare with the QPS ladder — the vendor may have changed limits; re-verify and update `QPS_LADDER` + verified-facts. |
| Boundary/content drift | `test_50` self-overlap: "kaitoinfra returned no followers"; `test_40`: "window unexpectedly empty"; `test_60`: jack pages < 5000 | A fixture *account* changed in the world (renamed, suspended, private, follower count collapsed) — not a code bug | Verify the account in a browser. Replace the fixture handle with another small/large account and note the swap in the test docstring. |
| Genuine regression in scripts/ | Invariant failures with healthy envelopes: wrong counts, nonzero spend on cached crawl, duplicate ids, exit codes not 2/3, movers containing `retweet`, `spent_credits != page_credits` sum | A change to `scripts/*.py` broke transport, accounting, pagination or a guard | The failing assertion names the contract. Use the logged repro command, bisect the script change. Do not loosen the assertion. |
| Budget hard-fail | `run_e2e.sh` exits with "BUDGET EXCEEDED" after the cost table | A crawl looped or a fixture grew (e.g. diffusion tweet went viral past its `limit` bound, new expensive test added) | Read the per-test cost table — the offender is obvious. Bound it with `limit`/`max_usd` in the test; the $0.25 ceiling protects the account and stays. |
| Body-error surfacing | `test_10` fails with "did NOT return the expected body error" | Either the client stopped raising on `status == "error"` (regression) or the account now HAS a monitoring subscription — the test attempts cleanup and says which | If a subscription exists: check `GET /oapi/x_user_stream/get_user_monitor_account` and remove any `kaitoinfra` monitor immediately (it bills). If not: transport regression in `_raw`/`_raw_json`. |

Notes that prevent false diagnoses:

- **Billing settles 20–60 s late server-side.** All cost assertions here use
  client-side accounting (`spent_credits` / `page_credits`). Never "verify" a
  test by reading the balance right after a call.
- **Order determinism:** `test_40` asserts id **set** equality across two runs
  of the same window; sequence order is logged but not asserted, because the
  API may reorder within a page. If sets differ, that is real nondeterminism
  (or a tweet was deleted mid-run — check the logged only-in-run-X ids).
- **`user_info` on a missing account raises `APIError(200, 'user not found')`**
  since the body-error fix. Code that expected `{}` for missing users must
  catch it — "no such user" no longer reads as "zero followers".
