# twitterapi-io

An [Agent Skill](https://docs.claude.com/en/docs/agents-and-tools/agent-skills/overview)
for reading X/Twitter data through [twitterapi.io](https://twitterapi.io) — for
Claude Code, Codex, Cursor, and any agent that loads `SKILL.md`.

It is **read-only**, **standard-library only** (nothing to `pip install`), and
every endpoint, response shape and price in it was verified by live API calls
rather than copied from documentation.

Beyond raw endpoint access it ships composable primitives — a cached cohort
store, set algebra, follow-graph ranking — and eight analytical jobs built on
them (entity brief, narrative tracking, authority mapping, audience overlap,
authenticity audit, diffusion trace, cohort drift, competitive benchmark).

---

## 1. Get an API key

1. Sign up at **https://twitterapi.io/dashboard** (free trial credit included).
2. Copy the API key from the dashboard.
3. Put it in your environment — **never** in a file, and never in a commit:

```bash
echo 'export TWITTERAPI_IO_KEY="your_key_here"' >> ~/.zshenv   # zsh
echo 'export TWITTERAPI_IO_KEY="your_key_here"' >> ~/.bashrc   # bash
```

Open a new shell, then check it:

```bash
curl -sS -H "x-api-key: $TWITTERAPI_IO_KEY" https://api.twitterapi.io/oapi/my/info
# -> {"recharge_credits": 12345, "total_bonus_credits": 0}
```

That balance call is free and unmetered — check it as often as you like.

The key is the only credential this skill uses. It never asks for your X
password, and it cannot post, like, follow, or DM. See
[Writes are out of scope](#writes-are-out-of-scope).

## 2. Install

```bash
git clone https://github.com/Esk3nder/twitterapi-io-skill.git \
  ~/.claude/skills/twitterapi-io
```

For Codex / Cursor / other runtimes, symlink it so there is one source of truth:

```bash
mkdir -p ~/.agents/skills
ln -s ~/.claude/skills/twitterapi-io ~/.agents/skills/twitterapi-io
```

Restart your agent session. Requires **Python 3.11+** (tested on 3.11 and 3.14).

**Confirm it works** — this re-probes the live API and exits non-zero if
anything is wrong with your key or the install:

```bash
cd ~/.claude/skills/twitterapi-io && python3 scripts/verify.py
# -> All recorded facts still hold.        (~$0.002, ~15s)
```

If that fails, see [`tests/TRIAGE.md`](tests/TRIAGE.md).

## 3. Use it from your agent

This is the intended path. Ask in plain language; the agent reads `SKILL.md`
and runs the right command with the right cost guard.

| You say | The agent does |
|---|---|
| "Get me @jack's follower IDs" | picks `followers_ids` (the cheap endpoint), prints an estimate, streams the IDs |
| "What has @openai tweeted in the last month?" | `history` with a sliding time window, deduped |
| "Who follows both @stripe and @vercel?" | crawls both, caches them, returns the intersection — the second run is free |
| "Is @someaccount's audience real?" | samples followers and returns account-age and empty-profile signals for you to judge |
| "Who reacted first to this tweet?" | orders repliers and quoters by time; counts retweeters separately |
| "Give me an update on @polymarket" | pulls the profile plus recent posts and hands you the raw material |

Two behaviors worth expecting:

- **It will refuse expensive things and tell you the price.** Ask for all of a
  large account's followers and you get "this is ~$1,085, over the $5 ceiling"
  rather than a surprise bill. Raise it deliberately with `--max-usd`.
- **It won't invent conclusions.** Jobs return structured data and the model
  interprets it. If it says an audience looks inorganic, that judgment is the
  model reading real signals, not a score the API returned.

## 4. Quickstart (running the scripts directly)

> **On a new account your first runs will feel slow, and that is correct.**
> Rate limit scales with credit balance: a near-zero balance means **one
> request every 5 seconds**. Nothing is broken; add credit and it speeds up to
> 20 requests/second. See [Rate limits](#rate-limits).

```bash
cd ~/.claude/skills/twitterapi-io

# cheapest follower enumeration: IDs, 5,000 per request   (~$0.02)
python3 scripts/workflows.py --max-usd 1 audience jack --limit 5000

# every tweet in a date range (sliding window, not cursors)  (~$0.02)
python3 scripts/workflows.py history openai --since 2026-06-01 --until 2026-07-01

# analytical jobs, JSON out                                  (~$0.01 each)
python3 scripts/jobs.py brief openai --days 7
python3 scripts/jobs.py diffusion 2084352161404920316
```

From Python:

```python
import sys; sys.path.insert(0, "scripts")
from twitterapi import Client
from store import Store

c = Client(store=Store(), max_usd=5.0)     # cache on, $5 ceiling
print(c.estimate("follower_ids", 200_000_000))   # -> 900.0  (dollars)
for uid in c.follower_ids("jack", limit=10_000):
    ...
```

---

## Money — read this before your first crawl

**100,000 credits = $1.** Pricing is tiered by page size, which most callers
get wrong:

| Endpoint | page size | $ / 1,000 records |
|---|---|---|
| `followers_ids` | 5,000 | **$0.0045** |
| `followers` (profiles) | 200 | $0.01 |
| `followers` (profiles) | 20 (the default) | $0.03 |

Enumerating a 200M-follower account costs **$900 by IDs**, **$2,000 by
profiles**, or **$6,000 at the default page size** — same data, 6.7x spread.

Three protections are on by default:

- `audience` and the analytical jobs estimate cost up front; `history`
  reports spend per page as it walks (an open-ended search has no upfront
  total).
- `--max-usd` (default **$5**) is a hard ceiling. `overlap` and `authority`
  estimate up front and **refuse before spending anything**.
- A truncated result **exits non-zero** (3) — a partial dataset never looks
  like success.

Two things to know: **billing settles 20–60 seconds late**, so never price a
call by reading your balance right after it; and **you cannot buy a partial
page** — asking for 3,000 IDs still bills the full 5,000-record page.

## Rate limits

QPS scales with your credit balance — **20 is the ceiling**, not the 200 that
gets advertised. A new free-tier account gets **1 request per 5 seconds**. The
client reads your balance and self-throttles; don't override it.

| Balance | QPS |
|---|---|
| free tier | 0.2 |
| ≥ 1,000 credits | 3 |
| ≥ 5,000 credits | 6 |
| ≥ 10,000 credits | 10 |
| ≥ 50,000 credits | 20 |

`Client.bulk_search([q1, q2, q3])` runs N searches in **one** request — the
main lever against that ceiling.

## Caching

Cohort resolution is the expensive part; re-querying it should be free. Graph,
identity and membership endpoints are cached in local sqlite indefinitely;
time-sensitive content (search, replies, timelines) always refetches.

A repeated 10,000-follower crawl costs **$0.045 the first time and $0.00 the
second** — verified in the test suite.

The cache lives at `~/.twitterapi-cache/store.db`. Deleting it is always safe;
it only makes the next crawl paid again.

## Writes are out of scope

Posting, liking, following and DMs are deliberately **not implemented**. They
require handing a third party your X password and 2FA seed plus a purchased
residential proxy, they risk suspension of the authenticating account, and the
vendor documents four mutually inconsistent login flows. Nothing about that
path is verified here, so it is not shipped.

## Verification and staleness

Every claim in [`references/verified-facts.md`](references/verified-facts.md)
was confirmed by a live API call on **2026-08-10**, including prices measured
by balance delta. Where the published OpenAPI spec disagreed with live
responses, the live result was recorded.

**Live APIs drift, and prose cannot notice.** So the facts are stored as
machine-checkable evidence in
[`references/facts.json`](references/facts.json), and one command re-observes
the API and diffs reality against that record:

```bash
python3 scripts/verify.py             # ~$0.002, ~15s. Exit 1 = drift.
python3 scripts/verify.py --pricing   # + re-measure prices by balance delta
python3 scripts/verify.py --update    # re-stamp facts.json after confirming
```

It checks three things: that `facts.json` still agrees with the client's own
endpoint table (offline, free), that live response key sets are unchanged, and
— with `--pricing` — that credits-per-call still match. On drift it names the
endpoint and prints expected-vs-actual.

Four layers keep a stale fact from passing as true:

| Layer | Catches | When |
|---|---|---|
| `verify.py` | contract or price drift | on demand, ~$0.002 |
| Runtime warning | facts older than 90 days | automatically, at first API call |
| `run_e2e.sh` (40 tests) | behavioural regressions | before release, ~$0.12 |
| Weekly CI | drift nobody looked for | Mondays, automatic |

The runtime warning matters most: it fires in the agent's own session, so a
fact that has gone stale announces itself at the point of use instead of
waiting for someone to check a dashboard.

**No key set** → all of the above skip with a notice; they do not fail.

See [`tests/TRIAGE.md`](tests/TRIAGE.md) for failure-class → cause → next-step.

## Known limitations

- **Community IDs are not discoverable.** The all-community firehose returns
  `communityInfo: null`; `community/info` and `community/members` need an ID
  taken from a community URL.
- **`authority()` needs a cohort with internal follow edges.** A handful of
  accounts returns an empty ranking — correctly, but uselessly.
- **Busy-topic narrative tracking costs dollars, not cents.** Bound it with a
  tighter window or an engagement filter.
- **Two prices are unmeasured:** whether a short final page bills as a whole
  page, and `verified_followers` per-record cost. Both are marked in the code.
- **Real-time monitoring endpoints** (`/oapi/x_user_stream/*`) require a paid
  subscription; `workflows.py monitor` polls instead and needs none.

## Your responsibility

This reads X data through an independent third party — not X's official API.
You are responsible for your own compliance with X's terms of service,
twitterapi.io's terms, and any applicable law or data-protection rules in your
jurisdiction. Filter rules bill continuously while active; `realtime.py` prints
the active-rule count on every command so a forgotten rule can't hide.

## Layout

```
SKILL.md                    agent-facing entry point
references/verified-facts.md  live-probe evidence for every claim
scripts/twitterapi.py       transport: casing, envelopes, cost, QPS, cache
scripts/store.py            sqlite cache + normalisers + versioned cohorts
scripts/cohort.py           resolve · set algebra · authority ranking
scripts/jobs.py             8 analytical jobs
scripts/workflows.py        audience · history · monitor
scripts/realtime.py         filter rules + stdlib websocket client
scripts/verify.py           re-probe the API, diff against facts.json
references/facts.json       machine-checkable evidence, dated
tests/ + run_e2e.sh         live E2E suite, cost-capped
tests/TRIAGE.md             failure class -> cause -> next step
```

## License

MIT — see [LICENSE](LICENSE). Not affiliated with or endorsed by twitterapi.io
or X Corp.
