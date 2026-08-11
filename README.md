# twitterapi-io

An [Agent Skill](https://docs.claude.com/en/docs/agents-and-tools/agent-skills/overview)
for reading X/Twitter data through [twitterapi.io](https://twitterapi.io) —
for Claude Code, Codex, Cursor, and any agent that loads `SKILL.md`.

**Read-only. Standard library only — nothing to `pip install`.** 31 endpoints,
with every path, parameter spelling, response shape and price confirmed by live
API calls and re-checkable in 15 seconds.

## What you get

| Question | How | Cost |
|---|---|---|
| "All of @jack's follower IDs" | cheapest endpoint, 5,000/request | **$0.0045 / 1k** |
| "Every tweet @openai posted in June" | sliding time window, deduped | ~$0.02 |
| "Who follows both @stripe and @vercel?" | crawl both once, intersect | **$0 on re-run** |
| "Who does the crypto-AI scene actually follow?" | follow-graph in-degree ranking | ~$3 / 300 accounts |
| "Is this account's audience real?" | account ages, empty-profile rate | ~$0.01 |
| "Who reacted first to this post?" | repliers + quoters, time-ordered | ~$0.02 |
| "Find people whose bio mentions X" | cohort + regex filter | free after crawl |

Two properties make the difference in practice: **crawls are cached**, so the
expensive part is paid once and every follow-up question over the same data is
free; and **cost is estimated before spending**, so an unaffordable request is
refused with a price rather than a surprise bill.

## 1. Get an API key

Sign up at **https://twitterapi.io/dashboard**, copy the key, and put it in your
environment — never in a file, never in a commit:

```bash
echo 'export TWITTERAPI_IO_KEY="your_key_here"' >> ~/.zshenv   # zsh
echo 'export TWITTERAPI_IO_KEY="your_key_here"' >> ~/.bashrc   # bash
```

Use `~/.zshenv` rather than `~/.zshrc`: it loads for non-interactive shells,
which is how agents invoke things. Open a new shell and check:

```bash
curl -sS -H "x-api-key: $TWITTERAPI_IO_KEY" https://api.twitterapi.io/oapi/my/info
# -> {"recharge_credits": 12345, "total_bonus_credits": 0}
```

That balance call is free. The key is the only credential used here — it never
asks for your X password and cannot post, like, follow, or DM.

## 2. Install

```bash
git clone https://github.com/Esk3nder/twitterapi-io-skill.git \
  ~/.claude/skills/twitterapi-io
```

For Codex / Cursor / other runtimes, symlink so there is one source of truth:

```bash
mkdir -p ~/.agents/skills
ln -s ~/.claude/skills/twitterapi-io ~/.agents/skills/twitterapi-io
```

Restart your agent session. Requires **Python 3.11+**. Then confirm the install
and key together:

```bash
cd ~/.claude/skills/twitterapi-io && python3 scripts/verify.py
# -> All recorded facts still hold.        (~$0.002, ~15s)
```

Non-zero exit means something is wrong — see [`tests/TRIAGE.md`](tests/TRIAGE.md).

## 3. Use it from your agent

The intended path. Ask in plain language; the agent reads `SKILL.md` and runs
the right command with the right cost guard.

> "Get me @jack's follower IDs"
> "What has @openai tweeted in the last month?"
> "Who follows both @stripe and @vercel?"
> "Is @someaccount's audience organic?"
> "Who reacted first to this tweet?"

Expect two behaviors. **It refuses expensive things and quotes the price** —
ask for a 200M-follower crawl and you get "this is ~$900, over the $5 ceiling"
instead of a bill. **It won't invent conclusions** — jobs return structured
data; any judgment about themes or authenticity is the model reading real
signals, not a score the API returned.

## 4. Command line

> **On a new account the first runs are slow, and that is correct.** Rate limit
> scales with credit balance: a near-zero balance is **one request every 5
> seconds**. Add credit and it rises to 20/second.

```bash
cd ~/.claude/skills/twitterapi-io

# follower IDs — the cheap path (~$0.02 for 5k)
python3 scripts/workflows.py --max-usd 1 audience jack --limit 5000
python3 scripts/workflows.py audience jack --profiles      # full profiles, 2.2x
python3 scripts/workflows.py audience jack --verified-only # verified subset

# history over a date range, streamed to JSONL
python3 scripts/workflows.py history openai --since 2026-06-01 --until 2026-07-01 \
  --exclude-replies --exclude-retweets --out tweets.jsonl

# poll for new posts; --state makes it resumable across restarts
python3 scripts/workflows.py monitor openai,elonmusk --interval 60 --state s.json

# analytical jobs, JSON to stdout
python3 scripts/jobs.py brief openai --days 7
python3 scripts/jobs.py overlap stripe vercel
python3 scripts/jobs.py authority "crypto AI"
python3 scripts/jobs.py authenticity someaccount --sample 500
python3 scripts/jobs.py diffusion 2084352161404920316
python3 scripts/jobs.py benchmark anthropicai,openai --days 30
python3 scripts/jobs.py drift my_cohort
```

`--max-usd` (default $5) caps every command. **Exit codes:** `0` complete ·
`2` refused before spending · `3` truncated by the ceiling, so the output is
partial rather than wrong.

Add `--out FILE` to stream JSONL instead of stdout, and `--max-pages N` to
bound an open-ended history walk.

## 5. Python API

The scripts are thin wrappers; the library is the real surface.

```python
import sys; sys.path.insert(0, "scripts")
from twitterapi import Client
from store import Store
from cohort import Cohort

c = Client(store=Store(), max_usd=10)      # caching on, hard ceiling

c.estimate("follower_ids", 200_000_000)    # -> 900.0 dollars, before spending
c.bulk_search(["from:openai", "from:anthropicai", "from:google"])  # 3 queries, 1 request
c.paginate("community_members", "1493446837214187523", limit=500)  # any of 31 endpoints
```

**Cohorts** are named, versioned account sets — the reusable asset. Build one,
save it, and every later question over it is free:

```python
team = (Cohort.from_search("polymarket", limit=200, client=c)
        .hydrate()                              # IDs -> full profiles
        .filter(bio_matches=r"polymarket", min_followers=500))
team.save("pm_team")                            # versioned

# set algebra across cohorts, matching on id OR handle
both = Cohort.load("pm_talkers", client=c).intersect(Cohort.load("crypto_ai", client=c))

# who does this scene itself follow? in-degree over its own follow graph
Cohort.from_search("crypto AI", limit=150, client=c).authority(max_usd=5).top(20)
```

Resolvers: `from_account`, `from_search`, `from_engagers`, `from_community`,
`from_list`. Combine with `union` / `intersect` / `minus`.

**The store** answers questions over already-paid-for data at no cost:

```python
s = Store()
s.tweets_for(user_names=["openai"], since=1780000000)   # cached corpus, free
s.list_cohorts(); s.cohort_versions("pm_team")          # what you've saved
s.stats()                                               # rows held, credits saved
```

## Money

**100,000 credits = $1.** Price scales with page size — the single most
expensive mistake is using the default:

| Endpoint | page size | $/1,000 |
|---|---|---|
| `followers_ids` | 5,000 | **$0.0045** |
| `followers` (profiles) | 200 | $0.01 |
| `followers` (profiles) | 20 (default) | $0.03 |

For a 200M-follower account: **$900** by IDs, **$2,000** by profiles, **$6,000**
at the default page size. Same data.

- Use `followers_ids` unless you need profile fields.
- Always request the maximum page size.
- You cannot buy a partial page — 3,000 IDs bills the full 5,000-record page.
- Billing settles 20–60s late, so a balance read straight after a call shows no
  change. Use `client.spend_report()` for the live figure.

`overlap` and `authority` estimate first and refuse rather than start a crawl
they cannot afford. `authority` returns `complete: false` if a ceiling cut its
walk short.

## Rate limits

QPS scales with credit balance; **20 is the ceiling**.

| Balance | QPS |
|---|---|
| near zero | 0.2 |
| ≥ 1,000 credits | 3 |
| ≥ 5,000 credits | 6 |
| ≥ 10,000 credits | 10 |
| ≥ 50,000 credits | 20 |

Requests, not credits, bound large jobs. `bulk_search` runs N queries in one
request; bigger pages cut request count (`follower_ids` 5,000 vs `followers`
200; `replies/v2` ~35 vs `replies` 20).

## Caching

Graph and membership endpoints cache indefinitely in local sqlite; content
endpoints always refetch. A repeated 10,000-follower crawl costs **$0.045 the
first time and $0.00 the second** — asserted in the test suite.

The cache lives at `~/.twitterapi-cache/store.db`. Deleting it is safe; it only
makes the next crawl paid again.

## Writes are out of scope

Posting, liking, following and DMs are not implemented. They require handing a
third party your X password and 2FA seed plus a purchased residential proxy,
and risk suspension of the authenticating account. None of that path is
verified here, so it is not shipped.

## Verification and staleness

Prices and response shapes are dated observations, recorded as machine-checkable
evidence in [`references/facts.json`](references/facts.json).

```bash
python3 scripts/verify.py             # ~$0.002, ~15s. Exit 1 = drift.
python3 scripts/verify.py --pricing   # also re-measures prices by balance delta
python3 scripts/verify.py --update    # re-stamp after confirming
```

It checks that the evidence file still agrees with the client's endpoint table
(offline, free), that live response key sets are unchanged, and optionally that
credits-per-call still match. On drift it names the endpoint with
expected-vs-actual.

Four layers keep a stale fact from passing as true:

| Layer | Catches | When |
|---|---|---|
| `verify.py` | contract or price drift | on demand, ~$0.002 |
| Runtime warning | facts older than 90 days | at first API call |
| `run_e2e.sh` (40 tests) | behavioural regressions | before release, ~$0.12 |
| Weekly CI | drift nobody looked for | Mondays, automatic |

Without a key, all of these skip with a notice rather than failing.
See [`tests/TRIAGE.md`](tests/TRIAGE.md) for failure class → cause → next step.

## Known limitations

- **Community IDs are not discoverable** — the all-community search returns
  `communityInfo: null`. `community/info` and `community/members` need an ID
  from a community URL.
- **`authority()` needs a cohort with internal follow edges.** A handful of
  accounts returns an empty ranking, correctly but uselessly.
- **Busy-topic narrative tracking costs dollars, not cents.** Bound it with a
  tighter window or an engagement filter.
- **Two prices are unmeasured:** whether a short final page bills as a whole
  page, and `verified_followers` per-record cost. Both are marked in the code.
- **Per-user monitoring endpoints** (`/oapi/x_user_stream/*`) need a separate
  paid subscription; `monitor` polls instead and needs none.
- **The websocket client** is verified through handshake and event delivery;
  sustained multi-hour streaming is not covered by the test suite.

## Your responsibility

This reads X data through an independent third party, not X's official API. You
are responsible for your own compliance with X's terms, twitterapi.io's terms,
and any applicable law or data-protection rules. Filter rules bill continuously
while active; `realtime.py` prints the active-rule count on every command.

## Layout

```
SKILL.md                      agent-facing entry point
references/facts.json         machine-checkable evidence, dated
references/verified-facts.md  prose evidence and rationale
scripts/twitterapi.py         31 endpoints: casing, envelopes, cost, QPS, cache
scripts/store.py              sqlite cache, normalisers, versioned cohorts
scripts/cohort.py             resolvers, set algebra, authority ranking
scripts/jobs.py               8 analytical jobs
scripts/workflows.py          audience, history, monitor
scripts/realtime.py           filter rules + stdlib websocket client
scripts/verify.py             re-probe the API, diff against facts.json
tests/ + run_e2e.sh           40-test live E2E suite, cost-capped
tests/TRIAGE.md               failure class -> cause -> next step
```

## License

MIT — see [LICENSE](LICENSE). Not affiliated with or endorsed by twitterapi.io
or X Corp.
