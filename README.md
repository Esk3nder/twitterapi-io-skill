# twitterapi-io

An [Agent Skill](https://docs.claude.com/en/docs/agents-and-tools/agent-skills/overview)
for reading X/Twitter data through [twitterapi.io](https://twitterapi.io) —
for Claude Code, Codex, Cursor, and any agent that loads `SKILL.md`.

**Invoke it as `/twitterapi-io`, or just mention an account.** The skill
advertises its own triggers, so an agent loads it on its own when you write
`@jack`, paste an `x.com` link or a tweet ID, or ask anything about followers,
tweet history, or how a post spread. The slash command is there for when you
want to force it or read what it says.

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

`verify.py` distinguishes the two failure modes: **exit 2** means a setup
problem (bad key, no credits, rate limited) with the cause named, **exit 1**
means the API itself has drifted from the recorded facts. See
[`tests/TRIAGE.md`](tests/TRIAGE.md).

**Platform:** developed and tested on macOS and Linux. The Python is portable,
but `run_e2e.sh` is a bash script and the setup lines above assume a POSIX
shell — on Windows use WSL, or set the environment variable your own way and
call the Python entry points directly.

**To remove it:** delete the clone and the symlink; the skill keeps no state
elsewhere except the cache.

```bash
rm -rf ~/.claude/skills/twitterapi-io ~/.agents/skills/twitterapi-io
rm -rf ~/.twitterapi-cache          # optional: the local data cache
```

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
  --exclude-replies --out tweets.jsonl
# `from:` omits native retweets; opt into the second, deduplicated search pass
python3 scripts/workflows.py history openai --include-retweets --out tweets.jsonl

# poll for new posts; --state makes it resumable across restarts
python3 scripts/workflows.py monitor openai,elonmusk --interval 60 --state s.json

# analytical jobs, JSON to stdout — cents unless marked
python3 scripts/jobs.py brief openai --days 7            # ~$0.01
python3 scripts/jobs.py authenticity someaccount --sample 500   # ~$0.01
python3 scripts/jobs.py diffusion 2084352161404920316    # ~$0.02
python3 scripts/jobs.py benchmark anthropicai,openai --days 30  # ~$0.02
python3 scripts/jobs.py drift my_cohort                  # $0, reads the store
python3 scripts/jobs.py catalogue openai                 # $0, cached summary
python3 scripts/jobs.py threads openai                   # $0, cached threads

# these two crawl graphs — DOLLARS, not cents. Start with a low ceiling.
python3 scripts/jobs.py authority "crypto AI" --max-usd 1   # ~$1.50 uncapped
python3 scripts/jobs.py overlap stripe vercel --max-usd 1   # scales with both
                                                            # follower counts
```

`--max-usd` (default $5) caps every command. **Exit codes:** `0` complete for
the stated scope · `2` refused before spending · `3` partial because a spend,
pagination, timestamp, page-cap, or search-index boundary prevented a complete
result.

Add `--out FILE` to stream JSONL instead of stdout, and `--max-pages N` to
bound an open-ended history walk. The page cap applies separately to each
search pass; `--include-retweets --max-pages N` can therefore fetch up to
`2N` pages total.

## 5. Python API

The scripts are thin wrappers; the library is the real surface.

```python
import sys; sys.path.insert(0, "scripts")
from twitterapi import Client, IncompleteDataError
from store import Store
from cohort import Cohort

s = Store()
c = Client(store=s, max_usd=10)            # caching on, hard ceiling

c.estimate("follower_ids", 200_000_000)    # -> 900.0 dollars, before spending
c.bulk_search(["from:openai", "from:anthropicai", "from:google"])  # 3 queries, 1 request
c.paginate("community_members", "1493446837214187523", limit=500)  # one of the 27 paginated endpoints
```

Pagination and history calls raise `IncompleteDataError` when the API claims
more data exists but omits a required field/cursor, a time window cannot split
a busy second, a page cap stops the walk, or another safe completion boundary
is unavailable. Treat records already yielded as valid but partial; do not turn
the exception into a zero count.

**Cohorts** are named, versioned account sets — the reusable asset. Build one,
save it, and every later question over it is free:

```python
team = Cohort.from_search("polymarket", limit=200, client=c, store=s)
profiles = team.hydrate(max_usd=1)          # returns profiles; fills handles
team = team.filter(bio_matches=r"polymarket", min_followers=500)
team.save("pm_team")                            # versioned

# set algebra across cohorts, matching on id OR handle
both = Cohort.load("pm_talkers", client=c, store=s).intersect(
    Cohort.load("crypto_ai", client=c, store=s))

# reuse follower IDs already present in paid cached pages; no API call
followers = Cohort.from_ids(s.follower_ids_for("openai"), client=c, store=s)

# who does this scene itself follow? in-degree over its own follow graph
Cohort.from_search("crypto AI", limit=150, client=c).authority(max_usd=5).top(20)
```

Resolvers: `from_ids`, `from_account`, `from_search`, `from_engagers`,
`from_community`, `from_list`. Combine with `union` / `intersect` / `minus`.

**The store** answers questions over already-paid-for data at no cost:

```python
s.tweets_for(user_names=["openai"], since=1780000000)   # cached posts authored by these accounts; free
s.follower_ids_for("openai")                            # paid crawl pages, free
s.list_cohorts(); s.cohort_versions("pm_team")          # what you've saved
s.delete_cohort("test_cohort")                          # members + metadata
s.stats()                                               # rows held, credits saved
```

`tweets_for(...)` filters by tweet author. Cached mentions *of* an account are
authored by other users and therefore are not returned when filtering for the
mentioned account's handle.

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
- `Client.estimate()` is a conservative **upper bound**, not a prediction of
  actual history spend. Deleted posts, query filters, native-retweet exclusion,
  and finite search-index depth can make the reachable corpus smaller.

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

Every workflow's final `spend_report()` names both session spend and, when
present, cache hits plus dollars saved. A `$0.00` rerun with saved credits means
the cache worked; it is not missing accounting.

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
| `run_e2e.sh` (80 tests) | behavioural regressions | before release, ~$0.12 |
| Weekly CI | drift nobody looked for | Mondays, automatic |

Without a key, all of these skip with a notice rather than failing.
See [`tests/TRIAGE.md`](tests/TRIAGE.md) for failure class → cause → next step.

## Known limitations

- **`from:USER` search excludes native retweets.** Default `history` output is
  originals + replies + quote tweets and prints that scope. Pass
  `--include-retweets` for a second `filter:nativeretweets` pass under the same
  spend ceiling; results are deduplicated by tweet id.
- **Search-index depth is not account age.** An open-ended history may stop
  years after the account was created even when the profile states more posts.
  `history` compares the oldest reached date with profile `createdAt`, prints
  `INDEX COVERAGE: PARTIAL`, and exits `3` when lifetime coverage cannot be
  established. `statusesCount`-based prices are therefore upper bounds.
- **Cached thread reconstruction is scoped, not omniscient.** `jobs.py threads`
  groups that handle's cached posts by `conversationId`; external participants
  and uncached or deleted posts may be absent, and the output says so.
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
scripts/jobs.py               analytical jobs, including $0 catalogue/threads
scripts/workflows.py          audience, history, monitor
scripts/realtime.py           filter rules + stdlib websocket client
scripts/verify.py             re-probe the API, diff against facts.json
tests/ + run_e2e.sh           cost-capped deterministic + live E2E suite
tests/TRIAGE.md               failure class -> cause -> next step
```

## License

MIT — see [LICENSE](LICENSE). Not affiliated with or endorsed by twitterapi.io
or X Corp.
