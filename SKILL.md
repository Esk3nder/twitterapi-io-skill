---
name: twitterapi-io
description: Use when reading X/Twitter data — followers or an audience graph, a user's tweet history over a date range, searching tweets with X operators, monitoring accounts for new posts, or streaming matched tweets — and whenever a request names an X/Twitter @handle, an x.com or twitter.com link, or a tweet ID and asks what was posted, who follows whom, or how something spread. Also for twitterapi.io, api.twitterapi.io, or x-api-key REST calls without the official X developer portal.
---

# twitterapi.io

Read-only access to X data. Standard library only; nothing to install.

Key in `TWITTERAPI_IO_KEY` (environment, never hardcoded). Scripts in `scripts/`.

**Run the workflows; do not hand-roll requests.** Parameter casing, response
envelopes, pagination and pricing all differ per endpoint, and `Client` encodes
the correct answer for each.

## Which command

| Need | Command |
|---|---|
| Followers / audience | `workflows.py audience USER` |
| Tweet history, date-ranged search | `workflows.py history USER --since D --until D` |
| Watch accounts for new posts | `workflows.py monitor U1,U2 --interval 60` |
| Push delivery of matched tweets | `realtime.py rules add` then `realtime.py stream` |
| Entity state | `jobs.py brief USER --days 30` |
| What's emerging in a topic | `jobs.py narrative "QUERY" --days 30` |
| Who a scene follows (frontier) | `jobs.py authority "QUERY"` |
| Shared followers of two accounts | `jobs.py overlap A B` |
| Organic-vs-bought audience signals | `jobs.py authenticity USER` |
| Who moved early on a post | `jobs.py diffusion TWEET_ID` |
| N entities on shared axes | `jobs.py benchmark a,b,c --days 30` |
| Who joined/left a saved cohort | `jobs.py drift COHORT_NAME` |
| Replies, quotes, lists, trends, bulk fan-out | `import twitterapi.Client` |

```bash
python3 scripts/workflows.py --max-usd 5 audience elonmusk --limit 100000
python3 scripts/jobs.py brief openai --days 7
```

`--max-usd` (default $5) is a hard ceiling on every command.

**Exit codes:** `0` complete · `2` refused before spending · `3` truncated by
the ceiling — the dataset is partial, not broken.

Jobs return JSON for you to interpret. They compute who and what; they do not
compute themes, consensus, or whether engagement is organic. Do not present a
job's output as if it contained a judgment it does not make.

## Cost

100,000 credits = $1. Billing settles 20–60s after a call, so reading the
balance immediately after a request shows no change.

| Endpoint | page size | $/1,000 |
|---|---|---|
| `followers_ids` | 5,000 | **$0.0045** |
| `followers` (profiles) | 200 | $0.01 |
| `followers` (profiles) | 20 (default) | $0.03 |

1. **Use `followers_ids` unless you need profile fields.** For an account with
   200M followers: **$900** by IDs, **$2,000** by profiles, **$6,000** at the
   default page size.
2. **Always request the maximum page size** — same data, up to 6.7x the price.
3. **You cannot buy a partial page.** Asking for 3,000 IDs bills the full
   5,000-record page.

`Client.estimate()` gives a figure before spending. Confirm with the user
before any crawl over a few dollars.

`overlap` and `authority` estimate up front and refuse rather than start an
unaffordable crawl. `authority` returns `complete: false` when the ceiling
truncated its follow-graph walk — do not present a partial frontier as whole.

## Rate limits

QPS scales with credit balance; **20 is the ceiling**. A zero-balance account
gets 1 request per 5 seconds. `Client` derives the rate from `/oapi/my/info`;
do not override it.

Requests, not credits, bound large jobs. Two levers:

- `Client.bulk_search([q1, q2, q3])` runs N searches in one request.
- Bigger pages: `follower_ids` returns 5,000/call against `followers`' 200;
  `replies/v2` returns ~35 against `replies`' 20.

## Traps that silently return wrong data

**`last_tweets` and `tweet_timeline`** nest tweets at `data.tweets` but keep
`has_next_page`/`next_cursor` at the top level. Reading pagination from inside
`data` stops after 20 records and looks successful.

**Historical search must not use cursors.** `advanced_search` caps at 20 per
call and cursors loop on old ranges. Walk the window: take the oldest
`createdAt`, set `until_time` to it, dedupe by id. `history_search()` does
this; getting the boundary wrong drops ~10% of results.

**A single second can hold more than 20 tweets**, and no time window pages
inside one second. `history_search` reports `INCOMPLETE` with the affected
timestamps rather than dropping them silently.

**Failures can arrive as HTTP 200** with `{"status": "error", ...}` in the
body. Check the body, not just the status code; `Client` does.

**Filter rules are created inactive** (`is_effect: 0`) and bill only once
activated via `update_rule(is_effect=1)`. Activating backfills recent tweets
rather than streaming only new ones. An active rule bills whether or not
anything is connected; `realtime.py` prints the active-rule count on every
command.

**`diffusion` time-orders replies and quotes only.** Retweeters return as
profiles with no retweet timestamp, so they are counted separately. Ordering
them by their profile `createdAt` invents a false timeline.

**`msg` everywhere except `check_follow_relationship`**, which uses `message`.

## Caching and composition

Resolving a cohort costs money; re-querying it is free. Graph and membership
endpoints cache indefinitely; content endpoints always refetch.

```python
from cohort import Cohort
from twitterapi import Client
from store import Store
c = Client(store=Store(), max_usd=10)
Cohort.from_search("polymarket", client=c).save("pm_talkers")
both = Cohort.load("pm_talkers", client=c).intersect(Cohort.load("crypto_ai", client=c))
```

## Not implemented

Posting, liking, following and DMs. They require the account's X password and
TOTP seed plus a purchased proxy, and risk suspension of that account. If a
user asks, explain the tradeoff rather than improvising a request.

`/oapi/x_user_stream/*` needs a separate paid subscription; `monitor` polls
instead and needs none.

## When a fact here looks wrong

Prices and response shapes are dated observations. `python3 scripts/verify.py`
(~$0.002, 15s) re-probes the API and exits non-zero naming any endpoint that
has drifted. Confirm, then update `references/facts.json` and the prose that
repeats it — do not code around a mismatch.

For an endpoint neither `facts.json` nor `references/verified-facts.md` covers,
probe it: a GET with no params returns 400 if the path exists, 404 if it does
not. The published OpenAPI spec disagrees with observed behavior on response
shapes and required parameters; prefer a live probe over it.
