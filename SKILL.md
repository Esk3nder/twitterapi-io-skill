---
name: twitterapi-io
description: Use when reading X/Twitter data — followers or an audience graph, a user's tweet history over a date range, searching tweets with X operators, monitoring accounts for new posts, or streaming matched tweets — and whenever a request names an X/Twitter @handle, an x.com or twitter.com link, or a tweet ID and asks what was posted, who follows whom, or how something spread. Also for twitterapi.io, api.twitterapi.io, or x-api-key REST calls without the official X developer portal.
---

# twitterapi.io

Read-only access to X data. Standard library only; nothing to install.

Key in `TWITTERAPI_IO_KEY` (environment, never hardcoded). Scripts in `scripts/`.

Use the workflows and jobs for their documented end-to-end tasks. Use the
`Client`, `Cohort`, and `Store` library surface for composition, cache reuse,
and endpoints without a dedicated workflow. Do not hand-roll HTTP requests:
parameter casing, response envelopes, pagination, and pricing differ per
endpoint, and `Client` encodes the measured contract.

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
| Summarise a cached account corpus ($0) | `jobs.py catalogue USER` |
| Reconstruct cached conversations ($0) | `jobs.py threads USER` |
| List cached media URLs ($0) | `jobs.py media USER` |
| Replies, quotes, lists, trends, bulk fan-out | `import twitterapi.Client` |

```bash
python3 scripts/workflows.py --max-usd 5 audience elonmusk --limit 100000
python3 scripts/jobs.py brief openai --days 7
```

`--max-usd` (default $5) is a hard ceiling on every command.

**Exit codes:** `0` valid complete or valid partial result · `2` usage, refusal,
or setup failure · `1` runtime, API, or internal failure. An exit 0 is not proof
of completeness: inspect `complete` and `completeness.status`. Partial payloads
repeat the signal as `complete: false` and `status: partial`, and include a
reason, records returned, and an actionable resume command.

Stdout is JSON (or JSON Lines for streaming records); progress, spend, and
warnings stay on stderr. Lists default to brief fields. Use `--fields <a,b,c>`
for additional supported fields and `--full` for complete long text. Default
text previews end with `... (truncated, N chars total)`, and bounded lists state
the displayed and total counts. Use `<command> --help` for each command's exact
flags and examples; `-v`, `-V`, and `--version` print only the version.

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

`Client.estimate()` gives a conservative upper bound before spending, not a
prediction. Deleted/filtered posts and finite search-index depth can make a
history cost less. Confirm with the user before any crawl over a few dollars.

`overlap` and `authority` estimate up front and refuse rather than start an
unaffordable crawl. `authority` uses a conservative 2,000-followings/member
estimate and prints member progress plus running spend to stderr. It returns
`complete: false` plus `completeness.status: partial` when the ceiling truncates
its follow-graph walk — do not present a partial frontier as whole even though
the valid partial payload exits 0. `--sample`
limits the authority cohort (or the authenticity tweet sample) and is rejected
for other jobs.

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
`last_tweets` is originals-oriented; `advanced_search from:USER` includes
replies, so equal-sized results can have little overlap without data loss.

**Advanced-search date and engagement operators need correction.** Bare
`since:YYYY-MM-DD` and `until:YYYY-MM-DD` are normalized to exact UTC midnight.
The API's `min_faves` index snapshot can lag its own returned `likeCount`, so
the client removes that operator and filters locally. This is exact but may
fetch more rows. For a positive threshold, `limit` bounds qualifying rows, not
paid rows scanned: pass an explicit `max_usd` ceiling or the client refuses
before transport, and expect a scan warning on stderr.

**Tweet media is evidence, not decoration.** Every tweet returned by the
client has a first-class `media` list with `type`, `url`, `alt_text`, and a
`full_resolution_url`; the store persists that list separately from opaque
`raw` JSON. The wire source is `extendedEntities.media`, with the CDN URL at
`media_url_https`. For photos, full resolution is
`?format=jpg&name=4096x4096` (the format follows the source extension).
`pbs.twimg.com` returns 403 to observed bare urllib downloads, so send a
browser `User-Agent` header when fetching directly. The skill inventories
media URLs but does not download or interpret bytes: `brief`, `catalogue`, and
`threads` report the exact undisplayed-media count, while `jobs.py media USER`
lists the cached artifacts.

**Historical search must not use cursors.** `advanced_search` caps at 20 per
call and cursors loop on old ranges. Walk the window: take the oldest
`createdAt`, set `until_time` to it, dedupe by id. `history_search()` does
this; getting the boundary wrong drops ~10% of results.
Identical searches are not guaranteed to return identical ID sets: the
upstream index has intermittently omitted ordinary mid-window tweets. Treat
counts as index observations, not deterministic ground truth.

**A single second can hold more than 20 tweets**, and no time window pages
inside one second. `history_search` reports `INCOMPLETE` with the affected
timestamps rather than dropping them silently.

**`from:USER` excludes native retweets.** Default `history` is originals +
replies + quote tweets and prints that scope. Pass `--include-retweets` for a
second `filter:nativeretweets` pass under the same ceiling. The two passes are
deduplicated by id. For the USER shorthand, `--exclude-retweets` is retained
only as an explicit no-op.
`--max-pages N` applies separately to each search pass even if the first pass
hits its cap, so including native retweets can fetch up to `2N` pages total.
With a raw `--query`,
`--exclude-retweets` is refused before spending because the query itself may
explicitly select native retweets.

**Search-index depth is not account age.** For an open-ended handle history,
the workflow compares the oldest reached tweet with the author's profile
`createdAt`. If lifetime coverage cannot be established it prints
`INDEX COVERAGE: PARTIAL` and emits an explicit partial completeness object;
do not call the result a full archive.

`IncompleteDataError` means records already returned are valid but the client
cannot prove the requested result complete (contract drift, cursor failure,
unsplittable timestamp, page cap, or equivalent). Propagate it or label the
result partial; never convert it to an empty list or confident count.

## Optional session context

This `SKILL.md` is the on-demand integration. For ambient local state at every
session start, the user may explicitly run `python3 scripts/jobs.py setup`.
That installs or repairs directory-scoped SessionStart and SessionEnd integration
for Claude Code, Codex, and OpenCode; ordinary commands never install anything.
Use `--agent <claude|codex|opencode>` to select targets or `--scope user` for
user scope. The hook command is path-repaired on repeat setup and captures only
scoped timestamps, working directory, and changed file paths at session end.

**Communities and lists do not provide general ID discovery.**
`community_tweets_all` returns tweets across communities, not communities;
`community_search` is a warning compatibility alias. Obtain IDs from
`x.com/i/communities/<id>` or `x.com/i/lists/<id>`. A null community identity
raises. Empty list pages also raise because this API cannot distinguish empty,
private, and nonexistent lists.

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

`Client.spend_report()` includes cache hits and dollars saved when nonzero.
`$0.00` spent plus saved credits is a successful cached rerun, not a billing bug.
`jobs.py catalogue USER` and `jobs.py threads USER` are pure local computations
over cached tweet rows and make no API calls. Thread output covers only cached
posts by that handle; external and uncached posts may be absent. Both state how
many referenced media artifacts remain undownloaded and uninterpreted;
`jobs.py media USER` exposes their URLs from the same local store.
`workflows.py history` persists each paid page into that cache before yielding
it; use `history ... --store FILE` when the store path must be explicit.

```python
from cohort import Cohort
from twitterapi import Client
from store import Store
s = Store()
c = Client(store=s, max_usd=10)
# A filesystem path is also accepted and converted before any paid request:
c2 = Client(store="/tmp/twitter.db", max_usd=10)
Cohort.from_search("polymarket", client=c, store=s).save("pm_talkers")
both = Cohort.load("pm_talkers", client=c, store=s).intersect(
    Cohort.load("crypto_ai", client=c, store=s))
```

Use `Cohort.from_ids(s.follower_ids_for("account"), client=c, store=s)` to
compose a cohort from follower IDs already bought. `hydrate(max_usd=...)`
returns the fetched profiles and fills the cohort's member handles.

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
