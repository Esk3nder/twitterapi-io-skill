---
name: twitterapi-io
description: Use when reading X/Twitter data — followers or an audience graph, a user's tweet history over a date range, searching tweets with X operators, monitoring accounts for new posts, or streaming matched tweets — and whenever a request names an X/Twitter @handle, an x.com or twitter.com link, or a tweet ID — whether it asks what was posted, who follows whom, and how something spread, or just drops the handle or link with no question attached. Also for twitterapi.io, api.twitterapi.io, or x-api-key REST calls without the official X developer portal.
---

# twitterapi.io

Read-only access to X data. Standard library only; nothing to install.

Key in `TWITTERAPI_IO_KEY` (environment, never hardcoded). Scripts in `scripts/`.

Use the workflows and jobs for their documented end-to-end tasks. Use the
`Client`, `Cohort`, and `Store` library surface for composition, cache reuse,
and endpoints without a dedicated workflow. Do not hand-roll HTTP requests:
parameter casing, response envelopes, pagination, and pricing differ per
endpoint, and `Client` encodes the measured contract.

## When this fires

A bare `@handle`, an x.com link, or a tweet ID dropped with no question attached
is a lookup request. Default to `jobs.py brief USER --days 30` and report what
it returns.

Confirm the platform first when the handle is ambiguous. `@name` also addresses
people on GitHub, Slack, and Linear, and the same string is often the same
person on several of them. If the surrounding message does not settle which
network is meant, ask before buying an X lookup for the wrong one.

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
| Who joined/left a saved cohort (free, cache only) | `jobs.py drift COHORT_NAME` |
| Summarise a cached account corpus (free, cache only) | `jobs.py catalogue USER` |
| Reconstruct cached conversations (free, cache only) | `jobs.py threads USER` |
| List cached media URLs (free, cache only) | `jobs.py media USER` |
| Replies, quotes, lists, trends, bulk fan-out | `import twitterapi.Client` |

```bash
python3 scripts/workflows.py --max-usd 5 audience elonmusk --limit 100000
python3 scripts/jobs.py brief openai --days 7
```

`--max-usd` (default USD 5) is a hard ceiling on every paid `jobs.py` and
`workflows.py` command. It may appear before or after the command name.

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

100,000 credits = USD 1. Billing settles 20–60s after a call, so reading the
balance immediately after a request shows no change.

| Endpoint | page size | USD per 1,000 records |
|---|---|---|
| `followers_ids` | 5,000 | **0.0045** |
| `followers` (profiles) | 200 | 0.01 |
| `followers` (profiles) | 20 (default) | 0.03 |

1. **Use `followers_ids` unless you need profile fields.** For an account with
   200M followers: **USD 900** by IDs, **USD 2,000** by profiles, **USD 6,000** at the
   default page size.
2. **Always request the maximum page size** — same data, up to 6.7x the price.
3. **You cannot buy a partial page.** Asking for 3,000 IDs bills the full
   5,000-record page.

`Client.estimate()` prices the requested records at full pages. It is not an
upper bound: the server decides how full each page is, and short or empty pages
still bill the 15-credit request floor. `max_usd` is the only hard bound; every
request books its worst-case cost against it before it is sent. Deleted or
filtered posts and finite search-index depth can make a history cost less.
Confirm with the user before any crawl over a few dollars.

`overlap` and `authority` estimate up front and refuse (exit 2) rather than
start an unaffordable crawl. `overlap` prices only follower pages not already
cached, so a cached rerun is free. `authority` uses a conservative
2,000-followings/member estimate and prints member progress plus running spend
to stderr.

When a job hits the ceiling, or cannot prove a walk complete
(`IncompleteDataError`), after money was spent, it returns the rows it already
bought as a valid partial (exit 0, `complete: false`, with `spend` and a resume
command). An API or network error is a partial only when it interrupts a
paginated leg that already returned rows (`overlap`, `diffusion`,
`authenticity`, `authority`); on a first request, or in the search walk behind
`brief`, `narrative`, and `benchmark`, it exits 1. Either way, pages already
bought stay in the cache, so a rerun does not pay for them again. `authority` is partial when the ceiling truncates
its follow-graph walk or any member cannot be crawled; `diffusion` is partial
when a leg reaches `--limit` (default 500). Do not present a partial result as
whole. `--sample` limits the authority seed-search tweets (so the cohort has at
most that many members) or the number of follower profiles `authenticity`
samples, and is rejected for other jobs.

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
fetch more rows. It applies only when every `min_faves` clause sits at the top
level with no top-level `OR`; a clause under `OR`, inside a group, or negated is
refused rather than rewritten into a different query. For a positive threshold, `limit` bounds qualifying rows, not
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
deduplicated by id. `--include-retweets` needs the USER shorthand; a raw
`--query` states its own retweet scope.
`--max-pages N` applies separately to each search pass even if the first pass
hits its cap, so including native retweets can fetch up to `2N` pages total.

**Search-index depth is not account age.** For an open-ended handle history,
the workflow compares the oldest reached tweet with the author's profile
`createdAt`. If lifetime coverage cannot be established it prints
`INDEX COVERAGE: PARTIAL` and emits an explicit partial completeness object;
do not call the result a full archive.

`IncompleteDataError` means records already returned are valid but the client
cannot prove the requested result complete (contract drift, cursor failure,
unsplittable timestamp, page cap, or equivalent). Propagate it or label the
result partial; never convert it to an empty list or confident count.

**Communities and lists do not provide general ID discovery.**
`community_tweets_all` returns tweets across communities, not communities.
Obtain IDs from
`x.com/i/communities/<id>` or `x.com/i/lists/<id>`. A null community identity
raises. Empty list pages also raise because this API cannot distinguish empty,
private, and nonexistent lists.

**Failures can arrive as HTTP 200** with `{"status": "error", ...}` in the
body. Check the body, not just the status code; `Client` does.

**Filter rules are created inactive** (`is_effect: 0`) and bill only once
activated (`realtime.py rules activate --rule-id ID --confirm`). Activating
keeps the rule's current tag, value and interval unless you pass new ones, and
backfills recent tweets rather than streaming only new ones. An active rule
bills whether or not anything is connected; `realtime.py` prints the
active-rule count after every command, and says "UNKNOWN — may be billing"
when the API's rule state cannot be read. `stream` is complete only when its
`--seconds` window was fully observed.

**`monitor` re-scans the last `--overlap` seconds (default 120) of each poll**
because posts can be indexed after the poll that should have found them. The
overlap is an estimate, not a measured index lag. Seen IDs are saved after each
emitted post, so a restart from `--state` does not repeat posts; a crash in the
instant between emitting and saving can repeat one.

**`diffusion` time-orders replies and quotes only.** Retweeters return as
profiles with no retweet timestamp, so they are counted separately. Ordering
them by their profile `createdAt` invents a false timeline.

**`msg` everywhere except `check_follow_relationship`**, which uses `message`.

## Caching and composition

Resolving a cohort costs money; re-querying it is free. Graph and membership
endpoints cache indefinitely; content endpoints always refetch.

`Client.spend_report()` includes cache hits and dollars saved when nonzero.
Zero spend plus saved credits is a successful cached rerun, not a billing bug.
`jobs.py catalogue USER` and `jobs.py threads USER` are pure local computations
over cached tweet rows and make no API calls. `catalogue` reports each metric
as `{total, average, known, unknown}`: a count the API never reported is
unknown, not 0. Thread output covers only cached
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
compose a cohort from follower IDs already bought. `hydrate(max_usd=...)` buys
only profiles not already stored, returns those, and fills the cohort's member
handles. `Cohort.load` raises `KeyError` for a name or version that was never
saved. A cohort cut short by its `limit`, a failed engager leg, or a filter
over unknown fields has `complete = False` and `incomplete_reasons`; `save()`
keeps that label, and `jobs.py drift` reports it. Loading, set algebra and
drift need no key; `Cohort` creates its `Client` only for a paid call.

## Not implemented

Posting, liking, following and DMs. They require the account's X password and
TOTP seed plus a purchased proxy, and risk suspension of that account. If a
user asks, explain the tradeoff rather than improvising a request.

`/oapi/x_user_stream/*` needs a separate paid subscription; `monitor` polls
instead and needs none.

## When a fact here looks wrong

Prices and response shapes are dated observations. `python3 scripts/verify.py`
(about USD 0.05) re-probes every probeable endpoint and exits 1 with the JSON
error naming each check that drifted; `--quick` (about USD 0.013) probes a
9-endpoint sample. Confirm, then update `references/facts.json` and the prose
that repeats it, and re-stamp with `verify.py --pricing --update` — do not code
around a mismatch.

For an endpoint neither `facts.json` nor `references/verified-facts.md` covers,
probe it: a GET with no params returns 400 if the path exists, 404 if it does
not. The published OpenAPI spec disagrees with observed behavior on response
shapes and required parameters; prefer a live probe over it.
