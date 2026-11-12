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

A bare `@handle` dropped with no question attached is a lookup request: run
`jobs.py brief USER` (the last 7 days) and report what it returns. A tweet link
or tweet ID is a request for that post: run `jobs.py tweet LINK_OR_ID`.

Confirm the platform first when the handle is ambiguous. `@name` also addresses
people on GitHub, Slack, and Linear, and the same string is often the same
person on several of them. If the surrounding message does not settle which
network is meant, ask before buying an X lookup for the wrong one.

## Which command

| Need | Command |
|---|---|
| One post and its thread | `jobs.py tweet LINK_OR_ID` |
| Entity state | `jobs.py brief USER --days 7` |
| What's emerging in a topic | `jobs.py narrative "QUERY" --days 30` |
| N entities on shared axes | `jobs.py benchmark a,b,c --days 30` |
| Who a scene follows (frontier) | `jobs.py authority "QUERY"` |
| Shared followers of two accounts | `jobs.py overlap A B` |
| Organic-vs-bought audience signals | `jobs.py authenticity USER` |
| Who moved early on a post | `jobs.py diffusion TWEET_ID` |
| Followers / audience | `workflows.py audience USER` |
| Tweet history, date-ranged search | `workflows.py history USER --since D --until D` |
| Watch accounts for new posts | `workflows.py monitor U1,U2 --interval 60` |
| Push delivery of matched tweets | `realtime.py rules add` then `realtime.py stream` |
| Who joined/left a saved cohort (free, cache only) | `jobs.py drift COHORT_NAME` |
| Cached corpus summary, threads, media (free, cache only) | `jobs.py catalogue USER`, `threads USER`, `media USER` |
| Replies, quotes, lists, trends, cohorts | `import twitterapi.Client`, `cohort.Cohort` |

```bash
python3 scripts/workflows.py --max-usd 5 audience elonmusk --limit 100000
python3 scripts/jobs.py narrative "ai agents min_faves:50" --days 7 --index-faves
```

Use `<command> --help` for each command's exact flags and examples.

## Reading the output

Stdout is JSON (or JSON Lines for streaming records); progress, spend, and
warnings stay on stderr. Lists default to brief fields: `--fields <a,b,c>` adds
supported fields and `--full` returns complete long text.

**Exit codes:** `0` valid complete or valid partial result · `2` usage, refusal,
or setup failure · `1` runtime, API, or internal failure. An exit 0 is not proof
of completeness: inspect `complete` and `completeness.status`. Partial payloads
carry `complete: false`, a reason, the records returned, `spend`, and a resume
command. Do not present a partial result as whole.

A walk that stops early (spend ceiling, Ctrl-C, a timeout, an API failure
after rows arrived) keeps every page it bought. Rerunning the same command
within 24 hours replays those pages from the store at no cost and continues
from where it stopped. `history --out` refuses to overwrite an existing file,
so give a rerun a new path.

Jobs return JSON for you to interpret. They compute who and what; they do not
compute themes, consensus, or whether engagement is organic. Do not present a
job's output as if it contained a judgment it does not make.
`narrative` reports counts per window plus top posts, top authors, new authors
and rising hashtags, cashtags, mentions and linked domains. `authority` ranks
members and lists `frontier_external`: accounts the cohort follows that are
not in it. `diffusion` time-orders replies and quotes only; retweeters carry no
retweet timestamp and are counted separately. Search counts are observations of
an index that is not deterministic, not ground truth.

## Cost

100,000 credits = USD 1. `--max-usd` (default USD 5) is a hard ceiling on every
paid command: each request books its worst-case cost against it before it is
sent. Confirm with the user before any crawl over a few dollars.

| Endpoint | page size | USD per 1,000 records |
|---|---|---|
| `followers_ids` | 5,000 | **0.0045** |
| `followers` (profiles) | 200 | 0.01 |
| `followers` (profiles) | 20 (default) | 0.03 |

1. **Use `followers_ids` unless you need profile fields.** For an account with
   200M followers: **USD 900** by IDs, **USD 2,000** by profiles.
2. **You cannot buy a partial page.** Asking for 3,000 IDs bills the full
   5,000-record page.
3. **Search costs 20 posts per request, and `min_faves` is filtered locally by
   default**, so a busy topic scans every match to keep the popular few. Add
   `--index-faves` (`narrative`, `history`) to let the search index filter: far
   cheaper, but it can miss posts whose likes rose recently. Otherwise narrow
   the window.
4. `Client.estimate()` is not an upper bound; `max_usd` is. `overlap` and
   `authority` estimate up front and refuse (exit 2) rather than start a crawl
   they cannot afford.

Follower and graph pages are cached indefinitely, so a repeated crawl is free;
pass `--max-age DAYS` to buy them again when the cached copy is older. `overlap`
reports `oldest_cached_data_days`. Search results are refetched, except for the
24-hour replay of an unfinished walk. `python3 scripts/store.py` shows what the
store holds, spend over the last 7 days and recent runs.

Rate scales with credit balance up to 20 requests a second. `Client` derives it
and searches a long window in parallel time shards; do not override it.

## Not implemented

Posting, liking, following and DMs. They require the account's X password and
TOTP seed plus a purchased proxy, and risk suspension of that account. If a
user asks, explain the tradeoff rather than improvising a request.

`/oapi/x_user_stream/*` needs a separate paid subscription; `monitor` polls
instead and needs none.

## When something looks wrong

Read `references/traps.md` before explaining a surprising result: it lists the
API behaviours that return plausible but wrong data, how the client handles
each, and how to compose `Cohort` and `Store` in Python.

Prices and response shapes are dated observations. `python3 scripts/verify.py`
(about USD 0.05) re-probes every probeable endpoint and exits 1 with the JSON
error naming each check that drifted; `--quick` (about USD 0.013) probes a
9-endpoint sample. Confirm, then update `references/facts.json` and the prose
that repeats it, and re-stamp with `verify.py --pricing --update`. Do not code
around a mismatch. For an endpoint the references do not cover, probe it: a GET
with no params returns 400 if the path exists, 404 if it does not.
