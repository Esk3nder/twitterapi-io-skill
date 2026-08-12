# twitterapi.io — facts verified against the live API

> **These are dated observations, not permanent truths.** The machine-checkable
> subset lives in [`facts.json`](facts.json); this file holds the prose and the
> reasoning. Before trusting a price or a response shape below, run:
>
> ```bash
> python3 scripts/verify.py        # ~$0.002 — exits 1 if reality has moved
> ```
>
> The client also warns at runtime once these facts pass 90 days old. If
> `verify.py` reports drift, fix `facts.json` **and** the matching prose here
> before relying on anything downstream.

Every line below was confirmed by a real HTTP call on **2026-08-10**, not read
from documentation. Where the vendor's OpenAPI spec or official skill disagreed,
the live result wins and the disagreement is noted.

Method: 41 read-only probes against api.twitterapi.io, plus controlled
balance-delta experiments for pricing. The raw probe output and the vendor
comparison material were deliberately discarded — re-derive from live calls
rather than from any vendor document. To re-probe an endpoint: a GET with no
params returns **400** if the path exists and **404** if it does not.

---

## Pricing — measured by balance delta, not quoted

Credit rate: **100,000 credits = $1**.

A controlled experiment (2 x `followers_ids` @5000, 1 x `followers` @200,
1 x `followers` @20) predicted 4,760 credits and cost **exactly 4,760**.

| Call | credits per record | $ per 1,000 |
|---|---|---|
| `followers_ids`, 5,000/page | **0.45** | **$0.0045** |
| `followers`, pageSize=200 | **1** | **$0.01** |
| `followers`, pageSize=20 | **3** | **$0.03** |

Two consequences that decide real money:

1. **Page size is a 6.7x cost multiplier on the same endpoint.** Always request
   the maximum. A default-page crawl costs 3x a max-page crawl.
2. **IDs cost 1/2.2 of profiles at max page size**, and `followers_ids` returns
   25x more per call, so it is also 25x fewer requests against the QPS ceiling.

> The vendor's official skill states `$0.15 / 1,000` for followers. That is
> **15x too high**, and it propagates: its own cost-guard example claims a
> 100M-follower crawl is "$15k+" when the real figure is ~$1,000 (~$450 by IDs).

Worked example, 200M followers: `followers_ids` **$900** · `followers`@200
**$2,000** · `followers`@20 **$6,000**. Agents reading the vendor skill
predicted $30,000-36,000 and concluded the job was infeasible.

**Billing is asynchronous.** Charges settle roughly 20-60s after the call, so an
immediate balance re-read shows no delta. Never measure per-call cost by reading
the balance straight after a request.

## Rate limits — QPS ladders with account balance

Source: twitterapi.io/qps-limits. Not independently measurable here.

| Balance | QPS |
|---|---|
| free tier | **1 request / 5 seconds** |
| >= 1,000 credits | 3 |
| >= 5,000 credits | 6 |
| >= 10,000 credits | 10 |
| >= 50,000 credits | 20 |

The "200 QPS" figure in the vendor skill and on docs/introduction is marketing
copy. **20 is the observed ceiling.** Check `/oapi/my/info` and derive the rate
limit from the balance rather than assuming.

## Response envelopes — four shapes, verified per endpoint

```
data-wrapped, pagination OUTSIDE the wrapper   <- the trap
  /twitter/user/last_tweets      {code, data{tweets,pin_tweet}, has_next_page, next_cursor, msg, status}
  /twitter/user/tweet_timeline   same shape

data-wrapped, no pagination
  /twitter/user/info             {data, msg, status}
  /twitter/user/{username}       {data, msg, status}   (path form works)
  /twitter/user_about            {data, msg, status}
  /twitter/user/check_follow_relationship  {data, status, message}   <- `message`, not `msg`

flat + envelope
  /twitter/tweet/replies         {tweets, has_next_page, next_cursor, msg, status}
  /twitter/tweet/thread_context  {tweets, has_next_page, next_cursor, msg, status}
  /twitter/user/mentions         {tweets, has_next_page, next_cursor, msg, status}
  /twitter/user/followers        {code, followers, has_next_page, next_cursor, msg, status}
  /twitter/user/followers_ids    {code, ids, has_next_page, next_cursor, msg, status}
  /twitter/tweets                {code, tweets, msg, status}

flat, NO envelope
  /twitter/tweet/advanced_search {tweets, has_next_page, next_cursor}
  /twitter/user/search           {users, has_next_page, next_cursor}
  /twitter/tweet/retweeters      {users, has_next_page, next_cursor}
  /twitter/community/get_tweets_from_all_community  {tweets, has_next_page, next_cursor}

named field
  /twitter/trends                {trends, metadata, msg, status}    <- NOT data-wrapped
  /oapi/my/info                  {recharge_credits, total_bonus_credits}
  /oapi/tweet_filter/get_rules   {rules, msg, status}
```

**`last_tweets` / `tweet_timeline` are the one real trap.** Tweets live at
`data.tweets`, but `has_next_page` and `next_cursor` are **top-level siblings of
`data`**. Code that looks for pagination inside the wrapper stops after one page.

`msg` is used everywhere except `check_follow_relationship`, which uses
`message`. (The OpenAPI spec claims `message` on 8 endpoints; live says one.)

## Corrections to the vendor skill, confirmed live

| Claim | Live result |
|---|---|
| `/twitter/trends` is data-wrapped | **wrong** — key is `trends`, plus `metadata` |
| `thread_context` has no envelope | **wrong** — has `msg`/`status` |
| Followers cost $0.15/1k | **wrong** — $0.01/1k at max page size |
| Rate limit ~200 req/s | **wrong** — 20 QPS ceiling, balance-tiered |
| `followers_ids` not mentioned | **exists**, 5,000/call, `ids` array |
| filter rule interval min 0.05s | vendor blog says 0.1s (untested) |

## Corrections to the OpenAPI spec — treat the spec as unreliable

The spec was wrong on every one of these; the skill was right:

- `tweet/replies` returns **`tweets`**, not `replies`
- `user/search` and `retweeters` have **no** status/msg envelope
- `advanced_search?queryType` is **optional** — 200 without it (spec: required)
- `get_tweets_from_all_community?queryType` **optional** (spec: required)
- `followers` **does** return `has_next_page`/`next_cursor`
- All 11 paths the spec omits **exist**: `user/articles`, `tweet/replies/v1`,
  `user/last_tweets/v2`, `logout` (POST), `x_user_stream/get_user_monitor_account`,
  `user/{username}`, `tweet/bulk_advanced_search`, `report_v2`,
  `list/add_member_v2`, `list/create`, `upload_video`

The spec also ships a `/plants/{id}` scaffold. **Do not trust it for response
shapes or required flags.**

## Page sizes and pagination, verified

| Endpoint | max per call | pagination |
|---|---|---|
| `followers_ids` | **5,000** (the default — no `count` needed) | `cursor`, numeric string |
| `followers` / `followings` | 200 (`pageSize`, clamps 20-200) | `cursor` + `has_next_page` |
| `advanced_search` | **20** (fixed, not tunable) | see warning below |
| `last_tweets` / `tweet_timeline` | 20 | cursor at TOP level |
| `user/search` | 20 | `cursor` |

`followers_ids` accepts **either** `userName` or `userId`.

**Historical search pagination warning** (vendor blog, not yet reproduced here):
cursor paging on `advanced_search` infinite-loops over historical ranges
(2019-2022). Documented fix: ignore the cursor; take the earliest `createdAt` in
the page, subtract one second, use as the next `until_time`. Stop when a page
returns <20 results or the timestamp stops decreasing.

## Field names on returned objects

Tweet: `id`, `text`, `createdAt`, `likeCount`, `retweetCount`, `replyCount`,
`quoteCount`, `bookmarkCount`, `conversationId`, `isReply`, `inReplyToId`,
`inReplyToUsername`, `lang`, `source`, `twitterUrl`, `type`, `entities`,
`quoted_tweet`, `retweeted_tweet`, `author{...}`.
`advanced_search` results additionally carry **`url`** and **`viewCount`**;
`last_tweets` results do **not**. Field sets differ per endpoint — do not assume.

`createdAt` format: `"Mon Aug 10 17:16:53 +0000 2026"` -> `%a %b %d %H:%M:%S %z %Y`

User: `id`, `userName`, `name`, `description`, `followers`, `following`,
`statusesCount`, `favouritesCount`, `mediaCount`, `isBlueVerified`, `isVerified`,
`verifiedType`, `protected`, `location`, `profilePicture`, `coverPicture`,
`createdAt`, `pinnedTweetIds`.
Follower/following **counts** are `followers` / `following` — plain nouns, not
`followersCount`.

Trends are **double-nested**: `trends[i].trend.name`, `.trend.rank`.

## Read surface completion — verified 2026-08-10 (second pass)

**`tweet/bulk_advanced_search` (POST) is the only lever against the QPS ceiling.**
Body `{queries:[{query, queryType, cursor?}]}` -> `{results: {query_0: {tweets,
has_next_page, next_cursor}, query_1: {...}}}`. N searches in ONE request.
Since 20 QPS is a hard maximum, batching multiplies effective throughput by N.

**`tweet/replies/v2` returns 31-38 replies per call; plain `tweet/replies`
returns 20.** ~1.8x fewer requests for identical data, and it sorts
(`queryType`: Relevance | Latest | Likes). Prefer v2. `replies/v1` also
returns ~35.

**`user/articles`** is `data`-wrapped (`data.articles`) with pagination at the
ROOT — the same trap as `last_tweets`. Param is all-lowercase **`username`**.

**`community/info`** -> named key `community_info` with `member_count`,
`join_policy`, `admin`, `creator`, `members_preview`.
**`community/members`, `moderators`, `tweets`** return BOTH `has_next` and
`has_next_page`; use `has_next_page` for consistency with everything else.

**`spaces/detail`** exists (400 `space_id is required` with no params). A 404
from it means the space id is unknown, NOT that the path is missing.

**`user/last_tweets/v2`** accepts only `userId`; `userName` returns 400.

### `/oapi/x_user_stream/*` field names are asymmetric

Not implemented by this skill (see the subscription note below), but recorded
because the convention is unguessable and was paid for:

| Operation | Method | Field |
|---|---|---|
| add a user to the monitor list | POST `/oapi/x_user_stream/add_user_to_monitor_tweet` | `x_user_name` |
| list monitored users | GET `/oapi/x_user_stream/get_user_to_monitor_tweet` | `query_type` (0 all / 1 tweet / 2 profile) |
| remove a user | POST `/oapi/x_user_stream/remove_user_to_monitor_tweet` | **`id_for_user`** |

Removal takes `id_for_user` from the list response, not the handle used to add —
so removing is a two-step operation. Both are POST; neither is DELETE.

### The `/oapi/x_user_stream/*` family needs a paid subscription

`add_user_to_monitor_tweet` returns **HTTP 200** with body
`{"status": "error", "msg": "No active monitoring subscription"}`.

Two consequences. First, that whole family is unusable on this account, which
is why `workflows.py monitor` uses stateless polling instead. Second, and
generally: **this API signals failure in the body while returning HTTP 200.**
Code that only checks the status code will treat the failure as success. Always
check `status == "error"` in the body as well.

## Live E2E findings — 2026-08-10 (full 20-workflow run, $0.89)

**Data requests bill the 15-credit minimum even when they return nothing.**
400s, 404s, HTTP-200 body errors, retried 5xx attempts and rule lookups all
charge $0.00015. Measured: a full live run's settled billing exceeded naive
client-side accounting by ~1.2%, and the gap was almost exactly (uncharged
probe requests x 15 credits). The client books this floor per request and
adds only the excess per page.

**`/oapi/my/info` is the exception — it is FREE.** Measured directly: 21
consecutive balance reads moved the balance by exactly **0 credits**. Charging
it would inflate every session by 15 credits per lazy QPS lookup. Check the
balance as often as you like.

**Community ids are NOT discoverable from the API.**
`/twitter/community/get_tweets_from_all_community?query=...` returns tweets
whose `communityInfo` is **null** on every record (verified by recursive key
scan over 23 results). With an id taken from a community URL, both
`/twitter/community/info` (19-key object incl. `member_count`, `join_policy`,
`admin`, `creator`) and `/twitter/community/members` work correctly. So: the
community pipeline needs an externally supplied id — there is no discovery
path through this API.

**`authority()` needs a cohort large enough to contain internal follow edges.**
An 8-member cohort from a narrow `from_search` seed returned an empty frontier
with `complete: true` — structurally correct, but no signal. In-degree ranking
only means something when members actually follow each other; seed with
enough accounts (dozens+) for a real scene, or expect an empty ranking.

**Busy topics cost dollars, not cents.** `narrative_tracker` on a high-volume
query exhausted a $0.40 ceiling even with `min_faves:100` filtering. The guard
correctly raised rather than returning a partial corpus. Bound narrative work
with a tighter window, an engagement filter, or a deliberate higher ceiling.

## `from:` search EXCLUDES native retweets — verified 2026-08-12

A `from:USER` advanced_search never returns native retweets. Measured on two
accounts, so this is search-syntax behaviour, not an account quirk:

| query | tweets | with `retweeted_tweet` |
|---|---|---|
| `from:doodlestein` | 20 | **0** |
| `from:doodlestein filter:nativeretweets` | 1 | 1 |
| `from:elonmusk` | 20 | **0** |
| `from:elonmusk filter:nativeretweets` | 20 | **20** |

Consequences:

- **A "complete history" built from `from:USER` is originals + replies +
  quote tweets only.** Retweets require a SECOND pass with
  `filter:nativeretweets`. Reporting the first pass as an account's full
  output silently omits everything they amplified.
- **`-filter:retweets` is a no-op** on a `from:` query — the records were
  never there. Adding it changes nothing and implies a filter that is not
  doing any work.
- `-filter:replies` DOES work: it drops replies (19 -> 0 in the same sample).
- Quote tweets are NOT retweets and ARE returned by `from:` (1,191 of them in
  a 12,262-tweet doodlestein archive), carrying `quoted_tweet`.

## Errors

- `400` -> `{"detail": "..."}` naming the bad/missing field. Also what an
  existing endpoint returns when called with no params.
- `405` -> path exists, wrong method (e.g. GET on `/twitter/logout`).
- `404` -> path genuinely absent. Useful for existence probing.
- `500` -> `{"message": ...}` (observed on `get_my_x_account_detail_v3`).

## Writes — NOT verified, deliberately out of scope

`create_tweet_v2` correctly rejects with 400 when `login_cookies` is absent, so
nothing here can change account state. Beyond that, nothing is verified.

Four mutually inconsistent login flows exist across vendor sources — session
field named `login_cookies` (official skill), `auth_session` (spec v1),
server-side keyed by `user_name` (spec v3), and `session` (their posting
guide) — and the text field is `tweet_text` in some, `text` in others. All v3
paths return 400-exists. Writes additionally require buying a residential
proxy and surrendering X credentials plus a TOTP seed, and risk suspension of
the authenticating account. Treat any write guidance as unverified.
