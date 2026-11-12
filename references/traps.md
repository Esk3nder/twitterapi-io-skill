# Traps and composition

Loaded on demand from `SKILL.md`. The client already handles every trap
below; read this when a result looks wrong or when composing the library
in Python.

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
refused rather than rewritten into a different query. `--index-faves` leaves
the operator in the query instead: far fewer paid rows, at the price of missing
posts whose likes rose after the index snapshot. For a positive threshold, `limit` bounds qualifying rows, not
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
inside one second. The walker splits such a second into replies and
non-replies and searches each half; if a half is still full it reports
`INCOMPLETE` with the affected timestamps rather than dropping them silently.

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
endpoints cache indefinitely (`--max-age DAYS` buys them again when older);
search pages are refetched, except that an unfinished walk replays its own
pages for 24 hours.

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
