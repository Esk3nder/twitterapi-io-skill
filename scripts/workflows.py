#!/usr/bin/env python3
"""Triggerable workflows over twitterapi.io. Standard library only.

Each workflow is a CLI entry point AND an importable function. They exist so an
agent never has to re-derive parameter names, pagination strategy or cost — all
three are where unaided agents measurably fail.

    python3 workflows.py audience   elonmusk --limit 50000
    python3 workflows.py history    openai --since 2026-01-01 --until 2026-02-01
    python3 workflows.py monitor    elonmusk,openai --interval 60

Every workflow prints a cost estimate and refuses to exceed --max-usd
(default $5). Nothing here writes to X.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import UTC, datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from axi import (
    ACCOUNT_FIELDS,
    TWEET_FIELDS,
    ArgumentParser,
    HelpFormatter,
    UsageError,
    add_common_output_flags,
    dashboard,
    emit,
    emit_line,
    error,
    fast_version,
    format_record,
    parse_fields,
    run_cli,
    success,
)
from store import DEFAULT_DB, parse_ts
from twitterapi import (
    APIError,
    Client,
    CostLimitExceeded,
    IncompleteDataError,
    credits_to_usd,
    enrich_tweet_media,
    filter_search_tweets,
    prepare_search_query,
)

SEARCH_PAGE = 20                        # advanced_search returns at most 20 per call
SEARCH_PATH = "/twitter/tweet/advanced_search"
RESUME_MAX_AGE = 86400                  # seconds a bought search page can be replayed
SECOND_SPLITS = ("filter:replies", "-filter:replies")   # disjoint and exhaustive
MAX_QUERY_CHARS = 480                   # X rejects longer search queries
DEFAULT_MAX_USD = 5.0                   # ceiling for a Client this module creates itself
MONITOR_SEEN_CAP = 5000                 # newest ids kept for monitor dedupe
MONITOR_OVERLAP = 120                   # seconds each poll re-scans for late-indexed posts
_HANDLE = re.compile(r"^[A-Za-z0-9_]+$")


def _strictest(*limits):
    """Lowest non-None ceiling. A caller passing max_usd=5 must never be
    loosened by a client that happens to carry max_usd=100."""
    vals = [v for v in limits if v is not None]
    return min(vals) if vals else None


def _bounded_client(client, max_usd):
    """The client a library workflow spends through.

    max_usd=None means "no extra ceiling": a caller-configured client keeps its
    own ceiling. A library default must never tighten it, or `--max-usd 50`
    silently stops at the default and "raise --max-usd" advice cannot work.
    Only a client this module creates itself falls back to DEFAULT_MAX_USD."""
    if client is None:
        return Client(max_usd=DEFAULT_MAX_USD if max_usd is None else max_usd)
    client.max_usd = _strictest(client.max_usd, max_usd)
    return client


def _cli_session(a):
    """(client, output file) for one workflow command.

    Every workflow writes through the store: followers bought by `audience`
    and posts seen by `monitor` are kept, and spend is recorded. An existing
    --out file is refused before anything is bought: a rerun after a partial
    result must not empty the rows the first run wrote."""
    out = getattr(a, "out", None)
    if out and os.path.exists(out):
        raise UsageError(f"--out {out} already exists; move it or pick another path")
    c = Client(max_usd=a.max_usd, store=getattr(a, "store", None) or DEFAULT_DB)
    return c, (open(out, "w") if out else sys.stdout)


def _clean_handle(raw, flag="USER"):
    handle = (raw or "").strip().lstrip("@")
    if not _HANDLE.match(handle):
        raise UsageError(f"{flag} must be an X handle (letters, digits, "
                         f"underscore), got {raw!r}")
    return handle


def _day_ts(value, flag):
    """UTC midnight of a YYYY-MM-DD day, or None. Malformed input is a usage
    error, never an internal failure."""
    if value is None:
        return None
    try:
        day = datetime.strptime(value, "%Y-%m-%d")
    except (TypeError, ValueError):
        raise UsageError(f"{flag} must be YYYY-MM-DD, got {value!r}") from None
    return int(day.replace(tzinfo=UTC).timestamp())


def _window_ts(since, until):
    """Validated (since_ts, until_ts). An empty or inverted window is refused
    before any paid request, never sent and billed."""
    since_ts, until_ts = _day_ts(since, "--since"), _day_ts(until, "--until")
    if since_ts is not None and until_ts is not None and since_ts >= until_ts:
        raise UsageError(f"--since {since} must be earlier than --until {until}")
    return since_ts, until_ts


def _int_or_none(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------- audience --
def audience(user, *, ids_only=True, limit=None, max_usd=None,
             verified_only=False, client=None, quiet=False):
    """Yield an account's followers (IDs by default, profiles on request).

    ids_only=True uses /twitter/user/followers_ids: 0.45 credits/record at
    5000 per call, versus 1.0 at 200 per call for full profiles. That is 2.2x
    cheaper and 25x fewer requests. Only turn it off when you actually need
    profile fields (bio, follower counts, verified status).

    When the record count is known, raises CostLimitExceeded BEFORE spending
    if the crawl would exceed the ceiling. When it is unknown (verified
    followers are an unknown subset of all followers; a profile may omit its
    follower count), the walk is bounded page by page by the client ceiling.
    Records already yielded are always paid for and valid.

    Sets client.last_audience_total to the endpoint's known record total, or
    None when no honest denominator exists.
    """
    if limit is not None and limit < 1:
        raise UsageError(f"--limit must be at least 1, got {limit}")
    c = _bounded_client(client, max_usd)
    info = c.user_info(user)            # Client.user_info charges its own 18 credits
    followers, uid = _int_or_none(info.get("followers")), info.get("id")
    endpoint = ("verified_followers" if verified_only
                else "follower_ids" if ids_only else "followers")
    if verified_only and uid in (None, ""):
        raise IncompleteDataError(
            f"could not resolve @{user} to the user id the verified-followers "
            f"endpoint requires")
    # The verified endpoint returns an unknown subset of all followers, so the
    # account's follower count is neither its cost basis nor its denominator.
    total = None if verified_only else followers
    want = limit if total is None else (min(limit, total) if limit else total)
    c.last_audience_total = total
    c.last_audience_context = {"account_followers": followers, "requested": want}
    est = None if want is None else c.estimate(endpoint, want)

    if not quiet:
        shown = "unknown" if followers is None else f"{followers:,}"
        if est is None:
            ceiling = ("no ceiling" if c.max_usd is None
                       else f"the ${c.max_usd:,.2f} ceiling")
            print(f"@{user}: {shown} followers | record count for {endpoint} "
                  f"unknown | walk bounded page by page by {ceiling}",
                  file=sys.stderr)
        else:
            print(f"@{user}: {shown} followers | {want:,} via {endpoint} "
                  f"| est ${est:,.2f}", file=sys.stderr)
        if ids_only and not verified_only and want and want > 1000:
            print(f"  full profiles would cost ${c.estimate('followers', want):,.2f}",
                  file=sys.stderr)
    if est is not None and c.max_usd is not None and est > c.max_usd:
        raise CostLimitExceeded(
            f"{endpoint} for {want:,} records ~= ${est:,.2f}, over the "
            f"${c.max_usd:,.2f} ceiling. Pass --limit, or raise --max-usd "
            f"deliberately. (${c.spent_usd:,.4f} already spent on the lookup.)")

    stream = (c.verified_followers(uid, limit=limit) if verified_only
              else c.follower_ids(user, limit=limit) if ids_only
              else c.followers(user, limit=limit))
    for r in stream:
        yield {"id": r} if isinstance(r, (str, int)) else r
    if getattr(stream, "complete", True) is False:
        raise IncompleteDataError(
            getattr(stream, "_warning", None) or
            f"the follower walk for @{user} ended before completeness was proven")


def _audience_cli(a):
    if a.limit is not None and a.limit < 1:
        raise UsageError(f"--limit must be at least 1, got {a.limit}")
    c, fh = _cli_session(a)
    got = 0
    was_truncated = False
    truncated_reason = None
    refused = False
    try:
        for rec in audience(a.user, ids_only=a.ids_only, limit=a.limit,
                            max_usd=a.max_usd, verified_only=a.verified_only,
                            client=c):
            rendered, clipped = format_record(
                rec, command="workflows.py audience <user>",
                schema=ACCOUNT_FIELDS, fields=getattr(a, "fields", None),
                defaults=(("id",) if a.ids_only else ("id", "user_name", "name")),
                full=getattr(a, "full", False))
            fh.write(json.dumps(rendered, ensure_ascii=False) + "\n")
            was_truncated = was_truncated or clipped
            got += 1
        truncated = False
    except (CostLimitExceeded, KeyboardInterrupt) as e:
        truncated = True
        refused = not got
        truncated_reason = str(e) or "stopped by the user"
        print(f"[audience] {'stopped' if got else 'refused'}: {e}", file=sys.stderr)
    except IncompleteDataError as e:
        truncated = True
        truncated_reason = str(e)
        print(f"[audience] partial: {e}", file=sys.stderr)
    finally:
        if a.out:
            fh.close()
        c.store.close()
    print(f"{got:,} records | {c.spend_report()}", file=sys.stderr)
    if refused:
        emit_line(error("refusal", truncated_reason, "workflows.py audience",
                   "Pass `--limit <count>` or raise `--max-usd <amount>`"))
        return 2
    complete = not truncated
    total = getattr(c, "last_audience_total", None)
    limit_reached = bool(a.limit) and got >= a.limit
    if total is None and complete and not limit_reached:
        total = got                     # the walk itself proved the full count
    more = (total is None and limit_reached) or (total is not None and got < total)
    summary = success(
        "workflows.py audience", {
            "records_count": f"{got} of {'unknown' if total is None else total} total",
            "records_state": (None if got else
                              f"0 followers found for @{a.user} in the requested scope"),
            "artifact": a.out,
            "help": (["Run `workflows.py audience <user> --profiles` for profile fields"]
                     + (["Run `workflows.py audience <user> --limit <count>` with a larger "
                         "count to show more"]
                        if more and complete else [])
                     + (["Run `workflows.py audience <user> --full` for complete text"]
                        if was_truncated else [])),
        }, complete=complete, reason=truncated_reason,
        resume=("Raise `--max-usd <amount>` and rerun" if truncated else None),
        records_returned=got)
    print(json.dumps(summary, ensure_ascii=False) if not a.out else
          json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


# ----------------------------------------------------------------- history --
def _search_page(c, q, replay):
    """One advanced_search page -> (tweets, has_next_page as the API sent it).

    A fresh page is charged and persisted before it is returned. With
    `replay`, the page's id list is also kept for RESUME_MAX_AGE, so a rerun
    of the same window is served from the store and pays nothing."""
    params = {"query": q, "queryType": "Latest"}
    index = c.store.get_page(SEARCH_PATH, params, RESUME_MAX_AGE) if replay else None
    rows = c.store.raw_tweets(index["ids"]) if index else None
    if rows is not None:
        c.cache_hits += 1
        c.saved_credits += Client.page_credits("search", len(rows), SEARCH_PAGE)
        return rows, index["has_next_page"]
    resp = c._raw("GET", SEARCH_PATH, params,
                  max_credits=Client.page_credits("search", SEARCH_PAGE, SEARCH_PAGE))
    tweets = resp.get("tweets")
    if not isinstance(tweets, list):
        problem = ("omitted the 'tweets' field" if "tweets" not in resp
                   else f"returned 'tweets' as {type(tweets).__name__}")
        raise IncompleteDataError(
            f"advanced_search {problem}; refusing to read contract drift as "
            f"an empty window, so the result is partial.")
    batch = [enrich_tweet_media(tweet) for tweet in tweets]
    c._charge("search", len(batch), SEARCH_PAGE)
    has_more = resp.get("has_next_page")
    # A fetched page is already paid for. Persist it before it is yielded so a
    # caller that stops consuming still retains every acquired row.
    if c.store is not None and batch:
        c.store.put_tweets(batch)
    if replay and isinstance(has_more, bool):
        c.store.put_page(SEARCH_PATH, params, {
            "ids": [t.get("id") for t in batch], "has_next_page": has_more})
    return batch, has_more


def _prepare(c, query, index_faves):
    """prepare_search_query, plus one warning per client when the walk must
    buy every match to apply `min_faves` (parallel shards share the client)."""
    wire, local_min_faves = prepare_search_query(query, index_faves)
    if local_min_faves and not getattr(c, "scan_warned", False):
        c.scan_warned = True
        ceiling = "no ceiling" if c.max_usd is None else f"the ${c.max_usd:,.2f} ceiling"
        print(f"[search] WARNING: min_faves:{local_min_faves} is checked against "
              f"live like counts, so every match in the window is bought and "
              f"most may be discarded, bounded only by {ceiling}. --index-faves "
              f"lets the search index filter instead: far cheaper, but it can "
              f"miss posts whose likes rose recently.", file=sys.stderr)
    return wire, local_min_faves


def search_window(client, query, since_ts=None, until_ts=None, *,
                  max_pages=None, progress=False, index_faves=False,
                  replay=True):
    """THE sliding-time-window walk over advanced_search. Generator.

    Cursor pagination loops on historical ranges, and advanced_search returns
    at most 20 tweets per call, so the walk requests newest-first within
    [since_ts, until_ts), takes the OLDEST createdAt on the page, and moves
    until_time to that second (re-including it; dedupe by id absorbs the
    overlap). Spends through `client` under the client's own ceiling.

    Yields qualifying tweet dicts newest-first, each id once. With a store
    and a fixed `until_ts`, a rerun of the same window replays the pages
    already bought (see _search_page); `replay=False` is for a caller whose
    windows never repeat.

    Completeness: normal exhaustion (StopIteration) happens ONLY when the
    window is proven walked: an empty page, a short page without
    has_next_page, or the boundary reaching since_ts. Otherwise it raises,
    after yielding everything already acquired (those records stay valid):
      CostLimitExceeded   the client ceiling stops the next paid page;
      IncompleteDataError contract drift, a page that asserts more data but
                          has no parseable timestamps, one second that still
                          holds a full page after being split, or max_pages.
    """
    c = client
    query, local_min_faves = _prepare(c, query, index_faves)
    replay = replay and c.store is not None and until_ts is not None
    seen, pages = set(), 0

    def unseen(batch):
        keep = {t.get("id") for t in filter_search_tweets(batch, local_min_faves)}
        for t in batch:
            if t.get("id") in seen:
                continue
            seen.add(t.get("id"))
            if t.get("id") in keep:
                yield t

    while True:
        q = query
        if since_ts is not None:
            q += f" since_time:{since_ts}"
        if until_ts is not None:
            q += f" until_time:{until_ts}"
        batch, has_more = _search_page(c, q, replay)
        pages += 1
        # Timestamps come from the WHOLE page, not just fresh tweets: an
        # all-duplicate page must still move the boundary.
        stamps = [s for s in (parse_ts(t.get("createdAt")) for t in batch)
                  if s is not None]
        yield from unseen(batch)
        if progress:
            print(f"  page {pages}: {len(batch)} returned, {len(seen):,} total, "
                  f"${c.spent_usd:,.4f}", file=sys.stderr)

        if not isinstance(has_more, bool):
            # Unknown, never "no more pages". This page's tweets were yielded
            # above; only whether more of the window exists is unknown.
            raise IncompleteDataError(
                f"advanced_search omitted has_next_page (got {has_more!r}); "
                f"whether more of the window exists is unknown. The "
                f"{len(seen):,} tweets already yielded are valid, but the "
                f"result is partial.")
        full = len(batch) >= SEARCH_PAGE
        if not batch:
            return                      # nothing left in [since, until)
        if not full and not has_more:
            return                      # short page, no more asserted: exhausted
        # A full page, or a short page that asserts has_next_page (results can
        # be filtered server-side), is NOT exhaustion: keep walking by time.
        if not stamps:
            raise IncompleteDataError(
                f"a search page asserting more data had no parseable createdAt "
                f"timestamps; the {len(seen):,} tweets already yielded are "
                f"valid, but no safe resume boundary exists")
        # Another paid request is needed. At exact exhaustion of the ceiling,
        # stop honestly before it; a terminal page above already proves
        # completeness and is allowed to finish at the ceiling.
        if c._over_ceiling():
            raise CostLimitExceeded(
                f"Spend ceiling hit at ${c.spent_usd:,.2f} after {pages} pages; "
                f"the {len(seen):,} tweets already yielded are valid and paid for.")
        if max_pages and pages >= max_pages:
            raise IncompleteDataError(
                f"stopped at --max-pages {max_pages} before the range was "
                f"exhausted; the {len(seen):,} tweets already yielded are valid "
                f"but the result is partial.")

        oldest = min(stamps)
        new_until = oldest + 1          # re-include the boundary second
        if full and max(stamps) == oldest:
            # A full page inside ONE second: a time window cannot page within
            # it, so fetch that second again as disjoint halves.
            for part in SECOND_SPLITS:
                half, more = _search_page(
                    c, f"{query} {part} since_time:{oldest} until_time:{oldest + 1}",
                    replay)
                yield from unseen(half)
                if more is not False or len(half) >= SEARCH_PAGE:
                    raise IncompleteDataError(
                        f"{SEARCH_PAGE}+ tweets share second {oldest} even "
                        f"split by reply status; time-window paging cannot "
                        f"split it further, so the result is partial.")
            new_until = oldest          # that whole second is now covered

        if until_ts is not None and new_until >= until_ts:
            new_until = until_ts - 1    # force progress rather than loop
        until_ts = new_until
        if since_ts is not None and until_ts <= since_ts:
            return                      # boundary reached = clean exhaustion


def search_shards(client, query, since_ts, until_ts, *, shards=8,
                  index_faves=False):
    """search_window over [since_ts, until_ts) as parallel sub-windows.

    One window is walked a page at a time (about two seconds each); the
    account allows 20 requests a second. Yields the newest sub-window first.
    A stop in any shard (ceiling, unprovable completeness, Ctrl-C) is raised
    only AFTER every row every shard acquired has been yielded."""
    span = until_ts - (since_ts or until_ts)
    _prepare(client, query, index_faves)            # warn once, before the threads
    shards = max(1, min(shards, span // 3600))      # under an hour each: don't
    edges = [until_ts - span * i // shards for i in range(shards)] + [since_ts]

    def walk(i):
        rows = []
        try:
            for t in search_window(client, query, edges[i + 1], edges[i],
                                   index_faves=index_faves):
                rows.append(t)
        except (CostLimitExceeded, IncompleteDataError) as exc:
            return rows, exc
        return rows, None

    futures = []
    with ThreadPoolExecutor(shards) as pool:
        try:
            for i in range(shards):
                futures.append(pool.submit(walk, i))
            while wait(futures, timeout=10).not_done:
                print(f"[search] {client.requests_made:,} requests, "
                      f"${client.spent_usd:,.4f} spent, "
                      f"{sum(f.done() for f in futures)}/{shards} shards done",
                      file=sys.stderr)
        except KeyboardInterrupt:
            client.halt("stopped by the user")
    stop, seen = None, set()
    for future in futures:
        rows, exc = future.result()
        for t in rows:
            # Whether the index treats a shard edge as inclusive is not a
            # documented contract; a tweet on an edge must still appear once.
            if t.get("id") not in seen:
                seen.add(t.get("id"))
                yield t
        stop = stop or exc
    if len(futures) < shards:           # interrupted before every shard began
        stop = stop or CostLimitExceeded(client.halted)
    if stop:
        raise stop


def anchored_now(client, key):
    """`now` for a walk that ends at the present.

    An unfinished walk with the same key in the last RESUME_MAX_AGE seconds
    gives its own start time back, so the rerun requests identical windows
    and replays every page already bought instead of paying for it again."""
    if client.store is None:
        return int(time.time())
    client.store.drop_pages(SEARCH_PATH, RESUME_MAX_AGE)    # too old to replay
    mark = client.store.get_page("walk", {"key": key}, RESUME_MAX_AGE) or {}
    now = mark.get("now") or int(time.time())
    client.store.put_page("walk", {"key": key}, {"now": now})
    return now


def finish_walk(client, key):
    """The walk completed: the next run starts from the real present."""
    if client.store is not None:
        client.store.put_page("walk", {"key": key}, {})


def history_search(query, since=None, until=None, *, max_usd=None,
                   max_pages=None, client=None, progress=True,
                   index_faves=False, now=None):
    """Historical tweet search over [since, until) as YYYY-MM-DD UTC days.

    Validates the window (malformed or inverted dates raise UsageError before
    any request). A bounded window without max_pages is walked in parallel
    shards; see search_window for the completeness semantics. max_usd=None
    keeps a supplied client's ceiling.
    """
    since_ts, until_ts = _window_ts(since, until)
    c = _bounded_client(client, max_usd)
    until_ts = until_ts or now or int(time.time())
    if since_ts is None or max_pages:
        return search_window(c, query, since_ts, until_ts, max_pages=max_pages,
                             progress=progress, index_faves=index_faves)
    return search_shards(c, query, since_ts, until_ts, index_faves=index_faves)


def _history_cli(a):
    if a.user and a.query:
        raise UsageError("pass a USER or --query, not both; a USER is the "
                         "query `from:USER`, so --query would be ignored")
    if a.user:
        a.user = _clean_handle(a.user)
    _window_ts(a.since, a.until)
    if a.max_pages is not None and a.max_pages < 1:
        raise UsageError(f"--max-pages must be at least 1, got {a.max_pages}")
    if a.include_retweets and not a.user:
        raise UsageError("--include-retweets needs the USER shorthand; put "
                         "`filter:nativeretweets` in the query instead")
    q = f"from:{a.user}" if a.user else a.query
    if a.exclude_replies:
        q += " -filter:replies"
    prepare_search_query(q, a.index_faves)  # refuses unsafe min_faves before any spend
    c, fh = _cli_session(a)
    with c.store:
        return _history_walk(a, c, q, fh)


def _history_walk(a, c, q, fh):
    if not a.user:
        scope = ("raw query results; composition depends on the query and "
                 "native retweets are not added automatically")
    elif a.include_retweets:
        scope = ("originals + replies + quote tweets + native retweets included "
                 "(two search passes)")
    elif a.exclude_replies:
        scope = ("originals + quote tweets; replies excluded; native retweets "
                 "excluded by `from:` search")
    else:
        scope = ("originals + replies + quote tweets; native retweets excluded "
                 "by `from:` search (pass --include-retweets for a second pass)")
    print(f"[history] scope: {scope}", file=sys.stderr)
    print(f"query: {q!r}  window {a.since or 'any'} -> {a.until or 'now'}",
          file=sys.stderr)
    walk_key = f"history:{q}:{a.since}:{a.until}:{a.include_retweets}"
    now = anchored_now(c, walk_key)
    queries = [q]
    if a.include_retweets:
        queries.append(f"from:{a.user} filter:nativeretweets")

    seen, n = set(), 0
    oldest_ts = account_created_ts = stated_posts = None
    composition = {"originals": 0, "replies": 0, "quotes": 0,
                   "native retweets": 0}
    truncated = False
    truncated_reason = None
    was_text_truncated = False
    stop_all_passes = False
    try:
        for search_query in queries:
            if len(queries) > 1:
                label = ("native-retweet pass" if "filter:nativeretweets" in search_query
                         else "original/reply/quote pass")
                print(f"[history] {label}: {search_query!r}", file=sys.stderr)
            records = iter(history_search(
                search_query, a.since, a.until, client=c, max_usd=a.max_usd,
                max_pages=a.max_pages, index_faves=a.index_faves, now=now))
            while True:
                try:
                    t = next(records)
                except StopIteration:
                    break
                except IncompleteDataError as e:
                    # Page/timestamp/contract completeness is scoped to this
                    # search pass. Preserve the partial status, but still run
                    # the independent native-retweet pass with its own cap.
                    truncated = True
                    truncated_reason = str(e)
                    print(f"\nSTOPPED: {e}", file=sys.stderr)
                    break
                except (CostLimitExceeded, KeyboardInterrupt) as e:
                    # The spend ceiling is shared across passes, so another
                    # pass cannot make useful progress once it is exhausted.
                    truncated = True
                    truncated_reason = str(e) or "stopped by the user"
                    stop_all_passes = True
                    print(f"\nSTOPPED: {e}", file=sys.stderr)
                    break
                # isRetweet is not 100% reliable per the vendor guide; corroborate
                # it with retweeted_tweet, and dedupe across the two searches.
                is_retweet = bool(t.get("isRetweet") or t.get("retweeted_tweet"))
                tweet_id = t.get("id")
                if tweet_id in seen:
                    continue
                seen.add(tweet_id)

                stamp = parse_ts(t.get("createdAt"))
                if stamp is not None:
                    oldest_ts = stamp if oldest_ts is None else min(oldest_ts, stamp)
                author = t.get("author") or {}
                created = parse_ts(author.get("createdAt"))
                if created is not None:
                    account_created_ts = (created if account_created_ts is None
                                          else min(account_created_ts, created))
                try:
                    posts = int(author.get("statusesCount"))
                    stated_posts = max(stated_posts or 0, posts)
                except (TypeError, ValueError):
                    pass

                is_reply = bool(t.get("isReply") or t.get("inReplyToId"))
                is_quote = bool(t.get("quoted_tweet"))
                if is_retweet:
                    composition["native retweets"] += 1
                if is_reply:
                    composition["replies"] += 1
                if is_quote:
                    composition["quotes"] += 1
                if not (is_retweet or is_reply or is_quote):
                    composition["originals"] += 1

                rendered, clipped = format_record(
                    t, command="workflows.py history <user>", schema=TWEET_FIELDS,
                    fields=getattr(a, "fields", None),
                    defaults=("id", "created_at", "text"),
                    full=getattr(a, "full", False))
                fh.write(json.dumps(rendered, ensure_ascii=False) + "\n")
                was_text_truncated = was_text_truncated or clipped
                n += 1
            if stop_all_passes:
                break
    finally:
        if a.out:
            fh.close()

    print("[history] composition: " + " | ".join(
        f"{name} {count:,}" for name, count in composition.items())
        + " (reply/quote flags can overlap)", file=sys.stderr)
    if stated_posts is not None:
        upper_usd = credits_to_usd(stated_posts * 15 + len(queries) * 15)
        print(f"[history] cost upper bound from {stated_posts:,} stated posts: "
              f"<= ${upper_usd:,.2f} at 15 credits/post plus request floors. "
              f"Actual spend can be lower because filters, deletions, excluded "
              f"native retweets, and search-index depth reduce reachable results.",
              file=sys.stderr)

    if a.user and not n and not truncated:
        # Zero posts is ambiguous: a quiet account, or a handle that does not
        # exist (search answers both with an empty page). Resolve the identity
        # only at this boundary; user_info raises for a missing account.
        try:
            profile = c.user_info(a.user)
        except APIError as exc:
            emit_line(error("api", f"@{a.user} could not be resolved: {exc}",
                       "workflows.py history",
                       "Check the handle spelling; suspended, deleted, and "
                       "nonexistent accounts have no searchable history"))
            return 1
        except CostLimitExceeded as exc:
            truncated = True
            truncated_reason = (f"0 posts found, but confirming @{a.user} exists "
                                f"would exceed the ceiling: {exc}")
        else:
            if not isinstance(profile, dict) or profile.get("id") in (None, ""):
                truncated = True
                truncated_reason = (f"0 posts found and the profile lookup for "
                                    f"@{a.user} returned no account id, so an "
                                    f"empty history cannot be confirmed")

    # Only an OPEN-ENDED handle history claims lifetime coverage, so only it
    # is checked against account creation. A dated window is not: its walk
    # ends by proven exhaustion, and a gap between --since and the oldest
    # post is the ordinary case for any account that was quiet at the start
    # of the window (measured 2026-10-23: @karpathy, --since 2026-10-01,
    # oldest post the next day, was falsely labelled partial by a --since
    # variant of this check).
    index_limited = False
    if (not truncated and a.user and not a.since and oldest_ts is not None
            and account_created_ts is not None
            and stated_posts is not None and stated_posts > n
            and oldest_ts > account_created_ts + 86400):
        index_limited = True
        truncated_reason = ("full-lifetime coverage cannot be established because "
                            "the search index does not reach account creation")
        oldest_day = datetime.fromtimestamp(oldest_ts, UTC).date()
        created_day = datetime.fromtimestamp(account_created_ts, UTC).date()
        gap_days = (oldest_day - created_day).days
        day_word = "day" if gap_days == 1 else "days"
        print(f"[history] INDEX COVERAGE: PARTIAL — the search reached back to "
              f"{oldest_day}, {gap_days:,} {day_word} after @{a.user}'s "
              f"{created_day} account creation. "
              f"The emitted data is valid, but full-lifetime coverage cannot be "
              f"established: search-index depth, filters, deletions, and quiet "
              f"periods can all separate these dates.",
              file=sys.stderr)
    print(f"{n:,} tweets | {c.spend_report()}", file=sys.stderr)
    complete = not (truncated or index_limited)
    if not truncated:
        finish_walk(c, walk_key)
    summary = success(
        "workflows.py history", {
            "tweets_count": f"{n} of {n if complete else 'unknown'} total",
            "tweets_state": (None if n else
                             f"0 posts found for {q!r} between {a.since or 'any'} "
                             f"and {a.until or 'now'}"),
            "artifact": a.out,
            "help": (["Run `workflows.py history <user> --full` for complete text"]
                     if was_text_truncated else []),
        }, complete=complete, reason=truncated_reason,
        resume=("Rerun the same command within 24 hours with a new `--out`: "
                "pages already bought replay free. Raise `--max-usd <amount>` "
                "or `--max-pages <count>` if one of them stopped the walk"
                if not complete else None), records_returned=n)
    print(json.dumps(summary, ensure_ascii=False) if not a.out else
          json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


# ----------------------------------------------------------------- monitor --
def _monitor_params(users, interval, lookback, overlap):
    """Validated handle list. Raises UsageError before any client or request:
    an empty handle list would send and bill the query `()`, and a zero or
    negative interval cannot be polled."""
    if isinstance(users, str):
        users = users.split(",")
    handles = [_clean_handle(u, "each monitored user") for u in users if u.strip()]
    if not handles:
        raise UsageError("monitor needs at least one handle, e.g. `alice,bob`")
    if not interval or interval <= 0:
        raise UsageError(f"--interval must be greater than 0 seconds, got {interval}")
    if lookback is None or lookback <= 0:
        raise UsageError(f"--lookback must be greater than 0 seconds, got {lookback}")
    if overlap is None or overlap < 0:
        raise UsageError(f"--overlap must be 0 or more seconds, got {overlap}")
    return handles


def _save_monitor_state(path, window_start, seen_ids):
    """Atomic checkpoint: a crash mid-write must not leave unreadable state."""
    tmp = f"{path}.tmp"
    with open(tmp, "w") as fh:
        json.dump({"last_ts": window_start,
                   "seen_ids": seen_ids[-MONITOR_SEEN_CAP:]}, fh)
    os.replace(tmp, path)


def monitor(users, *, interval=60, max_usd=None, state_file=None, client=None,
            once=False, on_tweet=None, lookback=900, overlap=MONITOR_OVERLAP):
    """Poll for new tweets from a set of accounts.

    Polling advanced_search over a sliding since_time window is the vendor's
    own documented monitoring pattern — cheaper and more reliable than polling
    last_tweets, and it needs no webhook endpoint or billable server-side rule.

    On a cold start (no state file) the first poll looks back `lookback`
    seconds. Without that the first window would be [now, now] — zero width —
    and the run would report nothing with no indication why.

    Each poll re-scans `overlap` seconds before the previous window end, so a
    post the index publishes late is still found; seen ids drop the repeats.

    Checkpoint (`state_file`): `last_ts` is the start of the window not yet
    fully walked; it advances only after a whole window completes. `seen_ids`
    is saved after EVERY emitted tweet, so a poll stopped by the ceiling or a
    crash resumes the same window without re-emitting what was already sent.
    """
    users = _monitor_params(users, interval, lookback, overlap)
    c = _bounded_client(client, max_usd)
    state = {}
    if state_file and os.path.exists(state_file):
        with open(state_file) as fh:
            state = json.load(fh)
    window_start = int(state.get("last_ts") or (time.time() - lookback))
    # A list, not a set: the trim must drop the OLDEST ids, and a set has no
    # order, so list(set)[-N:] keeps an arbitrary subset.
    seen_ids = list(state.get("seen_ids") or [])[-MONITOR_SEEN_CAP:]
    seen_set = set(seen_ids)
    # One OR query per group of handles that fits X's query length.
    queries = []
    for user in users:
        if queries and len(queries[-1]) + len(user) + 9 <= MAX_QUERY_CHARS:
            queries[-1] += f" OR from:{user}"
        else:
            queries.append(f"from:{user}")
    if state_file and "last_ts" not in state:
        # Pin the cold-start window now: a first poll that stops early must
        # resume here, not at a later now - lookback that skips posts.
        _save_monitor_state(state_file, window_start, seen_ids)

    while True:
        now = int(time.time())
        # advanced_search caps at 20 per call. A single request would silently
        # drop everything past the 20th tweet in a busy interval, so walk the
        # window with the shared sliding-window walker.
        new = []
        for t in (t for q in queries for t in search_window(
                c, f"({q})", max(0, window_start - overlap), now, replay=False)):
            tweet_id = t.get("id")
            if tweet_id in seen_set:
                continue
            seen_set.add(tweet_id)
            seen_ids.append(tweet_id)
            new.append(t)
            (on_tweet or _print_tweet)(t)
            if len(seen_ids) > 2 * MONITOR_SEEN_CAP:
                seen_ids = seen_ids[-MONITOR_SEEN_CAP:]
                seen_set = set(seen_ids)
            if state_file:              # emitted first: at-least-once on crash
                _save_monitor_state(state_file, window_start, seen_ids)
        window_start = now              # the whole window has been walked
        if state_file:
            _save_monitor_state(state_file, window_start, seen_ids)
        print(f"  [{datetime.now():%H:%M:%S}] {len(new)} new "
              f"| ${c.spent_usd:,.4f} spent", file=sys.stderr)
        if once:
            return new
        if c._over_ceiling():
            raise CostLimitExceeded(
                f"Monitor stopped: ${c.spent_usd:,.2f} of ${c.max_usd:,.2f} "
                f"spent. Tweets already emitted are valid.")
        time.sleep(interval)


def _print_tweet(t):
    a = (t.get("author") or {}).get("userName", "?")
    print(json.dumps({"id": t.get("id"), "author": a,
                      "createdAt": t.get("createdAt"),
                      "text": t.get("text"),
                      "likeCount": t.get("likeCount")}, ensure_ascii=False))


def _monitor_cli(a):
    users = _monitor_params(a.users, a.interval, a.lookback, a.overlap)
    c, _ = _cli_session(a)
    with c.store:
        return _monitor_run(a, c, users)


def _monitor_run(a, c, users):
    print(f"monitoring {users} every {a.interval}s "
          f"(~${credits_to_usd(15) * (86400 / a.interval):.2f}/day at minimum billing)",
          file=sys.stderr)
    state = {"text_truncated": False, "records": 0}
    interrupted = False

    def output_tweet(tweet):
        # The whole tweet goes to format_record: a pre-trimmed copy made every
        # --fields beyond the five kept here (views, quotes, ...) read null.
        rendered, clipped = format_record(
            tweet, command="workflows.py monitor <users>", schema=TWEET_FIELDS,
            fields=getattr(a, "fields", None), defaults=("id", "created_at", "text"),
            full=getattr(a, "full", False))
        state["text_truncated"] = state["text_truncated"] or clipped
        state["records"] += 1
        print(json.dumps(rendered, ensure_ascii=False))
    try:
        monitor(users, interval=a.interval, client=c, state_file=a.state,
                once=a.once, lookback=a.lookback, overlap=a.overlap,
                on_tweet=output_tweet)
    except KeyboardInterrupt:
        interrupted = True
        print(f"\nstopped | {c.spend_report()}", file=sys.stderr)
    except (CostLimitExceeded, IncompleteDataError) as e:
        # Preserve usable records and carry truncation in the final payload;
        # exit 0 alone never claims completeness. Single-line: this summary
        # ends a JSON Lines stream.
        print(f"\nSTOPPED: {e}", file=sys.stderr)
        restart = ("restart from `--state <path>` to resume the same window"
                   if a.state else "rerun with `--state <path>` to make runs resumable")
        emit_line(success("workflows.py monitor", {
                        "records_count": f"{state['records']} of unknown total",
                        "records_state": (None if state["records"] else
                                          "0 new posts emitted before the monitor stopped")},
                    complete=False, reason=str(e),
                    resume=(f"Raise `--max-usd <amount>` and {restart}"
                            if isinstance(e, CostLimitExceeded)
                            else restart[0].upper() + restart[1:]),
                    records_returned=state["records"]))
        return 0
    summary = success("workflows.py monitor", {
        "records_count": (f"{state['records']} of "
                          f"{'unknown' if interrupted else state['records']} total"),
        "records_state": (None if state["records"] else
                          "0 new posts found for the monitored accounts"),
        "help": (["Run `workflows.py monitor <users> --full` for complete text"]
                 if state["text_truncated"] else [])},
        complete=not interrupted,
        reason=("monitor stopped by the user" if interrupted else None),
        resume=("Rerun with `--state <path>` to resume" if interrupted else None),
        records_returned=state["records"])
    print(json.dumps(summary, ensure_ascii=False))
    return 0


def _main(argv=None):
    if fast_version(argv):
        return 0
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv:
        emit(dashboard(
            __file__, "Run streaming twitterapi.io audience, history, and monitor workflows"))
        return 0
    p = _parser()
    args = p.parse_args(argv)
    if args.cmd == "history" and not (args.user or args.query):
        raise UsageError("history needs a USER or --query")
    parse_fields(args.fields,
                 ACCOUNT_FIELDS if args.cmd == "audience" else TWEET_FIELDS,
                 f"workflows.py {args.cmd}")
    return args.func(args)


def _parser():
    p = ArgumentParser(description=__doc__, formatter_class=HelpFormatter)
    p.add_argument("--max-usd", type=float, default=5.0,
                   help="hard spend ceiling for this run (default 5.00)")
    sub = p.add_subparsers(dest="cmd", required=True)

    a = sub.add_parser("audience", help="enumerate an account's followers",
                       formatter_class=HelpFormatter,
                       epilog=("Examples:\n"
                               "  workflows.py audience <user> --limit <count>\n"
                               "  workflows.py audience <user> --profiles "
                               "--fields id,user_name,name"))
    a.add_argument("user", help="account handle")
    a.add_argument("--limit", type=int, help="maximum follower records")
    a.add_argument("--profiles", dest="ids_only", action="store_false",
                   help="fetch full profiles (2.2x cost) instead of IDs")
    a.add_argument("--verified-only", action="store_true",
                   help="use the verified-followers endpoint")
    a.add_argument("--out", help="write record JSONL to this new path")
    a.set_defaults(func=_audience_cli)

    h = sub.add_parser("history", help="historical tweet search",
                       formatter_class=HelpFormatter,
                       epilog=("Examples:\n"
                               "  workflows.py history <user> --since <YYYY-MM-DD>\n"
                               "  workflows.py history --query \"<query>\" --max-pages <count>"))
    h.add_argument("user", nargs="?", help="account handle (shorthand for from:USER)")
    h.add_argument("--query", help="raw X search query instead of a handle")
    h.add_argument("--since", help="YYYY-MM-DD")
    h.add_argument("--until", help="YYYY-MM-DD")
    h.add_argument("--exclude-replies", action="store_true")
    h.add_argument("--include-retweets", action="store_true",
                   help="second pass with filter:nativeretweets to capture "
                        "retweets, which `from:` alone omits entirely")
    h.add_argument("--max-pages", type=int, help="maximum pages per search pass")
    h.add_argument("--index-faves", action="store_true",
                   help="let the search index apply min_faves: far cheaper, "
                        "but it can miss posts whose likes rose recently")
    h.add_argument("--out", help="write record JSONL to this new path")
    h.set_defaults(func=_history_cli)

    m = sub.add_parser("monitor", help="poll accounts for new tweets",
                       formatter_class=HelpFormatter,
                       epilog=("Examples:\n"
                               "  workflows.py monitor <user>,<user> --once\n"
                               "  workflows.py monitor <users> --state <path>"))
    m.add_argument("users", help="comma-separated handles")
    m.add_argument("--interval", type=int, default=60,
                   help="poll interval in seconds")
    m.add_argument("--state", help="checkpoint JSON path for resume")
    m.add_argument("--once", action="store_true", help="poll once and exit")
    m.add_argument("--lookback", type=int, default=900,
                   help="cold-start lookback window in seconds (default 900)")
    m.add_argument("--overlap", type=int, default=MONITOR_OVERLAP,
                   help="seconds each poll re-scans before the last window, "
                        "to catch late-indexed posts; re-scanned posts are billed")
    m.set_defaults(func=_monitor_cli)
    for q in (a, h, m):
        q.add_argument("--max-usd", type=float, default=argparse.SUPPRESS,
                       help="hard spend ceiling (default 5.00)")
        q.add_argument("--store", default=argparse.SUPPRESS,
                       help="SQLite store path (default ~/.twitterapi-cache/store.db)")
        add_common_output_flags(q)
    return p


def main(argv=None):
    return run_cli("workflows.py", _main, argv, parser=_parser, dests=("cmd",))


if __name__ == "__main__":
    sys.exit(main())
