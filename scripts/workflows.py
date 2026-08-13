#!/usr/bin/env python3
"""Triggerable workflows over twitterapi.io. Standard library only.

Each workflow is a CLI entry point AND an importable function. They exist so an
agent never has to re-derive parameter names, pagination strategy or cost — all
three are where unaided agents measurably fail.

    python3 workflows.py audience   elonmusk --ids-only --limit 50000
    python3 workflows.py history    openai --since 2026-01-01 --until 2026-02-01
    python3 workflows.py monitor    elonmusk,openai --interval 60

Every workflow prints a cost estimate and refuses to exceed --max-usd
(default $5). Nothing here writes to X.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from twitterapi import (Client, CostLimitExceeded, IncompleteDataError,
                        credits_to_usd, enrich_tweet_media, filter_search_tweets,
                        prepare_search_query)  # noqa: E402
from store import Store  # noqa: E402
from axi import (ACCOUNT_FIELDS, TWEET_FIELDS, ArgumentParser, UsageError,
                 HelpFormatter, add_common_output_flags, dashboard, emit, error, fast_version,
                 format_record, parse_fields, run_cli, success)  # noqa: E402

TW_FMT = "%a %b %d %H:%M:%S %z %Y"      # verified: "Mon Aug 10 17:16:53 +0000 2026"


def parse_created_at(s):
    return datetime.strptime(s, TW_FMT)


def _strictest(*limits):
    """Lowest non-None ceiling. A caller passing max_usd=5 must never be
    loosened by a client that happens to carry max_usd=100."""
    vals = [v for v in limits if v is not None]
    return min(vals) if vals else None


def _emit(records, out):
    """JSONL to a file or stdout. JSONL so huge crawls stream without buffering."""
    fh = open(out, "w") if out else sys.stdout
    try:
        for r in records:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    finally:
        if out:
            fh.close()


# ---------------------------------------------------------------- audience --
def audience(user, *, ids_only=True, limit=None, max_usd=5.0,
             verified_only=False, client=None, quiet=False):
    """Yield an account's followers (IDs by default, profiles on request).

    ids_only=True uses /twitter/user/followers_ids: 0.45 credits/record at
    5000 per call, versus 1.0 at 200 per call for full profiles. That is 2.2x
    cheaper and 25x fewer requests. Only turn it off when you actually need
    profile fields (bio, follower counts, verified status).

    Raises CostLimitExceeded BEFORE spending if the full crawl would exceed
    max_usd. Records already yielded are always paid for and valid.
    """
    c = client or Client(max_usd=max_usd)
    c.max_usd = _strictest(c.max_usd, max_usd)
    info = c.user_info(user)            # Client.user_info charges its own 18 credits
    total, uid = int(info.get("followers") or 0), info.get("id")
    want = min(limit, total) if limit else total
    c.last_audience_total = total
    c.last_audience_context = {"account_followers": total, "requested": want}
    endpoint = ("verified_followers" if verified_only
                else "follower_ids" if ids_only else "followers")
    est = c.estimate(endpoint, want)

    if not quiet:
        print(f"@{user}: {total:,} followers | {want:,} via {endpoint} "
              f"| est ${est:,.2f}", file=sys.stderr)
        if ids_only and not verified_only and want > 1000:
            print(f"  full profiles would cost ${c.estimate('followers', want):,.2f}",
                  file=sys.stderr)
    if est > c.max_usd:
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
    c = Client(max_usd=a.max_usd)
    got = 0
    fh = open(a.out, "w") if a.out else sys.stdout
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
    except CostLimitExceeded as e:
        truncated = True
        refused = not got
        truncated_reason = str(e)
        print(f"[audience] {'stopped' if got else 'refused'}: {e}", file=sys.stderr)
    except IncompleteDataError as e:
        truncated = True
        truncated_reason = str(e)
        print(f"[audience] partial: {e}", file=sys.stderr)
    finally:
        if a.out:
            fh.close()
    print(f"{got:,} records | {c.spend_report()}", file=sys.stderr)
    if refused:
        emit(error("refusal", truncated_reason, "workflows.py audience",
                   "Pass `--limit <count>` or raise `--max-usd <amount>`"))
        return 2
    complete = not truncated
    total = getattr(c, "last_audience_total", got)
    summary = success(
        "workflows.py audience", {
            "records_count": f"{got} of {total} total",
            "records_state": (None if got else
                              f"0 followers found for @{a.user} in the requested scope"),
            "artifact": a.out,
            "help": (["Run `workflows.py audience <user> --profiles` for profile fields"]
                     + (["Run `workflows.py audience <user> --limit <count>` with a larger count to show more"]
                        if got < total and complete else [])
                     + (["Run `workflows.py audience <user> --full` for complete text"]
                        if was_truncated else [])),
        }, complete=complete, reason=truncated_reason,
        resume=("Raise `--max-usd <amount>` and rerun" if truncated else None),
        records_returned=got)
    print(json.dumps(summary, ensure_ascii=False) if not a.out else
          json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


# ----------------------------------------------------------------- history --
def history_search(query, since=None, until=None, *, max_usd=5.0, out=None,
                   max_pages=None, client=None, progress=True,
                   _since_ts=None, _until_ts=None):
    """Historical tweet search using a SLIDING TIME WINDOW, not cursors.

    twitterapi.io's own guide reports that cursor pagination infinite-loops on
    historical ranges (2019-2022). advanced_search caps at 20 tweets per call,
    so the reliable walk is: request newest-first within [since, until), take
    the OLDEST createdAt in the page, and set until_time to one second before
    it. Terminate when a page returns fewer than 20 results or the boundary
    stops moving.

    Yields tweet dicts newest-first. Deduplicates by tweet id.
    """
    c = client or Client(max_usd=max_usd)
    query, local_min_faves = prepare_search_query(query)
    c.max_usd = _strictest(c.max_usd, max_usd)
    since_ts = _since_ts if _since_ts is not None else (
        int(datetime.strptime(since, "%Y-%m-%d")
            .replace(tzinfo=timezone.utc).timestamp()) if since else None)
    until_ts = _until_ts if _until_ts is not None else (
        int(datetime.strptime(until, "%Y-%m-%d")
            .replace(tzinfo=timezone.utc).timestamp()) if until else None)
    seen, pages = set(), 0
    while True:
        q = query
        if since_ts:
            q += f" since_time:{since_ts}"
        if until_ts:
            q += f" until_time:{until_ts}"
        resp = c._raw("GET", "/twitter/tweet/advanced_search",
                      {"query": q, "queryType": "Latest"},
                      max_credits=Client.page_credits("search", 20, 20))
        batch = [enrich_tweet_media(tweet)
                 for tweet in (resp.get("tweets") or [])]
        c._charge("search", len(batch), 20)
        pages += 1

        # A fetched page is already paid for. Persist it before yielding so a
        # caller that stops consuming later still retains every acquired row,
        # and the documented $0 catalogue/threads jobs see the same corpus.
        if c.store is not None and batch:
            c.store.put_tweets(batch)

        # Timestamps come from the WHOLE page, not just fresh tweets. Using only
        # fresh ones leaves oldest=None on an all-duplicate page and terminates
        # the crawl early with data still unretrieved.
        qualifying = filter_search_tweets(batch, local_min_faves)
        qualifying_ids = {t.get("id") for t in qualifying}
        fresh, stamps = 0, []
        for t in batch:
            try:
                stamps.append(int(parse_created_at(t["createdAt"]).timestamp()))
            except Exception:
                pass
            if t.get("id") in seen:
                continue
            seen.add(t.get("id"))
            if t.get("id") not in qualifying_ids:
                continue
            fresh += 1
            yield t

        if progress:
            print(f"  page {pages}: {len(batch)} returned, {fresh} new, "
                  f"{len(seen):,} total, ${c.spent_usd:,.4f}", file=sys.stderr)

        if len(batch) < 20:
            break                       # short page = genuinely end of range
        if not stamps:
            raise IncompleteDataError(
                f"full search page had no parseable createdAt timestamps; "
                f"the {len(seen):,} tweets already yielded are valid, but no "
                f"safe resume boundary exists")
        # A full page may require another paid request. At exact exhaustion,
        # stop honestly before that next request; a short terminal page above
        # already proves completeness and is allowed to finish at the ceiling.
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
        if max(stamps) == oldest:
            # Every tweet on a FULL page shares one second. A time window
            # cannot page within a single second, so advancing past it drops
            # whatever else was posted in that second. Record it — never lose
            # data silently.
            if progress:
                print(f"  WARNING: 20+ tweets share second {oldest} "
                      f"({datetime.fromtimestamp(oldest, timezone.utc):%Y-%m-%d %H:%M:%S}Z). "
                      f"Time-window paging cannot split a second — some tweets "
                      f"in it are unreachable and are being skipped.",
                      file=sys.stderr)
            raise IncompleteDataError(
                f"20+ tweets share second {oldest}; time-window paging cannot "
                f"split it, so the result is partial.")
        else:
            # Re-include the boundary second; dedupe by id handles the overlap.
            new_until = oldest + 1

        if until_ts is not None and new_until >= until_ts:
            new_until = until_ts - 1    # force progress rather than loop
        until_ts = new_until
        if since_ts and until_ts <= since_ts:
            break
def _history_cli(a):
    if a.exclude_retweets and not a.user:
        emit(error("refusal", "--exclude-retweets cannot be combined with --query; "
                   "a raw query may explicitly select native retweets",
                   "workflows.py history",
                   "Put `-filter:nativeretweets` in `<query>` explicitly"))
        return 2
    c = Client(max_usd=a.max_usd)
    if c.store is None:
        store_path = getattr(a, "store", None)
        c.store = Store(store_path) if store_path else Store()
    q = f"from:{a.user}" if a.user else a.query
    if a.exclude_replies:
        q += " -filter:replies"
    if a.exclude_retweets:
        # No-op kept for compatibility: a `from:` query never returns native
        # retweets in the first place, so this filter removes nothing. Say so
        # rather than implying work is being done.
        print("[history] note: --exclude-retweets is redundant — `from:` search "
              "never returns native retweets. Use --include-retweets to fetch "
              "them in a second pass.", file=sys.stderr)
    if a.include_retweets and not a.user:
        emit(error("refusal", "--include-retweets needs the USER shorthand",
                   "workflows.py history",
                   "Put `filter:nativeretweets` in `<query>` explicitly"))
        return 2

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
    fh = open(a.out, "w") if a.out else sys.stdout
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
                search_query, a.since, a.until, client=c,
                max_pages=a.max_pages))
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
                except CostLimitExceeded as e:
                    # The spend ceiling is shared across passes, so another
                    # pass cannot make useful progress once it is exhausted.
                    truncated = True
                    truncated_reason = str(e)
                    stop_all_passes = True
                    print(f"\nSTOPPED: {e}", file=sys.stderr)
                    break
                # isRetweet is not 100% reliable per the vendor guide; corroborate
                # it with retweeted_tweet, and dedupe across the two searches.
                is_retweet = bool(t.get("isRetweet") or t.get("retweeted_tweet"))
                if a.exclude_retweets and is_retweet:
                    continue
                tweet_id = t.get("id")
                if tweet_id in seen:
                    continue
                seen.add(tweet_id)

                try:
                    stamp = int(parse_created_at(t["createdAt"]).timestamp())
                    oldest_ts = stamp if oldest_ts is None else min(oldest_ts, stamp)
                except Exception:
                    pass
                author = t.get("author") or {}
                try:
                    created = int(parse_created_at(author["createdAt"]).timestamp())
                    account_created_ts = (created if account_created_ts is None
                                          else min(account_created_ts, created))
                except Exception:
                    pass
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

    index_limited = False
    if (not truncated and a.user and not a.since and oldest_ts is not None
            and account_created_ts is not None
            and stated_posts is not None and stated_posts > n
            and oldest_ts > account_created_ts + 86400):
        index_limited = True
        truncated_reason = ("full-lifetime coverage cannot be established because "
                            "the search index does not reach account creation")
        oldest_day = datetime.fromtimestamp(oldest_ts, timezone.utc).date()
        created_day = datetime.fromtimestamp(account_created_ts, timezone.utc).date()
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
        resume=("Raise `--max-usd <amount>`, raise `--max-pages <count>`, or narrow "
                "the date window" if not complete else None), records_returned=n)
    print(json.dumps(summary, ensure_ascii=False) if not a.out else
          json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


# ----------------------------------------------------------------- monitor --
def monitor(users, *, interval=60, max_usd=5.0, state_file=None, client=None,
            once=False, on_tweet=None, lookback=900):
    """Poll for new tweets from a set of accounts.

    Polling advanced_search over a sliding since_time window is the vendor's
    own documented monitoring pattern — cheaper and more reliable than polling
    last_tweets, and it needs no webhook endpoint or billable server-side rule.

    On a cold start (no state file) the first poll looks back `lookback`
    seconds. Without that the first window would be [now, now] — zero width —
    and the run would report nothing with no indication why.
    """
    c = client or Client(max_usd=max_usd)
    c.max_usd = _strictest(c.max_usd, max_usd)
    state = {}
    if state_file and os.path.exists(state_file):
        state = json.load(open(state_file))
    last_seen = int(state.get("last_ts") or (time.time() - lookback))
    # A list, not a set: the trim below must drop the OLDEST ids, and a set
    # has no order, so list(set)[-5000:] keeps an arbitrary subset.
    seen_ids = list(state.get("seen_ids") or [])
    seen_set = set(seen_ids)

    while True:
        now = int(time.time())
        q = "(" + " OR ".join(f"from:{u.strip().lstrip('@')}" for u in users) + ")"
        # advanced_search caps at 20 per call. A single request would silently
        # drop everything past the 20th tweet in a busy interval, so walk the
        # window with the same sliding technique history_search uses.
        new = []
        for t in history_search(q, client=c, progress=False,
                                since=None, until=None,
                                _since_ts=last_seen, _until_ts=now):
            if t.get("id") in seen_set:
                continue
            seen_set.add(t["id"])
            seen_ids.append(t["id"])
            new.append(t)
            (on_tweet or _print_tweet)(t)
        last_seen = now
        if state_file:
            keep = seen_ids[-5000:]     # newest 5000, insertion-ordered
            json.dump({"last_ts": last_seen, "seen_ids": keep},
                      open(state_file, "w"))
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
    users = [u for u in a.users.split(",") if u.strip()]
    c = Client(max_usd=a.max_usd)
    print(f"monitoring {users} every {a.interval}s "
          f"(~${credits_to_usd(15) * (86400 / a.interval):.2f}/day at minimum billing)",
          file=sys.stderr)
    state = {"text_truncated": False, "records": 0}
    interrupted = False

    def output_tweet(tweet):
        record = {
            "id": tweet.get("id"),
            "author": (tweet.get("author") or {}).get("userName", "?"),
            "createdAt": tweet.get("createdAt"), "text": tweet.get("text"),
            "likeCount": tweet.get("likeCount"),
        }
        rendered, clipped = format_record(
            record, command="workflows.py monitor <users>", schema=TWEET_FIELDS,
            fields=getattr(a, "fields", None), defaults=("id", "created_at", "text"),
            full=getattr(a, "full", False))
        state["text_truncated"] = state["text_truncated"] or clipped
        state["records"] += 1
        print(json.dumps(rendered, ensure_ascii=False))
    try:
        monitor(users, interval=a.interval, client=c, state_file=a.state,
                once=a.once, lookback=a.lookback, on_tweet=output_tweet)
    except KeyboardInterrupt:
        interrupted = True
        print(f"\nstopped | {c.spend_report()}", file=sys.stderr)
    except (CostLimitExceeded, IncompleteDataError) as e:
        # Preserve usable records and carry truncation in the final payload;
        # exit 0 alone never claims completeness.
        print(f"\nSTOPPED: {e}", file=sys.stderr)
        emit(success("workflows.py monitor", {
                         "records_count": f"{state['records']} of unknown total",
                         "records_state": (None if state["records"] else
                                           "0 new posts emitted before the monitor stopped")},
                     complete=False, reason=str(e),
                     resume="Raise `--max-usd <amount>` and restart from `--state <path>`",
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
        emit(dashboard(__file__, "Run streaming twitterapi.io audience, history, and monitor workflows"))
        return 0
    p = ArgumentParser(description=__doc__, formatter_class=HelpFormatter)
    p.add_argument("--max-usd", type=float, default=5.0,
                   help="hard spend ceiling for this run (default 5.00)")
    sub = p.add_subparsers(dest="cmd", required=True)

    a = sub.add_parser("audience", help="enumerate an account's followers",
                       formatter_class=HelpFormatter,
                       epilog="Examples:\n  workflows.py audience <user> --limit <count>\n  workflows.py audience <user> --profiles --fields id,user_name,name")
    a.add_argument("user", help="account handle")
    a.add_argument("--limit", type=int, help="maximum follower records")
    a.add_argument("--ids-only", action="store_true", default=True,
                   help="return only account IDs")
    a.add_argument("--profiles", dest="ids_only", action="store_false",
                   help="fetch full profiles (2.2x cost) instead of IDs")
    a.add_argument("--verified-only", action="store_true",
                   help="use the verified-followers endpoint")
    a.add_argument("--out", help="write record JSONL to this path")
    a.add_argument("--max-usd", type=float, default=argparse.SUPPRESS,
                   help="hard spend ceiling (default 5.00)")
    add_common_output_flags(a)
    a.set_defaults(func=_audience_cli)

    h = sub.add_parser("history", help="historical tweet search",
                       formatter_class=HelpFormatter,
                       epilog="Examples:\n  workflows.py history <user> --since <YYYY-MM-DD>\n  workflows.py history --query \"<query>\" --max-pages <count>")
    h.add_argument("user", nargs="?", help="account handle (shorthand for from:USER)")
    h.add_argument("--query", help="raw X search query instead of a handle")
    h.add_argument("--since", help="YYYY-MM-DD")
    h.add_argument("--until", help="YYYY-MM-DD")
    h.add_argument("--exclude-replies", action="store_true")
    retweets = h.add_mutually_exclusive_group()
    retweets.add_argument("--exclude-retweets", action="store_true",
                          help="explicit no-op for USER shorthand; refused with "
                               "--query because raw queries may select retweets")
    retweets.add_argument("--include-retweets", action="store_true",
                          help="second pass with filter:nativeretweets to capture "
                               "retweets, which `from:` alone omits entirely")
    h.add_argument("--max-pages", type=int, help="maximum pages per search pass")
    h.add_argument("--store", help="sqlite path for the persisted history corpus")
    h.add_argument("--out", help="write record JSONL to this path")
    h.add_argument("--max-usd", type=float, default=argparse.SUPPRESS,
                   help="hard spend ceiling (default 5.00)")
    add_common_output_flags(h)
    h.set_defaults(func=_history_cli)

    m = sub.add_parser("monitor", help="poll accounts for new tweets",
                       formatter_class=HelpFormatter,
                       epilog="Examples:\n  workflows.py monitor <user>,<user> --once\n  workflows.py monitor <users> --state <path>")
    m.add_argument("users", help="comma-separated handles")
    m.add_argument("--interval", type=int, default=60,
                   help="poll interval in seconds")
    m.add_argument("--state", help="checkpoint JSON path for resume")
    m.add_argument("--once", action="store_true", help="poll once and exit")
    m.add_argument("--lookback", type=int, default=900,
                   help="cold-start lookback window in seconds (default 900)")
    m.add_argument("--max-usd", type=float, default=argparse.SUPPRESS,
                   help="hard spend ceiling (default 5.00)")
    add_common_output_flags(m)
    m.set_defaults(func=_monitor_cli)

    args = p.parse_args(argv)
    if args.cmd == "history" and not (args.user or args.query):
        p.error("history needs a USER or --query")
    try:
        parse_fields(args.fields,
                     ACCOUNT_FIELDS if args.cmd == "audience" else TWEET_FIELDS,
                     f"workflows.py {args.cmd}")
    except UsageError as exc:
        emit(error("usage", str(exc), f"workflows.py {args.cmd}",
                   f"Run `workflows.py {args.cmd} --help` for valid fields"))
        return 2
    try:
        return args.func(args)
    except RuntimeError as e:
        emit(error("runtime", str(e), f"workflows.py {args.cmd}",
                   "Set `TWITTERAPI_IO_KEY` in the environment and rerun"))
        return 1


def main(argv=None):
    return run_cli("workflows.py", _main, argv)


if __name__ == "__main__":
    sys.exit(main())
