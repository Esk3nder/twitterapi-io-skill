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
from twitterapi import Client, CostLimitExceeded, credits_to_usd  # noqa: E402

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


def _audience_cli(a):
    c = Client(max_usd=a.max_usd)
    got = 0
    fh = open(a.out, "w") if a.out else sys.stdout
    try:
        for rec in audience(a.user, ids_only=a.ids_only, limit=a.limit,
                            max_usd=a.max_usd, verified_only=a.verified_only,
                            client=c):
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
            got += 1
        truncated = False
    except CostLimitExceeded as e:
        truncated = True
        print(f"{'STOPPED' if got else 'REFUSED'}: {e}", file=sys.stderr)
    finally:
        if a.out:
            fh.close()
    print(f"{got:,} records | {c.spend_report()}", file=sys.stderr)
    # Non-zero on truncation: a partial dataset must not look like success.
    return 0 if not truncated else (2 if not got else 3)


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
    c.max_usd = _strictest(c.max_usd, max_usd)
    since_ts = _since_ts if _since_ts is not None else (
        int(datetime.strptime(since, "%Y-%m-%d")
            .replace(tzinfo=timezone.utc).timestamp()) if since else None)
    until_ts = _until_ts if _until_ts is not None else (
        int(datetime.strptime(until, "%Y-%m-%d")
            .replace(tzinfo=timezone.utc).timestamp()) if until else None)
    seen, pages = set(), 0
    gaps = []                       # seconds where tweets were provably dropped
    while True:
        q = query
        if since_ts:
            q += f" since_time:{since_ts}"
        if until_ts:
            q += f" until_time:{until_ts}"
        resp = c._raw("GET", "/twitter/tweet/advanced_search",
                      {"query": q, "queryType": "Latest"})
        batch = resp.get("tweets") or []
        c._charge("search", len(batch), 20)
        pages += 1

        # Timestamps come from the WHOLE page, not just fresh tweets. Using only
        # fresh ones leaves oldest=None on an all-duplicate page and terminates
        # the crawl early with data still unretrieved.
        fresh, stamps = 0, []
        for t in batch:
            try:
                stamps.append(int(parse_created_at(t["createdAt"]).timestamp()))
            except Exception:
                pass
            if t.get("id") in seen:
                continue
            seen.add(t.get("id"))
            fresh += 1
            yield t

        if progress:
            print(f"  page {pages}: {len(batch)} returned, {fresh} new, "
                  f"{len(seen):,} total, ${c.spent_usd:,.4f}", file=sys.stderr)

        # Ceiling is checked before deciding to fetch another page, not after
        # an early return, so a low ceiling cannot be silently overrun.
        if c._over_ceiling():
            raise CostLimitExceeded(
                f"Spend ceiling hit at ${c.spent_usd:,.2f} after {pages} pages; "
                f"the {len(seen):,} tweets already yielded are valid and paid for.")
        if len(batch) < 20 or not stamps:
            break                       # short page = genuinely end of range
        if max_pages and pages >= max_pages:
            print(f"  stopped at --max-pages {max_pages}; range NOT exhausted",
                  file=sys.stderr)
            break

        oldest = min(stamps)
        if max(stamps) == oldest:
            # Every tweet on a FULL page shares one second. A time window
            # cannot page within a single second, so advancing past it drops
            # whatever else was posted in that second. Record it — never lose
            # data silently.
            gaps.append(oldest)
            if progress:
                print(f"  WARNING: 20+ tweets share second {oldest} "
                      f"({datetime.fromtimestamp(oldest, timezone.utc):%Y-%m-%d %H:%M:%S}Z). "
                      f"Time-window paging cannot split a second — some tweets "
                      f"in it are unreachable and are being skipped.",
                      file=sys.stderr)
            new_until = oldest
        else:
            # Re-include the boundary second; dedupe by id handles the overlap.
            new_until = oldest + 1

        if until_ts is not None and new_until >= until_ts:
            new_until = until_ts - 1    # force progress rather than loop
        until_ts = new_until
        if since_ts and until_ts <= since_ts:
            break
    if gaps:
        print(f"  INCOMPLETE: {len(gaps)} second(s) had more tweets than a "
              f"time window can retrieve; results are missing some tweets at "
              f"{gaps[:5]}{'...' if len(gaps) > 5 else ''}", file=sys.stderr)


def _history_cli(a):
    c = Client(max_usd=a.max_usd)
    q = f"from:{a.user}" if a.user else a.query
    if a.exclude_replies:
        q += " -filter:replies"
    if a.exclude_retweets:
        q += " -filter:retweets"
    print(f"query: {q!r}  window {a.since or 'any'} -> {a.until or 'now'}",
          file=sys.stderr)
    fh = open(a.out, "w") if a.out else sys.stdout
    n = 0
    try:
        for t in history_search(q, a.since, a.until, client=c, max_pages=a.max_pages):
            # isRetweet is not 100% reliable per the vendor guide; re-check here
            if a.exclude_retweets and t.get("retweeted_tweet"):
                continue
            fh.write(json.dumps(t, ensure_ascii=False) + "\n")
            n += 1
        truncated = False
    except CostLimitExceeded as e:
        truncated = True
        print(f"\nSTOPPED: {e}", file=sys.stderr)
    finally:
        if a.out:
            fh.close()
    print(f"{n:,} tweets | {c.spend_report()}", file=sys.stderr)
    return 3 if truncated else 0


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
    try:
        monitor(users, interval=a.interval, client=c, state_file=a.state,
                once=a.once, lookback=a.lookback)
    except KeyboardInterrupt:
        print(f"\nstopped | {c.spend_report()}", file=sys.stderr)
    except CostLimitExceeded as e:
        # Non-zero on truncation, same contract as audience/history: a run cut
        # short by the ceiling must not report success.
        print(f"\nSTOPPED: {e}", file=sys.stderr)
        return 3
    return 0


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--max-usd", type=float, default=5.0,
                   help="hard spend ceiling for this run (default 5.00)")
    sub = p.add_subparsers(dest="cmd", required=True)

    a = sub.add_parser("audience", help="enumerate an account's followers")
    a.add_argument("user")
    a.add_argument("--limit", type=int)
    a.add_argument("--ids-only", action="store_true", default=True)
    a.add_argument("--profiles", dest="ids_only", action="store_false",
                   help="fetch full profiles (2.2x cost) instead of IDs")
    a.add_argument("--verified-only", action="store_true")
    a.add_argument("--out")
    a.set_defaults(func=_audience_cli)

    h = sub.add_parser("history", help="historical tweet search")
    h.add_argument("user", nargs="?", help="account handle (shorthand for from:USER)")
    h.add_argument("--query", help="raw X search query instead of a handle")
    h.add_argument("--since", help="YYYY-MM-DD")
    h.add_argument("--until", help="YYYY-MM-DD")
    h.add_argument("--exclude-replies", action="store_true")
    h.add_argument("--exclude-retweets", action="store_true")
    h.add_argument("--max-pages", type=int)
    h.add_argument("--out")
    h.set_defaults(func=_history_cli)

    m = sub.add_parser("monitor", help="poll accounts for new tweets")
    m.add_argument("users", help="comma-separated handles")
    m.add_argument("--interval", type=int, default=60)
    m.add_argument("--state", help="checkpoint file for resume")
    m.add_argument("--once", action="store_true")
    m.add_argument("--lookback", type=int, default=900,
                   help="cold-start lookback window in seconds (default 900)")
    m.set_defaults(func=_monitor_cli)

    args = p.parse_args(argv)
    if args.cmd == "history" and not (args.user or args.query):
        p.error("history needs a USER or --query")
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
