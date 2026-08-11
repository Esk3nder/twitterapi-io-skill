#!/usr/bin/env python3
"""Analytical workflows: SEED -> COHORT -> CORPUS -> SIGNAL. Stdlib only.

These compose the primitives (twitterapi.Client, store.Store, cohort.Cohort)
into the high-level jobs a user actually asks for. The division of labour is
strict and deliberate:

    CODE decides WHO and fetches WHAT — cohort resolution, corpus retrieval,
    set math, follow-graph ranking, cost control. All verifiable, all cheap
    to re-run once cached.

    THE MODEL decides what it MEANS — themes, consensus, "is this organic".
    These jobs never fake that; they return a well-shaped, cheaply-acquired
    corpus and leave interpretation to the caller.

Every job takes a shared Client (with store + max_usd) so cost accrues against
one ceiling and caching spans the whole session.

    python3 jobs.py brief polymarket
    python3 jobs.py overlap stripe vercel
    python3 jobs.py diffusion <tweet_id>
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from twitterapi import Client, CostLimitExceeded, APIError  # noqa: E402
from store import Store  # noqa: E402
from cohort import Cohort  # noqa: E402


def _client(max_usd=5.0):
    return Client(verbose=False, store=Store(), max_usd=max_usd)


def _days_ago(days):
    return int(time.time()) - days * 86400


# ---------------------------------------------------------------- corpus ----
def corpus(handles, since_ts=None, until_ts=None, *, client=None):
    """ALL tweets for a set of accounts in a window, paginated per account.

    An earlier version used a single bulk_search page and silently capped every
    account at 20 tweets — any active account was massively undercounted, which
    corrupted brief/benchmark. Search pages are fixed at 20, so completeness
    requires the sliding-window walk (same technique as workflows.history).
    Content is time-sensitive, so this always hits the API.
    """
    c = client or _client()
    handles = [h.lstrip("@") for h in handles if h]
    out, seen, needs_walk = [], set(), []

    # Batch the FIRST page of every handle: bulk_search runs N queries in one
    # request, the only lever against the 20 QPS ceiling. A handle whose first
    # page is short is already complete and needs no walk at all; only handles
    # that filled a page can have more behind them.
    BATCH, PAGE = 10, 20
    for i in range(0, len(handles), BATCH):
        chunk = handles[i:i + BATCH]
        queries = []
        for h in chunk:
            q = f"from:{h}"
            if since_ts:
                q += f" since_time:{since_ts}"
            if until_ts:
                q += f" until_time:{until_ts}"
            queries.append(q)
        try:
            from store import parse_ts
            for h, tweets in zip(chunk, c.bulk_search(queries)):
                stamps = []
                for t in tweets:
                    ts = parse_ts(t.get("createdAt") or "")
                    if ts:
                        stamps.append(ts)
                    if t.get("id") not in seen:
                        seen.add(t.get("id")); out.append(t)
                if len(tweets) >= PAGE and stamps:
                    # Resume the walk BELOW this page rather than re-fetching
                    # it: overlapping would bill the first page twice. Same
                    # boundary rule as _search_window — re-include the oldest
                    # second unless the whole page shares it.
                    oldest = min(stamps)
                    resume = oldest if max(stamps) == oldest else oldest + 1
                    needs_walk.append((h, resume))
                elif len(tweets) >= PAGE:
                    needs_walk.append((h, until_ts or int(time.time())))
            if c.store:
                c.store.put_tweets(out)
        except CostLimitExceeded:
            raise
        except Exception as e:
            # Batching is an optimisation, never a correctness dependency:
            # fall back to walking every handle rather than losing data.
            print(f"[jobs] bulk_search unavailable ({type(e).__name__}); "
                  f"falling back to per-handle walks", file=sys.stderr)
            needs_walk = [(h, until_ts or int(time.time())) for h in chunk] + needs_walk

    # Only the busy handles pay for a full sliding-window walk. Dedupe by id
    # absorbs the first page the walk re-fetches.
    for h, resume_ts in needs_walk:
        # CostLimitExceeded propagates: a truncated corpus must never be
        # returned as if it were complete — brief/narrative/benchmark would
        # silently compare complete-vs-partial windows.
        for t in _search_window(c, f"from:{h}", since_ts or 0, resume_ts):
            if t.get("id") not in seen:      # the walk re-fetches page 1
                seen.add(t.get("id")); out.append(t)
    return out


# ------------------------------------------------------------ entity brief --
def entity_brief(handle, *, days=30, client=None):
    """State of an entity: its official account + adjacent people + recent
    output. Returns structure; the model writes the actual brief."""
    c = client or _client()
    info = c.user_info(handle)
    tweets = corpus([handle], since_ts=_days_ago(days), client=c)
    tweets.sort(key=lambda t: t.get("likeCount", 0), reverse=True)
    return {
        "entity": handle,
        "profile": {"name": info.get("name"), "followers": info.get("followers"),
                    "description": info.get("description"),
                    "verified": bool(info.get("isBlueVerified"))},
        "window_days": days,
        "tweet_count": len(tweets),
        "top_tweets": [{"text": t.get("text"), "likes": t.get("likeCount"),
                        "retweets": t.get("retweetCount"),
                        "createdAt": t.get("createdAt")} for t in tweets[:15]],
        "_note": "Interpretation (themes, posture) is the caller's job, not this function's.",
        "spend": c.spend_report(),
    }


# --------------------------------------------------------- narrative track --
def narrative_tracker(query, *, days=30, client=None):
    """What's emerging in a topic: this window's talkers/terms vs the prior
    window's. Returns both corpora + a computed diff of authors and hashtags;
    the model names the narrative."""
    c = client or _client()
    now = int(time.time())
    # Both windows use the SAME bounded walk so the author diff is apples-to-
    # apples. An earlier version compared an unbounded current against a
    # capped prior, misclassifying dropped prior authors as "new".
    cur = _search_window(c, query, now - days * 86400, now)
    prior = _search_window(c, query, now - 2 * days * 86400, now - days * 86400)

    def authors(ts):
        return {(t.get("author") or {}).get("userName", "").lower()
                for t in ts if t.get("author")}

    def hashtags(ts):
        h = {}
        for t in ts:
            for tag in ((t.get("entities") or {}).get("hashtags") or []):
                k = (tag.get("text") or "").lower()
                if k:
                    h[k] = h.get(k, 0) + 1
        return h

    a_now, a_prior = authors(cur), authors(prior)
    h_now, h_prior = hashtags(cur), hashtags(prior)
    return {
        "query": query, "window_days": days,
        "new_authors": sorted(a_now - a_prior)[:40],
        "current_tweet_count": len(cur), "prior_tweet_count": len(prior),
        "rising_hashtags": sorted(
            ((k, h_now[k], h_prior.get(k, 0)) for k in h_now),
            key=lambda x: x[1] - x[2], reverse=True)[:20],
        "_note": "New authors and rising tags are computed; naming the narrative is the caller's job.",
        "spend": c.spend_report(),
    }


def _search_window(c, query, since_ts, until_ts, max_pages=1000):
    """Sliding-window historical search (same technique as workflows.history).

    max_pages defaults high (effectively "until the range is exhausted"); pass a
    smaller value only when a deliberate cap is wanted. A low fixed cap here
    silently truncates a busy window — the bug that biased narrative_tracker."""
    from store import parse_ts
    out, seen, gaps = [], set(), []
    ut = until_ts
    for _ in range(max_pages):
        q = f"{query} since_time:{since_ts} until_time:{ut}"
        resp = c._raw("GET", "/twitter/tweet/advanced_search",
                      {"query": q, "queryType": "Latest"})
        batch = resp.get("tweets") or []
        c._charge("search", len(batch), 20)
        if c.store:
            c.store.put_tweets(batch)
        stamps = []
        for t in batch:
            ts = parse_ts(t.get("createdAt") or "")
            if ts:
                stamps.append(ts)
            if t.get("id") not in seen:
                seen.add(t.get("id")); out.append(t)
        if len(batch) < 20 or not stamps:
            break                       # short page = range genuinely exhausted
        # Ceiling stop is an ERROR, not an end-of-data: raise so the caller
        # knows the corpus is partial. (A bare break here made corpus()'s
        # except-CostLimitExceeded dead code and silently truncated windows.)
        if c._over_ceiling():
            raise CostLimitExceeded(
                f"Spend ceiling hit at ${c.spent_usd:,.2f} of ${c.max_usd:,.2f} "
                f"mid-window on {query!r}; the corpus for this window is "
                f"INCOMPLETE. Raise max_usd to fetch it fully.")
        oldest = min(stamps)
        if max(stamps) == oldest:
            # A FULL page inside one second: a time window cannot page within
            # a single second, so whatever else was posted in it is dropped.
            # Record it — never lose data silently. (Same guard as
            # workflows.history_search.)
            gaps.append(oldest)
            print(f"[jobs] WARNING: 20+ tweets share second {oldest}; "
                  f"time-window paging cannot split a second — some tweets "
                  f"in it are being skipped.", file=sys.stderr)
            new_ut = oldest
        else:
            new_ut = oldest + 1         # re-include boundary; dedupe handles it
        if new_ut >= ut:
            new_ut = ut - 1             # force progress rather than refetch
        ut = new_ut
        if ut <= since_ts:
            break                       # boundary reached = clean exhaustion
    if gaps:
        print(f"[jobs] INCOMPLETE: {len(gaps)} second(s) had more tweets than "
              f"a time window can retrieve; results are missing some tweets at "
              f"{gaps[:5]}{'...' if len(gaps) > 5 else ''}", file=sys.stderr)
    return out


# --------------------------------------------------------- authority map ----
def authority_map(seed_query, *, cohort_limit=150, max_usd=5.0, client=None):
    """Who a scene itself follows (frontier), from a topic seed. Expensive —
    the follow-graph crawl — so it estimates and refuses past max_usd."""
    c = client or _client(max_usd)
    co = Cohort.from_search(seed_query, limit=cohort_limit, client=c, store=c.store)
    co.authority(max_usd=max_usd, confirm=True)
    ranked = co.top(30)
    complete = getattr(co, "authority_complete", True)
    return {
        "seed": seed_query, "cohort_size": len(co),
        "complete": complete,
        "members_crawled": getattr(co, "authority_crawled", len(co)),
        "frontier": [{"handle": m["user_name"], "in_degree": m["weight"]}
                     for m in ranked if m["weight"] > 0],
        "_warning": None if complete else
        "PARTIAL: spend ceiling hit mid-crawl; later members' follows were not "
        "counted. Raise max_usd for a complete frontier.",
        "spend": c.spend_report(),
    }


# ------------------------------------------------------------- overlap ------
def overlap(handle_a, handle_b, *, ids_only=True, max_usd=5.0, client=None):
    """Shared followers of two accounts. Cheap after first crawl — the whole
    point of caching. Returns counts + a sample of overlapping ids."""
    c = client or _client(max_usd)
    # Estimate BEFORE crawling: full follower sets of two large accounts can be
    # tens of dollars. Refuse up front rather than aborting mid-crawl.
    na = c.follower_count(handle_a)
    nb = c.follower_count(handle_b)
    est = c.estimate("follower_ids", na) + c.estimate("follower_ids", nb)
    if est > max_usd:
        raise CostLimitExceeded(
            f"overlap({handle_a},{handle_b}) ~= ${est:,.2f} "
            f"({na:,}+{nb:,} followers), over ${max_usd:,.2f}. Raise --max-usd "
            f"to run it (results cache, so a re-run is free).")
    A = Cohort(client=c, store=c.store, label=f"followers:{handle_a}")
    for uid in c.follower_ids(handle_a):
        A._add(uid, None, 1.0, "follower")
    B = Cohort(client=c, store=c.store, label=f"followers:{handle_b}")
    for uid in c.follower_ids(handle_b):
        B._add(uid, None, 1.0, "follower")
    both = A.intersect(B)
    empty = len(A) == 0 or len(B) == 0
    return {
        "a": handle_a, "b": handle_b,
        "a_followers": len(A), "b_followers": len(B), "overlap": len(both),
        "jaccard": round(len(both) / max(1, len(A) + len(B) - len(both)), 4),
        "sample_ids": both.ids()[:50],
        "_warning": ("one or both accounts returned 0 followers (suspended, "
                     "private, or renamed) — the overlap is not meaningful"
                     if empty else None),
        "spend": c.spend_report(),
    }


# --------------------------------------------------- authenticity audit -----
def authenticity_audit(handle, *, sample=1000, client=None):
    """Signals for organic-vs-purchased audience: follower account ages,
    default-ish handles, follower/following skew. Returns signals; the model
    renders the verdict."""
    c = client or _client()
    info = c.user_info(handle)
    followers = list(c.paginate("followers", handle, limit=sample))
    now = datetime.now(timezone.utc)
    ages, no_bio, egg_like = [], 0, 0
    from store import parse_ts
    for f in followers:
        ca = f.get("created_at") or f.get("createdAt") or ""
        ts = parse_ts(ca)
        if ts:
            ages.append((now - datetime.fromtimestamp(ts, timezone.utc)).days)
        if not (f.get("description") or f.get("bio")):
            no_bio += 1
        fo = f.get("followers_count") or f.get("followers") or 0
        fr = f.get("following_count") or f.get("friends_count") or f.get("following") or 0
        if fo == 0 and fr > 500:
            egg_like += 1
    n = len(followers) or 1
    ages.sort()
    return {
        "handle": handle,
        "stated_followers": info.get("followers"),
        "sampled": len(followers),
        "_warning": ("0 followers sampled — account may be suspended/private; "
                     "percentages below are not meaningful" if not followers else None),
        "median_follower_age_days": ages[len(ages) // 2] if ages else None,
        # Denominator is the PARSED subset: dividing by the whole sample
        # understates the young-account signal when created_at fails to parse.
        "pct_accounts_under_90d": (round(100 * sum(a < 90 for a in ages)
                                         / len(ages), 1) if ages else None),
        "created_at_parse_failures": len(followers) - len(ages),
        "pct_no_bio": round(100 * no_bio / n, 1),
        "pct_egg_like": round(100 * egg_like / n, 1),
        "_note": "These are signals, not a verdict. Interpretation is the caller's job.",
        "spend": c.spend_report(),
    }


# ------------------------------------------------------- diffusion trace ----
def diffusion_trace(tweet_id, *, limit=500, client=None):
    """How a post spread and who moved early.

    Replies and quotes ARE tweets with a real createdAt, so they can be
    time-ordered — that is the "earliest movers" signal. Retweeters come back
    as PROFILES with no retweet timestamp; their createdAt is account-creation
    date, which must NOT be used to order them (doing so invents a false
    timeline). They are reported as a separate count, not placed on the axis.

    `limit` bounds each of the three crawls (replies, quotes, retweeters) —
    a moderately viral tweet at the old fixed 500 cost ~$0.22 with no lever.
    CostLimitExceeded propagates: a truncated trace must never be returned as
    if it were complete (same contract as Cohort.from_engagers).
    """
    from store import parse_ts
    c = client or _client()
    timed, retweeters = [], 0
    for kind, ep in (("reply", "replies_v2"), ("quote", "quotes")):
        try:
            for rec in c.paginate(ep, str(tweet_id), limit=limit):
                a = rec.get("author") or {}
                ts = parse_ts(rec.get("createdAt") or "")
                if ts:
                    timed.append({"kind": kind,
                                  "handle": a.get("userName") or a.get("screen_name"),
                                  "followers": a.get("followers"),
                                  "createdAt": rec.get("createdAt"), "_ts": ts})
        except CostLimitExceeded:
            raise               # partial data must not masquerade as a trace
        except Exception as e:
            print(f"[jobs] {kind} fetch failed: {e}", file=sys.stderr)
    try:
        retweeters = len(list(c.paginate("retweeters", str(tweet_id), limit=limit)))
    except CostLimitExceeded:
        raise
    except Exception as e:
        print(f"[jobs] retweeter fetch failed: {e}", file=sys.stderr)
    timed.sort(key=lambda e: e["_ts"])
    for e in timed:
        e.pop("_ts", None)
    return {
        "tweet_id": tweet_id,
        "timed_engagers": len(timed), "retweeter_count": retweeters,
        "earliest_movers": timed[:25],
        "by_kind": {"reply": sum(1 for e in timed if e["kind"] == "reply"),
                    "quote": sum(1 for e in timed if e["kind"] == "quote"),
                    "retweet": retweeters},
        "_note": "Retweeters lack a retweet timestamp and are counted, not time-ordered.",
        "spend": c.spend_report(),
    }


# --------------------------------------------------------- cohort drift -----
def cohort_drift(name, v_old=None, v_new=None, *, client=None):
    """Who joined, left, or rose between two saved resolutions of a cohort."""
    c = client or _client()
    versions = c.store.cohort_versions(name)
    if len(versions) < 2:
        raise ValueError(
            f"cohort '{name}' has versions {versions or '[]'} — drift needs at "
            f"least 2 resolutions of the same name. Re-resolve and save it again.")
    v_old = v_old if v_old is not None else versions[0]
    v_new = v_new if v_new is not None else versions[-1]
    if v_old == v_new:
        raise ValueError(f"v_old and v_new are both {v_old}; pick two different "
                         f"versions from {versions}.")
    old = Cohort.load(name, v_old, client=c, store=c.store)
    new = Cohort.load(name, v_new, client=c, store=c.store)
    joined = new.minus(old)
    left = old.minus(new)
    return {
        "cohort": name, "from_version": v_old, "to_version": v_new,
        "old_size": len(old), "new_size": len(new),
        "joined": [m["user_name"] or m["account_id"] for m in joined][:60],
        "left": [m["user_name"] or m["account_id"] for m in left][:60],
    }


# ----------------------------------------------- competitive benchmark ------
def competitive_benchmark(handles, *, days=30, client=None):
    """N entities on shared axes: reach, recent volume, top-post engagement."""
    c = client or _client()
    rows = []
    for h in handles:
        info = c.user_info(h)
        tw = corpus([h], since_ts=_days_ago(days), client=c)
        peak = max((t.get("likeCount", 0) for t in tw), default=0)
        rows.append({"handle": h, "followers": info.get("followers"),
                     "tweets_in_window": len(tw), "peak_likes": peak,
                     "verified": bool(info.get("isBlueVerified"))})
    return {"window_days": days, "entities": rows, "spend": c.spend_report()}


# Every job takes the shared client so --max-usd actually binds. Jobs that
# omitted it silently fell back to _client()'s $5 default, so `--max-usd 0.50`
# could spend ten times what was asked for.
JOBS = {
    "brief": lambda a, c: entity_brief(a.target, days=a.days, client=c),
    "narrative": lambda a, c: narrative_tracker(a.target, days=a.days, client=c),
    "authority": lambda a, c: authority_map(a.target, max_usd=a.max_usd, client=c),
    "overlap": lambda a, c: overlap(a.target, a.target2, max_usd=a.max_usd, client=c),
    "authenticity": lambda a, c: authenticity_audit(a.target, sample=a.sample, client=c),
    "diffusion": lambda a, c: diffusion_trace(a.target, client=c),
    "drift": lambda a, c: cohort_drift(a.target, a.v_old, a.v_new, client=c),
    "benchmark": lambda a, c: competitive_benchmark(a.target.split(","),
                                                    days=a.days, client=c),
}


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("job", choices=JOBS)
    p.add_argument("target", help="handle / query / tweet_id / cohort name / csv")
    p.add_argument("target2", nargs="?", help="second handle for overlap")
    p.add_argument("--days", type=int, default=30)
    p.add_argument("--sample", type=int, default=1000)
    p.add_argument("--max-usd", type=float, default=5.0)
    p.add_argument("--v-old", type=int)
    p.add_argument("--v-new", type=int)
    a = p.parse_args(argv)
    if a.job == "overlap" and not a.target2:
        p.error("overlap needs two handles: jobs.py overlap A B")
    c = _client(a.max_usd)          # one client, one ceiling, shared cache
    try:
        print(json.dumps(JOBS[a.job](a, c), indent=1, ensure_ascii=False))
    except (CostLimitExceeded, ValueError) as e:
        print(f"REFUSED/STOPPED: {e}", file=sys.stderr)
        return 2
    except APIError as e:
        print(f"API error: {e}\nCheck the handle/id exists and that "
              f"TWITTERAPI_IO_KEY is valid.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
