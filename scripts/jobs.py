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
from statistics import median

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from twitterapi import (APIError, Client, CostLimitExceeded,
                        IncompleteDataError, enrich_tweet_media)  # noqa: E402
from store import Store  # noqa: E402
from cohort import Cohort  # noqa: E402


def _client(max_usd=5.0, store_path=None):
    store = Store(store_path) if store_path else Store()
    return Client(verbose=False, store=store, max_usd=max_usd)


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
    # Normalize once and preserve first-seen spelling/order. Repeating @Name as
    # name must not create another paid subquery for the same account.
    normalized, handle_keys = [], set()
    for handle in handles:
        clean = handle.lstrip("@") if handle else ""
        key = clean.casefold()
        if not clean or key in handle_keys:
            continue
        handle_keys.add(key)
        normalized.append(clean)
    handles = normalized
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
            results = c.bulk_search(queries)
        except CostLimitExceeded:
            raise
        except Exception as e:
            # Batching is an optimisation, never a correctness dependency:
            # fall back to walking every handle rather than losing data.
            print(f"[jobs] bulk_search unavailable ({type(e).__name__}); "
                  f"falling back to per-handle walks", file=sys.stderr)
            needs_walk = [(h, until_ts or int(time.time())) for h in chunk] + needs_walk
            continue
        if len(results) != len(chunk):
            print(f"[jobs] bulk_search returned {len(results)} results for "
                  f"{len(chunk)} queries; falling back to per-handle walks",
                  file=sys.stderr)
            needs_walk = [(h, until_ts or int(time.time())) for h in chunk] + needs_walk
            continue
        from store import parse_ts
        chunk_tweets = []
        for h, res in zip(chunk, results):
            tweets = res["tweets"]
            chunk_tweets.extend(tweets)
            stamps = []
            for t in tweets:
                ts = parse_ts(t.get("createdAt") or "")
                if ts:
                    stamps.append(ts)
                if t.get("id") not in seen:
                    seen.add(t.get("id")); out.append(t)
            # has_next_page is authoritative. A SHORT page can still have
            # more behind it (results get filtered), so inferring
            # completeness from len(tweets) < 20 silently loses data.
            if not res["has_next_page"]:
                continue
            if stamps:
                oldest = min(stamps)
                if max(stamps) == oldest and len(tweets) >= PAGE:
                    # A full page inside ONE second: resuming below it would
                    # drop the rest of that second silently. Hand the walk
                    # the original bound so its own gap detection reports it.
                    needs_walk.append((h, until_ts or int(time.time())))
                else:
                    # Resume BELOW this page rather than re-fetching it.
                    needs_walk.append((h, oldest + 1))
            else:
                needs_walk.append((h, until_ts or int(time.time())))
        # Persistence happens after the paid acquisition/fallback boundary. A
        # local SQLite failure must propagate; it is not permission to spend
        # again by walking every handle.
        if c.store:
            c.store.put_tweets(chunk_tweets)       # this chunk only, not all of `out`

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


# ----------------------------------------------------- cached catalogue ----
def _cached_rows(handle, store):
    """Read one handle's already-paid tweets without constructing a Client."""
    own_store = store is None
    s = store or Store()
    try:
        return s.tweets_for(user_names=[handle])
    finally:
        if own_store:
            s.close()


def _raw_tweet(row):
    raw = row.get("raw") or {}
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (TypeError, ValueError):
            raw = {}
    return raw if isinstance(raw, dict) else {}


def _row_media(row):
    """First-class media list, with raw fallback for pre-migration rows."""
    media = row.get("media") or []
    if isinstance(media, str):
        try:
            media = json.loads(media)
        except (TypeError, ValueError, json.JSONDecodeError):
            media = []
    if isinstance(media, list) and media:
        return media
    from store import extract_tweet_media
    return extract_tweet_media(_raw_tweet(row))


def _media_summary(rows, *, include_artifacts=False):
    artifacts, tweets_with_media = [], 0
    for row in rows:
        items = _row_media(row)
        if not items:
            continue
        tweets_with_media += 1
        for item in items:
            artifact = {"tweet_id": str(row.get("id") or ""), **item}
            artifacts.append(artifact)
    count = len(artifacts)
    result = {
        "tweets_with_media": tweets_with_media,
        "artifact_count": count,
        # This skill inventories URLs but deliberately does not fetch bytes.
        "retrieved_count": 0,
        "undisplayed_count": count,
        "_warning": (
            f"{count} media artifact{'s are' if count != 1 else ' is'} referenced "
            "by URL only; media bytes were not downloaded, displayed, or "
            "interpreted by this result."
        ),
    }
    if include_artifacts:
        result["artifacts"] = artifacts
    return result


def media_inventory(handle, *, store=None):
    """List first-class media metadata from an already-paid local corpus."""
    result = {"handle": handle.lstrip("@")}
    result.update(_media_summary(
        _cached_rows(handle, store), include_artifacts=True))
    result["spend"] = "$0.00 (local cache only)"
    return result


def catalogue(handle, *, store=None):
    """Summarise a cached account corpus. Pure local computation; $0 API spend.

    Composition flags intentionally overlap: a quote may also be a reply, and
    self-thread replies are a subset of replies. `originals` is the exclusive
    remainder with none of the reply/quote/native-retweet flags.
    """
    rows = _cached_rows(handle, store)
    unique_ids = {r.get("id") for r in rows if r.get("id")}
    counts = {
        "total": len(rows), "unique": len(unique_ids), "originals": 0,
        "replies": 0, "self_thread_replies": 0, "quotes": 0,
        "native_retweets": 0,
    }
    metric_names = ("likes", "retweets", "replies", "quotes", "views",
                    "bookmarks")
    metric_values = {name: [] for name in metric_names}
    monthly = {}
    stamps = []

    for row in rows:
        raw = _raw_tweet(row)
        is_reply = bool(row.get("is_reply"))
        is_quote = bool(row.get("is_quote"))
        is_retweet = bool(row.get("is_retweet"))
        if is_reply:
            counts["replies"] += 1
        if is_quote:
            counts["quotes"] += 1
        if is_retweet:
            counts["native_retweets"] += 1
        if not (is_reply or is_quote or is_retweet):
            counts["originals"] += 1

        author = raw.get("author") or {}
        reply_uid = str(raw.get("inReplyToUserId") or "")
        reply_name = (raw.get("inReplyToUsername") or "").casefold()
        author_uid = str(author.get("id") or row.get("author_id") or "")
        author_name = (author.get("userName") or row.get("author_name")
                       or "").casefold()
        if is_reply and ((reply_uid and reply_uid == author_uid)
                         or (reply_name and reply_name == author_name)):
            counts["self_thread_replies"] += 1

        values = {
            "likes": row.get("likes", raw.get("likeCount", 0)),
            "retweets": row.get("retweets", raw.get("retweetCount", 0)),
            "replies": row.get("replies", raw.get("replyCount", 0)),
            "quotes": row.get("quotes", raw.get("quoteCount", 0)),
            "views": row.get("views", raw.get("viewCount", 0)),
            "bookmarks": raw.get("bookmarkCount", 0),
        }
        for name, value in values.items():
            try:
                metric_values[name].append(int(value or 0))
            except (TypeError, ValueError):
                metric_values[name].append(0)

        stamp = int(row.get("created_ts") or 0)
        if stamp:
            stamps.append(stamp)
            month = datetime.fromtimestamp(stamp, timezone.utc).strftime("%Y-%m")
            monthly.setdefault(month, []).append(metric_values["likes"][-1])

    engagement = {}
    for name, values in metric_values.items():
        total = sum(values)
        engagement[name] = {
            "total": total,
            "average": round(total / len(values), 2) if values else None,
        }
    month_series = [
        {"month": month, "volume": len(likes),
         "median_likes": float(median(likes))}
        for month, likes in sorted(monthly.items())
    ]
    return {
        "handle": handle.lstrip("@"),
        "counts": counts,
        "date_range": {
            "earliest": (datetime.fromtimestamp(min(stamps), timezone.utc).isoformat()
                         if stamps else None),
            "latest": (datetime.fromtimestamp(max(stamps), timezone.utc).isoformat()
                       if stamps else None),
        },
        "engagement": engagement,
        "monthly": month_series,
        "media": _media_summary(rows),
        "_composition_note": ("replies, self_thread_replies, and quotes can "
                              "overlap; originals is the exclusive remainder"),
        "spend": "$0.00 (local cache only)",
    }


def reconstruct_threads(handle, *, store=None, min_posts=2):
    """Group one account's cached posts by conversationId, oldest first."""
    rows = _cached_rows(handle, store)
    grouped = {}
    for row in rows:
        conversation_id = str(row.get("conversation_id") or "")
        if conversation_id:
            grouped.setdefault(conversation_id, []).append(row)

    threads = []
    for conversation_id, posts in grouped.items():
        if len(posts) < min_posts:
            continue
        posts.sort(key=lambda r: (int(r.get("created_ts") or 0), str(r.get("id") or "")))
        threads.append({
            "conversation_id": conversation_id,
            "tweet_count": len(posts),
            "cached_root_present": any(str(r.get("id")) == conversation_id
                                       for r in posts),
            "tweets": [{
                "id": r.get("id"),
                "created_at": (datetime.fromtimestamp(
                    int(r.get("created_ts")), timezone.utc).isoformat()
                    if r.get("created_ts") else None),
                "in_reply_to": r.get("in_reply_to") or None,
                "text": r.get("text") or "",
                "media": _row_media(r),
            } for r in posts],
        })
    threads.sort(key=lambda t: (-t["tweet_count"], t["conversation_id"]))
    return {
        "handle": handle.lstrip("@"),
        "thread_count": len(threads),
        "threads": threads,
        "media": _media_summary(rows),
        "_scope": ("Reconstructed from this handle's cached posts only; external "
                   "participants and uncached or deleted posts may be absent."),
        "spend": "$0.00 (local cache only)",
    }


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
                        "createdAt": t.get("createdAt"),
                        "media": _row_media(t)} for t in tweets[:15]],
        "media": _media_summary(tweets),
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
    out, seen = [], set()
    ut = until_ts
    for _ in range(max_pages):
        q = f"{query} since_time:{since_ts} until_time:{ut}"
        resp = c._raw("GET", "/twitter/tweet/advanced_search",
                      {"query": q, "queryType": "Latest"},
                      max_credits=Client.page_credits("search", 20, 20))
        batch = [enrich_tweet_media(tweet)
                 for tweet in (resp.get("tweets") or [])]
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
        if len(batch) < 20:
            break                       # short page = range genuinely exhausted
        if not stamps:
            raise IncompleteDataError(
                f"full search page for {query!r} had no parseable createdAt "
                f"timestamps, so no safe resume boundary exists")
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
            print(f"[jobs] WARNING: 20+ tweets share second {oldest}; "
                  f"time-window paging cannot split a second — some tweets "
                  f"in it are being skipped.", file=sys.stderr)
            raise IncompleteDataError(
                f"20+ tweets share second {oldest}; a time window cannot split "
                f"that second, so the corpus for {query!r} is INCOMPLETE.")
        else:
            new_ut = oldest + 1         # re-include boundary; dedupe handles it
        if new_ut >= ut:
            new_ut = ut - 1             # force progress rather than refetch
        ut = new_ut
        if ut <= since_ts:
            break                       # boundary reached = clean exhaustion
    else:
        raise IncompleteDataError(
            f"search hit max_pages={max_pages} before exhausting {query!r}; "
            f"the corpus is INCOMPLETE.")
    return out


# --------------------------------------------------------- authority map ----
def authority_map(seed_query, *, cohort_limit=150, max_usd=5.0, client=None,
                  progress=False):
    """Who a scene itself follows (frontier), from a topic seed. Expensive —
    the follow-graph crawl — so it estimates and refuses past max_usd."""
    c = client or _client(max_usd)
    co = Cohort.from_search(seed_query, limit=cohort_limit, client=c, store=c.store)
    co.authority(max_usd=max_usd, confirm=True, progress=progress)
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
        for rec in c.paginate(ep, str(tweet_id), limit=limit):
            a = rec.get("author") or {}
            ts = parse_ts(rec.get("createdAt") or "")
            if ts:
                timed.append({"kind": kind,
                              "handle": a.get("userName") or a.get("screen_name"),
                              "followers": a.get("followers"),
                              "createdAt": rec.get("createdAt"), "_ts": ts})
    # Transport, API, incompleteness, and ceiling failures all propagate. A
    # partial three-leg crawl cannot be represented by confident zero counts.
    retweeters = len(list(c.paginate("retweeters", str(tweet_id), limit=limit)))
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
    # ONE corpus call for all handles so bulk_search actually batches them.
    # Calling corpus([h]) per handle made every batch a single query, which
    # defeated the point entirely.
    since = _days_ago(days)
    allt = corpus(handles, since_ts=since, client=c)
    by_handle = {}
    for t in allt:
        by_handle.setdefault(
            ((t.get("author") or {}).get("userName") or "").lower(), []).append(t)
    rows = []
    for h in handles:
        info = c.user_info(h)
        tw = by_handle.get(h.lstrip("@").lower(), [])
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
    "authority": lambda a, c: authority_map(
        a.target, cohort_limit=a.sample if a.sample is not None else 150,
        max_usd=a.max_usd, client=c, progress=True),
    "overlap": lambda a, c: overlap(a.target, a.target2, max_usd=a.max_usd, client=c),
    "authenticity": lambda a, c: authenticity_audit(
        a.target, sample=a.sample if a.sample is not None else 1000, client=c),
    "diffusion": lambda a, c: diffusion_trace(a.target, client=c),
    "drift": lambda a, c: cohort_drift(a.target, a.v_old, a.v_new, client=c),
    "benchmark": lambda a, c: competitive_benchmark(a.target.split(","),
                                                    days=a.days, client=c),
}

LOCAL_JOBS = {
    "catalogue": lambda a, s: catalogue(a.target, store=s),
    "media": lambda a, s: media_inventory(a.target, store=s),
    "threads": lambda a, s: reconstruct_threads(a.target, store=s),
}


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("job", choices=sorted(set(JOBS) | set(LOCAL_JOBS)))
    p.add_argument("target", help="handle / query / tweet_id / cohort name / csv")
    p.add_argument("target2", nargs="?", help="second handle for overlap")
    p.add_argument("--days", type=int, default=30)
    p.add_argument("--sample", type=int, default=None,
                   help="cohort limit for authority; tweet sample for authenticity")
    p.add_argument("--max-usd", type=float, default=5.0)
    p.add_argument("--v-old", type=int)
    p.add_argument("--v-new", type=int)
    p.add_argument("--store", help="sqlite store path for local and network jobs")
    a = p.parse_args(argv)
    if a.sample is not None and a.job not in {"authority", "authenticity"}:
        p.error("--sample applies only to authority and authenticity")
    if a.job == "overlap" and not a.target2:
        p.error("overlap needs two handles: jobs.py overlap A B")
    if a.job in LOCAL_JOBS:
        s = Store(a.store) if a.store else Store()
        try:
            print(json.dumps(LOCAL_JOBS[a.job](a, s), indent=1,
                             ensure_ascii=False))
        finally:
            s.close()
        return 0

    c = _client(a.max_usd, store_path=a.store)  # one ceiling + chosen cache
    try:
        result = JOBS[a.job](a, c)
        print(json.dumps(result, indent=1, ensure_ascii=False))
    except IncompleteDataError as e:
        print(f"INCOMPLETE: {e}", file=sys.stderr)
        return 3
    except (CostLimitExceeded, ValueError) as e:
        print(f"REFUSED/STOPPED: {e}", file=sys.stderr)
        return 2
    except APIError as e:
        print(f"API error: {e}\nCheck the handle/id exists and that "
              f"TWITTERAPI_IO_KEY is valid.", file=sys.stderr)
        return 1
    if a.job == "authority" and result.get("complete") is False:
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
