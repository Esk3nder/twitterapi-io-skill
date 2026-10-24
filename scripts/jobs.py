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
                        IncompleteDataError)  # noqa: E402
from store import Store  # noqa: E402
from workflows import search_window  # noqa: E402
from cohort import Cohort  # noqa: E402
from axi import (MEDIA_FIELDS, TWEET_FIELDS, ArgumentParser,
                 HelpFormatter, UsageError, add_common_output_flags, dashboard, emit, error,
                 display_path, fast_version, parse_fields, prepare_output,
                 run_cli, success)  # noqa: E402

DEFAULT_MAX_USD = 5.0
DEFAULT_RESUME = ("Rerun (cached pages are free); if the reason names the "
                  "spend ceiling, raise `--max-usd <amount>`, or narrow the "
                  "requested scope")


class SetupError(RuntimeError):
    """Missing configuration. The CLI contract maps this to exit 2."""


def _client(max_usd=DEFAULT_MAX_USD, store_path=None):
    # Validate configuration before opening sqlite, so a missing key cannot
    # leak a connection while the first-run error crosses the CLI boundary.
    if not os.environ.get("TWITTERAPI_IO_KEY"):
        raise SetupError(
            "TWITTERAPI_IO_KEY not set. Add it to ~/.zshenv; never hardcode it.")
    store = Store(store_path) if store_path else Store()
    try:
        return Client(verbose=False, store=store, max_usd=max_usd)
    except Exception:
        store.close()
        raise


def _days_ago(days):
    return int(time.time()) - days * 86400


# ------------------------------------------------------- partial results ----
# One mechanism for every paid job: a gather step that stops early (spend
# ceiling, unprovable completeness) hands back the rows it already holds plus
# a reason, and the job reports them as the partial contract. Only a stop that
# happened before anything was gathered OR paid is a true refusal; it re-raises
# so the CLI exits 2. An API or network failure is a stop only where the
# paginated stream already holds rows (Client.paginate marks it partial); on a
# first page, and in the search-window walker, it propagates and exits 1.
_STOPS = (CostLimitExceeded, IncompleteDataError)


def _stop_reason(c, exc, rows):
    if (isinstance(exc, CostLimitExceeded) and not rows
            and not (getattr(c, "spent_usd", 0) or 0) > 0):
        raise exc
    return str(exc)


def _drain(c, stream, label):
    """Consume a paginated stream -> (rows, stop reason or None)."""
    rows = []
    try:
        for row in stream:
            rows.append(row)
    except _STOPS as exc:
        return rows, _stop_reason(c, exc, rows)
    if getattr(stream, "complete", True) is False:
        return rows, (getattr(stream, "_warning", None) or
                      f"{label} ended before completeness was proven")
    return rows, None


def _walk(c, gather):
    """Run a walker -> (rows, stop reason or None).

    `gather()` returns an iterable. Rows are collected as they arrive, so a
    generator that raises after yielding (workflows.search_window) keeps what
    it already bought; a list-returning walker that stops early may attach
    its acquired rows as `exc.records` instead."""
    rows = []
    try:
        for row in gather():
            rows.append(row)
        return rows, None
    except _STOPS as exc:
        rows.extend(getattr(exc, "records", None) or [])
        return rows, _stop_reason(c, exc, rows)


def _status(reasons, resume=None):
    """Completeness keys every paid job merges into its payload."""
    reasons = [r.removeprefix("PARTIAL: ") for r in reasons if r]
    return {"complete": not reasons,
            "_warning": ("PARTIAL: " + "; ".join(reasons)) if reasons else None,
            "_resume": (resume or DEFAULT_RESUME) if reasons else None}


def _known_bool(record, key):
    """True/False when the field was reported; None when it was absent."""
    value = (record or {}).get(key)
    return None if value is None else bool(value)


def _tweet_row(t):
    """A tweet exposing every TWEET_FIELDS name; absent counts stay null."""
    return {"id": t.get("id"), "text": t.get("text"),
            "likes": t.get("likeCount"), "retweets": t.get("retweetCount"),
            "replies": t.get("replyCount"), "quotes": t.get("quoteCount"),
            "views": t.get("viewCount"), "author": t.get("author"),
            "created_at": t.get("createdAt"), "media": _row_media(t)}


def _clean_handles(handles):
    """Strip whitespace and '@', drop empties, dedupe case-insensitively."""
    out, keys = [], set()
    for handle in handles:
        clean = (handle or "").strip().lstrip("@").strip()
        if clean and clean.casefold() not in keys:
            keys.add(clean.casefold())
            out.append(clean)
    return out


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
        except CostLimitExceeded as exc:
            exc.records = out          # earlier batches were bought and stored
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
        # A stop propagates so a truncated corpus is never returned as if it
        # were complete — brief/narrative/benchmark would silently compare
        # complete-vs-partial windows. The rows already bought travel with
        # it as `exc.records` so the caller can return them as a partial.
        try:
            for t in search_window(c, f"from:{h}", since_ts or 0, resume_ts):
                if t.get("id") not in seen:  # the walk re-fetches page 1
                    seen.add(t.get("id")); out.append(t)
        except _STOPS as exc:
            exc.records = out
            raise
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


def _metric(row, raw, column, wire):
    """One engagement count, or None when it was never reported.

    The stored wire payload is authoritative for presence: a pre-migration
    column holds 0 for an absent count, which is indistinguishable from a
    real zero. Without a raw payload, a NULL column is unknown."""
    if raw:
        value = raw.get(wire)
    else:
        value = row.get(column) if column else None
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


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
    # Public metric -> (store column, wire field). The wire payload decides
    # whether a count was REPORTED: last_tweets omits viewCount, and an absent
    # count is unknown, not zero. Averages use known values only.
    metrics = {"likes": ("likes", "likeCount"),
               "retweets": ("retweets", "retweetCount"),
               "replies": ("replies", "replyCount"),
               "quotes": ("quotes", "quoteCount"),
               "views": ("views", "viewCount"),
               "bookmarks": (None, "bookmarkCount")}
    metric_values = {name: [] for name in metrics}
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

        likes = None
        for name, (column, wire) in metrics.items():
            value = _metric(row, raw, column, wire)
            if value is not None:
                metric_values[name].append(value)
            if name == "likes":
                likes = value

        stamp = int(row.get("created_ts") or 0)
        if stamp:
            stamps.append(stamp)
            month = datetime.fromtimestamp(stamp, timezone.utc).strftime("%Y-%m")
            monthly.setdefault(month, []).append(likes)

    engagement = {}
    for name, values in metric_values.items():
        total = sum(values)
        engagement[name] = {
            "total": total if values else None,
            "average": round(total / len(values), 2) if values else None,
            "known": len(values),
            "unknown": len(rows) - len(values),
        }
    month_series = []
    for month, likes in sorted(monthly.items()):
        known = [v for v in likes if v is not None]
        month_series.append({"month": month, "volume": len(likes),
                             "median_likes": (float(median(known))
                                              if known else None)})
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
    tweets, stop = _walk(c, lambda: corpus(
        [handle], since_ts=_days_ago(days), client=c))
    tweets.sort(key=lambda t: t.get("likeCount") or 0, reverse=True)
    return {
        "entity": handle,
        "profile": {"name": info.get("name"), "followers": info.get("followers"),
                    "description": info.get("description"),
                    "verified": _known_bool(info, "isBlueVerified")},
        "window_days": days,
        "tweet_count": len(tweets),
        "top_tweets": [_tweet_row(t) for t in tweets[:15]],
        "top_tweets_total": len(tweets),
        "media": _media_summary(tweets),
        "_note": "Interpretation (themes, posture) is the caller's job, not this function's.",
        **_status([stop]),
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
    cur, cur_stop = _walk(c, lambda: search_window(
        c, query, now - days * 86400, now))
    prior, prior_stop = _walk(c, lambda: search_window(
        c, query, now - 2 * days * 86400, now - days * 86400))

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

    reasons = [f"current window: {cur_stop}" if cur_stop else None,
               f"prior window: {prior_stop}" if prior_stop else None]
    if prior_stop:
        # Against a partial prior window, an author or tag it never reached
        # would be reported as "new" — absence read as a confident diff.
        new_authors, rising_hashtags = [], []
        reasons.append("new_authors and rising_hashtags are omitted because "
                       "the prior window is incomplete")
    else:
        a_now, a_prior = authors(cur), authors(prior)
        h_now, h_prior = hashtags(cur), hashtags(prior)
        new_authors = sorted(a_now - a_prior)
        rising_hashtags = sorted(
            ((k, h_now[k], h_prior.get(k, 0)) for k in h_now),
            key=lambda x: x[1] - x[2], reverse=True)
    return {
        "query": query, "window_days": days,
        "new_authors": new_authors[:40],
        "new_authors_total": len(new_authors),
        "current_tweet_count": len(cur), "prior_tweet_count": len(prior),
        "rising_hashtags": rising_hashtags[:20],
        "rising_hashtags_total": len(rising_hashtags),
        "_note": "New authors and rising tags are computed; naming the narrative is the caller's job.",
        **_status(reasons),
        "spend": c.spend_report(),
    }


# --------------------------------------------------------- authority map ----
def authority_map(seed_query, *, cohort_limit=150, max_usd=5.0, client=None,
                  progress=False):
    """Who a scene itself follows (frontier), from a topic seed. Expensive —
    the follow-graph crawl — so it estimates and refuses past max_usd."""
    c = client or _client(max_usd)
    try:
        co = Cohort.from_search(seed_query, limit=cohort_limit, client=c,
                                store=c.store)
    except _STOPS as exc:
        # The seed search stopped early: a partial cohort is a different
        # population, so no frontier is ranked from it.
        return {"seed": seed_query, "cohort_size": None, "members_crawled": 0,
                "frontier": [], "frontier_total": 0,
                **_status([f"seed search: {_stop_reason(c, exc, [])}"]),
                "spend": c.spend_report()}
    co.authority(max_usd=max_usd, confirm=True, progress=progress)
    ranked = [m for m in co.top(len(co)) if m["weight"] > 0]
    complete = getattr(co, "authority_complete", True) is not False
    return {
        "seed": seed_query, "cohort_size": len(co),
        "members_crawled": getattr(co, "authority_crawled", len(co)),
        "frontier": [{"handle": m["user_name"], "in_degree": m["weight"]}
                     for m in ranked[:30]],
        "frontier_total": len(ranked),
        # The reason comes from the crawl itself: a ceiling stop and an API
        # failure mid-walk are different problems with different fixes, and
        # hardcoding the ceiling text here misdiagnosed the latter.
        **_status([None if complete else (
            getattr(co, "authority_incomplete_reason", None)
            or "the follow-graph walk ended before completeness was proven")],
            resume=("Raise `--max-usd <amount>` and rerun if the spend "
                    "ceiling stopped the crawl; members whose followings "
                    "failed (protected, suspended) stay uncounted")),
        "spend": c.spend_report(),
    }


# ------------------------------------------------------------- overlap ------
def overlap(handle_a, handle_b, *, ids_only=True, max_usd=5.0, client=None):
    """Shared followers of two accounts. Cheap after first crawl — the whole
    point of caching. Returns counts + a sample of overlapping ids."""
    c = client or _client(max_usd)
    # Estimate BEFORE crawling: full follower sets of two large accounts can be
    # tens of dollars. Refuse up front rather than aborting mid-crawl. Pages
    # the store already holds are served free, so only the uncached remainder
    # is priced; a fully cached rerun needs no lookup at all.
    est, counts = 0.0, []
    for handle in (handle_a, handle_b):
        cached = c.store.follower_ids_for(handle) if c.store else []
        if getattr(cached, "complete", False):
            counts.append(f"@{handle} cached")
            continue
        n = c.user_info(handle).get("followers")
        if n is None:
            raise CostLimitExceeded(
                f"overlap({handle_a},{handle_b}): @{handle}'s follower count "
                f"is unknown, so the crawl cost cannot be bounded; refusing "
                f"before the crawl.")
        remaining = max(0, int(n) - len(cached))
        counts.append(f"@{handle} {remaining:,} uncached of {int(n):,}")
        est += c.estimate("follower_ids", remaining)
    if est > max_usd:
        raise CostLimitExceeded(
            f"overlap({handle_a},{handle_b}) ~= ${est:,.2f} "
            f"({', '.join(counts)}), over ${max_usd:,.2f}. Raise --max-usd "
            f"to run it (results cache, so a re-run is free).")
    ids_a, stop_a = _drain(c, c.follower_ids(handle_a), f"followers for @{handle_a}")
    ids_b, stop_b = _drain(c, c.follower_ids(handle_b), f"followers for @{handle_b}")
    A = Cohort(client=c, store=c.store, label=f"followers:{handle_a}")
    for uid in ids_a:
        A._add(uid, None, 1.0, "follower")
    B = Cohort(client=c, store=c.store, label=f"followers:{handle_b}")
    for uid in ids_b:
        B._add(uid, None, 1.0, "follower")
    both = A.intersect(B)
    partial = bool(stop_a or stop_b)
    empty = not partial and (len(A) == 0 or len(B) == 0)
    reasons = [stop_a, stop_b]
    if partial:
        reasons.append("overlap is a lower bound over the followers fetched; "
                       "jaccard is omitted")
    status = _status(reasons)
    if empty:
        status["_warning"] = ("one or both accounts returned 0 followers "
                              "(suspended, private, or renamed) — the overlap "
                              "is not meaningful")
    return {
        "a": handle_a, "b": handle_b,
        "a_followers": len(A), "b_followers": len(B), "overlap": len(both),
        "jaccard": (None if partial else
                    round(len(both) / max(1, len(A) + len(B) - len(both)), 4)),
        "sample_ids": both.ids()[:50], "sample_ids_total": len(both),
        **status,
        "spend": c.spend_report(),
    }


# --------------------------------------------------- authenticity audit -----
def authenticity_audit(handle, *, sample=1000, client=None):
    """Signals for organic-vs-purchased audience: follower account ages,
    default-ish handles, follower/following skew. Returns signals; the model
    renders the verdict."""
    c = client or _client()
    info = c.user_info(handle)
    followers, stop = _drain(
        c, c.paginate("followers", handle, limit=sample), f"followers for @{handle}")
    now = datetime.now(timezone.utc)
    ages, no_bio, bio_known, egg_like, counts_known = [], 0, 0, 0, 0
    from store import parse_ts

    def first_present(record, *keys):
        # 0 is a real count; only an absent key (or null) is unknown.
        return next((record[k] for k in keys if record.get(k) is not None), None)

    for f in followers:
        ca = f.get("created_at") or f.get("createdAt") or ""
        ts = parse_ts(ca)
        if ts:
            ages.append((now - datetime.fromtimestamp(ts, timezone.utc)).days)
        bio = first_present(f, "description", "bio")
        if bio is not None:
            bio_known += 1
            no_bio += not bio
        fo = first_present(f, "followers_count", "followers")
        fr = first_present(f, "following_count", "friends_count", "following")
        if fo is not None and fr is not None:
            counts_known += 1
            egg_like += fo == 0 and fr > 500
    ages.sort()
    status = _status([stop])
    if not followers and not stop:
        status["_warning"] = ("0 followers sampled — account may be "
                              "suspended/private; percentages below are not "
                              "meaningful")
    return {
        "handle": handle,
        "stated_followers": info.get("followers"),
        "sampled": len(followers),
        "median_follower_age_days": ages[len(ages) // 2] if ages else None,
        # Denominator is the PARSED subset: dividing by the whole sample
        # understates the young-account signal when created_at fails to parse.
        "pct_accounts_under_90d": (round(100 * sum(a < 90 for a in ages)
                                         / len(ages), 1) if ages else None),
        "created_at_parse_failures": len(followers) - len(ages),
        # Denominators are the profiles that REPORTED the field: an absent
        # bio or count is unknown, not "no bio" / 0 followers.
        "pct_no_bio": round(100 * no_bio / bio_known, 1) if bio_known else None,
        "bio_unknown": len(followers) - bio_known,
        "pct_egg_like": (round(100 * egg_like / counts_known, 1)
                         if counts_known else None),
        "counts_unknown": len(followers) - counts_known,
        "_note": "These are signals, not a verdict. Interpretation is the caller's job.",
        **status,
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

    `limit` (`--limit`) bounds each of the three crawls (replies, quotes,
    retweeters). A leg that stops at the limit while more data existed, at the
    spend ceiling, or with unprovable completeness makes the trace PARTIAL:
    replies arrive newest first (queryType=Latest), so a truncated leg can
    miss the true earliest movers. Transport and API failures propagate.
    """
    from store import parse_ts
    c = client or _client()
    timed, retweeters, reasons, limited = [], 0, [], False
    legs = (("reply", "replies_v2", {"queryType": "Latest"}),
            ("quote", "quotes", None),
            ("retweet", "retweeters", None))
    for kind, ep, extra in legs:
        stream = c.paginate(ep, str(tweet_id), limit=limit, extra=extra)
        records, stop = _drain(c, stream, f"{kind}s for {tweet_id}")
        if stop:
            reasons.append(stop)
        elif getattr(stream, "truncated_by_limit", False):
            limited = True
            reasons.append(
                f"{kind}s for {tweet_id} stopped at --limit {limit:,} while "
                f"more existed; earliest_movers may miss the true earliest")
        if kind == "retweet":
            retweeters = len(records)
            continue
        for rec in records:
            a = rec.get("author") or {}
            ts = parse_ts(rec.get("createdAt") or "")
            if ts:
                timed.append({"kind": kind,
                              "handle": a.get("userName") or a.get("screen_name"),
                              "followers": a.get("followers"),
                              "createdAt": rec.get("createdAt"), "_ts": ts})
    timed.sort(key=lambda e: e["_ts"])
    for e in timed:
        e.pop("_ts", None)
    return {
        "tweet_id": tweet_id,
        "timed_engagers": len(timed), "retweeter_count": retweeters,
        "earliest_movers": timed[:25], "earliest_movers_total": len(timed),
        "by_kind": {"reply": sum(1 for e in timed if e["kind"] == "reply"),
                    "quote": sum(1 for e in timed if e["kind"] == "quote"),
                    "retweet": retweeters},
        "_note": "Retweeters lack a retweet timestamp and are counted, not time-ordered.",
        **_status(reasons, resume=(
            "Raise `--limit <count>` (and `--max-usd <amount>` if needed) and rerun"
            if limited else None)),
        "spend": c.spend_report(),
    }


# --------------------------------------------------------- cohort drift -----
def cohort_drift(name, v_old=None, v_new=None, *, store):
    """Who joined or left between two saved resolutions of a cohort.

    Pure local computation over the store: no API key, no Client, $0."""
    versions = store.cohort_versions(name)
    if len(versions) < 2:
        raise ValueError(
            f"cohort '{name}' has versions {versions or '[]'} — drift needs at "
            f"least 2 resolutions of the same name. Re-resolve and save it again.")
    # Default = the LAST STEP, not the whole history. versions[0] here made a
    # 30-day-old daily watch report a month of churn as if it were today's:
    # invisible with two versions (first == previous) and worse every day
    # after, which is why the original two-version test never caught it.
    v_old = v_old if v_old is not None else versions[-2]
    v_new = v_new if v_new is not None else versions[-1]
    missing = [v for v in (v_old, v_new) if v not in versions]
    if missing:
        # Named here with the available versions; Cohort.load would also
        # refuse, but with a bare KeyError.
        raise ValueError(f"version(s) {missing} not saved for '{name}'; "
                         f"available: {versions}")
    if v_old == v_new:
        raise ValueError(f"v_old and v_new are both {v_old}; pick two different "
                         f"versions from {versions}.")
    # No client: Cohort creates one only for paid calls, and drift makes none.
    old = Cohort.load(name, v_old, store=store)
    new = Cohort.load(name, v_new, store=store)
    joined_values = [m["user_name"] or m["account_id"]
                     for m in new.minus(old).members.values()]
    left_values = [m["user_name"] or m["account_id"]
                   for m in old.minus(new).members.values()]
    partial = [f"version {v} was saved partial: {'; '.join(co.incomplete_reasons)}"
               for v, co in ((v_old, old), (v_new, new)) if not co.complete]
    return {
        **_status(partial, "Re-resolve the cohort with a higher limit or "
                           "`--max-usd`, save it, and rerun drift"),
        "cohort": name, "from_version": v_old, "to_version": v_new,
        "old_size": len(old.members), "new_size": len(new.members),
        "joined": joined_values[:60], "joined_total": len(joined_values),
        "left": left_values[:60], "left_total": len(left_values),
        "spend": "$0.00 (local cache only)",
    }


# ----------------------------------------------- competitive benchmark ------
def competitive_benchmark(handles, *, days=30, client=None):
    """N entities on shared axes: reach, recent volume, top-post engagement."""
    # "stripe, vercel" must not send `from: vercel` (a different, empty query).
    handles = _clean_handles(handles)
    if not handles:
        raise UsageError("benchmark needs at least one handle, e.g. "
                         "`jobs.py benchmark stripe,vercel`")
    c = client or _client()
    # ONE corpus call for all handles so bulk_search actually batches them.
    # Calling corpus([h]) per handle made every batch a single query, which
    # defeated the point entirely.
    since = _days_ago(days)
    allt, stop = _walk(c, lambda: corpus(handles, since_ts=since, client=c))
    reasons = [stop]
    by_handle = {}
    for t in allt:
        by_handle.setdefault(
            ((t.get("author") or {}).get("userName") or "").casefold(), []).append(t)
    rows = []
    for h in handles:
        try:
            info = c.user_info(h)
        except CostLimitExceeded as exc:
            info = {}
            reasons.append(f"profile for @{h}: {_stop_reason(c, exc, allt or rows)}")
        tw = by_handle.get(h.casefold(), [])
        likes = [t.get("likeCount") for t in tw if t.get("likeCount") is not None]
        rows.append({"handle": h, "followers": info.get("followers"),
                     "tweets_in_window": len(tw),
                     "peak_likes": max(likes) if likes else None,
                     "verified": _known_bool(info, "isBlueVerified")})
    return {"window_days": days, "entities": rows, **_status(reasons),
            "spend": c.spend_report()}


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
    "diffusion": lambda a, c: diffusion_trace(a.target, limit=a.limit, client=c),
    "benchmark": lambda a, c: competitive_benchmark(a.target.split(","),
                                                    days=a.days, client=c),
}

# Local jobs read only the store: no API key, no Client, no spend.
LOCAL_JOBS = {
    "catalogue": lambda a, s: catalogue(a.target, store=s),
    "media": lambda a, s: media_inventory(a.target, store=s),
    "threads": lambda a, s: reconstruct_threads(a.target, store=s),
    "drift": lambda a, s: cohort_drift(a.target, a.v_old, a.v_new, store=s),
}


COLLECTIONS = {
    "brief": {"top_tweets": TWEET_FIELDS},
    "narrative": {"new_authors": {}, "rising_hashtags": {}},
    "authority": {"frontier": {"handle": ("handle",), "in_degree": ("in_degree",)}},
    "overlap": {"sample_ids": {}},
    "diffusion": {"earliest_movers": {
        "kind": ("kind",), "handle": ("handle",), "followers": ("followers",),
        "created_at": ("created_at", "createdAt")}},
    "drift": {"joined": {}, "left": {}},
    "benchmark": {"entities": {
        "handle": ("handle",), "followers": ("followers",),
        "tweets_in_window": ("tweets_in_window",), "peak_likes": ("peak_likes",),
        "verified": ("verified",)}},
    "media": {"artifacts": MEDIA_FIELDS},
}

DEFAULT_FIELDS = {"brief": {"top_tweets": ("id", "created_at", "likes", "text")}}

SAMPLE_HELP = {
    "authority": ("number of seed-search tweets whose authors form the cohort; "
                  "the cohort has at most this many members, fewer when an "
                  "author posted more than once (default 150)"),
    "authenticity": "number of follower profiles to sample (default 1000)",
}


def _add_common_job_flags(parser, *, paid):
    """--max-usd and --store, accepted before or after the job name.

    SUPPRESS keeps a subparser from overwriting a value given before the job
    name, and lets the CLI tell an explicit flag from the default."""
    if paid:
        parser.add_argument("--max-usd", type=float, default=argparse.SUPPRESS,
                            help=f"hard spend ceiling in USD (default {DEFAULT_MAX_USD:.2f})")
    parser.add_argument("--store", default=argparse.SUPPRESS,
                        help="SQLite store path (default ~/.twitterapi-cache/store.db)")


def _job_parser():
    p = ArgumentParser(description=__doc__, formatter_class=HelpFormatter)
    _add_common_job_flags(p, paid=True)
    sub = p.add_subparsers(dest="job")
    descriptions = {
        "brief": "profile and top recent posts", "narrative": "compare topic windows",
        "authority": "rank a scene's follow graph", "overlap": "compare audiences",
        "authenticity": "sample audience quality signals", "diffusion": "trace early spread",
        "drift": "compare two saved cohort versions ($0, local)",
        "benchmark": "compare accounts",
        "catalogue": "summarize cached posts ($0, local)",
        "media": "list cached media ($0, local)",
        "threads": "reconstruct cached threads ($0, local)",
    }
    for name in sorted(set(JOBS) | set(LOCAL_JOBS)):
        examples = f"Examples:\n  jobs.py {name} <target>\n  jobs.py {name} <target> --full"
        q = sub.add_parser(name, help=descriptions[name], description=descriptions[name],
                           epilog=examples,
                           formatter_class=HelpFormatter)
        q.add_argument("target", help="handle, query, tweet id, cohort name, or CSV handles")
        if name == "overlap":
            q.add_argument("target2", help="second account handle")
        else:
            q.set_defaults(target2=None)
        if name in {"brief", "narrative", "benchmark"}:
            q.add_argument("--days", type=int, default=30,
                           help="lookback or comparison window in days")
        else:
            q.set_defaults(days=30)
        if name in SAMPLE_HELP:
            q.add_argument("--sample", type=int, default=None, help=SAMPLE_HELP[name])
        else:
            q.set_defaults(sample=None)
        if name == "diffusion":
            q.add_argument("--limit", type=int, default=500,
                           help="maximum records per leg (replies, quotes, "
                                "retweeters); a leg stopped by it is partial")
        if name == "drift":
            q.add_argument("--v-old", type=int, help="older saved cohort version")
            q.add_argument("--v-new", type=int, help="newer saved cohort version")
        else:
            q.set_defaults(v_old=None, v_new=None)
        _add_common_job_flags(q, paid=name in JOBS)
        add_common_output_flags(
            q, fields=any(bool(schema) for schema in COLLECTIONS.get(name, {}).values()))
    return p


def _format_job(a, result):
    result = dict(result)
    resume = result.pop("_resume", None)
    complete = result.get("complete", True) is not False
    reason = result.get("_warning") if not complete else None
    context = f"for `{a.job}` target {a.target!r}"
    payload = prepare_output(
        result, command=f"jobs.py {a.job} <target>", fields=getattr(a, "fields", None),
        full=a.full, collections=COLLECTIONS.get(a.job),
        defaults=DEFAULT_FIELDS.get(a.job), context=context,
        help_lines=(f"Run `jobs.py {a.job} <target> --help` for options",))
    if (a.job == "catalogue" and result.get("counts", {}).get("total") == 0):
        store_name = display_path(getattr(a, "store", None)
                                  or "~/.twitterapi-cache/store.db")
        payload["posts_state"] = (
            f"0 cached posts found for @{a.target.lstrip('@')} in local store {store_name}")
    return success(f"jobs.py {a.job}", payload, complete=complete, reason=reason,
                   resume=((resume or DEFAULT_RESUME) if not complete else None))


def _main(argv=None):
    if fast_version(argv):
        return 0
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv:
        emit(dashboard(__file__, "Run cached and live twitterapi.io research jobs"))
        return 0
    p = _job_parser()
    a = p.parse_args(argv)
    if a.job is None:
        p.error("a job name is required")
    command = f"jobs.py {a.job}"
    store_path = getattr(a, "store", None)
    # 0 or a negative count is never a real request: --limit 0 returned an
    # empty "partial", and --sample 0 an empty audit.
    for flag in ("days", "sample", "limit"):
        value = getattr(a, flag, None)
        if value is not None and value < 1:
            emit(error("usage", f"--{flag} must be at least 1, got {value}",
                       command, f"Run `jobs.py {a.job} <target> --help` for options"))
            return 2
    if getattr(a, "fields", None):
        schema = next(iter(COLLECTIONS[a.job].values()))
        try:
            parse_fields(a.fields, schema, command)
        except UsageError as exc:
            emit(error("usage", str(exc), command,
                       f"Run `jobs.py {a.job} <target> --help` for valid fields"))
            return 2
    if a.job in LOCAL_JOBS:
        if hasattr(a, "max_usd"):
            emit(error("usage", f"--max-usd does not apply to `{a.job}`: it reads "
                       "only the local store and spends $0", command,
                       f"Run `jobs.py {a.job} <target> --help` for options"))
            return 2
        s = Store(store_path) if store_path else Store()
        try:
            emit(_format_job(a, LOCAL_JOBS[a.job](a, s)))
        except ValueError as exc:
            emit(error("usage", str(exc), command,
                       f"Run `jobs.py {a.job} <target> --help` for options"))
            return 2
        finally:
            s.close()
        return 0

    a.max_usd = getattr(a, "max_usd", DEFAULT_MAX_USD)
    c = None
    try:
        c = _client(a.max_usd, store_path=store_path)  # one ceiling + chosen cache
        emit(_format_job(a, JOBS[a.job](a, c)))
    except SetupError as e:
        emit(error("setup", str(e), command,
                   "Set `TWITTERAPI_IO_KEY` in the environment and rerun"))
        return 2
    except UsageError as e:
        emit(error("usage", str(e), command,
                   f"Run `jobs.py {a.job} <target> --help` for options"))
        return 2
    except (CostLimitExceeded, ValueError) as e:
        # Jobs turn a mid-gather stop into a partial payload themselves, so a
        # CostLimitExceeded here is an up-front refusal. Lookups bought while
        # estimating are still disclosed.
        body = error("refusal", str(e), command,
                     f"Run `jobs.py {a.job} <target> --help` for valid limits")
        if c is not None and getattr(c, "requests_made", 0):
            body["spend"] = c.spend_report()
        emit(body)
        return 2
    except APIError as e:
        emit(error("api", str(e), command,
                   "Check the handle or ID and verify `TWITTERAPI_IO_KEY`"))
        return 1
    finally:
        close = getattr(getattr(c, "store", None), "close", None)
        if close:
            close()
    return 0


def main(argv=None):
    return run_cli("jobs.py", _main, argv)


if __name__ == "__main__":
    sys.exit(main())
