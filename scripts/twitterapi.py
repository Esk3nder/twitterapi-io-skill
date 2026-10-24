#!/usr/bin/env python3
"""twitterapi.io client. Standard library only — no pip install.

Every endpoint spec, response shape and price in this file was verified against
the live API on 2026-08-10. See ../references/verified-facts.md for the probe
evidence. Do not "fix" a parameter name here to look consistent: the API mixes
camelCase and snake_case per endpoint and the spellings below are the measured
ones.

Read-only. No write endpoints are implemented — see verified-facts.md for why.

Usage:
    from twitterapi import Client
    c = Client()
    for uid in c.follower_ids("jack"):        # cheapest path, 5000/call
        ...
    print(c.estimate("follower_ids", 200_000_000))   # -> $900.00
"""
from __future__ import annotations

import json
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

BASE = "https://api.twitterapi.io"
CREDITS_PER_USD = 100_000

# --------------------------------------------------------------------------
# Endpoint table — the single source of truth.
#
#   items      : top-level key holding the result array
#   items_in   : "root" or "data"  (data-wrapped endpoints nest the array,
#                but their pagination keys stay at the ROOT — the trap)
#   page_param : name of the page-size parameter, None if fixed
#   page_max   : maximum page size the API honours
#   key_list   : True when the key is a comma-joined id list; the request's
#                page size is then the number of ids sent, not page_max
#   paged      : False for single-response list endpoints that carry no
#                has_next_page (verified envelopes: /twitter/tweets, trends).
#                Every other list endpoint MUST carry has_next_page; its
#                absence is contract drift, never proof of completeness.
# --------------------------------------------------------------------------
ENDPOINTS = {
    "user_info": dict(
        path="/twitter/user/info", key_param="userName",
        items=None, items_in="data", page_param=None, page_max=1),
    "user_about": dict(
        path="/twitter/user_about", key_param="userName",
        items=None, items_in="data", page_param=None, page_max=1),
    "batch_users": dict(
        path="/twitter/user/batch_info_by_ids", key_param="userIds",
        items="users", items_in="root", page_param=None, page_max=100,
        key_list=True, paged=False),
    "search_users": dict(
        path="/twitter/user/search", key_param="query",
        items="users", items_in="root", page_param=None, page_max=20),
    # follower_ids is the cheap path: 0.45 credits/id at max page, 5000/call.
    "follower_ids": dict(
        path="/twitter/user/followers_ids", key_param="userName",
        items="ids", items_in="root", page_param="count", page_max=5000),
    "followers": dict(
        path="/twitter/user/followers", key_param="userName",
        items="followers", items_in="root", page_param="pageSize", page_max=200),
    "followings": dict(
        path="/twitter/user/followings", key_param="userName",
        items="followings", items_in="root", page_param="pageSize", page_max=200),
    "verified_followers": dict(                      # snake user_id, digits only
        path="/twitter/user/verifiedFollowers", key_param="user_id",
        items="followers", items_in="root", page_param=None, page_max=20),
    # data-wrapped: tweets at data.tweets, pagination at ROOT.
    "last_tweets": dict(
        path="/twitter/user/last_tweets", key_param="userName",
        items="tweets", items_in="data", page_param=None, page_max=20),
    "tweet_timeline": dict(
        path="/twitter/user/tweet_timeline", key_param="userId",
        items="tweets", items_in="data", page_param=None, page_max=20,
        key_resolver="user_id"),
    "mentions": dict(
        path="/twitter/user/mentions", key_param="userName",
        items="tweets", items_in="root", page_param=None, page_max=20),
    "tweets_by_ids": dict(
        path="/twitter/tweets", key_param="tweet_ids",
        items="tweets", items_in="root", page_param=None, page_max=100,
        key_list=True, paged=False),
    "replies": dict(
        path="/twitter/tweet/replies", key_param="tweetId",
        items="tweets", items_in="root", page_param=None, page_max=20),
    "quotes": dict(
        path="/twitter/tweet/quotes", key_param="tweetId",
        items="tweets", items_in="root", page_param=None, page_max=20),
    "retweeters": dict(
        path="/twitter/tweet/retweeters", key_param="tweetId",
        items="users", items_in="root", page_param=None, page_max=20),
    "thread_context": dict(
        path="/twitter/tweet/thread_context", key_param="tweetId",
        items="tweets", items_in="root", page_param=None, page_max=20),
    "search": dict(
        path="/twitter/tweet/advanced_search", key_param="query",
        items="tweets", items_in="root", page_param=None, page_max=20),
    "list_members": dict(                            # snake, unlike list_tweets
        path="/twitter/list/members", key_param="list_id",
        items="members", items_in="root", page_param=None, page_max=20),
    "list_followers": dict(
        path="/twitter/list/followers", key_param="list_id",
        items="followers", items_in="root", page_param=None, page_max=20),
    "list_tweets": dict(                             # camel, unlike list_members
        path="/twitter/list/tweets_timeline", key_param="listId",
        items="tweets", items_in="root", page_param=None, page_max=20),
    "trends": dict(                                  # NOT data-wrapped
        path="/twitter/trends", key_param="woeid",
        items="trends", items_in="root", page_param="count", page_max=50,
        paged=False),
    # Verified live. Note `query` is REQUIRED on this one.
    "community_tweets_all": dict(
        path="/twitter/community/get_tweets_from_all_community", key_param="query",
        items="tweets", items_in="root", page_param=None, page_max=20),
    # --- read surface completed 2026-08-10, all live-probed ---
    # replies/v2 returns 31-38 per call vs 20 for plain /replies: ~1.8x fewer
    # requests for the same data. Prefer it. queryType: Relevance|Latest|Likes.
    "replies_v2": dict(
        path="/twitter/tweet/replies/v2", key_param="tweetId",
        items="tweets", items_in="root", page_param=None, page_max=38),
    # data-wrapped with ROOT pagination — same trap as last_tweets.
    "articles": dict(                       # param is all-lowercase `username`
        path="/twitter/user/articles", key_param="username",
        items="articles", items_in="data", page_param=None, page_max=20),
    "community_info": dict(
        path="/twitter/community/info", key_param="community_id",
        items=None, items_in="root", page_param=None, page_max=1),
    "community_members": dict(
        path="/twitter/community/members", key_param="community_id",
        items="members", items_in="root", page_param=None, page_max=20),
    "community_moderators": dict(
        path="/twitter/community/moderators", key_param="community_id",
        items="moderators", items_in="root", page_param=None, page_max=20),
    "community_tweets": dict(
        path="/twitter/community/tweets", key_param="community_id",
        items="tweets", items_in="root", page_param=None, page_max=20),
    "list_tweets_filtered": dict(           # camel listId, supports time bounds
        path="/twitter/list/tweets", key_param="listId",
        items="tweets", items_in="root", page_param=None, page_max=20),
    "space_detail": dict(
        path="/twitter/spaces/detail", key_param="space_id",
        items=None, items_in="root", page_param=None, page_max=1),
}

# Endpoints whose results are stable enough to cache indefinitely (a follow
# graph or profile doesn't change between two questions in a session). Content
# endpoints (search, replies, last_tweets, mentions) are time-sensitive and
# always fetched fresh — caching them would silently serve stale corpora.
CACHEABLE = {"follower_ids", "followers", "followings", "verified_followers",
             "batch_users", "user_info", "user_about", "community_members",
             "community_moderators", "list_members", "list_followers"}
COMMUNITY_COLLECTIONS = {
    "community_members", "community_moderators", "community_tweets",
}
LIST_COLLECTIONS = {
    "list_members", "list_followers", "list_tweets", "list_tweets_filtered",
}

# Tiered pricing, measured. (min_page_size, credits_per_record) descending.
PRICE_TIERS = {
    "follower_ids": [(4000, 0.45), (200, 1.0), (50, 2.0), (0, 2.0)],
    "followers":    [(200, 1.0), (100, 2.0), (20, 3.0), (0, 3.0)],
    "followings":   [(200, 1.0), (100, 2.0), (20, 3.0), (0, 3.0)],
}
# Flat per-record costs (credits).
#   profile = 18 -> MEASURED (one user/info call cost exactly 18 credits)
#   tweet   = 15 -> DOCUMENTED ($0.15/1k), not yet cleanly measured.
FLAT_CREDITS = {"tweet": 15.0, "profile": 18.0}
MIN_REQUEST_CREDITS = 15.0   # $0.00015 floor, charged PER REQUEST, even if empty

# QPS ladder, from twitterapi.io/qps-limits. Keyed by credit balance.
QPS_LADDER = [(50_000, 20), (10_000, 10), (5_000, 6), (1_000, 3), (0, 0.2)]

_BARE_SEARCH_DATE = re.compile(
    r"(?<!\S)(since|until):(\d{4}-\d{2}-\d{2})(?=\s|$)")
_MIN_FAVES = re.compile(r"(?<!\S)min_faves:(\d+)(?=\s|$)")
# Any min_faves operator, however attached (inside a group, negated).
_ANY_MIN_FAVES = re.compile(r"min_faves:")
_TOP_LEVEL_OR = re.compile(r"(?<!\S)OR(?!\S)")


def _query_views(query):
    """Return (unquoted, top_level) views of a search query.

    ``unquoted`` blanks quoted phrases; ``top_level`` also blanks every
    parenthesised group. Positions are preserved, so a match in a view is at
    the same offset in the original query."""
    unquoted, top, depth, quoted = [], [], 0, False
    for ch in query:
        if ch == '"':
            quoted = not quoted
        if quoted or ch == '"':
            unquoted.append(" ")
            top.append(" ")
            continue
        unquoted.append(ch)
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(depth - 1, 0)
        top.append(ch if depth == 0 and ch not in "()" else " ")
    return "".join(unquoted), "".join(top)


def prepare_search_query(query):
    """Return an honest wire query and any predicate enforced from results.

    The search index resolves bare dates outside UTC and uses a stale likes
    snapshot for ``min_faves``. Exact UTC tokens go to the API; engagement is
    checked against each returned tweet's current ``likeCount`` instead.

    Moving ``min_faves`` out of the query is exact ONLY when every clause is a
    top-level AND conjunct: the query then means "rest AND likes >= max(N)".
    Under a top-level OR, inside a group, or negated, a clause binds to part
    of the expression, and one global threshold would change which tweets
    match. Those queries raise ValueError rather than being silently rewritten.
    """
    query = str(query or "")
    unquoted, top = _query_views(query)
    top_clauses = list(_MIN_FAVES.finditer(top))
    any_clauses = len(_ANY_MIN_FAVES.findall(unquoted))
    if any_clauses and len(top_clauses) != any_clauses:
        raise UsageError(
            f"min_faves inside a group or negated cannot be enforced exactly "
            f"in {query!r}. The search index filters min_faves on a stale "
            "likes snapshot, so this client moves min_faves out of the query "
            "and checks live likeCount instead, which is exact only for a "
            "top-level AND clause. Split the query into separate searches.")
    if top_clauses and _TOP_LEVEL_OR.search(top):
        raise UsageError(
            f"min_faves with a top-level OR in {query!r} cannot be enforced "
            "exactly: each min_faves binds to its own OR branch, and one "
            "global likes threshold would drop tweets that match. Run one "
            "search per OR branch instead.")
    for m in reversed(top_clauses):         # offsets are shared with `top`
        query = query[:m.start()] + query[m.end():]
    query = _BARE_SEARCH_DATE.sub(
        lambda m: f"{m.group(1)}:{m.group(2)}_00:00:00_UTC", query)
    thresholds = [int(m.group(1)) for m in top_clauses]
    return query.strip(), (max(thresholds) if thresholds else None)


def filter_search_tweets(tweets, min_faves):
    if min_faves is None:
        return tweets
    return [tweet for tweet in tweets
            if int(tweet.get("likeCount") or 0) >= min_faves]


def enrich_tweet_media(tweet):
    """Attach the public normalized media shape to a raw API tweet."""
    if not isinstance(tweet, dict):
        return tweet
    from store import extract_tweet_media
    enriched = dict(tweet)
    enriched["media"] = extract_tweet_media(tweet)
    return enriched


class APIError(RuntimeError):
    def __init__(self, status, detail, path):
        super().__init__(f"{status} on {path}: {detail}")
        self.status, self.detail, self.path = status, detail, path


class CostLimitExceeded(RuntimeError):
    pass


class UsageError(ValueError):
    """An invalid request, detected before any API call. The CLI contract
    maps it to exit 2."""


class SetupError(RuntimeError):
    """The client cannot be configured (no API key, invalid QPS). The CLI
    contract maps it to exit 2."""


class IncompleteDataError(RuntimeError):
    """The API indicated more data exists but the client cannot retrieve it safely."""


class PaginationStream:
    """Single-use iterator with explicit completeness metadata.

    A later page can fail after earlier records have already been retrieved and
    billed. Iteration then ends with ``complete=False`` and ``_warning`` set,
    preserving the usable records without presenting them as exhaustive.

    ``complete`` answers "did retrieval fail?". ``truncated_by_limit`` answers
    a different question: the caller's ``limit`` stopped the walk while the API
    still had more records, so the rows are the first N, not the population.
    A caller that asked for exactly N got what it asked for; a caller that
    treats the rows as a population (a cohort, a follower set) must check it.
    ``cached_pages`` counts pages served from the store instead of bought.
    """

    def __init__(self):
        self.complete = True
        self.truncated_by_limit = False
        self.cached_pages = 0
        self._warning = None
        self._iterator = None

    def __iter__(self):
        return self._iterator


def credits_to_usd(c: float) -> float:
    return c / CREDITS_PER_USD


_FACTS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "..", "references", "facts.json")
_stale_warned = False


def _warn_if_facts_stale():
    """Warn once per process when the recorded API facts are old.

    Prices, response shapes and page sizes here are OBSERVATIONS with a date,
    not permanent truths. A skill that states them confidently forever is the
    exact failure this one was built to replace, so the staleness is surfaced
    at the point of use rather than left for a reader to notice."""
    # Deliberately NOT gated on `verbose`: every analytical entry point builds
    # Client(verbose=False), so gating it meant the warning never fired for the
    # jobs most likely to act on a stale price. Once per process, to stderr.
    global _stale_warned
    if _stale_warned:
        return
    try:
        with open(_FACTS_PATH) as f:
            facts = json.load(f)
        stamped = facts.get("verified_at", "")
        limit = int(facts.get("staleness_warn_days", 90))
        age = int((time.time() - time.mktime(time.strptime(stamped, "%Y-%m-%d")))
                  / 86400)
    except Exception as e:
        _stale_warned = True
        print(f"[twitterapi] NOTE: cannot verify API fact freshness from "
              f"{_FACTS_PATH}: {type(e).__name__}. Prices and response shapes "
              f"may be stale. Re-verify with: python3 scripts/verify.py",
              file=sys.stderr)
        return                              # never let a warning break a run
    _stale_warned = True
    if age >= limit:
        print(f"[twitterapi] NOTE: API facts (prices, response shapes) were last "
              f"verified {age} days ago on {stamped}, over the {limit}-day limit. "
              f"Live APIs drift. Re-verify with: python3 scripts/verify.py",
              file=sys.stderr)


class Client:
    def __init__(self, api_key=None, timeout=45, max_usd=None, verbose=True,
                 store=None, cache_max_age=None, qps=None):
        # Configuration first: a missing key must fail before a store path is
        # opened, or the error leaks a sqlite connection.
        self.key = api_key or os.environ.get("TWITTERAPI_IO_KEY")
        if not self.key:
            raise SetupError(
                "TWITTERAPI_IO_KEY not set. Add it to ~/.zshenv; never hardcode it.")
        configured_qps = qps if qps is not None else os.environ.get(
            "TWITTERAPI_IO_QPS")
        try:
            self._qps = (float(configured_qps) if configured_qps is not None
                         else None)
        except ValueError:
            raise SetupError(f"qps must be a number, got {configured_qps!r}") from None
        if self._qps is not None and self._qps <= 0:
            raise SetupError("qps must be greater than zero")
        if isinstance(store, (str, os.PathLike)):
            # Accept the same path-oriented store contract as the CLIs, but
            # resolve it before any request can be sent and later fail during
            # paid write-through.
            from store import Store
            store = Store(os.fspath(store))
        self.timeout = timeout
        self.verbose = verbose
        self.max_usd = max_usd            # cumulative ceiling across all calls
        self.store = store                # optional read/write-through cache
        self.cache_max_age = cache_max_age  # seconds; None = cached copy never expires
        self.requests_made = 0
        self.cache_hits = 0
        self.oldest_cache_age = None   # seconds; how stale the served data is
        self.spent_credits = 0.0
        self.saved_credits = 0.0          # credits NOT spent thanks to cache hits
        self._last_call = 0.0
        self._fixed_qps = self._qps is not None
        self._qps_checked_at = 0
        self._lock = threading.Lock()
        # Worst-case credits booked by this thread's last successful request,
        # held until _charge/_settle replaces them with the real page cost.
        self._provisional = threading.local()
        _warn_if_facts_stale()

    # -- rate limiting ----------------------------------------------------
    def balance(self) -> int:
        """Credits remaining. NOTE: billing settles 20-60s late, so this
        lags real spend. Do not use it to price an individual call."""
        return int(self._raw("GET", "/oapi/my/info")["recharge_credits"])

    @property
    def qps(self) -> float:
        """Real QPS ceiling, fixed explicitly or derived from balance.

        The advertised 200 req/s is marketing copy; measured ceiling is 20.
        Explicit ``qps=`` (or ``TWITTERAPI_IO_QPS``) avoids balance reads; the
        derived mode re-checks because a falling balance changes the ladder.
        """
        if self._fixed_qps:
            return self._qps
        if self._qps is None or self.requests_made - self._qps_checked_at >= 500:
            try:
                bal = self.balance()
            except Exception:
                bal = 0
            new = next(q for floor, q in QPS_LADDER if bal >= floor)
            if self.verbose and new != self._qps:
                print(f"[twitterapi] balance {bal:,} credits "
                      f"(${credits_to_usd(bal):,.2f}) -> {new} QPS", file=sys.stderr)
            self._qps = new
            self._qps_checked_at = self.requests_made
        return self._qps

    def _throttle(self):
        gap = 1.0 / self.qps
        with self._lock:            # check+sleep+stamp must be atomic, or two
            now = time.monotonic()  # threads both pass the check and fire together
            wait = gap - (now - self._last_call)
            if wait > 0:
                time.sleep(wait)
            self._last_call = time.monotonic()

    # -- transport --------------------------------------------------------
    def _raw(self, method, path, params=None, *, max_credits=None):
        url = BASE + path
        if params:
            url += "?" + urllib.parse.urlencode(
                {k: v for k, v in params.items() if v not in (None, "")})

        def request():
            req = urllib.request.Request(url, method=method)
            req.add_header("x-api-key", self.key)
            return req
        body = self._send(method, path, request, max_credits)
        if not isinstance(body, dict):
            raise APIError(200, f"expected JSON object, got {type(body).__name__}", path)
        return body

    def _raw_json(self, method, path, body, *, max_credits=None):
        """Request with a JSON body. Used by bulk_search and the filter-rule
        endpoints — note DELETE /oapi/tweet_filter/delete_rule takes its
        rule_id in the BODY, not the query string. Shares _raw's transport, so
        a data path (bulk_search) has the same ceiling and retry rules."""
        data = json.dumps(body).encode()

        def request():
            req = urllib.request.Request(BASE + path, data=data, method=method)
            req.add_header("x-api-key", self.key)
            req.add_header("Content-Type", "application/json")
            return req
        return self._send(method, path, request, max_credits)

    def _send(self, method, path, make_request, max_credits):
        """One logical request with retries; every attempt books its cost.

        Each attempt books its WORST case (``max_credits``, default the
        15-credit floor) under the lock before it is sent, so concurrent
        requests cannot all pass the ceiling check and then overspend it.
        What happens to that booking depends on what the server told us:

        - success: the booking stays until _charge/_settle replaces it with
          the real page cost (same thread);
        - an HTTP error or an HTTP-200 body error: the server answered, and
          such answers bill the floor, so the excess is released;
        - a timeout or connection error: the response is lost and the server
          may have billed it in full, so the worst case stays booked. A retry
          books its own worst case and is refused if that crosses max_usd.

        Measured 2026-08-10: settled billing exceeded accounting that skipped
        data-less calls by ~1.2%, almost exactly the uncharged probes x 15
        credits, so every attempt books at least the floor. Exception:
        `/oapi/my/info` is FREE (21 calls moved the balance by 0 credits); it
        is counted as a request but never charged.
        """
        free = path == "/oapi/my/info"
        worst = max_credits or MIN_REQUEST_CREDITS
        attempts, last = 5, None
        for attempt in range(attempts):
            operation = (f"{method} {path} request" if last is None else
                         f"retry of {method} {path} after {type(last).__name__}")
            try:
                self._book_attempt(path, 0.0 if free else worst, operation)
            except CostLimitExceeded as exc:
                if last is None:
                    raise
                raise exc from last
            if not free:
                self._throttle()
            try:
                with urllib.request.urlopen(make_request(), timeout=self.timeout) as r:
                    raw_body = r.read()
                try:
                    body = json.loads(raw_body)
                except ValueError:
                    # A 200 with an HTML or truncated body is an API failure,
                    # not a bad argument. Whether it billed is unknown, so the
                    # worst case stays booked.
                    raise APIError(200, "response body is not JSON: "
                                   + raw_body[:120].decode("utf-8", "replace"),
                                   path) from None
            except urllib.error.HTTPError as e:
                self._release_to_floor(0.0 if free else worst)
                raw = e.read().decode("utf-8", "replace")
                try:
                    j = json.loads(raw)
                    detail = j.get("detail") or j.get("message") or j.get("msg") or raw
                except Exception:
                    detail = raw
                if e.code == 429 or e.code >= 500:
                    last = APIError(e.code, detail, path)
                    if attempt < attempts - 1:      # don't sleep before giving up
                        time.sleep(2 ** attempt)
                    continue
                raise APIError(e.code, detail, path) from None
            except (urllib.error.URLError, TimeoutError) as e:
                last = e                            # worst case stays booked
                if attempt < attempts - 1:
                    time.sleep(2 ** attempt)
                continue
            # This API signals failure in the body while returning HTTP 200
            # (verified: x_user_stream/add -> 200 {"status":"error", ...};
            # user/info on a missing account -> 200 {"status":"error",
            # "msg":"user not found"}). Treating that as success makes a
            # failed call look like an empty result. Surface it.
            if isinstance(body, dict) and body.get("status") == "error":
                self._release_to_floor(0.0 if free else worst)
                raise APIError(200, body.get("msg") or body.get("message")
                               or json.dumps(body)[:200], path)
            if not free:                # a free balance read settles nothing
                self._provisional.credits = worst
            return body
        raise last if isinstance(last, Exception) else RuntimeError("retries exhausted")

    # -- envelope normalisation ------------------------------------------
    @staticmethod
    def _unpack(resp, spec):
        """Return (items, has_next_page, next_cursor).

        The trap this exists for: data-wrapped endpoints nest the array under
        `data` but keep has_next_page/next_cursor at the ROOT. Reading
        pagination from inside `data` silently stops after one page.

        A paged endpoint MUST state has_next_page as a boolean at the root. A
        missing or non-boolean value is unknown, not "no more pages": reading
        it as False would mark a cut-short walk complete.
        """
        # `or {}` not a .get default: a suspended account returns data
        # present-but-null, and .get("data", {}) hands back None -> crash.
        container = (resp.get("data") or {}) if spec["items_in"] == "data" else resp
        if spec["items"] is None:
            items = container
        else:
            if spec["items"] not in container:
                raise IncompleteDataError(
                    f"response omitted required {spec['items']!r} field; "
                    f"refusing to interpret contract drift as an empty result")
            items = container.get(spec["items"])
            if not isinstance(items, list):
                raise IncompleteDataError(
                    f"response field {spec['items']!r} must be a list, got "
                    f"{type(items).__name__}; results are INCOMPLETE")
        if spec["items"] is None or not spec.get("paged", True):
            # A single object has no pages; neither does a list endpoint
            # whose verified envelope carries no pagination fields.
            return items, False, ""
        has_next = resp.get("has_next_page")
        if not isinstance(has_next, bool):
            raise IncompleteDataError(
                f"response omitted has_next_page (got {has_next!r}); whether "
                f"more pages exist is unknown, so results are INCOMPLETE")
        return items, has_next, resp.get("next_cursor") or ""

    # -- cost -------------------------------------------------------------
    @staticmethod
    def _rate_for(name, page_size):
        if name in PRICE_TIERS:
            return next(r for floor, r in PRICE_TIERS[name] if page_size >= floor)
        return FLAT_CREDITS["profile"] if name in (
            "user_info", "user_about", "batch_users", "search_users",
            "retweeters", "verified_followers", "followers", "followings",
            "list_members", "list_followers", "community_info",
            "check_follow") else FLAT_CREDITS["tweet"]

    @staticmethod
    def page_credits(name, records_in_page, page_size):
        """Cost of ONE request. The 15-credit minimum applies per request,
        not as a floor across the whole crawl."""
        rate = Client._rate_for(name, page_size)
        return max(records_in_page * rate, MIN_REQUEST_CREDITS)

    @staticmethod
    def effective_page_size(name, page_size=None):
        """Records one request can return, which is what it can bill.

        Only endpoints with a page-size parameter honour ``page_size``; a
        fixed endpoint returns its full page whatever the caller asked for, so
        budgeting with the caller's number under-reserves (search with
        page_size=1 still returns, and bills, 20 tweets). For id-list
        endpoints the page size is the number of ids sent."""
        spec = ENDPOINTS[name]
        if spec["page_param"] is None and not spec.get("key_list"):
            return spec["page_max"]
        return min(page_size or spec["page_max"], spec["page_max"])

    @staticmethod
    def credits_for(name, n_records, page_size=None):
        """Full-page credits to retrieve n_records.

        Bills WHOLE pages. You cannot buy a partial page: asking for 3,000 IDs
        at a 5,000 page size still returns — and still charges for — 5,000.
        Measured: limit=3000 cost 2,250 credits (a full page), not 1,350.

        This assumes the API fills each page. It is NOT an upper bound: the
        server decides how many records a page holds, every request bills at
        least the 15-credit floor, and short, empty or duplicate pages each
        cost a request. 200 ids served one per page cost 200 x 15 = 3,000
        credits against 2,250 here, and no record count bounds a server that
        keeps returning empty or repeated pages. The hard bound is max_usd:
        every request reserves its worst-case page cost against it first."""
        ps = Client.effective_page_size(name, page_size)
        if n_records <= 0:
            return MIN_REQUEST_CREDITS
        pages = -(-n_records // ps)                     # ceil
        return pages * Client.page_credits(name, ps, ps)

    def estimate(self, name, n_records, page_size=None) -> float:
        """Expected USD when every page is full. Use BEFORE any large crawl.

        Not an upper bound — see credits_for(). Pass max_usd to the Client
        for a hard bound."""
        return credits_to_usd(self.credits_for(name, n_records, page_size))

    @property
    def spent_usd(self) -> float:
        """Cost actually incurred this session, accumulated per page."""
        return credits_to_usd(self.spent_credits)

    def _charge(self, name, records_in_page, page_size):
        """Account one request's real cost; see _settle()."""
        self._settle(self.page_credits(name, records_in_page, page_size))

    def _settle(self, actual_credits):
        """Replace this thread's last request booking with its real cost.

        Deliberately actuals-based rather than estimate-based: this covers
        unbounded crawls, page-size overrides and repeated calls, none of
        which a pre-flight estimate can see. The request already booked its
        worst case (or, with no booking recorded, the 15-credit floor), so
        only the difference moves. Locked: a caller may share one Client
        across threads, and an unguarded += undercounts."""
        booked = getattr(self._provisional, "credits", MIN_REQUEST_CREDITS)
        self._provisional.credits = MIN_REQUEST_CREDITS
        with self._lock:
            self.spent_credits += actual_credits - booked

    def _book_attempt(self, path, credits, operation):
        """Count one request attempt and book its worst-case cost atomically.

        Check and booking share the lock, so two threads cannot both see room
        under max_usd and then both spend it."""
        with self._lock:
            if credits:
                self._require_budget(credits, operation)
            self.requests_made += 1     # count every attempt: failures bill too
            self.spent_credits += credits

    def _release_to_floor(self, booked):
        """The server answered with an error: it bills the floor, not a page."""
        if booked > MIN_REQUEST_CREDITS:
            with self._lock:
                self.spent_credits -= booked - MIN_REQUEST_CREDITS

    def _over_ceiling(self) -> bool:
        return self.max_usd is not None and self.spent_usd >= self.max_usd

    def _require_budget(self, credits, operation):
        """Refuse before transport when a bounded next charge is unauthorized.

        spent_credits includes the worst case booked by requests still in
        flight, so this also sees concurrent reservations."""
        if self.max_usd is None:
            return
        projected = self.spent_usd + credits_to_usd(credits)
        if projected > self.max_usd:
            raise CostLimitExceeded(
                f"{operation} can reach ${projected:,.5f}, over the "
                f"${self.max_usd:,.5f} ceiling (${self.spent_usd:,.5f} "
                f"already spent); refusing before the paid request.")

    def _preflight(self, name, n_records, page_size=None, *, full_scan=False):
        if full_scan:
            # A local predicate can reject every paid row, so no returned-row
            # limit can estimate the number of pages. Make max_usd the explicit
            # scan bound instead of presenting a limit-derived underestimate.
            if self.max_usd is None:
                raise UsageError(
                    f"{name} uses a local predicate that may require a full "
                    f"result scan; limit={n_records!r} bounds returned matches, "
                    "not fetched rows. Pass max_usd= to bound paid scan cost.")
            print(
                f"[twitterapi] WARNING: {name} uses a local predicate; "
                f"limit={n_records!r} bounds returned matches, not fetched "
                f"rows. The query may scan to exhaustion, bounded by the "
                f"cumulative max_usd=${self.max_usd:,.5f} ceiling.",
                file=sys.stderr,
            )
            return
        if self.max_usd is None or not n_records:
            return
        projected = self.spent_usd + self.estimate(name, n_records, page_size)
        if projected > self.max_usd:
            raise CostLimitExceeded(
                f"{name} for {n_records:,} records would reach ${projected:,.2f}, "
                f"over the ${self.max_usd:,.2f} ceiling "
                f"(${self.spent_usd:,.2f} already spent). Raise max_usd to proceed.")

    # -- generic paged fetch ----------------------------------------------
    def paginate(self, name, key, *, limit=None, extra=None, page_size=None,
                 key_param=None):
        """Yield records from a listing endpoint.

        Handles both envelope shapes, charges each page against the cumulative
        ceiling, and refuses to loop when the cursor stops advancing. The
        returned stream exposes ``complete``, ``truncated_by_limit``,
        ``cached_pages`` and ``_warning`` after iteration.
        """
        stream = PaginationStream()
        stream._iterator = self._paginate_records(
            stream, name, key, limit=limit, extra=extra, page_size=page_size,
            key_param=key_param)
        return stream

    def _paginate_records(self, stream, name, key, *, limit=None, extra=None,
                          page_size=None, key_param=None):
        if name not in ENDPOINTS:
            from difflib import get_close_matches
            valid = sorted(n for n, endpoint in ENDPOINTS.items()
                           if endpoint["items"] is not None)
            suggestion = get_close_matches(name, valid, n=1)
            hint = f" Did you mean {suggestion[0]!r}?" if suggestion else ""
            raise ValueError(
                f"unknown paginated endpoint {name!r}.{hint} Valid names: "
                f"{', '.join(valid)}")
        spec = ENDPOINTS[name]
        if spec["items"] is None:
            raise ValueError(
                f"'{name}' returns a single object, not a list — "
                f"call the dedicated method instead of paginate().")
        local_min_faves = None
        if name == "search":
            key, local_min_faves = prepare_search_query(key)
        key = self._resolve_endpoint_key(name, key)
        if spec.get("key_list"):
            ids = [part for part in str(key).split(",") if part.strip()]
            ps = min(max(len(ids), 1), spec["page_max"])
        else:
            ps = self.effective_page_size(name, page_size)
        self._preflight(
            name, limit, ps,
            full_scan=(local_min_faves is not None and local_min_faves > 0),
        )
        worst_page = self.page_credits(name, ps, ps)
        params = {key_param or spec["key_param"]: key}
        if spec["page_param"]:
            params[spec["page_param"]] = ps
        for k, v in (extra or {}).items():
            if spec["page_param"] and k == spec["page_param"]:
                # Would silently change the billing tier after preflight.
                raise ValueError(
                    f"set page size via page_size=, not extra[{k!r}]")
            params[k] = v

        cacheable = self.store is not None and name in CACHEABLE
        cursor, seen = "", 0
        seen_cursors, seen_items, empty_pages = set(), set(), 0
        while True:
            params["cursor"] = cursor
            cached = (self.store.get_page(spec["path"], params, self.cache_max_age)
                      if cacheable else None)
            try:
                if cached is not None:
                    resp = cached
                    items, has_next, next_cursor = self._unpack(resp, spec)
                    # Cache hit = no API call: don't charge, record what we avoided.
                    self.cache_hits += 1
                    stream.cached_pages += 1
                    self.saved_credits += self.page_credits(name, len(items), ps)
                    # Track HOW OLD the served data is. A follow graph cached
                    # indefinitely will answer "who follows X" with last year's
                    # snapshot, free and instantly, and look identical to a fresh
                    # crawl. Age must be visible or the caller cannot tell.
                    age = self.store.page_age(spec["path"], params)
                    if age is not None:
                        self.oldest_cache_age = max(self.oldest_cache_age or 0, age)
                else:
                    # The page has a known maximum. Reserve against that bound,
                    # not the hoped-for record count, or a terminal full page can
                    # cross the caller's ceiling and still return as "complete".
                    self._require_budget(worst_page, f"next '{name}' page")
                    resp = self._raw("GET", spec["path"], params,
                                     max_credits=worst_page)
                    try:
                        items, has_next, next_cursor = self._unpack(resp, spec)
                    except IncompleteDataError:
                        # The response arrived but cannot be read: what it
                        # billed is unknown, so keep the worst case booked.
                        self._settle(worst_page)
                        raise
                    self._charge(name, len(items), ps)
                    if cacheable:
                        self.store.put_page(
                            spec["path"], params, resp,
                            credits=self.page_credits(name, len(items), ps))
            except (APIError, IncompleteDataError, urllib.error.URLError,
                    TimeoutError) as exc:
                if not seen:
                    raise
                self._mark_partial(stream, name, seen, str(exc))
                return
            if name == "verified_followers":
                # This endpoint's membership is itself the verification fact,
                # but its user objects omit both verification fields. Attach
                # the independently known semantic before yielding/persisting;
                # the cached wire response remains untouched above.
                items = [dict(item, isBlueVerified=True)
                         if isinstance(item, dict) else item for item in items]
            if spec["items"] == "tweets":
                # Preserve the wire payload while adding a stable public field;
                # callers should not have to know extendedEntities.media.
                items = [enrich_tweet_media(item) for item in items]
            acquired_items = items
            # Persist records so a corpus can be read back for free later.
            # Only FRESH pages: a cache hit re-persisted would stamp an old
            # snapshot as just seen and overwrite newer rows with stale values.
            if (cached is None and self.store is not None and acquired_items
                    and isinstance(acquired_items[0], dict)):
                if spec["items"] == "tweets":
                    self.store.put_tweets(acquired_items)
                elif spec["items"] in ("followers", "followings", "members",
                                       "moderators", "users"):
                    self.store.put_accounts(acquired_items)

            items = (filter_search_tweets(acquired_items, local_min_faves)
                     if name == "search" else acquired_items)

            if not acquired_items:
                empty_pages += 1
            else:
                empty_pages = 0
            for position, it in enumerate(items):
                identity = self._item_identity(it)
                if identity is not None and identity in seen_items:
                    continue
                if identity is not None:
                    seen_items.add(identity)
                seen += 1
                if limit and seen >= limit:
                    # Decide BEFORE the last yield: a consumer that stops after
                    # N records never resumes the generator.
                    stream.truncated_by_limit = has_next or any(
                        self._item_identity(rest) is None
                        or self._item_identity(rest) not in seen_items
                        for rest in items[position + 1:])
                    yield it
                    return
                yield it
            if not acquired_items and not has_next and seen == 0:
                if name in COMMUNITY_COLLECTIONS:
                    # Empty is legitimate for a real community, so resolve
                    # the identity only at the otherwise ambiguous boundary.
                    # NOTE: community_info is an additional paid request for
                    # every genuinely empty community; this check is not free.
                    self.community_info(key)
                elif name in LIST_COLLECTIONS:
                    raise IncompleteDataError(
                        f"'{name}' returned no rows for list {key!r}, but this "
                        "API cannot distinguish an empty list from a private "
                        "list or one that does not exist. Results are "
                        "INCOMPLETE, not a confirmed zero.")
            if not has_next:
                return
            if not next_cursor:
                message = (f"'{name}' asserted has_next_page but returned no "
                           f"cursor after {seen:,} unique records; results are "
                           "INCOMPLETE.")
                if not seen:
                    raise IncompleteDataError(message)
                self._mark_partial(stream, name, seen, message)
                return
            if empty_pages >= 2:
                message = (f"'{name}' returned {empty_pages} empty pages while "
                           "asserting more data; results are INCOMPLETE.")
                if not seen:
                    raise IncompleteDataError(message)
                self._mark_partial(stream, name, seen, message)
                return
            if next_cursor in seen_cursors:
                message = (f"'{name}' repeated cursor {next_cursor!r} after "
                           f"{seen:,} unique records; refusing a paid loop. "
                           "Results are INCOMPLETE.")
                if not seen:
                    raise IncompleteDataError(message)
                self._mark_partial(stream, name, seen, message)
                return
            # Ceiling is checked here, after this page has been yielded, so the
            # caller keeps every record they were charged for.
            if self._over_ceiling():
                raise CostLimitExceeded(
                    f"Spend ceiling hit: ${self.spent_usd:,.2f} of "
                    f"${self.max_usd:,.2f} after {self.requests_made} requests "
                    f"on '{name}'. The {seen:,} records already yielded are "
                    f"valid and paid for. Raise max_usd to continue.")
            seen_cursors.add(next_cursor)
            cursor = next_cursor

    def _resolve_endpoint_key(self, name, key):
        """Translate a public endpoint key into the API's wire-level key."""
        spec = ENDPOINTS[name]
        if spec.get("key_resolver") != "user_id":
            return key
        value = str(key or "").strip().lstrip("@")
        if value.isdigit():
            return value
        profile = self.user_info(value)
        user_id = profile.get("id") if isinstance(profile, dict) else None
        if user_id in (None, ""):
            raise IncompleteDataError(
                f"could not resolve handle {key!r} to the numeric userId "
                f"required by '{name}'")
        return str(user_id)

    @staticmethod
    def _mark_partial(stream, name, seen, detail):
        stream.complete = False
        stream._warning = (
            f"PARTIAL: '{name}' stopped after {seen:,} usable, paid records: "
            f"{detail}")
        print(f"[twitterapi] STOPPED: {stream._warning}", file=sys.stderr)

    @staticmethod
    def _item_identity(item):
        """Stable cross-page identity, or None when deduplication is unsafe."""
        if isinstance(item, (str, int)):
            return ("scalar", str(item))
        if isinstance(item, dict):
            for key in ("id", "id_str", "user_id", "tweet_id"):
                value = item.get(key)
                if value not in (None, ""):
                    return (key, str(value))
        return None

    # -- convenience ------------------------------------------------------
    def user_info(self, user_name) -> dict:
        spec = ENDPOINTS["user_info"]
        # A missing/suspended account comes back HTTP 200 with
        # {"status":"error","msg":"user not found"} — _raw raises APIError(200)
        # for it rather than returning {}, so callers can't mistake "no such
        # user" for "user with zero followers". A success envelope whose
        # `data` is null or empty carries no profile either: same error, never
        # an empty dict that reads as a user with no followers.
        self._require_budget(self.page_credits("user_info", 1, 1),
                             "user_info lookup")
        resp = self._raw("GET", spec["path"], {"userName": user_name},
                         max_credits=self.page_credits("user_info", 1, 1))
        self._charge("user_info", 1, 1)     # a profile lookup bills 18 credits
        data = resp.get("data")
        if not isinstance(data, dict) or not data:
            raise APIError(200, f"user {user_name!r} not found: response "
                                f"data is {data!r}", spec["path"])
        return data

    def follower_count(self, user_name) -> int:
        followers = self.user_info(user_name).get("followers")
        if followers in (None, ""):
            raise IncompleteDataError(
                f"profile for {user_name!r} omitted the followers count; "
                "refusing to report an unknown count as 0")
        return int(followers)

    def follower_ids(self, user_name=None, *, user_id=None, limit=None):
        """Cheapest way to enumerate followers: 0.45 credits/id at 5000/call
        vs 1.0 for full profiles at 200/call — 2.2x cheaper and 25x fewer
        requests. Accepts userName OR userId."""
        if not (user_name or user_id):
            raise ValueError("pass user_name or user_id")
        return self.paginate(
            "follower_ids", str(user_id or user_name), limit=limit,
            key_param="userId" if user_id else "userName")

    def followers(self, user_name, *, limit=None):
        """Full profiles. Prefer follower_ids() unless you need profile fields."""
        return self.paginate("followers", user_name, limit=limit)

    def verified_followers(self, user_id, *, limit=None):
        return self.paginate("verified_followers", str(user_id), limit=limit)

    def last_tweets(self, user_name, *, limit=None, include_replies=False):
        return self.paginate("last_tweets", user_name, limit=limit,
                             extra={"includeReplies": str(include_replies).lower()})

    def bulk_search(self, queries, query_type="Latest"):
        """Run N searches in ONE request. Verified: returns
        {results: {query_0: {tweets, has_next_page, next_cursor}, ...}}.

        This is the only lever against the QPS ceiling — 20 req/s is the hard
        maximum, so batching N queries per request multiplies throughput by N.
        Use it whenever you have several independent searches."""
        if not queries:
            return []
        prepared, local_min_faves = [], []
        for query in queries:
            item = (dict(query) if isinstance(query, dict)
                    else {"query": query, "queryType": query_type})
            item["query"], threshold = prepare_search_query(item.get("query"))
            prepared.append(item)
            local_min_faves.append(threshold)
        body = {"queries": prepared}
        full_page = self.page_credits("search", 20, 20)
        self._require_budget(len(body["queries"]) * full_page,
                             f"bulk_search batch of {len(body['queries'])} queries")
        resp = self._raw_json("POST", "/twitter/tweet/bulk_advanced_search", body,
                              max_credits=len(body["queries"]) * full_page)
        results = resp.get("results") or {}
        if not isinstance(results, dict):
            results = {}
        out = []
        invalid = []
        charges = []
        for i in range(len(body["queries"])):
            result_key = f"query_{i}"
            r = results.get(result_key)
            if not isinstance(r, dict) or "tweets" not in r:
                invalid.append(result_key)
                charges.append(full_page)  # unknown response: never undercount
                continue
            tweets = r.get("tweets")
            if not isinstance(tweets, list) or not isinstance(
                    r.get("has_next_page"), bool):
                # A missing has_next_page is unknown, never "no more pages"
                # (verified 2026-10-23: every live subresult carries a bool,
                # including empty ones).
                invalid.append(result_key)
                charges.append(full_page)
                continue
            charges.append(self.page_credits("search", len(tweets), 20))
            tweets = [enrich_tweet_media(tweet) for tweet in tweets]
            tweets = filter_search_tweets(tweets, local_min_faves[i])
            # has_next_page is authoritative for "more exists"; a short page is
            # NOT proof of completeness (results can be filtered). Callers that
            # infer from length alone silently lose data.
            out.append({"tweets": tweets,
                        "has_next_page": r["has_next_page"],
                        "next_cursor": r.get("next_cursor") or ""})
        # ONE request produced N billable search results: settle its booking
        # to the combined pages. Missing/malformed subresults are
        # conservatively booked full.
        self._settle(sum(charges))
        if invalid:
            raise IncompleteDataError(
                f"bulk_search response omitted or malformed {invalid}; refusing "
                f"to report those queries as empty. Results are INCOMPLETE.")
        return out

    def community_info(self, community_id) -> dict:
        self._require_budget(self.page_credits("community_info", 1, 1),
                             "community_info lookup")
        resp = self._raw("GET", "/twitter/community/info",
                         {"community_id": community_id},
                         max_credits=self.page_credits("community_info", 1, 1)) or {}
        self._charge("community_info", 1, 1)   # single record, profile-priced
        info = resp.get("community_info") or {}
        if info.get("id") in (None, ""):
            raise APIError(
                200, f"community {community_id!r} not found (response id is null)",
                ENDPOINTS["community_info"]["path"])
        return info

    def check_follow(self, source_user_name, target_user_name) -> dict:
        """Returns {following, followed_by}. NOTE: this endpoint uniquely uses
        `message` instead of `msg` in its envelope."""
        self._require_budget(self.page_credits("check_follow", 1, 1),
                             "check_follow lookup")
        resp = self._raw("GET", "/twitter/user/check_follow_relationship",
                         {"source_user_name": source_user_name,
                          "target_user_name": target_user_name},
                         max_credits=self.page_credits("check_follow", 1, 1)) or {}
        self._charge("check_follow", 1, 1)     # single record, profile-priced
        data = resp.get("data")
        if not isinstance(data, dict) or not data:
            # An empty dict would read as "neither follows the other".
            raise IncompleteDataError(
                f"check_follow_relationship returned no relationship data for "
                f"@{source_user_name} -> @{target_user_name}; the relationship "
                f"is unknown, not absent")
        return data

    def replies(self, tweet_id, *, limit=None, query_type="Latest", v2=True):
        """Replies to a tweet. v2 returns ~31-38 per call vs 20 for v1/plain,
        so it is ~1.8x fewer requests for identical data — and it sorts."""
        if v2:
            return self.paginate("replies_v2", str(tweet_id), limit=limit,
                                 extra={"queryType": query_type})
        return self.paginate("replies", str(tweet_id), limit=limit)

    def spend_report(self) -> str:
        saved = (f" | {self.cache_hits} cache hits saved "
                 f"${credits_to_usd(self.saved_credits):,.4f}" if self.cache_hits else "")
        # Served-data age is part of the result's meaning: a follow graph has
        # no expiry, so a free instant answer may be an old snapshot. Say how
        # old rather than letting it pass for fresh.
        stale = ""
        if self.oldest_cache_age:
            a = self.oldest_cache_age
            human = (f"{a/86400:.1f} days" if a >= 86400 else
                     f"{a/3600:.1f} hours" if a >= 3600 else f"{a/60:.0f} min")
            stale = f" | oldest cached data served: {human} old"
        return (f"{self.requests_made} requests, ~${self.spent_usd:,.4f} "
                f"({self.spent_credits:,.0f} credits) this session{saved}{stale}. "
                f"Server-side billing settles ~60s late, so .balance() will "
                f"trail this figure briefly.")


def main(argv=None):
    from axi import run_cli
    return run_cli("twitterapi.py", _main, argv)


def _main(argv=None):
    from axi import ArgumentParser, display_path, emit, fast_version, success
    if fast_version(argv):
        return 0
    p = ArgumentParser(description="Inspect the read-only twitterapi.io client")
    p.add_argument("--balance", action="store_true",
                   help="read the current credit balance (live, free endpoint)")
    args = p.parse_args(argv)
    body = {
        "bin": display_path(__file__),
        "description": "Standard-library read-only client for twitterapi.io",
        "endpoint_count": len(ENDPOINTS),
        "write_operations": "none",
        "help": ["Run `twitterapi.py --balance` to read account balance",
                 "Run `jobs.py` for cached and analytical workflows"],
    }
    if not args.balance:
        emit(success("twitterapi.py", body)); return 0
    c = Client(verbose=False)
    credits = c.balance()
    emit(success("twitterapi.py --balance", {
        "balance_credits": credits, "balance_usd": round(credits_to_usd(credits), 2),
        "qps": c.qps}))
    return 0


if __name__ == "__main__":
    # Run through the importable module: run as a script, this file's classes
    # would be __main__.SetupError etc., which axi.classify (importing
    # `twitterapi`) would not recognise, so a missing key would exit 1, not 2.
    import twitterapi
    raise SystemExit(twitterapi.main())
