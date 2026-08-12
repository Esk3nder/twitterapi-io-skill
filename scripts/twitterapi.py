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
        items="users", items_in="root", page_param=None, page_max=100),
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
        items="tweets", items_in="data", page_param=None, page_max=20),
    "mentions": dict(
        path="/twitter/user/mentions", key_param="userName",
        items="tweets", items_in="root", page_param=None, page_max=20),
    "tweets_by_ids": dict(
        path="/twitter/tweets", key_param="tweet_ids",
        items="tweets", items_in="root", page_param=None, page_max=100),
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
        items="trends", items_in="root", page_param="count", page_max=50),
    # Verified live. Note `query` is REQUIRED on this one.
    "community_search": dict(
        path="/twitter/community/get_tweets_from_all_community", key_param="query",
        items="tweets", items_in="root", page_param=None, page_max=20),
    # --- read surface completed 2026-08-10, all live-probed ---
    # replies/v2 returns 31-38 per call vs 20 for plain /replies: ~1.8x fewer
    # requests for the same data. Prefer it. queryType: Relevance|Latest|Likes.
    "replies_v2": dict(
        path="/twitter/tweet/replies/v2", key_param="tweetId",
        items="tweets", items_in="root", page_param=None, page_max=38),
    "replies_v1": dict(
        path="/twitter/tweet/replies/v1", key_param="tweetId",
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


class APIError(RuntimeError):
    def __init__(self, status, detail, path):
        super().__init__(f"{status} on {path}: {detail}")
        self.status, self.detail, self.path = status, detail, path


class CostLimitExceeded(RuntimeError):
    pass


class IncompleteDataError(RuntimeError):
    """The API indicated more data exists but the client cannot retrieve it safely."""


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
                 store=None, cache_max_age=None):
        self.key = api_key or os.environ.get("TWITTERAPI_IO_KEY")
        if not self.key:
            raise RuntimeError(
                "TWITTERAPI_IO_KEY not set. Add it to ~/.zshenv; never hardcode it.")
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
        self._qps = None
        self._qps_checked_at = 0
        self._lock = threading.Lock()
        _warn_if_facts_stale()

    # -- rate limiting ----------------------------------------------------
    def balance(self) -> int:
        """Credits remaining. NOTE: billing settles 20-60s late, so this
        lags real spend. Do not use it to price an individual call."""
        return int(self._raw("GET", "/oapi/my/info")["recharge_credits"])

    @property
    def qps(self) -> float:
        """Real QPS ceiling, derived from balance. The advertised 200 req/s is
        marketing copy; measured ceiling is 20. Re-checked periodically because
        a falling balance drops you down the ladder mid-crawl."""
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
        req = urllib.request.Request(url, method=method)
        req.add_header("x-api-key", self.key)
        last = None
        attempts = 5
        for attempt in range(attempts):
            if path != "/oapi/my/info":
                self._require_budget(max_credits or MIN_REQUEST_CREDITS,
                                     f"{method} {path} request attempt")
                self._throttle()
            self._bill_request(path)    # non-data calls still bill the 15cr floor
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as r:
                    body = json.loads(r.read())
                if not isinstance(body, dict):
                    raise APIError(200, f"expected JSON object, got {type(body).__name__}", path)
                # This API signals failure in the body while returning HTTP 200
                # (verified: x_user_stream/add -> 200 {"status":"error", ...};
                # user/info on a missing account -> 200 {"status":"error",
                # "msg":"user not found"}). Treating that as success makes a
                # failed call look like an empty result. Surface it.
                if body.get("status") == "error":
                    raise APIError(200, body.get("msg") or body.get("message")
                                   or json.dumps(body)[:200], path)
                return body
            except urllib.error.HTTPError as e:
                body = e.read().decode("utf-8", "replace")
                try:
                    detail = json.loads(body).get("detail") or json.loads(body).get("message") or body
                except Exception:
                    detail = body
                if e.code == 429 or e.code >= 500:
                    last = APIError(e.code, detail, path)
                    if attempt < attempts - 1:      # don't sleep before giving up
                        time.sleep(2 ** attempt)
                    continue
                raise APIError(e.code, detail, path) from None
            except (urllib.error.URLError, TimeoutError) as e:
                last = e
                if attempt < attempts - 1:
                    time.sleep(2 ** attempt)
        raise last if isinstance(last, Exception) else RuntimeError("retries exhausted")

    def _raw_json(self, method, path, body, *, max_credits=None):
        """Request with a JSON body. Used by bulk_search and the filter-rule
        endpoints — note DELETE /oapi/tweet_filter/delete_rule takes its
        rule_id in the BODY, not the query string.

        Mirrors _raw's ceiling check and 5xx/429 retry so a data path
        (bulk_search) isn't less resilient than a GET."""
        data = json.dumps(body).encode()
        attempts, last = 5, None
        for attempt in range(attempts):
            self._require_budget(max_credits or MIN_REQUEST_CREDITS,
                                 f"{method} {path} request attempt")
            self._throttle()
            self._bill_request(path)   # non-data calls still bill the 15cr floor
            req = urllib.request.Request(BASE + path, data=data, method=method)
            req.add_header("x-api-key", self.key)
            req.add_header("Content-Type", "application/json")
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as r:
                    body_resp = json.loads(r.read())
                # Same HTTP-200-with-body-error pattern as _raw. Without this,
                # e.g. a failed add_rule prints as if it succeeded.
                if isinstance(body_resp, dict) and body_resp.get("status") == "error":
                    raise APIError(200, body_resp.get("msg") or body_resp.get("message")
                                   or json.dumps(body_resp)[:200], path)
                return body_resp
            except urllib.error.HTTPError as e:
                raw = e.read().decode("utf-8", "replace")
                try:
                    j = json.loads(raw)
                    detail = j.get("detail") or j.get("message") or j.get("msg") or raw
                except Exception:
                    detail = raw
                if e.code == 429 or e.code >= 500:
                    last = APIError(e.code, detail, path)
                    if attempt < attempts - 1:
                        time.sleep(2 ** attempt)
                    continue
                raise APIError(e.code, detail, path) from None
            except (urllib.error.URLError, TimeoutError) as e:
                last = e
                if attempt < attempts - 1:
                    time.sleep(2 ** attempt)
        raise last if isinstance(last, Exception) else RuntimeError("retries exhausted")

    # -- envelope normalisation ------------------------------------------
    @staticmethod
    def _unpack(resp, spec):
        """Return (items, has_next_page, next_cursor).

        The trap this exists for: data-wrapped endpoints nest the array under
        `data` but keep has_next_page/next_cursor at the ROOT. Reading
        pagination from inside `data` silently stops after one page.
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
        return items, bool(resp.get("has_next_page")), resp.get("next_cursor") or ""

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
    def credits_for(name, n_records, page_size=None):
        """Credits to retrieve n_records.

        Bills WHOLE pages. You cannot buy a partial page: asking for 3,000 IDs
        at a 5,000 page size still returns — and still charges for — 5,000.
        Measured: limit=3000 cost 2,250 credits (a full page), not 1,350.
        Rounding up is also the safe direction for a spend guard."""
        ps = min(page_size or ENDPOINTS[name]["page_max"],
                 ENDPOINTS[name]["page_max"])           # API clamps; so do we
        if n_records <= 0:
            return MIN_REQUEST_CREDITS
        pages = -(-n_records // ps)                     # ceil
        return pages * Client.page_credits(name, ps, ps)

    def estimate(self, name, n_records, page_size=None) -> float:
        """Estimated USD. Use BEFORE any large crawl."""
        return credits_to_usd(self.credits_for(name, n_records, page_size))

    @property
    def spent_usd(self) -> float:
        """Cost actually incurred this session, accumulated per page."""
        return credits_to_usd(self.spent_credits)

    def _charge(self, name, records_in_page, page_size):
        """Account one request's real cost.

        Deliberately actuals-based rather than estimate-based: this covers
        unbounded crawls, page-size overrides and repeated calls, none of
        which a pre-flight estimate can see. Accounting only — the ceiling is
        enforced by _over_ceiling() AFTER the page is yielded, so data already
        paid for is never discarded.

        Locked: the multi-threaded history path calls this concurrently and an
        unguarded += undercounts — the wrong direction for a spend guard."""
        # _bill_request already booked MIN_REQUEST_CREDITS for the HTTP call
        # that produced this page, so add only the amount ABOVE that floor.
        # (page_credits is max(records*rate, MIN), so this never goes negative.)
        cost = self.page_credits(name, records_in_page, page_size) - MIN_REQUEST_CREDITS
        with self._lock:
            self.spent_credits += max(0.0, cost)

    def _bill_request(self, path=""):
        """Book the per-request floor the server charges for calls that return
        no data: 400s, 404s, HTTP-200 body errors, retried 5xx attempts, and
        rule lookups.

        Measured 2026-08-10: a full live run's settled billing exceeded naive
        client-side accounting by ~1.2%, and the gap was almost exactly the
        number of uncharged probe requests x 15 credits. Undercounting is the
        wrong direction for a spend guard, so the floor is booked up front and
        _charge() adds only the excess.

        EXCEPTION, measured: `/oapi/my/info` is FREE — 21 consecutive calls
        moved the balance by exactly 0 credits. It is counted as a request but
        never charged, or every lazy qps lookup would inflate spend by 15."""
        with self._lock:
            self.requests_made += 1     # count every attempt: failures bill too
            if path != "/oapi/my/info":
                self.spent_credits += MIN_REQUEST_CREDITS

    def _over_ceiling(self) -> bool:
        return self.max_usd is not None and self.spent_usd >= self.max_usd

    def _require_budget(self, credits, operation):
        """Refuse before transport when a bounded next charge is unauthorized."""
        if self.max_usd is None:
            return
        projected = self.spent_usd + credits_to_usd(credits)
        if projected > self.max_usd:
            raise CostLimitExceeded(
                f"{operation} can reach ${projected:,.5f}, over the "
                f"${self.max_usd:,.5f} ceiling (${self.spent_usd:,.5f} "
                f"already spent); refusing before the paid request.")

    def _preflight(self, name, n_records, page_size=None):
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
        ceiling, and refuses to loop when the cursor stops advancing."""
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
        self._preflight(name, limit, page_size)
        ps = min(page_size or spec["page_max"], spec["page_max"])
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
            if cached is not None:
                resp = cached
                items, has_next, next_cursor = self._unpack(resp, spec)
                # Cache hit = no API call: don't charge, record what we avoided.
                self.cache_hits += 1
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
                self._require_budget(self.page_credits(name, ps, ps),
                                     f"next '{name}' page")
                resp = self._raw("GET", spec["path"], params,
                                 max_credits=self.page_credits(name, ps, ps))
                items, has_next, next_cursor = self._unpack(resp, spec)
                self._charge(name, len(items), ps)
                if cacheable:
                    self.store.put_page(spec["path"], params, resp,
                                        credits=self.page_credits(name, len(items), ps))
            # Persist records so a corpus can be read back for free later.
            if self.store is not None and items and isinstance(items[0], dict):
                if spec["items"] == "tweets":
                    self.store.put_tweets(items)
                elif spec["items"] in ("followers", "followings", "members",
                                       "moderators", "users"):
                    self.store.put_accounts(items)

            if not items:
                empty_pages += 1
            else:
                empty_pages = 0
            for it in items:
                identity = self._item_identity(it)
                if identity is not None and identity in seen_items:
                    continue
                if identity is not None:
                    seen_items.add(identity)
                yield it
                seen += 1
                if limit and seen >= limit:
                    return
            if not has_next:
                return
            if not next_cursor:
                raise IncompleteDataError(
                    f"'{name}' asserted has_next_page but returned no cursor "
                    f"after {seen:,} unique records; results are INCOMPLETE.")
            if empty_pages >= 2:
                raise IncompleteDataError(
                    f"'{name}' returned {empty_pages} empty pages while asserting "
                    f"more data; results are INCOMPLETE.")
            if next_cursor in seen_cursors:
                raise IncompleteDataError(
                    f"'{name}' repeated cursor {next_cursor!r} after {seen:,} "
                    f"unique records; refusing a paid loop. Results are INCOMPLETE.")
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
        # user" for "user with zero followers". `data` is coalesced in case it
        # is ever null on a success envelope.
        self._require_budget(self.page_credits("user_info", 1, 1),
                             "user_info lookup")
        resp = self._raw("GET", spec["path"], {"userName": user_name},
                         max_credits=self.page_credits("user_info", 1, 1))
        self._charge("user_info", 1, 1)     # a profile lookup bills 18 credits
        return resp.get("data") or {}

    def follower_count(self, user_name) -> int:
        return int(self.user_info(user_name).get("followers") or 0)

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
        body = {"queries": [
            q if isinstance(q, dict) else {"query": q, "queryType": query_type}
            for q in queries]}
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
            if not isinstance(tweets, list):
                invalid.append(result_key)
                charges.append(full_page)
                continue
            charges.append(self.page_credits("search", len(tweets), 20))
            # has_next_page is authoritative for "more exists"; a short page is
            # NOT proof of completeness (results can be filtered). Callers that
            # infer from length alone silently lose data.
            out.append({"tweets": tweets,
                        "has_next_page": bool(r.get("has_next_page")),
                        "next_cursor": r.get("next_cursor") or ""})
        # ONE request produced N billable search results. _bill_request booked
        # one 15-credit request floor, so add the combined pages minus that one
        # floor. Missing/malformed subresults are conservatively booked full.
        with self._lock:
            self.spent_credits += max(sum(charges) - MIN_REQUEST_CREDITS, 0.0)
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
        return resp.get("community_info", {})

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
        return resp.get("data", {})

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


if __name__ == "__main__":
    c = Client()
    print(f"balance ${credits_to_usd(c.balance()):,.2f} | {c.qps} QPS\n")
    print("200M followers of a mega-account:")
    for n in ("follower_ids", "followers"):
        print(f"  {n:14s} -> ${c.estimate(n, 200_000_000):>12,.2f}")
    print(f"  {'followers@20':14s} -> ${c.estimate('followers', 200_000_000, 20):>12,.2f}"
          "   <- same data, default page size")
