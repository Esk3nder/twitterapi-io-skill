#!/usr/bin/env python3
"""Local cache + cohort store. sqlite3 from the standard library.

Why this exists: resolving a cohort is the expensive part of every analytical
workflow, and reading what that cohort posted is cheap. Without persistence,
every follow-up question re-pays for the crawl, which makes composition
economically impossible. With it, cost moves from per-run to per-corpus:
buy "the polymarket cohort" once, then ask it unlimited questions for free.

Three tables:
  pages     raw API responses keyed by (path, params) — the fetch cache
  accounts  normalised user records
  tweets    normalised tweet records
  cohorts   named, versioned account sets with provenance and weight

Normalisation matters as much as caching: `audience` emits ids, `history`
emits tweets, `community_members` emits users. They cannot compose until they
share a shape.
"""
from __future__ import annotations

import contextlib
import json
import os
import sqlite3
import sys
import threading
import time
import urllib.parse
from datetime import datetime, timezone

DEFAULT_DB = os.path.expanduser("~/.twitterapi-cache/store.db")
# Request params that carry an X handle. Handles are case-insensitive, so the
# page-cache key folds their case (see Store._key).
HANDLE_PARAMS = ("userName", "username")
TW_FMT = "%a %b %d %H:%M:%S %z %Y"

# Every tweet fact column is nullable: NULL means the source record did not
# carry the field (UNKNOWN), never zero / false / no media. `media` holds a
# JSON list; NULL there means the record had no media keys at all, while '[]'
# means the record asserted "no media". {name} lets the migration build the
# same table under a temporary name — one definition, no drift.
TWEETS_DDL = """CREATE TABLE IF NOT EXISTS {name} (
  id TEXT PRIMARY KEY, author_id TEXT, author_name TEXT,
  created_ts INTEGER, text TEXT, lang TEXT,
  likes INTEGER, retweets INTEGER, replies INTEGER, quotes INTEGER,
  views INTEGER, is_reply INTEGER, is_quote INTEGER, is_retweet INTEGER,
  conversation_id TEXT, in_reply_to TEXT,
  media TEXT, raw TEXT, seen_at REAL);"""
TWEETS_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_tweets_author ON tweets(author_id);
CREATE INDEX IF NOT EXISTS idx_tweets_ts ON tweets(created_ts);"""
TWEET_COLUMNS = ("id", "author_id", "author_name", "created_ts", "text", "lang",
                 "likes", "retweets", "replies", "quotes", "views", "is_reply",
                 "is_quote", "is_retweet", "conversation_id", "in_reply_to",
                 "media")

SCHEMA = """
CREATE TABLE IF NOT EXISTS pages (
  key TEXT PRIMARY KEY, path TEXT, params TEXT,
  body TEXT, fetched_at REAL, credits REAL DEFAULT 0);
CREATE TABLE IF NOT EXISTS accounts (
  id TEXT PRIMARY KEY, user_name TEXT, name TEXT, description TEXT,
  followers INTEGER, following INTEGER, statuses INTEGER,
  is_blue INTEGER, is_verified INTEGER, created_at TEXT,
  location TEXT, raw TEXT, seen_at REAL);
CREATE INDEX IF NOT EXISTS idx_accounts_user ON accounts(user_name);
""" + TWEETS_DDL.format(name="tweets") + TWEETS_INDEXES + """
CREATE TABLE IF NOT EXISTS cohorts (
  name TEXT, version INTEGER, member_key TEXT, account_id TEXT, user_name TEXT,
  weight REAL DEFAULT 1.0, provenance TEXT, added_at REAL,
  PRIMARY KEY (name, version, member_key));
CREATE TABLE IF NOT EXISTS cohort_meta (
  name TEXT, version INTEGER, spec TEXT, created_at REAL, size INTEGER,
  PRIMARY KEY (name, version));
"""


def parse_ts(s):
    """createdAt -> unix seconds. Returns None on anything unexpected rather
    than raising, because one malformed record must not kill a crawl.

    TWO formats are in play and the endpoints disagree about which they use:
      'Tue Jun 27 13:41:42 +0000 2017'  tweets, tweet-embedded authors,
                                        /user/followers (as created_at)
      '2017-06-27T13:41:42.000000Z'     /user/info, /tweet/retweeters
    Handling only the first silently zeroed every age signal derived from
    retweeters — measured 0 of 40 parseable — which is what Cohort.from_engagers
    builds on."""
    if not s:
        return None
    try:
        return int(datetime.strptime(s, TW_FMT).timestamp())
    except Exception:
        pass
    try:
        # fromisoformat handles the trailing Z only from 3.11; normalise it so
        # the supported-Python floor stays where the rest of the skill sets it.
        return int(datetime.fromisoformat(
            str(s).replace("Z", "+00:00")).timestamp())
    except Exception:
        return None


def normalize_account(u: dict) -> dict:
    """One shape for user records regardless of which endpoint produced them.

    Necessary because endpoints disagree: /user/info returns camelCase
    (`userName`, `followers`), while /user/followers returns snake_case
    (`followers_count`, `created_at`). Verified live 2026-08-10.
    """
    if not isinstance(u, dict):
        return {}
    g = lambda *ks: next((u[k] for k in ks if u.get(k) not in (None, "")), None)
    def optional_bool(*keys):
        values = [u[k] for k in keys if k in u and u[k] not in (None, "")]
        return None if not values else int(any(bool(value) for value in values))

    def optional_bio(*keys):
        """Absent -> None (unknown). Present -> the value, including "" when the
        source genuinely asserts an empty bio — the upsert treats "" as
        authoritative, so a hollow "" must never come out of here.

        Author-shaped records put the real bio at profile_bio.description
        and leave the top-level key "" (measured on the production store:
        2,387 of 2,935 tweet authors). So a non-empty nested bio beats an
        empty top-level one, and a nested "" alone asserts nothing.
        put_tweets additionally strips the top-level key from tweet authors,
        whose "" is hollow even without a nested bio."""
        top = next((u[k] for k in keys if k in u), None)
        if top not in (None, ""):
            return str(top)
        nested = u.get("profile_bio")
        nested = nested.get("description") if isinstance(nested, dict) else None
        if nested not in (None, ""):
            return str(nested)
        return "" if any(k in u for k in keys) else None

    def optional_int(*keys):
        """Absent -> None (unknown). Present -> int, including a genuine 0.
        g() already returns 0 for a real zero and None only when every alias is
        missing or empty, so the distinction survives."""
        value = g(*keys)
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    return {
        "id": str(g("id", "user_id") or ""),
        "user_name": g("userName", "screen_name", "username") or "",
        "name": g("name") or "",
        # Tweet-embedded author objects return description="" and put the real
        # bio at profile_bio.description. Reading only the top-level key makes
        # every tweet-sourced account look bio-less — and then overwrite a
        # richer row. Measured: 143 chars -> 0.
        # ABSENT means unknown; PRESENT-but-empty is a real assertion that the
        # account has no bio (verified against /followers: genuinely bio-less
        # 10 of 10). g() collapses both to None, so the key is probed directly.
        "description": optional_bio("description", "bio"),
        # Absent counts mean UNKNOWN, not zero. Coercing them to 0 forced the
        # upsert to use MAX() to protect a good value from a thin record — and
        # MAX then made a genuine DECLINE unwritable, so the store recorded the
        # highest value ever seen rather than the current one. Nullable counts
        # let the upsert prefer the incoming value whenever there is one, which
        # keeps thin records harmless AND lets real decreases land. A genuine
        # zero still stores as 0, because g() returns 0 rather than None for it.
        "followers": optional_int("followers", "followers_count"),
        "following": optional_int("following", "following_count", "friends_count"),
        "statuses": optional_int("statusesCount", "statuses_count"),
        # Missing verification fields mean UNKNOWN, not false. Several thin
        # endpoint shapes omit both keys; coercing absence to 0 made those
        # accounts false positives for verified=False and could overwrite a
        # later authoritative value.
        "is_blue": optional_bool("isBlueVerified", "is_blue_verified"),
        "is_verified": optional_bool("isVerified", "verified"),
        "created_at": g("createdAt", "created_at") or "",
        "location": g("location") or "",
    }


def full_resolution_media_url(url):
    """Return the original-size pbs.twimg.com photo URL documented by X.

    Non-photo/CDN URLs are returned unchanged. The client does not download
    bytes; callers fetching this URL must send a browser User-Agent because
    pbs.twimg.com rejects bare urllib requests in observed use.
    """
    if not isinstance(url, str) or not url:
        return url
    parts = urllib.parse.urlsplit(url)
    if parts.netloc.casefold() != "pbs.twimg.com" or "/media/" not in parts.path:
        return url
    leaf = parts.path.rsplit("/", 1)[-1]
    extension = leaf.rsplit(".", 1)[-1].casefold() if "." in leaf else "jpg"
    if extension not in {"jpg", "jpeg", "png", "webp", "gif"}:
        extension = "jpg"
    query = urllib.parse.parse_qs(parts.query, keep_blank_values=True)
    query["format"] = [extension]
    query["name"] = ["4096x4096"]
    return urllib.parse.urlunsplit(parts._replace(
        query=urllib.parse.urlencode(query, doseq=True)))


def extract_tweet_media(t: dict) -> list[dict]:
    """Normalize tweet media into {type, url, alt_text, full_resolution_url}.

    The live API puts these records at ``extendedEntities.media``. Accept an
    already-normalized top-level ``media`` list too so enriched API results can
    pass through Store without losing their first-class shape.
    """
    if not isinstance(t, dict):
        return []
    candidates = t.get("media")
    if not isinstance(candidates, list) or not candidates:
        extended = t.get("extendedEntities") or t.get("extended_entities") or {}
        candidates = extended.get("media") if isinstance(extended, dict) else None
    if not isinstance(candidates, list):
        entities = t.get("entities") or {}
        candidates = entities.get("media") if isinstance(entities, dict) else []

    out, seen = [], set()
    for item in candidates or []:
        if not isinstance(item, dict):
            continue
        media_type = str(item.get("type") or "unknown")
        url = (item.get("url") if "media_url_https" not in item
               and "media_url" not in item else None)
        url = item.get("media_url_https") or item.get("media_url") or url
        if not url:
            continue
        identity = (media_type, str(url))
        if identity in seen:
            continue
        seen.add(identity)
        full = item.get("full_resolution_url")
        if not full:
            full = (full_resolution_media_url(str(url))
                    if media_type == "photo" else str(url))
        out.append({
            "type": media_type,
            "url": str(url),
            "alt_text": item.get("ext_alt_text") or item.get("alt_text") or None,
            "full_resolution_url": full,
        })
    return out


def carries_media(t: dict) -> bool:
    """Does this record say ANYTHING about media? Live records always carry
    `extendedEntities` ({} when there is no media; 2,935 of 2,935 on the
    production store), so its presence is an assertion and an empty value
    means "no media". A record with none of the media keys says nothing.

    A top-level `media` list counts only when non-empty: twitterapi's
    enrich_tweet_media attaches `media` to EVERY record, so `media: []`
    without a source key is derived from nothing, not an assertion."""
    if not isinstance(t, dict):
        return False
    if "extendedEntities" in t or "extended_entities" in t:
        return True
    if isinstance(t.get("media"), list) and t["media"]:
        return True
    entities = t.get("entities")
    return isinstance(entities, dict) and "media" in entities


def normalize_tweet(t: dict) -> dict:
    """One shape for tweets. Note `viewCount`/`url` appear on advanced_search
    results but not on last_tweets — never assume a field exists.

    ABSENT means UNKNOWN: every field whose source key is missing is None, so
    the upsert keeps the stored value instead of overwriting it. Coercing
    absence to 0 made a thin refetch erase paid data (views 12345 -> 0, one
    photo -> []). A PRESENT value — including 0, false, "" — is authoritative.
    """
    if not isinstance(t, dict):
        return {}
    a = t.get("author") if isinstance(t.get("author"), dict) else None

    def count(key):
        value = t.get(key)
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def text(key):
        if key not in t:
            return None
        return "" if t[key] is None else str(t[key])

    def flag(*keys):
        if not any(k in t for k in keys):
            return None
        return int(any(bool(t.get(k)) for k in keys))

    author_id = a.get("id") if a else None
    author_name = (a.get("userName") or a.get("screen_name")) if a else None
    return {
        "id": str(t.get("id") or ""),
        "author_id": str(author_id) if author_id not in (None, "") else None,
        "author_name": author_name or None,
        "created_ts": parse_ts(t.get("createdAt") or ""),
        "text": text("text"),
        "lang": text("lang"),
        "likes": count("likeCount"),
        "retweets": count("retweetCount"),
        "replies": count("replyCount"),
        "quotes": count("quoteCount"),
        "views": count("viewCount"),
        "is_reply": flag("isReply"),
        "is_quote": flag("quoted_tweet"),
        # The API's isRetweet flag is documented as unreliable; corroborate it
        # with the presence of retweeted_tweet.
        "is_retweet": flag("isRetweet", "retweeted_tweet"),
        "conversation_id": text("conversationId"),
        "in_reply_to": text("inReplyToId"),
        "media": extract_tweet_media(t) if carries_media(t) else None,
    }


class _FollowerIds(list):
    """A follower-id list that carries its crawl's completeness.

    Subclassing list keeps the documented Cohort.from_ids(...) pattern
    working unchanged while letting an honest caller check `.complete` and
    `.reason` — the bit an interrupted crawl leaves in its final cached page
    and this reader previously never consulted."""
    complete = True
    reason = None


class Store:
    def __init__(self, path=DEFAULT_DB):
        self.path = os.path.abspath(os.path.expanduser(path))
        path = self.path
        # A bare filename ("store.db") has no dirname; makedirs("") raises
        # FileNotFoundError. Only create a parent when there is one.
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self.db = sqlite3.connect(path, timeout=30, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.executescript(SCHEMA)
        self._migrate()
        self.db.execute("PRAGMA journal_mode=WAL")   # concurrent workers
        self.db.commit()
        # A shared connection is NOT safe for concurrent writes — measured 92/750
        # rows surviving 15 threads without this. Every write holds the lock.
        # (WAL lets reads proceed uncontended alongside.)
        self._wlock = threading.Lock()

    def _migrate(self):
        """CREATE TABLE IF NOT EXISTS won't alter a table that predates a
        schema change, so evolve in place. Incompatible old cohort tables are
        RENAMED to cohorts_old_<n>/cohort_meta_old_<n>, never dropped: cohort
        version history is user-controlled paid state (Cohort Drift diffs it),
        so it must stay recoverable after a schema change."""
        cols = [r["name"] for r in self.db.execute("PRAGMA table_info(cohorts)")]
        if cols and "member_key" not in cols:
            existing = {r["name"] for r in self.db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            n = 1
            while (f"cohorts_old_{n}" in existing
                   or f"cohort_meta_old_{n}" in existing):
                n += 1
            self.db.execute(f"ALTER TABLE cohorts RENAME TO cohorts_old_{n}")
            if "cohort_meta" in existing:
                self.db.execute(
                    f"ALTER TABLE cohort_meta RENAME TO cohort_meta_old_{n}")
            self.db.executescript(SCHEMA)
            self.db.commit()

        tweet_cols = {r["name"]: r for r in self.db.execute(
            "PRAGMA table_info(tweets)")}
        if tweet_cols and "media" not in tweet_cols:
            self.db.execute("ALTER TABLE tweets ADD COLUMN media TEXT")
            # Existing paid corpora already contain media in raw JSON. Recover
            # it locally during migration instead of requiring a paid re-crawl.
            # raw and the columns were written together by the old REPLACE, so
            # raw is exactly the record the row came from: a raw with media
            # keys gets its list (possibly []), one without stays NULL.
            updates = []
            for row in self.db.execute("SELECT id, raw FROM tweets"):
                try:
                    raw = json.loads(row["raw"] or "{}")
                except (TypeError, ValueError, json.JSONDecodeError):
                    raw = {}
                if carries_media(raw):
                    updates.append((json.dumps(extract_tweet_media(raw)),
                                    row["id"]))
            if updates:
                self.db.executemany(
                    "UPDATE tweets SET media=? WHERE id=?", updates)
            self.db.commit()
        elif tweet_cols and tweet_cols["media"]["notnull"]:
            self._relax_tweet_media()
        self._rekey_pages()

    def _rekey_pages(self):
        """Stores written before handle keys were case-folded hold paid pages
        under mixed-case keys (measured: 210 followings pages for one handle)
        that the folded key can no longer find. Re-key them in place; when two
        casings of one request both exist, keep the newer page."""
        rows = list(self.db.execute(
            "SELECT key, path, params FROM pages WHERE params LIKE "
            "'%\"userName\"%' OR params LIKE '%\"username\"%' "
            "ORDER BY fetched_at"))
        moves = []
        for row in rows:
            try:
                params = json.loads(row["params"] or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            new = self._key(row["path"], params)
            if new != row["key"]:
                moves.append((row["key"], new))
        if not moves:
            return
        # Oldest first: a later move to the same key replaces the earlier one.
        for old, new in moves:
            self.db.execute("DELETE FROM pages WHERE key=? AND fetched_at <= "
                            "(SELECT fetched_at FROM pages WHERE key=?)",
                            (new, old))
            self.db.execute("UPDATE OR IGNORE pages SET key=? WHERE key=?",
                            (new, old))
            self.db.execute("DELETE FROM pages WHERE key=?", (old,))
        self.db.commit()

    def _relax_tweet_media(self):
        """Stores created before 2026-10-23 declare media NOT NULL DEFAULT '[]',
        which cannot hold "unknown". SQLite cannot drop a NOT NULL constraint
        in place, so rebuild the table: copy every row by column NAME (the
        production table has media appended after seen_at by an older ALTER),
        swap, and recreate the indexes — all in one IMMEDIATE transaction, so
        a crash leaves the old table intact and a second process opening the
        same file concurrently waits, re-checks, and finds nothing to do.

        Existing values are copied unchanged. A stored '[]' cannot be told
        apart from "unknown" after the fact; measured on the production store,
        every one of its 2,935 raw records carries extendedEntities, so its
        '[]' values are genuine "no media" and nothing needs re-deriving."""
        self.db.commit()
        self.db.execute("BEGIN IMMEDIATE")
        try:
            info = list(self.db.execute("PRAGMA table_info(tweets)"))
            if not any(r["name"] == "media" and r["notnull"] for r in info):
                self.db.rollback()          # another process got here first
                return
            cols = ", ".join(r["name"] for r in info)
            self.db.execute("DROP TABLE IF EXISTS tweets_migrating")
            self.db.execute(TWEETS_DDL.format(name="tweets_migrating"))
            self.db.execute(f"INSERT INTO tweets_migrating ({cols}) "
                            f"SELECT {cols} FROM tweets")
            self.db.execute("DROP TABLE tweets")
            self.db.execute("ALTER TABLE tweets_migrating RENAME TO tweets")
            for statement in TWEETS_INDEXES.strip().split(";"):
                if statement.strip():
                    self.db.execute(statement)
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    @contextlib.contextmanager
    def _write_txn(self):
        """One write transaction that is atomic across THREADS (_wlock) and
        across CONNECTIONS/processes (BEGIN IMMEDIATE takes SQLite's write
        lock before the first read, so a read-then-write cannot interleave
        with another process's; the 30 s connect timeout is the wait).
        _wlock alone let two processes both allocate cohort version N."""
        with self._wlock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                yield self.db
            except BaseException:
                self.db.rollback()
                raise
            self.db.commit()

    # -- fetch cache ------------------------------------------------------
    @staticmethod
    def _key(path, params):
        # Stringify values so cursor=1 (int) and cursor="1" (str) map to one
        # key — otherwise an equivalent request is a false cache miss and pays
        # to refetch. urlencode stringifies anyway, so this loses nothing.
        # X handles are case-insensitive, and follower_ids_for() matches them
        # that way. A case-sensitive key let overlap("Stripe") count a cached
        # "stripe" crawl as free and then pay to re-buy it.
        norm = {str(k): (str(v).lstrip("@").casefold()
                         if k in HANDLE_PARAMS else str(v))
                for k, v in (params or {}).items()}
        return path + "?" + json.dumps(norm, sort_keys=True)

    def get_page(self, path, params, max_age=None):
        """Cached response, or None. max_age in seconds; None = never expires
        (follower graphs are expensive; don't re-buy them casually)."""
        # READS need the lock too. check_same_thread=False permits cross-thread
        # use; it does NOT make one connection safe for CONCURRENT use, and WAL
        # only helps multiple connections. An 8-worker crawl raised
        # sqlite3.InterfaceError here — a serialised read is far cheaper than
        # network latency, so guard rather than pool.
        with self._wlock:
            r = self.db.execute("SELECT body, fetched_at FROM pages WHERE key=?",
                                (self._key(path, params),)).fetchone()
        if not r:
            return None
        if max_age is not None and time.time() - r["fetched_at"] > max_age:
            return None
        return json.loads(r["body"])

    def page_age(self, path, params):
        """Seconds since this page was fetched, or None if absent.

        Cached graph data has no expiry by default, so a caller cannot tell a
        fresh crawl from a year-old snapshot without asking."""
        with self._wlock:               # see get_page: reads need it too
            r = self.db.execute("SELECT fetched_at FROM pages WHERE key=?",
                                (self._key(path, params),)).fetchone()
        return None if not r else time.time() - r["fetched_at"]

    def put_page(self, path, params, body, credits=0.0):
        with self._wlock:
            self.db.execute(
                "INSERT OR REPLACE INTO pages VALUES (?,?,?,?,?,?)",
                (self._key(path, params), path, json.dumps(params or {}, sort_keys=True),
                 json.dumps(body), time.time(), credits))
            self.db.commit()

    def follower_ids_for(self, user_name=None, *, user_id=None):
        """Return follower IDs from the LATEST cached crawl of this account.

        This is a local read: it never constructs a Client or calls the API.

        A crawl starts at a cursorless page; its continuation pages carry
        cursors. Because page rows are keyed by (path, params), a re-crawl
        REPLACES the cursorless start page but its continuation cursors
        differ, so the old crawl's cursor pages survive alongside the new
        ones. An earlier version of this method merged every cached page and
        therefore returned a union of crawl generations — measured on a real
        store, an account re-crawled 3 hours apart kept 39 pages spanning
        both crawls, and anyone who unfollowed between them was still
        reported as a follower. Only pages fetched at or after the newest
        crawl start are read now; a re-crawl replaces rather than blends.
        Staleness of that crawl is a separate question — see page_age().
        """
        if not (user_name or user_id):
            raise ValueError("pass user_name or user_id")
        wanted_id = str(user_id) if user_id is not None else None
        wanted_name = ((user_name or "").lstrip("@").casefold()
                       if user_name is not None else None)
        pages = []
        # Materialise under the lock: an unlocked cursor on the shared
        # connection, iterated while other threads used it, silently returned
        # 0 ids (16 threads) — and then a false PARTIAL warning.
        with self._wlock:
            rows = self.db.execute(
                "SELECT params, body, fetched_at FROM pages WHERE path=? "
                "ORDER BY fetched_at, key",
                ("/twitter/user/followers_ids",)).fetchall()
        for row in rows:
            try:
                params = json.loads(row["params"] or "{}")
                body = json.loads(row["body"] or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            matches = (str(params.get("userId")) == wanted_id
                       if wanted_id is not None else
                       str(params.get("userName") or "").lstrip("@").casefold()
                       == wanted_name)
            if not matches or not isinstance(body.get("ids"), list):
                continue
            pages.append((not params.get("cursor"), body["ids"],
                          bool(body.get("has_next_page"))))
        # Newest crawl = everything from the last cursorless start onward.
        # No start marker cached at all -> fall back to every page rather
        # than invent an empty answer for data the caller paid for — but that
        # union spans unknown generations, so it is never provably complete.
        starts = [i for i, (is_start, _, _) in enumerate(pages) if is_start]
        if not pages:
            # Nothing bought yet: unknown, not an empty follower set. No
            # warning — there is no partial data to warn about.
            complete = False
            reason = "no follower_ids pages are cached for this account"
        elif starts:
            pages = pages[starts[-1]:]
            # The crawl's own completeness bit sits in the cached body: the
            # FINAL page of an interrupted crawl (ceiling hit, SIGKILL, API
            # failure) still asserts has_next_page. Reading only body["ids"]
            # presented a 3-of-40-page crawl as the definite follower set.
            complete = not pages[-1][2]
            reason = (None if complete else
                      "the newest cached crawl was interrupted — its final "
                      "page still asserts has_next_page; re-crawl to complete")
        else:
            complete = False
            reason = ("no crawl-start page is cached, so generations cannot "
                      "be distinguished; the union below is not provably "
                      "a single snapshot")
        seen, out = set(), []
        for _, ids, _ in pages:
            for account_id in ids:
                account_id = str(account_id)
                if account_id not in seen:
                    seen.add(account_id)
                    out.append(account_id)
        if not complete and pages:
            print(f"[store] PARTIAL: follower_ids_for returned {len(out):,} "
                  f"ids but {reason}.", file=sys.stderr)
        result = _FollowerIds(out)
        result.complete = complete
        result.reason = reason
        return result

    # -- records ----------------------------------------------------------
    def put_accounts(self, users):
        rows = []
        for u in users:
            n = normalize_account(u)
            # Only records with a real id are persisted: the PK is n["id"], so
            # handle-only rows would all share id="" and INSERT OR REPLACE
            # would collapse them into one row (and clobber each other). An
            # id-less record is enrichment, not authoritative — drop it.
            if n.get("id"):
                rows.append((n["id"], n["user_name"], n["name"], n["description"],
                             n["followers"], n["following"], n["statuses"],
                             n["is_blue"], n["is_verified"], n["created_at"],
                             n["location"], json.dumps(u), time.time()))
        if rows:
            with self._wlock:
                # Never let a THIN record overwrite a richer one. Tweet-embedded
                # authors carry fewer fields than a paid profile crawl, so a
                # blind REPLACE silently destroys data the caller already paid
                # for. Keep the better value per column instead.
                self.db.executemany(
                    """INSERT INTO accounts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(id) DO UPDATE SET
                         user_name   = COALESCE(NULLIF(excluded.user_name,''), user_name),
                         name        = COALESCE(NULLIF(excluded.name,''), name),
                         -- COALESCE, not MAX / NULLIF: an incoming value is
                         -- authoritative when present (accounts lose followers,
                         -- let subscriptions lapse, clear their bio), and a
                         -- thin record carries NULL rather than 0 / false / "",
                         -- so it cannot clobber a known value. The normaliser
                         -- owns the "hollow empty bio" case and emits NULL.
                         description = COALESCE(excluded.description, description),
                         followers   = COALESCE(excluded.followers, followers),
                         following   = COALESCE(excluded.following, following),
                         statuses    = COALESCE(excluded.statuses, statuses),
                         is_blue     = COALESCE(excluded.is_blue, is_blue),
                         is_verified = COALESCE(excluded.is_verified, is_verified),
                         created_at  = COALESCE(NULLIF(excluded.created_at,''), created_at),
                         location    = COALESCE(NULLIF(excluded.location,''), location),
                         raw         = CASE WHEN LENGTH(excluded.raw) > LENGTH(raw)
                                            THEN excluded.raw ELSE raw END,
                         seen_at     = excluded.seen_at""", rows)
                self.db.commit()
        return len(rows)

    def put_tweets(self, tweets):
        """Upsert tweets. A PRESENT field wins; an ABSENT one (NULL from the
        normaliser) keeps the stored value. INSERT OR REPLACE used to rewrite
        the whole row, so a thin refetch erased views, media and flags.

        `raw` follows the same rule at its top level: the stored record is
        merged with the incoming one, incoming keys winning. Fields that live
        only in raw (bookmarkCount, inReplyToUserId) survive a thin refetch,
        and raw keeps agreeing with the columns derived from it."""
        parsed, authors = [], []
        for t in tweets:
            n = normalize_tweet(t)
            if not n.get("id"):
                continue
            parsed.append((n, t))
            if isinstance(t.get("author"), dict):
                authors.append(t["author"])
        if parsed:
            cols = TWEET_COLUMNS + ("raw", "seen_at")
            keep = ",\n".join(f"{c} = COALESCE(excluded.{c}, {c})"
                              for c in TWEET_COLUMNS if c != "id")
            sql = (f"INSERT INTO tweets ({', '.join(cols)}) "
                   f"VALUES ({','.join('?' * len(cols))}) "
                   f"ON CONFLICT(id) DO UPDATE SET {keep}, "
                   f"raw = excluded.raw, seen_at = excluded.seen_at")
            with self._write_txn() as db:
                ids = sorted({n["id"] for n, _ in parsed})
                stored = {}
                for i in range(0, len(ids), 500):
                    chunk = ids[i:i + 500]
                    for r in db.execute(
                            "SELECT id, raw FROM tweets WHERE id IN "
                            f"({','.join('?' * len(chunk))})", chunk):
                        try:
                            old = json.loads(r["raw"] or "{}")
                        except (TypeError, ValueError, json.JSONDecodeError):
                            old = {}
                        stored[r["id"]] = old if isinstance(old, dict) else {}
                rows = []
                for n, t in parsed:
                    merged = {**stored.get(n["id"], {}), **t}
                    stored[n["id"]] = merged        # duplicates within a batch
                    media = None if n["media"] is None else json.dumps(n["media"])
                    rows.append(tuple(media if c == "media" else n[c]
                                      for c in TWEET_COLUMNS)
                                + (json.dumps(merged), time.time()))
                db.executemany(sql, rows)
        if authors:
            # A tweet-embedded author object reports description="" even for
            # accounts that plainly have a bio. Measured against user_info:
            # 21 of 22 embedded objects were hollow this way, while the same
            # field from /followers was genuine 10 of 10. Every OTHER embedded
            # field (followers, statuses, location, verification) agreed with
            # the authoritative profile, so only this one is dropped. Removing
            # the key makes it read as UNKNOWN rather than as a false "no bio";
            # provenance is known here and nowhere downstream, so it has to
            # happen at this call site.
            authors = [{k: v for k, v in a.items() if k != "description"}
                       if isinstance(a, dict) else a for a in authors]
            self.put_accounts(authors)      # takes _wlock itself; must be OUTSIDE
        return len(parsed)

    # -- cohorts ----------------------------------------------------------
    @staticmethod
    def member_key(account_id, user_name):
        """Stable identity for a cohort member. account_id when known, else the
        handle — so two handle-only members don't collide on an empty id and
        silently overwrite each other (that lost members before this fix)."""
        return (str(account_id) if account_id
                else "@" + (user_name or "").lower().lstrip("@"))

    def save_cohort(self, name, members, spec=None, version=None):
        """members: iterable of (account_id, user_name, weight, provenance).
        Versioned so Cohort Drift can diff two resolutions of the same name."""
        materialized = list(members)
        # Version read + writes in ONE write transaction, else two savers race
        # on MAX+1 — across processes too, which _wlock alone cannot see.
        with self._write_txn():
            if version is None:
                r = self.db.execute(
                    "SELECT COALESCE(MAX(version),0)+1 v FROM cohort_meta WHERE name=?",
                    (name,)).fetchone()
                version = r["v"]
            rows, keys = [], set()
            for aid, un, w, prov in materialized:
                mk = self.member_key(aid, un)
                if not mk or mk == "@" or mk in keys:
                    continue
                keys.add(mk)
                # Only an ABSENT weight defaults to 1.0. `w or 1.0` turned an
                # authority weight of 0 into 1.0 and inverted the ranking.
                rows.append((name, version, mk, str(aid or ""), un or "",
                             1.0 if w is None else float(w), prov or "",
                             time.time()))
            # Clear the version before writing it. INSERT OR REPLACE only
            # touches supplied keys, so replacing a version with FEWER members
            # left the omitted ones behind as ghosts — metadata said 1 member,
            # the table held 2, and every later load/diff of that version read
            # the ghost as real. No-op for auto-incremented (new) versions;
            # the delete+insert+meta share one transaction, committed below.
            self.db.execute("DELETE FROM cohorts WHERE name=? AND version=?",
                            (name, version))
            self.db.executemany(
                "INSERT OR REPLACE INTO cohorts VALUES (?,?,?,?,?,?,?,?)", rows)
            self.db.execute("INSERT OR REPLACE INTO cohort_meta VALUES (?,?,?,?,?)",
                            (name, version, json.dumps(spec or {}), time.time(), len(rows)))
        return version

    def load_cohort(self, name, version=None):
        """Members of a saved cohort version (latest when version is None).

        Raises KeyError when that name/version was never saved. Returning []
        made a typo'd name indistinguishable from a saved empty cohort, so an
        intersect with it silently reported zero overlap. A saved empty
        cohort (cohort_meta row, size 0) still loads as []."""
        with self._wlock:               # see get_page: reads need it too
            if version is None:
                r = self.db.execute(
                    "SELECT MAX(version) v FROM cohort_meta WHERE name=?",
                    (name,)).fetchone()
                version = r["v"]
                if version is None:
                    raise KeyError(f"no saved cohort named {name!r}")
            elif self.db.execute(
                    "SELECT 1 FROM cohort_meta WHERE name=? AND version=?",
                    (name, version)).fetchone() is None:
                raise KeyError(f"cohort {name!r} has no saved version {version}")
            return [dict(r) for r in self.db.execute(
                "SELECT * FROM cohorts WHERE name=? AND version=? ORDER BY weight DESC",
                (name, version))]

    def list_cohorts(self):
        with self._wlock:
            return [dict(r) for r in self.db.execute(
                "SELECT name, version, size, created_at FROM cohort_meta "
                "ORDER BY name, version")]

    def cohort_versions(self, name):
        with self._wlock:
            return [r["version"] for r in self.db.execute(
                "SELECT version FROM cohort_meta WHERE name=? ORDER BY version",
                (name,))]

    def cohort_spec(self, name, version):
        """The spec dict saved with one cohort version ({} when none)."""
        with self._wlock:
            row = self.db.execute(
                "SELECT spec FROM cohort_meta WHERE name=? AND version=?",
                (name, version)).fetchone()
        return json.loads(row["spec"] or "{}") if row else {}

    def delete_cohort(self, name, version=None):
        """Delete one saved version, or all versions when version is omitted.

        Returns the number of metadata versions removed. Member and metadata
        rows share one transaction so deletion cannot leave an orphaned half.
        """
        where = "name=?" + (" AND version=?" if version is not None else "")
        params = (name, version) if version is not None else (name,)
        with self._wlock:
            count = self.db.execute(
                f"SELECT COUNT(*) c FROM cohort_meta WHERE {where}",
                params).fetchone()["c"]
            try:
                self.db.execute("BEGIN")
                self.db.execute(f"DELETE FROM cohorts WHERE {where}", params)
                self.db.execute(f"DELETE FROM cohort_meta WHERE {where}", params)
                self.db.commit()
            except Exception:
                self.db.rollback()
                raise
        return count

    # -- queries ----------------------------------------------------------
    def tweets_for(self, account_ids=None, user_names=None, since=None, until=None):
        """Stored tweets, newest first. A filter of None is "no filter"; an
        EMPTY list is a filter that matches nothing (an empty cohort has no
        tweets — `if account_ids:` used to return the whole corpus).

        Fact columns are None when the source never carried them, and
        `media` is None when no record of the tweet carried media keys —
        unknown, not zero / no media."""
        q = "SELECT * FROM tweets WHERE 1=1"
        p = []
        if account_ids is not None:
            account_ids = list(account_ids)
            q += f" AND author_id IN ({','.join('?' * len(account_ids))})"
            p += [str(a) for a in account_ids]
        if user_names is not None:
            user_names = list(user_names)
            q += f" AND lower(author_name) IN ({','.join('?' * len(user_names))})"
            p += [u.lower().lstrip("@") for u in user_names]
        if since is not None:
            q += " AND created_ts >= ?"; p.append(int(since))
        if until is not None:
            q += " AND created_ts < ?"; p.append(int(until))
        with self._wlock:               # see get_page: reads need it too
            records = self.db.execute(q + " ORDER BY created_ts DESC", p).fetchall()
        rows = []
        for record in records:
            row = dict(record)
            try:
                row["media"] = (None if row.get("media") is None
                                else json.loads(row["media"]))
            except (TypeError, ValueError, json.JSONDecodeError):
                row["media"] = None
            rows.append(row)
        return rows

    def close(self):
        """Close the sqlite connection. Optional in scripts (the OS reclaims it
        at exit) but required in tests and long-lived processes, which otherwise
        leak connections and emit ResourceWarning."""
        try:
            self.db.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    def stats(self):
        n = lambda t: self.db.execute(f"SELECT COUNT(*) c FROM {t}").fetchone()["c"]
        with self._wlock:               # see get_page: reads need it too
            cached = self.db.execute(
                "SELECT COALESCE(SUM(credits),0) s FROM pages").fetchone()["s"]
            # "cohorts" counts SAVED VERSIONS (cohort_meta). The rows that make
            # a store big live in the members table: a real store reported
            # cohorts=2 while holding 421,578 member rows, so growth was
            # invisible to the only diagnostic a user has. Report both, plus
            # the file size, which is what "is my store getting big" asks.
            db_bytes = (self.db.execute("PRAGMA page_count").fetchone()[0]
                        * self.db.execute("PRAGMA page_size").fetchone()[0])
            counts = {"pages": n("pages"), "accounts": n("accounts"),
                      "tweets": n("tweets"), "cohorts": n("cohort_meta"),
                      "cohort_members": n("cohorts")}
        return {**counts, "db_bytes": db_bytes, "credits_cached": cached,
                "usd_saved_on_rerun": round(cached / 100_000, 4)}


def main(argv=None):
    from axi import run_cli
    return run_cli("store.py", _main, argv)


def _main(argv=None):
    from axi import (ArgumentParser, UsageError, dashboard, emit, error,
                     fast_version, parse_fields, prepare_output)
    if fast_version(argv):
        return 0
    p = ArgumentParser(description="Inspect the local twitterapi.io cache and cohorts")
    p.add_argument("--store", help="SQLite store path")
    p.add_argument("--fields", help="cohort fields: name,version,size,created_at")
    args = p.parse_args(argv)
    if args.fields:
        schema = {field: (field,) for field in ("name", "version", "size", "created_at")}
        try:
            parse_fields(args.fields, schema, "store.py")
        except UsageError as exc:
            emit(error("usage", str(exc), "store.py",
                       "Run `store.py --help` for valid fields")); return 2
    body = dashboard(__file__, "Inspect the local twitterapi.io cache and saved cohorts",
                     store_path=args.store)
    if args.fields:
        body = prepare_output(
            body, command="store.py", fields=args.fields,
            collections={"cohorts": {field: (field,) for field in
                                      ("name", "version", "size", "created_at")}},
            context=f"in local store {body['store']}",
            help_lines=body.pop("help", ()))
    emit(body)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
