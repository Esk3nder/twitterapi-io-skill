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

import json
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone

DEFAULT_DB = os.path.expanduser("~/.twitterapi-cache/store.db")
TW_FMT = "%a %b %d %H:%M:%S %z %Y"

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
CREATE TABLE IF NOT EXISTS tweets (
  id TEXT PRIMARY KEY, author_id TEXT, author_name TEXT,
  created_ts INTEGER, text TEXT, lang TEXT,
  likes INTEGER, retweets INTEGER, replies INTEGER, quotes INTEGER,
  views INTEGER, is_reply INTEGER, is_quote INTEGER, is_retweet INTEGER,
  conversation_id TEXT, in_reply_to TEXT, raw TEXT, seen_at REAL);
CREATE INDEX IF NOT EXISTS idx_tweets_author ON tweets(author_id);
CREATE INDEX IF NOT EXISTS idx_tweets_ts ON tweets(created_ts);
CREATE TABLE IF NOT EXISTS cohorts (
  name TEXT, version INTEGER, member_key TEXT, account_id TEXT, user_name TEXT,
  weight REAL DEFAULT 1.0, provenance TEXT, added_at REAL,
  PRIMARY KEY (name, version, member_key));
CREATE TABLE IF NOT EXISTS cohort_meta (
  name TEXT, version INTEGER, spec TEXT, created_at REAL, size INTEGER,
  PRIMARY KEY (name, version));
"""


def parse_ts(s):
    """Twitter's createdAt -> unix seconds. Returns None on anything unexpected
    rather than raising, because one malformed record must not kill a crawl."""
    try:
        return int(datetime.strptime(s, TW_FMT).timestamp())
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
    return {
        "id": str(g("id", "user_id") or ""),
        "user_name": g("userName", "screen_name", "username") or "",
        "name": g("name") or "",
        # Tweet-embedded author objects return description="" and put the real
        # bio at profile_bio.description. Reading only the top-level key makes
        # every tweet-sourced account look bio-less — and then overwrite a
        # richer row. Measured: 143 chars -> 0.
        "description": (g("description", "bio")
                        or ((u.get("profile_bio") or {}).get("description")
                            if isinstance(u.get("profile_bio"), dict) else None)
                        or ""),
        "followers": int(g("followers", "followers_count") or 0),
        "following": int(g("following", "following_count", "friends_count") or 0),
        "statuses": int(g("statusesCount", "statuses_count") or 0),
        "is_blue": int(bool(g("isBlueVerified", "is_blue_verified"))),
        "is_verified": int(bool(g("isVerified", "verified"))),
        "created_at": g("createdAt", "created_at") or "",
        "location": g("location") or "",
    }


def normalize_tweet(t: dict) -> dict:
    """One shape for tweets. Note `viewCount`/`url` appear on advanced_search
    results but not on last_tweets — never assume a field exists."""
    if not isinstance(t, dict):
        return {}
    a = t.get("author") or {}
    return {
        "id": str(t.get("id") or ""),
        "author_id": str(a.get("id") or ""),
        "author_name": a.get("userName") or a.get("screen_name") or "",
        "created_ts": parse_ts(t.get("createdAt") or "") or 0,
        "text": t.get("text") or "",
        "lang": t.get("lang") or "",
        "likes": int(t.get("likeCount") or 0),
        "retweets": int(t.get("retweetCount") or 0),
        "replies": int(t.get("replyCount") or 0),
        "quotes": int(t.get("quoteCount") or 0),
        "views": int(t.get("viewCount") or 0),
        "is_reply": int(bool(t.get("isReply"))),
        "is_quote": int(bool(t.get("quoted_tweet"))),
        # The API's isRetweet flag is documented as unreliable; corroborate it
        # with the presence of retweeted_tweet.
        "is_retweet": int(bool(t.get("isRetweet") or t.get("retweeted_tweet"))),
        "conversation_id": str(t.get("conversationId") or ""),
        "in_reply_to": str(t.get("inReplyToId") or ""),
    }


class Store:
    def __init__(self, path=DEFAULT_DB):
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

    # -- fetch cache ------------------------------------------------------
    @staticmethod
    def _key(path, params):
        # Stringify values so cursor=1 (int) and cursor="1" (str) map to one
        # key — otherwise an equivalent request is a false cache miss and pays
        # to refetch. urlencode stringifies anyway, so this loses nothing.
        norm = {str(k): str(v) for k, v in (params or {}).items()}
        return path + "?" + json.dumps(norm, sort_keys=True)

    def get_page(self, path, params, max_age=None):
        """Cached response, or None. max_age in seconds; None = never expires
        (follower graphs are expensive; don't re-buy them casually)."""
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
        """Return follower IDs already present in paid cached pages.

        This is a local read: it never constructs a Client or calls the API.
        It intentionally ignores cursor and count so callers do not need to
        reproduce an earlier crawl's exact cache keys. When several cached
        crawls exist, IDs are returned once in first-fetched order.
        """
        if not (user_name or user_id):
            raise ValueError("pass user_name or user_id")
        wanted_id = str(user_id) if user_id is not None else None
        wanted_name = ((user_name or "").lstrip("@").casefold()
                       if user_name is not None else None)
        seen, out = set(), []
        rows = self.db.execute(
            "SELECT params, body FROM pages WHERE path=? ORDER BY fetched_at, key",
            ("/twitter/user/followers_ids",))
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
            for account_id in body["ids"]:
                account_id = str(account_id)
                if account_id not in seen:
                    seen.add(account_id)
                    out.append(account_id)
        return out

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
                         description = COALESCE(NULLIF(excluded.description,''), description),
                         followers   = MAX(excluded.followers, followers),
                         following   = MAX(excluded.following, following),
                         statuses    = MAX(excluded.statuses, statuses),
                         is_blue     = MAX(excluded.is_blue, is_blue),
                         is_verified = MAX(excluded.is_verified, is_verified),
                         created_at  = COALESCE(NULLIF(excluded.created_at,''), created_at),
                         location    = COALESCE(NULLIF(excluded.location,''), location),
                         raw         = CASE WHEN LENGTH(excluded.raw) > LENGTH(raw)
                                            THEN excluded.raw ELSE raw END,
                         seen_at     = excluded.seen_at""", rows)
                self.db.commit()
        return len(rows)

    def put_tweets(self, tweets):
        rows, authors = [], []
        for t in tweets:
            n = normalize_tweet(t)
            if not n.get("id"):
                continue
            rows.append((n["id"], n["author_id"], n["author_name"], n["created_ts"],
                         n["text"], n["lang"], n["likes"], n["retweets"], n["replies"],
                         n["quotes"], n["views"], n["is_reply"], n["is_quote"],
                         n["is_retweet"], n["conversation_id"], n["in_reply_to"],
                         json.dumps(t), time.time()))
            if isinstance(t.get("author"), dict):
                authors.append(t["author"])
        if rows:
            with self._wlock:
                self.db.executemany(
                    "INSERT OR REPLACE INTO tweets VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    rows)
                self.db.commit()
        if authors:
            self.put_accounts(authors)      # takes _wlock itself; must be OUTSIDE
        return len(rows)

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
        with self._wlock:                       # version read + writes atomic,
            if version is None:                 # else two savers race on MAX+1
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
                rows.append((name, version, mk, str(aid or ""), un or "",
                             float(w or 1.0), prov or "", time.time()))
            self.db.executemany(
                "INSERT OR REPLACE INTO cohorts VALUES (?,?,?,?,?,?,?,?)", rows)
            self.db.execute("INSERT OR REPLACE INTO cohort_meta VALUES (?,?,?,?,?)",
                            (name, version, json.dumps(spec or {}), time.time(), len(rows)))
            self.db.commit()
        return version

    def load_cohort(self, name, version=None):
        if version is None:
            r = self.db.execute(
                "SELECT MAX(version) v FROM cohort_meta WHERE name=?", (name,)).fetchone()
            version = r["v"]
            if version is None:
                return []
        return [dict(r) for r in self.db.execute(
            "SELECT * FROM cohorts WHERE name=? AND version=? ORDER BY weight DESC",
            (name, version))]

    def list_cohorts(self):
        return [dict(r) for r in self.db.execute(
            "SELECT name, version, size, created_at FROM cohort_meta "
            "ORDER BY name, version")]

    def cohort_versions(self, name):
        return [r["version"] for r in self.db.execute(
            "SELECT version FROM cohort_meta WHERE name=? ORDER BY version", (name,))]

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
        q = "SELECT * FROM tweets WHERE 1=1"
        p = []
        if account_ids:
            q += f" AND author_id IN ({','.join('?' * len(account_ids))})"
            p += [str(a) for a in account_ids]
        if user_names:
            q += f" AND lower(author_name) IN ({','.join('?' * len(user_names))})"
            p += [u.lower().lstrip("@") for u in user_names]
        if since:
            q += " AND created_ts >= ?"; p.append(int(since))
        if until:
            q += " AND created_ts < ?"; p.append(int(until))
        return [dict(r) for r in self.db.execute(q + " ORDER BY created_ts DESC", p)]

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
        cached = self.db.execute("SELECT COALESCE(SUM(credits),0) s FROM pages").fetchone()["s"]
        return {"pages": n("pages"), "accounts": n("accounts"), "tweets": n("tweets"),
                "cohorts": n("cohort_meta"), "credits_cached": cached,
                "usd_saved_on_rerun": round(cached / 100_000, 4)}


if __name__ == "__main__":
    s = Store()
    print(json.dumps(s.stats(), indent=1))
    for c in s.list_cohorts():
        print(f"  {c['name']} v{c['version']}  {c['size']} members")
