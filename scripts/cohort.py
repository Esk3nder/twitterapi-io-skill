#!/usr/bin/env python3
"""Cohorts: named, persisted, composable account sets. Stdlib only.

A cohort is the expensive, reusable asset behind every analytical workflow.
Resolving "who is Polymarket" or "who is the crypto-AI frontier" costs real
money; reading what they posted afterwards is cheap. So cohorts are saved and
versioned — build once, query forever, re-resolve to see who joined or left.

The generating structure of every high-level job:

    SEED  ->  COHORT  ->  CORPUS  ->  SIGNAL

This module is the COHORT stage: resolution, set algebra, and authority
ranking. Corpus and signal live in workflows.py. Nothing here interprets
content — that is the model's job, handed a cheaply-acquired, well-shaped set.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from twitterapi import Client, CostLimitExceeded, credits_to_usd  # noqa: E402
from store import Store  # noqa: E402

AUTHORITY_ESTIMATED_FOLLOWINGS = 2_000


class Cohort:
    """A set of accounts, each with a weight and provenance string.

    Backed by dict {account_id or user_name -> member}. Members carry whatever
    identity we have; some resolvers yield only ids (cheap), some yield full
    profiles. `hydrate()` fills profiles on demand.
    """

    def __init__(self, members=None, client=None, store=None, label=""):
        self.c = client or Client(verbose=False)
        client_store = getattr(self.c, "store", None)
        self.s = (store if store is not None else
                  client_store if client_store is not None else Store())
        # A caller who explicitly injects a Store into the cohort expects paid
        # hydration pages to use it too. Keep the Client's cache path aligned
        # when it has not already selected another store.
        if (store is not None and hasattr(self.c, "store")
                and self.c.store is None):
            self.c.store = store
        self.label = label
        # Deep-copy member dicts so set ops can't mutate an operand's members
        # in place (union() shared dict objects before this).
        self.members = {k: dict(v) for k, v in (members or {}).items()}

    # -- construction -----------------------------------------------------
    @staticmethod
    def _key(account_id, user_name):
        return str(account_id) if account_id else "@" + (user_name or "").lower().lstrip("@")

    def _add(self, account_id=None, user_name=None, weight=1.0, provenance="", raw=None):
        k = self._key(account_id, user_name)
        if not k or k == "@":
            return
        m = self.members.get(k, {"account_id": str(account_id or ""),
                                 "user_name": (user_name or "").lstrip("@"),
                                 "weight": 0.0, "provenance": provenance})
        m["weight"] += weight
        if account_id and not m["account_id"]:
            m["account_id"] = str(account_id)
        if user_name and not m["user_name"]:
            m["user_name"] = user_name.lstrip("@")
        self.members[k] = m

    def __len__(self):
        return len(self.members)

    def __iter__(self):
        return iter(self.members.values())

    def ids(self):
        return [m["account_id"] for m in self.members.values() if m["account_id"]]

    def handles(self):
        return [m["user_name"] for m in self.members.values() if m["user_name"]]

    def top(self, n=None, by="weight"):
        ms = sorted(self.members.values(), key=lambda m: m.get(by, 0), reverse=True)
        return ms[:n] if n else ms

    # -- resolvers (each returns a fresh Cohort) --------------------------
    @classmethod
    def from_account(cls, handle, **kw):
        co = cls(label=f"account:{handle}", **kw)
        d = co.c.user_info(handle)
        co._add(d.get("id"), d.get("userName") or handle, 1.0, "seed_account")
        co.s.put_accounts([d]) if d else None
        return co

    @classmethod
    def from_ids(cls, account_ids, **kw):
        """Build a zero-request cohort from account IDs already in hand."""
        label = kw.pop("label", "provided_ids")
        co = cls(label=label, **kw)
        seen = set()
        for account_id in account_ids:
            account_id = str(account_id or "")
            if not account_id or account_id in seen:
                continue
            seen.add(account_id)
            co._add(account_id=account_id, provenance="provided_id")
        return co

    @classmethod
    def from_search(cls, query, limit=200, **kw):
        """Authors of tweets matching a query — the people talking about X."""
        co = cls(label=f"search:{query}", **kw)
        batch = []
        for t in co.c.paginate("search", query, limit=limit):
            a = t.get("author") or {}
            co._add(a.get("id"), a.get("userName"), 1.0, "search_author")
            batch.append(t)
        if batch:
            co.s.put_tweets(batch)      # persist the corpus, not just the cohort
        return co

    @classmethod
    def from_engagers(cls, tweet_id, limit=500, **kw):
        """Everyone who replied, quoted or retweeted a post — an event cohort."""
        co = cls(label=f"engagers:{tweet_id}", **kw)
        for kind, ep, extract in (
                ("reply", "replies_v2", lambda t: t.get("author") or {}),
                ("quote", "quotes", lambda t: t.get("author") or {}),
                ("retweet", "retweeters", lambda u: u)):
            try:
                for rec in co.c.paginate(ep, str(tweet_id), limit=limit):
                    a = extract(rec)
                    co._add(a.get("id"), a.get("userName") or a.get("screen_name"),
                            1.0, f"engager_{kind}")
            except CostLimitExceeded:
                raise
            except Exception as e:
                # An empty result does NOT raise, so an exception here is a real
                # error (bad id, wrong param). Surface it — do not pass it off
                # as "this post had no engagers".
                print(f"[cohort] WARNING: {kind} fetch for {tweet_id} failed: "
                      f"{type(e).__name__}: {e}", file=sys.stderr)
        return co

    @classmethod
    def from_community(cls, community_id, role="members", limit=1000, **kw):
        ep = {"members": "community_members", "moderators": "community_moderators"}[role]
        co = cls(label=f"community:{community_id}:{role}", **kw)
        for u in co.c.paginate(ep, str(community_id), limit=limit):
            co._add(u.get("id"), u.get("userName") or u.get("screen_name"),
                    1.0, f"community_{role}")
        return co

    @classmethod
    def from_list(cls, list_id, limit=1000, **kw):
        co = cls(label=f"list:{list_id}", **kw)
        for u in co.c.paginate("list_members", str(list_id), limit=limit):
            co._add(u.get("id"), u.get("userName") or u.get("screen_name"),
                    1.0, "list_member")
        return co

    # -- set algebra (composition is why cohorts are persisted) -----------
    # Members may carry an id, a handle, or both, depending on the resolver.
    # Set ops match on ANY shared identity so the same account represented as
    # id-only in one cohort and handle-only in another is treated as one
    # account, not two (keying on a single field silently broke this).
    @staticmethod
    def _index(cohort):
        ix = {}
        for m in cohort.members.values():
            if m["account_id"]:
                ix["id:" + m["account_id"]] = m
            if m["user_name"]:
                ix["nm:" + m["user_name"].lower()] = m
        return ix

    @staticmethod
    def _lookup(ix, m):
        if m["account_id"] and "id:" + m["account_id"] in ix:
            return ix["id:" + m["account_id"]]
        if m["user_name"] and "nm:" + m["user_name"].lower() in ix:
            return ix["nm:" + m["user_name"].lower()]
        return None

    def union(self, other):
        out = Cohort(self.members, self.c, self.s, f"({self.label} u {other.label})")
        ix = self._index(out)
        for m in other:
            hit = self._lookup(ix, m)
            if hit:                         # same account: merge weight
                hit["weight"] += m["weight"]
            else:
                out._add(m["account_id"], m["user_name"], m["weight"], m["provenance"])
                if m["account_id"]:
                    ix["id:" + m["account_id"]] = out.members[out._key(m["account_id"], m["user_name"])]
                if m["user_name"]:
                    ix["nm:" + m["user_name"].lower()] = out.members[out._key(m["account_id"], m["user_name"])]
        return out

    def intersect(self, other):
        oix = self._index(other)
        out = Cohort(client=self.c, store=self.s,
                     label=f"({self.label} n {other.label})")
        for m in self.members.values():
            hit = self._lookup(oix, m)
            if hit:
                out._add(m["account_id"] or hit["account_id"],
                         m["user_name"] or hit["user_name"],
                         m["weight"] + hit["weight"], "intersection")
        return out

    def minus(self, other):
        oix = self._index(other)
        out = Cohort(client=self.c, store=self.s, label=f"({self.label} - {other.label})")
        for m in self.members.values():
            if not self._lookup(oix, m):
                out._add(m["account_id"], m["user_name"], m["weight"], m["provenance"])
        return out

    # -- enrichment -------------------------------------------------------
    def hydrate(self, max_usd=1.0):
        """Fill full profiles for members we only have ids/handles for, via the
        batch endpoint (100 ids/call). Return the purchased profiles and fill
        member handles. Needed before filter() on profile fields."""
        need_ids = [m["account_id"] for m in self.members.values() if m["account_id"]]
        profiles = []
        prior = self.c.max_usd
        method_ceiling = (self.c.spent_usd + max_usd
                          if max_usd is not None else None)
        ceilings = [v for v in (prior, method_ceiling) if v is not None]
        self.c.max_usd = min(ceilings) if ceilings else None
        try:
            for i in range(0, len(need_ids), 100):
                chunk = need_ids[i:i + 100]
                users = list(self.c.paginate(
                    "batch_users", ",".join(chunk), page_size=len(chunk)))
                profiles.extend(users)
                names = {
                    str(u.get("id") or u.get("user_id") or ""):
                    (u.get("userName") or u.get("screen_name") or "").lstrip("@")
                    for u in users if isinstance(u, dict)
                }
                for member in self.members.values():
                    if member["account_id"] in names and names[member["account_id"]]:
                        member["user_name"] = names[member["account_id"]]
                self.s.put_accounts(users)
        finally:
            self.c.max_usd = prior
        return profiles

    def filter(self, bio_matches=None, min_followers=None, verified=None):
        """Keep members whose STORED profile matches. Call hydrate() first, or
        this only sees members already in the accounts table. The returned
        cohort exposes skipped_unhydrated and skipped_unknown_verification."""
        import re
        pat = re.compile(bio_matches, re.I) if bio_matches else None
        out = Cohort(client=self.c, store=self.s, label=f"filter({self.label})")
        skipped_unhydrated = 0
        skipped_unknown_verification = 0
        rows = {r["id"]: r for r in (dict(x) for x in self.s.db.execute(
            "SELECT * FROM accounts"))}
        by_name = {r["user_name"].lower(): r for r in rows.values() if r["user_name"]}
        for m in self.members.values():
            prof = rows.get(m["account_id"]) or by_name.get(m["user_name"].lower())
            if not prof:
                skipped_unhydrated += 1
                continue
            if pat and not (pat.search(prof["description"] or "")
                            or pat.search(prof["name"] or "")):
                continue
            if min_followers and (prof["followers"] or 0) < min_followers:
                continue
            if verified is not None:
                verification = [prof["is_blue"], prof["is_verified"]]
                known = [value for value in verification if value is not None]
                if not known:
                    skipped_unknown_verification += 1
                    continue
                if bool(any(known)) != verified:
                    continue
            out._add(prof["id"], prof["user_name"], m["weight"], m["provenance"])
        out.skipped_unhydrated = skipped_unhydrated
        out.skipped_unknown_verification = skipped_unknown_verification
        if skipped_unhydrated:
            print(f"[cohort] WARNING: filter skipped {skipped_unhydrated:,} "
                  f"of {len(self):,} members absent from stored profiles; "
                  f"call hydrate() before filtering profile fields.",
                  file=sys.stderr)
        if skipped_unknown_verification:
            print(f"[cohort] WARNING: filter skipped "
                  f"{skipped_unknown_verification:,} members whose stored "
                  f"profiles omit verification fields; hydrate() before "
                  f"treating the result as complete.", file=sys.stderr)
        return out

    # -- ranking ----------------------------------------------------------
    def authority(self, max_usd=5.0, confirm=True, progress=False):
        """Rank members by IN-degree within the cohort's own follow graph:
        how many OTHER members follow each member. "Who does this scene itself
        follow" — a computed frontier, not a guessed one.

        Uses OUT-edges (each member's followings) because following-counts are
        bounded (thousands) while follower-counts are unbounded (millions), so
        this reads at most ~(members x avg_following) records. Estimates first
        and refuses past max_usd; a follow-graph crawl is the one primitive
        that can quietly cost real money.
        """
        members = [m for m in self.members.values() if m["account_id"] or m["user_name"]]
        # A 127-member measured crawl cost $0.0188/member, nearly 1.9x the old
        # 1,000-following assumption. Round that observation up to a simple,
        # conservative 2,000-following guard rather than advising a budget
        # known to stop around halfway through the crawl.
        est = credits_to_usd(
            len(members) * AUTHORITY_ESTIMATED_FOLLOWINGS * 1.0)
        if confirm and est > max_usd:
            raise CostLimitExceeded(
                f"authority() over {len(members)} members ~= ${est:,.2f}, "
                f"over ${max_usd:,.2f}. Shrink the cohort (filter/top) or raise "
                f"max_usd. Conservative estimate assumes ~"
                f"{AUTHORITY_ESTIMATED_FOLLOWINGS:,} followings/account.")
        # Actually enforce the ceiling: without this a single member with 500k
        # followings could blow past max_usd, since the estimate is only a gate.
        # EVERYTHING between the tighten and the restore lives in one
        # try/finally — an APIError in the handle-resolution batch below used
        # to escape before the old finally and leave the shared client
        # permanently throttled to spent+max_usd.
        prior = self.c.max_usd
        self.c.max_usd = min(x for x in (prior, self.c.spent_usd + max_usd)
                             if x is not None)
        self.authority_complete = True      # flips false if the ceiling truncates
        crawled = 0
        try:
            # id-only members can't have their followings fetched (the endpoint
            # takes a handle), so they'd be invisible as edge SOURCES and undercount
            # everyone's in-degree. Resolve their handles first — batch is cheap.
            missing = [m for m in members if not m["user_name"] and m["account_id"]]
            for i in range(0, len(missing), 100):
                ids = [m["account_id"] for m in missing[i:i + 100]]
                try:
                    resp = self.c._raw("GET", "/twitter/user/batch_info_by_ids",
                                       {"userIds": ",".join(ids)})
                    self.c._charge("batch_users", len(ids), 100)
                    for u in resp.get("users") or []:
                        for m in missing:
                            if m["account_id"] == str(u.get("id")):
                                m["user_name"] = u.get("userName") or ""
                except CostLimitExceeded:
                    break
            member_ids = {m["account_id"] for m in members if m["account_id"]}
            member_by_name = {m["user_name"].lower(): m for m in members if m["user_name"]}
            indeg = {self._key(m["account_id"], m["user_name"]): 0.0 for m in members}
            total = len(members)
            for position, m in enumerate(members, 1):
                handle = m["user_name"] or None
                if not handle:
                    continue
                if progress:
                    print(f"[authority] {position}/{total} @{handle}: starting | "
                          f"{self.c.spend_report()}", file=sys.stderr, flush=True)
                try:
                    for follows in self.c.paginate("followings", handle):
                        fid = str(follows.get("id") or "")
                        fname = (follows.get("userName") or follows.get("screen_name") or "").lower()
                        if fid in member_ids:
                            tgt = self._key(fid, fname)
                            indeg[tgt] = indeg.get(tgt, 0) + 1
                        elif fname in member_by_name:
                            mm = member_by_name[fname]
                            tgt = self._key(mm["account_id"], mm["user_name"])
                            indeg[tgt] = indeg.get(tgt, 0) + 1
                    crawled += 1
                    if progress:
                        print(f"[authority] {position}/{total} @{handle}: complete | "
                              f"{self.c.spend_report()}", file=sys.stderr,
                              flush=True)
                except CostLimitExceeded:
                    # Ceiling hit mid-crawl: later members never contributed
                    # their out-edges, so the ranking is PARTIAL. Mark it.
                    self.authority_complete = False
                    break
        finally:
            self.c.max_usd = prior      # never leave the ceiling tightened
        self.authority_crawled = crawled
        for k, deg in indeg.items():
            if k in self.members:
                self.members[k]["weight"] = deg
                self.members[k]["provenance"] = "authority_indegree"
        return self

    # -- persistence ------------------------------------------------------
    def save(self, name):
        members = [(m["account_id"], m["user_name"], m["weight"], m["provenance"])
                   for m in self.members.values()]
        v = self.s.save_cohort(name, members, spec={"label": self.label})
        return v

    @classmethod
    def load(cls, name, version=None, **kw):
        co = cls(label=f"saved:{name}", **kw)
        for row in co.s.load_cohort(name, version):
            co._add(row["account_id"], row["user_name"], row["weight"], row["provenance"])
        return co


def main(argv=None):
    from axi import run_cli
    return run_cli("cohort.py", _main, argv)


def _main(argv=None):
    from axi import ArgumentParser, dashboard, emit, fast_version
    if fast_version(argv):
        return 0
    p = ArgumentParser(description="Inspect saved twitterapi.io cohorts")
    p.add_argument("--store", help="SQLite store path")
    args = p.parse_args(argv)
    emit(dashboard(__file__, "Build, combine, persist, and inspect account cohorts",
                   store_path=args.store))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
