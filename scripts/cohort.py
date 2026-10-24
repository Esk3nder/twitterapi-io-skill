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
import urllib.error

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from twitterapi import (APIError, Client, CostLimitExceeded,
                        IncompleteDataError, credits_to_usd)  # noqa: E402
from store import Store  # noqa: E402

AUTHORITY_ESTIMATED_FOLLOWINGS = 2_000
# A request that failed for this account or this resource. Programming errors
# are deliberately absent: they must propagate, not become "partial".
_REQUEST_FAILURES = (APIError, IncompleteDataError, urllib.error.URLError,
                     TimeoutError)


class Cohort:
    """A set of accounts, each with a weight and provenance string.

    Backed by dict {account_id or user_name -> member}. Members carry whatever
    identity we have; some resolvers yield only ids (cheap), some yield full
    profiles. `hydrate()` fills profiles on demand.

    ``complete`` is False when the member set is known to be a sample of the
    population it names (a resolver stopped at its limit, an engager leg
    failed, a filter could not judge some members). ``incomplete_reasons``
    says why. Set operations, save() and load() carry the flag forward.
    """

    def __init__(self, members=None, client=None, store=None, label=""):
        # The Client is created on first paid use, not here: loading, set
        # algebra and drift are local and must work without an API key.
        self._client = client
        self._store_injected = store is not None
        client_store = getattr(client, "store", None)
        self.s = (store if store is not None else
                  client_store if client_store is not None else Store())
        self._align_client_store()
        self.label = label
        # Deep-copy member dicts so set ops can't mutate an operand's members
        # in place (union() shared dict objects before this).
        self.members = {k: dict(v) for k, v in (members or {}).items()}
        self.complete = True
        self.incomplete_reasons = []

    @property
    def c(self):
        if self._client is None:
            self._client = Client(verbose=False)
            self._align_client_store()
        return self._client

    def _align_client_store(self):
        # A caller who explicitly injects a Store into the cohort expects paid
        # hydration pages to use it too. Keep the Client's cache path aligned
        # when it has not already selected another store.
        if (self._store_injected and self._client is not None
                and hasattr(self._client, "store") and self._client.store is None):
            self._client.store = self.s

    def _mark_partial(self, reason):
        self.complete = False
        self.incomplete_reasons.append(reason)
        print(f"[cohort] PARTIAL: {self.label}: {reason}", file=sys.stderr)

    def _inherit_completeness(self, *sources):
        for source in sources:
            if getattr(source, "complete", True) is False:
                self.complete = False
                self.incomplete_reasons.extend(
                    getattr(source, "incomplete_reasons", []) or
                    [f"{source.label} is partial"])

    def _drain(self, stream, what, limit):
        """Consume a PaginationStream; refuse a failed walk, label a sample.

        A later page can fail AFTER earlier records were retrieved and billed;
        the stream then ends with complete=False rather than raising, and only
        callers that check notice — a 500-storm mid-crawl once yielded a
        partial cohort that every set operation treated as the population.
        That stays an IncompleteDataError. A walk stopped by ``limit`` while
        the API had more is a different, legitimate result: the first N
        members. The cohort keeps them and is marked partial, so it is never
        saved or compared as the whole population.
        """
        rows = list(stream)
        if getattr(stream, "complete", True) is False:
            raise IncompleteDataError(
                getattr(stream, "_warning", None) or
                f"{what} ended before completeness was proven; rows are partial")
        if getattr(stream, "truncated_by_limit", False):
            self._mark_partial(
                f"{what} stopped at limit={limit:,} while more existed; the "
                f"cohort is the first {len(rows):,}, not the population. "
                f"Raise limit for the whole set.")
        return rows

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
        for t in co._drain(co.c.paginate("search", query, limit=limit),
                           f"search {query!r}", limit):
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
                for rec in co._drain(
                        co.c.paginate(ep, str(tweet_id), limit=limit),
                        f"{kind} fetch for {tweet_id}", limit):
                    a = extract(rec)
                    co._add(a.get("id"), a.get("userName") or a.get("screen_name"),
                            1.0, f"engager_{kind}")
            except (CostLimitExceeded, IncompleteDataError):
                # A leg that failed after paid pages must not be downgraded —
                # an event cohort missing half its retweeters is not "fewer
                # engagers", it is a different population.
                raise
            except _REQUEST_FAILURES as e:
                # An empty result does NOT raise, so a failure here is real
                # (bad id, wrong param, outage). The other legs' engagers are
                # kept, but the cohort is NOT every engager: mark it partial.
                co._mark_partial(
                    f"{kind} fetch for {tweet_id} failed "
                    f"({type(e).__name__}: {e}); {kind} engagers are missing")
        return co

    @classmethod
    def from_community(cls, community_id, role="members", limit=1000, **kw):
        ep = {"members": "community_members", "moderators": "community_moderators"}[role]
        co = cls(label=f"community:{community_id}:{role}", **kw)
        for u in co._drain(co.c.paginate(ep, str(community_id), limit=limit),
                           f"community {community_id} {role}", limit):
            co._add(u.get("id"), u.get("userName") or u.get("screen_name"),
                    1.0, f"community_{role}")
        return co

    @classmethod
    def from_list(cls, list_id, limit=1000, **kw):
        co = cls(label=f"list:{list_id}", **kw)
        for u in co._drain(co.c.paginate("list_members", str(list_id), limit=limit),
                           f"list {list_id}", limit):
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
        out = Cohort(self.members, self._client, self.s, f"({self.label} u {other.label})")
        out._inherit_completeness(self, other)
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
        out = Cohort(client=self._client, store=self.s,
                     label=f"({self.label} n {other.label})")
        out._inherit_completeness(self, other)
        for m in self.members.values():
            hit = self._lookup(oix, m)
            if hit:
                out._add(m["account_id"] or hit["account_id"],
                         m["user_name"] or hit["user_name"],
                         m["weight"] + hit["weight"], "intersection")
        return out

    def minus(self, other):
        oix = self._index(other)
        out = Cohort(client=self._client, store=self.s, label=f"({self.label} - {other.label})")
        out._inherit_completeness(self, other)
        for m in self.members.values():
            if not self._lookup(oix, m):
                out._add(m["account_id"], m["user_name"], m["weight"], m["provenance"])
        return out

    # -- enrichment -------------------------------------------------------
    def _stored_profiles(self):
        """Stored account rows, by id and by lower-case handle."""
        rows = {r["id"]: r for r in (dict(x) for x in self.s.db.execute(
            "SELECT * FROM accounts"))}
        by_name = {r["user_name"].lower(): r for r in rows.values()
                   if r["user_name"]}
        return rows, by_name

    @staticmethod
    def _profiled(row):
        """True when a stored row holds every field filter() reads."""
        return (row["followers"] is not None and row["description"] is not None
                and (row["is_blue"] is not None
                     or row["is_verified"] is not None))

    def _set_account_id(self, key, account_id):
        """Give a handle-keyed member its id and re-key it under that id."""
        member = self.members.pop(key)
        member["account_id"] = str(account_id)
        same = self.members.get(member["account_id"])
        if same is None:
            self.members[member["account_id"]] = member
            return
        same["weight"] += member["weight"]          # one account, two entries
        if not same["user_name"]:
            same["user_name"] = member["user_name"]

    def hydrate(self, max_usd=1.0):
        """Fill full profiles for members we only have ids/handles for.

        A member whose stored profile already holds every field filter()
        reads is not bought again. Id members go through the batch endpoint
        (100 ids/call); handle-only members through user_info (one call each,
        the same 18-credit profile price). Returns the profiles retrieved
        (bought, or served from the page cache) and fills member handles and
        ids. Members the API returned no profile for are listed in
        ``self.unhydrated`` with a warning — never silently left behind."""
        rows, by_name = self._stored_profiles()
        need_ids, need_names, known = [], [], []
        for key, m in self.members.items():
            prof = (rows.get(m["account_id"]) if m["account_id"]
                    else by_name.get(m["user_name"].lower()))
            if prof and self._profiled(prof):
                if not m["user_name"] and prof["user_name"]:
                    m["user_name"] = prof["user_name"]
                if not m["account_id"]:
                    known.append((key, prof["id"]))
                continue
            if m["account_id"]:
                need_ids.append(m["account_id"])
            elif m["user_name"]:
                need_names.append((key, m["user_name"]))
        for key, account_id in known:       # re-keys members: after the walk
            self._set_account_id(key, account_id)
        profiles, unhydrated = [], []
        self.unhydrated = unhydrated
        if not (need_ids or need_names):
            return profiles     # all profiled locally: no request, no Client
        # Paid pages are persisted by the client into its own store. Write
        # here only when this cohort reads a different store, and never for a
        # page served from cache (an old snapshot must not overwrite newer rows).
        client_persists = getattr(self.c, "store", None) is self.s
        prior = self.c.max_usd
        method_ceiling = (self.c.spent_usd + max_usd
                          if max_usd is not None else None)
        ceilings = [v for v in (prior, method_ceiling) if v is not None]
        self.c.max_usd = min(ceilings) if ceilings else None
        try:
            for i in range(0, len(need_ids), 100):
                chunk = need_ids[i:i + 100]
                stream = self.c.paginate(
                    "batch_users", ",".join(chunk), page_size=len(chunk))
                users = [u for u in stream if isinstance(u, dict)]
                profiles.extend(users)
                names = {
                    str(u.get("id") or u.get("user_id") or ""):
                    (u.get("userName") or u.get("screen_name") or "").lstrip("@")
                    for u in users
                }
                for account_id in chunk:
                    if account_id not in names:
                        unhydrated.append(account_id)
                    elif names[account_id] and account_id in self.members:
                        self.members[account_id]["user_name"] = names[account_id]
                if (not client_persists
                        and not getattr(stream, "cached_pages", 0)):
                    self.s.put_accounts(users)
            for key, handle in need_names:
                try:
                    profile = self.c.user_info(handle)
                except APIError as e:
                    unhydrated.append("@" + handle)
                    print(f"[cohort] WARNING: no profile for @{handle}: "
                          f"{e}", file=sys.stderr)
                    continue
                profiles.append(profile)
                self.s.put_accounts([profile])
                if profile.get("id"):
                    self._set_account_id(key, profile["id"])
        finally:
            self.c.max_usd = prior
        if unhydrated:
            print(f"[cohort] WARNING: hydrate got no profile for "
                  f"{len(unhydrated):,} members (suspended, deleted, or "
                  f"renamed): {unhydrated[:10]}", file=sys.stderr)
        return profiles

    def filter(self, bio_matches=None, min_followers=None, verified=None):
        """Keep members whose STORED profile matches. Call hydrate() first, or
        this only sees members already in the accounts table.

        A member whose profile lacks a field the filter needs is unknown, not
        a mismatch: it is counted (skipped_unhydrated, skipped_unknown_bio,
        skipped_unknown_followers, skipped_unknown_verification), warned
        about, and the returned cohort is marked partial."""
        import re
        pat = re.compile(bio_matches, re.I) if bio_matches else None
        out = Cohort(client=self._client, store=self.s, label=f"filter({self.label})")
        out._inherit_completeness(self)
        skipped = {"unhydrated": 0, "unknown_bio": 0, "unknown_followers": 0,
                   "unknown_verification": 0}
        rows, by_name = self._stored_profiles()
        for m in self.members.values():
            prof = rows.get(m["account_id"]) or by_name.get(m["user_name"].lower())
            if not prof:
                skipped["unhydrated"] += 1
                continue
            if pat:
                text_hit = any(pat.search(prof[field] or "")
                               for field in ("description", "name"))
                if not text_hit and prof["description"] is None:
                    skipped["unknown_bio"] += 1
                    continue
                if not text_hit:
                    continue
            if min_followers:
                if prof["followers"] is None:
                    skipped["unknown_followers"] += 1
                    continue
                if prof["followers"] < min_followers:
                    continue
            if verified is not None:
                verification = [prof["is_blue"], prof["is_verified"]]
                known = [value for value in verification if value is not None]
                if not known:
                    skipped["unknown_verification"] += 1
                    continue
                if bool(any(known)) != verified:
                    continue
            out._add(prof["id"], prof["user_name"], m["weight"], m["provenance"])
        for kind, count in skipped.items():
            setattr(out, f"skipped_{kind}", count)
        if skipped["unhydrated"]:
            out._mark_partial(
                f"filter skipped {skipped['unhydrated']:,} of {len(self):,} "
                f"members absent from stored profiles; call hydrate() before "
                f"filtering profile fields.")
        for kind, field in (("unknown_bio", "bio"),
                            ("unknown_followers", "follower count"),
                            ("unknown_verification", "verification fields")):
            if skipped[kind]:
                out._mark_partial(
                    f"filter skipped {skipped[kind]:,} members whose stored "
                    f"profiles omit the {field}; hydrate() before treating "
                    f"the result as complete.")
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

        ``authority_complete`` is True only when EVERY member's followings
        were walked to the end. Any member left out — no resolvable handle, a
        failed or partial walk, the ceiling — sets it False and adds to
        ``authority_incomplete_reason``. One member's API failure is recorded
        and the walk continues, so the paid frontier is never thrown away.
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
        reasons = []
        crawled = 0
        indeg = {self._key(m["account_id"], m["user_name"]): 0.0 for m in members}
        try:
            # id-only members can't have their followings fetched (the endpoint
            # takes a handle), so they'd be invisible as edge SOURCES and undercount
            # everyone's in-degree. Resolve their handles first through the
            # batch endpoint, which reserves the whole batch's cost up front.
            missing = [m for m in members if not m["user_name"] and m["account_id"]]
            for i in range(0, len(missing), 100):
                chunk = missing[i:i + 100]
                try:
                    users = list(self.c.paginate(
                        "batch_users", ",".join(m["account_id"] for m in chunk)))
                except CostLimitExceeded as e:
                    reasons.append(f"spend ceiling hit while resolving handles "
                                   f"of id-only members: {e}")
                    break
                except _REQUEST_FAILURES as e:
                    reasons.append(f"handle resolution failed for {len(chunk)} "
                                   f"id-only members ({type(e).__name__}: {e})")
                    continue
                names = {str(u.get("id")): u.get("userName") or ""
                         for u in users if isinstance(u, dict)}
                for m in chunk:
                    m["user_name"] = names.get(m["account_id"], "")
            member_ids = {m["account_id"] for m in members if m["account_id"]}
            member_by_name = {m["user_name"].lower(): m for m in members if m["user_name"]}
            unresolved = sum(1 for m in members if not m["user_name"])
            if unresolved:
                reasons.append(f"{unresolved:,} members have no resolvable "
                               f"handle; their follows were not counted")
            total = len(members)
            for position, m in enumerate(members, 1):
                handle = m["user_name"] or None
                if not handle:
                    continue
                if progress:
                    print(f"[authority] {position}/{total} @{handle}: starting | "
                          f"{self.c.spend_report()}", file=sys.stderr, flush=True)
                try:
                    walk = self.c.paginate("followings", handle)
                    for follows in walk:
                        fid = str(follows.get("id") or "")
                        fname = (follows.get("userName") or follows.get("screen_name") or "").lower()
                        if fid in member_ids:
                            tgt = self._key(fid, fname)
                            indeg[tgt] = indeg.get(tgt, 0) + 1
                        elif fname in member_by_name:
                            mm = member_by_name[fname]
                            tgt = self._key(mm["account_id"], mm["user_name"])
                            indeg[tgt] = indeg.get(tgt, 0) + 1
                    if getattr(walk, "complete", True) is False:
                        # The member's out-edges were cut short by an API
                        # failure, not the ceiling. Edges already paid for
                        # stay counted, but this member is NOT fully crawled.
                        reasons.append(getattr(walk, "_warning", None) or
                                       f"followings walk for @{handle} ended partial")
                        continue
                    crawled += 1
                    if progress:
                        print(f"[authority] {position}/{total} @{handle}: complete | "
                              f"{self.c.spend_report()}", file=sys.stderr,
                              flush=True)
                except CostLimitExceeded:
                    # Ceiling hit mid-crawl: later members never contributed
                    # their out-edges, so the ranking is PARTIAL.
                    reasons.append(
                        "spend ceiling hit mid-crawl; later members' follows "
                        "were not counted. Raise max_usd for a complete "
                        "frontier.")
                    break
                except _REQUEST_FAILURES as e:
                    # One member's failure (a 403 on a protected account) must
                    # not discard every other member's paid edges, and a rerun
                    # must not die on the same member forever.
                    reasons.append(f"followings walk for @{handle} failed "
                                   f"({type(e).__name__}: {e}); its follows "
                                   f"were not counted")
                    print(f"[authority] {position}/{total} @{handle}: failed: "
                          f"{e}", file=sys.stderr, flush=True)
                    continue
        finally:
            self.c.max_usd = prior      # never leave the ceiling tightened
        self.authority_crawled = crawled
        self.authority_complete = crawled == len(members)
        self.authority_incomplete_reason = (
            None if self.authority_complete else
            "; ".join(reasons) or
            f"{len(members) - crawled:,} of {len(members):,} members not crawled")
        for k, deg in indeg.items():
            if k in self.members:
                self.members[k]["weight"] = deg
                self.members[k]["provenance"] = "authority_indegree"
        return self

    # -- persistence ------------------------------------------------------
    def save(self, name):
        """Save a new version. Completeness is saved with it: a partial
        cohort stays labelled partial when loaded back."""
        members = [(m["account_id"], m["user_name"], m["weight"], m["provenance"])
                   for m in self.members.values()]
        if not self.complete:
            print(f"[cohort] WARNING: saving '{name}' as a PARTIAL cohort: "
                  f"{'; '.join(self.incomplete_reasons)}", file=sys.stderr)
        v = self.s.save_cohort(name, members, spec={
            "label": self.label, "complete": self.complete,
            "incomplete_reasons": list(self.incomplete_reasons)})
        return v

    @classmethod
    def load(cls, name, version=None, **kw):
        """Load a saved version (latest by default).

        Raises KeyError (from Store.load_cohort) when the name or version was
        never saved: an empty cohort would let intersect() report "overlap 0"
        for a typo."""
        co = cls(label=f"saved:{name}", **kw)
        if version is None:
            versions = co.s.cohort_versions(name)
            version = versions[-1] if versions else None
        rows = co.s.load_cohort(name, version)
        spec = co.s.cohort_spec(name, version)
        for row in rows:
            co._add(row["account_id"], row["user_name"], row["weight"], row["provenance"])
        if spec.get("complete") is False:
            co._mark_partial(
                f"saved version {version} was partial: "
                f"{'; '.join(spec.get('incomplete_reasons') or []) or 'no reason recorded'}")
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
