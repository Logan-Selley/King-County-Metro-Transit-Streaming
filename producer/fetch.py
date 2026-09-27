"""Conditional GET against the King County S3 endpoints.

The feeds are S3 objects, so they carry ETag and Last-Modified. Polling with
If-None-Match gets a 304 with an empty body when nothing changed. Three
things come out of that, in increasing order of importance:

  1. Bandwidth. Real but boring -- these payloads are small.
  2. Politeness toward an unauthenticated public agency endpoint.
  3. A clean staleness signal. This is the one that matters. The publish
     period measures a tight 20.0s (findings.md §2), so an ETag that has
     not moved in, say, five minutes during peak service is unambiguous
     evidence that something upstream is broken. That belongs in the feed
     health mart. Most people poll blind and cannot tell a stalled feed from a
     quiet one.

So `fetch` is not just "get the bytes"; it is also the thing that knows
whether the feed is alive. Hence FetchResult carrying `status` rather than
just an optional body, and hence FeedState tracking consecutive 304s.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, time
from time import monotonic

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from producer.errors import FeedError
from producer.feeds import USER_AGENT, FeedSpec

log = logging.getLogger("producer.fetch")

# Generous. The trip updates payload is ~600 KB and these are public S3
# objects with no SLA; a slow response is not the same as a dead feed, and
# aborting early just turns a slow tick into a missed one.
TIMEOUT_S = 45

# A 24-hour collection window logs two transient faults: DNS resolution
# failures for s3.amazonaws.com, and S3 closing a pooled keep-alive connection
# mid-response ('Connection aborted.', RemoteDisconnected(...)). Neither is a
# dead feed, and neither should be counted as a missed poll. At a 10s interval
# a single blip costs nothing, so the fetch is retried where the failure
# actually happens, at the transport layer.
#
# The retry lives on the adapter rather than in fetch() for two reasons: it is
# the layer that can see a DNS or connect failure, and a session-level policy
# leaves the contract tests' fake session, which is not a real session, on its
# existing single-attempt semantics.
RETRY_TOTAL = 5
RETRY_BACKOFF_S = 1.5


def build_session() -> requests.Session:
    """A session with transport-level retries for the transient S3 faults."""
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    retry = Retry(
        total=RETRY_TOTAL,
        connect=RETRY_TOTAL,
        read=RETRY_TOTAL,
        status=3,
        backoff_factor=RETRY_BACKOFF_S,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "HEAD"}),
        respect_retry_after_header=True,
        # Return the final response instead of raising, so fetch() keeps sole
        # ownership of what a status means and the contract still holds.
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=4)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


@dataclass(frozen=True)
class FetchResult:
    """The outcome of one conditional GET."""

    spec: FeedSpec
    status: int
    fetched_at: datetime
    elapsed_ms: int
    body: bytes | None = None
    etag: str | None = None
    last_modified: str | None = None

    @property
    def changed(self) -> bool:
        """True when the server returned a new payload (200 with a body)."""
        return self.status == 200 and self.body is not None

    @property
    def unchanged(self) -> bool:
        """True when the server confirmed nothing moved (304)."""
        return self.status == 304


@dataclass
class FeedState:
    """Per-feed polling state.

    Lives across ticks, which is why the fetcher is a class and not a
    function. `consecutive_unchanged` is what a staleness alarm reads.
    """

    etag: str | None = None
    last_modified: str | None = None
    last_change: datetime | None = None
    consecutive_unchanged: int = 0
    polls: int = 0
    changes: int = 0
    errors: int = 0

    def record_change(self, at: datetime, etag: str | None, last_modified: str | None) -> None:
        self.etag = etag
        self.last_modified = last_modified
        self.last_change = at
        self.consecutive_unchanged = 0
        self.changes += 1

    def record_unchanged(self) -> None:
        self.consecutive_unchanged += 1

    def staleness_s(self, now: datetime | None = None) -> float | None:
        """Seconds since this feed last produced a new payload."""
        if self.last_change is None:
            return None
        now = now or datetime.now(timezone.utc)
        return (now - self.last_change).total_seconds()


class ConditionalFetcher:
    """Polls feeds with If-None-Match, tracking per-feed state.

    A single requests.Session is reused across polls so the TLS handshake and
    connection to S3 are amortised -- at a 10s interval across three feeds
    that is a meaningful fraction of the work.
    """

    def __init__(self, session: requests.Session | None = None) -> None:
        self.session = session or build_session()
        self.session.headers["User-Agent"] = USER_AGENT
        self.state: dict[str, FeedState] = {}

    def state_for(self, spec: FeedSpec) -> FeedState:
        return self.state.setdefault(spec.name, FeedState())

    def fetch(self, spec: FeedSpec) -> FetchResult:
        """Conditionally GET one feed and update its per-feed state.

        Sends `If-None-Match` when a previous ETag is known and omits the
        header entirely on the first poll -- an empty value is treated by
        some servers as a literal comparison, which would pin every poll
        to 200.

        Branching is on explicit status codes, never `r.ok`: 304 is not an
        error, but it is also not a payload, and requests counts anything
        under 400 as ok. The two success codes mean opposite things here
        (new payload vs no payload) and get separate branches.

        Paths:
          * 200 -- new payload: body and new ETag on the FetchResult, and
            state.record_change() updates the stored identity.
          * 304 -- nothing moved: body is None, state.record_unchanged()
            bumps the staleness counter, and the stored ETag is untouched.
            Clearing it is the silent failure mode: every later poll would
            return 200 and the conditional GET would stop working with no
            error anywhere.
          * anything else, or a transport error -- state.errors increments
            and FeedError carries the feed name and cause; run.py catches
            it per feed so one dead endpoint does not stop the others.

        state.polls increments on every path, failures included: the 304
        rate is polls-vs-changes, and an undercounted denominator makes
        the feed health metric lie. (`requests` follows redirects and
        returns the final response; S3 does not redirect here, but if it
        ever does, r.status_code is the redirected response's.)
        """
        state = self.state_for(spec)
        if state.etag is not None:
            headers = {"User-Agent": USER_AGENT, "If-None-Match": state.etag}
        else:
            headers = {"User-Agent": USER_AGENT}
        started = monotonic()
        state.polls += 1
        try:
            r = self.session.get(spec.url, headers=headers, timeout=TIMEOUT_S)
        except requests.RequestException as exc:
            state.errors += 1
            # No feed-name prefix here: run.py's handler logs "[%s] %s" with
            # spec.name, so embedding it would print "[trip_updates]
            # [trip_updates] fetch failed" on every line.
            raise FeedError(f"fetch failed: {exc}") from exc
        fetched_at = datetime.now(timezone.utc)
        elapsed_ms = int((monotonic() - started) * 1000)

        if r.status_code == 200:
            etag = r.headers.get("ETag")
            last_modified = r.headers.get("Last-Modified")
            state.record_change(fetched_at, etag, last_modified)
            return FetchResult(
                spec=spec,
                status=r.status_code,
                fetched_at=fetched_at,
                elapsed_ms=elapsed_ms,
                body=r.content,
                etag=etag,
                last_modified=last_modified,
            )

        if r.status_code == 304:
            state.record_unchanged()
            return FetchResult(
                spec=spec,
                status=r.status_code,
                fetched_at=fetched_at,
                elapsed_ms=elapsed_ms,
                # body/etag/last_modified stay None -- and state.etag is
                # deliberately untouched. Clearing it here is the silent
                # failure mode: every later poll would return 200 and the
                # conditional GET would stop doing its job with no error.
            )

        state.errors += 1
        raise FeedError(f"unexpected status {r.status_code}")

    # ------------------------------------------------------------------------

    def summary(self) -> str:
        """One-line health line per feed, for the run loop's periodic log."""
        parts = []
        for name, st in sorted(self.state.items()):
            stale = st.staleness_s()
            parts.append(
                f"{name}={st.changes}/{st.polls}"
                + (f" stale={stale:.0f}s" if stale is not None else "")
                + (f" err={st.errors}" if st.errors else "")
            )
        return "  ".join(parts)
