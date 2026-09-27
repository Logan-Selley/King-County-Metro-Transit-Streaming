# ADR 0011: A static findings site, cut from the marts and committed

**Status:** Accepted. The site, the export, and the mart have landed: `site/`,
`publish/export.py`, `mart_bunching_alerts`, and `.github/workflows/pages.yml`.
The numbers it shows are in docs/findings.md section 14.
**Date:** 2026-09-27

## Context

By the end of Phase 6 the project had produced a warehouse of measured results
and ten ADRs explaining them, and no way for anyone to see either without
standing up the local stack. The deliverable that closes that gap is a page.

The constraints are what make it a design question rather than a styling one:

- **There is no warehouse in CI.** A GitHub runner has the repository and
  nothing else, so anything the page needs has to be either committed or
  computed from committed files.
- **The replay topics expire after 7 days** (terraform/core/replay.tf). The
  fidelity numbers come from topics, not from the warehouse, so they cannot be
  recomputed later from anything.
- **The marts are the tested layer.** The contract suites, the reconciliation
  tests, and the grain tests all point at `marts.*`. A page that computed its
  own numbers from `raw.*` would be a second implementation of the same
  arithmetic with no tests behind it.
- **Serving has to cost nothing.** No server, no database, no build step that
  can fail between a commit and a reader.

## Decision

**A static page in `site/`, with its data committed, served by GitHub Pages from
an Actions artifact.** Branch-based Pages serves the repository root or `/docs`
and nothing else; `site/` is neither, and `/docs` holds the written
documentation. The artifact upload also means no Jekyll, which would silently
drop any file or directory whose name starts with a leading `_`.

**Every number comes from the marts.** `publish/export.py` reads
`mart_bunching_alerts`, `mart_feed_health`, and the two prediction
intermediates, and writes six JSON files. It reads no `raw.*` and no `staging`
table. What the page shows is therefore the layer the tests enforce, and a
change that breaks a mart breaks the page's data with it.

**No generated timestamp.** A re-export of an unchanged window is byte-identical,
so git shows nothing, and a diff between a re-cut and the committed file is
itself the check that the committed file came from this code. The window is
recorded in every file; the commit records when it was cut.

**`replay.json` is the one file this cannot produce.** It comes from the replay
topics, which keep 7 days, so `replay/report.py` reads them and the warehouse
and writes the file once per run. The committed copy is the only copy of the run
it documents, and the module is what makes its numbers reproducible while the
topics still exist.

**One new model: `mart_bunching_alerts`.** The page needs each alert placed on a
map, which no existing mart does; `mart_bunching_by_route_hour` aggregates all
history by route and hour and answers a different question. The new mart is
alert-grain, contract-enforced, and incremental on `window_at` with a 15-minute
lookback against the detector's 360 s lateness bound, so an hourly run touches
tens of alerts.

**The link preview is rendered from the same JSON.** `publish/card.py` draws
`site/og-card.png` with headless Chromium from `site/data/kpis.json` and
`site/data/replay.json`, so the card cannot advertise a number the page does not
show, and rendering it adds no dependency to the project.

## Alternatives considered

- **Build the data during the Pages workflow.** There is no warehouse on a
  runner, and the replay topics are long gone by then. It would mean committing
  a warehouse snapshot instead, which is what the JSON files already are.
- **Serve the JSON from an API.** That is a server, a deployment, and an uptime
  problem for a portfolio page that has to keep working unattended.
- **A client-side query against a database.** Same problem, plus credentials in
  a browser.
- **A hosted dashboard** rather than a page in the repository. The sibling
  parcel project has one, and it answers a different need: it goes stale when
  the account lapses, and it cannot be read from the repository it describes.
- **Deploy from a branch.** `site/` is not the root or `/docs`, and moving the
  written documentation out of `/docs` to make room for the site would trade the
  project's documentation layout for a hosting detail.

## Consequences

The page is a snapshot by design, and it says so: its window is stated in every
file and printed on the page. A reader who wants current numbers runs
`make exports`, and whoever publishes an update commits the result.

Regenerating the data needs the local stack, so nothing in CI can re-derive it.
The committed JSON is the contract between the warehouse and the page, and
re-cutting is a deliberate act rather than a build step.

The mart is on the hourly path, which keeps the export cheap: a full rebuild
takes 148 s for the whole alert history and the incremental run takes seconds.
The export reads closed days, whose numbers do not move.

The study window is 2026-09-24 to 09-30 Pacific, five weekdays and a weekend,
starting from the first day the stack ran complete. A claim about routes in
general wants a week, which is why the window is a week and why the page prints
it.
