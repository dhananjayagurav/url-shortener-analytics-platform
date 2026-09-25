# Analytics Engineering Guide

**This is the single learning, architecture, and reference document for the
`url-shortener-analytics-platform` repository.** It is meant to be opened
once now and reopened months from now to re-derive the whole system: every
concept is explained from first principles, every design decision is
recorded as an ADR, and every claim about "how this works" points at a real
file in this repository rather than duplicating it.

**How this document works:** concepts are explained here; implementation
lives in the repository. When you see a reference like
`ingestion/src/url_shortener_analytics/extract_full.py`, that file is the
real, current, executable implementation — this guide explains *why* it's
built the way it is and *how to run it*, not a copy of its source.

**Status of this document:** Phase 1 is in progress and this guide grows
with it. Sections are marked ✅ (implemented and documented) or ⏳ (planned,
not yet built) in the table of contents below — an honest, current map of
the project, not a promise of what will eventually exist. Sections marked
✅✅ have been written (or rewritten) to the **full teaching template**
described immediately below; a plain ✅ means the section exists but is
still due an upgrade to that depth in a later increment.

### How this guide is structured

This is not a reference manual — it's meant to teach the material the way
a principal engineer mentoring a new hire would, assuming nothing. Every
major concept in this guide is built from the same template, so once you
know the template you always know what's coming next and why:

| Subsection | What it does |
|---|---|
| **Concept** | Explains the idea from first principles, in plain language, assuming no prior knowledge. |
| **Why does this exist?** | The real production problem this concept solves — concepts taught without a motivating problem don't stick. |
| **Simple Example** | A small, generic example — nothing to do with URL shorteners yet — so the *shape* of the idea is clear before any project-specific detail gets added. |
| **URL Shortener Example** | The same idea, now grounded in this project's actual schema and numbers. |
| **Architecture** | Where this concept sits in the overall system, usually with a diagram. |
| **Design Decision** | The specific choice this project makes, and why. |
| **Alternatives** | What else could have been chosen instead. |
| **Trade-offs** | What's given up and what's gained by the choice actually made — never "X is just better." |
| **Implementation** | Exactly which file(s) this concept lives in, in the exact `CREATE / PURPOSE / DEPENDENCIES / IMPLEMENTATION / RUN / VERIFY / EXPECTED / TEST / PRODUCTION CONSIDERATIONS / INTERVIEW QUESTIONS` format for anything that's real, runnable code. |
| **How to Run / How to Verify** | Exact commands — never "run the script." |
| **Hands-on Exercise** | Something to actually do — run a lab, or implement a piece yourself using the *Implementation Guide* rather than the copy-paste *Reference Implementation* (see box below). |
| **Failure Scenario** | What breaks, deliberately explored, not glossed over. |
| **Production Considerations** | POC vs. real production, explicitly labeled `POC SIMPLIFICATION` wherever this repo cuts a corner intentionally. |
| **Principal Data Engineer Perspective** | What a principal engineer specifically would be thinking about here — the judgment calls, not just the mechanics. |
| **Interview Questions** | Realistic questions at this topic, with what a strong answer actually covers. |

**Copy-paste vs. implement-yourself.** For every real implementation
component, this guide gives you both an **Implementation Guide** (what to
build and why, detailed enough to write it yourself) and a **Reference
Implementation** (complete, working code you can paste directly into the
named file). Which one you use is your call — copy it to move fast, or
write it yourself from the guide for the hands-on learning that's the
actual point of this project. Every component also includes a **Hands-on
Challenge**: a specific modification or extension you implement yourself,
because reading a finished pipeline and *changing* one under your own
power exercise genuinely different muscles.

**On fabricated results.** Nowhere in this guide is a benchmark number,
row count, or test count invented. Every number you see under "ACTUAL
OBSERVED RESULT" was actually produced by running the stated command in
this environment. Anything not yet run is labeled **"Not yet executed"**,
with the exact command to produce the real result yourself.

---

## Table of Contents

**Foundations**
1. [Existing URL Shortener Assessment](#1-existing-url-shortener-assessment-) ✅✅
2. [OLTP vs OLAP](#2-oltp-vs-olap-) ✅✅
3. [Phase 1 Architecture](#3-phase-1-architecture) ✅
4. [Project Structure](#4-project-structure) ✅
5. [Git Workflow](#5-git-workflow) ✅
6. [Environment Setup](#6-environment-setup) ✅

**Data Modeling**
7. [Analytics Requirements & Business Metrics](#7-analytics-requirements--business-metrics-) ✅✅
8. [Data Grain](#8-data-grain-) ✅✅
9. [Source Data Model](#9-source-data-model-) ✅✅
10. [Analytical Data Model (Fact/Dimension Design)](#10-analytical-data-model-factdimension-design-) ✅✅
11. [Star Schema vs. Normalized Model](#11-star-schema-vs-normalized-model-) ✅✅
12. [Data Contracts](#12-data-contracts-) ✅✅

**Ingestion**
13. [Batch Ingestion Design](#13-batch-ingestion-design) ✅
14. [Full Load Ingestion](#14-full-load-ingestion-) ✅✅
15. [Incremental Load & Watermarks](#15-incremental-load--watermarks-) ✅✅
16. [Checkpointing](#16-checkpointing-) ✅✅
17. [Idempotency](#17-idempotency-) ✅✅

**Storage**
18. [Object Storage Fundamentals](#18-object-storage-fundamentals-) ✅✅
19. [Parquet](#19-parquet-) ✅✅
20. [Partitioning](#20-partitioning-) ✅✅
21. [File Layout](#21-file-layout-) ✅✅
22. [Ingestion Metadata (deep-dive)](#22-ingestion-metadata-deep-dive-) ✅✅

**Quality & Operations**
23. [PII and Security](#23-pii-and-security-) ✅✅
24. Testing (deep-dive) ⏳ *(tests exist now — see [Section 14.6](#146-how-to-test)*)
25. Failure Scenarios (all 10) ⏳ *(one is demonstrated now — see [Section 14.7](#147-failure-scenario)*)
26. Performance ⏳
27. Scale Design ⏳

**Reference**
28. [Architectural Principles](#28-architectural-principles) ✅ *(introduced now, extended as more are demonstrated)*
29. [Architecture Decision Records](#29-architecture-decision-records) ✅
30. Hands-on Labs (index) ⏳ *(LAB 1, LAB 4/5 — Section 14.5; LAB 2, LAB 3 — Section 15.5; LAB 6-9 — Sections 7.3/8.3/9.3/11.3; LAB 10 — Section 10.7; LAB 11 — Section 12.6; LAB 12 — Section 16.5; LAB 13 — Section 17.5; LAB 14 — Section 18.5; LAB 15 — Section 19.5; LAB 16 — Section 20.5; LAB 17 — Section 21.5; LAB 18 — Section 22.5; LAB 19 — Section 23.5)*
31. Interview Questions (consolidated, all categories) ⏳ *(Category C questions exist now — see Sections 7, 8, 9, 10, 11, 12, 14.9, 15.9, 16.9, 17.9, 18.9, 19.9, 20.9, 21.9, 22.9, and 23.9)*
32. Principal-Level Scenarios ⏳
33. [Phase 1 Summary](#33-phase-1-summary-so-far) (running, updated each increment)
34. [Phase 1 Completion Checklist](#34-phase-1-completion-checklist)
35. [Git Repository Review](#35-git-repository-review)
36. What Phase 2 Will Add ⏳

---

## 1. Existing URL Shortener Assessment ✅✅

### Concept

Before designing anything new, we read the real system. Every fact in this
section came from directly reading files in the `url-shortener` repository
on **2026-09-19** — nothing here is inferred or assumed. If you're new to
this idea: in real data engineering work, "the schema" is not a diagram
someone hands you — it's whatever the application's actual migration
history says it is, which is frequently different from what a requirements
document *says* it should be. Learning to read code as the source of truth,
rather than a document that describes it, is a habit this whole section
exists to build.

### Why does this matter?

Inventing a plausible-looking source schema is a common shortcut, and it's
the wrong one. A portfolio project built against an imaginary schema
teaches you to design against your own assumptions, not against a real
system's actual constraints — and in a real job, the gap between "what the
docs say" and "what the code does" is precisely where production incidents
come from. Section E below (the honest mismatch) is exactly the kind of
finding that would be invisible if this platform's schema had been
designed from the requirements doc instead of read from the code.

### Simple Example

Imagine you join a company and are told, "our `orders` table has a
`status` column with values `pending`, `shipped`, `delivered`." Before
building a dashboard that groups orders by status, you'd run
`SELECT DISTINCT status FROM orders` yourself — because "cancelled" might
exist in production and simply never made it into anyone's documentation.
That one query is the same instinct this entire section is built on, just
applied to a whole codebase instead of one column.

### A. Existing Application Architecture

```mermaid
flowchart TD
    Client([Client]) -->|POST /api/v1/urls| API[FastAPI app]
    Client -->|GET /short_code| API
    API --> Svc[UrlService]
    Svc --> Repo[UrlRepository]
    Svc --> Cache[UrlCache]
    Cache -.->|cache-aside| Redis[(Redis)]
    Repo --> PG[(PostgreSQL<br/>urls table)]
    API -->|302 redirect| Client
```

**Readable ASCII equivalent** (per this guide's rule of never depending
solely on Mermaid rendering):

```
Client --POST /api/v1/urls--> FastAPI app --> UrlService --> UrlRepository --> PostgreSQL (urls table)
Client --GET /{short_code}--> FastAPI app --> UrlService --> UrlCache --(cache-aside)--> Redis
                                                                     |
                                                            (on miss) --> UrlRepository --> PostgreSQL
FastAPI app --302 redirect--> Client
```

Stack, confirmed from `pyproject.toml` and `docker-compose.yml`: FastAPI,
SQLAlchemy 2.0 + Alembic, PostgreSQL 16, Redis (cache-aside), pydantic-settings
for config, pytest for tests, Docker Compose for local orchestration.
Kubernetes and Kafka are explicitly deferred in that project too (see its
own `docker-compose.yml` header comment) — the same "earn complexity, don't
front-load it" discipline this platform follows.

**System context** — where this analytics platform sits relative to the
application it observes:

```mermaid
flowchart LR
    User([End user]) -->|creates / clicks<br/>short URLs| App[url-shortener<br/>FastAPI application]
    App --> OLTP[(url-shortener's<br/>PostgreSQL)]
    Operator([You, the engineer]) -->|reads schema,<br/>never writes to it| OLTP
    Operator -->|runs| Platform[url-shortener-analytics-platform<br/>THIS repository]
    Platform --> OwnPG[(This platform's own<br/>Postgres mirror — ADR-009)]
    Platform --> MinIO[(MinIO / Bronze)]
    Analyst([Analyst / you, later]) -->|queries| MinIO
```

The two systems are deliberately **two separate Git repositories, two
separate Postgres instances**. This platform never connects to the real
`url-shortener` application's live database — see
[ADR-009](#adr-009-this-platform-owns-its-own-oltp-mirror-postgres) for
exactly why, and what changes about this diagram in a real production
deployment.

### B. Existing Database ER Diagram

```mermaid
erDiagram
    URLS {
        bigint id PK
        varchar short_code "nullable, unique, indexed"
        text original_url
        timestamptz created_at
        timestamptz expires_at "unenforced"
        bigint user_id "unenforced FK, no users table exists"
        boolean is_active
        timestamptz deleted_at "soft delete"
    }
```

**Readable ASCII equivalent:**

```
urls
├── id             BIGINT PK, autoincrement
├── short_code     VARCHAR(16), nullable, UNIQUE, indexed
├── original_url   TEXT, not null
├── created_at     TIMESTAMPTZ, not null
├── expires_at     TIMESTAMPTZ, nullable   <- exists, unenforced anywhere
├── user_id        BIGINT, nullable        <- exists, no users table exists
├── is_active      BOOLEAN, not null, default true
└── deleted_at     TIMESTAMPTZ, nullable   <- soft delete marker
```

One table. That's the entire persisted schema of the real application as
of this reading — confirmed by reading `app/models/url.py` (the SQLAlchemy
ORM model) against all four Alembic migrations in
`migrations/versions/`, in order:

| Migration | What it added |
|---|---|
| `3813da2d7b76` (baseline) | `id`, `short_code` (unique, not null then), `original_url`, `created_at` |
| `cd8736f28600` | `expires_at`, `user_id`, `is_active` |
| `ff8ab26cc6ea` | `deleted_at` |
| `957b3fbefa19` | `short_code` made nullable (supports an id-first generation strategy where a row can briefly exist before its code is assigned) |

### C. Source-to-Analytics Mapping

This is the table that turns "here's a schema" into "here's what we can
actually answer with it" — mapping each real (or hypothetical, clearly
marked) source field to the analytics questions from Section 5 it can
answer.

| Source field | Table | Analytics use | Status |
|---|---|---|---|
| `urls.id` | `urls` | Surrogate key for `dim_url`; join target from `clicks.short_code` (indirectly, via `short_code`) | Real |
| `urls.short_code` | `urls` | Natural key for a URL; join key from `clicks` | Real |
| `urls.created_at` | `urls` | "URLs created per day", cohort analysis | Real |
| `urls.user_id` | `urls` | "URLs per user" — **only answerable once a real `users` table exists** | Real column, unenforced, no real `users` table |
| `urls.expires_at` | `urls` | "% of URLs expired" | Real column, but the application never enforces it — see Section E |
| `urls.is_active` / `deleted_at` | `urls` | "Active vs. soft-deleted URLs" | Real |
| `users.email`, `users.plan_type` | `users` (**hypothetical**) | "Clicks by subscription plan", user-level rollups | Hypothetical — see Section E |
| `clicks.occurred_at`, `device_type`, `hashed_ip`, `user_id` | `clicks` (**hypothetical**) | Every traffic-analytics question in Section 5 | Hypothetical — see Section E |

Reading this table left to right tells you something important on its
own: **every "traffic analytics" question this project wants to answer
depends on the hypothetical `clicks` table**, because the real application
captures zero click events today. That's not a minor footnote — it's the
single biggest constraint on this entire platform's Phase 1 design, and
it's why Section E exists.

### D. Gaps

- No event/click table of any kind — `GET /{short_code}` in
  `app/api/urls.py` resolves and 302-redirects without writing anything.
  There is no logging, no background task, no queue message — nothing.
- No `users` table, despite `urls.user_id` existing as a column with no
  foreign key constraint enforcing it (Postgres will happily accept any
  `BIGINT`, including one that matches no real user, or no user at all).
- No geographic or device information captured anywhere in the schema or
  application code.
- No index on `urls.created_at` or `urls.user_id` — irrelevant to the
  live application (neither is queried directly today) but relevant to
  this platform's extraction queries, discussed in
  [Section 14.8](#148-production-considerations).

### E. Assumptions

Stated explicitly, because an assumption that's never written down is an
assumption nobody can challenge later:

1. **Assumption:** click events, once the real application implements
   them, will look approximately like this platform's hypothetical
   `clicks` table (`short_code`, `occurred_at`, `device_type`, a
   pseudonymized IP, an optional `user_id`). **Basis:** this mirrors the
   fields `docs/01-requirements.md`'s functional requirement #5 explicitly
   names ("click count, timestamp, coarse geo/device info"). **Risk if
   wrong:** if the real implementation later captures materially different
   fields (e.g. a session ID, a referrer URL), this platform's source
   contract (Section 12, planned) will need to be revised — which is
   exactly the "schema evolution" problem Section 12 teaches how to handle
   gracefully.
2. **Assumption:** a `users` table, if built, will look like the
   hypothetical one here (`email`, `plan_type`, `created_at`). **Basis:**
   `urls.user_id` already exists as a `BIGINT`, implying a numeric
   surrogate-keyed users table was originally planned. **Risk if wrong:**
   low — this is a conventional enough shape that most real
   implementations would resemble it.
3. **Assumption:** this platform's own standalone Postgres (ADR-009) is an
   acceptable stand-in for "the real OLTP database" for *learning*
   purposes, even though a real production pipeline would read from a
   replica of the actual application's database, never a hand-built
   mirror. **Risk if wrong:** none for learning purposes; explicitly not
   claimed to be production-accurate, and called out as a POC
   simplification everywhere it matters.

### F. Recommended Minimal Changes (to the real application, not implemented here)

Not implemented by this platform — these belong to the `url-shortener`
repository's own roadmap (its README already lists "Phase 10 — Analytics"
as a planned phase):

1. Add a `clicks` (or equivalent event) table and write to it from the
   redirect handler — ideally decoupled from the request/response cycle
   (a background task, or eventually a queue) so click logging never adds
   latency to the redirect's hot path.
2. Add a real `users` table if user accounts become in-scope, with a
   foreign key from `urls.user_id`.
3. Either enforce `expires_at` (a scheduled job, or a check in the
   redirect path) or remove the column — an unenforced column that looks
   load-bearing is a common source of production confusion.
4. Add an index on `urls.created_at` if this platform's extraction queries
   ever filter on it directly (they don't yet — Phase 1 uses full loads
   and an `id`-based watermark, per [ADR-005](#adr-005-watermark-based-incremental-ingestion-implemented)).

### Principal Data Engineer Perspective

A principal engineer reviewing a brand-new analytics project asks one
question before looking at any architecture diagram: **"Where did this
schema actually come from?"** An answer of "I read the migrations" is a
very different signal than "I assumed it based on the requirements doc" —
the first tells you the design is grounded in reality and will hold up
under a real data quality audit; the second tells you the design might be
elegant but could be quietly wrong about something that matters. This
section exists so that, from the very first page, this project can
honestly answer "I read the migrations."

### Interview Questions

**Q: "How would you approach designing an analytics pipeline for an
application you've never seen the code for?"**

*What's tested:* whether the candidate's default instinct is to trust
documentation or to verify against the system itself.

*Strong answer:* start by getting read access to the actual schema (via
migrations, an ORM model, or a live `\d table_name` in psql) rather than
relying on a requirements document or a verbal description from a
stakeholder — both routinely drift from what's actually deployed.
Cross-reference the schema against what the application's own API/service
layer actually does with it (does every column get written to? Is
anything enforced only in application code, not the database?). Document
gaps and assumptions explicitly, the way Sections D and E above do, so
anyone reviewing the design later can see exactly what was verified versus
assumed.

*Common mistake:* designing an idealized dimensional model from a
requirements document without ever looking at the real schema — it's the
single fastest way to build something wrong that looks right in a design
review.

---

## 2. OLTP vs OLAP ✅✅

### Concept

**OLTP** stands for **Online Transaction Processing**. An OLTP system is
built to handle a large number of small, fast operations on individual
rows: create this record, look up that one record, update this other
record — each one touching a handful of rows, each one needing to finish
in milliseconds, each one needing strong correctness guarantees even when
thousands of these happen concurrently.

**OLAP** stands for **Online Analytical Processing**. An OLAP system is
built for the opposite shape of work: scan potentially millions of rows,
aggregate them (count, sum, average, group by), and return one summarized
answer. A single OLAP query might legitimately take seconds — that's
acceptable, because it's answering a question like "how many clicks
happened per day last month," not serving a single user waiting on a page
load.

If you remember nothing else from this section: **OLTP optimizes for
"how fast can I touch one row," OLAP optimizes for "how fast can I
summarize a billion rows."** Those are different engineering problems,
and systems tuned for one are typically bad at the other.

### Why does this exist?

Because a single database, tuned for one workload, is *measurably worse*
at the other — not just "less ideal," but actually worse in ways that show
up as real production incidents. An OLTP database like Postgres is tuned
around B-tree indexes that make "find the one row with this key" fast, and
a buffer cache (recently-used pages of data, kept in memory) sized around
the *application's* normal access pattern. A query that scans millions of
rows to compute an aggregate has to pull all of those rows through that
same buffer cache — potentially evicting the "hot" pages the application
depends on for its own fast lookups. The two workloads are, quite
literally, fighting over the same limited RAM.

### Simple Example

Picture a library. OLTP is the front desk: "do you have this exact book,
by this exact call number?" — fast, one lookup, done in seconds, and the
front desk is built (organized shelves, a catalog system) specifically to
answer that fast. OLAP is a researcher who wants to know "how many books
on 19th-century France did this library acquire, by year, for the last 50
years?" — that requires walking through a huge fraction of the catalog,
takes much longer, and if the researcher tried to do this by constantly
interrupting the front desk clerk's one-lookup-at-a-time system, they'd
slow down everyone else trying to check out a book. The right answer isn't
"make the researcher stop asking questions" — it's to give the researcher
a separate, purpose-built research room with its own copy of the catalog,
organized differently (by topic and year, not by shelf location), so
neither one gets in the other's way.

### URL Shortener Example

```
Application
    |
    v
PostgreSQL
    |
    +---- application traffic  (redirects: the hot path, ~1,200 reads/sec peak
    |      per the source project's own capacity estimate in docs/01-requirements.md)
    |
    +---- analytics queries    (an aggregate scan competing for the same
           buffer cache, connection pool, and disk I/O)
```

A single expensive analytical query — "count clicks per URL for the last
90 days" — run directly against this Postgres instance has a real chance
of evicting hot pages from the buffer cache that the redirect path depends
on, degrading redirect latency for actual users. At this project's stated
scale (a 10:1 read:write ratio, redirects being the explicit hot path),
that's not a hypothetical risk — it's the exact failure mode the source
project's own `docs/01-requirements.md` is written to avoid. Worse: this
risk is *invisible* under light load and only shows up once analytics
query volume or table size grows — which is exactly the kind of problem
that's cheap to design away now and expensive to retrofit once it's
already hurting real users.

### Architecture: separating the two

```mermaid
flowchart TD
    App[Application] --> OLTP[(OLTP PostgreSQL)]
    OLTP -->|batch extraction<br/>ingestion/src| Obj[(Object Storage<br/>MinIO / Bronze)]
    Obj --> Analytics[Analytics Platform<br/>future: Silver / Gold, Phase 2+]
```

**Readable ASCII equivalent:**

```
Application --> OLTP PostgreSQL --(batch extraction, ingestion/src)--> Object Storage (Bronze)
                                                                              |
                                                                              v
                                                          Analytics Platform (future Silver/Gold, Phase 2+)
```

`OLTP PostgreSQL` keeps serving application traffic, untouched by
analytical query load. `Batch Extraction` (this repository's `ingestion/`
package) reads from it on a schedule, not on every request. `Object
Storage` holds the extracted data independently, so any number of
analytical engines can read from it later without going back to OLTP at
all.

### Design Decision

This platform separates OLTP and OLAP from day one — even though, at this
project's current tiny data volume, querying OLTP directly for analytics
would work *just fine* in practice. The decision is made anyway, for two
reasons specific to this being a learning project: first, the whole point
is to build (and feel) the real separation, not just talk about it;
second, the separation costs almost nothing to build now (one Python
extractor, one object store) and would cost considerably more to retrofit
later once "just query Postgres for the dashboard" has become load-bearing
habit across a team.

### Alternatives

1. **Query OLTP directly for everything, no pipeline at all.** Works fine
   at small scale and zero analytical query volume; breaks down exactly as
   described above once either grows. The simplest option, and the wrong
   one to default to without at least considering the alternative below.
2. **Add a read replica, query the replica directly.** Solves the resource
   *contention* problem — the replica has its own buffer cache and disk
   I/O, so an expensive analytical query there can't degrade the
   primary's redirect latency. Does **not** solve the *shape* problem: the
   replica has the exact same row-oriented, heavily-normalized OLTP schema
   as the primary, which is a poor fit for aggregation-heavy queries — see
   Trade-offs below and the interview question at the end of this section.
3. **Build a real batch-extraction pipeline into object storage (chosen).**
   Solves both the contention problem and the shape problem (Bronze data
   can later be reshaped into an analytics-friendly model, Sections 9-11)
   at the cost of added infrastructure and data that's only as fresh as
   the last extraction run.

### Trade-offs

| | Query OLTP directly | Read replica | Batch pipeline (chosen) |
|---|---|---|---|
| Protects production from analytical load | No | Yes | Yes |
| Analytics-shaped schema (not just OLTP mirror) | No | No | Yes (once Sections 9-11 land) |
| Data freshness | Real-time | Real-time | As fresh as the last run |
| Infrastructure to run | None | A replica | A replica (later) + extractor + object storage |
| Right choice when... | Query volume is trivially low | Freshness matters more than shape, and volume is moderate | Freshness can lag, and analytical query shape/volume genuinely differs from OLTP |

No option is universally correct — the right one depends on freshness
requirements, query volume, and query shape, which is exactly why the next
subsection exists.

### When is querying OLTP directly still acceptable?

Not every analytics need justifies a whole pipeline. Direct OLTP querying
remains reasonable when: query volume is low and infrequent (an
engineer checking one number by hand), the query is naturally
narrow (filtered by an indexed key, not a full scan), or a read replica
absorbs the load so it never touches the primary at all. The line is
crossed once queries become frequent, unpredictable in shape, or wide
(scanning large fractions of a table) — at that point, the buffer-cache
contention risk above stops being theoretical.

### Failure Scenario

**What actually happens, mechanically, when an analytical query degrades
redirect latency?** Postgres keeps recently-accessed data pages in a
memory area called the buffer cache (sized by the `shared_buffers`
setting). The redirect path's hot data — the small set of frequently
short-coded URLs, per this project's own stated Zipfian/power-law traffic
assumption in `docs/01-requirements.md` — normally stays resident there,
making lookups fast. A full-table scan for an analytical aggregate reads
far more distinct pages than fit in the buffer cache, and as Postgres
pulls those pages in, it evicts existing cached pages to make room —
including, potentially, the hot redirect pages. The next redirect for a
popular short code then has to fetch from disk instead of memory: slower,
and at high concurrency, contending for the same I/O bandwidth the
analytical query is also consuming. No single component "breaks" — it's a
resource-contention degradation, which is part of what makes it dangerous:
it doesn't throw an error, it just quietly gets slower.

### Production Considerations

| Aspect | This repo (POC) | Production |
|---|---|---|
| OLTP instance | This platform's own standalone Postgres mirror (ADR-009) — **POC SIMPLIFICATION**, never real application traffic | The pipeline reads from a real read replica of the production database, never the primary |
| Extraction frequency | Manual (`make ingest-full`) | Scheduled (an orchestrator, introduced Phase 3+) |
| Contention protection | N/A — this repo's mirror Postgres has no other traffic to contend with | A replica, connection limits, and query timeouts on the extraction connection specifically |

### Principal Data Engineer Perspective

A principal engineer doesn't treat "separate OLTP from OLAP" as a rule to
follow reflexively — they treat it as a trade-off to make consciously,
with a specific, named threshold for when it's worth the cost. The
judgment call is knowing *when* a team has crossed that threshold (query
volume, query shape, or organizational trust in "just query prod" all
matter) and being able to say so with concrete evidence, not vibes. Saying
"we should always separate OLTP and OLAP" in a design review is a weaker
answer than "here's the query pattern and volume at which direct OLTP
access degrades our SLO, and here's why we're either past it or not yet."

### Interview Questions

**Q: "Why not just add a read replica and run analytics queries against
that, instead of building a whole ingestion pipeline?"**

*What's tested:* whether the candidate understands that OLTP/OLAP
separation is really two separate problems (resource contention and
schema shape), not one.

*What a weak answer looks like:* "A replica adds lag, so it's not real-time"
— true, but misses the more important, structural reason.

*What a strong answer covers:* a read replica solves the *contention*
problem (analytics load no longer competes with the primary for
resources) but not the *shape* problem — a replica has the exact same
normalized, row-oriented OLTP schema as the primary, which is a poor fit
for aggregation-heavy analytical queries. Every "clicks per day per URL"
query against a replica still needs a full scan and a GROUP BY against
row-oriented storage with no columnar pruning or pre-aggregation. A
replica is a reasonable stopgap for light, occasional analytical load; it
doesn't replace a real analytics platform once query volume or complexity
grows.

*Expected follow-up:* "At what point would you introduce a replica versus
going straight to a pipeline?" — a strong candidate ties this to query
volume/shape thresholds, not a fixed rule.

*Common mistake:* treating "replica" and "analytics pipeline" as
interchangeable solutions to the same problem, rather than recognizing
they solve different halves of it.

---

## 3. Phase 1 Architecture

### 3.1 Proposed architecture

```mermaid
flowchart LR
    subgraph OLTP["OLTP (docker-compose.yml: postgres)"]
        PG[(urls / users / clicks)]
    end
    subgraph Ingestion["ingestion/src/url_shortener_analytics"]
        EX[extract_full.py]
        MD[(ingestion_metadata)]
    end
    subgraph Bronze["Object Storage (docker-compose.yml: minio)"]
        BZ[("bronze/table/ingestion_date=YYYY-MM-DD/table.parquet")]
    end

    PG --> EX
    EX --> BZ
    EX <--> MD
```

**Deliberately excluded from Phase 1**, and why: Kafka/Debezium (no
event stream exists yet to consume — see Section 1.5), Spark (row counts
here don't remotely justify distributed processing), Airflow (one Python
CLI command is simpler to operate and debug than a scheduler at this
scale), Kubernetes (Docker Compose is sufficient for a single-host batch
job). Each is introduced later only once a specific, named limitation of
the simpler tool is actually hit — see [Section 29 ADR-004](#adr-004-batch-ingestion-before-cdcstreaming)
and the future Scale Design section.

### 3.2 In simple language

Every night (or on demand, right now — there's no scheduler yet), a small
Python program connects to the OLTP database, reads an entire table, and
writes it as a compressed file into object storage. It writes down what it
did — which table, how many rows, whether it succeeded — in a separate
bookkeeping table, so the next run (or a human debugging a problem) can
tell what happened without guessing.

---

## 4. Project Structure

| Path | Purpose |
|---|---|
| `docs/analytics-engineering-guide.md` | This file. The single learning/reference document. |
| `ingestion/src/url_shortener_analytics/` | The importable Python package: all pipeline logic. |
| `ingestion/tests/unit/` | Fast tests against SQLite + a mocked S3 client. No Docker required. |
| `ingestion/tests/integration/` | Tests against the real Postgres + MinIO from `docker-compose.yml`. |
| `ingestion/configs/pipelines.yaml` | Which tables the pipeline extracts, and how — config, not code. |
| `schemas/source/`, `sql/source/` | Documentation and DDL for the source schema (real + clearly-labeled hypothetical tables). |
| `schemas/analytics/`, `sql/analytics/` | Reserved for the dimensional model — not yet populated. |
| `scripts/` | One-off operator scripts (`seed_sample_data.py`). Not imported by the pipeline. |
| `benchmarks/` | Executable performance benchmarks. `parquet_vs_csv_vs_json.py` (Section 19) genuinely run; extraction-time-at-scale benchmarks (Section 26) not yet built. |
| `sample_data/` | Convention-only landing spot for local generated files; nothing here is committed. |
| `tests/` (root) | Reserved for cross-phase end-to-end tests, once more than one phase's components exist. |

**Why both `ingestion/tests/` and a root `tests/`?** `ingestion/tests/` is
scoped to one package and owned by whoever owns that package. The root
`tests/` is reserved for tests that span multiple components once they
exist (e.g., a future Bronze → Silver → Gold end-to-end test) — keeping
that distinction from day one avoids a later, disruptive reorganization.

**Why does `scripts/seed_sample_data.py` live outside `ingestion/src/`?**
It's an operator convenience for local development, not something the
production pipeline imports or depends on. Mixing the two would make it
unclear, a year from now, whether `seed_sample_data` is part of the
pipeline's runtime behavior — it isn't.

---

## 5. Git Workflow

### What belongs in Git

Source code, tests, SQL, configuration templates (`.env.example`, not
`.env`), documentation, and this guide. Everything needed to reproduce the
system from a clean clone plus `docker compose up -d`.

### What must never be committed

- `.env` (real environment values — `.env.example` is the checked-in
  template)
- Generated data: `pg_data/`, `minio_data/` (docker compose volumes),
  anything under `sample_data/generated/` or `benchmarks/results/`
- Local caches/artifacts: `.venv/`, `__pycache__/`, `.pytest_cache/`,
  `.ruff_cache/`, `*.egg-info/`
- Credentials or secrets of any kind, real or accidentally-real-looking

See [`.gitignore`](../.gitignore) for the enforced list.

### Expected workflow

```bash
git checkout -b feature/incremental-load
# implement in ingestion/src/, tests in ingestion/tests/
pytest                                   # unit tests must pass locally
ruff check . && ruff format .
git add ingestion/ docs/analytics-engineering-guide.md
git commit -m "Add watermark-based incremental load for clicks"
git push -u origin feature/incremental-load
# open a pull request; guide + code + tests reviewed together, not code alone
```

Documentation changes ship in the *same* commit/PR as the code they
describe — a stale guide is worse than no guide, because it's actively
misleading.

---

## 6. Environment Setup

### Prerequisites

| Requirement | Why |
|---|---|
| Python 3.12+ | Matches the source `url-shortener` project's own requirement, for consistency across the two repos. |
| Docker + Docker Compose | Runs this repo's own Postgres (schema-mirror) and MinIO. |

### Setup

```bash
git clone <this-repo>
cd url-shortener-analytics-platform
cp .env.example .env

make venv
source .venv/bin/activate
make install          # pip install -e ".[dev]"

make up                # starts Postgres (port 5433) + MinIO (9000/9001) + createbuckets
docker compose ps       # confirm postgres and minio show (healthy)

make seed              # seeds reproducible synthetic urls/users/clicks (Faker, seed=42)
```

MinIO's web console: `http://localhost:9001` (`labadmin` / `labpassword`
by default — see `.env.example`). Postgres is deliberately on host port
**5433**, not 5432, so it doesn't collide with a `url-shortener` Postgres
that might already be running on the same machine.

**A note on the MinIO image:** as of September 2026, Docker Hub no longer
serves `minio/minio` or `minio/mc` at all (both repositories were pulled).
This repo's `docker-compose.yml` pulls from `quay.io/minio/minio` and
`quay.io/minio/mc` instead, pinned to specific release tags rather than
`:latest`, both verified against MinIO's own GitHub release history.

---

## 7. Analytics Requirements & Business Metrics ✅✅

### Concept

**Analytics requirements** are the specific business questions a platform
must be able to answer, written down and agreed on *before* any dimension
or fact table is designed. A **metric** is one such question turned into a
precise, computable definition: what's being counted or measured, at what
grain, from which columns, with what filters.

### Why does this exist?

Skipping straight from "we have a `clicks` table" to "let's build a star
schema" is how projects end up with a dimensional model that's elegant but
answers the wrong questions — or worse, one where two different analysts
compute "daily active users" two different ways because no one wrote down
what it actually means. Requirements-first isn't paperwork for its own
sake: every design decision in Sections 8-11 (the fact table's grain,
which dimensions exist, what gets pre-computed vs. left to query time)
follows directly from the metrics catalog below. Skipping this step
doesn't remove the decisions — it just makes them implicitly, one query at
a time, usually inconsistently.

### Simple Example (generic, pre-URL-Shortener)

Imagine a small e-commerce team is asked to "add an orders dashboard."
Without requirements, three engineers build three different things: one
computes "revenue" including tax, one excludes it, one excludes refunded
orders and the other two don't. All three are defensible in isolation —
the disagreement was never resolved because no one wrote "revenue = sum of
`order_total` for orders where `status != 'refunded'`, tax included" down
anywhere before the dashboards shipped. A five-minute requirements
conversation would have prevented three incompatible numbers from ever
reaching a stakeholder meeting.

### URL Shortener Example

The real `url-shortener` application's own `docs/01-requirements.md`
already states an explicit v1 functional requirement: "basic analytics:
click count, timestamp, coarse geo/device info per short code" (quoted
already in Section 1.5 and ADR-008). That's a starting point, not a
complete spec — it names *what kind* of thing is wanted without defining a
single metric precisely enough to implement. This section turns it into
the actual metrics catalog this platform's dimensional model (Sections
9-11) is designed to serve.

### 7.1 Metrics Catalog

| Metric | Definition | Grain | Source |
|---|---|---|---|
| Total clicks per URL | `COUNT(*)` (or `SUM(click_count)`) grouped by `short_code` | Per URL | `fact_clicks` |
| Clicks over time | `COUNT(*)` grouped by `date_key` (optionally + `short_code`) | Per day (per URL) | `fact_clicks` + `dim_date` |
| Clicks by device type | `COUNT(*)` grouped by `device_type` | Per device category | `fact_clicks` + `dim_device` |
| Top N URLs by clicks | `total clicks per URL`, ordered descending, limited to N | Per URL | `fact_clicks` + `dim_url` |
| Anonymous vs. attributed click share | `COUNT(*)` grouped by `dim_user.is_known` | Per click | `fact_clicks` + `dim_user` |
| Clicks by plan type | `COUNT(*)` grouped by `dim_user.plan_type` (known users only) | Per plan | `fact_clicks` + `dim_user` |
| Clicks by destination domain | `COUNT(*)` grouped by `dim_url.original_url_domain` | Per domain | `fact_clicks` + `dim_url` |
| Active vs. inactive URL click share | `COUNT(*)` grouped by `dim_url.is_active` | Per URL status | `fact_clicks` + `dim_url` |

Every metric above is expressible as a `GROUP BY` over `fact_clicks`
joined to at most one dimension — a direct consequence of the grain
decision in Section 8 and the star schema shape in Section 11. None of
them require a self-join, a window function, or a pre-aggregated rollup
table to compute correctly at this project's current scale; Section 26
(Performance, planned) is where that stops being true and pre-aggregation
gets evaluated.

### Design Decision

**Every metric in the catalog above is derived, at query time, from
`fact_clicks`' atomic grain — none are pre-aggregated into a separate
summary table in Phase 1.** This mirrors Section 8's grain decision and is
recorded together with it, not as a separate, independent choice — see
Section 8's Design Decision for the reasoning; the two decisions have to
be made together, since a coarser fact grain would have made some of the
metrics above (e.g. clicks by device type) unanswerable without the raw
event detail.

### Alternatives

1. **No formal metrics catalog — build the star schema first, work out
   metrics from whatever it supports (rejected).** This is the failure
   mode described in the Simple Example: the "requirements" end up being
   whatever the schema happens to allow, discovered ad hoc, rather than
   deliberately designed for.
2. **A pre-aggregated metrics/summary table per metric (rejected for
   Phase 1).** Would make each dashboard query trivially fast at read
   time, at the cost of a transformation pipeline to maintain every
   summary table's freshness — real complexity this project hasn't earned
   yet at 5,000 seeded click rows. Revisit once Section 26's benchmarks
   show `fact_clicks` queries are actually slow.
3. **A written metrics catalog, computed live from an atomic-grain fact
   table (chosen).** Slower per-query than pre-aggregation, at a scale
   that doesn't matter yet; keeps every metric's definition traceable to
   one row of raw fact data, which is worth more during Phase 1's
   correctness-first stage than query speed is.

### Trade-offs

| | Live query over atomic grain (chosen) | Pre-aggregated summary tables |
|---|---|---|
| Correctness / traceability | High — every number traces to individual `fact_clicks` rows | Lower — a bug in the aggregation job silently poisons every downstream number until caught |
| Query latency at scale | Degrades as `fact_clicks` grows | Stays flat — the point of pre-aggregating |
| New metric, same source data | Free — just a new `GROUP BY` | Requires a new aggregation job, or reprocessing history |
| Operational complexity | None beyond the fact/dim tables themselves | An extra pipeline stage to build, schedule, and monitor |

### 7.2 Requirements Traceability

| Requirement source | What it says | Where it's addressed |
|---|---|---|
| `url-shortener`'s `docs/01-requirements.md`, functional requirement #5 | "basic analytics: click count, timestamp, coarse geo/device info per short code" | Click count → row 1 of the catalog; timestamp → `dim_date`/`occurred_at`; device info → row 3; "coarse geo" is explicitly **not** implemented (no geo column exists anywhere in this project's schema — see the Known Limitations note this introduces into Section 33) |
| This project's own curriculum (Section 7's own existence) | Analytics requirements should be defined before the analytical model | This catalog, Section 8 (grain), Sections 9-11 (model design) |

**Stated honestly:** "coarse geo info" from the real requirement is
explicitly out of scope for this catalog — there's no IP-geolocation step
anywhere in this pipeline (`clicks.hashed_ip` is hashed specifically to
avoid needing to resolve real IPs to locations; see Section 23, PII and
Security), and adding it would mean a new source column, a new dimension,
and a defensible privacy story, none of which this increment builds. It's
named here rather than silently dropped, matching this project's
established pattern (compare Section 33's other honestly-stated gaps).

### 7.3 Hands-on Exercise

**LAB 6 — Trace one requirement to one query.**

Pick "Total clicks per URL" from the catalog above. Without looking ahead
to Section 11's DDL, write down (in plain SQL, on paper or in a scratch
file) what you'd expect the query to look like once `fact_clicks` and
`dim_url` exist and are populated:

```sql
SELECT du.short_code, COUNT(*) AS total_clicks
FROM fact_clicks fc
JOIN dim_url du ON fc.url_key = du.url_key
GROUP BY du.short_code
ORDER BY total_clicks DESC;
```

Then read Section 11's actual `fact_clicks`/`dim_url` DDL and check
whether every column your query needs actually exists with the name and
join key you assumed. (It should — the DDL was designed *from* this
catalog, not the other way around. If you find a mismatch, that's the
exercise working: it means either the catalog or the DDL needs to change,
and Section 12's Failure Scenario discusses exactly this kind of
requirements-vs-schema drift.)

### Failure Scenario

**What happens if a metric's definition is ambiguous and two people
implement it differently?**

Concretely: "clicks by device type" could mean `GROUP BY device_type` over
every click (including anonymous ones, correct per this catalog) or
`GROUP BY device_type` filtered to only clicks with a known `user_id`
(a plausible but *different* metric someone might build without checking
the catalog). Both produce a table with the same column names and shape —
nothing about the output alone reveals which definition was used. This is
the requirements-ambiguity failure mode named in the Simple Example, now
concrete: without a catalog entry stating explicitly that this metric
includes anonymous clicks, two dashboards can disagree with no error, no
alert, and no obvious way to tell which one is "right" without re-deriving
both definitions from scratch. **Production implication:** every metric
in a real metrics catalog needs enough precision that two engineers
implementing it independently would produce identical SQL — the catalog
above is written to that standard (e.g. "known users only" is stated
explicitly on the one row where it applies, and left off everywhere else
on purpose).

### Production Considerations

| Aspect | This repo (POC) | Production |
|---|---|---|
| Catalog format | A Markdown table in this guide | Often a dedicated metrics layer/semantic tool (e.g. a metrics-definition YAML consumed by a BI semantic layer) so the definition is enforced in code, not just documented |
| Ownership | Implicit (this project) | Each metric has a named business owner who signs off on its definition |
| Change management | None — this is Phase 1 | A metric definition change is itself a reviewable, versioned change, since dashboards built against the old definition silently drift otherwise |
| Ambiguity detection | Manual review (this section) | Automated: a metrics layer that only allows one definition per metric name, compile-time |

### Principal Data Engineer Perspective

The judgment call worth naming here: a metrics catalog is cheap to write
and easy to skip, and skipping it never produces an error — it produces a
project that *looks* done (dashboards render, numbers appear) while
quietly carrying unresolved ambiguity that surfaces only when two numbers
disagree in front of a stakeholder. A principal engineer treats "we have
dashboards" and "we have agreed-upon metric definitions" as two different
claims, and doesn't let the first one stand in for the second — this
section exists specifically so Sections 8-11's design decisions have
something concrete to be *for*, rather than being justified purely by
internal modeling elegance.

### Principal Engineer Interview Questions

**Q: "Someone asks you why the dashboard's 'clicks per URL' number doesn't
match a number a colleague pulled from the raw event table. Walk through
how you'd debug that."**

*What's tested:* whether the candidate defaults to a data-quality
investigation (checking the pipeline for bugs) or first checks whether the
two numbers are even supposed to match.

*What a weak answer looks like:* immediately assuming a bug in the
ingestion or transform pipeline and starting to trace data lineage.

*What a strong answer covers:* start by comparing the two *definitions*,
not the two pipelines — ask what filter conditions, date ranges, and
grain each query used before assuming either is "wrong." In this
project's own catalog, the most likely culprit for exactly this
disagreement is the anonymous-clicks question from this section's Failure
Scenario: one query included anonymous clicks, the other implicitly
excluded them via an inner join to a users table. Only after confirming
the definitions are supposed to match does a genuine data-quality bug
become the next hypothesis.

*Concepts:* metric definition ambiguity vs. data-quality defects — two
different failure classes that produce identical symptoms (numbers don't
match) and need different debugging approaches.

*Expected follow-up:* "How would you prevent this from happening again?" —
A written, precise metrics catalog (this section), ideally enforced by a
semantic layer rather than left to convention.

*Common mistake:* treating every numeric discrepancy as a pipeline bug by
default, without first checking whether the two queries were ever
computing the same thing.

---

## 8. Data Grain ✅✅

### Concept

The **grain** of a fact table is a precise statement of what a single row
represents — "one row per X." Every fact table needs exactly one grain
statement, decided before a single column is designed, because grain
determines which measures are valid (additive at that grain or not) and
which dimensions can join to it cleanly.

### Why does this exist?

Grain is, by wide consensus in dimensional modeling (Kimball's own stated
"first and most important" decision when designing a fact table), the
decision every other fact-table decision depends on. Pick a grain that's
too coarse (e.g. "one row per URL per day") and you permanently lose the
ability to answer device-type or hour-of-day questions without
re-extracting from source. Pick a grain that's ambiguous (some rows are
per-click, others are pre-aggregated) and *no* measure in the table is
safely additive anymore, because summing across rows of different grains
double-counts or under-counts depending on which rows you happen to
include.

### Simple Example (generic, pre-URL-Shortener)

A retail chain's "sales" fact table could be grained at "one row per
individual item scanned at checkout," "one row per checkout transaction
(receipt)," or "one row per store per day." All three are legitimate fact
tables — they just answer different questions. The item-level grain can
answer "what's our best-selling SKU" but is enormous; the daily-store
grain is compact but can never answer "what did this specific customer
buy in one visit," because that information was discarded before the
table was even built. Once a grain is chosen and loaded, there is no SQL
query that recovers detail the grain didn't keep.

### URL Shortener Example

`clicks` (Section 1.5, ADR-008) already exists at the finest grain this
project's schema can produce: one row per redirect event. Section 7's
metrics catalog was built assuming that grain is preserved all the way
into the analytical layer — "clicks by device type" and "clicks by hour"
both require row-level detail that a pre-aggregated "clicks per URL per
day" fact table would have already thrown away.

### 8.1 Design Decision

**`fact_clicks`' grain is one row per click event — identical to the
source `clicks` table's own grain.** No pre-aggregation happens between
Bronze and the analytical layer in Phase 1. This is stated here as its own
decision, separate from (but paired with) Section 7's requirements
decision, because grain is a big enough decision in dimensional modeling
to warrant being named and defended on its own, independent of which
specific metrics happen to be in the current catalog — a *future* metric
this project hasn't thought of yet is far more likely to be answerable if
the grain stayed atomic than if it didn't.

### Alternatives

1. **Daily grain: one row per (`short_code`, day) (rejected).** Would
   answer "clicks per URL per day" directly and compactly, but permanently
   loses device-type, anonymous-vs-known, and hour-of-day detail — three
   of Section 7's eight catalog metrics become unanswerable without
   re-extracting from Bronze.
2. **Hourly grain: one row per (`short_code`, hour) (rejected).**
   Recovers hour-of-day answerability but still loses device-type and
   anonymous-vs-known detail — same fundamental problem, one level less
   severe.
3. **Atomic grain: one row per click (chosen).** Every metric in Section
   7's catalog — including hypothetical future ones not yet in the
   catalog — is answerable by aggregating up from this grain. The cost is
   size: `fact_clicks` has as many rows as `clicks` itself, which is
   exactly the volume Section 15's incremental load already exists to
   manage the extraction cost of.

### Trade-offs

| | Atomic (event) grain (chosen) | Pre-aggregated (daily/hourly) grain |
|---|---|---|
| Answerable questions | Any aggregation the raw data supports, including ones not yet in the catalog | Limited to whatever was decided at design time — permanently |
| Row count | Grows with total click volume | Grows much more slowly (bounded by distinct URL × time-bucket combinations) |
| Query cost for coarse questions (e.g. "clicks this month") | Higher — must aggregate many rows at query time | Lower — the aggregation is already done |
| Reversibility | Fully reversible — any coarser grain can be derived from this one later | Not reversible — detail lost at load time can't be recovered |

### 8.2 Architecture

```mermaid
flowchart LR
    subgraph Source["clicks (source, grain = 1 row/click)"]
        C[(clicks)]
    end
    subgraph Bronze["Bronze (Section 14/15, grain preserved)"]
        B[(bronze/clicks/...<br/>Parquet, 1 row/click)]
    end
    subgraph Analytics["fact_clicks (Section 10/11, grain preserved)"]
        F[(fact_clicks<br/>1 row/click)]
    end

    C -->|full + incremental extract| B
    B -->|Phase 2 transform, not yet built| F
```

Grain is preserved end to end, deliberately — nothing between source and
the analytical layer ever aggregates. Aggregation, when it's needed, is
Section 7's live `GROUP BY` at query time, not a load-time transformation
step.

### 8.3 Hands-on Exercise

**LAB 7 — Prove a grain violation would break additivity.**

Without running any code, work through this on paper: suppose
`fact_clicks` were instead grained at one row per (`short_code`, day),
with a `click_count` column holding that day's total. Now suppose a
second, *separate* process also inserted one row per individual click
(perhaps someone added "detailed" rows later without changing the design).
`SUM(click_count)` over the table now double-counts every day that has
both a daily rollup row and its underlying detail rows — and there is
nothing in the table's schema that would catch this, because both kinds
of rows look identical (same columns, same table). Write down, in your own
words, what a `NOT NULL` grain-identifying column (e.g. a `grain` flag)
would need to look like to make this mistake structurally
impossible instead of merely undocumented. (This is exactly why
`fact_clicks` is committed to a single, explicit grain rather than trying
to serve multiple grains "flexibly" in one table.)

### Failure Scenario

**What happens if a future contributor, trying to speed up a slow
dashboard, adds a second set of pre-aggregated rows directly into
`fact_clicks` instead of creating a separate summary table?**

Every existing query that does `SUM(click_count)` or `COUNT(*)` over
`fact_clicks` silently starts producing inflated numbers the moment mixed
grain rows exist together, with no error, no constraint violation, and no
visible signal beyond "the totals look too high" — exactly the ambiguous-
grain failure from the Simple Example, now made concrete for this
project's own table. **Recovery:** requires identifying and removing the
wrongly-inserted rows, which is only possible if there's *some*
distinguishing signal (a load-batch id, a `source` column, or — better —
never having mixed grains in the same table in the first place).
**Production implication:** this is precisely why the correct fix for "the
dashboard is slow" is a *separate*, explicitly-named summary table (e.g. a
future `agg_clicks_daily`) rather than rows-with-a-different-grain
appended into `fact_clicks` — see Section 7's rejected Alternative #2 for
where that summary table would fit if the performance need becomes real.

### Production Considerations

| Aspect | This repo (POC) | Production |
|---|---|---|
| Grain enforcement | None beyond code review / this document | Often a `CHECK` constraint or a dedicated grain-identifying column when a table's grain is even slightly ambiguous; better still, physically separate tables per grain (this project's own choice) |
| Aggregation strategy | Live `GROUP BY` at query time (Section 7) | Same for ad hoc analysis; materialized/pre-aggregated views or a separate summary fact table for known, high-frequency queries once volume justifies it |
| Grain changes over time | Not expected to change in Phase 1 | A grain change is effectively a new fact table — real systems version fact tables (e.g. `fact_clicks_v2`) rather than silently reinterpreting an existing one |

### Principal Data Engineer Perspective

Grain is the decision most likely to be gotten wrong quietly, because a
table with the wrong or ambiguous grain still runs, still returns numbers,
and still *looks* like a working fact table right up until someone sums
the wrong column and gets a number nobody catches as implausible. The
discipline worth internalizing here isn't "pick atomic grain always" —
sometimes a coarser grain is the right call, e.g. when source volume makes
atomic-grain storage genuinely infeasible — it's that the grain decision
must be made explicitly, once, stated in one sentence per fact table
("`fact_clicks`: one row per click event"), and never silently violated
by a later addition. Kimball's own framing — grain first, before a single
column is designed — is echoed directly in how this section precedes
Section 10's actual column-by-column design.

### Principal Engineer Interview Questions

**Q: "You're handed an existing fact table with no documentation. How do
you determine its grain?"**

*What's tested:* whether the candidate has a concrete, repeatable method,
versus a hand-wavy "look at the columns."

*What a weak answer looks like:* "I'd look at the column names and guess"
— unreliable; column names don't reliably reveal grain (a table can have a
`click_count` column at either atomic or aggregated grain).

*What a strong answer covers:* find the smallest set of columns that,
together, uniquely identify a row (in this project, `click_id` alone
already does that) — if no single-column or small combination of columns
is unique, the table's grain is ambiguous or the data itself violates its
own intended grain, which is itself a serious finding. Cross-check by
sampling: pick one dimension value (e.g. one `short_code`) and manually
verify the row count and values make sense for the grain you've
hypothesized (e.g. "does this URL's row count roughly match how many times
I'd expect it to have been clicked, not how many days it's existed").

*Concepts:* grain identification via candidate key discovery, sanity-
checking a hypothesis against sampled data rather than trusting column
names alone.

*Expected follow-up:* "What would make you suspect the table's grain is
actually mixed or violated?" — Duplicate values in what should be a unique
identifying column, or measures that don't sum sensibly when grouped by an
attribute that should partition the data cleanly.

*Common mistake:* assuming a `COUNT`-like column name (e.g. `click_count`)
proves the grain is pre-aggregated, without checking whether that column
is simply always `1` at an atomic grain (as `fact_clicks.click_count` is
in this project — see Section 10.5).

---

## 9. Source Data Model ✅✅

### Concept

The **source data model** is a formal, column-by-column description of
exactly what's being extracted from — every table, every column's type
and nullability, every quirk worth knowing before writing a query against
it. It's documentation, but of a specific and narrow kind: not "how the
application works" (Section 1) and not "what the analytical layer looks
like" (Section 10) — specifically, precisely, "what does the data this
pipeline reads actually look like, right now."

### Why does this exist?

Section 1 already covered the real application's architecture and the
honest gaps in its schema at a narrative level. This section exists
because narrative understanding and a *formal, per-column reference* serve
different purposes: Section 1 is read once, to understand the system;
this section (and the files it's built from) is referenced constantly,
every time someone writes a query, designs a dimension, or debugs a
contract violation (Section 12). Without it, that per-column detail lives
only in people's heads or has to be re-derived from `\d table_name` every
time — exactly the kind of tribal knowledge a data platform should never
depend on.

### Simple Example (generic, pre-URL-Shortener)

A new engineer joins a team and is asked to write a query against a
`payments` table. Without a source data model, they run `\d payments`,
see a `status` column, and guess it's a free-text field — only to
discover, after a failed join, that `status` is actually a foreign key to
a `payment_statuses` lookup table with five specific values, one of which
means "reversed, do not count as revenue." A five-minute read of a
documented source data model would have surfaced that before the wrong
query ever ran; instead it was discovered by getting a wrong number and
working backwards.

### URL Shortener Example

This project's source data model already exists as three files:
[`schemas/source/urls.md`](../schemas/source/urls.md),
[`schemas/source/users.md`](../schemas/source/users.md), and
[`schemas/source/clicks.md`](../schemas/source/clicks.md) — one per
table, each with a full column table, an explicit grain statement, and a
"known quirks" section calling out exactly the kind of trap the Simple
Example describes (e.g. `clicks.md`'s note that `occurred_at` is business
time while Bronze's `ingestion_date=`/`watermark_start=...` partitioning
is processing time — two different clocks that are easy to conflate).
This guide doesn't repeat their content here; it explains *why* they exist
as their own artifacts and how they relate to everything else in this
project.

### 9.1 Design Decision

**The source data model lives as three standalone Markdown files under
`schemas/source/`, one per table, formalized to a consistent structure
(column table, grain, known quirks) — not as prose embedded in this
guide, and not merged into one document per source system.** Kept
separate from this guide because these files are referenced far more
often, by far more specific queries ("what's the exact nullability of
`urls.expires_at`?"), than the guide's own narrative sections are — a
reference document optimized for lookup reads differently than a document
optimized for a linear read. Kept one-file-per-table, rather than one
combined file, so a change to `clicks`' documented shape (e.g. adding a
new column later) touches exactly one file, not a section carved out of a
larger one.

### Alternatives

1. **No formal source data model — rely on Section 1's narrative writeup
   alone (rejected).** Section 1 already explains urls' gaps and the
   users/clicks hypothetical-table decision at a conceptual level, but
   doesn't give a queryable, glanceable per-column reference — exactly the
   gap the Simple Example illustrates.
2. **One combined `schemas/source/README.md` covering all three tables
   (rejected).** Would centralize everything in one place, at the cost of
   every table's documentation competing for space and growing awkward as
   more tables are added (a real concern once Phase 2+ potentially
   extracts from more source tables).
3. **Per-table files, formalized structure, cross-linked from this guide
   and from `contracts/source/*.yaml` (chosen).** Scales cleanly with the
   number of source tables and keeps each file focused.

### Trade-offs

| | Per-table Markdown files (chosen) | Inline in this guide |
|---|---|---|
| Discoverability for "what does column X look like" | High — one file, one table, `Ctrl+F` | Lower — buried inside a much longer document |
| Guide length / focus | This guide stays focused on concepts and design narrative | Guide would grow by hundreds of lines of pure reference material |
| Risk of drift from `contracts/source/*.yaml` (Section 12) | Two artifacts to keep in sync (mitigated: contracts are machine-checked against the real DB; these files aren't, yet) | Same risk, just harder to spot in a longer document |

### 9.2 How the pieces fit together

| Artifact | Purpose | Checked how |
|---|---|---|
| `schemas/source/*.md` | Human-readable, narrative-plus-table reference per source table | Manually reviewed; not machine-checked |
| `sql/source/*.sql` | The actual DDL — ground truth for what Postgres will create | Applied for real by `make up` |
| `contracts/source/*.yaml` | Machine-checked contract — declares the expected shape and validates it against the live database | `make validate-contracts` (Section 12) |

Three artifacts, three different jobs — a human reading to understand, a
database enforcing structure, and an automated check catching drift
between the two. Section 12 covers the third in depth.

### 9.3 Hands-on Exercise

**LAB 8 — Find a documented quirk before it bites you.**

Without looking at `object_store.py` or `extract_incremental.py` again,
open `schemas/source/clicks.md` and answer: which column does Section
15's watermark actually use, and which column would a newcomer likely
*guess* it uses if they only looked at the table's column names without
reading the "known quirks" section? (Expected answer: the watermark uses
`id`, not `occurred_at` — despite `occurred_at` being the column that
*sounds* like the natural choice for anything watermark/time-related. This
is precisely the kind of mismatch between intuition and actual behavior a
source data model's "known quirks" section exists to catch before it
causes a bug.)

### Failure Scenario

**What happens if `sql/source/002_hypothetical_users_and_clicks.sql`
changes (say, a column is added) but `schemas/source/clicks.md` is never
updated to match?**

Nothing enforces the two stay in sync — `schemas/source/*.md` files are
plain Markdown, not validated against the database the way
`contracts/source/*.yaml` is (Section 12). The documented model silently
becomes stale: a reader trusts `clicks.md`'s column table, writes a query
or a contract assuming it's complete, and either misses the new column
entirely or — worse — doesn't know a *new* quirk exists because nothing
was written about it. **Production implication:** this is exactly the gap
`contracts/source/*.yaml` closes for the machine-checkable subset of this
information (column existence, type, nullability) — Section 12's
validator would catch a schema drift the Markdown docs can't. What it
*wouldn't* catch: a change to a quirk's underlying behavior that doesn't
change the schema at all (e.g. if `occurred_at` started being set by a
different, less reliable process) — that class of drift still depends on
someone updating the prose by hand, which is a real, named limitation, not
a hidden one.

### Production Considerations

| Aspect | This repo (POC) | Production |
|---|---|---|
| Sync with real schema | Manual — a human updates `schemas/source/*.md` when `sql/source/*.sql` changes | Often generated or partially generated from the database itself (e.g. a script that diffs documented vs. actual columns and flags drift) |
| Ownership | Implicit (this project) | Each source table's documentation typically owned by whichever team owns the system of record |
| Versioning | Git history only | Sometimes paired with a formal schema registry, especially once multiple downstream consumers depend on the same source |

### Principal Data Engineer Perspective

The distinction worth holding onto from this section: a source data model
and a data contract (Section 12) look similar — both describe a table's
shape — but serve fundamentally different roles. The source data model is
optimized for a *human* deciding how to use the data (nullability quirks,
business-vs-processing-time distinctions, "don't guess, read this
first"); the contract is optimized for a *machine* catching drift
automatically. A principal engineer keeps both, deliberately, rather than
trying to make one artifact serve both jobs — a YAML contract could
technically hold prose explanations too, but cramming human-readable
nuance into a file whose primary job is automated comparison makes both
jobs worse.

### Principal Engineer Interview Questions

**Q: "What's the difference between documenting a table's schema and
defining a data contract for it? Why would a team need both?"**

*What's tested:* whether the candidate sees documentation and contracts as
distinct tools with different failure modes, or treats them as the same
thing with different formatting.

*What a weak answer looks like:* "A contract is just documentation in
YAML instead of Markdown" — misses the core distinction (one is checked
automatically, one isn't).

*What a strong answer covers:* documentation is written for a human to
read once and internalize; a contract is written to be checked by a
machine, continuously, against the live system, specifically to catch the
moment documentation (or the schema itself) silently drifts from what a
downstream consumer expects. A team needs both because they catch
different failures: documentation prevents a human from making a wrong
assumption (this project's `occurred_at`-vs-`id` watermark quirk); a
contract prevents an *undetected* schema change from silently breaking
every downstream consumer that assumed the old shape.

*Concepts:* the human-readable vs. machine-checked distinction; schema
drift as a class of failure documentation alone cannot catch.

*Expected follow-up:* "How would you keep the two from drifting apart from
each other?" — Either generate one from the other where possible, or treat
updating both as part of the same change's definition of done (a review
checklist item, not a separate follow-up task).

*Common mistake:* conflating "well-documented" with "safe from schema
drift" — documentation quality says nothing about whether anyone would
notice if the documentation stopped being true.

---

## 10. Analytical Data Model (Fact/Dimension Design) ✅✅

### Concept

**Dimensional modeling** (Kimball-style) organizes analytical data into
**fact tables** (the measurable events — "what happened, how many times,
how much") and **dimension tables** (the descriptive context those events
are analyzed by — "who, what, when, where"). A **fact table** holds
numeric measures at a defined grain (Section 8) plus foreign keys into
dimensions; a **dimension table** holds descriptive attributes, one row
per distinct real-world entity, with a surrogate key the fact table joins
on.

### Why does this exist?

The source schema (Section 9) is optimized for the OLTP application's own
needs — fast single-row lookups and writes, third-normal-form-ish
structure, no redundancy. That shape is actively bad for analytical
queries: answering "clicks by device type, by plan, by day" against a
normalized OLTP schema means joining several tables per query, every
query, at read time. A dimensional model restructures the same
information around how it's actually *queried* — one central fact table,
a handful of dimensions, most analytical questions answerable with one or
two joins — trading write-optimized normalization for read-optimized
predictability. This is the direct sequel to Section 2 (OLTP vs. OLAP):
that section explained *why* the two shapes differ in principle; this
section is where this project's own OLAP shape gets designed.

### Simple Example (generic, pre-URL-Shortener)

A normalized OLTP schema for a video-streaming service might spread "what
did this viewer watch" across `sessions`, `session_events`, `titles`,
`title_genres`, and `viewers` — five tables, several joins, to answer "how
many hours of comedy did premium subscribers watch last month." A
dimensional model for the same question puts one row per viewing event in
a `fact_views` table (with `viewer_key`, `title_key`, `date_key`,
`minutes_watched`) and denormalizes genre directly onto a `dim_title`
dimension — the same question becomes one fact table joined to two small
dimensions, filtered and grouped, no multi-hop join chain required.

### URL Shortener Example

This project's OLTP mirror (`urls`, `users`, `clicks`) is small enough
that the normalization cost is barely noticeable today — but Section 7's
metrics catalog was written assuming a dimensional shape (`GROUP BY
device_type`, `GROUP BY plan_type`, joined to a fact table each time), not
the source schema's own shape. This section designs exactly that: `dim_date`,
`dim_url`, `dim_user`, `dim_device`, and `fact_clicks` — full reference in
[`schemas/analytics/dimensional-model.md`](../schemas/analytics/dimensional-model.md),
DDL in [`sql/analytics/`](../sql/analytics/).

### 10.1 Architecture

```mermaid
erDiagram
    dim_date ||--o{ fact_clicks : date_key
    dim_url ||--o{ fact_clicks : url_key
    dim_user ||--o{ fact_clicks : user_key
    dim_device ||--o{ fact_clicks : device_key
```

(Full column-level diagram, with every field: `schemas/analytics/dimensional-model.md`.)
A classic **star schema** shape — one fact table at the center, each
dimension joined to it directly, never to each other. Section 11 covers
*why* this shape (vs. a normalized/snowflake alternative) in depth; this
section covers what's *in* each table and why.

### 10.2 `dim_date`: a conformed dimension, not derived from source

`dim_date` is the one table in this model with **no source table at
all** — it's generated once, at DDL time, via `generate_series` for a
fixed calendar range (`sql/analytics/001_dim_date.sql`). This is standard
Kimball practice, not a shortcut specific to this project: a date
dimension's content (day-of-week, month name, quarter, is-weekend) is
calendar math, not business data, so there's nothing to "extract" — only
something to generate once and reuse everywhere. It's also this project's
first **conformed dimension**: built independent of any one fact table,
so a future second fact table can join to the exact same `dim_date`
without redefining what a date means.

### 10.3 Design Decision: `fact_clicks.user_key` is never `NULL`

This is the single most consequential decision in this dimensional model,
worth its own callout. `clicks.user_id` is nullable at the source — about
70% of seeded clicks are anonymous. The naive translation would leave
`fact_clicks.user_key` nullable too, with `NULL` meaning "anonymous." This
is a well-known Kimball anti-pattern: most SQL tools and BI clients
generate an `INNER JOIN` by default when joining a fact to a dimension,
and an `INNER JOIN` silently **drops every row with a `NULL` foreign
key** — a dashboard built against `fact_clicks JOIN dim_user` would
silently under-count total clicks by exactly the anonymous share, with no
error and no obviously wrong-looking result (the numbers would just be
quietly too low). **The chosen design:** `dim_user` includes a designated
**Unknown member row** — `user_key = -1`, `is_known = false` — and every
anonymous click's `fact_clicks.user_key` resolves to `-1` instead of
`NULL`. `user_key` becomes `NOT NULL` in the DDL (`sql/analytics/005_fact_clicks.sql`),
and an `INNER JOIN` now returns every row, correctly, with the Unknown
member's attributes (`plan_type = 'UNKNOWN'`) standing in for "no
attributed user" instead of silently vanishing.

### Alternatives

1. **Nullable `user_key`, `NULL` means anonymous (rejected).** Matches the
   source data's own nullability most literally, at the cost of the
   silent-undercount failure mode above — the option every dimensional
   modeling reference explicitly warns against.
2. **`LEFT JOIN` everywhere instead of fixing the data (rejected).**
   Technically avoids the undercount, but only if every single query
   author remembers to use `LEFT JOIN` instead of the tool's default —
   fixing this once, in the data, is far more reliable than fixing it
   correctly in every query, forever.
3. **A designated Unknown/Not-Applicable member row, `NOT NULL` foreign
   key (chosen).** Standard Kimball technique for exactly this situation;
   makes the safe behavior (`INNER JOIN`, the default) also the *correct*
   behavior.

### Trade-offs

| | Unknown-member row, NOT NULL FK (chosen) | Nullable FK, NULL = anonymous |
|---|---|---|
| Default `INNER JOIN` behavior | Correct — every row included | Silently wrong — anonymous rows vanish |
| Query author has to remember anything special | No | Yes — must always `LEFT JOIN` `dim_user` |
| Schema honesty | `dim_user` explicitly models "no known user" as a real, queryable value | `NULL` is overloaded to mean "unknown" with no queryable attributes attached |
| Extra row per dimension | One (the Unknown member) | None |

### 10.4 Design Decision: `dim_user` excludes `email`

`dim_user` deliberately does **not** carry `users.email` through to the
analytical layer. Section 7's entire metrics catalog only ever needs
`plan_type` — no metric groups or filters by individual user identity —
so there is no analytical requirement pulling `email` into this model at
all. Given that, keeping it out is the conservative default: every column
that reaches the analytical layer is one more place PII (Section 23,
formally: `email` is this project's only `direct`-category column) has
to be tracked, access-controlled, and eventually governed;
not including a column nothing needs is simpler than including it and
then having to justify, audit, and restrict it later. This is a "shaped by
actual requirements" decision, in the same spirit as Section 7's
insistence that the model be built for known questions, not
speculative future ones.

### 10.5 `fact_clicks.click_count`: measure design

`click_count` is always `1` at this grain — every row is exactly one
click. Kimball calls this pattern a **factless-fact-adjacent** measure:
technically redundant with `COUNT(*)`, but included explicitly for two
concrete reasons. First, consistency: once a second additive measure ever
gets added to this fact table (e.g. a future "time to redirect" duration),
`SUM(click_count)` and `SUM(new_measure)` read identically in every query
— nobody has to remember "this one measure is `COUNT(*)`, that one is
`SUM(column)`." Second, portability: `SUM(click_count)` behaves
identically whether the query engine is Postgres, a Phase 3 analytical
warehouse, or a BI tool's own aggregation layer, whereas `COUNT(*)`
semantics can subtly differ across engines when combined with certain
join patterns. `short_code` and `occurred_at` are **degenerate
dimensions/attributes** — see [`schemas/analytics/dimensional-model.md`](../schemas/analytics/dimensional-model.md)
for the full reasoning on both.

### 10.6 Hands-on Challenge (implement-yourself)

Before reading Section 11's DDL, try this: **design `dim_url`'s columns
yourself**, using only Section 7's metrics catalog and
`schemas/source/urls.md` as inputs — don't look at
`sql/analytics/002_dim_url.sql` yet. Which source columns does the
catalog actually need? (Hint: check every catalog row that mentions a URL
attribute.) Would you include `deleted_at`? `expires_at`? Then compare
your answer against the committed DDL and this section's design
decisions — where they differ, ask whether your version serves a
requirement the committed one misses, or whether the committed version
deliberately left something out that you included on reflex (a common
outcome: reflexively including every source column "just in case," which
is exactly the anti-pattern Section 10.4's PII decision pushes back
against).

### 10.7 Hands-on Exercise

**LAB 10 — Prove the Unknown member resolves a real join, not just an
insert.**

*(Requires `make up` and `make create-analytics-schema` — DESIGN
EXPECTATION, not yet executed in this sandbox; see Section 11's How to
test for what genuinely was run.)*

```sql
-- dim_user has exactly one row (the Unknown member) before Phase 2 runs.
SELECT user_key, user_id, plan_type, is_known FROM dim_user;
-- Expected: exactly one row -- (-1, -1, 'UNKNOWN', false).

-- Confirm an INNER JOIN against dim_user, even with zero "real" rows
-- loaded yet, still has a member for user_key=-1 to resolve against --
-- the whole point of Section 10.3's design decision.
SELECT du.plan_type, COUNT(*)
FROM (SELECT -1 AS user_key) AS simulated_anonymous_click
INNER JOIN dim_user du ON du.user_key = simulated_anonymous_click.user_key
GROUP BY du.plan_type;
-- Expected: one row, plan_type = 'UNKNOWN', count = 1 -- an INNER JOIN
-- that would have silently dropped this row entirely had user_key been
-- NULL instead of -1.
```

What to observe: the second query is a stand-in for exactly what a BI
tool's default `INNER JOIN` would do against a real `fact_clicks` row for
an anonymous click — and it returns a row, correctly, instead of silently
excluding it. This is Section 10.3's entire design decision, made
concrete and checkable rather than just argued for in prose.

### Failure Scenario

**What happens if a new anonymous-click-heavy source (say, a public API
integration) is added later, and someone forgets the Unknown-member
convention when writing its transform?**

If a new transform path inserts `fact_clicks` rows with `user_key = NULL`
directly (bypassing the Unknown-member lookup), the `NOT NULL` constraint
on `fact_clicks.user_key` (Section 10.3, enforced in
`sql/analytics/005_fact_clicks.sql`) rejects the insert outright — a loud,
immediate failure at load time, not a silent under-count discovered
months later in a dashboard. **This is the entire point of making it a
database-level constraint rather than a convention documented only in
this guide**: a convention can be forgotten; a `NOT NULL` constraint
cannot be silently violated. **Production implication:** every new
data-loading path into a fact table with this pattern must resolve
unknown/missing dimension keys to the designated Unknown member *before*
the insert, and the constraint exists specifically to make skipping that
step impossible rather than merely discouraged.

### Production Considerations

| Aspect | This repo (POC) | Production |
|---|---|---|
| SCD strategy | Type 1 (overwrite) for `dim_url`/`dim_user` — no history tracked | Type 2 (versioned rows with valid-from/valid-to) once change history has real analytical value, and once the source has reliable change-detection (an `updated_at` column, or CDC) |
| Unknown-member pattern | One Unknown row per dimension that needs it (`dim_user`) | Same pattern, applied consistently across every dimension with nullable source foreign keys — a real anti-pattern audit checks for exactly this |
| Schema evolution | Manual DDL changes, reviewed by hand | Often paired with a schema registry / migration tool, and contract validation (Section 12) extended to cover the analytical layer too (explicitly not done yet — see Section 12's Production Considerations) |
| Surrogate key generation | Postgres `SERIAL` | Same idea at any scale; distributed systems sometimes use a dedicated key-generation service instead of a single sequence |

### Principal Data Engineer Perspective

The Unknown-member decision (10.3) is a good example of what
distinguishes a dimensional model that merely *works* from one that's
*correct under the tool's own defaults*. A junior implementation nullable
`user_key` design would pass every test that doesn't specifically check
join behavior — it would load, it would query, it would look done. The
failure only appears the moment someone (very possibly not the original
author) opens a BI tool, drags `fact_clicks` and `dim_user` onto a canvas,
and lets the tool's default join type quietly discard 70% of the
anonymous-click rows from every report built on top of it. A principal
engineer designs for the *tool's default behavior*, not just for what a
carefully-written test suite happens to exercise — which is exactly why
Section 10.3 treats "what does an `INNER JOIN` do by default" as a design
input, not an afterthought.

### Principal Engineer Interview Questions

**Q: "Why does `fact_clicks.user_key` reference a designated 'Unknown'
row instead of allowing `NULL` for anonymous clicks?"**

*What's tested:* whether the candidate knows this specific, well-known
dimensional-modeling pattern and can explain the failure it prevents, not
just recite that "NULLs are bad."

*What a weak answer looks like:* "NULLs are generally best avoided in
databases" — true in general, but doesn't explain *this specific*
mechanism or why it matters here more than anywhere else.

*What a strong answer covers:* most SQL clients and BI tools default to
`INNER JOIN` when joining a fact table to a dimension; a `NULL` foreign
key is excluded by an `INNER JOIN` by definition, so every report built
with the tool's default join silently loses every anonymous-click row —
not an error, just a quietly wrong (too-low) total. A designated Unknown
member row with a `NOT NULL` foreign key makes the default, unthinking
behavior also the correct one.

*Concepts:* the "fact table foreign keys should never be NULL" Kimball
principle; default join semantics as a design constraint, not just a
query-writing concern.

*Expected follow-up:* "How would you retrofit this onto an existing fact
table that already has NULL foreign keys in production?" — Add the
Unknown member row, backfill existing NULLs to reference it, then add the
`NOT NULL` constraint — in that order, since adding the constraint before
the backfill would simply fail.

*Common mistake:* treating this as a generic "NULLs are bad practice"
talking point without connecting it to the concrete, silent-undercount
failure mode it specifically prevents in a star schema.

---

## 11. Star Schema vs. Normalized Model ✅✅

### Concept

A **star schema** is a dimensional model where every dimension joins
directly to the fact table, never to another dimension — the shape Section
10 already designed. A **normalized model** (third normal form, roughly
what `urls`/`users`/`clicks` already are) eliminates data redundancy by
splitting attributes into their own tables, related by foreign keys,
however many hops deep that takes. A **snowflake schema** is a middle
ground: a star schema where some dimensions are further normalized into
sub-dimensions.

### Why does this exist?

Section 10 designed `fact_clicks` and its dimensions without stopping to
justify the star *shape* itself — this section closes that gap
explicitly, because "star vs. normalized" is a decision every analytical
data model has to make once, and the two shapes trade off correctness
guarantees and storage efficiency (normalized) against query simplicity
and read performance (star) in ways worth understanding on their own
terms, not just inheriting by convention.

### Simple Example (generic, pre-URL-Shortener)

A normalized `dim_url`-equivalent might split "destination domain" into
its own `domains` table, referenced by `url_id`, to avoid repeating domain
strings across rows (classic normalization: eliminate redundancy). A star
schema instead denormalizes `original_url_domain` directly onto `dim_url`
itself — some repeated text, but "clicks by domain" becomes a single join
to `fact_clicks` instead of two. At OLTP write-heavy scale, normalization
wins (less duplicated data to keep consistent on update); at OLAP
read-heavy analytical scale, the star schema usually wins (fewer joins per
query, and dimension tables are small enough that the storage cost of
denormalization is negligible).

### URL Shortener Example

This project's own `urls`/`users`/`clicks` OLTP schema is *already*
essentially normalized (Section 2's OLTP vs. OLAP distinction, applied
concretely) — `user_id` on `urls` and `clicks` is a foreign key, not a
duplicated `plan_type` string. Section 10's `dim_url`/`dim_user`/`dim_device`
design deliberately denormalizes: `fact_clicks` carries `short_code`
directly (Section 10.5's degenerate-dimension decision), and `dim_user`
flattens `plan_type` directly onto each user row rather than referencing a
separate `plans` lookup table — a snowflake alternative this section
explicitly considers and rejects below.

### 11.1 Design Decision

**This project uses a pure star schema — no snowflaking.** `dim_user`
keeps `plan_type` as a plain column rather than a foreign key to a
separate `plans` dimension; `dim_url` keeps `original_url_domain` as a
plain derived column rather than a foreign key to a `domains` dimension.
Every dimension in this model joins directly to `fact_clicks` and to
nothing else.

### Alternatives

1. **Fully normalized analytical layer — reuse the OLTP shape as-is,
   analytical queries join across `urls`/`users`/`clicks` directly
   (rejected).** No transformation pipeline needed at all, but every
   metric in Section 7's catalog requires multiple joins, and the
   OLTP tables aren't optimized for the read patterns analytical queries
   actually have (Section 2's entire argument, applied here).
2. **Snowflake schema — normalize `plan_type` into its own `dim_plan`
   table, `original_url_domain` into its own `dim_domain` table
   (rejected for Phase 1).** Would eliminate the small amount of
   redundancy a plain-column `plan_type`/`domain` carries (only 2 distinct
   plan values, a modest number of distinct domains in the seeded data),
   at the cost of one more join for every query that needs those
   attributes — not worth it at this cardinality. Worth reconsidering only
   if a dimension's low-cardinality attribute set grows large and
   genuinely shared across multiple fact tables.
3. **Pure star schema (chosen).** Every catalog metric in Section 7 is
   answerable with `fact_clicks` joined to at most one dimension — the
   simplest shape that fully serves the known requirements, per Section
   7's own "build for known questions" principle.

### Trade-offs

| | Star schema (chosen) | Normalized / snowflake |
|---|---|---|
| Joins per typical query | One (fact → one dimension) | Two or more (fact → dimension → sub-dimension, or fact → several OLTP tables) |
| Storage redundancy | Some (e.g. `plan_type` repeated once per `dim_user` row — negligible at this scale) | Minimal — every value stored once |
| Write complexity | N/A in Phase 1 (this schema isn't written to yet — Phase 2 builds the transform) | N/A here for the same reason, but normalized models generally make single-row updates cheaper |
| Query simplicity for analysts / BI tools | High — matches how most BI tools expect a model to look | Lower — more joins to configure correctly in every tool |

### 11.2 Implementation

The DDL for all five tables is committed at
[`sql/analytics/`](../sql/analytics/), one file per table, applied
in filename order (`001_dim_date.sql` through `005_fact_clicks.sql` — the
numeric prefix encodes dependency order: `dim_date`/`dim_user`/`dim_device`
before `fact_clicks`, which references all of them via foreign key).

**Implementation Guide vs. Reference Implementation:** if you want the
hands-on version, read the *Implementation Guide* below, close this
guide, and write the DDL yourself from
`schemas/analytics/dimensional-model.md`'s column reference — then
compare against the committed files. If you want to study the finished
schema directly, the excerpts below are exactly what's committed.

---

**CREATE:** `sql/analytics/001_dim_date.sql` through `005_fact_clicks.sql`

**PURPOSE:** Define the star schema (Section 10's design, made real) —
schema only in Phase 1; Phase 2's transform is what actually populates
`dim_url`, `dim_device`'s usage, and `fact_clicks` from Bronze.

**DEPENDENCIES:** A running Postgres (`make up`); no Python dependency —
this is pure DDL, applied with `psql`.

**IMPLEMENTATION GUIDE (write it yourself):** start from
`schemas/analytics/dimensional-model.md`'s column tables. For `dim_date`,
the only real design choice is the key encoding — use `YYYYMMDD` as an
`INTEGER` (Kimball convention: sorts and joins cheaply, human-readable
without a join), and populate it with one `INSERT ... SELECT` over
`generate_series` for a fixed range, rather than a loop or an external
script — Postgres can generate an entire decade of dates in one
statement. For `dim_user`, remember Section 10.3's constraint: the Unknown
member row (`user_key = -1`) must be inserted as part of the DDL itself
(`ON CONFLICT DO NOTHING` makes the insert idempotent across reruns), not
left for the transform to create later — a `fact_clicks` row referencing
`user_key = -1` must always be able to resolve, even before Phase 2's
transform has ever run. For `fact_clicks`, use `click_id` (the source
`clicks.id`) directly as the primary key rather than a separate surrogate
— at this grain, the natural key is already unique and stable, so a
surrogate key would add nothing (contrast this with `dim_url`/`dim_user`,
where a surrogate key genuinely earns its keep across SCD changes). Make
every foreign key `NOT NULL`.

**REFERENCE IMPLEMENTATION:**

```sql
-- sql/analytics/001_dim_date.sql (excerpt -- full file already committed)
CREATE TABLE IF NOT EXISTS dim_date (
    date_key     INTEGER PRIMARY KEY,      -- YYYYMMDD
    full_date    DATE NOT NULL UNIQUE,
    day_of_week  SMALLINT NOT NULL,
    is_weekend   BOOLEAN NOT NULL
    -- ... day_name, month, month_name, quarter, year (see full file)
);

INSERT INTO dim_date (date_key, full_date, day_of_week, is_weekend, ...)
SELECT CAST(to_char(d, 'YYYYMMDD') AS INTEGER), d, EXTRACT(DOW FROM d)::SMALLINT,
       EXTRACT(ISODOW FROM d) IN (6, 7), ...
FROM generate_series(DATE '2020-01-01', DATE '2030-12-31', INTERVAL '1 day') AS d
ON CONFLICT (date_key) DO NOTHING;
```

```sql
-- sql/analytics/003_dim_user.sql (excerpt)
CREATE TABLE IF NOT EXISTS dim_user (
    user_key   SERIAL PRIMARY KEY,      -- -1 reserved for the Unknown member
    user_id    BIGINT NOT NULL UNIQUE,  -- natural key; -1 for the Unknown member
    plan_type  VARCHAR(16) NOT NULL,
    is_known   BOOLEAN NOT NULL
);

INSERT INTO dim_user (user_key, user_id, plan_type, is_known)
VALUES (-1, -1, 'UNKNOWN', false)
ON CONFLICT (user_key) DO NOTHING;
```

```sql
-- sql/analytics/005_fact_clicks.sql (excerpt)
CREATE TABLE IF NOT EXISTS fact_clicks (
    click_id     BIGINT PRIMARY KEY,     -- natural key, source clicks.id
    date_key     INTEGER NOT NULL REFERENCES dim_date (date_key),
    url_key      INTEGER NOT NULL REFERENCES dim_url (url_key),
    user_key     INTEGER NOT NULL REFERENCES dim_user (user_key),
    device_key   INTEGER NOT NULL REFERENCES dim_device (device_key),
    short_code   VARCHAR(16) NOT NULL,
    occurred_at  TIMESTAMPTZ NOT NULL,
    click_count  SMALLINT NOT NULL DEFAULT 1
);
```

Full files: [`sql/analytics/`](../sql/analytics/).

**RUN:**

```bash
make up
make create-analytics-schema
```

**VERIFY:**

```bash
make db-shell
\dt                      -- confirm dim_date, dim_url, dim_user, dim_device, fact_clicks exist
SELECT COUNT(*) FROM dim_date;   -- expect 4,018 (2020-01-01 through 2030-12-31, inclusive)
SELECT * FROM dim_user WHERE user_key = -1;  -- confirm the Unknown member exists
SELECT * FROM dim_device;        -- expect exactly 4 rows: mobile, desktop, tablet, unknown
```

**EXPECTED:** five tables exist; `dim_date` has one row per calendar day
in the generated range; `dim_user` has exactly one row (the Unknown
member) until Phase 2's transform runs; `dim_device` has exactly its four
seeded rows; `dim_url` and `fact_clicks` are empty until Phase 2.

**ACTUAL OBSERVED result:** this sandbox has no Docker daemon, but it
does have a real, locally installed Postgres 16 — all five DDL files were
applied against it directly (`psql`, not through `docker-compose.yml`,
since that specifically needs Docker) while writing this section:

```
=== applying sql/analytics/001_dim_date.sql ===
CREATE TABLE
COMMENT
INSERT 0 4018
=== applying sql/analytics/002_dim_url.sql ===
CREATE TABLE
COMMENT
CREATE INDEX
=== applying sql/analytics/003_dim_user.sql ===
CREATE TABLE
COMMENT
INSERT 0 1
=== applying sql/analytics/004_dim_device.sql ===
CREATE TABLE
COMMENT
INSERT 0 4
=== applying sql/analytics/005_fact_clicks.sql ===
CREATE TABLE
COMMENT
CREATE INDEX
CREATE INDEX
CREATE INDEX
CREATE INDEX
```

`dim_date` really does hold exactly 4,018 rows (verified computationally:
`(date(2030,12,31) - date(2020,1,1)).days + 1 == 4018`, then confirmed
with `SELECT COUNT(*) FROM dim_date` against the real table); `dim_user`
holds exactly the one Unknown-member row; `dim_device` holds exactly its
four seeded rows. This is genuinely run, not projected — the one gap
still open is validating it through `docker-compose.yml` and
`make create-analytics-schema` specifically, since this sandbox can apply
the DDL directly but can't run Docker.

**TEST:** no automated test yet exercises this DDL directly (it's pure
schema, no Python code to unit test) — Section 12's contract validator is
the mechanism that will eventually catch drift here too, once the
analytical layer is added to its scope (see Section 12's Production
Considerations for why that's explicitly not done in Phase 1).

**PRODUCTION CONSIDERATIONS:** see Section 10's Production Considerations
table — the same SCD/schema-evolution concerns apply here, since this IS
that schema.

**INTERVIEW QUESTIONS:** see this section's own, below.

---

### Hands-on Challenge (implement-yourself)

Before reading further, try this: **write the DDL for a hypothetical
`dim_plan` table** (the snowflake alternative rejected in this section's
Alternatives), and the corresponding change to `dim_user` (replacing
`plan_type VARCHAR(16)` with `plan_key INTEGER REFERENCES dim_plan`).
Then write the query for "clicks by plan type" against *both* versions of
the schema — the current star-schema one and your snowflaked one — and
count the joins each requires. This is the entire star-vs-snowflake
trade-off, made concrete in one exercise: how much duplicate data (2
repeated `plan_type` strings per distinct value across every `dim_user`
row) is worth avoiding, against how much query complexity that avoidance
costs.

### 11.3 Hands-on Exercise

**LAB 9 — Confirm the Unknown member resolves correctly.**

After `make create-analytics-schema`:

```sql
-- Simulates what a Phase 2 transform's insert would look like for one
-- anonymous click, without Phase 2 actually existing yet.
INSERT INTO fact_clicks (click_id, date_key, url_key, user_key, device_key, short_code, occurred_at)
VALUES (999999, 20260919, 1, -1, 1, 'test01', now());
-- This should succeed -- user_key=-1 resolves to the Unknown member.

INSERT INTO fact_clicks (click_id, date_key, url_key, user_key, device_key, short_code, occurred_at)
VALUES (999998, 20260919, 1, NULL, 1, 'test02', now());
-- This should FAIL -- user_key is NOT NULL. Expected error: null value in
-- column "user_key" violates not-null constraint.
```

*(This exercise requires `dim_url` to have at least one row with
`url_key = 1` and `dim_device` a row with `device_key = 1` to satisfy the
other foreign keys — insert placeholder rows first if testing this before
Phase 2 populates them for real.)*

**ACTUAL OBSERVED result:** this sandbox turned out to have a local
Postgres 16 available after all (not Docker, but real Postgres) — this
exact exercise was run against it while writing this section. The first
insert succeeded; the second failed with exactly the predicted error:

```
ERROR:  null value in column "user_key" of relation "fact_clicks" violates not-null constraint
DETAIL:  Failing row contains (999998, 20260919, 1, null, 1, test02, 2026-09-19 17:49:53.775691+00, 1, 2026-09-19 17:49:53.775691+00).
```

### Failure Scenario

**What happens if Phase 2's transform is written against a snowflaked
schema (say, someone "improves" `dim_user` into `dim_user` + `dim_plan`
without updating this section or `schemas/analytics/dimensional-model.md`)?**

Every existing query written against the star-schema assumption (`dim_user.plan_type`
as a plain column, per Section 7's catalog) breaks — not silently, since
`plan_type` would no longer exist on `dim_user` at all and every such
query would fail with an explicit "column does not exist" error. This is
actually the *good* version of a schema-shape failure (loud, not silent) —
contrast with Section 10.3's Unknown-member scenario, where the failure
mode without a constraint would have been silent. **Production
implication:** a schema-shape decision like star-vs-snowflake, once
downstream queries and dashboards depend on it, is a breaking change to
reverse — exactly why this section documents the decision explicitly
rather than leaving it as an implicit consequence of however Phase 2 ends
up being written.

### Production Considerations

| Aspect | This repo (POC) | Production |
|---|---|---|
| Schema shape | Pure star, no snowflaking | Same principle at scale; snowflaking reconsidered only for genuinely large, genuinely shared low-cardinality attribute sets |
| Query tooling assumptions | None yet (Phase 3 builds analytical serving) | BI tools generally assume and are optimized for star schemas specifically — a snowflake schema often needs explicit join configuration per tool |
| Schema versioning | Numeric filename prefixes (`001_`, `002_`, ...), applied in order | Often a formal migration tool (e.g. Alembic, Flyway) once the schema changes after initial creation, rather than idempotent `CREATE TABLE IF NOT EXISTS` files |

### Principal Data Engineer Perspective

Star vs. snowflake is one of the rare dimensional-modeling decisions where
the "textbook-correct" answer is genuinely context-dependent rather than
universal — Kimball's own guidance favors stars for BI-tool
compatibility, but a genuinely large, genuinely shared dimension attribute
set (imagine hundreds of thousands of `plan_type`-equivalent rows shared
across a dozen fact tables) can make snowflaking the more defensible
choice. What matters, and what this section tries to model, is having an
explicit, cardinality-and-requirements-based reason for the choice made —
"we chose star because our dimension attributes are low-cardinality and
this data model only currently has one fact table" is a defensible,
revisitable decision; "we chose star because that's just what dimensional
models look like" is not a decision at all, just an unexamined default.

### Principal Engineer Interview Questions

**Q: "When would you choose a snowflake schema over a pure star schema?
Give a concrete scenario, not just a definition."**

*What's tested:* whether the candidate can reason about the trade-off
contextually, versus reciting that "star schemas are simpler, snowflakes
save space."

*What a weak answer looks like:* "Snowflake schemas normalize dimensions
to save space" — a correct definition that stops short of saying *when*
that trade is actually worth making.

*What a strong answer covers:* a concrete scenario where a dimension's
attribute has high enough cardinality, and is shared across enough
distinct fact tables, that keeping it denormalized would mean repeating
large amounts of the same data redundantly in every dimension row across
every one of those fact tables — e.g. a `dim_product` with a
deeply-nested category hierarchy (thousands of categories, shared across
sales, returns, and inventory fact tables) is a much stronger snowflaking
case than this project's `dim_user.plan_type` (two values, one fact
table).

*Concepts:* cardinality and fan-out as the actual deciding factors, not a
blanket rule; conformed dimensions shared across multiple fact tables as
the scenario where normalization's benefit compounds.

*Expected follow-up:* "What does snowflaking cost you, concretely, in
query terms?" — One additional join per snowflaked attribute, in every
query that needs it, and additional complexity configuring that join
correctly in whatever BI tool sits on top.

*Common mistake:* treating "star schema" and "snowflake schema" as a
binary style preference rather than a decision driven by the actual
cardinality and fan-out of the specific data being modeled.

---

## 12. Data Contracts ✅✅

### Concept

A **data contract** is a formal, versioned, machine-checkable agreement
about a table's shape — its columns, their types, their nullability — plus
metadata a human needs (an owner, a freshness expectation, a plain-English
description) that a schema alone doesn't carry. The defining feature that
separates a contract from documentation (Section 9) is that a contract is
*validated automatically* against the real, live system, not just read and
trusted.

### Why does this exist?

Section 9 named this gap directly: `schemas/source/*.md` is accurate right
up until the moment `sql/source/*.sql` changes and nobody updates the
Markdown to match — nothing catches that drift. A data contract closes
exactly that gap for the machine-checkable subset of what a source data
model documents (columns, types, nullability) by making "does the real
table still match what I expect" a command you can run, not a question you
have to trust a human kept current.

### Simple Example (generic, pre-URL-Shortener)

A downstream analytics team builds a dashboard against an `orders` table,
assuming `discount_pct` is always populated. Months later, the upstream
team (unaware anyone downstream cares) makes the column nullable to
support a new order type that has no discount. Nothing breaks loudly —
the dashboard just starts silently producing `NULL`-related gaps wherever
that assumption held. A data contract, checked automatically on every
upstream deploy, would have caught the nullability change *before* it
shipped, by comparing the new schema against the contract the downstream
team had implicitly been relying on — turning a silent, discovered-weeks-later
break into a loud, caught-at-deploy-time one.

### URL Shortener Example

This project's contracts —
[`contracts/source/urls.yaml`](../contracts/source/urls.yaml),
[`users.yaml`](../contracts/source/users.yaml), and
[`clicks.yaml`](../contracts/source/clicks.yaml) — declare exactly the
shape `schemas/source/*.md` describes in prose, plus an owner, a
freshness SLA, and explicit quality rules (e.g. `clicks.yaml`'s "rows are
never UPDATEd or DELETEd after insert," which is precisely the assumption
Section 15.9's interview question is built around). `ingestion/src/url_shortener_analytics/contracts.py`
validates them against the real, connected database; `make validate-contracts`
runs it.

### 12.1 Architecture

```mermaid
flowchart LR
    subgraph Contracts["contracts/source/*.yaml"]
        C1[urls.yaml]
        C2[users.yaml]
        C3[clicks.yaml]
    end
    subgraph Validator["contracts.py"]
        LD[load_contract]
        VC[validate_contract]
    end
    subgraph DB["Real Postgres"]
        T[(urls / users / clicks)]
    end

    C1 & C2 & C3 -->|yaml.safe_load| LD
    LD --> VC
    DB -->|sqlalchemy.inspect| VC
    VC -->|ContractValidationResult<br/>violations + warnings| CLI[cli.py validate-contracts]
```

### 12.2 Design Decision: coarse type categories, not exact SQL types

Contracts declare `type: integer` / `text` / `boolean` / `timestamp` /
`numeric` — never `BIGINT` or `VARCHAR(16)` specifically. This project's
own unit tests validate contracts against SQLite (via the `sqlite_engine`
fixture) while integration tests and real usage validate against Postgres
— two dialects whose reflected column types don't share an exact-string
vocabulary (SQLite reports its own affinity-inferred type names; Postgres
reports its native ones). Comparing exact type strings would force a
choice between contracts that only work against one dialect, or
duplicate per-dialect contract files — either way, more complexity than
this project's actual correctness needs justify. `contracts.py`'s
`_categorize_type` function normalizes both dialects' reflected types
into the same five coarse categories via `isinstance` checks against
SQLAlchemy's *generic* type hierarchy (`sqltypes.Integer`, `sqltypes.String`,
etc.), which both dialects' reflection maps into consistently.

### 12.3 Design Decision: violations vs. warnings, and a result object instead of an exception

A contract check reports **all** violations found, not just the first
one — implemented as `validate_contract` returning a
`ContractValidationResult` (a list of violations and a list of warnings)
rather than raising on the first mismatch. This mirrors a real test
suite's own reporting convention deliberately: a CI test run that stopped
at the first failing assertion and hid every other failure until the next
run would be far less useful than one that reports every failure in one
pass. `ContractError` (a real exception) is reserved for cases where
validation genuinely can't be attempted at all — a malformed contract
file, or a table that doesn't exist — as distinct from cases where
validation *runs* and finds the table doesn't match. **Violations vs.
warnings** is a second, related decision: a *missing* contracted column,
a *type* mismatch, or a *nullability* mismatch are violations (the
contract makes a promise the real table breaks); an *extra* column the
real table has but the contract doesn't declare is only a warning. This
matches how most real schema-evolution policies treat additive,
backward-compatible changes (a new column nothing downstream depends on
yet) as safe, while treating a removed or changed column as breaking.

### Alternatives

1. **Exact SQL type comparison (rejected).** More precise, but forces
   either a single validation dialect (breaking the SQLite-backed unit
   test strategy established since Section 14) or duplicate per-dialect
   contracts — complexity this project's actual risk (catching a
   genuinely wrong column type or an accidentally-dropped column) doesn't
   need exact types to catch.
2. **Raise on the first violation found (rejected).** Faster to
   implement, much less useful in practice — see Section 12.3's reasoning
   above; matches this project's own established "report everything, then
   let the caller decide severity" pattern from Section 15's checkpoint
   design.
3. **Treat every mismatch, including extra columns, as a violation
   (rejected).** Would make adding *any* new column to a source table —
   even one nothing downstream needs yet — a breaking contract failure,
   which actively discourages safe, additive schema evolution.
4. **Coarse type categories, violations vs. warnings split, result-object
   return (chosen).** Balances catching genuine breaking changes against
   not blocking safe, additive ones, and reports everything in one pass.

### Trade-offs

| | Coarse type categories + violation/warning split (chosen) | Exact types, all-mismatches-are-violations |
|---|---|---|
| Cross-dialect portability | Works against SQLite and Postgres unchanged | Requires per-dialect contracts, or Postgres-only validation |
| Tolerates safe schema evolution | Yes — new columns are warnings | No — any new column fails the check |
| Catches a genuinely wrong type (e.g. a column silently becoming `TEXT` instead of `INTEGER`) | Yes, at the category level (integer vs. text) | Yes, more precisely (e.g. `BIGINT` vs `INT` would also be caught) |
| False positives from irrelevant precision differences | Low | Higher — e.g. `VARCHAR(16)` vs `VARCHAR(32)` would fail a naive exact-type check even though neither breaks anything this project does |

### 12.4 Implementation

Already built and tested — see
[`ingestion/src/url_shortener_analytics/contracts.py`](../ingestion/src/url_shortener_analytics/contracts.py).

---

**CREATE:** `ingestion/src/url_shortener_analytics/contracts.py`

**PURPOSE:** Load a YAML contract, reflect a real table's actual shape via
SQLAlchemy, and report every difference between them as either a
violation (breaking) or a warning (safe).

**DEPENDENCIES:** `pyyaml` (already a project dependency, used by
`cli.py` for `pipelines.yaml`), a SQLAlchemy `Engine`, this package's
`exceptions` module (for `ContractError`).

**IMPLEMENTATION GUIDE (write it yourself):** three functions. `load_contract(path)`
should `yaml.safe_load` the file inside a `try/except` catching `OSError`
and `yaml.YAMLError`, re-raising as `ContractError`; also raise
`ContractError` if the parsed result is missing the `table` or `columns`
keys — a contract file that doesn't even declare what table it's for
can't be validated against anything. `_categorize_type(sa_type)` should
`isinstance`-check the reflected column type against SQLAlchemy's generic
type classes (`sqltypes.Boolean`, `sqltypes.Integer`, `sqltypes.DateTime`,
`sqltypes.Numeric`, `sqltypes.String`, in that order — order matters where
hierarchies could otherwise overlap) and return one of this project's five
category strings, or `"unknown"` if nothing matches. `validate_contract(engine, contract)`
is the core: use `sqlalchemy.inspect(engine)` to check `has_table` first
(raise `ContractError` if the table doesn't exist at all — validation
genuinely can't proceed), then `get_columns(table_name)` to get the real
shape. Walk every column the *contract* declares: if it's missing from the
real table, that's a `missing_column` violation; if present, compare
categorized type and nullability, recording `type_mismatch` /
`nullability_mismatch` violations as needed. Separately, walk every column
the *real table* has: any not declared in the contract is an `extra_column`
warning, not a violation. Return a `ContractValidationResult` collecting
everything found — never raise for a structural mismatch.

**REFERENCE IMPLEMENTATION:**

```python
# ingestion/src/url_shortener_analytics/contracts.py (excerpt -- full file
# is already committed at this path)

def _categorize_type(sa_type) -> str:
    if isinstance(sa_type, sqltypes.Boolean):
        return "boolean"
    if isinstance(sa_type, sqltypes.Integer):
        return "integer"
    if isinstance(sa_type, sqltypes.DateTime):
        return "timestamp"
    if isinstance(sa_type, sqltypes.Numeric):
        return "numeric"
    if isinstance(sa_type, sqltypes.String):
        return "text"
    return "unknown"


def validate_contract(engine, contract) -> ContractValidationResult:
    table_name = contract["table"]
    inspector = inspect(engine)
    if not inspector.has_table(table_name):
        raise ContractError(f"table '{table_name}' does not exist")

    actual = {c["name"]: c for c in inspector.get_columns(table_name)}
    declared = {c["name"]: c for c in contract["columns"]}
    violations, warnings = [], []

    for name, spec in declared.items():
        if name not in actual:
            violations.append(ContractViolation("missing_column", name, ...))
            continue
        if _categorize_type(actual[name]["type"]) != spec["type"]:
            violations.append(ContractViolation("type_mismatch", name, ...))
        if actual[name]["nullable"] != spec["nullable"]:
            violations.append(ContractViolation("nullability_mismatch", name, ...))

    for name in actual:
        if name not in declared:
            warnings.append(ContractWarning("extra_column", name, ...))

    return ContractValidationResult(table=table_name, violations=violations, warnings=warnings)
```

Full file: [`ingestion/src/url_shortener_analytics/contracts.py`](../ingestion/src/url_shortener_analytics/contracts.py).
CLI wiring: `cli.py`'s `validate-contracts` subcommand
(`validate_contracts_command`) — loads every `*.yaml` in
`contracts/source/`, runs `validate_all_contracts`, logs a pass/fail line
per table plus every violation/warning, and exits `1` if any table has at
least one violation.

**RUN:**

```bash
make up
make validate-contracts
```

**VERIFY:** the log output shows one `"contract passed"` or `"contract
FAILED"` line per table (`urls`, `users`, `clicks`), plus any warnings
(e.g. an extra column) logged individually.

**EXPECTED:** all three contracts pass against a freshly-created database —
`sql/source/*.sql` and `contracts/source/*.yaml` were written to match
each other.

**ACTUAL OBSERVED result:** run for real, against a real (locally
installed, non-Docker) Postgres 16 in this sandbox, using
`python -m url_shortener_analytics.cli validate-contracts` directly:

```
ts=2026-09-19T17:50:04+0000 level=INFO logger=__main__ msg="contract passed" table='clicks' warnings=0
ts=2026-09-19T17:50:04+0000 level=INFO logger=__main__ msg="contract passed" table='urls' warnings=0
ts=2026-09-19T17:50:04+0000 level=INFO logger=__main__ msg="contract passed" table='users' warnings=0
ts=2026-09-19T17:50:04+0000 level=INFO logger=__main__ msg="all contracts passed"
```

**TEST:** `ingestion/tests/unit/test_contracts.py` — 11 tests, covering a
passing contract, an extra-column warning that doesn't fail the result, a
missing-column violation, a type-mismatch violation, a nullability
violation, a nonexistent-table `ContractError`, contract-file loading
(valid, missing, malformed YAML, missing required keys), and — notably —
the **real, committed** `contracts/source/*.yaml` files validated against
a hand-built SQLite schema matching them exactly.
`ingestion/tests/integration/test_contracts_integration.py` proves the
same real contracts against real Postgres — genuinely run in this
sandbox (`pytest -m integration`, real Postgres, no MinIO needed for this
particular test): `1 passed in 0.47s`, an ACTUAL OBSERVED result, not a
projection. (This sandbox's Postgres isn't reached through
`docker-compose.yml` the way `make up` would — see How to test below for
the exact caveat.)

**PRODUCTION CONSIDERATIONS:** see 12.6 below.

**INTERVIEW QUESTIONS:** see 12.8 below.

---

### Hands-on Challenge (implement-yourself)

Before reading further, try this: **without looking at `contracts.py`,
write down what `_categorize_type` should return for a Postgres
`TIMESTAMPTZ` column and for a SQLite column declared `TIMESTAMP`** —
are they the same category? (They should be — both are timestamp-like,
and `sqltypes.DateTime` is the generic SQLAlchemy base both dialects'
reflected types subclass, which is exactly why the category-based
comparison in Section 12.2 works across both without special-casing
either dialect.) Then modify one of `contracts/source/*.yaml`'s column
`type` values to something deliberately wrong (e.g. change `clicks.yaml`'s
`id` column to `type: text`) and run `make test` — confirm
`test_validate_all_contracts_against_the_real_contracts_directory` now
fails, and read its failure output to see exactly how a violation is
reported. Revert the change afterward.

### 12.5 How to test

```bash
make test              # unit tests -- SQLite + mocked S3/contracts, no Docker required
make up
make test-integration   # requires `make up` first -- real Postgres + MinIO
```

The full unit suite (38 tests — 27 from Section 15 plus 11 new in
`test_contracts.py`) was run in this environment while writing this
section (Python 3.11, `PYTHONPATH=ingestion/src python3 -m pytest
ingestion/tests/unit -q`) and genuinely passed — this is an ACTUAL
OBSERVED result, not a projection:

```
38 passed in 6.78s
```

`ingestion/tests/integration/test_contracts_integration.py` was written,
`ruff check`-clean, and **genuinely executed** in this sandbox against a
real (non-Docker) local Postgres 16, using
`pytest -m integration ingestion/tests/integration/test_contracts_integration.py`:

```
ingestion/tests/integration/test_contracts_integration.py::test_all_source_contracts_pass_against_the_real_schema PASSED
1 passed in 0.47s
```

The other integration test files (full load, incremental load) still
need real MinIO, which this sandbox genuinely doesn't have — those remain
un-executed here; run them yourself with `make up && make
test-integration` for the complete suite.

### 12.6 Hands-on Exercise

**LAB 11 — Watch a real contract violation, end to end.**

```bash
make up
make validate-contracts   # baseline: all three contracts pass
```

Now deliberately break one, at the database level rather than the
contract level (the more realistic direction — a schema migration lands
without the matching contract update):

```bash
make db-shell
ALTER TABLE clicks ALTER COLUMN device_type DROP NOT NULL;
\q
make validate-contracts
```

What to observe: `clicks`' result flips to `"contract FAILED"`, with one
`nullability_mismatch` violation logged for `device_type` — the contract
still says `nullable: false`, the real table now disagrees. The command
exits `1`. Revert with
`ALTER TABLE clicks ALTER COLUMN device_type SET NOT NULL;` and re-run to
confirm it passes again.

**ACTUAL OBSERVED result:** run for real in this sandbox (via `psql` and
the CLI directly, not through `docker-compose.yml`/`make db-shell`, since
this sandbox has no Docker daemon but does have a local Postgres):

```
=== after ALTER TABLE clicks ALTER COLUMN device_type DROP NOT NULL ===
ts=2026-09-19T17:50:26+0000 level=ERROR logger=__main__ msg="contract FAILED" table='clicks' violations=1
ts=2026-09-19T17:50:26+0000 level=ERROR logger=__main__ msg="violation" column='device_type' detail='contract expects nullable=False, actual column has nullable=True' kind='nullability_mismatch' table='clicks'
ts=2026-09-19T17:50:26+0000 level=INFO logger=__main__ msg="contract passed" table='urls' warnings=0
ts=2026-09-19T17:50:26+0000 level=INFO logger=__main__ msg="contract passed" table='users' warnings=0
ts=2026-09-19T17:50:26+0000 level=ERROR logger=__main__ msg="contract validation finished with failures"
(exit code: 1)

=== after reverting with ALTER TABLE clicks ALTER COLUMN device_type SET NOT NULL ===
ts=2026-09-19T17:50:27+0000 level=INFO logger=__main__ msg="contract passed" table='clicks' warnings=0
ts=2026-09-19T17:50:27+0000 level=INFO logger=__main__ msg="contract passed" table='urls' warnings=0
ts=2026-09-19T17:50:27+0000 level=INFO logger=__main__ msg="contract passed" table='users' warnings=0
ts=2026-09-19T17:50:27+0000 level=INFO logger=__main__ msg="all contracts passed"
(exit code: 0)
```

Matches the prediction exactly, including the exact violation detail
message.

### Failure Scenario

**What happens if `sql/source/002_hypothetical_users_and_clicks.sql` is
changed (say, `clicks.device_type` is widened from `VARCHAR(16)` to
`TEXT`) but `contracts/source/clicks.yaml` is never updated?**

Nothing breaks — `VARCHAR` and `TEXT` both categorize to `"text"` under
this project's coarse-category comparison (Section 12.2's whole point).
**Now consider the case that does break:** if `device_type` were instead
changed from `NOT NULL` to nullable (a real, meaningful change — every
downstream query that assumes `device_type` is always populated would
need to know), `validate_contract` reports a `nullability_mismatch`
violation immediately, `make validate-contracts` exits `1`, and the CI
step (once one exists — not yet wired into this Phase 1 repo, see
Production Considerations) would fail loudly. **This is the contrast
worth internalizing**: a coarse-category type change (harmless, no
violation) vs. a nullability change (meaningful, always a violation) are
treated differently on purpose, matching which changes actually break a
downstream consumer's assumptions and which don't. **Production
implication:** the fix, in either direction, is to decide *which* artifact
is wrong and update it — a real nullability change usually means the
contract should be updated (the schema change was intentional); an
accidental schema drift usually means the schema should be reverted (the
contract was right, the migration was a mistake). The tool can't tell you
which — it can only tell you they disagree.

### 12.7 Production Considerations

| Aspect | This repo (POC) | Production |
|---|---|---|
| When contracts are checked | Manually, via `make validate-contracts` | Automatically, in CI, on every migration/schema change PR — before it merges, not after |
| Scope | Source tables only (`contracts/source/`) | Often extended to the analytical layer too (`sql/analytics/` — explicitly not contract-checked yet in this project; a real gap, named here rather than silently left) |
| Violation response | Logged, exit code 1 | Often paired with an alerting/paging system, and a documented escalation path (who gets notified, who decides schema-vs-contract is wrong) |
| Contract ownership | Implicit (this project) | Each contract typically owned by the team that owns the source system, with the downstream consuming teams as reviewers on any contract change |
| Quality rules (`quality_rules:` in each YAML) | Documented, not enforced by `contracts.py` | A mature contract framework enforces these too (e.g. actual uniqueness checks, actual enum-membership checks) — not yet built here; see Alternatives below for why |

### Principal Data Engineer Perspective

The choice to leave `quality_rules:` as documentation rather than enforced
checks (12.6) is a deliberate scope boundary, not an oversight, and it's
worth being able to articulate the difference between *shape* validation
(what this section builds: columns, types, nullability) and full *data
quality* validation (row-level rules: uniqueness, referential integrity,
value-set membership) as two related but genuinely different disciplines.
Shape validation is cheap to run (a schema reflection call, milliseconds)
and catches an entire class of breaking changes; row-level quality checks
require scanning actual data (potentially expensive at scale) and catch a
different, complementary class of problems (a duplicate `short_code`, an
out-of-range `plan_type`). A principal engineer builds the cheap,
high-leverage check first, ships it, and treats the more expensive
discipline as a deliberate next increment — exactly the sequencing this
project has followed since Section 13's "batch before streaming" framing.

### 12.8 Principal Engineer Interview Questions

**Q: "Why compare types by coarse category ('integer', 'text') instead of
exact SQL type strings ('BIGINT', 'VARCHAR(16)')? Isn't that less
precise?"**

*What's tested:* whether the candidate can defend a deliberately
lower-precision design choice on its actual merits, rather than assuming
more precision is always strictly better.

*What a weak answer looks like:* "Exact types would be more thorough, but
this is just a POC so it's fine to be less precise" — treats it as a
shortcut rather than a reasoned trade-off.

*What a strong answer covers:* exact type comparison would either force
validating against only one database dialect, or maintaining separate
per-dialect contracts — real cost, for catching a class of difference
(e.g. `VARCHAR(16)` vs `VARCHAR(32)`) that essentially never actually
breaks a downstream consumer, while the coarse category still reliably
catches the differences that *do* matter (a column silently changing from
numeric to text, say). The right precision level for a check should match
the cost of false positives against the value of what it actually
catches, not just maximize precision for its own sake.

*Concepts:* precision/recall trade-off applied to schema validation;
cross-dialect portability as a real constraint, not an excuse.

*Expected follow-up:* "Can you think of a real type change this approach
would miss?" — Yes: a column narrowing from `BIGINT` to `INTEGER` (both
categorize as "integer") could silently start truncating values the
contract wouldn't flag — a genuine, named limitation of the coarse-category
approach, worth stating rather than glossing over.

*Common mistake:* assuming a validation check with lower precision is
strictly worse, without weighing what it costs to get that precision
against what it actually protects against.

**Q: "Your contract validator finds an extra column in the real table that
the contract doesn't declare. Should that fail the check? Why or why
not?"**

*What's tested:* whether the candidate understands additive vs. breaking
schema changes as a real, load-bearing distinction, not an arbitrary
choice.

*What a weak answer looks like:* "Yes, anything different from the
contract should fail — the contract is supposed to be the source of
truth" — technically consistent, but ignores that this policy would make
routine, safe schema evolution (adding a new column nothing depends on
yet) indistinguishable from an actually breaking change.

*What a strong answer covers:* an extra column is additive and
backward-compatible — every existing query, every existing contract
consumer, keeps working exactly as before; treating it as a failure would
actively punish safe evolution and train teams to either avoid adding
contract-checked tests, or to update the contract reflexively without
review just to unblock a merge, defeating the contract's purpose. A
*missing* or *changed* contracted column is different in kind: something
a consumer was actively relying on has changed or disappeared, which is
exactly the class of change a contract should catch.

*Concepts:* schema evolution policy (additive/backward-compatible vs.
breaking), designing a check's failure criteria around what actually harms
a consumer rather than around "any difference at all."

*Expected follow-up:* "What if a downstream consumer *does* eventually
need to know about new columns proactively, not just tolerate them
silently?" — That's a legitimate reason to promote "extra column" from a
warning to something surfaced more visibly (e.g. a notification, not a
failure) — still not a hard failure, but not silent either; a real
contract framework often supports exactly this severity distinction.

*Common mistake:* treating "matches the contract exactly" and "safe for
consumers" as the same requirement, when in practice they diverge exactly
at additive changes.

---

## 13. Batch Ingestion Design

### Concept

**Batch ingestion** extracts data on a schedule or on demand, in discrete
runs, rather than reacting to each individual change as it happens
(streaming) or capturing every database write from the transaction log
(CDC). Each run reads a defined slice of the source (everything, for a
full load) and produces a complete, independent output.

### Why batch, and why first?

Three reasons, in order of how much they matter for this specific project
right now:

1. **There's no event stream to consume yet.** Section 1 (Gaps/Assumptions)
   established that the source application doesn't even write click events
   synchronously, let alone publish them anywhere. Kafka needs a producer;
   there isn't one.
2. **The data volume doesn't justify streaming's complexity.** Streaming
   infrastructure earns its keep when near-real-time freshness has real
   business value and batch genuinely can't deliver it. Neither is true
   yet at this project's scale.
3. **Batch is easier to reason about and debug.** A batch run either fully
   succeeded or didn't — there's no partial consumer state, no rebalancing,
   no "which offset was I at when it crashed" question. That simplicity is
   worth deliberately keeping for as long as it's sufficient.

### 13.1 What Phase 1 batch ingestion does NOT include

Explicitly out of scope for Phase 1, by design, not by oversight: Kafka,
Debezium/CDC, Apache Flink, Spark Structured Streaming, Kubernetes,
Airflow. See [ADR-004](#adr-004-batch-ingestion-before-cdcstreaming) for
the full reasoning and the conditions under which each would get
introduced.

### 13.2 Full load vs. incremental load vs. CDC

| | Full load | Incremental load | CDC |
|---|---|---|---|
| Reads | Entire table, every run | Only rows changed since last run | Every row-level write, via the database's transaction log |
| Needs | Nothing beyond table access | A reliable "what changed" signal (a watermark) | Log access (e.g. Postgres logical replication) |
| This repo, Phase 1 | ✅ Implemented — [Section 14](#14-full-load-ingestion) (`urls`, `users`, `clicks` all start here) | ✅ Implemented — [Section 15](#15-incremental-load--watermarks-) (`clicks` only) | Not planned before Phase 5 (post-Phase-4, see repository-level plan) |

### 13.3 Checkpointing and watermark mechanism (already built)

Full load doesn't strictly need a watermark (it always reads everything),
but this repo's `ingestion_metadata` table and
`ingestion/src/url_shortener_analytics/metadata.py` module were built to
support **both** full and incremental load from day one — see
[Section 14.2](#142-architecture) for why, and
[`sql/source/003_ingestion_metadata.sql`](../sql/source/003_ingestion_metadata.sql)
for the schema. This is a deliberate case of building the metadata layer
once, correctly, rather than retrofitting it when incremental load
(Section 15) needs it. See [Section 16](#16-checkpointing-) for the
dedicated deep-dive on checkpointing itself, once both load types have
made checkpoint recovery observable.

---

## 14. Full Load Ingestion ✅✅

### 1. CONCEPT

Full load ingestion means: on every run, read the entire source table and land it as one Bronze snapshot — no "what's new since last time" logic at all. It's the simplest possible ingestion strategy, and it's correct specifically for tables that are small and/or don't have a reliable "what changed" signal (no updated_at, or rows get hard-deleted, so an incremental id/timestamp filter would silently miss deletions).

It matters because it's the baseline every other ingestion pattern gets compared against. Incremental load (next concept) exists purely to solve full load's two real costs: it re-reads and re-writes data that hasn't changed, and that cost grows linearly with table size forever. You can't evaluate when incremental load is worth its added complexity (watermarks, gap-detection, replay logic) without first understanding exactly what full load costs and where it breaks down.

### 2. URL SHORTENER EXAMPLE

Your urls and users tables are full-loaded (ingestion/configs/pipelines.yaml presumably sets load_type: full for them — worth confirming). Both are small (500 and 200 rows in your seeded sandbox) and, more importantly, urls rows get mutated in place — is_active flips, title can change — with no guaranteed updated_at column your contract commits to. A naive incremental "give me rows with id > last_seen_id" would never see those mutations at all. Re-reading the whole table every run is the honest way to guarantee Bronze reflects current state.

clicks, by contrast, is append-only and immutable (its contract says so explicitly), which is exactly the property that makes it safe to load incrementally instead — that's next concept's territory.

### 3. DESIGN

Flow: cli.py's full-load command → loop over configured tables → run_full_load(engine, s3_client, bucket, pipeline_name, table_name) per table → inside that: metadata.start_run (checkpoint row, status=running) → extract_full (read) → write_bronze (write, deterministic key) → metadata.finish_run_success (checkpoint row, status=success, with the exact bronze_key written), with finish_run_failure + re-raise on any exception.

Key decision worth calling out explicitly: the Bronze key is bronze/{table}/ingestion_date={date}/{table}.parquet — date only, no run id, no timestamp-to-the-second (object_store.build_bronze_key). That single design choice is what makes full load idempotent: run it three times today, you PUT to the same S3 key three times, and the object store just overwrites in place. No dedup logic needed anywhere downstream for "did this run happen twice today." Contrast with incremental load's key (watermark_start=/watermark_end=), which must vary per batch — you'll see why that's a harder idempotency problem next.

Alternative rejected: a key with a run id or full timestamp (e.g. .../run_id=<uuid>/urls.parquet). That would make every run's output independently addressable/auditable, but it means Bronze accumulates a new full copy of urls every single day forever, and Silver's read layer (read_bronze_clicks, which you've already seen) would have to pick "the latest one" instead of just reading what's there — extra logic, for no benefit at this table's size and mutation pattern.

### 4. IMPLEMENTATION

This is already fully built and running in your repo — nothing to add. Reference code, so you can trace it against what you already have:

```python
# ingestion/src/url_shortener_analytics/extract_full.py

def extract_full(table_name: str, engine: Engine) -> pd.DataFrame:
    df = pd.read_sql_table(table_name, engine)   # POC: whole table, one query
    return df


def run_full_load(engine, s3_client, bucket, pipeline_name, table_name) -> dict[str, object]:
    run_id = metadata.start_run(engine, pipeline_name, table_name, load_type="full")
    try:
        df = extract_full(table_name, engine)
        run_date = datetime.now(UTC)
        key = write_bronze(df, table_name, run_date, s3_client, bucket)
        metadata.finish_run_success(engine, run_id, rows_read=len(df), rows_written=len(df), bronze_key=key)
    except Exception as err:
        metadata.finish_run_failure(engine, run_id, str(err))
        raise
    return {"run_id": run_id, "table": table_name, "rows": len(df), "key": key}

# ingestion/src/url_shortener_analytics/object_store.py

def build_bronze_key(table_name: str, run_date: datetime) -> str:
    return f"bronze/{table_name}/ingestion_date={run_date:%Y-%m-%d}/{table_name}.parquet"
```

### 5. CODE WALKTHROUGH

extract_full is deliberately pure — no S3, no metadata table, just SQL in, DataFrame out. That's why run_silver_clicks_job's split into clean_clicks/deduplicate_clicks (pure) vs the job wrapper (I/O) — which you already built in Section 40 — feels familiar: this is the same pattern, established first here, one layer down the pipeline.

run_full_load's try/except is the checkpoint contract in miniature: a run is always recorded as either success or failed — never left running — because get_last_watermark and (once you get to Section 16-equivalent) find_stale_running_runs both depend on running meaning "still actually in flight, or crashed." If an exception here weren't caught and re-recorded, a crashed run would sit as running forever and nothing downstream could tell "still going" from "died."

The bronze_key gets stored on the success row (finish_run_success(..., bronze_key=key)) rather than recomputed later from table_name + started_at — you can see in metadata.py's docstring this was a deliberate choice (Section 17.3 in the guide), because recomputing it later means duplicating build_bronze_key's exact date-formatting logic in a second place, and any drift between the two becomes a silent bug.

### 6. RUN

make ingest-full

which resolves to python -m url_shortener_analytics.cli full-load. Expected structured log lines, per table, from what you've already genuinely run in this sandbox (real numbers, your seeded baseline):

{"event": "ingestion run started", "run_id": "<uuid>", "source_table": "urls", "load_type": "full"}
{"event": "extracting table (full load)", "table": "urls"}
{"event": "extraction complete", "table": "urls", "rows": 500, "columns": <N>}
{"event": "wrote bronze object", "key": "bronze/urls/ingestion_date=2026-09-24/urls.parquet", "bytes": <N>, "rows": 500, "attempt": 1}
{"event": "ingestion run succeeded", "run_id": "<uuid>", "rows_written": 500, "bronze_key": "bronze/urls/ingestion_date=2026-09-24/urls.parquet"}
{"event": "table done", "run_id": "<uuid>", "table": "urls", "rows": 500, "key": "bronze/urls/..."}

...same shape for users (rows: 200). This part is DESIGN EXPECTATION for today's exact date/uuid — the row counts and key format are ACTUAL OBSERVED from your prior real runs (Section 14/LAB 1 in your guide).

### 7. EXPERIMENT

Run make ingest-full twice in a row, then check the object store:

python -m url_shortener_analytics.cli storage-stats --prefix bronze/urls/

Expected: object_count for urls stays at 1 after both runs, not 2 — that's the idempotency claim from Section 3 made concrete. Now try a failure scenario: kill the process (Ctrl+C) mid-way through the second run, after start_run has inserted its running row but before finish_run_success runs. Then:

python -m url_shortener_analytics.cli ingestion-history --table urls --limit 5

You'll see one success row (from run 1) and one running row stuck forever (from the killed run 2) — this is exactly the "stale running run" problem find_stale_running_runs exists to detect, which you'll hit properly when we get to Checkpoints. Worth seeing now so the motivation for that concept isn't abstract.

### 8. PRODUCTION VIEW

At your current scale (500/200 rows) pd.read_sql_table's "whole table in memory, one query" is free. At real scale it's the single biggest thing that breaks: a 200M-row table full-loaded this way either OOMs the extraction process or holds a long table-scan transaction open against a live OLTP database for however long the read takes — actively harmful to the production database it's reading from, not just slow. Real fix is chunked/paginated extraction (LIMIT/OFFSET or a keyset cursor), writing each chunk as its own Parquet part-file rather than one giant DataFrame.

Cost-wise, full load's defining trait is that its S3 PUT cost and compute cost are both O(table size) on every single run, forever — regardless of how much actually changed. That's the concrete number you'd put in front of a full-load-vs-incremental decision: "this table is N rows, growing at R rows/day, full-loading it costs $X/month in read+write, here's what incremental would cost instead."

### 9. PRINCIPAL ENGINEER VIEW

An interviewer asking "when would you full-load vs incrementally load a table" is really testing whether you reach for incremental load reflexively (many candidates do, because it "sounds more sophisticated") or whether you can name the actual precondition: a reliable append-only or monotonically-increasing change signal. No such signal → full load is correct, not a fallback. This table's urls (mutable, no updated_at) is a textbook case of that decision being correct as-is.

The key-determinism point (date-only key, no run id) is worth having ready as an idempotency example that isn't "add a dedup step" — it's "make the write itself naturally overwrite instead of accumulate." Idempotency-by-construction (the write target is deterministic) versus idempotency-by-cleanup (write and then dedup) is a real, recurring design fork, and this table is a case of choosing the first.

### 10. REMEMBER

Full load = read everything, every run. Correct precondition: table is small, or mutable with no reliable change signal, or can be hard-deleted.
Idempotency here comes from the key, not from logic: same table + same date = same S3 key = safe overwrite. No dedup step required.
running → success/failed is a hard invariant. A run that dies must never stay running — that's what makes watermark/checkpoint reads trustworthy later.
Full load's cost is O(table size) every run, forever — that's the exact cost incremental load exists to eliminate, not a stylistic alternative to it.

## 15. Incremental Load & Watermarks ✅✅

### 1. CONCEPT

Incremental load reads only the rows that are new since the last successful run, instead of re-reading the whole table. It needs one saved piece of state — a watermark — that tells the next run where to resume from.

It exists because full load's cost is O(table size) on every single run, forever. That's fine for a 500-row urls table; it's ruinous for clicks, a table designed to grow without bound. A watermark turns "read everything" into "read what's new," so the cost of a run is proportional to how much changed, not how big the table has gotten.

### 2. URL SHORTENER EXAMPLE

clicks is the one table this applies to in your schema — urls/users stay on full load because they're small and (as you already know from Section 14) urls is mutable, which incremental load can't handle anyway. clicks is the opposite: append-only, immutable, and has a BIGSERIAL id — exactly the shape incremental load wants. First run: watermark starts at 0, reads everything that exists. Every run after that: WHERE id > <last watermark>, gets back only what's new since then, and remembers the new highest id.

### 3. DESIGN

Flow: cli.py's run command (not full-load) → per table, checks pipelines.yaml's load_type → for clicks, calls run_incremental_load(engine, s3_client, bucket, pipeline_name, table_name) → inside: start_run → get_last_watermark (reads only status='success' rows) → extract_incremental → if empty, finish successfully without writing anything → otherwise write_bronze_incremental then finish_run_success with the new watermark.

Two decisions worth being deliberate about:

**Decision 1** — the watermark is id, not a timestamp. WHERE id > :watermark, not WHERE occurred_at > :watermark. A BIGSERIAL is assigned by Postgres itself, strictly in insertion order — no two rows can share a value, and it can't go backwards. A timestamp column, by contrast, is set by whatever produced the row, and under enough concurrent writes two rows genuinely can land on the same timestamp at typical precision, or even appear "out of order" if a writer's clock is skewed. An id-based watermark is immune to both problems by construction. The real cost: it only works because clicks is insert-only. It could never see an UPDATE or a DELETE — a row that changes after insertion just silently stays at whatever it was when first read. That's a genuine, accepted limitation, not an oversight.

**Decision 2** — the incremental Bronze key includes the watermark range, not just the date. Full load's date-only key is safe to reuse only if the pipeline runs at most once a day. Incremental load doesn't — it can run many times a day, and each run covers a different id range that must not collide with the previous run's batch. So the key is bronze/{table}/incremental/watermark_start={n}/watermark_end={m}/{table}.parquet — different range, different key, nothing gets silently overwritten by a different batch.

Alternative rejected: full CDC via Postgres logical replication — would capture every insert/update/delete with no polling gap at all, but is a much bigger infrastructure lift (replication slot, consumer process), and there's no operational need for it yet at this project's stage. That's deferred, not dismissed.

### 4. IMPLEMENTATION

Already built and running — here's the real code to trace against what you have:

```python
# ingestion/src/url_shortener_analytics/extract_incremental.py

def extract_incremental(table_name: str, engine: Engine, watermark: int) -> pd.DataFrame:
    query = text(f"SELECT * FROM {table_name} WHERE id > :watermark ORDER BY id")
    df = pd.read_sql_query(query, engine, params={"watermark": watermark})
    return df


def run_incremental_load(engine, s3_client, bucket, pipeline_name, table_name) -> dict[str, object]:
    run_id = metadata.start_run(engine, pipeline_name, table_name, load_type="incremental")
    try:
        watermark = metadata.get_last_watermark(engine, pipeline_name, table_name)
        df = extract_incremental(table_name, engine, watermark)

        if df.empty:
            metadata.finish_run_success(
                engine, run_id, rows_read=0, rows_written=0,
                watermark_start=watermark, watermark_end=watermark,
            )
            return {"run_id": run_id, "table": table_name, "rows": 0, "key": None,
                    "watermark_start": watermark, "watermark_end": watermark}

        new_watermark = int(df["id"].max())
        key = write_bronze_incremental(df, table_name, watermark, new_watermark, s3_client, bucket)
        metadata.finish_run_success(
            engine, run_id, rows_read=len(df), rows_written=len(df),
            watermark_start=watermark, watermark_end=new_watermark, bronze_key=key,
        )
    except Exception as err:
        metadata.finish_run_failure(engine, run_id, str(err))
        raise

    return {"run_id": run_id, "table": table_name, "rows": len(df), "key": key,
            "watermark_start": watermark, "watermark_end": new_watermark}

# object_store.py
def build_bronze_incremental_key(table_name: str, watermark_start: int, watermark_end: int) -> str:
    return (
        f"bronze/{table_name}/incremental/"
        f"watermark_start={watermark_start:012d}/watermark_end={watermark_end:012d}/{table_name}.parquet"
    )
```

### 5. CODE WALKTHROUGH

The df.empty branch is the most important line in this function, not a minor edge case. Read the comment on it in your own code: writing a zero-row Parquet object at a watermark_start == watermark_end key would represent nothing useful and would just clutter Bronze — every no-op run (and there will be many, since a real schedule polls far more often than clicks actually arrive) would otherwise produce a useless empty file. Treating "no new rows" as a normal successful outcome, not a special case bolted on, is what keeps the object count in Bronze meaningful.

Notice get_last_watermark (in metadata.py, which you can see is unchanged from Section 14) only ever reads status='success' rows. That's the same invariant from full load, doing more work here: a run that crashed mid-flight must never become the basis for where the next run resumes from — otherwise a crash could corrupt the entire pipeline's notion of "what's already been read."

ORDER BY id in the query isn't cosmetic — new_watermark = int(df["id"].max()) needs the max to be unambiguous, and depending on unspecified row order happening to already be sorted is exactly the kind of silent assumption that breaks later without warning.

### 6. RUN
```
make ingest

First run (watermark starts at 0, reads everything that exists so far):

extracting table (incremental load), table=clicks, watermark=0
extraction complete, table=clicks, rows=<N>, watermark=0
wrote bronze object, key=bronze/clicks/incremental/watermark_start=000000000000/watermark_end=000000000<N>/clicks.parquet

Insert a couple of rows directly, then run again — watermark is now <N>, and only the new rows are read:

extracting table (incremental load), table=clicks, watermark=<N>
extraction complete, table=clicks, rows=2, watermark=<N>
wrote bronze object, key=bronze/clicks/incremental/watermark_start=000000000<N>/watermark_end=000000000<N+2>/clicks.parquet

This is DESIGN EXPECTATION for the exact row counts — depends on your current clicks count, which you should confirm for yourself with SELECT COUNT(*) FROM clicks; before running.
```

### 7. EXPERIMENT

Run make ingest a third time with no new rows inserted. Expected: rows=0 in the log, no "wrote bronze object" line at all, and a new ingestion_metadata row with status='success', rows_written=0, watermark_end unchanged from the previous run. Confirm with:

```sql
SELECT status, rows_written, watermark_start, watermark_end
FROM ingestion_metadata WHERE source_table='clicks' ORDER BY started_at DESC LIMIT 3;
```

Now the more interesting failure scenario — do this as a thought experiment first, then verify it's what the code actually does: what would go wrong if finish_run_success were called before write_bronze_incremental instead of after? Answer: a process killed in that window would leave the watermark advanced in ingestion_metadata even though the Bronze object was never written — a silent data gap. The next run would trust the advanced watermark and start reading after rows that were never actually captured anywhere. That's strictly worse than a stuck running row (which is at least visible and detectable) — it's invisible corruption. Check run_incremental_load's actual ordering: write happens, then finish_run_success. That ordering is the whole defense.

### 8. PRODUCTION VIEW

The real gap this design accepts: it's insert-only by construction. It can never see an UPDATE or a DELETE on clicks — if a row changed after being read once, incremental load has no way to know. That's fine here because your contract says clicks are never updated/deleted after insert; it would be a real bug if that stopped being true and nobody revisited this design. At real scale, the id-based watermark also assumes a single writer sequence — if clicks were ever sharded across multiple databases (say, per-region), "the highest id" stops being a single meaningful number, and you'd need a different scheme (per-shard watermarks, or move to timestamp/CDC-based capture despite its trade-offs).

### 9. PRINCIPAL ENGINEER VIEW

The id-vs-timestamp watermark choice is a favorite interview probe because it looks like a minor implementation detail but is actually testing whether you understand why clock-dependent state is dangerous in distributed writes — "two events can share a timestamp, or even appear out of order, under concurrent writes or clock skew" is the kind of insight that separates someone who's internalized distributed systems failure modes from someone reciting definitions.

The ordering bug in the EXPERIMENT section (checkpoint-before-write vs write-before-checkpoint) is a real, general pattern worth having ready: whenever a "commit" or "checkpoint" step and a "write the data" step are two separate operations, the two possible failure windows have asymmetric severity — one produces a visible, detectable stuck state; the other produces an invisible data gap that nobody notices until someone asks "why is this data missing" much later. Good production systems always order the checkpoint after the write, never before.

### 10. REMEMBER

Watermark = one saved number that turns "read everything" into "read what's new." That's the whole trick.
ID-based watermarks beat timestamp-based ones specifically because they're immune to clock skew and duplicate values — at the cost of only working for insert-only tables.
"No new rows" is a success, not an edge case — don't write empty files just to have something to point at.
Checkpoint (mark success) must always happen after the write it's checkpointing, never before — reversed order turns a visible failure into a silent data gap.

## 16. Checkpointing ✅✅

### 1. CONCEPT

A checkpoint is a durable record of whether one specific run finished, is still in progress, or failed — separate from watermarks, separate from what data the run actually produced. You've already been using it without naming it: every start_run/finish_run_success/finish_run_failure call in metadata.py, since Section 14, has been writing checkpoints. This concept is the dedicated look at why that pattern exists on its own merits, independent of the idempotency/watermark stories built on top of it.

It matters because "did this job finish?" has no reliable answer once you can no longer ask the process itself — and you can't, the moment it's been killed (OOM, spot eviction, kubectl delete pod, a laptop losing power). A checkpoint answers the question a different way: write the intent down before doing the work, then update the record on definite success or definite failure. Anyone reading the table later — a human, an orchestrator, a monitor — reconstructs exactly what happened from the row alone, with zero dependency on the crashed process still being around to explain itself.

### 2. URL SHORTENER EXAMPLE

metadata.start_run(...) inserts status='running' before extract_full/extract_incremental ever touches Postgres. If the process gets OOM-killed mid-scan on a large clicks full load, that row is frozen exactly as written: status='running', completed_at still NULL — forever, because nothing is left running to update it. You already produced a real, tame version of this: your full row for urls/users at 11:10:39 and 11:23:54 both show status='success' with real completed_at timestamps — that's the checkpoint doing its job correctly. What this section adds is the machinery to notice when a row is stuck, instead of leaving that to a human eyeballing the table.

### 3. DESIGN

New piece: metadata.find_stale_running_runs(engine, max_runtime_minutes=60, pipeline_name=None), wired into a new CLI command check-stale-runs. It's a single read-only query: any status='running' row whose started_at is older than the threshold gets reported. Exit code 1 if any are found — that's the entire monitoring interface, meant to be wrapped by a cron or an Airflow sensor later.

Key decision — detect staleness by elapsed time, not a heartbeat. A status='running' row is ambiguous on its own: still legitimately in progress, or crashed? Two ways to disambiguate were on the table:

Heartbeat — a long-running process updates its own row every N seconds to prove it's alive; a row is stale only once its heartbeat also goes quiet. Precise, but requires touching every run_*_load function to add the heartbeat write, plus something to actually schedule those periodic writes.
Lock/lease — e.g. a Postgres advisory lock held for the run's duration; stale means "lock is free but row still says running." Also precise, but introduces lock-lifecycle bugs of its own (a lock that never gets released is a new failure mode you didn't have before).
Elapsed-time threshold (chosen) — "if started_at is more than 60 minutes ago and still running, call it stale." One query, no changes to any existing run function, no new infrastructure. The real cost: it's a guess, not a fact — a run that's legitimately still executing past the threshold gets misclassified as crashed.

That trade-off is exactly right for this project's current scale (Section 13.1's stated scope: no orchestrator, no concurrent distributed workers yet). It would stop being the right choice the moment job durations become highly variable or genuinely long-running.

The other real decision, smaller but worth internalizing: the cutoff timestamp is computed in Python (datetime.now(UTC) - timedelta(minutes=max_runtime_minutes)) and passed as a bound parameter — not now() - interval '60 minutes' written directly in SQL. Reason: this function is unit-tested against SQLite (no interval syntax at all) and run against Postgres in production. A plain bound timestamp parameter works identically against both; the same cross-dialect reasoning you'd have seen in contracts.py, applied here a second time.

### 4. IMPLEMENTATION

Already in your metadata.py — you read the whole file earlier this session:

```python
def find_stale_running_runs(
    engine: Engine, *, max_runtime_minutes: int = 60, pipeline_name: str | None = None
) -> list[dict[str, Any]]:
    cutoff = datetime.now(UTC) - timedelta(minutes=max_runtime_minutes)
    query = """
        SELECT run_id, pipeline_name, source_table, load_type, started_at
        FROM ingestion_metadata
        WHERE status = 'running' AND started_at < :cutoff
    """
    params: dict[str, Any] = {"cutoff": cutoff}
    if pipeline_name is not None:
        query += " AND pipeline_name = :pipeline_name"
        params["pipeline_name"] = pipeline_name
    query += " ORDER BY started_at ASC"

    with engine.begin() as conn:
        rows = conn.execute(text(query), params).fetchall()
    return [
        {"run_id": r.run_id, "pipeline_name": r.pipeline_name, "source_table": r.source_table,
         "load_type": r.load_type, "started_at": r.started_at}
        for r in rows
    ]
```
cli.py's check_stale_runs_command wraps this: prints a warning per stale row, exits 1 if any exist, exits 0 ("no stale running runs found") otherwise.

### 5. CODE WALKTHROUGH

Nothing about start_run/finish_run_success/finish_run_failure changed for this section — that's the point being made explicitly in your guide: because the checkpoint mechanism was already correct and complete from Section 14 onward, closing this real operational gap ("no stale-run alerting") cost exactly one small read-only function, not a rewrite of anything. That's the payoff of building the metadata table correctly on day one instead of patching fields into it as each new need shows up.

The WHERE status = 'running' filter is doing real work: find_stale_running_runs deliberately excludes success and failed rows — a completed run, whatever its outcome, is not "stale" by definition. Staleness is specifically about a run whose true final state is unknown.

### 6. RUN

```bash
make check-stale-runs
```

With no crashed runs, expect one line: "no stale running runs found", exit 0. To actually see it catch something, manufacture a stale run:

```bash
python3 -c "
from url_shortener_analytics.config import get_settings
from url_shortener_analytics.db import engine_from_settings
from url_shortener_analytics import metadata
engine = engine_from_settings(get_settings())
run_id = metadata.start_run(engine, 'lab12_pipeline', 'clicks', load_type='full')
print('started', run_id)"
```

Run make check-stale-runs right after — it should still report clean (started seconds ago, not stale yet). Then backdate it to simulate a crash 90 minutes in the past:

```sql
UPDATE ingestion_metadata
SET started_at = now() - interval '90 minutes'
WHERE pipeline_name = 'lab12_pipeline';
```

Run make check-stale-runs again — now it should log one "stale running run" warning and exit 1 (echo $? to confirm). Nothing else about the row changed except started_at — that's the entire mechanism, made concrete.

### 7. EXPERIMENT

This is the experiment that actually matters more than the lab above: think through the false-positive case before checking your intuition against it. Suppose clicks' full load genuinely takes 75 minutes once the table grows large (remember — Section 14's extract_full has no chunking, it's one memory-bound query). check-stale-runs runs on the default 60-minute threshold. At minute 61 of a perfectly healthy, still-executing run, it gets reported as stale. If that's wired into paging, someone gets woken up for nothing — and worse than the wasted page, they start trusting the alert less the next time it fires, which is a much harder cost to undo. Now the inverse: what happens if max_runtime_minutes is set too high instead? (Answer: a genuinely crashed run sits undetected longer, and anything downstream depending on freshness — a dashboard, later in Phase 2 — stays silently stale that much longer too.) There's no value that's simply "correct" here, only one tuned to this specific pipeline's actual p99 run duration.

### 8. PRODUCTION VIEW

Right now check-stale-runs's exit code is the entire monitoring interface — nothing calls it automatically, nothing pages anyone. Production wires it into a scheduled invocation (a cron, or an Airflow sensor once you're past Kafka) that pages on-call on a non-zero exit. Some shops go further and auto-mark a sufficiently stale running row as failed once alerting has fired, so a retry can be scheduled without a human in the loop. The threshold itself should ideally be derived per-pipeline from observed p99 duration, not left at one guessed global default — that's a real, ongoing operational tuning job, not a one-time setting.

### 9. PRINCIPAL ENGINEER VIEW

The interview-ready version of this section isn't "I built stale-run detection" — it's naming the exact condition under which your chosen mechanism gives a wrong answer, in the same breath as defending the choice. "I used an elapsed-time threshold because it needed zero new infrastructure at this project's current scale, and I know exactly when it breaks: once job duration becomes highly variable, a threshold starts producing false positives, and the fix at that point is a heartbeat, not a bigger threshold." That combination — the right choice for the current stage, plus a clear-eyed statement of its failure boundary — is what distinguishes "shipped something" from "understands the trade-off they shipped."

The second thing worth having ready: a real, previously-undiscovered dialect wrinkle showed up testing this against real Postgres versus SQLite — a Postgres uuid column round-trips as a Python uuid.UUID object through a raw SELECT, while the SQLite test fixture (run_id TEXT PRIMARY KEY) round-trips it as a plain str, so comparing run_ids read back from real Postgres needs an explicit str(...) cast that the SQLite-backed unit tests never needed. It's a good concrete example of "your unit tests passing against SQLite doesn't guarantee correctness against the real database" — a distinction interviewers like probing directly.

### 10. REMEMBER

A checkpoint answers "did this finish?" durably, independent of whether the process that ran it still exists to explain itself.
Write the running row before doing the work, update it only on definite success or failure — that ordering is what makes a stuck row a fact, not a guess.
An elapsed-time threshold is cheap and sufficient at small scale, but it's a guess: it will misclassify a slow-but-healthy run as stale if the threshold isn't tuned above your actual p99 duration.
Closing an operational gap by querying data you're already durably recording (because the metadata table was designed completely up front) is cheaper than retrofitting new instrumentation later — this section cost one function precisely because Section 14 built the table right the first time.

## 17. Idempotency ✅✅

### 1. CONCEPT

An operation is idempotent if running it more than once, with the same inputs, leaves things in exactly the same state as running it once. You already have this: build_bronze_key/build_bronze_incremental_key are deterministic, so a retry's put_object overwrites cleanly instead of duplicating. That's the "writes are safe to retry" half.

What's new: idempotent writes alone don't guarantee the system as a whole never drifts. A manually uploaded object, an object deleted by something outside the pipeline, or a retried run whose key genuinely doesn't collide with the original (a real residual edge case from Section 15.7) — none of these are caught by "the write itself is safe to repeat." You need a separate check that compares what the control plane (ingestion_metadata) believes it wrote against what's actually sitting in the bucket. That check is reconciliation, and it's the actual new content in this concept.

### 2. URL SHORTENER EXAMPLE

build_bronze_key("clicks", run_date) returns the same string every time today — you've already verified this by hand. What you haven't seen: since this increment, finish_run_success also records that exact key on the ingestion_metadata row itself (bronze_key column). So the control plane doesn't just idempotently write the object — it durably remembers what it wrote. That memory is the input reconciliation needs.

### 3. DESIGN

Two independent mechanisms, checked at different times, for different failure classes:

Idempotent writes (already built) — make a single run's own retry safe.
Reconciliation (new) — detects drift from anything else: a manual upload, an out-of-band deletion, a non-overlapping duplicate from a retry.

reconcile_bronze(engine, s3_client, bucket, pipeline_name) does two set comparisons:

list_bronze_keys (what's really in the bucket) minus list_successful_bronze_keys (what ingestion_metadata says was written) = orphaned — data nobody accounted for.
list_successful_bronze_keys minus what actually head_objects successfully = missing — a promise that wasn't kept.

Key decision: store bronze_key explicitly on the row, don't recompute it later. The alternative — recompute the expected key on demand from source_table/started_at/watermarks, reusing build_bronze_key — needs no new column and looks cheaper. It breaks the moment the key-building logic itself ever changes (a different date format, say): every historical row would silently get the wrong expected key computed for it, and reconciliation would report false drift for data that's actually fine. Storing the key explicitly means "what actually happened" is what gets compared, always — a strictly more robust invariant for one new nullable column's cost. Nullable specifically because a no-op incremental run (Section 15's empty-batch case) succeeds but writes nothing — bronze_key is NULL for that row, and list_successful_bronze_keys filters WHERE bronze_key IS NOT NULL so a legitimate no-op is never mistaken for "should exist but doesn't."

### 4. IMPLEMENTATION

New file, real and already in your repo:

```python
# ingestion/src/url_shortener_analytics/reconciliation.py

@dataclass
class ReconciliationResult:
    orphaned_objects: list[str] = field(default_factory=list)
    missing_objects: list[str] = field(default_factory=list)

    @property
    def clean(self) -> bool:
        return not self.orphaned_objects and not self.missing_objects


def find_orphaned_bronze_objects(engine, s3_client, bucket, pipeline_name=None, prefix="bronze/") -> list[str]:
    actual_keys = set(list_bronze_keys(s3_client, bucket, prefix))
    known_keys = set(metadata.list_successful_bronze_keys(engine, pipeline_name))
    return sorted(actual_keys - known_keys)


def find_missing_bronze_objects(engine, s3_client, bucket, pipeline_name=None) -> list[str]:
    known_keys = metadata.list_successful_bronze_keys(engine, pipeline_name)
    return sorted(key for key in known_keys if head_object(s3_client, bucket, key) is None)


def reconcile_bronze(engine, s3_client, bucket, pipeline_name=None) -> ReconciliationResult:
    return ReconciliationResult(
        orphaned_objects=find_orphaned_bronze_objects(engine, s3_client, bucket, pipeline_name),
        missing_objects=find_missing_bronze_objects(engine, s3_client, bucket, pipeline_name),
    )
```
Wired into cli.py's reconcile-bronze command — exits 1 unless result.clean.

### 5. CODE WALKTHROUGH

find_missing_bronze_objects reuses head_object (which you already read in object_store.py) rather than a bulk listing — it's a targeted existence check per known key, not "list everything and diff," because the question here is specifically "does this key I believe I wrote still exist," not "what's in the bucket generally" (that's find_orphaned_bronze_objects's job, which does need the bulk listing).

Both functions are plain set arithmetic once you have the two lists — the actual engineering decision already happened, back in Section 17.3, in how known_keys gets populated (stored, not recomputed). The functions themselves are almost trivially simple, which is the intended shape: a small, cheap check built on top of data that was already being durably recorded for other reasons.

### 6. RUN

bash
python -m url_shortener_analytics.cli reconcile-bronze

Clean state: "bronze reconciliation clean", exit 0. To manufacture drift, delete a real object directly from MinIO (or upload a stray one under bronze/) without going through the pipeline, then rerun — expect one "missing bronze object" (or "orphaned bronze object") warning line per drifted key, and exit 1.

### 7. EXPERIMENT

Think through this before running it: if you manually DELETE FROM ingestion_metadata WHERE run_id = '<some real successful clicks run>' — removing the row entirely, not just its bronze_key — what does reconciliation report? Walk it through: list_successful_bronze_keys no longer includes that key (the row is gone), but the real Parquet object is still sitting in MinIO. That object now shows up in actual_keys - known_keys — orphaned, even though nothing is actually wrong with the data itself. This is worth sitting with: reconciliation compares the record against reality, not "reality" against some independent ground truth — deleting the record itself, not the object, is enough to trigger a false-positive-looking orphan. It's not a bug in reconciliation; it's exactly what "the control plane's memory and the storage layer disagree" means, correctly detected, just triggered by an unusual cause (someone tampering with the metadata table itself, rather than the storage).

### 8. PRODUCTION VIEW

Right now reconciliation is a manual, on-demand CLI invocation — nobody's watching for drift unless someone runs reconcile-bronze and reads the output. Production wires this into a scheduled check (nightly, say) with alerting on result.clean == False. At real scale, list_bronze_keys's single list_objects_v2 call (capped at 1,000 keys, no pagination — you already saw this POC simplification in object_store.py) stops being sufficient the moment a table's object count grows past that cap; production needs the paginator. Also worth naming: reconciliation only proves drift exists — it doesn't auto-fix anything. A missing object still needs a human (or an automated backfill trigger) to decide whether to re-run that historical load or accept the gap.

### 9. PRINCIPAL ENGINEER VIEW

The distinction between "this write is idempotent" and "this system never drifts" is a genuinely underappreciated one, and it's a strong thing to say clearly in an interview: idempotency is a local property of one operation retried in isolation; reconciliation is a global property of two independently-maintained sources of truth staying in agreement over time, under influences the idempotent operation itself has no control over (manual intervention, external deletion, partial failures that land in non-overlapping states). Conflating the two — believing "my writes are idempotent, therefore my system can't drift" — is exactly the kind of gap that looks fine in a demo and quietly rots in production.

The bronze_key-storage decision is also a clean example of a recurring principle: prefer storing "what actually happened" over recomputing "what should have happened, assuming today's logic always applied." The second option is always tempting because it needs no schema change — and it's exactly the option that silently breaks the moment your own logic evolves.

### 10. REMEMBER

Idempotent write = safe to retry this one operation. It says nothing about whether the rest of the system stays in sync.
Reconciliation compares two independently-maintained records of truth (the metadata table, the actual bucket contents) and reports where they disagree — it doesn't fix anything by itself.
Store what actually happened (bronze_key on the row) rather than recomputing it later from logic that might change — recomputation quietly breaks every historical row the moment the key-building function's behavior changes.
NULL for "nothing was written" beats a sentinel value — let SQL's own "no value" do the work instead of inventing a magic string someone has to learn.

### 17.9 Principal Engineer Interview Questions

**Q: "You've built idempotent writes to Bronze. Does that alone guarantee
`ingestion_metadata` and the actual bucket contents can never disagree?
Why or why not?"**

*What's tested:* whether the candidate distinguishes "this specific
write is safe to retry" from "the system as a whole never drifts" — two
related but genuinely different guarantees that are easy to conflate.

*What a weak answer looks like:* "Yes, idempotent writes mean it's always
consistent" — conflates the two guarantees.

*What a strong answer covers:* no. Idempotency guarantees that *retrying
the exact same logical write* is safe and produces the same end state —
it says nothing about a manual out-of-band upload, an object deleted by
something outside this pipeline, or (concretely, in this repo) the
documented Section 15.7 edge case where a retry under changed conditions
computes a *different* key and lands a second, non-colliding object.
Detecting that kind of drift needs an independent check — comparing the
control plane's record against the actual storage listing — which is what
reconciliation is for, and which idempotent writes alone were never
designed to provide.

*Concepts:* the difference between write-level idempotency and
system-level consistency; drift as a category of bug distinct from
duplicate-write bugs.

*Expected follow-up:* "How would you detect drift caused by something
completely outside this pipeline — say, someone manually deleting an
object in the AWS console?" — Exactly `find_missing_bronze_objects`:
it doesn't know or care *why* a recorded key is gone, only that it is.

*Common mistake:* describing reconciliation as "redundant" with
idempotent writes, rather than as covering a genuinely different failure
class that idempotent writes structurally cannot cover.

**Q: "Your reconciliation job only detects drift, it doesn't fix
anything. Isn't that half-finished? Why not auto-delete orphans it
finds?"**

*What's tested:* risk judgment — whether the candidate can articulate a
concrete scenario where the "obvious" next feature (auto-remediation)
would actually be dangerous, not just assert a general preference for
caution.

*What a weak answer looks like:* "Deleting data automatically is risky" —
true, but generic; doesn't show the candidate has actually thought through
*this* system's specific failure mode.

*What a strong answer covers:* a concrete false-positive: a run that just
completed and wrote its Bronze object successfully, whose
`finish_run_success` UPDATE hasn't committed yet (or committed a moment
after `reconcile-bronze`'s query ran) looks *identical*, from
reconciliation's point of view, to a truly orphaned stray object — both
are "in storage, not yet recorded as known." Auto-deleting on that
signal alone would delete a legitimate, just-written object purely
because of a race with normal, expected latency between a write
completing and its checkpoint committing. Detection-only, with
remediation left to a human who can check additional context (how old is
this object, is there a run for it still in flight), avoids this
specific failure entirely, at the cost of needing a human step before
drift actually gets cleaned up.

*Concepts:* race conditions between a write and its own metadata commit;
separating detection from remediation when they have different risk
profiles; designing for the false-positive case, not just the true-positive
case.

*Expected follow-up:* "How would you eventually automate remediation
safely, if the manual step became a real operational burden?" — Add an
age threshold (only ever auto-remediate an orphan older than some safe
margin past any plausible in-flight run duration — directly reusing
Section 16's `max_runtime_minutes` reasoning), and require the run that
"should have" recorded it to be conclusively ruled out first, not just
absent from a single query.

*Common mistake:* treating "detection only" as an unfinished MVP rather
than as a deliberate scope boundary chosen because the alternative has a
real, specific failure mode — the interview signal is in naming *why*,
concretely, not just agreeing caution is generally good.

---

## 18. Object Storage Fundamentals ✅✅

### 18.1 Concept

**Object storage** holds data as opaque, whole objects — each addressed by
a flat `(bucket, key)` pair, written and read over HTTP verbs
(`PUT`/`GET`/`HEAD`/`DELETE`/`LIST`), with no concept of in-place partial
writes or a real directory tree underneath. This repo has been using
object storage since Section 14 — every `write_bronze` call is a `PUT` to
MinIO — but always in service of *landing Bronze data*, never taught as
its own subject. This section is that dedicated treatment: what an object
store actually is, structurally, and why its two most surprising
properties for anyone coming from a filesystem — no partial-object edits,
and no *real* directories, just key prefixes that merely *look* like
them — shape almost every design decision already made in this codebase
(deterministic whole-object keys in Section 14/15, `list_objects_v2`
prefix scans in Sections 17/20).

### Why does this exist?

A relational database and a local filesystem both assume the caller wants
to *modify* data: update a row in place, seek into a file and overwrite ten
bytes in the middle. Object storage deliberately does not offer this — an
object is replaced *wholesale* or not at all. That sounds like a
limitation, and for OLTP-style workloads it would be. But for landing raw,
append-mostly extraction output at scale, giving up in-place mutation buys
something valuable in return: no locking, no partial-write corruption to
reason about, effectively unlimited horizontal scale (a bucket doesn't run
out of "disk"), and a uniform HTTP API that's become a de facto industry
standard — the same `boto3` client code this repo writes against MinIO
today works unchanged against real AWS S3, and largely unchanged against
Google Cloud Storage's S3-compatible endpoint or Ceph's RGW. That
portability is *why* ADR-002 chose it over, say, a second Postgres schema
for raw data.

### Simple Example (generic, pre-URL-Shortener)

A local filesystem: `open("data.csv", "r+")`, seek to byte 500,
overwrite ten bytes, close — a genuine in-place partial edit, and
`ls data/2026-09-19/` walks a real directory the operating system
maintains. Object storage, generically: there is no `open(..., "r+")` at
all — the SDK's primitive is `put_object(Bucket=..., Key=..., Body=...)`,
which always writes the *entire* object; changing even one byte means
re-uploading the whole thing under the same key. And `list_objects_v2(
Prefix="data/2026-09-19/")` doesn't walk a directory — MinIO/S3 doesn't
have directories at all; it string-matches every object's key against the
prefix and returns whichever ones match. `data/2026-09-19/` "look" like a
folder purely by convention (a `/` character in a key), not because the
store tracks it as one.

### URL Shortener Example

`get_s3_client(settings)` builds a `boto3` S3 client pointed at
`http://localhost:9000` (MinIO's S3 API port; `9001` is its separate web
console), authenticated with the static access/secret key pair from
`.env`. Every Bronze object this repo has ever written lives in one
bucket, `analytics-lake`, under keys that *look* hierarchical
(`bronze/clicks/ingestion_date=2026-09-19/clicks.parquet`) purely by the
`/`-in-a-string convention described above — there is no
`bronze/clicks/` directory MinIO is separately tracking; `list_bronze_keys`
(Section 17) works precisely because it's a string-prefix match over a
flat key space, not a directory walk.

### 18.2 Architecture

```
 docker-compose.yml
   ┌───────────┐   depends_on (healthy)   ┌──────────────┐
   │   minio    │◀────────────────────────│ createbuckets │
   │ (S3 API,   │                         │ (mc mb --...  │
   │  port 9000;│                         │  ignore-exist)│
   │  console,  │                         └──────────────┘
   │  port 9001)│      runs once, on `docker compose up`,
   └─────┬──────┘      creates the `analytics-lake` bucket
         │             if it doesn't already exist
         │  HTTP (S3 API): PUT / GET / HEAD / LIST / DELETE
         │
   ┌─────▼──────────────────────────────────────┐
   │ boto3 S3 client (object_store.get_s3_client) │
   │   endpoint_url = settings.minio_endpoint      │
   │   signature_version = "s3v4"                  │
   │   retries = {"max_attempts": 0}  (this repo's │
   │     OWN retry loop in _put_parquet_with_retry │
   │     handles retries -- boto3's built-in retry │
   │     logic is deliberately disabled so exactly │
   │     one retry policy is in effect, not two    │
   │     stacked, hard-to-reason-about ones)        │
   └─────┬──────────────────────────────────────┘
         │
         ▼
   analytics-lake/
     bronze/urls/ingestion_date=.../urls.parquet
     bronze/users/ingestion_date=.../users.parquet
     bronze/clicks/incremental/watermark_start=.../clicks.parquet
     (flat key space -- the tree above is a READING convenience,
      not a real filesystem MinIO maintains)
```
### 1. CONCEPT
First, what problem are we even solving?

Every computer program needs somewhere to put data so it survives after the program stops running. You already know two ways to do this.

The first way is a file on a hard disk. You've used this your whole life. open("notes.txt", "w"), write some text, close the file. The operating system keeps track of where that file lives, inside folders, inside other folders.

The second way is a database, like Postgres, which you've already used a lot in this project. A database stores structured rows and lets you update one field of one row without touching anything else.

Object storage is a third way. It's neither a filesystem nor a database. It's a much simpler idea: a giant warehouse of numbered boxes, where each box holds one blob of data, and you can only ever put a box in or take a box out whole. You can't reach into a box and change one item inside it.

The generic analogy: a self-storage warehouse

Picture a real self-storage facility, like the kind that rents out storage units.

Every unit has a unique label, like Unit-4471. That label is the only way to find your stuff. There's no "walking down the third aisle, second shelf" the way a filesystem folder structure works. You just say "give me Unit-4471" and the warehouse hands it to you.
You can put a sealed box into Unit-4471. You can take that box back out. You can replace it with a completely different sealed box, still labeled Unit-4471.
You cannot open the box, take out one item, and put a different item in its place. If you want to change anything inside, you take the whole box away and drop off a whole new box.
If you rent a lot of units, and you name them cleverly, like photos/2024/vacation.zip and photos/2025/vacation.zip, it looks like you have folders called photos, 2024, 2025. But the warehouse doesn't actually have a folder called photos. It just has flat labels that happen to contain slash characters. The "folder look" is something you imaged onto the labels, not something the warehouse tracks.

Hold onto this analogy. Every real object storage system, including the one in your project, works exactly like this warehouse.

Now the real technical terms

Let's define each term exactly once, in plain words, before using it again.

Object. A single blob of data. Could be a photo, a video, a Parquet file, anything. In our warehouse analogy, this is the sealed box.

Key. The unique label used to find one object. In our analogy, this is Unit-4471. In your project, a real key looks like bronze/clicks/ingestion_date=2026-09-24/clicks.parquet. It's just a string of text. Nothing more.

Bucket. The warehouse itself, the top-level container that holds every object. In your project, the bucket is named analytics-lake.

Prefix. The part of a key before some marker, usually the last slash. If your key is bronze/clicks/ingestion_date=2026-09-24/clicks.parquet, then bronze/clicks/ is a prefix of it. Prefixes are how you search for a group of related objects, by asking "give me every key that starts with this exact text."

HTTP verb. Object storage isn't accessed through normal file-opening code like open(). It's accessed over the network, using the same HTTP protocol your web browser uses to load a webpage. There are five operations you need to know:

Verb	Plain meaning
PUT	Upload an object at a given key. Replaces whatever was already there.
GET	Download the full object at a given key.
HEAD	Ask "does this key exist, and how big is it?" without downloading the actual data.
LIST	Ask "give me every key that starts with this prefix."
DELETE	Remove the object at a given key.

That's the entire vocabulary of object storage. Five verbs, plus the idea of a bucket and a key.

The two properties that trip people up, explained slowly

Property 1: no partial edits, ever.

With a normal file, you can open it, seek to byte 500, overwrite ten bytes, and close it. Object storage has no equivalent of this. The only write operation is PUT, and PUT always uploads the entire object, from scratch, replacing anything that was there before under that key. If you want to change even one byte of a 10-gigabyte file, you have to re-upload all 10 gigabytes again, under the same key.

Why does an entire technology choose to work this way, when it sounds so much more limited than a filesystem? Because giving up partial edits removes a whole category of hard problems. Two different programs can't corrupt each other by writing to different parts of the same file at the same time, because "different parts of the same file" doesn't exist. There's no lock to acquire, no lock to forget to release, no half-written file if a program crashes mid-write, since a crash mid-PUT just means the old object under that key is still there, untouched, until the new PUT finishes successfully. That simplicity is what lets object storage scale to holding an effectively unlimited number of objects, spread across enormous numbers of physical machines, without needing to coordinate those machines the way a shared filesystem would.

Property 2: there are no real folders, only labels that look like folders.

In our warehouse analogy: the warehouse doesn't have a section called photos. It has flat unit labels, and some of those labels happen to contain the text photos/. When you ask "show me everything under photos/", the warehouse just scans every label and returns the ones that start with those exact characters. It's a text comparison, not a folder lookup.

This matters in practice for two big reasons. First, "listing a folder" in object storage is always at least as expensive as scanning through however many keys share that prefix. Filesystems can jump straight to a folder's contents because the operating system maintains a real directory structure; object storage can't take that shortcut. Second, you can accidentally match things you didn't mean to. If you ask for everything under the prefix bronze/cli (missing the trailing slash), you'll get back bronze/clicks/... and also, hypothetically, bronze/client_events/... if that ever existed, purely because both strings happen to start with the same six letters. This is exactly why real code always builds prefixes with an explicit trailing slash.

### 2. URL SHORTENER EXAMPLE (your project's real code)

Now let's connect the warehouse analogy to your actual code.

S3 and MinIO, defined. Amazon invented a specific object storage product called S3 ("Simple Storage Service"), and its HTTP API (the exact shape of the PUT/GET/HEAD/LIST/DELETE requests) became so widely used that it's now a de facto industry standard. MinIO is a separate, open-source piece of software that you can run yourself, on your own laptop or your own servers, which speaks that exact same S3 API. Your project runs MinIO locally instead of paying for real AWS S3, but the code you write doesn't know or care which one it's actually talking to, because both understand the identical HTTP requests.

boto3, defined. boto3 is the official Python library AWS publishes for talking to S3 (and every other AWS service). It handles building the correct HTTP requests for you, so your code calls plain Python functions like s3_client.put_object(...) instead of constructing raw HTTP requests by hand.

Here's the real function in your codebase that builds this connection:

```python
def get_s3_client(settings: Settings) -> BaseClient:
    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
        config=Config(signature_version="s3v4", retries={"max_attempts": 0}),
    )
```
Walking through this line by line:

boto3.client("s3", ...) says "build me a client that speaks the S3 API." This is the same call you'd make to talk to real AWS S3.
endpoint_url=settings.minio_endpoint is the one line that changes everything. It tells boto3 "don't talk to Amazon's real servers, talk to this address instead" — which in your sandbox is http://localhost:9000, MinIO running on your own machine. If you deleted this one line, and had real AWS credentials, this exact same code would talk to real AWS S3 instead. Nothing else in your entire ingestion pipeline would need to change.
aws_access_key_id / aws_secret_access_key are a username/password pair, used to prove your code is allowed to read and write this bucket. This is a simplification worth naming honestly: real production systems use short-lived, automatically-rotating credentials instead of a fixed password sitting in a config file, because a leaked long-lived key is a serious security problem.
retries={"max_attempts": 0} turns off boto3's own built-in automatic retrying. We'll come back to exactly why in the Design section below.

And here's the function that builds a key, the "unit label" from our warehouse analogy:

```python
def build_bronze_key(table_name: str, run_date: datetime) -> str:
    return f"bronze/{table_name}/ingestion_date={run_date:%Y-%m-%d}/{table_name}.parquet"

For table_name="clicks" and today's date, this returns the plain text string bronze/clicks/ingestion_date=2026-09-24/clicks.parquet. That's it. It's just building a string. There is no folder being created anywhere. MinIO will simply remember "there is an object whose label is exactly this string."
```
### 3. DESIGN
Architecture: how the pieces connect
```
 Your ingestion code (extract_full.py, extract_incremental.py)
         │
         │  calls object_store.write_bronze(...)
         ▼
 object_store.py
         │
         │  builds a boto3 S3 client, pointed at MinIO
         │  sends a PUT request over HTTP
         ▼
 MinIO (running in Docker, on your machine)
   listens on two different ports:
     - port 9000: the actual S3 API (PUT/GET/HEAD/LIST/DELETE)
     - port 9001: a separate web dashboard you can view in a browser
         │
         ▼
 Inside MinIO: one bucket, "analytics-lake"
   holding a flat list of objects, e.g.:
     bronze/urls/ingestion_date=2026-09-24/urls.parquet
     bronze/users/ingestion_date=2026-09-24/users.parquet
     bronze/clicks/incremental/watermark_start=.../clicks.parquet
```
   (the indentation above is just for YOUR eyes -- MinIO stores
    these as one flat list of strings, with no real tree structure)
Why boto3's own retries are turned off

You already learned, back in Full Load Ingestion, that _put_parquet_with_retry is a function in your own code that retries a failed write a few times, with a short pause between attempts. That's your own, custom retry logic.

boto3 also ships with its own, separate, built-in retry logic, which is on by default. If you left both enabled at once, a single failed write could get retried by two different systems, each with its own timing, stacked on top of each other. That's genuinely confusing to debug: if a write takes an unexpectedly long time to fail, is that your retry loop, boto3's retry loop, or both firing at once? Setting retries={"max_attempts": 0} disables boto3's copy, so there's exactly one retry policy in this whole codebase, and it's the one you can actually read, in Section 14.4's _put_parquet_with_retry.

The real design decision: one bucket, or many?

Here's a genuine engineering choice your project had to make, and it's worth understanding both sides.

As your project grows, it won't just have Bronze data. Section 2 of this learning plan already told you Silver and Gold layers are coming later. Where should those live?

Option A, the one chosen: one bucket, analytics-lake, with the layer name baked into the key as a prefix. So Bronze objects start with bronze/, and later, Silver objects will start with silver/, all inside the exact same bucket.

Option B, considered and rejected: a separate bucket per layer. So you'd have analytics-lake-bronze, analytics-lake-silver, analytics-lake-gold as three entirely separate buckets.

To understand why Option B is tempting, you need one more term defined: IAM. IAM stands for Identity and Access Management. It's the system, in AWS and similar platforms, for controlling who is allowed to do what. An "IAM policy" is a rule like "this particular username is allowed to read from bucket X, but not bucket Y." A bucket is the natural unit these policies attach to; you write one policy per bucket.

So Option B's real advantage is access control: with three separate buckets, you can write a rule like "the BI reporting team can only read from the Gold bucket, never Bronze," and that rule is simple and bulletproof, because it's enforced at the whole-bucket level. With Option A (one shared bucket), you can't write that same rule as easily. You'd need a more complicated kind of rule that looks inside the bucket at the key itself, which AWS supports but which is harder to write and easier to get wrong.

So why did this project still choose Option A? Because right now, in Phase 1, there's exactly one person (you) operating this whole pipeline, with no separate teams needing separate access. Setting up three buckets, three sets of permissions, and three things to keep in sync is real, unnecessary work at this stage, for a security boundary nobody currently needs. One bucket keeps the code simpler too: there's exactly one config value, MINIO_BUCKET, instead of three.

The honest trade-off, stated upfront rather than hidden: if a future need for that Bronze/Silver/Gold access boundary becomes real, someone will have to physically move every single object into new buckets. You can't just rename a bucket the way you can rename a folder. That's a real, non-trivial migration, and it's worth knowing this cost exists now, rather than discovering it by surprise later.

	One bucket, prefix-separated (chosen)	Bucket per layer
How many buckets to set up	1	3 or more
Can restrict "this team can only see Gold"	Not easily	Yes, naturally
Config values needed	1 (MINIO_BUCKET)	1 per bucket
Cost of switching to the other option later	Must physically move every object	N/A, already separated
Right choice for a single-person Phase 1 project	Yes	Not yet, too much setup for no current benefit
4. IMPLEMENTATION

The one genuinely new function in this section, already written and sitting in your object_store.py:

```python
def get_bucket_stats(s3_client: BaseClient, bucket: str, prefix: str = "bronze/") -> dict[str, int]:
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    contents = response.get("Contents", [])
    return {"object_count": len(contents), "total_bytes": sum(obj["Size"] for obj in contents)}
```
### 5. CODE WALKTHROUGH

Let's go through every line.

s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix). This sends the LIST request from our vocabulary table. It says "search the warehouse for every unit label that starts with this prefix text." In your project, that's every key starting with bronze/.

response.get("Contents", []). The response from list_objects_v2 comes back as a Python dictionary. If any objects matched, they're listed under the key "Contents". If nothing matched (say, the bucket is completely empty), that key might be missing entirely from the response, so .get("Contents", []) says "grab that list if it exists, otherwise just give me an empty list," instead of crashing with a KeyError.

{"object_count": len(contents), ...}. len(contents) just counts how many matching objects came back. This is the total number of "boxes" currently sitting in the warehouse under that prefix.

"total_bytes": sum(obj["Size"] for obj in contents). Here's the detail worth slowing down on. Each item inside contents isn't just a key string. It's a small dictionary that already includes metadata about that object, including its size in bytes, under the key "Size". This line adds up the "Size" field across every matching object.

Why this matters, and the mistake it avoids: a tempting-but-wrong way to write this function would be to first call list_bronze_keys to get just the key names, and then call head_object once per key to separately ask "how big is this one?" That would work, but it wastes network calls. Each head_object call is its own separate HTTP round-trip to MinIO. If you have 500 objects, that's 500 extra network requests, just to re-fetch information the single list_objects_v2 call already handed you for free, in one response. get_bucket_stats avoids this by reusing the size field that was already sitting in the LIST response.

### 6. RUN

Real command, real expected shape of the output:

bash
make storage-stats

This wraps python -m url_shortener_analytics.cli storage-stats --prefix bronze/ under the hood.

Try this exact sequence yourself, checking the output after each step:

```bash
make storage-stats          # expect: object_count=0, total_bytes=0 (nothing ingested yet)
make ingest-full             # writes urls.parquet and users.parquet
make storage-stats          # expect: object_count=2
make ingest                  # clicks, first incremental run, one more object
make storage-stats          # expect: object_count=3
make ingest-full             # SAME calendar day -- overwrites urls.parquet/users.parquet in place
make storage-stats          # expect: object_count STILL 3, not 5
```
### 7. EXPERIMENT (hands-on, including a "why did it not change" check)

The last make storage-stats call above is the actual proof this exercise is testing. If you run make ingest-full a second time on the same day, and object_count jumps to 5 instead of staying at 3, something is wrong with build_bronze_key's determinism from Section 14. It should never grow, because the second ingest-full overwrites the exact same two keys instead of creating new ones. Run this yourself and confirm the number really does stay at 3.

Now a break-it exercise, to build real intuition about the "no real folders" property. Open a Python shell (or a one-off script) and, using the same s3_client, deliberately call:

```python
s3_client.list_objects_v2(Bucket="analytics-lake", Prefix="bronze/cli")
```
Notice the missing trailing slash: bronze/cli, not bronze/clicks/. Predict what comes back before you run it. Because this is a plain string-prefix match, not a folder lookup, it will return every key starting with those exact six characters, bronze/cli, which happens to include your real bronze/clicks/... objects. If your project ever had a table named something like client_events, its Bronze keys would also match this same broken prefix, purely by coincidence of spelling, and you'd get back a mixed, wrong result with no error or warning at all. Now go check list_bronze_keys_for_date_range and get_file_layout_report in your real object_store.py, and confirm every prefix they build always ends in a trailing /. That trailing slash is not a style choice; it's the fix for exactly this bug.

### 8. PRODUCTION VIEW

Two real limits worth knowing before you hit them.

The 1,000-key cap. list_objects_v2, by default, only returns up to 1,000 keys per call, even if a prefix actually matches more than that. Your current code (list_bronze_keys, get_bucket_stats) makes exactly one list_objects_v2 call and trusts that it got everything. At your project's current scale, a few dozen objects per table at most, this is completely safe. Once a real production table accumulates more than 1,000 objects under one prefix, get_bucket_stats would silently under-report the true total_bytes, because it would only ever see the first 1,000 keys. The real fix is pagination: boto3 provides a paginator object specifically for this, which automatically makes as many follow-up LIST calls as needed and stitches the results together.

The access-control limit. As covered in the Design section, one shared bucket makes it genuinely harder to restrict "team A can only read Gold data" the way separate buckets would. This becomes a real requirement the moment more than one team, with different trust levels, needs to read from this data lake.

### 9. PRINCIPAL ENGINEER VIEW

If an interviewer asks "why doesn't object storage support in-place edits," the strong answer isn't "it just doesn't." It's connecting that limitation back to what it buys in return: no locking needed between concurrent writers, no half-written file if a write crashes partway through, and the ability to scale to effectively unlimited objects across many physical machines without needing those machines to coordinate closely with each other. That trade, giving up partial edits in exchange for massive, simple horizontal scale, is the single idea that explains almost every other object storage design choice you'll ever run into.

A second strong thing to be able to say: "prefixes are not real folders, they're string matching," and be ready to give the concrete failure example from the Experiment section above, a missing trailing slash accidentally matching an unrelated key. That's the kind of specific, first-hand detail that separates "I've read about this" from "I've actually used this."

### 10. REMEMBER
Object storage is a warehouse of labeled, whole boxes. You put a whole box in, or take a whole box out. You never reach inside one.
A "key" is just a text label. A "bucket" is the whole warehouse. There is no real folder tree underneath, only labels that happen to contain slashes.
PUT always replaces the entire object. There is no operation for editing part of one.
MinIO and real AWS S3 speak the exact same HTTP API, which is why the same boto3 code works against both, unchanged.
One shared bucket with prefixes is simpler to run; separate buckets per layer give cleaner access control. Pick based on how many people/teams actually need different permissions right now, not hypothetically.

### Principal Data Engineer Perspective

The judgment call worth defending here is naming Section 18.7's gap
explicitly rather than quietly working around it or, worse, not noticing
it at all. It would have been easy to write `storage_stats_command`,
watch it work against a bucket that already exists (which is all `make
up` ever leaves this repo with), and never exercise the one code path
where it breaks. Principal-level code review habitually asks "what
happens when this specific call fails," not just "does this work for the
input I tested" — and the honest answer here, confirmed by actually
running the failure case rather than guessing about it, is that three of
this file's six commands (`check-stale-runs`, `reconcile-bronze`,
`storage-stats`) have real, currently-unpatched gaps in their own
top-level error handling, inconsistent with the other three. Naming this
plainly, as a known limitation rather than a silent one, is worth more to
a reader evaluating this repository than quietly fixing it without
comment would be — it demonstrates the review habit itself, not just its
output. The second thing worth flagging: the single-bucket-with-prefixes
decision (18.3) is a real, if modest, piece of technical debt being taken
on deliberately — it trades away clean per-layer IAM boundaries for
Phase 1's actual, current operational simplicity, and says so plainly
rather than presenting "one bucket" as obviously, permanently correct.

**Q: "Someone asks you why S3 (or MinIO) can't just support editing ten
bytes in the middle of a large object the way a local filesystem can.
What would you tell them, and why does this repo's code look the way it
does as a direct consequence?"**

*What's tested:* whether the candidate understands object storage's core
constraint as a deliberate architectural trade-off, not an arbitrary
limitation — and can trace a concrete design consequence back to it.

*What a weak answer looks like:* "Object storage just isn't built for
that" — true but circular; doesn't explain *why not*, or connect it to
anything concrete.

*What a strong answer covers:* whole-object semantics are what let object
storage scale the way it does — no per-object lock manager, no
concurrent-writer coordination for partial updates, uniform HTTP
semantics regardless of object size. The direct consequence in this
codebase: `build_bronze_key`/`build_bronze_incremental_key` (Sections
14-15) are deterministic specifically *because* the only way to safely
"correct" a previous write is to overwrite the whole object under the
same key — there's no `UPDATE ... WHERE id = 5` equivalent available at
the object-storage layer at all.

*Concepts:* whole-object PUT/overwrite semantics; the trade-off between
mutability and horizontal scalability.

*Expected follow-up:* "What would change about this repo's approach if
Bronze objects needed row-level updates?" — A move to a table format like
Delta Lake/Iceberg/Hudi, which layer transactional, row-level semantics
*on top of* immutable object storage via a metadata/log layer, rather
than object storage itself gaining that capability.

*Common mistake:* describing this as a current *limitation of MinIO
specifically*, rather than a property of the object-storage model in
general that MinIO, S3, and GCS all share by design.

**Q: "Your `list_bronze_keys` function treats `bronze/clicks/` as if it
were a folder. Is it actually one? What would break if you assumed it
was?"**

*What's tested:* whether the candidate has internalized the
flat-key-space model deeply enough to predict a concrete failure from
treating prefixes as real directories.

*What a weak answer looks like:* "No, it's just a prefix" — correct but
stops short of a concrete consequence.

*What a strong answer covers:* no — every key in a bucket lives in one
flat namespace; `/` has no special meaning to the store itself, only to
`list_objects_v2`'s optional prefix-matching and (separately) its
`Delimiter` parameter, which this codebase doesn't use. What would break:
code that assumes an empty "folder" can exist (it can't — there's nothing
to list until at least one object with that prefix is written, unlike
`mkdir` creating a real, empty directory), or code that tries to "rename a
folder" as one operation (there's no such primitive — every object under
the old prefix has to be copied to a new key and the old one deleted,
individually, since a prefix isn't a real, addressable entity that can be
renamed).

*Concepts:* flat key namespace vs. hierarchical filesystem; prefix
matching as a string operation, not a directory traversal.

*Expected follow-up:* "How would you efficiently rename every object
under a prefix, then, given there's no rename primitive?" — List every
key under the old prefix, copy each to its new key (`copy_object`), then
delete the originals — an operation whose cost scales with object count,
not tree depth, unlike a filesystem `mv`.

*Common mistake:* asserting object storage "has folders" because a
console UI (MinIO's or AWS's) visually renders one — the UI is doing the
same prefix-based grouping this codebase's own key convention relies on,
not exposing a real filesystem feature underneath.

---

## 19. Parquet ✅✅

### 1. CONCEPT
The problem first

Every Bronze object you've written so far has been a Parquet file. You've seen the code call pq.write_table(...), but we've never stopped to ask: what actually is Parquet, and why not just write plain CSV files instead? CSV is simpler. Anyone can open it in a text editor. Let's build up to the real answer slowly.

Row-oriented vs. column-oriented, defined from scratch

Imagine a table of data, like a spreadsheet of employees. It has three columns: id, name, salary. It has three rows, for Alice, Bob, and Carol.

There are two fundamentally different ways to physically lay this data out in a file.

Row-oriented layout. Store one complete row, then the next complete row, then the next. This is exactly how CSV works.
```
1,Alice,50000
2,Bob,60000
3,Carol,70000
```
Column-oriented layout. Store every value from one column together, then every value from the next column together.
```
[1, 2, 3]
[Alice, Bob, Carol]
[50000, 60000, 70000]
```
Now ask yourself: you need the average salary. You don't care about id or name at all. How much of the file does each layout force you to read?

In the row-oriented file, there is no way to skip straight to the salaries. The salaries are scattered, one per row, mixed in between the other columns. You have to read every single row, in full, and only then discard the id and name you didn't need.

In the column-oriented file, the salaries are one contiguous block. You jump straight to that block and read only that. The id and name blocks are never even touched.

This is the entire idea behind Parquet. Parquet is a column-oriented file format. It groups the data column by column instead of row by row.

Two more properties, defined plainly

Parquet is binary, not text. CSV is plain text. You can open it in Notepad and read it with your own eyes. Parquet stores its data in a compact, specially-encoded binary format, the way a .jpg image or a .zip file is binary. This makes Parquet files smaller and faster for a computer to process, but it means a human can't just open one and read it directly. You need a Parquet-aware tool.

Parquet is self-describing. A CSV file is just raw text; nothing in the file itself tells you that the second column is supposed to be a whole number. Whatever program reads it has to guess, or has to be told separately. Every Parquet file, by contrast, ends with a small section called a footer, which stores the exact schema (every column's name and data type) and some statistics about the data. Any program reading the file can look at the footer first and know exactly what it's dealing with, with no guessing and no separate documentation needed.

Why does any of this matter for a real pipeline?

Go back to Section 2 of this whole learning plan, where you first learned the difference between OLTP and OLAP. Analytical queries, the kind this entire platform exists to answer, very commonly touch only a handful of columns out of many, across a huge number of rows. "What's the average time-to-click, broken down by device_type, across five million rows" only needs two of your clicks table's six columns. A row-oriented format forces you to pay the cost of all six columns anyway, for every single row. A column-oriented format only makes you pay for the two you actually asked for. That difference gets larger, not smaller, as your data grows, which is exactly the shape of workload this whole project is built around.

### 2. URL SHORTENER EXAMPLE

You've already been writing Parquet since Section 14, without a dedicated look at how. Here's the actual function, already sitting in your object_store.py:

```python
def _dataframe_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, buffer, compression="snappy")
    return buffer.getvalue()
```
Walking through each line:

io.BytesIO() creates an empty, in-memory buffer. Think of it as a temporary file that lives only in RAM, never touching the disk. This is used instead of a real temp file because the result needs to go straight into an S3 PUT request, not sit on disk first.

pa.Table.from_pandas(df, preserve_index=False). pa is PyArrow, a library built specifically for working with columnar data in memory, and it's the engine that actually knows how to write the Parquet format. This line converts your pandas DataFrame into PyArrow's own columnar Table structure, which is the format PyArrow's Parquet writer expects. preserve_index=False tells it to drop pandas' automatic row-numbering index, so it doesn't get written into the file as an extra, unwanted column.

pq.write_table(table, buffer, compression="snappy"). This is the actual write. pq is PyArrow's Parquet-specific module. This line encodes the table into the real Parquet binary format and writes those bytes into the in-memory buffer from step one. compression="snappy" names the compression algorithm to use. Compression codec, defined: an algorithm that shrinks data by finding and removing repetition, at the cost of some CPU time to compress and later decompress it. Snappy is chosen here specifically because it's fast to compress and decompress, at a moderate compression ratio, a good default when write/read speed matters more than squeezing out every possible byte.

### 3. DESIGN
The design decision: benchmark at two different sizes, not one

Your project made a genuine, deliberate choice here, worth understanding in full. ADR-003, an earlier design decision record in your project, had already asserted that Parquet would be "efficient for analytical, column-selective reads," two increments before anyone actually measured it. This section is where that assertion finally got tested against real numbers.

Here's the decision: run the benchmark twice. Once against your project's real, small seeded clicks table, which only has 5,000 rows. Once against a synthetic table of the exact same shape, but with 200,000 rows.

Why not just benchmark once, against the real data you already have? Because 5,000 rows turns out to be small enough that Parquet's advantage isn't fully visible yet, for a reason explained below. Benchmarking only at that scale would have quietly given a misleading picture.

Why not just benchmark once, against a large synthetic dataset? Because then the benchmark never actually touches your project's own real data at all, and a reader can't be sure the result generalizes back to the thing they're actually building.

Running both costs more script complexity and roughly double the runtime. In exchange, it produces an honest result instead of a convenient one. This matters enough that it's worth a general engineering habit, not just a one-off decision: measure a claim at the scale where it will actually apply, not at whatever scale happens to be easiest to test.

Amortization, defined, since it explains the real numbers below

Amortize means to spread a fixed, one-time cost across many uses, so its per-use impact shrinks the more you use it. Think of buying a $100 toolbox. If you use it once, that tool cost you $100. If you use it a thousand times, it effectively cost you 10 cents per use. The cost didn't change; how many times you divided it by did.

Every Parquet file has a fixed cost: that footer we defined earlier (the embedded schema and statistics) has to be written when saving, and parsed when reading, no matter how many rows are in the file. For a file with only 5,000 rows, that fixed cost is a real, noticeable fraction of the total work. For a file with 200,000 rows, that exact same fixed cost gets divided across 40 times more rows, so it barely matters anymore. This is the amortization effect, and it's the reason a benchmark run at only one scale can be misleading.

### 4. IMPLEMENTATION

The benchmark script itself, real and already committed at benchmarks/parquet_vs_csv_vs_json.py. Here's the part of it that isolates the single-column read test, for each format, using each format's own fairest available approach:

python
def _read_csv_one_column(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, usecols=["device_type"])          # scans every row regardless

def _read_json_one_column(path: Path) -> pd.DataFrame:
    return pd.read_json(path, orient="records", lines=True)[["device_type"]]  # no shortcut at all

def _read_parquet_one_column(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path, engine="pyarrow", columns=["device_type"])   # true columnar pushdown
5. CODE WALKTHROUGH

_read_csv_one_column uses pandas' usecols parameter. This tells pandas which columns you actually want back. But it's important to understand what this does and doesn't save you. CSV has no columnar structure at all. Pandas still has to read every single row from start to finish, character by character, because there's no way to know where the device_type value is on a given line without first reading past everything before it. usecols only saves you the work of building unused Python objects for the other columns; it does not save you any disk reading at all.

_read_json_one_column doesn't even get that partial benefit. It reads the entire file into a DataFrame with every column, and only afterward selects ["device_type"]] from the already-fully-loaded result. There is no columnar shortcut in JSON-lines whatsoever.

_read_parquet_one_column passes columns=["device_type"] directly into pd.read_parquet. This is genuine columnar pushdown: the request for "just this column" gets pushed all the way down to the file-reading layer itself. PyArrow looks at the file's footer, finds exactly which byte ranges on disk hold the device_type column, and reads only those bytes. The other five columns' data is never read off disk at all, not even to be discarded.

### 6. RUN

Real commands:

bash
make benchmark-parquet                 # your real, seeded clicks table (~5,000 rows)
make benchmark-parquet ROWS=200000     # a synthetic table of the same shape, 200,000 rows

Your project's guide already recorded a genuine run of both, in this same sandbox. These are real, actually-observed numbers, not projections. Run the command yourself and expect the file-size columns to match exactly; the timing columns will vary a little, since wall-clock timing on a shared machine is never perfectly repeatable.

At 5,000 real rows:
```
format      size_bytes   write_s   full_read_s   one_col_read_s
csv         597311       0.0405    0.0124         0.0063
json        961259       0.0376    0.0288         0.0252
parquet     417649       0.0164    0.0132         0.0024
```
At 200,000 synthetic rows:
```
format      size_bytes   write_s   full_read_s   one_col_read_s
csv         21626749     0.8884    0.4753         0.1645
json        38586801     0.6567    1.0284         0.9612
parquet     4329469      0.0912    0.0596         0.0082
```
###7. EXPERIMENT

Before reading further, make a written prediction. At only 5,000 rows, do you expect Parquet's full-table read to be faster or slower than CSV's? Most people confidently predict "Parquet wins on everything, always." Now look at the real numbers above: at 5,000 rows, Parquet's full read (0.0132s) is essentially tied with CSV's (0.0124s), if anything a touch slower. Only at 200,000 rows does Parquet's full read pull decisively ahead, at 0.0596s versus CSV's 0.4753s, roughly 8 times faster.

This is the amortization effect from the Design section, made concrete. At small scale, the fixed footer/schema overhead isn't paid off yet by the columnar savings. At large scale, it is, overwhelmingly. Notice, though, what doesn't flip between the two scales: the single-column read time favors Parquet decisively at both sizes, already about 2.6 times faster than CSV even at just 5,000 rows. That's the number that actually matters most for this whole project, since selective, few-column reads are exactly the query shape this platform exists to serve.

A second, hands-on experiment, the failure case. Find one of your real Bronze Parquet files on disk, and try to open it directly:

bash
cat data/bronze/urls/ingestion_date=2026-09-24/urls.parquet

You'll see unreadable binary garbage in your terminal. Now try the equivalent with a CSV file, if you have one lying around, or with data/bronze/urls/.../urls.parquet read properly instead:

bash
python3 -c "import pandas; print(pandas.read_parquet('data/bronze/urls/ingestion_date=2026-09-24/urls.parquet').head())"

That second command works, because it uses a Parquet-aware tool. This is a genuine, if small, operational cost worth feeling once with your own hands: debugging "what's actually inside this Bronze object" always requires the right tool for Parquet, unlike CSV or JSON, which you can just open and read.

### 8. PRODUCTION VIEW
Aspect	This project right now	Real production
Compression codec	snappy, chosen for write/read speed	Same is common; some teams switch to zstd for better compression once storage cost matters more than compute cost
Row-group sizing	PyArrow's own default, left unconfigured	Explicitly tuned, once files get large enough that this trade-off starts to matter
Benchmark rigor	One run per scale, honestly labeled as such	Many repeated runs, with variance reported, on dedicated hardware
Schema evolution	Not exercised yet — every write uses one fixed schema	Parquet does support adding/removing columns across files over time, but readers have to be written to expect that
Reading files by hand	Needs a Parquet-aware tool	Same constraint, usually hidden behind a query engine like Spark, Athena, or DuckDB, so nobody inspects raw files directly

Row group, defined, since production tuning depends on it: internally, a Parquet file is split into chunks called row groups, each one holding a subset of the rows, stored in columnar form within that chunk. This lets a reader process different row groups in parallel, or skip a whole row group entirely if its statistics show it can't possibly contain a value you're filtering for. Tuning row-group size is a real trade-off between more parallelism (smaller row groups) and less per-row-group bookkeeping overhead (larger row groups), and it becomes worth touching only once files are large enough for it to matter, which your project's current file sizes are not.

### 9. PRINCIPAL ENGINEER VIEW

The strongest thing to be able to say here isn't "Parquet is faster." It's this exact, more precise claim: "Parquet's advantage in full-table read time only shows up past a certain scale, because of fixed per-file overhead that has to be amortized, but its advantage in selective, few-column reads holds at every scale, including small ones, and that second number is the one that actually matters for this system's real query pattern." That's a materially stronger answer than a flat "Parquet is better," and it shows you actually looked at the numbers instead of repeating a claim you read somewhere.

A second point worth having ready: knowing which number in a table of benchmark results is actually load-bearing for your original design decision, rather than treating every column as equally important. This project chose object storage plus a columnar format specifically because of Section 2's OLAP query pattern, selective, few-column access. The single-column read numbers are the ones that actually justify that decision. The full-table read numbers are interesting, but they're not the reason Parquet was chosen in the first place.

### 10. REMEMBER
Row-oriented storage groups by row (CSV, JSON). Column-oriented storage groups by column (Parquet). A query needing few columns out of many is cheap in the second, expensive in the first.
Parquet is binary and self-describing. Binary means you need a Parquet-aware tool to read it. Self-describing means the schema is embedded in the file's own footer, no external documentation needed.
Amortization explains why Parquet's full-table-read advantage only appears at larger scale: a fixed per-file cost matters a lot when spread over few rows, and barely at all when spread over many.
When several benchmark numbers exist, know which one actually justifies your original design decision. Here, it's single-column read time, not full-table read time.

**Q: "Your own benchmark shows Parquet's full-table read time roughly
tied with CSV's at 5,000 rows, only pulling ahead at 200,000. Does that
mean Parquet was the wrong choice for this project at its current
scale?"**

*What's tested:* whether the candidate can separate "which number
matters for this specific system's actual query pattern" from "which
number looks best in a table" — a genuine judgment call, not a
lookup.

*What a weak answer looks like:* "No, Parquet is always better" —
contradicted by the candidate's own benchmark; a strong answer has to
engage with the actual, honest result, not wave it away.

*What a strong answer covers:* no, because full-table reads (`SELECT *`)
aren't this platform's target query shape at all — Section 2 established
that analytical workloads are column-selective, and the single-column
read numbers (Parquet decisively faster than CSV and JSON at *every*
tested scale, including 5,000 rows) are the metric that actually predicts
this project's real future workload. Choosing a format based on the
metric that matches your actual access pattern, not the metric that's
easiest to headline, is the right call here — and it happens to still be
Parquet, just for a more specific and better-justified reason than "it's
generally faster."

*Concepts:* matching a benchmark's chosen metric to the system's real
access pattern; not treating every number in a results table as equally
decision-relevant.

*Expected follow-up:* "At what row count would you expect Parquet's
full-read time to overtake CSV's, and how would you find out precisely?"
— Somewhere between 5,000 and 200,000 rows for this exact schema and
this exact hardware; finding the precise crossover would mean running
the same benchmark at several intermediate row counts (e.g. 10k, 25k,
50k, 100k) and plotting the trend, rather than guessing from two data
points.

*Common mistake:* citing only the size-reduction numbers (1.43x-8.91x)
as if they alone settled the question, without engaging with the
full-read timing result that doesn't uniformly favor Parquet — a subtler
version of the same "restate the assumption more confidently after
measuring it" failure this section's Principal Perspective calls out.

**Q: "Explain, mechanically, why `pd.read_csv(usecols=["device_type"])`
is NOT the same thing as true columnar I/O, even though it only returns
one column."**

*What's tested:* whether the candidate understands the actual mechanism
behind columnar pushdown, not just that Parquet "supports" it as a
feature checkbox.

*What a weak answer looks like:* "CSV doesn't support columns the same
way" — vague, doesn't explain the actual disk-I/O difference.

*What a strong answer covers:* a CSV file has no way to locate where
`device_type`'s values start without scanning through every preceding
field of every row first — the parser has to read and tokenize the
*entire* line, byte by byte, to even know where that column's value
begins on each row; `usecols` only skips the cost of *building a Python
object* for the columns it discards, not the cost of *reading and parsing
their bytes off disk*. Parquet's footer stores exactly which byte ranges
belong to which column, so `columns=["device_type"]` lets the reader seek
directly to that column's bytes and never touch the other columns' data
at all, at the I/O level, not just the object-construction level.

*Concepts:* the difference between skipping object construction and
skipping disk I/O; Parquet's footer-based column offset lookup as the
actual mechanism, not "compression" or "it's a modern format" as vague
substitutes for a real explanation.

*Expected follow-up:* "Does this same distinction apply to filtering
rows (a `WHERE` clause), not just selecting columns?" — Yes, via Parquet's
per-row-group statistics (min/max per column per row group), which let a
reader skip entire row groups that can't possibly match a filter without
reading them at all — "predicate pushdown," a related but distinct
mechanism from column pushdown, not exercised by this repo's benchmark
but worth naming as the next layer of the same idea.

*Common mistake:* conflating `usecols`' real (but different) benefit —
skipping wasted Python object construction — with Parquet's actual
disk-I/O-level skipping, as though they were the same optimization.

---

## 20. Partitioning ✅✅

1. CONCEPT
The problem first

Imagine your urls table has been full-loaded every single day for the last two years. That's over 700 Bronze objects sitting in MinIO by now. Someone asks a simple question: "show me what Bronze had for urls on September 19th, 2026." How does your code find just that one object, without wading through the other 729?

You already have the tool for "list everything": list_bronze_keys, from Section 17. It calls list_objects_v2 once, gets back every object under bronze/urls/, and hands you all 730 keys. To find just the one date you wanted, you'd then have to filter that list yourself, in Python, throwing away 729 results you never needed. That's real, wasted work: MinIO had to find, package, and send you metadata for 729 objects you were always going to discard.

Partitioning is the fix for exactly this. It means physically organizing where objects live based on the value of some column, usually a column people will commonly filter by, so a search can skip straight to what it needs without even looking at the rest.

The generic analogy: a library, organized two different ways

Picture a library with two years of daily newspapers, one per day.

Unorganized library. Every single newspaper, all 730 of them, is stacked in one giant pile in the lobby, in no particular order. To find September 19th's paper, a librarian has to pick up and check the date on every single paper in the pile, until they find the right one, or confirm it isn't there.

Organized library. The papers are sorted into 730 separate labeled shelf slots, one slot per date, like Sept-01, Sept-02, Sept-03, and so on. To find September 19th's paper, the librarian walks directly to the shelf labeled Sept-19 and picks it up. They never touch any of the other 729 papers at all.

Both libraries hold the exact same papers. The only difference is where each paper physically sits, organized in a way that matches how people are actually going to search for it. That's the entire idea of partitioning.

A term you need first: Hive-style partitioning

Hive-style partitioning is a specific naming convention for the "shelf labels" in our library analogy, when the library is really an object store instead of a real building. Instead of labeling a shelf just Sept-19, you encode both the column name and its value directly into the object's key, as columnname=value. So a key looks like:

bronze/urls/ingestion_date=2026-09-19/urls.parquet

That ingestion_date=2026-09-19 segment is doing exactly the same job as the Sept-19 shelf label. It's not a real folder MinIO tracks specially, as you learned in the Object Storage section. It's just a text convention, but a very useful one, because it lets you predict the exact prefix to search for, before you ever ask MinIO anything.

Partition pruning, the piece that's actually new this section

Having a key=value structure in your keys is only half the story. You have to actually use that structure when searching, instead of listing everything and filtering afterward. Partition pruning means constructing the exact narrow prefix for what you want, and only ever asking the object store for that prefix, so objects outside your range are never even considered by MinIO, let alone returned to you.

You've actually been doing the labeling half of this since Section 14, without ever naming it. build_bronze_key's ingestion_date=2026-09-19/ and build_bronze_incremental_key's watermark_start=.../watermark_end=.../ are both real, already-in-production Hive-style partitioning. What's genuinely new this section is the pruning half: actually exploiting that structure on the read side, instead of just on the write side.

2. URL SHORTENER EXAMPLE

Here's the real function, already in your object_store.py, that does real partition pruning for full-load tables:

python
def list_bronze_keys_for_date_range(
    s3_client: BaseClient, bucket: str, table_name: str, start_date: date, end_date: date
) -> list[str]:
    keys: list[str] = []
    current = start_date
    while current <= end_date:
        date_prefix = f"bronze/{table_name}/ingestion_date={current:%Y-%m-%d}/"
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=date_prefix)
        keys.extend(obj["Key"] for obj in response.get("Contents", []))
        current += timedelta(days=1)
    return keys
3. CODE WALKTHROUGH

Let's go through this one line at a time, since there's a subtlety here worth catching.

keys: list[str] = [] starts an empty list. This will collect every matching key across every date in the range.

current = start_date begins a loop, starting from the first day you asked about.

while current <= end_date: keeps looping, one day at a time, until it passes the last day you asked about.

date_prefix = f"bronze/{table_name}/ingestion_date={current:%Y-%m-%d}/". This is the important line. It builds the exact same prefix string that build_bronze_key would have used when writing that day's object. This has to match exactly, character for character, or the search would silently return nothing for that date.

response = s3_client.list_objects_v2(Bucket=bucket, Prefix=date_prefix). This is the actual LIST call from the Object Storage section, but notice it's scoped to just this one day's prefix, not the whole table's prefix. MinIO will only ever look at, and return, objects whose key starts with this narrow prefix.

keys.extend(obj["Key"] for obj in response.get("Contents", [])). Adds whatever matched for this one day onto the running list. If nothing was written that day, Contents comes back empty, and nothing gets added; no error happens.

current += timedelta(days=1). Moves forward exactly one calendar day, and the loop repeats.

The subtlety worth catching: this function makes one separate LIST call per day in the range, not one call total. A 3-day range means 3 calls. This is deliberate, and it's the whole mechanism. The temptation to avoid is calling list_bronze_keys once for the whole table, and then filtering the combined result down to your date range in Python. That would technically give the same answer, but it would defeat the entire purpose: MinIO would still have had to find and return metadata for every object the table has ever had, including all the dates you didn't want, before your code got the chance to throw most of it away.

4. DESIGN
The real design decision: why isn't there one pruning function for both partition schemes?

Here's something worth noticing: your project actually has two different partitioning schemes in production already. Full-load tables use ingestion_date=2026-09-19 (a calendar date). Incremental-load tables use watermark_start=.../watermark_end=... (an integer id range, from Section 15). You might expect one general "prune by whatever" function that handles both. Your project deliberately built only the date-range version this section, and it's worth understanding exactly why.

A calendar date range is something you can enumerate in advance. If you want September 1st through September 20th, you can list all 20 exact dates yourself, in Python, with nothing but a loop, before ever talking to the database or the object store. A watermark range is fundamentally different. A watermark is an opaque number, discovered by reading ingestion_metadata, and it's not something you can predict or enumerate ahead of time the way a calendar date is. You can't just guess "I want watermarks 5000 through 8000" and build 3,000 individual prefixes; you'd first have to query ingestion_metadata to find out which actual (watermark_start, watermark_end) pairs exist in that range at all, which is a genuinely different code path (and one you already have, list_successful_bronze_keys).

So building one "unified" function that pretended to handle both would have been dishonest: it would either silently do the wrong thing for incremental tables, or need an entirely separate internal path anyway, hidden behind a misleadingly uniform-looking interface. Naming the boundary explicitly (this function handles date-partitioned, full-load tables only) is more trustworthy than a function that looks complete but secretly isn't.

Two functions, kept deliberately separate, not one replacing the other
	list_bronze_keys (Section 17, full listing)	list_bronze_keys_for_date_range (this section, pruned)
Calls issued	One	One per date in the requested range
What S3/MinIO considers	Every object under the table's whole prefix	Only objects under the requested dates' prefixes
Right tool for	"Give me everything" — this is what reconciliation (Section 17) genuinely needs	"Give me a known, bounded range"
Works for incremental tables	Yes	No, by design (see above)
Cost as history grows	Grows forever, with total history	Stays constant, for a fixed-size range
5. EXPERIMENT (the hands-on challenge, worked through before you run it)

Here's a prediction exercise straight from your project's own guide, worth doing before you look at the answer.

Imagine a bucket with one object per day, going back 365 days. You call list_bronze_keys once, and list_bronze_keys_for_date_range once, both asking for the same 30-day range. How many times does each one call list_objects_v2?

Write your guess down before reading on.

The real answer: list_bronze_keys issues exactly 1 call. list_bronze_keys_for_date_range issues exactly 30 calls. More calls, not fewer. If your instinct says "then pruning made things worse," sit with that for a second, because it's the whole point of this exercise.

Here's why 30 calls is still the better choice in this scenario. list_bronze_keys's single call still has to find, package, and transfer metadata for all 365 objects, even though only 30 days were ever wanted. list_bronze_keys_for_date_range's 30 calls, together, only ever transfer metadata for the roughly 30 objects actually needed. Pruning trades call count for data volume. You make more, smaller, cheaper requests, in exchange for never touching data you didn't ask for. That's the right trade whenever the range you want is much smaller than the table's total history.

This trade has a real breaking point, and it's worth knowing before it bites you. If someone asks list_bronze_keys_for_date_range for a 365-day range, on a table that has exactly 365 days of history, it issues 365 separate calls, for zero pruning benefit at all, since there was nothing left to prune. In that specific case, the plain, unpruned list_bronze_keys (one call) would actually have been faster. Nothing in list_bronze_keys_for_date_range's own code warns you about this; it will happily issue however many calls a huge range implies, with no upper limit. A caller reaching for the pruned function with a range that turns out to be someone's entire history has picked the wrong tool for the job.

6. RUN

This one is fully runnable right now, in this sandbox or on your own machine, with no MinIO or Docker needed at all, because it tests against a mocked S3 client rather than a real one:

bash
PYTHONPATH=ingestion/src python3 -m pytest ingestion/tests/unit/test_object_store.py -k date_range -v

Expected: 2 tests pass. One proves a 3-day range issues exactly 3 calls, with the exact three Prefix values checked directly, not just the count. One proves a single-day range issues exactly 1 call.

7. PRODUCTION VIEW
Aspect	This project right now	Real production
Partition granularity	One file per day (full load), or per watermark range (incremental)	Same Hive-style convention is standard; a real deployment also splits an unusually large day's data across multiple files, since your project's current files are actually too small, not too large
Doing the pruning	Hand-written, per-date loop, in your own Python code	A real query engine (Spark, Athena, DuckDB, Trino) does this automatically, from something like WHERE date BETWEEN '...' AND '...', using its own internal catalog, no hand-written loop needed
Protection against a huge range	None — as shown above, it issues as many calls as the range implies	A real system would warn, reject, or automatically fall back to a full listing past some sensible size threshold
Pruning incremental tables	Not built, a named and deliberate gap	A real metastore (Hive Metastore, AWS Glue Catalog, Iceberg's metadata) tracks partition boundaries for every load type the same way, whether the partition key is a date or a watermark range
Knowing which partitions exist at all	The caller has to already know the date range they want	A real catalog tracks which partitions actually exist, so a reader doesn't have to already know the answer before asking
8. PRINCIPAL ENGINEER VIEW

If asked "your function issues one API call per date, isn't that worse than a single call, for a large range?", the strong answer is the one you just worked through by hand: yes, in call count, for a large-enough range, but the whole benefit was never about call count. It's about how much data gets transferred and held in memory per call. Being able to state precisely when the trade-off flips, "pruning wins when the requested range is much smaller than total history, and loses when it approaches the full history," is a materially stronger answer than either "pruning is always better" or getting talked out of the design by the call-count objection alone.

A second point worth having ready: recognizing when not to unify two similar-looking things. It would have looked more impressive to ship one "generic partition pruning" function handling both date ranges and watermark ranges. It also would have been dishonest, since a watermark range isn't enumerable the way a date range is. Explicitly scoping a function to exactly the case it can do correctly, and naming the boundary rather than hiding it, is a real engineering judgment call, and it's one interviewers specifically probe for when they ask "what would you not build, and why."

9. REMEMBER
Partitioning means organizing where data physically lives, by the value of a column people actually filter on, so a search can skip what it doesn't need.
Hive-style partitioning is just a key=value naming convention baked into the object's key string. It's not a real folder; it's a predictable pattern your code can reconstruct.
Pruning means constructing the narrow prefix yourself and asking only for that, instead of listing everything and filtering afterward in your own code.
Pruning trades more, smaller calls for less transferred data. That's a win when your range is small relative to total history, and a loss when your range basically is the total history.
### Principal Data Engineer Perspective

The judgment call worth defending here is scoping this section's new code
to exactly the case it can do honestly — date-range pruning for
full-load tables — rather than building a more impressive-sounding
"generic partition pruning" function that would have quietly done the
wrong thing for incremental tables' watermark-range partitions. Section
20.3's Design Decision is really about recognizing that two genuinely
different partitioning schemes exist in this codebase already, and that
papering over the difference with one interface would produce code that
*looks* more complete while actually being less trustworthy — a caller
passing a date range against an incremental table would get either wrong
results or a confusing failure, not a clean "this isn't supported for
that load type." Naming the scope boundary explicitly, and pointing to
the real (if less polished) alternative for the incremental case
(`list_successful_bronze_keys`, already available since Section 17), is
worth more than a unified-looking function that doesn't actually unify
anything correctly. The second thing worth flagging: 20.7's failure
scenario — pruning a range that turns out to be someone's whole
history — is the kind of "the optimization becomes the bug" case that's
easy to miss when a new mechanism is evaluated only in the scenario that
motivated building it (a small, known range) rather than stress-tested
against the scenario that breaks its assumption (a range nobody bounded).

### 20.9 Principal Engineer Interview Questions

**Q: "Your partition-pruned listing function issues one API call per
date in the requested range. For a 2-year date range, that's over 700
calls. Isn't that worse than one call, in practice?"**

*What's tested:* whether the candidate can reason about the actual
trade-off pruning makes (fewer objects touched, more calls issued) rather
than assuming "pruning" is an unconditional win.

*What a weak answer looks like:* "Pruning is always more efficient" —
contradicted by the candidate's own math (700+ calls vs. 1); a strong
answer has to grapple with the real cost pruning introduces.

*What a strong answer covers:* yes, in call *count* specifically, a
naive per-date loop over a 2-year range is worse than one
`list_bronze_keys` call — this is exactly Section 20.7's named failure
case. The real fix at that scale isn't "prune more" but "prune smarter":
batch contiguous date ranges into fewer, wider prefix queries where
possible, fall back to a full listing plus client-side filter past some
threshold, or — the production-grade answer — let a real catalog/metastore
(Glue, Hive Metastore, Iceberg) track partition existence so a query
planner can decide the right strategy per query, rather than a
hand-written client loop making a fixed, one-size-fits-all choice.

*Concepts:* pruning trades call count for data volume, not an
unconditional win; the point where that trade stops paying off; the role
a real catalog/metastore plays in making this decision dynamically
instead of statically.

*Expected follow-up:* "How would you decide, programmatically, whether
to prune or do a full listing for a given request?" — Compare the
requested range's implied call count against a full listing's single
call, and pick whichever is cheaper for that specific request — which
itself requires knowing (or estimating) total history size, information
this repo's current code doesn't track anywhere.

*Common mistake:* treating "partition pruning" as a keyword that's
always good to have, without being able to state the concrete condition
under which it stops being a win — the same failure mode as
over-indexing a database table without considering write cost.

**Q: "This repo has two different partitioning schemes — calendar dates
for full loads, watermark ranges for incremental loads. Why not just use
dates for both, for consistency?"**

*What's tested:* whether the candidate remembers *why* incremental
load's key scheme was chosen the way it was (Section 15) and can connect
that reasoning to this section's partitioning discussion, rather than
treating the two sections as unrelated.

*What a weak answer looks like:* "Consistency isn't that important" —
dismisses the question rather than engaging with the actual reason.

*What a strong answer covers:* a full load's key only ever needs to
answer "which calendar day is this snapshot from," which a date captures
perfectly. An incremental batch's key needs to answer a different
question: "which exact row range does this specific batch cover," which
is what makes retries safely overwrite-or-not-collide (Section 15.7).
Two incremental runs on the *same calendar day* cover *different* id
ranges and must NOT share a key, or a genuine idempotency bug results —
exactly the property `build_bronze_incremental_key`'s watermark-range
keys guarantee and a date-only key would silently break. The two
partitioning schemes differ because the two load types' actual
correctness requirements differ, not from an accidental lack of
consistency.

*Concepts:* partition key choice driven by correctness requirements
(Section 15's idempotency guarantee), not by surface-level naming
consistency across otherwise-similar-looking code paths.

*Expected follow-up:* "Could an incremental load ALSO be partitioned by
date, with watermark range as a second, secondary partition level inside
each date?" — Yes, and this is closer to how a mature lake table often
looks (multi-level Hive partitioning, e.g. `date=.../watermark_start=...`)
— explicitly out of scope for this repo's Phase 1 (named as "not true
multi-file partitioning" in Section 34's checklist), but a reasonable
Phase 2+ evolution once query patterns over incremental history become
concrete enough to justify the added key-structure complexity.

*Common mistake:* assuming every partitioning decision in a codebase
should look identical for aesthetic consistency, rather than recognizing
that different write patterns can have genuinely different correctness
requirements driving genuinely different key structures.

---

## 21. File Layout ✅✅

1. CONCEPT
First, how is this different from Partitioning, the thing we just covered?

This trips people up, so let's separate it cleanly before going further.

Partitioning answers: which column values split your data into separate objects? That's what put ingestion_date=2026-09-19 into your Bronze keys.

File layout answers a completely different question: once you know a piece of data belongs in one particular partition, how many separate files should hold it, and how big should each one be?

You could partition perfectly, and still get file layout wrong. Imagine a single day's worth of clicks data, correctly landing under ingestion_date=2026-09-19/. That's still just an address. Inside that address, you could put all of it in one file, or split it into a hundred tiny files, or one file so enormous nothing can read it efficiently. Partitioning decided the folder. File layout decides what's inside it.

Your project has actually had a real answer to this question since Section 14, without ever naming it: exactly one file per partition. One Parquet object per (table, date), for full loads. This section makes that choice explicit, and gives you a way to actually check whether it's still true.

Two failure modes, sitting on opposite sides of a healthy middle

Too many small files. You already measured, in the Parquet section, that every Parquet file carries a fixed cost: a footer has to be written and parsed, no matter how many rows are inside. If one partition's data got split across a thousand tiny files instead of one, a reader would pay that exact fixed cost a thousand separate times, for the exact same total amount of data a single file would have paid it for once. This is called the small-file problem, and it's one of the most common, genuinely-encountered failure modes in real data lakes.

Too few, overly large files. The opposite mistake. If one partition is a single, enormous file, two problems show up. First, many processing engines split their work by file, not by arbitrary byte ranges within a file, so one giant file can only ever be worked on by one worker at a time, wasting whatever parallel processing power is available. Second, a reader who only wants a small slice of that partition still has to open and scan the entire massive object to get it.

The generic analogy: packing boxes for a house move

Picture moving out of an apartment. You have one room's worth of belongings to pack, call it "the partition."

If you pack every single item into its own individually-labeled box, one box per fork, one box per book, you'll have hundreds of boxes. Loading the truck means physically lifting hundreds of separate boxes, each one adding its own handling time, even though the total amount of stuff hasn't changed.

If you pack everything from that entire room into one giant box, you'll need a forklift to move it, and if you need just one item out of it later, you have to unpack the entire box to find it.

The sensible middle ground: a reasonable number of medium-sized boxes, each holding a sensible amount, labeled clearly. Big enough that you're not managing hundreds of tiny units, small enough that any one box is still manageable to lift, open, and search. File layout is choosing where, on that same spectrum, your actual data lands.

2. URL SHORTENER EXAMPLE

Your real clicks table, at its current seeded size, has exactly one Bronze object: bronze/clicks/ingestion_date=2026-09-19/clicks.parquet, measured back in the Parquet section at 417,649 bytes. object_count = 1. That's the healthy, current state of your one-file-per-partition layout. Nothing to fix, nothing degrading, because with only one file, there's no problem, just an unremarkable fact.

But "healthy today" isn't something you should have to just assume forever. The real question is: how would you actually know, later, if that stopped being true? That's what this section's new function answers.

python
DEFAULT_SMALL_FILE_THRESHOLD_BYTES = 8 * 1024 * 1024  # 8 MiB

def get_file_layout_report(
    s3_client, bucket, table_name, *, small_file_threshold_bytes=DEFAULT_SMALL_FILE_THRESHOLD_BYTES
) -> dict:
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=f"bronze/{table_name}/")
    sizes = [obj["Size"] for obj in response.get("Contents", [])]
    if not sizes:
        return {"object_count": 0, "total_bytes": 0, "avg_bytes": 0, "min_bytes": 0, "max_bytes": 0, "small_file_count": 0}
    return {
        "object_count": len(sizes), "total_bytes": sum(sizes), "avg_bytes": sum(sizes) // len(sizes),
        "min_bytes": min(sizes), "max_bytes": max(sizes),
        "small_file_count": sum(1 for s in sizes if s < small_file_threshold_bytes),
    }
3. CODE WALKTHROUGH

DEFAULT_SMALL_FILE_THRESHOLD_BYTES = 8 * 1024 * 1024. This defines "small" as under 8 MiB. MiB, defined: a mebibyte, exactly 1,048,576 bytes, the binary-computing equivalent of "megabyte." 8 MiB is not derived from any deep principle; it's a commonly-cited rule-of-thumb cutoff in the industry, well below the multi-hundred-megabyte object sizes a real, mature data lake typically aims for.

s3_client.list_objects_v2(Bucket=bucket, Prefix=f"bronze/{table_name}/"). Notice this is scoped to one table's own prefix, not the whole bronze/ prefix the way get_bucket_stats (Object Storage section) was. That's deliberate: file layout health is naturally a per-table question, since different tables land at very different sizes and object counts, and averaging them together would hide exactly the thing you're trying to measure.

sizes = [obj["Size"] for obj in response.get("Contents", [])]. Same trick you already learned in get_bucket_stats: reuse the Size field the LIST call already returned, instead of making a separate call per object to ask for its size.

if not sizes: return {...all zeros...}. This handles the case where the table has no Bronze objects at all yet. Without this check, sum(sizes) // len(sizes) would try to divide by zero and crash. Handling this explicitly, with a clean all-zero result, is a small but real defensive habit: a brand-new table with nothing written yet is a completely normal state, not an error.

"avg_bytes": sum(sizes) // len(sizes). Notice the double-slash, //, not a single /. This is integer division: dividing and throwing away any decimal remainder, so the result is always a whole number. This is fine here specifically because avg_bytes is a rough reporting number for a human to glance at, not a value anything downstream does precise math with.

"small_file_count": sum(1 for s in sizes if s < small_file_threshold_bytes). For every object, this adds 1 if its size is under the threshold, and adds nothing otherwise, then totals it up. This is the field the whole function exists to produce.

4. DESIGN
Why report small_file_count, min_bytes, and max_bytes, instead of just avg_bytes?

Here's a real, checkable failure worth walking through carefully, because it's a general statistics lesson, not just a detail of this one function.

Imagine a partition with exactly three objects: 1 MB, 50 MB, and 0.5 MB. Add those up and divide by three, and you get an average of roughly 17 MB, comfortably above the 8 MB threshold. If avg_bytes were the only number you reported, this would read as "totally healthy, nothing to see here."

But look again at the actual three files. Two of them, the 1 MB and the 0.5 MB, genuinely are small files by the threshold. That's two out of three, the majority of the objects. The average was almost entirely dragged upward by one single large outlier, the 50 MB file. An average is the wrong single-number summary whenever a distribution is skewed like this, most objects small, a few large ones pulling the mean up, and a real small-file problem very often looks exactly this way in practice: one process writing correctly-sized files, alongside a separate, buggy process quietly dropping in a pile of tiny ones.

This is exactly why get_file_layout_report returns small_file_count as its own explicit field, rather than making a caller try to infer file health from avg_bytes alone. And it's worth noting min_bytes/max_bytes alone don't fully fix this either. They tell you the range exists, from 0.5 MB up to 50 MB, but not how many objects actually sit at the unhealthy end of that range. small_file_count is the one field that directly answers the operationally useful question: "how many files here are actually too small," rather than something a human has to derive by squinting at a range.

Why measure only, and never automatically fix a bad layout?

This is a real, deliberate design decision, and it's the second time your project has made essentially this same call. Back in the Idempotency section, reconcile_bronze detected drift between the metadata table and real storage, but never automatically fixed anything; a human had to act on what it found. This section makes the identical choice for a different problem.

The alternative considered was building an automatic compaction job: a process that detects too many small files in a partition and rewrites them into fewer, larger ones on its own. That would actually fix a degrading layout, not just report it. So why not build it?

Because compaction is a genuinely more dangerous operation than detection. Fixing a small-file problem means reading the small files' contents, writing new, larger combined files, and then deleting the original small files. Any bug in that rewrite logic risks real, permanent data loss, not just a stale report someone can rerun. Detection-only costs you a human having to notice the report and act on it manually, in exchange for a categorically safer default: nothing this function does can ever delete or corrupt a real object. This same posture, detect and report, never silently auto-remediate, is recorded in your project as its own standing design principle (ADR-013), specifically because it's now shown up twice independently, not as a one-off choice made in isolation.

	Detection only (chosen)	Automatic compaction
Risk of real data loss from a bug	None — this is read-only reporting	Real — compaction means deleting originals after rewriting them
Actually fixes a degrading layout	No, a human has to act on the report	Yes, automatically
Cost to build	One function, one CLI command	A full rewrite pipeline, plus a safe-deletion strategy
Matches how this project already handles similar problems	Yes, same posture as reconciliation	Would be the first self-modifying operation in the whole codebase
5. RUN
bash
make layout-report TABLE=clicks

wraps python -m url_shortener_analytics.cli layout-report --table clicks.

A real, already-verified consistency check worth trying yourself: sum layout-report's total_bytes across every one of your tables (urls, users, clicks), one command per table, and compare that sum against make storage-stats's single, whole-Bronze total_bytes. They should match exactly, since both are ultimately counting the same real bytes sitting in the same bucket, just grouped differently.

6. EXPERIMENT

A real arithmetic exercise, connecting back to the Parquet section's actual measured numbers. You already know, from a genuine measurement, that 5,000 real clicks rows produced a 417,649-byte Parquet file. That works out to roughly 83.5 bytes per row. Before reading further, calculate by hand: roughly how many rows would a single daily clicks file need before it crossed the 8 MiB small-file threshold?

8 MiB is 8,388,608 bytes. Divide that by 83.5 bytes per row, and you get roughly 100,000 rows needed to cross the threshold, about 20 times your project's current seeded volume. This tells you something concrete and reassuring: at your project's actual current data volume, there is no small-file risk to worry about yet, not because you're assuming it's fine, but because you've derived it from a real measurement. That's a materially stronger position than "it's probably fine."

A hands-on test run, fully runnable right now, no MinIO needed:

bash
PYTHONPATH=ingestion/src python3 -m pytest ingestion/tests/unit/test_object_store.py -k file_layout -v

Expected: 3 tests pass. The first one is the interesting one, and it's the actual, real proof of the average-hides-the-problem scenario from the Design section above: it constructs three mocked objects at 1 MB, 50 MB, and 0.5 MB, and asserts small_file_count == 2, while avg_bytes comes out around 17 MB. Read that test's source yourself, in test_object_store.py, and confirm the exact numbers match what was walked through above.

7. PRODUCTION VIEW
Aspect	This project right now	Real production
Checking layout health	Manual, one table at a time (make layout-report TABLE=...)	Scraped on a schedule, across every table, graphed over time, with alerting on a rising trend in small_file_count, not just its current value
Fixing a bad layout	Nothing — detection only, by deliberate design	A scheduled compaction job, often a Spark job, rewriting a partition's many small files into fewer, size-targeted ones, with careful atomic delete-after-verify handling so a crash mid-compaction can't lose data
Small-file threshold	One fixed default, 8 MiB, for every table alike	Often tuned per table, since different tables have genuinely different healthy target sizes depending on their query patterns
8. PRINCIPAL ENGINEER VIEW

If an interviewer asks "how would you detect a small-file problem," the weak answer is "check the average file size." The strong answer is the one your project's own test proves directly: an average can be pulled entirely upward by one large outlier while the majority of files are genuinely unhealthy, and a real-world small-file problem is very often shaped exactly this way, one healthy writer alongside one buggy one. Being able to give the specific 1 MB / 50 MB / 0.5 MB example, and explain exactly why the mean lies in that case, is a much stronger answer than a generic "yeah, averages can be misleading."

A second strong point: recognizing "detect, don't auto-remediate" as a repeating, deliberate pattern in this codebase, not a coincidence. It showed up first for Bronze reconciliation, and again here for file layout, for the same underlying reason both times: the cost of a false negative (a problem sitting unnoticed a bit longer) is much smaller than the cost of a false positive triggering an automatic, irreversible action, like deleting real data based on a buggy rewrite. Naming that as a conscious, repeated engineering posture, rather than two unrelated decisions that happened to land the same way, is exactly the kind of pattern-recognition a senior engineer is expected to demonstrate.

9. REMEMBER
Partitioning decides which folder your data lands in. File layout decides how many files, and how big, sit inside that folder. Different questions.
Too many small files means paying Parquet's fixed per-file overhead over and over, for the same total data. Too few, oversized files limits parallelism and forces full-object scans for small requests.
An average alone can hide a real small-file problem, when the distribution is skewed by one or two large outliers. Report the actual count of unhealthy files directly.
This project always detects and reports, never automatically deletes or rewrites data on its own. The same safety posture that governed reconciliation governs file-layout checking too, and it's a deliberate, repeated choice, not a limitation.

### Principal Data Engineer Perspective

The judgment call worth defending here is recognizing that this repo's
current file layout isn't "solved" simply because it happens to be
healthy today — it's healthy because of a specific, current condition
(low data volume) that Section 21's Hands-on Challenge quantified rather
than assumed: roughly 100,000+ rows before a single daily `clicks`
object would even cross the small-file threshold from the *other*
direction (growing too large for a single file to make sense, at which
point the opposite failure mode — Section 21.1's "too few, overly large
files" — starts to become the relevant risk instead). A principal
engineer doesn't just note "this works at current scale" — they name the
specific number at which it would stop working, the same discipline
Section 15's watermark reasoning and Section 20's partition-pruning
analysis already established elsewhere in this guide. The second thing
worth flagging: choosing `small_file_count` as an explicit field, rather
than trusting `avg_bytes` to communicate the same information, is a small
decision that only matters because of a genuine, if easy-to-miss,
statistical property (a skewed distribution's mean can look healthy while
its mode doesn't) — the kind of detail that separates a report someone
can actually operate from one that quietly gives false reassurance.

### 21.9 Principal Engineer Interview Questions

**Q: "Why does the small-file problem exist at all — what's the actual
mechanical cost of having many small files instead of fewer large ones,
covering the same total data?"**

*What's tested:* whether the candidate can name the concrete mechanism,
not just recite "small files are bad" as received wisdom.

*What a weak answer looks like:* "Lots of small files are inefficient" —
true but circular, doesn't explain the actual cost.

*What a strong answer covers:* two independent costs, both real. First,
per-file *format* overhead — this repo's own Section 19.6 measurement
showed Parquet pays a fixed cost (footer parse, embedded schema decode)
per file, paid again in full for every additional file covering the same
total rows. Second, per-object *listing/metadata* overhead at the object-
store and query-engine level — more objects means more entries a
`list_objects_v2` call has to return and a query planner has to reason
about, independent of the data format used at all. A candidate who names
both, and can point to a real number backing the first one (Section
19.6's actual measured overhead), is demonstrating they understand this
as a measured phenomenon, not a rule of thumb.

*Concepts:* fixed per-file overhead, distinguished from per-object
listing/metadata overhead; connecting a "operational tuning" question
back to a concrete, previously-measured number rather than treating it
as a separate, unconnected fact.

*Expected follow-up:* "At what point does the opposite problem — files
too large — start to matter instead?" — When a single file becomes large
enough to limit read parallelism (many engines parallelize by file) or
forces a reader wanting a small slice of a partition to scan far more
data than it needs; this repo hasn't reached that point either, at its
current volume.

*Common mistake:* describing the small-file problem only in terms of
"more files means more work," without being able to name the *specific*
work (footer parsing, listing overhead) that scales with file count.

**Q: "Your file-layout report includes `small_file_count` as its own
field, even though `avg_bytes` is also reported. Isn't that redundant?"**

*What's tested:* whether the candidate understands when a summary
statistic (a mean) fails to represent the underlying distribution, and
can connect that to a concrete operational consequence.

*What a weak answer looks like:* "More data is always better" — not
wrong, but doesn't explain *why* this specific redundancy is load-bearing
rather than decorative.

*What a strong answer covers:* no, not redundant — `avg_bytes` alone can
look healthy while the actual object population is mostly small files
dragged upward by a few large outliers (a concrete, provable case:
Section 21.7's exact three-object example, where the average sits well
above the threshold while two-thirds of the objects sit below it).
`small_file_count` answers the operationally relevant question — "how
many objects will pay the fixed per-file overhead penalty" — directly,
rather than requiring an operator to infer it from a statistic that can
mask exactly this shape of problem.

*Concepts:* mean vs. distribution shape; choosing a reported metric based
on what question it actually answers, not just what's cheap to compute.

*Expected follow-up:* "What would a genuinely complete size-distribution
report look like, beyond avg/min/max/small-file-count?" — A real
percentile histogram (p50/p90/p99), which this repo's simple four-number
summary deliberately doesn't attempt, named honestly as a production gap
in Section 21.8 rather than presented as already solved.

*Common mistake:* treating "we report more numbers" as automatically
better without being able to articulate the *specific* failure case each
additional number is there to catch.

---

## 22. Ingestion Metadata (deep-dive) ✅✅

### 22.1 Concept

`ingestion_metadata` has been this pipeline's control plane since Section
13 — watermark source (Section 15), checkpoint state machine (Section
16), Bronze reconciliation's source of truth (Section 17) — always in
service of some other mechanism, never taught as a subject of its own.
This section is that dedicated treatment: the table's full schema,
walked through column by column; a genuine, previously-unnoticed gap
found by re-reading that schema against the code that's supposed to
populate it (`watermark_start` — in the schema since Section 13, never
actually written, until this section fixes it); and two new
general-purpose query functions, `get_run_history` and
`get_ingestion_summary`, giving this table's contents to a human for the
first time without requiring hand-written SQL.

### Why does this exist?

Every prior reader of this table has been narrowly scoped to one
question and one status: `get_last_watermark` reads only `status =
'success'` rows, for exactly one table; `find_stale_running_runs` reads
only `status = 'running'` rows; `list_successful_bronze_keys` reads only
`status = 'success'` rows again, for a different purpose. None of them
answer the question a human debugging "why does this table's Bronze data
look wrong" actually asks first: **what happened, across every status,
in what order?** That's a genuinely different access pattern from
anything this table has needed to serve before — and until this section,
answering it meant writing ad-hoc SQL by hand every time, which is
exactly the kind of friction that makes an operator stop checking
something they should be checking regularly.

### Simple Example (generic, pre-URL-Shortener)

A build system's CI history: individual tools already answer narrow
questions ("is the current build green," "what's queued to run next")
the same way this table's existing readers do. But "show me the last 20
builds, whatever their outcome, so I can see the pattern" is a
*different*, general-purpose query that a CI system provides as a first-
class page precisely because it's the one a human reaches for first when
something looks wrong — not "is build #4521 green" in isolation, but "what's
the recent shape of things." `get_run_history` is this table's version of
that page.

### URL Shortener Example

Every column in `ingestion_metadata`, what it's for, and — as of this
section — whether it's actually populated:

| Column | Purpose | Populated? |
|---|---|---|
| `run_id` | Primary key, a UUID | Always |
| `pipeline_name`, `source_table`, `load_type` | Identify what this run was | Always |
| `status` | The checkpoint state machine (Section 16) | Always |
| `watermark_start` | This run's starting watermark | **Fixed this section** -- see below |
| `watermark_end` | This run's ending watermark (Section 15) | On success |
| `rows_read`, `rows_written` | Row counts | On success |
| `bronze_key` | The exact object written (Section 17) | On success, non-`NULL` write |
| `started_at`, `completed_at` | Checkpoint timing (Section 16) | Always / on completion |
| `error_message` | Why a run failed | On failure |

`watermark_start` is the interesting row: the column has existed in
`sql/source/003_ingestion_metadata.sql` since Section 13, and
`extract_incremental.py` has always *known* the value (it's a local
variable in `run_incremental_load`, and it's literally embedded in every
incremental run's own `bronze_key` string via
`build_bronze_incremental_key`) — but `metadata.finish_run_success`
never accepted a `watermark_start` parameter to actually write it,
before this section. This was genuinely confirmed, not assumed: querying
a real incremental run's row against this sandbox's real Postgres 16,
before this section's fix, returned `watermark_start = NULL` even though
`watermark_end` was correctly populated on the exact same row.

### 22.2 Architecture

```
 Before this section:

   run_incremental_load()
        │
        ├─ watermark = get_last_watermark(...)         # KNOWN locally
        ├─ new_watermark = df["id"].max()
        ├─ key = build_bronze_incremental_key(
        │      table, watermark, new_watermark)         # watermark used HERE
        └─ finish_run_success(..., watermark_end=new_watermark)
                                     ▲
                                     └── watermark (the START value) never passed through


 After this section:

   run_incremental_load()
        │
        ├─ watermark = get_last_watermark(...)
        ├─ new_watermark = df["id"].max()
        ├─ key = build_bronze_incremental_key(table, watermark, new_watermark)
        └─ finish_run_success(..., watermark_start=watermark, watermark_end=new_watermark)
                                     ▲
                                     └── now genuinely persisted


 get_run_history / get_ingestion_summary (NEW, read-only, general-purpose):

   ingestion_metadata (every status, every column)
        │
        ├─▶ get_run_history: ORDER BY started_at DESC LIMIT N -- "what happened, in order"
        │
        └─▶ get_ingestion_summary: GROUP BY source_table,
               COUNT(*) FILTER (WHERE status = 'success'), ...  -- "health, per table, at a glance"
```

### 22.3 Design Decision: aggregate in SQL, not in Python

**Context:** `get_ingestion_summary` needs per-table counts of successful
and failed runs, total rows written, and the most recent success
timestamp. **Decision:** one SQL query using Postgres/SQLite's `FILTER
(WHERE ...)` clause on aggregate functions (`COUNT(*) FILTER (WHERE
status = 'success')`), computed server-side, rather than fetching every
`ingestion_metadata` row into Python and reducing it there.
**Alternatives considered:** fetch all rows via something like
`get_run_history` with no limit, then aggregate in a Python loop.
**Trade-offs:** this is the *opposite* choice from Section 16's own
`find_stale_running_runs`, which deliberately computes its cutoff in
Python rather than in SQL, for cross-dialect portability — worth being
explicit about why the two sections land differently. Section 16's
concern was a SQL feature (`now() - interval`) that literally doesn't
exist in one of the two target dialects; `FILTER (WHERE ...)` is
supported by *both* SQLite (since version 3.30, 2019) and Postgres, so
the portability concern that drove Section 16's Python-side choice simply
doesn't apply here — and aggregation is precisely the kind of work a
database is built to do efficiently, at any real data volume, in a way a
Python loop over every fetched row is not. **Consequences:** this
function's correctness now depends on the `sqlite_engine` test fixture's
SQLite version actually supporting `FILTER` — confirmed directly in this
sandbox (SQLite 3.45.1) before relying on it, not assumed from a version
number alone; a project targeting an older embedded SQLite (pre-3.30)
would need the Python-reduction alternative instead.

### Alternatives

Covered above. A further alternative considered for `get_run_history`
specifically: returning a paginated result (cursor-based, for a table
that could eventually hold many thousands of rows) rather than a flat
`LIMIT N`. Rejected for this increment as premature — this repo's actual
`ingestion_metadata` row count (a few runs per table per day) is nowhere
near where pagination would matter, and a flat limit is simpler to reason
about and to test; see 22.8's Production Considerations for when this
trade-off would need revisiting.

### Trade-offs

| | Aggregate in SQL (chosen, `get_ingestion_summary`) | Aggregate in Python (chosen, `find_stale_running_runs`'s *cutoff computation*, Section 16) |
|---|---|---|
| Reason for the choice | `FILTER (WHERE ...)` is supported by both target dialects; aggregation is what a database does efficiently | `now() - interval` syntax genuinely doesn't exist in SQLite at all -- no portable SQL-side equivalent existed |
| Cost as row count grows | Scales with the database engine's own aggregate performance | Would require fetching every row into the Python process first -- doesn't scale the same way |
| Portability risk | Real, but resolved -- confirmed both dialects support the specific syntax used | N/A -- this is why the Python-side approach was chosen in the first place |

### 22.4 Implementation

---

**CREATE:** (edit) `ingestion/src/url_shortener_analytics/metadata.py` —
`finish_run_success` gains `watermark_start`; `extract_incremental.py`'s
two call sites pass it

**PURPOSE:** Close a real, previously-unnoticed gap: persist the
`watermark_start` column that has existed in the schema since Section 13
but was never actually written.

**IMPLEMENTATION GUIDE (write it yourself):** add `watermark_start: int |
None = None` to `finish_run_success`'s keyword arguments, add it to the
`UPDATE ... SET` statement and its bound parameters. Then, in
`extract_incremental.run_incremental_load`, pass `watermark_start=watermark`
(the value already computed locally, before this fix, purely for
`build_bronze_incremental_key` and the function's own return dict) on
**both** call sites — the normal success path *and* the no-op/empty-batch
path (a no-op run's `watermark_start` still equals its `watermark_end`;
both are genuinely known even when nothing was written).

**REFERENCE IMPLEMENTATION:**

```python
# ingestion/src/url_shortener_analytics/metadata.py (excerpt)

def finish_run_success(
    engine, run_id, *, rows_read, rows_written,
    watermark_start=None, watermark_end=None, bronze_key=None,
) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE ingestion_metadata
            SET status = 'success', rows_read = :rows_read, rows_written = :rows_written,
                watermark_start = :watermark_start, watermark_end = :watermark_end,
                bronze_key = :bronze_key, completed_at = :completed_at
            WHERE run_id = :run_id
        """), {..., "watermark_start": watermark_start, ...})
```

```python
# ingestion/src/url_shortener_analytics/extract_incremental.py (excerpt)

# no-op path:
metadata.finish_run_success(engine, run_id, rows_read=0, rows_written=0,
                             watermark_start=watermark, watermark_end=watermark)
# normal path:
metadata.finish_run_success(engine, run_id, rows_read=len(df), rows_written=len(df),
                             watermark_start=watermark, watermark_end=new_watermark, bronze_key=key)
```

Full files: [`metadata.py`](../ingestion/src/url_shortener_analytics/metadata.py),
[`extract_incremental.py`](../ingestion/src/url_shortener_analytics/extract_incremental.py).

**VERIFY:** this sandbox's real, local Postgres 16 was used to *prove*
the gap first (querying a genuine incremental run's row and observing
`watermark_start = NULL` while `watermark_end` was correctly set), then
to prove the fix (same query, same kind of run, `watermark_start` now
correctly populated) — both are ACTUAL OBSERVED results from this
sandbox, not assumed from reading the code alone. See 22.6.

**TEST:** `test_metadata.py`'s new `test_finish_run_success_persists_watermark_start`;
`test_extract_incremental.py`'s new
`test_run_incremental_load_persists_watermark_start_on_the_checkpoint_row`
and an added assertion in the existing no-op test.

---

**CREATE:** (edit) `ingestion/src/url_shortener_analytics/metadata.py` —
`get_run_history`, `get_ingestion_summary`

**PURPOSE:** General-purpose observability this table has never had:
"what happened recently, across every status" and "what's each table's
health, at a glance."

**IMPLEMENTATION GUIDE (write it yourself):** `get_run_history`: a plain
`SELECT ... ORDER BY started_at DESC LIMIT :limit`, with optional
`pipeline_name`/`source_table` filters appended conditionally (the same
pattern `find_stale_running_runs`, Section 16, already established for
optional filters). Deliberately select every status, not just one —
that's the entire point, distinguishing this from every other reader in
the module. `get_ingestion_summary`: one query, `GROUP BY source_table`,
using `COUNT(*) FILTER (WHERE status = 'success')` /
`COUNT(*) FILTER (WHERE status = 'failed')` /
`SUM(rows_written) FILTER (WHERE status = 'success')` /
`MAX(started_at) FILTER (WHERE status = 'success')` — see 22.3's Design
Decision for why this aggregates in SQL rather than Python.

**REFERENCE IMPLEMENTATION:**

```python
# ingestion/src/url_shortener_analytics/metadata.py (excerpt)

def get_run_history(engine, *, pipeline_name=None, source_table=None, limit=20) -> list[dict]:
    query = "SELECT run_id, pipeline_name, source_table, load_type, status, rows_read, " \
            "rows_written, bronze_key, started_at, completed_at, error_message " \
            "FROM ingestion_metadata WHERE 1=1"
    params = {"limit": limit}
    if pipeline_name is not None:
        query += " AND pipeline_name = :pipeline_name"; params["pipeline_name"] = pipeline_name
    if source_table is not None:
        query += " AND source_table = :source_table"; params["source_table"] = source_table
    query += " ORDER BY started_at DESC LIMIT :limit"
    with engine.begin() as conn:
        rows = conn.execute(text(query), params).fetchall()
    return [dict(row._mapping) for row in rows]  # simplified -- see committed file for the explicit-dict version


def get_ingestion_summary(engine, pipeline_name=None) -> list[dict]:
    query = """
        SELECT source_table,
               COUNT(*) FILTER (WHERE status = 'success') AS successful_runs,
               COUNT(*) FILTER (WHERE status = 'failed') AS failed_runs,
               COALESCE(SUM(rows_written) FILTER (WHERE status = 'success'), 0) AS total_rows_written,
               MAX(started_at) FILTER (WHERE status = 'success') AS last_success_at
        FROM ingestion_metadata WHERE 1=1
    """
    ...  # optional pipeline_name filter, GROUP BY source_table ORDER BY source_table
```

Full file: [`metadata.py`](../ingestion/src/url_shortener_analytics/metadata.py).

**RUN:**
```bash
make ingestion-history                       # last 20 runs, any status
python -m url_shortener_analytics.cli ingestion-history --table clicks --limit 5
make ingestion-summary                       # per-table health
```

**VERIFY / EXPECTED:** see 22.6 below — genuinely run against real
Postgres in this sandbox, not just asserted.

**TEST:** `test_metadata.py` — five new tests: most-recent-first ordering
across mixed statuses, filtering by `source_table`, `limit` respected,
per-table aggregation correctness (including a table with failures but
zero successes — `total_rows_written` must be `0`, not `NULL`, and
`last_success_at` must be `None`, not an error), and `pipeline_name`
filtering.

**PRODUCTION CONSIDERATIONS / INTERVIEW QUESTIONS:** see Sections
22.8/22.9.

---

### Hands-on Challenge (implement-yourself)

Before LAB 18 below, try this without looking at `metadata.py`: write
the `get_ingestion_summary` query yourself, but using `Python`-side
aggregation instead of SQL's `FILTER (WHERE ...)` — fetch every row via
something like `get_run_history` with no limit, then reduce it into the
same per-table summary shape by hand. Time both approaches conceptually
(you don't need real scale to reason about it): at what row count would
the Python version's "fetch everything, then reduce" cost start to matter
compared to the SQL version's server-side aggregation? (There's no single
right number — the exercise is in recognizing that the SQL version's cost
scales with the *database engine's* aggregate performance, while the
Python version's cost scales with *network transfer plus Python-loop*
overhead for every row fetched, which loses badly, the more the table
grows, regardless of what indexes exist.)

### 22.5 Hands-on Exercise

**LAB 18 — Prove the `watermark_start` fix and both new query functions
against this sandbox's real Postgres 16.**

This LAB was run for real, directly against real (non-Docker) Postgres in
this sandbox, seeding realistic run history via `metadata.py` itself (no
MinIO needed — this exercises the metadata layer only, the same
established pattern as LAB 12, Section 16.5). **ACTUAL OBSERVED, not
DESIGN EXPECTATION:**

```
--- get_run_history(limit=10) ---
  2026-09-19 18:28:18.965886+00:00  urls     success  rows_written=500
  2026-09-19 18:28:18.963676+00:00  clicks   failed   rows_written=None
  2026-09-19 18:28:18.961785+00:00  clicks   success  rows_written=5000
  2026-09-19 18:28:18.959580+00:00  users    success  rows_written=200
  2026-09-19 18:28:18.955460+00:00  urls     success  rows_written=500

--- get_run_history(source_table='clicks') ---
  2 rows returned, statuses: ['failed', 'success']

--- get_ingestion_summary (uses Postgres FILTER (WHERE ...) aggregate syntax) ---
  clicks   success=1 failed=1 total_rows=5000 last_success=2026-09-19 18:28:18.961785+00:00
  urls     success=2 failed=0 total_rows=1000 last_success=2026-09-19 18:28:18.965886+00:00
  users    success=1 failed=0 total_rows=200 last_success=2026-09-19 18:28:18.959580+00:00

ALL CHECKS PASSED against real Postgres 16 (including FILTER (WHERE ...) aggregate syntax).
```

What to observe: `get_run_history` returns rows most-recent-first
regardless of status — the failed `clicks` run appears second, sandwiched
between two successes, exactly reflecting the order things actually
happened rather than being grouped or filtered by outcome.
`get_ingestion_summary`'s `urls` row correctly shows `successful_runs=2`
(two separate full loads, different days) with `total_rows_written=1000`
— the sum across both, not just the most recent.

**The `watermark_start` fix itself, also genuinely proven** (separately,
against the same real Postgres instance): a run recorded with
`finish_run_success(..., watermark_start=5000, watermark_end=5100, ...)`
was queried back and its `watermark_start` column read exactly `5000` —
where, before this section's fix, the identical call (without the new
parameter existing at all) left that column `NULL`.

### 22.6 How to test

```bash
make test
```

ACTUAL OBSERVED, genuinely run in this environment:

```
73 passed in 7.04s
```

`ruff check ingestion/ benchmarks/` also passed cleanly. Combined with
22.5's direct verification against real Postgres 16 (both the
`watermark_start` fix and both new query functions, including the
Postgres-specific `FILTER (WHERE ...)` aggregate syntax), this section's
entire database-facing surface has now been genuinely exercised against
a real, non-SQLite dialect — not just unit-tested in isolation.

### 22.7 Failure Scenario

**What would have happened, concretely, if `watermark_start` had stayed
unpopulated indefinitely — is this actually a "real" bug, or just an
unused column?**

Worth answering honestly rather than assuming the answer is obviously
"real bug": today, *nothing* in this codebase currently reads
`ingestion_metadata.watermark_start` back — `build_bronze_incremental_key`
receives `watermark_start` as a function argument computed fresh each run
(from `get_last_watermark`), never read from this column. So the gap,
before this section's fix, caused **no incorrect behavior in anything
this repo currently does** — every existing feature (watermarking,
checkpointing, reconciliation) was, and remains, completely unaffected.
What it *did* cost: any future tool built directly against
`ingestion_metadata` (a dashboard querying "show me every batch's exact
row range," an audit report, a Phase 2 job wanting to reconstruct
incremental history without recomputing it) would have silently gotten
`NULL` for a value that was genuinely known and recorded nowhere durable
except inside a `bronze_key` string that would need parsing to recover
it. This is the honest shape of a real but *latent* bug — one with no
current observable symptom, found by reading the schema against the code
rather than by a failure report, and worth fixing specifically because
schema columns that exist but are silently never populated are exactly
the kind of thing that misleads a future reader (human or automated)
who reasonably assumes a non-`NULL`-looking column is actually populated.

### 22.8 Production Considerations

| Aspect | This repo (POC) | Production |
|---|---|---|
| Query surface | Two new functions, `get_run_history`/`get_ingestion_summary`, CLI-exposed | A real observability platform (Grafana over a Postgres datasource, or a dedicated internal tool) rather than a CLI a human runs on demand |
| `get_run_history` scale | Flat `LIMIT N`, no pagination (22.3's named, deliberate scope boundary) | Cursor-based pagination once row count genuinely grows past where a flat limit is sufficient |
| Schema drift detection | None -- this section's `watermark_start` gap was found by manual code review, not automated tooling | A schema-vs-code-usage linter (or simply a contract-style check, extending Section 12's pattern to this table itself) catching an unpopulated column automatically |
| Retention | Unbounded -- `ingestion_metadata` grows forever, no archival | A retention/archival policy once row count becomes operationally significant, consistent with ADR-007's stated gap for Bronze data itself |
| Aggregate query portability | `FILTER (WHERE ...)`, confirmed supported by both SQLite 3.45.1 (this sandbox) and Postgres | Re-verify against whatever SQLite version a CI pipeline's test runner actually ships, rather than assuming a recent-enough version everywhere |

### Principal Data Engineer Perspective

The judgment call worth defending here is treating a schema column that
exists but is never populated as worth actively finding and fixing,
rather than as harmless dead weight not worth the effort. Section 22.7's
honest framing matters: this genuinely was a latent, currently-harmless
gap, not an active bug — and a weaker engineering habit would reasonably
deprioritize fixing something with zero current observable impact. What
makes it worth fixing anyway is *why* the column exists in the first
place: `watermark_start` was deliberately added to the schema back in
Section 13, as part of this project's own stated principle ("build the
metadata layer once, correctly, rather than retrofitting it") — leaving
it silently unpopulated would have meant that stated principle quietly
failed to hold for one specific column, discoverable only by a future
reader who happened to check. Fixing it now, and documenting exactly how
and why it was found, is worth more to a portfolio reviewer than either
silently patching it with no explanation or leaving it as an unremarked
gap. The second thing worth flagging: Section 22.3's Design Decision
(aggregate in SQL here, in direct contrast to Section 16's aggregate-in-
Python choice) is a good example of the same underlying skill —
cross-dialect portability — leading to *opposite* concrete choices
depending on what's actually true about the two target dialects in each
specific case, rather than applying a single rule ("always compute in
Python for portability") mechanically everywhere.

### 22.9 Principal Engineer Interview Questions

**Q: "You found a database column that's existed in the schema for two
increments' worth of work but was never actually written by any code
path, with zero current impact on anything the system does. How do you
decide whether that's worth fixing right now?"**

*What's tested:* judgment about prioritizing a latent, currently-harmless
gap — a genuinely debatable call, not one with an obviously correct
answer, which is exactly what makes it a good interview question.

*What a weak answer looks like:* "Always fix bugs immediately" — too
absolute; doesn't engage with the real trade-off (opportunity cost,
whether it's actually causing harm right now).

*What a strong answer covers:* the decision should weigh what the gap
actually costs today (here: nothing observable) against what it would
cost to discover and fix *later*, once something depends on the column
being correct (here: a future consumer silently getting wrong data with
no error, the worst kind of bug to debug, because nothing fails loudly).
A column that exists specifically because an earlier design decision
said "build this once, correctly" (Section 13.3's own stated principle)
failing to actually hold is a signal the gap is worth closing
proactively, rather than waiting for a future consumer to discover it the
hard way.

*Concepts:* latent vs. active bugs; the asymmetric cost of a silent
wrong-data bug versus a loud failure; using a component's own stated
design intent as a signal for whether an inconsistency with that intent
is worth fixing now.

*Expected follow-up:* "How would you prevent this specific class of gap
— a schema column nobody writes to — from recurring?" — Some kind of
automated check that every declared column has at least one write path
exercised by tests (closer to a contract, extending Section 12's pattern
inward to this table's own schema, rather than only to source tables).

*Common mistake:* treating "it has zero current impact" as proof it
doesn't need fixing, without separately reasoning about what it would
cost to discover and fix later, once something actually depends on it.

**Q: "Section 16 computes its time cutoff in Python for cross-dialect
portability. Section 22's ingestion summary aggregates in SQL instead,
for the same underlying goal of dialect portability. Aren't these
contradictory?"**

*What's tested:* whether the candidate understands that "portability" is
a property that has to be checked per-feature, not a single rule applied
uniformly — and can articulate why these two sections land on opposite
concrete choices for a coherent reason.

*What a weak answer looks like:* "One of them must be wrong" — assumes
consistency-for-its-own-sake is the goal, missing the actual reasoning.

*What a strong answer covers:* not contradictory, because the two
sections are avoiding two *different* portability failures. Section 16's
concern is a SQL feature (`now() - interval`) with no equivalent syntax
at all in one target dialect (SQLite) — there, only Python-side
computation is genuinely portable. Section 22's concern is a *different*
SQL feature (`FILTER (WHERE ...)`) that both target dialects *do*
support (confirmed directly, not assumed) — there, doing the aggregation
in SQL is both portable and meaningfully more efficient than pulling
every row into Python first. The right general principle is "check what
each specific dialect actually supports for the specific feature in
question," not "always prefer Python" or "always prefer SQL" as a
blanket rule.

*Concepts:* portability as a per-feature property, not a global rule;
verifying an assumption about dialect support directly rather than
guessing from a version number or a vague sense of "SQL is more
portable" / "Python is more portable."

*Expected follow-up:* "How did you actually confirm SQLite supports
`FILTER (WHERE ...)` before relying on it, rather than just assuming a
'modern enough' SQLite would?" — Ran a direct, minimal repro against this
sandbox's actual bundled SQLite version (3.45.1) before writing any
production code against the assumption — the same "verify before
trusting" discipline this guide has applied to every other cross-dialect
claim it makes.

*Common mistake:* picking one of "always aggregate in SQL" or "always
compute in Python" as a universal rule and defending it in the abstract,
rather than engaging with the fact that the right answer is genuinely
feature-specific.

---

## 23. PII and Security ✅✅

### 23.1 Concept

**PII** stands for personally identifiable information. It means any data
that identifies one specific, real person, either by itself or combined
with other data.

This project has touched PII twice already, without ever naming the
topic directly. Section 7.2 noted that `clicks.hashed_ip` is hashed
specifically to avoid resolving real IP addresses to locations. Section
10.4 explained why `dim_user` leaves `users.email` out of the analytical
model. Both were real decisions, made for real reasons. Neither was part
of a repeatable process. This section builds that process: a way to
classify every source column once, check that the classification never
goes stale, and make the result visible to anyone who asks.

Three categories cover every column this project has:

- **none** — the column carries no personal data at all.
- **pseudonymized** — the column stands in for a real identity, but
  doesn't reveal it by itself. A hashed IP address is pseudonymized. So
  is a bare foreign-key integer like `user_id`.
- **direct** — the column identifies one person on its own, with nothing
  else needed. An email address is direct.

Pseudonymized data is still personal data. This surprises people coming
to privacy law for the first time. Hashing an IP address with SHA-256
doesn't erase who it belongs to. The same IP always produces the same
hash. Anyone who can join that hash back to a real IP, or who sees the
same hash show up across many records, can still track one person's
activity. **Anonymization** is different: it means the link back to a
real person has been destroyed, not just disguised. Nothing in this
project's `clicks` table is anonymized. `hashed_ip` and `user_id` are
both pseudonymized, and both are still personal data under a strict
reading of most privacy regulations (GDPR is the clearest example, and
the one this section uses as its reference point, without claiming to be
comprehensive legal advice about any one jurisdiction).

### Why does this exist?

A source database usually has a reason to hold personal data. A `users`
table needs `email` to send a password reset link. An analytics platform
is different. It copies data out of that source system, again and again,
into new places: Bronze objects, a data warehouse, a BI tool's cache, a
data scientist's notebook. Every one of those copies is a new place the
same personal data now lives. Every new place is a new thing to secure,
a new thing to include in a breach-notification plan, and a new thing a
person's "delete my data" request now has to reach.

An analytics engineer who never asks "does this column carry personal
data, and does it need to be here" widens that blast radius by default,
one pipeline at a time, without ever making an active decision to do so.
Section 10.4 already pushed back against that default once, for one
column, in one table. This section turns that same instinct into
something every column in every contract has to answer, not something
that only happens when someone happens to think of it.

### Simple Example (generic, pre-URL-Shortener)

Picture a generic e-commerce system with a `customers` table:

| Column | PII category | Why |
|---|---|---|
| `id` | none | An internal surrogate key. Meaningless outside this database. |
| `email` | direct | Identifies one real person by itself. |
| `favorite_color` | none | A preference. Nobody is identified by liking blue. |
| `device_fingerprint_hash` | pseudonymized | A hashed value used for fraud detection. Doesn't show a raw device ID, but the same device always hashes the same way, so it can still track one person's sessions over time. |

Nothing here is specific to URL shorteners. Any system that stores an
email address, or hashes a value that traces back to one person, faces
this same three-way classification.

### URL Shortener Example

This project has exactly three source contracts: `urls.yaml`,
`users.yaml`, and `clicks.yaml` (Section 12). Running the classification
this section builds against the real, committed contracts finds four
columns:

| Table | Column | PII category |
|---|---|---|
| `users` | `email` | direct |
| `urls` | `user_id` | pseudonymized |
| `clicks` | `user_id` | pseudonymized |
| `clicks` | `hashed_ip` | pseudonymized |

`users.email` is this project's only `direct` column. It's also the only
one Section 10.4 already had a name for. `urls.user_id` and
`clicks.user_id` are pseudonymized for the same reason as each other: a
bare integer means nothing alone, but joined against `users.id`, it
names exactly which account a URL or a click belongs to. `clicks.hashed_ip`
is pseudonymized for the reason explained in 23.1 above: hashing isn't
anonymizing.

Every other column in every other contract — `short_code`,
`original_url`, `occurred_at`, `device_type`, `plan_type`, and the rest —
is classified `none`. `original_url` deserves one honest caveat: a URL's
own query string could, in principle, carry an email address or a name
(a password-reset link is a common real-world example). This
classification doesn't inspect URL content, so that risk isn't caught
here. It's named as a stated limitation in 23.8, not silently ignored.

### 23.2 Architecture

```
 contracts/source/*.yaml  (Section 12)
   Every column already declares: name, type, nullable, description.
   This section adds one more REQUIRED field: pii (none | pseudonymized | direct).
         │
         │ pii.classify_all_contracts()
         │   -- reuses contracts.load_contract() from Section 12,
         │      does not re-implement YAML loading
         ▼
 pii.py
   For each contract: require every column's `pii` field present and
   valid (raises ContractError if not -- see 23.7's Failure Scenario).
   Returns one PiiColumn per column where pii != "none".
         │
         │ `make pii-report`
         ▼
 operator sees, e.g. (ACTUAL OBSERVED, this sandbox):
   users.email        -> direct
   urls.user_id       -> pseudonymized
   clicks.user_id     -> pseudonymized
   clicks.hashed_ip   -> pseudonymized

────────────────────────────────────────────────────────────────────────
 Separately, the DATA ITSELF flows through this pipeline like this:

   users.email (direct)
       │  extract_full: SELECT * -- every column, unmasked (Section 14)
       ▼
   Bronze (bronze/users/.../users.parquet) -- RAW, includes email as-is
       │
       │  dim_user's DDL simply has no email column (Section 10.4,
       │  an explicit, already-documented decision)
       ▼
   analytical layer -- no email anywhere

   clicks.hashed_ip (pseudonymized)
       │  extract_incremental: SELECT * -- already hashed at the SOURCE,
       │                                   before this pipeline ever sees it
       ▼
   Bronze (bronze/clicks/.../clicks.parquet) -- hashed, as it arrived
       │
       │  fact_clicks's DDL simply has no hashed_ip column either --
       │  true since Section 11, but never named as a PII decision
       │  until this section
       ▼
   analytical layer -- no hashed_ip either
```

The second half of this diagram is this section's real finding. Bronze
holds `users.email` completely unmasked, because `extract_full` reads
every column of every configured table with no exceptions (Section 14's
`pd.read_sql_table` call takes no column list). That's a genuine gap,
named plainly in 23.8. What keeps it from reaching further is
`dim_user`'s schema, and that decision was already made and written down
in Section 10.4, before this section existed. `clicks.hashed_ip` never
reaches the analytical layer either, for the same structural reason
(`fact_clicks` simply has no column for it), but nobody had framed that
as a PII decision before now. Both facts came from re-reading the actual
committed DDL against this section's classification, not from assuming
the earlier sections got it right.

### 23.3 Design Decision: classify PII inside the existing data contract, not a separate registry

**Context:** every source column already has one canonical description,
in one place — `contracts/source/*.yaml` (Section 12). A PII
classification needs to live somewhere too, and it needs to answer the
same question a schema contract already answers: "what does this column
actually contain?"

**Decision:** add `pii` as a required field on every column inside the
existing contract files. No new file, no second source of truth.

**Consequences:** a column can never have a schema but no PII
classification, or a PII classification for a column that doesn't exist.
Both are structurally impossible when they live in the same list.

### Alternatives

A separate `pii_registry.yaml`, mapping `table.column` to a category,
checked on its own schedule. This is a common real-world pattern —
larger companies often do exactly this, because a data-governance team
owns PII policy and a data-platform team owns schema, and those are two
different teams with two different review processes.

### Trade-offs

| | Inside the contract (chosen) | Separate registry |
|---|---|---|
| Can drift out of sync with the schema | No — one list describes both | Yes — a new column can be added to the contract and never added to the registry |
| Ownership matches team boundaries | Not by itself — one team edits both concerns | Cleaner in a larger org, where governance and platform teams differ |
| Enforcement | One function (`_require_pii_declared`) blocks a contract from loading incompletely classified | Needs its own separate check, run on its own schedule, against its own file |
| Right choice for this project | Yes — one team, one repo, and the drift risk of a second file is a real, concrete cost | Not yet — would be worth revisiting once a governance team actually exists as a separate owner |

This is the same reasoning Section 12 already used once, for a different
question: one canonical source beats two sources that can quietly
disagree. This section applies that same principle to a compliance
concern instead of a purely technical one.

### 23.4 Implementation

**Implementation Guide (write-it-yourself):** add a `pii` field to every
column in all three files under `contracts/source/`. Then write a small
module, `pii.py`, with two functions. The first, `classify_table`, takes
one already-loaded contract dictionary and returns every column where
`pii != "none"`. Before it returns anything, it must check that every
single column actually has a `pii` key, and that the value is one of
`none`, `pseudonymized`, or `direct` — raise on anything else, using the
same `ContractError` Section 12 already defined, rather than a brand new
exception type. The second function, `classify_all_contracts`, loops
over every `*.yaml` file in `contracts/source/` and calls the first
function on each one. Reuse `contracts.load_contract` for the YAML
parsing — don't write a second YAML loader.

**Reference Implementation** (`ingestion/src/url_shortener_analytics/pii.py`, excerpt):

```python
VALID_PII_CATEGORIES = {"none", "pseudonymized", "direct"}


def _require_pii_declared(contract: dict[str, Any]) -> None:
    table = contract["table"]
    undeclared = [c["name"] for c in contract["columns"] if "pii" not in c]
    if undeclared:
        raise ContractError(
            f"contract '{table}' is missing a 'pii' classification for "
            f"column(s): {', '.join(undeclared)}"
        )
    invalid = {c["name"]: c["pii"] for c in contract["columns"] if c["pii"] not in VALID_PII_CATEGORIES}
    if invalid:
        raise ContractError(
            f"contract '{table}' has invalid 'pii' value(s) {invalid} -- "
            f"must be one of {sorted(VALID_PII_CATEGORIES)}"
        )


def classify_table(contract: dict[str, Any]) -> list[PiiColumn]:
    _require_pii_declared(contract)
    return [
        PiiColumn(table=contract["table"], column=c["name"], category=c["pii"], description=c.get("description", ""))
        for c in contract["columns"]
        if c["pii"] != "none"
    ]
```

Walking through `_require_pii_declared`, one piece at a time: the first
block collects every column name missing a `pii` key at all. If any
exist, it raises immediately, and the error message names every missing
column, not just the first one — the same "report everything wrong in
one pass" habit Section 12's `validate_contract` already established.
The second block checks the columns that DO have a `pii` key, but where
the value itself is wrong (a typo like `"sort-of"` instead of
`"pseudonymized"`). It also raises, with the actual bad values shown.
`classify_table` calls this check first, before doing anything else.
That ordering matters: a contract with even one undeclared or invalid
column raises immediately, and the caller never sees a partial,
misleading result.

### Hands-on Challenge (implement-yourself)

Before looking at the full `pii.py` file, try writing `_require_pii_declared`
yourself from just the description two paragraphs up. Then compare your
version against the real one. A common shortcut: checking only for a
missing `pii` key, and skipping the second check for an invalid value.
Ask yourself what happens if someone writes `pii: Direct` (capitalized)
in a contract file — would your version catch it silently passing
through as neither `none` nor a real category?

### 23.5 Hands-on Exercise

**LAB 19 — Run the real PII report, then break it on purpose.**

First, the happy path:

```bash
make pii-report
```

ACTUAL OBSERVED, this sandbox:

```
ts=... msg="pii column" category='pseudonymized' column='hashed_ip' table='clicks'
ts=... msg="pii column" category='pseudonymized' column='user_id' table='clicks'
ts=... msg="pii column" category='pseudonymized' column='user_id' table='urls'
ts=... msg="pii column" category='direct' column='email' table='users'
ts=... msg="pii report complete" columns_found=4
```

(Exit code confirmed `0`.)

Now break it. Open `contracts/source/urls.yaml` in a scratch copy, and
delete the `pii: pseudonymized` line under `user_id` entirely. Run
`make pii-report` again against that copy. Confirm two things: the
command now exits `1`, and the log line names `user_id` specifically as
the missing column, not just "something is wrong with urls.yaml." Then
undo your edit and confirm `make pii-report` passes again. This is the
same "break it, then fix it, and confirm you understand why each state
looks the way it does" exercise this project has used before for the
Bronze reconciliation and stale-run detection labs — the fix isn't the
point; watching the failure mode actually happen, and reading its exact
message, is.

### 23.6 How to test

```bash
make test
```

ACTUAL OBSERVED, this sandbox:

```
80 passed in 6.76s
```

That's 7 new tests over the 73 from the previous increment: three cover
`classify_table`'s happy path (returns only non-`none` columns, reports
the right category per column, returns real `PiiColumn` instances), one
confirms an all-`none` contract returns an empty list, two confirm the
missing-field and invalid-value cases both raise `ContractError`, and
one runs `classify_all_contracts` against the real, committed
`contracts/source/*.yaml` files and asserts the exact four-column result
shown in 23.1 and 23.5 above. `ruff check ingestion/ benchmarks/` was run
against every file this section touched — ACTUAL OBSERVED: `All checks
passed!`

One further check, specific to this section's Failure Scenario below,
was also run directly in this sandbox: a contract with a schema-valid
but PII-undeclared column was validated with `contracts.validate_contract`
first, then classified with `pii.classify_table`. ACTUAL OBSERVED:

```
validate_contract passed: True violations: []
classify_table raised ContractError: contract 'widgets' is missing a 'pii' classification for column(s): secret
```

### 23.7 Failure Scenario

**What happens when someone adds a new column to a contract, and it has
a real schema (a type, a nullability rule) but no `pii` field?**

The check above answers this directly, and it's a genuine, currently
real gap in this project: `make validate-contracts` — Section 12's main
schema-contract gate, the one most likely to run in CI once this project
has any — passes cleanly. `validate_contract` never looks at `pii` at
all; it only compares type and nullability. The new column's missing PII
classification is caught **only** by `make pii-report`, and only if
someone remembers to run it separately. There is currently no single
command that runs both checks together, and no CI wiring in this repo at
all yet (a gap already named honestly in Section 33's running list).

This means, today, a column carrying an email address could be added to
`clicks.yaml` tomorrow, ship through `make validate-contracts` with a
clean pass, and only get flagged the next time someone happens to run
`make pii-report` by hand. That's a real ordering problem, not a
hypothetical one — see 23.8 for what closing it would take.

### 23.8 Production Considerations

| Aspect | This repo (POC) | Production |
|---|---|---|
| PII classification | `pii.py`, required field per contract column, checked on demand via `make pii-report` | Same idea, but wired into the same CI gate as schema validation, so a missing classification blocks a merge, not just a manual report |
| Bronze encryption at rest | None demonstrated — local MinIO with no server-side encryption configured | SSE-S3 or SSE-KMS enabled on the bucket, with `direct`-classified columns getting the strongest available option |
| Access control on Bronze | None — any credential with bucket access reads every object, including raw `users.email` | Column- or object-level access control, or a separate, more tightly-scoped bucket/prefix for objects known to carry `direct` PII |
| Right to erasure (GDPR "right to be forgotten") | No mechanism at all — Bronze objects are immutable, date-partitioned Parquet files (Section 20), and one person's row could be scattered across many of them | A defined process: locate every object containing a given `user_id`, rewrite each one without that person's rows, and log that the deletion happened — genuinely hard against immutable, partitioned storage, and worth naming as hard rather than pretending it's simple |
| `original_url` query-string PII | Not inspected — a stated limitation of this classification (23.1) | Content-scanning or redaction on ingest, if `original_url` values are ever expected to carry personal data in practice |
| Audit logging | None — nothing records who ran `pii-report`, or who read a Bronze object containing `email` | Every read of a `direct`-classified object logged, retained, and reviewable |

### Principal Data Engineer Perspective

The judgment call worth defending here is the same one Section 18.7 made
for a different gap: naming the CI-ordering problem in 23.7 plainly,
instead of quietly wiring `pii-report` into `validate-contracts` without
comment, or — worse — not noticing the two checks were separate at all.
A classification system that exists but that nothing forces anyone to
run is a real risk, not a solved problem, and a senior reviewer will ask
"what stops someone from shipping a new column with no PII review" long
before they ask whether the classification categories themselves are
well chosen.

The second thing worth naming plainly: this section closes one specific,
narrow gap — every column now has a declared classification — while
leaving open several much larger ones next to it. Encryption at rest,
access control, and the right-to-erasure story in the table above are
all still just "not built," not "built imperfectly." A junior engineer
often treats "we classified our PII" as equivalent to "we handle PII
correctly." A principal-level review keeps those two claims separate,
and says clearly which one this increment actually delivers.

### 23.9 Principal Engineer Interview Questions

**Q: "You hash an IP address with SHA-256 before storing it. Is the
result still personal data? Why or why not?"**

*What's tested:* whether the candidate understands the real difference
between pseudonymization and anonymization, not just the vocabulary.

*What a weak answer looks like:* "No, it's hashed, so it's anonymous
now" — treats hashing as if it destroys the link back to a real person.

*What a strong answer covers:* no, it's still personal data. SHA-256 is
deterministic: the same input IP always produces the same output hash.
That means the hash can still be used to track one person's activity
over time, even without ever recovering the original IP. It can also be
reversed by brute force for a small, guessable input space (IPv4 has
only about 4.3 billion possible values — a rainbow table over all of
them is entirely feasible). True anonymization would need to break the
link back to the original value entirely, not just disguise it.

*Concepts:* pseudonymization vs. anonymization; deterministic hashing as
a re-identification risk, not a privacy guarantee.

*Expected follow-up:* "What would make this genuinely harder to reverse
or re-link?" — A keyed hash (HMAC with a secret key, not plain SHA-256),
which at least removes the brute-force-the-whole-input-space attack,
though it still doesn't make the result anonymous in the strict sense —
the same key always produces the same output for the same input.

*Common mistake:* conflating "I can't easily read the original value" with "this is no longer personal data" — the legal and technical bar for anonymization is much higher than that.

**Q: "Your PII classification lives inside the same YAML file as your
schema contract. What's the failure mode of that choice, and how would
you catch it?"**

*What's tested:* whether the candidate can find the actual weak point in
a design they'd otherwise agree with, not just defend the choice in the
abstract.

*What a weak answer looks like:* "There's no failure mode, it's just
better" — every design choice trades something away; not naming it is a
red flag.

*What a strong answer covers:* co-locating the two concerns means they
can't drift apart from each other, but it doesn't mean the classification
is actually enforced anywhere useful. In this project, `validate-contracts`
(the schema check) and `pii-report` (the PII check) are two separate
commands, and only one of them is likely to run in CI by default. A new
column can pass schema validation while missing its PII classification
entirely, and nothing stops that from shipping. The fix is procedural,
not structural: run both checks in the same CI job, or fold the PII
check into `validate_contract` itself so a contract can't pass validation
at all without a complete, valid `pii` field on every column.

*Concepts:* co-locating related data doesn't automatically mean
co-enforcing it; a check that exists but isn't wired into the actual
gate people rely on provides much weaker protection than it appears to.

*Expected follow-up:* "Would you fold the PII check into `validate_contract`,
or keep it as a separate command?" — Reasonable answers on both sides:
folding it in guarantees it always runs together with schema validation,
but conflates two different concerns (schema correctness vs. governance)
into one function's responsibility, which section 12's own contracts.py
docstring already treats as worth keeping conceptually distinct even if the enforcement is unified.

*Common mistake:* assuming that because a check function exists and has
tests, the property it checks for is actually guaranteed in practice —
a check nobody runs guarantees nothing.

---

## 23. Testing (deep dive) ✅✅

1. CONCEPT
You've been testing since the beginning. This section asks what that actually proves.

Every section in this whole project has ended with make test, and you've watched the pass count climb: 27, then 38, then 59, then 73, now 80. What's never happened is a step back to ask two honest questions: what makes a test suite actually good, and what does "80 tests passing" prove, versus what it quietly doesn't prove at all?

Idea one: the test pyramid

Picture three layers, stacked like a pyramid.

At the bottom, a wide base of unit tests. Each one checks a single function or class, entirely in memory, with no real database, no real network call, no real file touching disk. Fast, cheap, and you can have thousands of them.

In the middle, a smaller layer of integration tests. Each one checks that two or more real pieces actually work together: your code and a real database, or your code and a real object store. Slower, and you need genuinely fewer of them.

At the top, a thin sliver of end-to-end tests. Each one drives the whole system, the way a real user or a real scheduled job actually would, start to finish.

The pyramid shape is the entire point. You want many unit tests, a moderate number of integration tests, and very few end-to-end tests, because every layer up costs more time to run and more effort to maintain, while catching a narrower, later class of bug. A single end-to-end test might take minutes and only tells you "something, somewhere, broke." A unit test takes milliseconds and tells you exactly which function has the bug.

Idea two: test doubles, and specifically mocks

A test double is a fake, fully-controllable stand-in for something a unit test doesn't want to depend on for real: a real database connection, a real network call, a real system clock. You've already used one, extensively, without this name for it yet.

The most common kind is a mock: an object that pretends to be the real thing, remembers exactly how it was called, and lets the test tell it precisely what to return, on command. You already met MagicMock() in an earlier session, when we walked through test_write_bronze_retries_then_succeeds line by line, the "stunt double" that failed twice then succeeded, on a script you wrote.

The generic example: testing a function that charges a credit card

Here's a small, made-up function, nothing to do with URL shorteners:

python
def charge_card(payment_gateway, amount_cents: int) -> str:
    if amount_cents <= 0:
        raise ValueError("amount must be positive")
    return payment_gateway.charge(amount_cents)

A unit test for the error path doesn't need a real payment gateway at all, because that branch never touches it:

python
def test_charge_card_rejects_a_non_positive_amount():
    with pytest.raises(ValueError):
        charge_card(None, amount_cents=0)

A unit test for the success path uses a mock, so the test never actually moves real money:

python
def test_charge_card_calls_the_gateway_with_the_right_amount():
    mock_gateway = MagicMock()
    mock_gateway.charge.return_value = "txn_123"

    result = charge_card(mock_gateway, amount_cents=500)

    mock_gateway.charge.assert_called_once_with(500)
    assert result == "txn_123"

Here's the important, honest limitation, worth sitting with. Neither of these two tests proves the real payment gateway's actual API accepts a call shaped this way. They prove your own code calls .charge(amount_cents) correctly, on whatever it's given. Only a real integration test, against a real sandbox account for that actual payment provider, can prove the real API agrees with your assumption about it.

2. URL SHORTENER EXAMPLE

Your project's own version of that mocked payment gateway is test_reconciliation.py's s3_client = MagicMock(). Your version of an in-memory database is conftest.py's sqlite_engine fixture, a real, working SQLite database that lives only in memory, used by every unit test that needs a real, queryable ingestion_metadata table without needing real Postgres running at all.

The pyramid's middle layer already lives in your project too, under ingestion/tests/integration/. Three real files, each marked @pytest.mark.integration, and deliberately excluded from make test's default run:

test_contracts_integration.py needs only real Postgres.
test_full_load_integration.py and test_incremental_load_integration.py need real Postgres and real MinIO, since they exercise your actual boto3 calls against a genuine S3-compatible endpoint, not a mock pretending to be one.

Notice what's missing: your project has no end-to-end layer at all yet. That's not an oversight. There's no scheduler running your pipeline automatically yet, so there's genuinely no "run the whole thing the way production would" scenario to write a test for.

3. DESIGN
A third, separate axis: coverage

Everything above answers "does my code do the right thing." Coverage answers a completely different question: "which lines of my code did the test suite actually run at all?" These are not the same question. A test can execute a line of code and still contain a wrong or missing assertion about what that line should have done. Coverage tells you nothing about assertion quality; it only tells you what got touched.

bash
make coverage

runs pytest --cov=url_shortener_analytics --cov-report=term-missing. --cov=url_shortener_analytics tells the tool which package to actually measure. Without it, the report would measure pytest's own internal code, which is never what you want. --cov-report=term-missing doesn't just print a percentage; it prints the exact line numbers that were never executed, which is what turns the report into something actionable instead of a single, uninformative number.

The real design decision: measure the number, but don't fail the build over it yet

Here's a real, genuine discovery this section made. pytest-cov, the tool make coverage depends on, has been sitting listed as a dependency in your project's pyproject.toml since this project's very first version. Nobody had ever actually run it. This section ran it for the first time, and got a real, honest number: 58% of the project's own source lines are actually exercised by the unit-test suite.

The decision made here: wire up make coverage so this number is visible and reproducible on demand, but deliberately do not add a hard failure threshold (something like --cov-fail-under=80) that would make make test itself fail if coverage drops.

Why not just set a strict threshold immediately? Because a threshold picked before you've ever seen your real number is close to arbitrary, and your real number, 58%, sits well below a typical target like 80% anyway. Forcing that gate on immediately would mean either quietly lowering the bar to match reality, or scrambling to write tests today, possibly chasing the number itself rather than testing what actually matters.

This mirrors a decision you've already seen twice before in this project. Back in File Layout, get_file_layout_report was built to detect a small-file problem without ever automatically fixing it, because automatic remediation without a strong-enough safety net can cause more harm than the problem it's solving. Measuring coverage honestly, without gating on it yet, is that exact same cautious posture, applied here to a testing concern instead of a storage one. There's no CI system in this project yet to consistently enforce a gate at all. A gate that nothing actually enforces is arguably worse than no gate, because it implies a guarantee that isn't real.

	Measure only (chosen)	Measure and gate at a fixed threshold
Honest about the real number	Yes, 58% reported exactly as it is	An arbitrary picked number, like 80%, risks looking authoritative when it's a guess
Forces new tests today	No	Yes, immediately, possibly for the wrong reasons
Right choice, right now	Yes, no CI exists yet to consistently enforce a gate	Becomes the right next step once real CI exists
4. IMPLEMENTATION
makefile
# Measured, not gated -- no --cov-fail-under threshold yet.
coverage:
	pytest --cov=url_shortener_analytics --cov-report=term-missing
5. RUN, and a real number worth understanding, not just reading
bash
make coverage

Real, already-observed output from this exact project:

Name                                                           Stmts   Miss  Cover   Missing
--------------------------------------------------------------------------------------------
cli.py                                                            222    222     0%   50-479
config.py                                                          15      0   100%
contracts.py                                                       79      2    97%   102, 105
db.py                                                               9      9     0%   11-32
extract_full.py                                                    29      0   100%
extract_incremental.py                                             34      0   100%
logging_setup.py                                                   20     20     0%   13-48
metadata.py                                                        98     16    84%
object_store.py                                                    78      1    99%
pii.py                                                             30      0   100%
reconciliation.py                                                  22      0   100%
--------------------------------------------------------------------------------------------
TOTAL                                                             643    270    58%

Before reading further, predict: which file has the lowest coverage, and why? A common, wrong guess is "whichever file is newest or most complex." Look at the real table. cli.py sits at a flat 0%, and db.py and logging_setup.py sit at 0% too.

Here's the honest, structural reason, and it's worth understanding rather than memorizing. Open cli.py yourself and look at one of its command functions, say run_full_load_command. It does four things: read config, build a database engine, build an S3 client, call run_full_load. It's a thin wrapper. Every real decision inside it, the actual logic worth testing, already lives in a separate function, run_full_load, which already has its own direct, dedicated unit tests that don't go through cli.py at all. cli.py's own code, the argument parsing and the wiring together of other pieces, never gets executed by any unit test, because unit tests call the real logic functions directly, skipping the CLI layer entirely.

Is 0% on cli.py therefore nothing to worry about? Not quite, and it's worth being precise here rather than dismissing it. It means a narrower, specific class of bug could slip through unnoticed: wrong argument names, a typo in a logged field name, a wrong exit code, things that live specifically in the wiring, not in the logic it calls. A shallow reading of "0% coverage" calls this file untested. A more careful reading asks exactly what would actually break if that 0% never improved, and the honest answer is real, but narrower, bugs than the raw number alone suggests.

6. EXPERIMENT (a real environment-mismatch failure, worth reproducing)

Here's a genuine, already-observed failure, worth understanding closely, because it's a distinct kind of test failure from anything you've hit so far.

test_contracts_integration.py needs a real Postgres connection. Its settings fixture falls back to a class-level default connection string whenever nothing overrides it, and that default names port 5433, matching this project's own docker-compose.yml mapping. In this cloud sandbox, Postgres was installed directly, not through Docker, and genuinely listens on the standard port 5432 instead. Running the test with no override produced a real sqlalchemy.exc.OperationalError, a connection-refused failure, happening in the network driver layer, before a single line of this project's own contract-validation logic ever ran.

bash
pytest ingestion/tests/integration/test_contracts_integration.py -v -m integration

Then, supplying the correct port:

bash
DATABASE_URL="postgresql+psycopg://analytics:analytics@localhost:5432/analytics" \
  pytest ingestion/tests/integration/test_contracts_integration.py -v -m integration

The honest, easy-to-miss lesson: that first failure proved nothing about whether validate_all_contracts actually works. It only proved that one test's default settings didn't match one specific sandbox's Postgres port. A developer unfamiliar with this distinction could see a failing integration test, wrongly assume the real logic is broken, and go debugging contracts.py, the wrong file entirely.

A note on your own real machine, since it's genuinely different here: your Mac runs Postgres through real docker-compose, which does map to port 5433, matching this fixture's own default. So this exact failure likely won't reproduce for you the same way it did in this sandbox. To feel the same class of failure yourself, deliberately point DATABASE_URL at a wrong port on purpose, something like port 9999, and run the same integration test. You should see the identical shape of failure: a connection error, before your real code ever runs, proving the same lesson, that an integration test's failure can be about the environment, not the code, even though the two look identical from the outside until you actually read the error.

7. PRODUCTION VIEW
Aspect	This project right now	Real production
Separating unit from integration	Two directories, one pytest marker, unit tests run by default	Same structure, typically split into two separate CI jobs, so integration tests don't slow down the fast unit-test feedback loop
Coverage	Measured on demand, not gated, 58% today	Measured on every CI run, gated at an agreed threshold, with that threshold raised gradually over time, not fixed once
CI	None. Every check in this whole project has been run by hand and reported honestly as such	A real pipeline running make test, make lint, and gated make coverage on every code change, plus make test-integration against real, disposable Postgres/MinIO containers
Environment mismatches	A developer has to already know this sandbox's port differs from docker-compose's default	A CI job's environment is defined once, centrally, so no developer ever has to guess a port by hand
Flaky tests	Not yet a concern, no test here has ever failed non-deterministically	A quarantine mechanism, once a real flaky test is found, so one intermittent failure can't block every other developer
8. PRINCIPAL ENGINEER VIEW

If asked "your unit tests all pass, does that mean your pipeline works," the strong answer isn't yes or no, it's precise: "unit tests prove my own logic is internally correct, in isolation. They cannot prove my code actually works against the real Postgres or real MinIO it depends on, because a mock only behaves however I told it to behave, and SQLite genuinely isn't Postgres." Having the real, concrete example ready, the port-mismatch failure above, that a real integration test genuinely caught and a unit test structurally never could have, is a much stronger answer than reciting the pyramid as theory.

The second thing worth naming plainly: the discovery pattern itself, not just this one result. pytest-cov sat declared and completely unused for the entire life of this project up to this point. Nobody lied about it; nobody had actually looked. This is the same shape of finding as watermark_start in the Ingestion Metadata section, and the validate-contracts-versus-pii-report ordering gap in the PII section: the real skill isn't writing correct code on the first try, it's habitually checking whether something you believe is done actually is, on a real schedule, rather than assuming it once and moving on.

9. REMEMBER
The test pyramid: many fast unit tests, fewer integration tests, almost no end-to-end tests. Each layer up costs more and catches a narrower, later class of bug.
A mock proves your code calls something correctly. It never proves the real thing on the other end actually behaves the way you assumed.
Coverage measures which lines ran, never whether the assertions checking them are any good. A high number with weak assertions can still hide real bugs.
An integration test can fail for a reason that has nothing to do with your code, an environment mismatch, not a real bug. Read the actual error before assuming the logic is broken.

## 28. Architectural Principles

Introduced here, demonstrated incrementally as more of Phase 1 is built.
Each entry: meaning, why it matters, and what's true about *this repo,
right now* — not an aspirational claim about the finished system.

**1. Separation of OLTP and OLAP.** *Meaning:* analytical workloads never
query the operational database directly. *Phase 1, now:* every read in
`extract_full.py` goes through this repo's own standalone Postgres mirror
(ADR-009), never a shared instance with live application traffic.

**2. Immutable raw data.** *Meaning:* once written, a Bronze object is
never edited in place — corrections happen by writing a new, superseding
version, not by mutating history. *Phase 1, now:* `write_bronze` always
writes a complete new object at a deterministic key; nothing in this
codebase opens an existing Parquet file and appends to or edits it.

**3. Idempotent processing.** *Meaning:* rerunning a pipeline step
produces the same end state as running it once. *Phase 1, now:* proven by
`ingestion/tests/integration/test_full_load_integration.py::test_rerunning_full_load_overwrites_not_duplicates`
and LAB 4/5.

**4. Reproducibility.** *Meaning:* the same inputs and code produce the
same outputs, and the whole system can be rebuilt from a clean clone.
*Phase 1, now:* `scripts/seed_sample_data.py` uses a fixed random seed
(42); `docker-compose.yml` pins every image to a specific tag, never
`:latest`.

**5. Schema contracts.** *Meaning:* producers and consumers of a dataset
agree on its shape explicitly, not by convention. *Phase 1, now:*
documented informally in `schemas/source/urls.md`; formal, versioned
contracts land in the planned Section 12.

**6. Explicit ownership.** *Meaning:* every dataset and pipeline has a
named owner accountable for it. *Phase 1, now:* not yet formalized — a
single-engineer portfolio project doesn't need this machinery yet, but the
principle is recorded for when Phase 3+ introduces multiple pipelines.

**7. Least privilege.** *Meaning:* every credential grants the minimum
access it needs. *Phase 1, now:* explicitly **not** met — `.env.example`
uses a single MinIO root credential for everything, called out as a POC
simplification in [Section 14.8](#148-production-considerations).

**8. Metadata-driven processing.** *Meaning:* what a pipeline does (which
tables, which strategy) is configuration, not hardcoded logic.
*Phase 1, now:* `ingestion/configs/pipelines.yaml` — adding a table means
editing YAML, not Python.

**9. Failure isolation.** *Meaning:* one component's failure shouldn't
silently corrupt or block unrelated components. *Phase 1, now:*
`cli.py`'s `run_full_load_command` catches each table's failure
independently and continues to the next table, reporting all failures at
the end rather than stopping at the first one.

**10. Backfillability.** *Meaning:* historical data can be
(re)constructed after the fact. *Phase 1, now:* full load inherently
supports this (rerun it any day) — the harder incremental-load version of
this principle is deferred to Section 15.

**11. Observability.** *Meaning:* the system's internal state is
queryable, not just inferred from external symptoms. *Phase 1, now:*
`ingestion_metadata` is exactly this for the ingestion pipeline — see
Section 14.7's failure scenario for what it does and doesn't tell you.

**12. Scalability.** *Meaning:* the design's bottlenecks are known and
have a described next step, not just "hope it holds." *Phase 1, now:*
named explicitly in Section 14.9's second interview question; a full
scale-design writeup is the planned Section 27.

**13. Cost awareness.** *Meaning:* every architectural choice is made with
an eye on what it costs to run, not just whether it works. *Phase 1, now:*
implicit in choosing batch over streaming (Section 13) — this is the
principle that decision is really an instance of.

---

## 29. Architecture Decision Records

### ADR-001: Separate OLTP and analytics

**Context:** Redirect latency is this project's stated hot path (10:1
read:write ratio, per the source repo's own capacity estimate).
**Decision:** Analytics never queries the OLTP database directly; all
analytical access goes through Bronze (and later Silver/Gold) data derived
by batch extraction. **Alternatives considered:** a read replica serving
analytics queries directly. **Trade-offs:** a replica alone solves resource
contention but not analytical query shape (row-oriented storage, no
pre-aggregation); a real pipeline solves both at the cost of data being
only as fresh as the last batch run. **Consequences:** analytics data has
inherent latency (currently: however often `make ingest-full` is run —
there is no scheduler yet).

### ADR-002: Use MinIO / object storage for raw data

**Context:** Extracted data needs somewhere to land that's independent of
any specific downstream query engine. **Decision:** MinIO locally
(S3-compatible), the same `boto3` client code as real AWS S3.
**Alternatives considered:** writing extracted data directly into another
Postgres schema/database. **Trade-offs:** object storage decouples storage
from compute (any engine can read Parquet later) but adds an extra
component to run and offers no transactional guarantees a database would.
**Consequences:** this repo depends on Docker to run MinIO locally; the
same client code moves to real S3 by changing only `MINIO_ENDPOINT`.

### ADR-003: Use Parquet for Bronze storage *(benchmarked)*

**Context:** Extracted tabular data needs a file format. **Decision:**
Apache Parquet, via PyArrow, snappy-compressed. **Alternatives
considered:** CSV, JSON-lines. **Trade-offs:** Parquet is columnar
(efficient for analytical, column-selective reads) and self-describing
(embeds its own schema) but isn't human-readable by opening the raw file,
unlike CSV. **Consequences:** every consumer of Bronze data needs a
Parquet-aware reader (trivial — `pandas`, `pyarrow`, every real analytical
engine supports it natively). The CSV/Parquet benchmark this ADR named as
"planned" has now genuinely been run (`benchmarks/parquet_vs_csv_vs_json.py`,
Section 19) at two scales — size and single-column-read time favor Parquet
decisively at both; full-table-read time only pulls ahead past a few tens
of thousands of rows, honestly reported in Section 19.6 rather than
glossed over.

### ADR-004: Batch ingestion before CDC/streaming

**Context:** The source application has no event stream to consume (see
Section 1.5) and this project's data volume doesn't currently justify
streaming infrastructure. **Decision:** batch (full load now, incremental
next) for the entirety of Phase 1-4; Kafka/CDC only after Phase 4.
**Alternatives considered:** building on Kafka/Debezium from the start.
**Trade-offs:** batch is simpler to build, run, and debug, at the cost of
data freshness being bounded by run frequency rather than near-real-time.
**Consequences:** every later phase's design must not assume streaming
exists; the eventual Kafka introduction (Phase 5+) needs its own
migration path onto whatever this pipeline has already built.

### ADR-005: Watermark-based incremental ingestion *(implemented)*

**Context:** `clicks` will grow without bound; full-reading it every run
doesn't scale. **Decision:** an `id`-based (not timestamp-based) watermark,
for the same clock-skew and duplicate-timestamp reasons documented in this
project's earlier notebook prototype — see
[Section 15.3](#153-design-decision) for the full reasoning and
[Section 15.9](#159-principal-engineer-interview-questions) for a worked
failure example of the rejected alternative. **Alternatives considered:**
timestamp-based watermark; full CDC. **Trade-offs:** id-based watermarking
is immune to clock skew but tracks insertion order, not event order —
late-arriving-data handling and update/delete capture are both explicitly
out of scope (see [Section 15.8](#158-production-considerations)).
**Consequences:** implemented in `extract_incremental.py` and
`metadata.py`; `clicks` is the only table currently onboarded to this
strategy (`ingestion/configs/pipelines.yaml`); the Bronze idempotency
guarantee this decision depends on has one documented residual edge case
— see [Section 15.7](#157-failure-scenario).

### ADR-006: Dimensional analytical model *(schema implemented; not yet populated)*

**Context:** Bronze data is a direct mirror of OLTP structure — not shaped
for analytical queries (Section 2). **Decision:** a pure star schema
(fact/dimension) analytical layer — `dim_date`, `dim_url`, `dim_user`,
`dim_device`, `fact_clicks` — designed in Sections 9-11, DDL committed at
[`sql/analytics/`](../sql/analytics/). **Alternatives considered:** a
fully normalized analytical layer (reusing the OLTP shape directly); a
snowflake schema (normalizing `plan_type` and `original_url_domain` into
their own dimensions). See Section 11's Alternatives/Trade-offs for the
full reasoning on both. **Trade-offs:** a star schema trades some storage
redundancy for query simplicity — the right trade at this project's
current dimension cardinality (see Section 11.1). **Consequences:** the
schema is committed and reviewable, but genuinely empty — no transform
populates it yet; that's explicitly Phase 2's job ("Data Lake,
Transformation & Data Quality"), not deferred silently but named as a
current limitation in Section 33 and the Phase 1 Completion Checklist.

### ADR-007: Preserve Bronze data (immutability)

**Context:** Downstream transformations (Phase 2+) need to be re-runnable
against history without re-extracting from a live OLTP database that has
since changed. **Decision:** Bronze objects are never edited in place or
deleted by this pipeline; corrections are new objects. **Alternatives
considered:** overwriting/cleaning data at extraction time. **Trade-offs:**
preserving raw data costs storage indefinitely but makes every downstream
transformation fully reproducible and backfillable. **Consequences:**
storage cost grows without bound unless a retention/lifecycle policy is
added later (not yet — noted as a gap in Section 34).

### ADR-008: Hypothetical `users` and `clicks` tables, clearly labeled

**Context:** The mega-prompt's/this project's curriculum assumes
`users` and `clicks` concepts that Section 1.5 confirmed don't exist in
the real application. **Decision:** build them as clearly-labeled
hypothetical tables (`COMMENT ON TABLE` in the SQL itself, explicit
callouts everywhere they're referenced) rather than skip every topic that
depends on them, or silently pretend they're real. **Alternatives
considered:** skip user/click-dependent topics until the real app builds
them; fork into "real-schema-only" vs. "hypothetical-extension" tracks.
**Trade-offs:** risks blurring what's real vs. illustrative for a future
reader — mitigated by labeling at every point of use. **Consequences:** if
the real application ever adds real `users`/click-tracking, this
platform's hypothetical schema should be reconciled against the real one,
not assumed to already match it.

### ADR-009: This platform owns its own OLTP-mirror Postgres

**Context:** This repository is meant to be independently clonable and
runnable as a standalone portfolio project — coupling its
`docker-compose.yml` to also require cloning and running the separate,
private `url-shortener` repository would break that. **Decision:** this
repo's `docker-compose.yml` provisions its own Postgres instance, schema
mirrored from the real application (`sql/source/001_urls_real_schema.sql`),
on a different host port (5433) so it doesn't collide with a real
`url-shortener` Postgres running on the same machine. **Alternatives
considered:** `docker compose -f ../url-shortener/docker-compose.yml -f docker-compose.yml up`,
spanning both repositories (this was the approach used in this project's
earlier notebook-based prototype). **Trade-offs:** a standalone mirror can
drift from the real schema if the real application's migrations change
without this repo being updated to match (mitigated by the dated,
explicit provenance comment at the top of `001_urls_real_schema.sql`); in
exchange, anyone can clone this one repository and run it. **Consequences:**
in a real production deployment, this pipeline would instead point
`DATABASE_URL` at the actual application's read replica — the mirror is a
Phase 1 development convenience, not a production design, and is
documented as such in the README.

### ADR-010: Data contract validation by coarse type category, not exact type

**Context:** source tables need a machine-checkable contract (Section 12)
that doesn't silently drift from the real schema the way
`schemas/source/*.md` can (Section 9's named gap). This project validates
contracts against SQLite in unit tests and Postgres in integration tests /
production. **Decision:** contracts declare column types as one of five
coarse categories (`integer`, `text`, `boolean`, `timestamp`, `numeric`),
compared via `contracts.py`'s `_categorize_type`, which normalizes both
dialects' reflected types through SQLAlchemy's generic type hierarchy —
not exact SQL type strings. Structural mismatches are reported as a full
`ContractValidationResult` (every violation and warning found), not raised
on the first one; an extra, undeclared column is a warning, not a
violation. **Alternatives considered:** exact SQL type comparison
(Postgres-only, or duplicated per-dialect contracts); raising on the first
violation found; treating every mismatch, including additive ones, as
breaking. **Trade-offs:** coarse categories trade some precision (e.g. a
`BIGINT`→`INTEGER` narrowing wouldn't be caught) for cross-dialect
portability and tolerance of safe, additive schema evolution — see
Section 12.2/12.3's full reasoning. **Consequences:** `make
validate-contracts` and its unit/integration tests (`test_contracts.py`,
`test_contracts_integration.py`) are the enforcement mechanism; the
analytical layer (`sql/analytics/`) is explicitly **not** yet
contract-checked — a named gap, not a silent one (Section 12.7).

### ADR-011: One bucket, prefix-separated layers

**Context:** Bronze exists today; Silver and Gold (Phase 2+) will need
somewhere to land, and that decision shapes key-naming conventions
everywhere in this codebase. **Decision:** one bucket, `analytics-lake`,
with each layer as a top-level key prefix (`bronze/`, later `silver/`,
`gold/`) rather than a separate bucket per layer. **Alternatives
considered:** a bucket per layer; a bucket per table. **Trade-offs:**
separate buckets give cleaner per-layer IAM boundaries in a real AWS
deployment and make a lifecycle policy trivial to scope per layer; a
single bucket with prefixes is simpler to provision and keeps
`MINIO_BUCKET` a single config value — the right trade for Phase 1's
actual operational complexity, at the cost of that per-layer
access-control convenience. See Section 18.3 for the full reasoning.
**Consequences:** migrating to per-layer buckets later, if a real
per-layer IAM requirement emerges, means moving every existing object to
a new bucket, not just renaming one — a real, non-trivial cost, named
here rather than discovered only once it's needed.

### ADR-012: Partition pruning scoped to date-partitioned full-load keys only

**Context:** this repo has two Bronze partitioning schemes already in
production — full-load's calendar-date keys and incremental-load's
watermark-range keys (Sections 14-15) — and a pruned listing function
needs to know which scheme it's constructing prefixes for. **Decision:**
`object_store.list_bronze_keys_for_date_range` (Section 20) handles the
date-partitioned, full-load case only; no equivalent watermark-range-pruned
function was built. **Alternatives considered:** a single, more general
pruning function dispatching on `load_type`. **Trade-offs:** a unified
interface would look more complete, but a watermark range isn't
enumerable the way a date range is — it has to be discovered from
`ingestion_metadata` first (already possible via
`list_successful_bronze_keys`, Section 17), which is a genuinely
different code path, not a variant of prefix construction. See Section
20.3 for the full reasoning. **Consequences:** a reader wanting a pruned
view of incremental Bronze data should query `ingestion_metadata`
directly rather than reach for `list_bronze_keys_for_date_range`, which
will not raise an error but will not prune correctly for that load type
either — a named scope boundary, not a silently-incomplete abstraction.

### ADR-013: Detect file-layout degradation, never auto-compact

**Context:** this repo's Bronze layout could, in principle, degrade toward
a small-file or too-few-too-large-files problem (Section 21) if a future
write path changed without file-count discipline; this is now the second
independent section (after Section 17.7's Bronze reconciliation) to face
essentially the same choice between reporting a problem and automatically
fixing it. **Decision:** `get_file_layout_report` (Section 21) measures
and reports layout health only; it never compacts, merges, or rewrites any
Bronze object. **Alternatives considered:** an automatic compaction job
that detects a partition with too many small files and rewrites them into
fewer, larger ones. **Trade-offs:** automatic compaction would actually
fix a degrading layout rather than just reporting it, but compaction is a
genuinely more dangerous operation than detection — it requires deleting
original objects after rewriting their contents elsewhere, and any bug in
that rewrite logic risks real data loss, not just a stale report;
detection-only costs a human having to act on what's found, in exchange
for a categorically safer default. **Consequences:** this formalizes
detect-don't-remediate as a recurring, deliberate posture across this
project's operational tooling (Section 17.7's reconciliation, now Section
21's layout reporting), not a one-off choice specific to either section —
any future operational-health check this codebase adds should default to
the same posture unless a specific, named reason justifies auto-remediation.

---

## 33. Phase 1 Summary (so far)

**What we've built in this increment:** PII and Security (Section 23),
turning two earlier one-off decisions — Section 7.2's `hashed_ip`
reasoning and Section 10.4's `dim_user`-excludes-`email` decision — into
one repeatable process every source column now goes through. Genuinely
new code: a required `pii` field (`none` / `pseudonymized` / `direct`)
added to every column in all three `contracts/source/*.yaml` files;
`pii.py`, with `classify_table` and `classify_all_contracts`, built as a
thin layer over Section 12's existing `contracts.load_contract` rather
than a second, separate loader; and a new `pii-report` CLI command. 7
new unit tests (73 → 80).

**A note on this increment specifically:** the real finding here is a
gap between two checks that look like they'd overlap, but don't. `make
validate-contracts` (Section 12) never looks at the new `pii` field at
all — it only compares type and nullability. A new column could ship
with a full schema and no PII classification, pass `validate-contracts`
cleanly, and only get caught by `make pii-report`, run separately, by
hand. This was proven directly in this sandbox, not assumed: a
schema-valid contract with an undeclared `pii` field passed
`validate_contract` with zero violations, then raised `ContractError`
from `classify_table` on the very next line (Section 23.6). Section
23.2's architecture diagram also surfaces a second, quieter finding:
`fact_clicks` has never carried a `hashed_ip` column, since Section 11,
but nothing before this section had named that as a PII-driven decision
the way Section 10.4 named `dim_user`'s `email` exclusion.

**Concepts taught so far, at full depth:** the real application's
architecture and schema, OLTP vs. OLAP, full load and incremental-load
ingestion (watermarks, idempotency, checkpointing, Bronze reconciliation),
the entire data modeling layer (Sections 7-12), the Storage block in
full (Sections 18-21), `ingestion_metadata` as this pipeline's control
plane (Section 22), and now PII classification: the real difference
between pseudonymization and anonymization, why a hashed value is still
personal data, and why this project chose to classify PII inside its
existing data contracts rather than in a separate registry (Section 23).

**Known limitations, stated honestly:** `pii-report` and
`validate-contracts` are two separate commands, and only the
classification check named in this increment's own note above closes
the gap between them by describing it — the gap itself is still open;
neither is wired into any CI gate, since this repo has none yet. PII
classification does not inspect column *content* — `original_url` is
classified `none`, but a URL's own query string could in principle carry
personal data, and this section's tooling would not catch that
(Section 23.1/23.8). No encryption at rest, no access control on Bronze
objects, no right-to-erasure mechanism, and no audit logging exist for
any of this project's `direct` or `pseudonymized` columns — all named
plainly as "not built" in Section 23.8's Production Considerations table,
not glossed over as already handled. The three-way classification itself
(`none`/`pseudonymized`/`direct`) is this project's own simplification;
real privacy frameworks (GDPR in particular) draw finer distinctions this
section does not attempt to fully cover. What *was* genuinely verified
in this sandbox this increment: `pii-report` run against the real,
committed contracts, producing the exact four-column result reported in
Section 23.1; the missing-`pii`-field and invalid-`pii`-value failure
cases, each confirmed to raise `ContractError` with the specific column
name included; and the `validate-contracts`-passes-while-`pii-report`-fails
gap, reproduced directly against a real SQLite engine.

**Immediate next increment:** Testing (deep-dive, Section 24) or Failure
Scenarios (Section 25), now that 80 unit tests and three increments'
worth of Failure Scenario subsections exist to consolidate, or
Performance (Section 26); whichever the reader wants to tackle next.

---

## 34. Phase 1 Completion Checklist

| Item | Status | Evidence | What remains |
|---|---|---|---|
| Existing application understood | ✅ Done | Section 1 | — |
| OLTP vs OLAP understood | ✅ Done | Section 2 | — |
| Analytics requirements defined | ✅ Done | Section 7's metrics catalog (8 metrics) | — |
| Metrics defined | ✅ Done | Section 7.1 | — |
| Grain defined | ✅ Done | Section 8 (`fact_clicks`: one row per click) | — |
| Analytical model designed | ✅ Done | Sections 9-10, `schemas/analytics/dimensional-model.md` | — |
| Star schema implemented | ✅ Done (schema only) | Section 11, `sql/analytics/*.sql`, genuinely applied against real Postgres in this sandbox (4,018-row `dim_date`, Unknown-member `dim_user`, `NOT NULL` constraint proven) | Not yet populated — Phase 2's transform |
| Data contracts defined | ✅ Done | Section 12, `contracts/source/*.yaml`, `contracts.py`, genuinely validated (pass and fail cases both) against real Postgres in this sandbox | Analytical-layer contracts not yet in scope (Section 12.7) |
| Full ingestion implemented | ✅ Done | `extract_full.py`, LAB 1 | — |
| Incremental ingestion implemented | ✅ Done | `extract_incremental.py`, LAB 2/3, Section 15 | — |
| Watermark implemented | ✅ Done | `metadata.get_last_watermark`, wired into `extract_incremental.run_incremental_load`, Section 15 | — |
| Checkpoint implemented | ✅ Done | `metadata.py`, `ingestion_metadata` table, exercised by both load types; stale-run detection (`find_stale_running_runs`, `check-stale-runs`), Section 16 | — |
| Idempotency implemented | ✅ Done | `object_store.build_bronze_key` / `build_bronze_incremental_key`; `bronze_key` persistence and Bronze reconciliation (`reconciliation.py`, `reconcile-bronze`), Section 17 | One documented residual edge case, Section 15.7; reconciliation detects drift but doesn't remediate it, by design, Section 17.7 |
| MinIO configured | ✅ Done | `docker-compose.yml`, `object_store.py`; single-bucket/prefix-layer convention formalized, Section 18, ADR-011 | — |
| Parquet implemented | ✅ Done | `object_store.write_bronze` / `write_bronze_incremental`; benchmark vs CSV/JSON genuinely run at two scales, Section 19 | — |
| Partitioning implemented | ✅ Done (single file per partition; pruning added) | Hive-style date/watermark-scoped keys since Sections 14-15, formalized in Section 18.2; genuine partition-pruned listing for full-load tables, `list_bronze_keys_for_date_range`, Section 20, ADR-012 | Not true multi-file-per-partition splitting; pruning not extended to incremental's watermark-range keys (named scope boundary, Section 20.3) |
| File layout health reporting implemented | ✅ Done (detect-only) | `object_store.get_file_layout_report`, `layout-report` CLI command, Section 21, ADR-013 | No auto-compaction, by deliberate design (ADR-013); 8 MB small-file threshold is a POC default, not empirically derived (Section 21.8) |
| Ingestion metadata deep-dive completed | ✅ Done | `watermark_start` gap found and fixed, `get_run_history`, `get_ingestion_summary`, `ingestion-history`/`ingestion-summary` CLI commands, Section 22 | `get_run_history` has no pagination guard on `limit` (Section 22.8) |
| PII identified | ✅ Done | Every column in `contracts/source/*.yaml` now declares `pii` (`none`/`pseudonymized`/`direct`); `pii.py`, `pii-report` CLI command, Section 23 | Not content-inspecting — `original_url` query strings aren't scanned (Section 23.8); no encryption/access-control/erasure mechanism built yet (Section 23.8) |
| Tests implemented | ✅ Done (unit + partial integration) | 80 passing unit tests (up from 38 four increments ago); the contracts integration test, and prior increments' `metadata.py` additions, genuinely passed against a real (non-Docker) local Postgres in this sandbox; this increment's PII classification tests, including the real-contracts-directory test, all genuinely run | Full-load, incremental-load, and reconciliation integration tests still need real MinIO, not available here — user should run `make up && make test-integration` locally for the complete suite |
| Failure scenarios tested | ✅ Partial | Sections 7-12 (data modeling), 14.7, 15.7, 16.7, 17.7, 18.7, 19.7, 20.7, 21.7, 22.7, 23.7 | Remaining named in Section 25's index |
| Performance benchmark completed | ✅ Partial | Parquet vs. CSV/JSON, Section 19, genuinely run at two scales | Extraction-time-at-scale and partition-pruning real-network-latency benchmarks not yet run, Section 26 |
| Architecture diagrams completed | ✅ Partial | 10+ diagrams so far, including the full star schema ER diagram (Section 10.1) and Sections 18/20/21/22/23's object-storage, partition-pruning, file-layout, control-plane, and PII-classification/data-flow diagrams | More land with later sections (data lifecycle, failure/recovery, final architecture) |
| ADRs documented | ✅ 13 of 13+ planned | Section 29 | No new ADR this increment — Section 23's design decision (classify PII inside contracts, not a separate registry) is documented in Section 23.3 but not yet promoted to its own numbered ADR |
| Interview questions reviewed | ✅ Partial | Sections 7, 8, 9, 10, 11, 12 (Category C-N, data modeling), 14.9, 15.9, 16.9, 17.9, 18.9, 19.9, 20.9, 21.9, 22.9, 23.9 | Remaining categories not yet covered, Section 31 |
| Hands-on labs completed | ✅ Partial | LAB 1-19 (LAB 1-5 ingestion, LAB 6-9 requirements/grain/source-model/star-schema, LAB 10 Unknown-member join, LAB 11 contract violation, LAB 12 stale-run detection, LAB 13 Bronze reconciliation, LAB 14 storage growth/idempotency, LAB 15 Parquet benchmark, LAB 16 partition pruning, LAB 17 file-layout report, LAB 18 watermark_start fix + metadata readers, LAB 19 PII report break/fix) | LAB 20+ |
| README updated | ✅ Done | `README.md` | — |
| Git repository clean | ✅ Done | Section 35 | — |
| No secrets committed | ✅ Done | `.gitignore`, `.env.example` reviewed | — |

---

## 35. Git Repository Review

**Structure:** clean, matches the map in Section 4; no stray files at the
repository root.

**Naming:** consistent `snake_case` for Python, `kebab-case` nowhere yet
needed, table/column names match the real source schema's own convention.

**Tests:** unit and integration cleanly separated by both directory and
pytest marker; unit tests genuinely run and pass in an environment with no
Docker at all (verified while writing this guide).

**Configuration:** `.env.example` present and complete; no credential in
any committed file resolves to anything beyond localhost containers.

**Secrets:** none committed. `git status`/`.gitignore` reviewed by hand
before this increment's commit.

**Duplicate code:** none yet — the codebase is small enough that this
hasn't become a risk.

**Generated artifacts:** none committed — `pg_data/`, `minio_data/`,
`.pytest_cache/`, `.ruff_cache/` all excluded.

**Reproducibility:** `docker-compose.yml` pins every image tag explicitly;
`scripts/seed_sample_data.py` uses a fixed seed; a clean clone plus the
Quick Start commands in `README.md` should reproduce this exact state.

**What should improve before this is "done" for Phase 1:** the items
marked ⏳ in Section 34's checklist — this is an honest, current snapshot,
not a claim of completeness.

---

## 36. What Phase 2 Will Add

Not implemented here — Phase 2 builds directly on this exact repository
and introduces: Spark for distributed transformation; a formal
Bronze → Silver → Gold refinement pipeline that **populates the star
schema Phase 1 already designed and committed** (`sql/analytics/` — see
Sections 9-11), rather than designing a new one from scratch;
deduplication and cleansing logic; genuine SCD Type 2 dimension handling
(Phase 1 chose Type 1 deliberately for `dim_url`/`dim_user` — see Section
10.2 — precisely because there's no reliable change-detection signal yet
without a real transform reading Bronze's history); automated data
quality and profiling checks, extending Section 12's contract validation
to the analytical layer (a named Phase 1 gap — Section 12.7); schema
evolution handling; late-arriving-data reprocessing (the gap named in
ADR-005); backfill tooling; small-file compaction; and tests for
transformation logic specifically (distinct from this phase's ingestion
tests). Phase 2 begins only when explicitly requested — consistent with
how this repository has been built so far, one reviewed increment at a
time.
