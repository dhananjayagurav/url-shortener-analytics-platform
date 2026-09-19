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

**Data Modeling** ⏳ *(next increments)*
7. Analytics Requirements & Business Metrics
8. Data Grain
9. Source Data Model (formalized data contracts)
10. Analytical Data Model (fact/dimension design)
11. Star Schema vs. Normalized Model
12. Data Contracts

**Ingestion**
13. [Batch Ingestion Design](#13-batch-ingestion-design) ✅
14. [Full Load Ingestion](#14-full-load-ingestion-) ✅✅
15. Incremental Load & Watermarks ⏳
16. Checkpointing ⏳ *(the mechanism exists — see [Section 13.3](#133-checkpointing-and-watermark-mechanism-already-built) — this section will teach it in depth once incremental load makes checkpoint *recovery* observable)*
17. Idempotency ⏳ *(the mechanism exists — see [Section 14](#14-full-load-ingestion) — deep-dive lands with LAB 4/5)*

**Storage**
18. Object Storage Fundamentals ⏳
19. Parquet ⏳
20. Partitioning ⏳
21. File Layout ⏳
22. Ingestion Metadata (deep-dive) ⏳

**Quality & Operations**
23. PII and Security ⏳
24. Testing (deep-dive) ⏳ *(tests exist now — see [Section 14.6](#146-how-to-test)*)
25. Failure Scenarios (all 10) ⏳ *(one is demonstrated now — see [Section 14.7](#147-failure-scenario)*)
26. Performance ⏳
27. Scale Design ⏳

**Reference**
28. [Architectural Principles](#28-architectural-principles) ✅ *(introduced now, extended as more are demonstrated)*
29. [Architecture Decision Records](#29-architecture-decision-records) ✅
30. Hands-on Labs (index) ⏳ *(LAB 1 exists — see [Section 14.5](#145-hands-on-exercise)*)
31. Interview Questions (consolidated, all categories) ⏳ *(Category C questions exist now — see [Section 14.9](#149-principal-engineer-interview-questions)*)
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
   and an `id`-based watermark, per [ADR-005](#adr-005-watermark-based-incremental-ingestion-planned-not-yet-implemented)).

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
| `benchmarks/` | Reserved for executable performance benchmarks. |
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
| This repo, Phase 1 | ✅ Implemented — [Section 14](#14-full-load-ingestion) | ⏳ Planned — Section 15 | Not planned before Phase 5 (post-Phase-4, see repository-level plan) |

### 13.3 Checkpointing and watermark mechanism (already built)

Full load doesn't strictly need a watermark (it always reads everything),
but this repo's `ingestion_metadata` table and
`ingestion/src/url_shortener_analytics/metadata.py` module were built to
support **both** full and incremental load from day one — see
[Section 14.2](#142-architecture) for why, and
[`sql/source/003_ingestion_metadata.sql`](../sql/source/003_ingestion_metadata.sql)
for the schema. This is a deliberate case of building the metadata layer
once, correctly, rather than retrofitting it when incremental load
(Section 15) needs it.

---

## 14. Full Load Ingestion ✅✅

### 14.1 Concept

A full load re-extracts an entire source table on every run and writes the
whole result as one snapshot — no matter what changed since the last run.
It's the simplest ingestion pattern that can possibly work: the query is
`SELECT * FROM table`.

### Why does this exist?

A full load is correct by construction (no watermark to get wrong, no row
that can be silently missed) and is the fallback every more sophisticated
pipeline eventually needs — when an incremental pipeline's state gets
corrupted, the standard fix is "run a full load and rebuild from there."
Small, slowly-changing tables (`users` here, at 200 rows) can legitimately
stay on full load forever; re-extracting 200 rows every run costs nothing.

### Simple Example

Imagine backing up your entire phone's photo library to a hard drive every
night, regardless of whether you took zero new photos or five hundred. The
backup script doesn't try to figure out "what's new" — it just copies
everything, every time. It's slow and wasteful once your library is huge,
but it's also the version of "backup my photos" that's almost impossible
to get wrong: there's no bookkeeping about what was already backed up, so
there's no bookkeeping to get out of sync with reality.

### Design Decision

Phase 1 uses full load for **all three** tables (`urls`, `users`,
`clicks`), even though `clicks` is the one table that will eventually need
to move to incremental load (Section 15, next). This is deliberate
sequencing, not an oversight: implementing full load first, cleanly, for
every table, gives every table a working baseline and gives the
checkpoint/metadata layer (`metadata.py`) real, immediate use — before
incremental load's extra complexity (watermarks, "what changed since
last time") gets layered on top of it.

### Alternatives

1. **Skip full load, implement incremental load directly.** Possible, but
   it means the *first* piece of ingestion code you write also has to get
   watermark logic right, with no simpler baseline to fall back to if
   something's wrong. Harder to debug, and skips the natural "build the
   simple thing, prove it, then add complexity" progression this whole
   project follows.
2. **Full load for everything, forever (rejected for `clicks`).** Simple,
   but doesn't scale — Section 15 explains exactly where this breaks down
   for a table that grows without bound.
3. **Full load now, incremental later, table by table (chosen).** Lets
   each table's load strategy match its actual growth pattern, and lets
   this guide teach both patterns clearly, one at a time, instead of
   conflating them.

### Trade-offs

| | Full load | Incremental load (Section 15) |
|---|---|---|
| Correctness | Simple — no watermark to get wrong | More moving parts — a wrong watermark can silently skip or duplicate rows |
| Cost as table grows | Grows with total table size, forever | Roughly constant — grows with new rows since last run, not total size |
| Right for | Small, slowly-changing tables (`users`, `urls` here) | Large, append-heavy tables (`clicks`, eventually) |

### 14.2 Architecture

```mermaid
sequenceDiagram
    participant CLI as cli.py
    participant MD as metadata.py
    participant EX as extract_full.py
    participant PG as Postgres
    participant OS as object_store.py
    participant S3 as MinIO

    CLI->>MD: start_run(pipeline, table, "full")
    MD-->>CLI: run_id (status=running)
    CLI->>EX: run_full_load(...)
    EX->>PG: extract_full() -- pd.read_sql_table
    PG-->>EX: DataFrame
    EX->>OS: write_bronze(df, table, run_date)
    OS->>S3: put_object (deterministic key)
    S3-->>OS: 200 OK
    OS-->>EX: bronze key
    EX->>MD: finish_run_success(run_id, rows, ...)
```

On any failure between `start_run` and `finish_run_success`,
`extract_full.run_full_load` catches the exception, calls
`finish_run_failure` (so the run is never left `running` forever), and
re-raises — see [Section 14.7](#147-failure-scenario).

### 14.3 URL Shortener example

This pipeline runs the same full-load logic against all three configured
tables (`ingestion/configs/pipelines.yaml`): `urls` (real schema), `users`
and `clicks` (hypothetical, per Section 1.5). Each table gets its own
`ingestion_metadata` row per run and its own Bronze object.

### 14.4 Implementation

This component spans five files. Two are taught here in full depth
(they're the ones with real design decisions in them); the other three get
a shorter treatment here and their own deep-dive later, where their
purpose becomes fully visible (`metadata.py` in Section 16 — Checkpointing;
`cli.py`'s command surface grows in Section 15).

**Implementation Guide vs. Reference Implementation, for this component:**
if you want the hands-on version, read the *Implementation Guide*
paragraph under each file below, close this guide, and write the function
yourself against the same signature — then compare against the *Reference
Implementation* code block. If you want to move faster and study the
finished code instead, copy the reference code directly into the named
path; it's exactly what's already committed in this repository.

---

**CREATE:** `ingestion/src/url_shortener_analytics/extract_full.py`

**PURPOSE:** Read an entire source table into memory and hand it off to be
written to Bronze, while recording a checkpoint before and after.

**DEPENDENCIES:** `pandas`, a SQLAlchemy `Engine` (from `db.py`), this
package's `metadata` module (for checkpointing) and `object_store` module
(for the actual write).

**IMPLEMENTATION GUIDE (write it yourself):** you need two functions.
The first, `extract_full(table_name, engine)`, should do exactly one
thing: run `SELECT * FROM <table_name>` and return the result as a
DataFrame — pandas' `pd.read_sql_table` does this in one call. Wrap it in
a `try/except` that catches whatever the database driver raises and
re-raises your own `ExtractionError` (see `exceptions.py`) with a message
naming the table — this is what makes failures debuggable without needing
to know pandas' or psycopg's specific exception types. The second
function, `run_full_load(engine, s3_client, bucket, pipeline_name,
table_name)`, is the orchestration: call `metadata.start_run(...)` first
to get a `run_id`, then call your `extract_full`, then call
`object_store.write_bronze(...)` to actually write it, then call
`metadata.finish_run_success(...)`. Wrap the extract-and-write portion in
a `try/except Exception` that calls `metadata.finish_run_failure(...)`
and **re-raises** — the caller (eventually the CLI) needs to know this
table failed, and the checkpoint must never be left silently `running`.

**REFERENCE IMPLEMENTATION:**

```python
# ingestion/src/url_shortener_analytics/extract_full.py  (excerpt — full file
# is already committed at this path)

def extract_full(table_name: str, engine: Engine) -> pd.DataFrame:
    """POC SIMPLIFICATION: reads the whole table in one query. Production
    equivalent: chunked/paged extraction with bounded memory."""
    try:
        df = pd.read_sql_table(table_name, engine)
    except Exception as err:
        raise ExtractionError(f"failed to extract table '{table_name}'") from err
    return df


def run_full_load(engine, s3_client, bucket, pipeline_name, table_name) -> dict:
    run_id = metadata.start_run(engine, pipeline_name, table_name, load_type="full")
    try:
        df = extract_full(table_name, engine)
        key = write_bronze(df, table_name, datetime.now(UTC), s3_client, bucket)
        metadata.finish_run_success(engine, run_id, rows_read=len(df), rows_written=len(df))
    except Exception as err:
        metadata.finish_run_failure(engine, run_id, str(err))
        raise
    return {"run_id": run_id, "table": table_name, "rows": len(df), "key": key}
```

Full file, with imports, logging and docstrings:
[`ingestion/src/url_shortener_analytics/extract_full.py`](../ingestion/src/url_shortener_analytics/extract_full.py).

**RUN:** `make ingest-full` (runs it for every table in `pipelines.yaml`)

**VERIFY:** `docker compose exec postgres psql -U analytics -d analytics -c "SELECT source_table, status, rows_written FROM ingestion_metadata ORDER BY started_at DESC LIMIT 5;"`

**EXPECTED:** three rows, one per table, each `status = success` with
`rows_written` matching what `make seed` inserted (500 / 200 / 5000 by
default). *(DESIGN EXPECTATION — run it yourself for the ACTUAL OBSERVED
numbers on your machine.)*

**TEST:** `ingestion/tests/unit/test_extract_full.py` —
`make test` runs it (part of the 15 unit tests, all currently passing).

**PRODUCTION CONSIDERATIONS:** see the table in Section 14.8 below.

**INTERVIEW QUESTIONS:** see Section 14.9 below (both questions there are
specifically about this file).

---

**CREATE:** `ingestion/src/url_shortener_analytics/object_store.py`

**PURPOSE:** Serialize a DataFrame to Parquet and write it to a
deterministic, idempotent key in MinIO/S3. This file is where the
idempotency guarantee this whole section leans on actually lives.

**DEPENDENCIES:** `boto3` (the AWS/S3 SDK — MinIO speaks the same API),
`pyarrow` (Parquet read/write).

**IMPLEMENTATION GUIDE (write it yourself):** start with the function that
matters most conceptually: `build_bronze_key(table_name, run_date) ->
str`. It should return a string of the shape
`bronze/{table}/ingestion_date={date}/{table}.parquet` — using only the
*calendar date* portion of `run_date`, not the time. Stop and think about
*why* before moving on: if you included the exact timestamp instead of
just the date, what would break? (Answer, once you've thought about it:
every rerun on the same day would produce a *different* key, so reruns
would pile up as duplicate objects instead of overwriting — which is
exactly the idempotency property Section 14's Hands-on Exercise proves.)
Then write `write_bronze(df, table_name, run_date, s3_client, bucket)`:
serialize `df` to Parquet bytes in memory (`pyarrow.Table.from_pandas`
then `pyarrow.parquet.write_table` into an `io.BytesIO()` buffer), call
`s3_client.put_object(Bucket=..., Key=build_bronze_key(...), Body=...)`,
and return the key. Wrap the `put_object` call in a small retry loop
(2-3 attempts, short backoff) — S3-compatible APIs do occasionally return
transient errors, and because the key is deterministic, retrying a write
is always safe.

**REFERENCE IMPLEMENTATION:**

```python
# ingestion/src/url_shortener_analytics/object_store.py (excerpt)

def build_bronze_key(table_name: str, run_date: datetime) -> str:
    return f"bronze/{table_name}/ingestion_date={run_date:%Y-%m-%d}/{table_name}.parquet"


def write_bronze(df, table_name, run_date, s3_client, bucket, *, max_attempts=3, backoff_seconds=1.0) -> str:
    key = build_bronze_key(table_name, run_date)
    body = _dataframe_to_parquet_bytes(df)
    for attempt in range(1, max_attempts + 1):
        try:
            s3_client.put_object(Bucket=bucket, Key=key, Body=body)
            return key
        except (ClientError, BotoCoreError) as err:
            if attempt == max_attempts:
                raise ObjectStoreWriteError(f"failed after {max_attempts} attempts") from err
            time.sleep(backoff_seconds * attempt)
```

Full file: [`ingestion/src/url_shortener_analytics/object_store.py`](../ingestion/src/url_shortener_analytics/object_store.py).

**RUN:** exercised indirectly via `make ingest-full` — there's no
standalone CLI for this file alone (by design; it's a library module, not
an entrypoint).

**VERIFY:** open `http://localhost:9001` (MinIO console), browse to
`analytics-lake/bronze/urls/`, confirm one `.parquet` object exists per
`ingestion_date=` prefix.

**EXPECTED:** one object per table per calendar day, regardless of how
many times you've run `make ingest-full` today.

**TEST:** `ingestion/tests/unit/test_object_store.py` — 5 tests, covering
key determinism, the actual `put_object` call shape, and both the retry
and give-up-and-raise paths (using a mocked S3 client with a scripted
`side_effect`, not a real network call).

**PRODUCTION CONSIDERATIONS:** see Section 14.8.

**INTERVIEW QUESTIONS:** the first question in Section 14.9 is about this
exact file.

---

**Supporting files** (shorter treatment — full teaching lands with their
own sections):

| File | Purpose | Deep-dive lands in |
|---|---|---|
| [`metadata.py`](../ingestion/src/url_shortener_analytics/metadata.py) | Watermark / checkpoint / run history reads and writes | Section 16 (Checkpointing) |
| [`config.py`](../ingestion/src/url_shortener_analytics/config.py) | Environment-variable-driven settings (`pydantic-settings`) | Referenced throughout; no dedicated section — it's a standard pattern, not a novel concept |
| [`db.py`](../ingestion/src/url_shortener_analytics/db.py) | Builds and caches the SQLAlchemy `Engine` | Same as above |
| [`cli.py`](../ingestion/src/url_shortener_analytics/cli.py) | `python -m url_shortener_analytics.cli full-load` entrypoint; wires config → engine → S3 client → `run_full_load` per table | Grows a second subcommand in Section 15 |
| [`pipelines.yaml`](../ingestion/configs/pipelines.yaml) | Which tables, which load type — config, not code | [Architectural Principle #8](#28-architectural-principles), Metadata-driven processing |

### Hands-on Challenge (implement-yourself)

Before reading LAB 1 below, try this: **without looking at
`object_store.py`, write your own version of `build_bronze_key` that
partitions by *hour* instead of by day** (`ingestion_hour=2026-09-19-14`
instead of `ingestion_date=2026-09-19`). Then answer, in your own words:
what would change about LAB 4/5's idempotency proof below if you made this
change and ran `make ingest-full` twice within the same hour versus twice
across an hour boundary? (You don't need to actually wire your version
into the pipeline — this is a design-reasoning exercise. The real answer:
idempotency would still hold *within* an hour, but a full load that
happens to straddle an hour boundary would now produce two Bronze objects
for what's conceptually "one day" of data — a preview of exactly the
partition-granularity trade-off Section 20, Partitioning, covers in full.)

### 14.5 Hands-on Exercise

**LAB 1 — Run a full load.**

Prerequisites: `make up` has been run and `docker compose ps` shows
`postgres` and `minio` healthy; `make seed` has been run at least once.

```bash
make ingest-full
```

Expected output (key=value structured log lines — see
`ingestion/src/url_shortener_analytics/logging_setup.py`):

```
ts=... level=INFO logger=url_shortener_analytics.cli msg="starting full load" pipeline='url_shortener_bronze_ingestion' tables=['urls', 'users', 'clicks']
ts=... level=INFO logger=url_shortener_analytics.extract_full msg="extracting table (full load)" table='urls'
ts=... level=INFO logger=url_shortener_analytics.extract_full msg="extraction complete" table='urls' rows=500 columns=8
ts=... level=INFO logger=url_shortener_analytics.object_store msg="wrote bronze object" key='bronze/urls/ingestion_date=2026-09-19/urls.parquet' bytes=... rows=500 attempt=1
...
ts=... level=INFO logger=url_shortener_analytics.cli msg="full load finished successfully"
```

*(Exact row counts and byte sizes are a DESIGN EXPECTATION based on
`scripts/seed_sample_data.py`'s fixed seed — run the command yourself to
see the ACTUAL OBSERVED values on your machine; nothing above was
fabricated as a claimed real run.)*

Inspect what landed in MinIO: open `http://localhost:9001`, browse to the
`analytics-lake` bucket, and confirm `bronze/urls/`, `bronze/users/`,
`bronze/clicks/` each contain one `.parquet` object.

**LAB 4/5 (compressed into one exercise here — full depth lands in the
dedicated Idempotency section) — prove reruns are safe.**

```bash
make ingest-full
make ingest-full   # run it again immediately
```

What to observe: both runs report success; the object at
`bronze/urls/ingestion_date=<today>/urls.parquet` is overwritten, not
duplicated (confirmed by `ingestion/tests/integration/test_full_load_integration.py::test_rerunning_full_load_overwrites_not_duplicates`,
which asserts `KeyCount == 1` after two runs). Why this matters: retries
after a crash are the normal recovery path for any batch pipeline — if
reruns produced duplicates, every crash-and-retry would corrupt downstream
counts.

### 14.6 How to test

```bash
make test                # unit tests: SQLite + mocked S3, no Docker needed. Currently: 15 passed.
make up
make test-integration     # real Postgres + MinIO
```

The 15 unit tests above were run in this environment while writing this
guide (Python 3.11, `pytest -q`) and genuinely passed — this is an ACTUAL
OBSERVED result, not a projection:

```
15 passed in 3.72s
```

### 14.7 Failure Scenario

**What happens if the process is killed between a successful `write_bronze`
call and the `finish_run_success` checkpoint update?**

The Bronze object for that run now exists in MinIO, but `ingestion_metadata`
still shows `status = 'running'` for that run — not `success`, and not
`failed` either, because nothing ever got the chance to update it.
`get_last_watermark` only reads `status = 'success'` rows (see
`metadata.py`), so a stuck `running` row is simply ignored by future
watermark reads — it doesn't corrupt anything for full load specifically
(there's no watermark to protect here), but it does mean the run's own
history is permanently ambiguous: did it actually finish? The honest
answer, visible from the data alone, is "we don't know — the process died
before it could tell us." **Production implication:** an orchestrator (not
yet part of Phase 1) should alert on any `ingestion_metadata` row that's
been `running` for longer than the pipeline's expected max runtime, and
treat it as a crash requiring investigation, not as still-in-progress.

### 14.8 Production Considerations

| Aspect | This repo (POC) | Production |
|---|---|---|
| Extraction | Whole table, one query, one DataFrame (`pd.read_sql_table`) | Chunked/paged extraction with bounded memory — see the docstring in `extract_full.py` |
| Credentials | Static MinIO access/secret keys from `.env` | Short-lived credentials from an IAM role / workload identity |
| Retry | 3 attempts, linear backoff, in-process (`object_store.write_bronze`) | Same idea, plus orchestrator-level retry/alerting across whole run failures |
| Crash recovery | Manual rerun (safe, because writes are idempotent) | Orchestrator-driven automatic retry with backoff and paging on repeated failure |
| OLTP instance | This repo's own standalone Postgres mirror (ADR-009) | Reads from the real application's read replica, never the primary |

### Principal Data Engineer Perspective

The interesting judgment call in this component isn't the happy path —
it's what a principal engineer would flag in review about the *unhappy*
path. Two things stand out here. First: `run_full_load` catches
*every* exception during extract-and-write, records it, and re-raises —
which means one table's `ExtractionError` doesn't corrupt another table's
run (see `cli.py`'s loop, which continues to the next table after logging
a failure), but it also means a systemic problem (e.g. the OLTP database
itself being down) gets logged three separate times, once per table,
instead of failing fast after the first failure. That's a real, debatable
trade-off — "fail isolated" versus "fail fast" — worth being able to
articulate rather than presenting as an obviously-correct default. Second:
the retry logic in `write_bronze` retries *blindly*, without checking
*why* the write failed — appropriate here because every retry is
idempotent, but it's exactly the kind of blind retry that becomes
dangerous the moment an operation *isn't* idempotent, which is precisely
why Section 19 (Idempotency) treats this property as a prerequisite for
safe retries, not an independent nice-to-have.

### 14.9 Principal Engineer Interview Questions

**Q: "Walk through exactly what makes `write_bronze` safe to call twice for
the same table on the same day."**

*What's tested:* whether the candidate can explain idempotency as a
concrete mechanism, not just define the word.

*What a weak answer looks like:* "It's idempotent because we designed it
to be" — true but circular; doesn't explain the mechanism.

*What a strong answer covers:* the S3 key returned by `build_bronze_key`
depends only on `table_name` and the calendar date, not on a run id or
exact timestamp — so two calls for the same table on the same day compute
the identical key. `s3_client.put_object` on both S3 and MinIO fully
replaces whatever object previously existed at that key; it's not an
append and not a partial write. So the second call's `PUT` simply
overwrites the first call's bytes with (in this case, identical) new
bytes — the *object storage system's own atomic-overwrite behavior* is
what `write_bronze` leans on, not any locking or deduplication logic built
into this codebase.

*Concepts:* idempotent-by-overwrite vs. idempotent-by-dedup, atomic PUT
semantics in object storage.

*Expected follow-up:* "What would break this guarantee?" — Including a
random UUID or a sub-day timestamp in the key; then every rerun would
produce a new object instead of overwriting.

*Common mistake:* describing this as "we check if the file exists first
and skip if it does" — that's not what happens, and that approach would
be wrong anyway (it would prevent legitimately re-extracting a table
whose data changed since the last run today).

**Q: "This pipeline currently reads `pd.read_sql_table` — the whole table,
every run. At what point does that become a real production problem, and
what's the first thing that actually breaks?"**

*What's tested:* whether the candidate can name a concrete failure mode
instead of a vague "it won't scale."

*What a weak answer looks like:* "At some point there'll be too many rows
and it'll be slow" — not wrong, but doesn't identify a mechanism, and
would prompt an interviewer to keep digging.

*What a strong answer covers:* the practical limit isn't a specific row
count in the abstract — it's whatever this process's available memory can
hold as both the raw query result set and the in-memory pandas DataFrame
simultaneously (pandas typically uses several times the raw on-disk size
once you account for Python object overhead on non-numeric columns).
Before that, though, the `SELECT *` itself holds a long-running read
against the OLTP database — on a real production `clicks` table, that
scan competing with live write traffic is often the first practical
problem, ahead of the client process actually running out of memory. The
fix precedes the memory limit: chunked, bounded extraction (see the
POC-simplification note in `extract_full.py`'s docstring).

*Concepts:* memory-bounded processing, long-running scans vs. OLTP write
concurrency.

*Expected follow-up:* "Why not just add `.limit()` in a loop?" — That's
exactly chunked extraction; the follow-up worth raising unprompted is how
to keep each chunk's boundary stable while the table is being concurrently
written to, which is precisely the watermark problem Section 15 exists to
solve properly for the incremental case.

*Common mistake:* answering purely in terms of "at N million rows it gets
slow" without naming *what* becomes slow or fails first.

---

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

### ADR-003: Use Parquet for Bronze storage

**Context:** Extracted tabular data needs a file format. **Decision:**
Apache Parquet, via PyArrow, snappy-compressed. **Alternatives
considered:** CSV, JSON-lines. **Trade-offs:** Parquet is columnar
(efficient for analytical, column-selective reads) and self-describing
(embeds its own schema) but isn't human-readable by opening the raw file,
unlike CSV. **Consequences:** every consumer of Bronze data needs a
Parquet-aware reader (trivial — `pandas`, `pyarrow`, every real analytical
engine supports it natively); a hands-on CSV vs. Parquet benchmark is
planned (Section 19) to make this comparison concrete rather than
asserted.

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

### ADR-005: Watermark-based incremental ingestion *(planned, not yet implemented)*

Recorded now so the decision and its context aren't lost between this
increment and the one that implements it. **Context:** `clicks` will grow
without bound; full-reading it every run doesn't scale. **Decision (planned):**
an `id`-based (not timestamp-based) watermark, for the same clock-skew and
duplicate-timestamp reasons documented in this project's earlier notebook
prototype. **Alternatives considered:** timestamp-based watermark; full
CDC. **Trade-offs:** id-based watermarking is immune to clock skew but
tracks insertion order, not event order — late-arriving-data handling is
deferred. **Consequences:** to be implemented in Section 15.

### ADR-006: Dimensional analytical model *(planned, not yet implemented)*

**Context:** Bronze data is a direct mirror of OLTP structure — not shaped
for analytical queries. **Decision (planned):** a star schema
(fact/dimension) analytical layer. Recorded now; implemented in the
planned Section 10.

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

---

## 33. Phase 1 Summary (so far)

**What we've built in this increment:** the repository skeleton; the real
(and clearly-labeled hypothetical) source schema, documented and
mirrored locally; a full-load batch ingestion pipeline
(`extract_full.py`, `object_store.py`, `metadata.py`, `cli.py`) that is
idempotent, checkpointed, retried, and covered by 15 passing unit tests
plus integration tests runnable against real infrastructure; this guide.

**A note on this increment specifically:** this pass didn't add new code —
it took the sections already built (1, 2, 14) and rewrote them to the full
teaching template (Concept → Why → Simple Example → URL Shortener Example
→ Architecture → Design Decision → Alternatives → Trade-offs →
Implementation → Code → How to Run → How to Verify → Hands-on Exercise →
Failure Scenario → Production Considerations → Principal Perspective →
Interview Questions), matching the depth of this project's original
notebook-based prototype. Sections marked ✅ (not ✅✅) in the table of
contents still need this same upgrade pass — they're accurate, just not
yet at full depth. Every future new section is written at this depth from
the start.

**Concepts taught so far, at full depth:** the real application's
architecture and schema (with an explicit gaps/assumptions/recommended-
changes breakdown), OLTP vs. OLAP (with a generic pre-URL-Shortener
example, design decision, alternatives and trade-offs), full load
ingestion (with the exact CREATE/PURPOSE/IMPLEMENTATION/RUN/VERIFY/TEST
format for its two core files, plus an implement-yourself hands-on
challenge). Batch ingestion design, checkpointing's existence, and
idempotency's existence are introduced but still await their own full-depth
passes (Sections 13, 16, 19).

**Known limitations, stated honestly:** no incremental load yet (`clicks`
still does a full read every run); no scheduler (runs are manual);
`ingestion_metadata` has no automated stale-`running`-row alerting; no
formal data contracts yet; no PII classification section yet, though the
hypothetical `clicks.hashed_ip` design already avoids storing raw IPs; no
benchmarks have been run yet (Parquet/partitioning claims in this guide so
far are conceptual, not benchmark-backed — that's explicitly what Sections
19-20 and `benchmarks/` are for); most sections in the table of contents
still need the full-depth pass this increment applied to Sections 1, 2 and
14.

**Immediate next increment:** either incremental load + watermarks for
`clicks` at full depth (Section 15 — the single most important remaining
Phase 1 topic per the original curriculum), or the analytics requirements
/ data modeling sections (7-12) — whichever the reader wants to tackle
next.

---

## 34. Phase 1 Completion Checklist

| Item | Status | Evidence | What remains |
|---|---|---|---|
| Existing application understood | ✅ Done | Section 1 | — |
| OLTP vs OLAP understood | ✅ Done | Section 2 | — |
| Analytics requirements defined | ⏳ Not started | — | Section 7 |
| Metrics defined | ⏳ Not started | — | Section 7 |
| Grain defined | ⏳ Not started | — | Section 8 |
| Analytical model designed | ⏳ Not started | — | Sections 9-11 |
| Star schema implemented | ⏳ Not started | — | Section 11, `sql/analytics/` |
| Data contracts defined | ⏳ Not started | — | Section 12 |
| Full ingestion implemented | ✅ Done | `extract_full.py`, LAB 1 | — |
| Incremental ingestion implemented | ⏳ Not started | — | Section 15 |
| Watermark implemented | ⏳ Not started (mechanism exists, unused by full load) | `metadata.get_last_watermark` | Wire into an incremental extractor, Section 15 |
| Checkpoint implemented | ✅ Done | `metadata.py`, `ingestion_metadata` table | Deep-dive section (16) still to write |
| Idempotency implemented | ✅ Done | `object_store.build_bronze_key`, integration test | Deep-dive section (17), LAB 6-8 |
| MinIO configured | ✅ Done | `docker-compose.yml`, `object_store.py` | — |
| Parquet implemented | ✅ Done | `object_store.write_bronze` | Benchmark vs CSV/JSON not yet run (Section 19) |
| Partitioning implemented | ⏳ Not started (only date-scoped keys, not true multi-file partitioning) | — | Section 20 |
| PII identified | ⏳ Not started | `clicks.hashed_ip` already avoids raw IPs by construction | Formal classification table, Section 23 |
| Tests implemented | ✅ Done (unit) | 15 passing unit tests, `ingestion/tests/unit/` | Integration tests written but not yet run against live Docker in this environment (no Docker daemon available here — user should run `make test-integration` locally) |
| Failure scenarios tested | ✅ Partial | Section 14.7 (1 of 10) | Remaining 9, Section 25 |
| Performance benchmark completed | ⏳ Not started | — | Section 26, `benchmarks/` |
| Architecture diagrams completed | ✅ Partial | 5 diagrams so far | More land with later sections (star schema, data lifecycle, failure/recovery, final architecture) |
| ADRs documented | ✅ 9 of 8+ planned | Section 29 | ADR-005/006 recorded as planned, not yet implemented |
| Interview questions reviewed | ✅ Partial | Section 14.9 (Category C) | Remaining categories, Section 31 |
| Hands-on labs completed | ✅ Partial | LAB 1, LAB 4/5 (compressed) | LAB 2-3, 6-12 |
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
Bronze → Silver → Gold refinement pipeline; deduplication and cleansing
logic; SCD Type 1 and Type 2 dimension handling; automated data quality
and profiling checks; schema evolution handling; late-arriving-data
reprocessing (the gap named in ADR-005); backfill tooling; small-file
compaction; and tests for transformation logic specifically (distinct from
this phase's ingestion tests). Phase 2 begins only when explicitly
requested — consistent with how this repository has been built so far,
one reviewed increment at a time.
