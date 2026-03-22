# Changelog

All notable changes to **MongoDB::Queue** are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
and the project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.1.0] — 2026-03-21

### Added

**Core queue operations**
- `enqueue(Hash $payload, :$priority, :$delay, :$max-attempts)` — insert a job;
  returns its `_id` string.
- `dequeue(:$worker-id)` — atomically claim the next visible, highest-priority
  job; returns the full job document or `Nil` when the queue is empty.
- `ack(Str $id)` — mark a job `done` after successful processing.
- `nack(Str $id, :$delay)` — return a job to the queue for retry; if
  `max_attempts` is exhausted the job is automatically moved to `failed`.
- `fail(Str $id)` — permanently fail a job, bypassing remaining attempts.

**Job scheduling**
- Priority delivery: higher `priority` value is always dequeued first.
- Delayed delivery: `:delay(N)` hides a job for *N* seconds before it becomes
  visible.
- Configurable `max-attempts` per job or as a queue-wide default.

**Maintenance**
- `reclaim-stale(:$older-than)` — re-queue (or fail) `processing` jobs whose
  `locked_at` is older than the threshold; used to recover from crashed workers.
  Called automatically by `listen` every `reclaim-every` seconds.
- `purge()` — delete all `done` and `failed` documents.
- `clear()` — delete every document regardless of status (testing/reset).
- `drop()` — drop the entire collection.
- `ensure-indexes()` — create the two compound indexes needed for efficient
  operation; idempotent and called automatically on construction.

**Metrics**
- `size()` — count of `pending` jobs.
- `in-flight()` — count of `processing` jobs.
- `failed()` — count of `failed` jobs.
- `total()` — count of all jobs in the collection.
- `stats()` — snapshot `Hash` with `pending`, `processing`, `failed`, `total`.

**Event loop**
- `listen(&callback, :$worker-id, :$poll-interval, :$reclaim-every)` — blocking
  loop that dequeues jobs and calls `&callback`; acks on success, nacks on
  exception, sleeps `poll-interval` seconds when the queue is empty.

**Factory constructor**
- `MongoDB::Queue.connect(:$host, :$port, :$username, :$password, :$db,
  :$collection, *%args)` — creates a `MongoDB::Fast` client and returns a
  ready-to-use queue in one call.

**Job document schema**

| Field         | Type | Notes                                          |
|---------------|------|------------------------------------------------|
| `_id`         | Str  | 32-char random hex                             |
| `payload`     | Hash | arbitrary application data                     |
| `status`      | Str  | `pending` \| `processing` \| `done` \| `failed` |
| `priority`    | Int  | higher = dequeued first (default `0`)          |
| `available_at`| Int  | epoch seconds; gates delayed delivery          |
| `created_at`  | Int  | epoch seconds                                  |
| `locked_at`   | Int  | epoch seconds when claimed (`Any` if pending)  |
| `locked_by`   | Str  | worker identifier (`Any` if pending)           |
| `attempts`    | Int  | incremented on every `dequeue`                 |
| `max_attempts`| Int  | permanent failure threshold                    |
| `done_at`     | Int  | epoch seconds when acked/failed (`Any` if not) |

### Implementation notes

- **Atomic dequeue** uses an optimistic-lock pattern: a small batch of
  candidates is retrieved sorted by `priority DESC`, then each is claimed with
  `update-one` that rechecks `status = 'pending'` — only one worker wins per
  document, eliminating duplicate delivery under concurrent load.
- The sort uses a **single-key** `{ priority => -1 }` specification to avoid
  Raku's unordered `Hash` serialisation producing a wrong MongoDB sort order
  when multiple sort keys are present (a limitation of `MongoDB::Fast`'s BSON
  encoder's `CMD-KEYWORDS` ordering heuristic).  `available_at` is enforced
  exclusively through the query filter, not the sort.
- Indexes are created with `try` so that conflicting index names left over from
  previous runs are silently ignored rather than crashing startup.

### Test suite

- `t/01-basic.t` — 10 subtests covering the fundamental enqueue → dequeue →
  ack/nack/fail lifecycle, priority ordering, delayed delivery, stale reclaim,
  stats, and purge.
- `t/02-battle.t` — 27 subtests covering payload edge cases (empty, nested,
  unicode, arrays, 50 KB blobs, integers/booleans), idempotency guards,
  priority correctness, timing-based delay and nack-with-delay, full retry
  exhaustion, stale reclaim of multiple workers (including exhausted-attempts
  auto-fail), concurrent producers (8 × 10 = 80 jobs, no lost inserts),
  concurrent consumers (6 workers, zero duplicate deliveries), stats
  consistency, cross-instance sharing of the same collection, throughput smoke
  test, and purge/clear semantics.
