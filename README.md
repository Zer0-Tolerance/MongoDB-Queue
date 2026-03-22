# MongoDB::Queue

A fast, persistent, prioritised job queue for Raku backed by [MongoDB::Fast](https://github.com/fastmongo/raku-mongodb-fast).

- **Typed queues** — every job carries a `type`; workers consume only their own type
- **At-least-once delivery** via optimistic locking — only one worker claims each job
- **Priority scheduling** — higher `priority` value is always dequeued first
- **Delayed delivery** — hide a job for N seconds before it becomes visible
- **Automatic retries** — `nack` returns a job to the queue; permanent failure after `max_attempts`
- **Stale job recovery** — `reclaim-stale` re-queues jobs from crashed workers
- **Bulk operations** — `enqueue-many` / `dequeue-many` / `ack-many` for high throughput
- **Built-in event loop** — `listen` handles the poll/ack/nack cycle for you
- **Human-readable timestamps** — every epoch field has a paired `_dt` DateTime field in UTC

---

## Installation

```
zef install MongoDB::Queue
```

Requires a running MongoDB instance (tested with MongoDB 6+).

---

## Quick start

```raku
use MongoDB::Queue;

my $q = MongoDB::Queue.connect(host => 'localhost', db => 'myapp');

# Produce
$q.enqueue({ task => 'send-email', to => 'user@example.com' }, 'email');

# Consume
while my $job = $q.dequeue('email') {
    send-email($job<payload><to>);
    $q.ack($job<_id>);
    CATCH { default { $q.nack($job<_id>) } }
}
```

---

## Constructor

### `MongoDB::Queue.connect`

The easiest way to create a queue. Opens a `MongoDB::Fast` connection and returns a ready-to-use instance.

```raku
# By host + port
my $q = MongoDB::Queue.connect(
    host       => 'localhost',   # default
    port       => 27017,         # default
    db         => 'myapp',       # database name  (default: 'queues')
    collection => 'jobs',        # collection name (default: 'jobs')
);

# Or by URL  (host:port)
my $q = MongoDB::Queue.connect(url => 'mongo.internal:27017', db => 'myapp');

# With credentials
my $q = MongoDB::Queue.connect(
    host     => 'localhost',
    username => 'myuser',
    password => 's3cret',
    db       => 'myapp',
);

# Queue-wide defaults (all optional)
my $q = MongoDB::Queue.connect(
    host               => 'localhost',
    db                 => 'myapp',
    visibility-timeout => 30,   # seconds before a processing job is reclaimable
    max-attempts       => 3,    # permanent failure threshold
    poll-interval      => 1,    # seconds to sleep when queue is empty (listen)
    dequeue-batch      => 5,    # candidates fetched per dequeue call
);
```

### `MongoDB::Queue.new`

Pass your own `MongoDB::Fast` client when you need fine-grained control.

```raku
use MongoDB::Fast;

my $client = MongoDB::Fast.new(host => 'mongo.internal', port => 27017);
my $q = MongoDB::Queue.new(
    client          => $client,
    db-name         => 'myapp',
    collection-name => 'jobs',
);
```

---

## Producing jobs

### `enqueue`

Insert a single job. `type` is a required positional argument. Returns the job's `_id` string.

```raku
# Minimal
my $id = $q.enqueue({ task => 'send-email', to => 'user@example.com' }, 'email');

# With priority (higher = dequeued first; default 0)
my $id = $q.enqueue({ task => 'urgent-alert' }, 'email', priority => 10);

# With delayed delivery (invisible for 60 seconds)
my $id = $q.enqueue({ task => 'reminder' }, 'email', delay => 60);

# Override per-queue max-attempts for this job only
my $id = $q.enqueue({ task => 'flaky-api' }, 'webhook', max-attempts => 10);
```

### `enqueue-many`

Insert multiple jobs of the same type in a **single MongoDB round trip**. Returns an `Array` of `_id` strings in insertion order.

```raku
my @payloads = ({ task => 'resize', file => "img$_.jpg" } for ^100);
my @ids = $q.enqueue-many(@payloads, 'image', priority => 5);
say "Inserted {@ids.elems} jobs";
```

---

## Consuming jobs

### `dequeue`

Atomically claim the next available job of the given type. Returns the full job document as a `Hash`, or `Nil` when the queue is empty.

```raku
while my $job = $q.dequeue('email') {
    my %data = $job<payload>;
    do-work(%data);
    $q.ack($job<_id>);
    CATCH { default { $q.nack($job<_id>) } }
}
```

Pass `:worker-id` to label the claim (useful for debugging):

```raku
my $job = $q.dequeue('email', worker-id => 'worker-3');
```

### `dequeue-many`

Claim up to N jobs of the given type in one pass (one `find` + N `update-one` claims). Returns an `Array` of job documents.

```raku
while my @jobs = $q.dequeue-many(20, 'email') {
    process($_<payload>) for @jobs;
    $q.ack-many(@jobs.map(*<_id>));
}
```

### `ack`

Mark a job `done` after successful processing.

```raku
$q.ack($job<_id>);   # returns True on success
```

### `ack-many`

Acknowledge a batch in a single round trip. Returns the count of jobs marked `done`.

```raku
my $n = $q.ack-many(@jobs.map(*<_id>));
```

### `nack`

Return a job to the queue for retry. If the job has exhausted `max_attempts` it is permanently failed instead.

```raku
$q.nack($job<_id>);                # retry immediately
$q.nack($job<_id>, delay => 30);   # retry after 30 seconds
```

### `fail`

Permanently fail a job right now, bypassing remaining attempts. Only works on `processing` jobs.

```raku
$q.fail($job<_id>);   # returns True on success
```

---

## Event loop

### `listen`

Block forever, processing jobs as they arrive. `ack` is called automatically on success; `nack` is called if the callback throws.

```raku
$q.listen(
    -> $job { do-work($job<payload>) },
    type => 'email',
);
```

All options:

```raku
$q.listen(
    -> $job { do-work($job<payload>) },
    type          => 'email',    # job type to consume (required)
    worker-id     => 'worker-1', # label for this process
    poll-interval => 2,          # seconds to sleep when queue is empty
    reclaim-every => 60,         # how often to scan for stale jobs (seconds)
    batch         => 5,          # jobs claimed per poll cycle (default 1)
);
```

When `batch > 1`, `listen` uses `dequeue-many` internally and processes all claimed jobs before sleeping, maximising throughput under high load.

---

## Maintenance

### `reclaim-stale`

Re-queue (or permanently fail) `processing` jobs whose `locked_at` is older than the threshold. Returns the count of jobs reclaimed. Called automatically by `listen` every `reclaim-every` seconds.

```raku
my $n = $q.reclaim-stale;                    # uses visibility-timeout
my $n = $q.reclaim-stale(older-than => 120); # custom threshold
```

### `purge`

Delete all `done` and `failed` jobs. Returns the count deleted.

```raku
my $n = $q.purge;
```

### `clear`

Delete every job regardless of status. Returns the count deleted.

```raku
$q.clear;
```

### `drop`

Drop the entire collection.

```raku
$q.drop;
```

---

## Metrics

All metric methods require the job type:

```raku
say $q.size('email');       # pending jobs of that type
say $q.in-flight('email');  # currently processing
say $q.failed('email');     # permanently failed
say $q.total('email');      # all jobs of that type

my %s = $q.stats('email');
# { pending => 42, processing => 3, failed => 1, total => 46 }
```

---

## Job document schema

Every job stored in MongoDB has these fields:

| Field              | Type       | Notes                                               |
|--------------------|------------|-----------------------------------------------------|
| `_id`              | `Str`      | 32-char random hex                                  |
| `payload`          | `Hash`     | Arbitrary application data                          |
| `type`             | `Str`      | Job type — used to route jobs to the right worker   |
| `status`           | `Str`      | `pending` \| `processing` \| `done` \| `failed`    |
| `priority`         | `Int`      | Higher = dequeued first (default `0`)               |
| `available_at`     | `Int`      | Epoch seconds; gates delayed delivery               |
| `available_at_dt`  | `DateTime` | UTC datetime companion                              |
| `created_at`       | `Int`      | Epoch seconds                                       |
| `created_at_dt`    | `DateTime` | UTC datetime companion                              |
| `locked_at`        | `Int`      | Epoch seconds when claimed (`Any` if pending)       |
| `locked_at_dt`     | `DateTime` | UTC datetime companion (`Any` if pending)           |
| `locked_by`        | `Str`      | Worker identifier (`Any` if pending)                |
| `attempts`         | `Int`      | Incremented on every `dequeue`                      |
| `max_attempts`     | `Int`      | Permanent failure threshold                         |
| `done_at`          | `Int`      | Epoch seconds when acked/failed (`Any` otherwise)   |
| `done_at_dt`       | `DateTime` | UTC datetime companion (`Any` otherwise)            |

---

## Complete example — email worker

Runnable scripts live in [`examples/`](examples/).

**[examples/producer.raku](examples/producer.raku)**

```raku
use lib 'lib';
use MongoDB::Queue;

my $q = MongoDB::Queue.connect(host => 'localhost', db => 'myapp');

my @emails = (
    %( to => 'alice@example.com', subject => 'Hello'  ),
    %( to => 'bob@example.com',   subject => 'Hi'     ),
    %( to => 'ceo@example.com',   subject => 'Urgent' ),
);

# Send the CEO's email first (high priority)
$q.enqueue(@emails[2], 'email', priority => 100);

# Bulk-insert the rest in one round trip
my @ids = $q.enqueue-many(@emails[0..1], 'email');

say "Queued 1 high-priority + {@ids.elems} normal emails.";
say "Stats: {$q.stats('email').raku}";
```

**[examples/worker.raku](examples/worker.raku)**

```raku
use lib 'lib';
use MongoDB::Queue;

my $q = MongoDB::Queue.connect(
    host               => 'localhost',
    db                 => 'myapp',
    visibility-timeout => 60,
    max-attempts       => 5,
);

say "Worker {$*PID} started. Waiting for jobs...";

$q.listen(
    -> $job {
        my %email = $job<payload>;
        say "[attempt {$job<attempts>}] Sending email to %email<to> — %email<subject>";
        send-smtp(%email);
        say "  ✓ done ({$job<_id>})";
    },
    type          => 'email',
    worker-id     => "w-{$*PID}",
    poll-interval => 2,
    reclaim-every => 60,
    batch         => 5,
);

sub send-smtp(%email) { ... }
```

Run them in separate terminals (MongoDB must be running):

```
raku examples/producer.raku
raku examples/worker.raku
```

---

## How it works

**Atomic dequeue** uses an optimistic-lock pattern:

1. `find` a small batch of `pending` candidates of the requested `type`, sorted by `priority DESC`.
2. For each candidate, issue `update-one` with `{ _id => $id, status => 'pending', type => $type }` as the filter.
3. MongoDB's single-document atomicity ensures only one worker wins each document — the first to update sees `modifiedCount = 1`; all others see `0` and skip to the next candidate.
4. If every candidate is stolen by a concurrent worker the loop retries with a fresh `find`.

This avoids `findAndModify` (which has a known serialisation bug in MongoDB::Fast) while still guaranteeing at-most-one delivery per dequeue call.

---

## License

Artistic-2.0
