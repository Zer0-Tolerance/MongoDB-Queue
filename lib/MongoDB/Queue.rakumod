use v6.d;

unit class MongoDB::Queue;

use MongoDB::Fast;

=begin pod

=head1 NAME

MongoDB::Queue - Fast persistent queueing system backed by MongoDB::Fast

=head1 SYNOPSIS

  use MongoDB::Queue;

  my $q = MongoDB::Queue.connect(host => 'localhost', db => 'myapp');

  # Single-job producer
  my $id = $q.enqueue({ task => 'send-email', to => 'user@example.com' });
  my $id = $q.enqueue({ task => 'resize-image' }, priority => 10, delay => 5);

  # Bulk producer — one MongoDB round trip for N jobs
  my @ids = $q.enqueue-many(@payloads, priority => 5);

  # Single-job consumer
  while my $job = $q.dequeue {
    process($job<payload>);
    $q.ack($job<_id>);
    CATCH { default { $q.nack($job<_id>) } }
  }

  # Bulk consumer — one find + N claims, then one round trip to ack all
  while my @jobs = $q.dequeue-many(20) {
    process($_<payload>) for @jobs;
    $q.ack-many(@jobs.map(*<_id>));
  }

  # Or use the built-in event loop
  $q.listen(-> $job { process($job<payload>) });

=head1 DESCRIPTION

Each job document stored in MongoDB has the following fields:

  _id      Str    — unique job ID (random hex)
  payload    Hash   — arbitrary job data supplied by caller
  status     Str    — pending | processing | done | failed
  priority     Int    — higher value = dequeued first (default 0)
  available_at   Int    — epoch seconds; supports delayed delivery
  available_at_dt DateTime — UTC datetime, e.g. 2025-01-01T12:00:00Z
  created_at   Int    — epoch seconds
  created_at_dt  DateTime — UTC datetime
  locked_at    Int    — epoch seconds when last claimed (Nil if pending)
  locked_at_dt   DateTime — UTC datetime (Nil if pending)
  locked_by    Str    — worker ID that holds the lock (Nil if pending)
  attempts     Int    — how many times this job has been dequeued
  max_attempts   Int    — fail permanently after this many attempts
  done_at    Int    — epoch seconds when acked/failed (Nil otherwise)
  done_at_dt   DateTime — UTC datetime (Nil otherwise)

=end pod

#| MongoDB::Fast client (required)
has MongoDB::Fast $.client is required;

#| Database name (default: 'queues')
has Str $.db-name = 'queues';

#| Collection name (default: 'jobs')
has Str $.collection-name = 'jobs';

#| Seconds before a processing job is considered stale and reclaimable
has Int $.visibility-timeout = 30;

#| Maximum delivery attempts before a job is permanently failed
has Int $.max-attempts = 3;

#| Seconds between polls when the queue is empty (used by listen)
has Int $.poll-interval = 1;

#| Number of candidate jobs fetched per single C<dequeue> call.
#| Larger values tolerate more concurrency; smaller values read fewer docs.
has Int $.dequeue-batch = 5;

has MongoDB::Fast::Database   $!db;
has MongoDB::Fast::Collection $!col;

submethod TWEAK {
  $!db  = $!client.db($!db-name);
  $!col = $!db.collection($!collection-name);
  self.ensure-indexes;
}

# ── Factory ──────────────────────────────────────────────────────────────────

#| Convenience constructor — creates a MongoDB::Fast client and returns a
#| ready-to-use MongoDB::Queue instance.
#|
#|   my $q = MongoDB::Queue.connect(host => 'localhost', db => 'myapp');
method connect(
  Str  :$host     = 'localhost',
  Str  :$url     = 'localhost:27017',
  Int  :$port     = 27017,
  Str  :$username,
  Str  :$password,
  Str  :$db     = 'queues',
  Str  :$collection = 'jobs',
     *%args
--> MongoDB::Queue) {
  my %opts;
  %opts = :$host, :$port if $host && $port;
  %opts = :host($url.split(':')[0]), :port($url.split(':')[1].Int) if $url;
  %opts<username> = $_ with $username;
  %opts<password> = $_ with $password;
  my $client = MongoDB::Fast.new(|%opts);
  self.new(:$client, db-name => $db, collection-name => $collection, |%args)
}

# ── Indexes ───────────────────────────────────────────────────────────────────

#| Create the indexes needed for efficient operation.  Called automatically
#| on construction; safe to call again (MongoDB ignores duplicate index
#| creation requests with the same key pattern).
method ensure-indexes(--> Bool) {
  # Primary dequeue index: filter by status + availability, sort by priority.
  # Silently ignore "already exists" conflicts (e.g. from a previous run).
  try await $!col.create-index(
    { status => 1, type => 1, priority => -1 },
    options => { name => 'queue_dequeue_idx' },
  );
  try await $!col.create-index(
    { status => 1, type => 1, locked_at => 1 },
    options => { name => 'queue_stale_idx' },
  );
  True
}

# ── Producer ─────────────────────────────────────────────────────────────────

#| Add a job to the queue.  Returns the job's C<_id> string.
#|
#| Named options:
#|   :priority(10)   — higher wins (default 0)
#|   :delay(30)    — seconds before the job becomes visible (default 0)
#|   :max-attempts(5)— override the per-queue default
method enqueue(
  Hash  $payload,
  Str  $type,
  Int  :$priority   = 0,
  Int  :$delay    = 0,
  Int  :$max-attempts = $.max-attempts,
  --> Str) {
  my Str $id  = self!gen-id;
  my Int $now   = now.to-posix[0].Int;
  my Int $avail = $now + $delay;
  await $!col.insert-one({
  _id      => $id,
  payload    => $payload,
  type       => $type,
  status     => 'pending',
  priority     => $priority,
  available_at   => $avail,
  available_at_dt => self!epoch-to-dt($avail),
  created_at   => $now,
  created_at_dt  => self!epoch-to-dt($now),
  locked_at    => Any,
  locked_at_dt   => Any,
  locked_by    => Any,
  attempts     => 0,
  max_attempts   => $max-attempts,
  done_at    => Any,
  done_at_dt   => Any,
  });
  $id
}

# ── Consumer ─────────────────────────────────────────────────────────────────

#| Atomically claim the next available job and return its document, or
#| return C<Nil> when the queue is empty.
#|
#| The returned Hash is the full MongoDB document; use C«$job<payload>»
#| for the application data and C«$job<_id>» when calling ack/nack.
method dequeue(Str $type, Str :$worker-id = '' --> Hash) {
  my Str $wid = $worker-id || self!default-worker-id;
  my Int $now = now.to-posix[0].Int;

  # Optimistic-lock pattern: find candidates then race to claim one.
  # Sort by priority only — a single key avoids BSON hash-ordering
  # ambiguity.  available_at is enforced by the filter.
  # Loop so that if every candidate is stolen by a concurrent worker we
  # retry with a fresh find rather than returning a false Nil.
  loop {
    my @candidates = await $!col.find(
      { status => 'pending', available_at => { '$lte' => $now }, :$type },
      options => { sort => { priority => -1 }, limit => $.dequeue-batch },
    ).all;

    return Nil unless @candidates;   # Queue is genuinely empty

    for @candidates -> $job {
      my $res = await $!col.update-one(
        { _id => $job<_id>, status => 'pending', type => $type },
        {
          '$set' => { status => 'processing', locked_at => $now, locked_at_dt => self!epoch-to-dt($now), locked_by => $wid },
          '$inc' => { attempts => 1 },
        },
      );
      return self!patch-job($job, $now, $wid) if $res<modifiedCount>;
    }
    # Every candidate was stolen by a concurrent worker — retry.
  }
}

# ── Bulk producer ────────────────────────────────────────────────────────────

#| Insert multiple jobs in a single MongoDB round trip.
#| All jobs share the same priority, delay, and max-attempts.
#| Returns an Array of the new job C<_id> strings in insertion order.
#|
#|   my @ids = $q.enqueue-many(@payloads, priority => 5);
method enqueue-many(
  @payloads,
  Str  $type,
  Int :$priority   = 0,
  Int :$delay    = 0,
  Int :$max-attempts = $.max-attempts,
--> Array) {
  my Int $now   = now.to-posix[0].Int;
  my Int $avail = $now + $delay;
  my @docs = @payloads.map(-> $payload {
    %(
    _id       => self!gen-id,
    payload     => $payload,
    type        => $type,
    status      => 'pending',
    priority    => $priority,
    available_at  => $avail,
    available_at_dt => self!epoch-to-dt($avail),
    created_at    => $now,
    created_at_dt   => self!epoch-to-dt($now),
    locked_at     => Any,
    locked_at_dt  => Any,
    locked_by     => Any,
    attempts    => 0,
    max_attempts  => $max-attempts,
    done_at     => Any,
    done_at_dt    => Any,
    )
  });
  await $!col.insert-many(@docs);
  @docs.map(*<_id>).Array
}

# ── Bulk consumer ─────────────────────────────────────────────────────────────

#| Claim up to C<$n> jobs in one pass: one C<find> round trip followed by at
#| most C<$n> C<update-one> claims.  Returns an Array of job documents
#| (possibly fewer than C<$n> under heavy contention or near queue-empty).
#|
#|   my @jobs = $q.dequeue-many(10);
#|   do-work($_<payload>) for @jobs;
#|   $q.ack-many(@jobs.map(*<_id>));
method dequeue-many(Int $n,Str $type,Str :$worker-id = '' --> Array) {
  my Str $wid = $worker-id || self!default-worker-id;
  my Int $now = now.to-posix[0].Int;

  # Over-fetch to absorb contention: extra 50 % + 3 gives breathing room
  # for concurrent workers without inflating the result set too much.
  my Int $fetch = $n + ($n div 2) + 3;
  my @candidates = await $!col.find(
    { status => 'pending', available_at => { '$lte' => $now }, :$type },
    options => { sort => { priority => -1 }, limit => $fetch },
  ).all;

  my @claimed;
  for @candidates -> $job {
    last if @claimed.elems >= $n;
    my $res = await $!col.update-one(
      %(  _id => $job<_id>, status => 'pending', :$type ),
      {
        '$set' => { status => 'processing', locked_at => $now, locked_by => $wid },
        '$inc' => { attempts => 1 },
      },
    );
    @claimed.push(self!patch-job($job, $now, $wid)) if $res<modifiedCount>;
  }
  @claimed
}

#| Acknowledge successful processing.  Marks the job C<done>.
method ack(Str $id --> Bool) {
  my Int $now = now.to-posix[0].Int;
  my $result = await $!col.update-one(
    { _id => $id, status => 'processing' },
    { '$set' => { status => 'done', done_at => $now, done_at_dt => self!epoch-to-dt($now) } },
  );
  so $result<modifiedCount>
}

#| Acknowledge a batch of jobs in a single MongoDB round trip.
#| Returns the count of jobs actually marked done.
#|
#|   $q.ack-many(@jobs.map(*<_id>));
method ack-many(@ids --> Int) {
  return 0 unless @ids;
  my Int $now = now.to-posix[0].Int;
  my $result = await $!col.update-many(
    { _id => { '$in' => @ids.Array }, status => 'processing' },
    { '$set' => { status => 'done', done_at => $now, done_at_dt => self!epoch-to-dt($now) } },
  );
  $result<modifiedCount>
}

#| Return a job to the queue so it can be retried.
#| If the job has exhausted C<max_attempts> it is failed instead.
#|
#|   :delay(60)  — seconds before the job re-enters the visible pool
method nack(Str $id, Int :$delay = 0 --> Bool) {
  my $doc = await $!col.find-one({ _id => $id, status => 'processing' });
  return False without $doc;

  if $doc<attempts> >= $doc<max_attempts> {
    return self!mark-failed($id);
  }

  my Int $avail = now.to-posix[0].Int + $delay;
  my $result = await $!col.update-one(
    { _id => $id, status => 'processing', attempts => $doc<attempts> },
    {
      '$set' => {
        status      => 'pending',
        locked_at     => Any,
        locked_at_dt  => Any,
        locked_by     => Any,
        available_at  => $avail,
        available_at_dt => self!epoch-to-dt($avail),
      },
    },
  );
  so $result<modifiedCount>
}

#| Permanently fail a job regardless of remaining attempts.
method fail(Str $id --> Bool) {
  self!mark-failed($id)
}

# ── Metrics ───────────────────────────────────────────────────────────────────

#| Number of jobs waiting to be dequeued.
method size(Str $type --> Int) {
  await $!col.count-documents({ status => 'pending', :$type})
}

#| Number of jobs currently being processed.
method in-flight(Str $type --> Int) {
  await $!col.count-documents({ status => 'processing', :$type })
}

#| Number of permanently failed jobs.
method failed(Str $type --> Int) {
  await $!col.count-documents({ status => 'failed', :$type })
}

#| Total number of jobs in the collection (all statuses).
method total(Str $type --> Int) {
  await $!col.count-documents({ :$type })
}

#| Snapshot of all queue counters as a Hash.
method stats(Str $type --> Hash) {
  %(
    pending  => self.size($type),
    processing => self.in-flight($type),
    failed   => self.failed($type),
    total    => self.total($type),
  )
}

# ── Maintenance ───────────────────────────────────────────────────────────────

#| Re-queue (or fail) jobs that have been processing longer than
#| C<:older-than> seconds (defaults to C<visibility-timeout>).
#| Returns the number of jobs reclaimed.
method reclaim-stale(Int :$older-than = $.visibility-timeout --> Int) {
  my Int $cutoff = now.to-posix[0].Int - $older-than;
  my @stale = await $!col.find({
    status  => 'processing',
    locked_at => { '$lt' => $cutoff },
  }).all;
  my int $count = 0;
  for @stale -> $doc {
    my Str $id = $doc<_id>;
    if $doc<attempts> >= $doc<max_attempts> {
      self!mark-failed($id);
    } else {
      await $!col.update-one(
        { _id => $id, status => 'processing' },
        {
          '$set' => {
            status     => 'pending',
            locked_at  => Any,
            locked_at_dt => Any,
            locked_by  => Any,
          },
        },
      );
    }
    $count++;
  }
  $count
}

#| Delete all C<done> and C<failed> jobs.  Returns the count deleted.
method purge(--> Int) {
  my $result = await $!col.delete-many({
    status => { '$in' => ['done', 'failed'] }
  });
  $result<deletedCount>
}

#| Delete every job in the collection regardless of status.  Useful in tests.
method clear(--> Int) {
  my $result = await $!col.delete-many({});
  $result<deletedCount>
}

#| Drop the entire jobs collection (destructive — useful in tests).
method drop(--> Bool) {
  await $!col.drop;
  True
}

# ── Event loop ────────────────────────────────────────────────────────────────

#| Block forever, processing jobs as they arrive.
#|
#|   $q.listen(-> $job {
#|     do-work($job<payload>);
#|   });
#|
#| The callback receives the full job document.  C<ack> is called
#| automatically on success; C<nack> is called if the callback throws.
#|
#| Named options:
#|   :worker-id<name>  — override the worker identifier
#|   :poll-interval(2)   — seconds to sleep when the queue is empty
#|   :reclaim-every(60)  — how often (seconds) to scan for stale jobs
#|   :batch(1)           — jobs claimed per poll cycle; >1 uses dequeue-many
method listen (
  &callback,
  Str :$worker-id = '',
  Str :$type = 'DNS',
  Int :$poll-interval = $.poll-interval,
  Int :$reclaim-every = 60,
  Int :$batch = 1,
--> Nil) {
  my Str $wid = $worker-id || self!default-worker-id;
  my Instant $last-reclaim = now;

  loop {
    CATCH { default { .note; sleep $poll-interval; next } }
    if now - $last-reclaim >= $reclaim-every {
      self.reclaim-stale;
      $last-reclaim = now;
    }
    my @jobs = $batch > 1
        ?? self.dequeue-many($batch,$type,:worker-id($wid))
        !! do { with self.dequeue($type,:worker-id($wid)) { ($_, ) } else { () } };
    if @jobs {
      for @jobs -> $job {
        my Str $id = $job<_id>;
        my $ok = try { callback($job); True };
        if $ok {
          try { self.ack($id) } or note "ack failed for $id: $!";
        } else {
          note $!;
          try { self.nack($id) } or note "nack failed for $id: $!";
        }
      }
    } 
    else {
      sleep $poll-interval;
    }
  }
}

# ── Private helpers ───────────────────────────────────────────────────────────

method !patch-job($job, Int $now, Str $wid --> Hash) {
  my %updated = %$job;
  %updated<status>    = 'processing';
  %updated<locked_at>   = $now;
  %updated<locked_at_dt> = self!epoch-to-dt($now);
  %updated<locked_by>   = $wid;
  %updated<attempts>  = ($job<attempts> // 0) + 1;
  %updated
}

method !gen-id(--> Str) {
  (^16).map({ (^256).pick.fmt('%02x') }).join
}

method !default-worker-id(--> Str) {
  state Str $id = "{$*PROGRAM.basename}-{$*PID}-{self!gen-id.substr(0, 8)}";
  $id
}

method !mark-failed(Str $id --> Bool) {
  my Int $now = now.to-posix[0].Int;
  my $result = await $!col.update-one(
    { _id => $id, status => 'processing' },
    { '$set' => { status => 'failed', done_at => $now, done_at_dt => self!epoch-to-dt($now) } },
  );
  so $result<modifiedCount>
}

method !epoch-to-dt(Int $epoch --> DateTime) {
  DateTime.new($epoch).utc
}
