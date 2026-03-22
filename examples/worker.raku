#!/usr/bin/env raku
use v6.d;
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
    batch         => 5,    # claim up to 5 jobs per poll cycle
);

# ── stub — replace with a real SMTP call ──────────────────────────────────────
sub send-smtp(%email) {
    # e.g. use Net::SMTP or an HTTP API client here
    sleep 0.001;  # simulate network latency
}
