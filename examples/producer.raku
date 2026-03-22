#!/usr/bin/env raku
use v6.d;
use lib 'lib';
use MongoDB::Queue;

my $q = MongoDB::Queue.connect(host => 'localhost', db => 'myapp');

my @emails = (
    %( to => 'alice@example.com', subject => 'Hello'  ),
    %( to => 'bob@example.com',   subject => 'Hi'     ),
    %( to => 'ceo@example.com',   subject => 'Urgent' ),
);

# Send the CEO's email first (high priority, same type)
$q.enqueue(@emails[2], 'email', priority => 100);

# Bulk-insert the rest in one round trip
my @ids = $q.enqueue-many(@emails[0..1], 'email');

say "Queued 1 high-priority + {@ids.elems} normal emails.";
say "Queue size: {$q.size('email')}";
say "Stats: {$q.stats('email').raku}";
