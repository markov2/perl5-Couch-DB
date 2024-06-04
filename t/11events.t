#!/usr/bin/env perl
# Test the Couch::DB::Result events: are they correctly collected an run?

use Test::More;
use HTTP::Status qw(HTTP_CREATED);
use JSON::PP;
use Scalar::Util qw(refaddr);

use lib 'lib', 't';
use Couch::DB::Util qw(simplified);
use Test;

#$dump_answers = 1;
#$dump_values  = 1;
#$trace = 1;

my $couch = _framework;
ok defined $couch, 'Created the framework';

my $db = $couch->db('test');
ok defined $db, 'Create database "test"';

#### Chain on any action

my ($c1, @cmore);
my $c  = $db->create(on_chain => sub { 
	($c1, @cmore) = @_;
	# do chain actions
	$c1;  # return a result
});
ok defined $c, 'Create with on_chain';
isa_ok $c, 'Couch::DB::Result';
ok !!$c, '... success;';
ok defined $c1, '... chain called';
ok !@cmore, '... no extra parameters expected';
is refaddr($c), refaddr($c1), '... chain return = last return';

#### on_final
#XXX

#### on_error
#XXX

#### on_values

my $pe = +{ a => 1 };
my $p = $db->ping(on_values => sub {
	my ($result, $raw) = @_;
	return $pe;
});
ok defined $p, 'Ping with on_values';
ok !!$p, '... success';
is_deeply $p->values, $pe, '... on_values called';

# Clean-up
_result removed          => $db->remove;

done_testing;
