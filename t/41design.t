#!/usr/bin/env perl

use Test::More;
use HTTP::Status    qw(HTTP_OK);

use lib 'lib', 't';
use Couch::DB::Util qw(simplified);
use Test;

#$dump_answers = 1;
#$dump_values  = 1;
#$trace = 1;

my $couch = _framework;
ok defined $couch, 'Created the framework';

my $db = $couch->db('test');
_result create           => $db->create;

####### $db->design;

my $t0 = $db->design;
ok defined $t0, "Created id-less design in test";
isa_ok $t0, 'Couch::DB::Design';
isa_ok $t0, 'Couch::DB::Document';

like $t0->id, qr!^_design/!, '... id = '. $t0->id;
is $t0->idBase, $t0->id =~ s!^_design/!!r, '... idBase = '. $t0->idBase;

####### $db->design('testddoc')

my $t1 = $db->design('testddoc1');
ok defined $t1, "Created testddoc1 in test";
isa_ok $t1, 'Couch::DB::Design';
isa_ok $t1, 'Couch::DB::Document';

is $t1->id, '_design/testddoc1', '... id = '. $t1->id;
is $t1->idBase, $t1->id =~ s!^_design/!!r, '... idBase = '. $t1->idBase;
is $t1->db, $db, '... db';
is $t1->couch, $couch, '... couch';

_result t1_create        => $t1->create({});
$trace && warn Dumper [ $t1->revisions ];

my $l1 = $db->designs({include_docs => 1, conflicts => 1});
$trace && warn Dumper [ $l1->answer ];

_result t1_update        => $t1->update({});
$trace && warn Dumper [ $t1->revisions ];

my $t2 = $db->design('testddoc1');
isa_ok $t2, 'Couch::DB::Design';

my $r2 = _result t2_get => $t2->get({
	conflicts => 1,
	deleted_conflicts => 1,
	latest => 1,
	local_seq => 1,
	meta => 1,
	revs => 1,
	revs_info => 1,
});

$trace && warn "INFO=", Dumper $t2->_info;
$trace && warn "LATEST=", Dumper $t2->latest;
$trace && warn "CONFLICTS=", Dumper $t2->conflicts;
$trace && warn "DEL CONFLICTS=", Dumper $t2->deletedConflicts;
$trace && warn "UPDATE SEQ=", Dumper $t2->updateSequence;
$trace && warn "REV INFO=", Dumper $t2->revisionsInfo;

$trace && warn $r2->request->to_string;
$trace && warn $r2->response->to_string;

####### Cleanup
_result removed          => $db->remove;

done_testing;
