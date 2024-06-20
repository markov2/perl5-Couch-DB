# SPDX-FileCopyrightText: 2024 Mark Overmeer <mark@overmeer.net>
# SPDX-License-Identifier: Artistic-2.0

package Couch::DB::Cluster;

use Couch::DB::Util  qw/flat/;;

use Log::Report 'couch-db';

use Scalar::Util  qw(weaken);
use URI::Escape   qw(uri_escape);
use Storable      qw(dclone);

=chapter NAME

Couch::DB::Cluster - interface for cluster management

=chapter SYNOPSIS

  my $cluster = $couchdb->cluster;

=chapter DESCRIPTION
This modules groups all CouchDB API calls which relate to clustering,
replication, sharind, and related jobs.  There are too many related
methods, so they got their own module.

=chapter METHODS

=section Constructors

=c_method new %options
B<Do not call> the method yourself: use M<Couch::DB::cluster()>.

=requires couch C<Couch::DB>-object
=cut

sub new(@) { my ($class, %args) = @_; (bless {}, $class)->init(\%args) }

sub init($)
{   my ($self, $args) = @_;

    $self->{CDC_couch} = delete $args->{couch} or panic "Requires couch";
    weaken $self->{CDC_couch};

    $self;
}


#-------------
=section Accessors
=method couch
=cut

sub couch() { $_[0]->{CDC_couch} }

#-------------
=section Managing a Cluster

B<All CouchDB API calls> documented below, support C<%options> like C<_delay>,
C<_client>, and C<on_error>.  See L<Couch::DB/Using the CouchDB API>.

=method clusterState %options
 [CouchDB API "GET /_cluster_setup", since 2.0]

Describes the status of this CouchDB instance is in the cluster.
Option C<ensure_dbs_exist>.
=cut

sub clusterState(%)
{	my ($self, %args) = @_;

	my %query;
	my @need = flat delete $args{ensure_dbs_exists};
	$query{ensure_dbs_exists} = $self->couch->jsonText(\@need, compact => 1)
		if @need;

	$self->couch->call(GET => '/_cluster_setup',
		introduced => '2.0.0',
		query      => \%query,
		$self->couch->_resultsConfig(\%args),
	);
}

=method clusterSetup $config, %options
 [CouchDB API "POST /_cluster_setup", since 2.0, UNTESTED]

Configure a node as a single (standalone) node, as part of a cluster,
or finalise a cluster.

=cut

sub clusterSetup($%)
{	my ($self, $config, %args) = @_;

	$self->couch->toJSON($config, int => qw/port node_count/);
	
	$self->couch->call(POST => '/_cluster_setup',
		introduced => '2.0.0',
		send       => $config,
		$self->couch->_resultsConfig(\%args),
	);
}

#-------------
=section Sharding

=method reshardStatus %options
 [CouchDB API "GET /_reshard", since 2.4]
 [CouchDB API "GET /_reshard/state", since 2.4]

Retrieve the state of resharding on the cluster.

B<Be warned> that the reply with counts returns C<state_reason>,
where the version without returns C<reason>.

=option  counts BOOLEAN
=default counts C<false>
Include the job counts in the result.
=cut

sub reshardStatus(%)
{	my ($self, %args) = @_;
	my $path = '/_reshard';
	$path   .= '/state' unless delete $args{counts};

	$self->couch->call(GET => $path,
		introduced => '2.4.0',
		$self->couch->_resultsConfig(\%args),
	);
}

=method resharding %options
 [CouchDB API "PUT /_reshard/state", since 2.4, UNTESTED]

Start or stop the resharding process.

=requires state STRING
Can be C<stopped> or C<running>.  Stopped state can be resumed into running.

=option   reason STRING
=default  reason C<undef>

=cut

sub resharding(%)
{	my ($self, %args) = @_;

	my %send   = (
		state  => (delete $args{state} or panic "Requires 'state'"),
		reason => delete $args{reason},
	);

	$self->couch->call(PUT => '/_reshard/state',
		introduced => '2.4.0',
		send       => \%send,
		$self->couch->_resultsConfig(\%args),
	);
}

=method reshardJobs %options
 [CouchDB API "GET /_reshard/jobs", since 2.4]

Show the resharding activity.
=cut

sub __jobValues($$)
{	my ($couch, $job) = @_;

	$couch->toPerl($job, isotime => qw/start_time update_time/)
	      ->toPerl($job, node => qw/node/);

	$couch->toPerl($_, isotime => qw/timestamp/)
		for @{$job->{history} || []};
}

sub __reshardJobsValues($$)
{	my ($result, $data) = @_;
	my $couch  = $result->couch;

	my $values = dclone $data;
	__jobValues($couch, $_) for @{$values->{jobs} || []};
	$values;
}

sub reshardJobs(%)
{	my ($self, %args) = @_;

	$self->couch->call(GET => '/_reshard/jobs',
		introduced => '2.4.0',
		$self->couch->_resultsConfig(\%args, on_values => \&__reshardJobsValues),
	);
}

=method reshardStart \%create, %options
 [CouchDB API "POST /_reshard/jobs", since 2.4, UNTESTED]

Create resharding jobs.
=cut

sub __reshardStartValues($$)
{	my ($result, $data) = @_;
	my $values = dclone $data;
	$result->couch->toPerl($_, node => 'node')
		for @$values;

	$values;
}

sub reshardStart($%)
{	my ($self, $create, %args) = @_;

	$self->couch->call(POST => '/_reshard/jobs',
		introduced => '2.4.0',
		send       => $create,
		$self->couch->_resultsConfig(\%args, on_values => \&__reshardStartValues),
	);
}

=method reshardJob $jobid, %options
 [CouchDB API "GET /_reshard/jobs/{jobid}", since 2.4, UNTESTED]

Show the resharding activity.
=cut

sub __reshardJobValues($$)
{	my ($result, $data) = @_;
	my $couch  = $result->couch;

	my $values = dclone $data;
	__jobValues($couch, $values);
	$values;
}

sub reshardJob($%)
{	my ($self, $jobid, %args) = @_;

	$self->couch->call(GET => "/_reshard/jobs/$jobid",
		introduced => '2.4.0',
		$self->couch->_resultsConfig(\%args, on_values => \&__reshardJobValues),
	);
}

=method reshardJobRemove $jobid, %options
 [CouchDB API "DELETE /_reshard/jobs/{jobid}", since 2.4, UNTESTED]

Show the resharding activity.
=cut

sub reshardJobRemove($%)
{	my ($self, $jobid, %args) = @_;

	$self->couch->call(DELETE => "/_reshard/jobs/$jobid",
		introduced => '2.4.0',
		$self->couch->_resultsConfig(\%args),
	);
}

=method reshardJobState $jobid, %options
 [CouchDB API "GET /_reshard/jobs/{jobid}/state", since 2.4, UNTESTED]

Show the resharding job status.
=cut

sub reshardJobState($%)
{	my ($self, $jobid, %args) = @_;

	$self->couch->call(GET => "/_reshard/job/$jobid/state",
		introduced => '2.4.0',
		$self->couch->_resultsConfig(\%args),
	);
}

=method reshardJobChange $jobid, %options
 [CouchDB API "PUT /_reshard/jobs/{jobid}/state", since 2.4, UNTESTED]
Change the resharding job status.

=requires state STRING
Can be C<new>, C<running>, C<stopped>, C<completed>, or C<failed>.

=option   reason STRING
=default  reason C<undef>
=cut

sub reshardJobChange($%)
{	my ($self, $jobid, %args) = @_;

	my %send = (
		state  => (delete $args{state} or panic "Requires 'state'"),
		reason => delete $args{reason},
	);

	$self->couch->call(PUT => "/_reshard/job/$jobid/state",
		introduced => '2.4.0',
		send       => \%send,
		$self->couch->_resultsConfig(\%args),
	);
}

=method shardsForDB $db, %options
 [CouchDB API "GET /{db}/_shards", since 2.0]

Returns the structure of the shared used to store a database.  Pass this
a C<$db> as M<Couch::DB::Database>-object.
=cut

sub __dbshards($$)
{	my ($result, $data) = @_;
	my $couch  = $result->couch;

	my %values = %$data;
	my $shards = delete $values{shards} || {};
	$values{shards} = [ map +($_ => $couch->listToPerl($_, node => $shards->{$_}) ), keys %$shards ];
	\%values;
}

sub shardsForDB($%)
{	my ($self, $db, %args) = @_;

	$self->couch->call(GET => $db->_pathToDB('_shards'),
		introduced => '2.0.0',
		$self->couch->_resultsConfig(\%args, on_values => \&__dbshards),
	);
}

=method shardsForDoc $doc, %options
 [CouchDB API "GET /{db}/_shards/{docid}", since 2.0]

Returns the structure of the shared used to store a database.  Pass this
a C<$db> as M<Couch::DB::Database>-object.
=cut

sub __docshards($$)
{	my ($result, $data) = @_;
	my $values = +{ %$data };
	$values->{nodes} = [ $result->couch->listToPerl($values, node => delete $values->{nodes}) ];
	$values;
}

sub shardsForDoc($%)
{	my ($self, $doc, %args) = @_;
	my $db = $doc->db;

	$self->couch->call(GET => $db->_pathToDB('_shards/'.$doc->id),
		introduced => '2.0.0',
		$self->couch->_resultsConfig(\%args, on_values => \&__docshards),
	);
}

=method syncShards $db, %options
 [CouchDB API "POST /{db}/_sync_shards", since 2.3.1]

Force (re-)sharding of documents, usually in response to changes in the setup.
Pass this a C<$db> as M<Couch::DB::Database>-object.
=cut

sub syncShards($%)
{	my ($self, $db, %args) = @_;

	$self->couch->call(POST => $db->_pathToDB('_sync_shards'),
		send => {},
		introduced => '2.3.1',
		$self->couch->_resultsConfig(\%args),
	);
}

1;
