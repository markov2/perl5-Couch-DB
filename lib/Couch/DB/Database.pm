# SPDX-FileCopyrightText: 2024 Mark Overmeer <mark@open-console.eu>
# SPDX-License-Identifier: EUPL-1.2-or-later

package Couch::DB::Database;

use Log::Report 'couch-db';

use Couch::DB::Util   qw(flat);

use Scalar::Util      qw(weaken);
use HTTP::Status      qw(HTTP_OK HTTP_NOT_FOUND);

=chapter NAME

Couch::DB::Database - One database connection

=chapter SYNOPSIS

   my $db = Couch::DB->db('my-db');

=chapter DESCRIPTION

One I<node> (server) contains multiple databases.  Databases
do not contain "collections", like MongoDB; each document is
a direct child of a database.  Per database, you get multiple
files to store that data, for views, replication, and so on.  
Per database, you need to set permissions.

Clustering, sharing, and replication activities on a database
are provided by the M<Couch::DB::Cluster> package.

=chapter METHODS

=section Constructors

=c_method new %options

=requires name STRING
The name of a database must match C<< ^[a-z][a-z0-9_$()+/-]*$ >>.

=requires couch C<Couch::DB>-object

=option  batch BOOLEAN
=default batch C<false>
When set, all write actions (which support this) to this database
will not wait for the actual update of the database.  This gives a
higher performance, but not all error may be reported.
=cut

sub new(@) { my ($class, %args) = @_; (bless {}, $class)->init(\%args) }

sub init($)
{	my ($self, $args) = @_;

	my $name = $self->{CDD_name} = delete $args->{name} or panic "Requires name";
	$name =~ m!^[a-z][a-z0-9_$()+/-]*$!
		or error __x"Illegal database name '{name}'.", name => $name;

	$self->{CDD_couch} = delete $args->{couch} or panic "Requires couch";
	weaken $self->{CDD_couch};

	$self->{CDD_batch} = delete $args->{batch};
	$self;
}

#-------------
=section Accessors

=method name
=method couch
=method batch
=cut

sub name()  { $_[0]->{CDD_name} }
sub couch() { $_[0]->{CDD_couch} }
sub batch() { $_[0]->{CDD_batch} }

sub _pathToDB(;$) { '/' . $_[0]->name . (defined $_[1] ? '/' . $_[1] : '') }

#-------------
=section Database information

B<All CouchDB API calls> documented below, support %options like C<_delay>
and C<on_error>.  See L<Couch::DB/Using the CouchDB API>.

=method ping %options
[CouchDB API "HEAD /{db}"]
Check whether the database exists.  You may get some useful response
headers, but nothing more: the response body is empty.
=cut

sub ping(%)
{	my ($self, %args) = @_;

	$self->couch->call(HEAD => $self->_pathToDB,
		$self->couch->_resultsConfig(\%args),
	);
}

=method exists
Returns a boolean, whether the database exists already.  This will
call M<ping()> and wait for an anwser.
=cut

sub exists()
{	my $self = shift;
	my $result = $self->ping(_delay => 0);
	  $result->code eq HTTP_NOT_FOUND ? 0
    : $result->code eq HTTP_OK        ? 1
	:                                   undef;  # will probably die in the next step
}

=method details %options
[CouchDB API "GET /{db}"],
[CouchDB API "GET /{db}/_partition/{partition}", UNTESTED].

Collect information from the database, for instance about its clustering.

=option
=cut

sub __detailsValues($$)
{	my ($result, $raw) = @_;
	my $couch = $result->couch;
	my %values = %$raw;   # deep not needed;
	$couch->toPerl(\%values, epoch => qw/instance_start_time/);
	\%values;
}

sub details(%)
{	my ($self, %args) = @_;
	my $part = delete $args{partition};

	#XXX Value instance_start_time is now always zero, useful to convert if not
	#XXX zero in old nodes?

	$self->couch->call(GET => $self->_pathToDB($part ? '_partition/'.uri_escape($part) : undef),
		to_values  => \&__detailsValues,
		$self->couch->_resultsConfig(\%args),
	);
}

=method create %options
[CouchDB API "PUT /{db}"]
Create a new database.  The result object will have code HTTP_CREATED when the
database is successfully created.  When the database already exists, it
returns HTTP_PRECONDITION_FAILED and an error in the body.

Options: C<partitioned> (bool), C<q> (shards, default 8), and C<n> (replicas,
default 3).
=cut

sub create(%)
{	my ($self, %args) = @_;
	my $couch = $self->couch;

	my %query;
	exists $args{$_} && ($query{$_} = delete $args{$_})
		for qw/partitioned q n/;
	$couch->toQuery(\%query, bool => qw/partitioned/);
	$couch->toQuery(\%query, int  => qw/q n/);

	$couch->call(PUT => $self->_pathToDB,
		query => \%query,
		send  => { },
		$self->couch->_resultsConfig(\%args),
	);
}

=method remove %options
[CouchDB API "DELETE /{db}"]
Remove the database.
=cut

sub remove(%)
{	my ($self, %args) = @_;

	$self->couch->call(DELETE => $self->_pathToDB,
		$self->couch->_resultsConfig(\%args),
	);
}

=method userRoles %options
[CouchDB API "GET /{db}/_security"]
Returns the users who have access to the database, including their roles
(permissions).

Usually, it is better to simply attempt to take an action, and handle the
errors: having a role does not mean that the action will be error-less
anyway.
=cut

sub userRoles(%)
{	my ($self, %args) = @_;

	$self->couch->call(GET => $self->_pathToDB('_security'),
		$self->couch->_resultsConfig(\%args),
	);
}

=method userRolesChange %options
[CouchDB API "PUT /{db}/_security", UNTESTED]
Returns the users who have access to the database, including their roles
(permissions).

=option  admin ARRAY
=default admin C<< [ ] >>

=option  members ARRAY
=default members C<< [ ] >>
=cut

sub userRolesChange(%)
{	my ($self, %args) = @_;
	my %send  = (
		admin   => delete $args{admin}   || [],
		members => delete $args{members} || [],
	);

	$self->couch->call(PUT => $self->_pathToDB('_security'),
		send  => \%send,
		$self->couch->_resultsConfig(\%args),
	);
}

=method changes %options
[CouchDB API "GET /{db}/_changes", TODO] and
[CouchDB API "POST /{db}/_changes", TODO].
=cut

sub changes { ... }

=method compact %options
[CouchDB API "POST /{db}/_compact"],
[CouchDB API "POST /{db}/_compact/{ddoc}", UNTESTED]
Instruct the database files to be compacted.  By default, the data gets
compacted.

=option  ddoc $ddoc
=default ddoc C<undef>
Compact all indexes related to this design document, instead.
=cut

sub compact(%)
{	my ($self, %args) = @_;
	my $path = $self->_pathToDB('_compact');

	if(my $ddoc = delete $args{ddoc})
	{	$path .= '/' . $ddoc->id;
	}

	$self->couch->call(POST => $path,
		send  => { },
		$self->couch->_resultsConfig(\%args),
	);
}

=method ensureFullCommit %options
[CouchDB API "POST /{db}/_ensure_full_commit", deprecated 3.0.0].
=cut

sub __ensure($$)
{	my ($result, $raw) = @_;
	return $raw unless $raw->{instance_start_time};  # exists && !=0
	my $v = { %$raw };
	$result->couch->toPerl($v, epoch => qw/instance_start_time/);
	$v;
}

sub ensureFullCommit(%)
{	my ($self, %args) = @_;

	$self->couch->call(POST => $self->_pathToDB('_ensure_full_commit'),
		deprecated => '3.0.0',
		send       => { },
		to_values  => \&__ensure,
		$self->couch->_resultsConfig(\%args),
	);
}

=method purgeDocuments \%plan, %options
[CouchDB API "POST /{db}/_purge", UNTESTED].
Remove selected document revisions from the database.

A deleted document is only marked as being deleted, but exists until
purge.  There must be sufficient time between deletion and purging,
to give replication a chance to distribute the fact of deletion.
=cut

sub purgeDocuments($%)
{	my ($self, $plan, %args) = @_;

	#XXX looking for smarter behavior here, to construct a plan.
	my $send = $plan;

	$self->couch->call(POST => $self->_pathToDB('_purge'),
		$self->couch->_resultsConfig(\%args),
	);
}

=method purgeRecordsLimit %options
[CouchDB API "GET /{db}/_purged_infos_limit", UNTESTED].
Returns the soft maximum number of records kept about deleting records.
=cut

#XXX seems not really a useful method.

sub purgeRecordsLimit(%)
{	my ($self, %args) = @_;

	$self->couch->call(GET => $self->_pathToDB('_purged_infos_limit'),
		$self->couch->_resultsConfig(\%args),
	);
}

=method purgeRecordsLimitSet $limit, %options
[CouchDB API "PUT /{db}/_purged_infos_limit", UNTESTED].
Set a new soft limit.  The default is 1000.
=cut

#XXX attribute of database creation

sub purgeRecordsLimitSet($%)
{	my ($self, $value, %args) = @_;

	$self->couch->call(PUT => $self->_pathToDB('_purged_infos_limit'),
		send => int($value),
		$self->couch->_resultsConfig(\%args),
	);
}

=method purgeUnusedViews %options
[CouchDB API "POST /{db}/_view_cleanup", UNTESTED].
=cut

sub purgeUnusedViews(%)
{	my ($self, %args) = @_;

	#XXX nothing to send?
	$self->couch->call(POST => $self->_pathToDB('_view_cleanup'),
		$self->couch->_resultsConfig(\%args),
	);
}

=method revisionsMissing \%plan, %options
[CouchDB API "POST /{db}/_missing_revs", UNTESTED].
With given a list of document revisions, returns the document revisions
that do not exist in the database.
=cut

sub revisionsMissing($%)
{	my ($self, $plan, %args) = @_;

	#XXX needs extra features
	$self->couch->call(POST => $self->_pathToDB('_missing_revs'),
		send => $plan,
		$self->couch->_resultsConfig(\%args),
	);
}

=method revisionsDiff \%plan, %options
[CouchDB API "POST /{db}/_revs_diff", UNTESTED].
With given a list of document revisions, returns the document revisions
that do not exist in the database.
=cut

sub revisionsDiff($%)
{	my ($self, $plan, %args) = @_;

	#XXX needs extra features
	$self->couch->call(POST => $self->_pathToDB('_revs_diff'),
		send => $plan,
		$self->couch->_resultsConfig(\%args),
	);
}

=method revisionLimit %options
[CouchDB API "GET /{db}/_revs_limit", UNTESTED].
Returns the soft maximum number of records kept about deleting records.
=cut

#XXX seems not really a useful method.

sub revisionLimit(%)
{	my ($self, %args) = @_;

	$self->couch->call(GET => $self->_pathToDB('_revs_limit'),
		$self->couch->_resultsConfig(\%args),
	);
}

=method revisionLimitSet $limit, %options
[CouchDB API "PUT /{db}/_revs_limit", UNTESTED].
Set a new soft limit.  The default is 1000.
=cut

#XXX attribute of database creation

sub revisionLimitSet($%)
{	my ($self, $value, %args) = @_;

	$self->couch->call(PUT => $self->_pathToDB('_revs_limit'),
		send => int($value),
		$self->couch->_resultsConfig(\%args),
	);
}

#-------------
=section Designs

=method listDesigns
[CouchDB API "GET /{db}/_design_docs", UNTESTED] and
[CouchDB API "POST /{db}/_design_docs", UNTESTED].
[CouchDB API "POST /{db}/_design_docs/queries", UNTESTED].
Get some design documents.

If there are searches, then C<GET> is used, otherwise the C<POST> version.
The returned structure depends on the searches and the number of searches.

=option  search \%query|ARRAY
=default search []
=cut

sub listDesigns(%)
{	my ($self, %args) = @_;
	my $couch   = $self->couch;

	my ($method, $path, $send) = (GET => $self->_pathToDB('_design_docs'), undef);
	my @search  = flat delete $args{search};
	if(@search)
	{	$method = 'POST';
	 	my @s;
		foreach (@search)
		{	my $s  = +{ %$_ };
			$couch->toJSON($s, bool => qw/conflicts descending include_docs inclusive_end update_seq/);
			push @s, $s;
		}
		if(@search==1)
		{	$send  = $search[0];
		}
		else
		{	$send  = +{ queries => \@search };
			$path .= '/queries';
		}
	}

	$self->couch->call($method => $path,
		($send ? (send => $send) : ()),
		$couch->_resultsConfig(\%args),
	);
}

=method createIndex %options
[CouchDB API "POST /{db}/_index", UNTESTED]
Create/confirm an index on the database.  By default, the index C<name>
and the name for the design document C<ddoc> are generated.  You can
also call C<Couch::DB::Design::createIndex()>.
=cut

sub createIndex($%)
{	my ($self, %args) = @_;
	my $couch  = $self->couch;

	my %config = $couch->_resultsConfig(\%args);
	my $send   = \%args;
	$couch->toJSON($send, bool => qw/partitioned/);

	$couch->call(POST => $self->_pathToDB('_index'),
		send => $send,
		%config,
	);
}

=method listIndexes %options
[CouchDB API "GET /{db}/_index", UNTESTED]
Collect all indexes for the database.
=cut

sub listIndexes(%)
{	my ($self, %args) = @_;

	$self->couch->call(GET => $self->_pathToDB('_index'),
		$self->couch->_resultsConfig(\%args),
	);
}

#-------------
=section Handling documents

=method doc ID, %options
Returns a M<Couch::DB::Document> for this ID.  Be aware that this does not have
any interaction with the CouchDB server.  Only when you call actions, like
M<Couch::DB::Document::exists()>, on that object, you can see the status and
content of the document.

The %options are passed to M<Couch::DB::Database::new()>.  Of course, you do not
need to pass the database object explicitly.
=cut

sub doc($%)
{	my ($self, $id) = @_;
	Couch::DB::Document->new(id => $id, db => $self, @_);
}

=method updateDocuments \@docs, %options
[CouchDB API "POST /{db}/_bulk_docs", UNTESTED]
Insert, update, and delete multiple documents in one go.  This is more efficient
than saving them one by one.

Pass the documents which need to be save/updated in an ARRAY as first argument.

=option  new_edits BOOLEAN
=default new_edits C<true>
When false, than the docs will replace the existing revisions.

=option  delete $doc|\@docs
=default delete C<< [ ] >>
List of documents to remove.  You should not call the C<delete()> method on
them yourself!

=option  on_error CODE
=default on_error C<undef>
By default, errors are ignored.  When a CODE is specified, it will be called
with the result object, the failing document, and named parameters error details.
The %details contain the C<error> type, the error C<reason>, and the optional
C<deleting> boolean boolean.

=example for error handling
  sub handle($result, $doc, %details) { ... }
  $db->updateDocuments(@save, on_error => \&handle);
=cut

sub __updated($$$$)
{	my ($self, $result, $saves, $deletes, $on_error) = @_;
	$result or return;

	my %saves   = map +($_->id => $_), @$saves;
	my %deletes = map +($_->id => $_), @$deletes;

	foreach my $report (@{$result->values})
	{	my $id     = $report->{id};
		my $delete = exists $deletes{$id};
		my $doc    = delete $deletes{$id} || delete $saves{$id}
			or panic "missing report for updated $id";

		if($report->{ok})
		{	$doc->saved($id, $report->{rev});
			$doc->deleted if $delete;
		}
		else
		{	$on_error->($result, $doc, +{ %$report, delete => $delete });
		}
	}

	$on_error->($result, $saves{$_},
		+{ error => 'missing', reason => "The server did not report back on saving $_." }
	) for keys %saves;

	$on_error->($result, $deletes{$_},
		+{ error => 'missing', reason => "The server did not report back on deleting $_.", delete => 1 }
	) for keys %deletes;
}

sub updateDocuments($%)
{	my ($self, $docs, %args) = @_;
	my $couch   = $self->couch;

	my @plan    = map $_->data, @$docs;
	my @deletes = flat delete $args{delete};

	foreach my $del (@deletes)
	{	push @plan, +{ _id => $del->id, _rev => $del->rev, _delete => 1 };
		$couch->toJSON($plan[-1], bool => qw/_delete/);
	}

	@plan or error __x"need at least on document for bulk processing.";
	my $send    = +{ docs => \@plan };

	$send->{new_edits} = delete $args{new_edits} if exists $args{new_edits};
	$couch->toJSON($send, bool => qw/new_edits/);

	$couch->call(POST => $self->_pathToDB('_bulk_docs'),
		send     => $send,
		$couch->_resultsConfig(\%args,
			on_final => sub { $self->_updated($_[0], $docs, \@deletes) },
		),
	);
}

=method inspectDocuments \@docs, %options
[CouchDB API "POST /{db}/_bulk_get", UNTESTED]
Return information on multiple documents at the same time.

=option  revs BOOLEAN
=default revs C<false>
Include the revision history of each document.
=cut

sub inspectDocuments($%)
{	my ($self, $docs, %args) = @_;
	my $couch = $self->couch;

	my $query;
	$query->{revs} = delete $args{revs} if exists $args{revs};
	$couch->toQuery($query, bool => qw/revs/);

	@$docs or error __x"need at least on document for bulk query.";

	#XXX what does "conflicted documents mean?
	#XXX what does "a": 1 mean in its response?

	$self->couch->call(POST =>  $self->_pathToDB('_bulk_get'),
		query => $query,
		send  => { docs => $docs },
		$couch->_resultsConfig(\%args),
	);
}

=method listDocuments %options
[CouchDB API "GET /{db}/_all_docs", UNTESTED],
[CouchDB API "POST /{db}/_all_docs", UNTESTED],
[CouchDB API "POST /{db}/_all_docs/queries", UNTESTED],
[CouchDB API "GET /{db}/_local_docs", UNTESTED],
[CouchDB API "POST /{db}/_local_docs", UNTESTED],
[CouchDB API "POST /{db}/_local_docs/queries", UNTESTED],
[CouchDB API "GET /{db}/_partition/{partition}/_all_docs", UNTESTED].

Get the documents, optionally limited by a view.
If there are searches, then C<POST> is used, otherwise the C<GET> version.
The returned structure depends on the searches and the number of searches.

The usual way to use this method with a view, is by calling
M<Couch::DB::Design::viewFind()>.

=option  search \%view|ARRAY
=default search []

=option  local  BOOLEAN
=default local C<false>
Search only in local (non-replicated) documents.  This does not support
a combination with C<partition> or C<view>.

=option  partition $name
=default partition C<undef>
Restrict the search to the specific partition.

=option  view $name
=default view C<undef>
Restrict the search to the named view.  Requires the C<design> document.

=option  design $ddoc|$ddoc_id
=default design C<undef>

=cut

sub listDocuments(%)
{	my ($self, %args) = @_;
	my $couch  = $self->couch;

	my @search = flat delete $args{search};
	my $part   = delete $args{partition};
	my $local  = delete $args{local};
	my $view   = delete $args{view};
	my $ddoc   = delete $args{ddoc};
	my $ddocid = blessed $ddoc ? $ddoc->id : $ddoc;

	!$view  || $ddoc  or panic "listDocuments(view) requires design document.";
	!$local || !$part or panic "listDocuments(local) cannot be combined with partition.";
	!$local || !$view or panic "listDocuments(local) cannot be combined with a view.";
	!$part  || @search < 2 or panic "listDocuments(partition) cannot work with multiple searches.";

	my $set
	  = $local ? '_local_docs'
	  :   ($part ? '_partition/'. uri_escape($part) . '/' : '')
        . ($view ? "_design/$ddocid/_view/". uri_escape($view) : '_all_docs');

	my $method = !@search || $part ? 'GET' : 'POST';
	my $path   = $self->_pathToDB($set);

	# According to the spec, _all_docs is just a special view.
	my @send   = map $self->_viewPrepare($method, $_, "listDocuments search"), @search;
		
	my @params;
	if($method eq 'GET')
	{	@send < 2 or panic "Only one search with listDocuments(GET)";
		@params = (query => $send[0]);
	}
	elsif(@send==1)
	{	@params = (send  => $send[0]);
	}
	else
	{	$couch->check(1, introduced => '2.2.0', 'Bulk queries');
		@params = (send => +{ queries => \@send });
		$path .= '/queries';
	}

	$couch->call($method => $path,
		@params,
		$couch->_resultsConfig(\%args),
	);
}

my @search_bools = qw/
	conflicts descending group include_docs attachments att_encoding_info
	inclusive_end reducs sorted stable update_seq
	/;

sub _viewPrepare($$$)
{	my ($self, $method, $data, $where) = @_;
	my $s     = +{ %$data };
	my $couch = $self->couch;

	# Main doc in 1.5.4.  /{db}/_design/{ddoc}/_view/{view}
	if($method eq 'GET')
	{	$couch
			->toQuery($s, bool => \@search_bools)
			->toQuery($s, json => qw/endkey end_key key keys start_key startkey/);
	}
	else
	{	$couch
			->toJSON($s, bool => \@search_bools)
			->toJSON($s, int  => qw/group_level limit skip/);
	}

	$couch
		->check($s->{attachments}, introduced => '1.6.0', 'Search attribute "attachments"')
		->check($s->{att_encoding_info}, introduced => '1.6.0', 'Search attribute "att_encoding_info"')
		->check($s->{sorted}, introduced => '2.0.0', 'Search attribute "sorted"')
		->check($s->{stable}, introduced => '2.1.0', 'Search attribute "stable"')
		->check($s->{update}, introduced => '2.1.0', 'Search attribute "update"');

	$s;
}

=method find $search, %options
[CouchDB API "POST /{db}/_find", UNTESTED],
[CouchDB API "POST /{db}/_partition/{partition_id}/_find", UNTESTED]

Search the database for matching components.

=option  partition $partition
=default partition C<undef>
=cut

sub find($%)
{	my ($self, $search, %args) = @_;
	my $part   = delete $args{partition};

	my $path   = $self->_pathToDB;
	$path     .= '/_partition/'. uri_espace($part) if $part;

	$self->couch->call(POST => "$path/_find",
		send => $self->_findPrepare(POST => $search),
		$self->couch->_resultsConfig(\%args),
	);
}

sub _findPrepare($$)
{	my ($self, $method, $data, $where) = @_;
	my $s = +{ %$data };  # no nesting

	$method eq 'POST' or panic;

	$self->couch
		->toJSON($s, bool => qw/conflicts update stable execution_stats/)
		->toJSON($s, int  => qw/limit sip r/);

	$s;
}

=method findExplain \%search, %options
[CouchDB API "POST /{db}/_explain", UNTESTED]
[CouchDB API "POST /{db}/_partition/{partition_id}/_explain", UNTESTED]

Explain how the a search will be executed.

=option  partition $partition
=default partition C<undef>
=cut

sub findExplain(%)
{	my ($self, $search, %args) = @_;
	my $part = delete $args{partition};

	my $path  = $self->_pathToDB;
	$path    .= '/_partition/' . uri_escape($part) if $part;

	$self->couch->call(POST => "$path/_explain",
		send => $self->_findPrepare(POST => $search),
		$self->couch->_resultsConfig(\%args),
	);
}

#-------------
=section Other
=cut

1;
