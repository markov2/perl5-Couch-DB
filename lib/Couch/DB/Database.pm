# SPDX-FileCopyrightText: 2024 Mark Overmeer <mark@overmeer.net>
# SPDX-License-Identifier: Artistic-2.0

package Couch::DB::Database;

use Log::Report 'couch-db';

use Couch::DB::Util   qw(flat);
use Couch::DB::Document ();
use Couch::DB::Design   ();

use Scalar::Util      qw(weaken blessed);
use HTTP::Status      qw(HTTP_OK HTTP_NOT_FOUND);
use JSON::PP ();

=chapter NAME

Couch::DB::Database - One database connection

=chapter SYNOPSIS

  my $db   = Couch::DB->db('my-db');

  # (search) documents in the database
  my @docs = $db->allDocs->docs;

=chapter DESCRIPTION

One I<node> (server) contains multiple databases.  Databases
do not contain "collections", like MongoDB: each document is
a direct child of a database.  Per database, you get multiple
files to store that data, for views, replication, and so on.  
Per database, you need to set permissions.

Clustering, sharing, and replication activities on a database
are provided by the M<Couch::DB::Cluster> package.

=chapter METHODS

=section Constructors

=c_method new %options
B<Do not call this> method yourself, but use C<Couch::DB::db()>
to instantiate this object.

=requires name STRING
The name of a database must match C<< ^[a-z][a-z0-9_$()+/-]*$ >>.

=requires couch C<Couch::DB>-object

=option  batch BOOLEAN
=default batch C<false>
When set, all write actions (which support this) to this database
will not wait for the actual update of the database.  This gives a
much higher performance, but not all errors may be reported.
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

B<All CouchDB API calls> documented below, support C<%options> like C<delay>,
C<client>, and C<on_error>.  See L<Couch::DB/Using the CouchDB API>.

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
Returns a boolean, whether the database exist on the server.  This will
call M<ping()> and wait for an anwser.
=cut

sub exists()
{	my $self = shift;
	my $result = $self->ping(delay => 0);

	  $result->code eq HTTP_NOT_FOUND ? 0
    : $result->code eq HTTP_OK        ? 1
	:     undef;  # will probably die in the next step
}

=method details %options
 [CouchDB API "GET /{db}"]
 [CouchDB API "GET /{db}/_partition/{partition}", UNTESTED]

Collect information from the database, for instance about its clustering.

=option  partition $partition
=default partition C<undef>
=cut

sub __detailsValues($$)
{	my ($self, $result, $raw) = @_;
	my %values = %$raw;   # deep not needed;
	$self->couch->toPerl(\%values, epoch => qw/instance_start_time/);
	\%values;
}

sub details(%)
{	my ($self, %args) = @_;
	my $part = delete $args{partition};

	#XXX Value instance_start_time is now always zero, useful to convert if not
	#XXX zero in old nodes?

	$self->couch->call(GET => $self->_pathToDB($part ? '_partition/'.uri_escape($part) : undef),
		$self->couch->_resultsConfig(\%args,
			on_values => sub { $self->__detailsValues(@_) },
		),
	);
}

=method create %options
 [CouchDB API "PUT /{db}"]

Create a new database.  The result object will have code C<HTTP_CREATED> when the
database is successfully created.  When the database already exists, it
returns C<HTTP_PRECONDITION_FAILED> and an error in the body.

Options: C<partitioned> (bool), C<q> (number of shards, default 8), and C<n> (number
of replicas, defaults to 3).
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

=example be sure a database does not exist
Any call returns a success, which you should test; there are many reasons
why they may fail.  So, for any call, you should write like this:

  my $r = $couch->db('test')->remove;
  $r or error "Cannot remove database 'test'; $r";

However, in this case you may not want to cast an error at reply code 404
(not found). Away means away.  So, this works:

  $r && $r->code != 404 or error $r;

  use HTTP::Status  qw(HTTP_NOT_FOUND);
  $r && $r->code != HTTP_NOT_FOUND or error $r;
  
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
 [CouchDB API "GET /{db}/_changes", TODO]
 [CouchDB API "POST /{db}/_changes", TODO]

Feed of changes made on this database.
=cut

sub changes { ... }

=method compact %options
 [CouchDB API "POST /{db}/_compact"]
 [CouchDB API "POST /{db}/_compact/{ddoc}", UNTESTED]

Instruct the database files to be compacted now.  By default, the data gets
compacted on unexpected moments.

=option  design $design|$ddocid
=default design C<undef>
Compact all indexes related to this design document, instead.
=cut

sub compact(%)
{	my ($self, %args) = @_;
	my $path = $self->_pathToDB('_compact');

	if(my $ddoc = delete $args{design})
	{	$path .= '/' . (blessed $ddoc ? $ddoc->id :$ddoc);
	}

	$self->couch->call(POST => $path,
		send  => { },
		$self->couch->_resultsConfig(\%args),
	);
}

=method ensureFullCommit %options
 [CouchDB API "POST /{db}/_ensure_full_commit", deprecated 3.0.0]

Support for old replicators.
=cut

sub __ensure($$)
{	my ($self, $result, $raw) = @_;
	return $raw unless $raw->{instance_start_time};  # exists && !=0
	my $v = { %$raw };
	$self->couch->toPerl($v, epoch => qw/instance_start_time/);
	$v;
}

sub ensureFullCommit(%)
{	my ($self, %args) = @_;

	$self->couch->call(POST => $self->_pathToDB('_ensure_full_commit'),
		deprecated => '3.0.0',
		send       => { },
		$self->couch->_resultsConfig(\%args,
			on_values => sub { $self->__ensureValues(@_) },
		),
	);
}

=method purgeDocs \%plan, %options
 [CouchDB API "POST /{db}/_purge", UNTESTED]

Remove selected document revisions from the database.

A deleted document is only marked as being deleted, but exists until
purge.  There must be sufficient time between deletion and purging,
to give replication a chance to distribute the fact of deletion.
=cut

sub purgeDocs($%)
{	my ($self, $plan, %args) = @_;

	#XXX looking for smarter behavior here, to construct a plan.
	my $send = $plan;

	$self->couch->call(POST => $self->_pathToDB('_purge'),
		$self->couch->_resultsConfig(\%args),
	);
}

=method purgedRecordsLimit %options
 [CouchDB API "GET /{db}/_purged_infos_limit", UNTESTED]

Returns the soft maximum number of records kept about deleting records.
=cut

#XXX seems not really a useful method.

sub purgedRecordsLimit(%)
{	my ($self, %args) = @_;

	$self->couch->call(GET => $self->_pathToDB('_purged_infos_limit'),
		$self->couch->_resultsConfig(\%args),
	);
}

=method purgedRecordsLimitSet $limit, %options
 [CouchDB API "PUT /{db}/_purged_infos_limit", UNTESTED]

Set a new soft limit.  The default is 1000.
=cut

#XXX attribute of database creation

sub purgedRecordsLimitSet($%)
{	my ($self, $value, %args) = @_;

	$self->couch->call(PUT => $self->_pathToDB('_purged_infos_limit'),
		send => int($value),
		$self->couch->_resultsConfig(\%args),
	);
}

=method purgeUnusedViews %options
 [CouchDB API "POST /{db}/_view_cleanup", UNTESTED]

Removes view files that are not used by any design document.
=cut

sub purgeUnusedViews(%)
{	my ($self, %args) = @_;

	#XXX nothing to send?
	$self->couch->call(POST => $self->_pathToDB('_view_cleanup'),
		$self->couch->_resultsConfig(\%args),
	);
}

=method revisionsMissing \%plan, %options
 [CouchDB API "POST /{db}/_missing_revs", UNTESTED]

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
 [CouchDB API "POST /{db}/_revs_diff", UNTESTED]

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
 [CouchDB API "GET /{db}/_revs_limit", UNTESTED]

Returns the limit of historical revisions to store for a single document
in the database.
=cut

#XXX seems not really a useful method.

sub revisionLimit(%)
{	my ($self, %args) = @_;

	$self->couch->call(GET => $self->_pathToDB('_revs_limit'),
		$self->couch->_resultsConfig(\%args),
	);
}

=method revisionLimitSet $limit, %options
 [CouchDB API "PUT /{db}/_revs_limit", UNTESTED]

Sets the limit of historical revisions to store for a single document
in the database.  The default is 1000.
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
=section Indexes

Three indexes exist:

=over 4
=item * json (Mango)
=item * text (Lucene via Cousteau, phased out)
=item * nouveau (Lucene via Nouveau, since 3.4.1)
=back

The details about each index are stored in design documents.  You may have
more than one index per design document, but any change to such document
will force a rebuild of all other indices in the same file.

=method design [$ddocid|$ddoc|undef]
Returns the M<Couch::DB::Design> object which manages a design document.
The document will not be read until an explicit call to C<get()>.
The C<$ddocid> may start with C<_design/> which will be ignored.
=cut

sub design($)
{	my ($self, $which) = @_;

	return $which if blessed $which && $which->isa('Couch::DB::Design');
	Couch::DB::Design->new(id => $which, db => $self);
}

=method designs [\%search|\@%search, %options]
 [CouchDB API "GET /{db}/_design_docs", UNTESTED]
 [CouchDB API "POST /{db}/_design_docs", UNTESTED]
 [CouchDB API "POST /{db}/_design_docs/queries", UNTESTED]

Pass one or more %search queries to be run.  The default returns all designs.
The search query looks very much like a generic view search, but a few
parameters are added and missing.

If there are searches, then C<GET> is used, otherwise the C<POST> version.
The returned structure depends on the searches and the number of searches.

Rows are supported.
=cut

sub __designsPrepare($$$)
{	my ($self, $method, $data, $where) = @_;
	$method eq 'POST' or panic;
	my $s     = +{ %$data };

	# Very close to a view search, but not equivalent.  At least: according to the
	# API documentation :-(
	$self->couch
		->toJSON($s, bool => qw/conflicts descending include_docs inclusive_end update_seq/)
		->toJSON($s, int  => qw/limit skip/);
	$s;
}

sub __designsRow($$%)
{	my ($self, $result, $index, %args) = @_;
	my $answer = $result->answer->{rows}[$index] or return;
	my $values = $result->values->{rows}[$index];

	  ( answer    => $answer,
		values    => $values,
		ddocdata  => $values->{doc},
		docparams => { db => $self },
	  );
}

sub designs(;$%)
{	my ($self, $search, %args) = @_;
	my $couch   = $self->couch;
	my @search  = flat $search;

	my ($method, $path, $send) = (GET => $self->_pathToDB('_design_docs'), undef);
	if(@search)
	{	$method = 'POST';
	 	my @s   = map $self->__designsPrepare($method, $_), @search;

		if(@search==1)
		{	$send  = $s[0];
		}
		else
		{	$send  = +{ queries => \@s };
			$path .= '/queries';
		}
	}

	$self->couch->call($method => $path,
		($send ? (send => $send) : ()),
		$couch->_resultsConfig(\%args,
			on_row => sub { $self->__designsRow(@_, queries => scalar(@search)) },
		),
	);
}

=method indexes %options
 [CouchDB API "GET /{db}/_index"]

Collect all indexes for the database.  This command supports rows.
=cut

sub __indexesRow($$%)
{	my ($self, $result, $index, %args) = @_;
	my $answer = $result->answer->{indexes}[$index] or return ();

	  (	answer => $answer,
		values => $result->values->{indexes}[$index],
	  );
}

sub __indexesValues()
{	my ($self, $raw) = @_;
	my %values = %$raw;   # deep not needed (yes)
	$self->couch->toPerl(\%values, bool => qw/partitioned/);
	$values{design} = $self->design($values{ddoc}) if $values{ddoc};
	\%values;
}

sub indexes(%)
{	my ($self, %args) = @_;

	$self->couch->call(GET => $self->_pathToDB('_index'),
		$self->couch->_resultsConfig(\%args,
			on_values => sub { $self->__indexesValues(@_) },
			on_row    => sub { $self->__indexesRow(@_) },
		),
	);
}

=method search $ddoc, $index, [\%search, %options]
Run a search (Mango or text) on the database.  The search base is
described in the C<$index> in design document C<$ddoc>.  The design
document may be specified as id or object.

=example search

  # Twice the same
  my $r = $db->search(myddoc => myindex => \%search);
  my $r = $db->design('myddoc')->search(myindex => \%search);

=cut

sub search($$;$%)
{	my ($self, $ddoc, $index, $search, %args) = @_;
	$self->design($ddoc)->search($index, $search, %args);
}

#-------------
=section Handling documents

=method doc $docid, %options
Returns a M<Couch::DB::Document> for this C<$docid>.  Be aware that this
does not have any interaction with the CouchDB server.  Only when you
call actions, like C<exists()>, on that object, you can see the status and
content of the document.

All C<%options> are passed to M<Couch::DB::Database::new()>.  Of course, you do
not need to pass the C<Couch::DB::Database> object explicitly.
=cut

sub doc($%)
{	my ($self, $id) = @_;
	Couch::DB::Document->new(id => $id, db => $self, @_);
}

=method saveBulk \@docs, %options
 [CouchDB API "POST /{db}/_bulk_docs"]

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

=option  issues CODE
=default issues C<undef>
By default, missing reports are ignored.  When a CODE is specified, it will be called
with the result object, the failing document, and named parameters error details.
The %details contain the C<error> type, the error C<reason>, and the optional
C<deleting> boolean boolean.

=example bulk adding documents
  my $doc1 = $db->doc('new1', content => $data1);
  my $doc2 = $db->doc('new2', content => $data2);
  $db->saveBulk([$doc1, $doc2]);

=example deleting a document
Can be combined with added documents.

  my $del1 = $db->doc('victim');
  $db->saveBulk([], delete => $del1);

=example for error handling
  sub handle($result, $doc, %details) { ... }
  $db->saveBulk(@save, issues => \&handle);
=cut

sub __bulk($$$$)
{	my ($self, $result, $saves, $deletes, $issues) = @_;
	$result or return;

	my %saves   = map +($_->id => $_), @$saves;
	my %deletes = map +($_->id => $_), @$deletes;

	foreach my $report (@{$result->values})
	{	my $id     = $report->{id};
		my $delete = exists $deletes{$id};
		my $doc    = delete $deletes{$id} || delete $saves{$id}
			or panic "missing report for updated $id";

		if($report->{ok})
		{	$doc->_saved($id, $report->{rev});
			$doc->_deleted($report->{rev}) if $delete;
		}
		else
		{	$issues->($result, $doc, +{ %$report, delete => $delete });
		}
	}

	$issues->($result, $saves{$_},
		+{ error => 'missing', reason => "The server did not report back on saving $_." }
	) for keys %saves;

	$issues->($result, $deletes{$_},
		+{ error => 'missing', reason => "The server did not report back on deleting $_.", delete => 1 }
	) for keys %deletes;
}

sub saveBulk($%)
{	my ($self, $docs, %args) = @_;
	my $couch   = $self->couch;
	my $issues  = delete $args{issues} || sub {};

	my @plan;
	foreach my $doc (@$docs)
	{	my $rev     = $doc->rev;
		my %plan    = %{$doc->revision($rev)};
		$plan{_id}  = $doc->id;
		$plan{_rev} = $rev if $rev ne '_new';
		push @plan, \%plan;
	}

	my @deletes = flat delete $args{delete};
	foreach my $del (@deletes)
	{	push @plan, +{ _id => $del->id, _rev => $del->rev, _deleted => JSON::PP::true };
		$couch->toJSON($plan[-1], bool => qw/_delete/);
	}

	@plan or error __x"need at least on document for bulk processing.";
	my $send    = +{ docs => \@plan };

	$send->{new_edits} = delete $args{new_edits} if exists $args{new_edits};  # default true
	$couch->toJSON($send, bool => qw/new_edits/);

	$couch->call(POST => $self->_pathToDB('_bulk_docs'),
		send     => $send,
		$couch->_resultsConfig(\%args,
			on_final => sub { $self->__bulk($_[0], $docs, \@deletes, $issues) },
		),
	);
}

=method inspectDocs \@docs, %options
 [CouchDB API "POST /{db}/_bulk_get", UNTESTED]

Return information on multiple documents at the same time.

=option  revs BOOLEAN
=default revs C<false>
Include the revision history of each document.
=cut

sub inspectDocs($%)
{	my ($self, $docs, %args) = @_;
	my $couch = $self->couch;

	my $query;
	$query->{revs} = delete $args{revs} if exists $args{revs};
	$couch->toQuery($query, bool => qw/revs/);

	@$docs or error __x"need at least on document for bulk query.";

	#XXX what does "conflicted documents mean?
	#XXX what does "a": 1 mean in its response?

	$self->couch->call(POST => $self->_pathToDB('_bulk_get'),
		query => $query,
		send  => { docs => $docs },
		$couch->_resultsConfig(\%args),
	);
}

=method allDocs [\%query|\@queries], %options]
 [CouchDB API "GET /{db}/_all_docs"]
 [CouchDB API "POST /{db}/_all_docs"]
 [CouchDB API "POST /{db}/_all_docs/queries", UNTESTED]
 [CouchDB API "GET /{db}/_local_docs", UNTESTED]
 [CouchDB API "POST /{db}/_local_docs", UNTESTED]
 [CouchDB API "POST /{db}/_local_docs/queries", UNTESTED]
 [CouchDB API "GET /{db}/_partition/{partition}/_all_docs", UNTESTED]

Get the documents, optionally limited by a view.  If there are queries,
then C<POST> is used, otherwise the C<GET> endpoint.

The returned structure depends on the C<%query> and the number of
C<@queries> (an ARRAY of query HASHes).  This method support pagination,
but only when a single query is given.

The preferred way to use this method with a C<view>, is by calling
M<Couch::DB::Design::viewDocs()> on its C<design> object.

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

=option  design $design|$ddocid
=default design C<undef>
Usually called via M<Couch::DB::Design::viewDocs()>.

=example getting all documents in a database
Be warned: doing it this way is memory hungry: better use paging.

  my $all  = $couch->db('users')->allDocs({include_docs => 1}, all => 1);
  my $rows = $all->page;
  my @docs = map $_->doc, @$rows;
=cut

sub __allDocsRow($$%)
{	my ($self, $result, $index, %args) = @_;
	my $answer = $result->answer->{rows}[$index] or return ();
	my $values = $result->values->{rows}[$index];

	 (	answer    => $answer,
		values    => $values,
		docdata   => $values->{doc},
		docparams => { local => $args{local}, db => $self },
	 );
}

sub allDocs(;$%)
{	my ($self, $search, %args) = @_;
	my $couch  = $self->couch;

	my @search = flat $search;
	my $part   = delete $args{partition};
	my $local  = delete $args{local};
	my $view   = delete $args{view};
	my $ddoc   = delete $args{design};
	my $ddocid = blessed $ddoc ? $ddoc->id : $ddoc;

	#XXX The API shows some difference in the parameter combinations, which do not
	#XXX need to be there.  For now, we produce an error for these cases.
	!$view  || $ddoc  or panic "allDocs(view) requires design document.";
	!$local || !$part or panic "allDocs(local) cannot be combined with partition.";
	!$local || !$view or panic "allDocs(local) cannot be combined with a view.";
	!$part  || @search < 2 or panic "allDocs(partition) cannot work with multiple searches.";

	my $set
	  = $local ? '_local_docs'
	  :   ($part ? '_partition/'. uri_escape($part) . '/' : '')
	    . ($view ? "_design/$ddocid/_view/". uri_escape($view) : '_all_docs');

	my $method = !@search || $part ? 'GET' : 'POST';
	my $path   = $self->_pathToDB($set);

	# According to the spec, _all_docs is just a special view.
	my @send   = map $self->_viewPrepare($method, $_, "docs search"), @search;

	my @params;
	if($method eq 'GET')
	{	@send < 2 or panic "Only one search with docs(GET)";
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
		$couch->_resultsPaging(\%args,
			on_row   => sub { $self->__allDocsRow(@_, local => $local, queries => scalar(@search)) },
		),
	);
}

my @docview_bools = qw/
	conflicts descending group include_docs attachments att_encoding_info
	inclusive_end reduce sorted stable update_seq
/;

# Handles standard view/_all_docs/_local_docs queries.
sub _viewPrepare($$$)
{	my ($self, $method, $data, $where) = @_;
	my $s     = +{ %$data };
	my $couch = $self->couch;

	# Main doc in 1.5.4.  /{db}/_design/{ddoc}/_view/{view}
	if($method eq 'GET')
	{	$couch
			->toQuery($s, bool => @docview_bools)
			->toQuery($s, json => qw/endkey end_key key keys start_key startkey/);
	}
	else
	{	$couch
			->toJSON($s, bool => @docview_bools)
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

=method find [\%search, %options]
 [CouchDB API "POST /{db}/_find"]
 [CouchDB API "POST /{db}/_partition/{partition_id}/_find", UNTESTED]

Search the database for matching documents, using Mango selectors.
The documents are always included in the reply, including attachment
information.  Attachment data is not included.

The default search will select everything (uses a blank HASH as required
C<selector>).  By default, the number of results has a C<limit> of 25.
Pass C<limit> and C<skip> in C<%options> with other pagination control,
not in C<%search>.

=option  partition $partition
=default partition C<undef>

=example of find() with a single query
  my $result = $couch->find or die;
  my @docs   = $result->docs;  # Couch::DB::Documents
  foreach my $doc (@docs) { ... }
=cut

sub __findRow($$%)
{	my ($self, $result, $index, %args) = @_;
	my $answer = $result->answer->{docs}[$index] or return ();
	my $values = $result->values->{docs}[$index];

	(	answer    => $answer,
		values    => $values,
		docdata   => $values,
		docparams => { local => $args{local}, db => $self },
	 );
}

sub find($%)
{	my ($self, $search, %args) = @_;

	my $part   = delete $args{partition};
	$search->{selector} ||= {};

	my $path   = $self->_pathToDB;
	$path     .= '/_partition/'. uri_escape($part) if $part;

	$self->couch->call(POST => "$path/_find",
		send   => $self->_findPrepare(POST => $search),
		$self->couch->_resultsPaging(\%args,
			on_row => sub { $self->__findRow(@_) },
		),
	);
}

sub _findPrepare($$)
{	my ($self, $method, $data, $where) = @_;
	my $s = +{ %$data };  # no nesting

	$method eq 'POST' or panic;

	$self->couch
		->toJSON($s, bool => qw/conflicts update stable execution_stats/)
		->toJSON($s, int  => qw/limit sip r/)
		#XXX Undocumented when this got deprecated
		->check(exists $s->{stale}, deprecated => '3.0.0', 'Database find(stale)');

	$s;
}

=method findExplain \%search, %options
 [CouchDB API "POST /{db}/_explain"]
 [CouchDB API "POST /{db}/_partition/{partition_id}/_explain", UNTESTED]

Explain how the a search will be executed.

=option  partition $partition
=default partition C<undef>
=cut

sub findExplain(%)
{	my ($self, $search, %args) = @_;
	my $part = delete $args{partition};
	$search->{selector} ||= {};

	my $path  = $self->_pathToDB;
	$path    .= '/_partition/' . uri_escape($part) if $part;

	$self->couch->call(POST => "$path/_explain",
		send => $self->_findPrepare(POST => $search),
		$self->couch->_resultsConfig(\%args),
	);
}

1;
