#oodist: *** DO NOT USE THIS VERSION FOR PRODUCTION ***
#oodist: This file contains OODoc-style documentation which will get stripped
#oodist: during its release in the distribution.  You can use this file for
#oodist: testing, however the code of this development version may be broken!

package Couch::DB::Design;
use parent 'Couch::DB::Document';

use warnings;
use strict;

use Couch::DB::Util;

use Log::Report 'couch-db';

use URI::Escape  qw/uri_escape/;
use Scalar::Util qw/blessed/;

my $id_generator;

#--------------------
=chapter NAME

Couch::DB::Design - handle design documents

=chapter SYNOPSIS

  my $ddoc = Couch::DB::Design->new(id => 'myname', db => $db);
  my $ddoc = $db->design('myname');          # same
  my $ddoc = $db->design('_design/myname');  # same
  my $ddoc = $db->design;  # id generated

  my $results = $db->design('d')->search('i', ...) or die;
  my $results = $db->search(d => 'i', ...); # same
  my $results = $db->search($ddoc => 'i', ...); # same


=chapter DESCRIPTION

In CouchDB, design documents provide the main interface for building
a CouchDB application. The design document defines the views used to
extract information from CouchDB through one or more views.

Design documents behave just like your own documents, but occupy the
C<_design/> namespace in your database.  A bunch of the methods are
therefore exactly the same as the methods in base-class
Couch::DB::Document.

=chapter METHODS

=section Constructors

=c_method new %options

=default id generated
If no id is passed, then one gets generated: a UUID is requested from
the server.  You may also use a local generator via UUID::URandom
or Data::UUID, which is (of course) more efficient.
=cut

sub init($)
{	my ($self, $args) = @_;
	my $which = $args->{id} || $id_generator->($args->{db} or panic);
	my ($id, $base) = $which =~ m!^_design/(.*)! ? ($which, $1) : ("_design/$which", $which);
	$args->{id} = $id;

	$self->SUPER::init($args);
	$self->{CDD_base} = $base;
	$self;
}

#--------------------
=section Accessors

=c_method setIdGenerator CODE
When a design document is created without explicit C<id>, that will
get generated.  By default, this is done by requesting a fresh UUID
from the server.  You may change this into some local random collission
free id generator for better performance.

The CODE is called with the daabase object as only parameter.
=cut

$id_generator = sub ($) { $_[0]->couch->freshUUID };
sub setIdGenerator($) { $id_generator = $_[1] }

=method idBase
Various calls need the C<id> without the C<_design>.  Whether the
full document id for the design document or only the unique part
is required/given is confusing.  This method returns the unique
part.
=cut

sub idBase() { $_[0]->{CDD_base} }

#--------------------
=section Document in the database
All methods below are inherited from standard documents.  Their call URI
differs, but their implementation is the same.  On the other hand: they
add interpretation on fields which do not start with '_'.

=method exists %option
  [CouchDB API "HEAD /{db}/_design/{ddoc}"]

Returns the HTTP Headers containing a minimal amount of information about the
specified design document.

=method create \%data, %options
  [CouchDB API "POST /{db}/_index", UNTESTED]
Create a new design document.

In Couch::DB, the client-side, not the server, generates ids.  Therefore,
this method is equivalent to M<update()>.
=cut

sub create($%)
{	my $self = shift;
	$self->update(@_);
}

=method update \%data, %options
  [CouchDB API "PUT /{db}/_design/{ddoc}"]

Options C<filters>, C<lists>, C<shows>, and C<updates> are HASHes which
map names to fragments of code written in programming language C<language>
(usually erlang or javascript).

Options C<lists>, C<show>, and C<rewrites> (query redirection) are
deprecated since 3.0, and are removed from 4.0.
=cut

sub update($%)
{	my ($self, $data, %args) = @_;
	$data->{_id} = $self->id;

	$self->couch
		->toJSON($data, bool => qw/autoupdate/)
		->check($data->{lists}, deprecated => '3.0.0', 'DesignDoc create() option list')
		->check($data->{lists}, removed    => '4.0.0', 'DesignDoc create() option list')
		->check($data->{show},  deprecated => '3.0.0', 'DesignDoc create() option show')
		->check($data->{show},  removed    => '4.0.0', 'DesignDoc create() option show')
		->check($data->{rewrites}, deprecated => '3.0.0', 'DesignDoc create() option rewrites');

	#XXX Do we need more parameter conversions in the nested queries?

	$self->SUPER::create($data, %args);
}

# get/delete/etc. are simply produced by extension of the _pathToDoc() which
# adds "_design/" to the front of the path.
=method get %options
  [CouchDB API "GET /{db}/_design/{ddoc}"]

=method delete %options
  [CouchDB API "DELETE /{db}/_design/{ddoc}"]

=method cloneInto $doc, %options
  [CouchDB API "COPY /{db}/_design/{ddoc}"]

=method appendTo $doc, %options
  [CouchDB API "COPY /{db}/_design/{ddoc}"]

=method details %options
  [CouchDB API "GET /{db}/_design/{ddoc}/_info", UNTESTED]

Obtains information about the specified design document, including the
index, index size and current status of the design document and associated
index information.
=cut

sub details(%)
{	my ($self, %args) = @_;

	$self->couch->call(GET => $self->_pathToDoc('_info'),
		$self->couch->_resultsConfig(\%args),
	);
}

#--------------------
=section Attachments

=method attExists $name, %options
  [CouchDB API "HEAD /{db}/_design/{ddoc}/{attname}"]

=method attLoad $name, %options
  [CouchDB API "GET /{db}/_design/{ddoc}/{attname}" ]

=method attSave $name, $data, %options
  [CouchDB API "PUT /{db}/_design/{ddoc}/{attname}" ]

=method attDelete $name, %options
  [CouchDB API "DELETE /{db}/_design/{ddoc}/{attname}" ]

=cut

#--------------------
=section Indexes (indices)

=method createIndex \%config, %options
  [CouchDB API "POST /{db}/_index", UNTESTED]

Create an index on the database.  If the name already exists and the
configuration is different, then the index be get regenerated.
=cut

sub createIndex($%)
{	my ($self, $config, %args) = @_;

	my $send  = +{ %$config, ddoc => $self->id };
	my $couch = $self->couch;
	$couch->toJSON($send, bool => qw/partitioned/);

	$couch->call(POST => $self->db->_pathToDB('_index'),
		send => $send,
		$couch->_resultsConfig(\%args),
	);
}

=method deleteIndex $index, %options
  [CouchDB API "DELETE /{db}/_index/{design_doc}/json/{name}", UNTESTED]

Remove an index from this design document.
=cut

sub deleteIndex($%)
{	my ($self, $ddoc, $index, %args) = @_;
	my $id = $self->idBase;  # id() would also work
	$self->couch->call(DELETE => $self->db->_pathToDB("_index/$id/json/" . uri_escape($index)),
		$self->couch->_resultsConfig(\%args),
	);
}

=method search $index, [\%search, %options]
  [CouchDB API "GET /{db}/_design/{ddoc}/_search/{index}", UNTESTED]

Executes a (text) search request against the named $index.  The default
%search contains the whole index.  When the search contains
C<include_docs>, then full docs are made available.

(Of course) this command supports paging.

=example return full index all as rows
  my $d    = $db->design('d');
  my $rows = $d->search('i', {}, all => 1)->page;

  my $search = +{ include_docs => 1 };
  my @docs = $d->search('i', \%search, all => 1)->pageDocs;

=cut

sub __searchRow($$$%)
{	my ($self, $result, $index, $column, %args) = @_;
	my $answer = $result->answer->{rows}[$index] or return ();
	my $values = $result->values->{rows}[$index];

	  (	answer    => $answer,
		values    => $values,
		docdata   => $args{full_docs} ? $values : undef,
		docparams => { db => $self },
	  );
}

sub search($$%)
{	my ($self, $index, $search, %args) = @_;
	my $query = defined $search ? +{ %$search } : {};

	# Everything into the query :-(  Why no POST version?
	my $couch = $self->couch;
	$couch
		->toQuery($query, json => qw/counts drilldown group_sort highlight_fields include_fields ranges sort/)
		->toQuery($query, int  => qw/highlight_number highlight_size limit/)
		->toQuery($query, bool => qw/include_docs/);

	$couch->call(GET => $self->_pathToDoc('_search/' . uri_escape $index),
		introduced => '3.0.0',
		query      => $query,
		$couch->_resultsPaging(\%args,
			on_row => sub { $self->__searchRow(@_, full_docs => $search->{include_docs}) },
		),
	);
}

=method indexDetails $index, %options
  [CouchDB API "GET /{db}/_design/{ddoc}/_search_info/{index}", UNTESTED]

Returns metadata for the specified search index.
=cut

sub indexDetails($%)
{	my ($self, $index, %args) = @_;

	$self->couch->call(GET => $self->_pathToDoc('_search_info/' . uri_escape($index)),
		introduced => '3.0.0',
		$self->couch->_resultsConfig(\%args),
	);
}

#--------------------
=section Views

=method viewDocs $view, [\%search|\@%search], %options]
  [CouchDB API "GET /{db}/_design/{ddoc}/_view/{view}", UNTESTED]
  [CouchDB API "POST /{db}/_design/{ddoc}/_view/{view}", UNTESTED]
  [CouchDB API "POST /{db}/_design/{ddoc}/_view/{view}/queries", UNTESTED]
  [CouchDB API "GET /{db}/_partition/{partition_id}/_design/{ddoc}/_view/{view}", UNTESTED]

Executes the specified view function.

This work is handled in M<Couch::DB::Database::allDocs()>.  See that method for
%options and results.

=example
  my %search;
  my $c = $db->design('people')->viewDocs(customers => \%search, all => 1);
  my $hits = $c->page;

  my %search = (design => 'people', view => 'customers');
  my $c = $db->allDocs(\%search, all => 1);
=cut

sub viewDocs($;$%)
{	my ($self, $view, $search, %args) = @_;
	$self->db->allDocs($search, view => $view, design => $self, %args);
}

=method compactViews %options
  [CouchDB API "POST /{db}/_compact/{ddoc}", UNTESTED]

Start the compacting (optimization) of all views in this design document.
See M<Couch::DB::Database::compactViews()> to start them for all design
documents at once.
=cut

sub compactViews(%)
{	my ($self, %args) = @_;

	$self->couch->call(POST => $self->_pathToDB('_compact/', uri_escape($self->baseId)),
		$self->couch->_resultsConfig(\%args),
	);
}

#--------------------
=section Functions

=method show $function, [$doc|$docid|undef, %options]
  [CouchDB API "GET /{db}/_design/{ddoc}/_show/{func}", deprecated 3.0, removed 4.0, UNTESTED]
  [CouchDB API "POST /{db}/_design/{ddoc}/_show/{func}", deprecated 3.0, removed 4.0, UNTESTED]
  [CouchDB API "GET /{db}/_design/{ddoc}/_show/{func}/{docid}", deprecated 3.0, removed 4.0, UNTESTED]
  [CouchDB API "POST /{db}/_design/{ddoc}/_show/{func}/{docid}", deprecated 3.0, removed 4.0, UNTESTED]

Apply show $function on the document, as specified by $docid or document object.  By
default or explicit undef, a "null" document will be used.
=cut

sub show($;$%)
{	my ($self, $function, $doc, %args) = @_;
	my $path = $self->_pathToDoc('_show/'.uri_escape($function));
	$path .= '/' . (blessed $doc ? $doc->id : $doc) if defined $doc;

	$self->couch->call(GET => $path,
		deprecated => '3.0.0',
		removed    => '4.0.0',
		$self->couch->_resultsConfig(\%args),
	);
}

=method list $function, $view, %options
  [CouchDB API "GET /{db}/_design/{ddoc}/_list/{func}/{view}", deprecated 3.0, removed 4.0, UNTESTED]
  [CouchDB API "POST /{db}/_design/{ddoc}/_list/{func}/{view}", deprecated 3.0, removed 4.0, UNTESTED]
  [CouchDB API "GET /{db}/_design/{ddoc}/_list/{func}/{other-ddoc}/{view}", deprecated 3.0, removed 4.0, UNTESTED]
  [CouchDB API "POST /{db}/_design/{ddoc}/_list/{func}/{other-ddoc}/{view}", deprecated 3.0, removed 4.0, UNTESTED]

Executes a list function against the $view.

=option  view_ddoc $ddoc|$ddocid
=default view_ddoc undef
When the $view resides in a different design.
=cut

sub list($$%)
{	my ($self, $function, $view, %args) = @_;

	my $other = defined $args{view_ddoc} ? '/'.delete $args{view_ddoc} : '';
	my $path = $self->_pathToDoc('_list/' . uri_escape($function) . $other . '/' . uri_escape($view));

	$self->couch->call(GET => $path,
		deprecated => '3.0.0',
		removed    => '4.0.0',
		$self->couch->_resultsConfig(\%args),
	);
}

=method applyUpdate $function, [$doc|$docid|undef, %options]
  [CouchDB API "POST /{db}/_design/{ddoc}/_update/{func}", UNTESTED]
  [CouchDB API "POST /{db}/_design/{ddoc}/_update/{func}/{docid}", UNTESTED]

See what the update function would change.  The update $function is run
on a document, specified by id or object.  By default or explicit undef,
a C<null> (missing) document will be used.
=cut

#XXX The 3.3.3 doc says /{docid} version requires PUT, but shows a POST example.
#XXX The 3.3.3post4 docs make the example patch with PUT.
#XXX The code probably says: anything except GET is okay.

sub applyUpdate($%)
{	my ($self, $function, $doc, %args) = @_;
	my $path = $self->_pathToDoc('_update/'.uri_escape($function));
	$path .= '/' . (blessed $doc ? $doc->id : $doc) if defined $doc;

	$self->couch->call(POST => $path,
		deprecated => '3.0.0',
		removed    => '4.0.0',
		send       => { },
		$self->couch->_resultsConfig(\%args),
	);
}

# [CouchDB API "ANY /{db}/_design/{ddoc}/_rewrite/{path}", deprecated 3.0, removed 4.0, UNSUPPORTED]
# The documentation of this method is really bad, and you probably should do this in your programming
# language anyway.

1;
