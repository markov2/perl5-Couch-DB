# SPDX-FileCopyrightText: 2024 Mark Overmeer <mark@overmeer.net>
# SPDX-License-Identifier: Artistic-2.0

package Couch::DB::Design;
use parent 'Couch::DB::Document';

use Couch::DB::Util;

use Log::Report 'couch-db';

use URI::Escape  qw/uri_escape/;
use Scalar::Util qw/blessed/;

=chapter NAME

Couch::DB::Design - handle design documents

=chapter SYNOPSIS

=chapter DESCRIPTION

In CouchDB, design documents provide the main interface for building
a CouchDB application. The design document defines the views used to
extract information from CouchDB through one or more views.

Design documents behave just like your own documents, but occupy the
C<_design/> namespace in your database.  A bunch of the methods are
therefore exactly the same as the methods in base-class
M<Couch::DB::Document>.

=chapter METHODS

=section Constructors

=c_method new %options
=cut

#-------------
=section Accessors
=cut

sub _pathToDoc(;$) { $_[0]->db->_pathToDB('_design/' . $_[0]->id) . (defined $_[1] ? '/' . uri_escape $_[1] : '')  }

#-------------
=section Document in the database
All methods below are inherited from standard documents.  Their call URI
differs, but their implementation is the same.  On the other hand: they
add interpretation on fields which do not start with '_'.

=method exists %option
 [CouchDB API "HEAD /{db}/_design/{ddoc}"]

Returns the HTTP Headers containing a minimal amount of information about the
specified design document.

=method create \%data, %options
Create a new design document.  Design documents do not use generated ids,
so: you have to have specified one with M<new(id)>.  Therefore, this method
is equivalent to M<update()>.
=cut

sub create($%)
{	my $self = shift;
	defined $self->id
		or error __x"Design documents do not generate an id by themselves.";
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
{	my ($self, $data, $args) = @_;
	$self->couch
		->toJSON($data, bool => qw/autoupdate/)
		->check($data->{lists}, deprecated => '3.0.0', 'DesignDoc create() option list')
		->check($data->{lists}, removed    => '4.0.0', 'DesignDoc create() option list')
		->check($data->{show},  deprecated => '3.0.0', 'DesignDoc create() option show')
		->check($data->{show},  removed    => '4.0.0', 'DesignDoc create() option show')
		->check($data->{rewrites}, deprecated => '3.0.0', 'DesignDoc create() option rewrites');

	#XXX Do we need more parameter conversions in the nested queries?

	$self->SUPER::create($data, $args);
}

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

#-------------
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

#-------------
=section Indexes

=method createIndex \%filter, %options
 [CouchDB API "POST /{db}/_index", UNTESTED]

Create/confirm an index on the database.  When you like a generated design
document name, you can use M<Couch::DB::Database::createIndex()>.
=cut

sub createIndex($%)
{	my ($self, $filter, %args) = @_;
	$self->db->createIndex(%args, design => $self);
}

=method deleteIndex $index, %options
 [CouchDB API "DELETE /{db}/_index/{designdoc}/json/{name}", UNTESTED]

Remove an index from this design document.
=cut

sub deleteIndex($%)
{	my ($self, $ddoc, $index, %args) = @_;
	$self->couch->call(DELETE => $self->db->_pathToDB('_index/' . uri_escape($self->id) . '/json/' . uri_escape($index)),
		$self->couch->_resultsConfig(\%args),
	);
}

=method indexFind $index, %options
 [CouchDB API "GET /{db}/_design/{ddoc}/_search/{index}", UNTESTED]

Executes a search request against the named $index.

When you have used C<include_docs>, then the documents can be found in
C<< $result->values->{docs} >>, not C<< {rows} >>.

=option  search HASH
=default search {}
The search query.
=cut

sub __indexValues($$%)
{	my ($self, $result, $raw, %args) = @_;
	delete $args{full_docs} or return $raw;

	my $values = +{ %$raw };
	$values->{docs} = delete $values->{rows};
	$self->db->__toDocs($result, $values, db => $self->db);
	$values;
}

sub indexFind($%)
{	my ($self, $index, %args) = @_;
	my $couch = $self->couch;

	my $search  = delete $args{search} || {};
	my $query   = +{ %$search };

	# Everything into the query :-(  Why no POST version?
	$couch
		->toQuery($query, json => qw/counts drilldown group_sort highlight_fields include_fields ranges sort/)
		->toQuery($query, int  => qw/highlight_number highlight_size limit/)
		->toQuery($query, bool => qw/include_docs/);

	$couch->call(GET => $self->_pathToDDoc('_search/' . uri_escape $index),
		introduced => '3.0.0',
		query      => $query,
		$couch->_resultsPaging(\%args,
			on_values  => sub { $self->__indexValues($_[0], $_[1], db => $self->db, full_docs => $search->{include_docs}) },
		),
	);
}

=method indexDetails $index, %options
 [CouchDB API "GET /{db}/_design/{ddoc}/_search_info/{index}", UNTESTED]

Returns metadata for the specified search index.
=cut

sub indexDetails($%)
{	my ($self, $index, %args) = @_;

	$self->couch->call(GET => $self->_pathToDDoc('_search_info/' . uri_escape($index)),
		introduced => '3.0.0',
		$self->couch->_resultsConfig(\%args),
	);
}

#-------------
=section Views

=method viewSearch $view, [\%search|\@%search), %options]
 [CouchDB API "GET /{db}/_design/{ddoc}/_view/{view}", UNTESTED]
 [CouchDB API "POST /{db}/_design/{ddoc}/_view/{view}", UNTESTED]
 [CouchDB API "POST /{db}/_design/{ddoc}/_view/{view}/queries", UNTESTED]
 [CouchDB API "GET /{db}/_partition/{partition}/_design/{ddoc}/_view/{view}", UNTESTED]

Executes the specified view function.

This work is handled in M<Couch::DB::Database::search()>.  See that method for
C<%options> and results.
=cut

sub viewSearch($;$%)
{	my ($self, $view, $search, %args) = @_;
	$self->db->search($search, view => $view, design => $self, %args);
}

#-------------
=section Functions

=method show $function, [$doc|$docid|undef, %options]
 [CouchDB API "GET /{db}/_design/{ddoc}/_show/{func}", deprecated 3.0, removed 4.0, UNTESTED]
 [CouchDB API "POST /{db}/_design/{ddoc}/_show/{func}", deprecated 3.0, removed 4.0, UNTESTED]
 [CouchDB API "GET /{db}/_design/{ddoc}/_show/{func}/{docid}", deprecated 3.0, removed 4.0, UNTESTED]
 [CouchDB API "POST /{db}/_design/{ddoc}/_show/{func}/{docid}", deprecated 3.0, removed 4.0, UNTESTED]

Apply show $function on the document, as specified by id or object.  By default or explicit
undef, a C<null> document will be used.
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

Executes a list function against the view.

=option  view_ddoc $ddoc|$ddocid
=default view_ddoc C<undef>
When the $view resides in a differen design.
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
