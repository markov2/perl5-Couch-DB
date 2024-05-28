# SPDX-FileCopyrightText: 2024 Mark Overmeer <mark@open-console.eu>
# SPDX-License-Identifier: EUPL-1.2-or-later

package Couch::DB::Design;
use parent 'Couch::DB::Document';

use Couch::DB::Util;

use Log::Report 'couch-db';

use URI::Escape  qw/uri_espace/;
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

sub init($)
{	my ($self, $args) = @_;
	$self->SUPER::init($args);
	$self;
}

#-------------
=section Accessors
=cut

sub _pathToDoc(;$) { $_[0]->db->_pathToDB('_design/' . $_[0]->id) . (defined $_[1] ? '/' . uri_espace $_[1] : '')  }

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

sub create($%)
{	my ($self, $data, $args) = @_;
	$self->couch
		->toJSON($data, bool => qw/autoupdate/)
		->check($data{lists}, deprecated => '3.0.0', 'DesignDoc create() option list'),
		->check($data{lists}, removed    => '4.0.0', 'DesignDoc create() option list'),
		->check($data{show},  deprecated => '3.0.0', 'DesignDoc create() option show'),
		->check($data{show},  removed    => '4.0.0', 'DesignDoc create() option show'),
		->check($data{rewrites}, deprecated => '3.0', 'DesignDoc create() option rewrites');

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

=method info %options
[CouchDB API "GET /{db}/_design/{ddoc}/_info", UNTESTED]
Obtains information about the specified design document, including the
index, index size and current status of the design document and associated
index information.
=cut

sub info(%)
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

=method createIndex %options
[CouchDB API "POST /{db}/_index", UNTESTED]
Create/confirm an index on the database.  By default, the index C<name> 
and the name for the design document C<ddoc> are generated.  You can
also call C<Couch::DB::createIndex()>.
=cut

sub createIndex(%)
{	my ($self, %args) = @_;
	$self->db->createIndex(%args, ddoc => $self->id);
}

=method deleteIndex $index, %options
[CouchDB API "DELETE /{db}/_index/{designdoc}/json/{name}", UNTESTED]
=cut

sub deleteIndex($%)
{	my ($self, $ddoc, $index, %args) = @_;
	$self->couch->call(DELETE => $self->db->_pathToDB('_index/' . uri_escape($self->id) . '/json/' . uri_escape($index)),
		$self->couch->_resultsConfig(\%args),
	);
}

=method findIndex $index, %options
[CouchDB API "GET /{db}/_design/{ddoc}/_search/{index}", UNTESTED]
Executes a search request against the named index.

=option  search HASH
=default search {}
=cut

# Everything into the query :-(
sub findIndex($%)
{	my ($self, $index, %args) = @_;
	my $couch = $self->couch;

	my $search = %{delete $args{search} || {}};
	$couch
		->toQuery($search, json => qw/counts drilldown group_sort highlight_fields include_fields ranges sort/)
		->toQuery($search, bool => qw/include_docs/);

	#XXX extract documents which include_docs
	$couch->call(GET => $self->_pathToDDoc('_search/' . uri_escape $index),
		introduced => '3.0',
		query      => $search,
		$couch->_resultsConfig(\%args),
	);
}

=method infoIndex $index, %options
[CouchDB API "GET /{db}/_design/{ddoc}/_search_info/{index}", UNTESTED]
=cut

sub infoIndex
{	my ($self, $index, %args) = @_;

	$self->couch->call(GET => $self->_pathToDDoc('_search_info/' . uri_escape($index)),
		introduced => '3.0',
		$self->couch->_resultsConfig(\%args),
	);
}

#-------------
=section Views

=method findView $view, %options
[CouchDB API "GET /{db}/_design/{ddoc}/_view/{view}"],
[CouchDB API "POST /{db}/_design/{ddoc}/_view/{view}", UNTESTED ]
[CouchDB API "POST /{db}/_design/{ddoc}/_view/{view}/queries", UNTESTED ]
Executes the specified view function.  The C<GET> alternative is never used.

=option  search $search|ARRAY
=default search []
=cut

sub __searchValues($$)
{	my ($result, $raw) = @_;
	#XXX When 'include_docs', then convert doc info in ::Documents with attachements.
	$raw;
}

sub findView($%)
{	my ($self, $view, %args) = @_;
	my $couch = $self->couch;

	my @search = flat delete $args{search};
	my @send;
	foreach my $search (@search)
	{	my $send = %$search;
		$couch
			->toJSON($send, bool => qw/conflicts descending group include_docs attachments
				att_encoding_info inclusive_end reducs sorted stable update_seq/)
			->toJSON($send, int  => qw/group_level limit skip/)
			->check($send{attachments}, introduced => '1.6.0', 'Search attribute "attachments"')
			->check($send{att_encoding_info}, introduced => '1.6.0', 'Search attribute "att_encoding_info"')
			->check($send{sorted}, introduced => '2.0.0', 'Search attribute "sorted"')
			->check($send{stable}, introduced => '2.1.0', 'Search attribute "stable"')
			->check($send{update}, introduced => '2.1.0', 'Search attribute "update"');
		}
		push @send, $send;
	}

	my ($method, $path, $send) = (GET => $self->_pathToDoc('_view/'.uri_escape($view)), undef);
	my $send;
	if(@send)
	{	$method = 'POST';
		if(@search==1)
		{	$send = $send[0];
		}
		else
		{	$couch->check(1, introduced => '2.2', 'Bulk queries');
			$send = +{ queries => \@send };
			$path .= '/queries';
		}
	}

	$couch->call($method => $path,
		send      => $send,
		to_values => \&__searchValues,
		$couch->_resultsConfig(\%args),
	);
}

#-------------
=section Functions

=method show $function, %options
[CouchDB API "GET /{db}/_design/{ddoc}/_show/{func}", deprecated 3.0, removed 4.0, UNTESTED],
[CouchDB API "POST /{db}/_design/{ddoc}/_show/{func}", deprecated 3.0, removed 4.0, UNTESTED],
[CouchDB API "GET /{db}/_design/{ddoc}/_show/{func}/{docid}", deprecated 3.0, removed 4.0, UNTESTED],
[CouchDB API "POST /{db}/_design/{ddoc}/_show/{func}/{docid}", deprecated 3.0, removed 4.0, UNTESTED].
Apply show $function on the document.

=option  doc $document|$docid
=default doc C<null>
Run the function on the specified document, by default a C<null> document.
=cut

sub show($%)
{	my ($self, $function, %args) = @_;
	my $path = $self->_pathToDoc('_show/'.uri_escape($function));
	if(my $doc = delete $args{doc})
	{	$path .= '/' . (blessed $doc ? $doc->id : $doc);
	}

	$couch->call(GET => $path,
		deprecated => '3.0',
		removed    => '4.0',
		$couch->_resultsConfig(\%args),
	);
}

=method list $function, $view, %options
[CouchDB API "GET /{db}/_design/{ddoc}/_list/{func}/{view}", deprecated 3.0, removed 4.0, UNTESTED],
[CouchDB API "POST /{db}/_design/{ddoc}/_list/{func}/{view}", deprecated 3.0, removed 4.0, UNTESTED],
[CouchDB API "GET /{db}/_design/{ddoc}/_list/{func}/{other-ddoc}/{view}", deprecated 3.0, removed 4.0, UNTESTED],
[CouchDB API "POST /{db}/_design/{ddoc}/_list/{func}/{other-ddoc}/{view}", deprecated 3.0, removed 4.0, UNTESTED].

=option  view_ddoc $ddoc|$ddocid
=default view_ddoc C<undef>
=cut

sub list($$%)
{	my ($self, $function, $view, %args) = @_;

	my $other = defined $args{view_ddoc} ? '/'.delete $args{view_ddoc} : '';
	my $path = $self->_pathToDoc('_list/' . uri_escape($function) . $other . '/' . uri_escape($view));

	$couch->call(GET => $path,
		deprecated => '3.0',
		removed    => '4.0',
		$couch->_resultsConfig(\%args),
	);
}

=method update $function, %options
[CouchDB API "POST /{db}/_design/{ddoc}/_update/{func}", UNTESTED],
[CouchDB API "POST /{db}/_design/{ddoc}/_update/{func}/{docid}", UNTESTED],

=option  doc $document|$docid
=default doc C<null>
Run the function on the specified document, by default a C<null> document.
=cut

sub show($%)
{	my ($self, $function, %args) = @_;
	my $path = $self->_pathToDoc('_update/'.uri_escape($function));

	if(my $doc = delete $args{doc})
	{	$path .= '/' . (blessed $doc ? $doc->id : $doc);
	}

	$couch->call(POST => $path,
		deprecated => '3.0',
		removed    => '4.0',
		send       => { },
		$couch->_resultsConfig(\%args),
	);
}

# [CouchDB API "ANY /{db}/_design/{ddoc}/_rewrite/{path}", deprecated 3.0, removed 4.0, UNSUPPORTED],
# The documentation of this method is really bad, and you probably should do this in your programming
# language anyway.

#-------------
=section Other
=cut

1;
