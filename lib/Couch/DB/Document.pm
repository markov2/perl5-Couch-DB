# SPDX-FileCopyrightText: 2024 Mark Overmeer <mark@open-console.eu>
# SPDX-License-Identifier: EUPL-1.2-or-later

package Couch::DB::Document;
use Couch::DB::Util;

use Log::Report 'couch-db';

=chapter NAME

Couch::DB::Document - one document as exchanged with a CouchDB server

=chapter SYNOPSIS

=chapter DESCRIPTION

=chapter METHODS

=section Constructors

=c_method new %options

=option   id  ID
=default  id  C<undef>

=option   data HASH
=default  data C<+{ }>
The document data, in CouchDB syntax.
 
=option   db   M<Couch::DB::Database>-object
=default  db   C<undef>
If this document is database related.
=cut

sub new(@) { my ($class, %args) = @_; (bless {}, $class)->init(\%args) }

sub init($)
{	my ($self, $args) = @_;
	$self->{CDD_data} = my $data = delete $args->{data} || {};
	$self->{CDD_id}   = delete $args->{id}   || $data->{_id};

	# The Document is (for now) not linked to its Result source, because
	# that might consume a lot of memory.  Although it may help debugging.
	# weaken $self->{CDD_result} = my $result = delete $args->{result};

	$self;
}

sub fromResult($$%)
{	my ($class, $result, $json, %args) = @_;
	$class->new(data => $json, %args);
}

#-------------
=section Accessors

=method data
This provides access to the raw data received from/to be sent to the CouchDB
server.

B<Warning:> Where Perl does not support the same data-types as JSON, you need to
be very careful when addressing fields from this structure.  B<Much better> is
it to use the provided abstraction methods, which hide those differences.  Those
also hide changes in the server software, over time.

=method id
=method db
=cut

sub data() { $_[0]->{CDD_data} }
sub id()   { $_[0]->{CDD_id} }
sub db()   { $_[0]->{CDD_db} }

sub _pathToDoc($) {	$_[0]->db->_pathToDB($_[1]) }

#-------------
=section Document in the database

=method ping %option
[CouchDB API "HEAD /{db}/{docid}", UNTESTED]
Check whether the document exists.  You may get some useful response headers.

=example
  if($db->doc($id)->ping) { ... }
=cut

sub ping(%)
{   my ($self, %args) = @_;

    ($self->couch->call(HEAD => $self->_pathToDoc,
        $self->couch->_resultsConfig(\%args),
    );
}


#-------------
=section Attachments
=cut

#-------------
=section Other
=cut

1;
