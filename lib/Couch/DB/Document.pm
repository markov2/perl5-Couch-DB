# SPDX-FileCopyrightText: 2024 Mark Overmeer <mark@overmeer.net>
# SPDX-License-Identifier: Artistic-2.0

package Couch::DB::Document;
use Couch::DB::Util;

use Log::Report 'couch-db';
use Scalar::Util qw(weaken);
use MIME::Base64 qw(decode_base64);

=chapter NAME

Couch::DB::Document - one document as exchanged with a CouchDB server

=chapter SYNOPSIS

  my $doc = $couch->db($dbname)->doc($docid);
  my $doc = $db->doc->create(\%data);
  my $doc = $db->doc($id, local => 1);

  my $content = $db->latest;

=chapter DESCRIPTION

This class manages one document, without understanding the payload.  When
exchanging questions and answers with the server, keys which start with
an underscore (C<_>) may get added and removed: they should not be visible
in your data.

=chapter METHODS

=section Constructors

=c_method new %options

=option   id  ID
=default  id  C<undef>
When you are creating a new document (M<create()>), you may leave this open to get
an id generated.  Otherwise, this parameter is required.

=option   batch BOOLEAN
=default  batch C<from database>
For all of the writes which support it, use batch (no wait) writing.  Of course,
this may cause data to be lost when technical or logical issues emerge while the
actual writing is done, but is much faster.

=option   db   M<Couch::DB::Database>-object
=default  db   C<undef>
If this document is database related.

=option   local BOOLEAN
=default  local C<false>
Use a local document: do not replicate it to other instances.  Only limited
actions are permitted on local documents... probably they do not support
attachments.
=cut

sub new(@) { my ($class, %args) = @_; (bless {}, $class)->init(\%args) }

sub init($)
{	my ($self, $args) = @_;
	$self->{CDD_id}    = delete $args->{id};
	$self->{CDD_db}    = my $db = delete $args->{db};
	$self->{CDD_info}  = {};
	$self->{CDD_batch} = exists $args->{batch} ? delete $args->{batch} : $db->batch;
	$self->{CDD_revs}  = {};
	$self->{CDD_local} = delete $args->{local};

	$self->{CDD_couch} = $db->couch;
	weaken $self->{CDD_couch};

	# The Document is (for now) not linked to its Result source, because
	# that might consume a lot of memory.  Although it may help debugging.
	# weaken $self->{CDD_result} = my $result = delete $args->{result};

	$self;
}

sub _consume($$)
{	my ($self, $result, $data) = @_;
	my $id       = $self->{CDD_id} = delete $data->{_id};
	my $rev      = delete $data->{_rev};

	# Add all received '_' labels to the existing info.
	my $info     = $self->{CDD_info} ||= {};
	$info->{$_}  = delete $data->{$_}
		for grep /^_/, keys %$data;

	my $attdata = $self->{CDD_atts} ||= {};
	if(my $atts = $info->{_attachments})
	{	foreach my $name (keys %$atts)
		{	my $details = $atts->{$name};
			$attdata->{$name} = $self->couch->_attachment($result->response, $name)
				if $details->{follows};

			# Remove sometimes large data
			$attdata->{$name} = decode_base64 delete $details->{data} #XXX need decompression?
				if defined $details->{data};
		}
	}
	$self->{CDD_revs}{$rev} = $data;
	$self;
}

sub _fromResponse($$$%)
{	my ($class, $result, $data, %args) = @_;
	$class->new(%args)->_consume($result, $data);
}

#-------------
=section Accessors

=method id
=method db
=method batch
=method couch
=cut

sub id()      { $_[0]->{CDD_id} }
sub db()      { $_[0]->{CDD_db} }
sub batch()   { $_[0]->{CDD_batch} }
sub couch()   { $_[0]->{CDD_couch} }

sub _pathToDoc(;$)
{	my ($self, $path) = @_;
	if($self->isLocal)
	{	$path and panic "Local documents not supported with path '$path'";
		return $self->db->_pathToDB('_local/' . $self->id);
	}
	$self->db->_pathToDB($self->id . (defined $path ? "/$path" : ''));
}

sub _deleted($)
{	my ($self, $rev) = @_;
	$self->{CDD_revs}{$rev} = {};
	$self->{CDD_deleted} = 1;
}

sub _saved($$$)
{	my ($self, $id, $rev, $data) = @_;
	$self->{CDD_id} ||= $id;
	$self->{CDD_revs}{$rev} = $data;
}

#-------------
=section Content

B<Warning:> Where Perl does not support the same data-types as JSON, you need to
be very careful when addressing fields from this structure.  B<Much better> is
it to use the provided abstraction methods, which hide those differences.  Those
also hide changes in the server software, over time.

=method isLocal
This documents does not get replicated over nodes.
=cut

sub isLocal() { $_[0]->{CDD_local} }

=method isDeleted
Returns a boolean whether the document is delete.
=cut

sub isDeleted() { $_[0]->{CDD_deleted} }

=method revision $rev
Returns the data for document revision $rev, if retreived by a former
call.
=cut

sub revision($) { $_[0]->{CDD_revs}{$_[1]} }

=method latest
Returns the data of the latest revision of the document, as retreived
by any former call on this document object.
=cut

sub latest() { $_[0]->revision(($_[0]->revisions)[0]) }

=method revisions
Returns a sorted list of all known revisions, as retreived by a former
call.  They are sorted, newest first.
=cut

sub revisions()
{	my $revs = $_[0]->{CDD_revs};
	no warnings 'numeric';   # forget the "-hex" part of the rev
	sort {$b <=> $a} keys %$revs;
}

=method rev
The latest revision of this document.
=cut

sub rev() { ($_[0]->revisions)[0] }

#-------------
=section Document details
These methods usually require a M<get()> with sufficient parameters to
be useable (they die on unsuffient details).

=cut

sub _info() { $_[0]->{CDD_info} or panic "No info yet" }

=method conflicts
Returns a LIST with conflict details.

=method deletedConflicts
Returns a LIST with deletedConflict details.

=method updateSequence
Returns the update sequence code for this document on the current server (local_seq).
Only useful when you use an explicit C<_client> when you M<get()> the document.
=cut

sub conflicts()        { @{ $_[0]->_info->{_conflicts} || [] } }
sub deletedConflicts() { @{ $_[0]->_info->{_deleted_conflicts} || [] } }
sub updateSequence()   { $_[0]->_info->{_local_seq} }

=method revisionsInfo
Returns a HASH with all revisions and their status.
=cut

sub revisionsInfo()
{	my $self = shift;
	return $self->{CDD_revinfo} if $self->{CDD_revinfo};

	my $c = $self->_info->{_revs_info}
		or error __x"You have requested the open_revs detail for the document yet.";

	$self->{CDD_revinfo} = +{ map +($_->{rev} => $_), @$c };
}

=method revisionInfo $revision
Returns a HASH with information about one $revision only.
=cut

sub revisionInfo($) { $_[0]->revisionsInfo->{$_[1]} }

#-------------
=section Document in the database

B<All CouchDB API calls> documented below, support %options like C<_delay>
and C<on_error>.  See L<Couch::DB/Using the CouchDB API>.

=method exists %option
 [CouchDB API "HEAD /{db}/{docid}"]

Check whether the document exists.  You may get some useful response headers.

=example
  if($db->doc($id)->exists) { ... }
=cut

sub exists(%)
{   my ($self, %args) = @_;

    $self->couch->call(HEAD => $self->_pathToDoc,
        $self->couch->_resultsConfig(\%args),
    );
}

=method create \%data, %options
 [CouchDB API "POST /{db}"]

Save this document for the first time to the database. Your content of the
document is in %data.  When you pick your own document ids, you can also use
M<update()> for a first save.

=option  batch BOOLEAN
=default batch M<new(batch)>
Do not wait for the write action to be completed.
=cut

sub __created($$)
{	my ($self, $result, $data) = @_;
	$result or return;

	my $v = $result->values;
	$v->{ok} or return;

	delete $data->{_id};  # do not polute the data
	$self->_saved($v->{id}, $v->{rev}, $data);
}
	
sub create($%)
{	my ($self, $data, %args) = @_;
	ref $data eq 'HASH' or panic "Attempt to create document without data.";

	my %query;
	$query{batch} = 'ok'
		if exists $args{batch} ? delete $args{batch} : $self->batch;

	$data->{_id} ||= $self->id;

	$self->couch->call(POST => $self->db->_pathToDB,  # !!
		send     => $data,
		query    => \%query,
		$self->couch->_resultsConfig(\%args,
			on_final => sub { $self->__created($_[0], $data) },
		),
	);
}

=method update \%data, %options
 [CouchDB API "PUT /{db}/{docid}"]
 [CouchDB API "PUT /{db}/_local/{docid}"]

Save a new revision of this document to the database.  If docid is new,
then it will be created, otherwise a new revision is added.  Your content
of the document is in %data.

When you want to create a new document, where the servers creates the id, then
use M<create()>.

=option  batch BOOLEAN
=default batch M<new(batch)>
Do not wait for the write action to be completed.
=cut

sub update($%)
{	my ($self, $data, %args) = @_;
	ref $data eq 'HASH' or panic "Attempt to update the document without data.";

	my $couch     = $self->couch;

	my %query;
	$query{batch} = 'ok' if exists $args{batch} ? delete $args{batch} : $self->batch;
	$query{rev}   = delete $args{rev} || $self->rev;
	$query{new_edits} = delete $args{new_edits} if exists $args{new_edits};
	$couch->toQuery(\%query, bool => qw/new_edits/);

	$couch->call(PUT => $self->_pathToDoc,
		query    => \%query,
		send     => $data,
		$couch->_resultsConfig(\%args, on_final => sub { $self->__created($_[0], $data) }),
	);
}

=method get [\%flags, %options]
 [CouchDB API "GET /{db}/{docid}"]
 [CouchDB API "GET /{db}/_local/{docid}"]

Retrieve document data and information from the database.  There are a zillion
of %options to collect additional meta-data.

When no explicit revision (option C<rev>) is given, then the latest
revision is collected.

Returned is, as usual, whether the database gave a successful response. The data
received will get merged into this object's attributes.

=example use of get()
  my $game = $db->doc('monopoly');
  $game->get(latest => 1) or die;
  my $data = $game->latest;

=cut

sub __get($$)
{	my ($self, $result, $flags) = @_;
	$result or return;   # do nothing on unsuccessful access
	$self->_consume($result, $result->answer);

	# meta is a shortcut for other flags
	$flags->{conflicts} = $flags->{deleted_conflicts} = $flags->{revs_info} = 1
		if $flags->{meta};

	$self->{CDD_flags}      = $flags;
}

sub get(%)
{	my ($self, $flags, %args) = @_;
	my $couch = $self->couch;

	my %query  = $flags ? %$flags : ();
	$couch->toQuery(\%query, bool => qw/attachments att_encoding_info conflicts
		deleted_conflicts latest local_seq meta revs revs_info/);

	$couch->call(GET => $self->_pathToDoc,
		query    => \%query,
		$couch->_resultsConfig(\%args,
			on_final => sub { $self->__get($_[0], $flags) },
			_headers => { Accept => $args{attachments} ? 'multipart/related' : 'application/json' },
		),
	);
}

=method delete %options
 [CouchDB API "DELETE /{db}/{docid}"]
 [CouchDB API "DELETE /{db}/_local/{docid}"]

Flag the document to be deleted.  A new revision is created, which reflects this.
Only later, when all replications know it and compaction is run, the document
versions will disappear.
=cut

sub __delete($)
{	my ($self, $result) = @_;
	$result or return;

	my $v = $result->values;
	$self->_deleted($v->{rev}) if $v->{ok};
}

sub delete(%)
{	my ($self, %args) = @_;
	my $couch = $self->couch;

	my %query;
	$query{batch} = 'ok' if exists $args{batch} ? delete $args{batch} : $self->batch;
	$query{rev}   = delete $args{rev} || $self->rev;
		
	$couch->call(DELETE => $self->_pathToDoc,
		query    => \%query,
		$couch->_resultsConfig(\%args, on_final => sub { $self->__delete($_[0]) }),
	);
}

=method cloneInto $doc, %options
 [CouchDB API "COPY /{db}/{docid}", PARTIAL]
 [CouchDB API "COPY /{db}/_local/{docid}", PARTIAL]

See also M<appendTo()>.

As %options, C<batch> and C<rev>.

=example cloning one document into a new one
   my $from = $db->doc('from');
   $from->get or die;
   my $to   = $db->doc('to');   # does not exist
   $from->cloneInto($to) or die;
=cut

# Not yet implemented.  I don't like chaning the headers of my generic UA.
sub cloneInto($%)
{	my ($self, $to, %args) = @_;
	my $couch = $self->couch;

	my %query;
	$query{batch} = 'ok' if exists $args{batch} ? delete $args{batch} : $self->batch;
	$query{rev}   = delete $args{rev} || $self->rev;

#XXX still work to do on updating the admin in 'to'
	$couch->call(COPY => $self->_pathToDoc,
		query    => \%query,
		$couch->_resultsConfig(\%args,
			on_final => sub { $self->__delete($_[0]) },
			_headers => +{ Destination => $to->id },
		),
	);
}

=method appendTo $doc, %options
 [CouchDB API "COPY /{db}/{docid}", PARTIAL]
 [CouchDB API "COPY /{db}/_local/{docid}", PARTIAL]

See also M<cloneInto()>.
As %options: C<batch> and C<rev>.

=example appending one document into an other
   my $from = $db->doc('from');
   $from->get or die;
   my $to   = $db->doc('to');   # does not exist
   $to->get or die;
   $from->appendTo($to) or die;
=cut

sub appendTo($%)
{	my ($self, $to, %args) = @_;
	my $couch = $self->couch;

	my %query;
	$query{batch} = 'ok' if exists $args{batch} ? delete $args{batch} : $self->batch;
	$query{rev}   = delete $args{rev} || $self->rev;

#XXX still work to do on updating the admin in 'to'
	my $dest_rev  = $to->rev or panic "No revision for destination document.";

	$couch->call(COPY => $self->_pathToDoc,
		query    => \%query,
		$couch->_resultsConfig(\%args,
			on_final => sub { $self->__delete($_[0]) },
			_headers => +{ Destination => $to->id . "?rev=$dest_rev" },
		),
	);
}


#-------------
=section Attachments

=method attInfo $name
Return a structure with meta-data about the attachments.

=method attachments
Returns the names of all attachments.

=method attachment $name
Returns the bytes of the named attachment (of course, you need to
get it first).
=cut

sub attInfo($)    { $_[0]->_info->{_attachments}{$_[1]} }
sub attachments() { keys %{$_[0]->_info->{_attachments}} }
sub attachment($) { $_[0]->{CDD_atts}{$_[1]} }

=method attExists $name, %options
 [CouchDB API "HEAD /{db}/{docid}/{attname}", UNTESTED]
The response is empty, but contains some useful headers.
=cut

sub attExists($%)
{	my ($self, $name, %args) = @_;
	my %query = ( rev => delete $args{rev} || $self->rev );

	$self->couch->call(HEAD => $self->_pathToDoc($name),
		query => \%query,
		$self->couch->_resultsConfig(\%args),
	);
}

=method attLoad $name, %options
 [CouchDB API "GET /{db}/{docid}/{attname}", UNTESTED]

Load the data of the attachment into this Document.

If the content-type of the attachment is C<application/octet-stream>,
then you can use the C<Accept-Ranges> header (option C<_header>) to
select bytes inside the attachement.
=cut

sub __attLoad($$)
{	my ($self, $result, $name) = @_;
	$result or return;
	my $data = $self->couch->_messageContent($result->response);
	$self->_info->{_attachments}{$name} = { length => length $data };
	$self->{CDD_atts}{$name} = $data;
}

sub attLoad($%)
{	my ($self, $name, %args) = @_;
	my %query = ( rev => delete $args{rev} || $self->rev );

	$self->couch->call(GET => $self->_pathToDoc($name),
		query => \%query,
		$self->couch->_resultsConfig(\%args,
			on_final => sub { $self->__attLoad($_[0], $name) },
		),
	);
}

=method attSave $name, $data, %options
 [CouchDB API "PUT /{db}/{docid}/{attname}", UNTESTED]

The data is bytes.

=option  type IANA-MediaType
=default type C<application/octet-stream>
For text documents, this may contain a charset like C<text/plain; charset="utf-8">.
=cut

sub attSave($$%)
{	my ($self, $name, $data, %args) = @_;

	my  $type = delete $args{type} || 'application/octet-stream';
	my %query = (rev => delete $args{rev} || $self->rev);
	$query{batch} = 'ok' if exists $args{batch} ? delete $args{batch} : $self->batch;

	$self->couch->call(PUT => $self->_pathToDoc($name),
		query => \%query,
		send  => $data,
		$self->couch->_resultsConfig(\%args,
			_headers => { 'Content-Type' => $type },
		),
	);
}

=method attDelete $name, %options
 [CouchDB API "DELETE /{db}/{docid}/{attname}", UNTESTED]

Deletes an attachment of this document.
=cut

sub attDelete($$$%)
{	my ($self, $name, %args) = @_;
	my %query = (rev => delete $args{rev} || $self->rev);
	$query{batch} = 'ok' if exists $args{batch} ? delete $args{batch} : $self->batch;

	$self->couch->call(DELETE => $self->_pathToDoc($name),
		query => \%query,
		$self->couch->_resultsConfig(\%args),
	);
}

1;
