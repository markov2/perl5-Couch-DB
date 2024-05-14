# SPDX-FileCopyrightText: 2024 Mark Overmeer <mark@open-console.eu>
# SPDX-License-Identifier: EUPL-1.2-or-later

package Couch::DB::Result;

use Couch::DB::Util;
use Couch::DB::Document ();

use Log::Report   'couch-db';
use HTTP::Status  qw(is_success status_constant_name HTTP_OK HTTP_CONTINUE HTTP_MULTIPLE_CHOICES);
use Scalar::Util  qw(weaken);

my %couch_code_names   = ();   # I think I saw them somewhere.  Maybe none

my %default_code_texts = (  # do not construct them all the time again
	HTTP_OK					=> 'Data collected successfully.',
	HTTP_CONTINUE			=> 'The data collection is delayed.',
	HTTP_MULTIPLE_CHOICES	=> 'The Result object does not know what to do, yet.',
);

=chapter NAME

Couch::DB::Result - the reply of a CouchDB server call

=chapter SYNOPSIS

  # Any call to the CouchDB server result in this object.
  my $result = $couch->call($method, $path, %call_options);

  if($result->isReady) { ... }
  unless($result)      { ... }   # same

  my $doc = $result->doc;      # Couch::DB::Document

  # It's not always needed to inspect the document
  if($result->ok)      { ... }

=chapter DESCRIPTION

The result of a call has many faces: it can be a usage error, a server
issue, empty, paged, or even delayed.  This Result object is able to
handle them all.  B<Read the DETAILS chapter below, to understand them all.>

=chapter OVERLOADING

=overload bool
These Return objecs are overloaded to return a false value when there is
any error.  For delayed collection of data, this status may change after
this object is initially created.
=cut

use overload bool => sub { $_[0]->code >= 400 };

=chapter METHODS

=section Constructors

=c_method new %options

=requires couch M<Couch::DB>-object

=option   on_error CODE
=default  on_error <do nothing>
Called each time when the result CODE changes to be "not a success".

=option   to_values CODE
=default  to_values <keep data>
Provide a sub which translates incoming JSON data from the server, into
pure perl.
=cut

sub new(@) { my ($class, %args) = @_; (bless {}, $class)->init(\%args) }

sub init($)
{	my ($self, $args) = @_;

	$self->{CDR_couch}     = delete $args->{couch} or panic;
	weaken $self->{CDR_couch};

	$self->{CDR_on_error}  = delete $args->{on_error}  || sub { };
	$self->{CDR_code}      = HTTP_MULTIPLE_CHOICES;
	$self->{CDR_to_values} = delete $args->{to_values} || sub { $_[1] };
	$self;
}

#-------------
=section Accessors

Generic accessors, not related to the Result content.

=method couch
=method isDelayed
=method isReady
=cut

sub couch()     { $_[0]->{CDR_couch}  }
sub isDelayed() { $_[0]->code == HTTP_CONTINUE }
sub isReady()   { $_[0]->code == HTTP_OK }

=method code
Returns an HTTP status code (please use M<HTTP::Status>), which reflects
the condition of the answer.
=cut

sub code()   { $_[0]->{CDR_code} }

=method codeName [$code]
Return a string which represents the code.  For instance, code 200 will
produce string "HTTP_OK".

See CouchDB API section 1.1.4: "HTTP Status Codes" for the interpretation
of the codes.
=cut

sub codeName(;$)
{	my ($self, $code) = @_;
	$code ||= $self->code;
	status_constant_name($code) || couch_code_names{$code} || $code;
}

=method message
Returns C<undef>, or a message (string) which explains why the status
is as it is.
=cut

sub message()
{	my $self = shift;
	$self->{CDR_msg} || $default_code_texts{$self->code} || $self->codeName;
}

#-------------
=section When the document is collected

=method client
Which client M<Couch::DB::Client> was used in the last action.  Initially,
none.  When the results are ready, the client is known.

=method request
=method response
=cut

sub client()    { $_[0]->{CDR_client} }
sub request()   { $_[0]->{CDR_request} }
sub response()  { $_[0]->{CDR_response} }

=method doc %options
Returns the received document.  When the Result was delayed, it will get
realized now.
=cut

sub doc(%)
{	my ($self, %args) = @_;

	return $self->{CDR_doc}
		if defined $self->{CDR_doc};

 	$self->isReady
		or error __x"Document not ready: {err}", err => $self->message;

	$self->{CDR_doc} = Couch::DB::Document->fromJSON(
		$self->couch->extractJSON($self->response),
		result => $self,
	);
}

=method values
Returns a scalar which represents a value included in the document,
made easily accessible and hiding protocol version differences.

When the value produces an ARRAY, then it is returned as reference.
See M<values()>.  See L</DETAILS> below.
=cut

sub values(@)
{	my $self = shift;
	$self->{CDR_values} ||= $self->{CDR_to_values}->($self, $self->doc->data);
}

#-------------
=section When the collecting is delayed

=method setFinalResult \%data, %options
Fill this Result object with the actual results.
=cut

sub setFinalResult($%)
{	my ($self, $data, %args) = @_;

	$self->{CDR_client}   = delete $data->{client} or panic "No client";
	weaken $self->{CDR_client};

	$self->{CDR_request}  = delete $data->{request};
	$self->{CDR_response} = delete $data->{response};

	# Must be last: may trigger on_error event
	$self->status(delete $data->{code} || HTTP_OK, delete $data->{message});
	$self;
}

=method setResultDelayed $plan, %options
When defined, the result document is not yet collected.  The $plan contains
framework specific information how to realize that in a later stage.
=cut

sub setResultDelayed($%)
{	my ($self, $plan, %args) = @_;

	$self->{CDR_delayed}  = $plan;
	$self->status(HTTP_CONTINUE);
	$self;
}

=method delayPlan
Returns the (framework specific) information about actions to be taken to
collect the document.
=cut

sub delayPlan() { $_[0]->{CDR_delayed} }

#-------------
=section Other
=cut

=method status $code, $message
Set the $code and $message to something else.  Your program should
probably not do this: it's the library which determines how the result
needs to be interpreted.
=cut

sub status($$)
{	my ($self, $code, $msg) = @_;
	$self->{CDR_code} = $code;
	$self->{CDR_msg}  = $msg;

	is_success $code
		or $self->{CDR_on_error}->($self);

	$self;
}

#-------------
=chapter DETAILS

This Result objects have many faces.  Understand them well, before you start
programming with M<Couch::DB>.

=section Result is an error

The Result object is overloaded to produce a false value when the command did
not succeed for any reason.

=example without error handler

  my $result = $db->find(...)
      or die $result->message;

=example with error handler

  my $result = $db->find(..., on_error => sub { die } );

=section Delay the result

When your website has only the slightest chance on having users, then
you need to use single server processes shared by many website users.
Couch::DB implementations will use event-driven programming to make this
possible, but your own program should be configured to make use of this
to benefit.

Usually, questions to the database are purely serial.  This is an easy
case to handle, and totally hidden for you, as user of this module.
For instance, when you want to query in parallel, you need to prepare
multiple queries, and then start them at the same time.

=example prepare a query, delayed

  my $find1 = $db->find(..., delay => 1)
      or die $result->message;  # only preparation errors
  
  if($find1->isDelayed) ...;    # true
  
  my $result = $find1->run
      or die $result->message;  # network/server errors

  # TODO
  my $result = $couch->parallel([$find1, $find2], concurrent => 2);

=section Understanding values

To bridge the gap between your program and JSON data received, Couch::DB
provides templated conversions.  This conversion scheme also attempts to
hide protocol changes between CouchDB server versions.

  my $result = $couch->client('local')->serverInfo;
  result or die;

  # Try to avoid this:
  print $result->doc->data->{version}; # string

  # Use this instead:
  print $result->value('version');     # version object

The M<value()> and M<values()> methods accept a $path which describes
the position of the required value in the raw data.  Then, it knows
the type of data, and converts it into convenient Perl objects.

In some cases, data is added or modified for convenience: to make it
compatible with the version your program has been written for.  See
M<Couch::DB::new(version)>.

The following data-types are used:
=over 4
=item * version
Returned as M<version> object
=item * timestamps
Returned as M<DateTime> object
=item * links
Returned as backend specific URL object
=back

=cut

1;
