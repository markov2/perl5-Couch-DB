# SPDX-FileCopyrightText: 2024 Mark Overmeer <mark@overmeer.net>
# SPDX-License-Identifier: Artistic-2.0

package Couch::DB::Result;

use Couch::DB::Util     qw(flat pile);
use Couch::DB::Document ();
use Couch::DB::Row      ();

use Log::Report   'couch-db';
use HTTP::Status  qw(is_success status_constant_name HTTP_OK HTTP_CONTINUE HTTP_MULTIPLE_CHOICES);
use Scalar::Util  qw(weaken blessed);

my %couch_code_names   = ();   # I think I saw them somewhere.  Maybe none

my %default_code_texts = (  # do not construct them all the time again
	&HTTP_OK				=> 'Data collected successfully.',
	&HTTP_CONTINUE			=> 'The data collection is delayed.',
	&HTTP_MULTIPLE_CHOICES	=> 'The Result object does not know what to do, yet.',
);

=chapter NAME

Couch::DB::Result - the reply of a CouchDB server call

=chapter SYNOPSIS

  # Any call to the CouchDB server result in this object.
  # But call() is for internal library use: avoid!
  my $result = $couch->call($method, $path, %call_options);

  if($result->isReady) { ... }
  if($result)          { ... }   # same
  $result or die;

  my $data = $result->answer;    # raw JSON response
  my $val  = $result->values;    # interpreted response

  # It's not always needed to inspect the document
  if($result->{ok})    { ... }

=chapter DESCRIPTION

The result of a call has many faces: it can be a usage error, a server
issue, empty, paged, or even delayed.  This Result object is able to
handle them all.  B<Read the DETAILS chapter below, to understand them all.>

This result objects are pretty heavy: it collects request, response, and much
more.  So: let them run out-of-scope once you have collected your C<values()>.

=chapter OVERLOADED

=overload bool
These Return objects are overloaded to return a false value when there is
any error.  For delayed collection of data, this status may change after
this object is initially created.
=cut

use overload
	bool => sub { $_[0]->code < 400 };

=chapter METHODS

=section Constructors

=c_method new %options

For details on the C<on_*> event handlers, see L<Couch::DB/DETAILS>.

=requires couch M<Couch::DB>-object

=option   on_final CODE|ARRAY
=default  on_final C<< [ ] >>
Called when the Result object has either an error or an success.

=option   on_error CODE|ARRAY
=default  on_error C<< [ ] >>
Called each time when the result CODE changes to be "not a success".

=option   on_values CODE|ARRAY
=default  on_values C<< [ ] >>
Provide a sub which translates incoming JSON data from the server, into
pure perl.

=option   on_chain CODE|ARRAY
=default  on_chain C<< [ ] >>
When a request was completed, a new request can be made immediately.  This
is especially usefull in combination with C<_delay>, and with internal
logic.

=option   on_row CODE|ARRAY
=default  on_row C<< [ ] >>
Produces a single M<Couch::DB::Row>-object when M<page()> is used.

=option   paging HASH
=default  paging C<undef>
When a call support paging, internal information about it is passed in
this HASH.
=cut

sub new(@) { my ($class, %args) = @_; (bless {}, $class)->init(\%args) }

sub init($)
{	my ($self, $args) = @_;

	$self->{CDR_couch}     = delete $args->{couch} or panic;
	$self->{CDR_on_final}  = pile delete $args->{on_final};
	$self->{CDR_on_error}  = pile delete $args->{on_error};
	$self->{CDR_on_chain}  = pile delete $args->{on_chain};
	$self->{CDR_on_values} = pile delete $args->{on_values};
	$self->{CDR_on_row}    = pile delete $args->{on_row};
	$self->{CDR_code}      = HTTP_MULTIPLE_CHOICES;
	$self->{CDR_page}      = delete $args->{paging};

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
sub isReady()   { $_[0]->{CDR_ready} }

=method code
Returns an HTTP status code (please use M<HTTP::Status>), which reflects
the condition of the answer.
=cut

sub code()      { $_[0]->{CDR_code} }

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

=method status $code, $message
Set the $code and $message to something else.  Your program should
probably not do this: it's the library which determines how the result
needs to be interpreted.
=cut

sub status($$)
{	my ($self, $code, $msg) = @_;
	$self->{CDR_code} = $code;
	$self->{CDR_msg}  = $msg;
	$self;
}

#-------------
=section When the document is collected

=method client
Which client M<Couch::DB::Client> was used in the last action.  Initially,
none.  When the results are ready, the client is known.

=method request
Returns the request (framework specific) object which was used to collect
the data.

=method response
When the call was completed, this will return the (framework specific) object
which contains the response received.
=cut

sub client()    { $_[0]->{CDR_client} }
sub request()   { $_[0]->{CDR_request} }
sub response()  { $_[0]->{CDR_response} }

=method answer %options
When the response was received, this returns the received json answer
as HASH of raw data: the bare result of the request.

You can better use the M<values()> method, which returns the data in a far more
Perlish way: as Perl booleans, DateTime objects, and so on.
=cut

sub answer(%)
{	my ($self, %args) = @_;

	return $self->{CDR_answer}
		if defined $self->{CDR_answer};

 	$self->isReady
		or error __x"Document not ready: {err}", err => $self->message;

	$self->{CDR_answer} = $self->couch->_extractAnswer($self->response);
}

=method values
Each CouchDB API call knows whether it passes data types which are
(potentially) incompatible between JSON and Perl. Those types get
converted for you for convenience in your main program.

The raw data is returned with M<answer()>.  See L</DETAILS> below.
=cut

sub values(@)
{	my $self = shift;
	return $self->{CDR_values} if exists $self->{CDR_values};

	my $values = $self->answer;
	$values = $_->($self, $values) for reverse @{$self->{CDR_on_values}};
	$self->{CDR_values} = $values;
}

#-------------
=section Results containing rows

When a result (potentially) contains multiple rows, then paging is supported.
But you may also wish to access the rows directly.

=method rows [$search_nr]
Some CouchDB calls can be used with paging.  In that case, the answer will
show something which reflects rows.  This method wraps the values in the
rows into M<Couch::DB::Row>-objects.

(At least with) M<Couch::DB::Database::find()> you can supply multiple
queries at the same time.  They will all use the same paging, usually C<_all>
records at once.  In this case, you must specify the query sequence number
(starts with zero)
=cut

sub rows(;$) { @{$_[0]->rowsRef($_[1])} }

=method rowsRef [$search_nr]
Returns a reference to the returned rows.
=cut

sub rowsRef(;$)
{	my ($self, $col) = @_;
	my $rows = $self->{CDR_rows}[$col ||= 0] ||= [];
	return $rows if $self->{CDR_rows_complete}[$col];

	for(my $rownr = 1; $self->row($rownr, $col); $rownr++) { }
	$self->{CDR_rows_complete}[$col] = 1;
	$rows;
}

=method docs [$search_nr]
Return only the document information which is kept in the rows.  Some
rows may contain more search information.
Returns a LIST of M<Couch::DB::Document>-objects.
=cut

sub docs(;$) { map $_->doc, $_[0]->rows($_[1]) }

=method docsRef
Returns a reference to the documents.
=cut

sub docsRef(;$) { [ map $_->doc, $_[0]->rows($_[1]) ] }

=method row $rownr, [$search_nr]
Returns a M<Couch::DB::Row> object (or an empty LIST) which represents one
row in a paging answer.  Row numbers start on 1.
=cut

sub row($$%)
{	my ($self, $rownr, $col, %args) = @_;
	my $rows = $self->{CDR_rows}[$col ||= 0];
	return $rows->[$rownr] if exists $rows->[$rownr];

	my %data = map $_->($self, $rownr-1, column => $col), reverse @{$self->{CDR_on_row}};
	keys %data or return ();

	my $doc;
	if(my $dd = delete $data{docdata})
	{	my $dp = delete $data{docparams} || {};
		$doc   = Couch::DB::Document->fromResult($self, $dd, %$dp);
	}

	my $row = Couch::DB::Row->new(%data, result => $self, rownr => $rownr, doc => $doc);
	$doc->row($row);

	$self->{CDR_rows}[$col][$rownr-1] = $row;    # Remember partial result for rows()
}

=method numberOfRows [$search_nr]
=cut

sub numberOfRows(;$) { scalar @{$_[0]->rowsRef($_[1])} }

#-------------
=section Paging through results

=method pagingState %options
Returns information about the logical next page for this response, in a format
which can be saved into a session.

=option  max_bookmarks INTEGER
=default max_bookmarks 10
When you save this paging information into a session cookie, you should not
store many bookmarks, because they are pretty large and do not compress.  Random
bookmarks are thrown away.  Set to '0' to disable this restriction.
=cut

sub pagingState(%)
{	my ($self, %args) = @_;
	my $next = $self->nextPageSettings;
	$next->{harvester} = defined $next->{harvester} ? 'CODE' : 'DEFAULT';
	$next->{map}       = defined $next->{map} ? 'CODE' : 'NONE';
	$next->{client}    = $self->client->name;

	if(my $maxbook = delete $args{max_bookmarks} // 10)
	{	my $bookmarks = $next->{bookmarks};
		$next->{bookmarks} = +{ (%$bookmarks)[0..(2*$maxbook-1)] } if keys %$bookmarks > $maxbook;
	}

	$next;
}

=method supportsPaging
[0.100] Returns whether the result supports paging.
=cut

sub supportsPaging() { defined $_[0]->{CDR_page} }

# The next is used r/w when _succeed is a result object, and when results
# have arrived.

sub _thisPage() { $_[0]->{CDR_page} or panic "Call does not support paging." }

=method nextPageSettings
Returns the details for the next page to be collected.  When you need these
details to be saved outside the program, than use M<pagingState()>.
=cut

sub nextPageSettings()
{	my $self = shift;
	my %next = %{$self->_thisPage};
	delete $next{harvested};
	$next{start} += (delete $next{skip}) + @{$self->page};
#use Data::Dumper;
#warn "NEXT PAGE=", Dumper \%next;
	\%next;
}

=method page
Returns an ARRAY with the elements collected (harvested) for this page.
When there are less elements than the requested page size, then there
are no more elements as result of the search.

Method M<pageRows()> will return the rows as a LIST.

=example compare page and pageRows

   my $r = $db->find(...);
   foreach my $row ($r->pageRows) { ... }
   foreach my $row ( @{$r->page} ) { ... }
   print template($t, rows => [ $r->pageRows ]);
   print template($t, rows => $r->page);
=cut

sub page() { $_[0]->_thisPage->{harvested} }

sub _pageAdd($@)
{	my $this     = shift->_thisPage;
	my $bookmark = shift;
	my $page     = $this->{harvested};
	if(@_)
	{	push @$page, @_;
		$this->{bookmarks}{$this->{start} + $this->{skip} + @$page} = $bookmark
			if defined $bookmark;
	}
	else
	{	$this->{end_reached} = 1;
	}
	$page;
}

=method pageRows
Returns the LIST of rows (M<Couch::DB::Row> objects), where M<page()> returns it
as ARRAY (reference).
=cut

sub pageRows() { @{$_[0]->page} }

=method pageDocs
Returns the LIST of documents (M<Couch::DB::Document> objects), which are
contained in the rows.

=example of pageDocs()
  my $r1 = $couch->find(...);
  my @docs1 = map $_->doc, $r1->page;

  my $r2 = $couch->find(..., _harvester => sub { $_[0]->docs });
  my @docs2 = $r2->page;

  my $r3 = $couch->find(...);
  my @docs3 = $r3->pageDocs;

=cut

sub pageDocs() { map $_->doc, @{$_[0]->page} }

=method pageIsPartial
Returns a true value when there should be made another attempt to fill the
page upto the the requested page size.
=cut

sub pageIsPartial()
{	my $this = shift->_thisPage;
	! $this->{end_reached} && ($this->{all} || @{$this->{harvested}} < $this->{page_size});
}

=method isLastPage
Returns a true value when there are no more page elements to be expected.  The
M<page()> may already be empty.
=cut

sub isLastPage() { $_[0]->_thisPage->{end_reached} }

#-------------
=section When the collecting is delayed

=method setFinalResult \%data, %options
Fill this Result object with the actual results.
=cut

sub setFinalResult($%)
{	my ($self, $data, %args) = @_;
	my $code = delete $data->{code} || HTTP_OK;

	$self->{CDR_client}   = my $client = delete $data->{client} or panic "No client";
	weaken $self->{CDR_client};

	$self->{CDR_ready}    = 1;
	$self->{CDR_request}  = delete $data->{request};
	$self->{CDR_response} = delete $data->{response};
	$self->status($code, delete $data->{message});

	delete $self->{CDR_answer};  # remove cached while paging
	delete $self->{CDR_values};

	# "on_error" handler
	unless(is_success $code)
	{	$_->($self) for @{$self->{CDR_on_error}};
	}

	# "on_final" handler
	$_->($self) for @{$self->{CDR_on_final}};

	# "on_change" handler
	# First run inner chains, working towards outer
	my @chains = @{$self->{CDR_on_chain} || []};
	my $tail   = $self;

	while(@chains && $tail)
 	{	$tail = (pop @chains)->($tail);
		blessed $tail && $tail->isa('Couch::DB::Result')
			or panic "Chain must return a Result object";
	}

	$tail;
}

=method setResultDelayed $plan, %options
When defined, the result document is not yet collected.  The C<$plan> contains
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

  my $find1 = $db->find(..., _delay => 1)
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
  print $result->answer->{version}; # string

  # Use this instead:
  print $result->values->{version};  # version object

In some cases, data is added or modified for convenience: to make it
compatible with the version your program has been written for.  See
M<Couch::DB::new(api)>.

=cut

1;
