# SPDX-FileCopyrightText: 2024 Mark Overmeer <mark@open-console.eu>
# SPDX-License-Identifier: EUPL-1.2-or-later

package Couch::DB;
use version;

use Log::Report 'couch-db';

use Couch::DB::Client   ();
use Couch::DB::Cluster  ();
use Couch::DB::Database ();
use Couch::DB::Design   ();
use Couch::DB::Node     ();
use Couch::DB::Util     qw(flat);

use Scalar::Util      qw(blessed);
use List::Util        qw(first);
use DateTime          ();
use DateTime::Format::Mail    ();
use DateTime::Format::ISO8601 ();
use URI               ();
use URI::Escape       qw/uri_escape uri_unescape/;
use JSON              qw/encode_json/;
use Storable          qw/dclone/;

use constant
{	DEFAULT_SERVER => 'http://127.0.0.1:5984',
};

=chapter NAME

Couch::DB - CouchDB database client

=chapter SYNOPSIS

   use Couch::DB::Mojolicious ();
   my $couch   = Couch::DB::Mojolicious->new(api => '3.3.3');
   my $db      = $couch->db('my-db'); # Couch::DB::Database object
   my $cluster = $couch->cluster;     # Couch::DB::Cluster object
   my $client  = $couch->createClient(...);  # Couch::DB::Client

=chapter DESCRIPTION

When this module was written, there were already a large number of
CouchDB implementations on CPAN.  Still, there was a need for one more.
This implementation provides a B<thick interface>: a far higher level
of abstraction than the other modules. This should make your work much,
much easier.

B<Please read> the L</DETAILS> section, further down, at least once
before you start!

=section Early adopters

B<Be warned> that this module is really new.  The 127 different endpoints
that the CouchDB 3.3.3 API defines, are grouped and combined.  The result
is often not tested, and certainly not battle ready.

B<Please help> me fix issues by reporting them.  Bugs will be solved within
a day.  Please, contribute useful ideas to make the use of the module lighter.
Together, we can make the quality grow fast.

=section Integration with your framework

You need to instantiate an extensions of this class.  At the moment,
you can pick from:

=over 4
=item * M<Couch::DB::Mojolicious>
Implements the client using the M<Mojolicious> framework, using M<Mojo::URL>,
M<Mojo::UserAgent>, M<Mojo::IOLoop>, and many other.
=back

Other extensions are hopefully added in the future.  Preferrably as part
of this release so it gets maintained together.  The extensions are not
too difficult to create and certainly quite small.

=section Where can I find what?

The CouchDB API lists all endpoints as URLs.  This library, however,
creates an Object Oriented interface around these calls: you do not
see the internals.  Knowing the CouchDB API, it is usually immediately
clear where to find a certain end-point: C<< /{db} >> will be in
M<Couch::DB::Database>.  A major exception is anything what has to
do with replication and sharding: this is bundled in M<Couch::DB::Cluster>.

Have a look at F<...index...>

=chapter METHODS

=section Constructors

=c_method new %options
Create a relation with a CouchDB server(cluster).  You should use
totally separated M<Couch::DB>-objects for totally separate database
clusters.  B<Note:> you can only instantiate extensions of this class.

When you do not specify a C<server>-url, but have an environment variable
C<PERL_COUCH_DB_SERVER>, then server url, username, and password are
derived from it.

=requires api $version
You have to specify the version of the server you expect to answer your
queries.  M<Couch::DB> tries to hide differences between your expectations
and the reality.

The $version can be a string or a version object (see "man version").

=option  server URL
=default server "http://127.0.0.1:5984"
The default server to connect to, by URL.  See C<< etc/local.ini[chttpd] >>
This server will be named 'local'.

You can add more servers using M<addClient()>.  In such case, you probably
do not want this default client to be created as well.  To achieve this,
explicitly set C<server =&gt; undef> here.

=option  auth 'BASIC'|'COOKIE'
=default auth 'BASIC'
Authentication method to be used by default for each client.

=option  username STRING
=default username C<undef>
When a C<username> is given, it will be used together with C<auth> and
C<password> to login to any created client.

=option  password STRING
=default password C<undef>

=option  to_perl HASH
=default to_perl C<< +{ } >>
A table with converter name and CODE, to override/add the default JSON to PERL
object conversions for M<value()>.  See M<toPerl()> and M<listToPerl()>.

=option  to_json HASH
=default to_json C<< +{ } >>
A table with converter name and CODE, to override/add the default PERL to JSON
object conversions for sending structures.  See M<toJSON()>.

=option  to_query HASH
=default to_query C<< +{ } >>
A table with converter name and CODE, to override/add the default PERL to URL
QUERY conversions.  Defaults to the json converters.  See M<toQuery()>.
=cut

sub new(%)
{	my ($class, %args) = @_;
	$class ne __PACKAGE__
		or panic "You have to instantiate extensions of this class";

	(bless {}, $class)->init(\%args);
}

sub init($)
{	my ($self, $args) = @_;

	my $v = delete $args->{api} or panic "Parameter 'api' is required";
	$self->{CD_api}     = blessed $v && $v->isa('version') ? $v : version->parse($v);
	$self->{CD_clients} = [];

	# explicit undef for server means: do not create
	my $create_client   = ! exists $args->{server} || defined $args->{server};
	my $server          = delete $args->{server};
	my $external        = $ENV{PERL_COUCH_DB_SERVER};
	my %auth            = ( auth => delete $args->{auth} || 'BASIC' );

	if($server || ! $external)
	{	$auth{username} = delete $args->{username};
		$auth{password} = delete $args->{password};
	}
	elsif($external)
	{	my $ext = URI->new($external);
		if(my $userinfo = $ext->userinfo)
		{	my ($username, $password) = split /:/, $userinfo;
			$auth{username} = uri_unescape $username;
			$auth{password} = uri_unescape $password;
			$ext->userinfo(undef);
		}
		$server = "$ext";
	}
	$self->{CD_auth}    = \%auth;

	$self->createClient(server => $server || DEFAULT_SERVER, name => '_local')
		if $create_client;

	$self->{CD_toperl}  = delete $args->{to_perl}  || {};
	$self->{CD_tojson}  = delete $args->{to_json}  || {};
	$self->{CD_toquery} = delete $args->{to_query} || {};
	$self;
}

#-------------
=section Accessors

=method api
Returns the interface version you expect the server runs, as a version
object.  Differences between reality and expectations are mostly
automatically resolved.
=cut

sub api() { $_[0]->{CD_api} }

#-------------
=section Interface starting points

=method createClient %options
Create a client object which handles a server.  All options are passed
to M<Couch::DB::Client>.  The C<couch> parameter is added for you.
The client will also be added via M<addClient()>, and is returned.

It may be useful to create to clients to the same server: one with admin
rights, and one without.  Or clients to different nodes, to create
fail-over.
=cut

sub createClient(%)
{	my ($self, %args) = @_;
	my $client = Couch::DB::Client->new(couch => $self, %{$self->{CD_auth}}, %args);
	$client ? $self->addClient($client) : undef;
}

=method db $name, %options
Define a dabase.  The database may not exist yet.  Calling this
method does nothing with the CouchDB server.

  my $db = $couch->db('authors');
  $db->ping or $db->create(...);

=cut

sub db($%)
{	my ($self, $name, %args) = @_;
	Couch::DB::Database->new(name => $name, couch => $self, %args);
}

=method searchAnalyse %options
 [CouchDB API "POST /_search_analyze", since 3.0, UNTESTED]
Check what the build-in Lucene tokenizer(s) will do with your text.

=requires analyzer KIND
=requires text STRING
=cut

#XXX the API-doc might be mistaken, calling the "analyzer" parameter "field".

sub searchAnalyse(%)
{	my ($self, %args) = @_;

	my %send = (
		analyzer => delete $args{analyzer} // panic "No analyzer specified.",
		text     => delete $args{text}     // panic "No text to inspect specified.",
	);

	$self->call(POST => '/_search_analyze',
		introduced => '3.0',
		send       => \%send,
		$self->_resultsConfig(\%args),
	);
}

=method node $name
Returns a M<Couch::DB::Node>-object with the $name.  If the object does not
exist yet, it gets created, otherwise reused.
=cut

sub node($)
{	my ($self, $name) = @_;
	$self->{CD_nodes}{$name} ||= Couch::DB::Node->new(name => $name, couch => $self);
}

=method cluster
Returns a M<Couch::DB::Cluster>-object, which organizes calls to
manipulate replication, sharding, and related jobs.  This will always
return the same object.
=cut

sub cluster() { $_[0]->{CD_cluster} ||= Couch::DB::Cluster->new(couch => $_[0]) }

#-------------
=section Server connections

=method addClient $client
Add a M<Couch::DB::Client>-object to be used to contact the CouchDB
cluster.  Returned is the couch object, so these calls are stackable.
=cut

sub addClient($)
{	my ($self, $client) = @_;
	$client or return $self;

	$client->isa('Couch::DB::Client') or panic;
	push @{$self->{CD_clients}}, $client;
	$self;
}

=method clients %options
Returns a LIST with the defined clients; M<Couch::DB::Client>-objects.

=option  role $role
=default role C<undef>
When defined, only return clients which are able to fulfill the
specific $role.
=cut

sub clients(%)
{	my ($self, %args) = @_;
	my $clients = $self->{CD_clients};

	my $role = delete $args{role};
	$role ? grep $_->canRole($role), @$clients : @$clients;
}

=method client $name
Returns the client with the specific $name (which defaults to the server's url).
=cut

sub client($)
{	my ($self, $name) = @_;
	$name = "$name" if blessed $name;
	first { $_->name eq $name } $self->clients;   # never many: no HASH needed
}

=method call $method, $path, %options
Call some couchDB server, to get work done.  This is the base for any
interaction with the server.  You should not need to call this yourself,
because module C<Couch::DB> is a complete implementation.

=option  delay BOOLEAN
=default delay C<false>
 [PARTIAL]
Do not execute the server call yet, but prepare it only in a way that
it can be combined with other clients in parallel.
See M<Couch::DB::Result> chapter L</DETAILS> about delayed requests.

=option  query HASH
=default query C<undef>
Query parameters for the request.

=option  send  HASH
=default send  C<undef>
The content to be sent with POST and PUT methods.
in those cases, even when there is nothing to pass on, simply to be
explicit about that.

=option  clients ARRAY|$role
=default clients C<undef>
Explicitly use only the specified clients (M<Couch::DB::Client>-objects)
for the query.  When none are given, then all are used (in order of
precedence).  When a $role (string) is provided, it is used to select
a subset of the defined clients.

=option  client M<Couch::DB::Client>
=default client C<undef>

=option  to_values CODE
=default to_values C<undef>
A function (sub) which transforms the data of the CouchDB answer into useful Perl
values and objects.  See M<Couch::DB::toPerl()>.
=cut

sub call($$%)
{	my ($self, $method, $path, %args) = @_;
	$args{method}   = $method;
	$args{path}     = $path;
	$args{query}  ||= {};

	my $headers     = $args{headers} ||= {};
	$headers->{Accept} ||= 'application/json';
	$headers->{'Content-Type'} ||= 'application/json';

use Data::Dumper;
warn "CALL ", Dumper \%args;

    defined $args{send} || ($method ne 'POST' && $method ne 'PUT')
		or panic "No send in $method $path";

	### On this level, we pick a client.  Extensions implement the transport.

	my @clients;
	if(my $client = delete $args{client})
	{	@clients = blessed $client ? $client : $self->client($client);
	}
	elsif(my $c = delete $args{clients})
	{	@clients = ref $c eq 'ARRAY' ? @$c : $self->clients(role => $c);
	}
	else
	{	@clients = $self->clients;
	}
	@clients or error __x"No clients can run {method} {path}.", method => $method, path => $path;

	my $introduced = $args{introduced};
	$self->check(exists $args{$_}, $_ => delete $args{$_}, "Endpoint '$method $path'")
		for qw/removed introduced deprecated/;

	my $result  = Couch::DB::Result->new(
		couch     => $self,
		to_values => delete $args{to_values},
		on_errors => delete $args{on_errors},
		on_final  => delete $args{on_final},
	);

  CLIENT:
	foreach my $client (@clients)
	{
		! $introduced || $client->version >= $introduced
			or next CLIENT;  # server release too old

		$self->_callClient($result, $client, %args)
			and last;
	}

	# The error from the last try will remain.
	$result;
}

sub _callClient { panic "must be extended" }

# Described in the DETAILS below
sub _resultsConfig($%)
{	my ($self, $args, @more) = @_;
	my %config;
	exists $args->{"_$_"} && ($config{$_} = delete $args->{"_$_"})
		for qw/delay client clients headers/;

	exists $args->{$_} && ($config{$_} = delete $args->{$_})
		for qw/on_error on_final/;

	while(@more)
	{	my ($key, $value) = (shift @more, shift @more);
		if($key eq 'headers')
		{	# Headers are added, as default only
			my $headers = $config{headers} ||= {};
			exists $headers->{$_} or ($headers->{$_} = $value->{$_}) for keys %$value;
			next;
		}
		elsif($key =~ /^on_/)
		{	# Events are added to list of events
			$config{$key} = exists $config{$key} ? [ flat $config{$key}, $value ] : $value;
		}
		else
		{	# Other parameters used as default
			exists $config{$key} or $config{$key} = $value;
		}
	}
	%config;
}

#-------------
=section Conversions

=method toPerl \%data, $type, @keys
Convert all fields with @keys in the %data HASH into object
of $type.  Fields which do not exist are ignored.

As default JSON to Perl translations are currently defined:
C<abs_uri>, C<epoch>, C<isotime>, C<mailtime>, C<version>, and
C<node>.
=cut

my %default_toperl = (  # sub ($couch, $name, $datum) returns value/object
	abs_uri   => sub { URI->new($_[2]) },
	epoch     => sub { DateTime->from_epoch(epoch => $_[2]) },
	isotime   => sub { DateTime::Format::ISO8601->parse_datetime($_[2]) },
	mailtime  => sub { DateTime::Format::Mail->parse_datetime($_[2]) },   # smart choice by CouchDB?
 	version   => sub { version->parse($_[2]) },
	node      => sub { $_[0]->node($_[2]) },
);

sub _toPerlHandler($)
{	my ($self, $type) = @_;
	$self->{CD_toperl}{$type} || $default_toperl{$type};
}

sub toPerl($$@)
{	my ($self, $data, $type) = (shift, shift, shift);
	my $conv  = $self->_toPerlHandler($type) or return $self;

	exists $data->{$_} && ($data->{$_} = $conv->($self, $_, $data->{$_}))
		for @_;

	$self;
}

=method listToPerl $set, $type, @data|\@data
Returns a LIST from all elements in the LIST @data or the ARRAY, each
converted from JSON to pure Perl according to rule $type.
=cut

sub listToPerl
{	my ($self, $name, $type) = (shift, shift, shift);
	my $conv  = $self->_toPerlHandler($type) or return flat @_;
	grep defined, map $conv->($self, $name, $_), flat @_;
}

=method toJSON \%data, $type, @keys
Convert the named fields in the %data into a JSON compatible format.
Fields which do not exist are left alone.
=cut

my %default_tojson = (  # sub ($couch, $name, $datum) returns JSON
	# All known backends support these booleans
	bool => sub { $_[2] ? JSON::PP::true : JSON::PP::false },

	# All known URL implementations correctly overload stringify
	uri  => sub { "$_[2]" },

	node => sub { my $n = $_[2]; blessed $n ? $n->name : $n },

	# In Perl, the int might come from text (for instance a configuration
	# file.  In that case, the JSON::XS will write "6".  But the server-side
	# JSON is type sensitive and may crash.
	int  => sub { defined $_[2] ? int($_[2]) : undef },
);

sub _toJsonHandler($)
{	my ($self, $type) = @_;
	$self->{CD_tojson}{$type} || $default_tojson{$type};
}

sub toJSON($@)
{	my ($self, $data, $type) = (shift, shift, shift);
	my $conv = $self->_toJsonHandler($type) or return $self;

	exists $data->{$_} && ($data->{$_} = $conv->($self, $_, $data->{$_}))
		for @_;

	$self;
}

=method toQuery \%data, $type, @keys
Convert the named fields in the %data HASH into a Query compatible
format.  Fields which do not exist are left alone.
=cut

# Extend/override the list of toJSON converters
my %default_toquery = (
	bool => sub { $_[2] ? 'true' : 'false' },
	json => sub { encode_json $_[2] },
);

sub _toQueryHandler($)
{	my ($self, $type) = @_;
	   $self->{CD_toquery}{$type} || $default_toquery{$type}
	|| $self->{CD_tojson}{$type}  || $default_tojson{$type};
}

sub toQuery($@)
{	my ($self, $data, $type) = (shift, shift, shift);
	my $conv = $self->_toQueryHandler($type) or return $self;

	exists $data->{$_} && ($data->{$_} = $conv->($self, $_, $data->{$_}))
		for @_;

	$self;
}

=method jsonText $json, %options
Convert the (complex) $json structure into serialized JSON.  By default, it
is beautified.

=option  compact BOOLEAN
=default compact C<false>
=cut

sub jsonText($%)
{	my ($self, $json, %args) = @_;
	JSON->new->pretty(not $args{compact})->encode($json);
}

=method check $condition, $change, $version, $what
If the $condition it true (usually the existence of some parameter), then
check whether api limitiations apply.

Parameter $change is either C<removed>, C<introduced>, or C<deprecated> (as
strings).  The C<version> is taken from the CouchDB API documentation.
The $what describes the element, to be used in error or warning messages.
=cut

my (%surpress_depr, %surpress_intro);

sub check($$$$)
{	defined $_[3] or return $_[0];
	my ($self, $element, $change, $version, $what) = @_;

	# API-doc versions are sometimes without 3rd part.
	my $cv = version->parse($version);

	if($change eq 'removed')
	{	$self->api < $cv
			or error __x"{what} got removed in {release}, but you specified api {api}.",
				what => $what, release => $version, api => $self->api;
	}
	elsif($change eq 'introduced')
	{	$self->api >= $cv && ! $surpress_intro{$what}++
			or warning __x"{what} was introduced in {release}, but you specified api {api}.",
				what => $what, release => $version, api => $self->api;
	}
	elsif($change eq 'deprecated')
	{	$self->api >= $cv && ! $surpress_depr{$what}++
			or warning __x"{what} got deprecated in api {release}.",
					what => $what, release => $version;
	}
	else { panic "$change $cv $what" }

	$self;
}

=method requestUUIDs $count, %options
 [CouchDB API "GET /_uuids", since 2.0, UNTESTED]
Returns UUIDs (Universally unique identifiers), when the call was
successful.  Better use M<freshUUIDs()>.  It is faster to use Perl
modules to generate UUIDs.
=cut

sub requestUUIDs($%)
{	my ($self, $count, %args) = @_;

	$self->call(GET => '/_uuids',
		introduced => '2.0.0',
		query      => { count => $count },
		$self->_resultsConfig(\%args),
	);
}

=method freshUUIDs $count, %options
 [UNTESTED]
Returns a $count number of UUIDs in a LIST.  This uses M<requestUUIDs()> to get
a bunch at the same time, for efficiency.  You may get fewer than you want, but
only when the server is not sending them.

=option  bulk INTEGER
=default bulk 50
When there are not enough UUIDs in stock, in how large chuncks should we ask for
more.
=cut

sub freshUUIDs($%)
{	my ($self, $count, %args) = @_;
	my $stock = $self->{CDC_uuids};
	my $bulk  = delete $args{bulk} || 50;

	while($count > @$stock)
	{	my $result = $self->requestUUIDs($bulk, _delay => 0) or last;
		push @$stock, @{$result->values->{uuids} || []};
	}

	splice @$stock, 0, $count;
}

#-------------

#### Extension which perform some tasks which are framework object specific.

# Returns the JSON structure which is part of the response by the CouchDB
# server.  Usually, this is the bofy of the response.  In multipart
# responses, it is the first part.
sub _extractAnswer($) { panic "must be extended" }

# The the decoded named extension from the multipart message
sub _attachment($$)   { panic "must be extended" }

# Extract the decoded body of the message
sub _messageContent($) { panic "must be extended" }

1;

#-------------
=chapter DETAILS

=section Thick interface

The CouchDB client interface is based on HTTP.  It is really easy to
create JSON, and then use a UserAgent to send it to the CouchDB server.
All other CPAN modules which support CouchDB stick on this level of
support; not C<Couch::DB>.

When your library is very low-level, your program needs to put effort
to create an abstraction around it itself.  In case the library offers
that abstraction, you need to write much less code.

The Perl programming language works with functions, methods, and
objects, so why would your libary require you to play with URLs?
So, C<Couch::DB> has the following extra features:
=over 4
=item *
Calls have a functional name, and are grouped into objects: the
endpoint URL processing is totally abstracted away;
=item *
Define multiple clients at the same time, for automatic fail-over,
read, write, and permission separation, or parallellism;
=item *
Resolving differences between CouchDB-server instances.  You may
even run different CouchDB versions on your nodes;
=item *
JSON-types do not match Perl's type concept: this module will
convert boolean and integer parameters (and more) from Perl to
JSON and back transparently;
=item *
Offer error handling and event processing on each call;
=item *
Event framework independent (currently only a Mojolicious connector).
=back

=section Using the CouchDB API

All methods which are marked with C<< [CouchDB API] >> are, as the name
says: client calls to some CouchDB server.  Often, this connects to a node
on your local server, but you can also connect to other servers and even
multiple servers.

All these API methods return a M<Couch::DB::Result> object, which can tell
you how the call worked, and the results.  The resulting object is overloaded
boolean to produce C<false> in case of an error.  So typically:

  my $couch  = Couch::DB::Mojolicious->new(version => '3.3.3');
  my $result = $couch->requestUUIDs(100);
  $result or die;

  my $uuids  = $result->values->{uuids};

This CouchDB library hides the fact that endpoint C</_uuids> has been called.
It also hides the client (UserAgent) which was used to collect the data.

=subsection Type conversions

With the C<values()> method, conversions between JSON syntax and pure
Perl are done.

More importantly: this library also converts parameters from Perl space
into JSON space.  POST and PUT parameters travel in JSON documents.
In JSON, a boolean is C<true> and C<false> (without quotes).  In Perl,
these are C<undef> and C<1> (and many alternatives).  For anything besides
your own documents, C<Couch::DB> will totally hide these differences
for you!

=subsection Generic parameters

Each method which is labeled C<< [CouchDB API] >> also accepts a few options
which are controlling the calling progress.  They are available everywhere,
hence no-where documented explicitly.  Those options start with an underscore (C<_>)
or with C<on_> (events).

At the moment, the following %options are supported:
=over 4
=item * C<_delay> BOOLEAN, default C<false>
Do not perform and wait for the actual call, but prepare it to be used in parallel
querying.  TO BE IMPLEMENTED/DOCUMENTED.

=item * C<_client> $client-object or -name
Use only the specified client (=server) to perform the call.

=item * C<_clients> ARRAY-of-clients or a role
Use any of the specified clients to perform the call.  When not an ARRAY, the
parameter is a C<role>: select all clients which can perform that role (the
logged-in user of that client is allowed to perform that task).

=item * C<_headers> HASH
Add headers to the request.  When applicable (for instance, the C<Accept>-header)
this will overrule the internally calculated defaults.
=back

Besides, at the moment we support the following events:

=over 4
=item * on_error CODE or ARRAY-of-CODE
A CODE (sub) which is called when the interaction with the server has been completed
without success.  The CODE gets the result object as only parameter.

=item * on_final CODE or ARRAY-of-CODE
A CODE (sub) which is called when the interaction with the server has been completed.
This may happen much later, when combined with C<_delay>.  The CODE gets the result
object as only parameter.
=cut
