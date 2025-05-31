# SPDX-FileCopyrightText: 2024 Mark Overmeer <mark@overmeer.net>
# SPDX-License-Identifier: Artistic-2.0

package Couch::DB;
use version;

use Log::Report 'couch-db';

use Couch::DB::Client   ();
use Couch::DB::Cluster  ();
use Couch::DB::Database ();
use Couch::DB::Design   ();
use Couch::DB::Node     ();
use Couch::DB::Util     qw(flat);

use DateTime          ();
use DateTime::Format::ISO8601 ();
use DateTime::Format::Mail    ();
use JSON              qw/encode_json/;
use List::Util        qw(first min);
use Scalar::Util      qw(blessed);
use Storable          qw/dclone/;
use URI               ();
use URI::Escape       qw/uri_escape uri_unescape/;

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

Also, open F<https://perl.overmeer.net/couch-db/reference.html>
in a browser window, as useful cross-reference: parameters for CouchDB
are not documented in this Perl documentation!

B<Please read> the L</DETAILS> section, further down, at least once
before you start!

=section Early adopters

B<Be warned> that this module is really new.  The 127 different endpoints
that the CouchDB 3.3.3 API defines, are grouped and combined.  The result
is often not tested, and certainly not battle ready.  Please, report
the result of calls which which are currently flagged "UNTESTED".

B<Please help> me fix issues by reporting them.  Bugs will be solved within
a day.  Please, contribute ideas to make the use of the module lighter.
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
see the internals in the resulting code.  Knowing the CouchDB API,
it is usually immediately clear where to find a certain end-point:
C<< /{db} >> will be in M<Couch::DB::Database>.  A major exception is
anything what has to do with replication and sharding: this is bundled
in M<Couch::DB::Cluster>.

Have a look at F<https://perl.overmeer.net/couch-db/reference.html>.
Keep that page open in your browser while developing.

=chapter METHODS

=section Constructors

=c_method new %options
Create a relation with a CouchDB server (cluster).  You should use
totally separated M<Couch::DB>-objects for totally separate database
clusters.  B<Note:> you can only instantiate extensions of this class.

When you do not specify a C<server>-url, but have an environment variable
C<PERL_COUCH_DB_SERVER>, then server url, username, and password are
derived from it.

=requires api $version
You MUST specify the version of the server you expect to answer your
queries.  M<Couch::DB> tries to hide differences between your expectations
and the reality of the server version.

The $version can be a string or a version object (see "man version").

=option  server URL
=default server "http://127.0.0.1:5984"
The default server to connect to, by URL.  See C<< etc/local.ini[chttpd] >>
This server will be named C<_local>.

You can add more servers using M<addClient()>.  In such case, you probably
do not want this default client to be created as well.  To achieve this,
explicitly set C<server =E<gt> undef>.

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
A table mapping converter name to CODE, to override/add the default JSON to PERL
object conversions for M<Couch::DB::Result::values()>.  See M<toPerl()> and M<listToPerl()>.

=option  to_json HASH
=default to_json C<< +{ } >>
A table mapping converter name to CODE, to override/add the default PERL to JSON
object conversions for sending structures.  See M<toJSON()>.

=option  to_query HASH
=default to_query C<< +{ } >>
A table mapping converter name to CODE, to override/add the default PERL to URL
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
Declare a database.  The database may not exist yet: calling this
method does nothing with the CouchDB server.

  my $db = $couch->db('authors');
  $db->ping or $db->create(...);

=cut

sub db($%)
{	my ($self, $name, %args) = @_;
	Couch::DB::Database->new(name => $name, couch => $self, %args);
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
=section Unrelated calls

=method searchAnalyze %options
 [CouchDB API "POST /_search_analyze", since 3.0, UNTESTED]

Check what the build-in Lucene tokenizer(s) will do with your text.

=requires analyzer KIND
=requires text STRING
=cut

#XXX the API-doc might be mistaken, calling the "analyzer" parameter "field".

sub searchAnalyze(%)
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

=method requestUUIDs $count, %options
 [CouchDB API "GET /_uuids", since 2.0]

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
	my $stock = $self->{CDC_uuids} || [];
	my $bulk  = delete $args{bulk} || 50;

	while($count > @$stock)
	{	my $result = $self->requestUUIDs($bulk, _delay => 0) or last;
		push @$stock, @{$result->values->{uuids} || []};
	}

	splice @$stock, 0, $count;
}

#-------------
=section Processing

The methods in this section implement the CouchDB API.  You should
usually not need to use these yourself, as this libary abstracts them.

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
interaction with the server.

B<Note:> you should probably not use this method yourself: all endpoint of
CouchDB are available via a nice, abstract wrapper.

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

=option  on_chain   CODE
=default on_chain   C<undef>
When the call ends successfully, then run the chain code.  Event
C<on_error> and C<on_final> are only called on the last results of
the chain.

=option  clients ARRAY|$role
=default clients C<undef>
Explicitly use only the specified clients (M<Couch::DB::Client>-objects)
for the query.  When none are given, then all are used (in order of
precedence).  When a $role (string) is provided, it is used to select
a subset of the defined clients.

=option  client M<Couch::DB::Client>|$name
=default client C<undef>
Select a specific client connection to be used, as object or by name.

=option  on_values CODE
=default on_values C<undef>
A function (sub) which transforms the data of the CouchDB answer into
useful Perl values and objects.  See M<Couch::DB::toPerl()>.
The function is called with the result and a partially or unprocessed
reponse (answer).  That data shall not be modified.  Return a new
data-structure which contains the processed information, which may
reuse parts which are not modified.

=option  paging HASH
=default paging {}
When the endpoint support paging, then its needed configuration
data has been collected in here.  This enables the use of C<_succeed>,
C<_page>, C<skip>, and friends.  See examples in section L</Pagination>.
=cut

sub call($$%)
{	my ($self, $method, $path, %args) = @_;
	$args{method}   = $method;
	$args{path}     = $path;
	$args{query}  ||= my $query = {};

	my $headers     = $args{headers} ||= {};
	$headers->{Accept} ||= 'application/json';
	$headers->{'Content-Type'} ||= 'application/json';

#use Data::Dumper;
#warn "CALL ", Dumper \%args;

    my $send = $args{send};
	defined $send || ($method ne 'POST' && $method ne 'PUT')
		or panic "No send in $method $path";

	my $introduced = $args{introduced};
	$self->check(exists $args{$_}, $_ => delete $args{$_}, "Endpoint '$method $path'")
		for qw/removed introduced deprecated/;

	### On this level, we pick a client.  Extensions implement the transport.

	my $paging = $args{paging};
	if($paging && (my $client = $paging->{client}))
	{	# No free choices for clients once we are on page 2
		$args{client} = $client;
		delete $args{clients};
	}

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

	my $result  = Couch::DB::Result->new(
		couch     => $self,
		on_values => $args{on_values},
		on_error  => $args{on_error},
		on_final  => $args{on_final},
		on_chain  => $args{on_chain},
		paging    => $paging,
	);

  CLIENT:
	foreach my $client (@clients)
	{
		! $introduced || $client->version >= $introduced
			or next CLIENT;  # server release too old

		if($paging)
		{	do
			{	# Merge paging setting into the request
	    		my $paging_setting = $self->_pageRequest($paging, $method, $query, $send);
				# Fix for GET only
				if ($method eq 'GET') {
					%{ $args{query} } = (
						%{ $args{query} },    # bestehende Einträge
						%{ $paging_setting }, # neue Einträge dazu
					);
					# bookmark does not like skip
					if (exists $args{query}->{bookmark}) {
						delete $args{query}->{skip};
					}
				}

				$self->_callClient($result, $client, %args);

				$result
					or next CLIENT;  # fail
			} while $result->pageIsPartial;

			last CLIENT;
		}
		else
		{	# Non-paging commands are simple
			$self->_callClient($result, $client, %args)
				and last CLIENT;
		}
	}

	# The error from the last try will remain.
	$result;
}

sub _callClient { panic "must be extended" }

# Described in the DETAILS below, non-paging commands
sub _resultsConfig($%)
{	my ($self, $args, @more) = @_;
	my %config;

	exists $args->{"_$_"} && ($config{$_} = delete $args->{"_$_"})
		for qw/delay client clients headers/;

	exists $args->{$_} && (push @{$config{$_}}, delete $args->{$_})
		for qw/on_error on_final on_chain on_values/;

	while(@more)
	{	my ($key, $value) = (shift @more, shift @more);
		if($key eq '_headers')
		{	# Headers are added, as default only
			my $headers = $config{headers} ||= {};
			exists $headers->{$_} or ($headers->{$_} = $value->{$_}) for keys %$value;
			next;
		}
		elsif($key =~ /^on_/)
		{	push @{$config{$key}}, $value;
		}
		else
		{	# Other parameters used as default
			exists $config{$key} or $config{$key} = $value;
		}
	}

	keys %$args and warn "Unused call parameters: ", join ', ', sort keys %$args;
	%config;
}

# Described in the DETAILS below, paging commands
sub _resultsPaging($%)
{	my ($self, $args, @more) = @_;

	my %state = (harvested => []);
	my $succ;  # successor
	if(my $succeeds = delete $args->{_succeed})
	{	delete $args->{_clients}; # no client switching within paging

		if(blessed $succeeds && $succeeds->isa('Couch::DB::Result'))
		{	# continue from living previous result
			$succ = $succeeds->nextPageSettings;
			$args->{_client} = $succeeds->client;
		}
		else
		{	# continue from resurrected from Result->pagingState()
			my $h = $succeeds->{harvester}
				or panic "_succeed does not contain data from pagingState() nor is a Result object.";

			$h eq 'DEFAULT' || $args->{_harvester}
				or panic "Harvester does not survive pagingState(), resupply.";

			$succeeds->{map} eq 'NONE' || $args->{_map}
				or panic "Map does not survive pagingState(), resupply.";

			$succ  = $succeeds;
			$args->{_client} = $succeeds->{client};
		}
	}

	$state{start}     = $succ->{start} || 0;
	$state{skip}      = delete $args->{skip} || 0;
	$state{all}       = delete $args->{_all} || 0;
	$state{map}       = my $map = delete $args->{_map} || $succ->{map};
	$state{harvester} = my $harvester = delete $args->{_harvester} || $succ->{harvester};
	$state{page_size} = my $size = delete $args->{_page_size} || $succ->{page_size} || 25;
	$state{req_max}   = delete $args->{limit} || $succ->{req_max} || 100;

	if(my $page = delete $args->{_page})
	{	$state{start}  = ($page - 1) * $state{page_size};
	}

	$state{bookmarks} = $succ->{bookmarks} ||= { };
	if(my $bm = delete $args->{_bookmark})
	{	$state{bookmarks}{$state{start}} = $bm;
	}

	$harvester ||= sub { my $v = $_[0]->values; $v->{docs} || $v->{rows} };
	my $harvest = sub {
		my $result = shift or return;
		my @found  = flat $harvester->($result);
		@found     = map $map->($result, $_), @found if $map;
		$result->_pageAdd($result->answer->{bookmark}, @found);  # also call with 0
	};

	# When less elements are returned
	return
	( $self->_resultsConfig($args, @more, on_final => $harvest),
	   paging => \%state,
	);
}

sub _pageRequest($$$$)
{	my ($self, $paging, $method, $query, $send) = @_;
	my $params   = $method eq 'GET' ? $query : $send;
	my $progress = @{$paging->{harvested}};      # within the page
	my $start    = $paging->{start};

	$params->{limit} = $paging->{all} ? $paging->{req_max} : (min $paging->{page_size} - $progress, $paging->{req_max});

	if(my $bookmark = $paging->{bookmarks}{$start + $progress})
	{	$params->{bookmark} = $bookmark;
		$params->{skip}     = $paging->{skip};
	}
	else
	{	delete $params->{bookmark};
		$params->{skip}     = $start + $paging->{skip} + $progress;
	}
	return $params;
}

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
	bool => sub { $_[2] ? $JSON::true : $JSON::false },

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
Produce compact (no white-space) JSON.
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
{	$_[1] or return $_[0];
	my ($self, $condition, $change, $version, $what) = @_;

	# API-doc versions are sometimes without 3rd part.
	my $cv = version->parse($version);

	if($change eq 'removed')
	{	$self->api < $cv
			or error __x"{what} got removed in {release}, but you specified api {api}.",
				what => $what, release => $version, api => $self->api;
	}
	elsif($change eq 'introduced')
	{	$self->api >= $cv || $surpress_intro{$what}++
			or warning __x"{what} was introduced in {release}, but you specified api {api}.",
				what => $what, release => $version, api => $self->api;
	}
	elsif($change eq 'deprecated')
	{	$self->api >= $cv || $surpress_depr{$what}++
			or warning __x"{what} got deprecated in api {release}.",
					what => $what, release => $version;
	}
	else { panic "$change $cv $what" }

	$self;
}

#-------------

#### Extension which perform some tasks which are framework object specific.

# Returns the JSON structure which is part of the response by the CouchDB
# server.  Usually, this is the body of the response.  In multipart
# responses, it is the first part.
sub _extractAnswer($)  { panic "must be extended" }

# The the decoded named extension from the multipart message
sub _attachment($$)    { panic "must be extended" }

# Extract the decoded body of the message
sub _messageContent($) { panic "must be extended" }

1;

#-------------
=chapter DETAILS

=section Thick interface

The CouchDB client interface is based on HTTP.  It is really easy to
construct a JSON, and then use a UserAgent to send it to the CouchDB
server.  All other CPAN modules which support CouchDB stick on this
level of support; except C<Couch::DB>.

When your library is very low-level, your program needs to put effort to
create an abstraction around the interface it itself.  When the library
offers that abstraction already, you need to write much less code!

The Perl programming language works with functions, methods, and
objects, so why would your libary require you to play with URLs?
So, C<Couch::DB> has the following extra features:
=over 4
=item *
Calls have a functional name, and are grouped into classes: the
endpoint URL processing is totally abstracted away;
=item *
Define multiple clients at the same time, for automatic fail-over,
read, write, and permission separation, or parallellism;
=item *
Resolving differences between CouchDB-server versions.  You may
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

You could also write

  my $uuids  = $couch->requestUUIDs(100)->values->{uuids};

because "values()" will terminate when the database call did not result
in a successful answer.  Last alternative:

   my @uuids = $couch->freshUUIDs(100);

Besides calls, there are all kinds of facility methods, which add
further abstraction from the server connection.

=subsection Type conversions

With the M<Couch::DB::Result::values()> method, conversions between JSON
syntax and pure Perl are done.  This also hides database interface changes
for you, based on your M<new(api)> setting.  Avoid M<Couch::DB::Result::answer()>,
which gives the uninterpreted, unabstracted results.

This library also converts parameters from Perl space into JSON space.
POST and PUT parameters travel in JSON documents.  In JSON, a boolean is
C<true> and C<false> (without quotes).  In Perl, these are C<undef> and
C<1> (and many alternatives).  For anything besides your own documents,
C<Couch::DB> will totally hide these differences for you!

=subsection Generic parameters

Each method which is labeled C<< [CouchDB API] >> also accepts a few options
which are controlling the calling progress.  These are available everywhere,
hence no-where documented explicitly.  Those options start with an underscore (C<_>)
or with C<on_> (events).

At the moment, the following C<%options> are supported everywhere:
=over 4
=item * C<_delay> =E<gt> BOOLEAN, default C<false>
Do not perform and wait for the actual call, but prepare it to be used in parallel
querying.  TO BE IMPLEMENTED/DOCUMENTED.

=item * C<_client> =E<gt> $client-object or -name
Use only the specified client (=server) to perform the call.

=item * C<_clients> =E<gt> ARRAY-of-clients or a role
Use any of the specified clients to perform the call.  When not an ARRAY, the
parameter is a C<role>: select all clients which can perform that role (the
logged-in user of that client is allowed to perform that task).

=item * C<_headers> =E<gt> HASH
Add headers to the request.  When applicable (for instance, the C<Accept>-header)
this will overrule the internally calculated defaults.
=back

Besides, at the moment we support the following events:

=over 4
=item * C<on_error> =E<gt> CODE or ARRAY-of-CODE
A CODE (sub) which is called when the interaction with the server has
been completed without success.  The CODE gets the result object as
only parameter.

=item * C<on_final> =E<gt> CODE or ARRAY-of-CODE
A CODE (sub) which is called when the interaction with the server has
been completed.  This may happen much later, when combined with C<_delay>.
The CODE gets the result object as only parameter, and returns a result
object which might be different... as calls can be chained.

=item * C<on_chain> =E<gt> CODE
Run the CODE after the call has been processed.  It works as if the
changed logic is run after the call, with the difference is that this
next step is defined before the call has been made.  This sometimes
produces a nicer interface (like paging).

=item * C<on_values> =E<gt> CODE
Run the CODE on the result on the returned JSON data, to translate the
raw C<answer()> into C<values()>.  Wherever seemed useful, this is
already hidden for you.  However: there may be cases where you want to
add changes.
=back

=section Pagination

Searches tend to give a large number of results.  CouchDB calls will
refuse to return too many answers at a time (typically 25).  When you
need more results, you will need more calls.

To get more answers, there are two mechanisms: some calls provide a
C<skip> and C<limit> only.  Other calls implement the more sofisticated
bookmark mechanism.  Both mechanisms are abstracted away by the
C<_succeed> mechanism.

B<Be aware> that you shall provide the same query parameters to each
call of the search method.  Succession may be broken when you change
some parameters: it is not fully documented which ones are needed to
continue, so simply pass all again.  Probably, it is save to change
the C<limit> between pages.

To manage paged results, selected calls support the following options:

=over 4
=item * C<all> =E<gt> BOOLEAN (default false)
Return all results at once.  This may involve multiple calls, like when
the number of results is larger than what the server wants to produce
in one go.

Do not use this when you expect many or large results.  Maybe in
combination with C<_map>.

=item * C<_page> =E<gt> INTEGER (default 1)
Start-point of returned results, for calls which support paging.
Pages are numbered starting from 1.  When available, bookmarks will
be used for next pages.  Succeeding searches will automatically move
through pages (see examples)

=item * C<_page_size> =E<gt> INTEGER (default 25)
The CouchDB server will often not give you more than 25 or 50 answers
at a time, but you do not want to know.

=item * C<_succeed> =E<gt> $result or $result->paging
Make this query as successor of a previous query.  Some requests support
paging (via bookmarks).  See examples in a section below.

=item * C<_harvester> =E<gt> CODE
How or what to extract per request.  You may add other information,
like collecting response objects.  The CODE returns the extract LIST of
objects/elements. Collection for a page stops once that combined list
reaches C<_page_size>.

=item * C<_bookmark> =E<gt> STRING
If you accidentally know the bookmark for the search.  Usually, this is
automatically picked-up via C<_succeed>.

=item * C<_map> =E<gt> CODE
Call the CODE on each of the (defined) harvested page elements.  The CODE
is called with the result object, and one of the harvested elements.  When
a single page requires multiple requests to the CouchDB server, this map
will happen on the moment each response has been received, which may help
to create a better interactive experience.

Your CODE may return the harvested object, but also something small
(even undef) which will free-up the memory use of the object immediately.
However: at least return a single scalar (it will be returned in the
"page"), because an empty list signals "end of results".

=item * C<skip> =E<gt> INTEGER
Do not return this amount of first following elements.
B<Be warned:> use as C<%option>, not as search parameter.

=item * C<limit> =E<gt> INTEGER
Do not request more than C<limit> number of results per request.  May be
less than C<_page_size>.
B<Be warned:> use as C<%option>, not as search parameter.
=back

=example paging through result
Get page by page, where you may use the C<limit> parameter to request
for a number of elements.  Do not use C<skip>, except in the first call.
The C<_succeed> handling will play tricks with C<_page>, C<_harvester>,
and C<_client>, which you do not wish to know.

  my $page1 = $couch->find(\%search, limit => 12, skip => 300);
  my $docs1 = $page1->page;
  my $page2 = $couch->find(\%search, _succeed => $page1);
  my $docs2 = $page2->page;

=example paging via a session
When you cannot ask for pages within a single continuous process, because
the page is shown to a user who has to take action to see an other page,
then save the pagingState.

The state cannot contain code references, so when you have a specific
harvester or map, then you need to resupply those.

  my $page1 = $couch->find(\%search);
  my $docs1 = $page1->page;
  $session->save(current => serialized $page1->pagingState);
  ...
  my $prev  = deserialize $session->load('current');
  my $page2 = $couch->find(\%search, _succeed => $prev);
  my $docs2 = $page2->page;

=example get all results in a loop
Handle the responses which are coming in one by one.  This is useful
when the documents (with attachements?) are large.  Each C<$list>
is a new result object.

  my $list;
  while($list = $couch->find(\%search, _succeed => $list))
  {   my $docs = $list->page;
      @$docs or last;    # nothing left
      ...;    # use the docs
  }
  $list or die "Stopped somewhere with ". $list->message;

=example get one page of results
You can jump back and forward in the pages: bookmarks will remember the
pages already seen.

  my $page4 = $couch->find(\%search,
	limit      => 10,  # results per server request
	_page_size => 50,  # results until complete
    _page      =>  4,  # start point, may use bookmark
    _harvester => sub { $_[0]->values->{docs} }, # default
  );
  my $docs4 = $page4->page;
  my $page5 = $couch->find(\%search, _succeed => $page4);
  my $docs5 = $page5->page;

=example get all results in one call
Do not attempt this unless you know there there is a limited number of
results, maybe just a bit more than a page.

  my $all   = $couch->find(\%search, _all => 1) or die;
  my $docs6 = $all->page;

=example processing results when they arrive
When a page (may) require multiple calls to the server, this may enhance
the user experience.

  sub do_something($$) { my ($result, $doc) = @_; ...; 42 }
  my $all = $couch->find(\%search, _all => 1, _map => \&do_something);
  # $all->page will now show elements containing '42'.

=cut
