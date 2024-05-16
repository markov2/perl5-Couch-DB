# SPDX-FileCopyrightText: 2024 Mark Overmeer <mark@open-console.eu>
# SPDX-License-Identifier: EUPL-1.2-or-later

package Couch::DB;
use version;

use Log::Report 'couch-db';

use Couch::DB::Util   qw(flat);
use Couch::DB::Client ();
use Couch::DB::Node   ();

use Scalar::Util      qw(blessed);
use List::Util        qw(first);
use DateTime          ();
use DateTime::Format::Mail    ();
use DateTime::Format::ISO8601 ();
use URI               ();
use JSON              ();
use Storable          qw/dclone/;

use constant
{	DEFAULT_SERVER => 'http://127.0.0.1:5984',
};

my (%default_toperl, %default_tojson);

=chapter NAME

Couch::DB - CouchDB backend framework

=chapter SYNOPSIS

   use Couch::DB::Mojo ();
   my $couch = Couch::DB::Mojo->new;
   my $db    = $couch->db('my-db');  # Couch::DB::Database object

=chapter DESCRIPTION

When this module was written, there were already a large number
of CouchDB implementations available on CPAN.  Still, there was
a need for one more.  This implementation has the following extra
features:
=over 4
=item *
JSON/Javascript's types do not match Perl: this module will
convert boolean and integer parameters from perl to JSON transparently;
=item *
Validation of parameters, accepting and resolving differences between
CouchDB server instances.  You may even run different CouchDB versions
on your nodes;
=item *
Only supports interfaces which uses Promises/Futures, to force thinking
in parallellism.
=item *
Automatic fail-over between server connections, when nodes disappear.
=item *
Event framework independent (at least in theory)
=back

=chapter METHODS

=section Constructors

=method new %options

=requires version $version
You have to specify the version of the server you expect to answer your
queries.  M<Couch::DB> tries to hide differences between your expectations
and the reality.

The $version can be a string or a version object (see "man version").

=option  server URL
=default server "http://127.0.0.1:5984"
The default server to connect to, by URL.  See C<< etc/local.ini[chttpd] >>
The server will be named 'local'.

You can add more servers using M<addClient()>.  When you do not want this
default client to be created as well, then explicitly set C<undef> here.

=option  username STRING
=default username C<undef>
Used to login to the default server.

=option  password STRING
=default password C<undef>

=option  to_perl HASH
=default to_perl C<< +{ } >>
A table with converter name and CODE, to override/add the default JSON to PERL
object conversions for M<value()>.

=option  to_json HASH
=default to_json C<< +{ } >>
A table with converter name and CODE, to override/add the default PERL to JSON
object conversions for sending structures.
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
	$self->{CD_api} = blessed $v && $v->isa('version') ? $v : version->parse($v);

	$self->{CD_clients} = [];
	my $username = delete $args->{username};
	my $password = delete $args->{password};

	if(! exists $args->{server} || defined $args->{server})
	{	my $server = delete $args->{server} || DEFAULT_SERVER;
		$self->createClient(server => $server, name => 'local',
			username => $username, password => $password);
	}

	$self->{CD_toperl} = +{ %default_toperl, %{delete $args->{to_perl} || {}} };
	$self->{CD_tojson} = +{ %default_tojson, %{delete $args->{to_json} || {}} };

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
=section Server connections

=method createClient %options
Create a client object which handles a server.  All options are passed
to M<Couch::DB::Client>.  The C<couch> parameter is added for you.
The client will also be added via M<addClient()>, and is returned.
=cut

sub createClient(%)
{	my ($self, %args) = @_;
	my $client = Couch::DB::Client->new(couch => $self, %args);
	$self->addClient($client);
	$client;
}

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

=method clients
Returns a LIST with the defined clients; M<Couch::DB::Client>-objects.
=cut

sub clients() { @{$_[0]->{CD_clients}} }

=method client $name
Returns the client with the specific $name (which defaults to the server's url).
=cut

sub client($)
{	my ($self, $name) = @_;
	$name = "$name" if blessed $name;
	first { $_->name eq $name } $self->clients;   # never many: no HASH needed
}

=method call $method, $path, %options
Call some couchDB server, to get work done.

=option  delay BOOLEAN
=default delay C<false>
See M<Couch::DB::Result> chapter DETAILS about delayed requests.

=option  query HASH
=default query C<< +{ } >>
Query parameters for the request.

=option  data  HASH
=default data  C<< +{ } >>

=option  clients ARRAY
=default clients C<undef>
Explicitly use only the specified clients (M<Couch::DB::Client> objects) for the query.
When none are given, then all are used (in order of precedence).

=option  client M<Couch::DB::Client>
=default client C<undef>

=option  to_values CODE
=default to_values C<undef>
A function (sub) which transforms the data of the CouchDB answer into useful Perl
values and objects.  See M<Couch::DB::toPerl()>.
=cut

my %surpress_depr;
sub __couchdb_version($)
{	my $v = shift or return;
	version->parse($v =~ /^\d+\.\d+$/ ? "$v.0" : $v);  # sometime without 3rd
}

my %to_query = (
	'JSON::PP::Boolean' => sub { $_[0] ? 'true' : 'false' },
	'Couch::DB::Node'   => sub { $_[0]->name },
);

sub call($$%)
{	my ($self, $method, $path, %args) = @_;
	$args{method} = $method;
	$args{path}   = $path;

	if(my $query = delete $args{query}) 
	{	# Cleanup the query
		my %query = %$query;

		foreach my $key (keys %$query)
		{	my $conv = $to_query{ref $query{$key}} or next;
			$query{$key} = $conv->($query{$key});
		}

		$args{query} = \%query;
	}

	### On this level, we pick a client.  Extensions implement the transport.

	my @clients = flat delete $args{client}, delete $args{clients};
	@clients or @clients = $self->clients;

	my $removed = __couchdb_version delete $args{removed};
	if($removed && $self->api >= $removed)
	{	error __x"Using {what} was deprecated in {release}, but you specified api {api}.",
			what => "$method($path)", release => $removed, api => $self->api;
	}

	my $introduced = __couchdb_version delete $args{introduced};
	if($introduced && $introduced <= $self->api)
	{	warning __x"Using {what}, introduced in {release} but you specified api {api}.",
			what => "$method($path)", release => $introduced, api => $self->api;
	}

	my $deprecated = __couchdb_version delete $args{deprecated};
	if($deprecated && $self->api >= $deprecated && ! $surpress_depr{"$method:$path"}++)
	{	warning __x"Using {what}, which got deprecated in {release}.",
			what => "$method($path)", release => $deprecated;
	}

	my $result  = Couch::DB::Result->new(
		couch     => $self,
		to_values => delete $args{to_values},
	);

  CLIENT:
	foreach my $client (@clients)
	{
		! $introduced || $introduced <= $client->version
			or next CLIENT;  # server release too old

		$self->_callClient($result, $client, %args)
			and last;
	}

	# The error from the last try will remain.
	$result;
}

sub _callClient { ... }

sub _resultsConfig($)
{	my ($self, $args) = @_;
	map +($_ => delete $args->{$_}), qw/delay client clients on_error on_final/;
}

#-------------
=section Interface

=method createDatabase $name, %options
See M<Couch::DB::Database::create()>
=cut

sub createDatabase($%)
{	my ($self, $name, %args) = @_;
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

	$self->call(POST => '/_search_analyze',
		introduced => '3.0',
		$self->_resultsConfig(\%args),
	);
}

=method requestUUIDs $count, %options
[CouchDB API "GET /_uuids", since 2.0, UNTESTED]
Returns a LIST of UUIDS, when the call was successful.  Cannot be delayed.
=cut

sub requestUUIDs($%)
{	my ($self, $count, %args) = @_;

	$self->call(GET => '/_uuids',
		introduced => '2.0',
		query      => { count => $count },
		$self->_resultsConfig(\%args),
	);
}

=method freshUUIDs $count, %options
[UNTESTED]
Returns a $count number of UUIDs in a LIST.  This uses M<requestUUIDs() to get
a bunch at the same time, for efficiency.  You may get fewer than you want, but
only when the server is not sending them.

=option  bulk INTEGER
=default bulk 50
When there are not enough UUIDs in stock, in how large sets should we ask them.
=cut

sub freshUUIDs($%)
{	my ($self, $count, %args) = @_;
	my $stock = $self->{CDC_uuids};
	my $bulk  = delete $args{bulk} || 50;

	while($count > @$stock)
	{	my $result = $self->requestUUIDs($bulk) or last;
		push @$stock, @{$result->doc->data->{uuids} || []};
	}

	splice @$stock, 0, $count;
}

#-------------
=section Interfaces

=method node $name
Returns a M<Couch::DB::Node> object with the $name.  If it does not exist
yet, it gets created, otherwise reused.
=cut

sub node($)
{	my ($self, $name) = @_;
	$self->{CD_nodes}{$name} ||= Couch::DB::Node->new(name => $name, couch => $self);
}

=method cluster
Returns a M<Couch::DB::Cluster> object, which organizes calls to
manipulate replication, sharding, and related jobs.
=cut

sub cluster() { $_[0]->{CD_cluster} ||= Couch::DB::Cluster->new(couch => $_[0]) }

#-------------
=section Conversions

=method toPerl \%data, $type, @keys
Convert all fields with @keys in the $data into object of $type.
Fields which do not exist are left alone.
=cut

%default_toperl = (  # sub ($couch, $name, $datum) returns value/object
	abs_uri   => sub { URI->new($_[2]) },
	epoch     => sub { DateTime->from_epoch(epoch => $_[2]) },
	isotime   => sub { DateTime::Format::ISO8601->parse_datetime($_[2]) },
	mailtime  => sub { DateTime::Format::Mail->parse_datetime($_[2]) },   # smart choice by CouchDB?
 	version   => sub { version->parse($_[2]) },
	node      => sub { $_[0]->node($_[2]) },
);

sub _toPerlHandler($) { $_[0]->{CD_toperl}{$_[1]} }
sub toPerl($$@)
{	my ($self, $data, $type) = (shift, shift, shift);
	my $conv  = $self->_toPerlHandler($type) or return $self;

	exists $data->{$_} && ($data->{$_} = $conv->($self, $_, $data->{$_}))
		for @_;

	$self;
}

=method listToPerl $set, $type, @data|\@data
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

%default_tojson = (  # sub ($couch, $name, $datum) returns JSON
	bool      => sub { $_[2] ? JSON::PP::true : JSON::PP::false },
	uri       => sub { "$_[2]" },
	node      => sub { my $n = $_[2]; blessed $n ? $n->name : undef },
);

sub _toJsonHandler($) { $_[0]->{CD_tojson}{$_[1]} }
sub toJSON($@)
{	my ($self, $data, $type) = (shift, shift, shift);
	my $conv = $self->_toJsonHandler($type) or return $self;

	exists $data->{$_} && ($data->{$_} = $conv->($self, $_, $data->{$_}))
		for @_;

	foreach (@_)
	{	exists $data->{$_} or next;
		$data->{$_} = $data->{$_} ? JSON::PP::true : JSON::PP::false;
	}
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

#-------------
=section Other
=cut

1;
