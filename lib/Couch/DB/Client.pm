# SPDX-FileCopyrightText: 2024 Mark Overmeer <mark@open-console.eu>
# SPDX-License-Identifier: EUPL-1.2-or-later

package Couch::DB::Client;

use Couch::DB::Util;
use Couch::DB::Result;

use Log::Report 'couch-db';

use Scalar::Util    qw(weaken);
use MIME::Base64    qw(encode_base64);

=chapter NAME

Couch::DB::Client - connect to a CouchDB node

=chapter SYNOPSIS

  my $client = Couch::DB::Client->new(couch => $couchdb, ...);
  $couchdb->addClient($client);

  my $client = $couchdb->createClient(...);  # same in one step

  # Even simpler
  my $couchdb = Couch::DB::Mojo->new(server => ...);
  my $client  = ($couchdb->clients)[0];      # usually not needed

=chapter DESCRIPTION

Connect to a couchDB node, potentially a cluster.

=chapter METHODS

=section Constructors

=c_method new %options

=requires couch  M<Couch::DB>-object

=requires server URL-object
Pass a URL-object which fits the framework your choose.

=requires user_agent UserAgent-Object
Pass a UserAgent-object which fits the framework your choose.

=option  name STRING
=default name C<server>
A good symbolic name for the client may make it more readible.
Defaults to the location of the server.

=option  username STRING
=default username C<undef>

=option  password STRING
=default password C<undef>

=option  headers HASH
=default headers <a few>
=cut

sub new(@) { (bless {}, shift)->init( {@_} ) }

sub init($)
{	my ($self, $args) = @_;
	$self->{CDC_server} = my $server = delete $args->{server} or panic "Requires 'server'";
	$self->{CDC_name}   = delete $args->{name} || "$server";
	$self->{CDC_ua}     = delete $args->{user_agent} or panic "Requires 'user_agent'";

	$self->{CDC_couch}  = delete $args->{couch}      or panic "Requires 'couch'";
	weaken $self->{CDC_couch};

	$self->{CDC_headers} = my $headers = delete $args->{headers} || {};
	$headers->{Accept} ||= 'application/json';

	my $username = delete $args->{username} // '';
	my $password = delete $args->{password} // '';
	$headers->{Authorization} = 'Basic ' . encode_base64("$username:$password", '')
		if length $username && length $password;

	$self;
}

#-------------
=section Accessors

=method name
=cut

sub name() { $_[0]->{CDC_name} }

=method couch
Returns the active M<Couch::DB> object.
=cut

sub couch() { $_[0]->{CDC_couch} }

=method server
Returns the URL of the server which is addressed by this client.

Which type of object is used, depends on the event framework.  For instance
a M<Mojo::URL> when using M<Couch::DB::Mojo>.
=cut

sub server() { $_[0]->{CDC_server} }

=method userAgent
Returns the user-agent object which connects to the servers.

Which type of object is used, depends on the event framework. for instance
a M<Mojo::UserAgent> when using M<Couch::DB::Mojo>.
=cut

sub userAgent() { $_[0]->{CDC_ua} }

=method headers
Returns a HASH with the default set of headers to be used when contacting
this client.
=cut

sub headers($) { $_[0]->{CDC_headers} }

#-------------
=section Server information

=method serverInfo %options
[CouchDB API 1.2.1 "GET /"]
Query details about the server this client is connected to.
Returns a M<Couch::DB::Result> object.

=option  delay BOOLEAN
=default delay C<false>
Create a delayed Result.

=option  cached 'YES'|'NEVER'|'RETRY'|'PING'
=default cached 'YES'
Reuse the results of the previous ping to the server?  This old request
might have resulted in a connection error, so the cached data may continue
to show an error while the problem has disappeared.  With C<RETRY>, the
cached data will be used when the previous ping was successful.  When C<PING>,
then the call will be made, but the old successfully retreived information will
not be lost.
=cut

sub __serverInfoValues
{	my $result = shift;
	my %values = %{$result->doc->data};

	# 3.3.3 does not contain the vendor/version, as the example in the spec says
	# Probably a mistake.
	$values{version} = $result->couch->toPerl(version => version => $values{version});
	\%values;
}

sub serverInfo(%)
{	my ($self, %args) = @_;
	my $cached = delete $args{cached} || 'YES';
	$cached =~ m!^(?:YES|NEVER|RETRY|PING)$!
		or panic "Unsupported cached parameter '$cached'.";

	if(my $result = $self->{CDC_info})
	{	return $self->{CDC_info}
			if $cached eq 'YES' || ($cached eq 'RETRY' && $result->isReady);
	}

	my $result = $self->couch->call(GET => '/',
		client    => $self,          # explicitly run only on this client
		delay     => delete $args{delay},
		to_values => \&__serverInfoValues,
	);

	if($cached ne 'PING')
	{	$self->{CDC_info} = $result;
		delete $self->{CDC_version};
	}

	$result;
}

=method version
Returns the version of the server software, as version object.
=cut

sub version()
{	my $self   = shift;
	return $self->{CDC_version} if exists $self->{CDC_version};

	my $result = $self->serverInfo(cached => 'YES');
	$result->isReady or return undef;

	my $version = $result->values->{version}
		or error __x"Server info field does not contain the server version.";

	$self->{CDC_version} = $version;
}

=method activeTasks %options
[CouchDB API 1.2.2 "GET /_active_tasks"]
Query details about the (maintenance) tasks which are currently running in the
connected server.  Returns a M<Couch::DB::Result> object.
=cut

sub __activeTasksValues
{	my $result = shift;
	my $tasks = $result->doc->data;
	my $couch = $result->couch;

	my @tasks;
	foreach my $task (@$tasks)
	{	my %task = %$task;
		$task{started_on} = $couch->toPerl(epoch => $task{type} => $task{started_on});
		$task{updated_on} = $couch->toPerl(epoch => $task{type} => $task{updated_on});
		push @tasks, \%task;
	}

	\@tasks;
}

sub activeTasks(%)
{	my ($self, %args) = @_;

	$self->couch->call(GET => '/_active_tasks',
		client    => $self,          # explicitly run only on this client
		delay     => delete $args{delay},
		to_values => \&__activeTasksValues,
	);
}

=method databaseKeys %options
[CouchDB API 1.2.3 "GET /_all_dbs"]
Returns the selected database names as present on the connected CouchDB
instance.

As %options, you can specify a key filter: specify a subset of keys to be
returned.  These options are C<descending> (boolean)
C<startkey>, C<endkey>, C<limit>, and C<skip>.
=cut

sub true { 1 }
sub enc_json { $_[0] }

sub __db_keyfilter($)
{	my $args = @_;

	+{
		descending => delete $args{descending} ? true : undef,
		startkey   => enc_json(delete $args{startkey} || delete $args{start_key}),
		endkey     => enc_json(delete $args{endkey}   || delete $args{end_key}),
		limit      => delete $args{limit},
		skip       => delete $args{skip}     || undef,
	 };
}

sub databaseKeys(%)
{	my ($self, %args) = @_;

	$self->couch->call(GET => '/_all_dbs',
		client    => $self,          # explicitly run only on this client
		delay     => delete $args{delay},
		query     => (__db_keyfilter \%args),
	);
}


=method databaseInfo %options
[CouchDB API 1.2.4 "GET /_dbs_info"] and
[CouchDB API 1.2.5 "POST /_dbs_info"]
Returns detailed information about the selected database keys,
on the connected CouchDB instance.  Both the GET and POST
alternatives produce the same structures.

When you provide a C<keys> option, then those database details are
collected.  Otherwise, you can use the filter options described by
M<databaseKeys()>.

=option  keys ARRAY
=default keys C<undef>
When you provide explicit database keys, then only those are displayed.
The amount is limited by the C<max_db_number_for_dbs_info_req> configuration
parameter, which defaults to 100.
=cut

sub databaseInfo(%)
{	my ($self, %args) = @_;

	my ($method, $query, $body, $intro) = $args{keys}
	  ?	(POST => undef,  +{ keys => delete $args{keys} }, '2.2')
	  :	(GET  => (__db_keyfilter \%args), undef, '3.2');

	$self->couch->call($method => '/_dbs_info',
		introduced => $intro,
		client     => $self,          # explicitly run only on this client
		delay      => delete $args{delay},
		query      => $query,
		body       => $body,
	);
}

#-------------
=section Other
=cut

1;
