# SPDX-FileCopyrightText: 2024 Mark Overmeer <mark@open-console.eu>
# SPDX-License-Identifier: EUPL-1.2-or-later

package Couch::DB::Client;

use Couch::DB::Util   qw(flat);
use Couch::DB::Result ();

use Log::Report 'couch-db';

use Scalar::Util    qw(weaken);
use MIME::Base64    qw(encode_base64);
use Storable        qw(dclone);

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
	$self->{CDC_uuids}  = [];

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

B<All CouchDB API calls> provide the C<delay> option, to create a result
object which will be run later.

Not supported from the CouchDB API:
=over 4
=item * C</favicon.ico>
=back

=method serverInfo %options
[CouchDB API "GET /"]
Query details about the server this client is connected to.
Returns a M<Couch::DB::Result> object.

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
{	my ($result, $data) = @_;
	my %values = %$data;

	# 3.3.3 does not contain the vendor/version, as the example in the spec says
	# Probably a mistake.
	$result->couch->toPerl(\%values, version => qw/version/);
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
[CouchDB API "GET /_active_tasks"]
Query details about the (maintenance) tasks which are currently running in the
connected server.  Returns a M<Couch::DB::Result> object.
=cut

sub __activeTasksValues($$)
{	my ($result, $tasks) = @_;
	my $couch = $result->couch;

	my @tasks;
	foreach my $task (@$tasks)
	{	my %task = %$task;
		$couch->toPerl(\%task, epoch => qw/started_on updated_on/);
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
[CouchDB API "GET /_all_dbs"]
Returns the selected database names as present on the connected CouchDB
instance.

As %options, you can specify a key filter: specify a subset of keys to be
returned.  These options are C<descending> (boolean)
C<startkey>, C<endkey>, C<limit>, and C<skip>.
=cut

sub _db_keyfilter($)
{	my ($self, $args) = @_;
	$self->couch->toJSON($args, bool => qw/descending/);

	my $filter = +{
		descending => delete $args->{descending},
		startkey   => delete $args->{startkey} || delete $args->{start_key},
		endkey     => delete $args->{endkey}   || delete $args->{end_key},
		limit      => delete $args->{limit},
		skip       => delete $args->{skip},
	};

	$filter;
}

sub databaseKeys(%)
{	my ($self, %args) = @_;

	$self->couch->call(GET => '/_all_dbs',
		client    => $self,          # explicitly run only on this client
		delay     => delete $args{delay},
		query     => $self->_db_keyfilter(\%args),
	);
}

=method databaseInfo %options
[CouchDB API "GET /_dbs_info", since 3.2] and
[CouchDB API "POST /_dbs_info", since 2.2]
Returns detailed information about the selected database keys,
on the connected CouchDB instance.  Both the GET and POST
alternatives produce the same structures.

When you provide a C<keys> option, then those database details are
collected.  Otherwise, you can use the filter options described by
M<databaseKeys()>.

=option  keys ARRAY
=default keys C<undef>
When you provide explicit database keys, then only those are displayed.
The number of keys is limited by the C<max_db_number_for_dbs_info_req>
configuration parameter, which defaults to 100.
=cut

sub databaseInfo(%)
{	my ($self, %args) = @_;

	my ($method, $query, $body, $intro) = $args{keys}
	  ?	(POST => undef,  +{ keys => delete $args{keys} }, '2.2')
	  :	(GET  => $self->_db_keyfilter(\%args), undef, '3.2');

	$self->couch->call($method => '/_dbs_info',
		introduced => $intro,
		client     => $self,          # explicitly run only on this client
		delay      => delete $args{delay},
		query      => $query,
		send       => $body,
	);
}

=method dbUpdates
[CouchDB API "GET /_db_updates", since 1.4, UNTESTED]
Get a feed of database changes, mainly for debugging purposes.

All %options are used as parameters: C<feed> (type),
C<timeout> (milliseconds!, default 60_000),
C<heartbeat> (milliseconds, default 60_000), C<since> (sequence ID).

=cut

sub dbUpdates(%)
{	my ($self, %args) = @_;

	my $delay = delete $args{delay};
	my %query = \%args;

	$self->couch->call(GET => '/_db_updates',
		introduced => '1.4',
		client     => $self,
		delay      => $delay,
		send       => \%args,
	);
}

=method clusterNodes %options
[CouchDB API "GET /_membership", since 2.0, UNTESTED]
List all known nodes, and those currently used for the cluster.
=cut

sub __clusterNodeValues($)
{	my ($result, $data) = @_;
	my $couch   = $result->couch;

	my %values  = %$data;
	foreach my $set (qw/all_nodes cluster_nodes/)
	{	my $v = $values{$set} or next;
		$values{$set} = [ $couch->listToPerl($set, node => $v) ];
	}

	\%values;
}

sub clusterNodes(%)
{	my ($self, %args) = @_;

	$self->couch->call(GET => '/_membership',
		introduced => '2.0',
		client     => $self,
		delay      => delete $args{delay},
		send       => \%args,
		to_values  => \&__clusterNodeValues,
	);
}

=method replicate %options
[CouchDB API "POST /_replicate", UNTESTED]
Configure replication: configure and stop.

All %options are posted as parameters.
=cut

sub __replicateValues($$)
{	my ($result, $raw) = @_;
	my $couch   = $result->couch;

	my $history = delete $raw->{history} or return $raw;
	my %values  = %$raw;
	my @history;

	foreach my $event (@$history)
	{	my %event = %$event;
		$couch->toPerl(\%event, mailtime => qw/start_time end_time/);
		push @history, \%event;
	}
	$values{history} = \@history;

	\%values;
}

sub replicate(%)
{	my ($self, %args) = @_;
	my $couch = $self->couch;

	my $delay = delete $args{delay};
	$couch->toJSON(\%args, bool => qw/cancel continuous create_target/);

    #TODO: warn for upcoming changes in source and target: absolute URLs required

	$couch->call(POST => '/_replicate',
		client     => $self,
		delay      => $delay,
		send       => \%args,
		to_values  => \&__replicateValues,
	);
}

=method replicationJobs %options
[CouchDB API "GET /_scheduler/jobs", UNTESTED]
Returns information about current replication jobs (which preform tasks), on
this CouchDB server instance.  The results are ordered by replication ID.

The %options can be C<limit> and C<skip>.
=cut

sub __replJobsValues($$)
{	my ($result, $raw) = @_;
	my $couch   = $result->couch;
	my $values  = dclone $raw;

	foreach my $job (@{$values->{jobs} || []})
	{
		$couch->toPerl($_, isotime => qw/timestamp/)
			foreach @{$job->{history} || []};

		$couch->toPerl($job, isotime => qw/start_time/)
		      ->toPerl($job, abs_url => qw/target source/)
		      ->toPerl($job, node    => qw/node/);
	}

	$values;
}

sub replicationJobs(%)
{	my ($self, %args) = @_;
	my %query = (
		limit => delete $args{limit},
		skip  => delete $args{skip},
	);

	$self->couch->call(GET => '/_scheduler/jobs',
		client     => $self,
		delay      => delete $args{delay},
		query      => \%query,
		to_values  => \&__replJobsValues,
	);
}

=method replicationDocs %options
[CouchDB API "GET /_scheduler/docs", UNTESTED] and
[CouchDB API "GET /_scheduler/docs/{replicator_db}", UNTESTED].

Pass a C<dbname> with %options to be specific about the database which
contains the replication information.
=cut

sub __replDocsValues($$)
{	my ($result, $raw) = @_;
	my $couch   = $result->couch;
	my $values  = dclone $raw;

	foreach my $doc (@{$values->{docs} || []})
	{	$couch->toPerl($doc, isotime => qw/start_time last_updated/)
		      ->toPerl($doc, abs_url => qw/target source/)
		      ->toPerl($doc, node    => qw/node/);
		# my $info = $doc->info;  # no conversions needed
	}

	$values;
}

sub replicationDocs($%)
{	my ($self, %args) = @_;

	my $path = '/_scheduler/docs';
	if(my $dbname = $args{dbname})
	{	# API-doc specifies the protocol twice, seemingly exactly the same in
		# docs 3.3.3 section 1.2.10.
		$path .= "/$dbname";    # '/' protection not needed
	}

	my %query = (
		limit => delete $args{limit},
		skip  => delete $args{skip},
	);

	$self->couch->call(GET => $path,
		client     => $self,
		delay      => delete $args{delay},
		query      => \%query,
		to_values  => \&__replDocsValues,
	);
}

=method nodeName $name, %options
[CouchDB API "GET /_node/{node-name}", UNTESTED]
The only useful application is with the abstract name C<_local>, which will
return you the name of the node represented by the CouchDB instance.
=cut

sub __nodeNameValues($)
{	my ($result, $raw) = @_;
	my $values = dclone $raw;
	$result->couch->toPerl($values, node => qw/name/);
	$values;
}

sub nodeName($%)
{	my ($self, $name, %args) = @_;
	my $path = "/_node/$name";

	$self->couch->call(GET => $path,
		client     => $self,
		delay      => delete $args{delay},
		to_values  => \&__nodeNameValues,
	);
}

=method node
Returns the C<Couch::DB::Node> which is run by the connected CouchDB instance.
This fact is cached.
=cut

sub node()
{	my $self = shift;
	return $self->{CDC_node} if defined $self->{CDC_node};

 	my $result = $self->nodeName('_local', client => $self);
	$result->isReady or return undef;   # (temporary?) failure

	my $name   = $result->value('name')
		or error __x"Did not get a node name for _local";

	$self->{CDC_node} = $self->couch->node($name);
}

=method adminInterface
[CouchDB API "GET /_utils", UNTESTED]
Returns the address of the admin interface.  This can be passed to a browser,
which will probably need to follow redirects and authenication procedures.
=cut

sub adminInterface()
{	my $self = shift;
	$self->server->path('/_utils');
}

=method serverStatus
[CouchDB API "GET /_up", since 2.0, UNTESTED]
Probably you want to use M<serverIsUp()>, because this reply contains little
information.
=cut

sub serverStatus(%)
{	my ($self, %args) = @_;

	$self->couch->call(GET => '/_up',
		introduced => '2.0',
		client     => $self,
		delay      => delete $args{delay},
	);
}

=method serverIsUp
[UNTESTED]
Returns a true value when the server status is "ok".
=cut

sub serverIsUp()
{	my $self = shift;
	my $result = $self->serverStatus;
	$result && $result->doc->data->{status} eq 'ok';
}

#-------------
=section Other
=cut

1;
