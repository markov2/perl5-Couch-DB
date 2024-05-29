# SPDX-FileCopyrightText: 2024 Mark Overmeer <mark@open-console.eu>
# SPDX-License-Identifier: EUPL-1.2-or-later

package Couch::DB::Client;

use Couch::DB::Util   qw(flat);
use Couch::DB::Result ();

use Log::Report 'couch-db';

use Scalar::Util    qw(weaken blessed);
use List::Util      qw(first);
use MIME::Base64    qw(encode_base64);
use Storable        qw(dclone);
use URI::Escape     qw(uri_escape);

=chapter NAME

Couch::DB::Client - connect to a CouchDB node

=chapter SYNOPSIS

  my $client = Couch::DB::Client->new(couch => $couchdb, ...);
  $couch->addClient($client);

  my $client = $couch->createClient(...);  # same in one step

  # Even simpler
  my $couch  = Couch::DB::Mojolicious->new(server => ...);
  my $client = $couchdb->client('local');   # default client

=chapter DESCRIPTION

Connect to a CouchDB-server which runs a CouchDB-node to host databases.  That
node may be part of a cluster, which can be managed via M<Couch::DB::Cluster>

=chapter METHODS

=section Constructors

=c_method new %options

Create the client. Whether it works will show when the first call is made.
You could try M<serverStatus()> on application startup.

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
When you specify a C<username>/C<password> here, then C<Basic>
authentication will be used.  Otherwise, call C<login()> to
use Cookies.

=option  auth     'BASIC'|'COOKIE'
=default auth     'BASIC'

=option  password STRING
=default password C<undef>

=option  headers HASH
=default headers <a few>
Some headers are set by default, for instance the 'Accept' header.
You can overrule them.  The defaults may change.

With this option you can also provide proxy authentication headers, of the form
C<X-Auth-CouchDB-*>.
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

	$self->{CDC_hdrs}   = my $headers = delete $args->{headers} || {};

	my $username        = delete $args->{username} // '';
	$self->login(
		auth     => delete $args->{auth} || 'BASIC',
		username => $username,
		password => delete $args->{password},
	) if length $username;

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

sub headers($) { $_[0]->{CDC_hdrs} }

#-------------
=section Session

=method login %options
[CouchDB API "POST /_session", UNTESTED]
Get a Cookie: Cookie authentication.

B<TODO>: implement refreshing of the session.

=requires username STRING
=requires password STRING

=option   next URL
=default  next C<undef>
When the login was successful, the UserAgent will get redirected to
the indicated location.
=cut

sub _clientIsMe($)   # check no client parameter is used
{	my ($self, $args) = @_;
	defined $args->{client} and panic "No parameter 'client' allowed.";
	$args->{clients} && @{delete $args->{clients}} and panic "No parameter 'clients' allowed.";
	$args->{client} = $self;
}

sub login(%)
{	my ($self, %args) = @_;
	$self->_clientIsMe(\%args);

	my $auth     = delete $args{auth} || 'BASIC';
	my $username = delete $args{username} or panic "Requires username";
	my $password = delete $args{password} or panic "Requires password";

	if($auth eq 'BASIC')
	{	$self->headers->{Authorization} = 'Basic ' . encode_base64("$username:$password", '');
		return $self;  #XXX must return Result object
	}

	$auth eq 'COOKIE'
		or error __x"Unsupport authorization '{how}'", how => $auth;

	my $send = $self->{CDC_login} =     # keep for cookie refresh (uninplemented)
	 	+{ name => $username, password => $password };

	$self->couch->call(POST => '/_session',
		send      => $send,
		query     => { next => delete $args{next} },
		$self->couch->_resultsConfig(\%args,
			on_final  => sub { $self->{CDC_roles} = $_[0]->isReady ? $_[0]->values->{roles} : undef },
		),
	);
}

=method session %options
[CouchDB API "GET /_session", UNTESTED]
Returns information about the current session, like information about the
user who is logged-in.  Part of the reply is the "userCtx" (user context)
which displays the roles of this user, and its name.

=option  basic BOOLEAN
=default basic C<undef>

=cut

sub session(%)
{	my ($self, %args) = @_;
	$self->_clientIsMe(\%args);
	my $couch = $self->couch;

	my %query;
	$query{basic} = delete $args{basic} if exists $args{basic};
	$couch->toQuery(\%query, bool => qw/basic/);

	$couch->call(GET => '/_session',
		query     => \%query,
		$couch->_resultsConfig(\%args,
			on_final  => sub { $self->{CDC_roles} = $_[0]->isReady ? $_[0]->values->{userCtx}{roles} : undef },
		),
	);
}

=method logout %options
[CouchDB API "DELETE /_session", UNTESTED]
=cut

sub logout(%)
{	my ($self, %args) = @_;
	$self->_clientIsMe(\%args);

	$self->couch->call(DELETE => '/_session',
		$self->couch->_resultsConfig(\%args),
	);
}

=method roles
[UNTESTED]
Returns a LIST of all roles this client can perform.
=cut

sub roles()
{	my $self = shift;
	$self->{CDC_roles} or $self->session(basic => 1);  # produced as side-effect
	@{$self->{CDC_roles} || []};
}

=method hasRole $role
[UNTESTED]
Return 'true' if (this user logged-in to the server with) this client can perform
a certain role.

B<It is often> preferred to try a certain action, and then check whether it
results in a permission error.
=cut

sub hasRole($) { first { $_[1] eq $_ } $_[0]->roles }

#-------------
=section Server information

B<All CouchDB API calls> documented below, support %options like C<_delay>
and C<on_error>.  See L<Couch::DB/Using the CouchDB API>.

# These are only for web-interfaces
# [CouchDB API "GET /favicon.ico", UNSUPPORTED]
# [CouchDB API "GET /_utils", UNSUPPORTED]
# [CouchDB API "GET /_utils/", UNSUPPORTED]

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
	$self->_clientIsMe(\%args);

	my $cached = delete $args{cached} || 'YES';
	$cached =~ m!^(?:YES|NEVER|RETRY|PING)$!
		or panic "Unsupported cached parameter '$cached'.";

	if(my $result = $self->{CDC_info})
	{	return $self->{CDC_info}
			if $cached eq 'YES' || ($cached eq 'RETRY' && $result->isReady);
	}

	my $result = $self->couch->call(GET => '/',
		$self->couch->_resultsConfig(\%args),
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
	$self->_clientIsMe(\%args);

	$self->couch->call(GET => '/_active_tasks',
		$self->couch->_resultsConfig(\%args),
		to_values => \&__activeTasksValues,
	);
}

=method databaseNames %options
[CouchDB API "GET /_all_dbs"]
Returns the selected database names as present on the connected CouchDB
instance.

As %options, you can specify a key filter: specify a subset of keys to be
returned.  These options are C<descending> (boolean)
C<startkey>, C<endkey>, C<limit>, and C<skip>.
=cut

sub _db_keyfilter($)
{	my ($self, $args) = @_;

	my $filter = +{
		descending => delete $args->{descending},
		startkey   => delete $args->{startkey} || delete $args->{start_key},
		endkey     => delete $args->{endkey}   || delete $args->{end_key},
		limit      => delete $args->{limit},
		skip       => delete $args->{skip},
	};
	$self->couch->toJSON($filter, bool => qw/descending/);

	$filter;
}

#XXX it is unclear why the database names are referred to as "keys".
sub databaseNames(%)
{	my ($self, %args) = @_;
	$self->_clientIsMe(\%args);

	$self->couch->call(GET => '/_all_dbs',
		query => $self->_db_keyfilter(\%args),
		$self->couch->_resultsConfig(\%args),
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
	$self->_clientIsMe(\%args);

	my ($method, $query, $body, $intro) = $args{keys}
	  ?	(POST => undef,  +{ keys => delete $args{keys} }, '2.2.0')
	  :	(GET  => $self->_db_keyfilter(\%args), undef, '3.2.0');

	$self->couch->call($method => '/_dbs_info',
		introduced => $intro,
		query      => $query,
		send       => $body,
		$self->couch->_resultsConfig(\%args),
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
	$self->_clientIsMe(\%args);

	my %config = $self->couch->_resultsConfig(\%args);
	my $query  = \%args;

	$self->couch->call(GET => '/_db_updates',
		introduced => '1.4.0',
		query      => $query,
		%config,
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
	$self->_clientIsMe(\%args);

	my %config = $self->couch->_resultsConfig(\%args);
	my $send   = \%args;

	$self->couch->call(GET => '/_membership',
		introduced => '2.0.0',
		send       => $send,
		to_values  => \&__clusterNodeValues,
		%config,
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
	$self->_clientIsMe(\%args);

	my $couch  = $self->couch;
	my %config = $couch->_resultsConfig(\%args),

	my $send   = \%args;
	$couch->toJSON($send, bool => qw/cancel continuous create_target/);

    #TODO: warn for upcoming changes in source and target: absolute URLs required

	$couch->call(POST => '/_replicate',
		send       => $send,
		to_values  => \&__replicateValues,
		%config,
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
	$self->_clientIsMe(\%args);

	my %query = (
		limit => delete $args{limit},
		skip  => delete $args{skip},
	);

	$self->couch->call(GET => '/_scheduler/jobs',
		query      => \%query,
		to_values  => \&__replJobsValues,
		$self->couch->_resultsConfig(\%args),
	);
}

=method replicationDocs %options
[CouchDB API "GET /_scheduler/docs", UNTESTED] and
[CouchDB API "GET /_scheduler/docs/{replicator_db}", UNTESTED].

=option  dbname NAME
=default dbname C<_replicator>
Pass a C<dbname> for the database which contains the replication information.
=cut

sub __replDocValues($$)
{	my ($result, $raw) = @_;
	my $v = +{ %$raw }; # $raw->{info} needs no conversions

	$result->couch
		->toPerl($v, isotime => qw/start_time last_updated/)
		->toPerl($v, abs_url => qw/target source/)
		->toPerl($v, node    => qw/node/);
	$v;
}

sub __replDocsValues($$)
{	my ($result, $raw) = @_;
	my $couch   = $result->couch;
	my $values  = dclone $raw;
	$values->{docs} = [ map __replDocValues($result, $_), @{$values->{docs} || []} ];
	$values;
}

sub replicationDocs(%)
{	my ($self, %args) = @_;
	$self->_clientIsMe(\%args);
	my $dbname = delete $args{dbname} || '_replicator';

	my $path = '/_scheduler/docs';
	if($dbname ne '_replicator')
	{	$path .= '/' . uri_escape($dbname);
	}

	my %query = (
		limit => delete $args{limit},
		skip  => delete $args{skip},
	);

	$self->couch->call(GET => $path,
		query      => \%query,
		to_values  => \&__replDocsValues,
		$self->couch->_resultsConfig(\%args),
	);
}

=method replicationDoc $doc|$docid, %options
[CouchDB API "GET /_scheduler/docs/{replicator_db}/{docid}", UNTESTED].

=option  dbname NAME
=default dbname C<_replicator>
Pass a C<dbname> for the database which contains the replication information.
=cut

#XXX the output differs from replicationDoc

sub replicationDoc($%)
{	my ($self, $doc, %args) = @_;
	$self->_clientIsMe(\%args);

	my $dbname = delete $args{dbname} || '_replicator';
	my $docid  = blessed $doc ? $doc->id : $doc;

	my $path = '/_scheduler/docs/' . uri_escape($dbname) . '/' . $docid;
	my %query = (
		limit => delete $args{limit},
		skip  => delete $args{skip},
	);

	$self->couch->call(GET => $path,
		query      => \%query,
		to_values  => \&__replDocValues,
		$self->couch->_resultsConfig(\%args),
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
	$self->_clientIsMe(\%args);

	$self->couch->call(GET => "/_node/$name",
		to_values  => \&__nodeNameValues,
		$self->couch->_resultsConfig(\%args),
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

=method serverStatus
[CouchDB API "GET /_up", since 2.0, UNTESTED]
Probably you want to use M<serverIsUp()>, because this reply contains little
information.
=cut

sub serverStatus(%)
{	my ($self, %args) = @_;
	$self->_clientIsMe(\%args);

	$self->couch->call(GET => '/_up',
		introduced => '2.0.0',
		$self->couch->_resultsConfig(\%args),
	);
}

=method serverIsUp
[UNTESTED]
Returns a true value when the server status is "ok".
=cut

sub serverIsUp()
{	my $self = shift;
	my $result = $self->serverStatus;
	$result && $result->values->{status} eq 'ok';
}

#-------------
=section Other
=cut

1;
