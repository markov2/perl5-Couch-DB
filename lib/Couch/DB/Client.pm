#oodist: *** DO NOT USE THIS VERSION FOR PRODUCTION ***
#oodist: This file contains OODoc-style documentation which will get stripped
#oodist: during its release in the distribution.  You can use this file for
#oodist: testing, however the code of this development version may be broken!

package Couch::DB::Client;

use warnings;
use strict;

use Couch::DB::Util   qw/flat/;
use Couch::DB::Result ();

use Log::Report     'couch-db';

use Scalar::Util    qw/weaken blessed/;
use List::Util      qw/first/;
use MIME::Base64    qw/encode_base64/;
use Storable        qw/dclone/;
use URI::Escape     qw/uri_escape/;

my $seqnr = 0;

#--------------------
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
node may be part of a cluster, which can be managed via Couch::DB::Cluster

=chapter METHODS

=section Constructors

=c_method new %options

Create the client. Whether it works will show when the first call is made.
You could try M<serverStatus()> on application startup.

=requires couch  Couch::DB-object

=requires server URL-object
Pass a URL-object which fits the framework your choose.

=requires user_agent UserAgent-Object
Pass a UserAgent-object which fits the framework your choose.

=option  name STRING
=default name C<server>
A good symbolic name for the client may make it more readible.
Defaults to the location of the server.

=option  username STRING
=default username undef
When you specify a P<username>/P<password> here, then C<Basic>
authentication will be used.  Otherwise, call C<login()> to
use Cookies.

=option  auth     'BASIC'|'COOKIE'
=default auth     'BASIC'

=option  password STRING
=default password undef

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
	$self->{CDC_seqnr}  = ++$seqnr;

	$self->{CDC_couch}  = delete $args->{couch} or panic "Requires 'couch'";
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

#--------------------
=section Accessors

=method name
=cut

sub name() { $_[0]->{CDC_name} }

=method couch
Returns the active Couch::DB object.
=cut

sub couch() { $_[0]->{CDC_couch} }

=method server
Returns the URL of the server which is addressed by this client.

Which type of object is used, depends on the event framework.  For instance
a Mojo::URL when using Couch::DB::Mojolicious.
=cut

sub server() { $_[0]->{CDC_server} }

=method userAgent
Returns the user-agent object which connects to the servers.

Which type of object is used, depends on the event framework. for instance
a Mojo::UserAgent when using Couch::DB::Mojolicious.
=cut

sub userAgent() { $_[0]->{CDC_ua} }

=method headers
Returns a HASH with the default set of headers to be used when contacting
this client.
=cut

sub headers($) { $_[0]->{CDC_hdrs} }

=method seqnr
Returns the (process space) unique sequence number for this client.  This may
make tracing errors easier.
=cut

sub seqnr() { $_[0]->{CDC_seqnr} }

#--------------------
=section Session

=method login %options
  [CouchDB API "POST /_session", UNTESTED]

Get a Cookie: Cookie authentication.

B<TODO>: implement refreshing of the session.

=requires username STRING
=requires password STRING

=option   next URL
=default  next undef
When the login was successful, the UserAgent will get redirected to
the indicated location.

=error Unsupport authorization '$how'
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

	my $send = $self->{CDC_login}  	 	=     # keep for cookie refresh (uninplemented)
+{name => $username, password => $password };

	$self->couch->call(POST => '/_session',
		send      => $send,
		query     => { next => delete $args{next} },
		$self->couch->_resultsConfig(\%args, on_final  => sub {
			$self->{CDC_roles} = $_[0]->isReady ? $_[0]->values->{roles} : undef;
		}),
	);
}

=method session %options
  [CouchDB API "GET /_session", UNTESTED]

Returns information about the current session, like information about the
user who is logged-in.  Part of the reply is the "userCtx" (user context)
which displays the roles of this user, and its name.

=option  basic BOOLEAN
=default basic undef
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
		$couch->_resultsConfig(\%args, on_final => sub {
			$self->{CDC_roles} = $_[0]->isReady ? $_[0]->values->{userCtx}{roles} : undef; $_[0];
		}),
	);
}

=method logout %options
  [CouchDB API "DELETE /_session", UNTESTED]

Terminate the session.
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

#--------------------
=section Server information

B<All CouchDB API calls> documented below, support C<%options> like C<_delay>
and C<on_error>.  See L<Couch::DB/Using the CouchDB API>.

These are only for web-interfaces:

  [CouchDB API "GET /favicon.ico", UNSUPPORTED]
  [CouchDB API "GET /_utils", UNSUPPORTED]
  [CouchDB API "GET /_utils/", UNSUPPORTED]

=method serverInfo %options
  [CouchDB API "GET /"]

Query details about the server this client is connected to.
Returns a Couch::DB::Result object.

=option  cached 'YES'|'NEVER'|'RETRY'|'PING'
=default cached 'YES'
Reuse the results of the previous ping to the server?  This old request
might have resulted in a connection error, so the cached data may continue
to show an error while the problem has disappeared.  With C<RETRY>, the
cached data will be used when the previous ping was successful.  When C<PING>,
then the call will be made, but the old successfully retreived information will
not be lost.
=cut

sub __serverInfoValues($$)
{	my ($self, $result, $data) = @_;
	my $values = { %$data };

	# 3.3.3 does not contain the vendor/version, as the example in the spec says
	# Probably a mistake.
	$result->couch->toPerl($values, version => qw/version/);
	$values;
}

sub serverInfo(%)
{	my ($self, %args) = @_;
	$self->_clientIsMe(\%args);

	my $cached = delete $args{cached} || 'YES';
	$cached =~ m!^(?:YES|NEVER|RETRY|PING)$! or panic "Unsupported cached parameter '$cached'.";

	if(my $result = $self->{CDC_info})
	{	return $self->{CDC_info}
			if $cached eq 'YES' || ($cached eq 'RETRY' && $result->isReady);
	}

	my $result = $self->couch->call(GET => '/',
		$self->couch->_resultsConfig(\%args,
			on_values => sub { $self->__serverInfoValues(@_) }
		),
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

=error Server info field does not contain the server version.
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
connected server.  Returns a Couch::DB::Result object which support rows.
=cut

sub __simpleArrayRow($$%)
{	my ($self, $result, $index, %args) = @_;
	my $answer = $result->answer->[$index] or return ();

	  (	answer => $answer,
		values => $result->values->[$index],
	  );
}

sub __activeTasksValues($$)
{	my ($self, $result, $tasks) = @_;
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
		$self->couch->_resultsConfig(\%args,
			on_values => sub { $self->__activeTasksValues(@_) },
			on_row    => sub { $self->__simpleArrayRow(@_) },
		),
	);
}

=method databaseNames [ \%search, %options ]
  [CouchDB API "GET /_all_dbs"]

Returns the selected database names as present on the connected CouchDB
instance.

You can specify a name (=key) filter: specify a subset of names to be
returned in the %search.
=cut

sub __dbNamesFilter($)
{	my ($self, $search) = @_;

	my $query = defined $search ? +{ %$search } : return {};
	$self->couch
		->toQuery($query, bool => qw/descending/)
		->toQuery($query, json => qw/endkey end_key startkey start_key/);
	$query;
}

sub databaseNames(;$%)
{	my ($self, $search, %args) = @_;
	$self->_clientIsMe(\%args);

	$self->couch->call(GET => '/_all_dbs',
		query => $self->__dbNamesFilter($search),
		$self->couch->_resultsConfig(\%args,
			on_row => sub { $self->__simpleArrayRow(@_) },
		),
	);
}

=method databaseInfo [\%search, %options]
  [CouchDB API "GET /_dbs_info", since 3.2]
  [CouchDB API "POST /_dbs_info", since 2.2]

Returns detailed information about the selected database keys, on the
connected CouchDB instance.  Both the GET and POST alternatives produce
the same structures.

When both C<keys> and C<search> are missing, then all databases are
reported.

=option  names \@names
=default names undef
When you provide explicit database keys, then only those are displayed.
The number of keys is limited by the C<max_db_number_for_dbs_info_req>
configuration parameter, which defaults to 100.

=cut

sub databaseInfo(;$%)
{	my ($self, $search, %args) = @_;
	$self->_clientIsMe(\%args);
	my $names  = delete $args{names};

	my ($method, $query, $send, $intro) = $names
	?	(POST => undef,  +{ keys => $names }, '2.2.0')
	:	(GET  => $self->_dbNamesFilter($search), undef, '3.2.0');

	$self->couch->call($method => '/_dbs_info',
		introduced => $intro,
		query      => $query,
		send       => $send,
		$self->couch->_resultsConfig(\%args,
			on_row => sub { $self->__simpleArrayRow(@_) },
		),
	);
}

=method dbUpdates \%feed, %options
  [CouchDB API "GET /_db_updates", since 1.4, UNTESTED]

Get a feed of database changes, mainly for debugging purposes.  It supports
rows.
=cut

sub __dbUpRow($$%)
{	my ($self, $result, $index, %args) = @_;
	my $answer = $result->answer->{results}[$index] or return ();
	(	answer => $answer,
		values => $result->values->{results}[$index],
	);
}

sub dbUpdates($%)
{	my ($self, $feed, %args) = @_;
	$self->_clientIsMe(\%args);

	my $query  = +{ %$feed };

	$self->couch->call(GET => '/_db_updates',
		introduced => '1.4.0',
		query      => $query,
		$self->couch->_resultsConfig(\%args,
			on_row => sub { $self->__dbUpRow(@_) },
		),
	);
}

=method clusterNodes %options
  [CouchDB API "GET /_membership", since 2.0, UNTESTED]

List all known nodes, and those currently used for the cluster.
=cut

sub __clusterNodeValues($$)
{	my ($self, $result, $data) = @_;
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

	$self->couch->call(GET => '/_membership',
		introduced => '2.0.0',
		$self->couch->_resultsConfig(\%args,
			on_values => sub { $self->__clusterNodeValues(@_) }
		),
	);
}

=method replicate \%rules, %options
  [CouchDB API "POST /_replicate", UNTESTED]

Configure replication: configure and stop.

All %options are posted as parameters.
=cut

sub __replicateValues($$)
{	my ($self, $result, $raw) = @_;
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

sub replicate($%)
{	my ($self, $rules, %args) = @_;
	$self->_clientIsMe(\%args);

	my $couch  = $self->couch;
	$couch->toJSON($rules, bool => qw/cancel continuous create_target winning_revs_only/);

	#TODO: warn for upcoming changes in source and target: absolute URLs required

	$couch->call(POST => '/_replicate',
		send   => $rules,
		$couch->_resultsConfig(\%args,
			on_values => sub { $self->__replicateValues(@_) }
		),
	);
}

=method replicationJobs %options
  [CouchDB API "GET /_scheduler/jobs", UNTESTED]

Returns information about current replication jobs (which preform tasks), on
this CouchDB server instance.  The results are ordered by replication ID.

Supports pagination.
=cut

sub __replJobsRow($$%)
{	my ($self, $result, $index, %args) = @_;
	my $answer = $result->answer->{jobs}[$index] or return ();

	  (	answer => $answer,
		values => $result->values->{jobs}[$index],
	  );
}

sub __replJobsValues($$)
{	my ($self, $result, $raw) = @_;
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

	$self->couch->call(GET => '/_scheduler/jobs',
		$self->couch->_resultsPaging(\%args,
			on_values => sub { $self->__replJobsValues(@_) },
			on_row    => sub { $self->__replJobsRow(@_) },
		),
	);
}

=method replicationDocs %options
  [CouchDB API "GET /_scheduler/docs", UNTESTED]
  [CouchDB API "GET /_scheduler/docs/{replicator_db}", UNTESTED]

Retrieve information about replication documents.
Supports pagination.

=option  dbname NAME
=default dbname C<_replicator>
Pass a P<dbname> for the database which contains the replication information.
=cut

sub __replDocRow($$%)
{	my ($self, $result, $index, %args) = @_;
	my $answer = $result->answer->{jobs}[$index] or return ();

	  (	answer => $answer,
		values => $result->values->{jobs}[$index],
	  );
}

sub __replDocValues($$)
{	my ($self, $result, $raw) = @_;
	my $v = +{ %$raw }; # $raw->{info} needs no conversions

	$result->couch
		->toPerl($v, isotime => qw/start_time last_updated/)
		->toPerl($v, abs_url => qw/target source/)
		->toPerl($v, node    => qw/node/);
	$v;
}

sub __replDocsValues($$)
{	my ($self, $result, $raw) = @_;
	my $couch   = $result->couch;
	my $values  = dclone $raw;
	$values->{docs} = [ map $self->__replDocValues($result, $_), @{$values->{docs} || []} ];
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

	$self->couch->call(GET => $path,
		$self->couch->_resultsPaging(\%args,
			on_values => sub { $self->__replDocsValues(@_) },
			on_row    => sub { $self->__replDocRow(@_) },
		),
	);
}

=method replicationDoc $doc|$docid, %options
  [CouchDB API "GET /_scheduler/docs/{replicator_db}/{docid}", UNTESTED]

Retrieve information about a particular replication document.

=option  dbname $name
=default dbname C<_replicator>
Pass a P<dbname> for the database which contains the replication information.
=cut

#XXX the output differs from replicationDoc, so different method

sub __replOneDocValues($$)
{	my ($self, $result, $raw) = @_;
	$self->__replDocValues($result, $raw);
}

sub replicationDoc($%)
{	my ($self, $doc, %args) = @_;
	$self->_clientIsMe(\%args);

	my $dbname = delete $args{dbname} || '_replicator';
	my $docid  = blessed $doc ? $doc->id : $doc;

	my $path = '/_scheduler/docs/' . uri_escape($dbname) . '/' . $docid;

	$self->couch->call(GET => $path,
		$self->couch->_resultsConfig(\%args,
			on_values => sub { $self->__replOneDocValues(@_) },
		),
	);
}

=method nodeName $name, %options
  [CouchDB API "GET /_node/{node-name}", UNTESTED]

The only useful application is with the abstract name C<_local>, which will
return you the name of the node represented by the CouchDB instance.
=cut

sub __nodeNameValues($)
{	my ($self, $result, $raw) = @_;
	my $values = dclone $raw;
	$result->couch->toPerl($values, node => qw/name/);
	$values;
}

sub nodeName($%)
{	my ($self, $name, %args) = @_;
	$self->_clientIsMe(\%args);

	$self->couch->call(GET => "/_node/$name",
		$self->couch->_resultsConfig(\%args,
			on_values => sub { $self->__nodeNameValues(@_) }
		),
	);
}

=method node
Returns the C<Couch::DB::Node> which is run by the connected CouchDB instance.
This fact is cached.

=error did not get a node name for _local.
=cut

sub node()
{	my $self = shift;
	return $self->{CDC_node} if defined $self->{CDC_node};

	my $result = $self->nodeName('_local', client => $self);
	$result->isReady or return undef;   # (temporary?) failure

	my $name   = $result->value('name')
		or error __x"did not get a node name for _local.";

	$self->{CDC_node} = $self->couch->node($name);
}

=method serverStatus %options
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
	$result && $result->answer->{status} eq 'ok';
}

1;
