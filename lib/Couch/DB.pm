# SPDX-FileCopyrightText: 2024 Mark Overmeer <mark@open-console.eu>
# SPDX-License-Identifier: EUPL-1.2-or-later

package Couch::DB;
use version;

use Log::Report 'couch-db';

use Couch::DB::Util   qw(flat);
use Couch::DB::Client ();

use Scalar::Util      qw(blessed);
use List::Util        qw(first);
use DateTime          ();
use URI               ();

use constant
{	DEFAULT_SERVER => 'http://127.0.0.1:5984',
};

my %default_converters =   # sub ($couch, $name, $datum) returns value/object
(	version   => sub { version->parse($_[2]) },
	epoch     => sub { DateTime->from_epoch(epoch => $_[2]) },
	uri       => sub { URI->new($_[2]) },
);

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
A table with converter name and CODE, to override/add the default JSON->PERL
object conversions for M<value()>.
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

	my $converters   = delete $args->{to_perl} || {};
	$self->{CD_conv} = +{ %default_converters, %$converters };

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
Returns the client with the specific $name (which defaults to the server url).
=cut

sub client($)
{	my ($self, $name) = @_;
	$name = "$name" if blessed $name;
	first { $_->name eq $name } $self->clients;   # never many: no HASH needed
}

#-------------
=section Database

=method createDatabase $name, %options
See M<Couch::DB::Database::create()>
=cut

sub createDatabase($%)
{	my ($self, $name, %args) = @_;
}

#-------------
=section Conversions

=method toPerl $type, $name, $datum
Convert a single value
=cut

sub toPerl($$)
{	my ($self, $type, $name, $datum) = @_;
	my $conv  = $_[0]->{CD_conv}{$type};
	defined $datum && defined $conv ? $conv->($self, $name, $datum) : undef;
}

#-------------
=section Other
=cut

1;
