# SPDX-FileCopyrightText: 2024 Mark Overmeer <mark@open-console.eu>
# SPDX-License-Identifier: EUPL-1.2-or-later

package Couch::DB::Mojo;
use parent 'Couch::DB';
use feature 'state';

use Log::Report 'couch-db';
use Couch::DB::Util qw(flat);

use Scalar::Util     qw(blessed);
use Mojo::URL        ();
use Mojo::UserAgent  ();
use HTTP::Status     qw(HTTP_OK);

=chapter NAME

Couch::DB::Mojo - CouchDB backend for Mojolicious

=chapter SYNOPSIS

   use Couch::DB::Mojo ();
   my $couch = Couch::DB::Mojo->new;

   # From here on: see the Couch::DB base class
   my $db    = $couch->db('my-db');

=chapter DESCRIPTION

This is the M<Couch::DB> implementation based on the M<Mojolicious> (=Mojo) event
framework.  It uses many Mojo specific modules, like M<Mojo::URL> and M<Mojo::UserAgent>.

=chapter METHODS

=section Constructors

=c_method new %options
=cut

sub init($)
{	my ($self, $args) = @_;

	$args->{to_perl} =
	 +{	uri => sub { Mojo::URL->new($_[2]) },
	  };

	$self->SUPER::init($args);
}

#-------------
=section Accessors

=cut

#-------------
=section Server connections

The C<server> is a M<Mojo::URL>, or will be transformed into one.
The C<user_agent> is a M<Mojo::UserAgent>.
=cut

sub createClient(%)
{	my ($self, %args) = @_;
	$args{couch} = $self;

	my $server = $args{server} || panic "Requires 'server'";
	$args{server} = Mojo::URL->new("$server")
		unless blessed $server && $server->isa('Mojo::URL');

	my $ua = $args{user_agent} ||= state $ua_shared = Mojo::UserAgent->new;
	blessed $ua && $ua->isa('Mojo::UserAgent') or panic "Illegal user_agent";

	$self->SUPER::createClient(%args);
}

=method call $method, $path, %options

=option  delay BOOLEAN
=default delay C<false>
See M<Couch::DB::Result> chapter DETAILS about delayed requests.

=option  query HASH
=default query C<+{ }>
Query parameters for the request.

=option  data  HASH
=default data  C<+{ }>

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

sub call($$%)
{	my ($self, $method, $path, %args) = @_;

	my @clients = flat delete $args{client}, delete $args{clients};
	@clients or @clients = $self->clients;

	my $query   = delete $args{query}  || {};
	my $delay   = delete $args{delay}  || 0;

	my $result  = Couch::DB::Result->new(
		couch     => $self,
		to_values => delete $args{to_values},
	);

  CLIENT:
	foreach my $client (@clients)
	{	my $url = $client->server->clone->path($path);
		$url->query($query) if keys %$query;

		my $ua  = $client->userAgent;

		# $tx is a Mojo::Transaction::HTTP
my $body;
		my @body = defined $body ? (json => $body) : ();
		my $tx   = $ua->build_tx($method => $url, $client->headers, @body);

		my $plan = $ua->start_p($tx)->then(sub ($) {
			my $tx = shift;
			my $response = $tx->res;
			$result->setFinalResult({
				client   => $client,
				request  => $tx->req,
				response => $response,
				code     => $response->code,
			});
		});

		if($delay)
		{	$result->setResultDelay({ client => $client });
		}
		else
		{	$plan->wait;
			$result->code == HTTP_OK or next CLIENT;
		}

		return $result;
	}

	# The error from the last try will remain.
	$result;
}

sub extractJSON($)
{	my ($self, $response) = @_;
	$response->json;
}

#-------------
=section Other
=cut

1;
