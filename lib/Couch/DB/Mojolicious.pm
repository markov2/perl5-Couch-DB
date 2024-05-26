# SPDX-FileCopyrightText: 2024 Mark Overmeer <mark@open-console.eu>
# SPDX-License-Identifier: EUPL-1.2-or-later

package Couch::DB::Mojolicious;
use parent 'Couch::DB';
use feature 'state';

use Log::Report 'couch-db';
use Couch::DB::Util qw(flat);

use Scalar::Util     qw(blessed);
use Mojo::URL        ();
use Mojo::UserAgent  ();
use Mojo::JSON       qw(decode_json);
use HTTP::Status     qw(HTTP_OK);

=chapter NAME

Couch::DB::Mojolicious - CouchDB backend for Mojolicious

=chapter SYNOPSIS

   use Couch::DB::Mojolicious ();
   my $couch = Couch::DB::Mojolicious->new;

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
	 +{	abs_uri => sub { Mojo::URL->new($_[2]) },
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

#method call

sub _callClient($$%)
{	my ($self, $result, $client, %args) = @_;

	my $method  = delete $args{method} or panic;
	my $delay   = delete $args{delay}  || 0;
	my $path    = delete $args{path};
	my $url     = $client->server->clone->path($path);

	$url->query(delete $args{query});

	my $ua  = $client->userAgent;
	my %headers = ( %{$client->headers}, %{delete $args{headers}} );
warn "HEADERS = ", join ';', %headers;

	my $send = delete $args{send};
	my @body
	  = ! defined $send ? ()
	  : $headers{'Content-Type'} eq 'application/json' ? (json => $send)
	  :                   $send;

	# $tx is a Mojo::Transaction::HTTP
	my $tx   = $ua->build_tx($method => $url, \%headers, @body);

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
	}

	1;
}

sub _extractAnswer($)
{	my ($self, $response) = @_;
	my $content = $response->content;
	return $response->json
		unless $response->content->is_multipart;

	my $part = $response->content->parts->[0];
	decode_json $part->asset->slurp;
}

sub _attachment($$)
{	my ($self, $response, $name) = @_;
	my $parts = $response->content->parts || [];
	foreach my $part (@$parts)
	{	my $disp = $part->headers->content_disposition;
		return $part->asset->slurp
			if $disp && $disp =~ /filename="([^"]+)"/ && $1 eq $name;
	}
warn "NOT FOUND";
	undef;
}

sub _messageContent($) { $_[1]->body }

#-------------
=section Other
=cut

1;
