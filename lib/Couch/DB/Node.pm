# SPDX-FileCopyrightText: 2024 Mark Overmeer <mark@open-console.eu>
# SPDX-License-Identifier: EUPL-1.2-or-later

package Couch::DB::Node;

use Couch::DB::Util;

use Log::Report 'couch-db';

use Scalar::Util   qw/weaken/;

=chapter NAME

Couch::DB::Node - represent a node in the cluster

=chapter SYNOPSIS

  my $node = $couch->node('node1@127.0.0.1');
  my $node = $client->node;

  # Internal use only
  my $node = Couch::DB::Node->new(name => $name, couch => $couch);

=chapter DESCRIPTION

This represents a Node in the database cluster.  When this object is created,
it may very well be that there is no knowledge about the node yet.

=chapter METHODS

=section Constructors

=c_method new %options

=requires name STRING
=requires couch C<Couch::DB>-object

=cut

sub new(@) { my ($class, %args) = @_; (bless {}, $class)->init(\%args) }

sub init($)
{	my ($self, $args) = @_;
	$self->{CDN_name} = delete $args->{name} // panic "Node has no name";

	$self->{CDN_couch} = delete $args->{couch} or panic "Requires couch";
	weaken $self->{CDN_couch};

	$self;
}

#-------------
=section Accessors

=method name
=method couch
=cut

sub name()  { $_[0]->{CDN_name} }
sub couch() { $_[0]->{CDN_couch} }

#-------------
=section Node information

B<All CouchDB API calls> provide the C<delay> option, to create a result
object which will be run later.  It also always has the C<client> and
C<client> options, which can be used to limit the used connections to
collect this data.

Endpoint "/_node/{node-name}/_prometeus" is not (yet) supported, because
it is a plain-text version of the M<stats()> and M<server()> calls.

=method stats %options
[CouchDB API "GET /_node/{node-name}/_stats", UNTESTED]
Collect node statistics.
=cut

sub _pathTo($) { '/_node/'. $_[0]->name . '/' . $_[1] }

sub stats(%)
{	my ($self, %args) = @_;
	my $couch = $self->couch;

	#XXX No idea which data transformations can be done
	$couch->call(GET => $self->_pathTo('_stats'),
		$couch->_resultsConfig(\%args),
	);
}

=method server %options
[CouchDB API "GET /_node/{node-name}/_system", UNTESTED]
Presents information about the system of the server where the node
runs on.

B<Be aware> that the method is called C<server>, not C<system>
to avoid confusion with the local system and Perl's C<system>
function.
=cut

sub server(%)
{	my ($self, %args) = @_;
	my $couch = $self->couch;

	#XXX No idea which data transformations can be done
	$couch->call(GET => $self->_pathTo('_system'),
		$couch->_resultsConfig(\%args),
	);
}

=method restart %options
[CouchDB API "POST /_node/{node-name}/_restart", UNTESTED]
This may help you in a test environment, but should not be used in
production, according to the API documentation.
=cut

sub restart(%)
{	my ($self, %args) = @_;
	my $couch = $self->couch;

	#XXX No idea which data transformations can be done
	$couch->call(POST => $self->_pathTo('_restart'),
		$couch->_resultsConfig(\%args),
	);
}

=method software %options
[CouchDB API "POST /_node/{node-name}/_versions", UNTESTED]
Get details of some software running the node.
=cut

sub software(%)
{	my ($self, %args) = @_;
	my $couch = $self->couch;

	#XXX No idea which data transformations can be done.
    #XXX Some versions would match Perl's version object, but that's uncertain.
	$couch->call(POST => $self->_pathTo('_versions'),
		$couch->_resultsConfig(\%args),
	);
}

#-------------
=section Other
=cut

1;
