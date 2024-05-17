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

sub _pathToNode($) { '/_node/'. $_[0]->name . '/' . $_[1] }

sub stats(%)
{	my ($self, %args) = @_;
	my $couch = $self->couch;

	#XXX No idea which data transformations can be done
	$couch->call(GET => $self->_pathToNode('_stats'),
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

	#XXX No idea which data transformations can be done
	$self->couch->call(GET => $self->_pathToNode('_system'),
		$self->couch->_resultsConfig(\%args),
	);
}

=method restart %options
[CouchDB API "POST /_node/{node-name}/_restart", UNTESTED]
This may help you in a test environment, but should not be used in
production, according to the API documentation.
=cut

sub restart(%)
{	my ($self, %args) = @_;

	#XXX No idea which data transformations can be done
	$self->couch->call(POST => $self->_pathToNode('_restart'),
		$self->couch->_resultsConfig(\%args),
	);
}

=method software %options
[CouchDB API "POST /_node/{node-name}/_versions", UNTESTED]
Get details of some software running the node.
=cut

sub software(%)
{	my ($self, %args) = @_;

	#XXX No idea which data transformations can be done.
    #XXX Some versions would match Perl's version object, but that's uncertain.
	$self->couch->call(POST => $self->_pathToNode('_versions'),
		$self->couch->_resultsConfig(\%args),
	);
}

=method config %options
[CouchDB API "GET /_node/{node-name}/_config", UNTESTED],
[CouchDB API "GET /_node/{node-name}/_config/{section}", UNTESTED], and
[CouchDB API "GET /_node/{node-name}/_config/{section}/{key}", UNTESTED].
Returns the node configuration.

At least according to the example in the spec, all values are strings.
So, a boolean will be string "true" or "false".  The API notes that the
actual type of values is unpredictable.

=option  section STRING
=default section C<undef>

=option  key     STRING
=default key     C<undef>
(Requires a section to be specified)

=examples of config
  # Three times the same.  The last may be the most efficient for the server.
  my $mode = $node->config->values->{log}{level};
  my $mode = $node->config(section => 'log')->values->{level};
  my $mode = $node->config(section => 'log', key => 'level')->values;
=cut

sub config(%)
{	my ($self, %args) = @_;
	my $path = $self->_pathToNode('_config');

	if(my $section = delete $args{section})
	{	$path .= "/$section";
		if(my $key = delete $args{key})
		{	$path .= "/$key";
		}
	}

	$self->couch->call(GET => $path,
		$self->couch->_resultsConfig(\%args),
	);
}

=method configChange $section, $key, $value, %options
[CouchDB API "PUT /_node/{node-name}/_config/{section}/{key}", UNTESTED]>
Change one value in the configuration.  Probably, it should be followed by
a M<configReload()>: changes may not be commited without reload.

You MAY need to convert booleans to string "true" or "false" by hand.
=cut

sub configChange($$$%)
{	my ($self, $section, $key, $value, %args) = @_;

	$self->couch->call(PUT => self->_pathToNode("_config/$section/$key"),
		send => $value,
		$self->couch->_resultsConfig(\%args),
	);
}


=method configDelete $section, $key, %options
[CouchDB API "DELETE /_node/{node-name}/_config/{section}/{key}", UNTESTED]>
Remove one value in the configuration.  Probably, it should be followed by
a M<configReload()>: changes may not be commited without reload.
=cut

sub configDelete($$%)
{	my ($self, $section, $key, %args) = @_;

	$self->couch->call(DELETE => self->_pathToNode("_config/$section/$key"),
		$self->couch->_resultsConfig(\%args),
	);
}

=method configReload %options
[CouchDB API "POST /_node/{node-name}/_config/_reload", UNTESTED]>
Re-apply the configuration to the node.  This has as side-effect that the
(changed) configuration of the node will be saved.
=cut

sub configReload(%)
{	my ($self, %args) = @_;

	$self->couch->call(POST => self->_pathToNode("_config/_reload"),
		$self->couch->_resultsConfig(\%args),
	);
}

#-------------
=section Other
=cut

1;
