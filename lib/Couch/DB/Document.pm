# SPDX-FileCopyrightText: 2024 Mark Overmeer <mark@open-console.eu>
# SPDX-License-Identifier: EUPL-1.2-or-later

package Couch::DB::Document;
use Couch::DB::Util;

use Log::Report 'couch-db';

=chapter NAME

Couch::DB::Document - one document as exchanged with a CouchDB server

=chapter SYNOPSIS

=chapter DESCRIPTION

=chapter METHODS

=section Constructors

=c_method new %options

=option   data HASH
=default  data C<+{ }>
The document data, in CouchDB syntax.

=cut

sub new(@) { my ($class, %args) = @_; (bless {}, $class)->init(\%args) }

sub init($)
{	my ($self, $args) = @_;
	$self->{CBD_data} = delete $args->{data} || {};

	# The Document is (for now) not linked to its Result source, because
	# that might consume a lot of memory.  Although it may help debugging.
	# weaken $self->{CBD_result} = my $result = delete $args->{result};

	$self;
}

sub fromJSON($%)
{	my ($class, $json, %args) = @_;
	$class->new(data => $json, %args);
}

#-------------
=section Accessors

=method data
This provides access to the raw data received from/to be sent to the CouchDB
server.

B<Warning:> Where Perl does not support the same data-types as JSON, you need to
be very careful when addressing fields from this structure.  B<Much better> is
it to use the provided abstraction methods, which hide those differences.  Those
also hide changes in the server software, over time.
=cut

sub data() { $_[0]->{CBD_data} }

#-------------
=section Attachments
=cut

#-------------
=section Other
=cut

1;
