# SPDX-FileCopyrightText: 2024 Mark Overmeer <mark@overmeer.net>
# SPDX-License-Identifier: Artistic-2.0

package Couch::DB::Row;

use Couch::DB::Util;

use Log::Report 'couch-db';

use Scalar::Util   qw/weaken/;

=chapter NAME

Couch::DB::Row - a single row of a page

=chapter SYNOPSIS

  my $list = $db->allDocs({include_docs => 1}, _all => 1);
  my @rows = $list->page;
  my @docs = map $_->doc, @rows;

=chapter DESCRIPTION

Many command can page their answers.  The resulting rows are
each wrapped in this object for nicer abstraction of the data
structures.

=chapter METHODS

=section Constructors

=c_method new %options

=cut

sub new(@) { my ($class, %args) = @_; (bless {}, $class)->init(\%args) }

sub init($)
{	my ($self, $args) = @_;
	$self;
}

#-------------
=section Accessors

=method doc
In case the response contains a document structure (you may need to
use C<include_docs> in the query), then this method will return a
C<Couch::DB::Document> object.
=cut

sub doc() { $_[0]->{CDR_doc} }

1;
