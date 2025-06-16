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

  foreach my $row (@rows)
  {   printf "page %3d item %4d: %s\n",
         $row->pageNumber,
         $row->rowNumberInPage,
         $row->doc->{name};
  }

=chapter DESCRIPTION

Many command can page their answers.  The resulting rows are
each wrapped in this object for nicer abstraction of the data
structures.

=chapter METHODS

=section Constructors

=c_method new %options

=requires result M<Couch::DB::Result>
The result-object which contains this row.

=requires answer JSON
The JSON structure from the result which represents this row.

=option  values HASH
=default values C<undef>
The answer about this row converted to Perl data types.  Default to
the C<answer>.

=option  doc M<Couch::DB::Document>-object
=default doc C<undef>

=requires rownr INTEGER
The location of this row in the result.  Starts at 1.
=cut

sub new(@) { my ($class, %args) = @_; (bless {}, $class)->init(\%args) }

sub init($)
{	my ($self, $args) = @_;

	$self->{CDR_result} = delete $args->{result} or panic;
	weaken $self->{CDR_result};

	$self->{CDR_doc}    = delete $args->{doc};
	$self->{CDR_answer} = delete $args->{answer} or panic;
	$self->{CDR_values} = delete $args->{values};
	$self->{CDR_rownr}  = delete $args->{rownr}  or panic;
	$self;
}

#-------------
=section Accessors

=method result
The M<Couch::DB::Result> structure which contained this row.  Within one
page, this may be different for different rows.
=cut

sub result() { $_[0]->{CDR_result} }

=method doc
In case the response contains a document structure (you may need to
use C<include_docs> in the query), then this method will return a
C<Couch::DB::Document> object.
=cut

sub doc() { $_[0]->{CDR_doc} }

=method answer
The JSON fragment from the result answer which contains the information
about this row.
=cut

sub answer() { $_[0]->{CDR_answer} }

=method values
The answer about this row, translated into Perl data types.
=cut

sub values() { $_[0]->{CDR_values} || $_[0]->answer }

#-------------
=section Paging

=method pageNumber
=method rowNumberInPage
=method rowNumberInSearch
=method rowNumberInResult
=cut

sub pageNumber() { $_[0]->result->pageNumber }
sub rowNumberInPage() { ... }
sub rowNumberInSearch() { ... }
sub rowNumberInResult() { $_[0]->{CDR_rownr} }

1;
