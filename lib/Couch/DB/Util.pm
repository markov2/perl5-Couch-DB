# SPDX-FileCopyrightText: 2024 Mark Overmeer <mark@overmeer.net>
# SPDX-License-Identifier: Artistic-2.0

package Couch::DB::Util;
use parent 'Exporter';

use warnings;
use strict;

use Log::Report 'couch-db';
use Data::Dumper ();
use Scalar::Util qw(blessed);

our @EXPORT_OK   = qw/flat pile apply_tree simplified/;
our %EXPORT_TAGS = ( all => \@EXPORT_OK );

sub import
{	my $class  = shift;
	$_->import for qw(strict warnings utf8 version);
	$class->export_to_level(1, undef, @_);
}

=chapter NAME

Couch::DB::Util - utility functions

=chapter SYNOPSIS

   use Couch::DB::Util;           # obligatory!
   use Couch::DB::Util  qw(flat); # alternative

=chapter DESCRIPTION

All modules in CouchDB B<must import> this module, because it also offers
additional features to the namespace, like 'warnings' and 'strict'.

=chapter Functions

=function flat LIST|ARRAY
Returns all defined elements found in the LIST or ARRAY.  The parameter
LIST may contain ARRAYs.
=cut

sub flat(@) { grep defined, map +(ref eq 'ARRAY' ? @$_ : $_), @_ }

=function pile LIST|ARRAY
Create a new ARRAY from the offered arguments, combining all elements
from the LIST and ARRAYs.  Undefined elements are removed.
=cut

sub pile(@) { +[ flat @_ ] }

=function apply_tree $tree, CODE
Apply the CODE to all elements in the $tree.  Returns a new tree.
=cut

#XXX why can't I find a CPAN module which does this?

sub apply_tree($$);
sub apply_tree($$)
{	my ($tree, $code) = @_;
	    ! ref $tree          ? $code->($tree)
	  : ref $tree eq 'ARRAY' ? +[ map apply_tree($_, $code), @$tree ]
	  : ref $tree eq 'HASH'  ? +{ map +($_ => apply_tree($tree->{$_}, $code)), keys %$tree }
	  : ref $tree eq 'CODE'  ? "$tree"
	  :                        $code->($tree);
}

=function simplified $name, $data
Returns a M<Data::Dumper> output, which is a simplified version of the $data.
A normal dump would show internals of objects which make the output very verbose,
hence harder to interpret.
=cut

sub simplified($$)
{	my ($name, $data) = @_;

	my $v = apply_tree $data, sub ($) {
		my $e = shift;
		    ! blessed $e         ? $e
		  : $e->isa('DateTime')  ? "DATETIME($e)"
		  : $e->isa('Couch::DB::Document') ? 'DOCUMENT('.$e->id.')'
		  : $e->isa('JSON::PP::Boolean')   ? ($e ? 'BOOL(true)' : 'BOOL(false)')
		  : $e->isa('version')   ? "VERSION($e)"
		  : 'OBJECT('.(ref $e).')';
	};

	Data::Dumper->new([$v], [$name])->Indent(1) ->Quotekeys(0)->Sortkeys(1)->Dump;
}

1;
