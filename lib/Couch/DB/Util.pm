# SPDX-FileCopyrightText: 2024 Mark Overmeer <mark@overmeer.net>
# SPDX-License-Identifier: Artistic-2.0

package Couch::DB::Util;
use parent 'Exporter';

use warnings;
use strict;

use Log::Report 'couch-db';

our @EXPORT_OK = qw/flat/;

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

1;
