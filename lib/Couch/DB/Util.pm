# SPDX-FileCopyrightText: 2024 Mark Overmeer <mark@open-console.eu>
# SPDX-License-Identifier: EUPL-1.2-or-later

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

=chapter DESCRIPTION

All modules in CouchDB must import this module, because it also offers
additional features to the namespace, like 'warnings' and 'strict'.

=chapter Functions

=function flat LIST|ARRAY
Returns all defined elements found in the LIST or ARRAY.  The parameter
LIST may contain ARRAYs.
=cut

sub flat(@) { grep defined, map +(ref eq 'ARRAY' ? @$_ : $_), @_ }

1;
