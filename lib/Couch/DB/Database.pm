# SPDX-FileCopyrightText: 2024 Mark Overmeer <mark@open-console.eu>
# SPDX-License-Identifier: EUPL-1.2-or-later

package Couch::DB::Database;
use Mojo::Base 'Couch::DB::Document';

use Couch::DB::Util;

use Log::Report 'couch-db';

=chapter NAME

Couch::DB::Database - One database connection

=chapter SYNOPSIS

   my $db = Couch::DB->db('my-db');

=chapter DESCRIPTION

=chapter METHODS

=section Constructors

=c_method new %options
=cut

=c_method create %options
=cut

#-------------
=section Accessors
=cut

has client => sub { panic }, weak => 1;

#-------------
=section Other
=cut

1;
