# SPDX-FileCopyrightText: 2024 Mark Overmeer <mark@open-console.eu>
# SPDX-License-Identifier: EUPL-1.2-or-later

package Couch::DB::Design;

use Couch::DB::Util;

use Log::Report 'couch-db';

=chapter NAME

Couch::DB::Design - handle design documents

=chapter SYNOPSIS

=chapter DESCRIPTION

In CouchDB, design documents provide the main interface for building
a CouchDB application. The design document defines the views used to
extract information from CouchDB through one or more views. Design
documents are created within your CouchDB instance in the same way as
you create database documents, but the content and definition of the
documents is different. Design Documents are named using an ID defined
with the design document URL path, and this URL can then be used to
access the database contents.

=chapter METHODS

=section Constructors

=c_method new %options
=cut

#-------------
=section Accessors
=cut

#-------------
=section Other
=cut

1;
