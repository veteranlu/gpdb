/* contrib/meta_sync/meta_sync--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gmeta" to load this file. \quit

-- Register the function.

-- Show visibility map information for each block in a relation.
CREATE FUNCTION pg_show_gmetadata(regclass, regclass)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'gmeta'
LANGUAGE C STRICT;