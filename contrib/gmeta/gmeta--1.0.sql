/* contrib/meta_sync/meta_sync--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gmeta" to load this file. \quit

-- Register the function.

-- Show visibility map information for each block in a relation.


-- Register the function.
CREATE FUNCTION pg_show_gmetadata(regclass, regclass)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_show_gmetadata'
LANGUAGE C PARALLEL SAFE;