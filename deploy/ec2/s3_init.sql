SET extension_directory='/opt/duckdb_ext';
SET secret_directory='/var/lib/tap/secrets';
LOAD httpfs;
SELECT count(*) FROM duckdb_secrets();
SET enable_object_cache=true;
-- DuckDB keeps what it reads from S3 in memory, up to the memory limit, for
-- later queries.  Here a query is a process of its own: nothing is reused,
-- and several requests at once each filled their share and were killed for
-- memory together.
SET enable_external_file_cache=false;
-- Work that does not fit in memory goes to the container's own disk, not to
-- the current directory, which is the shared work area.
SET temp_directory='/var/lib/tap/tmp';
SET max_temp_directory_size='__DUCKDB_TEMP__';
SET memory_limit='__DUCKDB_MEMORY__';
SET threads=__DUCKDB_THREADS__;
