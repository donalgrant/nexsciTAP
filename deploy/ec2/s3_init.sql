SET extension_directory='/opt/duckdb_ext';
SET secret_directory='/var/lib/tap/secrets';
LOAD httpfs;
SELECT count(*) FROM duckdb_secrets();
SET enable_object_cache=true;
SET memory_limit='__DUCKDB_MEMORY__';
SET threads=__DUCKDB_THREADS__;
