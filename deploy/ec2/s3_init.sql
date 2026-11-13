SET extension_directory='/opt/duckdb_ext';
LOAD httpfs;
LOAD aws;
CREATE OR REPLACE SECRET catalog_s3 (TYPE s3, PROVIDER credential_chain, REGION '__AWS_REGION__');
SET enable_object_cache=true;
SET memory_limit='__DUCKDB_MEMORY__';
SET threads=__DUCKDB_THREADS__;
