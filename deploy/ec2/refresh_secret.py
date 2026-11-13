#!/usr/bin/env python
"""
Store the container's current AWS credentials as a DuckDB persistent secret.

    refresh_secret.py <secret directory> <region>

Resolving credentials through DuckDB's "aws" extension costs a request about
0.3 s (loading the AWS SDK and asking the instance metadata service).  This
does that work once, outside any request:  it writes the temporary keys to a
secret file that a query connection reads with only the "httpfs" extension
loaded.  Run it again before the keys expire (the entry point does so every
few minutes).

The file is written elsewhere and moved into place, so a request never sees
half of it.  It is readable only by the service's user, and holds nothing
that user could not already get from the metadata service.
"""

import os
import sys
import tempfile

import duckdb

NAME = 'catalog_s3'


def main():
    target, region = sys.argv[1], sys.argv[2]
    os.makedirs(target, mode=0o700, exist_ok=True)

    # Beside the target, so the move stays on one file system.
    with tempfile.TemporaryDirectory(dir=os.path.dirname(os.path.abspath(target))) as tmp:
        con = duckdb.connect()
        con.execute("SET extension_directory='/opt/duckdb_ext'")
        con.execute(f"SET secret_directory='{tmp}'")
        con.execute('LOAD httpfs')
        con.execute('LOAD aws')
        con.execute(f"CREATE OR REPLACE PERSISTENT SECRET {NAME} "
                    f"(TYPE s3, PROVIDER credential_chain, REGION '{region}')")
        con.close()

        fname = NAME + '.duckdb_secret'
        os.replace(os.path.join(tmp, fname), os.path.join(target, fname))


if __name__ == '__main__':
    main()
