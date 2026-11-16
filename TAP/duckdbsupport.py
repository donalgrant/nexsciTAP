# Copyright (c) 2020, Caltech IPAC.
# This code is released with a BSD 3-clause license. License information is at
#   https://github.com/Caltech-IPAC/nexsciTAP/blob/master/LICENSE

"""
DuckDB support for nexsciTAP.

DuckDB is used here as a query engine over Parquet files (local or in S3)
rather than as a database server.  The "database" is a small DuckDB file
that holds only

  - one view per catalog, each a SELECT over read_parquet(...), and
  - the TAP_SCHEMA tables, in a schema of that name.

so the data themselves stay in the Parquet files and nothing is loaded or
rebuilt when the service starts.
"""

import logging
import re


def adql_dialect(dbms):
    """
    Name of the SQL dialect to request from the ADQL translator.

    The translator knows 'oracle' (ROWNUM) and 'sqlite3' (LIMIT) forms of
    TOP n.  DuckDB takes LIMIT, so it is translated as 'sqlite3'.
    """

    if dbms is not None and dbms.lower() == 'duckdb':
        return 'sqlite3'

    return dbms


def connect(connectInfo, debug=0):
    """
    Open a read-only DuckDB connection described by connectInfo.

    connectInfo['db']             path of the DuckDB database file
    connectInfo['init_sql_file']  optional file of SQL statements to run on
                                  the new connection, separated by ';'
                                  (e.g. LOAD httpfs; CREATE SECRET ...)
    connectInfo['allowed_directories']
                                  directories or URL prefixes holding the
                                  Parquet files; nothing else can be read
    connectInfo['allowed_paths']  individual files that must stay readable
                                  (e.g. the CA certificate bundle that
                                  HTTPS access to S3 needs)
    connectInfo['query_timeout']  seconds after which whatever the connection
                                  is running is cancelled (None or 0: never)
    """

    import duckdb

    conn = duckdb.connect(connectInfo['db'], read_only=True)

    init_sql_file = connectInfo.get('init_sql_file')

    if init_sql_file:

        with open(init_sql_file) as f:
            statements = f.read()

        for stmt in statements.split(';'):

            stmt = stmt.strip()

            if stmt:
                if debug:
                    logging.debug('DuckDB init: ' + stmt)

                conn.execute(stmt)

    # Lock the engine down before any user SQL runs.  DuckDB can read any
    # file or URL named in a query (read_csv('/etc/passwd'), read_parquet
    # of someone else's bucket), and the TAP_SCHEMA table check does not
    # see table functions inside subqueries.  External access is therefore
    # switched off except for the directories or URL prefixes that hold
    # the catalogs, and the configuration is frozen so a query cannot
    # switch it back on.

    for setting in ('allowed_directories', 'allowed_paths'):

        allowed = connectInfo.get(setting) or []

        if isinstance(allowed, str):
            allowed = [allowed]

        allowed = [a.strip() for a in allowed if a.strip()]

        if allowed:
            quoted = ', '.join("'" + a.replace("'", "''") + "'" for a in allowed)
            conn.execute('SET ' + setting + ' = [' + quoted + ']')

    conn.execute('SET enable_external_access = false')
    conn.execute('SET lock_configuration = true')

    return Connection(conn, connectInfo.get('upload_tables'),
                      Deadline(conn, connectInfo.get('query_timeout')))


class QueryTimeout(Exception):
    pass


class Deadline:
    """
    Cancels the connection's work once a time limit has passed.

    A signal cannot do this: Python runs signal handlers only between
    bytecodes, and a long query spends its time inside DuckDB.  A timer
    thread calls DuckDB's own interrupt instead, which makes the running
    statement raise; anything started afterwards is refused here.

    The clock starts when the connection is made.  Each request makes its
    own connection, so the limit is on the request's time in the engine,
    reading the result included.
    """

    def __init__(self, duck, seconds):
        import threading

        try:
            self.seconds = float(seconds or 0)
        except (TypeError, ValueError):
            self.seconds = 0

        self.expired = False

        # DuckDB interrupts one connection object at a time, and each
        # cursor is a connection of its own, so the ones in use are kept.
        self._targets = [duck]

        if self.seconds > 0:

            def expire():
                self.expired = True
                for target in list(self._targets):
                    try:
                        target.interrupt()
                    except Exception:
                        pass

            timer = threading.Timer(self.seconds, expire)
            timer.daemon = True
            timer.start()

    def watch(self, cursor):
        self._targets.append(cursor)

    def unwatch(self, cursor):
        if cursor in self._targets:
            self._targets.remove(cursor)

    def error(self):
        return QueryTimeout('Query exceeded the time limit of %g seconds.' % self.seconds)

    def check(self):
        if self.expired:
            raise self.error()

    def run(self, call, *args):
        """Run one DuckDB call, reporting an interrupt as a timeout."""

        self.check()

        try:
            return call(*args)
        except Exception:
            if self.expired:
                raise self.error() from None
            raise


class Cursor:
    """
    DB-API 2.0 cursor over a DuckDB connection.

    DuckDB's own cursor objects have no settable `arraysize`, which the
    rest of nexsciTAP relies on to size its fetchmany() batches.
    """

    def __init__(self, duck, upload_tables=None, deadline=None):
        self._cur = duck.cursor()
        self._deadline = deadline or Deadline(duck, 0)
        self._deadline.watch(self._cur)
        self._decimal = []
        self.arraysize = 1

        # Uploaded tables (TAP_UPLOAD) are in-memory Arrow tables.  DuckDB
        # scopes a registration to one cursor, so each cursor gets its own;
        # nothing is copied.
        for name, table in (upload_tables or {}).items():
            self._cur.register(name, table)

    @property
    def description(self):
        return self._cur.description

    @property
    def rowcount(self):
        return -1

    def execute(self, sql, params=None):
        if params is None:
            self._deadline.run(self._cur.execute, sql)
        else:
            self._deadline.run(self._cur.execute, sql, params)

        # DECIMAL columns arrive as decimal.Decimal, which the result
        # writer cannot format; they are reported and returned as doubles.
        desc = self._cur.description or []
        self._decimal = [i for i, d in enumerate(desc)
                         if str(d[1]).upper().startswith('DECIMAL')]
        return self

    def _rows(self, rows):
        if not self._decimal:
            return rows
        out = []
        for row in rows:
            row = list(row)
            for i in self._decimal:
                if row[i] is not None:
                    row[i] = float(row[i])
            out.append(tuple(row))
        return out

    def fetchone(self):
        row = self._deadline.run(self._cur.fetchone)
        return None if row is None else self._rows([row])[0]

    def fetchmany(self, size=None):
        return self._rows(self._deadline.run(self._cur.fetchmany, self.arraysize if size is None else size))

    def fetchall(self):
        return self._rows(self._deadline.run(self._cur.fetchall))

    def close(self):
        self._deadline.unwatch(self._cur)
        self._cur.close()


class Connection:
    """DB-API 2.0 connection wrapper returning the Cursor above."""

    def __init__(self, duck, upload_tables=None, deadline=None):
        self._duck = duck
        self._upload_tables = upload_tables
        self._deadline = deadline

    def cursor(self):
        return Cursor(self._duck, self._upload_tables, self._deadline)

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        self._duck.close()


def result_datatype(typecode):
    """
    Map a DuckDB cursor.description type to the type names writeResult
    already understands for other DBMSs.
    """

    name = str(typecode).upper()

    if name.startswith('DECIMAL') or name in ('DOUBLE', 'FLOAT', 'REAL'):
        return 'double'

    if name in ('BIGINT', 'UBIGINT', 'UINTEGER', 'HUGEINT', 'UHUGEINT'):
        return 'bigint'

    if name in ('INTEGER', 'SMALLINT', 'TINYINT', 'USMALLINT', 'UTINYINT',
                'BOOLEAN'):
        return 'integer'

    if name == 'DATE':
        return 'date'

    if name == 'TIME':
        return 'time'

    if name.startswith('TIMESTAMP'):
        return 'timestamp'

    return 'varchar'


def rescale_index_constraint(sql, colname, level, stored_level):
    """
    Rewrite spatial-index constraints computed at HEALPix `level` so they
    apply to a column that stores nested HEALPix indices at `stored_level`.

    The ADQL translator emits constraints such as

        (hpx BETWEEN 2355077129 AND 2355077135) OR (hpx = 2355077146)

    for cells at `level`.  In the nested scheme a cell c at `level` contains
    exactly the cells  c << s  ...  ((c+1) << s) - 1  at `stored_level`,
    where s = 2 * (stored_level - level).  The rewritten constraints are plain
    ranges on the stored column, so a B-tree, or Parquet row-group statistics
    on data sorted by that column, can prune on them directly.

    This lets a catalog carry a single deep index (for example the order-29
    index HATS calls _healpix_29) instead of one column per search level.

    Only the translator's own "(col BETWEEN a AND b)" and "(col = a)" forms
    are rewritten.
    """

    if stored_level is None:
        return sql

    shift = 2 * (int(stored_level) - int(level))

    if shift == 0:
        return sql

    if shift < 0:
        raise Exception('Spatial index STORED_LEVEL must not be lower than LEVEL.')

    col = re.escape(colname)

    def between(m):
        lo = int(m.group(1)) << shift
        hi = ((int(m.group(2)) + 1) << shift) - 1
        return '(' + colname + ' BETWEEN ' + str(lo) + ' AND ' + str(hi) + ')'

    def equal(m):
        lo = int(m.group(1)) << shift
        hi = ((int(m.group(1)) + 1) << shift) - 1
        return '(' + colname + ' BETWEEN ' + str(lo) + ' AND ' + str(hi) + ')'

    sql = re.sub(r'\(' + col + r' BETWEEN (\d+) AND (\d+)\)', between, sql)
    sql = re.sub(r'\(' + col + r' = (\d+)\)', equal, sql)

    return sql


_REGION = re.compile(r"\b(BOX|POLYGON)\s*\(\s*'([^']*)'\s*,([^()]*)\)", re.I)
MAX_REGION_RANGES = 300


def _index_clause(colname):
    col = re.escape(colname)
    term = r'\(' + col + r'\s+(?:BETWEEN\s+\d+\s+AND\s+\d+|=\s*\d+)\)'
    return re.compile(r'\s+AND\s+\(\s*' + term + r'(?:\s+OR\s+' + term + r')*\s*\)')


_HALF_SPACE = re.compile(r'\(([-+\d.eE]+)\*\w+\)\+\(([-+\d.eE]+)\*\w+\)\+\(([-+\d.eE]+)\*\w+\)\s*>=\s*0\.?(?![\d])')


def _bounding_cap(sql):
    """
    Center (lon, lat) and radius, in degrees, of a cap holding the region the
    translated query tests for.

    The translator turns a BOX or POLYGON into one condition per edge,
    "(a*x)+(b*y)+(c*z)>=0.", a half-space through the origin.  The region's
    corners are where consecutive edges meet, so they are read off the SQL
    itself rather than re-derived from the ADQL, whatever convention the
    translator used to build them.
    """
    import math

    normals = [tuple(float(v) for v in m) for m in _HALF_SPACE.findall(sql)]
    if len(normals) < 3:
        return None

    def cross(a, b):
        return (a[1] * b[2] - a[2] * b[1], a[2] * b[0] - a[0] * b[2], a[0] * b[1] - a[1] * b[0])

    def dot(a, b):
        return a[0] * b[0] + a[1] * b[1] + a[2] * b[2]

    corners = []
    for k in range(len(normals)):
        v = cross(normals[k], normals[(k + 1) % len(normals)])
        norm = math.sqrt(dot(v, v))
        if norm < 1e-12:
            return None
        v = tuple(c / norm for c in v)
        inside = [w for w in (v, tuple(-c for c in v)) if all(dot(n, w) >= -1e-9 for n in normals)]
        if len(inside) != 1:
            return None
        corners.append(inside[0])

    c = tuple(sum(v[i] for v in corners) for i in range(3))
    norm = math.sqrt(dot(c, c))
    if norm < 1e-9:
        return None
    c = tuple(x / norm for x in c)
    radius = max(math.degrees(math.acos(max(-1.0, min(1.0, dot(c, v))))) for v in corners)

    # Edges are great-circle arcs, which stay inside a cap that holds their
    # ends only while the cap is smaller than a hemisphere.
    if radius >= 60.0:
        return None

    lon = math.degrees(math.atan2(c[1], c[0])) % 360.0
    lat = math.degrees(math.asin(max(-1.0, min(1.0, c[2]))))
    return lon, lat, radius * 1.01 + 1e-4


def _cap_ranges(lon, lat, radius, index_level):
    """Ranges of the nested index at `index_level` covering a cap, a few hundred at most."""
    import math

    import astropy.units as u
    from astropy_healpix import HEALPix

    # Cells about a tenth of the radius across: a few hundred of them.
    order = int(math.floor(math.log2(58.6 / max(radius / 10.0, 1e-6))))
    order = max(0, min(order, int(index_level), 14))

    while True:
        cells = sorted(int(c) for c in HEALPix(nside=2 ** order, order='nested')
                       .cone_search_lonlat(lon * u.deg, lat * u.deg, radius * u.deg))
        ranges = []
        for c in cells:
            if ranges and c == ranges[-1][1] + 1:
                ranges[-1][1] = c
            else:
                ranges.append([c, c])
        if len(ranges) <= MAX_REGION_RANGES or order == 0:
            break
        order -= 1

    shift = 2 * (int(index_level) - order)
    return [(lo << shift, ((hi + 1) << shift) - 1) for lo, hi in ranges]


def replace_region_cover(sql, adql, colname, index_level):
    """
    Replace the spatial-index constraint of a BOX or POLYGON search.

    For those two shapes the spatial index library returns, in HPX mode, a list
    of cells that covers only part of the region, so sources in the rest are
    never looked at even though the exact geometry test would accept them.
    (Cone searches are not affected.)  This takes the library's cells out and
    puts in ranges covering a cap that holds the whole region, computed with
    astropy_healpix.  The exact geometry test in the query is untouched, so
    the answer is decided by it alone and the ranges only limit what is read.

    `sql` is the translated query, after rescale_index_constraint; `index_level`
    is the HEALPix level of the values in the index column.

    Anything this does not fully understand (two geometry constraints, edge
    conditions it cannot read, a region wider than about a third of the sky)
    loses its index constraint altogether:  the query then scans the table
    and is still answered correctly.
    """

    regions = _REGION.findall(adql or '')
    if not regions:
        return sql

    clause = _index_clause(colname)
    if not clause.search(sql):
        return sql

    ranges = None
    geometry = len(re.findall(r'\bCONTAINS\s*\(', adql, re.I)) + len(re.findall(r'\bINTERSECTS\s*\(', adql, re.I))

    if len(regions) == 1 and geometry == 1:
        cap = _bounding_cap(sql)
        if cap:
            ranges = _cap_ranges(*cap, index_level)

    if ranges is None:
        return clause.sub('', sql)

    cover = ' AND (' + ' OR '.join('(' + colname + ' BETWEEN ' + str(lo) + ' AND ' + str(hi) + ')'
                                   for lo, hi in ranges) + ')'
    return clause.sub(lambda m: cover, sql, count=1)

