#!/usr/bin/env python
# Copyright (c) 2020, Caltech IPAC.
# This code is released with a BSD 3-clause license. License information is at
#   https://github.com/Caltech-IPAC/nexsciTAP/blob/master/LICENSE

"""
Convert a table of sky positions to the Parquet layout the DuckDB backend serves.

    make_parquet_catalog.py INPUT OUTPUT_DIR [--ra COL] [--dec COL] [--schema FILE]
                            [--description TEXT] [--rows-per-file N] [--row-group N]

INPUT is a CSV, Parquet, FITS or VOTable file, or the directory of a HATS
catalog as it is published (CSV and Parquet may be a glob,
and may be s3:// or https:// locations DuckDB can read).  OUTPUT_DIR receives
part-00.parquet, part-01.parquet, ...; copy that directory to the catalog's
folder in S3 and list it with make_duckdb_catalog.py.

The output has every input column, with these changes:

  * the position columns are named "ra" and "dec" (degrees), whatever they
    were called, because the service uses one pair of names for all tables;
  * column names are lower-cased and anything but letters, digits and "_"
    becomes "_", so that they are plain ADQL identifiers;  a name is made to
    start with a letter ("2MASS" becomes "c_2mass", "_row" becomes "row"),
    a word ADQL keeps for itself gets a "_" after it ("size" becomes
    "size_"), and two columns that come to the same name are numbered;
  * a column "hpx29" is added: the nested HEALPix order-29 index of the
    position (the quantity HATS calls _healpix_29);
  * rows with no position are dropped, since they could never be found by one.

A CSV file says nothing about column types, so they are guessed from the
values.  The guess goes wrong where the values do not settle it: a column with
no values at all becomes text, a text column that happens to look like dates
becomes dates, and single precision is not told from double.  --schema FILE
gives the types instead.  FILE is a CSV with a header and two columns, the
column name and its type (int, long, short, float, double, char, boolean),
which is what "SELECT column_name, datatype FROM TAP_SCHEMA.columns WHERE
table_name = '...'" returns from a TAP service.  Columns it does not list keep
their guessed type.

The same file says what the columns mean.  Further columns headed "unit",
"ucd", "utype" and "description" (any of them, in any order; add them to the
TAP_SCHEMA query above) are recorded in the Parquet files, and --description
does the same for the table as a whole.  The service shows them to users, in
its table listing and in the results it returns.  FITS tables and VOTables
carry units and descriptions themselves, which are kept unless FILE says
otherwise.  The metadata is stored the way the IVOA note "Parquet in the
Virtual Observatory" lays out (see votable_metadata.py), so other tools that
read Parquet can use it too.

Rows are sorted by hpx29 and written in small row groups.  Parquet keeps the
smallest and largest value of every column in each row group, so a positional
search reads only the few groups whose hpx29 range it touches.  A group holds
about ten million values, so a wide table gets fewer rows in each (see
row_group_size); --row-group N sets the rows outright.
"""

import argparse
import csv
import glob
import os
import re
import sys

import duckdb
import numpy as np
import votable_metadata
import pyarrow as pa
import pyarrow.parquet as pq
from astropy_healpix import lonlat_to_healpix
from astropy import units as u

ORDER = 29

# A scan holds several row groups in memory at once, a block for every column
# it reads, so what matters is the number of values in a group, not of rows.
# At 100,000 rows a group, SELECT * with a filter that is not positional ran
# out of memory at 1 GB on a 520-column catalog (52 million values a group);
# at about 10 million values it ran in a second, with files no larger.  Fewer
# values still buys nothing more and slows scans of a few columns.
GROUP_VALUES = 10_000_000
GROUP_ROWS_MAX = 100_000
GROUP_ROWS_MIN = 2_048


def row_group_size(columns):
    """Rows in a row group for a table of this many columns: about GROUP_VALUES values, in whole DuckDB vectors."""
    rows = GROUP_VALUES // max(1, columns) // 2048 * 2048
    return max(GROUP_ROWS_MIN, min(GROUP_ROWS_MAX, rows))


TYPES = {'int': 'INTEGER', 'integer': 'INTEGER', 'short': 'SMALLINT', 'smallint': 'SMALLINT',
         'long': 'BIGINT', 'bigint': 'BIGINT', 'float': 'FLOAT', 'real': 'FLOAT',
         'double': 'DOUBLE', 'char': 'VARCHAR', 'varchar': 'VARCHAR', 'string': 'VARCHAR',
         'unicodechar': 'VARCHAR', 'boolean': 'BOOLEAN'}


NAME_HEADS = ('column_name', 'name', 'column')
TYPE_HEADS = ('datatype', 'type')
META_HEADS = ('unit', 'ucd', 'utype', 'description')


def read_schema(path):
    """
    ({column name: DuckDB type}, {column name: {unit, ucd, utype, description}})
    from a CSV with a header.  Columns are found by their headings; a file
    whose headings are not recognized is read as name, type.
    """

    with open(path, newline='') as f:
        rows = list(csv.reader(f))
    if not rows:
        return {}, {}

    heads = [h.strip().lower() for h in rows[0]]

    def position(names, default):
        return next((heads.index(n) for n in names if n in heads), default)

    name_at = position(NAME_HEADS, 0)
    type_at = position(TYPE_HEADS, 1 if not set(heads) & set(META_HEADS) else None)
    meta_at = {h: heads.index(h) for h in META_HEADS if h in heads}

    types, meta = {}, {}
    for row in rows[1:]:
        if len(row) <= name_at or not row[name_at].strip():
            continue
        name = row[name_at].strip()

        kind = row[type_at].strip().lower() if type_at is not None and len(row) > type_at else ''
        if kind:
            if kind not in TYPES:
                sys.exit(f'{path}: unknown type "{row[type_at]}" for column "{name}" '
                         f'(known: {", ".join(sorted(TYPES))})')
            types[name] = TYPES[kind]

        found = {h: row[i].strip() for h, i in meta_at.items() if len(row) > i and row[i].strip()}
        if found:
            meta[name] = found

    return types, meta


# ---- HATS ---------------------------------------------------------------------
# A HATS catalog (the LSDB project's layout, once "HiPSCat") is a directory:
# a properties file, and under dataset/ one Parquet file for each HEALPix cell
# it is divided into, Norder=N/Dir=D/Npix=P.parquet or, as IRSA writes them,
# Npix=P/part0.snappy.parquet.  It is read as it is published.  Its own
# division of the sky and its own index column are not kept:  this converter
# sorts the rows and writes the index the service uses.

HATS_PROPERTIES = ('properties', 'hats.properties')
# Columns HATS adds to a catalog:  its index (under its present name and its
# old one), IRSA's two coarser indexes, and the partition a row's file is in.
HATS_COLUMNS = ('_healpix_29', '_hipscat_index', '_healpix_19', '_healpix_9', 'Norder', 'Dir', 'Npix')


def read_properties(path):
    """The key=value lines of a HATS properties file.  Lines starting with # are comments; \\: is a colon."""
    out = {}
    with open(path, encoding='utf-8', errors='replace') as f:
        for line in f:
            line = line.strip()
            if line.startswith('#') and '#' not in out:
                out['#'] = line.lstrip('# ').strip()     # the first comment, often the catalog's title
            if not line or line.startswith('#') or '=' not in line:
                continue
            key, value = line.split('=', 1)
            out[key.strip()] = value.strip().replace('\\:', ':')
    return out


def hats_catalog(path):
    """
    What a directory holds, if it is a HATS catalog:  its root, its properties
    and its data files.  A HATS "collection" (a catalog with its margin and
    index tables beside it) is followed to its primary catalog.  None for a
    directory that is not HATS.
    """
    for _ in range(3):                                  # a collection names a catalog; no deeper
        coll = os.path.join(path, 'collection.properties')
        if not os.path.isfile(coll):
            break
        primary = read_properties(coll).get('hats_primary_table_url', '').strip('/')
        if not primary or '..' in primary or '://' in primary or not os.path.isdir(os.path.join(path, primary)):
            return None
        path = os.path.join(path, primary)
    props = next((os.path.join(path, n) for n in HATS_PROPERTIES if os.path.isfile(os.path.join(path, n))), None)
    data = os.path.join(path, 'dataset')
    if not props or not os.path.isdir(data):
        return None
    files = sorted(os.path.join(d, f) for d, _, names in os.walk(data) for f in names
                   if f.endswith('.parquet') and 'Norder=' in d)
    return {'root': path, 'properties': read_properties(props), 'files': files} if files else None


def field_metadata(path):
    """
    Units and descriptions written on the columns of a Parquet file itself
    (Arrow's field metadata), as IRSA writes them in its HATS catalogs:
    {column: {'unit': ..., 'description': ..., 'ucd': ...}}.  Empty where
    a file has none, which is most files.
    """
    try:
        schema = pq.read_schema(path)
    except Exception:                       # not readable as Parquet from here (a pattern, a remote address)
        return {}
    names = {'unit': 'unit', 'units': 'unit', 'description': 'description', 'desc': 'description',
             'comment': 'description', 'ucd': 'ucd'}
    out = {}
    for field in schema:
        found = {}
        for key, value in (field.metadata or {}).items():
            what = names.get(key.decode('utf-8', 'replace').lower())
            text = value.decode('utf-8', 'replace').strip()
            if what and text and text.lower() not in ('none', 'null', 'n/a'):
                found.setdefault(what, text)
        if found:
            out[field.name] = found
    return out


def hats_description(props):
    """What a HATS catalog says it is, for a table given no description:  its title, its reference, and where it is from."""
    title = props.get('obs_title') or props.get('#', '')
    if re.fullmatch(r'hats (catalog|collection)', title.strip(), re.I):
        title = ''
    title = re.sub(r'\s*-\s*HATS Catalog$', '', title).strip()
    parts = [title or props.get('obs_collection', ''), props.get('bib_reference', '')]
    where = props.get('bib_reference_url') or props.get('obs_copyright_url') or ''
    if where.startswith('http'):
        parts.append('Source:  ' + where)
    return '.  '.join(p.strip().rstrip('.') for p in parts if p.strip())[:500]


def read_input(con, path, types=None, hats=None):
    """(DuckDB relation, {column: metadata the input itself carries}) for the input table."""

    if hats:
        # The files by name, not by a pattern:  dataset/ also holds _metadata.
        # Their paths say Norder=N/Dir=D, which DuckDB would otherwise add as columns.
        listed = ', '.join("'" + f.replace("'", "''") + "'" for f in hats['files'])
        rel = con.sql(f'SELECT * FROM read_parquet([{listed}], hive_partitioning=false, union_by_name=true)')
        index = hats['properties'].get('hats_col_healpix', '')
        keep = [c for c in rel.columns if c not in HATS_COLUMNS and c != index]
        common = os.path.join(hats['root'], 'dataset', '_common_metadata')
        carried = field_metadata(common if os.path.isfile(common) else hats['files'][0]) or field_metadata(hats['files'][0])
        carried.update(parquet_metadata(con, hats['files'][0]))          # the IVOA note's metadata wins, if there is any
        return rel.project(', '.join('"' + c.replace('"', '""') + '"' for c in keep)), carried

    # A folder of files arrives as a pattern, "dir/*.csv*"; judge it by the name before the star.
    low = path.lower().rstrip('*')

    if low.endswith(('.parquet', '.pq')) or '.parquet' in low:
        first = sorted(glob.glob(path))[:1] if '*' in path else [path]
        carried = field_metadata(first[0]) if first and os.path.isfile(first[0]) else {}
        carried.update(parquet_metadata(con, path))
        return con.sql(f"SELECT * FROM read_parquet('{path}')"), carried

    if low.endswith(('.csv', '.csv.gz', '.tsv', '.txt')):
        if types:
            # Only for columns the file has; read_csv refuses unknown names.
            have = con.sql(f"SELECT * FROM read_csv('{path}', all_varchar=true) LIMIT 0").columns
            decl = ', '.join(f"'{c}': '{t}'" for c, t in types.items() if c in have)
            return con.sql(f"SELECT * FROM read_csv('{path}', sample_size=-1, types={{{decl}}})"), {}
        return con.sql(f"SELECT * FROM read_csv('{path}', sample_size=-1)"), {}

    # FITS tables and VOTables go through astropy.
    from astropy.table import Table

    table = Table.read(path)

    def column(col):
        """A pyarrow array for an astropy column: native byte order, text as text, blanks as nulls."""
        mask = np.asarray(col.mask) if hasattr(col, 'mask') and np.ndim(col.mask) else None
        data = np.asarray(col.filled() if hasattr(col, 'filled') else col)
        if data.ndim != 1:
            sys.exit(f'Column "{col.name}" holds arrays; only one value per row is supported.')
        if data.dtype.kind == 'S':
            data = np.char.decode(data, 'utf-8', 'replace')
        if data.dtype.kind == 'U':
            data = np.char.rstrip(data).astype(object)
        elif not data.dtype.isnative:                      # FITS is big-endian
            data = data.astype(data.dtype.newbyteorder('='))
        if data.dtype.kind == 'f':
            if mask is not None:
                data = np.where(mask, np.nan, data)
            mask = np.isnan(data)                          # a missing number is a null, as in CSV input
        return pa.array(data, mask=mask if mask is not None and mask.any() else None)

    arrow = pa.table({name: column(table[name]) for name in table.colnames})
    con.register('astropy_input', arrow)

    carried = {}
    for name in table.colnames:
        col = table[name]
        found = {'unit': str(col.unit) if col.unit is not None else '',
                 'description': col.description or '',
                 'ucd': (col.meta or {}).get('ucd', '') or ''}
        carried[name] = {k: v for k, v in found.items() if v}
    return con.sql('SELECT * FROM astropy_input'), carried


def parquet_metadata(con, path):
    """Column metadata already in Parquet input that follows the IVOA note."""

    try:
        row = con.execute('SELECT value FROM parquet_kv_metadata(?) WHERE key = ? LIMIT 1',
                          [path, votable_metadata.CONTENT_KEY.encode()]).fetchone()
    except duckdb.Error:
        return {}
    return votable_metadata.parse(bytes(row[0]).decode('utf-8', 'replace'))[1] if row else {}


# Words ADQL keeps for itself (SQL-92's, ADQL's functions and geometries, and
# the additions of ADQL 2.1), as TOPCAT's validator lists them.  A column
# with one of these names can be asked for only in double quotes.  "dec" is
# not among them:  ADQL leaves it out so that it can be a column.
ADQL_RESERVED = frozenset("""
absolute action add all allocate alter and any are as asc assertion at authorization avg begin between bit
bit_length both by cascade cascaded case cast catalog char character character_length char_length check close
coalesce collate collation column commit connect connection constraint constraints continue convert
corresponding count create cross current current_date current_time current_timestamp current_user cursor date
day deallocate decimal declare default deferrable deferred delete desc describe descriptor diagnostics
disconnect distinct domain double drop else end escape except exception exec execute exists external extract
false fetch first float for foreign found from full get global go goto grant group having hour identity
immediate in indicator initially inner input insensitive insert int integer intersect interval into is
isolation join key language last leading left level like local lower match max min minute module month names
national natural nchar next no not null nullif numeric octet_length of on only open option or order outer
output overlaps pad partial position precision prepare preserve primary prior privileges procedure public read
real references relative restrict revoke right rollback rows schema scroll second section select session
session_user set size smallint some space sql sqlcode sqlerror sqlstate substring sum system_user table
temporary then time timestamp timezone_hour timezone_minute to trailing transaction translate translation trim
true union unique unknown update upper usage user using value values varchar varying view when whenever where
with work write year zone abs acos asin atan atan2 ceiling cos degrees exp floor log log10 mod pi power radians
rand round sin sqrt tan top truncate area box centroid circle contains coord1 coord2 coordsys distance
intersects point polygon region cot bigint ilike in_unit offset
""".split())


def clean(name):
    """
    A column name anyone can type in ADQL without quotes:  lower case,
    letters, digits and "_", starting with a letter, and not a word ADQL
    keeps for itself.  "2MASS" becomes "c_2mass", "_planets" "planets",
    "**" "col" and "size" "size_".
    """
    name = re.sub(r'[^0-9a-z_]', '_', name.strip().lower()).lstrip('_')
    if not name:
        name = 'col'
    elif name[0].isdigit():
        name = 'c_' + name
    return name + '_' if name in ADQL_RESERVED else name


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('input')
    p.add_argument('output_dir')
    p.add_argument('--ra', default='ra', help='input column holding right ascension in degrees')
    p.add_argument('--dec', default='dec', help='input column holding declination in degrees')
    p.add_argument('--schema', metavar='FILE',
                   help='CSV of column name and type, to use instead of guessing types from the values; '
                        'may also give unit, ucd, utype and description')
    p.add_argument('--description', default='', help='what the table is, shown in the table listing')
    p.add_argument('--rows-per-file', type=int, default=5_000_000)
    p.add_argument('--row-group', type=int, default=None,
                   help='rows in a row group (default: by the table\'s width; see row_group_size)')
    p.add_argument('--work', metavar='DIR',
                   help='keep the working tables on disk in DIR, for a table too large for memory')
    p.add_argument('--memory', metavar='SIZE', help="most memory DuckDB may use, e.g. 8GB (default: DuckDB's own)")
    args = p.parse_args()

    # By default the whole table is held in memory, which is quickest and
    # fine for a few million rows.  --work puts the working tables in a
    # database file there instead, and --memory bounds what DuckDB keeps in
    # memory; with both, the size of the table is limited by the disk.
    # DuckDB reads a machine's memory, not a container's allowance, so in a
    # container --memory is what keeps the conversion from being killed.
    if args.work:
        os.makedirs(args.work, exist_ok=True)
        scratch = os.path.join(args.work, f'convert-{os.getpid()}.duckdb')
        con = duckdb.connect(scratch)
        con.execute(f"SET temp_directory = '{os.path.join(args.work, 'spill')}'")
    else:
        scratch = None
        con = duckdb.connect()
    if args.memory:
        con.execute(f"SET memory_limit = '{args.memory}'")
    con.execute('SET preserve_insertion_order = true')
    temp = '' if scratch else 'TEMP'       # a table in the database file, or in memory

    types, declared = read_schema(args.schema) if args.schema else ({}, {})
    hats = None
    if os.path.isdir(args.input):
        hats = hats_catalog(args.input)
        if not hats:
            sys.exit('The input is a directory, and not a HATS catalog:  it has no properties file with a '
                     'dataset/ folder of Parquet files beside it.  For a folder of CSV or Parquet files that '
                     'are one table, give a pattern such as "dir/*.parquet".')
    rel, carried = read_input(con, args.input, types, hats)  # noqa: F841  (rel is used by name in SQL)

    if hats:
        # The catalog says which columns hold its positions.  A name given
        # on the command line wins if the catalog has such a column.
        props = hats['properties']
        for arg, key in (('ra', 'hats_col_ra'), ('dec', 'hats_col_dec')):
            if getattr(args, arg) not in rel.columns and props.get(key) in rel.columns:
                setattr(args, arg, props[key])
        if not args.description:
            args.description = hats_description(props)
        said = props.get('hats_nrows', '')
        print(f"HATS catalog {props.get('obs_collection') or os.path.basename(hats['root'])}:  "
              f"{len(hats['files'])} data files" + (f", {int(said):,} rows by its properties" if said.isdigit() else '')
              + f";  positions in {args.ra}, {args.dec}.  Its own partitions and index are not kept.")

    # Output names: cleaned, positions renamed, no collisions.  Two columns
    # that clean to one name ("Planets" and "#Planets") are told apart by a
    # number, the later one getting it.
    names = {}
    taken = {'ra', 'dec'}
    for col in rel.columns:
        if col == args.ra:
            new = 'ra'
        elif col == args.dec:
            new = 'dec'
        else:
            new = clean(col)
            if new in ('ra', 'dec', 'hpx29', 'aiab_x', 'aiab_y', 'aiab_z'):
                # The aiab_ three are added by the service's view; the rest are ours.
                new = new + '_in'
            base, n = new, 1
            while new in taken:
                n += 1
                new = f'{base}_{n}'
            taken.add(new)
        names[col] = new

    for need, given in (('ra', args.ra), ('dec', args.dec)):
        if given not in rel.columns:
            sys.exit(f'No column "{given}" in the input for {need}; name it with --{need}.  '
                     f'Columns: {", ".join(rel.columns)}')

    select = ', '.join((f'CAST("{old}" AS {types[old]})' if old in types else f'"{old}"') + f' AS "{new}"'
                       for old, new in names.items())
    con.execute(f'CREATE {temp} TABLE src AS SELECT {select} FROM rel '
                'WHERE "{0}" IS NOT NULL AND "{1}" IS NOT NULL'.format(args.ra, args.dec))

    total_in = con.execute('SELECT count(*) FROM rel').fetchone()[0]
    if hats and hats['properties'].get('hats_nrows', '').isdigit() and int(hats['properties']['hats_nrows']) != total_in:
        print(f"NOTE:  the catalog's properties say {int(hats['properties']['hats_nrows']):,} rows;  its files hold {total_in:,}.")
    kept, lo, hi = con.execute('SELECT count(*), min(dec), max(dec) FROM src').fetchone()

    if kept == 0:
        sys.exit('No rows with a position.')
    if lo < -90 or hi > 90:
        sys.exit(f'Declination runs from {lo} to {hi}; it must be in degrees.')

    # The index is computed in numpy, a slice at a time, and joined back by row id.
    con.execute(f'CREATE {temp} TABLE idx (rid BIGINT, hpx29 BIGINT)')
    nside = 2 ** ORDER
    step = 5_000_000
    for start in range(0, kept, step):
        pos = con.sql(f'SELECT rowid, ra, dec FROM src WHERE rowid >= {start} AND rowid < {start + step}').to_arrow_table()
        hpx = lonlat_to_healpix(pos['ra'].to_numpy() * u.deg, pos['dec'].to_numpy() * u.deg,
                                nside, order='nested')
        chunk = pa.table({'rid': pos['rowid'], 'hpx29': pa.array(np.asarray(hpx, dtype=np.int64))})  # noqa: F841
        con.execute('INSERT INTO idx SELECT * FROM chunk')

    os.makedirs(args.output_dir, exist_ok=True)

    parts = max(1, -(-kept // args.rows_per_file))
    # Each row's place in the output.  Only the index is sorted as a whole,
    # three numbers a row; the rows themselves are brought together a file
    # at a time below, so memory is set by --rows-per-file, not by the table.
    con.execute(f'CREATE {temp} TABLE place AS SELECT rid, hpx29, '
                'row_number() OVER (ORDER BY hpx29, rid) - 1 AS _n FROM idx')
    con.execute('DROP TABLE idx')

    # What the columns mean, under their output names: what the input
    # carried, then what --schema says, which wins.
    meta = {}
    for old, new in names.items():
        meta[new] = {**carried.get(old, {}), **declared.get(old, {})}
    for pos in ('ra', 'dec'):
        meta[pos].setdefault('unit', 'deg')
    meta['hpx29'] = {'description': 'Nested HEALPix index of (ra, dec) at order 29, added on conversion; '
                                    'the files are sorted by it'}
    out_cols = [(r[0], r[1]) for r in con.execute('DESCRIBE SELECT src.*, place.hpx29 FROM src JOIN place ON src.rowid = place.rid').fetchall()]
    text_cols = [c for c, t in out_cols if t.upper() == 'VARCHAR']
    if text_cols:
        widths = con.execute('SELECT ' + ', '.join(f'max(length("{c}"))' for c in text_cols) + ' FROM src').fetchone()
        for c, w in zip(text_cols, widths):
            meta.setdefault(c, {})['width'] = max(1, w or 1)
    votable = votable_metadata.build([(c, t, meta.get(c, {})) for c, t in out_cols], args.description)
    kv = (f"KV_METADATA {{'{votable_metadata.VERSION_KEY}': '1.0', "
          f"'{votable_metadata.CONTENT_KEY}': '{votable.replace(chr(39), chr(39) * 2)}'}}")

    group = args.row_group or row_group_size(len(out_cols))

    for part in range(parts):
        out = os.path.join(args.output_dir, f'part-{part:02d}.parquet')
        a, b = part * args.rows_per_file, (part + 1) * args.rows_per_file
        con.execute(f"""COPY (SELECT src.*, place.hpx29 FROM src JOIN place ON src.rowid = place.rid
                              WHERE place._n >= {a} AND place._n < {b} ORDER BY place._n)
                        TO '{out}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE {group}, {kv})""")

    if scratch:
        con.close()
        for leftover in (scratch, scratch + '.wal'):
            if os.path.exists(leftover):
                os.remove(leftover)

    print(f'{kept:,} rows in {parts} file(s) under {args.output_dir}'
          + (f'  ({total_in - kept:,} rows without a position dropped)' if total_in != kept else ''))
    renamed = {o: n for o, n in names.items() if o != n}
    if renamed:
        print('renamed: ' + ', '.join(f'{o} -> {n}' for o, n in renamed.items()))
    described = sum(1 for c in names.values() if meta[c].get('description'))
    with_unit = sum(1 for c in names.values() if meta[c].get('unit'))
    print(f'{described} of {len(names)} columns have a description, {with_unit} a unit')


if __name__ == '__main__':
    main()
