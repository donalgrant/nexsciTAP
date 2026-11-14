"""tools/make_parquet_catalog.py: any table of positions to the served layout."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).parent.parent


def test_csv_becomes_sorted_indexed_parquet(tmp_path):
    duckdb = pytest.importorskip('duckdb')
    pytest.importorskip('astropy_healpix')
    import astropy.units as u
    from astropy_healpix import lonlat_to_healpix

    src = tmp_path / 'in.csv'
    src.write_text('Name,RA (deg),DEJ2000,x,Flux-1\n'
                   'a,359.9,-0.1,1,10\n'
                   'b,0.1,0.1,2,20\n'
                   'c,180.0,89.9,3,30\n'
                   'd,,5.0,4,40\n'
                   'e,45.0,-45.0,5,50\n')
    out = tmp_path / 'out'

    run = subprocess.run([sys.executable, str(REPO / 'tools' / 'make_parquet_catalog.py'), str(src), str(out),
                          '--ra', 'RA (deg)', '--dec', 'DEJ2000', '--rows-per-file', '3', '--row-group', '2'],
                         capture_output=True, text=True)
    assert run.returncode == 0, run.stderr
    assert sorted(p.name for p in out.iterdir()) == ['part-00.parquet', 'part-01.parquet']

    con = duckdb.connect()
    rows = con.execute(f"SELECT name, ra, dec, x_in, flux_1, hpx29 FROM read_parquet('{out}/*.parquet', "
                       "filename=true, file_row_number=true) ORDER BY filename, file_row_number").fetchall()

    # The row without a position is gone; names are plain identifiers.
    assert sorted(r[0] for r in rows) == ['a', 'b', 'c', 'e']

    # Sorted by the index across files, and the index is the order-29 nested pixel.
    assert [r[5] for r in rows] == sorted(r[5] for r in rows)
    for _, ra, dec, _, _, hpx in rows:
        assert hpx == int(lonlat_to_healpix(ra * u.deg, dec * u.deg, 2 ** 29, order='nested'))


def test_missing_position_column_is_reported(tmp_path):
    pytest.importorskip('duckdb')
    pytest.importorskip('astropy_healpix')

    src = tmp_path / 'in.csv'
    src.write_text('alpha,delta\n1.0,2.0\n')

    run = subprocess.run([sys.executable, str(REPO / 'tools' / 'make_parquet_catalog.py'), str(src), str(tmp_path / 'o')],
                         capture_output=True, text=True)
    assert run.returncode != 0
    assert 'No column "ra"' in run.stderr
