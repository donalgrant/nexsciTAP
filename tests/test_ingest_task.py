"""The checks an ingest makes on a converted table before it may be published (tools/ingest_task.py)."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

TOOLS = Path(__file__).resolve().parent.parent / "tools"

duckdb = pytest.importorskip("duckdb")
pytest.importorskip("astropy_healpix")
np = pytest.importorskip("numpy")

sys.path.insert(0, str(TOOLS))
import ingest_task  # noqa: E402


@pytest.fixture(scope="module")
def converted(tmp_path_factory) -> Path:
    """A small all-sky table, converted as an ingest would."""
    work = tmp_path_factory.mktemp("ingest")
    rng = np.random.default_rng(7)
    n = 40000
    ra = rng.uniform(0, 360, n)
    dec = np.degrees(np.arcsin(rng.uniform(-1, 1, n)))
    with open(work / "in.csv", "w") as f:
        f.write("id,ra,dec,mag\n")
        for i in range(n):
            f.write(f"{i},{ra[i]:.7f},{dec[i]:.7f},{rng.uniform(5, 20):.3f}\n")
        f.write(",,,\n")                                   # a row with no position
    subprocess.run([sys.executable, str(TOOLS / "make_parquet_catalog.py"), str(work / "in.csv"), str(work / "out"),
                    "--rows-per-file", "15000"], check=True, capture_output=True)
    return work / "out"


def test_a_good_table_passes_and_names_a_cone_to_check_later(converted: Path):
    report = {"notes": []}
    ingest_task.check(str(converted), report)
    test = report["searchTest"]
    assert report["rows"] == 40000 and len(report["files"]) == 3
    assert test["sorted"] and test["indexWrong"] == 0 and test["cones"] == 25
    assert test["rowsFound"] >= 25                          # each cone is centred on a row
    assert report["checkCone"]["count"] >= 1


def damaged(converted: Path, tmp_path: Path, select: str) -> str:
    con = duckdb.connect()
    for f in sorted(converted.glob("*.parquet")):
        con.execute(f"COPY ({select.format(src=f)}) TO '{tmp_path / f.name}' (FORMAT parquet)")
    return str(tmp_path)


@pytest.mark.parametrize("select, says", [
    ("SELECT * FROM read_parquet('{src}') ORDER BY mag", "not in index order"),
    ("SELECT * REPLACE (CASE WHEN id % 50 = 0 THEN hpx29 + 4096 ELSE hpx29 END AS hpx29) "
     "FROM read_parquet('{src}') ORDER BY hpx29", "does not match the positions"),
    ("SELECT * REPLACE (CASE WHEN id % 50 = 0 THEN (ra + 40) % 360 ELSE ra END AS ra) "
     "FROM read_parquet('{src}')", "does not match the positions"),
])
def test_a_table_that_would_search_wrongly_is_refused(converted: Path, tmp_path: Path, select: str, says: str):
    with pytest.raises(RuntimeError, match=says):
        ingest_task.check(damaged(converted, tmp_path, select), {"notes": []})


def test_cone_search_through_the_index_is_compared_with_every_row(converted: Path, tmp_path: Path, monkeypatch):
    """With the sample check out of the way, a wrong index still shows as a cone that comes up short."""
    bad = damaged(converted, tmp_path,
                  "SELECT * REPLACE (hpx29 + (1::BIGINT << 52) AS hpx29) FROM read_parquet('{src}')")
    import astropy_healpix
    real = astropy_healpix.lonlat_to_healpix
    monkeypatch.setattr(astropy_healpix, "lonlat_to_healpix",
                        lambda *a, **k: real(*a, **k) + (1 << 52) if a[2] == 2 ** 29 else real(*a, **k))
    with pytest.raises(RuntimeError, match="through the index"):
        ingest_task.check(bad, {"notes": []})


def test_the_cover_of_a_cone_holds_every_point_in_it():
    from astropy import units as u
    from astropy_healpix import lonlat_to_healpix
    rng = np.random.default_rng(3)
    for lon, lat in [(10.0, 89.95), (200.0, -89.9), (0.001, 0.0), (45.0, 41.8), (123.4, -41.8), (359.999, 20.0)]:
        for radius in (5 / 3600, 0.05, 1.0, 4.0):
            order, cells = ingest_task.cone_cover(lon, lat, radius)
            bearing, dist = rng.uniform(0, 2 * np.pi, 400), np.radians(radius * np.sqrt(rng.uniform(0, 1, 400)))
            a0, b0 = np.radians(lon), np.radians(lat)
            b1 = np.arcsin(np.sin(b0) * np.cos(dist) + np.cos(b0) * np.sin(dist) * np.cos(bearing))
            a1 = a0 + np.arctan2(np.sin(bearing) * np.sin(dist) * np.cos(b0), np.cos(dist) - np.sin(b0) * np.sin(b1))
            inside = lonlat_to_healpix((np.degrees(a1) % 360) * u.deg, np.degrees(b1) * u.deg, 2 ** order, order="nested")
            assert set(inside.tolist()) <= set(cells), (lon, lat, radius)
