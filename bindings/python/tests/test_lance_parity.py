"""Lance binding parity and full-cycle tests."""

from __future__ import annotations

import pyarrow as pa
import pytest

from nanoarrow_io import lance


def test_lance_write_read_roundtrip(sample_table, tmp_path):
    path = tmp_path / "sample.lance"
    lance.write_table(sample_table, path, compression=True)
    back = pa.table(lance.read_table(path))
    assert back.equals(sample_table)


def test_lance_nullable_roundtrip(nullable_table, tmp_path):
    path = tmp_path / "nulls.lance"
    table = nullable_table.drop(["nul"])
    lance.write_table(
        table,
        path,
        options=lance.WriteOptions(ignore_nullability=True, compression=True),
    )
    back = pa.table(lance.read_table(path))
    # With ignore_nullability, only the required ctrl column preserves exact null semantics.
    assert back.column("ctrl").equals(table.column("ctrl"))
    assert back.num_rows == table.num_rows


def test_lance_stock_reader(sample_table, tmp_path):
    """Files must be readable by the official lance Python package when installed."""
    lance_pkg = pytest.importorskip("lance")
    if not hasattr(lance_pkg, "dataset"):
        pytest.skip("lance-format package not installed (PyPI name collision with unrelated 'lance')")
    path = tmp_path / "stock.lance"
    lance.write_table(sample_table, path, compression=True)

    ds = lance_pkg.dataset(str(path))
    back = ds.to_table()
    assert back.equals(sample_table)


def test_lance_polars_cycle(sample_table, tmp_path):
    polars = pytest.importorskip("polars")
    path = tmp_path / "pl.lance"
    lance.write_table(sample_table, path, compression=True)
    exported = lance.read_table(path)
    pl_back = polars.from_arrow(pa.table(exported))
    pl_orig = polars.from_arrow(sample_table)
    assert pl_back.equals(pl_orig)
