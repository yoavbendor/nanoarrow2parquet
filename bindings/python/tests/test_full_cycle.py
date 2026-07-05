"""Full Python -> nanolib -> Python cycles for Parquet and Lance."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nanoarrow_io import lance, parquet


@pytest.mark.parametrize("codec", ["zstd", "uncompressed"])
def test_parquet_full_cycle(sample_table, tmp_path, codec):
    path = tmp_path / f"cycle_{codec}.parquet"

    # Python -> nanoarrow2parquet
    parquet.write_table(sample_table, path, codec=codec)

    # standard reader -> Python
    via_pq = pq.read_table(path)
    assert via_pq.equals(sample_table)

    # round-trip again through pyarrow writer for sanity
    path2 = tmp_path / f"cycle2_{codec}.parquet"
    pq.write_table(via_pq, path2)
    parquet.write_table(via_pq, tmp_path / f"cycle3_{codec}.parquet")
    assert pq.read_table(tmp_path / f"cycle3_{codec}.parquet").equals(sample_table)


def test_lance_full_cycle(sample_table, tmp_path):
    path = tmp_path / "cycle.lance"

    lance.write_table(sample_table, path, compression=True)
    native = pa.table(lance.read_table(path))
    assert native.equals(sample_table)

    lance_pkg = pytest.importorskip("lance")
    if not hasattr(lance_pkg, "dataset"):
        pytest.skip("lance-format package not installed")
    stock = lance_pkg.dataset(str(path)).to_table()
    assert stock.equals(sample_table)

    # write stock table back through nanolance reader/writer
    path2 = tmp_path / "cycle2.lance"
    lance.write_table(stock, path2, compression=True)
    again = pa.table(lance.read_table(path2))
    assert again.equals(sample_table)
