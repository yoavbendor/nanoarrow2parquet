"""Full Python -> nanolib -> Python cycles for Parquet."""

from __future__ import annotations

import pyarrow.parquet as pq
import pytest

from nanoarrow_io import parquet


@pytest.mark.parametrize("codec", ["zstd", "uncompressed"])
def test_parquet_full_cycle(sample_table, tmp_path, codec):
    path = tmp_path / f"cycle_{codec}.parquet"

    parquet.write_table(sample_table, path, codec=codec)
    via_pq = pq.read_table(path)
    assert via_pq.equals(sample_table)

    path2 = tmp_path / f"cycle2_{codec}.parquet"
    pq.write_table(via_pq, path2)
    parquet.write_table(via_pq, tmp_path / f"cycle3_{codec}.parquet")
    assert pq.read_table(tmp_path / f"cycle3_{codec}.parquet").equals(sample_table)
