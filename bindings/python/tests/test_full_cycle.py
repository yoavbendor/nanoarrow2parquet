"""Full Python -> nanolib -> Python cycles for Parquet and Lance."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nanoarrow_io import nanolance, parquet
from tests.support import require_pylance


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


def test_lance_full_cycle(sample_table, pylance_interop_table, tmp_path):
    path = tmp_path / "cycle.lance"

    nanolance.write_table(sample_table, path, compression=True)
    native = pa.table(nanolance.read_table(path))
    assert native.equals(sample_table)

    lance = require_pylance()
    interop_path = tmp_path / "interop.lance"
    nanolance.write_table(pylance_interop_table, interop_path, compression=False)
    stock = lance.dataset(str(interop_path)).to_table()
    assert stock.to_pydict() == pylance_interop_table.to_pydict()

    path2 = tmp_path / "cycle2.lance"
    nanolance.write_table(stock, path2, compression=False)
    again = pa.table(nanolance.read_table(path2))
    assert again.to_pydict() == pylance_interop_table.to_pydict()
