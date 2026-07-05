"""Parquet binding parity vs pyarrow writer/reader."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nanoarrow_io import parquet


def test_write_table_roundtrip(sample_table, tmp_path):
    path = tmp_path / "sample.parquet"
    parquet.write_table(sample_table, path)
    back = pq.read_table(path)
    assert back.equals(sample_table)


def test_write_batch_roundtrip(sample_table, tmp_path):
    path = tmp_path / "batch.parquet"
    batch = sample_table.to_batches()[0]
    parquet.write_batch(batch, path)
    back = pq.read_table(path)
    assert back.to_pydict() == batch.to_pydict()


def test_streaming_row_groups(sample_table, tmp_path):
    path = tmp_path / "stream.parquet"
    d = sample_table.to_pydict()
    batch_a = pa.table({k: v[:2] for k, v in d.items()}, schema=sample_table.schema)
    batch_b = pa.table({k: v[2:4] for k, v in d.items()}, schema=sample_table.schema)
    batch_c = pa.table({k: v[4:] for k, v in d.items()}, schema=sample_table.schema)
    batches = [
        batch_a.to_batches()[0],
        batch_b.to_batches()[0],
        batch_c.to_batches()[0],
    ]
    reader = pa.RecordBatchReader.from_batches(sample_table.schema, batches)
    parquet.write_table(reader, path)
    md = pq.ParquetFile(path).metadata
    assert md.num_row_groups == 3
    assert pq.read_table(path).equals(sample_table)


def test_nullable_roundtrip(nullable_table, tmp_path):
    path = tmp_path / "nulls.parquet"
    parquet.write_table(nullable_table, path)
    back = pq.read_table(path)
    # Arrow null-type columns are materialized as typed all-null columns in Parquet.
    assert back.drop(["nul"]).equals(nullable_table.drop(["nul"]))
    assert back.column("nul").null_count == nullable_table.num_rows


def test_pandas_and_polars_read(sample_table, tmp_path):
    path = tmp_path / "cross.parquet"
    parquet.write_table(sample_table, path)

    import pandas as pd

    pdf = pd.read_parquet(path)
    assert pdf.shape == (sample_table.num_rows, sample_table.num_columns)

    polars = pytest.importorskip("polars")
    pl_df = polars.read_parquet(path)
    assert pl_df.shape == (sample_table.num_rows, sample_table.num_columns)


def test_codec_uncompressed(sample_table, tmp_path):
    path = tmp_path / "raw.parquet"
    parquet.write_table(sample_table, path, codec="uncompressed")
    md = pq.ParquetFile(path).metadata
    for i in range(md.num_row_groups):
        rg = md.row_group(i)
        for j in range(rg.num_columns):
            assert rg.column(j).compression == "UNCOMPRESSED"


def test_zstd_compression(sample_table, tmp_path):
    path = tmp_path / "zstd.parquet"
    parquet.write_table(sample_table, path, codec="zstd")
    md = pq.ParquetFile(path).metadata
    col = md.row_group(0).column(0)
    assert col.compression == "ZSTD"
