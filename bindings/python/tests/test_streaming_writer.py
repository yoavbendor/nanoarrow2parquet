"""Tests for streaming ParquetWriter (chunked, larger-than-RAM writes)."""

from __future__ import annotations

import gc

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nanoarrow_io import parquet

psutil = pytest.importorskip("psutil")
PROC = psutil.Process()


def _rss_mb() -> float:
    return PROC.memory_info().rss / (1024 * 1024)


def test_streaming_writer_multiple_row_groups(sample_table, tmp_path):
    path = tmp_path / "stream.parquet"
    d = sample_table.to_pydict()
    batches = [
        pa.record_batch({k: v[:2] for k, v in d.items()}, schema=sample_table.schema),
        pa.record_batch({k: v[2:4] for k, v in d.items()}, schema=sample_table.schema),
        pa.record_batch({k: v[4:] for k, v in d.items()}, schema=sample_table.schema),
    ]

    with parquet.ParquetWriter(path, codec="zstd") as writer:
        for batch in batches:
            writer.write_batch(batch)

    assert writer.closed
    assert writer.row_groups == len(batches)
    assert writer.num_rows == sample_table.num_rows

    md = pq.ParquetFile(path).metadata
    assert md.num_row_groups == len(batches)
    assert pq.read_table(path).equals(sample_table)


def test_streaming_writer_write_table(sample_table, tmp_path):
    path = tmp_path / "stream_table.parquet"
    with parquet.ParquetWriter(path) as writer:
        writer.write_table(sample_table)

    assert writer.row_groups == len(sample_table.to_batches())
    assert pq.read_table(path).equals(sample_table)


def test_streaming_writer_context_manager_closes(tmp_path):
    path = tmp_path / "one.parquet"
    batch = pa.record_batch({"x": [1, 2, 3]}, schema=pa.schema([("x", pa.int64())]))
    with parquet.ParquetWriter(path) as writer:
        writer.write_batch(batch)
    assert writer.closed
    assert pq.read_table(path).column("x").to_pylist() == [1, 2, 3]


def test_streaming_writer_requires_batch_before_close(tmp_path):
    path = tmp_path / "empty.parquet"
    writer = parquet.ParquetWriter(path)
    with pytest.raises(RuntimeError, match="at least one batch"):
        writer.close()


def test_streaming_writer_rejects_after_close(sample_table, tmp_path):
    path = tmp_path / "closed.parquet"
    writer = parquet.ParquetWriter(path)
    writer.write_batch(sample_table.to_batches()[0])
    writer.close()
    with pytest.raises(RuntimeError, match="closed"):
        writer.write_batch(sample_table.to_batches()[0])


def test_streaming_larger_than_chunk_rss_bounded(tmp_path):
    """Many chunks streamed sequentially; peak RSS should stay near one chunk."""
    chunk_rows = 50_000
    n_chunks = 40
    schema = pa.schema([("id", pa.int64()), ("value", pa.float64())])

    baseline = _rss_mb()
    path = tmp_path / "big_stream.parquet"
    with parquet.ParquetWriter(path, codec="zstd") as writer:
        for i in range(n_chunks):
            start = i * chunk_rows
            batch = pa.record_batch(
                {
                    "id": pa.array(range(start, start + chunk_rows), type=pa.int64()),
                    "value": pa.array([float(j) * 0.01 for j in range(chunk_rows)], type=pa.float64()),
                },
                schema=schema,
            )
            writer.write_batch(batch)
            del batch
            if i % 10 == 0:
                gc.collect()

    gc.collect()
    growth = _rss_mb() - baseline
    md = pq.ParquetFile(path).metadata
    assert md.num_row_groups == n_chunks
    assert md.num_rows == chunk_rows * n_chunks
    assert growth < 180, f"RSS grew by {growth:.1f} MB during streaming write"
