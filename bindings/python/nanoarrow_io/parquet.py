"""Parquet writer bindings backed by nanoarrow2parquet."""

from __future__ import annotations

import os
from pathlib import Path
from types import TracebackType
from typing import Literal, Union

from nanoarrow_io import _n2p

Codec = Literal["zstd", "uncompressed"]


def write_table(
    table,
    path: Union[str, os.PathLike],
    *,
    codec: Codec = "zstd",
) -> None:
    """Write an Arrow table (pyarrow/polars/nanom export) to Parquet.

    Each record batch becomes one row group. The input is imported via the Arrow
    PyCapsule protocol without copying column buffers.

    For datasets larger than RAM, prefer :class:`ParquetWriter` and call
    :meth:`ParquetWriter.write_batch` in a loop so only one chunk is resident at
    a time.
    """
    _n2p.write_table(table, Path(path), codec)


def write_batch(
    batch,
    path: Union[str, os.PathLike],
    *,
    codec: Codec = "zstd",
) -> None:
    """Write a single Arrow RecordBatch to a one-row-group Parquet file."""
    _n2p.write_batch(batch, Path(path), codec)


class ParquetWriter:
    """Streaming Parquet writer: one row group per :meth:`write_batch` call.

    Open a file, append batches in a Python loop, then :meth:`close` (or use as a
    context manager) to write the footer and produce a valid multi-row-group file.
  Only one chunk needs to live in memory at a time — suitable for captures larger
    than RAM.

    Example::

        import pyarrow as pa
        from nanoarrow_io import parquet

        schema = pa.schema([("id", pa.int64()), ("value", pa.float64())])
        with parquet.ParquetWriter("big.parquet", codec="zstd") as writer:
            for i in range(1000):
                batch = pa.record_batch(
                    {
                        "id": pa.array(range(i * 10_000, (i + 1) * 10_000)),
                        "value": pa.array([float(j) for j in range(10_000)]),
                    },
                    schema=schema,
                )
                writer.write_batch(batch)
    """

    def __init__(self, path: Union[str, os.PathLike], *, codec: Codec = "zstd") -> None:
        self._writer = _n2p.ParquetWriter(Path(path), codec)

    @property
    def closed(self) -> bool:
        return self._writer.closed

    @property
    def row_groups(self) -> int:
        return self._writer.row_groups

    @property
    def num_rows(self) -> int:
        return self._writer.num_rows

    def write_batch(self, batch) -> None:
        """Append one Arrow record batch as a row group."""
        self._writer.write_batch(batch)

    def write_table(self, table) -> None:
        """Append every record batch from an Arrow table or stream."""
        import pyarrow as pa

        if isinstance(table, pa.RecordBatch):
            self.write_batch(table)
            return
        if isinstance(table, pa.Table):
            for batch in table.to_batches():
                self.write_batch(batch)
            return
        if hasattr(table, "to_batches"):
            for batch in table.to_batches():
                self.write_batch(batch)
            return
        raise TypeError("expected pyarrow Table/RecordBatch or Arrow-exportable table")

    def close(self) -> None:
        """Write the Parquet footer and close the file."""
        self._writer.close()

    def __enter__(self) -> ParquetWriter:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if exc_type is None:
            self.close()


__all__ = ["ParquetWriter", "write_table", "write_batch", "Codec"]
