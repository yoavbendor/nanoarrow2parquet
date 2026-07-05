"""Parquet writer bindings backed by nanoarrow2parquet."""

from __future__ import annotations

import os
from pathlib import Path
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


__all__ = ["write_table", "write_batch", "Codec"]
