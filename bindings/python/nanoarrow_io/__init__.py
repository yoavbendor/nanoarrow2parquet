"""nanoarrow-io: fast zero-copy Arrow I/O for Parquet and Lance."""

from __future__ import annotations

from nanoarrow_io import nanolance as nanolance
from nanoarrow_io import parquet as parquet

__all__ = ["parquet", "nanolance"]
