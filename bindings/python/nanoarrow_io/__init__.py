"""nanoarrow-io: fast zero-copy Arrow I/O for Parquet and Lance."""

from __future__ import annotations

from nanoarrow_io import lance as lance
from nanoarrow_io import parquet as parquet

__all__ = ["parquet", "lance"]
