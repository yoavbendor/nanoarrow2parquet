"""nanoarrow-io: fast zero-copy Arrow I/O for Parquet (plus NumPy bridge)."""

from __future__ import annotations

from nanoarrow_io import numpy as numpy
from nanoarrow_io import parquet as parquet

__all__ = ["parquet", "numpy"]
