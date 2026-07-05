"""Zero-copy NumPy column export for nanoarrow-io writers.

Builds an Arrow struct record batch that borrows numpy column buffers (no copy on
the Python/native boundary). Numeric dtypes supported: bool, signed/unsigned
integers (1/2/4/8 bytes), float32/float64. Columns must be one-dimensional and
C-contiguous.
"""

from __future__ import annotations

from typing import Mapping, Union

import numpy as np

from nanoarrow_io import _numpy

ArrayLike = Union[np.ndarray, "np.ma.MaskedArray"]
Columns = Mapping[str, ArrayLike]


def record_batch(columns: Columns) -> _numpy.RecordBatch:
    """Wrap a column dict of numpy arrays as an Arrow-exportable record batch."""
    return _numpy.RecordBatch(dict(columns))


__all__ = ["record_batch"]
