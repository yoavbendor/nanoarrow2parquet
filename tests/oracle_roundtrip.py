#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Yoav Bendor
"""Format correctness against independent readers.

Every table in a matrix (every supported type, nulls at every level, nested structs, several row
groups, empty / unicode / long strings, NaN and signed zeros, both codecs) is written by
nanoarrow2parquet (through its C API, from Python via the Arrow C Data Interface) and read back by:

  * pyarrow (Apache Arrow C++)                         — always
  * arrow-rs (the Apache Arrow Rust `parquet` crate)   — tests/arrow_rs_oracle, if given
  * parquet2nanoarrow (nanom-based reader)             — its C ABI library, if given

Each reader must return exactly the input data, and the footer as arrow-rs decodes it must agree
with pyarrow's view field by field (row groups, chunk offsets and sizes, encodings, codec, schema,
created_by). The file layout is checked too: column chunks tile the data region with no gap or
overlap, and the footer ends the file.

usage: oracle_roundtrip.py libn2p_oracle_c.so [arrow_rs_oracle|-] [libparquet2nanoarrow_c.so|-]
Exit 77 (skipped) when pyarrow / cffi are unavailable.
"""
import ctypes
import json
import math
import os
import random
import subprocess
import sys
import tempfile

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pyarrow.cffi import ffi
except ImportError as e:  # pragma: no cover
    print(f"pyarrow/cffi not available ({e}): skipping")
    sys.exit(77)


# ---------------------------------------------------------------------------- the writer (C API)
class Writer:
    def __init__(self, lib_path):
        lib = ctypes.CDLL(lib_path)
        lib.n2p_writer_open.argtypes = [ctypes.POINTER(ctypes.c_void_p), ctypes.c_char_p]
        lib.n2p_writer_write_batch.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p]
        lib.n2p_writer_set_codec.argtypes = [ctypes.c_void_p, ctypes.c_int]
        lib.n2p_writer_set_encoding.argtypes = [ctypes.c_void_p, ctypes.c_int]
        lib.n2p_writer_close.argtypes = [ctypes.c_void_p]
        lib.n2p_writer_last_error.argtypes = [ctypes.c_void_p]
        lib.n2p_writer_last_error.restype = ctypes.c_char_p
        self.lib = lib

    ENCODINGS = {"auto": 0, "plain": 1, "delta": 2, "byte_stream_split": 3}

    def write(self, path, batches, codec, encoding="auto"):
        w = ctypes.c_void_p()
        if self.lib.n2p_writer_open(ctypes.byref(w), path.encode()) != 0:
            raise RuntimeError("n2p_writer_open failed")
        try:
            if codec == "uncompressed" and self.lib.n2p_writer_set_codec(w, 1) != 0:
                raise RuntimeError("set_codec failed")
            if self.lib.n2p_writer_set_encoding(w, self.ENCODINGS[encoding]) != 0:
                raise RuntimeError("set_encoding failed")
            for b in batches:
                c_schema = ffi.new("struct ArrowSchema*")
                c_array = ffi.new("struct ArrowArray*")
                b._export_to_c(int(ffi.cast("uintptr_t", c_array)), int(ffi.cast("uintptr_t", c_schema)))
                rc = self.lib.n2p_writer_write_batch(w, int(ffi.cast("uintptr_t", c_schema)),
                                                    int(ffi.cast("uintptr_t", c_array)))
                if c_array.release != ffi.NULL:
                    c_array.release(c_array)
                if c_schema.release != ffi.NULL:
                    c_schema.release(c_schema)
                if rc != 0:
                    raise RuntimeError("write_batch: " + self.lib.n2p_writer_last_error(w).decode())
        finally:
            if self.lib.n2p_writer_close(w) != 0:
                raise RuntimeError("n2p_writer_close failed")


# ---------------------------------------------------------------------------- the readers
def read_pyarrow(path):
    return pq.read_table(path)


def read_arrow_rs(exe, path, tmp):
    out = os.path.join(tmp, "rs.arrow")
    r = subprocess.run([exe, path, out], capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError("arrow-rs: " + r.stderr.strip())
    with pa.ipc.open_file(out) as f:
        table = f.read_all()
    return table, json.loads(r.stdout)


class P2N:
    def __init__(self, lib_path):
        self.lib = ctypes.CDLL(lib_path)
        self.lib.p2n_open_stream.argtypes = [ctypes.c_char_p, ctypes.POINTER(ctypes.c_char_p), ctypes.c_int,
                                             ctypes.c_int, ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t]
        self.lib.p2n_open_stream.restype = ctypes.c_int

    def read(self, path):
        stream = ffi.new("struct ArrowArrayStream*")
        addr = int(ffi.cast("uintptr_t", stream))
        err = ctypes.create_string_buffer(4096)
        if self.lib.p2n_open_stream(path.encode(), None, 0, 0, addr, err, len(err)) != 0:
            raise RuntimeError("parquet2nanoarrow: " + err.value.decode())
        return pa.RecordBatchReader._import_from_c(addr).read_all()


# ---------------------------------------------------------------------------- data
# What every reader returns for a written type, when it differs. Parquet has no large_* types, and
# without an embedded ARROW:schema the plain ones come back. The Arrow null type is written as an
# all-null INT32 column (Parquet's UNKNOWN annotation is not emitted), so it reads back as int32.
READ_BACK = {pa.large_string(): pa.string(), pa.large_binary(): pa.binary(), pa.null(): pa.int32()}


def expected_type(t):
    if pa.types.is_struct(t):
        return pa.struct([pa.field(f.name, expected_type(f.type), f.nullable) for f in t])
    return READ_BACK.get(t, t)


def mask(rng, n, density):
    if density == 0:
        return None
    return pa.array([rng.random() < density for _ in range(n)])


def scalar_columns(rng, n, density):
    def ints(lo, hi):
        edge = [lo, hi, 0, -1 if lo < 0 else 1]
        return [edge[i] if i < len(edge) else rng.randint(lo, hi) for i in range(n)]

    def strs(pool):
        return [pool[rng.randrange(len(pool))] for _ in range(n)]

    words = ["", "a", "warn", "info", "héllo", "日本語", "x" * 300, "tab\tquote\"nl\n"]
    floats = [0.0, -0.0, math.inf, -math.inf, math.nan, 1e-310, 3.5]
    cols = {
        "i8": (ints(-128, 127), pa.int8()), "u8": (ints(0, 255), pa.uint8()),
        "i16": (ints(-32768, 32767), pa.int16()), "u16": (ints(0, 65535), pa.uint16()),
        "i32": (ints(-2**31, 2**31 - 1), pa.int32()), "u32": (ints(0, 2**32 - 1), pa.uint32()),
        "i64": (ints(-2**63, 2**63 - 1), pa.int64()), "u64": (ints(0, 2**64 - 1), pa.uint64()),
        "f32": ([floats[i] if i < len(floats) else rng.uniform(-1e6, 1e6) for i in range(n)], pa.float32()),
        "f64": ([floats[i] if i < len(floats) else rng.gauss(0, 1e9) for i in range(n)], pa.float64()),
        "b": ([rng.random() < 0.5 for _ in range(n)], pa.bool_()),
        "s_dict": (strs(words), pa.string()),                                        # low cardinality
        "s_plain": ([f"row-{i}-{rng.random()}" for i in range(n)], pa.string()),      # high cardinality
        "s_large": (strs(words), pa.large_string()),
        "bin": ([bytes(rng.randrange(256) for _ in range(rng.randrange(6))) for _ in range(n)], pa.binary()),
        "bin_large": ([bytes([i % 256]) * (i % 4) for i in range(n)], pa.large_binary()),
        "fsb": ([bytes(rng.randrange(256) for _ in range(16)) for _ in range(n)], pa.binary(16)),
    }
    arrays, fields = [], []
    for name, (vals, typ) in cols.items():
        m = mask(rng, n, density)
        arrays.append(pa.array(vals, typ, mask=m))
        fields.append(pa.field(name, typ, nullable=density > 0))
    return arrays, fields


def nested_column(rng, n, density):
    inner = pa.StructArray.from_arrays(
        [pa.array([f"c{i % 7}" for i in range(n)], pa.string(), mask=mask(rng, n, density)),
         pa.array([rng.gauss(0, 1) for _ in range(n)], pa.float64(), mask=mask(rng, n, density))],
        fields=[pa.field("c", pa.string(), density > 0), pa.field("d", pa.float64(), density > 0)],
        mask=mask(rng, n, density))
    outer = pa.StructArray.from_arrays(
        [pa.array([rng.randrange(1000) for _ in range(n)], pa.int32(), mask=mask(rng, n, density)), inner],
        fields=[pa.field("a", pa.int32(), density > 0), pa.field("b", inner.type, density > 0)],
        mask=mask(rng, n, density))
    return outer, pa.field("nested", outer.type, nullable=density > 0)


def make_cases(rng):
    cases = []
    for density in (0.0, 0.3, 1.0):
        for n, batches in ((1, 1), (1000, 1), (700, 3)):
            if density == 1.0 and n == 1:
                continue
            parts = []
            for _ in range(batches):
                arrays, fields = scalar_columns(rng, n, density)
                nested, nf = nested_column(rng, n, density)
                arrays.append(nested)
                fields.append(nf)
                if density > 0:
                    arrays.append(pa.nulls(n))
                    fields.append(pa.field("null_type", pa.null(), True))
                parts.append(pa.RecordBatch.from_arrays(arrays, schema=pa.schema(fields)))
            cases.append((f"nulls{density}_rows{n}x{batches}", parts))
    return cases


# ---------------------------------------------------------------------------- comparisons
def expected_table(batches):
    t = pa.Table.from_batches(batches)
    schema = pa.schema([pa.field(f.name, expected_type(f.type), f.nullable) for f in t.schema])
    return t.cast(schema)


def same_values(got, want):
    """Exact equality, floats by bit pattern (NaN == NaN, -0.0 != 0.0)."""
    if got.num_rows != want.num_rows or got.column_names != want.column_names:
        return False, "shape differs"
    for name in want.column_names:
        g, w = got[name].combine_chunks(), want[name].combine_chunks()
        if g.type != w.type:
            return False, f"column {name}: type {g.type} != {w.type}"
        if pa.types.is_floating(w.type):
            gv = [None if v is None else struct_bits(v, w.type) for v in g.to_pylist()]
            wv = [None if v is None else struct_bits(v, w.type) for v in w.to_pylist()]
            if gv != wv:
                return False, f"column {name}: float bits differ"
        elif not g.equals(w):
            first = next((i for i in range(len(w)) if not g[i].equals(w[i])), None)
            return False, f"column {name}: differs at row {first}: {g[first]} vs {w[first]}"
    return True, ""


def struct_bits(v, t):
    import struct
    return struct.pack("<f" if t == pa.float32() else "<d", v)


def leaf_columns(table):
    """Leaf columns by dotted path; struct parents' nulls are merged into their children."""
    while any(pa.types.is_struct(f.type) for f in table.schema):
        table = table.flatten()
    return {name: table[name].combine_chunks() for name in table.column_names}


def expected_stats(arr):
    """(null_count, min, max) by Parquet's rules: NaN ignored, bytes compared unsigned."""
    values = [v for v in arr.to_pylist() if v is not None]
    nulls = len(arr) - len(values)
    if pa.types.is_floating(arr.type):
        values = [v for v in values if not math.isnan(v)]
    if not values or pa.types.is_null(arr.type):
        return nulls, None, None
    if pa.types.is_string(arr.type) or pa.types.is_large_string(arr.type):
        key = lambda v: v.encode()  # noqa: E731 - UTF-8 byte order
    else:
        key = None
    return nulls, min(values, key=key), max(values, key=key)


def check_statistics(path, failures, name):
    pf = pq.ParquetFile(path)
    for g in range(pf.metadata.num_row_groups):
        leaves = leaf_columns(pf.read_row_group(g))
        rg = pf.metadata.row_group(g)
        for c in range(rg.num_columns):
            cc = rg.column(c)
            st = cc.statistics
            arr = leaves.get(cc.path_in_schema)
            if st is None or arr is None:
                failures.append(f"{name}: rg {g} {cc.path_in_schema}: no statistics")
                continue
            nulls, mn, mx = expected_stats(arr)
            if st.null_count != nulls:
                failures.append(f"{name}: rg {g} {cc.path_in_schema}: null_count {st.null_count} != {nulls}")
            if mn is None:
                if st.has_min_max and not pa.types.is_null(arr.type):
                    failures.append(f"{name}: rg {g} {cc.path_in_schema}: min/max without values")
                continue
            if not st.has_min_max:
                failures.append(f"{name}: rg {g} {cc.path_in_schema}: no min/max")
            elif st.min != mn or st.max != mx:
                failures.append(f"{name}: rg {g} {cc.path_in_schema}: min/max {st.min!r}/{st.max!r} != {mn!r}/{mx!r}")


def check_metadata(path, rs_meta, failures, name):
    pm = pq.ParquetFile(path).metadata
    size = os.path.getsize(path)

    def fail(msg):
        failures.append(f"{name}: metadata: {msg}")

    if rs_meta is not None:
        if rs_meta["num_rows"] != pm.num_rows:
            fail(f"num_rows {rs_meta['num_rows']} vs {pm.num_rows}")
        if rs_meta["created_by"] != pm.created_by:
            fail(f"created_by {rs_meta['created_by']!r} vs {pm.created_by!r}")
        if len(rs_meta["row_groups"]) != pm.num_row_groups:
            fail("row group count")
        if len(rs_meta["leaves"]) != pm.num_columns:
            fail("leaf count")
        for i, leaf in enumerate(rs_meta["leaves"]):
            col = pm.schema.column(i)
            if leaf["path"] != col.path or leaf["physical"] != col.physical_type or \
               leaf["max_def"] != col.max_definition_level or leaf["max_rep"] != col.max_repetition_level:
                fail(f"leaf {i}: {leaf} vs {col.path} {col.physical_type} "
                     f"def={col.max_definition_level} rep={col.max_repetition_level}")
    # chunks tile the data region exactly: PAR1 | chunk | chunk | ... | footer | len | PAR1
    at = 4
    for g in range(pm.num_row_groups):
        rg = pm.row_group(g)
        rs_rg = rs_meta["row_groups"][g] if rs_meta else None
        if rs_rg and (rs_rg["num_rows"] != rg.num_rows or rs_rg["total_byte_size"] != rg.total_byte_size):
            fail(f"row group {g}: rows/bytes differ")
        for c in range(rg.num_columns):
            cc = rg.column(c)
            start = cc.dictionary_page_offset if cc.has_dictionary_page else cc.data_page_offset
            if start != at:
                fail(f"rg {g} col {c}: chunk starts at {start}, previous ended at {at}")
            at = start + cc.total_compressed_size
            if rs_rg:
                rc = rs_rg["columns"][c]
                want = {"path": cc.path_in_schema, "codec": cc.compression, "num_values": cc.num_values,
                        "compressed": cc.total_compressed_size, "uncompressed": cc.total_uncompressed_size,
                        "data_page_offset": cc.data_page_offset,
                        "dictionary_page_offset": cc.dictionary_page_offset if cc.has_dictionary_page else None}
                got = {k: rc[k] for k in want}
                if got != want:
                    fail(f"rg {g} col {c}: arrow-rs {got} vs pyarrow {want}")
                if sorted(rc["encodings"]) != sorted(e.upper() for e in cc.encodings):
                    fail(f"rg {g} col {c}: encodings {rc['encodings']} vs {cc.encodings}")
    footer_len = int.from_bytes(open(path, "rb").read()[-8:-4], "little")
    if at + footer_len + 8 != size:
        fail(f"data ends at {at}, footer of {footer_len} bytes, file is {size} bytes")


def main():
    writer = Writer(sys.argv[1])
    rs = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] != "-" else None
    p2n = P2N(sys.argv[3]) if len(sys.argv) > 3 and sys.argv[3] != "-" else None
    rng = random.Random(20261002)
    failures, files = [], 0
    with tempfile.TemporaryDirectory() as tmp:
        variants = [("zstd", "auto"), ("uncompressed", "auto"), ("zstd", "plain"), ("zstd", "delta"),
                    ("uncompressed", "delta"), ("zstd", "byte_stream_split")]
        for name, batches in make_cases(rng):
            for codec, encoding in variants:
                case = f"{name}/{codec}/{encoding}"
                path = os.path.join(tmp, "t.parquet")
                try:
                    writer.write(path, batches, codec, encoding)
                except RuntimeError as e:
                    failures.append(f"{case}: write failed: {e}")
                    continue
                files += 1
                want = expected_table(batches)
                readers = [("pyarrow", lambda: read_pyarrow(path))]
                rs_meta = None
                if rs:
                    try:
                        rs_table, rs_meta = read_arrow_rs(rs, path, tmp)
                        readers.append(("arrow-rs", lambda t=rs_table: t))
                    except RuntimeError as e:
                        failures.append(f"{case}: {e}")
                if p2n:
                    readers.append(("parquet2nanoarrow", lambda: p2n.read(path)))
                for reader, read in readers:
                    try:
                        ok, why = same_values(read(), want)
                    except Exception as e:  # noqa: BLE001 - any reader error is a finding
                        ok, why = False, f"read failed: {e}"
                    if not ok:
                        failures.append(f"{case}: {reader}: {why}")
                check_metadata(path, rs_meta, failures, case)
                check_statistics(path, failures, case)
        # a writer that receives no batch still produces a valid, empty file
        path = os.path.join(tmp, "empty.parquet")
        writer.write(path, [], "zstd")
        files += 1
        try:
            if pq.ParquetFile(path).metadata.num_rows != 0:
                failures.append("empty: num_rows != 0")
        except Exception as e:  # noqa: BLE001
            failures.append(f"empty file unreadable: {e}")
    names = ["pyarrow"] + (["arrow-rs"] if rs else []) + (["parquet2nanoarrow"] if p2n else [])
    for f in failures:
        print("FAIL", f)
    print(f"oracle round trip: {files} files, readers: {', '.join(names)}: "
          f"{'all equal' if not failures else str(len(failures)) + ' failure(s)'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
