# Third-party licenses

nanoarrow2parquet is licensed under Apache-2.0 (see [LICENSE](LICENSE)). It builds
on the following third-party software. All are permissive and Apache-2.0-compatible.

## Format definitions (derived)

| Component | How it's used | License | Project |
|---|---|---|---|
| **Apache Parquet** `parquet.thrift` | nanoarrow2parquet's Thrift field ids and enum values (FileMetaData, SchemaElement, RowGroup, ColumnChunk/ColumnMetaData, PageHeader, encodings, codecs) are derived from the Parquet format definition. nanoarrow2parquet does **not** link the Parquet reference implementation. | Apache-2.0 | https://github.com/apache/parquet-format |

## Build-time dependencies (FetchContent, not redistributed)

| Component | Used for | License | Project |
|---|---|---|---|
| Apache Arrow nanoarrow | Arrow `ArrowSchema` / `ArrowArray` C Data Interface types | Apache-2.0 | https://github.com/apache/arrow-nanoarrow |
| Zstandard (zstd) | per-page compression | BSD-3-Clause | https://github.com/facebook/zstd |

License compatibility: Apache-2.0 and BSD-3-Clause are permissive and mutually
compatible. (zstd is also offered under GPLv2; this project uses it under the
BSD-3-Clause option.)
