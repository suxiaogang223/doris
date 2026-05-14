# DuckDB-style Iceberg + Parquet reader composition experiment

This experiment intentionally removes the old `IcebergReaderMixin<ParquetReader>`
inheritance design from the Iceberg + Parquet path.

Doris BE does not own Iceberg file enumeration. FE resolves the Iceberg snapshot,
manifests and data files, then sends BE a sequence of thrift scan ranges.
`FileScanner` still receives that thrift boundary, but `IcebergReaderAdapter`
immediately extracts it into small reader-owned structs. `IcebergTableReader`
and `ParquetReader` do not accept `TFileScanRangeParams` or `TFileRangeDesc`,
which keeps the reader API small enough for unit tests to construct directly.
The BE reader boundary is one split, but the API role still mirrors DuckDB:

- DuckDB `MultiFileReader` -> Doris `TableReader`
- DuckDB `BaseFileReader` -> Doris `BaseFileFormatReader`
- DuckDB `ParquetReaderScanState` -> Doris `ParquetScanState`

## Runtime stack

```text
FileScanner
  IcebergReaderAdapter              // GenericReader bridge only
    TableReader
      IcebergTableReader            // table-format semantics for one split
      FileFormatReader
        BaseFileFormatReader        // common file-format fields
          ParquetReader             // physical Parquet split reader
          ParquetScanState
          ColumnReader API          // column/page/level decoding boundary
```

## Ownership split

`IcebergTableReader` owns table semantics:

- Iceberg field-id schema mapping and name fallback
- partition fallback
- missing/default/generated/synthesized columns
- equality delete, position delete and deletion vector planning
- `$row_id`, `_row_id`, `_last_updated_sequence_number`
- residual predicates and final projection

`ParquetReader` owns physical format semantics:

- footer/schema loading
- row group/page pruning
- predicate-column read phase
- lazy payload read phase
- levels-only/reference-level reads for nested missing fields
- row position propagation
- hidden physical columns requested by the table layer

`ColumnReader` is only an API in this experiment. It is the future home for
page decoding, level reads, filter/select/skip and prefetch.

## Scan state split

Reader objects hold file/table metadata. Scan state objects hold mutable scan
cursors.

`IcebergTableReaderScanState` owns the split context, schema mapping, delete
plan, required fields, virtual column plan, the selected `FileFormatReader` and
its `FormatReaderScanState`. The file read plan is configured on
`BaseFileFormatReader::scan_properties`, mirroring DuckDB's `BaseFileReader`
members instead of using a separate task object.

`ParquetScanState` owns row-group cursor state, selection, lazy read plan,
output template, and pseudocode placeholders for the recursive `ColumnReader`
tree plus definition/repetition level buffers. This follows DuckDB's
`ParquetReaderScanState` shape. Selection is internal to this scan state and is
not returned across the file/table boundary.

`FileFormatReader` is a narrow interface. `BaseFileFormatReader` carries common
fields shared by physical formats: `FileSplit`, `ReaderRuntimeOptions` and
`scan_properties`.

`FileSplit` contains only physical split data such as path, offset, size and
format. `ReaderRuntimeOptions` contains runtime-only dependencies such as batch
size, timezone, IO context, file metadata cache and the legacy init context.
This is the only API shape table/file readers need; thrift conversion belongs
to the adapter layer.

## Lazy materialization contract

`BaseFileFormatReader::scan_properties.required_fields` classifies fields by purpose:

- `PREDICATE`
- `OUTPUT`
- `EQUALITY_DELETE_KEY`
- `ROW_ID`
- `ROW_LINEAGE`
- `LEVELS_ONLY`
- `REFERENCE_LEVELS`

`ParquetReader` must call `ColumnReader::filter` for predicate fields first,
materialize predicate virtual columns, evaluate selection, and then call
`ColumnReader::select` for payload fields only for selected rows. If no rows
survive, payload readers use `ColumnReader::skip`. Position deletes and
deletion vectors are represented as `RowVisibility` and are applied before
payload lazy reads. The file reader returns a `Block` directly; row positions,
equality-delete keys and levels-only data are hidden columns inside that block,
not side-channel batch fields.

## Schema change contract

`FieldMappingNode` is recursive and replaces hidden inherited state. It carries:

- table path
- physical file path
- Iceberg field id
- physical Parquet column id range
- missing/partition/generated/synthesized kind
- optional cast plan
- nested children

Nested missing fields request `REFERENCE_LEVELS` or `LEVELS_ONLY` so the table
layer fills them using nested element cardinality, not top-level row count.

## Delete contract

Position deletes and deletion vectors become `RowVisibility`.

Equality delete keys become hidden `RequiredField` entries. `ParquetReader`
reads them like normal physical columns, and `IcebergTableReader` filters and
removes them during finalization.

## Compilation status

This is a deliberately non-compilable architecture experiment. The old Parquet
reader and Iceberg reader bodies were replaced with pseudocode-level APIs so the
layering is explicit and reviewable without preserving all legacy call sites.
