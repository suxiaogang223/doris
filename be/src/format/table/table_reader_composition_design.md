# Split-driven Iceberg + Parquet reader composition experiment

This experiment intentionally removes the old `IcebergReaderMixin<ParquetReader>`
inheritance design from the Iceberg + Parquet path.

Doris BE does not own Iceberg file enumeration. FE resolves the Iceberg snapshot,
manifests and data files, then sends BE a sequence of `TFileRangeDesc` splits.
Therefore the BE reader boundary is one split, not a multi-file planning layer.

## Runtime stack

```text
FileScanner
  IcebergReaderAdapter              // GenericReader bridge only
    IcebergTableReader              // table-format semantics for one split
      FileFormatReader
        ParquetReader               // physical Parquet split reader
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
page decoding, level reads, skip/select and prefetch.

## Lazy materialization contract

`FormatScanTask::required_fields` classifies fields by purpose:

- `PREDICATE`
- `OUTPUT`
- `EQUALITY_DELETE_KEY`
- `ROW_ID`
- `ROW_LINEAGE`
- `LEVELS_ONLY`
- `REFERENCE_LEVELS`

`ParquetReader` must read predicate fields first, materialize predicate virtual
columns, evaluate selection, and then read payload fields only for selected
rows. Position deletes and deletion vectors are represented as `RowVisibility`
and are applied before payload lazy reads.

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
