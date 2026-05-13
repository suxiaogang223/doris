# Iceberg and Parquet reader composition experiment

This experiment makes the table-format reader and the physical file-format
reader separate components.

## Ownership split

`TableReader` returns the final Doris `Block`. It owns table-format semantics:
schema evolution, partition fallback, missing/default/generated columns,
Iceberg delete files, row lineage columns, and final projection.

`FileFormatReader` returns `PhysicalReadBatch`. It owns only physical file
work: opening file metadata, pruning row groups/pages, reading physical
columns, applying file-level predicates, preserving row positions, and exposing
hidden columns requested by the table layer.

`IcebergParquetTableReader` therefore has a `std::unique_ptr<FileFormatReader>`
instead of inheriting from `ParquetReader`. `IcebergParquetReaderAdapter` exists
only to let the current `FileScanner` keep a `GenericReader`.

## Lazy materialization contract

`FormatScanTask::required_fields` distinguishes output, predicate, delete-key,
row-id, row-lineage, and levels-only fields. Parquet uses that intent to keep
the existing lazy materialization shape:

1. read predicate physical fields;
2. materialize predicate virtual fields through `VirtualColumnPlan`;
3. evaluate visibility/predicate selection;
4. read lazy payload fields only for selected rows;
5. return row positions aligned with selected physical rows.

Virtual fields are explicit because partition columns, missing columns, and
generated columns can appear in predicates even though they are not Parquet
columns.

## Schema change contract

`FieldMappingNode` is recursive rather than a flat column map. It represents:

- table path to physical path mapping;
- Iceberg field id and name fallback;
- missing fields;
- casts and residual casts;
- reference-level reads for nested missing fields.

Nested missing fields use `REFERENCE_LEVELS` or `LEVELS_ONLY` required fields so
the file reader can read definition/repetition levels from a physical sibling.
That keeps `array<struct<missing_field>>` cardinality tied to element count
instead of top-level row count.

## Iceberg delete contract

Position deletes and deletion vectors are converted into `RowVisibility` before
Parquet reads lazy payload columns. Equality delete keys are requested as hidden
required fields and removed in `IcebergParquetTableReader::finalize_block` after
delete matching.

The current patch keeps the concrete delete-file reader body as experimental
follow-up work, but the API boundary already has the necessary data flow.
