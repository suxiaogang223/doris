#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Generate a metadata-heavy Iceberg table by repeatedly appending many tiny files.

Designed for Doris thirdparty Spark+MinIO stack under:
  docker/thirdparties/docker-compose/iceberg

Env vars (all optional):
  NUM_COMMITS        default 20
  FILES_PER_COMMIT   default 200
  ROWS_PER_FILE      default 1
  ICEBERG_CATALOG    default "demo"
  ICEBERG_DB         default "bench_iceberg"
  ICEBERG_TBL        default "meta_big"
  PAYLOAD_REPEAT     default 10
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr


def _get_int(name: str, default: int) -> int:
    value = os.environ.get(name, str(default)).strip()
    try:
        return int(value)
    except ValueError as e:
        raise ValueError(f"Invalid int env {name}={value!r}") from e


def main() -> None:
    num_commits = _get_int("NUM_COMMITS", 20)
    files_per_commit = _get_int("FILES_PER_COMMIT", 200)
    rows_per_file = _get_int("ROWS_PER_FILE", 1)
    payload_repeat = _get_int("PAYLOAD_REPEAT", 10)

    catalog = os.environ.get("ICEBERG_CATALOG", "demo").strip()
    db = os.environ.get("ICEBERG_DB", "bench_iceberg").strip()
    tbl = os.environ.get("ICEBERG_TBL", "meta_big").strip()
    full = f"{catalog}.{db}.{tbl}"

    spark = (
        SparkSession.builder.appName("gen-iceberg-meta-big")
        .config("spark.sql.adaptive.enabled", "false")
        # Keep files tiny and stable across runs.
        .config("spark.sql.files.maxRecordsPerFile", str(rows_per_file))
        .getOrCreate()
    )

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{db}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {full} (
          id BIGINT,
          payload STRING
        ) USING iceberg
        TBLPROPERTIES (
          'format-version' = '2',
          'write.format.default' = 'parquet',
          'commit.manifest-merge.enabled' = 'false',
          'commit.manifest.min-count-to-merge' = '1000000'
        )
        """
    )

    # One append -> one snapshot. Try to make each append produce FILES_PER_COMMIT data files
    # (and thus a lot of manifest entries / manifest files) with minimal data volume.
    for i in range(num_commits):
        total_rows = files_per_commit * rows_per_file
        df = (
            spark.range(i * total_rows, (i + 1) * total_rows, 1, files_per_commit)
            .selectExpr("id as id")
            .withColumn("payload", expr(f"repeat('x', {payload_repeat})"))
        )
        df.writeTo(full).append()

        if (i + 1) % 10 == 0 or i + 1 == num_commits:
            print(f"[Iceberg] committed {i+1}/{num_commits}")

    print("[Iceberg] done")


if __name__ == "__main__":
    main()

