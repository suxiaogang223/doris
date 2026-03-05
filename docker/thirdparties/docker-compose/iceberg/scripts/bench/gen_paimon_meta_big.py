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
Generate a metadata-heavy Paimon table by repeatedly appending bucketed tiny files.

Designed for Doris thirdparty Spark+MinIO stack under:
  docker/thirdparties/docker-compose/iceberg

We use Paimon bucketed append table + write-only to keep metadata growing:
  - bucket + bucket-key: spread data into many buckets (more small files)
  - write-only=true: skip compaction & snapshot expiration (keep snapshots/manifests)

Env vars (all optional):
  NUM_COMMITS           default 20
  BUCKETS               default 200
  BUCKET_COVER_FACTOR   default 5
  PAIMON_CATALOG        default "paimon"
  PAIMON_DB             default "bench_paimon"
  PAIMON_TBL            default "meta_big"
  PAYLOAD_REPEAT        default 10
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
    buckets = _get_int("BUCKETS", 200)
    bucket_cover_factor = _get_int("BUCKET_COVER_FACTOR", 5)
    payload_repeat = _get_int("PAYLOAD_REPEAT", 10)

    catalog = os.environ.get("PAIMON_CATALOG", "paimon").strip()
    db = os.environ.get("PAIMON_DB", "bench_paimon").strip()
    tbl = os.environ.get("PAIMON_TBL", "meta_big").strip()
    full = f"{catalog}.{db}.{tbl}"

    spark = (
        SparkSession.builder.appName("gen-paimon-meta-big")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{db}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {full} (
          id BIGINT,
          payload STRING
        ) USING paimon
        TBLPROPERTIES (
          'bucket' = '{buckets}',
          'bucket-key' = 'id',
          'write-only' = 'true',
          'file.format' = 'parquet'
        )
        """
    )

    rows_per_commit = buckets * bucket_cover_factor
    for i in range(num_commits):
        start = i * rows_per_commit
        end = (i + 1) * rows_per_commit
        df = (
            spark.range(start, end, 1, buckets)
            .withColumn("payload", expr(f"repeat('y', {payload_repeat})"))
        )
        df.writeTo(full).append()

        if (i + 1) % 10 == 0 or i + 1 == num_commits:
            print(f"[Paimon] committed {i+1}/{num_commits}")

    print("[Paimon] done")


if __name__ == "__main__":
    main()

