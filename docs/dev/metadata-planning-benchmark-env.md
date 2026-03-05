# 基线环境：稳定构建 metadata-heavy 的 Iceberg/Paimon 表（Spark + MinIO），并用 Doris Profile 建立对比基准

## 1. 背景与目标

Doris 在查询 Iceberg/Paimon 外表时，plan 阶段需要读取并展开大量元数据：
- Iceberg：snapshots / manifest list / manifests
- Paimon：snapshots / manifests / data files

当表很大、元数据很多时，瓶颈往往出现在 FE（coordinator）侧，典型表现为：
- plan 时间显著升高
- FE 产生大量远端 I/O（对象存储读取 metadata 文件，manifest 越多越“碎”）

本文目标：
- 构造一个“元数据规模可控、可复现”的 Iceberg/Paimon 大表环境（Spark + MinIO）
- 用 Doris 查询并收集 Profile，固化后续开发（分布式 plan / FE 并行 plan）的对比基线

本文不追求数据量巨大（TB 级），而是追求“元数据密度大”：大量小文件、manifest 多、snapshot 多。

---

## 2. 基线环境（推荐复用 Doris thirdparty）

### 2.1 启动组件（Doris 仓库自带）

推荐直接复用 Doris 仓库自带的 thirdparty 环境：

```bash
cd docker/thirdparties
./run-thirdparties-docker.sh -c iceberg
```

注意事项：
- 需要先在 `docker/thirdparties/custom_settings.env` 里把 `CONTAINER_UID` 改成一个唯一前缀（脚本会拒绝默认值 `doris--`）
- 该环境会启动：Spark + Iceberg REST catalog + MinIO + Postgres（给 REST catalog 用）
- 端口在 `docker/thirdparties/docker-compose/iceberg/iceberg.env` 里：
  - `REST_CATALOG_PORT` 默认 `18181`（宿主机 -> 容器 8181）
  - `MINIO_API_PORT` 默认 `19001`（宿主机 -> 容器 9000）

你也可以用自建 Spark + MinIO，只要满足：
- Spark 3.5
- Iceberg runtime（thirdparty 环境写入侧为 Iceberg 1.10.0）
- Paimon（thirdparty 环境为 1.0.1）
- Doris 所在机器能访问到 MinIO endpoint 与 Iceberg REST endpoint（若使用 REST catalog）

### 2.2 基线原则：尽量减少变量

后续你会用 Doris profile 对比不同 planning 方案，因此建议：
- 环境、版本、生成参数固定
- 先用一档参数把链路跑通（S 档），再上强度（M/L 档）
- 每次对比都明确是 “cold” 还是 “warm”

---

## 3. 稳定性原则（保证“可复现”）

为了让“每次生成的元数据规模相近”，建议遵循：
- 固定生成参数：`NUM_COMMITS / FILES_PER_COMMIT / ROWS_PER_FILE / BUCKETS`
- 禁用 Spark AQE，避免运行时自动合并/重分区导致文件数波动：`spark.sql.adaptive.enabled=false`
- Iceberg：关闭写入侧 manifest merge，避免 manifest 数被“写端自优化”吞掉
- Paimon：使用 bucketed append table，并设置 `write-only=true`，跳过 compaction 与 snapshot expiration
- 每次测量前明确 cache 策略（Doris 侧 `REFRESH CATALOG`）

---

## 4. Iceberg：构造 metadata-heavy 表（多 snapshots + 多 manifests + 多 data files）

### 4.1 放大元数据的核心旋钮

- `NUM_COMMITS`：commit 次数（≈ snapshot 数）
- `FILES_PER_COMMIT`：每次 commit 产生的数据文件数（≈ manifest entries 增量；也往往增加 manifest 文件数）
- `ROWS_PER_FILE`：每个数据文件行数（越小越偏“元数据密度”）

关键：关闭写入侧自动 manifest merge（Iceberg table properties）：
- `commit.manifest-merge.enabled=false`
- （可选）`commit.manifest.min-count-to-merge=1000000` 作为双保险

参考：Iceberg table properties（`commit.manifest.*`）。  
- Iceberg 文档：<https://iceberg.apache.org/docs/latest/configuration/#commit>

### 4.2 建议的表定义（Spark SQL）

示例（catalog 名以 thirdparty 默认的 `demo` 为例）：

```sql
CREATE DATABASE IF NOT EXISTS demo.bench_iceberg;
DROP TABLE IF EXISTS demo.bench_iceberg.meta_big;

CREATE TABLE demo.bench_iceberg.meta_big (
  id BIGINT,
  payload STRING
) USING iceberg
TBLPROPERTIES (
  'format-version' = '2',
  'write.format.default' = 'parquet',
  'commit.manifest-merge.enabled' = 'false'
);
```

说明：
- 故意不做分区：减少额外变量，让“文件数/manifest 数”更可控
- 后续如要测分区裁剪，可另建一张 partitioned 表作为补充（不要和这张混用）

### 4.3 生成脚本（在 Spark 容器内跑 spark-submit）

思路：
- 每次循环生成 `FILES_PER_COMMIT * ROWS_PER_FILE` 行
- range 的 `numPartitions=FILES_PER_COMMIT`，让输出 partition 数固定
- 配合 `spark.sql.files.maxRecordsPerFile=ROWS_PER_FILE`，尽量保证每个 partition 产出 1 个文件

建议三档参数：
- S：`NUM_COMMITS=20 FILES_PER_COMMIT=200 ROWS_PER_FILE=1`
- M：`NUM_COMMITS=200 FILES_PER_COMMIT=200 ROWS_PER_FILE=1`
- L：`NUM_COMMITS=500 FILES_PER_COMMIT=500 ROWS_PER_FILE=1`

执行方式（示意，按你的容器名替换）：
1. 找到 Spark 容器名（通常包含 `spark-iceberg`）
2. `docker exec -it <spark_container> bash`
3. 在容器内执行（脚本已随仓库放在 `/mnt/scripts/bench`）：

脚本位置：
- 仓库：`docker/thirdparties/docker-compose/iceberg/scripts/bench/gen_iceberg_meta_big.py`
- 容器：`/mnt/scripts/bench/gen_iceberg_meta_big.py`

```bash
NUM_COMMITS=20 FILES_PER_COMMIT=200 ROWS_PER_FILE=1 \
spark-submit \
  --master "spark://$(hostname):7077" \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  /mnt/scripts/bench/gen_iceberg_meta_big.py
```

### 4.4 校验元数据规模

推荐用 Iceberg 的 metadata tables 做校验（避免依赖文件命名）：
- `SELECT count(*) FROM demo.bench_iceberg.meta_big.files;`
- `SELECT count(*) FROM demo.bench_iceberg.meta_big.snapshots;`
- `SELECT count(*) FROM demo.bench_iceberg.meta_big.manifests;`

如果你更想从对象存储侧看文件数量，也可以用 MinIO `mc` 统计 `metadata/` 目录下对象数。

---

## 5. Paimon：构造 metadata-heavy 表（多 snapshots + 多 bucket 文件，且不 compaction/expire）

### 5.1 放大元数据的核心旋钮

- `NUM_COMMITS`：写入次数（≈ snapshots）
- `BUCKETS`：bucket 数（每次 commit 触发的 bucket 文件数量上限）
- `BUCKET_COVER_FACTOR`：每次写入行数放大系数（提高每次 commit 覆盖 bucket 的概率）
- `write-only=true`：跳过 compaction 与 snapshot expiration，保证元数据单调增长

参考：
- Paimon bucketed append table：<https://paimon.apache.org/docs/1.0/append-table/bucketed/>
- Paimon `write-only` 配置项：<https://paimon.apache.org/docs/1.0/maintenance/configurations/>

### 5.2 建议的表定义（Spark SQL）

```sql
CREATE DATABASE IF NOT EXISTS paimon.bench_paimon;
DROP TABLE IF EXISTS paimon.bench_paimon.meta_big;

CREATE TABLE paimon.bench_paimon.meta_big (
  id BIGINT,
  payload STRING
)
TBLPROPERTIES (
  'bucket' = '200',
  'bucket-key' = 'id',
  'write-only' = 'true',
  'file.format' = 'parquet'
);
```

### 5.3 生成脚本（spark-submit）

建议三档参数：
- S：`NUM_COMMITS=20 BUCKETS=200 BUCKET_COVER_FACTOR=5`
- M：`NUM_COMMITS=200 BUCKETS=200 BUCKET_COVER_FACTOR=5`
- L：`NUM_COMMITS=500 BUCKETS=500 BUCKET_COVER_FACTOR=5`

说明：
- `BUCKET_COVER_FACTOR` 用于提高每次 commit 覆盖 bucket 的概率（经验上 5 倍通常足够；不足再加）

```bash
NUM_COMMITS=20 BUCKETS=200 BUCKET_COVER_FACTOR=5 \
spark-submit \
  --master "spark://$(hostname):7077" \
  --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions \
  /mnt/scripts/bench/gen_paimon_meta_big.py
```

脚本位置：
- 仓库：`docker/thirdparties/docker-compose/iceberg/scripts/bench/gen_paimon_meta_big.py`
- 容器：`/mnt/scripts/bench/gen_paimon_meta_big.py`

### 5.4 校验元数据规模

推荐两类校验：
- 对象存储侧：查看 table 目录下 `snapshot/`、`manifest/`、bucket 文件是否随 commit 单调增长
- 逻辑侧：执行简单查询确保表可读（例如 `SELECT count(*)`）

---

## 6. Doris：接入 Catalog 并执行基线查询

### 6.1 创建 Catalog（沿用 Doris regression 的最小配置）

Iceberg REST（推荐与 thirdparty 环境一致）：

```sql
DROP CATALOG IF EXISTS iceberg_rest;
CREATE CATALOG iceberg_rest PROPERTIES (
  'type'='iceberg',
  'iceberg.catalog.type'='rest',
  'uri'='http://<externalEnvIp>:18181',
  's3.access_key'='admin',
  's3.secret_key'='password',
  's3.endpoint'='http://<externalEnvIp>:19001',
  's3.region'='us-east-1'
);
```

Iceberg Hadoop（可选）：

```sql
DROP CATALOG IF EXISTS iceberg_hadoop;
CREATE CATALOG iceberg_hadoop PROPERTIES (
  'type'='iceberg',
  'iceberg.catalog.type'='hadoop',
  'warehouse'='s3a://warehouse/wh',
  's3.access_key'='admin',
  's3.secret_key'='password',
  's3.endpoint'='http://<externalEnvIp>:19001',
  's3.region'='us-east-1'
);
```

Paimon：

```sql
DROP CATALOG IF EXISTS paimon_minio;
CREATE CATALOG paimon_minio PROPERTIES (
  'type'='paimon',
  'warehouse'='s3://warehouse/wh/',
  's3.access_key'='admin',
  's3.secret_key'='password',
  's3.endpoint'='http://<externalEnvIp>:19001',
  's3.region'='us-east-1'
);
```

`externalEnvIp` 选择建议：
- Doris（FE/BE）与 MinIO 在同一台宿主机：优先用宿主机 IP（不要用容器内网 IP）
- 单机开发场景通常可用 `127.0.0.1`，但多机/容器化 Doris 时要保证每个节点可达

常见坑：
- 如果你用的是 `http://<ip>:19001` 这类 endpoint，遇到“bucket 子域名解析失败”等问题，可以尝试在 Catalog PROPERTIES 增加：
  - `'use_path_style'='true'`（或 `'s3.path-style-access'='true'`）

### 6.2 执行查询（建议 EXPLAIN + 实跑各一条）

Profile 基本开关：

```sql
SET enable_profile=true;
SET profile_level=2;
```

建议 SQL：
- Iceberg：
  - `EXPLAIN SELECT count(*) FROM bench_iceberg.meta_big;`
  - `SELECT count(*) FROM bench_iceberg.meta_big;`
- Paimon：
  - `EXPLAIN SELECT count(*) FROM bench_paimon.meta_big;`
  - `SELECT count(*) FROM bench_paimon.meta_big;`

冷/热区分：
- 冷：每次 query 前执行 `REFRESH CATALOG <catalog_name>;`（默认 invalid cache）
- 热：不 refresh，连续执行相同 query

---

## 7. Profile 获取与基线记录模板

### 7.1 获取 Profile ID

```sql
SHOW QUERY PROFILE LIMIT 5;
```

取最新一条的 `Profile ID`。

### 7.2 拉取完整 Profile 文本

推荐用 FE HTTP 接口（端口以 FE 的 `http_port` 配置为准）：
- `GET /api/profile/text?query_id=<profile_id>`
- 或 `GET /api/profile/text`（拉最后一条）

示例：

```bash
curl -u root: "http://<fe_host>:8030/api/profile/text?query_id=<profile_id>" > profile.txt
```

说明：
- 该接口需要管理员权限（通常 root 用户具备）

### 7.3 需要记录的核心字段（用于后续对比）

从 Profile 的 Summary / Execution Summary 里至少记录：
- `Plan Time`
- `Get Splits Time`
- `Get Partition Files Time`
- `Create Scan Range Time`
- `Nereids Analysis/Rewrite/Optimize/Translate/Distribute Time`（作为“非元数据开销”的对照）

建议用如下表格固化基线：

| 数据源 | 档位 | 冷/热 | NUM_COMMITS | FILES_PER_COMMIT/BUCKETS | Doris SQL | Plan Time | Get Splits Time | Get Partition Files Time | Create Scan Range Time |
|---|---|---|---:|---:|---|---:|---:|---:|---:|

---

## 8. 清理与重置（保证可重复跑）

生成侧（Spark）：
- `DROP TABLE` / `DROP DATABASE`（按你是否希望保留数据决定）

对象存储侧（更彻底）：
- 用 MinIO `mc rm -r --force` 删除对应 table 前缀
- 再重新生成，保证 cold-start 完整一致

Doris 侧：
- `REFRESH CATALOG` 可以作为“每次 cold-run 前的标准动作”
- 如需彻底清 cache：`DROP CATALOG` 再 `CREATE CATALOG`
