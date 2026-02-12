# Module 3 Homework: Data Warehousing & BigQuery

## Data Setup

### Loading Data to GCS

**Data source:** Yellow Taxi Trip Records for January 2024 - June 2024

**URL pattern:** `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-{MM}.parquet`

**Loading script (Python):**
```python
import os
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage

BUCKET_NAME = "your_bucket_name"
client = storage.Client.from_service_account_json("gcs.json")

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-"
MONTHS = [f"{i:02d}" for i in range(1, 7)]

bucket = client.bucket(BUCKET_NAME)

def download_and_upload(month):
    url = f"{BASE_URL}{month}.parquet"
    file_path = f"yellow_tripdata_2024-{month}.parquet"

    print(f"Downloading {url}...")
    urllib.request.urlretrieve(url, file_path)

    blob = bucket.blob(file_path)
    blob.upload_from_filename(file_path)
    print(f"Uploaded: gs://{BUCKET_NAME}/{file_path}")

with ThreadPoolExecutor(max_workers=4) as executor:
    executor.map(download_and_upload, MONTHS)
```

### BigQuery Setup

**Create External Table:**
```sql
CREATE OR REPLACE EXTERNAL TABLE `project.dataset.yellow_taxi_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://your_bucket/yellow_tripdata_2024-*.parquet']
);
```

**Create Materialized Table (non-partitioned, non-clustered):**
```sql
CREATE OR REPLACE TABLE `project.dataset.yellow_taxi_materialized` AS
SELECT * FROM `project.dataset.yellow_taxi_external`;
```

---

## Question 1. Counting Records

**Question:** What is count of records for the 2024 Yellow Taxi Data?

**SQL Query:**
```sql
SELECT COUNT(*) as total_records
FROM `project.dataset.yellow_taxi_materialized`;
```

**Output:**
```
total_records
-------------
20,332,093
```

**Answer: 20,332,093**

---

## Question 2. Estimated Data for Distinct PULocationIDs

**Question:** Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables. What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

**SQL Query for External Table:**
```sql
SELECT COUNT(DISTINCT PULocationID)
FROM `project.dataset.yellow_taxi_external`;
```

**SQL Query for Materialized Table:**
```sql
SELECT COUNT(DISTINCT PULocationID)
FROM `project.dataset.yellow_taxi_materialized`;
```

**Analysis:**

| Table Type | Estimated Bytes |
|------------|-----------------|
| External Table | 0 MB (BigQuery cannot estimate for external tables) |
| Materialized Table | 155.12 MB |

**Explanation:**
- **External Table:** BigQuery shows 0 MB estimated because it cannot determine the data size for external tables without actually reading the data. The metadata about data size is not available for external sources.
- **Materialized Table:** BigQuery has full metadata about the stored data, so it can accurately estimate 155.12 MB will be processed.

**Answer: 0 MB for the External Table and 155.12 MB for the Materialized Table**

---

## Question 3. Why Are Estimated Bytes Different?

**Question:** Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?

**SQL Query - Single Column:**
```sql
SELECT PULocationID
FROM `project.dataset.yellow_taxi_materialized`;
-- Estimated: ~155 MB
```

**SQL Query - Two Columns:**
```sql
SELECT PULocationID, DOLocationID
FROM `project.dataset.yellow_taxi_materialized`;
-- Estimated: ~310 MB
```

**Analysis:**

| Query | Columns | Estimated Bytes |
|-------|---------|-----------------|
| Single column | PULocationID | ~155 MB |
| Two columns | PULocationID, DOLocationID | ~310 MB |

**Explanation:**
BigQuery uses a **columnar storage format**. This means:
- Data is stored column by column, not row by row
- When you query one column, only that column's data is read
- When you query two columns, both columns' data must be read
- This results in roughly double the bytes processed

**Answer: BigQuery is columnar; querying two columns requires reading more data than one column, resulting in higher estimated bytes processed.**

---

## Question 4. Records with fare_amount of 0

**Question:** How many records have a fare_amount of 0?

**SQL Query:**
```sql
SELECT COUNT(*) as zero_fare_count
FROM `project.dataset.yellow_taxi_materialized`
WHERE fare_amount = 0;
```

**Output:**
```
zero_fare_count
---------------
128,210
```

**Answer: 128,210**

---

## Question 5. Best Strategy for Optimized Table

**Question:** What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID?

**Analysis of Options:**

| Strategy | Filter Performance | Order Performance | Verdict |
|----------|-------------------|-------------------|---------|
| Partition by tpep_dropoff_datetime, Cluster on VendorID | ✅ Excellent (prunes partitions) | ✅ Good (pre-sorted within partitions) | **OPTIMAL** |
| Cluster on tpep_dropoff_datetime, Cluster on VendorID | ⚠️ Good | ⚠️ Good | Suboptimal |
| Cluster on tpep_dropoff_datetime, Partition by VendorID | ⚠️ Good | ❌ Poor (VendorID has few values) | Not recommended |
| Partition by both | ❌ Invalid | ❌ Invalid | Not possible |

**Best Practice:**
- **Partition by:** Columns used in WHERE clauses for filtering (especially date/time columns)
- **Cluster by:** Columns used in ORDER BY or additional filtering

**SQL to Create Optimized Table:**
```sql
CREATE OR REPLACE TABLE `project.dataset.yellow_taxi_partitioned_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT * FROM `project.dataset.yellow_taxi_materialized`;
```

**Answer: Partition by tpep_dropoff_datetime and Cluster on VendorID**

---

## Question 6. Comparing Materialized vs Partitioned Table

**Question:** Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive). Use the materialized table and the partitioned table. What are the estimated bytes?

**SQL Query for Materialized Table (non-partitioned):**
```sql
SELECT DISTINCT VendorID
FROM `project.dataset.yellow_taxi_materialized`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
-- Estimated: 310.24 MB
```

**SQL Query for Partitioned Table:**
```sql
SELECT DISTINCT VendorID
FROM `project.dataset.yellow_taxi_partitioned_clustered`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
-- Estimated: 26.84 MB
```

**Comparison:**

| Table Type | Estimated Bytes | Explanation |
|------------|-----------------|-------------|
| Materialized (non-partitioned) | 310.24 MB | Scans entire table |
| Partitioned + Clustered | 26.84 MB | Only scans relevant partitions (March 1-15) |

**Cost Savings:**
- Reduction: 310.24 MB → 26.84 MB
- Savings: ~91% less data processed
- This directly translates to lower query costs in BigQuery

**Answer: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table**

---

## Question 7. External Table Data Storage Location

**Question:** Where is the data stored in the External Table you created?

**Analysis:**

| Option | Description | Correct? |
|--------|-------------|----------|
| Big Query | Native BQ storage | ❌ |
| Container Registry | Docker images storage | ❌ |
| GCP Bucket | Cloud Storage | ✅ |
| Big Table | NoSQL database | ❌ |

**Explanation:**
An **External Table** in BigQuery is a table definition that references data stored outside of BigQuery's native storage. When we created the external table using:

```sql
CREATE EXTERNAL TABLE ... OPTIONS (uris = ['gs://bucket/...'])
```

The data remains in Google Cloud Storage (GCS bucket). BigQuery only stores the table schema and metadata, not the actual data.

**Answer: GCP Bucket**

---

## Question 8. Clustering Best Practice

**Question:** It is best practice in Big Query to always cluster your data:

**Analysis:**

| Scenario | Cluster? | Reason |
|----------|----------|--------|
| Large tables (>1 GB) with frequent filtered queries | ✅ Yes | Improves performance and reduces costs |
| Small tables (<1 GB) | ❌ No | Overhead exceeds benefits |
| Tables with highly random access patterns | ❌ No | Clustering won't help |
| Tables that are mostly appended, rarely queried | ❌ No | Maintenance cost outweighs benefits |

**Explanation:**
Clustering is **NOT** always beneficial:
- Adds maintenance overhead (automatic re-clustering)
- Small tables don't benefit significantly
- If queries don't filter/order by clustered columns, no benefit
- Clustering has a cost during data ingestion

**Answer: False**

---

## Question 9 (Bonus). COUNT(*) on Materialized Table

**Question:** Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

**SQL Query:**
```sql
SELECT COUNT(*)
FROM `project.dataset.yellow_taxi_materialized`;
-- Estimated bytes: 0 B
```

**Output:**
```
Estimated bytes processed: 0 B
Result: 20,332,093
```

**Explanation:**
BigQuery estimates **0 bytes** because:

1. **Metadata Optimization:** BigQuery stores table metadata including the total row count
2. **No Data Scan Required:** For a simple `COUNT(*)` without any WHERE clause, BigQuery can return the result directly from metadata
3. **Columnar Storage Benefit:** Even if it needed to scan, it wouldn't read all columns

This is different from:
- `COUNT(column_name)` - needs to scan to check for NULLs
- `COUNT(*)` with WHERE clause - needs to scan filtered rows
- `COUNT(DISTINCT column)` - needs to scan and deduplicate

**Answer: 0 bytes. BigQuery stores metadata about the table including row counts, so it can return COUNT(*) results without scanning any actual data.**

---

## Summary of Answers

| Question | Answer |
|----------|--------|
| Q1: Record count | **20,332,093** |
| Q2: Estimated data for distinct PULocationIDs | **0 MB for External Table, 155.12 MB for Materialized Table** |
| Q3: Why estimated bytes differ | **BigQuery is columnar; querying two columns requires reading more data than one column** |
| Q4: Records with fare_amount = 0 | **128,210** |
| Q5: Best optimization strategy | **Partition by tpep_dropoff_datetime and Cluster on VendorID** |
| Q6: Materialized vs Partitioned bytes | **310.24 MB for non-partitioned, 26.84 MB for partitioned** |
| Q7: External table data location | **GCP Bucket** |
| Q8: Always cluster data? | **False** |
| Q9: COUNT(*) estimated bytes | **0 bytes (metadata optimization)** |

---

## Submission

Submit answers at: https://courses.datatalks.club/de-zoomcamp-2025/homework/hw3
