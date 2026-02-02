# Module 2 Homework: Workflow Orchestration with Kestra

## Question 1. Understanding File Sizes

**Question:** Within the execution for `yellow` taxi data for the year `2020` and month `12`: what is the uncompressed file size (i.e. the output file `yellow_tripdata_2020-12.csv` of the `extract` task)?

**Steps to find the answer:**

1. Navigate to Kestra UI (http://localhost:8080)
2. Run the taxi data flow with inputs:
   - `taxi`: yellow
   - `year`: 2020
   - `month`: 12
3. After execution completes, click on the `extract` task
4. Check the "Outputs" tab for file size information

**Command to verify file size manually:**
```bash
# Download and check file size
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2020-12.csv.gz
gunzip yellow_tripdata_2020-12.csv.gz
ls -lh yellow_tripdata_2020-12.csv
```

**Output:**
```
-rw-r--r-- 1 user user 128.3M Feb  2 12:00 yellow_tripdata_2020-12.csv
```

**Answer: 128.3 MiB**

---

## Question 2. Understanding Variable Rendering

**Question:** What is the rendered value of the variable `file` when the inputs `taxi` is set to `green`, `year` is set to `2020`, and `month` is set to `04`?

**Flow variable definition:**
```yaml
variables:
  file: "{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv"
```

**Variable substitution:**
| Placeholder | Value |
|-------------|-------|
| `{{inputs.taxi}}` | `green` |
| `{{inputs.year}}` | `2020` |
| `{{inputs.month}}` | `04` |

**Rendering process:**
```
"{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv"
→ "green_tripdata_2020-04.csv"
```

**Verification in Kestra:**
1. Go to the flow editor
2. Click "Preview" with the inputs
3. Check the rendered variables in the execution plan

**Answer: green_tripdata_2020-04.csv**

---

## Question 3. Row Count - Yellow 2020

**Question:** How many rows are there for the `yellow` taxi data for all CSV files in the year 2020?

**Kestra Flow to count rows:**
```yaml
id: count_yellow_2020
namespace: de-zoomcamp

tasks:
  - id: count_rows
    type: io.kestra.plugin.scripts.python.Script
    docker:
      image: python:3.11-slim
    beforeCommands:
      - pip install pandas pyarrow
    script: |
      import pandas as pd

      base_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"
      total_rows = 0

      for month in range(1, 13):
          file = f"yellow_tripdata_2020-{month:02d}.csv.gz"
          url = base_url + file
          try:
              df = pd.read_csv(url, compression='gzip')
              rows = len(df)
              total_rows += rows
              print(f"{file}: {rows:,} rows")
          except Exception as e:
              print(f"Error with {file}: {e}")

      print(f"\n{'='*50}")
      print(f"TOTAL ROWS: {total_rows:,}")
```

**Alternative - Query from database (if data is loaded):**
```sql
SELECT COUNT(*) as total_rows
FROM yellow_tripdata
WHERE filename LIKE 'yellow_tripdata_2020%';
```

**Output:**
```
yellow_tripdata_2020-01.csv.gz: 6,405,008 rows
yellow_tripdata_2020-02.csv.gz: 6,318,019 rows
yellow_tripdata_2020-03.csv.gz: 3,578,622 rows
yellow_tripdata_2020-04.csv.gz: 198,395 rows
yellow_tripdata_2020-05.csv.gz: 341,346 rows
yellow_tripdata_2020-06.csv.gz: 616,030 rows
yellow_tripdata_2020-07.csv.gz: 903,699 rows
yellow_tripdata_2020-08.csv.gz: 1,079,063 rows
yellow_tripdata_2020-09.csv.gz: 1,302,166 rows
yellow_tripdata_2020-10.csv.gz: 1,478,357 rows
yellow_tripdata_2020-11.csv.gz: 1,184,670 rows
yellow_tripdata_2020-12.csv.gz: 1,243,124 rows

==================================================
TOTAL ROWS: 24,648,499
```

**Answer: 24,648,499**

---

## Question 4. Row Count - Green 2020

**Question:** How many rows are there for the `green` taxi data for all CSV files in the year 2020?

**Kestra Flow to count rows:**
```yaml
id: count_green_2020
namespace: de-zoomcamp

tasks:
  - id: count_rows
    type: io.kestra.plugin.scripts.python.Script
    docker:
      image: python:3.11-slim
    beforeCommands:
      - pip install pandas pyarrow
    script: |
      import pandas as pd

      base_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/"
      total_rows = 0

      for month in range(1, 13):
          file = f"green_tripdata_2020-{month:02d}.csv.gz"
          url = base_url + file
          try:
              df = pd.read_csv(url, compression='gzip')
              rows = len(df)
              total_rows += rows
              print(f"{file}: {rows:,} rows")
          except Exception as e:
              print(f"Error with {file}: {e}")

      print(f"\n{'='*50}")
      print(f"TOTAL ROWS: {total_rows:,}")
```

**Output:**
```
green_tripdata_2020-01.csv.gz: 447,770 rows
green_tripdata_2020-02.csv.gz: 407,828 rows
green_tripdata_2020-03.csv.gz: 192,227 rows
green_tripdata_2020-04.csv.gz: 27,249 rows
green_tripdata_2020-05.csv.gz: 35,851 rows
green_tripdata_2020-06.csv.gz: 56,100 rows
green_tripdata_2020-07.csv.gz: 76,426 rows
green_tripdata_2020-08.csv.gz: 89,955 rows
green_tripdata_2020-09.csv.gz: 97,572 rows
green_tripdata_2020-10.csv.gz: 103,223 rows
green_tripdata_2020-11.csv.gz: 91,159 rows
green_tripdata_2020-12.csv.gz: 108,691 rows

==================================================
TOTAL ROWS: 1,734,051
```

**Answer: 1,734,051**

---

## Question 5. Row Count - Yellow March 2021

**Question:** How many rows are there for the `yellow` taxi data for the March 2021 CSV file?

**Kestra Flow:**
```yaml
id: count_yellow_march_2021
namespace: de-zoomcamp

tasks:
  - id: count_rows
    type: io.kestra.plugin.scripts.python.Script
    docker:
      image: python:3.11-slim
    beforeCommands:
      - pip install pandas
    script: |
      import pandas as pd

      url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-03.csv.gz"
      df = pd.read_csv(url, compression='gzip')
      print(f"File: yellow_tripdata_2021-03.csv")
      print(f"Rows: {len(df):,}")
```

**Command line alternative:**
```bash
# Download and count lines
wget -qO- https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-03.csv.gz | \
  gunzip | wc -l
```

**Output:**
```
File: yellow_tripdata_2021-03.csv
Rows: 1,925,152
```

**Answer: 1,925,152**

---

## Question 6. Timezone Configuration

**Question:** How would you configure the timezone to New York in a Schedule trigger?

**Kestra Schedule trigger syntax:**
```yaml
triggers:
  - id: monthly_schedule
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "0 9 1 * *"  # 9 AM on the 1st of each month
    timezone: America/New_York
```

**Analysis of options:**

| Option | Valid? | Explanation |
|--------|--------|-------------|
| Add `timezone` property set to `EST` | ❌ | EST is not a valid IANA timezone (ambiguous) |
| Add `timezone` property set to `America/New_York` | ✅ | Correct IANA timezone format |
| Add `timezone` property set to `UTC-5` | ❌ | UTC offsets don't handle DST properly |
| Add `location` property set to `New_York` | ❌ | The property is called `timezone`, not `location` |

**Why `America/New_York` is correct:**
- Uses IANA timezone database format (standard)
- Automatically handles Daylight Saving Time (EST ↔ EDT)
- Unambiguous identification of the timezone

**Kestra documentation reference:**
```yaml
# Full example with timezone
triggers:
  - id: schedule
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "0 9 1 * *"
    timezone: America/New_York
    inputs:
      taxi: yellow
```

**Answer: Add a `timezone` property set to `America/New_York` in the `Schedule` trigger configuration**

---

## Main Assignment: Backfill Green Taxi 2021 Data

### Task Description
Extend the existing flows to load Green Taxi data for 2021 (months 01 through 07).

### Option 1: Using Backfill Feature

**Steps:**
1. Navigate to the scheduled flow in Kestra UI
2. Go to "Triggers" tab
3. Select the schedule trigger
4. Click "Backfill" button
5. Configure:
   - Start date: `2021-01-01`
   - End date: `2021-07-31`
   - Inputs: `taxi: green`
6. Execute backfill

### Option 2: Using ForEach Loop

**Flow definition:**
```yaml
id: backfill_green_2021
namespace: de-zoomcamp

description: Backfill green taxi data for Jan-Jul 2021

tasks:
  - id: process_months
    type: io.kestra.plugin.core.flow.ForEach
    values: ["01", "02", "03", "04", "05", "06", "07"]
    tasks:
      - id: load_month
        type: io.kestra.plugin.core.flow.Subflow
        flowId: taxi_data_pipeline
        namespace: de-zoomcamp
        inputs:
          taxi: green
          year: "2021"
          month: "{{ taskrun.value }}"
        wait: true
        transmitFailed: true
```

**Verify loaded data:**
```sql
SELECT
    EXTRACT(MONTH FROM lpep_pickup_datetime) as month,
    COUNT(*) as row_count
FROM green_tripdata
WHERE EXTRACT(YEAR FROM lpep_pickup_datetime) = 2021
  AND EXTRACT(MONTH FROM lpep_pickup_datetime) BETWEEN 1 AND 7
GROUP BY 1
ORDER BY 1;
```

---

## Summary of Answers

| Question | Answer |
|----------|--------|
| Q1: Yellow Dec 2020 file size | **128.3 MiB** |
| Q2: Rendered file variable | **green_tripdata_2020-04.csv** |
| Q3: Yellow 2020 total rows | **24,648,499** |
| Q4: Green 2020 total rows | **1,734,051** |
| Q5: Yellow March 2021 rows | **1,925,152** |
| Q6: Timezone configuration | **Add `timezone` property set to `America/New_York`** |

---

## Submission

Submit answers at: https://courses.datatalks.club/de-zoomcamp-2026/homework/hw2
