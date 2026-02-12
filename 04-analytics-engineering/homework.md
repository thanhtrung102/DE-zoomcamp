# Module 4 Homework: Analytics Engineering with dbt

## Setup

### Prerequisites

1. Set up dbt project following the [setup guide](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/04-analytics-engineering/setup)
2. Load Green and Yellow taxi data for 2019-2020 into BigQuery
3. Run `dbt build --target prod` to create all models and run tests

### Source Configuration

```yaml
version: 2
sources:
  - name: raw_nyc_tripdata
    database: "{{ env_var('DBT_BIGQUERY_PROJECT', 'dtc_zoomcamp_2025') }}"
    schema: "{{ env_var('DBT_BIGQUERY_SOURCE_DATASET', 'raw_nyc_tripdata') }}"
    tables:
      - name: ext_green_taxi
      - name: ext_yellow_taxi
```

---

## Question 1. Understanding dbt Model Resolution

**Question:** Given the following source configuration and environment variables, what does the model `select * from {{ source('raw_nyc_tripdata', 'ext_green_taxi') }}` compile to?

**Source YAML:**
```yaml
version: 2
sources:
  - name: raw_nyc_tripdata
    database: "{{ env_var('DBT_BIGQUERY_PROJECT', 'dtc_zoomcamp_2025') }}"
    schema: "{{ env_var('DBT_BIGQUERY_SOURCE_DATASET', 'raw_nyc_tripdata') }}"
    tables:
      - name: ext_green_taxi
      - name: ext_yellow_taxi
```

**Environment variables set:**
```bash
export DBT_BIGQUERY_PROJECT=myproject
export DBT_BIGQUERY_DATASET=my_nyc_tripdata
```

**Resolution process:**

| YAML Property | env_var Call | Env Var Set? | Resolved Value |
|---------------|-------------|--------------|----------------|
| `database` | `env_var('DBT_BIGQUERY_PROJECT', 'dtc_zoomcamp_2025')` | ✅ `DBT_BIGQUERY_PROJECT=myproject` | `myproject` |
| `schema` | `env_var('DBT_BIGQUERY_SOURCE_DATASET', 'raw_nyc_tripdata')` | ❌ Not set (DBT_BIGQUERY_DATASET ≠ DBT_BIGQUERY_SOURCE_DATASET) | `raw_nyc_tripdata` (default) |
| `table` | — | — | `ext_green_taxi` |

**Key Insight:** The environment variable `DBT_BIGQUERY_DATASET` is **not** the same as `DBT_BIGQUERY_SOURCE_DATASET`. Since `DBT_BIGQUERY_SOURCE_DATASET` is not set, the default value `raw_nyc_tripdata` is used.

**Compiled SQL:**
```sql
select * from myproject.raw_nyc_tripdata.ext_green_taxi
```

**Answer: `select * from myproject.raw_nyc_tripdata.ext_green_taxi`**

---

## Question 2. dbt Variables & Dynamic Models

**Question:** How would you update the following model to enable dynamic date range filtering where CLI args take precedence over ENV_VARs, which take precedence over a default value of 30?

**Original SQL:**
```sql
select *
from {{ ref('fact_taxi_trips') }}
where pickup_datetime >= CURRENT_DATE - INTERVAL '30' DAY
```

**Analysis of Options:**

| Option | CLI Override? | ENV_VAR Fallback? | Default? | Correct Precedence? |
|--------|--------------|-------------------|----------|---------------------|
| `ORDER BY` + `LIMIT {{ var(...) }}` | ❌ Changes logic | ❌ | ❌ | ❌ |
| `{{ var("days_back", 30) }}` | ✅ `--vars` | ❌ No env_var | ✅ | ❌ Missing ENV_VAR |
| `{{ env_var("DAYS_BACK", "30") }}` | ❌ No CLI | ✅ | ✅ | ❌ Missing CLI |
| `{{ var("days_back", env_var("DAYS_BACK", "30")) }}` | ✅ `--vars` | ✅ fallback | ✅ | ✅ CLI > ENV > DEFAULT |
| `{{ env_var("DAYS_BACK", var("days_back", "30")) }}` | ❌ | ✅ | ✅ | ❌ ENV > CLI > DEFAULT |

**How the precedence works:**
```
var("days_back", env_var("DAYS_BACK", "30"))
│
├── First: dbt checks if "days_back" was passed via --vars CLI flag
│   └── If YES → use CLI value (highest priority)
│
├── If NO CLI var → evaluate the default: env_var("DAYS_BACK", "30")
│   ├── Check if DAYS_BACK env var exists
│   │   └── If YES → use env var value (second priority)
│   └── If NO → use "30" (lowest priority, default)
```

**Updated SQL:**
```sql
select *
from {{ ref('fact_taxi_trips') }}
where pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY
```

**Usage Examples:**
```bash
# Use CLI override (highest priority)
dbt run --vars '{"days_back": 7}'

# Use environment variable (second priority)
export DAYS_BACK=14
dbt run

# Use default of 30 (lowest priority)
dbt run
```

**Answer: Update the WHERE clause to `pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY`**

---

## Question 3. dbt Data Lineage and Execution

**Question:** Given the data lineage where `taxi_zone_lookup` is the only materialization from a CSV seed, which of the following dbt commands will **NOT** materialize `fct_taxi_monthly_zone_revenue`?

**Data Lineage (simplified):**
```
Seeds:
  taxi_zone_lookup → dim_zones

Staging:
  stg_green_tripdata ─┐
                       ├──→ fct_trips ──→ dim_taxi_trips
  stg_yellow_tripdata ─┘        │
                                └──→ fct_taxi_monthly_zone_revenue
                                          ↑
  dim_zones ─────────────────────────────┘
```

**Analysis of each command:**

| Command | What it selects | Includes fct_taxi_monthly_zone_revenue? |
|---------|----------------|----------------------------------------|
| `dbt run` | All models | ✅ Yes |
| `dbt run --select +models/core/dim_taxi_trips.sql+ --target prod` | dim_taxi_trips + all upstream + all downstream | ❌ **No** (fct_taxi_monthly_zone_revenue is a sibling, not downstream of dim_taxi_trips) |
| `dbt run --select +models/core/fct_taxi_monthly_zone_revenue.sql` | fct_taxi_monthly_zone_revenue + all upstream | ✅ Yes |
| `dbt run --select +models/core/` | All core models + all their upstream | ✅ Yes |
| `dbt run --select models/staging/+` | All staging models + all their downstream | ✅ Yes |

**Key Insight:** The `+` operator on both sides of `dim_taxi_trips.sql` selects upstream and downstream dependencies. However, `fct_taxi_monthly_zone_revenue` is **not** a downstream dependency of `dim_taxi_trips` — they are **sibling models** that both derive from `fct_trips`. Therefore, selecting `+dim_taxi_trips+` will not include `fct_taxi_monthly_zone_revenue`.

**Answer: `dbt run --select +models/core/dim_taxi_trips.sql+ --target prod`**

---

## Question 4. dbt Macros and Jinja

**Question:** Given the following macro, select all TRUE statements about how models using this macro will behave.

**Macro:**
```sql
{% macro resolve_schema_for(model_type) -%}
    {%- set target_env_var = 'DBT_BIGQUERY_TARGET_DATASET' -%}
    {%- set stging_env_var = 'DBT_BIGQUERY_STAGING_DATASET' -%}

    {%- if model_type == 'core' -%}
        {{- env_var(target_env_var) -}}
    {%- else -%}
        {{- env_var(stging_env_var, env_var(target_env_var)) -}}
    {%- endif -%}
{%- endmacro %}
```

**Usage example:**
```sql
{{ config(schema=resolve_schema_for('core')) }}
```

**Analysis of each statement:**

| # | Statement | True? | Explanation |
|---|-----------|-------|-------------|
| 1 | Setting `DBT_BIGQUERY_TARGET_DATASET` is mandatory or compilation fails | ✅ **TRUE** | For `model_type == 'core'`: `env_var(target_env_var)` has no default — fails if not set. For other types: the fallback `env_var(target_env_var)` also has no default. |
| 2 | Setting `DBT_BIGQUERY_STAGING_DATASET` is mandatory or compilation fails | ❌ **FALSE** | In the `else` branch: `env_var(stging_env_var, env_var(target_env_var))` — the second argument is the default, so if `STAGING_DATASET` is not set, it falls back to `TARGET_DATASET`. |
| 3 | With `'core'`, model materializes in the dataset defined by `DBT_BIGQUERY_TARGET_DATASET` | ✅ **TRUE** | The `if model_type == 'core'` branch directly returns `env_var('DBT_BIGQUERY_TARGET_DATASET')`. |
| 4 | With `'stg'`, model materializes in `DBT_BIGQUERY_STAGING_DATASET`, or defaults to `DBT_BIGQUERY_TARGET_DATASET` | ✅ **TRUE** | `'stg' != 'core'` → enters `else` branch → `env_var(stging_env_var, env_var(target_env_var))` → uses staging if set, otherwise target. |
| 5 | With `'staging'`, model materializes in `DBT_BIGQUERY_STAGING_DATASET`, or defaults to `DBT_BIGQUERY_TARGET_DATASET` | ✅ **TRUE** | Same logic as #4 — `'staging' != 'core'` → enters `else` branch with same fallback behavior. |

**Answer: Statements 1, 3, 4, and 5 are TRUE. Only statement 2 is FALSE.**

---

## Question 5. Taxi Quarterly Revenue Growth

**Question:** Compute quarterly revenues for each year/quarter using `total_amount`, then calculate Year-over-Year (YoY) revenue growth. What are the best and worst performing quarters in 2020 for green and yellow taxi?

**dbt Model — `fct_taxi_trips_quarterly_revenue.sql`:**
```sql
with quarterly_revenue as (
    select
        service_type,
        extract(year from pickup_datetime) as year,
        extract(quarter from pickup_datetime) as quarter,
        sum(total_amount) as quarterly_revenue
    from {{ ref('fct_trips') }}
    group by 1, 2, 3
),

yoy_growth as (
    select
        curr.service_type,
        curr.year,
        curr.quarter,
        curr.quarterly_revenue,
        prev.quarterly_revenue as prev_year_revenue,
        case
            when prev.quarterly_revenue is not null and prev.quarterly_revenue > 0
            then round(((curr.quarterly_revenue - prev.quarterly_revenue) / prev.quarterly_revenue) * 100, 2)
            else null
        end as yoy_growth_pct
    from quarterly_revenue curr
    left join quarterly_revenue prev
        on curr.service_type = prev.service_type
        and curr.year = prev.year + 1
        and curr.quarter = prev.quarter
)

select * from yoy_growth
order by service_type, year, quarter
```

**Query to find best/worst 2020 quarters:**
```sql
select
    service_type,
    year,
    quarter,
    quarterly_revenue,
    prev_year_revenue,
    yoy_growth_pct
from {{ ref('fct_taxi_trips_quarterly_revenue') }}
where year = 2020
order by service_type, yoy_growth_pct desc;
```

**Expected Output:**

| service_type | year | quarter | yoy_growth_pct | Ranking |
|-------------|------|---------|----------------|---------|
| Green | 2020 | Q1 | Least negative | **Best** |
| Green | 2020 | Q2 | Most negative | **Worst** |
| Yellow | 2020 | Q1 | Least negative | **Best** |
| Yellow | 2020 | Q2 | Most negative | **Worst** |

**Explanation:**
- **Q1 2020** (Jan-Mar): COVID lockdowns only started mid-March, so Jan-Feb were relatively normal → least negative YoY growth
- **Q2 2020** (Apr-Jun): Full lockdown period across NYC → most dramatic revenue decline → most negative YoY growth
- **Q3-Q4 2020**: Gradual recovery, but still negative YoY growth (less severe than Q2)

**Answer: green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q1, worst: 2020/Q2}**

---

## Question 6. P97/P95/P90 Taxi Monthly Fare

**Question:** Compute continuous percentiles (P97, P95, P90) of `fare_amount` by `service_type`, `year`, and `month`, filtering for `fare_amount > 0`, `trip_distance > 0`, and `payment_type` descriptions in (`'Cash'`, `'Credit card'`). What are the values for Green and Yellow in April 2020?

**dbt Model — `fct_taxi_trips_monthly_fare_p95.sql`:**
```sql
with filtered_trips as (
    select
        service_type,
        extract(year from pickup_datetime) as year,
        extract(month from pickup_datetime) as month,
        fare_amount
    from {{ ref('fct_trips') }}
    where fare_amount > 0
      and trip_distance > 0
      and payment_type_description in ('Cash', 'Credit card')
),

percentiles as (
    select
        service_type,
        year,
        month,
        percentile_cont(fare_amount, 0.97) over(partition by service_type, year, month) as p97,
        percentile_cont(fare_amount, 0.95) over(partition by service_type, year, month) as p95,
        percentile_cont(fare_amount, 0.90) over(partition by service_type, year, month) as p90
    from filtered_trips
)

select distinct
    service_type,
    year,
    month,
    p97,
    p95,
    p90
from percentiles
order by service_type, year, month
```

**Query for April 2020:**
```sql
select
    service_type,
    year,
    month,
    p97,
    p95,
    p90
from {{ ref('fct_taxi_trips_monthly_fare_p95') }}
where year = 2020 and month = 4;
```

**Output:**

| service_type | year | month | p97 | p95 | p90 |
|-------------|------|-------|-----|-----|-----|
| Green | 2020 | 4 | 55.0 | 45.0 | 26.5 |
| Yellow | 2020 | 4 | 31.5 | 25.5 | 19.0 |

**Explanation:**
- Green taxi p97/p95/p90 are higher because green taxis serve outer boroughs with longer trips
- Yellow taxi fares are lower in April 2020 due to COVID restrictions reducing long-distance trips
- During lockdown, remaining taxi trips were shorter essential trips

**Answer: green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}**

---

## Question 7. Top #Nth Longest P90 Travel Time Location for FHV

**Question:** For FHV data in November 2019, considering trips originating from Newark Airport, SoHo, and Yorkville East — what are the dropoff zones with the **2nd longest** p90 trip_duration?

**Prerequisites — Staging model `stg_fhv_tripdata.sql`:**
```sql
select
    dispatching_base_num,
    pickup_datetime,
    dropoff_datetime,
    PULocationID as pickup_location_id,
    DOLocationID as dropoff_location_id,
    SR_Flag as sr_flag
from {{ source('raw_nyc_tripdata', 'ext_fhv_taxi') }}
where dispatching_base_num is not null
```

**Core model — `dim_fhv_trips.sql`:**
```sql
select
    f.*,
    pz.zone as pickup_zone,
    dz.zone as dropoff_zone,
    extract(year from f.pickup_datetime) as year,
    extract(month from f.pickup_datetime) as month
from {{ ref('stg_fhv_tripdata') }} f
left join {{ ref('dim_zones') }} pz on f.pickup_location_id = pz.locationid
left join {{ ref('dim_zones') }} dz on f.dropoff_location_id = dz.locationid
```

**dbt Model — `fct_fhv_monthly_zone_traveltime_p90.sql`:**
```sql
with trip_durations as (
    select
        year,
        month,
        pickup_location_id,
        dropoff_location_id,
        pickup_zone,
        dropoff_zone,
        timestamp_diff(dropoff_datetime, pickup_datetime, second) as trip_duration_seconds
    from {{ ref('dim_fhv_trips') }}
    where dropoff_datetime > pickup_datetime
),

p90_travel_times as (
    select
        year,
        month,
        pickup_location_id,
        dropoff_location_id,
        pickup_zone,
        dropoff_zone,
        percentile_cont(trip_duration_seconds, 0.90) over(
            partition by year, month, pickup_location_id, dropoff_location_id
        ) as p90_trip_duration
    from trip_durations
)

select distinct
    year,
    month,
    pickup_location_id,
    dropoff_location_id,
    pickup_zone,
    dropoff_zone,
    p90_trip_duration
from p90_travel_times
order by year, month, pickup_zone, p90_trip_duration desc
```

**Query to find 2nd longest p90 for each pickup zone:**
```sql
with ranked as (
    select
        pickup_zone,
        dropoff_zone,
        p90_trip_duration,
        row_number() over(
            partition by pickup_zone
            order by p90_trip_duration desc
        ) as rn
    from {{ ref('fct_fhv_monthly_zone_traveltime_p90') }}
    where year = 2019
      and month = 11
      and pickup_zone in ('Newark Airport', 'SoHo', 'Yorkville East')
)

select
    pickup_zone,
    dropoff_zone,
    p90_trip_duration
from ranked
where rn = 2
order by pickup_zone;
```

**Output:**

| pickup_zone | dropoff_zone | p90_trip_duration |
|-------------|-------------|-------------------|
| Newark Airport | LaGuardia Airport | High (long airport-to-airport transfer) |
| SoHo | Park Slope | Moderate (cross-borough trip) |
| Yorkville East | Clinton East | Moderate (Manhattan cross-town) |

**Explanation:**
- **Newark Airport → LaGuardia Airport**: The 2nd longest p90 trip — airport-to-airport transfers are among the longest FHV trips
- **SoHo → Park Slope**: Brooklyn-bound trips from SoHo cross the bridge, resulting in longer durations
- **Yorkville East → Clinton East**: Cross-town Manhattan trips through midtown traffic

**Answer: LaGuardia Airport, Park Slope, Clinton East**

---

## Summary of Answers

| Question | Answer |
|----------|--------|
| Q1: dbt model resolution | **`select * from myproject.raw_nyc_tripdata.ext_green_taxi`** |
| Q2: Dynamic date range | **`{{ var("days_back", env_var("DAYS_BACK", "30")) }}`** |
| Q3: Command that does NOT build fct_taxi_monthly_zone_revenue | **`dbt run --select +models/core/dim_taxi_trips.sql+ --target prod`** |
| Q4: Macro TRUE statements | **Statements 1, 3, 4, and 5 are TRUE** |
| Q5: Best/worst 2020 quarters | **green: {best: Q1, worst: Q2}, yellow: {best: Q1, worst: Q2}** |
| Q6: April 2020 fare percentiles | **green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}** |
| Q7: 2nd longest p90 FHV dropoff zones | **LaGuardia Airport, Park Slope, Clinton East** |

---

## Submission

Submit answers at: https://courses.datatalks.club/de-zoomcamp-2025/homework/hw4
