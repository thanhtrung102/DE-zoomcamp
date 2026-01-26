# Module 1 Homework: Docker & SQL

## Question 1. Understanding Docker images

**Command used:**
```bash
docker run -it --entrypoint bash python:3.13 -c "pip --version"
```

**Command explanation:**
- `docker run` - Create and run a new container
- `-it` - Interactive mode with TTY (terminal) attached
- `--entrypoint bash` - Override the default entrypoint to use bash shell
- `python:3.13` - Use the official Python 3.13 image
- `-c "pip --version"` - Execute this command in bash and exit

**Output:**
```
pip 25.3 from /usr/local/lib/python3.13/site-packages/pip (python 3.13)
```

**Answer: 25.3**

## Question 2. Understanding Docker networking and docker-compose


**Options analysis:**

| Option | Hostname | Port | Valid? |
|--------|----------|------|--------|
| `postgres:5433` | ✅ container_name works | ❌ 5433 is host port | **INCORRECT** |
| `localhost:5432` | ❌ refers to pgadmin itself | - | **INCORRECT** |
| `db:5433` | ✅ service name works | ❌ 5433 is host port | **INCORRECT** |
| `postgres:5432` | ✅ container_name works | ✅ internal port | **CORRECT** |
| `db:5432` | ✅ service name works | ✅ internal port | **CORRECT** |

**Commands to verify:**
```bash
# Start the containers
docker-compose up -d

# Check network connectivity from pgadmin to postgres using service name
docker exec pgadmin ping -c 2 db

# Check network connectivity using container name
docker exec pgadmin ping -c 2 postgres

# Verify postgres is listening on port 5432 inside the container
docker exec postgres netstat -tlnp | grep 5432
```

**Explanation:**
- In docker-compose, containers on the same network can communicate using:
  - **Service name** (`db`) - the standard docker-compose way
  - **Container name** (`postgres`) - also works as Docker creates DNS entries for it
- Port `5432` is the **internal container port** (right side of `5433:5432`)
- Port `5433` is the **host port** - only accessible from the host machine, not between containers
- `localhost` from pgadmin would refer to the pgadmin container itself, not postgres

**Answer: db:5432** (or postgres:5432 - both are correct)

## Question 3. Counting short trips

**Data preparation:**
```bash
# Download green taxi trips data for November 2025
wget https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet

# Download taxi zone lookup data
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
```

**SQL Query:**
```sql
SELECT COUNT(*)
FROM green_taxi_trips
WHERE lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime < '2025-12-01'
  AND trip_distance <= 1;
```

**Python alternative:**
```python
import pandas as pd

df = pd.read_parquet('green_tripdata_2025-11.parquet')

count = df[
    (df['lpep_pickup_datetime'] >= '2025-11-01') &
    (df['lpep_pickup_datetime'] < '2025-12-01') &
    (df['trip_distance'] <= 1)
].shape[0]

print(f"Trips with distance <= 1 mile: {count}")
```

**Output:**
```
Total rows in dataset: 46,912
Date range: 2025-10-26 to 2025-12-01
Trips with distance <= 1 mile in November 2025: 8,007
```

**Answer: 8,007**

## Question 4. Longest trip for each day

**Question:** Which was the pickup day with the longest trip distance? (excluding trips >= 100 miles)

**SQL Query:**
```sql
SELECT DATE(lpep_pickup_datetime) as pickup_date, MAX(trip_distance) as max_distance
FROM green_taxi_trips
WHERE trip_distance < 100
  AND lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime < '2025-12-01'
GROUP BY DATE(lpep_pickup_datetime)
ORDER BY max_distance DESC
LIMIT 1;
```

**Python alternative:**
```python
import pandas as pd

df = pd.read_parquet('green_tripdata_2025-11.parquet')

# Filter trips with distance < 100 miles
df_filtered = df[
    (df['trip_distance'] < 100) &
    (df['lpep_pickup_datetime'] >= '2025-11-01') &
    (df['lpep_pickup_datetime'] < '2025-12-01')
]

# Find the trip with max distance
idx_max = df_filtered['trip_distance'].idxmax()
longest_trip = df_filtered.loc[idx_max]
print(f"Pickup date: {longest_trip['lpep_pickup_datetime']}")
print(f"Trip distance: {longest_trip['trip_distance']} miles")
```

**Output:**
```
Top 5 longest trips:
      lpep_pickup_datetime  trip_distance
      2025-11-14 15:36:27          88.03
      2025-11-20 12:28:02          73.84
      2025-11-23 10:12:18          45.26
      2025-11-22 02:07:07          40.16
      2025-11-15 14:12:35          39.81
```

**Answer: 2025-11-14** (longest trip: 88.03 miles)

## Question 5. Biggest pickup zone

**Question:** Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025?

**SQL Query:**
```sql
SELECT z.Zone, SUM(g.total_amount) as zone_total
FROM green_taxi_trips g
JOIN taxi_zones z ON g.PULocationID = z.LocationID
WHERE g.lpep_pickup_datetime >= '2025-11-18'
  AND g.lpep_pickup_datetime < '2025-11-19'
GROUP BY z.Zone
ORDER BY zone_total DESC
LIMIT 5;
```

**Python alternative:**
```python
import pandas as pd

df = pd.read_parquet('green_tripdata_2025-11.parquet')
zones = pd.read_csv('taxi_zone_lookup.csv')

# Filter for November 18th, 2025
df_nov18 = df[
    (df['lpep_pickup_datetime'] >= '2025-11-18') &
    (df['lpep_pickup_datetime'] < '2025-11-19')
]

# Join with zones
df_joined = df_nov18.merge(zones, left_on='PULocationID', right_on='LocationID')

# Group by zone and sum total_amount
zone_totals = df_joined.groupby('Zone')['total_amount'].sum().sort_values(ascending=False)
print(zone_totals.head(5))
```

**Output:**
```
Top 5 zones by total_amount on Nov 18:
Zone
East Harlem North              9281.92
East Harlem South              6696.13
Central Park                   2378.79
Washington Heights South       2139.05
Morningside Heights            2100.59
```

**Answer: East Harlem North** (total: $9,281.92)
