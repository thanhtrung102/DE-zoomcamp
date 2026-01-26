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
