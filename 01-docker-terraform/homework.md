# Module 1 Homework: Docker & SQL

## Question 1. Understanding Docker images

**Command used:**
```bash
docker run -it --entrypoint bash python:3.13 -c "pip --version"
```

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
