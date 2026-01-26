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

**Given docker-compose.yaml:**
```yaml
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```

**Question:** What is the hostname and port that pgadmin should use to connect to the postgres database?

**Answer: db:5432**

**Explanation:**
- In docker-compose, containers communicate using the **service name** as hostname → `db`
- The **internal port** (right side of port mapping `5433:5432`) is used → `5432`
- `5433` is only for host machine access, not container-to-container communication
