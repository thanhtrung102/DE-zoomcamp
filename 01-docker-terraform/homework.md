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


**Answer: db:5432**

**Explanation:**
- In docker-compose, containers communicate using the **service name** as hostname → `db`
- The **internal port** (right side of port mapping `5433:5432`) is used → `5432`
- `5433` is only for host machine access, not container-to-container communication
