# Data Engineering Course 

Code for Startdataengineering's [Data Engineering Course Sample]()

## Setup 

**Prerequistites**

1. [git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
2. [Docker](https://docs.docker.com/engine/install/) and [Docker Compose](https://docs.docker.com/compose/install/)

Start the containers with 

```bash 
docker compose up -d --build
sleep 30 # sleep 30 seconds to wait for the container and its services to fully start

```

Start the course by opening Jupyter Lab at [http://localhost:8888/](http://localhost:8888/)

Open Airflow UI at [http://localhost:8080/](http://localhost:8080/) 

## Spin Down

Once you are done, stop the containers with the following command.

```bash
docker compose down -v
```
