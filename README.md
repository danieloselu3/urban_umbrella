# Urban Umbrella 
A healthcare project building analytic platform using DBT and Postgres, while leveraging Astro for orchestration



<!-- `
Astro dbt setup
`

Astro dev init - to initialize an empty astro project (do this if you have astro cli installed in your machine)
astro dev init


In the dags folder of the initialized folders, create a DBT folder the cd into the folder.
run dbt init to initialize an empty dbt project.

dbt init


Configure astro
In the requirements.txt, add the below code this will install astronomer in the docker environment we’ll create later

astronomer-cosmos[dbt.postgres]

In the packages.txt, add the below packages
gcc
python3-dev


Create a new file docker-compose.override.yaml add the below code. This enables the changes we make in the dbt folder to sync in the docker container we’ll be running

version: "3.1"
services:
  scheduler:
    volumes:
      - ./dbt:/usr/local/airflow/dbt:rw


  webserver:
    volumes:
      - ./dbt:/usr/local/airflow/dbt:rw


  triggerer:
    volumes:
      - ./dbt:/usr/local/airflow/dbt:rw






In the Dockerfile, add the code below to create a virtual environment for DBT. this helps prevent environment dependencies conflicts between dbt and airflow/astro

WORKDIR "/usr/local/airflow"
COPY dbt-requirements.txt ./
RUN python -m virtualenv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir -r dbt-requirements.txt && deactivate




Do not forget to create the dbt-requirements.txt file where we’ll add dependencies to be install in the venv we created in the previous step. In that dbt_requirements.txt file, add the below packages
dbt-core==1.3.1
dbt-postgres==1.3.1


After this setup is done, run the code below to start astro, and give it some time until the Airflow start up (while running the astro and dbt commands make sure you are in the root folder for the respective tools)

astro dev start


Your Airflow app should be up and running. User credentials login:admin password:admin to login, and you’ll see the astro default dat named exampleastronauts.

Connecting to postgres

We’ll need to connect to postgres from two places: Airflow and from dbt. With the right configurations, we’ll ensure that during development, our dbt commands can affect changes into the postgres instance in our docker, and airflow can orchestrate changes into the postgres database.

alpha_astro:
  outputs:
    dev:
      dbname: postgres     # project name
      host: 127.0.0.1
      pass: postgres
      port: 5432
      schema: do        # your name initials
      threads: 1
      type: postgres
      user: postgres
  target: dev
Creating our dag
Create an python alphadag.py file in the root of dags/ folder.
 -->

