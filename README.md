# Weather ETL Pipeline with Astro and Airflow

This project demonstrates an ETL (Extract, Transform, Load) pipeline using Apache Airflow managed by Astro. The pipeline extracts current weather data from the Open-Meteo API, transforms the data, and loads it into a PostgreSQL database. The entire setup is containerized using Docker, making it easy to deploy and manage.

## Prerequisites

- [Astro CLI](https://astro.build) installed on your local machine.
- [Docker](https://www.docker.com) installed and running.
- A database management tool like [DBeaver](https://dbeaver.io/) for verifying data in PostgreSQL.

## Installation

### 1. Clone the repository:
```bash
git clone https://github.com/nrnavaneet/airflow-weather-etl
cd airflow-weather-etl
```
2. Initialize and start the Astro development environment:
```bash

astro dev init
astro dev start
```
This will set up the Airflow environment with the DAGs and configurations.

Configuring Airflow Connections

1. Access the Airflow UI:

After starting the Astro development environment, access the Airflow dashboard at http://localhost:8080.

2. Set up the PostgreSQL connection:
	1.	Go to Admin > Connections.
	2.	Click on Create.
	3.	Fill in the following details:
	•	Conn Id: postgres_default
	•	Conn Type: Postgres
	•	Host: postgres_db (or the service name defined in docker-compose.yml)
	•	Schema: postgres
	•	Login: postgres
	•	Password: postgres
	•	Port: 5432

3. Set up the HTTP connection for Open-Meteo API:
	1.	Go to Admin > Connections.
	2.	Click on Create.
	3.	Fill in the following details:
	•	Conn Id: open_meteo_api
	•	Conn Type: HTTP
	•	Host: https://api.open-meteo.com

Running the DAG

1. Enable the DAG:

In the Airflow UI, go to DAGs. Find the weather_etl_pipeline DAG and toggle it to On.

2. Trigger the DAG manually (optional):
	•	Click on the DAG name.
	•	Click on the Trigger DAG button to run it immediately.

The DAG is scheduled to run daily, but you can trigger it manually for testing purposes.

Verifying the Data

You can use DBeaver or any other database management tool to connect to the PostgreSQL database and verify that the weather data is being loaded correctly.

1. Connect to the database:
	•	Host: localhost
	•	Port: 5432
	•	Database: postgres
	•	Username: postgres
	•	Password: postgres

2. Query the weather_data table:

SELECT * FROM weather_data;

This should display the weather data entries loaded by the ETL pipeline.

DAG Overview

The weather_etl_pipeline DAG consists of three tasks:
	1.	extract_weather_data: Fetches current weather data from the Open-Meteo API using the provided latitude and longitude.
	2.	transform_weather_data: Processes the raw weather data into a structured format suitable for database insertion.
	3.	load_weather_data: Inserts the transformed weather data into the weather_data table in the PostgreSQL database.

The tasks are executed sequentially, ensuring that data is properly extracted, transformed, and loaded.

License

This project is licensed under the MIT License. See the LICENSE file for details.
