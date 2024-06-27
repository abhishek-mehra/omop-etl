AIM :
A project to understand OMOP data model

ETL - extract from CSV , trasnform the data for omop , load in postgress

Docker - to maintain a constant environment for development

MORE on DOCKER:
Containers for Apache spark, postgres and Docker network


Creating a dedicated Docker network (etl-network) for isolated and secure communication.

Running an Apache Spark container (spark-master) connected to this network.

Connecting an existing PostgreSQL container to the same network.

Copying necessary files (ETL script and CSV data) into the Spark container.

Executing the ETL process within the Spark container, transforming and loading data into PostgreSQL.