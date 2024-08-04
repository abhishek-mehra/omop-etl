from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PersonDataEntry") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18") \
    .getOrCreate()

# Read the CSV file into a Spark DataFrame
csv_file_path = "sample_person_data.csv"
person_df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

# Define PostgreSQL connection properties
jdbc_url = "jdbc:postgresql://postgres-omop:5432/omop"
connection_properties = {
    "user": "postgres",
    "password": "mysecretpassword",
    "driver": "org.postgresql.Driver"
}

# Write the data to PostgreSQL
person_df.write.jdbc(
    url=jdbc_url,
    table="cdmdatabaseschema.person",
    mode="append",
    properties=connection_properties
)

# Stop the Spark session
spark.stop()

print("Data written to PostgreSQL database")
