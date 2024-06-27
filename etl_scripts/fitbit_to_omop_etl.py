from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ConceptEntry") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18") \
    .getOrCreate()

# Define concept entries
concepts = [
    (8507, "Male", "Gender", "OMOP Vocabulary", "Gender", None, "M", "1970-01-01", "2099-12-31"),
    (8532, "Female", "Gender", "OMOP Vocabulary", "Gender", None, "F", "1970-01-01", "2099-12-31"),
    (8527, "White", "Race", "OMOP Vocabulary", "Race", None, "W", "1970-01-01", "2099-12-31"),
    (8516, "Black or African American", "Race", "OMOP Vocabulary", "Race", None, "B", "1970-01-01", "2099-12-31"),
    (8515, "Asian", "Race", "OMOP Vocabulary", "Race", None, "A", "1970-01-01", "2099-12-31"),
    (38003563, "Hispanic or Latino", "Ethnicity", "OMOP Vocabulary", "Ethnicity", None, "HL", "1970-01-01", "2099-12-31"),
    (38003564, "Not Hispanic or Latino", "Ethnicity", "OMOP Vocabulary", "Ethnicity", None, "NHL", "1970-01-01", "2099-12-31")
]

# Create a DataFrame
columns = ["concept_id", "concept_name", "domain_id", "vocabulary_id", "concept_class_id", "standard_concept", "concept_code", "valid_start_date", "valid_end_date"]
concept_df = spark.createDataFrame(concepts, columns)

# Write to PostgreSQL (OMOP schema)
jdbc_url = "jdbc:postgresql://postgres-omop:5432/omop"
connection_properties = {
    "user": "postgres",
    "password": "mysecretpassword",
    "driver": "org.postgresql.Driver"
}

concept_df.write.jdbc(
    url=jdbc_url,
    table="cdmdatabaseschema.concept",
    mode="append",
    properties=connection_properties
)

spark.stop()
