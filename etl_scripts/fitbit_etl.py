from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, unix_timestamp, from_unixtime, expr, coalesce, current_timestamp, current_date, monotonically_increasing_id
from pyspark.sql.types import IntegerType, FloatType, TimestampType, StringType, DecimalType, DateType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("FitbitToOMOP") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18") \
    .getOrCreate()

# Database connection parameters
db_params = {
    "host": "postgres-omop",
    "database": "omop",
    "user": "postgres",
    "password": "mysecretpassword"
}

try:
    # Read data from CSV
    df = spark.read.csv("fitbit_dummy_data.csv", header=True, inferSchema=True)

    # Create measurement table
    measurement = df.withColumn("measurement_datetime", 
                                from_unixtime(unix_timestamp(col("DateTime"), "yyyy-MM-dd'T'HH:mm:ss")))

    # Map activity types to measurement concepts
    measurement = measurement.withColumn("measurement_concept_id",
        when(col("Type") == "activities-steps", 4107762)
        .when(col("Type") == "activities-calories", 4119389)
        .when(col("Type") == "activities-heart", 4239408)
        .when(col("Type") == "activities-sleep", 4173682)
        .otherwise(0)
    )

    # Map unit concepts
    measurement = measurement.withColumn("unit_concept_id",
        when(col("Type") == "activities-steps", 8554)  # count
        .when(col("Type") == "activities-calories", 9073)  # calories
        .when(col("Type") == "activities-heart", 8541)  # beats per minute
        .when(col("Type") == "activities-sleep", 8555)  # minutes
        .otherwise(0)
    )

    measurement = measurement.select(
        monotonically_increasing_id().cast(IntegerType()).alias("measurement_id"),
        col("ParticipantID").cast(IntegerType()).alias("person_id"),
        col("measurement_concept_id").cast(IntegerType()),
        coalesce(col("InsertedDate").cast(DateType()), current_date()).alias("measurement_date"),
        col("measurement_datetime").cast(TimestampType()),
        lit(None).cast(StringType()).alias("measurement_time"),
        lit(32817).cast(IntegerType()).alias("measurement_type_concept_id"),  # Patient reported
        lit(None).cast(IntegerType()).alias("operator_concept_id"),
        coalesce(col("Value").cast(DecimalType(38,18)), lit(0.0)).alias("value_as_number"),
        coalesce(col("Level").cast(IntegerType()), lit(0)).alias("value_as_concept_id"),
        col("unit_concept_id").cast(IntegerType()),
        lit(None).cast(DecimalType(38,18)).alias("range_low"),
        lit(None).cast(DecimalType(38,18)).alias("range_high"),
        lit(0).cast(IntegerType()).alias("provider_id"),
        lit(0).cast(IntegerType()).alias("visit_occurrence_id"),
        lit(0).cast(IntegerType()).alias("visit_detail_id"),
        col("Type").cast(StringType()).alias("measurement_source_value"),
        lit(0).cast(IntegerType()).alias("measurement_source_concept_id"),
        lit("Unknown").cast(StringType()).alias("unit_source_value"),
        lit(0).cast(IntegerType()).alias("unit_source_concept_id"),
        coalesce(col("Mets").cast(StringType()), lit("Unknown")).alias("value_source_value"),
        lit(None).cast(IntegerType()).alias("measurement_event_id"),
        lit(None).cast(IntegerType()).alias("meas_event_field_concept_id")
    )

    # Write to PostgreSQL (OMOP schema)
    jdbc_url = "jdbc:postgresql://postgres-omop:5432/omop"
    connection_properties = {
        "user": "postgres",
        "password": "mysecretpassword",
        "driver": "org.postgresql.Driver"
    }

    measurement.write \
        .jdbc(url=jdbc_url, table="cdmdatabaseschema.measurement", mode="append", properties=connection_properties)

    print("Fitbit data ingestion completed successfully.")
except Exception as e:
    print(f"An error occurred during the Fitbit data ingestion process: {str(e)}")
finally:
    spark.stop()