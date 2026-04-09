# ============================================================
# 1. Import Required Modules
# ============================================================

# DLT / Spark Declarative Pipeline module
# Used to define tables and pipeline flows
import dlt as dp

# PySpark functions (transformations)
from pyspark.sql.functions import *

# PySpark data types (schema definition)
from pyspark.sql.types import *


# ============================================================
# 2. Define Schema for Streaming JSON Data
# ============================================================

# This schema is used to parse raw JSON (coming from Event Hub / Kafka)
# into structured columns

rides_schema = StructType([
    StructField('ride_id', StringType(), True),
    StructField('confirmation_number', StringType(), True),
    StructField('passenger_id', StringType(), True),
    StructField('driver_id', StringType(), True),
    StructField('vehicle_id', StringType(), True),
    StructField('pickup_location_id', StringType(), True),
    StructField('dropoff_location_id', StringType(), True),
    StructField('vehicle_type_id', LongType(), True),
    StructField('vehicle_make_id', LongType(), True),
    StructField('payment_method_id', LongType(), True),
    StructField('ride_status_id', LongType(), True),
    StructField('pickup_city_id', LongType(), True),
    StructField('dropoff_city_id', LongType(), True),
    StructField('cancellation_reason_id', LongType(), True),
    StructField('passenger_name', StringType(), True),
    StructField('passenger_email', StringType(), True),
    StructField('passenger_phone', StringType(), True),
    StructField('driver_name', StringType(), True),
    StructField('driver_rating', DoubleType(), True),
    StructField('driver_phone', StringType(), True),
    StructField('driver_license', StringType(), True),
    StructField('vehicle_model', StringType(), True),
    StructField('vehicle_color', StringType(), True),
    StructField('license_plate', StringType(), True),
    StructField('pickup_address', StringType(), True),
    StructField('pickup_latitude', DoubleType(), True),
    StructField('pickup_longitude', DoubleType(), True),
    StructField('dropoff_address', StringType(), True),
    StructField('dropoff_latitude', DoubleType(), True),
    StructField('dropoff_longitude', DoubleType(), True),
    StructField('distance_miles', DoubleType(), True),
    StructField('duration_minutes', LongType(), True),
    StructField('booking_timestamp', TimestampType(), True),
    StructField('pickup_timestamp', TimestampType(), True),
    StructField('dropoff_timestamp', TimestampType(), True),
    StructField('base_fare', DoubleType(), True),
    StructField('distance_fare', DoubleType(), True),
    StructField('time_fare', DoubleType(), True),
    StructField('surge_multiplier', DoubleType(), True),
    StructField('subtotal', DoubleType(), True),
    StructField('tip_amount', DoubleType(), True),
    StructField('total_fare', DoubleType(), True),
    StructField('rating', DoubleType(), True)
])


# ============================================================
# 3. Create Staging Streaming Table (Silver Layer Entry Point)
# ============================================================

# This creates an EMPTY streaming table
# All data (batch + streaming) will be appended into this table

dp.create_streaming_table("stg_rides")


# ============================================================
# 4. Bulk / Initial Data Load (One-Time Load)
# ============================================================

# Append Flow → Used to insert data into streaming table
# This runs once for historical/bulk data

@dp.append_flow(
    target="stg_rides"
)
def rides_bulk():

    
    # This should ideally be spark.read.table() because bulk is batch data

    df = spark.read.table("bulk_rides")

    # Convert timestamp columns (ensure correct datatype)
    df = df.withColumn("booking_timestamp", col("booking_timestamp").cast("timestamp"))
    df = df.withColumn("pickup_timestamp", col("pickup_timestamp").cast("timestamp"))
    df = df.withColumn("dropoff_timestamp", col("dropoff_timestamp").cast("timestamp"))

    return df


# ============================================================
# 5. Streaming Data (Incremental Load)
# ============================================================

# This continuously appends new real-time data into stg_rides

@dp.append_flow(
    target="stg_rides"
)
def rides_stream():

    # Read streaming table created in Bronze layer
    df = spark.readStream.table("rides_raw")

    # Parse JSON string (rides column) into structured columns using schema
    df_parsed = df.withColumn(
        'parsed_rides',
        from_json(col("rides"), rides_schema)
    ).select("parsed_rides.*")

    return df_parsed