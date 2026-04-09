# Import DLT / Spark Declarative Pipeline module
# Used to define tables and pipeline logic
import dlt as dp

# Import PySpark functions (for transformations)
from pyspark.sql.functions import *

# Import PySpark data types (for casting, schema handling)
from pyspark.sql.types import *


# ============================================================
# 1.  Event Hub Configuration
# ============================================================

# Event Hub namespace → acts like Kafka cluster
EH_NAMESPACE = "hem-uberevents"

# Event Hub name → acts like Kafka topic
EH_NAME = "ubertopic"

# Securely fetch connection string from pipeline config
# (Do NOT hardcode secrets in code)
EH_CONN_STR = spark.conf.get("Connection_String")


# ============================================================
# 2. Kafka Consumer Configuration (Event Hub uses Kafka API)
# ============================================================

KAFKA_OPTIONS = {

    # Kafka broker endpoint (Event Hub namespace URL)
    "kafka.bootstrap.servers": f"{EH_NAMESPACE}.servicebus.windows.net:9093",

    # Topic to subscribe (Event Hub name)
    "subscribe": EH_NAME,

    # Authentication mechanism
    "kafka.sasl.mechanism": "PLAIN",

    # Security protocol (SSL encryption)
    "kafka.security.protocol": "SASL_SSL",

    # JAAS config → used to pass connection string securely
    "kafka.sasl.jaas.config": f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{EH_CONN_STR}";',

    # Timeout settings for stability
    "kafka.request.timeout.ms": "10000",
    "kafka.session.timeout.ms": "10000",

    # Maximum records processed per micro-batch
    "maxOffsetsPerTrigger": "10000",

    # Continue processing even if some data is lost
    "failOnDataLoss": "true",

    # Start reading from earliest available data
    "startingOffsets": "earliest"
}


# ============================================================
# 3.  Bronze Layer Streaming Table (Raw Ingestion)
# ============================================================
# create rides_raw() streaming table in DataBricks Catalog 

@dp.table
def rides_raw():

    # Step 1: Read real-time streaming data from Event Hub using Kafka protocol
    df = spark.readStream \
        .format("kafka") \
        .options(**KAFKA_OPTIONS) \
        .load()

    # Step 2: Kafka stores message value in binary format
    # Convert binary → string for readability and further processing
    df = df.withColumn("rides", col("value").cast(StringType()))

    # NOTE:
    # - 'key', 'value', 'topic', 'partition', 'offset', 'timestamp' are default Kafka columns
    # - We are extracting only 'value' for business data

    # Step 3: Return dataframe → automatically becomes a streaming table in SDP
    return df