# Fabric notebook: Spark raw sink  (Eventstream Kafka endpoints -> bronze.telemetry_raw)
# Each "# %%" block is one notebook cell. Attach the notebook to the lh_fleet lakehouse (default).
# SECURITY: this notebook holds connection strings. Do not share it or commit it with real values.

# %% CELL 1 - settings (fill from your .env: KAFKA_<SIGNAL>_BOOTSTRAP / _TOPIC / _CONN_STR)
SOURCES = {
    "gps":         dict(bootstrap="<KAFKA_GPS_BOOTSTRAP>",         topic="<KAFKA_GPS_TOPIC>",         conn="<KAFKA_GPS_CONN_STR>"),
    "engine":      dict(bootstrap="<KAFKA_ENGINE_BOOTSTRAP>",      topic="<KAFKA_ENGINE_TOPIC>",      conn="<KAFKA_ENGINE_CONN_STR>"),
    "fuel":        dict(bootstrap="<KAFKA_FUEL_BOOTSTRAP>",        topic="<KAFKA_FUEL_TOPIC>",        conn="<KAFKA_FUEL_CONN_STR>"),
    "temperature": dict(bootstrap="<KAFKA_TEMPERATURE_BOOTSTRAP>", topic="<KAFKA_TEMPERATURE_TOPIC>", conn="<KAFKA_TEMPERATURE_CONN_STR>"),
    "device_status": dict(bootstrap="<KAFKA_DEVICE_STATUS_BOOTSTRAP>", topic="<KAFKA_DEVICE_STATUS_TOPIC>", conn="<KAFKA_DEVICE_STATUS_CONN_STR>"),
}
TARGET = "bronze.telemetry_raw"
CHECKPOINT_ROOT = "Files/checkpoints"   # relative to the default lakehouse
TRIGGER = "10 seconds"
# "latest": only messages sent after the stream starts (use this for real runs).
# "earliest": re-reads everything still kept in the topic, including old test traffic.
# When you change this, also use a NEW checkpoint folder (for example Files/checkpoints_run1).
STARTING_OFFSETS = "latest"

# %% CELL 2 - ONE TIME: drop the old test table (it has the wrong schema). Run once, then comment it out.
# spark.sql("DROP TABLE IF EXISTS bronze.telemetry_raw")

# %% CELL 3 - create the real Bronze table (section 7.1 of the guide)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    raw_value       STRING,
    source_stream   STRING,
    source_type     STRING,
    kafka_partition INT,
    kafka_offset    BIGINT,
    source_file     STRING,
    file_line       INT,
    arrival_time    TIMESTAMP,
    ingestion_time  TIMESTAMP,
    ingestion_date  DATE
) USING DELTA
PARTITIONED BY (ingestion_date)
""")

# %% CELL 4 - start the streams
from collections import defaultdict
from pyspark.sql import functions as F

# If several signals use the SAME bootstrap+topic, start ONE stream for them (otherwise rows are duplicated).
groups = defaultdict(list)
for sig, cfg in SOURCES.items():
    groups[(cfg["bootstrap"], cfg["topic"])].append(sig)


def start_stream(sigs):
    cfg = SOURCES[sigs[0]]
    name = sigs[0] if len(sigs) == 1 else None          # None = shared endpoint, signal_type tells them apart
    ckpt = f"{CHECKPOINT_ROOT}/raw_sink_{sigs[0] if len(sigs) == 1 else 'shared'}"
    jaas = ('org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="$ConnectionString" password="{cfg["conn"]}";')
    df = (spark.readStream.format("kafka")
          .option("kafka.bootstrap.servers", cfg["bootstrap"])
          .option("subscribe", cfg["topic"])
          .option("kafka.security.protocol", "SASL_SSL")
          .option("kafka.sasl.mechanism", "PLAIN")
          .option("kafka.sasl.jaas.config", jaas)
          .option("startingOffsets", STARTING_OFFSETS)
          .option("failOnDataLoss", "false")
          .load())
    out = df.select(
        F.col("value").cast("string").alias("raw_value"),            # exact text, malformed included
        F.lit(name).cast("string").alias("source_stream"),
        F.lit("STREAM").alias("source_type"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.lit(None).cast("string").alias("source_file"),
        F.lit(None).cast("int").alias("file_line"),
        F.col("timestamp").alias("arrival_time"),                     # time the platform received it
        F.current_timestamp().alias("ingestion_time"),
        F.to_date(F.current_timestamp()).alias("ingestion_date"),
    )
    return (out.writeStream.format("delta").outputMode("append")
            .option("checkpointLocation", ckpt)
            .trigger(processingTime=TRIGGER)
            .toTable(TARGET))


queries = [start_stream(sigs) for sigs in groups.values()]
print(f"Started {len(queries)} stream(s):", [q.name or q.id for q in queries])

# %% CELL 5 - check (run after the generator sends data)
# spark.sql("SELECT source_stream, COUNT(*) n, MIN(kafka_offset) min_off, MAX(kafka_offset) max_off FROM bronze.telemetry_raw GROUP BY source_stream").show()
# spark.sql("SELECT raw_value, arrival_time, ingestion_time FROM bronze.telemetry_raw ORDER BY ingestion_time DESC LIMIT 5").show(truncate=False)

# %% CELL 6 - stop everything (frees Spark capacity)
# for q in spark.streams.active:
#     q.stop()
