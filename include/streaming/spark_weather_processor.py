from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType
import json
import os
from google.cloud import pubsub_v1

# ── Configuration ───────────────
# These can be overridden by environment variables on Dataproc
PROJECT_ID = os.getenv("GCP_PROJECT", "yelp-production")
BQ_SILVER_DATASET = "silver"
BQ_GOLD_STREAMING_DATASET = "gold_streaming"
SUBSCRIPTION_ID = "weather-sub"

def main():
    # Initialize Spark Session (dependencies are managed by Dataproc Serverless runtime)
    spark = SparkSession.builder \
        .appName("YelpWeatherPulse") \
        .getOrCreate()

    # 2. Initialize Pub/Sub Subscriber once! (Reusing this prevents memory leaks)
    print("📡 Initializing Pub/Sub Subscriber Client...")
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    # 3. Define Weather Schema explicitly to prevent inference errors
    weather_schema = StructType([
        StructField("city", StringType(), True),
        StructField("temp_c", DoubleType(), True),
        StructField("condition", StringType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("api_timestamp", LongType(), True),
        StructField("ingest_timestamp", StringType(), True),
        StructField("source", StringType(), True)
    ])

    def process_micro_batch(batch_df, batch_id):
        """
        Custom micro-batch handler that pulls from Pub/Sub using Python SDK.
        """
        # Use the SparkSession associated with the batch
        spark = batch_df.sparkSession
        
        print(f"📥 Batch {batch_id}: Checking for new weather events...")
        
        try:
            # Pull a batch of messages (Synchronous Pull)
            response = subscriber.pull(
                request={"subscription": subscription_path, "max_messages": 100},
                timeout=5
            )
            
            if not response.received_messages:
                print("📭 No new messages.")
                return

            messages = []
            ack_ids = []
            for msg in response.received_messages:
                try:
                    payload = json.loads(msg.message.data.decode("utf-8"))
                    messages.append(payload)
                    ack_ids.append(msg.ack_id)
                except Exception as e:
                    print(f"⚠️ Failed to parse message: {e}")

            if not messages:
                return

            print(f"🔄 Processing {len(messages)} weather events...")
            
            # Pre-process to ensure float types (prevents DoubleType/Int mismatch)
            for m in messages:
                if 'temp_c' in m: m['temp_c'] = float(m['temp_c'])
                if 'wind_speed' in m: m['wind_speed'] = float(m['wind_speed'])

            # Convert Python list to Spark DataFrame with explicit schema
            weather_batch_df = spark.createDataFrame(messages, schema=weather_schema)
            
            # ── Slight Transformation Logic ────────────────────────────────
            # 1. Unit Conversion: Celsius to Fahrenheit
            # 2. Windy Flag: wind_speed > 20 km/h
            final_batch = weather_batch_df \
                .withColumn("temp_f", (F.col("temp_c") * 9/5) + 32) \
                .withColumn("is_windy", F.col("wind_speed") > 20) \
                .withColumn("_processed_at", F.current_timestamp())

            # 3. Sink to BigQuery
            print(f"📤 Writing micro-batch to Gold Layer (Pure Stream)...")
            final_batch.write \
                .format("bigquery") \
                .option("table", f"{PROJECT_ID}.{BQ_GOLD_STREAMING_DATASET}.weather_pulse") \
                .option("temporaryGcsBucket", "yelp-production-bronze") \
                .mode("append") \
                .save()

            # 4. Acknowledge Messages
            subscriber.acknowledge(request={"subscription": subscription_path, "ack_ids": ack_ids})
            print(f"✅ Batch {batch_id} complete.")

        except Exception as e:
            print(f"❌ Batch {batch_id} failed: {e}")

    # ── Main Streaming Loop ───────────────────────────────────────
    # We use a 'rate' source to trigger the master loop every N seconds
    trigger_df = spark.readStream \
        .format("rate") \
        .option("rowsPerSecond", 1) \
        .load() \
        .writeStream \
        .foreachBatch(process_micro_batch) \
        .trigger(processingTime='10 seconds') \
        .start()

    trigger_df.awaitTermination()

if __name__ == "__main__":
    main()
