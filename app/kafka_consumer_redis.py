import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, to_json, concat_ws
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

class KafkaSparkConsumer:
    def __init__(self, bootstrap_servers, topic_name, batch_size=10, redis_host='redis', redis_port='6379'):
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = topic_name
        self.batch_size = batch_size
        self.redis_host = redis_host
        self.redis_port = redis_port

        # Initialize Spark session with Redis JAR
        self.spark = self.create_spark_session()

        # Kafka structured streaming
        self.kafka_stream = self.create_kafka_stream()

        # Extract value column
        self.message_stream = self.kafka_stream.selectExpr("CAST(value AS STRING)")

    def create_spark_session(self):
        """Creates and returns a Spark session with Redis support."""
        return SparkSession.builder \
            .appName("KafkaSparkConsumer") \
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1," \
                    "com.redislabs:spark-redis_2.12:3.3.0") \
            .config("spark.redis.host", self.redis_host) \
            .config("spark.redis.port", self.redis_port) \
            .getOrCreate()

    def create_kafka_stream(self):
        """Creates and returns Kafka streaming DataFrame."""
        return self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap_servers) \
            .option("subscribe", self.topic_name) \
            .option("startingOffsets", "earliest") \
            .load()

    def process_data(self, df, batch_id):
        """Processes Kafka messages and saves data to Redis using Spark-Redis."""
        print(f"Processing batch_id = {batch_id}")

        # Convert to RDD to extract JSON
        data = df.rdd.map(lambda row: json.loads(row['value'])).collect()

        if not data:
            print("No valid data to process.")
            return

        # Convert JSON to DataFrame
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("user", StructType([
                StructField("name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("location", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("registered", StructType([
                    StructField("date", StringType(), True),
                    StructField("age", IntegerType(), True)
                ]), True),
                StructField("phone", StringType(), True),
                StructField("cell", StringType(), True),
                StructField("time", StringType(), True)
            ]), True)
        ])

        user_df = self.spark.createDataFrame(data, schema=schema)

        # Extract user fields
        user_df = user_df.select(
            col("user.name").alias("name"),
            col("user.phone").alias("phone"),
            col("user.email").alias("email"),
            col("user.location").alias("location"),
            col("user.registered.date").alias("registered_date"),
            col("user.registered.age").alias("registered_age"),
            col("user.cell").alias("cell"),
            col("user.time").alias("time")
        )

        # Ensure name and phone are not null before creating Redis key
        user_df = user_df.filter(col("name").isNotNull() & col("phone").isNotNull())

        # Create a Redis key by combining name and phone
        user_df = user_df.withColumn("redis_key", concat_ws("_", col("name"), col("phone")))

        # Convert remaining columns into JSON value
        user_df = user_df.withColumn("redis_value", to_json(struct(
            col("email"), col("location"), col("registered_date"), col("registered_age"),
            col("cell"), col("time")
        )))

        # Ensure that Redis key is not null before saving
        user_df = user_df.filter(col("redis_key").isNotNull())

        # Write DataFrame to Redis
        user_df.select("redis_key", "redis_value").write \
            .format("org.apache.spark.sql.redis") \
            .option("table", "user_data") \
            .option("key.column", "redis_key") \
            .mode("append") \
            .save()

        print("Batch saved to Redis.")

    def start_consumer(self):
        print("Kafka Consumer started on Spark Cluster...")

        # Process the streaming data
        query = (
            self.message_stream
            .writeStream
            .foreachBatch(self.process_data)
            .outputMode("append")
            .start()
        )

        query.awaitTermination()

    def stop(self):
        self.spark.stop()
        print("Spark session closed.")

# Usage example
if __name__ == "__main__":
    kafka_consumer = KafkaSparkConsumer(
        bootstrap_servers='kafka-0:9092',
        topic_name='sample_topic',
        batch_size=20,
        redis_host='redis',
        redis_port='6379'
    )
    try:
        kafka_consumer.start_consumer()
    except KeyboardInterrupt:
        kafka_consumer.stop()