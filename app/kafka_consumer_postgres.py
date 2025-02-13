import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

class KafkaSparkConsumer:
    def __init__(self, bootstrap_servers, topic_name, batch_size=10):
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = topic_name
        self.batch_size = batch_size

        # Initialize PostgreSQL connection details
        self.pg_url, self.pg_properties = self.get_postgres_connection()

        # Initialize Spark session
        self.spark = self.create_spark_session()

        # Kafka structured streaming
        self.kafka_stream = self.create_kafka_stream()

        # Extract value column
        self.message_stream = self.kafka_stream.selectExpr("CAST(value AS STRING)")

    def create_spark_session(self):
        """Creates and returns a Spark session."""
        return SparkSession.builder \
            .appName("KafkaSparkConsumer") \
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1," \
                    "org.postgresql:postgresql:42.5.0") \
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

    def get_postgres_connection(self):
        """Returns PostgreSQL connection details."""
        return "jdbc:postgresql://postgres:5432/userdb", {
            "user": "postgres",
            "password": "postgres",
            "driver": "org.postgresql.Driver"
        }

    def process_data(self, df, batch_id):
        """Processes Kafka messages and saves all columns to PostgreSQL."""
        print(f"Processing batch_id = {batch_id}")

        # Convert to RDD to extract JSON
        data = df.rdd.map(lambda row: json.loads(row['value'])).collect()

        if not data:
            print("No valid data to process.")
            return

        # Flattening JSON structure to fit PostgreSQL schema
        user_data = []
        for user in data:
            user_details = user.get("user", {})
            if user_details:
                user_data.append({
                    "id": user.get("id"),
                    "name": user_details.get("name"),
                    "email": user_details.get("email"),
                    "location": user_details.get("location"),
                    "age": user_details.get("age"),
                    "registered_date": user_details.get("registered", {}).get("date"),
                    "registered_age": user_details.get("registered", {}).get("age"),
                    "phone": user_details.get("phone"),
                    "cell": user_details.get("cell"),
                    "time": user_details.get("time")
                })

        if not user_data:
            print("No valid user data found.")
            return

        # Define schema for DataFrame
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("location", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("registered_date", StringType(), True),
            StructField("registered_age", IntegerType(), True),
            StructField("phone", StringType(), True),
            StructField("cell", StringType(), True),
            StructField("time", StringType(), True)
        ])

        # Create a PySpark DataFrame
        user_df = self.spark.createDataFrame(user_data, schema=schema)

        # Save all records to PostgreSQL
        user_df.write \
            .jdbc(self.pg_url, "user_data", mode="append", properties=self.pg_properties)

        print("Batch saved to PostgreSQL.")

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
        batch_size=20
    )
    try:
        kafka_consumer.start_consumer()
    except KeyboardInterrupt:
        kafka_consumer.stop()