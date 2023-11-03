from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Initialize a Spark session
spark = SparkSession.builder.appName("KafkaConsumer").getOrCreate()

# Define the Kafka parameters
kafka_brokers = "broker:29092"  # Update with your Kafka broker address
kafka_topic = "test"  # The Kafka topic to consume from

# Create a DataFrame representing the stream of Kafka messages
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("subscribe", kafka_topic) \
    .load()

# Extract the message value from the Kafka message
messages = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Display the messages to the console
query = messages.writeStream.outputMode("append").format("console").start()

# Start the streaming query
query.awaitTermination()
