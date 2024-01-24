from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sum, expr, row_number, dense_rank, percent_rank, ntile, to_json, struct, unix_timestamp, window
from pyspark.sql.types import StructType, StringType, IntegerType, LongType, StructField
from pyspark.sql.window import Window
from confluent_kafka import Producer
import json
from datetime import datetime
from pyspark.sql import functions as F
spark = SparkSession.builder \
    .appName("PySparkKafkaStreaming") \
    .master("local[2]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

data_schema = StructType([
    StructField("Id", LongType()),
    StructField("OwnerUserId", StringType()),
    StructField("CreationDate", StringType()),  
    StructField("Score", IntegerType()),
    StructField("Title", StringType()),
    StructField("Body", StringType())
])

kafka_data_frame = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test4") \
    .option("startingOffsets", "earliest") \
    .load()

value_data_frame = kafka_data_frame.selectExpr("CAST(value AS STRING)")

parsed_data_frame = value_data_frame \
    .select(from_json(col("value"), data_schema).alias("parsed_value")) \
    .select("parsed_value.*")

parsed_data_frame = parsed_data_frame.withColumn("CreationTimestamp", unix_timestamp("CreationDate", "yyyy-MM-dd'T'HH:mm:ss'Z'").cast("timestamp"))


aggregated_data_frame = parsed_data_frame.groupBy("OwnerUserId") \
    .agg(sum("Score").alias("TotalScore"))


producer_config = {
    'bootstrap.servers': 'localhost:9092'
}

producer = Producer(producer_config)
def send_to_kafka(producer, topic, key, value):
    try:
        producer.produce(topic, key=key, value=value)
        producer.flush()
    except Exception as e:
        print(f"Error sending to Kafka: {e}")

def datetime_converter(o):
    if isinstance(o, datetime):
        return o.isoformat()
    raise TypeError(f"Type not serializable: {type(o)}")

def process_aggregated_batch(batch_df, batch_id):
    try:
        for row in batch_df.collect():
            record = row.asDict()
            json_record = json.dumps(record, default=datetime_converter, indent=6)
            send_to_kafka(producer, "aggregated2", str(record["OwnerUserId"]), json_record)

        print("Aggregated data have been sent to topic 'aggregated2'")
    except Exception as e:
        print(f"An error occurred in process_aggregated_batch: {e}")

query_aggregated = aggregated_data_frame \
    .writeStream \
    .foreachBatch(process_aggregated_batch) \
    .trigger(processingTime='5 seconds') \
    .outputMode("update") \
    .start()

def process_pivoted_batch(batch_df, batch_id):
    try:
        
        pivoted_batch_df = batch_df.groupBy("OwnerUserId").pivot("Score").sum("Score")

        for row in pivoted_batch_df.collect():
            record = row.asDict()
            json_record = json.dumps(record, default=datetime_converter, indent=6)
            send_to_kafka(producer, "pivoted2", str(record["OwnerUserId"]), json_record)

        print("Pivoted data have been sent to topic 'pivoted2'")
    except Exception as e:
        print(f"An error occurred in process_pivoted_batch: {e}")

query_pivoted = parsed_data_frame.writeStream \
                                .foreachBatch(process_pivoted_batch) \
                                .trigger(processingTime='5 seconds') \
                                .outputMode("append") \
                                .start()

def process_rollup_batch(batch_df, batch_id, topic):
    try:
        for row in batch_df.collect():
            record = row.asDict()
            json_record = json.dumps(record, default=datetime_converter, indent=6)
            send_to_kafka(producer, topic, str(record["OwnerUserId"]), json_record)

        print(f"Rollup data have been sent to topic '{topic}'")
    except Exception as e:
        print(f"An error occurred in process_rollup_batch: {e}")

rollup_data_frame = parsed_data_frame.rollup("OwnerUserId", "Title") \
    .agg(expr("sum(Score) as TotalScore"))
# .rollup(“Title”, “Score”).count().show()
# rollup_data_frame = parsed_data_frame.rollup("OwnerUserId", "Title").count()


query_rollup = rollup_data_frame.writeStream \
    .foreachBatch(lambda df, batch_id: process_rollup_batch(df, batch_id, "rollup2")) \
    .trigger(processingTime='5 seconds') \
    .outputMode("update") \
    .start()

def process_cubed_batch(batch_df, batch_id, topic):
    try:
        for row in batch_df.collect():
            record = row.asDict()
            json_record = json.dumps(record, default=datetime_converter, indent=6)
            send_to_kafka(producer, topic, str(record["OwnerUserId"]), json_record)

        print(f"Cubed data have been sent to topic '{topic}'")
    except Exception as e:
        print(f"An error occurred in process_cubed_batch: {e}")

cubed_data_frame = parsed_data_frame.cube("OwnerUserId", "Title") \
    .agg(expr("sum(Score) as TotalScore"))

query_cubed = cubed_data_frame.writeStream \
    .foreachBatch(lambda df, batch_id: process_cubed_batch(df, batch_id, "cubed2")) \
    .trigger(processingTime='5 seconds') \
    .outputMode("update") \
    .start()


def process_ranked_batch(batch_df, batch_id):
    try:
        windowSpec = Window.orderBy(col("Score").desc())
        ranked_batch_df = batch_df.withColumn("row_number", row_number().over(windowSpec)) \
                                  .withColumn("dense_rank", dense_rank().over(windowSpec)) \
                                  .withColumn("percent_rank", percent_rank().over(windowSpec)) \
                                  .withColumn("ntile", ntile(2).over(windowSpec))

        for row in ranked_batch_df.collect():
            record = row.asDict()
            json_record = json.dumps(record, default=datetime_converter, indent=6)
            send_to_kafka(producer, "ranked2", str(record["Id"]), json_record)

        print("Data have been sent to topic 'ranked2'")
    except Exception as e:
        print(f"An error occurred in process_ranked_batch: {e}")

query_ranked = parsed_data_frame \
    .writeStream \
    .foreachBatch(process_ranked_batch) \
    .trigger(processingTime='5 second') \
    .outputMode("update") \
    .start()

def process_analytic_batch(batch_df, batch_id):
    try:
        windowSpec = Window.partitionBy("OwnerUserId").orderBy("CreationTimestamp")

        batch_df = batch_df \
            .withColumn("cumulative_distribution", F.cume_dist().over(windowSpec)) \
            .withColumn("first_value", F.first("Score").over(windowSpec)) \
            .withColumn("last_value", F.last("Score").over(windowSpec)) \
            .withColumn("lag_value", F.lag("Score", 1).over(windowSpec)) \
            .withColumn("lead_value", F.lead("Score", 1).over(windowSpec))

        for row in batch_df.collect():
            record = row.asDict()
            json_record = json.dumps(record, default=datetime_converter, indent=6)
            send_to_kafka(producer, "analytic2", str(record["OwnerUserId"]), json_record)

        print("analytic data with window functions have been sent to topic 'analytic2'")
    except Exception as e:
        print(f"An error occurred in process_analytic_batch: {e}")

query_analytic = parsed_data_frame \
    .writeStream \
    .foreachBatch(process_analytic_batch) \
    .trigger(processingTime='5 seconds') \
    .outputMode("update") \
    .start()

query_ranked.awaitTermination()
query_aggregated.awaitTermination()
query_pivoted.awaitTermination()
query_rollup.awaitTermination()
query_cubed.awaitTermination()
query_analytic.awaitTermination()