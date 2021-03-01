from __future__ import print_function

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_json, col
from pyspark.sql.types import StringType, TimestampType, StructType

# spark-submit --packages /spark/jars/spark-sql-kafka-0-10_2.12-3.0.2.jar test.py
# spark-submit --jars /spark/jars/spark-streaming-kafka-0-10_2.12-3.0.2.jar test.py

# spark-submit --jars /spark/jars/spark-streaming-kafka-0-8-assembly_2.11-3.0.2.jar test.py
if __name__ == "__main__":
    """
    Usage: pi [partitions]
    """
    spark = SparkSession.builder.appName("PythonKafka").getOrCreate()
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "11.0.2.52:9092")
        .option("subscribe", "test")
        .option("startingOffsets", "earliest")
        .load()
    )
    to_string = udf(lambda k: str(k))
    schema = StructType().add("message", StringType(), "timestamp", TimestampType())

    # lines = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as json")
    value = from_json("value", schema)
    lines = df.select(
        to_string(col("key")).cast("string"),
        value["message"].alias("message"),
        value["timestamp"].alias("timestamp"),
    )
    query = lines.writeStream.outputMode("append").format("console").start()

    query.awaitTermination()
