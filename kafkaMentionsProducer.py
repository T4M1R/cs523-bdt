import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: spark-submit kafkaMentionsProducer.py <hostname> <port> <topic> <to-topic>",
           file=sys.stderr)
        exit(-1)

    host = sys.argv[1]
    port = sys.argv[2]
    topic = sys.argv[3]
    toTopic = sys.argv[4]

    spark = SparkSession \
        .builder \
        .appName("TwitterHashCount")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    lines = spark.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', host + ":" + port) \
        .option('subscribe', topic) \
        .load()

    words = lines.select(
        explode(
            split(lines.value, " ")
        ).alias("word")
    )

    def extract_tags(word):
        if word.lower().startswith("@"):
            return word
        else:
            return "nonMention"

    extract_tags_udf = udf(extract_tags, StringType())

    resultDF = words.withColumn("tags", extract_tags_udf(words.word))

    mentionsDf = resultDF.where(resultDF.tags != "nonMention")

    finalDf = mentionsDf.select(to_json(
        struct("word")).alias("value")
    )

    query = finalDf.writeStream \
                    .outputMode("append") \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", host + ":" + port) \
                    .option("checkpointLocation", "./checkpoint-kafka") \
                    .option("topic", toTopic) \
                    .start() \
                    .awaitTermination()
