import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: spark-submit kafkaCountHashtags.py <hostname> <port> <topic>",
           file=sys.stderr)
        exit(-1)

    host = sys.argv[1]
    port = sys.argv[2]
    topic = sys.argv[3]

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
        if word.lower().startswith("#"):
            return word
        else:
            return "nonTag"

    extract_tags_udf = udf(extract_tags, StringType())

    resultDF = words.withColumn("tags", extract_tags_udf(words.word))

    hashtagCountsDF = resultDF.where(resultDF.tags != "nonTag") \
                    .groupBy("tags") \
                    .count() \
                    .orderBy("count", ascending=False)

    query = hashtagCountsDF.writeStream \
                    .outputMode("complete") \
                    .format("console") \
                    .option("truncate", "false") \
                    .start() \
                    .awaitTermination()
