import os
import sys
import socketserver
from datetime import datetime

# --- THE WINDOWS PATCH ---
if not hasattr(socketserver, "UnixStreamServer"):
    socketserver.UnixStreamServer = socketserver.TCPServer
    socketserver.UnixDatagramServer = socketserver.UDPServer

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, hour, when, regexp_extract

import os
#os.environ['HADOOP_HOME'] = "E:\\hadoop"
#os.environ['PATH'] = os.environ['PATH'] + ";" + "E:\\hadoop\\bin"

def run_spark_transform():
    spark = SparkSession.builder \
        .appName("HN_Transformation") \
        .master("local[*]") \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.extraJavaOptions", 
                "--add-opens=java.base/java.lang=ALL-UNNAMED " +
                "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED " +
                "--add-opens=java.base/java.net=ALL-UNNAMED " +
                "--add-opens=java.base/java.io=ALL-UNNAMED " +
                "--add-opens=java.base/java.util=ALL-UNNAMED " +
                "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED " +
                "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED " +
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED") \
        .getOrCreate()

    today = datetime.now().strftime("%Y-%m-%d")
    input_path = f"data/raw/{today}/stories.json"
    output_dir = f"data/processed/{today}"
    os.makedirs(output_dir, exist_ok=True)

    # 1. Load JSON (with multiline fix)
    df = spark.read.option("multiline", "true").json(input_path)

    # 2. Transformations (Keep your Spark logic!)
    df_transformed = df.filter(col("title").isNotNull()) \
        .withColumn("posted_at", from_unixtime(col("time")).cast("timestamp")) \
        .withColumn("hour_posted", hour(col("posted_at"))) \
        .withColumn("story_type", 
            when(col("title").contains("Show HN"), "Show HN")
            .when(col("title").contains("Ask HN"), "Ask HN")
            .otherwise("Article")) \
        .withColumn("domain", regexp_extract(col("url"), r'https?://([^/]+)', 1))

    # 3. Select final columns
    final_df = df_transformed.select(
        "id", "title", "by", "score", "posted_at", 
        "hour_posted", "domain", "story_type"
    )

    # --- THE WORKAROUND ---
    # Instead of final_df.write.parquet...
    # Convert to Pandas and save using Pandas (bypasses Hadoop/winutils)
    print("Converting Spark DF to Pandas for saving...")
    pandas_df = final_df.toPandas()
    pandas_df.to_parquet(f"{output_dir}/stories.parquet", engine='pyarrow')
    
    print(f"Success! Parquet saved to {output_dir}/stories.parquet")
    spark.stop()

if __name__ == "__main__":
    run_spark_transform()