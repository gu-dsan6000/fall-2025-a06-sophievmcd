#!/usr/bin/env python3
"""
Problem 1 

This script analyzes the log level distribution across all log files.

Outputs: 
1. data/output/problem1_counts.csv: log level counts
    EX: 
        log_level,count
        INFO,125430
        WARN,342
        ERROR,89
        DEBUG,12

2. data/output/problem1_sample.csv: 10 random sample log entries with their levels
    EX:
    log_entry,log_level
    "17/03/29 10:04:41 INFO ApplicationMaster: Registered signal handlers",INFO
    "17/03/29 10:04:42 WARN YarnAllocator: Container request...",WARN

3. data/output/problem1_summary.txt: summary statistics
    EX:
    Total log lines processed: 3,234,567
    Total lines with log levels: 3,100,234
    Unique log levels found: 4

    Log level distribution:
    INFO  :    125,430 (40.45%)
    WARN  :        342 ( 0.01%)

Outputs:
   - `problem1_counts.csv` - Log level counts
   - `problem1_sample.csv` - Sample log entries with levels
   - `problem1_summary.txt` - Summary statistics

"""

import sys
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd


logging.basicConfig(
    level=logging.INFO,  # Set the log level to INFO
    # Define log message format
    format="%(asctime)s,p%(process)s,{%(filename)s:%(lineno)d},%(levelname)s,%(message)s",
)

logger = logging.getLogger(__name__)


def create_spark_session(master_url):
    """Create a Spark session optimized for cluster execution."""

    spark = (
        SparkSession.builder
        .appName("AO6P1-Cluster")

        # Cluster Configuration
        .master(master_url)  # Connect to Spark cluster


        # Memory Configuration
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "2g")

        # Executor Configuration
        .config("spark.executor.cores", "2")
        .config("spark.cores.max", "6")  # Use all available cores across cluster

        # S3 Configuration - Use S3A for AWS S3 access
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")

        # Performance settings for cluster execution
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

        # Serialization
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        # Arrow optimization for Pandas conversion
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")

        .getOrCreate()
    )

    logger.info("Spark session created - optimized for cluster.")
    return spark

def get_log_level_counts(df):
    """
    Gets the following information and outputs in below format:
    1. data/output/problem1_counts.csv: log level counts
    EX: 
        log_level,count
        INFO,125430
        WARN,342
        ERROR,89
        DEBUG,12"""
    
    # use format to get levels
    format = r'\b(INFO|WARN|ERROR|DEBUG|FATAL|TRACE)\b'
    df_with_level = df.withColumn("log_level", F.regexp_extract(F.col("value"), format, 1))
    counts_df = (
        df_with_level
        .filter(F.col("log_level") != "")
        .groupBy("log_level")
        .agg(F.count("*").alias("count"))
        .orderBy(F.desc("count"))
    )

    counts_df.toPandas().to_csv("data/output/problem1_counts.csv", index=False)
    logger.info("Saved log level counts to data/output/problem1_counts.csv")
    return counts_df, df_with_level

def get_log_sample(df_with_level):
    """
    Gets 10 randomly sampled logs and saves in following format:
    data/output/problem1_sample.csv: 10 random sample log entries with their levels
    EX:
    log_entry,log_level
    "17/03/29 10:04:41 INFO ApplicationMaster: Registered signal handlers",INFO
    "17/03/29 10:04:42 WARN YarnAllocator: Container request...",WARN
    """
    sample_10_df = (
        df_with_level
        .filter(F.col("log_level") != "")
        .orderBy(F.rand())
        .limit(10)
        .select(F.col("value").alias("log_entry"), "log_level")
    )
    sample_10_df.toPandas().to_csv("data/output/problem1_sample.csv", index=False)
    logger.info("Saved 10 randomly sampled log entries to data/output/problem1_sample.csv")
    return sample_10_df


def get_sum_stats(df_with_level, counts_df):
    """
    Gets and prints the summary statistics of the processing with the following output
    3. data/output/problem1_summary.txt: summary statistics
    EX:
    Total log lines processed: 3,234,567
    Total lines with log levels: 3,100,234
    Unique log levels found: 4

    Log level distribution:
    INFO  :    125,430 (40.45%)
    WARN  :        342 ( 0.01%)
    """
    total_lines = df_with_level.count()
    lines_with_levels = df_with_level.filter(F.col("log_level") != "").count()
    unique_levels = counts_df.count()

    total_detected = counts_df.agg(F.sum("count")).first()[0]
    counts = counts_df.collect()

    summary_lines = [
        f"Total log lines processed: {total_lines:,}",
        f"Total lines with log levels: {lines_with_levels:,}",
        f"Unique log levels found: {unique_levels}",
        "",
        "Log level distribution:",
    ]

    for row in counts:
        pct = (row['count'] / total_detected) * 100
        summary_lines.append(f"{row['log_level']:6}: {row['count']:10,} ({pct:6.2f}%)")

    with open("data/output/problem1_summary.txt", "w") as f:
        f.write("\n".join(summary_lines))

    logger.info("Saved summary statistics to data/output/problem1_summary.txt")
    return summary_lines




def main():
    """Main function for A06P1"""

    if len(sys.argv) > 1:
        master_url = sys.argv[1]
    else:
        # Try to get from environment variable
        master_private_ip = os.getenv("MASTER_PRIVATE_IP")
        if master_private_ip:
            master_url = f"spark://{master_private_ip}:7077"
        else:
            print("❌ Error: Master URL not provided")
            print("Usage: python problem1_cluster.py spark://MASTER_IP:7077")
            print("   or: export MASTER_PRIVATE_IP=xxx.xxx.xxx.xxx")
            return 1

    # create spark session and save parquet to df
    logger.info("Initializing Spark session")
    spark = create_spark_session(master_url)
    success = False

    try:
        # read files - from sample dir here
        logger.info("Reading logs")
        df = spark.read.text("data/raw/**")

        # Log level counts
        logger.info("Computing log level counts.")
        counts_df, df_with_level = get_log_level_counts(df)

        # Random sample
        logger.info("Getting random 10-log sample.")
        get_log_sample(df_with_level)

        # Summary statistics
        logger.info("Computing summary statistics.")
        get_sum_stats(df_with_level, counts_df)

        success = True

    except Exception as e:
        logger.exception(f"❌ Error during Problem 1 processing: {str(e)}")
        success = False

    # Clean up
    logger.info("Stopping Spark session")
    spark.stop()

    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())