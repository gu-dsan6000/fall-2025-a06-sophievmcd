#!/usr/bin/env python3
"""
Problem 2 

This script analyzes cluster usage patterns to understand which clusters are most heavily used over time.
It will: Extract cluster IDs, application IDs, and application start/end times to create a time-series dataset suitable for visualization with Seaborn.

**Key Questions to Answer:**
- How many unique clusters are in the dataset?
- How many applications ran on each cluster?
- Which clusters are most heavily used?
- What is the timeline of application execution across clusters?

```

**Outputs (5 files):**
1. `data/output/problem2_timeline.csv`: Time-series data for each application
2. `data/output/problem2_cluster_summary.csv`: Aggregated cluster statistics
3. `data/output/problem2_stats.txt`: Overall summary statistics
4. `data/output/problem2_bar_chart.png`: Bar chart visualization
5. `data/output/problem2_density_plot.png`: Faceted density plot visualization

**Expected output 1 (timeline):**
```
cluster_id,application_id,app_number,start_time,end_time
1485248649253,application_1485248649253_0001,0001,2017-03-29 10:04:41,2017-03-29 10:15:23
1485248649253,application_1485248649253_0002,0002,2017-03-29 10:16:12,2017-03-29 10:28:45
1448006111297,application_1448006111297_0137,0137,2015-11-20 14:23:11,2015-11-20 14:35:22
...
```

**Expected output 2 (cluster summary):**
```
cluster_id,num_applications,cluster_first_app,cluster_last_app
1485248649253,181,2017-03-29 10:04:41,2017-03-29 18:42:15
1472621869829,8,2016-08-30 12:15:30,2016-08-30 16:22:10
...
```

**Expected output 3 (stats):**
```
Total unique clusters: 6
Total applications: 194
Average applications per cluster: 32.33

Most heavily used clusters:
  Cluster 1485248649253: 181 applications
  Cluster 1472621869829: 8 applications
  ...
```

**Visualization Output:**
The script automatically generates two separate visualizations:

1. **Bar Chart** (`problem2_bar_chart.png`):
   - Number of applications per cluster
   - Value labels displayed on top of each bar
   - Color-coded by cluster ID

2. **Density Plot** (`problem2_density_plot.png`):
   - Shows job duration distribution for the **largest cluster** (cluster with most applications)
   - Histogram with KDE overlay
   - **Log scale** on x-axis to handle skewed duration data
   - Sample count (n=X) displayed in title


OUTPUTS:
   - `problem2_timeline.csv` - Time-series data for each application
   - `problem2_cluster_summary.csv` - Aggregated cluster statistics
   - `problem2_stats.txt` - Overall summary statistics
   - `problem2_bar_chart.png` - Bar chart showing applications per cluster
   - `problem2_density_plot.png` - Faceted density plots showing duration distribution per cluster

"""

import sys
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns



logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s,p%(process)s,{%(filename)s:%(lineno)d},%(levelname)s,%(message)s",
)

logger = logging.getLogger(__name__)


def create_spark_session():
    """Create spark session"""
    spark = SparkSession.builder \
    .appName("A06_P2_Local") \
    .getOrCreate()

    logger.info("Spark session created.")
    return spark

def get_timeline(df):
    """

    1. `data/output/problem2_timeline.csv`: Time-series data for each application

    **Expected output 1 (timeline):**
    ```
    cluster_id,application_id,app_number,start_time,end_time
    1485248649253,application_1485248649253_0001,0001,2017-03-29 10:04:41,2017-03-29 10:15:23
    1485248649253,application_1485248649253_0002,0002,2017-03-29 10:16:12,2017-03-29 10:28:45
    1448006111297,application_1448006111297_0137,0137,2015-11-20 14:23:11,2015-11-20 14:35:22

    """

    # cluster and app ids (from path)
    df = (
        df.withColumn("path", F.input_file_name())
          .withColumn("application_id", F.regexp_extract(F.col("path"), r"(application_\d+_\d+)", 1))
          .withColumn("cluster_id", F.regexp_extract(F.col("application_id"), r"application_(\d+)_", 1))
    )

    # timestamp extraction from log line
    df = df.withColumn(
        "timestamp_str",
        F.regexp_extract(F.col("value"), r"(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", 1)
    )

    # Explicit timestamp parsing with formatting fixes
    df = df.withColumn(
        "timestamp",
        F.coalesce(
            F.expr("try_to_timestamp(timestamp_str, 'yy/MM/dd HH:mm:ss')"),
            F.expr("try_to_timestamp(timestamp_str, 'yyyy-MM-dd HH:mm:ss')"),
            F.expr("try_to_timestamp(timestamp_str, 'MM/dd/yyyy HH:mm:ss')")
        )
    )

    # Filter out lines without valid timestamps
    df = df.filter(F.col("timestamp").isNotNull())

    # start and end times, for each application
    timeline_df = (
        df.groupBy("cluster_id", "application_id")
          .agg(
              F.min("timestamp").alias("start_time"),
              F.max("timestamp").alias("end_time")
          )
          .withColumn("app_number",
                      F.lpad(F.split("application_id", "_").getItem(2), 4, "0"))
          .orderBy("cluster_id", "app_number")
    )

    # save and log
    timeline_df.toPandas().to_csv("/home/ubuntu/spark-cluster/problem2_timeline.csv", index=False)
    logger.info("Saved timeline to /home/ubuntu/spark-cluster/problem2_timeline.csv")

    logger.info(f"Raw log lines: {df.count()}")

    # debug info
    df_debug = df.withColumn(
        "raw_timestamp",
        F.regexp_extract("value", r"(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", 1)
    )

    return timeline_df
   




def get_cluster_summary(timeline_df):
    """
    2. `data/output/problem2_cluster_summary.csv`: Aggregated cluster statistics

    **Expected output 2 (cluster summary):**
    ```
    cluster_id,num_applications,cluster_first_app,cluster_last_app
    1485248649253,181,2017-03-29 10:04:41,2017-03-29 18:42:15
    1472621869829,8,2016-08-30 12:15:30,2016-08-30 16:22:10
    ...
    """
    cluster_summary_df = (
        timeline_df.groupBy("cluster_id")
                   .agg(F.count("*").alias("num_applications"),
                        F.min("start_time").alias("cluster_first_app"),
                        F.max("end_time").alias("cluster_last_app"))
                   .orderBy(F.desc("num_applications"))
    )

    cluster_summary_df.toPandas().to_csv("/home/ubuntu/spark-cluster/problem2_cluster_summary.csv", index=False)
    logger.info("Saved cluster summary to /home/ubuntu/spark-cluster/problem2_cluster_summary.csv")
    return cluster_summary_df


def get_overall_summary(cluster_summary_df):
    """
    3. `data/output/problem2_stats.txt`: Overall summary statistics

    **Expected output 3 (stats):**
    ```
    Total unique clusters: 6
    Total applications: 194
    Average applications per cluster: 32.33

    Most heavily used clusters:
    Cluster 1485248649253: 181 applications
    Cluster 1472621869829: 8 applications
    ...

    """
    total_clusters = cluster_summary_df.count()
    total_apps = cluster_summary_df.agg(F.sum("num_applications")).first()[0]
    avg_apps = total_apps / total_clusters

    top_clusters = cluster_summary_df.orderBy(F.desc("num_applications")).limit(5).collect()
    lines = [
        f"Total unique clusters: {total_clusters}",
        f"Total applications: {total_apps}",
        f"Average applications per cluster: {avg_apps:.2f}",
        "",
        "Most heavily used clusters:"
    ]
    for r in top_clusters:
        lines.append(f"  Cluster {r['cluster_id']}: {r['num_applications']} applications")

    with open("/home/ubuntu/spark-cluster/problem2_stats.txt", "w") as f:
        f.write("\n".join(lines))
    logger.info("Saved overall stats to /home/ubuntu/spark-cluster/problem2_stats.txt")
    return lines
 

def get_bar_chart(cluster_summary_df):
    """
    4. `data/output/problem2_bar_chart.png`: Bar chart visualization

     **Bar Chart** (`problem2_bar_chart.png`):
    - Number of applications per cluster
    - Value labels displayed on top of each bar
    - Color-coded by cluster ID
    """
    pdf = cluster_summary_df.toPandas()
    plt.figure(figsize=(10, 6))
    sns.barplot(data=pdf, x="cluster_id", y="num_applications", palette="viridis")
    plt.xticks(rotation=45, ha="right")
    plt.title("Applications per Cluster")
    plt.xlabel("Cluster ID")
    plt.ylabel("Number of Applications")

    # Value labels
    for i, row in pdf.iterrows():
        plt.text(i, row.num_applications + 0.5, str(row.num_applications), ha="center")

    plt.tight_layout()
    plt.savefig("/home/ubuntu/spark-cluster/problem2_bar_chart.png")
    plt.close()
    logger.info("Saved bar chart to /home/ubuntu/spark-cluster/problem2_bar_chart.png")


def get_density_plot(timeline_df, cluster_summary_df):
    """
    5. `data/output/problem2_density_plot.png`: Faceted density plot visualization

    **Density Plot** (`problem2_density_plot.png`):
   - Shows job duration distribution for the **largest cluster** (cluster with most applications)
   - Histogram with KDE overlay
   - **Log scale** on x-axis to handle skewed duration data
   - Sample count (n=X) displayed in title
    """
    pdf = timeline_df.toPandas()
    pdf["duration_sec"] = (pdf["end_time"] - pdf["start_time"]).dt.total_seconds()

    # Find largest cluster
    top_cluster = cluster_summary_df.orderBy(F.desc("num_applications")).first()["cluster_id"]
    cluster_df = pdf[pdf["cluster_id"] == top_cluster]

    plt.figure(figsize=(8, 5))
    sns.histplot(cluster_df["duration_sec"], bins=30, kde=True, color="royalblue")
    plt.xscale("log")
    plt.xlabel("Job Duration (seconds, log scale)")
    plt.ylabel("Frequency")
    plt.title(f"Duration Distribution – Cluster {top_cluster} (n={len(cluster_df)})")
    plt.tight_layout()
    plt.savefig("/home/ubuntu/spark-cluster/problem2_density_plot.png")
    plt.close()
    logger.info("Saved density plot to /home/ubuntu/spark-cluster/problem2_density_plot.png")




def main():
    """Main function for A06P2"""

    # create spark session
    logger.info("Initializing Spark session")
    spark = create_spark_session()
    success = False

    try:
        # read files 
        input_path = "s3a://svm37-assignment-spark-cluster-logs/data/"
        logger.info(f"Reading input logs from {input_path}")
        df = (
            spark.read
            .option("recursiveFileLookup", "true")
            .text(input_path)
        )
        logger.info(f"✅ Loaded {df.count()} raw lines from data/sample/")

        # timeline
        logger.info("Building time series.")
        timeline_df = get_timeline(df)

        # cluster summary
        logger.info("Getting cluster summary.")
        cluster_summary_df = get_cluster_summary(timeline_df)

        # Summary statistics
        logger.info("Computing overall summary statistics.")
        get_overall_summary(cluster_summary_df)

        # vis1: bar chart
        logger.info("Building bar chart.")
        get_bar_chart(cluster_summary_df)

        # vis2: density plot
        logger.info("Building density plot.")
        get_density_plot(timeline_df, cluster_summary_df)

        success = True

    except Exception as e:
        logger.exception(f"❌ Error during Problem 2 processing: {str(e)}")
        success = False

    # Clean up
    logger.info("Stopping Spark session")
    spark.stop()

    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())





#### TIMELINE EARLY
"""
def get_timeline(df):

    1. `data/output/problem2_timeline.csv`: Time-series data for each application

    **Expected output 1 (timeline):**
    ```
    cluster_id,application_id,app_number,start_time,end_time
    1485248649253,application_1485248649253_0001,0001,2017-03-29 10:04:41,2017-03-29 10:15:23
    1485248649253,application_1485248649253_0002,0002,2017-03-29 10:16:12,2017-03-29 10:28:45
    1448006111297,application_1448006111297_0137,0137,2015-11-20 14:23:11,2015-11-20 14:35:22


    # cluster and app ids (from path)
    df = (
        df.withColumn("path", F.input_file_name())
          .withColumn("application_id", F.regexp_extract(F.col("path"), r"(application_\d+_\d+)", 1))
          .withColumn("cluster_id", F.regexp_extract(F.col("application_id"), r"application_(\d+)_", 1))
    )

    # timestamp from path - ADDED: handling for error of not expected timestamp
    df = df.withColumn(
        "timestamp",
        F.expr("try_to_timestamp(regexp_extract(value, '(\\d{2}/\\d{2}/\\d{2} \\d{2}:\\d{2}:\\d{2})', 1), 'yy/MM/dd HH:mm:ss')")
    )
    df = df.filter(F.col("timestamp").isNotNull())

    # start and end times, for each application
    timeline_df = (
        df.groupBy("cluster_id", "application_id")
          .agg(F.min("timestamp").alias("start_time"),
               F.max("timestamp").alias("end_time"))
          .withColumn("app_number",
                      F.lpad(F.split("application_id", "_").getItem(2), 4, "0"))
          .orderBy("cluster_id", "app_number")
    )

    timeline_df.toPandas().to_csv("data/output/problem2_timeline.csv", index=False)
    logger.info(" Saved timeline to data/output/problem2_timeline.csv")

    logger.info(f"➡️  Raw log lines: {df.count()}")

    # Try the regex again without filtering first
    df_debug = df.withColumn(
        "raw_timestamp",
        F.regexp_extract("value", r"(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", 1)
    )

    logger.info(f"➡️  Lines matching timestamp regex: {df_debug.filter(F.col('raw_timestamp') != '').count()}")


    return timeline_df
"""
