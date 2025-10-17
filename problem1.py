#!/usr/bin/env python3
"""
Problem 1 

This script analyzes the log level distribution across all log files.

Outputs: 
1. data/output/problem1_counts.csv: log level counts
2. data/output/problem1_sample.csv: 10 random sample log entries with their levels
3. data/output/problem1_summary.txt: summary statistics

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