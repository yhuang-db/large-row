from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
import pyspark.sql.functions as F
from contextlib import contextmanager
import time
import logging
import argparse


@contextmanager
def time_usage(name=""):
    """log the time usage in a code block
    prefix: the prefix text to show
    """
    start = time.time()
    yield
    end = time.time()
    elapsed_seconds = float("%.4f" % (end - start))
    logging.info("%s: elapsed seconds: %s", name, elapsed_seconds)


def test_1():
    spark.sql("select * from t limit 1").collect()


def test_2():
    spark.sql("select array_max(col_0) from t").collect()


def test_3():
    spark.sql("select array_sort(col_0) from t").collect()


def test_4():
    df.select(F.aggregate("col_0", F.lit(0.0), lambda acc, x: acc + x).alias("total")).collect()


def test_5():
    spark.sql("select get_second_max(col_0) from t").collect()


def get_second_max(arr):
    if arr is None or len(arr) < 2:
        return None
    sorted_arr = sorted(arr, reverse=True)
    if len(sorted_arr) < 2:
        return None
    return sorted_arr[1]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Large Row Benchmark")
    parser.add_argument("-t", "--test", help="file name of test data in 'data/'", default="1_cols_1_kb")
    args = parser.parse_args()
    test_file = args.test

    logging.getLogger().setLevel(logging.INFO)
    logging.info(f"[{test_file}]: start testing")
    spark = SparkSession.builder.appName("LargeRowBenchMark").getOrCreate()
    spark.udf.register("get_second_max", get_second_max, DoubleType())

    with time_usage("read parquet file"):
        df = spark.read.parquet(f"data/{test_file}.parquet")
    df.createOrReplaceTempView("t")

    logging.info(f"[{test_file}]: Done read parquet file")

    # Stress test
    for i, test in enumerate([test_1, test_2, test_3, test_4, test_5]):
        test()
        logging.info(f"[{test_file}]: Test {i+1} survived")

    spark.stop()
