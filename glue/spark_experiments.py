"""Small observable Spark experiments; works locally or as a Glue Spark job."""

from __future__ import annotations

import argparse
import time

from pyspark.sql import SparkSession, functions as F


def timed(label, fn):
    started = time.perf_counter()
    result = fn()
    print(f"METRIC {label}_seconds={time.perf_counter() - started:.4f}")
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--OUTPUT_BASE", default="/tmp/weather-spark-experiments")
    args, _ = parser.parse_known_args()
    spark = SparkSession.builder.appName("spark-learning-experiments").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "8")

    base = spark.range(0, 100_000, numPartitions=4).withColumn("group_id", F.col("id") % 100)
    lazy = base.filter("id % 2 = 0").select("id", "group_id")
    print("LAZY: nada foi materializado até aqui; explain mostra o plano.")
    lazy.explain("formatted")
    timed("lazy_count_action", lazy.count)

    print(f"PARTITIONS initial={base.rdd.getNumPartitions()}")
    print(f"PARTITIONS repartition_8={base.repartition(8).rdd.getNumPartitions()}")
    print(f"PARTITIONS coalesce_2={base.coalesce(2).rdd.getNumPartitions()}")

    shuffled = base.groupBy("group_id").count()
    print("SHUFFLE: procure Exchange no plano abaixo.")
    shuffled.explain("formatted")
    timed("shuffle_groupby", shuffled.count)

    lookup = spark.createDataFrame([(i, f"group-{i}") for i in range(100)], ["group_id", "group_name"])
    print("JOIN normal (o otimizador ainda pode escolher broadcast automaticamente):")
    base.join(lookup, "group_id").explain("formatted")
    print("BROADCAST explícito; procure BroadcastExchange/BroadcastHashJoin:")
    broadcasted = base.join(F.broadcast(lookup), "group_id")
    broadcasted.explain("formatted")
    timed("broadcast_join", broadcasted.count)

    skew = spark.range(0, 20_000, numPartitions=8).withColumn(
        "skew_key", F.when(F.col("id") < 18_000, F.lit("hot")).otherwise(F.concat(F.lit("k-"), F.col("id")))
    )
    distribution = skew.groupBy("skew_key").count().orderBy(F.desc("count"))
    print("SKEW: a chave hot concentra 90% das linhas:")
    distribution.show(5, truncate=False)

    small_path = f"{args.OUTPUT_BASE.rstrip('/')}/many-small-files"
    compact_path = f"{args.OUTPUT_BASE.rstrip('/')}/compact-files"
    timed("write_24_small_files", lambda: spark.range(0, 2400).repartition(24).write.mode("overwrite").parquet(small_path))
    timed("write_2_compact_files", lambda: spark.range(0, 2400).coalesce(2).write.mode("overwrite").parquet(compact_path))
    timed("read_small_files", lambda: spark.read.parquet(small_path).count())
    timed("read_compact_files", lambda: spark.read.parquet(compact_path).count())
    spark.stop()


if __name__ == "__main__":
    main()
