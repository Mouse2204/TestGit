# Spark_Rule_Fast.py - OPTIMIZED FOR PERFORMANCE
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import time
import os

print("=" * 70)
print("⚡ GENE RULE ENGINE - HIGH PERFORMANCE MODE")
print("=" * 70)

# --- SET ENV ---
os.environ['AWS_ACCESS_KEY_ID'] = 'minioadmin'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'minioadmin'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--driver-memory 2g pyspark-shell'

# --- OPTIMIZED SPARK CONFIG ---
spark = SparkSession.builder \
    .appName("GeneRuleEngineFast") \
    .master("local[4]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
            "io.delta:delta-core_2.12:2.4.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.default.parallelism", "4") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.memory.fraction", "0.8") \
    .config("spark.memory.storageFraction", "0.3") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.streaming.backpressure.enabled", "true") \
    .config("spark.streaming.backpressure.initialRate", "200") \
    .config("spark.streaming.kafka.maxRatePerPartition", "200") \
    .config("spark.streaming.kafka.consumer.cache.enabled", "false") \
    .config("spark.ui.showConsoleProgress", "false") \
    .config("spark.logConf", "false") \
    .getOrCreate()

# TẮT TẤT CẢ LOGS TRỪ ERROR
spark.sparkContext.setLogLevel("ERROR")

print(f"{time.strftime('%H:%M:%S')} - ✅ Spark optimized (4 cores, 2GB memory)")

def apply_gene_rules(max_val, min_val, t40, t50):
    if max_val > 1.5:
        return "UP-REGULATED"
    if min_val < -1.5:
        return "DOWN-REGULATED"
    if abs(t40) > 1.0 or abs(t50) > 1.0:
        return "EARLY-RESPONSE"
    return "NORMAL"

rule_udf = udf(apply_gene_rules, StringType())

# --- SIMPLIFIED SCHEMA ---
schema = StructType([
    StructField("gene_id", StringType()),
    StructField("t40", FloatType()),
    StructField("t50", FloatType()),
    StructField("max_val", FloatType()),
    StructField("min_val", FloatType())
])

# --- OPTIMIZED KAFKA READING ---
print(f"{time.strftime('%H:%M:%S')} - 📡 Reading from Kafka (optimized)...")
#s
# Dùng batch mode thay vì trigger time
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "gene-expression") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "200") \
    .option("failOnDataLoss", "false") \
    .option("fetchOffset.numRetries", "3") \
    .option("fetchOffset.retryIntervalMs", "100") \
    .load()

# --- EFFICIENT PROCESSING ---
# Chọn ít cột hơn
df_processed = df.selectExpr(
    "CAST(value AS STRING) as json_str",
    "CURRENT_TIMESTAMP() as processed_at"
).select(
    from_json(col("json_str"), schema).alias("data"),
    col("processed_at")
).select(
    col("data.gene_id"),
    col("data.t40"),
    col("data.t50"),
    col("data.max_val"),
    col("data.min_val"),
    col("processed_at")
).withColumn(
    "result",
    rule_udf(col("max_val"), col("min_val"), col("t40"), col("t50"))
).filter(col("result") != "NORMAL")

print(f"{time.strftime('%H:%M:%S')} - ✅ Processing pipeline ready")

# --- OPTIMIZED WRITING ---
# 1. Delta với batch mode (không dùng trigger time)
delta_query = df_processed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://gene-data/checkpoints/fast") \
    .option("path", "s3a://gene-data/delta/fast_results") \
    .option("mergeSchema", "true") \
    .trigger(processingTime="30 seconds") \
    .start()

# 2. Console với limit
console_query = df_processed.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "true") \
    .option("numRows", "20") \
    .start()

print(f"{time.strftime('%H:%M:%S')} - ✅ Streaming started")
print("\n" + "=" * 70)
print("⚡ PERFORMANCE OPTIMIZATIONS APPLIED:")
print("=" * 70)
print("• Cores: 4 (was 2)")
print("• Memory: 2GB driver/executor")
print("• Batch size: 200 messages/trigger")
print("• Trigger interval: 30 seconds")
print("• Backpressure: Enabled")
print("• Log level: ERROR only")
print("=" * 70)
print("🎯 Processing 4381 genes with optimized pipeline...")
print("=" * 70)

# --- MONITORING THREAD ---
def monitor_performance():
    """Hiển thị performance metrics"""
    import time as t
    while True:
        t.sleep(30)
        print(f"\n📊 [{time.strftime('%H:%M:%S')}] Pipeline active...")

import threading
monitor_thread = threading.Thread(target=monitor_performance, daemon=True)
monitor_thread.start()

# --- AWAIT TERMINATION ---
try:
    console_query.awaitTermination()
except KeyboardInterrupt:
    print(f"\n{time.strftime('%H:%M:%S')} - ⏹️ Stopping optimized pipeline...")
    console_query.stop()
    delta_query.stop()
finally:
    spark.stop()
    print(f"{time.strftime('%H:%M:%S')} - 🏁 Pipeline stopped")