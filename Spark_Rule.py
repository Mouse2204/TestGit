import great_expectations as gx
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, trim, initcap, lower, lit, when, min, max, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = SparkSession.builder \
    .appName("GX-MinMax-Normalization") \
    .master("local[*]") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("id", StringType()),
    StructField("name", StringType()),
    StructField("age", IntegerType()),
    StructField("role", StringType()),
    StructField("salary", DoubleType()),
    StructField("email", StringType()),
    StructField("transaction_date", StringType())
])

context = gx.get_context()
SUITE_NAME = "normalized_quality_suite"
try:
    context.suites.delete(SUITE_NAME)
except:
    pass
suite = gx.ExpectationSuite(name=SUITE_NAME)

suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="salary", min_value=0.0, max_value=1.0))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(column="role", value_set=["Admin", "User", "Dev", "Manager"])) 
suite.add_expectation(gx.expectations.ExpectColumnValuesToMatchRegex(column="email", regex=r"^.+@.+\..+$"))
context.suites.add(suite)


def process_normalization_flow(df, batch_id):
    if df.count() == 0: return

    print(f"Batch {batch_id} Raw Size: {df.count()}")
    df.cache()

    df.write.mode("append").format("parquet").save(f"s3a://datalake/bronze/batch_{batch_id}")

    parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    
    valid_id_df = parsed_df.filter(col("id").isNotNull())

    if valid_id_df.count() > 0:
        
        print("Running Cleaning & Min-Max Normalization...")
        
        batch_stats = valid_id_df.filter(col("salary") > 0) \
            .agg(
                min("salary").alias("min_sal"), 
                max("salary").alias("max_sal"),
                avg("salary").alias("avg_sal")
            ).first()
            
        min_val = batch_stats["min_sal"] if batch_stats["min_sal"] is not None else 0.0
        max_val = batch_stats["max_sal"] if batch_stats["max_sal"] is not None else 1.0
        avg_val = batch_stats["avg_sal"] if batch_stats["avg_sal"] is not None else 0.0
        
        range_val = max_val - min_val
        if range_val == 0: range_val = 1.0

        print(f"Stats - Min: {min_val}, Max: {max_val}, Avg: {avg_val}")

        cleaned_df = valid_id_df \
            .withColumn("name", trim(col("name"))) \
            .withColumn(
                "role_cleaned", 
                initcap(trim(col("role")))
            ) \
            .withColumn(
                "role",
                when(col("role_cleaned").isin(["Admin", "User", "Dev", "Manager"]), col("role_cleaned"))
                .otherwise(lit("User"))
            ) \
            .withColumn(
                "email_cleaned", 
                lower(trim(col("email")))
            ) \
            .withColumn(
                "email",
                when(col("email_cleaned").rlike(r"^.+@.+\..+$"), col("email_cleaned"))
                .otherwise(lit("unknown@example.com"))
            ) \
            .withColumn(
                "salary_temp", 
                when((col("salary").isNull()) | (col("salary") < 0), lit(avg_val))
                .otherwise(col("salary"))
            ) \
            .withColumn(
                "normalized_salary", 
                (col("salary_temp") - lit(min_val)) / lit(range_val)
            ) \
            .withColumn(
                "salary",
                when(col("normalized_salary") < 0.0, 0.0)
                .when(col("normalized_salary") > 1.0, 1.0)
                .otherwise(col("normalized_salary"))
            ) \
            .drop("salary_temp", "normalized_salary", "role_cleaned", "email_cleaned")

        ds_name = "spark_norm_ds"
        val_name = "norm_validation"
        try:
            context.validation_definitions.delete(val_name)
            context.data_sources.delete(ds_name)
        except: pass

        ds = context.data_sources.add_spark(ds_name)
        asset = ds.add_dataframe_asset("norm_asset")
        batch_def = asset.add_batch_definition_whole_dataframe("norm_batch")
        val_def = gx.ValidationDefinition(name=val_name, data=batch_def, suite=suite)
        
        result = val_def.run(batch_parameters={"dataframe": cleaned_df})

        if result.success:
            print("Data Normalized & Validated -> SILVER.")
            cleaned_df.write.mode("append").format("parquet").save(f"s3a://datalake/silver/batch_{batch_id}")
        else:
            print("Validation Failed -> QUARANTINE.")
            print("Validation Failure Details:")
            for r in result.results:
                if not r.success:
                    print(f"- {r.expectation_config.expectation_type}")
                    if 'partial_unexpected_list' in r.result:
                        bad_values = r.result['partial_unexpected_list'][:3]
                        print(f"  Bad values: {bad_values}")
            
            cleaned_df.write.mode("append").format("parquet").save(f"s3a://datalake/quarantine/batch_{batch_id}")

        if batch_id % 5 == 0:
            context.build_data_docs()

    df.unpersist()

kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:19092") \
    .option("subscribe", "user_data_topic") \
    .option("startingOffsets", "latest") \
    .load()

query = kafka_stream.writeStream \
    .foreachBatch(process_normalization_flow) \
    .option("checkpointLocation", "/tmp/spark-norm-checkpoint") \
    .trigger(processingTime="10 seconds") \
    .start()
#s
query.awaitTermination()