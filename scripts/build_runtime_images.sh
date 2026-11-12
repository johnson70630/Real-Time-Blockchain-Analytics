#!/usr/bin/env bash
# Local builds only: no ECR push, AWS login, or deployment.
set -euo pipefail
docker build --platform linux/amd64 -f deploy/runtime/Dockerfile.producer -t blockchain-pilot-producer:offline .
docker build --platform linux/amd64 -f deploy/runtime/Dockerfile.spark -t blockchain-pilot-spark:offline .
docker run --rm --network none --read-only --tmpfs /tmp:size=2g,mode=1777,exec \
  --entrypoint python blockchain-pilot-spark:offline -c \
  'from pyspark.sql import SparkSession; s=SparkSession.builder.master("local[1]").config("spark.ui.enabled","false").getOrCreate(); j=s.sparkContext._jvm; assert s.version=="4.0.1"; assert j.java.lang.System.getProperty("java.specification.version")=="17"; assert j.org.apache.hadoop.util.VersionInfo.getVersion()=="3.4.1"; c=j.java.lang.Thread.currentThread().getContextClassLoader(); [c.loadClass(n) for n in ["org.apache.spark.sql.kafka010.KafkaSourceProvider","org.apache.hadoop.fs.s3a.S3AFileSystem","org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider","software.amazon.awssdk.services.s3.S3Client"]]; s.range(3).write.parquet("/tmp/smoke.parquet"); assert s.read.parquet("/tmp/smoke.parquet").count()==3; s.stop(); print("JAVA_SPARK_S3A_KAFKA_PARQUET_VERIFIED")'
