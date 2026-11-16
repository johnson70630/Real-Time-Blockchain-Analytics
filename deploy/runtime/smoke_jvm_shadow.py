"""The cloud startup guard must reject a driver lacking override precedence."""

from pyspark.sql import SparkSession

from runtime.jvm import verify_class_origins

spark = (
    SparkSession.builder.master("local[1]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
try:
    try:
        verify_class_origins(spark)
    except RuntimeError as error:
        assert str(error) == "JVM_SECURITY_CLASSPATH_MISMATCH"
    else:
        raise AssertionError("Shadowed JVM dependencies were accepted")
finally:
    spark.stop()
print("JVM_CLASSPATH_SHADOW_REJECTED_BEFORE_S3")
