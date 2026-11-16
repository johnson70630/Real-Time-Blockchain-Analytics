"""The cloud startup guard must reject a driver lacking override precedence."""

from pyspark.java_gateway import launch_gateway

from runtime.jvm import verify_classloader

# Starting Spark with a mixed classpath can fail in Netty RPC initialization
# before the guard is reached, depending on filesystem JAR discovery order.
# Launch only the real JVM gateway: inspect classes without initializing Spark.
gateway = launch_gateway()
try:
    try:
        verify_classloader(gateway.jvm)
    except RuntimeError as error:
        assert str(error) == "JVM_SECURITY_CLASSPATH_MISMATCH"
    else:
        raise AssertionError("Shadowed JVM dependencies were accepted")
finally:
    try:
        gateway.shutdown()
    finally:
        gateway.proc.stdin.close()
        gateway.proc.wait(timeout=30)
print("JVM_CLASSPATH_SHADOW_REJECTED_BEFORE_S3")
