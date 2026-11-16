"""Verify the cloud driver's actual class origins before starting S3 work."""

import json
from pathlib import Path

from runtime.features import verify_profile


OVERRIDES = Path("/opt/pilot/jvm-overrides")
JARS = Path("/usr/local/lib/python3.12/site-packages/pyspark/jars")
CLASSES = (
    ("io.netty.util.Version", "netty-common", "netty"),
    ("io.netty.buffer.ByteBuf", "netty-buffer", "netty"),
    ("io.netty.channel.Channel", "netty-transport", "netty"),
    ("io.netty.handler.codec.ByteToMessageDecoder", "netty-codec", "netty"),
    ("io.netty.handler.codec.http.HttpObject", "netty-codec-http", "netty"),
    ("io.netty.handler.ssl.SslClientHelloHandler", "netty-handler", "netty"),
    ("io.netty.channel.epoll.Epoll", "netty-transport-classes-epoll", "netty"),
    ("io.netty.internal.tcnative.Library", "netty-tcnative-classes", "tcnative"),
    (
        "org.bouncycastle.jce.provider.BouncyCastleProvider",
        "bcprov-jdk18on",
        "bouncycastle",
    ),
    ("org.glassfish.jersey.client.JerseyClient", "jersey-client", "jersey"),
)
CONNECTOR_CLASSES = (
    ("org.apache.hadoop.util.VersionInfo", "hadoop-client-api", "hadoop"),
    (
        "org.apache.hadoop.shaded.org.apache.avro.Schema",
        "hadoop-client-runtime",
        "hadoop",
    ),
    ("org.apache.hadoop.fs.s3a.S3AFileSystem", "hadoop-aws", "hadoop"),
    ("software.amazon.awssdk.services.s3.S3Client", "bundle", "aws_sdk"),
    (
        "software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient",
        "bundle",
        "aws_sdk",
    ),
    (
        "software.amazon.awssdk.thirdparty.io.netty.handler.ssl.SslClientHelloHandler",
        "bundle",
        "aws_sdk",
    ),
    (
        "software.amazon.awssdk.thirdparty.org.apache.http.conn.ssl.SSLConnectionSocketFactory",
        "bundle",
        "aws_sdk",
    ),
    ("org.wildfly.openssl.OpenSSLProvider", "wildfly-openssl", "wildfly_openssl"),
    (
        "software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory",
        "analyticsaccelerator-s3",
        "analytics_accelerator",
    ),
)
NORMAL_CLASSES = (
    ("com.fasterxml.jackson.annotation.JsonCreator", "jackson-annotations", "jackson"),
    ("com.fasterxml.jackson.core.JsonFactory", "jackson-core", "jackson"),
    ("com.fasterxml.jackson.databind.ObjectMapper", "jackson-databind", "jackson"),
    (
        "com.fasterxml.jackson.dataformat.yaml.YAMLFactory",
        "jackson-dataformat-yaml",
        "jackson",
    ),
    (
        "com.fasterxml.jackson.datatype.jsr310.JavaTimeModule",
        "jackson-datatype-jsr310",
        "jackson",
    ),
    (
        "com.fasterxml.jackson.module.scala.DefaultScalaModule",
        "jackson-module-scala_2.13",
        "jackson",
    ),
    ("io.airlift.compress.snappy.SnappyDecompressor", "aircompressor", "aircompressor"),
    ("org.xerial.snappy.Snappy", "snappy-java", "snappy"),
)


def verify_classloader(jvm):
    """Check real Java classes without initializing a Spark RPC environment."""
    lock = json.loads((OVERRIDES / "patches.lock.json").read_text())
    if (
        lock["pyspark_version"] != "4.0.1"
        or lock["components"]["hadoop"] != lock["families"]["hadoop"]
    ):
        raise RuntimeError("REVIEWED_SPARK_JAVA_HADOOP_VERSIONS_REQUIRED")
    loader = jvm.java.lang.Thread.currentThread().getContextClassLoader()
    origins = {}
    expected_classes = (
        [
            (name, OVERRIDES / f"{artifact}-{lock['families'][family]}.jar")
            for name, artifact, family in CLASSES
        ]
        + [
            (name, JARS / f"{artifact}-{lock['families'][family]}.jar")
            for name, artifact, family in NORMAL_CLASSES
        ]
        + [
            (name, JARS / f"{artifact}-{lock['components'][component]}.jar")
            for name, artifact, component in CONNECTOR_CLASSES
        ]
    )
    for name, expected in expected_classes:
        cls = loader.loadClass(name)
        location = cls.getProtectionDomain().getCodeSource().getLocation()
        path = str(location.toURI().getPath())
        if location.getProtocol() != "file" or path != str(expected):
            raise RuntimeError("JVM_SECURITY_CLASSPATH_MISMATCH")
        origins[name] = path
    return {
        "families": lock["families"],
        "components": lock["components"],
        "class_origins": origins,
        "streaming_profile": verify_profile(loader),
    }


def verify_class_origins(spark):
    jvm = spark.sparkContext._jvm
    lock = json.loads((OVERRIDES / "patches.lock.json").read_text())
    if (
        spark.version != "4.0.1"
        or jvm.java.lang.System.getProperty("java.specification.version") != "17"
        or jvm.org.apache.hadoop.util.VersionInfo.getVersion()
        != lock["components"]["hadoop"]
    ):
        raise RuntimeError("REVIEWED_SPARK_JAVA_HADOOP_VERSIONS_REQUIRED")
    if spark.conf.get("spark.sql.catalogImplementation") != "in-memory":
        raise RuntimeError("PILOT_IN_MEMORY_CATALOG_REQUIRED")
    if (
        spark.sparkContext._jsc.hadoopConfiguration().get("fs.s3a.input.stream.type")
        != "classic"
    ):
        raise RuntimeError("PILOT_CLASSIC_S3A_INPUT_STREAM_REQUIRED")
    result = verify_classloader(jvm)
    result["s3a_input_stream"] = "classic"
    return result
