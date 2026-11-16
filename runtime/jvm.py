"""Verify the cloud driver's actual class origins before starting S3 work."""

import json
from pathlib import Path


OVERRIDES = Path("/opt/pilot/jvm-overrides")
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


def verify_classloader(jvm):
    """Check real Java classes without initializing a Spark RPC environment."""
    lock = json.loads((OVERRIDES / "patches.lock.json").read_text())
    if lock["pyspark_version"] != "4.0.1":
        raise RuntimeError("REVIEWED_SPARK_JAVA_HADOOP_VERSIONS_REQUIRED")
    loader = jvm.java.lang.Thread.currentThread().getContextClassLoader()
    origins = {}
    for name, artifact, family in CLASSES:
        cls = loader.loadClass(name)
        location = cls.getProtectionDomain().getCodeSource().getLocation()
        path = str(location.toURI().getPath())
        expected = str(OVERRIDES / f"{artifact}-{lock['families'][family]}.jar")
        if location.getProtocol() != "file" or path != expected:
            raise RuntimeError("JVM_SECURITY_CLASSPATH_MISMATCH")
        origins[name] = path
    return {"families": lock["families"], "class_origins": origins}


def verify_class_origins(spark):
    jvm = spark.sparkContext._jvm
    if (
        spark.version != "4.0.1"
        or jvm.java.lang.System.getProperty("java.specification.version") != "17"
        or jvm.org.apache.hadoop.util.VersionInfo.getVersion() != "3.4.1"
    ):
        raise RuntimeError("REVIEWED_SPARK_JAVA_HADOOP_VERSIONS_REQUIRED")
    if spark.conf.get("spark.sql.catalogImplementation") != "in-memory":
        raise RuntimeError("PILOT_IN_MEMORY_CATALOG_REQUIRED")
    return verify_classloader(jvm)
