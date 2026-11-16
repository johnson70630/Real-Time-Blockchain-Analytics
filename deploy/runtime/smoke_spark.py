"""Network-disabled Java/Spark/classpath/CA-store and Parquet checks."""

import hashlib
import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
try:
    jvm = spark.sparkContext._jvm
    assert spark.version == "4.0.1"
    assert jvm.java.lang.System.getProperty("java.specification.version") == "17"
    assert jvm.java.lang.System.getProperty("java.vendor") == "Eclipse Adoptium"
    assert jvm.java.awt.GraphicsEnvironment.isHeadless()
    assert jvm.org.apache.hadoop.util.VersionInfo.getVersion() == "3.4.1"
    assert jvm.org.apache.kafka.common.utils.AppInfoParser.getVersion() == "3.9.2"
    assert jvm.java.nio.charset.Charset.defaultCharset().name() == "UTF-8"
    loader = jvm.java.lang.Thread.currentThread().getContextClassLoader()
    for name in (
        "org.apache.spark.sql.kafka010.KafkaSourceProvider",
        "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider",
        "software.amazon.awssdk.services.s3.S3Client",
    ):
        loader.loadClass(name)

    # A copied JRE is outside dpkg's package inventory. Verify the actual CA
    # store instead of treating absence from an OS scan as a Java fix.
    cacerts = Path(os.environ["JAVA_HOME"]) / "lib/security/cacerts"
    store = jvm.java.security.KeyStore.getInstance("JKS")
    stream = jvm.java.io.FileInputStream(str(cacerts))
    try:
        store.load(stream, None)
    finally:
        stream.close()
    names = set()
    aliases = store.aliases()
    while aliases.hasMoreElements():
        certificate = store.getCertificate(aliases.nextElement())
        subject = certificate.getSubjectX500Principal().getName()
        for rdn in jvm.javax.naming.ldap.LdapName(subject).getRdns():
            if rdn.getType().upper() == "CN":
                names.add(str(rdn.getValue()))
    forbidden = {"emSign ECC Root CA - G3", "AAA Certificate Services", "emSign Root CA - G2"}
    assert store.size() >= 50 and not (names & forbidden)
    assert {"Amazon Root CA 1", "ISRG Root X1"} <= names
    store_sha256 = hashlib.sha256(cacerts.read_bytes()).hexdigest()
    assert len(sys.argv) == 2 and store_sha256 == sys.argv[1]
    print("TEMURIN_TRUST_STORE_VERIFIED " + json.dumps({
        "sha256": store_sha256,
        "anchors": store.size(), "forbidden_roots_present": False,
        "g1_present_per_vendor_policy": "emSign Root CA - G1" in names,
    }, sort_keys=True))

    spark.range(3).write.parquet("/tmp/smoke.parquet")
    assert spark.read.parquet("/tmp/smoke.parquet").count() == 3
finally:
    spark.stop()
print("JAVA_SPARK_S3A_KAFKA_PARQUET_VERIFIED")
