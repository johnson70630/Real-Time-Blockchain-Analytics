"""Network-disabled Java/Spark/classpath/CA-store and Parquet checks."""

import hashlib
import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from runtime.jvm import verify_class_origins

spark = (
    SparkSession.builder.master("local[1]")
    .config("spark.ui.enabled", "false")
    .config("spark.hadoop.fs.s3a.input.stream.type", "classic")
    .getOrCreate()
)
try:
    jvm = spark.sparkContext._jvm
    assert spark.version == "4.0.1"
    assert jvm.java.lang.System.getProperty("java.specification.version") == "17"
    assert jvm.java.lang.System.getProperty("java.vendor") == "Eclipse Adoptium"
    assert jvm.java.awt.GraphicsEnvironment.isHeadless()
    assert jvm.org.apache.hadoop.util.VersionInfo.getVersion() == "3.4.3"
    assert jvm.org.apache.kafka.common.utils.AppInfoParser.getVersion() == "3.9.2"
    assert jvm.java.nio.charset.Charset.defaultCharset().name() == "UTF-8"
    security = verify_class_origins(spark)
    assert (
        str(jvm.io.netty.util.Version.identify().get("netty-handler").artifactVersion())
        == security["families"]["netty"]
    )
    assert jvm.io.netty.channel.epoll.Epoll.isAvailable()
    assert jvm.io.netty.handler.ssl.OpenSsl.isAvailable()
    provider = jvm.org.bouncycastle.jce.provider.BouncyCastleProvider()
    assert str(provider.getVersionStr()) == security["families"]["bouncycastle"]
    # NIST AES-GCM known-answer vector: exercise the installed BC implementation.
    gateway = spark.sparkContext._gateway
    cipher = jvm.javax.crypto.Cipher.getInstance("AES/GCM/NoPadding", provider)
    cipher.init(
        1,
        jvm.javax.crypto.spec.SecretKeySpec(gateway.new_array(jvm.byte, 16), "AES"),
        jvm.javax.crypto.spec.GCMParameterSpec(128, gateway.new_array(jvm.byte, 12)),
    )
    encrypted = bytes(
        int(value) & 255 for value in cipher.doFinal(gateway.new_array(jvm.byte, 16))
    )
    assert (
        encrypted.hex()
        == "0388dace60b6a392f328c2b971b2fe78ab6e47d42cec13bdf53a67b21257bddf"
    )
    client = jvm.jakarta.ws.rs.client.ClientBuilder.newClient()
    client.close()
    assert (
        str(jvm.javax.net.ssl.SSLContext.getDefault().getProvider().getName())
        == "SunJSSE"
    )
    print(
        "SPARK_JVM_SECURITY_FAMILIES_VERIFIED " + json.dumps(security, sort_keys=True)
    )
    loader = jvm.java.lang.Thread.currentThread().getContextClassLoader()
    for name in (
        "org.apache.spark.sql.kafka010.KafkaSourceProvider",
        "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider",
        "software.amazon.awssdk.services.s3.S3Client",
    ):
        loader.loadClass(name)

    # Construct the existing SDK/S3A clients against the new classpath without
    # making requests or using an AWS credential chain. Network is disabled too.
    region = jvm.software.amazon.awssdk.regions.Region.of("us-west-2")
    credentials = jvm.software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider.create()
    sync = (
        jvm.software.amazon.awssdk.services.s3.S3Client.builder()
        .region(region)
        .credentialsProvider(credentials)
        .build()
    )
    sync.close()
    asynchronous = (
        jvm.software.amazon.awssdk.services.s3.S3AsyncClient.builder()
        .region(region)
        .credentialsProvider(credentials)
        .httpClientBuilder(
            jvm.software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient.builder()
        )
        .build()
    )
    asynchronous.close()
    configuration = jvm.org.apache.hadoop.conf.Configuration(False)
    configuration.set("fs.s3a.endpoint.region", "us-west-2")
    configuration.set("fs.s3a.input.stream.type", "classic")
    configuration.set("fs.s3a.bucket.probe", "0")
    configuration.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
    )
    filesystem = jvm.org.apache.hadoop.fs.s3a.S3AFileSystem()
    try:
        filesystem.initialize(
            jvm.java.net.URI("s3a://offline-never-contacted"), configuration
        )
    finally:
        filesystem.close()
    print("AWS_SYNC_ASYNC_AND_S3A_CONSTRUCTION_VERIFIED_WITHOUT_REQUESTS")

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
    forbidden = {
        "emSign ECC Root CA - G3",
        "AAA Certificate Services",
        "emSign Root CA - G2",
    }
    assert store.size() >= 50 and not (names & forbidden)
    assert {"Amazon Root CA 1", "ISRG Root X1"} <= names
    store_sha256 = hashlib.sha256(cacerts.read_bytes()).hexdigest()
    assert len(sys.argv) == 2 and store_sha256 == sys.argv[1]
    print(
        "TEMURIN_TRUST_STORE_VERIFIED "
        + json.dumps(
            {
                "sha256": store_sha256,
                "anchors": store.size(),
                "forbidden_roots_present": False,
                "g1_present_per_vendor_policy": "emSign Root CA - G1" in names,
            },
            sort_keys=True,
        )
    )

    spark.range(3).write.parquet("/tmp/smoke.parquet")
    assert spark.read.parquet("/tmp/smoke.parquet").count() == 3
finally:
    spark.stop()
print("JAVA_SPARK_S3A_KAFKA_PARQUET_VERIFIED")
