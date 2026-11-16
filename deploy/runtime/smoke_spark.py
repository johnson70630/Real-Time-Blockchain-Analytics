"""Network-disabled Java/Spark/classpath/CA-store and Parquet checks."""

import hashlib
import json
import os
import sys
import zipfile
from pathlib import Path

from pyspark.sql import SparkSession
from py4j.protocol import Py4JJavaError
from runtime.jvm import verify_class_origins
from runtime.features import FORBIDDEN_CLASSES, verify_profile

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
    assert spark.sparkContext.getConf().get("spark.driver.memory") == "3g"
    assert jvm.java.lang.Runtime.getRuntime().maxMemory() == 3 * 1024**3
    print("CLOUD_SPARK_LAUNCH_CONFIGURATION_VERIFIED")
    # Exercise a TLS client context in the SDK's own namespace. This is linkage
    # and role evidence, not a fix or an exception for its retained server code.
    relocated_tls = jvm.software.amazon.awssdk.thirdparty.io.netty.handler.ssl
    client_context = (
        relocated_tls.SslContextBuilder.forClient()
        .sslProvider(relocated_tls.SslProvider.JDK)
        .build()
    )
    assert client_context.isClient()
    print("SDK_RELOCATED_NETTY_CLIENT_CONTEXT_VERIFIED")
    assert security["streaming_profile"]["profile"] == "local_streaming_pilot"
    assert not security["streaming_profile"]["excluded_classes_resolvable"]
    print("SPARK_STREAMING_FEATURE_PROFILE_VERIFIED")
    # A renamed artifact cannot bypass the resource guard. This synthetic class
    # resource is deliberately never loaded or executed; use a child loader so
    # the real Spark driver classpath remains intact for subsequent I/O checks.
    resource = FORBIDDEN_CLASSES[-1].replace(".", "/") + ".class"
    shadow = Path("/tmp/renamed-feature.jar")
    with zipfile.ZipFile(shadow, "w") as jar:
        jar.writestr(resource, b"synthetic resource; never executable")
    gateway = spark.sparkContext._gateway
    urls = gateway.new_array(jvm.java.net.URL, 1)
    urls[0] = jvm.java.io.File(str(shadow)).toURI().toURL()
    child = jvm.java.net.URLClassLoader.newInstance(
        urls, jvm.java.lang.Thread.currentThread().getContextClassLoader()
    )
    try:
        try:
            verify_profile(child)
        except RuntimeError as error:
            assert str(error) == "PILOT_STREAMING_FEATURE_PROFILE_REQUIRED"
        else:
            raise AssertionError("Renamed excluded class resource was accepted")
    finally:
        child.close()
        shadow.unlink()
    print("RENAMED_EXCLUDED_CLASS_RESOURCE_REJECTED")
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
    immutable = jvm.com.google.common.collect.ImmutableMap.of("chain", "arbitrum")
    assert immutable.get("chain") == "arbitrum"
    cache = jvm.com.google.common.cache.CacheBuilder.newBuilder().maximumSize(2).build()
    for key in ("one", "two", "three"):
        cache.put(key, key)
    cache.cleanUp()
    assert cache.size() == 2
    future = jvm.com.google.common.util.concurrent.SettableFuture.create()
    assert future.set("offline-ready")
    assert future.get() == "offline-ready"
    print("GUAVA_COLLECTION_CACHE_AND_FAILUREACCESS_VERIFIED")

    parsed = jvm.org.jline.reader.impl.DefaultParser().parse("pilot arbitrum", 14)
    assert list(parsed.words()) == ["pilot", "arbitrum"]
    assert str(jvm.org.jline.utils.AttributedString("offline-pilot")) == "offline-pilot"
    print("JLINE_PARSER_VERIFIED_WITHOUT_LISTENERS")

    output_archive_stream = jvm.java.io.ByteArrayOutputStream()
    output_archive = jvm.org.apache.jute.BinaryOutputArchive.getArchive(
        output_archive_stream
    )
    identity = jvm.org.apache.zookeeper.data.Id("digest", "synthetic-offline-id")
    identity.serialize(output_archive, "identity")
    input_archive = jvm.org.apache.jute.BinaryInputArchive.getArchive(
        jvm.java.io.ByteArrayInputStream(output_archive_stream.toByteArray())
    )
    decoded_identity = jvm.org.apache.zookeeper.data.Id()
    decoded_identity.deserialize(input_archive, "identity")
    assert decoded_identity.getScheme() == "digest"
    assert decoded_identity.getId() == "synthetic-offline-id"
    client_config = jvm.org.apache.zookeeper.client.ZKClientConfig()
    assert client_config is not None
    curator = (
        jvm.org.apache.curator.framework.CuratorFrameworkFactory.builder()
        .connectString("127.0.0.1:2181")
        .retryPolicy(jvm.org.apache.curator.retry.ExponentialBackoffRetry(100, 1))
        .build()
    )
    assert str(curator.getState()) == "LATENT"
    curator.close()
    print("ZOOKEEPER_JUTE_CURATOR_LINKAGE_VERIFIED_WITHOUT_STARTING_CLIENT")
    mapper = jvm.com.fasterxml.jackson.databind.ObjectMapper()
    assert str(mapper.version()) == security["families"]["jackson"]
    mapper.registerModule(jvm.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule())
    mapper.disable(
        jvm.com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS
    )
    instant = jvm.java.time.Instant.parse("2026-01-01T00:00:00Z")
    assert mapper.writeValueAsString(instant) == '"2026-01-01T00:00:00Z"'
    scala_module = (
        loader.loadClass("com.fasterxml.jackson.module.scala.DefaultScalaModule$")
        .getField("MODULE$")
        .get(None)
    )
    mapper.registerModule(scala_module)
    values = jvm.java.util.LinkedHashMap()
    values.put("chain", "arbitrum")
    values.put("count", 3)
    scala_values = jvm.scala.jdk.javaapi.CollectionConverters.asScala(values)
    assert (
        mapper.readTree(mapper.writeValueAsString(scala_values)).get("count").asInt()
        == 3
    )
    yaml = jvm.com.fasterxml.jackson.databind.ObjectMapper(
        jvm.com.fasterxml.jackson.dataformat.yaml.YAMLFactory()
    )
    assert (
        yaml.readTree("chain: arbitrum\ncount: 3\n").get("chain").asText() == "arbitrum"
    )
    constraints = (
        jvm.com.fasterxml.jackson.core.StreamReadConstraints.builder()
        .maxNestingDepth(5)
        .build()
    )
    factory = (
        jvm.com.fasterxml.jackson.core.JsonFactory.builder()
        .streamReadConstraints(constraints)
        .build()
    )
    parser = factory.createParser("[[[[[[0]]]]]]")
    try:
        try:
            while parser.nextToken() is not None:
                pass
        except Py4JJavaError as error:
            assert (
                error.java_exception.getClass().getName()
                == "com.fasterxml.jackson.core.exc.StreamConstraintsException"
            )
        else:
            raise AssertionError("Configured JSON nesting limit was not enforced")
    finally:
        parser.close()
    print("JACKSON_JSON_YAML_TIME_SCALA_AND_LIMITS_VERIFIED")

    payload = b"pilot-codec-compatibility:" * 8
    # Captured from the f209652 image's Snappy 1.1.10.7 / Aircompressor 2.0.2.
    # Fixed synthetic frames verify existing data remains readable after update.
    legacy_snappy = bytes.fromhex(
        "d0016470696c6f742d636f6465632d636f6d7061746962696c6974793afe1a00fe1a00d61a00"
    )
    legacy_lz4 = bytes.fromhex(
        "ff0b70696c6f742d636f6465632d636f6d7061746962696c6974793a1a009e506c6974793a"
    )
    snappy = jvm.org.xerial.snappy.Snappy
    assert bytes(snappy.uncompress(snappy.compress(payload))) == payload
    assert bytes(snappy.uncompress(legacy_snappy)) == payload

    def buffer_bytes(buffer):
        return bytes(int(buffer.get(i)) & 255 for i in range(buffer.position()))

    for compressor, decompressor, legacy in (
        (
            jvm.io.airlift.compress.snappy.SnappyCompressor(),
            jvm.io.airlift.compress.snappy.SnappyDecompressor(),
            legacy_snappy,
        ),
        (
            jvm.io.airlift.compress.lz4.Lz4Compressor(),
            jvm.io.airlift.compress.lz4.Lz4Decompressor(),
            legacy_lz4,
        ),
    ):
        compressed = jvm.java.nio.ByteBuffer.allocate(
            compressor.maxCompressedLength(len(payload))
        )
        compressor.compress(jvm.java.nio.ByteBuffer.wrap(payload), compressed)
        for frame in (buffer_bytes(compressed), legacy):
            output = jvm.java.nio.ByteBuffer.allocate(len(payload))
            decompressor.decompress(jvm.java.nio.ByteBuffer.wrap(frame), output)
            assert buffer_bytes(output) == payload
    print("NATIVE_SNAPPY_AND_AIRCOMPRESSOR_COMPATIBILITY_VERIFIED")
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
    assert sorted(
        row.id for row in spark.read.parquet("/tmp/smoke.parquet").collect()
    ) == [0, 1, 2]
finally:
    spark.stop()
print("JAVA_SPARK_S3A_KAFKA_PARQUET_VERIFIED")
