"""Reject shadowed classes even when a patched version is installed on disk."""

import json
from types import SimpleNamespace as NS

import pytest

from runtime import jvm
from runtime import features

COMPONENTS = {
    "hadoop": "3.4.3",
    "aws_sdk": "2.35.4",
    "wildfly_openssl": "2.2.5.Final",
    "analytics_accelerator": "1.3.1",
}


def fake_spark(override, shadow=None, *, version="4.0.1", java="17", hadoop="3.4.3"):
    families = {
        "netty": "4.1.140.Final",
        "tcnative": "2.0.85.Final",
        "bouncycastle": "1.87",
        "jersey": "3.0.18",
        "hadoop": "3.4.3",
        "jackson": "2.18.12",
        "aircompressor": "2.0.3",
        "snappy": "1.1.10.12",
        "guava": "33.7.2-jre",
        "failureaccess": "1.0.3",
        "jline": "3.30.16",
        "zookeeper": "3.9.7",
    }
    paths = {
        name: str(override / f"{artifact}-{families[family]}.jar")
        for name, artifact, family in jvm.CLASSES
    }
    paths.update(
        {
            name: str(
                jvm.JARS
                / f"{artifact}-{families[family]}{jvm.NORMAL_CLASSIFIERS.get(artifact, '')}.jar"
            )
            for name, artifact, family in jvm.NORMAL_CLASSES
        }
    )
    paths.update(
        {
            name: str(jvm.JARS / f"{artifact}-{COMPONENTS[component]}.jar")
            for name, artifact, component in jvm.CONNECTOR_CLASSES
        }
    )
    if shadow:
        paths[shadow] = "/pyspark/jars/bundle-2.24.6.jar"

    def load(name):
        location = NS(
            getProtocol=lambda: "file", toURI=lambda: NS(getPath=lambda: paths[name])
        )
        return NS(
            getProtectionDomain=lambda: NS(
                getCodeSource=lambda: NS(getLocation=lambda: location)
            )
        )

    loader = NS(
        loadClass=load, getResources=lambda _: NS(hasMoreElements=lambda: False)
    )
    vm = NS(
        java=NS(
            lang=NS(
                System=NS(getProperty=lambda _: java),
                Thread=NS(
                    currentThread=lambda: NS(getContextClassLoader=lambda: loader)
                ),
            )
        ),
        org=NS(
            apache=NS(hadoop=NS(util=NS(VersionInfo=NS(getVersion=lambda: hadoop))))
        ),
    )
    return NS(
        version=version,
        sparkContext=NS(
            _jvm=vm, _jsc=NS(hadoopConfiguration=lambda: NS(get=lambda _: "classic"))
        ),
        conf=NS(get=lambda _: "in-memory"),
    ), families


@pytest.fixture(autouse=True)
def reviewed_streaming_profile(tmp_path, monkeypatch):
    from pathlib import Path

    monkeypatch.setattr(features, "MANIFEST", tmp_path / "streaming-profile.json")
    monkeypatch.setattr(features, "JARS", tmp_path / "jars")
    features.JARS.mkdir()
    features.MANIFEST.write_bytes(
        (
            Path(__file__).resolve().parents[1]
            / "deploy/runtime/spark-features.lock.json"
        ).read_bytes()
    )


@pytest.mark.parametrize(
    "shadow",
    [None]
    + [row[0] for row in jvm.CLASSES + jvm.NORMAL_CLASSES + jvm.CONNECTOR_CLASSES],
)
def test_actual_class_sources_are_required(tmp_path, monkeypatch, shadow):
    monkeypatch.setattr(jvm, "OVERRIDES", tmp_path)
    spark, families = fake_spark(tmp_path, shadow)
    (tmp_path / "patches.lock.json").write_text(
        json.dumps(
            {"pyspark_version": "4.0.1", "families": families, "components": COMPONENTS}
        )
    )
    if shadow:
        with pytest.raises(RuntimeError, match="JVM_SECURITY_CLASSPATH_MISMATCH"):
            jvm.verify_class_origins(spark)
    else:
        assert jvm.verify_class_origins(spark)["families"] == families


def test_normal_class_origins_match_the_reviewed_artifact_filenames(
    tmp_path, monkeypatch
):
    from pathlib import Path

    lock = json.loads(
        (
            Path(__file__).resolve().parents[1]
            / "deploy/runtime/spark-jvm-patches.lock.json"
        ).read_text()
    )
    filenames = {
        row["new"]["path"].split("/")[-3]: row["new"]["path"].split("/")[-1]
        for row in lock["replacements"]
    }
    monkeypatch.setattr(jvm, "OVERRIDES", tmp_path)
    spark, families = fake_spark(tmp_path)
    (tmp_path / "patches.lock.json").write_text(
        json.dumps(
            {"pyspark_version": "4.0.1", "families": families, "components": COMPONENTS}
        )
    )
    origins = jvm.verify_class_origins(spark)["class_origins"]
    for name, artifact, _ in jvm.NORMAL_CLASSES:
        assert Path(origins[name]).name == filenames[artifact]


@pytest.mark.parametrize("version,java", [("4.0.5", "17"), ("4.0.1", "21")])
def test_unreviewed_engine_versions_fail_closed(tmp_path, monkeypatch, version, java):
    monkeypatch.setattr(jvm, "OVERRIDES", tmp_path)
    spark, families = fake_spark(tmp_path, version=version, java=java)
    (tmp_path / "patches.lock.json").write_text(
        json.dumps(
            {"pyspark_version": "4.0.1", "families": families, "components": COMPONENTS}
        )
    )
    with pytest.raises(
        RuntimeError, match="REVIEWED_SPARK_JAVA_HADOOP_VERSIONS_REQUIRED"
    ):
        jvm.verify_class_origins(spark)


def test_hive_catalog_cannot_be_enabled_for_the_pilot(tmp_path, monkeypatch):
    monkeypatch.setattr(jvm, "OVERRIDES", tmp_path)
    spark, families = fake_spark(tmp_path)
    (tmp_path / "patches.lock.json").write_text(
        json.dumps(
            {"pyspark_version": "4.0.1", "families": families, "components": COMPONENTS}
        )
    )
    spark.conf.get = lambda _: "hive"
    with pytest.raises(RuntimeError, match="PILOT_IN_MEMORY_CATALOG_REQUIRED"):
        jvm.verify_class_origins(spark)


def test_classloader_guard_needs_no_spark_context_or_rpc_initialization(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(jvm, "OVERRIDES", tmp_path)
    spark, families = fake_spark(tmp_path, jvm.CLASSES[0][0])
    (tmp_path / "patches.lock.json").write_text(
        json.dumps(
            {"pyspark_version": "4.0.1", "families": families, "components": COMPONENTS}
        )
    )
    with pytest.raises(RuntimeError, match="JVM_SECURITY_CLASSPATH_MISMATCH"):
        jvm.verify_classloader(spark.sparkContext._jvm)


def test_unreviewed_s3a_stream_mode_is_rejected(tmp_path, monkeypatch):
    monkeypatch.setattr(jvm, "OVERRIDES", tmp_path)
    spark, families = fake_spark(tmp_path)
    (tmp_path / "patches.lock.json").write_text(
        json.dumps(
            {"pyspark_version": "4.0.1", "families": families, "components": COMPONENTS}
        )
    )
    spark.sparkContext._jsc.hadoopConfiguration = lambda: NS(get=lambda _: "analytics")
    with pytest.raises(RuntimeError, match="PILOT_CLASSIC_S3A_INPUT_STREAM_REQUIRED"):
        jvm.verify_class_origins(spark)


def test_old_hadoop_version_cannot_be_used_with_the_new_manifest(tmp_path, monkeypatch):
    monkeypatch.setattr(jvm, "OVERRIDES", tmp_path)
    spark, families = fake_spark(tmp_path, hadoop="3.4.1")
    (tmp_path / "patches.lock.json").write_text(
        json.dumps(
            {"pyspark_version": "4.0.1", "families": families, "components": COMPONENTS}
        )
    )
    with pytest.raises(
        RuntimeError, match="REVIEWED_SPARK_JAVA_HADOOP_VERSIONS_REQUIRED"
    ):
        jvm.verify_class_origins(spark)
