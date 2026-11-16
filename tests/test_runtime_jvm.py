"""Reject shadowed classes even when a patched version is installed on disk."""

import json
from types import SimpleNamespace as NS

import pytest

from runtime import jvm


def fake_spark(override, shadow=None, *, version="4.0.1", java="17"):
    families = {
        "netty": "4.1.140.Final",
        "tcnative": "2.0.85.Final",
        "bouncycastle": "1.87",
        "jersey": "3.0.18",
    }
    paths = {
        name: str(override / f"{artifact}-{families[family]}.jar")
        for name, artifact, family in jvm.CLASSES
    }
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

    loader = NS(loadClass=load)
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
            apache=NS(hadoop=NS(util=NS(VersionInfo=NS(getVersion=lambda: "3.4.1"))))
        ),
    )
    return NS(
        version=version, sparkContext=NS(_jvm=vm), conf=NS(get=lambda _: "in-memory")
    ), families


@pytest.mark.parametrize("shadow", [None] + [row[0] for row in jvm.CLASSES])
def test_actual_class_sources_are_required(tmp_path, monkeypatch, shadow):
    monkeypatch.setattr(jvm, "OVERRIDES", tmp_path)
    spark, families = fake_spark(tmp_path, shadow)
    (tmp_path / "patches.lock.json").write_text(
        json.dumps({"pyspark_version": "4.0.1", "families": families})
    )
    if shadow:
        with pytest.raises(RuntimeError, match="JVM_SECURITY_CLASSPATH_MISMATCH"):
            jvm.verify_class_origins(spark)
    else:
        assert jvm.verify_class_origins(spark)["families"] == families


@pytest.mark.parametrize("version,java", [("4.0.5", "17"), ("4.0.1", "21")])
def test_unreviewed_engine_versions_fail_closed(tmp_path, monkeypatch, version, java):
    monkeypatch.setattr(jvm, "OVERRIDES", tmp_path)
    spark, families = fake_spark(tmp_path, version=version, java=java)
    (tmp_path / "patches.lock.json").write_text(
        json.dumps({"pyspark_version": "4.0.1", "families": families})
    )
    with pytest.raises(
        RuntimeError, match="REVIEWED_SPARK_JAVA_HADOOP_VERSIONS_REQUIRED"
    ):
        jvm.verify_class_origins(spark)


def test_hive_catalog_cannot_be_enabled_for_the_pilot(tmp_path, monkeypatch):
    monkeypatch.setattr(jvm, "OVERRIDES", tmp_path)
    spark, families = fake_spark(tmp_path)
    (tmp_path / "patches.lock.json").write_text(
        json.dumps({"pyspark_version": "4.0.1", "families": families})
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
        json.dumps({"pyspark_version": "4.0.1", "families": families})
    )
    with pytest.raises(RuntimeError, match="JVM_SECURITY_CLASSPATH_MISMATCH"):
        jvm.verify_classloader(spark.sparkContext._jvm)
