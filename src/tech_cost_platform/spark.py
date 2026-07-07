"""Local SparkSession bootstrap with Delta Lake enabled."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Mapping

from pyspark.sql import SparkSession

DELTA_SQL_EXTENSIONS = "io.delta.sql.DeltaSparkSessionExtension"
DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
DELTA_LOCAL_LOG_STORE = "org.apache.spark.sql.delta.storage.LocalLogStore"


def repo_root() -> Path:
    """Return the repository root from the src package."""
    return Path(__file__).resolve().parents[2]


def resolve_delta_jars(jars_dir: Path | None = None) -> list[str]:
    """Return the local Delta runtime jars committed with the repository."""
    base_dir = jars_dir or repo_root() / "jars"
    jar_paths = sorted(base_dir.glob("*.jar"))
    if not jar_paths:
        raise RuntimeError(
            "No Delta Lake jars were found in the repository 'jars/' directory. "
            "Populate that directory before running the local Spark pipeline."
        )
    return [str(path.resolve()) for path in jar_paths]


def configure_windows_hadoop(root: Path) -> str | None:
    """Point local Windows Spark at the bundled winutils shim."""
    if os.name != "nt":
        return None

    hadoop_home = root / "tools" / "hadoop"
    winutils_path = hadoop_home / "bin" / "winutils.exe"
    if not winutils_path.exists():
        raise RuntimeError(
            "Windows local Spark requires tools/hadoop/bin/winutils.exe for local file writes."
        )

    hadoop_home_resolved = hadoop_home.resolve()
    hadoop_home_str = hadoop_home_resolved.as_posix()

    os.environ.setdefault("HADOOP_HOME", hadoop_home_str)
    os.environ["PATH"] = f"{winutils_path.parent.resolve()}{os.pathsep}{os.environ.get('PATH', '')}"
    return hadoop_home_str


def build_spark_session(
    *,
    app_name: str = "tech-cost-platform",
    master: str = "local[*]",
    warehouse_dir: str | Path | None = None,
    extra_conf: Mapping[str, str] | None = None,
) -> SparkSession:
    """Build a local Spark session that uses only repo-local Delta jars."""
    root = repo_root()
    warehouse_path = Path(warehouse_dir) if warehouse_dir is not None else root / "data" / "warehouse"
    local_dir = root / "data" / "spark-local"
    delta_classpath = os.pathsep.join(resolve_delta_jars())
    hadoop_home = configure_windows_hadoop(root)
    python_executable = sys.executable

    warehouse_path.mkdir(parents=True, exist_ok=True)
    local_dir.mkdir(parents=True, exist_ok=True)

    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "127.0.0.1")
    os.environ.setdefault("PYSPARK_PYTHON", python_executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", python_executable)

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.extensions", DELTA_SQL_EXTENSIONS)
        .config("spark.sql.catalog.spark_catalog", DELTA_CATALOG)
        .config("spark.delta.logStore.class", DELTA_LOCAL_LOG_STORE)
        .config("spark.sql.warehouse.dir", str(warehouse_path.resolve()))
        .config("spark.local.dir", str(local_dir.resolve()))
        .config("spark.driver.extraClassPath", delta_classpath)
        .config("spark.executor.extraClassPath", delta_classpath)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.pyspark.python", python_executable)
        .config("spark.pyspark.driver.python", python_executable)
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
    )

    if hadoop_home is not None:
        java_options = f"-Dhadoop.home.dir={hadoop_home}"
        builder = (
            builder.config("spark.driver.extraJavaOptions", java_options)
            .config("spark.executor.extraJavaOptions", java_options)
        )

    if extra_conf:
        for key, value in extra_conf.items():
            builder = builder.config(key, value)

    return builder.getOrCreate()
