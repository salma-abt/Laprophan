# =============================================================================
# FAAS4U — I/O Module (Fabric Delta vs Local CSV)
# =============================================================================
from __future__ import annotations

import os
import logging

logger = logging.getLogger(__name__)


def detect_spark() -> bool:
    """Checks if a live Spark session is available."""
    try:
        from pyspark.sql import SparkSession  # type: ignore
        s = SparkSession.getActiveSession()
        return s is not None
    except ImportError:
        return False


def write_delta(df, table_name: str) -> None:
    """Writes a Spark DataFrame to Delta Lake."""
    cnt = df.count()
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(table_name)
    logger.info(f"Delta  ← {table_name} ({cnt} rows)")


def write_csv(df, table_name: str, output_dir: str) -> None:
    """Writes a DataFrame (Spark or Pandas) to a local CSV file."""
    os.makedirs(output_dir, exist_ok=True)
    basename = table_name.split("/")[-1] + ".csv"
    path = os.path.join(output_dir, basename)

    try:
        pdf = df.toPandas()
    except AttributeError:
        pdf = df  # already Pandas

    pdf.to_csv(path, index=False)
    logger.info(f"CSV    ← {path} ({len(pdf)} rows)")


def write_output(df, table_name: str, output_mode: str, output_dir: str) -> None:
    """
    Dispatches writing to Delta or CSV based on output_mode.
    'auto' detects Spark at runtime.
    """
    if output_mode == "auto":
        mode = "fabric" if detect_spark() else "local"
    else:
        mode = output_mode

    if mode == "fabric":
        write_delta(df, table_name)
    else:
        write_csv(df, table_name, output_dir)
