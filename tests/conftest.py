import os
import time

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    # Inside Databricks: reuse the notebook's (serverless) session, never stop it.
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        yield SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
        return

    os.environ["TZ"] = "UTC"; time.tzset()                     # deterministic timestamps
    s = (SparkSession.builder.master("local[2]").appName("shopstream-tests")
         .config("spark.sql.ansi.enabled", "true")            # mimic Databricks serverless
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse"))).getOrCreate())
    yield s
    s.stop()
