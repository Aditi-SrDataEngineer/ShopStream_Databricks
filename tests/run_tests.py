# Databricks notebook source
# MAGIC %md
# MAGIC # Run the unit tests inside Databricks
# MAGIC Uses the notebook's own Spark session (serverless). The E2E test needs local `data/landing`
# MAGIC files, so it is skipped here; the unit tests cover the shared transformation library.

# COMMAND ----------

# MAGIC %pip install pytest -q

# COMMAND ----------

import os, sys
import pytest

sys.dont_write_bytecode = True        # workspace files are read-only for .pyc
repo_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
code = pytest.main([os.path.join(repo_root, "tests", "test_transforms.py"), "-q", "-p", "no:cacheprovider",
                    "--rootdir", repo_root])
assert code == 0, f"pytest failed with exit code {code}"
