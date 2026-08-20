# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Generate Repo SCD
# MAGIC
# MAGIC Triggered by changes to `lb_github_repos_history`.

# COMMAND ----------

# DBTITLE 1,Configuration
import re

dbutils.widgets.text("catalog", "bootcamp_students", "Unity Catalog name")
dbutils.widgets.text("schema", "sri", "Schema name")

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()

identifier_pattern = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
for parameter_name, parameter_value in (("catalog", catalog), ("schema", schema)):
    if not identifier_pattern.fullmatch(parameter_value):
        raise ValueError(
            f"Invalid {parameter_name} identifier: {parameter_value!r}. "
            "Use letters, numbers, and underscores, starting with a letter or underscore."
        )

# Source and target configuration derived from the notebook task parameters.
SOURCE_TABLE = f"{catalog}.{schema}.lb_github_repos_history"
TARGET_TABLE = f"{catalog}.{schema}.github_repos_scd"

# Watermark table tracks last processed _sort_by per source
WATERMARK_TABLE = f"{catalog}.{schema}.scd_watermarks"

# COMMAND ----------

# DBTITLE 1,Initialize watermark and get incremental window
# Create watermark table if it doesn't exist
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {WATERMARK_TABLE} (
        source_table STRING,
        last_processed_sort_by LONG,
        last_processed_at TIMESTAMP
    )
    USING DELTA
""")

# Get the last processed _sort_by value
watermark_df = spark.sql(f"""
    SELECT COALESCE(MAX(last_processed_sort_by), -1) AS last_sort_by
    FROM {WATERMARK_TABLE}
    WHERE source_table = '{SOURCE_TABLE}'
""")
last_processed_sort_by = watermark_df.collect()[0]["last_sort_by"]
print(f"Last processed _sort_by: {last_processed_sort_by}")

# COMMAND ----------

# DBTITLE 1,Read new events incrementally
# Read only new events since last watermark
# Skip update_preimage since postimage has the new state
new_events_df = spark.sql(f"""
    SELECT
        full_name,
        _pg_change_type,
        _sort_by,
        _timestamp,
        is_favorite,
        CASE
            WHEN _pg_change_type = 'delete' THEN TRUE
            ELSE FALSE
        END AS is_deleted
    FROM {SOURCE_TABLE}
    WHERE _sort_by > {last_processed_sort_by}
      AND _pg_change_type != 'update_preimage'
    ORDER BY _sort_by ASC
""")

new_event_count = new_events_df.count()
print(f"New events to process: {new_event_count}")

if new_event_count == 0:
    dbutils.notebook.exit("No new events to process")

# COMMAND ----------

# DBTITLE 1,Compute SCD state changes
from pyspark.sql import functions as F, Window

# For each repo, determine state transitions
# A new SCD record is needed when (is_deleted, is_favorite) changes
windowed = new_events_df.withColumn(
    "prev_is_deleted",
    F.lag("is_deleted").over(Window.partitionBy("full_name").orderBy("_sort_by"))
).withColumn(
    "prev_is_favorite",
    F.lag("is_favorite").over(Window.partitionBy("full_name").orderBy("_sort_by"))
).withColumn(
    "state_changed",
    F.when(
        F.col("prev_is_deleted").isNull(), F.lit(True)  # First event for this repo in batch
    ).otherwise(
        (F.col("is_deleted") != F.col("prev_is_deleted")) |
        (F.col("is_favorite") != F.col("prev_is_favorite"))
    )
)

# Keep only state-change events (these define new SCD records)
state_changes = windowed.filter(F.col("state_changed") == True).select(
    "full_name", "is_deleted", "is_favorite", "_sort_by", "_timestamp"
)

# Calculate end timestamps using lead
state_changes_with_end = state_changes.withColumn(
    "end_timestamp",
    F.lead("_timestamp").over(Window.partitionBy("full_name").orderBy("_sort_by"))
).withColumn(
    "end_sort_by",
    F.lead("_sort_by").over(Window.partitionBy("full_name").orderBy("_sort_by"))
).select(
    F.col("full_name"),
    F.col("is_deleted"),
    F.col("is_favorite"),
    F.col("_timestamp").alias("start_timestamp"),
    F.col("end_timestamp"),
    F.col("_sort_by").alias("start_sort_by"),
    F.col("end_sort_by"),
    F.when(F.col("end_timestamp").isNull(), True).otherwise(False).alias("is_current")
)

state_changes_with_end.createOrReplaceTempView("new_scd_records")
display(state_changes_with_end)

# COMMAND ----------

# DBTITLE 1,Create SCD target table and MERGE
# Create target SCD table if it doesn't exist
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        full_name STRING,
        is_deleted BOOLEAN,
        is_favorite BOOLEAN,
        start_timestamp TIMESTAMP_NTZ,
        end_timestamp TIMESTAMP_NTZ,
        start_sort_by LONG,
        end_sort_by LONG,
        is_current BOOLEAN
    )
    USING DELTA
""")

# MERGE logic:
# 1. Close existing current records for repos that have new state changes
# 2. Insert all new SCD records from this batch
spark.sql(f"""
    MERGE INTO {TARGET_TABLE} AS target
    USING (
        -- Get the earliest new event per repo to close the previous current record
        SELECT full_name, MIN(start_timestamp) AS new_start_timestamp, MIN(start_sort_by) AS new_start_sort_by
        FROM new_scd_records
        GROUP BY full_name
    ) AS closing
    ON target.full_name = closing.full_name
       AND target.is_current = TRUE
    WHEN MATCHED THEN UPDATE SET
        target.is_current = FALSE,
        target.end_timestamp = closing.new_start_timestamp,
        target.end_sort_by = closing.new_start_sort_by
""")

# Insert all new SCD records
spark.sql(f"""
    INSERT INTO {TARGET_TABLE}
    SELECT * FROM new_scd_records
""")

print(f"SCD merge complete. Inserted {state_changes_with_end.count()} new records.")

# COMMAND ----------

# DBTITLE 1,Update watermark
# Update the watermark with the max _sort_by we just processed
max_sort_by = new_events_df.agg(F.max("_sort_by")).collect()[0][0]

spark.sql(f"""
    MERGE INTO {WATERMARK_TABLE} AS target
    USING (SELECT '{SOURCE_TABLE}' AS source_table, {max_sort_by} AS last_processed_sort_by, current_timestamp() AS last_processed_at) AS source
    ON target.source_table = source.source_table
    WHEN MATCHED THEN UPDATE SET
        target.last_processed_sort_by = source.last_processed_sort_by,
        target.last_processed_at = source.last_processed_at
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"Watermark updated to _sort_by = {max_sort_by}")

# COMMAND ----------

# DBTITLE 1,Verify SCD output
# Show current state of the SCD table
display(
    spark.sql(f"""
        SELECT * FROM {TARGET_TABLE}
        ORDER BY full_name, start_sort_by
    """)
)
