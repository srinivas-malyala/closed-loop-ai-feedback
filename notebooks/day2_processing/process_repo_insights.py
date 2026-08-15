# Databricks notebook source
# MAGIC %md
# MAGIC # Process Repo Insights -> Gold Delta ("heavy" Spark stage)
# MAGIC
# MAGIC Reads the raw `github_repos_bronze` table and runs real aggregation/
# MAGIC ranking work with Spark: per-language rollups, a "top repo per language"
# MAGIC window function, and a normalized popularity score. This is the "heavy
# MAGIC processing in the lake" step of the demo - the kind of work you would
# MAGIC NOT want to run inside the operational Postgres database.
# MAGIC
# MAGIC Output: a Gold Delta table (`github_repo_insights_gold`) that gets synced
# MAGIC back into Lakebase Postgres via a **Synced Table** (see README.md), so the
# MAGIC Flask app can serve it as a fast, read-only `/insights` endpoint.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Unity Catalog name")
dbutils.widgets.text("schema", "ltap_lab_day1", "Schema name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

bronze_table = f"{catalog}.{schema}.github_repos_bronze"
gold_table = f"{catalog}.{schema}.github_repo_insights_gold"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

bronze_df = spark.table(bronze_table).withColumn(
    "language", F.coalesce(F.col("language"), F.lit("Unknown"))
)

# --- Per-language rollups: repo count, total/avg stars, total open issues ---
language_stats = bronze_df.groupBy("language").agg(
    F.count("*").alias("repo_count"),
    F.sum("stargazers_count").alias("total_stars"),
    F.avg("stargazers_count").alias("avg_stars"),
    F.sum("open_issues_count").alias("total_open_issues"),
    F.sum("forks_count").alias("total_forks"),
)

# --- Rank repos within each language by stars, keep the rank + a normalized
#     popularity score (0-1) relative to the top repo in that language. ---
window_spec = Window.partitionBy("language").orderBy(F.col("stargazers_count").desc())
max_stars_per_lang = Window.partitionBy("language")

ranked_repos = (
    bronze_df.withColumn("rank_in_language", F.row_number().over(window_spec))
    .withColumn("max_stars_in_language", F.max("stargazers_count").over(max_stars_per_lang))
    .withColumn(
        "popularity_score",
        F.round(F.col("stargazers_count") / F.greatest(F.col("max_stars_in_language"), F.lit(1)), 4),
    )
)

# --- Join repo-level ranking back with language-level rollups so each row
#     in the Gold table carries both its own stats and its language context. ---
gold_df = (
    ranked_repos.join(language_stats, on="language", how="left")
    .select(
        "full_name",
        "language",
        "stargazers_count",
        "open_issues_count",
        "forks_count",
        "rank_in_language",
        "popularity_score",
        "repo_count",
        "total_stars",
        F.round("avg_stars", 1).alias("avg_stars_in_language"),
        "total_open_issues",
        "total_forks",
    )
    .withColumn("processed_at", F.current_timestamp())
)

# COMMAND ----------

(
    gold_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_table)
)

print(f"Wrote {gold_df.count()} rows to {gold_table}")
display(spark.table(gold_table).orderBy("language", "rank_in_language").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next step
# MAGIC Create a **Synced Table** in the Lakebase UI pointing at `github_repo_insights_gold`
# MAGIC so this Gold table appears as a read-only table inside your Lakebase Postgres
# MAGIC instance. See the "Sync processed data back to Lakebase" section in README.md
# MAGIC for the exact click-path.
