# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest GitHub Repo Files -> Bronze Delta
# MAGIC
# MAGIC Reads the repos already landed by `ingest_github_repos.py` and, for each
# MAGIC one, walks its **entire file tree** via GitHub's Git Trees API
# MAGIC (`GET /repos/{full_name}/git/trees/{sha}?recursive=1`) - one API call per
# MAGIC repo returns every blob (file) and tree (directory) entry, no cloning
# MAGIC required. Every file entry is captured as-is (path, size, sha, mode) into
# MAGIC a Bronze Delta table, keyed by repo `full_name`.
# MAGIC
# MAGIC Repos are fetched with **limited concurrency** (a small `ThreadPoolExecutor`)
# MAGIC to stay within GitHub's rate limit (60 req/hr unauthenticated, 5,000 req/hr
# MAGIC with a PAT - see `setup_secrets.py`) while still being much faster than a
# MAGIC fully sequential walk over hundreds of repos.
# MAGIC
# MAGIC Downstream: none yet - this is raw capture-as-is, same spirit as
# MAGIC `github_repos_bronze`. A later Gold stage could aggregate file counts,
# MAGIC languages by extension, repo size, etc.

# COMMAND ----------

# MAGIC %pip install requests databricks-sdk --quiet

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Unity Catalog name")
dbutils.widgets.text("schema", "ltap_lab_day1", "Schema name")
dbutils.widgets.text("max_workers", "5", "Concurrent repo file-tree fetches")
dbutils.widgets.text("max_repos", "0", "Limit repos processed (0 = all in bronze table)")
dbutils.widgets.dropdown("mode", "overwrite", ["overwrite", "append"], "Write mode")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
max_workers = int(dbutils.widgets.get("max_workers"))
max_repos = int(dbutils.widgets.get("max_repos"))
write_mode = dbutils.widgets.get("mode")

repos_bronze_table = f"{catalog}.{schema}.github_repos_bronze"
files_bronze_table = f"{catalog}.{schema}.github_repo_files_bronze"

# COMMAND ----------

import json
from concurrent.futures import ThreadPoolExecutor, as_completed

from github_client import GitHubClient

# Pull (full_name, default_branch) pairs from the already-ingested repo
# metadata - default_branch lives inside the raw `payload` JSON, not as its
# own bronze column.
repos_df = spark.table(repos_bronze_table).select("full_name", "payload")
if max_repos > 0:
    repos_df = repos_df.limit(max_repos)

repo_rows = [
    {"full_name": r["full_name"], "default_branch": json.loads(r["payload"]).get("default_branch", "main")}
    for r in repos_df.collect()
]
print(f"Loaded {len(repo_rows)} repos from {repos_bronze_table} to walk file trees for.")

# COMMAND ----------


def fetch_repo_files(repo: dict) -> list[dict]:
    """Fetch one repo's full recursive tree and flatten it to file rows.

    Each GitHubClient instance opens its own requests.Session, so it is safe
    to construct one per worker thread rather than sharing a single client
    across threads.
    """
    client = GitHubClient()
    full_name = repo["full_name"]
    try:
        tree = client.get_repo_tree(full_name, ref=repo["default_branch"], recursive=True)
    except Exception as exc:
        print(f"  [skip] {full_name}: {exc}")
        return []

    truncated = bool(tree.get("truncated", False))
    if truncated:
        print(f"  [warn] {full_name}: tree truncated by GitHub API (very large repo)")

    return [
        {
            "repo_full_name": full_name,
            "path": entry.get("path"),
            "type": entry.get("type"),  # "blob" (file), "tree" (dir), or "commit" (submodule)
            "sha": entry.get("sha"),
            "size": entry.get("size"),  # only present for "blob" entries
            "mode": entry.get("mode"),
            "tree_truncated": truncated,
        }
        for entry in tree.get("tree", [])
        if entry.get("type") == "blob"  # capture files only, not directory nodes
    ]


# Limited concurrency: enough parallelism to make hundreds of repos feasible
# in a lab session, small enough to avoid tripping GitHub's rate limit.
file_rows: list[dict] = []
with ThreadPoolExecutor(max_workers=max_workers) as pool:
    futures = {pool.submit(fetch_repo_files, repo): repo["full_name"] for repo in repo_rows}
    for i, future in enumerate(as_completed(futures), start=1):
        full_name = futures[future]
        try:
            rows = future.result()
            file_rows.extend(rows)
            print(f"[{i}/{len(repo_rows)}] {full_name}: {len(rows)} files")
        except Exception as exc:
            print(f"[{i}/{len(repo_rows)}] {full_name}: failed ({exc})")

print(f"Captured {len(file_rows)} files across {len(repo_rows)} repos.")

# COMMAND ----------

from pyspark.sql import functions as F

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

if file_rows:
    files_df = spark.createDataFrame(file_rows).withColumn("ingested_at", F.current_timestamp())

    (
        files_df.write.format("delta")
        .mode(write_mode)
        .option("mergeSchema", "true")
        .saveAsTable(files_bronze_table)
    )
    print(f"Wrote {files_df.count()} rows to {files_bronze_table} (mode={write_mode})")
    display(spark.table(files_bronze_table).limit(10))
else:
    print("No file rows captured - nothing written.")
