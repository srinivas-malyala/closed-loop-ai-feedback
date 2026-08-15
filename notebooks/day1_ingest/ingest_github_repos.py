# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest GitHub Repositories -> Bronze Delta
# MAGIC
# MAGIC Pulls repository metadata from the GitHub Search API (paginated, no auth
# MAGIC required though a PAT raises the rate limit) and lands it as a raw Bronze
# MAGIC Delta table in Unity Catalog. This is the "raw ingestion" stage of the
# MAGIC LTAP pipeline - no transformation happens here, just capture-as-is.
# MAGIC
# MAGIC Downstream: `process_repo_insights.py` reads this table, does the "heavy"
# MAGIC Spark aggregation, and writes a Gold Delta table that gets synced back to
# MAGIC Lakebase via a Synced Table.

# COMMAND ----------

# MAGIC %pip install requests databricks-sdk --quiet

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Unity Catalog name")
dbutils.widgets.text("schema", "ltap_lab_day1", "Schema name")
dbutils.widgets.text("query", "language:python stars:>1000", "GitHub search query")
dbutils.widgets.text("max_results", "500", "Max repos to ingest")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
search_query = dbutils.widgets.get("query")
max_results = int(dbutils.widgets.get("max_results"))

bronze_table = f"{catalog}.{schema}.github_repos_bronze"

# COMMAND ----------

import base64
import time
from typing import Iterator

import requests
from databricks.sdk import WorkspaceClient

_w = WorkspaceClient()


def _get_token() -> str | None:
    """Optional GitHub PAT stored via setup_secrets.py. Falls back to
    unauthenticated requests (60 req/hr) if not configured."""
    try:
        secret = _w.secrets.get_secret(scope="github", key="token")
        return base64.b64decode(secret.value).decode("utf-8")
    except Exception:
        return None


def search_repositories(query: str, max_results: int = 500, per_page: int = 100) -> Iterator[dict]:
    """Yield repo items from GitHub's Search API, paginating until max_results
    is reached (GitHub caps search results at 1000 total per query)."""
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    token = _get_token()
    if token:
        headers["Authorization"] = f"Bearer {token}"

    session = requests.Session()
    session.headers.update(headers)

    page = 1
    yielded = 0
    while yielded < max_results:
        resp = session.get(
            "https://api.github.com/search/repositories",
            params={"q": query, "sort": "stars", "order": "desc", "per_page": per_page, "page": page},
            timeout=30,
        )
        resp.raise_for_status()
        items = resp.json().get("items", [])
        if not items:
            break
        for item in items:
            if yielded >= max_results:
                break
            yield item
            yielded += 1
        page += 1
        # Be polite to the unauthenticated rate limit.
        time.sleep(0.5)

# COMMAND ----------

repos = list(search_repositories(search_query, max_results=max_results))
print(f"Fetched {len(repos)} repos from GitHub for query: {search_query!r}")

# COMMAND ----------

import json

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Land the raw JSON payload per repo, alongside a few top-level columns
# that are cheap to project now and useful for later filtering/joins.
rows = [
    {
        "id": repo["id"],
        "full_name": repo["full_name"],
        "language": repo.get("language"),
        "stargazers_count": repo.get("stargazers_count", 0),
        "open_issues_count": repo.get("open_issues_count", 0),
        "forks_count": repo.get("forks_count", 0),
        "payload": json.dumps(repo),
    }
    for repo in repos
]

bronze_df = spark.createDataFrame(rows).withColumn("ingested_at", F.current_timestamp())

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

(
    bronze_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(bronze_table)
)

print(f"Wrote {bronze_df.count()} rows to {bronze_table}")
display(spark.table(bronze_table).limit(10))
