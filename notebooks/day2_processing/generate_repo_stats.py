# Databricks notebook source
# MAGIC %md
# MAGIC # Day 2: Generate Heavy Repo Stats + AI Processing
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Reads all repos from the student's `github_repos` table (Lakebase)
# MAGIC 2. Fetches file trees and commit history from the GitHub API
# MAGIC 3. Writes results to bronze Delta tables
# MAGIC 4. For repos marked `is_favorite`: AI-processes files + commits using
# MAGIC    Databricks Foundation Model API to generate insights
# MAGIC 5. Writes AI insights to a gold Delta table

# COMMAND ----------

# DBTITLE 1,Configuration
dbutils.widgets.text("catalog", "bootcamp_students", "Unity Catalog name")
dbutils.widgets.text("schema", "<your username>", "Schema name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# Source: CDC history table (mirrored from Lakebase via Change Data Feed)
repos_history_table = f"{catalog}.{schema}.lb_github_repos_history"

# Delta table targets
files_bronze_table = f"{catalog}.{schema}.github_repo_files_bronze"
commits_bronze_table = f"{catalog}.{schema}.github_repo_commits_bronze"
ai_insights_gold_table = f"{catalog}.{schema}.github_repo_ai_insights_gold"
file_contents_bronze_table = f"{catalog}.{schema}.github_file_contents_bronze"

print(f"Catalog: {catalog}")
print(f"Schema: {schema}")
print(f"Source (CDC): {repos_history_table}")
print(f"Files bronze: {files_bronze_table}")
print(f"File contents bronze: {file_contents_bronze_table}")
print(f"Commits bronze: {commits_bronze_table}")
print(f"AI insights gold: {ai_insights_gold_table}")

# COMMAND ----------

# DBTITLE 1,Read current repos from CDC history table
import sys
sys.path.insert(0, "/Workspace/Users/{}/ltap-lab-day-1".format(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
))

from github_client import GitHubClient

# Read the latest state of each repo from the CDC history table.
# The history table contains all change events; we want the most recent
# non-deleted state per repo (latest _sort_by, excluding deletes).
repos_df = spark.sql(f"""
    WITH latest AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY full_name ORDER BY _sort_by DESC) AS rn
        FROM {repos_history_table}
        WHERE _pg_change_type != 'delete'
          AND _pg_change_type != 'update_preimage'
    )
    SELECT full_name, is_favorite, language, stargazers_count
    FROM latest
    WHERE rn = 1
""")

repos = [row.asDict() for row in repos_df.collect()]

print(f"Found {len(repos)} current repos from {repos_history_table}")
for r in repos:
    fav = "⭐" if r["is_favorite"] else "  "
    print(f"  {fav} {r['full_name']} ({r['language']}, {r['stargazers_count']}★)")

# COMMAND ----------

# DBTITLE 1,Executor-side GitHub fetching setup
import base64
import time
from datetime import datetime
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, BooleanType, TimestampType
)

# File extensions we consider "text" and worth fetching content for
TEXT_EXTENSIONS = {
    "py", "js", "ts", "tsx", "jsx", "java", "scala", "go", "rs", "rb", "php",
    "c", "cpp", "h", "hpp", "cs", "swift", "kt", "r", "jl", "lua", "sh",
    "bash", "zsh", "fish", "sql", "graphql", "proto",
    "html", "htm", "css", "scss", "sass", "less",
    "json", "yaml", "yml", "toml", "xml", "ini", "cfg", "conf",
    "md", "rst", "txt", "csv", "tsv",
    "dockerfile", "makefile", "cmake",
    "tf", "hcl", "nix", "dhall",
    "gitignore", "env", "editorconfig",
}

# Max file size to fetch content for (100KB)
MAX_CONTENT_SIZE = 100_000

# Rate limit: pause between API calls to stay under GitHub's limits
# With N executors hitting in parallel, keep per-partition delay conservative
API_DELAY_SECONDS = 0.1

# COMMAND ----------

# DBTITLE 1,Fetch file trees + contents + commits (distributed on executors via mapInPandas)
import pandas as pd
from pyspark.sql import functions as F

# Store credentials in Spark conf so executors can access them
# (Serverless doesn't support sc.broadcast or RDDs)
_workspace_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
_github_token = dbutils.secrets.get(scope="github", key="token")

spark.conf.set("spark.custom.github_token", _github_token)
spark.conf.set("spark.custom.workspace_user", _workspace_user)

# Create a DataFrame of repos and repartition for parallelism
repos_input_df = spark.createDataFrame(
    [(r["full_name"],) for r in repos],
    schema=["full_name"]
).repartition(min(len(repos), 8))

# Output schema for the mapInPandas function - flat schema with a category column
# to distinguish file tree entries, file contents, and commits
output_schema = """
    category STRING,
    repo_full_name STRING,
    path STRING,
    file_type STRING,
    sha STRING,
    size LONG,
    mode STRING,
    tree_truncated BOOLEAN,
    content STRING,
    encoding STRING,
    language STRING,
    author_name STRING,
    author_email STRING,
    author_date STRING,
    committer_name STRING,
    committer_email STRING,
    committer_date STRING,
    message STRING,
    additions LONG,
    deletions LONG,
    ingested_at STRING
"""


def fetch_all_for_repos(iterator):
    """
    mapInPandas function: receives an iterator of pandas DataFrames (each with
    a 'full_name' column), fetches GitHub data on the executor, yields result
    DataFrames.

    Compatible with Databricks Serverless (no RDDs, no broadcast variables).
    """
    import base64
    import time
    import sys
    from datetime import datetime
    from pyspark.sql import SparkSession

    # Get Spark conf values (set on driver, readable on executors)
    spark_session = SparkSession.getActiveSession()
    token = spark_session.conf.get("spark.custom.github_token")
    workspace_user = spark_session.conf.get("spark.custom.workspace_user")

    sys.path.insert(0, f"/Workspace/Users/{workspace_user}/ltap-lab-day-1")
    from github_client import GitHubClient

    client = GitHubClient(token=token)

    for pdf in iterator:
        rows = []

        for full_name in pdf["full_name"].tolist():
            # --- 1. Fetch file tree ---
            try:
                repo_meta = client.get_repo(full_name)
                time.sleep(API_DELAY_SECONDS)
                default_branch = repo_meta.get("default_branch", "main")

                tree_data = client.get_repo_tree(full_name, ref=default_branch)
                time.sleep(API_DELAY_SECONDS)

                tree_entries = tree_data.get("tree", [])
                truncated = tree_data.get("truncated", False)
                ingested_at = datetime.utcnow().isoformat()

                for entry in tree_entries:
                    rows.append({
                        "category": "file",
                        "repo_full_name": full_name,
                        "path": entry.get("path"),
                        "file_type": entry.get("type"),
                        "sha": entry.get("sha"),
                        "size": entry.get("size"),
                        "mode": entry.get("mode"),
                        "tree_truncated": truncated,
                        "content": None,
                        "encoding": None,
                        "language": None,
                        "author_name": None,
                        "author_email": None,
                        "author_date": None,
                        "committer_name": None,
                        "committer_email": None,
                        "committer_date": None,
                        "message": None,
                        "additions": None,
                        "deletions": None,
                        "ingested_at": ingested_at,
                    })

                # --- 2. Fetch file contents for text files ---
                text_files = [
                    e for e in tree_entries
                    if e.get("type") == "blob"
                    and (e.get("size") or 0) <= MAX_CONTENT_SIZE
                    and (e["path"].rsplit(".", 1)[-1].lower() if "." in e["path"]
                         else e["path"].rsplit("/", 1)[-1].lower()) in TEXT_EXTENSIONS
                ]

                for file_entry in text_files:
                    file_path = file_entry["path"]
                    try:
                        content_data = client.get_file_content(full_name, file_path, ref=default_branch)
                        time.sleep(API_DELAY_SECONDS)

                        raw_content = content_data.get("content", "")
                        enc = content_data.get("encoding", "base64")
                        if enc == "base64" and raw_content:
                            decoded = base64.b64decode(raw_content).decode("utf-8", errors="replace")
                        else:
                            decoded = raw_content

                        ext = file_path.rsplit(".", 1)[-1].lower() if "." in file_path else ""

                        rows.append({
                            "category": "content",
                            "repo_full_name": full_name,
                            "path": file_path,
                            "file_type": None,
                            "sha": content_data.get("sha"),
                            "size": content_data.get("size"),
                            "mode": None,
                            "tree_truncated": None,
                            "content": decoded[:500_000],
                            "encoding": enc,
                            "language": ext,
                            "author_name": None,
                            "author_email": None,
                            "author_date": None,
                            "committer_name": None,
                            "committer_email": None,
                            "committer_date": None,
                            "message": None,
                            "additions": None,
                            "deletions": None,
                            "ingested_at": datetime.utcnow().isoformat(),
                        })
                    except Exception:
                        pass

            except Exception:
                pass  # Skip repos where tree fetch fails

            # --- 3. Fetch commits ---
            try:
                commits = client.get_commits(full_name, max_results=200)
                time.sleep(API_DELAY_SECONDS)

                for c in commits:
                    commit_data = c.get("commit", {})
                    author = commit_data.get("author", {}) or {}
                    committer = commit_data.get("committer", {}) or {}
                    stats = c.get("stats", {}) or {}

                    rows.append({
                        "category": "commit",
                        "repo_full_name": full_name,
                        "path": None,
                        "file_type": None,
                        "sha": c.get("sha"),
                        "size": None,
                        "mode": None,
                        "tree_truncated": None,
                        "content": None,
                        "encoding": None,
                        "language": None,
                        "author_name": author.get("name"),
                        "author_email": author.get("email"),
                        "author_date": author.get("date"),
                        "committer_name": committer.get("name"),
                        "committer_email": committer.get("email"),
                        "committer_date": committer.get("date"),
                        "message": commit_data.get("message", "")[:1000],
                        "additions": stats.get("additions"),
                        "deletions": stats.get("deletions"),
                        "ingested_at": datetime.utcnow().isoformat(),
                    })
            except Exception:
                pass

        # Yield a pandas DataFrame for this partition
        if rows:
            yield pd.DataFrame(rows)
        else:
            # Must yield an empty DataFrame with correct columns
            yield pd.DataFrame(columns=[
                "category", "repo_full_name", "path", "file_type", "sha", "size",
                "mode", "tree_truncated", "content", "encoding", "language",
                "author_name", "author_email", "author_date", "committer_name",
                "committer_email", "committer_date", "message", "additions",
                "deletions", "ingested_at",
            ])


# Execute distributed fetching using mapInPandas (Serverless-compatible)
print(f"Distributing GitHub API fetching across executors for {len(repos)} repos...")
results_df = repos_input_df.mapInPandas(fetch_all_for_repos, schema=output_schema)

# Cache the results so we can filter by category without re-executing
results_df.cache()
results_df.count()  # Force materialization

# Split into separate collections by category
all_files = [
    row.asDict() for row in
    results_df.filter("category = 'file'")
    .select("repo_full_name", "path", F.col("file_type").alias("type"), "sha", "size", "mode", "tree_truncated", "ingested_at")
    .collect()
]
all_file_contents = [
    row.asDict() for row in
    results_df.filter("category = 'content'")
    .select("repo_full_name", "path", "content", "encoding", "sha", "size", "language", "ingested_at")
    .collect()
]
all_commits = [
    row.asDict() for row in
    results_df.filter("category = 'commit'")
    .select("repo_full_name", "sha", "author_name", "author_email", "author_date",
            "committer_name", "committer_email", "committer_date", "message",
            "additions", "deletions", "ingested_at")
    .collect()
]

results_df.unpersist()

print(f"✓ File tree entries: {len(all_files)}")
print(f"✓ File contents fetched: {len(all_file_contents)}")
print(f"✓ Commits fetched: {len(all_commits)}")

# COMMAND ----------

# DBTITLE 1,Write files to bronze Delta table
files_schema = StructType([
    StructField("repo_full_name", StringType(), False),
    StructField("path", StringType(), True),
    StructField("type", StringType(), True),
    StructField("sha", StringType(), True),
    StructField("size", LongType(), True),
    StructField("mode", StringType(), True),
    StructField("tree_truncated", BooleanType(), True),
    StructField("ingested_at", StringType(), True),
])

if all_files:
    files_df = spark.createDataFrame(all_files, schema=files_schema)
    files_df = files_df.withColumn("ingested_at", F.to_timestamp("ingested_at"))

    (
        files_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(files_bronze_table)
    )
    print(f"✓ Wrote {files_df.count()} file entries to {files_bronze_table}")
    display(files_df.groupBy("repo_full_name").count().orderBy("count", ascending=False))
else:
    print("No file data to write.")

# COMMAND ----------

# DBTITLE 1,Write file contents to bronze Delta table
file_contents_schema = StructType([
    StructField("repo_full_name", StringType(), False),
    StructField("path", StringType(), False),
    StructField("content", StringType(), True),
    StructField("encoding", StringType(), True),
    StructField("sha", StringType(), True),
    StructField("size", LongType(), True),
    StructField("language", StringType(), True),
    StructField("ingested_at", StringType(), True),
])

if all_file_contents:
    contents_df = spark.createDataFrame(all_file_contents, schema=file_contents_schema)
    contents_df = contents_df.withColumn("ingested_at", F.to_timestamp("ingested_at"))

    (
        contents_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(file_contents_bronze_table)
    )
    print(f"✓ Wrote {contents_df.count()} file contents to {file_contents_bronze_table}")
    display(contents_df.groupBy("repo_full_name").count().orderBy("count", ascending=False))
else:
    print("No file content data to write.")

# COMMAND ----------

# DBTITLE 1,Write commits to bronze Delta table
commits_schema = StructType([
    StructField("repo_full_name", StringType(), False),
    StructField("sha", StringType(), False),
    StructField("author_name", StringType(), True),
    StructField("author_email", StringType(), True),
    StructField("author_date", StringType(), True),
    StructField("committer_name", StringType(), True),
    StructField("committer_email", StringType(), True),
    StructField("committer_date", StringType(), True),
    StructField("message", StringType(), True),
    StructField("additions", LongType(), True),
    StructField("deletions", LongType(), True),
    StructField("ingested_at", StringType(), True),
])

if all_commits:
    commits_df = spark.createDataFrame(all_commits, schema=commits_schema)
    commits_df = (
        commits_df
        .withColumn("author_date", F.to_timestamp("author_date"))
        .withColumn("committer_date", F.to_timestamp("committer_date"))
        .withColumn("ingested_at", F.to_timestamp("ingested_at"))
    )

    (
        commits_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(commits_bronze_table)
    )
    print(f"✓ Wrote {commits_df.count()} commits to {commits_bronze_table}")
    display(commits_df.groupBy("repo_full_name").count().orderBy("count", ascending=False))
else:
    print("No commit data to write.")

# COMMAND ----------

# DBTITLE 1,Compute heavy stats per repo
from pyspark.sql.window import Window

# --- File stats ---
if all_files:
    files_stats = (
        spark.table(files_bronze_table)
        .filter(F.col("type") == "blob")  # Only count actual files, not directories
        .groupBy("repo_full_name")
        .agg(
            F.count("*").alias("total_files"),
            F.sum("size").alias("total_size_bytes"),
            F.avg("size").alias("avg_file_size_bytes"),
            F.max("size").alias("max_file_size_bytes"),
            # Count by file extension
            F.countDistinct(
                F.regexp_extract("path", r"\.([^.]+)$", 1)
            ).alias("unique_extensions"),
        )
    )

# --- Commit stats ---
if all_commits:
    commits_stats = (
        spark.table(commits_bronze_table)
        .groupBy("repo_full_name")
        .agg(
            F.count("*").alias("total_commits"),
            F.countDistinct("author_name").alias("unique_authors"),
            F.countDistinct("author_email").alias("unique_author_emails"),
            F.min("author_date").alias("first_commit_date"),
            F.max("author_date").alias("last_commit_date"),
            F.sum("additions").alias("total_additions"),
            F.sum("deletions").alias("total_deletions"),
            F.avg("additions").alias("avg_additions_per_commit"),
            F.avg("deletions").alias("avg_deletions_per_commit"),
            # Commit message length stats
            F.avg(F.length("message")).alias("avg_message_length"),
        )
    )

    # Days active = last commit - first commit
    commits_stats = commits_stats.withColumn(
        "days_active",
        F.datediff("last_commit_date", "first_commit_date")
    ).withColumn(
        "commits_per_day",
        F.when(F.col("days_active") > 0,
               F.round(F.col("total_commits") / F.col("days_active"), 2))
        .otherwise(F.col("total_commits"))
    )

# --- Join file + commit stats ---
if all_files and all_commits:
    repo_stats = files_stats.join(commits_stats, on="repo_full_name", how="outer")
elif all_files:
    repo_stats = files_stats
elif all_commits:
    repo_stats = commits_stats
else:
    dbutils.notebook.exit("No data collected - nothing to process.")

repo_stats = repo_stats.withColumn("processed_at", F.current_timestamp())
display(repo_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ## AI Processing for Favorite Repos
# MAGIC
# MAGIC For repos marked `is_favorite`, we send the file tree structure and
# MAGIC commit history to a Foundation Model to generate:
# MAGIC - A purpose/description summary
# MAGIC - Key technology stack identification
# MAGIC - Commit pattern analysis
# MAGIC - A "health score" (0-100)

# COMMAND ----------

# DBTITLE 1,AI Processing - Favorites Only
import json

favorite_repos = [r for r in repos if r["is_favorite"]]
print(f"Found {len(favorite_repos)} favorite repos for AI processing")

if not favorite_repos:
    print("No favorite repos - skipping AI processing.")
    dbutils.notebook.exit("No favorites to AI-process. Stats written to bronze tables.")


def build_ai_prompt(full_name: str, files: list[dict], commits: list[dict], file_contents: list[dict]) -> str:
    """Build a structured prompt for the AI model from repo files, commits, and contents."""

    # Summarize file tree (top-level structure + extensions)
    file_paths = [f["path"] for f in files if f.get("type") == "blob"]
    top_level = sorted(set(p.split("/")[0] for p in file_paths))[:30]

    # Count extensions
    extensions = {}
    for p in file_paths:
        ext = p.rsplit(".", 1)[-1] if "." in p else "(none)"
        extensions[ext] = extensions.get(ext, 0) + 1
    top_extensions = sorted(extensions.items(), key=lambda x: -x[1])[:15]

    # Recent commit messages (last 30)
    recent_messages = [c.get("message", "")[:200] for c in commits[:30]]

    # Unique authors
    authors = list(set(c.get("author_name", "Unknown") for c in commits if c.get("author_name")))[:20]

    # Key file contents - prioritize config/entry files, limit total size
    key_files = []
    priority_patterns = [
        "README", "setup.py", "pyproject.toml", "package.json", "Cargo.toml",
        "go.mod", "build.gradle", "pom.xml", "Makefile", "Dockerfile",
        "requirements.txt", "setup.cfg", "app.py", "main.py", "index.ts",
        "index.js", "lib.rs", "mod.rs",
    ]

    # Sort contents: priority files first, then by path
    def file_priority(fc):
        name = fc["path"].rsplit("/", 1)[-1]
        for i, pat in enumerate(priority_patterns):
            if pat.lower() in name.lower():
                return i
        return len(priority_patterns)

    sorted_contents = sorted(file_contents, key=file_priority)

    # Include up to ~30KB of file content in the prompt
    content_budget = 30_000
    content_used = 0
    for fc in sorted_contents:
        content = fc.get("content", "")
        if not content:
            continue
        # Truncate individual files to 5KB
        truncated = content[:5000]
        if content_used + len(truncated) > content_budget:
            break
        key_files.append(f"### {fc['path']}\n```\n{truncated}\n```")
        content_used += len(truncated)

    file_contents_section = ""
    if key_files:
        file_contents_section = f"""

## Key File Contents ({len(key_files)} files)
{chr(10).join(key_files)}
"""

    prompt = f"""Analyze this GitHub repository: {full_name}

## File Structure
Total files: {len(file_paths)}
Top-level entries: {', '.join(top_level)}

File extensions (count):
{chr(10).join(f'  .{ext}: {count}' for ext, count in top_extensions)}
{file_contents_section}
## Commit History
Total commits (up to 200): {len(commits)}
Unique authors: {', '.join(authors[:10])}

Recent commit messages:
{chr(10).join(f'  - {msg.split(chr(10))[0]}' for msg in recent_messages)}

## Task
Based on this information (including the actual file contents), provide a JSON response with:
1. "purpose": A 2-3 sentence summary of what this repository does
2. "tech_stack": List of key technologies/frameworks/languages used
3. "commit_patterns": Brief analysis of commit activity and collaboration patterns
4. "strengths": 2-3 notable strengths of the project
5. "health_score": Integer 0-100 rating the project health (activity, maintenance, structure)
6. "health_rationale": One sentence explaining the health score
7. "code_quality_notes": 2-3 observations about code quality from the actual source

Respond ONLY with valid JSON, no markdown formatting."""

    return prompt


def call_foundation_model(prompt: str) -> dict:
    """Call a Foundation Model using Databricks ai_query()."""
    result_df = spark.sql("""
        SELECT ai_query(
            'databricks-meta-llama-3-1-70b-instruct',
            CONCAT(
                'You are a senior software engineer analyzing GitHub repositories. '
                'Always respond with valid JSON only, no markdown formatting.\n\n',
                :prompt
            )
        ) AS response
    """, args={"prompt": prompt})

    content = result_df.collect()[0]["response"]

    # Parse the JSON response (strip markdown code fences if present)
    content = content.strip()
    if content.startswith("```"):
        content = content.split("\n", 1)[1]  # Remove first line
        content = content.rsplit("```", 1)[0]  # Remove last fence

    return json.loads(content)


# Process each favorite repo
ai_insights = []

for repo in favorite_repos:
    full_name = repo["full_name"]
    print(f"\n🤖 AI-processing: {full_name}")

    # Get files, commits, and contents for this repo
    repo_files = [f for f in all_files if f["repo_full_name"] == full_name]
    repo_commits = [c for c in all_commits if c["repo_full_name"] == full_name]
    repo_contents = [f for f in all_file_contents if f["repo_full_name"] == full_name]

    print(f"   Files: {len(repo_files)}, Commits: {len(repo_commits)}, Contents: {len(repo_contents)}")

    try:
        prompt = build_ai_prompt(full_name, repo_files, repo_commits, repo_contents)
        insights = call_foundation_model(prompt)

        ai_insights.append({
            "repo_full_name": full_name,
            "purpose": insights.get("purpose", ""),
            "tech_stack": json.dumps(insights.get("tech_stack", [])),
            "commit_patterns": insights.get("commit_patterns", ""),
            "strengths": json.dumps(insights.get("strengths", [])),
            "health_score": int(insights.get("health_score", 0)),
            "health_rationale": insights.get("health_rationale", ""),
            "code_quality_notes": json.dumps(insights.get("code_quality_notes", [])),
            "processed_at": datetime.utcnow().isoformat(),
        })
        print(f"   ✓ Health score: {insights.get('health_score')}/100")
        print(f"   ✓ Purpose: {insights.get('purpose', '')[:100]}...")

    except Exception as e:
        print(f"   ✗ AI processing failed: {e}")
        ai_insights.append({
            "repo_full_name": full_name,
            "purpose": f"AI processing failed: {str(e)[:200]}",
            "tech_stack": "[]",
            "commit_patterns": "",
            "strengths": "[]",
            "health_score": -1,
            "health_rationale": "Processing error",
            "code_quality_notes": "[]",
            "processed_at": datetime.utcnow().isoformat(),
        })

print(f"\n\nAI processing complete: {len(ai_insights)} repos processed")

# COMMAND ----------

# DBTITLE 1,Write AI insights to gold Delta table
from pyspark.sql.types import IntegerType

ai_insights_schema = StructType([
    StructField("repo_full_name", StringType(), False),
    StructField("purpose", StringType(), True),
    StructField("tech_stack", StringType(), True),
    StructField("commit_patterns", StringType(), True),
    StructField("strengths", StringType(), True),
    StructField("health_score", IntegerType(), True),
    StructField("health_rationale", StringType(), True),
    StructField("code_quality_notes", StringType(), True),
    StructField("processed_at", StringType(), True),
])

if ai_insights:
    insights_df = spark.createDataFrame(ai_insights, schema=ai_insights_schema)
    insights_df = insights_df.withColumn("processed_at", F.to_timestamp("processed_at"))

    (
        insights_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(ai_insights_gold_table)
    )
    print(f"✓ Wrote {insights_df.count()} AI insight rows to {ai_insights_gold_table}")
    display(insights_df)
else:
    print("No AI insights to write.")

# COMMAND ----------

# DBTITLE 1,Final Summary
print("=" * 60)
print("DAY 2 PROCESSING COMPLETE")
print("=" * 60)
print(f"\n📁 Files bronze:      {files_bronze_table}")
print(f"   → {len(all_files)} file entries across {len(repos)} repos")
print(f"\n📄 File contents:     {file_contents_bronze_table}")
print(f"   → {len(all_file_contents)} files with content fetched")
print(f"\n📝 Commits bronze:    {commits_bronze_table}")
print(f"   → {len(all_commits)} commits across {len(repos)} repos")
print(f"\n🤖 AI insights gold:  {ai_insights_gold_table}")
print(f"   → {len(ai_insights)} favorite repos AI-analyzed")
print(f"\n⭐ Favorites processed:")
for insight in ai_insights:
    score = insight["health_score"]
    emoji = "🟢" if score >= 70 else "🟡" if score >= 40 else "🔴" if score >= 0 else "❌"
    print(f"   {emoji} {insight['repo_full_name']}: {score}/100")
print("=" * 60)
