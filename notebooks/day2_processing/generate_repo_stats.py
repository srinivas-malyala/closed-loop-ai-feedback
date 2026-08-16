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
dbutils.widgets.text("schema", "username", "Schema name")

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

# DBTITLE 1,Fetch file trees for all repos
from datetime import datetime

client = GitHubClient()
all_files = []

for repo in repos:
    full_name = repo["full_name"]
    print(f"Fetching file tree for {full_name}...")
    try:
        # Use default branch - get repo metadata first for the default branch
        repo_meta = client.get_repo(full_name)
        default_branch = repo_meta.get("default_branch", "main")
        tree_data = client.get_repo_tree(full_name, ref=default_branch)

        tree_entries = tree_data.get("tree", [])
        truncated = tree_data.get("truncated", False)

        for entry in tree_entries:
            all_files.append({
                "repo_full_name": full_name,
                "path": entry.get("path"),
                "type": entry.get("type"),  # "blob" or "tree"
                "sha": entry.get("sha"),
                "size": entry.get("size"),
                "mode": entry.get("mode"),
                "tree_truncated": truncated,
                "ingested_at": datetime.utcnow().isoformat(),
            })

        print(f"  → {len(tree_entries)} entries (truncated={truncated})")
    except Exception as e:
        print(f"  ✗ Error fetching tree for {full_name}: {e}")

print(f"\nTotal file entries collected: {len(all_files)}")

# COMMAND ----------

# DBTITLE 1,Fetch file contents for all repos
import base64

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

# Max file size to fetch (100KB) - skip large generated/vendored files
MAX_CONTENT_SIZE = 100_000

all_file_contents = []

for repo in repos:
    full_name = repo["full_name"]
    repo_meta = client.get_repo(full_name)
    default_branch = repo_meta.get("default_branch", "main")

    # Filter to text files under size limit
    repo_files = [
        f for f in all_files
        if f["repo_full_name"] == full_name
        and f["type"] == "blob"
        and (f.get("size") or 0) <= MAX_CONTENT_SIZE
        and (f["path"].rsplit(".", 1)[-1].lower() if "." in f["path"]
             else f["path"].rsplit("/", 1)[-1].lower()) in TEXT_EXTENSIONS
    ]

    print(f"Fetching contents for {full_name}: {len(repo_files)} text files...")
    fetched = 0
    errors = 0

    for file_entry in repo_files:
        file_path = file_entry["path"]
        try:
            content_data = client.get_file_content(full_name, file_path, ref=default_branch)

            # Decode base64 content
            raw_content = content_data.get("content", "")
            encoding = content_data.get("encoding", "base64")
            if encoding == "base64" and raw_content:
                decoded = base64.b64decode(raw_content).decode("utf-8", errors="replace")
            else:
                decoded = raw_content

            # Detect language from extension
            ext = file_path.rsplit(".", 1)[-1].lower() if "." in file_path else ""

            all_file_contents.append({
                "repo_full_name": full_name,
                "path": file_path,
                "content": decoded[:500_000],  # Safety cap at 500KB decoded
                "encoding": encoding,
                "sha": content_data.get("sha"),
                "size": content_data.get("size"),
                "language": ext,
                "ingested_at": datetime.utcnow().isoformat(),
            })
            fetched += 1

        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"    ✗ {file_path}: {e}")

    print(f"  → Fetched {fetched} files ({errors} errors)")

print(f"\nTotal file contents collected: {len(all_file_contents)}")

# COMMAND ----------

# DBTITLE 1,Fetch commits for all repos
all_commits = []

for repo in repos:
    full_name = repo["full_name"]
    print(f"Fetching commits for {full_name}...")
    try:
        commits = client.get_commits(full_name, max_results=200)

        for c in commits:
            commit_data = c.get("commit", {})
            author = commit_data.get("author", {}) or {}
            committer = commit_data.get("committer", {}) or {}
            stats = c.get("stats", {}) or {}

            all_commits.append({
                "repo_full_name": full_name,
                "sha": c.get("sha"),
                "author_name": author.get("name"),
                "author_email": author.get("email"),
                "author_date": author.get("date"),
                "committer_name": committer.get("name"),
                "committer_email": committer.get("email"),
                "committer_date": committer.get("date"),
                "message": commit_data.get("message", "")[:1000],  # Truncate long messages
                "additions": stats.get("additions"),
                "deletions": stats.get("deletions"),
                "ingested_at": datetime.utcnow().isoformat(),
            })

        print(f"  → {len(commits)} commits")
    except Exception as e:
        print(f"  ✗ Error fetching commits for {full_name}: {e}")

print(f"\nTotal commits collected: {len(all_commits)}")

# COMMAND ----------

# DBTITLE 1,Write files to bronze Delta table
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, BooleanType, TimestampType
)

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
