# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///


# COMMAND ----------

# MAGIC %md
# MAGIC # GitHub Graph Traversal
# MAGIC
# MAGIC BFS-style traversal of the GitHub social graph. Starting from seed repos, discovers new
# MAGIC repositories via AI dependency analysis: Parse dependency files → AI suggests upstream repos
# MAGIC
# MAGIC Supports two fetch modes:
# MAGIC - `graphql`: ~2 API calls/repo (1 GraphQL + 1 REST tree). Uses separate rate limit.
# MAGIC - `rest`: ~5-7 REST calls/repo.

# COMMAND ----------

import json
import time
from datetime import datetime

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, LongType, BooleanType, StructField, StructType

from github_client import GitHubClient

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "bootcamp_students", "Unity Catalog name")
dbutils.widgets.text("schema", "sri", "Schema name")
dbutils.widgets.text("target_repos", "500", "Target repos")
dbutils.widgets.text("max_hops", "3", "Max hops")
dbutils.widgets.text("fetch_mode", "graphql", "Fetch mode: graphql or rest")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
TARGET_REPOS = int(dbutils.widgets.get("target_repos"))
MAX_HOPS = int(dbutils.widgets.get("max_hops"))
FETCH_MODE = dbutils.widgets.get("fetch_mode")

repos_history_table = f"{catalog}.{schema}.lb_github_repos_history"
github_staging_table = f"{catalog}.{schema}.github_api_staging"
graph_edges_table = f"{catalog}.{schema}.github_graph_edges"
ai_query_cache_table = f"{catalog}.{schema}.ai_query_cache"
feedback_history_table = f"{catalog}.{schema}.lb_ai_suggestion_feedback_history"

# Budget: GitHub REST API = 5000 req/hr. Track usage to avoid blowing the limit.
MAX_GITHUB_REQUESTS_PER_RUN = 4000
_github_request_count = 0

DEPENDENCY_FILENAMES = {
    "package.json", "requirements.txt", "setup.py", "pyproject.toml",
    "Cargo.toml", "go.mod", "go.sum", "build.gradle", "pom.xml",
    "Gemfile", "composer.json", "setup.cfg", "Pipfile", "Pipfile.lock",
    "package-lock.json", "yarn.lock", "poetry.lock",
}

API_DELAY_SECONDS = 0.001

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared Helpers

# COMMAND ----------

_ROW_COLUMNS = [
    "category", "repo_full_name", "path", "file_type", "sha", "size",
    "mode", "tree_truncated", "content", "encoding", "language",
    "author_name", "author_email", "author_date", "committer_name",
    "committer_email", "committer_date", "message", "additions",
    "deletions", "ingested_at",
]


def _make_row(category, repo_full_name, ingested_at, **kwargs):
    """Build a staging row with sensible defaults (None for unset fields)."""
    row = {col: None for col in _ROW_COLUMNS}
    row["category"] = category
    row["repo_full_name"] = repo_full_name
    row["ingested_at"] = ingested_at
    row.update(kwargs)
    return row


def _empty_staging_df():
    """Return an empty DataFrame matching the staging schema."""
    return pd.DataFrame(columns=_ROW_COLUMNS)

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Loading

# COMMAND ----------

repos_df = spark.sql(f"""
    WITH latest AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY full_name ORDER BY _sort_by DESC) AS rn
        FROM {repos_history_table}
    )
    SELECT full_name, is_favorite, language, stargazers_count
    FROM latest
    WHERE rn = 1 AND _pg_change_type NOT IN('delete', 'update_preimage')
""")
repos = [row.asDict() for row in repos_df.collect()]
print(f"Found (repos) repos")
_github_token = dbutils.secrets.get(scope="github", key="token")

# COMMAND ----------

repos_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## REST Fetcher (mapInPandas)

# COMMAND ----------

def fetch_all_for_repos(iterator):
    """REST-based fetcher: ~5-7 API calls per repo (tree + file contents + commits)."""
    import base64
    import time
    from datetime import datetime
    from github_client import GitHubClient

    client = GitHubClient(token=_github_token)

    for pdf in iterator:
        rows = []
        for full_name in pdf["full_name"].tolist():
            ingested_at = datetime.utcnow().isoformat()

            # --- Tree + dependency file contents ---
            try:
                repo_meta = client.get_repo(full_name)
                time.sleep(API_DELAY_SECONDS)
                default_branch = repo_meta.get("default_branch", "main")

                tree_data = client.get_repo_tree(full_name, ref=default_branch)
                time.sleep(API_DELAY_SECONDS)

                tree_entries = tree_data.get("tree", [])
                truncated = tree_data.get("truncated", False)

                dep_files = [
                    e for e in tree_entries
                    if e.get("type") == "blob"
                    and e["path"].rsplit("/", 1)[-1] in DEPENDENCY_FILENAMES
                ]

                for entry in dep_files:
                    # "file" row (tree metadata)
                    rows.append(_make_row("file", full_name, ingested_at,
                        path=entry.get("path"),
                        file_type=entry.get("type"),
                        sha=entry.get("sha"),
                        size=entry.get("size"),
                        mode=entry.get("mode"),
                        tree_truncated=truncated,
                    ))

                    # "content" row (file body)
                    try:
                        content_data = client.get(
                            f"/repos/{full_name}/contents/{entry['path']}",
                            params={"ref": default_branch},
                        )
                        time.sleep(API_DELAY_SECONDS)
                        raw_content = content_data.get("content", "")
                        enc = content_data.get("encoding", "base64")
                        decoded = (
                            base64.b64decode(raw_content).decode("utf-8", errors="replace")
                            if enc == "base64" and raw_content
                            else raw_content
                        )
                        ext = entry["path"].rsplit(".", 1)[-1].lower() if "." in entry["path"] else ""
                        rows.append(_make_row("content", full_name, datetime.utcnow().isoformat(),
                            path=entry["path"],
                            sha=content_data.get("sha"),
                            size=content_data.get("size"),
                            content=decoded[:500_000],
                            encoding=enc,
                            language=ext,
                        ))
                    except Exception:
                        pass
            except Exception:
                pass

            # --- Commits (up to 200) ---
            try:
                commits = []
                page = 1
                while len(commits) < 200:
                    batch = client.get(
                        f"/repos/{full_name}/commits",
                        params={"per_page": 100, "page": page},
                    )
                    time.sleep(API_DELAY_SECONDS)
                    if not batch:
                        break
                    commits.extend(batch)
                    if len(batch) < 100:
                        break
                    page += 1

                for c in commits[:200]:
                    commit_data = c.get("commit", {})
                    author = commit_data.get("author") or {}
                    committer = commit_data.get("committer") or {}
                    stats = c.get("stats") or {}
                    github_author = c.get("author") or {}

                    rows.append(_make_row("commit", full_name, ingested_at,
                        sha=c.get("sha"),
                        author_name=github_author.get("login") or author.get("name"),
                        author_email=author.get("email"),
                        author_date=author.get("date"),
                        committer_name=committer.get("name"),
                        committer_email=committer.get("email"),
                        committer_date=committer.get("date"),
                        message=commit_data.get("message", "")[:1000],
                        additions=stats.get("additions"),
                        deletions=stats.get("deletions"),
                    ))
            except Exception:
                pass

        yield pd.DataFrame(rows) if rows else _empty_staging_df()

# COMMAND ----------

# MAGIC %md
# MAGIC ## GraphQL Fetcher (mapInPandas)

# COMMAND ----------

_GRAPHQL_REPO_QUERY = """
query RepoData($owner: String!, $name: String!, $commitCount: Int!) {
  repository(owner: $owner, name: $name) {
    defaultBranchRef {
      name
      target {
        ... on Commit {
          history(first: $commitCount) {
            nodes {
              oid
              message
              additions
              deletions
              author {
                name
                email
                date
                user { login }
              }
              committer {
                name
                email
                date
              }
            }
            pageInfo { hasNextPage endCursor }
          }
        }
      }
    }
    f_package_json: object(expression: "HEAD:package.json") { ... on Blob { text oid byteSize } }
    f_requirements_txt: object(expression: "HEAD:requirements.txt") { ... on Blob { text oid byteSize } }
    f_setup_py: object(expression: "HEAD:setup.py") { ... on Blob { text oid byteSize } }
    f_pyproject_toml: object(expression: "HEAD:pyproject.toml") { ... on Blob { text oid byteSize } }
    f_cargo_toml: object(expression: "HEAD:Cargo.toml") { ... on Blob { text oid byteSize } }
    f_go_mod: object(expression: "HEAD:go.mod") { ... on Blob { text oid byteSize } }
    f_go_sum: object(expression: "HEAD:go.sum") { ... on Blob { text oid byteSize } }
    f_build_gradle: object(expression: "HEAD:build.gradle") { ... on Blob { text oid byteSize } }
    f_pom_xml: object(expression: "HEAD:pom.xml") { ... on Blob { text oid byteSize } }
    f_gemfile: object(expression: "HEAD:Gemfile") { ... on Blob { text oid byteSize } }
    f_composer_json: object(expression: "HEAD:composer.json") { ... on Blob { text oid byteSize } }
    f_setup_cfg: object(expression: "HEAD:setup.cfg") { ... on Blob { text oid byteSize } }
    f_pipfile: object(expression: "HEAD:Pipfile") { ... on Blob { text oid byteSize } }
  }
}
"""

_GRAPHQL_COMMITS_PAGE_QUERY = """
query CommitHistory($owner: String!, $name: String!, $after: String!) {
  repository(owner: $owner, name: $name) {
    defaultBranchRef {
      target {
        ... on Commit {
          history(first: 100, after: $after) {
            nodes {
              oid
              message
              additions
              deletions
              author {
                name
                email
                date
                user { login }
              }
              committer {
                name
                email
                date
              }
            }
            pageInfo { hasNextPage endCursor }
          }
        }
      }
    }
  }
}
"""

# Map GraphQL aliases → file paths
_GRAPHQL_FILE_ALIAS_MAP = {
    "f_package_json": "package.json",
    "f_requirements_txt": "requirements.txt",
    "f_setup_py": "setup.py",
    "f_pyproject_toml": "pyproject.toml",
    "f_cargo_toml": "Cargo.toml",
    "f_go_mod": "go.mod",
    "f_go_sum": "go.sum",
    "f_build_gradle": "build.gradle",
    "f_pom_xml": "pom.xml",
    "f_gemfile": "Gemfile",
    "f_composer_json": "composer.json",
    "f_setup_cfg": "setup.cfg",
    "f_pipfile": "Pipfile",
}

# COMMAND ----------

def fetch_all_for_repos_graphql(iterator):
    """
    GraphQL-based fetcher: 1 GraphQL query (commits + dep files) per repo.
    GraphQL rate limit is separate from REST, effectively doubling throughput.
    """
    import time
    import requests
    from datetime import datetime

    graphql_url = "https://api.github.com/graphql"
    headers = {"Authorization": f"Bearer {_github_token}", "Content-Type": "application/json"}

    def run_graphql(query, variables):
        resp = requests.post(graphql_url, headers=headers, json={"query": query, "variables": variables}, timeout=30)
        resp.raise_for_status()
        result = resp.json()
        if "errors" in result:
            raise Exception(f"GraphQL errors: {result['errors']}")
        return result["data"]

    def _paginate_commits(owner, name, page_info):
        """Fetch remaining commit pages up to 200 total."""
        all_extra = []
        while page_info.get("hasNextPage") and len(all_extra) < 100:
            page_data = run_graphql(_GRAPHQL_COMMITS_PAGE_QUERY, {
                "owner": owner, "name": name, "after": page_info["endCursor"],
            })
            time.sleep(API_DELAY_SECONDS)
            page_ref = (page_data.get("repository", {}).get("defaultBranchRef") or {}).get("target") or {}
            page_history = page_ref.get("history") or {}
            all_extra.extend(page_history.get("nodes") or [])
            page_info = page_history.get("pageInfo") or {}
        return all_extra

    for pdf in iterator:
        rows = []
        for full_name in pdf["full_name"].tolist():
            owner, name = full_name.split("/", 1)
            ingested_at = datetime.utcnow().isoformat()

            try:
                data = run_graphql(_GRAPHQL_REPO_QUERY, {"owner": owner, "name": name, "commitCount": 100})
                repo_data = data.get("repository")
                if not repo_data:
                    continue

                # --- Commits ---
                branch_ref = repo_data.get("defaultBranchRef") or {}
                target = branch_ref.get("target") or {}
                history = target.get("history") or {}
                commit_nodes = list(history.get("nodes") or [])
                page_info = history.get("pageInfo") or {}

                commit_nodes.extend(_paginate_commits(owner, name, page_info))

                for c in commit_nodes[:200]:
                    author = c.get("author") or {}
                    committer_info = c.get("committer") or {}
                    rows.append(_make_row("commit", full_name, ingested_at,
                        sha=c.get("oid"),
                        author_name=(author.get("user") or {}).get("login") or author.get("name"),
                        author_email=author.get("email"),
                        author_date=author.get("date"),
                        committer_name=committer_info.get("name"),
                        committer_email=committer_info.get("email"),
                        committer_date=committer_info.get("date"),
                        message=(c.get("message") or "")[:1000],
                        additions=c.get("additions"),
                        deletions=c.get("deletions"),
                    ))

                # --- Dependency files (content + file rows in one pass) ---
                for alias, file_path in _GRAPHQL_FILE_ALIAS_MAP.items():
                    blob = repo_data.get(alias)
                    if not blob:
                        continue
                    ext = file_path.rsplit(".", 1)[-1].lower() if "." in file_path else ""

                    rows.append(_make_row("file", full_name, ingested_at,
                        path=file_path, file_type="blob",
                        sha=blob.get("oid"), size=blob.get("byteSize"),
                        mode="100644", tree_truncated=False,
                    ))
                    if blob.get("text"):
                        rows.append(_make_row("content", full_name, ingested_at,
                            path=file_path, sha=blob.get("oid"), size=blob.get("byteSize"),
                            content=blob["text"][:500_000], encoding="utf-8", language=ext,
                        ))

            except Exception:
                pass

        yield pd.DataFrame(rows) if rows else _empty_staging_df()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discovery: AI Dependency Analysis

# COMMAND ----------

_AI_DEPENDENCY_PROMPT = """You are analyzing dependency files from GitHub repository "{source_repo}".
Below are the contents of its dependency/manifest files.

{dep_contents}

Based on these dependencies, suggest up to 10 specific GitHub repositories (as "owner/repo" format)
that are:
1. Direct dependencies visible in the files above
2. Key upstream libraries this project depends on

Rules:
- ONLY suggest repos you are confident exist on GitHub in "owner/repo" format
- Focus on the most significant/popular dependencies, skip trivial ones
- Do NOT suggest the source repo itself
- Respond ONLY with a JSON object mapping package names to repos, e.g.: {{"package1": "owner/repo1", "package2": "owner/repo2"}}
- If you cannot determine any repos, respond with: {{}}"""


def load_blocked_ai_suggestions():
    """Load current bad-feedback mappings and remove them from the AI cache."""
    try:
        blocked_df = spark.sql(f"""
            WITH latest_feedback AS (
                SELECT
                    source_repo,
                    package_name,
                    suggested_repo,
                    feedback,
                    _pg_change_type,
                    ROW_NUMBER() OVER (
                        PARTITION BY source_repo, package_name, suggested_repo
                        ORDER BY _sort_by DESC
                    ) AS rn
                FROM {feedback_history_table}
                WHERE _pg_change_type != 'update_preimage'
            )
            SELECT source_repo, package_name, suggested_repo
            FROM latest_feedback
            WHERE rn = 1
              AND _pg_change_type != 'delete'
              AND feedback = 'bad'
        """).dropDuplicates()
        blocked_rows = blocked_df.collect()
    except Exception as e:
        print(f"    ⚠ Could not load AI suggestion feedback from {feedback_history_table}: {e}")
        return set(), set()

    blocked_set = {
        (row["source_repo"], row["package_name"], row["suggested_repo"])
        for row in blocked_rows
    }
    if not blocked_set:
        return blocked_set, set()

    invalidated_sources = set()
    try:
        if spark.catalog.tableExists(ai_query_cache_table):
            invalidated_sources = {
                row["source_repo"]
                for row in spark.table(ai_query_cache_table).join(
                    blocked_df,
                    on=["source_repo", "package_name", "suggested_repo"],
                    how="inner",
                ).select("source_repo").distinct().collect()
            }
            from delta.tables import DeltaTable
            DeltaTable.forName(spark, ai_query_cache_table).alias("cache").merge(
                blocked_df.alias("blocked"),
                "cache.source_repo = blocked.source_repo AND "
                "cache.package_name = blocked.package_name AND "
                "cache.suggested_repo = blocked.suggested_repo",
            ).whenMatchedDelete().execute()
            print(f"    ✓ Removed {len(blocked_set)} blocked mapping(s) from the AI query cache")
    except Exception as e:
        # Keep enforcing the in-memory block even if cache cleanup fails.
        print(f"    ⚠ Could not remove blocked mappings from {ai_query_cache_table}: {e}")

    return blocked_set, invalidated_sources


def write_ai_query_cache(cache_rows):
    """Insert new cache mappings without duplicating mappings already retained."""
    if not cache_rows:
        return

    cache_df = spark.createDataFrame(
        cache_rows,
        schema=["source_repo", "package_name", "suggested_repo", "model_name"],
    ).dropDuplicates(["source_repo", "package_name", "suggested_repo"])

    if not spark.catalog.tableExists(ai_query_cache_table):
        cache_df.write.format("delta").saveAsTable(ai_query_cache_table)
        return

    from delta.tables import DeltaTable
    DeltaTable.forName(spark, ai_query_cache_table).alias("cache").merge(
        cache_df.alias("updates"),
        "cache.source_repo = updates.source_repo AND "
        "cache.package_name = updates.package_name AND "
        "cache.suggested_repo = updates.suggested_repo",
    ).whenNotMatchedInsertAll().execute()


def discover_repos_from_dependencies_ai(staging_df, already_seen, client):
    """
    Use ai_query to analyze dependency files and discover related repos.
    Validates suggestions exist on GitHub (up to 50 per hop).
    """
    global _github_request_count

    blocked_set, invalidated_sources = load_blocked_ai_suggestions()

    dep_contents_df = staging_df.filter(
        "category = 'content' AND path IN ('package.json', 'requirements.txt', 'setup.py', "
        "'pyproject.toml', 'Cargo.toml', 'go.mod', 'build.gradle', 'pom.xml', "
        "'Gemfile', 'composer.json', 'setup.cfg', 'Pipfile')"
    ).select("repo_full_name", "path", "content")

    dep_rows = dep_contents_df.collect()
    if not dep_rows:
        print("    No dependency files found in staging for AI analysis")
        return []

    # Group contents by source repo (truncate each file to 3KB)
    repo_deps = {}
    for row in dep_rows:
        repo = row["repo_full_name"]
        content = (row["content"] or "")[:3000]
        if content:
            repo_deps.setdefault(repo, []).append(f"### {row['path']}\n```\n{content}\n```")

    # Analyze up to 10 repos per hop (with caching)
    ai_suggestions = []
    for source_repo, dep_files in list(repo_deps.items())[:10]:
        # Check cache first — if we already have mappings for this source_repo, reuse them
        try:
            cached_df = spark.sql(
                "SELECT package_name, suggested_repo FROM {} WHERE source_repo = :repo".format(ai_query_cache_table),
                args={"repo": source_repo},
            )
            cached_rows = cached_df.collect()
        except Exception:
            cached_rows = []

        cached_rows = [
            row for row in cached_rows
            if (source_repo, row["package_name"], row["suggested_repo"]) not in blocked_set
        ]
        if source_repo in invalidated_sources:
            # A rejected mapping invalidates this source's cache for this run, so
            # ai_query gets another opportunity to return a better replacement.
            cached_rows = []
            print(f"    ↻ Cache invalidated for {source_repo} by human feedback")

        if cached_rows:
            print(f"    ✓ Cache hit for {source_repo} ({len(cached_rows)} mappings)")
            for row in cached_rows:
                suggested = row["suggested_repo"]
                if "/" in suggested and suggested not in already_seen:
                    ai_suggestions.append((source_repo, suggested))
                    record_edge(source_repo, suggested, "dependency", {"discovered_by": "ai_query_cache"})
            continue

        # No cache — call ai_query
        prompt = _AI_DEPENDENCY_PROMPT.format(
            source_repo=source_repo,
            dep_contents="\n\n".join(dep_files),
        )
        try:
            result_df = spark.sql(
                "SELECT ai_query('databricks-meta-llama-3-3-70b-instruct', :prompt) AS response",
                args={"prompt": prompt},
            )
            response_text = result_df.collect()[0]["response"].strip()

            # Strip markdown code fences if present
            if response_text.startswith("```"):
                response_text = response_text.split("\n", 1)[1].rsplit("```", 1)[0]

            suggested_map = json.loads(response_text)
            if isinstance(suggested_map, dict):
                allowed_suggestions = [
                    (pkg, repo)
                    for pkg, repo in suggested_map.items()
                    if isinstance(pkg, str)
                    and isinstance(repo, str)
                    and "/" in repo
                    and (source_repo, pkg, repo) not in blocked_set
                ]
                suppressed_count = len(suggested_map) - len(allowed_suggestions)
                if suppressed_count:
                    print(f"    ✓ Suppressed {suppressed_count} blocked or invalid AI suggestion(s)")

                # Write package→repo mappings to cache
                cache_rows = [
                    (source_repo, pkg, repo, "databricks-meta-llama-3-3-70b-instruct")
                    for pkg, repo in allowed_suggestions
                ]
                write_ai_query_cache(cache_rows)

                for pkg, suggested in allowed_suggestions:
                    if suggested not in already_seen:
                        ai_suggestions.append((source_repo, suggested))
                        record_edge(source_repo, suggested, "dependency", {"discovered_by": "ai_query"})
        except Exception as e:
            print(f"    ⚠ AI analysis failed for {source_repo}: {e}")

    if not ai_suggestions:
        print("    No new repos suggested by AI")
        return []

    # Deduplicate and validate via GitHub API
    max_validations = min(50, MAX_GITHUB_REQUESTS_PER_RUN - _github_request_count)
    seen_suggestions = set()
    unique_suggestions = []
    for source, target in ai_suggestions:
        if target not in seen_suggestions and target not in already_seen:
            seen_suggestions.add(target)
            unique_suggestions.append((source, target))

    print(f"    AI suggested {len(unique_suggestions)} unique new repos, validating up to {max_validations}...")

    validated = []
    for source, suggested_repo in unique_suggestions[:max_validations]:
        if len(already_seen) >= TARGET_REPOS:
            break
        try:
            client.get_repo(suggested_repo)
            _github_request_count += 1
            time.sleep(API_DELAY_SECONDS)
            validated.append(suggested_repo)
            already_seen.add(suggested_repo)
            record_edge(source, suggested_repo, "ai_suggested", {"validated": True})
        except Exception:
            _github_request_count += 1

    print(f"    ✓ Validated {len(validated)} repos from AI suggestions")
    return validated

# COMMAND ----------

# MAGIC %md
# MAGIC ## Graph Edge Tracking

# COMMAND ----------

_graph_edges = []


def record_edge(source, target, edge_type, metadata=None):
    """Record a graph edge (committer→repo, dependency→repo, etc.)."""
    _graph_edges.append({
        "source": source,
        "target": target,
        "edge_type": edge_type,
        "metadata": json.dumps(metadata) if metadata else None,
        "discovered_at": datetime.utcnow().isoformat(),
    })


def write_graph_edges():
    """Persist accumulated graph edges to a Delta table via MERGE (keyed on source, target)."""
    if not _graph_edges:
        return
    edges_schema = StructType([
        StructField("source", StringType(), False),
        StructField("target", StringType(), False),
        StructField("edge_type", StringType(), False),
        StructField("metadata", StringType(), True),
        StructField("discovered_at", StringType(), False),
    ])
    edges_df = spark.createDataFrame(_graph_edges, schema=edges_schema)

    # Ensure target table exists
    if not spark.catalog.tableExists(graph_edges_table):
        edges_df.write.format("delta").option("mergeSchema", "true").saveAsTable(graph_edges_table)
    else:
        from delta.tables import DeltaTable
        target_table = DeltaTable.forName(spark, graph_edges_table)
        target_table.alias("t").merge(
            edges_df.dropDuplicates(['source', 'target']).alias("s"),
            "t.source = s.source AND t.target = s.target"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    print(f"  ✓ Merged {len(_graph_edges)} graph edges into {graph_edges_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orchestration Helpers

# COMMAND ----------

def fetch_and_stage(repo_names, mode="overwrite"):
    """Fetch repo data in parallel via mapInPandas and write to staging table."""
    if not repo_names:
        return
    batch_df = spark.createDataFrame(
        [(name,) for name in repo_names], schema=["full_name"]
    ).repartition(min(len(repo_names), 16))

    fetcher = fetch_all_for_repos_graphql if FETCH_MODE == "graphql" else fetch_all_for_repos
    results_df = batch_df.mapInPandas(fetcher, schema=output_schema)
    results_df.write.format("delta").mode(mode).option("overwriteSchema", "true").saveAsTable(github_staging_table)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Traversal Loop

# COMMAND ----------

print(f"Fetch mode: {FETCH_MODE.upper()}")
print(f"  graphql = 1 GraphQL query (commits+deps) + 1 REST (tree) per repo")
print(f"  rest    = ~5-7 REST calls per repo")
print(f"Target: {TARGET_REPOS} repos, max {MAX_HOPS} hops\n")

# Seed round
seed_names = [r["full_name"] for r in repos]
all_fetched = set(seed_names)
print(f"Round 0 (seed): Fetching {len(seed_names)} repos...")
fetch_and_stage(seed_names, mode="overwrite")
print(f"  ✓ Seed repos staged")

driver_client = GitHubClient(token=_github_token)

# COMMAND ----------

staging_df = spark.table(github_staging_table)
for hop in range(1, MAX_HOPS + 1):
    if len(all_fetched) >= TARGET_REPOS:
        break
    if _github_request_count >= MAX_GITHUB_REQUESTS_PER_RUN:
        print(f"  ⚠ Approaching API rate limit ({_github_request_count} requests used), stopping early")
        break

    print(f"\n--- Hop {hop}/{MAX_HOPS} (fetched so far: {len(all_fetched)}) ---")


    # --- Discovery: AI dependency analysis ---
    print(f"  AI dependency discovery:")
    dep_repos = discover_repos_from_dependencies_ai(staging_df, all_fetched, driver_client)
    print(f"  ✓ Dependency discovery found {len(dep_repos)} new repos")

    # --- Fetch newly discovered repos ---
    new_repos = dep_repos
    if not new_repos:
        print(f"  No new repos discovered in hop {hop}, stopping")
        break

    print(f"  Fetching {len(new_repos)} new repos")
    fetch_and_stage(new_repos, mode="append")

    calls_per_repo = 2 if FETCH_MODE == "graphql" else 6
    _github_request_count += len(new_repos) * calls_per_repo
    print(f"  Estimated API calls used so far: ~{_github_request_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Finalize

# COMMAND ----------

print(f"\nWriting {len(_graph_edges)} graph edges...")
write_graph_edges()

staging_df = spark.table(github_staging_table)
result = {
    "staging_table": github_staging_table,
    "graph_edges_table": graph_edges_table,
    "total_repos_staged": staging_df.filter("category = 'file'").select("repo_full_name").distinct().count(),
    "file_count": staging_df.filter("category = 'file'").count(),
    "content_count": staging_df.filter("category = 'content'").count(),
    "commit_count": staging_df.filter("category = 'commit'").count(),
    "graph_edges_count": len(_graph_edges),
    "github_api_calls_estimated": _github_request_count,
}
print(result)
dbutils.notebook.exit(json.dumps(result))
