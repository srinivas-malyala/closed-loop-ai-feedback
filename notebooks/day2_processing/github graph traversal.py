# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Traverse GitHub repo graph
import json
import base64
import time
from datetime import datetime

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, LongType, BooleanType, StructField, StructType

from github_client import GitHubClient


dbutils.widgets.text("catalog", "bootcamp_students", "Unity Catalog name")
dbutils.widgets.text("schema", "abhibastia", "Schema name")
dbutils.widgets.text("target_repos", "500", "Target repos")
dbutils.widgets.text("max_hops", "3", "Max hops")
dbutils.widgets.text("fetch_mode", "graphql", "Fetch mode: graphql or rest")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
TARGET_REPOS = int(dbutils.widgets.get("target_repos"))
MAX_HOPS = int(dbutils.widgets.get("max_hops"))
FETCH_MODE = dbutils.widgets.get("fetch_mode")  # "graphql" or "rest"

repos_history_table = f"{catalog}.{schema}.lb_github_repos_history"
github_staging_table = f"{catalog}.{schema}.github_api_staging"
graph_edges_table = f"{catalog}.{schema}.github_graph_edges"

# Budget: GitHub REST API = 5000 req/hr. Track usage to avoid blowing the limit.
# Each repo fetch costs ~2 calls (graphql) or ~5-7 (rest). Reserve some for discovery.
MAX_GITHUB_REQUESTS_PER_RUN = 4000  # Leave headroom
_github_request_count = 0

DEPENDENCY_FILENAMES = {
    "package.json", "requirements.txt", "setup.py", "pyproject.toml",
    "Cargo.toml", "go.mod", "go.sum", "build.gradle", "pom.xml",
    "Gemfile", "composer.json", "setup.cfg", "Pipfile", "Pipfile.lock",
    "package-lock.json", "yarn.lock", "poetry.lock",
}
API_DELAY_SECONDS = 0.001

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

_github_token = dbutils.secrets.get(scope="github", key="token")

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
    import base64
    import time
    from datetime import datetime
    from github_client import GitHubClient

    client = GitHubClient(token=_github_token)

    for pdf in iterator:
        rows = []
        for full_name in pdf["full_name"].tolist():
            try:
                repo_meta = client.get_repo(full_name)
                time.sleep(API_DELAY_SECONDS)
                default_branch = repo_meta.get("default_branch", "main")

                tree_data = client.get_repo_tree(full_name, ref=default_branch)
                time.sleep(API_DELAY_SECONDS)

                tree_entries = tree_data.get("tree", [])
                truncated = tree_data.get("truncated", False)
                ingested_at = datetime.utcnow().isoformat()

                # Only keep dependency indicator files
                dep_files = [
                    e for e in tree_entries
                    if e.get("type") == "blob"
                    and e["path"].rsplit("/", 1)[-1] in DEPENDENCY_FILENAMES
                ]

                for entry in dep_files:
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

                for file_entry in dep_files:
                    file_path = file_entry["path"]
                    try:
                        content_data = client.get(
                            f"/repos/{full_name}/contents/{file_path}",
                            params={"ref": default_branch},
                        )
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
                pass

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
                commits = commits[:200]

                for c in commits:
                    commit_data = c.get("commit", {})
                    author = commit_data.get("author", {}) or {}
                    committer = commit_data.get("committer", {}) or {}
                    stats = c.get("stats", {}) or {}
                    github_author = c.get("author") or {}

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
                        "author_name": github_author.get("login") or author.get("name"),
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

        if rows:
            yield pd.DataFrame(rows)
        else:
            yield pd.DataFrame(columns=[
                "category", "repo_full_name", "path", "file_type", "sha", "size",
                "mode", "tree_truncated", "content", "encoding", "language",
                "author_name", "author_email", "author_date", "committer_name",
                "committer_email", "committer_date", "message", "additions",
                "deletions", "ingested_at",
            ])


# =============================================================================
# GraphQL-based fetcher: gets commits + dep file contents in 1-2 queries per repo
# Uses REST only for the recursive tree (which GraphQL can't do in one shot).
# Net result: ~2 API calls per repo (1 GraphQL + 1 REST tree) vs ~5-7 REST calls.
# GraphQL rate limit is SEPARATE from REST (5000 points/hr each), so this
# effectively doubles the available budget.
# =============================================================================

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
    # Dependency file contents via aliases (returns null if file doesn't exist)
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

# Map GraphQL aliases back to file paths
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


def fetch_all_for_repos_graphql(iterator):
    """
    GraphQL-based mapInPandas fetcher.
    Per repo: 1 GraphQL query (commits + dep files) + 1 REST call (recursive tree).
    GraphQL rate limit is separate from REST, so this effectively doubles throughput.
    """
    import time
    import requests
    from datetime import datetime

    token = _github_token
    graphql_url = "https://api.github.com/graphql"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    def run_graphql(query, variables):
        resp = requests.post(
            graphql_url,
            headers=headers,
            json={"query": query, "variables": variables},
            timeout=30,
        )
        resp.raise_for_status()
        result = resp.json()
        if "errors" in result:
            raise Exception(f"GraphQL errors: {result['errors']}")
        return result["data"]

    for pdf in iterator:
        rows = []
        for full_name in pdf["full_name"].tolist():
            owner, name = full_name.split("/", 1)
            ingested_at = datetime.utcnow().isoformat()

            # --- 1. GraphQL: get commits + dependency file contents ---
            try:
                data = run_graphql(_GRAPHQL_REPO_QUERY, {
                    "owner": owner, "name": name, "commitCount": 100,
                })
                repo_data = data.get("repository")
                if not repo_data:
                    continue

                # Parse commits
                branch_ref = repo_data.get("defaultBranchRef") or {}
                default_branch = branch_ref.get("name", "main")
                target = branch_ref.get("target") or {}
                history = target.get("history") or {}
                commit_nodes = history.get("nodes") or []
                page_info = history.get("pageInfo") or {}

                # Paginate commits up to 200
                all_commits = list(commit_nodes)
                while page_info.get("hasNextPage") and len(all_commits) < 200:
                    page_data = run_graphql(_GRAPHQL_COMMITS_PAGE_QUERY, {
                        "owner": owner, "name": name,
                        "after": page_info["endCursor"],
                    })
                    time.sleep(API_DELAY_SECONDS)
                    page_repo = page_data.get("repository", {})
                    page_ref = (page_repo.get("defaultBranchRef") or {}).get("target") or {}
                    page_history = page_ref.get("history") or {}
                    new_nodes = page_history.get("nodes") or []
                    all_commits.extend(new_nodes)
                    page_info = page_history.get("pageInfo") or {}
                all_commits = all_commits[:200]

                for c in all_commits:
                    author = c.get("author") or {}
                    committer_info = c.get("committer") or {}
                    author_user = author.get("user") or {}
                    rows.append({
                        "category": "commit",
                        "repo_full_name": full_name,
                        "path": None,
                        "file_type": None,
                        "sha": c.get("oid"),
                        "size": None,
                        "mode": None,
                        "tree_truncated": None,
                        "content": None,
                        "encoding": None,
                        "language": None,
                        "author_name": author_user.get("login") or author.get("name"),
                        "author_email": author.get("email"),
                        "author_date": author.get("date"),
                        "committer_name": committer_info.get("name"),
                        "committer_email": committer_info.get("email"),
                        "committer_date": committer_info.get("date"),
                        "message": (c.get("message") or "")[:1000],
                        "additions": c.get("additions"),
                        "deletions": c.get("deletions"),
                        "ingested_at": ingested_at,
                    })

                # Parse dependency file contents from GraphQL aliases
                for alias, file_path in _GRAPHQL_FILE_ALIAS_MAP.items():
                    blob = repo_data.get(alias)
                    if blob and blob.get("text"):
                        ext = file_path.rsplit(".", 1)[-1].lower() if "." in file_path else ""
                        rows.append({
                            "category": "content",
                            "repo_full_name": full_name,
                            "path": file_path,
                            "file_type": None,
                            "sha": blob.get("oid"),
                            "size": blob.get("byteSize"),
                            "mode": None,
                            "tree_truncated": None,
                            "content": blob["text"][:500_000],
                            "encoding": "utf-8",
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
                            "ingested_at": ingested_at,
                        })

                # Emit "file" rows for dep files that exist (no tree call needed)
                for alias, file_path in _GRAPHQL_FILE_ALIAS_MAP.items():
                    blob = repo_data.get(alias)
                    if blob:
                        rows.append({
                            "category": "file",
                            "repo_full_name": full_name,
                            "path": file_path,
                            "file_type": "blob",
                            "sha": blob.get("oid"),
                            "size": blob.get("byteSize"),
                            "mode": "100644",
                            "tree_truncated": False,
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

            except Exception:
                pass

        if rows:
            yield pd.DataFrame(rows)
        else:
            yield pd.DataFrame(columns=[
                "category", "repo_full_name", "path", "file_type", "sha", "size",
                "mode", "tree_truncated", "content", "encoding", "language",
                "author_name", "author_email", "author_date", "committer_name",
                "committer_email", "committer_date", "message", "additions",
                "deletions", "ingested_at",
            ])


# =============================================================================
# GraphQL-based committer repo discovery
# =============================================================================

_GRAPHQL_USER_REPOS_QUERY = """
query UserRepos($login: String!) {
  user(login: $login) {
    repositories(first: 100, orderBy: {field: PUSHED_AT, direction: DESC}) {
      nodes { nameWithOwner }
    }
  }
}
"""


def discover_committer_repos_graphql(committer_usernames, already_seen, token):
    """Discover repos via GraphQL user queries. Uses GraphQL rate limit (separate from REST)."""
    import requests
    graphql_url = "https://api.github.com/graphql"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    discovered = []
    for username in committer_usernames:
        if len(already_seen) >= TARGET_REPOS:
            break
        try:
            resp = requests.post(
                graphql_url,
                headers=headers,
                json={"query": _GRAPHQL_USER_REPOS_QUERY, "variables": {"login": username}},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json().get("data", {})
            user_data = data.get("user")
            if not user_data:
                continue
            repos_nodes = user_data.get("repositories", {}).get("nodes", [])
            time.sleep(API_DELAY_SECONDS)
            for repo in repos_nodes:
                full_name = repo.get("nameWithOwner", "")
                if full_name and full_name not in already_seen:
                    discovered.append(full_name)
                    already_seen.add(full_name)
                    if len(already_seen) >= TARGET_REPOS:
                        break
        except Exception:
            pass
    return discovered


def fetch_and_stage(repo_names, mode="overwrite"):
    if not repo_names:
        return
    batch_df = spark.createDataFrame([(name,) for name in repo_names], schema=["full_name"]).repartition(min(len(repo_names), 16))
    fetcher = fetch_all_for_repos_graphql if FETCH_MODE == "graphql" else fetch_all_for_repos
    results_df = batch_df.mapInPandas(fetcher, schema=output_schema)
    (
        results_df.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .saveAsTable(github_staging_table)
    )


def discover_committer_repos(committer_usernames, already_seen, client):
    discovered = []
    for username in committer_usernames:
        if len(already_seen) >= TARGET_REPOS:
            break
        try:
            user_repos = client.get(f"/users/{username}/repos", params={"per_page": 100, "sort": "pushed"})
            time.sleep(API_DELAY_SECONDS)
            for repo in user_repos:
                full_name = repo.get("full_name", "")
                if full_name and full_name not in already_seen:
                    discovered.append(full_name)
                    already_seen.add(full_name)
                    if len(already_seen) >= TARGET_REPOS:
                        break
        except Exception:
            pass
    return discovered


# =============================================================================
# Graph edge tracking: records HOW repos/committers are connected
# =============================================================================

_graph_edges = []  # Accumulate edges during traversal, write at the end


def record_edge(source, target, edge_type, metadata=None):
    """Record a graph edge (repo->repo, committer->repo, dependency->repo)."""
    _graph_edges.append({
        "source": source,
        "target": target,
        "edge_type": edge_type,  # "committer", "dependency", "ai_suggested"
        "metadata": json.dumps(metadata) if metadata else None,
        "discovered_at": datetime.utcnow().isoformat(),
    })


def write_graph_edges():
    """Write all accumulated graph edges to a Delta table."""
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
    (
        edges_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(graph_edges_table)
    )
    print(f"  ✓ Wrote {len(_graph_edges)} graph edges to {graph_edges_table}")


# =============================================================================
# AI-powered repo discovery from dependency files
# Uses ai_query to analyze dependency contents and suggest real owner/repo names
# =============================================================================

_AI_DEPENDENCY_PROMPT = """You are analyzing dependency files from GitHub repository "{source_repo}".
Below are the contents of its dependency/manifest files.

{dep_contents}

Based on these dependencies, suggest up to 10 specific GitHub repositories (as "owner/repo" format)
that are:
1. Direct dependencies visible in the files above (e.g., if package.json lists "express", suggest "expressjs/express")
2. Key upstream libraries this project depends on

Rules:
- ONLY suggest repos you are confident exist on GitHub in "owner/repo" format
- Focus on the most significant/popular dependencies, skip trivial ones (e.g., skip type stubs, tiny utils)
- Do NOT suggest the source repo itself
- Respond ONLY with a JSON array of strings, e.g.: ["owner/repo1", "owner/repo2"]
- If you cannot determine any repos, respond with: []"""


def discover_repos_from_dependencies_ai(staging_df, already_seen, client):
    """
    Use ai_query to analyze dependency files and discover related repos.
    
    Strategy:
    1. Read dependency file contents from staging
    2. Group by source repo, batch into AI prompts
    3. AI suggests specific owner/repo names
    4. Validate suggestions exist via GitHub API (1 call each, but only for new repos)
    
    Rate-limit aware: validates at most 50 suggestions per hop to conserve API budget.
    """
    global _github_request_count

    # Get repos that have dependency content in staging
    dep_contents_df = staging_df.filter(
        "category = 'content' AND path IN ('package.json', 'requirements.txt', 'setup.py', "
        "'pyproject.toml', 'Cargo.toml', 'go.mod', 'build.gradle', 'pom.xml', "
        "'Gemfile', 'composer.json', 'setup.cfg', 'Pipfile')"
    ).select("repo_full_name", "path", "content")

    # Collect and group by repo (limit to avoid massive AI prompts)
    dep_rows = dep_contents_df.collect()
    if not dep_rows:
        print("    No dependency files found in staging for AI analysis")
        return []

    # Group contents by source repo
    repo_deps = {}
    for row in dep_rows:
        repo = row["repo_full_name"]
        if repo not in repo_deps:
            repo_deps[repo] = []
        # Truncate each file to 3KB to keep prompts manageable
        content = (row["content"] or "")[:3000]
        if content:
            repo_deps[repo].append(f"### {row['path']}\n```\n{content}\n```")

    # Process up to 10 repos per hop to control AI cost
    ai_suggestions = []
    repos_to_analyze = list(repo_deps.items())[:10]

    for source_repo, dep_files in repos_to_analyze:
        dep_contents_text = "\n\n".join(dep_files)
        prompt = _AI_DEPENDENCY_PROMPT.format(
            source_repo=source_repo,
            dep_contents=dep_contents_text
        )

        try:
            result_df = spark.sql("""
                SELECT ai_query(
                    'databricks-meta-llama-3-1-70b-instruct',
                    :prompt
                ) AS response
            """, args={"prompt": prompt})

            response_text = result_df.collect()[0]["response"].strip()
            # Parse JSON array from response
            if response_text.startswith("```"):
                response_text = response_text.split("\n", 1)[1]
                response_text = response_text.rsplit("```", 1)[0]
            
            suggested_repos = json.loads(response_text)
            
            if isinstance(suggested_repos, list):
                for suggested in suggested_repos:
                    if (isinstance(suggested, str) 
                        and "/" in suggested 
                        and suggested not in already_seen):
                        ai_suggestions.append((source_repo, suggested))
                        # Record the dependency edge regardless of validation
                        record_edge(source_repo, suggested, "dependency",
                                   {"discovered_by": "ai_query"})

        except Exception as e:
            print(f"    ⚠ AI analysis failed for {source_repo}: {e}")

    if not ai_suggestions:
        print("    No new repos suggested by AI")
        return []

    # Validate suggestions exist on GitHub (rate-limit aware)
    # Limit validations to conserve API budget
    max_validations = min(50, MAX_GITHUB_REQUESTS_PER_RUN - _github_request_count)
    validated = []
    
    # Deduplicate suggestions
    seen_suggestions = set()
    unique_suggestions = []
    for source, target in ai_suggestions:
        if target not in seen_suggestions and target not in already_seen:
            seen_suggestions.add(target)
            unique_suggestions.append((source, target))

    print(f"    AI suggested {len(unique_suggestions)} unique new repos, validating up to {max_validations}...")

    for source, suggested_repo in unique_suggestions[:max_validations]:
        if len(already_seen) >= TARGET_REPOS:
            break
        try:
            # One API call to verify the repo exists
            client.get_repo(suggested_repo)
            _github_request_count += 1
            time.sleep(API_DELAY_SECONDS)
            validated.append(suggested_repo)
            already_seen.add(suggested_repo)
            # Update the edge to mark it as validated
            record_edge(source, suggested_repo, "ai_suggested",
                       {"validated": True, "source_type": "dependency_analysis"})
        except Exception:
            _github_request_count += 1
            # Repo doesn't exist or is private - skip silently
            pass

    print(f"    ✓ Validated {len(validated)} repos from AI suggestions")
    return validated


print(f"Fetch mode: {FETCH_MODE.upper()}")
print(f"  graphql = 1 GraphQL query (commits+deps) + 1 REST (tree) per repo")
print(f"  rest    = ~5-7 REST calls per repo")
print(f"Target: {TARGET_REPOS} repos, max {MAX_HOPS} hops\n")

seed_names = [r["full_name"] for r in repos]
all_fetched = set(seed_names)
print(f"Round 0 (seed): Fetching {len(seed_names)} repos...")
fetch_and_stage(seed_names, mode="overwrite")
print(f"  ✓ Seed repos staged")

driver_client = GitHubClient(token=_github_token)

for hop in range(1, MAX_HOPS + 1):
    if len(all_fetched) >= TARGET_REPOS:
        break
    if _github_request_count >= MAX_GITHUB_REQUESTS_PER_RUN:
        print(f"  ⚠ Approaching API rate limit ({_github_request_count} requests used), stopping early")
        break

    print(f"\n--- Hop {hop}/{MAX_HOPS} (fetched so far: {len(all_fetched)}) ---")
    staging_df = spark.table(github_staging_table)

    # =========================================================================
    # Discovery method 1: Committer-based (committer -> repo edges)
    # =========================================================================
    committer_rows = staging_df.filter("category = 'commit'").select("author_name", "author_email", "repo_full_name").distinct().collect()
    committer_usernames = set()
    committer_to_source_repo = {}  # Track which repo each committer came from
    for row in committer_rows:
        login = (row["author_name"] or "").strip()
        email = (row["author_email"] or "").strip()
        source_repo = row["repo_full_name"]
        username = None
        if login and all(ch not in login for ch in (" ", "<", ">")):
            username = login
        elif "+" in email and "@users.noreply.github.com" in email:
            username = email.split("+")[1].split("@")[0]
        elif "@users.noreply.github.com" in email:
            username = email.split("@")[0]
        if username and len(username) > 1 and username not in ("noreply", "actions", "dependabot"):
            committer_usernames.add(username)
            if username not in committer_to_source_repo:
                committer_to_source_repo[username] = source_repo

    committer_usernames = list(committer_usernames)
    print(f"  Committer discovery: {len(committer_usernames)} unique usernames to explore")

    if FETCH_MODE == "graphql":
        committer_repos = discover_committer_repos_graphql(committer_usernames, all_fetched, _github_token)
    else:
        committer_repos = discover_committer_repos(committer_usernames, all_fetched, driver_client)

    # Record committer->repo edges
    for repo in committer_repos:
        # Find which committer led to this repo (best effort)
        for username, source_repo in committer_to_source_repo.items():
            record_edge(username, repo, "committer",
                       {"source_repo": source_repo})
            # Also record repo->repo via shared committer
            record_edge(source_repo, repo, "shared_committer",
                       {"via_committer": username})
            break  # One edge per discovered repo is enough

    print(f"  ✓ Committer discovery found {len(committer_repos)} new repos")

    # =========================================================================
    # Discovery method 2: AI-powered dependency analysis (repo -> repo edges)
    # =========================================================================
    print(f"  AI dependency discovery:")
    dep_repos = discover_repos_from_dependencies_ai(staging_df, all_fetched, driver_client)
    print(f"  ✓ Dependency discovery found {len(dep_repos)} new repos")

    # =========================================================================
    # Combine and fetch all newly discovered repos
    # =========================================================================
    new_repos = committer_repos + dep_repos
    if not new_repos:
        print(f"  No new repos discovered in hop {hop}, stopping")
        break

    print(f"  Fetching {len(new_repos)} new repos (committer: {len(committer_repos)}, deps: {len(dep_repos)})")
    fetch_and_stage(new_repos, mode="append")

    # Estimate API calls used this hop
    calls_per_repo = 2 if FETCH_MODE == "graphql" else 6
    _github_request_count += len(new_repos) * calls_per_repo
    print(f"  Estimated API calls used so far: ~{_github_request_count}")

# Write all graph edges at the end
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

# COMMAND ----------

