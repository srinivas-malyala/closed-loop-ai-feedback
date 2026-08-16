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

    staging_df = spark.table(github_staging_table)
    committer_rows = staging_df.filter("category = 'commit'").select("author_name", "author_email").distinct().collect()
    committer_usernames = set()
    for row in committer_rows:
        login = (row["author_name"] or "").strip()
        email = (row["author_email"] or "").strip()
        if login and all(ch not in login for ch in (" ", "<", ">")):
            committer_usernames.add(login)
        if "+" in email and "@users.noreply.github.com" in email:
            committer_usernames.add(email.split("+")[1].split("@")[0])
        elif "@users.noreply.github.com" in email:
            committer_usernames.add(email.split("@")[0])

    committer_usernames = [u for u in committer_usernames if u and len(u) > 1 and u not in ("noreply", "actions", "dependabot")]
    if FETCH_MODE == "graphql":
        new_repos = discover_committer_repos_graphql(committer_usernames, all_fetched, _github_token)
    else:
        new_repos = discover_committer_repos(committer_usernames, all_fetched, driver_client)
    if not new_repos:
        break
    fetch_and_stage(new_repos, mode="append")

staging_df = spark.table(github_staging_table)
result = {
    "staging_table": github_staging_table,
    "total_repos_staged": staging_df.filter("category = 'file'").select("repo_full_name").distinct().count(),
    "file_count": staging_df.filter("category = 'file'").count(),
    "content_count": staging_df.filter("category = 'content'").count(),
    "commit_count": staging_df.filter("category = 'commit'").count(),
}
print(result)
dbutils.notebook.exit(json.dumps(result))

# COMMAND ----------

