# LTAP Lab: GitHub Repo Insights via Spark + Lakebase

A Databricks App that demonstrates the full LTAP round trip, split across two
lab days:

```
Day 1: CDC / replication into Delta       Day 2: Graph traversal & discovery
------------------------------------      ------------------------------------
GitHub API --(ingest notebooks)-->        Watchlist (seed repos) --(multi-hop
  Bronze Delta (raw repos + files)          graph crawl)--> github_api_staging
                                             + github_graph_edges (Delta)
                                                        |
                                             (Databricks Synced Table, reverse ETL)
                                                        v
                                             Lakebase Postgres (read-only graph tables)
                                                        |
                                             Flask app  <-- also writes a personal
                                                            watchlist table, and
                                                            provisions per-student
                                                            schemas on sign-in
```

**Day 1** is about landing raw GitHub data into Delta as faithfully as
possible - full-snapshot replication into Bronze tables (`notebooks/day1_ingest/`).
GitHub's REST API has no native CDC/webhook change stream for this lab's
scope, so "replication" here means: re-fetch and overwrite/append the full
snapshot each run, rather than row-level change capture.

**Day 2** is where the heavy lifting happens: multi-hop graph traversal
starting from your watchlist repos (`notebooks/day2_processing/`). The graph
crawler discovers related repositories via shared committers and AI-powered
dependency analysis, landing staging and edge tables that get synced back to
Lakebase.

The point of the demo: **you should never run heavy graph traversal or
discovery logic inside your operational Postgres database.** Do it in Spark
on Delta, then sync the *result* back into Lakebase as fast, read-only
tables your app can query with simple SQL - no Spark cluster needed at
request time.

## Files

- `app.py` - Flask app: `/healthz`, `/login` (GET/POST, username sign-in + per-student Lakebase schema), `/logout`, `/insights` (GET, read-only synced table), `/watchlist` (GET/POST)
- `lakebase.py` - Lakebase connection helper (single `LAKEBASE_URL`, psycopg2 + SQLAlchemy)
- `github_client.py` - GitHub REST API client (paginated search + single-repo lookup)
- `setup_secrets.py` - One-time script to store the Lakebase URL and an optional GitHub PAT
- `notebooks/day1_ingest/ingest_github_repos.py` - Day 1. Pulls repos from GitHub's Search API into a Bronze Delta table
- `notebooks/day1_ingest/ingest_github_files.py` - Day 1. Walks every repo's full file tree (Git Trees API, limited concurrency) into a Bronze Delta table
- `app.yaml` - Databricks App deployment config (command + env vars)
- `.env.example` - Local dev env var template (copy to `.env`, do not commit real values)
- `templates/index.html` - UI: add repos to your watchlist, browse Spark-processed insights

## Step-by-step setup

### 1. Create a Lakebase instance and a native-password role

1. In your Databricks workspace, go to **Catalog** (left sidebar) and select the **Lakebase** tab (or search "Lakebase" in the workspace search bar).
2. Click **Create Lakebase instance**, give it a name (e.g. `github-insights-db`), choose defaults, and wait for **Available**.
3. Open the instance, go to **Roles & Databases**, enable **Native (password) authentication** if not already on.
4. **Create a new role** with **Password** auth (e.g. `github_app`), and copy the connection URL:

   ```
   postgresql://<role>:<password>@<host>.database.cloud.databricks.com:5432/databricks_postgres?sslmode=require
   ```

### 2. (Optional) Create a GitHub personal access token

Unauthenticated GitHub API requests work fine for this lab (60 req/hr is
plenty for a few hundred repos). If you want higher throughput:

1. Go to GitHub → **Settings** → **Developer settings** → **Personal access tokens** → **Fine-grained tokens**.
2. Generate a token with **read-only** access to public repositories.
3. Keep it handy for the next step - you can also just skip this entirely.

### 3. Store your secrets

Run once from a Databricks notebook or terminal attached to a cluster:

```python
%sh python setup_secrets.py
```

This prompts (via `getpass`) for your **Lakebase connection URL** and an
**optional GitHub token**. Press Enter at the GitHub prompt to skip it.

### 4. Day 1 - Ingest/replicate raw GitHub data into Bronze Delta

Open `notebooks/day1_ingest/ingest_github_repos.py` in Databricks, attach it
to a cluster, and run it. Adjust the widgets at the top:
- `catalog` / `schema` - where to land the Bronze table (defaults to `main.ltap_lab_day1`)
- `query` - a GitHub search query, e.g. `language:python stars:>1000`
- `max_results` - how many repos to pull (up to 1000 per GitHub Search API limits)

This writes `github_repos_bronze` with one row per repo and its raw JSON payload.

### 5. Day 1 - Capture every file in each repo

Open `notebooks/day1_ingest/ingest_github_files.py` and run it against the same
catalog/schema, after step 4 has populated `github_repos_bronze`. For each
repo it makes ONE GitHub API call (`GET /repos/{full_name}/git/trees/{sha}?recursive=1`)
to fetch the entire file tree, then flattens every file (`path`, `size`,
`sha`, `mode`) into a row. Adjust the widgets:
- `max_workers` - how many repos to fetch concurrently (default 5) - keeps
  the notebook within GitHub's rate limit (60 req/hr unauthenticated, 5,000
  req/hr with a PAT) while still parallelizing across hundreds of repos
- `max_repos` - cap how many repos to process (0 = all rows in the bronze
  repos table)
- `mode` - `overwrite` (default) or `append` the files Bronze table

Output lands in `github_repo_files_bronze`, one row per file, keyed by
`repo_full_name`.

### 6. Day 2 - Run the graph traversal

Open `notebooks/day2_processing/github graph traversal.py` and run it. Configure
the widgets:
- `catalog` / `schema` — your Unity Catalog location
- `target_repos` — how many repos to discover (default: 500)
- `max_hops` — depth of traversal (default: 3)
- `fetch_mode` — `graphql` (recommended, ~2 API calls/repo) or `rest` (~6 calls/repo)

This starts from your watchlist repos (seed repos) and discovers related
repositories via shared committers and AI-powered dependency analysis.
Output lands in `github_api_staging` and `github_graph_edges`.

### 7. Sync graph data back to Lakebase (Synced Tables)

1. In your Databricks workspace, open the **Lakebase** tab for your instance.
2. Go to **Synced Tables** and click **Create synced table**.
3. Create synced tables for both:
   - `github_api_staging` — all raw repo data (files, contents, commits)
   - `github_graph_edges` — the relationship graph
4. Choose a sync mode (snapshot or continuous - snapshot is fine for this demo).
5. Confirm - Databricks creates **read-only** Postgres tables inside your Lakebase instance, kept in sync from the Delta tables.

> **Note:** Synced Tables are read-only in Postgres by design - the app
> never writes to these tables, only reads from them. Re-run the notebook
> and the synced tables update automatically (per the sync schedule you chose).

### 8. Configure environment variables (local dev)

```bash
cp .env.example .env
```

Paste your Lakebase URL as `LAKEBASE_URL`. For deployment, `app.yaml`
already pulls it from the `database/lakebase-url` secret automatically.

### 9. Install dependencies and run locally

```bash
pip install -r requirements.txt
python app.py
```

### 10. Deploy as a Databricks App

Same as any Databricks App - create a **Git folder** pointing at this repo,
create a **Custom App** in **Compute > Apps**, point it at the Git folder,
and click **Deploy**. See the Databricks Apps docs for the full UI walkthrough.

## Endpoints

- `GET /healthz` - health check
- `GET /login` - sign-in page (enter a username, no password)
- `POST /login` with `{"username": "ada"}` (or a form field) - validates the username as a safe
  Postgres schema fragment, then creates (idempotently) a dedicated `student_<username>` schema
  in Lakebase with empty `github_repos` and `github_files` tables, and starts a session
- `POST /logout` - clear the current session
- `GET /insights?language=Python&limit=25` - read Spark-processed, Synced-Table-backed insights (read-only)
- `GET /watchlist` - get the current user's watched repos with last known stats
- `POST /watchlist` with `{"full_name": "owner/repo"}` - add/update a repo on the current user's watchlist (one GitHub API call)

## Student sign-in and per-student Lakebase schemas

Signing in at `/login` does NOT check a password - it is a lightweight "pick your name" flow for a
classroom setting, similar to how FastAPI tutorials often show a simple form-based session login.
Enter a username, which must be lowercase letters/digits/underscores and start with a letter or
underscore (validated server-side against a strict regex so it can never be used to inject SQL -
Postgres identifiers like schema/table names cannot be parameterized as query values, so the
username is validated *before* being interpolated into DDL, and `psycopg2.sql.Identifier` quotes
it besides).

On successful sign-in, the app validates the username and starts a session. Students create
their own schema and tables as a separate exercise by running the SQL files in `sql/`:

```bash
sql/create_student_schema.sql   # CREATE SCHEMA IF NOT EXISTS student_<username>
sql/create_github_repos.sql     # CREATE TABLE + ALTER TABLE ... REPLICA IDENTITY FULL
sql/create_github_files.sql     # CREATE TABLE + ALTER TABLE ... REPLICA IDENTITY FULL
```

Because students connect as their own OAuth identity when running these, they own the tables
and can set REPLICA IDENTITY (owner-only DDL) without permission issues. The app UI shows a
warning banner if the schema/tables haven't been created yet.

## Demo narrative (suggested flow)

1. **Day 1 - Ingest/replicate**: Run `notebooks/day1_ingest/ingest_github_repos.py` (and optionally `ingest_github_files.py`) live, show the raw Bronze table(s).
2. **Day 2 - Graph traversal**: Run `notebooks/day2_processing/github graph traversal.py`, show the multi-hop discovery crawling from seed repos, building the relationship graph.
3. **Sync back**: Create Synced Tables in the Lakebase UI for `github_api_staging` and `github_graph_edges`, show them appear in Postgres.
4. **Surface**: Load the app, show graph-discovered repos and relationships, add repos to the personal watchlist live.
5. **Payoff**: "Heavy graph traversal and AI-powered discovery happened entirely in the lake - the app never touched Spark, it just reads synced Postgres tables."

## Graph Traversal & Repo Discovery

The notebook `notebooks/day2_processing/github graph traversal.py` implements a
multi-hop graph crawl that discovers related repositories starting from your
watchlist (seed repos). It builds a relationship graph using two discovery methods:

### Discovery Methods

| Method | Edge Type | How it works |
|--------|-----------|--------------|
| **Committer-based** | `committer`, `shared_committer` | Finds GitHub usernames from commit history, then fetches their other public repos. Connects repos that share contributors. |
| **AI dependency analysis** | `dependency`, `ai_suggested` | Reads dependency files (package.json, requirements.txt, etc.), sends them to `ai_query` (Llama 3.1 70B) which suggests specific `owner/repo` names, then validates they exist on GitHub. |

### Output Tables

| Table | Description |
|-------|-------------|
| `github_api_staging` | Raw data (files, dependency contents, commits) for all discovered repos |
| `github_graph_edges` | Relationship graph: who/what led to each repo's discovery |

### Graph Edges Schema

```
source          STRING    -- committer username or source repo (owner/repo)
target          STRING    -- discovered repo (owner/repo)
edge_type       STRING    -- "committer", "shared_committer", "dependency", "ai_suggested"
metadata        STRING    -- JSON with context (e.g. {"via_committer": "username", "source_repo": "org/repo"})
discovered_at   STRING    -- ISO timestamp of when the edge was recorded
```

### Syncing Graph Data to Lakebase

To make the graph traversal results queryable from the Flask app (or any Postgres client):

1. **Run the graph traversal notebook** — configure widgets:
   - `catalog` / `schema` — your Unity Catalog location
   - `target_repos` — how many repos to discover (default: 500)
   - `max_hops` — depth of traversal (default: 3)
   - `fetch_mode` — `graphql` (recommended, ~2 API calls/repo) or `rest` (~6 calls/repo)

2. **Create Synced Tables** in the Lakebase UI for:
   - `github_api_staging` — all raw repo data (files, contents, commits)
   - `github_graph_edges` — the relationship graph

3. **Query the graph** in Postgres:
   ```sql
   -- Find all repos connected to a specific repo
   SELECT target, edge_type, metadata
   FROM github_graph_edges
   WHERE source = 'facebook/react';

   -- Find repos discovered via shared committers
   SELECT source, target, metadata->>'via_committer' AS committer
   FROM github_graph_edges
   WHERE edge_type = 'shared_committer';

   -- Find AI-suggested repos from dependency analysis
   SELECT source, target
   FROM github_graph_edges
   WHERE edge_type = 'ai_suggested';
   ```

### Rate Limit Awareness

The traversal tracks GitHub API usage and stops early if approaching the
5,000 requests/hour limit. Budget allocation:
- **Per-repo fetch**: ~2 calls (GraphQL mode) or ~6 calls (REST mode)
- **Committer discovery**: 1 call per username (GraphQL, separate rate limit)
- **AI validation**: 1 call per suggested repo (capped at 50/hop)
- **Hard cap**: 4,000 requests per run (leaves headroom)

## Notes

- Lakebase auth uses a single `LAKEBASE_URL` secret pointing at a native Postgres role with a
  static, non-expiring password - no token refresh logic needed in `lakebase.py`.
- GitHub auth is optional; `github_client.py` and the ingestion notebook both fall back to
  unauthenticated requests if no token secret is configured.
- The `watchlist` table is the only table this app writes to directly - `github_api_staging` and
  `github_graph_edges` are owned by the Synced Table pipeline and must not be written to from the app.
