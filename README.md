# LTAP Lab: GitHub Repo Insights via Spark + Lakebase

A Databricks App that demonstrates the full LTAP round trip, split across two
lab days:

```
Day 1: CDC / replication into Delta       Day 2: heavy Spark processing
------------------------------------      ------------------------------------
GitHub API --(ingest notebooks)-->        Bronze Delta --(Spark aggregation/
  Bronze Delta (raw repos + files)          ranking)--> Gold Delta (per-repo/
                                             per-language insights)
                                                        |
                                             (Databricks Synced Table, reverse ETL)
                                                        v
                                             Lakebase Postgres (read-only insights table)
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

**Day 2** is where the heavy lifting happens: real Spark aggregation/ranking
work on top of the Bronze tables (`notebooks/day2_processing/`), landing a
Gold Delta table that gets synced back to Lakebase.

The point of the demo: **you should never run heavy aggregation/ranking
inside your operational Postgres database.** Do it in Spark on Delta, then
sync the *result* back into Lakebase as a fast, read-only table your app can
query with simple SQL - no Spark cluster needed at request time.

## Files

- `app.py` - Flask app: `/healthz`, `/login` (GET/POST, username sign-in + per-student Lakebase schema), `/logout`, `/insights` (GET, read-only synced table), `/watchlist` (GET/POST)
- `lakebase.py` - Lakebase connection helper (single `LAKEBASE_URL`, psycopg2 + SQLAlchemy)
- `github_client.py` - GitHub REST API client (paginated search + single-repo lookup)
- `setup_secrets.py` - One-time script to store the Lakebase URL and an optional GitHub PAT
- `notebooks/day1_ingest/ingest_github_repos.py` - Day 1. Pulls repos from GitHub's Search API into a Bronze Delta table
- `notebooks/day1_ingest/ingest_github_files.py` - Day 1. Walks every repo's full file tree (Git Trees API, limited concurrency) into a Bronze Delta table
- `notebooks/day2_processing/process_repo_insights.py` - Day 2. The "heavy" Spark stage: per-language rollups + per-language ranking, writes a Gold Delta table
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

### 6. Day 2 - Run the "heavy" Spark processing stage

Open `notebooks/day2_processing/process_repo_insights.py` and run it against the same
catalog/schema. This reads `github_repos_bronze` and does real Spark work:
- Groups repos by language and computes rollups (repo count, total/avg stars, total open issues, total forks)
- Uses a window function to rank repos within each language by star count
- Computes a normalized `popularity_score` (0-1) relative to the top repo per language

Output lands in `github_repo_insights_gold`.

### 7. Sync processed data back to Lakebase (Synced Table)

1. In your Databricks workspace, open the **Lakebase** tab for your instance.
2. Go to **Synced Tables** and click **Create synced table** (sometimes under **Lakebase** > **Sync data** > **New synced table**).
3. Select your Unity Catalog **catalog.schema.github_repo_insights_gold** as the source table.
4. Choose a sync mode (snapshot or continuous - snapshot is fine for this demo since the Gold table is only refreshed when you re-run the notebook).
5. Confirm - Databricks creates a **read-only** Postgres table named `github_repo_insights_gold` (matching `INSIGHTS_TABLE_NAME` in `app.yaml`) inside your Lakebase instance, kept in sync from the Delta table.

> **Note:** Synced Tables are read-only in Postgres by design - this app
> never writes to `github_repo_insights_gold`, only reads from it via `/insights`.
> Re-run the notebooks and the synced table updates automatically (per the
> sync schedule you chose); no app changes needed.

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

On successful sign-in, the app runs (idempotently, safe to repeat):

```sql
CREATE SCHEMA IF NOT EXISTS student_<username>;
CREATE TABLE IF NOT EXISTS student_<username>.github_repos (...);
CREATE TABLE IF NOT EXISTS student_<username>.github_files (...);
```

Each student gets their own empty `github_repos` / `github_files` tables (matching the shape of the
`github_repos_bronze` / `github_repo_files_bronze` Delta tables) to practice loading and querying
data in Lakebase under their own schema, isolated from every other student.

## Demo narrative (suggested flow)

1. **Day 1 - Ingest/replicate**: Run `notebooks/day1_ingest/ingest_github_repos.py` (and optionally `ingest_github_files.py`) live, show the raw Bronze table(s).
2. **Day 2 - Process**: Run `notebooks/day2_processing/process_repo_insights.py`, show the Spark job plan/DAG and the resulting Gold table with rankings.
3. **Sync back**: Create the Synced Table in the Lakebase UI, show the new read-only table appear in Postgres (`psql` or SQL editor).
4. **Surface**: Load the app, show `/insights` serving that data instantly, and add a repo to the personal watchlist live.
5. **Payoff**: "Heavy Spark processing happened entirely in the lake - the app never touched Spark, it just reads a synced Postgres table."

## Notes

- Lakebase auth uses a single `LAKEBASE_URL` secret pointing at a native Postgres role with a
  static, non-expiring password - no token refresh logic needed in `lakebase.py`.
- GitHub auth is optional; `github_client.py` and the ingestion notebook both fall back to
  unauthenticated requests if no token secret is configured.
- The `watchlist` table is the only table this app writes to directly - `github_repo_insights_gold`
  is owned by the Synced Table pipeline and must not be written to from the app.
