# Closed-Loop AI Feedback for GitHub Repository Discovery

RepoSignal is a Databricks reference implementation of a closed-loop Lakehouse
application. Users manage a GitHub repository watchlist in a Databricks App,
Spark expands those seeds into a dependency graph with `ai_query`, and human
review feeds back into the next AI run so rejected suggestions are not reused.

The project demonstrates a practical LTAP pattern:

- **Lakebase Postgres** serves low-latency, per-user application state.
- **Lakehouse Sync** carries operational changes into Unity Catalog Delta
  tables for analytical and AI processing.
- **Spark and Databricks AI Functions** perform API-heavy graph discovery away
  from the application request path.
- **Lakebase synced tables** return the processed graph to the application as a
  read-optimized Postgres model.
- **Human feedback** invalidates stale AI cache entries and changes subsequent
  model prompts.

## Architecture

```mermaid
flowchart LR
    user[Reviewer] --> ui[RepoSignal UI]
    ui --> api[Flask API<br/>Databricks App]

    api -->|Add or update repo| github[GitHub API]
    github -->|Repository metadata| api
    api -->|Watchlist and feedback writes| operational[(Lakebase<br/>student_* schemas)]

    operational -->|Lakehouse Sync / CDC| history[(Unity Catalog Delta<br/>watchlist + feedback history)]
    history --> traversal[Spark graph traversal]
    github -->|Trees, manifests, commits| traversal

    traversal -->|Cache miss| ai[Databricks ai_query]
    ai -->|Package to repository mappings| traversal
    traversal <--> cache[(Delta<br/>ai_query_cache)]
    traversal --> graph[(Delta<br/>github_api_staging + github_graph_edges)]

    graph -->|Lakebase synced table| readmodel[(Lakebase read model<br/>uc_github_graph_edges)]
    readmodel -->|Graph and statistics| api
    api --> ui

    ui -. Mark suggestion bad .-> operational
    history -. Latest bad feedback .-> traversal
```

The architecture deliberately separates transactional serving from expensive
processing. The web app never runs Spark or an LLM in an HTTP request. It reads
and writes Postgres; the Lakehouse handles CDC, graph traversal, AI inference,
caching, validation, and durable analytical output.

## Forward data flow

1. A user signs in with a classroom username. The app maps it to the isolated
   Lakebase schema `student_<username>`.
2. The user adds `owner/repository` to the watchlist. The Flask API makes one
   GitHub repository call and upserts the latest metadata into
   `student_<username>.github_repos`.
3. Lakehouse Sync replicates the Postgres change stream into the Unity Catalog
   history table `<catalog>.<schema>.lb_github_repos_history`.
4. The graph traversal notebook selects the latest non-deleted watchlist state
   and uses those repositories as breadth-first-search seeds.
5. Spark workers fetch repository trees, dependency-file contents, and commit
   data through the GitHub GraphQL or REST APIs. Results are materialized in
   `<catalog>.<schema>.github_api_staging`.
6. For each source repository, the traversal checks
   `<catalog>.<schema>.ai_query_cache`:

   - A cache hit reuses persisted package-to-repository mappings.
   - A cache miss sends selected manifest contents to
     `ai_query('databricks-meta-llama-3-3-70b-instruct', ...)`.

7. Candidate repositories are deduplicated and validated against GitHub before
   becoming traversal seeds for the next hop.
8. Relationships are merged into
   `<catalog>.<schema>.github_graph_edges`, keyed by source and target.
9. A Lakebase synced table publishes that Delta result as
   `<username>.uc_github_graph_edges`.
10. The Flask API reads the synced table to serve graph rows and aggregate
    statistics to the RepoSignal UI.

## Human-feedback flow

The feedback path turns the pipeline from one-way inference into a closed loop:

1. The UI exposes **Mark bad** on `ai_suggested` graph edges.
2. `POST /graph/edges/feedback` validates the source repository, package,
   suggested repository, feedback value, and optional reason.
3. The API upserts the current review into
   `student_<username>.ai_suggestion_feedback`. The unique mapping key is
   `(source_repo, package_name, suggested_repo)`, so repeated reviews update the
   existing state.
4. `REPLICA IDENTITY FULL` allows Lakehouse Sync to preserve complete update and
   delete row images in
   `<catalog>.<schema>.lb_ai_suggestion_feedback_history`.
5. At the start of AI discovery, `load_blocked_ai_suggestions()` reconstructs
   the latest non-deleted state for every mapping and selects the latest `bad`
   reviews.
6. If a rejected source/target pair exists in `ai_query_cache`, a Delta
   `MERGE ... WHEN MATCHED DELETE` removes **all cache rows for that source
   repository**. Source-level invalidation forces a coherent replacement
   analysis rather than retaining a partially stale mapping set.
7. Invalidated sources are processed first. Their cache is bypassed and the
   prompt includes explicit `Do NOT suggest` rules for rejected repositories.
8. The notebook filters blocked or malformed model output, stores allowed
   replacements in the Delta cache, validates them on GitHub, and merges the
   new relationships into the graph.
9. The synced graph read model refreshes and the application can display the
   replacement results alongside the Lakebase feedback history.

If physical cache deletion fails, the traversal still applies the in-memory
blocked set and prompt exclusions for that run. This prevents a known bad
repository from being accepted merely because cleanup failed.

## Main implementation

### Databricks App and UI

`app.py` is a Flask application deployed as `week1-homework`. It provides:

- classroom-style username sessions and strict schema-name validation;
- watchlist add, remove, favorite, and list operations;
- graph reads and relationship statistics from a synced Lakebase table;
- feedback upsert and feedback-history endpoints; and
- JSON error handling so the browser always receives a consistent response.

`templates/index.html`, `static/app.js`, and `static/styles.css` implement the
RepoSignal experience: overview metrics, watchlist actions, graph filters,
repository-focused navigation, a feedback modal, feedback history, and explicit
loading, empty, success, and error states.

### Lakebase operational layer

The app writes only per-user operational state:

| Object | Access | Purpose |
|---|---|---|
| `student_<username>.github_repos` | Read/write | Watchlist and latest GitHub repository metadata |
| `student_<username>.github_files` | Read/write extension | File inventory used by the broader lab |
| `student_<username>.ai_suggestion_feedback` | Read/write | Current human-review state for AI mappings |
| `<username>.uc_github_graph_edges` | Read-only | Synced graph result served to the UI |

`lakebase.py` retrieves the Lakebase connection URL from the Databricks secret
`database/lakebase-url` through `WorkspaceClient`, then uses psycopg2 for
parameterized reads and writes.

### Lakehouse and AI processing

| Unity Catalog Delta table | Producer | Purpose |
|---|---|---|
| `lb_github_repos_history` | Lakehouse Sync | CDC history used to reconstruct current watchlist seeds |
| `lb_ai_suggestion_feedback_history` | Lakehouse Sync | CDC history used to reconstruct current rejected mappings |
| `github_repos_scd` | `generate_repo_scd.py` | Optional incremental SCD representation of watchlist state |
| `github_api_staging` | Graph traversal | Files, dependency contents, and commits fetched from GitHub |
| `ai_query_cache` | Graph traversal | Persisted AI package-to-repository mappings |
| `github_graph_edges` | Graph traversal | Durable source/target relationship graph |

The graph notebook supports configurable catalog, schema, target repository
count, maximum hops, and fetch mode. GraphQL mode uses approximately two GitHub
calls per repository; REST mode uses approximately five to seven. A run-level
request budget stops traversal before exhausting the authenticated GitHub API
allowance.

The standalone `dependency_parsers.py` module contains deterministic parsers for
common package manifests and is covered by unit tests. The current graph
notebook sends selected manifest contents directly to `ai_query`; the parser
module is available for a future deterministic pre-processing stage.

## Repository structure

```text
.
├── app.py                         # Flask routes and application logic
├── lakebase.py                    # Lakebase secret resolution and SQL helpers
├── github_client.py               # GitHub REST/GraphQL client helpers
├── dependency_parsers.py          # Deterministic manifest parsers
├── templates/                     # Login and RepoSignal HTML
├── static/                        # Browser behavior and styling
├── sql/                           # Per-user Lakebase table DDL
├── notebooks/
│   ├── day1_ingest/
│   │   └── generate_repo_scd.py   # Incremental CDC-to-SCD processing
│   └── day2_processing/
│       └── github graph traversal.py
├── app.yaml                       # Databricks App runtime configuration
├── databricks.yml                 # App and Lakeflow Job bundle metadata
├── deployment/                    # App-only metadata and Git deploy requests
├── DEPLOYMENT_VERIFICATION.md     # Deployment provenance checklist
└── test_*.py                      # Feedback API and parser tests
```

## Run the architecture end to end

### Prerequisites

- A Databricks workspace with Unity Catalog and compute capable of running the
  Spark notebook and `ai_query`.
- A Lakebase Postgres database reachable by the Databricks App.
- A Databricks secret named `database/lakebase-url` containing the Postgres
  connection URL used by the current implementation.
- An optional GitHub token in `github/token` for the notebook. Without it,
  GitHub applies the unauthenticated rate limit.
- A selected Databricks CLI profile. Always pass it explicitly as
  `--profile <PROFILE>`.

### 1. Create the per-user Lakebase schema

Replace `<username>` in the SQL files before execution. The feedback DDL is
currently checked in for `student_sri`; change that schema when using another
username.

```text
sql/create_student_schema.sql
sql/create_github_repos.sql
sql/create_github_files.sql
sql/create_ai_suggestion_feedback.sql
```

The commits and file-contents DDL files are optional extensions. Keep
`REPLICA IDENTITY FULL` on tables whose updates or deletes must be consumed from
CDC history.

### 2. Configure both synchronization directions

In the Databricks UI, configure Lakehouse Sync from Lakebase to Unity Catalog
for at least:

| Lakebase source | Expected Unity Catalog target |
|---|---|
| `student_<username>.github_repos` | `<catalog>.<schema>.lb_github_repos_history` |
| `student_<username>.ai_suggestion_feedback` | `<catalog>.<schema>.lb_ai_suggestion_feedback_history` |

Then configure a Lakebase synced table from
`<catalog>.<schema>.github_graph_edges` to
`<username>.uc_github_graph_edges`. The destination name and schema must match
`app.py` unless the application mapping is changed.

### 3. Add seed repositories

Start the app, sign in with the configured username, and add one or more
`owner/repository` values. Confirm the rows reach `lb_github_repos_history`
before running graph discovery.

For local execution, install dependencies and use an authenticated Databricks
SDK profile that can read the configured secrets:

```bash
python -m pip install -r requirements.txt
DATABRICKS_CONFIG_PROFILE=<PROFILE> python app.py
```

### 4. Run graph discovery

Run `notebooks/day2_processing/github graph traversal.py` and set:

| Widget | Meaning | Default |
|---|---|---|
| `catalog` | Unity Catalog containing CDC and graph tables | `bootcamp_students` |
| `schema` | User-specific Unity Catalog schema | `sri` |
| `target_repos` | Maximum repositories to discover | `500` |
| `max_hops` | Maximum breadth-first traversal depth | `3` |
| `fetch_mode` | `graphql` or `rest` | `graphql` |

Verify that `github_api_staging`, `ai_query_cache`, and `github_graph_edges`
are created or updated.

### 5. Close the feedback loop

1. Refresh the graph synced table and open RepoSignal.
2. Mark an `ai_suggested` edge as bad and optionally provide a reason.
3. Confirm the review appears in the UI feedback history.
4. Confirm CDC delivers the row to `lb_ai_suggestion_feedback_history`.
5. Run the graph traversal again.
6. Check notebook output for cache invalidation and feedback-aware re-querying.
7. Refresh the synced graph and inspect the replacement suggestion.

## API surface

| Method | Route | Purpose |
|---|---|---|
| `GET` | `/healthz` | App health check |
| `GET`, `POST` | `/login` | Classroom username session |
| `POST` | `/logout` | Clear the current session |
| `GET` | `/schema/status` | Check required per-user tables |
| `GET`, `POST`, `DELETE` | `/repos` | List, add/update, or remove watched repositories |
| `POST` | `/repos/favorite` | Update favorite state |
| `GET` | `/graph/edges` | Query graph edges with optional filters |
| `GET` | `/graph/stats` | Count graph edges by relationship type |
| `GET`, `POST` | `/graph/edges/feedback` | Read or upsert AI-suggestion feedback |

## Tests

```bash
python -m pytest -q
```

The test suite covers deterministic dependency parsing, feedback validation,
feedback upserts, missing-table behavior, and feedback-history rendering.

## Deployment

The Databricks App is configured to build from the independent repository:

```text
https://github.com/srinivas-malyala/closed-loop-ai-feedback
```

Use the app-specific requests so deployment does not create or update the
separate Lakeflow Job also declared in `databricks.yml`:

```bash
databricks bundle validate --strict -t prod --profile <PROFILE>

databricks apps create-update week1-homework \
  --profile <PROFILE> \
  --json @deployment/app-metadata-update.json

databricks apps deploy week1-homework \
  --profile <PROFILE> \
  --json @deployment/git-deployment.json

databricks apps get week1-homework --profile <PROFILE> -o json
```

After deployment, verify that the app is running and that
`active_deployment.git_source.resolved_commit` matches `origin/main`. See
`DEPLOYMENT_VERIFICATION.md` for the complete Git URL, commit SHA, resource,
health, and functional checklist.

## Design choices and current boundaries

- **Operational and analytical responsibilities are separate.** Lakebase
  serves application interactions; Spark and Delta own graph/AI processing.
- **AI results are materialized.** Cache hits avoid repeated LLM inference,
  reducing latency and token cost.
- **Invalidation is intentionally source-wide.** One rejected mapping causes a
  complete re-analysis for that source repository.
- **Model output is not trusted directly.** Suggestions must have repository
  shape, must not be blocked, and must resolve through the GitHub API.
- **Synced graph tables are read-only.** The app does not mutate Spark-owned
  outputs in Postgres.
- **The login is for a lab/demo.** It is not production authentication or
  authorization.
- **Rejected historical edges are not deleted by the current graph merge.**
  Feedback prevents reuse and drives replacement inference, while the existing
  source/target edge remains until a separate cleanup policy is implemented.
- **The current Lakebase connection uses a stored native-password URL.** A
  production implementation should prefer managed Lakebase app resources and
  OAuth-based connectivity.

## Core idea

This repository is not primarily a two-day lab. It is an implementation of a
repeatable closed-loop pattern:

> Capture operational intent in Lakebase, process it at scale in the Lakehouse,
> serve the result back through a low-latency read model, and use human feedback
> to invalidate stale AI state and improve the next computation.
