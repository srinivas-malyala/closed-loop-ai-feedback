"""
Databricks App: GitHub Repo Explorer

- Serves a small Flask API
- Reads/writes repos to the student's own Lakebase schema
  (student_<username>.github_repos) using a single GitHub API call per add
  (see github_client.py:get_repo)

Run locally:
    python app.py
Deploy as a Databricks App using app.yaml.
"""

import logging
import os
import re

import requests
from flask import Flask, jsonify, redirect, render_template, request, session, url_for
import lakebase
from github_client import GitHubClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("github-insights-app")

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY") or os.urandom(32)

# "owner/repo" shape check, e.g. "databricks/spark" or "sylph-ai/adal".
_REPO_RE = re.compile(r"^[\w.-]+/[\w.-]+$")

# Prefix applied to every student's Postgres schema name, e.g. username
# "ada" -> schema "student_ada". Namespacing this way avoids a raw username
# ever colliding with a system schema (public, pg_catalog, pg_toast, ...).
STUDENT_SCHEMA_PREFIX = "student_"

# Postgres unquoted identifiers: must start with a letter or underscore,
# followed by letters/digits/underscores, and fit within the 63-byte
# identifier length limit (minus our prefix). This is intentionally strict
# (lowercase only, no leading digit) so every valid username maps 1:1 to a
# safe, unquoted Postgres schema name - no chance of SQL injection via
# CREATE SCHEMA / CREATE TABLE, which can't be parameterized like data values.
_USERNAME_RE = re.compile(r"^[a-z_][a-z0-9_]{0,%d}$" % (63 - len(STUDENT_SCHEMA_PREFIX) - 1))


def _validate_username(username: str) -> str:
    """Normalize and validate a student username as a safe Postgres schema
    fragment. Raises ValueError with a student-facing message on failure."""
    if not isinstance(username, str):
        raise ValueError("Username is required.")
    username = username.strip().lower()
    if not username:
        raise ValueError("Username is required.")
    if not _USERNAME_RE.match(username):
        raise ValueError(
            "Username must be lowercase letters, digits, or underscores, "
            "start with a letter or underscore, and be short enough to fit "
            "a Postgres identifier (max %d chars)." % (63 - len(STUDENT_SCHEMA_PREFIX))
        )
    return username


def _schema_for(username: str) -> str:
    return f"{STUDENT_SCHEMA_PREFIX}{username}"








@app.route("/healthz")
def healthz():
    return jsonify({"status": "ok"})


@app.errorhandler(Exception)
def handle_exception(err):
    """Ensure all unhandled errors return JSON (not an HTML error page),
    so the frontend's resp.json() call never chokes on HTML."""
    logger.exception("Unhandled exception while processing request")
    status_code = getattr(err, "code", 500)
    if not isinstance(status_code, int):
        status_code = 500
    return jsonify({"error": str(err)}), status_code


def _current_username() -> str | None:
    """Return the signed-in student's validated username, or None."""
    return session.get("username")


@app.route("/")
def index():
    """Simple UI to add repos. Requires signing in with a username first."""
    username = _current_username()
    if not username:
        return redirect(url_for("login"))
    return render_template("index.html", username=username, schema=_schema_for(username))


@app.route("/login", methods=["GET"])
def login():
    """Sign-in page: student enters a username, no password (this mirrors a
    classroom "pick your name" flow, not a real auth system)."""
    if _current_username():
        return redirect(url_for("index"))
    return render_template("login.html")


@app.route("/login", methods=["POST"])
def do_login():
    """
    Validate the submitted username as a safe Postgres schema fragment
    and start a session. Students create their own schema/tables separately
    using the SQL files in sql/.
    """
    if request.is_json:
        raw_username = request.json.get("username", "")
    else:
        raw_username = request.form.get("username", "")

    try:
        username = _validate_username(raw_username)
    except ValueError as exc:
        if request.is_json:
            return jsonify({"error": str(exc)}), 400
        return render_template("login.html", error=str(exc)), 400

    session["username"] = username

    if request.is_json:
        return jsonify({"username": username, "schema": _schema_for(username)})
    return redirect(url_for("index"))


@app.route("/logout", methods=["POST"])
def logout():
    session.pop("username", None)
    if request.is_json:
        return jsonify({"status": "ok"})
    return redirect(url_for("login"))


@app.route("/schema/status")
def schema_status():
    """Check if the student's schema and tables exist in Lakebase."""
    username = _current_username()
    if not username:
        return jsonify({"error": "Not signed in"}), 401

    schema = _schema_for(username)
    rows = lakebase.run_query(
        """
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = %s AND table_name IN ('github_repos', 'github_files')
        """,
        (schema,),
    )
    existing_tables = [r["table_name"] for r in rows]

    # Check if schema itself exists
    schema_rows = lakebase.run_query(
        "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
        (schema,),
    )

    return jsonify({
        "schema": schema,
        "schema_exists": len(schema_rows) > 0,
        "github_repos_exists": "github_repos" in existing_tables,
        "github_files_exists": "github_files" in existing_tables,
    })


@app.route("/repos", methods=["GET"])
def get_repos():
    """Return the repos in the student's github_repos table."""
    username = _current_username()
    if not username:
        return jsonify({"error": "Not signed in"}), 401

    schema = _schema_for(username)
    try:
        rows = lakebase.run_query(
            f"SELECT full_name, language, stargazers_count, open_issues_count, "
            f"forks_count, is_favorite, ingested_at "
            f"FROM {schema}.github_repos ORDER BY full_name ASC"
        )
    except Exception as exc:
        if "does not exist" in str(exc):
            return jsonify({"error": "Table not found. Please create your schema and tables first (see sql/ folder)."}), 404
        raise

    return jsonify(rows)


@app.route("/repos", methods=["POST"])
def add_repo():
    """
    Fetch the latest stats for a single "owner/repo" from GitHub using
    exactly ONE API call (see GitHubClient.get_repo), then insert/update
    it in the student's github_repos table.
    """
    username = _current_username()
    if not username:
        return jsonify({"error": "Not signed in"}), 401

    if request.is_json:
        full_name = request.json.get("full_name", "")
    else:
        full_name = request.form.get("full_name", "")

    full_name = full_name.strip() if isinstance(full_name, str) else ""

    if not full_name or not _REPO_RE.match(full_name):
        return jsonify({"error": f"Invalid repo name (expected owner/repo): {full_name!r}"}), 400

    client = GitHubClient()
    try:
        data = client.get_repo(full_name)
    except requests.HTTPError:
        return jsonify({"error": f"Unknown repo: {full_name}"}), 400

    schema = _schema_for(username)

    try:
        lakebase.run_write(
            f"""
            INSERT INTO {schema}.github_repos
                (id, full_name, language, stargazers_count, open_issues_count, forks_count, payload, ingested_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (full_name) DO UPDATE
                SET stargazers_count = EXCLUDED.stargazers_count,
                    open_issues_count = EXCLUDED.open_issues_count,
                    forks_count = EXCLUDED.forks_count,
                    language = EXCLUDED.language,
                    payload = EXCLUDED.payload,
                    ingested_at = EXCLUDED.ingested_at
            """,
            (
                data.get("id"),
                full_name,
                data.get("language"),
                data.get("stargazers_count"),
                data.get("open_issues_count"),
                data.get("forks_count"),
                __import__("json").dumps(data),
            ),
        )
    except Exception as exc:
        if "does not exist" in str(exc):
            return jsonify({"error": "Table not found. Please create your schema and tables first (see sql/ folder)."}), 404
        raise

    return jsonify({
        "full_name": full_name,
        "language": data.get("language"),
        "stargazers_count": data.get("stargazers_count"),
        "open_issues_count": data.get("open_issues_count"),
        "forks_count": data.get("forks_count"),
    })


@app.route("/repos/favorite", methods=["POST"])
def toggle_favorite():
    """
    Mark or unmark a repo as a favorite.
    Expects {"full_name": "owner/repo", "is_favorite": true/false}.
    """
    username = _current_username()
    if not username:
        return jsonify({"error": "Not signed in"}), 401

    if request.is_json:
        full_name = request.json.get("full_name", "")
        is_favorite = request.json.get("is_favorite", True)
    else:
        full_name = request.form.get("full_name", "")
        is_favorite = request.form.get("is_favorite", "true").lower() in ("true", "1", "yes")

    full_name = full_name.strip() if isinstance(full_name, str) else ""

    if not full_name or not _REPO_RE.match(full_name):
        return jsonify({"error": f"Invalid repo name (expected owner/repo): {full_name!r}"}), 400

    schema = _schema_for(username)

    try:
        affected = lakebase.run_write(
            f"UPDATE {schema}.github_repos SET is_favorite = %s WHERE full_name = %s",
            (bool(is_favorite), full_name),
        )
    except Exception as exc:
        if "does not exist" in str(exc):
            return jsonify({"error": "Table not found. Please create your schema and tables first (see sql/ folder)."}), 404
        raise

    if affected == 0:
        return jsonify({"error": f"Repo not found: {full_name!r}"}), 404

    return jsonify({"full_name": full_name, "is_favorite": bool(is_favorite)})


@app.route("/graph/edges", methods=["GET"])
def get_graph_edges():
    """Return graph edges from the synced Unity Catalog table (uc_github_graph_edges).
    
    Optional query params:
      - edge_type: filter by type (committer, shared_committer, dependency, ai_suggested)
      - source: filter by source repo or committer
      - target: filter by target repo
      - limit: max results (default 200)
    """
    username = _current_username()
    if not username:
        return jsonify({"error": "Not signed in"}), 401

    edge_type = request.args.get("edge_type")
    source = request.args.get("source")
    target = request.args.get("target")
    limit = min(int(request.args.get("limit", 200)), 1000)

    conditions = []
    params = []

    if edge_type:
        conditions.append("edge_type = %s")
        params.append(edge_type)
    if source:
        conditions.append("source = %s")
        params.append(source)
    if target:
        conditions.append("target = %s")
        params.append(target)

    where_clause = (" WHERE " + " AND ".join(conditions)) if conditions else ""
    params.append(limit)

    try:
        rows = lakebase.run_query(
            f"SELECT source, target, edge_type, metadata, discovered_at "
            f"FROM {username}.uc_github_graph_edges"
            f"{where_clause} ORDER BY discovered_at DESC LIMIT %s",
            tuple(params),
        )
    except Exception as exc:
        if "does not exist" in str(exc):
            return jsonify({"error": "Graph edges table not found. Sync the github_graph_edges Delta table to Lakebase first."}), 404
        raise

    return jsonify(rows)


@app.route("/graph/stats", methods=["GET"])
def get_graph_stats():
    """Return summary stats about the graph edges."""
    username = _current_username()
    if not username:
        return jsonify({"error": "Not signed in"}), 401

    try:
        rows = lakebase.run_query(
            f"SELECT edge_type, COUNT(*) as count "
            f"FROM {username}.uc_github_graph_edges "
            f"GROUP BY edge_type ORDER BY count DESC"
        )
    except Exception as exc:
        if "does not exist" in str(exc):
            return jsonify({"error": "Graph edges table not found."}), 404
        raise

    total = sum(r["count"] for r in rows)
    return jsonify({"total_edges": total, "by_type": rows})


@app.route("/repos", methods=["DELETE"])
def remove_repo():
    """
    Remove a repo from the student's github_repos table.
    Expects {"full_name": "owner/repo"}.
    """
    username = _current_username()
    if not username:
        return jsonify({"error": "Not signed in"}), 401

    if request.is_json:
        full_name = request.json.get("full_name", "")
    else:
        full_name = request.form.get("full_name", "")

    full_name = full_name.strip() if isinstance(full_name, str) else ""

    if not full_name or not _REPO_RE.match(full_name):
        return jsonify({"error": f"Invalid repo name (expected owner/repo): {full_name!r}"}), 400

    schema = _schema_for(username)

    try:
        affected = lakebase.run_write(
            f"DELETE FROM {schema}.github_repos WHERE full_name = %s",
            (full_name,),
        )
    except Exception as exc:
        if "does not exist" in str(exc):
            return jsonify({"error": "Table not found. Please create your schema and tables first (see sql/ folder)."}), 404
        raise

    if affected == 0:
        return jsonify({"error": f"Repo not found: {full_name!r}"}), 404

    return jsonify({"full_name": full_name, "removed": True})


if __name__ == '__main__':
    host = os.getenv('FLASK_RUN_HOST', '0.0.0.0')
    port = int(os.getenv('FLASK_RUN_PORT', 8000))
    app.run(debug=True, host=host, port=port)
    print(f"Flask app running on http://{host}:{port}")
