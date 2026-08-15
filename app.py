"""
Databricks App: GitHub Repo Watchlist

- Serves a small Flask API
- Reads/writes a personal "watchlist" of GitHub repos in Lakebase (Databricks-
  managed Postgres) via lakebase.py, using a single GitHub API call per add
  (see github_client.py:get_repo)

Run locally:
    python app.py
Deploy as a Databricks App using app.yaml.
"""

import logging
import os
import re

import requests
from databricks.sdk import WorkspaceClient
from flask import Flask, jsonify, redirect, render_template, request, session, url_for
from psycopg2 import sql

import lakebase
from github_client import GitHubClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("github-insights-app")

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY") or os.urandom(32)
_w = WorkspaceClient()

WATCHLIST_TABLE_NAME = os.environ.get("WATCHLIST_TABLE_NAME", "watchlist")

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


def ensure_student_schema(username: str, email: str):
    """
    Create (if missing) a dedicated Postgres schema for this student, plus
    their own `github_repos` and `github_files` tables inside it - mirroring
    the shape of the Delta Bronze tables (github_repos_bronze /
    github_repo_files_bronze) so students can later copy/compare data
    between the lake and their personal Lakebase schema.

    Schema/table identifiers come from `sql.Identifier`, which quotes and
    escapes them for safe interpolation - identifiers can't be passed as
    query parameters (%s) the way values can, so this plus the username
    validation above are the two layers protecting against SQL injection.

    Ownership of the schema/tables is then reassigned from the app's shared
    connection role to a Postgres role matching the student's email (these
    per-user roles already exist in this Lakebase instance). Only the owner
    of a table can run privileged DDL like `ALTER TABLE ... REPLICA
    IDENTITY ...`, so without this, students would be stuck unable to set
    replica identity on their own tables.
    """
    schema = _schema_for(username)
    schema_ident = sql.Identifier(schema)
    owner_ident = sql.Identifier(email)

    with lakebase.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(schema_ident))
            cur.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {}.github_repos (
                        id BIGINT,
                        full_name TEXT NOT NULL,
                        language TEXT,
                        stargazers_count INTEGER,
                        open_issues_count INTEGER,
                        forks_count INTEGER,
                        payload JSONB,
                        ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        PRIMARY KEY (full_name)
                    )
                    """
                ).format(schema_ident)
            )
            cur.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {}.github_files (
                        repo_full_name TEXT NOT NULL,
                        path TEXT NOT NULL,
                        type TEXT,
                        sha TEXT,
                        size BIGINT,
                        mode TEXT,
                        tree_truncated BOOLEAN,
                        ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        PRIMARY KEY (repo_full_name, path)
                    )
                    """
                ).format(schema_ident)
            )
            # Reassigning ownership requires the connecting role to be a
            # member of the target role (or a superuser) - grant it first,
            # idempotently, then hand over ownership of the schema and both
            # tables to the student's own Postgres role.
            cur.execute(sql.SQL("GRANT {} TO CURRENT_USER").format(owner_ident))
            cur.execute(sql.SQL("ALTER SCHEMA {} OWNER TO {}").format(schema_ident, owner_ident))
            cur.execute(
                sql.SQL("ALTER TABLE {}.github_repos OWNER TO {}").format(schema_ident, owner_ident)
            )
            cur.execute(
                sql.SQL("ALTER TABLE {}.github_files OWNER TO {}").format(schema_ident, owner_ident)
            )
            conn.commit()


def ensure_watchlist_table():
    """Create the watchlist table in Lakebase if it doesn't exist yet."""
    lakebase.run_write(
        f"""
        CREATE TABLE IF NOT EXISTS {WATCHLIST_TABLE_NAME} (
            full_name TEXT NOT NULL,
            email TEXT NOT NULL,
            stars INTEGER,
            open_issues INTEGER,
            is_favorite BOOLEAN NOT NULL DEFAULT FALSE,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (full_name, email)
        )
        """
    )
    # Add is_favorite column for existing tables that predate this change.
    lakebase.run_write(
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = '{WATCHLIST_TABLE_NAME}' AND column_name = 'is_favorite'
            ) THEN
                ALTER TABLE {WATCHLIST_TABLE_NAME} ADD COLUMN is_favorite BOOLEAN NOT NULL DEFAULT FALSE;
            END IF;
        END $$;
        """
    )


def _current_user_email() -> str:
    """
    Resolve the current user's email so the watchlist can be personalized.

    Databricks Apps inject the logged-in user's identity via the
    X-Forwarded-Email header on every request. Fall back to the Databricks
    SDK's current_user API for local development where that header isn't set.
    """
    header_email = request.headers.get("X-Forwarded-Email")
    if header_email:
        return header_email
    return _w.current_user.me().user_name


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
    """Simple UI to add repos to a personal watchlist. Requires signing in
    with a username first, which provisions that student's own Lakebase
    schema/tables."""
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
    Validate the submitted username as a safe Postgres schema fragment,
    then provision (idempotently) a dedicated Lakebase schema for this
    student containing empty `github_repos` and `github_files` tables.
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

    ensure_student_schema(username, _current_user_email())
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


@app.route("/watchlist", methods=["GET"])
def get_watchlist():
    """Return the current user's watched repos, with their last known stats."""
    ensure_watchlist_table()
    email = _current_user_email()
    rows = lakebase.run_query(
        f"SELECT full_name, email, stars, open_issues, is_favorite, updated_at FROM {WATCHLIST_TABLE_NAME} "
        f"WHERE email = %s ORDER BY full_name ASC",
        (email,),
    )
    return jsonify(rows)


@app.route("/watchlist", methods=["POST"])
def add_to_watchlist():
    """
    Fetch the latest stats for a single "owner/repo" from GitHub using
    exactly ONE API call (see GitHubClient.get_repo), then add/update that
    repo on the current user's watchlist in Lakebase.
    """
    ensure_watchlist_table()

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
        # GitHub returns a 404 for repos it doesn't recognize (or private
        # repos the token can't see).
        return jsonify({"error": f"Unknown repo: {full_name}"}), 400

    stars = data.get("stargazers_count")
    open_issues = data.get("open_issues_count")

    email = _current_user_email()

    lakebase.run_write(
        f"""
        INSERT INTO {WATCHLIST_TABLE_NAME} (full_name, email, stars, open_issues, updated_at)
        VALUES (%s, %s, %s, %s, now())
        ON CONFLICT (full_name, email) DO UPDATE
            SET stars = EXCLUDED.stars,
                open_issues = EXCLUDED.open_issues,
                updated_at = EXCLUDED.updated_at
        """,
        (full_name, email, stars, open_issues),
    )

    return jsonify({"full_name": full_name, "email": email, "stars": stars, "open_issues": open_issues})


@app.route("/watchlist/favorite", methods=["POST"])
def toggle_favorite():
    """
    Mark or unmark a repo as a favorite on the current user's watchlist.
    Expects {"full_name": "owner/repo", "is_favorite": true/false}.
    """
    ensure_watchlist_table()

    if request.is_json:
        full_name = request.json.get("full_name", "")
        is_favorite = request.json.get("is_favorite", True)
    else:
        full_name = request.form.get("full_name", "")
        is_favorite = request.form.get("is_favorite", "true").lower() in ("true", "1", "yes")

    full_name = full_name.strip() if isinstance(full_name, str) else ""

    if not full_name or not _REPO_RE.match(full_name):
        return jsonify({"error": f"Invalid repo name (expected owner/repo): {full_name!r}"}), 400

    email = _current_user_email()

    affected = lakebase.run_write(
        f"""
        UPDATE {WATCHLIST_TABLE_NAME}
        SET is_favorite = %s, updated_at = now()
        WHERE full_name = %s AND email = %s
        """,
        (bool(is_favorite), full_name, email),
    )

    if affected == 0:
        return jsonify({"error": f"Repo not on your watchlist: {full_name!r}"}), 404

    return jsonify({"full_name": full_name, "is_favorite": bool(is_favorite)})


@app.route("/watchlist", methods=["DELETE"])
def remove_from_watchlist():
    """
    Remove a repo from the current user's watchlist.
    Expects {"full_name": "owner/repo"}.
    """
    ensure_watchlist_table()

    if request.is_json:
        full_name = request.json.get("full_name", "")
    else:
        full_name = request.form.get("full_name", "")

    full_name = full_name.strip() if isinstance(full_name, str) else ""

    if not full_name or not _REPO_RE.match(full_name):
        return jsonify({"error": f"Invalid repo name (expected owner/repo): {full_name!r}"}), 400

    email = _current_user_email()

    affected = lakebase.run_write(
        f"DELETE FROM {WATCHLIST_TABLE_NAME} WHERE full_name = %s AND email = %s",
        (full_name, email),
    )

    if affected == 0:
        return jsonify({"error": f"Repo not on your watchlist: {full_name!r}"}), 404

    return jsonify({"full_name": full_name, "removed": True})


if __name__ == '__main__':
    host = os.getenv('FLASK_RUN_HOST', '0.0.0.0')
    port = int(os.getenv('FLASK_RUN_PORT', 8000))
    app.run(debug=True, host=host, port=port)
    print(f"Flask app running on http://{host}:{port}")
