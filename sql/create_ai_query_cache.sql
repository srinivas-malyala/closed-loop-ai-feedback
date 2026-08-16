-- Table to cache ai_query dependency analysis results.
-- Stores the mapping of package (dependency) to suggested GitHub repo.
-- Replace <username> with your username.

CREATE TABLE IF NOT EXISTS student_<username>.ai_query_cache (
    source_repo TEXT NOT NULL,
    package_name TEXT NOT NULL,
    suggested_repo TEXT NOT NULL,
    model_name TEXT NOT NULL DEFAULT 'databricks-meta-llama-3-3-70b-instruct',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_repo, package_name, suggested_repo)
);
