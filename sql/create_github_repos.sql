-- Exercise: Create the github_repos table in your student schema.
-- Replace <username> with your username.
-- This table mirrors the shape of the github_repos_bronze Delta table,
-- with an added is_favorite column for marking repos you like.

CREATE TABLE IF NOT EXISTS student_<username>.github_repos (
    id BIGINT,
    full_name TEXT NOT NULL,
    language TEXT,
    stargazers_count INTEGER,
    open_issues_count INTEGER,
    forks_count INTEGER,
    is_favorite BOOLEAN NOT NULL DEFAULT FALSE,
    payload JSONB,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (full_name)
);

-- Set replica identity so CDC (logical replication) can track changes.
-- You must be the table owner to run this.
ALTER TABLE student_<username>.github_repos REPLICA IDENTITY FULL;
