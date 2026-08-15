-- Exercise: Create the github_files table in your student schema.
-- Replace <username> with your username.
-- This table mirrors the shape of the github_repo_files_bronze Delta table.

CREATE TABLE IF NOT EXISTS student_<username>.github_files (
    repo_full_name TEXT NOT NULL,
    path TEXT NOT NULL,
    type TEXT,
    sha TEXT,
    size BIGINT,
    mode TEXT,
    tree_truncated BOOLEAN,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (repo_full_name, path)
);

-- Set replica identity so CDC (logical replication) can track changes.
-- You must be the table owner to run this.
ALTER TABLE student_<username>.github_files REPLICA IDENTITY FULL;
