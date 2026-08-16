-- Exercise: Create the github_file_contents table in your student schema.
-- Replace <username> with your username.
-- This table stores actual file contents fetched from the GitHub Contents API.

CREATE TABLE IF NOT EXISTS student_<username>.github_file_contents (
    repo_full_name TEXT NOT NULL,
    path TEXT NOT NULL,
    content TEXT,
    encoding TEXT,
    sha TEXT,
    size BIGINT,
    language TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (repo_full_name, path)
);

-- Set replica identity so CDC (logical replication) can track changes.
-- You must be the table owner to run this.
ALTER TABLE student_<username>.github_file_contents REPLICA IDENTITY FULL;
