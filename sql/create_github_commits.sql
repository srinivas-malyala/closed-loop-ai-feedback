-- Exercise: Create the github_commits table in your student schema.
-- Replace <username> with your username.
-- This table stores commit history fetched from the GitHub API.

CREATE TABLE IF NOT EXISTS student_<username>.github_commits (
    repo_full_name TEXT NOT NULL,
    sha TEXT NOT NULL,
    author_name TEXT,
    author_email TEXT,
    author_date TIMESTAMPTZ,
    committer_name TEXT,
    committer_email TEXT,
    committer_date TIMESTAMPTZ,
    message TEXT,
    additions INTEGER,
    deletions INTEGER,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (repo_full_name, sha)
);

-- Set replica identity so CDC (logical replication) can track changes.
-- You must be the table owner to run this.
ALTER TABLE student_<username>.github_commits REPLICA IDENTITY FULL;
