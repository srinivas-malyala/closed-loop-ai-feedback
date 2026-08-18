-- Create the writable AI-suggestion feedback table for the sri student schema.
-- The application upserts one current feedback record per AI mapping.

CREATE TABLE IF NOT EXISTS student_sri.ai_suggestion_feedback (
    id SERIAL PRIMARY KEY,
    source_repo TEXT NOT NULL,
    package_name TEXT NOT NULL,
    suggested_repo TEXT NOT NULL,
    feedback TEXT NOT NULL CHECK (feedback IN ('bad', 'good')),
    reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ai_suggestion_feedback_suggestion_key
        UNIQUE (source_repo, package_name, suggested_repo)
);

-- Required for CDC to include the complete row image for updates and deletes.
ALTER TABLE student_sri.ai_suggestion_feedback REPLICA IDENTITY FULL;
