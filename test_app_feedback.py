"""API tests for AI suggestion feedback."""

import sys
from types import ModuleType

import pytest

pytest.importorskip("flask")
pytest.importorskip("requests")


fake_lakebase = ModuleType("lakebase")
fake_lakebase.run_query = lambda *args, **kwargs: []
fake_lakebase.run_write = lambda *args, **kwargs: 1
fake_lakebase.run_write_returning = lambda *args, **kwargs: {}
sys.modules["lakebase"] = fake_lakebase

fake_github_client = ModuleType("github_client")
fake_github_client.GitHubClient = object
sys.modules["github_client"] = fake_github_client

import app as app_module


@pytest.fixture
def client(monkeypatch):
    app_module.app.config.update(TESTING=True, SECRET_KEY="test-secret")
    with app_module.app.test_client() as test_client:
        yield test_client


def _sign_in(client):
    with client.session_transaction() as user_session:
        user_session["username"] = "sri"


def test_feedback_requires_sign_in(client):
    response = client.post("/graph/edges/feedback", json={})
    assert response.status_code == 401


@pytest.mark.parametrize(
    "payload,error_fragment",
    [
        ({}, "source_repo is required"),
        ({
            "source_repo": "not-a-repo",
            "package_name": "react",
            "suggested_repo": "facebook/react",
            "feedback": "bad",
        }, "Invalid source_repo"),
        ({
            "source_repo": "facebook/react",
            "package_name": "react",
            "suggested_repo": "not-a-repo",
            "feedback": "bad",
        }, "Invalid suggested_repo"),
        ({
            "source_repo": "facebook/react",
            "package_name": "react",
            "suggested_repo": "facebook/react",
            "feedback": "maybe",
        }, "feedback must be one of"),
    ],
)
def test_feedback_validates_input(client, payload, error_fragment):
    _sign_in(client)
    response = client.post("/graph/edges/feedback", json=payload)
    assert response.status_code == 400
    assert error_fragment in response.get_json()["error"]


def test_feedback_upserts_and_returns_row(client, monkeypatch):
    _sign_in(client)
    returned = {
        "id": 7,
        "source_repo": "facebook/react",
        "package_name": "loose-envify",
        "suggested_repo": "zertosh/loose-envify",
        "feedback": "bad",
        "reason": "Repo is archived",
        "created_at": "2026-08-18T12:00:00Z",
    }
    captured = {}

    def fake_write(sql, params):
        captured["sql"] = sql
        captured["params"] = params
        return returned

    monkeypatch.setattr(app_module.lakebase, "run_write_returning", fake_write)
    response = client.post(
        "/graph/edges/feedback",
        json={
            "source_repo": " facebook/react ",
            "package_name": " loose-envify ",
            "suggested_repo": " zertosh/loose-envify ",
            "feedback": "BAD",
            "reason": " Repo is archived ",
        },
    )

    assert response.status_code == 200
    assert response.get_json() == returned
    assert "INSERT INTO student_sri.ai_suggestion_feedback" in captured["sql"]
    assert "ON CONFLICT (source_repo, package_name, suggested_repo)" in captured["sql"]
    assert captured["params"] == (
        "facebook/react",
        "loose-envify",
        "zertosh/loose-envify",
        "bad",
        "Repo is archived",
    )


def test_feedback_returns_404_when_table_is_missing(client, monkeypatch):
    _sign_in(client)

    def missing_table(*args, **kwargs):
        raise RuntimeError('relation "student_sri.ai_suggestion_feedback" does not exist')

    monkeypatch.setattr(app_module.lakebase, "run_write_returning", missing_table)
    response = client.post(
        "/graph/edges/feedback",
        json={
            "source_repo": "facebook/react",
            "package_name": "loose-envify",
            "suggested_repo": "zertosh/loose-envify",
            "feedback": "bad",
        },
    )

    assert response.status_code == 404
    assert "Feedback table not found" in response.get_json()["error"]
