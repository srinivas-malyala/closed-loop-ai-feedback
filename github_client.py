"""
Client for the GitHub REST API.

The GitHub personal access token (PAT) is OPTIONAL. Unauthenticated requests
work but are capped at 60 requests/hour; a PAT raises that to 5,000/hour.
When present, the token is stored in a Databricks secret scope (see
setup_secrets.py) and resolved at runtime via the Databricks SDK - it is
never stored in code, env files, or app.yaml.
"""

import base64
import os
from typing import Any, Iterator

import requests
from databricks.sdk import WorkspaceClient

_w = WorkspaceClient()

_SCOPE = os.environ.get("GITHUB_SECRET_SCOPE", "github")
_KEY = os.environ.get("GITHUB_SECRET_KEY", "token")
_BASE_URL = os.environ.get("GITHUB_API_BASE_URL", "https://api.github.com")

_DEFAULT_TIMEOUT = 30


def _get_token() -> str | None:
    """Fetch and decode the GitHub PAT from the Databricks secret scope.

    Returns None (falls back to unauthenticated requests) if the scope/key
    doesn't exist - a token is a nice-to-have, not a hard requirement.
    """
    try:
        secret = _w.secrets.get_secret(scope=_SCOPE, key=_KEY)
        return base64.b64decode(secret.value).decode("utf-8")
    except Exception:
        return None


class GitHubClient:
    """Thin wrapper around the GitHub REST API with optional auth."""

    def __init__(self, base_url: str | None = None, timeout: int = _DEFAULT_TIMEOUT):
        self.base_url = (base_url or _BASE_URL).rstrip("/")
        self.timeout = timeout
        self._session = requests.Session()
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        token = _get_token()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        self._session.headers.update(headers)

    def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        resp = self._session.get(f"{self.base_url}{path}", params=params, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def search_repositories(
        self,
        query: str,
        max_results: int = 200,
        per_page: int = 100,
        sort: str = "stars",
        order: str = "desc",
    ) -> Iterator[dict]:
        """
        Generator that yields repository items matching a GitHub search query
        (e.g. "language:python", "org:databricks"), sorted by stars by
        default. GitHub's Search API caps results at 1000 per query and
        max per_page at 100, so this stops early once max_results is hit.
        """
        page = 1
        yielded = 0
        while yielded < max_results:
            data = self.get(
                "/search/repositories",
                params={
                    "q": query,
                    "sort": sort,
                    "order": order,
                    "per_page": per_page,
                    "page": page,
                },
            )
            items = data.get("items", [])
            if not items:
                break
            for item in items:
                if yielded >= max_results:
                    break
                yield item
                yielded += 1
            page += 1

    def get_repo(self, full_name: str) -> dict:
        """
        Fetch a single repository's metadata in ONE API call. Use this for
        the watchlist add flow instead of search_repositories() whenever the
        caller only needs one specific "owner/repo" and wants to stay within
        tight rate limits (e.g. unauthenticated classroom accounts).
        """
        return self.get(f"/repos/{full_name}")

    def get_commits(
        self,
        full_name: str,
        per_page: int = 100,
        max_results: int = 200,
        sha: str | None = None,
    ) -> list[dict]:
        """
        Fetch commit history for a repository. Returns up to max_results
        commits, newest first. Uses pagination to gather beyond per_page.

        Args:
            full_name: "owner/repo" string.
            per_page: Results per API page (max 100).
            max_results: Stop after this many commits total.
            sha: Optional branch/tag/SHA to list commits from.
        """
        commits: list[dict] = []
        page = 1
        while len(commits) < max_results:
            params: dict = {"per_page": per_page, "page": page}
            if sha:
                params["sha"] = sha
            data = self.get(f"/repos/{full_name}/commits", params=params)
            if not data:
                break
            for item in data:
                if len(commits) >= max_results:
                    break
                commits.append(item)
            if len(data) < per_page:
                break
            page += 1
        return commits

    def get_repo_tree(self, full_name: str, ref: str, recursive: bool = True) -> dict:
        """
        Fetch the full file tree for a repository in ONE API call via the Git
        Trees API (https://docs.github.com/en/rest/git/trees). With
        recursive=True, GitHub walks the whole tree server-side and returns
        every blob (file) and tree (directory) entry - this is the cheapest
        way to enumerate "every file in the repo" without cloning it.

        Note: GitHub truncates trees over ~100k entries or 7MB of response;
        check the returned "truncated" key and treat it as best-effort.
        """
        params = {"recursive": "1"} if recursive else None
        return self.get(f"/repos/{full_name}/git/trees/{ref}", params=params)
