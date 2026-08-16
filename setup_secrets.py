"""
One-time setup script: creates the Databricks secret scopes and stores the
Lakebase connection URL, plus an OPTIONAL GitHub personal access token (PAT).
Run this locally (with the Databricks CLI configured) or from a notebook -
never commit the resulting secret value anywhere.

A GitHub PAT is not required - unauthenticated requests work fine for this
lab's data volumes, just at a lower rate limit (60 req/hr vs 5,000 req/hr).
Press Enter at the GitHub prompt to skip it.

Usage:
    python setup_secrets.py
"""
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace
import getpass

username = ''
if not username:
    print('please enter your username')
else:
    w = WorkspaceClient()

    github_token = getpass.getpass(
        "Paste your GitHub personal access token (optional, press Enter to skip): "
    )
    if github_token:
        # w.secrets.create_scope(scope="github")
        w.secrets.put_secret(scope="github", key=username, string_value=github_token)
        w.secrets.put_acl(
            scope="github",
            principal="users",
            permission=workspace.AclPermission.READ,
        )
        print("Stored GitHub token in scope 'github', key ." + username)
        print("use databricks secrets for that")
    else:
        print("Skipped GitHub token - app will use unauthenticated GitHub API requests.")
