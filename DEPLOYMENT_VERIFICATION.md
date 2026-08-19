# Databricks App deployment verification

Use the `dataexpertio_srini` CLI profile and the `prod` bundle target for all
commands below. The expected source repository is:

`https://github.com/srinivas-malyala/closed-loop-ai-feedback`

## Before deployment

- [ ] Confirm the working branch is `main`.
- [ ] Confirm the intended changes are committed and pushed to `origin/main`.
- [ ] Record the expected commit with `git rev-parse origin/main`.
- [ ] Confirm `origin` is the expected repository with `git remote get-url origin`.
- [ ] Validate the bundle with
      `databricks bundle validate --strict -t prod --profile dataexpertio_srini`.

## Deploy from Git

- [ ] Review `deployment/app-metadata-update.json` and confirm the provider is
      `gitHub` and the URL is the expected independent repository.
- [ ] Update only the app description and Git repository metadata with
      `databricks apps create-update week1-homework --profile dataexpertio_srini --json @deployment/app-metadata-update.json`.
- [ ] Review `deployment/git-deployment.json` and confirm its branch is `main`,
      its source path is `.`, and its mode is `SNAPSHOT`.
- [ ] Deploy only the app from Git with
      `databricks apps deploy week1-homework --profile dataexpertio_srini --json @deployment/git-deployment.json`.
- [ ] Save the deployment ID returned by the command.

Do not use the project-level `databricks apps deploy -t prod` command for this
repository: the bundle also contains a Lakeflow Job, so a full bundle plan can
include that unrelated resource.

## Verify the deployed app and provenance

- [ ] Run
      `databricks apps get week1-homework --profile dataexpertio_srini -o json`.
- [ ] Confirm `app_status.state` is `RUNNING`.
- [ ] Confirm `active_deployment.status.state` is `SUCCEEDED`.
- [ ] Confirm the app URL is
      `https://week1-homework-1352785079224954.aws.databricksapps.com` and open
      it in a browser.
- [ ] Confirm `git_repository.url` is
      `https://github.com/srinivas-malyala/closed-loop-ai-feedback`.
- [ ] Confirm the active deployment contains `git_source`, not a workspace
      `source_code_path`.
- [ ] Confirm `git_source.branch` is `main`.
- [ ] Confirm `git_source.resolved_commit` exactly matches the recorded
      `git rev-parse origin/main` value.
- [ ] Confirm both application resources remain attached:
      `database/lakebase-url` with `READ` and `github/token` with `READ`.
- [ ] Exercise `/healthz`, sign in, load repository data, and submit one AI
      suggestion feedback value.
- [ ] If OAuth authentication is available, inspect startup/runtime logs with
      `databricks apps logs week1-homework --profile dataexpertio_srini`.
