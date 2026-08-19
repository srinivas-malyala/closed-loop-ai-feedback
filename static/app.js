const form = document.getElementById("repo-form");
const addRepoButton = document.getElementById("add-repo-button");
const resultEl = document.getElementById("result");
const reposBody = document.getElementById("repos-body");
const reposEmpty = document.getElementById("repos-empty");
const schemaWarning = document.getElementById("schema-warning");
const schemaWarningDetail = document.getElementById("schema-warning-detail");
const feedbackDialogEl = document.getElementById("feedback-dialog");
const feedbackDialog = bootstrap.Modal.getOrCreateInstance(feedbackDialogEl);
const feedbackForm = document.getElementById("feedback-form");
const feedbackStatus = document.getElementById("feedback-status");
const feedbackSubmit = document.getElementById("feedback-submit");
const numberFormatter = new Intl.NumberFormat();

let activeRepoFilter = null;
let feedbackEdge = null;

function showAlert(element, message, type = "info") {
  element.className = `alert alert-${type} mt-3 mb-0`;
  element.textContent = message;
  element.style.display = "block";
}

function hideAlert(element) {
  element.style.display = "none";
  element.textContent = "";
}

function setButtonBusy(button, busy, busyLabel, idleLabel) {
  button.disabled = busy;
  button.innerHTML = busy
    ? `<span class="spinner-border spinner-border-sm me-2" aria-hidden="true"></span>${busyLabel}`
    : idleLabel;
}

function appendCell(row, className = "") {
  const cell = document.createElement("td");
  if (className) cell.className = className;
  row.appendChild(cell);
  return cell;
}

function appendTextCell(row, value, className = "") {
  const cell = appendCell(row, className);
  cell.textContent = value;
  return cell;
}

async function readJson(response) {
  try {
    return await response.json();
  } catch (_) {
    return {};
  }
}

async function checkSchemaStatus() {
  try {
    const response = await fetch("/schema/status");
    const data = await readJson(response);
    if (!response.ok) return;

    const missing = [];
    if (!data.schema_exists) missing.push(`schema ${data.schema}`);
    if (!data.github_repos_exists) missing.push("github_repos table");
    if (!data.github_files_exists) missing.push("github_files table");
    if (!data.ai_suggestion_feedback_exists) missing.push("ai_suggestion_feedback table");

    if (missing.length) {
      schemaWarningDetail.textContent = `Missing: ${missing.join(", ")}.`;
      schemaWarning.style.display = "flex";
    } else {
      schemaWarning.style.display = "none";
    }
  } catch (_) {
    // Data sections surface their own actionable errors.
  }
}

function renderRepos(repos) {
  reposBody.replaceChildren();
  reposEmpty.style.display = repos.length ? "none" : "block";
  document.getElementById("repo-count").textContent = numberFormatter.format(repos.length);
  document.getElementById("favorite-count").textContent = numberFormatter.format(
    repos.filter((repo) => repo.is_favorite).length,
  );

  repos.forEach((repo) => {
    const row = document.createElement("tr");
    row.dataset.filterable = "true";
    row.classList.toggle("table-active", activeRepoFilter === repo.full_name);
    row.addEventListener("click", () => setRepoFilter(repo.full_name));

    const repoCell = appendCell(row);
    const repoWrap = document.createElement("div");
    repoWrap.className = "repo-name";
    const repoLogo = document.createElement("span");
    repoLogo.className = "repo-logo";
    repoLogo.innerHTML = '<i class="bi bi-git" aria-hidden="true"></i>';
    const repoName = document.createElement("span");
    repoName.textContent = repo.full_name;
    repoWrap.append(repoLogo, repoName);
    repoCell.appendChild(repoWrap);

    const languageCell = appendCell(row);
    const language = document.createElement("span");
    language.className = "language-badge";
    language.textContent = repo.language || "Unknown";
    languageCell.appendChild(language);

    appendTextCell(row, repo.stargazers_count == null ? "—" : numberFormatter.format(repo.stargazers_count), "text-end fw-semibold");
    appendTextCell(row, repo.open_issues_count == null ? "—" : numberFormatter.format(repo.open_issues_count), "text-end d-none d-lg-table-cell");
    appendTextCell(row, repo.forks_count == null ? "—" : numberFormatter.format(repo.forks_count), "text-end d-none d-md-table-cell");

    const favoriteCell = appendCell(row, "text-center");
    const favoriteButton = document.createElement("button");
    favoriteButton.type = "button";
    favoriteButton.className = `icon-button${repo.is_favorite ? " is-favorite" : ""}`;
    favoriteButton.title = repo.is_favorite ? "Remove from favorites" : "Add to favorites";
    favoriteButton.setAttribute("aria-label", favoriteButton.title);
    favoriteButton.innerHTML = `<i class="bi ${repo.is_favorite ? "bi-star-fill" : "bi-star"}" aria-hidden="true"></i>`;
    favoriteButton.addEventListener("click", (event) => {
      event.stopPropagation();
      toggleFavorite(repo.full_name, !repo.is_favorite);
    });
    favoriteCell.appendChild(favoriteButton);

    const actionCell = appendCell(row, "text-end");
    const removeButton = document.createElement("button");
    removeButton.type = "button";
    removeButton.className = "icon-button is-danger";
    removeButton.title = `Remove ${repo.full_name}`;
    removeButton.setAttribute("aria-label", removeButton.title);
    removeButton.innerHTML = '<i class="bi bi-trash3" aria-hidden="true"></i>';
    removeButton.addEventListener("click", (event) => {
      event.stopPropagation();
      removeRepo(repo.full_name);
    });
    actionCell.appendChild(removeButton);
    reposBody.appendChild(row);
  });
}

async function loadRepos() {
  try {
    const response = await fetch("/repos");
    const data = await readJson(response);
    if (!response.ok) {
      reposBody.replaceChildren();
      reposEmpty.style.display = "block";
      document.getElementById("repo-count").textContent = "0";
      document.getElementById("favorite-count").textContent = "0";
      if (response.status !== 404) {
        showAlert(resultEl, `Could not load repositories: ${data.error || response.statusText}`, "danger");
      }
      return;
    }
    renderRepos(data);
  } catch (error) {
    reposBody.replaceChildren();
    reposEmpty.style.display = "block";
    showAlert(resultEl, `Could not load repositories: ${error.message}`, "danger");
  }
}

async function toggleFavorite(fullName, isFavorite) {
  try {
    const response = await fetch("/repos/favorite", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ full_name: fullName, is_favorite: isFavorite }),
    });
    const data = await readJson(response);
    if (!response.ok) throw new Error(data.error || response.statusText);
    await loadRepos();
  } catch (error) {
    showAlert(resultEl, `Could not update favorite: ${error.message}`, "danger");
  }
}

async function removeRepo(fullName) {
  if (!window.confirm(`Remove ${fullName} from your watchlist?`)) return;
  try {
    const response = await fetch("/repos", {
      method: "DELETE",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ full_name: fullName }),
    });
    const data = await readJson(response);
    if (!response.ok) throw new Error(data.error || response.statusText);
    if (activeRepoFilter === fullName) clearRepoFilter();
    showAlert(resultEl, `${fullName} was removed from your watchlist.`, "success");
    await loadRepos();
  } catch (error) {
    showAlert(resultEl, `Could not remove repository: ${error.message}`, "danger");
  }
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (!form.checkValidity()) {
    form.reportValidity();
    return;
  }

  const fullNameInput = document.getElementById("full_name");
  hideAlert(resultEl);
  setButtonBusy(addRepoButton, true, "Adding…", '<i class="bi bi-plus-lg me-1"></i>Add repository');

  try {
    const response = await fetch("/repos", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ full_name: fullNameInput.value }),
    });
    const data = await readJson(response);
    if (!response.ok) throw new Error(data.error || response.statusText);
    showAlert(resultEl, `${data.full_name} was added with ${numberFormatter.format(data.stargazers_count || 0)} stars.`, "success");
    fullNameInput.value = "";
    await loadRepos();
  } catch (error) {
    showAlert(resultEl, `Could not add repository: ${error.message}`, "danger");
  } finally {
    setButtonBusy(addRepoButton, false, "Adding…", '<i class="bi bi-plus-lg me-1"></i>Add repository');
  }
});

function setRepoFilter(repoName) {
  activeRepoFilter = repoName;
  document.getElementById("repo-filter-name").textContent = repoName;
  document.getElementById("repo-filter-indicator").style.display = "inline-flex";
  loadRepos();
  loadEdges();
  document.getElementById("graph-section").scrollIntoView({ behavior: "smooth", block: "start" });
}

function clearRepoFilter() {
  activeRepoFilter = null;
  document.getElementById("repo-filter-indicator").style.display = "none";
  loadRepos();
  loadEdges();
}

document.getElementById("clear-repo-filter").addEventListener("click", clearRepoFilter);

async function loadEdgeStats() {
  const statsEl = document.getElementById("edge-stats");
  try {
    const response = await fetch("/graph/stats");
    const data = await readJson(response);
    if (!response.ok) throw new Error(data.error || response.statusText);

    document.getElementById("edge-count").textContent = numberFormatter.format(data.total_edges);
    const aiCount = data.by_type.find((item) => item.edge_type === "ai_suggested")?.count || 0;
    document.getElementById("ai-edge-count").textContent = numberFormatter.format(aiCount);

    statsEl.replaceChildren();
    const totalBadge = document.createElement("span");
    totalBadge.className = "edge-stat-badge";
    totalBadge.innerHTML = `Total <strong>${numberFormatter.format(data.total_edges)}</strong>`;
    statsEl.appendChild(totalBadge);
    data.by_type.forEach((item) => {
      const badge = document.createElement("span");
      badge.className = "edge-stat-badge";
      const label = item.edge_type === "ai_suggested" ? "AI suggested" : item.edge_type;
      badge.textContent = `${label}: ${numberFormatter.format(item.count)}`;
      statsEl.appendChild(badge);
    });
  } catch (_) {
    document.getElementById("edge-count").textContent = "—";
    document.getElementById("ai-edge-count").textContent = "—";
    statsEl.textContent = "Stats unavailable";
  }
}

function parseEdgeMetadata(rawMetadata) {
  if (!rawMetadata) return {};
  if (typeof rawMetadata === "object") return rawMetadata;
  try {
    return JSON.parse(rawMetadata);
  } catch (_) {
    return {};
  }
}

function renderEdges(edges) {
  const edgesBody = document.getElementById("edges-body");
  const emptyState = document.getElementById("edges-empty");
  const filtered = activeRepoFilter
    ? edges.filter((edge) => edge.source === activeRepoFilter || edge.target === activeRepoFilter)
    : edges;

  edgesBody.replaceChildren();
  emptyState.style.display = filtered.length ? "none" : "block";

  filtered.forEach((edge) => {
    const metadata = parseEdgeMetadata(edge.metadata);
    const row = document.createElement("tr");

    appendTextCell(row, edge.source, "graph-repo");
    const arrowCell = appendCell(row, "connection-arrow");
    arrowCell.innerHTML = '<i class="bi bi-arrow-right" aria-hidden="true"></i>';
    appendTextCell(row, edge.target, "graph-repo");

    const typeCell = appendCell(row);
    const typeBadge = document.createElement("span");
    typeBadge.className = `edge-type ${edge.edge_type === "ai_suggested" ? "edge-type-ai" : "edge-type-dependency"}`;
    typeBadge.textContent = edge.edge_type === "ai_suggested" ? "AI suggested" : edge.edge_type;
    typeCell.appendChild(typeBadge);

    let details = "—";
    if (metadata.source_repo) details = `From ${metadata.source_repo}`;
    else if (metadata.validated) details = "Validated on GitHub";
    else if (edge.metadata && typeof edge.metadata === "string") details = edge.metadata;
    appendTextCell(row, details, "text-secondary d-none d-lg-table-cell");
    appendTextCell(
      row,
      edge.discovered_at ? new Date(edge.discovered_at).toLocaleDateString() : "—",
      "text-secondary d-none d-xl-table-cell",
    );

    const actionCell = appendCell(row);
    if (edge.edge_type === "ai_suggested") {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "btn btn-outline-danger btn-sm feedback-btn";
      button.innerHTML = '<i class="bi bi-hand-thumbs-down me-1" aria-hidden="true"></i>Mark bad';
      button.addEventListener("click", () => openFeedbackDialog(edge, metadata));
      actionCell.appendChild(button);
    } else {
      const unavailable = document.createElement("span");
      unavailable.className = "text-secondary";
      unavailable.textContent = "—";
      actionCell.appendChild(unavailable);
    }
    edgesBody.appendChild(row);
  });
}

async function loadEdges() {
  const edgeFilter = document.getElementById("edge-filter").value;
  const query = edgeFilter ? `?edge_type=${encodeURIComponent(edgeFilter)}` : "";
  const edgesBody = document.getElementById("edges-body");
  const emptyState = document.getElementById("edges-empty");
  try {
    const response = await fetch(`/graph/edges${query}`);
    const data = await readJson(response);
    if (!response.ok) throw new Error(data.error || response.statusText);
    renderEdges(data);
  } catch (error) {
    edgesBody.replaceChildren();
    emptyState.style.display = "block";
    showAlert(feedbackStatus, `Could not load graph: ${error.message}`, "danger");
  }
}

document.getElementById("edge-filter").addEventListener("change", loadEdges);

function openFeedbackDialog(edge, metadata) {
  feedbackEdge = edge;
  document.getElementById("feedback-source").textContent = edge.source;
  document.getElementById("feedback-target").textContent = edge.target;
  document.getElementById("feedback-package").value = metadata.package_name || "";
  document.getElementById("feedback-reason").value = "";
  feedbackDialog.show();
}

feedbackDialogEl.addEventListener("shown.bs.modal", () => {
  document.getElementById("feedback-package").focus();
});

feedbackForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (!feedbackEdge || !feedbackForm.checkValidity()) {
    feedbackForm.reportValidity();
    return;
  }

  hideAlert(feedbackStatus);
  setButtonBusy(
    feedbackSubmit,
    true,
    "Submitting…",
    '<i class="bi bi-hand-thumbs-down me-1"></i>Submit feedback',
  );

  try {
    const response = await fetch("/graph/edges/feedback", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        source_repo: feedbackEdge.source,
        package_name: document.getElementById("feedback-package").value,
        suggested_repo: feedbackEdge.target,
        feedback: "bad",
        reason: document.getElementById("feedback-reason").value,
      }),
    });
    const data = await readJson(response);
    if (!response.ok) throw new Error(data.error || response.statusText);
    feedbackDialog.hide();
    showAlert(
      feedbackStatus,
      `Feedback saved. ${data.suggested_repo} was marked as bad for ${data.package_name}.`,
      "success",
    );
    feedbackEdge = null;
  } catch (error) {
    feedbackDialog.hide();
    showAlert(feedbackStatus, `Could not save feedback: ${error.message}`, "danger");
  } finally {
    setButtonBusy(
      feedbackSubmit,
      false,
      "Submitting…",
      '<i class="bi bi-hand-thumbs-down me-1"></i>Submit feedback',
    );
  }
});

checkSchemaStatus();
loadRepos();
loadEdgeStats();
loadEdges();
