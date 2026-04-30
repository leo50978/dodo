import { ensureFinanceDashboardSession } from "./dashboard-admin-auth.js";
import { functions as firebaseFunctions, httpsCallable } from "./firebase-init.js";

const dom = {
  status: document.getElementById("agentsStatus"),
  refreshBtn: document.getElementById("agentsRefreshBtn"),
  searchInput: document.getElementById("agentSearchInput"),
  desiredStatus: document.getElementById("agentDesiredStatus"),
  manualPromoCode: document.getElementById("agentManualPromoCode"),
  searchBtn: document.getElementById("agentSearchBtn"),
  reloadListBtn: document.getElementById("agentReloadListBtn"),
  searchResults: document.getElementById("agentSearchResults"),
  searchEmpty: document.getElementById("agentSearchEmpty"),
  agentsCount: document.getElementById("agentsCount"),
  activeCount: document.getElementById("agentsActiveCount"),
  inactiveCount: document.getElementById("agentsInactiveCount"),
  budgetRemaining: document.getElementById("agentsBudgetRemaining"),
  agentsList: document.getElementById("agentsList"),
  agentsListEmpty: document.getElementById("agentsListEmpty"),
  payrollMonthInput: document.getElementById("agentPayrollMonthInput"),
  payrollPreviewBtn: document.getElementById("agentPayrollPreviewBtn"),
  payrollCloseBtn: document.getElementById("agentPayrollCloseBtn"),
  payrollAgentsCount: document.getElementById("agentPayrollAgentsCount"),
  payrollEarnedTotal: document.getElementById("agentPayrollEarnedTotal"),
  payrollPaidTotal: document.getElementById("agentPayrollPaidTotal"),
  payrollPendingTotal: document.getElementById("agentPayrollPendingTotal"),
  payrollTableBody: document.getElementById("agentPayrollTableBody"),
  payrollEmpty: document.getElementById("agentPayrollEmpty"),
  overviewHighlight: document.getElementById("agentOverviewHighlight"),
  overviewHint: document.getElementById("agentOverviewHint"),
  overviewSvg: document.getElementById("agentOverviewSvg"),
};

const callables = {
  searchAgentCandidatesSecure: httpsCallable(firebaseFunctions, "searchAgentCandidatesSecure"),
  upsertAgentSecure: httpsCallable(firebaseFunctions, "upsertAgentSecure"),
  listAgentsSecure: httpsCallable(firebaseFunctions, "listAgentsSecure"),
  getAgentPayrollSnapshotSecure: httpsCallable(firebaseFunctions, "getAgentPayrollSnapshotSecure"),
  closeAgentPayrollMonthSecure: httpsCallable(firebaseFunctions, "closeAgentPayrollMonthSecure"),
  getAgentProgramOverviewSecure: httpsCallable(firebaseFunctions, "getAgentProgramOverviewSecure"),
};

function safeInt(value) {
  const num = Number(value);
  return Number.isFinite(num) ? Math.trunc(num) : 0;
}

function formatInt(value) {
  return new Intl.NumberFormat("fr-FR", { maximumFractionDigits: 0 }).format(safeInt(value));
}

function formatHtg(value) {
  return `${formatInt(value)} HTG`;
}

function formatDoes(value) {
  return `${formatInt(value)} Does`;
}

function formatDateTime(ms) {
  const safeMs = safeInt(ms);
  if (!safeMs) return "-";
  return new Date(safeMs).toLocaleString("fr-FR");
}

function getPreviousMonthKey() {
  const date = new Date();
  date.setUTCDate(1);
  date.setUTCHours(0, 0, 0, 0);
  date.setUTCMonth(date.getUTCMonth() - 1);
  const year = String(date.getUTCFullYear());
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  return `${year}-${month}`;
}

function escapeHtml(value = "") {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function normalizeCallableError(err, fallback = "Erreur serveur") {
  const codeRaw = String(err?.code || "");
  const firebaseCode = codeRaw.startsWith("functions/") ? codeRaw.slice("functions/".length) : codeRaw;
  const details = err?.details && typeof err.details === "object" ? err.details : {};
  const normalized = new Error(String(err?.message || fallback));
  normalized.code = String(details.code || firebaseCode || "unknown");
  Object.assign(normalized, details);
  return normalized;
}

async function invokeCallable(name, payload = {}, fallback = "Erreur serveur") {
  try {
    const response = await callables[name](payload);
    return response?.data || null;
  } catch (error) {
    throw normalizeCallableError(error, fallback);
  }
}

function setStatus(text, tone = "neutral") {
  if (!dom.status) return;
  dom.status.textContent = String(text || "");
  dom.status.dataset.tone = tone;
}

function renderSearchResults(items = []) {
  if (!dom.searchResults || !dom.searchEmpty) return;
  if (!Array.isArray(items) || items.length === 0) {
    dom.searchResults.innerHTML = "";
    dom.searchEmpty.style.display = "block";
    return;
  }

  dom.searchEmpty.style.display = "none";
  dom.searchResults.innerHTML = items.map((item) => {
    const isAgent = item.isAgent === true;
    const status = String(item.agentStatus || "").toLowerCase();
    return `
      <article class="result-card">
        <div class="result-head">
          <div>
            <div class="result-title">${escapeHtml(item.name || item.username || item.email || item.uid || "Utilisateur")}</div>
            <div class="meta">${escapeHtml(item.email || item.phone || item.uid || "-")}</div>
          </div>
          <div class="chips">
            <span class="chip ${status === "active" ? "active" : "inactive"}">${escapeHtml(isAgent ? `Agent ${status || "inactif"}` : "Non agent")}</span>
            ${item.agentPromoCode ? `<span class="chip">${escapeHtml(item.agentPromoCode)}</span>` : ""}
          </div>
        </div>
        <div class="meta">Dernière activité: ${escapeHtml(item.lastSeenAtMs ? new Date(item.lastSeenAtMs).toLocaleString("fr-FR") : "inconnue")}</div>
        <div class="actions-row">
          <button class="action-btn" type="button" data-action="assign" data-uid="${escapeHtml(item.uid)}" data-status="active">Nommer actif</button>
          <button class="action-btn" type="button" data-action="assign" data-uid="${escapeHtml(item.uid)}" data-status="inactive">Nommer inactif</button>
        </div>
      </article>
    `;
  }).join("");
}

function renderAgentList(items = []) {
  if (!dom.agentsList || !dom.agentsListEmpty) return;
  if (!Array.isArray(items) || items.length === 0) {
    dom.agentsList.innerHTML = "";
    dom.agentsListEmpty.style.display = "block";
    if (dom.agentsCount) dom.agentsCount.textContent = "0";
    if (dom.activeCount) dom.activeCount.textContent = "0";
    if (dom.inactiveCount) dom.inactiveCount.textContent = "0";
    if (dom.budgetRemaining) dom.budgetRemaining.textContent = "0 HTG";
    return;
  }

  const activeCount = items.filter((item) => String(item.status || "").toLowerCase() === "active").length;
  const inactiveCount = items.length - activeCount;
  const budgetRemaining = items.reduce((sum, item) => sum + Math.max(0, safeInt(item.signupBudgetRemainingHtg)), 0);

  if (dom.agentsCount) dom.agentsCount.textContent = formatInt(items.length);
  if (dom.activeCount) dom.activeCount.textContent = formatInt(activeCount);
  if (dom.inactiveCount) dom.inactiveCount.textContent = formatInt(inactiveCount);
  if (dom.budgetRemaining) dom.budgetRemaining.textContent = formatHtg(budgetRemaining);

  dom.agentsListEmpty.style.display = "none";
  dom.agentsList.innerHTML = items.map((item) => {
    const isActive = String(item.status || "").toLowerCase() === "active";
    return `
      <article class="agent-card">
        <div class="agent-head">
          <div>
            <div class="agent-title">${escapeHtml(item.displayName || item.username || item.email || item.uid || "Agent")}</div>
            <div class="meta">${escapeHtml(item.email || item.phone || item.uid || "-")}</div>
          </div>
          <div class="chips">
            <span class="chip ${isActive ? "active" : "inactive"}">${escapeHtml(isActive ? "Actif" : "Inactif")}</span>
            <span class="chip">${escapeHtml(item.promoCode || "Sans code")}</span>
          </div>
        </div>
        <div class="meta">Budget restant: ${escapeHtml(formatHtg(item.signupBudgetRemainingHtg))} · Gains du mois: ${escapeHtml(formatInt(item.currentMonthEarnedDoes))} Does</div>
        <div class="meta">Signups suivis: ${escapeHtml(formatInt(item.totalTrackedSignups))} · Dépôts: ${escapeHtml(formatInt(item.totalTrackedDeposits))} · Victoires: ${escapeHtml(formatInt(item.totalTrackedWins))}</div>
        <div class="actions-row">
          <button class="action-btn" type="button" data-action="toggle" data-uid="${escapeHtml(item.uid)}" data-status="${isActive ? "inactive" : "active"}">${isActive ? "Désactiver" : "Activer"}</button>
        </div>
      </article>
    `;
  }).join("");
}

async function refreshAgentsList() {
  const result = await invokeCallable("listAgentsSecure", {}, "Impossible de charger les agents.");
  renderAgentList(Array.isArray(result?.items) ? result.items : []);
}

function renderPayrollSnapshot(snapshot = {}) {
  const totals = snapshot?.totals || {};
  const items = Array.isArray(snapshot?.items) ? snapshot.items : [];

  if (dom.payrollAgentsCount) dom.payrollAgentsCount.textContent = formatInt(totals.agents);
  if (dom.payrollEarnedTotal) dom.payrollEarnedTotal.textContent = formatDoes(totals.earnedDoes);
  if (dom.payrollPaidTotal) dom.payrollPaidTotal.textContent = formatDoes(totals.paidDoes);
  if (dom.payrollPendingTotal) dom.payrollPendingTotal.textContent = formatDoes(totals.payableDoes);

  if (!dom.payrollTableBody || !dom.payrollEmpty) return;
  if (!items.length) {
    dom.payrollTableBody.innerHTML = "";
    dom.payrollEmpty.style.display = "block";
    return;
  }

  dom.payrollEmpty.style.display = "none";
  dom.payrollTableBody.innerHTML = items.map((item) => `
    <tr>
      <td>${escapeHtml(item.displayName || item.uid || "Agent")}</td>
      <td>${escapeHtml(item.promoCode || "-")}</td>
      <td>${formatDoes(item.earnedDoes)}</td>
      <td>${formatDoes(item.paidDoes)}</td>
      <td>${formatDoes(item.payableDoes)}</td>
      <td>${formatInt(item.signupsCount)}</td>
      <td>${formatInt(item.depositsCount)}</td>
      <td>${formatInt(item.winsCount)}</td>
      <td>${escapeHtml(item.closedAtMs ? formatDateTime(item.closedAtMs) : "Ouvert")}</td>
    </tr>
  `).join("");
}

function renderOverviewTrend(timeline = []) {
  if (!dom.overviewSvg) return;
  if (!Array.isArray(timeline) || !timeline.length) {
    dom.overviewSvg.innerHTML = `
      <rect x="0" y="0" width="720" height="220" fill="rgba(255,255,255,0.02)"></rect>
      <line x1="0" y1="176" x2="720" y2="176" stroke="rgba(255,255,255,0.08)" stroke-width="1"></line>
      <text x="24" y="36" fill="rgba(255,255,255,0.58)" font-size="14">Aucune donnée mensuelle agent disponible.</text>
    `;
    return;
  }

  const width = 720;
  const height = 220;
  const leftPad = 26;
  const rightPad = 20;
  const topPad = 26;
  const bottomPad = 32;
  const chartWidth = width - leftPad - rightPad;
  const chartHeight = height - topPad - bottomPad;
  const values = timeline.map((item) => Math.max(0, safeInt(item.earnedDoes)));
  const maxValue = Math.max(...values, 1);
  const points = values.map((value, index) => {
    const x = leftPad + (timeline.length === 1 ? chartWidth / 2 : (index * chartWidth) / (timeline.length - 1));
    const y = topPad + chartHeight - ((value / maxValue) * chartHeight);
    return { x, y, label: String(timeline[index]?.monthKey || "") };
  });
  const linePath = points.map((point) => `${point.x},${point.y}`).join(" ");
  const areaPath = [
    `M ${points[0].x} ${topPad + chartHeight}`,
    ...points.map((point) => `L ${point.x} ${point.y}`),
    `L ${points[points.length - 1].x} ${topPad + chartHeight}`,
    "Z",
  ].join(" ");

  dom.overviewSvg.innerHTML = `
    <defs>
      <linearGradient id="agentOverviewFill" x1="0" x2="0" y1="0" y2="1">
        <stop offset="0%" stop-color="rgba(104,215,255,0.24)"></stop>
        <stop offset="100%" stop-color="rgba(104,215,255,0.02)"></stop>
      </linearGradient>
    </defs>
    <rect x="0" y="0" width="${width}" height="${height}" fill="rgba(255,255,255,0.02)"></rect>
    <line x1="${leftPad}" y1="${topPad + chartHeight}" x2="${width - rightPad}" y2="${topPad + chartHeight}" stroke="rgba(255,255,255,0.08)" stroke-width="1"></line>
    <path d="${areaPath}" fill="url(#agentOverviewFill)"></path>
    <polyline points="${linePath}" fill="none" stroke="#68d7ff" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"></polyline>
    ${points.map((point) => `<circle cx="${point.x}" cy="${point.y}" r="4" fill="#68d7ff"></circle>`).join("")}
    ${points.map((point) => `<text x="${point.x}" y="${height - 8}" text-anchor="middle" fill="rgba(255,255,255,0.58)" font-size="12">${escapeHtml(point.label)}</text>`).join("")}
  `;
}

async function refreshProgramOverview() {
  const result = await invokeCallable(
    "getAgentProgramOverviewSecure",
    {},
    "Impossible de charger la tendance du programme agent."
  );
  const timeline = Array.isArray(result?.timeline) ? result.timeline : [];
  renderOverviewTrend(timeline);
  const latest = result?.latest || null;
  if (dom.overviewHighlight) {
    dom.overviewHighlight.textContent = latest
      ? `${latest.monthKey} · ${formatDoes(latest.earnedDoes)} gagnés`
      : "Aucune tendance disponible";
  }
  if (dom.overviewHint) {
    dom.overviewHint.textContent = latest
      ? `${formatDoes(latest.paidDoes)} déjà payés · ${formatDoes(latest.pendingDoes)} en attente · ${formatInt(latest.signupsCount)} inscriptions`
      : "La courbe suivra les gains, paiements et volumes d’inscriptions des agents.";
  }
}

async function refreshPayrollSnapshot() {
  const monthKey = String(dom.payrollMonthInput?.value || "").trim() || getPreviousMonthKey();
  const result = await invokeCallable(
    "getAgentPayrollSnapshotSecure",
    { monthKey },
    "Impossible de charger le payroll agent."
  );
  renderPayrollSnapshot(result || {});
  return result;
}

async function performSearch() {
  const query = String(dom.searchInput?.value || "").trim();
  if (!query) {
    renderSearchResults([]);
    setStatus("Entre un uid, un email, un username ou un téléphone pour lancer la recherche.");
    return;
  }

  setStatus("Recherche des utilisateurs...");
  const result = await invokeCallable(
    "searchAgentCandidatesSecure",
    { query },
    "Impossible de rechercher les utilisateurs."
  );
  renderSearchResults(Array.isArray(result?.results) ? result.results : []);
  setStatus("Recherche terminée.", "success");
}

async function assignAgent(uid, explicitStatus = "") {
  const status = String(explicitStatus || dom.desiredStatus?.value || "inactive").trim().toLowerCase() === "active"
    ? "active"
    : "inactive";
  const promoCode = String(dom.manualPromoCode?.value || "").trim().toUpperCase();
  setStatus("Enregistrement de l'agent...");
  await invokeCallable(
    "upsertAgentSecure",
    { clientId: uid, status, promoCode },
    "Impossible d'enregistrer l'agent."
  );
  setStatus(`Agent mis à jour (${status}).`, "success");
  await refreshAgentsList();
  if (dom.searchInput?.value) {
    await performSearch();
  }
}

async function closePayrollMonth() {
  const monthKey = String(dom.payrollMonthInput?.value || "").trim() || getPreviousMonthKey();
  const confirmed = window.confirm(`Clôturer le payroll agent pour ${monthKey} ?`);
  if (!confirmed) return;
  setStatus(`Clôture du payroll ${monthKey}...`);
  const result = await invokeCallable(
    "closeAgentPayrollMonthSecure",
    { monthKey },
    "Impossible de clôturer le payroll agent."
  );
  await refreshPayrollSnapshot();
  await refreshAgentsList();
  setStatus(
    `Payroll ${monthKey} clôturé pour ${formatInt(result?.closedCount)} agent(s), total payé ${formatDoes(result?.paidDoesTotal)}.`,
    "success"
  );
}

function bindDelegatedActions() {
  document.addEventListener("click", (event) => {
    const button = event.target instanceof Element ? event.target.closest("[data-action]") : null;
    if (!button) return;
    const action = String(button.getAttribute("data-action") || "");
    const uid = String(button.getAttribute("data-uid") || "");
    const status = String(button.getAttribute("data-status") || "");
    if (!uid) return;

    if (action === "assign" || action === "toggle") {
      void assignAgent(uid, status).catch((error) => {
        console.error("[DAGENTS] assign error", error);
        setStatus(error?.message || "Impossible de mettre à jour l'agent.", "error");
      });
    }
  });
}

async function bootstrap() {
  await ensureFinanceDashboardSession({ fallbackUrl: "./index.html" });
  bindDelegatedActions();

  dom.searchBtn?.addEventListener("click", () => {
    void performSearch().catch((error) => {
      console.error("[DAGENTS] search error", error);
      setStatus(error?.message || "Impossible de rechercher les utilisateurs.", "error");
    });
  });

  dom.reloadListBtn?.addEventListener("click", () => {
    void refreshAgentsList().then(() => {
      setStatus("Liste agents rechargée.", "success");
    }).catch((error) => {
      console.error("[DAGENTS] list error", error);
      setStatus(error?.message || "Impossible de charger les agents.", "error");
    });
  });

  dom.refreshBtn?.addEventListener("click", () => {
    void Promise.all([
      refreshAgentsList(),
      refreshPayrollSnapshot(),
      refreshProgramOverview(),
      dom.searchInput?.value ? performSearch() : Promise.resolve(),
    ]).then(() => {
      setStatus("Vue agents synchronisée.", "success");
    }).catch((error) => {
      console.error("[DAGENTS] refresh error", error);
      setStatus(error?.message || "Impossible d'actualiser la page agents.", "error");
    });
  });

  dom.payrollPreviewBtn?.addEventListener("click", () => {
    void refreshPayrollSnapshot().then(() => {
      setStatus("Payroll agent rechargé.", "success");
    }).catch((error) => {
      console.error("[DAGENTS] payroll preview error", error);
      setStatus(error?.message || "Impossible de charger le payroll agent.", "error");
    });
  });

  dom.payrollCloseBtn?.addEventListener("click", () => {
    void closePayrollMonth().catch((error) => {
      console.error("[DAGENTS] payroll close error", error);
      setStatus(error?.message || "Impossible de clôturer le payroll agent.", "error");
    });
  });

  dom.searchInput?.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      void performSearch().catch((error) => {
        console.error("[DAGENTS] search enter error", error);
        setStatus(error?.message || "Impossible de rechercher les utilisateurs.", "error");
      });
    }
  });

  if (dom.payrollMonthInput && !dom.payrollMonthInput.value) {
    dom.payrollMonthInput.value = getPreviousMonthKey();
  }

  await Promise.all([refreshAgentsList(), refreshPayrollSnapshot(), refreshProgramOverview()]);
  setStatus("Page agents prête.", "success");
}

void bootstrap().catch((error) => {
  console.error("[DAGENTS] bootstrap error", error);
  setStatus(error?.message || "Impossible d'ouvrir la page agents.", "error");
});
