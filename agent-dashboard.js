import { auth, watchAuthState } from "./auth.js";
import { getMyAgentDashboardSecure } from "./secure-functions.js";

const dom = {
  status: document.getElementById("agentDashboardStatus"),
  refreshBtn: document.getElementById("agentDashboardRefreshBtn"),
  statusPill: document.getElementById("agentStatusPill"),
  identity: document.getElementById("agentIdentity"),
  budgetRemaining: document.getElementById("agentBudgetRemaining"),
  budgetMeta: document.getElementById("agentBudgetMeta"),
  currentMonthEarned: document.getElementById("agentCurrentMonthEarned"),
  lifetimeEarned: document.getElementById("agentLifetimeEarned"),
  trackedSignups: document.getElementById("agentTrackedSignups"),
  trackedMeta: document.getElementById("agentTrackedMeta"),
  promoCode: document.getElementById("agentPromoCode"),
  promoLink: document.getElementById("agentPromoLink"),
  copyPromoBtn: document.getElementById("agentCopyPromoBtn"),
  copyLinkBtn: document.getElementById("agentCopyLinkBtn"),
  trendHighlight: document.getElementById("agentTrendHighlight"),
  trendSvg: document.getElementById("agentTrendSvg"),
  trendHint: document.getElementById("agentTrendHint"),
  monthlyBody: document.getElementById("agentMonthlyTableBody"),
  monthlyEmpty: document.getElementById("agentMonthlyEmpty"),
  referralsList: document.getElementById("agentReferralsList"),
  referralsEmpty: document.getElementById("agentReferralsEmpty"),
  ledgerList: document.getElementById("agentLedgerList"),
  ledgerEmpty: document.getElementById("agentLedgerEmpty"),
};

function safeInt(value) {
  const num = Number(value);
  return Number.isFinite(num) ? Math.trunc(num) : 0;
}

function formatInt(value) {
  return new Intl.NumberFormat("fr-FR", { maximumFractionDigits: 0 }).format(safeInt(value));
}

function formatDoes(value) {
  return `${formatInt(value)} Does`;
}

function formatHtg(value) {
  return `${formatInt(value)} HTG`;
}

function formatDateTime(ms) {
  const safeMs = safeInt(ms);
  if (!safeMs) return "-";
  return new Date(safeMs).toLocaleString("fr-FR");
}

function escapeHtml(value = "") {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function setStatus(text, tone = "neutral") {
  if (!dom.status) return;
  dom.status.textContent = String(text || "");
  dom.status.dataset.tone = tone;
}

async function copyToClipboard(text) {
  const value = String(text || "").trim();
  if (!value) return false;
  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(value);
      return true;
    }
  } catch (_) {}

  try {
    const area = document.createElement("textarea");
    area.value = value;
    area.style.position = "fixed";
    area.style.opacity = "0";
    document.body.appendChild(area);
    area.select();
    document.execCommand("copy");
    document.body.removeChild(area);
    return true;
  } catch (_) {
    return false;
  }
}

function renderTrend(trend = []) {
  if (!dom.trendSvg) return;

  if (!Array.isArray(trend) || trend.length === 0) {
    dom.trendSvg.innerHTML = `
      <rect x="0" y="0" width="720" height="220" fill="rgba(255,255,255,0.02)"></rect>
      <line x1="0" y1="176" x2="720" y2="176" stroke="rgba(255,255,255,0.08)" stroke-width="1"></line>
      <text x="24" y="36" fill="rgba(255,255,255,0.58)" font-size="14">Aucune donnée mensuelle pour tracer la courbe.</text>
    `;
    return;
  }

  const values = trend.map((item) => Math.max(0, safeInt(item.earnedDoes)));
  const maxValue = Math.max(...values, 1);
  const width = 720;
  const height = 220;
  const leftPad = 26;
  const rightPad = 20;
  const topPad = 26;
  const bottomPad = 32;
  const chartWidth = width - leftPad - rightPad;
  const chartHeight = height - topPad - bottomPad;

  const points = values.map((value, index) => {
    const x = leftPad + (trend.length === 1 ? chartWidth / 2 : (index * chartWidth) / (trend.length - 1));
    const y = topPad + chartHeight - ((value / maxValue) * chartHeight);
    return { x, y, value, label: String(trend[index]?.label || "") };
  });

  const linePath = points.map((point) => `${point.x},${point.y}`).join(" ");
  const areaPath = [
    `M ${points[0].x} ${topPad + chartHeight}`,
    ...points.map((point) => `L ${point.x} ${point.y}`),
    `L ${points[points.length - 1].x} ${topPad + chartHeight}`,
    "Z",
  ].join(" ");

  const labels = points.map((point) => `
    <text x="${point.x}" y="${height - 8}" text-anchor="middle" fill="rgba(255,255,255,0.58)" font-size="12">${escapeHtml(point.label)}</text>
  `).join("");

  const markers = points.map((point) => `
    <circle cx="${point.x}" cy="${point.y}" r="4" fill="#69d2ff"></circle>
  `).join("");

  dom.trendSvg.innerHTML = `
    <defs>
      <linearGradient id="agentTrendFill" x1="0" x2="0" y1="0" y2="1">
        <stop offset="0%" stop-color="rgba(105,210,255,0.28)"></stop>
        <stop offset="100%" stop-color="rgba(105,210,255,0.02)"></stop>
      </linearGradient>
    </defs>
    <rect x="0" y="0" width="${width}" height="${height}" fill="rgba(255,255,255,0.02)"></rect>
    <line x1="${leftPad}" y1="${topPad + chartHeight}" x2="${width - rightPad}" y2="${topPad + chartHeight}" stroke="rgba(255,255,255,0.08)" stroke-width="1"></line>
    <line x1="${leftPad}" y1="${topPad}" x2="${leftPad}" y2="${topPad + chartHeight}" stroke="rgba(255,255,255,0.05)" stroke-width="1"></line>
    <path d="${areaPath}" fill="url(#agentTrendFill)"></path>
    <polyline points="${linePath}" fill="none" stroke="#69d2ff" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"></polyline>
    ${markers}
    ${labels}
  `;
}

function renderMonthlyStatements(items = []) {
  if (!dom.monthlyBody || !dom.monthlyEmpty) return;
  if (!Array.isArray(items) || items.length === 0) {
    dom.monthlyBody.innerHTML = "";
    dom.monthlyEmpty.style.display = "block";
    return;
  }

  dom.monthlyEmpty.style.display = "none";
  dom.monthlyBody.innerHTML = items.map((item) => `
    <tr>
      <td>${escapeHtml(item.monthKey || "-")}</td>
      <td>${formatDoes(item.earnedDoes)}</td>
      <td>${formatDoes(item.paidDoes)}</td>
      <td>${formatInt(item.signupsCount)}</td>
      <td>${formatInt(item.depositsCount)}</td>
      <td>${formatInt(item.winsCount)}</td>
      <td>${escapeHtml(item.closedAtMs ? formatDateTime(item.closedAtMs) : "Ouvert")}</td>
    </tr>
  `).join("");
}

function renderReferrals(items = []) {
  if (!dom.referralsList || !dom.referralsEmpty) return;
  if (!Array.isArray(items) || items.length === 0) {
    dom.referralsList.innerHTML = "";
    dom.referralsEmpty.style.display = "block";
    return;
  }

  dom.referralsEmpty.style.display = "none";
  dom.referralsList.innerHTML = items.map((item) => `
    <article class="list-item">
      <strong>${escapeHtml(item.name || item.username || item.email || item.uid || "Utilisateur")}</strong>
      <span>${escapeHtml(item.email || item.phone || item.uid || "-")}</span>
      <span>Inscrit le ${escapeHtml(formatDateTime(item.createdAtMs))}${item.hasApprovedDeposit ? " · Dépôt approuvé" : ""}</span>
    </article>
  `).join("");
}

function renderLedger(items = []) {
  if (!dom.ledgerList || !dom.ledgerEmpty) return;
  if (!Array.isArray(items) || items.length === 0) {
    dom.ledgerList.innerHTML = "";
    dom.ledgerEmpty.style.display = "block";
    return;
  }

  dom.ledgerEmpty.style.display = "none";
  dom.ledgerList.innerHTML = items.map((item) => {
    const doesDelta = safeInt(item.deltaDoes);
    const deltaLabel = doesDelta === 0 ? "0 Does" : `${doesDelta > 0 ? "+" : ""}${formatDoes(doesDelta)}`;
    return `
      <article class="list-item">
        <strong>${escapeHtml(item.label || item.type || "Mouvement")}</strong>
        <span>${escapeHtml(deltaLabel)}${item.deltaHtg ? ` · ${item.deltaHtg > 0 ? "+" : ""}${formatHtg(item.deltaHtg)}` : ""}</span>
        <span>${escapeHtml(formatDateTime(item.createdAtMs))}</span>
      </article>
    `;
  }).join("");
}

function renderSnapshot(snapshot = {}) {
  const agent = snapshot.agent || {};
  const trend = Array.isArray(snapshot.trend) ? snapshot.trend : [];

  if (dom.statusPill) {
    const isActive = String(agent.status || "").toLowerCase() === "active";
    dom.statusPill.textContent = isActive ? "Compte agent actif" : "Compte agent inactif";
    dom.statusPill.classList.toggle("active", isActive);
    dom.statusPill.classList.toggle("inactive", !isActive);
  }

  if (dom.identity) {
    dom.identity.textContent = [
      agent.displayName || agent.username || "Agent",
      agent.email || agent.phone || agent.uid || "",
    ].filter(Boolean).join(" · ");
  }

  if (dom.budgetRemaining) dom.budgetRemaining.textContent = formatHtg(agent.signupBudgetRemainingHtg);
  if (dom.budgetMeta) dom.budgetMeta.textContent = `Sur un budget initial de ${formatHtg(agent.signupBudgetInitialHtg)}`;
  if (dom.currentMonthEarned) dom.currentMonthEarned.textContent = formatDoes(agent.currentMonthEarnedDoes);
  if (dom.lifetimeEarned) dom.lifetimeEarned.textContent = `Cumul: ${formatDoes(agent.lifetimeEarnedDoes)}`;
  if (dom.trackedSignups) dom.trackedSignups.textContent = formatInt(agent.totalTrackedSignups);
  if (dom.trackedMeta) {
    dom.trackedMeta.textContent = `${formatInt(agent.totalTrackedDeposits)} dépôts suivis · ${formatInt(agent.totalTrackedWins)} victoires suivies`;
  }
  if (dom.promoCode) dom.promoCode.textContent = agent.promoCode || "-";
  if (dom.promoLink) dom.promoLink.textContent = agent.promoLink || "Lien agent indisponible.";

  if (dom.trendHighlight) {
    const latestPoint = trend[trend.length - 1];
    dom.trendHighlight.textContent = latestPoint
      ? `${latestPoint.label} · ${formatDoes(latestPoint.earnedDoes)}`
      : "Aucune clôture mensuelle";
  }
  if (dom.trendHint) {
    const latestPaidMonth = Array.isArray(snapshot.monthlyStatements)
      ? snapshot.monthlyStatements.find((item) => safeInt(item.paidDoes) > 0 || safeInt(item.closedAtMs) > 0)
      : null;
    dom.trendHint.textContent = latestPaidMonth?.monthKey
      ? `Dernier mois payé: ${latestPaidMonth.monthKey}`
      : (agent.lastPayrollMonthKey
        ? `Dernier mois clôturé: ${agent.lastPayrollMonthKey}`
        : "Les mois clôturés apparaitront ici au moment du payroll.");
  }

  renderTrend(trend);
  renderMonthlyStatements(snapshot.monthlyStatements || []);
  renderReferrals(snapshot.recentReferrals || []);
  renderLedger(snapshot.recentLedger || []);
}

async function refreshDashboard() {
  setStatus("Chargement du dashboard agent...");
  try {
    const snapshot = await getMyAgentDashboardSecure();
    renderSnapshot(snapshot || {});
    setStatus("Dashboard agent synchronisé.", "success");
  } catch (error) {
    console.error("[AGENT_DASHBOARD] refresh error", error);
    renderSnapshot({});
    setStatus(error?.message || "Impossible de charger le dashboard agent.", "error");
  }
}

function bindActions() {
  dom.refreshBtn?.addEventListener("click", () => {
    void refreshDashboard();
  });

  dom.copyPromoBtn?.addEventListener("click", async () => {
    const ok = await copyToClipboard(dom.promoCode?.textContent || "");
    setStatus(ok ? "Code promo copié." : "Impossible de copier le code promo.", ok ? "success" : "error");
  });

  dom.copyLinkBtn?.addEventListener("click", async () => {
    const ok = await copyToClipboard(dom.promoLink?.textContent || "");
    setStatus(ok ? "Lien promo copié." : "Impossible de copier le lien promo.", ok ? "success" : "error");
  });
}

async function bootstrap() {
  bindActions();
  watchAuthState((user) => {
    if (!user && !auth.currentUser) {
      window.location.href = "./auth.html";
      return;
    }
    void refreshDashboard();
  });

  if (auth.currentUser) {
    await refreshDashboard();
  }
}

void bootstrap();
