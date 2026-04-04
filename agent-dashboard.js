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
  heroPayrollMonth: document.getElementById("agentHeroPayrollMonth"),
  heroLastSeen: document.getElementById("agentHeroLastSeen"),
  heroActivatedAt: document.getElementById("agentHeroActivatedAt"),
  heroPromo: document.getElementById("agentHeroPromo"),
  trendHighlight: document.getElementById("agentTrendHighlight"),
  trendSvg: document.getElementById("agentTrendSvg"),
  trendHint: document.getElementById("agentTrendHint"),
  networkSvg: document.getElementById("agentNetworkSvg"),
  networkLegend: document.getElementById("agentNetworkLegend"),
  networkHint: document.getElementById("agentNetworkHint"),
  volumeSvg: document.getElementById("agentVolumeSvg"),
  volumeHint: document.getElementById("agentVolumeHint"),
  monthlyBody: document.getElementById("agentMonthlyTableBody"),
  monthlyCards: document.getElementById("agentMonthlyCards"),
  monthlyEmpty: document.getElementById("agentMonthlyEmpty"),
  referralsList: document.getElementById("agentReferralsList"),
  referralsEmpty: document.getElementById("agentReferralsEmpty"),
  ledgerList: document.getElementById("agentLedgerList"),
  ledgerEmpty: document.getElementById("agentLedgerEmpty"),
};

let buttonFeedbackTimer = 0;

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

function formatDateShort(ms) {
  const safeMs = safeInt(ms);
  if (!safeMs) return "-";
  return new Date(safeMs).toLocaleDateString("fr-FR", {
    day: "2-digit",
    month: "short",
    year: "numeric",
  });
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

function resetCopyButtons() {
  [dom.copyPromoBtn, dom.copyLinkBtn].forEach((button) => {
    if (!button) return;
    button.classList.remove("success", "error");
    button.disabled = false;
    if (button.dataset.defaultLabel) {
      button.textContent = button.dataset.defaultLabel;
    }
  });
}

function showCopyFeedback(button, success, successLabel, errorLabel) {
  if (!button) return;
  if (buttonFeedbackTimer) {
    window.clearTimeout(buttonFeedbackTimer);
    buttonFeedbackTimer = 0;
  }
  resetCopyButtons();
  button.classList.add(success ? "success" : "error");
  button.textContent = success ? successLabel : errorLabel;
  button.disabled = true;
  buttonFeedbackTimer = window.setTimeout(() => {
    resetCopyButtons();
  }, 1600);
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

function renderNetworkBreakdown(agent = {}) {
  const items = [
    {
      key: "signups",
      label: "Inscriptions suivies",
      value: safeInt(agent.totalTrackedSignups),
      color: "#69d2ff",
      helper: "Personnes rattachées à ton code promo",
    },
    {
      key: "deposits",
      label: "Dépôts suivis",
      value: safeInt(agent.totalTrackedDeposits),
      color: "#6ef0b4",
      helper: "Filleuls ayant déjà validé un dépôt",
    },
    {
      key: "wins",
      label: "Victoires suivies",
      value: safeInt(agent.totalTrackedWins),
      color: "#ffb86c",
      helper: "Parties gagnées qui t’ont rapporté",
    },
  ];

  const total = items.reduce((sum, item) => sum + item.value, 0);
  if (dom.networkLegend) {
    dom.networkLegend.innerHTML = items.map((item) => `
      <div class="legend-item">
        <span class="legend-dot" style="background:${item.color};"></span>
        <div>
          <strong>${escapeHtml(item.label)}</strong>
          <div class="subvalue" style="margin-top:4px;">${escapeHtml(item.helper)}</div>
        </div>
        <span>${formatInt(item.value)}</span>
      </div>
    `).join("");
  }

  if (!dom.networkSvg) return;
  if (total <= 0) {
    dom.networkSvg.innerHTML = `
      <circle cx="120" cy="108" r="68" fill="none" stroke="rgba(255,255,255,0.08)" stroke-width="22"></circle>
      <circle cx="120" cy="108" r="68" fill="none" stroke="rgba(105,210,255,0.18)" stroke-width="22" stroke-dasharray="140 288" transform="rotate(-90 120 108)"></circle>
      <text x="120" y="104" text-anchor="middle" fill="#f4f7ff" font-size="20" font-weight="800">0</text>
      <text x="120" y="126" text-anchor="middle" fill="rgba(255,255,255,0.58)" font-size="12">activité suivie</text>
    `;
    if (dom.networkHint) {
      dom.networkHint.textContent = "Dès que ton réseau commence à convertir, le diagramme se remplit automatiquement.";
    }
    return;
  }

  const radius = 68;
  const circumference = 2 * Math.PI * radius;
  let offset = 0;
  const arcs = items.map((item) => {
    const ratio = item.value / total;
    const segment = Math.max(0, ratio * circumference);
    const markup = `
      <circle
        cx="120"
        cy="108"
        r="${radius}"
        fill="none"
        stroke="${item.color}"
        stroke-width="22"
        stroke-linecap="round"
        stroke-dasharray="${segment} ${circumference - segment}"
        stroke-dashoffset="${-offset}"
        transform="rotate(-90 120 108)"
      ></circle>
    `;
    offset += segment;
    return markup;
  }).join("");

  dom.networkSvg.innerHTML = `
    <circle cx="120" cy="108" r="${radius}" fill="none" stroke="rgba(255,255,255,0.08)" stroke-width="22"></circle>
    ${arcs}
    <circle cx="120" cy="108" r="44" fill="rgba(6,16,31,0.9)"></circle>
    <text x="120" y="100" text-anchor="middle" fill="#f4f7ff" font-size="23" font-weight="800">${escapeHtml(formatInt(total))}</text>
    <text x="120" y="122" text-anchor="middle" fill="rgba(255,255,255,0.58)" font-size="12">points suivis</text>
  `;
  if (dom.networkHint) {
    const conversion = items[0].value > 0 ? Math.round((items[1].value / Math.max(1, items[0].value)) * 100) : 0;
    dom.networkHint.textContent = `Taux de transformation inscription vers dépôt: ${conversion}%`;
  }
}

function renderMonthlyVolume(monthlyStatements = []) {
  if (!dom.volumeSvg) return;
  const items = Array.isArray(monthlyStatements)
    ? monthlyStatements
      .slice(0, 6)
      .slice()
      .reverse()
    : [];

  if (items.length === 0) {
    dom.volumeSvg.innerHTML = `
      <rect x="0" y="0" width="720" height="220" fill="rgba(255,255,255,0.02)"></rect>
      <text x="24" y="34" fill="rgba(255,255,255,0.58)" font-size="14">Aucun volume mensuel disponible pour le moment.</text>
    `;
    if (dom.volumeHint) {
      dom.volumeHint.textContent = "Les barres apparaitront ici après les premières clôtures mensuelles.";
    }
    return;
  }

  const width = 720;
  const height = 220;
  const leftPad = 28;
  const rightPad = 18;
  const topPad = 24;
  const bottomPad = 34;
  const chartWidth = width - leftPad - rightPad;
  const chartHeight = height - topPad - bottomPad;
  const maxValue = Math.max(
    1,
    ...items.map((item) => Math.max(
      safeInt(item.signupsCount),
      safeInt(item.depositsCount),
      safeInt(item.winsCount),
    )),
  );
  const groupWidth = chartWidth / Math.max(1, items.length);
  const barWidth = Math.min(24, Math.max(10, (groupWidth - 18) / 3));
  const colors = {
    signups: "#69d2ff",
    deposits: "#6ef0b4",
    wins: "#ffb86c",
  };

  const bars = items.map((item, index) => {
    const baseX = leftPad + (index * groupWidth) + (groupWidth / 2) - (barWidth * 1.5) - 6;
    const metrics = [
      { key: "signupsCount", color: colors.signups },
      { key: "depositsCount", color: colors.deposits },
      { key: "winsCount", color: colors.wins },
    ];
    return metrics.map((metric, metricIndex) => {
      const value = safeInt(item[metric.key]);
      const barHeight = (value / maxValue) * chartHeight;
      const x = baseX + metricIndex * (barWidth + 6);
      const y = topPad + chartHeight - barHeight;
      return `<rect x="${x}" y="${y}" width="${barWidth}" height="${barHeight}" rx="8" fill="${metric.color}" opacity="0.92"></rect>`;
    }).join("");
  }).join("");

  const labels = items.map((item, index) => {
    const x = leftPad + (index * groupWidth) + (groupWidth / 2);
    return `<text x="${x}" y="${height - 10}" text-anchor="middle" fill="rgba(255,255,255,0.58)" font-size="12">${escapeHtml(String(item.monthKey || ""))}</text>`;
  }).join("");

  dom.volumeSvg.innerHTML = `
    <rect x="0" y="0" width="${width}" height="${height}" fill="rgba(255,255,255,0.02)"></rect>
    <line x1="${leftPad}" y1="${topPad + chartHeight}" x2="${width - rightPad}" y2="${topPad + chartHeight}" stroke="rgba(255,255,255,0.08)" stroke-width="1"></line>
    <line x1="${leftPad}" y1="${topPad}" x2="${leftPad}" y2="${topPad + chartHeight}" stroke="rgba(255,255,255,0.05)" stroke-width="1"></line>
    ${bars}
    ${labels}
  `;

  if (dom.volumeHint) {
    const latest = items[items.length - 1];
    dom.volumeHint.textContent = latest
      ? `${latest.monthKey} · ${formatInt(latest.signupsCount)} inscriptions · ${formatInt(latest.depositsCount)} dépôts · ${formatInt(latest.winsCount)} victoires`
      : "Le mois le plus récent apparait à droite.";
  }
}

function renderMonthlyStatements(items = []) {
  if (!dom.monthlyBody || !dom.monthlyEmpty || !dom.monthlyCards) return;
  if (!Array.isArray(items) || items.length === 0) {
    dom.monthlyBody.innerHTML = "";
    dom.monthlyCards.innerHTML = "";
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
  dom.monthlyCards.innerHTML = items.map((item) => `
    <article class="statement-card">
      <div class="statement-card-head">
        <strong>${escapeHtml(item.monthKey || "-")}</strong>
        <span class="pill ${item.closedAtMs ? "active" : "inactive"}">${escapeHtml(item.closedAtMs ? "Clôturé" : "Ouvert")}</span>
      </div>
      <div class="statement-grid">
        <div class="statement-cell">
          <strong>Gains</strong>
          <span>${formatDoes(item.earnedDoes)}</span>
        </div>
        <div class="statement-cell">
          <strong>Payé</strong>
          <span>${formatDoes(item.paidDoes)}</span>
        </div>
        <div class="statement-cell">
          <strong>Inscriptions</strong>
          <span>${formatInt(item.signupsCount)}</span>
        </div>
        <div class="statement-cell">
          <strong>Dépôts</strong>
          <span>${formatInt(item.depositsCount)}</span>
        </div>
        <div class="statement-cell">
          <strong>Victoires</strong>
          <span>${formatInt(item.winsCount)}</span>
        </div>
        <div class="statement-cell">
          <strong>Statut</strong>
          <span>${escapeHtml(item.closedAtMs ? formatDateShort(item.closedAtMs) : "En cours")}</span>
        </div>
      </div>
    </article>
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
  const monthlyStatements = Array.isArray(snapshot.monthlyStatements) ? snapshot.monthlyStatements : [];

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
  if (dom.heroPromo) dom.heroPromo.textContent = agent.promoCode || "-";
  if (dom.heroPayrollMonth) {
    dom.heroPayrollMonth.textContent = agent.lastPayrollMonthKey
      ? `Dernier mois payé: ${agent.lastPayrollMonthKey}`
      : "Aucun mois clôturé";
  }
  if (dom.heroLastSeen) dom.heroLastSeen.textContent = formatDateTime(agent.lastSeenAtMs);
  if (dom.heroActivatedAt) dom.heroActivatedAt.textContent = formatDateShort(agent.activatedAtMs || agent.declaredAtMs);

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
  renderNetworkBreakdown(agent);
  renderMonthlyVolume(monthlyStatements);
  renderMonthlyStatements(monthlyStatements);
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
  [dom.copyPromoBtn, dom.copyLinkBtn].forEach((button) => {
    if (!button) return;
    button.dataset.defaultLabel = button.textContent || "";
  });

  dom.refreshBtn?.addEventListener("click", () => {
    void refreshDashboard();
  });

  dom.copyPromoBtn?.addEventListener("click", async () => {
    const ok = await copyToClipboard(dom.promoCode?.textContent || "");
    showCopyFeedback(dom.copyPromoBtn, ok, "Code copié", "Échec copie");
    setStatus(ok ? "Code promo copié." : "Impossible de copier le code promo.", ok ? "success" : "error");
  });

  dom.copyLinkBtn?.addEventListener("click", async () => {
    const ok = await copyToClipboard(dom.promoLink?.textContent || "");
    showCopyFeedback(dom.copyLinkBtn, ok, "Lien copié", "Échec copie");
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
