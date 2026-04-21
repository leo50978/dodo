import { db, doc, getDoc } from "./firebase-init.js";
import { getPublicWhatsappModalConfigSecure } from "./secure-functions.js";

const CHAMPIONNAT_DOC_PATH = ["championnats", "mopyon_current"];

const dom = {
  progressFill: document.getElementById("championnatProgressFill"),
  progressCopy: document.getElementById("championnatProgressCopy"),
  standingsCount: document.getElementById("championnatStandingsCount"),
  registerBtn: document.getElementById("championnatRegisterBtn"),
  classementBtn: document.getElementById("championnatClassementBtn"),
  rulesBtn: document.getElementById("championnatRulesBtn"),
  contactModal: document.getElementById("championnatContactModal"),
  contactCloseBtn: document.getElementById("championnatContactCloseBtn"),
  contactCopy: document.getElementById("championnatContactCopy"),
  contactLink: document.getElementById("championnatContactLink"),
  contactCopyBtn: document.getElementById("championnatContactCopyBtn"),
  standingsModal: document.getElementById("championnatStandingsModal"),
  standingsCloseBtn: document.getElementById("championnatStandingsCloseBtn"),
  standingsList: document.getElementById("championnatStandingsList"),
  rulesModal: document.getElementById("championnatRulesModal"),
  rulesCloseBtn: document.getElementById("championnatRulesCloseBtn"),
};

const state = {
  champion: {
    totalSlots: 64,
    registeredCount: 0,
    status: "collecting",
  },
  participants: [],
  contactNumber: "",
  contactMessage: "Bonjour, je veux m'inscrire au championnat Mopyon.",
};

let refreshTimer = null;

function safeInt(value) {
  const num = Number(value);
  return Number.isFinite(num) ? Math.trunc(num) : 0;
}

function escapeHtml(value = "") {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatInt(value) {
  return new Intl.NumberFormat("fr-FR", { maximumFractionDigits: 0 }).format(safeInt(value));
}

function badgeClass(status = "") {
  const value = String(status || "").toLowerCase();
  if (value === "ready" || value === "running" || value === "registered" || value === "qualified") return "good";
  if (value === "finished" || value === "eliminated" || value === "forfeit" || value === "late_disqualified") return "bad";
  return "warn";
}

function statusLabel(status = "") {
  const value = String(status || "").toLowerCase();
  if (value === "collecting") return "Collecte";
  if (value === "ready") return "Prêt";
  if (value === "running") return "En cours";
  if (value === "finished") return "Terminé";
  if (value === "registered") return "Inscrit";
  if (value === "qualified") return "Qualifié";
  if (value === "in_match") return "En match";
  if (value === "eliminated") return "Éliminé";
  if (value === "late_disqualified") return "Forfait retard";
  if (value === "forfeit") return "Forfait";
  return value || "Inconnu";
}

function roundLabel(round = "") {
  const value = String(round || "").toLowerCase();
  const map = {
    registered: "Inscrit",
    round_of_64: "64e",
    round_of_32: "32e",
    round_of_16: "16e",
    quarter_final: "Quart",
    semi_final: "Demi",
    final: "Finale",
  };
  return map[value] || "À venir";
}

function normalizeSnapshot(payload = {}) {
  const champion = payload.champion || payload.championship || payload.summary || {};
  return {
    champion: {
      totalSlots: safeInt(champion.totalSlots || payload.totalSlots || 64) || 64,
      registeredCount: safeInt(champion.registeredCount ?? payload.registeredCount ?? payload.count ?? 0),
      status: String(champion.status || payload.status || "collecting"),
    },
    participants: Array.isArray(payload.participants || champion.participants)
      ? [...(payload.participants || champion.participants)]
      : [],
  };
}

function getTotalSlots() {
  return safeInt(state.champion?.totalSlots || 64) || 64;
}

function getRegisteredCount() {
  return safeInt(state.champion?.registeredCount || state.participants.length || 0);
}

function updateProgress() {
  const registered = getRegisteredCount();
  const total = getTotalSlots();
  const pct = total > 0 ? Math.min(100, Math.round((registered / total) * 100)) : 0;
  if (dom.progressFill) dom.progressFill.style.width = `${pct}%`;
  if (dom.progressCopy) dom.progressCopy.textContent = `${formatInt(registered)} / ${formatInt(total)} participants validés.`;
  if (dom.standingsCount) dom.standingsCount.textContent = `${formatInt(registered)} inscrits`;
}

function renderStandings() {
  if (!dom.standingsList) return;
  const participants = [...(state.participants || [])].sort((a, b) => {
    const rankA = safeInt(a.rank || a.position || a.seed || 9999);
    const rankB = safeInt(b.rank || b.position || b.seed || 9999);
    if (rankA !== rankB) return rankA - rankB;
    return String(a.displayName || a.username || a.uid || "").localeCompare(String(b.displayName || b.username || b.uid || ""));
  });

  if (!participants.length) {
    dom.standingsList.innerHTML = `<div class="empty">Aucun participant enregistré pour le moment.</div>`;
    return;
  }

  dom.standingsList.innerHTML = participants.slice(0, 64).map((participant, index) => `
    <article class="standing-row">
      <div class="row-top">
        <div>
          <p class="row-title">#${formatInt(participant.rank || index + 1)} · ${escapeHtml(participant.displayName || participant.username || participant.uid || "Joueur")}</p>
          <p class="row-sub">${escapeHtml(participant.uid || participant.userId || "UID inconnu")} · ${escapeHtml(participant.note || "Inscription validée")}</p>
        </div>
        <span class="badge ${badgeClass(participant.status)}">${escapeHtml(statusLabel(participant.status))}</span>
      </div>
      <div class="inline-note">Tour: ${escapeHtml(roundLabel(participant.round || "registered"))}</div>
    </article>
  `).join("");
}

function render() {
  updateProgress();
  renderStandings();
}

function openModal(modal) {
  if (!modal) return;
  modal.classList.add("is-open");
  modal.setAttribute("aria-hidden", "false");
}

function closeModal(modal) {
  if (!modal) return;
  modal.classList.remove("is-open");
  modal.setAttribute("aria-hidden", "true");
}

function makeWhatsAppUrl(phoneDigits, message) {
  const phone = String(phoneDigits || "").replace(/\D/g, "");
  const text = encodeURIComponent(String(message || ""));
  if (!phone) return "#";
  return `https://wa.me/${phone}${text ? `?text=${text}` : ""}`;
}

function getContactDigits(payload = {}) {
  const contacts = payload?.contacts && typeof payload.contacts === "object" ? payload.contacts : payload || {};
  const candidates = [
    contacts.championnat_mopyon,
    contacts.championnat,
    contacts.agent_deposit,
    contacts.support_default,
    contacts.welcome_deposit_modal,
  ];
  return String(candidates.find((item) => String(item || "").trim()) || "").replace(/\D/g, "");
}

async function loadSnapshot() {
  try {
    const remoteDoc = await getDoc(doc(db, ...CHAMPIONNAT_DOC_PATH));
    if (remoteDoc.exists()) {
      const raw = remoteDoc.data() || {};
      const remoteSnapshot = {
        ...normalizeSnapshot(raw),
        updatedAtMs: safeInt(raw.updatedAtMs || raw.updatedAt?.toMillis?.() || Date.now()),
      };
      state.champion = remoteSnapshot.champion;
      state.participants = Array.isArray(remoteSnapshot.participants) ? remoteSnapshot.participants : [];
      render();
    } else {
      state.champion = normalizeSnapshot({}).champion;
      state.participants = [];
      render();
      console.warn("[CHAMPIONNAT_PUBLIC] snapshot absent");
    }
  } catch (error) {
    console.error("[CHAMPIONNAT_PUBLIC] snapshot load failed", error);
  }
}

async function loadContactChannel() {
  try {
    const response = await getPublicWhatsappModalConfigSecure({});
    state.contactNumber = getContactDigits(response || {});
  } catch (error) {
    console.error("[CHAMPIONNAT_PUBLIC] contact config load failed", error);
    state.contactNumber = "";
  }

  if (dom.contactCopy) {
    dom.contactCopy.textContent = state.contactNumber
      ? `Numéro de contact: ${state.contactNumber}. Envoie ton message au sujet du championnat Mopyon.`
      : "Le numéro de contact n'est pas encore chargé. L'agent restera joignable via le canal configuré.";
  }
  if (dom.contactLink) {
    const hasNumber = !!state.contactNumber;
    dom.contactLink.href = hasNumber ? makeWhatsAppUrl(state.contactNumber, state.contactMessage) : "#";
    dom.contactLink.setAttribute("aria-disabled", hasNumber ? "false" : "true");
    dom.contactLink.style.pointerEvents = hasNumber ? "" : "none";
    dom.contactLink.style.opacity = hasNumber ? "" : "0.5";
    dom.contactLink.textContent = hasNumber ? "Contacter un agent" : "Numéro indisponible";
  }
}

async function copyContactNumber() {
  if (!state.contactNumber) return;
  try {
    await navigator.clipboard.writeText(state.contactNumber);
    if (dom.contactCopy) dom.contactCopy.textContent = "Numéro copié dans le presse-papiers.";
  } catch (_) {
    if (dom.contactCopy) dom.contactCopy.textContent = `Numéro: ${state.contactNumber}`;
  }
}

function openContactModal() {
  void loadContactChannel().then(() => openModal(dom.contactModal));
}

function openStandingsModal() {
  renderStandings();
  openModal(dom.standingsModal);
}

function openRulesModal() {
  openModal(dom.rulesModal);
}

function bindEvents() {
  dom.registerBtn?.addEventListener("click", openContactModal);
  dom.classementBtn?.addEventListener("click", openStandingsModal);
  dom.rulesBtn?.addEventListener("click", openRulesModal);

  dom.contactCloseBtn?.addEventListener("click", () => closeModal(dom.contactModal));
  dom.contactCopyBtn?.addEventListener("click", copyContactNumber);
  dom.contactModal?.addEventListener("click", (event) => {
    if (event.target === dom.contactModal) closeModal(dom.contactModal);
  });

  dom.standingsCloseBtn?.addEventListener("click", () => closeModal(dom.standingsModal));
  dom.standingsModal?.addEventListener("click", (event) => {
    if (event.target === dom.standingsModal) closeModal(dom.standingsModal);
  });

  dom.rulesCloseBtn?.addEventListener("click", () => closeModal(dom.rulesModal));
  dom.rulesModal?.addEventListener("click", (event) => {
    if (event.target === dom.rulesModal) closeModal(dom.rulesModal);
  });

}

async function boot() {
  bindEvents();
  if (typeof window !== "undefined") {
    if (refreshTimer) window.clearInterval(refreshTimer);
    refreshTimer = window.setInterval(() => {
      if (document.visibilityState === "visible") {
        void loadSnapshot();
      }
    }, 30000);

  }
  await Promise.all([loadSnapshot(), loadContactChannel()]);
}

boot();
