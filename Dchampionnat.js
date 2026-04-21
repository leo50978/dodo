import { ensureFinanceDashboardSession } from "./dashboard-admin-auth.js";
import {
  db,
  serverTimestamp,
  doc,
  getDoc,
  getDocs,
  collection,
  query,
  where,
  orderBy,
  limit,
  runTransaction,
} from "./firebase-init.js";

const dom = {
  status: document.getElementById("championnatStatus"),
  searchInput: document.getElementById("championnatSearchInput"),
  searchBtn: document.getElementById("championnatSearchBtn"),
  searchResults: document.getElementById("championnatSearchResults"),
  participantsList: document.getElementById("championnatParticipantsList"),
};

const CLIENTS_COLLECTION = "clients";
const CHAMPIONNAT_DOC_PATH = ["championnats", "mopyon_current"];

const state = {
  query: "",
  champion: {
    totalSlots: 64,
    registeredCount: 0,
  },
  participants: [],
  results: [],
  busy: false,
};

function escapeHtml(value = "") {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function safeInt(value) {
  const num = Number(value);
  return Number.isFinite(num) ? Math.trunc(num) : 0;
}

function formatInt(value) {
  return new Intl.NumberFormat("fr-FR", { maximumFractionDigits: 0 }).format(safeInt(value));
}

function setStatus(message = "", tone = "neutral") {
  if (!dom.status) return;
  dom.status.textContent = message;
  dom.status.dataset.tone = tone;
}

function normalizeSnapshot(payload = {}) {
  const champion = payload.champion || payload.summary || {};
  return {
    champion: {
      totalSlots: safeInt(champion.totalSlots || payload.totalSlots || 64) || 64,
      registeredCount: safeInt(champion.registeredCount ?? payload.registeredCount ?? 0),
    },
    participants: Array.isArray(payload.participants || champion.participants)
      ? [...(payload.participants || champion.participants)]
      : [],
  };
}

function normalizeSearchItem(item = {}) {
  const uid = String(item.uid || item.userId || item.id || "").trim();
  const displayName = String(item.displayName || item.username || item.name || item.email || uid || "Utilisateur");
  return {
    uid,
    displayName,
    username: String(item.username || ""),
    email: String(item.email || ""),
    phone: String(item.phone || ""),
    paymentStatus: String(item.paymentStatus || item.status || "candidate"),
  };
}

function normalizeSearchText(value = "") {
  return String(value || "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function sanitizePhone(value = "") {
  return String(value || "").replace(/\D/g, "");
}

function renderParticipants(items = []) {
  if (!dom.participantsList) return;

  if (!Array.isArray(items) || items.length === 0) {
    dom.participantsList.innerHTML = `<div class="empty">Aucun participant ajouté pour le moment.</div>`;
    return;
  }

  dom.participantsList.innerHTML = items.map((item, index) => {
    const uid = String(item.uid || item.userId || item.id || "");
    const displayName = String(item.displayName || item.username || item.name || item.email || uid || "Utilisateur");
    const subline = String(item.email || item.phone || uid || "-");
    const status = String(item.status || item.paymentStatus || "registered").toLowerCase();
    const badgeClass = status === "registered" || status === "approved" ? "good" : "warn";
    const badgeText = status === "registered" || status === "approved" ? "Inscrit" : status;

    return `
      <article class="result-card">
        <div class="row-top">
          <div>
            <p class="row-title">#${formatInt(item.rank || index + 1)} · ${escapeHtml(displayName)}</p>
            <p class="row-sub">${escapeHtml(subline)}</p>
          </div>
          <span class="badge ${badgeClass}">${escapeHtml(badgeText)}</span>
        </div>
      </article>
    `;
  }).join("");
}

async function directFirestoreSearch(queryText = "") {
  const rawQuery = String(queryText || "").trim();
  const normalizedQuery = normalizeSearchText(rawQuery);
  const queryDigits = sanitizePhone(rawQuery);
  const queryEmail = rawQuery.includes("@") ? rawQuery.toLowerCase() : "";
  const queryUsername = rawQuery.replace(/\s+/g, "").toLowerCase();
  const results = new Map();

  if (!rawQuery) return [];

  const addItem = (docSnap) => {
    if (!docSnap?.exists) return;
    const raw = docSnap.data() || {};
    const item = normalizeSearchItem({
      uid: docSnap.id,
      displayName: raw.displayName || raw.username || raw.name || raw.email || docSnap.id,
      username: raw.username || "",
      email: raw.email || "",
      phone: raw.phone || raw.customerPhone || "",
      paymentStatus: raw.paymentStatus || raw.status || "candidate",
    });
    const key = item.uid || `${item.displayName}-${item.email}-${item.phone}`;
    if (key) results.set(key, item);
  };

  const exactLookups = [];
  if (rawQuery.length >= 8) {
    exactLookups.push(getDoc(doc(db, CLIENTS_COLLECTION, rawQuery)));
  }
  if (queryEmail) {
    exactLookups.push(getDocs(query(collection(db, CLIENTS_COLLECTION), where("email", "==", queryEmail), limit(6))));
  }
  if (queryUsername) {
    exactLookups.push(getDocs(query(collection(db, CLIENTS_COLLECTION), where("username", "==", queryUsername), limit(6))));
  }
  if (queryDigits.length >= 8) {
    exactLookups.push(getDocs(query(collection(db, CLIENTS_COLLECTION), where("phone", "==", queryDigits), limit(6))));
  }

  const exactSnaps = await Promise.allSettled(exactLookups);
  exactSnaps.forEach((entry) => {
    if (entry.status !== "fulfilled") return;
    const snap = entry.value;
    if (!snap) return;
    if (typeof snap.forEach === "function") {
      snap.forEach((docSnap) => addItem(docSnap));
    } else {
      addItem(snap);
    }
  });

  if (results.size < 12 && normalizedQuery.length >= 2) {
    let fallbackSnap = null;
    try {
      fallbackSnap = await getDocs(query(collection(db, CLIENTS_COLLECTION), orderBy("lastSeenAtMs", "desc"), limit(250)));
    } catch (_) {
      fallbackSnap = await getDocs(query(collection(db, CLIENTS_COLLECTION), limit(250)));
    }

    fallbackSnap.forEach((docSnap) => {
      if (results.size >= 12) return;
      const raw = docSnap.data() || {};
      const haystack = [
        docSnap.id,
        raw.uid,
        raw.name,
        raw.displayName,
        raw.username,
        raw.email,
        raw.phone,
        raw.customerPhone,
      ]
        .map((value) => normalizeSearchText(value))
        .filter(Boolean)
        .join(" ");
      const phoneHaystack = [
        sanitizePhone(raw.phone || ""),
        sanitizePhone(raw.customerPhone || ""),
      ].filter(Boolean).join(" ");

      const match = haystack.includes(normalizedQuery)
        || (queryDigits.length >= 4 && phoneHaystack.includes(queryDigits));
      if (match) addItem(docSnap);
    });
  }

  return Array.from(results.values()).slice(0, 12);
}

function renderResults(items = []) {
  if (!dom.searchResults) return;

  if (!Array.isArray(items) || items.length === 0) {
    dom.searchResults.innerHTML = `<div class="empty">Aucun utilisateur trouvé. Essaie un nom, un email ou un numéro.</div>`;
    return;
  }

  dom.searchResults.innerHTML = items.map((item) => {
    const uid = String(item.uid || item.userId || item.id || "");
    const displayName = String(item.displayName || item.username || item.name || item.email || uid || "Utilisateur");
    const subline = String(item.email || item.phone || uid || "-");
    const paymentStatus = String(item.paymentStatus || item.status || "candidate").toLowerCase();
    const badgeClass = paymentStatus === "approved" || paymentStatus === "registered" ? "good" : "warn";
    const badgeText = paymentStatus === "approved" || paymentStatus === "registered"
      ? "Prêt à inscrire"
      : String(item.paymentStatus || item.status || "Candidat");

    return `
      <article class="result-card">
        <div class="row-top">
          <div>
            <p class="row-title">${escapeHtml(displayName)}</p>
            <p class="row-sub">${escapeHtml(subline)}</p>
          </div>
          <span class="badge ${badgeClass}">${escapeHtml(badgeText)}</span>
        </div>
        <div class="toolbar">
          <button
            class="primary-btn"
            type="button"
            data-action="add"
            data-uid="${escapeHtml(uid)}"
            data-display-name="${escapeHtml(displayName)}"
            data-username="${escapeHtml(item.username || "")}"
            data-email="${escapeHtml(item.email || "")}"
            data-phone="${escapeHtml(item.phone || "")}"
          >Ajouter au championnat</button>
        </div>
      </article>
    `;
  }).join("");
}

async function refreshSnapshot() {
  try {
    const response = await getDoc(doc(db, ...CHAMPIONNAT_DOC_PATH));
    const snapshot = normalizeSnapshot(response?.exists() ? response.data() || {} : {});
    state.champion = snapshot.champion;
    state.participants = Array.isArray(snapshot.participants) ? snapshot.participants.map(normalizeSearchItem) : [];
    renderParticipants(state.participants);
  } catch (error) {
    console.error("[CHAMPIONNAT_DASHBOARD] snapshot load failed", error);
    state.champion = normalizeSnapshot({}).champion;
    state.participants = [];
    renderParticipants([]);
    setStatus(error?.message || "Impossible de charger le championnat.", "error");
  }
}

async function runSearch() {
  const query = String(dom.searchInput?.value || "").trim();
  state.query = query;

  if (!query) {
    state.results = [];
    renderResults([]);
    setStatus("Entre un nom, un email, un téléphone ou un UID.", "warn");
    return;
  }

  setStatus("Recherche en cours...", "neutral");
  state.busy = true;
  dom.searchBtn && (dom.searchBtn.disabled = true);

  try {
    let items = [];
    try {
      items = await directFirestoreSearch(query);
    } catch (firestoreError) {
      console.warn("[CHAMPIONNAT_DASHBOARD] direct firestore search failed", firestoreError);
    }

    state.results = items;
    renderResults(items);
    setStatus(`${formatInt(items.length)} résultat${items.length > 1 ? "s" : ""} trouvé${items.length > 1 ? "s" : ""}.`, "success");
  } catch (error) {
    state.results = [];
    renderResults([]);
    setStatus(error?.message || "Backend indisponible pour la recherche championnat.", "error");
  } finally {
    state.busy = false;
    dom.searchBtn && (dom.searchBtn.disabled = false);
  }
}

async function addParticipant(payload = {}) {
  const uid = String(payload.uid || "").trim();
  if (!uid) {
    setStatus("UID manquant pour l'ajout.", "error");
    return;
  }

  setStatus("Ajout du joueur au championnat...", "neutral");
  try {
    const ref = doc(db, ...CHAMPIONNAT_DOC_PATH);
    const result = await runTransaction(db, async (transaction) => {
      const snap = await transaction.get(ref);
      const current = normalizeSnapshot(snap.exists() ? snap.data() || {} : {});
      const participants = Array.isArray(current.participants) ? [...current.participants] : [];
      const nextParticipant = normalizeSearchItem({
        uid,
        displayName: payload.displayName || uid,
        username: payload.username || "",
        email: payload.email || "",
        phone: payload.phone || "",
        paymentStatus: "approved",
        note: "Paiement validé par l'agent.",
      });

      const existingIndex = participants.findIndex((item) => String(item.uid || "") === uid);
      if (existingIndex >= 0) {
        participants[existingIndex] = {
          ...participants[existingIndex],
          ...nextParticipant,
          status: "registered",
          round: participants[existingIndex].round || "registered",
        };
      } else {
        participants.push({
          ...nextParticipant,
          status: "registered",
          round: "registered",
        });
      }

      const champion = {
        ...current.champion,
        totalSlots: 64,
        registeredCount: participants.length,
        status: participants.length >= 64 ? "ready" : "collecting",
      };
      const nextSnapshot = {
        champion,
        participants: participants
          .slice(0, 64)
          .map((item, index) => ({
            ...item,
            rank: safeInt(item.rank || index + 1) || index + 1,
            position: safeInt(item.position || item.rank || index + 1) || index + 1,
            seed: safeInt(item.seed || item.rank || index + 1) || index + 1,
            updatedAtMs: Date.now(),
          })),
        updatedAtMs: Date.now(),
      };

      transaction.set(ref, {
        ...nextSnapshot,
        updatedAt: serverTimestamp(),
      }, { merge: true });

      return { champion: nextSnapshot.champion, participants: nextSnapshot.participants, participant: nextParticipant };
    });

    state.champion = result.champion;
    state.participants = Array.isArray(result.participants) ? result.participants.map(normalizeSearchItem) : [];
    renderParticipants(state.participants);

    await refreshSnapshot();
    setStatus(
      result?.participant?.displayName
        ? `${result.participant.displayName} a été ajouté au championnat.`
        : "Le joueur a été ajouté au championnat.",
      "success"
    );
  } catch (error) {
    setStatus(error?.message || "Impossible d'ajouter ce joueur.", "error");
  }
}

function bindEvents() {
  dom.searchBtn?.addEventListener("click", () => void runSearch());
  dom.searchInput?.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      void runSearch();
    }
  });

  dom.searchResults?.addEventListener("click", (event) => {
    const button = event.target.closest("[data-action='add']");
    if (!button) return;
    void addParticipant({
      uid: button.dataset.uid || "",
      displayName: button.dataset.displayName || "",
      username: button.dataset.username || "",
      email: button.dataset.email || "",
      phone: button.dataset.phone || "",
    });
  });
}

async function boot() {
  await ensureFinanceDashboardSession({
    title: "Championnat Mopyon",
    description: "Cherche un joueur, puis ajoute-le directement au championnat.",
  });

  bindEvents();
  renderResults([]);
  renderParticipants([]);
  await refreshSnapshot();
  setStatus("Cherche un utilisateur pour l'ajouter au championnat.", "neutral");
}

boot().catch((error) => {
  console.error("[CHAMPIONNAT_DASHBOARD] bootstrap failed", error);
  setStatus(error?.message || "Le dashboard championnat n'a pas pu démarrer.", "error");
});
