import { auth, db, collection, doc, getDoc, getDocs, limit, onAuthStateChanged, orderBy, query } from "../firebase-init.js";
import { startAfter } from "https://www.gstatic.com/firebasejs/10.12.5/firebase-firestore.js";
import { getDepositFundingStatusSecure } from "../secure-functions.js";
import { logoutCurrentUser } from "../auth.js";
import { buildWhatsappUrlForKey, getWhatsappContactLabel, refreshWhatsappModalContacts } from "../whatsapp-modal-config.js";

if (window.lucide) {
  window.lucide.createIcons();
}

function safeText(value, fallback = "-") {
  const out = String(value || "").trim();
  return out || fallback;
}

function formatHtg(value) {
  const amount = Number(value || 0);
  return `${new Intl.NumberFormat("fr-FR", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(amount)} HTG`;
}

function pickFirstFiniteNumber(...values) {
  for (const value of values) {
    const amount = Number(value);
    if (Number.isFinite(amount)) return amount;
  }
  return null;
}

function getBestBalance(...sources) {
  for (const source of sources) {
    if (!source) continue;
    const playable = pickFirstFiniteNumber(source.playableHtg, source.availableGourdes);
    if (Number.isFinite(playable) && playable >= 0) return playable;

    const approved = pickFirstFiniteNumber(source.approvedHtgAvailable, source.approvedGourdesAvailable);
    const provisional = pickFirstFiniteNumber(source.provisionalHtgAvailable, source.provisionalGourdesAvailable);
    if (Number.isFinite(approved) || Number.isFinite(provisional)) {
      return Math.max(
        0,
        (Number.isFinite(approved) ? approved : 0) + (Number.isFinite(provisional) ? provisional : 0)
      );
    }

    const withdrawable = pickFirstFiniteNumber(source.withdrawableHtg);
    if (Number.isFinite(withdrawable) && withdrawable >= 0) return withdrawable;
  }

  return null;
}

function getDisplayName(user, clientData = {}) {
  return safeText(
    clientData.name
    || clientData.displayName
    || clientData.username
    || user?.displayName
    || (user?.email ? user.email.split("@")[0] : ""),
    "Player"
  );
}

function getDisplayUsername(user, clientData = {}) {
  return safeText(
    clientData.username
    || "Username",
    "Username"
  );
}

function getDisplayEmail(user, clientData = {}) {
  return safeText(
    clientData.email
    || user?.email
    || "",
    ""
  );
}

function getDisplayPhone(user, clientData = {}) {
  return safeText(clientData.phone || clientData.customerPhone || "", "");
}

function getDisplayIdentifier(user, clientData = {}) {
  return safeText(
    clientData.username
    || clientData.email
    || user?.email
    || user?.uid
    || "",
    "Identifiant indisponible"
  );
}

function getSidebarStatus(funding = {}, clientData = {}) {
  if (funding.accountFrozen === true || clientData.accountFrozen === true) return "COMPTE GELÉ";
  if (funding.withdrawalHold === true || clientData.withdrawalHold === true) return "RETRAIT GELÉ";
  const approved = Number(funding?.approvedHtgAvailable || clientData?.approvedHtgAvailable || 0);
  const provisional = Number(funding?.provisionalHtgAvailable || clientData?.provisionalHtgAvailable || 0);
  if (approved > 0 || provisional > 0) return "MEMBRE ACTIF";
  return "MEMBRE";
}

function toMillis(value) {
  if (!value) return 0;
  if (typeof value?.toMillis === "function") return Number(value.toMillis()) || 0;
  if (typeof value?.seconds === "number") {
    const seconds = Number(value.seconds) || 0;
    const nanos = Number(value.nanoseconds) || 0;
    return (seconds * 1000) + Math.floor(nanos / 1e6);
  }
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function formatHistoryDate(value) {
  const ms = toMillis(value);
  if (!ms) return "-";
  return new Date(ms).toLocaleString("fr-HT", {
    dateStyle: "medium",
    timeStyle: "short",
  });
}

function getOperationKind(item = {}) {
  return String(item.type || item.collectionName || "").toLowerCase().includes("withdraw") ? "withdrawal" : "order";
}

function getOperationTitle(item = {}) {
  return getOperationKind(item) === "withdrawal" ? "Retrait" : "Dépôt";
}

function getOperationAmount(item = {}) {
  const direct = Number(item.amount);
  if (Number.isFinite(direct)) return Math.max(0, Math.floor(direct));
  const requested = Number(item.requestedAmount);
  if (Number.isFinite(requested)) return Math.max(0, Math.floor(requested));
  return 0;
}

function normalizeOperationStatus(item = {}) {
  const raw = String(item.resolutionStatus || item.status || "pending").trim().toLowerCase();
  if (raw === "approved" || raw === "success" || raw === "completed" || raw === "done") return "approved";
  if (raw === "cancelled" || raw === "canceled") return "cancelled";
  if (raw === "rejected" || raw === "refused" || raw === "failed") return "rejected";
  if (raw === "review" || raw === "pending_review" || raw === "verifying" || raw === "processing") return "review";
  return "pending";
}

function getOperationStatusLabel(status) {
  if (status === "approved") return "Approuvé";
  if (status === "rejected") return "Rejeté";
  if (status === "cancelled") return "Annulé";
  if (status === "review") return "En examen";
  return "En attente";
}

function getOperationStatusClass(status) {
  if (status === "approved") return "is-approved";
  if (status === "rejected") return "is-rejected";
  if (status === "cancelled") return "is-cancelled";
  if (status === "review") return "is-review";
  return "is-pending";
}

function isPendingOperation(item = {}) {
  if (!item || item.userHiddenByClient) return false;
  const status = normalizeOperationStatus(item);
  return status !== "approved" && status !== "cancelled";
}

function formatHistoryAmount(item = {}) {
  const amount = getOperationAmount(item);
  const kind = getOperationKind(item);
  const prefix = kind === "withdrawal" ? "-" : "+";
  return `${prefix}${formatHtg(amount)}`;
}

function renderOperationCard(item = {}) {
  const kind = getOperationKind(item);
  const status = normalizeOperationStatus(item);
  const title = getOperationTitle(item);
  const amount = formatHistoryAmount(item);
  const method = safeText(item.methodName || item.method || item.paymentMethod || "-", "-");
  const code = safeText(item.uniqueCode || item.id || "-", "-");
  const createdAt = formatHistoryDate(item.createdAt);
  const note = safeText(item.note || item.message || item.reason || "", "");
  const amountClass = kind === "withdrawal" ? "is-out" : "";

  const wrapper = document.createElement("article");
  wrapper.className = "profile-history-item";
  wrapper.innerHTML = `
    <div class="profile-history-item__top">
      <div class="min-w-0">
        <p class="profile-history-item__title">${title} ${code}</p>
        <p class="profile-history-item__meta">${createdAt}</p>
      </div>
      <div class="profile-history-item__amount ${amountClass}">${amount}</div>
    </div>
    <div class="profile-history-item__body">
      <span class="profile-history-item__badge ${getOperationStatusClass(status)}">${getOperationStatusLabel(status)}</span>
      <div>Méthode: ${method}</div>
      ${note ? `<div>Note: ${note}</div>` : ""}
    </div>
  `;
  return wrapper;
}

function renderOperationsList(target, items = [], emptyText = "Aucune opération.") {
  const listEl = typeof target === "string" ? document.querySelector(target) : target;
  if (!listEl) return;
  listEl.replaceChildren();
  if (!Array.isArray(items) || items.length === 0) {
    const empty = document.createElement("p");
    empty.className = "profile-history-empty";
    empty.textContent = emptyText;
    listEl.appendChild(empty);
    return;
  }
  items.forEach((item) => {
    listEl.appendChild(renderOperationCard(item));
  });
}

const AGENT_CONTACT_OPTIONS = {
  deposit: [
    { key: "agent_deposit", title: "Agent dépôt", role: "Dépôt / validation", note: "Contacte cet agent pour faire un dépôt." },
    { key: "support_default", title: "Support", role: "Assistance générale", note: "Si l'agent dépôt ne répond pas, contacte le support." },
  ],
  withdrawal: [
    { key: "withdrawal_assistance", title: "Agent retrait", role: "Retrait / suivi", note: "Contacte cet agent pour un retrait." },
    { key: "support_default", title: "Support", role: "Assistance générale", note: "Si l'agent retrait ne répond pas, contacte le support." },
  ],
};

function getAgentPanelMode() {
  const active = document.querySelector("[data-profile-panel='agents']")?.dataset.mode || "deposit";
  return active === "withdrawal" ? "withdrawal" : "deposit";
}

function setAgentPanelMode(mode = "deposit") {
  const panel = document.querySelector("[data-profile-panel='agents']");
  if (!panel) return;
  panel.dataset.mode = mode === "withdrawal" ? "withdrawal" : "deposit";
}

function renderAgentContacts(mode = "deposit") {
  const listEl = document.querySelector("[data-profile-agents-list]");
  const titleEl = document.querySelector("[data-profile-agents-title]");
  const copyEl = document.querySelector("[data-profile-agents-copy]");
  const normalizedMode = mode === "withdrawal" ? "withdrawal" : "deposit";
  const items = AGENT_CONTACT_OPTIONS[normalizedMode] || AGENT_CONTACT_OPTIONS.deposit;

  if (titleEl) {
    titleEl.textContent = normalizedMode === "withdrawal" ? "CONTACTER UN AGENT RETRAIT" : "CONTACTER UN AGENT DÉPÔT";
  }
  if (copyEl) {
    copyEl.textContent = normalizedMode === "withdrawal"
      ? "Chwazi yon ajan retrè pou kontinye sou WhatsApp."
      : "Chwazi yon ajan depo pou kontinye sou WhatsApp.";
  }
  if (!listEl) return;

  listEl.replaceChildren();
  items.forEach((item) => {
    const phoneLabel = getWhatsappContactLabel(item.key);
    const waLink = buildWhatsappUrlForKey(item.key, normalizedMode === "withdrawal"
      ? "Bonjou, mwen bezwen fè yon retrè sou kont mwen."
      : "Bonjou, mwen bezwen fè yon depo sou kont mwen.");
    const card = document.createElement("article");
    card.className = "profile-agent-card";
    card.innerHTML = `
      <div class="profile-agent-card__top">
        <div class="min-w-0">
          <h3 class="profile-agent-card__name">${item.title}</h3>
          <p class="profile-agent-card__role">${item.role}</p>
        </div>
        <p class="profile-agent-card__phone">${phoneLabel || ""}</p>
      </div>
      <div class="profile-agent-card__actions">
        <a class="profile-agent-card__button" href="${waLink}" target="_blank" rel="noopener noreferrer">
          <i data-lucide="message-circle"></i>
          WhatsApp
        </a>
        <span class="profile-agent-card__button is-secondary" aria-hidden="true">${item.note}</span>
      </div>
    `;
    listEl.appendChild(card);
  });

  if (window.lucide) {
    window.lucide.createIcons();
  }
}

function getAvatarInitials(name = "") {
  const parts = String(name || "")
    .trim()
    .split(/\s+/)
    .filter(Boolean);
  const first = parts[0]?.[0] || "P";
  const second = parts[1]?.[0] || parts[0]?.[1] || " ";
  return `${first}${second}`.trim().toUpperCase();
}

async function loadClientData(uid) {
  const safeUid = String(uid || "").trim();
  if (!safeUid) return {};
  try {
    const snap = await getDoc(doc(db, "clients", safeUid));
    return snap.exists() ? (snap.data() || {}) : {};
  } catch (error) {
    console.warn("[KOBPOSH][PROFILE] client load failed", error);
    return {};
  }
}

function setHeaderBalance(value) {
  const headerBalanceEl = document.querySelector("[data-profile-header-balance]");
  if (!headerBalanceEl) return;
  headerBalanceEl.textContent = Number.isFinite(Number(value)) ? formatHtg(value) : "-- HTG";
}

function setAvatar(user, clientData = {}) {
  const avatarEl = document.querySelector("[data-profile-avatar]");
  const avatarTextEl = document.querySelector("[data-profile-avatar-text]");
  if (!avatarEl) return;

  const photoUrl = String(clientData.photoURL || user?.photoURL || "").trim();
  const name = getDisplayName(user, clientData);
  const initials = getAvatarInitials(name);

  avatarEl.classList.toggle("has-photo", Boolean(photoUrl));
  if (photoUrl) {
    avatarEl.style.backgroundImage = `url("${photoUrl}")`;
    avatarEl.style.backgroundSize = "cover";
    avatarEl.style.backgroundPosition = "center";
    avatarEl.style.color = "transparent";
    if (avatarTextEl) avatarTextEl.textContent = "";
  } else {
    avatarEl.style.backgroundImage = "";
    avatarEl.style.backgroundSize = "";
    avatarEl.style.backgroundPosition = "";
    avatarEl.style.color = "#ffffff";
    if (avatarTextEl) avatarTextEl.textContent = initials;
  }
}

function updateProfileFields(user, clientData = {}, fundingData = {}) {
  const name = getDisplayName(user, clientData);
  const username = getDisplayUsername(user, clientData);
  const email = getDisplayEmail(user, clientData);
  const phone = getDisplayPhone(user, clientData);
  const identifier = getDisplayIdentifier(user, clientData);
  const balance = getBestBalance(fundingData, clientData);
  const sidebarStatus = getSidebarStatus(fundingData, clientData);

  const nameEl = document.querySelector("[data-profile-sidebar-name]");
  const statusEl = document.querySelector("[data-profile-sidebar-status]");
  const fullNameInput = document.getElementById("fullName");
  const emailInput = document.getElementById("email");
  const phoneInput = document.getElementById("phone");
  const identifierInput = document.getElementById("identifier");
  const walletAmountEl = document.querySelector("[data-profile-wallet-amount]");

  if (nameEl) nameEl.textContent = username;
  if (statusEl) statusEl.textContent = sidebarStatus;
  if (fullNameInput) fullNameInput.value = name;
  if (emailInput) emailInput.value = email || "";
  if (phoneInput) phoneInput.value = phone || "";
  if (identifierInput) identifierInput.value = identifier || "";
  if (walletAmountEl) walletAmountEl.textContent = Number.isFinite(balance) ? formatHtg(balance) : "-- HTG";

  if (document.title && name) {
    document.title = `${name} | Kobposh`;
  }

  setAvatar(user, clientData);
  setHeaderBalance(balance);
}

async function loadUserOperations(uid) {
  const safeUid = String(uid || "").trim();
  if (!safeUid) return { orders: [], withdrawals: [] };

  const ordersRef = collection(db, "clients", safeUid, "orders");
  const withdrawalsRef = collection(db, "clients", safeUid, "withdrawals");

  const [ordersSnap, withdrawalsSnap] = await Promise.all([
    getDocs(query(ordersRef, orderBy("createdAt", "desc"))).catch((error) => {
      console.warn("[KOBPOSH][PROFILE] orders history load failed", error);
      return { docs: [] };
    }),
    getDocs(query(withdrawalsRef, orderBy("createdAt", "desc"))).catch((error) => {
      console.warn("[KOBPOSH][PROFILE] withdrawals history load failed", error);
      return { docs: [] };
    }),
  ]);

  const orders = Array.isArray(ordersSnap.docs)
    ? ordersSnap.docs.map((snap) => ({ id: snap.id, type: "order", ...snap.data() }))
    : [];
  const withdrawals = Array.isArray(withdrawalsSnap.docs)
    ? withdrawalsSnap.docs.map((snap) => ({ id: snap.id, type: "withdrawal", ...snap.data() }))
    : [];

  const pending = [...orders, ...withdrawals]
    .filter(isPendingOperation)
    .sort((a, b) => toMillis(b.createdAt) - toMillis(a.createdAt));

  const deposits = [...orders]
    .sort((a, b) => toMillis(b.createdAt) - toMillis(a.createdAt));

  const withdrawalsSorted = [...withdrawals]
    .sort((a, b) => toMillis(b.createdAt) - toMillis(a.createdAt));

  return { orders, withdrawals: withdrawalsSorted, pending, deposits };
}

function setProfilePanel(panelKey = "info") {
  const key = String(panelKey || "info").trim();
  document.querySelectorAll("[data-profile-panel]").forEach((panel) => {
    panel.classList.toggle("is-active", panel.getAttribute("data-profile-panel") === key);
  });
  document.querySelectorAll("[data-profile-nav]").forEach((link) => {
    link.classList.toggle("is-active", link.getAttribute("data-profile-nav") === key);
  });
}

const HISTORY_PAGE_SIZE = 3;
const HISTORY_SECTION_CONFIG = {
  pending: {
    emptyText: "Clique sur ce bloc pour charger les opérations en cours.",
    moreText: "CHARGER PLUS",
  },
  deposit: {
    collectionName: "orders",
    emptyText: "Clique sur ce bloc pour charger les dépôts agents.",
    moreText: "CHARGER PLUS",
  },
  withdrawal: {
    collectionName: "withdrawals",
    emptyText: "Clique sur ce bloc pour charger les retraits.",
    moreText: "CHARGER PLUS",
  },
};

const historyState = {
  pending: {
    loaded: false,
    loading: false,
    open: false,
    items: [],
    visibleCount: HISTORY_PAGE_SIZE,
    hasMore: true,
    orderCursor: null,
    withdrawalCursor: null,
  },
  deposit: {
    loaded: false,
    loading: false,
    open: false,
    items: [],
    visibleCount: HISTORY_PAGE_SIZE,
    hasMore: true,
    cursor: null,
  },
  withdrawal: {
    loaded: false,
    loading: false,
    open: false,
    items: [],
    visibleCount: HISTORY_PAGE_SIZE,
    hasMore: true,
    cursor: null,
  },
};

function getHistorySectionElements(kind) {
  return {
    panel: document.querySelector(`[data-profile-history-panel="${kind}"]`),
    list: document.querySelector(`[data-profile-history-${kind}-list]`),
    count: document.querySelector(`[data-profile-history-${kind}-count]`),
    more: document.querySelector(`[data-profile-history-more="${kind}"]`),
    toggle: document.querySelector(`[data-profile-history-toggle="${kind}"]`),
  };
}

function resetHistorySections() {
  Object.values(historyState).forEach((state) => {
    state.loaded = false;
    state.loading = false;
    state.open = false;
    state.items = [];
    state.visibleCount = HISTORY_PAGE_SIZE;
    state.hasMore = true;
    state.cursor = null;
    state.orderCursor = null;
    state.withdrawalCursor = null;
  });

  Object.keys(HISTORY_SECTION_CONFIG).forEach((kind) => {
    const elements = getHistorySectionElements(kind);
    if (elements.panel) elements.panel.hidden = true;
    if (elements.toggle) elements.toggle.setAttribute("aria-expanded", "false");
    if (elements.count) elements.count.textContent = "0";
    if (elements.list) {
      elements.list.replaceChildren();
      const empty = document.createElement("p");
      empty.className = "profile-history-empty";
      empty.textContent = HISTORY_SECTION_CONFIG[kind].emptyText;
      elements.list.appendChild(empty);
    }
    if (elements.more) elements.more.hidden = true;
  });
}

function renderHistorySection(kind) {
  const state = historyState[kind];
  const elements = getHistorySectionElements(kind);
  if (!state || !elements.panel || !elements.list) return;

  elements.panel.hidden = !state.open;
  if (elements.toggle) elements.toggle.setAttribute("aria-expanded", state.open ? "true" : "false");
  if (elements.count) elements.count.textContent = String(state.loaded ? state.items.length : 0);

  if (!state.open) return;

  if (!state.loaded && state.loading) {
    elements.list.replaceChildren();
    const loading = document.createElement("p");
    loading.className = "profile-history-empty";
    loading.textContent = "Ap chaje...";
    elements.list.appendChild(loading);
    if (elements.more) elements.more.hidden = true;
    return;
  }

  const visibleItems = state.items.slice(0, state.visibleCount);
  renderOperationsList(elements.list, visibleItems, HISTORY_SECTION_CONFIG[kind].emptyText);

  if (elements.more) {
    const showMore = state.loaded && state.hasMore && state.visibleCount < state.items.length;
    elements.more.hidden = !showMore;
    elements.more.textContent = HISTORY_SECTION_CONFIG[kind].moreText;
  }
}

function appendUniqueHistoryItems(targetState, items = []) {
  const seen = new Set(targetState.items.map((item) => `${item.__sourceKey || ""}:${item.id || ""}`));
  items.forEach((item) => {
    const key = `${item.__sourceKey || ""}:${item.id || ""}`;
    if (seen.has(key)) return;
    seen.add(key);
    targetState.items.push(item);
  });
}

function mapHistoryDoc(collectionName, snap) {
  return {
    id: snap.id,
    __sourceKey: collectionName,
    type: collectionName === "withdrawals" ? "withdrawal" : "order",
    ...snap.data(),
  };
}

async function loadHistorySectionPage(uid, kind) {
  const safeUid = String(uid || "").trim();
  if (!safeUid) return;

  const state = historyState[kind];
  const elements = getHistorySectionElements(kind);
  if (!state || state.loading) return;

  state.loading = true;
  state.open = true;
  renderHistorySection(kind);

  try {
    if (kind === "pending") {
      const ordersRef = collection(db, "clients", safeUid, "orders");
      const withdrawalsRef = collection(db, "clients", safeUid, "withdrawals");

      const orderQuery = query(
        ordersRef,
        orderBy("createdAt", "desc"),
        ...(state.orderCursor ? [startAfter(state.orderCursor)] : []),
        limit(HISTORY_PAGE_SIZE)
      );
      const withdrawalQuery = query(
        withdrawalsRef,
        orderBy("createdAt", "desc"),
        ...(state.withdrawalCursor ? [startAfter(state.withdrawalCursor)] : []),
        limit(HISTORY_PAGE_SIZE)
      );

      const [ordersSnap, withdrawalsSnap] = await Promise.all([
        getDocs(orderQuery).catch((error) => {
          console.warn("[KOBPOSH][PROFILE] pending orders load failed", error);
          return { docs: [] };
        }),
        getDocs(withdrawalQuery).catch((error) => {
          console.warn("[KOBPOSH][PROFILE] pending withdrawals load failed", error);
          return { docs: [] };
        }),
      ]);

      state.orderCursor = ordersSnap.docs?.[ordersSnap.docs.length - 1] || state.orderCursor;
      state.withdrawalCursor = withdrawalsSnap.docs?.[withdrawalsSnap.docs.length - 1] || state.withdrawalCursor;

      const pendingItems = [
        ...(Array.isArray(ordersSnap.docs) ? ordersSnap.docs.map((snap) => mapHistoryDoc("orders", snap)) : []),
        ...(Array.isArray(withdrawalsSnap.docs) ? withdrawalsSnap.docs.map((snap) => mapHistoryDoc("withdrawals", snap)) : []),
      ]
        .filter(isPendingOperation)
        .sort((a, b) => toMillis(b.createdAt) - toMillis(a.createdAt));

      appendUniqueHistoryItems(state, pendingItems);
      state.hasMore = Boolean(
        (ordersSnap.docs?.length || 0) === HISTORY_PAGE_SIZE
        || (withdrawalsSnap.docs?.length || 0) === HISTORY_PAGE_SIZE
      );
    } else {
      const collectionName = HISTORY_SECTION_CONFIG[kind].collectionName;
      const baseRef = collection(db, "clients", safeUid, collectionName);
      const q = query(
        baseRef,
        orderBy("createdAt", "desc"),
        ...(state.cursor ? [startAfter(state.cursor)] : []),
        limit(HISTORY_PAGE_SIZE)
      );
      const snap = await getDocs(q).catch((error) => {
        console.warn("[KOBPOSH][PROFILE] history load failed", { kind, error });
        return { docs: [] };
      });
      state.cursor = snap.docs?.[snap.docs.length - 1] || state.cursor;
      const items = Array.isArray(snap.docs) ? snap.docs.map((docSnap) => mapHistoryDoc(collectionName, docSnap)) : [];
      appendUniqueHistoryItems(state, items);
      state.hasMore = (snap.docs?.length || 0) === HISTORY_PAGE_SIZE;
    }

    state.loaded = true;
    state.visibleCount = Math.min(Math.max(state.visibleCount, HISTORY_PAGE_SIZE), state.items.length || HISTORY_PAGE_SIZE);
    if (state.items.length < HISTORY_PAGE_SIZE && state.hasMore) {
      state.visibleCount = HISTORY_PAGE_SIZE;
    }
    renderHistorySection(kind);
  } catch (error) {
    console.warn("[KOBPOSH][PROFILE] history page load failed", { kind, error });
    const listEl = elements.list;
    if (listEl) {
      listEl.replaceChildren();
      const empty = document.createElement("p");
      empty.className = "profile-history-empty";
      empty.textContent = "Nou pa ka chaje istwa a kounye a.";
      listEl.appendChild(empty);
    }
  } finally {
    state.loading = false;
    renderHistorySection(kind);
  }
}

function loadMoreHistorySection(kind) {
  const state = historyState[kind];
  if (!state) return;
  state.visibleCount += HISTORY_PAGE_SIZE;
  renderHistorySection(kind);
  if (state.visibleCount > state.items.length && state.hasMore && !state.loading) {
    const user = auth.currentUser;
    if (user?.uid) {
      void loadHistorySectionPage(user.uid, kind);
    }
  }
}

async function refreshProfileForUser(user) {
  const uid = String(user?.uid || "").trim();
  if (!uid) {
    updateProfileFields(null, {}, {});
    resetHistorySections();
    return;
  }

  const [clientData, fundingData] = await Promise.all([
    loadClientData(uid),
    getDepositFundingStatusSecure({}).catch((error) => {
      console.warn("[KOBPOSH][PROFILE] funding load failed", error);
      return {};
    }),
  ]);

  updateProfileFields(user, clientData, fundingData || {});
  renderAgentContacts(getAgentPanelMode());
  resetHistorySections();
}

onAuthStateChanged(auth, (user) => {
  if (!user) {
    window.location.href = "./index.html?auth=login";
    return;
  }
  void refreshProfileForUser(user || null);
});

const profileBackBtn = document.querySelector("[data-profile-back]");
profileBackBtn?.addEventListener("click", () => {
  window.location.href = "./index.html";
});

document.querySelectorAll("[data-profile-nav]").forEach((link) => {
  link.addEventListener("click", (event) => {
    event.preventDefault();
    const navKey = link.getAttribute("data-profile-nav") || "";
    if (navKey === "logout") {
      void logoutCurrentUser().finally(() => {
        window.location.href = "./index.html?auth=login";
      });
      return;
    }
    const panel = navKey === "history" ? "history" : navKey === "deposit-agent" || navKey === "withdraw-agent" ? "agents" : "info";
    if (navKey === "deposit-agent") setAgentPanelMode("deposit");
    if (navKey === "withdraw-agent") setAgentPanelMode("withdrawal");
    setProfilePanel(panel);
    if (panel === "agents") {
      renderAgentContacts(getAgentPanelMode());
    }
  });
});

document.querySelectorAll("[data-profile-history-toggle]").forEach((button) => {
  button.addEventListener("click", () => {
    const kind = button.getAttribute("data-profile-history-toggle") || "";
    if (!historyState[kind]) return;
    const state = historyState[kind];
    state.open = !state.open;
    renderHistorySection(kind);
    if (state.open && !state.loaded && !state.loading) {
      void loadHistorySectionPage(auth.currentUser?.uid || "", kind);
    }
  });
});

document.querySelectorAll("[data-profile-history-more]").forEach((button) => {
  button.addEventListener("click", () => {
    const kind = button.getAttribute("data-profile-history-more") || "";
    if (!historyState[kind]) return;
    loadMoreHistorySection(kind);
  });
});

document.querySelector("[data-profile-become-agent]")?.addEventListener("click", () => {
  window.location.href = "../recrutement.html";
});

void refreshWhatsappModalContacts().then(() => {
  renderAgentContacts(getAgentPanelMode());
});

setProfilePanel("info");
