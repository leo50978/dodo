import { auth } from "./auth.js";
import {
  createTransferSecure,
  getDepositFundingStatusSecure,
  listTransferHistorySecure,
  searchTransferRecipientsSecure,
} from "./secure-functions.js";

const TRANSFER_MIN_HTG = 25;
const TRANSFER_FEE_HTG = 5;
const TRANSFER_HISTORY_PAGE_SIZE = 1;

function safeInt(value) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.max(0, Math.floor(n)) : 0;
}

function formatAmount(value) {
  return new Intl.NumberFormat("fr-HT", {
    style: "currency",
    currency: "HTG",
    maximumFractionDigits: 0,
  }).format(Number(value || 0));
}

function escapeHtml(value = "") {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function createClientRequestId(prefix = "transfer") {
  const safePrefix = String(prefix || "transfer").replace(/[^a-zA-Z0-9_-]/g, "").slice(0, 12) || "transfer";
  if (globalThis.crypto && typeof globalThis.crypto.randomUUID === "function") {
    return `${safePrefix}_${globalThis.crypto.randomUUID()}`;
  }
  return `${safePrefix}_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
}

function ensureTransferModal() {
  const existing = document.getElementById("transferModalOverlay");
  if (existing) return existing;

  const overlay = document.createElement("div");
  overlay.id = "transferModalOverlay";
  overlay.className = "fixed inset-0 z-[3300] hidden items-center justify-center bg-black/50 p-3 backdrop-blur-sm lg:items-stretch lg:justify-end lg:p-0";
  overlay.innerHTML = `
    <aside id="transferModalPanel" class="relative h-[90vh] w-[94vw] overflow-y-auto overscroll-contain rounded-3xl border border-white/20 bg-[#3F4766]/52 shadow-[14px_14px_34px_rgba(12,16,28,0.45),-10px_-10px_24px_rgba(98,113,151,0.18)] backdrop-blur-xl lg:h-screen lg:w-[52vw] lg:rounded-none lg:rounded-l-3xl" style="-webkit-overflow-scrolling: touch;">
      <div class="absolute inset-0 bg-gradient-to-b from-white/10 to-transparent"></div>
      <div class="relative flex h-full flex-col p-4 sm:p-6 lg:p-8">
        <div class="flex min-w-0 items-center justify-between gap-3">
          <div class="min-w-0">
            <p class="text-xs uppercase tracking-[0.16em] text-white/70">Transfert HTG</p>
            <h2 class="mt-1 text-2xl font-bold text-white sm:text-3xl">Voye lajan bay zanmi w</h2>
          </div>
          <button id="transferModalClose" type="button" class="grid h-11 w-11 place-items-center rounded-full border border-white/20 bg-white/10 text-white shadow-[7px_7px_16px_rgba(18,24,39,0.35),-5px_-5px_12px_rgba(124,138,176,0.2)]" aria-label="Close transfer">
            <i class="fa-solid fa-xmark text-lg"></i>
          </button>
        </div>

        <div class="mt-4 grid gap-3 sm:grid-cols-3">
          <div class="rounded-2xl border border-white/20 bg-white/10 p-4 text-white shadow-[8px_8px_18px_rgba(19,25,40,0.34),-6px_-6px_14px_rgba(111,126,164,0.2)]">
            <p class="text-[11px] uppercase tracking-[0.14em] text-white/65">Montant minimum</p>
            <p class="mt-2 text-lg font-semibold">${TRANSFER_MIN_HTG} HTG</p>
          </div>
          <div class="rounded-2xl border border-white/20 bg-white/10 p-4 text-white shadow-[8px_8px_18px_rgba(19,25,40,0.34),-6px_-6px_14px_rgba(111,126,164,0.2)]">
            <p class="text-[11px] uppercase tracking-[0.14em] text-white/65">Frais fixe</p>
            <p class="mt-2 text-lg font-semibold">${TRANSFER_FEE_HTG} HTG</p>
          </div>
          <div class="rounded-2xl border border-white/20 bg-white/10 p-4 text-white shadow-[8px_8px_18px_rgba(19,25,40,0.34),-6px_-6px_14px_rgba(111,126,164,0.2)]">
            <p class="text-[11px] uppercase tracking-[0.14em] text-white/65">Solde approuvé</p>
            <p id="transferApprovedBalance" class="mt-2 text-lg font-semibold">-</p>
          </div>
        </div>

        <div class="mt-4 flex flex-wrap gap-2">
          <button id="transferSearchTabBtn" type="button" class="rounded-full border border-white/20 bg-white/15 px-4 py-2 text-sm font-semibold text-white">Rechercher</button>
          <button id="transferHistoryTabBtn" type="button" class="rounded-full border border-white/20 bg-white/10 px-4 py-2 text-sm font-semibold text-white/85">Historique transfert</button>
        </div>

        <div id="transferSearchView" class="mt-4 flex flex-1 flex-col gap-4">
          <div class="rounded-2xl border border-white/20 bg-white/10 p-4 shadow-[8px_8px_18px_rgba(19,25,40,0.34),-6px_-6px_14px_rgba(111,126,164,0.2)]">
            <label for="transferRecipientQuery" class="block text-xs uppercase tracking-[0.14em] text-white/65">Username de ton ami</label>
            <div class="mt-2 flex flex-col gap-2 sm:flex-row">
              <input id="transferRecipientQuery" type="text" autocomplete="off" placeholder="ex: john_doe" class="h-12 flex-1 rounded-xl border border-white/20 bg-white/10 px-4 text-white outline-none placeholder:text-white/45" />
              <button id="transferSearchBtn" type="button" class="h-12 rounded-xl border border-[#ffb26e] bg-[#F57C00] px-5 text-sm font-semibold text-white shadow-[8px_8px_18px_rgba(163,82,27,0.45),-6px_-6px_14px_rgba(255,175,102,0.22)]">Chercher</button>
            </div>
            <p id="transferSearchHint" class="mt-2 text-xs text-white/70">Cherche un utilisateur par son username pour lui envoyer de l’argent HTG.</p>
          </div>

          <div id="transferSelectedCard" class="hidden rounded-2xl border border-emerald-300/20 bg-emerald-500/12 p-4 text-white shadow-[8px_8px_18px_rgba(19,25,40,0.34),-6px_-6px_14px_rgba(111,126,164,0.2)]">
            <div class="flex min-w-0 flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
              <div class="min-w-0">
                <p class="text-[11px] uppercase tracking-[0.14em] text-emerald-100/70">Destinataire sélectionné</p>
                <p id="transferSelectedName" class="mt-1 truncate text-lg font-semibold text-white">-</p>
                <p id="transferSelectedMeta" class="mt-1 truncate text-sm text-white/80">-</p>
              </div>
              <button id="transferClearSelectionBtn" type="button" class="rounded-xl border border-white/20 bg-white/10 px-3 py-2 text-xs font-semibold text-white">Changer</button>
            </div>

            <div class="mt-4 grid gap-3 sm:grid-cols-[minmax(0,1fr)_auto] sm:items-end">
              <div>
                <label for="transferAmountInput" class="block text-xs uppercase tracking-[0.14em] text-white/65">Montant à envoyer</label>
                <input id="transferAmountInput" type="number" min="25" step="1" inputmode="numeric" class="mt-2 h-12 w-full rounded-xl border border-white/20 bg-white/10 px-4 text-white outline-none placeholder:text-white/45" placeholder="25" />
              </div>
              <button id="transferSubmitBtn" type="button" onclick="window.__dominoTransferSend && window.__dominoTransferSend()" class="h-12 rounded-xl border border-[#34d399]/24 bg-[#139c55] px-5 text-sm font-semibold text-white shadow-[10px_12px_22px_rgba(8,61,34,0.34)]">Voye lajan</button>
            </div>

            <p id="transferPreviewText" class="mt-3 text-sm text-white/88">Sélectionne un montant pour voir le net reçu après frais.</p>
          </div>

          <div class="rounded-2xl border border-white/20 bg-white/10 p-4 shadow-[8px_8px_18px_rgba(19,25,40,0.34),-6px_-6px_14px_rgba(111,126,164,0.2)]">
            <div class="flex items-center justify-between gap-3">
              <p class="text-[11px] uppercase tracking-[0.14em] text-white/65">Résultats</p>
              <span id="transferSearchCount" class="text-xs text-white/65">0 résultat</span>
            </div>
            <div id="transferResults" class="mt-3 grid gap-3"></div>
            <p id="transferResultsEmpty" class="mt-3 text-sm text-white/70">Aucun ami trouvé pour le moment.</p>
          </div>
        </div>

        <div id="transferHistoryView" class="mt-4 hidden flex-1 flex-col gap-4">
          <div class="rounded-2xl border border-white/20 bg-white/10 p-4 shadow-[8px_8px_18px_rgba(19,25,40,0.34),-6px_-6px_14px_rgba(111,126,164,0.2)]">
            <div class="flex flex-wrap items-center justify-between gap-2">
              <div>
                <p class="text-[11px] uppercase tracking-[0.14em] text-white/65">Historique</p>
                <p class="mt-1 text-sm text-white/80">Les transferts se chargent un par un.</p>
              </div>
              <button id="transferHistoryLoadMoreBtn" type="button" class="rounded-xl border border-white/20 bg-white/10 px-3 py-2 text-xs font-semibold text-white">Charger le suivant</button>
            </div>
            <div id="transferHistoryList" class="mt-3 grid gap-3"></div>
            <p id="transferHistoryEmpty" class="mt-3 text-sm text-white/70">Aucun transfert trouvé.</p>
          </div>
        </div>

        <div id="transferStatus" class="mt-4 min-h-5 text-sm text-white/78"></div>
      </div>
    </aside>
  `;

  document.body.appendChild(overlay);

  const panel = overlay.querySelector("#transferModalPanel");
  const closeBtn = overlay.querySelector("#transferModalClose");
  const searchTabBtn = overlay.querySelector("#transferSearchTabBtn");
  const historyTabBtn = overlay.querySelector("#transferHistoryTabBtn");
  const searchView = overlay.querySelector("#transferSearchView");
  const historyView = overlay.querySelector("#transferHistoryView");
  const searchInput = overlay.querySelector("#transferRecipientQuery");
  const searchBtn = overlay.querySelector("#transferSearchBtn");
  const searchResults = overlay.querySelector("#transferResults");
  const searchResultsEmpty = overlay.querySelector("#transferResultsEmpty");
  const searchCount = overlay.querySelector("#transferSearchCount");
  const selectedCard = overlay.querySelector("#transferSelectedCard");
  const selectedName = overlay.querySelector("#transferSelectedName");
  const selectedMeta = overlay.querySelector("#transferSelectedMeta");
  const clearSelectionBtn = overlay.querySelector("#transferClearSelectionBtn");
  const amountInput = overlay.querySelector("#transferAmountInput");
  const submitBtn = overlay.querySelector("#transferSubmitBtn");
  const previewText = overlay.querySelector("#transferPreviewText");
  const statusEl = overlay.querySelector("#transferStatus");
  const approvedBalanceEl = overlay.querySelector("#transferApprovedBalance");
  const historyList = overlay.querySelector("#transferHistoryList");
  const historyEmpty = overlay.querySelector("#transferHistoryEmpty");
  const historyLoadMoreBtn = overlay.querySelector("#transferHistoryLoadMoreBtn");

  const state = {
    open: false,
    activeTab: "search",
    results: [],
    selectedRecipient: null,
    approvedBalance: 0,
    historyItems: [],
    historyCursorKey: "",
    historyHasMore: true,
    historyLoading: false,
  };

  const setStatus = (text = "") => {
    if (statusEl) statusEl.textContent = String(text || "");
  };

  const announce = (text = "", tone = "neutral") => {
    setStatus(text, tone);
    if (tone === "error" && typeof window.alert === "function" && text) {
      window.alert(String(text));
    }
  };

  const renderPreview = () => {
    if (!previewText) return;
    const amount = safeInt(amountInput?.value);
    if (!state.selectedRecipient) {
      previewText.textContent = "Sélectionne d'abord un ami.";
      return;
    }
    if (amount <= 0) {
      previewText.textContent = `Frais fixe: ${TRANSFER_FEE_HTG} HTG.`;
      return;
    }
    const net = Math.max(0, amount - TRANSFER_FEE_HTG);
    previewText.textContent = `Tu envoies ${formatAmount(amount)}. Ton ami reçoit ${formatAmount(net)} après les frais fixes de ${formatAmount(TRANSFER_FEE_HTG)}.`;
  };

  const renderSearchResults = () => {
    if (!searchResults || !searchResultsEmpty || !searchCount) return;
    if (!Array.isArray(state.results) || state.results.length === 0) {
      searchResults.innerHTML = "";
      searchResultsEmpty.style.display = "block";
      searchCount.textContent = "0 résultat";
      return;
    }
    searchResultsEmpty.style.display = "none";
    searchCount.textContent = `${state.results.length} résultat${state.results.length > 1 ? "s" : ""}`;
    searchResults.innerHTML = state.results.map((item) => {
      const isSelected = state.selectedRecipient?.uid === item.uid;
      return `
        <article class="rounded-2xl border ${isSelected ? "border-emerald-300/35 bg-emerald-500/15" : "border-white/15 bg-white/8"} p-4 text-white">
          <div class="flex min-w-0 flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
            <div class="min-w-0">
              <p class="truncate text-base font-semibold">${escapeHtml(item.name || item.username || item.uid || "Utilisateur")}</p>
              <p class="mt-1 truncate text-sm text-white/72">@${escapeHtml(item.username || "-")}</p>
            </div>
            <button data-action="select-recipient" data-uid="${escapeHtml(item.uid)}" class="h-10 rounded-xl border border-white/20 bg-white/10 px-4 text-sm font-semibold text-white">${isSelected ? "Sélectionné" : "Voye lajan"}</button>
          </div>
        </article>
      `;
    }).join("");
  };

  const bindSearchResultSelection = () => {
    if (!searchResults || searchResults.dataset.boundSelection === "1") return;
    searchResults.dataset.boundSelection = "1";
    searchResults.addEventListener("click", (ev) => {
      const actionBtn = ev.target?.closest?.("[data-action='select-recipient']");
      if (!actionBtn) return;
      const uid = String(actionBtn.getAttribute("data-uid") || "").trim();
      const recipient = state.results.find((item) => item.uid === uid) || null;
      if (recipient) {
        selectRecipient(recipient);
        setStatus(`@${recipient.username || recipient.uid} sélectionné.`, "success");
      }
    });
  };

  const renderSelectedRecipient = () => {
    const recipient = state.selectedRecipient;
    const hasRecipient = !!recipient;
    selectedCard?.classList.toggle("hidden", !hasRecipient);
    if (!hasRecipient) {
      if (selectedName) selectedName.textContent = "-";
      if (selectedMeta) selectedMeta.textContent = "-";
      return;
    }
    if (selectedName) selectedName.textContent = recipient.name || recipient.username || recipient.uid || "-";
    if (selectedMeta) selectedMeta.textContent = `@${recipient.username || "-"} · ${recipient.phone || recipient.email || "Compte trouvé"}`;
  };

  const renderHistory = () => {
    if (!historyList || !historyEmpty || !historyLoadMoreBtn) return;
    if (!Array.isArray(state.historyItems) || state.historyItems.length === 0) {
      historyList.innerHTML = "";
      historyEmpty.style.display = "block";
    } else {
      historyEmpty.style.display = "none";
      historyList.innerHTML = state.historyItems.map((item) => `
        <article class="rounded-2xl border border-white/15 bg-white/8 p-4 text-white">
          <p class="text-sm font-semibold">Transfert · ${escapeHtml(formatAmount(item.grossAmountHtg || 0))}</p>
          <p class="mt-1 text-xs text-white/72">Frais: ${escapeHtml(formatAmount(item.feeHtg || 0))} · Net: ${escapeHtml(formatAmount(item.netAmountHtg || 0))}</p>
        </article>
      `).join("");
    }
    historyLoadMoreBtn.disabled = !state.historyHasMore || state.historyLoading;
    historyLoadMoreBtn.textContent = state.historyLoading ? "Chargement..." : (state.historyHasMore ? "Charger le suivant" : "Plus rien à charger");
  };

  const applyTab = (tab) => {
    state.activeTab = tab === "history" ? "history" : "search";
    searchView?.classList.toggle("hidden", state.activeTab !== "search");
    historyView?.classList.toggle("hidden", state.activeTab !== "history");
  };

  const refreshApprovedBalance = async () => {
    try {
      const funding = await getDepositFundingStatusSecure({});
      state.approvedBalance = safeInt(funding?.approvedHtgAvailable);
      if (approvedBalanceEl) approvedBalanceEl.textContent = formatAmount(state.approvedBalance);
    } catch (_) {
      if (approvedBalanceEl) approvedBalanceEl.textContent = "-";
    }
  };

  const loadHistory = async ({ reset = false } = {}) => {
    if (state.historyLoading) return;
    state.historyLoading = true;
    if (reset) {
      state.historyItems = [];
      state.historyCursorKey = "";
      state.historyHasMore = true;
    }
    renderHistory();
    try {
      const result = await listTransferHistorySecure({
        pageSize: TRANSFER_HISTORY_PAGE_SIZE,
        cursorKey: reset ? "" : state.historyCursorKey,
      });
      const items = Array.isArray(result?.items) ? result.items : [];
      state.historyItems = reset ? items : [...state.historyItems, ...items];
      state.historyCursorKey = String(result?.nextCursorKey || "");
      state.historyHasMore = result?.hasMore === true && !!state.historyCursorKey;
      if (!items.length) state.historyHasMore = false;
    } catch (error) {
      state.historyHasMore = false;
      setStatus(error?.message || "Impossible de charger l'historique.");
    } finally {
      state.historyLoading = false;
      renderHistory();
    }
  };

  const searchRecipients = async () => {
    const query = String(searchInput?.value || "").trim();
    if (query.length < 2) {
      state.results = [];
      renderSearchResults();
      setStatus("Entre au moins 2 caractères pour chercher.");
      return;
    }
    setStatus("Recherche du username...");
    try {
      const result = await searchTransferRecipientsSecure({ query });
      state.results = Array.isArray(result?.results) ? result.results : [];
      renderSearchResults();
      setStatus(state.results.length ? "Destinataire trouvé." : "Aucun compte trouvé.");
    } catch (error) {
      state.results = [];
      renderSearchResults();
      setStatus(error?.message || "Recherche impossible.");
    }
  };

  const selectRecipient = (recipient) => {
    state.selectedRecipient = recipient || null;
    renderSelectedRecipient();
    renderSearchResults();
    renderPreview();
    if (amountInput) {
      amountInput.value = String(TRANSFER_MIN_HTG);
      amountInput.focus();
      amountInput.select?.();
    }
  };

  const clearSelection = () => {
    state.selectedRecipient = null;
    renderSelectedRecipient();
    renderSearchResults();
    renderPreview();
  };

  const sendTransfer = async () => {
    if (!state.selectedRecipient) {
      announce("Choisis d'abord un ami.", "error");
      return;
    }
    const amountHtg = safeInt(amountInput?.value);
    if (amountHtg < TRANSFER_MIN_HTG) {
      announce(`Le montant minimum est ${TRANSFER_MIN_HTG} HTG.`, "error");
      return;
    }
    if (amountHtg <= TRANSFER_FEE_HTG) {
      announce("Le montant doit être supérieur aux frais.", "error");
      return;
    }
    if (state.approvedBalance > 0 && amountHtg > state.approvedBalance) {
      announce("Solde approuvé insuffisant.", "error");
      return;
    }
    const requestId = createClientRequestId("transfer");
    submitBtn.disabled = true;
    submitBtn.textContent = "Envoi...";
    try {
      const result = await createTransferSecure({
        recipientUid: state.selectedRecipient.uid,
        amountHtg,
        clientRequestId: requestId,
        requestId,
      });
      setStatus(`Transfert effectué. ${formatAmount(result?.netAmountHtg || Math.max(0, amountHtg - TRANSFER_FEE_HTG))} a été reçu après les frais.`);
      amountInput.value = "";
      await refreshApprovedBalance();
      window.dispatchEvent(new CustomEvent("userBalanceUpdated"));
      window.dispatchEvent(new CustomEvent("transferUpdated", { detail: result || {} }));
      await loadHistory({ reset: true });
      applyTab("history");
    } catch (error) {
      announce(error?.message || "Le transfert a échoué.", "error");
    } finally {
      submitBtn.disabled = false;
      submitBtn.textContent = "Voye lajan";
    }
  };

  const openModal = async () => {
    overlay.classList.remove("hidden");
    overlay.classList.add("flex");
    state.open = true;
    applyTab("search");
    setStatus("");
    if (searchInput) {
      searchInput.focus();
      searchInput.select?.();
    }
    await refreshApprovedBalance();
  };

  const closeModal = () => {
    overlay.classList.add("hidden");
    overlay.classList.remove("flex");
    state.open = false;
  };

  overlay.addEventListener("click", (ev) => {
    if (ev.target === overlay) closeModal();
  });
  panel?.addEventListener("click", (ev) => ev.stopPropagation());
  bindSearchResultSelection();
  closeBtn?.addEventListener("click", closeModal);
  searchTabBtn?.addEventListener("click", () => applyTab("search"));
  historyTabBtn?.addEventListener("click", async () => {
    applyTab("history");
    if (!state.historyItems.length) await loadHistory({ reset: true });
  });
  searchBtn?.addEventListener("click", searchRecipients);
  searchInput?.addEventListener("keydown", (ev) => {
    if (ev.key === "Enter") {
      ev.preventDefault();
      void searchRecipients();
    }
  });
  amountInput?.addEventListener("input", renderPreview);
  clearSelectionBtn?.addEventListener("click", clearSelection);
  submitBtn?.addEventListener("click", sendTransfer);
  window.__dominoTransferSend = () => {
    void sendTransfer();
  };
  historyLoadMoreBtn?.addEventListener("click", async () => {
    if (!state.historyHasMore || state.historyLoading) return;
    await loadHistory({ reset: false });
  });

  window.addEventListener("userBalanceUpdated", () => {
    if (state.open) void refreshApprovedBalance();
  });
  window.addEventListener("xchangeUpdated", () => {
    if (state.open) void refreshApprovedBalance();
  });
  window.addEventListener("transferUpdated", () => {
    if (state.open) void refreshApprovedBalance();
  });

  renderSelectedRecipient();
  renderSearchResults();
  renderHistory();
  renderPreview();
  if (approvedBalanceEl) approvedBalanceEl.textContent = "-";

  overlay.__openTransferModal = openModal;
  overlay.__closeTransferModal = closeModal;
  window.openTransferDirectly = () => {
    void overlay.__openTransferModal?.();
  };
  window.__dominoTransferSend = () => {
    void sendTransfer();
  };
  return overlay;
}

export function mountTransferModal({ triggerSelector = "#profileTransferBtn" } = {}) {
  const overlay = ensureTransferModal();
  const trigger = document.querySelector(triggerSelector);
  if (trigger && trigger.dataset.bound !== "1") {
    trigger.dataset.bound = "1";
    trigger.addEventListener("click", async () => {
      await overlay.__openTransferModal?.();
    });
  }
  return overlay;
}
