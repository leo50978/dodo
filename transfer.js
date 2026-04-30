import { auth } from "./firebase-init.js";
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
  const amount = Number(value || 0);
  return new Intl.NumberFormat("fr-HT", {
    style: "currency",
    currency: "HTG",
    maximumFractionDigits: 0,
  }).format(amount);
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

function getTransferModalTheme(theme = "profile") {
  const isKobposh = theme === "kobposh";
  if (isKobposh) {
    return {
      overlayClass: "fixed inset-0 z-[3300] hidden items-stretch justify-center bg-[#f5f5f5]/96 backdrop-blur-sm",
      panelClass: "relative h-[100dvh] w-full overflow-y-auto overscroll-contain bg-white shadow-none",
      innerClass: "relative flex min-h-full flex-col px-4 pb-6 pt-4 sm:px-6 lg:px-8",
      headerClass: "flex min-w-0 items-start justify-between gap-3 border-b border-[#1fae5b]/12 pb-4",
      eyebrowClass: "text-xs font-semibold uppercase tracking-[0.18em] text-[#1fae5b]/70",
      titleClass: "mt-1 text-2xl font-extrabold text-[#0e5c34] sm:text-3xl",
      closeBtnClass: "grid h-11 w-11 place-items-center rounded-full border border-[#1fae5b]/15 bg-[#f3fbf6] text-[#0e5c34] shadow-[0_8px_18px_rgba(31,174,91,0.12)]",
      closeIconClass: "fa-solid fa-arrow-left text-lg",
      statGridClass: "mt-4 grid gap-3 sm:grid-cols-3",
      statCardClass: "rounded-2xl border border-[#1fae5b]/12 bg-[#f8fff9] p-4 text-[#0e5c34] shadow-[0_8px_18px_rgba(31,174,91,0.08)]",
      statLabelClass: "text-[11px] uppercase tracking-[0.14em] text-[#0e5c34]/55",
      statValueClass: "mt-2 text-lg font-semibold text-[#0e5c34]",
      tabsClass: "mt-4 flex flex-wrap gap-2",
      tabActiveClass: "rounded-full border border-[#1fae5b] bg-[#1fae5b] px-4 py-2 text-sm font-semibold text-white",
      tabInactiveClass: "rounded-full border border-[#1fae5b]/18 bg-white px-4 py-2 text-sm font-semibold text-[#0e5c34]/85",
      searchViewClass: "mt-4 flex flex-1 flex-col gap-4",
      sectionCardClass: "rounded-2xl border border-[#1fae5b]/12 bg-white p-4 shadow-[0_8px_18px_rgba(31,174,91,0.08)]",
      labelClass: "block text-xs uppercase tracking-[0.14em] text-[#0e5c34]/60",
      inputWrapClass: "mt-2 flex flex-col gap-2 sm:flex-row",
      inputClass: "h-12 flex-1 rounded-xl border border-[#1fae5b]/16 bg-white px-4 text-[#0e5c34] outline-none placeholder:text-[#0e5c34]/35 focus:border-[#1fae5b] focus:ring-2 focus:ring-[#1fae5b]/12",
      searchBtnClass: "h-12 rounded-xl border border-[#1fae5b] bg-[#1fae5b] px-5 text-sm font-semibold text-white shadow-[0_10px_20px_rgba(31,174,91,0.18)]",
      hintClass: "mt-2 text-xs text-[#0e5c34]/65",
      selectedCardClass: "hidden rounded-2xl border border-[#1fae5b]/18 bg-[#f2fbf5] p-4 text-[#0e5c34] shadow-[0_8px_18px_rgba(31,174,91,0.08)]",
      selectedLabelClass: "text-[11px] uppercase tracking-[0.14em] text-[#1fae5b]/75",
      selectedNameClass: "mt-1 truncate text-lg font-semibold text-[#0e5c34]",
      selectedMetaClass: "mt-1 truncate text-sm text-[#0e5c34]/75",
      secondaryBtnClass: "rounded-xl border border-[#1fae5b]/14 bg-white px-3 py-2 text-xs font-semibold text-[#0e5c34]",
      amountLabelClass: "block text-xs uppercase tracking-[0.14em] text-[#0e5c34]/60",
      amountInputClass: "mt-2 h-12 w-full rounded-xl border border-[#1fae5b]/16 bg-white px-4 text-[#0e5c34] outline-none placeholder:text-[#0e5c34]/35 focus:border-[#1fae5b] focus:ring-2 focus:ring-[#1fae5b]/12",
      submitBtnClass: "h-12 rounded-xl border border-[#1fae5b] bg-[#0e5c34] px-5 text-sm font-semibold text-white shadow-[0_10px_20px_rgba(14,92,52,0.18)]",
      previewClass: "mt-3 text-sm text-[#0e5c34]/80",
      resultsCardClass: "rounded-2xl border border-[#1fae5b]/12 bg-[#f8fff9] p-4 shadow-[0_8px_18px_rgba(31,174,91,0.08)]",
      resultsLabelClass: "text-[11px] uppercase tracking-[0.14em] text-[#0e5c34]/60",
      resultsCountClass: "text-xs text-[#0e5c34]/55",
      resultItemClass: (selected) => `rounded-2xl border ${selected ? "border-[#1fae5b]/35 bg-[#eefaf2]" : "border-[#1fae5b]/12 bg-white"} p-4 text-[#0e5c34] shadow-[0_8px_18px_rgba(31,174,91,0.08)]`,
      resultNameClass: "truncate text-base font-semibold",
      resultHandleClass: "mt-1 truncate text-sm text-[#0e5c34]/70",
      resultMetaClass: "mt-1 truncate text-xs text-[#0e5c34]/55",
      resultActionClass: "h-10 rounded-xl border border-[#1fae5b]/14 bg-white px-4 text-sm font-semibold text-[#0e5c34]",
      historyCardClass: "rounded-2xl border border-[#1fae5b]/12 bg-white p-4 text-[#0e5c34] shadow-[0_8px_18px_rgba(31,174,91,0.08)]",
      historyTextClass: "text-[#0e5c34]",
      historyMetaClass: "text-[#0e5c34]/70",
      historyBadgeClass: "rounded-full border border-[#1fae5b]/14 bg-[#f2fbf5] px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.14em] text-[#0e5c34]/80",
      statusClass: "mt-4 min-h-5 text-sm text-[#0e5c34]/75",
      searchTabLabel: "Rechercher",
      historyTabLabel: "Historique",
      searchPlaceholder: "ex: john_doe",
      searchCopy: "Cherche un utilisateur par son username pour lui envoyer de l’argent HTG.",
      selectedCopy: "Destinataire sélectionné",
      amountCopy: "Montant à envoyer",
      submitLabel: "Voye lajan",
      searchHintDefault: "Entre au moins 2 caractères pour chercher.",
      selectFirstCopy: "Sélectionne d'abord un ami.",
      previewEmpty: `Frais fixe: ${TRANSFER_FEE_HTG} HTG. Le destinataire reçoit ${formatAmount(0)} tant que le montant n'est pas saisi.`,
      resultsEmpty: "Aucun ami trouvé pour le moment.",
      historyEmpty: "Aucun transfert trouvé.",
      historyLead: "Les transferts se chargent un par un.",
      searchUsernameLabel: "Username de ton ami",
      resultCountZero: "0 résultat",
      resultCountOne: "1 résultat",
    };
  }

  return {
    overlayClass: "fixed inset-0 z-[3300] hidden items-center justify-center bg-black/50 p-3 backdrop-blur-sm lg:items-stretch lg:justify-end lg:p-0",
    panelClass: "relative h-[90vh] w-[94vw] overflow-y-auto overscroll-contain rounded-3xl border border-white/20 bg-[#3F4766]/52 shadow-[14px_14px_34px_rgba(12,16,28,0.45),-10px_-10px_24px_rgba(98,113,151,0.18)] backdrop-blur-xl lg:h-screen lg:w-[52vw] lg:rounded-none lg:rounded-l-3xl",
    innerClass: "relative flex h-full flex-col p-4 sm:p-6 lg:p-8",
    headerClass: "flex min-w-0 items-center justify-between gap-3",
    eyebrowClass: "text-xs uppercase tracking-[0.16em] text-white/70",
    titleClass: "mt-1 text-2xl font-bold text-white sm:text-3xl",
    closeBtnClass: "grid h-11 w-11 place-items-center rounded-full border border-white/20 bg-white/10 text-white shadow-[7px_7px_16px_rgba(18,24,39,0.35),-5px_-5px_12px_rgba(124,138,176,0.2)]",
    closeIconClass: "fa-solid fa-xmark text-lg",
    statGridClass: "mt-4 grid gap-3 sm:grid-cols-3",
    statCardClass: "rounded-2xl border border-white/20 bg-white/10 p-4 text-white shadow-[8px_8px_18px_rgba(19,25,40,0.34),-6px_-6px_14px_rgba(111,126,164,0.2)]",
    statLabelClass: "text-[11px] uppercase tracking-[0.14em] text-white/65",
    statValueClass: "mt-2 text-lg font-semibold text-white",
    tabsClass: "mt-4 flex flex-wrap gap-2",
    tabActiveClass: "rounded-full border border-white/20 bg-white/15 px-4 py-2 text-sm font-semibold text-white",
    tabInactiveClass: "rounded-full border border-white/20 bg-white/10 px-4 py-2 text-sm font-semibold text-white/85",
    searchViewClass: "mt-4 flex flex-1 flex-col gap-4",
    sectionCardClass: "rounded-2xl border border-white/20 bg-white/10 p-4 shadow-[8px_8px_18px_rgba(19,25,40,0.34),-6px_-6px_14px_rgba(111,126,164,0.2)]",
    labelClass: "block text-xs uppercase tracking-[0.14em] text-white/65",
    inputWrapClass: "mt-2 flex flex-col gap-2 sm:flex-row",
    inputClass: "h-12 flex-1 rounded-xl border border-white/20 bg-white/10 px-4 text-white outline-none placeholder:text-white/45",
    searchBtnClass: "h-12 rounded-xl border border-[#ffb26e] bg-[#F57C00] px-5 text-sm font-semibold text-white shadow-[8px_8px_18px_rgba(163,82,27,0.45),-6px_-6px_14px_rgba(255,175,102,0.22)]",
    hintClass: "mt-2 text-xs text-white/70",
    selectedCardClass: "hidden rounded-2xl border border-emerald-300/20 bg-emerald-500/12 p-4 text-white shadow-[8px_8px_18px_rgba(19,25,40,0.34),-6px_-6px_14px_rgba(111,126,164,0.2)]",
    selectedLabelClass: "text-[11px] uppercase tracking-[0.14em] text-emerald-100/70",
    selectedNameClass: "mt-1 truncate text-lg font-semibold text-white",
    selectedMetaClass: "mt-1 truncate text-sm text-white/80",
    secondaryBtnClass: "rounded-xl border border-white/20 bg-white/10 px-3 py-2 text-xs font-semibold text-white",
    amountLabelClass: "block text-xs uppercase tracking-[0.14em] text-white/65",
    amountInputClass: "mt-2 h-12 w-full rounded-xl border border-white/20 bg-white/10 px-4 text-white outline-none placeholder:text-white/45",
    submitBtnClass: "h-12 rounded-xl border border-[#34d399]/24 bg-[#139c55] px-5 text-sm font-semibold text-white shadow-[10px_12px_22px_rgba(8,61,34,0.34)]",
    previewClass: "mt-3 text-sm text-white/88",
    resultsCardClass: "rounded-2xl border border-white/20 bg-white/10 p-4 shadow-[8px_8px_18px_rgba(19,25,40,0.34),-6px_-6px_14px_rgba(111,126,164,0.2)]",
    resultsLabelClass: "text-[11px] uppercase tracking-[0.14em] text-white/65",
    resultsCountClass: "text-xs text-white/65",
    resultItemClass: (selected) => `rounded-2xl border ${selected ? "border-emerald-300/35 bg-emerald-500/15" : "border-white/15 bg-white/8"} p-4 text-white shadow-[8px_8px_18px_rgba(19,25,40,0.28),-6px_-6px_14px_rgba(111,126,164,0.16)]`,
    resultNameClass: "truncate text-base font-semibold",
    resultHandleClass: "mt-1 truncate text-sm text-white/72",
    resultMetaClass: "mt-1 truncate text-xs text-white/60",
    resultActionClass: "h-10 rounded-xl border border-white/20 bg-white/10 px-4 text-sm font-semibold text-white",
    historyCardClass: "rounded-2xl border border-white/15 bg-white/8 p-4 text-white",
    historyTextClass: "text-white",
    historyMetaClass: "text-white/72",
    historyBadgeClass: "rounded-full border border-white/15 bg-white/10 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.14em] text-white/80",
    statusClass: "mt-4 min-h-5 text-sm text-white/78",
    searchTabLabel: "Rechercher",
    historyTabLabel: "Historique transfert",
    searchPlaceholder: "ex: john_doe",
    searchCopy: "Cherche un utilisateur par son username pour lui envoyer de l’argent HTG.",
    selectedCopy: "Destinataire sélectionné",
    amountCopy: "Montant à envoyer",
    submitLabel: "Voye lajan",
    searchHintDefault: "Cherche un utilisateur par son username pour lui envoyer de l’argent HTG.",
    selectFirstCopy: "Sélectionne d'abord un ami.",
    previewEmpty: `Frais fixe: ${TRANSFER_FEE_HTG} HTG. Le destinataire reçoit ${formatAmount(0)} tant que le montant n'est pas saisi.`,
    resultsEmpty: "Aucun ami trouvé pour le moment.",
    historyEmpty: "Aucun transfert trouvé.",
    historyLead: "Les transferts se chargent un par un.",
    searchUsernameLabel: "Username de ton ami",
    resultCountZero: "0 résultat",
    resultCountOne: "1 résultat",
  };
}

function ensureTransferModal({ theme = "profile" } = {}) {
  const existing = document.getElementById("transferModalOverlay");
  if (existing) {
    existing.dataset.theme = theme === "kobposh" ? "kobposh" : "profile";
    return existing;
  }

  const ui = getTransferModalTheme(theme);

  const overlay = document.createElement("div");
  overlay.id = "transferModalOverlay";
  overlay.dataset.theme = theme === "kobposh" ? "kobposh" : "profile";
  overlay.className = ui.overlayClass;
  overlay.innerHTML = `
    <aside id="transferModalPanel" class="${ui.panelClass}" style="-webkit-overflow-scrolling: touch;">
      ${theme === "kobposh" ? '<div class="absolute inset-0 bg-[radial-gradient(circle_at_top,rgba(31,174,91,0.08),transparent_34%),linear-gradient(180deg,rgba(245,245,245,0.92),rgba(255,255,255,1))]"></div>' : '<div class="absolute inset-0 bg-gradient-to-b from-white/10 to-transparent"></div>'}
      <div class="${ui.innerClass}">
        <div class="${ui.headerClass}">
          <div class="min-w-0 pr-2">
            <p class="${ui.eyebrowClass}">Transfert HTG</p>
            <h2 class="${ui.titleClass}">Voye lajan bay zanmi w</h2>
          </div>
          <button id="transferModalClose" type="button" class="${ui.closeBtnClass}" aria-label="Fermer le transfert">
            <i class="${ui.closeIconClass}"></i>
          </button>
        </div>

        <div class="${ui.statGridClass}">
          <div class="${ui.statCardClass}">
            <p class="${ui.statLabelClass}">Montant minimum</p>
            <p class="${ui.statValueClass}">${TRANSFER_MIN_HTG} HTG</p>
          </div>
          <div class="${ui.statCardClass}">
            <p class="${ui.statLabelClass}">Frais fixe</p>
            <p class="${ui.statValueClass}">${TRANSFER_FEE_HTG} HTG</p>
          </div>
          <div class="${ui.statCardClass}">
            <p class="${ui.statLabelClass}">Solde approuvé</p>
            <p id="transferApprovedBalance" class="${ui.statValueClass}">-</p>
          </div>
        </div>

        <div class="${ui.tabsClass}">
          <button id="transferSearchTabBtn" type="button" class="${ui.tabActiveClass}">${ui.searchTabLabel}</button>
          <button id="transferHistoryTabBtn" type="button" class="${ui.tabInactiveClass}">${ui.historyTabLabel}</button>
        </div>

        <div id="transferSearchView" class="${ui.searchViewClass}">
          <div class="${ui.sectionCardClass}">
            <label for="transferRecipientQuery" class="${ui.labelClass}">${ui.searchUsernameLabel}</label>
            <div class="${ui.inputWrapClass}">
              <input id="transferRecipientQuery" type="text" autocomplete="off" placeholder="${ui.searchPlaceholder}" class="${ui.inputClass}" />
              <button id="transferSearchBtn" type="button" class="${ui.searchBtnClass}">Chercher</button>
            </div>
            <p id="transferSearchHint" class="${ui.hintClass}">${ui.searchCopy}</p>
          </div>

          <div id="transferSelectedCard" class="${ui.selectedCardClass}">
            <div class="flex min-w-0 flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
              <div class="min-w-0">
                <p class="${ui.selectedLabelClass}">${ui.selectedCopy}</p>
                <p id="transferSelectedName" class="${ui.selectedNameClass}">-</p>
                <p id="transferSelectedMeta" class="${ui.selectedMetaClass}">-</p>
              </div>
              <button id="transferClearSelectionBtn" type="button" class="${ui.secondaryBtnClass}">Changer</button>
            </div>

            <div class="mt-4 grid gap-3 sm:grid-cols-[minmax(0,1fr)_auto] sm:items-end">
              <div>
                <label for="transferAmountInput" class="${ui.amountLabelClass}">${ui.amountCopy}</label>
                <input id="transferAmountInput" type="number" min="25" step="1" inputmode="numeric" class="${ui.amountInputClass}" placeholder="25" />
              </div>
              <button id="transferSubmitBtn" type="button" onclick="window.__dominoTransferSend && window.__dominoTransferSend()" class="${ui.submitBtnClass}">${ui.submitLabel}</button>
            </div>

            <p id="transferPreviewText" class="${ui.previewClass}">Sélectionne un montant pour voir le net reçu après frais.</p>
          </div>

          <div class="${ui.resultsCardClass}">
            <div class="flex items-center justify-between gap-3">
              <p class="${ui.resultsLabelClass}">Résultats</p>
              <span id="transferSearchCount" class="${ui.resultsCountClass}">${ui.resultCountZero}</span>
            </div>
            <div id="transferResults" class="mt-3 grid gap-3"></div>
            <p id="transferResultsEmpty" class="mt-3 text-sm text-[#0e5c34]/70">${ui.resultsEmpty}</p>
          </div>
        </div>

        <div id="transferHistoryView" class="mt-4 hidden flex-1 flex-col gap-4">
          <div class="${ui.sectionCardClass}">
            <div class="flex flex-wrap items-center justify-between gap-2">
              <div>
                <p class="${ui.resultsLabelClass}">Historique</p>
                <p class="mt-1 text-sm ${theme === "kobposh" ? "text-[#0e5c34]/75" : "text-white/80"}">${ui.historyLead}</p>
              </div>
              <button id="transferHistoryLoadMoreBtn" type="button" class="${ui.secondaryBtnClass}">Charger le suivant</button>
            </div>
            <div id="transferHistoryList" class="mt-3 grid gap-3"></div>
            <p id="transferHistoryEmpty" class="mt-3 text-sm ${theme === "kobposh" ? "text-[#0e5c34]/70" : "text-white/70"}">${ui.historyEmpty}</p>
          </div>
        </div>

        <div id="transferStatus" class="${ui.statusClass}"></div>
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
  const searchHint = overlay.querySelector("#transferSearchHint");
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
    searchLoading: false,
    sendLoading: false,
    lastQuery: "",
  };

  const setStatus = (text = "", tone = "neutral") => {
    if (!statusEl) return;
    statusEl.textContent = String(text || "");
    statusEl.dataset.tone = tone;
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
      previewText.textContent = ui.selectFirstCopy;
      return;
    }
    if (amount <= 0) {
      previewText.textContent = ui.previewEmpty;
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
      searchCount.textContent = ui.resultCountZero;
      return;
    }

    searchResultsEmpty.style.display = "none";
    searchCount.textContent = `${state.results.length} résultat${state.results.length > 1 ? "s" : ""}`;
    searchResults.innerHTML = state.results.map((item) => {
      const isSelected = state.selectedRecipient?.uid === item.uid;
      return `
        <article class="${ui.resultItemClass(isSelected)}">
          <div class="flex min-w-0 flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
            <div class="min-w-0">
              <p class="${ui.resultNameClass}">${escapeHtml(item.name || item.username || item.uid || "Utilisateur")}</p>
              <p class="${ui.resultHandleClass}">@${escapeHtml(item.username || "-")}</p>
              <p class="${ui.resultMetaClass}">${escapeHtml(item.phone || item.email || item.uid || "-")}</p>
            </div>
            <button data-action="select-recipient" data-uid="${escapeHtml(item.uid)}" class="${ui.resultActionClass}">${isSelected ? "Sélectionné" : "Voye lajan"}</button>
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
    if (!selectedCard || !selectedName || !selectedMeta) return;
    const recipient = state.selectedRecipient;
    const hasRecipient = !!recipient;
    selectedCard.classList.toggle("hidden", !hasRecipient);
    if (!hasRecipient) {
      selectedName.textContent = "-";
      selectedMeta.textContent = "-";
      return;
    }
    selectedName.textContent = recipient.name || recipient.username || recipient.uid || "-";
    selectedMeta.textContent = `@${recipient.username || "-"} · ${recipient.phone || recipient.email || "Compte trouvé"}`;
  };

  const renderHistory = () => {
    if (!historyList || !historyEmpty || !historyLoadMoreBtn) return;
    if (!Array.isArray(state.historyItems) || state.historyItems.length === 0) {
      historyList.innerHTML = "";
      historyEmpty.style.display = "block";
    } else {
      historyEmpty.style.display = "none";
      historyList.innerHTML = state.historyItems.map((item) => {
        const direction = String(item.direction || "sent");
        const when = item.createdAtMs ? new Date(item.createdAtMs).toLocaleString("fr-FR") : "-";
        const otherName = direction === "sent"
          ? (item.recipientName || item.recipientUsername || item.recipientUid || "-")
          : (item.senderName || item.senderUsername || item.senderUid || "-");
        const otherHandle = direction === "sent"
          ? item.recipientUsername
          : item.senderUsername;
        return `
          <article class="${ui.historyCardClass}">
            <div class="flex min-w-0 flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
              <div class="min-w-0">
                <p class="text-sm font-semibold ${ui.historyTextClass}">${direction === "sent" ? "Envoyé" : "Reçu"} · ${escapeHtml(formatAmount(item.grossAmountHtg))}</p>
                <p class="mt-1 text-xs ${ui.historyMetaClass}">Avec frais: ${escapeHtml(formatAmount(item.feeHtg))} · Net: ${escapeHtml(formatAmount(item.netAmountHtg))}</p>
                <p class="mt-1 truncate text-xs ${ui.historyMetaClass}">${escapeHtml(otherName)}${otherHandle ? ` (@${escapeHtml(otherHandle)})` : ""}</p>
              </div>
              <span class="${ui.historyBadgeClass}">${escapeHtml(when)}</span>
            </div>
          </article>
        `;
      }).join("");
    }

    historyLoadMoreBtn.disabled = !state.historyHasMore || state.historyLoading;
    historyLoadMoreBtn.textContent = state.historyLoading ? "Chargement..." : (state.historyHasMore ? "Charger le suivant" : "Plus rien à charger");
  };

  const applyTab = (tab) => {
    state.activeTab = tab === "history" ? "history" : "search";
    searchView?.classList.toggle("hidden", state.activeTab !== "search");
    historyView?.classList.toggle("hidden", state.activeTab !== "history");
    if (searchTabBtn) {
      searchTabBtn.className = state.activeTab === "search" ? ui.tabActiveClass : ui.tabInactiveClass;
      searchTabBtn.textContent = ui.searchTabLabel;
    }
    if (historyTabBtn) {
      historyTabBtn.className = state.activeTab === "history" ? ui.tabActiveClass : ui.tabInactiveClass;
      historyTabBtn.textContent = ui.historyTabLabel;
    }
  };

  const refreshApprovedBalance = async () => {
    try {
      const funding = await getDepositFundingStatusSecure({});
      state.approvedBalance = safeInt(funding?.approvedHtgAvailable);
      if (approvedBalanceEl) approvedBalanceEl.textContent = formatAmount(state.approvedBalance);
      renderPreview();
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
      if (!items.length) {
        state.historyHasMore = false;
      }
      setStatus(state.historyItems.length ? "Historique chargé." : "Aucun transfert trouvé.", "success");
    } catch (error) {
      state.historyHasMore = false;
      setStatus(error?.message || "Impossible de charger l'historique.", "error");
    } finally {
      state.historyLoading = false;
      renderHistory();
    }
  };

  const searchRecipients = async () => {
    const query = String(searchInput?.value || "").trim();
    state.lastQuery = query;
    if (query.length < 2) {
      state.results = [];
      renderSearchResults();
      setStatus(ui.searchHintDefault, "neutral");
      return;
    }
    state.searchLoading = true;
    setStatus("Recherche du username...", "neutral");
    try {
      const result = await searchTransferRecipientsSecure({ query });
      state.results = Array.isArray(result?.results) ? result.results : [];
      renderSearchResults();
      setStatus(state.results.length ? "Destinataire trouvé." : "Aucun compte trouvé.", state.results.length ? "success" : "neutral");
    } catch (error) {
      state.results = [];
      renderSearchResults();
      setStatus(error?.message || "Recherche impossible.", "error");
    } finally {
      state.searchLoading = false;
    }
  };

  const selectRecipient = (recipient) => {
    state.selectedRecipient = recipient || null;
    renderSelectedRecipient();
    renderSearchResults();
    renderPreview();
    if (amountInput) {
      amountInput.value = String(amountInput.value || TRANSFER_MIN_HTG);
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
    setStatus("Envoi du transfert en cours...", "neutral");

    try {
      const result = await createTransferSecure({
        recipientUid: state.selectedRecipient.uid,
        amountHtg,
        clientRequestId: requestId,
        requestId,
      });
      setStatus(
        `Transfert effectué. ${formatAmount(result?.netAmountHtg || Math.max(0, amountHtg - TRANSFER_FEE_HTG))} a été reçu après les frais.`,
        "success"
      );
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
    setStatus("", "neutral");
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
    if (!state.historyItems.length) {
      await loadHistory({ reset: true });
    }
  });
  searchBtn?.addEventListener("click", searchRecipients);
  searchInput?.addEventListener("keydown", (ev) => {
    if (ev.key === "Enter") {
      ev.preventDefault();
      void searchRecipients();
    }
  });
  searchInput?.addEventListener("input", () => {
    setStatus("", "neutral");
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

  const handleExternalRefresh = () => {
    if (!state.open) return;
    void refreshApprovedBalance();
  };

  window.addEventListener("userBalanceUpdated", handleExternalRefresh);
  window.addEventListener("xchangeUpdated", handleExternalRefresh);
  window.addEventListener("transferUpdated", handleExternalRefresh);

  renderSelectedRecipient();
  renderSearchResults();
  renderHistory();
  renderPreview();
  if (approvedBalanceEl) approvedBalanceEl.textContent = "-";

  overlay.__openTransferModal = openModal;
  overlay.__closeTransferModal = closeModal;
  overlay.__transferState = state;
  window.openTransferDirectly = () => {
    void overlay.__openTransferModal?.();
  };
  window.__dominoTransferSend = () => {
    void sendTransfer();
  };
  return overlay;
}

export function mountTransferModal({ triggerSelector = "#profileTransferBtn", theme = "profile" } = {}) {
  const overlay = ensureTransferModal({ theme });
  const trigger = document.querySelector(triggerSelector);
  if (trigger && trigger.dataset.bound !== "1") {
    trigger.dataset.bound = "1";
    trigger.addEventListener("click", async () => {
      await overlay.__openTransferModal?.();
    });
  }
  return overlay;
}
