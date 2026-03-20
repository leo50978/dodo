import {
  auth,
  db,
  doc,
  onSnapshot,
  onAuthStateChanged,
} from "./firebase-init.js";
import { withButtonLoading } from "./loading-ui.js";
import { getDepositFundingStatusSecure, walletMutateSecure } from "./secure-functions.js";

const RATE_HTG_TO_DOES = 20;
const BALANCE_DEBUG = true;
const WALLET_CACHE = new Map();
let walletUnsub = null;
let activeUid = null;
let soldeModulePromise = null;

function safeInt(value) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.max(0, Math.floor(n)) : 0;
}

function safeSignedInt(value) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : 0;
}

function bindHideOnErrorImages(root) {
  if (!root) return;
  root.querySelectorAll('img[data-hide-on-error="1"]').forEach((img) => {
    if (img.dataset.errorBound === "1") return;
    img.dataset.errorBound = "1";
    img.addEventListener("error", () => {
      img.style.display = "none";
    });
  });
}

function defaultWallet() {
  return {
    does: 0,
    doesApprovedBalance: 0,
    doesProvisionalBalance: 0,
    exchangeableDoesAvailable: 0,
    exchangedGourdes: 0,
    approvedHtgAvailable: 0,
    provisionalHtgAvailable: 0,
    withdrawableHtg: 0,
    accountFrozen: false,
    freezeReason: "",
    rejectedDepositStrikeCount: 0,
    pendingPlayFromXchangeDoes: 0,
    pendingPlayFromReferralDoes: 0,
    totalExchangedHtgEver: 0,
    loaded: false,
  };
}

function currentUid() {
  return auth.currentUser?.uid || "guest";
}

async function waitForXchangeBalanceHydration(uid = currentUid(), timeoutMs = 2600) {
  const safeUid = String(uid || "").trim();
  if (!safeUid || safeUid === "guest") return false;
  try {
    if (!soldeModulePromise) {
      soldeModulePromise = import("./solde.js");
    }
    const soldeModule = await soldeModulePromise;
    if (typeof soldeModule?.waitForBalanceHydration === "function") {
      return await soldeModule.waitForBalanceHydration(safeUid, timeoutMs);
    }
  } catch (error) {
    console.warn("[XCHANGE] waitForBalanceHydration unavailable", error);
  }
  return false;
}

function walletRef(uid) {
  return doc(db, "clients", uid);
}

function getCachedWallet(uid) {
  return WALLET_CACHE.get(uid) || defaultWallet();
}

function setCachedWallet(uid, data, loaded = true) {
  WALLET_CACHE.set(uid, {
    does: safeInt(data?.does),
    doesApprovedBalance: safeInt(
      typeof data?.doesApprovedBalance === "number"
        ? data.doesApprovedBalance
        : (safeInt(data?.does) - safeInt(data?.doesProvisionalBalance))
    ),
    doesProvisionalBalance: safeInt(data?.doesProvisionalBalance),
    exchangeableDoesAvailable: safeInt(
      typeof data?.exchangeableDoesAvailable === "number"
        ? data.exchangeableDoesAvailable
        : ((safeInt(data?.pendingPlayFromXchangeDoes) + safeInt(data?.pendingPlayFromReferralDoes)) <= 0
            ? safeInt(
              typeof data?.doesApprovedBalance === "number"
                ? data.doesApprovedBalance
                : (safeInt(data?.does) - safeInt(data?.doesProvisionalBalance))
            )
            : 0)
    ),
    exchangedGourdes: safeSignedInt(data?.exchangedGourdes),
    approvedHtgAvailable: safeInt(data?.approvedHtgAvailable),
    provisionalHtgAvailable: safeInt(data?.provisionalHtgAvailable),
    withdrawableHtg: safeInt(data?.withdrawableHtg),
    accountFrozen: data?.accountFrozen === true,
    freezeReason: String(data?.freezeReason || ""),
    rejectedDepositStrikeCount: safeInt(data?.rejectedDepositStrikeCount),
    pendingPlayFromXchangeDoes: safeInt(data?.pendingPlayFromXchangeDoes),
    pendingPlayFromReferralDoes: safeInt(data?.pendingPlayFromReferralDoes),
    totalExchangedHtgEver: safeInt(data?.totalExchangedHtgEver),
    loaded,
  });
}

async function syncWalletFundingState(uid = currentUid()) {
  const safeUid = String(uid || "").trim();
  if (!safeUid || safeUid === "guest") {
    return getXchangeState(window.__userBaseBalance || window.__userBalance || 0, safeUid);
  }

  try {
    const funding = await getDepositFundingStatusSecure({});
    if (BALANCE_DEBUG) {
      console.log("[BALANCE_DEBUG][XCHANGE] syncWalletFundingState", {
        uid: safeUid,
        funding,
      });
    }
    setCachedWallet(safeUid, {
      does: safeInt(funding?.doesBalance),
      doesApprovedBalance: safeInt(funding?.approvedDoesBalance),
      doesProvisionalBalance: safeInt(funding?.provisionalDoesBalance),
      exchangeableDoesAvailable: safeInt(funding?.exchangeableDoesAvailable),
      exchangedGourdes: safeSignedInt(funding?.exchangedApprovedHtg),
      approvedHtgAvailable: safeInt(funding?.approvedHtgAvailable),
      provisionalHtgAvailable: safeInt(funding?.provisionalHtgAvailable),
      withdrawableHtg: safeInt(funding?.withdrawableHtg),
      accountFrozen: funding?.accountFrozen === true,
      freezeReason: String(funding?.freezeReason || ""),
      rejectedDepositStrikeCount: safeInt(funding?.rejectedDepositStrikeCount),
      pendingPlayFromXchangeDoes: safeInt(funding?.pendingPlayFromXchangeDoes),
      pendingPlayFromReferralDoes: safeInt(funding?.pendingPlayFromReferralDoes),
      totalExchangedHtgEver: safeInt(funding?.totalExchangedApprovedHtg),
    }, true);
    return emitXchangeUpdated(safeUid);
  } catch (error) {
    console.warn("[XCHANGE] syncWalletFundingState failed", error);
    return getXchangeState(window.__userBaseBalance || window.__userBalance || 0, safeUid);
  }
}

function emitXchangeUpdated(uid = currentUid()) {
  const updated = getXchangeState(window.__userBaseBalance || window.__userBalance || 0, uid);
  if (BALANCE_DEBUG) {
    console.log("[BALANCE_DEBUG][XCHANGE] emitXchangeUpdated", {
      uid,
      __userBaseBalance: window.__userBaseBalance,
      __userBalance: window.__userBalance,
      updated,
    });
  }
  window.dispatchEvent(new CustomEvent("xchangeUpdated", { detail: updated }));
  return updated;
}

function startWalletWatcher(uid) {
  if (walletUnsub) {
    walletUnsub();
    walletUnsub = null;
  }
  if (!uid || uid === "guest") return;

  walletUnsub = onSnapshot(walletRef(uid), (snap) => {
    if (!snap.exists()) {
      setCachedWallet(uid, { does: 0, exchangedGourdes: 0 }, true);
      emitXchangeUpdated(uid);
      return;
    }
    const data = snap.data() || {};
    if (BALANCE_DEBUG) {
      console.log("[BALANCE_DEBUG][XCHANGE] wallet snapshot", {
        uid,
        doesBalance: data.doesBalance,
        doesApprovedBalance: data.doesApprovedBalance,
        doesProvisionalBalance: data.doesProvisionalBalance,
        exchangeableDoesAvailable: data.exchangeableDoesAvailable,
        exchangedGourdes: data.exchangedGourdes,
        approvedHtgAvailable: data.approvedHtgAvailable,
        provisionalHtgAvailable: data.provisionalHtgAvailable,
        withdrawableHtg: data.withdrawableHtg,
        accountFrozen: data.accountFrozen,
        freezeReason: data.freezeReason,
        rejectedDepositStrikeCount: data.rejectedDepositStrikeCount,
        pendingPlayFromXchangeDoes: data.pendingPlayFromXchangeDoes,
        pendingPlayFromReferralDoes: data.pendingPlayFromReferralDoes,
        totalExchangedHtgEver: data.totalExchangedHtgEver,
      });
    }
    setCachedWallet(uid, {
      does: safeInt(data.doesBalance),
      doesApprovedBalance: safeInt(data.doesApprovedBalance),
      doesProvisionalBalance: safeInt(data.doesProvisionalBalance),
      exchangeableDoesAvailable: safeInt(data.exchangeableDoesAvailable),
      exchangedGourdes: safeSignedInt(data.exchangedGourdes),
      approvedHtgAvailable: safeInt(data.approvedHtgAvailable),
      provisionalHtgAvailable: safeInt(data.provisionalHtgAvailable),
      withdrawableHtg: safeInt(data.withdrawableHtg),
      accountFrozen: data.accountFrozen === true,
      freezeReason: String(data.freezeReason || ""),
      rejectedDepositStrikeCount: safeInt(data.rejectedDepositStrikeCount),
      pendingPlayFromXchangeDoes: safeInt(data.pendingPlayFromXchangeDoes),
      pendingPlayFromReferralDoes: safeInt(data.pendingPlayFromReferralDoes),
      totalExchangedHtgEver: safeInt(data.totalExchangedHtgEver),
    }, true);
    emitXchangeUpdated(uid);
  }, (err) => {
    console.error("[XCHANGE] wallet watcher error", err);
  });
}

async function applyWalletMutation({ uid, deltaDoes = 0, deltaExchangedGourdes = 0, type = "mutation", note = "", amountGourdes = 0, amountDoes = 0 }) {
  if (!uid || uid === "guest") {
    return { ok: false, does: 0, error: "Utilisateur non connecté" };
  }

  try {
    let result = null;
    if (type === "xchange_buy") {
      result = await walletMutateSecure({
        op: "xchange_buy",
        amountGourdes: safeInt(amountGourdes),
      });
    } else if (type === "xchange_sell") {
      result = await walletMutateSecure({
        op: "xchange_sell",
        amountDoes: safeInt(amountDoes),
      });
    } else if (type === "game_entry") {
      result = await walletMutateSecure({
        op: "game_entry",
        amountDoes: safeInt(amountDoes),
      });
    } else {
      throw new Error(`Mutation wallet non supportée côté client: ${type}`);
    }

    const nextDoes = safeInt(result?.does);
    const nextExchanged = safeSignedInt(result?.exchangedGourdes);
    const nextApprovedDoes = safeInt(result?.doesApprovedBalance);
    const nextProvisionalDoes = safeInt(result?.doesProvisionalBalance);
    const nextExchangeableDoes = safeInt(result?.exchangeableDoesAvailable);
    const nextPendingFromXchange = safeInt(result?.pendingPlayFromXchangeDoes);
    const nextPendingFromReferral = safeInt(result?.pendingPlayFromReferralDoes);
    const nextTotalExchanged = safeInt(result?.totalExchangedHtgEver);

    setCachedWallet(uid, {
      does: nextDoes,
      doesApprovedBalance: nextApprovedDoes,
      doesProvisionalBalance: nextProvisionalDoes,
      exchangeableDoesAvailable: nextExchangeableDoes,
      exchangedGourdes: nextExchanged,
      pendingPlayFromXchangeDoes: nextPendingFromXchange,
      pendingPlayFromReferralDoes: nextPendingFromReferral,
      totalExchangedHtgEver: nextTotalExchanged,
    }, true);
    if (BALANCE_DEBUG) {
      console.log("[BALANCE_DEBUG][XCHANGE] applyWalletMutation success", {
        uid,
        type,
        deltaDoes,
        deltaExchangedGourdes,
        amountGourdes,
        amountDoes,
        afterDoes: nextDoes,
        afterApprovedDoes: nextApprovedDoes,
        afterProvisionalDoes: nextProvisionalDoes,
        exchangeableDoesAvailable: nextExchangeableDoes,
        afterExchanged: nextExchanged,
        pendingPlayFromXchangeDoes: nextPendingFromXchange,
        pendingPlayFromReferralDoes: nextPendingFromReferral,
        totalExchangedHtgEver: nextTotalExchanged,
      });
    }
    emitXchangeUpdated(uid);
    return { ok: true, does: nextDoes };
  } catch (err) {
    if (type === "xchange_buy" || type === "xchange_sell") {
      await syncWalletFundingState(uid);
    }
    console.error("[XCHANGE] applyWalletMutation error", err);
    return {
      ok: false,
      does: getCachedWallet(uid).does,
      error: err?.message || "Erreur mutation wallet",
      code: err?.code || "",
      pendingPlayFromXchangeDoes: safeInt(err?.pendingPlayFromXchangeDoes),
      pendingPlayFromReferralDoes: safeInt(err?.pendingPlayFromReferralDoes),
      pendingPlayTotalDoes: safeInt(err?.pendingPlayTotalDoes),
      exchangeableDoesAvailable: safeInt(err?.exchangeableDoesAvailable),
    };
  }
}

export async function ensureXchangeState(uid = currentUid()) {
  const hydrated = await waitForXchangeBalanceHydration(uid, 2600);
  const state = await syncWalletFundingState(uid);
  if (BALANCE_DEBUG) {
    console.log("[BALANCE_DEBUG][XCHANGE] ensureXchangeState", {
      uid,
      hydrated,
      __userBaseBalance: window.__userBaseBalance,
      __userBalance: window.__userBalance,
      state,
    });
  }
  return state;
}

export function getXchangeState(balance = 0, uid = currentUid()) {
  const wallet = getCachedWallet(uid);
  const totalBalance = safeInt(balance);
  const exchanged = safeSignedInt(wallet.exchangedGourdes);
  const hasNonEmptyFundingBreakdown = (
    safeInt(wallet.approvedHtgAvailable) > 0
    || safeInt(wallet.provisionalHtgAvailable) > 0
    || safeInt(wallet.withdrawableHtg) > 0
    || safeInt(wallet.doesApprovedBalance) > 0
    || safeInt(wallet.doesProvisionalBalance) > 0
  );
  const shouldFallbackToLegacyBalance = wallet.loaded === true
    && !hasNonEmptyFundingBreakdown
    && totalBalance > 0;
  const hasFundingBreakdown = wallet.loaded === true && !shouldFallbackToLegacyBalance;
  const approvedGourdesAvailable = hasFundingBreakdown
    ? safeInt(wallet.approvedHtgAvailable)
    : Math.max(0, totalBalance - exchanged);
  const provisionalGourdesAvailable = hasFundingBreakdown
    ? safeInt(wallet.provisionalHtgAvailable)
    : 0;
  const available = approvedGourdesAvailable + provisionalGourdesAvailable;
  return {
    totalBalance: hasFundingBreakdown ? available : totalBalance,
    availableGourdes: available,
    approvedGourdesAvailable,
    provisionalGourdesAvailable,
    exchangedGourdes: exchanged,
    does: safeInt(wallet.does),
    doesApprovedBalance: safeInt(wallet.doesApprovedBalance),
    doesProvisionalBalance: safeInt(wallet.doesProvisionalBalance),
    exchangeableDoesAvailable: safeInt(wallet.exchangeableDoesAvailable),
    withdrawableHtg: safeInt(wallet.withdrawableHtg),
    accountFrozen: wallet.accountFrozen === true,
    freezeReason: String(wallet.freezeReason || ""),
    rejectedDepositStrikeCount: safeInt(wallet.rejectedDepositStrikeCount),
    pendingPlayFromXchangeDoes: safeInt(wallet.pendingPlayFromXchangeDoes),
    pendingPlayFromReferralDoes: safeInt(wallet.pendingPlayFromReferralDoes),
    pendingPlayTotalDoes: safeInt(wallet.pendingPlayFromXchangeDoes) + safeInt(wallet.pendingPlayFromReferralDoes),
    totalExchangedHtgEver: safeInt(wallet.totalExchangedHtgEver),
    rate: RATE_HTG_TO_DOES,
    loaded: wallet.loaded === true,
  };
}

export function getDoesBalance(uid = currentUid()) {
  return safeInt(getCachedWallet(uid).does);
}

export async function spendDoes(amount, uid = currentUid(), note = "Participation partie") {
  const cost = safeInt(amount);
  if (cost <= 0) return { ok: true, does: getDoesBalance(uid) };
  return applyWalletMutation({
    uid,
    deltaDoes: -cost,
    type: "game_entry",
    note,
    amountDoes: cost,
  });
}

export async function rewardDoes(amount, uid = currentUid(), note = "Gain de partie") {
  const bonus = safeInt(amount);
  if (bonus <= 0) return { ok: true, does: getDoesBalance(uid) };
  return {
    ok: false,
    does: getDoesBalance(uid),
    error: "Mutation game_reward désactivée côté client. Utilise claimWinReward.",
  };
}

async function exchangeHtgToDoes(amountHtg, uid = currentUid()) {
  const amount = safeInt(amountHtg);
  if (amount <= 0) return { ok: false, error: "Montant invalide" };
  return applyWalletMutation({
    uid,
    deltaDoes: amount * RATE_HTG_TO_DOES,
    deltaExchangedGourdes: amount,
    type: "xchange_buy",
    note: "Conversion HTG vers Does",
    amountGourdes: amount,
    amountDoes: amount * RATE_HTG_TO_DOES,
  });
}

async function exchangeDoesToHtg(amountDoes, uid = currentUid()) {
  const amount = safeInt(amountDoes);
  if (amount <= 0) return { ok: false, error: "Montant invalide" };
  if (amount % RATE_HTG_TO_DOES !== 0) {
    return { ok: false, error: `Le montant Does doit être multiple de ${RATE_HTG_TO_DOES}.` };
  }
  const backToHtg = Math.floor(amount / RATE_HTG_TO_DOES);
  return applyWalletMutation({
    uid,
    deltaDoes: -amount,
    deltaExchangedGourdes: -backToHtg,
    type: "xchange_sell",
    note: "Conversion Does vers HTG",
    amountGourdes: backToHtg,
    amountDoes: amount,
  });
}

function ensureXchangeRuleModal() {
  const existing = document.getElementById("xchangeRuleModalOverlay");
  if (existing) return existing;

  const overlay = document.createElement("div");
  overlay.id = "xchangeRuleModalOverlay";
  overlay.className = "fixed inset-0 z-[3450] hidden items-center justify-center bg-black/50 p-4 backdrop-blur-sm";
  overlay.innerHTML = `
    <div id="xchangeRuleModalPanel" class="w-full max-w-md rounded-3xl border border-white/20 bg-[#3F4766]/75 p-5 text-white shadow-[14px_14px_34px_rgba(16,23,40,0.5),-10px_-10px_24px_rgba(112,126,165,0.2)] backdrop-blur-xl sm:p-6">
      <h3 id="xchangeRuleModalTitle" class="text-lg font-bold">Action bloquée</h3>
      <p id="xchangeRuleModalMessage" class="mt-2 text-sm text-white/90"></p>
      <div id="xchangeRuleModalDetails" class="mt-3 rounded-2xl border border-white/20 bg-white/10 p-3 text-xs text-white/85"></div>
      <button id="xchangeRuleModalClose" type="button" class="mt-4 h-11 w-full rounded-2xl border border-[#ffb26e] bg-[#F57C00] text-sm font-semibold text-white shadow-[8px_8px_18px_rgba(163,82,27,0.45),-6px_-6px_14px_rgba(255,175,102,0.22)]">
        Compris
      </button>
    </div>
  `;
  document.body.appendChild(overlay);

  const panel = overlay.querySelector("#xchangeRuleModalPanel");
  const closeBtn = overlay.querySelector("#xchangeRuleModalClose");
  const close = () => {
    overlay.classList.add("hidden");
    overlay.classList.remove("flex");
  };
  if (closeBtn) closeBtn.addEventListener("click", close);
  overlay.addEventListener("click", (ev) => {
    if (ev.target === overlay) close();
  });
  if (panel) panel.addEventListener("click", (ev) => ev.stopPropagation());
  overlay.__close = close;
  return overlay;
}

function showXchangeRuleModal(payload = {}) {
  const overlay = ensureXchangeRuleModal();
  const titleEl = overlay.querySelector("#xchangeRuleModalTitle");
  const messageEl = overlay.querySelector("#xchangeRuleModalMessage");
  const detailsEl = overlay.querySelector("#xchangeRuleModalDetails");
  const lines = Array.isArray(payload.lines) ? payload.lines.filter(Boolean) : [];

  if (titleEl) titleEl.textContent = payload.title || "Action bloquée";
  if (messageEl) messageEl.textContent = payload.message || "Cette action n'est pas autorisée pour le moment.";
  if (detailsEl) {
    detailsEl.textContent = "";
    const safeLines = lines.length > 0 ? lines : ["Respecte les règles de conversion pour continuer."];
    safeLines.forEach((line) => {
      const p = document.createElement("p");
      p.textContent = String(line || "");
      detailsEl.appendChild(p);
    });
  }

  overlay.classList.remove("hidden");
  overlay.classList.add("flex");
}

function ensureXchangeModal() {
  const existing = document.getElementById("xchangeModalOverlay");
  if (existing) return existing;

  const overlay = document.createElement("div");
  overlay.id = "xchangeModalOverlay";
  overlay.className = "fixed inset-0 z-[3200] hidden items-center justify-center bg-black/45 p-4 backdrop-blur-sm";

  overlay.innerHTML = `
    <div id="xchangePanel" class="w-full max-w-md rounded-3xl border border-white/20 bg-[#3F4766]/55 p-5 shadow-[14px_14px_34px_rgba(16,23,40,0.5),-10px_-10px_24px_rgba(112,126,165,0.2)] backdrop-blur-xl sm:p-6">
      <div class="flex items-center justify-between">
        <div>
          <p class="text-xs font-semibold uppercase tracking-[0.16em] text-white/70">Xchange</p>
          <h3 class="mt-1 text-xl font-bold text-white">Xchange en crypto</h3>
        </div>
        <button id="xchangeClose" type="button" class="grid h-10 w-10 place-items-center rounded-full border border-white/20 bg-white/10 text-white">
          <i class="fa-solid fa-xmark"></i>
        </button>
      </div>

      <div class="mt-4 rounded-2xl border border-white/20 bg-white/10 p-4 text-sm text-white/90">
        <p>1 Gourde = <span class="font-semibold text-white">20 Does</span></p>
        <p class="mt-1">Solde HTG: <span id="xchangeAvailableHtg" class="font-semibold text-white">0</span> HTG</p>
        <p class="mt-1">Solde Does: <span id="xchangeAvailableDoes" class="font-semibold text-white">0</span> Does</p>
      </div>

      <div class="mt-4 grid grid-cols-2 gap-2 rounded-2xl border border-white/20 bg-white/10 p-2">
        <button id="xchangeModeBuy" type="button" class="h-10 rounded-xl border border-[#ffb26e] bg-[#F57C00] text-sm font-semibold text-white transition hover:-translate-y-0.5">
          HTG vers Does
        </button>
        <button id="xchangeModeSell" type="button" class="h-10 rounded-xl border border-white/20 bg-white/10 text-sm font-semibold text-white/85 transition hover:bg-white/15">
          Does vers HTG
        </button>
      </div>

      <div class="mt-4 rounded-2xl border border-white/20 bg-white/10 p-4 shadow-[inset_6px_6px_12px_rgba(19,26,43,0.42),inset_-6px_-6px_12px_rgba(120,134,172,0.22)]">
        <label for="xchangeAmount" id="xchangeAmountLabel" class="block text-sm font-medium text-white/90">Montant à échanger (HTG)</label>
        <input id="xchangeAmount" type="number" min="1" step="1" inputmode="numeric" class="mt-2 h-12 w-full rounded-xl border border-white/25 bg-white/10 px-4 text-white outline-none" />
        <p id="xchangeHint" class="mt-2 text-xs text-white/70">Décimales non autorisées.</p>
      </div>

      <div class="mt-4 rounded-2xl border border-white/20 bg-white/10 p-4 text-sm text-white/90">
        <div class="flex items-center gap-2">
          <img src="./does.png" alt="Does" class="h-5 w-5 rounded-full object-cover" data-hide-on-error="1" />
          <p id="xchangePreviewText">Vous recevrez: <span id="xchangePreview" class="font-semibold text-white">0</span> Does</p>
        </div>
      </div>

      <div id="xchangeError" class="mt-3 min-h-5 text-sm text-[#ffb0b0]"></div>

      <button id="xchangeSubmit" type="button" class="mt-2 h-12 w-full rounded-2xl border border-[#ffb26e] bg-[#F57C00] font-semibold text-white shadow-[9px_9px_20px_rgba(155,78,25,0.45),-7px_-7px_16px_rgba(255,173,96,0.2)] transition hover:-translate-y-0.5">
        Xchanger
      </button>
    </div>
  `;

  document.body.appendChild(overlay);
  bindHideOnErrorImages(overlay);

  const panel = overlay.querySelector("#xchangePanel");
  const closeBtn = overlay.querySelector("#xchangeClose");
  const amountInput = overlay.querySelector("#xchangeAmount");
  const previewTextEl = overlay.querySelector("#xchangePreviewText");
  const availableHtgEl = overlay.querySelector("#xchangeAvailableHtg");
  const availableDoesEl = overlay.querySelector("#xchangeAvailableDoes");
  const modeBuyBtn = overlay.querySelector("#xchangeModeBuy");
  const modeSellBtn = overlay.querySelector("#xchangeModeSell");
  const amountLabelEl = overlay.querySelector("#xchangeAmountLabel");
  const hintEl = overlay.querySelector("#xchangeHint");
  const errorEl = overlay.querySelector("#xchangeError");
  const submitBtn = overlay.querySelector("#xchangeSubmit");
  let mode = "buy";
  const getPreviewNode = () => overlay.querySelector("#xchangePreview");

  const setModeUi = (nextMode, state) => {
    mode = nextMode;
    const safeState = state || getXchangeState(window.__userBaseBalance || window.__userBalance || 0, currentUid());

    if (modeBuyBtn) {
      modeBuyBtn.className = mode === "buy"
        ? "h-10 rounded-xl border border-[#ffb26e] bg-[#F57C00] text-sm font-semibold text-white transition hover:-translate-y-0.5"
        : "h-10 rounded-xl border border-white/20 bg-white/10 text-sm font-semibold text-white/85 transition hover:bg-white/15";
    }
    if (modeSellBtn) {
      modeSellBtn.className = mode === "sell"
        ? "h-10 rounded-xl border border-[#ffb26e] bg-[#F57C00] text-sm font-semibold text-white transition hover:-translate-y-0.5"
        : "h-10 rounded-xl border border-white/20 bg-white/10 text-sm font-semibold text-white/85 transition hover:bg-white/15";
    }

    if (amountLabelEl) {
      amountLabelEl.textContent = mode === "buy"
        ? "Montant à échanger (HTG)"
        : "Montant à convertir (Does)";
    }
    if (hintEl) {
      hintEl.textContent = mode === "buy"
        ? "Décimales non autorisées."
        : `Décimales non autorisées. Le montant doit être multiple de ${RATE_HTG_TO_DOES} Does.`;
    }
    if (previewTextEl) {
      const currentPreview = String(getPreviewNode()?.textContent || "0");
      previewTextEl.textContent = "";
      const label = document.createTextNode("Vous recevrez: ");
      const value = document.createElement("span");
      value.id = "xchangePreview";
      value.className = "font-semibold text-white";
      value.textContent = currentPreview;
      const suffix = document.createTextNode(mode === "buy" ? " Does" : " HTG");
      previewTextEl.appendChild(label);
      previewTextEl.appendChild(value);
      previewTextEl.appendChild(suffix);
    }
    if (availableHtgEl) availableHtgEl.textContent = String(safeState.availableGourdes);
    if (availableDoesEl) availableDoesEl.textContent = String(safeState.does || 0);
  };

  const refreshPreview = () => {
    const raw = String(amountInput?.value || "").trim();
    const amount = /^\d+$/.test(raw) ? Number(raw) : 0;
    const value = mode === "buy" ? amount * RATE_HTG_TO_DOES : Math.floor(amount / RATE_HTG_TO_DOES);
    const previewNode = getPreviewNode();
    if (previewNode) previewNode.textContent = String(value);
  };

  const close = () => {
    overlay.classList.add("hidden");
    overlay.classList.remove("flex");
    if (errorEl) errorEl.textContent = "";
  };

  const open = async () => {
    const state = await ensureXchangeState(currentUid());
    if (state.accountFrozen) {
      showXchangeRuleModal({
        title: "Compte gelé",
        message: "Ton compte a été temporairement gelé après plusieurs dépôts refusés. Contacte l'assistance.",
        lines: [
          "Le dépôt, le retrait, le Xchange et les parties sont bloqués.",
          "Le support reste disponible pour demander un dégel.",
        ],
      });
      return;
    }
    if (availableHtgEl) availableHtgEl.textContent = String(state.availableGourdes);
    if (availableDoesEl) availableDoesEl.textContent = String(state.does || 0);
    if (amountInput) amountInput.value = "";
    if (errorEl) errorEl.textContent = "";
    setModeUi("buy", state);
    const previewNode = getPreviewNode();
    if (previewNode) previewNode.textContent = "0";
    overlay.classList.remove("hidden");
    overlay.classList.add("flex");
  };

  overlay.addEventListener("click", (ev) => {
    if (ev.target === overlay) close();
  });
  if (panel) panel.addEventListener("click", (ev) => ev.stopPropagation());
  if (closeBtn) closeBtn.addEventListener("click", close);
  if (amountInput) amountInput.addEventListener("input", refreshPreview);
  if (modeBuyBtn) {
    modeBuyBtn.addEventListener("click", async () => {
      await withButtonLoading(modeBuyBtn, async () => {
        const state = await ensureXchangeState(currentUid());
        if (amountInput) amountInput.value = "";
        if (errorEl) errorEl.textContent = "";
        setModeUi("buy", state);
        refreshPreview();
      }, { loadingLabel: "..." });
    });
  }
  if (modeSellBtn) {
    modeSellBtn.addEventListener("click", async () => {
      await withButtonLoading(modeSellBtn, async () => {
        const state = await ensureXchangeState(currentUid());
        if (amountInput) amountInput.value = "";
        if (errorEl) errorEl.textContent = "";
        setModeUi("sell", state);
        refreshPreview();
      }, { loadingLabel: "..." });
    });
  }

  if (submitBtn) {
    submitBtn.addEventListener("click", async () => {
      await withButtonLoading(submitBtn, async () => {
        const state = await ensureXchangeState(currentUid());
        const raw = String(amountInput?.value || "").trim();

        if (!/^\d+$/.test(raw)) {
          if (errorEl) errorEl.textContent = "Entrez un nombre entier valide.";
          return;
        }

        const amount = Number(raw);
        if (BALANCE_DEBUG) {
          console.log("[BALANCE_DEBUG][XCHANGE] submit attempt", {
            uid: currentUid(),
            mode,
            raw,
            amount,
            state,
          });
        }
        if (amount <= 0) {
          if (errorEl) errorEl.textContent = "Le montant doit être supérieur à zéro.";
          return;
        }

        if (mode === "buy") {
          if (state.accountFrozen) {
            if (errorEl) errorEl.textContent = "Compte gelé. Contacte l'assistance.";
            return;
          }
          if (amount > state.availableGourdes) {
            if (errorEl) errorEl.textContent = "Montant supérieur au solde disponible.";
            return;
          }
          const res = await exchangeHtgToDoes(amount, currentUid());
          if (!res.ok) {
            if (errorEl) errorEl.textContent = res.error || "Erreur de conversion.";
            return;
          }
        } else {
          if (state.accountFrozen) {
            if (errorEl) errorEl.textContent = "Compte gelé. Contacte l'assistance.";
            return;
          }
          if (amount > state.exchangeableDoesAvailable) {
            if (errorEl) errorEl.textContent = "Montant supérieur aux Does actuellement échangeables.";
            return;
          }
          const res = await exchangeDoesToHtg(amount, currentUid());
          if (!res.ok) {
            if (res.code === "account-frozen") {
              showXchangeRuleModal({
                title: "Compte gelé",
                message: res.error || "Ton compte a été temporairement gelé après plusieurs dépôts refusés.",
                lines: ["Contacte l'assistance pour demander un dégel."],
              });
            }
            if (res.code === "play-required-before-sell") {
              const pendingFromXchange = safeInt(res.pendingPlayFromXchangeDoes);
              const pendingFromReferral = safeInt(res.pendingPlayFromReferralDoes);
              const pendingTotal = safeInt(res.pendingPlayTotalDoes || (pendingFromXchange + pendingFromReferral));
              const exchangeableDoesAvailable = safeInt(res.exchangeableDoesAvailable);
              showXchangeRuleModal({
                title: "Conversion bloquée",
                message: `Tu peux reconvertir ${exchangeableDoesAvailable} Does pour le moment. Le reste sera débloqué en jouant.`,
                lines: [
                  `Reste à jouer (Does achetés): ${pendingFromXchange} Does`,
                  `Reste à jouer (bonus parrainage): ${pendingFromReferral} Does`,
                  `Total encore bloqué: ${pendingTotal} Does`,
                  "Joue des parties pour débloquer plus de reconversion.",
                ],
              });
            }
            if (errorEl) errorEl.textContent = res.error || "Erreur de conversion.";
            return;
          }
        }

        close();
      }, { loadingLabel: "Conversion..." });
    });
  }

  overlay.__openXchange = open;
  return overlay;
}

export function mountXchangeModal(options = {}) {
  const { triggerSelector = "#profileXchangeBtn" } = options;
  const overlay = ensureXchangeModal();
  const trigger = document.querySelector(triggerSelector);

  if (trigger && overlay.__openXchange && !trigger.dataset.boundXchange) {
    trigger.dataset.boundXchange = "1";
    trigger.addEventListener("click", () => {
      overlay.__openXchange();
    });
  }
}

onAuthStateChanged(auth, async (user) => {
  if (!user) {
    activeUid = null;
    if (walletUnsub) {
      walletUnsub();
      walletUnsub = null;
    }
    emitXchangeUpdated("guest");
    return;
  }

  activeUid = user.uid;
  startWalletWatcher(activeUid);
  emitXchangeUpdated(activeUid);
});
