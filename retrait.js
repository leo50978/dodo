import {
  auth,
  db,
  collection,
  getDocs,
  getDoc,
  doc,
  query,
  where,
} from "./firebase-init.js";
import {
  createWithdrawalSecure,
  getDepositFundingStatusSecure,
  getPublicPaymentOptionsSecure,
} from "./secure-functions.js";
import { waitForBalanceHydration } from "./solde.js";
import { getXchangeState } from "./xchange.js";
import { SUPPORT_WHATSAPP_PHONE } from "./support-contact.js";
import {
  buildWhatsappUrlForKey,
  getWhatsappContactLabel,
  refreshWhatsappModalContacts,
} from "./whatsapp-modal-config.js";
const MIN_WITHDRAWAL_HTG = 50;
const BALANCE_DEBUG = true;
const ASSISTANCE_PHONE = SUPPORT_WHATSAPP_PHONE;
const buildRetraitWhatsAppUrl = (message = "") => {
  return buildWhatsappUrlForKey("withdrawal_assistance", message, ASSISTANCE_PHONE);
};
void refreshWhatsappModalContacts().catch(() => {});

function createClientRequestId(prefix = "wd") {
  const safePrefix = String(prefix || "req").replace(/[^a-zA-Z0-9_-]/g, "").slice(0, 12) || "req";
  if (globalThis.crypto && typeof globalThis.crypto.randomUUID === "function") {
    return `${safePrefix}_${globalThis.crypto.randomUUID()}`;
  }
  return `${safePrefix}_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
}

function escapeHtml(input) {
  return String(input || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function formatAmount(value) {
  const amount = Number(value || 0);
  return new Intl.NumberFormat("fr-HT", {
    style: "currency",
    currency: "HTG",
    maximumFractionDigits: 0,
  }).format(amount);
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

function safeInt(value) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.max(0, Math.floor(n)) : 0;
}

function getBalanceBaseForUi() {
  const base = window.__userBaseBalance;
  const fallback = window.__userBalance;
  if (base === null || typeof base === "undefined" || Number.isNaN(Number(base))) {
    return Number(fallback || 0);
  }
  return Number(base);
}

function getLocalWithdrawalUiState(uid) {
  const baseForUi = getBalanceBaseForUi();
  const xState = getXchangeState(baseForUi, uid);
  const localAvailableHtg = Math.max(0, Number(xState?.availableGourdes || 0));

  if (BALANCE_DEBUG) {
    console.log("[BALANCE_DEBUG][RETRAIT] local ui state", {
      uid: uid || null,
      baseForUi,
      __userBaseBalance: window.__userBaseBalance,
      __userBalance: window.__userBalance,
      exchangedGourdes: Number(xState?.exchangedGourdes || 0),
      localAvailableHtg,
      does: Number(xState?.does || 0),
      walletLoaded: xState?.loaded === true,
    });
  }

  return {
    uid,
    baseForUi,
    xState,
    localAvailableHtg,
  };
}

async function resolveWithdrawalUiState(uid) {
  const localState = getLocalWithdrawalUiState(uid);
  const ruleStatus = await getWithdrawalRuleStatus(uid);
  const withdrawableHtg = ruleStatus.canWithdraw
    ? safeInt(typeof ruleStatus.withdrawableHtg === "number" ? ruleStatus.withdrawableHtg : localState.localAvailableHtg)
    : 0;

  if (BALANCE_DEBUG) {
    console.log("[BALANCE_DEBUG][RETRAIT] resolved withdrawal state", {
      uid: uid || null,
      baseForUi: localState.baseForUi,
      localAvailableHtg: localState.localAvailableHtg,
      exchangedGourdes: Number(localState.xState?.exchangedGourdes || 0),
      approvedDepositsHtg: ruleStatus.approvedDepositsHtg,
      convertedHtg: ruleStatus.convertedHtg,
      remainingToExchangeHtg: ruleStatus.remainingToExchangeHtg,
      canWithdraw: ruleStatus.canWithdraw,
      withdrawableHtg,
    });
  }

  return {
    ...localState,
    ruleStatus,
    withdrawableHtg,
  };
}

function resolveMethodAssetPath(value) {
  const out = String(value || "").trim();
  if (!out) return "";

  const baseValue = out.replace(/\\/g, "/").split(/[?#]/)[0];
  const fileName = baseValue.split("/").pop() || "";
  if (!/^[a-zA-Z0-9._-]+\.(png|jpe?g|gif|webp|svg)$/i.test(fileName)) {
    return "";
  }
  return `./${fileName}`;
}

function computeOrderAmount(order) {
  if (typeof order?.amount === "number" && Number.isFinite(order.amount)) {
    return safeInt(order.amount);
  }
  if (!Array.isArray(order?.items)) return 0;
  return safeInt(order.items.reduce((sum, item) => {
    const price = Number(item?.price) || 0;
    const quantity = Number(item?.quantity) || 1;
    return sum + (price * quantity);
  }, 0));
}

function isWelcomeBonusOrder(order) {
  const orderType = String(order?.orderType || order?.kind || "").trim().toLowerCase();
  return order?.isWelcomeBonus === true || orderType === "welcome_bonus";
}

function computeRealDepositAmount(order) {
  return isWelcomeBonusOrder(order) ? 0 : computeOrderAmount(order);
}

export async function getWithdrawalRuleStatus(uid) {
  const approvedOrdersQuery = query(
    collection(db, "clients", uid, "orders"),
    where("status", "==", "approved")
  );

  const [ordersSnap, clientSnap, fundingStatus] = await Promise.all([
    getDocs(approvedOrdersQuery),
    getDoc(doc(db, "clients", uid)),
    getDepositFundingStatusSecure({}).catch(() => null),
  ]);

  const approvedDepositsHtgFallback = ordersSnap.docs.reduce((sum, item) => {
    const data = item.data() || {};
    return sum + computeRealDepositAmount(data);
  }, 0);

  const clientData = clientSnap.exists() ? (clientSnap.data() || {}) : {};
  let convertedHtgFallback = safeInt(clientData.totalExchangedHtgEver);
  if (typeof clientData.totalExchangedHtgEver !== "number") {
    const xchangesSnap = await getDocs(collection(db, "clients", uid, "xchanges"));
    convertedHtgFallback = xchangesSnap.docs.reduce((sum, item) => {
      const data = item.data() || {};
      if (data.type !== "xchange_buy") return sum;
      return sum + safeInt(data.amountGourdes);
    }, 0);
  }

  const approvedDepositsHtg = safeInt(
    typeof fundingStatus?.approvedDepositsHtg === "number"
      ? fundingStatus.approvedDepositsHtg
      : approvedDepositsHtgFallback
  );
  const convertedHtg = safeInt(
    typeof fundingStatus?.totalExchangedApprovedHtg === "number"
      ? fundingStatus.totalExchangedApprovedHtg
      : convertedHtgFallback
  );
  const remainingToExchangeHtg = safeInt(
    typeof fundingStatus?.remainingToExchangeHtg === "number"
      ? fundingStatus.remainingToExchangeHtg
      : Math.max(0, approvedDepositsHtg - convertedHtg)
  );
  const accountFrozen = fundingStatus?.accountFrozen === true || clientData.accountFrozen === true;
  const withdrawalHold = fundingStatus?.withdrawalHold === true || clientData.withdrawalHold === true;
  const withdrawalBlocked = accountFrozen || withdrawalHold;
  const withdrawableHtg = withdrawalBlocked
    ? 0
    : safeInt(
      typeof fundingStatus?.withdrawableHtg === "number"
        ? fundingStatus.withdrawableHtg
        : clientData.withdrawableHtg
    );

  return {
    approvedDepositsHtg,
    convertedHtg,
    remainingToExchangeHtg,
    welcomeBonusHtgAvailable: safeInt(
      typeof fundingStatus?.welcomeBonusHtgAvailable === "number"
        ? fundingStatus.welcomeBonusHtgAvailable
        : clientData.welcomeBonusHtgAvailable
    ),
    canWithdraw: !withdrawalBlocked && withdrawableHtg > 0,
    withdrawableHtg,
    accountFrozen,
    withdrawalHold,
    withdrawalHoldReason: String(fundingStatus?.withdrawalHoldReason || clientData.withdrawalHoldReason || ""),
    withdrawalHoldAtMs: safeInt(
      typeof fundingStatus?.withdrawalHoldAtMs === "number"
        ? fundingStatus.withdrawalHoldAtMs
        : clientData.withdrawalHoldAtMs
    ),
    rejectedDepositStrikeCount: safeInt(
      typeof fundingStatus?.rejectedDepositStrikeCount === "number"
        ? fundingStatus.rejectedDepositStrikeCount
        : clientData.rejectedDepositStrikeCount
    ),
    freezeReason: String(fundingStatus?.freezeReason || clientData.freezeReason || ""),
    provisionalHtgAvailable: safeInt(
      typeof fundingStatus?.provisionalHtgAvailable === "number"
        ? fundingStatus.provisionalHtgAvailable
        : clientData.provisionalHtgAvailable
    ),
  };
}

function ensureRetraitRuleModal() {
  const existing = document.getElementById("retraitRuleModalOverlay");
  if (existing) return existing;

  const overlay = document.createElement("div");
  overlay.id = "retraitRuleModalOverlay";
  overlay.className = "fixed inset-0 z-[3460] hidden items-center justify-center bg-black/50 p-4 backdrop-blur-sm";
  overlay.innerHTML = `
    <div id="retraitRuleModalPanel" class="w-full max-w-md rounded-3xl border border-white/20 bg-[#3F4766]/78 p-5 text-white shadow-[14px_14px_34px_rgba(16,23,40,0.5),-10px_-10px_24px_rgba(112,126,165,0.2)] backdrop-blur-xl sm:p-6">
      <h3 id="retraitRuleModalTitle" class="text-lg font-bold">Retrait bloqué</h3>
      <p id="retraitRuleModalMessage" class="mt-2 text-sm text-white/90"></p>
      <div id="retraitRuleModalDetails" class="mt-3 rounded-2xl border border-white/20 bg-white/10 p-3 text-xs text-white/85"></div>
      <div class="mt-4 grid gap-2 sm:grid-cols-2">
        <button id="retraitRuleModalContact" type="button" class="h-11 w-full rounded-2xl border border-white/20 bg-white/10 text-sm font-semibold text-white">
          Contacter l'assistance
        </button>
        <button id="retraitRuleModalClose" type="button" class="h-11 w-full rounded-2xl border border-[#ffb26e] bg-[#F57C00] text-sm font-semibold text-white shadow-[8px_8px_18px_rgba(163,82,27,0.45),-6px_-6px_14px_rgba(255,175,102,0.22)]">
          Compris
        </button>
      </div>
    </div>
  `;

  document.body.appendChild(overlay);

  const panel = overlay.querySelector("#retraitRuleModalPanel");
  const closeBtn = overlay.querySelector("#retraitRuleModalClose");
  const contactBtn = overlay.querySelector("#retraitRuleModalContact");
  const close = () => {
    overlay.classList.add("hidden");
    overlay.classList.remove("flex");
  };

  if (closeBtn) closeBtn.addEventListener("click", close);
  if (contactBtn) {
    contactBtn.addEventListener("click", () => {
      window.open(buildRetraitWhatsAppUrl(), "_blank", "noopener,noreferrer");
    });
  }
  overlay.addEventListener("click", (ev) => {
    if (ev.target === overlay) close();
  });
  if (panel) panel.addEventListener("click", (ev) => ev.stopPropagation());

  return overlay;
}

function showRetraitRuleModal(payload = {}) {
  const overlay = ensureRetraitRuleModal();
  const titleEl = overlay.querySelector("#retraitRuleModalTitle");
  const messageEl = overlay.querySelector("#retraitRuleModalMessage");
  const detailsEl = overlay.querySelector("#retraitRuleModalDetails");
  const lines = Array.isArray(payload.lines) ? payload.lines.filter(Boolean) : [];

  if (titleEl) titleEl.textContent = payload.title || "Retrait bloqué";
  if (messageEl) messageEl.textContent = payload.message || "Cette action n'est pas autorisée pour le moment.";
  if (detailsEl) {
    detailsEl.textContent = "";
    const safeLines = lines.length > 0 ? lines : ["Consulte les règles puis réessaie."];
    safeLines.forEach((line) => {
      const p = document.createElement("p");
      p.textContent = String(line || "");
      detailsEl.appendChild(p);
    });
  }

  overlay.classList.remove("hidden");
  overlay.classList.add("flex");
}

function hasPendingExamWithdrawalLock(ruleStatus = {}) {
  return safeInt(ruleStatus?.provisionalHtgAvailable) > 0
    && safeInt(ruleStatus?.remainingToExchangeHtg) <= 0;
}

function openWhatsappForWithdrawal(phone, amount = 0) {
  const text = amount > 0
    ? `Bonjour, je viens de soumettre un retrait de ${amount} HTG et je veux un traitement rapide.`
    : "Bonjour, je viens de soumettre un retrait et je veux un traitement rapide.";
  window.open(buildRetraitWhatsAppUrl(text), "_blank", "noopener,noreferrer");
}

function ensureRetraitSuccessModal() {
  const existing = document.getElementById("retraitSuccessModalOverlay");
  if (existing) return existing;

  const assistanceLabel = getWhatsappContactLabel("withdrawal_assistance", ASSISTANCE_PHONE) || `+${ASSISTANCE_PHONE}`;
  const overlay = document.createElement("div");
  overlay.id = "retraitSuccessModalOverlay";
  overlay.className = "fixed inset-0 z-[3470] hidden items-end justify-center bg-black/55 p-0 backdrop-blur-sm sm:items-center sm:p-4";
  overlay.innerHTML = `
    <div id="retraitSuccessModalPanel" class="flex max-h-[92dvh] w-full max-w-lg flex-col overflow-hidden rounded-t-[30px] border border-white/20 bg-[#101827]/94 text-white shadow-[18px_18px_40px_rgba(2,6,17,0.58),-10px_-10px_24px_rgba(24,35,58,0.14)] sm:max-h-[86vh] sm:rounded-[30px]">
      <div class="flex-1 overflow-y-auto px-5 pb-[max(1.1rem,env(safe-area-inset-bottom))] pt-5 sm:px-6 sm:pt-6">
        <div class="flex items-start gap-3">
          <div class="grid h-12 w-12 shrink-0 place-items-center rounded-2xl border border-emerald-300/20 bg-emerald-500/15 text-emerald-100">
            <i class="fa-solid fa-circle-check text-lg"></i>
          </div>
          <div class="min-w-0">
            <p class="text-[11px] font-semibold uppercase tracking-[0.16em] text-emerald-100/70">Retrait soumis</p>
            <h3 class="mt-1 text-xl font-bold text-white">Ta demande a été envoyée avec succès</h3>
          </div>
        </div>

        <div class="mt-4 rounded-2xl border border-[#ffb26e]/25 bg-[#1b2437]/90 p-4">
          <p class="text-[11px] font-semibold uppercase tracking-[0.16em] text-[#ffcf9f]">Priorité</p>
          <p class="mt-1 text-base font-semibold text-white">Contact agent obligatoire</p>
          <p class="mt-2 text-sm leading-6 text-white/80">
            Tu dois obligatoirement contacter un agent sur WhatsApp et l'appeler pour recevoir ton retrait. Sinon, tu risques de ne pas le recevoir.
          </p>
          <div class="mt-4 grid gap-2">
            <button id="retraitSuccessWhatsapp1" type="button" class="min-h-[48px] w-full rounded-2xl border border-emerald-300/20 bg-emerald-500/15 px-4 text-sm font-semibold text-emerald-50">
              Ecrire sur WhatsApp ${assistanceLabel}
            </button>
          </div>
        </div>

        <div class="mt-4 rounded-2xl border border-white/10 bg-white/5 p-4">
          <p id="retraitSuccessPrimary" class="text-sm leading-6 text-white/90">
            Tu peux vérifier l'état de ton retrait dans Opérations en cours.
          </p>
          <p id="retraitSuccessSecondary" class="mt-2 text-sm leading-6 text-white/72">
            Tu peux aussi annuler le retrait à tout moment depuis cette section.
          </p>
        </div>
      </div>

      <div class="grid gap-2 border-t border-white/10 bg-[#101827]/96 px-5 pb-[max(1rem,env(safe-area-inset-bottom))] pt-4 sm:grid-cols-2 sm:px-6">
        <button id="retraitSuccessPending" type="button" class="h-11 rounded-2xl border border-white/15 bg-white/10 text-sm font-semibold text-white">
          Voir plus tard
        </button>
        <button id="retraitSuccessClose" type="button" class="h-11 rounded-2xl border border-[#ffb26e] bg-[#F57C00] text-sm font-semibold text-white shadow-[8px_8px_18px_rgba(163,82,27,0.45),-6px_-6px_14px_rgba(255,175,102,0.22)]">
          Compris
        </button>
      </div>
    </div>
  `;

  document.body.appendChild(overlay);

  const panel = overlay.querySelector("#retraitSuccessModalPanel");
  const closeBtn = overlay.querySelector("#retraitSuccessClose");
  const laterBtn = overlay.querySelector("#retraitSuccessPending");
  const whatsappBtn1 = overlay.querySelector("#retraitSuccessWhatsapp1");
  const close = () => {
    overlay.classList.add("hidden");
    overlay.classList.remove("flex");
  };

  if (closeBtn) closeBtn.addEventListener("click", close);
  if (laterBtn) laterBtn.addEventListener("click", close);
  if (whatsappBtn1) {
    whatsappBtn1.addEventListener("click", () => {
      openWhatsappForWithdrawal(ASSISTANCE_PHONE, safeInt(overlay.dataset.amount || 0));
    });
  }
  overlay.addEventListener("click", (ev) => {
    if (ev.target === overlay) close();
  });
  if (panel) panel.addEventListener("click", (ev) => ev.stopPropagation());

  return overlay;
}

function showRetraitSuccessModal(payload = {}) {
  const overlay = ensureRetraitSuccessModal();
  const primaryEl = overlay.querySelector("#retraitSuccessPrimary");
  const secondaryEl = overlay.querySelector("#retraitSuccessSecondary");
  const amount = safeInt(payload.amount || 0);
  overlay.dataset.amount = String(amount);

  if (primaryEl) {
    primaryEl.textContent = amount > 0
      ? `Ta demande de retrait de ${amount} HTG a été soumise avec succès. Tu dois maintenant contacter un agent pour recevoir ton retrait.`
      : "Ta demande de retrait a été soumise avec succès. Tu dois maintenant contacter un agent pour recevoir ton retrait.";
  }
  if (secondaryEl) {
    secondaryEl.textContent = "Si tu ne contactes pas un agent sur WhatsApp et par appel, tu risques de ne pas recevoir ton retrait.";
  }

  overlay.classList.remove("hidden");
  overlay.classList.add("flex");
}

async function loadActiveMethods() {
  const payload = await getPublicPaymentOptionsSecure({});
  return Array.isArray(payload?.methods)
    ? payload.methods.filter((m) => m && m.isActive !== false)
    : [];
}

function ensureRetraitModal() {
  const existing = document.getElementById("retraitModalOverlay");
  if (existing) return existing;

  const overlay = document.createElement("div");
  overlay.id = "retraitModalOverlay";
  overlay.className = "fixed inset-0 z-[3300] hidden items-center justify-center bg-black/45 p-4 backdrop-blur-sm";

  overlay.innerHTML = `
    <div id="retraitPanel" class="w-full max-w-2xl rounded-3xl border border-white/20 bg-[#3F4766]/55 p-5 shadow-[14px_14px_34px_rgba(16,23,40,0.5),-10px_-10px_24px_rgba(112,126,165,0.2)] backdrop-blur-xl sm:p-6">
      <div class="flex items-center justify-between">
        <div>
          <p class="text-xs font-semibold uppercase tracking-[0.16em] text-white/70">Retrait</p>
          <h3 class="mt-1 text-2xl font-bold text-white">Faire un retrait</h3>
        </div>
        <button id="retraitClose" type="button" class="grid h-10 w-10 place-items-center rounded-full border border-white/20 bg-white/10 text-white">
          <i class="fa-solid fa-xmark"></i>
        </button>
      </div>

      <div class="mt-3 rounded-xl border border-white/15 bg-white/10 p-3 text-sm text-white/80">
        Solde disponible: <span id="retraitAvailable" class="font-semibold text-white">0 HTG</span>
      </div>

      <div class="mt-4">
        <div id="retraitStep1">
          <p class="text-sm font-semibold text-white">Étape 1: Choisir une méthode</p>
          <div id="retraitMethods" class="mt-3 grid gap-2"></div>
        </div>

        <div id="retraitStep2" class="hidden">
          <p class="text-sm font-semibold text-white">Étape 2: Vos informations de retrait</p>
          <div class="mt-3 grid grid-cols-1 gap-3 sm:grid-cols-2">
            <input id="retraitAmount" type="number" min="50" step="1" placeholder="Montant HTG" class="h-12 rounded-xl border border-white/25 bg-white/10 px-4 text-white outline-none sm:col-span-2" />
            <input id="retraitFirstName" type="text" placeholder="Nom" class="h-12 rounded-xl border border-white/25 bg-white/10 px-4 text-white outline-none" />
            <input id="retraitLastName" type="text" placeholder="Prénom" class="h-12 rounded-xl border border-white/25 bg-white/10 px-4 text-white outline-none" />
            <input id="retraitPhone" type="tel" placeholder="Numéro de téléphone" class="h-12 rounded-xl border border-white/25 bg-white/10 px-4 text-white outline-none sm:col-span-2" />
          </div>
          <p id="retraitMethodLabel" class="mt-3 text-xs text-white/75"></p>
        </div>
      </div>

      <div id="retraitError" class="mt-3 min-h-5 text-sm text-[#ffb0b0]"></div>

      <div class="mt-2 flex flex-col-reverse gap-2 sm:flex-row sm:justify-end">
        <button id="retraitBack" type="button" class="hidden h-11 rounded-xl border border-white/20 bg-white/10 px-5 text-sm font-semibold text-white">Retour</button>
        <button id="retraitNext" type="button" class="h-11 rounded-xl border border-[#ffb26e] bg-[#F57C00] px-5 text-sm font-semibold text-white shadow-[9px_9px_20px_rgba(155,78,25,0.45),-7px_-7px_16px_rgba(255,173,96,0.2)]">Suivant</button>
      </div>
    </div>
  `;

  document.body.appendChild(overlay);

  const panel = overlay.querySelector("#retraitPanel");
  const closeBtn = overlay.querySelector("#retraitClose");
  const errorEl = overlay.querySelector("#retraitError");
  const availableEl = overlay.querySelector("#retraitAvailable");
  const methodsEl = overlay.querySelector("#retraitMethods");
  const step1El = overlay.querySelector("#retraitStep1");
  const step2El = overlay.querySelector("#retraitStep2");
  const methodLabelEl = overlay.querySelector("#retraitMethodLabel");
  const nextBtn = overlay.querySelector("#retraitNext");
  const backBtn = overlay.querySelector("#retraitBack");
  const amountInput = overlay.querySelector("#retraitAmount");
  const firstNameInput = overlay.querySelector("#retraitFirstName");
  const lastNameInput = overlay.querySelector("#retraitLastName");
  const phoneInput = overlay.querySelector("#retraitPhone");

  let step = 1;
  let selectedMethod = null;
  let methods = [];
  let availabilityToken = 0;
  let isSubmitting = false;
  let activeRequestId = "";

  const close = () => {
    overlay.classList.add("hidden");
    overlay.classList.remove("flex");
    if (errorEl) errorEl.textContent = "";
    isSubmitting = false;
    activeRequestId = "";
    if (nextBtn) {
      nextBtn.disabled = false;
      nextBtn.textContent = step === 1 ? "Suivant" : "Soumettre";
      nextBtn.classList.remove("opacity-70", "cursor-not-allowed");
    }
  };

  const setStep = (value) => {
    step = value;
    if (step1El) step1El.classList.toggle("hidden", step !== 1);
    if (step2El) step2El.classList.toggle("hidden", step !== 2);
    if (backBtn) backBtn.classList.toggle("hidden", step !== 2);
    if (nextBtn) nextBtn.textContent = step === 1 ? "Suivant" : "Soumettre";
  };

  const refreshAvailability = async () => {
    const user = auth.currentUser;
    if (!user?.uid) {
      if (availableEl) availableEl.textContent = formatAmount(0);
      return {
        uid: "",
        baseForUi: 0,
        xState: getXchangeState(0),
        ruleStatus: null,
        localAvailableHtg: 0,
        withdrawableHtg: 0,
      };
    }

    const hydrated = await waitForBalanceHydration(user.uid, 2600);
    if (BALANCE_DEBUG) {
      console.log("[BALANCE_DEBUG][RETRAIT] balance hydration before availability", {
        uid: user.uid,
        hydrated,
      });
    }

    const localState = getLocalWithdrawalUiState(user.uid);
    if (availableEl) availableEl.textContent = formatAmount(localState.localAvailableHtg);

    const token = ++availabilityToken;
    try {
      const resolvedState = await resolveWithdrawalUiState(user.uid);
      if (token !== availabilityToken) return resolvedState;
      if (availableEl) availableEl.textContent = formatAmount(resolvedState.withdrawableHtg);
      return resolvedState;
    } catch (err) {
      console.error("Erreur calcul disponibilité retrait modal:", err);
      if (token === availabilityToken && availableEl) {
        availableEl.textContent = formatAmount(localState.localAvailableHtg);
      }
      return {
        ...localState,
        ruleStatus: null,
        withdrawableHtg: localState.localAvailableHtg,
      };
    }
  };

  const setSubmitting = (value) => {
    isSubmitting = value === true;
    if (!nextBtn) return;
    nextBtn.disabled = isSubmitting;
    nextBtn.classList.toggle("opacity-70", isSubmitting);
    nextBtn.classList.toggle("cursor-not-allowed", isSubmitting);
    nextBtn.textContent = isSubmitting ? "Traitement..." : (step === 1 ? "Suivant" : "Soumettre");
  };

  const renderMethods = () => {
    if (!methodsEl) return;
    if (!methods.length) {
      methodsEl.innerHTML = `<p class="text-sm text-white/75">Aucune méthode active.</p>`;
      return;
    }
    methodsEl.innerHTML = methods.map((m) => {
      const imagePath = resolveMethodAssetPath(m.image);
      return `
      <button type="button" data-method-id="${escapeHtml(m.id)}" class="retrait-method w-full rounded-xl border border-white/20 bg-white/10 p-3 text-left text-white transition hover:bg-white/15">
        <div class="flex items-center gap-3">
          ${imagePath ? `
            <img src="${escapeHtml(imagePath)}" alt="${escapeHtml(m.name || "Méthode")}" class="h-10 w-10 rounded-xl object-cover border border-white/15 bg-white/10" data-hide-on-error="1">
          ` : `
            <div class="grid h-10 w-10 place-items-center rounded-xl border border-white/15 bg-white/10">
              <i class="fa-solid fa-wallet text-white/80"></i>
            </div>
          `}
          <p class="text-sm font-semibold">${escapeHtml(m.name || "Méthode")}</p>
        </div>
      </button>
    `;
    }).join("");
    bindHideOnErrorImages(methodsEl);

    methodsEl.querySelectorAll(".retrait-method").forEach((btn) => {
      btn.addEventListener("click", () => {
        const id = btn.getAttribute("data-method-id");
        selectedMethod = methods.find((m) => m.id === id) || null;
        methodsEl.querySelectorAll(".retrait-method").forEach((b) => {
          b.classList.remove("border-[#ffb26e]", "bg-[#F57C00]/20");
        });
        btn.classList.add("border-[#ffb26e]", "bg-[#F57C00]/20");
      });
    });
  };

  const open = async () => {
    const user = auth.currentUser;
    if (!user) {
      if (errorEl) errorEl.textContent = "Utilisateur non connecté.";
      return;
    }

    selectedMethod = null;
    methods = [];
    if (amountInput) amountInput.value = "";
    if (firstNameInput) firstNameInput.value = "";
    if (lastNameInput) lastNameInput.value = "";
    if (phoneInput) phoneInput.value = "";
    if (methodLabelEl) methodLabelEl.textContent = "";
    if (errorEl) errorEl.textContent = "";
    activeRequestId = "";
    setSubmitting(false);
    setStep(1);
    overlay.classList.remove("hidden");
    overlay.classList.add("flex");
    void refreshAvailability();

    try {
      methods = await loadActiveMethods();
      renderMethods();
    } catch (err) {
      console.error("Erreur chargement méthodes retrait:", err);
      if (errorEl) errorEl.textContent = "Impossible de charger les méthodes.";
    }
  };

  if (nextBtn) {
    nextBtn.addEventListener("click", async () => {
      if (errorEl) errorEl.textContent = "";

      if (step === 1) {
        if (!selectedMethod) {
          if (errorEl) errorEl.textContent = "Choisissez une méthode de retrait.";
          return;
        }
        if (methodLabelEl) {
          methodLabelEl.textContent = `Méthode: ${selectedMethod.name || selectedMethod.id}`;
        }
        setStep(2);
        return;
      }

      const user = auth.currentUser;
      if (!user) {
        if (errorEl) errorEl.textContent = "Utilisateur non connecté.";
        return;
      }

      const amount = safeInt(amountInput?.value || 0);
      const firstName = String(firstNameInput?.value || "").trim();
      const lastName = String(lastNameInput?.value || "").trim();
      const phone = String(phoneInput?.value || "").trim();

      if (amount < MIN_WITHDRAWAL_HTG) {
        if (errorEl) errorEl.textContent = `Le montant minimum est ${MIN_WITHDRAWAL_HTG} HTG.`;
        return;
      }
      if (!firstName || !lastName || !phone) {
        if (errorEl) errorEl.textContent = "Nom, prénom et téléphone sont requis.";
        return;
      }
      if (isSubmitting) {
        return;
      }
      activeRequestId = activeRequestId || createClientRequestId("withdrawal");
      setSubmitting(true);

      try {
        const availability = await refreshAvailability();
        const available = safeInt(availability?.withdrawableHtg || 0);
        const ruleStatus = availability?.ruleStatus || await getWithdrawalRuleStatus(user.uid);
        if (BALANCE_DEBUG) {
          console.log("[BALANCE_DEBUG][RETRAIT] submit attempt", {
            uid: user.uid,
            amount,
            availableAtSubmit: available,
            baseForUi: availability?.baseForUi ?? getBalanceBaseForUi(),
            localAvailableHtg: availability?.localAvailableHtg ?? 0,
            __userBaseBalance: window.__userBaseBalance,
            __userBalance: window.__userBalance,
            exchangedGourdes: Number(availability?.xState?.exchangedGourdes || 0),
            selectedMethod: selectedMethod?.id || null,
            ruleStatus,
          });
        }

        if (ruleStatus.accountFrozen) {
          showRetraitRuleModal({
            title: "Compte gelé",
            message: "Ton compte a été temporairement gelé après plusieurs dépôts refusés.",
            lines: ["Contacte l'assistance pour demander un dégel."],
          });
          if (errorEl) errorEl.textContent = "Compte gelé. Contacte l'assistance.";
          setSubmitting(false);
          return;
        }

        if (ruleStatus.withdrawalHold) {
          showRetraitRuleModal({
            title: "Compte gelé",
            message: "Ton compte est gelé pour les retraits après 3 demandes rejetées.",
            lines: [
              `Rejets enregistrés: ${safeInt(ruleStatus.rejectedDepositStrikeCount)}/3`,
              "Tu peux contacter l'assistance si tu penses que c'est une erreur ou si tu veux plaider ta cause.",
              `WhatsApp assistance: ${getWhatsappContactLabel("withdrawal_assistance", ASSISTANCE_PHONE) || `+${ASSISTANCE_PHONE}`}`,
            ],
          });
          if (errorEl) errorEl.textContent = "Compte gelé pour les retraits. Contacte l'assistance.";
          setSubmitting(false);
          return;
        }

        if (amount > available) {
          if (errorEl) errorEl.textContent = "Montant supérieur au solde disponible.";
          setSubmitting(false);
          return;
        }

        if (!ruleStatus.canWithdraw) {
          if (hasPendingExamWithdrawalLock(ruleStatus)) {
            showRetraitRuleModal({
              title: "Retrait en attente",
              message: "Une partie de ton solde est encore en cours d'examen. Elle reste jouable, mais elle n'est pas retirable pour le moment.",
              lines: [
                `En examen: ${formatAmount(ruleStatus.provisionalHtgAvailable)}`,
                `Retirable maintenant: ${formatAmount(available)}`,
                "Attends la validation du dépôt pour débloquer cette partie du solde.",
              ],
            });
            if (errorEl) {
              errorEl.textContent = "Une partie du solde est encore en examen et reste bloquée pour le retrait.";
            }
            setSubmitting(false);
            return;
          }
          showRetraitRuleModal({
            title: "Retrait bloqué",
            message: "Tu dois d'abord convertir la totalité de tes dépôts en Does avant un retrait.",
            lines: [
              `Dépôts approuvés: ${ruleStatus.approvedDepositsHtg} HTG`,
              `Déjà converti en Does: ${ruleStatus.convertedHtg} HTG`,
              `Reste à convertir: ${ruleStatus.remainingToExchangeHtg} HTG`,
              "Ouvre Xchange et convertis le montant restant (HTG vers Does).",
            ],
          });
          if (errorEl) {
            errorEl.textContent = "Retrait bloqué: convertis d'abord tout ton dépôt en Does.";
          }
          setSubmitting(false);
          return;
        }
      } catch (ruleErr) {
        console.error("Erreur validation règles retrait:", ruleErr);
        if (errorEl) errorEl.textContent = "Impossible de vérifier les règles de retrait.";
        setSubmitting(false);
        return;
      }

      try {
        const response = await createWithdrawalSecure({
          requestedAmount: amount,
          destinationType: selectedMethod?.id || "",
          destinationValue: phone,
          methodId: selectedMethod?.id || "",
          customerName: `${firstName} ${lastName}`.trim(),
          customerPhone: phone,
          requestId: activeRequestId,
        });
        const createdAt = new Date().toISOString();

        window.dispatchEvent(new CustomEvent("withdrawalSubmitted", {
          detail: {
            id: response?.withdrawalId || "",
            amount,
            requestedAmount: amount,
            status: response?.status || "pending",
            methodName: selectedMethod?.name || "",
            createdAt,
            type: "withdrawal",
            userHiddenByClient: false,
          },
        }));
        if (BALANCE_DEBUG) {
          console.log("[BALANCE_DEBUG][RETRAIT] submitted success", {
            withdrawalId: response?.withdrawalId || "",
            amount,
            requestedAmount: amount,
            status: response?.status || "pending",
          });
        }

        close();
        showRetraitSuccessModal({
          amount,
          withdrawalId: response?.withdrawalId || "",
          status: response?.status || "pending",
        });
      } catch (err) {
        console.error("Erreur soumission retrait:", {
          code: err?.code || "",
          message: err?.message || "",
          err,
        });
        if (err?.code === "account-frozen") {
          showRetraitRuleModal({
            title: "Compte gelé",
            message: err?.message || "Ton compte a été temporairement gelé après plusieurs dépôts refusés.",
            lines: ["Contacte l'assistance pour demander un dégel."],
          });
        } else if (err?.code === "withdrawal-hold") {
          showRetraitRuleModal({
            title: "Compte gelé",
            message: err?.message || "Ton compte est gelé pour les retraits après 3 demandes rejetées.",
            lines: [
              `Rejets enregistrés: ${safeInt(err?.rejectedDepositStrikeCount)}/3`,
              "Contacte l'assistance si tu penses que c'est une erreur ou si tu veux plaider ta cause.",
              `WhatsApp assistance: ${getWhatsappContactLabel("withdrawal_assistance", ASSISTANCE_PHONE) || `+${ASSISTANCE_PHONE}`}`,
            ],
          });
        }
        if (errorEl) errorEl.textContent = err?.message || "Impossible de soumettre la demande.";
        setSubmitting(false);
      }
    });
  }

  if (backBtn) {
    backBtn.addEventListener("click", () => {
      if (errorEl) errorEl.textContent = "";
      setStep(1);
    });
  }

  if (closeBtn) closeBtn.addEventListener("click", close);
  overlay.addEventListener("click", (ev) => {
    if (ev.target === overlay) close();
  });
  if (panel) panel.addEventListener("click", (ev) => ev.stopPropagation());
  window.addEventListener("userBalanceUpdated", () => {
    if (!overlay.classList.contains("hidden")) {
      void refreshAvailability();
    }
  });
  window.addEventListener("xchangeUpdated", () => {
    if (!overlay.classList.contains("hidden")) {
      void refreshAvailability();
    }
  });

  overlay.__openRetrait = open;
  return overlay;
}

export function mountRetraitModal(options = {}) {
  const { triggerSelector = "#profileWithdrawBtn" } = options;
  const overlay = ensureRetraitModal();
  const trigger = document.querySelector(triggerSelector);

  if (trigger && overlay.__openRetrait && !trigger.dataset.boundRetrait) {
    trigger.dataset.boundRetrait = "1";
    trigger.addEventListener("click", () => {
      overlay.__openRetrait();
    });
  }

  window.openRetraitDirectly = () => {
    if (overlay.__openRetrait) overlay.__openRetrait();
  };
}
