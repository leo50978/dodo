import "./firebase-init.js";
import {
  auth,
  formatAuthError,
  isValidEmail,
  normalizePhoneLogin,
  isValidPhoneLogin,
  isValidUsername,
  normalizeUsername,
  isOneClickAuthEmail,
  isPhoneAuthEmail,
  createOneClickAccountId,
  isValidPassword,
  loginWithEmail,
  loginWithPhone,
  loginWithUsername,
  signupWithEmail,
  signupWithPhone,
  signupWithUsername,
  sendSignupVerificationEmail,
  refreshCurrentUser,
  syncCurrentUserDisplayName,
  isEmailPasswordUser,
  logoutCurrentUser,
  watchAuthState,
} from "./auth.js";
import { ensureAnimeRuntime } from "./anime-loader.js";
import {
  withButtonLoading,
  showGlobalLoading,
  hideGlobalLoading,
} from "./loading-ui.js";
import {
  getReferralContextFromUrl,
  normalizeCode,
} from "./referral.js";
import { updateClientProfileSecure } from "./secure-functions.js";
import { buildSupportWhatsAppUrl } from "./support-contact.js";

const authEntryParams = new URLSearchParams(window.location.search || "");
const authEntryMode = String(authEntryParams.get("mode") || "").trim().toLowerCase();
const authEntryIdentifier = String(
  authEntryParams.get("identifier")
  || authEntryParams.get("email")
  || authEntryParams.get("phone")
  || authEntryParams.get("user")
  || ""
).trim();
let authMode = authEntryMode === "signin" || authEntryMode === "reset" ? "signin" : "signup";
let signupCreationMode = "chooser";
let oneClickSignupStep = 0;
let authFlowBusy = false;
let redirectingToApp = false;
let referralBootstrapPromise = null;
let authBootstrapReady = false;
let authStateResolved = false;
let latestObservedUser = undefined;
let authFallbackRenderTimer = null;
let authBootstrapMessage = "";
let authBootstrapTone = "info";
let authEntryResetAssistPending = authEntryMode === "reset";
let authEntryResetAssistOpened = false;
const PENDING_PROMO_STORAGE_KEY = "domino_pending_promo_code";
const PENDING_USERNAME_STORAGE_KEY = "domino_pending_username";
const PENDING_PHONE_STORAGE_KEY = "domino_pending_phone_v1";
const PENDING_ONECLICK_ID_STORAGE_KEY = "domino_pending_oneclick_id";
const CLIENT_DEVICE_STORAGE_KEY = "domino_device_id_v1";
const DEVICE_ACCOUNT_LOCK_STORAGE_KEY = "domino_device_account_lock_v1";
const AUTH_SUCCESS_NOTICE_STORAGE_KEY = "domino_auth_success_notice_v1";
const USER_IMPORTANCE_NOTICE_STORAGE_KEY = "domino_user_importance_notice_v1";
const AUTH_PROFILE_HINT_STORAGE_KEY = "domino_auth_profile_hint_v1";
const verificationEmailSentByUid = new Set();
const APP_HOME_ROUTE = "./index.html";
const TERMS_ROUTE = "./conditions-utilisation.html";
const PRIVACY_ROUTE = "./politique-confidentialite.html";
const LEGAL_ROUTE = "./mentions-legales.html";
let page2ModulePromise = null;
const PAGE1_DEBUG_VERSION = "page1-v4";

console.info("[DLK_BOOTSTRAP][PAGE1] module:load", {
  version: PAGE1_DEBUG_VERSION,
  href: String(window.location?.href || ""),
  buildHint: String(document.currentScript?.src || ""),
});

window.addEventListener("error", (event) => {
  console.error("[DLK_BOOTSTRAP][PAGE1] window:error", {
    message: String(event?.message || ""),
    filename: String(event?.filename || ""),
    lineno: Number(event?.lineno || 0),
    colno: Number(event?.colno || 0),
    version: PAGE1_DEBUG_VERSION,
  });
});

window.addEventListener("unhandledrejection", (event) => {
  console.error("[DLK_BOOTSTRAP][PAGE1] window:unhandledrejection", {
    reason: event?.reason || null,
    version: PAGE1_DEBUG_VERSION,
  });
});

function pageAuthDebug(event, data = {}) {
  try {
    const payload = {
      ts: new Date().toISOString(),
      href: String(window.location?.href || ""),
      authMode,
      authFlowBusy,
      redirectingToApp,
      authBootstrapReady,
      authStateResolved,
      latestObservedUser: latestObservedUser === undefined ? "undefined" : (latestObservedUser ? String(latestObservedUser?.uid || "user") : "null"),
      ...data,
    };
    console.log(`[AUTH_DEBUG][PAGE1] ${event}`, payload);
  } catch (error) {
    console.log(`[AUTH_DEBUG][PAGE1] ${event}`, { ts: new Date().toISOString(), logError: String(error?.message || error) });
  }
}

function getAuthShell() {
  return document.getElementById("domino-app-shell") || document.body;
}

function updateAuthModalBodyLock() {
  const modalIds = [
    "emailVerificationOverlay",
    "forgotPasswordAssistOverlay",
  ];
  const shouldLock = modalIds.some((id) => {
    const node = document.getElementById(id);
    return Boolean(node) && !node.classList.contains("hidden");
  });
  document.documentElement.classList.toggle("overflow-hidden", shouldLock);
  document.body.classList.toggle("overflow-hidden", shouldLock);
  document.documentElement.style.overflow = shouldLock ? "hidden" : "";
  document.body.style.overflow = shouldLock ? "hidden" : "";
}

function buildForgotPasswordSupportMessage(identifier = "") {
  const cleanIdentifier = String(identifier || "").trim();
  const base = "Bonjour assistance, j'ai oublie le mot de passe de mon compte Dominoes Lakay et j'ai besoin d'aide pour le recuperer.";
  if (!cleanIdentifier) return base;
  return `${base} Mon identifiant de connexion est: ${cleanIdentifier}`;
}

function ensureForgotPasswordAssistModal() {
  let overlay = document.getElementById("forgotPasswordAssistOverlay");
  if (overlay) return overlay;

  overlay = document.createElement("div");
  overlay.id = "forgotPasswordAssistOverlay";
  overlay.className = "fixed inset-0 z-[3600] hidden items-center justify-center bg-[#050814]/72 px-4 py-6 backdrop-blur-md";
  overlay.innerHTML = `
    <div class="w-full max-w-lg rounded-[32px] border border-white/12 bg-[linear-gradient(180deg,rgba(72,81,116,0.96),rgba(48,56,84,0.98))] p-5 text-white shadow-[18px_18px_42px_rgba(16,23,40,0.44),-10px_-10px_22px_rgba(114,128,169,0.14)] sm:p-6">
      <div class="flex items-start justify-between gap-4">
        <div>
          <p class="text-[11px] font-semibold uppercase tracking-[0.2em] text-white/65">Recuperation compte</p>
          <h2 class="mt-2 text-2xl font-bold leading-tight text-white">Mot de passe oublie ?</h2>
        </div>
        <button
          id="forgotPasswordAssistCloseBtn"
          type="button"
          class="grid h-10 w-10 shrink-0 place-items-center rounded-2xl border border-white/15 bg-white/10 text-white/80 transition hover:bg-white/15"
          aria-label="Fermer"
        >
          <i class="fa-solid fa-xmark"></i>
        </button>
      </div>

      <div class="mt-4 rounded-[26px] border border-white/10 bg-white/[0.06] p-4 text-sm leading-7 text-white/88">
        Si ou pedi modpas ou, kontakte yon ajan asistans. Ajan an ap poze w kestyon pou verifye idantite w, epi l ap ede w reprann aksè ak kont ou.
      </div>

      <div class="mt-4 rounded-[26px] border border-[#ffcf9e]/18 bg-[#f48f45]/10 p-4">
        <p class="text-xs font-semibold uppercase tracking-[0.16em] text-[#ffd8b5]">Sa k ap pase apre</p>
        <ol class="mt-3 space-y-2 text-sm leading-6 text-white/82">
          <li>1. Ekri ajan an sou WhatsApp.</li>
          <li>2. Reponn kestyon verifikasyon yo.</li>
          <li>3. Ajan an ap ede w mete yon nouvo modpas tanporè.</li>
          <li>4. Apre sa, ou ka antre nan pwofil ou pou chanje l ankò.</li>
        </ol>
      </div>

      <p id="forgotPasswordAssistIdentifier" class="mt-4 text-xs leading-5 text-white/65"></p>

      <div class="mt-5 grid gap-3 sm:grid-cols-2">
        <button
          id="forgotPasswordAssistWhatsappBtn"
          type="button"
          class="rounded-2xl bg-[#25D366] px-4 py-3 text-sm font-semibold text-[#042814] shadow-[10px_10px_20px_rgba(7,64,33,0.28),-6px_-6px_16px_rgba(96,232,152,0.12)] transition hover:-translate-y-0.5"
        >
          Contacter un agent
        </button>
        <button
          id="forgotPasswordAssistCancelBtn"
          type="button"
          class="rounded-2xl border border-white/14 bg-white/8 px-4 py-3 text-sm font-semibold text-white/90 transition hover:bg-white/12"
        >
          Fermer
        </button>
      </div>
    </div>
  `;

  const close = () => {
    overlay.classList.add("hidden");
    overlay.classList.remove("flex");
    updateAuthModalBodyLock();
  };

  overlay.querySelector("#forgotPasswordAssistCloseBtn")?.addEventListener("click", close);
  overlay.querySelector("#forgotPasswordAssistCancelBtn")?.addEventListener("click", close);
  overlay.addEventListener("click", (event) => {
    if (event.target === overlay) {
      close();
    }
  });
  overlay.querySelector("#forgotPasswordAssistWhatsappBtn")?.addEventListener("click", () => {
    const identifier = String(overlay.dataset.identifier || "").trim();
    window.open(buildSupportWhatsAppUrl(buildForgotPasswordSupportMessage(identifier)), "_blank", "noopener,noreferrer");
  });

  document.body.appendChild(overlay);
  return overlay;
}

function openForgotPasswordAssistModal(identifier = "") {
  const overlay = ensureForgotPasswordAssistModal();
  const cleanIdentifier = String(identifier || "").trim();
  const identifierEl = overlay.querySelector("#forgotPasswordAssistIdentifier");
  overlay.dataset.identifier = cleanIdentifier;
  if (identifierEl) {
    identifierEl.textContent = cleanIdentifier
      ? `Identifiant rempli actuellement: ${cleanIdentifier}`
      : "Si ou sonje nimewo, username oswa imel la, voye l bay ajan an pou ede l jwenn kont ou pi vit.";
  }
  overlay.classList.remove("hidden");
  overlay.classList.add("flex");
  updateAuthModalBodyLock();
}

async function ensurePage2Module() {
  if (!page2ModulePromise) {
    console.info("[DLK_BOOTSTRAP][PAGE1] page2:import:start", {
      version: PAGE1_DEBUG_VERSION,
      url: "./page2.js?v=page2-hero-v5",
    });
    page2ModulePromise = import("./page2.js?v=page2-hero-v5");
  }
  return page2ModulePromise;
}

function escapeAttr(text) {
  return String(text || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function savePendingPromoCode(code) {
  const normalized = normalizeCode(code || "");
  if (!normalized) {
    sessionStorage.removeItem(PENDING_PROMO_STORAGE_KEY);
    return;
  }
  sessionStorage.setItem(PENDING_PROMO_STORAGE_KEY, normalized);
}

function consumePendingPromoCode() {
  const raw = sessionStorage.getItem(PENDING_PROMO_STORAGE_KEY) || "";
  sessionStorage.removeItem(PENDING_PROMO_STORAGE_KEY);
  return normalizeCode(raw);
}

function savePendingUsername(username) {
  const normalized = normalizeUsername(username || "");
  if (!normalized) {
    sessionStorage.removeItem(PENDING_USERNAME_STORAGE_KEY);
    return;
  }
  sessionStorage.setItem(PENDING_USERNAME_STORAGE_KEY, normalized);
}

function consumePendingUsername() {
  const raw = sessionStorage.getItem(PENDING_USERNAME_STORAGE_KEY) || "";
  sessionStorage.removeItem(PENDING_USERNAME_STORAGE_KEY);
  return normalizeUsername(raw);
}

function savePendingPhone(phone) {
  const normalized = normalizePhoneLogin(phone || "");
  if (!normalized) {
    sessionStorage.removeItem(PENDING_PHONE_STORAGE_KEY);
    return;
  }
  sessionStorage.setItem(PENDING_PHONE_STORAGE_KEY, normalized);
}

function consumePendingPhone() {
  const raw = sessionStorage.getItem(PENDING_PHONE_STORAGE_KEY) || "";
  sessionStorage.removeItem(PENDING_PHONE_STORAGE_KEY);
  return normalizePhoneLogin(raw);
}

function savePendingOneClickId(oneClickId) {
  const clean = String(oneClickId || "").trim().toUpperCase().slice(0, 64);
  if (!clean) {
    sessionStorage.removeItem(PENDING_ONECLICK_ID_STORAGE_KEY);
    return;
  }
  sessionStorage.setItem(PENDING_ONECLICK_ID_STORAGE_KEY, clean);
}

function consumePendingOneClickId() {
  const raw = sessionStorage.getItem(PENDING_ONECLICK_ID_STORAGE_KEY) || "";
  sessionStorage.removeItem(PENDING_ONECLICK_ID_STORAGE_KEY);
  return String(raw || "").trim().toUpperCase().slice(0, 64);
}

function randomToken(size = 10) {
  const chars = "abcdefghjkmnpqrstuvwxyz23456789";
  let out = "";
  for (let i = 0; i < size; i += 1) {
    out += chars[Math.floor(Math.random() * chars.length)];
  }
  return out;
}

function countLetters(text) {
  return (String(text || "").match(/[a-z]/gi) || []).length;
}

function countDigits(text) {
  return (String(text || "").match(/\d/g) || []).length;
}

function isValidOneClickUsername(username) {
  const normalized = normalizeUsername(username || "");
  return isValidUsername(normalized) && countLetters(normalized) >= 4 && countDigits(normalized) >= 1;
}

function isValidOneClickPassword(password) {
  const raw = String(password || "");
  return isValidPassword(raw) && countLetters(raw) >= 1 && countDigits(raw) >= 1;
}

function getOrCreateDeviceId() {
  try {
    const existing = window.localStorage?.getItem(CLIENT_DEVICE_STORAGE_KEY) || "";
    if (existing) return existing;
    const created = `web_${Date.now().toString(36)}_${randomToken(8)}`;
    window.localStorage?.setItem(CLIENT_DEVICE_STORAGE_KEY, created);
    return created;
  } catch (_) {
    return `web_${Date.now().toString(36)}_${randomToken(8)}`;
  }
}

function readDeviceAccountLock() {
  try {
    const raw = window.localStorage?.getItem(DEVICE_ACCOUNT_LOCK_STORAGE_KEY) || "";
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") return null;
    const uid = String(parsed.uid || "").trim();
    if (!uid) return null;
    return {
      uid,
      email: String(parsed.email || "").trim(),
      source: String(parsed.source || "").trim(),
      deviceId: String(parsed.deviceId || "").trim(),
      createdAtMs: Number(parsed.createdAtMs || 0) || 0,
      lastSeenAtMs: Number(parsed.lastSeenAtMs || 0) || 0,
    };
  } catch (_) {
    return null;
  }
}

function writeDeviceAccountLock(payload = {}) {
  try {
    window.localStorage?.setItem(DEVICE_ACCOUNT_LOCK_STORAGE_KEY, JSON.stringify(payload));
  } catch (_) {}
}

function createDeviceAccountLockError() {
  const error = new Error("Un compte existe déjà sur cet appareil. Connecte-toi avec ce compte ou contacte l'assistance au 50940507232.");
  error.code = "auth/device-account-exists";
  return error;
}

function assertSignupAllowedOnThisDevice() {
  const existingLock = readDeviceAccountLock();
  const currentUid = String(auth.currentUser?.uid || "").trim();
  if (!existingLock?.uid) return;
  if (currentUid && existingLock.uid === currentUid) return;
  pageAuthDebug("deviceAccountLock:blockedSignup", {
    lockUid: existingLock.uid,
    currentUid,
    deviceId: existingLock.deviceId,
    source: existingLock.source,
  });
  throw createDeviceAccountLockError();
}

function rememberDeviceAccountOwner(user, source = "auth") {
  const uid = String(user?.uid || "").trim();
  if (!uid) return;

  const existingLock = readDeviceAccountLock();
  if (existingLock?.uid && existingLock.uid !== uid) {
    pageAuthDebug("deviceAccountLock:preserveExisting", {
      existingUid: existingLock.uid,
      currentUid: uid,
      source,
    });
    return;
  }

  const now = Date.now();
  const nextLock = {
    uid,
    email: String(user?.email || existingLock?.email || "").trim(),
    source: String(source || existingLock?.source || "auth").trim(),
    deviceId: getOrCreateDeviceId(),
    createdAtMs: existingLock?.createdAtMs || now,
    lastSeenAtMs: now,
  };
  writeDeviceAccountLock(nextLock);
  pageAuthDebug("deviceAccountLock:remembered", {
    uid,
    source: nextLock.source,
    deviceId: nextLock.deviceId,
  });
}

function detectBrowserName() {
  const ua = String(window.navigator?.userAgent || "");
  if (ua.includes("Edg/")) return "Edge";
  if (ua.includes("OPR/") || ua.includes("Opera")) return "Opera";
  if (ua.includes("Firefox/")) return "Firefox";
  if (ua.includes("Chrome/")) return "Chrome";
  if (ua.includes("Safari/")) return "Safari";
  return "Unknown";
}

function inferCountryCode() {
  const locale = String(
    window.navigator?.language ||
    Intl.DateTimeFormat().resolvedOptions().locale ||
    ""
  ).trim();
  const parts = locale.split(/[-_]/).filter(Boolean);
  if (parts.length > 1) {
    return String(parts[parts.length - 1] || "").toUpperCase();
  }
  return "";
}

function collectAnalyticsContext() {
  const params = new URLSearchParams(window.location.search || "");
  return {
    deviceId: getOrCreateDeviceId(),
    appVersion: "web-public",
    country: inferCountryCode(),
    browser: detectBrowserName(),
    landingPage: String(window.location.pathname || "/"),
    utmSource: String(params.get("utm_source") || ""),
    utmCampaign: String(params.get("utm_campaign") || ""),
    creativeId: String(params.get("creative_id") || params.get("creativeId") || ""),
  };
}

function clearAuthFallbackRenderTimer() {
  if (authFallbackRenderTimer) {
    window.clearTimeout(authFallbackRenderTimer);
    authFallbackRenderTimer = null;
  }
}

function setAuthBootstrapMessage(message = "", tone = "info") {
  authBootstrapMessage = String(message || "").trim();
  authBootstrapTone = tone || "info";
  pageAuthDebug("setBootstrapMessage", {
    tone: authBootstrapTone,
    message: authBootstrapMessage,
  });
}

function storeAuthSuccessNotice() {
  try {
    sessionStorage.setItem(
      AUTH_SUCCESS_NOTICE_STORAGE_KEY,
      JSON.stringify({ ts: Date.now(), type: "auth_success" })
    );
  } catch (_) {}
}

function storeUserImportanceNotice() {
  try {
    sessionStorage.setItem(
      USER_IMPORTANCE_NOTICE_STORAGE_KEY,
      JSON.stringify({ ts: Date.now(), type: "signup_success" })
    );
  } catch (_) {}
}

function saveAuthProfileHint(user, payload = {}) {
  const uid = String(user?.uid || auth.currentUser?.uid || "").trim();
  if (!uid) return;
  const username = normalizeUsername(payload.username || "");
  const phone = normalizePhoneLogin(payload.phone || "");
  try {
    window.localStorage?.setItem(
      AUTH_PROFILE_HINT_STORAGE_KEY,
      JSON.stringify({
        uid,
        username,
        phone,
        updatedAtMs: Date.now(),
      })
    );
    pageAuthDebug("saveAuthProfileHint", { uid, username, phone });
  } catch (_) {}
}

function scheduleAuthFallbackRender(delayMs = 1200) {
  pageAuthDebug("scheduleAuthFallbackRender", { delayMs });
  clearAuthFallbackRenderTimer();
  authFallbackRenderTimer = window.setTimeout(() => {
    authFallbackRenderTimer = null;
    pageAuthDebug("scheduleAuthFallbackRender:tick");
    if (redirectingToApp) return;
    if (auth.currentUser) return;
    if (latestObservedUser !== null) return;
    if (authBootstrapReady !== true || authStateResolved !== true) return;
    pageAuthDebug("scheduleAuthFallbackRender:renderPage1");
    renderPage1();
  }, Math.max(250, Number(delayMs) || 1200));
}

function userRequiresEmailVerification(user) {
  if (!user || !isEmailPasswordUser(user)) return false;
  const email = String(user?.email || "").trim().toLowerCase();
  if (isOneClickAuthEmail(email)) return false;
  if (isPhoneAuthEmail(email)) return false;
  return user.emailVerified !== true;
}

function setVerificationStatus(message, tone = "info") {
  const statusEl = document.getElementById("emailVerifyStatus");
  if (!statusEl) return;

  const toneClassMap = {
    info: "border-white/20 bg-white/10 text-white/90",
    success: "border-emerald-300/40 bg-emerald-500/15 text-emerald-100",
    warning: "border-amber-300/40 bg-amber-500/15 text-amber-100",
    error: "border-red-300/40 bg-red-500/15 text-red-100",
  };
  const toneClass = toneClassMap[tone] || toneClassMap.info;

  statusEl.className = `mt-4 rounded-2xl border px-4 py-3 text-xs sm:text-sm ${toneClass}`;
  statusEl.textContent = message || "";
}

function closeEmailVerificationModal() {
  const overlay = document.getElementById("emailVerificationOverlay");
  if (!overlay) return;
  overlay.classList.add("hidden");
  overlay.classList.remove("flex");
  updateAuthModalBodyLock();
}

function syncOneClickStepUi(rootNode, requestedStep = 0) {
  if (!rootNode) return 0;
  const cardId = String(rootNode.dataset.oneclickCardId || "").trim();
  const stepLabelId = String(rootNode.dataset.oneclickStepLabelId || "").trim();
  const prevBtnId = String(rootNode.dataset.oneclickPrevBtnId || "").trim();
  const nextBtnId = String(rootNode.dataset.oneclickNextBtnId || "").trim();
  const submitBtnId = String(rootNode.dataset.oneclickSubmitBtnId || "").trim();
  const cardNode = cardId ? document.getElementById(cardId) : null;
  const steps = Array.from(rootNode.querySelectorAll("[data-oneclick-step]"));
  const totalSteps = steps.length;
  if (totalSteps === 0) return 0;
  const safeStep = Math.max(0, Math.min(Number(requestedStep) || 0, totalSteps - 1));
  rootNode.dataset.step = String(safeStep);
  oneClickSignupStep = safeStep;
  if (cardNode) {
    const isMobile = window.matchMedia("(max-width: 640px)").matches;
    const mobileMinHeights = ["24rem", "25rem", "25rem", "24.5rem", "27rem"];
    const desktopMinHeights = ["23rem", "24rem", "24rem", "23.5rem", "25rem"];
    const targetHeight = (isMobile ? mobileMinHeights : desktopMinHeights)[safeStep] || (isMobile ? "24.5rem" : "23.5rem");
    cardNode.style.minHeight = targetHeight;
  }

  steps.forEach((stepNode, index) => {
    const active = index === safeStep;
    stepNode.classList.toggle("hidden", !active);
    stepNode.setAttribute("aria-hidden", active ? "false" : "true");
  });

  rootNode.querySelectorAll("[data-oneclick-dot]").forEach((dot, index) => {
    const active = index === safeStep;
    dot.classList.toggle("bg-[#f48f45]", active);
    dot.classList.toggle("border-[#f7c08d]", active);
    dot.classList.toggle("bg-white/12", !active);
    dot.classList.toggle("border-white/10", !active);
  });

  const label = stepLabelId ? document.getElementById(stepLabelId) : null;
  if (label) label.textContent = `Étape ${safeStep + 1} sur ${totalSteps}`;

  const prevBtn = prevBtnId ? document.getElementById(prevBtnId) : null;
  if (prevBtn) {
    prevBtn.classList.toggle("hidden", safeStep === 0);
  }

  const nextBtn = nextBtnId ? document.getElementById(nextBtnId) : null;
  if (nextBtn) {
    nextBtn.classList.toggle("hidden", safeStep === totalSteps - 1);
  }

  const submitBtn = submitBtnId ? document.getElementById(submitBtnId) : null;
  if (submitBtn) {
    submitBtn.classList.toggle("hidden", safeStep !== totalSteps - 1);
  }

  return safeStep;
}

function ensureEmailVerificationModal() {
  let overlay = document.getElementById("emailVerificationOverlay");
  if (!overlay) {
    overlay = document.createElement("div");
    overlay.id = "emailVerificationOverlay";
    overlay.className = "fixed inset-0 z-[5200] hidden items-center justify-center bg-black/60 p-4 backdrop-blur-md";
    overlay.innerHTML = `
      <div class="w-[min(94vw,34rem)] rounded-3xl border border-white/20 bg-[#3F4766]/85 p-5 text-white shadow-[14px_14px_34px_rgba(16,23,40,0.5),-10px_-10px_24px_rgba(112,126,165,0.2)] backdrop-blur-xl sm:p-6">
        <h2 class="text-xl font-bold tracking-wide sm:text-2xl">Vérifie ton email</h2>
        <p class="mt-2 text-sm text-white/85">
          Un email de vérification a été envoyé à <span id="emailVerifyTarget" class="font-semibold text-[#ffd8b5]">ton adresse</span>.
        </p>
        <p class="mt-1 text-xs text-amber-200/95">
          Important: regarde aussi dans les dossiers <span class="font-semibold">Spam</span> ou <span class="font-semibold">Courrier indésirable</span> si tu ne le vois pas.
        </p>
        <div class="mt-4 rounded-2xl border border-white/20 bg-white/10 p-4 text-xs text-white/80 sm:text-sm">
          Ouvre l'email reçu, clique sur le lien de confirmation, puis reviens ici. Si le message tarde, attends quelques secondes avant de recliquer.
        </div>
        <div id="emailVerifyStatus" class="mt-4 rounded-2xl border border-white/20 bg-white/10 px-4 py-3 text-xs text-white/90 sm:text-sm">
          Vérification en attente.
        </div>
        <div class="mt-4">
          <button id="emailVerifyRefreshBtn" type="button" class="h-11 w-full rounded-2xl border border-[#ffb26e] bg-[#F57C00] text-sm font-semibold text-white shadow-[8px_8px_18px_rgba(163,82,27,0.45),-6px_-6px_14px_rgba(255,175,102,0.22)] transition hover:-translate-y-0.5">
            J'ai vérifié mon email
          </button>
        </div>
        <button id="emailVerifyWrongEmailBtn" type="button" class="mt-3 w-full rounded-2xl border border-white/15 bg-white/6 px-4 py-3 text-sm font-semibold text-white/88 transition hover:bg-white/10">
          Ce n'etait pas mon email
        </button>
      </div>
    `;
    document.body.appendChild(overlay);
  }

  const refreshBtn = document.getElementById("emailVerifyRefreshBtn");
  const wrongEmailBtn = document.getElementById("emailVerifyWrongEmailBtn");

  if (refreshBtn && !refreshBtn.dataset.bound) {
    refreshBtn.dataset.bound = "1";
    refreshBtn.addEventListener("click", async () => {
      try {
        await withButtonLoading(refreshBtn, async () => {
          const verified = await checkEmailVerificationAndContinue("", { showWaitingMessage: true });
          if (!verified) {
            setVerificationStatus("Email pas encore vérifié. Ouvre le mail reçu, clique le lien puis recommence.", "warning");
          }
        }, { loadingLabel: "Vérification..." });
      } catch (err) {
        setVerificationStatus(formatAuthError(err, "Impossible de vérifier pour le moment."), "error");
      }
    });
  }

  if (wrongEmailBtn && !wrongEmailBtn.dataset.bound) {
    wrongEmailBtn.dataset.bound = "1";
    wrongEmailBtn.addEventListener("click", async () => {
      try {
        await withButtonLoading(wrongEmailBtn, async () => {
          authMode = "signup";
          redirectingToApp = false;
          savePendingPromoCode("");
          await logoutCurrentUser();
          closeEmailVerificationModal();
          renderPage1();
        }, { loadingLabel: "Retour..." });
      } catch (err) {
        setVerificationStatus(formatAuthError(err, "Impossible de revenir au formulaire pour le moment."), "error");
      }
    });
  }

  return overlay;
}

async function sendVerificationEmailIfNeeded(user) {
  if (!user || !user.uid || verificationEmailSentByUid.has(user.uid)) return;
  try {
    await sendSignupVerificationEmail(user);
    verificationEmailSentByUid.add(user.uid);
    setVerificationStatus("Email envoyé. Vérifie ta boîte de réception et le dossier Spam/Indésirable.", "success");
  } catch (err) {
    setVerificationStatus(formatAuthError(err, "Impossible d'envoyer l'email de vérification."), "error");
  }
}

async function showEmailVerificationModal(user) {
  pageAuthDebug("showEmailVerificationModal", {
    uid: String(user?.uid || ""),
    email: String(user?.email || ""),
  });
  hideGlobalLoading();
  const overlay = ensureEmailVerificationModal();
  const emailTarget = document.getElementById("emailVerifyTarget");
  if (emailTarget) emailTarget.textContent = user?.email || "ton adresse email";
  overlay.classList.remove("hidden");
  overlay.classList.add("flex");
  updateAuthModalBodyLock();

  setVerificationStatus("Confirme ton email puis reviens ici et clique sur le bouton ci-dessous.", "info");
  await sendVerificationEmailIfNeeded(user);
}

async function checkEmailVerificationAndContinue(explicitPromoCode = "", options = {}) {
  pageAuthDebug("checkEmailVerificationAndContinue:start", {
    explicitPromoCode: String(explicitPromoCode || ""),
    showWaitingMessage: options?.showWaitingMessage === true,
  });
  const refreshedUser = await refreshCurrentUser(auth.currentUser);
  if (!refreshedUser) {
    pageAuthDebug("checkEmailVerificationAndContinue:noRefreshedUser");
    closeEmailVerificationModal();
    return false;
  }
  if (userRequiresEmailVerification(refreshedUser)) {
    pageAuthDebug("checkEmailVerificationAndContinue:stillUnverified", {
      uid: String(refreshedUser?.uid || ""),
      emailVerified: refreshedUser?.emailVerified === true,
    });
    if (options.showWaitingMessage) {
      setVerificationStatus("Toujours en attente de vérification. Ouvre le mail reçu et clique le lien de confirmation.", "warning");
    }
    return false;
  }
  pageAuthDebug("checkEmailVerificationAndContinue:verified", {
    uid: String(refreshedUser?.uid || ""),
  });
  closeEmailVerificationModal();
  await bootstrapReferralBeforeRedirect(refreshedUser, explicitPromoCode);
  redirectToHomeApp(refreshedUser);
  return true;
}

function redirectToHomeApp(user) {
  pageAuthDebug("redirectToHomeApp:enter", {
    uid: String(user?.uid || auth.currentUser?.uid || ""),
  });
  if (redirectingToApp) return;
  redirectingToApp = true;
  showGlobalLoading("Connexion réussie. Chargement de l'accueil...");
  storeAuthSuccessNotice();
  setAuthBootstrapMessage("Connexion réussie. Redirection vers l'accueil...", "success");
  const currentPath = String(window.location.pathname || "");
  const onHomePage =
    currentPath.endsWith("/inedex.html") ||
    currentPath.endsWith("inedex.html") ||
    currentPath.endsWith("/index.html") ||
    currentPath.endsWith("index.html") ||
    currentPath === "/" ||
    currentPath === "";
  pageAuthDebug("redirectToHomeApp:pathCheck", {
    currentPath,
    onHomePage,
    target: APP_HOME_ROUTE,
  });
  if (onHomePage) {
    pageAuthDebug("redirectToHomeApp:renderPage2Inline");
    void ensurePage2Module()
      .then(({ renderPage2 }) => {
        hideGlobalLoading();
        renderPage2(user || auth.currentUser);
      })
      .catch((error) => {
        pageAuthDebug("redirectToHomeApp:renderPage2InlineError", {
          error: String(error?.message || error),
        });
        window.location.replace(APP_HOME_ROUTE);
      });
    return;
  }
  pageAuthDebug("redirectToHomeApp:replace");
  window.location.replace(APP_HOME_ROUTE);
  window.setTimeout(() => {
    const path = String(window.location.pathname || "");
    pageAuthDebug("redirectToHomeApp:fallbackCheck", { path });
    if (path.endsWith("/auth.html") || path.endsWith("auth.html")) {
      pageAuthDebug("redirectToHomeApp:fallbackAssign");
      window.location.assign(APP_HOME_ROUTE);
    }
  }, 1200);
}

async function bootstrapReferralBeforeRedirect(user, explicitPromoCode = "") {
  if (!user) return;
  const urlCtx = getReferralContextFromUrl(window.location.search);
  const typedPromoCode = normalizeCode(explicitPromoCode || "");
  const pendingPromoCode = consumePendingPromoCode();
  const pendingUsername = consumePendingUsername();
  const pendingPhone = consumePendingPhone();
  const pendingOneClickId = consumePendingOneClickId();
  const queryPromoCode = normalizeCode(urlCtx.promoCodeFromQuery || "");
  const linkReferralCode = normalizeCode(urlCtx.userCodeFromLink || "");

  let referralPayload = {};
  if (typedPromoCode) {
    referralPayload = { promoCode: typedPromoCode, referralSource: "promo" };
  } else if (pendingPromoCode) {
    referralPayload = { promoCode: pendingPromoCode, referralSource: "promo" };
  } else if (queryPromoCode) {
    referralPayload = { promoCode: queryPromoCode, referralSource: "promo" };
  } else if (linkReferralCode) {
    referralPayload = { promoCode: linkReferralCode, referralSource: "link" };
  }

  if (!referralBootstrapPromise) {
    pageAuthDebug("bootstrapReferralBeforeRedirect:start", {
      uid: String(user?.uid || ""),
      explicitPromoCode: typedPromoCode,
      pendingPromoCode,
      pendingUsername,
      pendingPhone,
      pendingOneClickId,
      queryPromoCode,
      linkReferralCode,
      referralPayload,
    });
    referralBootstrapPromise = updateClientProfileSecure({
      ...collectAnalyticsContext(),
      ...referralPayload,
      username: pendingUsername || undefined,
      phone: pendingPhone || undefined,
      oneClickId: pendingOneClickId || undefined,
    })
      .then((result) => {
        pageAuthDebug("bootstrapReferralBeforeRedirect:result", {
          uid: String(user?.uid || ""),
          result,
        });
        if (result?.profile) {
          saveAuthProfileHint(user, {
            username: result.profile.username || pendingUsername || "",
            phone: result.profile.phone || pendingPhone || "",
          });
        }
        return result;
      })
      .catch((err) => {
        console.error("Secure profile bootstrap error:", err);
        pageAuthDebug("bootstrapReferralBeforeRedirect:error", {
          error: String(err?.message || err),
          code: String(err?.code || ""),
        });
      })
      .finally(() => {
        pageAuthDebug("bootstrapReferralBeforeRedirect:done");
        referralBootstrapPromise = null;
      });
  }

  await referralBootstrapPromise;
}

async function handleAuthenticatedUser(user, explicitPromoCode = "") {
  pageAuthDebug("handleAuthenticatedUser:start", {
    uid: String(user?.uid || ""),
    email: String(user?.email || ""),
    emailVerified: user?.emailVerified === true,
    explicitPromoCode: String(explicitPromoCode || ""),
  });
  if (!user) return;
  rememberDeviceAccountOwner(user, "auth_success");
  showGlobalLoading("Connexion en cours...");
  clearAuthFallbackRenderTimer();
  if (userRequiresEmailVerification(user)) {
    pageAuthDebug("handleAuthenticatedUser:requiresEmailVerification");
    await showEmailVerificationModal(user);
    return;
  }
  closeEmailVerificationModal();
  const hasPendingPromo = Boolean(sessionStorage.getItem(PENDING_PROMO_STORAGE_KEY));
  const hasPendingUsername = Boolean(sessionStorage.getItem(PENDING_USERNAME_STORAGE_KEY));
  const hasPendingOneClickId = Boolean(sessionStorage.getItem(PENDING_ONECLICK_ID_STORAGE_KEY));
  const shouldBlockRedirectForReferral =
    Boolean(normalizeCode(explicitPromoCode || "")) ||
    hasPendingPromo ||
    hasPendingUsername ||
    hasPendingOneClickId;

  pageAuthDebug("handleAuthenticatedUser:bootstrapMode", {
    shouldBlockRedirectForReferral,
    hasPendingPromo,
    hasPendingUsername,
    hasPendingOneClickId,
  });

  if (shouldBlockRedirectForReferral) {
    await bootstrapReferralBeforeRedirect(user, explicitPromoCode);
  } else {
    void bootstrapReferralBeforeRedirect(user, explicitPromoCode).catch((err) => {
      pageAuthDebug("handleAuthenticatedUser:bootstrapBackgroundError", {
        error: String(err?.message || err),
        code: String(err?.code || ""),
      });
    });
  }
  pageAuthDebug("handleAuthenticatedUser:redirectToHome");
  redirectToHomeApp(user);
}

function renderAuthLoading() {
  getAuthShell().innerHTML = `
    <div class="min-h-screen grid place-items-center bg-[#0b1f3f] text-white font-['Poppins']">
      <div class="rounded-3xl border border-white/15 bg-white/10 px-6 py-5 text-center shadow-[12px_12px_28px_rgba(25,30,44,0.42),-10px_-10px_24px_rgba(97,110,150,0.16)] backdrop-blur-md">
        <div class="text-base font-semibold tracking-wide">Connexion en cours...</div>
      </div>
    </div>
  `;
}

function renderPage1() {
  pageAuthDebug("renderPage1");
  hideGlobalLoading();
  const isLegacySignin = authMode === "signin";
  const isSignupChooser = !isLegacySignin && signupCreationMode === "chooser";
  const isSignupPhoneFlow = !isLegacySignin && signupCreationMode === "phone";
  const isSignupOneClickFlow = !isLegacySignin && signupCreationMode === "oneclick";
  const referralCtx = getReferralContextFromUrl(window.location.search);
  const hintCode = referralCtx.userCodeFromLink;
  const promoPrefill = normalizeCode(
    referralCtx.promoCodeFromQuery ||
    referralCtx.userCodeFromLink ||
    ""
  );
  const referralHint = authMode === "signup" && hintCode
    ? `
      <div class="mt-3 rounded-2xl border border-[#ffb26e]/45 bg-[#f57c00]/15 px-4 py-3 text-xs text-white/90 sm:text-sm">
        Code promo détecté automatiquement: <span class="font-semibold text-[#ffd8b5]">${escapeAttr(hintCode)}</span>
      </div>
    `
    : "";
  const bootstrapInfo = authBootstrapMessage
    ? `<div id="authInfo" class="mt-2 min-h-5 text-xs ${authBootstrapTone === "success" ? "text-emerald-200" : authBootstrapTone === "error" ? "text-[#ffb0b0]" : "text-amber-200"}">${escapeAttr(authBootstrapMessage)}</div>`
    : `<div id="authInfo" class="mt-2 min-h-5 text-xs text-amber-200"></div>`;
  const signupMethodChooser = isSignupChooser
    ? `
      <div class="mt-6 grid grid-cols-1 gap-3">
        <button
          id="openPhoneFieldsBtn"
          type="button"
          class="rounded-[1.7rem] border border-[#ffb26e]/55 bg-[linear-gradient(180deg,rgba(245,124,0,0.2),rgba(255,255,255,0.06))] px-5 py-4 text-left shadow-[10px_10px_22px_rgba(163,82,27,0.32),-8px_-8px_18px_rgba(255,183,116,0.1)] backdrop-blur-md"
        >
          <div class="flex items-start justify-between gap-3">
            <div class="grid h-11 w-11 place-items-center rounded-2xl border border-[#ffcf9e]/35 bg-[#f57c00]/18 text-[#ffe0bf]">
              <i class="fa-solid fa-mobile-screen-button text-base"></i>
            </div>
            <span class="rounded-full border border-[#ffcf9e]/30 bg-[#f57c00]/16 px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.22em] text-[#ffe0bf]">Actif</span>
          </div>
          <div class="mt-4 text-sm font-semibold tracking-wide text-white sm:text-base">Creer avec numero</div>
        </button>
      </div>
    `
    : "";
  const legacyToggle = isLegacySignin
    ? `
      <p class="mt-5 text-sm text-white/80 sm:text-base">
        Vous n'avez pas de compte ?
        <button id="openPhoneSignupBtn" type="button" class="font-semibold text-[#f48f45] hover:text-[#ff9f58]">Inscrivez-vous</button>
      </p>
    `
    : `
      <p class="mt-5 text-sm text-white/72 sm:text-base">
        Vous avez deja un compte ?
        <button id="openLegacySigninBtn" type="button" class="font-semibold text-[#f48f45] hover:text-[#ff9f58]">Connectez-vous</button>
      </p>
    `;
  const signupFlowTopActions = (isSignupPhoneFlow || isSignupOneClickFlow)
    ? `
      <div class="mt-6 flex items-center justify-between gap-3">
        <button
          id="backToSignupMethodsBtn"
          type="button"
          class="inline-flex items-center gap-2 rounded-full border border-white/16 bg-white/8 px-4 py-2 text-xs font-semibold tracking-wide text-white/88 transition hover:bg-white/12"
        >
          <i class="fa-solid fa-arrow-left text-[11px]"></i>
          Methodes
        </button>
        <div class="rounded-full border ${isSignupOneClickFlow ? "border-white/16 bg-white/10 text-white/80" : "border-[#ffcf9e]/30 bg-[#f57c00]/16 text-[#ffe0bf]"} px-3 py-1 text-[10px] font-semibold uppercase tracking-[0.18em]">
          ${isSignupOneClickFlow ? "En un click" : "Avec numero"}
        </div>
      </div>
    `
    : "";
  const signupOneClickPanel = isSignupOneClickFlow
    ? `
      <div
        id="oneClickAuthInline"
        data-step="${oneClickSignupStep}"
        data-oneclick-card-id="oneClickAuthCard"
        data-oneclick-step-label-id="oneClickStepLabel"
        data-oneclick-prev-btn-id="oneClickStepPrevBtn"
        data-oneclick-next-btn-id="oneClickStepNextBtn"
        data-oneclick-submit-btn-id="oneClickAuthSubmitBtn"
        class="mt-6"
      >
        <div id="oneClickAuthCard" class="rounded-[28px] border border-white/18 bg-[radial-gradient(circle_at_top,rgba(85,98,139,0.42),rgba(18,24,40,0.92)_58%)] p-4 text-white shadow-[18px_18px_44px_rgba(11,16,29,0.5),-12px_-12px_28px_rgba(99,112,152,0.14)] backdrop-blur-xl sm:p-5">
          <div class="flex items-center gap-3">
            <div class="grid h-11 w-11 shrink-0 place-items-center rounded-2xl border border-[#ffb26e]/30 bg-[#f57c00]/14 text-[#ffd2ac] shadow-[inset_0_1px_0_rgba(255,255,255,0.12)]">
              <i class="fa-solid fa-bolt text-base"></i>
            </div>
            <div class="min-w-0">
              <div class="text-[15px] font-semibold tracking-[0.01em] text-white">Créer votre compte en un click</div>
              <div class="mt-1 text-xs text-white/60">Même logique qu'avant, avec une présentation plus propre et plus directe.</div>
            </div>
          </div>
          <div class="mt-4 rounded-2xl border border-white/10 bg-white/[0.04] px-4 py-2.5">
            <div class="flex items-center justify-between gap-4">
              <div id="oneClickStepLabel" class="text-[11px] font-semibold uppercase tracking-[0.18em] text-white/60">Étape 1 sur 5</div>
              <div class="flex items-center gap-1.5 sm:gap-2">
                <span data-oneclick-dot class="h-2.5 w-2.5 rounded-full border border-[#f7c08d] bg-[#f48f45] shadow-[0_0_14px_rgba(244,143,69,0.45)]"></span>
                <span data-oneclick-dot class="h-2.5 w-2.5 rounded-full border border-white/10 bg-white/12"></span>
                <span data-oneclick-dot class="h-2.5 w-2.5 rounded-full border border-white/10 bg-white/12"></span>
                <span data-oneclick-dot class="h-2.5 w-2.5 rounded-full border border-white/10 bg-white/12"></span>
                <span data-oneclick-dot class="h-2.5 w-2.5 rounded-full border border-white/10 bg-white/12"></span>
              </div>
            </div>
          </div>
          <div class="mt-4 min-h-0">
            <div data-oneclick-step="0" class="space-y-3">
              <div>
                <label for="oneClickUsername" class="mb-1.5 block text-xs font-medium text-white/70">Nom du player</label>
                <input id="oneClickUsername" type="text" autocomplete="off" placeholder="ex: player509" class="block w-full rounded-2xl border border-white/16 bg-white/[0.08] px-4 py-3 text-sm text-white placeholder-white/45 shadow-[inset_6px_6px_12px_rgba(8,12,22,0.35),inset_-4px_-4px_10px_rgba(84,96,136,0.12)] outline-none transition focus:border-[#f48f45]" />
                <div class="mt-1.5 text-[11px] leading-5 text-white/55">3 à 24 caractères, avec au moins 4 lettres et 1 chiffre.</div>
              </div>
            </div>
            <div data-oneclick-step="1" class="hidden space-y-3">
              <div>
                <label for="oneClickPassword" class="mb-1.5 block text-xs font-medium text-white/70">Passcode</label>
                <div class="relative">
                  <input id="oneClickPassword" type="password" autocomplete="new-password" placeholder="Minimum 6 caractères" class="block w-full rounded-2xl border border-white/16 bg-white/[0.08] px-4 py-3 pr-12 text-sm text-white placeholder-white/45 shadow-[inset_6px_6px_12px_rgba(8,12,22,0.35),inset_-4px_-4px_10px_rgba(84,96,136,0.12)] outline-none transition focus:border-[#f48f45]" />
                  <button
                    id="oneClickPasswordToggleBtn"
                    type="button"
                    aria-label="Afficher le mot de passe"
                    title="Afficher le mot de passe"
                    class="absolute inset-y-0 right-2 my-auto grid h-9 w-9 place-items-center rounded-xl border border-white/16 bg-white/[0.08] text-white/82 transition hover:bg-white/14"
                  >
                    <i class="fa-regular fa-eye"></i>
                  </button>
                </div>
                <div class="mt-1.5 text-[11px] leading-5 text-white/55">Le passcode doit contenir au moins 1 lettre et 1 chiffre.</div>
              </div>
            </div>
            <div data-oneclick-step="2" class="hidden space-y-3">
              <div class="rounded-2xl border border-white/10 bg-white/[0.04] px-4 py-3 text-[12px] leading-5 text-white/62">
                Réécris exactement le passcode précédent pour confirmer qu’il est correct.
              </div>
              <div>
                <label for="oneClickPasswordConfirm" class="mb-1.5 block text-xs font-medium text-white/70">Vérification du passcode</label>
                <div class="relative">
                  <input id="oneClickPasswordConfirm" type="password" autocomplete="new-password" placeholder="Confirme ton passcode" class="block w-full rounded-2xl border border-white/16 bg-white/[0.08] px-4 py-3 pr-12 text-sm text-white placeholder-white/45 shadow-[inset_6px_6px_12px_rgba(8,12,22,0.35),inset_-4px_-4px_10px_rgba(84,96,136,0.12)] outline-none transition focus:border-[#f48f45]" />
                  <button
                    id="oneClickPasswordConfirmToggleBtn"
                    type="button"
                    aria-label="Afficher le mot de passe de confirmation"
                    title="Afficher le mot de passe de confirmation"
                    class="absolute inset-y-0 right-2 my-auto grid h-9 w-9 place-items-center rounded-xl border border-white/16 bg-white/[0.08] text-white/82 transition hover:bg-white/14"
                  >
                    <i class="fa-regular fa-eye"></i>
                  </button>
                </div>
              </div>
            </div>
            <div data-oneclick-step="3" class="hidden space-y-2.5">
              <div class="rounded-2xl border border-[#ffb26e]/22 bg-[#f57c00]/10 px-4 py-3.5 text-sm leading-6 text-white/80">
                Tu peux ajouter le code promo de la personne qui t'a invite ou continuer sans code. <span class="font-semibold text-white">Le bonus de bienvenue est termine, mais ton inscription reste normale dans les deux cas.</span>
              </div>
              <div>
                <label for="oneClickPromoInput" class="mb-1.5 block text-xs font-medium text-white/70">Code promo optionnel</label>
                <input
                  id="oneClickPromoInput"
                  type="text"
                  placeholder="Ex: BONUS25"
                  autocapitalize="characters"
                  autocomplete="off"
                  spellcheck="false"
                  value="${escapeAttr(promoPrefill)}"
                  class="block w-full rounded-2xl border border-white/16 bg-white/[0.08] px-4 py-3.5 text-sm uppercase text-white placeholder-white/45 shadow-[inset_6px_6px_12px_rgba(8,12,22,0.35),inset_-4px_-4px_10px_rgba(84,96,136,0.12)] outline-none transition focus:border-[#f48f45]"
                />
              </div>
              <div class="rounded-2xl border border-white/10 bg-white/[0.04] px-4 py-3 text-[12px] leading-5 text-white/56">
                Si tu n’as pas de code promo, passe directement à l’étape suivante. Désolé, le bonus de bienvenue est terminé.
              </div>
            </div>
            <div data-oneclick-step="4" class="hidden space-y-2.5">
              <div class="space-y-3 rounded-2xl border border-white/12 bg-white/[0.05] px-4 py-3.5">
                <label class="flex items-start gap-3 text-sm text-white/90">
                  <input
                    id="oneClickAgeCheckbox"
                    type="checkbox"
                    class="mt-1 h-4 w-4 rounded border-white/30 bg-white/10 text-[#f48f45]"
                  />
                  <span>J'ai 18 ans ou plus.</span>
                </label>
                <label class="flex items-start gap-3 text-sm text-white/90">
                  <input
                    id="oneClickTermsCheckbox"
                    type="checkbox"
                    class="mt-1 h-4 w-4 rounded border-white/30 bg-white/10 text-[#f48f45]"
                  />
                  <span>
                    J'accepte les
                    <a href="${TERMS_ROUTE}" target="_blank" rel="noopener noreferrer" class="font-semibold text-[#ffd8b5] underline underline-offset-2">conditions d'utilisation</a>.
                  </span>
                </label>
                <div class="text-[11px] leading-5 text-white/62 sm:text-xs">
                  Tu confirmes aussi avoir lu la
                  <a href="${PRIVACY_ROUTE}" target="_blank" rel="noopener noreferrer" class="text-[#ffd8b5] underline underline-offset-2">politique de confidentialité</a>
                  et les
                  <a href="${LEGAL_ROUTE}" target="_blank" rel="noopener noreferrer" class="text-[#ffd8b5] underline underline-offset-2">mentions légales</a>.
                </div>
              </div>
            </div>
          </div>
          <div id="oneClickAuthError" class="mt-3 min-h-5 text-sm text-[#ffb0b0]"></div>
          <div class="mt-3 flex flex-col gap-3 sm:flex-row">
            <button id="oneClickStepPrevBtn" type="button" class="hidden h-11 flex-1 rounded-2xl border border-white/12 bg-white/[0.06] text-sm font-semibold text-white/82 transition hover:bg-white/[0.1]">
              Précédent
            </button>
            <button id="oneClickStepNextBtn" type="button" class="h-11 flex-1 rounded-2xl border border-[#ffb26e]/80 bg-[#F57C00] text-sm font-semibold text-white shadow-[8px_8px_18px_rgba(163,82,27,0.45),-6px_-6px_14px_rgba(255,175,102,0.22)] transition hover:-translate-y-0.5 hover:bg-[#ff9549]">
              Suivant
            </button>
            <button id="oneClickAuthSubmitBtn" type="button" class="hidden h-11 flex-1 rounded-2xl border border-[#ffb26e]/80 bg-[#F57C00] text-sm font-semibold text-white shadow-[8px_8px_18px_rgba(163,82,27,0.45),-6px_-6px_14px_rgba(255,175,102,0.22)] transition hover:-translate-y-0.5 hover:bg-[#ff9549]">
              S'inscrire
            </button>
          </div>
        </div>
      </div>
    `
    : "";
  const formBody = isLegacySignin
    ? `
      <form id="authForm" class="mt-7 space-y-4 sm:space-y-5">
        <input
          id="identifierInput"
          type="text"
          placeholder="Numero, username ou ancien email"
          autocomplete="username"
          class="block w-full rounded-2xl border border-white/20 bg-white/10 px-5 py-3.5 text-sm text-white placeholder-white/60 shadow-[inset_6px_6px_12px_rgba(34,40,59,0.45),inset_-6px_-6px_12px_rgba(93,105,143,0.28)] backdrop-blur-md outline-none ring-0 transition focus:border-[#f48f45] sm:text-base"
        />
        <div class="relative">
          <input
            id="passwordInput"
            type="password"
            placeholder="Mot de passe"
            autocomplete="current-password"
            class="block w-full rounded-2xl border border-white/20 bg-white/10 px-5 py-3.5 pr-14 text-sm text-white placeholder-white/60 shadow-[inset_6px_6px_12px_rgba(34,40,59,0.45),inset_-6px_-6px_12px_rgba(93,105,143,0.28)] backdrop-blur-md outline-none ring-0 transition focus:border-[#f48f45] sm:text-base"
          />
          <button
            id="togglePasswordBtn"
            type="button"
            aria-label="Afficher le mot de passe"
            title="Afficher le mot de passe"
            class="absolute inset-y-0 right-3 my-auto grid h-9 w-9 place-items-center rounded-xl border border-white/20 bg-white/10 text-white/90 transition hover:bg-white/20"
          >
            <i class="fa-regular fa-eye"></i>
          </button>
        </div>
      </form>
      <div class="mt-3">
        <button id="forgotPasswordBtn" type="button" class="text-sm font-medium text-[#f48f45] hover:text-[#ff9f58]">Mot de passe oublié ?</button>
        <div id="forgotPasswordStatus" class="mt-2 min-h-5 text-xs text-white/75"></div>
      </div>
      <div id="authError" class="mt-4 min-h-5 text-sm text-[#ffb0b0]"></div>
      ${bootstrapInfo}
      <button
        id="authSubmitBtn"
        type="button"
        class="mt-2 w-full rounded-full bg-[#f48f45] px-6 py-3.5 text-sm font-bold tracking-wide text-white shadow-[8px_8px_18px_rgba(179,92,34,0.45),-6px_-6px_14px_rgba(255,182,120,0.22)] transition hover:-translate-y-0.5 hover:bg-[#ff9a4f] sm:text-base"
      >
        Se connecter
      </button>
    `
    : `
      ${signupMethodChooser}
      ${signupFlowTopActions}
      ${signupOneClickPanel}
      <form id="authForm" class="${isSignupPhoneFlow ? "mt-6 space-y-4 sm:space-y-5" : "hidden"}">
        <input
          id="usernameInput"
          type="text"
          placeholder="Username"
          autocomplete="nickname"
          autocapitalize="off"
          spellcheck="false"
          class="block w-full rounded-2xl border border-white/20 bg-white/10 px-5 py-3.5 text-sm text-white placeholder-white/60 shadow-[inset_6px_6px_12px_rgba(34,40,59,0.45),inset_-6px_-6px_12px_rgba(93,105,143,0.28)] backdrop-blur-md outline-none ring-0 transition focus:border-[#f48f45] sm:text-base"
        />
        <input
          id="phoneInput"
          type="tel"
          inputmode="numeric"
          autocomplete="tel"
          placeholder="Numero telephone / WhatsApp"
          class="block w-full rounded-2xl border border-white/20 bg-white/10 px-5 py-3.5 text-sm text-white placeholder-white/60 shadow-[inset_6px_6px_12px_rgba(34,40,59,0.45),inset_-6px_-6px_12px_rgba(93,105,143,0.28)] backdrop-blur-md outline-none ring-0 transition focus:border-[#f48f45] sm:text-base"
        />
        <div class="px-1 text-[11px] text-white/65 sm:text-xs">
          Entre ton numero WhatsApp ou telephone. Exemple: 50940507232.
        </div>
        <div class="relative">
          <input
            id="passwordInput"
            type="password"
            placeholder="Mot de passe"
            autocomplete="new-password"
            class="block w-full rounded-2xl border border-white/20 bg-white/10 px-5 py-3.5 pr-14 text-sm text-white placeholder-white/60 shadow-[inset_6px_6px_12px_rgba(34,40,59,0.45),inset_-6px_-6px_12px_rgba(93,105,143,0.28)] backdrop-blur-md outline-none ring-0 transition focus:border-[#f48f45] sm:text-base"
          />
          <button
            id="togglePasswordBtn"
            type="button"
            aria-label="Afficher le mot de passe"
            title="Afficher le mot de passe"
            class="absolute inset-y-0 right-3 my-auto grid h-9 w-9 place-items-center rounded-xl border border-white/20 bg-white/10 text-white/90 transition hover:bg-white/20"
          >
            <i class="fa-regular fa-eye"></i>
          </button>
        </div>
        <div class="relative">
          <input
            id="passwordConfirmInput"
            type="password"
            placeholder="Confirmer le mot de passe"
            autocomplete="new-password"
            class="block w-full rounded-2xl border border-white/20 bg-white/10 px-5 py-3.5 pr-14 text-sm text-white placeholder-white/60 shadow-[inset_6px_6px_12px_rgba(34,40,59,0.45),inset_-6px_-6px_12px_rgba(93,105,143,0.28)] backdrop-blur-md outline-none ring-0 transition focus:border-[#f48f45] sm:text-base"
          />
          <button
            id="togglePasswordConfirmBtn"
            type="button"
            aria-label="Afficher le mot de passe de confirmation"
            title="Afficher le mot de passe de confirmation"
            class="absolute inset-y-0 right-3 my-auto grid h-9 w-9 place-items-center rounded-xl border border-white/20 bg-white/10 text-white/90 transition hover:bg-white/20"
          >
            <i class="fa-regular fa-eye"></i>
          </button>
        </div>
        <div>
          <input
            id="promoCodeInput"
            type="text"
            placeholder="Code promo (optionnel)"
            autocapitalize="characters"
            autocomplete="off"
            spellcheck="false"
            value="${escapeAttr(promoPrefill)}"
            class="block w-full rounded-2xl border border-white/20 bg-white/10 px-5 py-3.5 text-sm uppercase text-white placeholder-white/60 shadow-[inset_6px_6px_12px_rgba(34,40,59,0.45),inset_-6px_-6px_12px_rgba(93,105,143,0.28)] backdrop-blur-md outline-none ring-0 transition focus:border-[#f48f45] sm:text-base"
          />
          <div class="mt-2 px-1 text-[11px] text-white/65 sm:text-xs">
            Utilise le code promo de la personne qui t'a invitee. Si tu n'en as pas, laisse vide: le bonus de bienvenue est terminé.
          </div>
        </div>
        <div class="space-y-3 rounded-2xl border border-white/15 bg-white/6 px-4 py-4">
          <label class="flex items-start gap-3 text-sm text-white/90">
            <input
              id="signupAgeCheckbox"
              type="checkbox"
              class="mt-1 h-4 w-4 rounded border-white/30 bg-white/10 text-[#f48f45]"
            />
            <span>J'ai 18 ans ou plus.</span>
          </label>
          <label class="flex items-start gap-3 text-sm text-white/90">
            <input
              id="signupTermsCheckbox"
              type="checkbox"
              class="mt-1 h-4 w-4 rounded border-white/30 bg-white/10 text-[#f48f45]"
            />
            <span>
              J'accepte les
              <a href="${TERMS_ROUTE}" target="_blank" rel="noopener noreferrer" class="font-semibold text-[#ffd8b5] underline underline-offset-2">conditions d'utilisation</a>.
            </span>
          </label>
          <div class="text-[11px] text-white/65 sm:text-xs">
            En creant un compte, tu confirmes aussi avoir lu la
            <a href="${PRIVACY_ROUTE}" target="_blank" rel="noopener noreferrer" class="text-[#ffd8b5] underline underline-offset-2">politique de confidentialite</a>
            et les
            <a href="${LEGAL_ROUTE}" target="_blank" rel="noopener noreferrer" class="text-[#ffd8b5] underline underline-offset-2">mentions legales</a>.
          </div>
        </div>
      </form>
      <div id="authError" class="mt-4 min-h-5 text-sm text-[#ffb0b0]"></div>
      ${bootstrapInfo}
      <button
        id="authSubmitBtn"
        type="button"
        class="mt-2 w-full rounded-full bg-[#f48f45] px-6 py-3.5 text-sm font-bold tracking-wide text-white shadow-[8px_8px_18px_rgba(179,92,34,0.45),-6px_-6px_14px_rgba(255,182,120,0.22)] transition hover:-translate-y-0.5 hover:bg-[#ff9a4f] sm:text-base"
        ${isSignupPhoneFlow ? "" : "style=\"display:none;\""}
      >
        Creer compte avec numero
      </button>
    `;

  getAuthShell().innerHTML = `
    <div id="appRoot" class="bg-[#0b1f3f] text-white font-['Poppins']" style="min-height:100svh;">
      <div class="lg:grid lg:h-[100svh] lg:grid-cols-[1.05fr_0.95fr]" style="min-height:100svh;">
        <section class="auth-scroll-pane flex h-[100svh] max-h-[100svh] flex-col px-6 pb-8 pt-8 sm:px-10 lg:min-h-0 lg:max-h-[100svh] lg:px-0 lg:pl-24 lg:pr-16 lg:pt-10" style="min-height:100svh;">
          <div class="mx-auto w-full max-w-xl rounded-3xl border border-white/10 bg-white/5 p-6 shadow-[12px_12px_28px_rgba(25,30,44,0.42),-10px_-10px_24px_rgba(97,110,150,0.16)] backdrop-blur-md lg:mx-0 lg:bg-transparent lg:p-0 lg:shadow-none lg:backdrop-blur-0">
            <img src="logo.png" alt="Logo" class="h-auto w-[152px] max-w-full object-contain sm:w-[168px] lg:hidden" />

            <h1 class="mt-8 text-5xl font-extrabold leading-none tracking-tight sm:text-6xl lg:mt-10 lg:text-7xl">
              Dominoes
            </h1>

            ${legacyToggle}
            ${referralHint}
            ${formBody}
          </div>

          <div class="mt-auto pt-8 text-[11px] leading-relaxed text-white/70 sm:text-xs">
            <div class="flex flex-wrap gap-x-4 gap-y-1">
              <a href="${TERMS_ROUTE}" target="_blank" rel="noopener noreferrer" class="hover:text-white">Conditions d'utilisation</a>
              <a href="${PRIVACY_ROUTE}" target="_blank" rel="noopener noreferrer" class="hover:text-white">Politique de confidentialité</a>
              <a href="${LEGAL_ROUTE}" target="_blank" rel="noopener noreferrer" class="hover:text-white">Mentions légales</a>
            </div>
          </div>
        </section>

        <aside class="relative hidden items-center justify-center border-l border-white/10 bg-white/5 backdrop-blur-md lg:flex lg:h-[100svh]" style="min-height:100svh;">
          <img id="rightLogo" src="logo.png" alt="Logo" class="h-auto w-[220px] max-w-[70%] object-contain opacity-95" />
        </aside>
      </div>

      <div class="fixed bottom-4 left-4 z-[3400]">
        <button
          id="loginDiscussionFabBtn"
          type="button"
          class="grid h-14 w-14 place-items-center rounded-full border border-white/25 bg-[#0b2a57]/78 text-white shadow-[10px_10px_22px_rgba(10,28,55,0.48),-8px_-8px_18px_rgba(90,133,201,0.18)] backdrop-blur-xl transition hover:-translate-y-0.5"
          aria-label="Ouvrir la discussion"
        >
          <i class="fa-solid fa-comments text-xl"></i>
        </button>
      </div>
    </div>
  `;

  bindPage1Events();
  if (isSignupOneClickFlow) {
    syncOneClickStepUi(document.getElementById("oneClickAuthInline"), oneClickSignupStep);
  }
  animatePage1();
  if (authEntryResetAssistPending && !authEntryResetAssistOpened) {
    authEntryResetAssistOpened = true;
    const identifierInput = document.getElementById("identifierInput");
    if (identifierInput && authEntryIdentifier) {
      identifierInput.value = authEntryIdentifier;
    }
    window.setTimeout(() => {
      if (authEntryIdentifier) {
        const refreshedIdentifierInput = document.getElementById("identifierInput");
        if (refreshedIdentifierInput && !refreshedIdentifierInput.value) {
          refreshedIdentifierInput.value = authEntryIdentifier;
        }
      }
      openForgotPasswordAssistModal(authEntryIdentifier);
    }, 180);
  }
  updateAuthModalBodyLock();
}

function bindPage1Events() {
  const submitBtn = document.getElementById("authSubmitBtn");
  const form = document.getElementById("authForm");
  const openLegacySigninBtn = document.getElementById("openLegacySigninBtn");
  const openPhoneSignupBtn = document.getElementById("openPhoneSignupBtn");
  const openPhoneFieldsBtn = document.getElementById("openPhoneFieldsBtn");
  const backToSignupMethodsBtn = document.getElementById("backToSignupMethodsBtn");
  const identifierInput = document.getElementById("identifierInput");
  const usernameInput = document.getElementById("usernameInput");
  const phoneInput = document.getElementById("phoneInput");
  const passwordInput = document.getElementById("passwordInput");
  const passwordConfirmInput = document.getElementById("passwordConfirmInput");
  const togglePasswordBtn = document.getElementById("togglePasswordBtn");
  const togglePasswordConfirmBtn = document.getElementById("togglePasswordConfirmBtn");
  const promoCodeInput = document.getElementById("promoCodeInput");
  const forgotPasswordBtn = document.getElementById("forgotPasswordBtn");
  const forgotPasswordStatus = document.getElementById("forgotPasswordStatus");
  const discussionFabBtn = document.getElementById("loginDiscussionFabBtn");

  const getSignupConsentState = () => {
    const ageCheckbox = document.getElementById("signupAgeCheckbox");
    const termsCheckbox = document.getElementById("signupTermsCheckbox");
    return {
      ageCheckbox,
      termsCheckbox,
      ageAccepted: ageCheckbox?.checked === true,
      termsAccepted: termsCheckbox?.checked === true,
    };
  };

  const setForgotPasswordStatus = (text = "", tone = "neutral") => {
    if (!forgotPasswordStatus) return;
    const toneClass = tone === "success"
      ? "text-emerald-200"
      : tone === "error"
        ? "text-[#ffb0b0]"
        : "text-white/75";
    forgotPasswordStatus.className = `mt-2 min-h-5 text-xs ${toneClass}`;
    forgotPasswordStatus.textContent = text;
  };

  const bindPasswordToggle = (inputEl, buttonEl, hiddenLabel, visibleLabel) => {
    if (!inputEl || !buttonEl || buttonEl.dataset.bound === "1") return;
    buttonEl.dataset.bound = "1";
    const icon = buttonEl.querySelector("i");
    buttonEl.addEventListener("click", () => {
      const isHidden = inputEl.type === "password";
      inputEl.type = isHidden ? "text" : "password";
      const nextLabel = isHidden ? visibleLabel : hiddenLabel;
      buttonEl.setAttribute("aria-label", nextLabel);
      buttonEl.setAttribute("title", nextLabel);
      if (icon) {
        icon.classList.toggle("fa-eye", !isHidden);
        icon.classList.toggle("fa-eye-slash", isHidden);
      }
    });
  };

  bindPasswordToggle(
    passwordInput,
    togglePasswordBtn,
    "Afficher le mot de passe",
    "Masquer le mot de passe"
  );
  bindPasswordToggle(
    passwordConfirmInput,
    togglePasswordConfirmBtn,
    "Afficher le mot de passe de confirmation",
    "Masquer le mot de passe de confirmation"
  );

  if (openLegacySigninBtn && openLegacySigninBtn.dataset.bound !== "1") {
    openLegacySigninBtn.dataset.bound = "1";
    openLegacySigninBtn.addEventListener("click", () => {
      authMode = "signin";
      renderPage1();
    });
  }

  if (openPhoneSignupBtn && openPhoneSignupBtn.dataset.bound !== "1") {
    openPhoneSignupBtn.dataset.bound = "1";
    openPhoneSignupBtn.addEventListener("click", () => {
      authMode = "signup";
      signupCreationMode = "chooser";
      oneClickSignupStep = 0;
      renderPage1();
    });
  }

  if (openPhoneFieldsBtn && openPhoneFieldsBtn.dataset.bound !== "1") {
    openPhoneFieldsBtn.dataset.bound = "1";
    openPhoneFieldsBtn.addEventListener("click", () => {
      signupCreationMode = "phone";
      oneClickSignupStep = 0;
      renderPage1();
    });
  }

  if (backToSignupMethodsBtn && backToSignupMethodsBtn.dataset.bound !== "1") {
    backToSignupMethodsBtn.dataset.bound = "1";
    backToSignupMethodsBtn.addEventListener("click", () => {
      signupCreationMode = "chooser";
      oneClickSignupStep = 0;
      renderPage1();
    });
  }

  const submitAuth = async () => {
    const identifier = (identifierInput?.value || "").trim();
    const username = normalizeUsername(usernameInput?.value || "");
    const phone = normalizePhoneLogin(phoneInput?.value || "");
    const password = passwordInput?.value || "";
    const confirmPassword = passwordConfirmInput?.value || "";
    const promoCode = authMode === "signup" ? normalizeCode(promoCodeInput?.value || "") : "";
    const errorEl = document.getElementById("authError");
    const usernameCandidate = normalizeUsername(identifier);
    const signinByEmail = identifier.includes("@");
    const signinByPhone = !signinByEmail && isValidPhoneLogin(identifier);
    pageAuthDebug("submitAuth:begin", {
      identifier,
      username,
      phone,
      mode: authMode,
      promoCode,
      signinByEmail,
      signinByPhone,
    });

    if (authMode === "signin") {
      if (signinByEmail) {
        if (!isValidEmail(identifier)) {
          if (errorEl) errorEl.textContent = "Email invalide.";
          return;
        }
      } else if (signinByPhone) {
        if (!isValidPhoneLogin(identifier)) {
          if (errorEl) errorEl.textContent = "Numero invalide.";
          return;
        }
      } else if (!isValidUsername(usernameCandidate)) {
        if (errorEl) errorEl.textContent = "Username invalide.";
        return;
      }
    } else if (!isValidUsername(username)) {
      if (errorEl) errorEl.textContent = "Username invalide.";
      return;
    } else if (!isValidPhoneLogin(phone)) {
      if (errorEl) errorEl.textContent = "Numero telephone / WhatsApp invalide.";
      return;
    }
    if (!isValidPassword(password)) {
      if (errorEl) errorEl.textContent = "Mot de passe invalide (minimum 6 caractères).";
      return;
    }
    if (authMode === "signup" && password !== confirmPassword) {
      if (errorEl) errorEl.textContent = "Le mot de passe de confirmation ne correspond pas.";
      return;
    }
    const signupConsent = authMode === "signup" ? getSignupConsentState() : null;
    if (authMode === "signup" && signupConsent?.ageAccepted !== true) {
      if (errorEl) errorEl.textContent = "Tu dois confirmer que tu as 18 ans ou plus.";
      return;
    }
    if (authMode === "signup" && signupConsent?.termsAccepted !== true) {
      if (errorEl) errorEl.textContent = "Tu dois accepter les conditions d'utilisation pour créer un compte.";
      return;
    }

    if (errorEl) errorEl.textContent = "";
    setForgotPasswordStatus("", "neutral");

      try {
        await withButtonLoading(submitBtn, async () => {
          authFlowBusy = true;
          if (authMode === "signin") {
            savePendingPromoCode("");
            savePendingPhone("");
            if (signinByEmail) {
              await loginWithEmail(identifier, password);
            } else if (signinByPhone) {
              await loginWithPhone(identifier, password);
            } else {
              await loginWithUsername(usernameCandidate, password);
            }
            pageAuthDebug("submitAuth:signinSuccess", {
              uid: String(auth.currentUser?.uid || ""),
              signinByEmail,
              signinByPhone,
              username: usernameCandidate,
            });
            await handleAuthenticatedUser(auth.currentUser);
          } else {
            assertSignupAllowedOnThisDevice();
            savePendingPromoCode(promoCode);
            savePendingUsername(username);
            savePendingPhone(phone);
            await signupWithPhone(phone, password);
            await syncCurrentUserDisplayName(username);
            saveAuthProfileHint(auth.currentUser, { username, phone });
            storeUserImportanceNotice();
            pageAuthDebug("submitAuth:signupSuccess", {
              uid: String(auth.currentUser?.uid || ""),
              username,
              phone,
              currentDisplayName: String(auth.currentUser?.displayName || ""),
              currentEmail: String(auth.currentUser?.email || ""),
            });
            await handleAuthenticatedUser(auth.currentUser, promoCode);
          }
        }, { loadingLabel: authMode === "signin" ? "Connexion..." : "Création..." });
      } catch (err) {
      console.error("Auth error:", err);
      pageAuthDebug("submitAuth:error", {
        error: String(err?.message || err),
        code: String(err?.code || ""),
      });
      if (errorEl) errorEl.textContent = formatAuthError(err, "Erreur d'authentification");
    } finally {
      authFlowBusy = false;
      pageAuthDebug("submitAuth:finally");
    }
  };

  if (submitBtn) submitBtn.addEventListener("click", submitAuth);
  if (form) {
    form.addEventListener("submit", (ev) => {
      ev.preventDefault();
      submitAuth();
    });
  }

  const oneClickRoot = document.getElementById("oneClickAuthInline");
  const oneClickCancelBtn = document.getElementById("oneClickAuthCancelBtn");
  const oneClickSubmitBtn = document.getElementById("oneClickAuthSubmitBtn");
  const oneClickErrorEl = document.getElementById("oneClickAuthError");
  const oneClickUsernameInput = document.getElementById("oneClickUsername");
  const oneClickPasswordInput = document.getElementById("oneClickPassword");
  const oneClickPasswordConfirmInput = document.getElementById("oneClickPasswordConfirm");
  const oneClickPromoInput = document.getElementById("oneClickPromoInput");
  const oneClickPasswordToggleBtn = document.getElementById("oneClickPasswordToggleBtn");
  const oneClickPasswordConfirmToggleBtn = document.getElementById("oneClickPasswordConfirmToggleBtn");
  const oneClickAgeCheckbox = document.getElementById("oneClickAgeCheckbox");
  const oneClickTermsCheckbox = document.getElementById("oneClickTermsCheckbox");
  const oneClickStepPrevBtn = document.getElementById("oneClickStepPrevBtn");
  const oneClickStepNextBtn = document.getElementById("oneClickStepNextBtn");

  const validateOneClickUsernameStep = () => {
    const usernameRaw = String(oneClickUsernameInput?.value || "").trim();
    const username = normalizeUsername(usernameRaw);

    if (!isValidOneClickUsername(username)) {
      if (oneClickErrorEl) oneClickErrorEl.textContent = "Nom du player invalide : au moins 4 lettres, 1 chiffre, et seulement lettres, chiffres, point, tiret ou underscore.";
      return false;
    }
    return true;
  };

  const validateOneClickPasswordStep = () => {
    const password = String(oneClickPasswordInput?.value || "");

    if (!isValidOneClickPassword(password)) {
      if (oneClickErrorEl) oneClickErrorEl.textContent = "Passcode invalide : minimum 6 caractères avec au moins 1 lettre et 1 chiffre.";
      return false;
    }
    return true;
  };

  const validateOneClickPasswordConfirmStep = () => {
    const password = String(oneClickPasswordInput?.value || "");
    const passwordConfirm = String(oneClickPasswordConfirmInput?.value || "");

    if (!passwordConfirm) {
      if (oneClickErrorEl) oneClickErrorEl.textContent = "Tu dois confirmer le passcode.";
      return false;
    }
    if (password !== passwordConfirm) {
      if (oneClickErrorEl) oneClickErrorEl.textContent = "La vérification du passcode ne correspond pas.";
      return false;
    }
    return true;
  };

  bindPasswordToggle(
    oneClickPasswordInput,
    oneClickPasswordToggleBtn,
    "Afficher le mot de passe",
    "Masquer le mot de passe"
  );
  bindPasswordToggle(
    oneClickPasswordConfirmInput,
    oneClickPasswordConfirmToggleBtn,
    "Afficher le mot de passe de confirmation",
    "Masquer le mot de passe de confirmation"
  );

  if (oneClickCancelBtn && oneClickCancelBtn.dataset.bound !== "1") {
    oneClickCancelBtn.dataset.bound = "1";
    oneClickCancelBtn.addEventListener("click", () => {
      signupCreationMode = "chooser";
      oneClickSignupStep = 0;
      renderPage1();
    });
  }

  if (oneClickStepPrevBtn && oneClickStepPrevBtn.dataset.bound !== "1") {
    oneClickStepPrevBtn.dataset.bound = "1";
    oneClickStepPrevBtn.addEventListener("click", () => {
      if (oneClickErrorEl) oneClickErrorEl.textContent = "";
      syncOneClickStepUi(oneClickRoot, oneClickSignupStep - 1);
    });
  }

  if (oneClickStepNextBtn && oneClickStepNextBtn.dataset.bound !== "1") {
    oneClickStepNextBtn.dataset.bound = "1";
    oneClickStepNextBtn.addEventListener("click", () => {
      if (oneClickErrorEl) oneClickErrorEl.textContent = "";
      const currentStep = Number(oneClickRoot?.dataset.step || oneClickSignupStep || 0);
      if (currentStep === 0 && !validateOneClickUsernameStep()) return;
      if (currentStep === 1 && !validateOneClickPasswordStep()) return;
      if (currentStep === 2 && !validateOneClickPasswordConfirmStep()) return;
      syncOneClickStepUi(oneClickRoot, currentStep + 1);
    });
  }

  if (oneClickSubmitBtn && oneClickSubmitBtn.dataset.bound !== "1") {
    oneClickSubmitBtn.dataset.bound = "1";
    oneClickSubmitBtn.addEventListener("click", async () => {
      const usernameRaw = String(oneClickUsernameInput?.value || "").trim();
      const username = normalizeUsername(usernameRaw);
      const password = String(oneClickPasswordInput?.value || "");
      const passwordConfirm = String(oneClickPasswordConfirmInput?.value || "");
      const oneClickId = createOneClickAccountId();
      const promoCode = normalizeCode(oneClickPromoInput?.value || "");

      if (oneClickErrorEl) oneClickErrorEl.textContent = "";
      const ageAccepted = oneClickAgeCheckbox?.checked === true;
      const termsAccepted = oneClickTermsCheckbox?.checked === true;
      if (ageAccepted !== true) {
        if (oneClickErrorEl) oneClickErrorEl.textContent = "Tu dois confirmer que tu as 18 ans ou plus.";
        return;
      }
      if (termsAccepted !== true) {
        if (oneClickErrorEl) oneClickErrorEl.textContent = "Tu dois accepter les conditions d'utilisation pour créer ton compte.";
        return;
      }
      if (!validateOneClickUsernameStep()) return;
      if (!validateOneClickPasswordStep()) return;
      if (!validateOneClickPasswordConfirmStep()) return;

      try {
        await withButtonLoading(oneClickSubmitBtn, async () => {
          pageAuthDebug("oneClickSignup:start", { username, oneClickId });
          assertSignupAllowedOnThisDevice();
          savePendingUsername(username);
          savePendingOneClickId(oneClickId);
          savePendingPromoCode(promoCode);
          await signupWithUsername(username, password);
          await syncCurrentUserDisplayName(username);
          saveAuthProfileHint(auth.currentUser, { username });
          storeUserImportanceNotice();
          pageAuthDebug("oneClickSignup:success", {
            uid: String(auth.currentUser?.uid || ""),
            username,
            oneClickId,
            currentDisplayName: String(auth.currentUser?.displayName || ""),
            currentEmail: String(auth.currentUser?.email || ""),
          });
          await handleAuthenticatedUser(auth.currentUser);
        }, { loadingLabel: "Création..." });
      } catch (err) {
        console.error("One click auth error:", err);
        pageAuthDebug("oneClickSignup:error", {
          code: String(err?.code || ""),
          message: String(err?.message || err),
        });
        if (oneClickErrorEl) oneClickErrorEl.textContent = formatAuthError(err, "Impossible de créer ce compte.");
      }
    });
  }

  if (forgotPasswordBtn && forgotPasswordBtn.dataset.bound !== "1") {
    forgotPasswordBtn.dataset.bound = "1";
    forgotPasswordBtn.addEventListener("click", () => {
      const identifier = (identifierInput?.value || "").trim();
      const errorEl = document.getElementById("authError");
      if (errorEl) errorEl.textContent = "";
      setForgotPasswordStatus("Une fenetre d'assistance est ouverte pour t'aider a recuperer le compte.", "success");
      openForgotPasswordAssistModal(identifier);
    });
  }

  if (discussionFabBtn && discussionFabBtn.dataset.bound !== "1") {
    discussionFabBtn.dataset.bound = "1";
    discussionFabBtn.addEventListener("click", () => {
      window.location.href = "./discussion.html";
    });
  }
}

renderAuthLoading();
showGlobalLoading("Préparation de la connexion...");
pageAuthDebug("bootstrap:renderAuthLoadingDone");
authBootstrapReady = true;
pageAuthDebug("bootstrap:noGoogle:ready");
if (auth.currentUser) {
  handleAuthenticatedUser(auth.currentUser).catch((err) => {
    pageAuthDebug("bootstrap:noGoogle:currentUser:catch", {
      error: String(err?.message || err),
      code: String(err?.code || ""),
    });
  });
} else {
  hideGlobalLoading();
  renderPage1();
}

async function animatePage1() {
  let anime = null;
  try {
    anime = await ensureAnimeRuntime();
  } catch (error) {
    console.warn("[PAGE1] animation runtime unavailable", error);
    return;
  }
  if (!anime) return;

  anime({
    targets: "#appRoot",
    opacity: [0, 1],
    duration: 650,
    easing: "easeOutQuad",
  });

  const animatedInputs = authMode === "signin"
    ? ["#identifierInput", "#passwordInput"]
    : (signupCreationMode === "phone"
      ? ["#usernameInput", "#phoneInput", "#passwordInput"]
      : signupCreationMode === "oneclick"
        ? ["#oneClickUsername", "#oneClickPassword", "#oneClickPasswordConfirm"]
        : ["#openPhoneFieldsBtn"]);
  if (authMode === "signup" && signupCreationMode === "phone") {
    animatedInputs.push("#promoCodeInput");
    animatedInputs.push("#passwordConfirmInput");
  }
  if (authMode === "signup" && signupCreationMode === "oneclick") {
    animatedInputs.push("#oneClickPromoInput");
  }

  anime({
    targets: animatedInputs,
    translateY: [22, 0],
    opacity: [0, 1],
    delay: anime.stagger(120, { start: 200 }),
    duration: 600,
    easing: "easeOutCubic",
  });

  anime({
    targets: "#rightLogo",
    translateY: [-8, 8],
    direction: "alternate",
    loop: true,
    duration: 2200,
    easing: "easeInOutSine",
  });

  const signInBtn = document.getElementById("authSubmitBtn");
  if (signInBtn) {
    signInBtn.addEventListener("mouseenter", () => {
      anime({ targets: signInBtn, scale: 1.025, duration: 180, easing: "easeOutQuad" });
    });
    signInBtn.addEventListener("mouseleave", () => {
      anime({ targets: signInBtn, scale: 1, duration: 180, easing: "easeOutQuad" });
    });
  }
}

watchAuthState((user) => {
  pageAuthDebug("watchAuthState:callback", {
    hasUser: Boolean(user),
    uid: String(user?.uid || ""),
    email: String(user?.email || ""),
    emailVerified: user?.emailVerified === true,
    currentUid: String(auth.currentUser?.uid || ""),
  });
  authStateResolved = true;
  latestObservedUser = user || null;
  if (user) {
    setAuthBootstrapMessage("", "info");
    clearAuthFallbackRenderTimer();
    handleAuthenticatedUser(user).catch((err) => {
      console.error("Auth state redirect error:", err);
      pageAuthDebug("watchAuthState:handleAuthenticatedUser:catch", {
        error: String(err?.message || err),
        code: String(err?.code || ""),
      });
      if (userRequiresEmailVerification(user)) {
        showEmailVerificationModal(user).catch((modalErr) => {
          console.error("Email verification modal error:", modalErr);
          pageAuthDebug("watchAuthState:showEmailVerificationModal:catch", {
            error: String(modalErr?.message || modalErr),
          });
        });
        return;
      }
      redirectToHomeApp(user);
    });
    return;
  }
  redirectingToApp = false;
  pageAuthDebug("watchAuthState:noUser");
  hideGlobalLoading();
  if (authBootstrapReady !== true) return;
  scheduleAuthFallbackRender();
});
