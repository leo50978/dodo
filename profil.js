import { auth, formatAuthError, logoutCurrentUser, watchAuthState } from "./auth.js";
import { mountXchangeModal, getXchangeState } from "./xchange.js";
import { mountRetraitModal, getWithdrawalRuleStatus } from "./retrait.js";
import {
  bindPendingOperationsActions,
  getPendingOperations,
  mountSoldeModal,
  renderPendingOperationsList,
  waitForBalanceHydration,
} from "./solde.js";
import {
  EmailAuthProvider,
  db,
  doc,
  getDoc,
  reauthenticateWithCredential,
  updatePassword,
} from "./firebase-init.js";
import { getDepositFundingStatusSecure } from "./secure-functions.js";
import { SUPPORT_WHATSAPP_PHONE, buildSupportWhatsAppUrl } from "./support-contact.js";
const BALANCE_DEBUG = false;
const WELCOME_PROGRESS_DEBUG = true;
const WITHDRAWAL_CANCEL_DEBUG = true;
const ASSISTANCE_PHONE = SUPPORT_WHATSAPP_PHONE;
const RATE_HTG_TO_DOES = 20;
const AUTH_PROFILE_HINT_STORAGE_KEY = "domino_auth_profile_hint_v1";
const WELCOME_LOCKED_SELL_STORAGE_KEY = "domino_welcome_locked_sell_attempt_v1";
const PUBLIC_HOME_URL = "https://dominoeslakay.com/inedex.html";
let referralLoadToken = 0;
let referralHintFreezeUntil = 0;
let referralHintRestoreTimer = null;
let withdrawalAvailabilityToken = 0;
let profileRealtimeUid = "";
let profileRealtimeRefreshTimer = null;
let latestProfileClientData = null;
let latestProfileFundingData = null;
let profileFundingUid = "";
let profileFundingRefreshTimer = null;
let profileFundingRequestToken = 0;
let lastWithdrawalHoldSignature = "";
let profileClientPollTimer = null;
let profileVisibilityBound = false;
const PROFILE_CLIENT_REFRESH_MS = 3 * 60 * 1000;
let profilePendingOpsBound = false;
let profileWithdrawalEventsBound = false;

function safeCount(value) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.max(0, Math.floor(n)) : 0;
}

function pickFirstFiniteNumber(...values) {
  for (const value of values) {
    const n = Number(value);
    if (Number.isFinite(n)) return n;
  }
  return 0;
}

function normalizeReferralCode(value) {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9_-]/g, "");
}

function buildProfileReferralLink(code) {
  const normalized = normalizeReferralCode(code);
  if (!normalized) return "";
  const url = new URL(PUBLIC_HOME_URL);
  url.hash = "";
  url.searchParams.set("ref", normalized);
  return url.toString();
}

function getBalanceBaseForUi() {
  const base = window.__userBaseBalance;
  const fallback = window.__userBalance;
  if (base === null || typeof(base) === "undefined" || Number.isNaN(Number(base))) {
    return Number(fallback || 0);
  }
  return Number(base);
}

function isSyntheticPhoneLoginEmail(value) {
  const email = String(value || "").trim().toLowerCase();
  return email.endsWith("@phone.dominoeslakay.local");
}

function readAuthProfileHint(user) {
  const uid = String(user?.uid || auth.currentUser?.uid || "").trim();
  if (!uid) return null;
  try {
    const raw = window.localStorage?.getItem(AUTH_PROFILE_HINT_STORAGE_KEY) || "";
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") return null;
    if (String(parsed.uid || "").trim() !== uid) return null;
    return {
      username: String(parsed.username || "").trim(),
      phone: String(parsed.phone || "").trim(),
      updatedAtMs: Number(parsed.updatedAtMs || 0) || 0,
    };
  } catch (_) {
    return null;
  }
}

function getWelcomeLockedSellStorageKey(uid = "") {
  return `${WELCOME_LOCKED_SELL_STORAGE_KEY}:${String(uid || "").trim()}`;
}

function readWelcomeLockedSellAttempt(uid = "") {
  const safeUid = String(uid || "").trim();
  if (!safeUid) return null;
  try {
    const raw = window.localStorage?.getItem(getWelcomeLockedSellStorageKey(safeUid)) || "";
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    return {
      amountDoes: safeCount(parsed?.amountDoes),
      updatedAtMs: Number.isFinite(Number(parsed?.updatedAtMs)) ? Number(parsed.updatedAtMs) : 0,
    };
  } catch {
    return null;
  }
}

function resolveDisplayName(user, clientData = null) {
  const username = String(clientData?.username || "").trim();
  if (username) return { value: username, source: "client.username" };
  const profileHint = readAuthProfileHint(user);
  if (profileHint?.username) return { value: profileHint.username, source: "local.username_hint" };
  if (!user) return { value: "Guest", source: "guest" };
  if (user.displayName) return { value: user.displayName, source: "auth.displayName" };
  const phone = String(clientData?.phone || "").trim();
  if (phone) return { value: phone, source: "client.phone" };
  if (profileHint?.phone) return { value: profileHint.phone, source: "local.phone_hint" };
  if (user.email && !isSyntheticPhoneLoginEmail(user.email)) return { value: user.email.split("@")[0], source: "auth.email" };
  return { value: "Player", source: "fallback.player" };
}

function getDisplayName(user, clientData = null) {
  return resolveDisplayName(user, clientData).value;
}

function getDisplayContact(user, clientData = null) {
  const email = String(user?.email || "").trim();
  if (email && !isSyntheticPhoneLoginEmail(email)) return email;
  const phone = String(clientData?.phone || "").trim();
  if (phone) return phone;
  const profileHint = readAuthProfileHint(user);
  if (profileHint?.phone) return profileHint.phone;
  return "-";
}

function formatAmount(value) {
  const amount = Number(value || 0);
  return new Intl.NumberFormat("fr-HT", {
    style: "currency",
    currency: "HTG",
    maximumFractionDigits: 0,
  }).format(amount);
}

function formatDoesAmount(value) {
  const amount = Number(value || 0);
  return new Intl.NumberFormat("fr-HT", {
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

function ensureWithdrawalHoldModal() {
  const existing = document.getElementById("profileWithdrawalHoldOverlay");
  if (existing) return existing;

  const overlay = document.createElement("div");
  overlay.id = "profileWithdrawalHoldOverlay";
  overlay.className = "fixed inset-0 z-[3600] hidden items-center justify-center bg-black/55 p-4 backdrop-blur-sm";
  overlay.innerHTML = `
    <div class="w-full max-w-md rounded-3xl border border-white/20 bg-[#3F4766]/82 p-5 text-white shadow-[14px_14px_34px_rgba(16,23,40,0.5),-10px_-10px_24px_rgba(112,126,165,0.2)] backdrop-blur-xl sm:p-6">
      <p class="text-xs font-semibold uppercase tracking-[0.16em] text-white/70">Compte gelé</p>
      <h3 class="mt-2 text-xl font-bold text-white">Retraits bloqués</h3>
      <p id="profileWithdrawalHoldMessage" class="mt-3 text-sm leading-6 text-white/90"></p>
      <div id="profileWithdrawalHoldDetails" class="mt-3 rounded-2xl border border-white/20 bg-white/10 p-3 text-xs leading-5 text-white/82"></div>
      <div class="mt-4 grid gap-2 sm:grid-cols-2">
        <button id="profileWithdrawalHoldClose" type="button" class="h-11 rounded-2xl border border-white/20 bg-white/10 text-sm font-semibold text-white">
          Je comprends
        </button>
        <button id="profileWithdrawalHoldContact" type="button" class="h-11 rounded-2xl border border-[#ffb26e] bg-[#F57C00] text-sm font-semibold text-white shadow-[8px_8px_18px_rgba(163,82,27,0.45),-6px_-6px_14px_rgba(255,175,102,0.22)]">
          Contacter l'assistance
        </button>
      </div>
    </div>
  `;

  document.body.appendChild(overlay);
  const close = () => {
    overlay.classList.add("hidden");
    overlay.classList.remove("flex");
  };
  overlay.querySelector("#profileWithdrawalHoldClose")?.addEventListener("click", close);
  overlay.querySelector("#profileWithdrawalHoldContact")?.addEventListener("click", () => {
    window.open(buildSupportWhatsAppUrl("Bonjour, je veux plaider ma cause concernant le gel de mon compte pour retrait."), "_blank", "noopener,noreferrer");
  });
  overlay.addEventListener("click", (ev) => {
    if (ev.target === overlay) close();
  });
  return overlay;
}

function ensureProfilePendingOperationsModal() {
  const overlay = document.getElementById("profilePendingOpsOverlay");
  if (!overlay) return null;
  if (overlay.dataset.bound === "1") return overlay;
  overlay.dataset.bound = "1";

  const close = () => {
    overlay.classList.add("hidden");
    overlay.classList.remove("flex");
    document.body.classList.remove("overflow-hidden");
  };

  overlay.querySelector("#profilePendingOpsClose")?.addEventListener("click", close);
  overlay.addEventListener("click", (event) => {
    if (event.target === overlay) {
      close();
    }
  });

  window.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && overlay.classList.contains("flex")) {
      close();
    }
  });

  overlay.__openPendingOps = () => {
    overlay.classList.remove("hidden");
    overlay.classList.add("flex");
    document.body.classList.add("overflow-hidden");
    refreshProfilePendingOperationsModal();
  };
  overlay.__closePendingOps = close;

  return overlay;
}

function formatProfilePasswordError(error) {
  const code = String(error?.code || "");
  if (code.includes("wrong-password") || code.includes("invalid-credential")) {
    return "Mot de passe actuel incorrect.";
  }
  if (code.includes("too-many-requests")) {
    return "Trop de tentatives. Réessaie un peu plus tard.";
  }
  if (code.includes("requires-recent-login")) {
    return "Reconnecte-toi puis reviens changer ton mot de passe.";
  }
  return formatAuthError(error, "Impossible de changer le mot de passe.");
}

function bindPasswordVisibilityToggle(button, input) {
  if (!button || !input || button.dataset.bound === "1") return;
  button.dataset.bound = "1";
  const icon = button.querySelector("i");
  button.addEventListener("click", () => {
    const hidden = input.type === "password";
    input.type = hidden ? "text" : "password";
    button.setAttribute("aria-label", hidden ? "Masquer le mot de passe" : "Afficher le mot de passe");
    if (icon) {
      icon.classList.toggle("fa-eye", !hidden);
      icon.classList.toggle("fa-eye-slash", hidden);
    }
  });
}

function ensureProfilePasswordModal() {
  let overlay = document.getElementById("profilePasswordOverlay");
  if (overlay) return overlay;

  overlay = document.createElement("div");
  overlay.id = "profilePasswordOverlay";
  overlay.className = "fixed inset-0 z-[3700] hidden items-center justify-center bg-black/55 p-4 backdrop-blur-sm";
  overlay.innerHTML = `
    <div class="w-full max-w-md rounded-3xl border border-white/20 bg-[#3F4766]/82 p-5 text-white shadow-[14px_14px_34px_rgba(16,23,40,0.5),-10px_-10px_24px_rgba(112,126,165,0.2)] backdrop-blur-xl sm:p-6">
      <div class="flex items-center justify-between gap-3">
        <div>
          <p class="text-xs font-semibold uppercase tracking-[0.16em] text-white/70">Securite compte</p>
          <h3 class="mt-1 text-xl font-bold text-white">Changer mot de passe</h3>
        </div>
        <button id="profilePasswordClose" type="button" class="grid h-10 w-10 place-items-center rounded-2xl border border-white/20 bg-white/10 text-white">
          <i class="fa-solid fa-xmark"></i>
        </button>
      </div>

      <p class="mt-3 text-sm leading-6 text-white/84">
        Entre ton mot de passe actuel puis choisis un nouveau mot de passe pour mieux proteger ton compte.
      </p>

      <form id="profilePasswordForm" class="mt-4 space-y-3">
        <label class="block">
          <span class="mb-2 block text-xs font-semibold uppercase tracking-[0.14em] text-white/65">Mot de passe actuel</span>
          <div class="relative">
            <input id="profileCurrentPassword" type="password" autocomplete="current-password" class="w-full rounded-2xl border border-white/16 bg-white/10 px-4 py-3 pr-12 text-sm text-white outline-none transition focus:border-[#f48f45]" />
            <button id="profileCurrentPasswordToggle" type="button" class="absolute inset-y-0 right-3 my-auto grid h-9 w-9 place-items-center rounded-xl border border-white/15 bg-white/10 text-white/90">
              <i class="fa-regular fa-eye"></i>
            </button>
          </div>
        </label>

        <label class="block">
          <span class="mb-2 block text-xs font-semibold uppercase tracking-[0.14em] text-white/65">Nouveau mot de passe</span>
          <div class="relative">
            <input id="profileNewPassword" type="password" autocomplete="new-password" class="w-full rounded-2xl border border-white/16 bg-white/10 px-4 py-3 pr-12 text-sm text-white outline-none transition focus:border-[#f48f45]" />
            <button id="profileNewPasswordToggle" type="button" class="absolute inset-y-0 right-3 my-auto grid h-9 w-9 place-items-center rounded-xl border border-white/15 bg-white/10 text-white/90">
              <i class="fa-regular fa-eye"></i>
            </button>
          </div>
        </label>

        <label class="block">
          <span class="mb-2 block text-xs font-semibold uppercase tracking-[0.14em] text-white/65">Confirmer le nouveau</span>
          <div class="relative">
            <input id="profileConfirmPassword" type="password" autocomplete="new-password" class="w-full rounded-2xl border border-white/16 bg-white/10 px-4 py-3 pr-12 text-sm text-white outline-none transition focus:border-[#f48f45]" />
            <button id="profileConfirmPasswordToggle" type="button" class="absolute inset-y-0 right-3 my-auto grid h-9 w-9 place-items-center rounded-xl border border-white/15 bg-white/10 text-white/90">
              <i class="fa-regular fa-eye"></i>
            </button>
          </div>
        </label>

        <div id="profilePasswordStatus" class="min-h-5 text-sm text-white/75"></div>

        <div class="grid gap-2 sm:grid-cols-2">
          <button id="profilePasswordCancel" type="button" class="h-11 rounded-2xl border border-white/20 bg-white/10 text-sm font-semibold text-white">
            Annuler
          </button>
          <button id="profilePasswordSubmit" type="submit" class="h-11 rounded-2xl border border-[#ffb26e] bg-[#F57C00] text-sm font-semibold text-white shadow-[8px_8px_18px_rgba(163,82,27,0.45),-6px_-6px_14px_rgba(255,175,102,0.22)]">
            Mettre a jour
          </button>
        </div>
      </form>
    </div>
  `;

  document.body.appendChild(overlay);

  const form = overlay.querySelector("#profilePasswordForm");
  const closeBtn = overlay.querySelector("#profilePasswordClose");
  const cancelBtn = overlay.querySelector("#profilePasswordCancel");
  const submitBtn = overlay.querySelector("#profilePasswordSubmit");
  const statusEl = overlay.querySelector("#profilePasswordStatus");
  const currentInput = overlay.querySelector("#profileCurrentPassword");
  const nextInput = overlay.querySelector("#profileNewPassword");
  const confirmInput = overlay.querySelector("#profileConfirmPassword");

  const setStatus = (text = "", tone = "neutral") => {
    if (!statusEl) return;
    statusEl.textContent = String(text || "");
    statusEl.style.color = tone === "error"
      ? "#ffb0b0"
      : tone === "success"
        ? "#88f3ca"
        : "";
  };

  const close = () => {
    overlay.classList.add("hidden");
    overlay.classList.remove("flex");
    form?.reset();
    setStatus("");
    [currentInput, nextInput, confirmInput].forEach((input) => {
      if (input) input.type = "password";
    });
    [
      overlay.querySelector("#profileCurrentPasswordToggle"),
      overlay.querySelector("#profileNewPasswordToggle"),
      overlay.querySelector("#profileConfirmPasswordToggle"),
    ].forEach((button) => {
      const icon = button?.querySelector("i");
      if (button) {
        button.setAttribute("aria-label", "Afficher le mot de passe");
      }
      if (icon) {
        icon.classList.add("fa-eye");
        icon.classList.remove("fa-eye-slash");
      }
    });
  };

  const open = () => {
    overlay.classList.remove("hidden");
    overlay.classList.add("flex");
    setStatus("");
    window.setTimeout(() => currentInput?.focus(), 0);
  };

  bindPasswordVisibilityToggle(overlay.querySelector("#profileCurrentPasswordToggle"), currentInput);
  bindPasswordVisibilityToggle(overlay.querySelector("#profileNewPasswordToggle"), nextInput);
  bindPasswordVisibilityToggle(overlay.querySelector("#profileConfirmPasswordToggle"), confirmInput);

  const setBusy = (busy) => {
    const disabled = busy === true;
    [closeBtn, cancelBtn, submitBtn, currentInput, nextInput, confirmInput].forEach((el) => {
      if (el) el.disabled = disabled;
    });
    if (submitBtn) {
      submitBtn.textContent = disabled ? "Mise a jour..." : "Mettre a jour";
    }
  };

  closeBtn?.addEventListener("click", close);
  cancelBtn?.addEventListener("click", close);
  overlay.addEventListener("click", (event) => {
    if (event.target === overlay) {
      close();
    }
  });

  form?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const user = auth.currentUser;
    const email = String(user?.email || "").trim();
    const currentPassword = String(currentInput?.value || "");
    const nextPassword = String(nextInput?.value || "");
    const confirmPassword = String(confirmInput?.value || "");

    if (!user?.uid || !email) {
      setStatus("Reconnecte-toi puis reessaie.", "error");
      return;
    }
    if (!currentPassword) {
      setStatus("Entre ton mot de passe actuel.", "error");
      currentInput?.focus();
      return;
    }
    if (nextPassword.length < 6) {
      setStatus("Le nouveau mot de passe doit contenir au moins 6 caracteres.", "error");
      nextInput?.focus();
      return;
    }
    if (nextPassword !== confirmPassword) {
      setStatus("La confirmation du nouveau mot de passe ne correspond pas.", "error");
      confirmInput?.focus();
      return;
    }
    if (currentPassword === nextPassword) {
      setStatus("Choisis un nouveau mot de passe different de l'actuel.", "error");
      nextInput?.focus();
      return;
    }

    setBusy(true);
    setStatus("Verification du compte en cours...");

    try {
      const credential = EmailAuthProvider.credential(email, currentPassword);
      await reauthenticateWithCredential(user, credential);
      await updatePassword(user, nextPassword);
      setStatus("Mot de passe mis a jour avec succes.", "success");
      window.setTimeout(close, 900);
    } catch (error) {
      setStatus(formatProfilePasswordError(error), "error");
    } finally {
      setBusy(false);
    }
  });

  overlay.__openPasswordModal = open;
  overlay.__closePasswordModal = close;
  return overlay;
}

function refreshProfilePendingOperationsModal() {
  const listEl = document.getElementById("profilePendingOpsList");
  const countEl = document.getElementById("profilePendingOpsCount");
  const hintEl = document.getElementById("profilePendingOpsHint");
  if (!listEl) return;

  const operations = renderPendingOperationsList(listEl, {
    emptyText: "Aucune opération en cours sur ton compte.",
  });
  bindPendingOperationsActions(listEl);

  const count = Array.isArray(operations) ? operations.length : getPendingOperations().length;
  if (countEl) {
    countEl.textContent = `${count} opération${count > 1 ? "s" : ""}`;
  }
  if (hintEl) {
    hintEl.textContent = count > 0
      ? "Les dépôts et retraits non finalisés restent visibles ici jusqu'à leur résolution."
      : "Tu n'as actuellement aucun dépôt ou retrait en attente.";
  }
}

function applyWithdrawalCancelledFundingSnapshot(detail = {}) {
  const fundingSnapshot = detail?.fundingSnapshot;
  if (WITHDRAWAL_CANCEL_DEBUG) {
    console.log("[WITHDRAWAL_CANCEL_DEBUG][PROFILE] event received", {
      uid: String(auth.currentUser?.uid || ""),
      id: String(detail?.id || ""),
      status: String(detail?.status || ""),
      fundingSnapshot,
      previousFunding: latestProfileFundingData,
    });
  }
  if (fundingSnapshot && typeof fundingSnapshot === "object") {
    latestProfileFundingData = {
      ...(latestProfileFundingData || {}),
      ...fundingSnapshot,
    };
    if (WITHDRAWAL_CANCEL_DEBUG) {
      console.log("[WITHDRAWAL_CANCEL_DEBUG][PROFILE] funding merged", {
        uid: String(auth.currentUser?.uid || ""),
        mergedFunding: latestProfileFundingData,
      });
    }
  }
  if (WITHDRAWAL_CANCEL_DEBUG && !(fundingSnapshot && typeof fundingSnapshot === "object")) {
    console.log("[WITHDRAWAL_CANCEL_DEBUG][PROFILE] missing funding snapshot", {
      uid: String(auth.currentUser?.uid || ""),
      detail,
    });
  }
  scheduleProfileFundingRefresh(auth.currentUser, 80);
  updateProfileData(auth.currentUser);
}

function maybeShowWithdrawalHoldModal(user, payload = {}) {
  const uid = String(user?.uid || auth.currentUser?.uid || "").trim();
  if (!uid || payload.withdrawalHold !== true) return;

  const signature = `${uid}:${safeCount(payload.withdrawalHoldAtMs)}:${safeCount(payload.rejectedDepositStrikeCount)}`;
  if (!signature || signature === lastWithdrawalHoldSignature) return;
  lastWithdrawalHoldSignature = signature;

  const storageKey = `withdrawalHoldSeen:${signature}`;
  try {
    if (window.localStorage?.getItem(storageKey) === "1") return;
    window.localStorage?.setItem(storageKey, "1");
  } catch (_) {}

  const overlay = ensureWithdrawalHoldModal();
  const messageEl = overlay.querySelector("#profileWithdrawalHoldMessage");
  const detailsEl = overlay.querySelector("#profileWithdrawalHoldDetails");
  if (messageEl) {
    messageEl.textContent = "Ton compte est gelé pour les retraits après 3 demandes rejetées. Si tu penses que ce n'est pas vrai ou si tu veux plaider ta cause, contacte l'assistance.";
  }
  if (detailsEl) {
    const rejects = safeCount(payload.rejectedDepositStrikeCount);
    detailsEl.textContent = `Rejets enregistrés: ${rejects}/3. Dépôt, Xchange et parties restent actifs. Seuls les retraits sont bloqués.`;
  }
  overlay.classList.remove("hidden");
  overlay.classList.add("flex");
}

function clearProfileRealtimeWatchers() {
  profileRealtimeUid = "";
  latestProfileClientData = null;
  latestProfileFundingData = null;
  profileFundingUid = "";
  profileFundingRequestToken += 1;
  if (profileFundingRefreshTimer) {
    clearTimeout(profileFundingRefreshTimer);
    profileFundingRefreshTimer = null;
  }
  if (profileClientPollTimer) {
    clearInterval(profileClientPollTimer);
    profileClientPollTimer = null;
  }
  lastWithdrawalHoldSignature = "";
}

function scheduleProfileRealtimeRefresh(user) {
  if (profileRealtimeRefreshTimer) {
    clearTimeout(profileRealtimeRefreshTimer);
    profileRealtimeRefreshTimer = null;
  }
  profileRealtimeRefreshTimer = setTimeout(() => {
    profileRealtimeRefreshTimer = null;
    updateProfileData(user || auth.currentUser || null);
  }, 120);
}

function ensureProfileRealtimeWatchers(user) {
  const uid = String(user?.uid || "");
  if (!uid) {
    clearProfileRealtimeWatchers();
    return;
  }
  if (profileRealtimeUid === uid && profileClientPollTimer) {
    return;
  }

  clearProfileRealtimeWatchers();
  profileRealtimeUid = uid;
  const refreshClientSnapshot = async () => {
    const targetUid = String((user || auth.currentUser)?.uid || "");
    if (!targetUid || targetUid !== profileRealtimeUid) return;
    try {
      const snap = await getDoc(doc(db, "clients", targetUid));
      latestProfileClientData = snap.exists() ? (snap.data() || {}) : null;
      if (BALANCE_DEBUG) {
        console.log("[BALANCE_DEBUG][PROFILE] client snapshot", {
          uid: targetUid,
          exists: snap.exists(),
          data: latestProfileClientData,
        });
      }
      scheduleProfileRealtimeRefresh(user || auth.currentUser || null);
    } catch (err) {
      console.error("Erreur refresh profil client:", err);
    }
  };

  void refreshClientSnapshot();
  profileClientPollTimer = setInterval(() => {
    if (document.visibilityState !== "visible") return;
    void refreshClientSnapshot();
  }, PROFILE_CLIENT_REFRESH_MS);
}

function bindProfileVisibilityRefresh() {
  if (profileVisibilityBound) return;
  profileVisibilityBound = true;
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState !== "visible") return;
    const activeUser = auth.currentUser || null;
    ensureProfileRealtimeWatchers(activeUser);
    scheduleProfileFundingRefresh(activeUser, 0);
  });
}

function bindProfileWithdrawalEvents() {
  if (profileWithdrawalEventsBound) return;
  profileWithdrawalEventsBound = true;
  window.addEventListener("withdrawalCancelled", (event) => {
    applyWithdrawalCancelledFundingSnapshot(event?.detail || {});
  });
}

async function refreshProfileFundingStatus(user) {
  const uid = String(user?.uid || auth.currentUser?.uid || "").trim();
  if (!uid) {
    latestProfileFundingData = null;
    profileFundingUid = "";
    scheduleProfileRealtimeRefresh(null);
    return;
  }

  profileFundingUid = uid;
  const token = ++profileFundingRequestToken;
  try {
    if (WITHDRAWAL_CANCEL_DEBUG) {
      console.log("[WITHDRAWAL_CANCEL_DEBUG][PROFILE] funding refresh:start", {
        uid,
        token,
        currentFunding: latestProfileFundingData,
      });
    }
    if (BALANCE_DEBUG) {
      console.log("[BALANCE_DEBUG][PROFILE] funding status request", {
        uid,
        token,
      });
    }
    const result = await getDepositFundingStatusSecure();
    if (token !== profileFundingRequestToken) return;
    if (uid !== String(auth.currentUser?.uid || "").trim()) return;
    latestProfileFundingData = result && typeof result === "object" ? result : null;
    if (WITHDRAWAL_CANCEL_DEBUG) {
      console.log("[WITHDRAWAL_CANCEL_DEBUG][PROFILE] funding refresh:result", {
        uid,
        token,
        result: latestProfileFundingData,
      });
    }
    if (BALANCE_DEBUG) {
      console.log("[BALANCE_DEBUG][PROFILE] funding status", {
        uid,
        raw: latestProfileFundingData,
        approvedHtgAvailable: latestProfileFundingData?.approvedHtgAvailable,
        provisionalHtgAvailable: latestProfileFundingData?.provisionalHtgAvailable,
        withdrawableHtg: latestProfileFundingData?.withdrawableHtg,
        approvedDoesBalance: latestProfileFundingData?.approvedDoesBalance,
        provisionalDoesBalance: latestProfileFundingData?.provisionalDoesBalance,
        doesBalance: latestProfileFundingData?.doesBalance,
      });
    }
    if (WELCOME_PROGRESS_DEBUG) {
      console.log("[WELCOME_PROGRESS_DEBUG][PROFILE] funding snapshot", {
        uid,
        approvedDoesBalance: latestProfileFundingData?.approvedDoesBalance,
        exchangeableDoesAvailable: latestProfileFundingData?.exchangeableDoesAvailable,
        pendingPlayFromWelcomeDoes: latestProfileFundingData?.pendingPlayFromWelcomeDoes,
        welcomeBonusHtgConverted: latestProfileFundingData?.welcomeBonusHtgConverted,
        welcomeBonusHtgPlayed: latestProfileFundingData?.welcomeBonusHtgPlayed,
        hasRealApprovedDeposit: latestProfileFundingData?.hasRealApprovedDeposit === true,
      });
    }
  } catch (error) {
    console.warn("[PROFILE] funding status unavailable", error);
    if (token !== profileFundingRequestToken) return;
  }
  scheduleProfileRealtimeRefresh(user || auth.currentUser || null);
}

function scheduleProfileFundingRefresh(user, delayMs = 120) {
  const uid = String(user?.uid || auth.currentUser?.uid || "").trim();
  if (!uid) {
    latestProfileFundingData = null;
    profileFundingUid = "";
    if (profileFundingRefreshTimer) {
      clearTimeout(profileFundingRefreshTimer);
      profileFundingRefreshTimer = null;
    }
    return;
  }
  profileFundingUid = uid;
  if (profileFundingRefreshTimer) {
    clearTimeout(profileFundingRefreshTimer);
  }
  profileFundingRefreshTimer = setTimeout(() => {
    profileFundingRefreshTimer = null;
    void refreshProfileFundingStatus(user || auth.currentUser || null);
  }, Math.max(0, Number(delayMs) || 0));
}

function ensureProfileModal() {
  const existing = document.getElementById("profileModalOverlay");
  if (existing) return existing;

  const overlay = document.createElement("div");
  overlay.id = "profileModalOverlay";
  overlay.className = "fixed inset-0 z-[3000] hidden items-center justify-center bg-black/45 p-3 backdrop-blur-sm lg:items-stretch lg:justify-end lg:p-0";

  overlay.innerHTML = `
    <aside id="profileModalPanel" class="relative h-[88vh] w-[92vw] overflow-y-auto overscroll-contain rounded-3xl border border-white/20 bg-[#3F4766]/45 shadow-[14px_14px_34px_rgba(12,16,28,0.45),-10px_-10px_24px_rgba(98,113,151,0.18)] backdrop-blur-xl lg:h-screen lg:w-[50vw] lg:rounded-none lg:rounded-l-3xl" style="-webkit-overflow-scrolling: touch;">
      <div class="absolute inset-0 bg-gradient-to-b from-white/10 to-transparent"></div>
      <div class="relative flex h-full flex-col p-4 sm:p-6 lg:p-8">
        <div class="flex min-w-0 items-center justify-between gap-3">
          <div>
            <p class="text-xs uppercase tracking-[0.16em] text-white/70">Profile</p>
            <h2 class="mt-1 text-2xl font-bold text-white sm:text-3xl">Mon compte</h2>
          </div>
          <button id="profileModalClose" type="button" class="grid h-11 w-11 place-items-center rounded-full border border-white/20 bg-white/10 text-white shadow-[7px_7px_16px_rgba(18,24,39,0.35),-5px_-5px_12px_rgba(124,138,176,0.2)] transition hover:bg-white/15" aria-label="Close profile">
            <i class="fa-solid fa-xmark text-lg"></i>
          </button>
        </div>

        <div class="mt-6 grid grid-cols-1 gap-3 sm:grid-cols-2">
          <div class="rounded-2xl border border-white/20 bg-white/10 p-4 shadow-[8px_8px_18px_rgba(19,25,40,0.34),-6px_-6px_14px_rgba(111,126,164,0.2)]">
            <p class="text-[11px] uppercase tracking-[0.14em] text-white/65">Solde jouable</p>
            <p id="profileBalance" class="mt-2 text-sm text-white">-</p>
          </div>
          <div class="rounded-2xl border border-white/20 bg-white/10 p-4 shadow-[8px_8px_18px_rgba(19,25,40,0.34),-6px_-6px_14px_rgba(111,126,164,0.2)]">
            <button id="profileDepositBtn" type="button" class="inline-flex w-full min-w-0 items-center justify-between gap-2 rounded-xl border border-white/20 bg-white/10 px-3 py-2 text-sm font-semibold text-white shadow-[8px_8px_18px_rgba(19,25,40,0.34),-6px_-6px_14px_rgba(111,126,164,0.2)]">
              <span class="inline-flex min-w-0 flex-1 items-center gap-2">
                <i class="fa-solid fa-plus text-[11px]"></i>
                Faire un dépôt
              </span>
              <i class="fa-solid fa-wallet shrink-0 text-xs text-white/80"></i>
            </button>
            <button id="profileXchangeBtn" type="button" class="mt-2 inline-flex w-full min-w-0 items-center justify-between gap-2 rounded-xl border border-white/20 bg-white/10 px-3 py-2 text-sm font-semibold text-white shadow-[8px_8px_18px_rgba(19,25,40,0.34),-6px_-6px_14px_rgba(111,126,164,0.2)]">
              <span class="inline-flex min-w-0 flex-1 flex-wrap items-center gap-2">
                <img src="./does.png" alt="Does" class="h-4 w-4 rounded-full object-cover" data-hide-on-error="1" />
                Xchange en crypto
              </span>
              <i class="fa-solid fa-coins shrink-0 text-xs text-white/80"></i>
            </button>
            <button id="profileWithdrawBtn" type="button" class="mt-2 inline-flex w-full min-w-0 items-center justify-between gap-2 rounded-xl border border-white/20 bg-white/10 px-3 py-2 text-sm font-semibold text-white shadow-[8px_8px_18px_rgba(19,25,40,0.34),-6px_-6px_14px_rgba(111,126,164,0.2)]">
              <span class="inline-flex min-w-0 flex-1 items-center gap-2">
                <i class="fa-solid fa-arrow-up-right-from-square text-[11px]"></i>
                Faire un retrait
              </span>
              <i class="fa-solid fa-money-bill-transfer shrink-0 text-xs text-white/80"></i>
            </button>
            <button id="profilePasswordBtn" type="button" class="mt-2 inline-flex w-full min-w-0 items-center justify-between gap-2 rounded-xl border border-white/20 bg-white/10 px-3 py-2 text-sm font-semibold text-white shadow-[8px_8px_18px_rgba(19,25,40,0.34),-6px_-6px_14px_rgba(111,126,164,0.2)]">
              <span class="inline-flex min-w-0 flex-1 items-center gap-2">
                <i class="fa-solid fa-key text-[11px]"></i>
                Changer mot de passe
              </span>
              <i class="fa-solid fa-shield-halved shrink-0 text-xs text-white/80"></i>
            </button>
          </div>
        </div>

        <div class="mt-4 rounded-2xl border border-white/20 bg-white/10 p-4 shadow-[inset_6px_6px_14px_rgba(18,24,38,0.34),inset_-6px_-6px_14px_rgba(110,124,163,0.18)] backdrop-blur-md sm:p-5">
          <div class="flex min-w-0 items-center gap-3 sm:gap-4">
            <div class="grid h-16 w-16 shrink-0 place-items-center rounded-2xl border border-white/20 bg-white/10 text-white shadow-[8px_8px_18px_rgba(20,27,44,0.38),-6px_-6px_14px_rgba(120,133,172,0.2)]">
              <i class="fa-regular fa-circle-user text-3xl"></i>
            </div>
            <div class="min-w-0 flex-1">
              <p id="profileName" class="truncate text-lg font-semibold text-white">Player</p>
              <p id="profileEmail" class="mt-0.5 truncate text-sm text-white/75">-</p>
            </div>
          </div>
        </div>

        <div class="mt-4 rounded-2xl border border-white/20 bg-white/10 p-4 shadow-[8px_8px_18px_rgba(19,25,40,0.34),-6px_-6px_14px_rgba(111,126,164,0.2)]">
          <div class="flex min-w-0 flex-col items-start gap-3 sm:flex-row sm:items-center sm:justify-between">
            <p class="text-[11px] uppercase tracking-[0.14em] text-white/65">Parrainage</p>
            <button id="profileCopyReferralCode" type="button" class="w-full rounded-xl border border-white/20 bg-white/10 px-3 py-1.5 text-[11px] font-semibold text-white/90 sm:w-auto">
              Copier code
            </button>
          </div>

          <div class="mt-2 flex min-w-0 flex-col items-start gap-2 rounded-xl border border-white/15 bg-white/10 px-3 py-2 sm:flex-row sm:items-center sm:justify-between">
            <p class="w-full min-w-0 break-all text-sm text-white/85">Code: <span id="profileReferralCode" class="font-semibold text-white">-</span></p>
            <button id="profileCopyReferralLink" type="button" class="w-full rounded-lg border border-white/20 bg-white/10 px-2.5 py-1.5 text-[11px] font-semibold text-white/90 sm:w-auto">
              Copier lien
            </button>
          </div>

          <p id="profileReferralHint" class="mt-2 text-xs text-white/70">Partage ton code ou ton lien pour parrainer.</p>

          <div class="mt-3 grid grid-cols-1 gap-2 sm:grid-cols-2">
            <div class="rounded-xl border border-white/15 bg-white/10 p-3">
              <p class="text-[11px] uppercase tracking-[0.12em] text-white/60">Inscriptions</p>
              <p id="profileReferralSignups" class="mt-1 text-lg font-semibold text-white">0</p>
            </div>
            <div class="rounded-xl border border-white/15 bg-white/10 p-3">
              <p class="text-[11px] uppercase tracking-[0.12em] text-white/60">Dépôts</p>
              <p id="profileReferralDeposits" class="mt-1 text-lg font-semibold text-white">0</p>
            </div>
          </div>
          <button id="profileReferralRulesBtn" type="button" class="mt-3 w-full rounded-xl border border-white/20 bg-white/10 px-3 py-2 text-xs font-semibold tracking-wide text-white/90 transition hover:bg-white/15">
            Règles parrainage
          </button>
        </div>

        <div class="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
          <div class="rounded-2xl border border-white/20 bg-white/10 p-4 shadow-[8px_8px_18px_rgba(19,25,40,0.34),-6px_-6px_14px_rgba(111,126,164,0.2)] sm:col-span-2 xl:col-span-3">
            <div class="flex min-w-0 flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
              <div class="min-w-0">
                <p class="text-[11px] uppercase tracking-[0.14em] text-white/65">Statut compte</p>
                <p id="profileAccountStatusValue" class="mt-2 text-sm font-semibold text-white">Actif</p>
              </div>
              <span id="profileAccountStatusBadge" class="inline-flex w-fit items-center rounded-full border border-emerald-400/20 bg-emerald-500/15 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.14em] text-emerald-200">
                Actif
              </span>
            </div>
            <p id="profileAccountStatusStrike" class="mt-3 text-sm text-white/84">Rejets: 0/3</p>
            <p id="profileAccountStatusMeta" class="mt-1 text-xs text-white/62">Encore 3 rejets avant gel du retrait.</p>
          </div>
          <div class="rounded-2xl border border-white/20 bg-white/10 p-4 shadow-[8px_8px_18px_rgba(19,25,40,0.34),-6px_-6px_14px_rgba(111,126,164,0.2)]">
            <p class="text-[11px] uppercase tracking-[0.14em] text-white/65">HTG approuvé</p>
            <p id="profileApprovedHtg" class="mt-2 text-sm font-semibold text-white">-</p>
            <p class="mt-1 text-xs text-white/70">Partie validée qui reste encore en HTG.</p>
          </div>
          <div class="rounded-2xl border border-white/20 bg-white/10 p-4 shadow-[8px_8px_18px_rgba(19,25,40,0.34),-6px_-6px_14px_rgba(111,126,164,0.2)]">
            <p class="text-[11px] uppercase tracking-[0.14em] text-white/65">HTG en examen</p>
            <p id="profileProvisionalHtg" class="mt-2 text-sm font-semibold text-white">-</p>
            <p class="mt-1 text-xs text-white/70">Jouable, mais pas retirable tant que non validé.</p>
          </div>
          <div class="rounded-2xl border border-white/20 bg-white/10 p-4 shadow-[8px_8px_18px_rgba(19,25,40,0.34),-6px_-6px_14px_rgba(111,126,164,0.2)]">
            <p class="text-[11px] uppercase tracking-[0.14em] text-white/65">HTG dispo retrait</p>
            <p id="profileWithdrawAvailable" class="mt-2 text-sm font-semibold text-white">-</p>
            <p class="mt-1 text-xs text-white/70">Montant que tu peux demander en retrait maintenant.</p>
          </div>
          <div class="rounded-2xl border border-white/20 bg-white/10 p-4 shadow-[8px_8px_18px_rgba(19,25,40,0.34),-6px_-6px_14px_rgba(111,126,164,0.2)]">
            <p class="text-[11px] uppercase tracking-[0.14em] text-white/65">Does approuvés</p>
            <p class="mt-2 text-sm font-semibold text-white"><span id="profileApprovedDoes">0</span> Does</p>
            <p class="mt-1 text-xs text-white/70">Does venant d'un dépôt déjà validé.</p>
          </div>
          <div class="rounded-2xl border border-white/20 bg-white/10 p-4 shadow-[8px_8px_18px_rgba(19,25,40,0.34),-6px_-6px_14px_rgba(111,126,164,0.2)]">
            <p class="text-[11px] uppercase tracking-[0.14em] text-white/65">Does en examen</p>
            <p class="mt-2 text-sm font-semibold text-white"><span id="profileProvisionalDoes">0</span> Does</p>
            <p class="mt-1 text-xs text-white/70">Does venant d'un dépôt pas encore validé.</p>
          </div>
          <div class="rounded-2xl border border-white/20 bg-white/10 p-4 shadow-[8px_8px_18px_rgba(19,25,40,0.34),-6px_-6px_14px_rgba(111,126,164,0.2)]">
            <p class="text-[11px] uppercase tracking-[0.14em] text-white/65">Does dispo échange</p>
            <p class="mt-2 text-sm font-semibold text-white"><span id="profileExchangeableDoesAvailable">0</span> Does</p>
            <p class="mt-1 text-xs text-white/70">Does approuvés que tu peux reconvertir.</p>
          </div>
          <div id="profileLockedWelcomeDoesCard" class="hidden rounded-2xl border border-[#f6c177]/35 bg-[#3b2a16]/72 p-4 shadow-[8px_8px_18px_rgba(19,25,40,0.34),-6px_-6px_14px_rgba(111,126,164,0.2)]">
            <p class="text-[11px] uppercase tracking-[0.14em] text-[#ffd89a]">Does gelés en attente de dépôt</p>
            <p class="mt-2 text-sm font-semibold text-white"><span id="profileLockedWelcomeDoes">0</span> Does</p>
            <p class="mt-1 text-xs text-[#f7e8c7]">Ces Does viennent du bonus bienvenue. Ils seront débloqués après l'approbation de ton premier dépôt réel.</p>
          </div>
          <div class="rounded-2xl border border-white/20 bg-white/10 p-4 shadow-[8px_8px_18px_rgba(19,25,40,0.34),-6px_-6px_14px_rgba(111,126,164,0.2)] sm:col-span-2 xl:col-span-3">
            <p id="profileApprovedDepositsSummary" class="text-xs text-white/70">Dépôts approuvés: 0 HTG</p>
            <p id="profileExchanged" class="mt-1 text-xs text-white/70">Déjà converti: 0 HTG</p>
            <p id="profileVerifiedAvailableHint" class="mt-1 text-xs text-white/70">HTG vérifié dispo: 0 HTG</p>
            <p id="profilePendingBalanceHint" class="mt-1 text-xs text-white/70">HTG en examen: 0 HTG</p>
            <p id="profileDoesBreakdown" class="mt-1 text-xs text-white/70">Does approuvés: 0 | Does en examen: 0</p>
          </div>
        </div>

        <div class="mt-auto pt-6">
          <button id="profileLogoutBtn" type="button" class="h-12 w-full rounded-2xl border border-[#ffb26e] bg-[#F57C00] text-sm font-semibold tracking-wide text-white shadow-[8px_8px_18px_rgba(163,82,27,0.45),-6px_-6px_14px_rgba(255,175,102,0.22)] transition hover:-translate-y-0.5">
            Déconnexion
          </button>
        </div>
      </div>
    </aside>

    <div id="profileReferralRulesOverlay" class="fixed inset-0 z-[3050] hidden items-center justify-center bg-black/55 p-4 backdrop-blur-sm">
      <div id="profileReferralRulesPanel" class="w-full max-w-lg rounded-3xl border border-white/20 bg-[#3F4766]/80 p-5 text-white shadow-[14px_14px_34px_rgba(16,23,40,0.5),-10px_-10px_24px_rgba(112,126,165,0.2)] backdrop-blur-xl sm:p-6">
        <div class="flex min-w-0 items-center justify-between gap-3">
          <h3 class="text-lg font-bold">Règles parrainage</h3>
          <button id="profileReferralRulesClose" type="button" class="grid h-9 w-9 place-items-center rounded-full border border-white/20 bg-white/10 text-white">
            <i class="fa-solid fa-xmark"></i>
          </button>
        </div>

        <div class="mt-4 space-y-2 text-sm text-white/90">
          <p>1. Partage ton lien ou ton code promo avec tes amis.</p>
          <p>2. Ton ami crée son compte avec ton lien ou ton code.</p>
          <p>3. Tu reçois un bonus uniquement sur son premier dépôt approuvé.</p>
          <p>4. Bonus: <span class="font-semibold text-white">4 Does par 1 HTG déposé</span>.</p>
          <p>Exemples: 25 HTG = 100 Does, 50 HTG = 200 Does, 100 HTG = 400 Does.</p>
          <p>Le bonus n'est versé qu'une seule fois par filleul (premier dépôt seulement).</p>
        </div>
      </div>
    </div>
  `;

  document.body.appendChild(overlay);
  bindHideOnErrorImages(overlay);

  const closeBtn = overlay.querySelector("#profileModalClose");
  const panel = overlay.querySelector("#profileModalPanel");
  const logoutBtn = overlay.querySelector("#profileLogoutBtn");
  const rulesBtn = overlay.querySelector("#profileReferralRulesBtn");
  const rulesOverlay = overlay.querySelector("#profileReferralRulesOverlay");
  const rulesPanel = overlay.querySelector("#profileReferralRulesPanel");
  const rulesClose = overlay.querySelector("#profileReferralRulesClose");

  const closeRulesModal = () => {
    if (!rulesOverlay) return;
    rulesOverlay.classList.add("hidden");
    rulesOverlay.classList.remove("flex");
  };

  const openRulesModal = () => {
    if (!rulesOverlay) return;
    rulesOverlay.classList.remove("hidden");
    rulesOverlay.classList.add("flex");
  };

  const closeModal = () => {
    closeRulesModal();
    overlay.classList.add("hidden");
    overlay.classList.remove("flex");
  };

  const openModal = () => {
    overlay.classList.remove("hidden");
    overlay.classList.add("flex");
    if (panel) panel.scrollTop = 0;
  };

  overlay.addEventListener("click", (ev) => {
    if (ev.target === overlay) closeModal();
  });

  if (panel) {
    panel.addEventListener("click", (ev) => ev.stopPropagation());
  }

  if (rulesPanel) {
    rulesPanel.addEventListener("click", (ev) => ev.stopPropagation());
  }

  if (rulesOverlay) {
    rulesOverlay.addEventListener("click", (ev) => {
      if (ev.target === rulesOverlay) closeRulesModal();
    });
  }

  if (rulesClose) {
    rulesClose.addEventListener("click", closeRulesModal);
  }

  if (rulesBtn && rulesBtn.dataset.bound !== "1") {
    rulesBtn.dataset.bound = "1";
    rulesBtn.addEventListener("click", openRulesModal);
  }

  if (closeBtn) closeBtn.addEventListener("click", closeModal);

  if (logoutBtn) {
    logoutBtn.addEventListener("click", async () => {
      try {
        await logoutCurrentUser();
        closeModal();
      } catch (err) {
        console.error("Logout error:", err);
      }
    });
  }

  const copyReferralCodeBtn = overlay.querySelector("#profileCopyReferralCode");
  const copyReferralLinkBtn = overlay.querySelector("#profileCopyReferralLink");

  const copyToClipboard = async (text) => {
    const value = String(text || "").trim();
    if (!value || value === "-") return false;
    try {
      if (navigator.clipboard && navigator.clipboard.writeText) {
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
  };

  if (copyReferralCodeBtn && copyReferralCodeBtn.dataset.bound !== "1") {
    copyReferralCodeBtn.dataset.bound = "1";
    copyReferralCodeBtn.addEventListener("click", async () => {
      const code = document.getElementById("profileReferralCode")?.textContent || "";
      const ok = await copyToClipboard(code);
      showReferralCopyFeedback(ok ? "Code copié avec succès." : "Impossible de copier le code.", ok);
    });
  }

  if (copyReferralLinkBtn && copyReferralLinkBtn.dataset.bound !== "1") {
    copyReferralLinkBtn.dataset.bound = "1";
    copyReferralLinkBtn.addEventListener("click", async () => {
      const link = copyReferralLinkBtn.getAttribute("data-link") || "";
      const ok = await copyToClipboard(link);
      showReferralCopyFeedback(ok ? "Lien copié avec succès." : "Impossible de copier le lien.", ok);
    });
  }

  overlay.__openModal = openModal;
  overlay.__closeModal = closeModal;

  return overlay;
}

function showReferralCopyFeedback(message, success = true) {
  const hintEl = document.getElementById("profileReferralHint");
  if (!hintEl) return;

  referralHintFreezeUntil = Date.now() + 1800;
  hintEl.textContent = String(message || "");
  hintEl.style.color = success ? "#86efac" : "#fecaca";

  if (referralHintRestoreTimer) {
    clearTimeout(referralHintRestoreTimer);
    referralHintRestoreTimer = null;
  }

  referralHintRestoreTimer = setTimeout(() => {
    referralHintRestoreTimer = null;
    if (Date.now() < referralHintFreezeUntil) return;
    hintEl.style.color = "";
    updateReferralData(auth.currentUser);
  }, 1900);
}

async function updateWithdrawalAvailability(user, xState) {
  const htgEl = document.getElementById("profileWithdrawAvailable");
  const hintEl = document.getElementById("profileWithdrawRuleHint");
  const metaEl = document.getElementById("profileWithdrawRuleMeta");
  if (!htgEl && !hintEl && !metaEl) return;

  const uid = String(user?.uid || auth.currentUser?.uid || "").trim();
  const token = ++withdrawalAvailabilityToken;

  if (!uid) {
    if (htgEl) htgEl.textContent = "-";
    if (hintEl) hintEl.textContent = "Connecte-toi pour voir ton retrait disponible.";
    if (metaEl) metaEl.textContent = "";
    return;
  }

  if (hintEl) hintEl.textContent = "Vérification des règles de retrait...";
  if (metaEl) metaEl.textContent = "";

  try {
    const hydrated = await waitForBalanceHydration(uid, 2600);
    if (token !== withdrawalAvailabilityToken) return;
    if (BALANCE_DEBUG) {
      console.log("[BALANCE_DEBUG][PROFILE] withdrawal hydration", {
        uid,
        hydrated,
        __userBaseBalance: window.__userBaseBalance,
        __userBalance: window.__userBalance,
      });
    }

    const ruleStatus = await getWithdrawalRuleStatus(uid);
    if (token !== withdrawalAvailabilityToken) return;

    const withdrawableHtg = safeCount(
      ruleStatus.canWithdraw
        ? (typeof ruleStatus.withdrawableHtg === "number" ? ruleStatus.withdrawableHtg : Number(xState?.withdrawableHtg || xState?.availableGourdes || 0))
        : 0
    );
    if (htgEl) htgEl.textContent = formatAmount(withdrawableHtg);

    if (ruleStatus.accountFrozen) {
      if (hintEl) hintEl.textContent = "Compte gelé: dépôt, retrait, Xchange et parties sont bloqués.";
      if (metaEl) metaEl.textContent = "Contacte l'assistance pour demander un dégel.";
      return;
    }

    if (ruleStatus.withdrawalHold) {
      if (hintEl) hintEl.textContent = "Retrait gelé après 3 demandes rejetées.";
      if (metaEl) {
        metaEl.textContent = `Rejets: ${safeCount(ruleStatus.rejectedDepositStrikeCount)}/3 | Assistance: ${ASSISTANCE_PHONE}`;
      }
      return;
    }

    const provisionalLockedHtg = safeCount(ruleStatus.provisionalHtgAvailable);
    const welcomeBonusHtg = safeCount(ruleStatus.welcomeBonusHtgAvailable);
    if (ruleStatus.canWithdraw) {
      if (hintEl) {
        hintEl.textContent = provisionalLockedHtg > 0
          ? "Montant réellement retirable maintenant, hors dépôts encore en examen."
          : "Montant réellement retirable maintenant selon les règles actuelles.";
      }
      if (metaEl) {
        metaEl.textContent = provisionalLockedHtg > 0
          ? `Retirable: ${formatAmount(withdrawableHtg)} | En examen: ${formatAmount(provisionalLockedHtg)}${welcomeBonusHtg > 0 ? ` | Bonus bienvenue: ${formatAmount(welcomeBonusHtg)}` : ""}`
          : `Base retrait: ${formatAmount(withdrawableHtg)} | Taux: 1 HTG = ${Number(xState?.rate || 20)} Does${welcomeBonusHtg > 0 ? ` | Bonus bienvenue non inclus: ${formatAmount(welcomeBonusHtg)}` : ""}`;
      }
      return;
    }

    if (provisionalLockedHtg > 0 && safeCount(ruleStatus.remainingToExchangeHtg) <= 0) {
      if (hintEl) {
        hintEl.textContent = "Retrait partiellement bloqué: une partie de ton solde est encore en cours d'examen.";
      }
      if (metaEl) {
        metaEl.textContent = `En examen: ${formatAmount(provisionalLockedHtg)} | Retirable maintenant: ${formatAmount(withdrawableHtg)}${welcomeBonusHtg > 0 ? ` | Bonus bienvenue: ${formatAmount(welcomeBonusHtg)}` : ""}`;
      }
      return;
    }

    if (hintEl) {
      hintEl.textContent = welcomeBonusHtg > 0
        ? `Retrait bloque pour le moment. Ton bonus bienvenue reste jouable, mais n'entre pas encore dans le retrait normal.`
        : `Retrait bloqué pour le moment: il reste ${formatAmount(ruleStatus.remainingToExchangeHtg)} à convertir en Does.`;
    }
    if (metaEl) {
      metaEl.textContent = `Dépôts approuvés: ${formatAmount(ruleStatus.approvedDepositsHtg)} | Déjà converti: ${formatAmount(ruleStatus.convertedHtg)}${welcomeBonusHtg > 0 ? ` | Bonus bienvenue: ${formatAmount(welcomeBonusHtg)}` : ""}`;
    }
  } catch (error) {
    console.error("Erreur calcul disponibilité retrait profil:", error);
    if (token !== withdrawalAvailabilityToken) return;
    if (htgEl) htgEl.textContent = "-";
    if (hintEl) hintEl.textContent = "Impossible de vérifier la disponibilité du retrait.";
    if (metaEl) metaEl.textContent = "";
  }
}

function updateProfileData(user) {
  const nameEl = document.getElementById("profileName");
  const emailEl = document.getElementById("profileEmail");
  const balanceEl = document.getElementById("profileBalance");
  const totalHtgEl = document.getElementById("profileTotalHtg");
  const totalDoesEl = document.getElementById("profileTotalDoes");
  const approvedHtgEl = document.getElementById("profileApprovedHtg");
  const provisionalHtgEl = document.getElementById("profileProvisionalHtg");
  const approvedDoesEl = document.getElementById("profileApprovedDoes");
  const provisionalDoesEl = document.getElementById("profileProvisionalDoes");
  const htgApprovedBadgeEl = document.getElementById("profileHtgApprovedBadge");
  const htgPendingBadgeEl = document.getElementById("profileHtgPendingBadge");
  const htgBonusBadgeEl = document.getElementById("profileHtgBonusBadge");
  const htgSummaryEl = document.getElementById("profileHtgSummary");
  const doesApprovedBadgeEl = document.getElementById("profileDoesApprovedBadge");
  const doesPendingBadgeEl = document.getElementById("profileDoesPendingBadge");
  const doesLockedBadgeEl = document.getElementById("profileDoesLockedBadge");
  const doesSummaryEl = document.getElementById("profileDoesSummary");
  const approvedDepositsSummaryEl = document.getElementById("profileApprovedDepositsSummary");
  const exchangedEl = document.getElementById("profileExchanged");
  const verifiedAvailableHintEl = document.getElementById("profileVerifiedAvailableHint");
  const withdrawAvailableEl = document.getElementById("profileWithdrawAvailable");
  const exchangeableDoesEl = document.getElementById("profileExchangeableDoesAvailable");
  const exchangeableDoesLabelEl = document.getElementById("profileExchangeableDoesLabel");
  const exchangeableDoesHintEl = document.getElementById("profileExchangeableDoesHint");
  const lockedWelcomeDoesCardEl = document.getElementById("profileLockedWelcomeDoesCard");
  const lockedWelcomeDoesEl = document.getElementById("profileLockedWelcomeDoes");
  const pendingHintEl = document.getElementById("profilePendingBalanceHint");
  const doesBreakdownEl = document.getElementById("profileDoesBreakdown");
  const frozenBannerEl = document.getElementById("profileFrozenBanner");
  const frozenMessageEl = document.getElementById("profileFrozenMessage");
  const accountStatusValueEl = document.getElementById("profileAccountStatusValue");
  const accountStatusBadgeEl = document.getElementById("profileAccountStatusBadge");
  const accountStatusStrikeEl = document.getElementById("profileAccountStatusStrike");
  const accountStatusMetaEl = document.getElementById("profileAccountStatusMeta");
  const baseForUi = getBalanceBaseForUi();
  const xState = getXchangeState(baseForUi, user?.uid || auth.currentUser?.uid);
  const clientData = latestProfileClientData || {};
  const fundingData = latestProfileFundingData || {};
  if (WITHDRAWAL_CANCEL_DEBUG) {
    console.log("[WITHDRAWAL_CANCEL_DEBUG][PROFILE] updateProfileData:start", {
      uid: String(user?.uid || auth.currentUser?.uid || ""),
      baseForUi,
      fundingData,
      clientData,
      xState,
    });
  }
  const approvedHtgAvailable = safeCount(
    pickFirstFiniteNumber(
      fundingData.approvedHtgAvailable,
      xState?.approvedGourdesAvailable,
      clientData.approvedHtgAvailable
    )
  );
  const provisionalHtgAvailable = safeCount(
    pickFirstFiniteNumber(
      fundingData.provisionalHtgAvailable,
      xState?.provisionalGourdesAvailable,
      clientData.provisionalHtgAvailable
    )
  );
  const welcomeBonusHtgAvailable = safeCount(
    xState?.loaded === true
      ? pickFirstFiniteNumber(
        xState?.welcomeBonusHtgAvailable,
        fundingData.welcomeBonusHtgAvailable,
        clientData.welcomeBonusHtgAvailable
      )
      : pickFirstFiniteNumber(
        fundingData.welcomeBonusHtgAvailable,
        xState?.welcomeBonusHtgAvailable,
        clientData.welcomeBonusHtgAvailable
      )
  );
  const welcomeBonusHtgConverted = safeCount(
    xState?.loaded === true
      ? pickFirstFiniteNumber(
        xState?.welcomeBonusHtgConverted,
        fundingData.welcomeBonusHtgConverted,
        clientData.welcomeBonusHtgConverted
      )
      : pickFirstFiniteNumber(
        fundingData.welcomeBonusHtgConverted,
        xState?.welcomeBonusHtgConverted,
        clientData.welcomeBonusHtgConverted
      )
  );
  const welcomeBonusHtgPlayed = safeCount(
    xState?.loaded === true
      ? pickFirstFiniteNumber(
        xState?.welcomeBonusHtgPlayed,
        fundingData.welcomeBonusHtgPlayed,
        clientData.welcomeBonusHtgPlayed
      )
      : pickFirstFiniteNumber(
        fundingData.welcomeBonusHtgPlayed,
        xState?.welcomeBonusHtgPlayed,
        clientData.welcomeBonusHtgPlayed
      )
  );
  const doesApprovedBalance = safeCount(
    pickFirstFiniteNumber(
      fundingData.approvedDoesBalance,
      xState?.doesApprovedBalance,
      clientData.doesApprovedBalance
    )
  );
  const doesProvisionalBalance = safeCount(
    pickFirstFiniteNumber(
      fundingData.provisionalDoesBalance,
      xState?.doesProvisionalBalance,
      clientData.doesProvisionalBalance
    )
  );
  const exchangeableDoesAvailable = safeCount(
    pickFirstFiniteNumber(
      fundingData.exchangeableDoesAvailable,
      xState?.exchangeableDoesAvailable,
      clientData.exchangeableDoesAvailable
    )
  );
  const pendingPlayFromWelcomeDoes = safeCount(
    pickFirstFiniteNumber(
      fundingData.pendingPlayFromWelcomeDoes,
      xState?.pendingPlayFromWelcomeDoes,
      clientData.pendingPlayFromWelcomeDoes
    )
  );
  const hasRealApprovedDeposit = fundingData.hasRealApprovedDeposit === true
    || xState?.hasRealApprovedDeposit === true
    || clientData.hasApprovedDeposit === true;
  const requestedLockedWelcomeDoes = safeCount(
    readWelcomeLockedSellAttempt(String(user?.uid || auth.currentUser?.uid || ""))?.amountDoes
  );
  const lockedWelcomeDoes = hasRealApprovedDeposit
    ? 0
    : Math.min(
      requestedLockedWelcomeDoes,
      pendingPlayFromWelcomeDoes,
      safeCount(doesApprovedBalance)
    );
  const welcomeUnlockedByPlayDoes = hasRealApprovedDeposit
    ? 0
    : safeCount(Math.min(welcomeBonusHtgConverted, welcomeBonusHtgPlayed) * RATE_HTG_TO_DOES);
  const displayExchangeableDoes = !hasRealApprovedDeposit && welcomeBonusHtgConverted > 0
    ? welcomeUnlockedByPlayDoes
    : exchangeableDoesAvailable;
  if (WELCOME_PROGRESS_DEBUG) {
    console.log("[WELCOME_PROGRESS_DEBUG][PROFILE] computed display", {
      uid: user?.uid || auth.currentUser?.uid || null,
      doesApprovedBalance,
      exchangeableDoesAvailable,
      pendingPlayFromWelcomeDoes,
      welcomeBonusHtgConverted,
      welcomeBonusHtgPlayed,
      welcomeUnlockedByPlayDoes,
      displayExchangeableDoes,
      hasRealApprovedDeposit,
    });
  }
  const allowLegacyAvailableFallback = !latestProfileFundingData
    && safeCount(approvedHtgAvailable + provisionalHtgAvailable) <= 0
    && xState?.loaded !== true;
  const resolvedAvailableHtg = safeCount(
    xState?.loaded === true
      ? pickFirstFiniteNumber(
        xState.availableGourdes,
        fundingData.playableHtg,
        approvedHtgAvailable + provisionalHtgAvailable
      )
      : (
        allowLegacyAvailableFallback
          ? pickFirstFiniteNumber(
            fundingData.playableHtg,
            approvedHtgAvailable + provisionalHtgAvailable,
            xState.availableGourdes
          )
          : pickFirstFiniteNumber(
            fundingData.playableHtg,
            approvedHtgAvailable + provisionalHtgAvailable
          )
      )
  );
  const resolvedDoesBalance = safeCount(
    pickFirstFiniteNumber(
      fundingData.doesBalance,
      doesApprovedBalance + doesProvisionalBalance,
      xState.does
    )
  );
  const accountFrozen = fundingData.accountFrozen === true
    || clientData.accountFrozen === true
    || xState?.accountFrozen === true;
  const withdrawalHold = fundingData.withdrawalHold === true
    || clientData.withdrawalHold === true;
  const withdrawalLocked = withdrawalHold || accountFrozen;
  const rejectedDepositStrikeCount = safeCount(
    pickFirstFiniteNumber(
      fundingData.rejectedDepositStrikeCount,
      clientData.rejectedDepositStrikeCount
    )
  );
  const withdrawalHoldAtMs = safeCount(
    pickFirstFiniteNumber(
      fundingData.withdrawalHoldAtMs,
      clientData.withdrawalHoldAtMs
    )
  );
  const rejectsRemaining = Math.max(0, 3 - rejectedDepositStrikeCount);
  const approvedDepositsTotal = safeCount(
    pickFirstFiniteNumber(
      fundingData.approvedDepositsHtg,
      clientData.approvedDepositsHtg
    )
  );
  const convertedApprovedHtg = safeCount(
    pickFirstFiniteNumber(
      fundingData.totalExchangedApprovedHtg,
      xState?.totalExchangedHtgEver,
      clientData.totalExchangedHtgEver
    )
  );
  const resolvedXState = {
    ...xState,
    availableGourdes: resolvedAvailableHtg,
    approvedGourdesAvailable: approvedHtgAvailable,
    provisionalGourdesAvailable: provisionalHtgAvailable,
    does: resolvedDoesBalance,
    doesApprovedBalance,
    doesProvisionalBalance,
    exchangeableDoesAvailable,
    withdrawableHtg: safeCount(
      pickFirstFiniteNumber(
        fundingData.withdrawableHtg,
        xState?.withdrawableHtg,
        clientData.withdrawableHtg
      )
    ),
    accountFrozen,
    withdrawalHold: withdrawalLocked,
  };
  const totalVisibleHtg = safeCount(approvedHtgAvailable + provisionalHtgAvailable);
  const totalVisibleDoes = safeCount(resolvedDoesBalance);
  if (WITHDRAWAL_CANCEL_DEBUG) {
    console.log("[WITHDRAWAL_CANCEL_DEBUG][PROFILE] updateProfileData:resolved", {
      uid: String(user?.uid || auth.currentUser?.uid || ""),
      approvedHtgAvailable,
      provisionalHtgAvailable,
      resolvedAvailableHtg,
      withdrawableHtg: resolvedXState.withdrawableHtg,
      approvedDepositsTotal,
      fundingData,
    });
  }

  if (BALANCE_DEBUG) {
    console.log("[BALANCE_DEBUG][PROFILE] source comparison", {
      uid: user?.uid || auth.currentUser?.uid || null,
      clientData,
      fundingData,
      xState,
      resolvedXState,
    });
    console.log("[BALANCE_DEBUG][PROFILE] updateProfileData", {
      uid: user?.uid || auth.currentUser?.uid || null,
      baseForUi,
      __userBaseBalance: window.__userBaseBalance,
      __userBalance: window.__userBalance,
      availableFromXchange: xState.availableGourdes,
      availableResolved: resolvedAvailableHtg,
      approvedDepositsTotal,
      convertedApprovedHtg,
      approvedHtgAvailable,
      provisionalHtgAvailable,
      welcomeBonusHtgAvailable,
      welcomeBonusHtgConverted,
      welcomeBonusHtgPlayed,
      welcomeUnlockedByPlayDoes,
      exchanged: resolvedXState.exchangedGourdes,
      does: resolvedDoesBalance,
      doesApprovedBalance,
      doesProvisionalBalance,
      exchangeableDoesAvailable,
      displayExchangeableDoes,
      accountFrozen,
      withdrawalHold,
      withdrawalLocked,
      rejectedDepositStrikeCount,
    });
  }

  if (nameEl) {
    const displayResolution = resolveDisplayName(user, clientData);
    const displayName = displayResolution.value;
    nameEl.textContent = displayName;
    nameEl.title = displayName || "";
    if (BALANCE_DEBUG) {
      console.log("[PROFILE_DEBUG][DISPLAY_NAME] resolution", {
        uid: String(user?.uid || auth.currentUser?.uid || ""),
        chosenValue: displayResolution.value,
        source: displayResolution.source,
        authDisplayName: String(user?.displayName || ""),
        authEmail: String(user?.email || ""),
        clientUsername: String(clientData?.username || ""),
        clientPhone: String(clientData?.phone || ""),
        localHint: readAuthProfileHint(user),
        clientData,
      });
    }
  }
  if (emailEl) {
    const contact = getDisplayContact(user, clientData);
    emailEl.textContent = contact;
    emailEl.title = contact;
  }
  if (totalHtgEl) totalHtgEl.textContent = formatAmount(totalVisibleHtg);
  if (totalDoesEl) totalDoesEl.textContent = `${formatDoesAmount(totalVisibleDoes)} Does`;
  if (balanceEl) balanceEl.textContent = formatAmount(resolvedAvailableHtg);
  if (approvedHtgEl) approvedHtgEl.textContent = formatAmount(approvedHtgAvailable);
  if (provisionalHtgEl) provisionalHtgEl.textContent = formatAmount(provisionalHtgAvailable);
  if (approvedDoesEl) approvedDoesEl.textContent = formatDoesAmount(doesApprovedBalance);
  if (provisionalDoesEl) provisionalDoesEl.textContent = formatDoesAmount(doesProvisionalBalance);
  if (withdrawAvailableEl) withdrawAvailableEl.textContent = formatAmount(resolvedXState.withdrawableHtg);
  if (exchangeableDoesEl) exchangeableDoesEl.textContent = formatDoesAmount(displayExchangeableDoes);
  if (exchangeableDoesLabelEl) {
    exchangeableDoesLabelEl.textContent = !hasRealApprovedDeposit && welcomeBonusHtgConverted > 0
      ? "Does débloqués par jeu"
      : "Does dispo échange";
  }
  if (exchangeableDoesHintEl) {
    exchangeableDoesHintEl.textContent = !hasRealApprovedDeposit && welcomeBonusHtgConverted > 0
      ? "Part du bonus bienvenue déjà débloquée par tes parties. Elle reste gelée jusqu'au premier dépôt réel approuvé."
      : "Does approuvés que tu peux reconvertir.";
  }
  if (lockedWelcomeDoesEl) lockedWelcomeDoesEl.textContent = formatDoesAmount(lockedWelcomeDoes);
  if (lockedWelcomeDoesCardEl) lockedWelcomeDoesCardEl.classList.toggle("hidden", lockedWelcomeDoes <= 0);
  if (htgApprovedBadgeEl) {
    htgApprovedBadgeEl.textContent = `Approuve ${formatAmount(approvedHtgAvailable)}`;
  }
  if (htgPendingBadgeEl) {
    htgPendingBadgeEl.textContent = `En examen ${formatAmount(provisionalHtgAvailable)}`;
  }
  if (htgBonusBadgeEl) {
    htgBonusBadgeEl.textContent = `Bonus ${formatAmount(welcomeBonusHtgAvailable)}`;
    htgBonusBadgeEl.classList.toggle("hidden", welcomeBonusHtgAvailable <= 0);
    htgBonusBadgeEl.classList.toggle("inline-flex", welcomeBonusHtgAvailable > 0);
  }
  if (htgSummaryEl) {
    if (approvedHtgAvailable > 0 && provisionalHtgAvailable > 0) {
      htgSummaryEl.textContent = "Une partie de ton HTG est déjà utilisable. Le reste est encore en examen.";
    } else if (approvedHtgAvailable > 0) {
      htgSummaryEl.textContent = "Ton HTG visible est déjà approuvé et prêt à être utilisé.";
    } else if (provisionalHtgAvailable > 0) {
      htgSummaryEl.textContent = "Ton HTG visible est encore en examen avant validation.";
    } else if (welcomeBonusHtgAvailable > 0) {
      htgSummaryEl.textContent = "Tu n'as pas encore de HTG dépôt visible. Ton bonus bienvenue reste jouable séparément.";
    } else {
      htgSummaryEl.textContent = "Aucun HTG visible pour le moment sur ton compte.";
    }
  }
  if (doesApprovedBadgeEl) {
    doesApprovedBadgeEl.textContent = `Approuves ${formatDoesAmount(doesApprovedBalance)} Does`;
  }
  if (doesPendingBadgeEl) {
    doesPendingBadgeEl.textContent = `En examen ${formatDoesAmount(doesProvisionalBalance)} Does`;
  }
  if (doesLockedBadgeEl) {
    doesLockedBadgeEl.textContent = `Geles ${formatDoesAmount(lockedWelcomeDoes)} Does`;
    doesLockedBadgeEl.classList.toggle("hidden", lockedWelcomeDoes <= 0);
    doesLockedBadgeEl.classList.toggle("inline-flex", lockedWelcomeDoes > 0);
  }
  if (doesSummaryEl) {
    if (lockedWelcomeDoes > 0) {
      doesSummaryEl.textContent = "Une partie de tes Does reste gelée tant que ton premier dépôt réel n'est pas approuvé.";
    } else if (doesApprovedBalance > 0 && doesProvisionalBalance > 0) {
      doesSummaryEl.textContent = "Tu peux déjà jouer avec les Does approuvés. Le reste est encore en examen.";
    } else if (doesApprovedBalance > 0) {
      doesSummaryEl.textContent = "Tes Does visibles sont déjà approuvés et prêts pour le jeu.";
    } else if (doesProvisionalBalance > 0) {
      doesSummaryEl.textContent = "Tes Does visibles sont encore en examen avant validation.";
    } else {
      doesSummaryEl.textContent = "Aucun Does visible pour le moment sur ton compte.";
    }
  }
  if (approvedDepositsSummaryEl) approvedDepositsSummaryEl.textContent = `Dépôts approuvés: ${formatAmount(approvedDepositsTotal)}`;
  if (exchangedEl) exchangedEl.textContent = `Déjà converti: ${formatAmount(convertedApprovedHtg)}`;
  if (approvedDepositsSummaryEl && welcomeBonusHtgAvailable > 0) {
    approvedDepositsSummaryEl.textContent = `Dépôts approuvés: ${formatAmount(approvedDepositsTotal)} | Bonus bienvenue: ${formatAmount(welcomeBonusHtgAvailable)}`;
  }
  if (verifiedAvailableHintEl) {
    verifiedAvailableHintEl.textContent = welcomeBonusHtgAvailable > 0
      ? `HTG vérifié dispo: ${formatAmount(approvedHtgAvailable)} | Bonus bienvenue jouable: ${formatAmount(welcomeBonusHtgAvailable)}`
      : `HTG vérifié dispo: ${formatAmount(approvedHtgAvailable)}`;
  }
  if (pendingHintEl) {
    pendingHintEl.textContent = welcomeBonusHtgAvailable > 0
      ? `HTG en examen: ${formatAmount(provisionalHtgAvailable)} | Bonus bienvenue: ${formatAmount(welcomeBonusHtgAvailable)} | HTG dispo échange: ${formatAmount(resolvedAvailableHtg)}`
      : `HTG en examen: ${formatAmount(provisionalHtgAvailable)} | HTG dispo échange: ${formatAmount(resolvedAvailableHtg)}`;
  }
  if (doesBreakdownEl) {
    doesBreakdownEl.textContent = lockedWelcomeDoes > 0
      ? `Total Does: ${formatDoesAmount(resolvedDoesBalance)} | Approuvés: ${formatDoesAmount(doesApprovedBalance)} | En examen: ${formatDoesAmount(doesProvisionalBalance)} | Dispo échange: ${formatDoesAmount(displayExchangeableDoes)} | Gelés: ${formatDoesAmount(lockedWelcomeDoes)}`
      : `Total Does: ${formatDoesAmount(resolvedDoesBalance)} | Approuvés: ${formatDoesAmount(doesApprovedBalance)} | En examen: ${formatDoesAmount(doesProvisionalBalance)} | Dispo échange: ${formatDoesAmount(displayExchangeableDoes)}`;
  }
  if (frozenBannerEl) frozenBannerEl.classList.toggle("hidden", withdrawalLocked !== true);
  if (frozenMessageEl) {
    frozenMessageEl.textContent = accountFrozen
      ? "Ton compte a été temporairement gelé. Contacte l'assistance pour demander un dégel."
      : withdrawalHold
        ? "Ton compte est gelé pour les retraits après plusieurs dépôts refusés. Contacte l'assistance si tu penses que c'est une erreur."
        : "";
  }
  if (accountStatusValueEl) {
    accountStatusValueEl.textContent = accountFrozen ? "Gelé globalement" : withdrawalHold ? "Gelé pour retrait" : "Actif";
  }
  if (accountStatusBadgeEl) {
    accountStatusBadgeEl.textContent = withdrawalLocked ? "Gelé" : "Actif";
    accountStatusBadgeEl.classList.toggle("border-emerald-400/20", !withdrawalLocked);
    accountStatusBadgeEl.classList.toggle("bg-emerald-500/15", !withdrawalLocked);
    accountStatusBadgeEl.classList.toggle("text-emerald-200", !withdrawalLocked);
    accountStatusBadgeEl.classList.toggle("border-amber-300/25", withdrawalLocked);
    accountStatusBadgeEl.classList.toggle("bg-amber-500/15", withdrawalLocked);
    accountStatusBadgeEl.classList.toggle("text-amber-100", withdrawalLocked);
  }
  if (accountStatusStrikeEl) {
    accountStatusStrikeEl.textContent = `Rejets: ${rejectedDepositStrikeCount}/3`;
  }
  if (accountStatusMetaEl) {
    accountStatusMetaEl.textContent = accountFrozen
      ? "Dépôt, retrait, Xchange et parties sont bloqués."
      : withdrawalHold
        ? "Les retraits sont bloqués. Dépôt, Xchange et parties restent actifs."
        : `Encore ${rejectsRemaining} rejet${rejectsRemaining > 1 ? "s" : ""} avant gel du retrait.`;
  }

  ["profileDepositBtn", "profileXchangeBtn"].forEach((id) => {
    const btn = document.getElementById(id);
    if (!btn) return;
    btn.disabled = accountFrozen === true;
    btn.classList.toggle("opacity-60", accountFrozen === true);
    btn.classList.toggle("cursor-not-allowed", accountFrozen === true);
  });
  const withdrawBtn = document.getElementById("profileWithdrawBtn");
  if (withdrawBtn) {
    const withdrawDisabled = withdrawalLocked === true;
    withdrawBtn.disabled = withdrawDisabled;
    withdrawBtn.classList.toggle("opacity-60", withdrawDisabled);
    withdrawBtn.classList.toggle("cursor-not-allowed", withdrawDisabled);
  }
  maybeShowWithdrawalHoldModal(user, {
    withdrawalHold,
    withdrawalHoldAtMs,
    rejectedDepositStrikeCount,
  });
  void updateWithdrawalAvailability(user, resolvedXState);
  updateReferralData(user);
  updateAgentDashboardAccess(user);
}

function updateReferralData(user) {
  const codeEl = document.getElementById("profileReferralCode");
  const signupsEl = document.getElementById("profileReferralSignups");
  const depositsEl = document.getElementById("profileReferralDeposits");
  const hintEl = document.getElementById("profileReferralHint");
  const copyLinkBtn = document.getElementById("profileCopyReferralLink");

  const token = ++referralLoadToken;
  const hintLocked = Date.now() < referralHintFreezeUntil;

  if (!user?.uid) {
    if (codeEl) codeEl.textContent = "-";
    if (signupsEl) signupsEl.textContent = "0";
    if (depositsEl) depositsEl.textContent = "0";
    if (hintEl && !hintLocked) {
      hintEl.textContent = "Parrainage désactivé sur les pages publiques.";
      hintEl.style.color = "";
    }
    if (copyLinkBtn) copyLinkBtn.setAttribute("data-link", "");
    return;
  }

  if (token !== referralLoadToken) return;
  const clientData = latestProfileClientData || {};
  const referralCode = normalizeReferralCode(clientData.referralCode || "");
  const referralLink = referralCode ? buildProfileReferralLink(referralCode) : "";
  const signupsTotal = safeCount(clientData.referralSignupsTotal);
  const depositsTotal = safeCount(clientData.referralDepositsTotal);

  if (codeEl) codeEl.textContent = referralCode || "Génération...";
  if (signupsEl) signupsEl.textContent = String(signupsTotal);
  if (depositsEl) depositsEl.textContent = String(depositsTotal);
  if (copyLinkBtn) copyLinkBtn.setAttribute("data-link", referralLink);

  if (hintEl) {
    if (hintLocked) return;
    hintEl.style.color = "";
    hintEl.textContent = referralCode
      ? "Ton code et ton lien de parrainage sont prêts."
      : "Génération du code de parrainage...";
  }
}

function updateAgentDashboardAccess(user) {
  const agentBtn = document.getElementById("profileAgentDashboardBtn");
  if (!agentBtn) return;

  const clientData = latestProfileClientData || {};
  const isAgent = clientData.isAgent === true
    || clientData.agentDashboardEnabled === true
    || String(clientData.agentStatus || "").trim().length > 0
    || String(clientData.agentPromoCode || "").trim().length > 0;
  const status = String(clientData.agentStatus || "").trim().toLowerCase();

  if (!user?.uid || !isAgent) {
    agentBtn.classList.add("hidden");
    agentBtn.disabled = true;
    agentBtn.textContent = "Dashboard agent";
    return;
  }

  agentBtn.classList.remove("hidden");
  agentBtn.disabled = false;
  agentBtn.textContent = status === "active"
    ? "Dashboard agent"
    : "Dashboard agent (inactif)";
}

export function mountProfileModal(options = {}) {
  const { triggerSelector = "#p2Profile" } = options;
  const overlay = ensureProfileModal();
  const openModal = overlay.__openModal;
  const closeModal = overlay.__closeModal;

  const trigger = document.querySelector(triggerSelector);
  if (trigger && openModal) {
    trigger.addEventListener("click", () => {
      updateProfileData(auth.currentUser);
      openModal();

      const depositBtn = document.getElementById("profileDepositBtn");
      const withdrawBtn = document.getElementById("profileWithdrawBtn");
      const passwordBtn = document.getElementById("profilePasswordBtn");
      if (depositBtn && !depositBtn.dataset.bound) {
        depositBtn.dataset.bound = "1";
        depositBtn.addEventListener("click", () => {
          closeModal();
          const soldBadge = document.getElementById("soldBadge");
          if (soldBadge) {
            soldBadge.click();
          }
        });
      }
      if (withdrawBtn && !withdrawBtn.dataset.bound) {
        withdrawBtn.dataset.bound = "1";
        withdrawBtn.addEventListener("click", () => {
          closeModal();
          if (typeof window.openRetraitDirectly === "function") {
            window.openRetraitDirectly();
          }
        });
      }
      if (passwordBtn && !passwordBtn.dataset.bound) {
        passwordBtn.dataset.bound = "1";
        passwordBtn.addEventListener("click", () => {
          ensureProfilePasswordModal().__openPasswordModal?.();
        });
      }
    });
  }

  watchAuthState((user) => {
    const activeUser = user || auth.currentUser || null;
    ensureProfileRealtimeWatchers(activeUser);
    scheduleProfileFundingRefresh(activeUser, 0);
    updateProfileData(activeUser);
  });
  bindProfileVisibilityRefresh();
  bindProfileWithdrawalEvents();

  window.addEventListener("userBalanceUpdated", () => {
    scheduleProfileFundingRefresh(auth.currentUser, 80);
    updateProfileData(auth.currentUser);
  });
  window.addEventListener("xchangeUpdated", () => {
    scheduleProfileFundingRefresh(auth.currentUser, 80);
    updateProfileData(auth.currentUser);
  });

  mountXchangeModal({ triggerSelector: "#profileXchangeBtn" });
  mountRetraitModal({ triggerSelector: "#profileWithdrawBtn" });

  ensureProfileRealtimeWatchers(auth.currentUser);
  scheduleProfileFundingRefresh(auth.currentUser, 0);
  updateProfileData(auth.currentUser);
}

export function mountProfilePage(options = {}) {
  const {
    backSelector = "#profileBackBtn",
    logoutRedirectUrl = "./auth.html",
    fallbackBackUrl = "./inedex.html",
  } = options;

  const backBtn = document.querySelector(backSelector);
  const logoutBtn = document.getElementById("profileLogoutBtn");
  const referralRulesBtn = document.getElementById("profileReferralRulesBtn");
  const referralRulesOverlay = document.getElementById("profileReferralRulesOverlay");
  const referralRulesPanel = document.getElementById("profileReferralRulesPanel");
  const referralRulesClose = document.getElementById("profileReferralRulesClose");
  const generalRulesBtn = document.getElementById("profileGeneralRulesBtn");
  const generalRulesOverlay = document.getElementById("profileGeneralRulesOverlay");
  const generalRulesPanel = document.getElementById("profileGeneralRulesPanel");
  const generalRulesClose = document.getElementById("profileGeneralRulesClose");
  const copyReferralCodeBtn = document.getElementById("profileCopyReferralCode");
  const copyReferralLinkBtn = document.getElementById("profileCopyReferralLink");
  const agentDashboardBtn = document.getElementById("profileAgentDashboardBtn");
  const contactAgentBtn = document.getElementById("profileContactAgentBtn");

  const closeOverlay = (overlay) => {
    if (!overlay) return;
    overlay.classList.add("hidden");
    overlay.classList.remove("flex");
    document.body.classList.remove("overflow-hidden");
  };

  const openOverlay = (overlay) => {
    if (!overlay) return;
    overlay.classList.remove("hidden");
    overlay.classList.add("flex");
    document.body.classList.add("overflow-hidden");
  };

  if (backBtn && backBtn.dataset.bound !== "1") {
    backBtn.dataset.bound = "1";
    backBtn.addEventListener("click", () => {
      try {
        const sameOriginReferrer = document.referrer && new URL(document.referrer).origin === window.location.origin;
        if (sameOriginReferrer && window.history.length > 1) {
          window.history.back();
          return;
        }
      } catch (_) {}
      window.location.href = fallbackBackUrl;
    });
  }

  if (referralRulesPanel && referralRulesPanel.dataset.bound !== "1") {
    referralRulesPanel.dataset.bound = "1";
    referralRulesPanel.addEventListener("click", (ev) => ev.stopPropagation());
  }

  if (referralRulesOverlay && referralRulesOverlay.dataset.bound !== "1") {
    referralRulesOverlay.dataset.bound = "1";
    referralRulesOverlay.addEventListener("click", (ev) => {
      if (ev.target === referralRulesOverlay) closeOverlay(referralRulesOverlay);
    });
  }

  if (referralRulesClose && referralRulesClose.dataset.bound !== "1") {
    referralRulesClose.dataset.bound = "1";
    referralRulesClose.addEventListener("click", () => closeOverlay(referralRulesOverlay));
  }

  if (referralRulesBtn && referralRulesBtn.dataset.bound !== "1") {
    referralRulesBtn.dataset.bound = "1";
    referralRulesBtn.addEventListener("click", () => openOverlay(referralRulesOverlay));
  }

  if (generalRulesPanel && generalRulesPanel.dataset.bound !== "1") {
    generalRulesPanel.dataset.bound = "1";
    generalRulesPanel.addEventListener("click", (ev) => ev.stopPropagation());
  }

  if (generalRulesOverlay && generalRulesOverlay.dataset.bound !== "1") {
    generalRulesOverlay.dataset.bound = "1";
    generalRulesOverlay.addEventListener("click", (ev) => {
      if (ev.target === generalRulesOverlay) closeOverlay(generalRulesOverlay);
    });
  }

  if (generalRulesClose && generalRulesClose.dataset.bound !== "1") {
    generalRulesClose.dataset.bound = "1";
    generalRulesClose.addEventListener("click", () => closeOverlay(generalRulesOverlay));
  }

  if (generalRulesBtn && generalRulesBtn.dataset.bound !== "1") {
    generalRulesBtn.dataset.bound = "1";
    generalRulesBtn.addEventListener("click", () => openOverlay(generalRulesOverlay));
  }

  if (logoutBtn && logoutBtn.dataset.bound !== "1") {
    logoutBtn.dataset.bound = "1";
    logoutBtn.addEventListener("click", async () => {
      try {
        await logoutCurrentUser();
        window.location.href = logoutRedirectUrl;
      } catch (err) {
        console.error("Logout error:", err);
      }
    });
  }

  const copyToClipboard = async (text) => {
    const value = String(text || "").trim();
    if (!value || value === "-") return false;
    try {
      if (navigator.clipboard && navigator.clipboard.writeText) {
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
  };

  if (copyReferralCodeBtn && copyReferralCodeBtn.dataset.bound !== "1") {
    copyReferralCodeBtn.dataset.bound = "1";
    copyReferralCodeBtn.addEventListener("click", async () => {
      const code = document.getElementById("profileReferralCode")?.textContent || "";
      const ok = await copyToClipboard(code);
      showReferralCopyFeedback(ok ? "Code copié avec succès." : "Impossible de copier le code.", ok);
    });
  }

  if (copyReferralLinkBtn && copyReferralLinkBtn.dataset.bound !== "1") {
    copyReferralLinkBtn.dataset.bound = "1";
    copyReferralLinkBtn.addEventListener("click", async () => {
      const link = copyReferralLinkBtn.getAttribute("data-link") || "";
      const ok = await copyToClipboard(link);
      showReferralCopyFeedback(ok ? "Lien copié avec succès." : "Impossible de copier le lien.", ok);
    });
  }

  if (agentDashboardBtn && agentDashboardBtn.dataset.bound !== "1") {
    agentDashboardBtn.dataset.bound = "1";
    agentDashboardBtn.addEventListener("click", () => {
      window.location.href = "./agent-dashboard.html";
    });
  }

  if (contactAgentBtn && contactAgentBtn.dataset.bound !== "1") {
    contactAgentBtn.dataset.bound = "1";
    contactAgentBtn.addEventListener("click", () => {
      const url = buildSupportWhatsAppUrl("Bonjou agent, mwen bezwen asistans sou kont mwen tanpri.");
      const popup = window.open(url, "_blank", "noopener,noreferrer");
      if (!popup) {
        window.location.href = url;
      }
    });
  }

  const pendingOpsOverlay = ensureProfilePendingOperationsModal();
  const pendingOpsBtn = document.getElementById("profilePendingOpsBtn");
  const profilePasswordBtn = document.getElementById("profilePasswordBtn");
  if (pendingOpsBtn && pendingOpsBtn.dataset.bound !== "1") {
    pendingOpsBtn.dataset.bound = "1";
    pendingOpsBtn.addEventListener("click", async () => {
      await waitForBalanceHydration(auth.currentUser?.uid, 1800);
      pendingOpsOverlay?.__openPendingOps?.();
    });
  }

  if (profilePasswordBtn && profilePasswordBtn.dataset.bound !== "1") {
    profilePasswordBtn.dataset.bound = "1";
    profilePasswordBtn.addEventListener("click", () => {
      ensureProfilePasswordModal().__openPasswordModal?.();
    });
  }

  if (!profilePendingOpsBound) {
    profilePendingOpsBound = true;
    window.addEventListener("pendingOperationsUpdated", () => {
      const overlay = document.getElementById("profilePendingOpsOverlay");
      if (overlay?.classList.contains("flex")) {
        refreshProfilePendingOperationsModal();
      }
    });
  }

  mountSoldeModal({ triggerSelector: "#profileDepositBtn" });
  mountXchangeModal({ triggerSelector: "#profileXchangeBtn" });
  mountRetraitModal({ triggerSelector: "#profileWithdrawBtn" });

  watchAuthState((user) => {
    const activeUser = user || auth.currentUser || null;
    ensureProfileRealtimeWatchers(activeUser);
    scheduleProfileFundingRefresh(activeUser, 0);
    updateProfileData(activeUser);
  });
  bindProfileVisibilityRefresh();
  bindProfileWithdrawalEvents();

  window.addEventListener("userBalanceUpdated", () => {
    scheduleProfileFundingRefresh(auth.currentUser, 80);
    updateProfileData(auth.currentUser);
  });
  window.addEventListener("xchangeUpdated", () => {
    scheduleProfileFundingRefresh(auth.currentUser, 80);
    updateProfileData(auth.currentUser);
  });

  ensureProfileRealtimeWatchers(auth.currentUser);
  scheduleProfileFundingRefresh(auth.currentUser, 0);
  updateProfileData(auth.currentUser);
}
