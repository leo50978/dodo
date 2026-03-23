import { syncPwaInstallPrompt } from "./pwa-install.js";
import { ensureAnimeRuntime } from "./anime-loader.js";
import {
  withButtonLoading,
  showGlobalLoading,
  hideGlobalLoading,
} from "./loading-ui.js";
import {
  getPublicGameStakeOptionsSecure,
  updateClientProfileSecure,
  getShareSitePromoStatusSecure,
  recordShareSitePromoSecure,
  createFriendRoomSecure,
  joinFriendRoomByCodeSecure,
  getActiveSurveyForUserSecure,
  submitSurveyResponseSecure,
  ackClientFinanceNoticeSecure,
} from "./secure-functions.js";
import { auth, db, collection, query, orderBy, limit, doc, getDoc, getDocs, setDoc, serverTimestamp, onSnapshot } from "./firebase-init.js";

const CHAT_COLLECTION = "globalChannelMessages";
const SUPPORT_THREADS_COLLECTION = "supportThreads";
const AUTH_SUCCESS_NOTICE_STORAGE_KEY = "domino_auth_success_notice_v1";
const TOURNAMENT_INTRO_SEEN_STORAGE_KEY = "domino_tournament_intro_seen_v1";
const DEFAULT_STAKE_REWARD_MULTIPLIER = 3;
const PAGE2_BOOTSTRAP_MIN_MS = 650;
const PAGE2_BOOTSTRAP_TIMEOUT_MS = 2600;
const PAGE2_HERO_IMAGES = Object.freeze(["hero.jpg", "hero1.jpg", "hero2.jpg"]);
const PAGE2_HERO_ROTATION_MS = 10000;
const SHARE_SITE_PROMO_TARGET = 5;
const SHARE_SITE_PROMO_REWARD_DOES = 100;
const SHARE_SITE_PROMO_LINK = "https://dominoeslakay.com";
const SHARE_SITE_PROMO_TITLE = "Dominoes Lakay";
const SHARE_SITE_PROMO_TEXT = "Viens jouer au domino avec moi et gagne de l'argent. 25 Gdes gratuit comme prime d'inscription.";
const CLIENT_FINANCE_NOTICE_LAUNCH_MS = Date.parse("2026-03-23T00:00:00Z");
const DEFAULT_GAME_STAKE_OPTIONS = Object.freeze([
  Object.freeze({ id: "stake_100", stakeDoes: 100, rewardDoes: 300, enabled: true, sortOrder: 10 }),
  Object.freeze({ id: "stake_500", stakeDoes: 500, rewardDoes: 1500, enabled: false, sortOrder: 20 }),
  Object.freeze({ id: "stake_1000", stakeDoes: 1000, rewardDoes: 3000, enabled: false, sortOrder: 30 }),
  Object.freeze({ id: "stake_5000", stakeDoes: 5000, rewardDoes: 15000, enabled: false, sortOrder: 40 }),
]);
let page2NonCriticalRefreshTimer = null;
let page2NonCriticalVisibilityHandler = null;
let page2NonCriticalUid = "";
let applyPage2AccountState = () => {};
let page2PresenceVisibilityBound = false;
let page2PresenceUser = null;
let page2PresenceTick = null;
const profileBootstrapInFlightByUid = new Map();
let page2BootstrapRunId = 0;
let soldeModulePromise = null;
let xchangeModulePromise = null;
let soldeUiReadyRunId = 0;
let soldeUiReadyPromise = null;
let page2HeroRotationTimer = null;
let page2SharePromoCountdownTimer = null;
let page2FinanceNoticeUid = "";
let page2FinanceNoticeUnsubs = [];
let page2FinanceOrderDocs = [];
let page2FinanceWithdrawalDocs = [];
let page2FinanceNoticeQueue = [];
let page2FinanceNoticeActive = null;
const page2FinanceNoticeSessionSeen = new Set();
const PAGE2_PRESENCE_PING_MS = 10 * 60 * 1000;
const PAGE2_NON_CRITICAL_REFRESH_MS = 2 * 60 * 1000;

async function runPage2Animations() {
  try {
    const anime = await ensureAnimeRuntime();
    if (!anime) return;

    anime({
      targets: "#page2Root",
      opacity: [0, 1],
      duration: 550,
      easing: "easeOutQuad",
    });

    anime({
      targets: "header, section, #startGameBtn",
      translateY: [16, 0],
      opacity: [0, 1],
      delay: anime.stagger(90, { start: 130 }),
      duration: 520,
      easing: "easeOutCubic",
    });
  } catch (error) {
    console.warn("[PAGE2] animation runtime unavailable", error);
  }
}

async function loadSoldeModule() {
  if (!soldeModulePromise) {
    soldeModulePromise = import("./solde.js");
  }
  return soldeModulePromise;
}

async function loadXchangeModule() {
  if (!xchangeModulePromise) {
    xchangeModulePromise = import("./xchange.js");
  }
  return xchangeModulePromise;
}

function scheduleNonCriticalTask(runId, task, delayMs = 240) {
  const execute = () => {
    if (runId !== page2BootstrapRunId) return;
    Promise.resolve()
      .then(task)
      .catch((error) => {
        console.warn("[PAGE2] deferred task failed", error);
      });
  };

  if ("requestIdleCallback" in window) {
    window.requestIdleCallback(execute, { timeout: Math.max(600, Number(delayMs) + 900 || 1200) });
    return;
  }

  window.setTimeout(execute, Math.max(80, Number(delayMs) || 240));
}

function openProfilePage() {
  showGlobalLoading("Ouverture du profil...");
  window.location.href = "./profil.html";
}

function normalizeInviteCode(value = "") {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9_-]/g, "");
}

function buildFriendGameUrl(roomId, seatIndex, stakeDoes) {
  const params = new URLSearchParams();
  params.set("autostart", "1");
  params.set("stake", String(Math.max(1, Number.parseInt(String(stakeDoes || 0), 10) || 100)));
  params.set("friendRoomId", String(roomId || "").trim());
  params.set("seat", String(Math.max(0, Number.parseInt(String(seatIndex || 0), 10) || 0)));
  params.set("roomMode", "friends");
  return `./jeu.html?${params.toString()}`;
}

function hasSeenTournamentIntro() {
  try {
    return localStorage.getItem(TOURNAMENT_INTRO_SEEN_STORAGE_KEY) === "1";
  } catch (_) {
    return false;
  }
}

function markTournamentIntroSeen() {
  try {
    localStorage.setItem(TOURNAMENT_INTRO_SEEN_STORAGE_KEY, "1");
  } catch (_) {
  }
}

function makePromoActionId() {
  return `share_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 9)}`;
}

function isShareAbortError(error) {
  const name = String(error?.name || "").toLowerCase();
  const code = String(error?.code || "").toLowerCase();
  const message = String(error?.message || "").toLowerCase();
  return name === "aborterror"
    || code === "aborterror"
    || message.includes("cancel")
    || message.includes("annul");
}

function formatPromoCountdown(ms = 0) {
  const totalMs = Math.max(0, Number(ms) || 0);
  const totalSeconds = Math.ceil(totalMs / 1000);
  const days = Math.floor(totalSeconds / 86400);
  const hours = Math.floor((totalSeconds % 86400) / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);

  if (days > 0) {
    return `${days}j ${String(hours).padStart(2, "0")}h`;
  }
  if (hours > 0) {
    return `${hours}h ${String(minutes).padStart(2, "0")}m`;
  }
  return `${Math.max(0, minutes)}m`;
}

function isCompactSharePromoUi() {
  return window.matchMedia("(max-width: 639px)").matches;
}

function buildShareSitePromoPayload() {
  return {
    title: SHARE_SITE_PROMO_TITLE,
    text: SHARE_SITE_PROMO_TEXT,
    url: SHARE_SITE_PROMO_LINK,
  };
}

function buildShareSitePromoMessage() {
  const payload = buildShareSitePromoPayload();
  return `${payload.text} ${payload.url}`.trim();
}

function buildShareSitePromoTargets() {
  const payload = buildShareSitePromoPayload();
  const message = buildShareSitePromoMessage();
  return Object.freeze([
    {
      id: "whatsapp",
      label: "WhatsApp",
      icon: "fa-brands fa-whatsapp",
      url: `https://wa.me/?text=${encodeURIComponent(message)}`,
    },
    {
      id: "facebook",
      label: "Facebook",
      icon: "fa-brands fa-facebook-f",
      url: `https://www.facebook.com/sharer/sharer.php?u=${encodeURIComponent(payload.url)}`,
    },
    {
      id: "x",
      label: "X",
      icon: "fa-brands fa-x-twitter",
      url: `https://twitter.com/intent/tweet?text=${encodeURIComponent(payload.text)}&url=${encodeURIComponent(payload.url)}`,
    },
    {
      id: "telegram",
      label: "Telegram",
      icon: "fa-brands fa-telegram",
      url: `https://t.me/share/url?url=${encodeURIComponent(payload.url)}&text=${encodeURIComponent(payload.text)}`,
    },
  ]);
}

async function openShareSitePromoTarget(targetId = "") {
  const target = buildShareSitePromoTargets().find((item) => item.id === String(targetId || "").trim()) || null;
  if (!target) {
    throw new Error("Canal de partage introuvable.");
  }
  const popup = window.open(target.url, "_blank", "noopener,noreferrer");
  if (!popup) {
    window.location.href = target.url;
  }
  return { source: target.id };
}

async function ensureSoldeUiReady(triggerSelector = "#soldBadge") {
  if (soldeUiReadyRunId === page2BootstrapRunId && soldeUiReadyPromise) {
    return soldeUiReadyPromise;
  }
  soldeUiReadyRunId = page2BootstrapRunId;
  soldeUiReadyPromise = loadSoldeModule().then((soldeModule) => {
    soldeModule.mountSoldeModal({ triggerSelector });
    const trigger = document.querySelector(triggerSelector);
    if (trigger) trigger.dataset.modalBootstrapReady = "1";
    return soldeModule;
  });
  return soldeUiReadyPromise;
}

function bindDeferredModalTrigger(trigger, ensureReady, loadingMessage) {
  if (!trigger || trigger.dataset.deferredModalBound === "1") return;
  trigger.dataset.deferredModalBound = "1";

  trigger.addEventListener("click", (event) => {
    if (trigger.dataset.modalBootstrapReady === "1") return;
    event.preventDefault();
    event.stopPropagation();
    if (typeof event.stopImmediatePropagation === "function") {
      event.stopImmediatePropagation();
    }
    showGlobalLoading(loadingMessage);
    Promise.resolve()
      .then(() => ensureReady())
      .then(() => {
        hideGlobalLoading();
        window.setTimeout(() => {
          trigger.click();
        }, 0);
      })
      .catch((error) => {
        console.error("[PAGE2] deferred modal bootstrap error", error);
        hideGlobalLoading();
      });
  }, true);
}

function getPage2Shell() {
  return document.getElementById("domino-app-shell") || document.body;
}

function stopPage2HeroRotation() {
  if (!page2HeroRotationTimer) return;
  window.clearInterval(page2HeroRotationTimer);
  page2HeroRotationTimer = null;
}

function preloadPage2HeroImages() {
  PAGE2_HERO_IMAGES.forEach((src) => {
    const img = new Image();
    img.src = src;
  });
}

function initPage2HeroRotation() {
  const heroImage = document.getElementById("page2HeroImage");
  stopPage2HeroRotation();
  if (!heroImage) return;

  preloadPage2HeroImages();
  let activeIndex = 0;
  heroImage.src = PAGE2_HERO_IMAGES[activeIndex];

  if (PAGE2_HERO_IMAGES.length <= 1) return;

  page2HeroRotationTimer = window.setInterval(() => {
    heroImage.style.opacity = "0";
    window.setTimeout(() => {
      activeIndex = (activeIndex + 1) % PAGE2_HERO_IMAGES.length;
      heroImage.src = PAGE2_HERO_IMAGES[activeIndex];
      heroImage.style.opacity = "1";
    }, 320);
  }, PAGE2_HERO_ROTATION_MS);
}

function waitForMinimumDelay(ms = PAGE2_BOOTSTRAP_MIN_MS) {
  return new Promise((resolve) => {
    window.setTimeout(resolve, Math.max(0, Number(ms) || 0));
  });
}

function withBootstrapTimeout(promise, timeoutMs = PAGE2_BOOTSTRAP_TIMEOUT_MS, fallbackValue = null) {
  return new Promise((resolve) => {
    let settled = false;
    const done = (value) => {
      if (settled) return;
      settled = true;
      resolve(value);
    };
    const timer = window.setTimeout(() => done(fallbackValue), Math.max(300, Number(timeoutMs) || PAGE2_BOOTSTRAP_TIMEOUT_MS));
    Promise.resolve(promise)
      .then((value) => {
        window.clearTimeout(timer);
        done(value);
      })
      .catch(() => {
        window.clearTimeout(timer);
        done(fallbackValue);
      });
  });
}

async function runPage2BootstrapFlow({
  runId,
  user,
  isAuthenticated,
  hasConfirmedAuth,
}) {
  const minDelayPromise = waitForMinimumDelay(isAuthenticated ? PAGE2_BOOTSTRAP_MIN_MS : 180);
  const profilePromise = hasConfirmedAuth
    ? withBootstrapTimeout(ensureClientReferralBootstrap(user), PAGE2_BOOTSTRAP_TIMEOUT_MS, null)
    : Promise.resolve(null);
  const balancePromise = hasConfirmedAuth
    ? withBootstrapTimeout(
      ensureSoldeUiReady("#soldBadge").then((soldeModule) => soldeModule.waitForBalanceHydration(user?.uid)),
      PAGE2_BOOTSTRAP_TIMEOUT_MS,
      false
    )
    : Promise.resolve(false);

  if (isAuthenticated) {
    showGlobalLoading("Préparation de votre espace...");
  }

  if (hasConfirmedAuth) {
    showGlobalLoading("Préparation du profil...");
    await profilePromise;

    showGlobalLoading("Synchronisation du solde...");
    await balancePromise;
  }

  await minDelayPromise;
  if (runId === page2BootstrapRunId) {
    hideGlobalLoading();
    syncPwaInstallPrompt({ enabled: true });
  }
}

function safeInt(value) {
  const num = Number(value);
  return Number.isFinite(num) ? Math.max(0, Math.floor(num)) : 0;
}

function buildStakeRewardDoes(stakeDoes) {
  return safeInt(stakeDoes) * DEFAULT_STAKE_REWARD_MULTIPLIER;
}

function normalizeGameStakeOptions(rawOptions) {
  const source = Array.isArray(rawOptions) && rawOptions.length ? rawOptions : DEFAULT_GAME_STAKE_OPTIONS;
  const byStake = new Map();

  source.forEach((raw, index) => {
    const stakeDoes = safeInt(raw?.stakeDoes);
    if (stakeDoes <= 0) return;
    if (byStake.has(stakeDoes)) return;

    const sortOrderRaw = Number(raw?.sortOrder);
    const sortOrder = Number.isFinite(sortOrderRaw) ? Math.trunc(sortOrderRaw) : ((index + 1) * 10);
    const rewardDoes = safeInt(raw?.rewardDoes) || buildStakeRewardDoes(stakeDoes);

    byStake.set(stakeDoes, {
      id: String(raw?.id || `stake_${stakeDoes}`).trim() || `stake_${stakeDoes}`,
      stakeDoes,
      rewardDoes,
      enabled: raw?.enabled !== false,
      sortOrder,
    });
  });

  const normalized = Array.from(byStake.values()).sort((left, right) => {
    if (left.sortOrder !== right.sortOrder) return left.sortOrder - right.sortOrder;
    return left.stakeDoes - right.stakeDoes;
  });

  return normalized.length ? normalized : DEFAULT_GAME_STAKE_OPTIONS.map((item) => ({ ...item }));
}

async function loadPublicGameStakeOptions() {
  try {
    const response = await getPublicGameStakeOptionsSecure();
    return normalizeGameStakeOptions(response?.options);
  } catch (error) {
    console.warn("[GAME_STAKES] fallback local options", error);
    return normalizeGameStakeOptions();
  }
}

function stopPage2ChatWatchers() {
  if (page2NonCriticalRefreshTimer) {
    clearInterval(page2NonCriticalRefreshTimer);
    page2NonCriticalRefreshTimer = null;
  }
  if (page2NonCriticalVisibilityHandler) {
    document.removeEventListener("visibilitychange", page2NonCriticalVisibilityHandler);
    page2NonCriticalVisibilityHandler = null;
  }
  page2NonCriticalUid = "";
}

function stopPage2FinanceNoticeWatchers() {
  page2FinanceNoticeUnsubs.forEach((unsubscribe) => {
    try {
      unsubscribe?.();
    } catch (_) {
    }
  });
  page2FinanceNoticeUnsubs = [];
  page2FinanceNoticeUid = "";
  page2FinanceOrderDocs = [];
  page2FinanceWithdrawalDocs = [];
  page2FinanceNoticeQueue = [];
  page2FinanceNoticeActive = null;
}

async function refreshDiscussionFabState(user) {
  const badge = document.getElementById("discussionFabBadge");
  const uid = String(user?.uid || "");
  if (!badge || !uid) {
    badge?.classList.add("hidden");
    return;
  }

  try {
    const [latestSnap, clientSnap] = await Promise.all([
      getDocs(query(collection(db, CHAT_COLLECTION), orderBy("createdAt", "desc"), limit(1))),
      getDoc(doc(db, "clients", uid)),
    ]);
    const latestDoc = latestSnap.empty ? null : (latestSnap.docs[0]?.data() || {});
    const clientData = clientSnap.exists() ? (clientSnap.data() || {}) : {};
    const latestMessageMs = tsToMs(latestDoc?.createdAt);
    const seenMs = tsToMs(clientData.chatLastSeenAt);
    badge.classList.toggle("hidden", !(latestMessageMs > 0 && latestMessageMs > seenMs));
  } catch (err) {
    console.error("Erreur refresh messages discussion:", err);
    badge.classList.add("hidden");
  }
}

async function refreshAgentSupportAlertState(user) {
  const alertWrap = document.getElementById("agentSupportAlertWrap");
  const alertText = document.getElementById("agentSupportAlertText");
  const uid = String(user?.uid || "");
  if (!alertWrap || !alertText || !uid) {
    alertWrap?.classList.add("hidden");
    return;
  }

  try {
    const snap = await getDoc(doc(db, SUPPORT_THREADS_COLLECTION, `user_${uid}`));
    const data = snap.exists() ? (snap.data() || {}) : {};
    const unread = data.unreadForUser === true && String(data.lastSenderRole || "") === "agent";
    alertWrap.classList.toggle("hidden", !unread);
    if (!unread) return;
    const preview = String(data.lastMessageText || "").trim();
    alertText.textContent = preview
      ? `Vous avez recu un message par un agent: ${preview}`
      : "Vous avez recu un message par un agent.";
  } catch (err) {
    console.error("Erreur refresh alerte agent:", err);
    alertWrap.classList.add("hidden");
  }
}

async function refreshPage2AccountState(user) {
  const uid = String(user?.uid || "");
  if (!uid) {
    applyPage2AccountState({});
    return;
  }
  try {
    const snap = await getDoc(doc(db, "clients", uid));
    applyPage2AccountState(snap.exists() ? (snap.data() || {}) : {});
  } catch (error) {
    console.error("Erreur refresh statut compte accueil:", error);
    applyPage2AccountState({});
  }
}

async function refreshPage2NonCriticalUi(user) {
  await Promise.allSettled([
    refreshPage2AccountState(user),
    refreshDiscussionFabState(user),
    refreshAgentSupportAlertState(user),
  ]);
}

function startPage2NonCriticalPolling(user) {
  const uid = String(user?.uid || "");
  stopPage2ChatWatchers();
  if (!uid) {
    void refreshPage2NonCriticalUi(null);
    return;
  }

  page2NonCriticalUid = uid;
  void refreshPage2NonCriticalUi(user);
  page2NonCriticalRefreshTimer = setInterval(() => {
    if (document.visibilityState !== "visible") return;
    if (page2NonCriticalUid !== String(auth.currentUser?.uid || "")) return;
    void refreshPage2NonCriticalUi(auth.currentUser || user);
  }, PAGE2_NON_CRITICAL_REFRESH_MS);

  page2NonCriticalVisibilityHandler = () => {
    if (document.visibilityState !== "visible") return;
    if (page2NonCriticalUid !== String(auth.currentUser?.uid || "")) return;
    void refreshPage2NonCriticalUi(auth.currentUser || user);
  };
  document.addEventListener("visibilitychange", page2NonCriticalVisibilityHandler);
}

function tsToMs(value) {
  if (!value) return 0;
  if (typeof value.toMillis === "function") return value.toMillis();
  if (typeof value.toDate === "function") return value.toDate().getTime();
  if (typeof value.seconds === "number") return value.seconds * 1000;
  const parsed = Date.parse(String(value));
  return Number.isFinite(parsed) ? parsed : 0;
}

function formatFinanceAmountHtg(value = 0) {
  const amount = Number(value) || 0;
  if (!Number.isFinite(amount) || amount <= 0) return "0 HTG";
  const rounded = Math.round(amount * 100) / 100;
  const formatted = Number.isInteger(rounded)
    ? String(rounded)
    : rounded.toLocaleString("fr-FR", { minimumFractionDigits: 0, maximumFractionDigits: 2 });
  return `${formatted} HTG`;
}

function getFinanceNoticeEventMs(data = {}) {
  const candidates = [
    data.clientStatusNoticeEventAtMs,
    data.reviewResolvedAtMs,
    data.withdrawalApprovedAtMs,
    data.withdrawalRejectedAtMs,
    data.approvedAtMs,
    data.rejectedAtMs,
    data.fundingSettledAtMs,
    data.updatedAtMs,
    tsToMs(data.updatedAt),
    data.createdAtMs,
    tsToMs(data.createdAt),
  ];
  for (const candidate of candidates) {
    const numeric = Number(candidate) || 0;
    if (numeric > 0) return numeric;
  }
  return 0;
}

function getFinanceNoticeAmountHtg(kind, data = {}) {
  if (kind === "withdrawal") {
    return Number(
      data.requestedAmountHtg
      ?? data.amountHtg
      ?? data.amount
      ?? 0,
    ) || 0;
  }
  return Number(
    data.approvedAmountHtg
    ?? data.convertedAmountHtg
    ?? data.amountHtg
    ?? data.amount
    ?? 0,
  ) || 0;
}

function buildFinanceNotice(kind, id, data = {}) {
  const status = String(data.status || "").trim().toLowerCase();
  if (status !== "approved" && status !== "rejected") return null;
  if (data.userHiddenByClient === true) return null;

  const eventMs = getFinanceNoticeEventMs(data);
  if (!(eventMs >= CLIENT_FINANCE_NOTICE_LAUNCH_MS)) return null;

  const noticeKey = `${kind}:${id}:${status}:${eventMs}`;
  if (String(data.clientStatusNoticeSeenKey || "").trim() === noticeKey) return null;

  const amountLabel = formatFinanceAmountHtg(getFinanceNoticeAmountHtg(kind, data));
  const isApproved = status === "approved";
  const isWithdrawal = kind === "withdrawal";
  const title = isWithdrawal
    ? (isApproved ? "Ton retrait est approuvé" : "Ton retrait a été refusé")
    : (isApproved ? "Ton dépôt est approuvé" : "Ton dépôt a été refusé");
  const body = isWithdrawal
    ? (isApproved
      ? `Ta demande de retrait de ${amountLabel} a été validée.`
      : `Ta demande de retrait de ${amountLabel} n'a pas été validée.`)
    : (isApproved
      ? `Ton dépôt de ${amountLabel} a été approuvé et ajouté à ton compte.`
      : `Ton dépôt de ${amountLabel} n'a pas été approuvé.`);
  const reason = String(
    data.reviewReason
    || data.rejectionReason
    || data.adminNote
    || data.reason
    || "",
  ).trim();

  return {
    kind,
    id,
    status,
    eventMs,
    noticeKey,
    amountLabel,
    title,
    body,
    reason,
    accentClass: isApproved
      ? "border-emerald-300/35 bg-emerald-500/18 text-emerald-100"
      : "border-rose-300/35 bg-rose-500/18 text-rose-100",
    iconClass: isApproved
      ? "fa-solid fa-badge-check"
      : "fa-solid fa-circle-exclamation",
  };
}

function rebuildPage2FinanceNoticeQueue() {
  const candidates = [
    ...page2FinanceOrderDocs.map((item) => buildFinanceNotice("order", item.id, item.data)),
    ...page2FinanceWithdrawalDocs.map((item) => buildFinanceNotice("withdrawal", item.id, item.data)),
  ]
    .filter(Boolean)
    .filter((item) => !page2FinanceNoticeSessionSeen.has(item.noticeKey))
    .filter((item) => item.noticeKey !== page2FinanceNoticeActive?.noticeKey)
    .sort((left, right) => left.eventMs - right.eventMs);

  page2FinanceNoticeQueue = candidates;
}

function setPage2FinanceNoticeOpen(isOpen) {
  const overlay = document.getElementById("financeNoticeOverlay");
  if (!overlay) return;
  overlay.classList.toggle("hidden", !isOpen);
  overlay.classList.toggle("flex", isOpen);
  if (!isOpen) {
    if (
      document.getElementById("sharePromoOverlay")?.classList.contains("hidden")
      && document.getElementById("sharePromoSuccessOverlay")?.classList.contains("hidden")
      && document.getElementById("stakeSelectionOverlay")?.classList.contains("hidden")
      && document.getElementById("friendModeOverlay")?.classList.contains("hidden")
      && document.getElementById("friendCreateOverlay")?.classList.contains("hidden")
      && document.getElementById("friendJoinOverlay")?.classList.contains("hidden")
      && document.getElementById("friendCodeOverlay")?.classList.contains("hidden")
      && document.getElementById("doesRequiredOverlay")?.classList.contains("hidden")
      && document.getElementById("surveyPromptOverlay")?.classList.contains("hidden")
      && document.getElementById("tournamentIntroOverlay")?.classList.contains("hidden")
    ) {
      document.body.classList.remove("overflow-hidden");
    }
    return;
  }
  document.body.classList.add("overflow-hidden");
}

function renderPage2FinanceNotice() {
  const notice = page2FinanceNoticeActive;
  const badge = document.getElementById("financeNoticeBadge");
  const icon = document.getElementById("financeNoticeIcon");
  const title = document.getElementById("financeNoticeTitle");
  const body = document.getElementById("financeNoticeBody");
  const amount = document.getElementById("financeNoticeAmount");
  const reasonWrap = document.getElementById("financeNoticeReasonWrap");
  const reasonText = document.getElementById("financeNoticeReasonText");

  if (!notice || !badge || !icon || !title || !body || !amount || !reasonWrap || !reasonText) {
    setPage2FinanceNoticeOpen(false);
    return;
  }

  badge.className = `inline-flex w-fit rounded-full border px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.18em] ${notice.accentClass}`;
  badge.textContent = notice.kind === "withdrawal" ? "Retrait" : "Dépôt";
  icon.className = `${notice.iconClass} text-[22px]`;
  title.textContent = notice.title;
  body.textContent = notice.body;
  amount.textContent = notice.amountLabel;
  reasonWrap.classList.toggle("hidden", !notice.reason);
  reasonText.textContent = notice.reason || "";
  setPage2FinanceNoticeOpen(true);
}

function maybeShowNextPage2FinanceNotice() {
  if (page2FinanceNoticeActive) {
    renderPage2FinanceNotice();
    return;
  }
  if (!page2FinanceNoticeQueue.length) {
    setPage2FinanceNoticeOpen(false);
    return;
  }
  page2FinanceNoticeActive = page2FinanceNoticeQueue.shift() || null;
  renderPage2FinanceNotice();
}

async function acknowledgeActivePage2FinanceNotice() {
  const activeNotice = page2FinanceNoticeActive;
  if (!activeNotice) {
    setPage2FinanceNoticeOpen(false);
    return;
  }

  page2FinanceNoticeSessionSeen.add(activeNotice.noticeKey);
  page2FinanceNoticeActive = null;
  setPage2FinanceNoticeOpen(false);

  try {
    await ackClientFinanceNoticeSecure({
      kind: activeNotice.kind,
      id: activeNotice.id,
      status: activeNotice.status,
      noticeKey: activeNotice.noticeKey,
    });
  } catch (error) {
    console.warn("[PAGE2] finance notice ack failed", error);
  } finally {
    rebuildPage2FinanceNoticeQueue();
    maybeShowNextPage2FinanceNotice();
  }
}

function startPage2FinanceNoticeWatchers(user) {
  const uid = String(user?.uid || "");
  stopPage2FinanceNoticeWatchers();
  if (!uid) return;

  page2FinanceNoticeUid = uid;
  const clientRef = doc(db, "clients", uid);
  const ordersRef = collection(clientRef, "orders");
  const withdrawalsRef = collection(clientRef, "withdrawals");

  page2FinanceNoticeUnsubs.push(onSnapshot(ordersRef, (snap) => {
    if (page2FinanceNoticeUid !== String(auth.currentUser?.uid || uid)) return;
    page2FinanceOrderDocs = snap.docs.map((item) => ({ id: item.id, data: item.data() || {} }));
    rebuildPage2FinanceNoticeQueue();
    maybeShowNextPage2FinanceNotice();
  }, (error) => {
    console.error("Erreur écoute notifications dépôts:", error);
  }));

  page2FinanceNoticeUnsubs.push(onSnapshot(withdrawalsRef, (snap) => {
    if (page2FinanceNoticeUid !== String(auth.currentUser?.uid || uid)) return;
    page2FinanceWithdrawalDocs = snap.docs.map((item) => ({ id: item.id, data: item.data() || {} }));
    rebuildPage2FinanceNoticeQueue();
    maybeShowNextPage2FinanceNotice();
  }, (error) => {
    console.error("Erreur écoute notifications retraits:", error);
  }));
}

function consumeAuthSuccessNotice() {
  try {
    const raw = sessionStorage.getItem(AUTH_SUCCESS_NOTICE_STORAGE_KEY) || "";
    if (!raw) return false;
    sessionStorage.removeItem(AUTH_SUCCESS_NOTICE_STORAGE_KEY);
    const parsed = JSON.parse(raw);
    const ts = Number(parsed?.ts || 0);
    if (!Number.isFinite(ts) || ts <= 0) return false;
    return (Date.now() - ts) < 60_000;
  } catch (_) {
    return false;
  }
}

async function touchClientPresence(user) {
  const uid = String(user?.uid || "");
  if (!uid) return;
  try {
    await setDoc(doc(db, "clients", uid), {
      uid,
      email: String(user?.email || ""),
      lastSeenAt: serverTimestamp(),
      lastSeenAtMs: Date.now(),
      updatedAt: serverTimestamp(),
    }, { merge: true });
  } catch (error) {
    console.error("Erreur update presence client:", error);
  }
}

function stopPage2PresenceHeartbeat() {
  if (page2PresenceTick) {
    clearInterval(page2PresenceTick);
    page2PresenceTick = null;
  }
}

function startPage2PresenceHeartbeat(user) {
  const uid = String(user?.uid || "");
  if (!uid) {
    stopPage2PresenceHeartbeat();
    return;
  }
  stopPage2PresenceHeartbeat();
  touchClientPresence(user);
  page2PresenceTick = setInterval(() => {
    if (document.visibilityState !== "visible") return;
    touchClientPresence(page2PresenceUser || user);
  }, PAGE2_PRESENCE_PING_MS);
}

function ensureClientReferralBootstrap(user) {
  const uid = String(user?.uid || "");
  if (!uid) return Promise.resolve(null);
  if (profileBootstrapInFlightByUid.has(uid)) return profileBootstrapInFlightByUid.get(uid);

  const promise = (async () => {
    try {
      const result = await updateClientProfileSecure({});
      const referralCode = String(result?.profile?.referralCode || "").trim();
      if (!referralCode) {
        console.warn("[PROFILE_BOOTSTRAP] referralCode absent apres updateClientProfileSecure", { uid });
      }
      return result;
    } catch (error) {
      console.warn("[PROFILE_BOOTSTRAP] impossible de (re)generer le profil referral", {
        uid,
        code: String(error?.code || ""),
        message: String(error?.message || error),
      });
      return null;
    } finally {
      profileBootstrapInFlightByUid.delete(uid);
    }
  })();

  profileBootstrapInFlightByUid.set(uid, promise);
  return promise;
}

function initDiscussionFab(user) {
  const fabBtn = document.getElementById("discussionFabBtn");
  const badge = document.getElementById("discussionFabBadge");
  if (!fabBtn || !badge) return;

  fabBtn.addEventListener("click", () => {
    showGlobalLoading("Ouverture de la discussion...");
    window.location.href = "./discussion.html";
  });

  const uid = String(user?.uid || "");
  if (!uid) {
    badge.classList.add("hidden");
    return;
  }
  void refreshDiscussionFabState(user);
}

function initAgentSupportAlert(user) {
  const alertWrap = document.getElementById("agentSupportAlertWrap");
  const alertBtn = document.getElementById("agentSupportAlertBtn");
  const alertText = document.getElementById("agentSupportAlertText");
  if (!alertWrap || !alertBtn || !alertText) return;

  if (alertBtn.dataset.bound !== "1") {
    alertBtn.dataset.bound = "1";
    alertBtn.addEventListener("click", () => {
      showGlobalLoading("Ouverture du support...");
      window.location.href = "./discussion-agent.html";
    });
  }

  const uid = String(user?.uid || "");
  if (!uid) {
    alertWrap.classList.add("hidden");
    return;
  }
  void refreshAgentSupportAlertState(user);
}

export function renderPage2(user, options = {}) {
  stopPage2ChatWatchers();
  stopPage2FinanceNoticeWatchers();
  stopPage2HeroRotation();
  if (page2SharePromoCountdownTimer) {
    window.clearInterval(page2SharePromoCountdownTimer);
    page2SharePromoCountdownTimer = null;
  }
  const pageShell = getPage2Shell();
  const runId = ++page2BootstrapRunId;
  page2PresenceUser = user || null;
  const incomingUid = String(page2PresenceUser?.uid || "");
  const currentAuthUid = String(auth.currentUser?.uid || "");
  const hasConfirmedAuth = Boolean(incomingUid && currentAuthUid && incomingUid === currentAuthUid);
  const isOptimisticAuth = options?.optimisticAuth === true && !hasConfirmedAuth && Boolean(incomingUid);
  const isAuthenticated = Boolean(incomingUid);

  if (hasConfirmedAuth) {
    startPage2PresenceHeartbeat(page2PresenceUser);
  } else {
    stopPage2PresenceHeartbeat();
  }

  const headerActions = isAuthenticated
    ? `
                <button id="soldBadge" type="button" class="inline-flex items-center gap-2 rounded-xl border border-white/20 bg-white/10 px-3 py-2 text-xs font-semibold text-white/90 shadow-[inset_4px_4px_10px_rgba(20,28,45,0.42),inset_-4px_-4px_10px_rgba(123,137,180,0.2)] backdrop-blur-md transition hover:bg-white/15">
                  <span class="inline-flex h-5 w-5 items-center justify-center rounded-lg bg-white/20 text-[11px]">+</span>
                  <span class="hidden sm:inline">Faire un dépôt</span>
                  <span class="sm:hidden">Dépôt</span>
                </button>
                <button id="p2Profile" type="button" class="grid h-10 w-10 place-items-center rounded-xl border border-white/20 bg-white/10 text-white/85 shadow-[8px_8px_18px_rgba(22,29,45,0.4),-6px_-6px_14px_rgba(118,131,172,0.25)] backdrop-blur-md transition hover:bg-white/15 hover:text-white sm:h-11 sm:w-11" aria-label="Profil">
                  <i class="fa-regular fa-circle-user text-[18px] sm:text-[19px]"></i>
                </button>
    `
    : `
                <button id="authCtaBtn" type="button" class="inline-flex h-10 items-center rounded-xl border border-[#ffb26e] bg-[#F57C00] px-4 text-xs font-semibold text-white shadow-[8px_8px_18px_rgba(163,82,27,0.45),-6px_-6px_14px_rgba(255,175,102,0.22)] transition hover:-translate-y-0.5 sm:h-11 sm:px-5 sm:text-sm">
                  Connexion / Inscription
                </button>
    `;

  pageShell.innerHTML = `
    <div id="page2Root" class="h-[100dvh] bg-[#3F4766] px-0 pt-0 text-white font-['Poppins'] overflow-hidden">
      <div class="flex h-full w-full flex-col overflow-hidden">
        <section class="relative min-h-0 flex-1 w-full overflow-hidden rounded-none bg-[#3F4766]">
          <img id="page2HeroImage" src="hero.jpg" alt="Hero" width="600" height="600" fetchpriority="high" decoding="async" class="h-full w-full object-contain" style="opacity:1;transition:opacity 700ms ease;object-position:center;" />
          <div class="absolute inset-0"></div>
          <header class="fixed inset-x-0 top-0 z-40 px-3 sm:top-0 sm:px-5">
            <div class="mx-auto flex w-full max-w-[1080px] items-center justify-between px-1 py-1 sm:px-2 sm:py-1.5">
              <div class="flex items-center">
                <img id="p2Logo" src="./logo.png" alt="Logo" width="500" height="500" decoding="async" class="h-auto w-[96px] max-w-full object-contain sm:w-[148px]" />
                <span id="p2LogoFallback" class="hidden text-2xl font-semibold tracking-tight text-white/95">Dominoes</span>
              </div>
              <div class="flex items-center gap-2 sm:gap-3">
                ${headerActions}
              </div>
            </div>
          </header>
        </section>

        <section class="flex shrink-0 justify-center px-6 pb-[max(1rem,env(safe-area-inset-bottom))] pt-4 sm:pt-6">
          <div class="flex w-full max-w-[780px] flex-col items-center gap-3">
            <div id="page2FrozenBanner" class="hidden w-full rounded-[18px] border border-[#ff7c7c]/30 bg-[#6f1d1b]/38 px-4 py-3 text-sm text-[#ffe0df] shadow-[8px_8px_18px_rgba(53,15,14,0.35),-6px_-6px_14px_rgba(137,64,61,0.12)] backdrop-blur-md">
              <p id="page2FrozenBannerText" class="leading-6">Ton compte a été temporairement gelé après plusieurs dépôts refusés. Contacte l'assistance.</p>
            </div>
            <button id="startGameBtn" type="button" class="h-14 w-full rounded-[18px] border border-[#ffb26e] bg-[#F57C00] px-8 text-base font-semibold text-white shadow-[9px_9px_20px_rgba(155,78,25,0.45),-7px_-7px_16px_rgba(255,173,96,0.2)] transition hover:-translate-y-0.5">
                LANCER UNE PARTIE
            </button>
            <button id="tournamentBtn" type="button" class="h-12 w-full rounded-[16px] border border-white/25 bg-white/10 px-8 text-sm font-semibold text-white shadow-[8px_8px_18px_rgba(22,29,45,0.35),-6px_-6px_14px_rgba(118,131,172,0.2)] backdrop-blur-md transition hover:-translate-y-0.5 hover:bg-white/15">
              Tournois
            </button>
            <button id="sharePromoBtn" type="button" class="flex min-h-[56px] w-full items-center justify-between gap-2 rounded-[16px] border border-white/25 bg-white/10 px-4 py-3 text-left text-white shadow-[8px_8px_18px_rgba(22,29,45,0.35),-6px_-6px_14px_rgba(118,131,172,0.2)] backdrop-blur-md transition hover:-translate-y-0.5 hover:bg-white/15 sm:gap-3 sm:px-5">
              <span class="flex min-w-0 items-center gap-3">
                <span class="grid h-10 w-10 shrink-0 place-items-center rounded-2xl border border-white/15 bg-white/10 text-[18px] text-white/90">
                  <i class="fa-solid fa-share-nodes"></i>
                </span>
                <span class="min-w-0">
                  <span id="sharePromoBtnTitle" class="block truncate text-[13px] font-semibold leading-tight text-white sm:text-sm">Partager et gagner 100 Does</span>
                  <span id="sharePromoBtnMeta" class="hidden truncate text-xs text-white/68 sm:block">5 partages valides pour debloquer le bonus.</span>
                </span>
              </span>
              <span id="sharePromoBtnBadge" class="shrink-0 rounded-full border border-[#ffb26e]/35 bg-[#F57C00]/16 px-2.5 py-1 text-[11px] font-semibold text-[#ffd5ae] sm:px-3">0/5</span>
            </button>
          </div>
        </section>
      </div>
    </div>
  `;

  if (!page2PresenceVisibilityBound) {
    page2PresenceVisibilityBound = true;
    document.addEventListener("visibilitychange", () => {
      if (document.visibilityState === "visible") {
        const targetUid = String(page2PresenceUser?.uid || "");
        const currentUid = String(auth.currentUser?.uid || "");
        if (targetUid && currentUid && targetUid === currentUid) {
          touchClientPresence(page2PresenceUser);
        }
      }
    });
  }

  pageShell.insertAdjacentHTML("beforeend", `
    <div id="tournamentIntroOverlay" class="fixed inset-0 z-[3445] hidden items-end justify-center bg-[#12192b]/60 p-0 backdrop-blur-sm sm:items-center sm:p-4">
      <div id="tournamentIntroPanel" class="w-full rounded-t-[30px] border border-white/15 bg-[linear-gradient(180deg,rgba(82,94,132,0.98),rgba(55,65,95,0.98))] px-5 pb-[max(1.25rem,env(safe-area-inset-bottom))] pt-5 text-white shadow-[0_-16px_38px_rgba(12,18,31,0.42)] sm:max-w-xl sm:rounded-[30px] sm:border-white/20 sm:px-6 sm:pb-6 sm:pt-6 sm:shadow-[14px_14px_34px_rgba(16,23,40,0.5),-10px_-10px_24px_rgba(112,126,165,0.2)]">
        <div class="mx-auto mb-4 flex h-11 w-11 items-center justify-center rounded-2xl border border-[#ffd3aa]/35 bg-[#F57C00]/20 text-[#ffd9b8] shadow-[inset_4px_4px_10px_rgba(20,28,45,0.42),inset_-4px_-4px_10px_rgba(123,137,180,0.18)] sm:mx-0 sm:mb-5 sm:h-12 sm:w-12">
          <i class="fa-solid fa-trophy text-lg"></i>
        </div>
        <h3 class="text-[1.35rem] font-bold leading-tight sm:text-[1.55rem]">Bienvenue dans les tournois</h3>
        <p class="mt-2 text-sm leading-6 text-white/82 sm:text-[15px]">
          Quand tu cliques sur <span class="font-semibold text-white">Tournois</span>, un tournoi se lance automatiquement pour toi. Tous les joueurs qui cliquent dans la même période rejoignent ce tournoi.
        </p>
        <div class="mt-4 space-y-3 rounded-[24px] border border-white/12 bg-white/8 p-4 shadow-[inset_4px_4px_10px_rgba(20,28,45,0.28),inset_-4px_-4px_10px_rgba(123,137,180,0.08)]">
          <p class="text-sm leading-6 text-white/88">
            Pour gagner, tu dois finir <span class="font-semibold text-white">premier</span> avec le plus de victoires.
          </p>
          <p class="text-sm leading-6 text-white/88">
            Si tu gagnes, <span class="font-semibold text-white">10 000 Does</span> seront ajoutés à ton compte et tu pourras les retirer quand tu veux.
          </p>
          <p class="text-sm leading-6 text-white/88">
            Pour protéger les joueurs, les usernames sont cachés et seul leur <span class="font-semibold text-white">ID</span> est affiché. C’est valable pour toi aussi, mais tu verras toujours ta place dans le classement.
          </p>
        </div>
        <div class="mt-4 rounded-[24px] border border-[#ffb26e]/22 bg-[#F57C00]/10 p-4">
          <p class="text-sm leading-6 text-white/90">
            Chaque tournoi dure <span class="font-semibold text-white">15 minutes</span>. Tu as droit à <span class="font-semibold text-white">3 tournois par jour</span>. Ces règles servent à mieux réguler le volume des tournois vu le nombre de joueurs.
          </p>
        </div>
        <button id="tournamentIntroContinueBtn" type="button" class="mt-5 h-12 w-full rounded-[18px] border border-[#ffb26e] bg-[#F57C00] text-sm font-semibold text-white shadow-[9px_9px_20px_rgba(155,78,25,0.45),-7px_-7px_16px_rgba(255,173,96,0.2)] transition hover:-translate-y-0.5 sm:mt-6 sm:h-12 sm:text-[15px]">
          Continuer
        </button>
      </div>
    </div>
  `);

  pageShell.insertAdjacentHTML("beforeend", `
    <div id="sharePromoOverlay" class="fixed inset-0 z-[3455] hidden items-end justify-center bg-[#12192b]/65 px-[max(12px,env(safe-area-inset-left))] pb-[max(12px,env(safe-area-inset-bottom))] pt-[max(12px,env(safe-area-inset-top))] backdrop-blur-sm sm:items-center sm:px-4 sm:py-4">
      <div id="sharePromoPanel" class="max-h-full w-full overflow-y-auto rounded-[28px] border border-white/15 bg-[linear-gradient(180deg,rgba(82,94,132,0.98),rgba(55,65,95,0.98))] px-4 pb-[max(1rem,env(safe-area-inset-bottom))] pt-4 text-white shadow-[0_-16px_38px_rgba(12,18,31,0.42)] sm:max-h-[min(88vh,760px)] sm:max-w-lg sm:rounded-[30px] sm:border-white/20 sm:px-6 sm:pb-6 sm:pt-6 sm:shadow-[14px_14px_34px_rgba(16,23,40,0.5),-10px_-10px_24px_rgba(112,126,165,0.2)]">
        <div class="flex items-start justify-between gap-3">
          <div class="min-w-0 pr-2">
            <p class="text-xs font-semibold uppercase tracking-[0.24em] text-[#ffd4ab]/80">Bonus partage</p>
            <h3 class="mt-2 text-[1.2rem] font-bold leading-tight sm:text-[1.55rem]">Partage le site et gagne 100 Does</h3>
          </div>
          <button id="sharePromoCloseBtn" type="button" class="grid h-10 w-10 shrink-0 place-items-center rounded-full border border-white/20 bg-white/10 text-white">
            <i class="fa-solid fa-xmark"></i>
          </button>
        </div>
        <p class="mt-3 text-[13px] leading-6 text-white/84 sm:text-sm">
          Clique sur <span class="font-semibold text-white">Partager le site</span> 5 fois pour remplir la barre. A la fin, tu recois <span class="font-semibold text-white">100 Does</span> en bonus.
        </p>
        <p class="mt-2 text-xs leading-5 text-white/62">
          Ce bonus suit les regles bonus du wallet et doit etre joue avant une reconversion.
        </p>
        <div class="mt-4 rounded-[24px] border border-white/12 bg-white/8 p-4 shadow-[inset_4px_4px_10px_rgba(20,28,45,0.28),inset_-4px_-4px_10px_rgba(123,137,180,0.08)]">
          <div class="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between sm:gap-3">
            <p id="sharePromoProgressText" class="text-sm font-semibold text-white">0/5 partages</p>
            <span id="sharePromoRewardBadge" class="inline-flex w-fit rounded-full border border-[#ffb26e]/35 bg-[#F57C00]/16 px-3 py-1 text-[11px] font-semibold text-[#ffd5ae]">100 Does</span>
          </div>
          <div class="mt-3 h-3 overflow-hidden rounded-full bg-black/20">
            <div id="sharePromoProgressBar" class="h-full w-0 rounded-full bg-[linear-gradient(90deg,#f57c00,#ffb26e)] transition-[width] duration-300 ease-out"></div>
          </div>
          <p id="sharePromoStatusText" class="mt-3 text-sm leading-6 text-white/82">Partage le site 5 fois pour debloquer ton bonus.</p>
          <p id="sharePromoCooldownText" class="mt-1 text-xs text-white/60"></p>
        </div>
        <div class="mt-5 rounded-[24px] border border-white/12 bg-white/8 p-4 shadow-[inset_4px_4px_10px_rgba(20,28,45,0.24),inset_-4px_-4px_10px_rgba(123,137,180,0.08)]">
          <p class="text-xs font-semibold uppercase tracking-[0.18em] text-white/58">Choisis une application</p>
          <div id="sharePromoTargetGrid" class="mt-3 grid grid-cols-4 gap-2 sm:gap-3"></div>
          <p id="sharePromoPendingText" class="mt-3 text-xs leading-5 text-white/62">
            Choisis une application, partage le lien, puis reviens ici pour valider ton partage.
          </p>
          <button id="sharePromoConfirmBtn" type="button" class="mt-3 inline-flex h-11 w-full items-center justify-center gap-2 rounded-[16px] border border-white/18 bg-white/10 text-sm font-semibold text-white/78 transition hover:bg-white/15 disabled:cursor-not-allowed disabled:opacity-55" disabled>
            <i class="fa-solid fa-check"></i>
            <span id="sharePromoConfirmBtnLabel">Valider ce partage</span>
          </button>
        </div>
        <p class="mt-3 text-center text-xs leading-5 text-white/62">
          Le bonus revient une fois tous les 3 jours apres validation complete.
        </p>
      </div>
    </div>
  `);

  pageShell.insertAdjacentHTML("beforeend", `
    <div id="sharePromoSuccessOverlay" class="fixed inset-0 z-[3458] hidden items-end justify-center bg-[#12192b]/70 px-[max(12px,env(safe-area-inset-left))] pb-[max(12px,env(safe-area-inset-bottom))] pt-[max(12px,env(safe-area-inset-top))] backdrop-blur-sm sm:items-center sm:px-4 sm:py-4">
      <div id="sharePromoSuccessPanel" class="w-full rounded-[28px] border border-[#ffb26e]/30 bg-[linear-gradient(180deg,rgba(86,101,142,0.98),rgba(57,67,99,0.98))] px-4 pb-[max(1rem,env(safe-area-inset-bottom))] pt-5 text-white shadow-[0_-16px_38px_rgba(12,18,31,0.42)] sm:max-w-md sm:rounded-[30px] sm:border-white/20 sm:px-6 sm:pb-6 sm:pt-6 sm:shadow-[14px_14px_34px_rgba(16,23,40,0.5),-10px_-10px_24px_rgba(112,126,165,0.2)]">
        <div class="mx-auto flex h-14 w-14 items-center justify-center rounded-[20px] border border-[#ffcf9f]/45 bg-[#F57C00]/22 text-[#ffe1c4] shadow-[inset_4px_4px_10px_rgba(20,28,45,0.42),inset_-4px_-4px_10px_rgba(123,137,180,0.18)]">
          <i class="fa-solid fa-gift text-xl"></i>
        </div>
        <h3 class="mt-4 text-center text-[1.28rem] font-bold leading-tight sm:text-[1.45rem]">Bonus recu avec succes</h3>
        <p id="sharePromoSuccessMessage" class="mt-3 text-center text-sm leading-6 text-white/88">
          Tu as gagne avec succes 100 Does.
        </p>
        <div class="mt-4 rounded-[22px] border border-white/12 bg-white/8 p-4 text-center shadow-[inset_4px_4px_10px_rgba(20,28,45,0.28),inset_-4px_-4px_10px_rgba(123,137,180,0.08)]">
          <p class="text-xs font-semibold uppercase tracking-[0.18em] text-white/56">Prochain bonus</p>
          <p id="sharePromoSuccessCooldown" class="mt-2 text-sm font-semibold text-[#ffd7b2]">Disponible de nouveau dans 3 jours</p>
        </div>
        <button id="sharePromoSuccessCloseBtn" type="button" class="mt-5 h-12 w-full rounded-[18px] border border-[#ffb26e] bg-[#F57C00] text-sm font-semibold text-white shadow-[9px_9px_20px_rgba(155,78,25,0.45),-7px_-7px_16px_rgba(255,173,96,0.2)] transition hover:-translate-y-0.5">
          Compris
        </button>
      </div>
    </div>
  `);

  pageShell.insertAdjacentHTML("beforeend", `
    <div id="financeNoticeOverlay" class="fixed inset-0 z-[3459] hidden items-end justify-center bg-[#12192b]/72 px-[max(12px,env(safe-area-inset-left))] pb-[max(12px,env(safe-area-inset-bottom))] pt-[max(12px,env(safe-area-inset-top))] backdrop-blur-sm sm:items-center sm:px-4 sm:py-4">
      <div id="financeNoticePanel" class="w-full rounded-[28px] border border-white/15 bg-[linear-gradient(180deg,rgba(82,94,132,0.98),rgba(55,65,95,0.98))] px-4 pb-[max(1rem,env(safe-area-inset-bottom))] pt-5 text-white shadow-[0_-16px_38px_rgba(12,18,31,0.42)] sm:max-w-md sm:rounded-[30px] sm:border-white/20 sm:px-6 sm:pb-6 sm:pt-6 sm:shadow-[14px_14px_34px_rgba(16,23,40,0.5),-10px_-10px_24px_rgba(112,126,165,0.2)]">
        <div class="mx-auto flex h-14 w-14 items-center justify-center rounded-[20px] border border-white/18 bg-white/10 text-[#ffe1c4] shadow-[inset_4px_4px_10px_rgba(20,28,45,0.42),inset_-4px_-4px_10px_rgba(123,137,180,0.18)]">
          <i id="financeNoticeIcon" class="fa-solid fa-badge-check text-[22px]"></i>
        </div>
        <div class="mt-4 flex justify-center">
          <span id="financeNoticeBadge" class="inline-flex w-fit rounded-full border border-emerald-300/35 bg-emerald-500/18 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.18em] text-emerald-100">Dépôt</span>
        </div>
        <h3 id="financeNoticeTitle" class="mt-4 text-center text-[1.28rem] font-bold leading-tight sm:text-[1.45rem]">Ton dépôt est approuvé</h3>
        <p id="financeNoticeBody" class="mt-3 text-center text-sm leading-6 text-white/88">Ton opération a bien été traitée.</p>
        <div class="mt-4 rounded-[22px] border border-white/12 bg-white/8 p-4 text-center shadow-[inset_4px_4px_10px_rgba(20,28,45,0.28),inset_-4px_-4px_10px_rgba(123,137,180,0.08)]">
          <p class="text-xs font-semibold uppercase tracking-[0.18em] text-white/56">Montant</p>
          <p id="financeNoticeAmount" class="mt-2 text-lg font-semibold text-[#ffd7b2]">0 HTG</p>
        </div>
        <div id="financeNoticeReasonWrap" class="mt-4 hidden rounded-[22px] border border-white/12 bg-white/8 p-4 text-left shadow-[inset_4px_4px_10px_rgba(20,28,45,0.28),inset_-4px_-4px_10px_rgba(123,137,180,0.08)]">
          <p class="text-xs font-semibold uppercase tracking-[0.18em] text-white/56">Détail</p>
          <p id="financeNoticeReasonText" class="mt-2 text-sm leading-6 text-white/82"></p>
        </div>
        <button id="financeNoticeCloseBtn" type="button" class="mt-5 h-12 w-full rounded-[18px] border border-[#ffb26e] bg-[#F57C00] text-sm font-semibold text-white shadow-[9px_9px_20px_rgba(155,78,25,0.45),-7px_-7px_16px_rgba(255,173,96,0.2)] transition hover:-translate-y-0.5">
          Compris
        </button>
      </div>
    </div>
  `);

  pageShell.insertAdjacentHTML("beforeend", `
    <div id="surveyPromptOverlay" class="fixed inset-0 z-[3461] hidden items-end justify-center bg-[#12192b]/72 px-[max(12px,env(safe-area-inset-left))] pb-[max(12px,env(safe-area-inset-bottom))] pt-[max(12px,env(safe-area-inset-top))] backdrop-blur-sm sm:items-center sm:px-4 sm:py-4">
      <div id="surveyPromptPanel" class="max-h-full w-full overflow-y-auto rounded-[28px] border border-white/15 bg-[linear-gradient(180deg,rgba(82,94,132,0.98),rgba(55,65,95,0.98))] px-4 pb-[max(1rem,env(safe-area-inset-bottom))] pt-4 text-white shadow-[0_-16px_38px_rgba(12,18,31,0.42)] sm:max-h-[min(88vh,760px)] sm:max-w-lg sm:rounded-[30px] sm:border-white/20 sm:px-6 sm:pb-6 sm:pt-6 sm:shadow-[14px_14px_34px_rgba(16,23,40,0.5),-10px_-10px_24px_rgba(112,126,165,0.2)]">
        <div class="flex items-start justify-between gap-3">
          <div class="min-w-0 pr-2">
            <p class="text-xs font-semibold uppercase tracking-[0.24em] text-[#ffd4ab]/80">Sondage joueur</p>
            <h3 id="surveyPromptTitle" class="mt-2 text-[1.2rem] font-bold leading-tight sm:text-[1.55rem]">Ton avis nous aide</h3>
          </div>
          <button id="surveyPromptCloseBtn" type="button" class="grid h-10 w-10 shrink-0 place-items-center rounded-full border border-white/20 bg-white/10 text-white">
            <i class="fa-solid fa-xmark"></i>
          </button>
        </div>
        <p id="surveyPromptDescription" class="mt-3 text-[13px] leading-6 text-white/84 sm:text-sm"></p>
        <div id="surveyPromptChoices" class="mt-4 grid gap-2"></div>
        <div id="surveyPromptTextWrap" class="mt-4 hidden">
          <label for="surveyPromptTextInput" class="mb-2 block text-sm font-semibold text-white/88">Ta réponse</label>
          <textarea id="surveyPromptTextInput" rows="4" maxlength="500" class="w-full rounded-[20px] border border-white/16 bg-white/8 px-4 py-3 text-sm text-white outline-none placeholder:text-white/45" placeholder="Ecris ici ce que tu veux nous dire..."></textarea>
        </div>
        <p id="surveyPromptStatus" class="mt-3 min-h-[20px] text-sm text-[#ffd0d8]"></p>
        <div class="mt-5 grid grid-cols-1 gap-3 sm:grid-cols-2">
          <button id="surveyPromptDismissBtn" type="button" class="h-11 rounded-[18px] border border-white/18 bg-white/10 text-sm font-semibold text-white transition hover:bg-white/15">
            Plus tard
          </button>
          <button id="surveyPromptSubmitBtn" type="button" class="h-11 rounded-[18px] border border-[#ffb26e] bg-[#F57C00] text-sm font-semibold text-white shadow-[9px_9px_20px_rgba(155,78,25,0.45),-7px_-7px_16px_rgba(255,173,96,0.2)] transition hover:-translate-y-0.5">
            Envoyer ma réponse
          </button>
        </div>
      </div>
    </div>
  `);

  pageShell.insertAdjacentHTML("beforeend", `
    <div id="doesRequiredOverlay" class="fixed inset-0 z-[3450] hidden items-center justify-center bg-black/50 p-4 backdrop-blur-sm">
      <div class="w-full max-w-md rounded-3xl border border-white/20 bg-[#3F4766]/75 p-5 text-white shadow-[14px_14px_34px_rgba(16,23,40,0.5),-10px_-10px_24px_rgba(112,126,165,0.2)] backdrop-blur-xl sm:p-6">
        <h3 class="text-xl font-bold">Solde Does insuffisant</h3>
        <p class="mt-2 text-sm text-white/85">
          Tu n'as pas assez de Does pour démarrer une partie.
        </p>
        <p class="mt-2 text-sm text-white/85">
          Pour jouer, ouvre ton profil puis clique sur <span class="font-semibold text-white">Xchange en crypto</span> pour convertir ton argent en Does.
        </p>
        <div class="mt-5 grid grid-cols-1 gap-3 sm:grid-cols-2">
          <button id="doesRequiredOpenProfile" type="button" class="h-11 rounded-2xl border border-[#ffb26e] bg-[#F57C00] text-sm font-semibold text-white shadow-[8px_8px_18px_rgba(163,82,27,0.45),-6px_-6px_14px_rgba(255,175,102,0.22)] transition hover:-translate-y-0.5">
            Ouvrir profil
          </button>
          <button id="doesRequiredClose" type="button" class="h-11 rounded-2xl border border-white/20 bg-white/10 text-sm font-semibold text-white">
            Fermer
          </button>
        </div>
      </div>
    </div>
  `);

  pageShell.insertAdjacentHTML("beforeend", `
    <div id="stakeSelectionOverlay" class="fixed inset-0 z-[3460] hidden items-center justify-center bg-black/55 p-4 backdrop-blur-sm">
      <div id="stakeSelectionPanel" class="w-full max-w-lg rounded-3xl border border-white/20 bg-[#3F4766]/80 p-5 text-white shadow-[14px_14px_34px_rgba(16,23,40,0.5),-10px_-10px_24px_rgba(112,126,165,0.2)] backdrop-blur-xl sm:p-6">
        <div class="flex items-center justify-between gap-3">
          <h3 class="text-xl font-bold">Choisis ta mise</h3>
          <button id="stakeSelectionClose" type="button" class="grid h-10 w-10 place-items-center rounded-full border border-white/20 bg-white/10 text-white">
            <i class="fa-solid fa-xmark"></i>
          </button>
        </div>
        <p class="mt-2 text-sm text-white/90">
          Quand vous cliquez sur un des boutons, le jeu débute et la mise sélectionnée est automatiquement pariée selon la configuration active.
        </p>
        <div id="stakeOptionsGrid" class="mt-5 grid grid-cols-2 gap-3">
          <div class="col-span-2 rounded-2xl border border-white/15 bg-white/5 px-4 py-4 text-sm text-white/70">
            Chargement des mises...
          </div>
        </div>
        <div class="mt-4 border-t border-white/10 pt-4">
          <button id="playWithFriendsBtn" type="button" class="flex h-12 w-full items-center justify-center gap-2 rounded-2xl border border-white/20 bg-white/10 text-sm font-semibold text-white transition hover:-translate-y-0.5 hover:bg-white/15">
            <i class="fa-solid fa-user-group text-[15px]"></i>
            <span>Jouer avec des amis</span>
          </button>
          <p class="mt-2 text-center text-xs text-white/62">Crée une salle privée, copie un code et invite 3 amis.</p>
        </div>
      </div>
    </div>
  `);

  pageShell.insertAdjacentHTML("beforeend", `
    <div id="friendModeOverlay" class="fixed inset-0 z-[3462] hidden items-center justify-center bg-black/55 p-4 backdrop-blur-sm">
      <div id="friendModePanel" class="w-full max-w-md rounded-3xl border border-white/20 bg-[#3F4766]/82 p-5 text-white shadow-[14px_14px_34px_rgba(16,23,40,0.5),-10px_-10px_24px_rgba(112,126,165,0.2)] backdrop-blur-xl sm:p-6">
        <div class="flex items-center justify-between gap-3">
          <div>
            <p class="text-xs font-semibold uppercase tracking-[0.2em] text-[#ffd4ab]/80">Partie entre amis</p>
            <h3 class="mt-2 text-xl font-bold">Choisis une option</h3>
          </div>
          <button id="friendModeClose" type="button" class="grid h-10 w-10 place-items-center rounded-full border border-white/20 bg-white/10 text-white">
            <i class="fa-solid fa-xmark"></i>
          </button>
        </div>
        <div class="mt-5 space-y-3">
          <button id="friendJoinOpenBtn" type="button" class="flex min-h-[58px] w-full items-center justify-between gap-3 rounded-2xl border border-white/18 bg-white/10 px-4 py-3 text-left transition hover:bg-white/15">
            <span>
              <span class="block text-sm font-semibold text-white">J'ai ete invite</span>
              <span class="mt-1 block text-xs text-white/66">Entre un code recu pour rejoindre directement la salle.</span>
            </span>
            <i class="fa-solid fa-arrow-right text-white/70"></i>
          </button>
          <button id="friendCreateOpenBtn" type="button" class="flex min-h-[58px] w-full items-center justify-between gap-3 rounded-2xl border border-[#ffb26e]/35 bg-[#F57C00]/14 px-4 py-3 text-left transition hover:bg-[#F57C00]/18">
            <span>
              <span class="block text-sm font-semibold text-white">Creer une partie</span>
              <span class="mt-1 block text-xs text-white/70">Choisis la mise, genere un code et invite tes amis.</span>
            </span>
            <i class="fa-solid fa-plus text-[#ffd8b5]"></i>
          </button>
        </div>
      </div>
    </div>
  `);

  pageShell.insertAdjacentHTML("beforeend", `
    <div id="friendCreateOverlay" class="fixed inset-0 z-[3463] hidden items-center justify-center bg-black/55 p-4 backdrop-blur-sm">
      <div id="friendCreatePanel" class="w-full max-w-lg rounded-3xl border border-white/20 bg-[#3F4766]/82 p-5 text-white shadow-[14px_14px_34px_rgba(16,23,40,0.5),-10px_-10px_24px_rgba(112,126,165,0.2)] backdrop-blur-xl sm:p-6">
        <div class="flex items-center justify-between gap-3">
          <div>
            <p class="text-xs font-semibold uppercase tracking-[0.2em] text-[#ffd4ab]/80">Creer une partie</p>
            <h3 class="mt-2 text-xl font-bold">Choisis la mise obligatoire</h3>
          </div>
          <button id="friendCreateClose" type="button" class="grid h-10 w-10 place-items-center rounded-full border border-white/20 bg-white/10 text-white">
            <i class="fa-solid fa-xmark"></i>
          </button>
        </div>
        <p class="mt-3 text-sm leading-6 text-white/84">La même mise sera prélevée quand la salle sera complète et prête à démarrer.</p>
        <div id="friendCreateStakeGrid" class="mt-5 grid grid-cols-2 gap-3">
          <div class="col-span-2 rounded-2xl border border-white/15 bg-white/5 px-4 py-4 text-sm text-white/70">
            Chargement des mises...
          </div>
        </div>
      </div>
    </div>
  `);

  pageShell.insertAdjacentHTML("beforeend", `
    <div id="friendJoinOverlay" class="fixed inset-0 z-[3464] hidden items-center justify-center bg-black/55 p-4 backdrop-blur-sm">
      <div id="friendJoinPanel" class="w-full max-w-md rounded-3xl border border-white/20 bg-[#3F4766]/82 p-5 text-white shadow-[14px_14px_34px_rgba(16,23,40,0.5),-10px_-10px_24px_rgba(112,126,165,0.2)] backdrop-blur-xl sm:p-6">
        <div class="flex items-center justify-between gap-3">
          <div>
            <p class="text-xs font-semibold uppercase tracking-[0.2em] text-[#ffd4ab]/80">Rejoindre une salle</p>
            <h3 class="mt-2 text-xl font-bold">Entre le code d'invitation</h3>
          </div>
          <button id="friendJoinClose" type="button" class="grid h-10 w-10 place-items-center rounded-full border border-white/20 bg-white/10 text-white">
            <i class="fa-solid fa-xmark"></i>
          </button>
        </div>
        <p class="mt-3 text-sm leading-6 text-white/84">Le code est fourni par ton ami créateur de salle.</p>
        <label for="friendJoinCodeInput" class="mt-4 block text-xs font-semibold uppercase tracking-[0.16em] text-white/58">Code de salle</label>
        <input id="friendJoinCodeInput" type="text" inputmode="text" autocomplete="off" autocapitalize="characters" maxlength="12" class="mt-2 h-12 w-full rounded-2xl border border-white/18 bg-white/10 px-4 text-base font-semibold tracking-[0.3em] text-white outline-none placeholder:text-white/38 focus:border-[#ffb26e]/45 focus:bg-white/12" placeholder="ABC123" />
        <p id="friendJoinHint" class="mt-2 min-h-[1.2rem] text-xs text-white/62">Entre le code exactement comme il t'a ete envoye.</p>
        <button id="friendJoinSubmitBtn" type="button" class="mt-5 h-12 w-full rounded-[18px] border border-[#ffb26e] bg-[#F57C00] text-sm font-semibold text-white shadow-[9px_9px_20px_rgba(155,78,25,0.45),-7px_-7px_16px_rgba(255,173,96,0.2)] transition hover:-translate-y-0.5">
          Rejoindre maintenant
        </button>
      </div>
    </div>
  `);

  pageShell.insertAdjacentHTML("beforeend", `
    <div id="friendCodeOverlay" class="fixed inset-0 z-[3465] hidden items-center justify-center bg-black/60 p-4 backdrop-blur-sm">
      <div id="friendCodePanel" class="w-full max-w-md rounded-3xl border border-white/20 bg-[#3F4766]/86 p-5 text-white shadow-[14px_14px_34px_rgba(16,23,40,0.5),-10px_-10px_24px_rgba(112,126,165,0.2)] backdrop-blur-xl sm:p-6">
        <div class="mx-auto flex h-14 w-14 items-center justify-center rounded-[20px] border border-[#ffcf9f]/45 bg-[#F57C00]/22 text-[#ffe1c4] shadow-[inset_4px_4px_10px_rgba(20,28,45,0.42),inset_-4px_-4px_10px_rgba(123,137,180,0.18)]">
          <i class="fa-solid fa-key text-xl"></i>
        </div>
        <h3 class="mt-4 text-center text-[1.28rem] font-bold leading-tight">Code de salle genere</h3>
        <p class="mt-3 text-center text-sm leading-6 text-white/86">Copie le code et envoie-le a tes amis pour qu'ils accedent au jeu.</p>
        <div class="mt-4 rounded-[24px] border border-white/12 bg-white/8 p-4 text-center shadow-[inset_4px_4px_10px_rgba(20,28,45,0.28),inset_-4px_-4px_10px_rgba(123,137,180,0.08)]">
          <p class="text-xs font-semibold uppercase tracking-[0.18em] text-white/56">Code prive</p>
          <p id="friendCodeValue" class="mt-2 text-[1.8rem] font-bold tracking-[0.28em] text-[#ffd7b2]">------</p>
          <p id="friendCodeStakeMeta" class="mt-2 text-sm text-white/70"></p>
        </div>
        <button id="friendCodeCopyBtn" type="button" class="mt-4 h-12 w-full rounded-[18px] border border-white/20 bg-white/10 text-sm font-semibold text-white transition hover:-translate-y-0.5 hover:bg-white/15">
          Copier le code
        </button>
        <button id="friendCodeContinueBtn" type="button" class="mt-3 h-12 w-full rounded-[18px] border border-[#ffb26e] bg-[#F57C00] text-sm font-semibold text-white shadow-[9px_9px_20px_rgba(155,78,25,0.45),-7px_-7px_16px_rgba(255,173,96,0.2)] transition hover:-translate-y-0.5">
          Oui, j'ai copie et envoye
        </button>
        <button id="friendCodeCloseBtn" type="button" class="mt-3 h-11 w-full rounded-2xl border border-white/20 bg-white/10 text-sm font-semibold text-white">
          Fermer
        </button>
      </div>
    </div>
  `);

  pageShell.insertAdjacentHTML("beforeend", `
    <div id="stakeUnavailableOverlay" class="fixed inset-0 z-[3470] hidden items-center justify-center bg-black/55 p-4 backdrop-blur-sm">
      <div id="stakeUnavailablePanel" class="w-full max-w-sm rounded-3xl border border-white/20 bg-[#3F4766]/82 p-5 text-white shadow-[14px_14px_34px_rgba(16,23,40,0.5),-10px_-10px_24px_rgba(112,126,165,0.2)] backdrop-blur-xl sm:p-6">
        <h3 class="text-lg font-bold">Pas encore disponible</h3>
        <p class="mt-2 text-sm text-white/90">Cette mise sera activée prochainement.</p>
        <button id="stakeUnavailableClose" type="button" class="mt-4 h-11 w-full rounded-2xl border border-[#ffb26e] bg-[#F57C00] text-sm font-semibold text-white shadow-[8px_8px_18px_rgba(163,82,27,0.45),-6px_-6px_14px_rgba(255,175,102,0.22)] transition hover:-translate-y-0.5">
          Compris
        </button>
      </div>
    </div>
  `);

  pageShell.insertAdjacentHTML("beforeend", `
    <div class="fixed bottom-4 left-4 z-[3390]">
      <button id="discussionFabBtn" type="button" class="relative grid h-14 w-14 place-items-center rounded-full border border-white/25 bg-[#3F4766]/75 text-white shadow-[10px_10px_22px_rgba(16,23,40,0.45),-8px_-8px_18px_rgba(112,126,165,0.2)] backdrop-blur-xl transition hover:-translate-y-0.5" aria-label="Ouvrir la discussion">
        <i class="fa-solid fa-comments text-xl"></i>
        <span id="discussionFabBadge" class="hidden absolute -right-1 -top-1 min-w-[1.3rem] rounded-full border border-red-200/60 bg-red-500 px-1 py-0.5 text-[11px] font-bold leading-none text-white">1</span>
      </button>
    </div>
  `);

  pageShell.insertAdjacentHTML("beforeend", `
    <div id="agentSupportAlertWrap" class="hidden fixed bottom-4 right-4 z-[3395]">
      <button
        id="agentSupportAlertBtn"
        type="button"
        class="flex max-w-[min(86vw,360px)] items-start gap-3 rounded-2xl border border-[#7b9cff]/35 bg-[#20324d]/88 px-4 py-3 text-left text-white shadow-[12px_12px_28px_rgba(16,23,40,0.45),-8px_-8px_18px_rgba(88,116,173,0.16)] backdrop-blur-xl transition hover:-translate-y-0.5"
      >
        <span class="mt-0.5 grid h-9 w-9 flex-none place-items-center rounded-xl bg-[#4a76ff]/25 text-[#dfe8ff]">
          <i class="fa-solid fa-envelope-open-text"></i>
        </span>
        <span id="agentSupportAlertText" class="text-sm leading-5">Vous avez recu un message par un agent.</span>
      </button>
    </div>
  `);

  if (consumeAuthSuccessNotice()) {
    pageShell.insertAdjacentHTML("beforeend", `
      <div id="authSuccessToast" class="fixed top-4 left-1/2 z-[3500] -translate-x-1/2 rounded-2xl border border-emerald-300/40 bg-emerald-500/20 px-4 py-3 text-sm font-semibold text-emerald-100 shadow-[10px_10px_22px_rgba(14,36,28,0.45),-8px_-8px_18px_rgba(91,153,126,0.16)] backdrop-blur-xl">
        Connexion réussie.
      </div>
    `);
    window.setTimeout(() => {
      const toast = document.getElementById("authSuccessToast");
      if (toast) toast.remove();
    }, 2600);
  }

  void runPage2Animations();
  initPage2HeroRotation();

  const logo = document.getElementById("p2Logo");
  const logoFallback = document.getElementById("p2LogoFallback");
  const authCtaBtn = document.getElementById("authCtaBtn");
  const profileBtn = document.getElementById("p2Profile");
  const soldBadgeBtn = document.getElementById("soldBadge");
  const startGameBtn = document.getElementById("startGameBtn");
  const tournamentIntroOverlay = document.getElementById("tournamentIntroOverlay");
  const tournamentIntroPanel = document.getElementById("tournamentIntroPanel");
  const tournamentIntroContinueBtn = document.getElementById("tournamentIntroContinueBtn");
  const doesRequiredOverlay = document.getElementById("doesRequiredOverlay");
  const doesRequiredOpenProfile = document.getElementById("doesRequiredOpenProfile");
  const doesRequiredClose = document.getElementById("doesRequiredClose");
  const stakeSelectionOverlay = document.getElementById("stakeSelectionOverlay");
  const stakeSelectionPanel = document.getElementById("stakeSelectionPanel");
  const stakeSelectionClose = document.getElementById("stakeSelectionClose");
  const stakeOptionsGrid = document.getElementById("stakeOptionsGrid");
  const playWithFriendsBtn = document.getElementById("playWithFriendsBtn");
  const friendModeOverlay = document.getElementById("friendModeOverlay");
  const friendModePanel = document.getElementById("friendModePanel");
  const friendModeClose = document.getElementById("friendModeClose");
  const friendJoinOpenBtn = document.getElementById("friendJoinOpenBtn");
  const friendCreateOpenBtn = document.getElementById("friendCreateOpenBtn");
  const friendCreateOverlay = document.getElementById("friendCreateOverlay");
  const friendCreatePanel = document.getElementById("friendCreatePanel");
  const friendCreateClose = document.getElementById("friendCreateClose");
  const friendCreateStakeGrid = document.getElementById("friendCreateStakeGrid");
  const friendJoinOverlay = document.getElementById("friendJoinOverlay");
  const friendJoinPanel = document.getElementById("friendJoinPanel");
  const friendJoinClose = document.getElementById("friendJoinClose");
  const friendJoinCodeInput = document.getElementById("friendJoinCodeInput");
  const friendJoinHint = document.getElementById("friendJoinHint");
  const friendJoinSubmitBtn = document.getElementById("friendJoinSubmitBtn");
  const friendCodeOverlay = document.getElementById("friendCodeOverlay");
  const friendCodePanel = document.getElementById("friendCodePanel");
  const friendCodeValue = document.getElementById("friendCodeValue");
  const friendCodeStakeMeta = document.getElementById("friendCodeStakeMeta");
  const friendCodeCopyBtn = document.getElementById("friendCodeCopyBtn");
  const friendCodeContinueBtn = document.getElementById("friendCodeContinueBtn");
  const friendCodeCloseBtn = document.getElementById("friendCodeCloseBtn");
  const stakeUnavailableOverlay = document.getElementById("stakeUnavailableOverlay");
  const stakeUnavailablePanel = document.getElementById("stakeUnavailablePanel");
  const stakeUnavailableClose = document.getElementById("stakeUnavailableClose");
  const tournamentBtn = document.getElementById("tournamentBtn");
  const sharePromoBtn = document.getElementById("sharePromoBtn");
  const sharePromoBtnTitle = document.getElementById("sharePromoBtnTitle");
  const sharePromoBtnMeta = document.getElementById("sharePromoBtnMeta");
  const sharePromoBtnBadge = document.getElementById("sharePromoBtnBadge");
  const sharePromoOverlay = document.getElementById("sharePromoOverlay");
  const sharePromoPanel = document.getElementById("sharePromoPanel");
  const sharePromoCloseBtn = document.getElementById("sharePromoCloseBtn");
  const sharePromoProgressText = document.getElementById("sharePromoProgressText");
  const sharePromoProgressBar = document.getElementById("sharePromoProgressBar");
  const sharePromoStatusText = document.getElementById("sharePromoStatusText");
  const sharePromoCooldownText = document.getElementById("sharePromoCooldownText");
  const sharePromoTargetGrid = document.getElementById("sharePromoTargetGrid");
  const sharePromoPendingText = document.getElementById("sharePromoPendingText");
  const sharePromoConfirmBtn = document.getElementById("sharePromoConfirmBtn");
  const sharePromoConfirmBtnLabel = document.getElementById("sharePromoConfirmBtnLabel");
  const sharePromoSuccessOverlay = document.getElementById("sharePromoSuccessOverlay");
  const sharePromoSuccessPanel = document.getElementById("sharePromoSuccessPanel");
  const sharePromoSuccessMessage = document.getElementById("sharePromoSuccessMessage");
  const sharePromoSuccessCooldown = document.getElementById("sharePromoSuccessCooldown");
  const sharePromoSuccessCloseBtn = document.getElementById("sharePromoSuccessCloseBtn");
  const surveyPromptOverlay = document.getElementById("surveyPromptOverlay");
  const surveyPromptPanel = document.getElementById("surveyPromptPanel");
  const surveyPromptTitle = document.getElementById("surveyPromptTitle");
  const surveyPromptDescription = document.getElementById("surveyPromptDescription");
  const surveyPromptChoices = document.getElementById("surveyPromptChoices");
  const surveyPromptTextWrap = document.getElementById("surveyPromptTextWrap");
  const surveyPromptTextInput = document.getElementById("surveyPromptTextInput");
  const surveyPromptStatus = document.getElementById("surveyPromptStatus");
  const surveyPromptSubmitBtn = document.getElementById("surveyPromptSubmitBtn");
  const surveyPromptDismissBtn = document.getElementById("surveyPromptDismissBtn");
  const surveyPromptCloseBtn = document.getElementById("surveyPromptCloseBtn");
  const financeNoticeOverlay = document.getElementById("financeNoticeOverlay");
  const financeNoticePanel = document.getElementById("financeNoticePanel");
  const financeNoticeCloseBtn = document.getElementById("financeNoticeCloseBtn");
  const page2FrozenBanner = document.getElementById("page2FrozenBanner");
  const page2FrozenBannerText = document.getElementById("page2FrozenBannerText");
  let sharePromoState = null;
  let sharePromoStatusPromise = null;
  let sharePromoActionInFlight = false;
  let pendingShareSource = "";
  let page2AccountFrozen = false;
  let surveyPromptState = null;
  let surveyPromptSelection = "";
  let surveyPromptSubmitting = false;

  const setFrozenActionState = (btn, frozen) => {
    if (!btn) return;
    btn.disabled = frozen === true;
    btn.classList.toggle("opacity-60", frozen === true);
    btn.classList.toggle("cursor-not-allowed", frozen === true);
    btn.classList.toggle("pointer-events-none", frozen === true);
    btn.setAttribute("aria-disabled", frozen === true ? "true" : "false");
  };

  applyPage2AccountState = (clientData = {}) => {
    page2AccountFrozen = clientData?.accountFrozen === true;
    const frozenMessage = page2AccountFrozen
      ? "Ton compte a été temporairement gelé après plusieurs dépôts refusés. Dépôt, parties, tournois et bonus sont bloqués jusqu'au dégel."
      : "";

    if (page2FrozenBanner) {
      page2FrozenBanner.classList.toggle("hidden", page2AccountFrozen !== true);
    }
    if (page2FrozenBannerText) {
      page2FrozenBannerText.textContent = frozenMessage;
    }

    setFrozenActionState(soldBadgeBtn, page2AccountFrozen);
    setFrozenActionState(startGameBtn, page2AccountFrozen);
    setFrozenActionState(tournamentBtn, page2AccountFrozen);
    setFrozenActionState(sharePromoBtn, page2AccountFrozen);

    if (page2AccountFrozen) {
      closeSharePromo();
      closeStakeSelection();
      closeTournamentIntro();
    }
  };

  const clearSharePromoCountdownTimer = () => {
    if (!page2SharePromoCountdownTimer) return;
    window.clearInterval(page2SharePromoCountdownTimer);
    page2SharePromoCountdownTimer = null;
  };

  const renderSharePromoCooldown = (state = {}) => {
    const isCoolingDown = state.isCoolingDown === true;
    const remainingMs = isCoolingDown
      ? Math.max(0, Number(state.cooldownUntilMs) - Date.now())
      : Math.max(0, Number(state.cooldownRemainingMs) || 0);
    const cooldownLabel = isCoolingDown
      ? `Disponible de nouveau dans ${formatPromoCountdown(remainingMs)}`
      : "Le bonus revient une fois tous les 3 jours apres validation complete.";
    const compactUi = isCompactSharePromoUi();

    if (sharePromoCooldownText) {
      sharePromoCooldownText.textContent = cooldownLabel;
    }
    if (sharePromoSuccessCooldown) {
      sharePromoSuccessCooldown.textContent = isCoolingDown
        ? `Disponible de nouveau dans ${formatPromoCountdown(remainingMs)}`
        : "Disponible de nouveau dans 3 jours";
    }
    if (sharePromoBtnMeta) {
      const shareCount = Math.max(0, Number(state.shareCount) || 0);
      const targetCount = Math.max(1, Number(state.targetCount) || SHARE_SITE_PROMO_TARGET);
      const remainingCount = Math.max(0, Number(state.remainingCount) || (targetCount - shareCount));
      sharePromoBtnMeta.textContent = isCoolingDown
        ? (compactUi ? `Revient dans ${formatPromoCountdown(remainingMs)}` : cooldownLabel)
        : shareCount > 0
          ? (compactUi ? `${remainingCount} restant(s)` : `${remainingCount} partage(s) restants pour terminer ce cycle.`)
          : (compactUi ? "Bonus 100 Does" : "5 partages valides pour debloquer le bonus.");
    }
  };

  const setSharePromoActionLoading = (loading) => {
    sharePromoActionInFlight = loading === true;
    if (sharePromoConfirmBtn) {
      sharePromoConfirmBtn.disabled = loading === true || !pendingShareSource || sharePromoState?.isCoolingDown === true;
      sharePromoConfirmBtn.classList.toggle("opacity-70", loading === true);
      sharePromoConfirmBtn.classList.toggle("cursor-wait", loading === true);
    }
  };

  const renderSharePromoTargets = () => {
    if (!sharePromoTargetGrid) return;
    const targets = buildShareSitePromoTargets();
    sharePromoTargetGrid.innerHTML = targets.map((target) => `
      <button
        type="button"
        class="share-promo-target inline-flex min-h-[56px] items-center justify-center rounded-[18px] border border-white/15 bg-white/8 px-3 py-3 text-white/88 transition hover:bg-white/14"
        data-share-target="${target.id}"
        aria-label="${target.label}"
        title="${target.label}"
      >
        <i class="${target.icon} text-[20px]"></i>
        <span class="sr-only">${target.label}</span>
      </button>
    `).join("");
  };

  const setPendingShareSource = (source = "") => {
    pendingShareSource = String(source || "").trim();
    const hasPendingShare = !!pendingShareSource;
    if (sharePromoConfirmBtn) {
      sharePromoConfirmBtn.disabled = !hasPendingShare || sharePromoState?.isCoolingDown === true;
    }
    if (sharePromoConfirmBtnLabel) {
      sharePromoConfirmBtnLabel.textContent = hasPendingShare
        ? `Valider le partage ${pendingShareSource}`
        : "Valider ce partage";
    }
    if (sharePromoPendingText) {
      sharePromoPendingText.textContent = hasPendingShare
        ? `Fenetre ${pendingShareSource} ouverte. Reviens ici puis valide seulement si tu as bien partage le lien ${SHARE_SITE_PROMO_LINK}.`
        : "Choisis une application, partage le lien, puis reviens ici pour valider ton partage.";
    }
  };

  const applySharePromoState = (rawState = null) => {
    sharePromoState = rawState && typeof rawState === "object" ? { ...rawState } : null;
    const state = sharePromoState || {
      targetCount: SHARE_SITE_PROMO_TARGET,
      shareCount: 0,
      rewardDoes: SHARE_SITE_PROMO_REWARD_DOES,
      progressPercent: 0,
      remainingCount: SHARE_SITE_PROMO_TARGET,
      canShare: false,
      isCoolingDown: false,
      cooldownRemainingMs: 0,
      cooldownUntilMs: 0,
      rewardGranted: false,
    };

    const shareCount = Math.max(0, Number(state.shareCount) || 0);
    const targetCount = Math.max(1, Number(state.targetCount) || SHARE_SITE_PROMO_TARGET);
    const remainingCount = Math.max(0, Number(state.remainingCount) || (targetCount - shareCount));
    const progressPercent = Math.max(0, Math.min(100, Number(state.progressPercent) || Math.round((shareCount / targetCount) * 100)));
    const isCoolingDown = state.isCoolingDown === true;
    const rewardGranted = state.rewardGranted === true;

    if (sharePromoProgressText) {
      sharePromoProgressText.textContent = `${shareCount}/${targetCount} partages`;
    }
    if (sharePromoProgressBar) {
      sharePromoProgressBar.style.width = `${progressPercent}%`;
    }
    if (sharePromoBtnBadge) {
      sharePromoBtnBadge.textContent = `${shareCount}/${targetCount}`;
    }

    if (sharePromoBtnTitle) {
      sharePromoBtnTitle.textContent = isCompactSharePromoUi()
        ? (isCoolingDown ? "Bonus en pause" : "Bonus partage")
        : (isCoolingDown ? "Bonus partage deja utilise" : `Partager et gagner ${SHARE_SITE_PROMO_REWARD_DOES} Does`);
    }

    if (sharePromoStatusText) {
      if (rewardGranted && isCoolingDown) {
        const remainingMs = Math.max(
          0,
          Number(state.cooldownUntilMs) - Date.now() || Number(state.cooldownRemainingMs) || 0,
        );
        sharePromoStatusText.textContent = `Tu as deja gagne tes ${SHARE_SITE_PROMO_REWARD_DOES} Does. Reviens dans ${formatPromoCountdown(remainingMs)} pour relancer un nouveau cycle.`;
      } else if (remainingCount <= 0) {
        sharePromoStatusText.textContent = "Bonus valide. Le prochain cycle sera disponible apres le delai.";
      } else if (shareCount > 0) {
        sharePromoStatusText.textContent = `Encore ${remainingCount} partage(s) pour debloquer tes ${SHARE_SITE_PROMO_REWARD_DOES} Does.`;
      } else {
        sharePromoStatusText.textContent = `Partage le site ${targetCount} fois pour debloquer ton bonus.`;
      }
    }

    renderSharePromoCooldown({
      ...state,
      shareCount,
      targetCount,
      remainingCount,
      isCoolingDown,
    });

    if (sharePromoBtn) {
      sharePromoBtn.classList.toggle("opacity-65", isCoolingDown);
      sharePromoBtn.classList.toggle("border-white/15", isCoolingDown);
      sharePromoBtn.classList.toggle("bg-white/5", isCoolingDown);
      sharePromoBtn.setAttribute("aria-disabled", isCoolingDown ? "true" : "false");
    }

    if (!sharePromoActionInFlight) {
      if (sharePromoConfirmBtn) {
        sharePromoConfirmBtn.disabled = isCoolingDown || !pendingShareSource;
      }
    }

    clearSharePromoCountdownTimer();
    if (isCoolingDown && Number(state.cooldownUntilMs) > Date.now()) {
      page2SharePromoCountdownTimer = window.setInterval(() => {
        const remainingMs = Math.max(0, Number(state.cooldownUntilMs) - Date.now());
        const nextCoolingDown = remainingMs > 0;
        sharePromoState = {
          ...(sharePromoState || state),
          cooldownRemainingMs: remainingMs,
          isCoolingDown: nextCoolingDown,
          cooldownUntilMs: Number(state.cooldownUntilMs) || 0,
        };
        renderSharePromoCooldown({
          ...(sharePromoState || state),
          shareCount,
          targetCount,
          remainingCount,
          isCoolingDown: nextCoolingDown,
        });
        if (!nextCoolingDown) {
          clearSharePromoCountdownTimer();
          applySharePromoState({
            ...(sharePromoState || state),
            shareCount: 0,
            remainingCount: SHARE_SITE_PROMO_TARGET,
            progressPercent: 0,
            rewardGranted: false,
            isCoolingDown: false,
            cooldownRemainingMs: 0,
            cooldownUntilMs: 0,
          });
        }
      }, 1000);
    }
  };

  const loadSharePromoStatus = () => {
    if (!hasConfirmedAuth) {
      applySharePromoState(null);
      return Promise.resolve(null);
    }
    if (!sharePromoStatusPromise) {
      sharePromoStatusPromise = getShareSitePromoStatusSecure({})
        .then((result) => {
          if (result?.accountFrozen === true) {
            page2AccountFrozen = true;
          }
          applySharePromoState(result);
          return result;
        })
        .catch((error) => {
          console.warn("[SHARE_PROMO] status load failed", error);
          applySharePromoState(null);
          return null;
        })
        .finally(() => {
          sharePromoStatusPromise = null;
        });
    }
    return sharePromoStatusPromise;
  };

  const openSharePromo = () => {
    if (!sharePromoOverlay) return;
    sharePromoOverlay.classList.remove("hidden");
    sharePromoOverlay.classList.add("flex");
    document.body.classList.add("overflow-hidden");
    setPendingShareSource("");
  };

  const closeSharePromo = () => {
    if (!sharePromoOverlay) return;
    sharePromoOverlay.classList.add("hidden");
    sharePromoOverlay.classList.remove("flex");
    document.body.classList.remove("overflow-hidden");
  };

  const openSharePromoSuccess = (state = {}) => {
    if (!sharePromoSuccessOverlay) return;
    if (sharePromoSuccessMessage) {
      sharePromoSuccessMessage.textContent = `Tu as gagne avec succes ${SHARE_SITE_PROMO_REWARD_DOES} Does.`;
    }
    renderSharePromoCooldown(state);
    sharePromoSuccessOverlay.classList.remove("hidden");
    sharePromoSuccessOverlay.classList.add("flex");
    document.body.classList.add("overflow-hidden");
  };

  const closeSharePromoSuccess = () => {
    if (!sharePromoSuccessOverlay) return;
    sharePromoSuccessOverlay.classList.add("hidden");
    sharePromoSuccessOverlay.classList.remove("flex");
    if (sharePromoOverlay?.classList.contains("hidden")) {
      document.body.classList.remove("overflow-hidden");
    }
  };

  const surveyDismissKey = (survey = null) => {
    const surveyId = String(survey?.id || "").trim();
    const version = Number.parseInt(String(survey?.version || 1), 10) || 1;
    return surveyId ? `domino_survey_dismissed_${surveyId}_v${version}` : "";
  };

  const hasDismissedSurvey = (survey = null) => {
    const key = surveyDismissKey(survey);
    if (!key) return false;
    try {
      return window.sessionStorage.getItem(key) === "1";
    } catch (_) {
      return false;
    }
  };

  const markSurveyDismissed = (survey = null) => {
    const key = surveyDismissKey(survey);
    if (!key) return;
    try {
      window.sessionStorage.setItem(key, "1");
    } catch (_) {
    }
  };

  const setSurveyPromptStatus = (message = "") => {
    if (surveyPromptStatus) {
      surveyPromptStatus.textContent = String(message || "");
    }
  };

  const setSurveyPromptSubmitting = (submitting) => {
    surveyPromptSubmitting = submitting === true;
    if (surveyPromptSubmitBtn) {
      surveyPromptSubmitBtn.disabled = surveyPromptSubmitting;
      surveyPromptSubmitBtn.classList.toggle("opacity-70", surveyPromptSubmitting);
      surveyPromptSubmitBtn.classList.toggle("cursor-wait", surveyPromptSubmitting);
    }
  };

  const closeSurveyPrompt = ({ dismiss = false } = {}) => {
    if (dismiss) {
      markSurveyDismissed(surveyPromptState);
    }
    surveyPromptOverlay?.classList.add("hidden");
    surveyPromptOverlay?.classList.remove("flex");
    surveyPromptTextInput && (surveyPromptTextInput.value = "");
    surveyPromptSelection = "";
    surveyPromptState = null;
    setSurveyPromptStatus("");
    if (
      sharePromoOverlay?.classList.contains("hidden")
      && sharePromoSuccessOverlay?.classList.contains("hidden")
      && stakeSelectionOverlay?.classList.contains("hidden")
      && friendModeOverlay?.classList.contains("hidden")
      && friendCreateOverlay?.classList.contains("hidden")
      && friendJoinOverlay?.classList.contains("hidden")
      && friendCodeOverlay?.classList.contains("hidden")
      && doesRequiredOverlay?.classList.contains("hidden")
      && tournamentIntroOverlay?.classList.contains("hidden")
    ) {
      document.body.classList.remove("overflow-hidden");
    }
  };

  const renderSurveyChoices = (survey = null) => {
    if (!surveyPromptChoices) return;
    const choices = Array.isArray(survey?.choices) ? survey.choices : [];
    if (!survey.allowChoiceAnswer || !choices.length) {
      surveyPromptChoices.innerHTML = "";
      surveyPromptChoices.classList.add("hidden");
      return;
    }
    surveyPromptChoices.classList.remove("hidden");
    surveyPromptChoices.innerHTML = choices.map((choice) => {
      const active = surveyPromptSelection === choice.id;
      return `
        <button
          type="button"
          data-survey-choice="${choice.id}"
          class="survey-choice-btn flex min-h-[54px] w-full items-center justify-between gap-3 rounded-[18px] border px-4 py-3 text-left text-sm font-semibold transition ${active ? "border-[#ffb26e] bg-[#F57C00]/18 text-white" : "border-white/15 bg-white/8 text-white/88 hover:bg-white/12"}"
        >
          <span>${choice.label}</span>
          <span class="inline-flex h-5 w-5 items-center justify-center rounded-full border ${active ? "border-[#ffd9b8] bg-[#F57C00] text-white" : "border-white/18 bg-white/8 text-transparent"}">•</span>
        </button>
      `;
    }).join("");
  };

  const openSurveyPrompt = (survey = null) => {
    if (!surveyPromptOverlay || !survey) return;
    surveyPromptState = survey;
    surveyPromptSelection = "";
    setSurveyPromptSubmitting(false);
    setSurveyPromptStatus("");
    if (surveyPromptTitle) surveyPromptTitle.textContent = survey.title || "Ton avis nous aide";
    if (surveyPromptDescription) {
      surveyPromptDescription.textContent = survey.description || "Réponds en quelques secondes pour nous aider à améliorer le site.";
    }
    if (surveyPromptTextWrap) {
      surveyPromptTextWrap.classList.toggle("hidden", survey.allowTextAnswer !== true);
    }
    if (surveyPromptTextInput) {
      surveyPromptTextInput.value = "";
    }
    renderSurveyChoices(survey);
    surveyPromptOverlay.classList.remove("hidden");
    surveyPromptOverlay.classList.add("flex");
    document.body.classList.add("overflow-hidden");
  };

  const loadSurveyPrompt = () => {
    if (!hasConfirmedAuth || isOptimisticAuth) return Promise.resolve(null);
    return getActiveSurveyForUserSecure({})
      .then((result) => {
        const survey = result?.survey && typeof result.survey === "object" ? result.survey : null;
        if (!survey || hasDismissedSurvey(survey)) return null;
        openSurveyPrompt(survey);
        return survey;
      })
      .catch((error) => {
        console.warn("[SURVEY] active survey load failed", error);
        return null;
      });
  };

  const submitSurveyPrompt = async () => {
    if (!surveyPromptState || surveyPromptSubmitting) return;
    const choiceId = String(surveyPromptSelection || "").trim();
    const textAnswer = String(surveyPromptTextInput?.value || "").trim();
    if (surveyPromptState.allowChoiceAnswer === true && surveyPromptState.allowTextAnswer === true && !choiceId && !textAnswer) {
      setSurveyPromptStatus("Choisis une réponse ou écris ton avis.");
      return;
    }
    if (surveyPromptState.allowChoiceAnswer === true && surveyPromptState.allowTextAnswer !== true && !choiceId) {
      setSurveyPromptStatus("Choisis une réponse avant d'envoyer.");
      return;
    }
    if (surveyPromptState.allowTextAnswer === true && surveyPromptState.allowChoiceAnswer !== true && !textAnswer) {
      setSurveyPromptStatus("Ecris une réponse avant d'envoyer.");
      return;
    }
    setSurveyPromptSubmitting(true);
    setSurveyPromptStatus("");
    try {
      await submitSurveyResponseSecure({
        surveyId: surveyPromptState.id,
        choiceId,
        textAnswer,
      });
      closeSurveyPrompt({ dismiss: false });
    } catch (error) {
      setSurveyPromptStatus(error?.message || "Impossible d'envoyer ta réponse pour le moment.");
    } finally {
      setSurveyPromptSubmitting(false);
    }
  };

  if (logo && logoFallback) {
    logo.addEventListener("error", () => {
      logo.classList.add("hidden");
      logoFallback.classList.remove("hidden");
    });
  }
  if (authCtaBtn) {
    authCtaBtn.addEventListener("click", () => {
      showGlobalLoading("Ouverture de la connexion...");
      window.location.href = "./auth.html";
    });
  }
  if (isOptimisticAuth && profileBtn) {
    profileBtn.setAttribute("aria-disabled", "true");
    profileBtn.classList.add("pointer-events-none", "opacity-60", "cursor-wait");
  }
  if (isOptimisticAuth && soldBadgeBtn) {
    soldBadgeBtn.setAttribute("aria-disabled", "true");
    soldBadgeBtn.classList.add("pointer-events-none", "opacity-70", "cursor-wait");
  }
  if (profileBtn) {
    profileBtn.addEventListener("click", () => {
      if (isOptimisticAuth) {
        showGlobalLoading("Finalisation de la session...");
        window.setTimeout(() => {
          hideGlobalLoading();
        }, 1600);
        return;
      }
      openProfilePage();
    });
  }

  surveyPromptCloseBtn?.addEventListener("click", () => {
    closeSurveyPrompt({ dismiss: true });
  });

  surveyPromptDismissBtn?.addEventListener("click", () => {
    closeSurveyPrompt({ dismiss: true });
  });

  surveyPromptSubmitBtn?.addEventListener("click", () => {
    void submitSurveyPrompt();
  });

  surveyPromptChoices?.addEventListener("click", (event) => {
    const origin = event.target instanceof HTMLElement ? event.target : null;
    const target = origin ? origin.closest("[data-survey-choice]") : null;
    if (!(target instanceof HTMLElement)) return;
    surveyPromptSelection = String(target.dataset.surveyChoice || "").trim();
    renderSurveyChoices(surveyPromptState);
  });

  surveyPromptOverlay?.addEventListener("click", (event) => {
    if (event.target === surveyPromptOverlay) {
      closeSurveyPrompt({ dismiss: true });
    }
  });

  const openStakeSelection = () => {
    if (!stakeSelectionOverlay) return;
    stakeSelectionOverlay.classList.remove("hidden");
    stakeSelectionOverlay.classList.add("flex");
    document.body.classList.add("overflow-hidden");
  };
  const closeStakeSelection = () => {
    if (!stakeSelectionOverlay) return;
    stakeSelectionOverlay.classList.add("hidden");
    stakeSelectionOverlay.classList.remove("flex");
    document.body.classList.remove("overflow-hidden");
  };

  const friendRoomDraft = {
    roomId: "",
    seatIndex: 0,
    stakeDoes: 0,
    inviteCode: "",
  };

  const navigateToFriendRoom = (roomData = {}) => {
    const nextRoomId = String(roomData?.roomId || friendRoomDraft.roomId || "").trim();
    const nextSeatIndex = Number.parseInt(String(roomData?.seatIndex ?? friendRoomDraft.seatIndex ?? 0), 10) || 0;
    const nextStakeDoes = Number.parseInt(String(roomData?.stakeDoes || friendRoomDraft.stakeDoes || 0), 10) || 100;
    if (!nextRoomId) {
      throw new Error("Salle privée introuvable.");
    }
    showGlobalLoading("Connexion des joueurs en cours...");
    window.location.href = buildFriendGameUrl(nextRoomId, nextSeatIndex, nextStakeDoes);
  };

  const openFriendMode = () => {
    if (!friendModeOverlay) return;
    friendModeOverlay.classList.remove("hidden");
    friendModeOverlay.classList.add("flex");
    document.body.classList.add("overflow-hidden");
  };

  const closeFriendMode = () => {
    if (!friendModeOverlay) return;
    friendModeOverlay.classList.add("hidden");
    friendModeOverlay.classList.remove("flex");
    document.body.classList.remove("overflow-hidden");
  };

  const openFriendCreate = () => {
    if (!friendCreateOverlay) return;
    friendCreateOverlay.classList.remove("hidden");
    friendCreateOverlay.classList.add("flex");
    document.body.classList.add("overflow-hidden");
  };

  const closeFriendCreate = () => {
    if (!friendCreateOverlay) return;
    friendCreateOverlay.classList.add("hidden");
    friendCreateOverlay.classList.remove("flex");
    document.body.classList.remove("overflow-hidden");
  };

  const openFriendJoin = () => {
    if (!friendJoinOverlay) return;
    friendJoinOverlay.classList.remove("hidden");
    friendJoinOverlay.classList.add("flex");
    document.body.classList.add("overflow-hidden");
    window.setTimeout(() => {
      friendJoinCodeInput?.focus();
      friendJoinCodeInput?.select();
    }, 40);
  };

  const closeFriendJoin = () => {
    if (!friendJoinOverlay) return;
    friendJoinOverlay.classList.add("hidden");
    friendJoinOverlay.classList.remove("flex");
    document.body.classList.remove("overflow-hidden");
  };

  const openFriendCode = () => {
    if (!friendCodeOverlay) return;
    friendCodeOverlay.classList.remove("hidden");
    friendCodeOverlay.classList.add("flex");
    document.body.classList.add("overflow-hidden");
  };

  const closeFriendCode = () => {
    if (!friendCodeOverlay) return;
    friendCodeOverlay.classList.add("hidden");
    friendCodeOverlay.classList.remove("flex");
    document.body.classList.remove("overflow-hidden");
  };

  const renderFriendCreateStakeOptions = (options = []) => {
    if (!friendCreateStakeGrid) return;
    const items = normalizeGameStakeOptions(options);
    friendCreateStakeGrid.innerHTML = items.map((option) => {
      const enabled = option.enabled === true;
      const classes = enabled
        ? "friend-create-stake-btn h-14 rounded-2xl border border-[#ffb26e] bg-[#F57C00] text-sm font-semibold text-white shadow-[8px_8px_18px_rgba(163,82,27,0.45),-6px_-6px_14px_rgba(255,175,102,0.22)] transition hover:-translate-y-0.5"
        : "friend-create-stake-btn h-14 rounded-2xl border border-white/20 bg-white/10 text-sm font-semibold text-white/65 opacity-55 cursor-not-allowed";
      return `
        <button
          data-stake="${option.stakeDoes}"
          data-available="${enabled ? "1" : "0"}"
          type="button"
          class="${classes}"
        >
          <span class="block">${option.stakeDoes} Does</span>
          <span class="text-[11px] font-medium ${enabled ? "text-white/75" : "text-white/55"}">Gain ${option.rewardDoes} Does</span>
        </button>
      `;
    }).join("");
  };

  const openUnavailable = () => {
    if (!stakeUnavailableOverlay) return;
    stakeUnavailableOverlay.classList.remove("hidden");
    stakeUnavailableOverlay.classList.add("flex");
  };
  const closeUnavailable = () => {
    if (!stakeUnavailableOverlay) return;
    stakeUnavailableOverlay.classList.add("hidden");
    stakeUnavailableOverlay.classList.remove("flex");
  };

  const openTournamentIntro = () => {
    if (!tournamentIntroOverlay) return;
    tournamentIntroOverlay.classList.remove("hidden");
    tournamentIntroOverlay.classList.add("flex");
    document.body.classList.add("overflow-hidden");
  };

  const closeTournamentIntro = () => {
    if (!tournamentIntroOverlay) return;
    tournamentIntroOverlay.classList.add("hidden");
    tournamentIntroOverlay.classList.remove("flex");
    document.body.classList.remove("overflow-hidden");
  };

  const continueToTournament = () => {
    showGlobalLoading("Ouverture du tournoi...");
    window.location.href = "./tournois.html";
  };

  let currentStakeOptions = normalizeGameStakeOptions();

  const renderStakeOptions = (options = []) => {
    currentStakeOptions = normalizeGameStakeOptions(options);
    if (!stakeOptionsGrid) return;
    stakeOptionsGrid.innerHTML = currentStakeOptions.map((option) => {
      const enabled = option.enabled === true;
      const classes = enabled
        ? "stake-option-btn h-14 rounded-2xl border border-[#ffb26e] bg-[#F57C00] text-sm font-semibold text-white shadow-[8px_8px_18px_rgba(163,82,27,0.45),-6px_-6px_14px_rgba(255,175,102,0.22)] transition hover:-translate-y-0.5"
        : "stake-option-btn h-14 rounded-2xl border border-white/20 bg-white/10 text-sm font-semibold text-white/65 opacity-55 transition cursor-not-allowed";
      const badge = enabled
        ? `<span class="text-[11px] font-medium text-white/75">Gain ${option.rewardDoes} Does</span>`
        : `<span class="text-[11px] font-medium text-white/55">Indisponible</span>`;
      return `
        <button
          data-stake="${option.stakeDoes}"
          data-available="${enabled ? "1" : "0"}"
          type="button"
          class="${classes}"
        >
          <span class="block">${option.stakeDoes} Does</span>
          ${badge}
        </button>
      `;
    }).join("");
  };

  let stakeOptionsHydrationPromise = null;
  const ensureStakeOptionsLoaded = () => {
    if (stakeOptionsHydrationPromise) return stakeOptionsHydrationPromise;
    stakeOptionsHydrationPromise = loadPublicGameStakeOptions()
      .then((options) => {
        renderStakeOptions(options);
        renderFriendCreateStakeOptions(options);
        return options;
      })
      .catch((error) => {
        console.warn("[GAME_STAKES] render fallback", error);
        const fallback = normalizeGameStakeOptions();
        renderStakeOptions(fallback);
        renderFriendCreateStakeOptions(fallback);
        return fallback;
      });
    return stakeOptionsHydrationPromise;
  };

  renderStakeOptions(currentStakeOptions);
  renderFriendCreateStakeOptions(currentStakeOptions);

  if (startGameBtn) {
    startGameBtn.addEventListener("click", () => {
      if (page2AccountFrozen) return;
      if (!isAuthenticated) {
        showGlobalLoading("Redirection vers la connexion...");
        window.location.href = "./auth.html";
        return;
      }
      if (isOptimisticAuth) {
        showGlobalLoading("Finalisation de la session...");
        window.setTimeout(() => {
          hideGlobalLoading();
        }, 1600);
        return;
      }
      void ensureStakeOptionsLoaded();
      openStakeSelection();
    });
  }

  playWithFriendsBtn?.addEventListener("click", async () => {
    if (page2AccountFrozen) return;
    if (!isAuthenticated) {
      showGlobalLoading("Redirection vers la connexion...");
      window.location.href = "./auth.html";
      return;
    }
    if (isOptimisticAuth) {
      showGlobalLoading("Finalisation de la session...");
      window.setTimeout(() => {
        hideGlobalLoading();
      }, 1600);
      return;
    }
    await ensureStakeOptionsLoaded();
    closeStakeSelection();
    openFriendMode();
  });

  friendCreateOpenBtn?.addEventListener("click", async () => {
    if (page2AccountFrozen) return;
    await ensureStakeOptionsLoaded();
    closeFriendMode();
    openFriendCreate();
  });

  friendJoinOpenBtn?.addEventListener("click", () => {
    if (page2AccountFrozen) return;
    closeFriendMode();
    if (friendJoinCodeInput) {
      friendJoinCodeInput.value = "";
    }
    if (friendJoinHint) {
      friendJoinHint.textContent = "Entre le code exactement comme il t'a ete envoye.";
    }
    openFriendJoin();
  });

  friendCreateStakeGrid?.addEventListener("click", async (event) => {
    const btn = event.target.closest(".friend-create-stake-btn");
    if (!btn || !friendCreateStakeGrid.contains(btn)) return;
    if (btn.getAttribute("data-available") !== "1") {
      openUnavailable();
      return;
    }
    const stakeAmount = Number(btn.getAttribute("data-stake") || 100);
    try {
      await withButtonLoading(btn, async () => {
        const xchangeModule = await loadXchangeModule();
        await xchangeModule.ensureXchangeState(user?.uid);
        const state = xchangeModule.getXchangeState(window.__userBaseBalance || window.__userBalance || 0, user?.uid);
        if ((state?.does || 0) < stakeAmount) {
          closeFriendCreate();
          if (doesRequiredOverlay) {
            doesRequiredOverlay.classList.remove("hidden");
            doesRequiredOverlay.classList.add("flex");
          }
          return;
        }

        const result = await createFriendRoomSecure({
          stakeDoes: stakeAmount,
          requiredHumans: 4,
        });
        friendRoomDraft.roomId = String(result?.roomId || "");
        friendRoomDraft.seatIndex = Number.parseInt(String(result?.seatIndex || 0), 10) || 0;
        friendRoomDraft.stakeDoes = Number.parseInt(String(result?.stakeDoes || stakeAmount), 10) || stakeAmount;
        friendRoomDraft.inviteCode = String(result?.inviteCode || "").trim();

        if (friendCodeValue) {
          friendCodeValue.textContent = friendRoomDraft.inviteCode || "------";
        }
        if (friendCodeStakeMeta) {
          friendCodeStakeMeta.textContent = `${friendRoomDraft.stakeDoes} Does obligatoires pour 4 joueurs.`;
        }
        if (friendCodeCopyBtn) {
          friendCodeCopyBtn.textContent = "Copier le code";
        }

        closeFriendCreate();
        openFriendCode();
      }, { loadingLabel: "Creation..." });
    } catch (error) {
      console.error("[FRIEND_ROOM] create failed", error);
      if (
        String(error?.code || "") === "active-room-exists"
        && String(error?.roomMode || "public") === "friends"
        && error?.roomId
      ) {
        friendRoomDraft.roomId = String(error.roomId || "");
        friendRoomDraft.seatIndex = Number.parseInt(String(error?.seatIndex || 0), 10) || 0;
        friendRoomDraft.stakeDoes = stakeAmount;
        closeFriendCreate();
        navigateToFriendRoom(friendRoomDraft);
      }
    }
  });

  friendCodeCopyBtn?.addEventListener("click", async () => {
    const codeToCopy = String(friendRoomDraft.inviteCode || "").trim();
    if (!codeToCopy) return;
    try {
      await navigator.clipboard.writeText(codeToCopy);
      friendCodeCopyBtn.textContent = "Code copie";
    } catch (_) {
      friendCodeCopyBtn.textContent = "Copie impossible";
    }
  });

  friendCodeContinueBtn?.addEventListener("click", () => {
    closeFriendCode();
    navigateToFriendRoom(friendRoomDraft);
  });

  friendJoinCodeInput?.addEventListener("input", () => {
    friendJoinCodeInput.value = normalizeInviteCode(friendJoinCodeInput.value);
  });

  friendJoinCodeInput?.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      friendJoinSubmitBtn?.click();
    }
  });

  friendJoinSubmitBtn?.addEventListener("click", async () => {
    const inviteCode = normalizeInviteCode(friendJoinCodeInput?.value || "");
    if (!inviteCode) {
      if (friendJoinHint) {
        friendJoinHint.textContent = "Entre le code de ton ami pour continuer.";
      }
      friendJoinCodeInput?.focus();
      return;
    }

    try {
      await withButtonLoading(friendJoinSubmitBtn, async () => {
        const result = await joinFriendRoomByCodeSecure({ inviteCode });
        friendRoomDraft.roomId = String(result?.roomId || "");
        friendRoomDraft.seatIndex = Number.parseInt(String(result?.seatIndex || 0), 10) || 0;
        friendRoomDraft.stakeDoes = Number.parseInt(String(result?.stakeDoes || 0), 10) || 100;
        friendRoomDraft.inviteCode = String(result?.inviteCode || inviteCode).trim();
        closeFriendJoin();
        navigateToFriendRoom(friendRoomDraft);
      }, { loadingLabel: "Connexion..." });
    } catch (error) {
      console.error("[FRIEND_ROOM] join failed", error);
      if (
        String(error?.code || "") === "active-room-exists"
        && String(error?.roomMode || "public") === "friends"
        && error?.roomId
      ) {
        friendRoomDraft.roomId = String(error.roomId || "");
        friendRoomDraft.seatIndex = Number.parseInt(String(error?.seatIndex || 0), 10) || 0;
        friendRoomDraft.stakeDoes = Number.parseInt(String(error?.stakeDoes || friendRoomDraft.stakeDoes || 100), 10) || 100;
        closeFriendJoin();
        navigateToFriendRoom(friendRoomDraft);
        return;
      }
      if (String(error?.message || "").toLowerCase().includes("solde does insuffisant")) {
        closeFriendJoin();
        if (doesRequiredOverlay) {
          doesRequiredOverlay.classList.remove("hidden");
          doesRequiredOverlay.classList.add("flex");
        }
        return;
      }
      if (friendJoinHint) {
        friendJoinHint.textContent = error?.message || "Impossible de rejoindre cette salle pour le moment.";
      }
    }
  });

  if (stakeSelectionClose) stakeSelectionClose.addEventListener("click", closeStakeSelection);
  if (stakeSelectionOverlay) {
    stakeSelectionOverlay.addEventListener("click", (ev) => {
      if (ev.target === stakeSelectionOverlay) closeStakeSelection();
    });
  }
  if (stakeSelectionPanel) {
    stakeSelectionPanel.addEventListener("click", (ev) => ev.stopPropagation());
  }
  if (friendModeClose) friendModeClose.addEventListener("click", closeFriendMode);
  friendModeOverlay?.addEventListener("click", (ev) => {
    if (ev.target === friendModeOverlay) closeFriendMode();
  });
  friendModePanel?.addEventListener("click", (ev) => ev.stopPropagation());
  if (friendCreateClose) friendCreateClose.addEventListener("click", closeFriendCreate);
  friendCreateOverlay?.addEventListener("click", (ev) => {
    if (ev.target === friendCreateOverlay) closeFriendCreate();
  });
  friendCreatePanel?.addEventListener("click", (ev) => ev.stopPropagation());
  if (friendJoinClose) friendJoinClose.addEventListener("click", closeFriendJoin);
  friendJoinOverlay?.addEventListener("click", (ev) => {
    if (ev.target === friendJoinOverlay) closeFriendJoin();
  });
  friendJoinPanel?.addEventListener("click", (ev) => ev.stopPropagation());
  if (friendCodeCloseBtn) friendCodeCloseBtn.addEventListener("click", closeFriendCode);
  friendCodeOverlay?.addEventListener("click", (ev) => {
    if (ev.target === friendCodeOverlay) closeFriendCode();
  });
  friendCodePanel?.addEventListener("click", (ev) => ev.stopPropagation());

  if (stakeUnavailableClose) stakeUnavailableClose.addEventListener("click", closeUnavailable);
  if (stakeUnavailableOverlay) {
    stakeUnavailableOverlay.addEventListener("click", (ev) => {
      if (ev.target === stakeUnavailableOverlay) closeUnavailable();
    });
  }
  if (stakeUnavailablePanel) {
    stakeUnavailablePanel.addEventListener("click", (ev) => ev.stopPropagation());
  }

  if (tournamentBtn) {
    tournamentBtn.addEventListener("click", () => {
      if (page2AccountFrozen) return;
      if (hasSeenTournamentIntro()) {
        continueToTournament();
        return;
      }
      openTournamentIntro();
    });
  }

  if (sharePromoBtn) {
    sharePromoBtn.addEventListener("click", async () => {
      if (page2AccountFrozen) return;
      if (!isAuthenticated) {
        showGlobalLoading("Connexion requise pour le bonus...");
        window.location.href = "./auth.html";
        return;
      }
      if (isOptimisticAuth) {
        showGlobalLoading("Finalisation de la session...");
        window.setTimeout(() => {
          hideGlobalLoading();
        }, 1600);
        return;
      }
      openSharePromo();
      const status = await loadSharePromoStatus();
      if (status?.rewardGranted === true && status?.isCoolingDown === true) {
        closeSharePromo();
        openSharePromoSuccess(status);
      }
    });
  }

  sharePromoCloseBtn?.addEventListener("click", closeSharePromo);
  sharePromoOverlay?.addEventListener("click", (ev) => {
    if (ev.target === sharePromoOverlay) {
      closeSharePromo();
    }
  });
  sharePromoSuccessCloseBtn?.addEventListener("click", closeSharePromoSuccess);
  sharePromoSuccessOverlay?.addEventListener("click", (ev) => {
    if (ev.target === sharePromoSuccessOverlay) {
      closeSharePromoSuccess();
    }
  });
  sharePromoPanel?.addEventListener("click", (ev) => {
    ev.stopPropagation();
  });
  sharePromoSuccessPanel?.addEventListener("click", (ev) => {
    ev.stopPropagation();
  });
  sharePromoTargetGrid?.addEventListener("click", async (event) => {
    const btn = event.target.closest("[data-share-target]");
    if (!btn || sharePromoActionInFlight || sharePromoState?.isCoolingDown || page2AccountFrozen) return;
    const targetId = String(btn.getAttribute("data-share-target") || "").trim();
    if (!targetId) return;
    try {
      await withButtonLoading(btn, async () => {
        const result = await openShareSitePromoTarget(targetId);
        setPendingShareSource(result?.source || targetId);
      }, { loadingLabel: "..." });
    } catch (error) {
      if (isShareAbortError(error)) return;
      console.error("[SHARE_PROMO] target open failed", error);
      if (sharePromoPendingText) {
        sharePromoPendingText.textContent = "Impossible d'ouvrir ce canal de partage pour le moment.";
      }
    }
  });
  sharePromoConfirmBtn?.addEventListener("click", async () => {
    if (sharePromoActionInFlight || !hasConfirmedAuth || !pendingShareSource || page2AccountFrozen) return;
    if (sharePromoState?.isCoolingDown) return;
    try {
      setSharePromoActionLoading(true, "Validation du bonus...");
      const result = await recordShareSitePromoSecure({
        actionId: makePromoActionId(),
        shareSource: pendingShareSource,
      });
      applySharePromoState(result);
      setPendingShareSource("");
      if (result?.rewardGrantedNow) {
        closeSharePromo();
        openSharePromoSuccess(result);
      }
    } catch (error) {
      console.error("[SHARE_PROMO] confirm failed", error);
      if (sharePromoStatusText) {
        sharePromoStatusText.textContent = "Impossible de valider ce partage pour le moment.";
      }
    } finally {
      setSharePromoActionLoading(false);
    }
  });

  tournamentIntroContinueBtn?.addEventListener("click", () => {
    markTournamentIntroSeen();
    closeTournamentIntro();
    continueToTournament();
  });

  tournamentIntroOverlay?.addEventListener("click", (ev) => {
    if (ev.target === tournamentIntroOverlay) {
      closeTournamentIntro();
    }
  });

  tournamentIntroPanel?.addEventListener("click", (ev) => {
    ev.stopPropagation();
  });

  if (stakeOptionsGrid) {
    stakeOptionsGrid.addEventListener("click", async (event) => {
      const btn = event.target.closest(".stake-option-btn");
      if (!btn || !stakeOptionsGrid.contains(btn)) return;
      const available = btn.getAttribute("data-available") === "1";
      if (!available) {
        openUnavailable();
        return;
      }

      const stakeAmount = Number(btn.getAttribute("data-stake") || 100);
      await withButtonLoading(btn, async () => {
        const xchangeModule = await loadXchangeModule();
        await xchangeModule.ensureXchangeState(user?.uid);
        const state = xchangeModule.getXchangeState(window.__userBaseBalance || window.__userBalance || 0, user?.uid);
        if ((state?.does || 0) < stakeAmount) {
          closeStakeSelection();
          if (doesRequiredOverlay) {
            doesRequiredOverlay.classList.remove("hidden");
            doesRequiredOverlay.classList.add("flex");
          }
          return;
        }
        closeStakeSelection();
        showGlobalLoading("Ouverture de la partie...");
        window.location.href = `./jeu.html?autostart=1&stake=${stakeAmount}`;
      }, { loadingLabel: "Vérification..." });
    });
  }

  if (doesRequiredClose) {
    doesRequiredClose.addEventListener("click", () => {
      doesRequiredOverlay?.classList.add("hidden");
      doesRequiredOverlay?.classList.remove("flex");
    });
  }
  if (doesRequiredOpenProfile) {
    doesRequiredOpenProfile.addEventListener("click", () => {
      doesRequiredOverlay?.classList.add("hidden");
      doesRequiredOverlay?.classList.remove("flex");
      openProfilePage();
    });
  }
  if (doesRequiredOverlay) {
    doesRequiredOverlay.addEventListener("click", (ev) => {
      if (ev.target === doesRequiredOverlay) {
        doesRequiredOverlay.classList.add("hidden");
        doesRequiredOverlay.classList.remove("flex");
      }
    });
  }

  if (financeNoticeCloseBtn) {
    financeNoticeCloseBtn.addEventListener("click", () => {
      void acknowledgeActivePage2FinanceNotice();
    });
  }
  if (financeNoticeOverlay) {
    financeNoticeOverlay.addEventListener("click", (event) => {
      if (event.target === financeNoticeOverlay) {
        void acknowledgeActivePage2FinanceNotice();
      }
    });
  }
  if (financeNoticePanel) {
    financeNoticePanel.addEventListener("click", (event) => {
      event.stopPropagation();
    });
  }

  bindDeferredModalTrigger(soldBadgeBtn, () => ensureSoldeUiReady("#soldBadge"), "Chargement du solde...");
  applyPage2AccountState({});

  if (hasConfirmedAuth) {
    void refreshPage2AccountState(page2PresenceUser);
  }

  const effectiveUser = hasConfirmedAuth ? user : null;
  renderSharePromoTargets();
  setPendingShareSource("");
  applySharePromoState(null);
  scheduleNonCriticalTask(runId, () => ensureStakeOptionsLoaded(), 360);
  scheduleNonCriticalTask(runId, () => loadSharePromoStatus(), 420);
  scheduleNonCriticalTask(runId, () => loadSurveyPrompt(), 520);
  scheduleNonCriticalTask(runId, () => {
    initDiscussionFab(effectiveUser);
    initAgentSupportAlert(effectiveUser);
    startPage2NonCriticalPolling(effectiveUser);
    startPage2FinanceNoticeWatchers(effectiveUser);
  }, 460);
  void runPage2BootstrapFlow({
    runId,
    user,
    isAuthenticated,
    hasConfirmedAuth,
  });
}
