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
} from "./secure-functions.js";
import { auth, db, collection, query, orderBy, limit, onSnapshot, doc, setDoc, serverTimestamp } from "./firebase-init.js";

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
const DEFAULT_GAME_STAKE_OPTIONS = Object.freeze([
  Object.freeze({ id: "stake_100", stakeDoes: 100, rewardDoes: 300, enabled: true, sortOrder: 10 }),
  Object.freeze({ id: "stake_500", stakeDoes: 500, rewardDoes: 1500, enabled: false, sortOrder: 20 }),
  Object.freeze({ id: "stake_1000", stakeDoes: 1000, rewardDoes: 3000, enabled: false, sortOrder: 30 }),
  Object.freeze({ id: "stake_5000", stakeDoes: 5000, rewardDoes: 15000, enabled: false, sortOrder: 40 }),
]);
let page2ChatLatestUnsub = null;
let page2ChatSeenUnsub = null;
let page2SupportThreadUnsub = null;
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
const PAGE2_PRESENCE_PING_MS = 60 * 1000;

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
  if (page2ChatLatestUnsub) {
    page2ChatLatestUnsub();
    page2ChatLatestUnsub = null;
  }
  if (page2ChatSeenUnsub) {
    page2ChatSeenUnsub();
    page2ChatSeenUnsub = null;
  }
  if (page2SupportThreadUnsub) {
    page2SupportThreadUnsub();
    page2SupportThreadUnsub = null;
  }
}

function tsToMs(value) {
  if (!value) return 0;
  if (typeof value.toMillis === "function") return value.toMillis();
  if (typeof value.toDate === "function") return value.toDate().getTime();
  if (typeof value.seconds === "number") return value.seconds * 1000;
  const parsed = Date.parse(String(value));
  return Number.isFinite(parsed) ? parsed : 0;
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

  stopPage2ChatWatchers();

  let latestMessageMs = 0;
  let seenMs = 0;
  const renderBadge = () => {
    const liveBadge = document.getElementById("discussionFabBadge");
    if (!liveBadge) {
      stopPage2ChatWatchers();
      return;
    }
    const unread = latestMessageMs > 0 && latestMessageMs > seenMs;
    liveBadge.classList.toggle("hidden", !unread);
  };

  page2ChatLatestUnsub = onSnapshot(
    query(collection(db, CHAT_COLLECTION), orderBy("createdAt", "desc"), limit(1)),
    (snap) => {
      if (snap.empty) {
        latestMessageMs = 0;
        renderBadge();
        return;
      }
      const data = snap.docs[0].data() || {};
      latestMessageMs = tsToMs(data.createdAt);
      renderBadge();
    },
    (err) => {
      console.error("Erreur listener messages discussion:", err);
      latestMessageMs = 0;
      renderBadge();
    }
  );

  page2ChatSeenUnsub = onSnapshot(
    doc(db, "clients", uid),
    (snap) => {
      const data = snap.exists() ? (snap.data() || {}) : {};
      seenMs = tsToMs(data.chatLastSeenAt);
      renderBadge();
    },
    (err) => {
      console.error("Erreur listener lastSeen discussion:", err);
    }
  );
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

  page2SupportThreadUnsub = onSnapshot(
    doc(db, SUPPORT_THREADS_COLLECTION, `user_${uid}`),
    (snap) => {
      const data = snap.exists() ? (snap.data() || {}) : {};
      const unread = data.unreadForUser === true && String(data.lastSenderRole || "") === "agent";
      alertWrap.classList.toggle("hidden", !unread);
      if (!unread) return;

      const preview = String(data.lastMessageText || "").trim();
      alertText.textContent = preview
        ? `Vous avez recu un message par un agent: ${preview}`
        : "Vous avez recu un message par un agent.";
    },
    (err) => {
      console.error("Erreur listener alerte agent:", err);
      alertWrap.classList.add("hidden");
    }
  );
}

export function renderPage2(user, options = {}) {
  stopPage2ChatWatchers();
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
  let sharePromoState = null;
  let sharePromoStatusPromise = null;
  let sharePromoActionInFlight = false;
  let pendingShareSource = "";

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
        sharePromoStatusText.textContent = `Bravo, tes ${SHARE_SITE_PROMO_REWARD_DOES} Does bonus ont deja ete credites.`;
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
        return options;
      })
      .catch((error) => {
        console.warn("[GAME_STAKES] render fallback", error);
        const fallback = normalizeGameStakeOptions();
        renderStakeOptions(fallback);
        return fallback;
      });
    return stakeOptionsHydrationPromise;
  };

  renderStakeOptions(currentStakeOptions);

  if (startGameBtn) {
    startGameBtn.addEventListener("click", () => {
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

  if (stakeSelectionClose) stakeSelectionClose.addEventListener("click", closeStakeSelection);
  if (stakeSelectionOverlay) {
    stakeSelectionOverlay.addEventListener("click", (ev) => {
      if (ev.target === stakeSelectionOverlay) closeStakeSelection();
    });
  }
  if (stakeSelectionPanel) {
    stakeSelectionPanel.addEventListener("click", (ev) => ev.stopPropagation());
  }

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
      if (hasSeenTournamentIntro()) {
        continueToTournament();
        return;
      }
      openTournamentIntro();
    });
  }

  if (sharePromoBtn) {
    sharePromoBtn.addEventListener("click", () => {
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
      void loadSharePromoStatus();
    });
  }

  sharePromoCloseBtn?.addEventListener("click", closeSharePromo);
  sharePromoOverlay?.addEventListener("click", (ev) => {
    if (ev.target === sharePromoOverlay) {
      closeSharePromo();
    }
  });
  sharePromoPanel?.addEventListener("click", (ev) => {
    ev.stopPropagation();
  });
  sharePromoTargetGrid?.addEventListener("click", async (event) => {
    const btn = event.target.closest("[data-share-target]");
    if (!btn || sharePromoActionInFlight || sharePromoState?.isCoolingDown) return;
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
    if (sharePromoActionInFlight || !hasConfirmedAuth || !pendingShareSource) return;
    if (sharePromoState?.isCoolingDown) return;
    try {
      setSharePromoActionLoading(true, "Validation du bonus...");
      const result = await recordShareSitePromoSecure({
        actionId: makePromoActionId(),
        shareSource: pendingShareSource,
      });
      applySharePromoState(result);
      setPendingShareSource("");
      if (result?.rewardGrantedNow && sharePromoStatusText) {
        sharePromoStatusText.textContent = `Bravo, ${SHARE_SITE_PROMO_REWARD_DOES} Does bonus ont ete ajoutes a ton compte.`;
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

  bindDeferredModalTrigger(soldBadgeBtn, () => ensureSoldeUiReady("#soldBadge"), "Chargement du solde...");

  const effectiveUser = hasConfirmedAuth ? user : null;
  renderSharePromoTargets();
  setPendingShareSource("");
  applySharePromoState(null);
  scheduleNonCriticalTask(runId, () => ensureStakeOptionsLoaded(), 360);
  scheduleNonCriticalTask(runId, () => loadSharePromoStatus(), 420);
  scheduleNonCriticalTask(runId, () => {
    initDiscussionFab(effectiveUser);
    initAgentSupportAlert(effectiveUser);
  }, 460);
  void runPage2BootstrapFlow({
    runId,
    user,
    isAuthenticated,
    hasConfirmedAuth,
  });
}
