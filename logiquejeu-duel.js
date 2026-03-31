import {
  auth,
  db,
  collection,
  doc,
  query,
  orderBy,
  onSnapshot,
  onAuthStateChanged,
} from "./firebase-init.js";
import { ensureXchangeState, getXchangeState } from "./xchange.js";
import {
  getPublicDuelStakeOptionsSecure,
  joinMatchmakingDuelSecure,
  ensureRoomReadyDuelSecure,
  touchRoomPresenceDuelSecure,
  ackRoomStartSeenDuelSecure,
  leaveRoomDuelSecure,
  submitActionDuelSecure,
  claimWinRewardDuelSecure,
} from "./secure-functions.js";

const DUEL_ROOMS = "duelRooms";
const ALLOWED_DUEL_STAKE_AMOUNTS = Object.freeze([500, 1000]);
const DEFAULT_DUEL_STAKE_OPTIONS = Object.freeze([
  Object.freeze({ stakeDoes: 500, rewardDoes: 925, enabled: true, sortOrder: 10 }),
  Object.freeze({ stakeDoes: 1000, rewardDoes: 1850, enabled: true, sortOrder: 20 }),
]);
const TILE_VALUES = Object.freeze([
  [0, 0], [0, 1], [0, 2], [0, 3], [0, 4], [0, 5], [0, 6],
  [1, 1], [1, 2], [1, 3], [1, 4], [1, 5], [1, 6],
  [2, 2], [2, 3], [2, 4], [2, 5], [2, 6],
  [3, 3], [3, 4], [3, 5], [3, 6],
  [4, 4], [4, 5], [4, 6],
  [5, 5], [5, 6],
  [6, 6],
]);
const DUEL_DEBUG = false;
const URL_PARAMS = new URLSearchParams(window.location.search);
const SUPPORT_RETURN_URL = "./index.html";
const PRESENCE_PING_MS = 20 * 1000;
const DUEL_ABANDONED_ROOMS_STORAGE_KEY = "domino_duel_abandoned_rooms_v1";

let duelRoot = null;
let roomUnsub = null;
let actionsUnsub = null;
let presenceTimer = null;
let bootTimer = null;
let currentRoomId = "";
let currentSeatIndex = -1;
let currentRoomData = null;
let currentUser = null;
let authReady = false;
let currentActions = [];
let duelDeckOrder = [];
let availableStakeOptions = DEFAULT_DUEL_STAKE_OPTIONS.map((item) => ({ ...item }));
let selectedStakeDoes = 500;
let statusMessage = "";
let joining = false;
let ensuringRoom = false;
let actionSending = false;
let rewardClaiming = false;
let actionsReady = false;
let startRevealAcked = false;
let startRevealAcking = false;
let pendingSideChooser = null;
let duelJoinAutoStarted = false;
let gameLaunched = false;
let turnTimer = null;
let turnTick = null;
let turnTimeoutRequestInFlight = false;
let fullscreenHintTimer = null;
let onlineUsersTick = null;
let onlineUsersBucket = -1;
let lotModalOpen = false;
let lotActionSending = false;
let openingRuleNoticeMessage = "";
let openingRuleNoticeTimer = null;
let openingRuleNoticeDelayTimer = null;
let openingRuleNoticeShownKey = "";
let duelLotScene = null;
let duelLotCamera = null;
let duelLotRenderer = null;
let duelLotViewport = null;
let duelLotLightsReady = false;
let duelLotTileEntries = [];
let duelLotRaycaster = null;
let duelLotPointer = null;

const DUEL_LOT_COLUMNS = 7;
const DUEL_LOT_TILE_SCALE = 1.34;
const DUEL_LOT_TILE_WIDTH = 1.0 * DUEL_LOT_TILE_SCALE;
const DUEL_LOT_TILE_HEIGHT = 2.0 * DUEL_LOT_TILE_SCALE;
const DUEL_LOT_GAP_X = 0.22;
const DUEL_LOT_GAP_Z = 0.58;

const TURN_LIMIT_SECONDS = 15;
const TURN_LIMIT_MS = TURN_LIMIT_SECONDS * 1000;
const ONLINE_USERS_MIN = 30000;
const ONLINE_USERS_MAX = 100000;
const ONLINE_USERS_STEP_MS = 30000;
const ONLINE_USERS_WAVE_PERIOD = 29;
const ONLINE_USERS_SEED = 0x9e3779b9;

function duelDebug(label, payload = {}) {
  if (!DUEL_DEBUG) return;
  console.log(`[DUEL_DEBUG] ${label}`, payload);
}

function readAbandonedDuelRoomIds() {
  try {
    const raw = window.localStorage.getItem(DUEL_ABANDONED_ROOMS_STORAGE_KEY);
    const parsed = JSON.parse(raw || "[]");
    if (!Array.isArray(parsed)) return [];
    return parsed
      .map((item) => String(item || "").trim())
      .filter(Boolean)
      .slice(0, 8);
  } catch (_) {
    return [];
  }
}

function writeAbandonedDuelRoomIds(roomIds = []) {
  try {
    const normalized = Array.from(new Set(
      (Array.isArray(roomIds) ? roomIds : [])
        .map((item) => String(item || "").trim())
        .filter(Boolean)
    )).slice(0, 8);
    if (normalized.length > 0) {
      window.localStorage.setItem(DUEL_ABANDONED_ROOMS_STORAGE_KEY, JSON.stringify(normalized));
    } else {
      window.localStorage.removeItem(DUEL_ABANDONED_ROOMS_STORAGE_KEY);
    }
  } catch (_) {}
}

function rememberAbandonedDuelRoom(roomId = "") {
  const safeRoomId = String(roomId || "").trim();
  if (!safeRoomId) return;
  writeAbandonedDuelRoomIds([safeRoomId].concat(readAbandonedDuelRoomIds()));
}

function forgetAbandonedDuelRoom(roomId = "") {
  const safeRoomId = String(roomId || "").trim();
  if (!safeRoomId) return;
  writeAbandonedDuelRoomIds(readAbandonedDuelRoomIds().filter((item) => item !== safeRoomId));
}

function leaveCurrentRoomOnLifecycleExit(reason = "") {
  const roomId = String(currentRoomId || "").trim();
  if (!roomId) return;
  rememberAbandonedDuelRoom(roomId);
  if (navigator.onLine === false || !currentUser?.uid) return;
  leaveRoomDuelSecure({ roomId, reason: String(reason || "lifecycle-exit") }).catch(() => null);
}

function safeInt(value, fallback = 0) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  return Number.isFinite(parsed) ? Math.max(0, Math.trunc(parsed)) : fallback;
}

function safeSignedInt(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.trunc(parsed) : fallback;
}

function getFriendDuelRoomIdFromUrl() {
  return String(URL_PARAMS.get("friendDuelRoomId") || "").trim();
}

function isFriendDuelFlowFromUrl() {
  return getFriendDuelRoomIdFromUrl().length > 0 || String(URL_PARAMS.get("roomMode") || "").trim() === "duel_friends";
}

function setMatchLoading(visible, text) {
  const overlay = document.getElementById("MatchLoadingOverlay");
  const textNode = document.getElementById("MatchLoadingText");
  const roomStatus = String(currentRoomData?.status || "").trim();
  const shouldSuppressOverlay = roomStatus === "playing";
  if (textNode && typeof text === "string" && text.length > 0) {
    textNode.textContent = text;
  }
  if (!overlay) return;
  if (shouldSuppressOverlay) {
    overlay.classList.add("hidden");
    overlay.classList.remove("flex");
    return;
  }
  if (visible) {
    overlay.classList.remove("hidden");
    overlay.classList.add("flex");
    return;
  }
  overlay.classList.add("hidden");
  overlay.classList.remove("flex");
}

function escapeHtml(value = "") {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function formatDoes(value) {
  return `${safeInt(value).toLocaleString("fr-FR")} Does`;
}

function formatShortDuration(ms) {
  const safeMs = Math.max(0, safeSignedInt(ms));
  const totalSeconds = Math.ceil(safeMs / 1000);
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return `${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
}

function randomBetween(min, max) {
  const safeMin = Math.max(0, Number(min) || 0);
  const safeMax = Math.max(safeMin, Number(max) || safeMin);
  return Math.floor(Math.random() * (safeMax - safeMin + 1)) + safeMin;
}

function computeSharedOnlineUsers(nowMs) {
  const bucket = Math.floor(Math.max(0, nowMs) / ONLINE_USERS_STEP_MS);
  const phase = bucket % ONLINE_USERS_WAVE_PERIOD;
  const wave = Math.sin((phase / ONLINE_USERS_WAVE_PERIOD) * Math.PI * 2);
  const seed = (((bucket + ONLINE_USERS_SEED) * 1664525) + 1013904223) >>> 0;
  const noise = ((seed % 1000) / 999) - 0.5;
  const center = Math.round((ONLINE_USERS_MIN + ONLINE_USERS_MAX) / 2);
  const amplitude = Math.round((ONLINE_USERS_MAX - ONLINE_USERS_MIN) / 2);
  const candidate = center + Math.round((wave * 0.75 + noise * 0.25) * amplitude);
  return Math.max(ONLINE_USERS_MIN, Math.min(ONLINE_USERS_MAX, candidate));
}

function setOnlineUsersHud(count) {
  const hud = document.getElementById("OnlineUsersHud");
  if (!hud) return;
  hud.textContent = `Utilisateurs en ligne: ${safeInt(count).toLocaleString("fr-FR")}`;
}

function setStatus(message = "") {
  statusMessage = String(message || "").trim();
  renderApp();
}

function clearOpeningRuleNotice() {
  if (openingRuleNoticeTimer) {
    window.clearTimeout(openingRuleNoticeTimer);
    openingRuleNoticeTimer = null;
  }
  if (openingRuleNoticeDelayTimer) {
    window.clearTimeout(openingRuleNoticeDelayTimer);
    openingRuleNoticeDelayTimer = null;
  }
  openingRuleNoticeMessage = "";
}

function getOpeningIntroRemainingMs() {
  if (!currentRoomData || String(currentRoomData.status || "") !== "playing") return 0;
  if (currentRoomData.startRevealPending === true) return 250;
  const startedAtMs = tsToMs(currentRoomData.startedAt) || safeSignedInt(currentRoomData.startedAtMs, 0);
  if (startedAtMs <= 0) return 0;
  const introDurationMs = Math.max(
    0,
    safeSignedInt(window?.Domino?.Partida?.DuracionAnimacionInicio, 5000)
  );
  if (introDurationMs <= 0) return 0;
  return Math.max(0, introDurationMs - (Date.now() - startedAtMs));
}

function buildOpeningRuleNoticeHtml() {
  if (!openingRuleNoticeMessage) return "";
  return `
    <section class="pointer-events-none fixed inset-x-0 top-[max(1.2rem,env(safe-area-inset-top))] z-[2710] flex justify-center px-4">
      <div class="w-[min(92vw,29rem)] rounded-[24px] border border-amber-200/28 bg-[linear-gradient(180deg,rgba(82,58,25,0.94),rgba(44,28,10,0.96))] px-5 py-4 text-center text-sm font-semibold leading-6 text-[#fff4dc] shadow-[0_18px_42px_rgba(0,0,0,0.35)] backdrop-blur-xl">
        ${escapeHtml(openingRuleNoticeMessage)}
      </div>
    </section>
  `;
}

function maybeShowOpeningRuleNotice() {
  if (!currentRoomId || !currentRoomData || String(currentRoomData.status || "") !== "playing") return;
  if (safeSignedInt(currentRoomData.lastActionSeq, -1) > 0) return;
  const introRemainingMs = getOpeningIntroRemainingMs();
  if (introRemainingMs > 0) {
    if (!openingRuleNoticeDelayTimer) {
      openingRuleNoticeDelayTimer = window.setTimeout(() => {
        openingRuleNoticeDelayTimer = null;
        maybeShowOpeningRuleNotice();
      }, Math.max(180, introRemainingMs + 120));
    }
    return;
  }
  const openingReason = String(currentRoomData.openingReason || "").trim().toLowerCase();
  let message = "";
  if (openingReason === "highest_double") {
    message = "Personne n'a le 6-6. Le plus grand domino double commence.";
  } else if (openingReason === "highest_sum") {
    message = "Personne n'a de domino double. Le domino avec la somme la plus elevee commence.";
  } else {
    return;
  }

  const noticeKey = `${currentRoomId}:${safeSignedInt(currentRoomData.startedAtMs, 0)}:${openingReason}:${safeSignedInt(currentRoomData.openingTileId, -1)}`;
  if (openingRuleNoticeShownKey === noticeKey) return;
  openingRuleNoticeShownKey = noticeKey;
  openingRuleNoticeMessage = message;
  renderApp();
  if (openingRuleNoticeTimer) window.clearTimeout(openingRuleNoticeTimer);
  openingRuleNoticeTimer = window.setTimeout(() => {
    openingRuleNoticeTimer = null;
    openingRuleNoticeMessage = "";
    renderApp();
  }, 3000);
}

function refreshDoesHud() {
  const el = document.getElementById("LocalDoesValue");
  if (!el) return;
  try {
    const uid = auth.currentUser?.uid || "guest";
    const xState = getXchangeState(window.__userBaseBalance || window.__userBalance || 0, uid);
    el.textContent = String(xState?.does || 0);
  } catch (_) {
    el.textContent = "0";
  }
}

function fullscreenElement() {
  return document.fullscreenElement || document.webkitFullscreenElement || document.msFullscreenElement || null;
}

function requestFullscreenCompat(target) {
  if (!target) return null;
  if (typeof target.requestFullscreen === "function") return target.requestFullscreen();
  if (typeof target.webkitRequestFullscreen === "function") return target.webkitRequestFullscreen();
  if (typeof target.msRequestFullscreen === "function") return target.msRequestFullscreen();
  return null;
}

function exitFullscreenCompat() {
  if (typeof document.exitFullscreen === "function") return document.exitFullscreen();
  if (typeof document.webkitExitFullscreen === "function") return document.webkitExitFullscreen();
  if (typeof document.msExitFullscreen === "function") return document.msExitFullscreen();
  return null;
}

function isFullscreenSupported() {
  const root = document.documentElement;
  return Boolean(
    (root && (
      typeof root.requestFullscreen === "function" ||
      typeof root.webkitRequestFullscreen === "function" ||
      typeof root.msRequestFullscreen === "function"
    )) ||
    typeof document.exitFullscreen === "function" ||
    typeof document.webkitExitFullscreen === "function" ||
    typeof document.msExitFullscreen === "function"
  );
}

function isIOSDevice() {
  const ua = String(navigator?.userAgent || "");
  const iOSUA = /iPad|iPhone|iPod/i.test(ua);
  const iPadOS = navigator?.platform === "MacIntel" && ((navigator?.maxTouchPoints || 0) > 1);
  return iOSUA || iPadOS;
}

function isStandaloneDisplayMode() {
  return Boolean(
    window.matchMedia?.("(display-mode: standalone)")?.matches ||
    navigator?.standalone === true
  );
}

function setFullscreenHint(text) {
  const hint = document.getElementById("FullscreenHint");
  if (!hint) return;
  hint.textContent = text || "Plein ecran non supporte sur cet appareil.";
  hint.classList.remove("hidden");
  if (fullscreenHintTimer) clearTimeout(fullscreenHintTimer);
  fullscreenHintTimer = setTimeout(() => {
    const liveHint = document.getElementById("FullscreenHint");
    if (!liveHint) return;
    liveHint.classList.add("hidden");
  }, 2600);
}

function setFullscreenIcon(isActive) {
  const icon = document.getElementById("FullscreenToggleIcon");
  if (!icon) return;
  icon.innerHTML = isActive
    ? `
      <path d="M9 9H5V5"></path>
      <path d="M15 9h4V5"></path>
      <path d="M9 15H5v4"></path>
      <path d="M15 15h4v4"></path>
    `
    : `
      <path d="M8 3H5a2 2 0 0 0-2 2v3"></path>
      <path d="M16 3h3a2 2 0 0 1 2 2v3"></path>
      <path d="M8 21H5a2 2 0 0 1-2-2v-3"></path>
      <path d="M16 21h3a2 2 0 0 0 2-2v-3"></path>
    `;
}

function setHudViewIcon(isMinimal) {
  const icon = document.getElementById("HudViewToggleIcon");
  if (!icon) return;
  icon.innerHTML = isMinimal
    ? `
      <path d="M2 12s3.6-6 10-6c2.24 0 4.13.74 5.7 1.72"></path>
      <path d="M20.06 16.94C18.34 18.21 15.95 19 12 19c-6.4 0-10-7-10-7a21.8 21.8 0 0 1 4.31-4.92"></path>
      <path d="M10.58 10.58A2 2 0 0 0 10 12a2 2 0 0 0 2 2c.52 0 1-.2 1.36-.54"></path>
      <path d="M3 3l18 18"></path>
    `
    : `
      <path d="M2 12s3.6-6 10-6 10 6 10 6-3.6 6-10 6S2 12 2 12Z"></path>
      <circle cx="12" cy="12" r="2.8"></circle>
    `;
}

function syncFloatingHudVisibility() {
  const goBtn = document.getElementById("GameEndGoBtn");
  const hudViewBtn = document.getElementById("HudViewToggleBtn");
  const goVisible = !!(goBtn && !goBtn.classList.contains("hidden"));
  if (hudViewBtn) {
    hudViewBtn.classList.toggle("hidden", goVisible);
    hudViewBtn.classList.toggle("grid", !goVisible);
  }
}

function readHudMinimalMode() {
  try {
    return window.localStorage?.getItem("domino_duel_hud_minimal_v1") === "1";
  } catch (_) {
    return false;
  }
}

function writeHudMinimalMode(isMinimal) {
  try {
    if (isMinimal) window.localStorage?.setItem("domino_duel_hud_minimal_v1", "1");
    else window.localStorage?.removeItem("domino_duel_hud_minimal_v1");
  } catch (_) {}
}

function applyHudMinimalMode(isMinimal) {
  document.body?.classList.toggle("game-hud-minimal", !!isMinimal);
  const btn = document.getElementById("HudViewToggleBtn");
  if (btn) {
    const label = isMinimal ? "Afficher les panneaux" : "Masquer les panneaux";
    btn.setAttribute("aria-pressed", isMinimal ? "true" : "false");
    btn.setAttribute("aria-label", label);
    btn.setAttribute("title", label);
  }
  setHudViewIcon(!!isMinimal);
  syncFloatingHudVisibility();
}

function toggleHudMinimalMode() {
  const nextValue = !(document.body?.classList.contains("game-hud-minimal"));
  applyHudMinimalMode(nextValue);
  writeHudMinimalMode(nextValue);
}

function bindHudViewToggle() {
  const btn = document.getElementById("HudViewToggleBtn");
  if (!btn || btn.dataset.bound === "1") return;
  btn.dataset.bound = "1";
  btn.addEventListener("click", () => {
    toggleHudMinimalMode();
  });
  applyHudMinimalMode(readHudMinimalMode());
}

function syncFullscreenButtonState() {
  const btn = document.getElementById("FullscreenToggleBtn");
  if (!btn) return;
  const active = !!fullscreenElement();
  btn.setAttribute("aria-label", active ? "Quitter le plein ecran" : "Passer en plein ecran");
  btn.setAttribute("title", active ? "Quitter le plein ecran" : "Passer en plein ecran");
  setFullscreenIcon(active);
}

async function toggleFullscreen() {
  if (!isFullscreenSupported()) {
    if (isIOSDevice() && !isStandaloneDisplayMode()) {
      setFullscreenHint("Safari iPhone ne supporte pas ce plein ecran. Ajoute le site a l'ecran d'accueil pour un mode plein ecran.");
      return;
    }
    setFullscreenHint("Plein ecran non supporte sur cet appareil.");
    return;
  }
  try {
    if (fullscreenElement()) {
      await exitFullscreenCompat();
    } else {
      const result = requestFullscreenCompat(document.documentElement);
      if (result === null) {
        setFullscreenHint("Plein ecran non supporte sur cet appareil.");
        return;
      }
      if (result && typeof result.then === "function") await result;
    }
  } catch (_) {
    setFullscreenHint("Impossible d'activer le plein ecran.");
  } finally {
    syncFullscreenButtonState();
  }
}

function bindFullscreenToggle() {
  const btn = document.getElementById("FullscreenToggleBtn");
  if (!btn || btn.dataset.bound === "1") return;
  btn.dataset.bound = "1";
  btn.addEventListener("click", () => {
    toggleFullscreen();
  });
  document.addEventListener("fullscreenchange", syncFullscreenButtonState);
  document.addEventListener("webkitfullscreenchange", syncFullscreenButtonState);
  document.addEventListener("msfullscreenchange", syncFullscreenButtonState);
  document.addEventListener("MSFullscreenChange", syncFullscreenButtonState);
  syncFullscreenButtonState();
}

function clearTurnTimer() {
  if (turnTimer) {
    clearTimeout(turnTimer);
    turnTimer = null;
  }
  if (turnTick) {
    clearInterval(turnTick);
    turnTick = null;
  }
  const el = document.getElementById("TurnTimer");
  if (el) {
    el.textContent = "--";
    el.setAttribute("Urgent", "false");
  }
  const labelEl = document.getElementById("LocalTurnLabel");
  const valueEl = document.getElementById("LocalTurnValue");
  if (labelEl) labelEl.textContent = "En attente";
  if (valueEl) valueEl.textContent = "--";
}

function isLocalHumanTurn(currentPlayer) {
  const S = window.GameSession || null;
  const localSeat = (S && typeof S.seatIndex === "number") ? S.seatIndex : -1;
  let isLocalTurn = (typeof currentPlayer === "number" && localSeat === currentPlayer);
  const partida = window.Domino && window.Domino.Partida ? window.Domino.Partida : null;
  if (partida && partida.ModoRehidratacion !== true && typeof partida.EsTurnoHumanoLocal === "function") {
    isLocalTurn = partida.EsTurnoHumanoLocal() === true;
  }
  return isLocalTurn;
}

function setTurnTimerUI(remainingSec, currentPlayer) {
  const isLocalTurn = isLocalHumanTurn(currentPlayer);

  const legacy = document.getElementById("TurnTimer");
  const labelEl = document.getElementById("LocalTurnLabel");
  const valueEl = document.getElementById("LocalTurnValue");
  if (!isLocalTurn) {
    if (legacy) {
      legacy.textContent = "--";
      legacy.setAttribute("Urgent", "false");
    }
    if (labelEl) labelEl.textContent = "En attente";
    if (valueEl) valueEl.textContent = "--";
    return;
  }

  const safe = Math.max(0, Math.ceil(remainingSec));
  if (legacy) {
    legacy.textContent = String(safe);
    legacy.setAttribute("Urgent", safe <= 5 ? "true" : "false");
  }
  if (labelEl) labelEl.textContent = "Ton tour";
  if (valueEl) valueEl.textContent = String(safe);
}

async function maybeRequestTurnTimeoutResolution(reason = "") {
  if (
    turnTimeoutRequestInFlight ||
    !currentRoomId ||
    !currentRoomData ||
    String(currentRoomData.status || "") !== "playing" ||
    currentRoomData.startRevealPending === true
  ) {
    return;
  }

  const deadlineMs = safeSignedInt(currentRoomData.turnDeadlineMs, 0);
  if (deadlineMs > 0 && Date.now() + 120 < deadlineMs) return;

  turnTimeoutRequestInFlight = true;
  duelDebug("turn-timeout:nudge", { reason, roomId: currentRoomId });
  try {
    await touchRoomPresenceDuelSecure({ roomId: currentRoomId });
  } catch (error) {
    console.warn("[DUEL] timeout nudge failed", error);
  } finally {
    turnTimeoutRequestInFlight = false;
  }
}

function scheduleTurnTimeout(roomData) {
  clearTurnTimer();
  if (!roomData || String(roomData.status || "") !== "playing") return;
  if (roomData.startRevealPending === true) return;
  if (typeof roomData.currentPlayer !== "number") return;
  const deadlineMs = safeSignedInt(roomData.turnDeadlineMs, 0);
  const turnStartedMs = safeSignedInt(roomData.turnStartedAtMs, 0) || tsToMs(roomData.turnStartedAt);
  const elapsedMs = turnStartedMs > 0 ? Math.max(0, Date.now() - turnStartedMs) : 0;
  const remainingMs = deadlineMs > 0 ? Math.max(0, deadlineMs - Date.now()) : Math.max(0, TURN_LIMIT_MS - elapsedMs);
  const baseStartMs = deadlineMs > 0 ? (deadlineMs - TURN_LIMIT_MS) : (Date.now() - elapsedMs);
  setTurnTimerUI(remainingMs / 1000, roomData.currentPlayer);
  turnTick = setInterval(() => {
    const liveLeft = Math.max(0, TURN_LIMIT_MS - (Date.now() - baseStartMs));
    setTurnTimerUI(liveLeft / 1000, roomData.currentPlayer);
  }, 250);
  turnTimer = setTimeout(() => {
    setTurnTimerUI(0, roomData.currentPlayer);
    void maybeRequestTurnTimeoutResolution("turn-expired");
  }, remainingMs);
}

function startOnlineUsersTicker() {
  const hud = document.getElementById("OnlineUsersHud");
  if (!hud) return;
  if (onlineUsersTick) {
    clearInterval(onlineUsersTick);
    onlineUsersTick = null;
  }
  onlineUsersBucket = -1;
  const refreshHud = () => {
    const now = Date.now();
    const nextBucket = Math.floor(now / ONLINE_USERS_STEP_MS);
    if (nextBucket === onlineUsersBucket) return;
    onlineUsersBucket = nextBucket;
    setOnlineUsersHud(computeSharedOnlineUsers(now));
  };
  refreshHud();
  onlineUsersTick = setInterval(refreshHud, 1000);
}

function normalizeDeckOrder(raw) {
  if (!Array.isArray(raw) || raw.length !== 28) return [];
  const seen = new Set();
  const out = [];
  for (let i = 0; i < raw.length; i += 1) {
    const tileId = Number(raw[i]);
    if (!Number.isFinite(tileId) || tileId < 0 || tileId >= 28 || seen.has(tileId)) {
      return [];
    }
    seen.add(tileId);
    out.push(Math.trunc(tileId));
  }
  return out;
}

function normalizeStakeOptions(rawOptions = []) {
  const source = Array.isArray(rawOptions) && rawOptions.length ? rawOptions : DEFAULT_DUEL_STAKE_OPTIONS;
  return source
    .map((item) => ({
      stakeDoes: safeInt(item?.stakeDoes),
      rewardDoes: safeInt(item?.rewardDoes),
      enabled: item?.enabled !== false,
      sortOrder: safeInt(item?.sortOrder || 0),
    }))
    .filter((item) => item.stakeDoes > 0 && item.rewardDoes > 0 && ALLOWED_DUEL_STAKE_AMOUNTS.includes(item.stakeDoes))
    .sort((left, right) => left.sortOrder - right.sortOrder || left.stakeDoes - right.stakeDoes);
}

function readStakeFromUrl() {
  const configured = normalizeStakeOptions(availableStakeOptions);
  const fallback = configured[0]?.stakeDoes || DEFAULT_DUEL_STAKE_OPTIONS[0].stakeDoes;
  const rawStake = safeInt(URL_PARAMS.get("stake"), fallback);
  const found = configured.find((item) => item.stakeDoes === rawStake && item.enabled !== false);
  return found ? found.stakeDoes : fallback;
}

function updateStakeInUrl(stakeDoes) {
  URL_PARAMS.set("stake", String(safeInt(stakeDoes, selectedStakeDoes)));
  const nextUrl = `./jeu-duel.html?${URL_PARAMS.toString()}`;
  window.history.replaceState({}, "", nextUrl);
}

function getSelectedStakeOption() {
  const configured = normalizeStakeOptions(availableStakeOptions);
  return configured.find((item) => item.stakeDoes === selectedStakeDoes) || configured[0] || DEFAULT_DUEL_STAKE_OPTIONS[0];
}

function getTileLabel(tileId) {
  const values = TILE_VALUES[safeSignedInt(tileId, -1)];
  return values ? `${values[0]}|${values[1]}` : "?";
}

function getTileFaceAsset(tileId) {
  const values = TILE_VALUES[safeSignedInt(tileId, -1)];
  return values ? `./Domino.svg#Ficha_${values[1]}-${values[0]}` : "";
}

function cloneSeatHands(seatHands) {
  return Array.isArray(seatHands)
    ? seatHands.map((hand) => (Array.isArray(hand) ? hand.slice() : []))
    : [[], []];
}

function buildSeatHands(deckOrder) {
  if (!Array.isArray(deckOrder) || deckOrder.length !== 28) return [[], []];
  return [deckOrder.slice(0, 7), deckOrder.slice(7, 14)];
}

function buildStockPile(deckOrder) {
  if (!Array.isArray(deckOrder) || deckOrder.length !== 28) return [];
  return deckOrder.slice(14);
}

function findSeatWithTile(seatHands, tileId) {
  for (let seat = 0; seat < 2; seat += 1) {
    const hand = Array.isArray(seatHands?.[seat]) ? seatHands[seat] : [];
    for (let index = 0; index < hand.length; index += 1) {
      if (hand[index] === tileId) return seat;
    }
  }
  return -1;
}

function sumSeatPips(seatHands, seat) {
  const hand = Array.isArray(seatHands?.[seat]) ? seatHands[seat] : [];
  return hand.reduce((sum, tileId) => {
    if (tileId === null) return sum;
    const values = TILE_VALUES[safeSignedInt(tileId, -1)];
    return values ? sum + values[0] + values[1] : sum;
  }, 0);
}

function computeBlockedWinnerSeat(seatHands) {
  let winnerSeat = 0;
  let winnerScore = Number.POSITIVE_INFINITY;
  for (let seat = 0; seat < 2; seat += 1) {
    const score = sumSeatPips(seatHands, seat);
    if (score < winnerScore) {
      winnerScore = score;
      winnerSeat = seat;
    }
  }
  return winnerSeat;
}

function compareDuelOpeningTileIds(leftTileId, rightTileId) {
  const leftValues = TILE_VALUES[safeSignedInt(leftTileId, -1)] || [0, 0];
  const rightValues = TILE_VALUES[safeSignedInt(rightTileId, -1)] || [0, 0];
  const leftIsDouble = leftValues[0] === leftValues[1];
  const rightIsDouble = rightValues[0] === rightValues[1];
  if (leftIsDouble !== rightIsDouble) return leftIsDouble ? 1 : -1;

  if (leftIsDouble && rightIsDouble) {
    if (leftValues[0] !== rightValues[0]) return leftValues[0] > rightValues[0] ? 1 : -1;
    return 0;
  }

  const leftSum = leftValues[0] + leftValues[1];
  const rightSum = rightValues[0] + rightValues[1];
  if (leftSum !== rightSum) return leftSum > rightSum ? 1 : -1;

  const leftHigh = Math.max(leftValues[0], leftValues[1]);
  const rightHigh = Math.max(rightValues[0], rightValues[1]);
  if (leftHigh !== rightHigh) return leftHigh > rightHigh ? 1 : -1;

  const leftLow = Math.min(leftValues[0], leftValues[1]);
  const rightLow = Math.min(rightValues[0], rightValues[1]);
  if (leftLow !== rightLow) return leftLow > rightLow ? 1 : -1;
  return 0;
}

function resolveReplayOpeningConfig(seatHands = [[], []]) {
  let bestDouble = null;
  let bestNonDouble = null;

  for (let seat = 0; seat < 2; seat += 1) {
    const hand = Array.isArray(seatHands?.[seat]) ? seatHands[seat] : [];
    for (let slot = 0; slot < hand.length; slot += 1) {
      const tileId = safeSignedInt(hand[slot], -1);
      const values = TILE_VALUES[tileId];
      if (!values) continue;
      const candidate = { seat, slot, tileId };
      if (values[0] === values[1]) {
        if (!bestDouble || compareDuelOpeningTileIds(tileId, bestDouble.tileId) > 0) bestDouble = candidate;
      } else if (!bestNonDouble || compareDuelOpeningTileIds(tileId, bestNonDouble.tileId) > 0) {
        bestNonDouble = candidate;
      }
    }
  }

  const selected = bestDouble || bestNonDouble;
  if (!selected) {
    return { seat: 0, slot: 0, tileId: 27, reason: "double_six" };
  }

  const values = TILE_VALUES[selected.tileId] || [0, 0];
  return {
    seat: selected.seat,
    slot: selected.slot,
    tileId: selected.tileId,
    reason: values[0] === values[1]
      ? (selected.tileId === 27 ? "double_six" : "highest_double")
      : "highest_sum",
  };
}

function getLegalMovesForSeat(state, seat) {
  const moves = [];
  const hand = Array.isArray(state?.seatHands?.[seat]) ? state.seatHands[seat] : [];
  const openingMove = safeSignedInt(state?.appliedActionSeq) < 0;
  const openingTileId = safeSignedInt(state?.openingTileId, 27);

  for (let slot = 0; slot < hand.length; slot += 1) {
    const tileId = hand[slot];
    if (tileId === null) continue;
    const values = TILE_VALUES[safeSignedInt(tileId, -1)];
    if (!values) continue;

    if (openingMove) {
      if (tileId === openingTileId) {
        moves.push({
          tileId,
          slot,
          side: "center",
          branch: "centro",
          tileLeft: values[0],
          tileRight: values[1],
        });
      }
      continue;
    }

    if (values[0] === state.leftEnd || values[1] === state.leftEnd) {
      moves.push({
        tileId,
        slot,
        side: "left",
        branch: "izquierda",
        tileLeft: values[0],
        tileRight: values[1],
      });
    }
    if (values[0] === state.rightEnd || values[1] === state.rightEnd) {
      moves.push({
        tileId,
        slot,
        side: "right",
        branch: "derecha",
        tileLeft: values[0],
        tileRight: values[1],
      });
    }
  }

  return moves.sort((left, right) => {
    const leftValues = TILE_VALUES[safeSignedInt(left.tileId, -1)] || [0, 0];
    const rightValues = TILE_VALUES[safeSignedInt(right.tileId, -1)] || [0, 0];
    return (rightValues[0] + rightValues[1]) - (leftValues[0] + leftValues[1]);
  });
}

function createInitialReplayState(deckOrder) {
  const normalizedDeck = normalizeDeckOrder(deckOrder);
  const seatHands = buildSeatHands(normalizedDeck);
  const openingConfig = resolveReplayOpeningConfig(seatHands);
  return {
    deckOrder: normalizedDeck,
    seatHands,
    stockPile: buildStockPile(normalizedDeck),
    leftEnd: null,
    rightEnd: null,
    passesInRow: 0,
    appliedActionSeq: -1,
    currentPlayer: Math.max(0, openingConfig.seat),
    openingSeat: openingConfig.seat,
    openingTileId: openingConfig.tileId,
    openingReason: openingConfig.reason,
    winnerSeat: -1,
    winnerUid: "",
    endedReason: "",
  };
}

function removeTileFromHand(hand, tileId) {
  const index = hand.findIndex((candidate) => candidate === tileId);
  if (index >= 0) {
    hand.splice(index, 1);
    return true;
  }
  return false;
}

function applyPlayedTileToEnds(state, tileId, side) {
  const values = TILE_VALUES[safeSignedInt(tileId, -1)];
  if (!values) return;

  if (safeSignedInt(state.appliedActionSeq) < 0 || side === "center") {
    state.leftEnd = values[0];
    state.rightEnd = values[1];
    return;
  }

  if (side === "left") {
    state.leftEnd = values[0] === state.leftEnd ? values[1] : values[0];
    return;
  }

  state.rightEnd = values[0] === state.rightEnd ? values[1] : values[0];
}

function applyActionRecordToReplay(state, action, roomData = null) {
  if (!action || typeof action !== "object") return state;
  const nextState = {
    ...state,
    seatHands: cloneSeatHands(state.seatHands),
    stockPile: Array.isArray(state.stockPile) ? state.stockPile.slice() : [],
  };
  const seat = safeSignedInt(action.player, -1);
  const hand = Array.isArray(nextState.seatHands?.[seat]) ? nextState.seatHands[seat] : [];
  const type = String(action.type || "").trim();

  if (type === "play") {
    removeTileFromHand(hand, safeSignedInt(action.tileId, -1));
    applyPlayedTileToEnds(nextState, safeSignedInt(action.tileId, -1), String(action.side || "center").trim().toLowerCase());
    nextState.passesInRow = 0;
  } else if (type === "draw") {
    const drawnTileIds = Array.isArray(action.drawnTileIds) ? action.drawnTileIds.map((item) => safeSignedInt(item, -1)).filter((item) => item >= 0) : [];
    drawnTileIds.forEach((tileId) => {
      const stockIndex = nextState.stockPile.findIndex((candidate) => candidate === tileId);
      if (stockIndex >= 0) {
        nextState.stockPile.splice(stockIndex, 1);
      }
      hand.push(tileId);
    });
    nextState.passesInRow = 0;
  } else if (type === "pass") {
    nextState.passesInRow += 1;
    if (nextState.passesInRow >= 2) {
      nextState.winnerSeat = computeBlockedWinnerSeat(nextState.seatHands);
      nextState.endedReason = "block";
    }
  }

  if (hand.length === 0 && nextState.winnerSeat < 0 && type === "play") {
    nextState.winnerSeat = seat;
    nextState.endedReason = "out";
  }

  nextState.appliedActionSeq = safeInt(action.seq, nextState.appliedActionSeq);
  if (seat >= 0) {
    nextState.currentPlayer = type === "draw" ? seat : ((seat + 1) % 2);
  }

  if (roomData && safeSignedInt(roomData.winnerSeat, -1) >= 0) {
    nextState.winnerSeat = safeSignedInt(roomData.winnerSeat, -1);
    nextState.winnerUid = String(roomData.winnerUid || "").trim();
    nextState.endedReason = String(roomData.endedReason || nextState.endedReason || "").trim();
  }

  return nextState;
}

function buildReplayState(deckOrder, actions, roomData = null) {
  const state = createInitialReplayState(deckOrder);
  const sortedActions = Array.isArray(actions)
    ? actions
      .slice()
      .filter((item) => item && typeof item === "object")
      .sort((left, right) => safeInt(left.seq) - safeInt(right.seq))
    : [];

  let liveState = state;
  sortedActions.forEach((action) => {
    liveState = applyActionRecordToReplay(liveState, action, roomData);
  });

  if (roomData) {
    if (Number.isFinite(Number(roomData.openingSeat))) {
      liveState.openingSeat = Math.trunc(Number(roomData.openingSeat));
    }
    if (Number.isFinite(Number(roomData.openingTileId))) {
      liveState.openingTileId = Math.trunc(Number(roomData.openingTileId));
    }
    if (typeof roomData.openingReason === "string" && roomData.openingReason.trim()) {
      liveState.openingReason = roomData.openingReason.trim();
    }
    liveState.currentPlayer = safeInt(roomData.currentPlayer, liveState.currentPlayer);
    if (safeSignedInt(roomData.winnerSeat, -1) >= 0) {
      liveState.winnerSeat = safeSignedInt(roomData.winnerSeat, -1);
      liveState.winnerUid = String(roomData.winnerUid || "").trim();
      liveState.endedReason = String(roomData.endedReason || liveState.endedReason || "").trim();
    }
  }

  return liveState;
}

function getReplayState() {
  if (!Array.isArray(duelDeckOrder) || duelDeckOrder.length !== 28) return null;
  return buildReplayState(duelDeckOrder, currentActions, currentRoomData);
}

function mergeActionRecord(record) {
  if (!record || typeof record !== "object") return false;
  const seq = safeSignedInt(record.seq, -1);
  if (seq < 0) return false;
  const existingIndex = currentActions.findIndex((item) => safeSignedInt(item?.seq, -1) === seq);
  if (existingIndex >= 0) {
    currentActions[existingIndex] = { ...currentActions[existingIndex], ...record };
    return true;
  }
  currentActions = currentActions.concat([{ ...record }]).sort((left, right) => safeSignedInt(left?.seq, -1) - safeSignedInt(right?.seq, -1));
  return true;
}

function getSeatLabel(seat) {
  if (seat === currentSeatIndex) return "Toi";
  const room = currentRoomData || {};
  const playerUids = Array.isArray(room.playerUids) ? room.playerUids : ["", ""];
  const playerNames = Array.isArray(room.playerNames) ? room.playerNames : ["", ""];
  const uid = String(playerUids[seat] || "").trim();
  const name = String(playerNames[seat] || "").trim();
  if (uid) return name || `Joueur ${seat + 1}`;
  return "Bot";
}

function getStableAnonymousSeatId(seat) {
  const seed = `${String(currentRoomId || "duel")}:${safeSignedInt(seat, 0)}:seat`;
  let hash = 0;
  for (let i = 0; i < seed.length; i += 1) {
    hash = ((hash << 5) - hash) + seed.charCodeAt(i);
    hash |= 0;
  }
  return `ID-${Math.abs(hash).toString(36).slice(0, 6).toUpperCase().padEnd(6, "0")}`;
}

function getSeatWinnerLabel(seat) {
  if (seat === currentSeatIndex) return "Toi";
  const room = currentRoomData || {};
  const playerUids = Array.isArray(room.playerUids) ? room.playerUids : ["", ""];
  const playerNames = Array.isArray(room.playerNames) ? room.playerNames : ["", ""];
  const uid = String(playerUids[seat] || "").trim();
  const name = String(playerNames[seat] || "").trim();
  if (uid) return name || `Joueur ${seat + 1}`;
  return `Joueur ${getStableAnonymousSeatId(seat)}`;
}

function getOpponentSeat() {
  return currentSeatIndex === 0 ? 1 : 0;
}

function getLatestWalletState() {
  return getXchangeState(window.__userBaseBalance || window.__userBalance || 0, auth.currentUser?.uid || "guest");
}

function canCurrentPlayerDrawFromLot() {
  if (!currentRoomData || String(currentRoomData.status || "") !== "playing") return false;
  if (safeSignedInt(currentRoomData.currentPlayer, -1) !== currentSeatIndex) return false;
  const state = getReplayState();
  if (!state) return false;
  if (!Array.isArray(state.stockPile) || state.stockPile.length <= 0) return false;
  const legalMoves = getLegalMovesForSeat(state, currentSeatIndex);
  return legalMoves.length === 0;
}

function getLotStockCount() {
  const state = getReplayState();
  return Array.isArray(state?.stockPile) ? state.stockPile.length : 0;
}

function canOpenLotModal() {
  if (!currentRoomData || String(currentRoomData.status || "") !== "playing") return false;
  const state = getReplayState();
  return !!state && Array.isArray(state.stockPile);
}

function setLotModalOpen(open) {
  lotModalOpen = open === true;
  syncLotUi();
}

function clearLotSceneTiles() {
  if (!duelLotScene || !Array.isArray(duelLotTileEntries) || duelLotTileEntries.length === 0) return;
  duelLotTileEntries.forEach((entry) => {
    if (entry?.root && duelLotScene) duelLotScene.remove(entry.root);
  });
  duelLotTileEntries = [];
}

function ensureLotScene() {
  const viewport = document.getElementById("DuelLotViewport");
  if (!viewport || typeof window.THREE === "undefined" || typeof window.Domino_Ficha !== "function") return false;
  duelLotViewport = viewport;

  if (!duelLotRenderer) {
    duelLotRenderer = new window.THREE.WebGLRenderer({ alpha: true, antialias: true, powerPreference: "high-performance" });
    duelLotRenderer.setClearColor(0x000000, 0);
    duelLotRenderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
    viewport.appendChild(duelLotRenderer.domElement);
  }

  if (!duelLotScene) {
    duelLotScene = new window.THREE.Scene();
  }

  if (!duelLotCamera) {
    duelLotCamera = new window.THREE.OrthographicCamera(-10, 10, 6, -6, 0.1, 40);
    duelLotCamera.position.set(0, 14, 0.001);
    duelLotCamera.up.set(0, 0, -1);
    duelLotCamera.lookAt(0, 0, 0);
  }

  if (!duelLotLightsReady) {
    const ambient = new window.THREE.AmbientLight(0xffffff, 1.25);
    duelLotScene.add(ambient);
    duelLotLightsReady = true;
  }

  if (!duelLotRaycaster) duelLotRaycaster = new window.THREE.Raycaster();
  if (!duelLotPointer) duelLotPointer = new window.THREE.Vector2();
  return true;
}

function layoutLotCamera(tileCount) {
  if (!duelLotCamera || !duelLotViewport) return;
  const rect = duelLotViewport.getBoundingClientRect();
  const width = Math.max(1, rect.width || 1);
  const height = Math.max(1, rect.height || 1);
  const aspect = width / height;
  const columns = Math.min(DUEL_LOT_COLUMNS, Math.max(1, tileCount));
  const rows = tileCount > DUEL_LOT_COLUMNS ? 2 : 1;
  const contentWidth = Math.max(8.6, columns * DUEL_LOT_TILE_WIDTH + Math.max(0, columns - 1) * DUEL_LOT_GAP_X);
  const contentHeight = Math.max(5.1, rows * DUEL_LOT_TILE_HEIGHT + Math.max(0, rows - 1) * DUEL_LOT_GAP_Z);
  const halfHeight = Math.max(contentHeight / 2, (contentWidth / 2) / Math.max(0.65, aspect)) + 0.55;
  const halfWidth = halfHeight * aspect;
  duelLotCamera.left = -halfWidth;
  duelLotCamera.right = halfWidth;
  duelLotCamera.top = halfHeight;
  duelLotCamera.bottom = -halfHeight;
  duelLotCamera.updateProjectionMatrix();
  duelLotRenderer.setSize(width, height, false);
}

function renderLotScene() {
  if (!duelLotRenderer || !duelLotScene || !duelLotCamera) return;
  duelLotRenderer.render(duelLotScene, duelLotCamera);
}

function getLotTileSelectionFromPointer(event) {
  if (!duelLotViewport || !duelLotCamera || !duelLotRaycaster) return null;
  const rect = duelLotViewport.getBoundingClientRect();
  if (!rect.width || !rect.height) return null;
  duelLotPointer.x = ((event.clientX - rect.left) / rect.width) * 2 - 1;
  duelLotPointer.y = -(((event.clientY - rect.top) / rect.height) * 2 - 1);
  duelLotRaycaster.setFromCamera(duelLotPointer, duelLotCamera);
  const roots = duelLotTileEntries.map((entry) => entry.root).filter(Boolean);
  const hits = duelLotRaycaster.intersectObjects(roots, true);
  const hit = hits.find((item) => {
    const tileId = safeSignedInt(item?.object?.userData?.lotTileId, -1);
    const slotIndex = safeSignedInt(item?.object?.userData?.lotSlotIndex, -1);
    return tileId >= 0 && slotIndex >= 0;
  });
  if (!hit) return null;
  return {
    tileId: safeSignedInt(hit.object.userData.lotTileId, -1),
    slotIndex: safeSignedInt(hit.object.userData.lotSlotIndex, -1),
  };
}

function teardownLotScene() {
  clearLotSceneTiles();
  if (duelLotRenderer?.domElement?.parentNode) {
    duelLotRenderer.domElement.parentNode.removeChild(duelLotRenderer.domElement);
  }
  duelLotScene = null;
  duelLotCamera = null;
  duelLotRenderer = null;
  duelLotViewport = null;
  duelLotLightsReady = false;
  duelLotRaycaster = null;
  duelLotPointer = null;
}

function syncLotScene() {
  const emptyEl = document.getElementById("DuelLotEmpty");
  if (!ensureLotScene()) {
    if (emptyEl) {
      emptyEl.style.display = "flex";
      emptyEl.textContent = "Le lot est indisponible pour le moment.";
    }
    return;
  }

  const state = getReplayState();
  const stockPile = Array.isArray(state?.stockPile) ? state.stockPile : [];
  clearLotSceneTiles();

  if (emptyEl) {
    emptyEl.style.display = stockPile.length > 0 ? "none" : "flex";
    emptyEl.textContent = stockPile.length > 0 ? "" : "Le lot est vide.";
  }

  if (stockPile.length <= 0) {
    layoutLotCamera(1);
    renderLotScene();
    return;
  }

  stockPile.forEach((tileId, index) => {
    const tile = new window.Domino_Ficha();
    tile.Crear(tileId);
    tile.Ficha.rotation.set(Math.PI / 2, 0, 0);
    if (tile.Base?.material?.clone) {
      const matteBack = tile.Base.material.clone();
      if (typeof matteBack.color?.setHex === "function") matteBack.color.setHex(0x050505);
      if (typeof matteBack.specular?.setHex === "function") matteBack.specular.setHex(0x000000);
      if (typeof matteBack.emissive?.setHex === "function") matteBack.emissive.setHex(0x000000);
      matteBack.shininess = 0;
      tile.Base.material = matteBack;
    }
    if (tile.Cara1) tile.Cara1.visible = false;
    if (tile.Cara2) tile.Cara2.visible = false;
    if (tile.Textura1) tile.Textura1.visible = false;
    if (tile.Textura2) tile.Textura2.visible = false;
    if (tile.Bola) tile.Bola.visible = false;

    const tileWrap = new window.THREE.Group();
    tileWrap.add(tile.Ficha);
    tileWrap.rotation.y = Math.PI / 2;

    const col = index % DUEL_LOT_COLUMNS;
    const row = Math.floor(index / DUEL_LOT_COLUMNS);
    const columnsInRow = Math.min(DUEL_LOT_COLUMNS, stockPile.length - row * DUEL_LOT_COLUMNS);
    const x = (col - ((columnsInRow - 1) / 2)) * (DUEL_LOT_TILE_WIDTH + DUEL_LOT_GAP_X);
    const z = (row - 0.5) * (DUEL_LOT_TILE_HEIGHT + DUEL_LOT_GAP_Z);
    tileWrap.position.set(x, 0, z);
    tileWrap.scale.set(DUEL_LOT_TILE_SCALE, DUEL_LOT_TILE_SCALE, DUEL_LOT_TILE_SCALE);
    tileWrap.traverse((node) => {
      node.userData = {
        ...(node.userData || {}),
        lotTileId: tileId,
        lotSlotIndex: index,
      };
    });
    duelLotScene.add(tileWrap);
    duelLotTileEntries.push({ tileId, slotIndex: index, root: tileWrap });
  });

  layoutLotCamera(stockPile.length);
  renderLotScene();
}

function syncLotUi() {
  const btn = document.getElementById("LotModalOpenBtn");
  const callout = document.getElementById("LotModalCallout");
  const countEl = document.getElementById("LotModalCount");
  const overlay = document.getElementById("DuelLotModal");
  const hint = document.getElementById("DuelLotHint");
  const drawAllowed = canCurrentPlayerDrawFromLot();
  const stockCount = getLotStockCount();
  const canOpen = canOpenLotModal();
  const shouldGuideToLot = drawAllowed && canOpen && !lotModalOpen;

  if (btn) {
    btn.disabled = !canOpen;
    btn.classList.toggle("opacity-50", btn.disabled);
    btn.classList.toggle("pointer-events-none", btn.disabled);
    btn.classList.toggle("duel-lot-cta", shouldGuideToLot);
  }
  if (callout) {
    callout.classList.toggle("hidden", !shouldGuideToLot);
    callout.classList.toggle("duel-lot-cta-visible", shouldGuideToLot);
  }
  if (countEl) {
    countEl.textContent = String(stockCount);
  }
  if (hint) {
    hint.textContent = stockCount <= 0
      ? "Le lot est vide."
      : drawAllowed
        ? "Choisis un domino cache du lot pour le piocher."
        : "Tu peux consulter le lot quand tu veux, mais tu ne peux piocher que quand c'est ton tour et qu'aucun coup n'est possible.";
  }
  if (overlay) {
    const shouldShow = lotModalOpen && canOpen;
    overlay.classList.toggle("hidden", !shouldShow);
    overlay.classList.toggle("flex", shouldShow);
    if (shouldShow) {
      window.requestAnimationFrame(() => syncLotScene());
    }
  }
}

function getWaitingCountdownMs() {
  const waitingDeadlineMs = safeSignedInt(currentRoomData?.waitingDeadlineMs);
  if (waitingDeadlineMs <= 0) return 0;
  return Math.max(0, waitingDeadlineMs - Date.now());
}

function isCurrentUserWinner() {
  if (!currentRoomData) return false;
  const winnerUid = String(currentRoomData.winnerUid || "").trim();
  if (winnerUid) return winnerUid === String(currentUser?.uid || "").trim();
  return safeSignedInt(currentRoomData.winnerSeat, -1) >= 0 && safeSignedInt(currentRoomData.winnerSeat, -1) === currentSeatIndex;
}

function getLastActions(limitCount = 6) {
  return currentActions.slice(-Math.max(0, limitCount));
}

function describeAction(action) {
  const actor = getSeatLabel(safeSignedInt(action?.player, -1));
  const type = String(action?.type || "").trim();
  if (type === "play") {
    const side = String(action?.branch || action?.side || "").trim().toLowerCase();
    const placement = side === "izquierda" || side === "left"
      ? "a gauche"
      : side === "derecha" || side === "right"
        ? "a droite"
        : "au centre";
    return `${actor} joue ${getTileLabel(action?.tileId)} ${placement}.`;
  }
  if (type === "draw") {
    const drawCount = Array.isArray(action?.drawnTileIds) ? action.drawnTileIds.length : 0;
    return `${actor} pioche ${drawCount} domino${drawCount > 1 ? "s" : ""}.`;
  }
  if (type === "pass") {
    return `${actor} passe son tour.`;
  }
  return `${actor} agit.`;
}

function setSelectedStake(nextStakeDoes) {
  selectedStakeDoes = safeInt(nextStakeDoes, selectedStakeDoes);
  updateStakeInUrl(selectedStakeDoes);
  renderApp();
}

function clearWatchers() {
  if (roomUnsub) {
    roomUnsub();
    roomUnsub = null;
  }
  if (actionsUnsub) {
    actionsUnsub();
    actionsUnsub = null;
  }
  if (presenceTimer) {
    window.clearInterval(presenceTimer);
    presenceTimer = null;
  }
}

function resetRoomState() {
  clearWatchers();
  teardownLotScene();
  clearOpeningRuleNotice();
  openingRuleNoticeShownKey = "";
  currentRoomId = "";
  currentSeatIndex = -1;
  currentRoomData = null;
  currentActions = [];
  duelDeckOrder = [];
  actionsReady = false;
  startRevealAcked = false;
  startRevealAcking = false;
  pendingSideChooser = null;
  lotModalOpen = false;
  lotActionSending = false;
  duelJoinAutoStarted = false;
  gameLaunched = false;
  window.GameSession = null;
  setLeaveRoomButtonVisible(false);
  hideEndedOverlay();
}

async function refreshWalletState() {
  if (!currentUser?.uid) return;
  try {
    await ensureXchangeState(currentUser.uid);
  } catch (error) {
    console.warn("[DUEL] wallet refresh failed", error);
  }
  refreshDoesHud();
  renderApp();
}

async function loadStakeOptions() {
  try {
    const result = await getPublicDuelStakeOptionsSecure({});
    availableStakeOptions = normalizeStakeOptions(result?.options || []);
  } catch (error) {
    console.warn("[DUEL] using fallback stake options", error);
    availableStakeOptions = normalizeStakeOptions(DEFAULT_DUEL_STAKE_OPTIONS);
  }
  selectedStakeDoes = readStakeFromUrl();
}

async function maybeEnsureRoomReady(reason = "") {
  if (!currentUser?.uid || !currentRoomId || !currentRoomData || ensuringRoom) return;
  const status = String(currentRoomData.status || "").trim();
  const waitingCountdownMs = getWaitingCountdownMs();
  const shouldStartWaitingRoom =
    status === "waiting" &&
    (waitingCountdownMs <= 0 || safeInt(currentRoomData.humanCount) >= 2);
  const shouldHydrateDeck =
    status === "playing" &&
    (!Array.isArray(duelDeckOrder) || duelDeckOrder.length !== 28);

  if (!shouldStartWaitingRoom && !shouldHydrateDeck) return;

  ensuringRoom = true;
  duelDebug("ensureRoomReady:start", { reason, roomId: currentRoomId, status });
  try {
    const result = await ensureRoomReadyDuelSecure({ roomId: currentRoomId });
    if (Array.isArray(result?.privateDeckOrder) && result.privateDeckOrder.length === 28) {
      duelDeckOrder = normalizeDeckOrder(result.privateDeckOrder);
    }
    if (result?.status === "waiting") {
      setStatus("Le duel attend encore un joueur ou un bot.");
    }
  } catch (error) {
    console.error("[DUEL] ensureRoomReady failed", error);
  } finally {
    ensuringRoom = false;
    renderApp();
  }
}

async function maybeReleaseStartReveal(reason = "") {
  if (
    !currentUser?.uid ||
    !currentRoomId ||
    !currentRoomData ||
    currentRoomData.status !== "playing" ||
    currentRoomData.startRevealPending !== true ||
    startRevealAcked === true ||
    startRevealAcking === true ||
    !actionsReady ||
    !Array.isArray(duelDeckOrder) ||
    duelDeckOrder.length !== 28
  ) {
    return;
  }

  startRevealAcking = true;
  duelDebug("ack:startReveal", { reason, roomId: currentRoomId });
  try {
    const result = await ackRoomStartSeenDuelSecure({ roomId: currentRoomId });
    startRevealAcked = true;
    if (result?.released === true) {
      setStatus("Le duel commence.");
    }
  } catch (error) {
    console.error("[DUEL] ackRoomStartSeenDuel failed", error);
  } finally {
    startRevealAcking = false;
    renderApp();
  }
}

function startPresenceHeartbeat() {
  if (presenceTimer) {
    window.clearInterval(presenceTimer);
    presenceTimer = null;
  }
  if (!currentRoomId) return;
  const ping = async () => {
    try {
      await touchRoomPresenceDuelSecure({ roomId: currentRoomId });
    } catch (error) {
      console.warn("[DUEL] touchRoomPresenceDuel failed", error);
    }
  };
  void ping();
  presenceTimer = window.setInterval(() => {
    void ping();
  }, PRESENCE_PING_MS);
}

function watchActionsLegacy(roomId) {
  if (actionsUnsub) actionsUnsub();
  actionsReady = false;
  const actionsQuery = query(collection(db, DUEL_ROOMS, roomId, "actions"), orderBy("seq", "asc"));
  actionsUnsub = onSnapshot(
    actionsQuery,
    (snapshot) => {
      currentActions = snapshot.docs.map((docSnap) => ({ ...docSnap.data() }));
      actionsReady = true;
      void maybeReleaseStartReveal("actions-ready");
      renderApp();
    },
    (error) => {
      console.error("[DUEL] watchActions failed", error);
      setStatus("Impossible de lire les actions du duel.");
    }
  );
}

function watchRoomLegacy(roomId) {
  if (roomUnsub) roomUnsub();
  startPresenceHeartbeat();
  watchActions(roomId);

  roomUnsub = onSnapshot(
    doc(db, DUEL_ROOMS, roomId),
    (snapshot) => {
      if (!snapshot.exists()) {
        setStatus("Cette salle duel a disparu.");
        resetRoomState();
        renderApp();
        return;
      }

      currentRoomData = snapshot.data() || {};
      currentRoomId = roomId;
      setMatchLoading(false);
      if (currentUser?.uid) {
        const nextSeat = currentRoomData?.seats?.[currentUser.uid];
        if (typeof nextSeat === "number") {
          currentSeatIndex = nextSeat;
        }
      }
      if (currentRoomData.status === "waiting") {
        void maybeEnsureRoomReady("waiting-snapshot");
      } else if (currentRoomData.status === "playing") {
        void maybeEnsureRoomReady("playing-snapshot");
        void maybeReleaseStartReveal("room-playing");
      } else if (currentRoomData.status === "ended") {
        setStatus(isCurrentUserWinner() ? "Le duel est termine. Tu peux reclamer ton gain." : "Le duel est termine.");
      }
      renderApp();
    },
    (error) => {
      console.error("[DUEL] watchRoom failed", error);
      setMatchLoading(false);
      setStatus("Impossible de suivre cette salle duel.");
    }
  );
}

async function joinSelectedDuel() {
  if (!currentUser?.uid || joining) return;
  joining = true;
  pendingSideChooser = null;
  setMatchLoading(true, "Connexion des joueurs en cours.");
  renderApp();
  try {
    await refreshWalletState();
    const stake = getSelectedStakeOption();
    const result = await joinMatchmakingDuelSecure({
      stakeDoes: stake.stakeDoes,
      excludeRoomIds: readAbandonedDuelRoomIds(),
    });
    if (!result?.ok || !result?.roomId) {
      throw new Error("Impossible de rejoindre le duel.");
    }

    currentRoomId = String(result.roomId || "");
    forgetAbandonedDuelRoom(currentRoomId);
    currentSeatIndex = safeInt(result.seatIndex, 0);
    if (Array.isArray(result.privateDeckOrder) && result.privateDeckOrder.length === 28) {
      duelDeckOrder = normalizeDeckOrder(result.privateDeckOrder);
    }
    if (result.resumed === true) {
      setStatus(`Duel repris. Position ${currentSeatIndex + 1}/2.`);
    } else if (result.status === "waiting") {
      setStatus(`Salle duel rejointe. Position ${currentSeatIndex + 1}/2.`);
    } else {
      setStatus(`Duel pret. Position ${currentSeatIndex + 1}/2.`);
    }
    await refreshWalletState();
    watchRoom(currentRoomId);
  } catch (error) {
    console.error("[DUEL] joinSelectedDuel failed", error);
    setMatchLoading(false);
    setStatus(error?.message || "Impossible de lancer le duel.");
  } finally {
    joining = false;
    renderApp();
  }
}

async function resumeFriendDuelFromUrl() {
  const friendRoomId = getFriendDuelRoomIdFromUrl();
  if (!currentUser?.uid || joining || currentRoomId || !friendRoomId) return;
  joining = true;
  pendingSideChooser = null;
  setMatchLoading(true, "Connexion du duel prive en cours.");
  renderApp();
  try {
    await refreshWalletState();
    currentRoomId = friendRoomId;
    forgetAbandonedDuelRoom(currentRoomId);
    currentSeatIndex = safeInt(URL_PARAMS.get("seat"), 0);
    setStatus(`Salle duel privee. Position ${currentSeatIndex + 1}/2.`);
    watchRoom(currentRoomId);
  } catch (error) {
    console.error("[DUEL] resumeFriendDuelFromUrl failed", error);
    setMatchLoading(false);
    setStatus(error?.message || "Impossible de rejoindre ce duel prive.");
  } finally {
    joining = false;
    renderApp();
  }
}

function maybeAutoJoinSelectedDuel() {
  if (!currentUser?.uid || currentRoomId || joining || duelJoinAutoStarted || isFriendDuelFlowFromUrl()) return;
  duelJoinAutoStarted = true;
  setMatchLoading(true, "Connexion des joueurs en cours.");
  void joinSelectedDuel();
}

async function leaveCurrentRoom() {
  if (!currentRoomId) return;
  const roomId = currentRoomId;
  rememberAbandonedDuelRoom(roomId);
  setStatus("Fermeture de la salle duel...");
  renderApp();
  try {
    await leaveRoomDuelSecure({ roomId });
  } catch (error) {
    console.error("[DUEL] leaveRoomDuel failed", error);
  } finally {
    resetRoomState();
    await refreshWalletState();
    renderApp();
  }
}

async function sendPlayMove(tileId, side) {
  if (!currentRoomId || actionSending) return;
  actionSending = true;
  pendingSideChooser = null;
  renderApp();
  try {
    const result = await submitActionDuelSecure({
      roomId: currentRoomId,
      clientActionId: `duel_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`,
      action: {
        type: "play",
        tileId: safeInt(tileId),
        side: String(side || "center"),
      },
    });
    mergeActionRecord(result?.record);
    setStatus("Action envoyee.");
  } catch (error) {
    console.error("[DUEL] sendPlayMove failed", error);
    setStatus(error?.message || "Impossible de jouer cette tuile.");
  } finally {
    actionSending = false;
    renderApp();
  }
}

async function sendDrawMove(tileId) {
  if (!currentRoomId || actionSending || lotActionSending) return;
  const normalizedTileId = safeSignedInt(tileId, -1);
  if (normalizedTileId < 0) return;
  actionSending = true;
  lotActionSending = true;
  renderApp();
  syncLotUi();
  try {
    const result = await submitActionDuelSecure({
      roomId: currentRoomId,
      clientActionId: `duel_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`,
      action: { type: "draw", tileId: normalizedTileId },
    });
    mergeActionRecord(result?.record);
    setStatus(`Pioche envoyee: ${getTileLabel(normalizedTileId)}.`);
  } catch (error) {
    console.error("[DUEL] sendDrawMove failed", error);
    setStatus(error?.message || "Impossible de piocher.");
  } finally {
    actionSending = false;
    lotActionSending = false;
    renderApp();
    syncLotUi();
  }
}

async function sendPassMove() {
  if (!currentRoomId || actionSending) return;
  actionSending = true;
  renderApp();
  try {
    const result = await submitActionDuelSecure({
      roomId: currentRoomId,
      clientActionId: `duel_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`,
      action: { type: "pass" },
    });
    mergeActionRecord(result?.record);
    setStatus("Tour passe.");
  } catch (error) {
    console.error("[DUEL] sendPassMove failed", error);
    setStatus(error?.message || "Impossible de passer le tour.");
  } finally {
    actionSending = false;
    renderApp();
  }
}

async function claimReward() {
  if (!currentRoomId || rewardClaiming) return;
  rewardClaiming = true;
  renderApp();
  try {
    const result = await claimWinRewardDuelSecure({ roomId: currentRoomId });
    await refreshWalletState();
    if (result?.rewardGranted === true) {
      setStatus(`Victoire validee: +${formatDoes(result.rewardAmountDoes)}.`);
    } else {
      setStatus("Gain duel deja valide.");
    }
  } catch (error) {
    console.error("[DUEL] claimReward failed", error);
    setStatus(error?.message || "Impossible de valider le gain duel.");
  } finally {
    rewardClaiming = false;
    renderApp();
  }
}

function buildStatusBannerHtml() {
  const normalized = String(statusMessage || "").trim().toLowerCase();
  if (!normalized) return "";
  const shouldShow =
    normalized.includes("impossible") ||
    normalized.includes("erreur") ||
    normalized.includes("termine") ||
    normalized.includes("gagne") ||
    normalized.includes("victoire") ||
    normalized.includes("disparu");
  if (!shouldShow) return "";
  return `
    <section class="pointer-events-auto mx-auto w-[min(92vw,26rem)] rounded-[22px] border border-sky-300/18 bg-sky-500/14 px-4 py-3 text-sm leading-6 text-white/88 shadow-[0_16px_30px_rgba(0,0,0,0.28)] backdrop-blur-xl">
      ${escapeHtml(statusMessage)}
    </section>
  `;
}

function buildAuthRequiredHtml() {
  return `
    <section class="pointer-events-auto mt-5 rounded-[32px] border border-white/12 bg-[linear-gradient(180deg,rgba(54,64,95,0.95),rgba(31,40,63,0.95))] px-5 pb-5 pt-6 text-white shadow-[0_22px_50px_rgba(5,12,24,0.4)] backdrop-blur-xl">
      <p class="text-[11px] font-semibold uppercase tracking-[0.24em] text-[#ffd4ab]/80">Connexion requise</p>
      <h1 class="mt-3 text-[2rem] font-black leading-[1.02] tracking-[-0.03em] text-white">Connecte-toi pour lancer un duel</h1>
      <p class="mt-3 text-[15px] leading-7 text-white/82">Le duel 2 joueurs utilise le meme compte, le meme wallet et la meme logique de mise que le site principal.</p>
      <a href="./index.html" class="mt-5 inline-flex w-full items-center justify-center rounded-[24px] bg-[linear-gradient(135deg,#f6a33f,#d86a1d)] px-5 py-4 text-base font-bold text-[#241100] shadow-[0_18px_38px_rgba(0,0,0,0.26)] transition active:scale-[0.99]">
        Aller a l'accueil
      </a>
    </section>
  `;
}

function renderApp() {
  if (!duelRoot) return;
  const showAuthRequired = authReady === true && !currentUser;
  const mainContent = showAuthRequired ? buildAuthRequiredHtml() : "";
  const showShell = showAuthRequired;

  duelRoot.innerHTML = `
    ${buildOpeningRuleNoticeHtml()}
    <div class="pointer-events-none min-h-full ${showShell ? "" : "hidden"}">
      <div class="mx-auto flex min-h-screen max-w-xl flex-col">
        <div class="pointer-events-auto px-[max(14px,env(safe-area-inset-left))] pt-[max(16px,env(safe-area-inset-top))]">
          <div class="flex items-center justify-between gap-3">
            <a href="${SUPPORT_RETURN_URL}" class="inline-flex h-11 items-center gap-2 rounded-full border border-white/12 bg-white/10 px-4 text-sm font-semibold text-white/92 backdrop-blur-md">
              <span aria-hidden="true">←</span>
              <span>Accueil</span>
            </a>
          </div>
        </div>
        <div class="px-[max(14px,env(safe-area-inset-left))] pt-[max(14px,env(safe-area-inset-top))]">
          ${buildStatusBannerHtml()}
        </div>
        ${mainContent}
      </div>
    </div>
  `;
  syncLotUi();
}

function mountRoot() {
  const existing = document.getElementById("DuelEntryRoot");
  if (existing) existing.remove();
  duelRoot = document.createElement("div");
  duelRoot.id = "DuelEntryRoot";
  duelRoot.className = "pointer-events-none fixed inset-0 z-[2600]";
  document.body.appendChild(duelRoot);

  duelRoot.addEventListener("click", (event) => {
    const button = event.target.closest("[data-action]");
    if (!button || !duelRoot.contains(button)) return;
    const action = String(button.getAttribute("data-action") || "").trim();

    if (action === "leave-room") {
      void leaveCurrentRoom();
      return;
    }

    if (action === "claim-reward") {
      void claimReward();
    }
  });
}

function bindLotModalControls() {
  const openBtn = document.getElementById("LotModalOpenBtn");
  if (openBtn && openBtn.dataset.bound !== "1") {
    openBtn.dataset.bound = "1";
    openBtn.addEventListener("click", () => {
      if (!canOpenLotModal()) return;
      setLotModalOpen(true);
    });
  }

  const closeBtn = document.getElementById("DuelLotModalCloseBtn");
  if (closeBtn && closeBtn.dataset.bound !== "1") {
    closeBtn.dataset.bound = "1";
    closeBtn.addEventListener("click", () => {
      setLotModalOpen(false);
    });
  }

  const overlay = document.getElementById("DuelLotModal");
  if (overlay && overlay.dataset.bound !== "1") {
    overlay.dataset.bound = "1";
    overlay.addEventListener("click", (event) => {
      if (event.target === overlay) setLotModalOpen(false);
    });
  }

  const viewport = document.getElementById("DuelLotViewport");
  if (viewport && viewport.dataset.bound !== "1") {
    viewport.dataset.bound = "1";
    viewport.addEventListener("click", (event) => {
      const selection = getLotTileSelectionFromPointer(event);
      if (!selection) return;
      if (!canCurrentPlayerDrawFromLot() || actionSending || lotActionSending) return;
      if (window.Domino?.Partida && typeof window.Domino.Partida.DefinirPosePiocheDepuisModal === "function") {
        window.Domino.Partida.DefinirPosePiocheDepuisModal(selection.slotIndex);
      }
      setLotModalOpen(false);
      void sendDrawMove(selection.tileId);
    });
  }

  if (window.__duelLotResizeBound !== true) {
    window.__duelLotResizeBound = true;
    window.addEventListener("resize", () => {
      if (!lotModalOpen) return;
      window.requestAnimationFrame(() => syncLotScene());
    });
  }
}

function setLeaveRoomButtonVisible(visible) {
  const btn = document.getElementById("LeaveRoomTopBtn");
  if (!btn) return;
  if (visible) {
    btn.classList.remove("hidden");
    btn.classList.add("inline-flex");
  } else {
    btn.classList.add("hidden");
    btn.classList.remove("inline-flex");
  }
}

function bindLeaveRoomTopButton() {
  const btn = document.getElementById("LeaveRoomTopBtn");
  if (!btn || btn.dataset.bound === "1") return;
  btn.dataset.bound = "1";
  btn.addEventListener("click", async () => {
    btn.disabled = true;
    try {
      await leaveCurrentRoom();
      window.location.href = SUPPORT_RETURN_URL;
    } finally {
      btn.disabled = false;
    }
  });
}

function bindEndOverlayButtons() {
  const replayBtn = document.getElementById("GameEndReplayBtn");
  const backBtn = document.getElementById("GameEndBackBtn");
  const goBtn = document.getElementById("GameEndGoBtn");
  if (replayBtn && replayBtn.dataset.bound !== "1") {
    replayBtn.dataset.bound = "1";
    replayBtn.addEventListener("click", () => {
      window.location.href = `./jeu-duel.html?stake=${safeInt(selectedStakeDoes, 100)}`;
    });
  }
  if (backBtn && backBtn.dataset.bound !== "1") {
    backBtn.dataset.bound = "1";
    backBtn.addEventListener("click", () => {
      window.location.href = SUPPORT_RETURN_URL;
    });
  }
  if (goBtn && goBtn.dataset.bound !== "1") {
    goBtn.dataset.bound = "1";
    goBtn.addEventListener("click", () => {
      const overlay = document.getElementById("GameEndOverlay");
      if (!overlay) return;
      overlay.classList.add("hidden");
      overlay.classList.remove("flex");
      goBtn.classList.add("hidden");
      goBtn.classList.remove("block");
    });
  }
}

function hideEndedOverlay() {
  const overlay = document.getElementById("GameEndOverlay");
  const goBtn = document.getElementById("GameEndGoBtn");
  if (overlay) {
    overlay.classList.add("hidden");
    overlay.classList.remove("flex");
  }
  if (goBtn) {
    goBtn.classList.add("hidden");
    goBtn.classList.remove("block");
  }
}

function showEndedOverlay() {
  const overlay = document.getElementById("GameEndOverlay");
  const winnerEl = document.getElementById("GameEndWinnerText");
  const infoEl = document.getElementById("GameEndInfoText");
  const trophy = document.getElementById("GameEndTrophy");
  const actionsWrap = document.getElementById("GameEndActionsWrap");
  const viewWrap = document.getElementById("GameEndViewWrap");
  const goBtn = document.getElementById("GameEndGoBtn");
  if (!overlay || !currentRoomData) return;
  const winnerSeat = safeSignedInt(currentRoomData.winnerSeat, -1);
  const winnerName = winnerSeat >= 0 ? getSeatWinnerLabel(winnerSeat) : "La partie";
  if (winnerEl) winnerEl.textContent = `${winnerName} a gagne`;
  if (infoEl) {
    infoEl.textContent = isCurrentUserWinner()
      ? "Ton gain a ete verifie. Tu peux rejouer ou revenir a l'accueil."
      : "Observe la table puis continue quand tu veux.";
  }
  if (trophy) trophy.classList.toggle("hidden", !isCurrentUserWinner());
  if (actionsWrap) {
    actionsWrap.classList.remove("hidden");
    actionsWrap.classList.add("grid");
  }
  if (viewWrap) {
    viewWrap.classList.add("hidden");
    viewWrap.classList.remove("block");
  }
  if (goBtn) {
    goBtn.classList.add("hidden");
    goBtn.classList.remove("block");
  }
  overlay.classList.remove("hidden");
  overlay.classList.add("flex");
}

function isDominoEngineReady() {
  const partida = window.Domino && window.Domino.Partida ? window.Domino.Partida : null;
  return !!(
    window.Domino &&
    window.Domino.Escena &&
    partida &&
    typeof partida.Empezar === "function" &&
    window.UI &&
    typeof window.UI.MostrarEmpezar === "function"
  );
}

function tsToMs(value) {
  if (!value) return 0;
  if (typeof value === "number" && Number.isFinite(value)) return Math.trunc(value);
  if (typeof value.toMillis === "function") return value.toMillis();
  if (typeof value.seconds === "number") return Math.trunc(value.seconds * 1000);
  return 0;
}

function parseHumanSeats(seats = {}) {
  const out = [];
  Object.keys(seats || {}).forEach((uid) => {
    const seat = safeSignedInt(seats[uid], -1);
    if (seat >= 0 && seat < 2) out.push(seat);
  });
  return out.sort((a, b) => a - b);
}

function syncGameSessionFromRoom(roomData) {
  if (!roomData) return;
  const seats = roomData.seats && typeof roomData.seats === "object" ? roomData.seats : {};
  const humanSeats = parseHumanSeats(seats);
  const hostSeat = seats[roomData.ownerUid] !== undefined ? safeInt(seats[roomData.ownerUid], 0) : 0;
  const selectedStake = getSelectedStakeOption();
  const startedAtMs = tsToMs(roomData.startedAt) || safeSignedInt(roomData.startedAtMs, 0);

  window.GameSession = {
    mode: "duel_2p",
    roomId: currentRoomId,
    seatIndex: currentSeatIndex,
    hostSeat,
    isHost: currentSeatIndex === hostSeat,
    playerUids: Array.isArray(roomData.playerUids) ? roomData.playerUids.slice(0, 2) : ["", ""],
    playerNames: Array.isArray(roomData.playerNames) ? roomData.playerNames.slice(0, 2) : ["", ""],
    humanSeats,
    humans: safeInt(roomData.humanCount, humanSeats.length || 1),
    bots: safeInt(roomData.botCount, 0),
    status: String(roomData.status || ""),
    startRevealPending: roomData.startRevealPending === true,
    currentPlayer: safeInt(roomData.currentPlayer, 0),
    openingSeat: safeSignedInt(roomData.openingSeat, -1),
    openingTileId: safeSignedInt(roomData.openingTileId, -1),
    openingReason: String(roomData.openingReason || "").trim(),
    turnActual: safeInt(roomData.turnActual, 0),
    lastActionSeq: safeSignedInt(roomData.lastActionSeq, -1),
    entryCostDoes: safeInt(roomData.stakeDoes, selectedStake.stakeDoes),
    rewardAmountDoes: safeInt(roomData.rewardAmountDoes, selectedStake.rewardDoes),
    startedAtMs,
    deckOrder: Array.isArray(duelDeckOrder) ? duelDeckOrder.slice(0, 28) : [],
  };

  if (window.Domino && window.Domino.Partida && typeof window.Domino.Partida.PrepararSesion === "function") {
    window.Domino.Partida.PrepararSesion();
  }
}

async function pushAction(action) {
  if (!currentRoomId) throw new Error("Aucune salle duel active.");
  await submitActionDuelSecure({
    roomId: currentRoomId,
    clientActionId: `duel_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`,
    action,
  });
}

function onGameEnded() {
  setStatus("Fin du duel...");
}

function maybeFinishInitialHydration(snapshot) {
  const partida = window.Domino && window.Domino.Partida ? window.Domino.Partida : null;
  if (!partida || typeof partida.AplicarAccionMultijugador !== "function") return;
  if (typeof partida.HayAnimacionInicioActiva === "function" && partida.HayAnimacionInicioActiva() === true) {
    window.setTimeout(() => maybeFinishInitialHydration(snapshot), 120);
    return;
  }
  snapshot.docs.forEach((docSnap) => {
    const action = docSnap.data();
    if (typeof action?.seq !== "number") return;
    partida.AplicarAccionMultijugador(action);
  });
  if (typeof partida.FinalizarRehidratacion === "function") {
    partida.FinalizarRehidratacion();
  }
  actionsReady = true;
  if (currentRoomData?.startRevealPending === true) {
    void maybeReleaseStartReveal("initial-hydration");
  } else {
    setMatchLoading(false);
  }
}

function watchActions(roomId) {
  if (actionsUnsub) actionsUnsub();
  actionsReady = false;
  const actionsQuery = query(collection(db, DUEL_ROOMS, roomId, "actions"), orderBy("seq", "asc"));
  let firstSnapshot = true;
  actionsUnsub = onSnapshot(
    actionsQuery,
    (snapshot) => {
      currentActions = snapshot.docs.map((docSnap) => ({ ...docSnap.data() }));
      if (!gameLaunched || !window.Domino || !window.Domino.Partida || typeof window.Domino.Partida.AplicarAccionMultijugador !== "function") {
        actionsReady = true;
        if (currentRoomData && String(currentRoomData.status || "") === "playing") {
          scheduleTurnTimeout(currentRoomData);
        }
        renderApp();
        return;
      }

      if (firstSnapshot) {
        firstSnapshot = false;
        if (typeof window.Domino.Partida.IniciarRehidratacion === "function") {
          window.Domino.Partida.IniciarRehidratacion();
        }
        if (typeof window.Domino.Partida.Empezar === "function") {
          window.Domino.Partida.Empezar();
        }
        maybeFinishInitialHydration(snapshot);
        renderApp();
        return;
      }

      snapshot.docChanges().forEach((change) => {
        if (change.type !== "added") return;
        const action = change.doc.data();
        if (typeof action?.seq !== "number") return;
        window.Domino.Partida.AplicarAccionMultijugador(action);
      });
      actionsReady = true;
      if (currentRoomData?.startRevealPending !== true) {
        setMatchLoading(false);
      }
      if (currentRoomData && String(currentRoomData.status || "") === "playing") {
        scheduleTurnTimeout(currentRoomData);
      }
      renderApp();
    },
    (error) => {
      console.error("[DUEL] watchActions failed", error);
      setStatus("Impossible de lire les actions du duel.");
      renderApp();
    }
  );
}

function launchLocalGame(roomData) {
  syncGameSessionFromRoom(roomData);
  if (!Array.isArray(duelDeckOrder) || duelDeckOrder.length !== 28) {
    setMatchLoading(true, "Preparation du duel...");
    return;
  }
  if (!isDominoEngineReady()) {
    setMatchLoading(true, "Initialisation du duel...");
    window.setTimeout(() => {
      if (currentRoomId && currentRoomData && String(currentRoomData.status || "") === "playing") {
        launchLocalGame(currentRoomData);
      }
    }, 120);
    return;
  }
  if (gameLaunched) {
    if (currentRoomData?.startRevealPending !== true && actionsReady) setMatchLoading(false);
    if (window.UI && typeof window.UI.ActualizarBotonLucesJugadores === "function") {
      window.UI.ActualizarBotonLucesJugadores();
    }
    return;
  }
  gameLaunched = true;
  setLeaveRoomButtonVisible(true);
  refreshDoesHud();
  setMatchLoading(true, "Synchronisation du duel...");
  if (window.UI && typeof window.UI.ActualizarBotonLucesJugadores === "function") {
    window.UI.ActualizarBotonLucesJugadores();
  }
  watchActions(currentRoomId);
}

async function maybeHandleEndedRoom() {
  if (!currentRoomData || String(currentRoomData.status || "") !== "ended") return;
  if (isCurrentUserWinner() && !rewardClaiming) {
    try {
      await claimReward();
    } catch (_) {}
  }
  showEndedOverlay();
}

function watchRoom(roomId) {
  if (roomUnsub) roomUnsub();
  startPresenceHeartbeat();

  roomUnsub = onSnapshot(
    doc(db, DUEL_ROOMS, roomId),
    (snapshot) => {
      if (!snapshot.exists()) {
        setStatus("Cette salle duel a disparu.");
        resetRoomState();
        setMatchLoading(false);
        renderApp();
        return;
      }

      currentRoomData = snapshot.data() || {};
      currentRoomId = roomId;
      if (currentUser?.uid) {
        const nextSeat = currentRoomData?.seats?.[currentUser.uid];
        if (typeof nextSeat === "number") {
          currentSeatIndex = nextSeat;
        }
      }

      if (String(currentRoomData.status || "") === "waiting") {
        hideEndedOverlay();
        setLeaveRoomButtonVisible(true);
        clearTurnTimer();
        setMatchLoading(true, "Connexion des joueurs en cours.");
        void maybeEnsureRoomReady("waiting-snapshot");
      } else if (String(currentRoomData.status || "") === "playing") {
        hideEndedOverlay();
        if (!Array.isArray(duelDeckOrder) || duelDeckOrder.length !== 28) {
          void maybeEnsureRoomReady("playing-snapshot");
        }
        launchLocalGame(currentRoomData);
        maybeShowOpeningRuleNotice();
        if (currentRoomData.startRevealPending === true) {
          clearTurnTimer();
          setMatchLoading(true, "Le duel commence.");
          void maybeReleaseStartReveal("room-playing");
        } else if (actionsReady) {
          setMatchLoading(false);
          scheduleTurnTimeout(currentRoomData);
        }
      } else if (String(currentRoomData.status || "") === "ended") {
        setMatchLoading(false);
        setLeaveRoomButtonVisible(false);
        clearTurnTimer();
        void maybeHandleEndedRoom();
      }
      renderApp();
    },
    (error) => {
      console.error("[DUEL] watchRoom failed", error);
      setMatchLoading(false);
      setStatus("Impossible de suivre cette salle duel.");
      renderApp();
    }
  );
}

async function bootstrap() {
  mountRoot();
  bindLeaveRoomTopButton();
  bindEndOverlayButtons();
  bindHudViewToggle();
  bindFullscreenToggle();
  bindLotModalControls();
  startOnlineUsersTicker();
  await loadStakeOptions();
  refreshDoesHud();
  renderApp();

  onAuthStateChanged(auth, async (user) => {
    authReady = true;
    currentUser = user || null;
    if (!user) {
      resetRoomState();
      setMatchLoading(false);
      setStatus("");
      renderApp();
      return;
    }

    await refreshWalletState();
    renderApp();
    if (isFriendDuelFlowFromUrl()) {
      void resumeFriendDuelFromUrl();
      return;
    }
    maybeAutoJoinSelectedDuel();
  });

  if (bootTimer) {
    window.clearInterval(bootTimer);
  }
  bootTimer = window.setInterval(() => {
    if (currentRoomData?.status === "waiting") {
      void maybeEnsureRoomReady("boot-tick");
      renderApp();
      return;
    }
    if (currentRoomData?.status === "playing" && (!Array.isArray(duelDeckOrder) || duelDeckOrder.length !== 28)) {
      void maybeEnsureRoomReady("boot-tick-playing");
      renderApp();
      return;
    }
    if (currentRoomData?.status === "playing" && currentRoomData?.startRevealPending === true) {
      void maybeReleaseStartReveal("boot-tick");
      renderApp();
    }
  }, 1000);
}

window.LogiqueJeu = {
  pushAction,
  onGameEnded,
  leaveRoom: leaveCurrentRoom,
  hasActiveRoom: () => !!currentRoomId,
  getSession: () => window.GameSession || null,
};

window.addEventListener("xchangeUpdated", refreshDoesHud);
window.addEventListener("userBalanceUpdated", refreshDoesHud);
window.addEventListener("storage", refreshDoesHud);
window.addEventListener("pagehide", () => {
  leaveCurrentRoomOnLifecycleExit("pagehide");
});
window.addEventListener("offline", () => {
  leaveCurrentRoomOnLifecycleExit("offline");
});
window.addEventListener("beforeunload", () => {
  leaveCurrentRoomOnLifecycleExit("beforeunload");
  if (onlineUsersTick) {
    clearInterval(onlineUsersTick);
    onlineUsersTick = null;
  }
  if (fullscreenHintTimer) {
    clearTimeout(fullscreenHintTimer);
    fullscreenHintTimer = null;
  }
});

window.addEventListener("DOMContentLoaded", () => {
  void bootstrap();
});
