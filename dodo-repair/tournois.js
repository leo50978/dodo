import {
  auth,
  functions as firebaseFunctions,
  httpsCallable,
} from "./firebase-init.js";
import {
  getPublicGameStakeOptionsSecure,
} from "./secure-functions.js";
import {
  showGlobalLoading,
  withButtonLoading,
} from "./loading-ui.js";

const POLL_MS = 10 * 1000;
const MATCHING_DELAY_MS = 10 * 1000;
const ACTIVE_TOURNAMENT_STORAGE_KEY = "dlk_active_tournament_session_v2";
const DEFAULT_GAME_STAKE_OPTIONS = Object.freeze([
  { stakeDoes: 100, rewardDoes: 175, enabled: true, sortOrder: 1 },
  { stakeDoes: 250, rewardDoes: 440, enabled: true, sortOrder: 2 },
  { stakeDoes: 500, rewardDoes: 900, enabled: true, sortOrder: 3 },
  { stakeDoes: 1000, rewardDoes: 1800, enabled: true, sortOrder: 4 },
]);

const dom = {
  playNowBtn: document.getElementById("playNowBtn"),
  pageMessage: document.getElementById("pageMessage"),
  loaderPanel: document.getElementById("loaderPanel"),
  loaderMeta: document.getElementById("loaderMeta"),
  loaderQuota: document.getElementById("loaderQuota"),
  signedOutPanel: document.getElementById("signedOutPanel"),
  leaderboardPanel: document.getElementById("leaderboardPanel"),
  leaderboardMeta: document.getElementById("leaderboardMeta"),
  sessionBadge: document.getElementById("sessionBadge"),
  countdownChip: document.getElementById("countdownChip"),
  countdownText: document.getElementById("countdownText"),
  leaderboardState: document.getElementById("leaderboardState"),
  leaderTable: document.getElementById("leaderTable"),
  exitTournamentBtn: document.getElementById("exitTournamentBtn"),
  exitConfirmModal: document.getElementById("exitConfirmModal"),
  exitConfirmStayBtn: document.getElementById("exitConfirmStayBtn"),
  exitConfirmLeaveBtn: document.getElementById("exitConfirmLeaveBtn"),
  winnerModal: document.getElementById("winnerModal"),
  winnerModalName: document.getElementById("winnerModalName"),
  winnerModalReward: document.getElementById("winnerModalReward"),
  winnerReplayBtn: document.getElementById("winnerReplayBtn"),
  winnerBackBtn: document.getElementById("winnerBackBtn"),
  doesRequiredOverlay: document.getElementById("doesRequiredOverlay"),
  doesRequiredOpenProfile: document.getElementById("doesRequiredOpenProfile"),
  doesRequiredClose: document.getElementById("doesRequiredClose"),
  stakeSelectionOverlay: document.getElementById("stakeSelectionOverlay"),
  stakeSelectionClose: document.getElementById("stakeSelectionClose"),
  stakeOptionsGrid: document.getElementById("stakeOptionsGrid"),
};

const ensureSessionsFn = httpsCallable(firebaseFunctions, "ensureUserTournamentSessions");
const selectSessionFn = httpsCallable(firebaseFunctions, "selectUserTournament");
const abandonTournamentFn = httpsCallable(firebaseFunctions, "abandonUserTournament");
const stateFn = httpsCallable(firebaseFunctions, "getUserTournamentState");

let currentUser = null;
let sessions = [];
let currentSessionId = "";
let pollHandle = null;
let loaderHandle = null;
let loaderTickHandle = null;
let loaderStartedAt = 0;
let refreshToken = 0;
let countdownHandle = null;
let currentCountdownTargetMs = 0;
let currentCountdownMode = "";
let shownWinnerModalSessionId = "";
let xchangeModulePromise = null;
let stakeOptionsHydrationPromise = null;

function loadXchangeModule() {
  if (!xchangeModulePromise) {
    xchangeModulePromise = import("./xchange.js");
  }
  return xchangeModulePromise;
}

function normalizeGameStakeOptions(rawOptions) {
  const source = Array.isArray(rawOptions) && rawOptions.length ? rawOptions : DEFAULT_GAME_STAKE_OPTIONS;
  const byStake = new Map();

  source.forEach((item, index) => {
    const stakeDoes = Math.max(0, Math.trunc(Number(item?.stakeDoes || item?.amountDoes || item?.stake || 0)));
    if (!stakeDoes) return;
    const rewardDoes = Math.max(stakeDoes, Math.trunc(Number(item?.rewardDoes || item?.reward || Math.round(stakeDoes * 1.8))));
    const enabled = item?.enabled !== false;
    const sortOrder = Number.isFinite(Number(item?.sortOrder)) ? Number(item.sortOrder) : index + 1;
    byStake.set(stakeDoes, {
      stakeDoes,
      rewardDoes,
      enabled,
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
    console.warn("[TOURNOIS] fallback local options", error);
    return normalizeGameStakeOptions();
  }
}

function openProfilePage() {
  showGlobalLoading("Ouverture du profil...");
  window.location.href = "./profil.html";
}

function openStakeSelection() {
  if (!dom.stakeSelectionOverlay) return;
  dom.stakeSelectionOverlay.hidden = false;
}

function closeStakeSelection() {
  if (!dom.stakeSelectionOverlay) return;
  dom.stakeSelectionOverlay.hidden = true;
}

function openDoesRequired() {
  if (!dom.doesRequiredOverlay) return;
  dom.doesRequiredOverlay.hidden = false;
}

function closeDoesRequired() {
  if (!dom.doesRequiredOverlay) return;
  dom.doesRequiredOverlay.hidden = true;
}

function renderStakeOptions(options = []) {
  if (!dom.stakeOptionsGrid) return;
  const currentStakeOptions = normalizeGameStakeOptions(options);
  dom.stakeOptionsGrid.innerHTML = currentStakeOptions.map((option) => {
    const enabled = option.enabled === true;
    return `
      <button
        data-stake="${option.stakeDoes}"
        data-available="${enabled ? "1" : "0"}"
        type="button"
        class="btn stake-option-btn${enabled ? " btn-primary" : ""}"
        style="width:100%;min-height:58px;${enabled ? "" : "opacity:.55;cursor:not-allowed;"}"
      >
        <span>${option.stakeDoes} Does</span>
      </button>
    `;
  }).join("");
}

function ensureStakeOptionsLoaded() {
  if (stakeOptionsHydrationPromise) return stakeOptionsHydrationPromise;
  stakeOptionsHydrationPromise = loadPublicGameStakeOptions()
    .then((options) => {
      renderStakeOptions(options);
      return options;
    })
    .catch((error) => {
      console.warn("[TOURNOIS] stake render fallback", error);
      const fallback = normalizeGameStakeOptions();
      renderStakeOptions(fallback);
      return fallback;
    });
  return stakeOptionsHydrationPromise;
}

async function handlePlayNow() {
  if (!currentUser) {
    showGlobalLoading("Redirection vers la connexion...");
    window.location.href = "./auth.html";
    return;
  }
  await ensureStakeOptionsLoaded();
  openStakeSelection();
}

function hashId(uid = "") {
  let hash = 0;
  const safeUid = String(uid || "");
  for (let i = 0; i < safeUid.length; i += 1) {
    hash = ((hash << 5) - hash) + safeUid.charCodeAt(i);
    hash |= 0;
  }
  return `ID-${Math.abs(hash).toString(36).slice(0, 6).toUpperCase().padEnd(6, "0")}`;
}

function activeTournamentStorageKey(uid = "") {
  return `${ACTIVE_TOURNAMENT_STORAGE_KEY}:${String(uid || "").trim()}`;
}

function getStoredTournamentSession(uid = "") {
  const safeUid = String(uid || "").trim();
  if (!safeUid) return "";
  try {
    return String(localStorage.getItem(activeTournamentStorageKey(safeUid)) || "").trim();
  } catch (_) {
    return "";
  }
}

function setStoredTournamentSession(uid = "", sessionId = "") {
  const safeUid = String(uid || "").trim();
  if (!safeUid) return;
  try {
    const key = activeTournamentStorageKey(safeUid);
    const value = String(sessionId || "").trim();
    if (value) localStorage.setItem(key, value);
    else localStorage.removeItem(key);
  } catch (_) {
  }
}

function stopPolling() {
  if (pollHandle) {
    window.clearInterval(pollHandle);
    pollHandle = null;
  }
}

function stopCountdown() {
  if (countdownHandle) {
    window.clearInterval(countdownHandle);
    countdownHandle = null;
  }
  currentCountdownTargetMs = 0;
  currentCountdownMode = "";
  if (dom.countdownChip) dom.countdownChip.hidden = true;
}

function hideWinnerModal() {
  if (dom.winnerModal) dom.winnerModal.hidden = true;
}

function showExitConfirmModal() {
  hideWinnerModal();
  if (dom.exitConfirmModal) dom.exitConfirmModal.hidden = false;
}

function hideExitConfirmModal() {
  if (dom.exitConfirmModal) dom.exitConfirmModal.hidden = true;
}

function startPolling() {
  stopPolling();
  pollHandle = window.setInterval(() => {
    void loadState({ silent: true });
  }, POLL_MS);
}

function stopLoader() {
  if (loaderHandle) {
    window.clearTimeout(loaderHandle);
    loaderHandle = null;
  }
  if (loaderTickHandle) {
    window.clearInterval(loaderTickHandle);
    loaderTickHandle = null;
  }
  loaderStartedAt = 0;
  if (dom.loaderQuota) {
    dom.loaderQuota.hidden = true;
    dom.loaderQuota.textContent = "";
  }
}

function setPageMessage(text = "", type = "info") {
  if (!dom.pageMessage) return;
  const safeText = String(text || "").trim();
  if (!safeText) {
    dom.pageMessage.hidden = true;
    dom.pageMessage.textContent = "";
    dom.pageMessage.classList.remove("error");
    return;
  }
  dom.pageMessage.hidden = false;
  dom.pageMessage.textContent = safeText;
  dom.pageMessage.classList.toggle("error", type === "error");
}

function showOnly(section) {
  if (dom.loaderPanel) dom.loaderPanel.hidden = section !== "loader";
  if (dom.signedOutPanel) dom.signedOutPanel.hidden = section !== "signedOut";
  if (dom.leaderboardPanel) dom.leaderboardPanel.hidden = section !== "leaderboard";
}

function formatTimer(ms) {
  const totalSec = Math.max(0, Math.ceil(Number(ms || 0) / 1000));
  return `${totalSec}s`;
}

function formatCountdown(ms) {
  const totalSec = Math.max(0, Math.ceil(Number(ms || 0) / 1000));
  const minutes = Math.floor(totalSec / 60);
  const seconds = totalSec % 60;
  return `${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
}

function safeSessionStatus(session) {
  if (!session) return "active";
  if (String(session.status || "").toLowerCase() === "ended") return "ended";
  if (Number(session.endMs || 0) > 0 && Number(session.endMs) <= Date.now()) return "ended";
  return "active";
}

function normalizeSessions(rawSessions) {
  return (Array.isArray(rawSessions) ? rawSessions : [])
    .map((session) => ({
      sessionId: String(session?.sessionId || "").trim(),
      slotNumber: Number(session?.slotNumber || 0),
      startMs: Number(session?.startMs || 0),
      endMs: Number(session?.endMs || 0),
      status: String(session?.status || "active"),
    }))
    .filter((session) => session.sessionId);
}

function participantLabel(entry) {
  if (!entry) return "Participant";
  const rawId = String(entry.id || "").trim();
  if (entry.isUser || rawId === String(currentUser?.uid || "")) return "Vous";
  if (entry.isBot) return hashId(rawId || `bot-${Math.random()}`);
  return rawId || "Joueur";
}

function participantLabelFromId(id = "") {
  const rawId = String(id || "").trim();
  if (!rawId) return "Inconnu";
  if (rawId === String(currentUser?.uid || "")) return "Vous";
  if (rawId.startsWith("BOT-")) return hashId(rawId);
  return rawId;
}

function statusFromEntry(entry) {
  const activityStatus = String(entry?.activityStatus || "").trim().toLowerCase();
  return activityStatus === "playing"
    ? { label: "En train de jouer", className: "presence-playing" }
    : { label: "En ligne", className: "presence-live" };
}

function renderPresence(detailNode, status) {
  if (!detailNode || !status) return;
  detailNode.textContent = "";
  detailNode.className = status.className;
  const visual = document.createElement("span");
  visual.className = status.className === "presence-playing" ? "presence-loader" : "presence-dot";
  visual.setAttribute("aria-hidden", "true");
  const text = document.createElement("span");
  text.textContent = status.label;
  detailNode.appendChild(visual);
  detailNode.appendChild(text);
}

function updateCountdown() {
  if (!dom.countdownText || !dom.countdownChip) return;
  if (!currentCountdownTargetMs) {
    dom.countdownChip.hidden = true;
    return;
  }
  const remainingMs = Math.max(0, currentCountdownTargetMs - Date.now());
  dom.countdownChip.hidden = false;
  if (currentCountdownMode === "quota") {
    dom.countdownText.textContent = remainingMs > 0
      ? `Nouveau tournoi dans ${formatCountdown(remainingMs)}`
      : "Nouveau tournoi disponible";
    return;
  }
  dom.countdownText.textContent = remainingMs > 0
    ? `Fin dans ${formatCountdown(remainingMs)}`
    : "Tournoi terminé";
}

function startSessionCountdown(endMs) {
  stopCountdown();
  currentCountdownMode = "session";
  currentCountdownTargetMs = Number(endMs || 0);
  updateCountdown();
  if (!currentCountdownTargetMs) return;
  countdownHandle = window.setInterval(() => {
    updateCountdown();
  }, 1000);
}

function startQuotaCountdown(resetMs) {
  stopCountdown();
  currentCountdownMode = "quota";
  currentCountdownTargetMs = Number(resetMs || 0);
  updateCountdown();
  if (!currentCountdownTargetMs) return;
  countdownHandle = window.setInterval(() => {
    updateCountdown();
  }, 1000);
}

function renderEmptyLeaderboard(text) {
  if (dom.leaderboardState) {
    dom.leaderboardState.hidden = false;
    dom.leaderboardState.textContent = String(text || "");
  }
  if (dom.leaderTable) {
    dom.leaderTable.hidden = true;
    dom.leaderTable.innerHTML = "";
  }
}

function renderLeaderboard(entries, session) {
  const rows = Array.isArray(entries) ? entries : [];
  if (!rows.length) {
    renderEmptyLeaderboard("Le classement de ce tournoi n'est pas encore disponible.");
    return;
  }

  if (dom.leaderboardState) {
    dom.leaderboardState.hidden = true;
    dom.leaderboardState.textContent = "";
  }
  if (dom.leaderTable) {
    dom.leaderTable.hidden = false;
    dom.leaderTable.innerHTML = "";
  }

  rows.forEach((entry, index) => {
    const row = document.createElement("article");
    const isMe = entry.isUser || String(entry.id || "") === String(currentUser?.uid || "");
    row.className = `leader-row${isMe ? " me" : ""}`;

    const rank = document.createElement("div");
    rank.className = "rank-chip";
    rank.textContent = `#${index + 1}`;

    const name = document.createElement("div");
    name.className = "leader-name";
    const strong = document.createElement("strong");
    strong.textContent = participantLabel(entry);
    const detail = document.createElement("span");
    const status = statusFromEntry(entry);
    renderPresence(detail, status);
    name.appendChild(strong);
    name.appendChild(detail);

    const score = document.createElement("div");
    score.className = "leader-score";
    score.textContent = `${Number(entry.wins || 0)} victoire(s)`;

    row.appendChild(rank);
    row.appendChild(name);
    row.appendChild(score);
    dom.leaderTable?.appendChild(row);
  });

  if (dom.sessionBadge) {
    dom.sessionBadge.textContent = safeSessionStatus(session) === "ended"
      ? "Tournoi terminé"
      : "Tournoi actif";
  }

  startSessionCountdown(session?.endMs);

  if (dom.leaderboardMeta) {
    dom.leaderboardMeta.textContent = safeSessionStatus(session) === "ended"
      ? "Classement final du tournoi."
      : "Le classement se met à jour automatiquement pendant le tournoi.";
  }
}

function maybeShowWinnerModal(session) {
  const safeSessionId = String(session?.sessionId || "").trim();
  if (!safeSessionId || safeSessionStatus(session) !== "ended") return;
  if (shownWinnerModalSessionId === safeSessionId) return;

  const winnerId = String(session?.winnerId || "").trim();
  const winnerName = participantLabelFromId(winnerId);
  const rewardGranted = session?.rewardGranted === true && winnerId === String(currentUser?.uid || "");
  const rewardAmount = Number(session?.rewardAmountDoes || 0);

  if (dom.winnerModalName) {
    dom.winnerModalName.textContent = winnerName;
  }

  if (dom.winnerModalReward) {
    if (rewardGranted && rewardAmount > 0) {
      dom.winnerModalReward.textContent = `${rewardAmount.toLocaleString("fr-FR")} Does ont été ajoutés à ton compte.`;
    } else if (rewardAmount > 0) {
      dom.winnerModalReward.textContent = `Le tournoi est terminé. Le vainqueur remporte ${rewardAmount.toLocaleString("fr-FR")} Does.`;
    } else {
      dom.winnerModalReward.textContent = "Le tournoi est terminé.";
    }
  }

  if (dom.winnerModal) {
    dom.winnerModal.hidden = false;
  }
  shownWinnerModalSessionId = safeSessionId;
}

async function ensureSessions() {
  const response = await ensureSessionsFn({});
  const data = response?.data || {};
  sessions = normalizeSessions(data.sessions);
  return {
    sessions,
    currentSessionId: String(data.currentSessionId || "").trim(),
    quota: data.quota && typeof data.quota === "object" ? data.quota : {},
    canPlay: data.canPlay === true,
    hasActiveSession: data.hasActiveSession === true,
    isLocked: data.isLocked === true,
    playsUsedToday: Number(data.playsUsedToday || 0),
    playsRemainingToday: Number(data.playsRemainingToday || 0),
    dailyLimit: Number(data.dailyLimit || 3),
    nextResetMs: Number(data.nextResetMs || 0),
  };
}

function findUsableSessionId(preferredSessionId = "") {
  const preferred = String(preferredSessionId || "").trim();
  if (preferred && sessions.some((session) => session.sessionId === preferred && safeSessionStatus(session) !== "ended")) {
    return preferred;
  }
  const activeSession = sessions.find((session) => safeSessionStatus(session) !== "ended");
  return String(activeSession?.sessionId || sessions[0]?.sessionId || "").trim();
}

async function selectTournamentSession(sessionId) {
  const nextSessionId = String(sessionId || "").trim();
  if (!nextSessionId) return "";
  await selectSessionFn({ sessionId: nextSessionId });
  currentSessionId = nextSessionId;
  setStoredTournamentSession(currentUser?.uid, nextSessionId);
  return nextSessionId;
}

async function loadState({ silent = false } = {}) {
  if (!currentUser || !currentSessionId) return;

  try {
    const response = await stateFn({ sessionId: currentSessionId });
    const data = response?.data || {};
    const session = data.session || {};
    const leaderboard = Array.isArray(data.leaderboard) ? data.leaderboard : [];
    showOnly("leaderboard");
    renderLeaderboard(leaderboard, session);
    maybeShowWinnerModal(session);
    setPageMessage("");
  } catch (error) {
    console.error("[TOURNOIS] load state error", error);
    if (!silent) {
      showOnly("leaderboard");
      renderEmptyLeaderboard("Impossible de charger le classement pour le moment.");
      setPageMessage("Le tournoi n'a pas pu être chargé. Réessaie dans un instant.", "error");
    }
  }
}

function renderLoaderCountdown() {
  if (!dom.loaderMeta || !loaderStartedAt) return;
  const remainingMs = Math.max(0, MATCHING_DELAY_MS - (Date.now() - loaderStartedAt));
  dom.loaderMeta.textContent = `${formatTimer(remainingMs)} restantes`;
}

function renderLoaderQuota(quota = {}) {
  if (!dom.loaderQuota) return;

  const dailyLimit = Math.max(1, Number(quota?.dailyLimit || 3));
  const remaining = Math.max(0, Number(quota?.playsRemainingToday || 0));
  const used = Math.max(0, Number(quota?.playsUsedToday || 0));

  dom.loaderQuota.hidden = false;
  if (remaining <= 0) {
    dom.loaderQuota.textContent = `Apres ce tournoi, il ne te reste plus de tournoi aujourd'hui (${used}/${dailyLimit} utilises).`;
    return;
  }

  dom.loaderQuota.textContent = `Apres ce lancement, il te restera ${remaining} tournoi(s) sur ${dailyLimit} aujourd'hui.`;
}

function startMatchingLoader(quota, onDone) {
  stopLoader();
  loaderStartedAt = Date.now();
  showOnly("loader");
  renderLoaderCountdown();
  renderLoaderQuota(quota);
  loaderTickHandle = window.setInterval(() => {
    renderLoaderCountdown();
  }, 250);
  loaderHandle = window.setTimeout(() => {
    stopLoader();
    void onDone();
  }, MATCHING_DELAY_MS);
}

async function enterTournamentFlow() {
  const myToken = ++refreshToken;
  stopPolling();
  stopLoader();
  hideWinnerModal();

  try {
    const result = await ensureSessions();
    if (myToken !== refreshToken) return;

    if (result.isLocked && !result.hasActiveSession) {
      currentSessionId = "";
      showOnly("leaderboard");
      if (dom.sessionBadge) dom.sessionBadge.textContent = "Limite du jour atteinte";
      if (dom.leaderboardMeta) {
        dom.leaderboardMeta.textContent = `Tu as utilisé ${result.dailyLimit} tournoi(s) sur ${result.dailyLimit} aujourd'hui.`;
      }
      renderEmptyLeaderboard("Tu as deja utilise tes 3 tournois du jour. Reviens a la reinitialisation pour rejouer.");
      setPageMessage("Limite atteinte: 3 tournois maximum par jour pour ce compte.", "error");
      startQuotaCountdown(result.nextResetMs);
      return;
    }

    const storedSessionId = getStoredTournamentSession(currentUser?.uid);
    const activeSessionId = findUsableSessionId(storedSessionId || result.currentSessionId);

    if (!activeSessionId) {
      showOnly("leaderboard");
      renderEmptyLeaderboard("Aucun tournoi n'est disponible pour le moment.");
      setPageMessage("Aucun tournoi actif n'a été trouvé pour ce compte.", "error");
      return;
    }

    if (storedSessionId && storedSessionId === activeSessionId) {
      currentSessionId = activeSessionId;
      await loadState();
      startPolling();
      return;
    }

    startMatchingLoader(result, async () => {
      if (myToken !== refreshToken) return;
      try {
        await selectTournamentSession(activeSessionId);
        if (myToken !== refreshToken) return;
        await loadState();
        startPolling();
      } catch (error) {
        console.error("[TOURNOIS] select session error", error);
        showOnly("leaderboard");
        renderEmptyLeaderboard("Le tournoi n'a pas pu être rejoint.");
        setPageMessage("Impossible de rejoindre ce tournoi maintenant.", "error");
      }
    });
  } catch (error) {
    console.error("[TOURNOIS] ensure sessions error", error);
    showOnly("leaderboard");
    renderEmptyLeaderboard("Impossible de charger le tournoi.");
    setPageMessage("Le tournoi n'a pas pu être préparé pour le moment.", "error");
  }
}

function showSignedOutState() {
  ++refreshToken;
  stopPolling();
  stopLoader();
  stopCountdown();
  hideWinnerModal();
  hideExitConfirmModal();
  currentSessionId = "";
  sessions = [];
  shownWinnerModalSessionId = "";
  setPageMessage("");
  showOnly("signedOut");
}

async function exitTournament() {
  const sessionIdToAbandon = String(currentSessionId || getStoredTournamentSession(currentUser?.uid) || "").trim();
  setStoredTournamentSession(currentUser?.uid, "");
  ++refreshToken;
  stopPolling();
  stopLoader();
  stopCountdown();
  hideWinnerModal();
  hideExitConfirmModal();
  currentSessionId = "";
  if (sessionIdToAbandon) {
    try {
      await abandonTournamentFn({ sessionId: sessionIdToAbandon });
    } catch (error) {
      console.error("[TOURNOIS] abandon session error", error);
    }
  }
  window.location.href = "./inedex.html";
}

function replayTournament() {
  setStoredTournamentSession(currentUser?.uid, "");
  stopPolling();
  stopLoader();
  stopCountdown();
  hideWinnerModal();
  hideExitConfirmModal();
  currentSessionId = "";
  shownWinnerModalSessionId = "";
  void enterTournamentFlow();
}

function bindUi() {
  dom.playNowBtn?.addEventListener("click", () => {
    void handlePlayNow();
  });
  dom.exitTournamentBtn?.addEventListener("click", () => {
    showExitConfirmModal();
  });
  dom.exitConfirmStayBtn?.addEventListener("click", () => {
    hideExitConfirmModal();
  });
  dom.exitConfirmLeaveBtn?.addEventListener("click", () => {
    void exitTournament();
  });
  dom.winnerReplayBtn?.addEventListener("click", () => {
    replayTournament();
  });
  dom.winnerBackBtn?.addEventListener("click", () => {
    void exitTournament();
  });
  dom.stakeSelectionClose?.addEventListener("click", closeStakeSelection);
  dom.stakeSelectionOverlay?.addEventListener("click", (event) => {
    if (event.target === dom.stakeSelectionOverlay) {
      closeStakeSelection();
    }
  });
  dom.doesRequiredClose?.addEventListener("click", closeDoesRequired);
  dom.doesRequiredOverlay?.addEventListener("click", (event) => {
    if (event.target === dom.doesRequiredOverlay) {
      closeDoesRequired();
    }
  });
  dom.doesRequiredOpenProfile?.addEventListener("click", () => {
    closeDoesRequired();
    openProfilePage();
  });
  dom.stakeOptionsGrid?.addEventListener("click", async (event) => {
    const btn = event.target.closest(".stake-option-btn");
    if (!btn || !dom.stakeOptionsGrid?.contains(btn)) return;
    const available = btn.getAttribute("data-available") === "1";
    if (!available) return;

    const stakeAmount = Number(btn.getAttribute("data-stake") || 100);
    await withButtonLoading(btn, async () => {
      const xchangeModule = await loadXchangeModule();
      await xchangeModule.ensureXchangeState(currentUser?.uid);
      const state = xchangeModule.getXchangeState(window.__userBaseBalance || window.__userBalance || 0, currentUser?.uid);
      if ((state?.does || 0) < stakeAmount) {
        closeStakeSelection();
        openDoesRequired();
        return;
      }
      closeStakeSelection();
      showGlobalLoading("Ouverture de la partie...");
      window.location.href = `./jeu.html?autostart=1&stake=${stakeAmount}`;
    }, { loadingLabel: "Vérification..." });
  });
}

function initAuth() {
  auth.onAuthStateChanged((user) => {
    currentUser = user || null;
    if (!user) {
      showSignedOutState();
      return;
    }
    void enterTournamentFlow();
  });
}

function init() {
  bindUi();
  showSignedOutState();
  initAuth();
}

init();
