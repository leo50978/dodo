import {
  auth,
  db,
  doc,
  setDoc,
  serverTimestamp,
  onSnapshot,
  onAuthStateChanged,
} from "./firebase-init.js";
import {
  joinMatchmakingMorpionSecure,
  ensureRoomReadyMorpionSecure,
  touchRoomPresenceMorpionSecure,
  ackRoomStartSeenMorpionSecure,
  leaveRoomMorpionSecure,
  submitActionMorpionSecure,
  claimWinRewardMorpionSecure,
  getMyActiveMorpionInviteSecure,
  respondMorpionPlayInviteSecure,
  getMorpionMatchmakingHintSecure,
  getMyMorpionWhatsappPreferenceSecure,
  saveMorpionWhatsappPreferenceSecure,
  removeMorpionWhatsappPreferenceSecure,
  listRecentMorpionWhatsappContactsSecure,
} from "./secure-functions.js";

const MORPION_ROOMS = "morpionRooms";
const MORPION_GAME_STATES = "morpionGameStates";
const ALLOWED_MORPION_STAKE_AMOUNTS = Object.freeze([500]);
const TURN_LIMIT_SECONDS = 30;
const TURN_LIMIT_MS = TURN_LIMIT_SECONDS * 1000;
const MATCHMAKING_WAIT_SECONDS = 15;
const MATCHMAKING_WAIT_MS = MATCHMAKING_WAIT_SECONDS * 1000;
const PRESENCE_PING_MS = 20 * 1000;
const SITE_PRESENCE_PING_MS = 25 * 1000;
const SITE_PRESENCE_TTL_MS = 70 * 1000;
const INVITE_POLL_MS = 6 * 1000;
const ABANDONED_ROOMS_STORAGE_KEY = "domino_morpion_abandoned_rooms_v1";
const MORPION_BOT_NUMERIC_IDS = Object.freeze([35601379, 40507232, 41752992]);

const URL_PARAMS = new URLSearchParams(window.location.search);
const parsedRequestedStake = Number.parseInt(String(URL_PARAMS.get("stake") ?? 500), 10);
const requestedStake = Number.isFinite(parsedRequestedStake) ? parsedRequestedStake : 500;
function getFriendMorpionRoomIdFromUrl() {
  return String(URL_PARAMS.get("friendMorpionRoomId") || "").trim();
}

function isFriendMorpionFlowFromUrl() {
  return getFriendMorpionRoomIdFromUrl().length > 0 || String(URL_PARAMS.get("roomMode") || "").trim() === "morpion_friends";
}

const selectedStakeDoes = isFriendMorpionFlowFromUrl()
  ? Math.max(1, Number.parseInt(String(requestedStake || 0), 10) || 500)
  : (ALLOWED_MORPION_STAKE_AMOUNTS.includes(requestedStake) ? requestedStake : 500);

const dom = {
  board: document.getElementById("morpionBoard"),
  winLine: null,
  waitingModal: document.getElementById("morpionWaitingModal"),
  inviteModal: document.getElementById("morpionInviteModal"),
  inviteTitle: document.getElementById("morpionInviteTitle"),
  inviteCopy: document.getElementById("morpionInviteCopy"),
  inviteAcceptBtn: document.getElementById("morpionInviteAcceptBtn"),
  inviteRefuseBtn: document.getElementById("morpionInviteRefuseBtn"),
  ruleModal: document.getElementById("morpionRuleModal"),
  ruleContinueBtn: document.getElementById("morpionRuleContinueBtn"),
  waitingTitle: document.getElementById("morpionWaitingTitle"),
  waitingCopy: document.getElementById("morpionWaitingCopy"),
  waitingTimerWrap: document.getElementById("morpionWaitingTimerWrap"),
  waitingTimerValue: document.getElementById("morpionWaitingTimerValue"),
  waitingActions: document.getElementById("morpionWaitingActions"),
  waitingHomeBtn: document.getElementById("morpionWaitingHomeBtn"),
  waitingRetryBtn: document.getElementById("morpionWaitingRetryBtn"),
  waitingExtendBtn: document.getElementById("morpionWaitingExtendBtn"),
  waitingStopExtendBtn: document.getElementById("morpionWaitingStopExtendBtn"),
  waitingNotifyBtn: document.getElementById("morpionWaitingNotifyBtn"),
  waitingWhatsappBtn: document.getElementById("morpionWaitingWhatsappBtn"),
  waitingContactsBtn: document.getElementById("morpionWaitingContactsBtn"),
  whatsappModal: document.getElementById("morpionWhatsappModal"),
  whatsappInput: document.getElementById("morpionWhatsappInput"),
  whatsappStatus: document.getElementById("morpionWhatsappStatus"),
  whatsappSaveBtn: document.getElementById("morpionWhatsappSaveBtn"),
  whatsappRemoveBtn: document.getElementById("morpionWhatsappRemoveBtn"),
  whatsappCloseBtn: document.getElementById("morpionWhatsappCloseBtn"),
  whatsappSavedWrap: document.getElementById("morpionWhatsappSavedWrap"),
  whatsappSavedValue: document.getElementById("morpionWhatsappSavedValue"),
  whatsappCloseTargets: Array.from(document.querySelectorAll("[data-whatsapp-close]")),
  contactsModal: document.getElementById("morpionContactsModal"),
  contactsList: document.getElementById("morpionContactsList"),
  contactsCloseBtn: document.getElementById("morpionContactsCloseBtn"),
  contactsCloseTargets: Array.from(document.querySelectorAll("[data-contacts-close]")),
  resultModal: document.getElementById("morpionResultModal"),
  resultEyebrow: document.getElementById("morpionResultEyebrow"),
  resultTitle: document.getElementById("morpionResultTitle"),
  resultCopy: document.getElementById("morpionResultCopy"),
  resultReplayBtn: document.getElementById("morpionResultReplayBtn"),
  resultHomeBtn: document.getElementById("morpionResultHomeBtn"),
  quitBtn: document.getElementById("morpionQuitBtn"),
  quitModal: document.getElementById("morpionQuitModal"),
  quitReplayBtn: document.getElementById("morpionQuitReplayBtn"),
  quitHomeBtn: document.getElementById("morpionQuitHomeBtn"),
  quitCloseTargets: Array.from(document.querySelectorAll("[data-quit-close]")),
  revealResultBtn: document.getElementById("morpionRevealResultBtn"),
  opponentCard: document.querySelector('[data-player-side="opponent"]'),
  selfCard: document.querySelector('[data-player-side="self"]'),
  opponentLabel: document.getElementById("morpionOpponentLabel"),
  opponentName: document.getElementById("morpionOpponentName"),
  selfName: document.getElementById("morpionSelfName"),
  walletValue: document.getElementById("morpionWalletValue"),
  opponentSymbol: document.getElementById("morpionOpponentSymbol"),
  selfSymbol: document.getElementById("morpionSelfSymbol"),
  opponentTimerLabel: document.getElementById("morpionOpponentTimerLabel"),
  selfTimerLabel: document.getElementById("morpionSelfTimerLabel"),
  opponentTimerFill: document.getElementById("morpionOpponentTimerFill"),
  selfTimerFill: document.getElementById("morpionSelfTimerFill"),
};

let currentUser = null;
let currentRoomId = "";
let currentRoomData = null;
let currentGameState = null;
let currentSeatIndex = -1;
let roomUnsub = null;
let stateUnsub = null;
let presenceTimer = null;
let sitePresenceTimer = null;
let turnTick = null;
let waitingEnsureTimer = null;
let botTurnNudgeTimer = null;
let turnTimeoutNudgeTimer = null;
let joining = false;
let ensuringRoom = false;
let actionSending = false;
let rewardClaiming = false;
let rewardClaimed = false;
let startRevealAcked = false;
let leavingRoom = false;
let turnTimeoutRequestInFlight = false;
let presencePingInFlight = false;
let sitePresencePingInFlight = false;
let clientUnsub = null;
let currentDoesBalance = null;
let endResultTimer = null;
let lastHandledEndKey = "";
let pendingEndModalPayload = null;
let winLineVisible = false;
let fallbackOpponentAlias = "";
let fallbackOpponentAliasRoomId = "";
let turnRuleAccepted = false;
let invitePollTimer = null;
let invitePollInFlight = false;
let activeInviteId = "";
let matchmakingWaitDeadlineMs = 0;
let matchmakingWaitRoomId = "";
let matchmakingWaitExpired = false;
let matchmakingExtendedWaiting = false;
let matchmakingHintInFlight = false;
let matchmakingHintRoomId = "";
let matchmakingHintCheckedAtMs = 0;
let matchmakingHintHasOddPlayingHumans = false;
let matchmakingHintMessage = "";
let myWhatsappContact = null;
let recentWhatsappContacts = [];
let whatsappPreferenceLoaded = false;

function morpionDebug(event, payload = {}) {
  try {
    console.log("[MORPION_DEBUG]", event, {
      ts: new Date().toISOString(),
      roomId: currentRoomId || "",
      seat: currentSeatIndex,
      ...payload,
    });
  } catch (_) {
  }
}

function safeInt(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.trunc(parsed) : fallback;
}

function formatDoesAmount(value) {
  return new Intl.NumberFormat("fr-FR").format(Math.max(0, safeInt(value, 0)));
}

function makePlayerId(seed = "") {
  const source = String(seed || "").trim();
  let hash = 0;
  for (let index = 0; index < source.length; index += 1) {
    hash = ((hash * 31) + source.charCodeAt(index)) % 1_000_000;
  }
  const normalizedHash = Math.max(0, Math.abs(hash));
  const safeCode = ((normalizedHash % 900000) + 100000);
  return `Joueur ID-${String(safeCode).padStart(6, "0")}`;
}

function randomPlayerIdLabel() {
  const value = Math.floor(Math.random() * 900000) + 100000;
  return `Joueur ID-${String(value)}`;
}

function pickBotNumericId() {
  const roomSeed = String(currentRoomId || "").trim();
  let hash = 0;
  for (let index = 0; index < roomSeed.length; index += 1) {
    hash = ((hash * 31) + roomSeed.charCodeAt(index)) >>> 0;
  }
  const slot = MORPION_BOT_NUMERIC_IDS.length > 0
    ? (hash % MORPION_BOT_NUMERIC_IDS.length)
    : 0;
  return MORPION_BOT_NUMERIC_IDS[slot] || MORPION_BOT_NUMERIC_IDS[0];
}

function readAbandonedRoomIds() {
  try {
    const raw = window.localStorage.getItem(ABANDONED_ROOMS_STORAGE_KEY);
    const parsed = JSON.parse(raw || "[]");
    if (!Array.isArray(parsed)) return [];
    return parsed.map((item) => String(item || "").trim()).filter(Boolean).slice(0, 8);
  } catch (_) {
    return [];
  }
}

function writeAbandonedRoomIds(roomIds = []) {
  try {
    window.localStorage.setItem(ABANDONED_ROOMS_STORAGE_KEY, JSON.stringify(roomIds.slice(0, 8)));
  } catch (_) {
  }
}

function markRoomAbandoned(roomId = "") {
  const safeRoomId = String(roomId || "").trim();
  if (!safeRoomId) return;
  const next = [safeRoomId, ...readAbandonedRoomIds().filter((item) => item !== safeRoomId)];
  writeAbandonedRoomIds(next);
}

function clearRoomAbandoned(roomId = "") {
  const safeRoomId = String(roomId || "").trim();
  if (!safeRoomId) return;
  writeAbandonedRoomIds(readAbandonedRoomIds().filter((item) => item !== safeRoomId));
}

function otherSeat(seat) {
  return seat === 0 ? 1 : 0;
}

function seatSymbol(seat) {
  return seat === 0 ? "X" : "O";
}

function getOpponentSeat() {
  if (currentSeatIndex === 0) return 1;
  if (currentSeatIndex === 1) return 0;
  return 0;
}

function getSelfName() {
  const fallback = currentUser?.displayName || currentUser?.email || "Moi";
  const roomName = Array.isArray(currentRoomData?.playerNames) ? String(currentRoomData.playerNames[currentSeatIndex] || "").trim() : "";
  return roomName || fallback;
}

function getOpponentName() {
  const opponentSeat = getOpponentSeat();
  const roomName = Array.isArray(currentRoomData?.playerNames) ? String(currentRoomData.playerNames[opponentSeat] || "").trim() : "";
  const opponentUid = Array.isArray(currentRoomData?.playerUids) ? String(currentRoomData.playerUids[opponentSeat] || "").trim() : "";
  if (!opponentUid && safeInt(currentRoomData?.botCount, 0) > 0 && safeInt(currentRoomData?.humanCount, 0) <= 1) {
    return `Joueur ${pickBotNumericId()}`;
  }
  return roomName || "En attente...";
}

function getOpponentLabel() {
  const opponentSeat = getOpponentSeat();
  const opponentUid = Array.isArray(currentRoomData?.playerUids) ? String(currentRoomData.playerUids[opponentSeat] || "").trim() : "";
  if (!opponentUid) {
    if (safeInt(currentRoomData?.botCount, 0) > 0 && safeInt(currentRoomData?.humanCount, 0) <= 1) {
      return `Joueur ID-${pickBotNumericId()}`;
    }
    const roomKey = String(currentRoomId || "").trim();
    if (!fallbackOpponentAlias || fallbackOpponentAliasRoomId !== roomKey) {
      fallbackOpponentAlias = randomPlayerIdLabel();
      fallbackOpponentAliasRoomId = roomKey;
    }
    return fallbackOpponentAlias;
  }
  return makePlayerId(`${currentRoomId}:${opponentUid}:${opponentSeat}`);
}

function renderWalletValue() {
  if (!dom.walletValue) return;
  if (currentDoesBalance === null) {
    dom.walletValue.textContent = "--";
    return;
  }
  dom.walletValue.textContent = `${formatDoesAmount(currentDoesBalance)} Does`;
}

function normalizeWhatsappInput(value = "") {
  return String(value || "")
    .replace(/[^\d+\-\s().]/g, "")
    .trim()
    .slice(0, 40);
}

function extractDigits(value = "") {
  return String(value || "").replace(/\D/g, "");
}

function formatWhatsappValue(value = "") {
  const digits = extractDigits(value);
  if (!digits) return "";
  return value.startsWith("+") ? value : `+${digits}`;
}

function formatRecentContactTime(value = 0) {
  const safeValue = Number(value);
  if (!Number.isFinite(safeValue) || safeValue <= 0) return "";
  try {
    return new Intl.DateTimeFormat("fr-FR", {
      dateStyle: "short",
      timeStyle: "short",
    }).format(new Date(safeValue));
  } catch (_) {
    return "";
  }
}

function buildWhatsappDeepLink(digits = "") {
  const normalizedDigits = extractDigits(digits);
  if (!normalizedDigits) return "";
  const message = encodeURIComponent("Bonjour, je veux jouer au morpion sur Dominoes Lakay. Es-tu disponible ?");
  return `https://wa.me/${normalizedDigits}?text=${message}`;
}

function setWhatsappStatus(message = "", tone = "") {
  if (!dom.whatsappStatus) return;
  dom.whatsappStatus.textContent = String(message || "");
  dom.whatsappStatus.classList.toggle("is-error", tone === "error");
  dom.whatsappStatus.classList.toggle("is-success", tone === "success");
}

function renderWhatsappPreference() {
  const savedNumber = String(myWhatsappContact?.whatsappNumber || "").trim();
  if (dom.whatsappSavedWrap) dom.whatsappSavedWrap.classList.toggle("hidden", !savedNumber);
  if (dom.whatsappSavedValue) dom.whatsappSavedValue.textContent = savedNumber || "";
  if (dom.whatsappRemoveBtn) dom.whatsappRemoveBtn.classList.toggle("hidden", !savedNumber);
  if (dom.whatsappInput && document.activeElement !== dom.whatsappInput) {
    dom.whatsappInput.value = savedNumber || dom.whatsappInput.value || "";
  }
}

function openWhatsappModal() {
  dom.whatsappModal?.classList.remove("hidden");
  renderWhatsappPreference();
  if (whatsappPreferenceLoaded && myWhatsappContact) {
    setWhatsappStatus("Ton numero est deja partage. Tu peux le mettre a jour ou le retirer.", "success");
  } else if (whatsappPreferenceLoaded && !myWhatsappContact) {
    setWhatsappStatus("Ton numero n'est pas encore partage. Tu peux l'ajouter ici.", "");
  }
}

function closeWhatsappModal() {
  dom.whatsappModal?.classList.add("hidden");
}

function closeContactsModal() {
  dom.contactsModal?.classList.add("hidden");
}

function renderRecentWhatsappContacts() {
  if (!dom.contactsList) return;
  if (!Array.isArray(recentWhatsappContacts) || recentWhatsappContacts.length === 0) {
    dom.contactsList.innerHTML = `
      <div class="contact-empty">
        Aucun numero recent n'est disponible pour le moment. Laisse ton WhatsApp pour aider les prochains joueurs a te retrouver.
      </div>
    `;
    return;
  }

  dom.contactsList.innerHTML = recentWhatsappContacts.map((contact, index) => {
    const label = String(contact?.label || `Joueur ${index + 1}`);
    const whatsappNumber = String(contact?.whatsappNumber || "").trim();
    const whatsappDigits = extractDigits(contact?.whatsappDigits || whatsappNumber);
    const online = contact?.online === true;
    const presenceLabel = online ? "En ligne" : "Hors ligne";
    const lastSeen = formatRecentContactTime(contact?.lastInterestAtMs || contact?.lastSeenAtMs);
    const whatsappLink = buildWhatsappDeepLink(whatsappDigits);
    return `
      <article class="contact-card">
        <div class="contact-card__top">
          <div class="contact-card__identity">
            <div class="contact-card__label">${label}</div>
            <div class="contact-card__value">${whatsappNumber}</div>
            <div class="contact-card__meta">${lastSeen ? `Actif le ${lastSeen}` : "Activite recente"}<\/div>
          </div>
          <span class="presence-pill ${online ? "is-online" : ""}">
            <span class="presence-pill__dot"><\/span>
            ${presenceLabel}
          <\/span>
        </div>
        <div class="contact-card__actions">
          <button class="btn btn--primary" type="button" data-contact-action="copy" data-contact-number="${whatsappNumber}">
            Copier
          <\/button>
          <a class="btn btn--ghost" href="${whatsappLink || "#"}" ${whatsappLink ? `target="_blank" rel="noopener noreferrer"` : `aria-disabled="true"`}>
            WhatsApp
          <\/a>
        </div>
      </article>
    `;
  }).join("");
}

async function copyToClipboard(value = "") {
  const safeValue = String(value || "").trim();
  if (!safeValue) return false;
  try {
    await navigator.clipboard.writeText(safeValue);
    return true;
  } catch (_) {
    return false;
  }
}

async function loadWhatsappPreference(force = false) {
  if (!currentUser?.uid) return;
  if (whatsappPreferenceLoaded && !force) return;
  try {
    const result = await getMyMorpionWhatsappPreferenceSecure({});
    myWhatsappContact = result?.contact && typeof result.contact === "object" ? result.contact : null;
    whatsappPreferenceLoaded = true;
    renderWhatsappPreference();
  } catch (error) {
    console.warn("[MORPION] load whatsapp preference failed", error);
  }
}

async function saveWhatsappPreference() {
  const rawValue = normalizeWhatsappInput(dom.whatsappInput?.value || "");
  if (!extractDigits(rawValue)) {
    setWhatsappStatus("Entre un numero WhatsApp valide pour continuer.", "error");
    return;
  }

  if (dom.whatsappSaveBtn) dom.whatsappSaveBtn.disabled = true;
  setWhatsappStatus("Enregistrement du numero...", "");
  try {
    const result = await saveMorpionWhatsappPreferenceSecure({ whatsappNumber: rawValue });
    myWhatsappContact = result?.contact && typeof result.contact === "object" ? result.contact : null;
    whatsappPreferenceLoaded = true;
    renderWhatsappPreference();
    setWhatsappStatus("Ton numero WhatsApp est maintenant visible dans la liste des joueurs recents.", "success");
  } catch (error) {
    setWhatsappStatus(error?.message || "Impossible d'enregistrer ton numero pour le moment.", "error");
  } finally {
    if (dom.whatsappSaveBtn) dom.whatsappSaveBtn.disabled = false;
  }
}

async function removeWhatsappPreference() {
  if (dom.whatsappRemoveBtn) dom.whatsappRemoveBtn.disabled = true;
  setWhatsappStatus("Retrait du numero...", "");
  try {
    await removeMorpionWhatsappPreferenceSecure({});
    myWhatsappContact = null;
    whatsappPreferenceLoaded = true;
    if (dom.whatsappInput) dom.whatsappInput.value = "";
    renderWhatsappPreference();
    setWhatsappStatus("Ton numero a ete retire de la liste des joueurs recents.", "success");
  } catch (error) {
    setWhatsappStatus(error?.message || "Impossible de retirer ton numero pour le moment.", "error");
  } finally {
    if (dom.whatsappRemoveBtn) dom.whatsappRemoveBtn.disabled = false;
  }
}

async function loadRecentWhatsappContacts() {
  if (!dom.contactsList) return;
  dom.contactsList.innerHTML = `<div class="contact-empty">Chargement des joueurs recents...<\/div>`;
  try {
    const result = await listRecentMorpionWhatsappContactsSecure({});
    recentWhatsappContacts = Array.isArray(result?.contacts) ? result.contacts : [];
    renderRecentWhatsappContacts();
  } catch (error) {
    recentWhatsappContacts = [];
    dom.contactsList.innerHTML = `<div class="contact-empty">${String(error?.message || "Impossible de charger la liste pour le moment.")}<\/div>`;
  }
}

async function openContactsModal() {
  dom.contactsModal?.classList.remove("hidden");
  await loadRecentWhatsappContacts();
}

async function touchClientSitePresence() {
  if (!currentUser?.uid || sitePresencePingInFlight) return;
  sitePresencePingInFlight = true;
  const nowMs = Date.now();
  try {
    await setDoc(doc(db, "clients", currentUser.uid), {
      uid: currentUser.uid,
      email: String(currentUser.email || ""),
      lastSeenAt: serverTimestamp(),
      lastSeenAtMs: nowMs,
      updatedAt: serverTimestamp(),
      sitePresencePage: "morpion",
      sitePresenceExpiresAtMs: nowMs + SITE_PRESENCE_TTL_MS,
      morpionLastInterestAtMs: nowMs,
    }, { merge: true });
  } catch (error) {
    console.warn("[MORPION] site presence update failed", error);
  } finally {
    sitePresencePingInFlight = false;
  }
}

function currentBoard() {
  const board = Array.isArray(currentGameState?.board) ? currentGameState.board : [];
  return board.length === 225 ? board : Array.from({ length: 225 }, () => -1);
}

function isMyTurn() {
  return currentRoomData?.status === "playing"
    && currentRoomData?.startRevealPending !== true
    && safeInt(currentRoomData?.currentPlayer, -1) === currentSeatIndex;
}

function openWaitingModal(title = "", copy = "") {
  if (dom.waitingTitle) dom.waitingTitle.textContent = String(title || "Recherche d'un adversaire...");
  if (dom.waitingCopy) dom.waitingCopy.textContent = String(copy || "");
  dom.waitingModal?.classList.remove("hidden");
}

function closeWaitingModal() {
  dom.waitingModal?.classList.add("hidden");
}

function startMatchmakingWaitCycle() {
  matchmakingWaitDeadlineMs = Date.now() + MATCHMAKING_WAIT_MS;
  matchmakingWaitExpired = false;
  matchmakingExtendedWaiting = false;
  matchmakingHintInFlight = false;
  matchmakingHintRoomId = "";
  matchmakingHintCheckedAtMs = 0;
  matchmakingHintHasOddPlayingHumans = false;
  matchmakingHintMessage = "";
  if (dom.waitingActions) dom.waitingActions.classList.add("hidden");
}

function resetMatchmakingWaitState() {
  matchmakingWaitDeadlineMs = 0;
  matchmakingWaitRoomId = "";
  matchmakingWaitExpired = false;
  matchmakingExtendedWaiting = false;
  matchmakingHintInFlight = false;
  matchmakingHintRoomId = "";
  matchmakingHintCheckedAtMs = 0;
  matchmakingHintHasOddPlayingHumans = false;
  matchmakingHintMessage = "";
  if (dom.waitingActions) dom.waitingActions.classList.add("hidden");
}

async function refreshMatchmakingHintIfNeeded(force = false) {
  if (!currentRoomId) return;
  if (matchmakingHintInFlight) return;
  const nowMs = Date.now();
  const stale = (nowMs - matchmakingHintCheckedAtMs) > 10_000;
  if (!force && !stale && matchmakingHintRoomId === currentRoomId) return;

  matchmakingHintInFlight = true;
  try {
    const result = await getMorpionMatchmakingHintSecure({ roomId: currentRoomId });
    matchmakingHintRoomId = currentRoomId;
    matchmakingHintCheckedAtMs = Number(result?.checkedAtMs || nowMs);
    matchmakingHintHasOddPlayingHumans = result?.hasOddActivePlayingHumans === true;
    matchmakingHintMessage = String(result?.message || "").trim();
  } catch (_) {
  } finally {
    matchmakingHintInFlight = false;
    renderMatchmakingWaitingModal();
  }
}

function setWaitingActionsVisibility({
  showHome = true,
  showRetry = true,
  showExtend = true,
  showStopExtended = false,
  showNotify = true,
  showWhatsapp = true,
  showContacts = true,
} = {}) {
  if (dom.waitingHomeBtn) dom.waitingHomeBtn.classList.toggle("hidden", !showHome);
  if (dom.waitingRetryBtn) dom.waitingRetryBtn.classList.toggle("hidden", !showRetry);
  if (dom.waitingExtendBtn) dom.waitingExtendBtn.classList.toggle("hidden", !showExtend);
  if (dom.waitingStopExtendBtn) dom.waitingStopExtendBtn.classList.toggle("hidden", !showStopExtended);
  if (dom.waitingNotifyBtn) dom.waitingNotifyBtn.classList.toggle("hidden", !showNotify);
  if (dom.waitingWhatsappBtn) dom.waitingWhatsappBtn.classList.toggle("hidden", !showWhatsapp);
  if (dom.waitingContactsBtn) dom.waitingContactsBtn.classList.toggle("hidden", !showContacts);
}

function renderMatchmakingWaitingModal() {
  if (String(currentRoomData?.status || "") !== "waiting") {
    resetMatchmakingWaitState();
    return;
  }

  const humans = safeInt(currentRoomData?.humanCount, 0);
  if (humans >= 2) {
    openWaitingModal("Adversaire trouve", "La partie demarre...");
    if (dom.waitingTimerWrap) dom.waitingTimerWrap.classList.add("hidden");
    if (dom.waitingActions) dom.waitingActions.classList.add("hidden");
    return;
  }

  const roomKey = String(currentRoomId || "").trim();
  if (!roomKey) return;
  if (matchmakingWaitRoomId !== roomKey) {
    matchmakingWaitRoomId = roomKey;
    startMatchmakingWaitCycle();
  } else if (matchmakingWaitDeadlineMs <= 0) {
    startMatchmakingWaitCycle();
  }

  const now = Date.now();
  const remainingMs = Math.max(0, matchmakingWaitDeadlineMs - now);
  const remainingSeconds = Math.ceil(remainingMs / 1000);

  if (remainingMs > 0) {
    openWaitingModal(
      "Recherche d'un joueur...",
      "Nous cherchons un joueur reel. Si personne ne rejoint dans 15 secondes, il n'y aura pas de partie."
    );
    if (dom.waitingTimerWrap) dom.waitingTimerWrap.classList.remove("hidden");
    if (dom.waitingTimerValue) dom.waitingTimerValue.textContent = `${Math.max(1, remainingSeconds)}s`;
    if (dom.waitingActions) dom.waitingActions.classList.add("hidden");
    matchmakingWaitExpired = false;
    return;
  }

  if (!matchmakingWaitExpired) {
    matchmakingWaitExpired = true;
  }
  const notificationsSupported = typeof window !== "undefined" && ("Notification" in window);
  const notificationsGranted = notificationsSupported && Notification.permission === "granted";
  const showNotifyAction = notificationsSupported && !notificationsGranted;
  const oddPlayingHint = matchmakingHintHasOddPlayingHumans && String(matchmakingHintMessage || "").trim();

  if ((Date.now() - matchmakingHintCheckedAtMs) > 10_000 || matchmakingHintRoomId !== roomKey) {
    void refreshMatchmakingHintIfNeeded();
  }

  if (matchmakingExtendedWaiting) {
    openWaitingModal(
      "Attente prolongee active",
      oddPlayingHint
        ? matchmakingHintMessage
        : (notificationsGranted
          ? "Tu restes en attente sans limite. Les notifications sont deja actives: on te previendra des qu'un joueur est disponible."
          : "Tu restes en attente sans limite. Tu peux quitter l'attente a tout moment.")
    );
    if (dom.waitingTimerWrap) dom.waitingTimerWrap.classList.add("hidden");
    if (dom.waitingActions) dom.waitingActions.classList.remove("hidden");
    setWaitingActionsVisibility({
      showHome: false,
      showRetry: false,
      showExtend: false,
      showStopExtended: true,
      showNotify: showNotifyAction,
      showWhatsapp: true,
      showContacts: true,
    });
    return;
  }

  openWaitingModal(
    "Aucun joueur disponible",
    oddPlayingHint
      ? matchmakingHintMessage
      : (notificationsGranted
        ? "Aucun joueur n'a rejoint dans les 15 secondes. Les notifications sont deja activees, nous te previendrons quand des joueurs seront disponibles."
        : "Aucun joueur n'a rejoint dans les 15 secondes. Active les notifications pour etre alerte quand des joueurs sont disponibles.")
  );
  if (dom.waitingTimerWrap) dom.waitingTimerWrap.classList.add("hidden");
  if (dom.waitingActions) dom.waitingActions.classList.remove("hidden");
  setWaitingActionsVisibility({
    showHome: true,
    showRetry: true,
    showExtend: true,
    showStopExtended: false,
    showNotify: showNotifyAction,
    showWhatsapp: true,
    showContacts: true,
  });
}

async function requestMatchmakingNotifications() {
  if (!dom.waitingCopy) return;
  if (typeof window === "undefined" || !("Notification" in window)) {
    dom.waitingCopy.textContent = "Les notifications ne sont pas supportees sur cet appareil.";
    if (dom.waitingNotifyBtn) dom.waitingNotifyBtn.classList.add("hidden");
    return;
  }
  try {
    if (Notification.permission === "granted") {
      dom.waitingCopy.textContent = "Notifications deja actives. Nous te prevenirons quand des joueurs sont disponibles.";
      if (dom.waitingNotifyBtn) dom.waitingNotifyBtn.classList.add("hidden");
      return;
    }
    const permission = await Notification.requestPermission();
    if (permission === "granted") {
      dom.waitingCopy.textContent = "Notifications activees. Tu seras alerte quand des joueurs seront disponibles.";
      if (dom.waitingNotifyBtn) dom.waitingNotifyBtn.classList.add("hidden");
      try {
        const note = new Notification("Morpion", {
          body: "Notifications activees. Nous te prevenirons quand des joueurs arrivent.",
          tag: "morpion-notify-enabled",
          icon: "./favicon.ico",
        });
        window.setTimeout(() => note.close(), 3000);
      } catch (_) {
      }
      return;
    }
    dom.waitingCopy.textContent = "Notifications bloquees. Autorise-les dans les reglages du navigateur.";
    if (dom.waitingNotifyBtn) dom.waitingNotifyBtn.classList.remove("hidden");
  } catch (_) {
    dom.waitingCopy.textContent = "Impossible d'activer les notifications pour le moment.";
    if (dom.waitingNotifyBtn) dom.waitingNotifyBtn.classList.remove("hidden");
  }
}

function openInviteModal(title = "", copy = "") {
  if (dom.inviteTitle) dom.inviteTitle.textContent = String(title || "Invitation disponible");
  if (dom.inviteCopy) dom.inviteCopy.textContent = String(copy || "");
  dom.inviteModal?.classList.remove("hidden");
}

function closeInviteModal() {
  dom.inviteModal?.classList.add("hidden");
}

function openRuleModal() {
  dom.ruleModal?.classList.remove("hidden");
}

function closeRuleModal() {
  dom.ruleModal?.classList.add("hidden");
}

function openResultModal(eyebrow = "", title = "", copy = "") {
  if (dom.resultEyebrow) dom.resultEyebrow.textContent = String(eyebrow || "Fin de partie");
  if (dom.resultTitle) dom.resultTitle.textContent = String(title || "Fin de partie");
  if (dom.resultCopy) dom.resultCopy.textContent = String(copy || "");
  dom.resultModal?.classList.remove("hidden");
}

function closeResultModal() {
  dom.resultModal?.classList.add("hidden");
}

function openQuitModal() {
  dom.quitModal?.classList.remove("hidden");
}

function closeQuitModal() {
  dom.quitModal?.classList.add("hidden");
}

function renderPlayerCards() {
  const selfSeat = currentSeatIndex >= 0 ? currentSeatIndex : 1;
  const opponentSeat = getOpponentSeat();
  const activeSeat = safeInt(currentRoomData?.currentPlayer, -1);

  if (dom.selfName) dom.selfName.textContent = getSelfName();
  if (dom.opponentName) dom.opponentName.textContent = getOpponentName();
  if (dom.opponentLabel) dom.opponentLabel.textContent = getOpponentLabel();

  if (dom.selfSymbol) {
    const symbol = seatSymbol(selfSeat);
    dom.selfSymbol.textContent = symbol;
    dom.selfSymbol.dataset.symbol = symbol;
  }
  if (dom.opponentSymbol) {
    const symbol = seatSymbol(opponentSeat);
    dom.opponentSymbol.textContent = symbol;
    dom.opponentSymbol.dataset.symbol = symbol;
  }

  dom.selfCard?.classList.toggle("is-active", activeSeat === selfSeat && currentRoomData?.status === "playing" && currentRoomData?.startRevealPending !== true);
  dom.opponentCard?.classList.toggle("is-active", activeSeat === opponentSeat && currentRoomData?.status === "playing" && currentRoomData?.startRevealPending !== true);
}

function renderTimers() {
  const now = Date.now();
  const activeSeat = safeInt(currentRoomData?.currentPlayer, -1);
  const deadlineMs = safeInt(currentRoomData?.turnDeadlineMs, 0);
  const remainingMs = currentRoomData?.status === "playing" && currentRoomData?.startRevealPending !== true && deadlineMs > 0
    ? Math.max(0, deadlineMs - now)
    : TURN_LIMIT_MS;
  const remainingRatio = Math.max(0, Math.min(1, remainingMs / TURN_LIMIT_MS));
  const remainingSeconds = Math.ceil(remainingMs / 1000);
  const selfSeat = currentSeatIndex >= 0 ? currentSeatIndex : 1;
  const opponentSeat = getOpponentSeat();

  const applyTimer = (seat, labelEl, fillEl, cardEl) => {
    const isActive = currentRoomData?.status === "playing" && currentRoomData?.startRevealPending !== true && activeSeat === seat;
    if (labelEl) labelEl.textContent = `${isActive ? remainingSeconds : TURN_LIMIT_SECONDS}s`;
    if (fillEl) fillEl.style.width = `${(isActive ? remainingRatio : 1) * 100}%`;
    if (cardEl) cardEl.classList.toggle("is-danger", isActive && remainingMs <= 5000);
  };

  applyTimer(opponentSeat, dom.opponentTimerLabel, dom.opponentTimerFill, dom.opponentCard);
  applyTimer(selfSeat, dom.selfTimerLabel, dom.selfTimerFill, dom.selfCard);

  if (currentRoomData?.status === "playing" && currentRoomData?.startRevealPending !== true && deadlineMs > 0 && now >= deadlineMs) {
    maybeRequestTurnTimeoutResolution();
  }

  renderMatchmakingWaitingModal();
}

function buildWinningSet() {
  const line = Array.isArray(currentGameState?.winningLine) ? currentGameState.winningLine : [];
  return new Set(line.map((item) => safeInt(item, -1)).filter((item) => item >= 0));
}

function getWinningLineCells() {
  const line = Array.isArray(currentGameState?.winningLine) ? currentGameState.winningLine : [];
  return line.map((item) => safeInt(item, -1)).filter((item) => item >= 0);
}

function getLastMoveCellIndex() {
  const cellIndex = safeInt(currentRoomData?.lastMove?.cellIndex, -1);
  return cellIndex >= 0 && cellIndex < 225 ? cellIndex : -1;
}

function hideRevealResultButton() {
  dom.revealResultBtn?.classList.add("hidden");
}

function showRevealResultButton() {
  dom.revealResultBtn?.classList.remove("hidden");
}

function hideWinLine() {
  if (!dom.winLine) return;
  dom.winLine.classList.add("hidden");
  dom.winLine.style.width = "0px";
}

function isLineEndState() {
  return String(currentRoomData?.status || "") === "ended"
    && String(currentRoomData?.endedReason || currentGameState?.endedReason || "").trim() === "line";
}

function clearEndStateDecorations() {
  stopEndResultTimer();
  pendingEndModalPayload = null;
  winLineVisible = false;
  hideRevealResultButton();
  hideWinLine();
}

function buildEndModalPayload() {
  const winnerSeat = safeInt(currentRoomData?.winnerSeat, -1);
  const endedReason = String(currentRoomData?.endedReason || currentGameState?.endedReason || "").trim();
  if (endedReason === "draw") {
    return {
      eyebrow: "Match nul",
      title: "Partie nulle",
      copy: "Le plateau est complet et aucun alignement de 5 n'a ete forme.",
    };
  }
  if (winnerSeat === currentSeatIndex) {
    const rewardDoes = safeInt(currentRoomData?.rewardAmountDoes, 0);
    const rewardLine = rewardDoes > 0 ? ` Tu remportes ${rewardDoes} Does.` : "";
    return {
      eyebrow: endedReason === "timeout" ? "Temps ecoule" : "Victoire",
      title: "Tu as gagne",
      copy: endedReason === "timeout"
        ? `Ton adversaire a laisse son chrono tomber a zero.${rewardLine}`
        : `Tu as aligne 5 symboles.${rewardLine}`,
    };
  }
  return {
    eyebrow: endedReason === "timeout" ? "Temps ecoule" : "Defaite",
    title: endedReason === "timeout" ? "Tu as perdu au temps" : "Tu as perdu",
    copy: endedReason === "timeout"
      ? "Ton chrono est arrive a zero."
      : "L'adversaire a aligne 5 symboles.",
  };
}

function openPendingEndModal() {
  if (!pendingEndModalPayload) return;
  openResultModal(
    pendingEndModalPayload.eyebrow,
    pendingEndModalPayload.title,
    pendingEndModalPayload.copy,
  );
}

function createBoard() {
  if (!dom.board) return;
  dom.board.style.setProperty("--board-size", "15");
  dom.board.innerHTML = "";
  for (let row = 0; row < 15; row += 1) {
    for (let col = 0; col < 15; col += 1) {
      const cellIndex = (row * 15) + col;
      const button = document.createElement("button");
      button.type = "button";
      button.className = "cell";
      button.dataset.index = String(cellIndex);
      button.setAttribute("role", "gridcell");
      button.setAttribute("aria-label", `Ligne ${row + 1}, colonne ${col + 1}`);
      dom.board.appendChild(button);
    }
  }
  const winLine = document.createElement("div");
  winLine.id = "morpionWinLine";
  winLine.className = "board-win-line hidden";
  winLine.setAttribute("aria-hidden", "true");
  dom.board.appendChild(winLine);
  dom.winLine = winLine;
}

function ensureCellSymbol(cell, symbol) {
  const existingSymbol = cell.querySelector(".cell__symbol");
  const nextClass = `cell__symbol cell__symbol--${symbol.toLowerCase()}`;
  if (existingSymbol) {
    if (existingSymbol.className !== nextClass) {
      existingSymbol.className = nextClass;
    }
    return;
  }
  const symbolEl = document.createElement("span");
  symbolEl.className = nextClass;
  symbolEl.setAttribute("aria-hidden", "true");
  cell.appendChild(symbolEl);
}

function renderBoard() {
  if (!dom.board) return;
  const board = currentBoard();
  const winningLineCells = getWinningLineCells();
  const winningSet = new Set(winningLineCells);
  const lastMoveCellIndex = getLastMoveCellIndex();
  Array.from(dom.board.querySelectorAll(".cell")).forEach((cell) => {
    const cellIndex = safeInt(cell.dataset.index, -1);
    const occupant = board[cellIndex];
    const occupied = occupant === 0 || occupant === 1;
    const symbol = occupied ? seatSymbol(occupant) : "";
    const isWinning = winningSet.has(cellIndex);
    const isLastMove = cellIndex === lastMoveCellIndex;
    cell.classList.toggle("is-occupied", occupied);
    cell.classList.toggle("is-win", isWinning);
    cell.classList.toggle("is-last-move", isLastMove);
    cell.disabled = occupied || !isMyTurn();
    if (occupied) {
      ensureCellSymbol(cell, symbol);
    } else {
      const existingSymbol = cell.querySelector(".cell__symbol");
      if (existingSymbol) existingSymbol.remove();
    }
  });
}

function renderWinningLine() {
  if (!dom.board || !dom.winLine) return;
  if (!winLineVisible || !isLineEndState()) {
    hideWinLine();
    return;
  }
  const winningLineCells = getWinningLineCells();
  if (winningLineCells.length < 2) {
    hideWinLine();
    return;
  }
  const firstCell = dom.board.querySelector(`.cell[data-index="${winningLineCells[0]}"]`);
  const lastCell = dom.board.querySelector(`.cell[data-index="${winningLineCells[winningLineCells.length - 1]}"]`);
  if (!(firstCell instanceof HTMLElement) || !(lastCell instanceof HTMLElement)) {
    hideWinLine();
    return;
  }
  const boardRect = dom.board.getBoundingClientRect();
  const firstRect = firstCell.getBoundingClientRect();
  const lastRect = lastCell.getBoundingClientRect();
  const startX = (firstRect.left - boardRect.left) + (firstRect.width / 2);
  const startY = (firstRect.top - boardRect.top) + (firstRect.height / 2);
  const endX = (lastRect.left - boardRect.left) + (lastRect.width / 2);
  const endY = (lastRect.top - boardRect.top) + (lastRect.height / 2);
  const deltaX = endX - startX;
  const deltaY = endY - startY;
  const length = Math.hypot(deltaX, deltaY);
  const angleDeg = Math.atan2(deltaY, deltaX) * (180 / Math.PI);
  dom.winLine.style.left = `${startX}px`;
  dom.winLine.style.top = `${startY - 4}px`;
  dom.winLine.style.width = `${length}px`;
  dom.winLine.style.transform = `rotate(${angleDeg}deg)`;
  dom.winLine.classList.remove("hidden");
}

function stopRoomSubscriptions() {
  try { roomUnsub?.(); } catch (_) {}
  try { stateUnsub?.(); } catch (_) {}
  roomUnsub = null;
  stateUnsub = null;
}

function stopClientSubscription() {
  try { clientUnsub?.(); } catch (_) {}
  clientUnsub = null;
}

function stopEndResultTimer() {
  if (endResultTimer) {
    window.clearTimeout(endResultTimer);
    endResultTimer = null;
  }
}

function stopPresencePing() {
  if (presenceTimer) {
    window.clearInterval(presenceTimer);
    presenceTimer = null;
  }
}

function stopSitePresencePing() {
  if (sitePresenceTimer) {
    window.clearInterval(sitePresenceTimer);
    sitePresenceTimer = null;
  }
}

function stopTurnTick() {
  if (turnTick) {
    window.clearInterval(turnTick);
    turnTick = null;
  }
}

function stopInvitePoll() {
  if (invitePollTimer) {
    window.clearInterval(invitePollTimer);
    invitePollTimer = null;
  }
}

function stopWaitingEnsureTimer() {
  if (waitingEnsureTimer) {
    window.clearTimeout(waitingEnsureTimer);
    waitingEnsureTimer = null;
  }
}

function stopBotTurnNudgeTimer() {
  if (botTurnNudgeTimer) {
    window.clearTimeout(botTurnNudgeTimer);
    botTurnNudgeTimer = null;
  }
}

function stopTurnTimeoutNudgeTimer() {
  if (turnTimeoutNudgeTimer) {
    window.clearTimeout(turnTimeoutNudgeTimer);
    turnTimeoutNudgeTimer = null;
  }
}

async function pingPresence() {
  if (!currentRoomId || leavingRoom || presencePingInFlight) return;
  presencePingInFlight = true;
  try {
    morpionDebug("pingPresence:start");
    await touchRoomPresenceMorpionSecure({ roomId: currentRoomId });
    morpionDebug("pingPresence:done");
  } catch (error) {
    console.warn("[MORPION] touchRoomPresence failed", error);
    morpionDebug("pingPresence:error", { message: error?.message || String(error) });
  } finally {
    presencePingInFlight = false;
  }
}

function startPresencePing() {
  stopPresencePing();
  presenceTimer = window.setInterval(() => {
    void pingPresence();
  }, PRESENCE_PING_MS);
}

function startSitePresencePing() {
  stopSitePresencePing();
  void touchClientSitePresence();
  sitePresenceTimer = window.setInterval(() => {
    if (document.visibilityState !== "visible") return;
    void touchClientSitePresence();
  }, SITE_PRESENCE_PING_MS);
}

function startTurnTicker() {
  stopTurnTick();
  turnTick = window.setInterval(renderTimers, 250);
}

async function pollActiveInvite() {
  if (!currentUser?.uid || invitePollInFlight) return;
  invitePollInFlight = true;
  try {
    const result = await getMyActiveMorpionInviteSecure({});
    const invite = result?.invitation && typeof result.invitation === "object" ? result.invitation : null;
    const invitationId = String(invite?.invitationId || "").trim();
    if (!invitationId) {
      activeInviteId = "";
      closeInviteModal();
      return;
    }
    activeInviteId = invitationId;
    const gameLabel = String(invite?.gameLabel || "domino").toUpperCase();
    const copy = String(invite?.message || "Il y a actuellement des joueurs disponibles. Veux-tu jouer maintenant ?");
    openInviteModal(`Des joueurs sont disponibles sur ${gameLabel}`, copy);
  } catch (error) {
    console.warn("[MORPION] invite poll failed", error);
  } finally {
    invitePollInFlight = false;
  }
}

function startInvitePoll() {
  stopInvitePoll();
  invitePollTimer = window.setInterval(() => {
    void pollActiveInvite();
  }, INVITE_POLL_MS);
}

async function respondInvite(action = "refuse") {
  const invitationId = String(activeInviteId || "").trim();
  if (!invitationId) {
    closeInviteModal();
    return;
  }
  try {
    await respondMorpionPlayInviteSecure({ invitationId, action });
  } catch (error) {
    console.warn("[MORPION] invite response failed", error);
  } finally {
    activeInviteId = "";
    closeInviteModal();
  }
  if (action === "accept") {
    window.location.href = "./index.html";
  }
}

async function maybeRequestTurnTimeoutResolution() {
  if (!currentRoomId || turnTimeoutRequestInFlight) return;
  const activeSeat = safeInt(currentRoomData?.currentPlayer, -1);
  const activeSeatUid = activeSeat >= 0 ? String((currentRoomData?.playerUids || [])[activeSeat] || "").trim() : "";
  if (!activeSeatUid) return;
  turnTimeoutRequestInFlight = true;
  try {
    morpionDebug("timeoutNudge:start", {
      deadlineMs: safeInt(currentRoomData?.turnDeadlineMs, 0),
      currentPlayer: safeInt(currentRoomData?.currentPlayer, -1),
    });
    await touchRoomPresenceMorpionSecure({ roomId: currentRoomId });
    morpionDebug("timeoutNudge:done");
  } catch (error) {
    console.warn("[MORPION] timeout nudge failed", error);
    morpionDebug("timeoutNudge:error", { message: error?.message || String(error) });
  } finally {
    turnTimeoutRequestInFlight = false;
  }
}

function scheduleTurnTimeoutNudge() {
  stopTurnTimeoutNudgeTimer();
  if (!currentRoomId || !currentRoomData) return;
  if (String(currentRoomData.status || "") !== "playing") return;
  if (currentRoomData.startRevealPending === true) return;

  const activeSeat = safeInt(currentRoomData?.currentPlayer, -1);
  const activeSeatUid = activeSeat >= 0 ? String((currentRoomData?.playerUids || [])[activeSeat] || "").trim() : "";
  if (!activeSeatUid) return;

  const deadlineMs = safeInt(currentRoomData?.turnDeadlineMs, 0);
  if (deadlineMs <= 0) return;

  const delayMs = Math.max(50, deadlineMs - Date.now() + 40);
  morpionDebug("scheduleTurnTimeoutNudge", {
    activeSeat,
    deadlineMs,
    delayMs,
  });

  turnTimeoutNudgeTimer = window.setTimeout(() => {
    morpionDebug("scheduleTurnTimeoutNudge:fire", {
      activeSeat,
      deadlineMs,
    });
    void maybeRequestTurnTimeoutResolution();
  }, delayMs);
}

function scheduleBotTurnNudge() {
  stopBotTurnNudgeTimer();
  if (!currentRoomId || !currentRoomData) return;
  if (String(currentRoomData.status || "") !== "playing") return;
  if (currentRoomData.startRevealPending === true) return;

  const activeSeat = safeInt(currentRoomData.currentPlayer, -1);
  if (activeSeat < 0 || activeSeat === currentSeatIndex) return;

  const lockedUntilMs = safeInt(currentRoomData.turnLockedUntilMs, 0);
  const delayMs = lockedUntilMs > 0
    ? Math.max(80, Math.min(5000, lockedUntilMs - Date.now() + 50))
    : 120;

  morpionDebug("scheduleBotTurnNudge", {
    activeSeat,
    lockedUntilMs,
    delayMs,
    currentPlayer: safeInt(currentRoomData?.currentPlayer, -1),
  });

  botTurnNudgeTimer = window.setTimeout(() => {
    morpionDebug("scheduleBotTurnNudge:fire");
    void pingPresence();
  }, delayMs);
}

async function maybeAckStartReveal() {
  if (!currentRoomId || startRevealAcked || currentRoomData?.startRevealPending !== true || currentRoomData?.status !== "playing") return;
  startRevealAcked = true;
  try {
    morpionDebug("ackStartReveal:start");
    await ackRoomStartSeenMorpionSecure({ roomId: currentRoomId });
    morpionDebug("ackStartReveal:done");
  } catch (error) {
    startRevealAcked = false;
    console.warn("[MORPION] ackRoomStartSeen failed", error);
    morpionDebug("ackStartReveal:error", { message: error?.message || String(error) });
  }
}

async function ensureRoomReady() {
  if (!currentRoomId || ensuringRoom || currentRoomData?.status !== "waiting") return;
  ensuringRoom = true;
  try {
    morpionDebug("ensureRoomReady:start", {
      waitingDeadlineMs: safeInt(currentRoomData?.waitingDeadlineMs, 0),
      humanCount: safeInt(currentRoomData?.humanCount, 0),
      botCount: safeInt(currentRoomData?.botCount, 0),
    });
    await ensureRoomReadyMorpionSecure({ roomId: currentRoomId });
    morpionDebug("ensureRoomReady:done");
  } catch (error) {
    console.warn("[MORPION] ensureRoomReady failed", error);
    morpionDebug("ensureRoomReady:error", { message: error?.message || String(error) });
  } finally {
    ensuringRoom = false;
    if (currentRoomId && currentRoomData?.status === "waiting") {
      scheduleEnsureRoomReady();
    }
  }
}

function scheduleEnsureRoomReady() {
  stopWaitingEnsureTimer();
  if (!currentRoomId || currentRoomData?.status !== "waiting") return;
  const waitingDeadlineMs = safeInt(currentRoomData?.waitingDeadlineMs, 0);
  const delayMs = waitingDeadlineMs > 0
    ? Math.max(350, Math.min(30000, waitingDeadlineMs - Date.now() + 80))
    : 800;
  morpionDebug("scheduleEnsureRoomReady", {
    waitingDeadlineMs,
    delayMs,
    humanCount: safeInt(currentRoomData?.humanCount, 0),
    botCount: safeInt(currentRoomData?.botCount, 0),
  });
  waitingEnsureTimer = window.setTimeout(() => {
    morpionDebug("scheduleEnsureRoomReady:fire");
    void ensureRoomReady();
  }, delayMs);
}

async function claimRewardIfNeeded() {
  if (rewardClaiming || rewardClaimed || !currentRoomId) return;
  const winnerSeat = safeInt(currentRoomData?.winnerSeat, -1);
  if (winnerSeat !== currentSeatIndex) return;
  rewardClaiming = true;
  try {
    const result = await claimWinRewardMorpionSecure({ roomId: currentRoomId });
    rewardClaimed = result?.rewardGranted === true || result?.reason === "already_paid" || result?.reason === "no_reward";
  } catch (error) {
    console.warn("[MORPION] claim reward failed", error);
  } finally {
    rewardClaiming = false;
  }
}

function handleEndedState() {
  closeWaitingModal();
  void claimRewardIfNeeded();
  const endKey = [
    currentRoomId,
    safeInt(currentRoomData?.lastActionSeq, 0),
    String(currentRoomData?.endedReason || currentGameState?.endedReason || "").trim(),
    safeInt(currentRoomData?.winnerSeat, -1),
  ].join(":");
  if (lastHandledEndKey === endKey) return;
  lastHandledEndKey = endKey;

  const winnerSeat = safeInt(currentRoomData?.winnerSeat, -1);
  const endedReason = String(currentRoomData?.endedReason || currentGameState?.endedReason || "").trim();
  pendingEndModalPayload = buildEndModalPayload();
  stopEndResultTimer();

  if (endedReason === "line") {
    hideRevealResultButton();
    hideWinLine();
    endResultTimer = window.setTimeout(() => {
      endResultTimer = null;
      winLineVisible = true;
      renderWinningLine();
      showRevealResultButton();
    }, 240);
    return;
  }

  winLineVisible = false;
  hideWinLine();
  hideRevealResultButton();
  endResultTimer = window.setTimeout(() => {
    endResultTimer = null;
    if (!pendingEndModalPayload) return;
    openPendingEndModal();
  }, winnerSeat === currentSeatIndex ? 140 : 140);
}

function renderFromRoom() {
  renderPlayerCards();
  renderWalletValue();
  renderTimers();
  renderBoard();
  renderWinningLine();
  scheduleBotTurnNudge();
  scheduleTurnTimeoutNudge();

  if (!currentRoomData) return;
  if (currentRoomData.status === "waiting") {
    clearEndStateDecorations();
    renderMatchmakingWaitingModal();
    return;
  }

  if (currentRoomData.status === "playing" && currentRoomData.startRevealPending === true) {
    clearEndStateDecorations();
    closeWaitingModal();
    void maybeAckStartReveal();
    return;
  }

  if (currentRoomData.status === "playing") {
    resetMatchmakingWaitState();
    clearEndStateDecorations();
    closeWaitingModal();
    return;
  }

  if (currentRoomData.status === "ended") {
    resetMatchmakingWaitState();
    handleEndedState();
  }
}

function subscribeToClient(uid) {
  stopClientSubscription();
  if (!uid) {
    currentDoesBalance = null;
    renderWalletValue();
    return;
  }

  clientUnsub = onSnapshot(doc(db, "clients", uid), (clientSnap) => {
    const clientData = clientSnap.exists() ? (clientSnap.data() || {}) : {};
    currentDoesBalance = Math.max(0, Number(clientData?.doesApprovedBalance) || 0);
    renderWalletValue();
  }, () => {
    currentDoesBalance = null;
    renderWalletValue();
  });
}

function subscribeToRoom(roomId) {
  stopRoomSubscriptions();
  stopBotTurnNudgeTimer();
  stopTurnTimeoutNudgeTimer();

  roomUnsub = onSnapshot(doc(db, MORPION_ROOMS, roomId), (roomSnap) => {
    if (!roomSnap.exists()) {
      morpionDebug("roomSnapshot:missing");
      return;
    }
    currentRoomData = roomSnap.data() || {};
    currentRoomId = roomId;
    currentSeatIndex = safeInt(currentRoomData?.seats?.[currentUser?.uid], currentSeatIndex);
    morpionDebug("roomSnapshot", {
      status: String(currentRoomData?.status || ""),
      startRevealPending: currentRoomData?.startRevealPending === true,
      currentPlayer: safeInt(currentRoomData?.currentPlayer, -1),
      humanCount: safeInt(currentRoomData?.humanCount, 0),
      botCount: safeInt(currentRoomData?.botCount, 0),
      turnLockedUntilMs: safeInt(currentRoomData?.turnLockedUntilMs, 0),
      turnDeadlineMs: safeInt(currentRoomData?.turnDeadlineMs, 0),
      playerUids: Array.isArray(currentRoomData?.playerUids) ? currentRoomData.playerUids : [],
      seats: currentRoomData?.seats || {},
    });
    renderFromRoom();
  }, (error) => {
    console.error("[MORPION] room snapshot failed", error);
    morpionDebug("roomSnapshot:error", { message: error?.message || String(error) });
  });

  stateUnsub = onSnapshot(doc(db, MORPION_GAME_STATES, roomId), (stateSnap) => {
    currentGameState = stateSnap.exists() ? (stateSnap.data() || {}) : null;
    morpionDebug("stateSnapshot", {
      exists: stateSnap.exists(),
      currentPlayer: safeInt(currentGameState?.currentPlayer, -1),
      moveCount: safeInt(currentGameState?.moveCount, 0),
      endedReason: String(currentGameState?.endedReason || ""),
      winnerSeat: safeInt(currentGameState?.winnerSeat, -1),
    });
    renderBoard();
    renderFromRoom();
  }, (error) => {
    console.error("[MORPION] state snapshot failed", error);
    morpionDebug("stateSnapshot:error", { message: error?.message || String(error) });
  });
}

async function submitCell(cellIndex) {
  if (!currentRoomId || actionSending || !isMyTurn()) return;
  actionSending = true;
  try {
    await submitActionMorpionSecure({
      roomId: currentRoomId,
      clientActionId: `morpion_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`,
      action: { cellIndex },
    });
  } catch (error) {
    console.warn("[MORPION] submit move failed", error);
  } finally {
    actionSending = false;
  }
}

async function leaveCurrentRoom() {
  if (!currentRoomId || leavingRoom) return;
  leavingRoom = true;
  try {
    await leaveRoomMorpionSecure({ roomId: currentRoomId });
  } catch (_) {
  } finally {
    leavingRoom = false;
  }
}

async function abandonAndNavigate(destination = "home") {
  if (currentRoomId) {
    markRoomAbandoned(currentRoomId);
  }
  await leaveCurrentRoom();
  if (destination === "replay") {
    window.location.href = `./morpion.html?stake=${selectedStakeDoes}`;
    return;
  }
  window.location.href = "./index.html";
}

async function joinOrResumeRoom() {
  if (joining || !currentUser?.uid) return;
  joining = true;
  rewardClaimed = false;
  startRevealAcked = false;
  lastHandledEndKey = "";
  clearEndStateDecorations();
  closeResultModal();
  closeQuitModal();
  openWaitingModal("Connexion en cours...", "Nous cherchons une salle de morpion disponible.");

  try {
    const result = await joinMatchmakingMorpionSecure({
      stakeDoes: selectedStakeDoes,
      excludeRoomIds: readAbandonedRoomIds(),
    });
    morpionDebug("joinResult", result || {});
    currentRoomId = String(result?.roomId || "").trim();
    currentSeatIndex = safeInt(result?.seatIndex, 0);
    clearRoomAbandoned(currentRoomId);
    subscribeToRoom(currentRoomId);
    startPresencePing();
    startTurnTicker();
    void pingPresence();
    if (String(result?.status || "") === "waiting") {
      startMatchmakingWaitCycle();
    }
  } catch (error) {
    console.error("[MORPION] join failed", error);
    const reasonCode = String(error?.reason || error?.code || "").trim().toLowerCase();
    if (reasonCode === "morpion-skilled-wait-human-only") {
      openResultModal(
        "Aucun joueur disponible",
        "Aucune salle humaine disponible",
        "Il n'y a personne qui joue au morpion actuellement. Reviens plus tard ou essaie un autre jeu."
      );
      return;
    }
    openResultModal("Connexion impossible", "Impossible de rejoindre une salle", error?.message || "Reessaie dans un instant.");
  } finally {
    joining = false;
  }
}

async function resumeFriendMorpionFromUrl() {
  const friendRoomId = getFriendMorpionRoomIdFromUrl();
  if (!currentUser?.uid || joining || currentRoomId || !friendRoomId) return;
  joining = true;
  rewardClaimed = false;
  startRevealAcked = false;
  lastHandledEndKey = "";
  clearEndStateDecorations();
  closeResultModal();
  closeQuitModal();
  openWaitingModal("Connexion en cours...", "Nous rejoignons la salle privee de morpion.");

  try {
    await refreshWallet();
    currentRoomId = friendRoomId;
    currentSeatIndex = safeInt(URL_PARAMS.get("seat"), 0);
    clearRoomAbandoned(currentRoomId);
    subscribeToRoom(currentRoomId);
    startPresencePing();
    startTurnTicker();
    void pingPresence();
  } catch (error) {
    console.error("[MORPION] resumeFriendMorpionFromUrl failed", error);
    openResultModal("Connexion impossible", "Impossible de rejoindre cette salle privee", error?.message || "Reessaie dans un instant.");
  } finally {
    joining = false;
  }
}

function joinOrResumeCurrentFlow() {
  if (isFriendMorpionFlowFromUrl()) {
    void resumeFriendMorpionFromUrl();
    return;
  }
  void joinOrResumeRoom();
}

function bindEvents() {
  dom.board?.addEventListener("click", (event) => {
    const target = event.target instanceof HTMLElement ? event.target.closest(".cell") : null;
    if (!(target instanceof HTMLElement)) return;
    const cellIndex = safeInt(target.dataset.index, -1);
    if (cellIndex < 0) return;
    void submitCell(cellIndex);
  });

  dom.quitBtn?.addEventListener("click", openQuitModal);
  dom.quitReplayBtn?.addEventListener("click", () => { void abandonAndNavigate("replay"); });
  dom.quitHomeBtn?.addEventListener("click", () => { void abandonAndNavigate("home"); });
  dom.quitCloseTargets.forEach((target) => target.addEventListener("click", closeQuitModal));
  dom.revealResultBtn?.addEventListener("click", openPendingEndModal);
  dom.resultReplayBtn?.addEventListener("click", () => { void abandonAndNavigate("replay"); });
  dom.resultHomeBtn?.addEventListener("click", () => { void abandonAndNavigate("home"); });
  dom.inviteAcceptBtn?.addEventListener("click", () => { void respondInvite("accept"); });
  dom.inviteRefuseBtn?.addEventListener("click", () => { void respondInvite("refuse"); });
  dom.waitingRetryBtn?.addEventListener("click", () => {
    startMatchmakingWaitCycle();
    renderMatchmakingWaitingModal();
  });
  dom.waitingExtendBtn?.addEventListener("click", () => {
    matchmakingExtendedWaiting = true;
    renderMatchmakingWaitingModal();
  });
  dom.waitingStopExtendBtn?.addEventListener("click", () => {
    matchmakingExtendedWaiting = false;
    void abandonAndNavigate("home");
  });
  dom.waitingHomeBtn?.addEventListener("click", () => {
    void abandonAndNavigate("home");
  });
  dom.waitingNotifyBtn?.addEventListener("click", () => {
    void requestMatchmakingNotifications();
  });
  dom.waitingWhatsappBtn?.addEventListener("click", () => {
    if (!whatsappPreferenceLoaded) {
      void loadWhatsappPreference(true);
    }
    openWhatsappModal();
  });
  dom.waitingContactsBtn?.addEventListener("click", () => {
    void openContactsModal();
  });
  dom.whatsappSaveBtn?.addEventListener("click", () => {
    void saveWhatsappPreference();
  });
  dom.whatsappRemoveBtn?.addEventListener("click", () => {
    void removeWhatsappPreference();
  });
  dom.whatsappCloseBtn?.addEventListener("click", closeWhatsappModal);
  dom.whatsappCloseTargets.forEach((target) => target.addEventListener("click", closeWhatsappModal));
  dom.contactsCloseBtn?.addEventListener("click", closeContactsModal);
  dom.contactsCloseTargets.forEach((target) => target.addEventListener("click", closeContactsModal));
  dom.contactsList?.addEventListener("click", (event) => {
    const target = event.target instanceof HTMLElement ? event.target.closest("[data-contact-action]") : null;
    if (!(target instanceof HTMLElement)) return;
    const action = String(target.dataset.contactAction || "").trim();
    if (action !== "copy") return;
    const number = String(target.dataset.contactNumber || "").trim();
    void copyToClipboard(number).then((copied) => {
      if (copied) {
        target.textContent = "Copie !";
        window.setTimeout(() => {
          target.textContent = "Copier";
        }, 1200);
      }
    });
  });
  dom.ruleContinueBtn?.addEventListener("click", () => {
    turnRuleAccepted = true;
    closeRuleModal();
    if (currentUser?.uid) {
      joinOrResumeCurrentFlow();
    }
  });

  window.addEventListener("pagehide", () => {
    stopSitePresencePing();
    if (currentRoomId) {
      markRoomAbandoned(currentRoomId);
    }
  });

  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") {
      void pingPresence();
      void touchClientSitePresence();
    }
  });

  window.addEventListener("resize", renderWinningLine);
}

function init() {
  createBoard();
  bindEvents();
  startTurnTicker();
  startInvitePoll();
  renderWalletValue();
  openRuleModal();
  onAuthStateChanged(auth, (user) => {
    currentUser = user || null;
    if (!currentUser) {
      stopSitePresencePing();
      stopInvitePoll();
      closeInviteModal();
      stopClientSubscription();
      window.location.href = "./auth.html";
      return;
    }
    startSitePresencePing();
    startInvitePoll();
    void pollActiveInvite();
    subscribeToClient(currentUser.uid);
    void loadWhatsappPreference(true);
    if (!turnRuleAccepted) {
      openRuleModal();
      return;
    }
    joinOrResumeCurrentFlow();
  });
}

init();
