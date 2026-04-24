import {
  auth,
  db,
  collection,
  doc,
  orderBy,
  onSnapshot,
  onAuthStateChanged,
  query,
} from "./firebase-init.js";
import {
  joinMatchmakingDameSecure,
  resumeFriendDameRoomSecure,
  ensureRoomReadyDameSecure,
  touchRoomPresenceDameSecure,
  leaveRoomDameSecure,
  submitActionDameSecure,
  recordDameMatchResultSecure,
  getPublicWhatsappModalConfigSecure,
  updateClientProfileSecure,
} from "./secure-functions.js";

const urlParams = new URLSearchParams(window.location.search);
const stakeDoes = Number.parseInt(String(urlParams.get("stake") || "0"), 10) || 0;
const roomMode = String(urlParams.get("roomMode") || "dame_2p").trim() || "dame_2p";
const friendDameRoomId = String(urlParams.get("friendDameRoomId") || "").trim();

const boardEl = document.getElementById("board");
const statusEl = document.getElementById("dameStatusBadge") || document.getElementById("botStatus");
const balanceEl = document.getElementById("dameBalanceBadge");
const opponentBadgeEl = document.getElementById("dameOpponentBadge");
const opponentNameEl = document.getElementById("dameOpponentName");
const searchOpponentBadgeEl = document.getElementById("dameSearchOpponentBadge");
const searchOpponentNameEl = document.getElementById("dameSearchOpponentName");
const leaveRoomBtn = document.getElementById("dameLeaveBtn");
const searchOverlayEl = document.getElementById("dameSearchOverlay");
const searchCopyEl = document.getElementById("dameSearchCopy");
const searchCountdownEl = document.getElementById("dameSearchCountdown");
const expiredOverlayEl = document.getElementById("dameSearchExpiredOverlay");
const expiredAgentValue = document.getElementById("dameExpiredAgentValue");
const expiredRetryBtn = document.getElementById("dameExpiredRetryBtn");
const expiredStayBtn = document.getElementById("dameExpiredStayBtn");
const expiredPhoneRevealBtn = document.getElementById("dameExpiredPhoneRevealBtn");
const expiredViewNumberBtn = document.getElementById("dameExpiredViewNumberBtn");
const expiredNotifyBtn = document.getElementById("dameExpiredNotifyBtn");
const expiredPhoneBox = document.getElementById("dameExpiredPhoneBox");
const expiredPhoneInput = document.getElementById("dameExpiredPhoneInput");
const expiredPhoneSaveBtn = document.getElementById("dameExpiredPhoneSaveBtn");
const CLIENTS_COLLECTION = "clients";

let currentUid = "";
let startedAtMs = 0;
let submittedResultKey = "";
let currentRoomId = "";
let currentRoomData = null;
let mySeatIndex = -1;
let roomUnsub = null;
let actionsUnsub = null;
let ensureTimer = null;
let presenceTimer = null;
let turnSyncTimer = null;
let balanceUnsub = null;
let searchTimer = null;
let currentWaitingDeadlineMs = 0;
let dameExpiredModalVisible = false;
let dameWhatsappConfigPromise = null;
let dameWhatsappAgentDigits = "";
let hasAuthUser = false;
let replayingRemoteAction = false;
let rebuildingBoardState = false;
let lastAppliedActionSeq = 0;
let isLeavingRoom = false;
let latestDameActionDocs = [];

function formatDoes(value) {
  const num = Number(value);
  return Number.isFinite(num) ? Math.trunc(num).toLocaleString("fr-FR") : "--";
}

function updateStatus(text) {
  if (!statusEl) return;
  statusEl.textContent = String(text || "");
}

function updateBalanceLabel(value) {
  if (!balanceEl) return;
  balanceEl.textContent = `Does: ${formatDoes(value)}`;
}

function getRoomHumanCount(roomData = {}) {
  if (Array.isArray(roomData?.playerUids)) {
    return roomData.playerUids.filter((uid) => String(uid || "").trim()).length;
  }
  return Math.max(0, Number(roomData?.humanCount || 0));
}

function getOpponentSeatIndex(roomData = {}) {
  if (Number.isFinite(Number(mySeatIndex)) && mySeatIndex >= 0) {
    return mySeatIndex === 0 ? 1 : mySeatIndex === 1 ? 0 : -1;
  }

  const playerUids = Array.isArray(roomData?.playerUids) ? roomData.playerUids : [];
  const myUidIndex = playerUids.findIndex((uid) => String(uid || "").trim() === currentUid);
  if (myUidIndex >= 0) {
    return myUidIndex === 0 ? 1 : myUidIndex === 1 ? 0 : -1;
  }

  const seats = roomData?.seats && typeof roomData.seats === "object" ? roomData.seats : {};
  const seatEntries = Object.entries(seats);
  const foundSeat = seatEntries.find(([, seat]) => Number.isFinite(Number(seat)) && Number(seat) >= 0 && Number(seat) < 2);
  if (foundSeat) {
    const seatIndex = Number(foundSeat[1]);
    return seatIndex === 0 ? 1 : 0;
  }

  return -1;
}

function getOpponentName(roomData = {}) {
  if (getRoomHumanCount(roomData) < 2) return "";

  const names = Array.isArray(roomData?.playerNames) ? roomData.playerNames : [];
  const playerUids = Array.isArray(roomData?.playerUids) ? roomData.playerUids : [];
  const opponentSeat = getOpponentSeatIndex(roomData);
  if (opponentSeat >= 0 && opponentSeat < names.length) {
    const explicitName = String(names[opponentSeat] || "").trim();
    if (explicitName) return explicitName;
  }

  const otherUid = playerUids.find((uid) => String(uid || "").trim() && String(uid).trim() !== currentUid);
  if (otherUid) {
    const seatIndex = playerUids.findIndex((uid) => String(uid || "").trim() === String(otherUid || "").trim());
    if (seatIndex >= 0 && seatIndex < names.length) {
      const fallbackName = String(names[seatIndex] || "").trim();
      if (fallbackName) return fallbackName;
    }
  }

  return "";
}

function updateOpponentUi(roomData = currentRoomData || {}) {
  const visible = getRoomHumanCount(roomData) >= 2;
  const opponentName = visible ? getOpponentName(roomData) : "";
  const displayName = opponentName || "Adversaire dans la salle";

  if (opponentBadgeEl) {
    opponentBadgeEl.classList.toggle("hidden", !visible);
  }
  if (searchOpponentBadgeEl) {
    searchOpponentBadgeEl.classList.toggle("hidden", !visible);
  }
  if (opponentNameEl) {
    opponentNameEl.textContent = displayName;
  }
  if (searchOpponentNameEl) {
    searchOpponentNameEl.textContent = displayName;
  }
}

function updateDameRoomUi(roomData = currentRoomData || {}) {
  const status = String(roomData?.status || "").trim().toLowerCase();
  const humanCount = getRoomHumanCount(roomData);
  const waitingDeadlineMs = Number(roomData?.waitingDeadlineMs || 0);
  const opponentName = getOpponentName(roomData);
  const opponentText = opponentName || "Adversaire dans la salle";

  updateOpponentUi(roomData);

  if (status === "playing") {
    if (mySeatIndex >= 0 && Number.isFinite(Number(roomData?.currentPlayer)) && Number(roomData.currentPlayer) === mySeatIndex) {
      updateStatus("Partie en cours. A toi de jouer.");
    } else {
      updateStatus("Partie en cours. En attente du coup adverse...");
    }
    return;
  }

  if (status === "waiting") {
    if (humanCount >= 2) {
      const waitingCopy = opponentName
        ? `Ton adversaire ${opponentName} est dans la salle. La partie démarre sous peu.`
        : "Ton adversaire est dans la salle. La partie démarre sous peu.";
      if (searchCopyEl) {
        searchCopyEl.textContent = waitingCopy;
      }
      if (searchCountdownEl) {
        searchCountdownEl.textContent = "Adversaire trouvé. Préparation de la partie...";
      }
      updateStatus(waitingCopy);
      return;
    }

    const remaining = waitingDeadlineMs > Date.now()
      ? Math.max(0, Math.ceil((waitingDeadlineMs - Date.now()) / 1000))
      : 0;
    const waitingCopy = remaining > 0
      ? `En attente de l'autre joueur... (${remaining}s)`
      : "Aucun joueur trouve. Retourne au menu et relance.";
    if (searchCopyEl) {
      searchCopyEl.textContent = "Nous préparons ta partie et cherchons ton adversaire.";
    }
    updateStatus(waitingCopy);
    return;
  }

  if (status === "ended" || status === "closed") {
    updateStatus("Partie terminee. Relance une nouvelle partie.");
    return;
  }

  if (humanCount >= 2) {
    updateStatus(opponentName
      ? `Salle active (${humanCount}/2). ${opponentText}.`
      : `Salle active (${humanCount}/2).`);
  } else {
    updateStatus(`Salle active (${humanCount}/2).`);
  }
}

function syncBoardTurnFromRoom(roomData = currentRoomData || {}) {
  if (!boardEl) return;
  const status = String(roomData?.status || "").trim().toLowerCase();
  const currentPlayer = Number(roomData?.currentPlayer);
  if (status !== "playing" || !Number.isFinite(currentPlayer)) return;

  const nextBoardTurn = Math.max(0, Math.trunc(currentPlayer) + 1);
  if (Number(boardEl.turn) !== nextBoardTurn) {
    boardEl.turn = nextBoardTurn;
  }
}

function syncBoardTurnFromAction(action = {}) {
  if (!boardEl) return;
  const seatIndex = Number.isFinite(Number(action?.seatIndex))
    ? Number(action.seatIndex)
    : Number.isFinite(Number(action?.piecePlayer))
      ? Number(action.piecePlayer)
      : -1;
  if (seatIndex < 0 || seatIndex > 1) return;
  const moverBoardTurn = Math.max(0, Math.trunc(seatIndex) + 1);
  if (Number(boardEl.turn) !== moverBoardTurn) {
    boardEl.turn = moverBoardTurn;
  }
}

function resetDameBoardState() {
  if (!boardEl?.data) return false;
  if (typeof boardEl.data.destroy === "function") {
    boardEl.data.destroy();
  }
  try {
    boardEl.turn = 0;
  } catch (_) {}
  if (typeof boardEl.data.create === "function") {
    boardEl.data.create();
  }
  return true;
}

function replayDameActions(actions = []) {
  const docs = Array.isArray(actions) ? actions : [];
  if (!boardEl?.data || String(currentRoomData?.status || "").trim().toLowerCase() !== "playing") {
    return;
  }

  rebuildingBoardState = true;
  replayingRemoteAction = true;
  try {
    resetDameBoardState();
    syncBoardTurnFromRoom(currentRoomData || {});

    let latestSeq = 0;
    let replayFailed = false;
    for (const docSnap of docs) {
      const data = docSnap?.data ? (docSnap.data() || {}) : (docSnap || {});
      const seq = Number(data?.seq || 0);
      if (!Number.isFinite(seq) || seq <= 0) continue;
      latestSeq = Math.max(latestSeq, seq);
      syncBoardTurnFromAction(data);
      const ok = applyActionToBoard(data);
      if (!ok) {
        console.warn("[DAME] replay action failed", { seq, action: data });
        replayFailed = true;
        break;
      }
    }

    if (!replayFailed) {
      lastAppliedActionSeq = latestSeq;
    }
    const boardTurnValue = Number.isFinite(Number(boardEl?.turn))
      ? Math.trunc(Number(boardEl.turn))
      : NaN;
    const boardCurrentPlayer = Number.isFinite(boardTurnValue)
      ? (boardTurnValue % 2 ^ 1)
      : Number.isFinite(Number(currentRoomData?.currentPlayer))
        ? Math.max(0, Math.trunc(Number(currentRoomData.currentPlayer)))
        : -1;
    updateDameRoomUi({
      ...(currentRoomData || {}),
      currentPlayer: boardCurrentPlayer,
    });
    setBoardInteractionEnabled(mySeatIndex >= 0 && boardCurrentPlayer === mySeatIndex);
  } finally {
    replayingRemoteAction = false;
    rebuildingBoardState = false;
  }
}

function computeDoesBalance(profile = {}) {
  const balance = Number(profile?.doesBalance);
  if (Number.isFinite(balance)) return Math.trunc(balance);
  const approved = Number(profile?.doesApprovedBalance);
  const provisional = Number(profile?.doesProvisionalBalance);
  if (Number.isFinite(approved) || Number.isFinite(provisional)) {
    return Math.trunc((Number.isFinite(approved) ? approved : 0) + (Number.isFinite(provisional) ? provisional : 0));
  }
  return 0;
}

function setBoardInteractionEnabled(enabled) {
  if (!boardEl) return;
  const on = enabled === true;
  boardEl.style.pointerEvents = on ? "auto" : "none";
  boardEl.style.opacity = on ? "1" : "0.72";
}

function stopSearchTimer() {
  if (searchTimer) {
    window.clearInterval(searchTimer);
    searchTimer = null;
  }
}

function getSearchSecondsLeft() {
  if (!currentWaitingDeadlineMs) return 0;
  const remainingMs = Math.max(0, currentWaitingDeadlineMs - Date.now());
  return Math.max(0, Math.ceil(remainingMs / 1000));
}

function renderSearchCountdown() {
  if (!searchCountdownEl) return;
  const remaining = getSearchSecondsLeft();
  if (remaining > 0) {
    searchCountdownEl.textContent = `${remaining} s restantes`;
  } else if (currentWaitingDeadlineMs > 0) {
    searchCountdownEl.textContent = "Temps écoulé. On passe à la modale d'aide.";
  } else {
    searchCountdownEl.textContent = "Préparation de la partie...";
  }
}

function openSearchModal(message = "", deadlineMs = 0) {
  currentWaitingDeadlineMs = Number.isFinite(Number(deadlineMs)) ? Math.max(0, Number(deadlineMs)) : 0;
  if (searchCopyEl && message) {
    searchCopyEl.textContent = String(message);
  }
  if (expiredOverlayEl) {
    expiredOverlayEl.classList.add("hidden");
  }
  dameExpiredModalVisible = false;
  if (searchOverlayEl) {
    searchOverlayEl.classList.remove("hidden");
  }
  stopSearchTimer();
  renderSearchCountdown();
  searchTimer = window.setInterval(() => {
    renderSearchCountdown();
    if (currentWaitingDeadlineMs > 0 && Date.now() >= currentWaitingDeadlineMs) {
      stopSearchTimer();
      if (currentRoomData?.status === "waiting" && !dameExpiredModalVisible) {
        openExpiredModal();
      }
    }
  }, 1000);
}

function closeSearchModal() {
  if (searchOverlayEl) {
    searchOverlayEl.classList.add("hidden");
  }
  currentWaitingDeadlineMs = 0;
  stopSearchTimer();
}

function openExpiredModal() {
  if (searchOverlayEl) {
    searchOverlayEl.classList.add("hidden");
  }
  stopSearchTimer();
  currentWaitingDeadlineMs = 0;
  if (expiredOverlayEl) {
    expiredOverlayEl.classList.remove("hidden");
  }
  dameExpiredModalVisible = true;
  void loadDameWhatsappConfig();
}

function closeExpiredModal() {
  if (expiredOverlayEl) {
    expiredOverlayEl.classList.add("hidden");
  }
  if (expiredPhoneBox) {
    expiredPhoneBox.classList.remove("visible");
  }
  dameExpiredModalVisible = false;
}

function formatWhatsappDisplay(digits = "") {
  const clean = String(digits || "").replace(/\D/g, "");
  return clean ? `+${clean}` : "";
}

async function loadDameWhatsappConfig() {
  if (dameWhatsappConfigPromise) return dameWhatsappConfigPromise;
  dameWhatsappConfigPromise = getPublicWhatsappModalConfigSecure({})
    .then((result) => {
      const contacts = result?.contacts && typeof result.contacts === "object" ? result.contacts : {};
      dameWhatsappAgentDigits = String(contacts.championnat_mopyon || contacts.agent_deposit || contacts.support_default || "").replace(/\D/g, "");
      if (expiredAgentValue) {
        expiredAgentValue.textContent = dameWhatsappAgentDigits
          ? `Numéro WhatsApp agent: ${formatWhatsappDisplay(dameWhatsappAgentDigits)}`
          : "Numéro WhatsApp indisponible pour le moment.";
      }
      return result;
    })
    .catch((error) => {
      console.warn("[DAME] whatsapp config load failed", error);
      dameWhatsappAgentDigits = "";
      if (expiredAgentValue) {
        expiredAgentValue.textContent = "Numéro WhatsApp indisponible pour le moment.";
      }
      return null;
    })
    .finally(() => {
      dameWhatsappConfigPromise = null;
    });
  return dameWhatsappConfigPromise;
}

async function saveDameWaitlistInfo({ phone = "", notify = false } = {}) {
  if (!currentUid) {
    throw new Error("Connexion requise.");
  }
  const cleanPhone = String(phone || "").replace(/\D/g, "");
  const payload = {
    phone: cleanPhone || undefined,
  };
  if (cleanPhone) {
    payload.dameWhatsappNumber = cleanPhone;
  }
  if (notify) {
    payload.dameWaitingNotificationRequested = true;
    payload.dameWaitingNotificationRequestedAtMs = Date.now();
    payload.dameWhatsappVisible = true;
  }
  return updateClientProfileSecure(payload);
}

async function restartDameSearch({ fresh = true } = {}) {
  closeExpiredModal();
  closeSearchModal();
  if (fresh) {
    try {
      if (currentRoomId) {
        await leaveRoomDameSecure({ roomId: currentRoomId, reason: "search_restart" }).catch(() => null);
      }
    } catch (_) {}
    currentRoomId = "";
    currentRoomData = null;
    startedAtMs = 0;
    submittedResultKey = "";
    mySeatIndex = -1;
    stopRoomSync();
    await bootRoomFlow();
    return;
  }
  currentWaitingDeadlineMs = Date.now() + 15000;
  openSearchModal("Nous recherchons un adversaire pour ta partie de dame.", currentWaitingDeadlineMs);
}

async function leaveCurrentDameRoom({ redirect = true } = {}) {
  const roomId = String(currentRoomId || "").trim();
  if (!roomId) {
    if (redirect) {
      window.location.href = "./index.html";
    }
    return;
  }

  if (isLeavingRoom) return;
  isLeavingRoom = true;
  setBoardInteractionEnabled(false);
  updateStatus("Quitte la partie...");

  try {
    await leaveRoomDameSecure({ roomId, reason: "manual_quit" });
  } catch (error) {
    console.warn("[DAME] leave room failed", error);
    if (statusEl) {
      statusEl.textContent = error?.message || "Impossible de quitter la salle pour le moment.";
    }
  } finally {
    stopRoomSync();
    closeSearchModal();
    closeExpiredModal();
    currentRoomId = "";
    currentRoomData = null;
    mySeatIndex = -1;
    startedAtMs = 0;
    submittedResultKey = "";
    if (redirect) {
      window.location.href = "./index.html";
    }
  }
}

async function refreshBalance() {
  if (!currentUid) {
    if (balanceUnsub) {
      balanceUnsub();
      balanceUnsub = null;
    }
    updateBalanceLabel("--");
    return;
  }

  if (balanceUnsub) {
    balanceUnsub();
    balanceUnsub = null;
  }

  const walletRef = doc(db, CLIENTS_COLLECTION, currentUid);
  balanceUnsub = onSnapshot(walletRef, (snap) => {
    if (!snap.exists()) {
      updateBalanceLabel(0);
      return;
    }
    const profile = snap.data() || {};
    updateBalanceLabel(computeDoesBalance(profile));
  }, () => {
    updateBalanceLabel("--");
  });
}

function stopRoomSync() {
  if (roomUnsub) {
    roomUnsub();
    roomUnsub = null;
  }
  if (actionsUnsub) {
    actionsUnsub();
    actionsUnsub = null;
  }
  if (ensureTimer) {
    window.clearInterval(ensureTimer);
    ensureTimer = null;
  }
  if (presenceTimer) {
    window.clearInterval(presenceTimer);
    presenceTimer = null;
  }
  if (turnSyncTimer) {
    window.clearInterval(turnSyncTimer);
    turnSyncTimer = null;
  }
  if (balanceUnsub) {
    balanceUnsub();
    balanceUnsub = null;
  }
  latestDameActionDocs = [];
  lastAppliedActionSeq = 0;
}

function getFieldAt(line, column) {
  if (!boardEl) return null;
  return boardEl.querySelector(`div.line${Number(line)}.column${Number(column)}`);
}

function applyActionToBoard(action = {}) {
  const from = action?.from || {};
  const to = action?.to || {};
  const fromField = getFieldAt(from.line, from.column);
  const toField = getFieldAt(to.line, to.column);
  if (!fromField || !toField) return false;

  const piece = fromField.querySelector("a.piece");
  if (!piece) return false;

  piece.dispatchEvent(new MouseEvent("click", { view: window, bubbles: true, cancelable: true }));
  const mask = toField.querySelector("a.move");
  if (!mask) return false;
  mask.dispatchEvent(new MouseEvent("click", { view: window, bubbles: true, cancelable: true }));
  return true;
}

function startActionsSync() {
  if (!currentRoomId || !currentUid) return;
  if (actionsUnsub) {
    actionsUnsub();
    actionsUnsub = null;
  }
  lastAppliedActionSeq = 0;

  const actionsQuery = query(
    collection(db, "dameRooms", currentRoomId, "actions"),
    orderBy("seq", "asc")
  );
  actionsUnsub = onSnapshot(actionsQuery, (snap) => {
    latestDameActionDocs = snap.docs || [];
    if (String(currentRoomData?.status || "").trim().toLowerCase() === "playing") {
      replayDameActions(latestDameActionDocs);
    }
  });
}

async function syncRoomReady() {
  const roomId = String(currentRoomId || "").trim();
  if (!roomId || !currentUid) return;
  try {
    const result = await ensureRoomReadyDameSecure({ roomId });
    if (result?.status === "playing") {
      closeSearchModal();
      closeExpiredModal();
      setBoardInteractionEnabled(true);
      updateStatus("Partie en cours. A toi de jouer.");
    } else if (result?.status === "waiting") {
      const deadlineMs = Number(result?.waitingDeadlineMs || 0);
      if (deadlineMs > 0) {
        currentWaitingDeadlineMs = deadlineMs;
        renderSearchCountdown();
        if (result?.expired === true || (deadlineMs > 0 && Date.now() >= deadlineMs)) {
          openExpiredModal();
        }
      }
    } else if (result?.status === "closed" || result?.expired === true) {
      openExpiredModal();
    }
  } catch (_) {}
}

async function touchPresence() {
  const roomId = String(currentRoomId || "").trim();
  if (!roomId || !currentUid) return;
  try {
    await touchRoomPresenceDameSecure({ roomId });
  } catch (_) {}
}

function startRoomSync() {
  if (!currentRoomId || !currentUid) return;
  stopRoomSync();

  const roomRef = doc(db, "dameRooms", currentRoomId);
  roomUnsub = onSnapshot(roomRef, (snap) => {
    if (!snap.exists()) {
      updateStatus("Salle introuvable. Relance une nouvelle partie.");
      setBoardInteractionEnabled(false);
      openExpiredModal();
      closeSearchModal();
      return;
    }
    const previousRoomData = currentRoomData;
    currentRoomData = snap.data() || {};
    const seats = currentRoomData?.seats && typeof currentRoomData.seats === "object" ? currentRoomData.seats : {};
    mySeatIndex = Number.isFinite(Number(seats?.[currentUid])) ? Number(seats[currentUid]) : -1;
    const status = String(currentRoomData?.status || "").trim().toLowerCase();
    const humanCount = Array.isArray(currentRoomData?.playerUids)
      ? currentRoomData.playerUids.filter(Boolean).length
      : Number(currentRoomData?.humanCount || 0);
    const waitingDeadlineMs = Number(currentRoomData?.waitingDeadlineMs || 0);
    const currentPlayer = Number.isFinite(Number(currentRoomData?.currentPlayer))
      ? Number(currentRoomData.currentPlayer)
      : -1;
    const nextLastActionSeq = Number.isFinite(Number(currentRoomData?.lastActionSeq))
      ? Number(currentRoomData.lastActionSeq)
      : 0;
    const previousStatus = String(previousRoomData?.status || "").trim().toLowerCase();
    const shouldReplayActions = status === "playing" && (previousStatus !== "playing" || nextLastActionSeq !== lastAppliedActionSeq);

    syncBoardTurnFromRoom(currentRoomData);
    updateDameRoomUi(currentRoomData);

    if (status === "playing") {
      if (shouldReplayActions) {
        replayDameActions(latestDameActionDocs);
      }
      closeSearchModal();
      closeExpiredModal();
      if (startedAtMs <= 0) {
        startedAtMs = Number(currentRoomData?.startedAtMs || Date.now()) || Date.now();
      }
      const roomTurnForUi = shouldReplayActions && Number.isFinite(Number(boardEl?.turn))
        ? (Math.trunc(Number(boardEl.turn)) % 2 ^ 1)
        : currentPlayer;
      const myTurn = mySeatIndex >= 0 && roomTurnForUi === mySeatIndex;
      setBoardInteractionEnabled(myTurn);
      updateStatus(myTurn ? "Partie en cours. A toi de jouer." : "Partie en cours. En attente du coup adverse...");
      return;
    }

    setBoardInteractionEnabled(false);
    if (status === "waiting") {
      const opponentName = getOpponentName(currentRoomData);
      openSearchModal(
        humanCount >= 2 && opponentName
          ? `Ton adversaire ${opponentName} est dans la salle. La partie démarre sous peu.`
          : "Nous recherchons un adversaire pour ta partie de dame.",
        waitingDeadlineMs > 0 ? waitingDeadlineMs : Date.now() + 15000
      );
      if (waitingDeadlineMs > Date.now()) {
        const remaining = Math.max(0, Math.ceil((waitingDeadlineMs - Date.now()) / 1000));
        updateStatus(humanCount >= 2 && opponentName
          ? `Ton adversaire ${opponentName} est dans la salle. La partie démarre sous peu.`
          : `En attente de l'autre joueur... (${remaining}s)`);
      } else {
        updateStatus("Aucun joueur trouve. Retourne au menu et relance.");
        openExpiredModal();
      }
      return;
    }

    if (status === "ended" || status === "closed") {
      closeSearchModal();
      openExpiredModal();
      updateStatus("Partie terminee. Relance une nouvelle partie.");
      return;
    }

    closeSearchModal();
    updateStatus(`Salle active (${humanCount}/2).`);
  });
  startActionsSync();

  ensureTimer = window.setInterval(() => {
    void syncRoomReady();
  }, 2000);
  presenceTimer = window.setInterval(() => {
    void touchPresence();
  }, 20000);
  turnSyncTimer = window.setInterval(() => {
    if (String(currentRoomData?.status || "").trim().toLowerCase() !== "playing") return;
    syncBoardTurnFromRoom(currentRoomData);
  }, 750);

  void syncRoomReady();
  void touchPresence();
}

async function bootRoomFlow() {
  if (!hasAuthUser || !currentUid) return;
  try {
    let result = null;
    if (friendDameRoomId) {
      result = await resumeFriendDameRoomSecure({ roomId: friendDameRoomId });
    } else {
      result = await joinMatchmakingDameSecure({ stakeDoes: Math.max(0, stakeDoes) });
    }
    currentRoomId = String(result?.roomId || "").trim();
    if (!currentRoomId) {
      updateStatus("Impossible de rejoindre la salle dame.");
      setBoardInteractionEnabled(false);
      return;
    }
    openSearchModal(
      "Nous recherchons un adversaire pour ta partie de dame.",
      Number(result?.waitingDeadlineMs || Date.now() + 15000)
    );
    updateStatus("Recherche de joueur en cours...");
    setBoardInteractionEnabled(false);
    startRoomSync();
  } catch (error) {
    console.warn("[DAME] room flow error", error);
    updateStatus("Erreur de connexion salle. Reessaie depuis l'accueil.");
    setBoardInteractionEnabled(false);
    closeSearchModal();
  }
}

onAuthStateChanged(auth, (user) => {
  currentUid = String(user?.uid || "").trim();
  hasAuthUser = !!currentUid;
  void refreshBalance();
  if (currentUid) {
    updateStatus("Connexion salle dame...");
    void bootRoomFlow();
  } else {
    updateStatus("Connecte-toi pour jouer en ligne.");
    setBoardInteractionEnabled(false);
    closeSearchModal();
  }
});

expiredRetryBtn?.addEventListener("click", () => {
  void restartDameSearch({ fresh: true });
});

expiredStayBtn?.addEventListener("click", () => {
  void restartDameSearch({ fresh: false });
});

expiredPhoneRevealBtn?.addEventListener("click", () => {
  if (expiredPhoneBox) {
    expiredPhoneBox.classList.add("visible");
  }
  expiredPhoneInput?.focus();
});

expiredViewNumberBtn?.addEventListener("click", async () => {
  await loadDameWhatsappConfig();
  if (dameWhatsappAgentDigits && expiredAgentValue) {
    expiredAgentValue.textContent = `Numéro WhatsApp agent: ${formatWhatsappDisplay(dameWhatsappAgentDigits)}`;
  }
});

expiredPhoneSaveBtn?.addEventListener("click", async () => {
  const phone = String(expiredPhoneInput?.value || "").trim();
  if (!phone) {
    if (expiredAgentValue) {
      expiredAgentValue.textContent = "Entre un numéro WhatsApp avant d'enregistrer.";
    }
    return;
  }
  try {
    await saveDameWaitlistInfo({ phone, notify: false });
    if (expiredAgentValue) {
      expiredAgentValue.textContent = "Numéro enregistré.";
    }
  } catch (error) {
    console.warn("[DAME] save phone failed", error);
    if (expiredAgentValue) {
      expiredAgentValue.textContent = error?.message || "Impossible d'enregistrer le numéro pour le moment.";
    }
  }
});

expiredNotifyBtn?.addEventListener("click", async () => {
  try {
    const phone = String(expiredPhoneInput?.value || "").trim();
    await saveDameWaitlistInfo({ phone, notify: true });
    if (expiredAgentValue) {
      expiredAgentValue.textContent = "Ta demande de notification a été enregistrée.";
    }
  } catch (error) {
    console.warn("[DAME] notify request failed", error);
    if (expiredAgentValue) {
      expiredAgentValue.textContent = error?.message || "Impossible d'enregistrer la notification pour le moment.";
    }
  }
});

expiredOverlayEl?.addEventListener("click", (event) => {
  if (event.target === expiredOverlayEl) {
    closeExpiredModal();
  }
});

document.getElementById("dameSearchExpiredOverlay")?.querySelector(".expired-card")?.addEventListener("click", (event) => {
  event.stopPropagation();
});

void loadDameWhatsappConfig();

leaveRoomBtn?.addEventListener("click", () => {
  const opponentName = getOpponentName(currentRoomData || {});
  const confirmed = window.confirm(
    opponentName
      ? `Veux-tu vraiment quitter la partie de dame contre ${opponentName} ?`
      : "Veux-tu vraiment quitter la partie de dame ?"
  );
  if (!confirmed) return;
  void leaveCurrentDameRoom({ redirect: true });
});

boardEl?.addEventListener("piecemove", () => {
  if (replayingRemoteAction || rebuildingBoardState) return;
  if (startedAtMs <= 0) {
    startedAtMs = Date.now();
  }
});

boardEl?.addEventListener("piecemove", async (event) => {
  if (replayingRemoteAction || rebuildingBoardState) return;
  if (!currentUid || !currentRoomId) return;
  const status = String(currentRoomData?.status || "").trim().toLowerCase();
  if (status !== "playing") return;

  const piecePlayer = Number(event?.detail?.piece?.data?.player?.());
  if (!Number.isFinite(piecePlayer) || piecePlayer < 0 || piecePlayer > 1) return;
  if (mySeatIndex >= 0 && piecePlayer !== mySeatIndex) return;

  const fromField = event?.detail?.fromField;
  const toField = event?.detail?.toField;
  const fromLine = Number(fromField?.data?.line);
  const fromColumn = Number(fromField?.data?.column);
  const toLine = Number(toField?.data?.line);
  const toColumn = Number(toField?.data?.column);
  if (![fromLine, fromColumn, toLine, toColumn].every((n) => Number.isFinite(n))) return;

  try {
    const result = await submitActionDameSecure({
      roomId: currentRoomId,
      seatIndex: mySeatIndex,
      piecePlayer,
      from: { line: fromLine, column: fromColumn },
      to: { line: toLine, column: toColumn },
      changeTurn: event?.detail?.changeTurn !== false,
    });
    const seq = Number(result?.seq || 0);
    const nextPlayer = Number(result?.currentPlayer);
    if (Number.isFinite(nextPlayer)) {
      currentRoomData = {
        ...(currentRoomData || {}),
        currentPlayer: nextPlayer,
      };
      syncBoardTurnFromRoom(currentRoomData);
      updateDameRoomUi(currentRoomData);
      setBoardInteractionEnabled(mySeatIndex >= 0 && nextPlayer === mySeatIndex);
    }
    if (Number.isFinite(seq) && seq > 0) {
      lastAppliedActionSeq = Math.max(lastAppliedActionSeq, seq);
    }
  } catch (error) {
    console.warn("[DAME] submit action failed", error);
    updateStatus("Sync coup echoue. Verifie la connexion puis rejoue.");
  }
});

boardEl?.addEventListener("gameover", async (event) => {
  if (replayingRemoteAction || rebuildingBoardState) return;
  const winnerSeat = Number(event?.detail?.winner);
  const endedAtMs = Date.now();
  const dedupeKey = `${winnerSeat}:${endedAtMs}`;
  if (submittedResultKey === dedupeKey) return;
  submittedResultKey = dedupeKey;

  if (!currentUid) {
    updateStatus("Partie terminee. Connecte-toi pour enregistrer ce resultat.");
    return;
  }

  const matchId = `dame_${currentUid}_${endedAtMs}`;
  try {
    await recordDameMatchResultSecure({
      matchId,
      roomId: currentRoomId,
      roomMode,
      stakeDoes,
      winnerSeat: Number.isFinite(winnerSeat) ? winnerSeat : -1,
      winnerType: "human",
      startedAtMs: startedAtMs > 0 ? startedAtMs : 0,
      endedAtMs,
      endedReason: "gameover",
    });
    updateStatus("Partie terminee. Resultat dame enregistre.");
  } catch (error) {
    console.warn("[DAME] echec enregistrement resultat", error);
    updateStatus("Partie terminee. Echec enregistrement resultat.");
  }
});

window.addEventListener("beforeunload", () => {
  const roomId = String(currentRoomId || "").trim();
  if (!roomId || !currentUid || isLeavingRoom) return;
  leaveRoomDameSecure({ roomId, reason: "page_unload" }).catch(() => null);
});
