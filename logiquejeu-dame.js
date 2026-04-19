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
} from "./secure-functions.js";

const urlParams = new URLSearchParams(window.location.search);
const stakeDoes = Number.parseInt(String(urlParams.get("stake") || "0"), 10) || 0;
const roomMode = String(urlParams.get("roomMode") || "dame_2p").trim() || "dame_2p";
const friendDameRoomId = String(urlParams.get("friendDameRoomId") || "").trim();
const DAME_PRODUCTION_LOCKED = true;

const boardEl = document.getElementById("board");
const statusEl = document.getElementById("botStatus");

if (DAME_PRODUCTION_LOCKED) {
  if (boardEl) {
    boardEl.style.pointerEvents = "none";
    boardEl.style.opacity = "0.45";
  }
  if (statusEl) {
    statusEl.textContent = "Jeu de dame en production. Reviens plus tard.";
  }
}

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
let hasAuthUser = false;
let replayingRemoteAction = false;
let lastAppliedActionSeq = 0;
const localSubmittedActionSeqs = new Set();

function updateStatus(text) {
  if (!statusEl) return;
  statusEl.textContent = String(text || "");
}

function setBoardInteractionEnabled(enabled) {
  if (!boardEl) return;
  const on = enabled === true;
  boardEl.style.pointerEvents = on ? "auto" : "none";
  boardEl.style.opacity = on ? "1" : "0.72";
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
  localSubmittedActionSeqs.clear();

  const actionsQuery = query(
    collection(db, "dameRooms", currentRoomId, "actions"),
    orderBy("seq", "asc")
  );
  actionsUnsub = onSnapshot(actionsQuery, (snap) => {
    const docs = snap.docs || [];
    for (const docSnap of docs) {
      const data = docSnap.data() || {};
      const seq = Number(data.seq || 0);
      if (!Number.isFinite(seq) || seq <= 0 || seq <= lastAppliedActionSeq) continue;

      const alreadyAppliedLocally = localSubmittedActionSeqs.has(seq);
      if (!alreadyAppliedLocally) {
        replayingRemoteAction = true;
        const ok = applyActionToBoard(data);
        replayingRemoteAction = false;
        if (!ok) {
          console.warn("[DAME] replay action failed", { seq, action: data });
        }
      } else {
        localSubmittedActionSeqs.delete(seq);
      }
      lastAppliedActionSeq = seq;
    }
  });
}

async function syncRoomReady() {
  const roomId = String(currentRoomId || "").trim();
  if (!roomId || !currentUid) return;
  try {
    const result = await ensureRoomReadyDameSecure({ roomId });
    if (result?.status === "playing") {
      setBoardInteractionEnabled(true);
      updateStatus("Partie en cours. A toi de jouer.");
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
      return;
    }
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

    if (status === "playing") {
      if (startedAtMs <= 0) {
        startedAtMs = Number(currentRoomData?.startedAtMs || Date.now()) || Date.now();
      }
      const myTurn = mySeatIndex >= 0 && currentPlayer === mySeatIndex;
      setBoardInteractionEnabled(myTurn);
      updateStatus(myTurn ? "Partie en cours. A toi de jouer." : "Partie en cours. En attente du coup adverse...");
      return;
    }

    setBoardInteractionEnabled(false);
    if (status === "waiting") {
      if (waitingDeadlineMs > Date.now()) {
        const remaining = Math.max(0, Math.ceil((waitingDeadlineMs - Date.now()) / 1000));
        updateStatus(`En attente de l'autre joueur... (${remaining}s)`);
      } else {
        updateStatus("Aucun joueur trouve. Retourne au menu et relance.");
      }
      return;
    }

    if (status === "ended" || status === "closed") {
      updateStatus("Partie terminee. Relance une nouvelle partie.");
      return;
    }

    updateStatus(`Salle active (${humanCount}/2).`);
  });
  startActionsSync();

  ensureTimer = window.setInterval(() => {
    void syncRoomReady();
  }, 2000);
  presenceTimer = window.setInterval(() => {
    void touchPresence();
  }, 20000);

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
    updateStatus("Recherche de joueur en cours...");
    setBoardInteractionEnabled(false);
    startRoomSync();
  } catch (error) {
    console.warn("[DAME] room flow error", error);
    updateStatus("Erreur de connexion salle. Reessaie depuis l'accueil.");
    setBoardInteractionEnabled(false);
  }
}

onAuthStateChanged(auth, (user) => {
  if (DAME_PRODUCTION_LOCKED) {
    updateStatus("Jeu de dame en production. Reviens plus tard.");
    setBoardInteractionEnabled(false);
    stopRoomSync();
    return;
  }
  currentUid = String(user?.uid || "").trim();
  hasAuthUser = !!currentUid;
  if (currentUid) {
    updateStatus("Connexion salle dame...");
    void bootRoomFlow();
  } else {
    updateStatus("Connecte-toi pour jouer en ligne.");
    setBoardInteractionEnabled(false);
  }
});

boardEl?.addEventListener("piecemove", () => {
  if (replayingRemoteAction) return;
  if (startedAtMs <= 0) {
    startedAtMs = Date.now();
  }
});

boardEl?.addEventListener("piecemove", async (event) => {
  if (replayingRemoteAction) return;
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
    if (Number.isFinite(seq) && seq > 0) {
      localSubmittedActionSeqs.add(seq);
    }
  } catch (error) {
    console.warn("[DAME] submit action failed", error);
    updateStatus("Sync coup echoue. Verifie la connexion puis rejoue.");
  }
});

boardEl?.addEventListener("gameover", async (event) => {
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
  if (!roomId || !currentUid) return;
  leaveRoomDameSecure({ roomId, reason: "page_unload" }).catch(() => null);
});
