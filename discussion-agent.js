import {
  auth,
  db,
  collection,
  addDoc,
  doc,
  getDoc,
  setDoc,
  query,
  orderBy,
  limit,
  onSnapshot,
  onAuthStateChanged,
  serverTimestamp,
} from "./firebase-init.js";
import {
  SUPPORT_THREADS_COLLECTION,
  SUPPORT_MESSAGES_SUBCOLLECTION,
  THREAD_MESSAGES_LIMIT,
  getSupportThreadIdentity,
  formatMessageTime,
  uploadChatMedia,
  createMessagePayload,
  messagePreviewFromPayload,
} from "./discussion-shared.js";

let currentUser = null;
let currentThreadId = "";
let currentActor = null;
let messagesUnsub = null;
let pendingFile = null;

const backBtn = document.getElementById("agentChatBackBtn");
const liveStatusEl = document.getElementById("agentChatLiveStatus");
const identityEl = document.getElementById("agentChatIdentity");
const messagesWrap = document.getElementById("agentChatMessages");
const emptyStateEl = document.getElementById("agentChatEmptyState");
const inputEl = document.getElementById("agentChatInput");
const sendBtn = document.getElementById("agentChatSendBtn");
const attachBtn = document.getElementById("agentChatAttachBtn");
const fileInputEl = document.getElementById("agentChatFileInput");
const filePreviewEl = document.getElementById("agentChatFilePreview");
const fileLabelEl = document.getElementById("agentChatFileLabel");
const fileRemoveBtn = document.getElementById("agentChatFileRemoveBtn");
const composerStatusEl = document.getElementById("agentChatComposerStatus");

function setLiveStatus(text, tone = "neutral") {
  if (!liveStatusEl) return;
  liveStatusEl.textContent = String(text || "");
  if (tone === "error") {
    liveStatusEl.style.color = "#ffb0b0";
    return;
  }
  if (tone === "ok") {
    liveStatusEl.style.color = "#8ff0c6";
    return;
  }
  liveStatusEl.style.color = "";
}

function setComposerStatus(text = "", tone = "neutral") {
  if (!composerStatusEl) return;
  composerStatusEl.textContent = String(text || "");
  composerStatusEl.className = "status";
  if (tone === "error") composerStatusEl.classList.add("error");
  if (tone === "success") composerStatusEl.classList.add("success");
}

function setPendingFile(file) {
  pendingFile = file || null;
  if (!filePreviewEl || !fileLabelEl) return;

  if (!pendingFile) {
    filePreviewEl.classList.remove("visible");
    fileLabelEl.textContent = "";
    if (fileInputEl) fileInputEl.value = "";
    return;
  }

  filePreviewEl.classList.add("visible");
  fileLabelEl.textContent = `${pendingFile.name} (${Math.max(1, Math.round((pendingFile.size || 0) / 1024))} Ko)`;
}

function isNearBottom() {
  if (!messagesWrap) return true;
  const remaining = messagesWrap.scrollHeight - messagesWrap.scrollTop - messagesWrap.clientHeight;
  return remaining < 72;
}

function scrollToBottom(force = false) {
  if (!messagesWrap) return;
  if (!force && !isNearBottom()) return;
  messagesWrap.scrollTop = messagesWrap.scrollHeight;
}

function renderEmptyState(show) {
  if (!messagesWrap || !emptyStateEl) return;
  if (show) {
    if (!emptyStateEl.parentElement) messagesWrap.appendChild(emptyStateEl);
    return;
  }
  if (emptyStateEl.parentElement === messagesWrap) {
    emptyStateEl.remove();
  }
}

function createMediaNode(data) {
  const mediaType = String(data?.mediaType || "");
  const mediaUrl = String(data?.mediaUrl || "");
  if (!mediaType || !mediaUrl) return null;

  const wrap = document.createElement("div");
  wrap.className = "media";

  if (mediaType === "video") {
    const video = document.createElement("video");
    video.controls = true;
    video.preload = "metadata";
    video.src = mediaUrl;
    wrap.appendChild(video);
    return wrap;
  }

  const img = document.createElement("img");
  img.loading = "lazy";
  img.alt = String(data?.fileName || "Media");
  img.src = mediaUrl;
  wrap.appendChild(img);
  return wrap;
}

function renderMessages(entries) {
  if (!messagesWrap) return;
  const keepBottom = isNearBottom();
  messagesWrap.innerHTML = "";

  if (!entries.length) {
    renderEmptyState(true);
    scrollToBottom(true);
    return;
  }

  renderEmptyState(false);
  const viewerKey = String(currentActor?.senderKey || "");
  const frag = document.createDocumentFragment();

  entries.forEach((entry) => {
    const data = entry?.data || {};
    const senderKey = String(data.senderKey || data.uid || data.guestId || "");
    const mine = senderKey && senderKey === viewerKey;
    const isAgent = String(data.senderRole || "") === "agent";

    const row = document.createElement("div");
    row.className = `row ${mine ? "mine" : "other"}${isAgent ? " agent" : ""}`;

    const bubble = document.createElement("article");
    bubble.className = "bubble";

    if (!mine || isAgent) {
      const author = document.createElement("p");
      author.className = "author";
      author.textContent = String(data.displayName || (isAgent ? "Agent Dominoes" : "Utilisateur"));
      bubble.appendChild(author);
    }

    const text = String(data.text || "").trim();
    if (text) {
      const textEl = document.createElement("p");
      textEl.className = "text";
      textEl.textContent = text;
      bubble.appendChild(textEl);
    }

    const mediaNode = createMediaNode(data);
    if (mediaNode) bubble.appendChild(mediaNode);

    const meta = document.createElement("div");
    meta.className = "meta";
    meta.textContent = formatMessageTime(data.createdAt || data.createdAtMs);
    bubble.appendChild(meta);

    row.appendChild(bubble);
    frag.appendChild(row);
  });

  messagesWrap.appendChild(frag);
  scrollToBottom(keepBottom);
}

async function markThreadSeen() {
  if (!currentThreadId) return;
  try {
    await setDoc(doc(db, SUPPORT_THREADS_COLLECTION, currentThreadId), {
      unreadForUser: false,
      participantSeenAt: serverTimestamp(),
      updatedAt: serverTimestamp(),
    }, { merge: true });
  } catch (error) {
    console.error("[SUPPORT] markThreadSeen error", error);
  }
}

function watchThreadMessages(threadId) {
  if (messagesUnsub) {
    messagesUnsub();
    messagesUnsub = null;
  }

  messagesUnsub = onSnapshot(
    query(
      collection(db, SUPPORT_THREADS_COLLECTION, threadId, SUPPORT_MESSAGES_SUBCOLLECTION),
      orderBy("createdAtMs", "desc"),
      limit(THREAD_MESSAGES_LIMIT)
    ),
    async (snapshot) => {
      const entries = snapshot.docs.map((item) => ({
        id: item.id,
        data: item.data() || {},
      })).reverse();
      renderMessages(entries);
      setLiveStatus("Conversation active", "ok");
      await markThreadSeen();
    },
    (error) => {
      console.error("[SUPPORT] watchThreadMessages error", error);
      setLiveStatus("Conversation indisponible", "error");
    }
  );
}

async function ensureThread(threadId, actor, participantType, participantId) {
  const threadRef = doc(db, SUPPORT_THREADS_COLLECTION, threadId);
  const snap = await getDoc(threadRef);
  const base = {
    threadId,
    participantType,
    participantId,
    participantUid: actor.uid || "",
    guestId: actor.guestId || "",
    participantName: actor.displayName || "Utilisateur",
    participantEmail: actor.email || "",
    status: "open",
    unreadForAgent: false,
    unreadForUser: false,
    firstAgentReplyAt: snap.exists() ? (snap.data()?.firstAgentReplyAt || null) : null,
    firstAgentReplyAtMs: Number(snap.exists() ? (snap.data()?.firstAgentReplyAtMs || 0) : 0),
    resolvedAt: snap.exists() ? (snap.data()?.resolvedAt || null) : null,
    resolvedAtMs: Number(snap.exists() ? (snap.data()?.resolvedAtMs || 0) : 0),
    resolutionTag: String(snap.exists() ? (snap.data()?.resolutionTag || "") : ""),
    updatedAt: serverTimestamp(),
  };

  if (!snap.exists()) {
    base.createdAt = serverTimestamp();
    base.createdAtMs = Date.now();
    base.lastMessageText = "Aucun message";
    base.lastMessageAtMs = 0;
    base.lastSenderRole = "";
  }

  await setDoc(threadRef, base, { merge: true });
}

async function activateThreadForUser(user) {
  const info = getSupportThreadIdentity(user);
  currentUser = user || null;
  currentThreadId = info.threadId;
  currentActor = info.actor;

  if (identityEl) {
    identityEl.textContent = info.participantType === "user"
      ? `Fil utilisateur: ${info.actor.displayName} (${info.actor.email || info.actor.uid})`
      : `Fil anonyme: ${info.actor.displayName} (${info.threadId})`;
  }

  setLiveStatus("Preparation du fil...", "neutral");
  await ensureThread(info.threadId, info.actor, info.participantType, info.participantId);
  watchThreadMessages(info.threadId);
}

async function sendSupportMessage() {
  const text = String(inputEl?.value || "").trim();
  if (!text && !pendingFile) {
    setComposerStatus("Ecris un message ou ajoute un média.", "error");
    return;
  }
  if (!currentThreadId || !currentActor) {
    setComposerStatus("Le fil de discussion n'est pas encore prêt.", "error");
    return;
  }

  if (sendBtn) sendBtn.disabled = true;
  setComposerStatus("Envoi en cours...", "neutral");

  try {
    let media = null;
    if (pendingFile) {
      media = await uploadChatMedia(pendingFile, { scope: "support", threadId: currentThreadId });
    }

    const payload = createMessagePayload(currentActor, text, media, {
      createdAt: serverTimestamp(),
      scope: "support",
      threadId: currentThreadId,
    });

    await addDoc(collection(db, SUPPORT_THREADS_COLLECTION, currentThreadId, SUPPORT_MESSAGES_SUBCOLLECTION), payload);
    await setDoc(doc(db, SUPPORT_THREADS_COLLECTION, currentThreadId), {
      lastMessageText: messagePreviewFromPayload(payload),
      lastMessageAt: serverTimestamp(),
      lastMessageAtMs: payload.createdAtMs,
      lastSenderRole: payload.senderRole,
      status: "open",
      unreadForAgent: true,
      unreadForUser: false,
      resolvedAt: null,
      resolvedAtMs: 0,
      resolutionTag: "",
      updatedAt: serverTimestamp(),
      participantName: currentActor.displayName || "Utilisateur",
      participantEmail: currentActor.email || "",
    }, { merge: true });

    if (inputEl) inputEl.value = "";
    setPendingFile(null);
    setComposerStatus("Message envoye.", "success");
    scrollToBottom(true);
  } catch (error) {
    console.error("[SUPPORT] sendSupportMessage error", error);
    setComposerStatus(error?.message || "Impossible d'envoyer le message.", "error");
  } finally {
    if (sendBtn) sendBtn.disabled = false;
  }
}

function bindUI() {
  if (backBtn && backBtn.dataset.bound !== "1") {
    backBtn.dataset.bound = "1";
    backBtn.addEventListener("click", () => {
      window.location.href = "./discussion.html";
    });
  }

  if (attachBtn && attachBtn.dataset.bound !== "1") {
    attachBtn.dataset.bound = "1";
    attachBtn.addEventListener("click", () => {
      fileInputEl?.click();
    });
  }

  if (fileInputEl && fileInputEl.dataset.bound !== "1") {
    fileInputEl.dataset.bound = "1";
    fileInputEl.addEventListener("change", () => {
      const file = fileInputEl.files?.[0] || null;
      setPendingFile(file);
      if (file) setComposerStatus("Media pret a etre envoye.", "neutral");
    });
  }

  if (fileRemoveBtn && fileRemoveBtn.dataset.bound !== "1") {
    fileRemoveBtn.dataset.bound = "1";
    fileRemoveBtn.addEventListener("click", () => {
      setPendingFile(null);
      setComposerStatus("", "neutral");
    });
  }

  if (sendBtn && sendBtn.dataset.bound !== "1") {
    sendBtn.dataset.bound = "1";
    sendBtn.addEventListener("click", sendSupportMessage);
  }

  if (inputEl && inputEl.dataset.bound !== "1") {
    inputEl.dataset.bound = "1";
    inputEl.addEventListener("keydown", (event) => {
      if (event.key === "Enter" && !event.shiftKey) {
        event.preventDefault();
        sendSupportMessage();
      }
    });
  }

  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") {
      markThreadSeen();
    }
  });

  window.addEventListener("beforeunload", () => {
    if (messagesUnsub) {
      messagesUnsub();
      messagesUnsub = null;
    }
  });
}

bindUI();
setLiveStatus("Preparation du fil...", "neutral");

onAuthStateChanged(auth, async (user) => {
  try {
    await activateThreadForUser(user || null);
  } catch (error) {
    console.error("[SUPPORT] activateThreadForUser error", error);
    setLiveStatus("Impossible d'ouvrir ce fil", "error");
  }
});
