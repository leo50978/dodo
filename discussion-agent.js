import {
  auth,
  onAuthStateChanged,
} from "./firebase-init.js";
import {
  ensureSupportThreadSecure,
  getSupportMessagesSecure,
  createSupportMessageSecure,
  markSupportThreadSeenSecure,
} from "./secure-functions.js";
import {
  THREAD_MESSAGES_LIMIT,
  getSupportThreadIdentity,
  formatMessageTime,
  uploadChatMedia,
  getGuestAccessToken,
  setGuestAccessToken,
} from "./discussion-shared.js";

const SUPPORT_REFRESH_MS = 3000;
const LAST_SEEN_THROTTLE_MS = 4500;

let currentUser = null;
let currentThreadId = "";
let currentActor = null;
let pendingFile = null;
let refreshTimer = null;
let lastSeenWriteAt = 0;
let refreshToken = 0;

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

function canUploadMedia() {
  return !!currentUser?.uid;
}

function syncComposerPermissions() {
  const canUpload = canUploadMedia();
  if (attachBtn) attachBtn.disabled = !canUpload;
  if (fileInputEl) fileInputEl.disabled = !canUpload;
  if (fileRemoveBtn) fileRemoveBtn.disabled = !canUpload;
  if (!canUpload) {
    setPendingFile(null);
    setComposerStatus("Les médias sont disponibles après connexion.", "neutral");
  }
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

function stopRefreshLoop() {
  if (!refreshTimer) return;
  window.clearInterval(refreshTimer);
  refreshTimer = null;
}

function buildSupportPayload(extra = {}) {
  if (currentUser?.uid) return { ...extra };
  return {
    guestId: currentThreadId || String(currentActor?.guestId || ""),
    guestToken: getGuestAccessToken(),
    displayName: String(currentActor?.displayName || ""),
    ...extra,
  };
}

async function markThreadSeen(force = false) {
  if (!currentThreadId) return;
  const now = Date.now();
  if (!force && now - lastSeenWriteAt < LAST_SEEN_THROTTLE_MS) return;
  lastSeenWriteAt = now;

  try {
    const result = await markSupportThreadSeenSecure(buildSupportPayload({}));
    if (!currentUser?.uid && result?.guestToken) {
      setGuestAccessToken(result.guestToken);
    }
  } catch (error) {
    console.error("[SUPPORT] markThreadSeen error", error);
  }
}

async function refreshThreadMessages(forceSeen = false) {
  if (!currentThreadId) return;
  const myToken = ++refreshToken;

  try {
    const result = await getSupportMessagesSecure(
      buildSupportPayload({ limit: THREAD_MESSAGES_LIMIT })
    );
    if (myToken !== refreshToken) return;

    if (!currentUser?.uid && result?.guestToken) {
      setGuestAccessToken(result.guestToken);
    }

    const messages = Array.isArray(result?.messages) ? result.messages : [];
    const entries = messages.map((item) => ({
      id: item.id,
      data: item,
    }));

    renderMessages(entries);

    const thread = result?.thread && typeof result.thread === "object" ? result.thread : null;
    if (identityEl) {
      if (thread?.participantType === "guest") {
        identityEl.textContent = `Fil anonyme: ${thread.participantName || currentActor?.displayName || "Anonyme"} (${currentThreadId})`;
      } else {
        const label = thread?.participantName || currentActor?.displayName || "Utilisateur";
        const email = thread?.participantEmail || currentActor?.email || currentActor?.uid || "";
        identityEl.textContent = `Fil utilisateur: ${label}${email ? ` (${email})` : ""}`;
      }
    }

    setLiveStatus("Conversation active", "ok");
    await markThreadSeen(forceSeen);
  } catch (error) {
    if (myToken !== refreshToken) return;
    console.error("[SUPPORT] refreshThreadMessages error", error);
    setLiveStatus("Conversation indisponible", "error");
  }
}

function startRefreshLoop() {
  stopRefreshLoop();
  refreshTimer = window.setInterval(() => {
    refreshThreadMessages(false);
  }, SUPPORT_REFRESH_MS);
}

async function activateThreadForUser(user) {
  const info = getSupportThreadIdentity(user);
  currentUser = user || null;
  currentThreadId = info.threadId;
  currentActor = info.actor;
  lastSeenWriteAt = 0;
  refreshToken += 1;

  if (identityEl) {
    identityEl.textContent = info.participantType === "user"
      ? `Fil utilisateur: ${info.actor.displayName} (${info.actor.email || info.actor.uid})`
      : `Fil anonyme: ${info.actor.displayName} (${info.threadId})`;
  }

  setLiveStatus("Preparation du fil...", "neutral");

  const result = await ensureSupportThreadSecure(
    currentUser?.uid
      ? {}
      : {
          guestId: info.participantId,
          displayName: info.actor.displayName,
          guestToken: getGuestAccessToken(),
        }
  );

  currentThreadId = String(result?.threadId || info.threadId || "");
  if (!currentUser?.uid && result?.guestToken) {
    setGuestAccessToken(result.guestToken);
  }

  syncComposerPermissions();
  await refreshThreadMessages(true);
  startRefreshLoop();
}

async function sendSupportMessage() {
  const text = String(inputEl?.value || "").trim();
  if (!text && !pendingFile) {
    setComposerStatus("Ecris un message ou ajoute un média.", "error");
    return;
  }
  if (!currentThreadId || !currentActor) {
    setComposerStatus("Le fil de discussion n'est pas encore pret.", "error");
    return;
  }

  if (pendingFile && !canUploadMedia()) {
    setComposerStatus("Les médias sont réservés aux utilisateurs connectés.", "error");
    return;
  }

  if (sendBtn) sendBtn.disabled = true;
  setComposerStatus("Envoi en cours...", "neutral");

  try {
    let media = null;
    if (pendingFile) {
      media = await uploadChatMedia(pendingFile, { scope: "support", threadId: currentThreadId });
    }

    const result = await createSupportMessageSecure(
      buildSupportPayload({
        text,
        media,
      })
    );

    if (!currentUser?.uid && result?.guestToken) {
      setGuestAccessToken(result.guestToken);
    }

    if (inputEl) inputEl.value = "";
    setPendingFile(null);
    setComposerStatus("Message envoye.", "success");
    await refreshThreadMessages(true);
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
      if (!canUploadMedia()) {
        setComposerStatus("Les médias sont disponibles après connexion.", "neutral");
        return;
      }
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
      refreshThreadMessages(true);
    }
  });

  window.addEventListener("beforeunload", () => {
    stopRefreshLoop();
  });
}

bindUI();
setLiveStatus("Preparation du fil...", "neutral");

onAuthStateChanged(auth, async (user) => {
  stopRefreshLoop();
  try {
    await activateThreadForUser(user || null);
  } catch (error) {
    console.error("[SUPPORT] activateThreadForUser error", error);
    setLiveStatus("Impossible d'ouvrir ce fil", "error");
  }
});
