import {
  auth,
  db,
  collection,
  query,
  orderBy,
  limit,
  onSnapshot,
  onAuthStateChanged,
} from "./firebase-init.js";
import { markChatSeenSecure } from "./secure-functions.js";
import {
  CHAT_COLLECTION,
  CHANNEL_LIMIT,
  getGuestIdentity,
  formatMessageTime,
} from "./discussion-shared.js";

const LAST_SEEN_THROTTLE_MS = 4500;

let currentUser = null;
let channelUnsub = null;
let pendingFile = null;
let lastSeenWriteAt = 0;

const guestIdentity = getGuestIdentity();

const backBtn = document.getElementById("discussionBackBtn");
const openAgentChatBtn = document.getElementById("openAgentChatBtn");
const liveStatusEl = document.getElementById("discussionLiveStatus");
const messagesWrap = document.getElementById("discussionMessages");
const emptyStateEl = document.getElementById("discussionEmptyState");
const inputEl = document.getElementById("discussionInput");
const sendBtn = document.getElementById("discussionSendBtn");
const attachBtn = document.getElementById("discussionAttachBtn");
const fileInputEl = document.getElementById("discussionFileInput");
const filePreviewEl = document.getElementById("discussionFilePreview");
const fileLabelEl = document.getElementById("discussionFileLabel");
const fileRemoveBtn = document.getElementById("discussionFileRemoveBtn");
const composerStatusEl = document.getElementById("discussionComposerStatus");

function setLiveStatus(text, tone = "neutral") {
  if (!liveStatusEl) return;
  liveStatusEl.textContent = String(text || "Canal actif");
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
  if (tone === "error") {
    composerStatusEl.classList.add("error");
  } else if (tone === "success") {
    composerStatusEl.classList.add("success");
  }
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

function getViewerKey() {
  if (currentUser?.uid) return String(currentUser.uid);
  return String(guestIdentity.guestId);
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
  img.alt = String(data?.fileName || "Image discussion");
  img.src = mediaUrl;
  wrap.appendChild(img);
  return wrap;
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

  const viewerKey = getViewerKey();
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

function applyReadOnlyMode() {
  if (inputEl) {
    inputEl.value = "";
    inputEl.disabled = true;
    inputEl.setAttribute("placeholder", "Le canal public est en lecture seule.");
    inputEl.style.opacity = "0.65";
  }
  if (sendBtn) {
    sendBtn.disabled = true;
    sendBtn.style.display = "none";
  }
  if (attachBtn) {
    attachBtn.disabled = true;
    attachBtn.style.display = "none";
  }
  if (fileInputEl) fileInputEl.disabled = true;
  if (fileRemoveBtn) {
    fileRemoveBtn.disabled = true;
    fileRemoveBtn.style.display = "none";
  }
  if (filePreviewEl) filePreviewEl.classList.remove("visible");
  if (composerStatusEl) {
    setComposerStatus("Le canal public est en lecture seule. Utilise \"Discuter avec un agent\" pour écrire.", "neutral");
  }
}

async function markChatSeen(force = false) {
  if (!currentUser?.uid) return;
  const now = Date.now();
  if (!force && now - lastSeenWriteAt < LAST_SEEN_THROTTLE_MS) return;
  lastSeenWriteAt = now;

  try {
    await markChatSeenSecure({});
  } catch (error) {
    console.error("[DISCUSSION] markChatSeen error", error);
  }
}

function watchChannel() {
  if (channelUnsub) {
    channelUnsub();
    channelUnsub = null;
  }

  setLiveStatus("Connexion au canal...", "neutral");

  channelUnsub = onSnapshot(
    query(collection(db, CHAT_COLLECTION), orderBy("createdAtMs", "desc"), limit(CHANNEL_LIMIT)),
    async (snapshot) => {
      const entries = snapshot.docs.map((item) => ({
        id: item.id,
        data: item.data() || {},
      })).reverse();
      renderMessages(entries);
      const label = currentUser?.uid
        ? `Canal actif • Connecte en tant que ${currentUser.displayName || currentUser.email || "joueur"}`
        : `Canal actif • ${guestIdentity.displayName}`;
      setLiveStatus(label, "ok");
      await markChatSeen();
    },
    (error) => {
      console.error("[DISCUSSION] watchChannel error", error);
      setLiveStatus("Canal indisponible", "error");
    }
  );
}

function bindUI() {
  if (backBtn && backBtn.dataset.bound !== "1") {
    backBtn.dataset.bound = "1";
    backBtn.addEventListener("click", () => {
      window.location.href = "./inedex.html";
    });
  }

  if (openAgentChatBtn && openAgentChatBtn.dataset.bound !== "1") {
    openAgentChatBtn.dataset.bound = "1";
    openAgentChatBtn.addEventListener("click", () => {
      window.location.href = "./discussion-agent.html";
    });
  }

  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") {
      markChatSeen(true);
    }
  });

  window.addEventListener("beforeunload", () => {
    if (channelUnsub) {
      channelUnsub();
      channelUnsub = null;
    }
  });
}

bindUI();
applyReadOnlyMode();
watchChannel();

onAuthStateChanged(auth, async (user) => {
  currentUser = user || null;
  watchChannel();
  await markChatSeen(true);
});
