import {
  auth,
  db,
  collection,
  addDoc,
  query,
  orderBy,
  limit,
  onSnapshot,
  onAuthStateChanged,
  serverTimestamp,
} from "./firebase-init.js";
import { markChatSeenSecure } from "./secure-functions.js";
import {
  CHAT_COLLECTION,
  CHANNEL_LIMIT,
  getGuestIdentity,
  resolveActor,
  formatMessageTime,
  uploadChatMedia,
  createMessagePayload,
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

async function sendChannelMessage() {
  const text = String(inputEl?.value || "").trim();
  if (!text && !pendingFile) {
    setComposerStatus("Ecris un message ou ajoute un média.", "error");
    return;
  }

  if (sendBtn) sendBtn.disabled = true;
  setComposerStatus("Envoi en cours...", "neutral");

  try {
    let media = null;
    if (pendingFile) {
      media = await uploadChatMedia(pendingFile, { scope: "channel", threadId: "public" });
    }

    const actor = resolveActor(currentUser, "user");
    const payload = createMessagePayload(actor, text, media, {
      createdAt: serverTimestamp(),
      scope: "channel",
    });

    await addDoc(collection(db, CHAT_COLLECTION), payload);

    if (inputEl) inputEl.value = "";
    setPendingFile(null);
    setComposerStatus("Message envoye.", "success");
    scrollToBottom(true);
  } catch (error) {
    console.error("[DISCUSSION] sendChannelMessage error", error);
    setComposerStatus(error?.message || "Impossible d'envoyer le message.", "error");
  } finally {
    if (sendBtn) sendBtn.disabled = false;
  }
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
      if (!file) {
        setPendingFile(null);
        return;
      }
      setPendingFile(file);
      setComposerStatus("Media pret a etre envoye.", "neutral");
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
    sendBtn.addEventListener("click", sendChannelMessage);
  }

  if (inputEl && inputEl.dataset.bound !== "1") {
    inputEl.dataset.bound = "1";
    inputEl.addEventListener("keydown", (event) => {
      if (event.key === "Enter" && !event.shiftKey) {
        event.preventDefault();
        sendChannelMessage();
      }
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
watchChannel();

onAuthStateChanged(auth, async (user) => {
  currentUser = user || null;
  watchChannel();
  await markChatSeen(true);
});
