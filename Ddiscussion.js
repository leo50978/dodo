import {
  db,
  collection,
  addDoc,
  doc,
  getDoc,
  getDocs,
  setDoc,
  updateDoc,
  deleteDoc,
  query,
  orderBy,
  limit,
  onSnapshot,
  serverTimestamp,
} from "./firebase-init.js";
import { ensureFinanceDashboardSession } from "./dashboard-admin-auth.js";
import {
  CHAT_COLLECTION,
  SUPPORT_THREADS_COLLECTION,
  SUPPORT_MESSAGES_SUBCOLLECTION,
  CHANNEL_LIMIT,
  THREAD_MESSAGES_LIMIT,
  formatMessageTime,
  uploadChatMedia,
  deleteChatMedia,
  createMessagePayload,
  messagePreviewFromPayload,
} from "./discussion-shared.js";

const AGENT_ACTOR = {
  senderRole: "agent",
  senderType: "agent",
  senderKey: "agent_dashboard",
  uid: "",
  guestId: "",
  email: "",
  displayName: "Agent Dominoes",
};

const CLIENTS_COLLECTION = "clients";
const CLIENT_WINDOW_STEP = 160;
const CLIENT_WINDOW_MAX = 1280;
const CLIENT_RENDER_LIMIT = 80;
const THREAD_RENDER_LIMIT = 80;

let channelUnsub = null;
let threadsUnsub = null;
let clientsUnsub = null;
let selectedThreadUnsub = null;
let selectedThreadId = "";
let knownThreads = [];
let knownClients = [];
let pendingChannelFile = null;
let pendingThreadFile = null;
let clientWindowSize = CLIENT_WINDOW_STEP;
let clientsFallbackQuery = false;

const openPublicBtn = document.getElementById("dashboardOpenPublicBtn");
const metricThreadsEl = document.getElementById("dashMetricThreads");
const metricUnreadEl = document.getElementById("dashMetricUnread");
const metricUsersEl = document.getElementById("dashMetricUsers");
const metricSelectionEl = document.getElementById("dashMetricSelection");
const channelInputEl = document.getElementById("dashChannelInput");
const channelAttachBtn = document.getElementById("dashChannelAttachBtn");
const channelFileInputEl = document.getElementById("dashChannelFileInput");
const channelFileChipEl = document.getElementById("dashChannelFileChip");
const channelFileLabelEl = document.getElementById("dashChannelFileLabel");
const channelFileRemoveBtn = document.getElementById("dashChannelFileRemoveBtn");
const channelSendBtn = document.getElementById("dashChannelSendBtn");
const channelStatusEl = document.getElementById("dashChannelStatus");
const channelFeedEl = document.getElementById("dashChannelFeed");

const clientSearchEl = document.getElementById("dashClientSearch");
const clientSummaryEl = document.getElementById("dashClientSummary");
const clientMoreBtn = document.getElementById("dashClientMoreBtn");
const clientListEl = document.getElementById("dashClientList");
const threadSearchEl = document.getElementById("dashThreadSearch");
const threadSummaryEl = document.getElementById("dashThreadSummary");
const threadListEl = document.getElementById("dashThreadList");
const threadEmptyEl = document.getElementById("dashThreadEmpty");
const threadPanelEl = document.getElementById("dashThreadPanel");
const threadTitleEl = document.getElementById("dashThreadTitle");
const threadMetaEl = document.getElementById("dashThreadMeta");
const threadResolveBtn = document.getElementById("dashThreadResolveBtn");
const threadFeedEl = document.getElementById("dashThreadFeed");
const threadInputEl = document.getElementById("dashThreadInput");
const threadAttachBtn = document.getElementById("dashThreadAttachBtn");
const threadFileInputEl = document.getElementById("dashThreadFileInput");
const threadFileChipEl = document.getElementById("dashThreadFileChip");
const threadFileLabelEl = document.getElementById("dashThreadFileLabel");
const threadFileRemoveBtn = document.getElementById("dashThreadFileRemoveBtn");
const threadSendBtn = document.getElementById("dashThreadSendBtn");
const threadStatusEl = document.getElementById("dashThreadStatus");

function normalizeSearchTerm(value = "") {
  return String(value || "").trim().toLowerCase();
}

function setStatus(target, text = "", tone = "neutral") {
  if (!target) return;
  target.textContent = String(text || "");
  target.className = "status";
  if (tone === "error") target.classList.add("error");
  if (tone === "success") target.classList.add("success");
}

function setFileChip(targetChip, targetLabel, inputEl, file) {
  if (!targetChip || !targetLabel) return;
  if (!file) {
    targetChip.classList.remove("visible");
    targetLabel.textContent = "";
    if (inputEl) inputEl.value = "";
    return;
  }

  targetChip.classList.add("visible");
  targetLabel.textContent = `${file.name} (${Math.max(1, Math.round((file.size || 0) / 1024))} Ko)`;
}

function normalizePreviewText(value = "") {
  return String(value || "").trim().slice(0, 120);
}

function buildUserThreadId(uid) {
  return `user_${String(uid || "").trim()}`;
}

function safeLabel(value, fallback = "Utilisateur") {
  const text = String(value || "").trim();
  return text || fallback;
}

function sanitizeResolutionTag(value = "") {
  return String(value || "")
    .trim()
    .replace(/\s+/g, " ")
    .slice(0, 80);
}

function formatCount(value) {
  return Number(value || 0).toLocaleString("fr-FR");
}

function clientMatchesFilter(client, filterValue) {
  if (!filterValue) return true;
  const haystack = [
    client?.name,
    client?.email,
    client?.phone,
    client?.id,
  ]
    .map((item) => String(item || "").toLowerCase())
    .join(" ");
  return haystack.includes(filterValue);
}

function threadMatchesFilter(thread, filterValue) {
  if (!filterValue) return true;
  const haystack = [
    thread?.participantName,
    thread?.participantEmail,
    thread?.participantId,
    thread?.id,
    thread?.lastMessageText,
  ]
    .map((item) => String(item || "").toLowerCase())
    .join(" ");
  return haystack.includes(filterValue);
}

function getFilteredClients() {
  const term = normalizeSearchTerm(clientSearchEl?.value || "");
  return knownClients.filter((item) => clientMatchesFilter(item, term));
}

function getFilteredThreads() {
  const term = normalizeSearchTerm(threadSearchEl?.value || "");
  return knownThreads.filter((item) => threadMatchesFilter(item, term));
}

function updateOverviewMetrics() {
  const filteredThreads = getFilteredThreads();
  const filteredClients = getFilteredClients();
  const unreadCount = knownThreads.filter((item) => item?.unreadForAgent === true).length;
  const selectedThread = findThreadById(selectedThreadId) || buildUserThreadFallback(selectedThreadId);

  if (metricThreadsEl) {
    metricThreadsEl.textContent = formatCount(filteredThreads.length);
  }
  if (metricUnreadEl) {
    metricUnreadEl.textContent = formatCount(unreadCount);
  }
  if (metricUsersEl) {
    metricUsersEl.textContent = formatCount(filteredClients.length);
  }
  if (metricSelectionEl) {
    metricSelectionEl.textContent = selectedThread
      ? safeLabel(selectedThread.participantName || selectedThread.id, "Fil")
      : "Aucune";
  }
}

function updateClientSummary(filteredCount, renderedCount) {
  if (!clientSummaryEl) return;
  const windowLabel = clientsFallbackQuery ? "fenêtre brute" : "fenêtre active";
  const truncated = filteredCount > renderedCount
    ? `, affichage limité à ${formatCount(renderedCount)}`
    : "";
  clientSummaryEl.textContent =
    `${formatCount(knownClients.length)} clients chargés dans la ${windowLabel}, ${formatCount(filteredCount)} correspondent au filtre${truncated}.`;
  if (clientMoreBtn) {
    clientMoreBtn.disabled = clientWindowSize >= CLIENT_WINDOW_MAX;
    clientMoreBtn.innerHTML = clientWindowSize >= CLIENT_WINDOW_MAX
      ? '<i class="fa-solid fa-check"></i> Fenêtre max'
      : '<i class="fa-solid fa-layer-group"></i> Charger plus';
  }
}

function updateThreadSummary(filteredCount, renderedCount) {
  if (!threadSummaryEl) return;
  const unreadCount = knownThreads.filter((item) => item?.unreadForAgent === true).length;
  const truncated = filteredCount > renderedCount
    ? ` Affichage limité à ${formatCount(renderedCount)}.`
    : "";
  threadSummaryEl.textContent =
    `${formatCount(knownThreads.length)} fils actifs, ${formatCount(unreadCount)} non lus, ${formatCount(filteredCount)} visibles avec le filtre.${truncated}`;
}

function sortClients(items) {
  return [...items].sort((a, b) => {
    const left = safeLabel(a.name || a.email || a.id).toLowerCase();
    const right = safeLabel(b.name || b.email || b.id).toLowerCase();
    return left.localeCompare(right, "fr");
  });
}

function findClientById(clientId) {
  return knownClients.find((item) => item.id === clientId) || null;
}

function buildUserThreadFallback(threadId) {
  if (!String(threadId || "").startsWith("user_")) return null;
  const clientId = String(threadId || "").slice(5);
  const client = findClientById(clientId);
  if (!client) return null;
  return {
    id: threadId,
    participantType: "user",
    participantId: client.id,
    participantUid: client.id,
    participantName: safeLabel(client.name || client.email || client.id),
    participantEmail: String(client.email || ""),
    lastMessageText: "Aucun message",
  };
}

function createMediaNode(data) {
  const mediaType = String(data?.mediaType || "");
  const mediaUrl = String(data?.mediaUrl || "");
  if (!mediaType || !mediaUrl) return null;

  const wrap = document.createElement("div");
  wrap.className = "bubble-media";

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

function messageDocRef(scope, messageId, threadId = "") {
  if (scope === "channel") {
    return doc(db, CHAT_COLLECTION, messageId);
  }
  return doc(db, SUPPORT_THREADS_COLLECTION, threadId, SUPPORT_MESSAGES_SUBCOLLECTION, messageId);
}

async function refreshThreadSummary(threadId) {
  if (!threadId) return;

  const latestSnap = await getDocs(
    query(
      collection(db, SUPPORT_THREADS_COLLECTION, threadId, SUPPORT_MESSAGES_SUBCOLLECTION),
      orderBy("createdAtMs", "desc"),
      limit(1)
    )
  );

  if (latestSnap.empty) {
    await setDoc(doc(db, SUPPORT_THREADS_COLLECTION, threadId), {
      lastMessageText: "Aucun message",
      lastMessageAt: null,
      lastMessageAtMs: 0,
      lastSenderRole: "",
      updatedAt: serverTimestamp(),
    }, { merge: true });
    return;
  }

  const latest = latestSnap.docs[0].data() || {};
  const patch = {
    lastMessageText: messagePreviewFromPayload(latest),
    lastMessageAtMs: Number(latest.createdAtMs || 0),
    lastSenderRole: String(latest.senderRole || ""),
    updatedAt: serverTimestamp(),
  };
  if (latest.createdAt) {
    patch.lastMessageAt = latest.createdAt;
  }
  await setDoc(doc(db, SUPPORT_THREADS_COLLECTION, threadId), patch, { merge: true });
}

async function editMessage(scope, entry, threadId = "") {
  const currentText = String(entry?.data?.text || "");
  const hasMedia = Boolean(entry?.data?.mediaUrl);
  const nextText = window.prompt("Modifier le message", currentText);
  if (nextText === null) return;

  const trimmed = String(nextText || "").trim();
  if (!trimmed && !hasMedia) {
    window.alert("Un message sans texte doit au moins garder un média.");
    return;
  }

  try {
    await updateDoc(messageDocRef(scope, entry.id, threadId), {
      text: trimmed,
      editedAt: serverTimestamp(),
      editedAtMs: Date.now(),
      updatedAt: serverTimestamp(),
    });
    if (scope === "thread") {
      await refreshThreadSummary(threadId);
    }
  } catch (error) {
    console.error("[DASH_CHAT] editMessage error", error);
    setStatus(scope === "channel" ? channelStatusEl : threadStatusEl, "Impossible de modifier ce message.", "error");
  }
}

async function removeMessage(scope, entry, threadId = "") {
  const ok = window.confirm("Supprimer ce message ?");
  if (!ok) return;

  try {
    const mediaPath = String(entry?.data?.mediaPath || "").trim();
    if (mediaPath) {
      await deleteChatMedia(mediaPath);
    }
    await deleteDoc(messageDocRef(scope, entry.id, threadId));
    if (scope === "thread") {
      await refreshThreadSummary(threadId);
    }
  } catch (error) {
    console.error("[DASH_CHAT] removeMessage error", error);
    setStatus(scope === "channel" ? channelStatusEl : threadStatusEl, "Impossible de supprimer ce message.", "error");
  }
}

function renderBubbleList(target, entries, options = {}) {
  if (!target) return;
  target.innerHTML = "";

  if (!entries.length) {
    const empty = document.createElement("div");
    empty.className = "empty";
    empty.textContent = "Aucun message pour le moment.";
    target.appendChild(empty);
    return;
  }

  const frag = document.createDocumentFragment();
  entries.forEach((entry) => {
    const data = entry?.data || {};
    const role = String(data.senderRole || "user");
    const bubble = document.createElement("article");
    bubble.className = `bubble ${role === "agent" ? "agent" : "user"}`;

    const head = document.createElement("div");
    head.className = "bubble-head";
    const left = document.createElement("span");
    left.textContent = String(data.displayName || (role === "agent" ? "Agent Dominoes" : "Utilisateur"));
    const rightWrap = document.createElement("span");
    rightWrap.className = "bubble-head-right";
    const time = document.createElement("span");
    time.textContent = formatMessageTime(data.createdAt || data.createdAtMs);
    rightWrap.appendChild(time);

    if (options.editable === true) {
      const actions = document.createElement("span");
      actions.className = "bubble-actions";

      const editBtn = document.createElement("button");
      editBtn.type = "button";
      editBtn.className = "action-mini";
      editBtn.textContent = "Modifier";
      editBtn.addEventListener("click", () => {
        editMessage(options.scope || "channel", entry, options.threadId || "");
      });

      const deleteBtn = document.createElement("button");
      deleteBtn.type = "button";
      deleteBtn.className = "action-mini danger";
      deleteBtn.textContent = "Supprimer";
      deleteBtn.addEventListener("click", () => {
        removeMessage(options.scope || "channel", entry, options.threadId || "");
      });

      actions.append(editBtn, deleteBtn);
      rightWrap.appendChild(actions);
    }

    head.append(left, rightWrap);
    bubble.appendChild(head);

    const text = String(data.text || "").trim();
    if (text) {
      const textEl = document.createElement("p");
      textEl.className = "bubble-text";
      textEl.textContent = text;
      bubble.appendChild(textEl);
    }

    const mediaNode = createMediaNode(data);
    if (mediaNode) bubble.appendChild(mediaNode);

    frag.appendChild(bubble);
  });

  target.appendChild(frag);
  target.scrollTop = target.scrollHeight;
}

function renderClientList(clients) {
  if (!clientListEl) return;
  clientListEl.innerHTML = "";

  const visibleClients = clients.slice(0, CLIENT_RENDER_LIMIT);
  updateClientSummary(clients.length, visibleClients.length);

  if (!visibleClients.length) {
    const empty = document.createElement("div");
    empty.className = "empty";
    empty.textContent = knownClients.length
      ? "Aucun client ne correspond au filtre."
      : "Aucun client trouvé dans la fenêtre chargée.";
    clientListEl.appendChild(empty);
    updateOverviewMetrics();
    return;
  }

  const frag = document.createDocumentFragment();
  visibleClients.forEach((client) => {
    const threadId = buildUserThreadId(client.id);
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = `thread-item${threadId === selectedThreadId ? " active" : ""}`;

    const name = document.createElement("div");
    name.className = "thread-name";
    name.textContent = safeLabel(client.name || client.email || client.id);

    const meta = document.createElement("div");
    meta.className = "thread-meta";
    meta.textContent = String(client.email || client.phone || client.id);

    btn.append(name, meta);
    btn.addEventListener("click", () => {
      openClientThread(client.id);
    });
    frag.appendChild(btn);
  });

  clientListEl.appendChild(frag);
  updateOverviewMetrics();
}

function renderThreadList(threads) {
  if (!threadListEl) return;
  threadListEl.innerHTML = "";

  const visibleThreads = threads.slice(0, THREAD_RENDER_LIMIT);
  updateThreadSummary(threads.length, visibleThreads.length);

  if (!visibleThreads.length) {
    if (threadEmptyEl) threadEmptyEl.hidden = false;
    if (threadPanelEl) threadPanelEl.hidden = true;
    updateOverviewMetrics();
    return;
  }

  if (threadEmptyEl) threadEmptyEl.hidden = true;

  const frag = document.createDocumentFragment();
  visibleThreads.forEach((item) => {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = `thread-item${item.id === selectedThreadId ? " active" : ""}`;

    const name = document.createElement("div");
    name.className = "thread-name";
    name.textContent = String(item.participantName || item.displayName || item.id);

    const meta = document.createElement("div");
    meta.className = "thread-meta";
    const kind = String(item.participantType || "user");
    const unread = item.unreadForAgent ? " • nouveau message" : "";
    meta.textContent = `${kind === "guest" ? "Anonyme" : "Utilisateur"}${unread}`;

    const preview = document.createElement("div");
    preview.className = "thread-preview";
    preview.textContent = String(item.lastMessageText || "Aucun message");

    btn.append(name, meta, preview);
    btn.addEventListener("click", () => {
      selectThread(item.id);
    });
    frag.appendChild(btn);
  });

  threadListEl.appendChild(frag);
  updateOverviewMetrics();
}

function updateSelectedThreadHeader(thread) {
  if (!threadPanelEl || !threadTitleEl || !threadMetaEl) return;
  if (!thread) {
    threadPanelEl.hidden = true;
    updateOverviewMetrics();
    return;
  }

  threadPanelEl.hidden = false;
  const participantType = String(thread.participantType || "user");
  threadTitleEl.textContent = String(thread.participantName || thread.id);
  const parts = [
    participantType === "guest" ? "Fil anonyme" : "Fil utilisateur",
    thread.participantEmail || thread.participantId || "",
  ].filter(Boolean);
  const resolutionTag = String(thread.resolutionTag || "").trim();
  if (String(thread.status || "open") === "closed") {
    parts.push(resolutionTag ? `Resolue (${resolutionTag})` : "Resolue");
  }
  threadMetaEl.textContent = parts.join(" • ");
  updateOverviewMetrics();
}

function findThreadById(threadId) {
  return knownThreads.find((item) => item.id === threadId) || null;
}

async function ensureUserThread(client) {
  if (!client?.id) return;
  const threadId = buildUserThreadId(client.id);
  const threadRef = doc(db, SUPPORT_THREADS_COLLECTION, threadId);
  const existing = await getDoc(threadRef);
  const patch = {
    threadId,
    participantType: "user",
    participantId: client.id,
    participantUid: client.id,
    participantName: safeLabel(client.name || client.email || client.id),
    participantEmail: String(client.email || ""),
    status: "open",
    unreadForAgent: false,
    unreadForUser: false,
    updatedAt: serverTimestamp(),
  };
  if (!existing.exists()) {
    patch.createdAt = serverTimestamp();
    patch.createdAtMs = Date.now();
    patch.lastMessageText = "Aucun message";
    patch.lastMessageAtMs = 0;
    patch.firstAgentReplyAt = null;
    patch.firstAgentReplyAtMs = 0;
    patch.resolvedAt = null;
    patch.resolvedAtMs = 0;
    patch.resolutionTag = "";
  }
  await setDoc(threadRef, patch, { merge: true });
}

async function openClientThread(clientId) {
  const client = findClientById(clientId);
  if (!client) return;
  await ensureUserThread(client);
  selectThread(buildUserThreadId(client.id));
}

function selectThread(threadId) {
  if (!threadId) return;
  selectedThreadId = String(threadId);
  renderClientList(getFilteredClients());
  renderThreadList(getFilteredThreads());
  updateSelectedThreadHeader(findThreadById(selectedThreadId) || buildUserThreadFallback(selectedThreadId));
  watchSelectedThreadMessages(selectedThreadId);
}

function watchChannel() {
  if (channelUnsub) {
    channelUnsub();
    channelUnsub = null;
  }

  channelUnsub = onSnapshot(
    query(collection(db, CHAT_COLLECTION), orderBy("createdAtMs", "desc"), limit(CHANNEL_LIMIT)),
    (snapshot) => {
      const entries = snapshot.docs.map((item) => ({
        id: item.id,
        data: item.data() || {},
      })).reverse();
      renderBubbleList(channelFeedEl, entries, {
        editable: true,
        scope: "channel",
      });
    },
    (error) => {
      console.error("[DASH_CHAT] watchChannel error", error);
      setStatus(channelStatusEl, "Impossible de lire le canal public.", "error");
    }
  );
}

function watchClients() {
  if (clientsUnsub) {
    clientsUnsub();
    clientsUnsub = null;
  }

  const clientsQuery = clientsFallbackQuery
    ? query(collection(db, CLIENTS_COLLECTION), limit(clientWindowSize))
    : query(collection(db, CLIENTS_COLLECTION), orderBy("lastSeenAtMs", "desc"), limit(clientWindowSize));

  clientsUnsub = onSnapshot(
    clientsQuery,
    (snapshot) => {
      knownClients = sortClients(
        snapshot.docs.map((item) => ({
          id: item.id,
          ...item.data(),
        }))
      );
      renderClientList(getFilteredClients());
      if (selectedThreadId) {
        updateSelectedThreadHeader(findThreadById(selectedThreadId) || buildUserThreadFallback(selectedThreadId));
      }
    },
    (error) => {
      console.error("[DASH_CHAT] watchClients error", error);
      if (!clientsFallbackQuery) {
        clientsFallbackQuery = true;
        watchClients();
        return;
      }
      setStatus(threadStatusEl, "Impossible de lire les clients.", "error");
    }
  );
}

function watchThreads() {
  if (threadsUnsub) {
    threadsUnsub();
    threadsUnsub = null;
  }

  threadsUnsub = onSnapshot(
    query(collection(db, SUPPORT_THREADS_COLLECTION), orderBy("lastMessageAtMs", "desc"), limit(120)),
    (snapshot) => {
      knownThreads = snapshot.docs.map((item) => ({
        id: item.id,
        ...item.data(),
      }));
      if (selectedThreadId && !findThreadById(selectedThreadId)) {
        selectedThreadId = "";
        if (selectedThreadUnsub) {
          selectedThreadUnsub();
          selectedThreadUnsub = null;
        }
      }
      if (!selectedThreadId && knownThreads.length) {
        selectedThreadId = knownThreads[0].id;
      }
      renderClientList(getFilteredClients());
      renderThreadList(getFilteredThreads());
      updateSelectedThreadHeader(findThreadById(selectedThreadId) || buildUserThreadFallback(selectedThreadId));
      if (selectedThreadId) {
        watchSelectedThreadMessages(selectedThreadId);
      } else if (threadFeedEl) {
        renderBubbleList(threadFeedEl, [], { editable: true, scope: "thread", threadId: "" });
      }
    },
    (error) => {
      console.error("[DASH_CHAT] watchThreads error", error);
      setStatus(threadStatusEl, "Impossible de lire les fils support.", "error");
    }
  );
}

function watchSelectedThreadMessages(threadId) {
  if (!threadId) return;
  if (selectedThreadUnsub) {
    selectedThreadUnsub();
    selectedThreadUnsub = null;
  }

  selectedThreadUnsub = onSnapshot(
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
      renderBubbleList(threadFeedEl, entries, {
        editable: true,
        scope: "thread",
        threadId,
      });
      await setDoc(doc(db, SUPPORT_THREADS_COLLECTION, threadId), {
        unreadForAgent: false,
        agentSeenAt: serverTimestamp(),
        updatedAt: serverTimestamp(),
      }, { merge: true });
    },
    (error) => {
      console.error("[DASH_CHAT] watchSelectedThreadMessages error", error);
      setStatus(threadStatusEl, "Impossible de lire ce fil.", "error");
    }
  );
}

async function sendChannelPost() {
  const text = String(channelInputEl?.value || "").trim();
  if (!text && !pendingChannelFile) {
    setStatus(channelStatusEl, "Ecris un message ou ajoute un média.", "error");
    return;
  }

  if (channelSendBtn) channelSendBtn.disabled = true;
  setStatus(channelStatusEl, "Publication en cours...", "neutral");

  try {
    let media = null;
    if (pendingChannelFile) {
      media = await uploadChatMedia(pendingChannelFile, { scope: "channel", threadId: "public" });
    }

    const payload = createMessagePayload(AGENT_ACTOR, text, media, {
      createdAt: serverTimestamp(),
      scope: "channel",
    });

    await addDoc(collection(db, CHAT_COLLECTION), payload);
    if (channelInputEl) channelInputEl.value = "";
    pendingChannelFile = null;
    setFileChip(channelFileChipEl, channelFileLabelEl, channelFileInputEl, null);
    setStatus(channelStatusEl, "Publication envoyee.", "success");
  } catch (error) {
    console.error("[DASH_CHAT] sendChannelPost error", error);
    setStatus(channelStatusEl, error?.message || "Impossible de publier ce message.", "error");
  } finally {
    if (channelSendBtn) channelSendBtn.disabled = false;
  }
}

async function sendThreadReply() {
  const text = String(threadInputEl?.value || "").trim();
  if (!selectedThreadId) {
    setStatus(threadStatusEl, "Selectionne d'abord un fil.", "error");
    return;
  }
  if (!text && !pendingThreadFile) {
    setStatus(threadStatusEl, "Ecris un message ou ajoute un média.", "error");
    return;
  }

  if (threadSendBtn) threadSendBtn.disabled = true;
  setStatus(threadStatusEl, "Envoi de la reponse...", "neutral");

  try {
    const fallbackThread = buildUserThreadFallback(selectedThreadId);
    if (fallbackThread) {
      const client = findClientById(fallbackThread.participantId);
      if (client) {
        await ensureUserThread(client);
      }
    }

    const threadRef = doc(db, SUPPORT_THREADS_COLLECTION, selectedThreadId);
    const threadSnap = await getDoc(threadRef);
    const threadData = threadSnap.exists() ? (threadSnap.data() || {}) : {};

    let media = null;
    if (pendingThreadFile) {
      media = await uploadChatMedia(pendingThreadFile, { scope: "support", threadId: selectedThreadId });
    }

    const payload = createMessagePayload(AGENT_ACTOR, text, media, {
      createdAt: serverTimestamp(),
      scope: "support",
      threadId: selectedThreadId,
    });

    await addDoc(collection(db, SUPPORT_THREADS_COLLECTION, selectedThreadId, SUPPORT_MESSAGES_SUBCOLLECTION), payload);
    await setDoc(threadRef, {
      lastMessageText: messagePreviewFromPayload(payload),
      lastMessageAt: serverTimestamp(),
      lastMessageAtMs: payload.createdAtMs,
      lastSenderRole: "agent",
      unreadForAgent: false,
      unreadForUser: true,
      updatedAt: serverTimestamp(),
      status: "open",
      agentLastMessageAt: serverTimestamp(),
      firstAgentReplyAt: threadData.firstAgentReplyAt || serverTimestamp(),
      firstAgentReplyAtMs: Number(threadData.firstAgentReplyAtMs || payload.createdAtMs),
      resolvedAt: null,
      resolvedAtMs: 0,
      resolutionTag: "",
    }, { merge: true });

    if (threadInputEl) threadInputEl.value = "";
    pendingThreadFile = null;
    setFileChip(threadFileChipEl, threadFileLabelEl, threadFileInputEl, null);
    setStatus(threadStatusEl, "Reponse envoyee.", "success");
  } catch (error) {
    console.error("[DASH_CHAT] sendThreadReply error", error);
    setStatus(threadStatusEl, error?.message || "Impossible d'envoyer la reponse.", "error");
  } finally {
    if (threadSendBtn) threadSendBtn.disabled = false;
  }
}

async function markSelectedThreadResolved() {
  if (!selectedThreadId) {
    setStatus(threadStatusEl, "Selectionne d'abord un fil.", "error");
    return;
  }

  const rawTag = window.prompt("Tag de resolution (optionnel)", "resolved");
  if (rawTag === null) return;
  const resolutionTag = sanitizeResolutionTag(rawTag) || "resolved";

  try {
    await setDoc(doc(db, SUPPORT_THREADS_COLLECTION, selectedThreadId), {
      status: "closed",
      unreadForAgent: false,
      unreadForUser: false,
      resolvedAt: serverTimestamp(),
      resolvedAtMs: Date.now(),
      resolutionTag,
      updatedAt: serverTimestamp(),
    }, { merge: true });
    setStatus(threadStatusEl, `Fil marque comme resolu (${resolutionTag}).`, "success");
  } catch (error) {
    console.error("[DASH_CHAT] markSelectedThreadResolved error", error);
    setStatus(threadStatusEl, "Impossible de marquer ce fil comme resolu.", "error");
  }
}

function bindUI() {
  if (openPublicBtn) {
    openPublicBtn.addEventListener("click", () => {
      window.open("./discussion.html", "_blank");
    });
  }

  if (clientSearchEl) {
    clientSearchEl.addEventListener("input", () => {
      renderClientList(getFilteredClients());
    });
  }

  if (clientMoreBtn) {
    clientMoreBtn.addEventListener("click", () => {
      if (clientWindowSize >= CLIENT_WINDOW_MAX) {
        setStatus(threadStatusEl, "La fenêtre clients est déjà au maximum.", "error");
        return;
      }
      clientWindowSize = Math.min(CLIENT_WINDOW_MAX, clientWindowSize + CLIENT_WINDOW_STEP);
      watchClients();
      setStatus(threadStatusEl, "Fenêtre clients élargie.", "success");
    });
  }

  if (threadSearchEl) {
    threadSearchEl.addEventListener("input", () => {
      renderThreadList(getFilteredThreads());
    });
  }

  if (channelAttachBtn) {
    channelAttachBtn.addEventListener("click", () => {
      channelFileInputEl?.click();
    });
  }

  if (channelFileInputEl) {
    channelFileInputEl.addEventListener("change", () => {
      pendingChannelFile = channelFileInputEl.files?.[0] || null;
      setFileChip(channelFileChipEl, channelFileLabelEl, channelFileInputEl, pendingChannelFile);
    });
  }

  if (channelFileRemoveBtn) {
    channelFileRemoveBtn.addEventListener("click", () => {
      pendingChannelFile = null;
      setFileChip(channelFileChipEl, channelFileLabelEl, channelFileInputEl, null);
    });
  }

  if (channelSendBtn) {
    channelSendBtn.addEventListener("click", sendChannelPost);
  }

  if (channelInputEl) {
    channelInputEl.addEventListener("keydown", (event) => {
      if ((event.ctrlKey || event.metaKey) && event.key === "Enter") {
        event.preventDefault();
        sendChannelPost();
      }
    });
  }

  if (threadAttachBtn) {
    threadAttachBtn.addEventListener("click", () => {
      threadFileInputEl?.click();
    });
  }

  if (threadFileInputEl) {
    threadFileInputEl.addEventListener("change", () => {
      pendingThreadFile = threadFileInputEl.files?.[0] || null;
      setFileChip(threadFileChipEl, threadFileLabelEl, threadFileInputEl, pendingThreadFile);
    });
  }

  if (threadFileRemoveBtn) {
    threadFileRemoveBtn.addEventListener("click", () => {
      pendingThreadFile = null;
      setFileChip(threadFileChipEl, threadFileLabelEl, threadFileInputEl, null);
    });
  }

  if (threadSendBtn) {
    threadSendBtn.addEventListener("click", sendThreadReply);
  }

  if (threadResolveBtn) {
    threadResolveBtn.addEventListener("click", markSelectedThreadResolved);
  }

  if (threadInputEl) {
    threadInputEl.addEventListener("keydown", (event) => {
      if ((event.ctrlKey || event.metaKey) && event.key === "Enter") {
        event.preventDefault();
        sendThreadReply();
      }
    });
  }
}

async function initDashboard() {
  try {
    await ensureFinanceDashboardSession({
      title: "Dashboard Discussion",
      description: "Connecte-toi avec le compte admin pour piloter le canal et le support.",
    });
    bindUI();
    watchChannel();
    watchClients();
    watchThreads();
  } catch (error) {
    setStatus(channelStatusEl, error?.message || "Authentification administrateur requise.", "error");
    setStatus(threadStatusEl, error?.message || "Authentification administrateur requise.", "error");
  }
}

initDashboard();
