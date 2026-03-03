import {
  storage,
  storageRef,
  uploadBytes,
  getDownloadURL,
  deleteObject,
} from "./firebase-init.js";

export const CHAT_COLLECTION = "globalChannelMessages";
export const SUPPORT_THREADS_COLLECTION = "supportThreads";
export const SUPPORT_MESSAGES_SUBCOLLECTION = "messages";
export const CHANNEL_LIMIT = 220;
export const THREAD_MESSAGES_LIMIT = 250;
export const MAX_CHAT_UPLOAD_BYTES = 40 * 1024 * 1024;
export const MESSAGE_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;

const GUEST_STORAGE_KEY = "domino_guest_chat_identity_v1";

function randomToken(size = 10) {
  const chars = "abcdefghjkmnpqrstuvwxyz23456789";
  let out = "";
  for (let i = 0; i < size; i += 1) {
    out += chars[Math.floor(Math.random() * chars.length)];
  }
  return out;
}

function readStoredGuest() {
  try {
    const raw = window.localStorage?.getItem(GUEST_STORAGE_KEY) || "";
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") return null;
    const guestId = String(parsed.guestId || "").trim();
    const displayName = String(parsed.displayName || "").trim();
    if (!guestId || !displayName) return null;
    const guestAccessToken = String(parsed.guestAccessToken || "").trim();
    return { guestId, displayName, guestAccessToken };
  } catch (_) {
    return null;
  }
}

function storeGuestIdentity(identity) {
  try {
    window.localStorage?.setItem(GUEST_STORAGE_KEY, JSON.stringify(identity));
  } catch (_) {
    // Ignore storage errors and keep the in-memory identity only.
  }
}

export function getGuestIdentity() {
  const existing = readStoredGuest();
  if (existing) return existing;

  const guestId = `guest_${Date.now()}_${randomToken(8)}`;
  const displayName = `Anonyme ${guestId.slice(-4).toUpperCase()}`;
  const created = { guestId, displayName, guestAccessToken: "" };
  storeGuestIdentity(created);
  return created;
}

export function getGuestAccessToken() {
  return String(getGuestIdentity()?.guestAccessToken || "").trim();
}

export function setGuestAccessToken(token = "") {
  const identity = getGuestIdentity();
  const next = {
    ...identity,
    guestAccessToken: String(token || "").trim(),
  };
  storeGuestIdentity(next);
  return next;
}

export function getActorFromUser(user, role = "user") {
  if (!user?.uid) return null;
  const email = String(user.email || "").trim();
  const displayName = String(user.displayName || "").trim()
    || (email ? email.split("@")[0] : "Utilisateur");

  return {
    senderRole: role,
    senderType: "user",
    senderKey: String(user.uid),
    uid: String(user.uid),
    email,
    displayName,
    guestId: "",
  };
}

export function getGuestActor(role = "guest") {
  const guest = getGuestIdentity();
  return {
    senderRole: role,
    senderType: "guest",
    senderKey: guest.guestId,
    uid: "",
    email: "",
    displayName: guest.displayName,
    guestId: guest.guestId,
  };
}

export function resolveActor(user, role = "user") {
  return getActorFromUser(user, role) || getGuestActor("guest");
}

export function getSupportThreadIdentity(user) {
  const actor = getActorFromUser(user, "user");
  if (actor) {
    return {
      threadId: `user_${actor.uid}`,
      actor,
      participantType: "user",
      participantId: actor.uid,
    };
  }

  const guestActor = getGuestActor("guest");
  return {
    threadId: guestActor.guestId,
    actor: guestActor,
    participantType: "guest",
    participantId: guestActor.guestId,
  };
}

export function tsToMs(value) {
  if (!value) return 0;
  if (typeof value.toMillis === "function") return value.toMillis();
  if (typeof value.toDate === "function") return value.toDate().getTime();
  if (typeof value.seconds === "number") return value.seconds * 1000;
  const parsed = Date.parse(String(value));
  return Number.isFinite(parsed) ? parsed : 0;
}

export function formatMessageTime(value) {
  const ms = tsToMs(value);
  if (!ms) return "Envoi...";
  return new Date(ms).toLocaleTimeString("fr-FR", {
    hour: "2-digit",
    minute: "2-digit",
  });
}

function sanitizeFileName(fileName = "") {
  return String(fileName || "")
    .trim()
    .replace(/[^a-zA-Z0-9._-]+/g, "_")
    .slice(0, 80) || "media";
}

export function detectMediaType(file) {
  const type = String(file?.type || "").toLowerCase();
  if (type.startsWith("image/")) return "image";
  if (type.startsWith("video/")) return "video";
  return "";
}

export async function uploadChatMedia(file, options = {}) {
  if (!file) return null;
  const mediaType = detectMediaType(file);
  if (!mediaType) {
    throw new Error("Seules les images et les vidéos sont acceptées.");
  }
  if (Number(file.size || 0) > MAX_CHAT_UPLOAD_BYTES) {
    throw new Error("Le fichier dépasse la taille autorisée.");
  }

  const scope = String(options.scope || "channel").trim() || "channel";
  const threadId = String(options.threadId || "public").trim() || "public";
  const safeName = sanitizeFileName(file.name || "");
  const path = `chat-media/${scope}/${threadId}/${Date.now()}_${randomToken(6)}_${safeName}`;
  const ref = storageRef(storage, path);

  await uploadBytes(ref, file, {
    contentType: file.type || undefined,
    customMetadata: {
      scope,
      mediaType,
    },
  });

  const url = await getDownloadURL(ref);
  return {
    mediaType,
    mediaUrl: url,
    mediaPath: path,
    fileName: safeName,
  };
}

export async function deleteChatMedia(path = "") {
  const safePath = String(path || "").trim();
  if (!safePath) return false;
  const ref = storageRef(storage, safePath);
  try {
    await deleteObject(ref);
  } catch (error) {
    const code = String(error?.code || "");
    if (code !== "storage/object-not-found") {
      throw error;
    }
  }
  return true;
}

export function createMessagePayload(actor, text, media = null, extras = {}) {
  const nowMs = Date.now();
  const expiresAtMs = nowMs + MESSAGE_RETENTION_MS;
  const trimmedText = String(text || "").trim();
  return {
    text: trimmedText,
    mediaType: String(media?.mediaType || ""),
    mediaUrl: String(media?.mediaUrl || ""),
    mediaPath: String(media?.mediaPath || ""),
    fileName: String(media?.fileName || ""),
    senderRole: String(actor?.senderRole || "user"),
    senderType: String(actor?.senderType || "guest"),
    senderKey: String(actor?.senderKey || ""),
    uid: String(actor?.uid || ""),
    guestId: String(actor?.guestId || ""),
    email: String(actor?.email || ""),
    displayName: String(actor?.displayName || "Utilisateur"),
    createdAtMs: nowMs,
    expiresAtMs,
    expiresAt: new Date(expiresAtMs),
    editedAtMs: 0,
    updatedAt: new Date(nowMs),
    ...extras,
  };
}

export function messagePreviewFromPayload(payload) {
  const text = String(payload?.text || "").trim();
  if (text) return text.slice(0, 120);
  if (payload?.mediaType === "video") return "Video";
  if (payload?.mediaType === "image") return "Image";
  return "Message";
}
