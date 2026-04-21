import { getPublicWhatsappModalConfigSecure } from "./secure-functions.js";
import { SUPPORT_WHATSAPP_PHONE } from "./support-contact.js";

const WHATSAPP_MODAL_CONFIG_CACHE_KEY = "dl_whatsapp_modal_config_v1";
const WHATSAPP_MODAL_CONFIG_VERSION = "wmc-v1";

const DEFAULT_WHATSAPP_MODAL_CONTACTS = Object.freeze({
  support_default: SUPPORT_WHATSAPP_PHONE,
  rejected_order: SUPPORT_WHATSAPP_PHONE,
  agent_deposit: SUPPORT_WHATSAPP_PHONE,
  withdrawal_assistance: SUPPORT_WHATSAPP_PHONE,
  welcome_deposit_modal: SUPPORT_WHATSAPP_PHONE,
  recruitment_modal: SUPPORT_WHATSAPP_PHONE,
});

let CONTACTS_CACHE = { ...DEFAULT_WHATSAPP_MODAL_CONTACTS };
let CONTACTS_VERSION = WHATSAPP_MODAL_CONFIG_VERSION;
let CONTACTS_UPDATED_AT_MS = 0;
let LOAD_PROMISE = null;

function sanitizeWhatsappDigits(value, fallback = "") {
  const digits = String(value || "").replace(/\D/g, "").trim();
  if (digits.length >= 8 && digits.length <= 20) return digits;
  return String(fallback || "").replace(/\D/g, "").trim();
}

function normalizeContacts(rawContacts = {}) {
  const source = rawContacts && typeof rawContacts === "object" ? rawContacts : {};
  const supportDefault = sanitizeWhatsappDigits(source.support_default, DEFAULT_WHATSAPP_MODAL_CONTACTS.support_default);
  return {
    support_default: supportDefault,
    rejected_order: sanitizeWhatsappDigits(source.rejected_order, supportDefault || DEFAULT_WHATSAPP_MODAL_CONTACTS.rejected_order),
    agent_deposit: sanitizeWhatsappDigits(source.agent_deposit, supportDefault || DEFAULT_WHATSAPP_MODAL_CONTACTS.agent_deposit),
    withdrawal_assistance: sanitizeWhatsappDigits(source.withdrawal_assistance, supportDefault || DEFAULT_WHATSAPP_MODAL_CONTACTS.withdrawal_assistance),
    welcome_deposit_modal: sanitizeWhatsappDigits(source.welcome_deposit_modal, supportDefault || DEFAULT_WHATSAPP_MODAL_CONTACTS.welcome_deposit_modal),
    recruitment_modal: sanitizeWhatsappDigits(source.recruitment_modal, supportDefault || DEFAULT_WHATSAPP_MODAL_CONTACTS.recruitment_modal),
  };
}

function saveLocalCache() {
  try {
    window.localStorage.setItem(
      WHATSAPP_MODAL_CONFIG_CACHE_KEY,
      JSON.stringify({
        version: CONTACTS_VERSION,
        updatedAtMs: CONTACTS_UPDATED_AT_MS,
        contacts: CONTACTS_CACHE,
      })
    );
  } catch (_) {}
}

function hydrateFromLocalCache() {
  try {
    const raw = window.localStorage.getItem(WHATSAPP_MODAL_CONFIG_CACHE_KEY);
    if (!raw) return;
    const parsed = JSON.parse(raw);
    CONTACTS_CACHE = normalizeContacts(parsed?.contacts || {});
    CONTACTS_VERSION = String(parsed?.version || WHATSAPP_MODAL_CONFIG_VERSION);
    CONTACTS_UPDATED_AT_MS = Number(parsed?.updatedAtMs) || 0;
  } catch (_) {}
}

export function getWhatsappModalContactsSnapshot() {
  return {
    contacts: { ...CONTACTS_CACHE },
    version: CONTACTS_VERSION,
    updatedAtMs: CONTACTS_UPDATED_AT_MS,
  };
}

export function getWhatsappContactDigits(key = "support_default", fallback = SUPPORT_WHATSAPP_PHONE) {
  const safeKey = String(key || "support_default").trim();
  const fallbackDigits = sanitizeWhatsappDigits(fallback, SUPPORT_WHATSAPP_PHONE);
  const support = sanitizeWhatsappDigits(CONTACTS_CACHE.support_default, fallbackDigits) || fallbackDigits;
  const candidate = sanitizeWhatsappDigits(CONTACTS_CACHE[safeKey], support);
  return candidate || support;
}

export function getWhatsappContactLabel(key = "support_default", fallback = SUPPORT_WHATSAPP_PHONE) {
  const digits = getWhatsappContactDigits(key, fallback);
  return digits ? `+${digits}` : "";
}

export function buildWhatsappUrlForKey(key = "support_default", message = "", fallback = SUPPORT_WHATSAPP_PHONE) {
  const digits = getWhatsappContactDigits(key, fallback);
  const base = `https://wa.me/${digits}`;
  const text = String(message || "").trim();
  return text ? `${base}?text=${encodeURIComponent(text)}` : base;
}

export async function refreshWhatsappModalContacts(force = false) {
  if (!force && LOAD_PROMISE) return LOAD_PROMISE;
  LOAD_PROMISE = (async () => {
    try {
      const payload = await getPublicWhatsappModalConfigSecure({});
      CONTACTS_CACHE = normalizeContacts(payload?.contacts || {});
      CONTACTS_VERSION = String(payload?.version || WHATSAPP_MODAL_CONFIG_VERSION);
      CONTACTS_UPDATED_AT_MS = Number(payload?.updatedAtMs) || 0;
      saveLocalCache();
      return getWhatsappModalContactsSnapshot();
    } catch (error) {
      return getWhatsappModalContactsSnapshot();
    } finally {
      LOAD_PROMISE = null;
    }
  })();
  return LOAD_PROMISE;
}

hydrateFromLocalCache();

export { DEFAULT_WHATSAPP_MODAL_CONTACTS, WHATSAPP_MODAL_CONFIG_VERSION };
