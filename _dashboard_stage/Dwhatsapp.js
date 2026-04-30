import { ensureFinanceDashboardSession } from "./dashboard-admin-auth.js";
import {
  getPublicWhatsappModalConfigSecure,
  setWhatsappModalConfigSecure,
} from "../secure-functions.js";

const dom = {
  form: document.getElementById("whatsappConfigForm"),
  status: document.getElementById("whatsappStatus"),
  saveBtn: document.getElementById("whatsappSaveBtn"),
  reloadBtn: document.getElementById("whatsappReloadBtn"),
  fields: {
    support_default: document.getElementById("wa_support_default"),
    rejected_order: document.getElementById("wa_rejected_order"),
    agent_deposit: document.getElementById("wa_agent_deposit"),
    withdrawal_assistance: document.getElementById("wa_withdrawal_assistance"),
    welcome_deposit_modal: document.getElementById("wa_welcome_deposit_modal"),
    recruitment_modal: document.getElementById("wa_recruitment_modal"),
  },
};

function sanitizeDigits(value = "") {
  return String(value || "").replace(/\D/g, "").trim();
}

function setStatus(message = "", tone = "") {
  if (!dom.status) return;
  dom.status.textContent = String(message || "");
  dom.status.classList.remove("error", "success");
  if (tone === "error") dom.status.classList.add("error");
  if (tone === "success") dom.status.classList.add("success");
}

function setLoading(isLoading) {
  if (dom.saveBtn) dom.saveBtn.disabled = isLoading;
  if (dom.reloadBtn) dom.reloadBtn.disabled = isLoading;
}

function applyContacts(contacts = {}) {
  Object.entries(dom.fields).forEach(([key, input]) => {
    if (!input) return;
    input.value = sanitizeDigits(contacts[key] || "");
  });
}

function collectContacts() {
  const out = {};
  Object.entries(dom.fields).forEach(([key, input]) => {
    if (!input) return;
    out[key] = sanitizeDigits(input.value);
  });
  return out;
}

async function loadContacts() {
  setLoading(true);
  setStatus("Chargement des contacts WhatsApp...");
  try {
    const payload = await getPublicWhatsappModalConfigSecure({});
    applyContacts(payload?.contacts || {});
    setStatus("Configuration chargée.", "success");
  } catch (error) {
    console.error("[DWHATSAPP] load error", error);
    setStatus(error?.message || "Impossible de charger la configuration.", "error");
  } finally {
    setLoading(false);
  }
}

async function saveContacts() {
  const contacts = collectContacts();
  if (!contacts.support_default) {
    setStatus("Le numéro support par défaut est obligatoire.", "error");
    return;
  }

  setLoading(true);
  setStatus("Enregistrement en cours...");
  try {
    const payload = await setWhatsappModalConfigSecure({ contacts });
    applyContacts(payload?.contacts || contacts);
    setStatus("Configuration WhatsApp enregistrée.", "success");
  } catch (error) {
    console.error("[DWHATSAPP] save error", error);
    setStatus(error?.message || "Impossible d'enregistrer la configuration.", "error");
  } finally {
    setLoading(false);
  }
}

async function boot() {
  try {
    await ensureFinanceDashboardSession({ fallbackUrl: "../auth.html" });
  } catch (_) {
    return;
  }

  if (dom.form) {
    dom.form.addEventListener("submit", (event) => {
      event.preventDefault();
      saveContacts();
    });
  }

  if (dom.reloadBtn) {
    dom.reloadBtn.addEventListener("click", () => {
      loadContacts();
    });
  }

  await loadContacts();
}

boot();
