import "./firebase-init.js";
import {
  recordRecruitmentVisitSecure,
  submitRecruitmentApplicationSecure,
} from "./secure-functions.js";

const DEADLINE_MS = Date.parse("2026-04-07T03:59:59.999Z");
const VIRTUAL_PROGRESS_MIN = 0.86;
const VIRTUAL_PROGRESS_MAX = 0.95;
const VIRTUAL_PROGRESS_STEP_MS = 2800;
const VISIT_SESSION_STORAGE_KEY = "dl_recruitment_visit_session_v1";
const CONTRACT_SEEN_STORAGE_KEY = "dl_recruitment_contract_seen_v1";

const refs = {};
let progressTimer = null;
let virtualProgress = 0.89;
let progressDirection = 1;
let pendingPayload = null;

function formatRemaining(ms) {
  const safeMs = Math.max(0, Number(ms) || 0);
  if (safeMs <= 0) return "Cloture tres proche";

  const totalHours = Math.floor(safeMs / (60 * 60 * 1000));
  const days = Math.floor(totalHours / 24);
  const hours = totalHours % 24;

  if (days >= 2) return `${days} jours restants`;
  if (days === 1) return `1 jour et ${hours} h restantes`;
  if (hours >= 1) return `${hours} h restantes`;

  const minutes = Math.max(1, Math.floor(safeMs / (60 * 1000)));
  return `${minutes} min restantes`;
}

function setStatus(message, type = "") {
  if (!refs.status) return;
  refs.status.textContent = String(message || "").trim();
  refs.status.classList.remove("is-visible", "is-success", "is-error");
  if (!message) return;
  refs.status.classList.add("is-visible");
  if (type === "success") refs.status.classList.add("is-success");
  if (type === "error") refs.status.classList.add("is-error");
}

function updateDeadlinePill() {
  if (!refs.deadlinePill) return;
  const span = refs.deadlinePill.querySelector("span");
  if (span) {
    span.textContent = formatRemaining(DEADLINE_MS - Date.now());
  }
}

function paintProgress() {
  if (refs.progressFill) {
    refs.progressFill.style.width = `${(virtualProgress * 100).toFixed(1)}%`;
  }
}

function nudgeVirtualProgress(boost = 0) {
  const nextProgress = virtualProgress + (0.012 * progressDirection) + boost;
  if (nextProgress >= VIRTUAL_PROGRESS_MAX) {
    virtualProgress = VIRTUAL_PROGRESS_MAX;
    progressDirection = -1;
  } else if (nextProgress <= VIRTUAL_PROGRESS_MIN) {
    virtualProgress = VIRTUAL_PROGRESS_MIN;
    progressDirection = 1;
  } else {
    virtualProgress = nextProgress;
  }
  paintProgress();
}

function startProgressLoop() {
  if (progressTimer) {
    window.clearInterval(progressTimer);
  }
  paintProgress();
  updateDeadlinePill();
  progressTimer = window.setInterval(() => {
    nudgeVirtualProgress();
    updateDeadlinePill();
  }, VIRTUAL_PROGRESS_STEP_MS);
}

function updateMotivationCount() {
  if (!refs.motivation || !refs.motivationCount) return;
  refs.motivationCount.textContent = `${refs.motivation.value.length} / 2500`;
}

function setSubmitting(isSubmitting) {
  if (!refs.submitBtn) return;
  refs.submitBtn.disabled = isSubmitting === true;
  refs.submitBtn.textContent = isSubmitting === true ? "Envoi en cours..." : "Envoyer ma candidature";
}

async function handleSubmit(event) {
  event.preventDefault();
  if (!refs.form) return;

  setStatus("");
  const formData = new FormData(refs.form);
  const payload = {
    role: "ambassador",
    firstName: String(formData.get("firstName") || "").trim(),
    lastName: String(formData.get("lastName") || "").trim(),
    sex: String(formData.get("sex") || "").trim(),
    phone: String(formData.get("phone") || "").trim(),
    fullAddress: String(formData.get("fullAddress") || "").trim(),
    currentPosition: String(formData.get("currentPosition") || "").trim(),
    networkReach: Number(formData.get("networkReach") || 0),
    motivationLetter: String(formData.get("motivationLetter") || "").trim(),
  };

  if (!payload.firstName || !payload.lastName || !payload.sex || !payload.phone || !payload.fullAddress || !payload.currentPosition || !payload.networkReach || !payload.motivationLetter) {
    setStatus("Complete tous les champs pour envoyer ta candidature.", "error");
    return;
  }

  if (payload.motivationLetter.length < 40) {
    setStatus("La lettre de motivation doit etre un peu plus detaillee.", "error");
    return;
  }

  pendingPayload = payload;
  openContractModal();
}

async function submitApplication(payload) {
  try {
    setSubmitting(true);
    const response = await submitRecruitmentApplicationSecure(payload || {});
    virtualProgress = Math.min(VIRTUAL_PROGRESS_MAX, virtualProgress + 0.02);
    paintProgress();
    refs.form.reset();
    updateMotivationCount();
    pendingPayload = null;
    setStatus(
      `Candidature envoyee avec succes. Reference: ${String(response?.applicationCode || "REC")}. Nous avons bien recu ton dossier pour le recrutement ambassadeur.`,
      "success",
    );
    window.scrollTo({ top: 0, behavior: "smooth" });
  } catch (error) {
    console.error("[RECRUITMENT] submit failed", error);
    if (String(error?.code || "") === "recruitment-application-exists" || String(error?.message || "").toLowerCase().includes("deja")) {
      setStatus("Une candidature existe deja pour ce numero. Contacte l'assistance si tu veux mettre ton dossier a jour.", "error");
    } else {
      setStatus(String(error?.message || "Impossible d'envoyer la candidature pour le moment."), "error");
    }
  } finally {
    setSubmitting(false);
  }
}

function markContractSeen() {
  try {
    window.localStorage.setItem(CONTRACT_SEEN_STORAGE_KEY, String(Date.now()));
  } catch (_) {
    // ignore storage failure
  }
}

function openContractModal() {
  if (!refs.contractModal) return;
  refs.contractModal.classList.add("is-open");
  refs.contractModal.setAttribute("aria-hidden", "false");
}

function closeContractModal() {
  if (!refs.contractModal) return;
  refs.contractModal.classList.remove("is-open");
  refs.contractModal.setAttribute("aria-hidden", "true");
}

async function confirmContractAndSubmit() {
  if (!pendingPayload) {
    closeContractModal();
    return;
  }
  markContractSeen();
  closeContractModal();
  await submitApplication(pendingPayload);
}

function handleContractModalBackdrop(event) {
  if (event.target === refs.contractModal) {
    closeContractModal();
  }
}

function cacheDom() {
  refs.form = document.getElementById("recruitmentForm");
  refs.status = document.getElementById("recruitmentStatus");
  refs.submitBtn = document.getElementById("recruitmentSubmitBtn");
  refs.progressFill = document.getElementById("recruitmentProgressFill");
  refs.deadlinePill = document.getElementById("recruitmentDeadlinePill");
  refs.motivation = document.getElementById("motivationLetter");
  refs.motivationCount = document.getElementById("motivationCount");
  refs.contractModal = document.getElementById("contractConfirmModal");
  refs.contractConfirmYes = document.getElementById("contractConfirmYes");
  refs.contractConfirmView = document.getElementById("contractConfirmView");
}

function bindEvents() {
  refs.form?.addEventListener("submit", handleSubmit);
  refs.motivation?.addEventListener("input", updateMotivationCount);
  refs.contractConfirmYes?.addEventListener("click", confirmContractAndSubmit);
  refs.contractConfirmView?.addEventListener("click", markContractSeen);
  refs.contractModal?.addEventListener("click", handleContractModalBackdrop);
  window.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && refs.contractModal?.classList.contains("is-open")) {
      closeContractModal();
    }
  });
}

async function trackVisit() {
  let alreadyTracked = false;
  try {
    alreadyTracked = window.sessionStorage.getItem(VISIT_SESSION_STORAGE_KEY) === "1";
  } catch (_) {
    alreadyTracked = false;
  }
  if (alreadyTracked) return;

  const sessionId = `rec_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
  try {
    await recordRecruitmentVisitSecure({ sessionId });
    try {
      window.sessionStorage.setItem(VISIT_SESSION_STORAGE_KEY, "1");
    } catch (_) {
      // ignore storage failure
    }
  } catch (error) {
    console.warn("[RECRUITMENT] visit tracking failed", error);
  }
}

function init() {
  cacheDom();
  bindEvents();
  updateMotivationCount();
  startProgressLoop();
  void trackVisit();
}

window.addEventListener("beforeunload", () => {
  if (progressTimer) window.clearInterval(progressTimer);
});

init();
