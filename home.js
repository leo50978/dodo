import "./firebase-init.js";
import { auth, watchAuthState } from "./auth.js";
import {
  buildHomeHeroImagePath,
  refreshHomeHeroSlides,
} from "./home-hero-config.js?v=home-hero-v3";

const AUTH_SUCCESS_NOTICE_STORAGE_KEY = "domino_auth_success_notice_v1";
let lastRenderedStateKey = "__initial__";
let homeAuthBootstrapTimer = null;
let homeInitialAuthResolved = false;
let homeRenderToken = 0;
let page2ModulePromise = null;
let pwaSupportModulePromise = null;
const HOME_AUTH_BOOTSTRAP_TIMEOUT_MS = 900;
const HOME_AUTH_SUCCESS_TIMEOUT_MS = 2600;
const HOME_HERO_ROTATION_MS = 5000;
let homeHeroRotationTimer = null;
const HOME_HERO_FALLBACK_SLIDES = [
  { name: "hero.jpg", alt: "Interface Dominoes Lakay" },
];
const HOME_DEBUG_VERSION = "home-v4";
const PAGE2_BOOTSTRAP_MODULE_URL = "./page2.js?v=page2-hero-v5";

console.info("[DLK_BOOTSTRAP][HOME] module:load", {
  version: HOME_DEBUG_VERSION,
  href: String(window.location?.href || ""),
  buildHint: String(document.currentScript?.src || ""),
  page2ModuleUrl: PAGE2_BOOTSTRAP_MODULE_URL,
});

window.addEventListener("error", (event) => {
  console.error("[DLK_BOOTSTRAP][HOME] window:error", {
    message: String(event?.message || ""),
    filename: String(event?.filename || ""),
    lineno: Number(event?.lineno || 0),
    colno: Number(event?.colno || 0),
    version: HOME_DEBUG_VERSION,
  });
});

window.addEventListener("unhandledrejection", (event) => {
  console.error("[DLK_BOOTSTRAP][HOME] window:unhandledrejection", {
    reason: event?.reason || null,
    version: HOME_DEBUG_VERSION,
  });
});

function homeDebug(event, data = {}) {
  try {
    console.log(`[AUTH_DEBUG][HOME] ${event}`, {
      ts: new Date().toISOString(),
      href: String(window.location?.href || ""),
      currentUid: String(auth.currentUser?.uid || ""),
      ...data,
    });
  } catch (_) {}
}

function readRecentAuthSuccessNotice() {
  try {
    const raw = sessionStorage.getItem(AUTH_SUCCESS_NOTICE_STORAGE_KEY) || "";
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    const ts = Number(parsed?.ts || 0);
    if (!Number.isFinite(ts) || ts <= 0) return null;
    if ((Date.now() - ts) > 60_000) return null;
    return parsed;
  } catch (_) {
    return null;
  }
}

function getHomeShell() {
  return document.getElementById("domino-app-shell") || document.body;
}

function stopHomeHeroRotation() {
  if (!homeHeroRotationTimer) return;
  window.clearInterval(homeHeroRotationTimer);
  homeHeroRotationTimer = null;
}

function normalizeHeroPath(value = "") {
  return String(value || "").trim().replace(/^https?:\/\/[^/]+/i, "").replace(/^\/+/, "");
}

function buildHomeHeroSlides(rawSlides = HOME_HERO_FALLBACK_SLIDES) {
  const track = document.querySelector("[data-home-hero-track]");
  if (!track) return [];

  const slides = Array.isArray(rawSlides) ? rawSlides : HOME_HERO_FALLBACK_SLIDES;
  track.replaceChildren();

  slides.forEach((entry, index) => {
    const source = normalizeHeroPath(buildHomeHeroImagePath(entry?.name || entry?.src || ""));
    if (!source) return;

    const slide = document.createElement("div");
    slide.className = "home-shell__hero-slide";
    slide.setAttribute("data-home-hero-slide", "");
    if (index === 0) slide.classList.add("is-active");
    slide.innerHTML = `
      <img
        src="${source}"
        alt="${String(entry?.alt || `Interface Dominoes Lakay hero ${index + 1}`)}"
        width="600"
        height="600"
        fetchpriority="${index === 0 ? "high" : "auto"}"
        decoding="async"
      />
    `;
    track.appendChild(slide);
  });

  return Array.from(track.querySelectorAll("[data-home-hero-slide]"));
}

function initHomeHeroRotation() {
  const slides = Array.from(document.querySelectorAll("[data-home-hero-slide]"));
  stopHomeHeroRotation();
  if (slides.length === 0) return;

  let activeIndex = slides.findIndex((slide) => slide.classList.contains("is-active"));
  if (activeIndex < 0) activeIndex = 0;

  const renderActiveSlide = () => {
    slides.forEach((slide, index) => {
      slide.classList.toggle("is-active", index === activeIndex);
    });
  };

  renderActiveSlide();
  if (slides.length === 1) return;

  homeHeroRotationTimer = window.setInterval(() => {
    activeIndex = (activeIndex + 1) % slides.length;
    renderActiveSlide();
  }, HOME_HERO_ROTATION_MS);
}

async function refreshHomeHeroRotation() {
  try {
    const snapshot = await refreshHomeHeroSlides();
    const enabledSlides = Array.isArray(snapshot?.slides)
      ? snapshot.slides.filter((slide) => slide && slide.enabled === true)
      : [];
    buildHomeHeroSlides(enabledSlides.length ? enabledSlides : HOME_HERO_FALLBACK_SLIDES);
  } catch (error) {
    console.warn("[AUTH_DEBUG][HOME] hero config refresh failed", error);
    buildHomeHeroSlides(HOME_HERO_FALLBACK_SLIDES);
  }
  initHomeHeroRotation();
}

function ensureHomeLoadingOverlay() {
  const shell = getHomeShell();
  let overlay = document.getElementById("homeBootstrapOverlay");
  if (overlay && shell.contains(overlay)) return overlay;

  overlay = document.createElement("div");
  overlay.id = "homeBootstrapOverlay";
  overlay.className = "fixed inset-0 z-[3600] hidden items-center justify-center bg-[#3F4766]/74 px-5 text-white backdrop-blur-md";
  overlay.innerHTML = `
    <div class="rounded-3xl border border-white/15 bg-white/10 px-6 py-5 text-center shadow-[12px_12px_28px_rgba(25,30,44,0.42),-10px_-10px_24px_rgba(97,110,150,0.16)]">
      <div class="mx-auto mb-3 h-10 w-10 animate-spin rounded-full border-2 border-white/20 border-t-[#ffb26e]"></div>
      <div id="homeBootstrapOverlayMessage" class="text-sm font-semibold tracking-wide">Chargement...</div>
    </div>
  `;
  shell.appendChild(overlay);
  return overlay;
}

function showHomeLoadingOverlay(message = "Chargement...") {
  const overlay = ensureHomeLoadingOverlay();
  const label = document.getElementById("homeBootstrapOverlayMessage");
  if (label) label.textContent = String(message || "Chargement...");
  overlay.classList.remove("hidden");
  overlay.classList.add("flex");
}

function hideHomeLoadingOverlay() {
  const overlay = document.getElementById("homeBootstrapOverlay");
  if (!overlay) return;
  overlay.classList.add("hidden");
  overlay.classList.remove("flex");
}

async function ensurePage2Module() {
  if (!page2ModulePromise) {
    console.info("[DLK_BOOTSTRAP][HOME] page2:import:start", {
      url: PAGE2_BOOTSTRAP_MODULE_URL,
      version: HOME_DEBUG_VERSION,
    });
    page2ModulePromise = import(PAGE2_BOOTSTRAP_MODULE_URL);
  }
  return page2ModulePromise;
}

function warmPage2ModuleSoon() {
  const warm = () => {
    window.setTimeout(() => {
      void ensurePage2Module();
    }, 120);
  };
  if ("requestAnimationFrame" in window) {
    window.requestAnimationFrame(warm);
    return;
  }
  warm();
}

async function registerPwaSupportWhenIdle() {
  if (!pwaSupportModulePromise) {
    pwaSupportModulePromise = import("./pwa-install.js");
  }
  const { registerPwaSupport } = await pwaSupportModulePromise;
  registerPwaSupport();
}

function schedulePwaSupportRegistration() {
  const run = () => {
    if ("requestIdleCallback" in window) {
      window.requestIdleCallback(() => {
        void registerPwaSupportWhenIdle();
      }, { timeout: 1600 });
      return;
    }
    window.setTimeout(() => {
      void registerPwaSupportWhenIdle();
    }, 280);
  };

  if (document.readyState === "complete") {
    run();
    return;
  }

  window.addEventListener("load", run, { once: true });
}

async function renderHomeFromAuth(user, options = {}) {
  const uid = String(user?.uid || "");
  const optimistic = options?.optimistic === true;
  const stateKey = `${uid}|${optimistic ? "1" : "0"}`;
  homeDebug("renderHomeFromAuth:enter", {
    uid,
    optimistic,
    stateKey,
    lastRenderedStateKey,
  });
  if (!options?.force && stateKey === lastRenderedStateKey) return;
  lastRenderedStateKey = stateKey;
  const renderToken = ++homeRenderToken;
  homeDebug("renderHomeFromAuth:loadPage2", { uid, optimistic, renderToken, page2ModuleUrl: PAGE2_BOOTSTRAP_MODULE_URL });
  try {
    const { renderPage2 } = await ensurePage2Module();
    if (renderToken !== homeRenderToken) {
      homeDebug("renderHomeFromAuth:staleRenderAbort", { uid, optimistic, renderToken });
      return;
    }
    hideHomeLoadingOverlay();
    console.info("[DLK_BOOTSTRAP][HOME] page2:import:success", {
      renderToken,
      uid,
      optimistic,
      version: HOME_DEBUG_VERSION,
    });
    homeDebug("renderHomeFromAuth:renderPage2", { uid, optimistic, renderToken });
    renderPage2(user || null, { optimisticAuth: optimistic });
  } catch (error) {
    console.error("[DLK_BOOTSTRAP][HOME] page2:import:failed", {
      renderToken,
      uid,
      optimistic,
      version: HOME_DEBUG_VERSION,
      name: String(error?.name || ""),
      message: String(error?.message || ""),
      stack: String(error?.stack || ""),
    });
    hideHomeLoadingOverlay();
    const shell = getHomeShell();
    if (shell && !document.getElementById("homeBootstrapFallback")) {
      const fallback = document.createElement("div");
      fallback.id = "homeBootstrapFallback";
      fallback.style.cssText = [
        "position:fixed",
        "inset:0",
        "z-index:3600",
        "display:flex",
        "align-items:center",
        "justify-content:center",
        "padding:24px",
        "background:rgba(15,23,42,.92)",
        "color:white",
        "backdrop-filter:blur(10px)",
      ].join(";");
      fallback.innerHTML = `
        <div style="max-width:560px;width:100%;border:1px solid rgba(255,255,255,.12);border-radius:24px;padding:24px;background:rgba(255,255,255,.06);box-shadow:0 24px 60px rgba(0,0,0,.35);">
          <div style="font-size:1.05rem;font-weight:800;margin-bottom:10px;">Chargement temporairement indisponible</div>
          <div style="opacity:.82;line-height:1.55;">La page compte n'a pas pu se préparer correctement. Recharge la page, ou contacte le support si le problème persiste.</div>
        </div>
      `;
      shell.appendChild(fallback);
    }
  }
}

function renderHomeLoading(message = "Chargement...") {
  showHomeLoadingOverlay(message);
}

function clearHomeAuthBootstrapTimer() {
  if (!homeAuthBootstrapTimer) return;
  window.clearTimeout(homeAuthBootstrapTimer);
  homeAuthBootstrapTimer = null;
}

homeDebug("bootstrap:start");
schedulePwaSupportRegistration();
warmPage2ModuleSoon();
void refreshHomeHeroRotation();
const immediateUser = auth.currentUser || null;
if (immediateUser?.uid) {
  homeDebug("bootstrap:currentUserImmediate", { uid: String(immediateUser.uid || "") });
  homeInitialAuthResolved = true;
  renderHomeLoading("Préparation de votre espace...");
  void renderHomeFromAuth(immediateUser, { optimistic: false });
} else {
  const successNotice = readRecentAuthSuccessNotice();
  if (successNotice) {
    homeDebug("bootstrap:optimisticAuthRender", { successType: String(successNotice?.type || "") });
    renderHomeLoading("Connexion réussie. Préparation de votre espace...");
  } else {
    homeDebug("bootstrap:waitFirstAuthState");
    hideHomeLoadingOverlay();
  }
  homeAuthBootstrapTimer = window.setTimeout(() => {
    homeAuthBootstrapTimer = null;
    if (homeInitialAuthResolved) return;
    homeDebug("bootstrap:timeoutRenderGuest", { timeoutMs: HOME_AUTH_BOOTSTRAP_TIMEOUT_MS });
    homeInitialAuthResolved = true;
    void renderHomeFromAuth(null, { optimistic: false, force: true });
  }, successNotice ? HOME_AUTH_SUCCESS_TIMEOUT_MS : HOME_AUTH_BOOTSTRAP_TIMEOUT_MS);
}

watchAuthState((user) => {
  homeInitialAuthResolved = true;
  clearHomeAuthBootstrapTimer();
  homeDebug("watchAuthState:callback", {
    hasUser: Boolean(user),
    uid: String(user?.uid || ""),
  });
  if (user?.uid) {
    renderHomeLoading("Préparation de votre espace...");
  }
  void renderHomeFromAuth(user || null, { optimistic: false });
});
