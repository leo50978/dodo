import { auth, loginWithEmail, logoutCurrentUser, watchAuthState } from "./auth.js";
import { db, doc, getDoc } from "./firebase-init.js";

const FINANCE_ADMIN_EMAIL = "leovitch2004@gmail.com";
const BOOTSTRAP_DOC_ID = "dpayment_admin_bootstrap";

function normalizeEmail(value = "") {
  return String(value || "").trim().toLowerCase();
}

function ensureOverlay() {
  let overlay = document.getElementById("sharedDashboardAuthOverlay");
  if (overlay) return overlay;

  overlay = document.createElement("div");
  overlay.id = "sharedDashboardAuthOverlay";
  overlay.style.position = "fixed";
  overlay.style.inset = "0";
  overlay.style.zIndex = "6000";
  overlay.style.display = "none";
  overlay.style.alignItems = "center";
  overlay.style.justifyContent = "center";
  overlay.style.padding = "2rem 1.5rem";
  overlay.style.background = "rgba(2, 6, 23, 0.78)";
  overlay.style.backdropFilter = "blur(10px)";
  overlay.style.webkitBackdropFilter = "blur(10px)";
  document.body.appendChild(overlay);
  return overlay;
}

function renderOverlay({ title = "", description = "", lockedEmail = "", error = "", busy = false }) {
  const overlay = ensureOverlay();
  overlay.innerHTML = `
    <div style="width:100%;max-width:28rem;border-radius:1.5rem;border:1px solid rgba(255,255,255,0.12);background:rgba(2,6,23,0.92);padding:1.5rem;color:#fff;box-shadow:0 24px 80px rgba(0,0,0,0.55);font-family:inherit;">
      <div style="margin-bottom:1.25rem;">
        <p style="margin:0;font-size:0.72rem;font-weight:700;letter-spacing:0.16em;text-transform:uppercase;color:#67e8f9;">Connexion admin</p>
        <h2 style="margin:0.65rem 0 0;font-size:1.6rem;font-weight:700;">${escapeHtml(title || "Authentification")}</h2>
        <p style="margin:0.65rem 0 0;font-size:0.95rem;line-height:1.5;color:#cbd5e1;">${escapeHtml(description || "")}</p>
      </div>

      <form id="sharedDashboardAuthForm">
        <div style="display:grid;gap:0.35rem;margin-bottom:1rem;">
          <label style="font-size:0.9rem;font-weight:600;color:#e2e8f0;" for="sharedDashboardAuthEmail">Email administrateur</label>
          <input
            id="sharedDashboardAuthEmail"
            type="email"
            style="width:100%;border-radius:1rem;border:1px solid rgba(255,255,255,0.12);background:#0f172a;color:#fff;padding:0.9rem 1rem;outline:none;"
            value="${escapeHtml(lockedEmail)}"
            placeholder="admin@exemple.com"
            ${lockedEmail ? "readonly" : ""}
          />
        </div>

        <div style="display:grid;gap:0.35rem;margin-bottom:1rem;">
          <label style="font-size:0.9rem;font-weight:600;color:#e2e8f0;" for="sharedDashboardAuthPassword">Mot de passe</label>
          <input
            id="sharedDashboardAuthPassword"
            type="password"
            style="width:100%;border-radius:1rem;border:1px solid rgba(255,255,255,0.12);background:#0f172a;color:#fff;padding:0.9rem 1rem;outline:none;"
            placeholder="Votre mot de passe"
          />
        </div>

        <p style="min-height:1.25rem;margin:0 0 1rem;font-size:0.9rem;color:#fda4af;">${escapeHtml(error || "")}</p>

        <button
          id="sharedDashboardAuthSubmit"
          type="submit"
          style="width:100%;border:none;border-radius:1rem;background:#06b6d4;color:#082f49;padding:0.9rem 1rem;font-weight:700;cursor:${busy ? "not-allowed" : "pointer"};opacity:${busy ? "0.6" : "1"};"
          ${busy ? "disabled" : ""}
        >${busy ? "Connexion..." : "Se connecter"}</button>
      </form>
    </div>
  `;
  overlay.style.display = "flex";
  return overlay;
}

function hideOverlay() {
  const overlay = document.getElementById("sharedDashboardAuthOverlay");
  if (!overlay) return;
  overlay.style.display = "none";
}

function escapeHtml(value = "") {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

async function waitForFirstAuthUser() {
  return new Promise((resolve) => {
    const unsubscribe = watchAuthState((user) => {
      unsubscribe();
      resolve(user || null);
    });

    window.setTimeout(() => {
      try {
        unsubscribe();
      } catch (_) {}
      resolve(auth.currentUser || null);
    }, 800);
  });
}

async function loadBootstrapEmail() {
  try {
    const snap = await getDoc(doc(db, "settings", BOOTSTRAP_DOC_ID));
    if (!snap.exists()) return "";
    return normalizeEmail(snap.data()?.email || "");
  } catch (_) {
    return "";
  }
}

function isAllowedAdminUser(user, expectedEmail = "") {
  const email = normalizeEmail(user?.email || "");
  if (!email) return false;
  const target = normalizeEmail(expectedEmail || FINANCE_ADMIN_EMAIL);
  return email === target;
}

async function promptAdminLogin(expectedEmail = "", context = {}) {
  return new Promise((resolve, reject) => {
    const contextTitle = context.title || "Dashboard";
    const description = context.description
      || "Connecte-toi avec le compte administrateur autorisé pour accéder à ce dashboard.";

    const render = (error = "", busy = false) => {
      const overlay = renderOverlay({
        title: contextTitle,
        description,
        lockedEmail: expectedEmail || FINANCE_ADMIN_EMAIL,
        error,
        busy,
      });

      overlay.querySelector("#sharedDashboardAuthForm")?.addEventListener("submit", async (event) => {
        event.preventDefault();
        if (busy) return;

        const email = normalizeEmail(
          overlay.querySelector("#sharedDashboardAuthEmail")?.value || expectedEmail || FINANCE_ADMIN_EMAIL
        );
        const password = String(overlay.querySelector("#sharedDashboardAuthPassword")?.value || "");

        if (!email || !password) {
          render("Entre l'email et le mot de passe.");
          return;
        }

        render("", true);
        try {
          const credentials = await loginWithEmail(email, password);
          hideOverlay();
          resolve(credentials.user);
        } catch (error) {
          render(error?.message || "Connexion impossible.");
        }
      });
    };

    try {
      render();
    } catch (error) {
      reject(error);
    }
  });
}

export async function ensureFinanceDashboardSession(options = {}) {
  const expectedEmail = normalizeEmail((await loadBootstrapEmail()) || FINANCE_ADMIN_EMAIL);
  let user = auth.currentUser || await waitForFirstAuthUser();

  if (user && !isAllowedAdminUser(user, expectedEmail)) {
    await logoutCurrentUser().catch(() => {});
    user = null;
  }

  if (!user) {
    user = await promptAdminLogin(expectedEmail, options);
  }

  if (!isAllowedAdminUser(user, expectedEmail)) {
    await logoutCurrentUser().catch(() => {});
    throw new Error("Ce compte n'est pas autorisé pour ce dashboard.");
  }

  hideOverlay();
  return user;
}
