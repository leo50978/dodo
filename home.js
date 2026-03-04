import "./firebase-init.js";
import { auth, watchAuthState } from "./auth.js";
import { renderPage2 } from "./page2.js";

let lastRenderedAuthUid = "__initial__";

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

function renderHomeFromAuth(user) {
  const uid = String(user?.uid || "");
  homeDebug("renderHomeFromAuth:enter", {
    uid,
    lastRenderedAuthUid,
  });
  if (uid === lastRenderedAuthUid) return;
  lastRenderedAuthUid = uid;
  homeDebug("renderHomeFromAuth:renderPage2", { uid });
  renderPage2(user || null);
}

homeDebug("bootstrap:start");
renderHomeFromAuth(auth.currentUser || null);

watchAuthState((user) => {
  homeDebug("watchAuthState:callback", {
    hasUser: Boolean(user),
    uid: String(user?.uid || ""),
  });
  renderHomeFromAuth(user || null);
});
