import {
  auth,
  GoogleAuthProvider,
  onAuthStateChanged,
  signInWithPopup,
} from "./firebase-init.js";
import { adminCheckSecure } from "../secure-functions.js";

function waitForAuthUser(timeoutMs = 10000) {
  return new Promise((resolve) => {
    let settled = false;
    const timer = window.setTimeout(() => {
      if (settled) return;
      settled = true;
      unsubscribe();
      resolve(auth.currentUser || null);
    }, Math.max(1500, Number(timeoutMs) || 10000));

    const unsubscribe = onAuthStateChanged(auth, (user) => {
      if (settled) return;
      settled = true;
      window.clearTimeout(timer);
      unsubscribe();
      resolve(user || null);
    });
  });
}

export async function ensureFinanceDashboardSession({ fallbackUrl = "../auth.html" } = {}) {
  let user = auth.currentUser || await waitForAuthUser();
  if (!user) {
    try {
      const provider = new GoogleAuthProvider();
      const popupResult = await signInWithPopup(auth, provider);
      user = popupResult?.user || auth.currentUser || null;
    } catch (_) {}
  }

  if (!user) {
    window.location.href = fallbackUrl;
    throw new Error("auth-required");
  }

  try {
    await adminCheckSecure({});
    return user;
  } catch (error) {
    console.error("[DASHBOARD_AUTH] accès refusé", error);
    window.location.href = fallbackUrl;
    throw error;
  }
}
