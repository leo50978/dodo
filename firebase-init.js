import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.5/firebase-app.js";
import {
  initializeAppCheck,
  ReCaptchaV3Provider,
} from "https://www.gstatic.com/firebasejs/10.12.5/firebase-app-check.js";
import {
  getAuth,
  GoogleAuthProvider,
  signInWithPopup,
  signInWithRedirect,
  getRedirectResult,
  onAuthStateChanged,
  signInWithEmailAndPassword,
  createUserWithEmailAndPassword,
  sendPasswordResetEmail,
  signOut,
  sendEmailVerification,
  reload,
  applyActionCode,
} from "https://www.gstatic.com/firebasejs/10.12.5/firebase-auth.js";
import {
  getFunctions,
  httpsCallable,
} from "https://www.gstatic.com/firebasejs/10.12.5/firebase-functions.js";
 import {
  getFirestore,
  collection,
  collectionGroup,
  addDoc,
  doc,
  getDoc,
  setDoc,
  updateDoc,
  runTransaction,
  serverTimestamp,
  query,
  where,
  orderBy,
  limit,
  getDocs,
  onSnapshot,
  deleteDoc,
  writeBatch,
  arrayUnion,
  increment,
} from "https://www.gstatic.com/firebasejs/10.12.5/firebase-firestore.js";
import {
  getStorage,
  ref as storageRef,
  uploadBytes,
  getDownloadURL,
  deleteObject,
} from "https://www.gstatic.com/firebasejs/10.12.5/firebase-storage.js";

const firebaseConfig = {
  apiKey: "AIzaSyCUCzCtv4ynd117Ny_kKg7SDbn4QbtxwqI",
  authDomain: "does-2c234.firebaseapp.com",
  projectId: "does-2c234",
  storageBucket: "does-2c234.firebasestorage.app",
  messagingSenderId: "885708540637",
  appId: "1:885708540637:web:4f0c51c97c0d07aba7e934",
  measurementId: "G-3D8KT7BD8J",
};

const app = initializeApp(firebaseConfig);
const auth = getAuth(app);
const db = getFirestore(app);
const functions = getFunctions(app);
const storage = getStorage(app);

function readAppCheckSiteKey() {
  const meta = typeof document !== "undefined"
    ? document.querySelector('meta[name="firebase-app-check-site-key"]')
    : null;
  const metaValue = meta?.getAttribute("content") || "";
  const globalValue = typeof window !== "undefined" ? String(window.__DOMINO_APPCHECK_SITE_KEY || "") : "";
  const picked = String(metaValue || globalValue || "").trim();
  if (!picked || picked === "REPLACE_WITH_RECAPTCHA_V3_SITE_KEY") return "";
  return picked;
}

function setupAppCheckDebugToken() {
  if (typeof window === "undefined") return;
  const debugToken = String(
    window.__DOMINO_APPCHECK_DEBUG_TOKEN ||
    window.localStorage?.getItem("domino_app_check_debug_token") ||
    ""
  ).trim();
  if (!debugToken) return;
  window.FIREBASE_APPCHECK_DEBUG_TOKEN = debugToken;
}

let appCheck = null;
try {
  setupAppCheckDebugToken();
  const siteKey = readAppCheckSiteKey();
  if (siteKey) {
    appCheck = initializeAppCheck(app, {
      provider: new ReCaptchaV3Provider(siteKey),
      isTokenAutoRefreshEnabled: true,
    });
  } else if (typeof console !== "undefined") {
    console.warn("[APP_CHECK] firebase-app-check-site-key manquant; App Check web inactif.");
  }
} catch (error) {
  if (typeof console !== "undefined") {
    console.warn("[APP_CHECK] initialisation échouée", error);
  }
}

export {
  app,
  appCheck,
  auth,
  db,
  functions,
  storage,
  httpsCallable,
  GoogleAuthProvider,
  signInWithPopup,
  signInWithRedirect,
  getRedirectResult,
  onAuthStateChanged,
  signInWithEmailAndPassword,
  createUserWithEmailAndPassword,
  sendPasswordResetEmail,
  signOut,
  sendEmailVerification,
  reload,
  applyActionCode,
  collection,
  collectionGroup,
  addDoc,
  doc,
  getDoc,
  setDoc,
  updateDoc,
  runTransaction,
  serverTimestamp,
  query,
  where,
  orderBy,
  limit,
  getDocs,
  onSnapshot,
  deleteDoc,
  writeBatch,
  arrayUnion,
  increment,
  storageRef,
  uploadBytes,
  getDownloadURL,
  deleteObject,
};
