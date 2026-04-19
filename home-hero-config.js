import { db, doc, getDoc } from "./firebase-init.js";

const HOME_HERO_CONFIG_CACHE_KEY = "dl_home_hero_config_v3";
const HOME_HERO_CONFIG_VERSION = "hhs-v2";

const DEFAULT_HOME_HERO_SLIDES = Object.freeze([
  Object.freeze({ name: "hero.jpg", enabled: true, sortOrder: 10 }),
]);

let HERO_CACHE = DEFAULT_HOME_HERO_SLIDES.map((slide) => ({ ...slide }));
let HERO_VERSION = HOME_HERO_CONFIG_VERSION;
let HERO_UPDATED_AT_MS = 0;
let LOAD_PROMISE = null;

function normalizeHeroName(value = "", fallback = "") {
  const cleaned = String(value || "")
    .trim()
    .replace(/^https?:\/\/[^/]+/i, "")
    .replace(/^\/+/, "");
  return String(cleaned || fallback || "").trim();
}

function normalizeSlides(rawSlides = []) {
  const source = Array.isArray(rawSlides) && rawSlides.length ? rawSlides : DEFAULT_HOME_HERO_SLIDES;
  const out = [];
  const usedNames = new Set();

  source.forEach((raw, index) => {
    const rawName = typeof raw === "string"
      ? raw
      : raw?.name || raw?.src || raw?.file || raw?.image || "";
    const name = normalizeHeroName(rawName, "");
    if (!name) return;

    const key = name.toLowerCase();
    if (usedNames.has(key)) return;
    usedNames.add(key);

    const sortOrderRaw = Number(raw?.sortOrder);
    const sortOrder = Number.isFinite(sortOrderRaw) ? Math.trunc(sortOrderRaw) : ((index + 1) * 10);
    const enabled = raw?.enabled === undefined ? true : raw?.enabled === true;

    out.push({
      name,
      enabled,
      sortOrder,
    });
  });

  out.sort((left, right) => {
    if (left.sortOrder !== right.sortOrder) return left.sortOrder - right.sortOrder;
    return left.name.localeCompare(right.name);
  });

  return out;
}

function saveLocalCache() {
  try {
    window.localStorage.setItem(
      HOME_HERO_CONFIG_CACHE_KEY,
      JSON.stringify({
        version: HERO_VERSION,
        updatedAtMs: HERO_UPDATED_AT_MS,
        slides: HERO_CACHE,
      })
    );
  } catch (_) {}
}

function hydrateFromLocalCache() {
  try {
    const raw = window.localStorage.getItem(HOME_HERO_CONFIG_CACHE_KEY);
    if (!raw) return;
    const parsed = JSON.parse(raw);
    HERO_CACHE = normalizeSlides(parsed?.slides || []);
    HERO_VERSION = String(parsed?.version || HOME_HERO_CONFIG_VERSION);
    HERO_UPDATED_AT_MS = Number(parsed?.updatedAtMs) || 0;
  } catch (_) {}
}

function buildHeroSrc(name = "") {
  const cleanName = normalizeHeroName(name, "");
  return cleanName ? `/${cleanName}` : "";
}

export function getHomeHeroSlidesSnapshot() {
  return {
    slides: HERO_CACHE.map((slide) => ({ ...slide })),
    version: HERO_VERSION,
    updatedAtMs: HERO_UPDATED_AT_MS,
  };
}

export function getHomeHeroImageUrls() {
  return HERO_CACHE
    .filter((slide) => slide.enabled === true)
    .map((slide) => buildHeroSrc(slide.name))
    .filter(Boolean);
}

export function buildHomeHeroImagePath(name = "") {
  return buildHeroSrc(name);
}

export function normalizeHomeHeroSlides(rawSlides = []) {
  return normalizeSlides(rawSlides);
}

export async function refreshHomeHeroSlides(force = false) {
  if (!force && LOAD_PROMISE) return LOAD_PROMISE;

  LOAD_PROMISE = (async () => {
    try {
      const snap = await getDoc(doc(db, "settings", "home_hero_slides_v1"));
      const data = snap.exists() ? (snap.data() || {}) : {};
      HERO_CACHE = normalizeSlides(data.slides || data.images || data.items || []);
      HERO_VERSION = String(data.version || HOME_HERO_CONFIG_VERSION);
      HERO_UPDATED_AT_MS = Number(data.updatedAtMs) || 0;
      saveLocalCache();
      return getHomeHeroSlidesSnapshot();
    } catch (error) {
      return getHomeHeroSlidesSnapshot();
    } finally {
      LOAD_PROMISE = null;
    }
  })();

  return LOAD_PROMISE;
}

hydrateFromLocalCache();

export { DEFAULT_HOME_HERO_SLIDES, HOME_HERO_CONFIG_VERSION };
