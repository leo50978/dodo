const { onCall, onRequest, HttpsError } = require("firebase-functions/v2/https");
const { setGlobalOptions } = require("firebase-functions/v2");
const { onSchedule } = require("firebase-functions/v2/scheduler");
const { onDocumentCreated } = require("firebase-functions/v2/firestore");
const admin = require("firebase-admin");
const crypto = require("node:crypto");
const webpush = require("web-push");

// Avoid metadata-server lookups when project env vars are missing during local analysis/deploy
if (!process.env.FIREBASE_CONFIG) {
  const projectId = process.env.GCLOUD_PROJECT || process.env.GOOGLE_CLOUD_PROJECT || process.env.PROJECT_ID || "";
  if (projectId) {
    process.env.FIREBASE_CONFIG = JSON.stringify({ projectId });
  }
}

admin.initializeApp();
const db = admin.firestore();

setGlobalOptions({
  region: "us-central1",
  maxInstances: 10,
});

const ROOMS_COLLECTION = "rooms";
const GAME_STATES_COLLECTION = "gameStates";
const CLIENTS_COLLECTION = "clients";
const AMBASSADORS_COLLECTION = "ambassadors";
const AMBASSADOR_EVENTS_COLLECTION = "ambassadorGameEvents";
const AMBASSADOR_PRIVATE_SUBCOLLECTION = "private";
const AMBASSADOR_SECRETS_DOC = "credentials";
const CHAT_COLLECTION = "globalChannelMessages";
const SUPPORT_THREADS_COLLECTION = "supportThreads";
const SUPPORT_MESSAGES_SUBCOLLECTION = "messages";
const DASHBOARD_PUSH_SUBSCRIPTIONS_COLLECTION = "dashboardPushSubscriptions";
const MATCHMAKING_POOLS_COLLECTION = "matchmakingPools";
const ANALYTICS_META_COLLECTION = "analyticsMeta";
const ANALYTICS_PRESENCE_SNAPSHOTS_COLLECTION = "analyticsPresenceSnapshots";
const ANALYTICS_PRESENCE_DAILY_COLLECTION = "analyticsPresenceDaily";
const ANALYTICS_PRESENCE_MONTHLY_COLLECTION = "analyticsPresenceMonthly";
const ANALYTICS_PRESENCE_HOUR_COLLECTION = "analyticsPresenceHours";
const ANALYTICS_PRESENCE_WEEKDAY_COLLECTION = "analyticsPresenceWeekdays";

const RATE_HTG_TO_DOES = 20;
const DEFAULT_STAKE_REWARD_MULTIPLIER = 3;
const USER_REFERRAL_DEPOSIT_REWARD = 100;
const FINANCE_ADMIN_EMAIL = "leovitch2004@gmail.com";
const MIN_ORDER_HTG = 25;
const MIN_WITHDRAWAL_HTG = 50;
const MAX_WITHDRAWAL_HTG = 500000;
const MAX_PUBLIC_TEXT_LENGTH = 500;
const USER_REFERRAL_PREFIX = "USR";
const AMBASSADOR_LOSS_BONUS = 50;
const AMBASSADOR_WIN_PENALTY = 75;
const AMBASSADOR_PROMO_PREFIX = "AMB";
const AMBASSADOR_LINK_PREFIX = "AML";
const AMBASSADOR_SYSTEM_ENABLED = false;
const AUTH_HASH_ALGO = "scrypt_v1";
const AUTH_HASH_SALT_BYTES = 16;
const AUTH_HASH_KEYLEN = 64;
const AMBASSADOR_CODE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
const DISCUSSION_MESSAGE_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;
const DISCUSSION_PURGE_BATCH_SIZE = 200;
const DISCUSSION_MESSAGES_FETCH_LIMIT = 250;
const DEFAULT_PUBLIC_SETTINGS = Object.freeze({
  verificationHours: 12,
  expiredMessage: "Le délai de vérification est dépassé. Contactez le support.",
  provisionalDepositsEnabled: true,
});
const DPAYMENT_ADMIN_BOOTSTRAP_DOC = "dpayment_admin_bootstrap";
const APP_PUBLIC_SETTINGS_DOC = "public_app_settings";
const DASHBOARD_DEFAULT_NOTIFICATION_URL = "./Dpayment.html";
const DEFAULT_GAME_STAKE_OPTIONS = Object.freeze([
  Object.freeze({ stakeDoes: 100, enabled: true, sortOrder: 10 }),
  Object.freeze({ stakeDoes: 500, enabled: false, sortOrder: 20 }),
  Object.freeze({ stakeDoes: 1000, enabled: false, sortOrder: 30 }),
  Object.freeze({ stakeDoes: 5000, enabled: false, sortOrder: 40 }),
]);
const DEFAULT_BOT_DIFFICULTY = "expert";
const ROOM_WAIT_MS = 15 * 1000;
const ROOM_DISCONNECT_TAKEOVER_MS = 5 * 1000;
const ROOM_DISCONNECT_GRACE_MS = 15 * 1000;
const BOT_THINK_DELAY_MIN_MS = 1400;
const BOT_THINK_DELAY_MAX_MS = 2600;
const BOT_THINK_DELAY_PASS_MIN_MS = 900;
const BOT_THINK_DELAY_PASS_MAX_MS = 1700;
const BOT_DIFFICULTY_LEVELS = new Set(["amateur", "expert", "ultra", "userpro"]);
const BOT_DIFFICULTY_LOOKAHEAD = Object.freeze({
  amateur: 0,
  expert: 3,
  ultra: 5,
  userpro: 0,
});
const USER_TOURNAMENTS_COLLECTION = "userTournaments";
const USER_TOURNAMENT_SLOT_COUNT = 5;
const USER_TOURNAMENT_DAILY_LIMIT = 3;
const USER_TOURNAMENT_DURATION_MS = 15 * 60 * 1000;
const USER_TOURNAMENT_WARMUP_MS = 60 * 1000;
const USER_TOURNAMENT_OBSERVER_IDLE_MS = 5 * 60 * 1000;
const USER_TOURNAMENT_ACTIVITY_PROBE_MS = 30 * 1000;
const USER_TOURNAMENT_OBSERVER_TICK_MS = 20 * 1000;
const USER_TOURNAMENT_MIN_BOTS = 11; // user + 11 = 12 players
const USER_TOURNAMENT_MAX_BOTS = 19; // user + 19 = 20 players
const USER_TOURNAMENT_WIN_REWARD_DOES = 10000;
const SHARE_SITE_PROMO_DOC = "shareSiteV1";
const SHARE_SITE_PROMO_TARGET = 5;
const SHARE_SITE_PROMO_REWARD_DOES = 100;
const SHARE_SITE_PROMO_COOLDOWN_MS = 3 * 24 * 60 * 60 * 1000;
const SHARE_SITE_PROMO_ACTION_CACHE = 30;
const PROVISIONAL_FUNDING_VERSION = 2;
const PROVISIONAL_CREDIT_MODE = "provisional";
const ACCOUNT_FREEZE_REJECT_THRESHOLD = 3;
const PRESENCE_ANALYTICS_TIMEZONE = "America/Port-au-Prince";
const PRESENCE_ANALYTICS_CLIENT_WINDOW_MS = 2 * 60 * 1000;
const PRESENCE_ANALYTICS_ROOM_WINDOW_MS = 20 * 1000;
const PRESENCE_ANALYTICS_BUCKET_MS = 5 * 60 * 1000;
const PRESENCE_ANALYTICS_SNAPSHOT_RETENTION_MS = 14 * 24 * 60 * 60 * 1000;
const PRESENCE_ANALYTICS_RECENT_SNAPSHOT_DAYS = 7;
const PRESENCE_ANALYTICS_RECENT_DAYS_LIMIT = 120;
const PRESENCE_ANALYTICS_RECENT_MONTHS_LIMIT = 18;
const TILE_VALUES = Object.freeze([
  [0, 0], [0, 1], [0, 2], [0, 3], [0, 4], [0, 5], [0, 6],
  [1, 1], [1, 2], [1, 3], [1, 4], [1, 5], [1, 6],
  [2, 2], [2, 3], [2, 4], [2, 5], [2, 6],
  [3, 3], [3, 4], [3, 5], [3, 6],
  [4, 4], [4, 5], [4, 6],
  [5, 5], [5, 6],
  [6, 6],
]);

function isEmulator() {
  return process.env.FUNCTIONS_EMULATOR === "true";
}

function shouldEnforceAppCheck() {
  return process.env.ENFORCE_APP_CHECK === "true";
}

const presenceDateTimeFormatter = new Intl.DateTimeFormat("en-CA", {
  timeZone: PRESENCE_ANALYTICS_TIMEZONE,
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  hourCycle: "h23",
});

const presenceWeekdayFormatter = new Intl.DateTimeFormat("en-US", {
  timeZone: PRESENCE_ANALYTICS_TIMEZONE,
  weekday: "short",
});

function analyticsMetaRef(docId = "") {
  return db.collection(ANALYTICS_META_COLLECTION).doc(String(docId || "").trim());
}

function presenceSnapshotsCollection() {
  return db.collection(ANALYTICS_PRESENCE_SNAPSHOTS_COLLECTION);
}

function presenceDailyCollection() {
  return db.collection(ANALYTICS_PRESENCE_DAILY_COLLECTION);
}

function presenceMonthlyCollection() {
  return db.collection(ANALYTICS_PRESENCE_MONTHLY_COLLECTION);
}

function presenceHourCollection() {
  return db.collection(ANALYTICS_PRESENCE_HOUR_COLLECTION);
}

function presenceWeekdayCollection() {
  return db.collection(ANALYTICS_PRESENCE_WEEKDAY_COLLECTION);
}

function getPresenceBucketStartMs(nowMs = Date.now()) {
  const safeNow = safeSignedInt(nowMs) || Date.now();
  return safeNow - (safeNow % PRESENCE_ANALYTICS_BUCKET_MS);
}

function getPresenceLocalKeys(nowMs = Date.now()) {
  const parts = presenceDateTimeFormatter.formatToParts(new Date(nowMs));
  const values = {};
  parts.forEach((part) => {
    if (part.type !== "literal") {
      values[part.type] = part.value;
    }
  });
  const year = String(values.year || "0000");
  const month = String(values.month || "01");
  const day = String(values.day || "01");
  let hour = String(values.hour || "00");
  if (hour === "24") hour = "00";
  const weekday = String(presenceWeekdayFormatter.format(new Date(nowMs)) || "Sun").toLowerCase();
  return {
    timezone: PRESENCE_ANALYTICS_TIMEZONE,
    dayKey: `${year}-${month}-${day}`,
    monthKey: `${year}-${month}`,
    hourKey: hour.padStart(2, "0"),
    weekdayKey: weekday,
  };
}

function getTournamentQuotaLocalKeys(nowMs = Date.now()) {
  return getPresenceLocalKeys(nowMs);
}

function getTournamentNextResetMs(nowMs = Date.now()) {
  const safeNow = safeSignedInt(nowMs) || Date.now();
  const currentDayKey = getTournamentQuotaLocalKeys(safeNow).dayKey;
  let high = safeNow + (60 * 60 * 1000);

  while (getTournamentQuotaLocalKeys(high).dayKey === currentDayKey) {
    high += 60 * 60 * 1000;
  }

  let low = safeNow;
  while ((high - low) > 1000) {
    const mid = Math.floor((low + high) / 2);
    if (getTournamentQuotaLocalKeys(mid).dayKey === currentDayKey) {
      low = mid + 1;
    } else {
      high = mid;
    }
  }

  return high;
}

function normalizeTournamentDailyQuota(rawQuota = {}, nowMs = Date.now()) {
  const localKeys = getTournamentQuotaLocalKeys(nowMs);
  const storedDayKey = String(rawQuota?.dayKey || "").trim();
  const nextResetMs = getTournamentNextResetMs(nowMs);

  if (storedDayKey !== localKeys.dayKey) {
    return {
      dayKey: localKeys.dayKey,
      playsUsed: 0,
      maxPlays: USER_TOURNAMENT_DAILY_LIMIT,
      nextResetMs,
    };
  }

  return {
    dayKey: localKeys.dayKey,
    playsUsed: clamp(safeInt(rawQuota?.playsUsed), 0, USER_TOURNAMENT_DAILY_LIMIT),
    maxPlays: USER_TOURNAMENT_DAILY_LIMIT,
    nextResetMs,
  };
}

function buildTournamentQuotaPayload(quota, hasActiveSession = false) {
  const maxPlays = clamp(safeInt(quota?.maxPlays), 1, USER_TOURNAMENT_DAILY_LIMIT) || USER_TOURNAMENT_DAILY_LIMIT;
  const playsUsedToday = clamp(safeInt(quota?.playsUsed), 0, maxPlays);
  const playsRemainingToday = Math.max(0, maxPlays - playsUsedToday);
  const nextResetMs = safeSignedInt(quota?.nextResetMs);
  const isLocked = !hasActiveSession && playsRemainingToday <= 0;

  return {
    dayKey: String(quota?.dayKey || "").trim(),
    timezone: PRESENCE_ANALYTICS_TIMEZONE,
    dailyLimit: maxPlays,
    playsUsedToday,
    playsRemainingToday,
    hasActiveSession,
    canPlay: hasActiveSession || playsRemainingToday > 0,
    isLocked,
    nextResetMs,
    blockedUntilMs: isLocked ? nextResetMs : 0,
  };
}

async function collectPresenceAnalyticsNow(nowMs = Date.now()) {
  const safeNow = safeSignedInt(nowMs) || Date.now();
  const clientCutoffMs = safeNow - PRESENCE_ANALYTICS_CLIENT_WINDOW_MS;
  const roomCutoffMs = safeNow - PRESENCE_ANALYTICS_ROOM_WINDOW_MS;

  const [clientsSnap, roomsSnap] = await Promise.all([
    db.collection(CLIENTS_COLLECTION)
      .where("lastSeenAtMs", ">=", clientCutoffMs)
      .limit(5000)
      .get(),
    db.collection(ROOMS_COLLECTION)
      .where("status", "in", ["waiting", "playing"])
      .limit(200)
      .get(),
  ]);

  const onlineUsers = new Set();
  const inGameUsers = new Set();
  let playingRooms = 0;
  let waitingRooms = 0;
  let activeRooms = 0;

  clientsSnap.docs.forEach((docSnap) => {
    const data = docSnap.data() || {};
    const uid = String(data.uid || docSnap.id || "").trim();
    const lastSeenMs = safeSignedInt(data.lastSeenAtMs);
    if (!uid || lastSeenMs < clientCutoffMs) return;
    onlineUsers.add(uid);
  });

  roomsSnap.docs.forEach((docSnap) => {
    const room = docSnap.data() || {};
    const status = String(room.status || "");
    if (status === "playing") playingRooms += 1;
    if (status === "waiting") waitingRooms += 1;

    const roomPresence = room.roomPresenceMs && typeof room.roomPresenceMs === "object"
      ? room.roomPresenceMs
      : {};
    let roomHasPresence = false;

    Object.keys(roomPresence).forEach((uidRaw) => {
      const uid = String(uidRaw || "").trim();
      const lastSeenMs = safeSignedInt(roomPresence[uidRaw]);
      if (!uid || lastSeenMs < roomCutoffMs) return;
      roomHasPresence = true;
      inGameUsers.add(uid);
      onlineUsers.add(uid);
    });

    if (roomHasPresence) activeRooms += 1;
  });

  return {
    sampledAtMs: safeNow,
    onlineUsers: onlineUsers.size,
    onlineInGameUsers: inGameUsers.size,
    activeRooms,
    playingRooms,
    waitingRooms,
  };
}

function buildPresenceRollupUpdate(existing = {}, sample = {}, keyField = "", keyValue = "") {
  const nextSamples = safeInt(existing.samples) + 1;
  const nextOnlineUsersSum = safeInt(existing.onlineUsersSum) + safeInt(sample.onlineUsers);
  const nextOnlineInGameUsersSum = safeInt(existing.onlineInGameUsersSum) + safeInt(sample.onlineInGameUsers);
  const nextActiveRoomsSum = safeInt(existing.activeRoomsSum) + safeInt(sample.activeRooms);

  return {
    [keyField]: String(keyValue || ""),
    timezone: PRESENCE_ANALYTICS_TIMEZONE,
    samples: nextSamples,
    onlineUsersSum: nextOnlineUsersSum,
    onlineUsersMax: Math.max(safeInt(existing.onlineUsersMax), safeInt(sample.onlineUsers)),
    onlineInGameUsersSum: nextOnlineInGameUsersSum,
    onlineInGameUsersMax: Math.max(safeInt(existing.onlineInGameUsersMax), safeInt(sample.onlineInGameUsers)),
    activeRoomsSum: nextActiveRoomsSum,
    activeRoomsMax: Math.max(safeInt(existing.activeRoomsMax), safeInt(sample.activeRooms)),
    playingRoomsMax: Math.max(safeInt(existing.playingRoomsMax), safeInt(sample.playingRooms)),
    waitingRoomsMax: Math.max(safeInt(existing.waitingRoomsMax), safeInt(sample.waitingRooms)),
    lastSampleAtMs: safeSignedInt(sample.sampledAtMs) || Date.now(),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };
}

function logSecurityRejection(callable, request, code, extra = {}) {
  const payload = {
    code: String(code || "unknown"),
    callable: String(callable || "unknown"),
    uid: String(request?.auth?.uid || ""),
    hasAuth: !!request?.auth?.uid,
    hasAppCheck: !!request?.app,
    ...extra,
  };
  console.warn("[SECURITY_REJECT]", JSON.stringify(payload));
}

function assertAppCheck(request, callable) {
  if (!shouldEnforceAppCheck()) return;
  if (isEmulator()) return;
  if (request?.app) return;
  logSecurityRejection(callable, request, "app-check-required");
  throw new HttpsError("failed-precondition", "App Check requis.", {
    code: "app-check-required",
  });
}

function publicOnCall(callableName, handler) {
  return onCall(async (request) => {
    assertAppCheck(request, callableName);
    return handler(request);
  });
}

function safeInt(value) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.max(0, Math.floor(n)) : 0;
}

function safeSignedInt(value) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : 0;
}

function computeOrderAmount(order) {
  if (typeof order?.amount === "number" && Number.isFinite(order.amount)) {
    return safeInt(order.amount);
  }
  if (!Array.isArray(order?.items)) return 0;
  return safeInt(order.items.reduce((sum, item) => {
    const price = Number(item?.price) || 0;
    const quantity = Number(item?.quantity) || 1;
    return sum + (price * quantity);
  }, 0));
}

function computeReservedWithdrawalAmount(withdrawal) {
  return safeInt(withdrawal?.requestedAmount ?? withdrawal?.amount);
}

function computeWalletAvailableGourdes({
  orders = [],
  withdrawals = [],
  exchangedGourdes = 0,
} = {}) {
  const approvedDepositsHtg = (Array.isArray(orders) ? orders : []).reduce(
    (sum, item) => sum + computeOrderAmount(item),
    0
  );
  const reservedWithdrawalsHtg = (Array.isArray(withdrawals) ? withdrawals : []).reduce((sum, item) => {
    if (item?.status === "rejected") return sum;
    return sum + computeReservedWithdrawalAmount(item);
  }, 0);
  // Keep the raw base so gains already reconverted from Does do not recreate
  // phantom HTG after a withdrawal has reserved the full available amount.
  const baseBalanceHtg = approvedDepositsHtg - reservedWithdrawalsHtg;
  const exchanged = safeSignedInt(exchangedGourdes);

  return {
    approvedDepositsHtg,
    reservedWithdrawalsHtg,
    baseBalanceHtg,
    exchangedGourdes: exchanged,
    availableBalanceHtg: Math.max(0, baseBalanceHtg - exchanged),
  };
}

function isProvisionalFundingEnabled(settings = {}) {
  return settings?.provisionalDepositsEnabled === true;
}

function isFundingV2Order(order = {}) {
  return safeInt(order?.fundingVersion) >= PROVISIONAL_FUNDING_VERSION
    && String(order?.creditMode || "") === PROVISIONAL_CREDIT_MODE;
}

function getOrderResolutionStatus(order = {}) {
  const resolution = String(order?.resolutionStatus || "").trim().toLowerCase();
  if (resolution === "approved" || resolution === "rejected" || resolution === "pending") {
    return resolution;
  }
  const status = String(order?.status || "").trim().toLowerCase();
  if (status === "approved" || status === "rejected") return status;
  return "pending";
}

function getOrderApprovedAmountHtg(order = {}) {
  if (isFundingV2Order(order)) {
    if (getOrderResolutionStatus(order) !== "approved") return 0;
    const explicitAmount = safeInt(order?.approvedAmountHtg);
    return explicitAmount > 0 ? explicitAmount : computeOrderAmount(order);
  }
  return String(order?.status || "").trim().toLowerCase() === "approved"
    ? computeOrderAmount(order)
    : 0;
}

function getOrderProvisionalHtgRemaining(order = {}) {
  if (!isFundingV2Order(order)) return 0;
  if (getOrderResolutionStatus(order) !== "pending") return 0;
  return safeInt(order?.provisionalHtgRemaining);
}

function getPendingOrdersProvisionalConvertedHtg(orders = []) {
  return (Array.isArray(orders) ? orders : []).reduce((sum, item) => {
    if (!isFundingV2Order(item)) return sum;
    if (getOrderResolutionStatus(item) !== "pending") return sum;
    return sum + safeInt(item?.provisionalHtgConverted);
  }, 0);
}

function getOrderProvisionalCapitalDoesBalance(order = {}) {
  if (!isFundingV2Order(order)) return 0;
  if (getOrderResolutionStatus(order) !== "pending") return 0;

  const explicitRemainingDoes = safeInt(order?.provisionalDoesRemaining);
  if (explicitRemainingDoes > 0) return explicitRemainingDoes;

  const totalConvertedDoes = safeInt(order?.provisionalHtgConverted) * RATE_HTG_TO_DOES;
  if (totalConvertedDoes <= 0) return 0;

  const playedDoes = safeInt(order?.provisionalDoesPlayed);
  return Math.max(0, totalConvertedDoes - playedDoes);
}

function getOrderPendingProvisionalDoesTotal(order = {}) {
  if (!isFundingV2Order(order)) return 0;
  if (getOrderResolutionStatus(order) !== "pending") return 0;
  return getOrderProvisionalCapitalDoesBalance(order) + safeInt(order?.provisionalGainDoes);
}

function buildApprovedExchangeLedger({
  orders = [],
  walletData = {},
  exchangeHistory = [],
} = {}) {
  const storedNetExchangedHtg = safeSignedInt(walletData.exchangedGourdes);
  const storedTotalBoughtEverHtg = safeInt(walletData.totalExchangedHtgEver);
  const pendingProvisionalConvertedHtg = getPendingOrdersProvisionalConvertedHtg(orders);

  if (!Array.isArray(exchangeHistory) || exchangeHistory.length <= 0) {
    return {
      exchangedApprovedHtg: storedNetExchangedHtg,
      totalExchangedApprovedHtg: storedTotalBoughtEverHtg,
      pendingProvisionalConvertedHtg,
      source: "wallet",
    };
  }

  let historyNetExchangedHtg = 0;
  let historyTotalBoughtEverHtg = 0;

  exchangeHistory.forEach((item) => {
    const data = item && typeof item === "object" ? item : {};
    const type = String(data.type || "").trim();
    if (type === "xchange_buy") {
      const amountHtg = safeInt(data.amountGourdes ?? data.deltaExchangedGourdes);
      historyNetExchangedHtg += amountHtg;
      historyTotalBoughtEverHtg += amountHtg;
      return;
    }
    if (type === "xchange_sell") {
      const amountHtg = safeInt(data.amountGourdes ?? Math.abs(safeSignedInt(data.deltaExchangedGourdes)));
      historyNetExchangedHtg -= amountHtg;
    }
  });

  return {
    exchangedApprovedHtg: safeSignedInt(historyNetExchangedHtg - pendingProvisionalConvertedHtg),
    totalExchangedApprovedHtg: Math.max(0, historyTotalBoughtEverHtg - pendingProvisionalConvertedHtg),
    pendingProvisionalConvertedHtg,
    source: "history",
  };
}

function buildWalletFundingSnapshot({
  orders = [],
  withdrawals = [],
  walletData = {},
  exchangeHistory = [],
} = {}) {
  const approvedDepositsHtg = (Array.isArray(orders) ? orders : []).reduce(
    (sum, item) => sum + getOrderApprovedAmountHtg(item),
    0
  );
  const reservedWithdrawalsHtg = (Array.isArray(withdrawals) ? withdrawals : []).reduce((sum, item) => {
    if (String(item?.status || "").trim().toLowerCase() === "rejected") return sum;
    return sum + computeReservedWithdrawalAmount(item);
  }, 0);
  const approvedBaseHtg = approvedDepositsHtg - reservedWithdrawalsHtg;
  const exchangeLedger = buildApprovedExchangeLedger({
    orders,
    walletData,
    exchangeHistory,
  });
  const exchangedApprovedHtg = safeSignedInt(exchangeLedger.exchangedApprovedHtg);
  const approvedHtgAvailable = Math.max(0, approvedBaseHtg - exchangedApprovedHtg);
  const provisionalHtgAvailable = (Array.isArray(orders) ? orders : []).reduce(
    (sum, item) => sum + getOrderProvisionalHtgRemaining(item),
    0
  );
  const totalExchangedApprovedHtg = safeInt(exchangeLedger.totalExchangedApprovedHtg);
  const remainingToExchangeHtg = Math.max(0, approvedDepositsHtg - totalExchangedApprovedHtg);
  const provisionalDoesBalance = safeInt(walletData.doesProvisionalBalance);
  const rawDoesBalance = safeInt(walletData.doesBalance);
  const approvedDoesBalance = safeInt(
    typeof walletData.doesApprovedBalance === "number"
      ? walletData.doesApprovedBalance
      : Math.max(0, rawDoesBalance - provisionalDoesBalance)
  );
  const pendingPlayFromXchangeDoes = safeInt(walletData.pendingPlayFromXchangeDoes);
  const pendingPlayFromReferralDoes = safeInt(walletData.pendingPlayFromReferralDoes);
  const pendingPlayTotalDoes = pendingPlayFromXchangeDoes + pendingPlayFromReferralDoes;
  const exchangeableDoesAvailable = safeInt(
    typeof walletData.exchangeableDoesAvailable === "number"
      ? Math.min(walletData.exchangeableDoesAvailable, approvedDoesBalance)
      : (pendingPlayTotalDoes <= 0 ? approvedDoesBalance : 0)
  );

  return {
    approvedDepositsHtg,
    reservedWithdrawalsHtg,
    approvedBaseHtg,
    approvedHtgAvailable,
    provisionalHtgAvailable,
    playableHtg: approvedHtgAvailable + provisionalHtgAvailable,
    exchangedApprovedHtg,
    totalExchangedApprovedHtg,
    remainingToExchangeHtg,
    // HTG becomes withdrawable as it comes back from approved Does reconversion.
    withdrawableHtg: Math.max(0, approvedHtgAvailable - remainingToExchangeHtg),
    approvedDoesBalance,
    provisionalDoesBalance,
    doesBalance: approvedDoesBalance + provisionalDoesBalance,
    exchangeableDoesAvailable,
    pendingPlayFromXchangeDoes,
    pendingPlayFromReferralDoes,
    pendingPlayTotalDoes,
  };
}

function buildFundingWalletPatch(snapshot = {}) {
  return {
    approvedHtgAvailable: safeInt(snapshot.approvedHtgAvailable),
    provisionalHtgAvailable: safeInt(snapshot.provisionalHtgAvailable),
    withdrawableHtg: safeInt(snapshot.withdrawableHtg),
    approvedDoesBalance: safeInt(snapshot.approvedDoesBalance),
    doesProvisionalBalance: safeInt(snapshot.provisionalDoesBalance),
    doesBalance: safeInt(snapshot.doesBalance),
    exchangeableDoesAvailable: safeInt(snapshot.exchangeableDoesAvailable),
  };
}

function buildFrozenAccountError(walletData = {}) {
  return new HttpsError(
    "failed-precondition",
    "Ton compte a été temporairement gelé après plusieurs dépôts refusés. Contacte l'assistance.",
    {
      code: "account-frozen",
      accountFrozen: true,
      freezeReason: String(walletData.freezeReason || "3_rejected_deposits"),
      rejectedDepositStrikeCount: safeInt(walletData.rejectedDepositStrikeCount),
    }
  );
}

function assertWalletNotFrozen(walletData = {}) {
  if (walletData?.accountFrozen === true) {
    throw buildFrozenAccountError(walletData);
  }
}

function normalizeFundingSources(raw = []) {
  if (!Array.isArray(raw)) return [];
  return raw
    .map((item) => ({
      orderId: sanitizeText(item?.orderId || "", 120),
      amountDoes: safeInt(item?.amountDoes),
    }))
    .filter((item) => item.orderId && item.amountDoes > 0);
}

function allocateDoesProportionally(totalDoes = 0, rawEntries = []) {
  const total = safeInt(totalDoes);
  const entries = Array.isArray(rawEntries)
    ? rawEntries
      .map((item, index) => ({
        ...item,
        weight: safeInt(item?.weight ?? item?.amountDoes),
        index,
      }))
      .filter((item) => item.weight > 0)
    : [];
  if (total <= 0 || entries.length === 0) return [];

  const totalWeight = entries.reduce((sum, item) => sum + item.weight, 0);
  if (totalWeight <= 0) return [];

  let assigned = 0;
  const provisional = entries.map((item) => {
    const exact = (total * item.weight) / totalWeight;
    const floorValue = Math.floor(exact);
    assigned += floorValue;
    return {
      ...item,
      allocated: floorValue,
      remainder: exact - floorValue,
    };
  });

  let remaining = total - assigned;
  provisional
    .sort((left, right) => {
      if (right.remainder !== left.remainder) return right.remainder - left.remainder;
      return left.index - right.index;
    })
    .forEach((item) => {
      if (remaining <= 0) return;
      item.allocated += 1;
      remaining -= 1;
    });

  return provisional
    .sort((left, right) => left.index - right.index)
    .map((item) => ({
      orderId: sanitizeText(item.orderId || "", 120),
      amountDoes: safeInt(item.allocated),
    }))
    .filter((item) => item.orderId && item.amountDoes > 0);
}

function clamp(value, min, max) {
  return Math.min(Math.max(value, min), max);
}

function randomInt(min, max) {
  const low = Math.floor(min);
  const high = Math.floor(max);
  if (high <= low) return low;
  return low + Math.floor(Math.random() * (high - low + 1));
}

function seededInt(seed, min, max) {
  const low = Math.floor(min);
  const high = Math.floor(max);
  if (high <= low) return low;
  let x = Math.sin(seed) * 10000;
  x = x - Math.floor(x);
  return low + Math.floor(x * (high - low + 1));
}

function sanitizeText(value, maxLength = 160) {
  return String(value || "")
    .replace(/[\u0000-\u001f\u007f]/g, " ")
    .trim()
    .slice(0, maxLength);
}

function sanitizeUsername(value, maxLength = 24) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9._-]/g, "")
    .slice(0, maxLength);
}

function sanitizeEmail(value, maxLength = 160) {
  const out = sanitizeText(value, maxLength).toLowerCase();
  return /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/.test(out) ? out : "";
}

function sanitizePhone(value, maxLength = 40) {
  const out = sanitizeText(value, maxLength);
  return out.replace(/[^\d+\-\s().]/g, "");
}

function sanitizePublicAsset(value, maxLength = 400) {
  const out = sanitizeText(value, maxLength);
  if (!out) return "";
  if (/^(https:\/\/|\.\/|\/)/i.test(out)) return out;
  return "";
}

function userTournamentMetaRef(uid) {
  return db.collection(USER_TOURNAMENTS_COLLECTION).doc(uid);
}

function userTournamentSessionRef(uid, sessionId) {
  return userTournamentMetaRef(uid).collection("sessions").doc(sessionId);
}

function sanitizePaymentMethodAsset(value, maxLength = 180) {
  const out = sanitizeText(value, maxLength);
  if (!out) return "";

  const baseValue = out.replace(/\\/g, "/").split(/[?#]/)[0];
  const fileName = baseValue.split("/").pop() || "";
  if (!/^[a-zA-Z0-9._-]+\.(png|jpe?g|gif|webp|svg)$/i.test(fileName)) {
    return "";
  }
  return fileName;
}

function sanitizeStorageAssetUrl(value, maxLength = 2000) {
  const out = sanitizePublicAsset(value, maxLength);
  if (!out) return "";
  if (
    /^https:\/\/firebasestorage\.googleapis\.com\//i.test(out)
    || /^https:\/\/storage\.googleapis\.com\//i.test(out)
  ) {
    return out;
  }
  return "";
}

function sanitizePlayerLabel(email, fallbackSeat = 0) {
  const local = String(email || "").split("@")[0] || "";
  const cleaned = local.replace(/[^a-z0-9 _.-]/gi, "").trim().slice(0, 24);
  return cleaned || `Joueur ${fallbackSeat + 1}`;
}

function botSeatLabel(seat = 0) {
  return `Bot ${Number(seat) + 1}`;
}

function toMillis(value) {
  if (!value) return 0;
  if (value instanceof Date) return value.getTime();
  if (value instanceof admin.firestore.Timestamp) return value.toMillis();
  if (typeof value?.toMillis === "function") return value.toMillis();
  if (typeof value?.toDate === "function") return value.toDate().getTime();
  if (typeof value?.seconds === "number") return value.seconds * 1000;
  if (typeof value === "number") return Number.isFinite(value) ? value : 0;
  const parsed = Date.parse(String(value));
  return Number.isFinite(parsed) ? parsed : 0;
}

function extractClientIp(request) {
  const forwarded = String(
    request?.rawRequest?.headers?.["x-forwarded-for"] ||
    request?.rawRequest?.headers?.["fastly-client-ip"] ||
    request?.rawRequest?.headers?.["cf-connecting-ip"] ||
    request?.rawRequest?.ip ||
    ""
  ).trim();
  if (!forwarded) return "";
  return forwarded.split(",")[0].trim();
}

function hashIpAddress(rawIp = "") {
  const safeIp = String(rawIp || "").trim();
  if (!safeIp) return "";
  return crypto.createHash("sha256").update(`domino-ip:${safeIp}`).digest("hex").slice(0, 32);
}

function sanitizeAnalyticsContext(payload = {}, request = null) {
  const data = payload && typeof payload === "object" ? payload : {};
  return {
    deviceId: sanitizeText(data.deviceId || "", 120),
    appVersion: sanitizeText(data.appVersion || "", 48),
    country: sanitizeText(data.country || "", 48).toUpperCase(),
    browser: sanitizeText(data.browser || "", 120),
    landingPage: sanitizeText(data.landingPage || "", 240),
    utmSource: sanitizeText(data.utmSource || data.utm_source || "", 80),
    utmCampaign: sanitizeText(data.utmCampaign || data.utm_campaign || "", 120),
    creativeId: sanitizeText(data.creativeId || data.creative_id || "", 120),
    ipHash: hashIpAddress(extractClientIp(request)),
  };
}

function toSerializableValue(value) {
  if (value == null) return value;
  if (value instanceof Date) return value.getTime();
  if (value instanceof admin.firestore.Timestamp) return value.toMillis();
  if (Array.isArray(value)) return value.map((item) => toSerializableValue(item));
  if (typeof value === "object") {
    if (typeof value.path === "string" && typeof value.id === "string" && typeof value.parent === "object") {
      return value.path;
    }
    const out = {};
    Object.keys(value).forEach((key) => {
      out[key] = toSerializableValue(value[key]);
    });
    return out;
  }
  return value;
}

function snapshotRecordForCallable(docSnap) {
  return {
    id: docSnap.id,
    path: docSnap.ref.path,
    ...toSerializableValue(docSnap.data() || {}),
  };
}

function subcollectionRecordForCallable(docSnap) {
  const base = snapshotRecordForCallable(docSnap);
  const ownerDoc = docSnap.ref.parent?.parent || null;
  return {
    ...base,
    clientId: String(base.clientId || base.uid || ownerDoc?.id || "").trim(),
  };
}

function referralRecordForCallable(docSnap) {
  const base = snapshotRecordForCallable(docSnap);
  const ownerDoc = docSnap.ref.parent?.parent || null;
  const ownerCollection = String(ownerDoc?.parent?.id || "").trim();
  return {
    ...base,
    ownerId: String(ownerDoc?.id || "").trim(),
    ownerCollection,
  };
}

function supportMessageRecordForCallable(docSnap) {
  const base = snapshotRecordForCallable(docSnap);
  return {
    ...base,
    threadId: String(docSnap.ref.parent?.parent?.id || base.threadId || "").trim(),
  };
}

function mergePinnedDiscussionRecords(records = []) {
  const byId = new Map();
  records.forEach((record) => {
    if (!record || typeof record !== "object") return;
    const id = String(record.id || "").trim();
    if (!id) return;
    byId.set(id, record);
  });
  return Array.from(byId.values()).sort((left, right) => {
    const leftPinned = left.pinned === true;
    const rightPinned = right.pinned === true;
    if (leftPinned !== rightPinned) return leftPinned ? -1 : 1;
    if (leftPinned && rightPinned) {
      const rightPinnedAt = safeSignedInt(right.pinnedAtMs);
      const leftPinnedAt = safeSignedInt(left.pinnedAtMs);
      if (rightPinnedAt !== leftPinnedAt) return rightPinnedAt - leftPinnedAt;
    }
    return safeSignedInt(left.createdAtMs) - safeSignedInt(right.createdAtMs);
  });
}

function normalizeCode(value) {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9_-]/g, "");
}

function randomCode(size = 6) {
  let out = "";
  for (let i = 0; i < size; i += 1) {
    out += AMBASSADOR_CODE_CHARS[Math.floor(Math.random() * AMBASSADOR_CODE_CHARS.length)];
  }
  return out;
}

function buildAmbassadorReferralLink(linkCode) {
  const normalized = normalizeCode(linkCode);
  if (!normalized) return "";
  return `./inedex.html?amb=${encodeURIComponent(normalized)}`;
}

function buildUserReferralLink(referralCode) {
  const normalized = normalizeCode(referralCode);
  if (!normalized) return "";
  return `./inedex.html?ref=${encodeURIComponent(normalized)}`;
}

function safeCompareText(a, b) {
  const aBuf = Buffer.from(String(a || ""), "utf8");
  const bBuf = Buffer.from(String(b || ""), "utf8");
  if (aBuf.length !== bBuf.length) return false;
  return crypto.timingSafeEqual(aBuf, bBuf);
}

function hashAuthCode(authCode, saltHex = "") {
  const saltBuffer = saltHex ? Buffer.from(String(saltHex), "hex") : crypto.randomBytes(AUTH_HASH_SALT_BYTES);
  const hashBuffer = crypto.scryptSync(String(authCode || ""), saltBuffer, AUTH_HASH_KEYLEN);
  return {
    algo: AUTH_HASH_ALGO,
    saltHex: saltBuffer.toString("hex"),
    hashHex: hashBuffer.toString("hex"),
  };
}

function verifyAuthCode(authCode, hashHex, saltHex, algo = AUTH_HASH_ALGO) {
  if (String(algo || "") !== AUTH_HASH_ALGO) return false;
  try {
    const calc = crypto.scryptSync(String(authCode || ""), Buffer.from(String(saltHex || ""), "hex"), AUTH_HASH_KEYLEN);
    const expected = Buffer.from(String(hashHex || ""), "hex");
    if (calc.length !== expected.length) return false;
    return crypto.timingSafeEqual(calc, expected);
  } catch (_) {
    return false;
  }
}

function ambassadorSecretsRef(ambassadorRef) {
  return ambassadorRef.collection(AMBASSADOR_PRIVATE_SUBCOLLECTION).doc(AMBASSADOR_SECRETS_DOC);
}

async function readAmbassadorSecrets(ambassadorDoc) {
  const publicData = ambassadorDoc?.data() || {};
  const secretsSnap = await ambassadorSecretsRef(ambassadorDoc.ref).get();
  const secretData = secretsSnap.exists ? (secretsSnap.data() || {}) : {};
  return {
    hasPrivate: secretsSnap.exists,
    hashHex: String(secretData.authCodeHash || publicData.authCodeHash || ""),
    saltHex: String(secretData.authCodeSalt || publicData.authCodeSalt || ""),
    algo: String(secretData.authCodeAlgo || publicData.authCodeAlgo || AUTH_HASH_ALGO),
    legacyPlain: String(publicData.authCode || "").trim(),
    hasPublicSecrets:
      !!String(publicData.authCode || "").trim()
      || (!!String(publicData.authCodeHash || "").trim() && !!String(publicData.authCodeSalt || "").trim()),
  };
}

async function ambassadorCodeExists(code) {
  const normalized = normalizeCode(code);
  if (!normalized) return false;
  const [promoSnap, linkSnap] = await Promise.all([
    db.collection(AMBASSADORS_COLLECTION).where("promoCode", "==", normalized).limit(1).get(),
    db.collection(AMBASSADORS_COLLECTION).where("linkCode", "==", normalized).limit(1).get(),
  ]);
  return !promoSnap.empty || !linkSnap.empty;
}

async function clientReferralCodeExists(code, currentUid = "") {
  const normalized = normalizeCode(code);
  if (!normalized) return false;
  const snap = await db.collection(CLIENTS_COLLECTION)
    .where("referralCode", "==", normalized)
    .limit(1)
    .get();
  if (snap.empty) return false;
  const found = snap.docs[0];
  return !found || found.id !== String(currentUid || "");
}

async function generateUniqueClientReferralCode(currentUid = "") {
  for (let i = 0; i < 40; i += 1) {
    const candidate = `${USER_REFERRAL_PREFIX}${randomCode(6)}`;
    if (!(await clientReferralCodeExists(candidate, currentUid))) return candidate;
  }
  throw new HttpsError("aborted", "Impossible de générer un code de parrainage unique.");
}

async function findClientByReferralCode(code) {
  const normalized = normalizeCode(code);
  if (!normalized) return null;
  const snap = await db.collection(CLIENTS_COLLECTION)
    .where("referralCode", "==", normalized)
    .limit(1)
    .get();
  return snap.empty ? null : snap.docs[0];
}

async function findAmbassadorByCode(code) {
  const normalized = normalizeCode(code);
  if (!normalized) return null;
  const [promoSnap, linkSnap] = await Promise.all([
    db.collection(AMBASSADORS_COLLECTION).where("promoCode", "==", normalized).limit(1).get(),
    db.collection(AMBASSADORS_COLLECTION).where("linkCode", "==", normalized).limit(1).get(),
  ]);
  if (!promoSnap.empty) return promoSnap.docs[0];
  if (!linkSnap.empty) return linkSnap.docs[0];
  return null;
}

function deriveRootAmbassadorContext(sourceData = {}, directAmbassadorId = "") {
  const rootAmbassadorId = String(
    directAmbassadorId ||
    sourceData.rootAmbassadorId ||
    sourceData.referredByAmbassadorId ||
    ""
  ).trim();
  if (!rootAmbassadorId) return null;

  let depth = safeInt(sourceData.ambassadorDepthFromRoot);
  if (!depth && directAmbassadorId) depth = 1;
  if (!depth && String(sourceData.referredByType || "") === "ambassador") depth = 1;
  if (!depth && String(sourceData.referredByAmbassadorId || "").trim() === rootAmbassadorId) depth = 1;
  return {
    rootAmbassadorId,
    depth: Math.max(1, depth || 1),
  };
}

function buildAmbassadorReferralWrite(options = {}) {
  const currentData = options.currentData || {};
  const hasApprovedDeposit = options.hasApprovedDeposit === true;
  const totalGamesTracked = safeInt(currentData.totalGamesTracked || currentData.totalGames);
  const winsTracked = safeInt(currentData.winsTracked || currentData.winCount);
  const lossesTracked = safeInt(currentData.lossesTracked || currentData.lossCount);
  const depositCount = hasApprovedDeposit
    ? Math.max(1, safeInt(currentData.depositCount || 1))
    : safeInt(currentData.depositCount);

  return {
    userId: String(options.userId || "").trim(),
    clientUid: String(options.userId || "").trim(),
    email: sanitizeEmail(options.email || currentData.email || "", 160),
    displayName: sanitizeText(options.displayName || currentData.displayName || options.userId || "Utilisateur", 80),
    code: normalizeCode(options.code || currentData.code || ""),
    via: sanitizeText(options.via || currentData.via || "", 32),
    depth: Math.max(1, safeInt(options.depth || currentData.depth || 1)),
    parentClientUid: String(options.parentClientUid || currentData.parentClientUid || "").trim(),
    rootAmbassadorId: String(options.rootAmbassadorId || currentData.rootAmbassadorId || "").trim(),
    isCommissionEligible: options.isCommissionEligible === true,
    hasApprovedDeposit,
    depositCount,
    totalGamesTracked,
    totalGames: totalGamesTracked,
    winsTracked,
    winCount: winsTracked,
    lossesTracked,
    lossCount: lossesTracked,
    ambassadorDoesDelta: safeSignedInt(currentData.ambassadorDoesDelta),
    lastGameAt: currentData.lastGameAt || null,
    createdAt: currentData.createdAt || admin.firestore.FieldValue.serverTimestamp(),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };
}

async function applyUserReferralAttribution(options = {}) {
  const uid = String(options.uid || "").trim();
  const email = sanitizeEmail(options.email || "", 160);
  const promoCode = normalizeCode(options.promoCode || "");
  const via = String(options.via || "").toLowerCase() === "link" ? "link" : "promo";

  if (!uid || !promoCode) {
    return { applied: false, reason: "no_candidate" };
  }

  const referrerSnap = await findClientByReferralCode(promoCode);
  if (!referrerSnap || referrerSnap.id === uid) {
    return { applied: false, reason: "invalid_or_self" };
  }

  const clientRef = walletRef(uid);
  const referrerRef = walletRef(referrerSnap.id);
  const referralRef = referrerRef.collection("referrals").doc(uid);

  return db.runTransaction(async (tx) => {
    const clientSnap = await tx.get(clientRef);
    if (!clientSnap.exists) {
      return { applied: false, reason: "client_not_found" };
    }

    const clientData = clientSnap.data() || {};
    if (clientData.referredByType || clientData.referredByUserId || clientData.referredByAmbassadorId) {
      return { applied: false, reason: "already_set" };
    }
    if (normalizeCode(clientData.referralCode || "") === promoCode) {
      return { applied: false, reason: "invalid_or_self" };
    }

    const latestReferrerSnap = await tx.get(referrerRef);
    if (!latestReferrerSnap.exists || latestReferrerSnap.id === uid) {
      return { applied: false, reason: "invalid_or_self" };
    }

    const referralSnap = await tx.get(referralRef);
    const referrerData = latestReferrerSnap.data() || {};
    const referralData = referralSnap.exists ? (referralSnap.data() || {}) : {};
    const ambassadorContext = AMBASSADOR_SYSTEM_ENABLED ? deriveRootAmbassadorContext(referrerData) : null;
    let ambassadorSnap = null;
    let ambassadorReferralSnap = null;
    let ambassadorRef = null;
    let ambassadorReferralRef = null;
    let ambassadorDepth = 0;
    let ambassadorEligible = false;
    let ambassadorData = {};
    let ambassadorReferralData = {};

    if (ambassadorContext?.rootAmbassadorId) {
      ambassadorDepth = Math.max(1, safeInt(ambassadorContext.depth) + 1);
      ambassadorEligible = ambassadorDepth <= 3;
      ambassadorRef = db.collection(AMBASSADORS_COLLECTION).doc(ambassadorContext.rootAmbassadorId);
      ambassadorReferralRef = ambassadorRef.collection("referrals").doc(uid);
      ambassadorSnap = await tx.get(ambassadorRef);
      ambassadorData = ambassadorSnap.exists ? (ambassadorSnap.data() || {}) : {};
      if (ambassadorEligible && ambassadorSnap.exists) {
        ambassadorReferralSnap = await tx.get(ambassadorReferralRef);
        ambassadorReferralData = ambassadorReferralSnap.exists ? (ambassadorReferralSnap.data() || {}) : {};
      }
    }

    const clientPatch = {
      referredByType: "user",
      referredByUserId: latestReferrerSnap.id,
      referredByCode: promoCode,
      referredVia: via,
      referredAt: admin.firestore.FieldValue.serverTimestamp(),
      invitedByType: "user",
      invitedByUserId: latestReferrerSnap.id,
      hasApprovedDeposit: clientData.hasApprovedDeposit === true,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    };

    if (ambassadorContext?.rootAmbassadorId && ambassadorSnap?.exists) {
      clientPatch.rootAmbassadorId = ambassadorContext.rootAmbassadorId;
      clientPatch.ambassadorDepthFromRoot = ambassadorDepth;
      clientPatch.ambassadorCommissionEligible = ambassadorEligible;
    }

    tx.set(clientRef, clientPatch, { merge: true });

    tx.set(referrerRef, {
      referralSignupsTotal: safeInt(referrerData.referralSignupsTotal) + 1,
      referralSignupsViaLink: safeInt(referrerData.referralSignupsViaLink) + (via === "link" ? 1 : 0),
      referralSignupsViaCode: safeInt(referrerData.referralSignupsViaCode) + (via === "link" ? 0 : 1),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });

    tx.set(referralRef, {
      userId: uid,
      email: email || sanitizeEmail(referralData.email || "", 160),
      displayName: sanitizeText(clientData.name || String(email || "").split("@")[0] || referralData.displayName || "Utilisateur", 80),
      code: promoCode,
      via,
      hasApprovedDeposit: clientData.hasApprovedDeposit === true,
      depositCount: safeInt(clientData.hasApprovedDeposit === true ? 1 : referralData.depositCount),
      createdAt: referralData.createdAt || admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });

    if (ambassadorContext?.rootAmbassadorId && ambassadorEligible && ambassadorSnap?.exists) {
      tx.set(ambassadorRef, {
        totalSignups: safeInt(ambassadorData.totalSignups) + 1,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      }, { merge: true });

      tx.set(ambassadorReferralRef, buildAmbassadorReferralWrite({
        userId: uid,
        email,
        displayName: clientData.name || String(email || "").split("@")[0] || "Utilisateur",
        code: promoCode,
        via,
        depth: ambassadorDepth,
        parentClientUid: latestReferrerSnap.id,
        rootAmbassadorId: ambassadorContext.rootAmbassadorId,
        isCommissionEligible: true,
        hasApprovedDeposit: clientData.hasApprovedDeposit === true,
        currentData: ambassadorReferralData,
      }), { merge: true });
    }

    return {
      applied: true,
      targetType: "user",
      targetId: latestReferrerSnap.id,
      code: promoCode,
      via,
    };
  });
}

async function applyAmbassadorReferralAttribution(options = {}) {
  if (!AMBASSADOR_SYSTEM_ENABLED) {
    return { applied: false, reason: "ambassador_disabled" };
  }
  const uid = String(options.uid || "").trim();
  const email = sanitizeEmail(options.email || "", 160);
  const promoCode = normalizeCode(options.promoCode || "");
  const via = String(options.via || "").toLowerCase() === "link" ? "link" : "promo";

  if (!uid || !promoCode) {
    return { applied: false, reason: "no_candidate" };
  }

  const ambassadorSnap = await findAmbassadorByCode(promoCode);
  if (!ambassadorSnap) {
    return { applied: false, reason: "invalid_or_self" };
  }

  const clientRef = walletRef(uid);
  const ambassadorRef = db.collection(AMBASSADORS_COLLECTION).doc(ambassadorSnap.id);
  const ambassadorReferralRef = ambassadorRef.collection("referrals").doc(uid);

  return db.runTransaction(async (tx) => {
    const clientSnap = await tx.get(clientRef);
    if (!clientSnap.exists) {
      return { applied: false, reason: "client_not_found" };
    }

    const clientData = clientSnap.data() || {};
    if (clientData.referredByType || clientData.referredByUserId || clientData.referredByAmbassadorId) {
      return { applied: false, reason: "already_set" };
    }

    const latestAmbassadorSnap = await tx.get(ambassadorRef);
    if (!latestAmbassadorSnap.exists) {
      return { applied: false, reason: "invalid_or_self" };
    }

    const ambassadorData = latestAmbassadorSnap.data() || {};
    const ambassadorReferralSnap = await tx.get(ambassadorReferralRef);
    const ambassadorReferralData = ambassadorReferralSnap.exists ? (ambassadorReferralSnap.data() || {}) : {};

    tx.set(clientRef, {
      referredByType: "ambassador",
      referredByAmbassadorId: latestAmbassadorSnap.id,
      referredByCode: promoCode,
      referredVia: via,
      referredAt: admin.firestore.FieldValue.serverTimestamp(),
      invitedByType: "ambassador",
      invitedByUserId: "",
      rootAmbassadorId: latestAmbassadorSnap.id,
      ambassadorDepthFromRoot: 1,
      ambassadorCommissionEligible: true,
      hasApprovedDeposit: clientData.hasApprovedDeposit === true,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });

    tx.set(ambassadorRef, {
      totalSignups: safeInt(ambassadorData.totalSignups) + 1,
      totalSignupsViaLink: safeInt(ambassadorData.totalSignupsViaLink) + (via === "link" ? 1 : 0),
      totalSignupsViaCode: safeInt(ambassadorData.totalSignupsViaCode) + (via === "link" ? 0 : 1),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });

    tx.set(ambassadorReferralRef, buildAmbassadorReferralWrite({
      userId: uid,
      email,
      displayName: clientData.name || String(email || "").split("@")[0] || "Utilisateur",
      code: promoCode,
      via,
      depth: 1,
      parentClientUid: "",
      rootAmbassadorId: latestAmbassadorSnap.id,
      isCommissionEligible: true,
      hasApprovedDeposit: clientData.hasApprovedDeposit === true,
      currentData: ambassadorReferralData,
    }), { merge: true });

    return {
      applied: true,
      targetType: "ambassador",
      targetId: latestAmbassadorSnap.id,
      code: promoCode,
      via,
    };
  });
}

async function applyPromoAttribution(options = {}) {
  const promoCode = normalizeCode(options.promoCode || "");
  if (!promoCode) {
    return { applied: false, reason: "no_candidate" };
  }

  if (promoCode.startsWith(USER_REFERRAL_PREFIX)) {
    return applyUserReferralAttribution(options);
  }
  if (promoCode.startsWith(AMBASSADOR_PROMO_PREFIX) || promoCode.startsWith(AMBASSADOR_LINK_PREFIX)) {
    if (!AMBASSADOR_SYSTEM_ENABLED) {
      return { applied: false, reason: "ambassador_disabled" };
    }
    return applyAmbassadorReferralAttribution(options);
  }

  const userAttempt = await applyUserReferralAttribution(options);
  if (userAttempt.applied) return userAttempt;

  if (!AMBASSADOR_SYSTEM_ENABLED) {
    return userAttempt;
  }

  const ambassadorAttempt = await applyAmbassadorReferralAttribution(options);
  if (ambassadorAttempt.applied) return ambassadorAttempt;

  return userAttempt.reason === "already_set" ? userAttempt : ambassadorAttempt;
}

async function generateUniqueAmbassadorCode(prefix, size) {
  for (let i = 0; i < 40; i += 1) {
    const candidate = `${prefix}${randomCode(size)}`;
    if (!(await ambassadorCodeExists(candidate))) return candidate;
  }
  throw new HttpsError("aborted", "Impossible de générer un code unique.");
}

function assertAuth(request) {
  const uid = String(request.auth?.uid || "").trim();
  if (!uid) {
    throw new HttpsError("unauthenticated", "Authentification requise.");
  }
  const email = String(request.auth?.token?.email || "").trim();
  return { uid, email };
}

function hasFinanceAdminClaim(request) {
  return request?.auth?.token?.admin === true
    || request?.auth?.token?.financeAdmin === true;
}

function hasFinanceAdminEmail(request) {
  const email = String(request?.auth?.token?.email || "").trim().toLowerCase();
  return !!email && email === FINANCE_ADMIN_EMAIL;
}

function assertFinanceAdmin(request) {
  const authData = assertAuth(request);
  if (!hasFinanceAdminClaim(request) && !hasFinanceAdminEmail(request)) {
    throw new HttpsError("permission-denied", "Accès administrateur requis.");
  }
  return authData;
}

async function verifyIdTokenFromRequest(req) {
  const authHeader = String(req?.headers?.authorization || req?.headers?.Authorization || "");
  const token = authHeader.startsWith("Bearer ") ? authHeader.slice(7).trim() : "";
  if (!token) return null;
  try {
    return await admin.auth().verifyIdToken(token);
  } catch (_) {
    return null;
  }
}

function normalizeBotDifficulty(value) {
  const level = sanitizeText(value || "", 20).toLowerCase();
  return BOT_DIFFICULTY_LEVELS.has(level) ? level : DEFAULT_BOT_DIFFICULTY;
}

function assertAdmin(request) {
  const authData = assertAuth(request);
  if (request.auth?.token?.admin !== true) {
    throw new HttpsError("permission-denied", "Accès administrateur requis.");
  }
  return authData;
}

function dashboardPushSubscriptionsCollection() {
  return db.collection(DASHBOARD_PUSH_SUBSCRIPTIONS_COLLECTION);
}

function sanitizeWebPushEndpoint(value, maxLength = 2000) {
  const out = sanitizeText(value || "", maxLength);
  if (!out) return "";
  if (!/^https:\/\/[^\s]+$/i.test(out)) return "";
  return out;
}

function sanitizeWebPushKey(value, maxLength = 512) {
  return String(value || "")
    .trim()
    .replace(/[^A-Za-z0-9_-]/g, "")
    .slice(0, maxLength);
}

function sanitizeDashboardPushUrl(value) {
  const out = sanitizeText(value || "", 400);
  if (!out) return DASHBOARD_DEFAULT_NOTIFICATION_URL;
  if (/^(https:\/\/|\/|\.\/)/i.test(out)) return out;
  return DASHBOARD_DEFAULT_NOTIFICATION_URL;
}

function sanitizePushSubscriptionPayload(payload = {}) {
  const data = payload && typeof payload === "object" ? payload : {};
  const endpoint = sanitizeWebPushEndpoint(data.endpoint || "");
  const expirationTime = data.expirationTime == null ? null : Number(data.expirationTime);
  const keysRaw = data.keys && typeof data.keys === "object" ? data.keys : {};
  const p256dh = sanitizeWebPushKey(keysRaw.p256dh || "");
  const authKey = sanitizeWebPushKey(keysRaw.auth || "");
  const platform = sanitizeText(data.platform || "", 80).toLowerCase();
  const userAgent = sanitizeText(data.userAgent || "", 240);
  const enabled = data.enabled !== false;
  return {
    endpoint,
    expirationTime: Number.isFinite(expirationTime) ? expirationTime : null,
    keys: {
      p256dh,
      auth: authKey,
    },
    platform,
    userAgent,
    enabled,
  };
}

function validatePushSubscriptionPayload(payload) {
  if (!payload.endpoint) {
    throw new HttpsError("invalid-argument", "Endpoint push requis.");
  }
  if (!payload.keys?.p256dh || !payload.keys?.auth) {
    throw new HttpsError("invalid-argument", "Clés push invalides.");
  }
}

function dashboardPushSubscriptionIdFromEndpoint(endpoint = "") {
  const safeEndpoint = sanitizeWebPushEndpoint(endpoint || "");
  if (!safeEndpoint) {
    throw new HttpsError("invalid-argument", "Endpoint push invalide.");
  }
  return crypto.createHash("sha256").update(`dashboard-push:${safeEndpoint}`).digest("hex");
}

function getDashboardWebPushConfig() {
  return {
    publicKey: String(process.env.DASHBOARD_WEB_PUSH_PUBLIC_KEY || "").trim(),
    privateKey: String(process.env.DASHBOARD_WEB_PUSH_PRIVATE_KEY || "").trim(),
    subject: String(process.env.DASHBOARD_WEB_PUSH_SUBJECT || "mailto:admin@dominoeslakay.com").trim(),
  };
}

let webPushConfiguredOnce = false;

function ensureDashboardWebPushConfigured() {
  const { publicKey, privateKey, subject } = getDashboardWebPushConfig();
  if (!publicKey || !privateKey || !subject) {
    return false;
  }
  if (!webPushConfiguredOnce) {
    webpush.setVapidDetails(subject, publicKey, privateKey);
    webPushConfiguredOnce = true;
  }
  return true;
}

async function getConfiguredDashboardAdminEmail() {
  try {
    const snap = await adminBootstrapRef().get();
    if (snap.exists) {
      const email = sanitizeEmail(snap.data()?.email || "", 160);
      if (email) return email;
    }
  } catch (error) {
    console.warn("[DASHBOARD_PUSH] impossible de lire le bootstrap admin", error);
  }
  return sanitizeEmail(FINANCE_ADMIN_EMAIL, 160);
}

async function listActiveDashboardPushSubscriptions() {
  const targetEmail = await getConfiguredDashboardAdminEmail();
  if (!targetEmail) return [];
  const snap = await dashboardPushSubscriptionsCollection()
    .where("email", "==", targetEmail)
    .get();
  return snap.docs
    .map((docSnap) => ({ id: docSnap.id, ref: docSnap.ref, ...(docSnap.data() || {}) }))
    .filter((item) => item.enabled !== false && sanitizeWebPushEndpoint(item.endpoint || ""));
}

function normalizePushSendErrorStatus(error) {
  const statusCode = Number(error?.statusCode || error?.status || error?.code || 0);
  return Number.isFinite(statusCode) ? statusCode : 0;
}

function shouldDeletePushSubscription(error) {
  const statusCode = normalizePushSendErrorStatus(error);
  return statusCode === 404 || statusCode === 410;
}

async function sendDashboardPushToAdmins(payload = {}) {
  if (!ensureDashboardWebPushConfigured()) {
    console.warn("[DASHBOARD_PUSH] configuration VAPID manquante; envoi ignoré.");
    return { ok: false, sent: 0, skipped: true };
  }

  const targetEmail = await getConfiguredDashboardAdminEmail();
  const subscriptions = await listActiveDashboardPushSubscriptions();
  console.info("[DASHBOARD_PUSH] préparation envoi", {
    type: sanitizeText(payload.type || "", 80),
    entityId: sanitizeText(payload.entityId || "", 160),
    tag: sanitizeText(payload.tag || "", 160),
    targetEmail,
    subscriptions: subscriptions.length,
    sourceCreatedAt: String(payload.sourceCreatedAt || ""),
  });
  if (!subscriptions.length) {
    return { ok: true, sent: 0 };
  }

  const message = {
    type: sanitizeText(payload.type || "", 80),
    title: sanitizeText(payload.title || "Dashboard", 120),
    body: sanitizeText(payload.body || "", 240),
    url: sanitizeDashboardPushUrl(payload.url || DASHBOARD_DEFAULT_NOTIFICATION_URL),
    entityId: sanitizeText(payload.entityId || "", 160),
    createdAt: new Date().toISOString(),
    sourceCreatedAt: String(payload.sourceCreatedAt || ""),
    tag: sanitizeText(payload.tag || "", 160),
  };

  let sent = 0;
  await Promise.all(subscriptions.map(async (subscription) => {
    const pushPayload = JSON.stringify(message);
    try {
      await webpush.sendNotification(
        {
          endpoint: subscription.endpoint,
          expirationTime: subscription.expirationTime ?? null,
          keys: {
            p256dh: subscription.keys?.p256dh || "",
            auth: subscription.keys?.auth || "",
          },
        },
        pushPayload
      );
      sent += 1;
      await subscription.ref.set({
        lastSeenAt: admin.firestore.FieldValue.serverTimestamp(),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      }, { merge: true });
      console.info("[DASHBOARD_PUSH] envoi ok", {
        id: subscription.id,
        type: message.type,
        entityId: message.entityId,
        tag: message.tag,
      });
    } catch (error) {
      console.warn("[DASHBOARD_PUSH] échec envoi", {
        id: subscription.id,
        statusCode: normalizePushSendErrorStatus(error),
        message: String(error?.message || error),
      });
      if (shouldDeletePushSubscription(error)) {
        await subscription.ref.delete().catch(() => {});
      }
    }
  }));

  console.info("[DASHBOARD_PUSH] envoi terminé", {
    type: message.type,
    entityId: message.entityId,
    tag: message.tag,
    sent,
    subscriptions: subscriptions.length,
  });

  return {
    ok: true,
    sent,
  };
}

function walletRef(uid) {
  return db.collection(CLIENTS_COLLECTION).doc(uid);
}

function walletHistoryRef(uid) {
  return db.collection(CLIENTS_COLLECTION).doc(uid).collection("xchanges");
}

function shareSitePromoRef(uid) {
  return walletRef(uid).collection("growthCampaigns").doc(SHARE_SITE_PROMO_DOC);
}

function adminBootstrapRef() {
  return db.collection("settings").doc(DPAYMENT_ADMIN_BOOTSTRAP_DOC);
}

function appPublicSettingsRef() {
  return db.collection("settings").doc(APP_PUBLIC_SETTINGS_DOC);
}

function buildStakeRewardDoes(stakeDoes) {
  return safeInt(stakeDoes) * DEFAULT_STAKE_REWARD_MULTIPLIER;
}

function buildStakeOptionId(stakeDoes) {
  return `stake_${safeInt(stakeDoes)}`;
}

function normalizeGameStakeOptions(rawOptions) {
  const source = Array.isArray(rawOptions) && rawOptions.length ? rawOptions : DEFAULT_GAME_STAKE_OPTIONS;
  const byStake = new Map();

  source.forEach((raw, index) => {
    const stakeDoes = safeInt(raw?.stakeDoes);
    if (stakeDoes <= 0) return;
    if (byStake.has(stakeDoes)) return;

    const sortOrderRaw = Number(raw?.sortOrder);
    const sortOrder = Number.isFinite(sortOrderRaw) ? Math.trunc(sortOrderRaw) : ((index + 1) * 10);

    byStake.set(stakeDoes, {
      id: buildStakeOptionId(stakeDoes),
      stakeDoes,
      rewardDoes: buildStakeRewardDoes(stakeDoes),
      enabled: raw?.enabled !== false,
      sortOrder,
    });
  });

  const normalized = Array.from(byStake.values())
    .sort((left, right) => {
      if (left.sortOrder !== right.sortOrder) return left.sortOrder - right.sortOrder;
      return left.stakeDoes - right.stakeDoes;
    });

  if (normalized.length) return normalized;
  return DEFAULT_GAME_STAKE_OPTIONS.map((item) => ({
    id: buildStakeOptionId(item.stakeDoes),
    stakeDoes: item.stakeDoes,
    rewardDoes: buildStakeRewardDoes(item.stakeDoes),
    enabled: item.enabled !== false,
    sortOrder: item.sortOrder,
  }));
}

function findStakeConfigByAmount(stakeDoes, gameStakeOptions, requireEnabled = false) {
  const normalizedStake = safeInt(stakeDoes);
  if (normalizedStake <= 0) return null;
  const options = Array.isArray(gameStakeOptions) ? gameStakeOptions : normalizeGameStakeOptions();
  const found = options.find((item) => safeInt(item?.stakeDoes) === normalizedStake) || null;
  if (!found) return null;
  if (requireEnabled && found.enabled !== true) return null;
  return found;
}

function resolveRoomRewardDoes(room = {}) {
  const explicit = safeInt(room.rewardAmountDoes);
  if (explicit > 0) return explicit;
  return buildStakeRewardDoes(room.entryCostDoes || room.stakeDoes || 0);
}

function normalizePublicAppSettings(rawData = {}) {
  const hasExplicitProvisionalToggle = typeof rawData.provisionalDepositsEnabled === "boolean";
  return {
    verificationHours: Math.max(1, Math.min(72, safeInt(rawData.verificationHours || DEFAULT_PUBLIC_SETTINGS.verificationHours))),
    expiredMessage: sanitizeText(rawData.expiredMessage || DEFAULT_PUBLIC_SETTINGS.expiredMessage, MAX_PUBLIC_TEXT_LENGTH),
    gameStakeOptions: normalizeGameStakeOptions(rawData.gameStakeOptions),
    appCheckSiteKey: sanitizeText(rawData.appCheckSiteKey || "", 256),
    provisionalDepositsEnabled: hasExplicitProvisionalToggle
      ? rawData.provisionalDepositsEnabled === true
      : DEFAULT_PUBLIC_SETTINGS.provisionalDepositsEnabled === true,
  };
}

async function readRawPublicAppSettings() {
  const directSnap = await appPublicSettingsRef().get();
  if (directSnap.exists) {
    return directSnap.data() || {};
  }

  const fallbackSnap = await db.collection("settings").get();
  if (fallbackSnap.empty) return {};

  const legacy = fallbackSnap.docs.find((docSnap) => {
    return docSnap.id !== DPAYMENT_ADMIN_BOOTSTRAP_DOC && docSnap.id !== APP_PUBLIC_SETTINGS_DOC;
  });

  return legacy ? (legacy.data() || {}) : {};
}

async function getConfiguredBotDifficulty() {
  try {
    const snap = await adminBootstrapRef().get();
    if (!snap.exists) return DEFAULT_BOT_DIFFICULTY;
    return normalizeBotDifficulty(snap.data()?.botDifficulty);
  } catch (_) {
    return DEFAULT_BOT_DIFFICULTY;
  }
}

function makeDeckOrder() {
  const arr = Array.from({ length: 28 }, (_, i) => i);
  for (let i = arr.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    const t = arr[i];
    arr[i] = arr[j];
    arr[j] = t;
  }
  return arr;
}

function normalizePrivateDeckOrder(raw) {
  if (!Array.isArray(raw) || raw.length !== 28) return [];
  const seen = new Set();
  const out = [];
  for (let i = 0; i < raw.length; i += 1) {
    const tileId = Number(raw[i]);
    if (!Number.isFinite(tileId) || tileId < 0 || tileId >= 28 || seen.has(tileId)) {
      return [];
    }
    seen.add(tileId);
    out.push(Math.trunc(tileId));
  }
  return out;
}

async function readPrivateDeckOrderForRoom(roomId) {
  const safeRoomId = String(roomId || "").trim();
  if (!safeRoomId) return [];
  const snap = await gameStateRef(safeRoomId).get();
  if (!snap.exists) return [];
  return normalizePrivateDeckOrder(snap.data()?.deckOrder);
}

function starterSeatFromDeckOrder(deckOrder) {
  if (!Array.isArray(deckOrder) || deckOrder.length !== 28) {
    return Math.floor(Math.random() * 4);
  }
  const tile = Number(deckOrder[0]);
  if (!Number.isFinite(tile)) return Math.floor(Math.random() * 4);
  return Math.abs(tile) % 4;
}

function buildPlayRequiredError(payload = {}) {
  return new HttpsError(
    "failed-precondition",
    "Tu dois jouer les Does avant de les reconvertir en HTG.",
    {
      code: "play-required-before-sell",
      pendingPlayFromXchangeDoes: safeInt(payload.pendingPlayFromXchangeDoes),
      pendingPlayFromReferralDoes: safeInt(payload.pendingPlayFromReferralDoes),
      pendingPlayTotalDoes: safeInt(payload.pendingPlayTotalDoes),
      exchangeableDoesAvailable: safeInt(payload.exchangeableDoesAvailable),
    }
  );
}

function roomRef(roomId) {
  return db.collection(ROOMS_COLLECTION).doc(String(roomId || "").trim());
}

function gameStateRef(roomId) {
  return db.collection(GAME_STATES_COLLECTION).doc(String(roomId || "").trim());
}

function getTileValues(tileId) {
  const idx = safeInt(tileId);
  const values = TILE_VALUES[idx];
  return Array.isArray(values) ? values : null;
}

function buildSeatHands(deckOrder) {
  if (!Array.isArray(deckOrder) || deckOrder.length !== 28) return null;
  const seatHands = [];
  for (let seat = 0; seat < 4; seat += 1) {
    const hand = [];
    for (let slot = 0; slot < 7; slot += 1) {
      const tileId = Number(deckOrder[(seat * 7) + slot]);
      if (!Number.isFinite(tileId) || !TILE_VALUES[tileId]) return null;
      hand.push(tileId);
    }
    seatHands.push(hand);
  }
  return seatHands;
}

function cloneSeatHands(seatHands) {
  return Array.isArray(seatHands)
    ? seatHands.map((hand) => (Array.isArray(hand) ? hand.slice(0, 7) : Array(7).fill(null)))
    : Array.from({ length: 4 }, () => Array(7).fill(null));
}

function serializeSeatHands(seatHands) {
  const normalized = cloneSeatHands(seatHands);
  const out = {};
  for (let seat = 0; seat < 4; seat += 1) {
    out[String(seat)] = Array.isArray(normalized[seat]) ? normalized[seat].slice(0, 7) : Array(7).fill(null);
  }
  return out;
}

function normalizeSeatHands(raw, fallbackDeckOrder = []) {
  const fallback = buildSeatHands(fallbackDeckOrder) || Array.from({ length: 4 }, () => Array(7).fill(null));
  let source = null;

  if (Array.isArray(raw) && raw.length === 4) {
    source = raw;
  } else if (raw && typeof raw === "object") {
    source = Array.from({ length: 4 }, (_, seat) => raw[String(seat)] ?? raw[seat] ?? null);
  }

  if (!Array.isArray(source) || source.length !== 4) return fallback;

  return source.map((hand, seat) => {
    const base = Array.isArray(fallback[seat]) ? fallback[seat] : Array(7).fill(null);
    if (!Array.isArray(hand) || hand.length !== 7) return base.slice();
    return hand.map((tileId, slot) => {
      if (tileId === null) return null;
      const parsed = Number(tileId);
      return Number.isFinite(parsed) && TILE_VALUES[parsed] ? parsed : base[slot];
    });
  });
}

function getHumanSeatSet(room = {}) {
  const humans = new Set(
    Object.values(getRoomSeats(room))
      .map((seat) => Number(seat))
      .filter((seat) => Number.isFinite(seat) && seat >= 0 && seat < 4)
  );
  const takeoverSeats = getBotTakeoverSeatSet(room);
  takeoverSeats.forEach((seat) => humans.delete(seat));
  return humans;
}

function getBotTakeoverSeatSet(room = {}) {
  return new Set(
    Array.isArray(room.botTakeoverSeats)
      ? room.botTakeoverSeats
        .map((seat) => Number(seat))
        .filter((seat) => Number.isFinite(seat) && seat >= 0 && seat < 4)
      : []
  );
}

function getBlockedRejoinSet(room = {}) {
  return new Set(
    Array.isArray(room.blockedRejoinUids)
      ? room.blockedRejoinUids.map((uid) => String(uid || "").trim()).filter(Boolean)
      : []
  );
}

function isSeatHuman(room = {}, seat) {
  return getHumanSeatSet(room).has(Number(seat));
}

function findSeatWithTile(seatHands, tileId) {
  for (let seat = 0; seat < 4; seat += 1) {
    const hand = Array.isArray(seatHands?.[seat]) ? seatHands[seat] : [];
    for (let slot = 0; slot < 7; slot += 1) {
      if (hand[slot] === tileId) return seat;
    }
  }
  return -1;
}

function findSeatSlotByTileId(seatHands, seat, tileId) {
  const hand = Array.isArray(seatHands?.[seat]) ? seatHands[seat] : [];
  for (let slot = 0; slot < hand.length; slot += 1) {
    if (hand[slot] === tileId) return slot;
  }
  return -1;
}

function countRemainingTilesForSeat(seatHands, seat) {
  const hand = Array.isArray(seatHands?.[seat]) ? seatHands[seat] : [];
  return hand.reduce((count, tileId) => count + (tileId === null ? 0 : 1), 0);
}

function sumSeatPips(seatHands, seat) {
  const hand = Array.isArray(seatHands?.[seat]) ? seatHands[seat] : [];
  return hand.reduce((sum, tileId) => {
    if (tileId === null) return sum;
    const values = getTileValues(tileId);
    return values ? sum + values[0] + values[1] : sum;
  }, 0);
}

function computeBlockedWinnerSeat(seatHands) {
  let bestSeat = 0;
  let bestScore = Number.POSITIVE_INFINITY;
  for (let seat = 0; seat < 4; seat += 1) {
    const score = sumSeatPips(seatHands, seat);
    if (score < bestScore) {
      bestScore = score;
      bestSeat = seat;
    }
  }
  return bestSeat;
}

function getWinnerUidForSeat(room, winnerSeat) {
  if (typeof winnerSeat !== "number" || winnerSeat < 0) return "";
  const seats = getRoomSeats(room);
  for (const [uid, seat] of Object.entries(seats)) {
    if (seat === winnerSeat) return uid;
  }
  return "";
}

function normalizeLegacyBranch(value, isOpeningMove = false) {
  const raw = String(value || "").trim().toLowerCase();
  if (isOpeningMove) return "centro";
  if (raw === "izquierda" || raw === "left") return "izquierda";
  if (raw === "derecha" || raw === "right") return "derecha";
  return "";
}

function normalizeRequestedSide(value, branch, isOpeningMove = false) {
  if (isOpeningMove) return "center";
  const raw = String(value || "").trim().toLowerCase();
  if (raw === "left" || raw === "izquierda") return "left";
  if (raw === "right" || raw === "derecha") return "right";
  const legacy = normalizeLegacyBranch(branch, false);
  if (legacy === "izquierda") return "left";
  if (legacy === "derecha") return "right";
  return "";
}

function getLegalMovesForSeat(state, seat) {
  const moves = [];
  const hand = Array.isArray(state?.seatHands?.[seat]) ? state.seatHands[seat] : [];
  const openingMove = safeSignedInt(state?.appliedActionSeq) < 0;

  for (let slot = 0; slot < hand.length; slot += 1) {
    const tileId = hand[slot];
    if (tileId === null) continue;
    const values = getTileValues(tileId);
    if (!values) continue;

    if (openingMove) {
      if (tileId === 27) {
        moves.push({
          tileId,
          slot,
          side: "center",
          branch: "centro",
          tileLeft: values[0],
          tileRight: values[1],
        });
      }
      continue;
    }

    if (values[0] === state.leftEnd || values[1] === state.leftEnd) {
      moves.push({
        tileId,
        slot,
        side: "left",
        branch: "izquierda",
        tileLeft: values[0],
        tileRight: values[1],
      });
    }
    if (values[0] === state.rightEnd || values[1] === state.rightEnd) {
      moves.push({
        tileId,
        slot,
        side: "right",
        branch: "derecha",
        tileLeft: values[0],
        tileRight: values[1],
      });
    }
  }

  moves.sort((a, b) => {
    const aValues = getTileValues(a.tileId) || [0, 0];
    const bValues = getTileValues(b.tileId) || [0, 0];
    return (bValues[0] + bValues[1]) - (aValues[0] + aValues[1]);
  });
  return moves;
}

function normalizeGameState(raw = {}, room = {}) {
  const deckOrder = Array.isArray(raw.deckOrder) && raw.deckOrder.length === 28
    ? raw.deckOrder.slice(0, 28)
    : (Array.isArray(room.deckOrder) ? room.deckOrder.slice(0, 28) : makeDeckOrder());
  const seatHands = normalizeSeatHands(raw.seatHands, deckOrder);
  const appliedActionSeq = Number.isFinite(Number(raw.appliedActionSeq)) ? Math.trunc(Number(raw.appliedActionSeq)) : -1;
  const winnerSeat = Number.isFinite(Number(raw.winnerSeat)) ? Math.trunc(Number(raw.winnerSeat)) : -1;
  const currentPlayer = Number.isFinite(Number(raw.currentPlayer))
    ? Math.trunc(Number(raw.currentPlayer))
    : Math.max(0, findSeatWithTile(seatHands, 27));

  return {
    deckOrder,
    seatHands,
    leftEnd: Number.isFinite(Number(raw.leftEnd)) ? Math.trunc(Number(raw.leftEnd)) : null,
    rightEnd: Number.isFinite(Number(raw.rightEnd)) ? Math.trunc(Number(raw.rightEnd)) : null,
    passesInRow: safeInt(raw.passesInRow),
    appliedActionSeq,
    currentPlayer,
    winnerSeat,
    winnerUid: String(raw.winnerUid || "").trim(),
    endedReason: sanitizeText(raw.endedReason || "", 40),
    idempotencyKeys: raw.idempotencyKeys && typeof raw.idempotencyKeys === "object" ? { ...raw.idempotencyKeys } : {},
  };
}

function trimIdempotencyKeys(keys = {}, maxEntries = 200) {
  const entries = Object.entries(keys).slice(-maxEntries);
  return Object.fromEntries(entries);
}

function createInitialGameState(room = {}, deckOrder = []) {
  const cleanDeckOrder = Array.isArray(deckOrder) && deckOrder.length === 28 ? deckOrder.slice(0, 28) : makeDeckOrder();
  const seatHands = buildSeatHands(cleanDeckOrder) || Array.from({ length: 4 }, () => Array(7).fill(null));
  const currentPlayer = Math.max(0, findSeatWithTile(seatHands, 27));
  return {
    deckOrder: cleanDeckOrder,
    seatHands,
    leftEnd: null,
    rightEnd: null,
    passesInRow: 0,
    appliedActionSeq: -1,
    currentPlayer,
    winnerSeat: -1,
    winnerUid: "",
    endedReason: "",
    idempotencyKeys: {},
  };
}

function sanitizePublicStep(step = {}) {
  const safeFields = Array.isArray(step.fields)
    ? step.fields.slice(0, 8).map((field) => ({
        type: sanitizeText(field?.type || "text", 20),
        name: sanitizeText(field?.name || "", 40),
        label: sanitizeText(field?.label || "", 80),
        required: field?.required === true,
        options: Array.isArray(field?.options)
          ? field.options.slice(0, 12).map((opt) => sanitizeText(opt, 80)).filter(Boolean)
          : [],
      }))
    : [];

  return {
    type: sanitizeText(step.type || "custom", 20),
    title: sanitizeText(step.title || "", 120),
    content: sanitizeText(step.content || "", MAX_PUBLIC_TEXT_LENGTH),
    buttonText: sanitizeText(step.buttonText || "", 40),
    description: sanitizeText(step.description || "", MAX_PUBLIC_TEXT_LENGTH),
    instruction: sanitizeText(step.instruction || "", MAX_PUBLIC_TEXT_LENGTH),
    message: sanitizeText(step.message || "", MAX_PUBLIC_TEXT_LENGTH),
    fields: safeFields,
  };
}

function sanitizePublicMethod(docSnap) {
  const data = docSnap.data() || {};
  if (data.isActive === false) return null;
  return {
    id: docSnap.id,
    name: sanitizeText(data.name || "Methode", 80),
    instructions: sanitizeText(data.instructions || "", MAX_PUBLIC_TEXT_LENGTH),
    image: sanitizePaymentMethodAsset(data.image || ""),
    qrCode: sanitizePaymentMethodAsset(data.qrCode || ""),
    accountName: sanitizeText(data.accountName || "", 120),
    phoneNumber: sanitizePhone(data.phoneNumber || ""),
    isActive: true,
    steps: Array.isArray(data.steps) ? data.steps.slice(0, 8).map((step) => sanitizePublicStep(step)) : [],
  };
}

function resolveRequestedMove(state, seat, rawAction = {}) {
  const type = String(rawAction?.type || "").trim();
  if (type !== "play" && type !== "pass") {
    throw new HttpsError("invalid-argument", "Type d'action invalide.");
  }

  if (type === "pass") {
    const legalMoves = getLegalMovesForSeat(state, seat);
    if (legalMoves.length > 0) {
      throw new HttpsError("failed-precondition", "Pass interdit tant qu'un coup légal existe.");
    }
    return {
      type: "pass",
      player: seat,
      tileId: null,
      tilePos: null,
      tileLeft: null,
      tileRight: null,
      side: null,
      branch: "",
      slot: -1,
    };
  }

  const hand = Array.isArray(state?.seatHands?.[seat]) ? state.seatHands[seat] : [];
  const legalMoves = getLegalMovesForSeat(state, seat);
  let tileId = Number(rawAction?.tileId);
  let slot = Number.isFinite(tileId) ? findSeatSlotByTileId(state.seatHands, seat, Math.trunc(tileId)) : -1;
  tileId = Number.isFinite(tileId) ? Math.trunc(tileId) : -1;

  const tilePosRaw = Number(rawAction?.tilePos);
  if ((slot < 0 || !TILE_VALUES[tileId]) && Number.isFinite(tilePosRaw)) {
    const tilePos = Math.trunc(tilePosRaw);
    const seatFromPos = Math.floor(tilePos / 7);
    const slotFromPos = tilePos % 7;
    if (seatFromPos !== seat || slotFromPos < 0 || slotFromPos > 6) {
      throw new HttpsError("permission-denied", "Tuile invalide pour ce joueur.");
    }
    const tileAtSlot = hand[slotFromPos];
    if (tileAtSlot === null || !TILE_VALUES[tileAtSlot]) {
      throw new HttpsError("failed-precondition", "Cette tuile n'est plus dans ta main.");
    }
    tileId = tileAtSlot;
    slot = slotFromPos;
  }

  if (slot < 0 || !TILE_VALUES[tileId]) {
    throw new HttpsError("failed-precondition", "Tuile introuvable dans la main du joueur.");
  }

  const matchingMoves = legalMoves.filter((move) => move.tileId === tileId && move.slot === slot);
  if (matchingMoves.length === 0) {
    throw new HttpsError("failed-precondition", "Coup illégal pour cette tuile.");
  }

  const openingMove = safeSignedInt(state?.appliedActionSeq) < 0;
  const requestedSide = normalizeRequestedSide(rawAction?.side, rawAction?.branch, openingMove);
  let selectedMove = null;

  if (requestedSide && requestedSide !== "center") {
    selectedMove = matchingMoves.find((move) => move.side === requestedSide) || null;
  } else if (matchingMoves.length === 1) {
    selectedMove = matchingMoves[0];
  } else if (openingMove) {
    selectedMove = matchingMoves[0];
  }

  if (!selectedMove) {
    throw new HttpsError("failed-precondition", "Précise un côté valide pour jouer cette tuile.");
  }

  return {
    type: "play",
    player: seat,
    tileId,
    tilePos: (seat * 7) + slot,
    tileLeft: selectedMove.tileLeft,
    tileRight: selectedMove.tileRight,
    side: selectedMove.side,
    branch: selectedMove.branch,
    slot,
  };
}

function applyResolvedMove(state, room, move, actorUid) {
  const nextState = normalizeGameState(state, room);
  const seq = safeInt(nextState.appliedActionSeq + 1);
  const record = {
    seq,
    type: move.type,
    player: move.player,
    tileId: move.type === "play" ? move.tileId : null,
    tilePos: move.type === "play" ? move.tilePos : null,
    tileLeft: move.type === "play" ? move.tileLeft : null,
    tileRight: move.type === "play" ? move.tileRight : null,
    side: move.type === "play" ? move.side : null,
    branch: move.type === "play" ? move.branch : "",
    resolvedPlacement: move.type === "play" ? move.branch : "pass",
    by: String(actorUid || ""),
  };

  if (move.type === "play") {
    const values = getTileValues(move.tileId);
    if (!values) {
      throw new HttpsError("failed-precondition", "Tuile inconnue.");
    }
    if (!Array.isArray(nextState.seatHands[move.player]) || nextState.seatHands[move.player][move.slot] !== move.tileId) {
      throw new HttpsError("failed-precondition", "La tuile a déjà été consommée.");
    }
    nextState.seatHands[move.player][move.slot] = null;

    if (safeSignedInt(nextState.appliedActionSeq) < 0) {
      if (move.tileId !== 27) {
        throw new HttpsError("failed-precondition", "La partie doit commencer par le double six.");
      }
      nextState.leftEnd = values[0];
      nextState.rightEnd = values[1];
    } else if (move.side === "left") {
      if (values[0] !== nextState.leftEnd && values[1] !== nextState.leftEnd) {
        throw new HttpsError("failed-precondition", "Placement incompatible à gauche.");
      }
      nextState.leftEnd = values[0] === nextState.leftEnd ? values[1] : values[0];
    } else if (move.side === "right") {
      if (values[0] !== nextState.rightEnd && values[1] !== nextState.rightEnd) {
        throw new HttpsError("failed-precondition", "Placement incompatible à droite.");
      }
      nextState.rightEnd = values[0] === nextState.rightEnd ? values[1] : values[0];
    } else {
      throw new HttpsError("failed-precondition", "Côté de pose invalide.");
    }

    nextState.passesInRow = 0;
    if (countRemainingTilesForSeat(nextState.seatHands, move.player) === 0) {
      nextState.winnerSeat = move.player;
      nextState.winnerUid = getWinnerUidForSeat(room, move.player);
      nextState.endedReason = "out";
    }
  } else {
    const legalMoves = getLegalMovesForSeat(nextState, move.player);
    if (legalMoves.length > 0) {
      throw new HttpsError("failed-precondition", "Pass interdit tant qu'un coup légal existe.");
    }
    nextState.passesInRow = safeInt(nextState.passesInRow) + 1;
    if (nextState.passesInRow >= 4) {
      nextState.winnerSeat = computeBlockedWinnerSeat(nextState.seatHands);
      nextState.winnerUid = getWinnerUidForSeat(room, nextState.winnerSeat);
      nextState.endedReason = "block";
    }
  }

  nextState.appliedActionSeq = seq;
  if (nextState.winnerSeat < 0) {
    nextState.currentPlayer = (move.player + 1) % 4;
  }

  return {
    state: nextState,
    record,
    ended: nextState.winnerSeat >= 0,
  };
}

function buildPassMoveForSeat(seat) {
  return {
    type: "pass",
    player: seat,
    tileId: null,
    tilePos: null,
    tileLeft: null,
    tileRight: null,
    side: null,
    branch: "",
    slot: -1,
  };
}

function buildPlayMoveFromLegal(seat, move) {
  return {
    type: "play",
    player: seat,
    tileId: move.tileId,
    tilePos: (seat * 7) + move.slot,
    tileLeft: move.tileLeft,
    tileRight: move.tileRight,
    side: move.side,
    branch: move.branch,
    slot: move.slot,
  };
}

function getOtherSeats(seat) {
  const seats = [];
  for (let i = 0; i < 4; i += 1) {
    if (i !== seat) seats.push(i);
  }
  return seats;
}

function countValueMatchesInSeatHand(state, seat, value) {
  if (!Number.isFinite(value)) return 0;
  const hand = Array.isArray(state?.seatHands?.[seat]) ? state.seatHands[seat] : [];
  let count = 0;
  for (let i = 0; i < hand.length; i += 1) {
    const tileId = hand[i];
    if (tileId === null) continue;
    const values = getTileValues(tileId);
    if (!values) continue;
    if (values[0] === value) count += 1;
    if (values[1] === value) count += 1;
  }
  return count;
}

function countValueMatchesForSeats(state, seats, value) {
  return seats.reduce((sum, seat) => sum + countValueMatchesInSeatHand(state, seat, value), 0);
}

function countImmediateWinThreat(state, seat) {
  const remaining = countRemainingTilesForSeat(state.seatHands, seat);
  if (remaining <= 0) return 0;
  const legal = getLegalMovesForSeat(state, seat).length;
  if (legal <= 0) return 0;
  if (remaining === 1) return 1;
  if (remaining === 2) return 0.45;
  return 0;
}

function sleepMs(delayMs = 0) {
  const wait = Math.max(0, safeInt(delayMs));
  if (!wait) return Promise.resolve();
  return new Promise((resolve) => {
    setTimeout(resolve, wait);
  });
}

function scoreStateForSeat(room, state, perspectiveSeat) {
  const winnerSeat = Number.isFinite(Number(state?.winnerSeat))
    ? Math.trunc(Number(state.winnerSeat))
    : -1;
  const perspectiveIsHuman = isSeatHuman(room, perspectiveSeat);
  const otherSeats = getOtherSeats(perspectiveSeat);
  const humanOpponents = otherSeats.filter((seat) => isSeatHuman(room, seat));
  const robotPeers = otherSeats.filter((seat) => !isSeatHuman(room, seat));

  if (winnerSeat >= 0) {
    if (winnerSeat === perspectiveSeat) return 1_000_000;
    if (perspectiveIsHuman) return -1_000_000;
    return isSeatHuman(room, winnerSeat) ? -900_000 : -220_000;
  }

  const selfTiles = countRemainingTilesForSeat(state.seatHands, perspectiveSeat);
  const selfPips = sumSeatPips(state.seatHands, perspectiveSeat);
  const selfLegal = getLegalMovesForSeat(state, perspectiveSeat).length;
  const leftEnd = Number(state?.leftEnd);
  const rightEnd = Number(state?.rightEnd);

  const selfLeftControl = countValueMatchesInSeatHand(state, perspectiveSeat, leftEnd);
  const selfRightControl = countValueMatchesInSeatHand(state, perspectiveSeat, rightEnd);
  const selfControl = selfLeftControl + selfRightControl;

  const humanLegalTotal = humanOpponents.reduce((sum, seat) => sum + getLegalMovesForSeat(state, seat).length, 0);
  const humanThreat = humanOpponents.reduce((sum, seat) => sum + countImmediateWinThreat(state, seat), 0);
  const humanPipsTotal = humanOpponents.reduce((sum, seat) => sum + sumSeatPips(state.seatHands, seat), 0);
  const humanReach = countValueMatchesForSeats(state, humanOpponents, leftEnd) + countValueMatchesForSeats(state, humanOpponents, rightEnd);

  const robotLegalTotal = robotPeers.reduce((sum, seat) => sum + getLegalMovesForSeat(state, seat).length, 0);
  const robotThreat = robotPeers.reduce((sum, seat) => sum + countImmediateWinThreat(state, seat), 0);
  const robotReach = countValueMatchesForSeats(state, robotPeers, leftEnd) + countValueMatchesForSeats(state, robotPeers, rightEnd);

  let score = 0;
  score += (7 - selfTiles) * 260;
  score -= selfPips * (safeInt(state?.passesInRow) >= 2 ? 28 : 14);
  score += selfLegal * 22;
  score += selfControl * 34;

  if (Number.isFinite(leftEnd) && Number.isFinite(rightEnd) && leftEnd !== rightEnd) {
    score += Math.abs(selfLeftControl - selfRightControl) <= 1 ? 10 : 0;
  }

  if (perspectiveIsHuman) {
    const opponentLegalTotal = otherSeats.reduce((sum, seat) => sum + getLegalMovesForSeat(state, seat).length, 0);
    const opponentThreat = otherSeats.reduce((sum, seat) => sum + countImmediateWinThreat(state, seat), 0);
    const opponentReach = countValueMatchesForSeats(state, otherSeats, leftEnd) + countValueMatchesForSeats(state, otherSeats, rightEnd);
    score -= opponentLegalTotal * 18;
    score -= opponentThreat * 420;
    score -= opponentReach * 8;
  } else {
    score -= humanLegalTotal * 24;
    score -= humanThreat * 620;
    score += humanPipsTotal * 1.35;
    score -= humanReach * 12;

    // Les robots cherchent leur victoire, mais n'essaient pas de fermer le jeu aux autres robots.
    score += robotLegalTotal * 7;
    score += robotReach * 4;
    score -= robotThreat * 45;
  }

  const nextSeat = safeSignedInt(state?.currentPlayer);
  if (nextSeat >= 0 && nextSeat < 4 && nextSeat !== perspectiveSeat) {
    const nextIsHuman = isSeatHuman(room, nextSeat);
    const nextLegal = getLegalMovesForSeat(state, nextSeat).length;
    const nextRemaining = countRemainingTilesForSeat(state.seatHands, nextSeat);
    if (nextIsHuman) {
      if (nextLegal === 0) score += 140;
      if (nextRemaining === 1 && nextLegal > 0) score -= 380;
    } else if (!perspectiveIsHuman) {
      if (nextLegal === 0) score += 18;
      if (nextRemaining === 1 && nextLegal > 0) score -= 28;
    } else if (nextRemaining === 1 && nextLegal > 0) {
      score -= 180;
    }
  }

  return score;
}

function chooseStrategicMove(room, state, seat, options = {}) {
  const legalMoves = getLegalMovesForSeat(state, seat);
  const lookahead = Math.max(0, safeInt(options.lookaheadPlies));
  if (legalMoves.length === 0) return buildPassMoveForSeat(seat);

  let bestMove = buildPlayMoveFromLegal(seat, legalMoves[0]);
  let bestScore = Number.NEGATIVE_INFINITY;

  for (let i = 0; i < legalMoves.length; i += 1) {
    const legalMove = legalMoves[i];
    const candidate = buildPlayMoveFromLegal(seat, legalMove);
    let simulated = null;

    try {
      simulated = applyResolvedMove(state, room, candidate, `server:bot:eval:${seat}`);
    } catch (_) {
      continue;
    }

    const tileValues = getTileValues(candidate.tileId) || [0, 0];
    let score = scoreStateForSeat(room, simulated.state, seat);
    score += (tileValues[0] + tileValues[1]) * 6;

    if (lookahead > 0 && simulated.state.winnerSeat < 0) {
      let lookState = simulated.state;
      let weight = 0.62;
      for (let ply = 0; ply < lookahead && lookState.winnerSeat < 0; ply += 1) {
        const actor = safeSignedInt(lookState.currentPlayer);
        if (actor < 0 || actor > 3) break;
        const predicted = chooseStrategicMove(room, lookState, actor, { lookaheadPlies: 0 });
        const predictedResult = applyResolvedMove(lookState, room, predicted, `server:bot:sim:${actor}`);
        lookState = predictedResult.state;
        score += weight * scoreStateForSeat(room, lookState, seat);
        weight *= 0.62;
      }
    }

    if (score > bestScore) {
      bestScore = score;
      bestMove = candidate;
      continue;
    }

    if (score === bestScore) {
      const bestValues = getTileValues(bestMove.tileId) || [0, 0];
      const bestPips = bestValues[0] + bestValues[1];
      const candidatePips = tileValues[0] + tileValues[1];
      if (candidatePips > bestPips) {
        bestMove = candidate;
      }
    }
  }

  return bestMove;
}

function chooseBotMove(room, state, seat) {
  const difficulty = normalizeBotDifficulty(room?.botDifficulty);
  if (difficulty === "userpro") {
    const legalMoves = getLegalMovesForSeat(state, seat);
    if (legalMoves.length === 0) return buildPassMoveForSeat(seat);
    const randomIdx = Math.floor(Math.random() * legalMoves.length);
    return buildPlayMoveFromLegal(seat, legalMoves[randomIdx]);
  }
  return chooseStrategicMove(room, state, seat, {
    lookaheadPlies: safeInt(BOT_DIFFICULTY_LOOKAHEAD[difficulty]),
  });
}

function computeBotThinkDelayMs(room, state, seat) {
  const legalMoves = getLegalMovesForSeat(state, seat);
  if (legalMoves.length === 0) {
    return randomInt(BOT_THINK_DELAY_PASS_MIN_MS, BOT_THINK_DELAY_PASS_MAX_MS);
  }

  const difficulty = normalizeBotDifficulty(room?.botDifficulty);
  const branchingCount = Math.max(0, legalMoves.length - 1);
  const openingResponse = safeSignedInt(state?.appliedActionSeq) <= 0;

  let min = BOT_THINK_DELAY_MIN_MS;
  let max = BOT_THINK_DELAY_MAX_MS;

  if (openingResponse) {
    min += 180;
    max += 420;
  }

  min += Math.min(900, branchingCount * 220);
  max += Math.min(1700, branchingCount * 420);

  if (difficulty === "ultra") {
    min += 180;
    max += 360;
  } else if (difficulty === "amateur") {
    min = Math.max(900, min - 140);
    max = Math.max(min + 220, max - 180);
  }

  return randomInt(min, max);
}

function resolveBotTurnLockUntilMs(room, state, nowMs = Date.now()) {
  if (!state || safeSignedInt(state?.winnerSeat) >= 0) return 0;
  if (room?.startRevealPending === true) return 0;

  const botSeat = safeSignedInt(state?.currentPlayer);
  if (botSeat < 0 || botSeat > 3 || isSeatHuman(room, botSeat)) {
    return 0;
  }

  const safeNowMs = safeSignedInt(nowMs) || Date.now();
  const currentLockUntilMs = safeSignedInt(room?.turnLockedUntilMs);
  if (currentLockUntilMs > safeNowMs) {
    return currentLockUntilMs;
  }

  return safeNowMs + computeBotThinkDelayMs(room, state, botSeat);
}

function buildOpeningMoveForState(state) {
  const liveState = normalizeGameState(state);
  const openingSeat = findSeatWithTile(liveState.seatHands, 27);
  if (openingSeat < 0 || openingSeat > 3) {
    throw new HttpsError("failed-precondition", "Impossible de trouver le double six pour demarrer.");
  }

  const legalMoves = getLegalMovesForSeat(liveState, openingSeat);
  const openingMove = legalMoves.find((move) => move.tileId === 27) || null;
  if (!openingMove) {
    throw new HttpsError("failed-precondition", "Le double six ne peut pas ouvrir la partie.");
  }

  return {
    type: "play",
    player: openingSeat,
    tileId: openingMove.tileId,
    tilePos: (openingSeat * 7) + openingMove.slot,
    tileLeft: openingMove.tileLeft,
    tileRight: openingMove.tileRight,
    side: openingMove.side,
    branch: openingMove.branch,
    slot: openingMove.slot,
  };
}

function advanceBotsAndCollect(room, state, roomId, firstMove = null, actorUid = "", allowBotAdvance = true) {
  let liveState = normalizeGameState(state, room);
  const records = [];
  let autoBotMoves = 0;

  if (firstMove) {
    const result = applyResolvedMove(liveState, room, firstMove, actorUid);
    liveState = result.state;
    records.push({
      ...result.record,
      roomId,
    });
  }

  while (allowBotAdvance === true && liveState.winnerSeat < 0 && autoBotMoves < 12) {
    const botSeat = safeSignedInt(liveState.currentPlayer);
    if (botSeat < 0 || botSeat > 3 || isSeatHuman(room, botSeat)) {
      break;
    }

    const botMove = chooseBotMove(room, liveState, botSeat);
    const result = applyResolvedMove(liveState, room, botMove, "server:bot");
    liveState = result.state;
    records.push({
      ...result.record,
      roomId,
    });
    autoBotMoves += 1;
  }

  return {
    state: liveState,
    records,
  };
}

function applyActionBatchInTransaction(tx, roomRefDoc, room, state, roomId, firstMove = null, actorUid = "", options = {}) {
  const batchResult = advanceBotsAndCollect(
    room,
    state,
    roomId,
    firstMove,
    actorUid,
    options?.allowBotAdvance !== false
  );
  batchResult.records.forEach((record) => {
    const actionRef = roomRefDoc.collection("actions").doc(String(record.seq));
    tx.set(actionRef, {
      ...record,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
    });
  });
  return batchResult;
}

function buildRoomUpdateFromGameState(room, nextState, records = []) {
  const lastRecord = records.length > 0 ? records[records.length - 1] : null;
  const playedCountDelta = records.reduce((count, item) => count + (item.type === "play" ? 1 : 0), 0);
  const nextActionSeq = safeInt(nextState.appliedActionSeq + 1);
  const nextTurnLockUntilMs = resolveBotTurnLockUntilMs(room, nextState, Date.now());
  const update = {
    nextActionSeq,
    lastActionSeq: nextState.appliedActionSeq,
    currentPlayer: nextState.currentPlayer,
    turnActual: nextActionSeq,
    turnStartedAt: admin.firestore.FieldValue.serverTimestamp(),
    playedCount: safeInt(room.playedCount) + playedCountDelta,
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    turnLockedUntilMs: nextTurnLockUntilMs,
    deckOrder: admin.firestore.FieldValue.delete(),
  };

  if (lastRecord) {
    update.lastMove = {
      seq: lastRecord.seq,
      type: lastRecord.type,
      player: lastRecord.player,
      tileId: lastRecord.tileId,
      tilePos: lastRecord.tilePos,
      tileLeft: lastRecord.tileLeft,
      tileRight: lastRecord.tileRight,
      side: lastRecord.side,
      branch: lastRecord.branch,
    };
  }

  if (nextState.winnerSeat >= 0) {
    update.status = "ended";
    update.winnerSeat = nextState.winnerSeat;
    update.winnerUid = nextState.winnerUid;
    update.endedReason = nextState.endedReason || "out";
    update.endedAt = admin.firestore.FieldValue.serverTimestamp();
    update.endedAtMs = Date.now();
    update.endClicks = {};
  }

  return update;
}

async function processPendingBotTurns(roomId) {
  const safeRoomId = String(roomId || "").trim();
  if (!safeRoomId) return;

  const roomRef = db.collection(ROOMS_COLLECTION).doc(safeRoomId);
  const stateRef = gameStateRef(safeRoomId);

  while (true) {
    const roomSnap = await roomRef.get();
    if (!roomSnap.exists) return;
    const room = roomSnap.data() || {};

    if (String(room.status || "") !== "playing") return;
    if (room.startRevealPending === true) return;
    const roomWinnerSeat = Number.isFinite(Number(room.winnerSeat))
      ? Math.trunc(Number(room.winnerSeat))
      : -1;
    if (roomWinnerSeat >= 0) return;

    const botSeat = safeSignedInt(room.currentPlayer);
    if (botSeat < 0 || botSeat > 3 || isSeatHuman(room, botSeat)) {
      return;
    }

    const outcome = await db.runTransaction(async (tx) => {
      const [liveRoomSnap, stateSnap] = await Promise.all([
        tx.get(roomRef),
        tx.get(stateRef),
      ]);

      if (!liveRoomSnap.exists) {
        return { processed: false, stop: true };
      }

      const liveRoom = liveRoomSnap.data() || {};
      if (String(liveRoom.status || "") !== "playing") {
        return { processed: false, stop: true };
      }
      if (liveRoom.startRevealPending === true) {
        return { processed: false, stop: true };
      }
      const liveWinnerSeat = Number.isFinite(Number(liveRoom.winnerSeat))
        ? Math.trunc(Number(liveRoom.winnerSeat))
        : -1;
      if (liveWinnerSeat >= 0) {
        return { processed: false, stop: true };
      }

      const liveBotSeat = safeSignedInt(liveRoom.currentPlayer);
      if (liveBotSeat < 0 || liveBotSeat > 3 || isSeatHuman(liveRoom, liveBotSeat)) {
        return { processed: false, stop: true };
      }

      const currentState = stateSnap.exists
        ? normalizeGameState(
            stateSnap.data(),
            liveRoom
          )
        : createInitialGameState(
            liveRoom,
            Array.isArray(liveRoom.deckOrder) && liveRoom.deckOrder.length === 28 ? liveRoom.deckOrder : makeDeckOrder()
          );

      if (currentState.winnerSeat >= 0) {
        return { processed: false, stop: true };
      }

      const safeNowMs = Date.now();
      const lockedUntilMs = safeSignedInt(liveRoom.turnLockedUntilMs);
      if (lockedUntilMs > safeNowMs) {
        return { processed: false, stop: true };
      }

      if (lockedUntilMs <= 0) {
        const scheduledUntilMs = resolveBotTurnLockUntilMs(liveRoom, currentState, safeNowMs);
        if (scheduledUntilMs > safeNowMs) {
          tx.update(roomRef, {
            turnLockedUntilMs: scheduledUntilMs,
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          });
          return { processed: false, stop: true };
        }
      }

      const botMove = chooseBotMove(liveRoom, currentState, liveBotSeat);
      const batchResult = applyActionBatchInTransaction(
        tx,
        roomRef,
        liveRoom,
        currentState,
        safeRoomId,
        botMove,
        "server:bot",
        { allowBotAdvance: false }
      );
      const nextState = batchResult.state;
      tx.set(stateRef, buildGameStateWrite(nextState), { merge: true });
      tx.update(roomRef, buildRoomUpdateFromGameState(liveRoom, nextState, batchResult.records));

      return {
        processed: true,
        stop: nextState.winnerSeat >= 0 || isSeatHuman(liveRoom, nextState.currentPlayer),
      };
    });

    if (!outcome) return;
    if (!outcome.processed || outcome.stop) {
      return;
    }
  }
}

function buildGameStateWrite(nextState) {
  return {
    deckOrder: nextState.deckOrder,
    seatHands: serializeSeatHands(nextState.seatHands),
    leftEnd: nextState.leftEnd,
    rightEnd: nextState.rightEnd,
    passesInRow: nextState.passesInRow,
    appliedActionSeq: nextState.appliedActionSeq,
    currentPlayer: nextState.currentPlayer,
    winnerSeat: nextState.winnerSeat,
    winnerUid: nextState.winnerUid,
    endedReason: nextState.endedReason,
    idempotencyKeys: trimIdempotencyKeys(nextState.idempotencyKeys),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };
}

async function applyWalletMutationTx(tx, options) {
  const uid = String(options.uid || "").trim();
  const email = String(options.email || "").trim();
  const type = String(options.type || "mutation").trim();
  const note = String(options.note || "");
  const deltaDoes = safeSignedInt(options.deltaDoes);
  const deltaExchangedGourdes = safeSignedInt(options.deltaExchangedGourdes);
  const amountGourdes = safeInt(options.amountGourdes);
  const amountDoes = safeInt(options.amountDoes);
  const provisionalDepositsEnabled = options.provisionalDepositsEnabled === true;
  const rewardApprovedDoes = safeInt(
    typeof options.approvedRewardDoes === "number"
      ? options.approvedRewardDoes
      : (type === "game_reward" ? amountDoes : 0)
  );
  const rewardProvisionalDoes = safeInt(options.provisionalRewardDoes);

  const ref = walletRef(uid);
  const snap = await tx.get(ref);
  const data = snap.exists ? (snap.data() || {}) : {};
  if (type === "xchange_buy" || type === "xchange_sell" || type === "game_entry") {
    assertWalletNotFrozen(data);
  }

  const beforeDoes = safeInt(data.doesBalance);
  let beforeExchanged = safeSignedInt(data.exchangedGourdes);
  const beforePendingFromXchange = safeInt(data.pendingPlayFromXchangeDoes);
  const beforePendingFromReferral = safeInt(data.pendingPlayFromReferralDoes);
  let beforeTotalExchangedEver = safeInt(data.totalExchangedHtgEver);
  const beforeProvisionalDoes = safeInt(data.doesProvisionalBalance);
  const beforeApprovedDoes = safeInt(
    typeof data.doesApprovedBalance === "number"
      ? data.doesApprovedBalance
      : Math.max(0, beforeDoes - beforeProvisionalDoes)
  );
  const beforePendingPlayTotal = beforePendingFromXchange + beforePendingFromReferral;
  const beforeExchangeableDoes = safeInt(
    typeof data.exchangeableDoesAvailable === "number"
      ? Math.min(data.exchangeableDoesAvailable, beforeApprovedDoes)
      : (beforePendingPlayTotal <= 0 ? beforeApprovedDoes : 0)
  );

  let afterApprovedDoes = beforeApprovedDoes;
  let afterProvisionalDoes = beforeProvisionalDoes;
  let afterExchanged = safeSignedInt(beforeExchanged + deltaExchangedGourdes);
  let afterExchangeableDoes = beforeExchangeableDoes;

  let afterPendingFromXchange = beforePendingFromXchange;
  let afterPendingFromReferral = beforePendingFromReferral;
  let afterTotalExchangedEver = beforeTotalExchangedEver;
  let fundingPatch = {};
  let gameEntryFunding = {
    approvedDoes: 0,
    provisionalDoes: 0,
    provisionalSources: [],
  };
  let provisionalConversion = {
    consumedGourdes: 0,
    consumedDoes: 0,
    sources: [],
  };

  const orderCollectionRef = db.collection(CLIENTS_COLLECTION).doc(uid).collection("orders");
  const xchangeHistoryCollectionRef = walletHistoryRef(uid);
  let cachedOrders = null;
  let cachedExchangeHistory = null;

  const loadAllOrders = async () => {
    if (cachedOrders) return cachedOrders;
    const ordersSnap = await tx.get(orderCollectionRef);
    cachedOrders = ordersSnap.docs.map((item) => ({
      id: item.id,
      ref: item.ref,
      data: item.data() || {},
    }));
    return cachedOrders;
  };

  const loadExchangeHistory = async () => {
    if (cachedExchangeHistory) return cachedExchangeHistory;
    const historySnap = await tx.get(xchangeHistoryCollectionRef);
    cachedExchangeHistory = historySnap.docs.map((item) => item.data() || {});
    return cachedExchangeHistory;
  };

  const consumeProvisionalHtgForConversion = async (requestedAmountHtg = 0) => {
    const remainingTarget = safeInt(requestedAmountHtg);
    if (remainingTarget <= 0 || !provisionalDepositsEnabled) {
      return {
        consumedGourdes: 0,
        consumedDoes: 0,
        sources: [],
      };
    }

    const orders = await loadAllOrders();
    let remaining = remainingTarget;
    const sources = [];

    orders
      .filter((item) => getOrderResolutionStatus(item.data) === "pending" && getOrderProvisionalHtgRemaining(item.data) > 0)
      .sort((left, right) => safeInt(left.data.createdAtMs) - safeInt(right.data.createdAtMs))
      .forEach((item) => {
        if (remaining <= 0) return;
        const currentRemaining = getOrderProvisionalHtgRemaining(item.data);
        if (currentRemaining <= 0) return;
        const used = Math.min(remaining, currentRemaining);
        remaining -= used;
        tx.set(item.ref, {
          provisionalHtgRemaining: currentRemaining - used,
          provisionalHtgConverted: safeInt(item.data.provisionalHtgConverted) + used,
          provisionalDoesRemaining: safeInt(item.data.provisionalDoesRemaining) + (used * RATE_HTG_TO_DOES),
          updatedAt: new Date().toISOString(),
          updatedAtMs: Date.now(),
        }, { merge: true });
        sources.push({
          orderId: item.id,
          amountGourdes: used,
          amountDoes: used * RATE_HTG_TO_DOES,
        });
      });

    const consumedGourdes = remainingTarget - remaining;
    return {
      consumedGourdes,
      consumedDoes: consumedGourdes * RATE_HTG_TO_DOES,
      sources,
    };
  };

  const consumeProvisionalDoesForGameEntry = async (requestedAmountDoes = 0) => {
    const target = safeInt(requestedAmountDoes);
    if (target <= 0 || !provisionalDepositsEnabled || beforeProvisionalDoes <= 0) {
      return {
        consumedDoes: 0,
        sources: [],
        coveredByOrdersDoes: 0,
      };
    }

    const orders = await loadAllOrders();
    let remaining = Math.min(target, beforeProvisionalDoes);
    const sources = [];

    orders
      .filter((item) => getOrderResolutionStatus(item.data) === "pending")
      .sort((left, right) => safeInt(left.data.createdAtMs) - safeInt(right.data.createdAtMs))
      .forEach((item) => {
        if (remaining <= 0) return;
        const currentCapitalDoes = getOrderProvisionalCapitalDoesBalance(item.data);
        const currentGainDoes = safeInt(item.data.provisionalGainDoes);
        const totalAvailable = currentCapitalDoes + currentGainDoes;
        if (totalAvailable <= 0) return;

        const used = Math.min(remaining, totalAvailable);
        remaining -= used;

        const usedCapital = Math.min(used, currentCapitalDoes);
        const usedGain = used - usedCapital;

        tx.set(item.ref, {
          provisionalDoesRemaining: currentCapitalDoes - usedCapital,
          provisionalGainDoes: currentGainDoes - usedGain,
          provisionalDoesPlayed: safeInt(item.data.provisionalDoesPlayed) + used,
          updatedAt: new Date().toISOString(),
          updatedAtMs: Date.now(),
        }, { merge: true });

        sources.push({
          orderId: item.id,
          amountDoes: used,
        });
      });

    return {
      consumedDoes: target - remaining,
      sources,
      coveredByOrdersDoes: target - remaining,
    };
  };

  if (type === "xchange_buy" || type === "xchange_sell") {
    const [allOrders, exchangeHistory] = await Promise.all([
      loadAllOrders(),
      loadExchangeHistory(),
    ]);
    const reconciledExchangeLedger = buildApprovedExchangeLedger({
      orders: allOrders.map((item) => item.data || {}),
      walletData: data,
      exchangeHistory,
    });
    beforeExchanged = safeSignedInt(reconciledExchangeLedger.exchangedApprovedHtg);
    beforeTotalExchangedEver = safeInt(reconciledExchangeLedger.totalExchangedApprovedHtg);
    afterExchanged = safeSignedInt(beforeExchanged + deltaExchangedGourdes);
    afterTotalExchangedEver = beforeTotalExchangedEver;
  }

  if (type === "xchange_buy") {
    const [allOrders, withdrawalsSnap] = await Promise.all([
      loadAllOrders(),
      tx.get(
        db.collection(CLIENTS_COLLECTION)
          .doc(uid)
          .collection("withdrawals")
      ),
    ]);
    const fundingSnapshot = buildWalletFundingSnapshot({
      orders: allOrders.map((item) => item.data || {}),
      withdrawals: withdrawalsSnap.docs.map((item) => item.data() || {}),
      walletData: {
        ...data,
        exchangedGourdes: beforeExchanged,
        totalExchangedHtgEver: beforeTotalExchangedEver,
      },
    });
    provisionalConversion = await consumeProvisionalHtgForConversion(amountGourdes);
    const remainingApprovedAmount = Math.max(0, amountGourdes - provisionalConversion.consumedGourdes);
    const availableApprovedToConvertHtg = safeInt(fundingSnapshot.approvedHtgAvailable);

    console.log("[BALANCE_DEBUG][FUNCTIONS][xchange_buy] snapshot", JSON.stringify({
      uid,
      amountGourdes,
      beforeExchanged,
      beforeTotalExchangedEver,
      availableApprovedToConvertHtg,
      remainingApprovedAmount,
      provisionalConversion,
      fundingSnapshot,
      orders: allOrders.map((item) => ({
        id: item.id,
        amount: computeOrderAmount(item.data || {}),
        status: String(item.data?.status || ""),
        resolutionStatus: getOrderResolutionStatus(item.data || {}),
        approvedAmountHtg: safeInt(item.data?.approvedAmountHtg),
        provisionalHtgRemaining: safeInt(item.data?.provisionalHtgRemaining),
        provisionalHtgConverted: safeInt(item.data?.provisionalHtgConverted),
        provisionalDoesRemaining: safeInt(item.data?.provisionalDoesRemaining),
        provisionalGainDoes: safeInt(item.data?.provisionalGainDoes),
        fundingVersion: safeInt(item.data?.fundingVersion),
        creditMode: String(item.data?.creditMode || ""),
      })),
      withdrawalsCount: withdrawalsSnap.size,
    }));

    if (amountGourdes <= 0 || remainingApprovedAmount > availableApprovedToConvertHtg) {
      console.warn("[BALANCE_DEBUG][FUNCTIONS][xchange_buy] rejected", JSON.stringify({
        uid,
        amountGourdes,
        remainingApprovedAmount,
        availableApprovedToConvertHtg,
        provisionalConversion,
        fundingSnapshot,
      }));
      throw new HttpsError("failed-precondition", "Montant supérieur au solde HTG disponible.");
    }

    afterProvisionalDoes += provisionalConversion.consumedDoes;
    if (remainingApprovedAmount > 0) {
      const approvedDoesDelta = remainingApprovedAmount * RATE_HTG_TO_DOES;
      afterApprovedDoes += approvedDoesDelta;
      afterExchanged = beforeExchanged + remainingApprovedAmount;
      afterTotalExchangedEver = beforeTotalExchangedEver + remainingApprovedAmount;
      afterPendingFromXchange = beforePendingFromXchange + approvedDoesDelta;
    }

    const nextOrders = allOrders.map((item) => {
      const source = provisionalConversion.sources.find((entry) => entry.orderId === String(item.id || ""));
      if (!source) return item;
      return {
        ...item.data,
        provisionalHtgRemaining: Math.max(0, safeInt(item.data.provisionalHtgRemaining) - safeInt(source.amountGourdes)),
        provisionalHtgConverted: safeInt(item.data.provisionalHtgConverted) + safeInt(source.amountGourdes),
        provisionalDoesRemaining: safeInt(item.data.provisionalDoesRemaining) + safeInt(source.amountDoes),
      };
    });
    fundingPatch = buildFundingWalletPatch(buildWalletFundingSnapshot({
      orders: nextOrders,
      withdrawals: withdrawalsSnap.docs.map((item) => item.data() || {}),
      walletData: {
        ...data,
        exchangedGourdes: afterExchanged,
        totalExchangedHtgEver: afterTotalExchangedEver,
        doesApprovedBalance: afterApprovedDoes,
        doesProvisionalBalance: afterProvisionalDoes,
        doesBalance: afterApprovedDoes + afterProvisionalDoes,
      },
    }));
  }

  if (type === "game_entry") {
    if (amountDoes > beforeDoes) {
      throw new HttpsError("failed-precondition", "Solde Does insuffisant.");
    }
    const provisionalSpend = await consumeProvisionalDoesForGameEntry(amountDoes);
    const provisionalCoveredByOrders = safeInt(provisionalSpend.coveredByOrdersDoes);
    const provisionalFallbackDoes = Math.max(
      0,
      Math.min(beforeProvisionalDoes, amountDoes) - provisionalCoveredByOrders
    );
    const provisionalSpentDoes = safeInt(provisionalCoveredByOrders + provisionalFallbackDoes);
    const approvedSpentDoes = Math.max(0, amountDoes - provisionalSpentDoes);

    afterProvisionalDoes = Math.max(0, afterProvisionalDoes - provisionalSpentDoes);
    afterApprovedDoes = Math.max(0, afterApprovedDoes - approvedSpentDoes);

    let playedApprovedDoes = approvedSpentDoes;
    if (playedApprovedDoes > 0 && afterPendingFromXchange > 0) {
      const consumeXchange = Math.min(playedApprovedDoes, afterPendingFromXchange);
      afterPendingFromXchange -= consumeXchange;
      playedApprovedDoes -= consumeXchange;
      afterExchangeableDoes += consumeXchange;
    }
    if (playedApprovedDoes > 0 && afterPendingFromReferral > 0) {
      const consumeReferral = Math.min(playedApprovedDoes, afterPendingFromReferral);
      afterPendingFromReferral -= consumeReferral;
      playedApprovedDoes -= consumeReferral;
      afterExchangeableDoes += consumeReferral;
    }

    gameEntryFunding = {
      approvedDoes: approvedSpentDoes,
      provisionalDoes: provisionalSpentDoes,
      provisionalSources: normalizeFundingSources(provisionalSpend.sources),
    };
    console.log("[BALANCE_DEBUG][FUNCTIONS][game_entry]", JSON.stringify({
      uid,
      amountDoes,
      beforeDoes,
      beforeApprovedDoes,
      beforeProvisionalDoes,
      provisionalCoveredByOrders,
      provisionalFallbackDoes,
      approvedSpentDoes,
      provisionalSpentDoes,
      afterApprovedDoes,
      afterProvisionalDoes,
      afterDoesPreview: safeInt(afterApprovedDoes + afterProvisionalDoes),
      gameEntryFunding,
    }));
  }

  if (type === "share_reward_bonus") {
    afterApprovedDoes += amountDoes;
    afterPendingFromReferral = beforePendingFromReferral + amountDoes;
  }

  if (type === "game_reward") {
    const approvedReward = safeInt(rewardApprovedDoes);
    const provisionalReward = safeInt(rewardProvisionalDoes);
    if ((approvedReward + provisionalReward) !== amountDoes) {
      throw new HttpsError("failed-precondition", "Répartition de gain invalide.");
    }
    afterApprovedDoes += approvedReward;
    afterProvisionalDoes += provisionalReward;
  }

  if (type === "xchange_sell") {
    const pendingTotal = afterPendingFromXchange + afterPendingFromReferral;
    const availableExchangeableDoes = Math.min(beforeApprovedDoes, beforeExchangeableDoes);
    if (amountDoes > availableExchangeableDoes) {
      throw buildPlayRequiredError({
        pendingPlayFromXchangeDoes: afterPendingFromXchange,
        pendingPlayFromReferralDoes: afterPendingFromReferral,
        pendingPlayTotalDoes: pendingTotal,
        exchangeableDoesAvailable: availableExchangeableDoes,
      });
    }
    if (amountDoes > beforeApprovedDoes) {
      throw new HttpsError("failed-precondition", "Les Does en examen ne peuvent pas être reconvertis en HTG.");
    }
    afterApprovedDoes = Math.max(0, afterApprovedDoes - amountDoes);
    afterExchangeableDoes = Math.max(0, availableExchangeableDoes - amountDoes);
    const [allOrders, withdrawalsSnap] = await Promise.all([
      loadAllOrders(),
      tx.get(db.collection(CLIENTS_COLLECTION).doc(uid).collection("withdrawals")),
    ]);
    fundingPatch = buildFundingWalletPatch(buildWalletFundingSnapshot({
      orders: allOrders.map((item) => item.data || {}),
      withdrawals: withdrawalsSnap.docs.map((item) => item.data() || {}),
      walletData: {
        ...data,
        exchangedGourdes: afterExchanged,
        totalExchangedHtgEver: afterTotalExchangedEver,
        doesApprovedBalance: afterApprovedDoes,
        doesProvisionalBalance: afterProvisionalDoes,
        doesBalance: afterApprovedDoes + afterProvisionalDoes,
      },
    }));
  }

  if (type !== "xchange_buy" && type !== "game_entry" && type !== "share_reward_bonus" && type !== "game_reward" && type !== "xchange_sell") {
    const nextDoesRaw = beforeDoes + deltaDoes;
    if (nextDoesRaw < 0) {
      throw new HttpsError("failed-precondition", "Solde Does insuffisant.");
    }
    afterApprovedDoes = Math.max(0, nextDoesRaw - afterProvisionalDoes);
  }

  const afterDoes = safeInt(afterApprovedDoes + afterProvisionalDoes);
  if (afterDoes < 0) {
    throw new HttpsError("failed-precondition", "Solde Does insuffisant.");
  }
  if ((afterPendingFromXchange + afterPendingFromReferral) <= 0) {
    afterExchangeableDoes = safeInt(afterApprovedDoes);
  } else {
    afterExchangeableDoes = Math.min(safeInt(afterApprovedDoes), safeInt(afterExchangeableDoes));
  }

  const nextWallet = {
    uid,
    email: email || String(data.email || ""),
    doesBalance: afterDoes,
    doesApprovedBalance: safeInt(afterApprovedDoes),
    doesProvisionalBalance: safeInt(afterProvisionalDoes),
    ...fundingPatch,
    exchangedGourdes: afterExchanged,
    exchangeableDoesAvailable: safeInt(afterExchangeableDoes),
    pendingPlayFromXchangeDoes: afterPendingFromXchange,
    pendingPlayFromReferralDoes: afterPendingFromReferral,
    totalExchangedHtgEver: afterTotalExchangedEver,
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };

  if (!snap.exists) {
    nextWallet.createdAt = admin.firestore.FieldValue.serverTimestamp();
  }

  tx.set(ref, nextWallet, { merge: true });

  const historyDoc = walletHistoryRef(uid).doc();
  tx.set(historyDoc, {
    uid,
    email: email || String(data.email || ""),
    type,
    note,
    amountGourdes,
    amountDoes,
    deltaDoes,
    deltaExchangedGourdes,
    beforeDoes,
    afterDoes,
    beforeExchangedGourdes: beforeExchanged,
    afterExchangedGourdes: afterExchanged,
    beforePendingPlayFromXchangeDoes: beforePendingFromXchange,
    afterPendingPlayFromXchangeDoes: afterPendingFromXchange,
    beforePendingPlayFromReferralDoes: beforePendingFromReferral,
    afterPendingPlayFromReferralDoes: afterPendingFromReferral,
    beforeExchangeableDoesAvailable: beforeExchangeableDoes,
    afterExchangeableDoesAvailable: safeInt(afterExchangeableDoes),
    beforeApprovedDoesBalance: beforeApprovedDoes,
    afterApprovedDoesBalance: safeInt(afterApprovedDoes),
    beforeProvisionalDoesBalance: beforeProvisionalDoes,
    afterProvisionalDoesBalance: safeInt(afterProvisionalDoes),
    gameEntryFunding,
    provisionalConversion,
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
  });

  return {
    beforeDoes,
    afterDoes,
    beforeExchanged,
    afterExchanged,
    afterPendingFromXchange,
    afterPendingFromReferral,
    afterTotalExchangedEver,
    afterExchangeableDoes: safeInt(afterExchangeableDoes),
    afterApprovedDoes: safeInt(afterApprovedDoes),
    afterProvisionalDoes: safeInt(afterProvisionalDoes),
    gameEntryFunding,
    provisionalConversion,
  };
}

function buildDefaultShareSitePromoState(nowMs = Date.now()) {
  const safeNow = safeSignedInt(nowMs) || Date.now();
  return {
    campaignId: SHARE_SITE_PROMO_DOC,
    targetCount: SHARE_SITE_PROMO_TARGET,
    rewardDoes: SHARE_SITE_PROMO_REWARD_DOES,
    cooldownMs: SHARE_SITE_PROMO_COOLDOWN_MS,
    cycleStartedAtMs: 0,
    shareCount: 0,
    rewardGranted: false,
    rewardGrantedAtMs: 0,
    cooldownUntilMs: 0,
    lastShareAtMs: 0,
    lastShareSource: "",
    actionIds: {},
    lastResetAtMs: safeNow,
  };
}

function normalizeShareSitePromoState(raw = {}, nowMs = Date.now()) {
  const safeNow = safeSignedInt(nowMs) || Date.now();
  const fallback = buildDefaultShareSitePromoState(safeNow);
  const cooldownUntilMs = safeSignedInt(raw.cooldownUntilMs);

  if (cooldownUntilMs > 0 && cooldownUntilMs <= safeNow) {
    return {
      ...fallback,
      rewardGrantedAtMs: safeSignedInt(raw.rewardGrantedAtMs),
      lastResetAtMs: safeNow,
    };
  }

  return {
    ...fallback,
    cycleStartedAtMs: safeSignedInt(raw.cycleStartedAtMs),
    shareCount: Math.min(SHARE_SITE_PROMO_TARGET, safeInt(raw.shareCount)),
    rewardGranted: raw.rewardGranted === true,
    rewardGrantedAtMs: safeSignedInt(raw.rewardGrantedAtMs),
    cooldownUntilMs: Math.max(0, cooldownUntilMs),
    lastShareAtMs: safeSignedInt(raw.lastShareAtMs),
    lastShareSource: sanitizeText(raw.lastShareSource || "", 40),
    actionIds: trimIdempotencyKeys(
      raw.actionIds && typeof raw.actionIds === "object" ? raw.actionIds : {},
      SHARE_SITE_PROMO_ACTION_CACHE
    ),
    lastResetAtMs: safeSignedInt(raw.lastResetAtMs) || fallback.lastResetAtMs,
  };
}

function buildShareSitePromoResponse(state = {}, nowMs = Date.now()) {
  const safeNow = safeSignedInt(nowMs) || Date.now();
  const normalized = normalizeShareSitePromoState(state, safeNow);
  const isCoolingDown = normalized.cooldownUntilMs > safeNow;
  const shareCount = normalized.rewardGranted
    ? SHARE_SITE_PROMO_TARGET
    : Math.min(SHARE_SITE_PROMO_TARGET, safeInt(normalized.shareCount));
  const remainingCount = Math.max(0, SHARE_SITE_PROMO_TARGET - shareCount);

  return {
    campaignId: SHARE_SITE_PROMO_DOC,
    targetCount: SHARE_SITE_PROMO_TARGET,
    shareCount,
    remainingCount,
    rewardDoes: SHARE_SITE_PROMO_REWARD_DOES,
    progressPercent: Math.round((shareCount / SHARE_SITE_PROMO_TARGET) * 100),
    rewardGranted: normalized.rewardGranted === true,
    canShare: !isCoolingDown && normalized.rewardGranted !== true && shareCount < SHARE_SITE_PROMO_TARGET,
    isCoolingDown,
    cooldownMs: SHARE_SITE_PROMO_COOLDOWN_MS,
    cooldownUntilMs: isCoolingDown ? normalized.cooldownUntilMs : 0,
    cooldownRemainingMs: isCoolingDown ? Math.max(0, normalized.cooldownUntilMs - safeNow) : 0,
    cycleStartedAtMs: normalized.cycleStartedAtMs,
    rewardGrantedAtMs: normalized.rewardGrantedAtMs,
    lastShareAtMs: normalized.lastShareAtMs,
    lastResetAtMs: normalized.lastResetAtMs,
  };
}

function timestampToMillis(value) {
  if (value && typeof value.toMillis === "function") {
    return value.toMillis();
  }
  const raw = Number(value);
  return Number.isFinite(raw) ? raw : 0;
}

function resolveRoomCreatedAtMs(room = {}) {
  return safeSignedInt(room.createdAtMs) || timestampToMillis(room.createdAt);
}

function resolveWaitingDeadlineMs(room = {}, nowMs = Date.now()) {
  const explicit = safeSignedInt(room.waitingDeadlineMs);
  if (explicit > 0) return explicit;
  const createdAtMs = resolveRoomCreatedAtMs(room);
  if (createdAtMs > 0) return createdAtMs + ROOM_WAIT_MS;
  if (String(room.status || "") === "waiting") return nowMs;
  return nowMs + ROOM_WAIT_MS;
}

function shouldStartWaitingRoom(room = {}, nowMs = Date.now()) {
  const humans = Array.isArray(room.playerUids)
    ? room.playerUids.filter(Boolean).length
    : safeInt(room.humanCount);
  if (humans >= 4) return true;
  return nowMs >= resolveWaitingDeadlineMs(room, nowMs);
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function matchmakingPoolRef(stakeConfigId = "", stakeDoes = 0) {
  const normalizedStakeConfigId = String(stakeConfigId || "").trim();
  const poolKey = normalizedStakeConfigId
    ? `stake_${normalizedStakeConfigId}`
    : `does_${safeInt(stakeDoes)}`;
  return db.collection(MATCHMAKING_POOLS_COLLECTION).doc(poolKey);
}

function setMatchmakingPoolOpen(tx, poolRef, roomId, stakeConfigId = "", stakeDoes = 0) {
  tx.set(poolRef, {
    openRoomId: String(roomId || "").trim(),
    stakeConfigId: String(stakeConfigId || "").trim(),
    stakeDoes: safeInt(stakeDoes),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  }, { merge: true });
}

function clearMatchmakingPool(tx, poolRef) {
  tx.set(poolRef, {
    openRoomId: "",
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  }, { merge: true });
}

async function findActiveRoomForUser(uid) {
  const rooms = db.collection(ROOMS_COLLECTION);
  const membershipSnap = await rooms
    .where("playerUids", "array-contains", uid)
    .limit(12)
    .get();

  if (membershipSnap.empty) return null;

  let playingCandidate = null;
  let waitingCandidate = null;

  membershipSnap.docs.forEach((docSnap) => {
    const data = docSnap.data() || {};
    const status = String(data.status || "");
    if (status === "playing" && !playingCandidate) {
      playingCandidate = docSnap;
      return;
    }
    if (status === "waiting" && !waitingCandidate) {
      waitingCandidate = docSnap;
    }
  });

  const candidate = playingCandidate || waitingCandidate;

  if (!candidate) return null;

  const data = candidate.data() || {};
  const seats = data.seats && typeof data.seats === "object" ? data.seats : {};
  const seatIndex = typeof seats[uid] === "number" ? seats[uid] : -1;

  return {
    roomId: candidate.id,
    status: String(data.status || ""),
    seatIndex,
  };
}

function getRoomSeats(room) {
  return room?.seats && typeof room.seats === "object" ? room.seats : {};
}

function getSeatForUser(room, uid) {
  const seats = getRoomSeats(room);
  return typeof seats[uid] === "number" ? seats[uid] : -1;
}

async function deleteCollectionInChunks(collectionRef, batchSize = 400) {
  while (true) {
    const snap = await collectionRef.limit(batchSize).get();
    if (snap.empty) break;
    const batch = db.batch();
    snap.docs.forEach((item) => batch.delete(item.ref));
    await batch.commit();
  }
}

async function cleanupRoom(roomRef) {
  await deleteCollectionInChunks(roomRef.collection("actions"));
  await deleteCollectionInChunks(roomRef.collection("settlements"));
  await gameStateRef(roomRef.id).delete().catch(() => null);
  await roomRef.delete();
}

async function ensureRoomGameStartedTx(tx, roomRefDoc, room = {}) {
  const stateRef = gameStateRef(roomRefDoc.id);
  const stateSnap = await tx.get(stateRef);

  let state = stateSnap.exists
    ? normalizeGameState(stateSnap.data(), room)
    : createInitialGameState(room, Array.isArray(room.deckOrder) && room.deckOrder.length === 28 ? room.deckOrder : makeDeckOrder());

  const batchResult = applyActionBatchInTransaction(tx, roomRefDoc, room, state, roomRefDoc.id);
  state = batchResult.state;

  tx.set(stateRef, buildGameStateWrite(state), { merge: true });
  return {
    state,
    records: batchResult.records,
  };
}

function buildStartedRoomTransaction(tx, roomRefDoc, room = {}, options = {}) {
  const configuredBotDifficulty = String(options.configuredBotDifficulty || room.botDifficulty || DEFAULT_BOT_DIFFICULTY);
  const nowMs = safeSignedInt(options.nowMs) || Date.now();
  const humans = Array.isArray(room.playerUids) ? room.playerUids.filter(Boolean).length : safeInt(room.humanCount);
  const deckOrder = Array.isArray(room.deckOrder) && room.deckOrder.length === 28 ? room.deckOrder.slice(0, 28) : makeDeckOrder();
  const roomAtStart = {
    ...room,
    botDifficulty: configuredBotDifficulty,
    deckOrder,
    humanCount: humans,
    botCount: Math.max(0, 4 - humans),
    playedCount: 0,
  };
  const initialState = createInitialGameState(roomAtStart, deckOrder);
  const openingMove = buildOpeningMoveForState(initialState);
  const batchResult = applyActionBatchInTransaction(
    tx,
    roomRefDoc,
    roomAtStart,
    initialState,
    roomRefDoc.id,
    openingMove,
    "server:opening",
    { allowBotAdvance: false }
  );
  const finalState = batchResult.state;

  tx.set(gameStateRef(roomRefDoc.id), buildGameStateWrite(finalState), { merge: true });

  const updates = {
    playerUids: Array.isArray(room.playerUids) ? room.playerUids : ["", "", "", ""],
    playerNames: Array.isArray(room.playerNames) ? room.playerNames : ["", "", "", ""],
    seats: getRoomSeats(room),
    humanCount: humans,
    status: finalState.winnerSeat >= 0 ? "ended" : "playing",
    startRevealPending: finalState.winnerSeat < 0,
    startRevealAckUids: [],
    startedHumanCount: humans,
    startedBotCount: Math.max(0, 4 - humans),
    botCount: Math.max(0, 4 - humans),
    botDifficulty: configuredBotDifficulty,
    startedAt: admin.firestore.FieldValue.serverTimestamp(),
    startedAtMs: nowMs,
    deckOrder: admin.firestore.FieldValue.delete(),
    turnLockedUntilMs: 0,
    endClicks: {},
    playerEmails: admin.firestore.FieldValue.delete(),
    waitingDeadlineMs: admin.firestore.FieldValue.delete(),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };

  Object.assign(updates, buildRoomUpdateFromGameState(roomAtStart, finalState, batchResult.records));
  if (finalState.winnerSeat < 0) {
    updates.winnerSeat = admin.firestore.FieldValue.delete();
    updates.winnerUid = admin.firestore.FieldValue.delete();
    updates.endedReason = admin.firestore.FieldValue.delete();
    updates.endedAt = admin.firestore.FieldValue.delete();
    updates.endedAtMs = admin.firestore.FieldValue.delete();
    updates.turnLockedUntilMs = 0;
  }

  tx.update(roomRefDoc, updates);

  return {
    ok: true,
    started: true,
    status: String(updates.status || "playing"),
    startRevealPending: updates.startRevealPending === true,
    privateDeckOrder: String(updates.status || "playing") === "playing" ? finalState.deckOrder.slice(0, 28) : [],
    humanCount: humans,
    botCount: Math.max(0, 4 - humans),
    waitingDeadlineMs: 0,
  };
}

exports.walletMutate = publicOnCall("walletMutate", async (request) => {
  const { uid, email } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const op = String(payload.op || "").trim();
  const settingsSnapshot = await getSettingsSnapshotData();
  const provisionalDepositsEnabled = isProvisionalFundingEnabled(settingsSnapshot);

  let mutation = null;
  if (op === "xchange_buy") {
    const amountGourdes = safeInt(payload.amountGourdes);
    if (amountGourdes <= 0) {
      throw new HttpsError("invalid-argument", "Montant invalide.");
    }
    mutation = {
      uid,
      email,
      type: "xchange_buy",
      provisionalDepositsEnabled,
      note: "Conversion HTG vers Does",
      amountGourdes,
      amountDoes: amountGourdes * RATE_HTG_TO_DOES,
      deltaDoes: amountGourdes * RATE_HTG_TO_DOES,
      deltaExchangedGourdes: amountGourdes,
    };
  } else if (op === "xchange_sell") {
    const amountDoes = safeInt(payload.amountDoes);
    if (amountDoes <= 0 || amountDoes % RATE_HTG_TO_DOES !== 0) {
      throw new HttpsError("invalid-argument", `Le montant Does doit être multiple de ${RATE_HTG_TO_DOES}.`);
    }
    const amountGourdes = Math.floor(amountDoes / RATE_HTG_TO_DOES);
    mutation = {
      uid,
      email,
      type: "xchange_sell",
      provisionalDepositsEnabled,
      note: "Conversion Does vers HTG",
      amountGourdes,
      amountDoes,
      deltaDoes: -amountDoes,
      deltaExchangedGourdes: -amountGourdes,
    };
  } else if (op === "game_entry") {
    const amountDoes = safeInt(payload.amountDoes);
    if (!findStakeConfigByAmount(amountDoes, settingsSnapshot.gameStakeOptions, true)) {
      throw new HttpsError("invalid-argument", "Mise non autorisée.");
    }
    mutation = {
      uid,
      email,
      type: "game_entry",
      provisionalDepositsEnabled,
      note: "Participation partie",
      amountGourdes: 0,
      amountDoes,
      deltaDoes: -amountDoes,
      deltaExchangedGourdes: 0,
    };
  } else {
    throw new HttpsError("invalid-argument", "Opération non supportée.");
  }

  console.log("[BALANCE_DEBUG][FUNCTIONS][walletMutate] request", JSON.stringify({
    uid,
    op,
    payload,
    provisionalDepositsEnabled,
    mutation,
  }));

  const result = await db.runTransaction((tx) => applyWalletMutationTx(tx, mutation));
  console.log("[BALANCE_DEBUG][FUNCTIONS][walletMutate] result", JSON.stringify({
    uid,
    op,
    afterDoes: result.afterDoes,
    afterApprovedDoes: result.afterApprovedDoes,
    afterProvisionalDoes: result.afterProvisionalDoes,
    afterExchanged: result.afterExchanged,
    afterPendingFromXchange: result.afterPendingFromXchange,
    afterPendingFromReferral: result.afterPendingFromReferral,
    afterTotalExchangedEver: result.afterTotalExchangedEver,
    afterExchangeableDoes: result.afterExchangeableDoes,
    provisionalConversion: result.provisionalConversion,
    gameEntryFunding: result.gameEntryFunding,
  }));
  return {
    ok: true,
    does: result.afterDoes,
    doesApprovedBalance: result.afterApprovedDoes,
    doesProvisionalBalance: result.afterProvisionalDoes,
    exchangeableDoesAvailable: result.afterExchangeableDoes,
    exchangedGourdes: result.afterExchanged,
    pendingPlayFromXchangeDoes: result.afterPendingFromXchange,
    pendingPlayFromReferralDoes: result.afterPendingFromReferral,
    totalExchangedHtgEver: result.afterTotalExchangedEver,
    gameEntryFunding: result.gameEntryFunding,
    provisionalConversion: result.provisionalConversion,
  };
});

exports.joinMatchmaking = publicOnCall("joinMatchmaking", async (request) => {
  const { uid, email } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const stakeDoes = safeInt(payload.stakeDoes);
  const [configuredBotDifficulty, settingsSnapshot] = await Promise.all([
    getConfiguredBotDifficulty(),
    getSettingsSnapshotData(),
  ]);
  const selectedStakeConfig = findStakeConfigByAmount(stakeDoes, settingsSnapshot.gameStakeOptions, true);

  if (!selectedStakeConfig) {
    throw new HttpsError("invalid-argument", "Mise non autorisée.");
  }

  const rewardAmountDoes = selectedStakeConfig.rewardDoes;
  const poolRef = matchmakingPoolRef(selectedStakeConfig.id, stakeDoes);

  const active = await findActiveRoomForUser(uid);
  if (active && active.seatIndex >= 0) {
    const activeRoomRef = db.collection(ROOMS_COLLECTION).doc(active.roomId);
    const resumedActive = await db.runTransaction(async (tx) => {
      const roomSnap = await tx.get(activeRoomRef);
      if (!roomSnap.exists) return null;
      const room = roomSnap.data() || {};
      const seatIndex = getSeatForUser(room, uid);
      if (seatIndex < 0) return null;

      const nowMs = Date.now();
      const nextPresence = room.roomPresenceMs && typeof room.roomPresenceMs === "object"
        ? { ...room.roomPresenceMs }
        : {};
      nextPresence[uid] = nowMs;
      tx.update(activeRoomRef, {
        roomPresenceMs: nextPresence,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      });

      const status = String(room.status || "");
      if (status === "waiting") {
        const humans = Array.isArray(room.playerUids) ? room.playerUids.filter(Boolean).length : safeInt(room.humanCount);
        const waitingDeadlineMs = resolveWaitingDeadlineMs(room, nowMs);
        if (safeSignedInt(room.waitingDeadlineMs) !== waitingDeadlineMs) {
          tx.update(activeRoomRef, {
            waitingDeadlineMs,
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          });
        }

        if (shouldStartWaitingRoom({ ...room, humanCount: humans, waitingDeadlineMs }, nowMs)) {
          clearMatchmakingPool(tx, poolRef);
          return {
            resumed: true,
            charged: false,
            roomId: activeRoomRef.id,
            seatIndex,
            ...buildStartedRoomTransaction(tx, activeRoomRef, {
              ...room,
              humanCount: humans,
              botCount: Math.max(0, 4 - humans),
              waitingDeadlineMs,
            }, {
              configuredBotDifficulty,
              nowMs,
            }),
          };
        }

        return {
          ok: true,
          resumed: true,
          charged: false,
          roomId: activeRoomRef.id,
          seatIndex,
          status: "waiting",
          waitingDeadlineMs,
          humanCount: humans,
          botCount: Math.max(0, 4 - humans),
          privateDeckOrder: [],
        };
      }

      const privateDeckOrder = status === "playing"
        ? await readPrivateDeckOrderForRoom(activeRoomRef.id)
        : [];
      return {
        ok: true,
        resumed: true,
        charged: false,
        roomId: activeRoomRef.id,
        seatIndex,
        status,
        privateDeckOrder,
      };
    });

    if (resumedActive?.status === "playing" && resumedActive?.startRevealPending !== true) {
      await processPendingBotTurns(String(resumedActive.roomId || ""));
    }
    if (resumedActive) return resumedActive;
  }

  for (let pass = 0; pass < 3; pass += 1) {
    if (pass > 0) {
      await delay(120 * pass);
    }

    const waitingCandidates = await db
      .collection(ROOMS_COLLECTION)
      .where("status", "==", "waiting")
      .limit(64)
      .get();

    const waitingDocs = waitingCandidates.docs
      .slice()
      .sort((a, b) => {
        const humansLeft = safeInt(a.get("humanCount"));
        const humansRight = safeInt(b.get("humanCount"));
        if (humansRight !== humansLeft) return humansRight - humansLeft;
        const left = timestampToMillis(a.get("createdAt"));
        const right = timestampToMillis(b.get("createdAt"));
        return left - right;
      });

    const waitingRoomRefs = waitingDocs.map((docSnap) => docSnap.ref);
    let openRoomId = "";
    try {
      const poolSnap = await poolRef.get();
      openRoomId = String(poolSnap.exists ? (poolSnap.data() || {}).openRoomId || "" : "").trim();
    } catch (_) {
      openRoomId = "";
    }
    if (openRoomId && !waitingRoomRefs.some((ref) => ref.id === openRoomId)) {
      waitingRoomRefs.unshift(db.collection(ROOMS_COLLECTION).doc(openRoomId));
    }

    for (const roomRef of waitingRoomRefs) {
      try {
        const joined = await db.runTransaction(async (tx) => {
        const [roomSnap, walletSnap] = await Promise.all([
          tx.get(roomRef),
          tx.get(walletRef(uid)),
        ]);
        if (!roomSnap.exists) {
          throw new HttpsError("aborted", "Salle introuvable.");
        }
        const room = roomSnap.data() || {};
        if (room.status !== "waiting") {
          throw new HttpsError("aborted", "Salle non disponible.");
        }
        if (getBlockedRejoinSet(room).has(uid)) {
          throw new HttpsError("aborted", "Salle non disponible.");
        }

        const walletData = walletSnap.exists ? (walletSnap.data() || {}) : {};
        assertWalletNotFrozen(walletData);

        const roomEntryCostDoes = safeInt(room.entryCostDoes || room.stakeDoes);
        const roomRewardAmountDoes = resolveRoomRewardDoes(room);
        if (roomEntryCostDoes !== stakeDoes || roomRewardAmountDoes !== rewardAmountDoes) {
          throw new HttpsError("aborted", "Salle non compatible.");
        }

        const currentSeats = room.seats && typeof room.seats === "object" ? room.seats : {};
        const playerUids = Array.from({ length: 4 }, (_, idx) => String((room.playerUids || [])[idx] || ""));
        const nowMs = Date.now();
        const waitingDeadlineMs = resolveWaitingDeadlineMs(room, nowMs);
        const waitingDeadlineChanged = safeSignedInt(room.waitingDeadlineMs) !== waitingDeadlineMs;
        if (playerUids.includes(uid)) {
          const seats = currentSeats;
          const seatIndex = typeof seats[uid] === "number" ? seats[uid] : 0;
          const nextPresence = room.roomPresenceMs && typeof room.roomPresenceMs === "object"
            ? { ...room.roomPresenceMs }
            : {};
          nextPresence[uid] = nowMs;
          const resumeUpdates = {
            roomPresenceMs: nextPresence,
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          };
          if (waitingDeadlineChanged) {
            resumeUpdates.waitingDeadlineMs = waitingDeadlineMs;
          }
          tx.update(roomRef, resumeUpdates);
          const privateDeckOrder = room.status === "playing"
            ? await readPrivateDeckOrderForRoom(roomRef.id)
            : [];
          return {
            ok: true,
            resumed: true,
            charged: false,
            roomId: roomRef.id,
            seatIndex,
            status: room.status,
            privateDeckOrder,
          };
        }

        const humans = playerUids.filter(Boolean).length;
        if (shouldStartWaitingRoom({ ...room, waitingDeadlineMs }, nowMs)) {
          clearMatchmakingPool(tx, poolRef);
          const startedRoom = buildStartedRoomTransaction(tx, roomRef, { ...room, waitingDeadlineMs }, {
            configuredBotDifficulty,
            nowMs,
          });
          return {
            ...startedRoom,
            skipped: true,
            roomId: roomRef.id,
          };
        }
        if (humans >= 4) {
          throw new HttpsError("aborted", "Salle complète.");
        }

        const beforeDoes = safeInt(walletData.doesBalance);
        if (beforeDoes < stakeDoes) {
          throw new HttpsError("failed-precondition", "Solde Does insuffisant.");
        }

        const walletMutation = await applyWalletMutationTx(tx, {
          uid,
          email,
          type: "game_entry",
          note: "Participation partie",
          amountDoes: stakeDoes,
          amountGourdes: 0,
          deltaDoes: -stakeDoes,
          deltaExchangedGourdes: 0,
        });
        console.log("[BALANCE_DEBUG][FUNCTIONS][joinMatchmaking] charged waiting-room join", JSON.stringify({
          uid,
          roomId: roomRef.id,
          stakeDoes,
          afterDoes: safeInt(walletMutation.afterDoes),
          gameEntryFunding: walletMutation.gameEntryFunding || null,
        }));

        const usedSeats = new Set(
          Object.values(currentSeats)
            .map((seat) => Number(seat))
            .filter((seat) => Number.isFinite(seat) && seat >= 0 && seat < 4)
        );
        const seatIndex = [0, 1, 2, 3].find((seat) => !usedSeats.has(seat));
        if (typeof seatIndex !== "number") {
          throw new HttpsError("aborted", "Salle complète.");
        }

        const nextPlayerUids = playerUids.slice();
        nextPlayerUids[seatIndex] = uid;
        const currentNames = Array.from({ length: 4 }, (_, idx) => String((room.playerNames || [])[idx] || ""));
        const nextPlayerNames = currentNames.slice();
        nextPlayerNames[seatIndex] = sanitizePlayerLabel(email || uid, seatIndex);
        const nextSeats = {
          ...currentSeats,
          [uid]: seatIndex,
        };
        const nextHumans = nextPlayerUids.filter(Boolean).length;
        const nextPresence = room.roomPresenceMs && typeof room.roomPresenceMs === "object"
          ? { ...room.roomPresenceMs }
          : {};
        nextPresence[uid] = nowMs;
        const currentEntryFunding = room.entryFundingByUid && typeof room.entryFundingByUid === "object"
          ? { ...room.entryFundingByUid }
          : {};
        currentEntryFunding[uid] = {
          approvedDoes: safeInt(walletMutation.gameEntryFunding?.approvedDoes),
          provisionalDoes: safeInt(walletMutation.gameEntryFunding?.provisionalDoes),
          provisionalSources: normalizeFundingSources(walletMutation.gameEntryFunding?.provisionalSources),
        };

        const updates = {
          playerUids: nextPlayerUids,
          playerNames: nextPlayerNames,
          playerEmails: admin.firestore.FieldValue.delete(),
          seats: nextSeats,
          entryFundingByUid: currentEntryFunding,
          roomPresenceMs: nextPresence,
          humanCount: nextHumans,
          botCount: Math.max(0, 4 - nextHumans),
          botDifficulty: configuredBotDifficulty,
          stakeDoes,
          entryCostDoes: stakeDoes,
          rewardAmountDoes,
          stakeConfigId: selectedStakeConfig.id,
          turnLockedUntilMs: 0,
          waitingDeadlineMs,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        };

        if (nextHumans >= 4) {
          clearMatchmakingPool(tx, poolRef);
          return {
            ok: true,
            resumed: false,
            charged: true,
            roomId: roomRef.id,
            seatIndex,
            does: walletMutation.afterDoes,
            ...buildStartedRoomTransaction(tx, roomRef, {
              ...room,
              playerUids: nextPlayerUids,
              playerNames: nextPlayerNames,
              seats: nextSeats,
              humanCount: nextHumans,
              botCount: 0,
              waitingDeadlineMs,
            }, {
              configuredBotDifficulty,
              nowMs,
            }),
          };
        }

        tx.update(roomRef, updates);
        setMatchmakingPoolOpen(tx, poolRef, roomRef.id, selectedStakeConfig.id, stakeDoes);

        return {
          ok: true,
          resumed: false,
          charged: true,
          roomId: roomRef.id,
          seatIndex,
          status: String(updates.status || "waiting"),
          startRevealPending: updates.startRevealPending === true,
          does: walletMutation.afterDoes,
          waitingDeadlineMs,
          privateDeckOrder: [],
        };
        });

        if (joined?.status === "playing") {
          if (joined?.startRevealPending !== true) {
            await processPendingBotTurns(String(joined.roomId || ""));
          }
        }
        if (joined?.skipped === true) {
          continue;
        }
        return joined;
      } catch (err) {
        if (err instanceof HttpsError && err.code === "failed-precondition") {
          throw err;
        }
        continue;
      }
    }
  }

  const newRoomRef = db.collection(ROOMS_COLLECTION).doc();
  const created = await db.runTransaction(async (tx) => {
    const nowMs = Date.now();
    const [poolSnap, walletSnap] = await Promise.all([
      tx.get(poolRef),
      tx.get(walletRef(uid)),
    ]);

        const walletData = walletSnap.exists ? (walletSnap.data() || {}) : {};
        assertWalletNotFrozen(walletData);

        const existingOpenRoomId = String(poolSnap.exists ? (poolSnap.data() || {}).openRoomId || "" : "").trim();
    if (existingOpenRoomId) {
      const openRoomRef = db.collection(ROOMS_COLLECTION).doc(existingOpenRoomId);
      const roomSnap = await tx.get(openRoomRef);
      if (roomSnap.exists) {
        const room = roomSnap.data() || {};
        const status = String(room.status || "");
        const roomEntryCostDoes = safeInt(room.entryCostDoes || room.stakeDoes);
        const roomRewardAmountDoes = resolveRoomRewardDoes(room);
        const playerUids = Array.from({ length: 4 }, (_, idx) => String((room.playerUids || [])[idx] || ""));
        const waitingDeadlineMs = resolveWaitingDeadlineMs(room, nowMs);
        const waitingDeadlineChanged = safeSignedInt(room.waitingDeadlineMs) !== waitingDeadlineMs;
        if (status === "waiting" && !getBlockedRejoinSet(room).has(uid) && roomEntryCostDoes === stakeDoes && roomRewardAmountDoes === rewardAmountDoes) {
          if (playerUids.includes(uid)) {
            const currentSeats = room.seats && typeof room.seats === "object" ? room.seats : {};
            const seatIndex = typeof currentSeats[uid] === "number" ? currentSeats[uid] : 0;
            const nextPresence = room.roomPresenceMs && typeof room.roomPresenceMs === "object"
              ? { ...room.roomPresenceMs }
              : {};
            nextPresence[uid] = nowMs;
            const resumeUpdates = {
              roomPresenceMs: nextPresence,
              updatedAt: admin.firestore.FieldValue.serverTimestamp(),
            };
            if (waitingDeadlineChanged) {
              resumeUpdates.waitingDeadlineMs = waitingDeadlineMs;
            }
            tx.update(openRoomRef, resumeUpdates);
            return {
              ok: true,
              resumed: true,
              charged: false,
              roomId: openRoomRef.id,
              seatIndex,
              status: "waiting",
              waitingDeadlineMs,
              humanCount: playerUids.filter(Boolean).length,
              botCount: Math.max(0, 4 - playerUids.filter(Boolean).length),
              privateDeckOrder: [],
            };
          }

          if (!shouldStartWaitingRoom({ ...room, waitingDeadlineMs }, nowMs)) {
            const currentSeats = room.seats && typeof room.seats === "object" ? room.seats : {};
            const humans = playerUids.filter(Boolean).length;
            if (humans < 4) {
              const beforeDoes = safeInt(walletData.doesBalance);
              if (beforeDoes < stakeDoes) {
                throw new HttpsError("failed-precondition", "Solde Does insuffisant.");
              }

              const walletMutation = await applyWalletMutationTx(tx, {
                uid,
                email,
                type: "game_entry",
                note: "Participation partie",
                amountDoes: stakeDoes,
                amountGourdes: 0,
                deltaDoes: -stakeDoes,
                deltaExchangedGourdes: 0,
              });
              console.log("[BALANCE_DEBUG][FUNCTIONS][joinMatchmaking] charged open-room join", JSON.stringify({
                uid,
                roomId: openRoomRef.id,
                stakeDoes,
                afterDoes: safeInt(walletMutation.afterDoes),
                gameEntryFunding: walletMutation.gameEntryFunding || null,
              }));

              const usedSeats = new Set(
                Object.values(currentSeats)
                  .map((seat) => Number(seat))
                  .filter((seat) => Number.isFinite(seat) && seat >= 0 && seat < 4)
              );
              const seatIndex = [0, 1, 2, 3].find((seat) => !usedSeats.has(seat));
              if (typeof seatIndex === "number") {
                const nextPlayerUids = playerUids.slice();
                nextPlayerUids[seatIndex] = uid;
                const currentNames = Array.from({ length: 4 }, (_, idx) => String((room.playerNames || [])[idx] || ""));
                const nextPlayerNames = currentNames.slice();
                nextPlayerNames[seatIndex] = sanitizePlayerLabel(email || uid, seatIndex);
                const nextSeats = {
                  ...currentSeats,
                  [uid]: seatIndex,
                };
                const nextHumans = nextPlayerUids.filter(Boolean).length;
                const nextPresence = room.roomPresenceMs && typeof room.roomPresenceMs === "object"
                  ? { ...room.roomPresenceMs }
                  : {};
                nextPresence[uid] = nowMs;
                const currentEntryFunding = room.entryFundingByUid && typeof room.entryFundingByUid === "object"
                  ? { ...room.entryFundingByUid }
                  : {};
                currentEntryFunding[uid] = {
                  approvedDoes: safeInt(walletMutation.gameEntryFunding?.approvedDoes),
                  provisionalDoes: safeInt(walletMutation.gameEntryFunding?.provisionalDoes),
                  provisionalSources: normalizeFundingSources(walletMutation.gameEntryFunding?.provisionalSources),
                };

                if (nextHumans >= 4) {
                  clearMatchmakingPool(tx, poolRef);
                  return {
                    ok: true,
                    resumed: false,
                    charged: true,
                    roomId: openRoomRef.id,
                    seatIndex,
                    does: walletMutation.afterDoes,
                    ...buildStartedRoomTransaction(tx, openRoomRef, {
                      ...room,
                      playerUids: nextPlayerUids,
                      playerNames: nextPlayerNames,
                      seats: nextSeats,
                      humanCount: nextHumans,
                      botCount: 0,
                      waitingDeadlineMs,
                    }, {
                      configuredBotDifficulty,
                      nowMs,
                    }),
                  };
                }

                tx.update(openRoomRef, {
                  playerUids: nextPlayerUids,
                  playerNames: nextPlayerNames,
                  playerEmails: admin.firestore.FieldValue.delete(),
                  seats: nextSeats,
                  entryFundingByUid: currentEntryFunding,
                  roomPresenceMs: nextPresence,
                  humanCount: nextHumans,
                  botCount: Math.max(0, 4 - nextHumans),
                  botDifficulty: configuredBotDifficulty,
                  stakeDoes,
                  entryCostDoes: stakeDoes,
                  rewardAmountDoes,
                  stakeConfigId: selectedStakeConfig.id,
                  turnLockedUntilMs: 0,
                  waitingDeadlineMs,
                  updatedAt: admin.firestore.FieldValue.serverTimestamp(),
                });
                setMatchmakingPoolOpen(tx, poolRef, openRoomRef.id, selectedStakeConfig.id, stakeDoes);

                return {
                  ok: true,
                  resumed: false,
                  charged: true,
                  roomId: openRoomRef.id,
                  seatIndex,
                  status: "waiting",
                  does: walletMutation.afterDoes,
                  waitingDeadlineMs,
                  humanCount: nextHumans,
                  botCount: Math.max(0, 4 - nextHumans),
                  privateDeckOrder: [],
                };
              }
            }
          } else {
            buildStartedRoomTransaction(tx, openRoomRef, {
              ...room,
              humanCount: playerUids.filter(Boolean).length,
              botCount: Math.max(0, 4 - playerUids.filter(Boolean).length),
              waitingDeadlineMs,
            }, {
              configuredBotDifficulty,
              nowMs,
            });
          }
        }
      }
    }

    const walletMutation = await applyWalletMutationTx(tx, {
      uid,
      email,
      type: "game_entry",
      note: "Participation partie",
      amountDoes: stakeDoes,
      amountGourdes: 0,
      deltaDoes: -stakeDoes,
      deltaExchangedGourdes: 0,
    });
    console.log("[BALANCE_DEBUG][FUNCTIONS][joinMatchmaking] charged new-room create", JSON.stringify({
      uid,
      roomId: newRoomRef.id,
      stakeDoes,
      afterDoes: safeInt(walletMutation.afterDoes),
      gameEntryFunding: walletMutation.gameEntryFunding || null,
    }));

    tx.set(newRoomRef, {
      status: "waiting",
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      createdAtMs: nowMs,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      ownerUid: uid,
      playerUids: [uid, "", "", ""],
      playerNames: [sanitizePlayerLabel(email || uid, 0), "", "", ""],
      entryFundingByUid: {
        [uid]: {
          approvedDoes: safeInt(walletMutation.gameEntryFunding?.approvedDoes),
          provisionalDoes: safeInt(walletMutation.gameEntryFunding?.provisionalDoes),
          provisionalSources: normalizeFundingSources(walletMutation.gameEntryFunding?.provisionalSources),
        },
      },
      blockedRejoinUids: [],
      humanCount: 1,
      seats: { [uid]: 0 },
      roomPresenceMs: { [uid]: nowMs },
      botCount: 3,
      botDifficulty: configuredBotDifficulty,
      startRevealPending: false,
      startRevealAckUids: [],
      waitingDeadlineMs: nowMs + ROOM_WAIT_MS,
      startedAt: null,
      startedAtMs: 0,
      endedAtMs: 0,
      turnLockedUntilMs: 0,
      nextActionSeq: 0,
      gameMode: "domino-ffa",
      engineVersion: 2,
      stakeDoes,
      entryCostDoes: stakeDoes,
      rewardAmountDoes,
      stakeConfigId: selectedStakeConfig.id,
    });
    setMatchmakingPoolOpen(tx, poolRef, newRoomRef.id, selectedStakeConfig.id, stakeDoes);

    return {
      ok: true,
      resumed: false,
      charged: true,
      roomId: newRoomRef.id,
      seatIndex: 0,
      status: "waiting",
      does: walletMutation.afterDoes,
      waitingDeadlineMs: nowMs + ROOM_WAIT_MS,
    };
  });

  return created;
});

exports.ensureRoomReady = publicOnCall("ensureRoomReady", async (request) => {
  const { uid } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const roomId = String(payload.roomId || "").trim();
  const configuredBotDifficulty = await getConfiguredBotDifficulty();

  if (!roomId) {
    throw new HttpsError("invalid-argument", "roomId requis.");
  }

  const roomRef = db.collection(ROOMS_COLLECTION).doc(roomId);
  const startResult = await db.runTransaction(async (tx) => {
    const roomSnap = await tx.get(roomRef);
    if (!roomSnap.exists) {
      throw new HttpsError("not-found", "Salle introuvable.");
    }

    const room = roomSnap.data() || {};
    const poolRef = matchmakingPoolRef(String(room.stakeConfigId || ""), safeInt(room.entryCostDoes || room.stakeDoes));
    const seatIndex = getSeatForUser(room, uid);
    if (seatIndex < 0) {
      throw new HttpsError("permission-denied", "Tu ne fais pas partie de cette salle.");
    }

    const status = String(room.status || "");
    if (status !== "waiting") {
      return {
        ok: true,
        started: false,
        status,
        startRevealPending: room.startRevealPending === true,
        waitingDeadlineMs: safeSignedInt(room.waitingDeadlineMs),
        humanCount: safeInt(room.humanCount),
        botCount: safeInt(room.botCount),
        privateDeckOrder: [],
      };
    }

    const nowMs = Date.now();
    const humans = Array.isArray(room.playerUids) ? room.playerUids.filter(Boolean).length : safeInt(room.humanCount);
    const waitingDeadlineMs = resolveWaitingDeadlineMs(room, nowMs);
    if (safeSignedInt(room.waitingDeadlineMs) !== waitingDeadlineMs) {
      tx.update(roomRef, {
        waitingDeadlineMs,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      });
    }

    if (!shouldStartWaitingRoom({ ...room, humanCount: humans, waitingDeadlineMs }, nowMs)) {
      return {
        ok: true,
        started: false,
        status: "waiting",
        startRevealPending: false,
        waitingDeadlineMs,
        humanCount: humans,
        botCount: Math.max(0, 4 - humans),
        privateDeckOrder: [],
      };
    }

    clearMatchmakingPool(tx, poolRef);
    return buildStartedRoomTransaction(tx, roomRef, {
      ...room,
      humanCount: humans,
      botCount: Math.max(0, 4 - humans),
      waitingDeadlineMs,
    }, {
      configuredBotDifficulty,
      nowMs,
    });
  });

  if (startResult?.status === "playing") {
    if (!Array.isArray(startResult.privateDeckOrder) || startResult.privateDeckOrder.length !== 28) {
      startResult.privateDeckOrder = await readPrivateDeckOrderForRoom(roomId);
    }
    if (startResult.startRevealPending !== true) {
      await processPendingBotTurns(roomId);
    }
  }

  return startResult;
});

exports.touchRoomPresence = publicOnCall("touchRoomPresence", async (request) => {
  const { uid } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const roomId = String(payload.roomId || "").trim();

  if (!roomId) {
    throw new HttpsError("invalid-argument", "roomId requis.");
  }

  const roomRef = db.collection(ROOMS_COLLECTION).doc(roomId);
  let shouldNudgeBots = false;

  const result = await db.runTransaction(async (tx) => {
    const roomSnap = await tx.get(roomRef);
    if (!roomSnap.exists) {
      throw new HttpsError("not-found", "Salle introuvable.");
    }

    const room = roomSnap.data() || {};
    const seatIndex = getSeatForUser(room, uid);
    if (seatIndex < 0) {
      throw new HttpsError("permission-denied", "Tu ne fais pas partie de cette salle.");
    }

    const nowMs = Date.now();
    const currentPresence = room.roomPresenceMs && typeof room.roomPresenceMs === "object"
      ? { ...room.roomPresenceMs }
      : {};
    currentPresence[uid] = nowMs;

    const playerUids = Array.from({ length: 4 }, (_, idx) => String((room.playerUids || [])[idx] || ""));
    const playerNames = Array.from({ length: 4 }, (_, idx) => String((room.playerNames || [])[idx] || ""));
    const seats = { ...getRoomSeats(room) };
    const takeoverSeats = getBotTakeoverSeatSet(room);
    const graceUntil = room.botGraceUntilMs && typeof room.botGraceUntilMs === "object"
      ? { ...room.botGraceUntilMs }
      : {};
    const blockedRejoinUids = Array.from(getBlockedRejoinSet(room));

    if (takeoverSeats.has(seatIndex)) {
      takeoverSeats.delete(seatIndex);
      delete graceUntil[String(seatIndex)];
    }

    let removedAny = false;

    for (let seat = 0; seat < 4; seat += 1) {
      const seatUid = String(playerUids[seat] || "").trim();
      if (!seatUid || seatUid === uid) continue;

      const lastSeen = safeSignedInt(currentPresence[seatUid]);
      if (lastSeen <= 0) continue;

      const offlineForMs = nowMs - lastSeen;
      if (offlineForMs < ROOM_DISCONNECT_TAKEOVER_MS) continue;

      if (!takeoverSeats.has(seat)) {
        takeoverSeats.add(seat);
        graceUntil[String(seat)] = lastSeen + ROOM_DISCONNECT_GRACE_MS;
      }

      const seatGraceUntil = safeSignedInt(graceUntil[String(seat)]);
      if (seatGraceUntil > 0 && nowMs > seatGraceUntil) {
        removedAny = true;
        playerUids[seat] = "";
        playerNames[seat] = String(room.status || "") === "playing" ? botSeatLabel(seat) : "";
        delete seats[seatUid];
        delete currentPresence[seatUid];
        takeoverSeats.delete(seat);
        delete graceUntil[String(seat)];
        if (!blockedRejoinUids.includes(seatUid)) {
          blockedRejoinUids.push(seatUid);
        }
      }
    }

    const updates = {
      roomPresenceMs: currentPresence,
      botTakeoverSeats: Array.from(takeoverSeats),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    };

    if (Object.keys(graceUntil).length > 0) {
      updates.botGraceUntilMs = graceUntil;
    } else {
      updates.botGraceUntilMs = admin.firestore.FieldValue.delete();
    }

    if (removedAny) {
      const humans = playerUids.filter(Boolean).length;
      updates.playerUids = playerUids;
      updates.playerNames = playerNames;
      updates.seats = seats;
      updates.humanCount = humans;
      updates.botCount = Math.max(0, 4 - humans);
      updates.blockedRejoinUids = blockedRejoinUids;
      updates.playerEmails = admin.firestore.FieldValue.delete();
    }

    const effectiveRoom = {
      ...room,
      playerUids: updates.playerUids || room.playerUids,
      playerNames: updates.playerNames || room.playerNames,
      seats: updates.seats || room.seats,
      botTakeoverSeats: updates.botTakeoverSeats,
    };

    if (String(room.status || "") === "playing" && room.startRevealPending !== true) {
      const currentPlayerSeat = safeInt(room.currentPlayer);
      if (currentPlayerSeat >= 0 && currentPlayerSeat < 4 && !isSeatHuman(effectiveRoom, currentPlayerSeat)) {
        shouldNudgeBots = true;
      }
    }

    tx.update(roomRef, updates);

    return {
      ok: true,
      roomId: roomRef.id,
      seatIndex,
      nowMs,
      removed: removedAny,
      takeoverCount: updates.botTakeoverSeats.length,
    };
  });

  if (shouldNudgeBots) {
    await processPendingBotTurns(roomId);
  }

  return result;
});

exports.ackRoomStartSeen = publicOnCall("ackRoomStartSeen", async (request) => {
  const { uid } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const roomId = String(payload.roomId || "").trim();

  if (!roomId) {
    throw new HttpsError("invalid-argument", "roomId requis.");
  }

  const roomRef = db.collection(ROOMS_COLLECTION).doc(roomId);
  const ackResult = await db.runTransaction(async (tx) => {
    const roomSnap = await tx.get(roomRef);
    if (!roomSnap.exists) {
      throw new HttpsError("not-found", "Salle introuvable.");
    }

    const room = roomSnap.data() || {};
    const botTakeoverSeats = getBotTakeoverSeatSet(room);
    const humanUids = Array.isArray(room.playerUids)
      ? room.playerUids
        .map((item) => String(item || "").trim())
        .map((value, idx) => ({ value, idx }))
        .filter((item) => item.value && !botTakeoverSeats.has(item.idx))
        .map((item) => item.value)
      : [];
    const ackUids = Array.isArray(room.startRevealAckUids)
      ? room.startRevealAckUids.map((item) => String(item || "").trim()).filter(Boolean)
      : [];
    const ackSet = new Set(ackUids);

    if (String(room.status || "") !== "playing") {
      return {
        ok: true,
        pending: false,
        released: false,
        humanCount: humanUids.length,
        ackCount: ackSet.size,
      };
    }

    const seatIndex = getSeatForUser(room, uid);
    if (seatIndex < 0) {
      throw new HttpsError("permission-denied", "Tu ne fais pas partie de cette salle.");
    }

    ackSet.add(uid);
    if (room.startRevealPending !== true) {
      return {
        ok: true,
        pending: false,
        released: false,
        humanCount: humanUids.length,
        ackCount: ackSet.size,
      };
    }

    const nextAckUids = Array.from(ackSet);
    const ready = humanUids.length > 0 && humanUids.every((humanUid) => ackSet.has(humanUid));

    tx.update(roomRef, {
      startRevealAckUids: nextAckUids,
      startRevealPending: ready ? false : true,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    });

    return {
      ok: true,
      pending: !ready,
      released: ready,
      humanCount: humanUids.length,
      ackCount: nextAckUids.length,
    };
  });

  if (ackResult?.released === true) {
    await processPendingBotTurns(roomId);
  }

  return ackResult;
});

exports.leaveRoom = publicOnCall("leaveRoom", async (request) => {
  const { uid } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const roomId = String(payload.roomId || "").trim();

  if (!roomId) {
    throw new HttpsError("invalid-argument", "roomId requis.");
  }

  const roomRef = db.collection(ROOMS_COLLECTION).doc(roomId);
  let shouldCleanup = false;
  let shouldNudgeBots = false;

  const result = await db.runTransaction(async (tx) => {
    const roomSnap = await tx.get(roomRef);
    if (!roomSnap.exists) {
      return {
        ok: true,
        deleted: true,
        status: "missing",
      };
    }

    const room = roomSnap.data() || {};
    const currentUids = Array.from({ length: 4 }, (_, idx) => String((room.playerUids || [])[idx] || ""));
    if (!currentUids.includes(uid)) {
      return {
        ok: true,
        deleted: false,
        status: String(room.status || ""),
      };
    }

    const status = String(room.status || "");
    const seatIndex = currentUids.findIndex((candidate) => candidate === uid);
    const nextPlayerUids = currentUids.slice();
    if (seatIndex >= 0) nextPlayerUids[seatIndex] = "";
    const currentNames = Array.from({ length: 4 }, (_, idx) => String((room.playerNames || [])[idx] || ""));
    const nextPlayerNames = currentNames.slice();
    if (seatIndex >= 0) {
      nextPlayerNames[seatIndex] = status === "playing" ? botSeatLabel(seatIndex) : "";
    }

    const nextSeats = { ...getRoomSeats(room) };
    delete nextSeats[uid];
    const blockedRejoinUids = Array.from(getBlockedRejoinSet(room));
    if (!blockedRejoinUids.includes(uid)) {
      blockedRejoinUids.push(uid);
    }
    const nextPresence = room.roomPresenceMs && typeof room.roomPresenceMs === "object"
      ? { ...room.roomPresenceMs }
      : {};
    delete nextPresence[uid];
    const nextBotTakeoverSeats = Array.isArray(room.botTakeoverSeats)
      ? room.botTakeoverSeats
        .map((seat) => Number(seat))
        .filter((seat) => Number.isFinite(seat) && seat >= 0 && seat < 4 && seat !== seatIndex)
      : [];
    const nextGraceUntil = room.botGraceUntilMs && typeof room.botGraceUntilMs === "object"
      ? { ...room.botGraceUntilMs }
      : {};
    delete nextGraceUntil[String(seatIndex)];

    const humans = nextPlayerUids.filter(Boolean).length;
    if (humans <= 0) {
      shouldCleanup = true;
      tx.set(roomRef, {
        status: "closing",
        playerUids: ["", "", "", ""],
        playerNames: ["", "", "", ""],
        blockedRejoinUids,
        playerEmails: admin.firestore.FieldValue.delete(),
        seats: {},
        roomPresenceMs: nextPresence,
        botTakeoverSeats: nextBotTakeoverSeats,
        botGraceUntilMs: Object.keys(nextGraceUntil).length > 0 ? nextGraceUntil : admin.firestore.FieldValue.delete(),
        humanCount: 0,
        botCount: 4,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      }, { merge: true });
      return {
        ok: true,
        deleted: true,
        status: "closing",
      };
    }

    const nextAckUids = Array.isArray(room.startRevealAckUids)
      ? room.startRevealAckUids.map((item) => String(item || "").trim()).filter(Boolean).filter((item) => item !== uid)
      : [];
    const revealPending = room.startRevealPending === true;
    const revealReady = revealPending === true
      && nextPlayerUids.filter(Boolean).every((playerUid) => nextAckUids.includes(playerUid));
    const nextBotCount = Math.max(0, 4 - humans);

    tx.update(roomRef, {
      playerUids: nextPlayerUids,
      playerNames: nextPlayerNames,
      blockedRejoinUids,
      playerEmails: admin.firestore.FieldValue.delete(),
      seats: nextSeats,
      roomPresenceMs: nextPresence,
      botTakeoverSeats: nextBotTakeoverSeats,
      botGraceUntilMs: Object.keys(nextGraceUntil).length > 0 ? nextGraceUntil : admin.firestore.FieldValue.delete(),
      humanCount: humans,
      botCount: nextBotCount,
      startRevealAckUids: nextAckUids,
      startRevealPending: revealPending === true ? !revealReady : false,
      ownerUid: room.ownerUid === uid
        ? String(nextPlayerUids.find(Boolean) || "")
        : String(room.ownerUid || ""),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    });

    if (status === "playing") {
      shouldNudgeBots = true;
    }

    return {
      ok: true,
      deleted: false,
      status: String(room.status || ""),
      humanCount: humans,
      botCount: nextBotCount,
      revealPending: revealPending === true ? !revealReady : false,
    };
  });

  if (shouldNudgeBots) {
    await processPendingBotTurns(roomId);
  }

  if (!shouldCleanup) {
    return result;
  }

  await cleanupRoom(roomRef);
  return {
    ok: true,
    deleted: true,
    status: "deleted",
  };
});

exports.finalizeGame = publicOnCall("finalizeGame", async (request) => {
  const { uid } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const roomId = String(payload.roomId || "").trim();

  if (!roomId) {
    throw new HttpsError("invalid-argument", "roomId requis.");
  }

  const roomRef = db.collection(ROOMS_COLLECTION).doc(roomId);
  return db.runTransaction(async (tx) => {
    const [roomSnap, stateSnap] = await Promise.all([
      tx.get(roomRef),
      tx.get(gameStateRef(roomId)),
    ]);
    if (!roomSnap.exists) {
      throw new HttpsError("not-found", "Salle introuvable.");
    }

    const room = roomSnap.data() || {};
    if (getSeatForUser(room, uid) < 0) {
      throw new HttpsError("permission-denied", "Tu ne fais pas partie de cette salle.");
    }

    const status = String(room.status || "");
    if (status === "ended" || status === "closing" || status === "closed") {
      return {
        ok: true,
        alreadyFinalized: true,
        status,
        winnerSeat: typeof room.winnerSeat === "number" ? room.winnerSeat : -1,
        winnerUid: String(room.winnerUid || ""),
      };
    }
    if (status !== "playing") {
      throw new HttpsError("failed-precondition", "La partie n'est pas en cours.");
    }

    const state = stateSnap.exists ? normalizeGameState(stateSnap.data(), room) : normalizeGameState({}, room);
    let winnerSeat = state.winnerSeat;
    let endedReason = state.endedReason || "";

    if (winnerSeat < 0) {
      for (let seat = 0; seat < 4; seat += 1) {
        if (countRemainingTilesForSeat(state.seatHands, seat) === 0) {
          winnerSeat = seat;
          endedReason = "out";
          break;
        }
      }
    }
    if (winnerSeat < 0 && safeInt(state.passesInRow) >= 4) {
      winnerSeat = computeBlockedWinnerSeat(state.seatHands);
      endedReason = "block";
    }
    if (winnerSeat < 0 || winnerSeat > 3) {
      throw new HttpsError("failed-precondition", "Aucun gagnant serveur disponible.");
    }

    const winnerUid = getWinnerUidForSeat(room, winnerSeat);
    tx.update(roomRef, {
      status: "ended",
      endedAt: admin.firestore.FieldValue.serverTimestamp(),
      endedAtMs: Date.now(),
      winnerSeat,
      winnerUid,
      endedReason: endedReason || "out",
      endClicks: {},
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    });
    tx.set(gameStateRef(roomId), {
      winnerSeat,
      winnerUid,
      endedReason: endedReason || "out",
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });

    return {
      ok: true,
      alreadyFinalized: false,
      status: "ended",
      winnerSeat,
      winnerUid,
      endedReason: endedReason || "out",
    };
  });
});

exports.confirmGameEnd = publicOnCall("confirmGameEnd", async (request) => {
  const { uid } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const roomId = String(payload.roomId || "").trim();

  if (!roomId) {
    throw new HttpsError("invalid-argument", "roomId requis.");
  }

  const roomRef = db.collection(ROOMS_COLLECTION).doc(roomId);
  let shouldCleanup = false;

  const result = await db.runTransaction(async (tx) => {
    const roomSnap = await tx.get(roomRef);
    if (!roomSnap.exists) {
      return {
        ok: true,
        state: "missing",
      };
    }

    const room = roomSnap.data() || {};
    if (getSeatForUser(room, uid) < 0) {
      throw new HttpsError("permission-denied", "Tu ne fais pas partie de cette salle.");
    }

    const status = String(room.status || "");
    if (status === "closed") {
      return {
        ok: true,
        state: "deleted",
      };
    }
    if (status === "closing") {
      shouldCleanup = true;
      return {
        ok: true,
        state: "pending",
      };
    }
    if (status !== "ended") {
      return {
        ok: true,
        state: "no_room",
      };
    }

    const playerUids = Array.isArray(room.playerUids) ? room.playerUids.filter(Boolean) : [];
    const currentEndClicks = room.endClicks && typeof room.endClicks === "object" ? room.endClicks : {};
    const nextEndClicks = {
      ...currentEndClicks,
      [uid]: true,
    };

    const allClicked = playerUids.length > 0 && playerUids.every((playerUid) => nextEndClicks[playerUid] === true);
    if (!allClicked) {
      tx.update(roomRef, {
        endClicks: nextEndClicks,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      });
      return {
        ok: true,
        state: "pending",
      };
    }

    shouldCleanup = true;
    tx.update(roomRef, {
      status: "closing",
      endClicks: nextEndClicks,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    });
    return {
      ok: true,
      state: "deleted",
    };
  });

  if (!shouldCleanup) {
    return result;
  }

  await cleanupRoom(roomRef);
  return {
    ok: true,
    state: "deleted",
  };
});

exports.submitAction = publicOnCall("submitAction", async (request) => {
  const { uid } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const roomId = String(payload.roomId || "").trim();
  const clientActionId = sanitizeText(payload.clientActionId || "", 80);
  const action = payload.action && typeof payload.action === "object" ? payload.action : null;

  if (!roomId || !action) {
    throw new HttpsError("invalid-argument", "roomId et action sont requis.");
  }
  if (!clientActionId) {
    throw new HttpsError("invalid-argument", "clientActionId requis.");
  }

  const type = String(action.type || "").trim();
  if (type !== "play" && type !== "pass") {
    throw new HttpsError("invalid-argument", "Type d'action invalide.");
  }

  const roomRef = db.collection(ROOMS_COLLECTION).doc(roomId);
  const result = await db.runTransaction(async (tx) => {
    const [roomSnap, stateSnap] = await Promise.all([
      tx.get(roomRef),
      tx.get(gameStateRef(roomId)),
    ]);
    if (!roomSnap.exists) {
      throw new HttpsError("not-found", "Salle introuvable.");
    }

    const room = roomSnap.data() || {};
    if (room.status !== "playing") {
      throw new HttpsError("failed-precondition", "La partie n'est pas en cours.");
    }
    if (room.startRevealPending === true) {
      throw new HttpsError("failed-precondition", "La partie se synchronise encore.");
    }
    const localSeat = getSeatForUser(room, uid);
    if (localSeat < 0) {
      throw new HttpsError("permission-denied", "Tu ne fais pas partie de cette salle.");
    }

    if (typeof room.currentPlayer === "number" && room.currentPlayer !== localSeat) {
      throw new HttpsError("failed-precondition", `Hors tour. Joueur attendu: ${room.currentPlayer + 1}`);
    }

    const currentState = stateSnap.exists
      ? normalizeGameState(stateSnap.data(), room)
      : createInitialGameState(room, Array.isArray(room.deckOrder) && room.deckOrder.length === 28 ? room.deckOrder : makeDeckOrder());

    if (currentState.winnerSeat >= 0) {
      throw new HttpsError("failed-precondition", "La partie est déjà terminée.");
    }
    if (typeof currentState.currentPlayer === "number" && currentState.currentPlayer !== localSeat) {
      throw new HttpsError("failed-precondition", `Hors tour. Joueur attendu: ${currentState.currentPlayer + 1}`);
    }

    if (currentState.idempotencyKeys[clientActionId] === true) {
      return {
        ok: true,
        duplicate: true,
        seq: safeSignedInt(currentState.appliedActionSeq),
        nextPlayer: currentState.currentPlayer,
        status: room.status,
      };
    }

    const resolvedMove = resolveRequestedMove(currentState, localSeat, action);
    const batchResult = applyActionBatchInTransaction(
      tx,
      roomRef,
      room,
      currentState,
      roomId,
      resolvedMove,
      uid,
      { allowBotAdvance: false }
    );
    const nextState = batchResult.state;
    nextState.idempotencyKeys[clientActionId] = true;

    tx.set(gameStateRef(roomId), buildGameStateWrite(nextState), { merge: true });

    const roomUpdate = buildRoomUpdateFromGameState(room, nextState, batchResult.records);
    tx.update(roomRef, roomUpdate);

    const lastRecord = batchResult.records.length > 0 ? batchResult.records[batchResult.records.length - 1] : null;
    return {
      ok: true,
      duplicate: false,
      seq: lastRecord ? lastRecord.seq : safeSignedInt(nextState.appliedActionSeq),
      nextPlayer: nextState.currentPlayer,
      status: nextState.winnerSeat >= 0 ? "ended" : "playing",
      winnerSeat: nextState.winnerSeat,
      winnerUid: nextState.winnerUid,
      endedReason: nextState.endedReason,
    };
  });

  if (result?.status === "playing" && typeof result.nextPlayer === "number") {
    await processPendingBotTurns(roomId);
  }

  return result;
});

exports.claimWinReward = publicOnCall("claimWinReward", async (request) => {
  const { uid, email } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const roomId = String(payload.roomId || "").trim();

  if (!roomId) {
    throw new HttpsError("invalid-argument", "roomId requis.");
  }

  const roomRef = db.collection(ROOMS_COLLECTION).doc(roomId);
  const settlementRef = roomRef.collection("settlements").doc(uid);

  const result = await db.runTransaction(async (tx) => {
    const [roomSnap, settlementSnap, stateSnap] = await Promise.all([
      tx.get(roomRef),
      tx.get(settlementRef),
      tx.get(gameStateRef(roomId)),
    ]);

    if (!roomSnap.exists) {
      throw new HttpsError("not-found", "Salle introuvable.");
    }

    const room = roomSnap.data() || {};
    const seat = getSeatForUser(room, uid);
    const state = stateSnap.exists ? normalizeGameState(stateSnap.data(), room) : null;
    const winnerSeat = typeof room.winnerSeat === "number"
      ? room.winnerSeat
      : (state && typeof state.winnerSeat === "number" ? state.winnerSeat : -1);
    const winnerUid = String(room.winnerUid || state?.winnerUid || "").trim();

    if (winnerUid) {
      if (winnerUid !== uid) {
        throw new HttpsError("permission-denied", "Ce compte n'est pas gagnant de cette partie.");
      }
    } else if (seat < 0) {
      throw new HttpsError("permission-denied", "Ce compte ne fait pas partie de cette partie.");
    } else if (winnerSeat < 0 || seat !== winnerSeat) {
      throw new HttpsError("permission-denied", "Ce compte n'est pas gagnant de cette partie.");
    }

    const settlementData = settlementSnap.exists ? (settlementSnap.data() || {}) : {};
    if (settlementData.rewardPaid === true) {
      return {
        ok: true,
        rewardGranted: false,
        reason: "already_paid",
        rewardAmountDoes: safeInt(settlementData.rewardAmountDoes) || resolveRoomRewardDoes(room),
      };
    }

    const rewardAmountDoes = resolveRoomRewardDoes(room);
    if (rewardAmountDoes <= 0) {
      throw new HttpsError("failed-precondition", "Gain invalide pour cette salle.");
    }

    const entryFundingRaw = room.entryFundingByUid && typeof room.entryFundingByUid === "object"
      ? (room.entryFundingByUid[uid] || null)
      : null;
    const provisionalSources = normalizeFundingSources(entryFundingRaw?.provisionalSources);
    const approvedEntryDoes = safeInt(entryFundingRaw?.approvedDoes);
    const provisionalEntryDoes = safeInt(entryFundingRaw?.provisionalDoes);
    let approvedRewardDoes = rewardAmountDoes;
    let provisionalRewardDoes = 0;

    if (provisionalEntryDoes > 0 && provisionalSources.length > 0) {
      const totalEntryDoes = Math.max(approvedEntryDoes + provisionalEntryDoes, provisionalEntryDoes);
      const provisionalRewardPool = Math.min(
        rewardAmountDoes,
        Math.round((rewardAmountDoes * provisionalEntryDoes) / Math.max(1, totalEntryDoes))
      );
      const allocatedRewardSources = allocateDoesProportionally(provisionalRewardPool, provisionalSources.map((item) => ({
        orderId: item.orderId,
        weight: item.amountDoes,
      })));
      const ordersSnap = await tx.get(db.collection(CLIENTS_COLLECTION).doc(uid).collection("orders"));
      const ordersById = new Map(ordersSnap.docs.map((item) => [item.id, item]));
      let pendingRewardDoes = 0;
      let promotedApprovedRewardDoes = 0;

      allocatedRewardSources.forEach((item) => {
        const orderSnap = ordersById.get(item.orderId);
        const orderData = orderSnap?.data() || {};
        const resolutionStatus = getOrderResolutionStatus(orderData);
        if (resolutionStatus === "pending" && isFundingV2Order(orderData)) {
          pendingRewardDoes += item.amountDoes;
          tx.set(orderSnap.ref, {
            provisionalGainDoes: safeInt(orderData.provisionalGainDoes) + safeInt(item.amountDoes),
            updatedAt: new Date().toISOString(),
            updatedAtMs: Date.now(),
          }, { merge: true });
          return;
        }
        if (resolutionStatus === "approved") {
          promotedApprovedRewardDoes += item.amountDoes;
        }
      });

      provisionalRewardDoes = pendingRewardDoes;
      approvedRewardDoes = Math.max(0, rewardAmountDoes - provisionalRewardPool) + promotedApprovedRewardDoes;
    } else if (approvedEntryDoes <= 0 && provisionalEntryDoes > 0) {
      approvedRewardDoes = 0;
      provisionalRewardDoes = rewardAmountDoes;
    }

    console.log("[BALANCE_DEBUG][FUNCTIONS][claimWinReward] reward split", JSON.stringify({
      uid,
      roomId,
      rewardAmountDoes,
      approvedEntryDoes,
      provisionalEntryDoes,
      provisionalSources,
      approvedRewardDoes,
      provisionalRewardDoes,
    }));

    const walletMutation = await applyWalletMutationTx(tx, {
      uid,
      email,
      type: "game_reward",
      note: `Gain de partie (${roomId})`,
      amountDoes: rewardAmountDoes,
      approvedRewardDoes,
      provisionalRewardDoes,
      amountGourdes: 0,
      deltaDoes: rewardAmountDoes,
      deltaExchangedGourdes: 0,
    });
    console.log("[BALANCE_DEBUG][FUNCTIONS][claimWinReward] wallet mutation", JSON.stringify({
      uid,
      roomId,
      rewardAmountDoes,
      afterDoes: safeInt(walletMutation.afterDoes),
      approvedRewardDoes,
      provisionalRewardDoes,
    }));

    tx.set(settlementRef, {
      uid,
      roomId,
      rewardPaid: true,
      rewardAmountDoes,
      claimedAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });

    return {
      ok: true,
      rewardGranted: true,
      rewardAmountDoes,
      does: walletMutation.afterDoes,
      approvedRewardDoes,
      provisionalRewardDoes,
    };
  });

  return result;
});

exports.recordAmbassadorOutcome = publicOnCall("recordAmbassadorOutcome", async (request) => {
  const { uid } = assertAuth(request);
  if (!AMBASSADOR_SYSTEM_ENABLED) {
    return {
      applied: false,
      changed: 0,
      skipped: 0,
      reason: "ambassador_disabled",
      results: [],
    };
  }
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const roomId = String(payload.roomId || "").trim();

  if (!roomId) {
    throw new HttpsError("invalid-argument", "roomId requis.");
  }

  const roomSnap = await db.collection(ROOMS_COLLECTION).doc(roomId).get();
  if (!roomSnap.exists) {
    throw new HttpsError("not-found", "Salle introuvable.");
  }

  const room = roomSnap.data() || {};
  const participants = Array.isArray(room.playerUids)
    ? [...new Set(room.playerUids.map((item) => String(item || "").trim()).filter(Boolean))]
    : [];
  if (!participants.includes(uid)) {
    throw new HttpsError("permission-denied", "Tu ne fais pas partie de cette salle.");
  }

  let winnerUid = String(room.winnerUid || "").trim();
  const winnerSeat = typeof room.winnerSeat === "number" ? Math.trunc(room.winnerSeat) : -1;
  if (!winnerUid && Array.isArray(room.playerUids) && winnerSeat >= 0 && winnerSeat < room.playerUids.length) {
    winnerUid = String(room.playerUids[winnerSeat] || "").trim();
  }

  if (!winnerUid) {
    throw new HttpsError("failed-precondition", "Le gagnant de la partie n'est pas encore connu.");
  }

  const results = [];
  let changed = 0;

  for (const playerUid of participants) {
    const result = await db.runTransaction(async (tx) => {
      const playerRef = walletRef(playerUid);
      const eventRef = db.collection(AMBASSADOR_EVENTS_COLLECTION).doc(`${roomId}_${playerUid}`);
      const [playerSnap, eventSnap] = await Promise.all([
        tx.get(playerRef),
        tx.get(eventRef),
      ]);

      if (eventSnap.exists) {
        return { playerUid, applied: false, reason: "already_recorded" };
      }
      if (!playerSnap.exists) {
        return { playerUid, applied: false, reason: "client_not_found" };
      }

      const playerData = playerSnap.data() || {};
      const ambassadorContext = deriveRootAmbassadorContext(playerData);
      const depth = Math.max(1, safeInt(playerData.ambassadorDepthFromRoot || ambassadorContext?.depth || 0));
      const isEligible = playerData.ambassadorCommissionEligible !== false && depth <= 3;
      if (!ambassadorContext?.rootAmbassadorId || !isEligible) {
        return { playerUid, applied: false, reason: "no_eligible_ambassador" };
      }

      const ambassadorRef = db.collection(AMBASSADORS_COLLECTION).doc(ambassadorContext.rootAmbassadorId);
      const ambassadorReferralRef = ambassadorRef.collection("referrals").doc(playerUid);
      const [ambassadorSnap, ambassadorReferralSnap] = await Promise.all([
        tx.get(ambassadorRef),
        tx.get(ambassadorReferralRef),
      ]);

      if (!ambassadorSnap.exists) {
        return { playerUid, applied: false, reason: "ambassador_not_found" };
      }

      const ambassadorData = ambassadorSnap.data() || {};
      const referralData = ambassadorReferralSnap.exists ? (ambassadorReferralSnap.data() || {}) : {};
      const delta = playerUid === winnerUid ? -AMBASSADOR_WIN_PENALTY : AMBASSADOR_LOSS_BONUS;
      const totalGamesTracked = safeInt(referralData.totalGamesTracked || referralData.totalGames) + 1;
      const winsTracked = safeInt(referralData.winsTracked || referralData.winCount) + (delta < 0 ? 1 : 0);
      const lossesTracked = safeInt(referralData.lossesTracked || referralData.lossCount) + (delta > 0 ? 1 : 0);

      tx.set(ambassadorRef, {
        doesBalance: safeSignedInt(ambassadorData.doesBalance) + delta,
        totalGames: safeInt(ambassadorData.totalGames) + 1,
        totalInvitedWins: safeInt(ambassadorData.totalInvitedWins) + (delta < 0 ? 1 : 0),
        totalInvitedLosses: safeInt(ambassadorData.totalInvitedLosses) + (delta > 0 ? 1 : 0),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      }, { merge: true });

      tx.set(ambassadorReferralRef, {
        userId: playerUid,
        clientUid: playerUid,
        email: sanitizeEmail(playerData.email || referralData.email || "", 160),
        displayName: sanitizeText(playerData.name || playerData.email || referralData.displayName || playerUid, 80),
        depth,
        parentClientUid: String(referralData.parentClientUid || "").trim(),
        rootAmbassadorId: ambassadorContext.rootAmbassadorId,
        isCommissionEligible: true,
        hasApprovedDeposit: playerData.hasApprovedDeposit === true,
        depositCount: playerData.hasApprovedDeposit === true
          ? Math.max(1, safeInt(referralData.depositCount || 1))
          : safeInt(referralData.depositCount),
        totalGamesTracked,
        totalGames: totalGamesTracked,
        winsTracked,
        winCount: winsTracked,
        lossesTracked,
        lossCount: lossesTracked,
        ambassadorDoesDelta: safeSignedInt(referralData.ambassadorDoesDelta) + delta,
        lastGameAt: admin.firestore.FieldValue.serverTimestamp(),
        createdAt: referralData.createdAt || admin.firestore.FieldValue.serverTimestamp(),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      }, { merge: true });

      tx.set(eventRef, {
        roomId,
        playerUid,
        ambassadorId: ambassadorContext.rootAmbassadorId,
        depth,
        delta,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
      }, { merge: true });

      tx.set(playerRef, {
        lastGameAt: admin.firestore.FieldValue.serverTimestamp(),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      }, { merge: true });

      return {
        playerUid,
        ambassadorId: ambassadorContext.rootAmbassadorId,
        delta,
        applied: true,
      };
    });

    results.push(result);
    if (result.applied) changed += 1;
  }

  return {
    applied: changed > 0,
    roomId,
    changed,
    skipped: results.length - changed,
    results,
  };
});

function messagePreviewFromRecord(data = {}) {
  const text = sanitizeText(data.text || "", 120);
  if (text) return text;
  if (String(data.mediaType || "") === "video") return "Video";
  if (String(data.mediaType || "") === "image") return "Image";
  return "Message";
}

function sanitizeGuestThreadId(value = "") {
  const safe = String(value || "")
    .trim()
    .replace(/[^a-zA-Z0-9_-]/g, "")
    .slice(0, 160);
  return safe.startsWith("guest_") ? safe : "";
}

function defaultGuestDisplayName(guestId = "") {
  const suffix = String(guestId || "").slice(-4).toUpperCase();
  return suffix ? `Anonyme ${suffix}` : "Anonyme";
}

function randomGuestAccessToken() {
  return crypto.randomBytes(24).toString("hex");
}

function supportThreadRecordForCallable(docSnap) {
  const base = snapshotRecordForCallable(docSnap);
  delete base.guestAccessToken;
  return base;
}

function sanitizeSupportMediaPayload(raw = {}) {
  const mediaType = sanitizeText(raw?.mediaType || "", 16).toLowerCase();
  if (mediaType !== "image" && mediaType !== "video") {
    return {
      mediaType: "",
      mediaUrl: "",
      mediaPath: "",
      fileName: "",
    };
  }

  const mediaUrl = sanitizeStorageAssetUrl(raw?.mediaUrl || "", 2000);
  const mediaPath = sanitizeText(raw?.mediaPath || "", 600);
  if (!mediaUrl || !mediaPath.startsWith("chat-media/")) {
    return {
      mediaType: "",
      mediaUrl: "",
      mediaPath: "",
      fileName: "",
    };
  }

  return {
    mediaType,
    mediaUrl,
    mediaPath,
    fileName: sanitizeText(raw?.fileName || "", 120),
  };
}

function supportMediaMatchesThread(mediaPath = "", threadId = "") {
  const safePath = String(mediaPath || "").trim();
  const safeThreadId = String(threadId || "").trim();
  if (!safePath || !safeThreadId) return false;
  return safePath.startsWith(`chat-media/support/${safeThreadId}/`);
}

function buildSupportMessageRecord(actor = {}, text = "", media = null, extras = {}) {
  const createdAtMs = Date.now();
  const expiresAtMs = createdAtMs + DISCUSSION_MESSAGE_RETENTION_MS;
  const safeMedia = sanitizeSupportMediaPayload(media);

  return {
    text: sanitizeText(text || "", MAX_PUBLIC_TEXT_LENGTH),
    mediaType: safeMedia.mediaType,
    mediaUrl: safeMedia.mediaUrl,
    mediaPath: safeMedia.mediaPath,
    fileName: safeMedia.fileName,
    senderRole: sanitizeText(actor.senderRole || "user", 20),
    senderType: sanitizeText(actor.senderType || "user", 20),
    senderKey: sanitizeText(actor.senderKey || "", 160),
    uid: sanitizeText(actor.uid || "", 160),
    guestId: sanitizeText(actor.guestId || "", 160),
    email: sanitizeEmail(actor.email || "", 160),
    displayName: sanitizeText(actor.displayName || "Utilisateur", 80) || "Utilisateur",
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
    createdAtMs,
    expiresAtMs,
    expiresAt: admin.firestore.Timestamp.fromMillis(expiresAtMs),
    pinned: false,
    pinnedAtMs: 0,
    pinnedAt: null,
    pinnedBy: "",
    editedAtMs: 0,
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    ...extras,
  };
}

async function resolveSupportThreadContext(request, payload = {}, options = {}) {
  const allowCreate = options.allowCreate !== false;

  if (request.auth?.uid) {
    const { uid, email } = assertAuth(request);
    const threadId = `user_${uid}`;
    const threadRef = db.collection(SUPPORT_THREADS_COLLECTION).doc(threadId);
    const [threadSnap, clientSnap] = await Promise.all([
      threadRef.get(),
      walletRef(uid).get(),
    ]);
    const clientData = clientSnap.exists ? (clientSnap.data() || {}) : {};
    const displayName = sanitizeText(
      clientData.name || clientData.displayName || String(email || "").split("@")[0] || "Utilisateur",
      80
    ) || "Utilisateur";

    if (!threadSnap.exists && !allowCreate) {
      throw new HttpsError("not-found", "Fil support introuvable.");
    }

    const patch = {
      threadId,
      participantType: "user",
      participantId: uid,
      participantUid: uid,
      guestId: "",
      participantName: displayName,
      participantEmail: sanitizeEmail(email || "", 160),
      status: threadSnap.exists ? sanitizeText(threadSnap.data()?.status || "open", 16) || "open" : "open",
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    };

    if (!threadSnap.exists) {
      patch.createdAt = admin.firestore.FieldValue.serverTimestamp();
      patch.createdAtMs = Date.now();
      patch.lastMessageText = "Aucun message";
      patch.lastMessageAt = null;
      patch.lastMessageAtMs = 0;
      patch.lastSenderRole = "";
      patch.unreadForAgent = false;
      patch.unreadForUser = false;
      patch.firstAgentReplyAt = null;
      patch.firstAgentReplyAtMs = 0;
      patch.resolvedAt = null;
      patch.resolvedAtMs = 0;
      patch.resolutionTag = "";
    }

    await threadRef.set(patch, { merge: true });
    const freshSnap = await threadRef.get();

    return {
      threadId,
      threadRef,
      threadSnap: freshSnap,
      guestToken: "",
      actor: {
        senderRole: "user",
        senderType: "user",
        senderKey: uid,
        uid,
        guestId: "",
        email: sanitizeEmail(email || "", 160),
        displayName,
      },
    };
  }

  const guestId = sanitizeGuestThreadId(payload.guestId || "");
  if (!guestId) {
    throw new HttpsError("invalid-argument", "Identifiant invité invalide.");
  }

  const threadId = guestId;
  const threadRef = db.collection(SUPPORT_THREADS_COLLECTION).doc(threadId);
  const requestedToken = sanitizeText(payload.guestToken || "", 128);
  const requestedName = sanitizeText(payload.displayName || "", 80);
  const displayName = requestedName || defaultGuestDisplayName(guestId);
  const threadSnap = await threadRef.get();

  if (!threadSnap.exists && !allowCreate) {
    throw new HttpsError("not-found", "Fil support introuvable.");
  }

  let issuedToken = "";
  if (!threadSnap.exists) {
    issuedToken = randomGuestAccessToken();
    await threadRef.set({
      threadId,
      participantType: "guest",
      participantId: guestId,
      participantUid: "",
      guestId,
      participantName: displayName,
      participantEmail: "",
      guestAccessToken: issuedToken,
      status: "open",
      unreadForAgent: false,
      unreadForUser: false,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      createdAtMs: Date.now(),
      lastMessageText: "Aucun message",
      lastMessageAt: null,
      lastMessageAtMs: 0,
      lastSenderRole: "",
      firstAgentReplyAt: null,
      firstAgentReplyAtMs: 0,
      resolvedAt: null,
      resolvedAtMs: 0,
      resolutionTag: "",
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
  } else {
    const existingData = threadSnap.data() || {};
    issuedToken = sanitizeText(existingData.guestAccessToken || "", 128);
    if (!issuedToken) {
      issuedToken = randomGuestAccessToken();
    } else if (!requestedToken || requestedToken !== issuedToken) {
      throw new HttpsError("permission-denied", "Accès invité refusé.");
    }

    await threadRef.set({
      participantName: displayName,
      guestAccessToken: issuedToken,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
  }

  const freshSnap = await threadRef.get();
  return {
    threadId,
    threadRef,
    threadSnap: freshSnap,
    guestToken: issuedToken,
    actor: {
      senderRole: "guest",
      senderType: "guest",
      senderKey: guestId,
      uid: "",
      guestId,
      email: "",
      displayName,
    },
  };
}

async function refreshSupportThreadSummaryAdmin(threadId = "") {
  const safeThreadId = String(threadId || "").trim();
  if (!safeThreadId) return;

  const latestSnap = await db.collection(SUPPORT_THREADS_COLLECTION)
    .doc(safeThreadId)
    .collection(SUPPORT_MESSAGES_SUBCOLLECTION)
    .orderBy("createdAtMs", "desc")
    .limit(1)
    .get();

  if (latestSnap.empty) {
    await db.collection(SUPPORT_THREADS_COLLECTION).doc(safeThreadId).set({
      lastMessageText: "Aucun message",
      lastMessageAt: null,
      lastMessageAtMs: 0,
      lastSenderRole: "",
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
    return;
  }

  const latest = latestSnap.docs[0].data() || {};
  const patch = {
    lastMessageText: messagePreviewFromRecord(latest),
    lastMessageAtMs: safeInt(latest.createdAtMs),
    lastSenderRole: sanitizeText(latest.senderRole || "", 20),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };
  if (latest.createdAt) {
    patch.lastMessageAt = latest.createdAt;
  }
  await db.collection(SUPPORT_THREADS_COLLECTION).doc(safeThreadId).set(patch, { merge: true });
}

function collectUniqueDocsFromSnapshots(...snapshots) {
  const seen = new Set();
  const out = [];
  for (const snap of snapshots) {
    for (const docSnap of snap?.docs || []) {
      const path = String(docSnap?.ref?.path || "");
      if (!path || seen.has(path)) continue;
      seen.add(path);
      out.push(docSnap);
    }
  }
  return out;
}

async function deleteDiscussionMediaIfNeeded(data = {}) {
  const mediaPath = String(data.mediaPath || "").trim();
  if (!mediaPath) return;
  try {
    await admin.storage().bucket().file(mediaPath).delete();
  } catch (error) {
    const code = Number(error?.code || 0);
    const notFound = code === 404 || String(error?.message || "").toLowerCase().includes("no such object");
    if (!notFound) {
      throw error;
    }
  }
}

exports.purgeExpiredDiscussionMessages = onSchedule("every 60 minutes", async () => {
  const nowMs = Date.now();
  const legacyCutoffMs = nowMs - DISCUSSION_MESSAGE_RETENTION_MS;

  const [
    channelByExpirySnap,
    channelLegacySnap,
    supportByExpirySnap,
    supportLegacySnap,
  ] = await Promise.all([
    db.collection(CHAT_COLLECTION)
      .where("expiresAtMs", "<=", nowMs)
      .limit(DISCUSSION_PURGE_BATCH_SIZE)
      .get(),
    db.collection(CHAT_COLLECTION)
      .where("createdAtMs", "<=", legacyCutoffMs)
      .limit(DISCUSSION_PURGE_BATCH_SIZE)
      .get(),
    db.collectionGroup(SUPPORT_MESSAGES_SUBCOLLECTION)
      .where("expiresAtMs", "<=", nowMs)
      .limit(DISCUSSION_PURGE_BATCH_SIZE)
      .get(),
    db.collectionGroup(SUPPORT_MESSAGES_SUBCOLLECTION)
      .where("createdAtMs", "<=", legacyCutoffMs)
      .limit(DISCUSSION_PURGE_BATCH_SIZE)
      .get(),
  ]);

  const expiredChannelDocs = collectUniqueDocsFromSnapshots(channelByExpirySnap, channelLegacySnap);
  const expiredSupportDocs = collectUniqueDocsFromSnapshots(supportByExpirySnap, supportLegacySnap);
  const touchedThreads = new Set();
  let channelDeleted = 0;
  let supportDeleted = 0;
  let mediaErrors = 0;

  for (const docSnap of expiredChannelDocs) {
    const data = docSnap.data() || {};
    if (data.pinned === true) continue;
    const expiresAtMs = safeSignedInt(data.expiresAtMs) || (safeSignedInt(data.createdAtMs) + DISCUSSION_MESSAGE_RETENTION_MS);
    if (expiresAtMs > nowMs) continue;
    try {
      await deleteDiscussionMediaIfNeeded(data);
      await docSnap.ref.delete();
      channelDeleted += 1;
    } catch (error) {
      mediaErrors += 1;
      console.warn("[DISCUSSION_PURGE][CHANNEL]", docSnap.ref.path, error?.message || error);
    }
  }

  for (const docSnap of expiredSupportDocs) {
    const data = docSnap.data() || {};
    const threadId = String(docSnap.ref.parent?.parent?.id || "").trim();
    if (data.pinned === true) continue;
    const expiresAtMs = safeSignedInt(data.expiresAtMs) || (safeSignedInt(data.createdAtMs) + DISCUSSION_MESSAGE_RETENTION_MS);
    if (expiresAtMs > nowMs) continue;
    try {
      await deleteDiscussionMediaIfNeeded(data);
      await docSnap.ref.delete();
      if (threadId) touchedThreads.add(threadId);
      supportDeleted += 1;
    } catch (error) {
      mediaErrors += 1;
      console.warn("[DISCUSSION_PURGE][SUPPORT]", docSnap.ref.path, error?.message || error);
    }
  }

  for (const threadId of touchedThreads) {
    try {
      await refreshSupportThreadSummaryAdmin(threadId);
    } catch (error) {
      console.warn("[DISCUSSION_PURGE][THREAD_SUMMARY]", threadId, error?.message || error);
    }
  }

  console.info("[DISCUSSION_PURGE]", JSON.stringify({
    channelDeleted,
    supportDeleted,
    touchedThreads: touchedThreads.size,
    mediaErrors,
  }));
});

exports.sweepRoomPresence = onSchedule("every 1 minutes", async () => {
  const nowMs = Date.now();
  const roomsSnap = await db
    .collection(ROOMS_COLLECTION)
    .where("status", "in", ["waiting", "playing"])
    .limit(200)
    .get();

  const roomsToNudge = [];

  for (const docSnap of roomsSnap.docs) {
    const roomId = docSnap.id;
    try {
      const result = await db.runTransaction(async (tx) => {
        const freshSnap = await tx.get(docSnap.ref);
        if (!freshSnap.exists) return { changed: false };

        const room = freshSnap.data() || {};
        const playerUids = Array.from({ length: 4 }, (_, idx) => String((room.playerUids || [])[idx] || ""));
        if (!playerUids.some(Boolean)) return { changed: false };

        const currentPresence = room.roomPresenceMs && typeof room.roomPresenceMs === "object"
          ? { ...room.roomPresenceMs }
          : {};
        const playerNames = Array.from({ length: 4 }, (_, idx) => String((room.playerNames || [])[idx] || ""));
        const seats = { ...getRoomSeats(room) };
        const takeoverSeats = getBotTakeoverSeatSet(room);
        const graceUntil = room.botGraceUntilMs && typeof room.botGraceUntilMs === "object"
          ? { ...room.botGraceUntilMs }
          : {};
        const blockedRejoinUids = Array.from(getBlockedRejoinSet(room));

        let removedAny = false;
        let changed = false;

        for (let seat = 0; seat < 4; seat += 1) {
          const seatUid = String(playerUids[seat] || "").trim();
          if (!seatUid) continue;

          const lastSeen = safeSignedInt(currentPresence[seatUid]);
          if (lastSeen <= 0) continue;

          const offlineForMs = nowMs - lastSeen;
          if (offlineForMs < ROOM_DISCONNECT_TAKEOVER_MS) continue;

          if (!takeoverSeats.has(seat)) {
            takeoverSeats.add(seat);
            graceUntil[String(seat)] = lastSeen + ROOM_DISCONNECT_GRACE_MS;
            changed = true;
          }

          const seatGraceUntil = safeSignedInt(graceUntil[String(seat)]);
          if (seatGraceUntil > 0 && nowMs > seatGraceUntil) {
            removedAny = true;
            changed = true;
            playerUids[seat] = "";
            playerNames[seat] = String(room.status || "") === "playing" ? botSeatLabel(seat) : "";
            delete seats[seatUid];
            delete currentPresence[seatUid];
            takeoverSeats.delete(seat);
            delete graceUntil[String(seat)];
            if (!blockedRejoinUids.includes(seatUid)) {
              blockedRejoinUids.push(seatUid);
            }
          }
        }

        if (!changed && !removedAny) return { changed: false };

        const updates = {
          roomPresenceMs: currentPresence,
          botTakeoverSeats: Array.from(takeoverSeats),
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        };

        if (Object.keys(graceUntil).length > 0) {
          updates.botGraceUntilMs = graceUntil;
        } else {
          updates.botGraceUntilMs = admin.firestore.FieldValue.delete();
        }

        if (removedAny) {
          const humans = playerUids.filter(Boolean).length;
          updates.playerUids = playerUids;
          updates.playerNames = playerNames;
          updates.seats = seats;
          updates.humanCount = humans;
          updates.botCount = Math.max(0, 4 - humans);
          updates.blockedRejoinUids = blockedRejoinUids;
          updates.playerEmails = admin.firestore.FieldValue.delete();
        }

        const effectiveRoom = {
          ...room,
          playerUids: updates.playerUids || room.playerUids,
          playerNames: updates.playerNames || room.playerNames,
          seats: updates.seats || room.seats,
          botTakeoverSeats: updates.botTakeoverSeats,
        };

        const shouldNudgeBots = String(room.status || "") === "playing"
          && room.startRevealPending !== true
          && Number.isFinite(safeInt(room.currentPlayer))
          && !isSeatHuman(effectiveRoom, safeInt(room.currentPlayer));

        tx.update(docSnap.ref, updates);
        return { changed: true, shouldNudgeBots };
      });

      if (result?.shouldNudgeBots === true) {
        roomsToNudge.push(roomId);
      }
    } catch (error) {
      console.warn("[SWEEP_PRESENCE]", roomId, error?.message || error);
    }
  }

  for (const targetRoomId of roomsToNudge) {
    await processPendingBotTurns(targetRoomId);
  }
});

exports.capturePresenceAnalytics = onSchedule("every 5 minutes", async () => {
  const bucketMs = getPresenceBucketStartMs(Date.now());
  const localKeys = getPresenceLocalKeys(bucketMs);
  const sample = await collectPresenceAnalyticsNow(bucketMs);
  const snapshotRef = presenceSnapshotsCollection().doc(String(bucketMs));
  const dailyRef = presenceDailyCollection().doc(localKeys.dayKey);
  const monthlyRef = presenceMonthlyCollection().doc(localKeys.monthKey);
  const hourRef = presenceHourCollection().doc(localKeys.hourKey);
  const weekdayRef = presenceWeekdayCollection().doc(localKeys.weekdayKey);
  const liveRef = analyticsMetaRef("presenceLive");

  await db.runTransaction(async (tx) => {
    const [dailySnap, monthlySnap, hourSnap, weekdaySnap] = await Promise.all([
      tx.get(dailyRef),
      tx.get(monthlyRef),
      tx.get(hourRef),
      tx.get(weekdayRef),
    ]);

    tx.set(snapshotRef, {
      bucketMs,
      ...localKeys,
      ...sample,
      createdAtMs: bucketMs,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });

    tx.set(dailyRef, buildPresenceRollupUpdate(dailySnap.data() || {}, sample, "dayKey", localKeys.dayKey), { merge: true });
    tx.set(monthlyRef, buildPresenceRollupUpdate(monthlySnap.data() || {}, sample, "monthKey", localKeys.monthKey), { merge: true });
    tx.set(hourRef, buildPresenceRollupUpdate(hourSnap.data() || {}, sample, "hourKey", localKeys.hourKey), { merge: true });
    tx.set(weekdayRef, buildPresenceRollupUpdate(weekdaySnap.data() || {}, sample, "weekdayKey", localKeys.weekdayKey), { merge: true });
    tx.set(liveRef, {
      ...localKeys,
      ...sample,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
  });

  const expiredBeforeMs = bucketMs - PRESENCE_ANALYTICS_SNAPSHOT_RETENTION_MS;
  const expiredSnap = await presenceSnapshotsCollection()
    .where("bucketMs", "<", expiredBeforeMs)
    .limit(50)
    .get();

  if (!expiredSnap.empty) {
    const batch = db.batch();
    expiredSnap.docs.forEach((docSnap) => batch.delete(docSnap.ref));
    await batch.commit();
  }
});

async function getSettingsSnapshotData() {
  const data = await readRawPublicAppSettings();
  return normalizePublicAppSettings(data);
}

async function getPublicPaymentConfig() {
  const [settings, methodsSnap] = await Promise.all([
    getSettingsSnapshotData(),
    db.collection("paymentMethods").get(),
  ]);
  const methods = methodsSnap.docs
    .map((item) => sanitizePublicMethod(item))
    .filter(Boolean);
  return { settings, methods };
}

exports.getPublicPaymentOptionsSecure = publicOnCall("getPublicPaymentOptionsSecure", async () => {
  const data = await getPublicPaymentConfig();
  return {
    methods: data.methods,
    settings: data.settings,
  };
});

exports.getPublicGameStakeOptionsSecure = publicOnCall("getPublicGameStakeOptionsSecure", async () => {
  const settings = await getSettingsSnapshotData();
  return {
    options: settings.gameStakeOptions.map((item) => ({
      id: item.id,
      stakeDoes: item.stakeDoes,
      rewardDoes: item.rewardDoes,
      enabled: item.enabled === true,
      sortOrder: item.sortOrder,
    })),
  };
});

exports.getPublicRuntimeConfigSecure = publicOnCall("getPublicRuntimeConfigSecure", async () => {
  const settings = await getSettingsSnapshotData();
  const pushConfig = getDashboardWebPushConfig();
  return {
    appCheckSiteKey: String(settings.appCheckSiteKey || ""),
    appCheckConfigured: !!String(settings.appCheckSiteKey || "").trim(),
    dashboardWebPushPublicKey: String(pushConfig.publicKey || ""),
    dashboardWebPushEnabled: !!String(pushConfig.publicKey || "").trim(),
    provisionalDepositsEnabled: isProvisionalFundingEnabled(settings),
  };
});

exports.getShareSitePromoStatus = publicOnCall("getShareSitePromoStatus", async (request) => {
  const { uid } = assertAuth(request);
  const [snap, walletSnap] = await Promise.all([
    shareSitePromoRef(uid).get(),
    walletRef(uid).get(),
  ]);
  const walletData = walletSnap.exists ? (walletSnap.data() || {}) : {};
  const response = buildShareSitePromoResponse(snap.exists ? (snap.data() || {}) : {}, Date.now());
  return {
    ...response,
    accountFrozen: walletData.accountFrozen === true,
    freezeReason: String(walletData.freezeReason || ""),
  };
});

exports.recordShareSitePromo = publicOnCall("recordShareSitePromo", async (request) => {
  const { uid, email } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const actionId = sanitizeText(payload.actionId || "", 80);
  const shareSource = sanitizeText(payload.shareSource || "", 40).toLowerCase();
  if (!actionId) {
    throw new HttpsError("invalid-argument", "actionId requis.");
  }

  const promoRef = shareSitePromoRef(uid);
  const result = await db.runTransaction(async (tx) => {
    const [promoSnap, walletSnap] = await Promise.all([
      tx.get(promoRef),
      tx.get(walletRef(uid)),
    ]);
    const walletData = walletSnap.exists ? (walletSnap.data() || {}) : {};
    assertWalletNotFrozen(walletData);
    const nowMs = Date.now();
    const nextState = normalizeShareSitePromoState(promoSnap.exists ? (promoSnap.data() || {}) : {}, nowMs);

    if (nextState.rewardGranted === true && nextState.cooldownUntilMs > nowMs) {
      tx.set(promoRef, {
        ...nextState,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      }, { merge: true });
      return buildShareSitePromoResponse(nextState, nowMs);
    }

    if (nextState.actionIds[actionId]) {
      return buildShareSitePromoResponse(nextState, nowMs);
    }

    if (nextState.cycleStartedAtMs <= 0) {
      nextState.cycleStartedAtMs = nowMs;
    }

    nextState.actionIds = trimIdempotencyKeys({
      ...nextState.actionIds,
      [actionId]: true,
    }, SHARE_SITE_PROMO_ACTION_CACHE);
    nextState.shareCount = Math.min(
      SHARE_SITE_PROMO_TARGET,
      Math.max(0, safeInt(nextState.shareCount) + 1)
    );
    nextState.lastShareAtMs = nowMs;
    nextState.lastShareSource = shareSource;

    let rewardGrantedNow = false;
    if (nextState.shareCount >= SHARE_SITE_PROMO_TARGET && nextState.rewardGranted !== true) {
      rewardGrantedNow = true;
      nextState.rewardGranted = true;
      nextState.rewardGrantedAtMs = nowMs;
      nextState.cooldownUntilMs = nowMs + SHARE_SITE_PROMO_COOLDOWN_MS;

      await applyWalletMutationTx(tx, {
        uid,
        email: email || String(walletData.email || ""),
        type: "share_reward_bonus",
        note: "Bonus partage du site",
        amountDoes: SHARE_SITE_PROMO_REWARD_DOES,
        deltaDoes: SHARE_SITE_PROMO_REWARD_DOES,
      });
    }

    tx.set(promoRef, {
      ...nextState,
      rewardGrantedNow,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });

    return {
      ...buildShareSitePromoResponse(nextState, nowMs),
      rewardGrantedNow,
    };
  });

  return result;
});

exports.registerDashboardPushSubscriptionSecure = publicOnCall("registerDashboardPushSubscriptionSecure", async (request) => {
  const { uid, email } = assertFinanceAdmin(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const subscription = sanitizePushSubscriptionPayload(payload.subscription || payload);
  validatePushSubscriptionPayload(subscription);

  const subscriptionId = dashboardPushSubscriptionIdFromEndpoint(subscription.endpoint);
  const nowMs = Date.now();
  await dashboardPushSubscriptionsCollection().doc(subscriptionId).set({
    uid,
    email: sanitizeEmail(email || "", 160),
    endpoint: subscription.endpoint,
    expirationTime: subscription.expirationTime,
    keys: subscription.keys,
    platform: subscription.platform,
    userAgent: subscription.userAgent,
    enabled: true,
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
    createdAtMs: nowMs,
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    lastSeenAt: admin.firestore.FieldValue.serverTimestamp(),
    lastSeenAtMs: nowMs,
  }, { merge: true });

  return {
    ok: true,
    subscriptionId,
    enabled: true,
    webPushEnabled: ensureDashboardWebPushConfigured(),
  };
});

exports.unregisterDashboardPushSubscriptionSecure = publicOnCall("unregisterDashboardPushSubscriptionSecure", async (request) => {
  const { uid } = assertFinanceAdmin(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const endpoint = sanitizeWebPushEndpoint(payload.endpoint || "");
  const subscriptionId = sanitizeText(payload.subscriptionId || "", 128)
    || (endpoint ? dashboardPushSubscriptionIdFromEndpoint(endpoint) : "");

  if (!subscriptionId) {
    throw new HttpsError("invalid-argument", "Subscription introuvable.");
  }

  const ref = dashboardPushSubscriptionsCollection().doc(subscriptionId);
  const snap = await ref.get();
  if (snap.exists) {
    const data = snap.data() || {};
    if (String(data.uid || "") !== String(uid)) {
      throw new HttpsError("permission-denied", "Subscription non autorisée.");
    }
    await ref.delete();
  }

  return {
    ok: true,
    subscriptionId,
  };
});

exports.getDpaymentBootstrapConfig = publicOnCall("getDpaymentBootstrapConfig", async () => {
  const snap = await adminBootstrapRef().get();
  if (!snap.exists) {
    return {
      ok: true,
      bootstrapped: false,
      email: "",
    };
  }

  const data = snap.data() || {};
  const email = String(data.email || "").trim().toLowerCase();
  return {
    ok: true,
    bootstrapped: email === FINANCE_ADMIN_EMAIL,
    email: email === FINANCE_ADMIN_EMAIL ? email : "",
  };
});

exports.getGlobalAnalyticsSnapshot = publicOnCall("getGlobalAnalyticsSnapshot", async (request) => {
  assertFinanceAdmin(request);
  const botDifficulty = await getConfiguredBotDifficulty();
  const nowMs = Date.now();
  const presenceSnapshotsCutoffMs = nowMs - (PRESENCE_ANALYTICS_RECENT_SNAPSHOT_DAYS * 24 * 60 * 60 * 1000);
  const [
    clientsSnap,
    ambassadorsSnap,
    roomsSnap,
    ordersSnap,
    withdrawalsSnap,
    xchangesSnap,
    referralRewardsSnap,
    referralsSnap,
    channelSnap,
    threadsSnap,
    supportMessagesSnap,
    livePresence,
    presenceSnapshotsSnap,
    presenceDailySnap,
    presenceMonthlySnap,
    presenceHourSnap,
    presenceWeekdaySnap,
  ] = await Promise.all([
    db.collection(CLIENTS_COLLECTION).get(),
    db.collection(AMBASSADORS_COLLECTION).get(),
    db.collection(ROOMS_COLLECTION).get(),
    db.collectionGroup("orders").get(),
    db.collectionGroup("withdrawals").get(),
    db.collectionGroup("xchanges").get(),
    db.collectionGroup("referralRewards").get(),
    db.collectionGroup("referrals").get(),
    db.collection(CHAT_COLLECTION).get(),
    db.collection(SUPPORT_THREADS_COLLECTION).get(),
    db.collectionGroup(SUPPORT_MESSAGES_SUBCOLLECTION).get(),
    collectPresenceAnalyticsNow(nowMs),
    presenceSnapshotsCollection()
      .where("bucketMs", ">=", presenceSnapshotsCutoffMs)
      .orderBy("bucketMs", "asc")
      .get(),
    presenceDailyCollection()
      .orderBy("dayKey", "desc")
      .limit(PRESENCE_ANALYTICS_RECENT_DAYS_LIMIT)
      .get(),
    presenceMonthlyCollection()
      .orderBy("monthKey", "desc")
      .limit(PRESENCE_ANALYTICS_RECENT_MONTHS_LIMIT)
      .get(),
    presenceHourCollection()
      .orderBy("hourKey", "asc")
      .get(),
    presenceWeekdayCollection()
      .orderBy("weekdayKey", "asc")
      .get(),
  ]);

  const referrals = referralsSnap.docs.map(referralRecordForCallable);

  return {
    generatedAtMs: Date.now(),
    botDifficulty,
    clients: clientsSnap.docs.map(snapshotRecordForCallable),
    ambassadors: ambassadorsSnap.docs.map(snapshotRecordForCallable),
    rooms: roomsSnap.docs.map(snapshotRecordForCallable),
    orders: ordersSnap.docs.map(subcollectionRecordForCallable),
    withdrawals: withdrawalsSnap.docs.map(subcollectionRecordForCallable),
    xchanges: xchangesSnap.docs.map(subcollectionRecordForCallable),
    referralRewards: referralRewardsSnap.docs.map(subcollectionRecordForCallable),
    clientReferrals: referrals.filter((item) => item.ownerCollection === CLIENTS_COLLECTION),
    ambassadorReferrals: referrals.filter((item) => item.ownerCollection === AMBASSADORS_COLLECTION),
    channelMessages: channelSnap.docs.map(snapshotRecordForCallable),
    supportThreads: threadsSnap.docs.map(snapshotRecordForCallable),
    supportMessages: supportMessagesSnap.docs.map(supportMessageRecordForCallable),
    presenceAnalytics: {
      timezone: PRESENCE_ANALYTICS_TIMEZONE,
      live: livePresence,
      snapshots: presenceSnapshotsSnap.docs.map(snapshotRecordForCallable),
      daily: presenceDailySnap.docs.map(snapshotRecordForCallable).reverse(),
      monthly: presenceMonthlySnap.docs.map(snapshotRecordForCallable).reverse(),
      hourOfDay: presenceHourSnap.docs.map(snapshotRecordForCallable),
      weekday: presenceWeekdaySnap.docs.map(snapshotRecordForCallable),
    },
  };
});

exports.updateClientProfileSecure = publicOnCall("updateClientProfileSecure", async (request) => {
  const { uid, email } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};

  const name = sanitizeText(payload.name || "", 80);
  const phone = sanitizePhone(payload.phone || "", 40);
  const photoURL = sanitizePublicAsset(payload.photoURL || "", 400);
  const usernameInput = sanitizeUsername(payload.username || "", 24);
  const oneClickIdInput = sanitizeText(payload.oneClickId || "", 64).toUpperCase();
  const promoCode = normalizeCode(payload.promoCode || "");
  const referralSource = String(payload.referralSource || "").toLowerCase() === "link" ? "link" : "promo";
  const context = sanitizeAnalyticsContext(payload, request);
  const ref = walletRef(uid);
  const snap = await ref.get();
  const current = snap.exists ? (snap.data() || {}) : {};
  const isNewProfile = !snap.exists;
  let referralCode = normalizeCode(current.referralCode || "");

  if (!referralCode) {
    referralCode = await generateUniqueClientReferralCode(uid);
  }

  const profile = {
    uid,
    email: email || String(current.email || ""),
    name: name || sanitizeText(current.name || String(email || "").split("@")[0] || "Player", 80),
    phone: phone || sanitizePhone(current.phone || ""),
    photoURL: photoURL || sanitizePublicAsset(current.photoURL || ""),
    username: usernameInput || sanitizeUsername(current.username || "", 24),
    oneClickId: oneClickIdInput || sanitizeText(current.oneClickId || "", 64).toUpperCase(),
    referralCode,
    deviceId: context.deviceId || String(current.deviceId || ""),
    appVersion: context.appVersion || String(current.appVersion || ""),
    country: context.country || String(current.country || ""),
    browser: context.browser || String(current.browser || ""),
    ipHash: context.ipHash || String(current.ipHash || ""),
    utmSource: String(current.utmSource || "") || context.utmSource || "",
    utmCampaign: String(current.utmCampaign || "") || context.utmCampaign || "",
    landingPage: String(current.landingPage || "") || context.landingPage || "",
    creativeId: String(current.creativeId || "") || context.creativeId || "",
    lastLandingPage: context.landingPage || String(current.lastLandingPage || current.landingPage || ""),
    lastSeenAt: admin.firestore.FieldValue.serverTimestamp(),
    lastSeenAtMs: Date.now(),
    lastAuthAt: admin.firestore.FieldValue.serverTimestamp(),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };
  if (isNewProfile) {
    profile.createdAt = admin.firestore.FieldValue.serverTimestamp();
    profile.createdAtMs = Date.now();
    profile.doesBalance = safeInt(current.doesBalance);
    profile.exchangedGourdes = safeSignedInt(current.exchangedGourdes);
    profile.pendingPlayFromXchangeDoes = safeInt(current.pendingPlayFromXchangeDoes);
    profile.pendingPlayFromReferralDoes = safeInt(current.pendingPlayFromReferralDoes);
    profile.totalExchangedHtgEver = safeInt(current.totalExchangedHtgEver);
    profile.referralSignupsTotal = safeInt(current.referralSignupsTotal);
    profile.referralSignupsViaLink = safeInt(current.referralSignupsViaLink);
    profile.referralSignupsViaCode = safeInt(current.referralSignupsViaCode);
    profile.referralDepositsTotal = safeInt(current.referralDepositsTotal);
  } else {
    if (typeof current.referralSignupsTotal !== "number") profile.referralSignupsTotal = safeInt(current.referralSignupsTotal);
    if (typeof current.referralSignupsViaLink !== "number") profile.referralSignupsViaLink = safeInt(current.referralSignupsViaLink);
    if (typeof current.referralSignupsViaCode !== "number") profile.referralSignupsViaCode = safeInt(current.referralSignupsViaCode);
    if (typeof current.referralDepositsTotal !== "number") profile.referralDepositsTotal = safeInt(current.referralDepositsTotal);
  }

  await ref.set(profile, { merge: true });

  let referralBootstrap = { applied: false, reason: "no_candidate" };
  if (isNewProfile && promoCode) {
    referralBootstrap = await applyPromoAttribution({
      uid,
      email,
      promoCode,
      via: referralSource,
    });
  }

  const finalSnap = await ref.get();
  const finalProfile = finalSnap.exists ? (finalSnap.data() || {}) : {};
  const finalReferralCode = normalizeCode(finalProfile.referralCode || referralCode);

  return {
    ok: true,
    profile: {
      name: String(finalProfile.name || profile.name || ""),
      phone: sanitizePhone(finalProfile.phone || profile.phone || ""),
      photoURL: sanitizePublicAsset(finalProfile.photoURL || profile.photoURL || ""),
      username: sanitizeUsername(finalProfile.username || profile.username || "", 24),
      oneClickId: sanitizeText(finalProfile.oneClickId || profile.oneClickId || "", 64).toUpperCase(),
      referralCode: finalReferralCode,
      referralLink: buildUserReferralLink(finalReferralCode),
      referralSignupsTotal: safeInt(finalProfile.referralSignupsTotal),
      referralDepositsTotal: safeInt(finalProfile.referralDepositsTotal),
      referredByType: sanitizeText(finalProfile.referredByType || "", 20),
      referredByCode: normalizeCode(finalProfile.referredByCode || ""),
      updatedAt: new Date().toISOString(),
    },
    referralApplied: referralBootstrap.applied === true,
    referralReason: String(referralBootstrap.reason || ""),
  };
});

exports.createOrderSecure = publicOnCall("createOrderSecure", async (request) => {
  const { uid, email } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const methodId = sanitizeText(payload.methodId || "", 120);
  const amountHtg = safeInt(payload.amountHtg);
  const customerName = sanitizeText(payload.customerName || "", 120);
  const customerEmail = sanitizeEmail(payload.customerEmail || email || "", 160) || sanitizeEmail(email || "", 160);
  const customerPhone = sanitizePhone(payload.customerPhone || "", 40);
  const proofRef = sanitizeText(payload.proofRef || "", 180);

  if (!methodId || amountHtg < MIN_ORDER_HTG || !customerName || !proofRef) {
    throw new HttpsError("invalid-argument", "Commande invalide.");
  }

  const methodSnap = await db.collection("paymentMethods").doc(methodId).get();
  if (!methodSnap.exists) {
    throw new HttpsError("not-found", "Méthode introuvable.");
  }
  const publicMethod = sanitizePublicMethod(methodSnap);
  if (!publicMethod) {
    throw new HttpsError("failed-precondition", "Méthode indisponible.");
  }

  const settings = await getSettingsSnapshotData();
  const provisionalDepositsEnabled = isProvisionalFundingEnabled(settings);
  const orderRef = db.collection(CLIENTS_COLLECTION).doc(uid).collection("orders").doc();
  const nowIso = new Date().toISOString();
  const nowMs = Date.now();
  const clientRef = walletRef(uid);
  await db.runTransaction(async (tx) => {
    const [clientSnap, ordersSnap, withdrawalsSnap] = await Promise.all([
      tx.get(clientRef),
      tx.get(db.collection(CLIENTS_COLLECTION).doc(uid).collection("orders")),
      tx.get(db.collection(CLIENTS_COLLECTION).doc(uid).collection("withdrawals")),
    ]);
    const clientData = clientSnap.exists ? (clientSnap.data() || {}) : {};
    assertWalletNotFrozen(clientData);

    const orderData = {
      uid,
      clientId: uid,
      clientUid: uid,
      amount: amountHtg,
      methodId,
      methodName: publicMethod.name,
      methodDetails: {
        name: publicMethod.name,
        accountName: publicMethod.accountName,
        phoneNumber: publicMethod.phoneNumber,
      },
      status: "pending",
      uniqueCode: `VLX-${crypto.randomBytes(4).toString("hex").toUpperCase()}-${Date.now().toString(36).toUpperCase()}`,
      proofRef,
      customerName,
      customerEmail,
      customerPhone,
      extractedText: sanitizeText(payload.extractedText || "", MAX_PUBLIC_TEXT_LENGTH),
      extractedTextStatus: ["pending", "success", "empty", "failed"].includes(String(payload.extractedTextStatus || ""))
        ? String(payload.extractedTextStatus)
        : "pending",
      createdAtMs: nowMs,
      createdAt: nowIso,
      expiresAt: new Date(Date.now() + (settings.verificationHours * 60 * 60 * 1000)).toISOString(),
      updatedAt: nowIso,
      updatedAtMs: nowMs,
      deviceId: sanitizeText(clientData.deviceId || "", 120),
      appVersion: sanitizeText(clientData.appVersion || "", 48),
      country: sanitizeText(clientData.country || "", 48),
      browser: sanitizeText(clientData.browser || "", 120),
      ipHash: sanitizeText(clientData.ipHash || "", 64),
      utmSource: sanitizeText(clientData.utmSource || "", 80),
      utmCampaign: sanitizeText(clientData.utmCampaign || "", 120),
      landingPage: sanitizeText(clientData.landingPage || "", 240),
      creativeId: sanitizeText(clientData.creativeId || "", 120),
    };

    if (provisionalDepositsEnabled) {
      Object.assign(orderData, {
        fundingVersion: PROVISIONAL_FUNDING_VERSION,
        creditMode: PROVISIONAL_CREDIT_MODE,
        resolutionStatus: "pending",
        approvedAmountHtg: 0,
        provisionalHtgRemaining: amountHtg,
        provisionalHtgConverted: 0,
        provisionalDoesRemaining: 0,
        provisionalDoesPlayed: 0,
        provisionalGainDoes: 0,
        rejectedReason: "",
        resolvedAtMs: 0,
        fundingSettledAtMs: 0,
        creditedProvisionallyAtMs: nowMs,
      });
    }

    const nextWallet = {
      uid,
      email,
      name: customerName || sanitizeText(clientData.name || "", 80) || sanitizeText(String(email || "").split("@")[0], 80) || "Player",
      phone: customerPhone || sanitizePhone(clientData.phone || ""),
      lastSeenAt: admin.firestore.FieldValue.serverTimestamp(),
      lastSeenAtMs: nowMs,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      ...(clientSnap.exists ? {} : { createdAt: admin.firestore.FieldValue.serverTimestamp() }),
    };

    if (provisionalDepositsEnabled) {
      const fundingSnapshot = buildWalletFundingSnapshot({
        orders: [
          ...ordersSnap.docs.map((item) => item.data() || {}),
          orderData,
        ],
        withdrawals: withdrawalsSnap.docs.map((item) => item.data() || {}),
        walletData: clientData,
      });
      Object.assign(nextWallet, buildFundingWalletPatch(fundingSnapshot));
    }

    tx.set(clientRef, nextWallet, { merge: true });
    tx.set(orderRef, orderData, { merge: true });
  });

  return {
    ok: true,
    orderId: orderRef.id,
    status: "pending",
    creditedProvisionally: provisionalDepositsEnabled,
    message: provisionalDepositsEnabled
      ? "Ton dépôt est en cours d'examen. Tu peux jouer avec ce solde, mais tu ne peux pas le retirer tant qu'il n'est pas validé."
      : "Votre demande est en cours de vérification.",
  };
});

exports.notifyDashboardClientCreated = onDocumentCreated(`${CLIENTS_COLLECTION}/{clientId}`, async (event) => {
  const snapshot = event.data;
  if (!snapshot?.exists) return;
  const clientId = String(event.params?.clientId || snapshot.id || "").trim();
  const data = snapshot.data() || {};
  const createdAtMs = safeInt(data.createdAtMs);
  const createdAtValue = toMillis(data.createdAt);
  if (!createdAtMs && !createdAtValue) {
    return;
  }

  const playerLabel = sanitizeText(
    data.name || data.username || String(data.email || "").split("@")[0] || `Client ${clientId.slice(0, 6)}`,
    80
  );
  const sourceCreatedAt = String(data.createdAt || "").trim()
    || (createdAtMs ? new Date(createdAtMs).toISOString() : "");

  console.info("[DASHBOARD_PUSH] trigger client créé", {
    clientId,
    eventId: String(event.id || ""),
    sourceCreatedAt,
  });

  await sendDashboardPushToAdmins({
    type: "client_signup",
    title: "Nouveau client inscrit",
    body: `${playerLabel} vient de créer un compte.`,
    url: DASHBOARD_DEFAULT_NOTIFICATION_URL,
    entityId: clientId,
    sourceCreatedAt,
    tag: `dashboard_client_${clientId}`,
  });
});

exports.notifyDashboardOrderCreated = onDocumentCreated(`${CLIENTS_COLLECTION}/{clientId}/orders/{orderId}`, async (event) => {
  const snapshot = event.data;
  if (!snapshot?.exists) return;
  const orderId = String(event.params?.orderId || snapshot.id || "").trim();
  const data = snapshot.data() || {};
  const amount = safeInt(data.amount || data.amountHtg);
  const methodName = sanitizeText(data.methodName || data.methodId || "", 80);
  const labelParts = [];
  if (amount > 0) labelParts.push(`${amount} HTG`);
  if (methodName) labelParts.push(methodName);
  const sourceCreatedAt = String(data.createdAt || "").trim()
    || (safeInt(data.createdAtMs) ? new Date(safeInt(data.createdAtMs)).toISOString() : "");

  console.info("[DASHBOARD_PUSH] trigger commande créée", {
    orderId,
    clientId: String(event.params?.clientId || "").trim(),
    eventId: String(event.id || ""),
    sourceCreatedAt,
    amount,
    methodName,
  });

  await sendDashboardPushToAdmins({
    type: "order_created",
    title: "Nouvelle commande",
    body: labelParts.length
      ? `Commande ${orderId} reçue (${labelParts.join(" • ")}).`
      : `Commande ${orderId} reçue.`,
    url: DASHBOARD_DEFAULT_NOTIFICATION_URL,
    entityId: orderId,
    sourceCreatedAt,
    tag: `dashboard_order_${orderId}`,
  });
});

exports.createWithdrawalSecure = publicOnCall("createWithdrawalSecure", async (request) => {
  const { uid, email } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const requestedAmount = safeInt(payload.requestedAmount ?? payload.amountHtg ?? payload.amountDoes);
  const destinationType = sanitizeText(payload.destinationType || payload.methodId || "", 80);
  const destinationValue = sanitizeText(payload.destinationValue || payload.phone || "", 160);
  const customerName = sanitizeText(payload.customerName || "", 120);
  const customerPhone = sanitizePhone(payload.customerPhone || payload.phone || "", 40);

  if (!destinationType || !destinationValue || requestedAmount < MIN_WITHDRAWAL_HTG || requestedAmount > MAX_WITHDRAWAL_HTG) {
    console.warn("[WITHDRAWAL_DEBUG] invalid-argument", JSON.stringify({
      uid,
      requestedAmount,
      destinationType,
      hasDestinationValue: !!destinationValue,
      hasCustomerPhone: !!customerPhone,
    }));
    throw new HttpsError("invalid-argument", "Retrait invalide.");
  }

  const [ordersSnap, withdrawalsSnap, clientSnap, xchangesSnap] = await Promise.all([
    db.collection(CLIENTS_COLLECTION).doc(uid).collection("orders").get(),
    db.collection(CLIENTS_COLLECTION).doc(uid).collection("withdrawals").get(),
    db.collection(CLIENTS_COLLECTION).doc(uid).get(),
    walletHistoryRef(uid).get(),
  ]);

  const clientData = clientSnap.exists ? (clientSnap.data() || {}) : {};
  assertWalletNotFrozen(clientData);
  const walletSummary = buildWalletFundingSnapshot({
    orders: ordersSnap.docs.map((item) => item.data() || {}),
    withdrawals: withdrawalsSnap.docs.map((item) => item.data() || {}),
    walletData: clientData,
    exchangeHistory: xchangesSnap.docs.map((item) => item.data() || {}),
  });
  const available = walletSummary.withdrawableHtg;

  console.log("[WITHDRAWAL_DEBUG] summary", JSON.stringify({
    uid,
    requestedAmount,
    available,
    destinationType,
    ordersCount: ordersSnap.size,
    withdrawalsCount: withdrawalsSnap.size,
    exchangedGourdesRaw: clientData.exchangedGourdes,
    walletSummary,
  }));

  if (requestedAmount > available) {
    console.warn("[WITHDRAWAL_DEBUG] rejected-insufficient", JSON.stringify({
      uid,
      requestedAmount,
      available,
      walletSummary,
    }));
    throw new HttpsError("failed-precondition", "Montant supérieur au solde disponible.");
  }

  const ref = db.collection(CLIENTS_COLLECTION).doc(uid).collection("withdrawals").doc();
  const nowIso = new Date().toISOString();
  const withdrawalPayload = {
    uid,
    clientId: uid,
    clientUid: uid,
    status: "pending",
    requestedAmount,
    amount: requestedAmount,
    methodId: destinationType,
    methodName: destinationType,
    destinationType,
    destinationValue,
    customerName,
    customerEmail: sanitizeEmail(email || "", 160),
    customerPhone,
    createdAt: nowIso,
    updatedAt: nowIso,
  };
  await ref.set(withdrawalPayload, { merge: true });

  const nextFundingSnapshot = buildWalletFundingSnapshot({
    orders: ordersSnap.docs.map((item) => item.data() || {}),
    withdrawals: [
      ...withdrawalsSnap.docs.map((item) => item.data() || {}),
      withdrawalPayload,
    ],
    walletData: clientData,
    exchangeHistory: xchangesSnap.docs.map((item) => item.data() || {}),
  });

  await walletRef(uid).set({
    uid,
    email,
    name: customerName || sanitizeText(String(email || "").split("@")[0], 80) || "Player",
    phone: customerPhone,
    ...buildFundingWalletPatch(nextFundingSnapshot),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  }, { merge: true });

  return {
    ok: true,
    withdrawalId: ref.id,
    status: "pending",
  };
});

exports.orderClientActionSecure = publicOnCall("orderClientActionSecure", async (request) => {
  const { uid } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const kind = String(payload.kind || "").trim();
  const id = sanitizeText(payload.id || "", 160);
  const action = String(payload.action || "").trim();

  if (!id || (kind !== "order" && kind !== "withdrawal") || (action !== "hide" && action !== "review")) {
    throw new HttpsError("invalid-argument", "Action client invalide.");
  }

  const subcollection = kind === "withdrawal" ? "withdrawals" : "orders";
  const ref = db.collection(CLIENTS_COLLECTION).doc(uid).collection(subcollection).doc(id);
  const updates = {
    updatedAt: new Date().toISOString(),
  };

  if (action === "hide") {
    updates.userHiddenByClient = true;
    updates.userHiddenAt = new Date().toISOString();
  } else {
    updates.status = "review";
    updates.reviewRequestedByClient = true;
    updates.reviewRequestedAt = new Date().toISOString();
    updates.userHiddenByClient = false;
  }

  await ref.set(updates, { merge: true });
  return { ok: true };
});

exports.getDepositFundingStatusSecure = publicOnCall("getDepositFundingStatusSecure", async (request) => {
  const { uid } = assertAuth(request);
  const [ordersSnap, withdrawalsSnap, walletSnap, xchangesSnap] = await Promise.all([
    db.collection(CLIENTS_COLLECTION).doc(uid).collection("orders").get(),
    db.collection(CLIENTS_COLLECTION).doc(uid).collection("withdrawals").get(),
    walletRef(uid).get(),
    walletHistoryRef(uid).get(),
  ]);
  const walletData = walletSnap.exists ? (walletSnap.data() || {}) : {};
  const fundingSnapshot = buildWalletFundingSnapshot({
    orders: ordersSnap.docs.map((item) => item.data() || {}),
    withdrawals: withdrawalsSnap.docs.map((item) => item.data() || {}),
    walletData,
    exchangeHistory: xchangesSnap.docs.map((item) => item.data() || {}),
  });

  return {
    ...fundingSnapshot,
    accountFrozen: walletData.accountFrozen === true,
    freezeReason: String(walletData.freezeReason || ""),
    rejectedDepositStrikeCount: safeInt(walletData.rejectedDepositStrikeCount),
    pendingOrders: ordersSnap.docs
      .map((item) => ({ id: item.id, ...(item.data() || {}) }))
      .filter((item) => getOrderResolutionStatus(item) === "pending" && isFundingV2Order(item))
      .map((item) => ({
        id: item.id,
        amountHtg: computeOrderAmount(item),
        provisionalHtgRemaining: safeInt(item.provisionalHtgRemaining),
        provisionalDoesRemaining: safeInt(item.provisionalDoesRemaining),
        provisionalGainDoes: safeInt(item.provisionalGainDoes),
        createdAtMs: safeInt(item.createdAtMs),
        status: String(item.status || "pending"),
      })),
  };
});

exports.resolveDepositReviewSecure = publicOnCall("resolveDepositReviewSecure", async (request) => {
  assertFinanceAdmin(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const orderId = sanitizeText(payload.orderId || "", 160);
  const clientId = sanitizeText(payload.clientId || payload.uid || "", 160);
  const decision = String(payload.decision || "").trim().toLowerCase();
  const reason = sanitizeText(payload.reason || "", 240);

  if (!orderId || (decision !== "approve" && decision !== "reject")) {
    throw new HttpsError("invalid-argument", "Payload de validation invalide.");
  }

  let orderDoc = null;
  if (clientId) {
    const directRef = db.collection(CLIENTS_COLLECTION).doc(clientId).collection("orders").doc(orderId);
    const directSnap = await directRef.get();
    if (directSnap.exists) orderDoc = directSnap;
  }
  if (!orderDoc) {
    const groupSnap = await db.collectionGroup("orders")
      .where(admin.firestore.FieldPath.documentId(), "==", orderId)
      .limit(1)
      .get();
    if (!groupSnap.empty) {
      orderDoc = groupSnap.docs[0];
    }
  }
  if (!orderDoc) {
    throw new HttpsError("not-found", "Dépôt introuvable.");
  }

  const ownerRef = orderDoc.ref.parent.parent;
  const ownerUid = String(ownerRef?.id || "").trim();
  if (!ownerUid) {
    throw new HttpsError("failed-precondition", "Compte dépôt introuvable.");
  }

  const result = await db.runTransaction(async (tx) => {
    const [orderSnap, walletSnap, ordersSnap, withdrawalsSnap, xchangesSnap] = await Promise.all([
      tx.get(orderDoc.ref),
      tx.get(walletRef(ownerUid)),
      tx.get(db.collection(CLIENTS_COLLECTION).doc(ownerUid).collection("orders")),
      tx.get(db.collection(CLIENTS_COLLECTION).doc(ownerUid).collection("withdrawals")),
      tx.get(walletHistoryRef(ownerUid)),
    ]);

    if (!orderSnap.exists) {
      throw new HttpsError("not-found", "Dépôt introuvable.");
    }

    const orderData = orderSnap.data() || {};
    const walletData = walletSnap.exists ? (walletSnap.data() || {}) : {};
    const resolutionStatus = getOrderResolutionStatus(orderData);
    const fundingNeedsSettlement = isFundingV2Order(orderData)
      && safeInt(orderData.fundingSettledAtMs) <= 0;
    const nowIso = new Date().toISOString();
    const nowMs = Date.now();
    const orderAmountHtg = computeOrderAmount(orderData);

    if (resolutionStatus === decision && !fundingNeedsSettlement) {
      const fundingSnapshot = buildWalletFundingSnapshot({
        orders: ordersSnap.docs.map((item) => item.data() || {}),
        withdrawals: withdrawalsSnap.docs.map((item) => item.data() || {}),
        walletData,
        exchangeHistory: xchangesSnap.docs.map((item) => item.data() || {}),
      });
      return {
        ok: true,
        orderId,
        uid: ownerUid,
        status: String(orderData.status || resolutionStatus),
        resolutionStatus,
        ...fundingSnapshot,
        accountFrozen: walletData.accountFrozen === true,
        rejectedDepositStrikeCount: safeInt(walletData.rejectedDepositStrikeCount),
      };
    }

    let nextOrder = {
      ...orderData,
      updatedAt: nowIso,
      updatedAtMs: nowMs,
      resolvedAtMs: nowMs,
    };
    let nextWallet = {
      ...walletData,
      uid: ownerUid,
      email: String(walletData.email || orderData.customerEmail || ""),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    };

    const beforeDoes = safeInt(walletData.doesBalance);
    const beforeProvisionalDoes = safeInt(walletData.doesProvisionalBalance);
    const beforeApprovedDoes = safeInt(
      typeof walletData.doesApprovedBalance === "number"
        ? walletData.doesApprovedBalance
        : Math.max(0, beforeDoes - beforeProvisionalDoes)
    );
    const beforePendingFromXchange = safeInt(walletData.pendingPlayFromXchangeDoes);
    const beforePendingFromReferral = safeInt(walletData.pendingPlayFromReferralDoes);
    const beforePendingPlayTotal = beforePendingFromXchange + beforePendingFromReferral;
    const beforeExchangeableDoes = safeInt(
      typeof walletData.exchangeableDoesAvailable === "number"
        ? Math.min(walletData.exchangeableDoesAvailable, beforeApprovedDoes)
        : (beforePendingPlayTotal <= 0 ? beforeApprovedDoes : 0)
    );
    const otherPendingOrdersDoes = ordersSnap.docs.reduce((sum, item) => {
      if (item.id === orderSnap.id) return sum;
      return sum + getOrderPendingProvisionalDoesTotal(item.data() || {});
    }, 0);
    const orderPendingCapitalDoes = getOrderProvisionalCapitalDoesBalance(orderData);
    const orderPendingGainDoes = safeInt(orderData.provisionalGainDoes);
    const orderPendingTotalDoes = orderPendingCapitalDoes + orderPendingGainDoes;
    const walletScopedPendingDoes = Math.max(0, beforeProvisionalDoes - otherPendingOrdersDoes);
    const settledPendingTotalDoes = Math.min(orderPendingTotalDoes, walletScopedPendingDoes);
    const settledGainDoes = Math.min(orderPendingGainDoes, settledPendingTotalDoes);
    const settledCapitalDoes = Math.min(
      orderPendingCapitalDoes,
      Math.max(0, settledPendingTotalDoes - settledGainDoes)
    );

    console.log("[BALANCE_DEBUG][FUNCTIONS][resolveDepositReview] reconcile", JSON.stringify({
      uid: ownerUid,
      orderId,
      decision,
      beforeProvisionalDoes,
      otherPendingOrdersDoes,
      orderPendingCapitalDoes,
      orderPendingGainDoes,
      orderPendingTotalDoes,
      walletScopedPendingDoes,
      settledCapitalDoes,
      settledGainDoes,
      settledPendingTotalDoes,
    }));

    if (decision === "approve") {
      const promoteCapitalDoes = settledCapitalDoes;
      const promoteGainDoes = settledGainDoes;
      const promoteDoes = promoteCapitalDoes + promoteGainDoes;
      const totalConvertedDoes = safeInt(orderData.provisionalHtgConverted) * RATE_HTG_TO_DOES;
      const unlockedFromPlayedDoes = Math.max(0, totalConvertedDoes - promoteCapitalDoes);
      nextOrder = {
        ...nextOrder,
        status: "approved",
        resolutionStatus: "approved",
        approvedAmountHtg: orderAmountHtg,
        rejectedReason: "",
        provisionalDoesRemaining: promoteCapitalDoes,
        provisionalGainDoes: promoteGainDoes,
        fundingSettledAtMs: nowMs,
      };
      nextWallet.doesApprovedBalance = beforeApprovedDoes + promoteDoes;
      nextWallet.doesProvisionalBalance = Math.max(0, beforeProvisionalDoes - promoteDoes);
      nextWallet.doesBalance = safeInt(nextWallet.doesApprovedBalance) + safeInt(nextWallet.doesProvisionalBalance);
      nextWallet.exchangedGourdes = safeSignedInt(walletData.exchangedGourdes) + safeInt(orderData.provisionalHtgConverted);
      nextWallet.totalExchangedHtgEver = safeInt(walletData.totalExchangedHtgEver) + safeInt(orderData.provisionalHtgConverted);
      nextWallet.pendingPlayFromXchangeDoes = beforePendingFromXchange + promoteCapitalDoes;
      nextWallet.pendingPlayFromReferralDoes = beforePendingFromReferral;
      nextWallet.exchangeableDoesAvailable = beforeExchangeableDoes + unlockedFromPlayedDoes;
    } else {
      const removeDoes = settledPendingTotalDoes;
      const nextStrikeCount = safeInt(walletData.rejectedDepositStrikeCount) + (resolutionStatus === "rejected" ? 0 : 1);
      const shouldFreeze = nextStrikeCount >= ACCOUNT_FREEZE_REJECT_THRESHOLD;
      nextOrder = {
        ...nextOrder,
        status: "rejected",
        resolutionStatus: "rejected",
        approvedAmountHtg: 0,
        rejectedReason: reason || "Dépôt refusé",
        provisionalDoesRemaining: settledCapitalDoes,
        provisionalGainDoes: settledGainDoes,
        fundingSettledAtMs: nowMs,
      };
      nextWallet.doesApprovedBalance = beforeApprovedDoes;
      nextWallet.doesProvisionalBalance = Math.max(0, beforeProvisionalDoes - removeDoes);
      nextWallet.doesBalance = safeInt(nextWallet.doesApprovedBalance) + safeInt(nextWallet.doesProvisionalBalance);
      nextWallet.rejectedDepositStrikeCount = nextStrikeCount;
      nextWallet.accountFrozen = shouldFreeze;
      nextWallet.freezeReason = shouldFreeze ? "3_rejected_deposits" : String(walletData.freezeReason || "");
      nextWallet.frozenAtMs = shouldFreeze ? nowMs : safeInt(walletData.frozenAtMs);
      nextWallet.pendingPlayFromXchangeDoes = beforePendingFromXchange;
      nextWallet.pendingPlayFromReferralDoes = beforePendingFromReferral;
      nextWallet.exchangeableDoesAvailable = beforeExchangeableDoes;
    }

    if ((safeInt(nextWallet.pendingPlayFromXchangeDoes) + safeInt(nextWallet.pendingPlayFromReferralDoes)) <= 0) {
      nextWallet.exchangeableDoesAvailable = safeInt(nextWallet.doesApprovedBalance);
    } else {
      nextWallet.exchangeableDoesAvailable = Math.min(
        safeInt(nextWallet.doesApprovedBalance),
        safeInt(nextWallet.exchangeableDoesAvailable)
      );
    }

    const nextOrders = ordersSnap.docs.map((item) => (
      item.id === orderSnap.id ? nextOrder : (item.data() || {})
    ));
    const fundingSnapshot = buildWalletFundingSnapshot({
      orders: nextOrders,
      withdrawals: withdrawalsSnap.docs.map((item) => item.data() || {}),
      walletData: nextWallet,
      exchangeHistory: xchangesSnap.docs.map((item) => item.data() || {}),
    });

    tx.set(orderSnap.ref, nextOrder, { merge: true });
    tx.set(walletRef(ownerUid), {
      ...buildFundingWalletPatch(fundingSnapshot),
      doesApprovedBalance: safeInt(nextWallet.doesApprovedBalance),
      doesProvisionalBalance: safeInt(nextWallet.doesProvisionalBalance),
      doesBalance: safeInt(nextWallet.doesBalance),
      exchangeableDoesAvailable: safeInt(nextWallet.exchangeableDoesAvailable),
      exchangedGourdes: safeSignedInt(nextWallet.exchangedGourdes),
      pendingPlayFromXchangeDoes: safeInt(nextWallet.pendingPlayFromXchangeDoes),
      pendingPlayFromReferralDoes: safeInt(nextWallet.pendingPlayFromReferralDoes),
      totalExchangedHtgEver: safeInt(nextWallet.totalExchangedHtgEver),
      rejectedDepositStrikeCount: safeInt(nextWallet.rejectedDepositStrikeCount),
      accountFrozen: nextWallet.accountFrozen === true,
      freezeReason: String(nextWallet.freezeReason || ""),
      frozenAtMs: safeInt(nextWallet.frozenAtMs),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });

    return {
      ok: true,
      orderId,
      uid: ownerUid,
      status: nextOrder.status,
      resolutionStatus: nextOrder.resolutionStatus,
      ...fundingSnapshot,
      accountFrozen: nextWallet.accountFrozen === true,
      rejectedDepositStrikeCount: safeInt(nextWallet.rejectedDepositStrikeCount),
    };
  });

  return result;
});

exports.unfreezeClientAccountSecure = publicOnCall("unfreezeClientAccountSecure", async (request) => {
  assertFinanceAdmin(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const uid = sanitizeText(payload.uid || "", 160);
  const reason = sanitizeText(payload.reason || "", 240);

  if (!uid) {
    throw new HttpsError("invalid-argument", "uid requis.");
  }

  await walletRef(uid).set({
    accountFrozen: false,
    freezeReason: "",
    rejectedDepositStrikeCount: 0,
    unfrozenAtMs: Date.now(),
    unfreezeReason: reason || "",
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  }, { merge: true });

  return {
    ok: true,
    uid,
    accountFrozen: false,
    rejectedDepositStrikeCount: 0,
  };
});

exports.markChatSeenSecure = publicOnCall("markChatSeenSecure", async (request) => {
  const { uid, email } = assertAuth(request);
  await walletRef(uid).set({
    uid,
    email,
    chatLastSeenAt: admin.firestore.FieldValue.serverTimestamp(),
    lastSeenAt: admin.firestore.FieldValue.serverTimestamp(),
    lastSeenAtMs: Date.now(),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  }, { merge: true });
  return { ok: true };
});

exports.ensureSupportThreadSecure = publicOnCall("ensureSupportThreadSecure", async (request) => {
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const context = await resolveSupportThreadContext(request, payload, { allowCreate: true });
  return {
    ok: true,
    threadId: context.threadId,
    guestToken: context.guestToken || "",
    thread: supportThreadRecordForCallable(context.threadSnap),
  };
});

exports.getSupportMessagesSecure = publicOnCall("getSupportMessagesSecure", async (request) => {
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const context = await resolveSupportThreadContext(request, payload, { allowCreate: true });
  const limitValue = Math.max(
    1,
    Math.min(
      DISCUSSION_MESSAGES_FETCH_LIMIT,
      safeInt(payload.limit || DISCUSSION_MESSAGES_FETCH_LIMIT) || DISCUSSION_MESSAGES_FETCH_LIMIT
    )
  );
  const [messagesSnap, pinnedMessagesSnap] = await Promise.all([
    context.threadRef
      .collection(SUPPORT_MESSAGES_SUBCOLLECTION)
      .orderBy("createdAtMs", "desc")
      .limit(limitValue)
      .get(),
    context.threadRef
      .collection(SUPPORT_MESSAGES_SUBCOLLECTION)
      .where("pinned", "==", true)
      .get(),
  ]);

  const messages = mergePinnedDiscussionRecords([
    ...messagesSnap.docs.map((item) => supportMessageRecordForCallable(item)),
    ...pinnedMessagesSnap.docs.map((item) => supportMessageRecordForCallable(item)),
  ]);

  return {
    ok: true,
    threadId: context.threadId,
    guestToken: context.guestToken || "",
    thread: supportThreadRecordForCallable(context.threadSnap),
    messages,
  };
});

exports.createSupportMessageSecure = publicOnCall("createSupportMessageSecure", async (request) => {
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const context = await resolveSupportThreadContext(request, payload, { allowCreate: true });
  const text = sanitizeText(payload.text || "", MAX_PUBLIC_TEXT_LENGTH);
  const media = sanitizeSupportMediaPayload(payload.media || {});
  if (media.mediaPath && !supportMediaMatchesThread(media.mediaPath, context.threadId)) {
    throw new HttpsError("invalid-argument", "Le média ne correspond pas à ce fil.");
  }
  if (!text && !media.mediaUrl) {
    throw new HttpsError("invalid-argument", "Le message est vide.");
  }

  const record = buildSupportMessageRecord(context.actor, text, media, {
    scope: "support",
    threadId: context.threadId,
  });

  const ref = await context.threadRef
    .collection(SUPPORT_MESSAGES_SUBCOLLECTION)
    .add(record);

  const threadData = context.threadSnap.exists ? (context.threadSnap.data() || {}) : {};
  await context.threadRef.set({
    lastMessageText: messagePreviewFromRecord(record),
    lastMessageAt: admin.firestore.FieldValue.serverTimestamp(),
    lastMessageAtMs: record.createdAtMs,
    lastSenderRole: record.senderRole,
    status: "open",
    unreadForAgent: true,
    unreadForUser: false,
    resolvedAt: null,
    resolvedAtMs: 0,
    resolutionTag: "",
    participantName: record.displayName,
    participantEmail: record.email,
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    firstAgentReplyAt: threadData.firstAgentReplyAt || null,
    firstAgentReplyAtMs: safeInt(threadData.firstAgentReplyAtMs),
  }, { merge: true });

  return {
    ok: true,
    threadId: context.threadId,
    guestToken: context.guestToken || "",
    message: {
      id: ref.id,
      threadId: context.threadId,
      text: record.text,
      mediaType: record.mediaType,
      mediaUrl: record.mediaUrl,
      mediaPath: record.mediaPath,
      fileName: record.fileName,
      senderRole: record.senderRole,
      senderType: record.senderType,
      senderKey: record.senderKey,
      uid: record.uid,
      guestId: record.guestId,
      email: record.email,
      displayName: record.displayName,
      createdAtMs: record.createdAtMs,
      expiresAtMs: record.expiresAtMs,
    },
  };
});

exports.markSupportThreadSeenSecure = publicOnCall("markSupportThreadSeenSecure", async (request) => {
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const context = await resolveSupportThreadContext(request, payload, { allowCreate: true });
  await context.threadRef.set({
    unreadForUser: false,
    participantSeenAt: admin.firestore.FieldValue.serverTimestamp(),
    participantSeenAtMs: Date.now(),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  }, { merge: true });

  return {
    ok: true,
    threadId: context.threadId,
    guestToken: context.guestToken || "",
  };
});

exports.adminCheck = publicOnCall("adminCheck", async (request) => {
  const { uid, email } = assertFinanceAdmin(request);
  return {
    ok: true,
    uid,
    email,
    botDifficulty: await getConfiguredBotDifficulty(),
  };
});

exports.setBotDifficulty = publicOnCall("setBotDifficulty", async (request) => {
  assertFinanceAdmin(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const botDifficulty = normalizeBotDifficulty(payload.botDifficulty);

  await adminBootstrapRef().set({
    email: FINANCE_ADMIN_EMAIL,
    botDifficulty,
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  }, { merge: true });

  return {
    ok: true,
    botDifficulty,
  };
});

exports.getTournamentLeaderboard = publicOnCall("getTournamentLeaderboard", async (request) => {
  assertAuth(request);
  const nowMs = Date.now();
  const windowStart = nowMs - (60 * 60 * 1000);
  const snap = await db.collection(ROOMS_COLLECTION)
    .where("endedAtMs", ">=", windowStart)
    .limit(2000)
    .get();

  const counts = new Map();
  snap.forEach((docSnap) => {
    const data = docSnap.data() || {};
    const status = String(data.status || "").toLowerCase();
    if (status !== "ended") return;
    const winnerUid = String(data.winnerUid || "").trim();
    if (!winnerUid) return;
    const wins = safeInt(counts.get(winnerUid) || 0) + 1;
    counts.set(winnerUid, wins);
  });

  const leaders = Array.from(counts.entries())
    .map(([uid, wins]) => ({ uid, wins: safeInt(wins) }))
    .sort((a, b) => b.wins - a.wins || a.uid.localeCompare(b.uid))
    .slice(0, 200);

  return {
    generatedAt: nowMs,
    leaders,
  };
});

function buildBotId(seed, idx) {
  const raw = Math.abs(seed + idx * 17).toString(36).toUpperCase();
  return `BOT-${raw.slice(-6)}`;
}

function generateInitialBots(sessionSeed, userWins) {
  const botCount = seededInt(sessionSeed + 11, USER_TOURNAMENT_MIN_BOTS, USER_TOURNAMENT_MAX_BOTS);
  const bots = [];
  for (let i = 0; i < botCount; i += 1) {
    const id = buildBotId(sessionSeed, i + 1);
    // Un tournoi doit toujours commencer avec un classement vierge.
    const wins = 0;
    bots.push({
      id,
      wins,
      seed: sessionSeed + i,
      isChampion: false,
      lastUpdatedMs: sessionSeed,
      presenceStatus: "online",
      presenceUntilMs: sessionSeed + randomInt(15 * 1000, 35 * 1000),
    });
  }
  const champIdx = seededInt(sessionSeed + 999, 0, bots.length - 1);
  bots[champIdx].isChampion = true;
  return { bots, championId: bots[champIdx].id };
}

async function countUserWinsInWindow(uid, startMs, endMs) {
  // Single where clause to avoid composite index requirement; filter window in code
  const snap = await db.collection(ROOMS_COLLECTION)
    .where("winnerUid", "==", uid)
    .limit(2000)
    .get();
  let wins = 0;
  snap.forEach((docSnap) => {
    const data = docSnap.data() || {};
    const status = String(data.status || "").toLowerCase();
    const endedAt = toMillis(data.endedAtMs);
    if (status !== "ended") return;
    if (endedAt < startMs || endedAt > endMs) return;
    wins += 1;
  });
  return safeInt(wins);
}

function capBotWins(bot, userWins, timeLeftMs, observerMode = false) {
  const beforeFinal = timeLeftMs > (2 * 60 * 1000);
  const cap = observerMode
    ? (beforeFinal ? Math.max(userWins + 3, 4) : Math.max(userWins + 6, 7))
    : (beforeFinal ? userWins + 1 : userWins + 3);
  bot.wins = clamp(bot.wins, 0, cap);
}

function normalizeTournamentPresenceStatus(status = "") {
  return String(status || "").trim().toLowerCase() === "playing" ? "playing" : "online";
}

function setBotPresence(bot, status, nowMs, minDurationMs = 12000, maxDurationMs = 26000) {
  bot.presenceStatus = normalizeTournamentPresenceStatus(status);
  bot.presenceUntilMs = nowMs + randomInt(minDurationMs, maxDurationMs);
}

function refreshBotPresence(bots, nowMs) {
  bots.forEach((bot) => {
    const currentStatus = normalizeTournamentPresenceStatus(bot.presenceStatus);
    const currentUntil = toMillis(bot.presenceUntilMs);
    if (currentUntil > nowMs) {
      bot.presenceStatus = currentStatus;
      return;
    }
    const playingChance = bot.isChampion ? 42 : 28;
    const nextStatus = randomInt(1, 100) <= playingChance ? "playing" : "online";
    if (nextStatus === "playing") {
      setBotPresence(bot, "playing", nowMs, 12000, 24000);
    } else {
      setBotPresence(bot, "online", nowMs, 18000, 42000);
    }
  });
}

async function resolveTournamentGameplayActivityMs(uid, fallbackMs = 0) {
  const currentKnown = safeInt(fallbackMs);
  const activeRoom = await findActiveRoomForUser(uid);
  if (!activeRoom?.roomId) return currentKnown;

  const roomSnap = await db.collection(ROOMS_COLLECTION).doc(String(activeRoom.roomId)).get();
  if (!roomSnap.exists) return currentKnown;

  const room = roomSnap.data() || {};
  const roomPresence = room.roomPresenceMs && typeof room.roomPresenceMs === "object"
    ? room.roomPresenceMs
    : {};
  const directPresenceMs = safeInt(roomPresence[uid]);
  const updatedAtMs = toMillis(room.updatedAt);
  const startedAtMs = toMillis(room.startedAtMs);

  return Math.max(currentKnown, directPresenceMs, updatedAtMs, startedAtMs);
}

function spreadBotsOnUserGain(bots, userWins, nowMs) {
  const pool = bots.slice();
  const targetCount = Math.max(1, Math.min(pool.length, randomInt(1, Math.max(1, Math.floor(pool.length / 3)))));
  const picked = new Set();

  while (picked.size < targetCount && picked.size < pool.length) {
    const nextBot = pool[randomInt(0, pool.length - 1)];
    if (nextBot) picked.add(nextBot.id);
  }

  bots.forEach((bot) => {
    if (!picked.has(bot.id)) {
      if (normalizeTournamentPresenceStatus(bot.presenceStatus) !== "playing") {
        setBotPresence(bot, "online", nowMs, 18000, 42000);
      }
      return;
    }

    const beforeWins = safeInt(bot.wins);
    const min = Math.max(0, userWins - 1);
    const max = Math.max(userWins + (bot.isChampion ? 1 : 0), min);
    bot.wins = clamp(randomInt(min, max), 0, userWins + 1);
    if (bot.wins > beforeWins) {
      setBotPresence(bot, "playing", nowMs, 16000, 28000);
    }
    bot.lastUpdatedMs = nowMs;
  });
}

function bumpIdleBots(bots, userWins, nowMs) {
  const allEligible = bots.filter((b) => !b.isChampion);
  let eligible = allEligible.filter((b) => normalizeTournamentPresenceStatus(b.presenceStatus) === "playing");
  if (!eligible.length && allEligible.length) {
    const activations = randomInt(1, Math.max(1, Math.floor(allEligible.length / 4)));
    for (let i = 0; i < activations; i += 1) {
      const pick = allEligible[randomInt(0, allEligible.length - 1)];
      if (!pick) continue;
      setBotPresence(pick, "playing", nowMs, 16000, 28000);
    }
    eligible = allEligible.filter((b) => normalizeTournamentPresenceStatus(b.presenceStatus) === "playing");
  }
  const bumps = randomInt(1, Math.max(1, Math.min(eligible.length || 1, Math.floor(allEligible.length / 3) || 1)));
  for (let i = 0; i < bumps; i += 1) {
    const pick = eligible[randomInt(0, eligible.length - 1)];
    if (!pick) continue;
    pick.wins = clamp(pick.wins + 1, 0, userWins + 3);
    setBotPresence(pick, "playing", nowMs, 16000, 28000);
    pick.lastUpdatedMs = nowMs;
  }
}

function simulateObserverBots(bots, userWins, nowMs, timeLeftMs) {
  if (!Array.isArray(bots) || !bots.length) return false;

  const intensity = timeLeftMs <= 2 * 60 * 1000 ? 3 : 2;
  let activePool = bots.filter((b) => normalizeTournamentPresenceStatus(b.presenceStatus) === "playing");

  if (!activePool.length) {
    const activationCount = Math.max(1, Math.min(bots.length, randomInt(1, Math.max(1, Math.floor(bots.length / 4)))));
    for (let i = 0; i < activationCount; i += 1) {
      const pick = bots[randomInt(0, bots.length - 1)];
      if (!pick) continue;
      setBotPresence(pick, "playing", nowMs, 16000, 28000);
    }
    activePool = bots.filter((b) => normalizeTournamentPresenceStatus(b.presenceStatus) === "playing");
  }

  if (!activePool.length) return false;

  const progressCount = Math.max(1, Math.min(activePool.length, randomInt(1, Math.min(4, activePool.length))));
  const picked = new Set();
  let changed = false;

  while (picked.size < progressCount && picked.size < activePool.length) {
    const nextBot = activePool[randomInt(0, activePool.length - 1)];
    if (!nextBot) continue;
    picked.add(nextBot.id);
  }

  bots.forEach((bot) => {
    if (!picked.has(bot.id)) return;
    const beforeWins = safeInt(bot.wins);
    const maxDelta = bot.isChampion ? intensity : Math.max(1, intensity - 1);
    const delta = Math.max(1, randomInt(1, maxDelta));
    bot.wins = clamp(beforeWins + delta, 0, userWins + 4);
    setBotPresence(bot, "playing", nowMs, 18000, 32000);
    bot.lastUpdatedMs = nowMs;
    if (bot.wins !== beforeWins) changed = true;
  });

  return changed;
}

function championProgress(bots, championId, userWins, timeLeftMs, nowMs, observerMode = false) {
  const champ = bots.find((b) => b.id === championId) || bots.find((b) => b.isChampion);
  if (!champ) return;
  const beforeWins = safeInt(champ.wins);
  champ.isChampion = true;
  if (timeLeftMs > 2 * 60 * 1000) {
    // pas de dépassement avant le sprint final
    if (observerMode) {
      champ.wins = Math.min(champ.wins, Math.max(userWins + 2, 3));
    } else {
      champ.wins = Math.min(champ.wins, userWins);
    }
    return;
  }
  const target = observerMode
    ? clamp(Math.max(champ.wins, userWins + randomInt(2, 5)), userWins + 2, Math.max(userWins + 5, 6))
    : clamp(userWins + randomInt(1, 3), userWins + 1, userWins + 3);
  champ.wins = Math.max(champ.wins, target);
  if (champ.wins > beforeWins) {
    setBotPresence(champ, "playing", nowMs, 18000, 30000);
  }
  champ.lastUpdatedMs = nowMs;
}

function sortLeaderboard(entries) {
  return entries.sort((a, b) => {
    const byScore = safeInt(b.wins) - safeInt(a.wins);
    if (byScore !== 0) return byScore;
    if (a.isUser && !b.isUser) return -1;
    if (b.isUser && !a.isUser) return 1;
    if (!a.isBot && b.isBot) return -1;
    if (!b.isBot && a.isBot) return 1;
    return String(a.id || a.uid || "").localeCompare(String(b.id || b.uid || ""));
  });
}

function enforceUserTopFive(entries, userId, timeLeftMs) {
  const beforeFinal = timeLeftMs > (2 * 60 * 1000);
  if (!beforeFinal) return entries;
  const sorted = sortLeaderboard(entries.slice());
  const userIndex = sorted.findIndex((e) => e.isUser);
  if (userIndex >= 0 && userIndex < 5) return entries;
  // trop bas -> on réduit les bots dominants pour remonter l'utilisateur
  const userEntry = sorted.find((e) => e.isUser);
  const userWins = userEntry ? userEntry.wins : 0;
  const trimmed = sorted.map((e, idx) => {
    if (e.isUser) return e;
    if (idx < 4) {
      return { ...e, wins: Math.min(e.wins, userWins) };
    }
    return e;
  });
  return trimmed;
}

async function settleUserTournamentRewardIfNeeded({ uid, email, sessionRef, winnerId, rewardAmountDoes }) {
  const safeWinnerId = String(winnerId || "").trim();
  const safeRewardAmount = safeInt(rewardAmountDoes);
  if (!safeWinnerId || safeRewardAmount <= 0) {
    return { rewardGranted: false, rewardAmountDoes: safeRewardAmount };
  }

  return db.runTransaction(async (tx) => {
    const sessionSnap = await tx.get(sessionRef);
    if (!sessionSnap.exists) {
      throw new HttpsError("not-found", "Session introuvable");
    }

    const liveSession = sessionSnap.data() || {};
    const liveRewardAmount = safeInt(liveSession.rewardAmountDoes || safeRewardAmount);
    const alreadyGranted = liveSession.rewardGranted === true;
    const alreadyGrantedTo = String(liveSession.rewardWinnerId || "").trim();

    if (alreadyGranted) {
      return {
        rewardGranted: true,
        rewardAmountDoes: liveRewardAmount,
        rewardWinnerId: alreadyGrantedTo || safeWinnerId,
      };
    }

    if (safeWinnerId !== uid) {
      tx.set(sessionRef, {
        rewardGranted: false,
        rewardAmountDoes: liveRewardAmount,
        rewardWinnerId: safeWinnerId,
        rewardResolvedAt: admin.firestore.FieldValue.serverTimestamp(),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      }, { merge: true });
      return {
        rewardGranted: false,
        rewardAmountDoes: liveRewardAmount,
        rewardWinnerId: safeWinnerId,
      };
    }

    const walletMutation = await applyWalletMutationTx(tx, {
      uid,
      email,
      type: "tournament_reward",
      note: "Recompense de victoire tournoi",
      amountDoes: liveRewardAmount,
      deltaDoes: liveRewardAmount,
    });

    tx.set(sessionRef, {
      rewardGranted: true,
      rewardAmountDoes: liveRewardAmount,
      rewardWinnerId: uid,
      rewardGrantedAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });

    return {
      rewardGranted: true,
      rewardAmountDoes: liveRewardAmount,
      rewardWinnerId: uid,
      doesBalance: walletMutation.afterDoes,
    };
  });
}

async function ensureUserTournamentSessionsInternal(uid) {
  const nowMs = Date.now();
  const metaRef = userTournamentMetaRef(uid);
  return db.runTransaction(async (tx) => {
    const [metaSnap, sessionsSnap] = await Promise.all([
      tx.get(metaRef),
      tx.get(metaRef.collection("sessions")),
    ]);

    const metaData = metaSnap.exists ? (metaSnap.data() || {}) : {};
    const existing = sessionsSnap.docs.map((d) => ({ id: d.id, ...d.data() }));
    const active = existing
      .filter((s) => toMillis(s.endMs) > nowMs && String(s.status || "active") !== "ended")
      .sort((a, b) => safeInt(a.slotNumber || 0) - safeInt(b.slotNumber || 0));

    const quota = normalizeTournamentDailyQuota(metaData.dailyQuota || {}, nowMs);

    if (active.length) {
      const currentSessionId = active.find((s) => s.id === String(metaData.currentSessionId || "").trim())?.id
        || active[0]?.id
        || "";
      tx.set(metaRef, {
        currentSessionId,
        lastAccessMs: nowMs,
        dailyQuota: quota,
        slots: active.map((s) => ({
          sessionId: s.id,
          slotNumber: safeInt(s.slotNumber),
          startMs: s.startMs,
          endMs: s.endMs,
        })),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      }, { merge: true });

      return {
        sessions: active,
        currentSessionId,
        quota: buildTournamentQuotaPayload(quota, true),
      };
    }

    if (quota.playsUsed >= quota.maxPlays) {
      tx.set(metaRef, {
        currentSessionId: "",
        lastAccessMs: nowMs,
        dailyQuota: quota,
        slots: [],
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      }, { merge: true });

      return {
        sessions: [],
        currentSessionId: "",
        quota: buildTournamentQuotaPayload(quota, false),
      };
    }

    const slotNumber = 1;
    const startMs = nowMs;
    const endMs = startMs + USER_TOURNAMENT_DURATION_MS;
    const seed = startMs + slotNumber;
    const { bots, championId } = generateInitialBots(seed, 0);
    const sessionId = `T-${randomCode(10)}`;
    const sessionData = {
      sessionId,
      startMs,
      endMs,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      status: "active",
      slotNumber,
      bots,
      championId,
      lastBotTickMs: startMs,
      lastUserWins: 0,
      lastGameplayActivityMs: startMs,
      lastGameplayProbeMs: startMs,
      lastObserverSimTickMs: startMs,
      winnerId: "",
      rewardAmountDoes: USER_TOURNAMENT_WIN_REWARD_DOES,
      rewardGranted: false,
      rewardWinnerId: "",
      quotaDayKey: quota.dayKey,
      quotaPlayNumber: quota.playsUsed + 1,
    };

    const nextQuota = {
      ...quota,
      playsUsed: quota.playsUsed + 1,
    };

    tx.set(userTournamentSessionRef(uid, sessionId), sessionData);
    tx.set(metaRef, {
      currentSessionId: sessionId,
      lastAccessMs: nowMs,
      dailyQuota: nextQuota,
      slots: [{
        sessionId,
        slotNumber,
        startMs,
        endMs,
      }],
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });

    return {
      sessions: [{ id: sessionId, ...sessionData }],
      currentSessionId: sessionId,
      quota: buildTournamentQuotaPayload(nextQuota, true),
    };
  });
}

exports.ensureUserTournamentSessions = publicOnCall("ensureUserTournamentSessions", async (request) => {
  const { uid } = assertAuth(request);
  const { sessions, currentSessionId, quota } = await ensureUserTournamentSessionsInternal(uid);
  return {
    sessions: sessions.map((s) => ({
      sessionId: s.id || s.sessionId,
      slotNumber: safeInt(s.slotNumber),
      startMs: toMillis(s.startMs),
      endMs: toMillis(s.endMs),
      status: s.status || "active",
    })),
    currentSessionId,
    quota,
    canPlay: quota?.canPlay === true,
    hasActiveSession: quota?.hasActiveSession === true,
    isLocked: quota?.isLocked === true,
    playsUsedToday: safeInt(quota?.playsUsedToday),
    playsRemainingToday: safeInt(quota?.playsRemainingToday),
    dailyLimit: safeInt(quota?.dailyLimit || USER_TOURNAMENT_DAILY_LIMIT),
    nextResetMs: safeSignedInt(quota?.nextResetMs),
    blockedUntilMs: safeSignedInt(quota?.blockedUntilMs),
  };
});

exports.selectUserTournament = publicOnCall("selectUserTournament", async (request) => {
  const { uid } = assertAuth(request);
  const sessionId = sanitizeText(request.data?.sessionId || "", 120);
  if (!sessionId) throw new HttpsError("invalid-argument", "sessionId requis");
  const sessionRef = userTournamentSessionRef(uid, sessionId);
  const sessionSnap = await sessionRef.get();
  if (!sessionSnap.exists) throw new HttpsError("not-found", "Session introuvable");
  const session = sessionSnap.data() || {};
  if (toMillis(session.endMs) <= Date.now()) throw new HttpsError("failed-precondition", "Session expirée");
  await userTournamentMetaRef(uid).set({ currentSessionId: sessionId, lastAccessMs: Date.now() }, { merge: true });
  return { sessionId };
});

exports.abandonUserTournament = publicOnCall("abandonUserTournament", async (request) => {
  const { uid } = assertAuth(request);
  const sessionId = sanitizeText(request.data?.sessionId || "", 120);
  if (!sessionId) throw new HttpsError("invalid-argument", "sessionId requis");

  const metaRef = userTournamentMetaRef(uid);
  const sessionRef = userTournamentSessionRef(uid, sessionId);

  await db.runTransaction(async (tx) => {
    const [metaSnap, sessionSnap] = await Promise.all([
      tx.get(metaRef),
      tx.get(sessionRef),
    ]);

    if (!sessionSnap.exists) {
      return;
    }

    tx.delete(sessionRef);

    const metaData = metaSnap.exists ? (metaSnap.data() || {}) : {};
    if (String(metaData.currentSessionId || "") === sessionId) {
      tx.set(metaRef, {
        currentSessionId: "",
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      }, { merge: true });
    }
  });

  return { ok: true, sessionId };
});

async function getUserTournamentStateInternal(uid, sessionId, email = "") {
  const sessionRef = userTournamentSessionRef(uid, sessionId);
  const snap = await sessionRef.get();
  if (!snap.exists) throw new HttpsError("not-found", "Session introuvable");
  const session = snap.data() || {};
  const nowMs = Date.now();
  const startMs = toMillis(session.startMs) || nowMs;
  const endMs = toMillis(session.endMs) || (startMs + USER_TOURNAMENT_DURATION_MS);
  let status = String(session.status || "active");
  let bots = Array.isArray(session.bots) ? session.bots.map((b) => ({ ...b })) : [];
  let championId = session.championId || "";
  const lastUserWins = safeInt(session.lastUserWins || 0);
  const lastBotTickMs = toMillis(session.lastBotTickMs) || startMs;
  let lastGameplayActivityMs = toMillis(session.lastGameplayActivityMs) || startMs;
  let lastGameplayProbeMs = toMillis(session.lastGameplayProbeMs) || startMs;
  let lastObserverSimTickMs = toMillis(session.lastObserverSimTickMs) || startMs;
  const sessionRewardAmount = safeInt(session.rewardAmountDoes || USER_TOURNAMENT_WIN_REWARD_DOES);

  if (!bots.length) {
    const init = generateInitialBots(startMs, 0);
    bots = init.bots;
    championId = init.championId;
  }

  const userWins = await countUserWinsInWindow(uid, startMs, endMs);
  const timeLeftMs = Math.max(0, endMs - nowMs);
  const elapsedMs = Math.max(0, nowMs - startMs);
  const warmupActive = elapsedMs < USER_TOURNAMENT_WARMUP_MS;
  const shouldProbeActivity = (nowMs - lastGameplayProbeMs) >= USER_TOURNAMENT_ACTIVITY_PROBE_MS || lastGameplayActivityMs <= 0;

  if (shouldProbeActivity) {
    lastGameplayActivityMs = await resolveTournamentGameplayActivityMs(uid, lastGameplayActivityMs || startMs);
    lastGameplayProbeMs = nowMs;
  }

  const observerMode = !warmupActive && (nowMs - Math.max(lastGameplayActivityMs, startMs)) >= USER_TOURNAMENT_OBSERVER_IDLE_MS;

  if (status !== "ended") {
    if (warmupActive) {
      bots.forEach((bot) => {
        setBotPresence(bot, "online", nowMs, USER_TOURNAMENT_WARMUP_MS, USER_TOURNAMENT_WARMUP_MS + 1000);
      });
    } else {
      refreshBotPresence(bots, nowMs);

      if (observerMode) {
        if ((nowMs - lastObserverSimTickMs) >= USER_TOURNAMENT_OBSERVER_TICK_MS) {
          simulateObserverBots(bots, userWins, nowMs, timeLeftMs);
          lastObserverSimTickMs = nowMs;
        }
      } else if (userWins > lastUserWins) {
        spreadBotsOnUserGain(bots, userWins, nowMs);
      } else if (nowMs - lastBotTickMs > 60 * 1000) {
        bumpIdleBots(bots, userWins, nowMs);
      }

      bots.forEach((b) => capBotWins(b, userWins, timeLeftMs, observerMode));
      championProgress(bots, championId, userWins, timeLeftMs, nowMs, observerMode);
    }

    const leaderboardEntries = [
      { id: uid, wins: warmupActive ? 0 : userWins, isBot: false, isUser: true, activityStatus: "online" },
      ...bots.map((b) => ({
        id: b.id,
        wins: warmupActive ? 0 : safeInt(b.wins),
        isBot: true,
        isChampion: !!b.isChampion,
        activityStatus: warmupActive ? "online" : normalizeTournamentPresenceStatus(b.presenceStatus),
      })),
    ];
    const enforced = observerMode
      ? leaderboardEntries
      : enforceUserTopFive(leaderboardEntries, uid, timeLeftMs);
    const sorted = sortLeaderboard(enforced);
    const winnerId = timeLeftMs <= 0 ? (sorted[0]?.id || championId || uid) : (session.winnerId || "");
    status = timeLeftMs <= 0 ? "ended" : status;

    let rewardState = {
      rewardGranted: session.rewardGranted === true,
      rewardAmountDoes: sessionRewardAmount,
      rewardWinnerId: String(session.rewardWinnerId || "").trim(),
    };

    if (status === "ended") {
      rewardState = await settleUserTournamentRewardIfNeeded({
        uid,
        email,
        sessionRef,
        winnerId,
        rewardAmountDoes: sessionRewardAmount,
      });
    }

    await sessionRef.set({
      status,
      bots,
      championId,
      lastUserWins: warmupActive ? lastUserWins : userWins,
      lastBotTickMs: warmupActive ? lastBotTickMs : nowMs,
      lastGameplayActivityMs,
      lastGameplayProbeMs,
      lastObserverSimTickMs,
      winnerId,
      rewardAmountDoes: rewardState.rewardAmountDoes,
      rewardGranted: rewardState.rewardGranted,
      rewardWinnerId: rewardState.rewardWinnerId || winnerId,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });

    return {
      session: {
        sessionId,
        startMs,
        endMs,
        status,
        slotNumber: safeInt(session.slotNumber || 0),
        winnerId,
        rewardAmountDoes: rewardState.rewardAmountDoes,
        rewardGranted: rewardState.rewardGranted,
        rewardWinnerId: rewardState.rewardWinnerId || winnerId,
      },
      userWins,
      timeLeftMs,
      leaderboard: sorted,
    };
  }

  const leaderboardEntries = [
    { id: uid, wins: userWins, isBot: false, isUser: true, activityStatus: "online" },
    ...bots.map((b) => ({
      id: b.id,
      wins: safeInt(b.wins),
      isBot: true,
      isChampion: !!b.isChampion,
      activityStatus: normalizeTournamentPresenceStatus(b.presenceStatus),
    })),
  ];
  const sorted = sortLeaderboard(leaderboardEntries);
  return {
    session: {
      sessionId,
      startMs,
      endMs,
      status,
      slotNumber: safeInt(session.slotNumber || 0),
      winnerId: session.winnerId || sorted[0]?.id || championId || uid,
      rewardAmountDoes: sessionRewardAmount,
      rewardGranted: session.rewardGranted === true,
      rewardWinnerId: String(session.rewardWinnerId || "").trim(),
    },
    userWins,
    timeLeftMs,
    leaderboard: sorted,
  };
}

exports.getUserTournamentState = publicOnCall("getUserTournamentState", async (request) => {
  const { uid, email } = assertAuth(request);
  const sessionId = sanitizeText(request.data?.sessionId || "", 120);
  if (!sessionId) throw new HttpsError("invalid-argument", "sessionId requis");
  return getUserTournamentStateInternal(uid, sessionId, email);
});


exports.createAmbassadorSecure = publicOnCall("createAmbassadorSecure", async (request) => {
  if (!AMBASSADOR_SYSTEM_ENABLED) {
    throw new HttpsError("failed-precondition", "Système ambassadeur désactivé.");
  }
  const { uid, email } = assertFinanceAdmin(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};

  const name = String(payload.name || "").trim();
  const authCode = String(payload.authCode || "").trim();
  if (!name) {
    throw new HttpsError("invalid-argument", "Nom ambassadeur requis.");
  }
  if (authCode.length < 4 || authCode.length > 128) {
    throw new HttpsError("invalid-argument", "Code d'auth invalide.");
  }

  let promoCode = normalizeCode(payload.promoCode || "");
  if (!promoCode) {
    promoCode = await generateUniqueAmbassadorCode(AMBASSADOR_PROMO_PREFIX, 6);
  } else if (await ambassadorCodeExists(promoCode)) {
    throw new HttpsError("already-exists", "Ce code promo existe déjà.");
  }

  let linkCode = normalizeCode(payload.linkCode || "");
  if (!linkCode) {
    linkCode = await generateUniqueAmbassadorCode(AMBASSADOR_LINK_PREFIX, 6);
  } else if (await ambassadorCodeExists(linkCode)) {
    throw new HttpsError("already-exists", "Ce code lien existe déjà.");
  }

  const hashed = hashAuthCode(authCode);
  const ref = db.collection(AMBASSADORS_COLLECTION).doc();
  const batch = db.batch();
  batch.set(ref, {
    name,
    promoCode,
    linkCode,
    doesBalance: 0,
    totalSignups: 0,
    totalSignupsViaLink: 0,
    totalSignupsViaCode: 0,
    totalDeposits: 0,
    totalGames: 0,
    totalInvitedWins: 0,
    totalInvitedLosses: 0,
    createdByUid: uid,
    createdByEmail: email,
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  });
  batch.set(ambassadorSecretsRef(ref), {
    authCodeHash: hashed.hashHex,
    authCodeSalt: hashed.saltHex,
    authCodeAlgo: hashed.algo,
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  }, { merge: true });
  await batch.commit();

  return {
    ok: true,
    account: {
      id: ref.id,
      name,
      promoCode,
      linkCode,
      doesBalance: 0,
      totalSignups: 0,
      totalDeposits: 0,
      totalGames: 0,
      totalInvitedWins: 0,
      totalInvitedLosses: 0,
      createdByUid: uid,
      createdByEmail: email,
      referralLink: buildAmbassadorReferralLink(linkCode || promoCode),
    },
  };
});

exports.ambassadorLoginSecure = publicOnCall("ambassadorLoginSecure", async (request) => {
  if (!AMBASSADOR_SYSTEM_ENABLED) {
    return { ok: false, reason: "disabled" };
  }
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const code = normalizeCode(payload.promoCode || payload.code || "");
  const authCode = String(payload.authCode || "").trim();
  if (!code || !authCode) {
    return { ok: false, reason: "missing" };
  }

  const [promoSnap, linkSnap] = await Promise.all([
    db.collection(AMBASSADORS_COLLECTION).where("promoCode", "==", code).limit(1).get(),
    db.collection(AMBASSADORS_COLLECTION).where("linkCode", "==", code).limit(1).get(),
  ]);
  const candidateDoc = !promoSnap.empty ? promoSnap.docs[0] : (!linkSnap.empty ? linkSnap.docs[0] : null);
  if (!candidateDoc) {
    return { ok: false, reason: "invalid" };
  }

  const candidate = candidateDoc.data() || {};
  const secrets = await readAmbassadorSecrets(candidateDoc);
  const hashHex = secrets.hashHex;
  const saltHex = secrets.saltHex;
  const algo = secrets.algo;
  const legacyPlain = secrets.legacyPlain;

  let valid = false;
  if (hashHex && saltHex) {
    valid = verifyAuthCode(authCode, hashHex, saltHex, algo);
  } else if (legacyPlain) {
    valid = safeCompareText(legacyPlain.trim(), authCode);
  }

  if (!valid) {
    return { ok: false, reason: "invalid" };
  }

  if (!secrets.hasPrivate || secrets.hasPublicSecrets) {
    await ambassadorSecretsRef(candidateDoc.ref).set({
      authCodeHash: hashHex,
      authCodeSalt: saltHex,
      authCodeAlgo: algo,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
    await candidateDoc.ref.set({
      authCode: admin.firestore.FieldValue.delete(),
      authCodeHash: admin.firestore.FieldValue.delete(),
      authCodeSalt: admin.firestore.FieldValue.delete(),
      authCodeAlgo: admin.firestore.FieldValue.delete(),
      authCodeMigratedAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
  }

  return {
    ok: true,
    ambassador: {
      id: candidateDoc.id,
      name: String(candidate.name || ""),
      promoCode: normalizeCode(candidate.promoCode || ""),
      linkCode: normalizeCode(candidate.linkCode || ""),
      doesBalance: safeSignedInt(candidate.doesBalance),
      totalSignups: safeInt(candidate.totalSignups),
      totalDeposits: safeInt(candidate.totalDeposits),
      totalGames: safeInt(candidate.totalGames),
      totalInvitedWins: safeInt(candidate.totalInvitedWins),
      totalInvitedLosses: safeInt(candidate.totalInvitedLosses),
      referralLink: buildAmbassadorReferralLink(candidate.linkCode || candidate.promoCode || ""),
    },
  };
});

exports.migrateAmbassadorSecrets = publicOnCall("migrateAmbassadorSecrets", async (request) => {
  assertFinanceAdmin(request);
  const snap = await db.collection(AMBASSADORS_COLLECTION).get();

  let migrated = 0;
  let skipped = 0;
  let pendingOps = 0;
  let batch = db.batch();

  for (const item of snap.docs) {
    const secrets = await readAmbassadorSecrets(item);
    let nextHash = "";
    let nextSalt = "";
    let nextAlgo = AUTH_HASH_ALGO;

    if (secrets.hasPrivate && secrets.hashHex && secrets.saltHex) {
      nextHash = secrets.hashHex;
      nextSalt = secrets.saltHex;
      nextAlgo = secrets.algo;
    } else if (secrets.hashHex && secrets.saltHex) {
      nextHash = secrets.hashHex;
      nextSalt = secrets.saltHex;
      nextAlgo = secrets.algo;
    } else if (secrets.legacyPlain) {
      const next = hashAuthCode(secrets.legacyPlain);
      nextHash = next.hashHex;
      nextSalt = next.saltHex;
      nextAlgo = next.algo;
    }

    if (!nextHash || !nextSalt) {
      skipped += 1;
      continue;
    }

    batch.set(ambassadorSecretsRef(item.ref), {
      authCodeHash: nextHash,
      authCodeSalt: nextSalt,
      authCodeAlgo: nextAlgo,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
    batch.set(item.ref, {
      authCode: admin.firestore.FieldValue.delete(),
      authCodeHash: admin.firestore.FieldValue.delete(),
      authCodeSalt: admin.firestore.FieldValue.delete(),
      authCodeAlgo: admin.firestore.FieldValue.delete(),
      authCodeMigratedAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
    pendingOps += 2;
    migrated += 1;

    if (pendingOps >= 350) {
      await batch.commit();
      batch = db.batch();
      pendingOps = 0;
    }
  }

  if (pendingOps > 0) {
    await batch.commit();
  }

  return {
    ok: true,
    total: snap.size,
    migrated,
    skipped,
  };
});
