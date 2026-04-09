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
const ROOM_RESULTS_COLLECTION = "roomResults";
const GAME_STATES_COLLECTION = "gameStates";
const DUEL_ROOMS_COLLECTION = "duelRooms";
const DUEL_ROOM_RESULTS_COLLECTION = "duelRoomResults";
const DUEL_GAME_STATES_COLLECTION = "duelGameStates";
const MORPION_ROOMS_COLLECTION = "morpionRooms";
const MORPION_ROOM_RESULTS_COLLECTION = "morpionRoomResults";
const MORPION_GAME_STATES_COLLECTION = "morpionGameStates";
const CLIENTS_COLLECTION = "clients";
const AGENTS_COLLECTION = "agents";
const AGENT_LEDGER_SUBCOLLECTION = "ledger";
const AGENT_MONTHLY_STATEMENTS_SUBCOLLECTION = "monthlyStatements";
const AMBASSADORS_COLLECTION = "ambassadors";
const AMBASSADOR_EVENTS_COLLECTION = "ambassadorGameEvents";
const AMBASSADOR_PRIVATE_SUBCOLLECTION = "private";
const AMBASSADOR_SECRETS_DOC = "credentials";
const CHAT_COLLECTION = "globalChannelMessages";
const SUPPORT_THREADS_COLLECTION = "supportThreads";
const SUPPORT_MESSAGES_SUBCOLLECTION = "messages";
const SURVEYS_COLLECTION = "surveys";
const SURVEY_RESPONSES_SUBCOLLECTION = "responses";
const DASHBOARD_PUSH_SUBSCRIPTIONS_COLLECTION = "dashboardPushSubscriptions";
const MATCHMAKING_POOLS_COLLECTION = "matchmakingPools";
const DUEL_MATCHMAKING_POOLS_COLLECTION = "duelMatchmakingPools";
const MORPION_MATCHMAKING_POOLS_COLLECTION = "morpionMatchmakingPools";
const MORPION_PLAYER_PROFILES_COLLECTION = "morpionPlayerProfiles";
const MORPION_WAITING_REQUESTS_COLLECTION = "morpionWaitingRequests";
const MORPION_PLAY_INVITATIONS_COLLECTION = "morpionPlayInvitations";
const ANALYTICS_META_COLLECTION = "analyticsMeta";
const ANALYTICS_PRESENCE_SNAPSHOTS_COLLECTION = "analyticsPresenceSnapshots";
const ANALYTICS_PRESENCE_DAILY_COLLECTION = "analyticsPresenceDaily";
const ANALYTICS_PRESENCE_MONTHLY_COLLECTION = "analyticsPresenceMonthly";
const ANALYTICS_PRESENCE_HOUR_COLLECTION = "analyticsPresenceHours";
const ANALYTICS_PRESENCE_WEEKDAY_COLLECTION = "analyticsPresenceWeekdays";
const RECRUITMENT_APPLICATIONS_COLLECTION = "recruitmentApplications";
const RECRUITMENT_CAMPAIGN_DOC = "recruitmentCampaign";

const RATE_HTG_TO_DOES = 20;
const DEFAULT_STAKE_REWARD_MULTIPLIER = 3;
const DEPOSIT_BONUS_MIN_HTG = 100;
const DEPOSIT_BONUS_PERCENT = 10;
const WELCOME_BONUS_LAUNCH_AT_MS = 1774142207000;
const WELCOME_BONUS_END_AT_MS = Date.parse("2026-04-02T03:59:59.999Z");
const PUBLIC_HOME_URL = "https://dominoeslakay.com/inedex.html";
const USER_REFERRAL_DEPOSIT_REWARD = 100;
const FINANCE_ADMIN_EMAIL = "leovitch2004@gmail.com";
const MIN_ORDER_HTG = 25;
const MIN_WITHDRAWAL_HTG = 50;
const MAX_WITHDRAWAL_HTG = 500000;
const MAX_PUBLIC_TEXT_LENGTH = 500;
const USER_REFERRAL_PREFIX = "USR";
const AGENT_PROMO_PREFIX = "AGT";
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
const WELCOME_BONUS_ORDER_TYPE = "welcome_bonus";
const WELCOME_BONUS_HTG_AMOUNT = 25;
const DPAYMENT_ADMIN_BOOTSTRAP_DOC = "dpayment_admin_bootstrap";
const APP_PUBLIC_SETTINGS_DOC = "public_app_settings";
const DASHBOARD_DEFAULT_NOTIFICATION_URL = "./Dpayment.html";
const RECRUITMENT_TARGET_COUNT = 100;
const RECRUITMENT_DEADLINE_MS = Date.parse("2026-04-07T03:59:59.999Z");
const DEFAULT_GAME_STAKE_OPTIONS = Object.freeze([
  Object.freeze({ stakeDoes: 100, enabled: true, sortOrder: 10 }),
  Object.freeze({ stakeDoes: 500, enabled: false, sortOrder: 20 }),
  Object.freeze({ stakeDoes: 1000, enabled: false, sortOrder: 30 }),
  Object.freeze({ stakeDoes: 5000, enabled: false, sortOrder: 40 }),
]);
const DEFAULT_DUEL_STAKE_OPTIONS = Object.freeze([
  Object.freeze({ id: "duel_500", stakeDoes: 500, rewardDoes: 925, enabled: true, sortOrder: 10 }),
  Object.freeze({ id: "duel_1000", stakeDoes: 1000, rewardDoes: 1850, enabled: true, sortOrder: 20 }),
]);
const DEFAULT_MORPION_STAKE_OPTIONS = Object.freeze([
  Object.freeze({ id: "morpion_500", stakeDoes: 500, rewardDoes: 900, enabled: true, sortOrder: 10 }),
  Object.freeze({ id: "morpion_1000", stakeDoes: 1000, rewardDoes: 1800, enabled: false, sortOrder: 20 }),
]);
const MAX_FRIEND_MORPION_STAKE_DOES = 100_000_000;

function computeDepositBonusSnapshot(amountHtg = 0) {
  const safeAmountHtg = Math.max(0, Number(amountHtg) || 0);
  const eligible = safeAmountHtg >= DEPOSIT_BONUS_MIN_HTG;
  const bonusHtgRaw = eligible ? (safeAmountHtg * DEPOSIT_BONUS_PERCENT) / 100 : 0;
  const bonusDoes = eligible ? Math.max(0, Math.floor(bonusHtgRaw * RATE_HTG_TO_DOES)) : 0;
  return {
    eligible,
    thresholdHtg: DEPOSIT_BONUS_MIN_HTG,
    bonusPercent: DEPOSIT_BONUS_PERCENT,
    bonusHtgRaw,
    bonusDoes,
    rateHtgToDoes: RATE_HTG_TO_DOES,
  };
}
const DEFAULT_BOT_DIFFICULTY = "expert";
const DEFAULT_DUEL_BOT_DIFFICULTY = "expert";
const ROOM_WAIT_MS = 15 * 1000;
const DUEL_TURN_LIMIT_MS = 15 * 1000;
const MORPION_TURN_LIMIT_MS = 30 * 1000;
const FRIEND_ROOM_WAIT_MS = 10 * 60 * 1000;
const FRIEND_ROOM_CODE_SIZE = 6;
const ROOM_DISCONNECT_TAKEOVER_MS = 45 * 1000;
const ROOM_DISCONNECT_GRACE_MS = 45 * 1000;
const BOT_THINK_DELAY_MIN_MS = 650;
const BOT_THINK_DELAY_MAX_MS = 1200;
const BOT_THINK_DELAY_PASS_MIN_MS = 320;
const BOT_THINK_DELAY_PASS_MAX_MS = 650;
const BOT_DIFFICULTY_LEVELS = new Set(["amateur", "expert", "ultra", "userpro"]);
const BOT_PILOT_MODES = new Set(["manual", "auto"]);
const BOT_PILOT_WINDOW_MS = 24 * 60 * 60 * 1000;
const BOT_PILOT_SNAPSHOT_LIMIT = 5000;
const BOT_PILOT_TREND_POINT_LIMIT = 12;
const BOT_PILOT_EQUITY_POINT_LIMIT = 24;
const MORPION_PILOT_MODES = new Set(["manual", "auto"]);
const MORPION_PILOT_WINDOW_MS = 24 * 60 * 60 * 1000;
const MORPION_PILOT_SNAPSHOT_LIMIT = 5000;
const MORPION_PILOT_TREND_POINT_LIMIT = 12;
const MORPION_PILOT_EQUITY_POINT_LIMIT = 24;
const MORPION_BOT_BLOCK_MIN_GAMES = 5;
const MORPION_BOT_BLOCK_MIN_WIN_RATE = 0.6;
const MORPION_BOT_BLOCK_WIN_STREAK = 2;
const MORPION_MATCHMAKING_REASON_SKILLED_WAIT_ONLY = "morpion-skilled-wait-human-only";
const MORPION_WAITING_ONLINE_WINDOW_MS = 45 * 1000;
const MORPION_INVITATION_TTL_MS = 90 * 1000;
const MORPION_WAITING_QUEUE_FETCH_LIMIT = 220;
const MORPION_WHATSAPP_RECENT_WINDOW_MS = 72 * 60 * 60 * 1000;
const MORPION_WHATSAPP_LIST_LIMIT = 40;
const CLIENT_SITE_PRESENCE_WINDOW_MS = 70 * 1000;
const ACQUISITION_DEFAULT_WINDOW_MS = 30 * 24 * 60 * 60 * 1000;
const ACQUISITION_MAX_WINDOW_MS = 180 * 24 * 60 * 60 * 1000;
const ACQUISITION_ACTIVE_LOOKBACK_MS = 7 * 24 * 60 * 60 * 1000;
const ACQUISITION_FIDELITY_MIN_AGE_MS = 3 * 24 * 60 * 60 * 1000;
const ACQUISITION_PAGE_FETCH_SIZE = 1000;
const ACQUISITION_DOC_LIMIT = 10000;
const DEPOSIT_ANALYTICS_DEFAULT_WINDOW_MS = 30 * 24 * 60 * 60 * 1000;
const DEPOSIT_ANALYTICS_MAX_WINDOW_MS = 180 * 24 * 60 * 60 * 1000;
const DEPOSIT_ANALYTICS_PAGE_FETCH_SIZE = 1000;
const DEPOSIT_ANALYTICS_DOC_LIMIT = 12000;
const AGENT_DEPOSIT_SEARCH_FALLBACK_LIMIT = 250;
const AGENT_DEPOSIT_CONTEXT_ORDER_LIMIT = 12;
const AGENT_DEPOSIT_SEARCH_RESULT_LIMIT = 12;
const AGENT_SEARCH_RESULT_LIMIT = 12;
const AGENT_LIST_LIMIT = 180;
const AGENT_INITIAL_SIGNUP_BUDGET_HTG = 25000;
const AGENT_COMMISSION_MATRIX = Object.freeze({
  domino_classic: Object.freeze({
    100: 50,
    500: 250,
  }),
  domino_duel: Object.freeze({
    100: 1,
    500: 40,
  }),
  morpion: Object.freeze({
    500: 50,
  }),
});
const AGENT_ASSISTED_METHOD_ID = "agent_assisted";
const DEPOSIT_PROOF_MIN_DELAY_MS = 6 * 60 * 1000;
const DEPOSIT_SHADOW_GUARD_WINDOW_MS = DEPOSIT_PROOF_MIN_DELAY_MS;
const DEPOSIT_SHADOW_GUARD_RAPID_THRESHOLD = 2;
const DEPOSIT_SHADOW_GUARD_LOCK_MS = 30 * 60 * 1000;
const BOT_DIFFICULTY_LOOKAHEAD = Object.freeze({
  amateur: 0,
  expert: 3,
  ultra: 5,
  userpro: 0,
});
const DUEL_ELITE_LOOKAHEAD_PLIES = 4;
const DUEL_BOT_DIFFICULTY_LOOKAHEAD = Object.freeze({
  amateur: 0,
  expert: 2,
  ultra: DUEL_ELITE_LOOKAHEAD_PLIES,
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
const SURVEY_MAX_CHOICES = 8;
const SURVEY_MAX_CHOICE_LABEL = 90;
const SURVEY_MAX_TITLE = 140;
const SURVEY_MAX_DESCRIPTION = 600;
const SURVEY_MAX_TEXT_ANSWER = 500;
const PROVISIONAL_FUNDING_VERSION = 2;
const PROVISIONAL_CREDIT_MODE = "provisional";
const ACCOUNT_FREEZE_REJECT_THRESHOLD = 3;
const PRESENCE_ANALYTICS_TIMEZONE = "America/Port-au-Prince";
const PRESENCE_ANALYTICS_CLIENT_WINDOW_MS = 15 * 60 * 1000;
const PRESENCE_ANALYTICS_ROOM_WINDOW_MS = 60 * 1000;
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

function publicOnCall(callableName, handler, options = {}) {
  return onCall({
    ...options,
  }, async (request) => {
    assertAppCheck(request, callableName);
    return handler(request);
  });
}

function safeInt(value, fallback = 0) {
  const n = Number(value);
  if (Number.isFinite(n)) {
    return Math.max(0, Math.floor(n));
  }
  const safeFallback = Number(fallback);
  return Number.isFinite(safeFallback) ? Math.max(0, Math.floor(safeFallback)) : 0;
}

function safeSignedInt(value, fallback = 0) {
  const n = Number(value);
  if (Number.isFinite(n)) {
    return Math.trunc(n);
  }
  const safeFallback = Number(fallback);
  return Number.isFinite(safeFallback) ? Math.trunc(safeFallback) : 0;
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

function getWithdrawalStatus(withdrawal = {}) {
  const resolution = String(withdrawal?.resolutionStatus || "").trim().toLowerCase();
  if (
    resolution === "approved"
    || resolution === "rejected"
    || resolution === "pending"
    || resolution === "review"
    || resolution === "cancelled"
    || resolution === "canceled"
  ) {
    return resolution;
  }
  const status = String(withdrawal?.status || "").trim().toLowerCase();
  if (
    status === "approved"
    || status === "rejected"
    || status === "pending"
    || status === "review"
    || status === "cancelled"
    || status === "canceled"
  ) {
    return status;
  }
  return "pending";
}

function isWithdrawalReservedStatus(withdrawalOrStatus = "") {
  const normalized = typeof withdrawalOrStatus === "object" && withdrawalOrStatus
    ? getWithdrawalStatus(withdrawalOrStatus)
    : String(withdrawalOrStatus || "").trim().toLowerCase();
  return normalized !== "rejected" && normalized !== "cancelled" && normalized !== "canceled";
}

function isWithdrawalClientCancellableStatus(withdrawalOrStatus = "") {
  const normalized = typeof withdrawalOrStatus === "object" && withdrawalOrStatus
    ? getWithdrawalStatus(withdrawalOrStatus)
    : String(withdrawalOrStatus || "").trim().toLowerCase();
  return normalized === "pending" || normalized === "review";
}

function computeWalletAvailableGourdes({
  orders = [],
  withdrawals = [],
  exchangedGourdes = 0,
} = {}) {
  const approvedDepositsHtg = (Array.isArray(orders) ? orders : []).reduce(
    (sum, item) => sum + getOrderApprovedRealAmountHtg(item),
    0
  );
  const reservedWithdrawalsHtg = (Array.isArray(withdrawals) ? withdrawals : []).reduce((sum, item) => {
    if (!isWithdrawalReservedStatus(item)) return sum;
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

function getOrderType(order = {}) {
  return String(order?.orderType || order?.kind || "").trim().toLowerCase();
}

function isWelcomeBonusOrder(order = {}) {
  return getOrderType(order) === WELCOME_BONUS_ORDER_TYPE || order?.isWelcomeBonus === true;
}

function getOrderResolutionStatus(order = {}) {
  const resolution = String(order?.resolutionStatus || "").trim().toLowerCase();
  if (
    resolution === "approved"
    || resolution === "rejected"
    || resolution === "pending"
    || resolution === "review"
    || resolution === "cancelled"
    || resolution === "canceled"
  ) {
    return resolution;
  }
  const status = String(order?.status || "").trim().toLowerCase();
  if (
    status === "approved"
    || status === "rejected"
    || status === "review"
    || status === "cancelled"
    || status === "canceled"
  ) return status;
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

function getOrderApprovedRealAmountHtg(order = {}) {
  if (isWelcomeBonusOrder(order)) return 0;
  return getOrderApprovedAmountHtg(order);
}

function getOrderApprovedWelcomeAmountHtg(order = {}) {
  if (!isWelcomeBonusOrder(order)) return 0;
  return getOrderApprovedAmountHtg(order);
}

function hasRealApprovedDepositFromOrders(orders = []) {
  return (Array.isArray(orders) ? orders : []).some((item) => getOrderApprovedRealAmountHtg(item) > 0);
}

function hasWelcomeBonusOrder(orders = []) {
  return (Array.isArray(orders) ? orders : []).some((item) => isWelcomeBonusOrder(item));
}

function normalizeWelcomeBonusPromptStatus(value = "") {
  const normalized = sanitizeText(value || "", 32).toLowerCase();
  if (normalized === "accepted" || normalized === "declined" || normalized === "pending") return normalized;
  return "";
}

function generateWelcomeBonusProofCode(uid = "") {
  const safeUid = sanitizeText(uid || "", 80).toUpperCase().replace(/[^A-Z0-9]/g, "");
  const head = safeUid.slice(0, 6) || crypto.randomBytes(3).toString("hex").toUpperCase();
  const tail = crypto.randomBytes(2).toString("hex").toUpperCase();
  return `CLIENT-${head}-${tail}`;
}

function normalizeDepositShadowGuard(value = {}) {
  const data = value && typeof value === "object" ? value : {};
  return {
    windowStartedAtMs: safeSignedInt(data.windowStartedAtMs),
    lastAttemptAtMs: safeSignedInt(data.lastAttemptAtMs),
    rapidAttemptCount: safeInt(data.rapidAttemptCount),
    lastProofStepDurationMs: safeInt(data.lastProofStepDurationMs),
    lockedUntilMs: safeSignedInt(data.lockedUntilMs),
  };
}

function resolveWelcomeBonusEligibility({
  walletData = {},
  orders = [],
  fundingSnapshot = {},
} = {}) {
  const createdAtMs = safeSignedInt(walletData.createdAtMs) || toMillis(walletData.createdAt);
  const hasRealApprovedDeposit = fundingSnapshot.hasRealApprovedDeposit === true
    || walletData.hasApprovedDeposit === true
    || hasRealApprovedDepositFromOrders(orders);
  const alreadyClaimed = walletData.welcomeBonusClaimed === true
    || safeSignedInt(walletData.welcomeBonusReceivedAtMs) > 0
    || !!String(walletData.welcomeBonusOrderId || "").trim()
    || hasWelcomeBonusOrder(orders);
  const accountFrozen = walletData.accountFrozen === true;
  const launchedAccount = createdAtMs > 0 && createdAtMs >= WELCOME_BONUS_LAUNCH_AT_MS;
  const offerEnded = Date.now() > WELCOME_BONUS_END_AT_MS;

  let reason = "eligible";
  if (accountFrozen) {
    reason = "account-frozen";
  } else if (alreadyClaimed) {
    reason = "already-claimed";
  } else if (hasRealApprovedDeposit) {
    reason = "existing-real-deposit";
  } else if (!launchedAccount) {
    reason = "legacy-account";
  } else if (offerEnded) {
    reason = "offer-ended";
  }

  return {
    eligible: reason === "eligible",
    reason,
    createdAtMs,
    launchAtMs: WELCOME_BONUS_LAUNCH_AT_MS,
    endAtMs: WELCOME_BONUS_END_AT_MS,
    hasRealApprovedDeposit,
    alreadyClaimed,
    accountFrozen,
    isLegacyAccount: !launchedAccount,
    offerEnded,
  };
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
  const normalizedOrders = Array.isArray(orders) ? orders : [];
  const normalizedExchangeHistory = Array.isArray(exchangeHistory) ? exchangeHistory : [];
  const approvedDepositsHtg = normalizedOrders.reduce(
    (sum, item) => sum + getOrderApprovedRealAmountHtg(item),
    0
  );
  const welcomeBonusApprovedHtg = normalizedOrders.reduce(
    (sum, item) => sum + getOrderApprovedWelcomeAmountHtg(item),
    0
  );
  const reservedWithdrawalsHtg = (Array.isArray(withdrawals) ? withdrawals : []).reduce((sum, item) => {
    if (!isWithdrawalReservedStatus(item)) return sum;
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
  const welcomeBonusHtgAvailable = safeInt(walletData.welcomeBonusHtgAvailable);
  const welcomeBonusHtgConverted = safeInt(walletData.welcomeBonusHtgConverted);
  const hasRealApprovedDeposit = hasRealApprovedDepositFromOrders(normalizedOrders);
  const onlyWelcomeLockedContext = !hasRealApprovedDeposit
    && welcomeBonusHtgConverted > 0
    && pendingPlayFromXchangeDoes <= 0
    && pendingPlayFromReferralDoes <= 0;
  const welcomePlayedDoesFromHistory = normalizedExchangeHistory.reduce((sum, item) => {
    if (String(item?.type || "").trim() !== "game_entry") return sum;
    const explicitWelcomeDoes = safeInt(item?.gameEntryFunding?.welcomeDoes);
    if (explicitWelcomeDoes > 0) return sum + explicitWelcomeDoes;
    if (!onlyWelcomeLockedContext) return sum;
    const inferredAmountDoes = Math.min(
      safeInt(item?.amountDoes),
      safeInt(item?.beforePendingPlayFromWelcomeDoes)
    );
    return sum + Math.max(0, inferredAmountDoes);
  }, 0);
  const welcomeBonusHtgPlayed = Math.max(
    safeInt(walletData.welcomeBonusHtgPlayed),
    Math.floor(welcomePlayedDoesFromHistory / RATE_HTG_TO_DOES)
  );
  const pendingPlayFromWelcomeDoes = safeInt(
    onlyWelcomeLockedContext
      ? approvedDoesBalance
      : walletData.pendingPlayFromWelcomeDoes
  );
  const pendingPlayTotalDoes = pendingPlayFromXchangeDoes + pendingPlayFromReferralDoes + pendingPlayFromWelcomeDoes;
  const exchangeableDoesAvailable = safeInt(
    pendingPlayFromWelcomeDoes > 0
      ? 0
      : (
        typeof walletData.exchangeableDoesAvailable === "number"
          ? Math.min(walletData.exchangeableDoesAvailable, approvedDoesBalance)
          : (pendingPlayTotalDoes <= 0 ? approvedDoesBalance : 0)
      )
  );

  return {
    approvedDepositsHtg,
    realApprovedDepositsHtg: approvedDepositsHtg,
    welcomeBonusApprovedHtg,
    reservedWithdrawalsHtg,
    approvedBaseHtg,
    approvedHtgAvailable,
    provisionalHtgAvailable,
    playableHtg: approvedHtgAvailable + provisionalHtgAvailable + welcomeBonusHtgAvailable,
    exchangedApprovedHtg,
    totalExchangedApprovedHtg,
    remainingToExchangeHtg,
    // HTG becomes withdrawable as it comes back from approved Does reconversion.
    withdrawableHtg: Math.max(0, approvedHtgAvailable - remainingToExchangeHtg),
    approvedDoesBalance,
    provisionalDoesBalance,
    doesBalance: approvedDoesBalance + provisionalDoesBalance,
    exchangeableDoesAvailable,
    welcomeBonusHtgAvailable,
    welcomeBonusHtgConverted,
    welcomeBonusHtgPlayed,
    pendingPlayFromXchangeDoes,
    pendingPlayFromReferralDoes,
    pendingPlayFromWelcomeDoes,
    pendingPlayTotalDoes,
    hasRealApprovedDeposit,
    hasApprovedDeposit: hasRealApprovedDeposit,
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
    welcomeBonusHtgAvailable: safeInt(snapshot.welcomeBonusHtgAvailable),
    welcomeBonusHtgConverted: safeInt(snapshot.welcomeBonusHtgConverted),
    welcomeBonusHtgPlayed: safeInt(snapshot.welcomeBonusHtgPlayed),
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

function buildWithdrawalHoldError(walletData = {}) {
  return new HttpsError(
    "failed-precondition",
    "Ton compte est gelé pour les retraits après plusieurs dépôts refusés. Contacte l'assistance si tu veux plaider ta cause.",
    {
      code: "withdrawal-hold",
      withdrawalHold: true,
      withdrawalHoldReason: String(walletData.withdrawalHoldReason || "3_rejected_deposits"),
      rejectedDepositStrikeCount: safeInt(walletData.rejectedDepositStrikeCount),
    }
  );
}

function assertWalletNotFrozen(walletData = {}) {
  if (walletData?.accountFrozen === true) {
    throw buildFrozenAccountError(walletData);
  }
}

function assertWithdrawalAllowed(walletData = {}) {
  if (walletData?.accountFrozen === true) {
    throw buildFrozenAccountError(walletData);
  }
  if (walletData?.withdrawalHold === true) {
    throw buildWithdrawalHoldError(walletData);
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

function phoneDigits(value = "") {
  return String(value || "").replace(/\D/g, "");
}

function hashText(value = "") {
  return crypto.createHash("sha256").update(String(value || "")).digest("hex");
}

function normalizeRecruitmentRole(value = "") {
  const normalized = sanitizeText(value || "", 20).toLowerCase();
  if (!normalized) return "ambassador";
  if (normalized === "agent" || normalized === "ambassador" || normalized === "both") {
    return normalized;
  }
  return "ambassador";
}

function normalizeRecruitmentSex(value = "") {
  const normalized = sanitizeText(value || "", 16).toLowerCase();
  if (normalized === "homme" || normalized === "femme" || normalized === "autre") {
    return normalized;
  }
  return "";
}

function buildRecruitmentCampaignSnapshot(data = {}) {
  const applicationsCount = safeInt(data?.applicationsCount);
  const targetCount = Math.max(1, safeInt(data?.targetCount) || RECRUITMENT_TARGET_COUNT);
  const deadlineMs = safeInt(data?.deadlineMs) || RECRUITMENT_DEADLINE_MS;
  const nowMs = Date.now();
  return {
    applicationsCount,
    targetCount,
    deadlineMs,
    remainingMs: Math.max(0, deadlineMs - nowMs),
    progressRatio: Math.min(1, applicationsCount / targetCount),
    updatedAtMs: safeInt(data?.updatedAtMs),
  };
}

function normalizeSearchText(value = "") {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .trim();
}

function getAgentDepositMethodMeta(methodId = "", methodDoc = {}) {
  const normalized = sanitizeText(methodId || "", 80).toLowerCase();
  if (normalized === "moncash") {
    return {
      id: "moncash",
      name: sanitizeText(methodDoc.name || "MonCash", 80) || "MonCash",
      accountName: sanitizeText(methodDoc.accountName || "", 120),
      phoneNumber: sanitizePhone(methodDoc.phoneNumber || "", 40),
    };
  }
  if (normalized === "natcash") {
    return {
      id: "natcash",
      name: sanitizeText(methodDoc.name || "NatCash", 80) || "NatCash",
      accountName: sanitizeText(methodDoc.accountName || "", 120),
      phoneNumber: sanitizePhone(methodDoc.phoneNumber || "", 40),
    };
  }
  return {
    id: AGENT_ASSISTED_METHOD_ID,
    name: sanitizeText(methodDoc.name || "Depot via agent", 80) || "Depot via agent",
    accountName: sanitizeText(methodDoc.accountName || "", 120),
    phoneNumber: sanitizePhone(methodDoc.phoneNumber || "", 40),
  };
}

function buildAgentDepositSearchRecord(clientId = "", raw = {}) {
  const approvedHtgAvailable = safeInt(raw.approvedHtgAvailable);
  const provisionalHtgAvailable = safeInt(raw.provisionalHtgAvailable);
  const doesApprovedBalance = safeInt(raw.doesApprovedBalance);
  const doesProvisionalBalance = safeInt(raw.doesProvisionalBalance);
  return {
    id: String(clientId || raw.uid || "").trim(),
    uid: String(raw.uid || clientId || "").trim(),
    name: sanitizeText(raw.name || raw.displayName || raw.username || "", 120),
    username: sanitizeUsername(raw.username || "", 24),
    email: sanitizeEmail(raw.email || "", 160),
    phone: sanitizePhone(raw.phone || raw.customerPhone || "", 40),
    createdAtMs: safeSignedInt(raw.createdAtMs),
    lastSeenAtMs: safeSignedInt(raw.lastSeenAtMs),
    approvedHtgAvailable,
    provisionalHtgAvailable,
    htgBalance: approvedHtgAvailable + provisionalHtgAvailable,
    doesBalance: safeInt(raw.doesBalance || (doesApprovedBalance + doesProvisionalBalance)),
    accountFrozen: raw.accountFrozen === true,
    hasApprovedDeposit: raw.hasApprovedDeposit === true,
  };
}

function buildAgentDepositContextOrder(docSnap) {
  const data = docSnap.data() || {};
  return {
    id: docSnap.id,
    amountHtg: computeOrderAmount(data),
    status: String(data.status || getOrderResolutionStatus(data) || "pending"),
    methodId: sanitizeText(data.methodId || "", 120),
    methodName: sanitizeText(data.methodName || data.methodId || "", 80),
    orderType: getOrderType(data),
    source: sanitizeText(data.source || "", 80),
    createdAtMs: safeSignedInt(data.createdAtMs),
    approvedAtMs: safeSignedInt(data.approvedAtMs || data.reviewResolvedAtMs || data.fundingSettledAtMs),
    bonusDoesAwarded: safeInt(data.bonusDoesAwarded),
    agentAssisted: data.agentAssisted === true,
  };
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
  const url = new URL(PUBLIC_HOME_URL);
  url.searchParams.set("amb", normalized);
  return url.toString();
}

function buildUserReferralLink(referralCode) {
  const normalized = normalizeCode(referralCode);
  if (!normalized) return "";
  const url = new URL(PUBLIC_HOME_URL);
  url.searchParams.set("ref", normalized);
  return url.toString();
}

function buildAgentPromoLink(promoCode) {
  const normalized = normalizeCode(promoCode);
  if (!normalized) return "";
  const url = new URL(PUBLIC_HOME_URL);
  url.searchParams.set("promo", normalized);
  return url.toString();
}

function normalizeAgentStatus(value = "") {
  return sanitizeText(value || "", 24).toLowerCase() === "active" ? "active" : "inactive";
}

function agentsCollection() {
  return db.collection(AGENTS_COLLECTION);
}

function agentRef(uid = "") {
  return agentsCollection().doc(String(uid || "").trim());
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

async function agentPromoCodeExists(code, currentUid = "") {
  const normalized = normalizeCode(code);
  if (!normalized) return false;
  const snap = await agentsCollection()
    .where("promoCode", "==", normalized)
    .limit(1)
    .get();
  if (snap.empty) return false;
  const found = snap.docs[0];
  return !found || found.id !== String(currentUid || "");
}

async function generateUniqueAgentPromoCode(currentUid = "") {
  for (let i = 0; i < 40; i += 1) {
    const candidate = `${AGENT_PROMO_PREFIX}${randomCode(6)}`;
    if (!(await agentPromoCodeExists(candidate, currentUid))) return candidate;
  }
  throw new HttpsError("aborted", "Impossible de générer un code agent unique.");
}

function buildAgentSearchRecord(clientId = "", raw = {}) {
  const status = normalizeAgentStatus(raw.agentStatus || "");
  return {
    id: String(clientId || raw.uid || "").trim(),
    uid: String(raw.uid || clientId || "").trim(),
    name: sanitizeText(raw.name || raw.displayName || raw.username || "", 120),
    username: sanitizeUsername(raw.username || "", 24),
    email: sanitizeEmail(raw.email || "", 160),
    phone: sanitizePhone(raw.phone || "", 40),
    createdAtMs: safeSignedInt(raw.createdAtMs),
    lastSeenAtMs: safeSignedInt(raw.lastSeenAtMs),
    isAgent: raw.isAgent === true,
    agentStatus: status,
    agentPromoCode: normalizeCode(raw.agentPromoCode || ""),
    agentDashboardEnabled: raw.agentDashboardEnabled === true,
    accountFrozen: raw.accountFrozen === true,
  };
}

function buildAgentProfileSummary(uid = "", clientData = {}, agentData = {}) {
  const promoCode = normalizeCode(agentData.promoCode || clientData.agentPromoCode || "");
  const status = normalizeAgentStatus(agentData.status || clientData.agentStatus || "");
  const currentMonthKey = getMonthKeyFromMs(Date.now());
  const currentEarningsMonthKey = sanitizeText(agentData.currentEarningsMonthKey || "", 16);
  const displayName = sanitizeText(
    agentData.displayName
    || clientData.name
    || clientData.displayName
    || clientData.username
    || "",
    120
  );
  const activatedAtMs = safeSignedInt(agentData.activatedAtMs || clientData.agentActivatedAtMs);
  return {
    id: String(uid || clientData.uid || "").trim(),
    uid: String(uid || clientData.uid || "").trim(),
    displayName,
    username: sanitizeUsername(clientData.username || "", 24),
    email: sanitizeEmail(clientData.email || "", 160),
    phone: sanitizePhone(clientData.phone || "", 40),
    status,
    isActive: status === "active",
    promoCode,
    promoLink: buildAgentPromoLink(promoCode),
    signupBudgetInitialHtg: resolveAgentSignupBudgetInitialHtg(agentData, clientData),
    signupBudgetRemainingHtg: resolveAgentSignupBudgetRemainingHtg(agentData, clientData),
    currentMonthEarnedDoes: currentEarningsMonthKey === currentMonthKey ? safeInt(agentData.currentMonthEarnedDoes) : 0,
    lifetimeEarnedDoes: safeInt(agentData.lifetimeEarnedDoes),
    lastPayrollMonthKey: sanitizeText(agentData.lastPayrollMonthKey || "", 16),
    totalTrackedSignups: safeInt(agentData.totalTrackedSignups),
    totalTrackedDeposits: safeInt(agentData.totalTrackedDeposits),
    totalTrackedWins: safeInt(agentData.totalTrackedWins),
    declaredAtMs: safeSignedInt(agentData.declaredAtMs || clientData.agentDeclaredAtMs),
    activatedAtMs,
    createdAtMs: safeSignedInt(agentData.createdAtMs || clientData.createdAtMs),
    updatedAtMs: safeSignedInt(agentData.updatedAtMs || clientData.updatedAtMs || clientData.lastSeenAtMs),
    lastSeenAtMs: safeSignedInt(clientData.lastSeenAtMs),
  };
}

function buildAgentLedgerItem(docSnap) {
  const data = docSnap?.data() || {};
  return {
    id: String(docSnap?.id || "").trim(),
    type: sanitizeText(data.type || "", 40),
    label: sanitizeText(data.label || data.type || "", 120),
    deltaDoes: safeSignedInt(data.deltaDoes),
    deltaHtg: safeSignedInt(data.deltaHtg),
    monthKey: sanitizeText(data.monthKey || "", 16),
    createdAtMs: safeSignedInt(data.createdAtMs),
    createdByUid: sanitizeText(data.createdByUid || "", 160),
  };
}

function buildAgentMonthlyStatement(docSnap) {
  const data = docSnap?.data() || {};
  return {
    id: String(docSnap?.id || "").trim(),
    monthKey: sanitizeText(data.monthKey || docSnap?.id || "", 16),
    earnedDoes: safeInt(data.earnedDoes),
    paidDoes: safeInt(data.paidDoes),
    signupsCount: safeInt(data.signupsCount),
    depositsCount: safeInt(data.depositsCount),
    winsCount: safeInt(data.winsCount),
    closedAtMs: safeSignedInt(data.closedAtMs),
    createdAtMs: safeSignedInt(data.createdAtMs),
    signupBudgetRemainingHtg: safeInt(data.signupBudgetRemainingHtg),
  };
}

function getMonthKeyFromMs(value = Date.now()) {
  const date = new Date(Number(value) || Date.now());
  const year = String(date.getUTCFullYear());
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  return `${year}-${month}`;
}

function getPreviousMonthKeyFromMs(value = Date.now()) {
  const date = new Date(Number(value) || Date.now());
  date.setUTCDate(1);
  date.setUTCHours(0, 0, 0, 0);
  date.setUTCMonth(date.getUTCMonth() - 1);
  return getMonthKeyFromMs(date.getTime());
}

function normalizeMonthKey(value = "", fallbackValue = "") {
  const safeValue = sanitizeText(value || "", 16);
  if (/^\d{4}-\d{2}$/.test(safeValue)) return safeValue;
  return fallbackValue ? normalizeMonthKey(fallbackValue, "") : "";
}

function resolveAgentSignupBudgetInitialHtg(agentData = {}, clientData = {}) {
  const activatedAtMs = safeSignedInt(agentData.activatedAtMs || clientData.agentActivatedAtMs);
  const status = normalizeAgentStatus(agentData.status || clientData.agentStatus || "");
  if (typeof agentData.signupBudgetInitialHtg === "number") {
    const storedValue = safeInt(agentData.signupBudgetInitialHtg);
    if (activatedAtMs > 0 || status === "active") {
      return storedValue > 0 ? storedValue : AGENT_INITIAL_SIGNUP_BUDGET_HTG;
    }
    return 0;
  }
  return activatedAtMs > 0 ? AGENT_INITIAL_SIGNUP_BUDGET_HTG : 0;
}

function resolveAgentSignupBudgetRemainingHtg(agentData = {}, clientData = {}) {
  const activatedAtMs = safeSignedInt(agentData.activatedAtMs || clientData.agentActivatedAtMs);
  const status = normalizeAgentStatus(agentData.status || clientData.agentStatus || "");
  if (typeof agentData.signupBudgetRemainingHtg === "number") {
    const storedValue = safeInt(agentData.signupBudgetRemainingHtg);
    if (activatedAtMs > 0 || status === "active") {
      return storedValue;
    }
    return 0;
  }
  return resolveAgentSignupBudgetInitialHtg(agentData, clientData);
}

function getAgentCommissionDoesForWin(gameType = "", stakeDoes = 0) {
  const safeType = sanitizeText(gameType || "", 40);
  const safeStakeDoes = safeInt(stakeDoes);
  const gameMatrix = AGENT_COMMISSION_MATRIX[safeType];
  if (!gameMatrix) return 0;
  return safeInt(gameMatrix[safeStakeDoes]);
}

function normalizeRewardSettlementSplit(options = {}) {
  const rewardAmountDoes = safeInt(options.rewardAmountDoes);
  const provisionalRewardDoes = Math.min(
    rewardAmountDoes,
    safeInt(options.provisionalRewardDoes)
  );
  const approvedRewardDoes = Math.max(0, rewardAmountDoes - provisionalRewardDoes);
  const welcomeRewardDoes = Math.min(
    approvedRewardDoes,
    safeInt(options.welcomeRewardDoes)
  );

  return {
    approvedRewardDoes,
    provisionalRewardDoes,
    welcomeRewardDoes,
  };
}

function buildAgentCommissionLedgerId(gameType = "", roomId = "", playerUid = "") {
  const safeType = sanitizeText(gameType || "", 32).toLowerCase().replace(/[^a-z0-9_]/g, "") || "game";
  const safeRoomId = sanitizeText(roomId || "", 160).replace(/[^a-zA-Z0-9_-]/g, "") || "room";
  const safePlayerUid = sanitizeText(playerUid || "", 160).replace(/[^a-zA-Z0-9_-]/g, "") || "player";
  return `${safeType}_${safeRoomId}_${safePlayerUid}`;
}

function buildAgentDepositLedgerId(clientUid = "") {
  const safeClientUid = sanitizeText(clientUid || "", 160).replace(/[^a-zA-Z0-9_-]/g, "") || "client";
  return `deposit_${safeClientUid}`;
}

function buildAgentReferralClientRecord(docSnap) {
  const data = docSnap?.data() || {};
  return {
    id: String(docSnap?.id || data.uid || "").trim(),
    uid: String(data.uid || docSnap?.id || "").trim(),
    name: sanitizeText(data.name || data.displayName || data.username || "", 120),
    username: sanitizeUsername(data.username || "", 24),
    email: sanitizeEmail(data.email || "", 160),
    phone: sanitizePhone(data.phone || "", 40),
    createdAtMs: safeSignedInt(data.createdAtMs),
    lastSeenAtMs: safeSignedInt(data.lastSeenAtMs),
    hasApprovedDeposit: data.hasApprovedDeposit === true,
    doesBalance: safeInt(data.doesBalance),
    referredByAgentUid: sanitizeText(data.referredByAgentUid || "", 160),
    referredByAgentCode: normalizeCode(data.referredByAgentCode || ""),
  };
}

function normalizeWhatsappDigits(value = "") {
  const digits = phoneDigits(value);
  if (!digits) return "";
  if (digits.startsWith("00")) return digits.slice(2, 17);
  return digits.slice(0, 15);
}

function isValidWhatsappDigits(value = "") {
  const digits = normalizeWhatsappDigits(value);
  return digits.length >= 8 && digits.length <= 15;
}

function formatWhatsappDisplayNumber(value = "") {
  const digits = normalizeWhatsappDigits(value);
  if (!digits) return "";
  return `+${digits}`;
}

function buildPrivatePlayerAlias(seed = "") {
  const source = String(seed || "").trim();
  if (!source) return "Joueur ID-000000";
  let hash = 0;
  for (let index = 0; index < source.length; index += 1) {
    hash = ((hash * 31) + source.charCodeAt(index)) % 1000000;
  }
  const normalizedHash = Math.max(0, Math.abs(hash));
  const safeCode = ((normalizedHash % 900000) + 100000);
  return `Joueur ID-${String(safeCode).padStart(6, "0")}`;
}

function isClientOnlineFromPresence(clientData = {}, nowMs = Date.now()) {
  const expiresAtMs = safeSignedInt(clientData.sitePresenceExpiresAtMs);
  if (expiresAtMs > nowMs) return true;
  return (nowMs - safeSignedInt(clientData.lastSeenAtMs)) <= CLIENT_SITE_PRESENCE_WINDOW_MS;
}

function buildMorpionWhatsappContactRecord(docSnap, nowMs = Date.now()) {
  const data = docSnap?.data() || {};
  const digits = normalizeWhatsappDigits(data.morpionWhatsappDigits || data.morpionWhatsappNumber || "");
  if (!digits || data.morpionWhatsappVisible !== true) return null;
  const lastInterestAtMs = safeSignedInt(data.morpionLastInterestAtMs);
  return {
    uid: String(docSnap?.id || data.uid || "").trim(),
    label: buildPrivatePlayerAlias(docSnap?.id || data.uid || ""),
    whatsappNumber: formatWhatsappDisplayNumber(digits),
    whatsappDigits: digits,
    online: isClientOnlineFromPresence(data, nowMs),
    sitePresencePage: sanitizeText(data.sitePresencePage || "", 40).toLowerCase(),
    lastInterestAtMs,
    lastSeenAtMs: safeSignedInt(data.lastSeenAtMs),
  };
}

async function awardAgentCommissionForClientWinTx(tx, options = {}) {
  const playerUid = String(options.playerUid || "").trim();
  const gameType = sanitizeText(options.gameType || "", 40);
  const roomId = String(options.roomId || "").trim();
  const stakeDoes = safeInt(options.stakeDoes);
  const rewardDoes = safeInt(options.rewardDoes);
  const wonAtMs = safeSignedInt(options.wonAtMs) || Date.now();

  if (!playerUid || !gameType || !roomId || stakeDoes <= 0 || rewardDoes <= 0) {
    return { awarded: false, reason: "invalid_context", commissionDoes: 0 };
  }

  const playerRef = walletRef(playerUid);
  const playerSnap = await tx.get(playerRef);
  if (!playerSnap.exists) {
    return { awarded: false, reason: "player_not_found", commissionDoes: 0 };
  }

  const playerData = playerSnap.data() || {};
  const agentUid = sanitizeText(playerData.referredByAgentUid || "", 160);
  if (!agentUid) {
    return { awarded: false, reason: "no_agent", commissionDoes: 0 };
  }

  const commissionDoes = getAgentCommissionDoesForWin(gameType, stakeDoes);
  const targetAgentRef = agentRef(agentUid);
  const monthKey = getMonthKeyFromMs(wonAtMs);
  const ledgerId = buildAgentCommissionLedgerId(gameType, roomId, playerUid);
  const ledgerRef = targetAgentRef.collection(AGENT_LEDGER_SUBCOLLECTION).doc(ledgerId);
  const monthlyRef = targetAgentRef.collection(AGENT_MONTHLY_STATEMENTS_SUBCOLLECTION).doc(monthKey);

  const [agentSnap, ledgerSnap, monthlySnap] = await Promise.all([
    tx.get(targetAgentRef),
    tx.get(ledgerRef),
    tx.get(monthlyRef),
  ]);

  if (!agentSnap.exists) {
    return { awarded: false, reason: "agent_not_found", commissionDoes: 0 };
  }

  const agentData = agentSnap.data() || {};
  if (normalizeAgentStatus(agentData.status || "") !== "active") {
    return { awarded: false, reason: "agent_inactive", commissionDoes: 0 };
  }

  if (ledgerSnap.exists) {
    return { awarded: false, reason: "already_recorded", commissionDoes: safeInt((ledgerSnap.data() || {}).deltaDoes) };
  }

  const currentEarningsMonthKey = sanitizeText(agentData.currentEarningsMonthKey || "", 16);
  const baseCurrentMonthEarnedDoes = currentEarningsMonthKey === monthKey
    ? safeInt(agentData.currentMonthEarnedDoes)
    : 0;
  const nextCurrentMonthEarnedDoes = baseCurrentMonthEarnedDoes + commissionDoes;
  const nextLifetimeEarnedDoes = safeInt(agentData.lifetimeEarnedDoes) + commissionDoes;
  const nextWinsCount = safeInt(agentData.totalTrackedWins) + 1;
  const monthlyData = monthlySnap.exists ? (monthlySnap.data() || {}) : {};

  tx.set(targetAgentRef, {
    currentMonthEarnedDoes: nextCurrentMonthEarnedDoes,
    currentEarningsMonthKey: monthKey,
    lifetimeEarnedDoes: nextLifetimeEarnedDoes,
    totalTrackedWins: nextWinsCount,
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    updatedAtMs: wonAtMs,
  }, { merge: true });

  tx.set(monthlyRef, {
    monthKey,
    earnedDoes: safeInt(monthlyData.earnedDoes) + commissionDoes,
    paidDoes: safeInt(monthlyData.paidDoes),
    signupsCount: safeInt(monthlyData.signupsCount),
    depositsCount: safeInt(monthlyData.depositsCount),
    winsCount: safeInt(monthlyData.winsCount) + 1,
    signupBudgetRemainingHtg: resolveAgentSignupBudgetRemainingHtg(agentData),
    createdAt: monthlySnap.exists ? (monthlyData.createdAt || admin.firestore.FieldValue.serverTimestamp()) : admin.firestore.FieldValue.serverTimestamp(),
    createdAtMs: safeSignedInt(monthlyData.createdAtMs) || wonAtMs,
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    updatedAtMs: wonAtMs,
  }, { merge: true });

  tx.set(ledgerRef, {
    type: "game_commission",
    label: `Commission ${gameType} gagne`,
    deltaDoes: commissionDoes,
    deltaHtg: 0,
    monthKey,
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
    createdAtMs: wonAtMs,
    createdByUid: agentUid,
    linkedClientUid: playerUid,
    linkedRoomId: roomId,
    linkedGameType: gameType,
    linkedStakeDoes: stakeDoes,
    linkedRewardDoes: rewardDoes,
  }, { merge: true });

  return {
    awarded: commissionDoes > 0,
    reason: commissionDoes > 0 ? "awarded" : "tracked_zero_commission",
    commissionDoes,
    agentUid,
    monthKey,
  };
}

async function awardAgentCommissionForClientWin(options = {}) {
  return db.runTransaction(async (tx) => awardAgentCommissionForClientWinTx(tx, options));
}

async function trackAgentDepositApprovalTx(tx, options = {}) {
  const clientUid = String(options.clientUid || "").trim();
  const approvedAtMs = safeSignedInt(options.approvedAtMs) || Date.now();
  const orderId = sanitizeText(options.orderId || "", 160);
  const amountHtg = safeInt(options.amountHtg);

  if (!clientUid) {
    return { tracked: false, reason: "invalid_client" };
  }

  const clientRef = walletRef(clientUid);
  const clientSnap = await tx.get(clientRef);
  if (!clientSnap.exists) {
    return { tracked: false, reason: "client_not_found" };
  }

  const clientData = clientSnap.data() || {};
  const agentUid = sanitizeText(clientData.referredByAgentUid || "", 160);
  if (!agentUid) {
    return { tracked: false, reason: "no_agent" };
  }
  if (clientData.hasApprovedDeposit === true || safeSignedInt(clientData.agentFirstApprovedDepositTrackedAtMs) > 0) {
    return { tracked: false, reason: "already_tracked", agentUid };
  }

  const targetAgentRef = agentRef(agentUid);
  const monthKey = getMonthKeyFromMs(approvedAtMs);
  const ledgerRef = targetAgentRef.collection(AGENT_LEDGER_SUBCOLLECTION).doc(buildAgentDepositLedgerId(clientUid));
  const monthlyRef = targetAgentRef.collection(AGENT_MONTHLY_STATEMENTS_SUBCOLLECTION).doc(monthKey);

  const [agentSnap, ledgerSnap, monthlySnap] = await Promise.all([
    tx.get(targetAgentRef),
    tx.get(ledgerRef),
    tx.get(monthlyRef),
  ]);

  if (!agentSnap.exists) {
    return { tracked: false, reason: "agent_not_found", agentUid };
  }
  if (ledgerSnap.exists) {
    return { tracked: false, reason: "already_recorded", agentUid };
  }

  const agentData = agentSnap.data() || {};
  const monthlyData = monthlySnap.exists ? (monthlySnap.data() || {}) : {};
  const nextDepositsCount = safeInt(agentData.totalTrackedDeposits) + 1;

  tx.set(clientRef, {
    agentFirstApprovedDepositTrackedAtMs: approvedAtMs,
    agentFirstApprovedDepositTrackedOrderId: orderId,
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  }, { merge: true });

  tx.set(targetAgentRef, {
    totalTrackedDeposits: nextDepositsCount,
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    updatedAtMs: approvedAtMs,
  }, { merge: true });

  tx.set(monthlyRef, {
    monthKey,
    earnedDoes: safeInt(monthlyData.earnedDoes),
    paidDoes: safeInt(monthlyData.paidDoes),
    signupsCount: safeInt(monthlyData.signupsCount),
    depositsCount: safeInt(monthlyData.depositsCount) + 1,
    winsCount: safeInt(monthlyData.winsCount),
    signupBudgetRemainingHtg: resolveAgentSignupBudgetRemainingHtg(agentData),
    createdAt: monthlySnap.exists ? (monthlyData.createdAt || admin.firestore.FieldValue.serverTimestamp()) : admin.firestore.FieldValue.serverTimestamp(),
    createdAtMs: safeSignedInt(monthlyData.createdAtMs) || approvedAtMs,
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    updatedAtMs: approvedAtMs,
  }, { merge: true });

  tx.set(ledgerRef, {
    type: "deposit_tracked",
    label: "Premier depot approuve d'un filleul",
    deltaDoes: 0,
    deltaHtg: 0,
    monthKey,
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
    createdAtMs: approvedAtMs,
    createdByUid: agentUid,
    linkedClientUid: clientUid,
    linkedOrderId: orderId,
    linkedAmountHtg: amountHtg,
  }, { merge: true });

  return {
    tracked: true,
    reason: "tracked",
    agentUid,
    monthKey,
  };
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

async function findAgentByPromoCode(code) {
  const normalized = normalizeCode(code);
  if (!normalized) return null;
  const snap = await agentsCollection()
    .where("promoCode", "==", normalized)
    .limit(1)
    .get();
  return snap.empty ? null : snap.docs[0];
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

async function applyAgentPromoAttribution(options = {}) {
  const uid = String(options.uid || "").trim();
  const promoCode = normalizeCode(options.promoCode || "");
  const via = String(options.via || "").toLowerCase() === "link" ? "link" : "promo";

  if (!uid || !promoCode) {
    return { applied: false, reason: "no_candidate" };
  }

  const agentSnap = await findAgentByPromoCode(promoCode);
  if (!agentSnap || agentSnap.id === uid) {
    return { applied: false, reason: "invalid_or_self" };
  }

  const agentData = agentSnap.data() || {};
  if (normalizeAgentStatus(agentData.status || "") !== "active") {
    return { applied: false, reason: "agent_inactive" };
  }

  const clientRef = walletRef(uid);
  const targetAgentRef = agentRef(agentSnap.id);
  const ledgerRef = targetAgentRef.collection(AGENT_LEDGER_SUBCOLLECTION).doc();
  const nowMs = Date.now();
  const monthKey = getMonthKeyFromMs(nowMs);
  const monthlyRef = targetAgentRef.collection(AGENT_MONTHLY_STATEMENTS_SUBCOLLECTION).doc(monthKey);

  return db.runTransaction(async (tx) => {
    const [clientSnap, liveAgentSnap, monthlySnap] = await Promise.all([
      tx.get(clientRef),
      tx.get(targetAgentRef),
      tx.get(monthlyRef),
    ]);

    if (!clientSnap.exists) {
      return { applied: false, reason: "client_not_found" };
    }
    if (!liveAgentSnap.exists || liveAgentSnap.id === uid) {
      return { applied: false, reason: "invalid_or_self" };
    }

    const clientData = clientSnap.data() || {};
    const liveAgentData = liveAgentSnap.data() || {};
    const monthlyData = monthlySnap.exists ? (monthlySnap.data() || {}) : {};
    if (
      clientData.referredByType
      || clientData.referredByUserId
      || clientData.referredByAmbassadorId
      || clientData.referredByAgentUid
    ) {
      return { applied: false, reason: "already_set" };
    }
    if (normalizeCode(clientData.referralCode || "") === promoCode) {
      return { applied: false, reason: "invalid_or_self" };
    }
    if (normalizeAgentStatus(liveAgentData.status || "") !== "active") {
      return { applied: false, reason: "agent_inactive" };
    }

    const remainingBefore = resolveAgentSignupBudgetRemainingHtg(liveAgentData);
    const remainingAfter = Math.max(0, remainingBefore - WELCOME_BONUS_HTG_AMOUNT);

    tx.set(clientRef, {
      referredByType: "agent",
      referredByCode: promoCode,
      referredVia: via,
      referredAt: admin.firestore.FieldValue.serverTimestamp(),
      referredByAgentUid: liveAgentSnap.id,
      referredByAgentCode: promoCode,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });

    tx.set(targetAgentRef, {
      totalTrackedSignups: safeInt(liveAgentData.totalTrackedSignups) + 1,
      signupBudgetRemainingHtg: remainingAfter,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAtMs: nowMs,
    }, { merge: true });

    tx.set(monthlyRef, {
      monthKey,
      earnedDoes: safeInt(monthlyData.earnedDoes),
      paidDoes: safeInt(monthlyData.paidDoes),
      signupsCount: safeInt(monthlyData.signupsCount) + 1,
      depositsCount: safeInt(monthlyData.depositsCount),
      winsCount: safeInt(monthlyData.winsCount),
      signupBudgetRemainingHtg: remainingAfter,
      createdAt: monthlySnap.exists ? (monthlyData.createdAt || admin.firestore.FieldValue.serverTimestamp()) : admin.firestore.FieldValue.serverTimestamp(),
      createdAtMs: safeSignedInt(monthlyData.createdAtMs) || nowMs,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAtMs: nowMs,
    }, { merge: true });

    tx.set(ledgerRef, {
      type: "signup_cost",
      label: "Bonus inscription rattache a un agent",
      deltaDoes: 0,
      deltaHtg: -WELCOME_BONUS_HTG_AMOUNT,
      monthKey,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      createdAtMs: nowMs,
      createdByUid: liveAgentSnap.id,
      linkedClientUid: uid,
      linkedPromoCode: promoCode,
      signupBudgetRemainingHtg: remainingAfter,
    }, { merge: true });

    return {
      applied: true,
      targetType: "agent",
      targetId: liveAgentSnap.id,
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

  if (promoCode.startsWith(AGENT_PROMO_PREFIX)) {
    return applyAgentPromoAttribution(options);
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

  const agentAttempt = await applyAgentPromoAttribution(options);
  if (agentAttempt.applied) return agentAttempt;

  if (!AMBASSADOR_SYSTEM_ENABLED) {
    return agentAttempt.reason === "no_candidate" ? userAttempt : agentAttempt;
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

function surveysCollection() {
  return db.collection(SURVEYS_COLLECTION);
}

function surveyRef(surveyId) {
  return surveysCollection().doc(String(surveyId || "").trim());
}

function surveyResponsesCollection(surveyId) {
  return surveyRef(surveyId).collection(SURVEY_RESPONSES_SUBCOLLECTION);
}

function surveyResponseRef(surveyId, uid) {
  return surveyResponsesCollection(surveyId).doc(String(uid || "").trim());
}

function normalizeSurveyStatus(value, fallback = "draft") {
  const normalized = sanitizeText(value || "", 24).toLowerCase();
  return ["draft", "live", "closed", "deleted"].includes(normalized) ? normalized : fallback;
}

function tsFieldToMs(value) {
  if (!value) return 0;
  if (typeof value?.toMillis === "function") {
    try {
      return value.toMillis();
    } catch (_) {
      return 0;
    }
  }
  if (typeof value?.toDate === "function") {
    try {
      return value.toDate().getTime();
    } catch (_) {
      return 0;
    }
  }
  if (typeof value?.seconds === "number") return value.seconds * 1000;
  if (typeof value === "number") return Number.isFinite(value) ? value : 0;
  const parsed = Date.parse(String(value));
  return Number.isFinite(parsed) ? parsed : 0;
}

function normalizeSurveyChoiceId(value, fallbackIndex = 0) {
  const normalized = sanitizeText(value || "", 40)
    .toLowerCase()
    .replace(/[^a-z0-9_-]/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_+|_+$/g, "");
  return normalized || `choice_${fallbackIndex + 1}`;
}

function normalizeSurveyChoices(rawChoices = []) {
  const out = [];
  const usedIds = new Set();
  const source = Array.isArray(rawChoices) ? rawChoices : [];

  for (let index = 0; index < source.length; index += 1) {
    const raw = source[index];
    const label = sanitizeText(
      typeof raw === "string" ? raw : raw?.label || raw?.text || raw?.value || "",
      SURVEY_MAX_CHOICE_LABEL,
    );
    if (!label) continue;
    let id = normalizeSurveyChoiceId(typeof raw === "string" ? "" : raw?.id, index);
    while (usedIds.has(id)) {
      id = `${id}_${out.length + 1}`;
    }
    usedIds.add(id);
    out.push({ id, label });
    if (out.length >= SURVEY_MAX_CHOICES) break;
  }

  return out;
}

function normalizeSurveyPayload(payload = {}, existing = {}) {
  const title = sanitizeText(payload.title ?? existing.title ?? "", SURVEY_MAX_TITLE);
  const description = sanitizeText(
    payload.description ?? existing.description ?? "",
    SURVEY_MAX_DESCRIPTION,
  );
  const allowChoiceAnswer = payload.allowChoiceAnswer === undefined
    ? existing.allowChoiceAnswer !== false
    : payload.allowChoiceAnswer === true;
  const allowTextAnswer = payload.allowTextAnswer === undefined
    ? existing.allowTextAnswer === true
    : payload.allowTextAnswer === true;
  const choices = normalizeSurveyChoices(payload.choices ?? existing.choices ?? []);
  const status = normalizeSurveyStatus(payload.status, normalizeSurveyStatus(existing.status || "draft", "draft"));

  if (!title) {
    throw new HttpsError("invalid-argument", "Le titre du sondage est obligatoire.", {
      code: "survey-title-required",
    });
  }
  if (!allowChoiceAnswer && !allowTextAnswer) {
    throw new HttpsError("invalid-argument", "Active au moins un mode de réponse.", {
      code: "survey-answer-mode-required",
    });
  }
  if (allowChoiceAnswer && choices.length < 2) {
    throw new HttpsError("invalid-argument", "Ajoute au moins 2 choix pour les réponses guidées.", {
      code: "survey-choices-required",
    });
  }

  return {
    title,
    description,
    allowChoiceAnswer,
    allowTextAnswer,
    choices,
    status,
  };
}

function buildSurveySummary(docSnap) {
  const data = docSnap.data() || {};
  return {
    id: docSnap.id,
    title: sanitizeText(data.title || "", SURVEY_MAX_TITLE),
    description: sanitizeText(data.description || "", SURVEY_MAX_DESCRIPTION),
    allowChoiceAnswer: data.allowChoiceAnswer !== false,
    allowTextAnswer: data.allowTextAnswer === true,
    choices: normalizeSurveyChoices(data.choices || []),
    status: normalizeSurveyStatus(data.status || "draft", "draft"),
    version: Math.max(1, safeInt(data.version) || 1),
    responseCount: safeInt(data.responseCount),
    createdAtMs: tsFieldToMs(data.createdAt) || safeSignedInt(data.createdAtMs),
    updatedAtMs: tsFieldToMs(data.updatedAt) || safeSignedInt(data.updatedAtMs),
    publishedAtMs: tsFieldToMs(data.publishedAt) || safeSignedInt(data.publishedAtMs),
    closedAtMs: tsFieldToMs(data.closedAt) || safeSignedInt(data.closedAtMs),
    deletedAtMs: tsFieldToMs(data.deletedAt) || safeSignedInt(data.deletedAtMs),
    lastResponseAtMs: tsFieldToMs(data.lastResponseAt) || safeSignedInt(data.lastResponseAtMs),
  };
}

async function loadClientSurveySnapshot(uid, fallbackEmail = "") {
  const safeUid = String(uid || "").trim();
  if (!safeUid) {
    return {
      uid: "",
      displayName: "",
      email: sanitizeEmail(fallbackEmail, 160),
      phone: "",
    };
  }

  const clientSnap = await walletRef(safeUid).get();
  const data = clientSnap.exists ? (clientSnap.data() || {}) : {};
  return {
    uid: safeUid,
    displayName: sanitizeText(data.name || data.displayName || data.username || "", 120),
    email: sanitizeEmail(data.email || fallbackEmail || "", 160),
    phone: sanitizePhone(data.phone || data.customerPhone || "", 40),
  };
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
    const data = snap.data() || {};
    const mode = normalizeBotPilotMode(data.botPilotMode || "manual");
    if (mode === "auto") {
      return normalizeBotDifficulty(data.autoBotDifficulty || data.botDifficulty);
    }
    return normalizeBotDifficulty(data.manualBotDifficulty || data.botDifficulty);
  } catch (_) {
    return DEFAULT_BOT_DIFFICULTY;
  }
}

async function getConfiguredDuelBotDifficulty() {
  try {
    const snap = await adminBootstrapRef().get();
    if (!snap.exists) return DEFAULT_DUEL_BOT_DIFFICULTY;
    const data = snap.data() || {};
    const mode = normalizeBotPilotMode(data.duelBotPilotMode || "manual");
    if (mode === "auto") {
      return normalizeBotDifficulty(data.autoDuelBotDifficulty || data.duelBotDifficulty || DEFAULT_DUEL_BOT_DIFFICULTY);
    }
    return normalizeBotDifficulty(data.manualDuelBotDifficulty || data.duelBotDifficulty || DEFAULT_DUEL_BOT_DIFFICULTY);
  } catch (_) {
    return DEFAULT_DUEL_BOT_DIFFICULTY;
  }
}

async function getConfiguredMorpionMatchmakingPolicy() {
  return {
    mode: "manual",
    allowHumanOnly: true,
  };
}

function normalizeBotPilotMode(value = "") {
  const normalized = String(value || "").trim().toLowerCase();
  return BOT_PILOT_MODES.has(normalized) ? normalized : "manual";
}

function normalizeBotPilotWindow(value = "") {
  const normalized = String(value || "").trim().toLowerCase();
  return normalized === "today" || normalized === "24h" || normalized === "7d"
    ? normalized
    : "today";
}

function getBotPilotRange(windowKey = "today", nowMs = Date.now()) {
  const normalized = normalizeBotPilotWindow(windowKey);
  if (normalized === "24h") {
    return { windowKey: normalized, startMs: nowMs - BOT_PILOT_WINDOW_MS, endMs: nowMs };
  }
  if (normalized === "7d") {
    return { windowKey: normalized, startMs: nowMs - (7 * BOT_PILOT_WINDOW_MS), endMs: nowMs };
  }
  const now = new Date(nowMs);
  const startMs = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime();
  return { windowKey: "today", startMs, endMs: nowMs };
}

function getBotPilotDayKey(ms = 0) {
  if (!ms) return "";
  const date = new Date(ms);
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function getBotPilotHourKey(ms = 0) {
  if (!ms) return "";
  const date = new Date(ms);
  const hour = String(date.getHours()).padStart(2, "0");
  return `${getBotPilotDayKey(ms)} ${hour}:00`;
}

function getBotPilotTrendKey(windowKey = "today", ms = 0) {
  const normalized = normalizeBotPilotWindow(windowKey);
  return normalized === "7d" ? getBotPilotDayKey(ms) : getBotPilotHourKey(ms);
}

function getBotPilotTrendLabel(windowKey = "today", ms = 0) {
  if (!ms) return "-";
  const normalized = normalizeBotPilotWindow(windowKey);
  const date = new Date(ms);
  if (normalized === "7d") {
    return `${String(date.getDate()).padStart(2, "0")}/${String(date.getMonth() + 1).padStart(2, "0")}`;
  }
  return `${String(date.getHours()).padStart(2, "0")}h`;
}

function normalizeMorpionPilotMode(value = "") {
  const normalized = String(value || "").trim().toLowerCase();
  return MORPION_PILOT_MODES.has(normalized) ? normalized : "manual";
}

function normalizeMorpionPilotWindow(value = "") {
  const normalized = String(value || "").trim().toLowerCase();
  return normalized === "today" || normalized === "24h" || normalized === "7d"
    ? normalized
    : "today";
}

function getMorpionPilotRange(windowKey = "today", nowMs = Date.now()) {
  const normalized = normalizeMorpionPilotWindow(windowKey);
  if (normalized === "24h") {
    return { windowKey: normalized, startMs: nowMs - MORPION_PILOT_WINDOW_MS, endMs: nowMs };
  }
  if (normalized === "7d") {
    return { windowKey: normalized, startMs: nowMs - (7 * MORPION_PILOT_WINDOW_MS), endMs: nowMs };
  }
  const now = new Date(nowMs);
  const startMs = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime();
  return { windowKey: "today", startMs, endMs: nowMs };
}

function getMorpionPilotTrendKey(windowKey = "today", ms = 0) {
  const normalized = normalizeMorpionPilotWindow(windowKey);
  return normalized === "7d" ? getBotPilotDayKey(ms) : getBotPilotHourKey(ms);
}

function getMorpionPilotTrendLabel(windowKey = "today", ms = 0) {
  if (!ms) return "-";
  const normalized = normalizeMorpionPilotWindow(windowKey);
  const date = new Date(ms);
  if (normalized === "7d") {
    return `${String(date.getDate()).padStart(2, "0")}/${String(date.getMonth() + 1).padStart(2, "0")}`;
  }
  return `${String(date.getHours()).padStart(2, "0")}h`;
}

function normalizeMorpionPilotDecision(value = "") {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "bot_only_temp") return "bot_only_temp";
  return "normal";
}

function normalizeAcquisitionGranularity(value = "", rangeMs = 0) {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "hour" && rangeMs > 0 && rangeMs <= (7 * 24 * 60 * 60 * 1000)) {
    return "hour";
  }
  return "day";
}

function normalizeAcquisitionRange(options = {}, nowMs = Date.now()) {
  const safeNow = safeSignedInt(nowMs) || Date.now();
  let endMs = safeSignedInt(options.endMs ?? options.dateToMs ?? options.toMs) || safeNow;
  if (endMs <= 0 || endMs > safeNow) endMs = safeNow;

  let startMs = safeSignedInt(options.startMs ?? options.dateFromMs ?? options.fromMs)
    || (endMs - ACQUISITION_DEFAULT_WINDOW_MS);
  if (startMs <= 0 || startMs >= endMs) {
    startMs = endMs - ACQUISITION_DEFAULT_WINDOW_MS;
  }

  if ((endMs - startMs) > ACQUISITION_MAX_WINDOW_MS) {
    startMs = endMs - ACQUISITION_MAX_WINDOW_MS;
  }

  const rangeMs = Math.max(1, endMs - startMs);
  const granularity = normalizeAcquisitionGranularity(
    options.granularity || options.bucket || options.resolution,
    rangeMs
  );

  return {
    startMs,
    endMs,
    rangeMs,
    granularity,
  };
}

function getUtcDayKey(ms = 0) {
  if (!ms) return "";
  const date = new Date(ms);
  return `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, "0")}-${String(date.getUTCDate()).padStart(2, "0")}`;
}

function getUtcHourKey(ms = 0) {
  if (!ms) return "";
  const date = new Date(ms);
  return `${getUtcDayKey(ms)} ${String(date.getUTCHours()).padStart(2, "0")}:00`;
}

function getAcquisitionBucketSizeMs(granularity = "day") {
  return granularity === "hour" ? (60 * 60 * 1000) : (24 * 60 * 60 * 1000);
}

function getAcquisitionBucketStartMs(ms = 0, granularity = "day") {
  const safeMs = safeSignedInt(ms);
  if (!safeMs) return 0;
  const bucketSizeMs = getAcquisitionBucketSizeMs(granularity);
  return safeMs - (safeMs % bucketSizeMs);
}

function getAcquisitionBucketKey(ms = 0, granularity = "day") {
  if (!ms) return "";
  return granularity === "hour" ? getUtcHourKey(ms) : getUtcDayKey(ms);
}

function getAcquisitionBucketLabel(ms = 0, granularity = "day") {
  if (!ms) return "-";
  const date = new Date(ms);
  const dd = String(date.getUTCDate()).padStart(2, "0");
  const mm = String(date.getUTCMonth() + 1).padStart(2, "0");
  if (granularity === "hour") {
    const hh = String(date.getUTCHours()).padStart(2, "0");
    return `${dd}/${mm} ${hh}h`;
  }
  return `${dd}/${mm}`;
}

function buildAcquisitionBucketSeed(startMs = 0, endMs = 0, granularity = "day") {
  const bucketSizeMs = getAcquisitionBucketSizeMs(granularity);
  const firstBucketStartMs = getAcquisitionBucketStartMs(startMs, granularity);
  const lastBucketStartMs = getAcquisitionBucketStartMs(endMs, granularity);
  const buckets = [];

  for (let cursor = firstBucketStartMs; cursor <= lastBucketStartMs; cursor += bucketSizeMs) {
    buckets.push({
      startMs: cursor,
      key: getAcquisitionBucketKey(cursor, granularity),
      label: getAcquisitionBucketLabel(cursor, granularity),
      signups: 0,
      depositingSignups: 0,
      activeSignups: 0,
      fidelizedSignups: 0,
      welcomeBonusSignups: 0,
      frozenSignups: 0,
      cumulativeAccounts: 0,
    });
  }

  return buckets;
}

async function getAggregationCount(query) {
  const aggregateSnap = await query.count().get();
  const data = typeof aggregateSnap?.data === "function" ? (aggregateSnap.data() || {}) : {};
  return safeInt(data.count);
}

async function fetchClientSignupRowsForRange(startMs = 0, endMs = 0) {
  const rows = [];
  let lastDoc = null;
  let truncated = false;

  while (rows.length < ACQUISITION_DOC_LIMIT) {
    let query = db.collection(CLIENTS_COLLECTION)
      .where("createdAtMs", ">=", startMs)
      .where("createdAtMs", "<=", endMs)
      .orderBy("createdAtMs", "asc")
      .select(
        "createdAtMs",
        "lastSeenAtMs",
        "hasApprovedDeposit",
        "welcomeBonusClaimed",
        "accountFrozen"
      )
      .limit(Math.min(ACQUISITION_PAGE_FETCH_SIZE, ACQUISITION_DOC_LIMIT - rows.length));

    if (lastDoc) {
      query = query.startAfter(lastDoc);
    }

    const snap = await query.get();
    if (snap.empty) break;

    snap.forEach((docSnap) => {
      rows.push(docSnap.data() || {});
    });

    lastDoc = snap.docs[snap.docs.length - 1] || null;
    if (snap.size < ACQUISITION_PAGE_FETCH_SIZE) break;
  }

  if (lastDoc) {
    const moreSnap = await db.collection(CLIENTS_COLLECTION)
      .where("createdAtMs", ">=", startMs)
      .where("createdAtMs", "<=", endMs)
      .orderBy("createdAtMs", "asc")
      .startAfter(lastDoc)
      .limit(1)
      .get();
    truncated = !moreSnap.empty;
  }

  return { rows, truncated };
}

async function computeClientAcquisitionSnapshot(options = {}) {
  const nowMs = safeSignedInt(options.nowMs) || Date.now();
  const range = normalizeAcquisitionRange(options, nowMs);
  const activeCutoffMs = Math.max(0, range.endMs - ACQUISITION_ACTIVE_LOOKBACK_MS);
  const clientsCollection = db.collection(CLIENTS_COLLECTION);

  const [
    totalAccounts,
    accountsBeforeWindow,
    activeAccounts,
    realClients,
    frozenAccounts,
    signupRowsResult,
  ] = await Promise.all([
    getAggregationCount(clientsCollection),
    getAggregationCount(clientsCollection.where("createdAtMs", "<", range.startMs)),
    getAggregationCount(
      clientsCollection
        .where("lastSeenAtMs", ">=", activeCutoffMs)
        .where("lastSeenAtMs", "<=", range.endMs)
    ),
    getAggregationCount(clientsCollection.where("hasApprovedDeposit", "==", true)),
    getAggregationCount(clientsCollection.where("accountFrozen", "==", true)),
    fetchClientSignupRowsForRange(range.startMs, range.endMs),
  ]);

  const bucketSeed = buildAcquisitionBucketSeed(range.startMs, range.endMs, range.granularity);
  const bucketMap = new Map(bucketSeed.map((item) => [item.key, item]));
  let signupsCount = 0;
  let depositingSignupsCount = 0;
  let activeSignupsCount = 0;
  let fidelizedSignupsCount = 0;
  let welcomeBonusSignupsCount = 0;
  let frozenSignupsCount = 0;

  signupRowsResult.rows.forEach((row) => {
    const createdAtMs = safeSignedInt(row.createdAtMs) || toMillis(row.createdAt);
    if (createdAtMs < range.startMs || createdAtMs > range.endMs) return;

    const lastSeenAtMs = safeSignedInt(row.lastSeenAtMs) || toMillis(row.lastSeenAt);
    const hasApprovedDeposit = row.hasApprovedDeposit === true;
    const welcomeBonusClaimed = row.welcomeBonusClaimed === true;
    const accountFrozen = row.accountFrozen === true;
    const isActive = lastSeenAtMs >= activeCutoffMs;
    const isFidelized = hasApprovedDeposit && lastSeenAtMs >= (createdAtMs + ACQUISITION_FIDELITY_MIN_AGE_MS);
    const bucket = bucketMap.get(getAcquisitionBucketKey(createdAtMs, range.granularity));
    if (!bucket) return;

    signupsCount += 1;
    bucket.signups += 1;

    if (hasApprovedDeposit) {
      depositingSignupsCount += 1;
      bucket.depositingSignups += 1;
    }
    if (isActive) {
      activeSignupsCount += 1;
      bucket.activeSignups += 1;
    }
    if (isFidelized) {
      fidelizedSignupsCount += 1;
      bucket.fidelizedSignups += 1;
    }
    if (welcomeBonusClaimed) {
      welcomeBonusSignupsCount += 1;
      bucket.welcomeBonusSignups += 1;
    }
    if (accountFrozen) {
      frozenSignupsCount += 1;
      bucket.frozenSignups += 1;
    }
  });

  let runningAccounts = accountsBeforeWindow;
  const buckets = bucketSeed.map((bucket) => {
    runningAccounts += safeInt(bucket.signups);
    const signups = safeInt(bucket.signups);
    const depositingSignups = safeInt(bucket.depositingSignups);
    const activeSignups = safeInt(bucket.activeSignups);
    const fidelizedSignups = safeInt(bucket.fidelizedSignups);
    return {
      startMs: safeSignedInt(bucket.startMs),
      key: String(bucket.key || ""),
      label: String(bucket.label || "-"),
      signups,
      depositingSignups,
      activeSignups,
      fidelizedSignups,
      welcomeBonusSignups: safeInt(bucket.welcomeBonusSignups),
      frozenSignups: safeInt(bucket.frozenSignups),
      signupToDepositRatePct: signups > 0 ? Number(((depositingSignups / signups) * 100).toFixed(2)) : 0,
      signupToActiveRatePct: signups > 0 ? Number(((activeSignups / signups) * 100).toFixed(2)) : 0,
      signupToFidelizedRatePct: signups > 0 ? Number(((fidelizedSignups / signups) * 100).toFixed(2)) : 0,
      cumulativeAccounts: runningAccounts,
    };
  });

  return {
    generatedAtMs: nowMs,
    timezone: "UTC",
    window: {
      startMs: range.startMs,
      endMs: range.endMs,
      rangeMs: range.rangeMs,
      granularity: range.granularity,
    },
    definitions: {
      activeLookbackDays: Math.round(ACQUISITION_ACTIVE_LOOKBACK_MS / (24 * 60 * 60 * 1000)),
      fidelizedMinAgeDays: Math.round(ACQUISITION_FIDELITY_MIN_AGE_MS / (24 * 60 * 60 * 1000)),
      fidelizedRule: "Compte avec vrai depot approuve et retour constate au moins 3 jours apres l'inscription.",
      cohortScope: "Les taux de conversion et de fidelisation portent sur les comptes inscrits dans la periode choisie.",
    },
    summary: {
      totalAccounts,
      accountsBeforeWindow,
      signupsCount,
      activeAccounts,
      realClients,
      frozenAccounts,
      depositingSignupsCount,
      activeSignupsCount,
      fidelizedSignupsCount,
      welcomeBonusSignupsCount,
      frozenSignupsCount,
      activeRatePct: totalAccounts > 0 ? Number(((activeAccounts / totalAccounts) * 100).toFixed(2)) : 0,
      realClientRatePct: totalAccounts > 0 ? Number(((realClients / totalAccounts) * 100).toFixed(2)) : 0,
      signupToDepositRatePct: signupsCount > 0 ? Number(((depositingSignupsCount / signupsCount) * 100).toFixed(2)) : 0,
      signupToActiveRatePct: signupsCount > 0 ? Number(((activeSignupsCount / signupsCount) * 100).toFixed(2)) : 0,
      signupToFidelizedRatePct: signupsCount > 0 ? Number(((fidelizedSignupsCount / signupsCount) * 100).toFixed(2)) : 0,
    },
    series: {
      signups: buckets.map((item) => ({ startMs: item.startMs, label: item.label, value: item.signups })),
      cumulativeAccounts: buckets.map((item) => ({ startMs: item.startMs, label: item.label, value: item.cumulativeAccounts })),
      depositingSignups: buckets.map((item) => ({ startMs: item.startMs, label: item.label, value: item.depositingSignups })),
      activeSignups: buckets.map((item) => ({ startMs: item.startMs, label: item.label, value: item.activeSignups })),
      fidelizedSignups: buckets.map((item) => ({ startMs: item.startMs, label: item.label, value: item.fidelizedSignups })),
    },
    buckets,
    truncated: signupRowsResult.truncated === true,
    scannedSignupDocs: safeInt(signupRowsResult.rows.length),
    scanLimit: ACQUISITION_DOC_LIMIT,
  };
}

function normalizeDepositAnalyticsGranularity(rawValue = "", rangeMs = DEPOSIT_ANALYTICS_DEFAULT_WINDOW_MS) {
  const raw = String(rawValue || "").trim().toLowerCase();
  if (raw === "hour" || raw === "day" || raw === "week") return raw;
  if (rangeMs <= (3 * 24 * 60 * 60 * 1000)) return "hour";
  if (rangeMs <= (75 * 24 * 60 * 60 * 1000)) return "day";
  return "week";
}

function normalizeDepositAnalyticsRange(options = {}, nowMs = Date.now()) {
  const safeNow = safeSignedInt(nowMs) || Date.now();
  let endMs = safeSignedInt(options.endMs ?? options.dateToMs ?? options.toMs) || safeNow;
  if (endMs <= 0 || endMs > safeNow) endMs = safeNow;

  let startMs = safeSignedInt(options.startMs ?? options.dateFromMs ?? options.fromMs)
    || (endMs - DEPOSIT_ANALYTICS_DEFAULT_WINDOW_MS);
  if (startMs <= 0 || startMs >= endMs) {
    startMs = endMs - DEPOSIT_ANALYTICS_DEFAULT_WINDOW_MS;
  }

  if ((endMs - startMs) > DEPOSIT_ANALYTICS_MAX_WINDOW_MS) {
    startMs = endMs - DEPOSIT_ANALYTICS_MAX_WINDOW_MS;
  }

  const rangeMs = Math.max(1, endMs - startMs);
  return {
    startMs,
    endMs,
    rangeMs,
    granularity: normalizeDepositAnalyticsGranularity(
      options.granularity || options.bucket || options.resolution,
      rangeMs
    ),
  };
}

function getUtcWeekStartMs(ms = 0) {
  const safeMs = safeSignedInt(ms);
  if (!safeMs) return 0;
  const date = new Date(safeMs);
  const weekday = date.getUTCDay();
  const mondayOffset = weekday === 0 ? -6 : (1 - weekday);
  return Date.UTC(
    date.getUTCFullYear(),
    date.getUTCMonth(),
    date.getUTCDate() + mondayOffset,
    0, 0, 0, 0
  );
}

function getDepositAnalyticsBucketStartMs(ms = 0, granularity = "day") {
  const safeMs = safeSignedInt(ms);
  if (!safeMs) return 0;
  if (granularity === "hour") return getAcquisitionBucketStartMs(safeMs, "hour");
  if (granularity === "week") return getUtcWeekStartMs(safeMs);
  return getAcquisitionBucketStartMs(safeMs, "day");
}

function getDepositAnalyticsBucketKey(ms = 0, granularity = "day") {
  if (!ms) return "";
  if (granularity === "hour") return getUtcHourKey(ms);
  if (granularity === "week") {
    const startMs = getUtcWeekStartMs(ms);
    return `W:${getUtcDayKey(startMs)}`;
  }
  return getUtcDayKey(ms);
}

function getDepositAnalyticsBucketLabel(ms = 0, granularity = "day") {
  if (!ms) return "-";
  const date = new Date(ms);
  const dd = String(date.getUTCDate()).padStart(2, "0");
  const mm = String(date.getUTCMonth() + 1).padStart(2, "0");
  if (granularity === "hour") {
    const hh = String(date.getUTCHours()).padStart(2, "0");
    return `${dd}/${mm} ${hh}h`;
  }
  if (granularity === "week") {
    const end = new Date(ms + ((7 * 24 * 60 * 60 * 1000) - 1));
    const endDd = String(end.getUTCDate()).padStart(2, "0");
    const endMm = String(end.getUTCMonth() + 1).padStart(2, "0");
    return `${dd}/${mm} - ${endDd}/${endMm}`;
  }
  return `${dd}/${mm}`;
}

function buildDepositAnalyticsBucketSeed(startMs = 0, endMs = 0, granularity = "day") {
  const firstBucketStartMs = getDepositAnalyticsBucketStartMs(startMs, granularity);
  const lastBucketStartMs = getDepositAnalyticsBucketStartMs(endMs, granularity);
  const buckets = [];
  let cursor = firstBucketStartMs;

  while (cursor <= lastBucketStartMs) {
    buckets.push({
      startMs: cursor,
      key: getDepositAnalyticsBucketKey(cursor, granularity),
      label: getDepositAnalyticsBucketLabel(cursor, granularity),
      requestedCount: 0,
      approvedCount: 0,
      rejectedCount: 0,
      pendingCount: 0,
      requestedHtg: 0,
      approvedHtg: 0,
      rejectedHtg: 0,
      pendingHtg: 0,
      moncashRequestedHtg: 0,
      natcashRequestedHtg: 0,
      otherRequestedHtg: 0,
      moncashApprovedHtg: 0,
      natcashApprovedHtg: 0,
      otherApprovedHtg: 0,
      moncashRejectedHtg: 0,
      natcashRejectedHtg: 0,
      otherRejectedHtg: 0,
      cumulativeApprovedHtg: 0,
    });

    cursor += granularity === "hour"
      ? (60 * 60 * 1000)
      : granularity === "week"
        ? (7 * 24 * 60 * 60 * 1000)
        : (24 * 60 * 60 * 1000);
  }

  return buckets;
}

function normalizeDepositAnalyticsMethod(order = {}) {
  if (isWelcomeBonusOrder(order)) return "welcome_bonus";
  const raw = `${String(order?.methodId || "")} ${String(order?.methodName || "")}`.trim().toLowerCase();
  if (!raw) return "other";
  if (/mon\s*cash/.test(raw) || raw.includes("moncash")) return "moncash";
  if (/nat\s*cash/.test(raw) || raw.includes("natcash")) return "natcash";
  return "other";
}

async function fetchDepositAnalyticsRowsForRange(startMs = 0, endMs = 0) {
  console.info("[DEPOSIT_ANALYTICS_DEBUG] fetch:start", {
    startMs: safeSignedInt(startMs),
    endMs: safeSignedInt(endMs),
    limit: DEPOSIT_ANALYTICS_DOC_LIMIT + 1,
  });

  const probes = [
    {
      name: "collection-group-base",
      run: () => db.collectionGroup("orders").limit(1).get(),
    },
    {
      name: "createdAtMs-gte",
      run: () => db.collectionGroup("orders")
        .where("createdAtMs", ">=", startMs)
        .limit(1)
        .get(),
    },
    {
      name: "createdAtMs-range",
      run: () => db.collectionGroup("orders")
        .where("createdAtMs", ">=", startMs)
        .where("createdAtMs", "<=", endMs)
        .limit(1)
        .get(),
    },
  ];

  for (const probe of probes) {
    try {
      const probeSnap = await probe.run();
      console.info("[DEPOSIT_ANALYTICS_DEBUG] probe:ok", {
        name: probe.name,
        size: safeInt(probeSnap?.size),
      });
    } catch (error) {
      console.error("[DEPOSIT_ANALYTICS_DEBUG] probe:error", {
        name: probe.name,
        message: String(error?.message || error),
        code: String(error?.code || ""),
        details: error?.details || "",
        stack: String(error?.stack || ""),
      });
      throw error;
    }
  }

  const querySnap = await db.collectionGroup("orders")
    .where("createdAtMs", ">=", startMs)
    .where("createdAtMs", "<=", endMs)
    .select(
      "amount",
      "items",
      "status",
      "resolutionStatus",
      "approvedAmountHtg",
      "fundingVersion",
      "creditMode",
      "createdAtMs",
      "createdAt",
      "methodId",
      "methodName",
      "orderType",
      "kind"
    )
    .limit(DEPOSIT_ANALYTICS_DOC_LIMIT + 1)
    .get();

  console.info("[DEPOSIT_ANALYTICS_DEBUG] fetch:done", {
    size: safeInt(querySnap.size),
    truncated: querySnap.size > DEPOSIT_ANALYTICS_DOC_LIMIT,
  });

  const rows = querySnap.docs
    .slice(0, DEPOSIT_ANALYTICS_DOC_LIMIT)
    .map((docSnap) => docSnap.data() || {});

  return {
    rows,
    truncated: querySnap.size > DEPOSIT_ANALYTICS_DOC_LIMIT,
  };
}

async function computeDepositAnalyticsSnapshot(options = {}) {
  const nowMs = safeSignedInt(options.nowMs) || Date.now();
  const range = normalizeDepositAnalyticsRange(options, nowMs);
  console.info("[DEPOSIT_ANALYTICS_DEBUG] snapshot:start", {
    nowMs,
    startMs: range.startMs,
    endMs: range.endMs,
    rangeMs: range.rangeMs,
    granularity: range.granularity,
  });
  const rowsResult = await fetchDepositAnalyticsRowsForRange(range.startMs, range.endMs);
  const bucketSeed = buildDepositAnalyticsBucketSeed(range.startMs, range.endMs, range.granularity);
  const bucketMap = new Map(bucketSeed.map((item) => [item.key, item]));

  const summary = {
    requestedCount: 0,
    approvedCount: 0,
    rejectedCount: 0,
    pendingCount: 0,
    requestedHtg: 0,
    approvedHtg: 0,
    rejectedHtg: 0,
    pendingHtg: 0,
    moncashRequestedHtg: 0,
    moncashApprovedHtg: 0,
    moncashRejectedHtg: 0,
    moncashRequestedCount: 0,
    moncashApprovedCount: 0,
    moncashRejectedCount: 0,
    natcashRequestedHtg: 0,
    natcashApprovedHtg: 0,
    natcashRejectedHtg: 0,
    natcashRequestedCount: 0,
    natcashApprovedCount: 0,
    natcashRejectedCount: 0,
    otherRequestedHtg: 0,
    otherApprovedHtg: 0,
    otherRejectedHtg: 0,
    otherRequestedCount: 0,
    otherApprovedCount: 0,
    otherRejectedCount: 0,
  };

  rowsResult.rows.forEach((row) => {
    if (isWelcomeBonusOrder(row)) return;
    const createdAtMs = safeSignedInt(row.createdAtMs) || toMillis(row.createdAt);
    if (createdAtMs < range.startMs || createdAtMs > range.endMs) return;

    const bucket = bucketMap.get(getDepositAnalyticsBucketKey(createdAtMs, range.granularity));
    if (!bucket) return;

    const method = normalizeDepositAnalyticsMethod(row);
    const requestedHtg = Math.max(0, computeOrderAmount(row));
    const approvedHtg = Math.max(0, getOrderApprovedRealAmountHtg(row));
    const resolution = getOrderResolutionStatus(row);
    const rejectedHtg = resolution === "rejected" ? requestedHtg : 0;
    const pendingHtg = resolution === "pending" ? requestedHtg : 0;

    summary.requestedCount += 1;
    summary.requestedHtg += requestedHtg;
    bucket.requestedCount += 1;
    bucket.requestedHtg += requestedHtg;

    if (resolution === "approved") {
      summary.approvedCount += 1;
      summary.approvedHtg += approvedHtg;
      bucket.approvedCount += 1;
      bucket.approvedHtg += approvedHtg;
    } else if (resolution === "rejected") {
      summary.rejectedCount += 1;
      summary.rejectedHtg += rejectedHtg;
      bucket.rejectedCount += 1;
      bucket.rejectedHtg += rejectedHtg;
    } else {
      summary.pendingCount += 1;
      summary.pendingHtg += pendingHtg;
      bucket.pendingCount += 1;
      bucket.pendingHtg += pendingHtg;
    }

    if (method === "moncash") {
      summary.moncashRequestedCount += 1;
      summary.moncashRequestedHtg += requestedHtg;
      bucket.moncashRequestedHtg += requestedHtg;
      if (resolution === "approved") {
        summary.moncashApprovedCount += 1;
        summary.moncashApprovedHtg += approvedHtg;
        bucket.moncashApprovedHtg += approvedHtg;
      } else if (resolution === "rejected") {
        summary.moncashRejectedCount += 1;
        summary.moncashRejectedHtg += rejectedHtg;
        bucket.moncashRejectedHtg += rejectedHtg;
      }
    } else if (method === "natcash") {
      summary.natcashRequestedCount += 1;
      summary.natcashRequestedHtg += requestedHtg;
      bucket.natcashRequestedHtg += requestedHtg;
      if (resolution === "approved") {
        summary.natcashApprovedCount += 1;
        summary.natcashApprovedHtg += approvedHtg;
        bucket.natcashApprovedHtg += approvedHtg;
      } else if (resolution === "rejected") {
        summary.natcashRejectedCount += 1;
        summary.natcashRejectedHtg += rejectedHtg;
        bucket.natcashRejectedHtg += rejectedHtg;
      }
    } else {
      summary.otherRequestedCount += 1;
      summary.otherRequestedHtg += requestedHtg;
      bucket.otherRequestedHtg += requestedHtg;
      if (resolution === "approved") {
        summary.otherApprovedCount += 1;
        summary.otherApprovedHtg += approvedHtg;
        bucket.otherApprovedHtg += approvedHtg;
      } else if (resolution === "rejected") {
        summary.otherRejectedCount += 1;
        summary.otherRejectedHtg += rejectedHtg;
        bucket.otherRejectedHtg += rejectedHtg;
      }
    }
  });

  let cumulativeApprovedHtg = 0;
  const buckets = bucketSeed.map((bucket) => {
    cumulativeApprovedHtg += safeInt(bucket.approvedHtg);
    return {
      startMs: safeSignedInt(bucket.startMs),
      key: String(bucket.key || ""),
      label: String(bucket.label || "-"),
      requestedCount: safeInt(bucket.requestedCount),
      approvedCount: safeInt(bucket.approvedCount),
      rejectedCount: safeInt(bucket.rejectedCount),
      pendingCount: safeInt(bucket.pendingCount),
      requestedHtg: safeInt(bucket.requestedHtg),
      approvedHtg: safeInt(bucket.approvedHtg),
      rejectedHtg: safeInt(bucket.rejectedHtg),
      pendingHtg: safeInt(bucket.pendingHtg),
      moncashRequestedHtg: safeInt(bucket.moncashRequestedHtg),
      natcashRequestedHtg: safeInt(bucket.natcashRequestedHtg),
      otherRequestedHtg: safeInt(bucket.otherRequestedHtg),
      moncashApprovedHtg: safeInt(bucket.moncashApprovedHtg),
      natcashApprovedHtg: safeInt(bucket.natcashApprovedHtg),
      otherApprovedHtg: safeInt(bucket.otherApprovedHtg),
      moncashRejectedHtg: safeInt(bucket.moncashRejectedHtg),
      natcashRejectedHtg: safeInt(bucket.natcashRejectedHtg),
      otherRejectedHtg: safeInt(bucket.otherRejectedHtg),
      approvalRatePct: safeInt(bucket.requestedHtg) > 0
        ? Number(((safeInt(bucket.approvedHtg) / safeInt(bucket.requestedHtg)) * 100).toFixed(2))
        : 0,
      cumulativeApprovedHtg,
    };
  });

  const approvedRatePct = summary.requestedHtg > 0
    ? Number(((summary.approvedHtg / summary.requestedHtg) * 100).toFixed(2))
    : 0;
  const rejectedRatePct = summary.requestedHtg > 0
    ? Number(((summary.rejectedHtg / summary.requestedHtg) * 100).toFixed(2))
    : 0;
  const moncashApprovedSharePct = summary.approvedHtg > 0
    ? Number(((summary.moncashApprovedHtg / summary.approvedHtg) * 100).toFixed(2))
    : 0;
  const natcashApprovedSharePct = summary.approvedHtg > 0
    ? Number(((summary.natcashApprovedHtg / summary.approvedHtg) * 100).toFixed(2))
    : 0;

  console.info("[DEPOSIT_ANALYTICS_DEBUG] snapshot:done", {
    scannedOrderDocs: safeInt(rowsResult.rows.length),
    truncated: rowsResult.truncated === true,
    requestedCount: safeInt(summary.requestedCount),
    approvedCount: safeInt(summary.approvedCount),
    rejectedCount: safeInt(summary.rejectedCount),
    pendingCount: safeInt(summary.pendingCount),
  });

  return {
    generatedAtMs: nowMs,
    timezone: "UTC",
    window: {
      startMs: range.startMs,
      endMs: range.endMs,
      rangeMs: range.rangeMs,
      granularity: range.granularity,
    },
    definitions: {
      inflowRule: "Les entrees HTG de l'entreprise correspondent aux montants de depots reels approuves.",
      rejectionRule: "Les bonus bienvenue sont exclus. Les montants rejetes reprennent le montant demande sur la commande.",
      source: "Source: collectionGroup(orders), excluant welcome_bonus.",
    },
    summary: {
      ...summary,
      approvedRatePct,
      rejectedRatePct,
      moncashApprovedSharePct,
      natcashApprovedSharePct,
    },
    series: {
      requestedHtg: buckets.map((item) => ({ startMs: item.startMs, label: item.label, value: item.requestedHtg })),
      approvedHtg: buckets.map((item) => ({ startMs: item.startMs, label: item.label, value: item.approvedHtg })),
      rejectedHtg: buckets.map((item) => ({ startMs: item.startMs, label: item.label, value: item.rejectedHtg })),
      cumulativeApprovedHtg: buckets.map((item) => ({ startMs: item.startMs, label: item.label, value: item.cumulativeApprovedHtg })),
      moncashApprovedHtg: buckets.map((item) => ({ startMs: item.startMs, label: item.label, value: item.moncashApprovedHtg })),
      natcashApprovedHtg: buckets.map((item) => ({ startMs: item.startMs, label: item.label, value: item.natcashApprovedHtg })),
      approvalsVsRejects: buckets.map((item) => ({
        startMs: item.startMs,
        label: item.label,
        approvedHtg: item.approvedHtg,
        rejectedHtg: item.rejectedHtg,
      })),
    },
    buckets,
    scannedOrderDocs: safeInt(rowsResult.rows.length),
    truncated: rowsResult.truncated === true,
    scanLimit: DEPOSIT_ANALYTICS_DOC_LIMIT,
  };
}

function chooseAutoBotDifficulty(snapshot = {}) {
  const netDoes = safeSignedInt(snapshot.netDoes);
  const collectedDoes = safeInt(snapshot.collectedDoes);
  const marginPct = collectedDoes > 0 ? (netDoes / collectedDoes) : 0;
  const drawdownDoes = Math.max(0, safeInt(snapshot.drawdownDoes));
  const drawdownPct = Math.max(0, Number(snapshot.drawdownPct || 0));
  const highWaterMarkDoes = Math.max(0, safeInt(snapshot.highWaterMarkDoes));
  const isNearPeak = highWaterMarkDoes <= 0 || drawdownPct <= 0.02;

  if (collectedDoes <= 0) {
    return {
      level: DEFAULT_BOT_DIFFICULTY,
      band: "neutral",
      reason: "no_volume",
      marginPct: 0,
    };
  }
  if (drawdownPct >= 0.18 || (drawdownDoes >= 400 && drawdownPct >= 0.12)) {
    return { level: "ultra", band: "danger", reason: "drawdown_critical", marginPct, drawdownPct };
  }
  if (netDoes < 0 || marginPct < 0.03) {
    return { level: "ultra", band: "danger", reason: "margin_too_low", marginPct, drawdownPct };
  }
  if (drawdownPct >= 0.08) {
    return { level: "expert", band: "defense", reason: "drawdown_high", marginPct, drawdownPct };
  }
  if (marginPct < 0.08) {
    return { level: "expert", band: "defense", reason: "margin_low", marginPct, drawdownPct };
  }
  if (!isNearPeak) {
    return { level: "amateur", band: "equilibrium", reason: "recovery_guard", marginPct, drawdownPct };
  }
  if (marginPct < 0.16) {
    return { level: "amateur", band: "equilibrium", reason: "margin_ok", marginPct, drawdownPct };
  }
  return { level: "userpro", band: "comfort", reason: "new_high_comfort", marginPct, drawdownPct };
}

function chooseAutoDuelBotDifficulty(snapshot = {}) {
  const roomsCount = safeInt(snapshot.roomsCount);
  const netDoes = safeSignedInt(snapshot.netDoes);
  const collectedDoes = safeInt(snapshot.collectedDoes);
  const marginPct = collectedDoes > 0 ? (netDoes / collectedDoes) : 0;
  const drawdownDoes = Math.max(0, safeInt(snapshot.drawdownDoes));
  const drawdownPct = Math.max(0, Number(snapshot.drawdownPct || 0));
  const botWinRatePct = Math.max(0, Number(snapshot.botWinRatePct || 0));
  const humanWinRatePct = Math.max(0, Number(snapshot.humanWinRatePct || 0));

  if (collectedDoes <= 0) {
    return {
      level: DEFAULT_DUEL_BOT_DIFFICULTY,
      band: "neutral",
      reason: "no_volume",
      marginPct: 0,
      drawdownPct: 0,
      botWinRatePct,
      humanWinRatePct,
    };
  }
  if (roomsCount < 8) {
    return {
      level: DEFAULT_DUEL_BOT_DIFFICULTY,
      band: "neutral",
      reason: "low_volume",
      marginPct,
      drawdownPct,
      botWinRatePct,
      humanWinRatePct,
    };
  }
  if (drawdownPct >= 0.16 || (drawdownDoes >= 250 && drawdownPct >= 0.1)) {
    return { level: "ultra", band: "danger", reason: "drawdown_critical", marginPct, drawdownPct, botWinRatePct, humanWinRatePct };
  }
  if (netDoes < 0 || marginPct < 0.04) {
    return { level: "ultra", band: "danger", reason: "margin_too_low", marginPct, drawdownPct, botWinRatePct, humanWinRatePct };
  }
  if (roomsCount >= 12 && humanWinRatePct >= 0.66) {
    return { level: "ultra", band: "defense", reason: "human_overperforming", marginPct, drawdownPct, botWinRatePct, humanWinRatePct };
  }
  if (botWinRatePct >= 0.82 && marginPct >= 0.18 && drawdownPct <= 0.03) {
    return { level: "userpro", band: "comfort", reason: "bot_overwhelming", marginPct, drawdownPct, botWinRatePct, humanWinRatePct };
  }
  if (botWinRatePct >= 0.74 && marginPct >= 0.12) {
    return { level: "amateur", band: "comfort", reason: "bot_too_strong", marginPct, drawdownPct, botWinRatePct, humanWinRatePct };
  }
  if (drawdownPct >= 0.08 || marginPct < 0.08) {
    return { level: "expert", band: "defense", reason: drawdownPct >= 0.08 ? "drawdown_high" : "margin_low", marginPct, drawdownPct, botWinRatePct, humanWinRatePct };
  }
  if (marginPct >= 0.14 && botWinRatePct <= 0.58 && drawdownPct <= 0.04) {
    return { level: "amateur", band: "equilibrium", reason: "margin_healthy", marginPct, drawdownPct, botWinRatePct, humanWinRatePct };
  }

  return { level: "expert", band: "equilibrium", reason: "stable", marginPct, drawdownPct, botWinRatePct, humanWinRatePct };
}

async function computeBotPilotSnapshot(options = {}) {
  const nowMs = safeSignedInt(options.nowMs) || Date.now();
  const range = getBotPilotRange(options.window || "today", nowMs);
  const querySnap = await db.collection(ROOM_RESULTS_COLLECTION)
    .where("endedAtMs", ">=", range.startMs)
    .orderBy("endedAtMs", "desc")
    .limit(BOT_PILOT_SNAPSHOT_LIMIT)
    .get();

  let roomsCount = 0;
  let collectedDoes = 0;
  let payoutDoes = 0;
  let netDoes = 0;
  let grossCollectedDoes = 0;
  let grossPayoutDoes = 0;
  let grossNetDoes = 0;
  let promoExposureDoes = 0;
  let botRooms = 0;
  let humanWins = 0;
  let botWins = 0;
  let truncated = querySnap.size >= BOT_PILOT_SNAPSHOT_LIMIT;
  const trendMap = new Map();
  const botMixMap = new Map([
    ["0", { botCount: 0, rooms: 0, netDoes: 0, botWins: 0, humanWins: 0 }],
    ["1", { botCount: 1, rooms: 0, netDoes: 0, botWins: 0, humanWins: 0 }],
    ["2", { botCount: 2, rooms: 0, netDoes: 0, botWins: 0, humanWins: 0 }],
    ["3", { botCount: 3, rooms: 0, netDoes: 0, botWins: 0, humanWins: 0 }],
  ]);

  querySnap.forEach((docSnap) => {
    const data = docSnap.data() || {};
    const endedAtMs = safeSignedInt(data.endedAtMs);
    if (endedAtMs < range.startMs || endedAtMs > range.endMs) return;
    if (String(data.status || "").trim().toLowerCase() !== "ended") return;

    roomsCount += 1;
    const roomCollectedGross = safeInt(data.companyCollectedDoes);
    const roomPayoutGross = safeInt(data.companyPayoutDoes);
    const roomNetGross = safeSignedInt(
      typeof data.companyNetDoes === "number"
        ? data.companyNetDoes
        : (roomCollectedGross - roomPayoutGross)
    );
    const roomCollected = safeInt(
      typeof data.companyCollectedRealDoes === "number"
        ? data.companyCollectedRealDoes
        : roomCollectedGross
    );
    const roomPayout = safeInt(
      typeof data.companyPayoutRealDoes === "number"
        ? data.companyPayoutRealDoes
        : roomPayoutGross
    );
    const roomNet = safeSignedInt(
      typeof data.companyNetRealDoes === "number"
        ? data.companyNetRealDoes
        : (roomCollected - roomPayout)
    );
    const roomPromoExposure = safeInt(data.companyPromoExposureDoes);

    grossCollectedDoes += roomCollectedGross;
    grossPayoutDoes += roomPayoutGross;
    grossNetDoes += roomNetGross;
    promoExposureDoes += roomPromoExposure;
    collectedDoes += roomCollected;
    payoutDoes += roomPayout;
    netDoes += roomNet;

    const botCount = safeInt(data.botCount);
    if (botCount > 0) botRooms += 1;
    const winnerType = String(data.winnerType || "").trim().toLowerCase();
    if (winnerType === "human") humanWins += 1;
    else if (winnerType === "bot") botWins += 1;

     const trendKey = getBotPilotTrendKey(range.windowKey, endedAtMs);
     const existingTrend = trendMap.get(trendKey) || {
       key: trendKey,
       label: getBotPilotTrendLabel(range.windowKey, endedAtMs),
       periodMs: endedAtMs,
       rooms: 0,
       collectedDoes: 0,
       payoutDoes: 0,
       netDoes: 0,
       grossCollectedDoes: 0,
       grossPayoutDoes: 0,
       grossNetDoes: 0,
       promoExposureDoes: 0,
     };
     existingTrend.rooms += 1;
     existingTrend.collectedDoes += roomCollected;
     existingTrend.payoutDoes += roomPayout;
     existingTrend.netDoes += roomNet;
     existingTrend.grossCollectedDoes += roomCollectedGross;
     existingTrend.grossPayoutDoes += roomPayoutGross;
     existingTrend.grossNetDoes += roomNetGross;
     existingTrend.promoExposureDoes += roomPromoExposure;
     if (endedAtMs > safeSignedInt(existingTrend.periodMs)) {
       existingTrend.periodMs = endedAtMs;
       existingTrend.label = getBotPilotTrendLabel(range.windowKey, endedAtMs);
     }
     trendMap.set(trendKey, existingTrend);

     const mixKey = String(Math.min(botCount, 3));
     const mix = botMixMap.get(mixKey) || {
       botCount: Math.min(botCount, 3),
       rooms: 0,
       netDoes: 0,
       botWins: 0,
       humanWins: 0,
     };
     mix.rooms += 1;
     mix.netDoes += roomNet;
     if (winnerType === "bot") mix.botWins += 1;
     if (winnerType === "human") mix.humanWins += 1;
     botMixMap.set(mixKey, mix);
  });

  const fullTrend = Array.from(trendMap.values())
    .sort((a, b) => safeSignedInt(a.periodMs) - safeSignedInt(b.periodMs))
    .map((item) => ({
      key: item.key,
      label: item.label,
      periodMs: safeSignedInt(item.periodMs),
      rooms: safeInt(item.rooms),
      collectedDoes: safeInt(item.collectedDoes),
      payoutDoes: safeInt(item.payoutDoes),
      netDoes: safeSignedInt(item.netDoes),
      grossCollectedDoes: safeInt(item.grossCollectedDoes),
      grossPayoutDoes: safeInt(item.grossPayoutDoes),
      grossNetDoes: safeSignedInt(item.grossNetDoes),
      promoExposureDoes: safeInt(item.promoExposureDoes),
    }));
  let runningEquityDoes = 0;
  let highWaterMarkDoes = 0;
  let lastPeakAtMs = range.startMs;
  const fullEquityCurve = [{
    key: "baseline",
    label: "Debut",
    periodMs: range.startMs,
    deltaNetDoes: 0,
    equityDoes: 0,
    drawdownDoes: 0,
    drawdownPct: 0,
  }];
  fullTrend.forEach((item) => {
    runningEquityDoes += safeSignedInt(item.netDoes);
    if (runningEquityDoes >= highWaterMarkDoes) {
      highWaterMarkDoes = runningEquityDoes;
      lastPeakAtMs = safeSignedInt(item.periodMs) || lastPeakAtMs;
    }
    const pointDrawdownDoes = Math.max(0, highWaterMarkDoes - runningEquityDoes);
    const pointDrawdownPct = highWaterMarkDoes > 0 ? (pointDrawdownDoes / highWaterMarkDoes) : 0;
    fullEquityCurve.push({
      key: item.key,
      label: item.label,
      periodMs: safeSignedInt(item.periodMs),
      deltaNetDoes: safeSignedInt(item.netDoes),
      equityDoes: runningEquityDoes,
      drawdownDoes: pointDrawdownDoes,
      drawdownPct: pointDrawdownPct,
    });
  });
  const currentEquityDoes = runningEquityDoes;
  const drawdownDoes = Math.max(0, highWaterMarkDoes - currentEquityDoes);
  const drawdownPct = highWaterMarkDoes > 0 ? (drawdownDoes / highWaterMarkDoes) : 0;
  const trend = fullTrend.slice(-BOT_PILOT_TREND_POINT_LIMIT);
  const equityCurve = fullEquityCurve.slice(-(BOT_PILOT_EQUITY_POINT_LIMIT + 1));
  const recommended = chooseAutoBotDifficulty({
    netDoes,
    collectedDoes,
    highWaterMarkDoes,
    currentEquityDoes,
    drawdownDoes,
    drawdownPct,
  });
  const botMix = Array.from(botMixMap.values()).map((item) => ({
    botCount: safeInt(item.botCount),
    rooms: safeInt(item.rooms),
    netDoes: safeSignedInt(item.netDoes),
    botWins: safeInt(item.botWins),
    humanWins: safeInt(item.humanWins),
  }));

  return {
    ok: true,
    window: range.windowKey,
    startMs: range.startMs,
    endMs: range.endMs,
    dayKey: getBotPilotDayKey(range.startMs),
    roomsCount,
    collectedDoes,
    payoutDoes,
    netDoes,
    grossCollectedDoes,
    grossPayoutDoes,
    grossNetDoes,
    promoExposureDoes,
    marginPct: collectedDoes > 0 ? netDoes / collectedDoes : 0,
    currentEquityDoes,
    highWaterMarkDoes,
    drawdownDoes,
    drawdownPct,
    lastPeakAtMs,
    botRooms,
    humanWins,
    botWins,
    botWinRatePct: roomsCount > 0 ? botWins / roomsCount : 0,
    humanWinRatePct: roomsCount > 0 ? humanWins / roomsCount : 0,
    truncated,
    fetchLimit: BOT_PILOT_SNAPSHOT_LIMIT,
    recommendedLevel: recommended.level,
    recommendedBand: recommended.band,
    recommendedReason: recommended.reason,
    trend,
    equityCurve,
    botMix,
    computedAtMs: nowMs,
  };
}

async function computeDuelBotPilotSnapshot(options = {}) {
  const nowMs = safeSignedInt(options.nowMs) || Date.now();
  const range = getBotPilotRange(options.window || "today", nowMs);
  const querySnap = await db.collection(DUEL_ROOM_RESULTS_COLLECTION)
    .where("endedAtMs", ">=", range.startMs)
    .orderBy("endedAtMs", "desc")
    .limit(BOT_PILOT_SNAPSHOT_LIMIT)
    .get();

  let roomsCount = 0;
  let collectedDoes = 0;
  let payoutDoes = 0;
  let netDoes = 0;
  let grossCollectedDoes = 0;
  let grossPayoutDoes = 0;
  let grossNetDoes = 0;
  let promoExposureDoes = 0;
  let botWins = 0;
  let humanWins = 0;
  let truncated = querySnap.size >= BOT_PILOT_SNAPSHOT_LIMIT;
  const trendMap = new Map();
  const stakeMixMap = new Map();
  const difficultyMixMap = new Map(
    Array.from(BOT_DIFFICULTY_LEVELS).map((level) => [level, {
      level,
      rooms: 0,
      netDoes: 0,
      botWins: 0,
      humanWins: 0,
    }])
  );

  querySnap.forEach((docSnap) => {
    const data = docSnap.data() || {};
    const endedAtMs = safeSignedInt(data.endedAtMs);
    if (endedAtMs < range.startMs || endedAtMs > range.endMs) return;
    if (String(data.status || "").trim().toLowerCase() !== "ended") return;
    if (String(data.roomMode || "").trim().toLowerCase() === "duel_friends") return;
    if (inferDuelResultBotCount(data) <= 0) return;

    roomsCount += 1;
    const roomCollectedGross = safeInt(data.companyCollectedDoes);
    const roomPayoutGross = safeInt(data.companyPayoutDoes);
    const roomNetGross = safeSignedInt(
      typeof data.companyNetDoes === "number"
        ? data.companyNetDoes
        : (roomCollectedGross - roomPayoutGross)
    );
    const roomCollected = safeInt(
      typeof data.companyCollectedRealDoes === "number"
        ? data.companyCollectedRealDoes
        : roomCollectedGross
    );
    const roomPayout = safeInt(
      typeof data.companyPayoutRealDoes === "number"
        ? data.companyPayoutRealDoes
        : roomPayoutGross
    );
    const roomNet = safeSignedInt(
      typeof data.companyNetRealDoes === "number"
        ? data.companyNetRealDoes
        : (roomCollected - roomPayout)
    );
    const roomPromoExposure = safeInt(data.companyPromoExposureDoes);
    const winnerType = String(data.winnerType || "").trim().toLowerCase();
    const stakeDoes = safeInt(data.entryCostDoes || data.stakeDoes);
    const difficulty = normalizeBotDifficulty(data.botDifficulty || DEFAULT_DUEL_BOT_DIFFICULTY);

    grossCollectedDoes += roomCollectedGross;
    grossPayoutDoes += roomPayoutGross;
    grossNetDoes += roomNetGross;
    collectedDoes += roomCollected;
    payoutDoes += roomPayout;
    netDoes += roomNet;
    promoExposureDoes += roomPromoExposure;
    if (winnerType === "bot") botWins += 1;
    if (winnerType === "human") humanWins += 1;

    const trendKey = getBotPilotTrendKey(range.windowKey, endedAtMs);
    const trendItem = trendMap.get(trendKey) || {
      key: trendKey,
      label: getBotPilotTrendLabel(range.windowKey, endedAtMs),
      periodMs: endedAtMs,
      rooms: 0,
      collectedDoes: 0,
      payoutDoes: 0,
      netDoes: 0,
      grossCollectedDoes: 0,
      grossPayoutDoes: 0,
      grossNetDoes: 0,
      promoExposureDoes: 0,
      botWins: 0,
      humanWins: 0,
    };
    trendItem.rooms += 1;
    trendItem.collectedDoes += roomCollected;
    trendItem.payoutDoes += roomPayout;
    trendItem.netDoes += roomNet;
    trendItem.grossCollectedDoes += roomCollectedGross;
    trendItem.grossPayoutDoes += roomPayoutGross;
    trendItem.grossNetDoes += roomNetGross;
    trendItem.promoExposureDoes += roomPromoExposure;
    if (winnerType === "bot") trendItem.botWins += 1;
    if (winnerType === "human") trendItem.humanWins += 1;
    if (endedAtMs > safeSignedInt(trendItem.periodMs)) {
      trendItem.periodMs = endedAtMs;
      trendItem.label = getBotPilotTrendLabel(range.windowKey, endedAtMs);
    }
    trendMap.set(trendKey, trendItem);

    const stakeItem = stakeMixMap.get(String(stakeDoes)) || {
      stakeDoes,
      label: `${stakeDoes} Does`,
      rooms: 0,
      netDoes: 0,
    };
    stakeItem.rooms += 1;
    stakeItem.netDoes += roomNet;
    stakeMixMap.set(String(stakeDoes), stakeItem);

    const difficultyItem = difficultyMixMap.get(difficulty) || {
      level: difficulty,
      rooms: 0,
      netDoes: 0,
      botWins: 0,
      humanWins: 0,
    };
    difficultyItem.rooms += 1;
    difficultyItem.netDoes += roomNet;
    if (winnerType === "bot") difficultyItem.botWins += 1;
    if (winnerType === "human") difficultyItem.humanWins += 1;
    difficultyMixMap.set(difficulty, difficultyItem);
  });

  const fullTrend = Array.from(trendMap.values())
    .sort((a, b) => safeSignedInt(a.periodMs) - safeSignedInt(b.periodMs))
    .map((item) => ({
      key: item.key,
      label: item.label,
      periodMs: safeSignedInt(item.periodMs),
      rooms: safeInt(item.rooms),
      collectedDoes: safeInt(item.collectedDoes),
      payoutDoes: safeInt(item.payoutDoes),
      netDoes: safeSignedInt(item.netDoes),
      grossCollectedDoes: safeInt(item.grossCollectedDoes),
      grossPayoutDoes: safeInt(item.grossPayoutDoes),
      grossNetDoes: safeSignedInt(item.grossNetDoes),
      promoExposureDoes: safeInt(item.promoExposureDoes),
      botWins: safeInt(item.botWins),
      humanWins: safeInt(item.humanWins),
    }));
  let runningEquityDoes = 0;
  let highWaterMarkDoes = 0;
  let lastPeakAtMs = range.startMs;
  const fullEquityCurve = [{
    key: "baseline",
    label: "Debut",
    periodMs: range.startMs,
    deltaNetDoes: 0,
    equityDoes: 0,
    drawdownDoes: 0,
    drawdownPct: 0,
  }];
  fullTrend.forEach((item) => {
    runningEquityDoes += safeSignedInt(item.netDoes);
    if (runningEquityDoes >= highWaterMarkDoes) {
      highWaterMarkDoes = runningEquityDoes;
      lastPeakAtMs = safeSignedInt(item.periodMs) || lastPeakAtMs;
    }
    const pointDrawdownDoes = Math.max(0, highWaterMarkDoes - runningEquityDoes);
    const pointDrawdownPct = highWaterMarkDoes > 0 ? (pointDrawdownDoes / highWaterMarkDoes) : 0;
    fullEquityCurve.push({
      key: item.key,
      label: item.label,
      periodMs: safeSignedInt(item.periodMs),
      deltaNetDoes: safeSignedInt(item.netDoes),
      equityDoes: runningEquityDoes,
      drawdownDoes: pointDrawdownDoes,
      drawdownPct: pointDrawdownPct,
    });
  });
  const currentEquityDoes = runningEquityDoes;
  const drawdownDoes = Math.max(0, highWaterMarkDoes - currentEquityDoes);
  const drawdownPct = highWaterMarkDoes > 0 ? (drawdownDoes / highWaterMarkDoes) : 0;
  const trend = fullTrend.slice(-BOT_PILOT_TREND_POINT_LIMIT);
  const equityCurve = fullEquityCurve.slice(-(BOT_PILOT_EQUITY_POINT_LIMIT + 1));
  const recommended = chooseAutoDuelBotDifficulty({
    roomsCount,
    netDoes,
    collectedDoes,
    highWaterMarkDoes,
    currentEquityDoes,
    drawdownDoes,
    drawdownPct,
    botWinRatePct: roomsCount > 0 ? (botWins / roomsCount) : 0,
    humanWinRatePct: roomsCount > 0 ? (humanWins / roomsCount) : 0,
  });

  return {
    ok: true,
    window: range.windowKey,
    startMs: range.startMs,
    endMs: range.endMs,
    dayKey: getBotPilotDayKey(range.startMs),
    roomsCount,
    collectedDoes,
    payoutDoes,
    netDoes,
    grossCollectedDoes,
    grossPayoutDoes,
    grossNetDoes,
    promoExposureDoes,
    marginPct: collectedDoes > 0 ? netDoes / collectedDoes : 0,
    currentEquityDoes,
    highWaterMarkDoes,
    drawdownDoes,
    drawdownPct,
    lastPeakAtMs,
    botWins,
    humanWins,
    botWinRatePct: roomsCount > 0 ? botWins / roomsCount : 0,
    humanWinRatePct: roomsCount > 0 ? humanWins / roomsCount : 0,
    truncated,
    fetchLimit: BOT_PILOT_SNAPSHOT_LIMIT,
    recommendedLevel: recommended.level,
    recommendedBand: recommended.band,
    recommendedReason: recommended.reason,
    trend,
    equityCurve,
    stakeMix: Array.from(stakeMixMap.values())
      .sort((left, right) => safeInt(left.stakeDoes) - safeInt(right.stakeDoes))
      .map((item) => ({
        stakeDoes: safeInt(item.stakeDoes),
        label: item.label,
        rooms: safeInt(item.rooms),
        netDoes: safeSignedInt(item.netDoes),
      })),
    difficultyMix: Array.from(difficultyMixMap.values()).map((item) => ({
      level: normalizeBotDifficulty(item.level),
      rooms: safeInt(item.rooms),
      netDoes: safeSignedInt(item.netDoes),
      botWins: safeInt(item.botWins),
      humanWins: safeInt(item.humanWins),
    })),
    computedAtMs: nowMs,
  };
}

function chooseMorpionAutoRouting(snapshot = {}) {
  const roomsCount = safeInt(snapshot.roomsCount);
  const netDoes = safeSignedInt(snapshot.netDoes);
  const marginPct = Number(snapshot.marginPct || 0);
  const drawdownPct = Math.max(0, Number(snapshot.drawdownPct || 0));
  const humanOnlySharePct = Number(snapshot.humanOnlySharePct || 0);

  if (roomsCount <= 10) {
    return {
      decision: "normal",
      band: "neutral",
      reason: "low_volume",
      humanOnlyEnabled: true,
    };
  }
  if (netDoes < 0) {
    return {
      decision: "bot_only_temp",
      band: "danger",
      reason: "negative_net",
      humanOnlyEnabled: false,
    };
  }
  if (marginPct < 0.04) {
    return {
      decision: "bot_only_temp",
      band: "danger",
      reason: "margin_too_low",
      humanOnlyEnabled: false,
    };
  }
  if (drawdownPct >= 0.15) {
    return {
      decision: "bot_only_temp",
      band: "danger",
      reason: "drawdown_critical",
      humanOnlyEnabled: false,
    };
  }
  if (humanOnlySharePct >= 0.72 && marginPct < 0.08) {
    return {
      decision: "bot_only_temp",
      band: "defense",
      reason: "too_many_human_rooms",
      humanOnlyEnabled: false,
    };
  }

  return {
    decision: "normal",
    band: "comfort",
    reason: "profit_ok",
    humanOnlyEnabled: true,
  };
}

async function computeMorpionPilotSnapshot(options = {}) {
  const nowMs = safeSignedInt(options.nowMs) || Date.now();
  const range = getMorpionPilotRange(options.window || "today", nowMs);
  const querySnap = await db.collection(MORPION_ROOM_RESULTS_COLLECTION)
    .where("endedAtMs", ">=", range.startMs)
    .orderBy("endedAtMs", "desc")
    .limit(MORPION_PILOT_SNAPSHOT_LIMIT)
    .get();

  let roomsCount = 0;
  let collectedDoes = 0;
  let payoutDoes = 0;
  let netDoes = 0;
  let grossCollectedDoes = 0;
  let grossPayoutDoes = 0;
  let grossNetDoes = 0;
  let promoExposureDoes = 0;
  let humanOnlyRooms = 0;
  let withBotRooms = 0;
  let withBotBotWins = 0;
  let withBotHumanWins = 0;
  let truncated = querySnap.size >= MORPION_PILOT_SNAPSHOT_LIMIT;
  const trendMap = new Map();

  querySnap.forEach((docSnap) => {
    const data = docSnap.data() || {};
    const endedAtMs = safeSignedInt(data.endedAtMs);
    if (endedAtMs < range.startMs || endedAtMs > range.endMs) return;
    if (String(data.status || "").trim().toLowerCase() !== "ended") return;

    roomsCount += 1;
    const roomCollectedGross = safeInt(data.companyCollectedDoes);
    const roomPayoutGross = safeInt(data.companyPayoutDoes);
    const roomNetGross = safeSignedInt(
      typeof data.companyNetDoes === "number"
        ? data.companyNetDoes
        : (roomCollectedGross - roomPayoutGross)
    );
    const roomCollected = safeInt(
      typeof data.companyCollectedRealDoes === "number"
        ? data.companyCollectedRealDoes
        : roomCollectedGross
    );
    const roomPayout = safeInt(
      typeof data.companyPayoutRealDoes === "number"
        ? data.companyPayoutRealDoes
        : roomPayoutGross
    );
    const roomNet = safeSignedInt(
      typeof data.companyNetRealDoes === "number"
        ? data.companyNetRealDoes
        : (roomCollected - roomPayout)
    );
    const roomPromoExposure = safeInt(data.companyPromoExposureDoes);

    grossCollectedDoes += roomCollectedGross;
    grossPayoutDoes += roomPayoutGross;
    grossNetDoes += roomNetGross;
    promoExposureDoes += roomPromoExposure;
    collectedDoes += roomCollected;
    payoutDoes += roomPayout;
    netDoes += roomNet;

    const humanCount = safeInt(data.humanCount);
    const botCount = inferDuelResultBotCount(data);
    const composition = getMorpionCompositionMeta(humanCount, botCount);
    const winnerType = String(data.winnerType || "").trim().toLowerCase();
    if (composition.key === "human_only") {
      humanOnlyRooms += 1;
    } else if (composition.key === "with_bot") {
      withBotRooms += 1;
      if (winnerType === "bot") withBotBotWins += 1;
      if (winnerType === "human") withBotHumanWins += 1;
    }

    const trendKey = getMorpionPilotTrendKey(range.windowKey, endedAtMs);
    const trendEntry = trendMap.get(trendKey) || {
      key: trendKey,
      label: getMorpionPilotTrendLabel(range.windowKey, endedAtMs),
      periodMs: endedAtMs,
      rooms: 0,
      collectedDoes: 0,
      payoutDoes: 0,
      netDoes: 0,
      humanOnlyRooms: 0,
      withBotRooms: 0,
      grossCollectedDoes: 0,
      grossPayoutDoes: 0,
      grossNetDoes: 0,
      promoExposureDoes: 0,
    };
    trendEntry.rooms += 1;
    trendEntry.collectedDoes += roomCollected;
    trendEntry.payoutDoes += roomPayout;
    trendEntry.netDoes += roomNet;
    trendEntry.grossCollectedDoes += roomCollectedGross;
    trendEntry.grossPayoutDoes += roomPayoutGross;
    trendEntry.grossNetDoes += roomNetGross;
    trendEntry.promoExposureDoes += roomPromoExposure;
    if (composition.key === "human_only") trendEntry.humanOnlyRooms += 1;
    if (composition.key === "with_bot") trendEntry.withBotRooms += 1;
    if (endedAtMs > safeSignedInt(trendEntry.periodMs)) {
      trendEntry.periodMs = endedAtMs;
      trendEntry.label = getMorpionPilotTrendLabel(range.windowKey, endedAtMs);
    }
    trendMap.set(trendKey, trendEntry);
  });

  const fullTrend = Array.from(trendMap.values())
    .sort((a, b) => safeSignedInt(a.periodMs) - safeSignedInt(b.periodMs))
    .map((item) => ({
      key: item.key,
      label: item.label,
      periodMs: safeSignedInt(item.periodMs),
      rooms: safeInt(item.rooms),
      collectedDoes: safeInt(item.collectedDoes),
      payoutDoes: safeInt(item.payoutDoes),
      netDoes: safeSignedInt(item.netDoes),
      humanOnlyRooms: safeInt(item.humanOnlyRooms),
      withBotRooms: safeInt(item.withBotRooms),
      grossCollectedDoes: safeInt(item.grossCollectedDoes),
      grossPayoutDoes: safeInt(item.grossPayoutDoes),
      grossNetDoes: safeSignedInt(item.grossNetDoes),
      promoExposureDoes: safeInt(item.promoExposureDoes),
    }));

  let runningEquityDoes = 0;
  let highWaterMarkDoes = 0;
  let lastPeakAtMs = range.startMs;
  const fullEquityCurve = [{
    key: "baseline",
    label: "Debut",
    periodMs: range.startMs,
    deltaNetDoes: 0,
    equityDoes: 0,
    drawdownDoes: 0,
    drawdownPct: 0,
  }];

  fullTrend.forEach((item) => {
    runningEquityDoes += safeSignedInt(item.netDoes);
    if (runningEquityDoes >= highWaterMarkDoes) {
      highWaterMarkDoes = runningEquityDoes;
      lastPeakAtMs = safeSignedInt(item.periodMs) || lastPeakAtMs;
    }
    const pointDrawdownDoes = Math.max(0, highWaterMarkDoes - runningEquityDoes);
    const pointDrawdownPct = highWaterMarkDoes > 0 ? (pointDrawdownDoes / highWaterMarkDoes) : 0;
    fullEquityCurve.push({
      key: item.key,
      label: item.label,
      periodMs: safeSignedInt(item.periodMs),
      deltaNetDoes: safeSignedInt(item.netDoes),
      equityDoes: runningEquityDoes,
      drawdownDoes: pointDrawdownDoes,
      drawdownPct: pointDrawdownPct,
    });
  });

  const currentEquityDoes = runningEquityDoes;
  const drawdownDoes = Math.max(0, highWaterMarkDoes - currentEquityDoes);
  const drawdownPct = highWaterMarkDoes > 0 ? (drawdownDoes / highWaterMarkDoes) : 0;
  const marginPct = collectedDoes > 0 ? netDoes / collectedDoes : 0;
  const humanOnlySharePct = roomsCount > 0 ? humanOnlyRooms / roomsCount : 0;
  const withBotSharePct = roomsCount > 0 ? withBotRooms / roomsCount : 0;

  const recommended = chooseMorpionAutoRouting({
    roomsCount,
    netDoes,
    marginPct,
    drawdownPct,
    humanOnlySharePct,
  });

  return {
    ok: true,
    window: range.windowKey,
    startMs: range.startMs,
    endMs: range.endMs,
    roomsCount,
    collectedDoes,
    payoutDoes,
    netDoes,
    grossCollectedDoes,
    grossPayoutDoes,
    grossNetDoes,
    promoExposureDoes,
    marginPct,
    currentEquityDoes,
    highWaterMarkDoes,
    drawdownDoes,
    drawdownPct,
    lastPeakAtMs,
    humanOnlyRooms,
    withBotRooms,
    humanOnlySharePct,
    withBotSharePct,
    withBotBotWins,
    withBotHumanWins,
    withBotBotWinRatePct: withBotRooms > 0 ? withBotBotWins / withBotRooms : 0,
    withBotHumanWinRatePct: withBotRooms > 0 ? withBotHumanWins / withBotRooms : 0,
    truncated,
    fetchLimit: MORPION_PILOT_SNAPSHOT_LIMIT,
    recommendedDecision: normalizeMorpionPilotDecision(recommended.decision),
    recommendedBand: String(recommended.band || ""),
    recommendedReason: String(recommended.reason || ""),
    recommendedHumanOnlyEnabled: recommended.humanOnlyEnabled !== false,
    trend: fullTrend.slice(-MORPION_PILOT_TREND_POINT_LIMIT),
    equityCurve: fullEquityCurve.slice(-(MORPION_PILOT_EQUITY_POINT_LIMIT + 1)),
    computedAtMs: nowMs,
  };
}

function normalizeDuelAnalyticsWindow(value = "") {
  const normalized = String(value || "").trim().toLowerCase();
  return normalized === "today" || normalized === "7d" || normalized === "30d" || normalized === "global"
    ? normalized
    : "30d";
}

function getDuelAnalyticsDayKey(ms = 0) {
  if (!ms) return "";
  const date = new Date(ms);
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function getDuelAnalyticsHourKey(ms = 0) {
  if (!ms) return "";
  const date = new Date(ms);
  const hour = String(date.getHours()).padStart(2, "0");
  return `${getDuelAnalyticsDayKey(ms)} ${hour}:00`;
}

function getDuelAnalyticsBucketKey(granularity = "day", ms = 0) {
  return granularity === "hour" ? getDuelAnalyticsHourKey(ms) : getDuelAnalyticsDayKey(ms);
}

function getDuelAnalyticsBucketLabel(granularity = "day", ms = 0) {
  if (!ms) return "-";
  const date = new Date(ms);
  if (granularity === "hour") {
    return date.toLocaleString("fr-FR", {
      day: "2-digit",
      month: "short",
      hour: "2-digit",
      minute: "2-digit",
    });
  }
  return date.toLocaleDateString("fr-FR", {
    day: "2-digit",
    month: "short",
  });
}

function getDuelAnalyticsRange(options = {}, nowMs = Date.now()) {
  const customStartMs = safeSignedInt(options.startMs);
  const customEndMs = safeSignedInt(options.endMs);
  if (customStartMs > 0 && customEndMs > 0 && customEndMs >= customStartMs) {
    const rangeMs = Math.max(1, customEndMs - customStartMs);
    return {
      windowKey: "custom",
      startMs: customStartMs,
      endMs: customEndMs,
      granularity: rangeMs <= (2 * 24 * 60 * 60 * 1000) ? "hour" : "day",
      isGlobal: false,
    };
  }

  const windowKey = normalizeDuelAnalyticsWindow(options.window || "30d");
  const now = new Date(nowMs);
  const todayStartMs = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime();
  if (windowKey === "today") {
    return {
      windowKey,
      startMs: todayStartMs,
      endMs: nowMs,
      granularity: "hour",
      isGlobal: false,
    };
  }
  if (windowKey === "7d") {
    return {
      windowKey,
      startMs: todayStartMs - (6 * 24 * 60 * 60 * 1000),
      endMs: nowMs,
      granularity: "day",
      isGlobal: false,
    };
  }
  if (windowKey === "30d") {
    return {
      windowKey,
      startMs: todayStartMs - (29 * 24 * 60 * 60 * 1000),
      endMs: nowMs,
      granularity: "day",
      isGlobal: false,
    };
  }
  return {
    windowKey: "global",
    startMs: 0,
    endMs: nowMs,
    granularity: "day",
    isGlobal: true,
  };
}

function normalizeGlobalAnalyticsWindow(value = "") {
  const normalized = String(value || "").trim().toLowerCase();
  return normalized === "today" || normalized === "7d" || normalized === "30d" || normalized === "global"
    ? normalized
    : "today";
}

function getGlobalAnalyticsRange(options = {}, nowMs = Date.now()) {
  const customStartMs = safeSignedInt(options.startMs);
  const customEndMs = safeSignedInt(options.endMs);
  if (customStartMs > 0 && customEndMs > 0 && customEndMs >= customStartMs) {
    return {
      windowKey: "custom",
      startMs: customStartMs,
      endMs: customEndMs,
    };
  }

  const windowKey = normalizeGlobalAnalyticsWindow(options.window || "today");
  const now = new Date(nowMs);
  const todayStartMs = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime();
  if (windowKey === "today") {
    return { windowKey, startMs: todayStartMs, endMs: nowMs };
  }
  if (windowKey === "7d") {
    return {
      windowKey,
      startMs: todayStartMs - (6 * 24 * 60 * 60 * 1000),
      endMs: nowMs,
    };
  }
  if (windowKey === "30d") {
    return {
      windowKey,
      startMs: todayStartMs - (29 * 24 * 60 * 60 * 1000),
      endMs: nowMs,
    };
  }
  return {
    windowKey: "global",
    startMs: 0,
    endMs: nowMs,
  };
}

function applyAnalyticsTimeRange(query, field, range, direction = "desc") {
  let next = query.orderBy(field, direction);
  if (range.startMs > 0) {
    next = next.where(field, ">=", range.startMs);
  }
  if (range.endMs > 0) {
    next = next.where(field, "<=", range.endMs);
  }
  return next;
}

async function safeAnalyticsQueryGet(primaryQuery, fallbackQuery = null, label = "") {
  try {
    return await primaryQuery.get();
  } catch (error) {
    const code = safeSignedInt(error?.code);
    if ((code === 9 || String(error?.message || "").includes("FAILED_PRECONDITION")) && fallbackQuery) {
      console.warn("[GLOBAL_ANALYTICS] range query fallback", {
        label,
        code,
        message: String(error?.message || ""),
      });
      return fallbackQuery.get();
    }
    throw error;
  }
}

async function computeDuelAnalyticsSnapshot(options = {}) {
  const nowMs = safeSignedInt(options.nowMs) || Date.now();
  const range = getDuelAnalyticsRange(options, nowMs);
  let query = db.collection(DUEL_ROOM_RESULTS_COLLECTION).orderBy("endedAtMs", "asc");

  if (range.startMs > 0) {
    query = query.where("endedAtMs", ">=", range.startMs);
  }
  if (range.endMs > 0) {
    query = query.where("endedAtMs", "<=", range.endMs);
  }

  const querySnap = await query.get();
  let matchesPlayed = 0;
  let matchesWithBot = 0;
  let botWins = 0;
  let humanWins = 0;
  let publicMatches = 0;
  let friendMatches = 0;
  let totalDurationMs = 0;
  let durationSamples = 0;
  let totalStakeDoes = 0;
  const trendMap = new Map();
  const stakeMixMap = new Map();
  const recentResults = [];

  querySnap.forEach((docSnap) => {
    const data = docSnap.data() || {};
    const status = String(data.status || "").trim().toLowerCase();
    const endedAtMs = safeSignedInt(data.endedAtMs);
    if (status !== "ended") return;
    if (range.startMs > 0 && endedAtMs < range.startMs) return;
    if (range.endMs > 0 && endedAtMs > range.endMs) return;

    matchesPlayed += 1;

    const botCount = inferDuelResultBotCount(data);
    const withBot = botCount > 0;
    if (withBot) matchesWithBot += 1;

    const winnerType = String(data.winnerType || "").trim().toLowerCase();
    if (winnerType === "bot") botWins += 1;
    if (winnerType === "human") humanWins += 1;

    const roomModeRaw = String(data.roomMode || "").trim().toLowerCase();
    const modeKey = roomModeRaw === "duel_friends" ? "friends" : "public";
    if (modeKey === "friends") friendMatches += 1;
    else publicMatches += 1;

    const stakeDoes = safeInt(data.entryCostDoes || data.stakeDoes);
    totalStakeDoes += stakeDoes;
    const stakeEntry = stakeMixMap.get(String(stakeDoes)) || {
      stakeDoes,
      label: `${stakeDoes} Does`,
      count: 0,
    };
    stakeEntry.count += 1;
    stakeMixMap.set(String(stakeDoes), stakeEntry);

    const startedAtMs = safeSignedInt(data.startedAtMs);
    const durationMs = startedAtMs > 0 && endedAtMs >= startedAtMs
      ? Math.max(0, endedAtMs - startedAtMs)
      : 0;
    if (durationMs > 0) {
      totalDurationMs += durationMs;
      durationSamples += 1;
    }

    const bucketKey = getDuelAnalyticsBucketKey(range.granularity, endedAtMs);
    const bucket = trendMap.get(bucketKey) || {
      key: bucketKey,
      label: getDuelAnalyticsBucketLabel(range.granularity, endedAtMs),
      periodMs: endedAtMs,
      matchesPlayed: 0,
      matchesWithBot: 0,
      botWins: 0,
      humanWins: 0,
      publicMatches: 0,
      friendMatches: 0,
      totalStakeDoes: 0,
      totalDurationMs: 0,
      durationSamples: 0,
    };
    bucket.matchesPlayed += 1;
    if (withBot) bucket.matchesWithBot += 1;
    if (winnerType === "bot") bucket.botWins += 1;
    if (winnerType === "human") bucket.humanWins += 1;
    if (modeKey === "friends") bucket.friendMatches += 1;
    else bucket.publicMatches += 1;
    bucket.totalStakeDoes += stakeDoes;
    if (durationMs > 0) {
      bucket.totalDurationMs += durationMs;
      bucket.durationSamples += 1;
    }
    if (endedAtMs > safeSignedInt(bucket.periodMs)) {
      bucket.periodMs = endedAtMs;
      bucket.label = getDuelAnalyticsBucketLabel(range.granularity, endedAtMs);
    }
    trendMap.set(bucketKey, bucket);

    recentResults.push({
      roomId: docSnap.id,
      endedAtMs,
      stakeDoes,
      botCount,
      withBot,
      winnerType,
      roomMode: modeKey,
      durationMs,
    });
  });

  recentResults.sort((left, right) => safeSignedInt(right.endedAtMs) - safeSignedInt(left.endedAtMs));

  const trend = Array.from(trendMap.values())
    .sort((left, right) => safeSignedInt(left.periodMs) - safeSignedInt(right.periodMs))
    .map((bucket) => ({
      key: bucket.key,
      label: bucket.label,
      periodMs: safeSignedInt(bucket.periodMs),
      matchesPlayed: safeInt(bucket.matchesPlayed),
      matchesWithBot: safeInt(bucket.matchesWithBot),
      matchesWithoutBot: Math.max(0, safeInt(bucket.matchesPlayed) - safeInt(bucket.matchesWithBot)),
      botWins: safeInt(bucket.botWins),
      humanWins: safeInt(bucket.humanWins),
      publicMatches: safeInt(bucket.publicMatches),
      friendMatches: safeInt(bucket.friendMatches),
      avgStakeDoes: safeInt(bucket.matchesPlayed) > 0 ? Math.round(bucket.totalStakeDoes / bucket.matchesPlayed) : 0,
      avgDurationMs: safeInt(bucket.durationSamples) > 0 ? Math.round(bucket.totalDurationMs / bucket.durationSamples) : 0,
    }));

  const stakeMix = Array.from(stakeMixMap.values())
    .sort((left, right) => safeInt(left.stakeDoes) - safeInt(right.stakeDoes))
    .map((item) => ({
      stakeDoes: safeInt(item.stakeDoes),
      label: String(item.label || `${safeInt(item.stakeDoes)} Does`),
      count: safeInt(item.count),
    }));

  return {
    ok: true,
    generatedAtMs: nowMs,
    range: {
      window: range.windowKey,
      startMs: range.startMs,
      endMs: range.endMs,
      granularity: range.granularity,
      isGlobal: range.isGlobal,
    },
    summary: {
      matchesPlayed,
      matchesWithBot,
      matchesWithoutBot: Math.max(0, matchesPlayed - matchesWithBot),
      botWins,
      humanWins,
      publicMatches,
      friendMatches,
      avgDurationMs: durationSamples > 0 ? Math.round(totalDurationMs / durationSamples) : 0,
      avgStakeDoes: matchesPlayed > 0 ? Math.round(totalStakeDoes / matchesPlayed) : 0,
      botMatchRatePct: matchesPlayed > 0 ? matchesWithBot / matchesPlayed : 0,
      botWinRatePct: matchesPlayed > 0 ? botWins / matchesPlayed : 0,
      humanWinRatePct: matchesPlayed > 0 ? humanWins / matchesPlayed : 0,
    },
    modeMix: [
      { key: "public", label: "Public", count: publicMatches },
      { key: "friends", label: "Entre amis", count: friendMatches },
    ],
    stakeMix,
    trend,
    recentResults: recentResults.slice(0, 12),
  };
}

function normalizeMorpionAnalyticsComposition(value = "") {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "human_only" || normalized === "human-vs-human") return "human_only";
  if (normalized === "with_bot" || normalized === "human-vs-bot" || normalized === "bot") return "with_bot";
  return "all";
}

function normalizeAnalyticsWinnerFilter(value = "") {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "human" || normalized === "bot") return normalized;
  return "all";
}

function getMorpionCompositionMeta(humanCount = 0, botCount = 0) {
  const safeHumanCount = safeInt(humanCount);
  const safeBotCount = safeInt(botCount);
  if (safeHumanCount >= 2 && safeBotCount <= 0) {
    return {
      key: "human_only",
      label: "2 humains",
    };
  }
  if (safeHumanCount >= 1 && safeBotCount >= 1) {
    return {
      key: "with_bot",
      label: "1 humain + 1 bot",
    };
  }
  return {
    key: "other",
    label: "Autre",
  };
}

async function computeMorpionAnalyticsSnapshot(options = {}) {
  const nowMs = safeSignedInt(options.nowMs) || Date.now();
  const range = getDuelAnalyticsRange(options, nowMs);
  const compositionFilter = normalizeMorpionAnalyticsComposition(options.composition);
  const winnerFilter = normalizeAnalyticsWinnerFilter(options.winnerType);
  const stakeFilter = safeInt(options.stakeDoes);

  let query = db.collection(MORPION_ROOM_RESULTS_COLLECTION).orderBy("endedAtMs", "asc");

  if (range.startMs > 0) {
    query = query.where("endedAtMs", ">=", range.startMs);
  }
  if (range.endMs > 0) {
    query = query.where("endedAtMs", "<=", range.endMs);
  }

  const querySnap = await query.get();
  let matchesPlayed = 0;
  let matchesWithBot = 0;
  let matchesHumanOnly = 0;
  let botWins = 0;
  let humanWins = 0;
  let botMatchBotWins = 0;
  let botMatchHumanWins = 0;
  let totalDurationMs = 0;
  let durationSamples = 0;
  let totalStakeDoes = 0;
  const trendMap = new Map();
  const stakeMixMap = new Map();
  const compositionMixMap = new Map([
    ["human_only", { key: "human_only", label: "2 humains", count: 0 }],
    ["with_bot", { key: "with_bot", label: "1 humain + 1 bot", count: 0 }],
  ]);
  const recentResults = [];

  querySnap.forEach((docSnap) => {
    const data = docSnap.data() || {};
    const status = String(data.status || "").trim().toLowerCase();
    const endedAtMs = safeSignedInt(data.endedAtMs);
    if (status !== "ended") return;
    if (range.startMs > 0 && endedAtMs < range.startMs) return;
    if (range.endMs > 0 && endedAtMs > range.endMs) return;

    const humanCount = safeInt(data.humanCount);
    const botCount = safeInt(data.botCount);
    const composition = getMorpionCompositionMeta(humanCount, botCount);
    if (compositionFilter !== "all" && composition.key !== compositionFilter) return;

    const winnerType = String(data.winnerType || "").trim().toLowerCase();
    if (winnerFilter !== "all" && winnerType !== winnerFilter) return;

    const stakeDoes = safeInt(data.entryCostDoes || data.stakeDoes);
    if (stakeFilter > 0 && stakeDoes !== stakeFilter) return;

    matchesPlayed += 1;
    totalStakeDoes += stakeDoes;

    if (composition.key === "with_bot") matchesWithBot += 1;
    if (composition.key === "human_only") matchesHumanOnly += 1;

    if (winnerType === "bot") {
      botWins += 1;
      if (composition.key === "with_bot") botMatchBotWins += 1;
    }
    if (winnerType === "human") {
      humanWins += 1;
      if (composition.key === "with_bot") botMatchHumanWins += 1;
    }

    const compositionEntry = compositionMixMap.get(composition.key);
    if (compositionEntry) {
      compositionEntry.count += 1;
    }

    const stakeEntry = stakeMixMap.get(String(stakeDoes)) || {
      stakeDoes,
      label: `${stakeDoes} Does`,
      count: 0,
    };
    stakeEntry.count += 1;
    stakeMixMap.set(String(stakeDoes), stakeEntry);

    const startedAtMs = safeSignedInt(data.startedAtMs);
    const durationMs = startedAtMs > 0 && endedAtMs >= startedAtMs
      ? Math.max(0, endedAtMs - startedAtMs)
      : 0;
    if (durationMs > 0) {
      totalDurationMs += durationMs;
      durationSamples += 1;
    }

    const bucketKey = getDuelAnalyticsBucketKey(range.granularity, endedAtMs);
    const bucket = trendMap.get(bucketKey) || {
      key: bucketKey,
      label: getDuelAnalyticsBucketLabel(range.granularity, endedAtMs),
      periodMs: endedAtMs,
      matchesPlayed: 0,
      matchesWithBot: 0,
      matchesHumanOnly: 0,
      botWins: 0,
      humanWins: 0,
      botMatchBotWins: 0,
      botMatchHumanWins: 0,
      totalStakeDoes: 0,
      totalDurationMs: 0,
      durationSamples: 0,
    };
    bucket.matchesPlayed += 1;
    if (composition.key === "with_bot") bucket.matchesWithBot += 1;
    if (composition.key === "human_only") bucket.matchesHumanOnly += 1;
    if (winnerType === "bot") {
      bucket.botWins += 1;
      if (composition.key === "with_bot") bucket.botMatchBotWins += 1;
    }
    if (winnerType === "human") {
      bucket.humanWins += 1;
      if (composition.key === "with_bot") bucket.botMatchHumanWins += 1;
    }
    bucket.totalStakeDoes += stakeDoes;
    if (durationMs > 0) {
      bucket.totalDurationMs += durationMs;
      bucket.durationSamples += 1;
    }
    if (endedAtMs > safeSignedInt(bucket.periodMs)) {
      bucket.periodMs = endedAtMs;
      bucket.label = getDuelAnalyticsBucketLabel(range.granularity, endedAtMs);
    }
    trendMap.set(bucketKey, bucket);

    recentResults.push({
      roomId: docSnap.id,
      endedAtMs,
      startedAtMs,
      durationMs,
      stakeDoes,
      humanCount,
      botCount,
      compositionKey: composition.key,
      compositionLabel: composition.label,
      winnerType,
      winnerSeat: safeSignedInt(data.winnerSeat),
      endedReason: String(data.endedReason || "").trim(),
      botDifficulty: normalizeBotDifficulty(data.botDifficulty),
    });
  });

  recentResults.sort((left, right) => safeSignedInt(right.endedAtMs) - safeSignedInt(left.endedAtMs));

  const trend = Array.from(trendMap.values())
    .sort((left, right) => safeSignedInt(left.periodMs) - safeSignedInt(right.periodMs))
    .map((bucket) => ({
      key: bucket.key,
      label: bucket.label,
      periodMs: safeSignedInt(bucket.periodMs),
      matchesPlayed: safeInt(bucket.matchesPlayed),
      matchesWithBot: safeInt(bucket.matchesWithBot),
      matchesHumanOnly: safeInt(bucket.matchesHumanOnly),
      botWins: safeInt(bucket.botWins),
      humanWins: safeInt(bucket.humanWins),
      botMatchBotWins: safeInt(bucket.botMatchBotWins),
      botMatchHumanWins: safeInt(bucket.botMatchHumanWins),
      avgStakeDoes: safeInt(bucket.matchesPlayed) > 0 ? Math.round(bucket.totalStakeDoes / bucket.matchesPlayed) : 0,
      avgDurationMs: safeInt(bucket.durationSamples) > 0 ? Math.round(bucket.totalDurationMs / bucket.durationSamples) : 0,
    }));

  const stakeMix = Array.from(stakeMixMap.values())
    .sort((left, right) => safeInt(left.stakeDoes) - safeInt(right.stakeDoes))
    .map((item) => ({
      stakeDoes: safeInt(item.stakeDoes),
      label: String(item.label || `${safeInt(item.stakeDoes)} Does`),
      count: safeInt(item.count),
    }));

  const compositionMix = Array.from(compositionMixMap.values())
    .filter((item) => safeInt(item.count) > 0 || item.key === "human_only" || item.key === "with_bot")
    .map((item) => ({
      key: String(item.key || ""),
      label: String(item.label || ""),
      count: safeInt(item.count),
    }));

  return {
    ok: true,
    generatedAtMs: nowMs,
    filters: {
      composition: compositionFilter,
      winnerType: winnerFilter,
      stakeDoes: stakeFilter,
    },
    range: {
      window: range.windowKey,
      startMs: range.startMs,
      endMs: range.endMs,
      granularity: range.granularity,
      isGlobal: range.isGlobal,
    },
    summary: {
      matchesPlayed,
      matchesWithBot,
      matchesHumanOnly,
      botWins,
      humanWins,
      botMatchBotWins,
      botMatchHumanWins,
      avgDurationMs: durationSamples > 0 ? Math.round(totalDurationMs / durationSamples) : 0,
      avgStakeDoes: matchesPlayed > 0 ? Math.round(totalStakeDoes / matchesPlayed) : 0,
      withBotRatePct: matchesPlayed > 0 ? matchesWithBot / matchesPlayed : 0,
      humanOnlyRatePct: matchesPlayed > 0 ? matchesHumanOnly / matchesPlayed : 0,
      botWinRatePct: matchesPlayed > 0 ? botWins / matchesPlayed : 0,
      humanWinRatePct: matchesPlayed > 0 ? humanWins / matchesPlayed : 0,
      botMatchBotWinRatePct: matchesWithBot > 0 ? botMatchBotWins / matchesWithBot : 0,
      botMatchHumanWinRatePct: matchesWithBot > 0 ? botMatchHumanWins / matchesWithBot : 0,
    },
    compositionMix,
    stakeMix,
    trend,
    recentResults: recentResults.slice(0, 12),
  };
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

async function readPrivateDeckOrderForDuelRoom(roomId) {
  const safeRoomId = String(roomId || "").trim();
  if (!safeRoomId) return [];
  const snap = await duelGameStateRef(safeRoomId).get();
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
      pendingPlayFromWelcomeDoes: safeInt(payload.pendingPlayFromWelcomeDoes),
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

function roomResultRef(roomId = "") {
  return db.collection(ROOM_RESULTS_COLLECTION).doc(String(roomId || "").trim());
}

function buildRoomResultSnapshot(roomId = "", room = {}, overrides = {}) {
  const snapshot = { ...room, ...overrides };
  const playerUids = Array.isArray(snapshot.playerUids)
    ? snapshot.playerUids.map((item) => String(item || "").trim())
    : ["", "", "", ""];
  const playerNames = Array.isArray(snapshot.playerNames)
    ? snapshot.playerNames.map((item) => String(item || "").trim())
    : ["", "", "", ""];
  const humanCount = safeInt(
    typeof snapshot.humanCount === "number"
      ? snapshot.humanCount
      : playerUids.filter(Boolean).length
  );
  const botCount = safeInt(
    typeof snapshot.botCount === "number"
      ? snapshot.botCount
      : Math.max(0, 4 - humanCount)
  );
  const winnerSeat = Number.isFinite(Number(snapshot.winnerSeat))
    ? Math.trunc(Number(snapshot.winnerSeat))
    : -1;
  const winnerUid = String(snapshot.winnerUid || "").trim() || getWinnerUidForSeat({ seats: getRoomSeats(room), playerUids }, winnerSeat);
  const winnerType = winnerUid
    ? "human"
    : (winnerSeat >= 0 ? "bot" : "unknown");
  const entryCostDoes = safeInt(snapshot.entryCostDoes || snapshot.stakeDoes);
  const rewardAmountDoes = resolveRoomRewardDoes(snapshot);
  const endedAtMs = safeSignedInt(snapshot.endedAtMs) || Date.now();
  const startedAtMs = safeSignedInt(snapshot.startedAtMs);
  const createdAtMs = safeSignedInt(snapshot.createdAtMs);
  const status = String(snapshot.status || "").trim().toLowerCase() || "ended";
  const entryFundingByUid = snapshot.entryFundingByUid && typeof snapshot.entryFundingByUid === "object"
    ? snapshot.entryFundingByUid
    : {};
  const welcomeFundedDoes = playerUids.reduce((sum, playerUid) => {
    if (!playerUid) return sum;
    const funding = entryFundingByUid[playerUid] && typeof entryFundingByUid[playerUid] === "object"
      ? entryFundingByUid[playerUid]
      : {};
    return sum + safeInt(funding.welcomeDoes);
  }, 0);
  const companyCollectedDoes = humanCount * entryCostDoes;
  const companyCollectedRealDoes = Math.max(0, companyCollectedDoes - welcomeFundedDoes);
  const companyPromoExposureDoes = welcomeFundedDoes;
  const winnerFunding = winnerUid && entryFundingByUid[winnerUid] && typeof entryFundingByUid[winnerUid] === "object"
    ? entryFundingByUid[winnerUid]
    : {};
  const winnerWelcomeEntryDoes = safeInt(winnerFunding.welcomeDoes);
  const companyPayoutDoes = winnerType === "human" ? rewardAmountDoes : 0;
  const companyPayoutRealDoes = winnerType === "human"
    ? Math.max(0, companyPayoutDoes - Math.min(companyPayoutDoes, Math.round((companyPayoutDoes * winnerWelcomeEntryDoes) / Math.max(1, entryCostDoes))))
    : 0;
  const companyNetDoes = companyCollectedDoes - companyPayoutDoes;
  const companyNetRealDoes = companyCollectedRealDoes - companyPayoutRealDoes;

  return {
    roomId: String(roomId || "").trim(),
    status,
    roomMode: String(snapshot.roomMode || "").trim(),
    isPrivate: snapshot.isPrivate === true,
    ownerUid: String(snapshot.ownerUid || "").trim(),
    inviteCode: String(snapshot.inviteCode || "").trim(),
    stakeConfigId: String(snapshot.stakeConfigId || "").trim(),
    entryCostDoes,
    rewardAmountDoes,
    humanCount,
    botCount,
    totalSeats: humanCount + botCount,
    playerUids,
    playerNames,
    winnerSeat,
    winnerUid,
    winnerType,
    endedReason: sanitizeText(snapshot.endedReason || "", 40),
    botDifficulty: normalizeBotDifficulty(snapshot.botDifficulty),
    createdAtMs,
    startedAtMs,
    endedAtMs,
    companyCollectedDoes,
    companyCollectedRealDoes,
    companyPayoutDoes,
    companyPayoutRealDoes,
    companyNetDoes,
    companyNetRealDoes,
    companyPromoExposureDoes,
    welcomeFundedDoes,
    archiveVersion: 1,
    archivedAtMs: Date.now(),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };
}

function writeRoomResultIfEndedTx(tx, roomRefDoc, room = {}, roomUpdate = {}) {
  const nextStatus = String(roomUpdate.status || room.status || "").trim().toLowerCase();
  if (nextStatus !== "ended") return;
  const snapshot = buildRoomResultSnapshot(roomRefDoc.id, room, roomUpdate);
  tx.set(roomResultRef(roomRefDoc.id), {
    ...snapshot,
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
  }, { merge: true });
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
    min += 80;
    max += 170;
  }

  min += Math.min(320, branchingCount * 80);
  max += Math.min(650, branchingCount * 160);

  if (difficulty === "ultra") {
    min += 90;
    max += 180;
  } else if (difficulty === "amateur") {
    min = Math.max(420, min - 90);
    max = Math.max(min + 140, max - 120);
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
      const roomUpdate = buildRoomUpdateFromGameState(liveRoom, nextState, batchResult.records);
      tx.update(roomRef, roomUpdate);
      writeRoomResultIfEndedTx(tx, roomRef, liveRoom, roomUpdate);

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

function duelRoomRef(roomId = "") {
  const safeRoomId = String(roomId || "").trim();
  return safeRoomId
    ? db.collection(DUEL_ROOMS_COLLECTION).doc(safeRoomId)
    : db.collection(DUEL_ROOMS_COLLECTION).doc();
}

function duelGameStateRef(roomId = "") {
  return db.collection(DUEL_GAME_STATES_COLLECTION).doc(String(roomId || "").trim());
}

function duelRoomResultRef(roomId = "") {
  return db.collection(DUEL_ROOM_RESULTS_COLLECTION).doc(String(roomId || "").trim());
}

function duelMatchmakingPoolRef(stakeConfigId = "", stakeDoes = 0) {
  const normalizedStakeConfigId = String(stakeConfigId || "").trim();
  const poolKey = normalizedStakeConfigId
    ? `stake_${normalizedStakeConfigId}`
    : `does_${safeInt(stakeDoes)}`;
  return db.collection(DUEL_MATCHMAKING_POOLS_COLLECTION).doc(poolKey);
}

function setDuelMatchmakingPoolOpen(tx, poolRef, roomId, stakeConfigId = "", stakeDoes = 0) {
  tx.set(poolRef, {
    openRoomId: String(roomId || "").trim(),
    stakeConfigId: String(stakeConfigId || "").trim(),
    stakeDoes: safeInt(stakeDoes),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  }, { merge: true });
}

function clearDuelMatchmakingPool(tx, poolRef) {
  tx.set(poolRef, {
    openRoomId: "",
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  }, { merge: true });
}

function resolveDuelWaitingDeadlineMs(room = {}, nowMs = Date.now()) {
  const explicit = safeSignedInt(room.waitingDeadlineMs);
  if (explicit > 0) return explicit;
  const createdAtMs = safeSignedInt(room.createdAtMs);
  const waitMs = isFriendDuelRoom(room) ? FRIEND_ROOM_WAIT_MS : ROOM_WAIT_MS;
  if (createdAtMs > 0) return createdAtMs + waitMs;
  return nowMs + waitMs;
}

function getDuelStakeConfigByAmount(stakeDoes) {
  const targetStakeDoes = safeInt(stakeDoes);
  return DEFAULT_DUEL_STAKE_OPTIONS.find((item) => item.enabled !== false && safeInt(item.stakeDoes) === targetStakeDoes) || null;
}

async function findActiveDuelRoomForUser(uid) {
  const rooms = db.collection(DUEL_ROOMS_COLLECTION);
  const membershipSnap = await rooms
    .where("playerUids", "array-contains", uid)
    .limit(8)
    .get();

  if (membershipSnap.empty) return null;

  let playingCandidate = null;
  let waitingCandidate = null;

  membershipSnap.docs.forEach((docSnap) => {
    const data = docSnap.data() || {};
    if (getBlockedRejoinSet(data).has(uid)) return;
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
    roomMode: String(data.roomMode || "duel_2p"),
    stakeDoes: safeInt(data.entryCostDoes || data.stakeDoes),
    inviteCode: String(data.inviteCode || "").trim(),
  };
}

function normalizeDuelExcludedRoomIds(value) {
  const rawItems = Array.isArray(value) ? value : [value];
  return Array.from(new Set(
    rawItems
      .map((item) => String(item || "").trim())
      .filter(Boolean)
  )).slice(0, 8);
}

function buildDuelPresenceUpdates(room = {}, actorUid = "", nowMs = Date.now()) {
  const currentPresence = room.roomPresenceMs && typeof room.roomPresenceMs === "object"
    ? { ...room.roomPresenceMs }
    : {};
  const playerUids = Array.from({ length: 2 }, (_, idx) => String((room.playerUids || [])[idx] || ""));
  const playerNames = Array.from({ length: 2 }, (_, idx) => String((room.playerNames || [])[idx] || ""));
  const seats = { ...getRoomSeats(room) };
  const takeoverSeats = getBotTakeoverSeatSet(room);
  const graceUntil = room.botGraceUntilMs && typeof room.botGraceUntilMs === "object"
    ? { ...room.botGraceUntilMs }
    : {};
  const blockedRejoinUids = Array.from(getBlockedRejoinSet(room));
  const actor = String(actorUid || "").trim();
  const actorSeat = actor ? getSeatForUser(room, actor) : -1;

  let changed = false;
  let removedAny = false;

  if (actor) {
    currentPresence[actor] = nowMs;
    changed = true;
    if (takeoverSeats.has(actorSeat)) {
      takeoverSeats.delete(actorSeat);
      delete graceUntil[String(actorSeat)];
    }
  }

  for (let seat = 0; seat < 2; seat += 1) {
    const seatUid = String(playerUids[seat] || "").trim();
    if (!seatUid || seatUid === actor) continue;

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
    updates.botCount = Math.max(0, 2 - humans);
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

  const currentPlayerSeat = safeInt(room.currentPlayer);
  const shouldNudgeBots = String(room.status || "") === "playing"
    && room.startRevealPending !== true
    && currentPlayerSeat >= 0
    && currentPlayerSeat < 2
    && !isSeatHuman(effectiveRoom, currentPlayerSeat);
  const shouldResolveExpiredHumanTurn = String(room.status || "") === "playing"
    && room.startRevealPending !== true
    && currentPlayerSeat >= 0
    && currentPlayerSeat < 2
    && isSeatHuman(effectiveRoom, currentPlayerSeat)
    && resolveDuelTurnDeadlineMs(room, nowMs) <= nowMs;

  return {
    changed,
    removedAny,
    updates,
    shouldNudgeBots,
    shouldResolveExpiredHumanTurn,
    takeoverCount: updates.botTakeoverSeats.length,
  };
}

function applyDuelLeaveForUidTx(tx, roomRefDoc, room = {}, uid = "") {
  const safeUid = String(uid || "").trim();
  const currentUids = Array.from({ length: 2 }, (_, idx) => String((room.playerUids || [])[idx] || ""));
  if (!safeUid || !currentUids.includes(safeUid)) {
    return {
      result: {
        ok: true,
        deleted: false,
        status: String(room.status || ""),
      },
      shouldCleanup: false,
      shouldNudgeBots: false,
    };
  }

  const status = String(room.status || "");
  const seatIndex = currentUids.findIndex((candidate) => candidate === safeUid);
  const nextPlayerUids = currentUids.slice();
  if (seatIndex >= 0) nextPlayerUids[seatIndex] = "";
  const currentNames = Array.from({ length: 2 }, (_, idx) => String((room.playerNames || [])[idx] || ""));
  const nextPlayerNames = currentNames.slice();
  if (seatIndex >= 0) {
    nextPlayerNames[seatIndex] = status === "playing" ? botSeatLabel(seatIndex) : "";
  }
  const nextSeats = { ...getRoomSeats(room) };
  delete nextSeats[safeUid];
  const blockedRejoinUids = Array.from(getBlockedRejoinSet(room));
  if (!blockedRejoinUids.includes(safeUid)) {
    blockedRejoinUids.push(safeUid);
  }
  const nextPresence = room.roomPresenceMs && typeof room.roomPresenceMs === "object"
    ? { ...room.roomPresenceMs }
    : {};
  delete nextPresence[safeUid];
  const nextBotTakeoverSeats = getBotTakeoverSeatSet(room);
  nextBotTakeoverSeats.delete(seatIndex);
  const nextGraceUntil = room.botGraceUntilMs && typeof room.botGraceUntilMs === "object"
    ? { ...room.botGraceUntilMs }
    : {};
  delete nextGraceUntil[String(seatIndex)];
  const humans = nextPlayerUids.filter(Boolean).length;

  if (humans <= 0) {
    tx.set(roomRefDoc, {
      status: "closing",
      playerUids: ["", ""],
      playerNames: ["", ""],
      blockedRejoinUids,
      seats: {},
      roomPresenceMs: nextPresence,
      humanCount: 0,
      botCount: 2,
      botTakeoverSeats: [],
      botGraceUntilMs: admin.firestore.FieldValue.delete(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
    return {
      result: {
        ok: true,
        deleted: true,
        status: "closing",
      },
      shouldCleanup: true,
      shouldNudgeBots: false,
    };
  }

  const nextAckUids = Array.isArray(room.startRevealAckUids)
    ? room.startRevealAckUids.map((item) => String(item || "").trim()).filter(Boolean).filter((item) => item !== safeUid)
    : [];
  const revealPending = room.startRevealPending === true;
  const revealReady = revealPending === true
    && nextPlayerUids.filter(Boolean).every((playerUid) => nextAckUids.includes(playerUid));
  const nextBotCount = Math.max(0, 2 - humans);
  const updates = {
    playerUids: nextPlayerUids,
    playerNames: nextPlayerNames,
    blockedRejoinUids,
    seats: nextSeats,
    roomPresenceMs: nextPresence,
    humanCount: humans,
    botCount: nextBotCount,
    botTakeoverSeats: Array.from(nextBotTakeoverSeats),
    botGraceUntilMs: Object.keys(nextGraceUntil).length > 0 ? nextGraceUntil : admin.firestore.FieldValue.delete(),
    startRevealAckUids: nextAckUids,
    startRevealPending: revealPending === true ? !revealReady : false,
    ownerUid: room.ownerUid === safeUid
      ? String(nextPlayerUids.find(Boolean) || "")
      : String(room.ownerUid || ""),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };

  tx.update(roomRefDoc, updates);

  const effectiveRoom = {
    ...room,
    playerUids: nextPlayerUids,
    playerNames: nextPlayerNames,
    seats: nextSeats,
    botTakeoverSeats: updates.botTakeoverSeats,
  };
  const currentPlayerSeat = safeInt(room.currentPlayer);
  const shouldNudgeBots = status === "playing"
    && updates.startRevealPending !== true
    && currentPlayerSeat >= 0
    && currentPlayerSeat < 2
    && !isSeatHuman(effectiveRoom, currentPlayerSeat);

  return {
    result: {
      ok: true,
      deleted: false,
      status: status,
      humanCount: humans,
      botCount: nextBotCount,
      revealPending: updates.startRevealPending === true,
    },
    shouldCleanup: false,
    shouldNudgeBots,
  };
}

async function forceRemoveUserFromDuelRoom(roomId = "", uid = "") {
  const safeRoomId = String(roomId || "").trim();
  const safeUid = String(uid || "").trim();
  if (!safeRoomId || !safeUid) {
    return { ok: true, deleted: false, status: "skipped" };
  }

  const roomRefDoc = duelRoomRef(safeRoomId);
  const outcome = await db.runTransaction(async (tx) => {
    const roomSnap = await tx.get(roomRefDoc);
    if (!roomSnap.exists) {
      return {
        result: {
          ok: true,
          deleted: true,
          status: "missing",
        },
        shouldCleanup: false,
        shouldNudgeBots: false,
      };
    }
    return applyDuelLeaveForUidTx(tx, roomRefDoc, roomSnap.data() || {}, safeUid);
  });

  if (outcome?.shouldNudgeBots) {
    await processPendingBotTurnsDuel(safeRoomId);
  }

  if (outcome?.shouldCleanup) {
    await cleanupDuelRoom(roomRefDoc);
    return {
      ok: true,
      deleted: true,
      status: "deleted",
    };
  }

  return outcome?.result || { ok: true, deleted: false, status: "left" };
}

function buildDuelSeatHands(deckOrder = []) {
  if (!Array.isArray(deckOrder) || deckOrder.length !== 28) return null;
  return [
    deckOrder.slice(0, 7),
    deckOrder.slice(7, 14),
  ].map((hand) => hand.every((tileId) => Number.isFinite(Number(tileId)) && TILE_VALUES[Number(tileId)]) ? hand.map((tileId) => Math.trunc(Number(tileId))) : []);
}

function buildDuelStockPile(deckOrder = []) {
  if (!Array.isArray(deckOrder) || deckOrder.length !== 28) return [];
  return deckOrder.slice(14).map((tileId) => Math.trunc(Number(tileId))).filter((tileId) => TILE_VALUES[tileId]);
}

function cloneDuelSeatHands(seatHands) {
  return Array.isArray(seatHands)
    ? seatHands.map((hand) => (Array.isArray(hand) ? hand.slice() : []))
    : [[], []];
}

function serializeDuelSeatHands(seatHands) {
  const normalized = cloneDuelSeatHands(seatHands);
  return {
    "0": Array.isArray(normalized[0]) ? normalized[0].slice() : [],
    "1": Array.isArray(normalized[1]) ? normalized[1].slice() : [],
  };
}

function normalizeDuelSeatHands(raw, fallbackDeckOrder = []) {
  const fallback = buildDuelSeatHands(fallbackDeckOrder) || [[], []];
  let source = null;

  if (Array.isArray(raw) && raw.length === 2) {
    source = raw;
  } else if (raw && typeof raw === "object") {
    source = [raw["0"] ?? raw[0] ?? null, raw["1"] ?? raw[1] ?? null];
  }

  if (!Array.isArray(source) || source.length !== 2) {
    return fallback;
  }

  return source.map((hand, seat) => {
    if (!Array.isArray(hand)) return fallback[seat].slice();
    return hand
      .map((tileId) => {
        if (tileId === null) return null;
        const parsed = Number(tileId);
        return Number.isFinite(parsed) && TILE_VALUES[parsed] ? Math.trunc(parsed) : null;
      })
      .filter((tileId) => tileId === null || TILE_VALUES[tileId]);
  });
}

function normalizeDuelStockPile(raw, fallbackDeckOrder = []) {
  const fallback = buildDuelStockPile(fallbackDeckOrder);
  if (!Array.isArray(raw)) return fallback;
  return raw
    .map((tileId) => Number(tileId))
    .filter((tileId) => Number.isFinite(tileId) && TILE_VALUES[tileId])
    .map((tileId) => Math.trunc(tileId));
}

function findDuelSeatWithTile(seatHands, tileId) {
  for (let seat = 0; seat < 2; seat += 1) {
    const hand = Array.isArray(seatHands?.[seat]) ? seatHands[seat] : [];
    for (let slot = 0; slot < hand.length; slot += 1) {
      if (hand[slot] === tileId) return seat;
    }
  }
  return -1;
}

function findDuelSeatSlotByTileId(seatHands, seat, tileId) {
  const hand = Array.isArray(seatHands?.[seat]) ? seatHands[seat] : [];
  for (let slot = 0; slot < hand.length; slot += 1) {
    if (hand[slot] === tileId) return slot;
  }
  return -1;
}

function countRemainingTilesForDuelSeat(seatHands, seat) {
  const hand = Array.isArray(seatHands?.[seat]) ? seatHands[seat] : [];
  return hand.reduce((count, tileId) => count + (tileId === null ? 0 : 1), 0);
}

function sumDuelSeatPips(seatHands, seat) {
  const hand = Array.isArray(seatHands?.[seat]) ? seatHands[seat] : [];
  return hand.reduce((sum, tileId) => {
    if (tileId === null) return sum;
    const values = getTileValues(tileId);
    return values ? sum + values[0] + values[1] : sum;
  }, 0);
}

function computeBlockedWinnerSeatForDuel(seatHands) {
  let bestSeat = 0;
  let bestScore = Number.POSITIVE_INFINITY;
  for (let seat = 0; seat < 2; seat += 1) {
    const score = sumDuelSeatPips(seatHands, seat);
    if (score < bestScore) {
      bestScore = score;
      bestSeat = seat;
    }
  }
  return bestSeat;
}

function compareDuelOpeningTiles(leftTileId, rightTileId) {
  const leftValues = getTileValues(leftTileId) || [0, 0];
  const rightValues = getTileValues(rightTileId) || [0, 0];
  const leftIsDouble = leftValues[0] === leftValues[1];
  const rightIsDouble = rightValues[0] === rightValues[1];
  if (leftIsDouble !== rightIsDouble) return leftIsDouble ? 1 : -1;

  if (leftIsDouble && rightIsDouble) {
    if (leftValues[0] !== rightValues[0]) return leftValues[0] > rightValues[0] ? 1 : -1;
    return 0;
  }

  const leftSum = leftValues[0] + leftValues[1];
  const rightSum = rightValues[0] + rightValues[1];
  if (leftSum !== rightSum) return leftSum > rightSum ? 1 : -1;

  const leftHigh = Math.max(leftValues[0], leftValues[1]);
  const rightHigh = Math.max(rightValues[0], rightValues[1]);
  if (leftHigh !== rightHigh) return leftHigh > rightHigh ? 1 : -1;

  const leftLow = Math.min(leftValues[0], leftValues[1]);
  const rightLow = Math.min(rightValues[0], rightValues[1]);
  if (leftLow !== rightLow) return leftLow > rightLow ? 1 : -1;
  return 0;
}

function resolveDuelOpeningConfig(seatHands = [[], []]) {
  let bestDouble = null;
  let bestNonDouble = null;

  for (let seat = 0; seat < 2; seat += 1) {
    const hand = Array.isArray(seatHands?.[seat]) ? seatHands[seat] : [];
    for (let slot = 0; slot < hand.length; slot += 1) {
      const tileId = safeSignedInt(hand[slot], -1);
      const values = getTileValues(tileId);
      if (!values) continue;
      const candidate = { seat, slot, tileId };
      if (values[0] === values[1]) {
        if (!bestDouble || compareDuelOpeningTiles(tileId, bestDouble.tileId) > 0) {
          bestDouble = candidate;
        }
      } else if (!bestNonDouble || compareDuelOpeningTiles(tileId, bestNonDouble.tileId) > 0) {
        bestNonDouble = candidate;
      }
    }
  }

  const selected = bestDouble || bestNonDouble;
  if (!selected) {
    return { seat: 0, slot: 0, tileId: 27, reason: "double_six" };
  }

  let reason = "highest_sum";
  const values = getTileValues(selected.tileId) || [0, 0];
  if (values[0] === values[1]) {
    reason = selected.tileId === 27 ? "double_six" : "highest_double";
  }

  return {
    seat: selected.seat,
    slot: selected.slot,
    tileId: selected.tileId,
    reason,
  };
}

function getLegalMovesForDuelSeat(state, seat) {
  const moves = [];
  const hand = Array.isArray(state?.seatHands?.[seat]) ? state.seatHands[seat] : [];
  const openingMove = safeSignedInt(state?.appliedActionSeq) < 0;
  const openingTileId = safeSignedInt(state?.openingTileId, 27);

  for (let slot = 0; slot < hand.length; slot += 1) {
    const tileId = hand[slot];
    if (tileId === null) continue;
    const values = getTileValues(tileId);
    if (!values) continue;

    if (openingMove) {
      if (tileId === openingTileId) {
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

function normalizeDuelGameState(raw = {}, room = {}) {
  const deckOrder = Array.isArray(raw.deckOrder) && raw.deckOrder.length === 28
    ? raw.deckOrder.slice(0, 28)
    : (Array.isArray(room.deckOrder) ? room.deckOrder.slice(0, 28) : makeDeckOrder());
  const seatHands = normalizeDuelSeatHands(raw.seatHands, deckOrder);
  const stockPile = normalizeDuelStockPile(raw.stockPile, deckOrder);
  const appliedActionSeq = Number.isFinite(Number(raw.appliedActionSeq)) ? Math.trunc(Number(raw.appliedActionSeq)) : -1;
  const winnerSeat = Number.isFinite(Number(raw.winnerSeat)) ? Math.trunc(Number(raw.winnerSeat)) : -1;
  const openingConfig = resolveDuelOpeningConfig(seatHands);
  const openingSeat = Number.isFinite(Number(raw.openingSeat)) ? Math.trunc(Number(raw.openingSeat)) : openingConfig.seat;
  const openingTileId = Number.isFinite(Number(raw.openingTileId)) ? Math.trunc(Number(raw.openingTileId)) : openingConfig.tileId;
  const openingReason = sanitizeText(raw.openingReason || openingConfig.reason || "", 40);
  const currentPlayer = Number.isFinite(Number(raw.currentPlayer))
    ? Math.trunc(Number(raw.currentPlayer))
    : Math.max(0, openingSeat);

  return {
    deckOrder,
    seatHands,
    stockPile,
    leftEnd: Number.isFinite(Number(raw.leftEnd)) ? Math.trunc(Number(raw.leftEnd)) : null,
    rightEnd: Number.isFinite(Number(raw.rightEnd)) ? Math.trunc(Number(raw.rightEnd)) : null,
    passesInRow: safeInt(raw.passesInRow),
    appliedActionSeq,
    currentPlayer,
    openingSeat,
    openingTileId,
    openingReason,
    winnerSeat,
    winnerUid: String(raw.winnerUid || "").trim(),
    endedReason: sanitizeText(raw.endedReason || "", 40),
    idempotencyKeys: raw.idempotencyKeys && typeof raw.idempotencyKeys === "object" ? { ...raw.idempotencyKeys } : {},
  };
}

function createInitialDuelGameState(room = {}, deckOrder = []) {
  const cleanDeckOrder = Array.isArray(deckOrder) && deckOrder.length === 28 ? deckOrder.slice(0, 28) : makeDeckOrder();
  const seatHands = buildDuelSeatHands(cleanDeckOrder) || [[], []];
  const openingConfig = resolveDuelOpeningConfig(seatHands);
  return {
    deckOrder: cleanDeckOrder,
    seatHands,
    stockPile: buildDuelStockPile(cleanDeckOrder),
    leftEnd: null,
    rightEnd: null,
    passesInRow: 0,
    appliedActionSeq: -1,
    currentPlayer: Math.max(0, openingConfig.seat),
    openingSeat: openingConfig.seat,
    openingTileId: openingConfig.tileId,
    openingReason: openingConfig.reason,
    winnerSeat: -1,
    winnerUid: "",
    endedReason: "",
    idempotencyKeys: {},
  };
}

function buildOpeningMoveForDuelState(state) {
  const liveState = normalizeDuelGameState(state);
  const openingSeat = safeSignedInt(liveState.openingSeat, -1);
  const openingTileId = safeSignedInt(liveState.openingTileId, -1);
  if (openingSeat < 0 || openingSeat > 1) {
    throw new HttpsError("failed-precondition", "Impossible de determiner qui doit commencer le duel.");
  }
  const legalMoves = getLegalMovesForDuelSeat(liveState, openingSeat);
  const openingMove = legalMoves.find((move) => move.tileId === openingTileId) || null;
  if (!openingMove) {
    throw new HttpsError("failed-precondition", "La tuile d'ouverture ne peut pas ouvrir le duel.");
  }
  return {
    type: "play",
    player: openingSeat,
    tileId: openingMove.tileId,
    tilePos: openingMove.slot,
    tileLeft: openingMove.tileLeft,
    tileRight: openingMove.tileRight,
    side: openingMove.side,
    branch: openingMove.branch,
    slot: openingMove.slot,
  };
}

function buildDuelPassMove(seat) {
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

function buildDuelDrawMove(seat, tileId = null) {
  return {
    type: "draw",
    player: seat,
    tileId: Number.isFinite(tileId) ? Math.trunc(tileId) : null,
    tilePos: null,
    tileLeft: null,
    tileRight: null,
    side: null,
    branch: "",
    slot: -1,
  };
}

function pickRandomItem(items = []) {
  if (!Array.isArray(items) || items.length <= 0) return null;
  return items[randomInt(0, items.length - 1)] || null;
}

function getDuelTurnStartedAtMs(room = {}) {
  const directMs = safeSignedInt(room?.turnStartedAtMs);
  if (directMs > 0) return directMs;
  const tsValue = room?.turnStartedAt;
  if (tsValue && typeof tsValue.toMillis === "function") {
    const millis = safeSignedInt(tsValue.toMillis());
    if (millis > 0) return millis;
  }
  if (tsValue && Number.isFinite(Number(tsValue?._seconds))) {
    return safeSignedInt(Number(tsValue._seconds) * 1000);
  }
  return 0;
}

function resolveDuelTurnDeadlineMs(room = {}, nowMs = Date.now()) {
  const startedAtMs = getDuelTurnStartedAtMs(room);
  if (startedAtMs > 0) return startedAtMs + DUEL_TURN_LIMIT_MS;
  const safeNowMs = safeSignedInt(nowMs) || Date.now();
  return safeNowMs + DUEL_TURN_LIMIT_MS;
}

function getOtherDuelSeat(seat) {
  return seat === 0 ? 1 : 0;
}

function countValueMatchesInDuelSeatHand(state, seat, value) {
  if (!Number.isFinite(Number(value))) return 0;
  const target = Math.trunc(Number(value));
  const hand = Array.isArray(state?.seatHands?.[seat]) ? state.seatHands[seat] : [];
  return hand.reduce((count, tileId) => {
    if (tileId === null) return count;
    const values = getTileValues(tileId);
    if (!values) return count;
    return count + ((values[0] === target || values[1] === target) ? 1 : 0);
  }, 0);
}

function countValueMatchesInDuelStock(state, value) {
  if (!Number.isFinite(Number(value))) return 0;
  const target = Math.trunc(Number(value));
  const stock = Array.isArray(state?.stockPile) ? state.stockPile : [];
  return stock.reduce((count, tileId) => {
    const values = getTileValues(tileId);
    if (!values) return count;
    return count + ((values[0] === target || values[1] === target) ? 1 : 0);
  }, 0);
}

function countImmediateWinThreatForDuel(state, seat) {
  if (seat < 0 || seat > 1) return 0;
  if (countRemainingTilesForDuelSeat(state?.seatHands, seat) !== 1) return 0;
  return getLegalMovesForDuelSeat(state, seat).length > 0 ? 1 : 0;
}

function scoreDuelStateForSeat(room, state, perspectiveSeat) {
  const winnerSeat = Number.isFinite(Number(state?.winnerSeat))
    ? Math.trunc(Number(state.winnerSeat))
    : -1;
  if (winnerSeat >= 0) {
    return winnerSeat === perspectiveSeat ? 1_000_000 : -1_000_000;
  }

  const opponentSeat = getOtherDuelSeat(perspectiveSeat);
  const selfTiles = countRemainingTilesForDuelSeat(state?.seatHands, perspectiveSeat);
  const opponentTiles = countRemainingTilesForDuelSeat(state?.seatHands, opponentSeat);
  const selfPips = sumDuelSeatPips(state?.seatHands, perspectiveSeat);
  const opponentPips = sumDuelSeatPips(state?.seatHands, opponentSeat);
  const selfLegal = getLegalMovesForDuelSeat(state, perspectiveSeat).length;
  const opponentLegal = getLegalMovesForDuelSeat(state, opponentSeat).length;
  const stockCount = Array.isArray(state?.stockPile) ? state.stockPile.length : 0;
  const leftEnd = Number.isFinite(Number(state?.leftEnd)) ? Math.trunc(Number(state.leftEnd)) : null;
  const rightEnd = Number.isFinite(Number(state?.rightEnd)) ? Math.trunc(Number(state.rightEnd)) : null;

  const selfLeftMatches = countValueMatchesInDuelSeatHand(state, perspectiveSeat, leftEnd);
  const selfRightMatches = countValueMatchesInDuelSeatHand(state, perspectiveSeat, rightEnd);
  const opponentLeftMatches = countValueMatchesInDuelSeatHand(state, opponentSeat, leftEnd);
  const opponentRightMatches = countValueMatchesInDuelSeatHand(state, opponentSeat, rightEnd);
  const stockLeftMatches = countValueMatchesInDuelStock(state, leftEnd);
  const stockRightMatches = countValueMatchesInDuelStock(state, rightEnd);

  let score = 0;
  score += (7 - selfTiles) * 440;
  score -= selfTiles * 150;
  score -= selfPips * (stockCount <= 0 ? 30 : 18);
  score += opponentTiles * 125;
  score += opponentPips * (stockCount <= 0 ? 12 : 5);
  score += selfLegal * 38;
  score -= opponentLegal * 110;

  if (leftEnd !== null || rightEnd !== null) {
    score += selfLeftMatches * 34;
    score -= opponentLeftMatches * 76;
    if (rightEnd !== leftEnd) {
      score += selfRightMatches * 34;
      score -= opponentRightMatches * 76;
    }
  }

  if (leftEnd !== null) {
    const opponentBlockedLeft = opponentLeftMatches <= 0;
    const selfBlockedLeft = selfLeftMatches <= 0;
    if (opponentBlockedLeft) {
      score += 115 + Math.max(0, 55 - (stockLeftMatches * 10));
    }
    if (selfBlockedLeft) {
      score -= 90 + Math.max(0, 40 - (stockLeftMatches * 8));
    }
  }

  if (rightEnd !== null && rightEnd !== leftEnd) {
    const opponentBlockedRight = opponentRightMatches <= 0;
    const selfBlockedRight = selfRightMatches <= 0;
    if (opponentBlockedRight) {
      score += 115 + Math.max(0, 55 - (stockRightMatches * 10));
    }
    if (selfBlockedRight) {
      score -= 90 + Math.max(0, 40 - (stockRightMatches * 8));
    }
  }

  if (opponentLegal === 0) {
    const rescueCount = stockLeftMatches + ((rightEnd !== leftEnd) ? stockRightMatches : 0);
    score += 260 + Math.max(0, 140 - (rescueCount * 10));
  }
  if (selfLegal === 0) {
    const rescueCount = stockLeftMatches + ((rightEnd !== leftEnd) ? stockRightMatches : 0);
    score -= 220 + Math.max(0, 120 - (rescueCount * 10));
  }

  const selfThreat = countImmediateWinThreatForDuel(state, perspectiveSeat);
  const opponentThreat = countImmediateWinThreatForDuel(state, opponentSeat);
  if (selfThreat > 0) score += 1_600;
  if (opponentThreat > 0) score -= 2_400;

  if (stockCount <= 0 && safeInt(state?.passesInRow) >= 1) {
    score += (opponentPips - selfPips) * 24;
  }

  const nextSeat = safeSignedInt(state?.currentPlayer, -1);
  if (nextSeat === perspectiveSeat) {
    score += 70;
  } else if (nextSeat === opponentSeat) {
    score -= 45;
  }

  return score;
}

function buildDuelBotCandidateMoves(state, seat) {
  const legalMoves = getLegalMovesForDuelSeat(state, seat);
  if (legalMoves.length > 0) {
    return legalMoves.map((move) => buildPlayMoveFromDuelLegal(seat, move));
  }

  if (Array.isArray(state?.stockPile) && state.stockPile.length > 0) {
    // The bot must not inspect every tile left in the stock to "choose" the best draw.
    // A draw from the lot should behave like a genuine random pick, just like a human turn.
    return [buildDuelDrawMove(seat)];
  }

  return [buildDuelPassMove(seat)];
}

function scoreDuelMoveTieBreaker(state, move) {
  if (!move) return Number.NEGATIVE_INFINITY;
  if (move.type === "pass") return -1_000;

  if (move.type === "draw" && !Number.isFinite(Number(move.tileId))) {
    return 1_000;
  }

  const values = getTileValues(move.tileId) || [0, 0];
  const pipSum = values[0] + values[1];
  const isDouble = values[0] === values[1];
  let score = move.type === "play" ? 5_000 : 1_000;
  score += pipSum * 11;
  if (isDouble) score += 90 + (values[0] * 14);

  if (move.type === "draw") {
    const leftEnd = Number.isFinite(Number(state?.leftEnd)) ? Math.trunc(Number(state.leftEnd)) : null;
    const rightEnd = Number.isFinite(Number(state?.rightEnd)) ? Math.trunc(Number(state.rightEnd)) : null;
    if (leftEnd !== null && (values[0] === leftEnd || values[1] === leftEnd)) {
      score += 260;
    }
    if (rightEnd !== null && rightEnd !== leftEnd && (values[0] === rightEnd || values[1] === rightEnd)) {
      score += 260;
    }
  }

  return score;
}

function evaluateDuelFutureForSeat(room, state, perspectiveSeat, remainingPlies) {
  const baseScore = scoreDuelStateForSeat(room, state, perspectiveSeat);
  if (remainingPlies <= 0 || safeSignedInt(state?.winnerSeat, -1) >= 0) {
    return baseScore;
  }

  const actor = safeSignedInt(state?.currentPlayer, -1);
  if (actor < 0 || actor > 1) {
    return baseScore;
  }

  let predictedMove = null;
  try {
    predictedMove = chooseStrategicDuelMove(room, state, actor, { lookaheadPlies: 0 });
  } catch (_) {
    return baseScore;
  }
  if (!predictedMove) return baseScore;

  try {
    const predictedResult = applyResolvedDuelMove(state, room, predictedMove, `server:bot:duel:sim:${actor}`);
    return baseScore + (0.68 * evaluateDuelFutureForSeat(
      room,
      predictedResult.state,
      perspectiveSeat,
      remainingPlies - 1
    ));
  } catch (_) {
    return baseScore;
  }
}

function chooseStrategicDuelMove(room, state, seat, options = {}) {
  const liveState = normalizeDuelGameState(state, room);
  const requestedLookahead = Number.isFinite(Number(options?.lookaheadPlies))
    ? Math.max(0, Math.trunc(Number(options.lookaheadPlies)))
    : DUEL_ELITE_LOOKAHEAD_PLIES;
  const candidateMoves = buildDuelBotCandidateMoves(liveState, seat);
  if (candidateMoves.length <= 0) {
    return buildDuelPassMove(seat);
  }

  let bestMove = candidateMoves[0];
  let bestScore = Number.NEGATIVE_INFINITY;
  let bestTieBreaker = Number.NEGATIVE_INFINITY;

  for (let index = 0; index < candidateMoves.length; index += 1) {
    const candidate = candidateMoves[index];
    let simulated = null;

    try {
      simulated = applyResolvedDuelMove(liveState, room, candidate, `server:bot:duel:eval:${seat}`);
    } catch (_) {
      continue;
    }

    let score = scoreDuelStateForSeat(room, simulated.state, seat);
    const tileValues = getTileValues(candidate.tileId) || [0, 0];
    const pipSum = tileValues[0] + tileValues[1];

    if (candidate.type === "play") {
      score += pipSum * 15;
      if (tileValues[0] === tileValues[1]) score += 45 + (tileValues[0] * 12);
      if (getLegalMovesForDuelSeat(simulated.state, getOtherDuelSeat(seat)).length === 0) {
        score += 220;
      }
    } else if (candidate.type === "draw") {
      score += pipSum * 7;
      const drawnTileId = safeSignedInt(simulated.record?.drawnTileIds?.[0], -1);
      const legalAfterDraw = getLegalMovesForDuelSeat(simulated.state, seat);
      score += legalAfterDraw.length * 20;
      if (drawnTileId >= 0 && legalAfterDraw.some((move) => move.tileId === drawnTileId)) {
        score += 260;
      }
    } else {
      score -= 320;
    }

    if (requestedLookahead > 0 && simulated.state.winnerSeat < 0) {
      score += 0.74 * evaluateDuelFutureForSeat(room, simulated.state, seat, requestedLookahead - 1);
    }

    const tieBreaker = scoreDuelMoveTieBreaker(liveState, candidate);
    if (score > bestScore || (score === bestScore && tieBreaker > bestTieBreaker)) {
      bestScore = score;
      bestMove = candidate;
      bestTieBreaker = tieBreaker;
    }
  }

  return bestMove;
}

function buildExpiredHumanDuelMoves(room, state) {
  const liveState = normalizeDuelGameState(state, room);
  const seat = safeSignedInt(liveState.currentPlayer, -1);
  if (seat < 0 || seat > 1) {
    throw new HttpsError("failed-precondition", "Impossible de resoudre le tour duel expire.");
  }

  const plannedMoves = [];
  const legalMoves = getLegalMovesForDuelSeat(liveState, seat);
  if (legalMoves.length > 0) {
    const selectedMove = pickRandomItem(legalMoves);
    if (!selectedMove) {
      throw new HttpsError("failed-precondition", "Impossible de choisir un coup duel expire.");
    }
    plannedMoves.push(buildPlayMoveFromDuelLegal(seat, selectedMove));
    return plannedMoves;
  }

  if (Array.isArray(liveState.stockPile) && liveState.stockPile.length > 0) {
    plannedMoves.push(buildDuelDrawMove(seat));
    return plannedMoves;
  }

  plannedMoves.push(buildDuelPassMove(seat));
  return plannedMoves;
}

function inferDuelResultBotCount(data = {}) {
  const storedBotCount = safeInt(data.botCount);
  if (storedBotCount > 0) return storedBotCount;
  const humanCount = safeInt(data.humanCount);
  if (humanCount >= 0 && humanCount <= 2) {
    return Math.max(0, 2 - humanCount);
  }
  const playerUids = Array.isArray(data.playerUids)
    ? data.playerUids.map((item) => String(item || "").trim()).filter(Boolean)
    : [];
  return Math.max(0, 2 - playerUids.length);
}

function buildPlayMoveFromDuelLegal(seat, move) {
  return {
    type: "play",
    player: seat,
    tileId: move.tileId,
    tilePos: move.slot,
    tileLeft: move.tileLeft,
    tileRight: move.tileRight,
    side: move.side,
    branch: move.branch,
    slot: move.slot,
  };
}

function resolveRequestedDuelMove(state, seat, rawAction = {}) {
  const type = String(rawAction?.type || "").trim();
  if (type !== "play" && type !== "pass" && type !== "draw") {
    throw new HttpsError("invalid-argument", "Type d'action duel invalide.");
  }

  const legalMoves = getLegalMovesForDuelSeat(state, seat);
  const stockCount = Array.isArray(state?.stockPile) ? state.stockPile.length : 0;

  if (type === "pass") {
    if (legalMoves.length > 0) {
      throw new HttpsError("failed-precondition", "Pass interdit tant qu'un coup legal existe.");
    }
    if (stockCount > 0) {
      throw new HttpsError("failed-precondition", "Tu dois piocher tant qu'il reste des dominos dans le lot.");
    }
    return buildDuelPassMove(seat);
  }

  if (type === "draw") {
    if (legalMoves.length > 0) {
      throw new HttpsError("failed-precondition", "Pioche interdite tant qu'un coup legal existe.");
    }
    if (stockCount <= 0) {
      throw new HttpsError("failed-precondition", "Le lot est vide. Tu dois passer.");
    }
    const requestedTileId = Number(rawAction?.tileId);
    const tileId = Number.isFinite(requestedTileId) ? Math.trunc(requestedTileId) : -1;
    if (!TILE_VALUES[tileId]) {
      throw new HttpsError("invalid-argument", "Tuile de pioche duel invalide.");
    }
    if (!Array.isArray(state?.stockPile) || !state.stockPile.includes(tileId)) {
      throw new HttpsError("failed-precondition", "Cette tuile n'est plus disponible dans le lot.");
    }
    return buildDuelDrawMove(seat, tileId);
  }

  const hand = Array.isArray(state?.seatHands?.[seat]) ? state.seatHands[seat] : [];
  let tileId = Number(rawAction?.tileId);
  let slot = Number.isFinite(tileId) ? findDuelSeatSlotByTileId(state.seatHands, seat, Math.trunc(tileId)) : -1;
  tileId = Number.isFinite(tileId) ? Math.trunc(tileId) : -1;

  const tilePosRaw = Number(rawAction?.tilePos);
  if ((slot < 0 || !TILE_VALUES[tileId]) && Number.isFinite(tilePosRaw)) {
    const tilePos = Math.trunc(tilePosRaw);
    if (tilePos < 0 || tilePos >= hand.length) {
      throw new HttpsError("permission-denied", "Tuile duel invalide.");
    }
    const tileAtSlot = hand[tilePos];
    if (tileAtSlot === null || !TILE_VALUES[tileAtSlot]) {
      throw new HttpsError("failed-precondition", "Cette tuile n'est plus dans ta main.");
    }
    tileId = tileAtSlot;
    slot = tilePos;
  }

  if (slot < 0 || !TILE_VALUES[tileId]) {
    throw new HttpsError("failed-precondition", "Tuile introuvable dans la main duel.");
  }

  const matchingMoves = legalMoves.filter((move) => move.tileId === tileId && move.slot === slot);
  if (matchingMoves.length === 0) {
    throw new HttpsError("failed-precondition", "Coup duel illegal pour cette tuile.");
  }

  const openingMove = safeSignedInt(state?.appliedActionSeq) < 0;
  const requestedSide = normalizeRequestedSide(rawAction?.side, rawAction?.branch, openingMove);
  let selectedMove = null;

  if (requestedSide && requestedSide !== "center") {
    selectedMove = matchingMoves.find((move) => move.side === requestedSide) || null;
  } else if (matchingMoves.length === 1 || openingMove) {
    selectedMove = matchingMoves[0];
  }

  if (!selectedMove) {
    throw new HttpsError("failed-precondition", "Precise un cote valide pour jouer cette tuile duel.");
  }

  if (openingMove) {
    const requiredOpeningTileId = safeSignedInt(state?.openingTileId, -1);
    if (requiredOpeningTileId >= 0 && tileId !== requiredOpeningTileId) {
      throw new HttpsError("failed-precondition", "Cette tuile ne peut pas ouvrir le duel.");
    }
  }

  return {
    type: "play",
    player: seat,
    tileId,
    tilePos: slot,
    tileLeft: selectedMove.tileLeft,
    tileRight: selectedMove.tileRight,
    side: selectedMove.side,
    branch: selectedMove.branch,
    slot,
  };
}

function applyResolvedDuelMove(state, room, move, actorUid) {
  const nextState = normalizeDuelGameState(state, room);
  const seq = safeInt(nextState.appliedActionSeq + 1);
  const record = {
    seq,
    type: move.type,
    player: move.player,
    tileId: move.tileId,
    tilePos: move.tilePos,
    tileLeft: move.tileLeft,
    tileRight: move.tileRight,
    side: move.side,
    branch: move.branch,
    resolvedPlacement: move.type === "play" ? move.branch : move.type,
    drawnTileIds: [],
    autoPlayedTileId: null,
    by: String(actorUid || ""),
  };

  if (move.type === "play") {
    const hand = Array.isArray(nextState.seatHands?.[move.player]) ? nextState.seatHands[move.player] : [];
    if (!Array.isArray(hand) || hand[move.slot] !== move.tileId) {
      throw new HttpsError("failed-precondition", "La tuile duel a deja ete consommee.");
    }
    const values = getTileValues(move.tileId);
    if (!values) {
      throw new HttpsError("failed-precondition", "Tuile duel inconnue.");
    }

    hand.splice(move.slot, 1);
    if (safeSignedInt(nextState.appliedActionSeq) < 0) {
      if (move.tileId !== safeSignedInt(nextState.openingTileId, -1)) {
        throw new HttpsError("failed-precondition", "Le duel doit commencer par la tuile d'ouverture choisie.");
      }
      nextState.leftEnd = values[0];
      nextState.rightEnd = values[1];
    } else if (move.side === "left") {
      if (values[0] !== nextState.leftEnd && values[1] !== nextState.leftEnd) {
        throw new HttpsError("failed-precondition", "Placement duel incompatible a gauche.");
      }
      nextState.leftEnd = values[0] === nextState.leftEnd ? values[1] : values[0];
    } else if (move.side === "right") {
      if (values[0] !== nextState.rightEnd && values[1] !== nextState.rightEnd) {
        throw new HttpsError("failed-precondition", "Placement duel incompatible a droite.");
      }
      nextState.rightEnd = values[0] === nextState.rightEnd ? values[1] : values[0];
    } else {
      throw new HttpsError("failed-precondition", "Cote duel invalide.");
    }

    nextState.passesInRow = 0;
    if (countRemainingTilesForDuelSeat(nextState.seatHands, move.player) === 0) {
      nextState.winnerSeat = move.player;
      nextState.winnerUid = getWinnerUidForSeat(room, move.player);
      nextState.endedReason = "out";
    }
  } else if (move.type === "draw") {
    const legalMoves = getLegalMovesForDuelSeat(nextState, move.player);
    if (legalMoves.length > 0) {
      throw new HttpsError("failed-precondition", "Pioche duel interdite tant qu'un coup legal existe.");
    }
    if (!Array.isArray(nextState.stockPile) || nextState.stockPile.length <= 0) {
      throw new HttpsError("failed-precondition", "Le lot duel est vide.");
    }

    const requestedTileId = safeSignedInt(move.tileId, -1);
    let stockIndex = nextState.stockPile.findIndex((tileId) => tileId === requestedTileId);
    const canFallbackToRandomDraw = String(actorUid || "").startsWith("server:");
    if (stockIndex < 0 && requestedTileId < 0) {
      stockIndex = randomInt(0, nextState.stockPile.length - 1);
    }
    if (stockIndex < 0 && canFallbackToRandomDraw) {
      console.warn("[DUEL_DRAW_INCIDENT] stale-requested-tile-fallback", {
        actorUid: String(actorUid || ""),
        player: safeSignedInt(move.player, -1),
        requestedTileId,
        stockCount: Array.isArray(nextState.stockPile) ? nextState.stockPile.length : 0,
        appliedActionSeq: safeSignedInt(nextState.appliedActionSeq, -1),
        currentPlayer: safeSignedInt(nextState.currentPlayer, -1),
        leftEnd: safeSignedInt(nextState.leftEnd, -1),
        rightEnd: safeSignedInt(nextState.rightEnd, -1),
      });
      stockIndex = randomInt(0, nextState.stockPile.length - 1);
    }
    if (stockIndex < 0) {
      console.warn("[DUEL_DRAW_INCIDENT] draw-rejected", {
        actorUid: String(actorUid || ""),
        player: safeSignedInt(move.player, -1),
        requestedTileId,
        stockCount: Array.isArray(nextState.stockPile) ? nextState.stockPile.length : 0,
        appliedActionSeq: safeSignedInt(nextState.appliedActionSeq, -1),
        currentPlayer: safeSignedInt(nextState.currentPlayer, -1),
        leftEnd: safeSignedInt(nextState.leftEnd, -1),
        rightEnd: safeSignedInt(nextState.rightEnd, -1),
      });
      throw new HttpsError("failed-precondition", "La tuile choisie n'est plus disponible dans le lot duel.");
    }

    const [drawnTileId] = nextState.stockPile.splice(stockIndex, 1);
    nextState.seatHands[move.player].push(drawnTileId);
    record.drawnTileIds.push(drawnTileId);
    record.tileId = drawnTileId;
    nextState.passesInRow = 0;
  } else {
    const legalMoves = getLegalMovesForDuelSeat(nextState, move.player);
    if (legalMoves.length > 0) {
      throw new HttpsError("failed-precondition", "Pass duel interdit tant qu'un coup legal existe.");
    }
    if (Array.isArray(nextState.stockPile) && nextState.stockPile.length > 0) {
      throw new HttpsError("failed-precondition", "Pass duel interdit tant qu'il reste des dominos a piocher.");
    }
    nextState.passesInRow = safeInt(nextState.passesInRow) + 1;
    if (nextState.passesInRow >= 2) {
      nextState.winnerSeat = computeBlockedWinnerSeatForDuel(nextState.seatHands);
      nextState.winnerUid = getWinnerUidForSeat(room, nextState.winnerSeat);
      nextState.endedReason = "block";
    }
  }

  nextState.appliedActionSeq = seq;
  if (nextState.winnerSeat < 0) {
    nextState.currentPlayer = (move.type === "draw") ? move.player : ((move.player + 1) % 2);
  }

  return {
    state: nextState,
    record,
    ended: nextState.winnerSeat >= 0,
  };
}

function chooseDuelBotMove(room, state, seat) {
  const difficulty = normalizeBotDifficulty(room?.botDifficulty || DEFAULT_DUEL_BOT_DIFFICULTY);
  const legalMoves = getLegalMovesForDuelSeat(state, seat);
  if (difficulty === "userpro") {
    if (legalMoves.length > 0) {
      const selectedMove = pickRandomItem(legalMoves);
      return selectedMove ? buildPlayMoveFromDuelLegal(seat, selectedMove) : buildDuelPassMove(seat);
    }
    if (Array.isArray(state?.stockPile) && state.stockPile.length > 0) {
      return buildDuelDrawMove(seat);
    }
    return buildDuelPassMove(seat);
  }

  if (difficulty === "amateur" && legalMoves.length > 1 && Math.random() < 0.28) {
    const selectedMove = pickRandomItem(legalMoves);
    if (selectedMove) {
      return buildPlayMoveFromDuelLegal(seat, selectedMove);
    }
  }

  return chooseStrategicDuelMove(room, state, seat, {
    lookaheadPlies: safeInt(DUEL_BOT_DIFFICULTY_LOOKAHEAD[difficulty]),
  });
}

function resolveDuelBotTurnLockUntilMs(room, state, nowMs = Date.now()) {
  if (!state || safeSignedInt(state?.winnerSeat) >= 0) return 0;
  if (room?.startRevealPending === true) return 0;

  const botSeat = safeSignedInt(state?.currentPlayer);
  if (botSeat < 0 || botSeat > 1 || isSeatHuman(room, botSeat)) {
    return 0;
  }

  const safeNowMs = safeSignedInt(nowMs) || Date.now();
  const currentLockUntilMs = safeSignedInt(room?.turnLockedUntilMs);
  if (currentLockUntilMs > safeNowMs) {
    return currentLockUntilMs;
  }

  const turnDeadlineMs = resolveDuelTurnDeadlineMs(room, safeNowMs);
  if (turnDeadlineMs <= safeNowMs) return 0;

  const desiredLockUntilMs = safeNowMs + computeBotThinkDelayMs(room, {
    ...state,
    seatHands: [state?.seatHands?.[0] || [], state?.seatHands?.[1] || [], [], []],
  }, botSeat);
  return Math.min(turnDeadlineMs, desiredLockUntilMs);
}

function advanceDuelBotsAndCollect(room, state, roomId, firstMove = null, actorUid = "", allowBotAdvance = true) {
  let liveState = normalizeDuelGameState(state, room);
  const records = [];
  let autoBotMoves = 0;

  const initialMoves = Array.isArray(firstMove)
    ? firstMove.filter(Boolean)
    : (firstMove ? [firstMove] : []);

  for (let moveIndex = 0; moveIndex < initialMoves.length; moveIndex += 1) {
    const result = applyResolvedDuelMove(liveState, room, initialMoves[moveIndex], actorUid);
    liveState = result.state;
    records.push({
      ...result.record,
      roomId,
    });
    if (liveState.winnerSeat >= 0) break;
  }

  while (allowBotAdvance === true && liveState.winnerSeat < 0 && autoBotMoves < 12) {
    const botSeat = safeSignedInt(liveState.currentPlayer);
    if (botSeat < 0 || botSeat > 1 || isSeatHuman(room, botSeat)) {
      break;
    }

    const botMove = chooseDuelBotMove(room, liveState, botSeat);
    const result = applyResolvedDuelMove(liveState, room, botMove, "server:bot");
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

function applyDuelActionBatchInTransaction(tx, roomRefDoc, room, state, roomId, firstMove = null, actorUid = "", options = {}) {
  const batchResult = advanceDuelBotsAndCollect(
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

function buildDuelRoomUpdateFromGameState(room, nextState, records = []) {
  const lastRecord = records.length > 0 ? records[records.length - 1] : null;
  const playedCountDelta = records.reduce((count, item) => count + (item.type === "play" || item.autoPlayedTileId !== null ? 1 : 0), 0);
  const nextActionSeq = safeInt(nextState.appliedActionSeq + 1);
  const nextTurnStartedAtMs = nextState.winnerSeat >= 0 ? 0 : Date.now();
  const nextTurnRoom = {
    ...room,
    turnStartedAtMs: nextTurnStartedAtMs,
  };
  const nextTurnLockUntilMs = nextState.winnerSeat >= 0 ? 0 : resolveDuelBotTurnLockUntilMs(nextTurnRoom, nextState, nextTurnStartedAtMs);
  const update = {
    nextActionSeq,
    lastActionSeq: nextState.appliedActionSeq,
    currentPlayer: nextState.currentPlayer,
    openingSeat: nextState.openingSeat,
    openingTileId: nextState.openingTileId,
    openingReason: nextState.openingReason,
    turnActual: nextActionSeq,
    turnStartedAt: nextState.winnerSeat >= 0 ? admin.firestore.FieldValue.delete() : admin.firestore.FieldValue.serverTimestamp(),
    turnStartedAtMs: nextTurnStartedAtMs,
    turnDeadlineMs: nextState.winnerSeat >= 0 ? 0 : (nextTurnStartedAtMs + DUEL_TURN_LIMIT_MS),
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
      drawnTileIds: Array.isArray(lastRecord.drawnTileIds) ? lastRecord.drawnTileIds.slice(0, 14) : [],
      autoPlayedTileId: lastRecord.autoPlayedTileId ?? null,
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

function buildDuelGameStateWrite(nextState) {
  return {
    deckOrder: nextState.deckOrder,
    seatHands: serializeDuelSeatHands(nextState.seatHands),
    stockPile: Array.isArray(nextState.stockPile) ? nextState.stockPile.slice(0, 14) : [],
    leftEnd: nextState.leftEnd,
    rightEnd: nextState.rightEnd,
    passesInRow: nextState.passesInRow,
    appliedActionSeq: nextState.appliedActionSeq,
    currentPlayer: nextState.currentPlayer,
    openingSeat: nextState.openingSeat,
    openingTileId: nextState.openingTileId,
    openingReason: nextState.openingReason,
    winnerSeat: nextState.winnerSeat,
    winnerUid: nextState.winnerUid,
    endedReason: nextState.endedReason,
    idempotencyKeys: trimIdempotencyKeys(nextState.idempotencyKeys),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };
}

function writeDuelRoomResultIfEndedTx(tx, roomRefDoc, room = {}, roomUpdate = {}) {
  const nextStatus = String(roomUpdate.status || room.status || "").trim().toLowerCase();
  if (nextStatus !== "ended") return;
  const snapshot = buildRoomResultSnapshot(roomRefDoc.id, room, roomUpdate);
  tx.set(duelRoomResultRef(roomRefDoc.id), {
    ...snapshot,
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
  }, { merge: true });
}

function cleanupDuelRoom(roomRefDoc) {
  return Promise.all([
    deleteCollectionInChunks(roomRefDoc.collection("actions")),
    deleteCollectionInChunks(roomRefDoc.collection("settlements")),
    duelGameStateRef(roomRefDoc.id).delete().catch(() => null),
  ]).then(() => roomRefDoc.delete());
}

function buildStartedDuelRoomTransaction(tx, roomRefDoc, room = {}, options = {}) {
  const configuredBotDifficulty = String(options.configuredBotDifficulty || room.botDifficulty || DEFAULT_BOT_DIFFICULTY);
  const nowMs = safeSignedInt(options.nowMs) || Date.now();
  const humans = Array.isArray(room.playerUids) ? room.playerUids.filter(Boolean).length : safeInt(room.humanCount);
  const deckOrder = Array.isArray(room.deckOrder) && room.deckOrder.length === 28 ? room.deckOrder.slice(0, 28) : makeDeckOrder();
  const roomAtStart = {
    ...room,
    botDifficulty: configuredBotDifficulty,
    deckOrder,
    humanCount: humans,
    botCount: Math.max(0, 2 - humans),
    playedCount: 0,
  };
  const initialState = createInitialDuelGameState(roomAtStart, deckOrder);
  const openingMove = buildOpeningMoveForDuelState(initialState);
  const batchResult = applyDuelActionBatchInTransaction(
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

  tx.set(duelGameStateRef(roomRefDoc.id), buildDuelGameStateWrite(finalState), { merge: true });

  const updates = {
    playerUids: Array.isArray(room.playerUids) ? room.playerUids : ["", ""],
    playerNames: Array.isArray(room.playerNames) ? room.playerNames : ["", ""],
    seats: getRoomSeats(room),
    humanCount: humans,
    status: finalState.winnerSeat >= 0 ? "ended" : "playing",
    startRevealPending: finalState.winnerSeat < 0,
    startRevealAckUids: [],
    startedHumanCount: humans,
    startedBotCount: Math.max(0, 2 - humans),
    botCount: Math.max(0, 2 - humans),
    botDifficulty: configuredBotDifficulty,
    startedAt: admin.firestore.FieldValue.serverTimestamp(),
    startedAtMs: nowMs,
    deckOrder: admin.firestore.FieldValue.delete(),
    turnLockedUntilMs: 0,
    endClicks: {},
    waitingDeadlineMs: admin.firestore.FieldValue.delete(),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };

  Object.assign(updates, buildDuelRoomUpdateFromGameState(roomAtStart, finalState, batchResult.records));
  if (finalState.winnerSeat < 0) {
    updates.winnerSeat = admin.firestore.FieldValue.delete();
    updates.winnerUid = admin.firestore.FieldValue.delete();
    updates.endedReason = admin.firestore.FieldValue.delete();
    updates.endedAt = admin.firestore.FieldValue.delete();
    updates.endedAtMs = admin.firestore.FieldValue.delete();
    updates.turnLockedUntilMs = 0;
  }

  tx.update(roomRefDoc, updates);
  writeDuelRoomResultIfEndedTx(tx, roomRefDoc, roomAtStart, updates);

  return {
    ok: true,
    started: true,
    status: String(updates.status || "playing"),
    startRevealPending: updates.startRevealPending === true,
    privateDeckOrder: String(updates.status || "playing") === "playing" ? finalState.deckOrder.slice(0, 28) : [],
    humanCount: humans,
    botCount: Math.max(0, 2 - humans),
    waitingDeadlineMs: 0,
  };
}

async function processPendingBotTurnsDuel(roomId) {
  const safeRoomId = String(roomId || "").trim();
  if (!safeRoomId) return;

  const roomRefDoc = duelRoomRef(safeRoomId);
  const stateRef = duelGameStateRef(safeRoomId);

  while (true) {
    const roomSnap = await roomRefDoc.get();
    if (!roomSnap.exists) return;
    const room = roomSnap.data() || {};

    if (String(room.status || "") !== "playing") return;
    if (room.startRevealPending === true) return;
    const roomWinnerSeat = Number.isFinite(Number(room.winnerSeat))
      ? Math.trunc(Number(room.winnerSeat))
      : -1;
    if (roomWinnerSeat >= 0) return;

    const outcome = await db.runTransaction(async (tx) => {
      const [liveRoomSnap, stateSnap] = await Promise.all([
        tx.get(roomRefDoc),
        tx.get(stateRef),
      ]);

      if (!liveRoomSnap.exists) {
        return { processed: false, stop: true };
      }

      const liveRoom = liveRoomSnap.data() || {};
      if (String(liveRoom.status || "") !== "playing" || liveRoom.startRevealPending === true) {
        return { processed: false, stop: true };
      }

      const currentState = stateSnap.exists
        ? normalizeDuelGameState(stateSnap.data(), liveRoom)
        : createInitialDuelGameState(
            liveRoom,
            Array.isArray(liveRoom.deckOrder) && liveRoom.deckOrder.length === 28 ? liveRoom.deckOrder : makeDeckOrder()
          );

      if (currentState.winnerSeat >= 0) {
        return { processed: false, stop: true };
      }

      const safeNowMs = Date.now();
      const activeSeat = safeSignedInt(liveRoom.currentPlayer, -1);
      const activeSeatIsHuman = activeSeat >= 0 && activeSeat <= 1 && isSeatHuman(liveRoom, activeSeat);
      const turnDeadlineMs = resolveDuelTurnDeadlineMs(liveRoom, safeNowMs);

      if (activeSeatIsHuman) {
        if (turnDeadlineMs > safeNowMs) {
          return { processed: false, stop: true };
        }

        const timeoutMoves = buildExpiredHumanDuelMoves(liveRoom, currentState);
        const batchResult = applyDuelActionBatchInTransaction(
          tx,
          roomRefDoc,
          liveRoom,
          currentState,
          safeRoomId,
          timeoutMoves,
          "server:timeout",
          { allowBotAdvance: false }
        );
        const nextState = batchResult.state;
        tx.set(stateRef, buildDuelGameStateWrite(nextState), { merge: true });
        const roomUpdate = buildDuelRoomUpdateFromGameState(liveRoom, nextState, batchResult.records);
        tx.update(roomRefDoc, roomUpdate);
        writeDuelRoomResultIfEndedTx(tx, roomRefDoc, liveRoom, roomUpdate);

        return {
          processed: true,
          stop: nextState.winnerSeat >= 0 || isSeatHuman(liveRoom, nextState.currentPlayer),
        };
      }

      const botSeat = activeSeat;
      if (botSeat < 0 || botSeat > 1) {
        return { processed: false, stop: true };
      }

      const lockedUntilMs = safeSignedInt(liveRoom.turnLockedUntilMs);
      if (lockedUntilMs > safeNowMs) {
        return { processed: false, stop: true };
      }

      if (lockedUntilMs <= 0) {
        const scheduledUntilMs = resolveDuelBotTurnLockUntilMs(liveRoom, currentState, safeNowMs);
        if (scheduledUntilMs > safeNowMs) {
          tx.update(roomRefDoc, {
            turnLockedUntilMs: scheduledUntilMs,
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          });
          return { processed: false, stop: true };
        }
      }

      const botMove = chooseDuelBotMove(liveRoom, currentState, safeSignedInt(liveRoom.currentPlayer));
      const batchResult = applyDuelActionBatchInTransaction(
        tx,
        roomRefDoc,
        liveRoom,
        currentState,
        safeRoomId,
        botMove,
        "server:bot",
        { allowBotAdvance: false }
      );
      const nextState = batchResult.state;
      tx.set(stateRef, buildDuelGameStateWrite(nextState), { merge: true });
      const roomUpdate = buildDuelRoomUpdateFromGameState(liveRoom, nextState, batchResult.records);
      tx.update(roomRefDoc, roomUpdate);
      writeDuelRoomResultIfEndedTx(tx, roomRefDoc, liveRoom, roomUpdate);

      return {
        processed: true,
        stop: nextState.winnerSeat >= 0 || isSeatHuman(liveRoom, nextState.currentPlayer),
      };
    });

    if (!outcome || !outcome.processed || outcome.stop) {
      return;
    }
  }
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
  const rewardWelcomeDoes = safeInt(options.welcomeRewardDoes);

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
  let beforePendingFromWelcome = safeInt(data.pendingPlayFromWelcomeDoes);
  let beforeTotalExchangedEver = safeInt(data.totalExchangedHtgEver);
  const beforeProvisionalDoes = safeInt(data.doesProvisionalBalance);
  const beforeApprovedDoes = safeInt(
    typeof data.doesApprovedBalance === "number"
      ? data.doesApprovedBalance
      : Math.max(0, beforeDoes - beforeProvisionalDoes)
  );
  const beforeWelcomeBonusHtgAvailable = safeInt(data.welcomeBonusHtgAvailable);
  const beforeWelcomeBonusHtgConverted = safeInt(data.welcomeBonusHtgConverted);
  let beforeWelcomeBonusHtgPlayed = safeInt(data.welcomeBonusHtgPlayed);
  const beforePendingPlayTotal = beforePendingFromXchange + beforePendingFromReferral + beforePendingFromWelcome;
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
  let afterPendingFromWelcome = beforePendingFromWelcome;
  let afterTotalExchangedEver = beforeTotalExchangedEver;
  let afterWelcomeBonusHtgAvailable = beforeWelcomeBonusHtgAvailable;
  let afterWelcomeBonusHtgConverted = beforeWelcomeBonusHtgConverted;
  let afterWelcomeBonusHtgPlayed = beforeWelcomeBonusHtgPlayed;
  let fundingPatch = {};
  let gameEntryFunding = {
    approvedDoes: 0,
    provisionalDoes: 0,
    welcomeDoes: 0,
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

  if (beforeWelcomeBonusHtgConverted > 0) {
    const [allOrders, exchangeHistory] = await Promise.all([
      loadAllOrders(),
      loadExchangeHistory(),
    ]);
    const hasRealApprovedDeposit = hasRealApprovedDepositFromOrders(allOrders.map((item) => item.data || {}));
    const onlyWelcomeLockedContext = !hasRealApprovedDeposit
      && beforePendingFromXchange <= 0
      && beforePendingFromReferral <= 0;
    if (onlyWelcomeLockedContext) {
      beforePendingFromWelcome = Math.max(0, beforeApprovedDoes);
      const inferredWelcomePlayedDoes = exchangeHistory.reduce((sum, item) => {
        if (String(item?.type || "").trim() !== "game_entry") return sum;
        const explicitWelcomeDoes = safeInt(item?.gameEntryFunding?.welcomeDoes);
        if (explicitWelcomeDoes > 0) return sum + explicitWelcomeDoes;
        const inferredAmountDoes = Math.min(
          safeInt(item?.amountDoes),
          safeInt(item?.beforePendingPlayFromWelcomeDoes)
        );
        return sum + Math.max(0, inferredAmountDoes);
      }, 0);
      beforeWelcomeBonusHtgPlayed = Math.max(
        beforeWelcomeBonusHtgPlayed,
        Math.floor(inferredWelcomePlayedDoes / RATE_HTG_TO_DOES)
      );
    }
  }
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
        welcomeBonusHtgAvailable: beforeWelcomeBonusHtgAvailable,
        welcomeBonusHtgConverted: beforeWelcomeBonusHtgConverted,
        welcomeBonusHtgPlayed: beforeWelcomeBonusHtgPlayed,
        pendingPlayFromWelcomeDoes: beforePendingFromWelcome,
      },
    });
    provisionalConversion = await consumeProvisionalHtgForConversion(amountGourdes);
    const remainingAfterProvisionalAmount = Math.max(0, amountGourdes - provisionalConversion.consumedGourdes);
    const consumeWelcomeAmount = Math.min(remainingAfterProvisionalAmount, beforeWelcomeBonusHtgAvailable);
    const remainingApprovedAmount = Math.max(0, remainingAfterProvisionalAmount - consumeWelcomeAmount);
    const availableApprovedToConvertHtg = safeInt(fundingSnapshot.approvedHtgAvailable);

    console.log("[BALANCE_DEBUG][FUNCTIONS][xchange_buy] snapshot", JSON.stringify({
      uid,
      amountGourdes,
      beforeExchanged,
      beforeTotalExchangedEver,
      beforeWelcomeBonusHtgAvailable,
      consumeWelcomeAmount,
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
    if (consumeWelcomeAmount > 0) {
      const welcomeDoesDelta = consumeWelcomeAmount * RATE_HTG_TO_DOES;
      afterApprovedDoes += welcomeDoesDelta;
      afterPendingFromWelcome = beforePendingFromWelcome + welcomeDoesDelta;
      afterWelcomeBonusHtgAvailable = Math.max(0, beforeWelcomeBonusHtgAvailable - consumeWelcomeAmount);
      afterWelcomeBonusHtgConverted = beforeWelcomeBonusHtgConverted + consumeWelcomeAmount;
    }
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
        welcomeBonusHtgAvailable: afterWelcomeBonusHtgAvailable,
        welcomeBonusHtgConverted: afterWelcomeBonusHtgConverted,
        welcomeBonusHtgPlayed: afterWelcomeBonusHtgPlayed,
        pendingPlayFromWelcomeDoes: afterPendingFromWelcome,
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
    let consumedXchangeDoes = 0;
    let consumedReferralDoes = 0;
    let consumedWelcomeDoes = 0;
    if (playedApprovedDoes > 0 && afterPendingFromXchange > 0) {
      const consumeXchange = Math.min(playedApprovedDoes, afterPendingFromXchange);
      afterPendingFromXchange -= consumeXchange;
      playedApprovedDoes -= consumeXchange;
      consumedXchangeDoes += consumeXchange;
      afterExchangeableDoes += consumeXchange;
    }
    if (playedApprovedDoes > 0 && afterPendingFromReferral > 0) {
      const consumeReferral = Math.min(playedApprovedDoes, afterPendingFromReferral);
      afterPendingFromReferral -= consumeReferral;
      playedApprovedDoes -= consumeReferral;
      consumedReferralDoes += consumeReferral;
      afterExchangeableDoes += consumeReferral;
    }
    if (playedApprovedDoes > 0 && afterPendingFromWelcome > 0) {
      const consumeWelcome = Math.min(playedApprovedDoes, afterPendingFromWelcome);
      afterPendingFromWelcome -= consumeWelcome;
      playedApprovedDoes -= consumeWelcome;
      consumedWelcomeDoes += consumeWelcome;
      afterWelcomeBonusHtgPlayed += Math.floor(consumeWelcome / RATE_HTG_TO_DOES);
    }

    gameEntryFunding = {
      approvedDoes: approvedSpentDoes,
      provisionalDoes: provisionalSpentDoes,
      welcomeDoes: consumedWelcomeDoes,
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
    const welcomeReward = safeInt(rewardWelcomeDoes);
    if ((approvedReward + provisionalReward) !== amountDoes) {
      throw new HttpsError("failed-precondition", "Répartition de gain invalide.");
    }
    if (welcomeReward > approvedReward) {
      throw new HttpsError("failed-precondition", "Répartition bonus bienvenue invalide.");
    }
    afterApprovedDoes += approvedReward;
    afterProvisionalDoes += provisionalReward;
    afterPendingFromWelcome += welcomeReward;
  }

  if (type === "xchange_sell") {
    const pendingTotal = afterPendingFromXchange + afterPendingFromReferral + afterPendingFromWelcome;
    const availableExchangeableDoes = afterPendingFromWelcome > 0
      ? 0
      : Math.min(beforeApprovedDoes, beforeExchangeableDoes);
    if (amountDoes > availableExchangeableDoes) {
      throw buildPlayRequiredError({
        pendingPlayFromXchangeDoes: afterPendingFromXchange,
        pendingPlayFromReferralDoes: afterPendingFromReferral,
        pendingPlayFromWelcomeDoes: afterPendingFromWelcome,
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
        welcomeBonusHtgAvailable: afterWelcomeBonusHtgAvailable,
        welcomeBonusHtgConverted: afterWelcomeBonusHtgConverted,
        welcomeBonusHtgPlayed: afterWelcomeBonusHtgPlayed,
        pendingPlayFromWelcomeDoes: afterPendingFromWelcome,
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
  if (afterPendingFromWelcome > 0) {
    afterExchangeableDoes = 0;
  } else if ((afterPendingFromXchange + afterPendingFromReferral + afterPendingFromWelcome) <= 0) {
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
    pendingPlayFromWelcomeDoes: afterPendingFromWelcome,
    welcomeBonusHtgAvailable: safeInt(afterWelcomeBonusHtgAvailable),
    welcomeBonusHtgConverted: safeInt(afterWelcomeBonusHtgConverted),
    welcomeBonusHtgPlayed: safeInt(afterWelcomeBonusHtgPlayed),
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
    beforePendingPlayFromWelcomeDoes: beforePendingFromWelcome,
    afterPendingPlayFromWelcomeDoes: afterPendingFromWelcome,
    beforeExchangeableDoesAvailable: beforeExchangeableDoes,
    afterExchangeableDoesAvailable: safeInt(afterExchangeableDoes),
    beforeApprovedDoesBalance: beforeApprovedDoes,
    afterApprovedDoesBalance: safeInt(afterApprovedDoes),
    beforeProvisionalDoesBalance: beforeProvisionalDoes,
    afterProvisionalDoesBalance: safeInt(afterProvisionalDoes),
    beforeWelcomeBonusHtgAvailable,
    afterWelcomeBonusHtgAvailable: safeInt(afterWelcomeBonusHtgAvailable),
    beforeWelcomeBonusHtgConverted,
    afterWelcomeBonusHtgConverted: safeInt(afterWelcomeBonusHtgConverted),
    beforeWelcomeBonusHtgPlayed,
    afterWelcomeBonusHtgPlayed: safeInt(afterWelcomeBonusHtgPlayed),
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
    afterPendingFromWelcome,
    afterTotalExchangedEver,
    afterExchangeableDoes: safeInt(afterExchangeableDoes),
    afterApprovedDoes: safeInt(afterApprovedDoes),
    afterProvisionalDoes: safeInt(afterProvisionalDoes),
    afterWelcomeBonusHtgAvailable: safeInt(afterWelcomeBonusHtgAvailable),
    afterWelcomeBonusHtgConverted: safeInt(afterWelcomeBonusHtgConverted),
    afterWelcomeBonusHtgPlayed: safeInt(afterWelcomeBonusHtgPlayed),
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

function isFriendRoom(room = {}) {
  return String(room?.roomMode || "").trim() === "friends";
}

function isFriendDuelRoom(room = {}) {
  return String(room?.roomMode || "").trim() === "duel_friends";
}

function isFriendMorpionRoom(room = {}) {
  return String(room?.roomMode || "").trim() === "morpion_friends";
}

function getRoomTargetHumanCount(room = {}) {
  const requested = safeInt(room?.requiredHumans);
  if (requested >= 2 && requested <= 4) return requested;
  return 4;
}

function resolveFriendRoomDeadlineMs(room = {}, nowMs = Date.now()) {
  const explicit = safeSignedInt(room.waitingDeadlineMs);
  if (explicit > 0) return explicit;
  const createdAtMs = resolveRoomCreatedAtMs(room);
  if (createdAtMs > 0) return createdAtMs + FRIEND_ROOM_WAIT_MS;
  return nowMs + FRIEND_ROOM_WAIT_MS;
}

function resolveWaitingDeadlineMs(room = {}, nowMs = Date.now()) {
  if (isFriendRoom(room)) {
    return resolveFriendRoomDeadlineMs(room, nowMs);
  }
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
  const requiredHumans = isFriendRoom(room)
    ? getRoomTargetHumanCount(room)
    : 4;
  if (humans >= requiredHumans) return true;
  if (isFriendRoom(room)) return false;
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
    if (isFriendRoom(data)) return;
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

async function generateUniqueFriendRoomInviteCode(size = FRIEND_ROOM_CODE_SIZE, maxAttempts = 18) {
  const targetSize = Math.max(4, safeInt(size) || FRIEND_ROOM_CODE_SIZE);
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    const candidate = normalizeCode(randomCode(targetSize));
    if (!candidate) continue;
    const existing = await db
      .collection(ROOMS_COLLECTION)
      .where("inviteCodeNormalized", "==", candidate)
      .limit(1)
      .get();
    if (existing.empty) return candidate;
  }
  throw new HttpsError("aborted", "Impossible de générer un code de salle unique.");
}

async function generateUniqueFriendDuelInviteCode(size = FRIEND_ROOM_CODE_SIZE, maxAttempts = 18) {
  const targetSize = Math.max(4, safeInt(size) || FRIEND_ROOM_CODE_SIZE);
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    const candidate = normalizeCode(randomCode(targetSize));
    if (!candidate) continue;
    const existing = await db
      .collection(DUEL_ROOMS_COLLECTION)
      .where("inviteCodeNormalized", "==", candidate)
      .limit(1)
      .get();
    if (existing.empty) return candidate;
  }
  throw new HttpsError("aborted", "Impossible de générer un code de duel unique.");
}

async function generateUniqueFriendMorpionInviteCode(size = FRIEND_ROOM_CODE_SIZE, maxAttempts = 18) {
  const targetSize = Math.max(4, safeInt(size) || FRIEND_ROOM_CODE_SIZE);
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    const candidate = normalizeCode(randomCode(targetSize));
    if (!candidate) continue;
    const existing = await db
      .collection(MORPION_ROOMS_COLLECTION)
      .where("inviteCodeNormalized", "==", candidate)
      .limit(1)
      .get();
    if (existing.empty) return candidate;
  }
  throw new HttpsError("aborted", "Impossible de générer un code morpion unique.");
}

async function chargeRoomEntriesTx(tx, room = {}, playerUids = [], stakeDoes = 0) {
  const normalizedStakeDoes = safeInt(stakeDoes);
  if (normalizedStakeDoes <= 0) {
    throw new HttpsError("invalid-argument", "Mise invalide.");
  }

  const uniquePlayerUids = Array.from(
    new Set(
      (Array.isArray(playerUids) ? playerUids : [])
        .map((item) => String(item || "").trim())
        .filter(Boolean)
    )
  );

  const entryFundingByUid = {};
  const afterDoesByUid = {};

  for (const playerUid of uniquePlayerUids) {
    const walletMutation = await applyWalletMutationTx(tx, {
      uid: playerUid,
      email: "",
      type: "game_entry",
      note: isFriendRoom(room) ? "Participation partie privee" : "Participation partie",
      amountDoes: normalizedStakeDoes,
      amountGourdes: 0,
      deltaDoes: -normalizedStakeDoes,
      deltaExchangedGourdes: 0,
    });

    entryFundingByUid[playerUid] = {
      approvedDoes: safeInt(walletMutation.gameEntryFunding?.approvedDoes),
      provisionalDoes: safeInt(walletMutation.gameEntryFunding?.provisionalDoes),
      welcomeDoes: safeInt(walletMutation.gameEntryFunding?.welcomeDoes),
      provisionalSources: normalizeFundingSources(walletMutation.gameEntryFunding?.provisionalSources),
    };
    afterDoesByUid[playerUid] = safeInt(walletMutation.afterDoes);
  }

  return {
    entryFundingByUid,
    afterDoesByUid,
  };
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
  writeRoomResultIfEndedTx(tx, roomRefDoc, roomAtStart, updates);

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
    afterPendingFromWelcome: result.afterPendingFromWelcome,
    afterTotalExchangedEver: result.afterTotalExchangedEver,
    afterExchangeableDoes: result.afterExchangeableDoes,
    afterWelcomeBonusHtgAvailable: result.afterWelcomeBonusHtgAvailable,
    afterWelcomeBonusHtgConverted: result.afterWelcomeBonusHtgConverted,
    afterWelcomeBonusHtgPlayed: result.afterWelcomeBonusHtgPlayed,
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
    pendingPlayFromWelcomeDoes: result.afterPendingFromWelcome,
    welcomeBonusHtgAvailable: result.afterWelcomeBonusHtgAvailable,
    welcomeBonusHtgConverted: result.afterWelcomeBonusHtgConverted,
    welcomeBonusHtgPlayed: result.afterWelcomeBonusHtgPlayed,
    totalExchangedHtgEver: result.afterTotalExchangedEver,
    gameEntryFunding: result.gameEntryFunding,
    provisionalConversion: result.provisionalConversion,
  };
});

exports.createFriendRoom = publicOnCall("createFriendRoom", async (request) => {
  const { uid, email } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const stakeDoes = safeInt(payload.stakeDoes);
  const requiredHumans = 4;
  const [settingsSnapshot, inviteCode] = await Promise.all([
    getSettingsSnapshotData(),
    generateUniqueFriendRoomInviteCode(),
  ]);
  const selectedStakeConfig = findStakeConfigByAmount(stakeDoes, settingsSnapshot.gameStakeOptions, true);

  if (!selectedStakeConfig) {
    throw new HttpsError("invalid-argument", "Mise non autorisée.");
  }

  const activeMembership = await db
    .collection(ROOMS_COLLECTION)
    .where("playerUids", "array-contains", uid)
    .limit(12)
    .get();
  const activeDoc = activeMembership.docs.find((docSnap) => {
    const data = docSnap.data() || {};
    const status = String(data.status || "");
    return status === "waiting" || status === "playing";
  });
  if (activeDoc) {
    const activeData = activeDoc.data() || {};
    const activeSeat = getSeatForUser(activeData, uid);
    throw new HttpsError("failed-precondition", "Tu participes déjà à une salle active.", {
      code: "active-room-exists",
      roomId: activeDoc.id,
      status: String(activeData.status || ""),
      seatIndex: activeSeat,
      roomMode: String(activeData.roomMode || "public"),
    });
  }

  const rewardAmountDoes = selectedStakeConfig.rewardDoes;
  const roomRef = db.collection(ROOMS_COLLECTION).doc();

  return db.runTransaction(async (tx) => {
    const walletSnap = await tx.get(walletRef(uid));
    const walletData = walletSnap.exists ? (walletSnap.data() || {}) : {};
    assertWalletNotFrozen(walletData);

    if (safeInt(walletData.doesBalance) < stakeDoes) {
      throw new HttpsError("failed-precondition", "Solde Does insuffisant.");
    }

    const nowMs = Date.now();
    const waitingDeadlineMs = nowMs + FRIEND_ROOM_WAIT_MS;

    tx.set(roomRef, {
      status: "waiting",
      roomMode: "friends",
      isPrivate: true,
      allowBots: false,
      inviteCode,
      inviteCodeNormalized: normalizeCode(inviteCode),
      requiredHumans,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      createdAtMs: nowMs,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      ownerUid: uid,
      playerUids: [uid, "", "", ""],
      playerNames: [sanitizePlayerLabel(email || uid, 0), "", "", ""],
      blockedRejoinUids: [],
      humanCount: 1,
      seats: { [uid]: 0 },
      roomPresenceMs: { [uid]: nowMs },
      botCount: 0,
      startRevealPending: false,
      startRevealAckUids: [],
      waitingDeadlineMs,
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

    return {
      ok: true,
      roomId: roomRef.id,
      seatIndex: 0,
      status: "waiting",
      charged: false,
      inviteCode,
      requiredHumans,
      waitingDeadlineMs,
      privateDeckOrder: [],
    };
  });
});

exports.joinFriendRoomByCode = publicOnCall("joinFriendRoomByCode", async (request) => {
  const { uid, email } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const inviteCodeNormalized = normalizeCode(payload.inviteCode || payload.code || "");
  const configuredBotDifficulty = await getConfiguredBotDifficulty();

  if (!inviteCodeNormalized) {
    throw new HttpsError("invalid-argument", "Code de salle requis.");
  }

  const activeMembership = await db
    .collection(ROOMS_COLLECTION)
    .where("playerUids", "array-contains", uid)
    .limit(12)
    .get();
  const activeDoc = activeMembership.docs.find((docSnap) => {
    const data = docSnap.data() || {};
    const status = String(data.status || "");
    return status === "waiting" || status === "playing";
  });
  if (activeDoc) {
    const activeData = activeDoc.data() || {};
    const activeSeat = getSeatForUser(activeData, uid);
    throw new HttpsError("failed-precondition", "Tu participes déjà à une salle active.", {
      code: "active-room-exists",
      roomId: activeDoc.id,
      status: String(activeData.status || ""),
      seatIndex: activeSeat,
      roomMode: String(activeData.roomMode || "public"),
    });
  }

  const matchingSnap = await db
    .collection(ROOMS_COLLECTION)
    .where("inviteCodeNormalized", "==", inviteCodeNormalized)
    .limit(6)
    .get();

  const roomDoc = matchingSnap.docs.find((docSnap) => isFriendRoom(docSnap.data() || {})) || null;
  if (!roomDoc) {
    throw new HttpsError("not-found", "Code de salle introuvable.");
  }

  const roomRef = roomDoc.ref;

  const result = await db.runTransaction(async (tx) => {
    const [roomSnap, walletSnap] = await Promise.all([
      tx.get(roomRef),
      tx.get(walletRef(uid)),
    ]);

    if (!roomSnap.exists) {
      throw new HttpsError("not-found", "Salle introuvable.");
    }

    const room = roomSnap.data() || {};
    if (!isFriendRoom(room)) {
      throw new HttpsError("failed-precondition", "Cette salle n'est pas disponible.");
    }

    const roomStatus = String(room.status || "");
    const nowMs = Date.now();
    const waitingDeadlineMs = resolveFriendRoomDeadlineMs(room, nowMs);
    const requiredHumans = getRoomTargetHumanCount(room);
    const playerUids = Array.from({ length: 4 }, (_, idx) => String((room.playerUids || [])[idx] || ""));
    const humans = playerUids.filter(Boolean).length;
    const roomStakeDoes = safeInt(room.entryCostDoes || room.stakeDoes);
    const roomRewardAmountDoes = resolveRoomRewardDoes(room);
    const roomInviteCode = String(room.inviteCode || inviteCodeNormalized || "").trim();

    if (roomStatus === "playing") {
      throw new HttpsError("failed-precondition", "Cette salle a déjà démarré.");
    }
    if (roomStatus !== "waiting") {
      throw new HttpsError("failed-precondition", "Cette salle n'est plus disponible.");
    }
    if (getBlockedRejoinSet(room).has(uid)) {
      throw new HttpsError("permission-denied", "Tu ne peux plus rejoindre cette salle.");
    }
    if (nowMs >= waitingDeadlineMs && humans < requiredHumans) {
      tx.set(roomRef, {
        status: "closed",
        endedReason: "expired",
        endedAt: admin.firestore.FieldValue.serverTimestamp(),
        endedAtMs: nowMs,
        waitingDeadlineMs: admin.firestore.FieldValue.delete(),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      }, { merge: true });
      return {
        ok: false,
        expired: true,
        roomId: roomRef.id,
      };
    }

    const walletData = walletSnap.exists ? (walletSnap.data() || {}) : {};
    assertWalletNotFrozen(walletData);

    if (safeInt(walletData.doesBalance) < roomStakeDoes) {
      throw new HttpsError("failed-precondition", "Solde Does insuffisant.");
    }

    if (playerUids.includes(uid)) {
      const seat = getSeatForUser(room, uid);
      const nextPresence = room.roomPresenceMs && typeof room.roomPresenceMs === "object"
        ? { ...room.roomPresenceMs }
        : {};
      nextPresence[uid] = nowMs;
      tx.update(roomRef, {
        roomPresenceMs: nextPresence,
        waitingDeadlineMs,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      });
      return {
        ok: true,
        resumed: true,
        charged: false,
        roomId: roomRef.id,
        seatIndex: seat >= 0 ? seat : 0,
        status: "waiting",
        stakeDoes: roomStakeDoes,
        rewardAmountDoes: roomRewardAmountDoes,
        inviteCode: roomInviteCode,
        requiredHumans,
        waitingDeadlineMs,
        privateDeckOrder: [],
      };
    }

    if (humans >= requiredHumans) {
      throw new HttpsError("failed-precondition", "Cette salle est complète.");
    }

    const currentSeats = room.seats && typeof room.seats === "object" ? room.seats : {};
    const usedSeats = new Set(
      Object.values(currentSeats)
        .map((seat) => Number(seat))
        .filter((seat) => Number.isFinite(seat) && seat >= 0 && seat < 4)
    );
    const seatIndex = [0, 1, 2, 3].find((seat) => !usedSeats.has(seat));
    if (typeof seatIndex !== "number") {
      throw new HttpsError("failed-precondition", "Cette salle est complète.");
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
    const nextPresence = room.roomPresenceMs && typeof room.roomPresenceMs === "object"
      ? { ...room.roomPresenceMs }
      : {};
    nextPresence[uid] = nowMs;
    const nextHumans = nextPlayerUids.filter(Boolean).length;

    if (nextHumans >= requiredHumans) {
      const chargeResult = await chargeRoomEntriesTx(tx, room, nextPlayerUids, roomStakeDoes);
      tx.set(roomRef, {
        playerUids: nextPlayerUids,
        playerNames: nextPlayerNames,
        seats: nextSeats,
        roomPresenceMs: nextPresence,
        humanCount: nextHumans,
        botCount: 0,
        entryFundingByUid: chargeResult.entryFundingByUid,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      }, { merge: true });

      return {
        ok: true,
        resumed: false,
        charged: true,
        roomId: roomRef.id,
        seatIndex,
        does: safeInt(chargeResult.afterDoesByUid[uid]),
        inviteCode: roomInviteCode,
        requiredHumans,
        stakeDoes: roomStakeDoes,
        rewardAmountDoes: roomRewardAmountDoes,
        ...buildStartedRoomTransaction(tx, roomRef, {
          ...room,
          playerUids: nextPlayerUids,
          playerNames: nextPlayerNames,
          seats: nextSeats,
          humanCount: nextHumans,
          botCount: 0,
          roomPresenceMs: nextPresence,
          waitingDeadlineMs,
        }, {
          configuredBotDifficulty,
          nowMs,
        }),
      };
    }

    tx.update(roomRef, {
      playerUids: nextPlayerUids,
      playerNames: nextPlayerNames,
      playerEmails: admin.firestore.FieldValue.delete(),
      seats: nextSeats,
      roomPresenceMs: nextPresence,
      humanCount: nextHumans,
      botCount: 0,
      waitingDeadlineMs,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    });

    return {
      ok: true,
      resumed: false,
      charged: false,
      roomId: roomRef.id,
      seatIndex,
      status: "waiting",
      inviteCode: roomInviteCode,
      requiredHumans,
      stakeDoes: roomStakeDoes,
      rewardAmountDoes: roomRewardAmountDoes,
      waitingDeadlineMs,
      privateDeckOrder: [],
    };
  });

  if (result?.expired === true) {
    throw new HttpsError("failed-precondition", "Ce code a expiré.");
  }

  if (result?.status === "playing" && result?.startRevealPending !== true) {
    await processPendingBotTurns(String(result.roomId || ""));
  }

  return result;
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
      if (isFriendRoom(room)) return null;
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
      .filter((docSnap) => !isFriendRoom(docSnap.data() || {}))
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
        if (isFriendRoom(room)) {
          throw new HttpsError("aborted", "Salle non disponible.");
        }
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
          welcomeDoes: safeInt(walletMutation.gameEntryFunding?.welcomeDoes),
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
        if (isFriendRoom(room)) {
          throw new HttpsError("aborted", "Salle non disponible.");
        }
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
                  welcomeDoes: safeInt(walletMutation.gameEntryFunding?.welcomeDoes),
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
          welcomeDoes: safeInt(walletMutation.gameEntryFunding?.welcomeDoes),
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

    if (isFriendRoom(room)) {
      const requiredHumans = getRoomTargetHumanCount(room);
      const waitingDeadlineMs = resolveFriendRoomDeadlineMs(room, nowMs);
      if (safeSignedInt(room.waitingDeadlineMs) !== waitingDeadlineMs) {
        tx.update(roomRef, {
          waitingDeadlineMs,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        });
      }

      if (humans < requiredHumans) {
        if (nowMs >= waitingDeadlineMs) {
          tx.set(roomRef, {
            status: "closed",
            endedReason: "expired",
            endedAt: admin.firestore.FieldValue.serverTimestamp(),
            endedAtMs: nowMs,
            waitingDeadlineMs: admin.firestore.FieldValue.delete(),
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          }, { merge: true });
          return {
            ok: true,
            started: false,
            expired: true,
            status: "closed",
            startRevealPending: false,
            waitingDeadlineMs: 0,
            humanCount: humans,
            botCount: 0,
            requiredHumans,
            privateDeckOrder: [],
          };
        }

        return {
          ok: true,
          started: false,
          status: "waiting",
          startRevealPending: false,
          waitingDeadlineMs,
          humanCount: humans,
          botCount: 0,
          requiredHumans,
          privateDeckOrder: [],
        };
      }

      const playerUids = Array.isArray(room.playerUids) ? room.playerUids.slice(0, 4) : ["", "", "", ""];
      let entryFundingByUid = room.entryFundingByUid && typeof room.entryFundingByUid === "object"
        ? { ...room.entryFundingByUid }
        : null;
      let afterDoesForCaller = 0;

      if (!entryFundingByUid || Object.keys(entryFundingByUid).length < humans) {
        const chargeResult = await chargeRoomEntriesTx(tx, room, playerUids, safeInt(room.entryCostDoes || room.stakeDoes));
        entryFundingByUid = chargeResult.entryFundingByUid;
        afterDoesForCaller = safeInt(chargeResult.afterDoesByUid[uid]);
        tx.set(roomRef, {
          entryFundingByUid,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        }, { merge: true });
      }

      return {
        does: afterDoesForCaller,
        charged: true,
        requiredHumans,
        ...buildStartedRoomTransaction(tx, roomRef, {
          ...room,
          entryFundingByUid,
          humanCount: humans,
          botCount: 0,
          waitingDeadlineMs,
        }, {
          configuredBotDifficulty,
          nowMs,
        }),
      };
    }

    const poolRef = matchmakingPoolRef(String(room.stakeConfigId || ""), safeInt(room.entryCostDoes || room.stakeDoes));
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
    const roomUpdate = {
      status: "ended",
      endedAt: admin.firestore.FieldValue.serverTimestamp(),
      endedAtMs: Date.now(),
      winnerSeat,
      winnerUid,
      endedReason: endedReason || "out",
      endClicks: {},
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    };
    tx.update(roomRef, roomUpdate);
    tx.set(gameStateRef(roomId), {
      winnerSeat,
      winnerUid,
      endedReason: endedReason || "out",
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
    writeRoomResultIfEndedTx(tx, roomRef, room, roomUpdate);

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
    writeRoomResultIfEndedTx(tx, roomRef, room, roomUpdate);

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
    const welcomeEntryDoes = safeInt(entryFundingRaw?.welcomeDoes);
    let approvedRewardDoes = rewardAmountDoes;
    let provisionalRewardDoes = 0;
    let welcomeRewardDoes = 0;

    if ((provisionalEntryDoes > 0 && provisionalSources.length > 0) || welcomeEntryDoes > 0) {
      const totalEntryDoes = Math.max(approvedEntryDoes + provisionalEntryDoes + welcomeEntryDoes, provisionalEntryDoes + welcomeEntryDoes);
      const provisionalRewardPool = Math.min(
        rewardAmountDoes,
        Math.round((rewardAmountDoes * provisionalEntryDoes) / Math.max(1, totalEntryDoes))
      );
      welcomeRewardDoes = Math.min(
        rewardAmountDoes - provisionalRewardPool,
        Math.round((rewardAmountDoes * welcomeEntryDoes) / Math.max(1, totalEntryDoes))
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

      provisionalRewardDoes = Math.max(0, provisionalRewardPool - promotedApprovedRewardDoes);
    } else if (approvedEntryDoes <= 0 && provisionalEntryDoes > 0) {
      provisionalRewardDoes = rewardAmountDoes;
    }

    ({
      approvedRewardDoes,
      provisionalRewardDoes,
      welcomeRewardDoes,
    } = normalizeRewardSettlementSplit({
      rewardAmountDoes,
      provisionalRewardDoes,
      welcomeRewardDoes,
    }));

    console.log("[BALANCE_DEBUG][FUNCTIONS][claimWinReward] reward split", JSON.stringify({
      uid,
      roomId,
      rewardAmountDoes,
      approvedEntryDoes,
      provisionalEntryDoes,
      welcomeEntryDoes,
      provisionalSources,
      approvedRewardDoes,
      provisionalRewardDoes,
      welcomeRewardDoes,
    }));

    const walletMutation = await applyWalletMutationTx(tx, {
      uid,
      email,
      type: "game_reward",
      note: `Gain de partie (${roomId})`,
      amountDoes: rewardAmountDoes,
      approvedRewardDoes,
      provisionalRewardDoes,
      welcomeRewardDoes,
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
      welcomeRewardDoes,
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
      stakeDoes: safeInt(room.entryCostDoes || room.stakeDoes),
      does: walletMutation.afterDoes,
      approvedRewardDoes,
      provisionalRewardDoes,
    };
  });

  let agentCommissionDoes = 0;
  if (result?.rewardGranted === true) {
    try {
      const agentCommission = await awardAgentCommissionForClientWin({
        playerUid: uid,
        gameType: "domino_classic",
        roomId,
        stakeDoes: safeInt(result.stakeDoes),
        rewardDoes: safeInt(result.rewardAmountDoes),
        wonAtMs: Date.now(),
      });
      agentCommissionDoes = safeInt(agentCommission?.commissionDoes);
    } catch (error) {
      console.error("[AGENT_COMMISSION][claimWinReward] skipped", {
        uid,
        roomId,
        error: error?.message || String(error || ""),
      });
    }
  }

  return {
    ...result,
    agentCommissionDoes,
  };
});

function morpionRoomRef(roomId = "") {
  const safeRoomId = String(roomId || "").trim();
  return safeRoomId
    ? db.collection(MORPION_ROOMS_COLLECTION).doc(safeRoomId)
    : db.collection(MORPION_ROOMS_COLLECTION).doc();
}

function morpionGameStateRef(roomId = "") {
  return db.collection(MORPION_GAME_STATES_COLLECTION).doc(String(roomId || "").trim());
}

function morpionRoomResultRef(roomId = "") {
  return db.collection(MORPION_ROOM_RESULTS_COLLECTION).doc(String(roomId || "").trim());
}

function morpionMatchmakingPoolRef(stakeConfigId = "", stakeDoes = 0) {
  const cleanStakeConfigId = String(stakeConfigId || "").trim() || `morpion_${safeInt(stakeDoes)}`;
  return db.collection(MORPION_MATCHMAKING_POOLS_COLLECTION).doc(`${cleanStakeConfigId}_${safeInt(stakeDoes)}`);
}

function morpionPlayerProfileRef(uid = "") {
  return db.collection(MORPION_PLAYER_PROFILES_COLLECTION).doc(String(uid || "").trim());
}

function morpionWaitingRequestRef(uid = "") {
  return db.collection(MORPION_WAITING_REQUESTS_COLLECTION).doc(String(uid || "").trim());
}

function morpionPlayInvitationRef(invitationId = "") {
  const safeId = String(invitationId || "").trim();
  return safeId
    ? db.collection(MORPION_PLAY_INVITATIONS_COLLECTION).doc(safeId)
    : db.collection(MORPION_PLAY_INVITATIONS_COLLECTION).doc();
}

function isMorpionWaitingRequestOnline(requestData = {}, nowMs = Date.now()) {
  const lastSeenMs = safeSignedInt(requestData.lastSeenMs);
  return lastSeenMs > 0 && (nowMs - lastSeenMs) <= MORPION_WAITING_ONLINE_WINDOW_MS;
}

async function upsertMorpionWaitingRequest(uid = "", payload = {}) {
  const safeUid = String(uid || "").trim();
  if (!safeUid) return;
  const nowMs = Date.now();
  await morpionWaitingRequestRef(safeUid).set({
    uid: safeUid,
    roomId: String(payload.roomId || "").trim(),
    stakeDoes: safeInt(payload.stakeDoes),
    status: String(payload.status || "pending").trim() || "pending",
    createdAtMs: safeSignedInt(payload.createdAtMs) > 0 ? safeSignedInt(payload.createdAtMs) : nowMs,
    lastAttemptAtMs: nowMs,
    lastSeenMs: safeSignedInt(payload.lastSeenMs) > 0 ? safeSignedInt(payload.lastSeenMs) : nowMs,
    updatedAtMs: nowMs,
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  }, { merge: true });
}

function computeMorpionBotBlockingState(profile = {}, didWinVsBot = false) {
  const prevGames = safeInt(profile.botGames);
  const prevWins = safeInt(profile.botWins);
  const prevLosses = safeInt(profile.botLosses);
  const prevDraws = safeInt(profile.botDraws);
  const prevWinStreak = safeInt(profile.botWinStreak);
  const wasBlocked = profile.botBlocked === true;

  const nextGames = prevGames + 1;
  const nextWins = prevWins + (didWinVsBot ? 1 : 0);
  const nextLosses = prevLosses + (didWinVsBot ? 0 : 1);
  const nextDraws = prevDraws;
  const nextWinStreak = didWinVsBot ? (prevWinStreak + 1) : 0;
  const nextWinRate = nextGames > 0 ? (nextWins / nextGames) : 0;
  const hitsStreakThreshold = nextWinStreak >= MORPION_BOT_BLOCK_WIN_STREAK;
  const hitsRateThreshold = nextGames >= MORPION_BOT_BLOCK_MIN_GAMES && nextWinRate >= MORPION_BOT_BLOCK_MIN_WIN_RATE;
  const shouldBlockNow = wasBlocked || hitsStreakThreshold || hitsRateThreshold;
  const blockReason = wasBlocked
    ? String(profile.botBlockedReason || "locked")
    : (hitsStreakThreshold ? "win_streak" : (hitsRateThreshold ? "win_rate" : ""));

  return {
    nextGames,
    nextWins,
    nextLosses,
    nextDraws,
    nextWinStreak,
    nextWinRate,
    shouldBlockNow,
    blockReason,
  };
}

async function recordMorpionBotOutcomeProfilesIfNeeded(roomId = "") {
  const safeRoomId = String(roomId || "").trim();
  if (!safeRoomId) return false;
  const roomRefDoc = morpionRoomRef(safeRoomId);

  return db.runTransaction(async (tx) => {
    const roomSnap = await tx.get(roomRefDoc);
    if (!roomSnap.exists) return false;

    const room = roomSnap.data() || {};
    if (String(room.status || "").trim().toLowerCase() !== "ended") return false;
    if (safeInt(room.botCount) <= 0) return false;
    if (safeSignedInt(room.botOutcomeProfileAppliedAtMs) > 0) return false;

    const playerUids = Array.from({ length: 2 }, (_, idx) => String((room.playerUids || [])[idx] || "").trim());
    const humanPlayers = playerUids
      .map((playerUid, seatIndex) => ({ uid: playerUid, seatIndex }))
      .filter((entry) => entry.uid && isMorpionSeatHuman(room, entry.seatIndex));

    if (!humanPlayers.length) {
      tx.update(roomRefDoc, {
        botOutcomeProfileAppliedAtMs: Date.now(),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      });
      return false;
    }

    const winnerSeat = safeSignedInt(room.winnerSeat, -1);
    const winnerUid = String(room.winnerUid || "").trim();
    const nowMs = Date.now();
    const profileRefs = humanPlayers.map((entry) => morpionPlayerProfileRef(entry.uid));
    const profileSnaps = await Promise.all(profileRefs.map((ref) => tx.get(ref)));

    humanPlayers.forEach((entry, idx) => {
      const profileData = profileSnaps[idx].exists ? (profileSnaps[idx].data() || {}) : {};
      const didWinVsBot = (winnerUid && winnerUid === entry.uid) || (winnerSeat >= 0 && winnerSeat === entry.seatIndex);
      const next = computeMorpionBotBlockingState(profileData, didWinVsBot);
      tx.set(profileRefs[idx], {
        uid: entry.uid,
        botGames: next.nextGames,
        botWins: next.nextWins,
        botLosses: next.nextLosses,
        botDraws: next.nextDraws,
        botWinStreak: next.nextWinStreak,
        botWinRate: Number(next.nextWinRate.toFixed(4)),
        botBlocked: next.shouldBlockNow,
        botBlockedReason: next.blockReason || admin.firestore.FieldValue.delete(),
        lastBotOutcome: didWinVsBot ? "win" : "loss",
        lastBotRoomId: safeRoomId,
        lastBotEndedAtMs: safeSignedInt(room.endedAtMs) || nowMs,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        ...(profileSnaps[idx].exists ? {} : { createdAt: admin.firestore.FieldValue.serverTimestamp() }),
      }, { merge: true });
    });

    tx.update(roomRefDoc, {
      botOutcomeProfileAppliedAtMs: nowMs,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    });

    logMorpionServerDebug("botOutcome:profilesUpdated", {
      roomId: safeRoomId,
      players: humanPlayers.map((entry) => entry.uid),
      winnerSeat,
      winnerUid,
    });
    return true;
  });
}

function logMorpionServerDebug(event, payload = {}) {
  try {
    console.log("[MORPION_DEBUG][FUNCTIONS]", JSON.stringify({
      event: String(event || ""),
      ts: new Date().toISOString(),
      ...payload,
    }));
  } catch (_) {
    console.log("[MORPION_DEBUG][FUNCTIONS]", event, payload);
  }
}

function setMorpionMatchmakingPoolOpen(tx, poolRef, roomId, stakeConfigId = "", stakeDoes = 0) {
  tx.set(poolRef, {
    openRoomId: String(roomId || "").trim(),
    stakeConfigId: String(stakeConfigId || "").trim(),
    stakeDoes: safeInt(stakeDoes),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  }, { merge: true });
}

function clearMorpionMatchmakingPool(tx, poolRef) {
  tx.set(poolRef, {
    openRoomId: "",
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  }, { merge: true });
}

function resolveMorpionWaitingDeadlineMs(room = {}, nowMs = Date.now()) {
  const explicit = safeSignedInt(room.waitingDeadlineMs);
  if (explicit > 0) return explicit;
  const createdAtMs = safeSignedInt(room.createdAtMs);
  if (createdAtMs > 0) return createdAtMs + (isFriendMorpionRoom(room) ? FRIEND_ROOM_WAIT_MS : ROOM_WAIT_MS);
  return nowMs + (isFriendMorpionRoom(room) ? FRIEND_ROOM_WAIT_MS : ROOM_WAIT_MS);
}

function getMorpionStakeConfigByAmount(stakeDoes) {
  const targetStakeDoes = safeInt(stakeDoes);
  return DEFAULT_MORPION_STAKE_OPTIONS.find((item) => item.enabled !== false && safeInt(item.stakeDoes) === targetStakeDoes) || null;
}

function buildPrivateMorpionRewardDoes(stakeDoes = 0) {
  const safeStakeDoes = safeInt(stakeDoes);
  if (safeStakeDoes <= 0) return 0;
  return Math.max(1, Math.round(safeStakeDoes * 1.8));
}

function parseStrictWholePositiveDoes(value, fallback = 0) {
  if (typeof value === "number") {
    if (!Number.isFinite(value) || !Number.isInteger(value) || value <= 0) return fallback;
    return Math.trunc(value);
  }
  const raw = String(value ?? "").trim();
  if (!/^\d+$/.test(raw)) return fallback;
  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function resolveMorpionFriendStakeDoes(value) {
  const parsedStakeDoes = parseStrictWholePositiveDoes(value, 500);
  if (parsedStakeDoes <= 0) {
    return 500;
  }
  return 500;
}

function buildZeroMorpionEntryFunding() {
  return {
    approvedDoes: 0,
    provisionalDoes: 0,
    welcomeDoes: 0,
    provisionalSources: [],
  };
}

function isMorpionSeatHuman(room = {}, seat) {
  const safeSeat = safeSignedInt(seat, -1);
  if (safeSeat < 0 || safeSeat > 1) return false;
  const playerUids = Array.isArray(room.playerUids) ? room.playerUids : [];
  const seatUid = String(playerUids[safeSeat] || "").trim();
  if (!seatUid) return false;
  return !getBotTakeoverSeatSet(room).has(safeSeat);
}

async function findActiveMorpionRoomForUser(uid) {
  const rooms = db.collection(MORPION_ROOMS_COLLECTION);
  const membershipSnap = await rooms
    .where("playerUids", "array-contains", uid)
    .limit(8)
    .get();

  if (membershipSnap.empty) return null;

  const candidate = membershipSnap.docs
    .filter((docSnap) => {
      const data = docSnap.data() || {};
      if (getBlockedRejoinSet(data).has(uid)) return false;
      const status = String(data.status || "");
      return status === "playing" || status === "waiting";
    })
    .sort((left, right) => {
      const leftData = left.data() || {};
      const rightData = right.data() || {};
      const statusScore = (value) => (String(value || "") === "playing" ? 2 : 1);
      const statusDelta = statusScore(rightData.status) - statusScore(leftData.status);
      if (statusDelta !== 0) return statusDelta;
      const rightUpdated = Math.max(
        safeSignedInt(rightData.updatedAtMs),
        safeSignedInt(rightData.startedAtMs),
        safeSignedInt(rightData.createdAtMs),
      );
      const leftUpdated = Math.max(
        safeSignedInt(leftData.updatedAtMs),
        safeSignedInt(leftData.startedAtMs),
        safeSignedInt(leftData.createdAtMs),
      );
      return rightUpdated - leftUpdated;
    })[0] || null;

  if (!candidate) return null;

  const data = candidate.data() || {};
  const seats = data.seats && typeof data.seats === "object" ? data.seats : {};
  const seatIndex = typeof seats[uid] === "number" ? seats[uid] : -1;

  return {
    roomId: candidate.id,
    status: String(data.status || ""),
    seatIndex,
    stakeDoes: safeInt(data.entryCostDoes || data.stakeDoes),
    roomMode: String(data.roomMode || "morpion_2p"),
    inviteCode: String(data.inviteCode || "").trim(),
  };
}

function normalizeMorpionExcludedRoomIds(value) {
  const rawItems = Array.isArray(value) ? value : [value];
  return Array.from(new Set(
    rawItems
      .map((item) => String(item || "").trim())
      .filter(Boolean)
  )).slice(0, 8);
}

function buildMorpionPresenceUpdates(room = {}, actorUid = "", nowMs = Date.now()) {
  const currentPresence = room.roomPresenceMs && typeof room.roomPresenceMs === "object"
    ? { ...room.roomPresenceMs }
    : {};
  const actor = String(actorUid || "").trim();
  if (actor) currentPresence[actor] = nowMs;

  const updates = {
    roomPresenceMs: currentPresence,
    botTakeoverSeats: [],
    botCount: 0,
    botGraceUntilMs: admin.firestore.FieldValue.delete(),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };
  const currentPlayerSeat = safeInt(room.currentPlayer);
  const shouldNudgeBots = false;
  const shouldResolveExpiredHumanTurn = String(room.status || "") === "playing"
    && room.startRevealPending !== true
    && currentPlayerSeat >= 0
    && currentPlayerSeat < 2
    && isMorpionSeatHuman(room, currentPlayerSeat)
    && resolveMorpionTurnDeadlineMs(room, nowMs) <= nowMs;

  return {
    updates,
    shouldNudgeBots,
    shouldResolveExpiredHumanTurn,
  };
}

async function applyMorpionLeaveForUidTx(tx, roomRefDoc, room = {}, uid = "", userEmail = "") {
  const safeUid = String(uid || "").trim();
  const currentUids = Array.from({ length: 2 }, (_, idx) => String((room.playerUids || [])[idx] || ""));
  if (!safeUid || !currentUids.includes(safeUid)) {
    return {
      result: { ok: true, deleted: false, status: String(room.status || "") },
      shouldCleanup: false,
      shouldNudgeBots: false,
    };
  }

  const status = String(room.status || "");
  const seatIndex = currentUids.findIndex((candidate) => candidate === safeUid);
  const nextPlayerUids = currentUids.slice();
  if (seatIndex >= 0) nextPlayerUids[seatIndex] = "";
  const nextPlayerNames = Array.from({ length: 2 }, (_, idx) => String((room.playerNames || [])[idx] || ""));
  if (seatIndex >= 0) {
    nextPlayerNames[seatIndex] = "";
  }
  const nextSeats = { ...getRoomSeats(room) };
  delete nextSeats[safeUid];
  const blockedRejoinUids = Array.from(getBlockedRejoinSet(room));
  if (!blockedRejoinUids.includes(safeUid)) {
    blockedRejoinUids.push(safeUid);
  }
  const nextPresence = room.roomPresenceMs && typeof room.roomPresenceMs === "object"
    ? { ...room.roomPresenceMs }
    : {};
  delete nextPresence[safeUid];
  const nextBotTakeoverSeats = getBotTakeoverSeatSet(room);
  nextBotTakeoverSeats.delete(seatIndex);
  const nextGraceUntil = room.botGraceUntilMs && typeof room.botGraceUntilMs === "object"
    ? { ...room.botGraceUntilMs }
    : {};
  delete nextGraceUntil[String(seatIndex)];
  const currentEntryFundingByUid = room.entryFundingByUid && typeof room.entryFundingByUid === "object"
    ? { ...room.entryFundingByUid }
    : {};
  const leavingEntryFunding = currentEntryFundingByUid[safeUid] && typeof currentEntryFundingByUid[safeUid] === "object"
    ? currentEntryFundingByUid[safeUid]
    : null;
  delete currentEntryFundingByUid[safeUid];
  const leavingApprovedDoes = safeInt(leavingEntryFunding?.approvedDoes);
  const leavingProvisionalDoes = safeInt(leavingEntryFunding?.provisionalDoes);
  const leavingWelcomeDoes = Math.max(0, Math.min(leavingApprovedDoes, safeInt(leavingEntryFunding?.welcomeDoes)));
  const refundAmountDoes = Math.max(0, leavingApprovedDoes + leavingProvisionalDoes);

  if (status === "waiting" && refundAmountDoes > 0) {
    await applyWalletMutationTx(tx, {
      uid: safeUid,
      email: String(userEmail || ""),
      type: "game_reward",
      note: `Remboursement morpion (${roomRefDoc.id})`,
      amountDoes: refundAmountDoes,
      approvedRewardDoes: leavingApprovedDoes,
      provisionalRewardDoes: leavingProvisionalDoes,
      welcomeRewardDoes: leavingWelcomeDoes,
      amountGourdes: 0,
      deltaDoes: refundAmountDoes,
      deltaExchangedGourdes: 0,
    });
  }
  const humans = nextPlayerUids.filter(Boolean).length;

  if (humans <= 0) {
    tx.set(roomRefDoc, {
      status: "closing",
      playerUids: ["", ""],
      playerNames: ["", ""],
      blockedRejoinUids,
      seats: {},
      roomPresenceMs: nextPresence,
      humanCount: 0,
      botCount: 0,
      entryFundingByUid: currentEntryFundingByUid,
      botTakeoverSeats: [],
      botGraceUntilMs: admin.firestore.FieldValue.delete(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
    return {
      result: { ok: true, deleted: true, status: "closing" },
      shouldCleanup: true,
      shouldNudgeBots: false,
    };
  }

  const nextAckUids = Array.isArray(room.startRevealAckUids)
    ? room.startRevealAckUids.map((item) => String(item || "").trim()).filter(Boolean).filter((item) => item !== safeUid)
    : [];
  const revealPending = room.startRevealPending === true;
  const revealReady = revealPending === true
    && nextPlayerUids.filter(Boolean).every((playerUid) => nextAckUids.includes(playerUid));
  const nextBotCount = 0;
  const winnerSeatOnLeave = status === "playing" && humans === 1
    ? nextPlayerUids.findIndex((candidate) => String(candidate || "").trim())
    : -1;
  const updates = {
    playerUids: nextPlayerUids,
    playerNames: nextPlayerNames,
    blockedRejoinUids,
    seats: nextSeats,
    roomPresenceMs: nextPresence,
    entryFundingByUid: currentEntryFundingByUid,
    humanCount: humans,
    botCount: nextBotCount,
    botTakeoverSeats: [],
    botGraceUntilMs: Object.keys(nextGraceUntil).length > 0 ? nextGraceUntil : admin.firestore.FieldValue.delete(),
    startRevealAckUids: nextAckUids,
    startRevealPending: revealPending === true ? !revealReady : false,
    ownerUid: room.ownerUid === safeUid ? String(nextPlayerUids.find(Boolean) || "") : String(room.ownerUid || ""),
    ...(winnerSeatOnLeave >= 0
      ? {
        status: "ended",
        winnerSeat: winnerSeatOnLeave,
        winnerUid: String(nextPlayerUids[winnerSeatOnLeave] || "").trim(),
        endedReason: "opponent_left",
        endedAt: admin.firestore.FieldValue.serverTimestamp(),
        endedAtMs: Date.now(),
      }
      : {}),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };

  tx.update(roomRefDoc, updates);

  const effectiveRoom = {
    ...room,
    playerUids: nextPlayerUids,
    playerNames: nextPlayerNames,
    seats: nextSeats,
    botTakeoverSeats: updates.botTakeoverSeats,
  };
  const currentPlayerSeat = safeInt(room.currentPlayer);
  const shouldNudgeBots = false;

  return {
    result: {
      ok: true,
      deleted: false,
      status,
      humanCount: humans,
      botCount: nextBotCount,
      revealPending: updates.startRevealPending === true,
    },
    shouldCleanup: false,
    shouldNudgeBots,
  };
}

async function forceRemoveUserFromMorpionRoom(roomId = "", uid = "", userEmail = "") {
  const safeRoomId = String(roomId || "").trim();
  const safeUid = String(uid || "").trim();
  if (!safeRoomId || !safeUid) return { ok: true, deleted: false, status: "skipped" };

  const roomRefDoc = morpionRoomRef(safeRoomId);
  const outcome = await db.runTransaction(async (tx) => {
    const roomSnap = await tx.get(roomRefDoc);
    if (!roomSnap.exists) {
      return {
        result: { ok: true, deleted: true, status: "missing" },
        shouldCleanup: false,
        shouldNudgeBots: false,
      };
    }
    return await applyMorpionLeaveForUidTx(tx, roomRefDoc, roomSnap.data() || {}, safeUid, userEmail);
  });

  if (outcome?.shouldNudgeBots) {
    await processPendingBotTurnsMorpion(safeRoomId);
  }
  if (outcome?.shouldCleanup) {
    await cleanupMorpionRoom(roomRefDoc);
    return { ok: true, deleted: true, status: "deleted" };
  }
  return outcome?.result || { ok: true, deleted: false, status: "left" };
}

function buildEmptyMorpionBoard() {
  return Array.from({ length: 15 * 15 }, () => -1);
}

function normalizeMorpionBoard(raw = []) {
  if (!Array.isArray(raw) || raw.length !== 225) return buildEmptyMorpionBoard();
  return raw.map((cell) => {
    const parsed = Number(cell);
    return parsed === 0 || parsed === 1 ? parsed : -1;
  });
}

function getMorpionRowCol(index) {
  const safeIndex = Math.max(0, Math.min(224, safeInt(index)));
  return {
    row: Math.floor(safeIndex / 15),
    col: safeIndex % 15,
  };
}

function getMorpionCellIndex(row, col) {
  return (row * 15) + col;
}

function checkMorpionWinningLine(board = [], cellIndex = 0, seat = 0) {
  const { row, col } = getMorpionRowCol(cellIndex);
  const directions = [
    [1, 0],
    [0, 1],
    [1, 1],
    [1, -1],
  ];

  for (const [deltaRow, deltaCol] of directions) {
    const line = [cellIndex];
    let nextRow = row + deltaRow;
    let nextCol = col + deltaCol;

    while (nextRow >= 0 && nextRow < 15 && nextCol >= 0 && nextCol < 15) {
      const nextIndex = getMorpionCellIndex(nextRow, nextCol);
      if (board[nextIndex] !== seat) break;
      line.push(nextIndex);
      nextRow += deltaRow;
      nextCol += deltaCol;
    }

    nextRow = row - deltaRow;
    nextCol = col - deltaCol;
    while (nextRow >= 0 && nextRow < 15 && nextCol >= 0 && nextCol < 15) {
      const nextIndex = getMorpionCellIndex(nextRow, nextCol);
      if (board[nextIndex] !== seat) break;
      line.unshift(nextIndex);
      nextRow -= deltaRow;
      nextCol -= deltaCol;
    }

    if (line.length >= 5) return line.slice(0, 5);
  }

  return [];
}

function isMorpionBoardFull(board = []) {
  return Array.isArray(board) && board.length === 225 && board.every((cell) => cell === 0 || cell === 1);
}

function createInitialMorpionGameState(room = {}) {
  const initialCurrentPlayer = Math.random() >= 0.5 ? 1 : 0;
  return {
    board: buildEmptyMorpionBoard(),
    currentPlayer: initialCurrentPlayer,
    moveCount: 0,
    winnerSeat: -1,
    winnerUid: "",
    endedReason: "",
    winningLine: [],
    appliedActionSeq: 0,
    idempotencyKeys: {},
  };
}

function normalizeMorpionGameState(raw = {}, room = {}) {
  const winnerSeat = safeSignedInt(raw.winnerSeat);
  const board = normalizeMorpionBoard(raw.board);
  return {
    board,
    currentPlayer: safeSignedInt(raw.currentPlayer, 0),
    moveCount: safeInt(raw.moveCount),
    winnerSeat: winnerSeat >= 0 ? winnerSeat : -1,
    winnerUid: String(raw.winnerUid || "").trim(),
    endedReason: String(raw.endedReason || "").trim(),
    winningLine: Array.isArray(raw.winningLine) ? raw.winningLine.map((item) => safeInt(item)).filter((item) => item >= 0 && item < 225).slice(0, 5) : [],
    appliedActionSeq: safeInt(raw.appliedActionSeq),
    idempotencyKeys: raw.idempotencyKeys && typeof raw.idempotencyKeys === "object" ? { ...raw.idempotencyKeys } : {},
    playerUids: Array.from({ length: 2 }, (_, idx) => String((room.playerUids || [])[idx] || "")),
  };
}

function buildMorpionGameStateWrite(nextState) {
  return {
    board: Array.isArray(nextState.board) ? nextState.board.slice(0, 225) : buildEmptyMorpionBoard(),
    currentPlayer: safeSignedInt(nextState.currentPlayer),
    moveCount: safeInt(nextState.moveCount),
    winnerSeat: safeSignedInt(nextState.winnerSeat, -1),
    winnerUid: String(nextState.winnerUid || "").trim(),
    endedReason: String(nextState.endedReason || "").trim(),
    winningLine: Array.isArray(nextState.winningLine) ? nextState.winningLine.slice(0, 5) : [],
    appliedActionSeq: safeInt(nextState.appliedActionSeq),
    idempotencyKeys: trimIdempotencyKeys(nextState.idempotencyKeys),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };
}

function getLegalMorpionMoves(state = {}) {
  if (safeSignedInt(state.winnerSeat, -1) >= 0) return [];
  const board = Array.isArray(state.board) ? state.board : [];
  const moves = [];
  for (let index = 0; index < 225; index += 1) {
    if (board[index] === -1) {
      moves.push(index);
    }
  }
  return moves;
}

function evaluateMorpionDirection(board = [], row = 0, col = 0, seat = 0, deltaRow = 0, deltaCol = 0) {
  let forward = 0;
  let nextRow = row + deltaRow;
  let nextCol = col + deltaCol;
  while (nextRow >= 0 && nextRow < 15 && nextCol >= 0 && nextCol < 15) {
    const nextIndex = getMorpionCellIndex(nextRow, nextCol);
    if (board[nextIndex] !== seat) break;
    forward += 1;
    nextRow += deltaRow;
    nextCol += deltaCol;
  }
  const forwardOpen = nextRow >= 0
    && nextRow < 15
    && nextCol >= 0
    && nextCol < 15
    && board[getMorpionCellIndex(nextRow, nextCol)] === -1;

  let backward = 0;
  nextRow = row - deltaRow;
  nextCol = col - deltaCol;
  while (nextRow >= 0 && nextRow < 15 && nextCol >= 0 && nextCol < 15) {
    const nextIndex = getMorpionCellIndex(nextRow, nextCol);
    if (board[nextIndex] !== seat) break;
    backward += 1;
    nextRow -= deltaRow;
    nextCol -= deltaCol;
  }
  const backwardOpen = nextRow >= 0
    && nextRow < 15
    && nextCol >= 0
    && nextCol < 15
    && board[getMorpionCellIndex(nextRow, nextCol)] === -1;

  return {
    lineLength: 1 + forward + backward,
    openEnds: (forwardOpen ? 1 : 0) + (backwardOpen ? 1 : 0),
  };
}

function scoreMorpionMoveCandidate(board = [], index = 0, seat = 0) {
  const { row, col } = getMorpionRowCol(index);
  const directions = [
    [1, 0],
    [0, 1],
    [1, 1],
    [1, -1],
  ];
  let bestLength = 1;
  let bestOpenEnds = 0;
  let totalPressure = 0;

  directions.forEach(([deltaRow, deltaCol]) => {
    const result = evaluateMorpionDirection(board, row, col, seat, deltaRow, deltaCol);
    bestLength = Math.max(bestLength, result.lineLength);
    bestOpenEnds = Math.max(bestOpenEnds, result.openEnds);
    totalPressure += (result.lineLength * result.lineLength * 10) + (result.openEnds * 12);
  });

  const centerDistance = Math.abs(7 - row) + Math.abs(7 - col);
  return {
    bestLength,
    bestOpenEnds,
    score: totalPressure - (centerDistance * 3),
  };
}

const MORPION_SEARCH_WIN_SCORE = 10_000_000;
const MORPION_ULTRA_SEARCH_DEPTH = 5;
const MORPION_ULTRA_ROOT_CANDIDATES = 28;
const MORPION_ULTRA_CHILD_CANDIDATES = 18;

function scoreMorpionThreatPattern(result = {}) {
  const lineLength = safeInt(result.lineLength);
  const openEnds = safeInt(result.openEnds);
  if (lineLength >= 5) return 5_000_000;
  if (lineLength === 4 && openEnds >= 2) return 650_000;
  if (lineLength === 4 && openEnds === 1) return 160_000;
  if (lineLength === 3 && openEnds >= 2) return 48_000;
  if (lineLength === 3 && openEnds === 1) return 9_500;
  if (lineLength === 2 && openEnds >= 2) return 2_200;
  if (lineLength === 2 && openEnds === 1) return 520;
  return Math.max(0, lineLength * 32) + (openEnds * 18);
}

function analyzeMorpionPlacement(board = [], index = 0, seat = 0) {
  const placedBoard = Array.isArray(board) ? board.slice(0, 225) : buildEmptyMorpionBoard();
  placedBoard[index] = seat;
  const line = checkMorpionWinningLine(placedBoard, index, seat);
  const candidate = scoreMorpionMoveCandidate(placedBoard, index, seat);
  const { row, col } = getMorpionRowCol(index);
  const directions = [
    [1, 0],
    [0, 1],
    [1, 1],
    [1, -1],
  ];
  let openFourCount = 0;
  let closedFourCount = 0;
  let openThreeCount = 0;
  directions.forEach(([deltaRow, deltaCol]) => {
    const directional = evaluateMorpionDirection(placedBoard, row, col, seat, deltaRow, deltaCol);
    const lineLength = safeInt(directional.lineLength);
    const openEnds = safeInt(directional.openEnds);
    if (lineLength >= 4 && openEnds >= 2) openFourCount += 1;
    else if (lineLength >= 4 && openEnds === 1) closedFourCount += 1;
    else if (lineLength === 3 && openEnds >= 2) openThreeCount += 1;
  });
  let tacticalBonus = 0;
  if (openFourCount >= 2) tacticalBonus += 2_400_000;
  else if (openFourCount >= 1 && openThreeCount >= 1) tacticalBonus += 1_350_000;
  else if (openFourCount >= 1) tacticalBonus += 900_000;
  if (openThreeCount >= 2) tacticalBonus += 320_000;
  if (closedFourCount >= 2) tacticalBonus += 180_000;

  return {
    board: placedBoard,
    isWin: line.length >= 5,
    winningLine: line,
    lineLength: safeInt(candidate.bestLength),
    openEnds: safeInt(candidate.bestOpenEnds),
    rawScore: safeInt(candidate.score),
    openFourCount,
    closedFourCount,
    openThreeCount,
    threatScore: scoreMorpionThreatPattern({
      lineLength: candidate.bestLength,
      openEnds: candidate.bestOpenEnds,
    }) + safeInt(candidate.score) + tacticalBonus,
  };
}

function getImmediateWinningMorpionMoves(board = [], seat = 0, candidateMoves = null) {
  const legalMoves = Array.isArray(candidateMoves)
    ? candidateMoves
    : getCandidateMorpionMovesFromBoard(board, seat, { maxCandidates: 225 });
  const winners = [];
  for (const index of legalMoves) {
    if (board[index] !== -1) continue;
    const analysis = analyzeMorpionPlacement(board, index, seat);
    if (analysis.isWin) winners.push(index);
  }
  return winners;
}

function getMorpionThreatMovesComprehensive(board = [], seat = 0) {
  const threats = [];
  for (let index = 0; index < 225; index += 1) {
    if (board[index] !== -1) continue;
    const analysis = analyzeMorpionPlacement(board, index, seat);
    const isCritical =
      analysis.isWin
      || analysis.openFourCount >= 1
      || analysis.closedFourCount >= 1
      || (analysis.lineLength >= 4 && analysis.openEnds >= 1)
      || (analysis.openThreeCount >= 1);
    if (!isCritical) continue;

    let severity = 0;
    if (analysis.isWin) severity += 25_000_000;
    severity += (analysis.openFourCount * 3_000_000);
    severity += (analysis.closedFourCount * 1_300_000);
    severity += (analysis.openThreeCount * 460_000);
    severity += safeInt(analysis.threatScore);
    threats.push({
      move: index,
      severity,
      analysis,
    });
  }
  threats.sort((left, right) => right.severity - left.severity);
  return threats;
}

function boardHashMorpion(board = [], currentSeat = 0, depth = 0) {
  const compact = board.map((cell) => {
    if (cell === -1) return "0";
    if (cell === 0) return "1";
    return "2";
  }).join("");
  return `${currentSeat}|${depth}|${compact}`;
}

function createsUnstoppableMorpionFork(board = [], move = 0, botSeat = 0) {
  if (board[move] !== -1) return false;
  const opponentSeat = botSeat === 0 ? 1 : 0;
  const nextBoard = board.slice(0, 225);
  nextBoard[move] = botSeat;
  const opponentImmediateWins = getImmediateWinningMorpionMoves(nextBoard, opponentSeat);
  if (opponentImmediateWins.length > 0) return false;
  const botImmediateWins = getImmediateWinningMorpionMoves(nextBoard, botSeat);
  return botImmediateWins.length >= 2;
}

function getCriticalOpponentThreatMoves(board = [], botSeat = 0) {
  const opponentSeat = botSeat === 0 ? 1 : 0;
  return getMorpionThreatMovesComprehensive(board, opponentSeat).map((item) => ({
    move: item.move,
    severity: item.severity,
    analysis: item.analysis,
  }));
}

function analyzeOpponentForcedPressure(board = [], botSeat = 0) {
  const opponentSeat = botSeat === 0 ? 1 : 0;
  const opponentImmediateWins = getImmediateWinningMorpionMoves(board, opponentSeat);
  const criticalThreats = getCriticalOpponentThreatMoves(board, botSeat);
  const openFourThreats = criticalThreats.filter((item) => safeInt(item?.analysis?.openFourCount) >= 1);
  const doubleOpenThreeThreats = criticalThreats.filter((item) => safeInt(item?.analysis?.openThreeCount) >= 2);
  const closedFourThreats = criticalThreats.filter((item) => safeInt(item?.analysis?.closedFourCount) >= 1);
  const maxSeverity = criticalThreats.length ? safeInt(criticalThreats[0].severity) : 0;

  const isCatastrophic =
    opponentImmediateWins.length > 0
    || openFourThreats.length > 0
    || doubleOpenThreeThreats.length > 0
    || (closedFourThreats.length >= 2)
    || maxSeverity >= 5_000_000;

  return {
    opponentImmediateWins,
    criticalThreats,
    openFourThreats,
    doubleOpenThreeThreats,
    closedFourThreats,
    maxSeverity,
    isCatastrophic,
  };
}

function chooseForcedDefensiveMorpionMove(board = [], botSeat = 0) {
  const threats = getCriticalOpponentThreatMoves(board, botSeat);
  if (!threats.length) return null;

  const opponentSeat = botSeat === 0 ? 1 : 0;
  const blockCandidates = threats
    .map((item) => item.move)
    .filter((move, idx, arr) => board[move] === -1 && arr.indexOf(move) === idx);
  if (!blockCandidates.length) return null;

  const ranked = blockCandidates.map((move) => {
    const nextBoard = board.slice(0, 225);
    nextBoard[move] = botSeat;
    const opponentImmediateWins = getImmediateWinningMorpionMoves(nextBoard, opponentSeat).length;
    const remainingThreats = getCriticalOpponentThreatMoves(nextBoard, botSeat);
    const strongestRemainingThreat = remainingThreats.length > 0
      ? safeInt(remainingThreats[0].severity)
      : 0;
    const ownThreat = analyzeMorpionPlacement(board, move, botSeat).threatScore;
    const evaluation =
      (opponentImmediateWins * -25_000_000)
      + (remainingThreats.length * -180_000)
      - strongestRemainingThreat
      + safeInt(ownThreat);

    return {
      move,
      evaluation,
      opponentImmediateWins,
      remainingThreats: remainingThreats.length,
      strongestRemainingThreat,
      ownThreat,
    };
  }).sort((left, right) => right.evaluation - left.evaluation);

  return ranked[0] || null;
}

function hasMorpionNeighbor(board = [], index = 0, radius = 2) {
  const { row, col } = getMorpionRowCol(index);
  for (let deltaRow = -radius; deltaRow <= radius; deltaRow += 1) {
    for (let deltaCol = -radius; deltaCol <= radius; deltaCol += 1) {
      if (deltaRow === 0 && deltaCol === 0) continue;
      const nextRow = row + deltaRow;
      const nextCol = col + deltaCol;
      if (nextRow < 0 || nextRow >= 15 || nextCol < 0 || nextCol >= 15) continue;
      const nextIndex = getMorpionCellIndex(nextRow, nextCol);
      if (board[nextIndex] === 0 || board[nextIndex] === 1) {
        return true;
      }
    }
  }
  return false;
}

function countMorpionNeighbors(board = [], index = 0, seat = -1, radius = 2) {
  const { row, col } = getMorpionRowCol(index);
  let count = 0;
  for (let deltaRow = -radius; deltaRow <= radius; deltaRow += 1) {
    for (let deltaCol = -radius; deltaCol <= radius; deltaCol += 1) {
      if (deltaRow === 0 && deltaCol === 0) continue;
      const nextRow = row + deltaRow;
      const nextCol = col + deltaCol;
      if (nextRow < 0 || nextRow >= 15 || nextCol < 0 || nextCol >= 15) continue;
      const nextIndex = getMorpionCellIndex(nextRow, nextCol);
      if (seat === -1) {
        if (board[nextIndex] === 0 || board[nextIndex] === 1) count += 1;
      } else if (board[nextIndex] === seat) {
        count += 1;
      }
    }
  }
  return count;
}

function getCandidateMorpionMovesFromBoard(board = [], seat = 0, options = {}) {
  const opponentSeat = seat === 0 ? 1 : 0;
  const maxCandidates = Math.max(1, safeInt(options.maxCandidates, MORPION_ULTRA_ROOT_CANDIDATES));
  const legalMoves = [];
  let occupiedCount = 0;

  for (let index = 0; index < 225; index += 1) {
    const occupant = board[index];
    if (occupant === -1) {
      legalMoves.push(index);
      continue;
    }
    if (occupant === 0 || occupant === 1) occupiedCount += 1;
  }

  if (!legalMoves.length) return [];
  if (occupiedCount === 0) return [getMorpionCellIndex(7, 7)];

  const nearbyMoves = legalMoves.filter((index) => hasMorpionNeighbor(board, index, 2));
  const sourceMoves = nearbyMoves.length ? nearbyMoves : legalMoves;
  const scored = sourceMoves.map((index) => {
    const own = analyzeMorpionPlacement(board, index, seat);
    const opponent = analyzeMorpionPlacement(board, index, opponentSeat);
    const { row, col } = getMorpionRowCol(index);
    const centerBias = 18 - (Math.abs(7 - row) + Math.abs(7 - col));
    const allyNeighbors = countMorpionNeighbors(board, index, seat, 2);
    const enemyNeighbors = countMorpionNeighbors(board, index, opponentSeat, 2);
    const totalNeighbors = countMorpionNeighbors(board, index, -1, 2);
    const tacticalScore =
      (own.isWin ? MORPION_SEARCH_WIN_SCORE : own.threatScore * 1.18)
      + (opponent.isWin ? 4_800_000 : opponent.threatScore * 1.09)
      + (allyNeighbors * 85)
      + (enemyNeighbors * 62)
      + (totalNeighbors * 20)
      + centerBias;
    return {
      index,
      score: tacticalScore,
    };
  }).sort((left, right) => right.score - left.score);

  return scored.slice(0, maxCandidates).map((item) => item.index);
}

function evaluateMorpionBoardState(board = [], botSeat = 0) {
  const opponentSeat = botSeat === 0 ? 1 : 0;
  const botCandidates = getCandidateMorpionMovesFromBoard(board, botSeat, { maxCandidates: 6 });
  const opponentCandidates = getCandidateMorpionMovesFromBoard(board, opponentSeat, { maxCandidates: 6 });
  const topBotThreats = botCandidates.map((index) => analyzeMorpionPlacement(board, index, botSeat).threatScore).sort((a, b) => b - a);
  const topOpponentThreats = opponentCandidates.map((index) => analyzeMorpionPlacement(board, index, opponentSeat).threatScore).sort((a, b) => b - a);
  const botScore =
    safeInt(topBotThreats[0]) * 1.45
    + safeInt(topBotThreats[1]) * 0.92
    + safeInt(topBotThreats[2]) * 0.5;
  const opponentScore =
    safeInt(topOpponentThreats[0]) * 1.52
    + safeInt(topOpponentThreats[1]) * 0.97
    + safeInt(topOpponentThreats[2]) * 0.55;
  return botScore - opponentScore;
}

function searchMorpionMinimax(board = [], currentSeat = 0, botSeat = 0, depth = 0, alpha = -Infinity, beta = Infinity, ply = 0, cache = null) {
  const transposition = cache instanceof Map ? cache : null;
  const cacheKey = transposition ? boardHashMorpion(board, currentSeat, depth) : "";
  if (transposition && transposition.has(cacheKey)) {
    return transposition.get(cacheKey);
  }

  const legalMoves = getCandidateMorpionMovesFromBoard(
    board,
    currentSeat,
    { maxCandidates: ply <= 0 ? MORPION_ULTRA_ROOT_CANDIDATES : MORPION_ULTRA_CHILD_CANDIDATES }
  );
  if (!legalMoves.length) {
    const score = evaluateMorpionBoardState(board, botSeat);
    if (transposition) transposition.set(cacheKey, score);
    return score;
  }
  if (depth <= 0) {
    const score = evaluateMorpionBoardState(board, botSeat);
    if (transposition) transposition.set(cacheKey, score);
    return score;
  }

  const maximizing = currentSeat === botSeat;
  const nextSeat = currentSeat === 0 ? 1 : 0;

  if (maximizing) {
    let bestScore = -Infinity;
    for (const move of legalMoves) {
      const analysis = analyzeMorpionPlacement(board, move, currentSeat);
      const score = analysis.isWin
        ? (MORPION_SEARCH_WIN_SCORE - (ply * 1000))
        : searchMorpionMinimax(analysis.board, nextSeat, botSeat, depth - 1, alpha, beta, ply + 1, transposition);
      bestScore = Math.max(bestScore, score);
      alpha = Math.max(alpha, score);
      if (beta <= alpha) break;
    }
    if (transposition) transposition.set(cacheKey, bestScore);
    return bestScore;
  }

  let bestScore = Infinity;
  for (const move of legalMoves) {
    const analysis = analyzeMorpionPlacement(board, move, currentSeat);
    const score = analysis.isWin
      ? (-MORPION_SEARCH_WIN_SCORE + (ply * 1000))
      : searchMorpionMinimax(analysis.board, nextSeat, botSeat, depth - 1, alpha, beta, ply + 1, transposition);
    bestScore = Math.min(bestScore, score);
    beta = Math.min(beta, score);
    if (beta <= alpha) break;
  }
  if (transposition) transposition.set(cacheKey, bestScore);
  return bestScore;
}

function chooseUltraMorpionBotMove(state = {}, botSeat = 0) {
  const board = Array.isArray(state.board) ? state.board.slice(0, 225) : buildEmptyMorpionBoard();
  const rootMoves = getCandidateMorpionMovesFromBoard(board, botSeat, { maxCandidates: MORPION_ULTRA_ROOT_CANDIDATES });
  if (!rootMoves.length) {
    throw new HttpsError("failed-precondition", "Aucun coup morpion disponible pour le bot.");
  }

  const opponentSeat = botSeat === 0 ? 1 : 0;
  const immediateWinMoves = getImmediateWinningMorpionMoves(board, botSeat, rootMoves);
  if (immediateWinMoves.length > 0) {
    return { seat: botSeat, cellIndex: immediateWinMoves[0] };
  }

  const opponentImmediateWins = getImmediateWinningMorpionMoves(board, opponentSeat);
  if (opponentImmediateWins.length === 1 && board[opponentImmediateWins[0]] === -1) {
    return { seat: botSeat, cellIndex: opponentImmediateWins[0] };
  }

  if (opponentImmediateWins.length > 1) {
    const defensive = rootMoves.map((move) => {
      const nextBoard = board.slice(0, 225);
      nextBoard[move] = botSeat;
      const remainingImmediateWins = getImmediateWinningMorpionMoves(nextBoard, opponentSeat);
      return {
        move,
        remaining: remainingImmediateWins.length,
        ownThreat: analyzeMorpionPlacement(board, move, botSeat).threatScore,
      };
    }).sort((left, right) => {
      if (left.remaining !== right.remaining) return left.remaining - right.remaining;
      return right.ownThreat - left.ownThreat;
    });
    if (defensive.length > 0) {
      return { seat: botSeat, cellIndex: defensive[0].move };
    }
  }

  const forcedDefense = chooseForcedDefensiveMorpionMove(board, botSeat);
  if (forcedDefense && forcedDefense.opponentImmediateWins === 0) {
    return { seat: botSeat, cellIndex: forcedDefense.move };
  }

  const forkMoves = rootMoves.filter((move) => createsUnstoppableMorpionFork(board, move, botSeat));
  if (forkMoves.length > 0) {
    return { seat: botSeat, cellIndex: forkMoves[0] };
  }

  const moveCount = safeInt(state.moveCount);
  const currentOpponentThreats = getCriticalOpponentThreatMoves(board, botSeat);
  const isDangerPhase = currentOpponentThreats.some((item) => {
    const threat = item?.analysis || {};
    return safeInt(threat.openFourCount) >= 1
      || safeInt(threat.closedFourCount) >= 1
      || safeInt(threat.openThreeCount) >= 2;
  });
  const searchDepth = moveCount < 6 ? 5 : (isDangerPhase ? 6 : MORPION_ULTRA_SEARCH_DEPTH);
  const transposition = new Map();
  let bestMove = rootMoves[0];
  let bestScore = -Infinity;
  let alpha = -Infinity;
  const scoredRoot = [];
  const safeRootMoves = [];

  for (const move of rootMoves) {
    const analysis = analyzeMorpionPlacement(board, move, botSeat);
    const pressure = analyzeOpponentForcedPressure(analysis.board, botSeat);
    const botImmediateWinsAfterMove = getImmediateWinningMorpionMoves(analysis.board, botSeat).length;
    const catastrophicWithoutCompensation = pressure.isCatastrophic && botImmediateWinsAfterMove <= 0;
    if (!catastrophicWithoutCompensation) {
      safeRootMoves.push(move);
    }

    let score = analysis.isWin
      ? MORPION_SEARCH_WIN_SCORE
      : searchMorpionMinimax(analysis.board, opponentSeat, botSeat, searchDepth - 1, alpha, Infinity, 1, transposition);
    const nextOpponentThreats = pressure.criticalThreats;
    const nextImmediateWins = pressure.opponentImmediateWins.length;
    if (nextImmediateWins > 0) {
      score -= 50_000_000 * nextImmediateWins;
    } else if (nextOpponentThreats.length > 0) {
      const topSeverity = safeInt(pressure.maxSeverity);
      score -= topSeverity * 1.1;
      score -= nextOpponentThreats.length * 240_000;
      if (pressure.openFourThreats.length > 0) score -= 18_000_000;
      if (pressure.doubleOpenThreeThreats.length > 0) score -= 8_000_000;
      if (pressure.closedFourThreats.length >= 2) score -= 4_000_000;
    }
    if (catastrophicWithoutCompensation) {
      score -= 120_000_000;
    }
    scoredRoot.push({ move, score, threatScore: analysis.threatScore });
    if (score > bestScore) {
      bestScore = score;
      bestMove = move;
    }
    alpha = Math.max(alpha, score);
  }

  scoredRoot.sort((left, right) => {
    if (right.score !== left.score) return right.score - left.score;
    return right.threatScore - left.threatScore;
  });
  if (safeRootMoves.length > 0) {
    const safest = scoredRoot.find((item) => safeRootMoves.includes(item.move));
    if (safest) {
      return { seat: botSeat, cellIndex: safest.move };
    }
  }
  return { seat: botSeat, cellIndex: scoredRoot[0]?.move ?? bestMove };
}

function buildMorpionTimeoutState(state = {}, room = {}) {
  const winnerSeat = state.currentPlayer === 0 ? 1 : 0;
  return {
    ...state,
    winnerSeat,
    winnerUid: isMorpionSeatHuman(room, winnerSeat) ? String((room.playerUids || [])[winnerSeat] || "").trim() : "",
    endedReason: "timeout",
    currentPlayer: winnerSeat,
    winningLine: [],
    appliedActionSeq: safeInt(state.appliedActionSeq) + 1,
  };
}

function applyMorpionMove(state = {}, room = {}, move = {}, actorUid = "") {
  const seat = safeSignedInt(move.seat, -1);
  const cellIndex = safeInt(move.cellIndex, -1);
  if (seat < 0 || seat > 1) {
    throw new HttpsError("invalid-argument", "Joueur morpion invalide.");
  }
  if (cellIndex < 0 || cellIndex >= 225) {
    throw new HttpsError("invalid-argument", "Case morpion invalide.");
  }
  if (safeSignedInt(state.currentPlayer, -1) !== seat) {
    throw new HttpsError("failed-precondition", "Ce n'est pas le tour de ce joueur.");
  }
  if ((state.board || [])[cellIndex] !== -1) {
    throw new HttpsError("failed-precondition", "Cette case est deja occupee.");
  }

  const board = Array.isArray(state.board) ? state.board.slice(0, 225) : buildEmptyMorpionBoard();
  board[cellIndex] = seat;
  const winningLine = checkMorpionWinningLine(board, cellIndex, seat);
  const { row, col } = getMorpionRowCol(cellIndex);
  const winnerSeat = winningLine.length >= 5 ? seat : -1;
  const nextPlayer = winnerSeat >= 0 ? seat : (seat === 0 ? 1 : 0);
  const nextMoveCount = safeInt(state.moveCount) + 1;
  const draw = winnerSeat < 0 && isMorpionBoardFull(board);
  const nextState = {
    ...state,
    board,
    currentPlayer: nextPlayer,
    moveCount: nextMoveCount,
    winnerSeat: draw ? -1 : winnerSeat,
    winnerUid: winnerSeat >= 0 && isMorpionSeatHuman(room, winnerSeat)
      ? String((room.playerUids || [])[winnerSeat] || "").trim()
      : "",
    endedReason: winnerSeat >= 0 ? "line" : (draw ? "draw" : ""),
    winningLine,
    appliedActionSeq: safeInt(state.appliedActionSeq) + 1,
  };
  const record = {
    seq: nextState.appliedActionSeq,
    type: "place",
    player: seat,
    symbol: seat === 0 ? "X" : "O",
    cellIndex,
    row,
    col,
    actorUid: String(actorUid || ""),
  };
  return { state: nextState, record };
}

function chooseHeuristicMorpionBotMove(state = {}, botSeat = 0) {
  const legalMoves = getLegalMorpionMoves(state);
  if (!legalMoves.length) {
    throw new HttpsError("failed-precondition", "Aucun coup morpion disponible pour le bot.");
  }
  for (const index of legalMoves) {
    const board = state.board.slice(0, 225);
    board[index] = botSeat;
    if (checkMorpionWinningLine(board, index, botSeat).length >= 5) {
      return { seat: botSeat, cellIndex: index };
    }
  }

  const opponentSeat = botSeat === 0 ? 1 : 0;
  for (const index of legalMoves) {
    const board = state.board.slice(0, 225);
    board[index] = opponentSeat;
    if (checkMorpionWinningLine(board, index, opponentSeat).length >= 5) {
      return { seat: botSeat, cellIndex: index };
    }
  }

  const scored = legalMoves.map((index) => {
    const botBoard = state.board.slice(0, 225);
    botBoard[index] = botSeat;
    const ownEval = scoreMorpionMoveCandidate(botBoard, index, botSeat);

    const blockBoard = state.board.slice(0, 225);
    blockBoard[index] = opponentSeat;
    const opponentEval = scoreMorpionMoveCandidate(blockBoard, index, opponentSeat);

    const proximityBonus = (() => {
      const { row, col } = getMorpionRowCol(index);
      let nearby = 0;
      for (let deltaRow = -1; deltaRow <= 1; deltaRow += 1) {
        for (let deltaCol = -1; deltaCol <= 1; deltaCol += 1) {
          if (deltaRow === 0 && deltaCol === 0) continue;
          const nextRow = row + deltaRow;
          const nextCol = col + deltaCol;
          if (nextRow < 0 || nextRow >= 15 || nextCol < 0 || nextCol >= 15) continue;
          const nextIndex = getMorpionCellIndex(nextRow, nextCol);
          const occupant = state.board[nextIndex];
          if (occupant === botSeat) nearby += 4;
          else if (occupant === opponentSeat) nearby += 3;
        }
      }
      return nearby;
    })();

    return {
      index,
      score:
        (ownEval.score * 1.25)
        + (ownEval.bestLength * 120)
        + (ownEval.bestOpenEnds * 26)
        + (opponentEval.score * 0.92)
        + (opponentEval.bestLength * 105)
        + (opponentEval.bestOpenEnds * 18)
        + proximityBonus
        + (Math.random() * 0.08),
    };
  }).sort((left, right) => right.score - left.score);

  return { seat: botSeat, cellIndex: scored[0]?.index ?? legalMoves[0] };
}

function chooseMorpionBotMove(room = {}, state = {}, botSeat = 0) {
  return chooseUltraMorpionBotMove(state, botSeat);
}

function computeMorpionBotThinkDelayMs() {
  return Math.min(5000, BOT_THINK_DELAY_MIN_MS + Math.floor(Math.random() * Math.max(120, BOT_THINK_DELAY_MAX_MS - BOT_THINK_DELAY_MIN_MS + 1)));
}

function resolveMorpionTurnDeadlineMs(room = {}, nowMs = Date.now()) {
  const explicit = safeSignedInt(room.turnDeadlineMs);
  if (explicit > 0) return explicit;
  const startedAtMs = safeSignedInt(room.turnStartedAtMs);
  if (startedAtMs > 0) return startedAtMs + MORPION_TURN_LIMIT_MS;
  return nowMs + MORPION_TURN_LIMIT_MS;
}

function buildMorpionRoomUpdateFromGameState(room, nextState, records = []) {
  const lastRecord = records.length > 0 ? records[records.length - 1] : null;
  const nextActionSeq = safeInt(nextState.appliedActionSeq + 1);
  const nowMs = Date.now();
  const nextTurnStartedAtMs = nextState.endedReason ? 0 : nowMs;
  const roomForNextTurn = {
    ...room,
    currentPlayer: nextState.currentPlayer,
    turnStartedAtMs: nextTurnStartedAtMs,
  };
  const turnDeadlineMs = nextState.endedReason ? 0 : (nextTurnStartedAtMs + MORPION_TURN_LIMIT_MS);
  const turnLockedUntilMs = nextState.endedReason
    ? 0
    : (!isMorpionSeatHuman(roomForNextTurn, nextState.currentPlayer)
      ? Math.min(turnDeadlineMs, nextTurnStartedAtMs + computeMorpionBotThinkDelayMs())
      : 0);

  const update = {
    nextActionSeq,
    lastActionSeq: nextState.appliedActionSeq,
    currentPlayer: nextState.currentPlayer,
    turnActual: nextActionSeq,
    turnStartedAt: nextState.endedReason ? admin.firestore.FieldValue.delete() : admin.firestore.FieldValue.serverTimestamp(),
    turnStartedAtMs: nextTurnStartedAtMs,
    turnDeadlineMs,
    playedCount: safeInt(room.playedCount) + records.length,
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    turnLockedUntilMs,
    symbolBySeat: ["X", "O"],
  };

  if (lastRecord) {
    update.lastMove = {
      seq: lastRecord.seq,
      type: lastRecord.type,
      player: lastRecord.player,
      symbol: lastRecord.symbol,
      cellIndex: lastRecord.cellIndex,
      row: lastRecord.row,
      col: lastRecord.col,
    };
  }

  if (nextState.endedReason) {
    update.status = "ended";
    update.winnerSeat = nextState.winnerSeat;
    update.winnerUid = String(nextState.winnerUid || "").trim();
    update.endedReason = nextState.endedReason;
    update.endedAt = admin.firestore.FieldValue.serverTimestamp();
    update.endedAtMs = nowMs;
    update.endClicks = {};
  }

  return update;
}

function writeMorpionRoomResultIfEndedTx(tx, roomRefDoc, room = {}, roomUpdate = {}) {
  const nextStatus = String(roomUpdate.status || room.status || "").trim().toLowerCase();
  if (nextStatus !== "ended") return;
  const snapshot = buildRoomResultSnapshot(roomRefDoc.id, room, roomUpdate);
  tx.set(morpionRoomResultRef(roomRefDoc.id), {
    ...snapshot,
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
  }, { merge: true });
}

function cleanupMorpionRoom(roomRefDoc) {
  return Promise.all([
    deleteCollectionInChunks(roomRefDoc.collection("actions")),
    deleteCollectionInChunks(roomRefDoc.collection("settlements")),
    morpionGameStateRef(roomRefDoc.id).delete().catch(() => null),
  ]).then(() => roomRefDoc.delete());
}

function buildStartedMorpionRoomTransaction(tx, roomRefDoc, room = {}, options = {}) {
  const nowMs = safeSignedInt(options.nowMs) || Date.now();
  const humans = Array.isArray(room.playerUids) ? room.playerUids.filter(Boolean).length : safeInt(room.humanCount);
  const initialState = createInitialMorpionGameState(room);
  tx.set(morpionGameStateRef(roomRefDoc.id), buildMorpionGameStateWrite(initialState), { merge: true });

  const turnDeadlineMs = nowMs + MORPION_TURN_LIMIT_MS;
  const turnLockedUntilMs = !isMorpionSeatHuman(room, initialState.currentPlayer)
    ? Math.min(turnDeadlineMs, nowMs + computeMorpionBotThinkDelayMs())
    : 0;

  const updates = {
    playerUids: Array.isArray(room.playerUids) ? room.playerUids : ["", ""],
    playerNames: Array.isArray(room.playerNames) ? room.playerNames : ["", ""],
    seats: getRoomSeats(room),
    humanCount: humans,
    status: "playing",
    startRevealPending: true,
    startRevealAckUids: [],
    startedHumanCount: humans,
    startedBotCount: Math.max(0, 2 - humans),
    botCount: Math.max(0, 2 - humans),
    symbolBySeat: ["X", "O"],
    startedAt: admin.firestore.FieldValue.serverTimestamp(),
    startedAtMs: nowMs,
    waitingDeadlineMs: admin.firestore.FieldValue.delete(),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    turnStartedAt: admin.firestore.FieldValue.serverTimestamp(),
    turnStartedAtMs: nowMs,
    turnDeadlineMs,
    turnLockedUntilMs,
    currentPlayer: initialState.currentPlayer,
    nextActionSeq: 1,
    lastActionSeq: 0,
    playedCount: 0,
    winnerSeat: admin.firestore.FieldValue.delete(),
    winnerUid: admin.firestore.FieldValue.delete(),
    endedReason: admin.firestore.FieldValue.delete(),
    endedAt: admin.firestore.FieldValue.delete(),
    endedAtMs: admin.firestore.FieldValue.delete(),
    endClicks: {},
  };

  tx.update(roomRefDoc, updates);

  logMorpionServerDebug("buildStartedRoom", {
    roomId: roomRefDoc.id,
    humans,
    botCount: 0,
    initialCurrentPlayer: initialState.currentPlayer,
    startRevealPending: true,
    turnDeadlineMs,
    turnLockedUntilMs,
    playerUids: Array.isArray(room.playerUids) ? room.playerUids : ["", ""],
    seats: getRoomSeats(room),
  });

  return {
    ok: true,
    started: true,
    status: "playing",
    startRevealPending: true,
    humanCount: humans,
    botCount: 0,
    waitingDeadlineMs: 0,
  };
}

async function processPendingBotTurnsMorpion(roomId) {
  const safeRoomId = String(roomId || "").trim();
  if (!safeRoomId) return;

  const roomRefDoc = morpionRoomRef(safeRoomId);
  const stateRef = morpionGameStateRef(safeRoomId);

  while (true) {
    const roomSnap = await roomRefDoc.get();
    if (!roomSnap.exists) {
      logMorpionServerDebug("processBot:roomMissing", { roomId: safeRoomId });
      return;
    }
    const room = roomSnap.data() || {};
    if (String(room.status || "") !== "playing") {
      logMorpionServerDebug("processBot:skipStatus", { roomId: safeRoomId, status: room.status || "" });
      return;
    }
    if (room.startRevealPending === true) {
      logMorpionServerDebug("processBot:skipRevealPending", { roomId: safeRoomId });
      return;
    }
    if (safeSignedInt(room.winnerSeat, -1) >= 0 || String(room.endedReason || "").trim()) {
      logMorpionServerDebug("processBot:skipEnded", {
        roomId: safeRoomId,
        winnerSeat: safeSignedInt(room.winnerSeat, -1),
        endedReason: String(room.endedReason || "").trim(),
      });
      return;
    }

    const outcome = await db.runTransaction(async (tx) => {
      const [liveRoomSnap, stateSnap] = await Promise.all([
        tx.get(roomRefDoc),
        tx.get(stateRef),
      ]);
      if (!liveRoomSnap.exists) {
        logMorpionServerDebug("processBot:txRoomMissing", { roomId: safeRoomId });
        return { processed: false, stop: true };
      }

      const liveRoom = liveRoomSnap.data() || {};
      if (String(liveRoom.status || "") !== "playing" || liveRoom.startRevealPending === true) {
        logMorpionServerDebug("processBot:txSkip", {
          roomId: safeRoomId,
          status: String(liveRoom.status || ""),
          startRevealPending: liveRoom.startRevealPending === true,
        });
        return { processed: false, stop: true };
      }

      const currentState = stateSnap.exists
        ? normalizeMorpionGameState(stateSnap.data(), liveRoom)
        : createInitialMorpionGameState(liveRoom);
      if (currentState.endedReason) {
        return { processed: false, stop: true };
      }

      const safeNowMs = Date.now();
      const activeSeat = safeSignedInt(liveRoom.currentPlayer, -1);
      const activeSeatIsHuman = activeSeat >= 0 && activeSeat <= 1 && isMorpionSeatHuman(liveRoom, activeSeat);
      const turnDeadlineMs = resolveMorpionTurnDeadlineMs(liveRoom, safeNowMs);
      const lockedUntilMs = safeSignedInt(liveRoom.turnLockedUntilMs);

      logMorpionServerDebug("processBot:evaluate", {
        roomId: safeRoomId,
        activeSeat,
        activeSeatIsHuman,
        turnDeadlineMs,
        lockedUntilMs,
        nowMs: safeNowMs,
        humanCount: safeInt(liveRoom.humanCount),
        botCount: safeInt(liveRoom.botCount),
        playerUids: Array.isArray(liveRoom.playerUids) ? liveRoom.playerUids : ["", ""],
        seats: getRoomSeats(liveRoom),
      });

      if (activeSeatIsHuman) {
        if (turnDeadlineMs > safeNowMs) {
          logMorpionServerDebug("processBot:humanStillHasTime", {
            roomId: safeRoomId,
            activeSeat,
            msLeft: turnDeadlineMs - safeNowMs,
          });
          return { processed: false, stop: true };
        }

        const nextState = buildMorpionTimeoutState(currentState, liveRoom);
        const record = {
          seq: nextState.appliedActionSeq,
          type: "timeout",
          player: activeSeat,
          symbol: activeSeat === 0 ? "X" : "O",
          cellIndex: -1,
          row: -1,
          col: -1,
          actorUid: "server:timeout",
        };
        tx.set(stateRef, buildMorpionGameStateWrite(nextState), { merge: true });
        const roomUpdate = buildMorpionRoomUpdateFromGameState(liveRoom, nextState, [record]);
        tx.update(roomRefDoc, roomUpdate);
        tx.set(roomRefDoc.collection("actions").doc(String(record.seq)), {
          ...record,
          roomId: safeRoomId,
          createdAt: admin.firestore.FieldValue.serverTimestamp(),
        }, { merge: true });
        writeMorpionRoomResultIfEndedTx(tx, roomRefDoc, liveRoom, roomUpdate);
        logMorpionServerDebug("processBot:humanTimeout", {
          roomId: safeRoomId,
          loserSeat: activeSeat,
          winnerSeat: nextState.winnerSeat,
        });
        return { processed: true, stop: true, ended: true };
      }

      if (lockedUntilMs > safeNowMs) {
        logMorpionServerDebug("processBot:botLocked", {
          roomId: safeRoomId,
          activeSeat,
          msLeft: lockedUntilMs - safeNowMs,
        });
        return { processed: false, stop: true };
      }

      const botMove = chooseMorpionBotMove(liveRoom, currentState, activeSeat);
      const result = applyMorpionMove(currentState, liveRoom, botMove, "server:bot");
      tx.set(stateRef, buildMorpionGameStateWrite(result.state), { merge: true });
      const roomUpdate = buildMorpionRoomUpdateFromGameState(liveRoom, result.state, [result.record]);
      tx.update(roomRefDoc, roomUpdate);
      tx.set(roomRefDoc.collection("actions").doc(String(result.record.seq)), {
        ...result.record,
        roomId: safeRoomId,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
      }, { merge: true });
      writeMorpionRoomResultIfEndedTx(tx, roomRefDoc, liveRoom, roomUpdate);

      logMorpionServerDebug("processBot:botMoveApplied", {
        roomId: safeRoomId,
        activeSeat,
        cellIndex: botMove.cellIndex,
        nextPlayer: result.state.currentPlayer,
        endedReason: result.state.endedReason || "",
      });

      return {
        processed: true,
        ended: !!result.state.endedReason,
        stop: result.state.endedReason || isMorpionSeatHuman(liveRoom, result.state.currentPlayer),
      };
    });

    if (outcome?.ended) {
      await recordMorpionBotOutcomeProfilesIfNeeded(safeRoomId);
    }

    if (!outcome?.processed || outcome.stop) {
      return;
    }
  }
}

exports.getPublicMorpionStakeOptionsSecure = publicOnCall("getPublicMorpionStakeOptionsSecure", async () => {
  return {
    ok: true,
    options: DEFAULT_MORPION_STAKE_OPTIONS.map((item) => ({
      id: item.id,
      stakeDoes: item.stakeDoes,
      rewardDoes: item.rewardDoes,
      enabled: item.enabled !== false,
      sortOrder: item.sortOrder,
    })),
  };
}, { invoker: "public" });

exports.createFriendMorpionRoom = publicOnCall("createFriendMorpionRoom", async (request) => {
  const { uid, email } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const stakeDoes = resolveMorpionFriendStakeDoes(payload.stakeDoes ?? payload.amountDoes ?? payload.amount);

  const activeRoom = await findActiveMorpionRoomForUser(uid);
  if (activeRoom) {
    throw new HttpsError("failed-precondition", "Tu participes deja a une salle morpion active.", {
      code: "active-room-exists",
      roomId: activeRoom.roomId,
      status: String(activeRoom.status || ""),
      seatIndex: safeInt(activeRoom.seatIndex, 0),
      roomMode: String(activeRoom.roomMode || "morpion_2p"),
      stakeDoes: safeInt(activeRoom.stakeDoes),
      inviteCode: String(activeRoom.inviteCode || "").trim(),
    });
  }

  const [inviteCode] = await Promise.all([
    generateUniqueFriendMorpionInviteCode(),
  ]);
  const rewardAmountDoes = buildPrivateMorpionRewardDoes(stakeDoes);
  const roomRefDoc = morpionRoomRef();

  return db.runTransaction(async (tx) => {
    const walletSnap = await tx.get(walletRef(uid));
    const walletData = walletSnap.exists ? (walletSnap.data() || {}) : {};
    assertWalletNotFrozen(walletData);
    if (safeInt(walletData.doesBalance) < stakeDoes) {
      throw new HttpsError("failed-precondition", "Solde Does insuffisant.");
    }

    const nowMs = Date.now();
    const waitingDeadlineMs = nowMs + FRIEND_ROOM_WAIT_MS;

    tx.set(roomRefDoc, {
      status: "waiting",
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      createdAtMs: nowMs,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      ownerUid: uid,
      roomMode: "morpion_friends",
      isPrivate: true,
      allowBots: false,
      inviteCode,
      inviteCodeNormalized: normalizeCode(inviteCode),
      requiredHumans: 2,
      gameMode: "morpion-5",
      engineVersion: 1,
      playerUids: [uid, ""],
      playerNames: [sanitizePlayerLabel(email || uid, 0), ""],
      entryFundingByUid: {},
      blockedRejoinUids: [],
      humanCount: 1,
      seats: { [uid]: 0 },
      roomPresenceMs: { [uid]: nowMs },
      botCount: 0,
      startRevealPending: false,
      startRevealAckUids: [],
      waitingDeadlineMs,
      startedAt: null,
      startedAtMs: 0,
      endedAtMs: 0,
      turnLockedUntilMs: 0,
      nextActionSeq: 0,
      playedCount: 0,
      symbolBySeat: ["X", "O"],
      stakeDoes,
      entryCostDoes: stakeDoes,
      rewardAmountDoes,
      stakeConfigId: "morpion_friends_500",
    });

    return {
      ok: true,
      roomId: roomRefDoc.id,
      seatIndex: 0,
      status: "waiting",
      roomMode: "morpion_friends",
      charged: false,
      inviteCode,
      requiredHumans: 2,
      waitingDeadlineMs,
      stakeDoes,
      rewardAmountDoes,
      privateDeckOrder: [],
    };
  });
}, { invoker: "public" });

exports.resumeFriendMorpionRoom = publicOnCall("resumeFriendMorpionRoom", async (request) => {
  const { uid } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const roomId = String(payload.roomId || "").trim();
  if (!roomId) {
    throw new HttpsError("invalid-argument", "roomId requis.");
  }

  const roomSnap = await morpionRoomRef(roomId).get();
  if (!roomSnap.exists) {
    throw new HttpsError("not-found", "Salle morpion introuvable.");
  }

  const room = roomSnap.data() || {};
  if (!isFriendMorpionRoom(room)) {
    throw new HttpsError("failed-precondition", "Cette salle morpion n'est pas une salle privee valide.");
  }
  if (getBlockedRejoinSet(room).has(uid)) {
    throw new HttpsError("permission-denied", "Tu ne peux plus rejoindre cette salle morpion.");
  }

  const seatIndex = getSeatForUser(room, uid);
  if (seatIndex < 0) {
    throw new HttpsError("permission-denied", "Tu ne fais pas partie de cette salle morpion.");
  }

  const status = String(room.status || "").trim().toLowerCase();
  const nowMs = Date.now();
  const waitingDeadlineMs = resolveMorpionWaitingDeadlineMs(room, nowMs);
  const humans = Array.isArray(room.playerUids)
    ? room.playerUids.map((item) => String(item || "").trim()).filter(Boolean).length
    : safeInt(room.humanCount);

  if (status === "closed") {
    throw new HttpsError("failed-precondition", "Cette salle morpion n'est plus disponible.");
  }
  if (status === "waiting" && humans < 2 && nowMs >= waitingDeadlineMs) {
    throw new HttpsError("failed-precondition", "Cette salle morpion a expire.");
  }

  return {
    ok: true,
    roomId,
    seatIndex,
    status,
    roomMode: "morpion_friends",
    stakeDoes: safeInt(room.entryCostDoes || room.stakeDoes),
    rewardAmountDoes: safeInt(room.rewardAmountDoes || buildPrivateMorpionRewardDoes(room.entryCostDoes || room.stakeDoes)),
    inviteCode: String(room.inviteCode || "").trim(),
    waitingDeadlineMs,
  };
}, { invoker: "public" });

exports.joinFriendMorpionRoomByCode = publicOnCall("joinFriendMorpionRoomByCode", async (request) => {
  const { uid, email } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const inviteCodeNormalized = normalizeCode(payload.inviteCode || payload.code || "");

  if (!inviteCodeNormalized) {
    throw new HttpsError("invalid-argument", "Code de salle requis.");
  }

  const activeRoom = await findActiveMorpionRoomForUser(uid);
  if (activeRoom) {
    throw new HttpsError("failed-precondition", "Tu participes deja a une salle morpion active.", {
      code: "active-room-exists",
      roomId: activeRoom.roomId,
      status: String(activeRoom.status || ""),
      seatIndex: safeInt(activeRoom.seatIndex, 0),
      roomMode: String(activeRoom.roomMode || "morpion_2p"),
      stakeDoes: safeInt(activeRoom.stakeDoes),
      inviteCode: String(activeRoom.inviteCode || "").trim(),
    });
  }

  const matchingSnap = await db
    .collection(MORPION_ROOMS_COLLECTION)
    .where("inviteCodeNormalized", "==", inviteCodeNormalized)
    .limit(6)
    .get();

  const roomDoc = matchingSnap.docs.find((docSnap) => isFriendMorpionRoom(docSnap.data() || {})) || null;
  if (!roomDoc) {
    throw new HttpsError("not-found", "Code de morpion introuvable.");
  }

  const roomRefDoc = roomDoc.ref;
  const result = await db.runTransaction(async (tx) => {
    const [roomSnap, walletSnap] = await Promise.all([
      tx.get(roomRefDoc),
      tx.get(walletRef(uid)),
    ]);

    if (!roomSnap.exists) {
      throw new HttpsError("not-found", "Salle morpion introuvable.");
    }

    const room = roomSnap.data() || {};
    if (!isFriendMorpionRoom(room)) {
      throw new HttpsError("failed-precondition", "Cette salle morpion n'est pas disponible.");
    }

    const roomStatus = String(room.status || "");
    const nowMs = Date.now();
    const waitingDeadlineMs = resolveMorpionWaitingDeadlineMs(room, nowMs);
    const playerUids = Array.from({ length: 2 }, (_, idx) => String((room.playerUids || [])[idx] || ""));
    const humans = playerUids.filter(Boolean).length;
    const roomStakeDoes = safeInt(room.entryCostDoes || room.stakeDoes);
    const roomRewardAmountDoes = safeInt(room.rewardAmountDoes || buildPrivateMorpionRewardDoes(roomStakeDoes));
    const roomInviteCode = String(room.inviteCode || inviteCodeNormalized || "").trim();

    const walletData = walletSnap.exists ? (walletSnap.data() || {}) : {};
    assertWalletNotFrozen(walletData);

    if (playerUids.includes(uid)) {
      const seatIndex = getSeatForUser(room, uid);
      const nextPresence = room.roomPresenceMs && typeof room.roomPresenceMs === "object"
        ? { ...room.roomPresenceMs }
        : {};
      nextPresence[uid] = nowMs;
      tx.update(roomRefDoc, {
        roomPresenceMs: nextPresence,
        waitingDeadlineMs,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      });
      return {
        ok: true,
        resumed: true,
        charged: false,
        roomId: roomRefDoc.id,
        seatIndex: seatIndex >= 0 ? seatIndex : 0,
        status: roomStatus,
        roomMode: "morpion_friends",
        stakeDoes: roomStakeDoes,
        rewardAmountDoes: roomRewardAmountDoes,
        inviteCode: roomInviteCode,
        waitingDeadlineMs,
      };
    }

    if (roomStatus === "playing") {
      throw new HttpsError("failed-precondition", "Cette salle morpion a deja demarre.");
    }
    if (roomStatus !== "waiting") {
      throw new HttpsError("failed-precondition", "Cette salle morpion n'est plus disponible.");
    }
    if (getBlockedRejoinSet(room).has(uid)) {
      throw new HttpsError("permission-denied", "Tu ne peux plus rejoindre cette salle morpion.");
    }
    if (nowMs >= waitingDeadlineMs && humans < 2) {
      tx.set(roomRefDoc, {
        status: "closed",
        endedReason: "expired",
        endedAt: admin.firestore.FieldValue.serverTimestamp(),
        endedAtMs: nowMs,
        waitingDeadlineMs: admin.firestore.FieldValue.delete(),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      }, { merge: true });
      return {
        ok: false,
        expired: true,
        roomId: roomRefDoc.id,
      };
    }

    if (safeInt(walletData.doesBalance) < roomStakeDoes) {
      throw new HttpsError("failed-precondition", "Solde Does insuffisant.");
    }

    const currentSeats = room.seats && typeof room.seats === "object" ? room.seats : {};
    const usedSeats = new Set(
      Object.values(currentSeats)
        .map((seat) => Number(seat))
        .filter((seat) => Number.isFinite(seat) && seat >= 0 && seat < 2)
    );
    const seatIndex = [0, 1].find((seat) => !usedSeats.has(seat));
    if (typeof seatIndex !== "number" || humans >= 2) {
      throw new HttpsError("failed-precondition", "Cette salle morpion est complete.");
    }

    const nextPlayerUids = playerUids.slice();
    nextPlayerUids[seatIndex] = uid;
    const currentNames = Array.from({ length: 2 }, (_, idx) => String((room.playerNames || [])[idx] || ""));
    const nextPlayerNames = currentNames.slice();
    nextPlayerNames[seatIndex] = sanitizePlayerLabel(email || uid, seatIndex);
    const nextSeats = {
      ...currentSeats,
      [uid]: seatIndex,
    };
    const nextPresence = room.roomPresenceMs && typeof room.roomPresenceMs === "object"
      ? { ...room.roomPresenceMs }
      : {};
    nextPresence[uid] = nowMs;
    const nextHumans = nextPlayerUids.filter(Boolean).length;

    if (nextHumans >= 2) {
      const chargeResult = await chargeRoomEntriesTx(tx, room, nextPlayerUids, roomStakeDoes);
      tx.set(roomRefDoc, {
        playerUids: nextPlayerUids,
        playerNames: nextPlayerNames,
        seats: nextSeats,
        roomPresenceMs: nextPresence,
        humanCount: nextHumans,
        botCount: 0,
        entryFundingByUid: chargeResult.entryFundingByUid,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      }, { merge: true });

      return {
        ok: true,
        resumed: false,
        charged: true,
        roomId: roomRefDoc.id,
        seatIndex,
        does: safeInt(chargeResult.afterDoesByUid[uid]),
        roomMode: "morpion_friends",
        inviteCode: roomInviteCode,
        stakeDoes: roomStakeDoes,
        rewardAmountDoes: roomRewardAmountDoes,
        ...buildStartedMorpionRoomTransaction(tx, roomRefDoc, {
          ...room,
          playerUids: nextPlayerUids,
          playerNames: nextPlayerNames,
          seats: nextSeats,
          entryFundingByUid: chargeResult.entryFundingByUid,
          roomPresenceMs: nextPresence,
          humanCount: nextHumans,
          botCount: 0,
          waitingDeadlineMs,
        }, { nowMs }),
      };
    }

    tx.update(roomRefDoc, {
      playerUids: nextPlayerUids,
      playerNames: nextPlayerNames,
      seats: nextSeats,
      roomPresenceMs: nextPresence,
      humanCount: nextHumans,
      botCount: 0,
      waitingDeadlineMs,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    });

    return {
      ok: true,
      resumed: false,
      charged: false,
      roomId: roomRefDoc.id,
      seatIndex,
      status: "waiting",
      roomMode: "morpion_friends",
      inviteCode: roomInviteCode,
      stakeDoes: roomStakeDoes,
      rewardAmountDoes: roomRewardAmountDoes,
      waitingDeadlineMs,
    };
  });

  if (result?.expired === true) {
    throw new HttpsError("failed-precondition", "Ce code a expire.");
  }

  if (result?.status === "playing" && result?.startRevealPending !== true) {
    await processPendingBotTurnsMorpion(roomRefDoc.id);
  }

  return result;
}, { invoker: "public" });

exports.joinMatchmakingMorpion = publicOnCall("joinMatchmakingMorpion", async (request) => {
  const { uid, email } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const stakeDoes = safeInt(payload.stakeDoes);
  const excludedRoomIds = normalizeMorpionExcludedRoomIds(payload.excludeRoomIds);
  const excludedRoomIdSet = new Set(excludedRoomIds);
  const selectedStakeConfig = getMorpionStakeConfigByAmount(stakeDoes);

  if (!selectedStakeConfig) {
    throw new HttpsError("invalid-argument", "Mise morpion non autorisee.");
  }

  const allowHumanOnlyMatchmaking = true;

  for (const excludedRoomId of excludedRoomIds) {
    await forceRemoveUserFromMorpionRoom(excludedRoomId, uid, email).catch(() => null);
  }

  const activeRoom = await findActiveMorpionRoomForUser(uid);
  if (activeRoom) {
    if (excludedRoomIdSet.has(String(activeRoom.roomId || "").trim())) {
      await forceRemoveUserFromMorpionRoom(activeRoom.roomId, uid, email).catch(() => null);
    } else {
      return {
        ok: true,
        resumed: true,
        charged: false,
        roomId: activeRoom.roomId,
        seatIndex: activeRoom.seatIndex,
        status: activeRoom.status,
        stakeDoes: safeInt(activeRoom.stakeDoes),
      };
    }
  }

  const rewardAmountDoes = selectedStakeConfig.rewardDoes;
  const poolRef = morpionMatchmakingPoolRef(selectedStakeConfig.id, stakeDoes);

  const created = await db.runTransaction(async (tx) => {
    const nowMs = Date.now();
    const [poolSnap, walletSnap] = await Promise.all([
      tx.get(poolRef),
      tx.get(walletRef(uid)),
    ]);
    const walletData = walletSnap.exists ? (walletSnap.data() || {}) : {};
    assertWalletNotFrozen(walletData);
    if (stakeDoes > 0 && safeInt(walletData.doesBalance) < stakeDoes) {
      throw new HttpsError("failed-precondition", "Solde Does insuffisant.");
    }

    const existingOpenRoomId = allowHumanOnlyMatchmaking
      ? String(poolSnap.exists ? (poolSnap.data() || {}).openRoomId || "" : "").trim()
      : "";
    if (existingOpenRoomId) {
      const openRoomRef = morpionRoomRef(existingOpenRoomId);
      const roomSnap = await tx.get(openRoomRef);
      if (roomSnap.exists) {
        const room = roomSnap.data() || {};
        const status = String(room.status || "");
        const roomEntryCostDoes = safeInt(room.entryCostDoes || room.stakeDoes);
        const roomRewardAmountDoes = safeInt(room.rewardAmountDoes || 0);
        const playerUids = Array.from({ length: 2 }, (_, idx) => String((room.playerUids || [])[idx] || ""));
        const waitingDeadlineMs = resolveMorpionWaitingDeadlineMs(room, nowMs);
        const humans = playerUids.filter(Boolean).length;

        if (
          status === "waiting"
          && !getBlockedRejoinSet(room).has(uid)
          && roomEntryCostDoes === stakeDoes
          && roomRewardAmountDoes === rewardAmountDoes
        ) {
          const currentSeats = room.seats && typeof room.seats === "object" ? room.seats : {};
          const usedSeats = new Set(
            Object.values(currentSeats)
              .map((seat) => Number(seat))
              .filter((seat) => Number.isFinite(seat) && seat >= 0 && seat < 2)
          );
          const seatIndex = [0, 1].find((seat) => !usedSeats.has(seat));
          if (typeof seatIndex === "number" && humans < 2) {
            const walletMutation = stakeDoes > 0
              ? await applyWalletMutationTx(tx, {
                uid,
                email,
                type: "game_entry",
                note: "Participation morpion 5",
                amountDoes: stakeDoes,
                amountGourdes: 0,
                deltaDoes: -stakeDoes,
                deltaExchangedGourdes: 0,
              })
              : null;

            const nextPlayerUids = playerUids.slice();
            nextPlayerUids[seatIndex] = uid;
            const currentNames = Array.from({ length: 2 }, (_, idx) => String((room.playerNames || [])[idx] || ""));
            const nextPlayerNames = currentNames.slice();
            nextPlayerNames[seatIndex] = sanitizePlayerLabel(email || uid, seatIndex);
            const nextSeats = { ...currentSeats, [uid]: seatIndex };
            const nextHumans = nextPlayerUids.filter(Boolean).length;
            const nextPresence = room.roomPresenceMs && typeof room.roomPresenceMs === "object" ? { ...room.roomPresenceMs } : {};
            nextPresence[uid] = nowMs;
            const currentEntryFunding = room.entryFundingByUid && typeof room.entryFundingByUid === "object"
              ? { ...room.entryFundingByUid }
              : {};
            currentEntryFunding[uid] = walletMutation
              ? {
                approvedDoes: safeInt(walletMutation.gameEntryFunding?.approvedDoes),
                provisionalDoes: safeInt(walletMutation.gameEntryFunding?.provisionalDoes),
                welcomeDoes: safeInt(walletMutation.gameEntryFunding?.welcomeDoes),
                provisionalSources: normalizeFundingSources(walletMutation.gameEntryFunding?.provisionalSources),
              }
              : buildZeroMorpionEntryFunding();

            if (nextHumans >= 2) {
              clearMorpionMatchmakingPool(tx, poolRef);
              nextPlayerUids
                .map((playerUid) => String(playerUid || "").trim())
                .filter(Boolean)
                .forEach((playerUid) => {
                  tx.set(morpionWaitingRequestRef(playerUid), {
                    uid: playerUid,
                    roomId: openRoomRef.id,
                    stakeDoes,
                    status: "matched",
                    updatedAtMs: nowMs,
                    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
                  }, { merge: true });
                });
              logMorpionServerDebug("joinMatchmaking:matchedHuman", {
                roomId: openRoomRef.id,
                uid,
                seatIndex,
                stakeDoes,
                nextHumans,
              });
              return {
                ok: true,
                resumed: false,
                charged: stakeDoes > 0,
                roomId: openRoomRef.id,
                seatIndex,
                does: walletMutation ? walletMutation.afterDoes : safeInt(walletData.doesBalance),
                ...buildStartedMorpionRoomTransaction(tx, openRoomRef, {
                  ...room,
                  playerUids: nextPlayerUids,
                  playerNames: nextPlayerNames,
                  seats: nextSeats,
                  entryFundingByUid: currentEntryFunding,
                  roomPresenceMs: nextPresence,
                  humanCount: nextHumans,
                  botCount: 0,
                  waitingDeadlineMs,
                }, { nowMs }),
              };
            }
          }
        }
      }
    }

    const walletMutation = stakeDoes > 0
      ? await applyWalletMutationTx(tx, {
        uid,
        email,
        type: "game_entry",
        note: "Participation morpion 5",
        amountDoes: stakeDoes,
        amountGourdes: 0,
        deltaDoes: -stakeDoes,
        deltaExchangedGourdes: 0,
      })
      : null;

    const newRoomRef = morpionRoomRef();
    tx.set(newRoomRef, {
      status: "waiting",
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      createdAtMs: nowMs,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      ownerUid: uid,
      roomMode: "morpion_2p",
      gameMode: "morpion-5",
      engineVersion: 1,
      playerUids: [uid, ""],
      playerNames: [sanitizePlayerLabel(email || uid, 0), ""],
      entryFundingByUid: {
        [uid]: walletMutation
          ? {
            approvedDoes: safeInt(walletMutation.gameEntryFunding?.approvedDoes),
            provisionalDoes: safeInt(walletMutation.gameEntryFunding?.provisionalDoes),
            welcomeDoes: safeInt(walletMutation.gameEntryFunding?.welcomeDoes),
            provisionalSources: normalizeFundingSources(walletMutation.gameEntryFunding?.provisionalSources),
          }
          : buildZeroMorpionEntryFunding(),
      },
      blockedRejoinUids: [],
      humanCount: 1,
      seats: { [uid]: 0 },
      roomPresenceMs: { [uid]: nowMs },
      botCount: 0,
      startRevealPending: false,
      startRevealAckUids: [],
      waitingDeadlineMs: nowMs + ROOM_WAIT_MS,
      startedAt: null,
      startedAtMs: 0,
      endedAtMs: 0,
      turnLockedUntilMs: 0,
      nextActionSeq: 0,
      playedCount: 0,
      symbolBySeat: ["X", "O"],
      stakeDoes,
      entryCostDoes: stakeDoes,
      rewardAmountDoes,
      stakeConfigId: selectedStakeConfig.id,
    });
    tx.set(morpionWaitingRequestRef(uid), {
      uid,
      roomId: newRoomRef.id,
      stakeDoes,
      status: "pending",
      createdAtMs: nowMs,
      lastAttemptAtMs: nowMs,
      lastSeenMs: nowMs,
      updatedAtMs: nowMs,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
    setMorpionMatchmakingPoolOpen(tx, poolRef, newRoomRef.id, selectedStakeConfig.id, stakeDoes);
    logMorpionServerDebug("joinMatchmaking:createdWaitingRoom", {
      roomId: newRoomRef.id,
      uid,
      stakeDoes,
      waitingDeadlineMs: nowMs + ROOM_WAIT_MS,
    });

    return {
      ok: true,
      resumed: false,
      charged: stakeDoes > 0,
      roomId: newRoomRef.id,
      seatIndex: 0,
      status: "waiting",
      does: walletMutation ? walletMutation.afterDoes : safeInt(walletData.doesBalance),
      waitingDeadlineMs: nowMs + ROOM_WAIT_MS,
      humanCount: 1,
      botCount: 0,
    };
  });

  if (created?.status === "playing" && created?.startRevealPending !== true) {
    await processPendingBotTurnsMorpion(String(created.roomId || ""));
  }

  return created;
}, { invoker: "public" });

exports.ensureRoomReadyMorpion = publicOnCall("ensureRoomReadyMorpion", async (request) => {
  const { uid } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const roomId = String(payload.roomId || "").trim();
  if (!roomId) {
    throw new HttpsError("invalid-argument", "roomId requis.");
  }

  const roomRefDoc = morpionRoomRef(roomId);
  const startResult = await db.runTransaction(async (tx) => {
    const roomSnap = await tx.get(roomRefDoc);
    if (!roomSnap.exists) {
      throw new HttpsError("not-found", "Salle morpion introuvable.");
    }

    const room = roomSnap.data() || {};
    const seatIndex = getSeatForUser(room, uid);
    if (seatIndex < 0) {
      throw new HttpsError("permission-denied", "Tu ne fais pas partie de ce morpion.");
    }

    const status = String(room.status || "");
    if (status !== "waiting") {
      logMorpionServerDebug("ensureRoomReady:skipStatus", {
        roomId,
        uid,
        status,
        startRevealPending: room.startRevealPending === true,
      });
      return {
        ok: true,
        started: false,
        status,
        startRevealPending: room.startRevealPending === true,
        waitingDeadlineMs: safeSignedInt(room.waitingDeadlineMs),
        humanCount: safeInt(room.humanCount),
        botCount: safeInt(room.botCount),
      };
    }

    const nowMs = Date.now();
    const humans = Array.isArray(room.playerUids) ? room.playerUids.filter(Boolean).length : safeInt(room.humanCount);
    const waitingDeadlineMs = resolveMorpionWaitingDeadlineMs(room, nowMs);
    if (safeSignedInt(room.waitingDeadlineMs) !== waitingDeadlineMs) {
      tx.update(roomRefDoc, { waitingDeadlineMs, updatedAt: admin.firestore.FieldValue.serverTimestamp() });
    }

    if (humans < 2) {
      logMorpionServerDebug("ensureRoomReady:stillWaiting", {
        roomId,
        uid,
        humans,
        waitingDeadlineMs,
        nowMs,
      });
      return {
        ok: true,
        started: false,
        status: "waiting",
        startRevealPending: false,
        waitingDeadlineMs,
        humanCount: humans,
        botCount: 0,
      };
    }

    clearMorpionMatchmakingPool(tx, morpionMatchmakingPoolRef(String(room.stakeConfigId || ""), safeInt(room.entryCostDoes || room.stakeDoes)));
    (Array.isArray(room.playerUids) ? room.playerUids : [])
      .map((playerUid) => String(playerUid || "").trim())
      .filter(Boolean)
      .forEach((playerUid) => {
        tx.set(morpionWaitingRequestRef(playerUid), {
          uid: playerUid,
          roomId,
          stakeDoes: safeInt(room.entryCostDoes || room.stakeDoes),
          status: "matched",
          updatedAtMs: nowMs,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        }, { merge: true });
      });
    logMorpionServerDebug("ensureRoomReady:startingRoom", {
      roomId,
      uid,
      humans,
      waitingDeadlineMs,
      nowMs,
    });
    return buildStartedMorpionRoomTransaction(tx, roomRefDoc, {
      ...room,
      humanCount: humans,
      botCount: 0,
      waitingDeadlineMs,
    }, { nowMs });
  });

  if (startResult?.status === "playing" && startResult?.startRevealPending !== true) {
    await processPendingBotTurnsMorpion(roomId);
  }

  return startResult;
}, { invoker: "public" });

exports.touchRoomPresenceMorpion = publicOnCall("touchRoomPresenceMorpion", async (request) => {
  const { uid } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const roomId = String(payload.roomId || "").trim();
  if (!roomId) {
    throw new HttpsError("invalid-argument", "roomId requis.");
  }

  const roomRefDoc = morpionRoomRef(roomId);
  let shouldNudgeBots = false;
  let shouldResolveExpiredHumanTurn = false;
  const result = await db.runTransaction(async (tx) => {
    const roomSnap = await tx.get(roomRefDoc);
    if (!roomSnap.exists) {
      throw new HttpsError("not-found", "Salle morpion introuvable.");
    }

    const room = roomSnap.data() || {};
    const seatIndex = getSeatForUser(room, uid);
    if (seatIndex < 0) {
      throw new HttpsError("permission-denied", "Tu ne fais pas partie de cette salle morpion.");
    }

    const nowMs = Date.now();
    const presenceResult = buildMorpionPresenceUpdates(room, uid, nowMs);
    shouldNudgeBots = presenceResult.shouldNudgeBots === true;
    shouldResolveExpiredHumanTurn = presenceResult.shouldResolveExpiredHumanTurn === true;
    tx.update(roomRefDoc, presenceResult.updates);
    tx.set(morpionWaitingRequestRef(uid), {
      uid,
      roomId,
      stakeDoes: safeInt(room.entryCostDoes || room.stakeDoes),
      status: String(room.status || "") === "waiting" ? "pending" : "matched",
      lastSeenMs: nowMs,
      updatedAtMs: nowMs,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });

    logMorpionServerDebug("touchPresence", {
      roomId,
      uid,
      status: String(room.status || ""),
      currentPlayer: Number.isFinite(Number(room.currentPlayer)) ? Math.trunc(Number(room.currentPlayer)) : -1,
      shouldNudgeBots,
      shouldResolveExpiredHumanTurn,
      startRevealPending: room.startRevealPending === true,
      turnLockedUntilMs: safeSignedInt(room.turnLockedUntilMs),
      turnDeadlineMs: safeSignedInt(room.turnDeadlineMs),
    });

    return {
      ok: true,
      roomId: roomRefDoc.id,
      status: String(room.status || ""),
      currentPlayer: Number.isFinite(Number(room.currentPlayer)) ? Math.trunc(Number(room.currentPlayer)) : -1,
      humanCount: safeInt(presenceResult.updates.humanCount, safeInt(room.humanCount)),
      botCount: safeInt(presenceResult.updates.botCount, safeInt(room.botCount)),
    };
  });

  if (result?.status === "playing") {
    await processPendingBotTurnsMorpion(roomId);
  }

  return result;
}, { invoker: "public" });

exports.ackRoomStartSeenMorpion = publicOnCall("ackRoomStartSeenMorpion", async (request) => {
  const { uid } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const roomId = String(payload.roomId || "").trim();
  if (!roomId) {
    throw new HttpsError("invalid-argument", "roomId requis.");
  }

  const roomRefDoc = morpionRoomRef(roomId);
  const ackResult = await db.runTransaction(async (tx) => {
    const roomSnap = await tx.get(roomRefDoc);
    if (!roomSnap.exists) {
      throw new HttpsError("not-found", "Salle morpion introuvable.");
    }

    const room = roomSnap.data() || {};
    const humanUids = Array.isArray(room.playerUids) ? room.playerUids.map((item) => String(item || "").trim()).filter(Boolean) : [];
    const ackUids = Array.isArray(room.startRevealAckUids) ? room.startRevealAckUids.map((item) => String(item || "").trim()).filter(Boolean) : [];
    const ackSet = new Set(ackUids);

    if (String(room.status || "") !== "playing") {
      return { ok: true, pending: false, released: false, humanCount: humanUids.length, ackCount: ackSet.size };
    }
    const seatIndex = getSeatForUser(room, uid);
    if (seatIndex < 0) {
      throw new HttpsError("permission-denied", "Tu ne fais pas partie de cette salle morpion.");
    }

    ackSet.add(uid);
    if (room.startRevealPending !== true) {
      return { ok: true, pending: false, released: false, humanCount: humanUids.length, ackCount: ackSet.size };
    }

    const nextAckUids = Array.from(ackSet);
    const ready = humanUids.length > 0 && humanUids.every((humanUid) => ackSet.has(humanUid));
    tx.update(roomRefDoc, {
      startRevealAckUids: nextAckUids,
      startRevealPending: ready ? false : true,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    });

    return { ok: true, pending: !ready, released: ready, humanCount: humanUids.length, ackCount: nextAckUids.length };
  });

  if (ackResult?.released === true) {
    await processPendingBotTurnsMorpion(roomId);
  }

  return ackResult;
}, { invoker: "public" });

exports.leaveRoomMorpion = publicOnCall("leaveRoomMorpion", async (request) => {
  const { uid, email } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const roomId = String(payload.roomId || "").trim();
  if (!roomId) {
    throw new HttpsError("invalid-argument", "roomId requis.");
  }

  const roomRefDoc = morpionRoomRef(roomId);
  const outcome = await db.runTransaction(async (tx) => {
    const roomSnap = await tx.get(roomRefDoc);
    if (!roomSnap.exists) {
      return {
        result: { ok: true, deleted: true, status: "missing" },
        shouldCleanup: false,
        shouldNudgeBots: false,
      };
    }
    const roomData = roomSnap.data() || {};
    const result = await applyMorpionLeaveForUidTx(tx, roomRefDoc, roomData, uid, email);
    tx.set(morpionWaitingRequestRef(uid), {
      uid,
      roomId,
      status: "closed",
      updatedAtMs: Date.now(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
    return result;
  });

  if (outcome?.shouldNudgeBots) {
    await processPendingBotTurnsMorpion(roomId);
  }
  if (!outcome?.shouldCleanup) {
    return outcome?.result || { ok: true, deleted: false, status: "left" };
  }

  await cleanupMorpionRoom(roomRefDoc);
  return { ok: true, deleted: true, status: "deleted" };
}, { invoker: "public" });

exports.getMyActiveMorpionInvite = publicOnCall("getMyActiveMorpionInvite", async (request) => {
  const { uid } = assertAuth(request);
  const nowMs = Date.now();
  const inviteSnap = await db.collection(MORPION_PLAY_INVITATIONS_COLLECTION)
    .where("targetUid", "==", uid)
    .limit(12)
    .get();

  if (inviteSnap.empty) {
    return { ok: true, invitation: null };
  }

  const activeDoc = inviteSnap.docs
    .filter((docSnap) => String(docSnap.data()?.status || "").trim().toLowerCase() === "pending")
    .sort((left, right) => safeSignedInt(right.data()?.createdAtMs) - safeSignedInt(left.data()?.createdAtMs))
    .find((docSnap) => safeSignedInt(docSnap.data()?.expiresAtMs) > nowMs);
  if (!activeDoc) {
    return { ok: true, invitation: null };
  }

  const data = activeDoc.data() || {};
  return {
    ok: true,
    invitation: {
      invitationId: activeDoc.id,
      createdAtMs: safeSignedInt(data.createdAtMs),
      expiresAtMs: safeSignedInt(data.expiresAtMs),
      stakeDoes: safeInt(data.stakeDoes),
      gameLabel: String(data.gameLabel || "domino"),
      message: String(data.message || "Il y a actuellement des joueurs disponibles. Veux-tu jouer maintenant ?"),
    },
  };
}, { invoker: "public" });

exports.respondMorpionPlayInvite = publicOnCall("respondMorpionPlayInvite", async (request) => {
  const { uid } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const invitationId = String(payload.invitationId || "").trim();
  const action = String(payload.action || "").trim().toLowerCase();
  if (!invitationId || !["accept", "refuse"].includes(action)) {
    throw new HttpsError("invalid-argument", "invitationId et action requis.");
  }

  const invitationRef = morpionPlayInvitationRef(invitationId);
  const nowMs = Date.now();
  return db.runTransaction(async (tx) => {
    const invitationSnap = await tx.get(invitationRef);
    if (!invitationSnap.exists) {
      throw new HttpsError("not-found", "Invitation introuvable.");
    }
    const invitation = invitationSnap.data() || {};
    if (String(invitation.targetUid || "").trim() !== uid) {
      throw new HttpsError("permission-denied", "Invitation invalide.");
    }

    const currentStatus = String(invitation.status || "").trim().toLowerCase();
    const expiresAtMs = safeSignedInt(invitation.expiresAtMs);
    if (currentStatus !== "pending") {
      return { ok: true, status: currentStatus };
    }
    if (expiresAtMs > 0 && expiresAtMs <= nowMs) {
      tx.update(invitationRef, {
        status: "expired",
        respondedAtMs: nowMs,
        updatedAtMs: nowMs,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      });
      return { ok: true, status: "expired" };
    }

    const nextStatus = action === "accept" ? "accepted" : "refused";
    tx.update(invitationRef, {
      status: nextStatus,
      respondedAtMs: nowMs,
      updatedAtMs: nowMs,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    });
    tx.set(morpionWaitingRequestRef(uid), {
      uid,
      status: nextStatus === "accepted" ? "accepted_invite" : "pending",
      lastInviteResponseAtMs: nowMs,
      updatedAtMs: nowMs,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
    return { ok: true, status: nextStatus };
  });
}, { invoker: "public" });

exports.getMorpionWaitingQueueDashboard = publicOnCall("getMorpionWaitingQueueDashboard", async (request) => {
  assertFinanceAdmin(request);
  const nowMs = Date.now();
  const queueSnap = await db.collection(MORPION_WAITING_REQUESTS_COLLECTION)
    .orderBy("updatedAtMs", "desc")
    .limit(MORPION_WAITING_QUEUE_FETCH_LIMIT)
    .get();

  const rows = [];
  queueSnap.forEach((docSnap) => {
    const data = docSnap.data() || {};
    const uid = String(data.uid || docSnap.id || "").trim();
    if (!uid) return;
    const status = String(data.status || "pending").trim().toLowerCase();
    if (!["pending", "accepted_invite"].includes(status)) return;

    const online = isMorpionWaitingRequestOnline(data, nowMs);
    const lastInviteAtMs = safeSignedInt(data.lastInviteAtMs);
    const canInvite = online && (lastInviteAtMs <= 0 || (nowMs - lastInviteAtMs) >= MORPION_INVITATION_TTL_MS);

    rows.push({
      uid,
      roomId: String(data.roomId || "").trim(),
      stakeDoes: safeInt(data.stakeDoes),
      status,
      online,
      canInvite,
      createdAtMs: safeSignedInt(data.createdAtMs),
      updatedAtMs: safeSignedInt(data.updatedAtMs),
      lastAttemptAtMs: safeSignedInt(data.lastAttemptAtMs),
      lastSeenMs: safeSignedInt(data.lastSeenMs),
      lastInviteAtMs,
      lastInviteResponseAtMs: safeSignedInt(data.lastInviteResponseAtMs),
      lastInvitationId: String(data.lastInvitationId || "").trim(),
    });
  });

  rows.sort((a, b) => {
    if (a.online !== b.online) return a.online ? -1 : 1;
    return safeSignedInt(b.updatedAtMs) - safeSignedInt(a.updatedAtMs);
  });

  return {
    ok: true,
    nowMs,
    onlineWindowMs: MORPION_WAITING_ONLINE_WINDOW_MS,
    invitationTtlMs: MORPION_INVITATION_TTL_MS,
    rows,
  };
}, { invoker: "public" });

exports.getMorpionLiveMatchmakingSignal = publicOnCall("getMorpionLiveMatchmakingSignal", async (request) => {
  const { uid } = assertAuth(request);
  const nowMs = Date.now();
  const queueSnap = await db.collection(MORPION_WAITING_REQUESTS_COLLECTION)
    .where("status", "in", ["pending", "accepted_invite"])
    .limit(60)
    .get();

  const rows = [];
  queueSnap.forEach((docSnap) => {
    const data = docSnap.data() || {};
    if (!isMorpionWaitingRequestOnline(data, nowMs)) return;
    const rowUid = String(data.uid || docSnap.id || "").trim();
    if (!rowUid) return;
    rows.push({
      uid: rowUid,
      updatedAtMs: safeSignedInt(data.updatedAtMs),
      lastSeenMs: safeSignedInt(data.lastSeenMs),
      lastAttemptAtMs: safeSignedInt(data.lastAttemptAtMs),
      createdAtMs: safeSignedInt(data.createdAtMs),
    });
  });

  rows.sort((left, right) => (
    Math.max(
      safeSignedInt(right.updatedAtMs),
      safeSignedInt(right.lastSeenMs),
      safeSignedInt(right.lastAttemptAtMs),
      safeSignedInt(right.createdAtMs)
    ) - Math.max(
      safeSignedInt(left.updatedAtMs),
      safeSignedInt(left.lastSeenMs),
      safeSignedInt(left.lastAttemptAtMs),
      safeSignedInt(left.createdAtMs)
    )
  ));

  const activeOthers = rows.filter((row) => row.uid !== uid);
  const newest = activeOthers[0] || null;
  const signalTsMs = newest
    ? Math.max(
      safeSignedInt(newest.updatedAtMs),
      safeSignedInt(newest.lastSeenMs),
      safeSignedInt(newest.lastAttemptAtMs),
      safeSignedInt(newest.createdAtMs)
    )
    : 0;

  return {
    ok: true,
    active: activeOthers.length > 0,
    activeCount: activeOthers.length,
    signalTsMs,
    message: activeOthers.length > 0
      ? "Des joueurs sont disponibles sur Morpion."
      : "",
  };
}, { invoker: "public" });

exports.getMorpionMatchmakingHint = publicOnCall("getMorpionMatchmakingHint", async (request) => {
  assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const roomId = String(payload.roomId || "").trim();

  const roomsSnap = await db.collection(MORPION_ROOMS_COLLECTION)
    .where("status", "==", "playing")
    .limit(220)
    .get();

  let activePlayingHumans = 0;
  let activePlayingRooms = 0;
  roomsSnap.forEach((docSnap) => {
    const data = docSnap.data() || {};
    const thisRoomId = String(docSnap.id || "").trim();
    if (roomId && thisRoomId === roomId) return;
    const humans = Math.max(0, Math.min(2, safeInt(data.humanCount)));
    if (humans <= 0) return;
    activePlayingHumans += humans;
    activePlayingRooms += 1;
  });

  const hasOddActivePlayingHumans = activePlayingHumans > 0 && (activePlayingHumans % 2) === 1;
  return {
    ok: true,
    activePlayingHumans,
    activePlayingRooms,
    hasOddActivePlayingHumans,
    message: hasOddActivePlayingHumans
      ? "Il y a des joueurs qui jouent en ce moment, mais leur nombre est impair. Reste en attente prolongee pour jouer avec un joueur qui terminera bientot."
      : "",
    checkedAtMs: Date.now(),
  };
}, { invoker: "public" });

exports.getMyMorpionWhatsappPreferenceSecure = publicOnCall("getMyMorpionWhatsappPreferenceSecure", async (request) => {
  const { uid } = assertAuth(request);
  const clientSnap = await walletRef(uid).get();
  const clientData = clientSnap.exists ? (clientSnap.data() || {}) : {};
  const contact = buildMorpionWhatsappContactRecord(clientSnap, Date.now());
  return {
    ok: true,
    contact,
    hasSavedNumber: isValidWhatsappDigits(clientData.morpionWhatsappDigits || clientData.morpionWhatsappNumber || ""),
    visibleInRecentList: clientData.morpionWhatsappVisible === true,
  };
}, { invoker: "public" });

exports.saveMorpionWhatsappPreferenceSecure = publicOnCall("saveMorpionWhatsappPreferenceSecure", async (request) => {
  const { uid, email } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const rawWhatsappNumber = sanitizePhone(payload.whatsappNumber || payload.phone || "", 40);
  const whatsappDigits = normalizeWhatsappDigits(rawWhatsappNumber);
  if (!isValidWhatsappDigits(whatsappDigits)) {
    throw new HttpsError("invalid-argument", "Numero WhatsApp invalide.");
  }

  const nowMs = Date.now();
  await walletRef(uid).set({
    uid,
    email: sanitizeEmail(email || "", 160),
    morpionWhatsappNumber: formatWhatsappDisplayNumber(whatsappDigits),
    morpionWhatsappDigits: whatsappDigits,
    morpionWhatsappVisible: true,
    morpionWhatsappConsentAtMs: nowMs,
    morpionWhatsappUpdatedAtMs: nowMs,
    morpionLastInterestAtMs: nowMs,
    lastSeenAt: admin.firestore.FieldValue.serverTimestamp(),
    lastSeenAtMs: nowMs,
    sitePresencePage: "morpion",
    sitePresenceExpiresAtMs: nowMs + CLIENT_SITE_PRESENCE_WINDOW_MS,
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  }, { merge: true });

  const savedSnap = await walletRef(uid).get();
  return {
    ok: true,
    contact: buildMorpionWhatsappContactRecord(savedSnap, nowMs),
  };
}, { invoker: "public" });

exports.removeMorpionWhatsappPreferenceSecure = publicOnCall("removeMorpionWhatsappPreferenceSecure", async (request) => {
  const { uid, email } = assertAuth(request);
  const nowMs = Date.now();
  await walletRef(uid).set({
    uid,
    email: sanitizeEmail(email || "", 160),
    morpionWhatsappVisible: false,
    morpionWhatsappRemovedAtMs: nowMs,
    morpionWhatsappNumber: admin.firestore.FieldValue.delete(),
    morpionWhatsappDigits: admin.firestore.FieldValue.delete(),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    lastSeenAt: admin.firestore.FieldValue.serverTimestamp(),
    lastSeenAtMs: nowMs,
    sitePresenceExpiresAtMs: nowMs + CLIENT_SITE_PRESENCE_WINDOW_MS,
  }, { merge: true });

  return {
    ok: true,
    removed: true,
  };
}, { invoker: "public" });

exports.listRecentMorpionWhatsappContactsSecure = publicOnCall("listRecentMorpionWhatsappContactsSecure", async (request) => {
  const { uid } = assertAuth(request);
  const nowMs = Date.now();
  const cutoffMs = nowMs - MORPION_WHATSAPP_RECENT_WINDOW_MS;
  const contactsSnap = await db.collection(CLIENTS_COLLECTION)
    .where("morpionWhatsappVisible", "==", true)
    .limit(180)
    .get();

  const contacts = [];
  contactsSnap.forEach((docSnap) => {
    const data = docSnap.data() || {};
    if (String(docSnap.id || "").trim() === uid) return;
    const lastInterestAtMs = safeSignedInt(data.morpionLastInterestAtMs);
    if (lastInterestAtMs <= 0 || lastInterestAtMs < cutoffMs) return;
    const record = buildMorpionWhatsappContactRecord(docSnap, nowMs);
    if (!record) return;
    contacts.push(record);
  });

  contacts.sort((left, right) => {
    if (left.online !== right.online) return left.online ? -1 : 1;
    return safeSignedInt(right.lastInterestAtMs) - safeSignedInt(left.lastInterestAtMs)
      || safeSignedInt(right.lastSeenAtMs) - safeSignedInt(left.lastSeenAtMs);
  });

  return {
    ok: true,
    nowMs,
    recentWindowMs: MORPION_WHATSAPP_RECENT_WINDOW_MS,
    contacts: contacts.slice(0, MORPION_WHATSAPP_LIST_LIMIT),
  };
}, { invoker: "public" });

exports.inviteMorpionWaitingPlayer = publicOnCall("inviteMorpionWaitingPlayer", async (request) => {
  const { uid: adminUid, email: adminEmail } = assertFinanceAdmin(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const targetUid = String(payload.targetUid || "").trim();
  if (!targetUid) {
    throw new HttpsError("invalid-argument", "targetUid requis.");
  }

  const nowMs = Date.now();
  return db.runTransaction(async (tx) => {
    const waitingRef = morpionWaitingRequestRef(targetUid);
    const waitingSnap = await tx.get(waitingRef);
    if (!waitingSnap.exists) {
      throw new HttpsError("not-found", "Ce joueur n'est plus en attente.");
    }

    const waitingData = waitingSnap.data() || {};
    const status = String(waitingData.status || "pending").trim().toLowerCase();
    if (!["pending", "accepted_invite"].includes(status)) {
      throw new HttpsError("failed-precondition", "Le joueur n'est plus disponible.");
    }
    if (!isMorpionWaitingRequestOnline(waitingData, nowMs)) {
      throw new HttpsError("failed-precondition", "Le joueur est hors ligne.");
    }

    const lastInviteAtMs = safeSignedInt(waitingData.lastInviteAtMs);
    if (lastInviteAtMs > 0 && (nowMs - lastInviteAtMs) < MORPION_INVITATION_TTL_MS) {
      throw new HttpsError("failed-precondition", "Une invitation est deja active.");
    }

    const inviteRef = morpionPlayInvitationRef();
    const lastSeenMs = safeSignedInt(waitingData.lastSeenMs);
    const onlineExpiryCapMs = lastSeenMs > 0 ? (lastSeenMs + MORPION_WAITING_ONLINE_WINDOW_MS) : 0;
    const fallbackExpiryMs = nowMs + MORPION_INVITATION_TTL_MS;
    const expiresAtMs = Math.max(
      nowMs + 20_000,
      Math.min(fallbackExpiryMs, onlineExpiryCapMs > 0 ? onlineExpiryCapMs : fallbackExpiryMs)
    );
    const stakeDoes = 500;

    tx.set(inviteRef, {
      invitationId: inviteRef.id,
      targetUid,
      status: "pending",
      gameLabel: "domino",
      stakeDoes,
      message: "Il y a actuellement des joueurs disponibles sur Domino. Veux-tu jouer maintenant ?",
      createdAtMs: nowMs,
      expiresAtMs,
      createdByUid: adminUid,
      createdByEmail: String(adminEmail || ""),
      updatedAtMs: nowMs,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });

    tx.set(waitingRef, {
      uid: targetUid,
      status: "pending",
      lastInviteAtMs: nowMs,
      lastInvitationId: inviteRef.id,
      updatedAtMs: nowMs,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });

    return {
      ok: true,
      invitationId: inviteRef.id,
      targetUid,
      expiresAtMs,
      stakeDoes,
    };
  });
}, { invoker: "public" });

exports.submitActionMorpion = publicOnCall("submitActionMorpion", async (request) => {
  const { uid } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const roomId = String(payload.roomId || "").trim();
  const clientActionId = sanitizeText(payload.clientActionId || "", 80);
  const action = payload.action && typeof payload.action === "object" ? payload.action : null;

  if (!roomId || !action) {
    throw new HttpsError("invalid-argument", "roomId et action morpion sont requis.");
  }
  if (!clientActionId) {
    throw new HttpsError("invalid-argument", "clientActionId requis.");
  }

  const cellIndex = safeInt(action.cellIndex, -1);
  if (cellIndex < 0 || cellIndex >= 225) {
    throw new HttpsError("invalid-argument", "Case morpion invalide.");
  }

  const roomRefDoc = morpionRoomRef(roomId);
  const stateRef = morpionGameStateRef(roomId);
  const result = await db.runTransaction(async (tx) => {
    const [roomSnap, stateSnap] = await Promise.all([
      tx.get(roomRefDoc),
      tx.get(stateRef),
    ]);
    if (!roomSnap.exists) {
      throw new HttpsError("not-found", "Salle morpion introuvable.");
    }

    const room = roomSnap.data() || {};
    if (room.status !== "playing") {
      throw new HttpsError("failed-precondition", "Le morpion n'est pas en cours.");
    }
    if (room.startRevealPending === true) {
      throw new HttpsError("failed-precondition", "Le morpion se synchronise encore.");
    }
    const localSeat = getSeatForUser(room, uid);
    if (localSeat < 0) {
      throw new HttpsError("permission-denied", "Tu ne fais pas partie de ce morpion.");
    }
    if (typeof room.currentPlayer === "number" && room.currentPlayer !== localSeat) {
      throw new HttpsError("failed-precondition", `Hors tour morpion. Joueur attendu: ${room.currentPlayer + 1}`);
    }

    const currentState = stateSnap.exists ? normalizeMorpionGameState(stateSnap.data(), room) : createInitialMorpionGameState(room);
    if (currentState.endedReason) {
      throw new HttpsError("failed-precondition", "Le morpion est deja termine.");
    }
    if (safeSignedInt(currentState.currentPlayer, -1) !== localSeat) {
      throw new HttpsError("failed-precondition", `Hors tour morpion. Joueur attendu: ${currentState.currentPlayer + 1}`);
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

    const applied = applyMorpionMove(currentState, room, { seat: localSeat, cellIndex }, uid);
    applied.state.idempotencyKeys[clientActionId] = true;

    tx.set(stateRef, buildMorpionGameStateWrite(applied.state), { merge: true });
    const roomUpdate = buildMorpionRoomUpdateFromGameState(room, applied.state, [applied.record]);
    tx.update(roomRefDoc, roomUpdate);
    tx.set(roomRefDoc.collection("actions").doc(String(applied.record.seq)), {
      ...applied.record,
      roomId,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
    writeMorpionRoomResultIfEndedTx(tx, roomRefDoc, room, roomUpdate);

    return {
      ok: true,
      duplicate: false,
      seq: applied.record.seq,
      nextPlayer: applied.state.currentPlayer,
      status: applied.state.endedReason ? "ended" : "playing",
      winnerSeat: applied.state.winnerSeat,
      winnerUid: applied.state.winnerUid,
      endedReason: applied.state.endedReason,
      record: applied.record,
    };
  });

  if (result?.status === "playing" && typeof result.nextPlayer === "number") {
    await processPendingBotTurnsMorpion(roomId);
  }
  if (result?.status === "ended") {
    await recordMorpionBotOutcomeProfilesIfNeeded(roomId);
  }
  return result;
}, { invoker: "public" });

exports.claimWinRewardMorpion = publicOnCall("claimWinRewardMorpion", async (request) => {
  const { uid, email } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const roomId = String(payload.roomId || "").trim();
  if (!roomId) {
    throw new HttpsError("invalid-argument", "roomId requis.");
  }

  const roomRefDoc = morpionRoomRef(roomId);
  const settlementRef = roomRefDoc.collection("settlements").doc(uid);

  const result = await db.runTransaction(async (tx) => {
    const [roomSnap, settlementSnap, stateSnap] = await Promise.all([
      tx.get(roomRefDoc),
      tx.get(settlementRef),
      tx.get(morpionGameStateRef(roomId)),
    ]);
    if (!roomSnap.exists) {
      throw new HttpsError("not-found", "Salle morpion introuvable.");
    }

    const room = roomSnap.data() || {};
    const seat = getSeatForUser(room, uid);
    const state = stateSnap.exists ? normalizeMorpionGameState(stateSnap.data(), room) : null;
    const winnerSeat = typeof room.winnerSeat === "number"
      ? room.winnerSeat
      : (state && typeof state.winnerSeat === "number" ? state.winnerSeat : -1);
    const winnerUid = String(room.winnerUid || state?.winnerUid || "").trim();

    if (winnerUid) {
      if (winnerUid !== uid) {
        throw new HttpsError("permission-denied", "Ce compte n'est pas gagnant de ce morpion.");
      }
    } else if (seat < 0) {
      throw new HttpsError("permission-denied", "Ce compte ne fait pas partie de ce morpion.");
    } else if (winnerSeat < 0 || seat !== winnerSeat) {
      throw new HttpsError("permission-denied", "Ce compte n'est pas gagnant de ce morpion.");
    }

    const settlementData = settlementSnap.exists ? (settlementSnap.data() || {}) : {};
    if (settlementData.rewardPaid === true) {
      return {
        ok: true,
        rewardGranted: false,
        reason: "already_paid",
        rewardAmountDoes: safeInt(settlementData.rewardAmountDoes) || safeInt(room.rewardAmountDoes),
      };
    }

    const rewardAmountDoes = safeInt(room.rewardAmountDoes);
    if (rewardAmountDoes <= 0) {
      tx.set(settlementRef, {
        uid,
        roomId,
        rewardPaid: true,
        rewardAmountDoes: 0,
        reason: "no_reward",
        settledAt: admin.firestore.FieldValue.serverTimestamp(),
      }, { merge: true });
      return {
        ok: true,
        rewardGranted: false,
        reason: "no_reward",
        rewardAmountDoes: 0,
      };
    }

    const entryFundingRaw = room.entryFundingByUid && typeof room.entryFundingByUid === "object"
      ? (room.entryFundingByUid[uid] || null)
      : null;
    const provisionalSources = normalizeFundingSources(entryFundingRaw?.provisionalSources);
    const approvedEntryDoes = safeInt(entryFundingRaw?.approvedDoes);
    const provisionalEntryDoes = safeInt(entryFundingRaw?.provisionalDoes);
    const welcomeEntryDoes = safeInt(entryFundingRaw?.welcomeDoes);
    let approvedRewardDoes = rewardAmountDoes;
    let provisionalRewardDoes = 0;
    let welcomeRewardDoes = 0;

    if ((provisionalEntryDoes > 0 && provisionalSources.length > 0) || welcomeEntryDoes > 0) {
      const totalEntryDoes = Math.max(approvedEntryDoes + provisionalEntryDoes + welcomeEntryDoes, provisionalEntryDoes + welcomeEntryDoes);
      const provisionalRewardPool = Math.min(
        rewardAmountDoes,
        Math.round((rewardAmountDoes * provisionalEntryDoes) / Math.max(1, totalEntryDoes))
      );
      welcomeRewardDoes = Math.min(
        rewardAmountDoes - provisionalRewardPool,
        Math.round((rewardAmountDoes * welcomeEntryDoes) / Math.max(1, totalEntryDoes))
      );
      provisionalRewardDoes = provisionalRewardPool;
    } else if (approvedEntryDoes <= 0 && provisionalEntryDoes > 0) {
      provisionalRewardDoes = rewardAmountDoes;
    }

    ({
      approvedRewardDoes,
      provisionalRewardDoes,
      welcomeRewardDoes,
    } = normalizeRewardSettlementSplit({
      rewardAmountDoes,
      provisionalRewardDoes,
      welcomeRewardDoes,
    }));

    const walletMutation = await applyWalletMutationTx(tx, {
      uid,
      email,
      type: "game_reward",
      note: `Gain de morpion (${roomId})`,
      amountDoes: rewardAmountDoes,
      approvedRewardDoes,
      provisionalRewardDoes,
      welcomeRewardDoes,
      amountGourdes: 0,
      deltaDoes: rewardAmountDoes,
      deltaExchangedGourdes: 0,
    });
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
      stakeDoes: safeInt(room.entryCostDoes || room.stakeDoes),
      does: walletMutation.afterDoes,
    };
  });

  let agentCommissionDoes = 0;
  if (result?.rewardGranted === true) {
    try {
      const agentCommission = await awardAgentCommissionForClientWin({
        playerUid: uid,
        gameType: "morpion",
        roomId,
        stakeDoes: safeInt(result.stakeDoes),
        rewardDoes: safeInt(result.rewardAmountDoes),
        wonAtMs: Date.now(),
      });
      agentCommissionDoes = safeInt(agentCommission?.commissionDoes);
    } catch (error) {
      console.error("[AGENT_COMMISSION][claimWinRewardMorpion] skipped", {
        uid,
        roomId,
        error: error?.message || String(error || ""),
      });
    }
  }

  return {
    ...result,
    agentCommissionDoes,
  };
}, { invoker: "public" });

exports.getPublicDuelStakeOptionsSecure = publicOnCall("getPublicDuelStakeOptionsSecure", async () => {
  return {
    ok: true,
    options: DEFAULT_DUEL_STAKE_OPTIONS.map((item) => ({
      id: item.id,
      stakeDoes: item.stakeDoes,
      rewardDoes: item.rewardDoes,
      enabled: item.enabled !== false,
      sortOrder: item.sortOrder,
    })),
  };
}, { invoker: "public" });

exports.createFriendDuelRoom = publicOnCall("createFriendDuelRoom", async (request) => {
  const { uid, email } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const stakeDoes = safeInt(payload.stakeDoes);
  const selectedStakeConfig = getDuelStakeConfigByAmount(stakeDoes);
  const configuredBotDifficulty = await getConfiguredDuelBotDifficulty();

  if (!selectedStakeConfig) {
    throw new HttpsError("invalid-argument", "Mise duel non autorisee.");
  }

  const activeRoom = await findActiveDuelRoomForUser(uid);
  if (activeRoom) {
    throw new HttpsError("failed-precondition", "Tu participes deja a une salle duel active.", {
      code: "active-room-exists",
      roomId: activeRoom.roomId,
      status: String(activeRoom.status || ""),
      seatIndex: safeInt(activeRoom.seatIndex, 0),
      roomMode: String(activeRoom.roomMode || "duel_2p"),
      stakeDoes: safeInt(activeRoom.stakeDoes),
      inviteCode: String(activeRoom.inviteCode || "").trim(),
    });
  }

  const [inviteCode] = await Promise.all([
    generateUniqueFriendDuelInviteCode(),
  ]);
  const rewardAmountDoes = selectedStakeConfig.rewardDoes;
  const roomRefDoc = duelRoomRef();

  return db.runTransaction(async (tx) => {
    const walletSnap = await tx.get(walletRef(uid));
    const walletData = walletSnap.exists ? (walletSnap.data() || {}) : {};
    assertWalletNotFrozen(walletData);

    const nowMs = Date.now();
    const waitingDeadlineMs = nowMs + FRIEND_ROOM_WAIT_MS;

    tx.set(roomRefDoc, {
      status: "waiting",
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      createdAtMs: nowMs,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      ownerUid: uid,
      roomMode: "duel_friends",
      isPrivate: true,
      allowBots: false,
      inviteCode,
      inviteCodeNormalized: normalizeCode(inviteCode),
      requiredHumans: 2,
      gameMode: "domino-duel",
      engineVersion: 1,
      playerUids: [uid, ""],
      playerNames: [sanitizePlayerLabel(email || uid, 0), ""],
      entryFundingByUid: {},
      blockedRejoinUids: [],
      humanCount: 1,
      seats: { [uid]: 0 },
      roomPresenceMs: { [uid]: nowMs },
      botCount: 0,
      botDifficulty: configuredBotDifficulty,
      startRevealPending: false,
      startRevealAckUids: [],
      waitingDeadlineMs,
      startedAt: null,
      startedAtMs: 0,
      endedAtMs: 0,
      turnLockedUntilMs: 0,
      nextActionSeq: 0,
      playedCount: 0,
      stakeDoes,
      entryCostDoes: stakeDoes,
      rewardAmountDoes,
      stakeConfigId: selectedStakeConfig.id,
    });

    return {
      ok: true,
      roomId: roomRefDoc.id,
      seatIndex: 0,
      status: "waiting",
      roomMode: "duel_friends",
      charged: false,
      inviteCode,
      requiredHumans: 2,
      waitingDeadlineMs,
      stakeDoes,
      rewardAmountDoes,
      privateDeckOrder: [],
    };
  });
}, { invoker: "public" });

exports.joinFriendDuelRoomByCode = publicOnCall("joinFriendDuelRoomByCode", async (request) => {
  const { uid, email } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const inviteCodeNormalized = normalizeCode(payload.inviteCode || payload.code || "");
  const configuredBotDifficulty = await getConfiguredDuelBotDifficulty();

  if (!inviteCodeNormalized) {
    throw new HttpsError("invalid-argument", "Code de salle requis.");
  }

  const activeRoom = await findActiveDuelRoomForUser(uid);
  if (activeRoom) {
    throw new HttpsError("failed-precondition", "Tu participes deja a une salle duel active.", {
      code: "active-room-exists",
      roomId: activeRoom.roomId,
      status: String(activeRoom.status || ""),
      seatIndex: safeInt(activeRoom.seatIndex, 0),
      roomMode: String(activeRoom.roomMode || "duel_2p"),
      stakeDoes: safeInt(activeRoom.stakeDoes),
      inviteCode: String(activeRoom.inviteCode || "").trim(),
    });
  }

  const matchingSnap = await db
    .collection(DUEL_ROOMS_COLLECTION)
    .where("inviteCodeNormalized", "==", inviteCodeNormalized)
    .limit(6)
    .get();

  const roomDoc = matchingSnap.docs.find((docSnap) => isFriendDuelRoom(docSnap.data() || {})) || null;
  if (!roomDoc) {
    throw new HttpsError("not-found", "Code de duel introuvable.");
  }

  const roomRefDoc = roomDoc.ref;
  const result = await db.runTransaction(async (tx) => {
    const [roomSnap, walletSnap] = await Promise.all([
      tx.get(roomRefDoc),
      tx.get(walletRef(uid)),
    ]);

    if (!roomSnap.exists) {
      throw new HttpsError("not-found", "Salle duel introuvable.");
    }

    const room = roomSnap.data() || {};
    if (!isFriendDuelRoom(room)) {
      throw new HttpsError("failed-precondition", "Cette salle duel n'est pas disponible.");
    }

    const roomStatus = String(room.status || "");
    const nowMs = Date.now();
    const waitingDeadlineMs = resolveDuelWaitingDeadlineMs(room, nowMs);
    const playerUids = Array.from({ length: 2 }, (_, idx) => String((room.playerUids || [])[idx] || ""));
    const humans = playerUids.filter(Boolean).length;
    const roomStakeDoes = safeInt(room.entryCostDoes || room.stakeDoes);
    const roomRewardAmountDoes = safeInt(room.rewardAmountDoes);
    const roomInviteCode = String(room.inviteCode || inviteCodeNormalized || "").trim();

    const walletData = walletSnap.exists ? (walletSnap.data() || {}) : {};
    assertWalletNotFrozen(walletData);

    if (playerUids.includes(uid)) {
      const seatIndex = getSeatForUser(room, uid);
      const nextPresence = room.roomPresenceMs && typeof room.roomPresenceMs === "object"
        ? { ...room.roomPresenceMs }
        : {};
      nextPresence[uid] = nowMs;
      tx.update(roomRefDoc, {
        roomPresenceMs: nextPresence,
        waitingDeadlineMs,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      });
      return {
        ok: true,
        resumed: true,
        charged: false,
        roomId: roomRefDoc.id,
        seatIndex: seatIndex >= 0 ? seatIndex : 0,
        status: roomStatus,
        roomMode: "duel_friends",
        stakeDoes: roomStakeDoes,
        rewardAmountDoes: roomRewardAmountDoes,
        inviteCode: roomInviteCode,
        waitingDeadlineMs,
        privateDeckOrder: roomStatus === "playing" ? [] : [],
      };
    }

    if (roomStatus === "playing") {
      throw new HttpsError("failed-precondition", "Cette salle duel a deja demarre.");
    }
    if (roomStatus !== "waiting") {
      throw new HttpsError("failed-precondition", "Cette salle duel n'est plus disponible.");
    }
    if (getBlockedRejoinSet(room).has(uid)) {
      throw new HttpsError("permission-denied", "Tu ne peux plus rejoindre cette salle duel.");
    }

    const currentSeats = room.seats && typeof room.seats === "object" ? room.seats : {};
    const usedSeats = new Set(
      Object.values(currentSeats)
        .map((seat) => Number(seat))
        .filter((seat) => Number.isFinite(seat) && seat >= 0 && seat < 2)
    );
    const seatIndex = [0, 1].find((seat) => !usedSeats.has(seat));
    if (typeof seatIndex !== "number" || humans >= 2) {
      throw new HttpsError("failed-precondition", "Cette salle duel est complete.");
    }

    if (safeInt(walletData.doesBalance) < roomStakeDoes) {
      throw new HttpsError("failed-precondition", "Solde Does insuffisant.");
    }

    const nextPlayerUids = playerUids.slice();
    nextPlayerUids[seatIndex] = uid;
    const currentNames = Array.from({ length: 2 }, (_, idx) => String((room.playerNames || [])[idx] || ""));
    const nextPlayerNames = currentNames.slice();
    nextPlayerNames[seatIndex] = sanitizePlayerLabel(email || uid, seatIndex);
    const nextSeats = {
      ...currentSeats,
      [uid]: seatIndex,
    };
    const nextPresence = room.roomPresenceMs && typeof room.roomPresenceMs === "object"
      ? { ...room.roomPresenceMs }
      : {};
    nextPresence[uid] = nowMs;
    const nextHumans = nextPlayerUids.filter(Boolean).length;

    if (nextHumans >= 2) {
      const chargeResult = await chargeRoomEntriesTx(tx, room, nextPlayerUids, roomStakeDoes);
      tx.set(roomRefDoc, {
        playerUids: nextPlayerUids,
        playerNames: nextPlayerNames,
        seats: nextSeats,
        roomPresenceMs: nextPresence,
        humanCount: nextHumans,
        botCount: 0,
        entryFundingByUid: chargeResult.entryFundingByUid,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      }, { merge: true });

      return {
        ok: true,
        resumed: false,
        charged: true,
        roomId: roomRefDoc.id,
        seatIndex,
        does: safeInt(chargeResult.afterDoesByUid[uid]),
        roomMode: "duel_friends",
        inviteCode: roomInviteCode,
        stakeDoes: roomStakeDoes,
        rewardAmountDoes: roomRewardAmountDoes,
        ...buildStartedDuelRoomTransaction(tx, roomRefDoc, {
          ...room,
          playerUids: nextPlayerUids,
          playerNames: nextPlayerNames,
          seats: nextSeats,
          entryFundingByUid: chargeResult.entryFundingByUid,
          roomPresenceMs: nextPresence,
          humanCount: nextHumans,
          botCount: 0,
          waitingDeadlineMs,
        }, {
          configuredBotDifficulty,
          nowMs,
        }),
      };
    }

    tx.update(roomRefDoc, {
      playerUids: nextPlayerUids,
      playerNames: nextPlayerNames,
      seats: nextSeats,
      roomPresenceMs: nextPresence,
      humanCount: nextHumans,
      botCount: 0,
      waitingDeadlineMs,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    });

    return {
      ok: true,
      resumed: false,
      charged: false,
      roomId: roomRefDoc.id,
      seatIndex,
      status: "waiting",
      roomMode: "duel_friends",
      inviteCode: roomInviteCode,
      stakeDoes: roomStakeDoes,
      rewardAmountDoes: roomRewardAmountDoes,
      waitingDeadlineMs,
      privateDeckOrder: [],
    };
  });

  if (result?.status === "playing") {
    if (!Array.isArray(result.privateDeckOrder) || result.privateDeckOrder.length !== 28) {
      result.privateDeckOrder = await readPrivateDeckOrderForDuelRoom(roomRefDoc.id);
    }
  }

  if (result?.status === "playing" && result?.startRevealPending !== true) {
    await processPendingBotTurnsDuel(roomRefDoc.id);
  }

  return result;
}, { invoker: "public" });

exports.joinMatchmakingDuel = publicOnCall("joinMatchmakingDuel", async (request) => {
  const { uid, email } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const stakeDoes = safeInt(payload.stakeDoes);
  const excludedRoomIds = normalizeDuelExcludedRoomIds(payload.excludeRoomIds);
  const excludedRoomIdSet = new Set(excludedRoomIds);
  const selectedStakeConfig = getDuelStakeConfigByAmount(stakeDoes);
  const configuredBotDifficulty = await getConfiguredDuelBotDifficulty();

  if (!selectedStakeConfig) {
    throw new HttpsError("invalid-argument", "Mise duel non autorisee.");
  }

  for (const excludedRoomId of excludedRoomIds) {
    await forceRemoveUserFromDuelRoom(excludedRoomId, uid).catch(() => null);
  }

  const activeRoom = await findActiveDuelRoomForUser(uid);
  if (activeRoom) {
    if (excludedRoomIdSet.has(String(activeRoom.roomId || "").trim())) {
      await forceRemoveUserFromDuelRoom(activeRoom.roomId, uid).catch(() => null);
    } else {
    const privateDeckOrder = activeRoom.status === "playing"
      ? await readPrivateDeckOrderForDuelRoom(activeRoom.roomId)
      : [];
    return {
      ok: true,
      resumed: true,
      charged: false,
      roomId: activeRoom.roomId,
      seatIndex: activeRoom.seatIndex,
      status: activeRoom.status,
      roomMode: String(activeRoom.roomMode || "duel_2p"),
      stakeDoes: safeInt(activeRoom.stakeDoes),
      inviteCode: String(activeRoom.inviteCode || "").trim(),
      privateDeckOrder,
    };
    }
  }

  const rewardAmountDoes = selectedStakeConfig.rewardDoes;
  const poolRef = duelMatchmakingPoolRef(selectedStakeConfig.id, stakeDoes);

  const created = await db.runTransaction(async (tx) => {
    const nowMs = Date.now();
    const [poolSnap, walletSnap] = await Promise.all([
      tx.get(poolRef),
      tx.get(walletRef(uid)),
    ]);

    const walletData = walletSnap.exists ? (walletSnap.data() || {}) : {};
    assertWalletNotFrozen(walletData);

    const beforeDoes = safeInt(walletData.doesBalance);
    if (beforeDoes < stakeDoes) {
      throw new HttpsError("failed-precondition", "Solde Does insuffisant.");
    }

    const existingOpenRoomId = String(poolSnap.exists ? (poolSnap.data() || {}).openRoomId || "" : "").trim();
    if (existingOpenRoomId) {
      const openRoomRef = duelRoomRef(existingOpenRoomId);
      const roomSnap = await tx.get(openRoomRef);
      if (roomSnap.exists) {
        const room = roomSnap.data() || {};
        const status = String(room.status || "");
        const roomEntryCostDoes = safeInt(room.entryCostDoes || room.stakeDoes);
        const roomRewardAmountDoes = safeInt(room.rewardAmountDoes || 0);
        const playerUids = Array.from({ length: 2 }, (_, idx) => String((room.playerUids || [])[idx] || ""));
        const waitingDeadlineMs = resolveDuelWaitingDeadlineMs(room, nowMs);
        const humans = playerUids.filter(Boolean).length;

        if (
          status === "waiting"
          && !getBlockedRejoinSet(room).has(uid)
          && roomEntryCostDoes === stakeDoes
          && roomRewardAmountDoes === rewardAmountDoes
        ) {
          if (nowMs >= waitingDeadlineMs) {
            clearDuelMatchmakingPool(tx, poolRef);
            return {
              ok: true,
              resumed: false,
              charged: false,
              roomId: openRoomRef.id,
              seatIndex: 0,
              ...buildStartedDuelRoomTransaction(tx, openRoomRef, {
                ...room,
                humanCount: humans,
                botCount: Math.max(0, 2 - humans),
                waitingDeadlineMs,
              }, {
                configuredBotDifficulty,
                nowMs,
              }),
            };
          }

          const currentSeats = room.seats && typeof room.seats === "object" ? room.seats : {};
          const usedSeats = new Set(
            Object.values(currentSeats)
              .map((seat) => Number(seat))
              .filter((seat) => Number.isFinite(seat) && seat >= 0 && seat < 2)
          );
          const seatIndex = [0, 1].find((seat) => !usedSeats.has(seat));
          if (typeof seatIndex === "number" && humans < 2) {
            const walletMutation = await applyWalletMutationTx(tx, {
              uid,
              email,
              type: "game_entry",
              note: "Participation duel 2 joueurs",
              amountDoes: stakeDoes,
              amountGourdes: 0,
              deltaDoes: -stakeDoes,
              deltaExchangedGourdes: 0,
            });

            const nextPlayerUids = playerUids.slice();
            nextPlayerUids[seatIndex] = uid;
            const currentNames = Array.from({ length: 2 }, (_, idx) => String((room.playerNames || [])[idx] || ""));
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
              welcomeDoes: safeInt(walletMutation.gameEntryFunding?.welcomeDoes),
              provisionalSources: normalizeFundingSources(walletMutation.gameEntryFunding?.provisionalSources),
            };

            if (nextHumans >= 2) {
              clearDuelMatchmakingPool(tx, poolRef);
              return {
                ok: true,
                resumed: false,
                charged: true,
                roomId: openRoomRef.id,
                seatIndex,
                does: walletMutation.afterDoes,
                ...buildStartedDuelRoomTransaction(tx, openRoomRef, {
                  ...room,
                  playerUids: nextPlayerUids,
                  playerNames: nextPlayerNames,
                  seats: nextSeats,
                  entryFundingByUid: currentEntryFunding,
                  roomPresenceMs: nextPresence,
                  humanCount: nextHumans,
                  botCount: 0,
                  waitingDeadlineMs,
                }, {
                  configuredBotDifficulty,
                  nowMs,
                }),
              };
            }
          }
        }
      }
    }

    const walletMutation = await applyWalletMutationTx(tx, {
      uid,
      email,
      type: "game_entry",
      note: "Participation duel 2 joueurs",
      amountDoes: stakeDoes,
      amountGourdes: 0,
      deltaDoes: -stakeDoes,
      deltaExchangedGourdes: 0,
    });

    const newRoomRef = duelRoomRef();
    tx.set(newRoomRef, {
      status: "waiting",
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      createdAtMs: nowMs,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      ownerUid: uid,
      roomMode: "duel_2p",
      gameMode: "domino-duel",
      engineVersion: 1,
      playerUids: [uid, ""],
      playerNames: [sanitizePlayerLabel(email || uid, 0), ""],
      entryFundingByUid: {
        [uid]: {
          approvedDoes: safeInt(walletMutation.gameEntryFunding?.approvedDoes),
          provisionalDoes: safeInt(walletMutation.gameEntryFunding?.provisionalDoes),
          welcomeDoes: safeInt(walletMutation.gameEntryFunding?.welcomeDoes),
          provisionalSources: normalizeFundingSources(walletMutation.gameEntryFunding?.provisionalSources),
        },
      },
      blockedRejoinUids: [],
      humanCount: 1,
      seats: { [uid]: 0 },
      roomPresenceMs: { [uid]: nowMs },
      botCount: 1,
      botDifficulty: configuredBotDifficulty,
      startRevealPending: false,
      startRevealAckUids: [],
      waitingDeadlineMs: nowMs + ROOM_WAIT_MS,
      startedAt: null,
      startedAtMs: 0,
      endedAtMs: 0,
      turnLockedUntilMs: 0,
      nextActionSeq: 0,
      playedCount: 0,
      stakeDoes,
      entryCostDoes: stakeDoes,
      rewardAmountDoes,
      stakeConfigId: selectedStakeConfig.id,
    });
    setDuelMatchmakingPoolOpen(tx, poolRef, newRoomRef.id, selectedStakeConfig.id, stakeDoes);

    return {
      ok: true,
      resumed: false,
      charged: true,
      roomId: newRoomRef.id,
      seatIndex: 0,
      status: "waiting",
      does: walletMutation.afterDoes,
      waitingDeadlineMs: nowMs + ROOM_WAIT_MS,
      humanCount: 1,
      botCount: 1,
      privateDeckOrder: [],
    };
  });

  if (created?.status === "playing" && created?.startRevealPending !== true) {
    await processPendingBotTurnsDuel(String(created.roomId || ""));
  }

  return created;
}, { invoker: "public" });

exports.ensureRoomReadyDuel = publicOnCall("ensureRoomReadyDuel", async (request) => {
  const { uid } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const roomId = String(payload.roomId || "").trim();
  const configuredBotDifficulty = await getConfiguredDuelBotDifficulty();

  if (!roomId) {
    throw new HttpsError("invalid-argument", "roomId requis.");
  }

  const roomRefDoc = duelRoomRef(roomId);
  const startResult = await db.runTransaction(async (tx) => {
    const roomSnap = await tx.get(roomRefDoc);
    if (!roomSnap.exists) {
      throw new HttpsError("not-found", "Salle duel introuvable.");
    }

    const room = roomSnap.data() || {};
    const seatIndex = getSeatForUser(room, uid);
    if (seatIndex < 0) {
      throw new HttpsError("permission-denied", "Tu ne fais pas partie de ce duel.");
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
        privateDeckOrder: status === "playing" ? [] : [],
      };
    }

    const nowMs = Date.now();
    const humans = Array.isArray(room.playerUids) ? room.playerUids.filter(Boolean).length : safeInt(room.humanCount);
    const waitingDeadlineMs = resolveDuelWaitingDeadlineMs(room, nowMs);
    if (safeSignedInt(room.waitingDeadlineMs) !== waitingDeadlineMs) {
      tx.update(roomRefDoc, {
        waitingDeadlineMs,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      });
    }

    if (humans < 2 && (isFriendDuelRoom(room) || nowMs < waitingDeadlineMs)) {
      return {
        ok: true,
        started: false,
        status: "waiting",
        startRevealPending: false,
        waitingDeadlineMs,
        humanCount: humans,
        botCount: isFriendDuelRoom(room) ? 0 : Math.max(0, 2 - humans),
        privateDeckOrder: [],
      };
    }

    clearDuelMatchmakingPool(tx, duelMatchmakingPoolRef(String(room.stakeConfigId || ""), safeInt(room.entryCostDoes || room.stakeDoes)));
    return buildStartedDuelRoomTransaction(tx, roomRefDoc, {
      ...room,
      humanCount: humans,
      botCount: Math.max(0, 2 - humans),
      waitingDeadlineMs,
    }, {
      configuredBotDifficulty,
      nowMs,
    });
  });

  if (startResult?.status === "playing") {
    if (!Array.isArray(startResult.privateDeckOrder) || startResult.privateDeckOrder.length !== 28) {
      startResult.privateDeckOrder = await readPrivateDeckOrderForDuelRoom(roomId);
    }
  }

  if (startResult?.status === "playing" && startResult?.startRevealPending !== true) {
    await processPendingBotTurnsDuel(roomId);
  }

  return startResult;
}, { invoker: "public" });

exports.touchRoomPresenceDuel = publicOnCall("touchRoomPresenceDuel", async (request) => {
  const { uid } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const roomId = String(payload.roomId || "").trim();

  if (!roomId) {
    throw new HttpsError("invalid-argument", "roomId requis.");
  }

  const roomRefDoc = duelRoomRef(roomId);
  let shouldNudgeBots = false;
  let shouldResolveExpiredHumanTurn = false;
  const result = await db.runTransaction(async (tx) => {
    const roomSnap = await tx.get(roomRefDoc);
    if (!roomSnap.exists) {
      throw new HttpsError("not-found", "Salle duel introuvable.");
    }

    const room = roomSnap.data() || {};
    const seatIndex = getSeatForUser(room, uid);
    if (seatIndex < 0) {
      throw new HttpsError("permission-denied", "Tu ne fais pas partie de cette salle duel.");
    }

    const nowMs = Date.now();
    const presenceResult = buildDuelPresenceUpdates(room, uid, nowMs);
    shouldNudgeBots = presenceResult.shouldNudgeBots === true;
    shouldResolveExpiredHumanTurn = presenceResult.shouldResolveExpiredHumanTurn === true;
    tx.update(roomRefDoc, presenceResult.updates);

    return {
      ok: true,
      roomId: roomRefDoc.id,
      status: String(room.status || ""),
      currentPlayer: Number.isFinite(Number(room.currentPlayer)) ? Math.trunc(Number(room.currentPlayer)) : -1,
      takeoverCount: presenceResult.takeoverCount,
      humanCount: safeInt(presenceResult.updates.humanCount, safeInt(room.humanCount)),
      botCount: safeInt(presenceResult.updates.botCount, safeInt(room.botCount)),
    };
  });

  if (result?.status === "playing" && (shouldNudgeBots || shouldResolveExpiredHumanTurn)) {
    await processPendingBotTurnsDuel(roomId);
  }

  return result;
}, { invoker: "public" });

exports.ackRoomStartSeenDuel = publicOnCall("ackRoomStartSeenDuel", async (request) => {
  const { uid } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const roomId = String(payload.roomId || "").trim();

  if (!roomId) {
    throw new HttpsError("invalid-argument", "roomId requis.");
  }

  const roomRefDoc = duelRoomRef(roomId);
  const ackResult = await db.runTransaction(async (tx) => {
    const roomSnap = await tx.get(roomRefDoc);
    if (!roomSnap.exists) {
      throw new HttpsError("not-found", "Salle duel introuvable.");
    }

    const room = roomSnap.data() || {};
    const humanUids = Array.isArray(room.playerUids)
      ? room.playerUids.map((item) => String(item || "").trim()).filter(Boolean)
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
      throw new HttpsError("permission-denied", "Tu ne fais pas partie de cette salle duel.");
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
    tx.update(roomRefDoc, {
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
    await processPendingBotTurnsDuel(roomId);
  }

  return ackResult;
}, { invoker: "public" });

exports.leaveRoomDuel = publicOnCall("leaveRoomDuel", async (request) => {
  const { uid } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const roomId = String(payload.roomId || "").trim();

  if (!roomId) {
    throw new HttpsError("invalid-argument", "roomId requis.");
  }

  const roomRefDoc = duelRoomRef(roomId);
  const outcome = await db.runTransaction(async (tx) => {
    const roomSnap = await tx.get(roomRefDoc);
    if (!roomSnap.exists) {
      return {
        result: {
          ok: true,
          deleted: true,
          status: "missing",
        },
        shouldCleanup: false,
        shouldNudgeBots: false,
      };
    }

    return applyDuelLeaveForUidTx(tx, roomRefDoc, roomSnap.data() || {}, uid);
  });

  if (outcome?.shouldNudgeBots) {
    await processPendingBotTurnsDuel(roomId);
  }

  if (!outcome?.shouldCleanup) {
    return outcome?.result || {
      ok: true,
      deleted: false,
      status: "left",
    };
  }

  await cleanupDuelRoom(roomRefDoc);
  return {
    ok: true,
    deleted: true,
    status: "deleted",
  };
}, { invoker: "public" });

exports.submitActionDuel = publicOnCall("submitActionDuel", async (request) => {
  const { uid } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const roomId = String(payload.roomId || "").trim();
  const clientActionId = sanitizeText(payload.clientActionId || "", 80);
  const action = payload.action && typeof payload.action === "object" ? payload.action : null;

  if (!roomId || !action) {
    throw new HttpsError("invalid-argument", "roomId et action duel sont requis.");
  }
  if (!clientActionId) {
    throw new HttpsError("invalid-argument", "clientActionId requis.");
  }

  const type = String(action.type || "").trim();
  if (type !== "play" && type !== "pass" && type !== "draw") {
    throw new HttpsError("invalid-argument", "Type d'action duel invalide.");
  }

  const roomRefDoc = duelRoomRef(roomId);
  const result = await db.runTransaction(async (tx) => {
    const [roomSnap, stateSnap] = await Promise.all([
      tx.get(roomRefDoc),
      tx.get(duelGameStateRef(roomId)),
    ]);
    if (!roomSnap.exists) {
      throw new HttpsError("not-found", "Salle duel introuvable.");
    }

    const room = roomSnap.data() || {};
    if (room.status !== "playing") {
      throw new HttpsError("failed-precondition", "Le duel n'est pas en cours.");
    }
    if (room.startRevealPending === true) {
      throw new HttpsError("failed-precondition", "Le duel se synchronise encore.");
    }
    const localSeat = getSeatForUser(room, uid);
    if (localSeat < 0) {
      throw new HttpsError("permission-denied", "Tu ne fais pas partie de ce duel.");
    }

    if (typeof room.currentPlayer === "number" && room.currentPlayer !== localSeat) {
      throw new HttpsError("failed-precondition", `Hors tour duel. Joueur attendu: ${room.currentPlayer + 1}`);
    }

    const currentState = stateSnap.exists
      ? normalizeDuelGameState(stateSnap.data(), room)
      : createInitialDuelGameState(room, Array.isArray(room.deckOrder) && room.deckOrder.length === 28 ? room.deckOrder : makeDeckOrder());

    if (currentState.winnerSeat >= 0) {
      throw new HttpsError("failed-precondition", "Le duel est deja termine.");
    }
    if (typeof currentState.currentPlayer === "number" && currentState.currentPlayer !== localSeat) {
      throw new HttpsError("failed-precondition", `Hors tour duel. Joueur attendu: ${currentState.currentPlayer + 1}`);
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

    const resolvedMove = resolveRequestedDuelMove(currentState, localSeat, action);
    const batchResult = applyDuelActionBatchInTransaction(
      tx,
      roomRefDoc,
      room,
      currentState,
      roomId,
      resolvedMove,
      uid,
      { allowBotAdvance: false }
    );
    const nextState = batchResult.state;
    nextState.idempotencyKeys[clientActionId] = true;

    tx.set(duelGameStateRef(roomId), buildDuelGameStateWrite(nextState), { merge: true });

    const roomUpdate = buildDuelRoomUpdateFromGameState(room, nextState, batchResult.records);
    tx.update(roomRefDoc, roomUpdate);
    writeDuelRoomResultIfEndedTx(tx, roomRefDoc, room, roomUpdate);

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
      record: lastRecord || null,
    };
  });

  if (result?.status === "playing" && typeof result.nextPlayer === "number") {
    await processPendingBotTurnsDuel(roomId);
  }

  return result;
}, { invoker: "public" });

exports.claimWinRewardDuel = publicOnCall("claimWinRewardDuel", async (request) => {
  const { uid, email } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const roomId = String(payload.roomId || "").trim();

  if (!roomId) {
    throw new HttpsError("invalid-argument", "roomId requis.");
  }

  const roomRefDoc = duelRoomRef(roomId);
  const settlementRef = roomRefDoc.collection("settlements").doc(uid);

  const result = await db.runTransaction(async (tx) => {
    const [roomSnap, settlementSnap, stateSnap] = await Promise.all([
      tx.get(roomRefDoc),
      tx.get(settlementRef),
      tx.get(duelGameStateRef(roomId)),
    ]);

    if (!roomSnap.exists) {
      throw new HttpsError("not-found", "Salle duel introuvable.");
    }

    const room = roomSnap.data() || {};
    const seat = getSeatForUser(room, uid);
    const state = stateSnap.exists ? normalizeDuelGameState(stateSnap.data(), room) : null;
    const winnerSeat = typeof room.winnerSeat === "number"
      ? room.winnerSeat
      : (state && typeof state.winnerSeat === "number" ? state.winnerSeat : -1);
    const winnerUid = String(room.winnerUid || state?.winnerUid || "").trim();

    if (winnerUid) {
      if (winnerUid !== uid) {
        throw new HttpsError("permission-denied", "Ce compte n'est pas gagnant de ce duel.");
      }
    } else if (seat < 0) {
      throw new HttpsError("permission-denied", "Ce compte ne fait pas partie de ce duel.");
    } else if (winnerSeat < 0 || seat !== winnerSeat) {
      throw new HttpsError("permission-denied", "Ce compte n'est pas gagnant de ce duel.");
    }

    const settlementData = settlementSnap.exists ? (settlementSnap.data() || {}) : {};
    if (settlementData.rewardPaid === true) {
      return {
        ok: true,
        rewardGranted: false,
        reason: "already_paid",
        rewardAmountDoes: safeInt(settlementData.rewardAmountDoes) || safeInt(room.rewardAmountDoes),
      };
    }

    const rewardAmountDoes = safeInt(room.rewardAmountDoes);
    if (rewardAmountDoes <= 0) {
      throw new HttpsError("failed-precondition", "Gain duel invalide pour cette salle.");
    }

    const entryFundingRaw = room.entryFundingByUid && typeof room.entryFundingByUid === "object"
      ? (room.entryFundingByUid[uid] || null)
      : null;
    const provisionalSources = normalizeFundingSources(entryFundingRaw?.provisionalSources);
    const approvedEntryDoes = safeInt(entryFundingRaw?.approvedDoes);
    const provisionalEntryDoes = safeInt(entryFundingRaw?.provisionalDoes);
    const welcomeEntryDoes = safeInt(entryFundingRaw?.welcomeDoes);
    let approvedRewardDoes = rewardAmountDoes;
    let provisionalRewardDoes = 0;
    let welcomeRewardDoes = 0;

    if ((provisionalEntryDoes > 0 && provisionalSources.length > 0) || welcomeEntryDoes > 0) {
      const totalEntryDoes = Math.max(approvedEntryDoes + provisionalEntryDoes + welcomeEntryDoes, provisionalEntryDoes + welcomeEntryDoes);
      const provisionalRewardPool = Math.min(
        rewardAmountDoes,
        Math.round((rewardAmountDoes * provisionalEntryDoes) / Math.max(1, totalEntryDoes))
      );
      welcomeRewardDoes = Math.min(
        rewardAmountDoes - provisionalRewardPool,
        Math.round((rewardAmountDoes * welcomeEntryDoes) / Math.max(1, totalEntryDoes))
      );
      provisionalRewardDoes = provisionalRewardPool;
    } else if (approvedEntryDoes <= 0 && provisionalEntryDoes > 0) {
      provisionalRewardDoes = rewardAmountDoes;
    }

    ({
      approvedRewardDoes,
      provisionalRewardDoes,
      welcomeRewardDoes,
    } = normalizeRewardSettlementSplit({
      rewardAmountDoes,
      provisionalRewardDoes,
      welcomeRewardDoes,
    }));

    const walletMutation = await applyWalletMutationTx(tx, {
      uid,
      email,
      type: "game_reward",
      note: `Gain de duel (${roomId})`,
      amountDoes: rewardAmountDoes,
      approvedRewardDoes,
      provisionalRewardDoes,
      welcomeRewardDoes,
      amountGourdes: 0,
      deltaDoes: rewardAmountDoes,
      deltaExchangedGourdes: 0,
    });
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
      stakeDoes: safeInt(room.entryCostDoes || room.stakeDoes),
      does: walletMutation.afterDoes,
      approvedRewardDoes,
      provisionalRewardDoes,
    };
  });

  let agentCommissionDoes = 0;
  if (result?.rewardGranted === true) {
    try {
      const agentCommission = await awardAgentCommissionForClientWin({
        playerUid: uid,
        gameType: "domino_duel",
        roomId,
        stakeDoes: safeInt(result.stakeDoes),
        rewardDoes: safeInt(result.rewardAmountDoes),
        wonAtMs: Date.now(),
      });
      agentCommissionDoes = safeInt(agentCommission?.commissionDoes);
    } catch (error) {
      console.error("[AGENT_COMMISSION][claimWinRewardDuel] skipped", {
        uid,
        roomId,
        error: error?.message || String(error || ""),
      });
    }
  }

  return {
    ...result,
    agentCommissionDoes,
  };
}, { invoker: "public" });

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

exports.sweepDuelRoomPresence = onSchedule("every 1 minutes", async () => {
  const nowMs = Date.now();
  const roomsSnap = await db
    .collection(DUEL_ROOMS_COLLECTION)
    .where("status", "in", ["waiting", "playing"])
    .limit(200)
    .get();

  const roomsToNudge = [];

  for (const docSnap of roomsSnap.docs) {
    const roomId = docSnap.id;
    try {
      const result = await db.runTransaction(async (tx) => {
        const freshSnap = await tx.get(docSnap.ref);
        if (!freshSnap.exists) return { changed: false, shouldNudgeBots: false };

        const room = freshSnap.data() || {};
        const playerUids = Array.from({ length: 2 }, (_, idx) => String((room.playerUids || [])[idx] || ""));
        if (!playerUids.some(Boolean)) return { changed: false, shouldNudgeBots: false };

        const presenceResult = buildDuelPresenceUpdates(room, "", nowMs);
        if (!presenceResult.changed) {
          return { changed: false, shouldNudgeBots: false };
        }

        tx.update(docSnap.ref, presenceResult.updates);
        return {
          changed: true,
          shouldNudgeBots: presenceResult.shouldNudgeBots === true,
        };
      });

      if (result?.shouldNudgeBots === true) {
        roomsToNudge.push(roomId);
      }
    } catch (error) {
      console.warn("[SWEEP_DUEL_PRESENCE]", roomId, error?.message || error);
    }
  }

  for (const targetRoomId of roomsToNudge) {
    await processPendingBotTurnsDuel(targetRoomId);
  }
});

exports.sweepMorpionRoomPresence = onSchedule("every 1 minutes", async () => {
  const nowMs = Date.now();
  const roomsSnap = await db
    .collection(MORPION_ROOMS_COLLECTION)
    .where("status", "in", ["waiting", "playing"])
    .limit(200)
    .get();

  const roomsToNudge = [];

  for (const docSnap of roomsSnap.docs) {
    const roomId = docSnap.id;
    try {
      const result = await db.runTransaction(async (tx) => {
        const freshSnap = await tx.get(docSnap.ref);
        if (!freshSnap.exists) return { changed: false, shouldNudgeBots: false };

        const room = freshSnap.data() || {};
        const playerUids = Array.from({ length: 2 }, (_, idx) => String((room.playerUids || [])[idx] || ""));
        if (!playerUids.some(Boolean)) return { changed: false, shouldNudgeBots: false };

        const presenceResult = buildMorpionPresenceUpdates(room, "", nowMs);
        tx.update(docSnap.ref, presenceResult.updates);
        return {
          changed: true,
          shouldNudgeBots: presenceResult.shouldNudgeBots === true || presenceResult.shouldResolveExpiredHumanTurn === true,
        };
      });

      if (result?.shouldNudgeBots === true) {
        roomsToNudge.push(roomId);
      }
    } catch (error) {
      console.warn("[SWEEP_MORPION_PRESENCE]", roomId, error?.message || error);
    }
  }

  for (const targetRoomId of roomsToNudge) {
    await processPendingBotTurnsMorpion(targetRoomId);
  }
});

exports.capturePresenceAnalytics = onSchedule("every 30 minutes", async () => {
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
}, { minInstances: 1 });

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
}, { minInstances: 1 });

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
}, { minInstances: 1 });

exports.getRecruitmentCampaignSnapshotSecure = publicOnCall("getRecruitmentCampaignSnapshotSecure", async () => {
  const snap = await db.collection(ANALYTICS_META_COLLECTION).doc(RECRUITMENT_CAMPAIGN_DOC).get();
  const data = snap.exists ? (snap.data() || {}) : {};
  return {
    ok: true,
    snapshot: buildRecruitmentCampaignSnapshot(data),
  };
});

exports.recordRecruitmentVisitSecure = publicOnCall("recordRecruitmentVisitSecure", async (request) => {
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const sessionId = sanitizeText(payload.sessionId || "", 120);
  const nowMs = Date.now();
  const nowIso = new Date(nowMs).toISOString();
  const campaignRef = db.collection(ANALYTICS_META_COLLECTION).doc(RECRUITMENT_CAMPAIGN_DOC);

  const snapshot = await db.runTransaction(async (tx) => {
    const campaignSnap = await tx.get(campaignRef);
    const campaignData = campaignSnap.exists ? (campaignSnap.data() || {}) : {};
    const knownSessions = Array.isArray(campaignData.recentVisitSessions)
      ? campaignData.recentVisitSessions.filter((item) => typeof item === "string")
      : [];
    const nextSessions = sessionId
      ? [sessionId, ...knownSessions.filter((item) => item !== sessionId)].slice(0, 40)
      : knownSessions.slice(0, 40);
    const hasSession = sessionId && knownSessions.includes(sessionId);
    const nextVisitCount = safeInt(campaignData.pageVisitCount) + (hasSession ? 0 : 1);

    tx.set(campaignRef, {
      pageVisitCount: nextVisitCount,
      targetCount: safeInt(campaignData.targetCount) || RECRUITMENT_TARGET_COUNT,
      deadlineMs: safeInt(campaignData.deadlineMs) || RECRUITMENT_DEADLINE_MS,
      recentVisitSessions: nextSessions,
      lastVisitAtMs: nowMs,
      lastVisitAt: nowIso,
      updatedAtMs: nowMs,
      updatedAt: nowIso,
    }, { merge: true });

    return {
      ...campaignData,
      pageVisitCount: nextVisitCount,
      targetCount: safeInt(campaignData.targetCount) || RECRUITMENT_TARGET_COUNT,
      deadlineMs: safeInt(campaignData.deadlineMs) || RECRUITMENT_DEADLINE_MS,
      updatedAtMs: nowMs,
      lastVisitAtMs: nowMs,
    };
  });

  return {
    ok: true,
    snapshot: buildRecruitmentCampaignSnapshot(snapshot),
  };
});

exports.submitRecruitmentApplicationSecure = publicOnCall("submitRecruitmentApplicationSecure", async (request) => {
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const firstName = sanitizeText(payload.firstName || "", 80);
  const lastName = sanitizeText(payload.lastName || "", 80);
  const sex = normalizeRecruitmentSex(payload.sex || "");
  const phone = sanitizePhone(payload.phone || "", 40);
  const phoneKey = phoneDigits(phone);
  const fullAddress = sanitizeText(payload.fullAddress || "", 220);
  const currentPosition = sanitizeText(payload.currentPosition || "", 120);
  const networkReach = safeInt(payload.networkReach);
  const motivationLetter = sanitizeText(payload.motivationLetter || "", 2500);
  const role = normalizeRecruitmentRole(payload.role || "");

  if (!firstName || !lastName || !sex || !phoneKey || !fullAddress || !currentPosition) {
    throw new HttpsError("invalid-argument", "Informations de candidature incomplètes.");
  }

  if (phoneKey.length < 8) {
    throw new HttpsError("invalid-argument", "Numéro de téléphone invalide.");
  }

  if (networkReach <= 0) {
    throw new HttpsError("invalid-argument", "Quantité de réseau invalide.");
  }

  if (motivationLetter.length < 40) {
    throw new HttpsError("invalid-argument", "Lettre de motivation trop courte.");
  }

  const authUid = sanitizeText(request.auth?.uid || "", 128);
  const authEmail = sanitizeEmail(request.auth?.token?.email || "", 160);
  const applicationRef = db.collection(RECRUITMENT_APPLICATIONS_COLLECTION).doc(hashText(phoneKey));
  const campaignRef = db.collection(ANALYTICS_META_COLLECTION).doc(RECRUITMENT_CAMPAIGN_DOC);
  const applicationCode = `REC-${randomCode(8)}`;
  const nowMs = Date.now();
  const nowIso = new Date(nowMs).toISOString();

  const snapshot = await db.runTransaction(async (tx) => {
    const [applicationSnap, campaignSnap] = await Promise.all([
      tx.get(applicationRef),
      tx.get(campaignRef),
    ]);

    if (applicationSnap.exists) {
      throw new HttpsError("already-exists", "Une candidature existe déjà pour ce numéro.", {
        code: "recruitment-application-exists",
      });
    }

    const campaignData = campaignSnap.exists ? (campaignSnap.data() || {}) : {};
    const nextCount = safeInt(campaignData.applicationsCount) + 1;

    tx.set(applicationRef, {
      applicationCode,
      firstName,
      lastName,
      fullName: `${firstName} ${lastName}`.trim(),
      sex,
      phone,
      phoneDigits: phoneKey,
      phoneHash: hashText(phoneKey),
      fullAddress,
      currentPosition,
      networkReach,
      motivationLetter,
      role,
      status: "pending",
      source: "public_form",
      authUid,
      authEmail,
      createdAt: nowIso,
      createdAtMs: nowMs,
      updatedAt: nowIso,
      updatedAtMs: nowMs,
    }, { merge: true });

    tx.set(campaignRef, {
      applicationsCount: nextCount,
      pageVisitCount: safeInt(campaignData.pageVisitCount),
      targetCount: safeInt(campaignData.targetCount) || RECRUITMENT_TARGET_COUNT,
      deadlineMs: safeInt(campaignData.deadlineMs) || RECRUITMENT_DEADLINE_MS,
      updatedAtMs: nowMs,
      updatedAt: nowIso,
    }, { merge: true });

    return buildRecruitmentCampaignSnapshot({
      ...campaignData,
      applicationsCount: nextCount,
      targetCount: safeInt(campaignData.targetCount) || RECRUITMENT_TARGET_COUNT,
      deadlineMs: safeInt(campaignData.deadlineMs) || RECRUITMENT_DEADLINE_MS,
      updatedAtMs: nowMs,
    });
  });

  return {
    ok: true,
    applicationCode,
    snapshot,
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
}, { minInstances: 1 });

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

exports.getGlobalAnalyticsSnapshot = publicOnCall(
  "getGlobalAnalyticsSnapshot",
  async (request) => {
    assertFinanceAdmin(request);
    const payload = request.data && typeof request.data === "object" ? request.data : {};
    const botDifficulty = await getConfiguredBotDifficulty();
    const nowMs = Date.now();
    const range = getGlobalAnalyticsRange(payload, nowMs);
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
      safeAnalyticsQueryGet(
        applyAnalyticsTimeRange(db.collection(ROOMS_COLLECTION), "createdAtMs", range),
        db.collection(ROOMS_COLLECTION),
        "rooms"
      ),
      safeAnalyticsQueryGet(
        applyAnalyticsTimeRange(db.collectionGroup("orders"), "createdAtMs", range),
        db.collectionGroup("orders"),
        "orders"
      ),
      safeAnalyticsQueryGet(
        applyAnalyticsTimeRange(db.collectionGroup("withdrawals"), "createdAtMs", range),
        db.collectionGroup("withdrawals"),
        "withdrawals"
      ),
      safeAnalyticsQueryGet(
        applyAnalyticsTimeRange(db.collectionGroup("xchanges"), "createdAtMs", range),
        db.collectionGroup("xchanges"),
        "xchanges"
      ),
      safeAnalyticsQueryGet(
        applyAnalyticsTimeRange(db.collectionGroup("referralRewards"), "createdAtMs", range),
        db.collectionGroup("referralRewards"),
        "referralRewards"
      ),
      safeAnalyticsQueryGet(
        applyAnalyticsTimeRange(db.collectionGroup("referrals"), "createdAtMs", range),
        db.collectionGroup("referrals"),
        "referrals"
      ),
      safeAnalyticsQueryGet(
        applyAnalyticsTimeRange(db.collection(CHAT_COLLECTION), "createdAtMs", range),
        db.collection(CHAT_COLLECTION),
        "channelMessages"
      ),
      safeAnalyticsQueryGet(
        applyAnalyticsTimeRange(db.collection(SUPPORT_THREADS_COLLECTION), "createdAtMs", range),
        db.collection(SUPPORT_THREADS_COLLECTION),
        "supportThreads"
      ),
      safeAnalyticsQueryGet(
        applyAnalyticsTimeRange(db.collectionGroup(SUPPORT_MESSAGES_SUBCOLLECTION), "createdAtMs", range),
        db.collectionGroup(SUPPORT_MESSAGES_SUBCOLLECTION),
        "supportMessages"
      ),
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
      requestedRange: range,
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
  },
  {
    memory: "1GiB",
  }
);

async function computePresenceAnalyticsSnapshot(options = {}) {
  const nowMs = safeSignedInt(options.nowMs) || Date.now();
  const range = getGlobalAnalyticsRange(options, nowMs);
  const live = await collectPresenceAnalyticsNow(nowMs);
  const defaultSnapshotsStartMs = nowMs - (PRESENCE_ANALYTICS_RECENT_SNAPSHOT_DAYS * 24 * 60 * 60 * 1000);
  const snapshotsStartMs = range.startMs > 0
    ? Math.max(range.startMs, defaultSnapshotsStartMs)
    : defaultSnapshotsStartMs;
  const startDayKey = range.startMs > 0 ? getPresenceLocalKeys(range.startMs).dayKey : "";
  const endDayKey = getPresenceLocalKeys(range.endMs || nowMs).dayKey;

  let dailyQuery = presenceDailyCollection().orderBy("dayKey", "desc");
  if (startDayKey) {
    dailyQuery = dailyQuery.where("dayKey", ">=", startDayKey);
  }
  if (endDayKey) {
    dailyQuery = dailyQuery.where("dayKey", "<=", endDayKey);
  }

  const [
    snapshotsSnap,
    dailySnap,
    hourSnap,
    weekdaySnap,
  ] = await Promise.all([
    presenceSnapshotsCollection()
      .where("bucketMs", ">=", snapshotsStartMs)
      .where("bucketMs", "<=", range.endMs || nowMs)
      .orderBy("bucketMs", "asc")
      .get(),
    dailyQuery.limit(PRESENCE_ANALYTICS_RECENT_DAYS_LIMIT).get(),
    presenceHourCollection().orderBy("hourKey", "asc").get(),
    presenceWeekdayCollection().orderBy("weekdayKey", "asc").get(),
  ]);

  const snapshots = snapshotsSnap.docs.map(snapshotRecordForCallable);
  const daily = dailySnap.docs.map(snapshotRecordForCallable).reverse();
  const hourOfDay = hourSnap.docs.map(snapshotRecordForCallable);
  const weekday = weekdaySnap.docs.map(snapshotRecordForCallable);

  const trend = daily.map((item) => {
    const samples = Math.max(1, safeInt(item.samples));
    return {
      label: String(item.dayKey || ""),
      peakVisitors: safeInt(item.onlineUsersMax),
      avgVisitors: Math.round(safeInt(item.onlineUsersSum) / samples),
      peakPlayers: safeInt(item.onlineInGameUsersMax),
      avgPlayers: Math.round(safeInt(item.onlineInGameUsersSum) / samples),
      peakRooms: safeInt(item.playingRoomsMax),
      samples,
    };
  });

  const snapshotTrend = snapshots.map((item) => ({
    bucketMs: safeSignedInt(item.bucketMs),
    label: String(item.dayKey || ""),
    onlineUsers: safeInt(item.onlineUsers),
    onlineInGameUsers: safeInt(item.onlineInGameUsers),
    playingRooms: safeInt(item.playingRooms),
    waitingRooms: safeInt(item.waitingRooms),
  }));

  const peakMoments = snapshots
    .map((item) => ({
      bucketMs: safeSignedInt(item.bucketMs),
      onlineUsers: safeInt(item.onlineUsers),
      onlineInGameUsers: safeInt(item.onlineInGameUsers),
      playingRooms: safeInt(item.playingRooms),
    }))
    .sort((a, b) => b.onlineUsers - a.onlineUsers || b.onlineInGameUsers - a.onlineInGameUsers || a.bucketMs - b.bucketMs)
    .slice(0, 8);

  const activeDays = Math.max(1, trend.length);
  const avgDailyPeakVisitors = trend.length > 0
    ? Math.round(trend.reduce((sum, item) => sum + safeInt(item.peakVisitors), 0) / activeDays)
    : safeInt(live.onlineUsers);
  const avgDailyPeakPlayers = trend.length > 0
    ? Math.round(trend.reduce((sum, item) => sum + safeInt(item.peakPlayers), 0) / activeDays)
    : safeInt(live.onlineInGameUsers);
  const peakVisitors = Math.max(
    safeInt(live.onlineUsers),
    ...trend.map((item) => safeInt(item.peakVisitors)),
    ...snapshotTrend.map((item) => safeInt(item.onlineUsers))
  );
  const peakPlayers = Math.max(
    safeInt(live.onlineInGameUsers),
    ...trend.map((item) => safeInt(item.peakPlayers)),
    ...snapshotTrend.map((item) => safeInt(item.onlineInGameUsers))
  );
  const peakPlayingRooms = Math.max(
    safeInt(live.playingRooms),
    ...trend.map((item) => safeInt(item.peakRooms)),
    ...snapshotTrend.map((item) => safeInt(item.playingRooms))
  );

  const peakDay = trend
    .slice()
    .sort((a, b) => b.peakVisitors - a.peakVisitors || b.peakPlayers - a.peakPlayers)
    .at(0) || null;

  return {
    generatedAtMs: nowMs,
    range,
    snapshot: {
      live,
      summary: {
        activeDays,
        currentOnlineUsers: safeInt(live.onlineUsers),
        currentInGameUsers: safeInt(live.onlineInGameUsers),
        currentPlayingRooms: safeInt(live.playingRooms),
        currentWaitingRooms: safeInt(live.waitingRooms),
        currentActiveRooms: safeInt(live.activeRooms),
        peakVisitors,
        peakPlayers,
        peakPlayingRooms,
        avgDailyPeakVisitors,
        avgDailyPeakPlayers,
        peakDayLabel: String(peakDay?.label || ""),
      },
      trend,
      snapshotTrend,
      peakMoments,
      hourOfDay,
      weekday,
      snapshotsCoverage: {
        startMs: snapshotsStartMs,
        endMs: range.endMs || nowMs,
        limitedToRecentWindow: snapshotsStartMs > safeSignedInt(range.startMs),
      },
    },
  };
}

exports.getPresenceAnalyticsSnapshot = publicOnCall(
  "getPresenceAnalyticsSnapshot",
  async (request) => {
    assertFinanceAdmin(request);
    const payload = request.data && typeof request.data === "object" ? request.data : {};
    return computePresenceAnalyticsSnapshot(payload);
  },
  {
    memory: "512MiB",
  }
);

exports.getDuelAnalyticsSnapshot = publicOnCall(
  "getDuelAnalyticsSnapshot",
  async (request) => {
    assertFinanceAdmin(request);
    const payload = request.data && typeof request.data === "object" ? request.data : {};
    const snapshot = await computeDuelAnalyticsSnapshot(payload);
    return {
      ok: true,
      snapshot,
    };
  },
  {
    memory: "1GiB",
  }
);

exports.getMorpionAnalyticsSnapshot = publicOnCall(
  "getMorpionAnalyticsSnapshot",
  async (request) => {
    assertFinanceAdmin(request);
    const payload = request.data && typeof request.data === "object" ? request.data : {};
    const snapshot = await computeMorpionAnalyticsSnapshot(payload);
    return {
      ok: true,
      snapshot,
    };
  },
  {
    memory: "1GiB",
  }
);

exports.getRecruitmentAnalyticsSnapshot = publicOnCall("getRecruitmentAnalyticsSnapshot", async (request) => {
  assertFinanceAdmin(request);

  const [campaignSnap, applicationsSnap] = await Promise.all([
    db.collection(ANALYTICS_META_COLLECTION).doc(RECRUITMENT_CAMPAIGN_DOC).get(),
    db.collection(RECRUITMENT_APPLICATIONS_COLLECTION)
      .orderBy("createdAtMs", "desc")
      .limit(100)
      .get(),
  ]);

  const campaignData = campaignSnap.exists ? (campaignSnap.data() || {}) : {};
  const campaign = buildRecruitmentCampaignSnapshot(campaignData);
  const pageVisitCount = safeInt(campaignData.pageVisitCount);
  const recentApplications = applicationsSnap.docs.map((docSnap) => {
    const data = docSnap.data() || {};
    return {
      id: docSnap.id,
      applicationCode: sanitizeText(data.applicationCode || "", 40),
      fullName: sanitizeText(data.fullName || "", 180),
      sex: normalizeRecruitmentSex(data.sex || ""),
      phone: sanitizePhone(data.phone || "", 40),
      fullAddress: sanitizeText(data.fullAddress || "", 220),
      currentPosition: sanitizeText(data.currentPosition || "", 120),
      networkReach: safeInt(data.networkReach),
      motivationLetter: sanitizeText(data.motivationLetter || "", 2500),
      status: sanitizeText(data.status || "", 24).toLowerCase() || "pending",
      createdAtMs: safeInt(data.createdAtMs),
    };
  });

  const applicationsCount = safeInt(campaign.applicationsCount);
  return {
    ok: true,
    snapshot: {
      summary: {
        pageVisitCount,
        applicationsCount,
        conversionRatePct: pageVisitCount > 0 ? applicationsCount / pageVisitCount : 0,
        targetCount: campaign.targetCount,
        deadlineMs: campaign.deadlineMs,
        remainingMs: campaign.remainingMs,
        generatedAtMs: Date.now(),
      },
      recentApplications: recentApplications.slice(0, 40),
    },
  };
});

exports.getClientAcquisitionSnapshot = publicOnCall("getClientAcquisitionSnapshot", async (request) => {
  assertFinanceAdmin(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const snapshot = await computeClientAcquisitionSnapshot(payload);

  return {
    ok: true,
    snapshot,
  };
});

exports.getDepositMethodAnalyticsSnapshot = publicOnCall(
  "getDepositMethodAnalyticsSnapshot",
  async (request) => {
    assertFinanceAdmin(request);
    const payload = request.data && typeof request.data === "object" ? request.data : {};
    try {
      console.info("[DEPOSIT_ANALYTICS_DEBUG] callable:start", {
        uid: String(request?.auth?.uid || ""),
        email: String(request?.auth?.token?.email || ""),
        payload,
      });
      const snapshot = await computeDepositAnalyticsSnapshot(payload);

      return {
        ok: true,
        snapshot,
      };
    } catch (error) {
      console.error("[DEPOSIT_ANALYTICS_DEBUG] callable:error", {
        message: String(error?.message || error),
        code: String(error?.code || ""),
        details: error?.details || "",
        stack: String(error?.stack || ""),
        payload,
      });
      throw error;
    }
  },
  { invoker: "public" }
);

async function grantWelcomeBonusForClient(options = {}) {
  const uid = String(options.uid || "").trim();
  const email = sanitizeEmail(options.email || "", 160);
  const customerName = sanitizeText(options.customerName || "", 120);
  const customerPhone = sanitizePhone(options.customerPhone || "", 40);
  const depositorPhone = sanitizePhone(options.depositorPhone || "", 40);
  const proofRef = sanitizeText(options.proofRef || "auto-signup-bonus", 180) || "auto-signup-bonus";
  const methodId = sanitizeText(options.methodId || "welcome_bonus", 120) || "welcome_bonus";
  const metadata = options.metadata && typeof options.metadata === "object" ? options.metadata : {};

  if (!uid) {
    return { granted: false, reason: "invalid_uid" };
  }

  const nowIso = new Date().toISOString();
  const nowMs = Date.now();
  const orderRef = db.collection(CLIENTS_COLLECTION).doc(uid).collection("orders").doc();

  return db.runTransaction(async (tx) => {
    const [walletSnap, ordersSnap, withdrawalsSnap, xchangesSnap] = await Promise.all([
      tx.get(walletRef(uid)),
      tx.get(db.collection(CLIENTS_COLLECTION).doc(uid).collection("orders")),
      tx.get(db.collection(CLIENTS_COLLECTION).doc(uid).collection("withdrawals")),
      tx.get(walletHistoryRef(uid)),
    ]);

    const walletData = walletSnap.exists ? (walletSnap.data() || {}) : {};
    if (!walletSnap.exists) {
      return { granted: false, reason: "client_not_found" };
    }
    assertWalletNotFrozen(walletData);

    const existingOrders = ordersSnap.docs.map((item) => ({ id: item.id, ...(item.data() || {}) }));
    const existingWithdrawals = withdrawalsSnap.docs.map((item) => item.data() || {});
    const existingXchanges = xchangesSnap.docs.map((item) => item.data() || {});
    const alreadyClaimed = walletData.welcomeBonusClaimed === true
      || safeSignedInt(walletData.welcomeBonusReceivedAtMs) > 0
      || !!String(walletData.welcomeBonusOrderId || "").trim()
      || hasWelcomeBonusOrder(existingOrders);

    if (alreadyClaimed) {
      return { granted: false, reason: "already-claimed" };
    }

    const nextWallet = {
      uid,
      email: sanitizeEmail(email || walletData.email || "", 160),
      name: customerName || sanitizeText(walletData.name || String(email || "").split("@")[0] || "Player", 80),
      phone: customerPhone || sanitizePhone(walletData.phone || ""),
      welcomeBonusClaimed: true,
      welcomeBonusOrderId: orderRef.id,
      welcomeBonusReceivedAtMs: nowMs,
      welcomeBonusHtgAvailable: safeInt(walletData.welcomeBonusHtgAvailable) + WELCOME_BONUS_HTG_AMOUNT,
      welcomeBonusHtgConverted: safeInt(walletData.welcomeBonusHtgConverted),
      welcomeBonusHtgPlayed: safeInt(walletData.welcomeBonusHtgPlayed),
      pendingPlayFromWelcomeDoes: safeInt(walletData.pendingPlayFromWelcomeDoes),
      signupBonusAutoGrantedAtMs: safeInt(metadata.signupBonusAutoGrantedAtMs) || nowMs,
      signupBonusAutoGrantedHtg: safeInt(metadata.signupBonusAutoGrantedHtg) || WELCOME_BONUS_HTG_AMOUNT,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      lastSeenAt: admin.firestore.FieldValue.serverTimestamp(),
      lastSeenAtMs: nowMs,
    };

    const welcomeOrder = {
      uid,
      clientId: uid,
      clientUid: uid,
      amount: WELCOME_BONUS_HTG_AMOUNT,
      approvedAmountHtg: WELCOME_BONUS_HTG_AMOUNT,
      methodId,
      methodName: methodId === "signup_bonus_auto" ? "Bonus inscription" : "Bonus bienvenue",
      orderType: WELCOME_BONUS_ORDER_TYPE,
      kind: WELCOME_BONUS_ORDER_TYPE,
      isWelcomeBonus: true,
      autoApproved: true,
      countsAsCashIn: false,
      countsAsApprovedDeposit: false,
      countsForReferral: false,
      countsForDepositBonus: false,
      excludeFromProfit: true,
      customerName: nextWallet.name,
      customerEmail: nextWallet.email,
      customerPhone: nextWallet.phone,
      depositorPhone,
      proofRef,
      status: "approved",
      resolutionStatus: "approved",
      rejectedReason: "",
      createdAtMs: nowMs,
      createdAt: nowIso,
      updatedAtMs: nowMs,
      updatedAt: nowIso,
      resolvedAtMs: nowMs,
      approvedAtMs: nowMs,
      deviceId: sanitizeText(walletData.deviceId || "", 120),
      appVersion: sanitizeText(walletData.appVersion || "", 48),
      country: sanitizeText(walletData.country || "", 48),
      browser: sanitizeText(walletData.browser || "", 120),
      ipHash: sanitizeText(walletData.ipHash || "", 64),
      utmSource: sanitizeText(walletData.utmSource || "", 80),
      utmCampaign: sanitizeText(walletData.utmCampaign || "", 120),
      landingPage: sanitizeText(walletData.landingPage || "", 240),
      creativeId: sanitizeText(walletData.creativeId || "", 120),
      uniqueCode: `WBON-${crypto.randomBytes(3).toString("hex").toUpperCase()}-${Date.now().toString(36).toUpperCase()}`,
    };

    const fundingSnapshot = buildWalletFundingSnapshot({
      orders: [
        ...existingOrders,
        welcomeOrder,
      ],
      withdrawals: existingWithdrawals,
      walletData: nextWallet,
      exchangeHistory: existingXchanges,
    });

    tx.set(orderRef, welcomeOrder, { merge: true });
    tx.set(walletRef(uid), {
      ...buildFundingWalletPatch(fundingSnapshot),
      welcomeBonusClaimed: true,
      welcomeBonusOrderId: orderRef.id,
      welcomeBonusReceivedAtMs: nowMs,
      pendingPlayFromWelcomeDoes: safeInt(nextWallet.pendingPlayFromWelcomeDoes),
      signupBonusAutoGrantedAtMs: safeInt(nextWallet.signupBonusAutoGrantedAtMs),
      signupBonusAutoGrantedHtg: safeInt(nextWallet.signupBonusAutoGrantedHtg),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      lastSeenAt: admin.firestore.FieldValue.serverTimestamp(),
      lastSeenAtMs: nowMs,
    }, { merge: true });

    return {
      granted: true,
      reason: "granted",
      orderId: orderRef.id,
      welcomeBonusHtgGranted: WELCOME_BONUS_HTG_AMOUNT,
      fundingSnapshot,
    };
  });
}

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
  const welcomeBonusPromptStatusInput = normalizeWelcomeBonusPromptStatus(payload.welcomeBonusPromptStatus || "");
  const completeWelcomeBonusTutorial = payload.welcomeBonusTutorialCompleted === true;
  const markSignupBonusModalSeen = payload.signupBonusModalSeen === true;
  const context = sanitizeAnalyticsContext(payload, request);
  const ref = walletRef(uid);
  const snap = await ref.get();
  const current = snap.exists ? (snap.data() || {}) : {};
  const isNewProfile = !snap.exists;
  let referralCode = normalizeCode(current.referralCode || "");
  const currentWelcomeBonusPromptStatus = normalizeWelcomeBonusPromptStatus(current.welcomeBonusPromptStatus || "");
  const currentWelcomeBonusPromptAnsweredAtMs = safeInt(current.welcomeBonusPromptAnsweredAtMs);

  if (!referralCode) {
    referralCode = await generateUniqueClientReferralCode(uid);
  }

  let nextWelcomeBonusPromptStatus = currentWelcomeBonusPromptStatus || (isNewProfile ? "pending" : "");
  let nextWelcomeBonusPromptAnsweredAtMs = currentWelcomeBonusPromptAnsweredAtMs;
  let nextWelcomeBonusProofCode = sanitizeText(current.welcomeBonusProofCode || "", 80).toUpperCase();
  let nextWelcomeBonusTutorialCompletedAtMs = safeInt(current.welcomeBonusTutorialCompletedAtMs);
  let nextSignupBonusModalSeenAtMs = safeSignedInt(current.signupBonusModalSeenAtMs);
  if (
    (welcomeBonusPromptStatusInput === "accepted" || welcomeBonusPromptStatusInput === "declined")
    && nextWelcomeBonusPromptStatus !== "accepted"
    && nextWelcomeBonusPromptStatus !== "declined"
  ) {
    nextWelcomeBonusPromptStatus = welcomeBonusPromptStatusInput;
    nextWelcomeBonusPromptAnsweredAtMs = Date.now();
    if (welcomeBonusPromptStatusInput === "accepted" && !nextWelcomeBonusProofCode) {
      nextWelcomeBonusProofCode = generateWelcomeBonusProofCode(uid);
    }
  } else if (welcomeBonusPromptStatusInput === "pending" && !nextWelcomeBonusPromptStatus) {
    nextWelcomeBonusPromptStatus = "pending";
  }
  if (completeWelcomeBonusTutorial && nextWelcomeBonusTutorialCompletedAtMs <= 0) {
    nextWelcomeBonusTutorialCompletedAtMs = Date.now();
  }
  if (markSignupBonusModalSeen && nextSignupBonusModalSeenAtMs <= 0) {
    nextSignupBonusModalSeenAtMs = Date.now();
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
    welcomeBonusPromptStatus: nextWelcomeBonusPromptStatus,
    welcomeBonusPromptAnsweredAtMs: nextWelcomeBonusPromptAnsweredAtMs,
    welcomeBonusProofCode: nextWelcomeBonusProofCode,
    welcomeBonusTutorialCompletedAtMs: nextWelcomeBonusTutorialCompletedAtMs,
    signupBonusModalSeenAtMs: nextSignupBonusModalSeenAtMs,
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
    profile.pendingPlayFromWelcomeDoes = safeInt(current.pendingPlayFromWelcomeDoes);
    profile.totalExchangedHtgEver = safeInt(current.totalExchangedHtgEver);
    profile.welcomeBonusClaimed = current.welcomeBonusClaimed === true;
    profile.welcomeBonusOrderId = sanitizeText(current.welcomeBonusOrderId || "", 160);
    profile.welcomeBonusReceivedAtMs = safeInt(current.welcomeBonusReceivedAtMs);
    profile.welcomeBonusHtgAvailable = safeInt(current.welcomeBonusHtgAvailable);
    profile.welcomeBonusHtgConverted = safeInt(current.welcomeBonusHtgConverted);
    profile.welcomeBonusHtgPlayed = safeInt(current.welcomeBonusHtgPlayed);
    profile.signupBonusAutoGrantedAtMs = safeInt(current.signupBonusAutoGrantedAtMs);
    profile.signupBonusAutoGrantedHtg = safeInt(current.signupBonusAutoGrantedHtg);
    profile.signupBonusModalSeenAtMs = nextSignupBonusModalSeenAtMs;
    profile.referralSignupsTotal = safeInt(current.referralSignupsTotal);
    profile.referralSignupsViaLink = safeInt(current.referralSignupsViaLink);
    profile.referralSignupsViaCode = safeInt(current.referralSignupsViaCode);
    profile.referralDepositsTotal = safeInt(current.referralDepositsTotal);
  } else {
    if (typeof current.referralSignupsTotal !== "number") profile.referralSignupsTotal = safeInt(current.referralSignupsTotal);
    if (typeof current.referralSignupsViaLink !== "number") profile.referralSignupsViaLink = safeInt(current.referralSignupsViaLink);
    if (typeof current.referralSignupsViaCode !== "number") profile.referralSignupsViaCode = safeInt(current.referralSignupsViaCode);
    if (typeof current.referralDepositsTotal !== "number") profile.referralDepositsTotal = safeInt(current.referralDepositsTotal);
    if (typeof current.pendingPlayFromWelcomeDoes !== "number") profile.pendingPlayFromWelcomeDoes = safeInt(current.pendingPlayFromWelcomeDoes);
    if (typeof current.welcomeBonusReceivedAtMs !== "number") profile.welcomeBonusReceivedAtMs = safeInt(current.welcomeBonusReceivedAtMs);
    if (typeof current.welcomeBonusHtgAvailable !== "number") profile.welcomeBonusHtgAvailable = safeInt(current.welcomeBonusHtgAvailable);
    if (typeof current.welcomeBonusHtgConverted !== "number") profile.welcomeBonusHtgConverted = safeInt(current.welcomeBonusHtgConverted);
    if (typeof current.welcomeBonusHtgPlayed !== "number") profile.welcomeBonusHtgPlayed = safeInt(current.welcomeBonusHtgPlayed);
    if (typeof current.signupBonusAutoGrantedAtMs !== "number") profile.signupBonusAutoGrantedAtMs = safeInt(current.signupBonusAutoGrantedAtMs);
    if (typeof current.signupBonusAutoGrantedHtg !== "number") profile.signupBonusAutoGrantedHtg = safeInt(current.signupBonusAutoGrantedHtg);
    if (typeof current.signupBonusModalSeenAtMs !== "number" || nextSignupBonusModalSeenAtMs > 0) {
      profile.signupBonusModalSeenAtMs = nextSignupBonusModalSeenAtMs;
    }
    if (current.welcomeBonusClaimed !== true) profile.welcomeBonusClaimed = false;
    if (!current.welcomeBonusOrderId) profile.welcomeBonusOrderId = sanitizeText(current.welcomeBonusOrderId || "", 160);
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

  let signupBonusGrant = { granted: false, reason: "already-claimed" };
  if (isNewProfile) {
    signupBonusGrant = await grantWelcomeBonusForClient({
      uid,
      email,
      methodId: "signup_bonus_auto",
      customerName: profile.name,
      customerPhone: profile.phone,
      depositorPhone: profile.phone,
      proofRef: "auto-signup-bonus",
      metadata: {
        signupBonusAutoGrantedAtMs: Date.now(),
        signupBonusAutoGrantedHtg: WELCOME_BONUS_HTG_AMOUNT,
      },
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
      referredByAgentUid: sanitizeText(finalProfile.referredByAgentUid || "", 160),
      referredByAgentCode: normalizeCode(finalProfile.referredByAgentCode || ""),
      welcomeBonusPromptStatus: normalizeWelcomeBonusPromptStatus(finalProfile.welcomeBonusPromptStatus || profile.welcomeBonusPromptStatus || ""),
      welcomeBonusPromptAnsweredAtMs: safeInt(finalProfile.welcomeBonusPromptAnsweredAtMs || profile.welcomeBonusPromptAnsweredAtMs),
      welcomeBonusProofCode: sanitizeText(finalProfile.welcomeBonusProofCode || profile.welcomeBonusProofCode || "", 80).toUpperCase(),
      welcomeBonusTutorialCompletedAtMs: safeInt(finalProfile.welcomeBonusTutorialCompletedAtMs || profile.welcomeBonusTutorialCompletedAtMs),
      signupBonusAutoGrantedAtMs: safeInt(finalProfile.signupBonusAutoGrantedAtMs || profile.signupBonusAutoGrantedAtMs),
      signupBonusAutoGrantedHtg: safeInt(finalProfile.signupBonusAutoGrantedHtg || profile.signupBonusAutoGrantedHtg),
      signupBonusModalSeenAtMs: safeInt(finalProfile.signupBonusModalSeenAtMs || profile.signupBonusModalSeenAtMs),
      updatedAt: new Date().toISOString(),
    },
    referralApplied: referralBootstrap.applied === true,
    referralReason: String(referralBootstrap.reason || ""),
    signupBonusGranted: signupBonusGrant.granted === true,
    signupBonusReason: String(signupBonusGrant.reason || ""),
  };
}, { minInstances: 1 });

function normalizeDepositOcrSearchText(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase();
}

function isLikelyDepositIdCandidate(value) {
  const token = String(value || "")
    .trim()
    .replace(/^[^A-Z0-9]+|[^A-Z0-9]+$/g, "");
  if (!token) return false;
  if (/^\d{5,}$/.test(token)) return true;
  if (!/^[A-Z0-9-]{5,}$/.test(token)) return false;
  return /[A-Z]/.test(token) || /\d{4,}/.test(token);
}

function extractDepositIdFromOcrText(value) {
  const text = normalizeDepositOcrSearchText(value);
  if (!text) return "";

  const patterns = [
    /(?:TRANSACTION|TRANS|REFERENCE|REF|IDENTIFIANT|IDENTIFIANT DE TRANSACTION|ID)\s*(?:NO|N0|NUMERO|NUM|NUMBER|#|:|-)?\s*([A-Z0-9-]{5,})/g,
    /(?:NO|N0|NUMERO|NUM|NUMBER|#)\s*(?:DE\s+)?(?:TRANSACTION|TRANS|REFERENCE|REF|IDENTIFIANT|ID)\s*[:\-]?\s*([A-Z0-9-]{5,})/g,
    /(?:ID|REF)[\s:.-]*([A-Z0-9-]{5,})/g,
  ];

  for (const pattern of patterns) {
    let match;
    while ((match = pattern.exec(text)) !== null) {
      const candidate = String(match[1] || "").trim();
      if (isLikelyDepositIdCandidate(candidate)) {
        return candidate;
      }
    }
  }

  return "";
}
exports.createOrderSecure = publicOnCall("createOrderSecure", async (request) => {
  const { uid, email } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const methodId = sanitizeText(payload.methodId || "", 120);
  const amountHtg = safeInt(payload.amountHtg);
  const customerName = sanitizeText(payload.customerName || "", 120);
  const customerEmail = sanitizeEmail(payload.customerEmail || email || "", 160) || sanitizeEmail(email || "", 160);
  const customerPhone = sanitizePhone(payload.customerPhone || "", 40);
  const depositorPhone = sanitizePhone(payload.depositorPhone || "", 40);
  const proofRef = sanitizeText(payload.proofRef || "", 180);
  const proofStepDurationMs = safeInt(payload.proofStepDurationMs);
  const extractedText = sanitizeText(payload.extractedText || "", MAX_PUBLIC_TEXT_LENGTH);
  const extractedTextStatus = ["pending", "success", "empty", "failed"].includes(String(payload.extractedTextStatus || ""))
    ? String(payload.extractedTextStatus)
    : "pending";
  const extractedProofId = extractDepositIdFromOcrText(extractedText);

  if (!methodId || amountHtg < MIN_ORDER_HTG || !customerName || !proofRef) {
    throw new HttpsError("invalid-argument", "Commande invalide.");
  }

  if (!extractedProofId) {
    console.info("[DEPOSIT_GUARD_DEBUG][FUNCTIONS] createOrderSecure:missing_proof_id", {
      uid,
      methodId,
      amountHtg,
      proofRef,
      extractedTextStatus,
      extractedTextLength: extractedText.length,
    });
    throw new HttpsError(
      "failed-precondition",
      "Cette image ne passe pas nos mesures de securite. Veuillez contacter un agent.",
      { code: "deposit-proof-security-check-failed" },
    );
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
  return db.runTransaction(async (tx) => {
    const [clientSnap, ordersSnap, withdrawalsSnap, gameHistorySnap] = await Promise.all([
      tx.get(clientRef),
      tx.get(db.collection(CLIENTS_COLLECTION).doc(uid).collection("orders")),
      tx.get(db.collection(CLIENTS_COLLECTION).doc(uid).collection("withdrawals")),
      tx.get(walletHistoryRef(uid).where("type", "==", "game_entry").limit(1)),
    ]);
    const clientData = clientSnap.exists ? (clientSnap.data() || {}) : {};
    assertWalletNotFrozen(clientData);
    const existingOrders = ordersSnap.docs.map((item) => item.data() || {});
    const hasRealApprovedDeposit = hasRealApprovedDepositFromOrders(existingOrders);
    const hasPlayedGame = !gameHistorySnap.empty;
    const priorShadowGuard = normalizeDepositShadowGuard(clientData.depositShadowGuard);
    const withinShadowWindow = priorShadowGuard.windowStartedAtMs > 0
      && (nowMs - priorShadowGuard.windowStartedAtMs) <= DEPOSIT_SHADOW_GUARD_WINDOW_MS;
    const lockActive = priorShadowGuard.lockedUntilMs > nowMs;
    const rapidAttempt = proofStepDurationMs > 0 && proofStepDurationMs < DEPOSIT_PROOF_MIN_DELAY_MS;
    const nextRapidAttemptCount = rapidAttempt
      ? ((withinShadowWindow ? priorShadowGuard.rapidAttemptCount : 0) + 1)
      : 0;
    const shouldActivateLock = rapidAttempt
      && !hasRealApprovedDeposit
      && nextRapidAttemptCount >= DEPOSIT_SHADOW_GUARD_RAPID_THRESHOLD;
    const shouldShadowDrop = !hasRealApprovedDeposit && (lockActive || shouldActivateLock);

    let nextShadowGuard = {
      windowStartedAtMs: 0,
      lastAttemptAtMs: nowMs,
      rapidAttemptCount: 0,
      lastProofStepDurationMs: proofStepDurationMs,
      lockedUntilMs: 0,
    };

    if (!hasRealApprovedDeposit) {
      if (shouldActivateLock) {
        nextShadowGuard = {
          windowStartedAtMs: withinShadowWindow ? priorShadowGuard.windowStartedAtMs : nowMs,
          lastAttemptAtMs: nowMs,
          rapidAttemptCount: nextRapidAttemptCount,
          lastProofStepDurationMs: proofStepDurationMs,
          lockedUntilMs: nowMs + DEPOSIT_SHADOW_GUARD_LOCK_MS,
        };
      } else if (lockActive) {
        nextShadowGuard = {
          windowStartedAtMs: priorShadowGuard.windowStartedAtMs,
          lastAttemptAtMs: nowMs,
          rapidAttemptCount: priorShadowGuard.rapidAttemptCount,
          lastProofStepDurationMs: proofStepDurationMs,
          lockedUntilMs: priorShadowGuard.lockedUntilMs,
        };
      } else if (rapidAttempt) {
        nextShadowGuard = {
          windowStartedAtMs: withinShadowWindow ? priorShadowGuard.windowStartedAtMs : nowMs,
          lastAttemptAtMs: nowMs,
          rapidAttemptCount: nextRapidAttemptCount,
          lastProofStepDurationMs: proofStepDurationMs,
          lockedUntilMs: 0,
        };
      }
    }

    console.info("[DEPOSIT_GUARD_DEBUG][FUNCTIONS] createOrderSecure:decision", {
      uid,
      proofStepDurationMs,
      rapidAttempt,
      nextRapidAttemptCount,
      windowStartedAtMs: priorShadowGuard.windowStartedAtMs,
      withinShadowWindow,
      lockActive,
      lockedUntilMs: priorShadowGuard.lockedUntilMs,
      hasRealApprovedDeposit,
      hasPlayedGame,
      shouldActivateLock,
      shouldShadowDrop,
      ordersCount: ordersSnap.size,
      withdrawalsCount: withdrawalsSnap.size,
      methodId,
      amountHtg,
    });

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
      depositorPhone,
      extractedText,
      extractedTextStatus,
      extractedProofId,
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
      depositShadowGuard: nextShadowGuard,
      lastSeenAt: admin.firestore.FieldValue.serverTimestamp(),
      lastSeenAtMs: nowMs,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      ...(clientSnap.exists ? {} : { createdAt: admin.firestore.FieldValue.serverTimestamp() }),
    };

    if (shouldShadowDrop) {
      console.info("[DEPOSIT_GUARD_DEBUG][FUNCTIONS] createOrderSecure:shadow_drop", {
        uid,
        proofStepDurationMs,
        nextRapidAttemptCount,
        lockActive,
        lockedUntilMs: nextShadowGuard.lockedUntilMs,
      });
      tx.set(clientRef, nextWallet, { merge: true });
      return {
        ok: true,
        orderId: "",
        status: "pending",
        creditedProvisionally: false,
        message: "Votre demande est en cours de vérification.",
      };
    }

    if (provisionalDepositsEnabled) {
      const fundingSnapshot = buildWalletFundingSnapshot({
        orders: [
          ...existingOrders,
          orderData,
        ],
        withdrawals: withdrawalsSnap.docs.map((item) => item.data() || {}),
        walletData: clientData,
      });
      Object.assign(nextWallet, buildFundingWalletPatch(fundingSnapshot));
    }

    tx.set(clientRef, nextWallet, { merge: true });
    tx.set(orderRef, orderData, { merge: true });
    console.info("[DEPOSIT_GUARD_DEBUG][FUNCTIONS] createOrderSecure:order_created", {
      uid,
      orderId: orderRef.id,
      proofStepDurationMs,
      rapidAttempt,
      nextRapidAttemptCount,
      provisionalDepositsEnabled,
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
});

exports.claimWelcomeBonusSecure = publicOnCall("claimWelcomeBonusSecure", async (request) => {
  const { uid, email } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const customerName = sanitizeText(payload.customerName || "", 120);
  const customerPhone = sanitizePhone(payload.customerPhone || "", 40);
  const depositorPhone = sanitizePhone(payload.depositorPhone || "", 40);
  const proofRef = sanitizeText(payload.proofRef || "", 180);
  const methodId = sanitizeText(payload.methodId || "welcome_bonus", 120) || "welcome_bonus";

  if (!proofRef) {
    throw new HttpsError("invalid-argument", "Preuve bienvenue requise.");
  }

  const result = await grantWelcomeBonusForClient({
    uid,
    email,
    customerName,
    customerPhone,
    depositorPhone,
    proofRef,
    methodId,
  });

  if (result.granted !== true) {
    throw new HttpsError(
      "failed-precondition",
      result.reason === "already-claimed"
        ? "Bonus bienvenue déjà réclamé."
        : "Ce compte n'est pas éligible au bonus de bienvenue.",
      {
        code: result.reason === "already-claimed"
          ? "welcome-bonus-already-claimed"
          : "welcome-bonus-not-eligible",
        reason: result.reason,
      }
    );
  }

  return {
    ok: true,
    orderId: String(result.orderId || ""),
    welcomeBonusHtgGranted: WELCOME_BONUS_HTG_AMOUNT,
    welcomeBonusClaimed: true,
    ...(result.fundingSnapshot || {}),
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
  if (isWelcomeBonusOrder(data)) return;
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
  const clientRequestId = sanitizeText(payload.requestId || payload.clientRequestId || "", 120);

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

  const result = await db.runTransaction(async (tx) => {
    const clientRef = walletRef(uid);
    const withdrawalsCollectionRef = clientRef.collection("withdrawals");
    const newWithdrawalRef = withdrawalsCollectionRef.doc();
    const [ordersSnap, withdrawalsSnap, clientSnap, xchangesSnap] = await Promise.all([
      tx.get(clientRef.collection("orders")),
      tx.get(withdrawalsCollectionRef),
      tx.get(clientRef),
      tx.get(walletHistoryRef(uid)),
    ]);

    const clientData = clientSnap.exists ? (clientSnap.data() || {}) : {};
    assertWithdrawalAllowed(clientData);

    const existingWithdrawals = withdrawalsSnap.docs.map((item) => ({
      id: item.id,
      data: item.data() || {},
    }));

    if (clientRequestId) {
      const duplicate = existingWithdrawals.find((item) => String(item.data?.clientRequestId || "") === clientRequestId);
      if (duplicate) {
        return {
          ok: true,
          duplicate: true,
          withdrawalId: duplicate.id,
          status: String(duplicate.data?.status || "pending"),
        };
      }
    }

    const walletSummary = buildWalletFundingSnapshot({
      orders: ordersSnap.docs.map((item) => item.data() || {}),
      withdrawals: existingWithdrawals.map((item) => item.data),
      walletData: clientData,
      exchangeHistory: xchangesSnap.docs.map((item) => item.data() || {}),
    });
    const available = walletSummary.withdrawableHtg;

    console.log("[WITHDRAWAL_DEBUG] summary", JSON.stringify({
      uid,
      requestedAmount,
      available,
      destinationType,
      clientRequestId,
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
        clientRequestId,
        walletSummary,
      }));
      throw new HttpsError("failed-precondition", "Montant supérieur au solde disponible.");
    }

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
      clientRequestId: clientRequestId || newWithdrawalRef.id,
      createdAt: nowIso,
      updatedAt: nowIso,
    };

    tx.set(newWithdrawalRef, withdrawalPayload, { merge: true });

    const nextFundingSnapshot = buildWalletFundingSnapshot({
      orders: ordersSnap.docs.map((item) => item.data() || {}),
      withdrawals: [
        ...existingWithdrawals.map((item) => item.data),
        withdrawalPayload,
      ],
      walletData: clientData,
      exchangeHistory: xchangesSnap.docs.map((item) => item.data() || {}),
    });

    tx.set(clientRef, {
      uid,
      email,
      name: customerName || sanitizeText(String(email || "").split("@")[0], 80) || "Player",
      phone: customerPhone,
      ...buildFundingWalletPatch(nextFundingSnapshot),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });

    return {
      ok: true,
      duplicate: false,
      withdrawalId: newWithdrawalRef.id,
      status: "pending",
    };
  });

  return result;
});

exports.cancelWithdrawalSecure = publicOnCall("cancelWithdrawalSecure", async (request) => {
  const { uid, email } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const withdrawalId = sanitizeText(payload.withdrawalId || payload.id || "", 160);

  if (!withdrawalId) {
    throw new HttpsError("invalid-argument", "Retrait introuvable.");
  }

  const clientRef = walletRef(uid);
  const withdrawalRef = clientRef.collection("withdrawals").doc(withdrawalId);

  const result = await db.runTransaction(async (tx) => {
    const [withdrawalSnap, clientSnap, ordersSnap, withdrawalsSnap, xchangesSnap] = await Promise.all([
      tx.get(withdrawalRef),
      tx.get(clientRef),
      tx.get(clientRef.collection("orders")),
      tx.get(clientRef.collection("withdrawals")),
      tx.get(walletHistoryRef(uid)),
    ]);

    if (!withdrawalSnap.exists) {
      throw new HttpsError("not-found", "Demande de retrait introuvable.");
    }

    const clientData = clientSnap.exists ? (clientSnap.data() || {}) : {};
    const withdrawalData = withdrawalSnap.data() || {};
    const currentStatus = getWithdrawalStatus(withdrawalData);

    console.info("[WITHDRAWAL_CANCEL_DEBUG][FUNCTIONS] start", {
      uid,
      withdrawalId,
      currentStatus,
      docStatus: String(withdrawalData.status || ""),
      docResolutionStatus: String(withdrawalData.resolutionStatus || ""),
      requestedAmount: safeInt(withdrawalData.requestedAmount ?? withdrawalData.amount),
      allWithdrawals: withdrawalsSnap.docs.map((item) => ({
        id: item.id,
        status: String(item.data()?.status || ""),
        resolutionStatus: String(item.data()?.resolutionStatus || ""),
        requestedAmount: safeInt(item.data()?.requestedAmount ?? item.data()?.amount),
      })),
    });

    if (
      currentStatus === "cancelled"
      || currentStatus === "canceled"
      || (currentStatus === "rejected" && String(withdrawalData.cancelledBy || "").trim().toLowerCase() === "client")
    ) {
      return {
        ok: true,
        alreadyCancelled: true,
        withdrawalId,
        status: "rejected",
      };
    }

    if (!isWithdrawalClientCancellableStatus(currentStatus)) {
      throw new HttpsError("failed-precondition", "Ce retrait ne peut plus être annulé.");
    }

    const nowMs = Date.now();
    const nowIso = new Date(nowMs).toISOString();
    const nextWithdrawals = withdrawalsSnap.docs.map((item) => {
      if (item.id !== withdrawalId) return item.data() || {};
      return {
        ...(item.data() || {}),
        status: "rejected",
        resolutionStatus: "rejected",
        rejectedReason: "Retrait annulé par le client",
        cancelledBy: "client",
        cancelledAtMs: nowMs,
        cancelledAt: nowIso,
        updatedAt: nowIso,
      };
    });

    const nextFundingSnapshot = buildWalletFundingSnapshot({
      orders: ordersSnap.docs.map((item) => item.data() || {}),
      withdrawals: nextWithdrawals,
      walletData: clientData,
      exchangeHistory: xchangesSnap.docs.map((item) => item.data() || {}),
    });

    console.info("[WITHDRAWAL_CANCEL_DEBUG][FUNCTIONS] recompute", {
      uid,
      withdrawalId,
      nextWithdrawals: nextWithdrawals.map((item) => ({
        id: String(item?.id || item?.withdrawalId || ""),
        status: String(item?.status || ""),
        resolutionStatus: String(item?.resolutionStatus || ""),
        requestedAmount: safeInt(item?.requestedAmount ?? item?.amount),
      })),
      nextFundingSnapshot: {
        reservedWithdrawalsHtg: safeInt(nextFundingSnapshot.reservedWithdrawalsHtg),
        approvedBaseHtg: safeInt(nextFundingSnapshot.approvedBaseHtg),
        approvedHtgAvailable: safeInt(nextFundingSnapshot.approvedHtgAvailable),
        withdrawableHtg: safeInt(nextFundingSnapshot.withdrawableHtg),
      },
    });

    tx.set(withdrawalRef, {
      status: "rejected",
      resolutionStatus: "rejected",
      rejectedReason: "Retrait annulé par le client",
      cancelledBy: "client",
      cancelledAtMs: nowMs,
      cancelledAt: nowIso,
      updatedAt: nowIso,
      customerEmail: sanitizeEmail(email || "", 160),
    }, { merge: true });

    tx.set(clientRef, {
      uid,
      email,
      ...buildFundingWalletPatch(nextFundingSnapshot),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });

    return {
      ok: true,
      alreadyCancelled: false,
      withdrawalId,
      status: "rejected",
      ...nextFundingSnapshot,
    };
  });

  return result;
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

exports.ackClientFinanceNoticeSecure = publicOnCall("ackClientFinanceNoticeSecure", async (request) => {
  const { uid } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const kind = String(payload.kind || "").trim();
  const id = sanitizeText(payload.id || "", 160);
  const noticeKey = sanitizeText(payload.noticeKey || "", 240);
  const status = sanitizeText(payload.status || "", 80);

  if (!id || !noticeKey || (kind !== "order" && kind !== "withdrawal")) {
    throw new HttpsError("invalid-argument", "Accusé de notification invalide.");
  }

  const subcollection = kind === "withdrawal" ? "withdrawals" : "orders";
  const ref = db.collection(CLIENTS_COLLECTION).doc(uid).collection(subcollection).doc(id);
  const nowIso = new Date().toISOString();
  const nowMs = Date.now();

  await ref.set({
    clientStatusNoticeSeenKey: noticeKey,
    clientStatusNoticeSeenStatus: status || "",
    clientStatusNoticeSeenAt: nowIso,
    clientStatusNoticeSeenAtMs: nowMs,
    updatedAt: nowIso,
  }, { merge: true });

  return {
    ok: true,
    kind,
    id,
    noticeKey,
    seenAtMs: nowMs,
  };
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
  const welcomeBonusEligibility = resolveWelcomeBonusEligibility({
    walletData,
    orders: ordersSnap.docs.map((item) => item.data() || {}),
    fundingSnapshot,
  });

  return {
    ...fundingSnapshot,
    welcomeBonusClaimed: walletData.welcomeBonusClaimed === true,
    welcomeBonusOrderId: String(walletData.welcomeBonusOrderId || ""),
    welcomeBonusReceivedAtMs: safeInt(walletData.welcomeBonusReceivedAtMs),
    welcomeBonusPromptStatus: normalizeWelcomeBonusPromptStatus(walletData.welcomeBonusPromptStatus || ""),
    welcomeBonusPromptAnsweredAtMs: safeInt(walletData.welcomeBonusPromptAnsweredAtMs),
    welcomeBonusProofCode: sanitizeText(walletData.welcomeBonusProofCode || "", 80).toUpperCase(),
    welcomeBonusTutorialCompletedAtMs: safeInt(walletData.welcomeBonusTutorialCompletedAtMs),
    welcomeBonusEligible: welcomeBonusEligibility.eligible === true,
    welcomeBonusEligibilityReason: String(welcomeBonusEligibility.reason || ""),
    welcomeBonusLaunchAtMs: safeSignedInt(welcomeBonusEligibility.launchAtMs),
    welcomeBonusEndAtMs: safeSignedInt(welcomeBonusEligibility.endAtMs),
    welcomeBonusOfferEnded: welcomeBonusEligibility.offerEnded === true,
    isLegacyAccount: welcomeBonusEligibility.isLegacyAccount === true,
    accountFrozen: walletData.accountFrozen === true,
    freezeReason: String(walletData.freezeReason || ""),
    withdrawalHold: walletData.withdrawalHold === true,
    withdrawalHoldReason: String(walletData.withdrawalHoldReason || ""),
    withdrawalHoldAtMs: safeInt(walletData.withdrawalHoldAtMs),
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

exports.searchAgentDepositClientsSecure = publicOnCall(
  "searchAgentDepositClientsSecure",
  async (request) => {
    assertFinanceAdmin(request);
    const payload = request.data && typeof request.data === "object" ? request.data : {};
    const rawQuery = sanitizeText(payload.query || "", 160);
    const normalizedQuery = normalizeSearchText(rawQuery);
    const queryDigits = phoneDigits(rawQuery);
    const queryUsername = sanitizeUsername(rawQuery || "", 24);
    const queryEmail = sanitizeEmail(rawQuery || "", 160);
    const results = new Map();

    if (!rawQuery) {
      return { ok: true, results: [] };
    }

    const addClientSnap = (docSnap) => {
      if (!docSnap?.exists) return;
      results.set(docSnap.id, buildAgentDepositSearchRecord(docSnap.id, docSnap.data() || {}));
    };

    const addClientDocs = (snap) => {
      (snap?.docs || []).forEach((docSnap) => addClientSnap(docSnap));
    };

    if (rawQuery.length >= 20 && /^[A-Za-z0-9_-]+$/.test(rawQuery)) {
      addClientSnap(await walletRef(rawQuery).get());
    }

    const exactLookups = [];
    if (queryEmail) {
      exactLookups.push(
        db.collection(CLIENTS_COLLECTION).where("email", "==", queryEmail).limit(6).get()
      );
    }
    if (queryUsername) {
      exactLookups.push(
        db.collection(CLIENTS_COLLECTION).where("username", "==", queryUsername).limit(6).get()
      );
    }
    if (queryDigits.length >= 8) {
      const sanitizedPhone = sanitizePhone(rawQuery, 40);
      exactLookups.push(
        db.collection(CLIENTS_COLLECTION).where("phone", "==", sanitizedPhone).limit(6).get()
      );
    }

    if (exactLookups.length) {
      const exactSnaps = await Promise.allSettled(exactLookups);
      exactSnaps.forEach((entry) => {
        if (entry.status === "fulfilled") {
          addClientDocs(entry.value);
        }
      });
    }

    if (results.size < AGENT_DEPOSIT_SEARCH_RESULT_LIMIT && normalizedQuery.length >= 2) {
      let fallbackSnap = null;
      try {
        fallbackSnap = await db.collection(CLIENTS_COLLECTION)
          .orderBy("lastSeenAtMs", "desc")
          .limit(AGENT_DEPOSIT_SEARCH_FALLBACK_LIMIT)
          .get();
      } catch (_) {
        fallbackSnap = await db.collection(CLIENTS_COLLECTION)
          .limit(AGENT_DEPOSIT_SEARCH_FALLBACK_LIMIT)
          .get();
      }

      (fallbackSnap?.docs || []).forEach((docSnap) => {
        if (results.size >= AGENT_DEPOSIT_SEARCH_RESULT_LIMIT) return;
        const raw = docSnap.data() || {};
        const haystack = [
          docSnap.id,
          raw.uid,
          raw.name,
          raw.displayName,
          raw.username,
          raw.email,
          raw.phone,
        ]
          .map((value) => normalizeSearchText(value))
          .filter(Boolean)
          .join(" ");

        const phoneHaystack = [
          phoneDigits(raw.phone || ""),
          phoneDigits(raw.customerPhone || ""),
        ].filter(Boolean).join(" ");

        const match = haystack.includes(normalizedQuery)
          || (queryDigits.length >= 4 && phoneHaystack.includes(queryDigits));
        if (match) {
          addClientSnap(docSnap);
        }
      });
    }

    const sorted = Array.from(results.values())
      .sort((left, right) =>
        safeSignedInt(right.lastSeenAtMs) - safeSignedInt(left.lastSeenAtMs)
        || safeSignedInt(right.createdAtMs) - safeSignedInt(left.createdAtMs)
        || String(left.name || left.email || left.id).localeCompare(String(right.name || right.email || right.id), "fr")
      )
      .slice(0, AGENT_DEPOSIT_SEARCH_RESULT_LIMIT);

    return {
      ok: true,
      query: rawQuery,
      results: sorted,
    };
  },
  { invoker: "public" }
);

exports.getAgentDepositClientContextSecure = publicOnCall(
  "getAgentDepositClientContextSecure",
  async (request) => {
    assertFinanceAdmin(request);
    const payload = request.data && typeof request.data === "object" ? request.data : {};
    const clientId = sanitizeText(payload.clientId || payload.uid || "", 160);
    if (!clientId) {
      throw new HttpsError("invalid-argument", "Client introuvable.");
    }

    const clientRef = walletRef(clientId);
    const clientSnap = await clientRef.get();
    if (!clientSnap.exists) {
      throw new HttpsError("not-found", "Compte client introuvable.");
    }

    let ordersSnap = null;
    try {
      ordersSnap = await clientRef.collection("orders")
        .orderBy("createdAtMs", "desc")
        .limit(AGENT_DEPOSIT_CONTEXT_ORDER_LIMIT)
        .get();
    } catch (_) {
      ordersSnap = await clientRef.collection("orders")
        .limit(AGENT_DEPOSIT_CONTEXT_ORDER_LIMIT)
        .get();
    }

    const client = buildAgentDepositSearchRecord(clientSnap.id, clientSnap.data() || {});
    const recentOrders = (ordersSnap?.docs || [])
      .map((docSnap) => buildAgentDepositContextOrder(docSnap))
      .sort((left, right) => safeSignedInt(right.createdAtMs) - safeSignedInt(left.createdAtMs));

    return {
      ok: true,
      client,
      recentOrders,
    };
  },
  { invoker: "public" }
);

exports.creditAgentDepositSecure = publicOnCall(
  "creditAgentDepositSecure",
  async (request) => {
    const { uid: agentUid, email: agentEmail } = assertFinanceAdmin(request);
    const payload = request.data && typeof request.data === "object" ? request.data : {};
    const clientId = sanitizeText(payload.clientId || payload.uid || "", 160);
    const amountHtg = safeInt(payload.amountHtg);
    const note = sanitizeText(payload.note || "", 240);
    const requestedMethodId = sanitizeText(payload.methodId || AGENT_ASSISTED_METHOD_ID, 80).toLowerCase();

    if (!clientId || amountHtg < MIN_ORDER_HTG) {
      throw new HttpsError("invalid-argument", "Crédit agent invalide.");
    }

    const clientRef = walletRef(clientId);
    const methodMetaBase = getAgentDepositMethodMeta(requestedMethodId);
    let methodMeta = methodMetaBase;

    if (methodMetaBase.id !== AGENT_ASSISTED_METHOD_ID) {
      const methodSnap = await db.collection("paymentMethods").doc(methodMetaBase.id).get();
      if (methodSnap.exists) {
        methodMeta = getAgentDepositMethodMeta(methodMetaBase.id, methodSnap.data() || {});
      }
    }

    const result = await db.runTransaction(async (tx) => {
      const [clientSnap, ordersSnap, withdrawalsSnap, xchangesSnap] = await Promise.all([
        tx.get(clientRef),
        tx.get(clientRef.collection("orders")),
        tx.get(clientRef.collection("withdrawals")),
        tx.get(walletHistoryRef(clientId)),
      ]);

      if (!clientSnap.exists) {
        throw new HttpsError("not-found", "Compte client introuvable.");
      }

      const clientData = clientSnap.data() || {};
      const nowMs = Date.now();
      const nowIso = new Date(nowMs).toISOString();
      const orderRef = clientRef.collection("orders").doc();
      const depositBonusSnapshot = computeDepositBonusSnapshot(amountHtg);
      const bonusDoesAwarded = safeInt(depositBonusSnapshot.bonusDoes);
      const beforeApprovedDoes = safeInt(
        typeof clientData.doesApprovedBalance === "number"
          ? clientData.doesApprovedBalance
          : Math.max(0, safeInt(clientData.doesBalance) - safeInt(clientData.doesProvisionalBalance))
      );
      const beforeProvisionalDoes = safeInt(clientData.doesProvisionalBalance);
      const beforeExchangeableDoes = safeInt(
        typeof clientData.exchangeableDoesAvailable === "number"
          ? Math.min(clientData.exchangeableDoesAvailable, beforeApprovedDoes)
          : beforeApprovedDoes
      );
      const beforePendingFromXchange = safeInt(clientData.pendingPlayFromXchangeDoes);
      const beforePendingFromReferral = safeInt(clientData.pendingPlayFromReferralDoes);
      const beforePendingFromWelcome = safeInt(clientData.pendingPlayFromWelcomeDoes);

      const orderData = {
        uid: clientId,
        clientId,
        clientUid: clientId,
        amount: amountHtg,
        methodId: methodMeta.id,
        methodName: methodMeta.name,
        methodDetails: {
          name: methodMeta.name,
          accountName: methodMeta.accountName,
          phoneNumber: methodMeta.phoneNumber,
        },
        status: "approved",
        resolutionStatus: "approved",
        approvedAmountHtg: amountHtg,
        uniqueCode: `AGT-${crypto.randomBytes(4).toString("hex").toUpperCase()}-${Date.now().toString(36).toUpperCase()}`,
        proofRef: `agent_credit_${nowMs}`,
        customerName: sanitizeText(clientData.name || clientData.displayName || clientData.username || "", 120),
        customerEmail: sanitizeEmail(clientData.email || "", 160),
        customerPhone: sanitizePhone(clientData.phone || "", 40),
        depositorPhone: "",
        extractedText: "",
        extractedTextStatus: "agent_assisted",
        createdAtMs: nowMs,
        createdAt: nowIso,
        updatedAt: nowIso,
        updatedAtMs: nowMs,
        resolvedAtMs: nowMs,
        reviewResolvedAtMs: nowMs,
        approvedAtMs: nowMs,
        approvedAt: nowIso,
        fundingSettledAtMs: nowMs,
        source: AGENT_ASSISTED_METHOD_ID,
        agentAssisted: true,
        creditedByAgentUid: agentUid,
        creditedByAgentEmail: sanitizeEmail(agentEmail || "", 160),
        creditedAtMs: nowMs,
        creditedAt: nowIso,
        adminNote: note,
        bonusEligible: depositBonusSnapshot.eligible,
        bonusThresholdHtg: safeInt(depositBonusSnapshot.thresholdHtg),
        bonusPercent: safeInt(depositBonusSnapshot.bonusPercent),
        bonusRateHtgToDoes: safeInt(depositBonusSnapshot.rateHtgToDoes),
        bonusHtgBasis: amountHtg,
        bonusHtgRaw: Number(depositBonusSnapshot.bonusHtgRaw || 0),
        bonusDoesAwarded,
        bonusAwardedAtMs: bonusDoesAwarded > 0 ? nowMs : 0,
        bonusAwardedAt: bonusDoesAwarded > 0 ? nowIso : "",
        bonusSettledAtMs: bonusDoesAwarded > 0 ? nowMs : 0,
        clientStatusNoticeEventAtMs: nowMs,
      };

      const nextOrders = [
        ...ordersSnap.docs.map((item) => item.data() || {}),
        orderData,
      ];

      const nextWallet = {
        ...clientData,
        uid: clientId,
        email: sanitizeEmail(clientData.email || "", 160),
        doesApprovedBalance: beforeApprovedDoes + bonusDoesAwarded,
        doesProvisionalBalance: beforeProvisionalDoes,
        doesBalance: beforeApprovedDoes + beforeProvisionalDoes + bonusDoesAwarded,
        pendingPlayFromXchangeDoes: beforePendingFromXchange,
        pendingPlayFromReferralDoes: beforePendingFromReferral + bonusDoesAwarded,
        pendingPlayFromWelcomeDoes: beforePendingFromWelcome,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        hasApprovedDeposit: true,
      };

      const pendingTotalAfter = safeInt(nextWallet.pendingPlayFromXchangeDoes)
        + safeInt(nextWallet.pendingPlayFromReferralDoes)
        + safeInt(nextWallet.pendingPlayFromWelcomeDoes);
      nextWallet.exchangeableDoesAvailable = pendingTotalAfter <= 0
        ? safeInt(nextWallet.doesApprovedBalance)
        : Math.min(safeInt(nextWallet.doesApprovedBalance), beforeExchangeableDoes);

      const fundingSnapshot = buildWalletFundingSnapshot({
        orders: nextOrders,
        withdrawals: withdrawalsSnap.docs.map((item) => item.data() || {}),
        walletData: nextWallet,
        exchangeHistory: xchangesSnap.docs.map((item) => item.data() || {}),
      });

      await trackAgentDepositApprovalTx(tx, {
        clientUid: clientId,
        approvedAtMs: nowMs,
        orderId: orderRef.id,
        amountHtg,
      });

      tx.set(orderRef, orderData, { merge: true });
      tx.set(clientRef, {
        ...buildFundingWalletPatch(fundingSnapshot),
        doesApprovedBalance: safeInt(nextWallet.doesApprovedBalance),
        doesProvisionalBalance: safeInt(nextWallet.doesProvisionalBalance),
        doesBalance: safeInt(nextWallet.doesBalance),
        exchangeableDoesAvailable: safeInt(nextWallet.exchangeableDoesAvailable),
        pendingPlayFromXchangeDoes: safeInt(nextWallet.pendingPlayFromXchangeDoes),
        pendingPlayFromReferralDoes: safeInt(nextWallet.pendingPlayFromReferralDoes),
        pendingPlayFromWelcomeDoes: safeInt(nextWallet.pendingPlayFromWelcomeDoes),
        hasApprovedDeposit: true,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        lastSeenAt: admin.firestore.FieldValue.serverTimestamp(),
        lastSeenAtMs: nowMs,
      }, { merge: true });

      return {
        ok: true,
        orderId: orderRef.id,
        uid: clientId,
        amountHtg,
        methodId: methodMeta.id,
        methodName: methodMeta.name,
        bonusEligible: depositBonusSnapshot.eligible,
        bonusDoesAwarded,
        ...fundingSnapshot,
      };
    });

    return result;
  },
  { invoker: "public" }
);

exports.searchAgentCandidatesSecure = publicOnCall(
  "searchAgentCandidatesSecure",
  async (request) => {
    assertFinanceAdmin(request);
    const payload = request.data && typeof request.data === "object" ? request.data : {};
    const rawQuery = sanitizeText(payload.query || "", 160);
    const normalizedQuery = normalizeSearchText(rawQuery);
    const queryDigits = phoneDigits(rawQuery);
    const queryUsername = sanitizeUsername(rawQuery || "", 24);
    const queryEmail = sanitizeEmail(rawQuery || "", 160);
    const results = new Map();

    if (!rawQuery) {
      return { ok: true, results: [] };
    }

    const addClientSnap = (docSnap) => {
      if (!docSnap?.exists) return;
      results.set(docSnap.id, buildAgentSearchRecord(docSnap.id, docSnap.data() || {}));
    };

    const addClientDocs = (snap) => {
      (snap?.docs || []).forEach((docSnap) => addClientSnap(docSnap));
    };

    if (rawQuery.length >= 20 && /^[A-Za-z0-9_-]+$/.test(rawQuery)) {
      addClientSnap(await walletRef(rawQuery).get());
    }

    const exactLookups = [];
    if (queryEmail) {
      exactLookups.push(
        db.collection(CLIENTS_COLLECTION).where("email", "==", queryEmail).limit(6).get()
      );
    }
    if (queryUsername) {
      exactLookups.push(
        db.collection(CLIENTS_COLLECTION).where("username", "==", queryUsername).limit(6).get()
      );
    }
    if (queryDigits.length >= 8) {
      const sanitizedPhone = sanitizePhone(rawQuery, 40);
      exactLookups.push(
        db.collection(CLIENTS_COLLECTION).where("phone", "==", sanitizedPhone).limit(6).get()
      );
    }

    if (exactLookups.length) {
      const exactSnaps = await Promise.allSettled(exactLookups);
      exactSnaps.forEach((entry) => {
        if (entry.status === "fulfilled") {
          addClientDocs(entry.value);
        }
      });
    }

    if (results.size < AGENT_SEARCH_RESULT_LIMIT && normalizedQuery.length >= 2) {
      let fallbackSnap = null;
      try {
        fallbackSnap = await db.collection(CLIENTS_COLLECTION)
          .orderBy("lastSeenAtMs", "desc")
          .limit(AGENT_DEPOSIT_SEARCH_FALLBACK_LIMIT)
          .get();
      } catch (_) {
        fallbackSnap = await db.collection(CLIENTS_COLLECTION)
          .limit(AGENT_DEPOSIT_SEARCH_FALLBACK_LIMIT)
          .get();
      }

      (fallbackSnap?.docs || []).forEach((docSnap) => {
        if (results.size >= AGENT_SEARCH_RESULT_LIMIT) return;
        const raw = docSnap.data() || {};
        const haystack = [
          docSnap.id,
          raw.uid,
          raw.name,
          raw.displayName,
          raw.username,
          raw.email,
          raw.phone,
        ]
          .map((value) => normalizeSearchText(value))
          .filter(Boolean)
          .join(" ");

        const phoneHaystack = phoneDigits(raw.phone || "");
        const match = haystack.includes(normalizedQuery)
          || (queryDigits.length >= 4 && phoneHaystack.includes(queryDigits));
        if (match) {
          addClientSnap(docSnap);
        }
      });
    }

    const sorted = Array.from(results.values())
      .sort((left, right) =>
        safeSignedInt(right.lastSeenAtMs) - safeSignedInt(left.lastSeenAtMs)
        || safeSignedInt(right.createdAtMs) - safeSignedInt(left.createdAtMs)
        || String(left.name || left.email || left.id).localeCompare(String(right.name || right.email || right.id), "fr")
      )
      .slice(0, AGENT_SEARCH_RESULT_LIMIT);

    return {
      ok: true,
      query: rawQuery,
      results: sorted,
    };
  },
  { invoker: "public" }
);

exports.upsertAgentSecure = publicOnCall(
  "upsertAgentSecure",
  async (request) => {
    const { uid: adminUid, email: adminEmail } = assertFinanceAdmin(request);
    const payload = request.data && typeof request.data === "object" ? request.data : {};
    const clientId = sanitizeText(payload.clientId || payload.uid || "", 160);
    const requestedStatus = normalizeAgentStatus(payload.status || payload.agentStatus || "");
    const manualPromoCode = normalizeCode(payload.promoCode || "");

    if (!clientId) {
      throw new HttpsError("invalid-argument", "Utilisateur introuvable.");
    }

    const clientRef = walletRef(clientId);
    const targetAgentRef = agentRef(clientId);
    const [clientSnap, existingAgentSnap] = await Promise.all([
      clientRef.get(),
      targetAgentRef.get(),
    ]);

    if (!clientSnap.exists) {
      throw new HttpsError("not-found", "Compte client introuvable.");
    }

    const clientData = clientSnap.data() || {};
    const existingAgentData = existingAgentSnap.exists ? (existingAgentSnap.data() || {}) : {};

    if (manualPromoCode && await agentPromoCodeExists(manualPromoCode, clientId)) {
      throw new HttpsError("already-exists", "Ce code agent existe déjà.");
    }

    let promoCode = manualPromoCode
      || normalizeCode(existingAgentData.promoCode || clientData.agentPromoCode || "");
    if (!promoCode) {
      promoCode = await generateUniqueAgentPromoCode(clientId);
    }

    const nowMs = Date.now();
    const existingDeclaredAtMs = safeSignedInt(existingAgentData.declaredAtMs || clientData.agentDeclaredAtMs);
    const existingActivatedAtMs = safeSignedInt(existingAgentData.activatedAtMs || clientData.agentActivatedAtMs);
    const declaredAtMs = existingDeclaredAtMs > 0 ? existingDeclaredAtMs : nowMs;
    const activatedAtMs = requestedStatus === "active"
      ? (existingActivatedAtMs > 0 ? existingActivatedAtMs : nowMs)
      : existingActivatedAtMs;

    await db.runTransaction(async (tx) => {
      const [liveClientSnap, liveAgentSnap] = await Promise.all([
        tx.get(clientRef),
        tx.get(targetAgentRef),
      ]);
      if (!liveClientSnap.exists) {
        throw new HttpsError("not-found", "Compte client introuvable.");
      }

      const liveClientData = liveClientSnap.data() || {};
      const liveAgentData = liveAgentSnap.exists ? (liveAgentSnap.data() || {}) : {};
      const wasActivatedAtMs = safeSignedInt(liveAgentData.activatedAtMs || liveClientData.agentActivatedAtMs);
      const isFirstActivation = requestedStatus === "active" && wasActivatedAtMs <= 0;
      const displayName = sanitizeText(
        liveClientData.name
        || liveClientData.displayName
        || liveClientData.username
        || String(liveClientData.email || "").split("@")[0]
        || "Agent",
        120
      );
      const nextInitialBudget = resolveAgentSignupBudgetInitialHtg(liveAgentData, liveClientData) > 0
        ? resolveAgentSignupBudgetInitialHtg(liveAgentData, liveClientData)
        : (isFirstActivation ? AGENT_INITIAL_SIGNUP_BUDGET_HTG : 0);
      const nextRemainingBudget = isFirstActivation
        ? AGENT_INITIAL_SIGNUP_BUDGET_HTG
        : resolveAgentSignupBudgetRemainingHtg(liveAgentData, liveClientData);

      tx.set(targetAgentRef, {
        uid: clientId,
        displayName,
        status: requestedStatus,
        promoCode,
        createdAt: liveAgentSnap.exists ? liveAgentData.createdAt || admin.firestore.FieldValue.serverTimestamp() : admin.firestore.FieldValue.serverTimestamp(),
        createdAtMs: safeSignedInt(liveAgentData.createdAtMs) || nowMs,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        updatedAtMs: nowMs,
        declaredAtMs,
        declaredByUid: sanitizeText(liveAgentData.declaredByUid || adminUid, 160) || adminUid,
        declaredByEmail: sanitizeEmail(liveAgentData.declaredByEmail || adminEmail || "", 160),
        activatedAtMs,
        signupBudgetInitialHtg: nextInitialBudget,
        signupBudgetRemainingHtg: nextRemainingBudget,
        currentMonthEarnedDoes: safeInt(liveAgentData.currentMonthEarnedDoes),
        currentEarningsMonthKey: sanitizeText(liveAgentData.currentEarningsMonthKey || "", 16),
        lifetimeEarnedDoes: safeInt(liveAgentData.lifetimeEarnedDoes),
        totalTrackedSignups: safeInt(liveAgentData.totalTrackedSignups),
        totalTrackedDeposits: safeInt(liveAgentData.totalTrackedDeposits),
        totalTrackedWins: safeInt(liveAgentData.totalTrackedWins),
        totalTrackedLosses: safeInt(liveAgentData.totalTrackedLosses),
        lastPayrollMonthKey: sanitizeText(liveAgentData.lastPayrollMonthKey || "", 16),
      }, { merge: true });

      if (isFirstActivation) {
        tx.set(targetAgentRef.collection(AGENT_LEDGER_SUBCOLLECTION).doc("activation_credit"), {
          type: "activation_credit",
          label: "Credit initial budget agent",
          deltaDoes: 0,
          deltaHtg: AGENT_INITIAL_SIGNUP_BUDGET_HTG,
          monthKey: getMonthKeyFromMs(nowMs),
          createdAt: admin.firestore.FieldValue.serverTimestamp(),
          createdAtMs: nowMs,
          createdByUid: adminUid,
          createdByEmail: sanitizeEmail(adminEmail || "", 160),
          linkedAgentUid: clientId,
          signupBudgetRemainingHtg: nextRemainingBudget,
        }, { merge: true });
      }

      tx.set(clientRef, {
        isAgent: true,
        agentStatus: requestedStatus,
        agentPromoCode: promoCode,
        agentDashboardEnabled: true,
        agentDeclaredAtMs: declaredAtMs,
        agentDeclaredByUid: adminUid,
        agentDeclaredByEmail: sanitizeEmail(adminEmail || "", 160),
        agentActivatedAtMs: activatedAtMs,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        updatedAtMs: nowMs,
      }, { merge: true });
    });

    const [finalClientSnap, finalAgentSnap] = await Promise.all([
      clientRef.get(),
      targetAgentRef.get(),
    ]);

    return {
      ok: true,
      agent: buildAgentProfileSummary(
        clientId,
        finalClientSnap.exists ? (finalClientSnap.data() || {}) : {},
        finalAgentSnap.exists ? (finalAgentSnap.data() || {}) : {}
      ),
    };
  },
  { invoker: "public" }
);

exports.listAgentsSecure = publicOnCall(
  "listAgentsSecure",
  async (request) => {
    assertFinanceAdmin(request);
    const payload = request.data && typeof request.data === "object" ? request.data : {};
    const statusFilter = String(payload.status || "all").trim().toLowerCase();
    const rawQuery = sanitizeText(payload.query || "", 160);
    const normalizedQuery = normalizeSearchText(rawQuery);

    let agentsSnap = null;
    try {
      agentsSnap = await agentsCollection()
        .orderBy("updatedAtMs", "desc")
        .limit(AGENT_LIST_LIMIT)
        .get();
    } catch (_) {
      agentsSnap = await agentsCollection()
        .limit(AGENT_LIST_LIMIT)
        .get();
    }

    const agentDocs = agentsSnap?.docs || [];
    const clientSnaps = await Promise.all(agentDocs.map((docSnap) => walletRef(docSnap.id).get()));

    const items = agentDocs
      .map((docSnap, index) => buildAgentProfileSummary(
        docSnap.id,
        clientSnaps[index]?.exists ? (clientSnaps[index].data() || {}) : {},
        docSnap.data() || {}
      ))
      .filter((item) => {
        if (statusFilter !== "all" && item.status !== statusFilter) return false;
        if (!normalizedQuery) return true;
        const haystack = [
          item.uid,
          item.displayName,
          item.username,
          item.email,
          item.phone,
          item.promoCode,
        ]
          .map((value) => normalizeSearchText(value))
          .filter(Boolean)
          .join(" ");
        return haystack.includes(normalizedQuery);
      })
      .sort((left, right) =>
        safeSignedInt(right.updatedAtMs) - safeSignedInt(left.updatedAtMs)
        || safeSignedInt(right.activatedAtMs) - safeSignedInt(left.activatedAtMs)
        || String(left.displayName || left.uid).localeCompare(String(right.displayName || right.uid), "fr")
      );

    return {
      ok: true,
      items,
      total: items.length,
    };
  },
  { invoker: "public" }
);

exports.getMyAgentDashboardSecure = publicOnCall(
  "getMyAgentDashboardSecure",
  async (request) => {
    const { uid } = assertAuth(request);
    const targetAgentRef = agentRef(uid);
    const [clientSnap, agentSnap] = await Promise.all([
      walletRef(uid).get(),
      targetAgentRef.get(),
    ]);

    const clientData = clientSnap.exists ? (clientSnap.data() || {}) : {};
    if (!agentSnap.exists && clientData.isAgent !== true) {
      throw new HttpsError("permission-denied", "Ce compte n'a pas accès au dashboard agent.");
    }

    const agentData = agentSnap.exists ? (agentSnap.data() || {}) : {};
    let ledgerSnap = null;
    try {
      ledgerSnap = await targetAgentRef.collection(AGENT_LEDGER_SUBCOLLECTION)
        .orderBy("createdAtMs", "desc")
        .limit(24)
        .get();
    } catch (_) {
      ledgerSnap = await targetAgentRef.collection(AGENT_LEDGER_SUBCOLLECTION)
        .limit(24)
        .get();
    }

    let statementsSnap = null;
    try {
      statementsSnap = await targetAgentRef.collection(AGENT_MONTHLY_STATEMENTS_SUBCOLLECTION)
        .orderBy("monthKey", "desc")
        .limit(18)
        .get();
    } catch (_) {
      statementsSnap = await targetAgentRef.collection(AGENT_MONTHLY_STATEMENTS_SUBCOLLECTION)
        .limit(18)
        .get();
    }

    const referralsSnap = await db.collection(CLIENTS_COLLECTION)
      .where("referredByAgentUid", "==", uid)
      .limit(60)
      .get()
      .catch(() => null);

    const monthlyStatements = (statementsSnap?.docs || [])
      .map((docSnap) => buildAgentMonthlyStatement(docSnap))
      .sort((left, right) => String(right.monthKey || "").localeCompare(String(left.monthKey || "")));

    const trend = monthlyStatements
      .slice()
      .sort((left, right) => String(left.monthKey || "").localeCompare(String(right.monthKey || "")))
      .map((item) => ({
        label: item.monthKey,
        earnedDoes: safeInt(item.earnedDoes),
        signupsCount: safeInt(item.signupsCount),
        paidDoes: safeInt(item.paidDoes),
      }));

    const recentLedger = (ledgerSnap?.docs || [])
      .map((docSnap) => buildAgentLedgerItem(docSnap))
      .sort((left, right) => safeSignedInt(right.createdAtMs) - safeSignedInt(left.createdAtMs));

    const recentReferrals = (referralsSnap?.docs || [])
      .map((docSnap) => buildAgentReferralClientRecord(docSnap))
      .sort((left, right) => safeSignedInt(right.createdAtMs) - safeSignedInt(left.createdAtMs))
      .slice(0, 24);

    const summary = buildAgentProfileSummary(uid, clientData, agentData);

    return {
      ok: true,
      agent: summary,
      recentLedger,
      monthlyStatements,
      recentReferrals,
      trend,
    };
  },
  { invoker: "public" }
);

exports.getAgentPayrollSnapshotSecure = publicOnCall(
  "getAgentPayrollSnapshotSecure",
  async (request) => {
    assertFinanceAdmin(request);
    const payload = request.data && typeof request.data === "object" ? request.data : {};
    const monthKey = normalizeMonthKey(payload.monthKey || "", getPreviousMonthKeyFromMs(Date.now()));

    let agentsSnap = null;
    try {
      agentsSnap = await agentsCollection()
        .orderBy("updatedAtMs", "desc")
        .limit(AGENT_LIST_LIMIT)
        .get();
    } catch (_) {
      agentsSnap = await agentsCollection()
        .limit(AGENT_LIST_LIMIT)
        .get();
    }

    const agentDocs = agentsSnap?.docs || [];
    const [clientSnaps, statementSnaps] = await Promise.all([
      Promise.all(agentDocs.map((docSnap) => walletRef(docSnap.id).get())),
      Promise.all(agentDocs.map((docSnap) => docSnap.ref.collection(AGENT_MONTHLY_STATEMENTS_SUBCOLLECTION).doc(monthKey).get())),
    ]);

    const items = agentDocs
      .map((docSnap, index) => {
        const summary = buildAgentProfileSummary(
          docSnap.id,
          clientSnaps[index]?.exists ? (clientSnaps[index].data() || {}) : {},
          docSnap.data() || {}
        );
        const statement = statementSnaps[index]?.exists
          ? buildAgentMonthlyStatement(statementSnaps[index])
          : {
              monthKey,
              earnedDoes: 0,
              paidDoes: 0,
              signupsCount: 0,
              depositsCount: 0,
              winsCount: 0,
              closedAtMs: 0,
            };
        const payableDoes = Math.max(0, safeInt(statement.earnedDoes) - safeInt(statement.paidDoes));
        return {
          uid: summary.uid,
          displayName: summary.displayName,
          promoCode: summary.promoCode,
          status: summary.status,
          monthKey,
          earnedDoes: safeInt(statement.earnedDoes),
          paidDoes: safeInt(statement.paidDoes),
          payableDoes,
          signupsCount: safeInt(statement.signupsCount),
          depositsCount: safeInt(statement.depositsCount),
          winsCount: safeInt(statement.winsCount),
          closedAtMs: safeSignedInt(statement.closedAtMs),
        };
      })
      .filter((item) => item.earnedDoes > 0 || item.paidDoes > 0 || item.signupsCount > 0 || item.depositsCount > 0 || item.winsCount > 0)
      .sort((left, right) =>
        safeInt(right.payableDoes) - safeInt(left.payableDoes)
        || safeInt(right.earnedDoes) - safeInt(left.earnedDoes)
        || String(left.displayName || left.uid).localeCompare(String(right.displayName || right.uid), "fr")
      );

    return {
      ok: true,
      monthKey,
      items,
      totals: {
        agents: items.length,
        earnedDoes: items.reduce((sum, item) => sum + safeInt(item.earnedDoes), 0),
        paidDoes: items.reduce((sum, item) => sum + safeInt(item.paidDoes), 0),
        payableDoes: items.reduce((sum, item) => sum + safeInt(item.payableDoes), 0),
      },
    };
  },
  { invoker: "public" }
);

exports.getAgentProgramOverviewSecure = publicOnCall(
  "getAgentProgramOverviewSecure",
  async (request) => {
    assertFinanceAdmin(request);

    let agentsSnap = null;
    try {
      agentsSnap = await agentsCollection()
        .orderBy("updatedAtMs", "desc")
        .limit(AGENT_LIST_LIMIT)
        .get();
    } catch (_) {
      agentsSnap = await agentsCollection()
        .limit(AGENT_LIST_LIMIT)
        .get();
    }

    const monthMap = new Map();
    const currentMonthKey = getMonthKeyFromMs(Date.now());
    const agentDocs = agentsSnap?.docs || [];
    const statementSnaps = await Promise.all(
      agentDocs.map((docSnap) => docSnap.ref.collection(AGENT_MONTHLY_STATEMENTS_SUBCOLLECTION)
        .orderBy("monthKey", "desc")
        .limit(12)
        .get()
        .catch(() => null))
    );

    agentDocs.forEach((docSnap, index) => {
      const agentData = docSnap.data() || {};
      const currentEarningsMonthKey = sanitizeText(agentData.currentEarningsMonthKey || "", 16);
      if (currentEarningsMonthKey === currentMonthKey) {
        const currentEntry = monthMap.get(currentMonthKey) || {
          monthKey: currentMonthKey,
          earnedDoes: 0,
          paidDoes: 0,
          signupsCount: 0,
          depositsCount: 0,
          winsCount: 0,
          activeAgents: 0,
        };
        currentEntry.earnedDoes += safeInt(agentData.currentMonthEarnedDoes);
        currentEntry.activeAgents += normalizeAgentStatus(agentData.status || "") === "active" ? 1 : 0;
        monthMap.set(currentMonthKey, currentEntry);
      }

      (statementSnaps[index]?.docs || []).forEach((statementDoc) => {
        const item = buildAgentMonthlyStatement(statementDoc);
        const base = monthMap.get(item.monthKey) || {
          monthKey: item.monthKey,
          earnedDoes: 0,
          paidDoes: 0,
          signupsCount: 0,
          depositsCount: 0,
          winsCount: 0,
          activeAgents: 0,
        };
        base.earnedDoes += safeInt(item.earnedDoes);
        base.paidDoes += safeInt(item.paidDoes);
        base.signupsCount += safeInt(item.signupsCount);
        base.depositsCount += safeInt(item.depositsCount);
        base.winsCount += safeInt(item.winsCount);
        monthMap.set(item.monthKey, base);
      });
    });

    const timeline = Array.from(monthMap.values())
      .sort((left, right) => String(left.monthKey || "").localeCompare(String(right.monthKey || "")))
      .slice(-12)
      .map((item) => ({
        monthKey: item.monthKey,
        earnedDoes: safeInt(item.earnedDoes),
        paidDoes: safeInt(item.paidDoes),
        pendingDoes: Math.max(0, safeInt(item.earnedDoes) - safeInt(item.paidDoes)),
        signupsCount: safeInt(item.signupsCount),
        depositsCount: safeInt(item.depositsCount),
        winsCount: safeInt(item.winsCount),
        activeAgents: safeInt(item.activeAgents),
      }));

    const latest = timeline[timeline.length - 1] || null;
    return {
      ok: true,
      timeline,
      latest,
      currentMonthKey,
    };
  },
  { invoker: "public" }
);

exports.closeAgentPayrollMonthSecure = publicOnCall(
  "closeAgentPayrollMonthSecure",
  async (request) => {
    const { uid: adminUid, email: adminEmail } = assertFinanceAdmin(request);
    const payload = request.data && typeof request.data === "object" ? request.data : {};
    const monthKey = normalizeMonthKey(payload.monthKey || "", getPreviousMonthKeyFromMs(Date.now()));
    const nowMs = Date.now();
    const results = [];

    let agentsSnap = null;
    try {
      agentsSnap = await agentsCollection()
        .orderBy("updatedAtMs", "desc")
        .limit(AGENT_LIST_LIMIT)
        .get();
    } catch (_) {
      agentsSnap = await agentsCollection()
        .limit(AGENT_LIST_LIMIT)
        .get();
    }

    for (const agentDoc of agentsSnap?.docs || []) {
      const agentUid = String(agentDoc.id || "").trim();
      if (!agentUid) continue;
      const targetAgentRef = agentRef(agentUid);
      const monthlyRef = targetAgentRef.collection(AGENT_MONTHLY_STATEMENTS_SUBCOLLECTION).doc(monthKey);
      const ledgerRef = targetAgentRef.collection(AGENT_LEDGER_SUBCOLLECTION).doc(`payroll_${monthKey.replace(/[^0-9-]/g, "")}`);

      const item = await db.runTransaction(async (tx) => {
        const [agentSnap, clientSnap, monthlySnap, ledgerSnap] = await Promise.all([
          tx.get(targetAgentRef),
          tx.get(walletRef(agentUid)),
          tx.get(monthlyRef),
          tx.get(ledgerRef),
        ]);

        if (!agentSnap.exists) return null;
        const agentData = agentSnap.data() || {};
        const clientData = clientSnap.exists ? (clientSnap.data() || {}) : {};
        const monthlyData = monthlySnap.exists ? (monthlySnap.data() || {}) : {};
        const earnedDoes = safeInt(monthlyData.earnedDoes);
        const paidDoes = safeInt(monthlyData.paidDoes);
        const payableDoes = Math.max(0, earnedDoes - paidDoes);
        if (payableDoes <= 0) {
          return {
            uid: agentUid,
            displayName: sanitizeText(agentData.displayName || clientData.name || clientData.username || "", 120),
            earnedDoes,
            paidDoes,
            payableDoes: 0,
            closed: false,
          };
        }

        const nextPaidDoes = earnedDoes;
        const currentMonthEarnedDoes = safeInt(agentData.currentMonthEarnedDoes);
        const currentEarningsMonthKey = sanitizeText(agentData.currentEarningsMonthKey || "", 16);
        const nextCurrentMonthEarnedDoes = currentEarningsMonthKey === monthKey
          ? Math.max(0, currentMonthEarnedDoes - payableDoes)
          : currentMonthEarnedDoes;

        tx.set(monthlyRef, {
          monthKey,
          earnedDoes,
          paidDoes: nextPaidDoes,
          signupsCount: safeInt(monthlyData.signupsCount),
          depositsCount: safeInt(monthlyData.depositsCount),
          winsCount: safeInt(monthlyData.winsCount),
          closedAtMs: nowMs,
          closedByUid: adminUid,
          closedByEmail: sanitizeEmail(adminEmail || "", 160),
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          updatedAtMs: nowMs,
        }, { merge: true });

        tx.set(targetAgentRef, {
          currentMonthEarnedDoes: nextCurrentMonthEarnedDoes,
          lastPayrollMonthKey: monthKey,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          updatedAtMs: nowMs,
        }, { merge: true });

        if (!ledgerSnap.exists) {
          tx.set(ledgerRef, {
            type: "payroll_close",
            label: `Payroll ${monthKey}`,
            deltaDoes: -payableDoes,
            deltaHtg: 0,
            monthKey,
            createdAt: admin.firestore.FieldValue.serverTimestamp(),
            createdAtMs: nowMs,
            createdByUid: adminUid,
            createdByEmail: sanitizeEmail(adminEmail || "", 160),
            linkedAgentUid: agentUid,
            paidDoes: payableDoes,
          }, { merge: true });
        }

        return {
          uid: agentUid,
          displayName: sanitizeText(agentData.displayName || clientData.name || clientData.username || "", 120),
          earnedDoes,
          paidDoes: nextPaidDoes,
          payableDoes,
          closed: true,
        };
      });

      if (item) {
        results.push(item);
      }
    }

    const closedItems = results.filter((item) => item.closed === true);
    return {
      ok: true,
      monthKey,
      closedCount: closedItems.length,
      paidDoesTotal: closedItems.reduce((sum, item) => sum + safeInt(item.payableDoes), 0),
      items: results,
    };
  },
  { invoker: "public" }
);

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
        bonusEligible: orderData.bonusEligible === true,
        bonusPercent: safeInt(orderData.bonusPercent),
        bonusDoesAwarded: safeInt(orderData.bonusDoesAwarded),
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
    let beforePendingFromWelcome = safeInt(walletData.pendingPlayFromWelcomeDoes);
    const hasRealApprovedDepositBefore = hasRealApprovedDepositFromOrders(ordersSnap.docs.map((item) => item.data() || {}));
    if (!hasRealApprovedDepositBefore && safeInt(walletData.welcomeBonusHtgConverted) > 0 && beforePendingFromXchange <= 0 && beforePendingFromReferral <= 0) {
      beforePendingFromWelcome = Math.max(0, beforeApprovedDoes);
    }
    const beforePendingPlayTotal = beforePendingFromXchange + beforePendingFromReferral + beforePendingFromWelcome;
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

    const depositBonusSnapshot = computeDepositBonusSnapshot(orderAmountHtg);
    const existingBonusDoesAwarded = safeInt(orderData.bonusDoesAwarded);
    const bonusAlreadySettled = safeInt(orderData.bonusSettledAtMs) > 0 || existingBonusDoesAwarded > 0;

    if (decision === "approve") {
      const promoteCapitalDoes = settledCapitalDoes;
      const promoteGainDoes = settledGainDoes;
      const promoteDoes = promoteCapitalDoes + promoteGainDoes;
      const totalConvertedDoes = safeInt(orderData.provisionalHtgConverted) * RATE_HTG_TO_DOES;
      const unlockedFromPlayedDoes = Math.max(0, totalConvertedDoes - promoteCapitalDoes);
      const bonusDoesAwarded = bonusAlreadySettled ? existingBonusDoesAwarded : safeInt(depositBonusSnapshot.bonusDoes);
      const bonusAwardedAtMs = bonusDoesAwarded > 0
        ? (safeInt(orderData.bonusAwardedAtMs) || nowMs)
        : 0;
      const bonusAwardedAt = bonusAwardedAtMs > 0
        ? String(orderData.bonusAwardedAt || nowIso)
        : "";
      const unlockWelcomeOnApprovedDeposit = beforePendingFromWelcome > 0;
      nextOrder = {
        ...nextOrder,
        status: "approved",
        resolutionStatus: "approved",
        approvedAmountHtg: orderAmountHtg,
        rejectedReason: "",
        provisionalDoesRemaining: promoteCapitalDoes,
        provisionalGainDoes: promoteGainDoes,
        fundingSettledAtMs: nowMs,
        bonusEligible: depositBonusSnapshot.eligible,
        bonusThresholdHtg: safeInt(depositBonusSnapshot.thresholdHtg),
        bonusPercent: safeInt(depositBonusSnapshot.bonusPercent),
        bonusRateHtgToDoes: safeInt(depositBonusSnapshot.rateHtgToDoes),
        bonusHtgBasis: orderAmountHtg,
        bonusHtgRaw: Number(depositBonusSnapshot.bonusHtgRaw || 0),
        bonusDoesAwarded,
        bonusAwardedAtMs,
        bonusAwardedAt,
        bonusSettledAtMs: bonusDoesAwarded > 0 ? nowMs : 0,
      };
      nextWallet.doesApprovedBalance = beforeApprovedDoes + promoteDoes + bonusDoesAwarded;
      nextWallet.doesProvisionalBalance = Math.max(0, beforeProvisionalDoes - promoteDoes);
      nextWallet.doesBalance = safeInt(nextWallet.doesApprovedBalance) + safeInt(nextWallet.doesProvisionalBalance);
      nextWallet.exchangedGourdes = safeSignedInt(walletData.exchangedGourdes) + safeInt(orderData.provisionalHtgConverted);
      nextWallet.totalExchangedHtgEver = safeInt(walletData.totalExchangedHtgEver) + safeInt(orderData.provisionalHtgConverted);
      nextWallet.pendingPlayFromXchangeDoes = beforePendingFromXchange + promoteCapitalDoes;
      nextWallet.pendingPlayFromReferralDoes = beforePendingFromReferral + bonusDoesAwarded;
      nextWallet.pendingPlayFromWelcomeDoes = unlockWelcomeOnApprovedDeposit ? 0 : beforePendingFromWelcome;
      nextWallet.exchangeableDoesAvailable = beforeExchangeableDoes
        + unlockedFromPlayedDoes
        + (unlockWelcomeOnApprovedDeposit ? beforePendingFromWelcome : 0);
    } else {
      const removeDoes = settledPendingTotalDoes;
      const nextStrikeCount = safeInt(walletData.rejectedDepositStrikeCount) + 1;
      const shouldWithdrawalHold = walletData.withdrawalHold === true
        || nextStrikeCount >= ACCOUNT_FREEZE_REJECT_THRESHOLD;
      nextOrder = {
        ...nextOrder,
        status: "rejected",
        resolutionStatus: "rejected",
        approvedAmountHtg: 0,
        rejectedReason: reason || "Dépôt refusé",
        provisionalDoesRemaining: settledCapitalDoes,
        provisionalGainDoes: settledGainDoes,
        fundingSettledAtMs: nowMs,
        bonusEligible: depositBonusSnapshot.eligible,
        bonusThresholdHtg: safeInt(depositBonusSnapshot.thresholdHtg),
        bonusPercent: safeInt(depositBonusSnapshot.bonusPercent),
        bonusRateHtgToDoes: safeInt(depositBonusSnapshot.rateHtgToDoes),
        bonusHtgBasis: orderAmountHtg,
        bonusHtgRaw: Number(depositBonusSnapshot.bonusHtgRaw || 0),
        bonusDoesAwarded: 0,
        bonusAwardedAtMs: 0,
        bonusAwardedAt: "",
        bonusSettledAtMs: 0,
      };
      nextWallet.doesApprovedBalance = beforeApprovedDoes;
      nextWallet.doesProvisionalBalance = Math.max(0, beforeProvisionalDoes - removeDoes);
      nextWallet.doesBalance = safeInt(nextWallet.doesApprovedBalance) + safeInt(nextWallet.doesProvisionalBalance);
      nextWallet.rejectedDepositStrikeCount = nextStrikeCount;
      nextWallet.withdrawalHold = shouldWithdrawalHold;
      nextWallet.withdrawalHoldReason = shouldWithdrawalHold ? "3_rejected_deposits" : String(walletData.withdrawalHoldReason || "");
      nextWallet.withdrawalHoldAtMs = shouldWithdrawalHold ? nowMs : safeInt(walletData.withdrawalHoldAtMs);
      nextWallet.accountFrozen = walletData.accountFrozen === true;
      nextWallet.freezeReason = String(walletData.freezeReason || "");
      nextWallet.frozenAtMs = safeInt(walletData.frozenAtMs);
      nextWallet.pendingPlayFromXchangeDoes = beforePendingFromXchange;
      nextWallet.pendingPlayFromReferralDoes = beforePendingFromReferral;
      nextWallet.pendingPlayFromWelcomeDoes = beforePendingFromWelcome;
      nextWallet.exchangeableDoesAvailable = beforeExchangeableDoes;
    }

    if (
      (
        safeInt(nextWallet.pendingPlayFromXchangeDoes)
        + safeInt(nextWallet.pendingPlayFromReferralDoes)
        + safeInt(nextWallet.pendingPlayFromWelcomeDoes)
      ) <= 0
    ) {
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

    if (decision === "approve") {
      await trackAgentDepositApprovalTx(tx, {
        clientUid: ownerUid,
        approvedAtMs: nowMs,
        orderId,
        amountHtg: orderAmountHtg,
      });
    }

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
      pendingPlayFromWelcomeDoes: safeInt(nextWallet.pendingPlayFromWelcomeDoes),
      totalExchangedHtgEver: safeInt(nextWallet.totalExchangedHtgEver),
      hasApprovedDeposit: fundingSnapshot.hasRealApprovedDeposit === true,
      rejectedDepositStrikeCount: safeInt(nextWallet.rejectedDepositStrikeCount),
      accountFrozen: nextWallet.accountFrozen === true,
      freezeReason: String(nextWallet.freezeReason || ""),
      frozenAtMs: safeInt(nextWallet.frozenAtMs),
      withdrawalHold: nextWallet.withdrawalHold === true,
      withdrawalHoldReason: String(nextWallet.withdrawalHoldReason || ""),
      withdrawalHoldAtMs: safeInt(nextWallet.withdrawalHoldAtMs),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });

      return {
        ok: true,
        orderId,
        uid: ownerUid,
        status: nextOrder.status,
        resolutionStatus: nextOrder.resolutionStatus,
        bonusEligible: nextOrder.bonusEligible === true,
        bonusPercent: safeInt(nextOrder.bonusPercent),
        bonusDoesAwarded: safeInt(nextOrder.bonusDoesAwarded),
        ...fundingSnapshot,
        accountFrozen: nextWallet.accountFrozen === true,
        withdrawalHold: nextWallet.withdrawalHold === true,
      withdrawalHoldReason: String(nextWallet.withdrawalHoldReason || ""),
      withdrawalHoldAtMs: safeInt(nextWallet.withdrawalHoldAtMs),
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
    withdrawalHold: false,
    withdrawalHoldReason: "",
    withdrawalHoldAtMs: 0,
    rejectedDepositStrikeCount: 0,
    unfrozenAtMs: Date.now(),
    unfreezeReason: reason || "",
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  }, { merge: true });

  return {
    ok: true,
    uid,
    accountFrozen: false,
    withdrawalHold: false,
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

exports.upsertSurveySecure = publicOnCall("upsertSurveySecure", async (request) => {
  const { uid } = assertFinanceAdmin(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const surveyId = sanitizeText(payload.surveyId || "", 120);
  const ref = surveyId ? surveyRef(surveyId) : surveysCollection().doc();
  const existingSnap = surveyId ? await ref.get() : null;
  const existing = existingSnap?.exists ? (existingSnap.data() || {}) : {};
  const normalized = normalizeSurveyPayload(payload, existing);
  const nowMs = Date.now();
  const nextVersion = existingSnap?.exists ? Math.max(1, safeInt(existing.version) || 1) : 1;

  await ref.set({
    title: normalized.title,
    description: normalized.description,
    allowChoiceAnswer: normalized.allowChoiceAnswer,
    allowTextAnswer: normalized.allowTextAnswer,
    choices: normalized.choices,
    status: normalized.status === "live" ? "draft" : normalized.status,
    version: nextVersion,
    updatedByUid: uid,
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    updatedAtMs: nowMs,
    ...(existingSnap?.exists
      ? {}
      : {
          createdByUid: uid,
          createdAt: admin.firestore.FieldValue.serverTimestamp(),
          createdAtMs: nowMs,
          responseCount: 0,
          lastResponseAtMs: 0,
        }),
  }, { merge: true });

  const savedSnap = await ref.get();
  return {
    ok: true,
    survey: buildSurveySummary(savedSnap),
  };
});

exports.listSurveysSecure = publicOnCall("listSurveysSecure", async (request) => {
  assertFinanceAdmin(request);
  const snap = await surveysCollection().orderBy("updatedAt", "desc").limit(200).get();
  const surveys = snap.docs
    .map((docSnap) => buildSurveySummary(docSnap))
    .filter((survey) => survey.status !== "deleted");
  return {
    ok: true,
    surveys,
  };
});

exports.publishSurveySecure = publicOnCall("publishSurveySecure", async (request) => {
  const { uid } = assertFinanceAdmin(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const surveyId = sanitizeText(payload.surveyId || "", 120);
  if (!surveyId) {
    throw new HttpsError("invalid-argument", "Sondage introuvable.", {
      code: "survey-id-required",
    });
  }

  const targetRef = surveyRef(surveyId);
  const targetSnap = await targetRef.get();
  if (!targetSnap.exists) {
    throw new HttpsError("not-found", "Sondage introuvable.", {
      code: "survey-not-found",
    });
  }

  const current = targetSnap.data() || {};
  const normalized = normalizeSurveyPayload(current, current);
  const nowMs = Date.now();
  const batch = db.batch();
  const liveSnap = await surveysCollection().where("status", "==", "live").get();

  liveSnap.docs.forEach((docSnap) => {
    if (docSnap.id === surveyId) return;
    batch.set(docSnap.ref, {
      status: "closed",
      closedAt: admin.firestore.FieldValue.serverTimestamp(),
      closedAtMs: nowMs,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAtMs: nowMs,
    }, { merge: true });
  });

  batch.set(targetRef, {
    title: normalized.title,
    description: normalized.description,
    allowChoiceAnswer: normalized.allowChoiceAnswer,
    allowTextAnswer: normalized.allowTextAnswer,
    choices: normalized.choices,
    status: "live",
    version: Math.max(1, safeInt(current.version) || 1) + 1,
    publishedByUid: uid,
    publishedAt: admin.firestore.FieldValue.serverTimestamp(),
    publishedAtMs: nowMs,
    closedAt: admin.firestore.FieldValue.delete(),
    closedAtMs: admin.firestore.FieldValue.delete(),
    deletedAt: admin.firestore.FieldValue.delete(),
    deletedAtMs: admin.firestore.FieldValue.delete(),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    updatedAtMs: nowMs,
  }, { merge: true });

  await batch.commit();
  const savedSnap = await targetRef.get();
  return {
    ok: true,
    survey: buildSurveySummary(savedSnap),
  };
});

exports.deleteSurveySecure = publicOnCall("deleteSurveySecure", async (request) => {
  assertFinanceAdmin(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const surveyId = sanitizeText(payload.surveyId || "", 120);
  if (!surveyId) {
    throw new HttpsError("invalid-argument", "Sondage introuvable.", {
      code: "survey-id-required",
    });
  }
  const ref = surveyRef(surveyId);
  await ref.set({
    status: "deleted",
    deletedAt: admin.firestore.FieldValue.serverTimestamp(),
    deletedAtMs: Date.now(),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    updatedAtMs: Date.now(),
  }, { merge: true });
  return {
    ok: true,
    surveyId,
  };
});

exports.getSurveyResponsesSecure = publicOnCall("getSurveyResponsesSecure", async (request) => {
  assertFinanceAdmin(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const surveyId = sanitizeText(payload.surveyId || "", 120);
  if (!surveyId) {
    throw new HttpsError("invalid-argument", "Sondage introuvable.", {
      code: "survey-id-required",
    });
  }

  const [surveySnap, responsesSnap] = await Promise.all([
    surveyRef(surveyId).get(),
    surveyResponsesCollection(surveyId).orderBy("answeredAt", "desc").limit(5000).get(),
  ]);

  if (!surveySnap.exists) {
    throw new HttpsError("not-found", "Sondage introuvable.", {
      code: "survey-not-found",
    });
  }

  const survey = buildSurveySummary(surveySnap);
  const responses = responsesSnap.docs.map((docSnap) => {
    const data = docSnap.data() || {};
    return {
      id: docSnap.id,
      uid: sanitizeText(data.uid || docSnap.id, 160),
      choiceId: sanitizeText(data.choiceId || "", 80),
      choiceLabel: sanitizeText(data.choiceLabel || "", SURVEY_MAX_CHOICE_LABEL),
      textAnswer: sanitizeText(data.textAnswer || "", SURVEY_MAX_TEXT_ANSWER),
      answeredAtMs: tsFieldToMs(data.answeredAt) || safeSignedInt(data.answeredAtMs),
      clientSnapshot: {
        uid: sanitizeText(data.clientSnapshot?.uid || data.uid || docSnap.id, 160),
        displayName: sanitizeText(data.clientSnapshot?.displayName || "", 160),
        email: sanitizeEmail(data.clientSnapshot?.email || "", 160),
        phone: sanitizePhone(data.clientSnapshot?.phone || "", 40),
      },
    };
  });

  return {
    ok: true,
    survey,
    responses,
  };
});

exports.getActiveSurveyForUserSecure = publicOnCall("getActiveSurveyForUserSecure", async (request) => {
  const { uid } = assertAuth(request);
  const liveSnap = await surveysCollection().where("status", "==", "live").limit(1).get();
  if (liveSnap.empty) {
    return {
      ok: true,
      survey: null,
    };
  }

  const surveyDoc = liveSnap.docs[0];
  const responseSnap = await surveyResponseRef(surveyDoc.id, uid).get();
  if (responseSnap.exists) {
    return {
      ok: true,
      survey: null,
      answered: true,
      surveyId: surveyDoc.id,
    };
  }

  return {
    ok: true,
    answered: false,
    survey: buildSurveySummary(surveyDoc),
  };
}, { minInstances: 1 });

exports.submitSurveyResponseSecure = publicOnCall("submitSurveyResponseSecure", async (request) => {
  const { uid, email } = assertAuth(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const surveyId = sanitizeText(payload.surveyId || "", 120);
  const choiceId = sanitizeText(payload.choiceId || "", 80);
  const textAnswer = sanitizeText(payload.textAnswer || "", SURVEY_MAX_TEXT_ANSWER);
  if (!surveyId) {
    throw new HttpsError("invalid-argument", "Sondage introuvable.", {
      code: "survey-id-required",
    });
  }

  const responseRef = surveyResponseRef(surveyId, uid);
  const ref = surveyRef(surveyId);
  const clientSnapshot = await loadClientSurveySnapshot(uid, email);
  const nowMs = Date.now();

  const result = await db.runTransaction(async (tx) => {
    const [surveySnap, responseSnap] = await Promise.all([
      tx.get(ref),
      tx.get(responseRef),
    ]);

    if (!surveySnap.exists) {
      throw new HttpsError("not-found", "Sondage introuvable.", {
        code: "survey-not-found",
      });
    }
    if (responseSnap.exists) {
      throw new HttpsError("already-exists", "Tu as déjà répondu à ce sondage.", {
        code: "survey-already-answered",
      });
    }

    const surveyData = surveySnap.data() || {};
    if (normalizeSurveyStatus(surveyData.status || "draft", "draft") !== "live") {
      throw new HttpsError("failed-precondition", "Ce sondage n'est plus disponible.", {
        code: "survey-not-live",
      });
    }

    const choices = normalizeSurveyChoices(surveyData.choices || []);
    const allowChoiceAnswer = surveyData.allowChoiceAnswer !== false;
    const allowTextAnswer = surveyData.allowTextAnswer === true;
    const selectedChoice = choices.find((choice) => choice.id === choiceId) || null;

    if (allowChoiceAnswer && !allowTextAnswer && !selectedChoice) {
      throw new HttpsError("invalid-argument", "Choisis une réponse.", {
        code: "survey-choice-required",
      });
    }
    if (!allowChoiceAnswer && allowTextAnswer && !textAnswer) {
      throw new HttpsError("invalid-argument", "Ecris une réponse.", {
        code: "survey-text-required",
      });
    }
    if (allowChoiceAnswer && allowTextAnswer && !selectedChoice && !textAnswer) {
      throw new HttpsError("invalid-argument", "Choisis une réponse ou écris ton avis.", {
        code: "survey-answer-required",
      });
    }
    if (!allowTextAnswer && textAnswer) {
      throw new HttpsError("invalid-argument", "Les réponses texte ne sont pas activées.", {
        code: "survey-text-disabled",
      });
    }
    if (!allowChoiceAnswer && selectedChoice) {
      throw new HttpsError("invalid-argument", "Les réponses guidées ne sont pas activées.", {
        code: "survey-choice-disabled",
      });
    }

    tx.set(responseRef, {
      uid,
      choiceId: selectedChoice?.id || "",
      choiceLabel: selectedChoice?.label || "",
      textAnswer,
      clientSnapshot,
      answeredAt: admin.firestore.FieldValue.serverTimestamp(),
      answeredAtMs: nowMs,
    });

    tx.set(ref, {
      responseCount: admin.firestore.FieldValue.increment(1),
      lastResponseAt: admin.firestore.FieldValue.serverTimestamp(),
      lastResponseAtMs: nowMs,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAtMs: nowMs,
    }, { merge: true });

    return {
      surveyId,
      choiceId: selectedChoice?.id || "",
      choiceLabel: selectedChoice?.label || "",
      textAnswer,
    };
  });

  return {
    ok: true,
    ...result,
  };
});

exports.adminCheck = publicOnCall("adminCheck", async (request) => {
  const { uid, email } = assertFinanceAdmin(request);
  const snap = await adminBootstrapRef().get();
  const data = snap.exists ? (snap.data() || {}) : {};
  return {
    ok: true,
    uid,
    email,
    botDifficulty: await getConfiguredBotDifficulty(),
    botPilotMode: normalizeBotPilotMode(data.botPilotMode || "manual"),
    manualBotDifficulty: normalizeBotDifficulty(data.manualBotDifficulty || data.botDifficulty),
    autoBotDifficulty: normalizeBotDifficulty(data.autoBotDifficulty || data.botDifficulty),
    duelBotDifficulty: await getConfiguredDuelBotDifficulty(),
    duelBotPilotMode: normalizeBotPilotMode(data.duelBotPilotMode || "manual"),
    manualDuelBotDifficulty: normalizeBotDifficulty(data.manualDuelBotDifficulty || data.duelBotDifficulty || DEFAULT_DUEL_BOT_DIFFICULTY),
    autoDuelBotDifficulty: normalizeBotDifficulty(data.autoDuelBotDifficulty || data.duelBotDifficulty || DEFAULT_DUEL_BOT_DIFFICULTY),
  };
});

exports.setBotDifficulty = publicOnCall("setBotDifficulty", async (request) => {
  assertFinanceAdmin(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const botDifficulty = normalizeBotDifficulty(payload.botDifficulty);

  await adminBootstrapRef().set({
    email: FINANCE_ADMIN_EMAIL,
    botDifficulty,
    manualBotDifficulty: botDifficulty,
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  }, { merge: true });

  return {
    ok: true,
    botDifficulty,
  };
});

exports.setDuelBotDifficulty = publicOnCall("setDuelBotDifficulty", async (request) => {
  assertFinanceAdmin(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const botDifficulty = normalizeBotDifficulty(payload.botDifficulty || DEFAULT_DUEL_BOT_DIFFICULTY);

  await adminBootstrapRef().set({
    email: FINANCE_ADMIN_EMAIL,
    duelBotDifficulty: botDifficulty,
    manualDuelBotDifficulty: botDifficulty,
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  }, { merge: true });

  return {
    ok: true,
    botDifficulty,
  };
});

exports.getBotPilotSnapshot = publicOnCall("getBotPilotSnapshot", async (request) => {
  assertFinanceAdmin(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const nowMs = Date.now();
  const settingsSnap = await adminBootstrapRef().get();
  const settings = settingsSnap.exists ? (settingsSnap.data() || {}) : {};
  const mode = normalizeBotPilotMode(payload.mode || settings.botPilotMode || "manual");
  const windowKey = normalizeBotPilotWindow(payload.window || settings.botPilotWindow || "today");
  const snapshot = await computeBotPilotSnapshot({ nowMs, window: windowKey });
  const appliedDifficulty = mode === "auto"
    ? normalizeBotDifficulty(snapshot.recommendedLevel || settings.autoBotDifficulty || settings.botDifficulty)
    : normalizeBotDifficulty(settings.manualBotDifficulty || settings.botDifficulty);

  return {
    ok: true,
    mode,
    window: windowKey,
    manualBotDifficulty: normalizeBotDifficulty(settings.manualBotDifficulty || settings.botDifficulty),
    autoBotDifficulty: normalizeBotDifficulty(settings.autoBotDifficulty || settings.botDifficulty || snapshot.recommendedLevel),
    appliedBotDifficulty: appliedDifficulty,
    snapshot,
  };
});

exports.getDuelBotPilotSnapshot = publicOnCall("getDuelBotPilotSnapshot", async (request) => {
  assertFinanceAdmin(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const nowMs = Date.now();
  const settingsSnap = await adminBootstrapRef().get();
  const settings = settingsSnap.exists ? (settingsSnap.data() || {}) : {};
  const mode = normalizeBotPilotMode(payload.mode || settings.duelBotPilotMode || "manual");
  const windowKey = normalizeBotPilotWindow(payload.window || settings.duelBotPilotWindow || "today");
  const snapshot = await computeDuelBotPilotSnapshot({ nowMs, window: windowKey });
  const appliedDifficulty = mode === "auto"
    ? normalizeBotDifficulty(snapshot.recommendedLevel || settings.autoDuelBotDifficulty || settings.duelBotDifficulty || DEFAULT_DUEL_BOT_DIFFICULTY)
    : normalizeBotDifficulty(settings.manualDuelBotDifficulty || settings.duelBotDifficulty || DEFAULT_DUEL_BOT_DIFFICULTY);

  return {
    ok: true,
    mode,
    window: windowKey,
    manualBotDifficulty: normalizeBotDifficulty(settings.manualDuelBotDifficulty || settings.duelBotDifficulty || DEFAULT_DUEL_BOT_DIFFICULTY),
    autoBotDifficulty: normalizeBotDifficulty(settings.autoDuelBotDifficulty || settings.duelBotDifficulty || snapshot.recommendedLevel || DEFAULT_DUEL_BOT_DIFFICULTY),
    appliedBotDifficulty: appliedDifficulty,
    snapshot,
  };
});

exports.setBotPilotControl = publicOnCall("setBotPilotControl", async (request) => {
  assertFinanceAdmin(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const currentSnap = await adminBootstrapRef().get();
  const current = currentSnap.exists ? (currentSnap.data() || {}) : {};
  const mode = normalizeBotPilotMode(payload.mode || current.botPilotMode || "manual");
  const windowKey = normalizeBotPilotWindow(payload.window || current.botPilotWindow || "today");
  const manualBotDifficulty = normalizeBotDifficulty(payload.manualBotDifficulty || current.manualBotDifficulty || current.botDifficulty);

  let autoBotDifficulty = normalizeBotDifficulty(current.autoBotDifficulty || current.botDifficulty);
  let appliedBotDifficulty = manualBotDifficulty;
  let snapshot = null;

  if (mode === "auto") {
    snapshot = await computeBotPilotSnapshot({ nowMs: Date.now(), window: windowKey });
    autoBotDifficulty = normalizeBotDifficulty(snapshot.recommendedLevel || autoBotDifficulty);
    appliedBotDifficulty = autoBotDifficulty;
  }

  await adminBootstrapRef().set({
    email: FINANCE_ADMIN_EMAIL,
    botPilotMode: mode,
    botPilotWindow: windowKey,
    manualBotDifficulty,
    autoBotDifficulty,
    botDifficulty: appliedBotDifficulty,
    botPilotLastComputedAtMs: Date.now(),
    botPilotUpdatedAt: admin.firestore.FieldValue.serverTimestamp(),
    botPilotMetricsSnapshot: snapshot ? {
      window: snapshot.window,
      startMs: snapshot.startMs,
      endMs: snapshot.endMs,
      roomsCount: safeInt(snapshot.roomsCount),
      collectedDoes: safeInt(snapshot.collectedDoes),
      payoutDoes: safeInt(snapshot.payoutDoes),
      netDoes: safeSignedInt(snapshot.netDoes),
      grossCollectedDoes: safeInt(snapshot.grossCollectedDoes),
      grossPayoutDoes: safeInt(snapshot.grossPayoutDoes),
      grossNetDoes: safeSignedInt(snapshot.grossNetDoes),
      promoExposureDoes: safeInt(snapshot.promoExposureDoes),
      marginPct: Number(snapshot.marginPct || 0),
      currentEquityDoes: safeSignedInt(snapshot.currentEquityDoes),
      highWaterMarkDoes: safeSignedInt(snapshot.highWaterMarkDoes),
      drawdownDoes: safeInt(snapshot.drawdownDoes),
      drawdownPct: Number(snapshot.drawdownPct || 0),
      lastPeakAtMs: safeSignedInt(snapshot.lastPeakAtMs),
      recommendedLevel: normalizeBotDifficulty(snapshot.recommendedLevel),
      recommendedBand: String(snapshot.recommendedBand || ""),
      recommendedReason: String(snapshot.recommendedReason || ""),
      computedAtMs: Date.now(),
    } : admin.firestore.FieldValue.delete(),
  }, { merge: true });

  return {
    ok: true,
    mode,
    window: windowKey,
    manualBotDifficulty,
    autoBotDifficulty,
    appliedBotDifficulty,
    snapshot,
  };
});

exports.setDuelBotPilotControl = publicOnCall("setDuelBotPilotControl", async (request) => {
  assertFinanceAdmin(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const currentSnap = await adminBootstrapRef().get();
  const current = currentSnap.exists ? (currentSnap.data() || {}) : {};
  const mode = normalizeBotPilotMode(payload.mode || current.duelBotPilotMode || "manual");
  const windowKey = normalizeBotPilotWindow(payload.window || current.duelBotPilotWindow || "today");
  const manualBotDifficulty = normalizeBotDifficulty(payload.manualBotDifficulty || current.manualDuelBotDifficulty || current.duelBotDifficulty || DEFAULT_DUEL_BOT_DIFFICULTY);

  let autoBotDifficulty = normalizeBotDifficulty(current.autoDuelBotDifficulty || current.duelBotDifficulty || DEFAULT_DUEL_BOT_DIFFICULTY);
  let appliedBotDifficulty = manualBotDifficulty;
  let snapshot = null;

  if (mode === "auto") {
    snapshot = await computeDuelBotPilotSnapshot({ nowMs: Date.now(), window: windowKey });
    autoBotDifficulty = normalizeBotDifficulty(snapshot.recommendedLevel || autoBotDifficulty);
    appliedBotDifficulty = autoBotDifficulty;
  }

  await adminBootstrapRef().set({
    email: FINANCE_ADMIN_EMAIL,
    duelBotPilotMode: mode,
    duelBotPilotWindow: windowKey,
    manualDuelBotDifficulty: manualBotDifficulty,
    autoDuelBotDifficulty: autoBotDifficulty,
    duelBotDifficulty: appliedBotDifficulty,
    duelBotPilotLastComputedAtMs: Date.now(),
    duelBotPilotUpdatedAt: admin.firestore.FieldValue.serverTimestamp(),
    duelBotPilotMetricsSnapshot: snapshot ? {
      window: snapshot.window,
      startMs: snapshot.startMs,
      endMs: snapshot.endMs,
      roomsCount: safeInt(snapshot.roomsCount),
      collectedDoes: safeInt(snapshot.collectedDoes),
      payoutDoes: safeInt(snapshot.payoutDoes),
      netDoes: safeSignedInt(snapshot.netDoes),
      grossCollectedDoes: safeInt(snapshot.grossCollectedDoes),
      grossPayoutDoes: safeInt(snapshot.grossPayoutDoes),
      grossNetDoes: safeSignedInt(snapshot.grossNetDoes),
      promoExposureDoes: safeInt(snapshot.promoExposureDoes),
      marginPct: Number(snapshot.marginPct || 0),
      currentEquityDoes: safeSignedInt(snapshot.currentEquityDoes),
      highWaterMarkDoes: safeSignedInt(snapshot.highWaterMarkDoes),
      drawdownDoes: safeInt(snapshot.drawdownDoes),
      drawdownPct: Number(snapshot.drawdownPct || 0),
      lastPeakAtMs: safeSignedInt(snapshot.lastPeakAtMs),
      botWins: safeInt(snapshot.botWins),
      humanWins: safeInt(snapshot.humanWins),
      botWinRatePct: Number(snapshot.botWinRatePct || 0),
      humanWinRatePct: Number(snapshot.humanWinRatePct || 0),
      recommendedLevel: normalizeBotDifficulty(snapshot.recommendedLevel),
      recommendedBand: String(snapshot.recommendedBand || ""),
      recommendedReason: String(snapshot.recommendedReason || ""),
      computedAtMs: Date.now(),
    } : admin.firestore.FieldValue.delete(),
  }, { merge: true });

  return {
    ok: true,
    mode,
    window: windowKey,
    manualBotDifficulty,
    autoBotDifficulty,
    appliedBotDifficulty,
    snapshot,
  };
});

exports.refreshBotPilotAuto = onSchedule("every 10 minutes", async () => {
  const settingsSnap = await adminBootstrapRef().get();
  if (!settingsSnap.exists) return;
  const settings = settingsSnap.data() || {};
  const mode = normalizeBotPilotMode(settings.botPilotMode || "manual");
  if (mode !== "auto") return;

  const nowMs = Date.now();
  const windowKey = normalizeBotPilotWindow(settings.botPilotWindow || "today");
  const snapshot = await computeBotPilotSnapshot({ nowMs, window: windowKey });
  const autoBotDifficulty = normalizeBotDifficulty(snapshot.recommendedLevel || settings.autoBotDifficulty || settings.botDifficulty);

  await adminBootstrapRef().set({
    email: FINANCE_ADMIN_EMAIL,
    botPilotMode: "auto",
    botPilotWindow: windowKey,
    autoBotDifficulty,
    botDifficulty: autoBotDifficulty,
    botPilotLastComputedAtMs: nowMs,
    botPilotUpdatedAt: admin.firestore.FieldValue.serverTimestamp(),
    botPilotMetricsSnapshot: {
      window: snapshot.window,
      startMs: snapshot.startMs,
      endMs: snapshot.endMs,
      roomsCount: safeInt(snapshot.roomsCount),
      collectedDoes: safeInt(snapshot.collectedDoes),
      payoutDoes: safeInt(snapshot.payoutDoes),
      netDoes: safeSignedInt(snapshot.netDoes),
      grossCollectedDoes: safeInt(snapshot.grossCollectedDoes),
      grossPayoutDoes: safeInt(snapshot.grossPayoutDoes),
      grossNetDoes: safeSignedInt(snapshot.grossNetDoes),
      promoExposureDoes: safeInt(snapshot.promoExposureDoes),
      marginPct: Number(snapshot.marginPct || 0),
      currentEquityDoes: safeSignedInt(snapshot.currentEquityDoes),
      highWaterMarkDoes: safeSignedInt(snapshot.highWaterMarkDoes),
      drawdownDoes: safeInt(snapshot.drawdownDoes),
      drawdownPct: Number(snapshot.drawdownPct || 0),
      lastPeakAtMs: safeSignedInt(snapshot.lastPeakAtMs),
      recommendedLevel: normalizeBotDifficulty(snapshot.recommendedLevel),
      recommendedBand: String(snapshot.recommendedBand || ""),
      recommendedReason: String(snapshot.recommendedReason || ""),
      computedAtMs: nowMs,
    },
  }, { merge: true });
});

exports.refreshDuelBotPilotAuto = onSchedule("every 10 minutes", async () => {
  const settingsSnap = await adminBootstrapRef().get();
  if (!settingsSnap.exists) return;
  const settings = settingsSnap.data() || {};
  const mode = normalizeBotPilotMode(settings.duelBotPilotMode || "manual");
  if (mode !== "auto") return;

  const nowMs = Date.now();
  const windowKey = normalizeBotPilotWindow(settings.duelBotPilotWindow || "today");
  const snapshot = await computeDuelBotPilotSnapshot({ nowMs, window: windowKey });
  const autoBotDifficulty = normalizeBotDifficulty(snapshot.recommendedLevel || settings.autoDuelBotDifficulty || settings.duelBotDifficulty || DEFAULT_DUEL_BOT_DIFFICULTY);

  await adminBootstrapRef().set({
    email: FINANCE_ADMIN_EMAIL,
    duelBotPilotMode: "auto",
    duelBotPilotWindow: windowKey,
    autoDuelBotDifficulty,
    duelBotDifficulty: autoBotDifficulty,
    duelBotPilotLastComputedAtMs: nowMs,
    duelBotPilotUpdatedAt: admin.firestore.FieldValue.serverTimestamp(),
    duelBotPilotMetricsSnapshot: {
      window: snapshot.window,
      startMs: snapshot.startMs,
      endMs: snapshot.endMs,
      roomsCount: safeInt(snapshot.roomsCount),
      collectedDoes: safeInt(snapshot.collectedDoes),
      payoutDoes: safeInt(snapshot.payoutDoes),
      netDoes: safeSignedInt(snapshot.netDoes),
      grossCollectedDoes: safeInt(snapshot.grossCollectedDoes),
      grossPayoutDoes: safeInt(snapshot.grossPayoutDoes),
      grossNetDoes: safeSignedInt(snapshot.grossNetDoes),
      promoExposureDoes: safeInt(snapshot.promoExposureDoes),
      marginPct: Number(snapshot.marginPct || 0),
      currentEquityDoes: safeSignedInt(snapshot.currentEquityDoes),
      highWaterMarkDoes: safeSignedInt(snapshot.highWaterMarkDoes),
      drawdownDoes: safeInt(snapshot.drawdownDoes),
      drawdownPct: Number(snapshot.drawdownPct || 0),
      lastPeakAtMs: safeSignedInt(snapshot.lastPeakAtMs),
      botWins: safeInt(snapshot.botWins),
      humanWins: safeInt(snapshot.humanWins),
      botWinRatePct: Number(snapshot.botWinRatePct || 0),
      humanWinRatePct: Number(snapshot.humanWinRatePct || 0),
      recommendedLevel: normalizeBotDifficulty(snapshot.recommendedLevel),
      recommendedBand: String(snapshot.recommendedBand || ""),
      recommendedReason: String(snapshot.recommendedReason || ""),
      computedAtMs: nowMs,
    },
  }, { merge: true });
});

exports.getMorpionPilotSnapshot = publicOnCall("getMorpionPilotSnapshot", async (request) => {
  assertFinanceAdmin(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const nowMs = Date.now();
  const settingsSnap = await adminBootstrapRef().get();
  const settings = settingsSnap.exists ? (settingsSnap.data() || {}) : {};
  const mode = normalizeMorpionPilotMode(settings.morpionPilotMode || "manual");
  const windowKey = normalizeMorpionPilotWindow(payload.window || settings.morpionPilotWindow || "today");
  const snapshot = await computeMorpionPilotSnapshot({ nowMs, window: windowKey });
  const manualForceBotOnly = settings.morpionPilotForceBotOnly === true;
  const autoHumanOnlyEnabled = settings.morpionPilotAutoHumanOnlyEnabled !== false;
  const appliedHumanOnlyEnabled = mode === "auto" ? autoHumanOnlyEnabled : !manualForceBotOnly;

  return {
    ok: true,
    mode,
    window: windowKey,
    manualForceBotOnly,
    autoHumanOnlyEnabled,
    appliedHumanOnlyEnabled,
    snapshot,
  };
});

exports.setMorpionPilotControl = publicOnCall("setMorpionPilotControl", async (request) => {
  assertFinanceAdmin(request);
  const payload = request.data && typeof request.data === "object" ? request.data : {};
  const currentSnap = await adminBootstrapRef().get();
  const current = currentSnap.exists ? (currentSnap.data() || {}) : {};
  const mode = normalizeMorpionPilotMode(payload.mode || current.morpionPilotMode || "manual");
  const windowKey = normalizeMorpionPilotWindow(payload.window || current.morpionPilotWindow || "today");
  const manualForceBotOnly = payload.manualForceBotOnly === true;
  let autoHumanOnlyEnabled = current.morpionPilotAutoHumanOnlyEnabled !== false;
  let snapshot = null;

  if (mode === "auto") {
    snapshot = await computeMorpionPilotSnapshot({ nowMs: Date.now(), window: windowKey });
    autoHumanOnlyEnabled = snapshot.recommendedHumanOnlyEnabled !== false;
  }

  const appliedHumanOnlyEnabled = mode === "auto" ? autoHumanOnlyEnabled : !manualForceBotOnly;

  await adminBootstrapRef().set({
    email: FINANCE_ADMIN_EMAIL,
    morpionPilotMode: mode,
    morpionPilotWindow: windowKey,
    morpionPilotForceBotOnly: manualForceBotOnly,
    morpionPilotAutoHumanOnlyEnabled: autoHumanOnlyEnabled,
    morpionPilotAppliedHumanOnlyEnabled: appliedHumanOnlyEnabled,
    morpionPilotLastComputedAtMs: Date.now(),
    morpionPilotUpdatedAt: admin.firestore.FieldValue.serverTimestamp(),
    morpionPilotMetricsSnapshot: snapshot ? {
      window: snapshot.window,
      startMs: snapshot.startMs,
      endMs: snapshot.endMs,
      roomsCount: safeInt(snapshot.roomsCount),
      collectedDoes: safeInt(snapshot.collectedDoes),
      payoutDoes: safeInt(snapshot.payoutDoes),
      netDoes: safeSignedInt(snapshot.netDoes),
      grossCollectedDoes: safeInt(snapshot.grossCollectedDoes),
      grossPayoutDoes: safeInt(snapshot.grossPayoutDoes),
      grossNetDoes: safeSignedInt(snapshot.grossNetDoes),
      promoExposureDoes: safeInt(snapshot.promoExposureDoes),
      marginPct: Number(snapshot.marginPct || 0),
      currentEquityDoes: safeSignedInt(snapshot.currentEquityDoes),
      highWaterMarkDoes: safeSignedInt(snapshot.highWaterMarkDoes),
      drawdownDoes: safeInt(snapshot.drawdownDoes),
      drawdownPct: Number(snapshot.drawdownPct || 0),
      lastPeakAtMs: safeSignedInt(snapshot.lastPeakAtMs),
      humanOnlyRooms: safeInt(snapshot.humanOnlyRooms),
      withBotRooms: safeInt(snapshot.withBotRooms),
      humanOnlySharePct: Number(snapshot.humanOnlySharePct || 0),
      withBotSharePct: Number(snapshot.withBotSharePct || 0),
      recommendedDecision: normalizeMorpionPilotDecision(snapshot.recommendedDecision),
      recommendedBand: String(snapshot.recommendedBand || ""),
      recommendedReason: String(snapshot.recommendedReason || ""),
      recommendedHumanOnlyEnabled: snapshot.recommendedHumanOnlyEnabled !== false,
      computedAtMs: Date.now(),
    } : admin.firestore.FieldValue.delete(),
  }, { merge: true });

  return {
    ok: true,
    mode,
    window: windowKey,
    manualForceBotOnly,
    autoHumanOnlyEnabled,
    appliedHumanOnlyEnabled,
    snapshot,
  };
});

exports.refreshMorpionPilotAuto = onSchedule("every 10 minutes", async () => {
  const settingsSnap = await adminBootstrapRef().get();
  if (!settingsSnap.exists) return;
  const settings = settingsSnap.data() || {};
  const mode = normalizeMorpionPilotMode(settings.morpionPilotMode || "manual");
  if (mode !== "auto") return;

  const nowMs = Date.now();
  const windowKey = normalizeMorpionPilotWindow(settings.morpionPilotWindow || "today");
  const snapshot = await computeMorpionPilotSnapshot({ nowMs, window: windowKey });
  const autoHumanOnlyEnabled = snapshot.recommendedHumanOnlyEnabled !== false;

  await adminBootstrapRef().set({
    email: FINANCE_ADMIN_EMAIL,
    morpionPilotMode: "auto",
    morpionPilotWindow: windowKey,
    morpionPilotAutoHumanOnlyEnabled: autoHumanOnlyEnabled,
    morpionPilotAppliedHumanOnlyEnabled: autoHumanOnlyEnabled,
    morpionPilotLastComputedAtMs: nowMs,
    morpionPilotUpdatedAt: admin.firestore.FieldValue.serverTimestamp(),
    morpionPilotMetricsSnapshot: {
      window: snapshot.window,
      startMs: snapshot.startMs,
      endMs: snapshot.endMs,
      roomsCount: safeInt(snapshot.roomsCount),
      collectedDoes: safeInt(snapshot.collectedDoes),
      payoutDoes: safeInt(snapshot.payoutDoes),
      netDoes: safeSignedInt(snapshot.netDoes),
      grossCollectedDoes: safeInt(snapshot.grossCollectedDoes),
      grossPayoutDoes: safeInt(snapshot.grossPayoutDoes),
      grossNetDoes: safeSignedInt(snapshot.grossNetDoes),
      promoExposureDoes: safeInt(snapshot.promoExposureDoes),
      marginPct: Number(snapshot.marginPct || 0),
      currentEquityDoes: safeSignedInt(snapshot.currentEquityDoes),
      highWaterMarkDoes: safeSignedInt(snapshot.highWaterMarkDoes),
      drawdownDoes: safeInt(snapshot.drawdownDoes),
      drawdownPct: Number(snapshot.drawdownPct || 0),
      lastPeakAtMs: safeSignedInt(snapshot.lastPeakAtMs),
      humanOnlyRooms: safeInt(snapshot.humanOnlyRooms),
      withBotRooms: safeInt(snapshot.withBotRooms),
      humanOnlySharePct: Number(snapshot.humanOnlySharePct || 0),
      withBotSharePct: Number(snapshot.withBotSharePct || 0),
      recommendedDecision: normalizeMorpionPilotDecision(snapshot.recommendedDecision),
      recommendedBand: String(snapshot.recommendedBand || ""),
      recommendedReason: String(snapshot.recommendedReason || ""),
      recommendedHumanOnlyEnabled: snapshot.recommendedHumanOnlyEnabled !== false,
      computedAtMs: nowMs,
    },
  }, { merge: true });
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
