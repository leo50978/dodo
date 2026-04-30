import { auth, db, collection, getDocs, limit, onAuthStateChanged, orderBy, query, where } from "../firebase-init.js";
import { formatAuthError, isValidEmail, isValidPhoneLogin, isValidUsername, loginWithEmail, loginWithPhone, loginWithUsername, signupWithPhone, syncCurrentUserDisplayName } from "../auth.js";
import { getDepositFundingStatusSecure, getMyGameHistorySecure } from "../secure-functions.js";
import { mountTransferModal } from "../transfer.js";
import { buildWhatsappUrlForKey, getWhatsappContactLabel } from "../whatsapp-modal-config.js";
import PaymentModal from "../payment.js";

document.documentElement.classList.add("kobposh-ready");

if (window.lucide) {
  window.lucide.createIcons();
}

mountTransferModal({ triggerSelector: "#kobposhTransferBtn", theme: "kobposh" });

const isPublicView = new URLSearchParams(window.location.search).get("view") === "public";

const HERO_ROTATION_MS = 5000;
const BALANCE_REFRESH_MS = 2 * 60 * 1000;
let heroRotationTimer = null;
let balanceRefreshTimer = null;
const gamesModal = document.querySelector("[data-games-modal]");
const openGamesModalBtn = document.querySelector("[data-open-games-modal]");
const closeGamesModalBtn = document.querySelector("[data-close-games-modal]");
const openDepositModalBtns = document.querySelectorAll("[data-open-deposit-modal]");
const withdrawalQuickBtn = document.querySelector("#kobposhWithdrawalBtn");
const balanceEl = document.querySelector("[data-kobposh-balance]");
const recentMatchesEl = document.querySelector("[data-kobposh-recent-matches]");
const authScreenEl = document.querySelector("[data-kobposh-auth-screen]");
const loginFormEl = document.querySelector("[data-kobposh-login-form]");
const loginIdentifierEl = document.querySelector("[data-kobposh-login-identifier]");
const loginPasswordEl = document.querySelector("[data-kobposh-login-password]");
const loginErrorEl = document.querySelector("[data-kobposh-login-error]");
const signupToggleBtn = document.querySelector("[data-kobposh-open-signup]");
const loginFieldsEl = document.querySelector("[data-kobposh-login-fields]");
const signupFieldsEl = document.querySelector("[data-kobposh-signup-fields]");
const signupUsernameEl = document.querySelector("[data-kobposh-signup-username]");
const signupPhoneEl = document.querySelector("[data-kobposh-signup-phone]");
const signupPasswordEl = document.querySelector("[data-kobposh-signup-password]");
const signupPasswordConfirmEl = document.querySelector("[data-kobposh-signup-password-confirm]");
const signupAgeEl = document.querySelector("[data-kobposh-signup-age]");
const signupTermsEl = document.querySelector("[data-kobposh-signup-terms]");
const authCardTitleEl = document.querySelector(".auth-screen__title");
const authCardSubtitleEl = document.querySelector(".auth-screen__subtitle");
const authSubmitBtn = document.querySelector(".auth-screen__button");
const accountLabelEl = document.querySelector("[data-kobposh-account-label]");
const passwordToggleBtns = document.querySelectorAll("[data-kobposh-toggle-password]");
let authMode = "login";
let depositModal = null;
let depositAmountInput = null;
let depositAmountSummary = null;
let depositErrorEl = null;
let depositSubmitBtn = null;
let activePaymentModal = null;
let stakeModal = null;
let stakeAmountSummary = null;
let stakeErrorEl = null;
let stakeSubmitBtn = null;
let activeGameKey = "";
let selectedGameStake = 100;
let withdrawalAgentModal = null;
let historyModal = null;
const historyModalState = {
  rows: [],
  offset: 0,
  pageSize: 3,
  hasMore: true,
  loading: false,
};

const WITHDRAWAL_AGENT_CONTACTS = [
  {
    key: "withdrawal_assistance",
    title: "Agent retrait",
    role: "Retrait / suivi",
    note: "Contacte cet agent pour un retrait rapide.",
    message: "Bonjou, mwen bezwen fè yon retrè sou kont mwen.",
  },
  {
    key: "agent_deposit",
    title: "Support secours",
    role: "Assistance générale",
    note: "Si l'agent retrait ne répond pas, contacte le support.",
    message: "Bonjou, mwen bezwen asistans pou yon retrè sou kont mwen.",
  },
];

const GAME_LAUNCH_CONFIG = {
  domino: {
    title: "DOMINO",
    label: "Domino",
    description: "Chwazi kantite Does ou vle mete pou kòmanse yon pati Domino.",
    image: "./domino.png",
    amounts: [100, 250, 500, 1000],
    buildHref: (amount) => `./jeu.html?autostart=1&stake=${amount}`,
  },
  morpion: {
    title: "MOPYON",
    label: "Mopyon",
    description: "Chwazi kantite Does ou vle jwe pou Mopyon.",
    image: "./mopyon.png",
    amounts: [500],
    buildHref: (amount) => `../morpion.html?stake=${amount}`,
  },
  dame: {
    title: "DAME",
    label: "Dame",
    description: "Chwazi kantite Does ou vle mete pou Dame.",
    image: "./dame.png",
    amounts: [100, 250, 500, 1000],
    buildHref: (amount) => `../dame.html?stake=${amount}`,
  },
  pong: {
    title: "PONG",
    label: "Pong",
    description: "Chwazi kantite Does ou vle jwe pou Pong.",
    image: "./pong.png",
    amounts: [100, 500],
    buildHref: (amount) => `../pong.html?stake=${amount}`,
  },
};

const GAME_HISTORY_SOURCES = [
  { collectionName: "roomResults", gameLabel: "Domino", gameKey: "domino" },
  { collectionName: "duelRoomResults", gameLabel: "Duel", gameKey: "duel" },
  { collectionName: "morpionRoomResults", gameLabel: "Mopyon", gameKey: "morpion" },
  { collectionName: "dameRoomResults", gameLabel: "Dame", gameKey: "dame" },
  { collectionName: "pongMatchResults", gameLabel: "Pong", gameKey: "pong" },
];

function buildHeroSlides() {
  const track = document.querySelector("[data-kobposh-hero-track]");
  if (!track) return [];

  const slides = [
    { src: "../hero.jpg", alt: "Entèfas Kobposh" },
    { src: "../hero1.jpg", alt: "Entèfas Kobposh 1" },
    { src: "../hero2.jpg", alt: "Entèfas Kobposh 2" },
  ];

  track.replaceChildren();

  slides.forEach((slideData, index) => {
    const slide = document.createElement("div");
    slide.className = "hero-banner__slide";
    slide.setAttribute("data-kobposh-hero-slide", "");
    if (index === 0) slide.classList.add("is-active");
    slide.innerHTML = `
      <img
        src="${slideData.src}"
        alt="${slideData.alt}"
        width="600"
        height="600"
        fetchpriority="${index === 0 ? "high" : "auto"}"
        decoding="async"
      />
    `;
    track.appendChild(slide);
  });

  return Array.from(track.querySelectorAll("[data-kobposh-hero-slide]"));
}

function initHeroRotation() {
  const slides = Array.from(document.querySelectorAll("[data-kobposh-hero-slide]"));
  if (heroRotationTimer) {
    window.clearInterval(heroRotationTimer);
    heroRotationTimer = null;
  }
  if (slides.length === 0) return;

  let activeIndex = slides.findIndex((slide) => slide.classList.contains("is-active"));
  if (activeIndex < 0) activeIndex = 0;

  const render = () => {
    slides.forEach((slide, index) => {
      slide.classList.toggle("is-active", index === activeIndex);
    });
  };

  render();
  if (slides.length === 1) return;

  heroRotationTimer = window.setInterval(() => {
    activeIndex = (activeIndex + 1) % slides.length;
    render();
  }, HERO_ROTATION_MS);
}

function ensureWithdrawalAgentModal() {
  if (withdrawalAgentModal) return withdrawalAgentModal;

  withdrawalAgentModal = document.createElement("section");
  withdrawalAgentModal.id = "kobposhWithdrawalAgentModal";
  withdrawalAgentModal.className = "kobposh-agent-modal hidden";
  withdrawalAgentModal.setAttribute("aria-hidden", "true");
  withdrawalAgentModal.innerHTML = `
    <div class="kobposh-agent-modal__backdrop" data-kobposh-agent-close></div>
    <div class="kobposh-agent-modal__panel" role="dialog" aria-modal="true" aria-labelledby="kobposhAgentTitle">
      <div class="kobposh-agent-modal__header">
        <div class="min-w-0">
          <p class="kobposh-agent-modal__eyebrow">RETRAIT RAPIDE</p>
          <h2 id="kobposhAgentTitle" class="kobposh-agent-modal__title">Contacte un agent en 1 clic</h2>
          <p class="kobposh-agent-modal__subtitle">Chwazi yon ajan pou fè retrè a fèt rapidman.</p>
        </div>
        <button class="kobposh-agent-modal__back" type="button" aria-label="Retour" data-kobposh-agent-close>
          <i data-lucide="arrow-left" class="icon" aria-hidden="true"></i>
        </button>
      </div>

      <div class="kobposh-agent-modal__list">
        ${WITHDRAWAL_AGENT_CONTACTS.map((agent) => {
          const phoneLabel = getWhatsappContactLabel(agent.key);
          const waLink = buildWhatsappUrlForKey(agent.key, agent.message);
          return `
            <a class="kobposh-agent-card" href="${waLink}" target="_blank" rel="noopener noreferrer">
              <div class="kobposh-agent-card__top">
                <div class="min-w-0">
                  <h3 class="kobposh-agent-card__name">${agent.title}</h3>
                  <p class="kobposh-agent-card__role">${agent.role}</p>
                </div>
                <p class="kobposh-agent-card__phone">${phoneLabel || ""}</p>
              </div>
              <div class="kobposh-agent-card__action">
                <span>Ouvrir WhatsApp</span>
                <i data-lucide="message-circle"></i>
              </div>
              <p class="kobposh-agent-card__note">${agent.note}</p>
            </a>
          `;
        }).join("")}
      </div>

      <a class="kobposh-agent-modal__cta" href="./recrutement.html">
        Devenir un agent
      </a>
    </div>
  `;

  document.body.appendChild(withdrawalAgentModal);
  if (window.lucide) {
    window.lucide.createIcons();
  }

  withdrawalAgentModal.addEventListener("click", (event) => {
    if (event.target === withdrawalAgentModal || event.target?.closest?.("[data-kobposh-agent-close]")) {
      closeWithdrawalAgentModal();
    }
  });

  return withdrawalAgentModal;
}

function openWithdrawalAgentModal() {
  const modal = ensureWithdrawalAgentModal();
  modal.classList.remove("hidden");
  modal.classList.add("is-open");
  modal.setAttribute("aria-hidden", "false");
  document.body.classList.add("is-modal-open");
}

function closeWithdrawalAgentModal() {
  if (!withdrawalAgentModal) return;
  withdrawalAgentModal.classList.add("hidden");
  withdrawalAgentModal.classList.remove("is-open");
  withdrawalAgentModal.setAttribute("aria-hidden", "true");
  document.body.classList.remove("is-modal-open");
}

function getHistoryNetLabel(row) {
  const net = Number(row?.netDoes || 0);
  if (net > 0) return `+${formatMatchAmount(net)}`;
  if (net < 0) return `-${formatMatchAmount(Math.abs(net))}`;
  return "0 HTG";
}

function renderHistoryModalRows() {
  const list = historyModal?.querySelector("[data-kobposh-history-list]");
  const empty = historyModal?.querySelector("[data-kobposh-history-empty]");
  const loadMoreBtn = historyModal?.querySelector("[data-kobposh-history-load-more]");
  const status = historyModal?.querySelector("[data-kobposh-history-status]");
  if (!list || !empty || !loadMoreBtn) return;

  if (status) {
    status.textContent = historyModalState.loading
      ? "Ap chaje..."
      : historyModalState.rows.length
        ? `${historyModalState.rows.length} jwèt`
        : "0 jwèt";
  }

  const visibleRows = historyModalState.rows.slice(0, historyModalState.offset + historyModalState.pageSize);
  if (!visibleRows.length) {
    list.replaceChildren();
    empty.hidden = historyModalState.loading;
    empty.textContent = historyModalState.loading ? "Ap chaje..." : "Pa gen istorik jwèt pou montre.";
    loadMoreBtn.hidden = true;
    return;
  }

  empty.hidden = true;
  list.innerHTML = visibleRows.map((row) => {
    const resultClass = row.won ? "win" : "loss";
    const resultLabel = row.resultLabel || (row.won ? "Genyen" : "Pèdi");
    const metaParts = [
      row.gameLabel || "Jwèt",
      row.scoreLabel ? `Nòt ${row.scoreLabel}` : "",
      row.opponentLabel ? `Kont ${row.opponentLabel}` : "",
      row.endedAtMs ? formatMatchDate(row.endedAtMs) : "",
    ].filter(Boolean);

    return `
      <article class="kobposh-history-card">
        <div class="kobposh-history-card__top">
          <div class="min-w-0">
            <h3 class="kobposh-history-card__title">${row.gameLabel || "Jwèt"}</h3>
            <p class="kobposh-history-card__meta">${metaParts.join(" • ")}</p>
          </div>
          <span class="kobposh-history-card__result kobposh-history-card__result--${resultClass}">${resultLabel}</span>
        </div>
        <div class="kobposh-history-card__bottom">
          <span class="kobposh-history-card__amount ${row.netDoes >= 0 ? "is-win" : "is-loss"}">${getHistoryNetLabel(row)}</span>
          <span class="kobposh-history-card__details">Mise ${formatMatchAmount(row.wageredDoes || row.stakeDoes || 0)} · Gain ${formatMatchAmount(row.wonDoes || 0)}</span>
        </div>
      </article>
    `;
  }).join("");

  loadMoreBtn.hidden = !historyModalState.hasMore;
  loadMoreBtn.disabled = historyModalState.loading;
  loadMoreBtn.textContent = historyModalState.loading ? "Ap chaje..." : "Chaje 3 lòt";
}

async function loadHistoryModalPage() {
  const user = auth.currentUser;
  if (!user?.uid || historyModalState.loading) return;
  historyModalState.loading = true;
  renderHistoryModalRows();
  try {
    const payload = await loadRecentMatchesForUser(user.uid, historyModalState.offset, historyModalState.pageSize);
    historyModalState.rows = historyModalState.rows.concat(Array.isArray(payload?.rows) ? payload.rows : []);
    historyModalState.offset += historyModalState.pageSize;
    historyModalState.hasMore = Boolean(payload?.hasMore);
  } catch (error) {
    console.warn("[KOBPOSH] history modal load failed", error);
    historyModalState.hasMore = false;
  } finally {
    historyModalState.loading = false;
    renderHistoryModalRows();
  }
}

function ensureHistoryModal() {
  if (historyModal) return historyModal;

  historyModal = document.createElement("section");
  historyModal.className = "kobposh-history-modal";
  historyModal.setAttribute("aria-hidden", "true");
  historyModal.innerHTML = `
    <div class="kobposh-history-modal__panel" role="dialog" aria-modal="true" aria-labelledby="kobposhHistoryTitle">
      <header class="kobposh-history-modal__header">
        <div>
          <p class="kobposh-history-modal__eyebrow">ISTORIK</p>
          <h2 id="kobposhHistoryTitle" class="kobposh-history-modal__title">Istwa jwèt ou yo</h2>
          <p class="kobposh-history-modal__subtitle">Wè 3 jwèt pa 3 jwèt, ak gan oswa pèt sou chak pati.</p>
        </div>
        <button class="kobposh-history-modal__back" type="button" aria-label="Retour" data-kobposh-history-close>
          <i data-lucide="arrow-left" class="icon" aria-hidden="true"></i>
        </button>
      </header>

      <div class="kobposh-history-modal__content">
        <div class="kobposh-history-modal__summary">
          <span>3 a la fwa</span>
          <strong data-kobposh-history-status>0 jwèt</strong>
        </div>
        <div class="kobposh-history-modal__list" data-kobposh-history-list></div>
        <p class="kobposh-history-modal__empty" data-kobposh-history-empty hidden>Pa gen istorik jwèt pou montre.</p>
        <button class="kobposh-history-modal__more" type="button" data-kobposh-history-load-more>Chaje 3 lòt</button>
      </div>
    </div>
  `;

  document.body.appendChild(historyModal);

  historyModal.addEventListener("click", (event) => {
    if (event.target === historyModal || event.target?.closest?.("[data-kobposh-history-close]")) {
      closeHistoryModal();
    }
  });

  historyModal.querySelector("[data-kobposh-history-load-more]")?.addEventListener("click", () => {
    void loadHistoryModalPage();
  });

  if (window.lucide) {
    window.lucide.createIcons();
  }

  return historyModal;
}

function openHistoryModal() {
  const modal = ensureHistoryModal();
  historyModalState.rows = [];
  historyModalState.offset = 0;
  historyModalState.hasMore = true;
  historyModalState.loading = false;
  modal.classList.add("is-open");
  modal.setAttribute("aria-hidden", "false");
  document.body.classList.add("is-modal-open");
  renderHistoryModalRows();
  void loadHistoryModalPage();
}

function closeHistoryModal() {
  if (!historyModal) return;
  historyModal.classList.remove("is-open");
  historyModal.setAttribute("aria-hidden", "true");
  document.body.classList.remove("is-modal-open");
}

function formatDepositAmount(value) {
  const amount = Number(value || 0);
  return `${new Intl.NumberFormat("fr-FR", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(amount)} HTG`;
}

function formatDoesOnly(value) {
  const amount = Number(value || 0);
  return `${new Intl.NumberFormat("fr-FR", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(amount)} Does`;
}

function getGameLaunchConfig(gameKey) {
  return GAME_LAUNCH_CONFIG[String(gameKey || "").trim().toLowerCase()] || null;
}

function closeStakeModal() {
  if (!stakeModal) return;
  stakeModal.classList.remove("is-open");
  stakeModal.setAttribute("aria-hidden", "true");
  document.body.classList.remove("is-modal-open");
  if (stakeErrorEl) stakeErrorEl.textContent = "";
}

function ensureStakeModal() {
  if (stakeModal) return stakeModal;

  stakeModal = document.createElement("section");
  stakeModal.className = "stake-modal";
  stakeModal.setAttribute("aria-hidden", "true");
  stakeModal.innerHTML = `
    <div class="stake-modal__panel" role="dialog" aria-modal="true" aria-labelledby="stakeModalTitle">
      <header class="stake-modal__header">
        <button class="stake-modal__back" type="button" aria-label="Fèmen modal la" data-close-stake-modal>
          <i data-lucide="arrow-left" class="icon icon--modal-back" aria-hidden="true"></i>
        </button>

        <div class="stake-modal__brand">
          <p class="stake-modal__eyebrow">JWÈT</p>
          <h2 id="stakeModalTitle" class="stake-modal__title">JWÈT</h2>
        </div>

        <div class="stake-modal__badge">CHWAZI</div>
      </header>

      <div class="stake-modal__body">
        <div class="stake-modal__card">
          <div class="stake-modal__visual">
            <img src="" alt="" data-stake-modal-image />
          </div>

          <p class="stake-modal__lead" data-stake-modal-copy></p>

          <div class="stake-modal__chips" aria-label="Kantite rapid" data-stake-modal-chips></div>

          <div class="stake-modal__summary" aria-live="polite">
            <span>Ou pral jwe ak</span>
            <strong data-stake-modal-total>100 Does</strong>
          </div>

          <div class="stake-modal__error" data-stake-modal-error></div>

          <button class="stake-modal__submit" type="button" data-stake-modal-submit>
            Kontinye nan jwèt la
          </button>
        </div>
      </div>
    </div>
  `;

  document.body.appendChild(stakeModal);

  stakeAmountSummary = stakeModal.querySelector("[data-stake-modal-total]");
  stakeErrorEl = stakeModal.querySelector("[data-stake-modal-error]");
  stakeSubmitBtn = stakeModal.querySelector("[data-stake-modal-submit]");
  const stakeImageEl = stakeModal.querySelector("[data-stake-modal-image]");
  const stakeCopyEl = stakeModal.querySelector("[data-stake-modal-copy]");
  const stakeTitleEl = stakeModal.querySelector("#stakeModalTitle");
  const stakeChipsEl = stakeModal.querySelector("[data-stake-modal-chips]");
  const closeBtn = stakeModal.querySelector("[data-close-stake-modal]");

  const syncStakeAmountState = (amountValue) => {
    const amount = Math.max(25, Math.floor(Number(amountValue) || 0));
    selectedGameStake = amount;
    if (stakeAmountSummary) stakeAmountSummary.textContent = formatDoesOnly(amount);

    stakeModal.querySelectorAll("[data-stake-amount-chip]").forEach((chip) => {
      const chipAmount = Number(chip.getAttribute("data-stake-amount-chip") || 0);
      chip.classList.toggle("is-active", chipAmount === amount);
    });
  };

  stakeChipsEl?.addEventListener("click", (event) => {
    const button = event.target.closest("[data-stake-amount-chip]");
    if (!button) return;
    const amount = Number(button.getAttribute("data-stake-amount-chip") || 0);
    syncStakeAmountState(amount);
    if (stakeErrorEl) stakeErrorEl.textContent = "";
  });

  const openGameHref = () => {
    const config = getGameLaunchConfig(activeGameKey);
    const amount = Math.max(25, Math.floor(Number(selectedGameStake || 0)));
    if (!config) {
      if (stakeErrorEl) stakeErrorEl.textContent = "Jeu a pa disponib ankò.";
      return;
    }
    if (!Number.isFinite(amount) || amount < 25) {
      if (stakeErrorEl) stakeErrorEl.textContent = "Mete yon montan ki valab, omwen 25 Does.";
      return;
    }

    closeStakeModal();
    window.location.href = config.buildHref(amount);
  };

  stakeSubmitBtn?.addEventListener("click", openGameHref);

  stakeModal.addEventListener("click", (event) => {
    if (event.target === stakeModal) closeStakeModal();
  });

  closeBtn?.addEventListener("click", closeStakeModal);

  syncStakeAmountState(100);

  if (window.lucide) {
    window.lucide.createIcons();
  }

  return stakeModal;
}

function openStakeModal(gameKey) {
  const config = getGameLaunchConfig(gameKey);
  if (!config) return;

  const modal = ensureStakeModal();
  closeGamesModal();
  closeDepositModal();

  activeGameKey = String(gameKey || "").trim().toLowerCase();

  const titleEl = modal.querySelector("#stakeModalTitle");
  const imageEl = modal.querySelector("[data-stake-modal-image]");
  const copyEl = modal.querySelector("[data-stake-modal-copy]");
  const chipsEl = modal.querySelector("[data-stake-modal-chips]");

  if (titleEl) titleEl.textContent = config.title;
  if (imageEl) {
    imageEl.src = config.image;
    imageEl.alt = config.label;
  }
  if (copyEl) copyEl.textContent = config.description;

  chipsEl.replaceChildren();
  const amountOptions = Array.isArray(config.amounts) && config.amounts.length ? config.amounts : [100];
  amountOptions.forEach((amount, index) => {
    const chip = document.createElement("button");
    chip.type = "button";
    chip.className = `stake-modal__chip${index === 0 ? " is-active" : ""}`;
    chip.setAttribute("data-stake-amount-chip", String(amount));
    chip.textContent = formatDoesOnly(amount);
    chipsEl.appendChild(chip);
  });

  const firstAmount = Number(amountOptions[0] || 100);
  selectedGameStake = firstAmount;
  if (stakeAmountSummary) stakeAmountSummary.textContent = formatDoesOnly(firstAmount);
  if (stakeErrorEl) stakeErrorEl.textContent = "";

  modal.classList.add("is-open");
  modal.setAttribute("aria-hidden", "false");
  document.body.classList.add("is-modal-open");

  if (window.lucide) {
    window.lucide.createIcons();
  }
}

function ensureDepositModal() {
  if (depositModal) return depositModal;

  depositModal = document.createElement("section");
  depositModal.className = "deposit-modal";
  depositModal.setAttribute("aria-hidden", "true");
  depositModal.innerHTML = `
    <div class="deposit-modal__panel" role="dialog" aria-modal="true" aria-labelledby="depositModalTitle">
      <header class="deposit-modal__header">
        <button class="deposit-modal__back" type="button" aria-label="Fèmen depo a" data-close-deposit-modal>
          <i data-lucide="arrow-left" class="icon icon--modal-back" aria-hidden="true"></i>
        </button>

        <div class="deposit-modal__brand">
          <p class="deposit-modal__eyebrow">DEPÒ</p>
          <h2 id="depositModalTitle" class="deposit-modal__title">Fè yon depo</h2>
        </div>

        <div class="deposit-modal__badge">Kobposh</div>
      </header>

      <div class="deposit-modal__body">
        <div class="deposit-modal__card">
          <p class="deposit-modal__lead">
            Mete kantite lajan ou vle depoze a.
          </p>

          <div class="deposit-modal__field">
            <label class="deposit-modal__label" for="depositAmount">Montan depo (HTG)</label>
            <input
              id="depositAmount"
              class="deposit-modal__input"
              type="number"
              min="25"
              step="25"
              inputmode="numeric"
              value="25"
            />
          </div>

          <div class="deposit-modal__chips" aria-label="Kantite rapid">
            <button class="deposit-modal__chip is-active" type="button" data-deposit-amount-chip="25">25</button>
            <button class="deposit-modal__chip" type="button" data-deposit-amount-chip="50">50</button>
            <button class="deposit-modal__chip" type="button" data-deposit-amount-chip="100">100</button>
            <button class="deposit-modal__chip" type="button" data-deposit-amount-chip="250">250</button>
          </div>

          <div class="deposit-modal__summary" aria-live="polite">
            <span>Total ou pral antre a</span>
            <strong data-deposit-total>25 HTG</strong>
          </div>

          <div class="deposit-modal__note">
            Depo a pral kontinye sou sistèm peman an. Asire montan an kòrèk anvan ou kontinye.
          </div>

          <div class="deposit-modal__error" data-deposit-error></div>

          <button class="deposit-modal__submit" type="button" data-deposit-submit>
            Kontinye nan depo a
          </button>
        </div>
      </div>
    </div>
  `;

  document.body.appendChild(depositModal);

  depositAmountInput = depositModal.querySelector("#depositAmount");
  depositAmountSummary = depositModal.querySelector("[data-deposit-total]");
  depositErrorEl = depositModal.querySelector("[data-deposit-error]");
  depositSubmitBtn = depositModal.querySelector("[data-deposit-submit]");
  const closeBtn = depositModal.querySelector("[data-close-deposit-modal]");

  const syncAmountState = (amountValue) => {
    const amount = Math.max(25, Number(amountValue) || 0);
    if (depositAmountInput) depositAmountInput.value = String(amount);
    if (depositAmountSummary) depositAmountSummary.textContent = formatDepositAmount(amount);

    depositModal.querySelectorAll("[data-deposit-amount-chip]").forEach((chip) => {
      const chipAmount = Number(chip.getAttribute("data-deposit-amount-chip") || 0);
      chip.classList.toggle("is-active", chipAmount === amount);
    });
  };

  depositAmountInput?.addEventListener("input", () => {
    syncAmountState(depositAmountInput.value);
    if (depositErrorEl) depositErrorEl.textContent = "";
  });

  depositModal.querySelectorAll("[data-deposit-amount-chip]").forEach((chip) => {
    chip.addEventListener("click", () => {
      const amount = Number(chip.getAttribute("data-deposit-amount-chip") || 0);
      syncAmountState(amount);
      if (depositErrorEl) depositErrorEl.textContent = "";
    });
  });

  closeBtn?.addEventListener("click", closeDepositModal);
  depositModal.addEventListener("click", (event) => {
    if (event.target === depositModal) closeDepositModal();
  });

  depositSubmitBtn?.addEventListener("click", () => {
    const amount = Math.max(25, Math.floor(Number(depositAmountInput?.value || 0)));
    if (!Number.isFinite(amount) || amount < 25) {
      if (depositErrorEl) depositErrorEl.textContent = "Mete yon montan ki valab, omwen 25 HTG.";
      return;
    }

    const user = auth.currentUser;
    if (!user?.uid) {
      if (depositErrorEl) depositErrorEl.textContent = "Ou dwe konekte pou fè depo a.";
      return;
    }

    const clientName =
      user.displayName?.trim()
      || user.email?.split("@")?.[0]?.trim()
      || "Itilizatè Kobposh";

    closeDepositModal();
    activePaymentModal = new PaymentModal({
      amount,
      theme: "kobposh",
      client: {
        id: user.uid,
        uid: user.uid,
        name: clientName,
        email: user.email || "",
        photoURL: user.photoURL || "",
      },
      cart: [
        {
          productId: "kobposh-deposit",
          name: "Depo Kobposh",
          price: amount,
          quantity: 1,
          image: "logokobpash.png",
        },
      ],
      imageBasePath: "./",
      onClose: () => {
        activePaymentModal = null;
        void refreshBalance();
        void refreshRecentMatches();
      },
      onSuccess: () => {
        void refreshBalance();
        void refreshRecentMatches();
      },
    });
  });

  syncAmountState(25);

  if (window.lucide) {
    window.lucide.createIcons();
  }

  return depositModal;
}

function openDepositModal() {
  const modal = ensureDepositModal();
  if (!modal) return;
  closeGamesModal();
  closeStakeModal();
  modal.classList.add("is-open");
  modal.setAttribute("aria-hidden", "false");
  document.body.classList.add("is-modal-open");
  depositAmountInput?.focus();
  depositAmountInput?.select?.();
}

function closeDepositModal() {
  if (!depositModal) return;
  depositModal.classList.remove("is-open");
  depositModal.setAttribute("aria-hidden", "true");
  document.body.classList.remove("is-modal-open");
  if (depositErrorEl) depositErrorEl.textContent = "";
}

buildHeroSlides();
initHeroRotation();

function formatBalance(value) {
  const amount = Number(value || 0);
  return `${new Intl.NumberFormat("en-US", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(amount)} HTG`;
}

function getBestFundingBalance(data = {}) {
  const playable = Number(data?.playableHtg);
  if (Number.isFinite(playable) && playable >= 0) return playable;

  const approved = Number(data?.approvedHtgAvailable);
  const provisional = Number(data?.provisionalHtgAvailable);
  if (Number.isFinite(approved) || Number.isFinite(provisional)) {
    return Math.max(0, (Number.isFinite(approved) ? approved : 0) + (Number.isFinite(provisional) ? provisional : 0));
  }

  const withdrawable = Number(data?.withdrawableHtg);
  if (Number.isFinite(withdrawable) && withdrawable >= 0) return withdrawable;

  return null;
}

async function refreshBalance() {
  if (!balanceEl) return;
  const user = auth.currentUser;
  if (!user?.uid) {
    balanceEl.textContent = "-- HTG";
    return;
  }

  try {
    const funding = await getDepositFundingStatusSecure({});
    const balance = getBestFundingBalance(funding);
    if (Number.isFinite(balance)) {
      balanceEl.textContent = formatBalance(balance);
      balanceEl.title = `Balans HTG: ${formatBalance(balance)}`;
      return;
    }
    balanceEl.textContent = "-- HTG";
  } catch (error) {
    console.warn("[KOBPOSH] balance refresh failed", error);
    balanceEl.textContent = "-- HTG";
  }
}

function formatMatchAmount(value) {
  const amount = Number(value || 0);
  return `${new Intl.NumberFormat("en-US", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(amount)} HTG`;
}

function formatMatchDate(value) {
  const date = value ? new Date(value) : null;
  if (!date || Number.isNaN(date.getTime())) return "";
  return new Intl.DateTimeFormat("fr-FR", {
    day: "2-digit",
    month: "short",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}

function inferMatchOutcome(docData = {}, uid = "") {
  const safeUid = String(uid || "").trim();
  const winnerUid = String(docData?.winnerUid || "").trim();
  const winnerType = String(docData?.winnerType || "").trim().toLowerCase();
  const playerUids = Array.isArray(docData?.playerUids)
    ? docData.playerUids.map((item) => String(item || "").trim()).filter(Boolean)
    : [];
  const won = winnerUid
    ? winnerUid === safeUid
    : winnerType === "human" && playerUids.includes(safeUid);
  const lost = !won;
  return {
    won,
    lost,
    label: won ? "Genyen" : "Pèdi",
  };
}

function buildMatchRecord(collectionKey, docSnap, uid) {
  const data = docSnap.data() || {};
  const playerUids = Array.isArray(data.playerUids)
    ? data.playerUids.map((item) => String(item || "").trim()).filter(Boolean)
    : [];
  const safeUid = String(uid || "").trim();
  const participantUid = String(data.uid || data.clientId || data.playerUid || "").trim();
  const winnerUid = String(data.winnerUid || "").trim();
  const isRelevant = (
    participantUid === safeUid
    || playerUids.includes(safeUid)
    || winnerUid === safeUid
  );
  if (!isRelevant) return null;

  const endedAtMs = Number(data.endedAtMs || data.endedAt || data.createdAtMs || 0);
  const startedAtMs = Number(data.startedAtMs || 0);
  const outcome = inferMatchOutcome(data, safeUid);
  const rewardDoes = Number(data.rewardAmountDoes || data.rewardDoes || 0);
  const stakeDoes = Number(data.stakeDoes || data.entryCostDoes || 0);
  const netDoes = outcome.won ? Math.max(0, rewardDoes || stakeDoes) : -Math.max(0, stakeDoes);
  return {
    id: String(docSnap.id || "").trim(),
    collectionKey,
    gameLabel: GAME_HISTORY_SOURCES.find((item) => item.collectionName === collectionKey)?.gameLabel || "Jeu",
    endedAtMs: Number.isFinite(endedAtMs) ? endedAtMs : 0,
    startedAtMs: Number.isFinite(startedAtMs) ? startedAtMs : 0,
    resultLabel: outcome.label,
    won: outcome.won,
    lost: outcome.lost,
    scoreLabel: String(data.scoreLabel || "").trim(),
    stakeDoes: Number.isFinite(stakeDoes) ? Math.max(0, Math.floor(stakeDoes)) : 0,
    rewardDoes: Number.isFinite(rewardDoes) ? Math.max(0, Math.floor(rewardDoes)) : 0,
    netDoes: Number.isFinite(netDoes) ? Math.trunc(netDoes) : 0,
    opponentLabel: String(data.opponentLabel || (data.botCount > 0 ? "Bot" : "") || "").trim(),
  };
}

async function loadRecentMatchesForUser(uid, offset = 0, pageSize = 3) {
  const safeUid = String(uid || "").trim();
  if (!safeUid) {
    return { rows: [], hasMore: false, total: 0 };
  }

  try {
    const payload = await getMyGameHistorySecure({
      uid: safeUid,
      pageSize,
      offset,
      game: "all",
      opponent: "all",
      result: "all",
    });
    return payload || { rows: [], hasMore: false, total: 0 };
  } catch (error) {
    console.warn("[KOBPOSH] recent matches load failed", error);
    return { rows: [], hasMore: false, total: 0 };
  }
}

function renderRecentMatches(rows = []) {
  if (!recentMatchesEl) return;
  if (!rows.length) {
    recentMatchesEl.innerHTML = "<li>Pa gen istwa jwèt pou kounye a.</li>";
    return;
  }

  recentMatchesEl.innerHTML = rows.map((row) => {
    const subtitleParts = [
      row.gameLabel,
      row.scoreLabel ? `Nòt ${row.scoreLabel}` : "",
      row.opponentLabel ? `Vs ${row.opponentLabel}` : "",
      row.endedAtMs ? formatMatchDate(row.endedAtMs) : "",
    ].filter(Boolean);

    return `
      <li class="recent-match">
        <div class="recent-match__top">
          <span class="recent-match__game">${row.gameLabel}</span>
          <strong class="recent-match__result recent-match__result--${row.won ? "win" : "loss"}">${row.resultLabel || (row.won ? "Genyen" : "Pèdi")}</strong>
        </div>
        <div class="recent-match__meta">${subtitleParts.join(" • ")}</div>
        <div class="recent-match__bottom">
          <span>${row.scoreLabel || "Match fini"}</span>
          <span>${row.netDoes > 0 ? `+${formatMatchAmount(row.netDoes)}` : row.netDoes < 0 ? `-${formatMatchAmount(Math.abs(row.netDoes))}` : "0 HTG"}</span>
        </div>
      </li>
    `;
  }).join("");
}

async function refreshRecentMatches() {
  if (!recentMatchesEl) return;
  const user = auth.currentUser;
  if (!user?.uid) {
    recentMatchesEl.innerHTML = "<li>Konekte pou w wè 3 dènye jwèt ou yo.</li>";
    return;
  }

  recentMatchesEl.innerHTML = "<li>Ap chaje...</li>";
  try {
    const payload = await loadRecentMatchesForUser(user.uid, 0, 3);
    renderRecentMatches(Array.isArray(payload?.rows) ? payload.rows : []);
  } catch (error) {
    console.warn("[KOBPOSH] recent matches refresh failed", error);
    recentMatchesEl.innerHTML = "<li>Nou pa ka chaje istwa jwèt la kounye a.</li>";
  }
}

function startBalanceRefreshLoop() {
  if (balanceRefreshTimer) {
    window.clearInterval(balanceRefreshTimer);
    balanceRefreshTimer = null;
  }
  balanceRefreshTimer = window.setInterval(() => {
    if (document.visibilityState !== "visible") return;
    void refreshBalance();
  }, BALANCE_REFRESH_MS);
}

function getCurrentAuthFields() {
  if (authMode === "signup") {
    return {
      identifier: String(signupUsernameEl?.value || "").trim(),
      phone: String(signupPhoneEl?.value || "").trim(),
      password: String(signupPasswordEl?.value || ""),
      confirmPassword: String(signupPasswordConfirmEl?.value || ""),
      ageAccepted: signupAgeEl?.checked === true,
      termsAccepted: signupTermsEl?.checked === true,
    };
  }

  return {
    identifier: String(loginIdentifierEl?.value || "").trim(),
    password: String(loginPasswordEl?.value || ""),
  };
}

function updateFormValidity() {
  const fields = getCurrentAuthFields();
  const loginBtn = authSubmitBtn;

  if (authMode === "signup") {
    const usernameOk = isValidUsername(fields.identifier);
    const phoneOk = isValidPhoneLogin(fields.phone);
    const passOk = String(fields.password || "").length >= 6;
    const confirmOk = String(fields.password || "") === String(fields.confirmPassword || "") && String(fields.confirmPassword || "").length >= 6;
    const ageOk = fields.ageAccepted === true;
    const termsOk = fields.termsAccepted === true;
    if (loginBtn) loginBtn.disabled = !(usernameOk && phoneOk && passOk && confirmOk && ageOk && termsOk);
    return { valid: usernameOk && phoneOk && passOk && confirmOk && ageOk && termsOk, ...fields };
  }

  const emailOk = isValidEmail(fields.identifier);
  const phoneOk = isValidPhoneLogin(fields.identifier);
  const usernameOk = isValidUsername(fields.identifier);
  const passOk = String(fields.password || "").length >= 6;
  if (loginBtn) loginBtn.disabled = !(passOk && (emailOk || phoneOk || usernameOk));
  return { valid: passOk && (emailOk || phoneOk || usernameOk), ...fields };
}

function setLoggedOutState(isLoggedOut) {
  const shouldLock = isLoggedOut && !isPublicView;
  document.body.classList.toggle("is-auth-locked", shouldLock);
  if (authScreenEl) {
    authScreenEl.hidden = !shouldLock;
  }
  if (shouldLock) {
    window.setTimeout(() => {
      loginIdentifierEl?.focus?.();
    }, 0);
  }
}

function setLoginError(message = "") {
  if (loginErrorEl) loginErrorEl.textContent = message;
}

function setPasswordVisibility(inputId, visible) {
  const inputEl = document.getElementById(inputId);
  const toggleBtn = document.querySelector(`[data-kobposh-toggle-password="${inputId}"]`);
  if (!inputEl || !toggleBtn) return;

  inputEl.type = visible ? "text" : "password";
  toggleBtn.setAttribute("aria-pressed", visible ? "true" : "false");
  toggleBtn.setAttribute("aria-label", visible ? "Maske modpas la" : "Montre modpas la");
  toggleBtn.innerHTML = visible
    ? '<i data-lucide="eye-off" class="icon auth-screen__password-icon" aria-hidden="true"></i>'
    : '<i data-lucide="eye" class="icon auth-screen__password-icon" aria-hidden="true"></i>';

  if (window.lucide) {
    window.lucide.createIcons();
  }
}

function setAuthMode(mode = "login") {
  authMode = mode === "signup" ? "signup" : "login";
  if (loginFieldsEl) loginFieldsEl.hidden = authMode === "signup";
  if (signupFieldsEl) signupFieldsEl.hidden = authMode !== "signup";
  if (authSubmitBtn) {
    authSubmitBtn.textContent = authMode === "signup" ? "Kreye kont" : "Konekte";
  }
  if (signupToggleBtn) {
    signupToggleBtn.textContent = authMode === "signup"
      ? "Mwen deja gen kont, konekte"
      : "Si w pa gen kont, kreye youn la";
  }
  if (authCardTitleEl) {
    authCardTitleEl.textContent = authMode === "signup" ? "KREYE KONT" : "KOBPOSH";
  }
  if (authCardSubtitleEl) {
    authCardSubtitleEl.textContent = authMode === "signup"
      ? "Kreye kont ou pou kontinye."
      : "Konekte pou kontinye.";
  }
}

passwordToggleBtns.forEach((btn) => {
  btn.addEventListener("click", () => {
    const inputId = btn.getAttribute("data-kobposh-toggle-password");
    if (!inputId) return;
    const inputEl = document.getElementById(inputId);
    if (!inputEl) return;
    setPasswordVisibility(inputId, inputEl.type === "password");
  });
});

loginFormEl?.addEventListener("submit", async (event) => {
  event.preventDefault();
  setLoginError("");
  const state = updateFormValidity();
  try {
    if (authMode === "signup") {
      if (!state.valid) {
        setLoginError("Vérifie tout chan yo avan ou kontinye.");
        return;
      }
      if (!isValidUsername(state.identifier)) {
        setLoginError("Antre yon username ki valab.");
        return;
      }
      if (!isValidPhoneLogin(state.phone)) {
        setLoginError("Antre yon numero ki valab.");
        return;
      }
      if (state.password !== state.confirmPassword) {
        setLoginError("Modpas verifikasyon an pa menm.");
        return;
      }
      if (!state.ageAccepted || !state.termsAccepted) {
        setLoginError("Ou dwe konfime ou gen plis pase 18 an epi ou aksepte kondisyon yo.");
        return;
      }
      await signupWithPhone(state.phone, state.password);
      await syncCurrentUserDisplayName(state.identifier);
    } else {
      if (!state.valid) {
        setLoginError("Antre yon username, email oswa numero ki valab ak modpas ou.");
        return;
      }
      if (isValidEmail(state.identifier)) {
        await loginWithEmail(state.identifier, state.password);
      } else if (isValidPhoneLogin(state.identifier)) {
        await loginWithPhone(state.identifier, state.password);
      } else if (isValidUsername(state.identifier)) {
        await loginWithUsername(state.identifier, state.password);
      } else {
        setLoginError("Antre yon username oswa yon email ki valab.");
        return;
      }
    }
  } catch (error) {
    setLoginError(formatAuthError(error, authMode === "signup" ? "Kreyasyon kont la pa mache." : "Koneksyon an pa mache."));
  }
});

signupToggleBtn?.addEventListener("click", () => {
  setLoginError("");
  setAuthMode(authMode === "signup" ? "login" : "signup");
});

[loginIdentifierEl, loginPasswordEl, signupUsernameEl, signupPhoneEl, signupPasswordEl, signupPasswordConfirmEl, signupAgeEl, signupTermsEl].forEach((input) => {
  input?.addEventListener("input", () => {
    setLoginError("");
    updateFormValidity();
  });
});

onAuthStateChanged(auth, (user) => {
  const loggedOut = !user;
  setLoggedOutState(loggedOut);
  if (accountLabelEl) {
    const label = loggedOut
      ? "Ou pagen kont"
      : String(user?.displayName || user?.email || user?.uid || "").split("@")[0] || "Ou pagen kont";
    accountLabelEl.textContent = label;
  }
  if (loggedOut) {
    setLoginError("");
    if (recentMatchesEl) recentMatchesEl.innerHTML = "<li>Konekte pou w wè 3 dènye match ou yo.</li>";
    if (balanceEl) balanceEl.textContent = "-- HTG";
    return;
  }
  void refreshBalance();
  void refreshRecentMatches();
});

document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "visible") {
    void refreshBalance();
    void refreshRecentMatches();
  }
});

startBalanceRefreshLoop();
void refreshBalance();
void refreshRecentMatches();
setAuthMode("login");
updateFormValidity();

function openGamesModal() {
  if (!gamesModal) return;
  gamesModal.classList.add("is-open");
  gamesModal.setAttribute("aria-hidden", "false");
  document.body.classList.add("is-modal-open");
}

function closeGamesModal() {
  if (!gamesModal) return;
  gamesModal.classList.remove("is-open");
  gamesModal.setAttribute("aria-hidden", "true");
  document.body.classList.remove("is-modal-open");
}

function openHistoryModalAndRefresh() {
  openHistoryModal();
}

openGamesModalBtn?.addEventListener("click", (event) => {
  event.preventDefault();
  openGamesModal();
});

document.querySelector("[data-open-history-modal]")?.addEventListener("click", (event) => {
  event.preventDefault();
  openHistoryModalAndRefresh();
});

openDepositModalBtns.forEach((btn) => {
  btn.addEventListener("click", () => {
    openDepositModal();
  });
});

withdrawalQuickBtn?.addEventListener("click", () => {
  openWithdrawalAgentModal();
});

document.querySelectorAll("[data-kobposh-launch-game]").forEach((item) => {
  item.addEventListener("click", (event) => {
    event.preventDefault();
    openStakeModal(item.getAttribute("data-kobposh-launch-game"));
  });
});

closeGamesModalBtn?.addEventListener("click", closeGamesModal);

window.addEventListener("keydown", (event) => {
  if (event.key !== "Escape") return;
  closeGamesModal();
  closeDepositModal();
  closeStakeModal();
  closeWithdrawalAgentModal();
  closeHistoryModal();
});
