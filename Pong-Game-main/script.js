const canvas = document.getElementById("pong");
const ctx = canvas.getContext("2d");
const leftScoreEl = document.getElementById("leftScore");
const rightScoreEl = document.getElementById("rightScore");
const rightPlayerLabelEl = document.getElementById("rightPlayerLabel");
const matchStatusEl = document.getElementById("matchStatus");
const replayBtn = document.getElementById("replayBtn");

const WIDTH = canvas.width;
const HEIGHT = canvas.height;

const PADDLE_WIDTH = 15;
const PADDLE_HEIGHT = 100;
const PADDLE_MARGIN = 20;
const PADDLE_SPEED = 5;

const BALL_SIZE = 16;
const BALL_START_SPEED = 6;
const BALL_MAX_SPEED = 12;
const BALL_ACCEL_PER_SECOND = 0.8;

const WIN_SCORE = 3;
const ROUND_COUNTDOWN_MS = 3000;
const MATCH_RESULT_DELAY_MS = 1200;
const OPPONENT_NAMES = Object.freeze([
  "Jerome",
  "Pierre",
  "Donal",
  "Mickael",
  "Kevin",
  "Stevenson",
  "James",
  "David",
  "Jonas",
  "Samuel",
  "Frantz",
  "Jean",
  "Rony",
  "Ralph",
  "Nixon",
  "Wilson",
  "Patrick",
  "Christo",
  "Dimitri",
  "Esteban",
  "Enzo",
  "Alex",
  "Nicolas",
  "Thomas",
  "Brandon",
  "Jordan",
  "Sebastien",
  "Claude",
  "Luckner",
  "Reginald",
  "Marlon",
  "Evans",
  "Cyril",
  "Didier",
  "Kervens",
  "Ricardo",
  "Christopher",
  "Evens",
  "Robenson",
  "Yvens",
  "Carlo",
  "Rodolphe",
  "Brice",
  "Gary",
  "Henry",
  "Ludovic",
  "Maxime",
  "Francois",
  "Benson",
  "Teddy",
]);

const leftPaddle = {
  x: PADDLE_MARGIN,
  y: HEIGHT / 2 - PADDLE_HEIGHT / 2,
  width: PADDLE_WIDTH,
  height: PADDLE_HEIGHT,
};

const rightPaddle = {
  x: WIDTH - PADDLE_MARGIN - PADDLE_WIDTH,
  y: HEIGHT / 2 - PADDLE_HEIGHT / 2,
  width: PADDLE_WIDTH,
  height: PADDLE_HEIGHT,
};

const ball = {
  x: WIDTH / 2 - BALL_SIZE / 2,
  y: HEIGHT / 2 - BALL_SIZE / 2,
  size: BALL_SIZE,
  speedX: BALL_START_SPEED,
  speedY: BALL_START_SPEED * (Math.random() * 2 - 1),
};

const AI_PROFILES = Object.freeze({
  soft: Object.freeze({
    key: "soft",
    label: "Mode detendu",
    maxSpeed: 5.1,
    reactionFrames: 1,
    deadZone: 12,
    noiseAmplitude: 10,
    hesitationChance: 0.04,
    throwChance: 0.2,
    throwDurationFrames: 26,
    throwOffset: 230,
  }),
  normal: Object.freeze({
    key: "normal",
    label: "Mode normal",
    maxSpeed: 5.4,
    reactionFrames: 1,
    deadZone: 10,
    noiseAmplitude: 7,
    hesitationChance: 0.03,
    throwChance: 0.14,
    throwDurationFrames: 20,
    throwOffset: 190,
  }),
  ultra: Object.freeze({
    key: "ultra",
    label: "Mode ultra",
    maxSpeed: 6.6,
    reactionFrames: 1,
    deadZone: 6,
    noiseAmplitude: 2,
    hesitationChance: 0.008,
    throwChance: 0,
    throwDurationFrames: 0,
    throwOffset: 0,
  }),
});

function parseAiProfile() {
  try {
    const params = new URLSearchParams(window.location.search);
    const raw = String(params.get("aiProfile") || "normal").trim().toLowerCase();
    return AI_PROFILES[raw] || AI_PROFILES.normal;
  } catch (_) {
    return AI_PROFILES.normal;
  }
}

function parseFriendMode() {
  try {
    const params = new URLSearchParams(window.location.search);
    return params.get("friendMode") === "1";
  } catch (_) {
    return false;
  }
}

function parseOpponentName() {
  try {
    const params = new URLSearchParams(window.location.search);
    const raw = String(params.get("opponentName") || "").trim();
    return raw.slice(0, 40);
  } catch (_) {
    return "";
  }
}

const aiProfile = parseAiProfile();
const friendMode = parseFriendMode();
const matchId = `pong_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 9)}`;
function buildOpponentHandle() {
  const baseName = (OPPONENT_NAMES[Math.floor(Math.random() * OPPONENT_NAMES.length)] || "Adversaire").toLowerCase();
  const suffix = Math.floor(Math.random() * 90) + 10; // 10..99
  return `${baseName}${suffix}`;
}

const opponentName = parseOpponentName() || buildOpponentHandle();

let leftScore = 0;
let rightScore = 0;
let roundCountdownMs = ROUND_COUNTDOWN_MS;
let roundRunning = false;
let matchOver = false;
let matchOverDelayMs = MATCH_RESULT_DELAY_MS;
let lastFrameTs = performance.now();

let aiReactionTick = 0;
let aiNoiseTick = 0;
let aiAimOffset = 0;
let aiThrowFrames = 0;
let aiThrowDirection = 1;
let postedResult = false;

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function updateMatchStatus(text) {
  if (!matchStatusEl) return;
  matchStatusEl.textContent = text;
}

function renderOpponentLabel() {
  if (!rightPlayerLabelEl) return;
  rightPlayerLabelEl.textContent = opponentName;
}

function renderScore() {
  if (leftScoreEl) leftScoreEl.textContent = String(leftScore);
  if (rightScoreEl) rightScoreEl.textContent = String(rightScore);
}

function resetBall(towardPlayer = null) {
  ball.x = WIDTH / 2 - BALL_SIZE / 2;
  ball.y = HEIGHT / 2 - BALL_SIZE / 2;
  const randomY = BALL_START_SPEED * (Math.random() * 2 - 1);
  ball.speedY = Math.abs(randomY) < 1 ? (Math.random() < 0.5 ? -2 : 2) : randomY;

  if (towardPlayer === "left") {
    ball.speedX = -BALL_START_SPEED;
  } else if (towardPlayer === "right") {
    ball.speedX = BALL_START_SPEED;
  } else {
    ball.speedX = BALL_START_SPEED * (Math.random() < 0.5 ? 1 : -1);
  }
}

function accelerateBall(deltaMs) {
  if (!Number.isFinite(deltaMs) || deltaMs <= 0) return;
  const currentSpeed = Math.hypot(ball.speedX, ball.speedY);
  if (currentSpeed <= 0 || currentSpeed >= BALL_MAX_SPEED) return;
  const deltaSeconds = deltaMs / 1000;
  const targetSpeed = Math.min(BALL_MAX_SPEED, currentSpeed + (BALL_ACCEL_PER_SECOND * deltaSeconds));
  const ratio = targetSpeed / currentSpeed;
  ball.speedX *= ratio;
  ball.speedY *= ratio;
}

function startRoundCountdown() {
  roundRunning = false;
  roundCountdownMs = ROUND_COUNTDOWN_MS;
  aiReactionTick = 0;
  aiNoiseTick = 0;
  aiAimOffset = 0;
}

function startRoundNow() {
  roundRunning = true;
  updateMatchStatus(friendMode ? "Salle entre amis · Partie en cours" : "Partie en cours");
}

function moveAiPaddle() {
  aiReactionTick += 1;
  aiNoiseTick += 1;

  if (aiReactionTick < aiProfile.reactionFrames) {
    return;
  }
  aiReactionTick = 0;

  if (aiNoiseTick >= 18) {
    aiNoiseTick = 0;
    aiAimOffset = (Math.random() * 2 - 1) * aiProfile.noiseAmplitude;
  }

  if (Math.random() < aiProfile.hesitationChance) {
    return;
  }

  const centerY = rightPaddle.y + rightPaddle.height / 2;
  const ballCenterY = ball.y + ball.size / 2;
  const ballComingToAi = ball.speedX > 0;
  const ballNearAiSide = ball.x > WIDTH * 0.58;

  if (
    aiThrowFrames <= 0
    && ballComingToAi
    && ballNearAiSide
    && Math.random() < aiProfile.throwChance
  ) {
    aiThrowFrames = Math.max(0, aiProfile.throwDurationFrames);
    aiThrowDirection = Math.random() < 0.5 ? -1 : 1;
  }

  let targetY = ballCenterY + aiAimOffset;
  if (aiThrowFrames > 0) {
    aiThrowFrames -= 1;
    targetY = ballCenterY + (aiThrowDirection * aiProfile.throwOffset);
  }
  const diff = targetY - centerY;

  if (Math.abs(diff) <= aiProfile.deadZone) {
    return;
  }

  const step = Math.sign(diff) * Math.min(Math.abs(diff), aiProfile.maxSpeed);
  rightPaddle.y += step;
  rightPaddle.y = clamp(rightPaddle.y, 0, HEIGHT - rightPaddle.height);
}

function endMatch() {
  matchOver = true;
  roundRunning = false;
  matchOverDelayMs = MATCH_RESULT_DELAY_MS;
  const winner = leftScore > rightScore ? "user" : "ai";
  updateMatchStatus(
    winner === "user"
      ? `Victoire ${leftScore}-${rightScore} !`
      : `Defaite ${leftScore}-${rightScore}.`
  );
  if (replayBtn) replayBtn.classList.remove("hidden");
}

function postMatchResult() {
  if (postedResult) return;
  postedResult = true;
  const payload = {
    type: "pong:matchResult",
    payload: {
      matchId,
      aiProfile: aiProfile.key,
      winner: leftScore > rightScore ? "user" : "ai",
      leftScore,
      rightScore,
      winScore: WIN_SCORE,
      endedAt: Date.now(),
    },
  };
  window.parent?.postMessage(payload, window.location.origin);
}

function scorePoint(side) {
  if (side === "left") {
    leftScore += 1;
    resetBall("right");
  } else {
    rightScore += 1;
    resetBall("left");
  }
  renderScore();

  if (leftScore >= WIN_SCORE || rightScore >= WIN_SCORE) {
    endMatch();
    return;
  }
  updateMatchStatus(`Manche terminee (${leftScore}-${rightScore}). Nouvelle manche...`);
  startRoundCountdown();
}

function draw() {
  ctx.clearRect(0, 0, WIDTH, HEIGHT);

  const gradient = ctx.createLinearGradient(0, 0, WIDTH, HEIGHT);
  gradient.addColorStop(0, "#0a1933");
  gradient.addColorStop(1, "#121f3d");
  ctx.fillStyle = gradient;
  ctx.fillRect(0, 0, WIDTH, HEIGHT);

  ctx.fillStyle = "rgba(148, 163, 184, 0.45)";
  for (let i = 0; i < HEIGHT; i += 30) {
    ctx.fillRect(WIDTH / 2 - 2, i, 4, 20);
  }

  ctx.fillStyle = "#f57c00";
  ctx.fillRect(leftPaddle.x, leftPaddle.y, leftPaddle.width, leftPaddle.height);

  ctx.fillStyle = "#2e8dff";
  ctx.fillRect(rightPaddle.x, rightPaddle.y, rightPaddle.width, rightPaddle.height);

  ctx.beginPath();
  ctx.arc(ball.x + ball.size / 2, ball.y + ball.size / 2, ball.size / 2, 0, Math.PI * 2, false);
  ctx.fillStyle = "#f8fafc";
  ctx.fill();

  if (!roundRunning && !matchOver) {
    const seconds = Math.max(1, Math.ceil(roundCountdownMs / 1000));
    ctx.fillStyle = "rgba(4, 8, 18, 0.55)";
    ctx.fillRect(0, 0, WIDTH, HEIGHT);
    ctx.fillStyle = "#f8fafc";
    ctx.font = "700 34px Poppins, sans-serif";
    ctx.textAlign = "center";
    ctx.fillText(String(seconds), WIDTH / 2, HEIGHT / 2);
  }

  if (matchOver) {
    ctx.fillStyle = "rgba(4, 8, 18, 0.6)";
    ctx.fillRect(0, 0, WIDTH, HEIGHT);
    ctx.fillStyle = "#f8fafc";
    ctx.font = "700 30px Poppins, sans-serif";
    ctx.textAlign = "center";
    ctx.fillText(leftScore > rightScore ? "VICTOIRE" : "DEFAITE", WIDTH / 2, HEIGHT / 2 - 16);
    ctx.font = "600 20px Poppins, sans-serif";
    ctx.fillText(`${leftScore} - ${rightScore}`, WIDTH / 2, HEIGHT / 2 + 20);
  }
}

function update(deltaMs) {
  if (matchOver) {
    matchOverDelayMs -= deltaMs;
    if (matchOverDelayMs <= 0) {
      postMatchResult();
    }
    return;
  }

  if (!roundRunning) {
    roundCountdownMs -= deltaMs;
      updateMatchStatus(
      `Debut dans ${Math.max(1, Math.ceil(roundCountdownMs / 1000))}s`
      );
    if (roundCountdownMs <= 0) {
      startRoundNow();
    }
    return;
  }

  ball.x += ball.speedX;
  ball.y += ball.speedY;
  accelerateBall(deltaMs);

  if (ball.y <= 0) {
    ball.y = 0;
    ball.speedY *= -1;
  }
  if (ball.y + ball.size >= HEIGHT) {
    ball.y = HEIGHT - ball.size;
    ball.speedY *= -1;
  }

  if (
    ball.x <= leftPaddle.x + leftPaddle.width &&
    ball.y + ball.size >= leftPaddle.y &&
    ball.y <= leftPaddle.y + leftPaddle.height
  ) {
    const speed = Math.hypot(ball.speedX, ball.speedY);
    ball.x = leftPaddle.x + leftPaddle.width;
    let collidePoint = (ball.y + ball.size / 2) - (leftPaddle.y + leftPaddle.height / 2);
    collidePoint = collidePoint / (leftPaddle.height / 2);
    const angle = collidePoint * Math.PI / 4;
    ball.speedX = Math.abs(speed * Math.cos(angle));
    ball.speedY = speed * Math.sin(angle);
  }

  if (
    ball.x + ball.size >= rightPaddle.x &&
    ball.y + ball.size >= rightPaddle.y &&
    ball.y <= rightPaddle.y + rightPaddle.height
  ) {
    const speed = Math.hypot(ball.speedX, ball.speedY);
    ball.x = rightPaddle.x - ball.size;
    let collidePoint = (ball.y + ball.size / 2) - (rightPaddle.y + rightPaddle.height / 2);
    collidePoint = collidePoint / (rightPaddle.height / 2);
    const angle = collidePoint * Math.PI / 4;
    ball.speedX = -Math.abs(speed * Math.cos(angle));
    ball.speedY = speed * Math.sin(angle);
  }

  if (ball.x < 0) {
    scorePoint("right");
    return;
  }
  if (ball.x > WIDTH) {
    scorePoint("left");
    return;
  }

  moveAiPaddle();
}

function updatePlayerPaddleFromClientY(clientY) {
  const rect = canvas.getBoundingClientRect();
  const localY = clientY - rect.top;
  const scaledY = (localY / Math.max(1, rect.height)) * HEIGHT;
  leftPaddle.y = clamp(scaledY - leftPaddle.height / 2, 0, HEIGHT - leftPaddle.height);
}

canvas.addEventListener("pointerdown", (event) => {
  updatePlayerPaddleFromClientY(event.clientY);
});

canvas.addEventListener("pointermove", (event) => {
  updatePlayerPaddleFromClientY(event.clientY);
});

replayBtn?.addEventListener("click", () => {
  window.parent?.postMessage({ type: "pong:playAgain" }, window.location.origin);
});

window.addEventListener("message", (event) => {
  if (event.origin !== window.location.origin) return;
  const data = event?.data;
  if (!data || typeof data !== "object") return;
  if (data.type !== "pong:setAiProfile") return;
  const incoming = String(data.payload?.aiProfile || "").toLowerCase();
  const nextProfile = AI_PROFILES[incoming];
  if (!nextProfile || roundRunning || matchOver || leftScore !== 0 || rightScore !== 0) return;
  updateMatchStatus("Debut dans 3s");
});

function gameLoop(ts) {
  const deltaMs = Math.max(0, ts - lastFrameTs);
  lastFrameTs = ts;
  update(deltaMs);
  draw();
  window.requestAnimationFrame(gameLoop);
}

renderScore();
resetBall();
startRoundCountdown();
renderOpponentLabel();
updateMatchStatus("Debut dans 3s");
window.requestAnimationFrame(gameLoop);
