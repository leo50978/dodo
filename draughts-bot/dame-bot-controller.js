(function (root) {
  "use strict";

  const BOT_PLAYER = 1;
  const BOT_THINK_MIN_MS = 820;
  const BOT_THINK_MAX_MS = 1650;
  const BOT_STEP_MIN_MS = 230;
  const BOT_STEP_MAX_MS = 360;

  function currentPlayerFromTurn(turn) {
    return ((Number(turn) || 0) % 2) ^ 1;
  }

  function wait(ms) {
    return new Promise((resolve) => {
      setTimeout(resolve, ms);
    });
  }

  function randInt(min, max) {
    return Math.floor(min + Math.random() * (max - min + 1));
  }

  function getBoardElement() {
    return document.getElementById("board");
  }

  function getField(boardEl, r, c) {
    return boardEl.querySelector(".line" + r + ".column" + c);
  }

  function readSettings() {
    const getChecked = (id, fallback) => {
      const el = document.getElementById(id);
      return el ? !!el.checked : fallback;
    };
    return {
      forceAttack: getChecked("forceattack", true),
      allowBackwardAttack: getChecked("backwardattack", false),
      allowQueenRun: getChecked("queenfreerun", true),
      allowQueenAttackRun: getChecked("queenattackrun", false),
    };
  }

  function extractMapFromBoard(boardEl) {
    const map = [];
    for (let r = 0; r < 8; r += 1) {
      const row = [];
      for (let c = 0; c < 8; c += 1) {
        const field = getField(boardEl, r, c);
        if (!field) {
          row.push(0);
          continue;
        }
        const pieceEl = field.querySelector("a.player0, a.player1");
        if (!pieceEl) {
          row.push(0);
          continue;
        }
        const isQ = pieceEl.className.indexOf("queen") !== -1;
        const isP0 = pieceEl.className.indexOf("player0") !== -1;
        if (isP0) {
          row.push(isQ ? 3 : 1);
        } else {
          row.push(isQ ? 4 : 2);
        }
      }
      map.push(row);
    }
    return map;
  }

  function boardHasWinnerBanner() {
    const banner = document.getElementById("banner-msg");
    if (!banner) return false;
    return banner.style.display !== "none";
  }

  function setPlayerLabels() {
    const p0 = document.querySelector("#dash .player0 h3");
    const p1 = document.querySelector("#dash .player1 h3");
    if (p0) p0.textContent = "Ou";
    if (p1) p1.textContent = "Bot Dame";
  }

  function updateBotStatus(text) {
    const node = document.getElementById("botStatus");
    if (!node) return;
    node.textContent = text;
  }

  function makeMaskFromCaptures(boardEl, captures) {
    const opponents = [];
    for (let i = 0; i < captures.length; i += 1) {
      const [cr, cc] = captures[i];
      const field = getField(boardEl, cr, cc);
      if (!field) continue;
      const opp = field.querySelector("a.player0, a.player1");
      if (opp) opponents.push(opp);
    }
    return { data: { opponent: opponents } };
  }

  async function executeTurnSequence(boardEl, sequence) {
    for (let i = 0; i < sequence.length; i += 1) {
      if (boardHasWinnerBanner()) return false;
      const step = sequence[i];
      const fromField = getField(boardEl, step.from[0], step.from[1]);
      const toField = getField(boardEl, step.to[0], step.to[1]);
      if (!fromField || !toField) return false;
      const pieceEl = fromField.querySelector("a.player0, a.player1");
      if (!pieceEl || !pieceEl.data || typeof pieceEl.data.moveTo !== "function") return false;
      const mask = makeMaskFromCaptures(boardEl, step.captures || []);
      pieceEl.data.moveTo(toField, mask);
      await wait(randInt(BOT_STEP_MIN_MS, BOT_STEP_MAX_MS));
    }
    return true;
  }

  async function waitForBoardReady(maxWaitMs) {
    const start = Date.now();
    while (Date.now() - start < maxWaitMs) {
      const boardEl = getBoardElement();
      if (boardEl && boardEl.data) {
        return boardEl;
      }
      await wait(60);
    }
    return null;
  }

  async function initDameBot() {
    if (!root.DameBotEngine) {
      console.error("[DAME_BOT] engine missing");
      return;
    }

    const boardEl = await waitForBoardReady(5000);
    if (!boardEl) {
      console.error("[DAME_BOT] board unavailable");
      return;
    }

    setPlayerLabels();
    updateBotStatus("Bot la pare.");
    let isThinking = false;
    let botPositionHistory = Object.create(null);
    const debugMode = (() => {
      try {
        const params = new URLSearchParams(window.location.search);
        return params.get("botDebug") === "1";
      } catch (_err) {
        return false;
      }
    })();

    function notePosition(map, activePlayer, settings) {
      if (!root.DameBotEngine || typeof root.DameBotEngine.positionKey !== "function") return;
      const key = root.DameBotEngine.positionKey(map, activePlayer, settings);
      botPositionHistory[key] = (botPositionHistory[key] || 0) + 1;
    }

    async function maybePlayBotTurn(source) {
      if (isThinking) return;
      if (!boardEl.isConnected) return;
      if (boardHasWinnerBanner()) return;
      if (currentPlayerFromTurn(boardEl.turn) !== BOT_PLAYER) return;

      isThinking = true;
      try {
        updateBotStatus("Bot ap reflechi...");
        await wait(randInt(BOT_THINK_MIN_MS, BOT_THINK_MAX_MS));
        if (boardHasWinnerBanner()) return;
        if (currentPlayerFromTurn(boardEl.turn) !== BOT_PLAYER) return;

        const map = extractMapFromBoard(boardEl);
        const settings = readSettings();
        notePosition(map, BOT_PLAYER, settings);
        const chosen = root.DameBotEngine.chooseMove(map, BOT_PLAYER, settings, {
          maxDepth: 11,
          timeBudgetMs: 1800,
          historyCounts: botPositionHistory,
          debug: debugMode,
        });

        if (!chosen || chosen.length === 0) {
          console.warn("[DAME_BOT] no legal move", { source });
          updateBotStatus("Bot pa jwenn mouvman.");
          return;
        }

        updateBotStatus("Bot ap jwe kou li...");
        const ok = await executeTurnSequence(boardEl, chosen);
        if (!ok) {
          console.warn("[DAME_BOT] sequence interrupted", { source });
          updateBotStatus("Mouvman bot la entewonp.");
          return;
        }
        updateBotStatus("A ou jwe.");
        if (debugMode && root.DameBotEngine.getLastDebugInfo) {
          console.log("[DAME_BOT_DEBUG]", root.DameBotEngine.getLastDebugInfo());
        }
      } catch (err) {
        console.error("[DAME_BOT] turn failed", err);
        updateBotStatus("Bot an erè tanporè.");
      } finally {
        isThinking = false;
      }
    }

    boardEl.addEventListener("changeturn", () => {
      if (currentPlayerFromTurn(boardEl.turn) === BOT_PLAYER) {
        updateBotStatus("Bot ap reflechi...");
      } else {
        updateBotStatus("A ou jwe.");
      }
      void maybePlayBotTurn("changeturn");
    });

    boardEl.addEventListener("created", () => {
      botPositionHistory = Object.create(null);
      void maybePlayBotTurn("created");
    });

    setTimeout(() => {
      void maybePlayBotTurn("init");
    }, 380);
  }

  root.DameBotController = {
    init: initDameBot,
  };

  window.addEventListener("load", () => {
    void initDameBot();
  });
})(window);
