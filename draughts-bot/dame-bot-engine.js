(function (root) {
  "use strict";

  const DEFAULT_SETTINGS = Object.freeze({
    forceAttack: true,
    allowBackwardAttack: false,
    allowQueenRun: true,
    allowQueenAttackRun: false,
  });
  const OPENING_BOOK_PREFS = Object.freeze({
    centralSquares: Object.freeze([
      "2:3", "2:5", "3:2", "3:4", "3:6",
      "4:1", "4:3", "4:5", "5:2", "5:4",
    ]),
    developmentSquares: Object.freeze([
      "2:1", "2:7", "3:0", "3:6",
      "4:7", "5:0", "5:6",
    ]),
  });
  const OPENING_BOOK_VERSION = "book-v1";
  const TABLEBASE_VERSION = "tb6-v1";
  const TABLEBASE_STORAGE_KEY = `dame_tablebase_cache_${TABLEBASE_VERSION}`;
  const TABLEBASE_MAX_CACHE_ENTRIES = 1200;
  const TABLEBASE_PROBE_MAX_MS = 220;
  const TABLEBASE_PROBE_MAX_NODES = 42000;
  const TABLEBASE_PROBE_MAX_PLIES = 24;
  const FORCED_LINE_BRANCH_LIMIT = 2;
  const FORCED_LINE_MAX_DEPTH = 10;
  const FORCED_LINE_MAX_NODES = 18000;
  const FORCED_LINE_MAX_MS = 110;
  let LAST_DEBUG_INFO = null;
  let TABLEBASE_MEMORY = null;
  let OPENING_BOOK_DB = null;

  function cloneMap(map) {
    return map.map((row) => row.slice());
  }

  function inBounds(r, c) {
    return r >= 0 && r < 8 && c >= 0 && c < 8;
  }

  function ownerOf(piece) {
    if (piece === 1 || piece === 3) return 0;
    if (piece === 2 || piece === 4) return 1;
    return -1;
  }

  function isQueen(piece) {
    return piece === 3 || piece === 4;
  }

  function isOpponent(piece, player) {
    const owner = ownerOf(piece);
    return owner !== -1 && owner !== player;
  }

  function asQueen(piece, player) {
    return player === 0 ? 3 : 4;
  }

  function isPromotionRow(player, row) {
    return player === 0 ? row === 7 : row === 0;
  }

  function createStep(fromR, fromC, toR, toC, captures) {
    return {
      from: [fromR, fromC],
      to: [toR, toC],
      captures: captures || [],
      isCapture: (captures || []).length > 0,
    };
  }

  function applyStep(map, step, player, promoteNow) {
    const next = cloneMap(map);
    let piece = next[step.from[0]][step.from[1]];
    next[step.from[0]][step.from[1]] = 0;
    for (let i = 0; i < step.captures.length; i += 1) {
      const [cr, cc] = step.captures[i];
      next[cr][cc] = 0;
    }
    if (promoteNow !== false && !isQueen(piece) && isPromotionRow(player, step.to[0])) {
      piece = asQueen(piece, player);
    }
    next[step.to[0]][step.to[1]] = piece;
    return next;
  }

  function applyTurnMove(map, turnMove, player) {
    let cur = map;
    for (let i = 0; i < turnMove.length; i += 1) {
      // Mirror draughts.js behavior: during multi-capture, promotion is not
      // used to compute already-forced continuation from the same click flow.
      cur = applyStep(cur, turnMove[i], player, false);
    }
    if (turnMove.length > 0) {
      const last = turnMove[turnMove.length - 1];
      const toR = last.to[0];
      const toC = last.to[1];
      const piece = cur[toR][toC];
      if (!isQueen(piece) && isPromotionRow(player, toR)) {
        cur = cloneMap(cur);
        cur[toR][toC] = asQueen(piece, player);
      }
    }
    return cur;
  }

  function findNextFieldsLikeEngine(currentR, currentC, fieldR, fieldC, player, backward) {
    let line = fieldR + (player === 0 ? 1 : -1);
    const out = [];

    const pushCoord = (r, c) => {
      if (inBounds(r, c)) out.push([r, c]);
    };

    if (fieldR === currentR && fieldC === currentC) {
      pushCoord(line, fieldC - 1);
      pushCoord(line, fieldC + 1);
    } else if (currentC < fieldC) {
      pushCoord(line, fieldC + 1);
    } else {
      pushCoord(line, fieldC - 1);
    }

    if (backward) {
      line += player === 0 ? -2 : 2;
      if (fieldR === currentR && fieldC === currentC) {
        pushCoord(line, fieldC - 1);
        pushCoord(line, fieldC + 1);
      } else {
        out.length = 0;
        if (currentC < fieldC) {
          pushCoord(line, fieldC + 1);
        } else {
          pushCoord(line, fieldC - 1);
        }
      }
    }
    return out;
  }

  function generateManMoves(map, r, c, player) {
    const coords = findNextFieldsLikeEngine(r, c, r, c, player, false);
    const moves = [];
    for (let i = 0; i < coords.length; i += 1) {
      const [nr, nc] = coords[i];
      if (map[nr][nc] === 0) {
        moves.push(createStep(r, c, nr, nc, []));
      }
    }
    return moves;
  }

  function generateAttacksFromField(
    map,
    currentR,
    currentC,
    fromR,
    fromC,
    player,
    settings,
    attackTurn,
    allowBackwardAttackParam
  ) {
    const opponentId = player ^ 1;
    let isABK = !!allowBackwardAttackParam;
    if (!allowBackwardAttackParam) {
      isABK = !!settings.allowBackwardAttack && (attackTurn | 0) > 0;
    }

    const atkFields = findNextFieldsLikeEngine(currentR, currentC, fromR, fromC, player, isABK);
    const attacks = [];

    for (let i = 0; i < atkFields.length; i += 1) {
      const [atkR, atkC] = atkFields[i];
      if (ownerOf(map[atkR][atkC]) !== opponentId) continue;

      let dir = isABK;
      if (isABK) {
        dir = !(((currentR > atkR) ? 1 : 0) ^ opponentId);
      }

      const moveFields = findNextFieldsLikeEngine(currentR, currentC, atkR, atkC, player, !!dir);
      for (let j = 0; j < moveFields.length; j += 1) {
        const [toR, toC] = moveFields[j];
        if (map[toR][toC] !== 0) continue;
        attacks.push(createStep(currentR, currentC, toR, toC, [[atkR, atkC]]));
      }
    }
    return attacks;
  }

  function generateQueenMoveCoords(map, r, c, player, settings) {
    const moves = findNextFieldsLikeEngine(r, c, r, c, player, true).filter(
      ([nr, nc]) => map[nr][nc] === 0
    );

    if (!settings.allowQueenRun) {
      return moves;
    }

    for (let i = 0; i < moves.length; i += 1) {
      const [mr, mc] = moves[i];
      let dir = mr > r;
      dir = !!(dir ^ (player ^ 1));
      const ext = findNextFieldsLikeEngine(r, c, mr, mc, player, dir).filter(
        ([nr, nc]) => map[nr][nc] === 0
      );
      if (ext.length > 0) {
        moves.push(ext[0]);
      }
    }

    return moves;
  }

  function generateQueenAttacks(map, r, c, player, settings, attackTurn) {
    let attacks = generateAttacksFromField(
      map,
      r,
      c,
      r,
      c,
      player,
      settings,
      attackTurn,
      true
    );

    if (!settings.allowQueenRun) {
      return attacks;
    }

    const moveCoords = generateQueenMoveCoords(map, r, c, player, settings);
    for (let i = 0; i < moveCoords.length; i += 1) {
      const [mr, mc] = moveCoords[i];
      let dir = mr > r;
      dir = !!(dir ^ (player ^ 1));
      attacks = attacks.concat(
        generateAttacksFromField(map, r, c, mr, mc, player, settings, attackTurn, dir)
      );
    }

    if (!settings.allowQueenAttackRun) {
      return attacks;
    }

    for (let i = 0; i < attacks.length; i += 1) {
      const attack = attacks[i];
      let dir = attack.to[0] > r;
      dir = !!(dir ^ (player ^ 1));
      const fields = findNextFieldsLikeEngine(r, c, attack.to[0], attack.to[1], player, dir);
      for (let j = 0; j < fields.length; j += 1) {
        const [fr, fc] = fields[j];
        if (map[fr][fc] !== 0) continue;
        attacks.push(createStep(r, c, fr, fc, attack.captures.slice()));
      }
    }

    return attacks;
  }

  function generatePieceMoves(map, r, c, player, settings, attackTurn) {
    const piece = map[r][c];
    if (piece === 0 || ownerOf(piece) !== player) return { moves: [], captures: [] };
    if (isQueen(piece)) {
      const queenMoves = generateQueenMoveCoords(map, r, c, player, settings)
        .map(([toR, toC]) => createStep(r, c, toR, toC, []));
      return {
        moves: queenMoves,
        captures: generateQueenAttacks(map, r, c, player, settings, attackTurn),
      };
    }
    return {
      moves: generateManMoves(map, r, c, player),
      captures: generateAttacksFromField(map, r, c, r, c, player, settings, attackTurn, false),
    };
  }

  function expandCaptureChains(map, player, atR, atC, settings, prefix, out, attackTurn) {
    const pieceData = generatePieceMoves(map, atR, atC, player, settings, attackTurn);
    if (pieceData.captures.length === 0) {
      if (prefix.length > 0) out.push(prefix);
      return;
    }
    for (let i = 0; i < pieceData.captures.length; i += 1) {
      const step = pieceData.captures[i];
      const nextMap = applyStep(map, step, player, false);
      const nr = step.to[0];
      const nc = step.to[1];
      expandCaptureChains(nextMap, player, nr, nc, settings, prefix.concat(step), out, (attackTurn | 0) + 1);
    }
  }

  function generateTurnMoves(map, player, settings) {
    const cfg = Object.assign({}, DEFAULT_SETTINGS, settings || {});
    const captureTurns = [];
    const quietTurns = [];

    for (let r = 0; r < 8; r += 1) {
      for (let c = 0; c < 8; c += 1) {
        if (ownerOf(map[r][c]) !== player) continue;
        const pieceData = generatePieceMoves(map, r, c, player, cfg, 0);
        if (pieceData.captures.length > 0) {
          expandCaptureChains(map, player, r, c, cfg, [], captureTurns, 0);
        } else {
          for (let i = 0; i < pieceData.moves.length; i += 1) {
            quietTurns.push([pieceData.moves[i]]);
          }
        }
      }
    }

    if (captureTurns.length > 0 && cfg.forceAttack) {
      return captureTurns;
    }
    return captureTurns.length > 0 ? captureTurns.concat(quietTurns) : quietTurns;
  }

  function materialScore(map, player) {
    let score = 0;
    for (let r = 0; r < 8; r += 1) {
      for (let c = 0; c < 8; c += 1) {
        const piece = map[r][c];
        if (piece === 0) continue;
        const owner = ownerOf(piece);
        const base = isQueen(piece) ? 245 : 120;
        const advance = !isQueen(piece)
          ? (owner === 0 ? r : 7 - r) * 3
          : 0;
        const center =
          r >= 2 && r <= 5 && c >= 2 && c <= 5 ? (isQueen(piece) ? 14 : 8) : 0;
        const edge =
          (r === 0 || r === 7 || c === 0 || c === 7) && !isQueen(piece) ? 5 : 0;
        const val = base + advance + center + edge;
        score += owner === player ? val : -val;
      }
    }
    return score;
  }

  function capturePressure(moves) {
    let best = 0;
    for (let i = 0; i < moves.length; i += 1) {
      best = Math.max(best, totalCaptureCount(moves[i]));
    }
    return best;
  }

  function threatenedSetFromMoves(moves) {
    const set = new Set();
    for (let i = 0; i < moves.length; i += 1) {
      const seq = moves[i];
      for (let j = 0; j < seq.length; j += 1) {
        const step = seq[j];
        for (let k = 0; k < step.captures.length; k += 1) {
          const [r, c] = step.captures[k];
          set.add(String(r) + ":" + String(c));
        }
      }
    }
    return set;
  }

  function promotionDistanceScore(map, player) {
    let score = 0;
    for (let r = 0; r < 8; r += 1) {
      for (let c = 0; c < 8; c += 1) {
        const piece = map[r][c];
        if (ownerOf(piece) !== player || isQueen(piece)) continue;
        const dist = player === 0 ? (7 - r) : r;
        const laneBonus = (c >= 2 && c <= 5) ? 1 : 0;
        score += Math.max(0, 7 - dist) + laneBonus;
      }
    }
    return score;
  }

  function queenOppositionScore(map, player) {
    const myQueens = [];
    const opQueens = [];
    for (let r = 0; r < 8; r += 1) {
      for (let c = 0; c < 8; c += 1) {
        const piece = map[r][c];
        if (!isQueen(piece)) continue;
        if (ownerOf(piece) === player) myQueens.push([r, c]);
        else opQueens.push([r, c]);
      }
    }
    if (myQueens.length === 0 || opQueens.length === 0) return 0;
    let score = 0;
    for (let i = 0; i < myQueens.length; i += 1) {
      let best = Infinity;
      for (let j = 0; j < opQueens.length; j += 1) {
        const d = Math.abs(myQueens[i][0] - opQueens[j][0]) + Math.abs(myQueens[i][1] - opQueens[j][1]);
        if (d < best) best = d;
      }
      score += Math.max(0, 10 - best);
    }
    return score;
  }

  function evaluateExchangeIntent(map, player, settings) {
    const tb = probeTablebase(map, player, settings);
    if (tb.hit) {
      if (tb.result === "win") return 4;
      if (tb.result === "loss") return -4;
      return 0;
    }
    const matLead = materialOnlyScore(map, player);
    const myMoves = generateTurnMoves(map, player, settings);
    const opMoves = generateTurnMoves(map, player ^ 1, settings);
    const myBestCap = bestCaptureValueFromMoves(map, myMoves);
    const opBestCap = bestCaptureValueFromMoves(map, opMoves);
    if (matLead > 120) {
      return (myBestCap > 0 ? 2 : 0) + (opMoves.length <= 6 ? 1 : 0);
    }
    if (matLead < -120) {
      return -((opBestCap > 0 ? 2 : 0) + (myMoves.length <= 6 ? 1 : 0));
    }
    return (myBestCap - opBestCap) > 120 ? 1 : 0;
  }

  function evaluate(map, player, settings) {
    const ownMoves = generateTurnMoves(map, player, settings);
    const oppMoves = generateTurnMoves(map, player ^ 1, settings);
    if (ownMoves.length === 0) return -100000;
    if (oppMoves.length === 0) return 100000;

    const pieceStats = countPiecesByType(map);
    const totalPieces = pieceStats.total;
    const phase = classifyPhase(totalPieces);
    const isEndgame = phase === "endgame";

    const myMen = player === 0 ? pieceStats.p0Men : pieceStats.p1Men;
    const opMen = player === 0 ? pieceStats.p1Men : pieceStats.p0Men;
    const myQueens = player === 0 ? pieceStats.p0Queens : pieceStats.p1Queens;
    const opQueens = player === 0 ? pieceStats.p1Queens : pieceStats.p0Queens;

    const materialDeltaMen = myMen - opMen;
    const materialDeltaQueens = myQueens - opQueens;
    const materialDelta = materialDeltaMen * 100 + materialDeltaQueens * 290;

    const myCenter = countCenterControl(map, player);
    const opCenter = countCenterControl(map, player ^ 1);
    const myAdv = countAdvancedPawns(map, player);
    const opAdv = countAdvancedPawns(map, player ^ 1);
    const myProt = countProtectedPieces(map, player);
    const opProt = countProtectedPieces(map, player ^ 1);
    const myHang = countHangingPieces(map, player, oppMoves);
    const opHang = countHangingPieces(map, player ^ 1, ownMoves);
    const myPromo = countPromotionThreats(map, player);
    const opPromo = countPromotionThreats(map, player ^ 1);
    const myPromoNow = countImmediatePromotionMoves(map, ownMoves, player);
    const opPromoNow = countImmediatePromotionMoves(map, oppMoves, player ^ 1);
    const myQueenAct = countQueenActivity(map, player, settings);
    const opQueenAct = countQueenActivity(map, player ^ 1, settings);
    const oppBestCaptureValue = bestCaptureValueFromMoves(map, oppMoves);
    const myBestCaptureValue = bestCaptureValueFromMoves(map, ownMoves);
    const myTrapped = countTrappedPieces(map, player, settings);
    const opTrapped = countTrappedPieces(map, player ^ 1, settings);
    const myKeySquares = evaluateKeySquares(map, player);
    const opKeySquares = evaluateKeySquares(map, player ^ 1);
    const myBackRank = countBackRankGuard(map, player);
    const opBackRank = countBackRankGuard(map, player ^ 1);
    const myChains = countPawnChains(map, player);
    const opChains = countPawnChains(map, player ^ 1);
    const myWingBalance = evaluateWingBalance(map, player);
    const opWingBalance = evaluateWingBalance(map, player ^ 1);
    const myDead = countDeadPieces(map, player, settings);
    const opDead = countDeadPieces(map, player ^ 1, settings);

    const ownThreat = threatenedSetFromMoves(ownMoves);
    const oppThreat = threatenedSetFromMoves(oppMoves);
    const ownCapturePressure = capturePressure(ownMoves);
    const oppCapturePressure = capturePressure(oppMoves);
    const myTempo =
      (oppMoves.length <= 2 ? (3 - oppMoves.length) : 0)
      + (ownCapturePressure > 0 ? 1 : 0)
      + (myPromoNow > 0 ? 2 : 0)
      + (myQueenAct > opQueenAct ? 1 : 0);
    const opTempo =
      (ownMoves.length <= 2 ? (3 - ownMoves.length) : 0)
      + (oppCapturePressure > 0 ? 1 : 0)
      + (opPromoNow > 0 ? 2 : 0)
      + (opQueenAct > myQueenAct ? 1 : 0);

    const phaseW = phase === "opening"
      ? { men: 100, queens: 270, mobility: 10, center: 20, advanced: 16, prot: 28, hang: 30, promo: 26, promoNow: 230, pressure: 14, threat: 10, qAct: 6, bestCap: 0.45 }
      : phase === "middlegame"
        ? { men: 100, queens: 290, mobility: 13, center: 15, advanced: 22, prot: 25, hang: 31, promo: 40, promoNow: 300, pressure: 20, threat: 13, qAct: 9, bestCap: 0.55 }
        : { men: 92, queens: 330, mobility: 22, center: 8, advanced: 28, prot: 20, hang: 26, promo: 62, promoNow: 430, pressure: 24, threat: 16, qAct: 16, bestCap: 0.62 };
    const finalStrict = isEndgame
      ? { backRank: 12, chains: 10, wing: 8, dead: 20, promoDistance: 28, queenOpposition: 24, exchange: 18 }
      : null;

    let score = 0;
    score += materialDeltaMen * phaseW.men;
    score += materialDeltaQueens * phaseW.queens;
    score += (ownMoves.length - oppMoves.length) * phaseW.mobility;
    score += (myCenter - opCenter) * phaseW.center;
    score += (myAdv - opAdv) * phaseW.advanced;
    score += (myProt - opProt) * phaseW.prot;
    score -= (myHang - opHang) * phaseW.hang;
    score += (myPromo - opPromo) * phaseW.promo;
    score += (myPromoNow - opPromoNow) * phaseW.promoNow;
    score += (ownCapturePressure - oppCapturePressure) * phaseW.pressure;
    score += (ownThreat.size - oppThreat.size) * phaseW.threat;
    score += (myQueenAct - opQueenAct) * phaseW.qAct;
    score += (myBestCaptureValue - oppBestCaptureValue) * phaseW.bestCap;
    score += (myTempo - opTempo) * (isEndgame ? 20 : 13);
    score += (opTrapped - myTrapped) * (isEndgame ? 44 : 30);
    score += (myKeySquares - opKeySquares) * (phase === "opening" ? 22 : 14);
    score += (myBackRank - opBackRank) * (isEndgame ? 8 : 12);
    score += (myChains - opChains) * (phase === "opening" ? 12 : 8);
    score += (myWingBalance - opWingBalance) * 5;
    score += (opDead - myDead) * (isEndgame ? 20 : 12);

    // Critical anti-blunder guardrails.
    score -= opPromoNow * 1400;
    score -= Math.max(0, oppBestCaptureValue - 180) * 1.35;

    // If ahead, simplify. If behind, seek complexity.
    const complexity = ownMoves.length + oppMoves.length + ownCapturePressure + oppCapturePressure;
    if (materialDelta > 120) {
      score += (26 - totalPieces) * 10;
      score -= complexity * 1.5;
    } else if (materialDelta < -120) {
      score -= (26 - totalPieces) * 6;
      score += complexity * 2.2;
    }

    if (finalStrict) {
      // FINAL_STRICT: prioritize conversion and defensive resources.
      const myPromoDist = promotionDistanceScore(map, player);
      const opPromoDist = promotionDistanceScore(map, player ^ 1);
      const myQueenOpp = queenOppositionScore(map, player);
      const opQueenOpp = queenOppositionScore(map, player ^ 1);
      const exchangeScore = evaluateExchangeIntent(map, player, settings);
      score += (myPromoDist - opPromoDist) * finalStrict.promoDistance;
      score += (myQueenOpp - opQueenOpp) * finalStrict.queenOpposition;
      score += exchangeScore * finalStrict.exchange;
    }

    // Keep positional tie-breaker.
    score += materialScore(map, player) * 0.22;

    return score;
  }

  function quickMoveOrdering(turnMove, player, urgency) {
    let score = 0;
    for (let i = 0; i < turnMove.length; i += 1) {
      const step = turnMove[i];
      score += step.captures.length * 150;
      if (isPromotionRow(player, step.to[0])) {
        score += 80;
      }
    }
    score += urgency || 0;
    score += turnMove.length;
    return score;
  }

  function moveKey(turnMove) {
    if (!turnMove || turnMove.length === 0) return "empty";
    let out = "";
    for (let i = 0; i < turnMove.length; i += 1) {
      const s = turnMove[i];
      out += `${s.from[0]}${s.from[1]}-${s.to[0]}${s.to[1]}:`;
      for (let j = 0; j < s.captures.length; j += 1) {
        out += `${s.captures[j][0]}${s.captures[j][1]}`;
      }
      out += "|";
    }
    return out;
  }

  function moveSpec(turnMove) {
    let out = [];
    for (let i = 0; i < turnMove.length; i += 1) {
      const s = turnMove[i];
      out.push(`${s.from[0]}${s.from[1]}-${s.to[0]}${s.to[1]}`);
    }
    return out.join("x");
  }

  function parseMoveSpec(spec) {
    if (!spec || typeof spec !== "string") return null;
    const parts = spec.split("x");
    const out = [];
    for (let i = 0; i < parts.length; i += 1) {
      const p = parts[i];
      const chunks = p.split("-");
      if (chunks.length !== 2 || chunks[0].length !== 2 || chunks[1].length !== 2) return null;
      const r1 = Number(chunks[0][0]);
      const c1 = Number(chunks[0][1]);
      const r2 = Number(chunks[1][0]);
      const c2 = Number(chunks[1][1]);
      if (![r1, c1, r2, c2].every((v) => Number.isInteger(v) && v >= 0 && v <= 7)) return null;
      out.push({ from: [r1, c1], to: [r2, c2] });
    }
    return out;
  }

  function mirrorMapHoriz(map) {
    const mirrored = [];
    for (let r = 0; r < 8; r += 1) {
      mirrored.push([]);
      for (let c = 0; c < 8; c += 1) {
        mirrored[r][7 - c] = map[r][c];
      }
    }
    return mirrored;
  }

  function mirrorMoveSpec(spec) {
    const parsed = parseMoveSpec(spec);
    if (!parsed) return spec;
    const mirroredParts = [];
    for (let i = 0; i < parsed.length; i += 1) {
      const s = parsed[i];
      mirroredParts.push(`${s.from[0]}${7 - s.from[1]}-${s.to[0]}${7 - s.to[1]}`);
    }
    return mirroredParts.join("x");
  }

  function getHistoryScore(ctx, key) {
    return ctx.historyHeuristic.get(key) || 0;
  }

  function isKillerMove(ctx, depth, key) {
    const killers = ctx.killerMoves.get(depth);
    if (!killers) return false;
    return killers[0] === key || killers[1] === key;
  }

  function addKillerMove(ctx, depth, key) {
    const killers = ctx.killerMoves.get(depth) || ["", ""];
    if (killers[0] === key) return;
    killers[1] = killers[0];
    killers[0] = key;
    ctx.killerMoves.set(depth, killers);
  }

  function addHistoryScore(ctx, key, depth) {
    const prev = ctx.historyHeuristic.get(key) || 0;
    ctx.historyHeuristic.set(key, prev + depth * depth + 1);
  }

  function hasAnyCaptureMove(moves) {
    for (let i = 0; i < moves.length; i += 1) {
      if (totalCaptureCount(moves[i]) > 0) return true;
    }
    return false;
  }

  function topMovesByUrgency(map, moves, player, settings, maxCount) {
    const ranked = moves.map((mv) => ({
      mv,
      urgency: quickMoveOrdering(
        mv,
        player,
        computeMoveUrgency(map, mv, player, settings, { quick: true })
      ),
    }));
    ranked.sort((a, b) => b.urgency - a.urgency);
    return ranked.slice(0, Math.max(1, maxCount | 0)).map((x) => x.mv);
  }

  function mapKey(map, player, settings) {
    let out = String(player) + "|";
    out += settings.forceAttack ? "1" : "0";
    out += settings.allowBackwardAttack ? "1" : "0";
    out += settings.allowQueenRun ? "1" : "0";
    out += settings.allowQueenAttackRun ? "1" : "0";
    out += "|";
    for (let r = 0; r < 8; r += 1) {
      out += map[r].join("");
      out += "/";
    }
    return out;
  }

  function canonicalPositionKey(map, player, settings) {
    const direct = mapKey(map, player, settings);
    const mirroredMap = mirrorMapHoriz(map);
    const mirrored = mapKey(mirroredMap, player, settings);
    if (mirrored < direct) {
      return { key: mirrored, mirrored: true };
    }
    return { key: direct, mirrored: false };
  }

  function serializeTablebaseEntry(entry) {
    if (!entry || !entry.result) return "";
    const dtw = Number.isInteger(entry.dtw) ? entry.dtw : "";
    return `${entry.result},${dtw},${entry.score | 0}`;
  }

  function deserializeTablebaseEntry(raw) {
    if (!raw || typeof raw !== "string") return null;
    const [result, dtwRaw, scoreRaw] = raw.split(",");
    if (!result) return null;
    const dtw = dtwRaw === "" ? undefined : Number(dtwRaw);
    const score = Number(scoreRaw || 0);
    return {
      result,
      dtw: Number.isFinite(dtw) ? dtw : undefined,
      score: Number.isFinite(score) ? score : 0,
      hit: true,
      source: "cache",
    };
  }

  function loadPersistentTablebaseCache() {
    const out = new Map();
    if (typeof window === "undefined" || !window.localStorage) return out;
    try {
      const raw = window.localStorage.getItem(TABLEBASE_STORAGE_KEY);
      if (!raw) return out;
      const parsed = JSON.parse(raw);
      if (!parsed || typeof parsed !== "object") return out;
      const keys = Object.keys(parsed);
      for (let i = 0; i < keys.length; i += 1) {
        const k = keys[i];
        const v = deserializeTablebaseEntry(parsed[k]);
        if (v) out.set(k, v);
      }
    } catch (_err) {
      return out;
    }
    return out;
  }

  function persistTablebaseCache(cacheMap) {
    if (typeof window === "undefined" || !window.localStorage) return;
    try {
      const obj = Object.create(null);
      const keys = Array.from(cacheMap.keys());
      const tail = keys.slice(Math.max(0, keys.length - TABLEBASE_MAX_CACHE_ENTRIES));
      for (let i = 0; i < tail.length; i += 1) {
        const k = tail[i];
        const v = cacheMap.get(k);
        obj[k] = serializeTablebaseEntry(v);
      }
      window.localStorage.setItem(TABLEBASE_STORAGE_KEY, JSON.stringify(obj));
    } catch (_err) {
      // Ignore storage quota / private mode errors.
    }
  }

  function decodeCompressedTablebase(compressed) {
    const out = new Map();
    if (!compressed || typeof compressed !== "string") return out;
    const entries = compressed.split("|");
    for (let i = 0; i < entries.length; i += 1) {
      const e = entries[i];
      if (!e) continue;
      const sep = e.indexOf(">");
      if (sep <= 0) continue;
      const key = e.slice(0, sep);
      const payload = deserializeTablebaseEntry(e.slice(sep + 1));
      if (payload) out.set(key, { ...payload, source: "seed" });
    }
    return out;
  }

  function getTablebaseMemory() {
    if (TABLEBASE_MEMORY) return TABLEBASE_MEMORY;
    const seedData = root.DameBotTablebaseData || null;
    const seedVersion = seedData && seedData.version ? String(seedData.version) : TABLEBASE_VERSION;
    const seedCompressed = seedData && typeof seedData.compressed === "string" ? seedData.compressed : "";
    const seedMap = decodeCompressedTablebase(seedCompressed);
    const persistent = seedVersion === TABLEBASE_VERSION ? loadPersistentTablebaseCache() : new Map();
    TABLEBASE_MEMORY = {
      version: TABLEBASE_VERSION,
      map: new Map([...seedMap, ...persistent]),
      writes: 0,
    };
    return TABLEBASE_MEMORY;
  }

  function totalCaptureCount(turnMove) {
    let score = 0;
    for (let i = 0; i < turnMove.length; i += 1) {
      score += turnMove[i].captures.length;
    }
    return score;
  }

  function countPiecesByType(map) {
    const out = {
      p0Men: 0,
      p0Queens: 0,
      p1Men: 0,
      p1Queens: 0,
      total: 0,
    };
    for (let r = 0; r < 8; r += 1) {
      for (let c = 0; c < 8; c += 1) {
        const v = map[r][c];
        if (v === 0) continue;
        out.total += 1;
        if (v === 1) out.p0Men += 1;
        else if (v === 2) out.p1Men += 1;
        else if (v === 3) out.p0Queens += 1;
        else if (v === 4) out.p1Queens += 1;
      }
    }
    return out;
  }

  function countCenterControl(map, player) {
    let score = 0;
    for (let r = 2; r <= 5; r += 1) {
      for (let c = 2; c <= 5; c += 1) {
        const v = map[r][c];
        if (ownerOf(v) !== player) continue;
        score += isQueen(v) ? 2 : 1;
      }
    }
    return score;
  }

  function countAdvancedPawns(map, player) {
    let score = 0;
    for (let r = 0; r < 8; r += 1) {
      for (let c = 0; c < 8; c += 1) {
        const v = map[r][c];
        if (ownerOf(v) !== player || isQueen(v)) continue;
        score += player === 0 ? r : (7 - r);
      }
    }
    return score;
  }

  function countPromotionThreats(map, player) {
    let threats = 0;
    for (let r = 0; r < 8; r += 1) {
      for (let c = 0; c < 8; c += 1) {
        const v = map[r][c];
        if (ownerOf(v) !== player || isQueen(v)) continue;
        const dir = player === 0 ? 1 : -1;
        const nr = r + dir;
        if (nr < 0 || nr > 7) continue;
        for (let dc = -1; dc <= 1; dc += 2) {
          const nc = c + dc;
          if (!inBounds(nr, nc)) continue;
          if (map[nr][nc] === 0 && isPromotionRow(player, nr)) {
            threats += 1;
          }
        }
      }
    }
    return threats;
  }

  function countImmediatePromotionMoves(map, moves, player) {
    let count = 0;
    for (let i = 0; i < moves.length; i += 1) {
      const seq = moves[i];
      if (!seq || seq.length === 0) continue;
      const first = seq[0];
      const fromPiece = map[first.from[0]][first.from[1]];
      if (isQueen(fromPiece)) continue;
      const last = seq[seq.length - 1];
      if (isPromotionRow(player, last.to[0])) {
        count += 1;
      }
    }
    return count;
  }

  function countQueenActivity(map, player, settings) {
    let activity = 0;
    for (let r = 0; r < 8; r += 1) {
      for (let c = 0; c < 8; c += 1) {
        const v = map[r][c];
        if (ownerOf(v) !== player || !isQueen(v)) continue;
        const pm = generatePieceMoves(map, r, c, player, settings, 0);
        activity += pm.moves.length * 2 + pm.captures.length * 3;
      }
    }
    return activity;
  }

  function countSafeReplies(nextMap, player, settings, oppMoves) {
    // player = side that just played the candidate move.
    // We check how many opponent replies avoid immediate tactical pressure.
    const opponent = player ^ 1;
    const maxScan = Math.min(oppMoves.length, 8);
    let safe = 0;
    for (let i = 0; i < maxScan; i += 1) {
      const reply = oppMoves[i];
      const afterReply = applyTurnMove(nextMap, reply, opponent);
      const myMoves = generateTurnMoves(afterReply, player, settings);
      const myPromoNow = countImmediatePromotionMoves(afterReply, myMoves, player);
      const myBestCap = bestCaptureValueFromMoves(afterReply, myMoves);
      const pressure = myPromoNow * 300 + myBestCap * 1.2 + capturePressure(myMoves) * 90;
      if (pressure < 260) {
        safe += 1;
      }
    }
    return safe;
  }

  function detectDoubleThreatFeatures(map, move, player, settings) {
    const next = applyTurnMove(map, move, player);
    const oppMoves = generateTurnMoves(next, player ^ 1, settings);
    const myMoves = generateTurnMoves(next, player, settings);
    const currentOppMoves = generateTurnMoves(map, player ^ 1, settings);
    const currentOppPromoNow = countImmediatePromotionMoves(map, currentOppMoves, player ^ 1);
    const currentOppPromoSoon = countPromotionThreats(map, player ^ 1);
    const nextOppPromoNow = countImmediatePromotionMoves(next, oppMoves, player ^ 1);
    const nextOppPromoSoon = countPromotionThreats(next, player ^ 1);
    const safeReplies = countSafeReplies(next, player, settings, oppMoves);

    const features = {
      createPromoNow: countImmediatePromotionMoves(next, myMoves, player) > 0,
      createCaptureThreat: bestCaptureValueFromMoves(next, myMoves) >= 120,
      blockOppPromo:
        (currentOppPromoNow > nextOppPromoNow)
        || (currentOppPromoSoon > nextOppPromoSoon),
      forceReply: oppMoves.length <= 1 || safeReplies <= 1,
      attackHighValue: bestCaptureValueFromMoves(next, myMoves) >= 300,
    };

    let count = 0;
    if (features.createPromoNow) count += 1;
    if (features.createCaptureThreat) count += 1;
    if (features.blockOppPromo) count += 1;
    if (features.forceReply) count += 1;
    if (features.attackHighValue) count += 1;

    return {
      ...features,
      count,
      isDoubleThreat: count >= 2,
      isTripleThreat: count >= 3,
    };
  }

  function pieceTacticalValue(piece) {
    if (piece === 3 || piece === 4) return 300;
    if (piece === 1 || piece === 2) return 120;
    return 0;
  }

  function bestCaptureValueFromMoves(map, moves) {
    let best = 0;
    for (let i = 0; i < moves.length; i += 1) {
      const seq = moves[i];
      let sum = 0;
      for (let j = 0; j < seq.length; j += 1) {
        const step = seq[j];
        for (let k = 0; k < step.captures.length; k += 1) {
          const [r, c] = step.captures[k];
          sum += pieceTacticalValue(map[r][c]);
        }
      }
      if (sum > best) best = sum;
    }
    return best;
  }

  function countProtectedPieces(map, player) {
    let count = 0;
    const dirs = [[1, -1], [1, 1], [-1, -1], [-1, 1]];
    for (let r = 0; r < 8; r += 1) {
      for (let c = 0; c < 8; c += 1) {
        const v = map[r][c];
        if (ownerOf(v) !== player) continue;
        let protectedByFriend = false;
        for (let i = 0; i < dirs.length; i += 1) {
          const nr = r + dirs[i][0];
          const nc = c + dirs[i][1];
          if (!inBounds(nr, nc)) continue;
          if (ownerOf(map[nr][nc]) === player) {
            protectedByFriend = true;
            break;
          }
        }
        if (protectedByFriend) count += 1;
      }
    }
    return count;
  }

  function countTrappedPieces(map, player, settings) {
    let trapped = 0;
    for (let r = 0; r < 8; r += 1) {
      for (let c = 0; c < 8; c += 1) {
        const v = map[r][c];
        if (ownerOf(v) !== player) continue;
        const pm = generatePieceMoves(map, r, c, player, settings, 0);
        const totalMoves = pm.moves.length + pm.captures.length;
        if (totalMoves === 0) {
          trapped += 2;
          continue;
        }
        if (totalMoves === 1 && pm.captures.length === 0) {
          trapped += 1;
        }
      }
    }
    return trapped;
  }

  function evaluateKeySquares(map, player) {
    // Strategic squares for 8x8 play: center pivots and promotion lanes.
    const keySquares = [
      [2, 3], [2, 4], [3, 2], [3, 5], [4, 2], [4, 5], [5, 3], [5, 4],
      [1, 2], [1, 4], [6, 3], [6, 5],
    ];
    let score = 0;
    for (let i = 0; i < keySquares.length; i += 1) {
      const [r, c] = keySquares[i];
      const piece = map[r][c];
      if (ownerOf(piece) !== player) continue;
      score += isQueen(piece) ? 3 : 2;
    }

    // Promotion entry lane control.
    const promoRow = player === 0 ? 6 : 1;
    for (let c = 0; c < 8; c += 1) {
      const piece = map[promoRow][c];
      if (ownerOf(piece) === player && !isQueen(piece)) {
        score += 2;
      }
    }
    return score;
  }

  function countBackRankGuard(map, player) {
    const row = player === 0 ? 0 : 7;
    let guards = 0;
    for (let c = 0; c < 8; c += 1) {
      const piece = map[row][c];
      if (ownerOf(piece) !== player) continue;
      guards += isQueen(piece) ? 1 : 2;
    }
    return guards;
  }

  function countPawnChains(map, player) {
    let chains = 0;
    const dir = player === 0 ? 1 : -1;
    for (let r = 0; r < 8; r += 1) {
      for (let c = 0; c < 8; c += 1) {
        const piece = map[r][c];
        if (ownerOf(piece) !== player || isQueen(piece)) continue;
        const nr = r - dir;
        if (!inBounds(nr, c - 1) && !inBounds(nr, c + 1)) continue;
        if (inBounds(nr, c - 1) && ownerOf(map[nr][c - 1]) === player) chains += 1;
        if (inBounds(nr, c + 1) && ownerOf(map[nr][c + 1]) === player) chains += 1;
      }
    }
    return chains;
  }

  function evaluateWingBalance(map, player) {
    let left = 0;
    let right = 0;
    for (let r = 0; r < 8; r += 1) {
      for (let c = 0; c < 8; c += 1) {
        const piece = map[r][c];
        if (ownerOf(piece) !== player) continue;
        const w = isQueen(piece) ? 2 : 1;
        if (c <= 3) left += w;
        else right += w;
      }
    }
    return -Math.abs(left - right);
  }

  function countDeadPieces(map, player, settings) {
    let dead = 0;
    for (let r = 0; r < 8; r += 1) {
      for (let c = 0; c < 8; c += 1) {
        if (ownerOf(map[r][c]) !== player) continue;
        const pieceMoves = generatePieceMoves(map, r, c, player, settings, 0);
        const options = pieceMoves.moves.length + pieceMoves.captures.length;
        if (options === 0) {
          dead += 2;
        } else if (options === 1 && pieceMoves.captures.length === 0) {
          dead += 1;
        }
      }
    }
    return dead;
  }

  function detectForcedOutcomeLocal(map, player, settings, maxPlies) {
    const seen = new Map();
    let curMap = map;
    let curPlayer = player;
    const limit = Math.max(2, maxPlies | 0);
    for (let ply = 0; ply < limit; ply += 1) {
      const key = mapKey(curMap, curPlayer, settings);
      const cnt = (seen.get(key) || 0) + 1;
      seen.set(key, cnt);
      if (cnt >= 3) {
        return { type: "draw", ply };
      }
      const moves = generateTurnMoves(curMap, curPlayer, settings);
      if (moves.length === 0) {
        return { type: curPlayer === player ? "loss" : "win", ply };
      }
      if (moves.length > 1) {
        return { type: "unclear", ply };
      }
      curMap = applyTurnMove(curMap, moves[0], curPlayer);
      curPlayer ^= 1;
    }
    return { type: "unclear", ply: limit };
  }

  function decodeWdlScore(result, dtw) {
    const d = Number.isFinite(dtw) ? Math.max(0, dtw) : 0;
    if (result === "win") return 90000 - d * 12;
    if (result === "loss") return -90000 + d * 12;
    return 0;
  }

  function solveTablebaseWdl(map, player, settings, limits) {
    const maxPlies = Math.max(6, limits.maxPlies | 0);
    const deadline = Date.now() + Math.max(40, limits.maxMs | 0);
    const maxNodes = Math.max(1500, limits.maxNodes | 0);
    let nodes = 0;
    const memo = new Map();
    const lineRep = new Map();

    function dfs(curMap, curPlayer, depth) {
      if (Date.now() > deadline) return null;
      nodes += 1;
      if (nodes > maxNodes) return null;

      const c = canonicalPositionKey(curMap, curPlayer, settings);
      const rep = (lineRep.get(c.key) || 0) + 1;
      lineRep.set(c.key, rep);
      if (rep >= 3) {
        lineRep.set(c.key, rep - 1);
        if (rep - 1 <= 0) lineRep.delete(c.key);
        return { result: "draw", dtw: depth };
      }

      if (depth >= maxPlies) {
        lineRep.set(c.key, rep - 1);
        if (rep - 1 <= 0) lineRep.delete(c.key);
        return { result: "draw", dtw: depth };
      }

      const memoKey = `${c.key}|${maxPlies - depth}`;
      const hit = memo.get(memoKey);
      if (hit) {
        lineRep.set(c.key, rep - 1);
        if (rep - 1 <= 0) lineRep.delete(c.key);
        return hit;
      }

      const moves = generateTurnMoves(curMap, curPlayer, settings);
      if (moves.length === 0) {
        const out = { result: curPlayer === player ? "loss" : "win", dtw: depth };
        memo.set(memoKey, out);
        lineRep.set(c.key, rep - 1);
        if (rep - 1 <= 0) lineRep.delete(c.key);
        return out;
      }

      let bestWin = null;
      let bestDraw = null;
      let bestLoss = null;

      for (let i = 0; i < moves.length; i += 1) {
        const next = applyTurnMove(curMap, moves[i], curPlayer);
        const child = dfs(next, curPlayer ^ 1, depth + 1);
        if (!child) {
          lineRep.set(c.key, rep - 1);
          if (rep - 1 <= 0) lineRep.delete(c.key);
          return null;
        }
        if (child.result === "loss") {
          const cand = { result: "win", dtw: child.dtw };
          if (!bestWin || cand.dtw < bestWin.dtw) bestWin = cand;
        } else if (child.result === "draw") {
          const cand = { result: "draw", dtw: child.dtw };
          if (!bestDraw || cand.dtw < bestDraw.dtw) bestDraw = cand;
        } else {
          const cand = { result: "loss", dtw: child.dtw };
          if (!bestLoss || cand.dtw > bestLoss.dtw) bestLoss = cand;
        }
      }

      const out = bestWin || bestDraw || bestLoss || { result: "draw", dtw: depth };
      memo.set(memoKey, out);
      lineRep.set(c.key, rep - 1);
      if (rep - 1 <= 0) lineRep.delete(c.key);
      return out;
    }

    const solved = dfs(map, player, 0);
    if (!solved) return null;
    return {
      hit: true,
      result: solved.result,
      dtw: solved.dtw,
      score: decodeWdlScore(solved.result, solved.dtw),
      source: "solver",
    };
  }

  function probeTablebase(map, player, settings) {
    const pieces = countPiecesByType(map);
    if (pieces.total > 6) return { hit: false };

    const keyData = canonicalPositionKey(map, player, settings);
    const mem = getTablebaseMemory();
    const memHit = mem.map.get(keyData.key);
    if (memHit) {
      return {
        hit: true,
        result: memHit.result,
        dtw: memHit.dtw,
        score: Number.isFinite(memHit.score) ? memHit.score : decodeWdlScore(memHit.result, memHit.dtw),
        source: memHit.source || "cache",
      };
    }

    const solved = solveTablebaseWdl(map, player, settings, {
      maxMs: TABLEBASE_PROBE_MAX_MS,
      maxNodes: TABLEBASE_PROBE_MAX_NODES,
      maxPlies: TABLEBASE_PROBE_MAX_PLIES,
    });
    if (!solved || !solved.hit) return { hit: false };

    mem.map.set(keyData.key, {
      result: solved.result,
      dtw: solved.dtw,
      score: solved.score,
      source: "cache",
    });
    mem.writes += 1;
    if (mem.writes % 8 === 0) {
      persistTablebaseCache(mem.map);
    }
    return solved;
  }

  function searchForcedLine(map, player, settings, depthLimit) {
    const maxDepth = Math.max(2, Math.min(depthLimit | 0, FORCED_LINE_MAX_DEPTH));
    const deadline = Date.now() + FORCED_LINE_MAX_MS;
    let nodes = 0;

    function dfs(curMap, curPlayer, depth) {
      if (Date.now() > deadline) return null;
      nodes += 1;
      if (nodes > FORCED_LINE_MAX_NODES) return null;

      const moves = generateTurnMoves(curMap, curPlayer, settings);
      if (moves.length === 0) {
        return { result: curPlayer === player ? "loss" : "win", pv: [] };
      }
      if (depth >= maxDepth) {
        return { result: "unclear", pv: [] };
      }

      const ranked = topMovesByUrgency(curMap, moves, curPlayer, settings, FORCED_LINE_BRANCH_LIMIT);
      let sawDraw = false;
      let firstDrawPv = null;
      let worstLossPv = null;
      for (let i = 0; i < ranked.length; i += 1) {
        const mv = ranked[i];
        const next = applyTurnMove(curMap, mv, curPlayer);
        const child = dfs(next, curPlayer ^ 1, depth + 1);
        if (!child) return null;
        if (child.result === "loss") {
          return { result: "win", pv: [mv].concat(child.pv || []) };
        }
        if (child.result === "draw") {
          sawDraw = true;
          if (!firstDrawPv) firstDrawPv = [mv].concat(child.pv || []);
        } else if (!worstLossPv) {
          worstLossPv = [mv].concat(child.pv || []);
        }
      }
      if (sawDraw) return { result: "draw", pv: firstDrawPv || [] };
      return { result: "loss", pv: worstLossPv || [] };
    }

    const res = dfs(map, player, 0);
    if (!res || res.result === "unclear") {
      return { found: false, resultType: "unclear", score: 0, pv: [] };
    }
    const dtw = (res.pv && res.pv.length) || 0;
    return {
      found: true,
      resultType: res.result,
      score: decodeWdlScore(res.result, dtw),
      pv: res.pv || [],
    };
  }

  function validateSacrifice(map, move, player, settings) {
    const next = applyTurnMove(map, move, player);
    const immediateSwing = materialOnlyScore(next, player) - materialOnlyScore(map, player);
    if (immediateSwing >= -60) {
      return { isSacrifice: false, valid: false, score: 0 };
    }

    const oppMoves = topMovesByUrgency(next, generateTurnMoves(next, player ^ 1, settings), player ^ 1, settings, 3);
    let bestComp = -Infinity;
    let bestFlags = null;
    for (let i = 0; i < oppMoves.length; i += 1) {
      const afterOpp = applyTurnMove(next, oppMoves[i], player ^ 1);
      const myMoves = topMovesByUrgency(afterOpp, generateTurnMoves(afterOpp, player, settings), player, settings, 3);
      if (myMoves.length === 0) {
        continue;
      }
      let branchBest = -Infinity;
      let branchFlags = null;
      for (let j = 0; j < myMoves.length; j += 1) {
        const afterMy = applyTurnMove(afterOpp, myMoves[j], player);
        const swing = materialOnlyScore(afterMy, player) - materialOnlyScore(map, player);
        const myPromoNow = countImmediatePromotionMoves(afterMy, generateTurnMoves(afterMy, player, settings), player);
        const dt = detectDoubleThreatFeatures(afterOpp, myMoves[j], player, settings);
        const oppTrapped = countTrappedPieces(afterMy, player ^ 1, settings);
        const oppDead = countDeadPieces(afterMy, player ^ 1, settings);
        const comp = swing + myPromoNow * 210 + (dt.isDoubleThreat ? 160 : 0) + (dt.isTripleThreat ? 120 : 0) + oppTrapped * 30 + oppDead * 26;
        if (comp > branchBest) {
          branchBest = comp;
          branchFlags = {
            recoveredMaterial: swing >= -20,
            forcedPromotion: myPromoNow > 0,
            doubleThreat: dt.isDoubleThreat,
            structuralBreak: oppTrapped >= 2 || oppDead >= 2,
          };
        }
      }
      if (branchBest > bestComp) {
        bestComp = branchBest;
        bestFlags = branchFlags;
      }
    }

    if (!bestFlags) {
      return { isSacrifice: true, valid: false, score: immediateSwing };
    }

    const valid =
      bestFlags.recoveredMaterial
      || bestFlags.forcedPromotion
      || bestFlags.doubleThreat
      || bestFlags.structuralBreak
      || bestComp >= -40;

    return {
      isSacrifice: true,
      valid,
      score: bestComp,
      ...bestFlags,
    };
  }

  function countHangingPieces(map, player, oppMoves) {
    const threatened = threatenedSetFromMoves(oppMoves);
    let hanging = 0;
    for (let r = 0; r < 8; r += 1) {
      for (let c = 0; c < 8; c += 1) {
        const v = map[r][c];
        if (ownerOf(v) !== player) continue;
        if (threatened.has(String(r) + ":" + String(c))) {
          hanging += 1;
        }
      }
    }
    return hanging;
  }

  function materialOnlyScore(map, player) {
    const p = countPiecesByType(map);
    const myMen = player === 0 ? p.p0Men : p.p1Men;
    const opMen = player === 0 ? p.p1Men : p.p0Men;
    const myQ = player === 0 ? p.p0Queens : p.p1Queens;
    const opQ = player === 0 ? p.p1Queens : p.p0Queens;
    return (myMen - opMen) * 100 + (myQ - opQ) * 290;
  }

  function moveIsPromotion(move, player) {
    for (let i = 0; i < move.length; i += 1) {
      if (isPromotionRow(player, move[i].to[0])) {
        return true;
      }
    }
    return false;
  }

  function computeMoveUrgency(map, move, player, settings, context) {
    const ctx = context || {};
    const currentOppMoves = generateTurnMoves(map, player ^ 1, settings);
    const currentOppPromoNow = countImmediatePromotionMoves(map, currentOppMoves, player ^ 1);
    const currentOppPromoSoon = countPromotionThreats(map, player ^ 1);

    const next = applyTurnMove(map, move, player);
    const oppMoves = generateTurnMoves(next, player ^ 1, settings);
    if (oppMoves.length === 0) {
      return 1000000;
    }

    const captures = totalCaptureCount(move);
    const promotes = moveIsPromotion(move, player) ? 1 : 0;
    const nextMyMoves = generateTurnMoves(next, player, settings);
    const oppPressure = capturePressure(oppMoves);
    const myPressure = capturePressure(nextMyMoves);
    const matSwing = materialOnlyScore(next, player) - materialOnlyScore(map, player);
    const nextOppPromoNow = countImmediatePromotionMoves(next, oppMoves, player ^ 1);
    const nextOppPromoSoon = countPromotionThreats(next, player ^ 1);
    const myPromoNow = countImmediatePromotionMoves(next, nextMyMoves, player);
    const antiPromoDeltaNow = currentOppPromoNow - nextOppPromoNow;
    const antiPromoDeltaSoon = currentOppPromoSoon - nextOppPromoSoon;
    const oppBestCaptureValue = bestCaptureValueFromMoves(next, oppMoves);
    const myBestCaptureValue = bestCaptureValueFromMoves(next, nextMyMoves);
    const doubleThreat = detectDoubleThreatFeatures(map, move, player, settings);
    const forcedOutcome = detectForcedOutcomeLocal(next, player ^ 1, settings, 6);
    let forcedExtended = null;
    const shouldProbeForcedExtended =
      !ctx.quick
      && (nextOppPromoNow > 0 || myPromoNow > 0 || doubleThreat.isDoubleThreat || oppMoves.length <= 2 || nextMyMoves.length <= 2);
    if (shouldProbeForcedExtended) {
      forcedExtended = searchForcedLine(next, player ^ 1, settings, 8);
    }

    const createsPromoThreat = doubleThreat.createPromoNow || countPromotionThreats(next, player) > 0;
    const createsCaptureThreat = doubleThreat.createCaptureThreat || capturePressure(nextMyMoves) > 0;
    const blocksOppPromo = antiPromoDeltaNow > 0 || antiPromoDeltaSoon > 0 || doubleThreat.blockOppPromo;
    const forcesReply = doubleThreat.forceReply;

    let threatCount = 0;
    if (createsPromoThreat) threatCount += 1;
    if (createsCaptureThreat) threatCount += 1;
    if (blocksOppPromo) threatCount += 1;
    if (forcesReply) threatCount += 1;
    const doubleThreatBonus = doubleThreat.isTripleThreat ? 1120 : (threatCount >= 2 ? 780 : 0);

    const isSacrifice = matSwing < -60;
    const compensation = (
      myPromoNow * 260
      + antiPromoDeltaNow * 220
      + antiPromoDeltaSoon * 90
      + (myBestCaptureValue - oppBestCaptureValue) * 1.3
      + doubleThreatBonus * 0.6
    );
    const sacrificeProbe = ctx.quick
      ? { isSacrifice, valid: false, score: compensation }
      : validateSacrifice(map, move, player, settings);
    const validSacrificeBonus = sacrificeProbe.isSacrifice && (sacrificeProbe.valid || compensation > (-matSwing * 1.2)) ? 520 : 0;
    const invalidSacrificePenalty = sacrificeProbe.isSacrifice && !sacrificeProbe.valid && compensation < (-matSwing * 0.65) ? 560 : 0;

    let forcedOutcomeBonus = 0;
    if (forcedOutcome.type === "loss") forcedOutcomeBonus += 740;
    else if (forcedOutcome.type === "draw" && materialOnlyScore(next, player) < -80) forcedOutcomeBonus += 220;
    else if (forcedOutcome.type === "win") forcedOutcomeBonus -= 300;
    if (forcedExtended && forcedExtended.found) {
      if (forcedExtended.resultType === "loss") forcedOutcomeBonus += 850;
      else if (forcedExtended.resultType === "draw" && materialOnlyScore(next, player) < -50) forcedOutcomeBonus += 260;
      else if (forcedExtended.resultType === "win") forcedOutcomeBonus -= 420;
    }

    return (
      captures * 900
      + promotes * 700
      + myPromoNow * 320
      + matSwing * 6
      + myPressure * 90
      - oppPressure * 120
      - nextOppPromoNow * 1600
      - nextOppPromoSoon * 280
      - oppBestCaptureValue * 2.1
      + antiPromoDeltaNow * 920
      + antiPromoDeltaSoon * 180
      + doubleThreatBonus
      + (doubleThreat.attackHighValue ? 260 : 0)
      + validSacrificeBonus
      - invalidSacrificePenalty
      + forcedOutcomeBonus
      + (nextMyMoves.length - oppMoves.length) * 10
    );
  }

  function classifyPhase(totalPieces) {
    if (totalPieces >= 18) return "opening";
    if (totalPieces >= 9) return "middlegame";
    return "endgame";
  }

  function isNoisyMove(map, move, player, settings) {
    if (totalCaptureCount(move) > 0) return true;
    if (moveIsPromotion(move, player)) return true;
    const dt = detectDoubleThreatFeatures(map, move, player, settings);
    if (dt.isDoubleThreat) return true;
    const next = applyTurnMove(map, move, player);
    const oppMoves = generateTurnMoves(next, player ^ 1, settings);
    if (oppMoves.length <= 1) return true; // forced reply / near-forced
    if (countImmediatePromotionMoves(next, oppMoves, player ^ 1) > 0) return true;
    const myMoves = generateTurnMoves(next, player, settings);
    if (countImmediatePromotionMoves(next, myMoves, player) > 0) return true;
    const myBestCap = bestCaptureValueFromMoves(next, myMoves);
    const oppBestCap = bestCaptureValueFromMoves(next, oppMoves);
    if (myBestCap >= 240 || oppBestCap >= 240) return true;
    const forced = detectForcedOutcomeLocal(next, player ^ 1, settings, 6);
    if (forced.type !== "unclear") return true;
    return false;
  }

  function hasCriticalBlockage(map, player, settings) {
    let blocked = 0;
    for (let r = 0; r < 8; r += 1) {
      for (let c = 0; c < 8; c += 1) {
        if (ownerOf(map[r][c]) !== player) continue;
        const pm = generatePieceMoves(map, r, c, player, settings, 0);
        if ((pm.moves.length + pm.captures.length) === 0) blocked += 1;
      }
    }
    return blocked >= 2;
  }

  function isPositionUnstable(map, player, settings) {
    const myMoves = generateTurnMoves(map, player, settings);
    const oppMoves = generateTurnMoves(map, player ^ 1, settings);
    if (myMoves.length === 0 || oppMoves.length === 0) return true;
    if (hasAnyCaptureMove(myMoves) || hasAnyCaptureMove(oppMoves)) return true;
    if (countImmediatePromotionMoves(map, myMoves, player) > 0) return true;
    if (countImmediatePromotionMoves(map, oppMoves, player ^ 1) > 0) return true;
    if (countPromotionThreats(map, player) > 0 || countPromotionThreats(map, player ^ 1) > 0) return true;
    if (myMoves.length <= 1 || oppMoves.length <= 1) return true;
    if (hasCriticalBlockage(map, player, settings) || hasCriticalBlockage(map, player ^ 1, settings)) return true;
    const myBestCap = bestCaptureValueFromMoves(map, myMoves);
    const oppBestCap = bestCaptureValueFromMoves(map, oppMoves);
    if (myBestCap >= 240 || oppBestCap >= 240) return true;
    return false;
  }

  function quiescence(ctx, map, player, alpha, beta, ply) {
    if (Date.now() > ctx.deadline) {
      ctx.timedOut = true;
      return evaluate(map, ctx.rootPlayer, ctx.settings);
    }

    const standPat = evaluate(map, ctx.rootPlayer, ctx.settings);
    if (standPat >= beta) return beta;
    if (standPat > alpha) alpha = standPat;

    if (ply >= 8) return alpha;

    if (!isPositionUnstable(map, player, ctx.settings) && ply > 0) {
      return alpha;
    }

    const allMoves = generateTurnMoves(map, player, ctx.settings);
    const noisy = allMoves.filter((mv) => {
      if (isNoisyMove(map, mv, player, ctx.settings)) return true;
      const urgency = computeMoveUrgency(map, mv, player, ctx.settings, { quick: true });
      return urgency >= 820;
    });
    if (noisy.length === 0) return alpha;

    noisy.sort((a, b) => quickMoveOrdering(b, player, 0) - quickMoveOrdering(a, player, 0));
    for (let i = 0; i < noisy.length; i += 1) {
      const nextMap = applyTurnMove(map, noisy[i], player);
      const score = -quiescence(ctx, nextMap, player ^ 1, -beta, -alpha, ply + 1);
      if (score >= beta) return beta;
      if (score > alpha) alpha = score;
      if (ctx.timedOut) break;
    }
    return alpha;
  }

  function negamax(ctx, map, player, depth, alpha, beta) {
    if (Date.now() > ctx.deadline) {
      ctx.timedOut = true;
      return evaluate(map, ctx.rootPlayer, ctx.settings);
    }

    const tb = probeTablebase(map, player, ctx.settings);
    if (tb.hit) {
      return player === ctx.rootPlayer ? tb.score : -tb.score;
    }

    const key = mapKey(map, player, ctx.settings);
    const lineCount = (ctx.lineRepetition.get(key) || 0) + 1;
    ctx.lineRepetition.set(key, lineCount);
    const globalCount = (ctx.historyCounts && ctx.historyCounts[key]) ? ctx.historyCounts[key] : 0;
    if (lineCount >= 3) {
      // Repetition: treat as draw-ish, biased by match status.
      const mat = materialOnlyScore(map, ctx.rootPlayer);
      let drawScore = 0;
      if (mat < -80) drawScore = 40;
      else if (mat > 80) drawScore = -25;
      ctx.lineRepetition.set(key, lineCount - 1);
      if (lineCount - 1 <= 0) ctx.lineRepetition.delete(key);
      return drawScore;
    }
    if (globalCount >= 3) {
      const matGlobal = materialOnlyScore(map, ctx.rootPlayer);
      let drawScore = 0;
      if (matGlobal < -80) drawScore = 55;
      else if (matGlobal > 80) drawScore = -35;
      ctx.lineRepetition.set(key, lineCount - 1);
      if (lineCount - 1 <= 0) ctx.lineRepetition.delete(key);
      return drawScore;
    }
    const alpha0 = alpha;
    const tt = ctx.transposition.get(key);
    if (tt && tt.depth >= depth) {
      if (tt.flag === "EXACT") return tt.score;
      if (tt.flag === "LOWER") alpha = Math.max(alpha, tt.score);
      else if (tt.flag === "UPPER") beta = Math.min(beta, tt.score);
      if (alpha >= beta) {
        ctx.lineRepetition.set(key, lineCount - 1);
        if (lineCount - 1 <= 0) ctx.lineRepetition.delete(key);
        return tt.score;
      }
    }

    if (depth === 0) {
      const q = quiescence(ctx, map, player, alpha, beta, 0);
      ctx.lineRepetition.set(key, lineCount - 1);
      if (lineCount - 1 <= 0) ctx.lineRepetition.delete(key);
      return q;
    }

    let legalMoves = generateTurnMoves(map, player, ctx.settings);
    if (legalMoves.length === 0) {
      const mate = player === ctx.rootPlayer ? -100000 + (ctx.maxDepth - depth) : 100000 - (ctx.maxDepth - depth);
      ctx.lineRepetition.set(key, lineCount - 1);
      if (lineCount - 1 <= 0) ctx.lineRepetition.delete(key);
      return mate;
    }

    const ordered = legalMoves.map((mv) => {
      const keyMove = moveKey(mv);
      const history = getHistoryScore(ctx, keyMove);
      const killer = isKillerMove(ctx, depth, keyMove) ? 520 : 0;
      return {
        mv,
        keyMove,
        urgency: computeMoveUrgency(map, mv, player, ctx.settings, { quick: true }) + history * 0.15 + killer,
      };
    });
    ordered.sort(
      (a, b) => quickMoveOrdering(b.mv, player, b.urgency) - quickMoveOrdering(a.mv, player, a.urgency)
    );
    legalMoves = ordered.map((x) => x.mv);

    let best = -Infinity;
    let bestIdx = 0;
    for (let i = 0; i < legalMoves.length; i += 1) {
      const mv = legalMoves[i];
      const nextMap = applyTurnMove(map, mv, player);
      const mvKey = moveKey(mv);
      const noisy = isNoisyMove(map, mv, player, ctx.settings);
      const riskFlag = !noisy && i <= 2 && computeMoveUrgency(map, mv, player, ctx.settings, { quick: true }) < -450;
      const nextOppMoves = generateTurnMoves(nextMap, player ^ 1, ctx.settings);
      const forcingCritical =
        nextOppMoves.length <= 1
        || countImmediatePromotionMoves(nextMap, nextOppMoves, player ^ 1) > 0
        || hasAnyCaptureMove(nextOppMoves);

      // Selective extension on highly tactical moves.
      const extension = noisy && depth <= 6 ? 1 : 0;
      let nextDepth = Math.max(0, depth - 1 + extension);

      // Late Move Reduction: reduce depth for late quiet moves, then re-search if needed.
      const canReduce = depth >= 4 && i >= 3 && !noisy && !forcingCritical;
      let val;
      if (canReduce) {
        const reducedDepth = Math.max(0, nextDepth - 1);
        val = -negamax(ctx, nextMap, player ^ 1, reducedDepth, -beta, -alpha);
        if (!ctx.timedOut && (val > alpha || riskFlag)) {
          val = -negamax(ctx, nextMap, player ^ 1, nextDepth, -beta, -alpha);
        }
      } else {
        val = -negamax(ctx, nextMap, player ^ 1, nextDepth, -beta, -alpha);
      }
      if (val > best) {
        best = val;
        bestIdx = i;
      }
      if (val > alpha) alpha = val;
      if (alpha >= beta) {
        if (!noisy) {
          addKillerMove(ctx, depth, mvKey);
          addHistoryScore(ctx, mvKey, depth);
        }
        break;
      }
      if (ctx.timedOut) break;
    }

    if (!ctx.timedOut) {
      let flag = "EXACT";
      if (best <= alpha0) flag = "UPPER";
      else if (best >= beta) flag = "LOWER";
      ctx.transposition.set(key, {
        depth,
        score: best,
        flag,
        bestIdx,
      });
    }

    ctx.lineRepetition.set(key, lineCount - 1);
    if (lineCount - 1 <= 0) ctx.lineRepetition.delete(key);
    return best;
  }

  function createInitialBoardMap() {
    const m = Array.from({ length: 8 }, () => Array(8).fill(0));
    for (let r = 0; r < 3; r += 1) {
      for (let c = 0; c < 8; c += 1) {
        if ((r + c) % 2 === 1) m[r][c] = 1;
      }
    }
    for (let r = 5; r < 8; r += 1) {
      for (let c = 0; c < 8; c += 1) {
        if ((r + c) % 2 === 1) m[r][c] = 2;
      }
    }
    return m;
  }

  function findMoveBySpec(moves, spec) {
    for (let i = 0; i < moves.length; i += 1) {
      if (moveSpec(moves[i]) === spec) return moves[i];
    }
    return null;
  }

  function buildOpeningBookDb() {
    const db = Object.create(null);
    const cfg = Object.assign({}, DEFAULT_SETTINGS);

    function addEntry(map, player, candidates, lineId) {
      const c = canonicalPositionKey(map, player, cfg);
      const normalized = [];
      for (let i = 0; i < candidates.length; i += 1) {
        const item = candidates[i];
        const spec = c.mirrored ? mirrorMoveSpec(item.spec) : item.spec;
        normalized.push({
          spec,
          weight: item.weight | 0,
          lineId: item.lineId || lineId || OPENING_BOOK_VERSION,
        });
      }
      db[c.key] = normalized;
    }

    function applySpec(map, player, spec) {
      const moves = generateTurnMoves(map, player, cfg);
      const mv = findMoveBySpec(moves, spec);
      if (!mv) return null;
      return applyTurnMove(map, mv, player);
    }

    const start = createInitialBoardMap();
    addEntry(start, 1, [
      { spec: "52-43", weight: 64, lineId: "p1-main-a" },
      { spec: "54-43", weight: 22, lineId: "p1-main-b" },
      { spec: "54-45", weight: 14, lineId: "p1-main-c" },
    ], "p1-start");
    addEntry(start, 0, [
      { spec: "23-32", weight: 56, lineId: "p0-main-a" },
      { spec: "23-34", weight: 24, lineId: "p0-main-b" },
      { spec: "25-34", weight: 20, lineId: "p0-main-c" },
    ], "p0-start");

    const s1 = applySpec(start, 1, "52-43");
    if (s1) {
      addEntry(s1, 0, [
        { spec: "23-32", weight: 58, lineId: "p0-vs-5243-a" },
        { spec: "23-34", weight: 22, lineId: "p0-vs-5243-b" },
        { spec: "25-34", weight: 20, lineId: "p0-vs-5243-c" },
      ], "p0-vs-5243");
      const s2 = applySpec(s1, 0, "23-32");
      if (s2) {
        addEntry(s2, 1, [
          { spec: "43-34", weight: 62, lineId: "p1-line-a" },
          { spec: "54-45", weight: 22, lineId: "p1-line-b" },
          { spec: "50-41", weight: 16, lineId: "p1-line-c" },
        ], "p1-vs-2ply");
      }
    }

    const s3 = applySpec(start, 1, "54-43");
    if (s3) {
      addEntry(s3, 0, [
        { spec: "23-34", weight: 52, lineId: "p0-vs-5443-a" },
        { spec: "23-32", weight: 28, lineId: "p0-vs-5443-b" },
        { spec: "25-34", weight: 20, lineId: "p0-vs-5443-c" },
      ], "p0-vs-5443");
    }

    return db;
  }

  function getOpeningBookDb() {
    if (OPENING_BOOK_DB) return OPENING_BOOK_DB;
    OPENING_BOOK_DB = buildOpeningBookDb();
    return OPENING_BOOK_DB;
  }

  function weightedDeterministicPick(candidates, seed) {
    let total = 0;
    for (let i = 0; i < candidates.length; i += 1) {
      total += Math.max(1, candidates[i].weight | 0);
    }
    if (total <= 0) return candidates[0];
    let cursor = Math.abs(seed | 0) % total;
    for (let i = 0; i < candidates.length; i += 1) {
      cursor -= Math.max(1, candidates[i].weight | 0);
      if (cursor < 0) return candidates[i];
    }
    return candidates[candidates.length - 1];
  }

  function openingBookLookup(map, player, settings, legalMoves) {
    const pieces = countPiecesByType(map);
    if (pieces.total < 18) return { hit: false };
    if (hasAnyCaptureMove(legalMoves)) return { hit: false };
    const c = canonicalPositionKey(map, player, settings);
    const db = getOpeningBookDb();
    const entry = db[c.key];
    if (!entry || entry.length === 0) return { hit: false };

    const normalized = entry.map((x) => ({
      spec: c.mirrored ? mirrorMoveSpec(x.spec) : x.spec,
      weight: Math.max(1, x.weight | 0),
      lineId: x.lineId || OPENING_BOOK_VERSION,
    }));

    const seed = (() => {
      let h = 0;
      for (let i = 0; i < c.key.length; i += 1) {
        h = ((h * 33) ^ c.key.charCodeAt(i)) | 0;
      }
      return h;
    })();

    const picked = weightedDeterministicPick(normalized, seed);
    const directMove = findMoveBySpec(legalMoves, picked.spec);
    if (directMove) {
      return { hit: true, move: directMove, weight: picked.weight, lineId: picked.lineId };
    }

    // Fallback in case move legality differs due to variant toggles.
    for (let i = 0; i < normalized.length; i += 1) {
      const mv = findMoveBySpec(legalMoves, normalized[i].spec);
      if (mv) {
        return { hit: true, move: mv, weight: normalized[i].weight, lineId: normalized[i].lineId };
      }
    }
    return { hit: false };
  }

  function makeMoveFlags(map, move, player, settings) {
    const dt = detectDoubleThreatFeatures(map, move, player, settings);
    const sacrifice = validateSacrifice(map, move, player, settings);
    const forced = detectForcedOutcomeLocal(applyTurnMove(map, move, player), player ^ 1, settings, 6);
    const forcedDeep = searchForcedLine(applyTurnMove(map, move, player), player ^ 1, settings, 8);
    const tb = probeTablebase(applyTurnMove(map, move, player), player ^ 1, settings);
    return {
      doubleThreat: dt.isDoubleThreat,
      tripleThreat: dt.isTripleThreat,
      sacrificeValid: !!(sacrifice.isSacrifice && sacrifice.valid),
      forcing: forced.type !== "unclear",
      forcedType: forced.type,
      forcedDeep: forcedDeep.found ? forcedDeep.resultType : "unclear",
      tablebaseHit: !!tb.hit,
      tablebaseResult: tb.hit ? tb.result : "none",
      isPromotion: moveIsPromotion(move, player),
      isCapture: totalCaptureCount(move) > 0,
    };
  }

  function chooseMove(map, player, settings, options) {
    const opts = Object.assign(
      {
        maxDepth: 9,
        timeBudgetMs: 1400,
      },
      options || {}
    );

    let legalMoves = generateTurnMoves(map, player, settings);
    if (legalMoves.length === 0) return null;
    if (legalMoves.length === 1) return legalMoves[0];

    const ctx = {
      rootPlayer: player,
      settings: Object.assign({}, DEFAULT_SETTINGS, settings || {}),
      deadline: Date.now() + Math.max(80, opts.timeBudgetMs | 0),
      timedOut: false,
      maxDepth: 1,
      transposition: new Map(),
      killerMoves: new Map(),
      historyHeuristic: new Map(),
      lineRepetition: new Map(),
      historyCounts: options && options.historyCounts ? options.historyCounts : null,
    };

    const historyCounts = options && options.historyCounts ? options.historyCounts : null;
    const debugEnabled = !!(opts.debug || (typeof window !== "undefined" && window.localStorage && window.localStorage.getItem("dameBotDebug") === "1"));
    const materialLead = materialOnlyScore(map, player);

    const rootTb = probeTablebase(map, player, ctx.settings);
    if (rootTb.hit) {
      const rankedTb = legalMoves.map((mv) => {
        const next = applyTurnMove(map, mv, player);
        const nextTb = probeTablebase(next, player ^ 1, ctx.settings);
        const fallback = detectForcedOutcomeLocal(next, player ^ 1, ctx.settings, 8);
        let score = -999999;
        if (nextTb.hit) {
          score = -nextTb.score;
        } else if (fallback.type === "loss") {
          score = 88000;
        } else if (fallback.type === "draw") {
          score = 0;
        } else if (fallback.type === "win") {
          score = -88000;
        }
        return { mv, score };
      }).sort((a, b) => b.score - a.score);
      const picked = rankedTb[0] ? rankedTb[0].mv : legalMoves[0];
      if (debugEnabled) {
        LAST_DEBUG_INFO = {
          source: "tablebase",
          tablebaseHit: true,
          tablebaseResult: rootTb.result,
          chosen: moveKey(picked),
          top: rankedTb.slice(0, 3).map((x) => ({
            key: moveKey(x.mv),
            score: x.score,
            urgency: x.score,
            flags: makeMoveFlags(map, x.mv, player, ctx.settings),
          })),
        };
      }
      return picked;
    }

    const bookMove = openingBookLookup(map, player, ctx.settings, legalMoves);
    if (bookMove.hit) {
      if (debugEnabled) {
        LAST_DEBUG_INFO = {
          source: "opening-book",
          lineId: bookMove.lineId,
          chosen: moveKey(bookMove.move),
          bookWeight: bookMove.weight,
          top: [{
            key: moveKey(bookMove.move),
            score: bookMove.weight,
            urgency: bookMove.weight,
            flags: makeMoveFlags(map, bookMove.move, player, ctx.settings),
          }],
        };
      }
      return bookMove.move;
    }

    const legalWithUrgency = legalMoves.map((mv) => ({
      mv,
      urgency: (() => {
        let u = computeMoveUrgency(map, mv, player, ctx.settings);
        if (historyCounts) {
          const next = applyTurnMove(map, mv, player);
          const repKey = mapKey(next, player ^ 1, ctx.settings);
          const repCount = historyCounts[repKey] || 0;
          if (repCount >= 2) {
            // If behind, repetition can be life-saving. If ahead, avoid passive repetition.
            u += materialLead < -60 ? 260 : -180;
          }
        }
        const forced = detectForcedOutcomeLocal(applyTurnMove(map, mv, player), player ^ 1, ctx.settings, 7);
        if (forced.type === "loss") u += 620;
        else if (forced.type === "draw" && materialLead < -50) u += 180;
        else if (forced.type === "win") u -= 280;
        const nextMap = applyTurnMove(map, mv, player);
        const nextMoves = generateTurnMoves(nextMap, player ^ 1, ctx.settings);
        const criticalForcedProbe =
          nextMoves.length <= 2
          || countImmediatePromotionMoves(nextMap, nextMoves, player ^ 1) > 0
          || totalCaptureCount(mv) > 0;
        if (criticalForcedProbe) {
          const forcedDeep = searchForcedLine(nextMap, player ^ 1, ctx.settings, 9);
          if (forcedDeep.found) {
            if (forcedDeep.resultType === "loss") u += 920;
            else if (forcedDeep.resultType === "draw" && materialLead < -40) u += 260;
            else if (forcedDeep.resultType === "win") u -= 460;
          }
        }
        return u;
      })(),
    }));
    legalWithUrgency.sort(
      (a, b) => quickMoveOrdering(b.mv, player, b.urgency) - quickMoveOrdering(a.mv, player, a.urgency)
    );
    legalMoves = legalWithUrgency.map((x) => x.mv);

    // Règle 1 prioritaire: gain forcé immédiat.
    if (legalWithUrgency[0] && legalWithUrgency[0].urgency >= 900000) {
      return legalWithUrgency[0].mv;
    }

    let bestMove = legalMoves[0];
    let bestScore = -Infinity;

    for (let depth = 1; depth <= opts.maxDepth; depth += 1) {
      if (Date.now() > ctx.deadline) break;
      ctx.maxDepth = depth;
      ctx.timedOut = false;

      let depthBestMove = bestMove;
      let depthBestScore = -Infinity;
      let alpha = -Infinity;
      const beta = Infinity;

      for (let i = 0; i < legalMoves.length; i += 1) {
        if (Date.now() > ctx.deadline) {
          ctx.timedOut = true;
          break;
        }
        const mv = legalMoves[i];
        const nextMap = applyTurnMove(map, mv, player);
        let score = -negamax(ctx, nextMap, player ^ 1, depth - 1, -beta, -alpha);
        if (!ctx.timedOut && depth >= 3 && score < alpha - 120 && legalWithUrgency[i] && legalWithUrgency[i].urgency > -200) {
          // Root tactical safety re-search on suspiciously downgraded candidate.
          score = -negamax(ctx, nextMap, player ^ 1, depth, -beta, -alpha);
        }
        if (score > depthBestScore) {
          depthBestScore = score;
          depthBestMove = mv;
        }
        if (score > alpha) alpha = score;
      }

      if (!ctx.timedOut) {
        bestMove = depthBestMove;
        bestScore = depthBestScore;
      } else {
        break;
      }
    }

    if (debugEnabled) {
      const top = legalWithUrgency
        .slice(0, 3)
        .map((x) => ({
          key: moveKey(x.mv),
          score: x.urgency,
          urgency: x.urgency,
          flags: makeMoveFlags(map, x.mv, player, ctx.settings),
        }));
      LAST_DEBUG_INFO = {
        source: "search",
        tablebaseHit: false,
        chosen: moveKey(bestMove),
        bestScore,
        top,
      };
    } else {
      LAST_DEBUG_INFO = null;
    }

    return bestScore === -Infinity ? bestMove : bestMove;
  }

  root.DameBotEngine = {
    chooseMove,
    generateTurnMoves,
    applyTurnMove,
    positionKey: mapKey,
    canonicalPositionKey,
    probeTablebase,
    openingBookLookup,
    searchForcedLine,
    getLastDebugInfo: () => LAST_DEBUG_INFO,
  };
})(window);
