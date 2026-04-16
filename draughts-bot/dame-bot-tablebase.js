(function (root) {
  "use strict";

  // Format:
  // "canonicalKey>result,dtw,score|canonicalKey>result,dtw,score|..."
  // result: win|draw|loss
  // dtw: distance-to-win/loss in plies (optional, empty string allowed)
  // score: pre-bounded numeric score from side-to-move perspective.
  //
  // Kept intentionally compact for v1; runtime solver fills persistent cache.
  root.DameBotTablebaseData = {
    version: "tb6-v1",
    compressed: "",
  };
})(window);

