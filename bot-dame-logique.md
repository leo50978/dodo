# Logique Globale Du Bot De Dame (Local)

Ce document explique **exactement** comment le bot de dame réfléchit actuellement dans le site.

## 1) Contexte Technique

- Le bot est **100% local** (aucune Cloud Function).
- Fichiers principaux:
  - `draughts-bot/dame-bot-engine.js` (moteur de réflexion)
  - `draughts-bot/dame-bot-controller.js` (pilotage UI, timing, exécution)
- Le bot joue en tant que **joueur 1** (pièces `2/4` côté moteur de map interne).

## 2) Représentation Du Plateau

Le plateau est converti en matrice 8x8:

- `0` = case vide
- `1` = pion joueur 0
- `2` = pion joueur 1
- `3` = dame joueur 0
- `4` = dame joueur 1

Le contrôleur lit le DOM (`#board`) et reconstruit cette map avant chaque coup.

## 3) Génération Des Coups Légaux

Le moteur génère les coups en respectant la logique de `draughts.js`:

- déplacements simples
- captures
- captures en chaîne (rafles)
- promotion en dame
- variantes activables via settings:
  - `forceAttack`
  - `allowBackwardAttack`
  - `allowQueenRun`
  - `allowQueenAttackRun`

Fonctions clés:
- `generateTurnMoves`
- `generatePieceMoves`
- `expandCaptureChains`
- `applyTurnMove`

## 4) Pipeline De Décision À Chaque Tour

1. Générer tous les coups légaux (`generateTurnMoves`).
2. Calculer une urgence tactique par coup (`computeMoveUrgency`), incluant:
   - captures / promotions
   - anti-promotion adverse
   - valeur de rafle adverse
   - détection de double menace / triple menace
   - validation de sacrifice (compensation tactique)
3. Trier les coups par priorité (`quickMoveOrdering`) avec:
   - urgence tactique
   - heuristique historique (history heuristic)
   - killer moves par profondeur
4. Règle d’arrêt rapide:
   - si un coup est évalué comme gain forcé immédiat, il peut être joué directement.
5. Sinon recherche profonde:
   - iterative deepening
   - negamax + alpha-beta
   - transposition table
   - quiescence élargie
   - extensions sélectives
   - LMR (late move reduction)
6. Choisir le meilleur score final à la profondeur atteinte dans le budget temps.

## 5) Recherche (Cerveau)

### 5.1 Negamax + Alpha-Beta

Le moteur simule:

- mon coup
- meilleure réponse adverse
- meilleure suite

jusqu’à profondeur limite ou limite temps.

### 5.2 Iterative Deepening

Le bot cherche profondeur 1, puis 2, puis 3, etc., jusqu’au budget temps.

### 5.3 Table De Transposition

Positions déjà vues mémorisées par clé (`mapKey`) avec:

- profondeur
- score
- type de borne (`EXACT`, `LOWER`, `UPPER`)

### 5.4 Quiescence Search

Extension des feuilles instables sur:

- captures
- promotions imminentes
- réponses forcées / quasi forcées
- doubles menaces explicites
- swings tactiques élevés (grosse rafle)

But: éviter les erreurs de “horizon”.

### 5.5 Ordonnancement Avancé Des Coups

Le moteur applique actuellement:

- `killerMoves[depth]`
- `historyHeuristic`
- LMR sur coups calmes tardifs
- re-search si un coup réduit remonte au-dessus d’alpha
- extensions sélectives pour coups tactiques/noisy

### 5.6 Gestion De Répétition Dans La Recherche

Dans `negamax`, une map `lineRepetition` suit les positions de la ligne courante:

- si répétition forte (3 occurrences dans la ligne), la position est traitée comme **draw-ish**
- biais adaptatif:
  - si le bot est derrière: légère préférence pour la nulle
  - si le bot est devant: légère pénalisation de la nulle

## 6) Évaluation Positionnelle (Quand Pas De Fin Forcée)

La fonction `evaluate` calcule un score global à partir de:

- matériel (pions / dames)
- mobilité
- contrôle du centre
- avancement des pions
- pièces protégées
- pièces pendues
- menaces de promotion
- promotions immédiates
- activité des dames
- pression tactique (rafles possibles)
- pièces piégées (trapped pieces)
- contrôle des cases stratégiques (key squares)
- tempo explicite (qui force qui)

## 7) Évaluation Selon La Phase De Jeu

La phase est classée automatiquement:

- ouverture
- milieu
- finale

Les poids changent par phase:

- ouverture: centre/structure plus lourds
- milieu: tactique + structure équilibrés
- finale: mobilité, dames et promotion plus lourds

La phase est déterminée par le nombre total de pièces via `classifyPhase`.

## 8) Gestion Devant / Derrière

Le bot ajuste son style:

- s’il est devant: tendance à simplifier
- s’il est derrière: tendance à chercher plus de complexité
- en cas de répétition potentielle, ce biais est aussi appliqué (draw handling).

## 9) Anti-Promotion Et Anti-Rafle

Le bot contient des protections explicites:

- pénalise les coups qui laissent une promotion adverse immédiate
- pénalise les coups qui laissent une grosse rafle adverse
- valorise les coups qui coupent une route de promotion adverse
- valorise les coups qui forcent des réponses faibles (safe replies faibles)
- pénalise les coups qui laissent une grosse rafle de valeur

Fonctions clés:
- `countImmediatePromotionMoves`
- `countPromotionThreats`
- `bestCaptureValueFromMoves`
- `countSafeReplies`
- `detectDoubleThreatFeatures`

## 10) Exécution UI Du Coup

Le contrôleur:

- attend un délai de réflexion aléatoire (effet humain)
- joue la séquence de coups (rafle comprise)
- met à jour le statut (`Bot ap reflechi...`, etc.)

## 11) Ce Que Le Bot Fait Bien Actuellement

- bon niveau tactique court/moyen
- bonne prise en compte promotion / anti-promotion
- gestion des rafles multiples
- recherche structurée (alpha-beta + TT + quiescence)
- ordonnancement avancé (killer/history)
- extensions tactiques + LMR
- gestion de répétition dans la ligne de recherche
- détection explicite des doubles menaces

## 12) Limites Actuelles

Le bot n’est pas encore “monstre absolu” tant que ces blocs ne sont pas ajoutés:

- livre d’ouvertures
- tablebases de finale
- motifs tactiques avancés encore plus riches (sacrifices positionnels complexes, enfermement dame profond)
- gestion de nulle/répétition plus complète (au-delà des heuristiques actuelles)
- reconnaissance experte des finales théoriques gagnantes/perdantes

## 13) Résumé En Une Phrase

Le bot actuel est un moteur local fort basé sur génération légale + recherche alpha-beta + évaluation multi-facteurs phase-aware, avec protections anti-promotion/anti-rafle, mais sans encore les modules d’élite finale/ouverture/répétition.

---

## 14) Améliorations Pour Rendre Le Bot Ultra Fort

### 14.1 Faiblesses Structurelles Actuelles

1. Le moteur dépend encore trop de l’évaluation statique.
2. Les sacrifices sont sous-évalués.
3. La quiescence reste incomplète.
4. Le filtre de sécurité racine peut être trop agressif.
5. L’évaluation de structure profonde reste limitée.
6. La gestion de finale n’est pas encore spécialisée au niveau élite.
7. Pas de gestion robuste des répétitions / nulles forcées.

### 14.2 Priorités D’Impact Maximal

1. Étendre la quiescence:
- promotions imminentes (1-2 coups)
- doubles menaces
- réponses forcées
- blocages critiques
- menaces directes sur dame

2. Détection explicite des doubles menaces:
- capture + promotion
- promotion + blocage
- capture + seconde menace tactique
- pression sur deux cibles

3. Validation des sacrifices:
- simuler perte matérielle
- tester récupération tactique 2-4 coups
- tester promotion forcée
- tester rupture structurelle adverse

4. Finale renforcée (`FINAL_STRICT`):
- augmenter poids mobilité / activité dames / promotion
- réduire poids centre / structure d’ouverture
- reconnaître finales gagnantes / nulles

5. Gestion de répétition:
- historique des hash de position
- favoriser répétition défensive si derrière
- éviter répétitions inutiles si devant

6. Évaluation des pièces piégées:
- peu de sorties
- sorties tactiquement perdantes
- dépendance à une case unique

7. Contrôle des cases stratégiques:
- cases d’entrée de promotion
- cases de blocage
- diagonales longues
- pivots d’infiltration dame

### 14.3 Améliorations Moteur (Recherche)

Ordre de coups recommandé:
1. captures gagnantes
2. promotions
3. doubles menaces
4. coups forcés
5. killer moves
6. coups calmes

Modules à ajouter:
- `killerMoves[depth]`
- `historyScore[move]`
- LMR (late move reduction) pour coups tardifs non critiques
- extensions sélectives (capture majeure, promotion, réponse unique, double menace)

### 14.4 Améliorations Stratégiques

- tempo advantage (qui force qui)
- structure avancée (cohésion, diagonales, équilibre ailes, ligne arrière)
- blocage positionnel progressif

### 14.5 Modules Élite (Optionnels Mais Cruciaux)

- livre d’ouvertures local
- tablebases de finale
- reconnaissance de motifs tactiques précalculés

### 14.6 Règles De Priorité Absolue

1. Toujours vérifier les suites tactiques avant l’évaluation stratégique.
2. Une promotion forcée est prioritaire sur un gain matériel court terme.
3. Une double menace est prioritaire sur un coup positionnel passif.
4. Un sacrifice validé est prioritaire sur un coup “sûr” mais stérile.
5. En finale, l’activité peut primer le matériel brut selon la position.
6. En position inférieure, rechercher la nulle forcée si la victoire n’est pas réaliste.

### 14.7 Objectif Final

Passer d’un moteur tactique solide à un moteur hybride:
- tactique
- positionnel
- finale-aware
- draw-aware

Le bot doit pouvoir:
- anticiper
- forcer
- piéger
- simplifier ou compliquer selon le contexte
- reconnaître les positions gagnées / nulles / perdues sauvables.

---

## 15) Améliorations Ultra Bot Dame (Version Élite)

### 15.1 Objectif

Transformer le moteur actuel (déjà fort) en moteur élite capable de:
- reconnaître positions gagnantes / nulles
- jouer les finales parfaitement
- utiliser sacrifices corrects
- exploiter structure profonde
- éviter erreurs d’horizon

### 15.2 Amélioration Critique: Extension Quiescence Élite

Problème:
- quiescence actuelle trop centrée sur captures/promotions immédiates.

À ajouter:
- promotion imminente (1 ou 2 coups)
- double menace explicite
- réponse unique forcée
- blocage critique (pièce devient immobile)
- menace directe sur dame
- entrée forcée en finale simplifiée
- rupture structurelle majeure

Règle:
- si position instable, continuer quiescence.

Position instable si:
- capture possible
- promotion possible
- double menace
- seule réponse valable
- swing matériel potentiel élevé

### 15.3 Module: Validation De Sacrifice

Problème:
- sacrifices encore sous-évalués.

À ajouter:
- fonction `validateSacrifice(move)`.

Logique:
1. Simuler perte matérielle immédiate.
2. Explorer 2 à 4 coups plus loin.
3. Vérifier:
   - récupération de matériel
   - promotion forcée
   - double menace créée
   - affaiblissement structure adverse
   - enfermement pièce clé adverse
4. Si condition vraie:
   - marquer coup comme sacrifice valide
   - augmenter priorité fortement

Règle:
- un sacrifice validé > coup passif sûr.

### 15.4 Module: Détection Double Menace Avancée

Problème:
- double menace déjà détectée mais pas assez exploitée.

Types à détecter explicitement:
- capture + promotion
- promotion + blocage
- capture + seconde capture future
- attaque sur deux pièces
- menace sur deux lignes de promotion
- menace + enfermement

Règle:
- si un coup crée au moins 2 menaces indépendantes, bonus très élevé.

### 15.5 Module: Pièces Piégées (Trapped Pieces)

Problème:
- détection partielle seulement.

À ajouter:
- fonction `evaluateTrappedPieces(board)`.

Critères:
- nombre de sorties légales sous seuil
- sorties disponibles perdent du matériel
- dépendance à une seule case
- mobilité quasi nulle
- blocage par ses propres pièces

Effet:
- malus élevé pour pièce piégée
- bonus si on piège une pièce adverse

### 15.6 Module: Contrôle Des Cases Stratégiques

Problème:
- évaluation trop centrée sur les pièces, pas assez sur les cases.

À ajouter:
- cases d’entrée de promotion
- cases de blocage
- diagonales longues
- pivots de dame
- zones d’infiltration
- cases qui coupent 2 trajectoires

Règle:
- une case stratégique contrôlée > simple présence de pièce.

### 15.7 Module: Mode Finale Strict (`FINAL_STRICT`)

Problème:
- finale pas encore spécialisée élite.

À ajouter:
- si phase finale, activer `FINAL_STRICT`.

Modifications:
- augmenter poids:
  - mobilité
  - activité des dames
  - promotion
  - tempo
- réduire poids:
  - centre
  - structure ouverture
- ajouter:
  - distance à promotion
  - contrôle des cases d’entrée
  - opposition de dames
  - possibilité d’échange gagnant

Objectif:
- jouer les finales comme un solveur.

### 15.8 Module: Gestion Avancée Des Répétitions

Problème:
- gestion actuelle limitée à la ligne.

À ajouter:
- historique global des positions (hash).

Règles:
- si position répétée au moins 3 fois, considérer comme nulle.
- si bot derrière, augmenter valeur répétition.
- si bot devant, pénaliser répétition inutile.
- si répétition évite perte, prioriser fortement.

### 15.9 Module: Détection Positions Forcées

Problème:
- le bot ne reconnaît pas toujours les lignes forcées longues.

À ajouter:
- gain forcé (même si loin)
- nulle forcée
- perte inévitable

Règle:
- victoire forcée: priorité maximale
- nulle forcée: priorité si position mauvaise
- perte forcée: éviter si alternative existe

### 15.10 Amélioration: Filtre Racine Intelligent

Problème:
- filtre racine peut supprimer de bons coups.

Correction:
- ne pas supprimer un coup uniquement car il donne promotion ou capture adverse.
- faire une mini recherche tactique courte.
- si compensation détectée, conserver le coup.

### 15.11 Amélioration: Évaluation Structure Profonde

Ajouter:
- cohésion de groupe
- équilibre gauche/droite
- ligne arrière
- chaînes de pions
- pièces mortes (présentes mais inutiles)
- verrouillage positionnel

### 15.12 Amélioration: Tempo Et Initiative

Ajouter:
- qui force qui
- nombre de réponses forcées adverses
- pression continue
- séquences où l’adversaire subit

Règle:
- initiative > matériel temporaire (dans les lignes critiques).

### 15.13 Module: Priorité Absolue (Override Logic)

Ordre de décision absolu:
1. gain forcé
2. promotion forcée
3. double menace
4. sacrifice validé
5. défense obligatoire
6. amélioration stratégique

### 15.14 Modules Élite (Optionnels Mais Recommandés)

- livre d’ouvertures (positions safe)
- tablebases de finale
- motifs tactiques précalculés
- killer moves améliorés
- history heuristic renforcé

### 15.15 Règles Finales Du Bot Élite

Toujours:
- vérifier tactique avant stratégie
- préférer promotion à gain matériel court
- exploiter doubles menaces
- accepter sacrifices validés
- simplifier si devant
- compliquer si derrière
- forcer nulle si nécessaire
- reconnaître positions gagnées / perdues / nulles

### 15.16 Résultat Attendu

Après ces améliorations, le bot devient:
- beaucoup plus dangereux tactiquement
- plus stable en finale
- capable de sacrifices intelligents
- difficile à piéger
- capable de sauver des positions perdues
- proche d’un niveau élite réel

---

## 16) Statut Implémentation Actuelle (Mise À Jour Réelle)

Les éléments ci-dessous sont maintenant **effectivement branchés dans le code** (`draughts-bot/dame-bot-engine.js`):

- `validateSacrifice(map, move, player, settings)` ajouté et utilisé dans `computeMoveUrgency`.
- Quiescence élargie:
  - `isPositionUnstable` (captures, promotions, réponses forcées, blocages, swings tactiques)
  - extension quiescence jusqu’aux positions stables.
- Détection forcing locale:
  - `detectForcedOutcomeLocal` utilisé pour prioriser victoire/nulle défensive/évitement perte.
- Renfort structure profonde:
  - `countBackRankGuard`
  - `countPawnChains`
  - `evaluateWingBalance`
  - `countDeadPieces`
- Mode finale renforcé dans `evaluate`:
  - termes promo-distance, opposition des dames, intention d’échange.
- Répétition:
  - répétition ligne (`lineRepetition`) + prise en compte historique global racine (`historyCounts`) dans la recherche.
- Root safety:
  - mini re-search tactique pour candidats racine à risque (au lieu d’exclusion brutale).
- Opening book local minimal:
  - sélection d’ouverture guidée (préférences centrales/développement) avec fallback moteur.
- Debug explicatif:
  - `getLastDebugInfo()`
  - top coups + flags (double menace, sacrifice validé, forcing, etc.)
  - activable côté page avec `?botDebug=1`.

---

## 17) Logique Exacte Actuelle (Niveau Code)

Cette section décrit la logique **exactement comme elle est implémentée** dans `draughts-bot/dame-bot-engine.js` et `draughts-bot/dame-bot-controller.js`.

### 17.1 Paramètres par défaut moteur

- `DEFAULT_SETTINGS`:
  - `forceAttack: true`
  - `allowBackwardAttack: false`
  - `allowQueenRun: true`
  - `allowQueenAttackRun: false`

- `chooseMove` (si options absentes):
  - `maxDepth: 9`
  - `timeBudgetMs: 1400`

- Contrôleur (`dame-bot-controller.js`):
  - bot = joueur `1`
  - réflexion visuelle avant calcul: `820..1650 ms`
  - délai entre steps d’une séquence: `230..360 ms`
  - appel moteur actuel: `maxDepth: 11`, `timeBudgetMs: 1800`

### 17.2 Représentation et application de coup

- Cases:
  - `0`: vide
  - `1`: pion joueur 0
  - `2`: pion joueur 1
  - `3`: dame joueur 0
  - `4`: dame joueur 1

- Un “coup” est une **séquence** de `step`:
  - `from: [r,c]`
  - `to: [r,c]`
  - `captures: [[r,c], ...]`

- `applyTurnMove`:
  - applique chaque step sans promotion intermédiaire (`promoteNow=false`)
  - applique promotion seulement à la fin de la séquence (si rangée de promo atteinte)

### 17.3 Génération légale

- `generateTurnMoves`:
  - génère captures + coups calmes pour toutes les pièces du joueur
  - construit les rafles complètes via `expandCaptureChains`
  - si `forceAttack=true` et au moins une capture existe, retourne uniquement captures

- Dames:
  - `allowQueenRun` active mouvement prolongé
  - `allowQueenAttackRun` active continuation après attaque en glisse

- Pions:
  - `allowBackwardAttack` n’est activé qu’en attaque et après `attackTurn > 0`

### 17.4 Évaluation exacte (`evaluate`)

#### 17.4.1 Scores de base

- terminal:
  - si le joueur évalué n’a aucun coup: `-100000`
  - si l’adversaire n’a aucun coup: `+100000`

- `materialDelta`:
  - pions: `100`
  - dames: `290`

#### 17.4.2 Features calculées

- mobilité: `ownMoves.length - oppMoves.length`
- centre: `countCenterControl`
- avancement pions: `countAdvancedPawns`
- protection: `countProtectedPieces`
- pièces pendues: `countHangingPieces`
- menaces promo:
  - `countPromotionThreats`
  - `countImmediatePromotionMoves`
- activité dame: `countQueenActivity`
- pression capture:
  - `capturePressure`
  - `bestCaptureValueFromMoves`
- structure:
  - `countTrappedPieces`
  - `evaluateKeySquares`
  - `countBackRankGuard`
  - `countPawnChains`
  - `evaluateWingBalance`
  - `countDeadPieces`
- tempo:
  - bonus si adversaire a ≤2 réponses
  - bonus si capture possible
  - bonus si promo immédiate
  - bonus si activité dames supérieure

#### 17.4.3 Poids par phase (`classifyPhase`)

- ouverture (`totalPieces >= 18`):
  - `{ men:100, queens:270, mobility:10, center:20, advanced:16, prot:28, hang:30, promo:26, promoNow:230, pressure:14, threat:10, qAct:6, bestCap:0.45 }`
- milieu (`9..17`):
  - `{ men:100, queens:290, mobility:13, center:15, advanced:22, prot:25, hang:31, promo:40, promoNow:300, pressure:20, threat:13, qAct:9, bestCap:0.55 }`
- finale (`<=8`):
  - `{ men:92, queens:330, mobility:22, center:8, advanced:28, prot:20, hang:26, promo:62, promoNow:430, pressure:24, threat:16, qAct:16, bestCap:0.62 }`

#### 17.4.4 Garde-fous anti-blunder

- `score -= opPromoNow * 1400`
- `score -= max(0, oppBestCaptureValue - 180) * 1.35`

#### 17.4.5 Style ahead/behind

- si devant (`materialDelta > 120`):
  - bonus simplification: `+(26 - totalPieces)*10`
  - malus complexité: `-complexity*1.5`
- si derrière (`materialDelta < -120`):
  - malus simplification: `-(26 - totalPieces)*6`
  - bonus complexité: `+complexity*2.2`

#### 17.4.6 FINAL_STRICT actif en finale

- poids finalStrict:
  - `{ backRank:12, chains:10, wing:8, dead:20, promoDistance:28, queenOpposition:24, exchange:18 }`

- termes ajoutés:
  - `promotionDistanceScore` (distance vers promo + bonus couloir central)
  - `queenOppositionScore` (proximité utile des dames)
  - `evaluateExchangeIntent` (échanges forcés selon avance/retard)

- tie-break final:
  - `score += materialScore(map, player) * 0.22`

### 17.5 Urgence coup (`computeMoveUrgency`) exact

- victoire immédiate (adversaire sans coup après le move): `1000000`

- composantes fortes:
  - captures: `+900 * captures`
  - promotion du move: `+700`
  - promo immédiate créée: `+320 * myPromoNow`
  - swing matériel: `+6 * matSwing`
  - pression:
    - `+90 * myPressure`
    - `-120 * oppPressure`
  - anti-promo:
    - `-1600 * nextOppPromoNow`
    - `-280 * nextOppPromoSoon`
    - `+920 * antiPromoDeltaNow`
    - `+180 * antiPromoDeltaSoon`
  - rafle adverse:
    - `-2.1 * oppBestCaptureValue`
  - mobilité relative:
    - `+(nextMyMoves.length - oppMoves.length) * 10`

- double menace:
  - bonus `1120` si triple menace
  - sinon `780` si au moins double menace
  - `+260` si `attackHighValue=true`

- sacrifice:
  - sacrifice détecté si `matSwing < -60`
  - bonus validé: `+520`
  - malus sacrifice non valide: `-560`
  - en mode quick (`context.quick=true`), pas de validation complète (estimation rapide)

- forcing local:
  - si `detectForcedOutcomeLocal` retourne `loss`: `+740`
  - si `draw` et position matérielle mauvaise: `+220`
  - si `win`: `-300` (on évite de laisser ligne gagnante adverse)

### 17.6 Détection double menace (`detectDoubleThreatFeatures`)

- features:
  - `createPromoNow`
  - `createCaptureThreat` (>= 120 de capture value)
  - `blockOppPromo`
  - `forceReply` (`oppMoves<=1` ou `safeReplies<=1`)
  - `attackHighValue` (>= 300)

- agrégation:
  - `isDoubleThreat = count >= 2`
  - `isTripleThreat = count >= 3`

### 17.7 Validation sacrifice (`validateSacrifice`)

- active seulement si perte immédiate significative (`immediateSwing < -60`)
- mini exploration locale:
  - top 3 réponses adverses (par urgence)
  - top 3 suites bot (par urgence)
- compensation:
  - `swing`
  - `+ myPromoNow * 210`
  - `+160` double menace
  - `+120` triple menace
  - `+30 * oppTrapped`
  - `+26 * oppDead`

- flags:
  - `recoveredMaterial` (`swing >= -20`)
  - `forcedPromotion`
  - `doubleThreat`
  - `structuralBreak` (`oppTrapped>=2 || oppDead>=2`)

- valid si un flag majeur vrai ou `bestComp >= -40`

### 17.8 Bruit tactique / instabilité

- `isNoisyMove` = vrai si:
  - capture
  - promotion
  - double menace
  - réponse adverse forcée (`<=1`)
  - promo immédiate pour un camp
  - rafle forte (>=240)
  - forcing local non “unclear”

- `isPositionUnstable` = vrai si:
  - absence de coups d’un camp
  - capture dispo
  - promo immédiate/menace promo
  - réponses très limitées
  - blocage critique (`>=2` pièces sans move)
  - grosse rafle possible (>=240)

### 17.9 Quiescence exacte

- timeout -> retourne `evaluate` et `ctx.timedOut=true`
- `standPat` alpha/beta standard
- profondeur max quiescence: `ply >= 8`
- arrêt si position stable et `ply>0`
- candidats = `isNoisyMove` OU `urgency>=820`

### 17.10 Negamax/Alpha-Beta exact

- répétition ligne:
  - 3 occurrences => draw-ish:
    - si derrière (`mat < -80`): `+40`
    - si devant (`mat > 80`): `-25`
- répétition globale (`historyCounts`):
  - si count >=3:
    - derrière: `+55`
    - devant: `-35`

- TT:
  - flag `EXACT/LOWER/UPPER`
  - cutoffs standard

- ordre coups:
  - `computeMoveUrgency(quick)`
  - `history * 0.15`
  - killer bonus `+520`
  - tri avec `quickMoveOrdering`

- extensions/réductions:
  - extension `+1` si coup noisy et `depth<=6`
  - LMR: si `depth>=4`, coup tardif (`i>=3`) et calme
  - re-search après LMR si amélioration alpha ou `riskFlag` déclenché
  - `riskFlag`: top3 calme avec urgence < `-450`

- mise à jour killer/history:
  - seulement sur cutoff et coup non noisy

### 17.11 Opening book local

- activé seulement si:
  - `totalPieces >= 20`
  - aucune capture forcée dans coups légaux

- scoring ouverture:
  - case centrale préférée: `+180`
  - case développement: `+90`
  - réduit menaces promo adverse: `+40`
  - garde mobilité (>=8 coups): `+35`
  - capture en ouverture: `-80`

- utilisé si meilleur score >= `120`

### 17.12 Pipeline réel `chooseMove`

1. Générer coups légaux.
2. Si 0 coup -> `null`, si 1 coup -> retour direct.
3. Initialiser contexte recherche (`deadline`, TT, killers, history, repetition, historyCounts).
4. Tenter opening book.
5. Calculer urgence de chaque coup (inclut répétition historique + forcing local).
6. Trier coups.
7. Si meilleur urgence >= `900000`, jouer immédiatement.
8. Iterative deepening de profondeur 1 à `maxDepth`.
9. Dans chaque profondeur, évaluer tous les coups triés via `negamax`.
10. Root tactical re-search si score chute suspecte (`depth>=3`, score<alpha-120, urgence>-200`).
11. Conserver meilleur coup complet avant timeout.
12. Debug optionnel: stocker top 3 + flags dans `LAST_DEBUG_INFO`.

### 17.13 Flags debug exposés

- `doubleThreat`
- `tripleThreat`
- `sacrificeValid`
- `forcing`
- `forcedType` (`win/draw/loss/unclear`)
- `isPromotion`
- `isCapture`

- API:
  - `DameBotEngine.getLastDebugInfo()`

### 17.14 Contrôleur exact

- lit l’état réel du DOM pour reconstruire map 8x8.
- track historique de positions bot via `positionKey`.
- lance coup bot uniquement quand:
  - board prêt
  - partie non finie
  - tour bot actif
- statut UI:
  - `Bot ap reflechi...`
  - `Bot ap jwe kou li...`
  - `A ou jwe.`
- debug console actif si URL contient `?botDebug=1`.

---

## 18) État v1 Élite (Étapes 1–3) — Implémenté

Les 3 briques du plan v1 sont maintenant branchées:

1. Tablebase locale 6 pièces
- module: `probeTablebase(map, player, settings)`
- activation uniquement si `totalPieces <= 6`
- priorité de décision au niveau racine (`chooseMove`) et dans `negamax`
- cache local versionné:
  - version: `tb6-v1`
  - clé storage: `dame_tablebase_cache_tb6-v1`
  - seed statique: `draughts-bot/dame-bot-tablebase.js`
  - cache mémoire + persistance locale

2. Opening book réel (dictionnaire canonisé)
- clé canonique: `canonicalPositionKey(map, player, settings)` (normal/mirror)
- lookup: `openingBookLookup(map, player, settings, legalMoves)`
- plusieurs coups pondérés par position
- politique: déterministe avec légère variété pondérée
- garde-fou: book désactivé si capture forcée
- fallback moteur automatique hors couverture

3. Forced-line search plus profond
- module: `searchForcedLine(map, player, settings, depthLimit)`
- utilisé dans:
  - `computeMoveUrgency` (signal forcing étendu ciblé)
  - ordering racine (bonus/malus selon `win/draw/loss`)
- LMR protégé: pas de réduction agressive sur positions critiques forcing

API internes exposées par `DameBotEngine`:
- `canonicalPositionKey`
- `probeTablebase`
- `openingBookLookup`
- `searchForcedLine`

Debug enrichi:
- `getLastDebugInfo()` inclut source (`tablebase|opening-book|search`) et drapeaux forcing/tablebase.
