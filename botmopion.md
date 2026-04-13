# Morpion Bot Test - Guide vivant

Ce fichier est un document de reference que l'on relit a chaque modification pour verifier la logique, noter ce qui a change, et eviter les regressions. Il doit etre mis a jour a chaque evolution du test bot.

## Objectif du test bot

Permettre de lancer instantanement une partie de morpion contre un bot en cliquant sur "0 does", sans mise, pour tester rapidement le comportement du bot et l'etat du flow.

## Fonctionnement general (vue d'ensemble)

1. Le bouton "0 does" envoie vers `morpion.html` avec `roomMode=morpion_bot_test`.
2. `logiquejeu-morpion.js` detecte ce mode et lance le flow "bot test".
3. Le client appelle la function `createMorpionBotTestRoom` pour creer une salle speciale.
4. Le serveur cree la salle, demarre la partie, et declenche le bot.
5. Le client s'abonne a la room, gere la presence, et affiche le plateau.

## Cote client (logiquejeu-morpion.js)

### Detection du mode
- Le flow bot test est active si l'URL contient `roomMode=morpion_bot_test` ou `botTestMorpionRoomId`.
- `joinOrResumeCurrentFlow()` choisit entre:
  - `startMorpionBotTestFromUrl()` si pas de room id
  - `resumeMorpionBotTestFromUrl()` si room id present

### Creation et reprise de salle
- `startMorpionBotTestFromUrl()`:
  - appelle `createMorpionBotTestRoomSecure({})`
  - demarre les subscriptions room + presence
  - en cas de salle active:
    - si salle bot test, redirige vers la room
    - sinon quitte la salle et retente une creation
- `resumeMorpionBotTestFromUrl()`:
  - appelle `resumeMorpionBotTestRoomSecure({ roomId })`
  - demarre les subscriptions room + presence

### URL de test bot
- `buildMorpionBotTestGameUrl(roomId, seatIndex)` construit l'URL:
  - `roomMode=morpion_bot_test`
  - optionnel: `botTestMorpionRoomId` et `seat`

### Presence
- `touchClientSitePresence()` ecrit dans `clients/{uid}`:
  - `sitePresencePage`, `sitePresenceExpiresAtMs`, `morpionLastInterestAtMs`
- Si les regles Firestore refusent, on log `sitePresenceUnavailable`.

## Cote serveur (functions/index.js)

### createMorpionBotTestRoom
- Verifie l'auth.
- Refuse si user deja dans une room active (erreur `active-room-exists`).
- Cree une room `morpion_bot_test` avec:
  - `allowBots: true`, `requiredHumans: 1`, `botCount: 1`
  - `stakeDoes: 0`, `rewardAmountDoes: 0`
  - `startRevealPending: false` pour autoriser un bot immediat
- Lance `processPendingBotTurnsMorpion()` si la partie est deja en "playing".

### resumeMorpionBotTestRoom
- Verifie que la room existe et est bien un bot test.
- Force `startRevealPending` a `false` si necessaire.
- Lance `processPendingBotTurnsMorpion()` si la partie est "playing".

### App Check
- Le test bot doit fonctionner sans App Check.
- Bypass App Check pour:
  - `createMorpionBotTestRoom`
  - `resumeMorpionBotTestRoom`
  - `touchRoomPresenceMorpion` (si room bot test)
  - `submitActionMorpion` (si room bot test)
  - `ackRoomStartSeenMorpion` (si room bot test)

## Regles Firestore (firestore.rules)

La presence client ecrit dans `clients/{uid}` doit autoriser:
- `morpionLastInterestAtMs`
Sans cette cle, la presence echoue et log `Missing or insufficient permissions`.

## Checklist avant de tester

- L'URL contient `roomMode=morpion_bot_test`.
- Le client est bien authentifie.
- `createMorpionBotTestRoom` ne retourne pas `active-room-exists`.
- `startRevealPending` est `false` apres creation.
- Si le bot ne rejoue pas, verifier que `turnLockedUntilMs` n'est pas en train de bloquer un bot test.
- Si `turnLockedUntilMs` est non-zero en bot test, verifier que `isMorpionBotTestRoom` detecte aussi `stakeConfigId=morpion_bot_test_0`.
- Les regles Firestore acceptent `morpionLastInterestAtMs`.
- Si un correctif client vient d'etre deploye, verifier que le cache Service Worker a ete invalide (changer `CACHE_VERSION` dans `sw.js`).

## Journal des modifications

### 2026-04-10
- Ajout du flow `morpion_bot_test` cote client:
  - `startMorpionBotTestFromUrl()` et `resumeMorpionBotTestFromUrl()`
  - `buildMorpionBotTestGameUrl()` dans `logiquejeu-morpion.js`
- Correction du blocage bot:
  - `startRevealPending` force a `false`
  - relance bot via `processPendingBotTurnsMorpion`
- Ajout du bypass App Check pour le test bot
- Mise a jour des regles Firestore pour `morpionLastInterestAtMs`
- App Check: bypass conditionnel pour `touchRoomPresenceMorpion`, `submitActionMorpion`, `ackRoomStartSeenMorpion` si la room est un bot test
- Bot test: suppression du verrou de tour (`turnLockedUntilMs`) pour les bots afin d'eviter un stall apres le premier coup
- Invalidation du cache Service Worker (`CACHE_VERSION`) pour forcer le rechargement des scripts du morpion
- Bot test: le joueur humain commence toujours (currentPlayer force a 0 lors de l'initialisation)
- Bot test: detection durcie via `stakeConfigId=morpion_bot_test_0` si `roomMode` n'est pas present
- Salles privees Morpion: durcissement du flow `Rejouer`
  - fermeture forcee des modales de fin et d'attente quand une nouvelle manche demarre
  - ajout d'une resynchronisation client de la room pendant l'attente de revanche
  - objectif: eviter qu'un premier joueur reste bloque sur la modal pendant que l'autre est deja reparti

### 2026-04-12
- Morpion humain: ajout d'une protection "pas de perte sans premier coup"
  - si un joueur humain laisse son chrono tomber a zero sans avoir place le moindre symbole
  - la fin de partie ne passe plus par `timeout`
  - la partie se termine avec `endedReason=no_play_refund`
  - aucun gagnant n'est designe
  - les deux joueurs sont rembourses automatiquement cote serveur a partir de `entryFundingByUid`
- Cette regle s'applique:
  - au matchmaking Morpion entre humains
  - aux salles privees Morpion / invitation entre amis
- Cette regle ne s'applique pas:
  - au flow `morpion_bot_test`
  - au bot, qui reste desactive dans le projet actuel

## A faire quand on modifie ce flow

1. Decrire clairement la modification ici, dans "Journal des modifications".
2. Verifier que la checklist passe.
3. Relire les etapes de bout en bout pour s'assurer que la logique reste coherente.
