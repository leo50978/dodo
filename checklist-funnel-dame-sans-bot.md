# Checklist Funnel Morpion -> Dame (Sans Bot)

Objectif: debloquer le jeu de Dame et lui donner **le meme funnel que Morpion**, en **100% humain vs humain** (aucun bot).

---

## 0) Etat actuel releve

- Blocage "Jeu en developpement" trouve dans `page2.js`:
  - carte Dame cliquee -> `openComingSoonModal("Jeu de dame")` (autour de `page2.js:4409`).
- `dame.html` actuel est en mode local vs bot:
  - badge "Jeu de dame vs bot local"
  - scripts bot charges:
    - `/draughts-bot/dame-bot-tablebase.js`
    - `/draughts-bot/dame-bot-engine.js`
    - `/draughts-bot/dame-bot-controller.js`

Conclusion: pour faire Dame comme Morpion, il faut:
1. enlever la barriere coming-soon pour Dame,
2. remplacer le mode local-bot par un vrai flux multijoueur room-based.

---

## 1) Funnel Morpion complet (reference a copier)

## 1.1 Entree depuis home (`page2.js`)

- UI stake Morpion: `morpionStakeOverlay`.
- Bouton "Jouer avec un ami": `morpionFriendModeOpenBtn`.
- Modales friend Morpion:
  - `morpionFriendModeOverlay` (choix: invite / creer)
  - `morpionFriendCreateOverlay`
  - `morpionFriendJoinOverlay`
  - `morpionFriendCodeOverlay`
- URL generees:
  - public: `./morpion.html?stake=...`
  - friend: `./morpion.html?...&friendMorpionRoomId=...&roomMode=morpion_friends`

## 1.2 In-game runtime (`logiquejeu-morpion.js`)

- Au chargement:
  - lit URL params (`stake`, `friendMorpionRoomId`, `roomMode`).
  - choisit le flow avec `joinOrResumeCurrentFlow()`:
    - public -> `joinMatchmakingMorpionSecure`
    - friend -> `resumeFriendMorpionRoomSecure`
- En attente (public):
  - timer 15s (`MATCHMAKING_WAIT_MS`)
  - modal attente avec actions:
    - retry
    - rester en attente
    - quitter attente prolongee
    - notifications
    - groupe WhatsApp / contacts
- En attente (friend):
  - message "En attente de ton ami..."
  - pas de timeout dur comme public.
- Demarrage de partie:
  - `ensureRoomReadyMorpionSecure` appele tant que room `waiting`.
  - la partie passe `playing` quand 2 humains sont presents et financements OK.
- Pendant partie:
  - snapshots Firestore room + gameState.
  - actions envoyees via `submitActionMorpionSecure`.
- Fin de partie:
  - resultat + claim gain via `claimWinRewardMorpionSecure`.
  - replay / revanche friend.

## 1.3 Backend Morpion (`functions/index.js`)

- Fonctions cle:
  - `joinMatchmakingMorpion`
  - `createFriendMorpionRoom`
  - `joinFriendMorpionRoomByCode`
  - `resumeFriendMorpionRoom`
  - `ensureRoomReadyMorpion`
  - `touchRoomPresenceMorpion`
  - `leaveRoomMorpion`
  - `submitActionMorpion`
  - `claimWinRewardMorpion`
  - `requestFriendMorpionRematch`
- Proprietes room importantes:
  - `status: waiting|playing|ended|closed`
  - `roomMode: morpion_2p | morpion_friends`
  - `playerUids`, `playerNames`, `seats`
  - `humanCount`, `botCount`
  - `waitingDeadlineMs`
  - `entryFundingByUid`, `stakeDoes`, `rewardAmountDoes`

## 1.4 Modales sociales et engagement (Morpion)

- Le funnel Morpion inclut aussi:
  - activation notifications navigateur (`morpionWaitingNotifyBtn`)
  - bouton groupe WhatsApp (`morpionWaitingGroupBtn`)
  - modal "laisser mon WhatsApp" (`morpionWhatsappModal`)
  - modal "voir les joueurs actifs" (`morpionContactsModal`)
- Fonctions backend liees:
  - `getMyMorpionWhatsappPreferenceSecure`
  - `saveMorpionWhatsappPreferenceSecure`
  - `removeMorpionWhatsappPreferenceSecure`
  - `listRecentMorpionWhatsappContactsSecure`
  - + invitations live: `getMyActiveMorpionInvite`, `respondMorpionPlayInvite`

---

## 1.5 Verif de parite "exactement comme Morpion"

La cible Dame est validee seulement si toutes ces cases sont vraies:

- [ ] Meme sequence d’entree home -> stake -> matchmaking/friend.
- [ ] Meme logique public: 15s, puis retry / attente prolongee / home.
- [ ] Meme logique friend: attente de l’ami sans auto-expiration brutale UX.
- [ ] Meme robustesse room state (waiting/playing/ended/closed).
- [ ] Meme niveau de resilience: presence ping, ensure ready, leave safe.
- [ ] Meme qualite de fin de partie: resultat + payout + replay/rejoin.
- [ ] Meme pack social: notifications + groupe WhatsApp + contact joueur.
- [ ] Aucun appel bot cote client/back.

---

## 2) Cible Dame (copie Morpion, sans bot)

## 2.1 Regles produit

- Dame doit etre:
  - 2 joueurs humains uniquement.
  - modes:
    - public matchmaking
    - salle privee avec code (entre amis)
- aucun bot:
  - ni bot local JS
  - ni bot backend
  - `allowBots=false` partout.

## 2.2 UX cible

- Meme UX que Morpion:
  - meme sequence d’overlays depuis page2
  - meme patterns de boutons/feedback
  - meme logique d’attente 15s en public
  - meme logique "en attente de ton ami" en friend.
- Meme style des modales:
  - overlays full-screen, carte centrale, CTA identiques.
  - textes adaptes a "Dame" mais structure identique.

## 2.3 Contrat technique "jeu separe"

- Dame doit etre un jeu autonome:
  - [ ] propres collections Firestore
  - [ ] propres fonctions cloud
  - [ ] propre runtime client (`logiquejeu-dame.js` recommande)
  - [ ] propre page de jeu (`dame.html`) sans dependance bot.
- Interdit:
  - [ ] reutiliser les fonctions Morpion en mode "hack"
  - [ ] melanger documents Morpion et Dame dans les memes collections.

---

## 3) Plan implementation ultra-safe (ordre conseille)

## 3.1 Phase A - Deblocage minimal et non destructif

- [x] Remplacer le click Dame (home) pour ouvrir un funnel Dame au lieu de `openComingSoonModal`.
- [x] Garder un feature flag (ex: `ENABLE_DAME_MULTIPLAYER`) pour rollback rapide.
- [x] Ne pas toucher Morpion existant.

## 3.2 Phase B - UI page2 pour Dame (copie Morpion)

- [ ] Creer overlays Dame dedies (ids prefix `dame...`) en copiant la structure Morpion:
  - [ ] `dameStakeOverlay`
  - [ ] `dameFriendModeOverlay`
  - [ ] `dameFriendCreateOverlay`
  - [ ] `dameFriendJoinOverlay`
  - [ ] `dameFriendCodeOverlay`
- [ ] Ajouter builders URL Dame:
  - [ ] public `dame.html?stake=...`
  - [ ] friend `dame.html?...&friendDameRoomId=...&roomMode=dame_friends`
- [ ] Valider UX active-room-exists (resume room existante).

## 3.2.b Phase B-social - pack engagement a copier

- [ ] Ajouter dans le waiting Dame:
  - [ ] bouton notifications
  - [ ] bouton groupe WhatsApp
  - [ ] bouton laisser numero WhatsApp
  - [ ] bouton voir joueurs actifs
- [ ] Prevoir modales Dame:
  - [ ] `dameWhatsappModal`
  - [ ] `dameContactsModal`
  - [ ] `dameInviteModal` (si on garde la meme logique d’invite proactive)

## 3.3 Phase C - Secure functions client

- [x] Ajouter wrappers dans `secure-functions.js`:
  - [x] `joinMatchmakingDameSecure`
  - [x] `createFriendDameRoomSecure`
  - [x] `joinFriendDameRoomByCodeSecure`
  - [x] `resumeFriendDameRoomSecure`
  - [x] `ensureRoomReadyDameSecure`
  - [x] `touchRoomPresenceDameSecure`
  - [x] `leaveRoomDameSecure`
  - [ ] `submitActionDameSecure`
  - [ ] `claimWinRewardDameSecure`
  - [ ] `requestFriendDameRematchSecure` (si replay like Morpion)
  - [ ] `getMyDameWhatsappPreferenceSecure`
  - [ ] `saveDameWhatsappPreferenceSecure`
  - [ ] `removeDameWhatsappPreferenceSecure`
  - [ ] `listRecentDameWhatsappContactsSecure`
  - [ ] `getMyActiveDameInviteSecure` / `respondDamePlayInviteSecure` (optionnel mais recommande pour parite totale)

## 3.4 Phase D - Backend Dame

- [x] Creer collections Dame (ex: `dameRooms`, `dameGameStates`, `dameRoomResults`).
- [x] Implementer callables Dame paralleles a Morpion:
  - [x] matchmaking public 2p humain uniquement
  - [x] friend room code
  - [x] resume/join/leave/presence
  - [x] ensureReady + start game
  - [ ] submit action + validation
  - [ ] fin de partie + payout
- [ ] Verrouiller `allowBots=false` et `botCount=0`.
- [ ] Copier patterns anti-race condition (transactions, actionSeq, idempotence).
- [ ] Ajouter analytics Dame dedie:
  - [ ] `getDameAnalyticsSnapshot`
  - [ ] `computeDameAnalyticsSnapshot`
  - [ ] filtres composition/winner/stake/range (meme shape que Morpion pour dashboard).

## 3.5 Phase E - Runtime `dame.html`

- [ ] Creer runtime multiplayer Dame (nouveau script dedie recommandé).
- [ ] Retirer les scripts bot locaux de `dame.html`.
- [ ] Ajouter logique URL flow:
  - [ ] friend room resume si `friendDameRoomId`
  - [ ] sinon matchmaking public.
- [ ] Ajouter modal attente copie Morpion:
  - [ ] timer 15s public
  - [ ] retry/extend/home
  - [ ] attente friend sans timeout agressif.
- [ ] Ajouter loop presence + ensureReady.
- [ ] Integrer social pack:
  - [ ] notifications
  - [ ] modal WhatsApp numero
  - [ ] modal contacts joueurs.

## 3.6 Phase F - Dashboard / analytics

- [ ] Enregistrer resultats Dame dans `dameRoomResults`.
- [ ] Injecter Dame dans agrégations dashboard global (comme `morpionMatches`, `pongMatches`).
- [ ] Verifier compatibilite tableaux/graphes.
- [ ] Ajouter dashboard Dame dedie (equivalent Dmorpion).
- [ ] Ajouter carte/lien Dashboard Dame dans le hub dashboard.

## 3.7 Phase G - Scan dashboard impact (obligatoire avant code)

Fichiers detectes dans ce repo qui seront impactes:

- `functions/index.js`
  - global timeline: `computeTimelineGamesVolumeSnapshot`
  - snapshots exposes: `getGlobalAnalyticsSnapshot`, `getGamesVolumeAnalyticsSnapshot`
  - snapshots jeu specifiques: pattern `getMorpionAnalyticsSnapshot`, `getPongAnalyticsSnapshot` a dupliquer pour Dame.
- `secure-functions.js`
  - ajout wrapper `getDameAnalyticsSnapshotSecure`.
- `agent-dashboard.*`
  - peu probable pour analytics jeu, mais verifier si consommation de `getGlobalAnalyticsSnapshot`.
- `_dashboard_stage/index.html`
  - hub dashboard: ajouter tuile "Dame analytics" + lien.
  - note: plusieurs pages dashboard (ex `Dmorpion.html`) ne sont pas dans ce repo -> probablement dans le repo dashboard separé.

Checklist scan repo dashboard séparé (important):

- [ ] Rechercher `getGlobalAnalyticsSnapshot` / `getMorpionAnalyticsSnapshot` / `getPongAnalyticsSnapshot`.
- [ ] Ajouter consommation `getDameAnalyticsSnapshot`.
- [ ] Ajouter graphes Dame (trend, stakeMix, composition, winners).
- [ ] Mettre à jour les totaux globaux (gameMix) pour inclure Dame.
- [ ] Verifier qu’aucun composant ne casse si champ `dameMatches` absent (compat backward).

---

## 4) Garde-fous anti-casse

- [ ] Aucun changement breaking sur Morpion/Pong.
- [ ] Noms de fonctions Dame nouveaux (pas d’overload).
- [ ] Feature flag + fallback rapide vers coming-soon si incident.
- [ ] Jeux existants continuent de marcher sans migration immediate des anciens docs.
- [ ] Schémas Firestore Dame versionnés (`schemaVersion` recommandé).
- [ ] Tests manuels minimum:
  - [ ] public: joueur A attend 15s -> UI retry.
  - [ ] public: A + B match -> start auto.
  - [ ] friend: create -> copy code -> join -> start.
  - [ ] friend: create deux fois -> comportement defini (replace ou resume, a choisir).
  - [ ] leave room en attente.
  - [ ] payout gagnant.

---

## 5) Decisions a verrouiller avant codage Dame

- [ ] Politique "active-room-exists" Dame:
  - Option A: resume la room existante (comme Morpion classique)
  - Option B: ecraser ancienne room (comme Pong recent)
- [ ] Stakes Dame:
  - [ ] meme barème que Morpion ?
  - [ ] ou stakes specifiques ?
- [ ] Format exact des modales:
  - [ ] copy/texte identiques Morpion
  - [ ] ou version Dame labelisee.
- [ ] Invite proactive Dame:
  - [ ] inclure (parite stricte Morpion)
  - [ ] ou deferer v2.
- [ ] Politique WhatsApp Dame:
  - [ ] champs clients dedies (`dameWhatsapp...`)
  - [ ] ou mutualiser avec Morpion (risque de couplage a evaluer).

---

## 6) Sequence recommandee d’execution

1. Debloquer entry Dame (sans ouvrir le jeu finalement).
2. Poser UI page2 Dame complete.
3. Poser secure-functions Dame.
4. Poser backend Dame minimal (create/join/resume/ensure/leave).
5. Brancher runtime `dame.html` en waiting + start.
6. Ajouter submit action + fin de partie + payout.
7. Brancher analytics/dashboard.
8. QA complete + deploy progressif.

---

## 7) Strategie de deploiement cible (ne deployer que le necessaire)

Regle: ne jamais deployer `--only functions` globalement.

## 7.1 Deploiement functions par lots

- Lot room lifecycle Dame:
  - `createFriendDameRoom`
  - `joinFriendDameRoomByCode`
  - `resumeFriendDameRoom`
  - `joinMatchmakingDame`
  - `ensureRoomReadyDame`
  - `touchRoomPresenceDame`
  - `leaveRoomDame`
- Lot gameplay/payout Dame:
  - `submitActionDame`
  - `claimWinRewardDame`
  - `requestFriendDameRematch` (si implemente)
- Lot social Dame:
  - `getMyDameWhatsappPreferenceSecure`
  - `saveDameWhatsappPreferenceSecure`
  - `removeDameWhatsappPreferenceSecure`
  - `listRecentDameWhatsappContactsSecure`
  - `getMyActiveDameInvite`
  - `respondDamePlayInvite`
- Lot analytics Dame:
  - `getDameAnalyticsSnapshot`
  - `getGlobalAnalyticsSnapshot` (si schema global modifie)
  - `getGamesVolumeAnalyticsSnapshot` (si modifie)

Exemple commande:

`npx firebase deploy --only functions:createFriendDameRoom,functions:joinFriendDameRoomByCode,functions:resumeFriendDameRoom`

## 7.2 Deploiement hosting

- deploy hosting uniquement quand UI stable:
  - `page2.js`
  - `dame.html`
  - `logiquejeu-dame.js` (nouveau)
  - dashboard pages (repo dashboard si separé).

---

## 8) Definition of Done (DoD)

- [ ] Carte Dame sur home ouvre le vrai funnel (plus de coming soon).
- [ ] Dame operationnel public + friend, 2 humains uniquement.
- [ ] Aucun code bot charge sur Dame.
- [ ] UX attente/retry/extend identique Morpion.
- [ ] Social pack actif (notif + WhatsApp + contacts) selon decision.
- [ ] Analytics globaux incluent Dame sans casser les dashboards existants.
- [ ] Dashboard Dame dedie disponible avec graphes.
- [ ] Deploy fait par sous-lots de fonctions uniquement.

---

## 9) Mapping de reference Morpion -> Dame

Objectif: garantir parite de comportement, avec composants Dame autonomes.

## 9.1 Collections

- Morpion -> Dame:
  - `morpionRooms` -> `dameRooms`
  - `morpionGameStates` -> `dameGameStates`
  - `morpionRoomResults` -> `dameRoomResults`
  - `morpionMatchmakingPools` -> `dameMatchmakingPools`
  - `morpionWaitingRequests` -> `dameWaitingRequests` (si utilise)
  - `morpionPlayInvitations` -> `damePlayInvitations` (si invite proactive)

## 9.2 Callables gameplay

- Morpion -> Dame:
  - `joinMatchmakingMorpion` -> `joinMatchmakingDame`
  - `createFriendMorpionRoom` -> `createFriendDameRoom`
  - `joinFriendMorpionRoomByCode` -> `joinFriendDameRoomByCode`
  - `resumeFriendMorpionRoom` -> `resumeFriendDameRoom`
  - `ensureRoomReadyMorpion` -> `ensureRoomReadyDame`
  - `touchRoomPresenceMorpion` -> `touchRoomPresenceDame`
  - `leaveRoomMorpion` -> `leaveRoomDame`
  - `submitActionMorpion` -> `submitActionDame`
  - `claimWinRewardMorpion` -> `claimWinRewardDame`
  - `requestFriendMorpionRematch` -> `requestFriendDameRematch`

## 9.3 Callables social

- Morpion -> Dame:
  - `getMyMorpionWhatsappPreferenceSecure` -> `getMyDameWhatsappPreferenceSecure`
  - `saveMorpionWhatsappPreferenceSecure` -> `saveDameWhatsappPreferenceSecure`
  - `removeMorpionWhatsappPreferenceSecure` -> `removeDameWhatsappPreferenceSecure`
  - `listRecentMorpionWhatsappContactsSecure` -> `listRecentDameWhatsappContactsSecure`
  - `getMyActiveMorpionInvite` -> `getMyActiveDameInvite`
  - `respondMorpionPlayInvite` -> `respondDamePlayInvite`

## 9.4 Analytics

- Morpion/Pong -> Dame:
  - `computeMorpionAnalyticsSnapshot` / `computePongAnalyticsSnapshot` -> `computeDameAnalyticsSnapshot`
  - `getMorpionAnalyticsSnapshot` -> `getDameAnalyticsSnapshot`
  - Global timeline:
    - ajouter `dameQuery`
    - ajouter `summary.dameMatches` et `summary.dameWithBots` (attendu 0 bot)
    - ajouter `bucket.dameMatches`
    - ajouter `gameMix` entry `{ key: "dame", label: "Jeu de dame", ... }`
