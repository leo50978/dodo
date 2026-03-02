# Migration anti-triche - Dominoes Lakay

## 1) Analyse de risque (état actuel du site)

### Risques critiques identifiés
- Les mutations économiques (`doesBalance`, `xchanges`, `pendingPlay*`) étaient exécutées côté client (`xchange.js`) avec `runTransaction` direct Firestore.
- L'entrée en partie (débit de mise) était orchestrée côté client (`logiquejeu.js`), donc facilement contournable par console/injection JS.
- Les récompenses de victoire et la logique ambassadeur étaient déclenchées côté client (`logiquejeu.js` + `referral.js`).
- Les actions de jeu (tour `play/pass`) étaient aussi écrites côté client dans `rooms/{id}/actions`.

### Conséquence
Un joueur malveillant pouvait manipuler le front, rejouer des requêtes, ou forger des écritures Firestore pour tricher sur la monnaie et l'état de partie.

## 2) Objectif de sécurité

Déplacer toute logique sensible vers Firebase Functions (exécution serveur) et garder le front comme simple UI/lecture temps réel.

## 3) Implémentation réalisée (cette passe)

## Backend Functions (nouveau dossier `functions/`)
- `walletMutate`
  - Opérations autorisées: `xchange_buy`, `xchange_sell`, `game_entry`
  - Vérifications serveur: montants, multiplicateurs, mises autorisées, anti-solde négatif, règle "play before sell"
  - Écriture atomique wallet + historique
- `joinMatchmaking`
  - Vérifie auth
  - Débite la mise côté serveur
  - Rejoint une salle en attente ou crée une salle
  - Démarre auto la partie à 4 humains
- `submitAction`
  - Vérifie auth, appartenance à la salle, tour courant, seat joueur
  - Écrit l'action + met à jour l'état de tour côté serveur
- `claimWinReward`
  - Vérifie gagnant réel via `rooms/{roomId}.winnerSeat` et `seats[uid]`
  - Idempotence via `rooms/{roomId}/settlements/{uid}`
  - Crédite le gain côté serveur
- `recordAmbassadorOutcome`
  - Calcule win/loss depuis la salle (pas depuis le client)
  - Applique bonus/pénalité ambassadeur en transaction
  - Idempotence via `ambassadorGameEvents/{roomId_uid}`

## Frontend branché sur callables
- Nouveau wrapper: `secure-functions.js`
- `xchange.js`
  - Plus de transaction Firestore sensible locale
  - Toutes les mutations wallet passent par `walletMutate`
- `logiquejeu.js`
  - Matchmaking via `joinMatchmaking`
  - Actions de tour via `submitAction`
  - Récompense gagnant via `claimWinReward`
- `referral.js`
  - Résultat ambassadeur via `recordAmbassadorOutcome`

## 4) Plan béton - prochaines étapes (priorité)

### Phase A (immédiat)
- Déployer Functions
- Activer App Check
- Basculer Firestore Rules pour bloquer les écritures économiques directes côté client

### Phase B (durcissement jeu)
- Déplacer `startRoomIfNeeded`, `leaveRoom`, `endGameClick` en Functions
- Interdire en rules les updates sensibles sur `rooms` depuis le client

### Phase C (intégrité complète)
- Moteur d'arbitrage serveur (validation stricte des coups `play/pass`)
- Signature/session anti-replay pour actions critiques
- Monitoring & alerting (patterns anormaux)

## 5) Déploiement

Depuis `/home/leo/Music/domino v1/functions`:

```bash
npm install
firebase deploy --only functions
```

## 6) Règles Firestore (recommandation)

Ne pas déployer des rules ultra restrictives avant migration complète des écritures restantes.
Procéder par étapes pour éviter de casser les flux actifs.
