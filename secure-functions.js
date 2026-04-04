import { functions as firebaseFunctions, httpsCallable } from "./firebase-init.js";

const CALLABLE_CACHE = new Map();

function getCallable(name) {
  const key = String(name || "").trim();
  if (!key) throw new Error("Callable name is required");
  if (!CALLABLE_CACHE.has(key)) {
    CALLABLE_CACHE.set(key, httpsCallable(firebaseFunctions, key));
  }
  return CALLABLE_CACHE.get(key);
}

function normalizeCallableError(err, fallback = "Erreur serveur") {
  const codeRaw = String(err?.code || "");
  const firebaseCode = codeRaw.startsWith("functions/") ? codeRaw.slice("functions/".length) : codeRaw;
  const details = err?.details && typeof err.details === "object" ? err.details : {};

  const normalized = new Error(String(err?.message || fallback));
  normalized.code = String(details.code || firebaseCode || "unknown");

  Object.keys(details).forEach((k) => {
    normalized[k] = details[k];
  });

  return normalized;
}

async function invokeCallable(name, payload = {}, fallbackError = "Erreur serveur") {
  try {
    const callable = getCallable(name);
    const res = await callable(payload);
    return res?.data || null;
  } catch (err) {
    throw normalizeCallableError(err, fallbackError);
  }
}

export async function walletMutateSecure(payload = {}) {
  return invokeCallable("walletMutate", payload, "Impossible de mettre à jour le wallet.");
}

export async function joinMatchmakingSecure(payload = {}) {
  return invokeCallable("joinMatchmaking", payload, "Impossible de rejoindre une partie.");
}

export async function createFriendRoomSecure(payload = {}) {
  return invokeCallable("createFriendRoom", payload, "Impossible de créer une partie entre amis.");
}

export async function joinFriendRoomByCodeSecure(payload = {}) {
  return invokeCallable("joinFriendRoomByCode", payload, "Impossible de rejoindre la partie entre amis.");
}

export async function ensureRoomReadySecure(payload = {}) {
  return invokeCallable("ensureRoomReady", payload, "Impossible de demarrer la partie.");
}

export async function touchRoomPresenceSecure(payload = {}) {
  return invokeCallable("touchRoomPresence", payload, "Impossible de mettre à jour la présence.");
}

export async function ackRoomStartSeenSecure(payload = {}) {
  return invokeCallable("ackRoomStartSeen", payload, "Impossible de synchroniser le démarrage de la partie.");
}

export async function leaveRoomSecure(payload = {}) {
  return invokeCallable("leaveRoom", payload, "Impossible de quitter la salle.");
}

export async function finalizeGameSecure(payload = {}) {
  return invokeCallable("finalizeGame", payload, "Impossible de finaliser la partie.");
}

export async function confirmGameEndSecure(payload = {}) {
  return invokeCallable("confirmGameEnd", payload, "Impossible de valider la fin de partie.");
}

export async function claimWinRewardSecure(payload = {}) {
  return invokeCallable("claimWinReward", payload, "Impossible de valider le gain.");
}

export async function recordAmbassadorOutcomeSecure(payload = {}) {
  return invokeCallable("recordAmbassadorOutcome", payload, "Impossible de traiter le résultat ambassadeur.");
}

export async function submitActionSecure(payload = {}) {
  return invokeCallable("submitAction", payload, "Impossible d'envoyer l'action.");
}

export async function updateClientProfileSecure(payload = {}) {
  return invokeCallable("updateClientProfileSecure", payload, "Impossible de mettre à jour le profil.");
}

export async function createOrderSecure(payload = {}) {
  return invokeCallable("createOrderSecure", payload, "Impossible de créer la commande.");
}

export async function claimWelcomeBonusSecure(payload = {}) {
  return invokeCallable("claimWelcomeBonusSecure", payload, "Impossible de réclamer le bonus de bienvenue.");
}

export async function createWithdrawalSecure(payload = {}) {
  return invokeCallable("createWithdrawalSecure", payload, "Impossible de créer le retrait.");
}

export async function cancelWithdrawalSecure(payload = {}) {
  return invokeCallable("cancelWithdrawalSecure", payload, "Impossible d'annuler le retrait.");
}

export async function orderClientActionSecure(payload = {}) {
  return invokeCallable("orderClientActionSecure", payload, "Impossible de mettre à jour la demande.");
}

export async function ackClientFinanceNoticeSecure(payload = {}) {
  return invokeCallable("ackClientFinanceNoticeSecure", payload, "Impossible de marquer la notification comme lue.");
}

export async function getPublicPaymentOptionsSecure(payload = {}) {
  return invokeCallable("getPublicPaymentOptionsSecure", payload, "Impossible de charger les options de paiement.");
}

export async function getPublicGameStakeOptionsSecure(payload = {}) {
  return invokeCallable("getPublicGameStakeOptionsSecure", payload, "Impossible de charger les mises de partie.");
}

export async function getPublicDuelStakeOptionsSecure(payload = {}) {
  return invokeCallable("getPublicDuelStakeOptionsSecure", payload, "Impossible de charger les mises duel.");
}

export async function getPublicMorpionStakeOptionsSecure(payload = {}) {
  return invokeCallable("getPublicMorpionStakeOptionsSecure", payload, "Impossible de charger les mises morpion.");
}

export async function joinMatchmakingDuelSecure(payload = {}) {
  return invokeCallable("joinMatchmakingDuel", payload, "Impossible de rejoindre un duel.");
}

export async function createFriendDuelRoomSecure(payload = {}) {
  return invokeCallable("createFriendDuelRoom", payload, "Impossible de creer un duel entre amis.");
}

export async function joinFriendDuelRoomByCodeSecure(payload = {}) {
  return invokeCallable("joinFriendDuelRoomByCode", payload, "Impossible de rejoindre le duel entre amis.");
}

export async function ensureRoomReadyDuelSecure(payload = {}) {
  return invokeCallable("ensureRoomReadyDuel", payload, "Impossible de demarrer le duel.");
}

export async function touchRoomPresenceDuelSecure(payload = {}) {
  return invokeCallable("touchRoomPresenceDuel", payload, "Impossible de mettre a jour la presence duel.");
}

export async function ackRoomStartSeenDuelSecure(payload = {}) {
  return invokeCallable("ackRoomStartSeenDuel", payload, "Impossible de synchroniser le demarrage du duel.");
}

export async function leaveRoomDuelSecure(payload = {}) {
  return invokeCallable("leaveRoomDuel", payload, "Impossible de quitter la salle duel.");
}

export async function submitActionDuelSecure(payload = {}) {
  return invokeCallable("submitActionDuel", payload, "Impossible d'envoyer l'action duel.");
}

export async function claimWinRewardDuelSecure(payload = {}) {
  return invokeCallable("claimWinRewardDuel", payload, "Impossible de valider le gain duel.");
}

export async function joinMatchmakingMorpionSecure(payload = {}) {
  return invokeCallable("joinMatchmakingMorpion", payload, "Impossible de rejoindre une partie de morpion.");
}

export async function createFriendMorpionRoomSecure(payload = {}) {
  return invokeCallable("createFriendMorpionRoom", payload, "Impossible de creer une salle morpion entre amis.");
}

export async function joinFriendMorpionRoomByCodeSecure(payload = {}) {
  return invokeCallable("joinFriendMorpionRoomByCode", payload, "Impossible de rejoindre la salle morpion entre amis.");
}

export async function ensureRoomReadyMorpionSecure(payload = {}) {
  return invokeCallable("ensureRoomReadyMorpion", payload, "Impossible de demarrer la partie de morpion.");
}

export async function touchRoomPresenceMorpionSecure(payload = {}) {
  return invokeCallable("touchRoomPresenceMorpion", payload, "Impossible de mettre a jour la presence morpion.");
}

export async function ackRoomStartSeenMorpionSecure(payload = {}) {
  return invokeCallable("ackRoomStartSeenMorpion", payload, "Impossible de synchroniser le demarrage du morpion.");
}

export async function leaveRoomMorpionSecure(payload = {}) {
  return invokeCallable("leaveRoomMorpion", payload, "Impossible de quitter la salle morpion.");
}

export async function submitActionMorpionSecure(payload = {}) {
  return invokeCallable("submitActionMorpion", payload, "Impossible d'envoyer l'action morpion.");
}

export async function claimWinRewardMorpionSecure(payload = {}) {
  return invokeCallable("claimWinRewardMorpion", payload, "Impossible de valider le gain morpion.");
}

export async function getMyActiveMorpionInviteSecure(payload = {}) {
  return invokeCallable("getMyActiveMorpionInvite", payload, "Impossible de charger l'invitation morpion.");
}

export async function respondMorpionPlayInviteSecure(payload = {}) {
  return invokeCallable("respondMorpionPlayInvite", payload, "Impossible de repondre a l'invitation.");
}

export async function getMorpionLiveMatchmakingSignalSecure(payload = {}) {
  return invokeCallable("getMorpionLiveMatchmakingSignal", payload, "Impossible de charger le signal Morpion.");
}

export async function getMorpionMatchmakingHintSecure(payload = {}) {
  return invokeCallable("getMorpionMatchmakingHint", payload, "Impossible de charger l'indication de file Morpion.");
}

export async function getMyMorpionWhatsappPreferenceSecure(payload = {}) {
  return invokeCallable("getMyMorpionWhatsappPreferenceSecure", payload, "Impossible de charger ton numero WhatsApp morpion.");
}

export async function saveMorpionWhatsappPreferenceSecure(payload = {}) {
  return invokeCallable("saveMorpionWhatsappPreferenceSecure", payload, "Impossible d'enregistrer ton numero WhatsApp.");
}

export async function removeMorpionWhatsappPreferenceSecure(payload = {}) {
  return invokeCallable("removeMorpionWhatsappPreferenceSecure", payload, "Impossible de retirer ton numero WhatsApp.");
}

export async function listRecentMorpionWhatsappContactsSecure(payload = {}) {
  return invokeCallable("listRecentMorpionWhatsappContactsSecure", payload, "Impossible de charger les joueurs recemment actifs.");
}

export async function getRecruitmentCampaignSnapshotSecure(payload = {}) {
  return invokeCallable("getRecruitmentCampaignSnapshotSecure", payload, "Impossible de charger la campagne de recrutement.");
}

export async function recordRecruitmentVisitSecure(payload = {}) {
  return invokeCallable("recordRecruitmentVisitSecure", payload, "Impossible d'enregistrer la visite recrutement.");
}

export async function submitRecruitmentApplicationSecure(payload = {}) {
  return invokeCallable("submitRecruitmentApplicationSecure", payload, "Impossible d'envoyer la candidature.");
}

export async function getShareSitePromoStatusSecure(payload = {}) {
  return invokeCallable("getShareSitePromoStatus", payload, "Impossible de charger le bonus de partage.");
}

export async function recordShareSitePromoSecure(payload = {}) {
  return invokeCallable("recordShareSitePromo", payload, "Impossible d'enregistrer le partage.");
}

export async function getDepositFundingStatusSecure(payload = {}) {
  return invokeCallable("getDepositFundingStatusSecure", payload, "Impossible de charger l'état du dépôt.");
}

export async function searchAgentCandidatesSecure(payload = {}) {
  return invokeCallable("searchAgentCandidatesSecure", payload, "Impossible de rechercher les utilisateurs.");
}

export async function upsertAgentSecure(payload = {}) {
  return invokeCallable("upsertAgentSecure", payload, "Impossible de mettre à jour l'agent.");
}

export async function listAgentsSecure(payload = {}) {
  return invokeCallable("listAgentsSecure", payload, "Impossible de charger les agents.");
}

export async function getMyAgentDashboardSecure(payload = {}) {
  return invokeCallable("getMyAgentDashboardSecure", payload, "Impossible de charger le dashboard agent.");
}

export async function getAgentPayrollSnapshotSecure(payload = {}) {
  return invokeCallable("getAgentPayrollSnapshotSecure", payload, "Impossible de charger le payroll agent.");
}

export async function closeAgentPayrollMonthSecure(payload = {}) {
  return invokeCallable("closeAgentPayrollMonthSecure", payload, "Impossible de clôturer le payroll agent.");
}

export async function resolveDepositReviewSecure(payload = {}) {
  return invokeCallable("resolveDepositReviewSecure", payload, "Impossible de résoudre le dépôt.");
}

export async function unfreezeClientAccountSecure(payload = {}) {
  return invokeCallable("unfreezeClientAccountSecure", payload, "Impossible de dégeler le compte.");
}

export async function getGlobalAnalyticsSnapshotSecure(payload = {}) {
  return invokeCallable("getGlobalAnalyticsSnapshot", payload, "Impossible de charger les analytics globaux.");
}

export async function getClientAcquisitionSnapshotSecure(payload = {}) {
  return invokeCallable("getClientAcquisitionSnapshot", payload, "Impossible de charger les analytics d'acquisition.");
}

export async function markChatSeenSecure(payload = {}) {
  return invokeCallable("markChatSeenSecure", payload, "Impossible de marquer la discussion comme lue.");
}

export async function ensureSupportThreadSecure(payload = {}) {
  return invokeCallable("ensureSupportThreadSecure", payload, "Impossible d'ouvrir le fil de support.");
}

export async function getSupportMessagesSecure(payload = {}) {
  return invokeCallable("getSupportMessagesSecure", payload, "Impossible de charger les messages du support.");
}

export async function createSupportMessageSecure(payload = {}) {
  return invokeCallable("createSupportMessageSecure", payload, "Impossible d'envoyer le message au support.");
}

export async function markSupportThreadSeenSecure(payload = {}) {
  return invokeCallable("markSupportThreadSeenSecure", payload, "Impossible de marquer le support comme lu.");
}

export async function createAmbassadorSecure(payload = {}) {
  return invokeCallable("createAmbassadorSecure", payload, "Impossible de créer le compte ambassadeur.");
}

export async function ambassadorLoginSecure(payload = {}) {
  return invokeCallable("ambassadorLoginSecure", payload, "Impossible de connecter l'ambassadeur.");
}

export async function adminCheckSecure(payload = {}) {
  return invokeCallable("adminCheck", payload, "Accès administrateur refusé.");
}

export async function setBotDifficultySecure(payload = {}) {
  return invokeCallable("setBotDifficulty", payload, "Impossible de changer le niveau des bots.");
}

export async function setDuelBotDifficultySecure(payload = {}) {
  return invokeCallable("setDuelBotDifficulty", payload, "Impossible de changer le niveau du bot duel.");
}

export async function getDuelBotPilotSnapshotSecure(payload = {}) {
  return invokeCallable("getDuelBotPilotSnapshot", payload, "Impossible de charger le pilotage duel.");
}

export async function setDuelBotPilotControlSecure(payload = {}) {
  return invokeCallable("setDuelBotPilotControl", payload, "Impossible de mettre a jour le pilotage duel.");
}

export async function upsertSurveySecure(payload = {}) {
  return invokeCallable("upsertSurveySecure", payload, "Impossible d'enregistrer le sondage.");
}

export async function listSurveysSecure(payload = {}) {
  return invokeCallable("listSurveysSecure", payload, "Impossible de charger les sondages.");
}

export async function publishSurveySecure(payload = {}) {
  return invokeCallable("publishSurveySecure", payload, "Impossible de publier le sondage.");
}

export async function deleteSurveySecure(payload = {}) {
  return invokeCallable("deleteSurveySecure", payload, "Impossible de supprimer le sondage.");
}

export async function getSurveyResponsesSecure(payload = {}) {
  return invokeCallable("getSurveyResponsesSecure", payload, "Impossible de charger les réponses du sondage.");
}

export async function getActiveSurveyForUserSecure(payload = {}) {
  return invokeCallable("getActiveSurveyForUserSecure", payload, "Impossible de charger le sondage actif.");
}

export async function submitSurveyResponseSecure(payload = {}) {
  return invokeCallable("submitSurveyResponseSecure", payload, "Impossible d'envoyer la réponse au sondage.");
}
