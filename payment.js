// ============= PAYMENT COMPONENT - PROCESSUS DE PAIEMENT =============
import {
  claimWelcomeBonusSecure,
  createOrderSecure,
  getDepositFundingStatusSecure,
  getPublicPaymentOptionsSecure,
} from './secure-functions.js';

const OCR_LANGUAGE = 'fra+eng';
const DEPOSIT_BONUS_MIN_HTG = 100;
const DEPOSIT_BONUS_PERCENT = 10;
const DEPOSIT_BONUS_RATE_HTG_TO_DOES = 20;
const WELCOME_BONUS_HTG = 25;
const DEPOSIT_PROOF_TIMER_STORAGE_PREFIX = 'deposit_proof_started_at';
const DEPOSIT_RAPID_WARNING_STORAGE_PREFIX = 'deposit_rapid_warning_guard';
const DEPOSIT_RAPID_WARNING_DELAY_MS = 6 * 60 * 1000;
const DEPOSIT_RAPID_WARNING_THRESHOLD = 2;
const SUPPORT_WHATSAPP_DIGITS = '50940507232';
const SUPPORT_WHATSAPP_LABEL = '40507232';
let tesseractRuntimePromise = null;

async function loadTesseractRuntime() {
  if (typeof window !== 'undefined' && window.Tesseract && typeof window.Tesseract.recognize === 'function') {
    return window.Tesseract;
  }

  if (!tesseractRuntimePromise) {
    tesseractRuntimePromise = (async () => {
      const moduleUrls = [
        'https://cdn.jsdelivr.net/npm/tesseract.js@5.1.1/dist/tesseract.esm.min.js',
        'https://unpkg.com/tesseract.js@5.1.1/dist/tesseract.esm.min.js',
      ];

      for (const url of moduleUrls) {
        try {
          const mod = await import(url);
          const maybeLib = (mod && mod.default && typeof mod.default.recognize === 'function')
            ? mod.default
            : mod;
          if (maybeLib && typeof maybeLib.recognize === 'function') {
            return maybeLib;
          }
        } catch (_) {
          // fallback sur autre source
        }
      }

      await new Promise((resolve, reject) => {
        const existing = document.getElementById('tesseract-runtime-script');
        if (existing) {
          if (window.Tesseract && typeof window.Tesseract.recognize === 'function') {
            resolve();
            return;
          }
          existing.addEventListener('load', resolve, { once: true });
          existing.addEventListener('error', () => reject(new Error('Impossible de charger Tesseract')), { once: true });
          return;
        }

        const script = document.createElement('script');
        script.id = 'tesseract-runtime-script';
        script.src = 'https://cdn.jsdelivr.net/npm/tesseract.js@5.1.1/dist/tesseract.min.js';
        script.async = true;
        script.onload = resolve;
        script.onerror = () => reject(new Error('Impossible de charger Tesseract'));
        document.head.appendChild(script);
      });

      if (window.Tesseract && typeof window.Tesseract.recognize === 'function') {
        return window.Tesseract;
      }

      throw new Error('Tesseract indisponible');
    })().catch((error) => {
      tesseractRuntimePromise = null;
      throw error;
    });
  }

  return tesseractRuntimePromise;
}

function escapeHtml(value) {
  return String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

function escapeAttr(value) {
  return escapeHtml(value).replace(/`/g, '&#096;');
}

function sanitizePhoneInput(value) {
  return String(value || "")
    .replace(/[^\d+\-\s().]/g, "")
    .trim()
    .slice(0, 40);
}

function extractPhoneDigits(value) {
  return String(value || "").replace(/\D/g, "");
}

function sanitizeAsset(value) {
  const out = String(value || '').trim();
  if (!out) return '';

  const baseValue = out.replace(/\\/g, '/').split(/[?#]/)[0];
  const fileName = baseValue.split('/').pop() || '';
  if (!/^[a-zA-Z0-9._-]+\.(png|jpe?g|gif|webp|svg)$/i.test(fileName)) {
    return '';
  }
  return fileName;
}

function getPaymentFriendlyErrorMessage(error) {
  if (error?.code === 'account-frozen') {
    return error?.message || "Ton compte a été temporairement gelé après plusieurs dépôts refusés. Contacte l'assistance.";
  }
  const message = String(error?.message || '').trim();
  if (message) {
    return message;
  }
  return 'Une erreur est survenue. Veuillez réessayer.';
}

class PaymentModal {
  constructor(options = {}) {
    this.options = {
      amount: 0,
      client: null,
      cart: [],
      methodId: null,
      onClose: null,
      onSuccess: null,
      imageBasePath: './',
      delivery: null,
      ...options
    };
    
    this.uniqueId = 'payment_' + Math.random().toString(36).substr(2, 9);
    this.modal = null;
    this.methods = [];
    this.method = null;
    this.steps = [];
    this.currentStep = 0;
    this.clientData = this.options.client ? { ...this.options.client } : {};
    this.selectedMethod = null;
    this.settings = null;
    this.countdownInterval = null;
    this.timeLeft = 0;
    this.proofImageFile = null;
    this.extractedText = '';
    this.extractedTextStatus = 'pending';
    this.isSubmitted = false;
    this.confirmationMessage = "";
    this.isCompleted = false;
    this.fundingStatus = null;
    this.proofMode = this.options.flowType === 'welcome_bonus' ? 'welcome_bonus' : 'deposit';
    this.completedFlowType = this.options.flowType === 'welcome_bonus' ? 'welcome_bonus' : 'deposit';
    this.welcomeBonusCaptureReady = this.options.flowType === 'welcome_bonus' ? false : true;
    this.proofStepStartedAtMs = 0;
    this.proofSubmitAttemptDurationMs = 0;
    
    this.init();
  }

  getClientUid() {
    return String(this.options.client?.uid || this.options.client?.id || '').trim();
  }

  getProofTimerStorageKey() {
    const uid = this.getClientUid();
    return uid ? `${DEPOSIT_PROOF_TIMER_STORAGE_PREFIX}_${uid}` : '';
  }

  getRapidWarningStorageKey() {
    const uid = this.getClientUid();
    return uid ? `${DEPOSIT_RAPID_WARNING_STORAGE_PREFIX}_${uid}` : '';
  }

  readRapidWarningState() {
    const storageKey = this.getRapidWarningStorageKey();
    if (!storageKey) {
      return {
        windowStartedAtMs: 0,
        rapidAttemptCount: 0,
        lastAttemptAtMs: 0,
      };
    }
    try {
      const raw = window.localStorage.getItem(storageKey);
      const parsed = raw ? JSON.parse(raw) : null;
      return {
        windowStartedAtMs: Number(parsed?.windowStartedAtMs) || 0,
        rapidAttemptCount: Number(parsed?.rapidAttemptCount) || 0,
        lastAttemptAtMs: Number(parsed?.lastAttemptAtMs) || 0,
      };
    } catch (_) {
      return {
        windowStartedAtMs: 0,
        rapidAttemptCount: 0,
        lastAttemptAtMs: 0,
      };
    }
  }

  writeRapidWarningState(state) {
    const storageKey = this.getRapidWarningStorageKey();
    if (!storageKey) return;
    try {
      window.localStorage.setItem(storageKey, JSON.stringify({
        windowStartedAtMs: Number(state?.windowStartedAtMs) || 0,
        rapidAttemptCount: Number(state?.rapidAttemptCount) || 0,
        lastAttemptAtMs: Number(state?.lastAttemptAtMs) || 0,
      }));
    } catch (_) {
      // ignore storage failure
    }
  }

  clearRapidWarningState() {
    const storageKey = this.getRapidWarningStorageKey();
    if (!storageKey) return;
    try {
      window.localStorage.removeItem(storageKey);
    } catch (_) {
      // ignore storage failure
    }
  }

  shouldPromptRapidDepositWarning() {
    if (this.isWelcomeBonusSelected()) return false;
    if (!(this.proofSubmitAttemptDurationMs > 0) || this.proofSubmitAttemptDurationMs >= DEPOSIT_RAPID_WARNING_DELAY_MS) {
      this.clearRapidWarningState();
      return false;
    }

    const nowMs = Date.now();
    const previousState = this.readRapidWarningState();
    const withinWindow = previousState.windowStartedAtMs > 0
      && (nowMs - previousState.windowStartedAtMs) < DEPOSIT_RAPID_WARNING_DELAY_MS;
    const nextRapidAttemptCount = withinWindow
      ? previousState.rapidAttemptCount + 1
      : 1;

    this.writeRapidWarningState({
      windowStartedAtMs: withinWindow ? previousState.windowStartedAtMs : nowMs,
      rapidAttemptCount: nextRapidAttemptCount,
      lastAttemptAtMs: nowMs,
    });

    return nextRapidAttemptCount >= DEPOSIT_RAPID_WARNING_THRESHOLD;
  }

  ensureProofStepStartedAtMs() {
    if (this.isWelcomeBonusSelected()) {
      this.clearProofStepStartedAtMs();
      return 0;
    }
    if (this.proofStepStartedAtMs > 0) {
      return this.proofStepStartedAtMs;
    }
    const storageKey = this.getProofTimerStorageKey();
    let startedAtMs = 0;
    if (storageKey) {
      try {
        startedAtMs = Number(window.localStorage.getItem(storageKey)) || 0;
      } catch (_) {
        startedAtMs = 0;
      }
    }
    if (startedAtMs <= 0) {
      startedAtMs = Date.now();
      if (storageKey) {
        try {
          window.localStorage.setItem(storageKey, String(startedAtMs));
        } catch (_) {
          // ignore storage failure
        }
      }
    }
    this.proofStepStartedAtMs = startedAtMs;
    return startedAtMs;
  }

  clearProofStepStartedAtMs() {
    this.proofStepStartedAtMs = 0;
    this.proofSubmitAttemptDurationMs = 0;
    const storageKey = this.getProofTimerStorageKey();
    if (!storageKey) return;
    try {
      window.localStorage.removeItem(storageKey);
    } catch (_) {
      // ignore storage failure
    }
  }

  getProofStepDurationMs() {
    if (this.proofSubmitAttemptDurationMs > 0) {
      return this.proofSubmitAttemptDurationMs;
    }
    const startedAtMs = this.ensureProofStepStartedAtMs();
    if (startedAtMs <= 0) return 0;
    return Math.max(0, Date.now() - startedAtMs);
  }

  async confirmRapidDepositSubmission() {
    return new Promise((resolve) => {
      const overlay = document.createElement('div');
      overlay.style.cssText = `
        position: fixed;
        inset: 0;
        z-index: 2147483647;
        background: rgba(15, 23, 42, 0.72);
        backdrop-filter: blur(3px);
        display: flex;
        align-items: center;
        justify-content: center;
        padding: 1rem;
      `;

      const modal = document.createElement('div');
      modal.style.cssText = `
        width: min(100%, 460px);
        background: linear-gradient(180deg, #FFF9E8 0%, #F6E7B8 100%);
        border: 1px solid rgba(127, 29, 29, 0.18);
        border-radius: 24px;
        box-shadow: 0 28px 80px rgba(15, 23, 42, 0.28);
        padding: 1.35rem;
        color: #3F2D14;
      `;

      modal.innerHTML = `
        <div style="display:flex; align-items:flex-start; gap:0.85rem;">
          <div style="
            width:42px;
            height:42px;
            border-radius:999px;
            background: rgba(180, 83, 9, 0.12);
            color:#9A3412;
            display:flex;
            align-items:center;
            justify-content:center;
            font-size:1.2rem;
            flex-shrink:0;
          ">!</div>
          <div style="min-width:0;">
            <div style="font-size:1.08rem; font-weight:800; margin-bottom:0.35rem;">
              Avez-vous effectue ce depot ?
            </div>
            <div style="font-size:0.95rem; line-height:1.55; color:#6B4F2A;">
              Si vous ne l'avez pas effectue, le systeme le remarquera automatiquement et votre solde ne sera pas credite.
            </div>
            <div style="font-size:0.92rem; line-height:1.5; color:#7C2D12; margin-top:0.65rem; font-weight:700;">
              En cas de probleme, veuillez contacter l'assistance.
            </div>
          </div>
        </div>
        <div style="display:flex; flex-wrap:wrap; gap:0.65rem; margin-top:1.2rem;">
          <button type="button" data-rapid-confirm="cancel" style="
            flex:1 1 120px;
            min-height:46px;
            border:none;
            border-radius:14px;
            background:#E5E7EB;
            color:#374151;
            font-weight:700;
            cursor:pointer;
            padding:0.85rem 1rem;
          ">Annuler</button>
          <a href="https://wa.me/${SUPPORT_WHATSAPP_DIGITS}" target="_blank" rel="noopener noreferrer" data-rapid-confirm="support" style="
            flex:1 1 160px;
            min-height:46px;
            border-radius:14px;
            background:#16A34A;
            color:white;
            font-weight:800;
            text-decoration:none;
            display:flex;
            align-items:center;
            justify-content:center;
            padding:0.85rem 1rem;
          ">Contacter l'assistance</a>
          <button type="button" data-rapid-confirm="continue" style="
            flex:1 1 180px;
            min-height:46px;
            border:none;
            border-radius:14px;
            background:linear-gradient(135deg, #B45309 0%, #D97706 100%);
            color:white;
            font-weight:800;
            cursor:pointer;
            padding:0.85rem 1rem;
          ">Oui, j'ai effectue ce depot</button>
        </div>
        <div style="margin-top:0.75rem; font-size:0.82rem; color:#6B7280; text-align:center;">
          Assistance WhatsApp: ${SUPPORT_WHATSAPP_LABEL}
        </div>
      `;

      const cleanup = (result) => {
        overlay.remove();
        resolve(result);
      };

      overlay.addEventListener('click', (event) => {
        if (event.target === overlay) {
          cleanup(false);
        }
      });

      modal.querySelector('[data-rapid-confirm="cancel"]')?.addEventListener('click', () => cleanup(false));
      modal.querySelector('[data-rapid-confirm="continue"]')?.addEventListener('click', () => cleanup(true));

      overlay.appendChild(modal);
      document.body.appendChild(overlay);
    });
  }

  getDefaultSteps() {
    return [
      {
        type: 'custom',
        title: 'Vérification avant paiement',
        content: 'Vérifiez que votre compte de paiement sélectionné contient le montant du dépôt plus les taxes, puis continuez.',
        buttonText: 'Suivant'
      },
      {
        type: 'payment',
        title: 'Informations de paiement',
        instruction: 'Utilisez les données ci-dessous pour faire un dépôt ou transfert. Si vous utilisez le code QR, vous ne paierez pas de frais.',
        buttonText: 'Suivant'
      },
      {
        type: 'proof',
        title: 'Preuve de paiement',
        description: 'Ajoutez votre capture ou référence de transaction.',
        buttonText: 'Soumettre ma demande'
      },
      {
        type: 'confirmation',
        title: 'Confirmation',
        message: 'Votre demande est en cours de vérification. Le délai est de 12 heures.'
      }
    ];
  }

  getMethodSteps(method) {
    const steps = Array.isArray(method?.steps) ? method.steps.filter(Boolean) : [];
    return steps.length > 0 ? steps : this.getDefaultSteps();
  }
  
  async init() {
    await this.loadSettings();
    await this.loadFundingStatus();
    await this.loadPaymentMethods();
    this.render();
    this.attachEvents();
    this.animateIn();
    
    document.body.style.overflow = 'hidden';
  }
  
  async loadSettings() {
    try {
      const payload = await getPublicPaymentOptionsSecure({});
      this.settings = payload?.settings || {
        verificationHours: 12,
        expiredMessage: 'Le délai de vérification est dépassé. Contactez le support.'
      };
      this.methods = Array.isArray(payload?.methods)
        ? payload.methods
          .map((item) => {
            const data = { ...(item || {}) };
            data.steps = this.getMethodSteps(data);
            return data;
          })
          .filter((m) => m && m.isActive !== false)
        : [];
    } catch (error) {
      console.error('❌ Erreur chargement paramètres:', error);
      this.settings = { verificationHours: 12 };
      this.methods = [];
    }
  }

  async loadFundingStatus() {
    try {
      this.fundingStatus = await getDepositFundingStatusSecure({});
    } catch (error) {
      console.warn('⚠️ Impossible de charger le statut funding:', error);
      this.fundingStatus = null;
    }
  }
  
  async loadPaymentMethods() {
    if (!Array.isArray(this.methods)) {
      this.methods = [];
    }
    try {
      if (this.options.methodId) {
        this.selectedMethod = this.methods.find(m => m.id === this.options.methodId);
        if (this.selectedMethod) {
          this.steps = this.getMethodSteps(this.selectedMethod);
          this.currentStep = 1;
        }
      }
      
      if (this.methods.length === 1 && !this.selectedMethod) {
        this.selectedMethod = this.methods[0];
        this.steps = this.getMethodSteps(this.selectedMethod);
        this.currentStep = 1;
      }
    } catch (error) {
      console.error('❌ Erreur chargement méthodes:', error);
      this.methods = [];
    }
  }
  
  getImagePath(filename) {
    const safeFilename = sanitizeAsset(filename);
    if (!safeFilename) return '';
    if (safeFilename.startsWith('http')) return safeFilename;
    const cleanName = safeFilename.split('/').pop();
    return `${this.options.imageBasePath}${cleanName}`;
  }
  
  formatPrice(price) {
    return new Intl.NumberFormat('fr-FR', { 
      style: 'currency', 
      currency: 'HTG',
      minimumFractionDigits: 0
    }).format(price || 0);
  }

  formatInlineNumber(value, maximumFractionDigits = 2) {
    return new Intl.NumberFormat('fr-FR', {
      minimumFractionDigits: 0,
      maximumFractionDigits,
    }).format(Number(value) || 0);
  }

  getDepositBonusPreview() {
    const amountHtg = Math.max(0, Number(this.options?.amount) || 0);
    const eligible = amountHtg >= DEPOSIT_BONUS_MIN_HTG;
    const bonusHtgRaw = eligible ? (amountHtg * DEPOSIT_BONUS_PERCENT) / 100 : 0;
    const bonusDoes = eligible ? Math.floor(bonusHtgRaw * DEPOSIT_BONUS_RATE_HTG_TO_DOES) : 0;

    return {
      amountHtg,
      eligible,
      thresholdHtg: DEPOSIT_BONUS_MIN_HTG,
      bonusPercent: DEPOSIT_BONUS_PERCENT,
      bonusHtgRaw,
      bonusDoes,
      rateHtgToDoes: DEPOSIT_BONUS_RATE_HTG_TO_DOES,
    };
  }

  getWelcomeBonusStatus() {
    const funding = this.fundingStatus && typeof this.fundingStatus === 'object'
      ? this.fundingStatus
      : {};
    const hasRealApprovedDeposit = funding.hasRealApprovedDeposit === true
      || funding.hasApprovedDeposit === true
      || Number(funding.realApprovedDepositsHtg) > 0
      || Number(funding.approvedDepositsHtg) > 0;
    const alreadyClaimed = funding.welcomeBonusClaimed === true
      || Number(funding.welcomeBonusReceivedAtMs) > 0
      || Number(funding.welcomeBonusApprovedHtg) > 0;
    const eligibilityReason = String(funding.welcomeBonusEligibilityReason || '');
    const eligible = funding.welcomeBonusEligible === true;

    return {
      eligible,
      alreadyClaimed,
      hasRealApprovedDeposit,
      accountFrozen: funding.accountFrozen === true,
      isLegacyAccount: funding.isLegacyAccount === true,
      eligibilityReason,
      grantedHtg: WELCOME_BONUS_HTG,
      proofCode: String(funding.welcomeBonusProofCode || '').trim(),
      endAtMs: Number(funding.welcomeBonusEndAtMs) || 0,
    };
  }

  buildWelcomeBonusCaptureStep() {
    return {
      type: 'custom',
      variant: 'welcome_bonus_capture',
      title: 'Capture la preuve du bonus',
      buttonText: 'Suivant',
    };
  }

  getWelcomeBonusProofCode() {
    const fundingCode = String(this.fundingStatus?.welcomeBonusProofCode || '').trim().toUpperCase();
    if (fundingCode) return fundingCode;
    const profileCode = String(this.clientData?.welcomeBonusProofCode || this.options?.client?.welcomeBonusProofCode || '').trim().toUpperCase();
    if (profileCode) return profileCode;
    const uid = String(this.options?.client?.uid || this.options?.client?.id || '').replace(/[^a-zA-Z0-9]/g, '').toUpperCase();
    return uid ? `CLIENT-${uid.slice(0, 6)}-LOCAL` : 'CLIENT-BONUS';
  }

  isWelcomeBonusSelected() {
    return this.proofMode === 'welcome_bonus' && this.getWelcomeBonusStatus().eligible;
  }
  
  render() {
    this.modal = document.createElement('div');
    this.modal.className = `payment-modal-${this.uniqueId}`;
    this.modal.style.cssText = `
      position: fixed;
      top: 0;
      left: 0;
      right: 0;
      bottom: 0;
      width: 100vw;
      height: 100vh;
      background: rgba(0, 0, 0, 0.5);
      backdrop-filter: blur(8px);
      z-index: 1000000;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 1rem;
      opacity: 0;
      transition: opacity 0.3s ease;
    `;
    
    this.modal.innerHTML = `
      <div class="payment-container-${this.uniqueId} payment-theme-${this.uniqueId}" style="
        background: rgba(63, 71, 102, 0.58);
        border-radius: 1.5rem;
        width: 100%;
        max-width: 600px;
        max-height: 90vh;
        overflow-y: auto;
        border: 1px solid rgba(255,255,255,0.18);
        box-shadow: 14px 14px 34px rgba(17, 24, 39, 0.48), -10px -10px 24px rgba(113, 128, 168, 0.2);
        backdrop-filter: blur(14px);
        transform: scale(0.95);
        transition: transform 0.3s ease;
        position: relative;
      ">
        <!-- Header avec progression -->
        <div style="
          position: sticky;
          top: 0;
          background: rgba(63, 71, 102, 0.52);
          border-bottom: 1px solid rgba(255,255,255,0.14);
          padding: 1.5rem;
          z-index: 10;
          border-radius: 1.5rem 1.5rem 0 0;
        ">
          <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;">
            <div style="display: flex; align-items: center; gap: 1rem;">
              ${this.currentStep > 0 ? `
                <button class="back-step payment-icon-btn" style="
                  background: none;
                  border: none;
                  font-size: 1.2rem;
                  cursor: pointer;
                  color: rgba(255,255,255,0.82);
                  padding: 0.5rem;
                  width: 40px;
                  height: 40px;
                  display: flex;
                  align-items: center;
                  justify-content: center;
                  border-radius: 50%;
                  transition: all 0.2s;
                ">
                  <i class="fas fa-arrow-left"></i>
                </button>
              ` : ''}
              <h2 style="
                font-family: 'Cormorant Garamond', serif;
                font-size: 1.5rem;
                color: #ffffff;
                margin: 0;
              ">
                Paiement sécurisé
              </h2>
            </div>
            <button class="close-payment payment-icon-btn" style="
              background: none;
              border: none;
              font-size: 1.5rem;
              cursor: pointer;
              color: rgba(255,255,255,0.82);
              transition: all 0.2s;
              padding: 0.5rem;
              width: 40px;
              height: 40px;
              display: flex;
              align-items: center;
              justify-content: center;
              border-radius: 50%;
            ">
              <i class="fas fa-times"></i>
            </button>
          </div>
          
          ${this.renderProgressBar()}
        </div>
        
        <div style="padding: 1.5rem;">
          ${this.renderCurrentStep()}
        </div>
      </div>
      
      <style>
        .payment-container-${this.uniqueId} {
          animation: paymentSlideIn 0.3s ease forwards;
        }

        .payment-theme-${this.uniqueId} p,
        .payment-theme-${this.uniqueId} span,
        .payment-theme-${this.uniqueId} h1,
        .payment-theme-${this.uniqueId} h2,
        .payment-theme-${this.uniqueId} h3,
        .payment-theme-${this.uniqueId} h4,
        .payment-theme-${this.uniqueId} label {
          color: #ffffff !important;
        }

        .payment-theme-${this.uniqueId} .payment-icon-btn:hover {
          background: rgba(198, 167, 94, 0.1) !important;
          color: #C6A75E !important;
        }
        
        @keyframes paymentSlideIn {
          from {
            opacity: 0;
            transform: scale(0.95);
          }
          to {
            opacity: 1;
            transform: scale(1);
          }
        }
        
        .payment-container-${this.uniqueId}::-webkit-scrollbar {
          width: 6px;
        }
        
        .payment-container-${this.uniqueId}::-webkit-scrollbar-track {
          background: rgba(255,255,255,0.14);
          border-radius: 3px;
        }

        .payment-container-${this.uniqueId}::-webkit-scrollbar-thumb {
          background: rgba(245,124,0,0.85);
          border-radius: 3px;
        }
        
        .method-card {
          transition: all 0.25s ease;
          cursor: pointer;
          border: 1px solid rgba(255,255,255,0.2) !important;
          background: rgba(255,255,255,0.10) !important;
          backdrop-filter: blur(8px);
          box-shadow: 10px 10px 22px rgba(18,25,42,0.38), -8px -8px 18px rgba(121,135,173,0.18), inset 5px 5px 10px rgba(255,255,255,0.05), inset -5px -5px 10px rgba(8,13,24,0.18);
        }
        
        .method-card:hover {
          transform: translateY(-2px);
          background: rgba(255,255,255,0.14) !important;
          box-shadow: 12px 12px 24px rgba(16,22,38,0.42), -8px -8px 18px rgba(132,147,188,0.20), inset 5px 5px 10px rgba(255,255,255,0.06), inset -5px -5px 10px rgba(8,13,24,0.22);
        }
        
        .method-card.selected {
          border-color: #ffb26e !important;
          background: rgba(245,124,0,0.18) !important;
          box-shadow: 12px 12px 26px rgba(120,61,23,0.45), -8px -8px 18px rgba(255,174,98,0.14), inset 5px 5px 10px rgba(255,255,255,0.06), inset -5px -5px 10px rgba(8,13,24,0.22);
        }
        
        .countdown-timer {
          font-family: monospace;
          font-size: 1.5rem;
          font-weight: bold;
          color: #F57C00;
        }
        
        .form-group {
          margin-bottom: 1rem;
        }
        
        .form-group label {
          display: block;
          margin-bottom: 0.25rem;
          font-size: 0.9rem;
          color: rgba(255,255,255,0.82);
        }
        
        .form-group input,
        .form-group textarea,
        .form-group select {
          width: 100%;
          padding: 0.75rem;
          border: 1px solid rgba(255,255,255,0.24);
          border-radius: 0.9rem;
          background: rgba(255,255,255,0.12);
          color: #ffffff;
          box-shadow: inset 6px 6px 12px rgba(19, 26, 43, 0.42), inset -6px -6px 12px rgba(120, 134, 172, 0.22);
          font-size: 0.95rem;
        }
        
        .form-group input:focus,
        .form-group textarea:focus,
        .form-group select:focus {
          outline: none;
          border-color: #F57C00;
        }
        
        .next-step-btn {
          width: 100%;
          background: #F57C00;
          color: #ffffff;
          border: 1px solid #ffb26e;
          padding: 1rem;
          border-radius: 0.9rem;
          font-size: 1rem;
          font-weight: 500;
          cursor: pointer;
          transition: all 0.3s;
          margin-top: 1.5rem;
          box-shadow: 8px 8px 18px rgba(17, 24, 39, 0.42), -6px -6px 14px rgba(123, 137, 180, 0.2);
        }

        .next-step-btn:hover {
          background: #ff8b1f;
          color: #ffffff;
        }
        
        .next-step-btn:disabled {
          opacity: 0.5;
          cursor: not-allowed;
        }
        
        .warning-message {
          background: rgba(255,255,255,0.12);
          border-left: 4px solid #F57C00;
          padding: 1rem;
          border-radius: 0.5rem;
          margin-bottom: 1.5rem;
          font-size: 0.9rem;
        }
        
        .loading-spinner {
          display: inline-block;
          width: 20px;
          height: 20px;
          border: 2px solid rgba(255,255,255,0.3);
          border-radius: 50%;
          border-top-color: white;
          animation: spin 0.8s linear infinite;
        }
        
        @keyframes spin {
          to { transform: rotate(360deg); }
        }
        
      </style>
    `;
    
    document.body.appendChild(this.modal);
  }
  
  renderProgressBar() {
    const totalSteps = 1 + (this.steps?.length || 0);
    const currentStepDisplay = this.currentStep + 1;
    const progress = (currentStepDisplay / totalSteps) * 100;
    
    return `
      <div style="margin-top: 0.5rem;">
        <div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
          <span style="font-size: 0.85rem; color: #8B7E6B;">Étape ${currentStepDisplay}/${totalSteps}</span>
          <span style="font-size: 0.85rem; color: #8B7E6B;">${Math.round(progress)}%</span>
        </div>
        <div style="
          width: 100%;
          height: 4px;
          background: rgba(198, 167, 94, 0.2);
          border-radius: 2px;
          overflow: hidden;
        ">
          <div style="
            width: ${progress}%;
            height: 100%;
            background: #C6A75E;
            transition: width 0.3s ease;
          "></div>
        </div>
      </div>
    `;
  }
  
  renderCurrentStep() {
    if (this.currentStep === 0) {
      return this.renderStep0();
    }
    
    if (!this.steps || this.steps.length === 0) {
      return this.renderNoSteps();
    }
    
    const stepIndex = this.currentStep - 1;
    const step = this.steps[stepIndex];
    
    if (!step) {
      return this.renderNoSteps();
    }
    
    switch(step.type) {
      case 'form':
        return this.renderFormStep(step);
      case 'payment':
        return this.renderPaymentStep(step);
      case 'proof':
        return this.renderProofStep(step);
      case 'confirmation':
        return this.renderConfirmationStep(step);
      default:
        return this.renderCustomStep(step);
    }
  }
  
  renderStep0() {
    if (this.options.flowType === 'welcome_bonus' && this.getWelcomeBonusStatus().eligible && !this.welcomeBonusCaptureReady) {
      return this.renderCustomStep(this.buildWelcomeBonusCaptureStep());
    }

    if (this.methods.length === 0) {
      return `
        <div style="text-align: center; padding: 2rem;">
          <i class="fas fa-exclamation-triangle" style="font-size: 3rem; color: #B76E2E; margin-bottom: 1rem;"></i>
          <h3 style="font-size: 1.2rem; margin-bottom: 1rem;">Aucune méthode disponible</h3>
          <p style="color: #8B7E6B;">Veuillez réessayer plus tard.</p>
        </div>
      `;
    }
    
    return `
      <div>
        <h3 style="font-size: 1.3rem; margin-bottom: 1rem;">Choisissez votre méthode de paiement</h3>
        <p style="color: #8B7E6B; margin-bottom: 1.5rem;">Sélectionnez parmi nos options disponibles</p>
        
        <div id="methodsList" style="display: flex; flex-direction: column; gap: 1rem;">
          ${this.methods.map(method => this.renderMethodCard(method)).join('')}
        </div>
      </div>
    `;
  }
  
  renderMethodCard(method) {
    const isSelected = this.selectedMethod?.id === method.id;
    const safeMethodId = escapeAttr(method?.id || '');
    const safeMethodName = escapeHtml(method?.name || 'Méthode');
    const safeInstructions = escapeHtml(method?.instructions || '');
    const safeImagePath = escapeAttr(this.getImagePath(method?.image));
    
    return `
      <div class="method-card" data-method-id="${safeMethodId}" data-welcome-coach="payment-method" style="
        display: flex;
        align-items: center;
        gap: 1rem;
        padding: 1rem;
        border: 1px solid ${isSelected ? '#ffb26e' : 'rgba(255,255,255,0.2)'};
        border-radius: 1rem;
        background: ${isSelected ? 'rgba(245,124,0,0.18)' : 'rgba(255,255,255,0.10)'};
        color: #ffffff;
        cursor: pointer;
      ">
        <div style="
          width: 60px;
          height: 60px;
          min-width: 60px;
          min-height: 60px;
          flex-shrink: 0;
          background: rgba(255,255,255,0.14);
          border: 1px solid rgba(255,255,255,0.18);
          border-radius: 0.9rem;
          display: flex;
          align-items: center;
          justify-content: center;
          overflow: hidden;
          box-shadow: inset 4px 4px 9px rgba(255,255,255,0.05), inset -4px -4px 9px rgba(8,13,24,0.2);
        ">
          ${method.image ? 
            `<img src="${safeImagePath}" data-fallback-icon="fa-money-bill-wave" style="width: 100%; height: 100%; object-fit: cover;">` :
            `<i class="fas fa-money-bill-wave" style="font-size: 1.5rem; color: #C6A75E;"></i>`
          }
        </div>
        <div style="flex: 1;">
          <h4 style="font-weight: 600; margin-bottom: 0.25rem; color: #ffffff;">${safeMethodName}</h4>
          <p style="font-size: 0.85rem; color: rgba(255,255,255,0.75);">${safeInstructions}</p>
        </div>
        <div style="width: 24px; height: 24px; min-width: 24px; min-height: 24px; flex-shrink: 0; border-radius: 999px; border: 2px solid #ffb26e; display: flex; align-items: center; justify-content: center;">
          ${isSelected ? '<div style="width: 12px; height: 12px; border-radius: 999px; background: #ffb26e;"></div>' : ''}
        </div>
      </div>
    `;
  }
  
  renderFormStep(step) {
    const safeTitle = escapeHtml(step?.title || 'Vos informations');
    const safeDescription = escapeHtml(step?.description || '');
    const safeButtonText = escapeHtml(step?.buttonText || 'Continuer');
    return `
      <div>
        <h3 style="font-size: 1.3rem; margin-bottom: 0.5rem;">${safeTitle}</h3>
        <p style="color: #8B7E6B; margin-bottom: 1.5rem;">${safeDescription}</p>
        
        <form id="clientForm" class="space-y-4">
          ${step.fields?.map(field => this.renderFormField(field)).join('') || ''}
        </form>
        
        <button class="next-step-btn" id="nextStepBtn" data-welcome-coach="payment-step-next">
          ${safeButtonText}
        </button>
      </div>
    `;
  }
  
  renderFormField(field) {
    const value = this.clientData[field.name] || '';
    const required = field.required ? 'required' : '';
    const safeLabel = escapeHtml(field?.label || '');
    const safeName = escapeAttr(field?.name || '');
    const safeValue = escapeAttr(value);
    
    switch(field.type) {
      case 'textarea':
        return `
          <div class="form-group">
            <label>${safeLabel}${field.required ? ' *' : ''}</label>
            <textarea name="${safeName}" ${required} rows="3">${escapeHtml(value)}</textarea>
          </div>
        `;
      case 'select':
        return `
          <div class="form-group">
            <label>${safeLabel}${field.required ? ' *' : ''}</label>
            <select name="${safeName}" ${required}>
              <option value="">Sélectionnez...</option>
              ${field.options?.map(opt => `
                <option value="${escapeAttr(opt)}" ${value === opt ? 'selected' : ''}>${escapeHtml(opt)}</option>
              `).join('') || ''}
            </select>
          </div>
        `;
      case 'checkbox':
        return `
          <div class="form-group" style="display: flex; align-items: center; gap: 0.5rem;">
            <input type="checkbox" name="${safeName}" id="${safeName}" ${value ? 'checked' : ''}>
            <label for="${safeName}" style="margin: 0;">${safeLabel}${field.required ? ' *' : ''}</label>
          </div>
        `;
      default:
        return `
          <div class="form-group">
            <label>${safeLabel}${field.required ? ' *' : ''}</label>
            <input type="${escapeAttr(field?.type || 'text')}" name="${safeName}" value="${safeValue}" ${required}>
          </div>
        `;
    }
  }
  
  renderPaymentStep(step) {
    if (!this.selectedMethod) {
      return '<p class="text-accent">Veuillez d\'abord sélectionner une méthode</p>';
    }

    const accountName = this.selectedMethod.accountName || 'Jean Pierre';
    const phoneNumber = this.selectedMethod.phoneNumber || '45678909';
    const qrCodePath = this.getImagePath(this.selectedMethod.qrCode || 'qr.png');
    const safeTitle = escapeHtml(step?.title || 'Effectuez le paiement');
    const safeInstruction = escapeHtml(step?.instruction || 'Payez aux coordonnées suivantes :');
    const safeMethodName = escapeHtml(this.selectedMethod?.name || 'Méthode');
    const safeAccountName = escapeHtml(accountName);
    const safePhoneNumber = escapeHtml(phoneNumber);
    const safeMethodImage = escapeAttr(this.getImagePath(this.selectedMethod?.image));
    const safeQrCodePath = escapeAttr(qrCodePath);
    const safeButtonText = escapeHtml(step?.buttonText || "J'ai payé");
    
    return `
      <div>
        <h3 style="font-size: 1.3rem; margin-bottom: 1rem;">${safeTitle}</h3>
        
        <p style="color: #8B7E6B; margin-bottom: 1.5rem;">${safeInstruction}</p>
        
        <div style="
          background: rgba(255,255,255,0.1);
          border-radius: 1rem;
          padding: 1.5rem;
          margin-bottom: 1.5rem;
          border: 1px solid rgba(255,255,255,0.2);
        ">
          <div style="display: flex; align-items: center; gap: 1rem; margin-bottom: 1rem;">
            <div style="
              width: 60px;
              height: 60px;
              background: rgba(198,167,94,0.1);
              border-radius: 50%;
              display: flex;
              align-items: center;
              justify-content: center;
              overflow: hidden;
            ">
              ${this.selectedMethod.image ? 
                `<img src="${safeMethodImage}" data-fallback-icon="fa-university" style="width: 100%; height: 100%; object-fit: cover;">` :
                `<i class="fas fa-university" style="font-size: 1.5rem; color: #C6A75E;"></i>`
              }
            </div>
            <div>
              <h4 style="font-weight: 600;">${safeMethodName}</h4>
              <p style="font-size: 0.85rem; color: #8B7E6B;">Compte: ${safeAccountName}</p>
            </div>
          </div>
          
          <div style="
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 1rem 0;
            border-top: 1px solid rgba(198,167,94,0.2);
            border-bottom: 1px solid rgba(198,167,94,0.2);
          ">
            <span style="color: #8B7E6B;">Numéro</span>
            <span style="font-weight: 500;">${safePhoneNumber}</span>
          </div>
          
          <div style="
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 1rem 0;
          ">
            <span style="color: #8B7E6B;">Montant</span>
            <span style="font-weight: bold; font-size: 1.2rem;">${this.formatPrice(this.options.amount || 0)}</span>
          </div>
          
          ${qrCodePath ? `
            <div style="
              display: flex;
              flex-direction: column;
              align-items: center;
              padding: 1rem;
              background: rgba(255,255,255,0.15);
              border-radius: 0.5rem;
            ">
              <p style="font-size: 0.85rem; color: #8B7E6B; margin-bottom: 0.5rem;">Scannez le QR code</p>
              <img src="${safeQrCodePath}" data-hide-on-error="1" style="width: 150px; height: 150px; object-fit: contain;">
            </div>
          ` : ''}
        </div>
        
        <button class="next-step-btn" id="nextStepBtn">
          ${safeButtonText}
        </button>
      </div>
    `;
  }
  
  renderProofStep(step) {
    const expectedName = this.clientData.fullName || this.clientData.name || this.options.client?.name || '';
    const expectedDepositorPhone = sanitizePhoneInput(
      this.clientData.depositorPhone
      || this.clientData.phone
      || this.options.client?.depositorPhone
      || this.options.client?.phone
      || ''
    );
    const safeTitle = escapeHtml(step?.title || 'Confirmez votre paiement');
    const safeDescription = escapeHtml(step?.description || "Téléchargez une capture d'écran de votre transaction");
    const safeExpectedName = escapeHtml(expectedName);
    const safeExpectedAttr = escapeAttr(expectedName);
    const safeDepositorPhoneAttr = escapeAttr(expectedDepositorPhone);
    const safeButtonText = escapeHtml(step?.buttonText || 'Soumettre ma demande');
    const welcomeBonus = this.getWelcomeBonusStatus();
    const allowWelcomeChoice = this.options.allowWelcomeBonusChoice === true && welcomeBonus.eligible;
    const selectedProofMode = this.isWelcomeBonusSelected() ? 'welcome_bonus' : 'deposit';
    
    return `
      <div>
        <h3 style="font-size: 1.3rem; margin-bottom: 1rem;">${safeTitle}</h3>
        
        ${expectedName ? `
          <div class="warning-message">
            <i class="fas fa-exclamation-triangle" style="color: #B76E2E; margin-right: 0.5rem;"></i>
            <strong>Important :</strong> Le nom que vous saisissez doit correspondre exactement à celui de l'étape précédente : 
            <strong style="color: #1F1E1C;">${safeExpectedName}</strong>
          </div>
        ` : ''}
        
        <p style="color: #8B7E6B; margin-bottom: 1.5rem;">${safeDescription}</p>

        ${allowWelcomeChoice ? `
          <div id="proofModeWrap" style="
            display: grid;
            gap: 0.85rem;
            margin-bottom: 1.35rem;
          ">
            <label
              data-proof-mode-card="deposit"
              style="
                display: flex;
                align-items: flex-start;
                gap: 0.9rem;
                padding: 1rem;
                border-radius: 1rem;
                border: 1px solid ${selectedProofMode === 'deposit' ? 'rgba(255,178,110,0.9)' : 'rgba(255,255,255,0.16)'};
                background: ${selectedProofMode === 'deposit' ? 'rgba(245,124,0,0.14)' : 'rgba(255,255,255,0.08)'};
                cursor: pointer;
                transition: all 0.2s ease;
              "
            >
              <input type="radio" name="depositMode" value="deposit" ${selectedProofMode === 'deposit' ? 'checked' : ''} style="margin-top: 0.2rem;">
              <div>
                <p style="margin: 0; font-size: 0.75rem; letter-spacing: 0.14em; text-transform: uppercase; color: #CBD5E1;">Demande normale</p>
                <h4 style="margin: 0.35rem 0 0; font-size: 1rem; color: #FFFFFF;">J'envoie une vraie preuve de depot</h4>
                <p style="margin: 0.45rem 0 0; font-size: 0.9rem; color: #D7DFEF;">Ta demande sera revue par l'administration comme d'habitude.</p>
              </div>
            </label>

            <label
              data-proof-mode-card="welcome_bonus"
              style="
                display: flex;
                align-items: flex-start;
                gap: 0.9rem;
                padding: 1rem;
                border-radius: 1rem;
                border: 1px solid ${selectedProofMode === 'welcome_bonus' ? 'rgba(251,191,36,0.75)' : 'rgba(255,255,255,0.16)'};
                background: ${selectedProofMode === 'welcome_bonus' ? 'rgba(251,191,36,0.14)' : 'rgba(255,255,255,0.08)'};
                cursor: pointer;
                transition: all 0.2s ease;
              "
            >
              <input type="radio" name="depositMode" value="welcome_bonus" ${selectedProofMode === 'welcome_bonus' ? 'checked' : ''} style="margin-top: 0.2rem;">
              <div>
                <p style="margin: 0; font-size: 0.75rem; letter-spacing: 0.14em; text-transform: uppercase; color: #FCD34D;">Bonus bienvenue</p>
                <h4 style="margin: 0.35rem 0 0; font-size: 1rem; color: #FFFFFF;">Prendre mon bonus ${escapeHtml(this.formatInlineNumber(welcomeBonus.grantedHtg, 0))} HTG</h4>
                <p style="margin: 0.45rem 0 0; font-size: 0.9rem; color: #F8FAFC;">Tu suis les memes etapes pour te familiariser avec le depot, puis le bonus est credite automatiquement si ton compte est eligible.</p>
              </div>
            </label>
          </div>
        ` : ''}
        
        <form id="proofForm" class="space-y-4">
          <div class="form-group">
            <label>Confirmez votre nom *</label>
            <input type="text" id="proofName" required placeholder="Votre nom exact" value="${safeExpectedAttr}">
          </div>

          <div class="form-group">
            <label>Numero qui a effectue le depot *</label>
            <input type="tel" id="proofDepositorPhone" data-welcome-coach="proof-phone" required inputmode="tel" autocomplete="tel" placeholder="Ex: 50940507232" value="${safeDepositorPhoneAttr}">
            <p style="font-size: 0.8rem; color: #8B7E6B; margin-top: 0.25rem;">
              Cette information doit etre exacte. Une erreur peut entrainer le rejet de la demande.
            </p>
          </div>
          
          <div class="form-group">
            <label id="proofImageLabel">${selectedProofMode === 'welcome_bonus' ? "Image demandee pour le bonus *" : "Capture d'écran de la transaction *"}</label>
            <input type="file" id="proofImage" data-welcome-coach="proof-upload" accept="image/*" required>
            <p
              id="proofImageHelp"
              data-proof-mode-target="image-help"
              style="font-size: 0.8rem; color: #8B7E6B; margin-top: 0.25rem;"
            >${selectedProofMode === 'welcome_bonus'
              ? 'Charge l image recue pour activer ton bonus de bienvenue. Format accepte : JPG, PNG (max 5 Mo).'
              : "Format accepte : JPG, PNG (max 5 Mo)"}</p>
          </div>
          
          <div id="imagePreview" style="display: none; margin-top: 1rem; text-align: center;">
            <img id="previewImg" style="max-width: 100%; max-height: 200px; border-radius: 0.5rem; border: 1px solid rgba(198,167,94,0.3);">
          </div>
        </form>
        
        <button class="next-step-btn" id="nextStepBtn" data-welcome-coach="proof-submit">
          ${safeButtonText}
        </button>
      </div>
    `;
  }
  
  renderConfirmationStep(step) {
    if (this.completedFlowType === 'welcome_bonus') {
      this.stopCountdown();
    } else {
      this.startCountdown();
    }
    const safeMessage = escapeHtml(this.confirmationMessage || step?.message || 'Votre demande est en cours de vérification. Elle sera traitée sous 12 heures.');
    const bonusPreview = this.getDepositBonusPreview();
    const timingPanel = this.completedFlowType === 'welcome_bonus'
      ? `
        <div style="
          background: rgba(255,255,255,0.08);
          border-radius: 1rem;
          padding: 1.2rem;
          margin-bottom: 1.5rem;
          border: 1px solid rgba(255,255,255,0.1);
        ">
          <p style="font-size: 0.9rem; color: #CBD5E1; margin-bottom: 0.45rem;">Statut</p>
          <div style="font-size: 1.25rem; font-weight: 800; color: #FBBF24;">Activation immediate</div>
        </div>
      `
      : `
        <div style="
          background: white;
          border-radius: 1rem;
          padding: 1.5rem;
          margin-bottom: 1.5rem;
        ">
          <p style="font-size: 0.9rem; color: #8B7E6B; margin-bottom: 0.5rem;">Temps restant avant vérification</p>
          <div class="countdown-timer" id="countdownTimer">12:00:00</div>
        </div>
      `;
    const bonusPanel = this.completedFlowType === 'welcome_bonus'
      ? `
        <div style="
          margin: 1.25rem 0 0;
          border: 1px solid rgba(255,255,255,0.14);
          border-radius: 1.15rem;
          background: linear-gradient(180deg, rgba(44, 52, 78, 0.94), rgba(33, 39, 60, 0.96));
          padding: 1rem;
          text-align: left;
          color: #F8FAFC;
          box-shadow: 0 16px 34px rgba(15,23,42,0.24), inset 0 1px 0 rgba(255,255,255,0.06);
        ">
          <p style="margin: 0; font-size: 0.72rem; letter-spacing: 0.16em; text-transform: uppercase; color: #FBBF24; font-weight: 800;">Bonus bienvenue</p>
          <h4 style="margin: 0.55rem 0 0; font-size: 1.05rem; color: #FFFFFF;">Ton bonus ${escapeHtml(this.formatInlineNumber(WELCOME_BONUS_HTG, 0))} HTG a ete ajoute</h4>
          <div style="
            margin-top: 0.9rem;
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
            gap: 0.75rem;
          ">
            <div style="border-radius: 0.95rem; background: rgba(255,255,255,0.08); padding: 0.9rem; border: 1px solid rgba(255,255,255,0.08);">
              <p style="margin: 0; font-size: 0.75rem; color: #CBD5E1; font-weight: 700;">Bonus credite</p>
              <p style="margin: 0.4rem 0 0; font-size: 1.05rem; color: #FFFFFF; font-weight: 900;">${escapeHtml(this.formatInlineNumber(WELCOME_BONUS_HTG, 0))} HTG</p>
            </div>
            <div style="border-radius: 0.95rem; background: rgba(251,191,36,0.12); padding: 0.9rem; border: 1px solid rgba(251,191,36,0.18);">
              <p style="margin: 0; font-size: 0.75rem; color: #FCD34D; font-weight: 700;">Type</p>
              <p style="margin: 0.4rem 0 0; font-size: 1.05rem; color: #FFFFFF; font-weight: 900;">Bienvenue</p>
            </div>
          </div>
          <div style="
            margin-top: 0.9rem;
            border-radius: 0.95rem;
            background: rgba(15,23,42,0.24);
            padding: 0.9rem;
            color: #E2E8F0;
            line-height: 1.65;
            font-size: 0.92rem;
            border: 1px solid rgba(255,255,255,0.08);
          ">
            <p style="margin: 0;"><strong>Important :</strong> ce bonus de bienvenue est bien reel, mais il suit des regles bonus distinctes d un depot classique.</p>
            <p style="margin: 0.65rem 0 0;">Tu peux maintenant explorer le systeme, jouer et te familiariser avec les etapes de depot avant ton premier vrai depot approuve.</p>
          </div>
        </div>
      `
      : bonusPreview.eligible
      ? `
        <div style="
          margin: 1.25rem 0 0;
          border: 1px solid rgba(255,255,255,0.14);
          border-radius: 1.15rem;
          background: linear-gradient(180deg, rgba(44, 52, 78, 0.94), rgba(33, 39, 60, 0.96));
          padding: 1rem;
          text-align: left;
          color: #F8FAFC;
          box-shadow: 0 16px 34px rgba(15,23,42,0.24), inset 0 1px 0 rgba(255,255,255,0.06);
        ">
          <p style="margin: 0; font-size: 0.72rem; letter-spacing: 0.16em; text-transform: uppercase; color: #FBBF24; font-weight: 800;">Bonus depot</p>
          <h4 style="margin: 0.55rem 0 0; font-size: 1.05rem; color: #FFFFFF;">Ton depot peut recevoir un bonus apres approbation</h4>
          <div style="
            margin-top: 0.9rem;
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
            gap: 0.75rem;
          ">
            <div style="border-radius: 0.95rem; background: rgba(255,255,255,0.08); padding: 0.9rem; border: 1px solid rgba(255,255,255,0.08);">
              <p style="margin: 0; font-size: 0.75rem; color: #CBD5E1; font-weight: 700;">Depot soumis</p>
              <p style="margin: 0.4rem 0 0; font-size: 1.05rem; color: #FFFFFF; font-weight: 900;">${escapeHtml(this.formatInlineNumber(bonusPreview.amountHtg, 0))} HTG</p>
            </div>
            <div style="border-radius: 0.95rem; background: rgba(251,191,36,0.12); padding: 0.9rem; border: 1px solid rgba(251,191,36,0.18);">
              <p style="margin: 0; font-size: 0.75rem; color: #FCD34D; font-weight: 700;">Bonus promo</p>
              <p style="margin: 0.4rem 0 0; font-size: 1.05rem; color: #FFFFFF; font-weight: 900;">+${escapeHtml(this.formatInlineNumber(bonusPreview.bonusDoes, 0))} Does</p>
            </div>
          </div>
          <div style="
            margin-top: 0.9rem;
            border-radius: 0.95rem;
            background: rgba(15,23,42,0.24);
            padding: 0.9rem;
            color: #E2E8F0;
            line-height: 1.65;
            font-size: 0.92rem;
            border: 1px solid rgba(255,255,255,0.08);
          ">
            <p style="margin: 0;"><strong>Comment ca marche:</strong> ton depot monte d'abord en <strong>HTG en examen</strong>. Si l'administration approuve la demande, le systeme calcule automatiquement <strong>${escapeHtml(this.formatInlineNumber(bonusPreview.bonusPercent, 0))}%</strong> du depot, puis convertit ce bonus en Does.</p>
            <p style="margin: 0.65rem 0 0;">Pour ce depot, cela represente environ <strong>${escapeHtml(this.formatInlineNumber(bonusPreview.bonusHtgRaw))} HTG</strong> de bonus, soit <strong>${escapeHtml(this.formatInlineNumber(bonusPreview.bonusDoes, 0))} Does</strong> au taux actuel de <strong>${escapeHtml(this.formatInlineNumber(bonusPreview.rateHtgToDoes, 0))} Does</strong> par HTG.</p>
            <p style="margin: 0.65rem 0 0;">Le bonus n'apparait pas avant l'approbation. S'il y a rejet, aucun bonus n'est ajoute.</p>
          </div>
        </div>
      `
      : `
        <div style="
          margin: 1.25rem 0 0;
          border: 1px solid rgba(255,255,255,0.14);
          border-radius: 1.15rem;
          background: linear-gradient(180deg, rgba(44, 52, 78, 0.94), rgba(33, 39, 60, 0.96));
          padding: 1rem;
          text-align: left;
          color: #E2E8F0;
          line-height: 1.65;
          font-size: 0.92rem;
          box-shadow: 0 16px 34px rgba(15,23,42,0.24), inset 0 1px 0 rgba(255,255,255,0.06);
        ">
          <p style="margin: 0; font-size: 0.72rem; letter-spacing: 0.16em; text-transform: uppercase; color: #FBBF24; font-weight: 800;">Bonus depot</p>
          <p style="margin: 0.6rem 0 0; color: #F8FAFC;"><strong>Info importante:</strong> le bonus promo commence a partir de <strong>${escapeHtml(this.formatInlineNumber(bonusPreview.thresholdHtg, 0))} HTG</strong> approuves.</p>
          <p style="margin: 0.55rem 0 0; color: #CBD5E1;">Ce depot sera donc traite normalement: il monte en <strong>HTG en examen</strong>, puis sera valide ou rejete par l'administration.</p>
        </div>
      `;
    
    return `
      <div style="text-align: center; padding: 1rem 0;">
        <div style="
          width: 100px;
          height: 100px;
          background: #2E5D3A;
          border-radius: 50%;
          display: flex;
          align-items: center;
          justify-content: center;
          margin: 0 auto 1.5rem;
        ">
          <i class="fas fa-check" style="font-size: 3rem; color: white;"></i>
        </div>
        
        <h3 style="font-size: 1.5rem; margin-bottom: 1rem;">Demande soumise avec succès !</h3>
        
        <p style="color: #8B7E6B; margin-bottom: 2rem;">
          ${safeMessage}
        </p>

        ${timingPanel}

        ${bonusPanel}
        
        <p style="font-size: 0.9rem; color: #8B7E6B;">
          <i class="fas fa-clock" style="margin-right: 0.3rem;"></i>
          Vous pouvez suivre le statut de votre demande dans le module solde.
        </p>
        
        <button class="next-step-btn" id="closeAfterConfirmation" style="margin-top: 2rem;">
          Fermer
        </button>
      </div>
    `;
  }
  
  renderCustomStep(step) {
    if (step?.variant === 'welcome_bonus_capture') {
      const proofCode = escapeHtml(this.getWelcomeBonusProofCode());
      return `
        <div>
          <h3 style="font-size: 1.3rem; margin-bottom: 1rem;">Capture la preuve du bonus</h3>
          <p style="color: #8B7E6B; margin-bottom: 1rem;">Fais une capture d'écran de cette carte. Elle sera utilisée comme preuve dans la dernière étape.</p>

          <div data-welcome-coach="proof-card" style="
            border: 1px solid rgba(251,191,36,0.24);
            border-radius: 1.25rem;
            padding: 1.25rem;
            background: linear-gradient(180deg, rgba(50,57,84,0.96), rgba(34,40,61,0.98));
            color: #F8FAFC;
            box-shadow: 0 18px 36px rgba(15,23,42,0.25), inset 0 1px 0 rgba(255,255,255,0.06);
          ">
            <p style="margin: 0; font-size: 0.74rem; letter-spacing: 0.16em; text-transform: uppercase; color: #FBBF24; font-weight: 800;">Bonus bienvenue</p>
            <h4 style="margin: 0.7rem 0 0; font-size: 1.18rem; color: #FFFFFF;">Obtenir mon bonus de 25 Gdes</h4>
            <div style="
              margin-top: 1rem;
              border-radius: 1rem;
              border: 1px solid rgba(255,255,255,0.08);
              background: rgba(255,255,255,0.07);
              padding: 1rem;
            ">
              <p style="margin: 0; font-size: 0.74rem; text-transform: uppercase; letter-spacing: 0.12em; color: #CBD5E1;">Client ID</p>
              <p style="margin: 0.45rem 0 0; font-size: 1.1rem; font-weight: 900; color: #FFFFFF; letter-spacing: 0.08em;">${proofCode}</p>
            </div>
          </div>

          <div style="
            margin-top: 1rem;
            border-radius: 1rem;
            background: rgba(245,124,0,0.10);
            border: 1px solid rgba(255,178,110,0.18);
            padding: 1rem;
            color: #F8FAFC;
            line-height: 1.65;
          ">
            Cette image doit apparaître dans ta capture. Garde-la bien, puis clique sur suivant pour continuer le processus.
          </div>

          <button class="next-step-btn" id="nextStepBtn" data-welcome-coach="proof-card-next">
            ${escapeHtml(step?.buttonText || 'Suivant')}
          </button>
        </div>
      `;
    }

    const safeTitle = escapeHtml(step?.title || 'Étape personnalisée');
    const safeContent = escapeHtml(step?.content || '');
    const safeButtonText = escapeHtml(step?.buttonText || 'Continuer');
    return `
      <div>
        <h3 style="font-size: 1.3rem; margin-bottom: 1rem;">${safeTitle}</h3>
        <div style="
          background: white;
          border-radius: 1rem;
          padding: 1.5rem;
          white-space: pre-line;
        ">
          ${safeContent}
        </div>
        
        <button class="next-step-btn" id="nextStepBtn">
          ${safeButtonText}
        </button>
      </div>
    `;
  }
  
  renderNoSteps() {
    return `
      <div style="text-align: center; padding: 2rem;">
        <i class="fas fa-exclamation-triangle" style="font-size: 3rem; color: #B76E2E; margin-bottom: 1rem;"></i>
        <h3 style="font-size: 1.2rem; margin-bottom: 1rem;">Configuration incomplète</h3>
        <p style="color: #8B7E6B;">Cette méthode de paiement n'est pas correctement configurée.</p>
      </div>
    `;
  }
  
  attachEvents() {
    this.bindAssetFallbacks();

    const closeBtn = this.modal.querySelector('.close-payment');
    if (closeBtn) {
      closeBtn.addEventListener('click', () => this.close());
    }
    
    const backBtn = this.modal.querySelector('.back-step');
    if (backBtn) {
      backBtn.addEventListener('click', () => this.goBack());
    }
    
    this.modal.addEventListener('click', (e) => {
      if (e.target === this.modal) {
        this.close();
      }
    });
    
    if (this.currentStep === 0) {
      this.attachStep0Events();
    } else {
      this.attachStepEvents();
    }
    
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') {
        this.close();
      }
    });
  }

  bindAssetFallbacks() {
    if (!this.modal) return;

    this.modal.querySelectorAll('img[data-hide-on-error="1"]').forEach((img) => {
      if (img.dataset.errorBound === '1') return;
      img.dataset.errorBound = '1';
      img.addEventListener('error', () => {
        img.style.display = 'none';
      });
    });

    this.modal.querySelectorAll('img[data-fallback-icon]').forEach((img) => {
      if (img.dataset.errorBound === '1') return;
      img.dataset.errorBound = '1';
      img.addEventListener('error', () => {
        const parent = img.parentElement;
        if (!parent) {
          img.style.display = 'none';
          return;
        }
        if (parent.dataset.fallbackApplied === '1') return;
        parent.dataset.fallbackApplied = '1';
        while (parent.firstChild) {
          parent.removeChild(parent.firstChild);
        }
        const icon = document.createElement('i');
        icon.className = `fas ${img.dataset.fallbackIcon || 'fa-image'}`;
        icon.style.fontSize = '1.5rem';
        icon.style.color = '#C6A75E';
        parent.appendChild(icon);
      });
    });
  }
  
  attachStep0Events() {
    const introNextBtn = this.modal.querySelector('#nextStepBtn');
    if (introNextBtn && this.options.flowType === 'welcome_bonus' && this.getWelcomeBonusStatus().eligible && !this.welcomeBonusCaptureReady) {
      introNextBtn.addEventListener('click', () => {
        this.welcomeBonusCaptureReady = true;
        this.updateStepDisplay();
      });
      return;
    }

    const methodsList = this.modal.querySelector('#methodsList');
    
    if (methodsList) {
      methodsList.querySelectorAll('.method-card').forEach(card => {
        card.addEventListener('click', () => {
          const methodId = card.dataset.methodId;
          const method = this.methods.find(m => m.id === methodId);
          
          if (method) {
            this.selectedMethod = method;
            this.steps = this.getMethodSteps(this.selectedMethod);
            this.currentStep = 1;
            this.updateStepDisplay();
          }
        });
      });
    }
  }
  
  attachStepEvents() {
    const nextBtn = this.modal.querySelector('#nextStepBtn');
    if (nextBtn) {
      nextBtn.addEventListener('click', () => this.handleNextStep());
    }
    
    const closeBtn = this.modal.querySelector('#closeAfterConfirmation');
    if (closeBtn) {
      closeBtn.addEventListener('click', () => this.close());
    }
    
    const proofImage = this.modal.querySelector('#proofImage');
    if (proofImage) {
      proofImage.addEventListener('change', (e) => {
        const file = e.target.files[0];
        if (file) {
          if (file.size > 5 * 1024 * 1024) {
            alert('L\'image est trop volumineuse. Taille maximum : 5 Mo');
            proofImage.value = '';
            return;
          }
          
          const reader = new FileReader();
          reader.onload = (e) => {
            const preview = this.modal.querySelector('#imagePreview');
            const img = this.modal.querySelector('#previewImg');
            if (preview && img) {
              img.src = e.target.result;
              preview.style.display = 'block';
            }
            this.proofImageFile = file;
          };
          reader.readAsDataURL(file);
        }
      });
    }

    this.modal.querySelectorAll('input[name="depositMode"]').forEach((input) => {
      input.addEventListener('change', () => {
        this.proofMode = input.value === 'welcome_bonus' ? 'welcome_bonus' : 'deposit';
        this.syncProofModeUi();
      });
    });
    this.syncProofModeUi();
  }

  syncProofModeUi() {
    if (!this.modal) return;
    const selectedMode = this.isWelcomeBonusSelected() ? 'welcome_bonus' : 'deposit';
    this.proofMode = selectedMode;
    if (selectedMode === 'welcome_bonus') {
      this.clearProofStepStartedAtMs();
    } else {
      this.ensureProofStepStartedAtMs();
    }

    this.modal.querySelectorAll('[data-proof-mode-card]').forEach((card) => {
      const mode = card.getAttribute('data-proof-mode-card');
      const active = mode === selectedMode;
      card.style.borderColor = active
        ? (mode === 'welcome_bonus' ? 'rgba(251,191,36,0.75)' : 'rgba(255,178,110,0.9)')
        : 'rgba(255,255,255,0.16)';
      card.style.background = active
        ? (mode === 'welcome_bonus' ? 'rgba(251,191,36,0.14)' : 'rgba(245,124,0,0.14)')
        : 'rgba(255,255,255,0.08)';
    });

    const helpEl = this.modal.querySelector('#proofImageHelp');
    if (helpEl) {
      helpEl.textContent = selectedMode === 'welcome_bonus'
        ? "Charge l image recue pour activer ton bonus de bienvenue. Format accepte : JPG, PNG (max 5 Mo)."
        : "Format accepte : JPG, PNG (max 5 Mo)";
    }

    const labelEl = this.modal.querySelector('#proofImageLabel');
    if (labelEl) {
      labelEl.textContent = selectedMode === 'welcome_bonus'
        ? "Image demandee pour le bonus *"
        : "Capture d'écran de la transaction *";
    }

    const nextBtn = this.modal.querySelector('#nextStepBtn');
    if (nextBtn) {
      nextBtn.textContent = selectedMode === 'welcome_bonus'
        ? `Recevoir mon bonus ${this.formatInlineNumber(WELCOME_BONUS_HTG, 0)} HTG`
        : 'Soumettre ma demande';
    }
  }
  
  goBack() {
    if (this.currentStep > 0 && this.currentStep < this.steps.length) {
      this.currentStep--;
      this.updateStepDisplay();
    }
  }
  
  async handleNextStep() {
    const stepIndex = this.currentStep - 1;
    const step = this.steps[stepIndex];
    
    if (!step) return;
    
    const nextBtn = this.modal.querySelector('#nextStepBtn');
    if (nextBtn) {
      nextBtn.disabled = true;
      nextBtn.innerHTML = '<div class="loading-spinner"></div> Traitement...';
    }
    
    try {
      let isValid = true;
      
      switch(step.type) {
        case 'form':
          isValid = this.validateFormStep();
          break;
        case 'proof':
          isValid = await this.validateProofStep();
          break;
        case 'payment':
          break;
        default:
          break;
      }
      
      if (!isValid) {
        if (nextBtn) {
          nextBtn.disabled = false;
          nextBtn.innerHTML = step.type === 'proof'
            ? (this.isWelcomeBonusSelected()
              ? `Recevoir mon bonus ${this.formatInlineNumber(WELCOME_BONUS_HTG, 0)} HTG`
              : (step.buttonText || 'Soumettre ma demande'))
            : (step.buttonText || 'Continuer');
        }
        return;
      }
      
      if (step.type === 'proof') {
        this.clearProofStepStartedAtMs();
        this.isSubmitted = true;
        this.isCompleted = true;
        
        this.currentStep++;
        this.updateStepDisplay();
        
        return;
      }
      
      if (this.currentStep < this.steps.length) {
        this.currentStep++;
        this.updateStepDisplay();
      }
    } catch (error) {
      console.error('❌ Erreur:', error);
      if (nextBtn) {
        nextBtn.disabled = false;
        nextBtn.innerHTML = step.type === 'proof'
          ? (this.isWelcomeBonusSelected()
            ? `Recevoir mon bonus ${this.formatInlineNumber(WELCOME_BONUS_HTG, 0)} HTG`
            : (step.buttonText || 'Soumettre ma demande'))
          : (step.buttonText || 'Continuer');
      }
      alert(getPaymentFriendlyErrorMessage(error));
    }
  }
  
  validateFormStep() {
    const form = this.modal.querySelector('#clientForm');
    if (!form) return false;
    
    const inputs = form.querySelectorAll('input, textarea, select');
    let isValid = true;
    let firstInvalid = null;
    
    inputs.forEach(input => {
      if (input.hasAttribute('required') && !input.value.trim()) {
        input.style.borderColor = '#7F1D1D';
        isValid = false;
        if (!firstInvalid) firstInvalid = input;
      } else {
        input.style.borderColor = 'rgba(198,167,94,0.3)';
      }
    });
    
    if (!isValid && firstInvalid) {
      firstInvalid.focus();
      alert('Veuillez remplir tous les champs obligatoires');
      return false;
    }
    
    if (isValid) {
      inputs.forEach(input => {
        if (input.type === 'checkbox') {
          this.clientData[input.name] = input.checked;
        } else {
          this.clientData[input.name] = input.value.trim();
        }
      });
    }
    
    return isValid;
  }

  async extractTextFromProofImage(imageFile) {
    if (!imageFile) return '';
    const tesseract = await loadTesseractRuntime();
    const result = await tesseract.recognize(imageFile, OCR_LANGUAGE, { logger: () => {} });
    const raw = String(result?.data?.text || '');
    return raw.replace(/[ \t]+\n/g, '\n').trim();
  }
  
  async validateProofStep() {
    this.proofSubmitAttemptDurationMs = this.getProofStepDurationMs();
    console.info('[DEPOSIT_GUARD_DEBUG][PAYMENT] proof-submit', {
      uid: this.getClientUid(),
      durationMs: this.proofSubmitAttemptDurationMs,
      proofMode: this.proofMode,
      welcomeSelected: this.isWelcomeBonusSelected(),
    });

    const proofName = this.modal.querySelector('#proofName')?.value.trim();
    const proofDepositorPhoneInput = this.modal.querySelector('#proofDepositorPhone');
    const proofDepositorPhone = sanitizePhoneInput(proofDepositorPhoneInput?.value || '');
    const proofImage = this.modal.querySelector('#proofImage')?.files[0];
    
    if (!proofName) {
      alert('Veuillez confirmer votre nom');
      return false;
    }
    
    const expectedName = this.clientData.fullName || this.clientData.name || this.options.client?.name || '';
    if (expectedName && proofName !== expectedName) {
      alert(`Le nom "${proofName}" ne correspond pas à "${expectedName}". Veuillez saisir le même nom.`);
      return false;
    }

    const depositorPhoneDigits = extractPhoneDigits(proofDepositorPhone);
    if (!proofDepositorPhone || depositorPhoneDigits.length < 8) {
      alert('Veuillez saisir le numero exact qui a effectue le depot.');
      proofDepositorPhoneInput?.focus();
      return false;
    }
    
    if (!proofImage && !this.proofImageFile) {
      alert('Veuillez sélectionner une image');
      return false;
    }

    if (this.shouldPromptRapidDepositWarning()) {
      const confirmedRapidSubmission = await this.confirmRapidDepositSubmission();
      if (!confirmedRapidSubmission) {
        return false;
      }
    }
    
    const imageFile = this.proofImageFile || proofImage;
    this.extractedText = '';
    this.extractedTextStatus = 'pending';

    try {
      this.extractedText = await this.extractTextFromProofImage(imageFile);
      this.extractedTextStatus = this.extractedText ? 'success' : 'empty';
    } catch (ocrError) {
      console.error('❌ Erreur OCR:', ocrError);
      this.extractedText = '';
      this.extractedTextStatus = 'failed';
    }

    this.clientData.depositorPhone = proofDepositorPhone;

    if (this.isWelcomeBonusSelected()) {
      await this.saveWelcomeBonusClaim(proofName);
    } else {
      await this.saveOrder(proofName);
    }
    
    return true;
  }

  async saveWelcomeBonusClaim(proofName) {
    const customerName = this.clientData.fullName || this.clientData.name || this.options.client?.name || '';
    const customerPhone = this.clientData.phone || this.options.client?.phone || '';
    const depositorPhone = this.clientData.depositorPhone || '';

    const response = await claimWelcomeBonusSecure({
      customerName,
      customerPhone,
      depositorPhone,
      proofRef: proofName,
      methodId: this.selectedMethod?.id || 'welcome_bonus',
    });
    this.completedFlowType = 'welcome_bonus';
    this.confirmationMessage = String(
      response?.message
      || `Ton bonus de bienvenue de ${WELCOME_BONUS_HTG} HTG a ete active avec succes.`
    ).trim();
    await this.loadFundingStatus();

    try {
      const { ensureXchangeState } = await import('./xchange.js');
      await ensureXchangeState(this.options.client?.uid || this.options.client?.id || '');
    } catch (error) {
      console.warn('⚠️ Impossible de rafraichir l état Xchange après bonus:', error);
    }

    const eventDetail = {
      uid: this.options.client?.uid || this.options.client?.id || '',
      welcomeBonusHtgGranted: Number(response?.welcomeBonusHtgGranted) || WELCOME_BONUS_HTG,
    };
    document.dispatchEvent(new CustomEvent('welcomeBonusClaimed', {
      detail: eventDetail
    }));
    window.dispatchEvent(new CustomEvent('welcomeBonusClaimed', {
      detail: {
        ...eventDetail,
      }
    }));

    if (this.options.onSuccess) {
      this.options.onSuccess({
        type: 'welcome_bonus',
        welcomeBonusHtgGranted: Number(response?.welcomeBonusHtgGranted) || WELCOME_BONUS_HTG,
      });
    }

    return true;
  }
  
  async saveOrder(proofName) {
    try {
      if (!this.options.client || !this.options.client.id) {
        console.error('❌ Client non disponible');
        return false;
      }

      const normalizedItems = Array.isArray(this.options.cart)
        ? this.options.cart.map((item) => {
            const quantity = Number(item?.quantity) || 1;
            const price = Number(item?.price) || 0;
            return {
              productId: item?.productId || '',
              name: item?.name || 'Produit',
              price,
              quantity,
              sku: item?.sku || '',
              image: item?.image || '',
              selectedOptions: Array.isArray(item?.selectedOptions) ? item.selectedOptions : []
            };
          })
        : [];
      const computedAmount = normalizedItems.reduce((sum, item) => sum + (item.price * item.quantity), 0);
      const finalAmount = Number(this.options.amount) || computedAmount;
      
      const uniqueCode = 'VLX-' + Math.random().toString(36).substr(2, 8).toUpperCase() + '-' + Date.now().toString(36).toUpperCase();
      
      const orderData = {
        amount: finalAmount,
        clientId: this.options.client?.id || '',
        clientUid: this.options.client?.uid || '',
        methodId: this.selectedMethod?.id,
        methodName: this.selectedMethod?.name,
        methodDetails: {
          name: this.selectedMethod?.name,
          accountName: this.selectedMethod?.accountName,
          phoneNumber: this.selectedMethod?.phoneNumber
        },
        delivery: this.options.delivery || null,
        shippingAmount: Number(this.options.delivery?.totalFee || 0),
        weightFee: Number(this.options.delivery?.weightFee || 0),
        items: normalizedItems,
        status: 'pending',
        uniqueCode: uniqueCode,
        extractedText: this.extractedText,
        extractedTextStatus: this.extractedTextStatus,
        extractedTextAt: new Date().toISOString(),
        proofName: proofName,
        clientData: this.clientData,
        customerName: this.clientData.fullName || this.clientData.name || this.options.client?.name || '',
        customerEmail: this.clientData.email || this.options.client?.email || '',
        customerPhone: this.clientData.phone || this.options.client?.phone || '',
        depositorPhone: this.clientData.depositorPhone || '',
        customerAddress: this.clientData.address || this.options.client?.address || '',
        customerCity: this.clientData.city || this.options.client?.city || '',
        createdAt: new Date().toISOString(),
        expiresAt: new Date(Date.now() + ((this.settings.verificationHours || 12) * 60 * 60 * 1000)).toISOString()
      };

      const response = await createOrderSecure({
        methodId: this.selectedMethod?.id || '',
        amountHtg: finalAmount,
        customerName: orderData.customerName,
        customerEmail: orderData.customerEmail,
        customerPhone: orderData.customerPhone,
        depositorPhone: orderData.depositorPhone,
        proofRef: proofName,
        extractedText: this.extractedText,
        extractedTextStatus: this.extractedTextStatus,
        proofStepDurationMs: this.getProofStepDurationMs(),
      });
      console.info('[DEPOSIT_GUARD_DEBUG][PAYMENT] create-order:response', {
        uid: this.getClientUid(),
        orderId: String(response?.orderId || ''),
        status: String(response?.status || ''),
        creditedProvisionally: response?.creditedProvisionally === true,
        message: String(response?.message || ''),
      });
      this.completedFlowType = 'deposit';
      this.confirmationMessage = String(response?.message || "").trim();
      const orderId = response?.orderId || '';
      
      document.dispatchEvent(new CustomEvent('orderSaved', {
        detail: { id: orderId, clientId: this.options.client.id, order: orderData }
      }));
      
      if (this.options.onSuccess) {
        this.options.onSuccess({ id: orderId, ...orderData });
      }
      
      return true;
    } catch (error) {
      console.error('❌ Erreur sauvegarde commande:', error);
      throw error;
    }
  }
  
  updateStepDisplay() {
    const header = this.modal.querySelector('.payment-container-' + this.uniqueId + ' > div:first-child');
    if (header) {
      const titleDiv = header.querySelector('div:first-child');
      if (titleDiv) {
        titleDiv.innerHTML = `
          <div style="display: flex; align-items: center; gap: 1rem;">
            ${this.currentStep > 0 && this.currentStep < (this.steps?.length || 0) && !this.isSubmitted ? `
              <button class="back-step payment-icon-btn" style="
                background: none;
                border: none;
                font-size: 1.2rem;
                cursor: pointer;
                color: #8B7E6B;
                padding: 0.5rem;
                width: 40px;
                height: 40px;
                display: flex;
                align-items: center;
                justify-content: center;
                border-radius: 50%;
                transition: all 0.2s;
              ">
                <i class="fas fa-arrow-left"></i>
              </button>
            ` : ''}
            <h2 style="
              font-family: 'Cormorant Garamond', serif;
              font-size: 1.5rem;
              color: #1F1E1C;
              margin: 0;
            ">
              Paiement sécurisé
            </h2>
          </div>
          <button class="close-payment payment-icon-btn" style="
            background: none;
            border: none;
            font-size: 1.5rem;
            cursor: pointer;
            color: #8B7E6B;
            transition: all 0.2s;
            padding: 0.5rem;
            width: 40px;
            height: 40px;
            display: flex;
            align-items: center;
            justify-content: center;
            border-radius: 50%;
          ">
            <i class="fas fa-times"></i>
          </button>
        `;
      }
      
      const oldProgress = header.querySelector('div[style*="margin-top: 0.5rem"]');
      if (oldProgress) {
        oldProgress.remove();
      }
      
      if (this.currentStep < (this.steps?.length || 0) && !this.isSubmitted) {
        const newProgress = document.createElement('div');
        newProgress.innerHTML = this.renderProgressBar();
        header.appendChild(newProgress.firstChild);
      }
    }
    
    const content = this.modal.querySelector('.payment-container-' + this.uniqueId + ' > div:nth-child(2)');
    if (content) {
      content.innerHTML = this.renderCurrentStep();
    }
    
    this.attachEvents();
  }
  
  startCountdown() {
    this.stopCountdown();
    const hours = this.settings.verificationHours || 12;
    this.timeLeft = hours * 60 * 60;
    
    const updateTimer = () => {
      if (this.timeLeft <= 0) {
        clearInterval(this.countdownInterval);
        const timer = this.modal.querySelector('#countdownTimer');
        if (timer) {
          timer.textContent = 'Expiré';
          timer.style.color = '#7F1D1D';
        }
        return;
      }
      
      const h = Math.floor(this.timeLeft / 3600);
      const m = Math.floor((this.timeLeft % 3600) / 60);
      const s = this.timeLeft % 60;
      
      const timer = this.modal.querySelector('#countdownTimer');
      if (timer) {
        timer.textContent = `${h.toString().padStart(2, '0')}:${m.toString().padStart(2, '0')}:${s.toString().padStart(2, '0')}`;
      }
      
      this.timeLeft--;
    };
    
    updateTimer();
    this.countdownInterval = setInterval(updateTimer, 1000);
  }

  stopCountdown() {
    if (this.countdownInterval) {
      clearInterval(this.countdownInterval);
      this.countdownInterval = null;
    }
  }
  
  animateIn() {
    setTimeout(() => {
      this.modal.style.opacity = '1';
    }, 50);
  }
  
  animateOut() {
    return new Promise(resolve => {
      this.modal.style.opacity = '0';
      const container = this.modal.querySelector('.payment-container-' + this.uniqueId);
      if (container) {
        container.style.transform = 'scale(0.95)';
      }
      setTimeout(resolve, 300);
    });
  }
  
  async close() {
    this.stopCountdown();
    this.clearProofStepStartedAtMs();
    
    await this.animateOut();
    this.modal.remove();
    document.body.style.overflow = '';
    
    if (this.options.onClose) {
      this.options.onClose();
    }
  }
}

export default PaymentModal;
