import { playUiSound } from './ui-sound.js';

class PrevisualisationCV extends HTMLElement {
    constructor() {
        super();
        this.initialized = false;
        this.isDownloading = false;
        this.pagesRenderToken = null;
        this.pagesRenderTimer = null;
        this.fastRenderDelayMs = 260;
        this.softRenderDelayMs = 420;
        this.moveRenderDelayMs = 120;
        this.data = {};
        this.typographyOptions = {
            poppins: {
                label: 'Poppins',
                fontFamily: '"Poppins", sans-serif',
                stylesheetHref: 'https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600;700;800&display=swap'
            },
            inter: {
                label: 'Inter',
                fontFamily: '"Inter", sans-serif',
                stylesheetHref: 'https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap'
            },
            montserrat: {
                label: 'Montserrat',
                fontFamily: '"Montserrat", sans-serif',
                stylesheetHref: 'https://fonts.googleapis.com/css2?family=Montserrat:wght@300;400;500;600;700;800&display=swap'
            },
            nunito: {
                label: 'Nunito Sans',
                fontFamily: '"Nunito Sans", sans-serif',
                stylesheetHref: 'https://fonts.googleapis.com/css2?family=Nunito+Sans:wght@300;400;500;600;700;800&display=swap'
            },
            dmSans: {
                label: 'DM Sans',
                fontFamily: '"DM Sans", sans-serif',
                stylesheetHref: 'https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700;800&display=swap'
            },
            sourceSans: {
                label: 'Source Sans 3',
                fontFamily: '"Source Sans 3", sans-serif',
                stylesheetHref: 'https://fonts.googleapis.com/css2?family=Source+Sans+3:wght@300;400;500;600;700;800&display=swap'
            },
            plusJakarta: {
                label: 'Plus Jakarta Sans',
                fontFamily: '"Plus Jakarta Sans", sans-serif',
                stylesheetHref: 'https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&display=swap'
            },
            raleway: {
                label: 'Raleway',
                fontFamily: '"Raleway", sans-serif',
                stylesheetHref: 'https://fonts.googleapis.com/css2?family=Raleway:wght@300;400;500;600;700;800&display=swap'
            },
            lora: {
                label: 'Lora',
                fontFamily: '"Lora", serif',
                stylesheetHref: 'https://fonts.googleapis.com/css2?family=Lora:wght@400;500;600;700&display=swap'
            },
            merriweather: {
                label: 'Merriweather',
                fontFamily: '"Merriweather", serif',
                stylesheetHref: 'https://fonts.googleapis.com/css2?family=Merriweather:wght@300;400;700&display=swap'
            },
            playfair: {
                label: 'Playfair Display',
                fontFamily: '"Playfair Display", serif',
                stylesheetHref: 'https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;500;600;700&display=swap'
            },
            robotoSlab: {
                label: 'Roboto Slab',
                fontFamily: '"Roboto Slab", serif',
                stylesheetHref: 'https://fonts.googleapis.com/css2?family=Roboto+Slab:wght@300;400;500;600;700&display=swap'
            }
        };
        this.typographyKey = 'poppins';
        this.leftColumnColor = '#2f3e4e';
        this.leftColumnWidth = 30;
        this.minLeftColumnWidth = 22;
        this.maxLeftColumnWidth = 45;
        this.photoShape = 'rect';
        this.titleColor = '#1f2937';
        this.subtitleColor = '#555555';
        this.fontScale = 1;
        this.fontScaleStep = 0.05;
        this.minFontScale = 0.8;
        this.maxFontScale = 1.35;
        this.lineHeightScale = 1;
        this.minLineHeightScale = 0.8;
        this.maxLineHeightScale = 1.4;
        this.elementStyleMap = {};
        this.activeStyleTargetKey = '';
        this.styleEditorVisible = false;
        this.moveModeEnabled = false;
        this.moveStepPx = 6;
        this.maxMoveOffsetPx = 600;
        this.topbarScrollContainer = null;
        this.topbarScrollHandler = null;
        this.topbarLastScrollTop = 0;
        this.topbarHidden = false;
        this.textOverrideMap = {};
        this.inlineEditorState = null;
        this.busyOverlayRef = null;
        this.busyIndicatorTimer = null;
        this.busyTokens = new Set();
        this.defaultData = {
            firstname: 'Alexandre-Jean-Baptiste',
            middlename: 'Maximilien',
            lastname: 'Martin-De-La-Rochefoucauld',
            designation: 'Responsable Transformation Digitale et Architecte Frontend Senior',
            email: 'alexandre.jean.baptiste.martin.rochefoucauld.super.long@email-exemple-professionnel.com',
            phoneno: '+33 6 12 34 56 78 / +33 7 98 76 54 32',
            address: '12 bis Rue des Tres Longs Noms de Quartier Historique, Batiment C Escalier 4 Appartement 27, 75008 Paris, France',
            address_number: '12',
            address_street: 'Rue des Tres Longs Noms de Quartier Historique, Batiment C Escalier 4 Appartement 27',
            address_postal: '75008',
            address_city: 'Paris Centre Rive Droite',
            address_country: 'France',
            summary: 'Professionnel polyvalent specialise dans la conception de produits numeriques, la coordination de projets transverses et l optimisation continue de l experience utilisateur. Habitude de collaborer avec des equipes pluridisciplinaires dans des environnements exigeants, avec un haut niveau de qualite, de rigueur et de communication. Oriente resultats mesurables, performance applicative et coherence design a grande echelle. Experience solide en accompagnement d equipes, gouvernance des priorites, redaction de contenus clairs et standardisation des pratiques de livraison.',
            exp_title: 'Lead Frontend Engineer et Coordinateur Qualite Produit',
            exp_organization: 'Digital Experience Innovation Studio International',
            exp_location: 'Paris, Ile-de-France',
            exp_start_date: '2022-01-01',
            exp_end_date: '2025-01-01',
            exp_description: 'Pilotage de la migration d une base applicative legacy vers une architecture moderne basee sur des composants reutilisables, accompagnement des equipes sur les bonnes pratiques de qualite et reduction significative des regressions en production.\nMise en place d une strategie UI systemique avec documentation detaillee, normalisation des patterns d interface et amelioration sensible de la maintenabilite globale.\nCollaboration continue avec produit, design, QA et direction technique pour accelerer les cycles de livraison sans degrader l experience utilisateur.\nStructuration d un cadre de revue de contenu RH afin de renforcer la qualite des profils et clarifier les messages cles dans chaque rubrique du CV.',
            experiences: [
                {
                    title: 'Lead Frontend Engineer et Coordinateur Qualite Produit',
                    organization: 'Digital Experience Innovation Studio International',
                    location: 'Paris, Ile-de-France',
                    start_date: '2022-01-01',
                    end_date: '2025-01-01',
                    description: 'Pilotage de la migration d une base applicative legacy vers une architecture moderne basee sur des composants reutilisables, accompagnement des equipes sur les bonnes pratiques de qualite et reduction significative des regressions en production.\nMise en place d une strategie UI systemique avec documentation detaillee, normalisation des patterns d interface et amelioration sensible de la maintenabilite globale.\nCollaboration continue avec produit, design, QA et direction technique pour accelerer les cycles de livraison sans degrader l experience utilisateur.'
                },
                {
                    title: 'Responsable Experience Produit et Parcours Utilisateur',
                    organization: 'Cabinet Conseil Transformation Metiers',
                    location: 'Lyon, Auvergne-Rhone-Alpes',
                    start_date: '2019-03-01',
                    end_date: '2021-12-31',
                    description: 'Coordination de projets d harmonisation des processus de candidature, cadrage des attentes metier et alignement des livrables avec les standards RH.\nAnimation d ateliers de relecture CV avec experts recrutement pour formuler des recommandations directement exploitables dans chaque champ.\nMise en place d indicateurs de comprehension utilisateur et ajustement iteratif des interfaces de saisie.'
                },
                {
                    title: 'Chef de Projet Contenu Professionnel',
                    organization: 'Agence Editoriale Carriere et Emploi',
                    location: 'Bordeaux, Nouvelle-Aquitaine',
                    start_date: '2016-02-01',
                    end_date: '2019-02-28',
                    description: 'Conception de guides pratiques pour rediger des experiences, des resumes et des competences avec un niveau de precision adapte au marche.\nAccompagnement de profils juniors et seniors pour structurer des candidatures lisibles, credibles et coherentes avec leurs objectifs.\nSupervision d un cycle complet de qualite contenu, de la collecte d informations a la validation finale.'
                },
                {
                    title: 'Consultant Redaction et Positionnement Profil',
                    organization: 'Centre de Services Carriere Professionnelle',
                    location: 'Marseille, Provence-Alpes-Cote d Azur',
                    start_date: '2013-01-01',
                    end_date: '2016-01-31',
                    description: 'Accompagnement individuel de candidats pour clarifier leurs experiences et renforcer l impact de leurs candidatures sur des postes cibles.\nCreation de trames de presentation pour harmoniser les rubriques et faciliter la lecture par les recruteurs.\nFormation des equipes internes sur les bonnes pratiques de redaction professionnelle et sur les erreurs frequentes a corriger.'
                }
            ],
            educations: [
                {
                    start_date: '2018-09-01',
                    end_date: '2020-06-01',
                    degree: 'Master Informatique et Ingenierie des Systemes Distribues',
                    school: 'Universite Internationale des Sciences Numeriques et de l Innovation',
                    school_address: '15 Rue des Ecoles et de la Recherche Technologique, 75005 Paris, France'
                },
                {
                    start_date: '2015-09-01',
                    end_date: '2017-06-01',
                    degree: 'Master Management de Projet et Conduite du Changement',
                    school: 'Ecole Superieure de Management Operationnel',
                    school_address: '22 Avenue des Organisations, 69000 Lyon, France'
                },
                {
                    start_date: '2012-09-01',
                    end_date: '2015-06-01',
                    degree: 'Licence Information Communication et Strategies Editoriales',
                    school: 'Faculte Lettres et Sciences Humaines',
                    school_address: '9 Boulevard Universitaire, 33000 Bordeaux, France'
                }
            ],
            phones: ['+33 6 12 34 56 78', '+33 7 12 98 44 10'],
            languages: [
                { name: 'Francais Professionnel et Redaction Avancee', level: 'Tres bien' },
                { name: 'Anglais Technique et Communication Internationale', level: 'Bien' },
                { name: 'Espagnol Professionnel', level: 'Intermediaire' },
                { name: 'Allemand Professionnel', level: 'Intermediaire' },
                { name: 'Italien Conversationnel', level: 'Notions avancees' },
                { name: 'Portugais Ecrit Professionnel', level: 'Notions' }
            ],
            tools: [
                { type: 'Logiciel', name: 'Figma Design System Enterprise', level: 'Expert' },
                { type: 'Logiciel', name: 'Jira Advanced Workflow Management', level: 'Intermediaire' },
                { type: 'Logiciel', name: 'Notion Documentation Collaborative', level: 'Expert' },
                { type: 'Logiciel', name: 'Confluence Knowledge Base', level: 'Intermediaire' }
            ],
            softwares: 'Figma Design System Enterprise, Jira Advanced Workflow Management, VS Code Workspace Automation, GitHub Enterprise, Notion Documentation Collaborative, Confluence Knowledge Base, Trello Pilotage Editorial, Miro Ateliers Co-conception',
            interests: [
                'Design d interfaces numeriques a haute accessibilite',
                'Veille technologique frontend et architecture web moderne',
                'Photographie urbaine et composition visuelle professionnelle',
                'Mentorat de profils juniors en redaction de CV',
                'Analyse des tendances recrutement et adaptation des candidatures',
                'Optimisation de la lisibilite des contenus professionnels',
                'Communication ecrite orientee resultats et impact',
                'Ateliers de co-construction de parcours carriere',
                'Documentation et normalisation des pratiques RH',
                'Accompagnement des transitions de poste et reconversion'
            ],
            image: 'assets/images/profile-image.jpg'
        };
    }

    async connectedCallback() {
        if (this.initialized) return;
        this.initialized = true;

        await this.ensureDependencies();
        this.render();
        this.bindEvents();
        this.setupTopbarAutoHide();
        await this.ensureTypographyStylesheet(this.typographyKey);
        this.updatePreview();
    }

    disconnectedCallback() {
        this.closeInlineTextEditor({ applyChanges: false });
        this.teardownTopbarAutoHide();
        this.stopBusyIndicator();
    }

    setupTopbarAutoHide() {
        this.teardownTopbarAutoHide();
        const root = this.querySelector('.pv-root');
        const topbar = this.querySelector('.pv-topbar');
        if (!root || !topbar) return;

        this.topbarScrollContainer = root;
        this.topbarLastScrollTop = Number(root.scrollTop || 0);
        this.topbarHidden = false;
        topbar.classList.remove('pv-topbar-hidden');

        const hideThreshold = 8;
        const revealAtTop = 18;
        this.topbarScrollHandler = () => {
            const currentTop = Number(root.scrollTop || 0);
            const delta = currentTop - this.topbarLastScrollTop;
            this.topbarLastScrollTop = currentTop;

            if (currentTop <= revealAtTop) {
                if (this.topbarHidden) {
                    this.topbarHidden = false;
                    topbar.classList.remove('pv-topbar-hidden');
                }
                return;
            }

            if (delta > hideThreshold && !this.topbarHidden) {
                this.topbarHidden = true;
                topbar.classList.add('pv-topbar-hidden');
                return;
            }

            if (delta < -hideThreshold && this.topbarHidden) {
                this.topbarHidden = false;
                topbar.classList.remove('pv-topbar-hidden');
            }
        };
        root.addEventListener('scroll', this.topbarScrollHandler, { passive: true });
    }

    teardownTopbarAutoHide() {
        if (this.topbarScrollContainer && this.topbarScrollHandler) {
            this.topbarScrollContainer.removeEventListener('scroll', this.topbarScrollHandler);
        }
        this.topbarScrollContainer = null;
        this.topbarScrollHandler = null;
        this.topbarLastScrollTop = 0;
        this.topbarHidden = false;
        const topbar = this.querySelector('.pv-topbar');
        if (topbar) {
            topbar.classList.remove('pv-topbar-hidden');
        }
    }

    ensureBusyOverlay() {
        if (this.busyOverlayRef && this.busyOverlayRef.isConnected) {
            return this.busyOverlayRef;
        }
        const overlay = document.createElement('div');
        overlay.className = 'pointer-events-none fixed inset-0 z-[5600] hidden items-center justify-center bg-slate-900/20 backdrop-blur-[1px]';
        overlay.setAttribute('data-pv-busy-overlay', '1');
        overlay.innerHTML = `
            <div class="flex min-w-[210px] items-center gap-3 rounded-xl border border-slate-200 bg-white px-4 py-3 shadow-xl">
                <span class="inline-block h-5 w-5 animate-spin rounded-full border-2 border-slate-300 border-t-sky-600"></span>
                <p class="text-xs font-semibold text-slate-700" data-pv-busy-message>Mise a jour...</p>
            </div>
        `;
        this.appendChild(overlay);
        this.busyOverlayRef = overlay;
        return overlay;
    }

    startBusyIndicator(message = 'Mise a jour...', delayMs = 220) {
        const token = Symbol('pv-busy');
        this.busyTokens.add(token);
        const overlay = this.ensureBusyOverlay();
        const messageEl = overlay.querySelector('[data-pv-busy-message]');
        if (messageEl && message) {
            messageEl.textContent = String(message);
        }
        if (!this.busyIndicatorTimer && overlay.classList.contains('hidden')) {
            this.busyIndicatorTimer = window.setTimeout(() => {
                this.busyIndicatorTimer = null;
                if (!this.busyTokens.size) return;
                overlay.classList.remove('hidden');
                overlay.classList.add('flex', 'pointer-events-auto');
                overlay.classList.remove('pointer-events-none');
            }, Math.max(0, Math.floor(Number(delayMs) || 0)));
        }
        return token;
    }

    stopBusyIndicator(token = null) {
        if (token) {
            this.busyTokens.delete(token);
        } else {
            this.busyTokens.clear();
        }
        if (this.busyTokens.size) return;

        if (this.busyIndicatorTimer) {
            clearTimeout(this.busyIndicatorTimer);
            this.busyIndicatorTimer = null;
        }
        const overlay = this.busyOverlayRef;
        if (!overlay) return;
        overlay.classList.add('hidden', 'pointer-events-none');
        overlay.classList.remove('flex', 'pointer-events-auto');
    }

    async ensureDependencies() {
        await Promise.all([
            this.loadScript('https://cdn.tailwindcss.com', 'cvb-tailwind'),
            this.loadStylesheet('https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.2/css/all.min.css', 'cvb-fa'),
            this.loadStylesheet('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600&display=swap', 'cvb-poppins'),
            this.loadScript('https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js', 'cvb-html2canvas'),
            this.loadScript('https://cdnjs.cloudflare.com/ajax/libs/jspdf/2.5.1/jspdf.umd.min.js', 'cvb-jspdf')
        ]);
    }

    loadScript(src, marker) {
        if (document.querySelector(`script[data-${marker}]`)) return Promise.resolve();
        return new Promise((resolve, reject) => {
            const script = document.createElement('script');
            script.src = src;
            script.setAttribute(`data-${marker}`, 'true');
            script.onload = resolve;
            script.onerror = reject;
            document.head.appendChild(script);
        });
    }

    loadStylesheet(href, marker) {
        if (document.querySelector(`link[data-${marker}]`)) return Promise.resolve();
        return new Promise((resolve, reject) => {
            const link = document.createElement('link');
            link.rel = 'stylesheet';
            link.href = href;
            link.setAttribute(`data-${marker}`, 'true');
            link.onload = resolve;
            link.onerror = reject;
            document.head.appendChild(link);
        });
    }

    getTypographyConfig(key) {
        const fallback = this.typographyOptions.poppins;
        if (!key || !Object.prototype.hasOwnProperty.call(this.typographyOptions, key)) {
            return fallback;
        }
        return this.typographyOptions[key] || fallback;
    }

    async ensureTypographyStylesheet(key) {
        const config = this.getTypographyConfig(key);
        if (!config || !config.stylesheetHref) return;
        try {
            const marker = key === 'poppins' ? 'cvb-poppins' : `cvb-font-${key}`;
            await this.loadStylesheet(config.stylesheetHref, marker);
        } catch (error) {
            console.warn('Impossible de charger la police:', key, error);
        }
    }

    getTypographyOptionsMarkup() {
        return Object.entries(this.typographyOptions)
            .map(([key, option]) => {
                const selected = key === this.typographyKey ? ' selected' : '';
                return `<option value="${key}"${selected}>${option.label}</option>`;
            })
            .join('');
    }

    render() {
        this.innerHTML = `
            <style>
                .pv-root {
                    background:
                        radial-gradient(circle at 12% 10%, rgba(56, 189, 248, 0.16), transparent 36%),
                        radial-gradient(circle at 86% 8%, rgba(148, 163, 184, 0.2), transparent 40%),
                        linear-gradient(165deg, #0f172a 0%, #111827 45%, #0b1220 100%);
                }
                .pv-card {
                    position: relative;
                    padding-top: 156px;
                    padding-bottom: 18px;
                }
                .pv-topbar {
                    position: fixed;
                    top: 0.75rem;
                    left: 50%;
                    width: min(1240px, calc(100vw - 1.5rem));
                    transform: translateX(-50%);
                    border-radius: 20px;
                    border: 1px solid rgba(226, 232, 240, 0.22);
                    background: linear-gradient(135deg, rgba(15, 23, 42, 0.82), rgba(30, 41, 59, 0.66));
                    box-shadow: 16px 16px 38px rgba(2, 6, 23, 0.62), -12px -12px 34px rgba(30, 41, 59, 0.34);
                    backdrop-filter: blur(16px);
                    z-index: 90;
                    transition: transform 0.24s ease, opacity 0.2s ease;
                    will-change: transform, opacity;
                }
                .pv-topbar.pv-topbar-hidden {
                    transform: translate(-50%, -130%);
                    opacity: 0;
                    pointer-events: none;
                }
                .pv-soft-panel {
                    border: 1px solid rgba(148, 163, 184, 0.36);
                    background: linear-gradient(145deg, rgba(30, 41, 59, 0.58), rgba(15, 23, 42, 0.7));
                    border-radius: 14px;
                    box-shadow: inset 4px 4px 10px rgba(15, 23, 42, 0.62), inset -4px -4px 10px rgba(71, 85, 105, 0.3);
                }
                .pv-topbar label,
                .pv-topbar .pv-title-sm,
                .pv-topbar .pv-title-lg {
                    color: #e2e8f0;
                }
                .pv-topbar select,
                .pv-topbar input[type="text"],
                .pv-topbar input[type="number"],
                .pv-topbar input[type="color"] {
                    border: 1px solid rgba(148, 163, 184, 0.45);
                    background: linear-gradient(145deg, rgba(15, 23, 42, 0.84), rgba(30, 41, 59, 0.72));
                    color: #e2e8f0;
                    box-shadow: inset 2px 2px 6px rgba(15, 23, 42, 0.58), inset -2px -2px 6px rgba(71, 85, 105, 0.25);
                }
                .pv-topbar select option {
                    background: #0f172a;
                    color: #e2e8f0;
                }
                .pv-topbar [data-style-target-label] {
                    color: #bae6fd;
                }
                .pv-topbar button:not(.pv-action-btn) {
                    border: 1px solid rgba(148, 163, 184, 0.45);
                    background: linear-gradient(145deg, rgba(15, 23, 42, 0.84), rgba(30, 41, 59, 0.72));
                    color: #e2e8f0;
                    box-shadow: inset 2px 2px 6px rgba(15, 23, 42, 0.58), inset -2px -2px 6px rgba(71, 85, 105, 0.25);
                }
                .pv-topbar button:not(.pv-action-btn):hover {
                    filter: brightness(1.08);
                }
                .pv-topbar button:disabled {
                    opacity: 0.5;
                    cursor: not-allowed;
                }
                .pv-topbar input[type="range"] {
                    accent-color: #38bdf8;
                }
                .pv-action-btn {
                    border: 1px solid rgba(148, 163, 184, 0.42) !important;
                    background: linear-gradient(145deg, rgba(15, 23, 42, 0.7), rgba(30, 41, 59, 0.7)) !important;
                    color: #e2e8f0 !important;
                    border-radius: 14px !important;
                    box-shadow: 7px 7px 18px rgba(2, 6, 23, 0.5), -6px -6px 14px rgba(71, 85, 105, 0.24) !important;
                }
                .pv-action-btn:hover {
                    filter: brightness(1.08);
                }
                .pv-action-btn[data-action="download-pdf"] {
                    border-color: rgba(14, 165, 233, 0.62) !important;
                    background: linear-gradient(135deg, #0284c7, #0ea5e9) !important;
                    color: #e0f2fe !important;
                }
                .pv-page-shell {
                    border-radius: 16px;
                    border: 1px solid rgba(203, 213, 225, 0.52);
                    background: rgba(255, 255, 255, 0.66);
                    box-shadow: 12px 12px 26px rgba(15, 23, 42, 0.2), -8px -8px 20px rgba(255, 255, 255, 0.72);
                    backdrop-filter: blur(8px);
                    padding: 7px;
                }
                .pv-page-label {
                    color: #cbd5e1;
                }
                .pv-page-frame {
                    position: relative;
                    overflow: hidden;
                    border-radius: 12px;
                    background: #ffffff;
                    border: 1px solid rgba(148, 163, 184, 0.55);
                    box-shadow: inset 2px 2px 8px rgba(241, 245, 249, 0.92), inset -2px -2px 8px rgba(203, 213, 225, 0.72);
                }
                .pv-hotspot {
                    transition: background-color 120ms ease, box-shadow 120ms ease, transform 70ms linear;
                    border-radius: 3px;
                }
                .pv-hotspot:hover {
                    background: rgba(125, 211, 252, 0.22);
                }
                .pv-hotspot.is-active {
                    background: rgba(56, 189, 248, 0.22);
                    box-shadow: inset 0 0 0 2px rgba(14, 165, 233, 0.85);
                }
                .pv-hotspot.is-draggable {
                    cursor: grab;
                    touch-action: none;
                }
                .pv-hotspot.is-dragging {
                    cursor: grabbing;
                }
                .pv-last-resize-handle {
                    position: absolute;
                    left: 50%;
                    bottom: 8px;
                    transform: translateX(-50%);
                    width: min(58%, 260px);
                    height: 16px;
                    border: 1px solid rgba(14, 165, 233, 0.42);
                    border-radius: 999px;
                    background: linear-gradient(135deg, rgba(186, 230, 253, 0.96), rgba(125, 211, 252, 0.96));
                    box-shadow: 0 8px 18px rgba(14, 116, 144, 0.28);
                    cursor: ns-resize;
                    z-index: 5;
                }
                .pv-last-resize-handle::before {
                    content: '';
                    position: absolute;
                    inset: 4px 36%;
                    border-radius: 999px;
                    background: rgba(2, 132, 199, 0.78);
                }
                .pv-last-resize-handle:hover {
                    filter: brightness(1.03);
                }
                .pv-drag-cursor {
                    cursor: grabbing !important;
                    user-select: none !important;
                }
                @media (max-width: 1024px) {
                    .pv-topbar {
                        top: 0.5rem;
                        width: min(1240px, calc(100vw - 1rem));
                    }
                    .pv-card {
                        padding-top: 198px;
                    }
                }
                @media (max-width: 640px) {
                    .pv-card {
                        padding-top: 232px;
                    }
                }
            </style>
            <section class="pv-root relative h-screen overflow-y-auto p-2 font-[Poppins] sm:p-3">
                <div class="pv-card mx-auto flex max-w-[1240px] flex-col gap-3">
                    <div class="pv-topbar flex flex-wrap items-center justify-end gap-2 p-2 sm:p-3">
                        <div class="pv-actions flex flex-wrap items-center justify-end gap-2">
                            <div class="pv-soft-panel flex flex-wrap items-center gap-2 px-2 py-1.5">
                                <label class="inline-flex items-center gap-2 text-[11px] font-semibold text-slate-700 sm:text-xs">
                                    Typo
                                    <select data-action="typography-select" class="h-8 rounded-md border border-slate-300 bg-white px-2 text-[11px] font-semibold text-slate-800 sm:text-xs">
                                        ${this.getTypographyOptionsMarkup()}
                                    </select>
                                </label>
                                <label class="inline-flex items-center gap-2 text-[11px] font-semibold text-slate-700 sm:text-xs">
                                    Colonne
                                    <input type="range" min="${this.minLeftColumnWidth}" max="${this.maxLeftColumnWidth}" step="1" value="${this.leftColumnWidth}" data-action="left-width-input" class="h-2 w-24 cursor-pointer accent-sky-700" aria-label="Largeur colonne gauche">
                                    <span class="min-w-[38px] text-right text-[11px] text-slate-700 sm:text-xs" data-left-col-width-label>${this.leftColumnWidth}%</span>
                                </label>
                                <label class="inline-flex items-center gap-2 text-[11px] font-semibold text-slate-700 sm:text-xs">
                                    Photo
                                    <select data-action="photo-shape-select" class="h-8 rounded-md border border-slate-300 bg-white px-2 text-[11px] font-semibold text-slate-800 sm:text-xs">
                                        <option value="rect"${this.photoShape === 'rect' ? ' selected' : ''}>Rectangle</option>
                                        <option value="circle"${this.photoShape === 'circle' ? ' selected' : ''}>Ronde</option>
                                    </select>
                                </label>
                                <label class="inline-flex items-center gap-2 text-[11px] font-semibold text-slate-700 sm:text-xs">
                                    Gauche
                                    <input type="color" value="${this.leftColumnColor}" data-action="left-color-input" class="h-8 w-9 cursor-pointer rounded border border-slate-300 bg-white p-0.5" aria-label="Couleur de la colonne gauche">
                                </label>
                            </div>
                            <div class="pv-soft-panel hidden max-w-full flex-wrap items-center gap-2 px-2 py-1.5" data-style-editor>
                                <p class="mr-2 whitespace-nowrap text-[11px] font-semibold text-sky-800 sm:text-xs">
                                    Bloc: <span data-style-target-label>Aucun</span>
                                </p>
                                <label class="inline-flex items-center gap-1 text-[11px] font-semibold text-slate-700 sm:text-xs">
                                    Texte
                                    <input type="color" data-style-control="textColor" class="h-8 w-9 cursor-pointer rounded border border-slate-300 bg-white p-0.5" value="#222222">
                                </label>
                                <label class="inline-flex items-center gap-1 text-[11px] font-semibold text-slate-700 sm:text-xs">
                                    Fond
                                    <input type="color" data-style-control="bgColor" class="h-8 w-9 cursor-pointer rounded border border-slate-300 bg-white p-0.5" value="#ffffff">
                                </label>
                                <label class="inline-flex items-center gap-1 text-[11px] font-semibold text-slate-700 sm:text-xs">
                                    Taille
                                    <input type="text" inputmode="decimal" data-style-control="fontSizePx" class="h-8 w-16 rounded border border-slate-300 bg-white px-2 text-[11px] font-semibold text-slate-800 sm:text-xs" placeholder="14">
                                </label>
                                <label class="inline-flex items-center gap-1 text-[11px] font-semibold text-slate-700 sm:text-xs">
                                    Interligne
                                    <input type="number" min="0.8" max="3" step="0.05" data-style-control="lineHeight" class="h-8 w-16 rounded border border-slate-300 bg-white px-2 text-[11px] font-semibold text-slate-800 sm:text-xs" placeholder="auto">
                                </label>
                                <label class="inline-flex items-center gap-1 text-[11px] font-semibold text-slate-700 sm:text-xs">
                                    Align
                                    <select data-style-control="textAlign" class="h-8 rounded border border-slate-300 bg-white px-2 text-[11px] font-semibold text-slate-800 sm:text-xs">
                                        <option value="">Auto</option>
                                        <option value="left">Left</option>
                                        <option value="center">Center</option>
                                        <option value="right">Right</option>
                                        <option value="justify">Justify</option>
                                    </select>
                                </label>
                                <label class="inline-flex items-center gap-1 text-[11px] font-semibold text-slate-700 sm:text-xs">
                                    Puces
                                    <select data-style-control="bulletStyle" class="h-8 rounded border border-slate-300 bg-white px-2 text-[11px] font-semibold text-slate-800 sm:text-xs">
                                        <option value="none">Aucune</option>
                                        <option value="disc">Disc</option>
                                        <option value="circle">Circle</option>
                                        <option value="square">Square</option>
                                        <option value="decimal">Decimal</option>
                                        <option value="lower-alpha">a,b,c</option>
                                        <option value="upper-alpha">A,B,C</option>
                                    </select>
                                </label>
                                <label class="inline-flex items-center gap-1 text-[11px] font-semibold text-slate-700 sm:text-xs">
                                    L
                                    <input type="number" min="20" max="900" step="1" data-style-control="widthPx" class="h-8 w-16 rounded border border-slate-300 bg-white px-2 text-[11px] font-semibold text-slate-800 sm:text-xs" placeholder="auto">
                                </label>
                                <label class="inline-flex items-center gap-1 text-[11px] font-semibold text-slate-700 sm:text-xs">
                                    H
                                    <input type="number" min="20" max="900" step="1" data-style-control="heightPx" class="h-8 w-16 rounded border border-slate-300 bg-white px-2 text-[11px] font-semibold text-slate-800 sm:text-xs" placeholder="auto">
                                </label>
                                <div class="inline-flex items-center gap-1">
                                    <button type="button" data-action="style-bold" class="inline-flex h-8 min-w-8 items-center justify-center rounded border border-slate-300 bg-white px-2 text-xs font-bold text-slate-900 hover:bg-slate-100">B</button>
                                    <button type="button" data-action="style-italic" class="inline-flex h-8 min-w-8 items-center justify-center rounded border border-slate-300 bg-white px-2 text-xs italic text-slate-900 hover:bg-slate-100">I</button>
                                    <button type="button" data-action="style-underline" class="inline-flex h-8 min-w-8 items-center justify-center rounded border border-slate-300 bg-white px-2 text-xs underline text-slate-900 hover:bg-slate-100">U</button>
                                </div>
                                <button type="button" data-action="style-reset" class="inline-flex h-8 items-center justify-center rounded border border-slate-300 bg-white px-2 text-[11px] font-semibold text-slate-800 hover:bg-slate-100 sm:text-xs">
                                    Reset bloc
                                </button>
                                <button type="button" data-action="style-open-inline-editor" class="inline-flex h-8 items-center justify-center rounded border border-slate-300 bg-white px-2 text-[11px] font-semibold text-slate-800 hover:bg-slate-100 sm:text-xs">
                                    Editer
                                </button>
                                <button type="button" data-action="style-toggle-move" class="inline-flex h-8 items-center justify-center rounded border border-slate-300 bg-white px-2 text-[11px] font-semibold text-slate-800 hover:bg-slate-100 sm:text-xs">
                                    Deplacer
                                </button>
                                <button type="button" data-action="style-close-editor" class="inline-flex h-8 items-center justify-center rounded border border-slate-300 bg-white px-2 text-[11px] font-semibold text-slate-800 hover:bg-slate-100 sm:text-xs">
                                    Fermer
                                </button>
                            </div>
                            <button type="button" class="pv-action-btn inline-flex min-h-9 items-center gap-2 rounded-lg border border-slate-300 bg-slate-200 px-3 text-xs font-semibold text-slate-800 hover:bg-slate-300 sm:min-h-10 sm:text-sm" data-action="back">
                                <i class="fa-solid fa-arrow-left"></i>Retour
                            </button>
                            <button type="button" class="pv-action-btn inline-flex min-h-9 items-center gap-2 rounded-lg border border-slate-300 bg-slate-200 px-3 text-xs font-semibold text-slate-800 hover:bg-slate-300 sm:min-h-10 sm:text-sm" data-action="close-builder">
                                <i class="fa-solid fa-xmark"></i>Fermer
                            </button>
                            <button type="button" class="pv-action-btn inline-flex min-h-9 items-center gap-2 rounded-lg border border-sky-700 bg-sky-700 px-3 text-xs font-semibold text-white hover:bg-sky-800 sm:min-h-10 sm:text-sm" data-action="download-pdf">
                                <i class="fa-solid fa-file-arrow-down"></i>Telecharger PDF
                            </button>
                        </div>
                    </div>

                    <div class="pv-cv-wrap overflow-x-auto overflow-y-hidden snap-x snap-mandatory pb-3">
                        <div class="pv-viewport mx-auto w-full max-w-full px-2">
                            <div class="pv-pages flex min-w-max snap-x snap-mandatory flex-row items-start gap-6 pb-2 md:gap-8" data-preview-pages>
                                <div class="shrink-0 snap-start w-[254px] rounded-lg border border-slate-300 bg-white px-3 py-2 text-center text-xs text-slate-500 sm:w-[349px] md:w-[476px] lg:w-[635px] xl:w-[794px]">Generation de la previsualisation paginee...</div>
                            </div>

                            <div class="hidden" aria-hidden="true">
                                <div id="pv-preview" class="pv-paper pv-print grid min-h-[1123px] w-[794px] grid-cols-[30%_70%] border border-slate-300 bg-white text-[#222222] shadow-sm">
                                    <aside class="pv-left flex min-w-0 flex-col bg-[#2f3e4e] text-white">
                                        <img data-preview="image" data-edit-step="image" data-edit-block alt="photo" class="pv-photo h-[250px] w-full cursor-pointer object-cover" style="display:block;width:100%;height:250px;object-fit:cover;object-position:center;">

                                        <section class="pv-l-section flex min-h-[291px] flex-col px-5 py-5 text-center" data-autofit-box data-edit-step="summary">
                                            <h4 class="pv-l-title text-center text-[14px] font-medium uppercase tracking-[4px]">Profile</h4>
                                            <div class="pv-l-line mx-auto my-3 h-[3px] w-10 rounded bg-white"></div>
                                            <p class="pv-l-text flex-1 cursor-pointer break-words px-5 text-center text-[14px] leading-[1.6] hover:opacity-90" data-preview="summary" data-edit-step="summary" data-edit-block data-autofit data-autofit-min="8"></p>
                                        </section>

                                        <section class="pv-l-section flex min-h-[291px] flex-col px-5 py-5 text-center" data-autofit-box data-edit-step="email">
                                            <h4 class="pv-l-title text-center text-[14px] font-medium uppercase tracking-[4px]">Contact</h4>
                                            <div class="pv-l-line mx-auto my-3 h-[3px] w-10 rounded bg-white"></div>
                                            <ul class="pv-l-list m-0 flex-1 list-none px-5">
                                                <li class="pv-l-item mb-2 flex flex-wrap cursor-pointer items-center justify-center gap-2 break-words text-[14px] leading-[1.6] opacity-90 hover:opacity-100" data-edit-step="email" data-edit-block data-autofit data-autofit-min="8"><i class="fa-solid fa-envelope text-base"></i><span class="break-all" data-preview="email"></span></li>
                                                <li class="pv-l-item mb-2 flex cursor-pointer items-center justify-center gap-2 break-words text-[14px] leading-[1.6] opacity-90 hover:opacity-100" data-edit-step="phones" data-edit-block data-autofit data-autofit-min="8"><i class="fa-solid fa-phone text-base"></i><span data-preview="phoneno"></span></li>
                                                <li class="pv-l-item mb-2 flex cursor-pointer items-center justify-center gap-2 break-words text-[14px] leading-[1.6] opacity-90 hover:opacity-100" data-edit-step="address" data-edit-block data-autofit data-autofit-min="8"><i class="fa-solid fa-location-dot text-base"></i><span data-preview="address"></span></li>
                                            </ul>
                                        </section>

                                        <section class="pv-l-section flex min-h-[291px] flex-col px-5 py-5 text-center" data-autofit-box data-edit-step="interests">
                                            <h4 class="pv-l-title text-center text-[14px] font-medium uppercase tracking-[4px]">Interets</h4>
                                            <div class="pv-l-line mx-auto my-3 h-[3px] w-10 rounded bg-white"></div>
                                            <ul class="pv-l-list m-0 flex-1 list-none px-5" data-preview-list="interests" data-edit-step="interests"></ul>
                                        </section>
                                    </aside>

                                    <main class="pv-right flex h-full min-w-0 flex-col gap-6 bg-white p-10 text-[#222222]">
                                        <header class="pv-r-header min-h-[130px] shrink-0" data-autofit-box data-edit-step="firstname">
                                            <h1 class="pv-r-name m-0 mb-[5px] cursor-pointer break-words text-[40px] font-semibold leading-[1.1] tracking-[1px] hover:opacity-90" data-preview="fullname" data-edit-step="firstname" data-edit-block data-autofit data-autofit-min="16"></h1>
                                            <p class="pv-r-role m-0 cursor-pointer break-words text-[14px] font-normal uppercase tracking-[6px] text-[#555555] hover:opacity-90" data-preview="designation" data-edit-step="designation" data-edit-block data-autofit data-autofit-min="8"></p>
                                        </header>

                                        <section class="pv-r-section flex min-h-[230px] flex-col" data-autofit-box data-edit-step="educations">
                                            <h4 class="pv-r-title m-0 mb-[15px] text-[16px] font-semibold uppercase tracking-[3px]">Formation</h4>
                                            <div class="pv-r-line mb-3.5 h-1 w-[50px] rounded bg-[#2f3e4e]"></div>
                                            <ul class="pv-edu-list flex-1 list-disc pl-[22px]" data-preview-list="educations" data-edit-step="educations"></ul>
                                        </section>

                                        <section class="pv-r-section flex min-h-[300px] flex-col" data-autofit-box data-edit-step="experiences">
                                            <h4 class="pv-r-title m-0 mb-[15px] text-[16px] font-semibold uppercase tracking-[3px]">Experience</h4>
                                            <div class="pv-r-line mb-3.5 h-1 w-[50px] rounded bg-[#2f3e4e]"></div>
                                            <div class="flex-1" data-autofit-box>
                                                <article class="pv-exp-item flex gap-[14px]" data-edit-step="experiences" data-edit-block>
                                                    <div class="pv-exp-date w-[110px] shrink-0 cursor-pointer text-[14px] font-semibold hover:opacity-90" data-preview="exp_dates" data-edit-step="experiences" data-edit-block data-autofit data-autofit-min="8"></div>
                                                    <div class="pv-exp-content flex-1">
                                                        <p class="pv-exp-role m-0 cursor-pointer break-words text-[14px] font-semibold uppercase hover:opacity-90" data-preview="exp_title" data-edit-step="experiences" data-edit-block data-autofit data-autofit-min="8"></p>
                                                        <p class="pv-exp-company m-0 mt-0.5 cursor-pointer break-words text-[14px] font-medium hover:opacity-90" data-edit-step="experiences" data-edit-block data-autofit data-autofit-min="8"><span data-preview="exp_organization"></span> - <span data-preview="exp_location"></span></p>
                                                        <ul class="pv-exp-list mb-0 mt-2 list-disc pl-[18px] text-[14px] leading-[1.5]" data-preview-list="exp_bullets" data-edit-step="experiences" data-edit-block data-autofit data-autofit-min="8"></ul>
                                                    </div>
                                                </article>
                                            </div>
                                        </section>

                                        <section class="pv-r-section flex min-h-[220px] flex-col" data-autofit-box>
                                            <h4 class="pv-r-title m-0 mb-[15px] text-[16px] font-semibold uppercase tracking-[3px]">Competences</h4>
                                            <div class="pv-r-line mb-3.5 h-1 w-[50px] rounded bg-[#2f3e4e]"></div>
                                            <div class="pv-skills-grid grid flex-1 grid-cols-2 gap-[18px]" data-autofit-box>
                                                <div data-edit-step="languages">
                                                    <p class="pv-sk-col-title m-0 mb-[7px] text-[14px] font-semibold uppercase tracking-[1px]">Langues</p>
                                                    <ul class="pv-sk-list m-0 list-disc pl-4 text-[14px] leading-[1.6]" data-preview-list="languages" data-edit-step="languages" data-autofit data-autofit-min="8"></ul>
                                                </div>
                                                <div data-edit-step="tools">
                                                    <p class="pv-sk-col-title m-0 mb-[7px] cursor-pointer text-[14px] font-semibold uppercase tracking-[1px] hover:opacity-90" data-preview="tools_title" data-edit-step="tools" data-edit-block>Logiciels</p>
                                                    <ul class="pv-sk-list m-0 list-disc pl-4 text-[14px] leading-[1.6]" data-preview-list="softwares" data-edit-step="tools" data-autofit data-autofit-min="8"></ul>
                                                </div>
                                            </div>
                                        </section>
                                    </main>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </section>
        `;
    }

    bindEvents() {
        this.addEventListener('click', (event) => {
            const insideSelectionScope = Boolean(event.target.closest('[data-style-hotspot="1"], [data-style-editor], [data-style-move-controls="1"], [data-inline-text-editor="1"]'));
            if (!insideSelectionScope && (this.activeStyleTargetKey || this.styleEditorVisible || this.inlineEditorState)) {
                this.clearStyleSelectionUi();
            }

            if (event.target.closest('[data-action="inline-text-save"]')) {
                event.preventDefault();
                event.stopPropagation();
                this.commitInlineTextEditor();
                return;
            }

            if (event.target.closest('[data-action="inline-text-cancel"]')) {
                event.preventDefault();
                event.stopPropagation();
                this.closeInlineTextEditor({ applyChanges: false });
                return;
            }

            if (event.target.closest('[data-inline-text-editor="1"]')) {
                return;
            }

            if (event.target.closest('[data-action="style-open-inline-editor"]')) {
                if (!this.activeStyleTargetKey) return;
                const hotspot = this.getPreferredHotspotForKey(this.activeStyleTargetKey);
                this.openInlineTextEditor(this.activeStyleTargetKey, hotspot);
                return;
            }

            if (event.target.closest('[data-action="style-toggle-move"]')) {
                if (!this.activeStyleTargetKey) return;
                this.moveModeEnabled = !this.moveModeEnabled;
                this.updateCustomControlsUi();
                return;
            }

            if (event.target.closest('[data-action="style-close-editor"]')) {
                this.closeInlineTextEditor({ applyChanges: false });
                this.styleEditorVisible = false;
                this.moveModeEnabled = false;
                this.updateCustomControlsUi();
                return;
            }

            const moveBtn = event.target.closest('[data-action="move-up"], [data-action="move-down"], [data-action="move-left"], [data-action="move-right"]');
            if (moveBtn) {
                if (!this.moveModeEnabled) return;
                event.preventDefault();
                event.stopPropagation();
                const targetKey = String(moveBtn.getAttribute('data-style-key') || '').trim();
                if (targetKey && targetKey !== this.activeStyleTargetKey) {
                    this.selectStyleTargetByKey(targetKey, false);
                }
                const step = event.shiftKey ? this.moveStepPx * 2 : this.moveStepPx;
                const action = moveBtn.getAttribute('data-action');
                if (action === 'move-up') this.nudgeActiveStyleTarget(0, -step);
                if (action === 'move-down') this.nudgeActiveStyleTarget(0, step);
                if (action === 'move-left') this.nudgeActiveStyleTarget(-step, 0);
                if (action === 'move-right') this.nudgeActiveStyleTarget(step, 0);
                return;
            }

            const hotspot = event.target.closest('[data-style-hotspot="1"]');
            if (hotspot) {
                this.closeInlineTextEditor({ applyChanges: false });
                this.selectStyleTargetByKey(hotspot.getAttribute('data-style-key') || '', false);
                return;
            }

            if (event.target.closest('[data-action="back"]')) {
                playUiSound('prev');
                this.dispatchEvent(new CustomEvent('preview-back', { bubbles: true }));
                return;
            }

            if (event.target.closest('[data-action="close-builder"]')) {
                playUiSound('close');
                this.dispatchEvent(new CustomEvent('close-builder', { bubbles: true }));
                return;
            }

            const downloadBtn = event.target.closest('[data-action="download-pdf"]');
            if (downloadBtn) {
                if (this.isDownloading) return;
                playUiSound('download');
                this.downloadPdf(downloadBtn);
                return;
            }

            if (event.target.closest('[data-action="style-bold"]')) {
                this.toggleActiveStyleFlag('bold');
                return;
            }

            if (event.target.closest('[data-action="style-italic"]')) {
                this.toggleActiveStyleFlag('italic');
                return;
            }

            if (event.target.closest('[data-action="style-underline"]')) {
                this.toggleActiveStyleFlag('underline');
                return;
            }

            if (event.target.closest('[data-action="style-reset"]')) {
                this.resetActiveStyleTarget();
                return;
            }

            const editTarget = event.target.closest('[data-edit-step]');
            if (editTarget) {
                const step = editTarget.getAttribute('data-edit-step');
                if (step) {
                    this.dispatchEvent(new CustomEvent('preview-edit-step', {
                        bubbles: true,
                        detail: { step }
                    }));
                }
            }
        });

        this.addEventListener('dblclick', (event) => {
            const hotspot = event.target.closest('[data-style-hotspot="1"]');
            if (hotspot) {
                const styleKey = hotspot.getAttribute('data-style-key') || '';
                this.selectStyleTargetByKey(styleKey, true);
                return;
            }
            const directTarget = event.target.closest('[data-style-target="1"]');
            if (directTarget) {
                const styleKey = directTarget.getAttribute('data-style-key') || '';
                this.selectStyleTargetByKey(styleKey, true);
            }
        });

        this.addEventListener('input', (event) => {
            const target = event.target;
            if (!(target instanceof HTMLElement)) return;
            if (target.matches('[data-action="left-color-input"]')) {
                this.setLeftColumnColor(target.value);
                return;
            }
            if (target.matches('[data-action="left-width-input"]')) {
                this.setLeftColumnWidth(target.value);
                return;
            }
            if (target.matches('[data-style-control="textColor"]')) {
                this.updateActiveStyleValue('textColor', target.value);
                return;
            }
            if (target.matches('[data-style-control="bgColor"]')) {
                this.updateActiveStyleValue('bgColor', target.value);
                return;
            }
            if (target.matches('[data-style-control="fontSizePx"]')) {
                this.updateActiveStyleValue('fontSizePx', target.value);
                return;
            }
            if (target.matches('[data-style-control="lineHeight"]')) {
                this.updateActiveStyleValue('lineHeight', target.value);
                return;
            }
            if (target.matches('[data-style-control="textAlign"]')) {
                this.updateActiveStyleValue('textAlign', target.value);
                return;
            }
            if (target.matches('[data-style-control="bulletStyle"]')) {
                this.updateActiveStyleValue('bulletStyle', target.value);
                return;
            }
            if (target.matches('[data-style-control="widthPx"]')) {
                this.updateActiveStyleValue('widthPx', target.value);
                return;
            }
            if (target.matches('[data-style-control="heightPx"]')) {
                this.updateActiveStyleValue('heightPx', target.value);
            }
        });

        this.addEventListener('change', (event) => {
            const target = event.target;
            if (!(target instanceof HTMLElement)) return;
            if (target.matches('[data-action="typography-select"]')) {
                this.setTypography(target.value);
                return;
            }
            if (target.matches('[data-action="photo-shape-select"]')) {
                this.setPhotoShape(target.value);
                return;
            }
            if (target.matches('[data-style-control]')) {
                this.syncStyleEditorControls();
            }
        });
    }

    loadData(data) {
        this.data = data || {};
        this.updatePreview();
    }

    updatePreview() {
        const data = this.resolveDataWithDefaults();
        this.setText('fullname', [data.firstname, data.middlename, data.lastname].filter(Boolean).join(' ') || 'Nom Prenom');
        this.setText('designation', data.designation || 'Poste');
        this.setText('email', data.email || 'email@exemple.com');
        this.setText('phoneno', this.formatPhoneDisplay(data) || '+00 0 00 00 00 00');
        this.setText('address', this.formatAddressDisplay(data) || 'Adresse complete');
        this.setText('summary', data.summary || 'Profil a renseigner depuis le formulaire.');

        this.setText('exp_title', data.exp_title || 'Poste');
        this.setText('exp_organization', data.exp_organization || 'Entreprise');
        this.setText('exp_location', data.exp_location || 'Lieu');
        this.setText('exp_dates', this.formatDateRange(data.exp_start_date, data.exp_end_date) || 'Periode');

        this.renderEducationList(this.buildEducationItems(data));
        this.renderExperienceList(this.buildExperienceItems(data));

        this.renderList('interests', this.buildInterestItems(data, this.data));
        this.renderList('languages', this.buildLanguageItems(data));
        const toolData = this.buildSoftwareItems(data);
        this.setText('tools_title', toolData.title);
        this.renderList('softwares', toolData.items);

        const imageEl = this.querySelector('[data-preview="image"]');
        if (imageEl) {
            if (data.image) imageEl.src = data.image;
            else imageEl.removeAttribute('src');
        }
        const preview = this.querySelector('#pv-preview');
        this.setupStyleTargets(preview);
        this.applyTextOverrides(preview);
        this.applyPreviewCustomStyles();
        requestAnimationFrame(() => {
            this.updatePdfPagesHint();
            this.schedulePaginatedPreviewRender({ delayMs: this.fastRenderDelayMs, showLoading: false });
        });
        // Garde des tailles de texte stables et lisibles
        // au lieu de reduire automatiquement toute la mise en page.
    }

    schedulePaginatedPreviewRender(options = {}) {
        const delayMsRaw = Number(options?.delayMs);
        const delayMs = Number.isFinite(delayMsRaw) ? Math.max(0, Math.floor(delayMsRaw)) : this.fastRenderDelayMs;
        const showLoading = Boolean(options?.showLoading);
        if (this.pagesRenderTimer) {
            clearTimeout(this.pagesRenderTimer);
        }
        this.pagesRenderTimer = window.setTimeout(() => {
            this.renderPaginatedPreview({ showLoading });
        }, delayMs);
    }

    updatePdfPagesHint() {
        const hint = this.querySelector('[data-pdf-pages-hint]');
        const preview = this.querySelector('#pv-preview');
        if (!hint || !preview) return;

        const marginXmm = 6;
        const marginYmm = 0;
        const pageWidthMm = 210;
        const pageHeightMm = 297;
        const contentWidthMm = pageWidthMm - (marginXmm * 2);
        const contentHeightMm = pageHeightMm - (marginYmm * 2);
        const exportWidthPx = 794;
        const pageHeightPx = Math.floor((contentHeightMm / contentWidthMm) * exportWidthPx);

        const contentHeightPx = Math.max(preview.scrollHeight, preview.offsetHeight, 1123);
        const pages = Math.max(1, Math.ceil(contentHeightPx / pageHeightPx));

        hint.classList.remove('hidden');
        if (pages > 1) {
            hint.textContent = `PDF estime: ${pages} pages. Si vous voulez une seule page, reduisez un peu le texte.`;
            hint.classList.remove('text-slate-600');
            hint.classList.add('text-amber-700');
            return;
        }

        hint.textContent = 'PDF estime: 1 page.';
        hint.classList.remove('text-amber-700');
        hint.classList.add('text-slate-600');
    }

    normalizeCssPixel(value, min = 0, max = 4000) {
        const raw = String(value ?? '').trim();
        if (!raw) return '';
        const parsed = Number(raw.replace(',', '.'));
        if (!Number.isFinite(parsed)) return '';
        const clamped = Math.max(min, Math.min(max, parsed));
        return String(Math.round(clamped));
    }

    normalizeCssNumber(value, min = 0, max = 4000, decimals = 2) {
        const raw = String(value ?? '').trim();
        if (!raw) return '';
        const parsed = Number(raw.replace(',', '.'));
        if (!Number.isFinite(parsed)) return '';
        const clamped = Math.max(min, Math.min(max, parsed));
        const factor = Math.pow(10, Math.max(0, Math.floor(decimals)));
        const rounded = Math.round(clamped * factor) / factor;
        return String(rounded);
    }

    normalizeLineHeightValue(value, min = 0.8, max = 3) {
        const raw = String(value ?? '').trim();
        if (!raw) return '';
        const parsed = Number(raw.replace(',', '.'));
        if (!Number.isFinite(parsed)) return '';
        const clamped = Math.max(min, Math.min(max, parsed));
        return String(Number(clamped.toFixed(2)));
    }

    cssColorToHex(value, fallback = '#222222') {
        const hex = this.normalizeHexColor(value);
        if (hex) return hex;
        const raw = String(value || '').trim();
        const rgbMatch = raw.match(/^rgba?\(([^)]+)\)$/i);
        if (!rgbMatch) return fallback;
        const parts = rgbMatch[1].split(',').map((p) => Number(p.trim()));
        if (parts.length < 3 || parts.some((n, idx) => idx < 3 && !Number.isFinite(n))) return fallback;
        if (parts.length >= 4 && Number.isFinite(parts[3]) && parts[3] <= 0.01) return fallback;
        const toHex = (n) => Math.max(0, Math.min(255, Math.round(n))).toString(16).padStart(2, '0');
        return `#${toHex(parts[0])}${toHex(parts[1])}${toHex(parts[2])}`;
    }

    buildStyleTargetKey(el, preview) {
        if (!el || !preview) return '';
        if (el.matches('img') || el.getAttribute('data-preview') === 'image') return '';
        if (el.dataset.preview) return `preview:${el.dataset.preview}`;
        const previewList = el.closest('[data-preview-list]');
        if (previewList) {
            const listName = previewList.getAttribute('data-preview-list') || 'list';
            if (el.matches('li')) {
                const index = Array.from(previewList.children).indexOf(el);
                return `list-item:${listName}:${Math.max(0, index)}`;
            }
            return `list:${listName}`;
        }
        const expItem = el.closest('.pv-exp-item');
        if (expItem) {
            const expIndex = Array.from(preview.querySelectorAll('.pv-exp-item')).indexOf(expItem);
            if (el.classList.contains('pv-exp-date')) return `exp:${expIndex}:date`;
            if (el.classList.contains('pv-exp-role')) return `exp:${expIndex}:role`;
            if (el.classList.contains('pv-exp-company')) return `exp:${expIndex}:company`;
            if (el.matches('li')) {
                const list = el.closest('.pv-exp-list');
                const liIndex = list ? Array.from(list.children).indexOf(el) : 0;
                return `exp:${expIndex}:bullet:${Math.max(0, liIndex)}`;
            }
        }
        const eduItem = el.closest('.pv-edu-list');
        if (eduItem && el.matches('.pv-edu-li')) {
            const index = Array.from(eduItem.children).indexOf(el);
            return `edu:${Math.max(0, index)}`;
        }
        return `path:${this.buildElementPath(el, preview)}`;
    }

    buildElementPath(el, stopNode) {
        if (!el || !stopNode) return '';
        const segments = [];
        let current = el;
        while (current && current !== stopNode) {
            const parent = current.parentElement;
            if (!parent) break;
            const siblings = Array.from(parent.children || []);
            const idx = siblings.indexOf(current);
            segments.push(`${current.tagName.toLowerCase()}:${Math.max(0, idx)}`);
            current = parent;
        }
        segments.reverse();
        return segments.join('/');
    }

    setupStyleTargets(preview) {
        if (!preview) return;
        const selectors = [
            '[data-edit-block]:not(img):not([data-preview="image"])',
            '[data-preview]:not(img):not([data-preview="image"])',
            '.pv-r-title',
            '.pv-l-title',
            '.pv-sk-col-title',
            '.pv-l-item',
            '.pv-l-text',
            '.pv-r-role',
            '.pv-r-name',
            '.pv-exp-date',
            '.pv-exp-role',
            '.pv-exp-company',
            '.pv-edu-li',
            '.pv-exp-list li',
            '.pv-sk-list li'
        ];
        const seen = new Set();
        selectors.forEach((selector) => {
            preview.querySelectorAll(selector).forEach((el) => seen.add(el));
        });
        const usedKeys = new Map();
        Array.from(seen).forEach((el) => {
            const baseKey = this.buildStyleTargetKey(el, preview);
            if (!baseKey) return;
            const count = usedKeys.get(baseKey) || 0;
            const key = count > 0 ? `${baseKey}__${count}` : baseKey;
            usedKeys.set(baseKey, count + 1);
            el.setAttribute('data-style-target', '1');
            el.setAttribute('data-style-key', key);
            if (!el.getAttribute('data-style-label')) {
                const snippet = (el.textContent || '').replace(/\s+/g, ' ').trim();
                el.setAttribute('data-style-label', snippet.slice(0, 48) || key);
            }
        });
        if (this.activeStyleTargetKey && !this.getStyleTargetElement(this.activeStyleTargetKey, preview)) {
            this.activeStyleTargetKey = '';
        }
    }

    getStyleTargetElement(key, root = null) {
        const cleanKey = String(key || '').trim();
        if (!cleanKey) return null;
        const host = root || this.querySelector('#pv-preview');
        if (!host) return null;
        return host.querySelector(`[data-style-key="${cleanKey.replace(/"/g, '\\"')}"]`);
    }

    getActiveStyleObject() {
        const key = this.activeStyleTargetKey;
        if (!key) return null;
        if (!this.elementStyleMap[key]) this.elementStyleMap[key] = {};
        return this.elementStyleMap[key];
    }

    selectStyleTargetByKey(key, openEditor = false) {
        const cleanKey = String(key || '').trim();
        if (!cleanKey) return;
        const preview = this.querySelector('#pv-preview');
        if (!preview) return;
        const target = this.getStyleTargetElement(cleanKey, preview);
        if (!target) return;
        this.activeStyleTargetKey = cleanKey;
        if (openEditor) this.styleEditorVisible = true;
        this.syncStyleEditorControls();
        this.updateCustomControlsUi();
    }

    clearStyleSelectionUi() {
        if (this.inlineEditorState) {
            this.closeInlineTextEditor({ applyChanges: false });
        }
        this.activeStyleTargetKey = '';
        this.styleEditorVisible = false;
        this.moveModeEnabled = false;
        this.updateCustomControlsUi();
    }

    getPreferredHotspotForKey(styleKey) {
        const cleanKey = String(styleKey || '').trim();
        if (!cleanKey) return null;
        const selectorKey = cleanKey.replace(/"/g, '\\"');
        const zones = Array.from(this.querySelectorAll(`[data-style-hotspot="1"][data-style-key="${selectorKey}"]`));
        if (!zones.length) return null;
        let best = zones[0];
        let bestArea = 0;
        zones.forEach((zone) => {
            const rect = zone.getBoundingClientRect();
            const area = rect && rect.width && rect.height ? (rect.width * rect.height) : 0;
            if (area > bestArea) {
                bestArea = area;
                best = zone;
            }
        });
        return best;
    }

    makeStyleLabel(text, fallback = 'Bloc') {
        const normalized = String(text ?? '').replace(/\s+/g, ' ').trim();
        if (normalized) return normalized.slice(0, 48);
        return String(fallback || 'Bloc');
    }

    canInlineEditTarget(target) {
        if (!target || !(target instanceof HTMLElement)) return false;
        if (target.matches('ul,ol,img,svg,canvas,input,textarea,select,button')) return false;
        if (target.querySelector('ul,ol')) return false;
        if (target.querySelector('[data-style-target="1"]')) return false;
        return true;
    }

    applyInlineTextToTarget(target, text, styleKey = '') {
        if (!target) return;
        const normalized = String(text ?? '').replace(/\r/g, '').trim();
        target.textContent = normalized;
        const key = String(styleKey || target.getAttribute('data-style-key') || '').trim();
        const nextLabel = this.makeStyleLabel(normalized, key || 'Bloc');
        target.setAttribute('data-style-label', nextLabel);
        if (key) {
            this.textOverrideMap[key] = normalized;
        }
    }

    openInlineTextEditor(styleKey, anchorNode = null) {
        const cleanKey = String(styleKey || '').trim();
        if (!cleanKey) return false;
        const preview = this.querySelector('#pv-preview');
        if (!preview) return false;
        const target = this.getStyleTargetElement(cleanKey, preview);
        if (!this.canInlineEditTarget(target)) return false;

        this.closeInlineTextEditor({ applyChanges: true });
        this.selectStyleTargetByKey(cleanKey, false);

        const layer = anchorNode?.parentElement || this.querySelector('[data-preview-pages]');
        if (!layer || !(layer instanceof HTMLElement)) return false;
        const layerRect = layer.getBoundingClientRect();
        const anchorRect = (anchorNode instanceof HTMLElement) ? anchorNode.getBoundingClientRect() : layerRect;
        if (!layerRect.width || !layerRect.height) return false;

        const box = document.createElement('div');
        box.className = 'absolute z-[7] rounded-xl border border-slate-300 bg-white p-2 shadow-xl';
        box.setAttribute('data-inline-text-editor', '1');
        const preferredWidth = Math.max(220, Math.min(360, anchorRect.width + 60));
        const editorWidth = Math.max(180, Math.min(preferredWidth, Math.max(180, layerRect.width - 16)));
        const leftPx = Math.max(8, Math.min(layerRect.width - editorWidth - 8, anchorRect.left - layerRect.left));
        const preferredTop = (anchorRect.top - layerRect.top) - 126;
        const fallbackTop = (anchorRect.bottom - layerRect.top) + 8;
        const topPx = preferredTop >= 8
            ? preferredTop
            : Math.max(8, Math.min(layerRect.height - 140, fallbackTop));
        box.style.left = `${leftPx}px`;
        box.style.top = `${topPx}px`;
        box.style.width = `${editorWidth}px`;
        box.innerHTML = `
            <textarea data-inline-input class="h-24 w-full resize-y rounded-md border border-slate-300 bg-white px-2 py-1 text-xs text-slate-800 outline-none focus:border-sky-500"></textarea>
            <div class="mt-2 flex items-center justify-end gap-2">
                <button type="button" data-action="inline-text-cancel" class="inline-flex h-8 items-center justify-center rounded border border-slate-300 bg-white px-2 text-xs font-semibold text-slate-700 hover:bg-slate-100">Annuler</button>
                <button type="button" data-action="inline-text-save" class="inline-flex h-8 items-center justify-center rounded border border-sky-700 bg-sky-700 px-2 text-xs font-semibold text-white hover:bg-sky-800">Appliquer</button>
            </div>
            <p class="mt-1 text-[10px] text-slate-500">Entree pour valider, Echap pour annuler</p>
        `;
        layer.appendChild(box);

        const input = box.querySelector('[data-inline-input]');
        const initialText = Object.prototype.hasOwnProperty.call(this.textOverrideMap, cleanKey)
            ? this.textOverrideMap[cleanKey]
            : (target.textContent || '');
        if (!(input instanceof HTMLTextAreaElement)) return false;
        input.value = String(initialText || '');
        input.addEventListener('keydown', (event) => {
            if (event.key === 'Escape') {
                event.preventDefault();
                this.closeInlineTextEditor({ applyChanges: false });
                return;
            }
            if (event.key === 'Enter' && !event.shiftKey) {
                event.preventDefault();
                this.commitInlineTextEditor();
            }
        });
        input.focus();
        input.select();

        this.inlineEditorState = { styleKey: cleanKey, layer, box, input };
        return true;
    }

    closeInlineTextEditor(options = {}) {
        const state = this.inlineEditorState;
        if (!state) return;
        if (options?.applyChanges === true) {
            this.commitInlineTextEditor();
            return;
        }
        if (state.box && state.box.parentElement) {
            state.box.parentElement.removeChild(state.box);
        }
        this.inlineEditorState = null;
    }

    commitInlineTextEditor() {
        const state = this.inlineEditorState;
        if (!state) return false;
        const styleKey = String(state.styleKey || '').trim();
        const nextText = state.input instanceof HTMLTextAreaElement ? state.input.value : '';
        const preview = this.querySelector('#pv-preview');
        const target = this.getStyleTargetElement(styleKey, preview);
        if (target) {
            this.applyInlineTextToTarget(target, nextText, styleKey);
        } else {
            this.textOverrideMap[styleKey] = String(nextText || '').trim();
        }
        this.closeInlineTextEditor({ applyChanges: false });
        this.refreshAfterStyleChange({ delayMs: this.fastRenderDelayMs });
        return true;
    }

    applyTextOverrides(preview) {
        if (!preview || !this.textOverrideMap || typeof this.textOverrideMap !== 'object') return;
        Object.keys(this.textOverrideMap).forEach((key) => {
            if (!Object.prototype.hasOwnProperty.call(this.textOverrideMap, key)) return;
            const target = this.getStyleTargetElement(key, preview);
            if (!this.canInlineEditTarget(target)) return;
            this.applyInlineTextToTarget(target, this.textOverrideMap[key], key);
        });
    }

    toggleActiveStyleFlag(flag) {
        const style = this.getActiveStyleObject();
        if (!style) return;
        if (flag !== 'bold' && flag !== 'italic' && flag !== 'underline') return;
        style[flag] = !Boolean(style[flag]);
        this.refreshAfterStyleChange();
    }

    nudgeActiveStyleTarget(dx = 0, dy = 0) {
        const style = this.getActiveStyleObject();
        if (!style) return;
        const clamp = (value) => Math.max(-this.maxMoveOffsetPx, Math.min(this.maxMoveOffsetPx, Math.round(value)));
        const currentX = Number(style.offsetXPx || 0);
        const currentY = Number(style.offsetYPx || 0);
        const nextX = clamp((Number.isFinite(currentX) ? currentX : 0) + dx);
        const nextY = clamp((Number.isFinite(currentY) ? currentY : 0) + dy);

        if (nextX !== 0) style.offsetXPx = String(nextX);
        else delete style.offsetXPx;

        if (nextY !== 0) style.offsetYPx = String(nextY);
        else delete style.offsetYPx;

        this.refreshAfterStyleChange({ delayMs: this.moveRenderDelayMs, skipHint: true });
    }

    updateActiveStyleValue(field, value) {
        const style = this.getActiveStyleObject();
        if (!style) return;
        let changed = false;
        switch (field) {
            case 'textColor': {
                const next = this.normalizeHexColor(value);
                if (next && style.textColor !== next) {
                    style.textColor = next;
                    changed = true;
                }
                break;
            }
            case 'bgColor': {
                const next = this.normalizeHexColor(value);
                if (next && style.bgColor !== next) {
                    style.bgColor = next;
                    changed = true;
                }
                break;
            }
            case 'fontSizePx': {
                const next = this.normalizeCssNumber(value, 8, 160, 2);
                if (next) {
                    if (style.fontSizePx !== next) {
                        style.fontSizePx = next;
                        changed = true;
                    }
                } else if (style.fontSizePx !== undefined) {
                    delete style.fontSizePx;
                    changed = true;
                }
                break;
            }
            case 'lineHeight': {
                const next = this.normalizeLineHeightValue(value, 0.8, 3);
                if (next) {
                    if (style.lineHeight !== next) {
                        style.lineHeight = next;
                        changed = true;
                    }
                } else if (style.lineHeight !== undefined) {
                    delete style.lineHeight;
                    changed = true;
                }
                break;
            }
            case 'textAlign': {
                const next = String(value || '').trim();
                if (next) {
                    if (style.textAlign !== next) {
                        style.textAlign = next;
                        changed = true;
                    }
                } else if (style.textAlign !== undefined) {
                    delete style.textAlign;
                    changed = true;
                }
                break;
            }
            case 'bulletStyle': {
                const next = String(value || '').trim() || 'none';
                if (style.bulletStyle !== next) {
                    style.bulletStyle = next;
                    changed = true;
                }
                break;
            }
            case 'widthPx': {
                const next = this.normalizeCssPixel(value, 20, 1600);
                if (next) {
                    if (style.widthPx !== next) {
                        style.widthPx = next;
                        changed = true;
                    }
                } else if (style.widthPx !== undefined) {
                    delete style.widthPx;
                    changed = true;
                }
                break;
            }
            case 'heightPx': {
                const next = this.normalizeCssPixel(value, 20, 1600);
                if (next) {
                    if (style.heightPx !== next) {
                        style.heightPx = next;
                        changed = true;
                    }
                } else if (style.heightPx !== undefined) {
                    delete style.heightPx;
                    changed = true;
                }
                break;
            }
            default:
                return;
        }
        if (changed) this.refreshAfterStyleChange();
    }

    resetActiveStyleTarget() {
        if (!this.activeStyleTargetKey) return;
        delete this.elementStyleMap[this.activeStyleTargetKey];
        this.refreshAfterStyleChange();
    }

    syncStyleEditorControls() {
        const editor = this.querySelector('[data-style-editor]');
        if (!editor) return;
        const hasTarget = Boolean(this.activeStyleTargetKey);
        const preview = this.querySelector('#pv-preview');
        const target = hasTarget ? this.getStyleTargetElement(this.activeStyleTargetKey, preview) : null;
        const style = hasTarget ? (this.elementStyleMap[this.activeStyleTargetKey] || {}) : {};
        const computed = target ? window.getComputedStyle(target) : null;
        const label = this.querySelector('[data-style-target-label]');
        if (label) {
            label.textContent = target ? (target.getAttribute('data-style-label') || this.activeStyleTargetKey) : 'Aucun';
        }

        const setValue = (selector, val) => {
            const node = this.querySelector(selector);
            if (!node) return;
            node.value = val;
            node.disabled = !hasTarget;
        };
        setValue('[data-style-control="textColor"]', this.cssColorToHex(style.textColor || computed?.color || '', '#222222'));
        setValue('[data-style-control="bgColor"]', this.cssColorToHex(style.bgColor || computed?.backgroundColor || '', '#ffffff'));
        setValue('[data-style-control="fontSizePx"]', style.fontSizePx || this.normalizeCssNumber(computed?.fontSize || '', 8, 160, 2));
        setValue('[data-style-control="lineHeight"]', style.lineHeight || this.normalizeLineHeightValue(computed?.lineHeight || '', 0.8, 3));
        setValue('[data-style-control="textAlign"]', style.textAlign || (computed ? String(computed.textAlign || '') : ''));
        setValue('[data-style-control="bulletStyle"]', style.bulletStyle || 'none');
        setValue('[data-style-control="widthPx"]', style.widthPx || '');
        setValue('[data-style-control="heightPx"]', style.heightPx || '');

        ['style-bold', 'style-italic', 'style-underline', 'style-reset', 'style-open-inline-editor', 'style-toggle-move', 'style-close-editor'].forEach((action) => {
            const btn = this.querySelector(`[data-action="${action}"]`);
            if (btn) btn.disabled = !hasTarget;
        });
    }

    applyElementStyle(target, style) {
        if (!target || !style || typeof style !== 'object') return;
        if (style.textColor) target.style.color = style.textColor;
        if (style.bgColor) target.style.setProperty('background-color', style.bgColor, 'important');
        if (style.fontSizePx) target.style.fontSize = `${style.fontSizePx}px`;
        if (style.lineHeight) target.style.lineHeight = style.lineHeight;
        if (style.bold !== undefined) target.style.fontWeight = style.bold ? '700' : '400';
        if (style.italic !== undefined) target.style.fontStyle = style.italic ? 'italic' : 'normal';
        if (style.underline !== undefined) target.style.textDecoration = style.underline ? 'underline' : 'none';
        if (style.textAlign) target.style.textAlign = style.textAlign;
        if (style.widthPx) {
            target.style.width = `${style.widthPx}px`;
            if (window.getComputedStyle(target).display === 'inline') {
                target.style.display = 'inline-block';
            }
        }
        if (style.heightPx) {
            target.style.height = `${style.heightPx}px`;
            if (window.getComputedStyle(target).display === 'inline') {
                target.style.display = 'inline-block';
            }
        }

        const offsetX = Number(style.offsetXPx || 0);
        const offsetY = Number(style.offsetYPx || 0);
        const hasOffset = Number.isFinite(offsetX) && Number.isFinite(offsetY) && (Math.abs(offsetX) > 0 || Math.abs(offsetY) > 0);
        if (hasOffset) {
            target.style.transform = `translate(${Math.round(offsetX)}px, ${Math.round(offsetY)}px)`;
        } else {
            target.style.removeProperty('transform');
        }

        const listTarget = target.closest('ul,ol');
        if (listTarget) {
            const bulletStyle = style.bulletStyle || 'none';
            listTarget.style.listStyleType = bulletStyle;
            listTarget.style.paddingLeft = bulletStyle === 'none' ? '0' : '1.1rem';
        }
    }

    refreshVisibleHotspotSelection() {
        const selectedKey = (this.activeStyleTargetKey || '').trim();
        this.querySelectorAll('[data-style-move-controls="1"]').forEach((node) => node.remove());
        let anchorZone = null;
        let anchorArea = -1;
        this.querySelectorAll('[data-style-hotspot="1"]').forEach((zone) => {
            const isActive = Boolean(selectedKey) && zone.getAttribute('data-style-key') === selectedKey;
            zone.classList.toggle('ring-2', isActive);
            zone.classList.toggle('ring-sky-500', isActive);
            zone.classList.toggle('ring-inset', isActive);
            zone.classList.toggle('bg-sky-300/20', isActive);
            if (isActive) {
                const width = Number.parseFloat(String(zone.style.width || '0').replace('%', ''));
                const height = Number.parseFloat(String(zone.style.height || '0').replace('%', ''));
                const area = (Number.isFinite(width) ? width : 0) * (Number.isFinite(height) ? height : 0);
                if (!anchorZone || area > anchorArea) {
                    anchorZone = zone;
                    anchorArea = area;
                }
            }
        });
        if (anchorZone && selectedKey && this.moveModeEnabled) {
            this.appendDirectionalMoveControls(anchorZone, selectedKey);
        }
    }

    syncVisibleHotspotsWithOffsets() {
        let activeMoved = false;
        this.querySelectorAll('[data-style-hotspot="1"]').forEach((zone) => {
            const styleKey = String(zone.getAttribute('data-style-key') || '').trim();
            if (!styleKey) return;

            const style = this.elementStyleMap[styleKey] || {};
            const renderedOffsetX = Number(zone.dataset.renderedOffsetX || 0);
            const renderedOffsetY = Number(zone.dataset.renderedOffsetY || 0);
            const scale = Number(zone.dataset.hotspotScale || 1);
            const safeScale = Number.isFinite(scale) && scale > 0 ? scale : 1;
            const targetOffsetX = Number(style.offsetXPx || 0);
            const targetOffsetY = Number(style.offsetYPx || 0);
            const safeTargetOffsetX = Number.isFinite(targetOffsetX) ? targetOffsetX : 0;
            const safeTargetOffsetY = Number.isFinite(targetOffsetY) ? targetOffsetY : 0;
            const safeRenderedOffsetX = Number.isFinite(renderedOffsetX) ? renderedOffsetX : 0;
            const safeRenderedOffsetY = Number.isFinite(renderedOffsetY) ? renderedOffsetY : 0;

            const shiftX = (safeTargetOffsetX - safeRenderedOffsetX) * safeScale;
            const shiftY = (safeTargetOffsetY - safeRenderedOffsetY) * safeScale;
            if (Math.abs(shiftX) > 0.05 || Math.abs(shiftY) > 0.05) {
                zone.style.transform = `translate(${shiftX.toFixed(2)}px, ${shiftY.toFixed(2)}px)`;
                if (styleKey === this.activeStyleTargetKey) {
                    activeMoved = true;
                }
            } else {
                zone.style.removeProperty('transform');
            }
        });
        if (activeMoved) {
            this.refreshVisibleHotspotSelection();
        }
    }

    appendDirectionalMoveControls(zone, styleKey) {
        const layer = zone?.parentElement;
        if (!layer) return;
        const layerRect = layer.getBoundingClientRect();
        const zoneRect = zone.getBoundingClientRect();
        if (!layerRect?.width || !layerRect?.height || !zoneRect?.width || !zoneRect?.height) return;

        const centerXPx = (zoneRect.left - layerRect.left) + (zoneRect.width / 2);
        const centerYPx = (zoneRect.top - layerRect.top) + Math.min(24, Math.max(zoneRect.height / 2, 8));
        const centerX = Math.max(6, Math.min(94, (centerXPx / layerRect.width) * 100));
        const centerY = Math.max(8, Math.min(92, (centerYPx / layerRect.height) * 100));

        const controls = document.createElement('div');
        controls.className = 'absolute z-[4] grid grid-cols-3 grid-rows-3 gap-1 rounded-lg border border-slate-200 bg-white/95 p-1 shadow-lg backdrop-blur-sm';
        controls.style.left = `${centerX}%`;
        controls.style.top = `${centerY}%`;
        controls.style.transform = 'translate(-50%, -50%)';
        controls.setAttribute('data-style-move-controls', '1');
        controls.innerHTML = `
            <button type="button" data-action="move-up" data-style-key="${styleKey}" class="col-start-2 row-start-1 inline-flex h-7 w-7 items-center justify-center rounded border border-slate-300 bg-white text-xs text-slate-700 hover:bg-slate-100" title="Monter"><i class="fa-solid fa-arrow-up"></i></button>
            <button type="button" data-action="move-left" data-style-key="${styleKey}" class="col-start-1 row-start-2 inline-flex h-7 w-7 items-center justify-center rounded border border-slate-300 bg-white text-xs text-slate-700 hover:bg-slate-100" title="Gauche"><i class="fa-solid fa-arrow-left"></i></button>
            <button type="button" data-action="move-right" data-style-key="${styleKey}" class="col-start-3 row-start-2 inline-flex h-7 w-7 items-center justify-center rounded border border-slate-300 bg-white text-xs text-slate-700 hover:bg-slate-100" title="Droite"><i class="fa-solid fa-arrow-right"></i></button>
            <button type="button" data-action="move-down" data-style-key="${styleKey}" class="col-start-2 row-start-3 inline-flex h-7 w-7 items-center justify-center rounded border border-slate-300 bg-white text-xs text-slate-700 hover:bg-slate-100" title="Descendre"><i class="fa-solid fa-arrow-down"></i></button>
        `;
        layer.appendChild(controls);
    }

    normalizeHexColor(value) {
        const raw = (value || '').trim();
        if (!raw) return '';
        if (/^#[0-9a-f]{6}$/i.test(raw)) {
            return raw.toLowerCase();
        }
        const shortMatch = raw.match(/^#([0-9a-f]{3})$/i);
        if (!shortMatch) return '';
        const hex = shortMatch[1].toLowerCase();
        return `#${hex[0]}${hex[0]}${hex[1]}${hex[1]}${hex[2]}${hex[2]}`;
    }

    clampFontScale(value) {
        const next = Number(value);
        if (!Number.isFinite(next)) return this.fontScale;
        return Math.max(this.minFontScale, Math.min(this.maxFontScale, next));
    }

    clampLeftColumnWidth(value) {
        const next = Number(value);
        if (!Number.isFinite(next)) return this.leftColumnWidth;
        return Math.max(this.minLeftColumnWidth, Math.min(this.maxLeftColumnWidth, Math.round(next)));
    }

    clampLineHeightScale(value) {
        const next = Number(value);
        if (!Number.isFinite(next)) return this.lineHeightScale;
        return Math.max(this.minLineHeightScale, Math.min(this.maxLineHeightScale, next));
    }

    refreshAfterStyleChange(options = {}) {
        const delayMsRaw = Number(options?.delayMs);
        const delayMs = Number.isFinite(delayMsRaw) ? Math.max(0, Math.floor(delayMsRaw)) : this.softRenderDelayMs;
        const skipHint = Boolean(options?.skipHint);
        this.applyPreviewCustomStyles();
        this.syncVisibleHotspotsWithOffsets();
        if (!skipHint) {
            this.updatePdfPagesHint();
        }
        this.schedulePaginatedPreviewRender({ delayMs, showLoading: false });
    }

    adjustFontScale(direction) {
        const step = Number(direction) > 0 ? this.fontScaleStep : -this.fontScaleStep;
        const nextScale = this.clampFontScale(this.fontScale + step);
        if (Math.abs(nextScale - this.fontScale) < 0.0001) {
            this.updateCustomControlsUi();
            return;
        }
        this.fontScale = nextScale;
        this.refreshAfterStyleChange();
    }

    setTypography(key) {
        const resolvedKey = Object.prototype.hasOwnProperty.call(this.typographyOptions, key) ? key : 'poppins';
        if (resolvedKey === this.typographyKey) {
            this.updateCustomControlsUi();
            return;
        }
        this.typographyKey = resolvedKey;
        this.refreshAfterStyleChange();
        this.ensureTypographyStylesheet(this.typographyKey).then(() => {
            this.refreshAfterStyleChange();
        });
    }

    setLeftColumnColor(value) {
        const nextColor = this.normalizeHexColor(value);
        if (!nextColor) return;
        if (nextColor === this.leftColumnColor) {
            this.updateCustomControlsUi();
            return;
        }
        this.leftColumnColor = nextColor;
        this.refreshAfterStyleChange();
    }

    setLeftColumnWidth(value) {
        const nextWidth = this.clampLeftColumnWidth(value);
        if (nextWidth === this.leftColumnWidth) {
            this.updateCustomControlsUi();
            return;
        }
        this.leftColumnWidth = nextWidth;
        this.refreshAfterStyleChange();
    }

    setPhotoShape(value) {
        const shape = value === 'circle' ? 'circle' : 'rect';
        if (shape === this.photoShape) {
            this.updateCustomControlsUi();
            return;
        }
        this.photoShape = shape;
        this.refreshAfterStyleChange();
    }

    setTitleColor(value) {
        const nextColor = this.normalizeHexColor(value);
        if (!nextColor) return;
        if (nextColor === this.titleColor) {
            this.updateCustomControlsUi();
            return;
        }
        this.titleColor = nextColor;
        this.refreshAfterStyleChange();
    }

    setSubtitleColor(value) {
        const nextColor = this.normalizeHexColor(value);
        if (!nextColor) return;
        if (nextColor === this.subtitleColor) {
            this.updateCustomControlsUi();
            return;
        }
        this.subtitleColor = nextColor;
        this.refreshAfterStyleChange();
    }

    setLineHeightScale(value) {
        const nextScale = this.clampLineHeightScale(value);
        if (Math.abs(nextScale - this.lineHeightScale) < 0.0001) {
            this.updateCustomControlsUi();
            return;
        }
        this.lineHeightScale = nextScale;
        this.refreshAfterStyleChange();
    }

    applyPreviewCustomStyles() {
        const preview = this.querySelector('#pv-preview');
        if (!preview) {
            this.updateCustomControlsUi();
            return;
        }
        this.applyStylesToPreviewNode(preview);
        this.updateCustomControlsUi();
    }

    applyStylesToPreviewNode(preview) {
        if (!preview) return;
        this.setupStyleTargets(preview);
        this.applyTextOverrides(preview);
        this.resetStyleTargetInlineStyles(preview);
        const typography = this.getTypographyConfig(this.typographyKey);
        if (typography && typography.fontFamily) {
            preview.style.fontFamily = typography.fontFamily;
        }

        const rightColumnWidth = Math.max(5, 100 - this.leftColumnWidth);
        preview.style.gridTemplateColumns = `${this.leftColumnWidth}% ${rightColumnWidth}%`;

        const leftColumn = preview.querySelector('.pv-left');
        const textColor = this.getContrastingTextColor(this.leftColumnColor);
        if (leftColumn) {
            leftColumn.style.backgroundColor = this.leftColumnColor;
            leftColumn.style.color = textColor;
            leftColumn.querySelectorAll('.pv-l-title').forEach((title) => {
                title.style.color = textColor;
            });
        }

        preview.querySelectorAll('.pv-left .pv-l-line').forEach((line) => {
            line.style.backgroundColor = textColor;
        });

        preview.querySelectorAll('.pv-r-line').forEach((line) => {
            line.style.backgroundColor = this.leftColumnColor;
        });

        preview.querySelectorAll('.pv-l-list, .pv-edu-list, .pv-exp-list, .pv-sk-list').forEach((list) => {
            list.style.listStyleType = 'none';
            list.style.paddingLeft = '0';
        });

        preview.querySelectorAll('.pv-r-name, .pv-r-title, .pv-sk-col-title').forEach((el) => {
            el.style.color = this.titleColor;
        });
        preview.querySelectorAll('.pv-r-role, .pv-exp-role, .pv-exp-company, .pv-exp-date, .pv-edu-year, .pv-edu-degree').forEach((el) => {
            el.style.color = this.subtitleColor;
        });

        const photo = preview.querySelector('.pv-photo');
        if (photo) {
            this.applyPhotoShapeStyles(photo, textColor);
        }

        this.applyScaledFontSizes(preview);
        Object.entries(this.elementStyleMap || {}).forEach(([key, style]) => {
            const target = this.getStyleTargetElement(key, preview);
            if (!target) return;
            this.applyElementStyle(target, style);
        });
    }

    resetStyleTargetInlineStyles(preview) {
        if (!preview) return;
        preview.querySelectorAll('[data-style-target="1"]').forEach((el) => {
            el.style.removeProperty('color');
            el.style.removeProperty('background-color');
            el.style.removeProperty('font-size');
            el.style.removeProperty('line-height');
            el.style.removeProperty('font-weight');
            el.style.removeProperty('font-style');
            el.style.removeProperty('text-decoration');
            el.style.removeProperty('text-align');
            el.style.removeProperty('width');
            el.style.removeProperty('height');
            el.style.removeProperty('display');
            el.style.removeProperty('transform');
        });
    }

    applyPhotoShapeStyles(photo, textColor = '#ffffff') {
        if (!photo) return;
        photo.style.display = 'block';
        photo.style.objectFit = 'cover';
        photo.style.objectPosition = 'center center';

        if (this.photoShape === 'circle') {
            photo.style.width = 'min(188px, calc(100% - 28px))';
            photo.style.height = 'min(188px, calc(100% - 28px))';
            photo.style.maxWidth = '100%';
            photo.style.margin = '18px auto 8px';
            photo.style.borderRadius = '9999px';
            photo.style.border = `4px solid ${textColor}`;
            return;
        }

        photo.style.width = '100%';
        photo.style.height = '250px';
        photo.style.maxWidth = '100%';
        photo.style.margin = '0';
        photo.style.borderRadius = '0';
        photo.style.border = '0';
    }

    applyScaledFontSizes(preview) {
        if (!preview) return;
        const targets = this.getScalableTextTargets(preview);
        targets.forEach((el) => {
            const computed = window.getComputedStyle(el);
            if (!el.dataset.scaleBaseFontPx) {
                const baseFont = parseFloat(computed.fontSize || '0');
                if (Number.isFinite(baseFont) && baseFont > 0) {
                    el.dataset.scaleBaseFontPx = String(baseFont);
                }
            }

            const baseFontPx = parseFloat(el.dataset.scaleBaseFontPx || '0');
            if (Number.isFinite(baseFontPx) && baseFontPx > 0) {
                const nextFontPx = (baseFontPx * this.fontScale).toFixed(2);
                el.style.fontSize = `${nextFontPx}px`;
            }

            if (!el.dataset.scaleBaseLinePx) {
                const lineHeightRaw = computed.lineHeight || '';
                if (/px$/i.test(lineHeightRaw)) {
                    const baseLine = parseFloat(lineHeightRaw);
                    if (Number.isFinite(baseLine) && baseLine > 0) {
                        el.dataset.scaleBaseLinePx = String(baseLine);
                    }
                }
            }

            const baseLinePx = parseFloat(el.dataset.scaleBaseLinePx || '0');
            if (Number.isFinite(baseLinePx) && baseLinePx > 0) {
                const nextLinePx = Math.max(baseLinePx * this.fontScale * this.lineHeightScale, 1).toFixed(2);
                el.style.lineHeight = `${nextLinePx}px`;
            }
        });
    }

    getScalableTextTargets(preview) {
        if (!preview) return [];
        const selectors = [
            '[data-autofit]',
            '.pv-l-title',
            '.pv-r-title',
            '.pv-sk-col-title',
            '.pv-r-name',
            '.pv-r-role',
            '.pv-l-item i',
            '.pv-edu-li',
            '.pv-exp-date',
            '.pv-exp-role',
            '.pv-exp-company',
            '.pv-l-text'
        ];
        const seen = new Set();
        selectors.forEach((selector) => {
            preview.querySelectorAll(selector).forEach((el) => seen.add(el));
        });
        return Array.from(seen);
    }

    updateCustomControlsUi() {
        if (!this.activeStyleTargetKey && this.moveModeEnabled) {
            this.moveModeEnabled = false;
        }
        const colorInput = this.querySelector('[data-action="left-color-input"]');
        if (colorInput && this.leftColumnColor) {
            const nextColor = this.normalizeHexColor(this.leftColumnColor);
            if (nextColor && colorInput.value !== nextColor) {
                colorInput.value = nextColor;
            }
        }
        const typographySelect = this.querySelector('[data-action="typography-select"]');
        if (typographySelect && typographySelect.value !== this.typographyKey) {
            typographySelect.value = this.typographyKey;
        }

        const leftWidthInput = this.querySelector('[data-action="left-width-input"]');
        if (leftWidthInput) {
            const expected = String(this.leftColumnWidth);
            if (leftWidthInput.value !== expected) {
                leftWidthInput.value = expected;
            }
        }
        const leftWidthLabel = this.querySelector('[data-left-col-width-label]');
        if (leftWidthLabel) {
            leftWidthLabel.textContent = `${this.leftColumnWidth}%`;
        }

        const shapeSelect = this.querySelector('[data-action="photo-shape-select"]');
        if (shapeSelect && shapeSelect.value !== this.photoShape) {
            shapeSelect.value = this.photoShape;
        }

        const editor = this.querySelector('[data-style-editor]');
        if (editor) {
            editor.classList.toggle('hidden', !this.styleEditorVisible);
        }

        const activeStyle = this.activeStyleTargetKey ? (this.elementStyleMap[this.activeStyleTargetKey] || {}) : null;
        const toggleState = (action, enabled) => {
            const btn = this.querySelector(`[data-action="${action}"]`);
            if (!btn) return;
            btn.classList.toggle('bg-sky-700', Boolean(enabled));
            btn.classList.toggle('text-white', Boolean(enabled));
            btn.classList.toggle('border-sky-700', Boolean(enabled));
        };
        toggleState('style-bold', Boolean(activeStyle?.bold));
        toggleState('style-italic', Boolean(activeStyle?.italic));
        toggleState('style-underline', Boolean(activeStyle?.underline));
        toggleState('style-toggle-move', Boolean(this.moveModeEnabled && this.activeStyleTargetKey));
        this.syncStyleEditorControls();
        this.refreshVisibleHotspotSelection();
    }

    getContrastingTextColor(hexColor) {
        const hex = this.normalizeHexColor(hexColor) || '#2f3e4e';
        const r = parseInt(hex.slice(1, 3), 16);
        const g = parseInt(hex.slice(3, 5), 16);
        const b = parseInt(hex.slice(5, 7), 16);
        const luminance = ((r * 299) + (g * 587) + (b * 114)) / 1000;
        return luminance >= 156 ? '#0f172a' : '#ffffff';
    }

    buildPageSlices(canvas, maxPageHeightPx, textFlow = null) {
        if (!canvas || !maxPageHeightPx) return [];
        const scaledFlow = this.scaleTextFlowForCanvas(textFlow, canvas.height);
        const slices = [];
        let renderedPx = 0;
        while (renderedPx < canvas.height) {
            const sliceHeightPx = this.findSmartSliceHeight(canvas, renderedPx, maxPageHeightPx, scaledFlow);
            if (!sliceHeightPx || sliceHeightPx < 1) break;
            slices.push(sliceHeightPx);
            renderedPx += sliceHeightPx;
        }
        if (!slices.length) {
            slices.push(Math.max(1, Math.min(maxPageHeightPx, canvas.height)));
        }
        return slices;
    }

    findSmartSliceHeight(canvas, startY, maxSliceHeightPx, textFlow = null) {
        const remaining = canvas.height - startY;
        if (remaining <= maxSliceHeightPx) return remaining;
        const blockRanges = Array.isArray(textFlow?.blockRanges) ? textFlow.blockRanges : [];

        // Evite les fins de page trop "vides" : on coupe pres du bas par defaut.
        const minSliceHeightPx = Math.max(760, Math.floor(maxSliceHeightPx * 0.78));
        const hardMinSlicePx = Math.max(120, Math.floor(maxSliceHeightPx * 0.2));
        const idealEndY = startY + maxSliceHeightPx;
        const minEndY = startY + minSliceHeightPx;
        if (minEndY >= idealEndY - 10) {
            const forcedCut = this.adjustCutForBlocks(idealEndY, startY, blockRanges, maxSliceHeightPx);
            if (forcedCut === null) return maxSliceHeightPx;
            return Math.max(hardMinSlicePx, Math.min(maxSliceHeightPx, forcedCut - startY));
        }
        const maxBottomGapPx = Math.max(48, Math.floor(maxSliceHeightPx * 0.08));

        const safeCut = this.findNearestSafeCut(textFlow, minEndY, idealEndY);
        if (safeCut !== null) {
            const adjustedSafeCut = this.adjustCutForBlocks(safeCut, startY, blockRanges, maxSliceHeightPx);
            if (adjustedSafeCut === null) return maxSliceHeightPx;
            const safeHeight = adjustedSafeCut - startY;
            const bottomGap = idealEndY - adjustedSafeCut;
            if (safeHeight >= hardMinSlicePx && bottomGap <= maxBottomGapPx) {
                return Math.min(maxSliceHeightPx, safeHeight);
            }
        }

        const scanBackPx = Math.min(160, idealEndY - minEndY);
        const searchStartY = idealEndY - scanBackPx;
        const searchEndY = idealEndY - 8;
        if (searchEndY <= searchStartY) return maxSliceHeightPx;

        const xStart = Math.floor(canvas.width * 0.34);
        const xEnd = Math.floor(canvas.width * 0.985);
        const scanWidth = Math.max(8, xEnd - xStart);
        const scanHeight = Math.max(1, searchEndY - searchStartY);

        const ctx = canvas.getContext('2d', { willReadFrequently: true }) || canvas.getContext('2d');
        if (!ctx) return maxSliceHeightPx;

        let bestLocalRow = -1;
        let nearBottomRow = -1;
        let bestScore = Number.POSITIVE_INFINITY;
        const xStep = 3;
        const nearWhiteThreshold = 0.012;

        try {
            const strip = ctx.getImageData(xStart, searchStartY, scanWidth, scanHeight).data;
            for (let row = scanHeight - 1; row >= 0; row -= 1) {
                let nonWhite = 0;
                let samples = 0;
                for (let x = 0; x < scanWidth; x += xStep) {
                    const idx = (row * scanWidth + x) * 4;
                    const r = strip[idx];
                    const g = strip[idx + 1];
                    const b = strip[idx + 2];
                    const a = strip[idx + 3];
                    if (a > 10) {
                        samples += 1;
                        if (r < 242 || g < 242 || b < 242) {
                            nonWhite += 1;
                        }
                    }
                }
                if (!samples) continue;
                const score = nonWhite / samples;
                if (nearBottomRow < 0 && score <= nearWhiteThreshold) {
                    nearBottomRow = row;
                    break;
                }
                if (score < bestScore || (Math.abs(score - bestScore) < 0.0001 && row > bestLocalRow)) {
                    bestScore = score;
                    bestLocalRow = row;
                }
            }
        } catch (error) {
            return maxSliceHeightPx;
        }

        const chosenRow = nearBottomRow >= 0 ? nearBottomRow : bestLocalRow;
        if (chosenRow < 0 || bestScore > 0.075) return maxSliceHeightPx;

        const breakY = searchStartY + chosenRow;
        const adjustedBreakY = this.adjustCutForBlocks(breakY, startY, blockRanges, maxSliceHeightPx);
        if (adjustedBreakY === null) return maxSliceHeightPx;
        const sliceHeight = adjustedBreakY - startY;
        if (sliceHeight < hardMinSlicePx) {
            const fallbackCut = this.adjustCutForBlocks(idealEndY, startY, blockRanges, maxSliceHeightPx);
            if (fallbackCut === null) return maxSliceHeightPx;
            return Math.max(hardMinSlicePx, Math.min(maxSliceHeightPx, fallbackCut - startY));
        }
        return Math.min(maxSliceHeightPx, sliceHeight);
    }

    adjustCutForBlocks(cutY, startY, blockRanges = [], maxSliceHeightPx = 0) {
        if (!Number.isFinite(cutY)) return cutY;
        if (!Array.isArray(blockRanges) || !blockRanges.length) return Math.floor(cutY);

        const originalCut = Math.floor(cutY);
        let adjustedCut = originalCut;
        let guard = 0;

        while (guard < 30) {
            const crossing = this.findCrossingBlock(blockRanges, adjustedCut);
            if (!crossing) break;

            const nextCut = crossing.top - 2;
            // Si le bloc commence en tete de page, on tente de couper APRES ce bloc.
            if (nextCut <= startY + 2) {
                const afterBlockCut = crossing.bottom + 2;
                const maxAllowedCut = startY + (Number.isFinite(maxSliceHeightPx) ? maxSliceHeightPx : 0);
                if (afterBlockCut > startY + 2 && (!maxAllowedCut || afterBlockCut <= maxAllowedCut)) {
                    adjustedCut = afterBlockCut;
                    break;
                }
                return null;
            }

            adjustedCut = nextCut;
            guard += 1;
        }

        return Math.max(startY + 2, adjustedCut);
    }

    findCrossingBlock(blockRanges, cutY) {
        if (!Array.isArray(blockRanges) || !blockRanges.length) return null;
        for (let i = 0; i < blockRanges.length; i += 1) {
            const range = blockRanges[i];
            if (cutY < range.top - 3) break;
            if (cutY <= range.bottom + 3) return range;
        }
        return null;
    }

    normalizeBlockRanges(ranges = []) {
        if (!Array.isArray(ranges) || !ranges.length) return [];
        const sorted = ranges
            .filter((item) => Number.isFinite(item?.top) && Number.isFinite(item?.bottom) && item.bottom > item.top)
            .map((item) => ({ top: Math.floor(item.top), bottom: Math.floor(item.bottom) }))
            .sort((a, b) => (a.top - b.top) || (a.bottom - b.bottom));
        if (!sorted.length) return [];

        const merged = [{ ...sorted[0] }];
        for (let i = 1; i < sorted.length; i += 1) {
            const current = sorted[i];
            const prev = merged[merged.length - 1];
            if (current.top <= prev.bottom + 2) {
                prev.bottom = Math.max(prev.bottom, current.bottom);
            } else {
                merged.push({ ...current });
            }
        }
        return merged;
    }

    findNearestSafeCut(textFlow, minEndY, idealEndY) {
        if (!textFlow) return null;
        const density = textFlow.density instanceof Uint16Array ? textFlow.density : null;
        const boundaries = textFlow.boundaries instanceof Uint8Array ? textFlow.boundaries : null;
        if (!density || !boundaries || !density.length || !boundaries.length) return null;

        const startY = Math.max(2, Math.floor(minEndY));
        const endY = Math.min(Math.min(density.length, boundaries.length) - 3, Math.floor(idealEndY));
        if (endY <= startY) return null;

        let bestRow = null;
        let bestScore = Number.POSITIVE_INFINITY;
        for (let y = endY; y >= startY; y -= 1) {
            if (!boundaries[y]) continue;
            const score = density[y - 1] + density[y] + density[y + 1];
            if (score === 0) return y;
            if (score < bestScore) {
                bestScore = score;
                bestRow = y;
            }
        }

        for (let y = endY; y >= startY; y -= 1) {
            if (density[y - 1] === 0 && density[y] === 0 && density[y + 1] === 0) return y;
        }

        if (bestRow !== null) return bestRow;
        return null;
    }

    collectTextRowOccupancy(rootNode, maxY) {
        if (!rootNode) return null;
        const rootRect = rootNode.getBoundingClientRect();
        const maxAllowed = Math.max(256, Math.floor(maxY || rootNode.scrollHeight || 0) + 2);
        const density = new Uint16Array(maxAllowed + 2);
        const boundaries = new Uint8Array(maxAllowed + 2);
        const blockRanges = [];
        const addBlockRange = (rect, options = {}) => {
            if (!rect || rect.width < 4 || rect.height < 6) return;
            const padPx = Number.isFinite(options.padPx) ? Math.max(0, Math.floor(options.padPx)) : 4;
            const maxHeightRatio = Number.isFinite(options.maxHeightRatio) ? options.maxHeightRatio : 0.55;
            const top = Math.max(0, Math.floor(rect.top - rootRect.top) - padPx);
            const bottom = Math.min(maxAllowed, Math.ceil(rect.bottom - rootRect.top) - 1 + padPx);
            if (bottom <= top) return;
            const height = bottom - top + 1;
            if (maxHeightRatio > 0 && height > Math.floor(maxAllowed * maxHeightRatio)) return;
            blockRanges.push({ top, bottom });
        };

        const walker = document.createTreeWalker(
            rootNode,
            NodeFilter.SHOW_TEXT,
            {
                acceptNode: (node) => {
                    if (!node || !node.nodeValue || !node.nodeValue.trim()) return NodeFilter.FILTER_REJECT;
                    const parent = node.parentElement;
                    if (!parent) return NodeFilter.FILTER_REJECT;
                    const style = window.getComputedStyle(parent);
                    if (style.display === 'none' || style.visibility === 'hidden') return NodeFilter.FILTER_REJECT;
                    return NodeFilter.FILTER_ACCEPT;
                }
            }
        );

        while (walker.nextNode()) {
            const textNode = walker.currentNode;
            const range = document.createRange();
            range.selectNodeContents(textNode);
            const rects = Array.from(range.getClientRects());
            rects.forEach((rect) => {
                if (!rect || rect.height < 3 || rect.width < 2) return;
                const top = Math.max(0, Math.floor(rect.top - rootRect.top));
                const bottom = Math.min(maxAllowed, Math.ceil(rect.bottom - rootRect.top) - 1);
                if (bottom < top) return;
                for (let y = top; y <= bottom; y += 1) {
                    density[y] = Math.min(65535, density[y] + 1);
                }
                const cutBefore = Math.max(1, top - 1);
                const cutAfter = Math.min(maxAllowed, bottom + 1);
                boundaries[cutBefore] = 1;
                boundaries[cutAfter] = 1;
            });
            range.detach();
        }

        rootNode.querySelectorAll('[data-edit-block]').forEach((el) => {
            addBlockRange(el.getBoundingClientRect(), { padPx: 6, maxHeightRatio: 0.55 });
        });

        // Evite surtout les coupures dans les lignes d'experience/education.
        rootNode.querySelectorAll('.pv-exp-item, .pv-edu-li').forEach((el) => {
            addBlockRange(el.getBoundingClientRect(), { padPx: 10, maxHeightRatio: 0.92 });
        });

        return {
            density,
            boundaries,
            blockRanges: this.normalizeBlockRanges(blockRanges),
            sourceHeight: maxAllowed + 1
        };
    }

    scaleTextFlowForCanvas(textFlow, canvasHeight) {
        if (!textFlow || !(textFlow.density instanceof Uint16Array) || !(textFlow.boundaries instanceof Uint8Array)) {
            return null;
        }
        const sourceHeight = Math.max(1, Number(textFlow.sourceHeight || textFlow.density.length - 1));
        const targetHeight = Math.max(1, Math.floor(canvasHeight));
        if (Math.abs(sourceHeight - targetHeight) <= 1) {
            return textFlow;
        }

        const scaledDensity = new Uint16Array(targetHeight + 2);
        const scaledBoundaries = new Uint8Array(targetHeight + 2);
        const scaledBlockRanges = [];
        const ratio = targetHeight / sourceHeight;
        const maxSrc = Math.min(sourceHeight, textFlow.density.length - 1);

        for (let y = 0; y <= maxSrc; y += 1) {
            const mapped = Math.max(0, Math.min(targetHeight + 1, Math.round(y * ratio)));
            if (textFlow.density[y] > 0) {
                scaledDensity[mapped] = Math.min(65535, scaledDensity[mapped] + textFlow.density[y]);
                if (mapped + 1 <= targetHeight + 1) {
                    scaledDensity[mapped + 1] = Math.min(65535, scaledDensity[mapped + 1] + textFlow.density[y]);
                }
            }
            if (textFlow.boundaries[y]) {
                scaledBoundaries[mapped] = 1;
            }
        }

        if (Array.isArray(textFlow.blockRanges) && textFlow.blockRanges.length) {
            textFlow.blockRanges.forEach((range) => {
                if (!Number.isFinite(range?.top) || !Number.isFinite(range?.bottom)) return;
                const top = Math.max(0, Math.min(targetHeight, Math.floor(range.top * ratio) - 6));
                const bottom = Math.max(top + 1, Math.min(targetHeight + 1, Math.ceil(range.bottom * ratio) + 6));
                if (bottom <= top) return;
                scaledBlockRanges.push({ top, bottom });
            });
        }

        return {
            density: scaledDensity,
            boundaries: scaledBoundaries,
            blockRanges: this.normalizeBlockRanges(scaledBlockRanges),
            sourceHeight: targetHeight
        };
    }

    collectEditHotspots(rootNode, maxY) {
        if (!rootNode) return [];
        const rootRect = rootNode.getBoundingClientRect();
        const maxAllowedY = Math.max(0, Math.floor(maxY || rootNode.scrollHeight || 0));
        const seen = new Set();
        const hotspots = [];

        rootNode.querySelectorAll('[data-style-target="1"]').forEach((el) => {
            const styleKey = (el.getAttribute('data-style-key') || '').trim();
            if (!styleKey) return;
            const nearestStep = el.getAttribute('data-edit-step') || el.closest('[data-edit-step]')?.getAttribute('data-edit-step') || '';
            const step = String(nearestStep || '').trim();

            const rect = el.getBoundingClientRect();
            if (!rect || rect.width < 4 || rect.height < 4) return;

            const x = Math.max(0, rect.left - rootRect.left);
            const y = Math.max(0, rect.top - rootRect.top);
            const width = Math.min(rootRect.width - x, rect.width);
            const height = Math.min(maxAllowedY - y, rect.height);
            if (width < 4 || height < 4) return;

            const key = `${styleKey}:${Math.round(x)}:${Math.round(y)}:${Math.round(width)}:${Math.round(height)}`;
            if (seen.has(key)) return;
            seen.add(key);
            hotspots.push({
                step,
                styleKey,
                label: (el.getAttribute('data-style-label') || '').trim(),
                x,
                y,
                width,
                height
            });
        });

        return hotspots;
    }

    appendPageHotspots(layer, options) {
        if (!layer) return;
        const {
            hotspots = [],
            pageStartPx = 0,
            pageEndPx = 0,
            pageCanvasWidthPx = 1,
            pageCanvasHeightPx = 1,
            marginXPx = 0,
            marginYPx = 0,
            drawWidthPx = 1
        } = options || {};
        if (!Array.isArray(hotspots) || !hotspots.length) return;

        const scale = drawWidthPx / pageCanvasWidthPx;
        const maxRight = marginXPx + drawWidthPx;
        const maxBottom = pageCanvasHeightPx - marginYPx;

        hotspots.forEach((spot) => {
            const spotTop = spot.y;
            const spotBottom = spot.y + spot.height;
            if (spotBottom <= pageStartPx || spotTop >= pageEndPx) return;

            const visibleTop = Math.max(spotTop, pageStartPx);
            const visibleBottom = Math.min(spotBottom, pageEndPx);
            const visibleHeight = (visibleBottom - visibleTop) * scale;
            if (visibleHeight < 3) return;

            const leftPx = marginXPx + (spot.x * scale);
            const topPx = marginYPx + ((visibleTop - pageStartPx) * scale);
            const widthPx = spot.width * scale;

            const clampedLeft = Math.max(marginXPx, Math.min(leftPx, maxRight - 4));
            const clampedTop = Math.max(marginYPx, Math.min(topPx, maxBottom - 4));
            const clampedWidth = Math.max(4, Math.min(widthPx, maxRight - clampedLeft));
            const clampedHeight = Math.max(4, Math.min(visibleHeight, maxBottom - clampedTop));
            if (clampedWidth < 4 || clampedHeight < 4) return;

            const zone = document.createElement('div');
            zone.className = 'absolute z-[2] cursor-pointer bg-sky-300/0 transition-colors duration-150 hover:bg-sky-300/20';
            if (spot.styleKey && spot.styleKey === this.activeStyleTargetKey) {
                zone.className += ' ring-2 ring-sky-500 ring-inset bg-sky-300/20';
            }
            zone.style.left = `${(clampedLeft / pageCanvasWidthPx) * 100}%`;
            zone.style.top = `${(clampedTop / pageCanvasHeightPx) * 100}%`;
            zone.style.width = `${(clampedWidth / pageCanvasWidthPx) * 100}%`;
            zone.style.height = `${(clampedHeight / pageCanvasHeightPx) * 100}%`;
            zone.setAttribute('data-edit-step', spot.step);
            zone.setAttribute('data-style-hotspot', '1');
            if (spot.styleKey) zone.setAttribute('data-style-key', spot.styleKey);
            if (spot.label) zone.setAttribute('title', spot.label);
            const renderedStyle = spot.styleKey ? (this.elementStyleMap[spot.styleKey] || {}) : null;
            zone.dataset.renderedOffsetX = String(Math.round(Number(renderedStyle?.offsetXPx || 0)));
            zone.dataset.renderedOffsetY = String(Math.round(Number(renderedStyle?.offsetYPx || 0)));
            zone.dataset.hotspotScale = String(scale);
            layer.appendChild(zone);
        });
    }

    async renderPaginatedPreview(options = {}) {
        const source = this.querySelector('#pv-preview');
        const host = this.querySelector('[data-preview-pages]');
        if (!source || !host || typeof window.html2canvas !== 'function') return;
        const showLoading = Boolean(options?.showLoading);
        const busyToken = this.startBusyIndicator('Mise a jour en cours...', 220);

        const token = Symbol('pv-pages-render');
        this.pagesRenderToken = token;
        if (showLoading && !host.children.length) {
            host.innerHTML = '<div class="shrink-0 snap-start w-[254px] rounded-lg border border-slate-300 bg-white px-3 py-2 text-center text-xs text-slate-500 sm:w-[349px] md:w-[476px] lg:w-[635px] xl:w-[794px]">Generation de la previsualisation paginee...</div>';
        }

        let exportNode = null;
        try {
            const exportWidth = 794;
            const pageWidthMm = 210;
            const pageHeightMm = 297;
            const marginXmm = 6;
            const marginYmm = 0;
            const contentWidthMm = pageWidthMm - (marginXmm * 2);
            const contentHeightMm = pageHeightMm - (marginYmm * 2);

            exportNode = source.cloneNode(true);
            exportNode.id = 'pv-preview-pages-export';
            exportNode.style.position = 'fixed';
            exportNode.style.left = '-10000px';
            exportNode.style.top = '0';
            exportNode.style.width = `${exportWidth}px`;
            exportNode.style.height = 'auto';
            exportNode.style.transform = 'none';
            exportNode.style.transformOrigin = 'top left';
            exportNode.style.maxWidth = 'none';
            exportNode.style.maxHeight = 'none';
            exportNode.style.pointerEvents = 'none';
            exportNode.style.zIndex = '-1';
            document.body.appendChild(exportNode);
            this.applyStylesToPreviewNode(exportNode);

            await this.waitForExportAssets(exportNode);
            const contentHeightPx = Math.max(exportNode.scrollHeight, exportNode.offsetHeight, 1123);
            exportNode.style.height = `${contentHeightPx}px`;
            const textOccupancy = this.collectTextRowOccupancy(exportNode, contentHeightPx);
            const editHotspots = this.collectEditHotspots(exportNode, contentHeightPx);

            const canvas = await window.html2canvas(exportNode, {
                scale: 1,
                useCORS: true,
                backgroundColor: '#ffffff',
                width: exportWidth,
                height: contentHeightPx,
                windowWidth: exportWidth,
                windowHeight: contentHeightPx,
                scrollX: 0,
                scrollY: 0
            });

            if (exportNode.parentElement) exportNode.parentElement.removeChild(exportNode);
            if (this.pagesRenderToken !== token) return;

            const pageHeightPx = Math.floor((contentHeightMm / contentWidthMm) * canvas.width);
            const pageCanvasHeightPx = Math.round((pageHeightMm / pageWidthMm) * canvas.width);
            const marginXPx = Math.round((marginXmm / pageWidthMm) * canvas.width);
            const marginYPx = Math.round((marginYmm / pageWidthMm) * canvas.width);
            const drawWidthPx = canvas.width - (marginXPx * 2);

            const slices = this.buildPageSlices(canvas, pageHeightPx, textOccupancy);
            const nextPageNodes = [];
            let renderedPx = 0;
            let pageNumber = 1;
            for (const sliceHeightPx of slices) {
                const sliceCanvas = document.createElement('canvas');
                sliceCanvas.width = canvas.width;
                sliceCanvas.height = sliceHeightPx;
                const sliceCtx = sliceCanvas.getContext('2d');
                if (!sliceCtx) break;

                sliceCtx.drawImage(
                    canvas,
                    0,
                    renderedPx,
                    canvas.width,
                    sliceHeightPx,
                    0,
                    0,
                    canvas.width,
                    sliceHeightPx
                );

                const pageCanvas = document.createElement('canvas');
                pageCanvas.width = canvas.width;
                pageCanvas.height = pageCanvasHeightPx;
                const pageCtx = pageCanvas.getContext('2d');
                if (!pageCtx) break;
                pageCtx.fillStyle = '#ffffff';
                pageCtx.fillRect(0, 0, pageCanvas.width, pageCanvas.height);
                const drawHeightPx = Math.round((sliceHeightPx / canvas.width) * drawWidthPx);
                pageCtx.drawImage(sliceCanvas, marginXPx, marginYPx, drawWidthPx, drawHeightPx);

                const pageWrap = document.createElement('div');
                pageWrap.className = 'shrink-0 snap-start flex flex-col items-center gap-1';
                const pageLabel = document.createElement('p');
                pageLabel.className = 'text-[11px] font-semibold uppercase tracking-wide text-slate-500';
                pageLabel.textContent = `Page ${pageNumber}`;
                const pageFrame = document.createElement('div');
                pageFrame.className = 'relative w-[254px] sm:w-[349px] md:w-[476px] lg:w-[635px] xl:w-[794px]';
                const pageImg = document.createElement('img');
                pageImg.src = pageCanvas.toDataURL('image/png', 1.0);
                pageImg.alt = `Previsualisation page ${pageNumber}`;
                pageImg.className = 'h-auto w-full box-border border border-slate-300 bg-white shadow-sm';
                const hotspotLayer = document.createElement('div');
                hotspotLayer.className = 'absolute inset-0 z-[2]';
                this.appendPageHotspots(hotspotLayer, {
                    hotspots: editHotspots,
                    pageStartPx: renderedPx,
                    pageEndPx: renderedPx + sliceHeightPx,
                    pageCanvasWidthPx: pageCanvas.width,
                    pageCanvasHeightPx: pageCanvas.height,
                    marginXPx,
                    marginYPx,
                    drawWidthPx
                });
                pageFrame.appendChild(pageImg);
                pageFrame.appendChild(hotspotLayer);

                pageWrap.appendChild(pageLabel);
                pageWrap.appendChild(pageFrame);
                nextPageNodes.push(pageWrap);

                renderedPx += sliceHeightPx;
                pageNumber += 1;
            }
            if (this.pagesRenderToken !== token) return;
            if (nextPageNodes.length) {
                host.replaceChildren(...nextPageNodes);
            }
            this.refreshVisibleHotspotSelection();
        } catch (error) {
            console.error('Echec rendu pagine:', error);
            if (!host.children.length) {
                host.innerHTML = '<div class="shrink-0 snap-start w-[254px] rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-center text-xs text-red-700 sm:w-[349px] md:w-[476px] lg:w-[635px] xl:w-[794px]">Impossible de generer la previsualisation paginee.</div>';
            }
        } finally {
            if (exportNode && exportNode.parentElement) {
                exportNode.parentElement.removeChild(exportNode);
            }
            this.stopBusyIndicator(busyToken);
        }
    }

    resolveDataWithDefaults() {
        const output = { ...this.defaultData };
        Object.keys(this.defaultData).forEach((key) => {
            const incoming = this.data[key];
            if (Array.isArray(incoming) && incoming.length > 0) {
                output[key] = incoming;
            } else if (typeof incoming === 'string' && incoming.trim().length > 0) {
                output[key] = incoming.trim();
            }
        });
        return output;
    }

    setText(key, value) {
        const el = this.querySelector(`[data-preview="${key}"]`);
        if (el) el.textContent = value || '';
    }

    renderList(listKey, items) {
        const list = this.querySelector(`[data-preview-list="${listKey}"]`);
        if (!list) return;
        list.innerHTML = '';
        const stepByList = {
            interests: 'interests',
            languages: 'languages',
            softwares: 'tools'
        };
        const targetStep = stepByList[listKey] || '';
        items.forEach((item) => {
            const li = document.createElement('li');
            if (listKey === 'interests') {
                li.className = 'pv-l-item mb-2 flex items-center justify-center gap-2 break-words text-[14px] leading-[1.6] opacity-90';
                li.setAttribute('data-autofit', '');
                li.setAttribute('data-autofit-min', '8');
            }
            if (targetStep) {
                li.setAttribute('data-edit-step', targetStep);
                li.setAttribute('data-edit-block', '');
                li.classList.add('cursor-pointer');
            }
            li.textContent = item;
            list.appendChild(li);
        });
    }

    splitToItems(value) {
        if (Array.isArray(value)) {
            return value
                .map((item) => (typeof item === 'string' ? item.trim() : ''))
                .filter(Boolean);
        }
        return (value || '')
            .split(',')
            .map((item) => item.trim())
            .filter(Boolean);
    }

    buildExperienceBullets(data) {
        const raw = (data.exp_description || '').trim();
        const parts = raw
            .split(/\n+/)
            .map((s) => s.trim())
            .filter(Boolean);
        if (!parts.length) return ['Description de l experience a renseigner.'];
        return parts.slice(0, 6);
    }

    buildExperienceItems(data) {
        if (Array.isArray(data.experiences) && data.experiences.length) {
            const rows = data.experiences
                .map((item) => {
                    if (!item || typeof item !== 'object') return null;
                    const title = (item.title || '').trim();
                    const organization = (item.organization || '').trim();
                    const location = (item.location || '').trim();
                    const startDate = (item.start_date || '').trim();
                    const endDate = (item.end_date || '').trim();
                    const description = (item.description || '').trim();
                    if (!title && !organization && !location && !startDate && !endDate && !description) return null;
                    return {
                        title,
                        organization,
                        location,
                        dates: this.formatDateRange(startDate, endDate),
                        bullets: this.buildBulletsFromText(description)
                    };
                })
                .filter(Boolean);
            if (rows.length) return rows;
        }

        return [{
            title: (data.exp_title || '').trim() || 'Poste',
            organization: (data.exp_organization || '').trim() || 'Entreprise',
            location: (data.exp_location || '').trim() || 'Lieu',
            dates: this.formatDateRange(data.exp_start_date, data.exp_end_date) || 'Periode',
            bullets: this.buildExperienceBullets(data)
        }];
    }

    buildBulletsFromText(rawText) {
        const parts = (rawText || '')
            .split(/\n+/)
            .map((s) => s.trim())
            .filter(Boolean);
        if (!parts.length) return ['Description de l experience a renseigner.'];
        return parts.slice(0, 6);
    }

    renderExperienceList(items) {
        const list = this.querySelector('[data-preview-list="exp_bullets"]');
        const role = this.querySelector('[data-preview="exp_title"]');
        const org = this.querySelector('[data-preview="exp_organization"]');
        const loc = this.querySelector('[data-preview="exp_location"]');
        const dates = this.querySelector('[data-preview="exp_dates"]');
        if (!list || !role || !org || !loc || !dates) return;

        const first = items[0] || {};
        role.textContent = first.title || 'Poste';
        org.textContent = first.organization || 'Entreprise';
        loc.textContent = first.location || 'Lieu';
        dates.textContent = first.dates || 'Periode';

        const target = role.closest('.pv-exp-content');
        const article = role.closest('.pv-exp-item');
        if (!target || !article) return;

        target.querySelectorAll('.pv-exp-extra').forEach((node) => node.remove());
        list.innerHTML = '';
        (first.bullets || []).forEach((item) => {
            const li = document.createElement('li');
            li.setAttribute('data-edit-step', 'experiences');
            li.setAttribute('data-edit-block', '');
            li.classList.add('cursor-pointer');
            li.textContent = item;
            list.appendChild(li);
        });

        if (items.length > 1) {
            items.slice(1).forEach((item) => {
                const extra = document.createElement('article');
                extra.className = 'pv-exp-item pv-exp-extra mt-3 flex gap-[14px]';
                extra.setAttribute('data-edit-step', 'experiences');
                extra.setAttribute('data-edit-block', '');
                extra.innerHTML = `
                    <div class="pv-exp-date w-[110px] shrink-0 cursor-pointer text-[14px] font-semibold hover:opacity-90" data-edit-step="experiences" data-edit-block data-autofit data-autofit-min="8">${item.dates || 'Periode'}</div>
                    <div class="pv-exp-content flex-1">
                        <p class="pv-exp-role m-0 cursor-pointer break-words text-[14px] font-semibold uppercase hover:opacity-90" data-edit-step="experiences" data-edit-block data-autofit data-autofit-min="8">${item.title || 'Poste'}</p>
                        <p class="pv-exp-company m-0 mt-0.5 cursor-pointer break-words text-[14px] font-medium hover:opacity-90" data-edit-step="experiences" data-edit-block data-autofit data-autofit-min="8">${item.organization || 'Entreprise'} - ${item.location || 'Lieu'}</p>
                        <ul class="pv-exp-list mb-0 mt-2 list-disc pl-[18px] text-[14px] leading-[1.5]" data-edit-step="experiences" data-edit-block data-autofit data-autofit-min="8"></ul>
                    </div>
                `;
                const bulletList = extra.querySelector('.pv-exp-list');
                (item.bullets || []).forEach((bullet) => {
                    const li = document.createElement('li');
                    li.setAttribute('data-edit-step', 'experiences');
                    li.setAttribute('data-edit-block', '');
                    li.classList.add('cursor-pointer');
                    li.textContent = bullet;
                    bulletList.appendChild(li);
                });
                article.parentElement.appendChild(extra);
            });
        }
    }

    buildInterestItems(data, rawData = {}) {
        if (Object.prototype.hasOwnProperty.call(rawData, 'interests')) {
            const explicitItems = this.splitToItems(rawData.interests);
            return explicitItems.length ? explicitItems : ['Interets a renseigner'];
        }
        const items = this.splitToItems(data.interests);
        return items.length ? items : ['Interets a renseigner'];
    }

    buildEducationItems(data) {
        if (Array.isArray(data.educations) && data.educations.length) {
            const rows = data.educations
                .map((item) => {
                    if (!item || typeof item !== 'object') return '';
                    const dates = this.formatDateRange(item.start_date, item.end_date);
                    const degree = (item.degree || '').trim();
                    const school = (item.school || '').trim();
                    const schoolAddress = (item.school_address || '').trim();
                    return { dates, degree, school, schoolAddress };
                })
                .filter((item) => item.degree || item.school || item.schoolAddress || item.dates);
            if (rows.length) return rows;
        }

        return [];
    }

    renderEducationList(items) {
        const list = this.querySelector('[data-preview-list="educations"]');
        if (!list) return;
        list.innerHTML = '';

        if (!items.length) {
            const li = document.createElement('li');
            li.className = 'pv-edu-li mb-[10px] list-item text-[14px] leading-[1.6]';
            li.setAttribute('data-edit-step', 'educations');
            li.setAttribute('data-edit-block', '');
            li.classList.add('cursor-pointer');
            li.setAttribute('data-autofit', '');
            li.setAttribute('data-autofit-min', '8');
            li.textContent = 'Formations a renseigner';
            list.appendChild(li);
            return;
        }

        items.forEach((item) => {
            const li = document.createElement('li');
            li.className = 'pv-edu-li mb-[10px] list-item break-words text-[14px] leading-[1.6]';
            li.setAttribute('data-edit-step', 'educations');
            li.setAttribute('data-edit-block', '');
            li.classList.add('cursor-pointer');
            li.setAttribute('data-autofit', '');
            li.setAttribute('data-autofit-min', '8');

            let hasContent = false;
            if (item.dates) {
                const dates = document.createElement('span');
                dates.className = 'pv-edu-year font-semibold';
                dates.textContent = item.dates;
                li.appendChild(dates);
                hasContent = true;
            }

            if (item.degree) {
                if (hasContent) li.appendChild(document.createTextNode(' - '));
                const degree = document.createElement('span');
                degree.className = 'pv-edu-degree font-semibold';
                degree.textContent = item.degree;
                li.appendChild(degree);
                hasContent = true;
            }

            if (item.school) {
                if (hasContent) li.appendChild(document.createTextNode(' - '));
                li.appendChild(document.createTextNode(item.school));
                hasContent = true;
            }

            if (item.schoolAddress) {
                if (hasContent) li.appendChild(document.createTextNode(' - '));
                li.appendChild(document.createTextNode(item.schoolAddress));
            }

            list.appendChild(li);
        });
    }

    buildLanguageItems(data) {
        if (Array.isArray(data.languages) && data.languages.length) {
            const rows = data.languages
                .map((item) => {
                    if (!item || typeof item !== 'object') return '';
                    const name = (item.name || '').trim();
                    const level = (item.level || '').trim();
                    if (!name) return '';
                    return level ? `${name} - ${level}` : name;
                })
                .filter(Boolean);
            if (rows.length) return rows;
        }

        return ['Langues a renseigner'];
    }

    buildSoftwareItems(data) {
        if (Array.isArray(data.tools) && data.tools.length) {
            const rows = data.tools
                .map((item) => {
                    if (!item || typeof item !== 'object') return '';
                    const type = (item.type || '').trim();
                    const name = (item.name || '').trim();
                    const level = (item.level || '').trim();
                    if (!name) return '';
                    return { type, text: `${name}${level ? ` (${level})` : ''}` };
                })
                .filter(Boolean);
            if (rows.length) {
                const selectedType = rows[0].type === 'Materiel' ? 'Materiel' : 'Logiciel';
                const filtered = rows.filter((row) => (row.type || 'Logiciel') === selectedType).map((row) => row.text);
                return {
                    title: selectedType === 'Materiel' ? 'Materiels' : 'Logiciels',
                    items: filtered.length ? filtered : ['A renseigner']
                };
            }
        }

        const fromSoftwares = this.splitToItems(data.softwares);
        if (fromSoftwares.length) {
            return {
                title: 'Logiciels',
                items: fromSoftwares
            };
        }

        return {
            title: 'Logiciels',
            items: ['Logiciels a renseigner']
        };
    }

    formatDateRange(start, end) {
        const startVal = start || '';
        const endVal = end || '';
        if (startVal && endVal) return `Debut: ${startVal} | Fin: ${endVal}`;
        if (startVal) return `Debut: ${startVal}`;
        if (endVal) return `Fin: ${endVal}`;
        return '';
    }

    formatPhoneDisplay(data) {
        if (Array.isArray(data.phones) && data.phones.length) {
            return data.phones.join(' / ');
        }
        return data.phoneno || '';
    }

    formatAddressDisplay(data) {
        const line1 = [data.address_number, data.address_street]
            .map((value) => (value || '').trim())
            .filter(Boolean)
            .join(' ');
        const line2 = [data.address_postal, data.address_city]
            .map((value) => (value || '').trim())
            .filter(Boolean)
            .join(' ');
        const country = (data.address_country || '').trim();

        const fromStructured = [line1, line2, country].filter(Boolean).join(', ');
        if (fromStructured) return fromStructured;
        return data.address || '';
    }

    setDownloadLoading(button, loading) {
        if (!button || !(button instanceof HTMLElement)) return;
        if (!button.dataset.defaultHtml) {
            button.dataset.defaultHtml = button.innerHTML;
        }

        button.disabled = loading;
        button.classList.toggle('opacity-70', loading);
        button.classList.toggle('cursor-not-allowed', loading);
        button.innerHTML = loading
            ? '<i class="fa-solid fa-spinner fa-spin"></i>Generation PDF...'
            : (button.dataset.defaultHtml || button.innerHTML);
    }

    async waitForExportAssets(node) {
        if (!node) return;
        const images = Array.from(node.querySelectorAll('img'));
        await Promise.all(images.map((img) => {
            if (img.complete) return Promise.resolve();
            return new Promise((resolve) => {
                const done = () => {
                    img.removeEventListener('load', done);
                    img.removeEventListener('error', done);
                    resolve();
                };
                img.addEventListener('load', done, { once: true });
                img.addEventListener('error', done, { once: true });
            });
        }));

        if (document.fonts && document.fonts.ready) {
            await document.fonts.ready;
        }
    }

    async downloadPdf(button) {
        if (this.isDownloading) return;
        const target = this.querySelector('#pv-preview');
        if (!target || typeof window.html2canvas !== 'function' || !window.jspdf?.jsPDF) return;

        this.isDownloading = true;
        this.setDownloadLoading(button, true);
        const busyToken = this.startBusyIndicator('Generation du PDF...', 120);

        try {
            const exportWidth = 794;
            const exportNode = target.cloneNode(true);
            exportNode.id = 'pv-preview-export';
            exportNode.style.position = 'fixed';
            exportNode.style.left = '-10000px';
            exportNode.style.top = '0';
            exportNode.style.width = `${exportWidth}px`;
            exportNode.style.height = 'auto';
            exportNode.style.transform = 'none';
            exportNode.style.transformOrigin = 'top left';
            exportNode.style.maxWidth = 'none';
            exportNode.style.maxHeight = 'none';
            exportNode.style.pointerEvents = 'none';
            exportNode.style.zIndex = '-1';
            document.body.appendChild(exportNode);
            this.applyStylesToPreviewNode(exportNode);

            await this.waitForExportAssets(exportNode);
            const contentHeight = Math.max(exportNode.scrollHeight, exportNode.offsetHeight, 1123);
            exportNode.style.height = `${contentHeight}px`;
            const textOccupancy = this.collectTextRowOccupancy(exportNode, contentHeight);

            const canvas = await window.html2canvas(exportNode, {
                scale: 2,
                useCORS: true,
                backgroundColor: '#ffffff',
                width: exportWidth,
                height: contentHeight,
                windowWidth: exportWidth,
                windowHeight: contentHeight,
                scrollX: 0,
                scrollY: 0,
                onclone: (doc) => {
                    const exportRoot = doc.querySelector('#pv-preview-export') || doc.querySelector('#pv-preview');
                    if (exportRoot && exportRoot.style) {
                        exportRoot.style.transform = 'none';
                        exportRoot.style.transformOrigin = 'top left';
                        exportRoot.style.width = `${exportWidth}px`;
                        exportRoot.style.height = `${contentHeight}px`;
                        this.applyStylesToPreviewNode(exportRoot);
                    }
                }
            });

            if (exportNode.parentElement) {
                exportNode.parentElement.removeChild(exportNode);
            }

            const pdf = new window.jspdf.jsPDF({
                orientation: 'portrait',
                unit: 'mm',
                format: 'a4'
            });
            const marginXmm = 6;
            const marginYmm = 0;
            const pageWidthMm = 210;
            const pageHeightMm = 297;
            const contentWidthMm = pageWidthMm - (marginXmm * 2);
            const contentHeightMm = pageHeightMm - (marginYmm * 2);
            const pageHeightPx = Math.floor((contentHeightMm / contentWidthMm) * canvas.width);
            const slices = this.buildPageSlices(canvas, pageHeightPx, textOccupancy);
            let renderedPx = 0;
            let pageIndex = 0;
            for (const sliceHeightPx of slices) {
                const sliceCanvas = document.createElement('canvas');
                sliceCanvas.width = canvas.width;
                sliceCanvas.height = sliceHeightPx;
                const ctx = sliceCanvas.getContext('2d');
                if (!ctx) throw new Error('Contexte canvas indisponible pour export PDF.');

                ctx.drawImage(
                    canvas,
                    0,
                    renderedPx,
                    canvas.width,
                    sliceHeightPx,
                    0,
                    0,
                    canvas.width,
                    sliceHeightPx
                );

                const pageImage = sliceCanvas.toDataURL('image/png', 1.0);
                if (pageIndex > 0) {
                    pdf.addPage();
                }
                const renderHeightMm = (sliceHeightPx / canvas.width) * contentWidthMm;
                pdf.addImage(pageImage, 'PNG', marginXmm, marginYmm, contentWidthMm, renderHeightMm);

                renderedPx += sliceHeightPx;
                pageIndex += 1;
            }
            pdf.save('mon-cv.pdf');
        } catch (error) {
            console.error('Echec export PDF:', error);
        } finally {
            const staleNode = document.querySelector('#pv-preview-export');
            if (staleNode && staleNode.parentElement) {
                staleNode.parentElement.removeChild(staleNode);
            }
            this.isDownloading = false;
            this.setDownloadLoading(button, false);
            this.stopBusyIndicator(busyToken);
        }
    }

    applyTextAutoFit() {
        const preview = this.querySelector('#pv-preview');
        if (!preview) return;

        const targets = Array.from(preview.querySelectorAll('[data-autofit]'));
        targets.forEach((el) => this.resetAutoFit(el));
        const boxes = Array.from(preview.querySelectorAll('[data-autofit-box]'));
        boxes.forEach((box) => this.fitContainerText(box));
        this.fitContainerText(preview);
    }

    resetAutoFit(el) {
        if (!el) return;
        if (!el.dataset.baseFontSize) {
            const computed = window.getComputedStyle(el);
            el.dataset.baseFontSize = computed.fontSize;
            el.dataset.baseLineHeight = computed.lineHeight;
        }
        el.style.fontSize = el.dataset.baseFontSize;
        el.style.lineHeight = el.dataset.baseLineHeight;
    }

    shrinkTextStep(el) {
        if (!el) return false;
        const computed = window.getComputedStyle(el);
        const currentSize = parseFloat(computed.fontSize || '0');
        const minSize = parseFloat(el.getAttribute('data-autofit-min') || '8');
        if (!currentSize || currentSize <= minSize) return false;

        const nextSize = Math.max(minSize, currentSize - 0.35);
        if (nextSize >= currentSize) return false;
        el.style.fontSize = `${nextSize}px`;

        const currentLineHeight = parseFloat(computed.lineHeight || '0');
        if (currentLineHeight && Number.isFinite(currentLineHeight)) {
            const ratio = currentLineHeight / currentSize;
            const nextLineHeight = Math.max(nextSize * 1.1, nextSize * ratio);
            el.style.lineHeight = `${nextLineHeight}px`;
        }
        return true;
    }

    fitContainerText(container) {
        if (!container) return;
        const targets = Array.from(container.querySelectorAll('[data-autofit]'));
        if (!targets.length) return;

        let guard = 0;
        while (this.isOverflowing(container) && guard < 60) {
            let changed = false;
            targets.forEach((el) => {
                if (this.shrinkTextStep(el)) changed = true;
            });
            if (!changed) break;
            guard += 1;
        }
    }

    isOverflowing(el) {
        if (!el) return false;
        return el.scrollHeight > el.clientHeight + 1 || el.scrollWidth > el.clientWidth + 1;
    }
}

if (!customElements.get('previsualisation-cv')) {
    customElements.define('previsualisation-cv', PrevisualisationCV);
}
