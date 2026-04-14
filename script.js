const NETWORKS = [
  { id: 'ATACADÃO DIA A DIA', label: 'ATACADÃO DIA A DIA', hasBreak: true },
  { id: 'COMPER/FORT', label: 'COMPER/FORT', hasBreak: true },
  { id: 'VIVENDAS', label: 'VIVENDAS', hasBreak: true },
  { id: 'BRETAS', label: 'BRETAS', hasBreak: true },
  { id: 'COSTA', label: 'COSTA', hasBreak: true },
  { id: 'ASSAÍ', label: 'ASSAÍ', hasBreak: true },
  { id: 'VARIADOS', label: 'VARIADOS', hasBreak: true },
  { id: 'CONSIGNADOS', label: 'CONSIGNADOS', hasBreak: true }
];

const ADM_CREDENTIALS = {
  username: 'richard.martins',
  password: 'sofolhas2026'
};

const STORAGE_KEY = 'sofolhas-dashboard-v7';
const LEGACY_STORAGE_KEYS = ['sofolhas-dashboard-v6'];
const BREAK_LIMIT = 12;
const WARNING_LIMIT = 10;
const CIRCLE_LENGTH = 326.73;
const CHART_PALETTE = ['#36d27c', '#77d4ff', '#f4c84b', '#ff6464', '#9b8cff', '#47d7c5', '#ff8f4d', '#8bd34b'];
const PRIMARY_HIGHLIGHT_NETWORKS = ['ATACADÃO DIA A DIA', 'COSTA', 'COMPER/FORT'];
const WEEK_OPTIONS = ['1ª semana', '2ª semana', '3ª semana', '4ª semana', '5ª semana'];
const MONTH_OPTIONS = [
  { value: '01', label: 'Janeiro' },
  { value: '02', label: 'Fevereiro' },
  { value: '03', label: 'Março' },
  { value: '04', label: 'Abril' },
  { value: '05', label: 'Maio' },
  { value: '06', label: 'Junho' },
  { value: '07', label: 'Julho' },
  { value: '08', label: 'Agosto' },
  { value: '09', label: 'Setembro' },
  { value: '10', label: 'Outubro' },
  { value: '11', label: 'Novembro' },
  { value: '12', label: 'Dezembro' }
];
const COMPER_FORT_STORE_MAPPINGS = [
  ['G.P - 77 VALPARAISO', 'FORT VALPARAÍSO'],
  ['G.P - 58 AGUAS CLARAS', 'COMPER ÁGUAS CLARAS'],
  ['G.P - 39 CEILÂNDIA', 'FORT CEILÂNDIA'],
  ['G.P - 82 PLANALTINA', 'FORT PLANALTINA'],
  ['G.P - 55 ASA SUL', 'COMPER ASA SUL'],
  ['G.P - 74 TAGUATINGA', 'FORT TAGUATINGA'],
  ['G.P - 17 COMPER GAMA', 'COMPER GAMA'],
  ['G.P - 30 COMPER SOBRAD', 'COMPER SOBRADINHO'],
  ['G.P - 22 SOL NASCENTE', 'FORT SOL NASCENTE'],
  ['G.P - 64 RECANTO DAS EMAS', 'FORT RECANTO DAS EMAS']
].map(([from, to]) => ({ from, to, key: normalizeStoreKey(from) }));

const appState = {
  data: [],
  config: {
    metaGeral: 1000000,
    metasPorRede: {},
    ultimaAtualizacao: null,
    ultimaImportacao: null
  },
  filters: {
    rede: 'Todas',
    loja: 'Todas',
    mes: 'Todas',
    semana: 'Todas'
  },
  detailsRede: 'Todas',
  rankingRede: 'Todas',
  detailsExpanded: false,
  admTab: 'metas',
  drawerOpen: false,
  isAdmAuthenticated: false,
  customSelects: {},
  charts: {},
  imports: []
};

const els = {};

const FIREBASE_STATE_PATH = 'painelComercialState';
const firebaseBridge = {
  enabled: false,
  database: null,
  ref: null,
  listenerAttached: false,
  initialLoadComplete: false,
  lastSyncedHash: '',
  remotePersistTimer: null
};

document.addEventListener('DOMContentLoaded', init);

function exportStateSnapshot() {
  return {
    data: appState.data,
    config: appState.config,
    imports: appState.imports
  };
}

function getStateHash(snapshot) {
  try {
    return JSON.stringify(snapshot || {});
  } catch {
    return String(Date.now());
  }
}

function applyPersistedState(saved) {
  appState.config.metasPorRede = defaultMetasPorRede();

  if (saved) {
    appState.data = Array.isArray(saved.data) ? saved.data : [];
    appState.imports = Array.isArray(saved.imports)
      ? saved.imports.sort((a, b) => new Date(b.importedAt || 0) - new Date(a.importedAt || 0))
      : [];
    appState.config = {
      ...appState.config,
      ...(saved.config || {}),
      metasPorRede: { ...defaultMetasPorRede(), ...(saved.config?.metasPorRede || {}) }
    };
  } else {
    appState.data = [];
    appState.imports = [];
    appState.config = {
      ...appState.config,
      metasPorRede: defaultMetasPorRede(),
      ultimaAtualizacao: null,
      ultimaImportacao: null
    };
  }

  if (looksLikeSampleData(appState.data, appState.imports)) {
    appState.data = [];
    appState.imports = [];
    appState.config.ultimaAtualizacao = null;
    appState.config.ultimaImportacao = null;
  }

  appState.data = appState.data.map(normalizeStoredRecord);
  appState.imports = appState.imports.map(normalizeStoredImport);
  appState.config.metaGeral = calculateMetaGeralFromNetworks(appState.config.metasPorRede);
}

async function initFirebaseBridge() {
  const config = window.__SOFOLHAS_FIREBASE__ || null;
  if (!config || !config.databaseURL || !window.firebase) return false;

  try {
    if (!window.firebase.apps.length) {
      window.firebase.initializeApp(config);
    }
    firebaseBridge.database = window.firebase.database();
    firebaseBridge.ref = firebaseBridge.database.ref(FIREBASE_STATE_PATH);
    firebaseBridge.enabled = true;
    subscribeRemoteState();
    return true;
  } catch (error) {
    console.error('Falha ao inicializar Firebase:', error);
    firebaseBridge.enabled = false;
    firebaseBridge.database = null;
    firebaseBridge.ref = null;
    return false;
  }
}

async function loadRemoteStateOnce() {
  if (!firebaseBridge.enabled || !firebaseBridge.ref) return null;
  try {
    const snapshot = await firebaseBridge.ref.once('value');
    return snapshot.val();
  } catch (error) {
    console.error('Falha ao carregar estado remoto:', error);
    return null;
  }
}

function subscribeRemoteState() {
  if (!firebaseBridge.enabled || !firebaseBridge.ref || firebaseBridge.listenerAttached) return;
  firebaseBridge.listenerAttached = true;

  firebaseBridge.ref.on('value', snapshot => {
    if (!firebaseBridge.initialLoadComplete) return;
    const remoteState = snapshot.val();
    if (!remoteState) return;

    const remoteHash = getStateHash(remoteState);
    if (remoteHash === firebaseBridge.lastSyncedHash) return;

    applyPersistedState(remoteState);
    firebaseBridge.lastSyncedHash = remoteHash;
    persistLocal({ skipRemote: true });

    syncLojaOptions();
    syncSemanaOptions();
    buildMetaInputs();
    refreshAll();
    renderImportBatchesTable();
    renderAdminTable();
  });
}

function queueRemotePersist() {
  if (!firebaseBridge.enabled || !firebaseBridge.initialLoadComplete || !firebaseBridge.ref) return;
  clearTimeout(firebaseBridge.remotePersistTimer);
  firebaseBridge.remotePersistTimer = setTimeout(async () => {
    try {
      const snapshot = exportStateSnapshot();
      const hash = getStateHash(snapshot);
      if (hash === firebaseBridge.lastSyncedHash) return;
      await firebaseBridge.ref.set(snapshot);
      firebaseBridge.lastSyncedHash = hash;
    } catch (error) {
      console.error('Falha ao salvar estado remoto:', error);
    }
  }, 250);
}


function normalizeStoreKey(value) {
  return normalizeHeader(value || '').replace(/\s+/g, ' ').trim();
}

function normalizeHeader(value) {
  return normalizeText(value || '').replace(/[^a-z0-9]+/g, ' ').trim();
}

function weekSortValue(label) {
  const match = String(label || '').match(/(\d+)/);
  return match ? Number(match[1]) : Number.MAX_SAFE_INTEGER;
}

function getMonthLabel(value) {
  const normalizedValue = String(value || '').trim();
  if (!normalizedValue) return '';
  const foundByValue = MONTH_OPTIONS.find(month => month.value === normalizedValue.padStart(2, '0'));
  if (foundByValue) return foundByValue.label;
  const normalizedTextValue = normalizeText(normalizedValue);
  return MONTH_OPTIONS.find(month => normalizeText(month.label) === normalizedTextValue)?.label || normalizedValue;
}

function suggestWeekLabel(index) {
  const safeIndex = Number(index || 1);
  return WEEK_OPTIONS[Math.max(0, Math.min(WEEK_OPTIONS.length - 1, safeIndex - 1))] || WEEK_OPTIONS[0];
}

function inferRecordMonthKey(record) {
  if (!record) return '';
  const directMonthKey = String(record.monthKey || '').trim();
  if (/^\d{4}-\d{2}$/.test(directMonthKey)) return directMonthKey;

  const rawMonth = String(record.monthValue || '').trim();
  const normalizedMonthValue = rawMonth && /^\d{1,2}$/.test(rawMonth) ? rawMonth.padStart(2, '0') : '';

  if (normalizedMonthValue) {
    const yearSource = record.importedAt || record.dataImportacao || record.ultimaImportacao || record.ultimaAtualizacao;
    const year = new Date(yearSource || Date.now()).getFullYear();
    return `${year}-${normalizedMonthValue}`;
  }

  const monthLabel = getMonthLabel(record.monthLabel || '');
  if (monthLabel) {
    const month = MONTH_OPTIONS.find(item => item.label === monthLabel)?.value;
    const yearSource = record.importedAt || record.dataImportacao || record.ultimaImportacao || record.ultimaAtualizacao;
    const year = new Date(yearSource || Date.now()).getFullYear();
    if (month) return `${year}-${month}`;
  }

  const periodMatch = String(record.periodLabel || '').match(/(?:^|\s)(Janeiro|Fevereiro|Mar[oç]o|Abril|Maio|Junho|Julho|Agosto|Setembro|Outubro|Novembro|Dezembro)(?:$|\s)/i);
  if (periodMatch) {
    const label = getMonthLabel(periodMatch[1]);
    const month = MONTH_OPTIONS.find(item => item.label === label)?.value;
    const yearSource = record.importedAt || record.dataImportacao || record.ultimaImportacao || record.ultimaAtualizacao;
    const year = new Date(yearSource || Date.now()).getFullYear();
    if (month) return `${year}-${month}`;
  }

  const dateSource = record.importedAt || record.dataImportacao || record.ultimaImportacao || record.ultimaAtualizacao;
  if (dateSource) {
    const date = new Date(dateSource);
    if (!Number.isNaN(date.getTime())) return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
  }

  return '';
}

function normalizeNetworkName(value) {
  const normalized = normalizeText(value || '').replace(/[^a-z0-9]+/g, ' ').trim();
  if (!normalized) return '';
  if (normalized.includes('dia a dia')) return 'ATACADÃO DIA A DIA';
  if (normalized.includes('comper') || normalized.includes('fort')) return 'COMPER/FORT';
  if (normalized.includes('vivendas')) return 'VIVENDAS';
  if (normalized.includes('bretas')) return 'BRETAS';
  if (normalized.includes('costa')) return 'COSTA';
  if (normalized.includes('assai') || normalized.includes('assai')) return 'ASSAÍ';
  if (normalized.includes('variados') || normalized.includes('variado')) return 'VARIADOS';
  if (normalized.includes('consignados') || normalized.includes('consignado')) return 'CONSIGNADOS';
  return String(value || '').trim().toUpperCase();
}

function normalizeStoreAndNetwork(store, network) {
  const originalStore = String(store || '').replace(/\s+/g, ' ').trim();
  let resolvedNetwork = normalizeNetworkName(network);
  const storeKey = normalizeStoreKey(originalStore);
  const mappedComperFort = COMPER_FORT_STORE_MAPPINGS.find(item => item.key === storeKey);

  if (mappedComperFort) {
    return { loja: mappedComperFort.to, rede: 'COMPER/FORT' };
  }

  if (!resolvedNetwork) {
    resolvedNetwork = inferNetworkByStore(originalStore) || '';
  }

  return {
    loja: originalStore,
    rede: resolvedNetwork
  };
}

function normalizeStoredRecord(record) {
  if (!record || typeof record !== 'object') return record;
  const normalizedStore = normalizeStoreAndNetwork(record.loja, record.rede);
  const monthLabel = getMonthLabel(record.monthLabel || record.monthValue || '');
  const monthKey = inferRecordMonthKey({ ...record, monthLabel });
  const venda = Number(record.valorVenda || 0);
  const quebraOperacional = Number(record.valorQuebraOperacional || record.valorQuebra || 0);
  const falta = Number(record.valorFalta || 0);
  const qualidade = Number(record.valorQualidade || 0);
  const estoque = Number(record.valorEstoque || 0);
  const quebraTotal = Number(record.valorQuebraTotal || (quebraOperacional + falta + qualidade) || 0);
  const percentualQuebra = Number.isFinite(Number(record.percentualQuebra))
    ? Number(record.percentualQuebra)
    : (venda > 0 ? Number(((quebraTotal / venda) * 100).toFixed(2)) : 0);

  return {
    ...record,
    rede: normalizedStore.rede || normalizeNetworkName(record.rede),
    loja: normalizedStore.loja || String(record.loja || '').trim(),
    semana: String(record.semana || '').trim() || suggestWeekLabel(1),
    monthLabel,
    monthKey,
    periodLabel: record.periodLabel || [record.semana, monthLabel].filter(Boolean).join(' de '),
    valorVenda: venda,
    valorQuebraOperacional: quebraOperacional,
    valorQuebraTotal: quebraTotal,
    valorQuebra: quebraTotal,
    valorFalta: falta,
    valorQualidade: qualidade,
    valorEstoque: estoque,
    percentualQuebra,
    percentualFalta: Number(record.percentualFalta || (venda > 0 ? ((falta / venda) * 100) : 0)),
    percentualQualidade: Number(record.percentualQualidade || (venda > 0 ? ((qualidade / venda) * 100) : 0)),
    statusQuebra: getBreakStatus(percentualQuebra).label
  };
}

function normalizeStoredImport(batch) {
  if (!batch || typeof batch !== 'object') return batch;
  const monthLabel = getMonthLabel(batch.monthLabel || batch.monthValue || '');
  const monthKey = inferRecordMonthKey({ ...batch, monthLabel, importedAt: batch.importedAt });
  return {
    ...batch,
    monthLabel,
    monthKey,
    periodLabel: batch.periodLabel || [batch.weekLabel, monthLabel].filter(Boolean).join(' de '),
    verificationStatus: batch.verificationStatus || 'ok',
    verificationItems: Array.isArray(batch.verificationItems) ? batch.verificationItems : []
  };
}



async function init() {
  cacheElements();
  await seedInitialState();
  buildMetaInputs();
  initCustomSelects();
  bindEvents();
  refreshAll();
}

function cacheElements() {
  Object.assign(els, {
    drawer: document.getElementById('filterDrawer'),
    drawerBackdrop: document.getElementById('drawerBackdrop'),
    openDrawerBtn: document.getElementById('openDrawerBtn'),
    closeDrawerBtn: document.getElementById('closeDrawerBtn'),
    applyFiltersBtn: document.getElementById('applyFiltersBtn'),
    clearFiltersBtn: document.getElementById('clearFiltersBtn'),
    openAdmBtn: document.getElementById('openAdmBtn'),
    authModal: document.getElementById('authModal'),
    closeAuthModalBtn: document.getElementById('closeAuthModalBtn'),
    authForm: document.getElementById('authForm'),
    authUser: document.getElementById('authUser'),
    authPass: document.getElementById('authPass'),
    authFeedback: document.getElementById('authFeedback'),
    admModal: document.getElementById('admModal'),
    closeAdmModalBtn: document.getElementById('closeAdmModalBtn'),
    metaGeralInput: document.getElementById('metaGeralInput'),
    metasRedeForm: document.getElementById('metasRedeForm'),
    saveGoalsBtn: document.getElementById('saveGoalsBtn'),
    excelFileInput: document.getElementById('excelFileInput'),
    monthInput: document.getElementById('monthInput'),
    weekInput: document.getElementById('weekInput'),
    importExcelBtn: document.getElementById('importExcelBtn'),
    importFeedback: document.getElementById('importFeedback'),
    importBatchCount: document.getElementById('importBatchCount'),
    importBatchesTableBody: document.getElementById('importBatchesTableBody'),
    verificationModal: document.getElementById('verificationModal'),
    closeVerificationModalBtn: document.getElementById('closeVerificationModalBtn'),
    verificationModalTitle: document.getElementById('verificationModalTitle'),
    verificationModalFileName: document.getElementById('verificationModalFileName'),
    verificationModalPeriod: document.getElementById('verificationModalPeriod'),
    verificationModalStatus: document.getElementById('verificationModalStatus'),
    verificationModalBody: document.getElementById('verificationModalBody'),
    adminTableBody: document.getElementById('adminTableBody'),
    lastUpdateText: document.getElementById('lastUpdateText'),
    viewTitle: document.getElementById('viewTitle'),
    viewSubtitle: document.getElementById('viewSubtitle'),
    activeFiltersChips: document.getElementById('activeFiltersChips'),
    metaPercent: document.getElementById('metaPercent'),
    metaPercentInner: document.getElementById('metaPercentInner'),
    metaLegend: document.getElementById('metaLegend'),
    metaTotalValue: document.getElementById('metaTotalValue'),
    salesTotalValue: document.getElementById('salesTotalValue'),
    lastImportValue: document.getElementById('lastImportValue'),
    metaCircle: document.getElementById('metaCircle'),
    cardVenda: document.getElementById('cardVenda'),
    cardQuebra: document.getElementById('cardQuebra'),
    cardPercQuebra: document.getElementById('cardPercQuebra'),
    cardFalta: document.getElementById('cardFalta'),
    cardQualidade: document.getElementById('cardQualidade'),
    cardStatusQuebra: document.getElementById('cardStatusQuebra'),
    stockCard: document.getElementById('stockCard'),
    cardEstoque: document.getElementById('cardEstoque'),
    summaryTableBody: document.getElementById('summaryTableBody'),
    rankingHighlights: document.getElementById('rankingHighlights'),
    rankingNetworkFilter: document.getElementById('rankingNetworkFilter'),
    rankingPositivePanel: document.getElementById('rankingPositivePanel'),
    networkWinnersPanel: document.getElementById('networkWinnersPanel'),
    rankingTableBody: document.getElementById('rankingTableBody'),
    detailsTableTitle: document.getElementById('detailsTableTitle'),
    detailsTableSubtitle: document.getElementById('detailsTableSubtitle'),
    detailsNetworkTabs: document.getElementById('detailsNetworkTabs'),
    detailsTableHeadRow: document.getElementById('detailsTableHeadRow'),
    detailsTableBody: document.getElementById('detailsTableBody'),
    detailsSection: document.getElementById('detailsSection'),
    toggleDetailsBtn: document.getElementById('toggleDetailsBtn'),
    breakRealValue: document.getElementById('breakRealValue'),
    breakStatusBadge: document.getElementById('breakStatusBadge'),
    alertsGrid: document.getElementById('alertsGrid'),
    salesTrendChart: document.getElementById('salesTrendChart'),
    breakByNetworkChart: document.getElementById('breakByNetworkChart'),
    breakByStoreChart: document.getElementById('breakByStoreChart'),
    bestBreakByStoreChart: document.getElementById('bestBreakByStoreChart'),
    networkDistributionChart: document.getElementById('networkDistributionChart'),
    admNavItems: [...document.querySelectorAll('[data-adm-tab]')],
    admPanels: [...document.querySelectorAll('[data-adm-panel]')]
  });
}

async function seedInitialState() {
  const saved = readStorage();
  applyPersistedState(saved);

  const firebaseReady = await initFirebaseBridge();
  if (firebaseReady) {
    const remoteState = await loadRemoteStateOnce();
    const hasRemoteState = remoteState && (Array.isArray(remoteState.data) || Array.isArray(remoteState.imports) || remoteState.config);

    if (hasRemoteState) {
      applyPersistedState(remoteState);
      firebaseBridge.lastSyncedHash = getStateHash(remoteState);
      persistLocal({ skipRemote: true });
    } else if (saved) {
      persistLocal();
    } else {
      persistLocal({ skipRemote: true });
    }
  } else {
    persistLocal({ skipRemote: true });
  }

  firebaseBridge.initialLoadComplete = true;
}

function defaultMetasPorRede() {
  return {
    'ATACADÃO DIA A DIA': 180000,
    'COMPER/FORT': 150000,
    'VIVENDAS': 120000,
    'BRETAS': 90000,
    'COSTA': 110000,
    'ASSAÍ': 100000,
    'VARIADOS': 80000,
    'CONSIGNADOS': 170000
  };
}

function calculateMetaGeralFromNetworks(source = appState.config.metasPorRede) {
  return NETWORKS.reduce((total, network) => total + Number(source?.[network.id] || 0), 0);
}

function syncMetaGeralInputFromNetworkInputs({ updateState = false } = {}) {
  const networkInputs = els.metasRedeForm
    ? [...els.metasRedeForm.querySelectorAll('input[data-network]')]
    : [];

  const total = networkInputs.length
    ? networkInputs.reduce((sum, input) => sum + parseCurrencyInput(input.value), 0)
    : calculateMetaGeralFromNetworks();

  setCurrencyInputValue(els.metaGeralInput, total);
  if (updateState) appState.config.metaGeral = total;
  return total;
}

function generateSampleData() {
  const weeks = ['Semana 1', 'Semana 2', 'Semana 3', 'Semana 4'];
  const storesByNetwork = {
    'ATACADÃO DIA A DIA': ['DD Goiânia Sul', 'DD Aparecida', 'DD Trindade'],
    'COMPER/FORT': ['Comper Centro', 'Fort Norte', 'Fort Sul'],
    'VIVENDAS': ['Vivendas 01', 'Vivendas 02', 'Vivendas 03'],
    'BRETAS': ['Bretas Setor Bueno', 'Bretas Centro'],
    'COSTA': ['Costa Campinas', 'Costa Buriti'],
    'ASSAÍ': ['Assaí Anhanguera', 'Assaí Perimetral'],
    'VARIADOS': ['Empório Verde', 'Mercado Central'],
    'CONSIGNADOS': ['Consignado A', 'Consignado B', 'Consignado C']
  };

  const now = new Date().toISOString();
  const data = [];
  NETWORKS.forEach((network, nIndex) => {
    storesByNetwork[network.id].forEach((store, sIndex) => {
      weeks.forEach((week, wIndex) => {
        const sale = 13000 + nIndex * 2800 + sIndex * 1650 + wIndex * 1200;
        const breakValue = Number((sale * (0.045 + (nIndex % 4) * 0.01 + wIndex * 0.003)).toFixed(2));
        const missValue = Number((sale * (0.008 + (sIndex % 2) * 0.004)).toFixed(2));
        const qualityValue = Number((sale * (0.006 + (wIndex % 2) * 0.003)).toFixed(2));
        const stockValue = network.id === 'COSTA' ? Number((sale * 0.38).toFixed(2)) : 0;
        const percBreak = sale ? Number(((breakValue / sale) * 100).toFixed(2)) : 0;
        data.push({
          id: `${network.id}-${store}-${week}`,
          rede: network.id,
          loja: store,
          semana: week,
          valorVenda: sale,
          valorQuebra: breakValue,
          valorFalta: missValue,
          valorQualidade: qualityValue,
          valorEstoque: stockValue,
          percentualQuebra: percBreak,
          percentualFalta: sale ? Number(((missValue / sale) * 100).toFixed(2)) : 0,
          percentualQualidade: sale ? Number(((qualityValue / sale) * 100).toFixed(2)) : 0,
          statusQuebra: getBreakStatus(percBreak).label,
          dataImportacao: now,
          modeloImportacao: network.id === 'COSTA' ? 'COSTA' : 'PADRAO'
        });
      });
    });
  });
  return data;
}

function buildMetaInputs() {
  els.metaGeralInput.dataset.currency = 'true';
  els.metaGeralInput.readOnly = true;
  els.metaGeralInput.title = 'Calculada automaticamente pela soma das metas por rede';
  els.metasRedeForm.innerHTML = NETWORKS.map(network => `
    <div class="field">
      <label for="meta-${slugify(network.id)}">Meta ${network.label}</label>
      <input id="meta-${slugify(network.id)}" data-network="${network.id}" data-currency="true" type="text" inputmode="decimal" autocomplete="off" placeholder="R$ 0,00" />
    </div>
  `).join('');

  [...els.metasRedeForm.querySelectorAll('input[data-network]')].forEach(input => {
    setCurrencyInputValue(input, appState.config.metasPorRede[input.dataset.network] || 0);
  });

  syncMetaGeralInputFromNetworkInputs({ updateState: true });
  [...els.metasRedeForm.querySelectorAll('input[data-network]')].forEach(input => {
    attachCurrencyMask(input);
    input.addEventListener('input', () => syncMetaGeralInputFromNetworkInputs());
    input.addEventListener('blur', () => syncMetaGeralInputFromNetworkInputs());
  });
}

function initCustomSelects() {
  createCustomSelect('filterRede', [{ value: 'Todas', label: 'Todas' }, ...NETWORKS.map(n => ({ value: n.id, label: n.label }))], appState.filters.rede);
  syncLojaOptions();
  syncMesOptions();
  syncSemanaOptions();
}

function createCustomSelect(id, options, selectedValue) {
  const root = document.getElementById(id);
  root.innerHTML = '';
  root.dataset.value = selectedValue;

  const trigger = document.createElement('button');
  trigger.type = 'button';
  trigger.className = 'custom-select__trigger';
  trigger.innerHTML = `<span>${getOptionLabel(options, selectedValue)}</span><span class="custom-select__caret">▾</span>`;

  const menu = document.createElement('div');
  menu.className = 'custom-select__menu';
  menu.hidden = true;

  options.forEach(option => {
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'custom-select__option';
    if (option.value === selectedValue) btn.classList.add('is-selected');
    btn.textContent = option.label;
    btn.dataset.value = option.value;
    btn.addEventListener('click', () => {
      root.dataset.value = option.value;
      trigger.querySelector('span').textContent = option.label;
      [...menu.children].forEach(child => child.classList.remove('is-selected'));
      btn.classList.add('is-selected');
      menu.hidden = true;

      if (id === 'filterRede') {
        if (appState.filters.rede !== option.value) appState.filters.loja = 'Todas';
        appState.filters.rede = option.value;
        syncLojaOptions();
      } else if (id === 'filterLoja') {
        appState.filters.loja = option.value;
      } else if (id === 'filterMes') {
        if (appState.filters.mes !== option.value) appState.filters.semana = 'Todas';
        appState.filters.mes = option.value;
        syncLojaOptions();
        syncSemanaOptions();
      } else if (id === 'filterSemana') {
        appState.filters.semana = option.value;
      }
    });
    menu.appendChild(btn);
  });

  trigger.addEventListener('click', () => {
    closeAllSelectMenus(menu);
    menu.hidden = !menu.hidden;
  });

  root.append(trigger, menu);
  appState.customSelects[id] = { root, trigger, menu, options };
}

function syncLojaOptions() {
  let dataForStores = appState.data;
  if (appState.filters.rede !== 'Todas') {
    dataForStores = dataForStores.filter(item => item.rede === appState.filters.rede);
  }
  if (appState.filters.mes !== 'Todas') {
    dataForStores = dataForStores.filter(item => (item.monthKey || inferRecordMonthKey(item)) === appState.filters.mes);
  }
  const stores = [...new Set(dataForStores.map(item => item.loja))].sort((a, b) => a.localeCompare(b, 'pt-BR'));
  const selected = stores.includes(appState.filters.loja) ? appState.filters.loja : 'Todas';
  appState.filters.loja = selected;
  createCustomSelect('filterLoja', [{ value: 'Todas', label: 'Todas' }, ...stores.map(store => ({ value: store, label: store }))], selected);
}

function formatMonthFilterLabel(monthKey) {
  if (!monthKey || monthKey === 'Todas') return 'Todos';
  const [year, month] = String(monthKey).split('-');
  const monthLabel = getMonthLabel(month || '');
  return year ? `${monthLabel}/${year}` : monthLabel;
}

function syncMesOptions() {
  const monthKeys = [...new Set(appState.data.map(item => item.monthKey || inferRecordMonthKey(item)).filter(Boolean))]
    .sort((a, b) => String(b).localeCompare(String(a), 'pt-BR', { numeric: true }));
  const selected = monthKeys.includes(appState.filters.mes) ? appState.filters.mes : 'Todas';
  appState.filters.mes = selected;
  createCustomSelect('filterMes', [{ value: 'Todas', label: 'Todos' }, ...monthKeys.map(monthKey => ({ value: monthKey, label: formatMonthFilterLabel(monthKey) }))], selected);
}

function syncSemanaOptions() {
  let source = appState.data;
  if (appState.filters.mes !== 'Todas') {
    source = source.filter(item => (item.monthKey || inferRecordMonthKey(item)) === appState.filters.mes);
  }
  const weeks = [...new Set(source.map(item => item.semana))].sort((a, b) => weekSortValue(a) - weekSortValue(b) || a.localeCompare(b, 'pt-BR', { numeric: true }));
  const selected = weeks.includes(appState.filters.semana) ? appState.filters.semana : 'Todas';
  appState.filters.semana = selected;
  createCustomSelect('filterSemana', [{ value: 'Todas', label: 'Todas' }, ...weeks.map(week => ({ value: week, label: week }))], selected);
}

function bindEvents() {
  document.addEventListener('click', event => {
    const isInsideSelect = event.target.closest('.custom-select');
    if (!isInsideSelect) closeAllSelectMenus();
  });

  if (els.openDrawerBtn) els.openDrawerBtn.addEventListener('click', () => setDrawer(true));
  if (els.closeDrawerBtn) els.closeDrawerBtn.addEventListener('click', () => setDrawer(false));
  if (els.drawerBackdrop) els.drawerBackdrop.addEventListener('click', () => setDrawer(false));
  if (els.applyFiltersBtn) els.applyFiltersBtn.addEventListener('click', () => {
    setDrawer(false);
    refreshAll();
  });
  if (els.clearFiltersBtn) els.clearFiltersBtn.addEventListener('click', clearFilters);

  if (els.openAdmBtn) els.openAdmBtn.addEventListener('click', openAuthModal);
  if (els.closeAuthModalBtn) els.closeAuthModalBtn.addEventListener('click', closeAuthModal);
  if (els.authForm) els.authForm.addEventListener('submit', handleAuthSubmit);
  if (els.toggleDetailsBtn) els.toggleDetailsBtn.addEventListener('click', toggleDetailsSection);
  if (els.closeAdmModalBtn) els.closeAdmModalBtn.addEventListener('click', closeAdmModal);
  if (els.saveGoalsBtn) els.saveGoalsBtn.addEventListener('click', saveGoals);
  if (els.importExcelBtn) els.importExcelBtn.addEventListener('click', importExcelFile);
  if (els.importBatchesTableBody) els.importBatchesTableBody.addEventListener('click', handleImportBatchAction);
  if (els.closeVerificationModalBtn) els.closeVerificationModalBtn.addEventListener('click', closeVerificationModal);
  if (els.verificationModal) els.verificationModal.addEventListener('click', event => {
    if (event.target === els.verificationModal) closeVerificationModal();
  });
  if (els.admNavItems?.length) els.admNavItems.forEach(button => button.addEventListener('click', () => setAdmTab(button.dataset.admTab)));
  if (els.rankingNetworkFilter) els.rankingNetworkFilter.addEventListener('change', event => {
    appState.rankingRede = event.target.value;
    refreshAll();
  });
}

function setDrawer(open) {
  appState.drawerOpen = open;
  els.drawer.classList.toggle('is-open', open);
  els.drawer.setAttribute('aria-hidden', String(!open));
  els.drawerBackdrop.hidden = !open;
}

function clearFilters() {
  appState.filters = { rede: 'Todas', loja: 'Todas', mes: 'Todas', semana: 'Todas' };
  appState.detailsRede = 'Todas';
  appState.rankingRede = 'Todas';
  initCustomSelects();
  refreshAll();
}

function updateDetailsSectionVisibility() {
  if (!els.detailsSection || !els.toggleDetailsBtn) return;
  els.detailsSection.hidden = !appState.detailsExpanded;
  els.toggleDetailsBtn.setAttribute('aria-expanded', String(appState.detailsExpanded));
  els.toggleDetailsBtn.textContent = appState.detailsExpanded ? 'Ocultar detalhamento por loja' : 'Ver detalhamento por loja';
}

function toggleDetailsSection() {
  appState.detailsExpanded = !appState.detailsExpanded;
  updateDetailsSectionVisibility();
}

function openAuthModal() {
  els.authModal.hidden = false;
  els.authFeedback.textContent = '';
  els.authUser.value = '';
  els.authPass.value = '';
}
function closeAuthModal() { els.authModal.hidden = true; }

function setAdmTab(tab) {
  appState.admTab = tab || 'metas';
  if (!els.admNavItems?.length || !els.admPanels?.length) return;
  els.admNavItems.forEach(button => button.classList.toggle('is-active', button.dataset.admTab === appState.admTab));
  els.admPanels.forEach(panel => {
    const active = panel.dataset.admPanel === appState.admTab;
    panel.hidden = !active;
    panel.classList.toggle('is-active', active);
  });
}

function openAdmModal() {
  buildMetaInputs();
  renderImportBatchesTable();
  renderAdminTable();
  setAdmTab(appState.admTab || 'metas');
  els.admModal.hidden = false;
}
function closeAdmModal() { els.admModal.hidden = true; }

function handleAuthSubmit(event) {
  event.preventDefault();
  const user = els.authUser.value.trim();
  const pass = els.authPass.value.trim();
  if (user === ADM_CREDENTIALS.username && pass === ADM_CREDENTIALS.password) {
    appState.isAdmAuthenticated = true;
    closeAuthModal();
    openAdmModal();
    return;
  }
  els.authFeedback.textContent = 'Usuário ou senha inválidos.';
  els.authFeedback.className = 'feedback is-error';
}

function saveGoals() {
  const inputs = [...els.metasRedeForm.querySelectorAll('input[data-network]')];
  inputs.forEach(input => {
    appState.config.metasPorRede[input.dataset.network] = parseCurrencyInput(input.value);
    setCurrencyInputValue(input, appState.config.metasPorRede[input.dataset.network]);
  });
  appState.config.metaGeral = calculateMetaGeralFromNetworks(appState.config.metasPorRede);
  setCurrencyInputValue(els.metaGeralInput, appState.config.metaGeral);
  appState.config.ultimaAtualizacao = new Date().toISOString();
  persistLocal();
  refreshAll();
}

function importExcelFile() {
  const file = els.excelFileInput.files[0];
  const weekValue = (els.weekInput.value || '').trim();
  const monthValue = (els.monthInput?.value || '').trim();
  const monthLabel = getMonthLabel(monthValue);
  if (!file) return setImportFeedback('Selecione um arquivo Excel para importar.', true);
  if (!monthValue) return setImportFeedback('Selecione o mês da planilha.', true);
  if (!weekValue) return setImportFeedback('Selecione a semana da planilha.', true);
  if (!window.XLSX) return setImportFeedback('Biblioteca XLSX não carregada.', true);

  const reader = new FileReader();
  reader.onload = event => {
    try {
      const workbook = XLSX.read(event.target.result, { type: 'array', cellDates: true });
      const parsed = parseImportedWorkbook(workbook, file.name, { weekLabel: weekValue, monthValue, monthLabel });
      if (!parsed.records.length) return setImportFeedback('Nenhum registro válido encontrado na planilha.', true);

      appState.data = [...appState.data, ...parsed.records];
      appState.imports = [parsed.batch, ...appState.imports].sort((a, b) => new Date(b.importedAt || 0) - new Date(a.importedAt || 0));
      appState.config.ultimaImportacao = parsed.batch.importedAt;
      appState.config.ultimaAtualizacao = new Date().toISOString();
      persistLocal();
      syncLojaOptions();
      syncSemanaOptions();
      refreshAll();
      renderImportBatchesTable();
      renderAdminTable();
      els.excelFileInput.value = '';
      if (els.weekInput) els.weekInput.value = '';
      if (els.monthInput) els.monthInput.value = '';
      const conferenceLabel = parsed.batch.verificationStatus === 'ok'
        ? 'Conferência OK'
        : parsed.batch.verificationStatus === 'warn'
          ? 'Conferência com alerta'
          : 'Conferência com divergência';
      setImportFeedback(`${parsed.records.length} registros importados. ${conferenceLabel}.`, parsed.batch.verificationStatus === 'bad');
    } catch (error) {
      setImportFeedback(`Erro ao processar planilha: ${error.message}`, true);
    }
  };
  reader.readAsArrayBuffer(file);
}

function parseImportedWorkbook(workbook, fileName, periodInfo = {}) {
  const batchId = `batch-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const importedAt = new Date().toISOString();
  const currentYear = new Date(importedAt).getFullYear();
  const weekLabel = periodInfo.weekLabel || suggestWeekLabel(appState.imports.length + 1);
  const monthValue = periodInfo.monthValue || String(new Date(importedAt).getMonth() + 1).padStart(2, '0');
  const monthLabel = periodInfo.monthLabel || getMonthLabel(monthValue);
  const monthKey = `${currentYear}-${monthValue}`;
  const periodLabel = `${weekLabel} de ${monthLabel}`;
  const records = [];
  const verificationItems = [];
  const totalSheet = [];

  workbook.SheetNames.forEach(sheetName => {
    const sheet = workbook.Sheets[sheetName];
    const extracted = extractWorksheetObjects(sheet);
    if (!extracted.rows.length) return;

    if (isWorkbookTotalSheet(sheetName, extracted.headers)) {
      totalSheet.push(...parseWorkbookTotalsSheet(extracted.rows));
      return;
    }

    const parsedSheet = parseDataWorksheet({
      sheetName,
      headers: extracted.headers,
      rows: extracted.rows,
      batchId,
      fileName,
      importedAt,
      weekLabel,
      monthLabel,
      monthKey,
      periodLabel
    });

    if (parsedSheet.records.length) records.push(...parsedSheet.records);
    if (parsedSheet.verification) verificationItems.push(parsedSheet.verification);
  });

  const grouped = aggregateByNetwork(records);
  const totalsVerification = compareWorkbookTotals(grouped, totalSheet);
  const combinedVerifications = [...verificationItems, ...totalsVerification].filter(Boolean);
  const verificationStatus = combinedVerifications.some(item => item.status === 'bad')
    ? 'bad'
    : combinedVerifications.some(item => item.status === 'warn')
      ? 'warn'
      : 'ok';

  const totals = aggregateRecords(records);
  return {
    records,
    batch: {
      id: batchId,
      fileName,
      importedAt,
      weekLabel,
      monthLabel,
      monthKey,
      periodLabel,
      recordCount: records.length,
      totalVenda: Number(totals.venda.toFixed(2)),
      totalQuebra: Number(totals.quebra.toFixed(2)),
      totalFalta: Number(totals.falta.toFixed(2)),
      totalQualidade: Number(totals.qualidade.toFixed(2)),
      verificationStatus,
      verificationItems: combinedVerifications
    }
  };
}

function extractWorksheetObjects(sheet) {
  const matrix = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: '', raw: true });
  const headerIndex = matrix.findIndex(row => Array.isArray(row) && row.filter(cell => String(cell ?? '').trim() !== '').length >= 2);
  if (headerIndex === -1) return { headers: [], rows: [] };
  const headers = (matrix[headerIndex] || []).map(normalizeHeader);
  const rows = matrix.slice(headerIndex + 1)
    .map(row => headers.reduce((acc, header, index) => {
      if (header) acc[header] = row[index];
      return acc;
    }, {}))
    .filter(row => Object.values(row).some(value => String(value ?? '').trim() !== ''));
  return { headers, rows };
}

function isWorkbookTotalSheet(sheetName, headers) {
  const normalizedSheet = normalizeText(sheetName);
  if (normalizedSheet.includes('total')) return true;
  return headers.includes('REDES') && headers.some(header => header.includes('VENDA TOTAL'));
}

function parseWorkbookTotalsSheet(rows) {
  return rows.map(row => {
    const rede = String(getFlexibleCell(row, [['REDES'], ['REDE']]) || '').trim();
    if (!rede || isTotalRow(rede)) return null;

    const quebraOperacionalInformada = parseMoney(getFlexibleCell(row, [['QUEBRA', 'REAL'], ['QUEBRA', 'PARCIAL']]));
    const quebraColunaPrincipal = parseMoney(getFlexibleCell(row, [['QUEBRA', 'TOTAL'], ['QUEBRA']]));
    const falta = parseMoney(getFlexibleCell(row, [['DEV', 'FALTA'], ['FALTA']]));
    const qualidade = parseMoney(getFlexibleCell(row, [['DEV', 'QUALIDADE'], ['QUALIDADE']]));
    const quebraOperacional = quebraOperacionalInformada > 0
      ? quebraOperacionalInformada
      : quebraColunaPrincipal;
    const quebraTotalCalculada = quebraOperacional + falta + qualidade;

    return {
      rede: normalizeNetworkName(rede),
      venda: parseMoney(getFlexibleCell(row, [['VENDA', 'TOTAL'], ['VENDA']])),
      quebraOperacional: Number(quebraOperacional.toFixed(2)),
      quebra: Number(quebraTotalCalculada.toFixed(2)),
      quebraTotal: Number(quebraTotalCalculada.toFixed(2)),
      acordoComercial: parseMoney(getFlexibleCell(row, [['ACORDO', 'COMERCIAL']])),
      falta: Number(falta.toFixed(2)),
      qualidade: Number(qualidade.toFixed(2)),
      estoque: parseMoney(getFlexibleCell(row, [['ESTOQUE', 'EM', 'LOJA'], ['ESTOQUE'], ['VALOR', 'EST']])),
      percentual: parsePercent(getFlexibleCell(row, [['%', 'TOTAL'], ['PERCENTUAL']])),
      percentualEstoque: parsePercent(getFlexibleCell(row, [['PERCENTUAL', 'EST'], ['%', 'EST']]))
    };
  }).filter(Boolean);
}

function normalizeHeaderWords(value) {
  return normalizeHeader(value).split(' ').filter(Boolean);
}

function headerMatchesGroup(header, group) {
  const headerWords = normalizeHeaderWords(header);
  return group.every(token => normalizeHeaderWords(token).every(word => headerWords.includes(word)));
}

function buildVerificationDetail(label, expected, actual, options = {}) {
  const { okThreshold = 1, warnThreshold = 10, alwaysInclude = false } = options;
  const safeExpected = Number.isFinite(expected) ? Number(expected.toFixed(2)) : 0;
  const safeActual = Number.isFinite(actual) ? Number(actual.toFixed(2)) : 0;

  if (!alwaysInclude && safeExpected === 0 && safeActual === 0) return null;

  const diff = Math.abs(safeActual - safeExpected);
  const status = diff <= okThreshold ? 'ok' : diff <= warnThreshold ? 'warn' : 'bad';

  return {
    label,
    expected: safeExpected,
    actual: safeActual,
    diff: Number(diff.toFixed(2)),
    status
  };
}

function summarizeVerificationDetails(details) {
  const applicable = (details || []).filter(Boolean);
  if (!applicable.length) return { status: 'ok', label: 'Total não informado' };
  const status = applicable.some(item => item.status === 'bad')
    ? 'bad'
    : applicable.some(item => item.status === 'warn')
      ? 'warn'
      : 'ok';
  return {
    status,
    label: status === 'ok' ? 'Conferido' : status === 'warn' ? 'Pequena diferença' : 'Divergência'
  };
}

function parseDataWorksheet({ sheetName, rows, batchId, fileName, importedAt, weekLabel, monthLabel, monthKey, periodLabel }) {
  const normalizedSheet = normalizeText(sheetName);
  const networkFromSheet = inferNetworkBySheetName(sheetName);
  const model = detectWorksheetModel(sheetName, rows[0] || {});
  const totalRow = rows.find(row => isTotalRow(getRowLabel(row)));
  const dataRows = rows.filter(row => !isTotalRow(getRowLabel(row)));

  const records = dataRows.map((row, index) => {
    const originalStore = String(getRowLabel(row) || '').trim();
    if (!originalStore) return null;

    let rede = networkFromSheet || normalizeNetworkName(String(getFlexibleCell(row, [['REDE']]) || '').trim()) || inferNetworkByStore(originalStore) || 'VARIADOS';
    let venda = 0;
    let falta = 0;
    let qualidade = 0;
    let quebraOperacional = 0;
    let estoque = 0;
    let percentualQuebra = 0;

    if (model === 'DIA_A_DIA') {
      rede = 'ATACADÃO DIA A DIA';
      venda = parseMoney(getFlexibleCell(row, [['VENDA']]));
      falta = parseMoney(getFlexibleCell(row, [['FALTA']]));
      qualidade = parseMoney(getFlexibleCell(row, [['QUALIDADE']]));
      quebraOperacional = parseMoney(getFlexibleCell(row, [['QUEBRA', 'REAL'], ['QUEBRA']]));
      percentualQuebra = parsePercent(getFlexibleCell(row, [['%', 'QUEBRA'], ['%', 'TOTAL']]));
    } else if (model === 'PEREIRA') {
      rede = 'COMPER/FORT';
      venda = parseMoney(getFlexibleCell(row, [['VENDA']]));
      falta = parseMoney(getFlexibleCell(row, [['DEV', 'FALTA'], ['DEVOLUCAO', 'FALTA'], ['FALTA']]));
      qualidade = parseMoney(getFlexibleCell(row, [['DEVOLUCAO', 'QUALIDADE'], ['DEV', 'QUALIDADE'], ['QUALIDADE']]));
      quebraOperacional = parseMoney(getFlexibleCell(row, [['QUEBRA', 'PARCIAL'], ['QUEBRA', 'REAL'], ['QUEBRA']]));
      percentualQuebra = parsePercent(getFlexibleCell(row, [['%', 'QUEBRA', 'REAL'], ['%', 'TOTAL'], ['PERCENTUAL']]));
    } else if (model === 'VIVENDAS') {
      rede = 'VIVENDAS';
      venda = parseMoney(getFlexibleCell(row, [['VENDA']]));
      quebraOperacional = parseMoney(getFlexibleCell(row, [['QUEBRA']]));
      percentualQuebra = parsePercent(getFlexibleCell(row, [['%', 'TOTAL'], ['PERCENTUAL']]));
    } else if (model === 'COSTA') {
      rede = 'COSTA';
      venda = parseMoney(getFlexibleCell(row, [['VALOR', 'ENTREGA', 'TOTAL'], ['VENDA']]));
      estoque = parseMoney(getFlexibleCell(row, [['ESTOQUE', 'EM', 'LOJA'], ['VALOR', 'EST', 'ATUAL'], ['VALOR', 'ESTOQUE', 'ATUAL'], ['ESTOQUE']]));
      falta = parseMoney(getFlexibleCell(row, [['DEVOLUCAO', 'FALTA'], ['VALOR', 'FALTA'], ['FALTA']]));
      qualidade = parseMoney(getFlexibleCell(row, [['DEVOLUCAO', 'QUALIDADE'], ['VALOR', 'QUALIDADE'], ['QUALIDADE']]));
      quebraOperacional = parseMoney(getFlexibleCell(row, [['QUEBRA', 'REAL'], ['VALOR', 'QUEBRA'], ['QUEBRA']]));
      percentualQuebra = parsePercent(getFlexibleCell(row, [['%', 'REAL'], ['%', 'QUEBRA', 'REAL'], ['PORCENTAGEM', 'TOTAL'], ['%', 'TOTAL']]));
    } else if (model === 'VARIADOS') {
      rede = 'VARIADOS';
      venda = parseMoney(getFlexibleCell(row, [['VENDA']]));
    } else if (model === 'CONSIGNADOS') {
      rede = 'CONSIGNADOS';
      venda = parseMoney(getFlexibleCell(row, [['VENDA']]));
      quebraOperacional = parseMoney(getFlexibleCell(row, [['QUEBRA']]));
      percentualQuebra = parsePercent(getFlexibleCell(row, [['PERCENTUAL'], ['%', 'TOTAL']]));
    } else if (model === 'ASSAI') {
      rede = 'ASSAÍ';
      venda = parseMoney(getFlexibleCell(row, [['VENDA']]));
    } else if (model === 'BRETAS') {
      rede = 'BRETAS';
      venda = parseMoney(getFlexibleCell(row, [['VENDA']]));
      quebraOperacional = parseMoney(getFlexibleCell(row, [['QUEBRA', 'TOTAL'], ['QUEBRA']]));
      percentualQuebra = parsePercent(getFlexibleCell(row, [['%', 'TOTAL'], ['PERCENTUAL']]));
    } else {
      venda = parseMoney(getFlexibleCell(row, [['VALOR', 'ENTREGA', 'TOTAL'], ['VENDA']]));
      falta = parseMoney(getFlexibleCell(row, [['DEV', 'FALTA'], ['VALOR', 'FALTA'], ['FALTA']]));
      qualidade = parseMoney(getFlexibleCell(row, [['DEV', 'QUALIDADE'], ['VALOR', 'QUALIDADE'], ['QUALIDADE']]));
      quebraOperacional = parseMoney(getFlexibleCell(row, [['QUEBRA', 'PARCIAL'], ['QUEBRA', 'REAL'], ['VALOR', 'QUEBRA'], ['QUEBRA']]));
      estoque = parseMoney(getFlexibleCell(row, [['ESTOQUE', 'LOJA'], ['VALOR', 'EST'], ['ESTOQUE']]));
      percentualQuebra = parsePercent(getFlexibleCell(row, [['%', 'REAL'], ['%', 'QUEBRA', 'REAL'], ['%', 'QUEBRA'], ['PERCENTUAL'], ['%', 'TOTAL']]));
    }

    if (normalizedSheet.includes('consignad') && model === 'GENERICO') {
      rede = 'CONSIGNADOS';
      qualidade = 0;
      falta = 0;
      estoque = 0;
    } else if (normalizedSheet.includes('bretas') && model === 'GENERICO') {
      rede = 'BRETAS';
      falta = 0;
      qualidade = 0;
      estoque = 0;
    } else if (normalizedSheet.includes('variados') && model === 'GENERICO') {
      rede = 'VARIADOS';
      falta = 0;
      qualidade = 0;
      quebraOperacional = 0;
      estoque = 0;
      percentualQuebra = 0;
    } else if (normalizedSheet.includes('costa') && model === 'GENERICO') {
      rede = 'COSTA';
    } else if ((normalizedSheet.includes('comper') || normalizedSheet.includes('fort')) && model === 'GENERICO') {
      rede = 'COMPER/FORT';
    } else if (normalizedSheet.includes('dia a dia') && model === 'GENERICO') {
      rede = 'ATACADÃO DIA A DIA';
    }

    const normalizedStore = normalizeStoreAndNetwork(originalStore, rede);
    rede = normalizedStore.rede || rede;
    const store = normalizedStore.loja;

    const quebraTotalCalculada = quebraOperacional + falta + qualidade;

    if ((!percentualQuebra || !Number.isFinite(percentualQuebra)) && venda > 0 && quebraTotalCalculada !== 0) {
      percentualQuebra = Number(((quebraTotalCalculada / venda) * 100).toFixed(2));
    }

    return {
      id: `${batchId}-${slugify(rede)}-${slugify(store)}-${index + 1}`,
      sourceBatchId: batchId,
      sourceFileName: fileName,
      sourceSheetName: sheetName,
      rede,
      loja: store,
      semana: weekLabel,
      monthLabel,
      monthKey,
      periodLabel,
      valorVenda: Number(venda.toFixed(2)),
      valorQuebraOperacional: Number(quebraOperacional.toFixed(2)),
      valorQuebraTotal: Number(quebraTotalCalculada.toFixed(2)),
      valorQuebra: Number(quebraTotalCalculada.toFixed(2)),
      valorFalta: Number(falta.toFixed(2)),
      valorQualidade: Number(qualidade.toFixed(2)),
      valorEstoque: rede === 'COSTA' ? Number(estoque.toFixed(2)) : 0,
      percentualQuebra: Number((percentualQuebra || 0).toFixed(2)),
      percentualFalta: venda > 0 ? Number(((falta / venda) * 100).toFixed(2)) : 0,
      percentualQualidade: venda > 0 ? Number(((qualidade / venda) * 100).toFixed(2)) : 0,
      statusQuebra: getBreakStatus(percentualQuebra).label,
      dataImportacao: importedAt,
      modeloImportacao: model || 'GENERICO'
    };
  }).filter(Boolean);

  return {
    records,
    verification: buildSheetVerification(sheetName, records, totalRow)
  };
}

function getRowLabel(row) {
  return getFlexibleCell(row, [['LOJAS'], ['LOJA', 'BRETAS'], ['LOJA'], ['NOME', 'LOJA'], ['REDES']]);
}

function getFlexibleCell(row, tokenGroups) {
  const entries = Object.entries(row || {});
  for (const group of tokenGroups || []) {
    for (const [key, value] of entries) {
      if (headerMatchesGroup(key, group)) {
        return value;
      }
    }
  }
  return '';
}

function inferNetworkBySheetName(sheetName) {
  const normalized = normalizeText(sheetName);
  if (normalized.includes('dia a dia')) return 'ATACADÃO DIA A DIA';
  if (normalized.includes('comper') || normalized.includes('fort')) return 'COMPER/FORT';
  if (normalized.includes('costa')) return 'COSTA';
  if (normalized.includes('variados')) return 'VARIADOS';
  if (normalized.includes('consignados')) return 'CONSIGNADOS';
  if (normalized.includes('bretas')) return 'BRETAS';
  if (normalized.includes('assa')) return 'ASSAÍ';
  if (normalized.includes('vivendas')) return 'VIVENDAS';
  return '';
}

function detectWorksheetModel(sheetName, row) {
  const normalized = normalizeText(sheetName);
  if (normalized.includes('dia a dia')) return 'DIA_A_DIA';
  if (normalized.includes('comper') || normalized.includes('fort')) return 'PEREIRA';
  if (normalized.includes('vivendas')) return 'VIVENDAS';
  if (normalized.includes('costa')) return 'COSTA';
  if (normalized.includes('variados')) return 'VARIADOS';
  if (normalized.includes('consignados')) return 'CONSIGNADOS';
  if (normalized.includes('assa')) return 'ASSAI';
  if (normalized.includes('bretas')) return 'BRETAS';

  if (getFlexibleCell(row, [['LOJAS', 'VIVENDAS']])) return 'VIVENDAS';
  if (getFlexibleCell(row, [['LOJAS', 'ASSAI']])) return 'ASSAI';
  if (getFlexibleCell(row, [['LOJA', 'BRETAS']])) return 'BRETAS';
  if (getFlexibleCell(row, [['ESTOQUE', 'EM', 'LOJA']])) return 'COSTA';
  if (getFlexibleCell(row, [['RECEBIDO']])) return 'CONSIGNADOS';
  if (getFlexibleCell(row, [['DEV', 'FALTA']]) && getFlexibleCell(row, [['QUEBRA', 'PARCIAL']])) return 'PEREIRA';
  if (getFlexibleCell(row, [['QUEBRA', 'REAL']])) return 'DIA_A_DIA';
  return 'GENERICO';
}

function buildSheetVerification(sheetName, records, totalRow) {
  if (!records.length) {
    return { sheetName, status: 'ok', label: 'Sem dados para conferir' };
  }
  if (!totalRow) {
    return { sheetName, status: 'ok', label: 'Sem total para conferir' };
  }

  const model = detectWorksheetModel(sheetName, totalRow || records[0] || {});
  const totals = aggregateRecords(records);
  const totalEstoque = records.reduce((sum, item) => sum + Number(item.valorEstoque || 0), 0);

  const vendaEsperada = parseMoney(getFlexibleCell(totalRow, [['VALOR', 'ENTREGA', 'TOTAL'], ['VENDA']]));
  let quebraTotalEsperada = 0;

  if (model === 'DIA_A_DIA') {
    const faltaEsperada = parseMoney(getFlexibleCell(totalRow, [['FALTA']]));
    const qualidadeEsperada = parseMoney(getFlexibleCell(totalRow, [['QUALIDADE']]));
    const quebraOperacionalEsperada = parseMoney(getFlexibleCell(totalRow, [['QUEBRA', 'REAL'], ['QUEBRA']]));
    quebraTotalEsperada = quebraOperacionalEsperada + faltaEsperada + qualidadeEsperada;
  } else if (model === 'PEREIRA') {
    const faltaEsperada = parseMoney(getFlexibleCell(totalRow, [['DEV', 'FALTA'], ['FALTA']]));
    const qualidadeEsperada = parseMoney(getFlexibleCell(totalRow, [['DEVOLUCAO', 'QUALIDADE'], ['DEV', 'QUALIDADE'], ['QUALIDADE']]));
    const quebraTotalInformada = parseMoney(getFlexibleCell(totalRow, [['QUEBRA', 'TOTAL']]));
    quebraTotalEsperada = quebraTotalInformada > 0
      ? quebraTotalInformada
      : parseMoney(getFlexibleCell(totalRow, [['QUEBRA', 'PARCIAL'], ['QUEBRA', 'REAL'], ['QUEBRA']])) + faltaEsperada + qualidadeEsperada;
  } else if (model === 'VIVENDAS') {
    quebraTotalEsperada = parseMoney(getFlexibleCell(totalRow, [['QUEBRA']]));
  } else if (model === 'COSTA') {
    const faltaEsperada = parseMoney(getFlexibleCell(totalRow, [['DEVOLUCAO', 'FALTA'], ['VALOR', 'FALTA'], ['FALTA']]));
    const qualidadeEsperada = parseMoney(getFlexibleCell(totalRow, [['DEVOLUCAO', 'QUALIDADE'], ['VALOR', 'QUALIDADE'], ['QUALIDADE']]));
    const quebraOperacionalEsperada = parseMoney(getFlexibleCell(totalRow, [['QUEBRA', 'REAL'], ['VALOR', 'QUEBRA'], ['QUEBRA']]));
    quebraTotalEsperada = quebraOperacionalEsperada + faltaEsperada + qualidadeEsperada;
  } else if (model === 'CONSIGNADOS') {
    quebraTotalEsperada = parseMoney(getFlexibleCell(totalRow, [['QUEBRA']]));
  } else if (model === 'BRETAS') {
    quebraTotalEsperada = parseMoney(getFlexibleCell(totalRow, [['QUEBRA', 'TOTAL'], ['QUEBRA']]));
  }

  const details = [
    buildVerificationDetail('Venda', vendaEsperada, totals.venda, { alwaysInclude: true })
  ];

  if (model === 'DIA_A_DIA') {
    details.push(buildVerificationDetail('Falta', parseMoney(getFlexibleCell(totalRow, [['FALTA']])), totals.falta));
    details.push(buildVerificationDetail('Qualidade', parseMoney(getFlexibleCell(totalRow, [['QUALIDADE']])), totals.qualidade));
    details.push(buildVerificationDetail('Quebra total', quebraTotalEsperada, totals.quebra));
  } else if (model === 'PEREIRA') {
    details.push(buildVerificationDetail('Falta', parseMoney(getFlexibleCell(totalRow, [['DEV', 'FALTA'], ['FALTA']])), totals.falta));
    details.push(buildVerificationDetail('Qualidade', parseMoney(getFlexibleCell(totalRow, [['DEVOLUCAO', 'QUALIDADE'], ['DEV', 'QUALIDADE'], ['QUALIDADE']])), totals.qualidade));
    details.push(buildVerificationDetail('Quebra total', quebraTotalEsperada, totals.quebra));
  } else if (model === 'VIVENDAS') {
    details.push(buildVerificationDetail('Quebra', quebraTotalEsperada, totals.quebra));
  } else if (model === 'COSTA') {
    details.push(buildVerificationDetail('Estoque atual', parseMoney(getFlexibleCell(totalRow, [['ESTOQUE', 'EM', 'LOJA'], ['VALOR', 'EST'], ['ESTOQUE']])), totalEstoque));
    details.push(buildVerificationDetail('Falta', parseMoney(getFlexibleCell(totalRow, [['DEVOLUCAO', 'FALTA'], ['VALOR', 'FALTA'], ['FALTA']])), totals.falta));
    details.push(buildVerificationDetail('Qualidade', parseMoney(getFlexibleCell(totalRow, [['DEVOLUCAO', 'QUALIDADE'], ['VALOR', 'QUALIDADE'], ['QUALIDADE']])), totals.qualidade));
    details.push(buildVerificationDetail('Quebra total', quebraTotalEsperada, totals.quebra));
  } else if (model === 'CONSIGNADOS') {
    details.push(buildVerificationDetail('Quebra', quebraTotalEsperada, totals.quebra));
  } else if (model === 'BRETAS') {
    details.push(buildVerificationDetail('Quebra', quebraTotalEsperada, totals.quebra));
  }

  const applicableDetails = details.filter(Boolean);
  const summary = summarizeVerificationDetails(applicableDetails);
  const vendaDetail = applicableDetails.find(item => item.label === 'Venda');
  const quebraDetail = applicableDetails.find(item => normalizeText(item.label).includes('quebra'));

  return {
    type: 'sheet-total',
    sheetName,
    status: summary.status,
    label: summary.label,
    expectedVenda: vendaDetail?.expected,
    actualVenda: vendaDetail?.actual,
    vendaDiff: vendaDetail?.diff,
    expectedQuebra: quebraDetail?.expected,
    actualQuebra: quebraDetail?.actual,
    quebraDiff: quebraDetail?.diff,
    details: applicableDetails
  };
}

function compareWorkbookTotals(grouped, totalSheet) {
  if (!totalSheet.length) return [];
  return totalSheet.map(summary => {
    const network = normalizeNetworkName(summary.rede);
    const found = grouped.find(item => normalizeNetworkName(item.rede) === network);
    if (!found) return null;

    const details = [
      buildVerificationDetail('Venda', Number(summary.venda || 0), Number(found.venda || 0), { alwaysInclude: true })
    ];

    if (network === 'ATACADÃO DIA A DIA' || network === 'COMPER/FORT' || network === 'COSTA') {
      details.push(buildVerificationDetail('Falta', Number(summary.falta || 0), Number(found.falta || 0)));
      details.push(buildVerificationDetail('Qualidade', Number(summary.qualidade || 0), Number(found.qualidade || 0)));
      details.push(buildVerificationDetail('Quebra total', Number(summary.quebraTotal || 0), Number(found.quebra || 0)));
      if (network === 'COSTA') {
        details.push(buildVerificationDetail('Estoque atual', Number(summary.estoque || 0), Number(found.estoque || 0)));
      }
    } else if (network === 'VIVENDAS' || network === 'CONSIGNADOS' || network === 'BRETAS') {
      if (Number(summary.quebraTotal || 0) > 0 || Number(found.quebra || 0) > 0) {
        details.push(buildVerificationDetail('Quebra', Number(summary.quebraTotal || 0), Number(found.quebra || 0)));
      }
    }

    const applicableDetails = details.filter(Boolean);
    const summaryStatus = summarizeVerificationDetails(applicableDetails);
    const vendaDetail = applicableDetails.find(item => item.label === 'Venda');
    const quebraDetail = applicableDetails.find(item => normalizeText(item.label).includes('quebra'));

    return {
      type: 'workbook-total',
      sheetName: `SÓ FOLHAS TOTAL • ${network}`,
      status: summaryStatus.status,
      label: summaryStatus.label,
      expectedVenda: vendaDetail?.expected,
      actualVenda: vendaDetail?.actual,
      vendaDiff: vendaDetail?.diff,
      expectedQuebra: quebraDetail?.expected,
      actualQuebra: quebraDetail?.actual,
      quebraDiff: quebraDetail?.diff,
      details: applicableDetails
    };
  }).filter(Boolean);
}

function isTotalRow(loja) {
  const normalized = normalizeText(loja);
  return normalized === 'total' || normalized === 'totais' || normalized.startsWith('total ');
}

function inferNetworkByStore(store) {
  const normalized = normalizeText(store);
  const rules = [
    ['ATACADÃO DIA A DIA', ['dd ', 'dia a dia', 'atacadao dia a dia', 'atacadão dia a dia']],
    ['COMPER/FORT', ['comper', 'fort']],
    ['VIVENDAS', ['vivendas']],
    ['BRETAS', ['bretas']],
    ['COSTA', ['costa']],
    ['ASSAÍ', ['assai', 'assaí']],
    ['VARIADOS', ['emporio', 'mercado', 'variado']],
    ['CONSIGNADOS', ['consignado']]
  ];
  const match = rules.find(([_, terms]) => terms.some(term => normalized.includes(term)));
  return match ? match[0] : '';
}

function setImportFeedback(message, isError) {
  els.importFeedback.textContent = message;
  els.importFeedback.className = `feedback ${isError ? 'is-error' : 'is-success'}`;
}

function getFilteredData() {
  return appState.data.filter(item => {
    const monthKey = item.monthKey || inferRecordMonthKey(item);
    const byRede = appState.filters.rede === 'Todas' || item.rede === appState.filters.rede;
    const byLoja = appState.filters.loja === 'Todas' || item.loja === appState.filters.loja;
    const byMes = appState.filters.mes === 'Todas' || monthKey === appState.filters.mes;
    const bySemana = appState.filters.semana === 'Todas' || item.semana === appState.filters.semana;
    return byRede && byLoja && byMes && bySemana;
  });
}

function shouldUseLatestMonthFallback() {
  return appState.filters.mes === 'Todas' && appState.filters.semana === 'Todas';
}

function getDisplayRecords(records) {
  return shouldUseLatestMonthFallback() ? filterToLatestImportMonth(records) : records;
}

function refreshAll() {
  const filtered = getFilteredData();
  renderTopInfo(filtered);
  renderSummaryTable(filtered);
  renderRankingTable(filtered);
  renderDetailsNetworkTabs(filtered);
  renderDetailsTable(filtered);
  updateDetailsSectionVisibility();
  renderAlerts(filtered);
  renderCharts(filtered);
  renderImportBatchesTable();
  renderAdminTable();
}

function getLatestImportMonthKey() {
  return appState.imports[0]?.monthKey || inferRecordMonthKey({ importedAt: appState.config.ultimaImportacao || appState.config.ultimaAtualizacao });
}

function filterToLatestImportMonth(records) {
  const key = getLatestImportMonthKey();
  if (!key) return records;
  return records.filter(item => (item.monthKey || inferRecordMonthKey(item)) === key);
}

function aggregateByStore(records) {
  const map = new Map();
  records.forEach(item => {
    const key = `${item.rede}||${item.loja}`;
    if (!map.has(key)) {
      map.set(key, {
        rede: item.rede,
        loja: item.loja,
        valorVenda: 0,
        valorQuebra: 0,
        valorQuebraOperacional: 0,
        valorFalta: 0,
        valorQualidade: 0,
        valorEstoque: 0
      });
    }
    const row = map.get(key);
    row.valorVenda += Number(item.valorVenda || 0);
    row.valorQuebra += Number(item.valorQuebra || 0);
    row.valorQuebraOperacional += Number(item.valorQuebraOperacional || 0);
    row.valorFalta += Number(item.valorFalta || 0);
    row.valorQualidade += Number(item.valorQualidade || 0);
    row.valorEstoque += Number(item.valorEstoque || 0);
  });
  return [...map.values()].map(row => ({
    ...row,
    percentualQuebra: row.valorVenda ? Number(((row.valorQuebra / row.valorVenda) * 100).toFixed(2)) : 0,
    percentualQuebraOperacional: row.valorVenda ? Number(((row.valorQuebraOperacional / row.valorVenda) * 100).toFixed(2)) : 0
  }));
}

function getRankingStatus(value, hasBreak = true) {
  if (!hasBreak) return { label: 'Sem quebra', className: 'status--neutral' };
  if (value <= WARNING_LIMIT) return { label: '0% a 10%', className: 'status--good' };
  if (value <= BREAK_LIMIT) return { label: '10% a 12%', className: 'status--warn' };
  return { label: 'Acima de 12%', className: 'status--bad' };
}

function renderRankingTable(filtered) {
  const monthlyRecords = getDisplayRecords(filtered);
  const fullAggregated = aggregateByStore(monthlyRecords).sort((a, b) => {
    const aScore = Number(a.percentualQuebra || 0);
    const bScore = Number(b.percentualQuebra || 0);
    return aScore - bScore || b.valorVenda - a.valorVenda;
  });

  syncRankingNetworkFilter(fullAggregated);
  const aggregated = appState.rankingRede === 'Todas'
    ? fullAggregated
    : fullAggregated.filter(item => item.rede === appState.rankingRede);

  renderRankingHighlights(aggregated);
  renderRankingPositivePanel(aggregated);
  const maxBreak = Math.max(...aggregated.map(item => Number(item.percentualQuebra || 0)), BREAK_LIMIT, 1);

  els.rankingTableBody.innerHTML = aggregated.length ? aggregated.map((item, index) => {
    const rankingStatus = getRankingStatus(item.percentualQuebra, true);
    const width = Math.max(8, Math.min(100, (Number(item.percentualQuebra || 0) / maxBreak) * 100));
    return `<tr>
      <td class="ranking-col--index"><span class="ranking-index-badge">${index + 1}</span></td>
      <td><span class="ranking-network">${item.rede}</span></td>
      <td>
        <div class="ranking-store">
          <strong>${item.loja}</strong>
          <span>${rankingStatus.label}</span>
        </div>
      </td>
      <td><span class="ranking-sale">${formatCurrency(item.valorVenda)}</span></td>
      <td class="ranking-break">
        <div class="ranking-break__top">
          <span class="ranking-break__value">${formatPercent(item.percentualQuebra)}</span>
        </div>
        <div class="ranking-break__bar"><span class="ranking-break__fill" style="width:${width}%"></span></div>
      </td>
      <td><span class="status-badge ranking-badge ${rankingStatus.className}">${rankingStatus.label}</span></td>
    </tr>`;
  }).join('') : `<tr><td colspan="6">Nenhuma loja encontrada para montar o ranking.</td></tr>`;
}

function syncRankingNetworkFilter(aggregated) {
  if (!els.rankingNetworkFilter) return;
  const networks = [...new Set(aggregated.map(item => item.rede))].sort((a, b) => a.localeCompare(b, 'pt-BR'));
  const options = ['Todas', ...networks];
  if (!options.includes(appState.rankingRede)) appState.rankingRede = 'Todas';

  const html = options.map(option => {
    const label = option === 'Todas' ? 'Todas as redes' : option;
    return `<option value="${option}">${label}</option>`;
  }).join('');

  if (els.rankingNetworkFilter.innerHTML !== html) {
    els.rankingNetworkFilter.innerHTML = html;
  }
  els.rankingNetworkFilter.value = appState.rankingRede;
}

function renderRankingHighlights(aggregated) {
  if (!els.rankingHighlights) return;
  if (!aggregated.length) {
    els.rankingHighlights.innerHTML = '';
    return;
  }

  const highestBreak = [...aggregated].sort((a, b) => Number(b.percentualQuebra || 0) - Number(a.percentualQuebra || 0) || b.valorVenda - a.valorVenda)[0];
  const highestSale = [...aggregated].sort((a, b) => b.valorVenda - a.valorVenda)[0];
  const attentionCount = aggregated.filter(item => Number(item.percentualQuebra || 0) > BREAK_LIMIT).length;
  const activeNetworkLabel = appState.rankingRede === 'Todas' ? 'todas as redes' : appState.rankingRede;

  els.rankingHighlights.innerHTML = [
    {
      label: 'Maior quebra',
      value: highestBreak.loja,
      meta: `${formatPercent(highestBreak.percentualQuebra)} • ${highestBreak.rede}`
    },
    {
      label: 'Maior venda no ranking',
      value: highestSale.loja,
      meta: `${formatCurrency(highestSale.valorVenda)} • ${highestSale.rede}`
    },
    {
      label: 'Lojas acima de 12%',
      value: `${attentionCount} ${attentionCount === 1 ? 'loja' : 'lojas'}`,
      meta: `Monitoramento de ${activeNetworkLabel}`
    }
  ].map(card => `
    <div class="ranking-highlight">
      <span class="ranking-highlight__label">${card.label}</span>
      <strong class="ranking-highlight__value">${card.value}</strong>
      <span class="ranking-highlight__meta">${card.meta}</span>
    </div>
  `).join('');
}

function getPositiveHighlightSource(items) {
  const explicitNetwork = appState.rankingRede !== 'Todas'
    ? appState.rankingRede
    : appState.filters.rede !== 'Todas'
      ? appState.filters.rede
      : 'Todas';
  const filteredItems = explicitNetwork === 'Todas'
    ? items.filter(item => PRIMARY_HIGHLIGHT_NETWORKS.includes(item.rede))
    : items.filter(item => item.rede === explicitNetwork);
  return {
    explicitNetwork,
    items: filteredItems.length ? filteredItems : items
  };
}

function renderRankingPositivePanel(aggregated) {
  if (!els.rankingPositivePanel) return;
  if (!aggregated.length) {
    els.rankingPositivePanel.innerHTML = '';
    return;
  }

  const highlightSource = getPositiveHighlightSource(aggregated);
  const bestStores = [...highlightSource.items]
    .sort((a, b) => {
      const diff = Number(a.percentualQuebra || 0) - Number(b.percentualQuebra || 0);
      return diff || b.valorVenda - a.valorVenda;
    })
    .slice(0, 3);

  const intro = highlightSource.explicitNetwork === 'Todas'
    ? 'Somente DIA A DIA, COSTA e COMPER/FORT por padrão'
    : `Resultados positivos da rede ${highlightSource.explicitNetwork}`;

  els.rankingPositivePanel.innerHTML = `
    <div class="positive-panel__header">
      <div>
        <span class="positive-panel__eyebrow">Destaques positivos</span>
        <strong class="positive-panel__title">Lojas com menor quebra</strong>
      </div>
      <span class="positive-panel__meta">${intro}</span>
    </div>
    <div class="positive-panel__cards">
      ${bestStores.map((item, index) => `
        <article class="positive-card">
          <span class="positive-card__rank">Top ${index + 1}</span>
          <strong class="positive-card__store">${item.loja}</strong>
          <span class="positive-card__network">${item.rede}</span>
          <div class="positive-card__metrics">
            <span>${formatPercent(item.percentualQuebra)} de quebra</span>
            <span>${formatCurrency(item.valorVenda)} em venda</span>
          </div>
        </article>
      `).join('')}
    </div>
  `;
}

function renderNetworkWinnersPanel(records) {
  if (!els.networkWinnersPanel) return;
  const aggregated = aggregateByStore(records);
  if (!aggregated.length) {
    els.networkWinnersPanel.innerHTML = '';
    return;
  }

  const highlightSource = getPositiveHighlightSource(aggregated);
  const winners = [...highlightSource.items]
    .sort((a, b) => Number(a.percentualQuebraOperacional || 0) - Number(b.percentualQuebraOperacional || 0) || b.valorVenda - a.valorVenda)
    .slice(0, 6);

  const intro = highlightSource.explicitNetwork === 'Todas'
    ? 'Top 6 lojas com menor quebra parcial no recorte padrão'
    : `Top 6 da rede ${highlightSource.explicitNetwork} com menor quebra parcial`;

  els.networkWinnersPanel.innerHTML = `
    <div class="network-winners__header">
      <div>
        <span class="positive-panel__eyebrow">🥇 Reconhecimento</span>
        <strong class="positive-panel__title">PROMOTORES EM DESTAQUE</strong>
      </div>
      <span class="positive-panel__meta">${intro}</span>
    </div>
    <div class="network-winners__list network-winners__list--featured">
      ${winners.map((item, index) => `
        <article class="network-winner-card network-winner-card--gold">
          <span class="network-winner-card__position">${index + 1}º</span>
          <div class="network-winner-card__content">
            <strong>${item.loja}</strong>
            <span>${item.rede} • quebra parcial ${formatPercent(item.percentualQuebraOperacional)}</span>
          </div>
          <div class="network-winner-card__metric">${formatPercent(item.percentualQuebraOperacional)}</div>
        </article>
      `).join('')}
    </div>
  `;
}

function renderDetailsNetworkTabs(filtered) {
  const availableNetworks = [...new Set(getDisplayRecords(filtered).map(item => item.rede))];
  const options = ['Todas', ...NETWORKS.map(n => n.id).filter(id => availableNetworks.includes(id))];
  if (!options.includes(appState.detailsRede)) appState.detailsRede = 'Todas';
  els.detailsNetworkTabs.innerHTML = options.map(option => {
    const label = option === 'Todas' ? 'Todas as redes' : option;
    const active = option === appState.detailsRede ? 'is-active' : '';
    return `<button type="button" class="pill-btn ${active}" data-detail-network="${option}">${label}</button>`;
  }).join('');
  els.detailsNetworkTabs.querySelectorAll('[data-detail-network]').forEach(btn => {
    btn.addEventListener('click', () => {
      appState.detailsRede = btn.dataset.detailNetwork;
      renderDetailsNetworkTabs(filtered);
      renderDetailsTable(filtered);
    });
  });
}

function renderTopInfo(filtered) {
  const displayRecords = getDisplayRecords(filtered);
  const totals = aggregateRecords(displayRecords);
  const metaTarget = resolveActiveMetaTarget(displayRecords);
  const metaPercent = metaTarget > 0 ? (totals.venda / metaTarget) * 100 : 0;
  const quebraReal = Math.max(0, totals.quebra - totals.falta - totals.qualidade);
  const percQuebraReal = totals.venda > 0 ? (quebraReal / totals.venda) * 100 : 0;
  const status = getBreakStatus(percQuebraReal);
  const stockVisible = appState.filters.rede === 'COSTA';
  const totalStock = displayRecords.reduce((sum, item) => sum + Number(item.valorEstoque || 0), 0);

  if (els.lastUpdateText) els.lastUpdateText.textContent = `Última atualização: ${formatDateTime(appState.config.ultimaAtualizacao)}`;
  els.metaPercent.textContent = `${metaPercent.toFixed(0)}%`;
  els.metaPercentInner.textContent = `${metaPercent.toFixed(0)}%`;
  els.metaLegend.textContent = `Venda atual ${formatCurrency(totals.venda)} de ${formatCurrency(metaTarget)}`;
  els.metaTotalValue.textContent = formatCurrency(metaTarget);
  els.salesTotalValue.textContent = formatCurrency(totals.venda);
  els.lastImportValue.textContent = formatDateTime(appState.config.ultimaImportacao);
  els.cardVenda.textContent = formatCurrency(totals.venda);
  els.cardQuebra.textContent = formatCurrency(totals.quebra);
  els.cardPercQuebra.textContent = formatPercent(percQuebraReal);
  els.cardFalta.textContent = formatCurrency(totals.falta);
  els.cardQualidade.textContent = formatCurrency(totals.qualidade);
  els.cardStatusQuebra.textContent = formatCurrency(quebraReal);
  els.breakRealValue.textContent = formatPercent(percQuebraReal);
  els.breakStatusBadge.textContent = status.label;
  els.breakStatusBadge.className = `status-badge ${status.className}`;
  els.stockCard.hidden = !stockVisible;
  if (stockVisible) els.cardEstoque.textContent = formatCurrency(totalStock);
  updateProgressCircle(metaPercent);
  updateViewHeader(displayRecords, totals);
}

function renderSummaryTable(filtered) {
  const displayRecords = getDisplayRecords(filtered);
  const networks = appState.filters.rede === 'Todas'
    ? NETWORKS.map(item => item.id)
    : [appState.filters.rede];

  els.summaryTableBody.innerHTML = networks.map(networkId => {
    const records = displayRecords.filter(item => item.rede === networkId);
    if (!records.length) {
      return `<tr>
        <td>${networkId}</td>
        <td>${formatCurrency(0)}</td>
        <td>${formatCurrency(appState.config.metasPorRede[networkId] || 0)}</td>
        <td>0%</td>
        <td>${formatCurrency(0)}</td>
        <td>0%</td>
        <td><span class="status-badge status--neutral">Sem dados</span></td>
      </tr>`;
    }
    const totals = aggregateRecords(records);
    const meta = appState.config.metasPorRede[networkId] || 0;
    const percentMeta = meta > 0 ? (totals.venda / meta) * 100 : 0;
    const status = getBreakStatus(totals.percQuebra);
    return `<tr>
      <td>${networkId}</td>
      <td>${formatCurrency(totals.venda)}</td>
      <td>${formatCurrency(meta)}</td>
      <td>${percentMeta.toFixed(0)}%</td>
      <td>${formatCurrency(totals.quebra)}</td>
      <td>${formatPercent(totals.percQuebra)}</td>
      <td><span class="status-badge ${status.className}">${status.label}</span></td>
    </tr>`;
  }).filter(Boolean);
}

function renderDetailsTable(filtered) {
  const hasWeekFilter = appState.filters.semana !== 'Todas';
  const monthlyRecords = getDisplayRecords(filtered);
  const recordsForDetails = appState.detailsRede !== 'Todas' ? monthlyRecords.filter(item => item.rede === appState.detailsRede) : monthlyRecords;
  const showStock = shouldShowStock(recordsForDetails);

  if (hasWeekFilter) {
    els.detailsTableTitle.textContent = 'Resultados semanais por loja';
    els.detailsTableSubtitle.textContent = 'Visão semanal exibida porque o filtro de semana foi aplicado.';
    els.detailsTableHeadRow.innerHTML = `
      <th>Rede</th>
      <th>Loja</th>
      <th>Semana</th>
      <th>Venda</th>
      <th>Meta</th>
      <th>% Meta</th>
      ${showStock ? '<th>Estoque</th>' : ''}
      <th>Quebra</th>
      <th>Falta</th>
      <th>Qualidade</th>
      <th>% Quebra</th>
      <th>Status</th>`;

    els.detailsTableBody.innerHTML = recordsForDetails.length ? recordsForDetails.map(item => {
      const meta = getRowMeta(item);
      const percentMeta = meta > 0 ? (item.valorVenda / meta) * 100 : 0;
      const status = getBreakStatus(item.percentualQuebra);
      return `<tr>
        <td>${item.rede}</td>
        <td>${item.loja}</td>
        <td>${item.semana}</td>
        <td>${formatCurrency(item.valorVenda)}</td>
        <td>${formatCurrency(meta)}</td>
        <td>${percentMeta.toFixed(0)}%</td>
        ${showStock ? `<td>${formatCurrency(item.valorEstoque)}</td>` : ''}
        <td>${formatCurrency(item.valorQuebra)}</td>
        <td>${formatCurrency(item.valorFalta)}</td>
        <td>${formatCurrency(item.valorQualidade)}</td>
        <td>${formatPercent(item.percentualQuebra)}</td>
        <td><span class="status-badge ${status.className}">${status.label}</span></td>
      </tr>`;
    }).join('') : `<tr><td colspan="${showStock ? 12 : 11}">Nenhum registro semanal encontrado para os filtros selecionados.</td></tr>`;
    return;
  }

  const aggregated = aggregateByStore(recordsForDetails).sort((a, b) => a.rede.localeCompare(b.rede) || b.valorVenda - a.valorVenda);
  els.detailsTableTitle.textContent = 'Acumulado mensal por loja';
  els.detailsTableSubtitle.textContent = appState.filters.mes !== 'Todas' ? `Exibe o acumulado de ${formatMonthFilterLabel(appState.filters.mes)}. Para ver semana a semana, aplique o filtro de semana.` : 'Exibe o acumulado do mês da última importação. Para ver semana a semana, aplique o filtro de semana.';
  els.detailsTableHeadRow.innerHTML = `
    <th>Rede</th>
    <th>Loja</th>
    <th>Venda acumulada</th>
    <th>Meta</th>
    <th>% Meta</th>
    ${showStock ? '<th>Estoque atual</th>' : ''}
    <th>Quebra</th>
    <th>Falta</th>
    <th>Qualidade</th>
    <th>% Quebra</th>
    <th>Status</th>`;

  els.detailsTableBody.innerHTML = aggregated.length ? aggregated.map(item => {
    const meta = getStoreMeta(item.rede, item.loja);
    const percentMeta = meta > 0 ? (item.valorVenda / meta) * 100 : 0;
    const status = getBreakStatus(item.percentualQuebra);
    return `<tr>
      <td>${item.rede}</td>
      <td>${item.loja}</td>
      <td>${formatCurrency(item.valorVenda)}</td>
      <td>${formatCurrency(meta)}</td>
      <td>${percentMeta.toFixed(0)}%</td>
      ${showStock ? `<td>${formatCurrency(item.valorEstoque)}</td>` : ''}
      <td>${formatCurrency(item.valorQuebra)}</td>
      <td>${formatCurrency(item.valorFalta)}</td>
      <td>${formatCurrency(item.valorQualidade)}</td>
      <td>${formatPercent(item.percentualQuebra)}</td>
      <td><span class="status-badge ${status.className}">${status.label}</span></td>
    </tr>`;
  }).join('') : `<tr><td colspan="${showStock ? 11 : 10}">Nenhum acumulado mensal encontrado para os filtros selecionados.</td></tr>`;
}

function renderAdminTable() {
  const sorted = [...appState.data].sort((a, b) => new Date(b.dataImportacao || 0) - new Date(a.dataImportacao || 0) || a.rede.localeCompare(b.rede) || a.loja.localeCompare(b.loja));
  els.adminTableBody.innerHTML = sorted.length ? sorted.map(item => `
    <tr>
      <td>${item.rede}</td>
      <td>${item.loja}</td>
      <td>${item.semana}</td>
      <td>${formatCurrency(item.valorVenda)}</td>
      <td>${formatCurrency(item.valorQuebra)}</td>
      <td>${formatCurrency(item.valorFalta)}</td>
      <td>${formatCurrency(item.valorQualidade)}</td>
      <td>${item.rede === 'COSTA' ? formatCurrency(item.valorEstoque) : '—'}</td>
      <td>${formatPercent(item.percentualQuebra)}</td>
    </tr>
  `).join('') : `<tr><td colspan="9">Nenhum registro importado até o momento.</td></tr>`;
}

function renderAlerts(filtered) {
  const displayRecords = getDisplayRecords(filtered);
  const totals = aggregateRecords(displayRecords);
  const summaryByNetwork = aggregateByNetwork(displayRecords).sort((a, b) => b.percQuebra - a.percQuebra);
  const rankingByStore = aggregateByStore(displayRecords).sort((a, b) => b.percentualQuebra - a.percentualQuebra);
  const metaTarget = resolveActiveMetaTarget(displayRecords);
  const metaPercent = metaTarget > 0 ? (totals.venda / metaTarget) * 100 : 0;

  const alerts = [];
  if (summaryByNetwork.length) {
    const topNetwork = summaryByNetwork[0];
    const tone = topNetwork.percQuebra > BREAK_LIMIT ? 'bad' : topNetwork.percQuebra > WARNING_LIMIT ? 'warn' : 'good';
    const title = tone === 'bad' ? `${topNetwork.rede} com quebra elevada` : tone === 'warn' ? `${topNetwork.rede} em atenção` : `${topNetwork.rede} dentro da meta`;
    alerts.push({ tone, icon: tone === 'bad' ? '!' : tone === 'warn' ? '⚠' : '✓', title, text: `Quebra atual de ${formatPercent(topNetwork.percQuebra)} na rede.` });
  }

  if (rankingByStore.length) {
    const worstStore = rankingByStore[0];
    const tone = worstStore.percentualQuebra > BREAK_LIMIT ? 'bad' : worstStore.percentualQuebra > WARNING_LIMIT ? 'warn' : 'good';
    alerts.push({ tone, icon: tone === 'bad' ? '!' : tone === 'warn' ? '⚠' : '✓', title: `${worstStore.loja} no radar`, text: `Loja com ${formatPercent(worstStore.percentualQuebra)} de quebra no acumulado exibido.` });
  }

  const metaTone = metaPercent >= 100 ? 'good' : metaPercent >= 85 ? 'warn' : 'bad';
  alerts.push({
    tone: metaTone,
    icon: metaTone === 'good' ? '✓' : metaTone === 'warn' ? '⚠' : '!',
    title: metaTone === 'good' ? 'Venda em crescimento' : metaTone === 'warn' ? 'Meta em acompanhamento' : 'Meta abaixo do esperado',
    text: `Performance atual em ${metaPercent.toFixed(0)}% da meta definida.`
  });

  els.alertsGrid.innerHTML = alerts.slice(0, 3).map(alert => `
    <article class="alert-card alert-card--${alert.tone}">
      <div class="alert-card__icon">${alert.icon}</div>
      <div class="alert-card__text">
        <strong>${alert.title}</strong>
        <span>${alert.text}</span>
      </div>
    </article>
  `).join('');
}

function renderCharts(filtered) {
  if (!window.Chart) return;

  const monthlyBase = getDisplayRecords(filtered);

  const salesSeries = buildSalesSeries(monthlyBase);
  upsertChart('salesTrend', els.salesTrendChart, {
    type: 'line',
    data: {
      labels: salesSeries.labels,
      datasets: [{
        label: 'Venda',
        data: salesSeries.values,
        borderColor: '#36d27c',
        backgroundColor: 'rgba(54,210,124,0.18)',
        fill: true,
        tension: 0.35,
        pointRadius: 4,
        pointHoverRadius: 5,
        borderWidth: 3
      }]
    },
    options: baseChartOptions({ currencyTicks: true })
  });

  const networkSummary = aggregateByNetwork(monthlyBase);
  upsertChart('breakByNetwork', els.breakByNetworkChart, {
    type: 'bar',
    data: {
      labels: networkSummary.map(item => item.rede),
      datasets: [{
        label: '% Quebra parcial',
        data: networkSummary.map(item => Number(item.percQuebraOperacional.toFixed(2))),
        backgroundColor: networkSummary.map(item => getBreakChartColor(item.percQuebraOperacional)),
        borderRadius: 10,
        maxBarThickness: 48
      }]
    },
    options: baseChartOptions({ percentTicks: true, legend: false })
  });

  const storeRankingSource = aggregateByStore(monthlyBase);
  const storeRanking = [...storeRankingSource]
    .filter(item => item.valorVenda > 0)
    .sort((a, b) => b.percentualQuebraOperacional - a.percentualQuebraOperacional || b.valorVenda - a.valorVenda)
    .slice(0, 10);
  upsertChart('breakByStore', els.breakByStoreChart, {
    type: 'bar',
    data: {
      labels: storeRanking.map(item => item.loja),
      datasets: [{
        label: '% Quebra parcial',
        data: storeRanking.map(item => Number(item.percentualQuebraOperacional.toFixed(2))),
        backgroundColor: storeRanking.map(item => getBreakChartColor(item.percentualQuebraOperacional)),
        borderRadius: 10,
        maxBarThickness: 26
      }]
    },
    options: baseChartOptions({ percentTicks: true, indexAxis: 'y', legend: false })
  });

  const bestPerformanceNetworks = new Set(['ATACADÃO DIA A DIA', 'COSTA', 'COMPER/FORT', 'VIVENDAS']);
  const bestStoreRanking = [...storeRankingSource]
    .filter(item => item.valorVenda > 0 && bestPerformanceNetworks.has(item.rede))
    .sort((a, b) => a.percentualQuebraOperacional - b.percentualQuebraOperacional || b.valorVenda - a.valorVenda)
    .slice(0, 10);
  upsertChart('bestBreakByStore', els.bestBreakByStoreChart, {
    type: 'bar',
    data: {
      labels: bestStoreRanking.map(item => item.loja),
      datasets: [{
        label: '% Menor quebra parcial',
        data: bestStoreRanking.map(item => Number(item.percentualQuebraOperacional.toFixed(2))),
        backgroundColor: bestStoreRanking.map(item => getBreakChartColor(item.percentualQuebraOperacional)),
        borderRadius: 10,
        maxBarThickness: 26
      }]
    },
    options: baseChartOptions({ percentTicks: true, indexAxis: 'y', legend: false })
  });

  const distribution = aggregateByNetwork(monthlyBase).filter(item => item.venda > 0);
  upsertChart('networkDistribution', els.networkDistributionChart, {
    type: 'doughnut',
    data: {
      labels: distribution.map(item => item.rede),
      datasets: [{
        label: 'Venda',
        data: distribution.map(item => item.venda),
        backgroundColor: distribution.map((_, index) => CHART_PALETTE[index % CHART_PALETTE.length]),
        borderWidth: 0,
        hoverOffset: 8
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          position: 'bottom',
          labels: { color: '#dbe8e0', padding: 18, usePointStyle: true, pointStyle: 'circle' }
        },
        tooltip: {
          callbacks: {
            label: context => `${context.label}: ${formatCurrency(context.raw)}`
          }
        }
      },
      cutout: '62%'
    }
  });

  renderNetworkWinnersPanel(monthlyBase);
}

function buildSalesSeries(records) {
  const map = new Map();
  records.forEach(item => {
    const key = item.semana || 'Sem semana';
    map.set(key, (map.get(key) || 0) + Number(item.valorVenda || 0));
  });
  const labels = [...map.keys()].sort((a, b) => weekSortValue(a) - weekSortValue(b) || a.localeCompare(b, 'pt-BR', { numeric: true }));
  return {
    labels,
    values: labels.map(label => Number((map.get(label) || 0).toFixed(2)))
  };
}

function aggregateByNetwork(records) {
  const map = new Map();
  records.forEach(item => {
    if (!map.has(item.rede)) {
      map.set(item.rede, { rede: item.rede, venda: 0, quebra: 0, quebraOperacional: 0, falta: 0, qualidade: 0, estoque: 0 });
    }
    const row = map.get(item.rede);
    row.venda += Number(item.valorVenda || 0);
    row.quebra += Number(item.valorQuebra || 0);
    row.quebraOperacional += Number(item.valorQuebraOperacional || 0);
    row.falta += Number(item.valorFalta || 0);
    row.qualidade += Number(item.valorQualidade || 0);
    row.estoque += Number(item.valorEstoque || 0);
  });
  return [...map.values()].map(item => ({
    ...item,
    percQuebra: item.venda ? (item.quebra / item.venda) * 100 : 0,
    percQuebraOperacional: item.venda ? (item.quebraOperacional / item.venda) * 100 : 0
  }));
}

function upsertChart(key, canvas, config) {
  if (!canvas) return;
  if (appState.charts[key]) {
    appState.charts[key].destroy();
  }
  appState.charts[key] = new Chart(canvas, config);
}

function baseChartOptions({ currencyTicks = false, percentTicks = false, indexAxis = 'x', legend = true } = {}) {
  return {
    responsive: true,
    maintainAspectRatio: false,
    indexAxis,
    plugins: {
      legend: legend ? { labels: { color: '#dbe8e0', usePointStyle: true } } : { display: false },
      tooltip: {
        callbacks: {
          label: context => {
            const value = context.raw;
            if (currencyTicks) return `${context.dataset.label}: ${formatCurrency(value)}`;
            if (percentTicks) return `${context.dataset.label}: ${formatPercent(value)}`;
            return `${context.dataset.label}: ${value}`;
          }
        }
      }
    },
    scales: indexAxis === 'x' ? {
      x: {
        ticks: { color: '#b3c4bc' },
        grid: { color: 'rgba(255,255,255,0.04)' }
      },
      y: {
        beginAtZero: true,
        ticks: {
          color: '#b3c4bc',
          callback: value => currencyTicks ? compactCurrency(value) : percentTicks ? `${value}%` : value
        },
        grid: { color: 'rgba(255,255,255,0.05)' }
      }
    } : {
      x: {
        beginAtZero: true,
        ticks: {
          color: '#b3c4bc',
          callback: value => percentTicks ? `${value}%` : value
        },
        grid: { color: 'rgba(255,255,255,0.05)' }
      },
      y: {
        ticks: { color: '#b3c4bc' },
        grid: { display: false }
      }
    }
  };
}

function getBreakChartColor(value) {
  if (value <= WARNING_LIMIT) return 'rgba(54,210,124,0.90)';
  if (value <= BREAK_LIMIT) return 'rgba(244,200,75,0.92)';
  return 'rgba(255,100,100,0.92)';
}

function shouldShowStock(records) {
  return records.length > 0 && records.every(item => item.rede === 'COSTA');
}

function updateViewHeader(filtered, totals) {
  let title = 'Resumo Geral da Empresa';
  const parts = [];
  if (appState.filters.rede !== 'Todas') parts.push(appState.filters.rede);
  if (appState.filters.loja !== 'Todas') parts.push(appState.filters.loja);
  if (appState.filters.mes !== 'Todas') parts.push(formatMonthFilterLabel(appState.filters.mes));
  if (appState.filters.semana !== 'Todas') parts.push(appState.filters.semana);
  if (parts.length) title = parts.join(' • ');

  els.viewTitle.textContent = title;
  if (els.viewSubtitle) els.viewSubtitle.textContent = '';

  const chips = [];
  if (appState.filters.rede !== 'Todas') chips.push(`Rede: ${appState.filters.rede}`);
  if (appState.filters.loja !== 'Todas') chips.push(`Loja: ${appState.filters.loja}`);
  if (appState.filters.mes !== 'Todas') chips.push(`Mês: ${formatMonthFilterLabel(appState.filters.mes)}`);
  if (appState.filters.semana !== 'Todas') chips.push(`Semana: ${appState.filters.semana}`);
  els.activeFiltersChips.innerHTML = chips.map(text => `<span class="chip">${text}</span>`).join('');
}

function resolveActiveMetaTarget(filtered) {
  if (appState.filters.rede !== 'Todas') {
    return appState.config.metasPorRede[appState.filters.rede] || 0;
  }
  if (appState.filters.loja !== 'Todas') {
    const store = filtered[0];
    return store ? getStoreMeta(store.rede, store.loja) : 0;
  }
  if ((appState.filters.semana !== 'Todas' || appState.filters.mes !== 'Todas') && filtered.length) {
    const uniqueWeeks = [...new Set(appState.data.map(item => item.semana))].length || 1;
    return (appState.config.metaGeral || 0) / uniqueWeeks;
  }
  return appState.config.metaGeral || 0;
}

function getRowMeta(item) {
  return getStoreMeta(item.rede, item.loja);
}

function getStoreMeta(rede, loja) {
  const monthlyNetworkRecords = getDisplayRecords(appState.data).filter(record => record.rede === rede);
  const uniqueStores = [...new Set(monthlyNetworkRecords.map(record => record.loja))].length || 1;
  return (appState.config.metasPorRede[rede] || 0) / uniqueStores;
}

function aggregateRecords(records) {
  const venda = records.reduce((sum, item) => sum + Number(item.valorVenda || 0), 0);
  const quebra = records.reduce((sum, item) => sum + Number(item.valorQuebra || 0), 0);
  const falta = records.reduce((sum, item) => sum + Number(item.valorFalta || 0), 0);
  const qualidade = records.reduce((sum, item) => sum + Number(item.valorQualidade || 0), 0);
  return {
    venda,
    quebra,
    falta,
    qualidade,
    percQuebra: venda > 0 ? (quebra / venda) * 100 : 0
  };
}

function getBreakStatus(value) {
  if (!Number.isFinite(value) || value <= 0) return { label: 'Sem dados', className: 'status--neutral' };
  if (value <= WARNING_LIMIT) return { label: 'Dentro da meta', className: 'status--good' };
  if (value <= BREAK_LIMIT) return { label: 'Atenção', className: 'status--warn' };
  return { label: 'Acima da meta', className: 'status--bad' };
}

function updateProgressCircle(percent) {
  const bounded = Math.max(0, Math.min(percent, 100));
  const offset = CIRCLE_LENGTH - (bounded / 100) * CIRCLE_LENGTH;
  els.metaCircle.style.strokeDashoffset = `${offset}`;
}

function parseMoney(value) {
  return parseLocalizedNumber(value);
}

function parsePercent(value) {
  const parsed = parseLocalizedNumber(value);
  if (!Number.isFinite(parsed)) return 0;
  if (Math.abs(parsed) <= 1) return parsed * 100;
  return parsed;
}

function parseLocalizedNumber(value) {
  if (typeof value === 'number') return Number.isFinite(value) ? value : 0;
  let sanitized = String(value ?? '').trim();
  if (!sanitized) return 0;
  sanitized = sanitized.replace(/R\$/gi, '').replace(/%/g, '').replace(/\s+/g, '');

  if (sanitized.includes(',') && sanitized.includes('.')) {
    if (sanitized.lastIndexOf(',') > sanitized.lastIndexOf('.')) {
      sanitized = sanitized.replace(/\./g, '').replace(',', '.');
    } else {
      sanitized = sanitized.replace(/,/g, '');
    }
  } else if (sanitized.includes(',')) {
    sanitized = sanitized.replace(/\./g, '').replace(',', '.');
  } else {
    sanitized = sanitized.replace(/,/g, '');
  }

  sanitized = sanitized.replace(/[^0-9.-]/g, '');
  const parsed = Number(sanitized);
  return Number.isFinite(parsed) ? parsed : 0;
}

function formatCurrency(value) {
  return new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(Number(value || 0));
}

function parseCurrencyInput(value) {
  return parseLocalizedNumber(String(value || '').replace(/R\$/gi, '').trim());
}

function setCurrencyInputValue(input, value) {
  if (!input) return;
  const numericValue = Number(value || 0);
  input.dataset.rawValue = String(numericValue);
  input.value = formatCurrency(numericValue);
}

function attachCurrencyMask(input) {
  if (!input || input.dataset.currencyBound === 'true') return;
  input.dataset.currencyBound = 'true';
  input.addEventListener('focus', () => {
    const numericValue = parseCurrencyInput(input.value);
    input.value = formatEditableCurrencyValue(numericValue);
  });
  input.addEventListener('input', () => {
    input.value = sanitizeCurrencyEditValue(input.value);
  });
  input.addEventListener('blur', () => {
    const numericValue = parseCurrencyInput(input.value);
    setCurrencyInputValue(input, numericValue);
  });
}

function sanitizeCurrencyEditValue(value) {
  let text = String(value || '').replace(/[^0-9,.-]/g, '');
  const negative = text.startsWith('-') ? '-' : '';
  text = text.replace(/-/g, '');
  const parts = text.split(/[,.]/);
  if (parts.length <= 1) return negative + parts[0];
  const decimalPart = parts.pop().slice(0, 2);
  const integerPart = parts.join('');
  return `${negative}${integerPart}${decimalPart ? ',' + decimalPart : ''}`;
}

function formatEditableCurrencyValue(value) {
  const numericValue = Number(value || 0);
  if (!Number.isFinite(numericValue) || numericValue === 0) return '';
  const isInteger = Number.isInteger(numericValue);
  if (isInteger) return String(numericValue);
  return numericValue.toFixed(2).replace('.', ',');
}

function compactCurrency(value) {
  return new Intl.NumberFormat('pt-BR', { notation: 'compact', compactDisplay: 'short' }).format(Number(value || 0));
}

function formatPercent(value) {
  return `${Number(value || 0).toFixed(2).replace('.', ',')}%`;
}

function formatDateTime(value) {
  if (!value) return '--';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '--';
  return new Intl.DateTimeFormat('pt-BR', { dateStyle: 'short', timeStyle: 'medium' }).format(date);
}

function slugify(value) {
  return normalizeText(value).replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
}

function normalizeText(value) {
  return String(value || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase();
}

function readStorage() {
  const keys = [STORAGE_KEY, ...LEGACY_STORAGE_KEYS];
  for (const key of keys) {
    try {
      const raw = localStorage.getItem(key);
      if (raw) return JSON.parse(raw);
    } catch {
      // continua tentando as próximas chaves
    }
  }
  return null;
}

function persistLocal(options = {}) {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(exportStateSnapshot()));
  if (!options.skipRemote) queueRemotePersist();
}

function getVerificationStatusLabel(status) {
  return status === 'ok' ? 'Conferido' : status === 'warn' ? 'Com alerta' : 'Divergência';
}

function getVerificationStatusBadgeClass(status) {
  return status === 'ok' ? 'conference-badge--ok' : status === 'warn' ? 'conference-badge--warn' : 'conference-badge--bad';
}

function closeVerificationModal() {
  if (!els.verificationModal) return;
  els.verificationModal.hidden = true;
}

function openVerificationModal(batchId) {
  const batch = appState.imports.find(item => item.id === batchId);
  if (!batch || !els.verificationModal) return;

  const statusText = getVerificationStatusLabel(batch.verificationStatus);
  const detailItems = (batch.verificationItems || []).filter(item => item && item.status !== 'ok');
  const itemsToRender = detailItems.length ? detailItems : (batch.verificationItems || []);

  if (els.verificationModalTitle) {
    els.verificationModalTitle.textContent = batch.verificationStatus === 'bad' ? 'Detalhes da divergência' : 'Detalhes da conferência';
  }
  if (els.verificationModalFileName) els.verificationModalFileName.textContent = batch.fileName || '—';
  if (els.verificationModalPeriod) els.verificationModalPeriod.textContent = batch.periodLabel || batch.weekLabel || '—';
  if (els.verificationModalStatus) els.verificationModalStatus.innerHTML = `<span class="conference-badge ${getVerificationStatusBadgeClass(batch.verificationStatus)}">${statusText}</span>`;

  if (els.verificationModalBody) {
    els.verificationModalBody.innerHTML = itemsToRender.length ? itemsToRender.map(item => {
      const statusBadge = `<span class="conference-badge ${getVerificationStatusBadgeClass(item.status)}">${getVerificationStatusLabel(item.status)}</span>`;
      const details = (item.details || []).filter(Boolean);
      const detailMarkup = details.length ? `
        <div class="verification-grid">
          ${details.map(detail => `
            <div class="verification-field">
              <span>${detail.label} da planilha</span>
              <strong>${formatCurrency(detail.expected || 0)}</strong>
            </div>
            <div class="verification-field">
              <span>${detail.label} apurado no sistema</span>
              <strong>${formatCurrency(detail.actual || 0)}</strong>
            </div>
            <div class="verification-field verification-field--diff">
              <span>Diferença de ${detail.label.toLowerCase()}</span>
              <strong>${formatCurrency(detail.diff || 0)}</strong>
            </div>
          `).join('')}
        </div>
      ` : (() => {
        const hasVenda = Number.isFinite(item.expectedVenda) || Number.isFinite(item.actualVenda) || Number.isFinite(item.vendaDiff);
        const hasQuebra = Number.isFinite(item.expectedQuebra) || Number.isFinite(item.actualQuebra) || Number.isFinite(item.quebraDiff);
        return `
          <div class="verification-grid">
            ${hasVenda ? `
              <div class="verification-field">
                <span>Venda da planilha</span>
                <strong>${formatCurrency(item.expectedVenda || 0)}</strong>
              </div>
              <div class="verification-field">
                <span>Venda apurada no sistema</span>
                <strong>${formatCurrency(item.actualVenda || 0)}</strong>
              </div>
              <div class="verification-field verification-field--diff">
                <span>Diferença de venda</span>
                <strong>${formatCurrency(item.vendaDiff || 0)}</strong>
              </div>
            ` : ''}
            ${hasQuebra ? `
              <div class="verification-field">
                <span>Quebra da planilha</span>
                <strong>${formatCurrency(item.expectedQuebra || 0)}</strong>
              </div>
              <div class="verification-field">
                <span>Quebra apurada no sistema</span>
                <strong>${formatCurrency(item.actualQuebra || 0)}</strong>
              </div>
              <div class="verification-field verification-field--diff">
                <span>Diferença de quebra</span>
                <strong>${formatCurrency(item.quebraDiff || 0)}</strong>
              </div>
            ` : ''}
          </div>
        `;
      })();
      return `
        <section class="verification-item verification-item--${item.status || 'ok'}">
          <div class="verification-item__head">
            <div>
              <h4>${item.sheetName || 'Conferência'}</h4>
              <p>${item.label || 'Detalhe da conferência'}</p>
            </div>
            ${statusBadge}
          </div>
          ${detailMarkup}
        </section>
      `;
    }).join('') : '<div class="verification-empty">Nenhum detalhe de conferência foi encontrado para esta planilha.</div>';
  }

  els.verificationModal.hidden = false;
}

function renderImportBatchesTable() {
  if (!els.importBatchesTableBody || !els.importBatchCount) return;
  const batches = [...appState.imports].sort((a, b) => new Date(b.importedAt || 0) - new Date(a.importedAt || 0));
  els.importBatchCount.textContent = String(batches.length);
  els.importBatchesTableBody.innerHTML = batches.length ? batches.map(batch => {
    const badgeClass = getVerificationStatusBadgeClass(batch.verificationStatus);
    const badgeText = getVerificationStatusLabel(batch.verificationStatus);
    const detailButton = batch.verificationStatus !== 'ok'
      ? `<button type="button" class="btn btn--ghost btn--sm" data-view-verification="${batch.id}">${batch.verificationStatus === 'bad' ? 'Ver divergência' : 'Ver alerta'}</button>`
      : '';
    return `<tr>
      <td>
        <div class="table-file">
          <strong>${batch.fileName}</strong>
          <small>${batch.verificationItems?.length || 0} conferências</small>
        </div>
      </td>
      <td>${batch.periodLabel || batch.weekLabel || '—'}</td>
      <td>${formatDateTime(batch.importedAt)}</td>
      <td>${batch.recordCount || 0}</td>
      <td>${formatCurrency(batch.totalVenda || 0)}</td>
      <td>${formatCurrency(batch.totalQuebra || 0)}</td>
      <td><span class="conference-badge ${badgeClass}">${badgeText}</span></td>
      <td>
        <div class="table-actions">
          ${detailButton}
          <button type="button" class="btn btn--danger btn--sm" data-delete-batch="${batch.id}">Excluir</button>
        </div>
      </td>
    </tr>`;
  }).filter(Boolean).join('') : `<tr><td colspan="8">Nenhuma planilha anexada até o momento.</td></tr>`;
}

function handleImportBatchAction(event) {
  const detailButton = event.target.closest('[data-view-verification]');
  if (detailButton) {
    openVerificationModal(detailButton.dataset.viewVerification);
    return;
  }

  const button = event.target.closest('[data-delete-batch]');
  if (!button) return;
  const batchId = button.dataset.deleteBatch;
  const batch = appState.imports.find(item => item.id === batchId);
  if (!batch) return;
  const confirmed = window.confirm(`Excluir a planilha "${batch.fileName}" e remover todos os dados importados por ela?`);
  if (!confirmed) return;

  appState.imports = appState.imports.filter(item => item.id !== batchId);
  appState.data = appState.data.filter(item => item.sourceBatchId !== batchId);
  appState.config.ultimaImportacao = appState.imports[0]?.importedAt || null;
  appState.config.ultimaAtualizacao = new Date().toISOString();
  closeVerificationModal();
  persistLocal();
  syncLojaOptions();
  syncSemanaOptions();
  refreshAll();
  setImportFeedback(`Planilha ${batch.fileName} excluída com sucesso.`, false);
}

function looksLikeSampleData(data, imports) {
  if (!Array.isArray(data) || !data.length || (imports && imports.length)) return false;
  const sampleStores = ['dd goiania sul', 'comper centro', 'fort norte', 'vivendas 01', 'consignado a', 'costa campinas'];
  const hits = data.filter(item => sampleStores.includes(normalizeText(item.loja))).length;
  return hits >= 3;
}

function closeAllSelectMenus(exceptionMenu) {
  Object.values(appState.customSelects).forEach(select => {
    if (select.menu !== exceptionMenu) select.menu.hidden = true;
  });
}

function getOptionLabel(options, value) {
  return options.find(option => option.value === value)?.label || value;
}
