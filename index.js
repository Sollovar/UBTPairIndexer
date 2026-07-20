import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import cron from 'node-cron';
import { WebSocketServer } from 'ws';
import pkg from 'pg';

const { Pool } = pkg;

// ── Parse DATABASE_URL only when DB_* vars are not already set ─────────────
if (process.env.DATABASE_URL) {
  try {
    const u = new URL(process.env.DATABASE_URL);
    if (!process.env.DB_HOST) process.env.DB_HOST = u.hostname;
    if (!process.env.DB_PORT) process.env.DB_PORT = u.port || '5432';
    if (!process.env.DB_USER) process.env.DB_USER = u.username;
    if (!process.env.DB_PASSWORD) process.env.DB_PASSWORD = u.password;
    if (!process.env.DB_NAME) process.env.DB_NAME = u.pathname.replace(/^\//, '');
  } catch (_) {}
}

// ── Fall back to Replit PG* vars if still not set ────────────────────────────
if (!process.env.DB_HOST && process.env.PGHOST)         process.env.DB_HOST     = process.env.PGHOST;
if (!process.env.DB_USER && process.env.PGUSER)         process.env.DB_USER     = process.env.PGUSER;
if (!process.env.DB_PASSWORD && process.env.PGPASSWORD) process.env.DB_PASSWORD = process.env.PGPASSWORD;
if (!process.env.DB_NAME && process.env.PGDATABASE)     process.env.DB_NAME     = process.env.PGDATABASE;
if (!process.env.DB_PORT && process.env.PGPORT)         process.env.DB_PORT     = process.env.PGPORT;

// ── Prefer the local Docker database for development testing ────────────────
if (!process.env.DB_HOST) process.env.DB_HOST = '127.0.0.1';
if (!process.env.DB_PORT) process.env.DB_PORT = '55432';
if (!process.env.DB_USER) process.env.DB_USER = 'postgres';
if (!process.env.DB_PASSWORD) process.env.DB_PASSWORD = 'Supabase123!';
if (!process.env.DB_NAME) process.env.DB_NAME = 'postgres';

// ── Mandatory secrets check ───────────────────────────────────────────────────
const REQUIRED_SECRETS = ['DB_HOST', 'DB_USER', 'DB_PASSWORD'];
const missing = REQUIRED_SECRETS.filter(k => !process.env[k]);
if (missing.length > 0) {
  console.error(`\n[FATAL] Missing required Replit Secrets: ${missing.join(', ')}`);
  console.error('Go to the Secrets tab in Replit and add the missing values.\n');
  process.exit(1);
}

// Strip protocol prefix and trailing slash from host (handles "http://host/" → "host")
function sanitizeHost(raw) {
  if (!raw) return raw;
  return raw.trim().replace(/^https?:\/\//, '').replace(/\/$/, '');
}

const dbHost = sanitizeHost(process.env.DB_HOST);
const dbUser = process.env.DB_USER;
const dbPassword = process.env.DB_PASSWORD;
const dbName = process.env.DB_NAME || 'postgres';
const dbPort = parseInt(process.env.DB_PORT || '5432');

console.log(`DB connecting → host=${dbHost} user=${dbUser} db=${dbName} password_length=${dbPassword.length}`);

const isLocalDb = dbHost === 'helium' || dbHost === 'localhost' || dbHost === '127.0.0.1';
const pool = new Pool({
  host: dbHost,
  port: dbPort,
  user: dbUser,
  password: dbPassword,
  database: dbName,
  ssl: isLocalDb ? false : { rejectUnauthorized: false },
});

const app = express();
const PORT = 3001;

app.use(cors());
app.use(express.json());

const SUPPORTED_CHAINS = ['bsc', 'base', 'solana'];
const GECKO_API_BASE = 'https://api.geckoterminal.com/api/v2';

const pairs = new Map();
let orderbookCache = {};

const RATE_LIMIT_CONFIG = {
  MIN_DELAY_MS: parseInt(process.env.MIN_REQUEST_DELAY_MS) || 20000,
  CHAIN_FETCH_DELAY_MS: parseInt(process.env.CHAIN_FETCH_DELAY_MS) || 120000,
  MAX_BATCH_SIZE: parseInt(process.env.MAX_BATCH_SIZE) || 2,
  RETRY_COUNT: parseInt(process.env.RETRY_COUNT) || 5,
  RETRY_BACKOFF_MS: parseInt(process.env.RETRY_BACKOFF_MS) || 5000,
};

const requestQueue = [];
let isProcessing = false;

const fetchWithRetry = async (url, retries = RATE_LIMIT_CONFIG.RETRY_COUNT) => {
  for (let i = 0; i < retries; i++) {
    try {
      const response = await fetch(url, {
        headers: {
          'Accept': 'application/json',
          'User-Agent': 'UNBOUND-Bot/1.0'
        }
      });
      if (!response.ok) {
        if (response.status === 429) {
          const retryAfter = parseInt(response.headers.get('Retry-After') || '30', 10);
          const waitMs = (Number.isNaN(retryAfter) ? 30000 : retryAfter * 1000) * (i + 1);
          console.log(`Rate limited! Waiting ${waitMs / 1000}s before retry ${i + 1}/${retries}...`);
          await new Promise(r => setTimeout(r, waitMs));
          continue;
        }
        if (response.status >= 500 && i < retries - 1) {
          const waitMs = RATE_LIMIT_CONFIG.RETRY_BACKOFF_MS * (i + 1);
          await new Promise(r => setTimeout(r, waitMs));
          continue;
        }
        throw new Error(`HTTP ${response.status}`);
      }
      return await response.json();
    } catch (err) {
      if (i === retries - 1) throw err;
      const waitMs = 1000 * Math.pow(2, i);
      await new Promise(r => setTimeout(r, waitMs));
    }
  }
  throw new Error(`Failed to fetch ${url} after ${retries} retries`);
};

const enqueueRequest = async (fn) => {
  return new Promise((resolve, reject) => {
    requestQueue.push({ fn, resolve, reject });
    if (!isProcessing) processQueue();
  });
};

const processQueue = async () => {
  if (isProcessing || requestQueue.length === 0) return;
  isProcessing = true;
  while (requestQueue.length > 0) {
    const batch = requestQueue.splice(0, RATE_LIMIT_CONFIG.MAX_BATCH_SIZE);
    await Promise.allSettled(batch.map(item => item.fn().then(item.resolve).catch(item.reject)));
    if (requestQueue.length > 0) {
      await new Promise(r => setTimeout(r, RATE_LIMIT_CONFIG.MIN_DELAY_MS));
    }
  }
  isProcessing = false;
};

const getPoolInfo = async (network, poolAddress) => {
  const url = `${GECKO_API_BASE}/networks/${network}/pools/${poolAddress}/info?include=pool`;
  return fetchWithRetry(url);
};

const getTrendingPools = async (network, page = 1, duration = '6h') => {
  const url = `${GECKO_API_BASE}/networks/${network}/trending_pools?include=base_token,quote_token&include_gt_community_data=false&page=${page}&duration=${duration}`;
  return fetchWithRetry(url);
};

const getPoolDetails = async (network, poolAddresses) => {
  const addressesParam = poolAddresses.join(',');
  const url = `${GECKO_API_BASE}/networks/${network}/pools/multi/${addressesParam}?include=base_token,quote_token&include_volume_breakdown=false&include_composition=false`;
  return fetchWithRetry(url);
};

const findTokenInIncluded = (included, tokenId) => {
  if (!included || !tokenId) return null;
  return included.find(t => t.id === tokenId);
};

const isSolanaNetwork = (network) => network === 'solana';
const normalizeAddress = (address, network) => {
  if (!address) return '';
  return isSolanaNetwork(network) ? address : address.toLowerCase();
};

const extractTokenMetadata = (tokenData, network) => {
  if (!tokenData || !tokenData.attributes) return null;
  const attrs = tokenData.attributes;
  let imageThumb = '', imageSmall = '', imageLarge = '';
  if (attrs.image && typeof attrs.image === 'object') {
    imageThumb = attrs.image.thumb || '';
    imageSmall = attrs.image.small || '';
    imageLarge = attrs.image.large || '';
  }
  let websites = [];
  if (Array.isArray(attrs.websites)) {
    websites = attrs.websites.filter(w => typeof w === 'string' && w);
  }
  return {
    address: normalizeAddress(attrs.address || '', network),
    name: attrs.name || '',
    symbol: attrs.symbol || '',
    decimals: parseInt(attrs.decimals, 10) || 18,
    description: attrs.description || '',
    image_url: attrs.image_url || '',
    image_thumb: imageThumb,
    image_small: imageSmall,
    image_large: imageLarge,
    websites,
    twitter_handle: attrs.twitter_handle || '',
    telegram_handle: attrs.telegram_handle || '',
    discord_url: attrs.discord_url || '',
    gt_score: parseFloat(attrs.gt_score) || 0,
    gt_verified: attrs.gt_verified === true,
    coingecko_id: attrs.coingecko_coin_id || ''
  };
};

const getTokenAddress = (tokenData, network) =>
  normalizeAddress(tokenData?.attributes?.address || '', network);

const mapPoolInfoTokens = (poolInfoData, baseAddress, quoteAddress, network) => {
  const result = { base: null, quote: null };
  if (!poolInfoData?.data || !Array.isArray(poolInfoData.data)) return result;
  for (const item of poolInfoData.data) {
    if (!item || item.type !== 'token' || !item.attributes) continue;
    const address = getTokenAddress(item, network);
    const tokenMetadata = extractTokenMetadata(item, network);
    if (!address || !tokenMetadata) continue;
    if (address === baseAddress) result.base = tokenMetadata;
    else if (address === quoteAddress) result.quote = tokenMetadata;
  }
  return result;
};

// ─── DB helpers ──────────────────────────────────────────────────────────────

const ensurePairsTable = async () => {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS pairs (
      id TEXT PRIMARY KEY,
      network TEXT,
      pair_address TEXT,
      dex_name TEXT,
      base_token JSONB,
      quote_token JSONB,
      base_symbol TEXT,
      quote_symbol TEXT,
      dex TEXT,
      pool_address TEXT,
      base_token_decimals INTEGER DEFAULT 18,
      quote_token_decimals INTEGER DEFAULT 18,
      base_token_info JSONB,
      quote_token_info JSONB,
      pool_name TEXT,
      market_cap_usd NUMERIC DEFAULT 0,
      market_cap NUMERIC DEFAULT 0,
      created_at TIMESTAMPTZ,
      indexed_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  const alterStatements = [
    "ALTER TABLE pairs ADD COLUMN IF NOT EXISTS pair_address TEXT",
    "ALTER TABLE pairs ADD COLUMN IF NOT EXISTS dex_name TEXT",
    "ALTER TABLE pairs ADD COLUMN IF NOT EXISTS base_token JSONB",
    "ALTER TABLE pairs ADD COLUMN IF NOT EXISTS quote_token JSONB",
    "ALTER TABLE pairs ADD COLUMN IF NOT EXISTS base_symbol TEXT",
    "ALTER TABLE pairs ADD COLUMN IF NOT EXISTS quote_symbol TEXT",
    "ALTER TABLE pairs ADD COLUMN IF NOT EXISTS dex TEXT",
    "ALTER TABLE pairs ADD COLUMN IF NOT EXISTS pool_address TEXT",
    "ALTER TABLE pairs ADD COLUMN IF NOT EXISTS base_token_decimals INTEGER DEFAULT 18",
    "ALTER TABLE pairs ADD COLUMN IF NOT EXISTS quote_token_decimals INTEGER DEFAULT 18",
    "ALTER TABLE pairs ADD COLUMN IF NOT EXISTS base_token_info JSONB",
    "ALTER TABLE pairs ADD COLUMN IF NOT EXISTS quote_token_info JSONB",
    "ALTER TABLE pairs ADD COLUMN IF NOT EXISTS pool_name TEXT",
    "ALTER TABLE pairs ADD COLUMN IF NOT EXISTS market_cap_usd NUMERIC DEFAULT 0",
    "ALTER TABLE pairs ADD COLUMN IF NOT EXISTS market_cap NUMERIC DEFAULT 0",
    "ALTER TABLE pairs ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ",
    "ALTER TABLE pairs ADD COLUMN IF NOT EXISTS indexed_at TIMESTAMPTZ DEFAULT NOW()"
  ];

  for (const statement of alterStatements) {
    try {
      await pool.query(statement);
    } catch (err) {
      console.warn(`Schema migration warning: ${err.message}`);
    }
  }
};

const savePairsToDB = async (pairsData) => {
  if (!pairsData || pairsData.length === 0) return false;
  try {
    for (const p of pairsData) {
      await pool.query(`
        INSERT INTO pairs (
          id, network, pair_address, dex_name, base_token, quote_token,
          base_symbol, quote_symbol, dex, pool_address,
          base_token_decimals, quote_token_decimals,
          base_token_info, quote_token_info, pool_name,
          market_cap_usd, market_cap, created_at, indexed_at
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,NOW())
        ON CONFLICT (id) DO UPDATE SET
          network = EXCLUDED.network,
          pair_address = EXCLUDED.pair_address,
          dex_name = EXCLUDED.dex_name,
          base_token = EXCLUDED.base_token,
          quote_token = EXCLUDED.quote_token,
          base_symbol = EXCLUDED.base_symbol,
          quote_symbol = EXCLUDED.quote_symbol,
          dex = EXCLUDED.dex,
          pool_address = EXCLUDED.pool_address,
          base_token_decimals = EXCLUDED.base_token_decimals,
          quote_token_decimals = EXCLUDED.quote_token_decimals,
          base_token_info = EXCLUDED.base_token_info,
          quote_token_info = EXCLUDED.quote_token_info,
          pool_name = EXCLUDED.pool_name,
          market_cap_usd = EXCLUDED.market_cap_usd,
          market_cap = EXCLUDED.market_cap,
          indexed_at = NOW()
      `, [
        p.id, p.network, p.pair_address, p.dex_name,
        p.base_token, p.quote_token,
        p.base_symbol, p.quote_symbol, p.dex, p.pool_address,
        p.base_token_decimals, p.quote_token_decimals,
        p.base_token_info, p.quote_token_info,
        p.pool_name, p.market_cap_usd || 0, p.market_cap || 0,
        p.created_at || null
      ]);
    }
    console.log(`Saved ${pairsData.length} pairs to DB`);
    return true;
  } catch (err) {
    console.error('DB save error:', err.message);
    return false;
  }
};

const fetchPairsFromDB = async (network) => {
  try {
    // Explicitly select columns, EXCLUDING volume_24h and volume_24h_usd
    // Volume must ONLY come from fills calculated by the backend, never from GeckoTerminal
    const selectColumns = `
      id, network, pair_address, dex_name, base_token, quote_token,
      base_symbol, quote_symbol, dex, pool_address,
      base_token_decimals, quote_token_decimals,
      base_token_info, quote_token_info, pool_name,
      price, price_usd, price_change_24h, 
      price_high_24h, price_low_24h,
      liquidity, liquidity_usd,
      market_cap_usd, market_cap,
      created_at, indexed_at, updated_at
    `;
    
    let query = `SELECT ${selectColumns} FROM pairs ORDER BY indexed_at DESC LIMIT 200`;
    const params = [];
    if (network) {
      query = `SELECT ${selectColumns} FROM pairs WHERE network = $1 ORDER BY indexed_at DESC LIMIT 200`;
      params.push(network);
    }
    const { rows } = await pool.query(query, params);
    return rows;
  } catch (err) {
    console.error('DB fetch error:', err.message);
    return null;
  }
};

// ─── GeckoTerminal sync ───────────────────────────────────────────────────────

const syncTrendingPairs = async () => {
  console.log('Starting GeckoTerminal sync...');
  let totalSynced = 0;

  for (const network of SUPPORTED_CHAINS) {
    try {
      console.log(`Fetching trending pools from ${network}...`);
      const trendingData = await getTrendingPools(network, 1, '6h');
      if (!trendingData.data || trendingData.data.length === 0) {
        console.log(`No trending pools found for ${network}`);
        continue;
      }

      const poolAddresses = trendingData.data
        .map(pool => normalizeAddress(pool.attributes.address, network))
        .slice(0, 20);

      const multiPoolData = await getPoolDetails(network, poolAddresses);
      if (!multiPoolData.data) continue;

      const included = multiPoolData.included || [];

      for (const pool of multiPoolData.data) {
        const attrs = pool.attributes;
        const relationships = pool.relationships;
        const baseTokenId = relationships?.base_token?.data?.id;
        const quoteTokenId = relationships?.quote_token?.data?.id;

        const baseTokenData = findTokenInIncluded(included, baseTokenId);
        const quoteTokenData = findTokenInIncluded(included, quoteTokenId);

        const normalizedPairAddress = normalizeAddress(attrs.address, network);
        const pairId = `${network}_${normalizedPairAddress}`;
        const baseSymbol = baseTokenData?.attributes?.symbol || attrs.name?.split('/')[0]?.trim() || '???';
        const quoteSymbol = quoteTokenData?.attributes?.symbol || attrs.name?.split('/')[1]?.replace(/\s*\d+(\.\d+)?%?$/, '')?.trim() || '???';

        if ((network === 'bsc' && quoteSymbol.toUpperCase() === 'BNB') ||
          (network === 'base' && ['ETH', 'MUSD', 'FIETH'].includes(quoteSymbol.toUpperCase()))) {
          continue;
        }

        let baseTokenInfo = baseTokenData ? extractTokenMetadata(baseTokenData, network) : null;
        let quoteTokenInfo = quoteTokenData ? extractTokenMetadata(quoteTokenData, network) : null;
        let baseTokenDecimals = baseTokenInfo?.decimals || 18;
        let quoteTokenDecimals = quoteTokenInfo?.decimals || 18;
        const baseTokenAddress = baseTokenInfo?.address || normalizeAddress(baseTokenData?.attributes?.address, network) || '';
        const quoteTokenAddress = quoteTokenInfo?.address || normalizeAddress(quoteTokenData?.attributes?.address, network) || '';

        if (baseTokenAddress && quoteTokenAddress) {
          try {
            const poolInfoData = await enqueueRequest(() =>
              getPoolInfo(network, normalizedPairAddress)
            );
            const mapped = mapPoolInfoTokens(poolInfoData, baseTokenAddress, quoteTokenAddress, network);
            if (mapped.base) { baseTokenInfo = mapped.base; baseTokenDecimals = mapped.base.decimals || baseTokenDecimals; }
            if (mapped.quote) { quoteTokenInfo = mapped.quote; quoteTokenDecimals = mapped.quote.decimals || quoteTokenDecimals; }
          } catch (err) {
            console.log(`⚠ Could not fetch pool info for ${pairId}: ${err.message}`);
          }
        }

        if (!baseTokenInfo) baseTokenInfo = { address: '', name: baseSymbol, symbol: baseSymbol, decimals: baseTokenDecimals };
        if (!quoteTokenInfo) quoteTokenInfo = { address: '', name: quoteSymbol, symbol: quoteSymbol, decimals: quoteTokenDecimals };

        pairs.set(pairId, {
          id: pairId,
          network,
          pair_address: normalizedPairAddress,
          dex_name: attrs.pool_name?.includes('PancakeSwap') ? 'PancakeSwap' :
            attrs.pool_name?.includes('Uniswap') ? 'Uniswap' :
            attrs.pool_name?.includes('Aero') ? 'Aero' :
            attrs.pool_name?.includes('Raydium') ? 'Raydium' :
            attrs.pool_name?.split(' ')[0] || 'DEX',
          base_token: { address: baseTokenInfo.address, name: baseTokenInfo.name, symbol: baseTokenInfo.symbol, logo: baseTokenInfo.image_url || baseTokenInfo.image_large || '', decimals: baseTokenInfo.decimals },
          quote_token: { address: quoteTokenInfo.address, name: quoteTokenInfo.name, symbol: quoteTokenInfo.symbol, logo: quoteTokenInfo.image_url || quoteTokenInfo.image_large || '', decimals: quoteTokenInfo.decimals },
          base_symbol: baseTokenInfo?.symbol || '',
          quote_symbol: quoteTokenInfo?.symbol || '',
          dex: attrs.pool_name?.split(' ')[0] || 'DEX',
          pool_address: normalizedPairAddress,
          base_token_decimals: baseTokenDecimals,
          quote_token_decimals: quoteTokenDecimals,
          base_token_info: baseTokenInfo,
          quote_token_info: quoteTokenInfo,
          pool_name: attrs.pool_name || attrs.name,
          market_cap_usd: attrs.market_cap_usd || 0,
          market_cap: Math.floor(attrs.market_cap_usd || 0),
          created_at: attrs.pool_created_at,
          indexed_at: new Date().toISOString()
        });
        totalSynced++;
      }

      console.log(`Synced ${multiPoolData.data.length} pools from ${network}`);

      if (network !== SUPPORTED_CHAINS[SUPPORTED_CHAINS.length - 1]) {
        await new Promise(r => setTimeout(r, RATE_LIMIT_CONFIG.CHAIN_FETCH_DELAY_MS));
      }
    } catch (error) {
      console.error(`Error syncing ${network}:`, error.message);
    }
  }

  console.log(`Total synced: ${totalSynced} pairs`);
  return totalSynced;
};

const initializePairs = async () => {
  await ensurePairsTable();
  const cachedPairs = await fetchPairsFromDB();
  if (cachedPairs && cachedPairs.length > 0) {
    cachedPairs.forEach(pair => pairs.set(pair.id, pair));
    console.log(`Loaded ${pairs.size} pairs from DB`);
  } else {
    console.log('No cached pairs in DB, fetching from GeckoTerminal...');
  }

  const count = await syncTrendingPairs();
  const pairsArray = Array.from(pairs.values());
  await savePairsToDB(pairsArray);
  console.log(`Initial sync complete: ${count} pairs loaded`);
};

cron.schedule('*/15 * * * *', async () => {
  console.log('Running scheduled GeckoTerminal sync...');
  await syncTrendingPairs();
  const pairsArray = Array.from(pairs.values());
  await savePairsToDB(pairsArray);
});

// ─── API Endpoints ────────────────────────────────────────────────────────────

const handlePairsList = async (req, res) => {
  const { network } = req.query;
  let dbPairs = await fetchPairsFromDB(network);
  if (dbPairs && dbPairs.length > 0) return res.json(dbPairs);
  let memoryPairs = Array.from(pairs.values());
  if (network) memoryPairs = memoryPairs.filter(p => p.network === network);
  res.json(memoryPairs);
};

const handleTrendingPairs = async (req, res) => {
  const { network } = req.query;
  let dbPairs = await fetchPairsFromDB(network);
  if (dbPairs && dbPairs.length > 0) {
    return res.json(dbPairs.sort((a, b) => (b.trending_score || 0) - (a.trending_score || 0)));
  }
  let memoryPairs = Array.from(pairs.values());
  if (network) memoryPairs = memoryPairs.filter(p => p.network === network);
  res.json(memoryPairs.sort((a, b) => (b.trendingScore || 0) - (a.trendingScore || 0)));
};

const handlePairById = (req, res) => {
  const pair = pairs.get(req.params.id);
  if (!pair) return res.status(404).json({ error: 'Pair not found' });
  res.json(pair);
};

const handleSyncPairs = async (req, res) => {
  const count = await syncTrendingPairs();
  res.json({ success: true, count: pairs.size });
};

const handleCleanup = async (req, res) => {
  try {
    const bnb = await pool.query(`DELETE FROM pairs WHERE network='bsc' AND quote_symbol='BNB' RETURNING id`);
    const base = await pool.query(`DELETE FROM pairs WHERE network='base' AND quote_symbol IN ('ETH','MUSD','FIETH') RETURNING id`);
    res.json({ success: true, deleted_bnb: bnb.rowCount, deleted_base: base.rowCount });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};

app.get('/api/pairs', handlePairsList);
app.get('/api/v1/pairs', handlePairsList);
app.get('/api/pairs/trending', handleTrendingPairs);
app.get('/api/v1/pairs/trending', handleTrendingPairs);
app.get('/api/pairs/:id', handlePairById);
app.get('/api/v1/pairs/:id', handlePairById);
app.post('/api/pairs/sync', handleSyncPairs);
app.post('/api/v1/pairs/sync', handleSyncPairs);
app.post('/api/pairs/cleanup', handleCleanup);
app.post('/api/v1/pairs/cleanup', handleCleanup);

app.get('/health', (req, res) => res.json({ status: 'ok', pairs: pairs.size }));

const server = app.listen(PORT, () => {
  console.log(`Pair indexer server running on port ${PORT}`);
  initializePairs().catch(err => console.error('Init error:', err));
});

const wss = new WebSocketServer({ server });
wss.on('connection', (ws) => {
  ws.send(JSON.stringify({ type: 'connected', pairsCount: pairs.size }));
});
