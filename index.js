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
if (!process.env.DB_PORT) process.env.DB_PORT = '55422';
if (!process.env.DB_USER) process.env.DB_USER = 'postgres';
if (!process.env.DB_PASSWORD) process.env.DB_PASSWORD = 'postgres';
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
  MIN_DELAY_MS: parseInt(process.env.MIN_REQUEST_DELAY_MS) || 10000,
  CHAIN_FETCH_DELAY_MS: parseInt(process.env.CHAIN_FETCH_DELAY_MS) || 10000,
  RETRY_COUNT: parseInt(process.env.RETRY_COUNT) || 5,
  RETRY_BACKOFF_MS: parseInt(process.env.RETRY_BACKOFF_MS) || 5000,
};

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
          const retryAfterHeader = parseInt(response.headers.get('Retry-After') || '0', 10);
          // Never trust Retry-After: 0 — always wait at least 30s on rate limit,
          // plus exponential backoff per retry attempt.
          const baseWait = Math.max(retryAfterHeader * 1000, 30000);
          const waitMs = baseWait * (i + 1);
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

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const getTrendingPools = async (network, page = 10, duration = '24h') => {
  const url = `${GECKO_API_BASE}/networks/${network}/trending_pools?include=base_token,quote_token&include_gt_community_data=false&page=${page}&duration=${duration}`;
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

// ─── DB helpers ──────────────────────────────────────────────────────────────

const ensurePairsTable = async () => {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS pairs (
      id TEXT PRIMARY KEY,
      network TEXT,
      pair_address TEXT,
      dex_id TEXT,
      dex_name TEXT,
      pool_type TEXT,
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
    "ALTER TABLE pairs ADD COLUMN IF NOT EXISTS dex_id TEXT COMMENT 'GeckoTerminal dex.id: pancakeswap_v2, pancakeswap_v3, pancakeswap_infinity, uniswap_v2, uniswap_v3, etc.'",
    "ALTER TABLE pairs ADD COLUMN IF NOT EXISTS dex_name TEXT",
    "ALTER TABLE pairs ADD COLUMN IF NOT EXISTS pool_type TEXT COMMENT 'Pool version: v2, v3, v4, infinity, bin'",
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

// Map GeckoTerminal dex.id to pool type
const mapDexIdToPoolType = (dexId) => {
  if (!dexId) return 'v2'; // Default to V2
  const id = dexId.toLowerCase();
  if (id.includes('v3')) return 'v3';
  if (id.includes('v4')) return 'v4';
  if (id.includes('infinity')) return 'infinity';
  if (id.includes('bin') || id.includes('lbamm')) return 'bin';
  if (id.includes('stable') || id.includes('curve')) return 'stable';
  return 'v2'; // Default
};

const savePairsToDB = async (pairsData) => {
  if (!pairsData || pairsData.length === 0) return false;
  try {
    for (const p of pairsData) {
      await pool.query(`
        INSERT INTO pairs (
          id, network, pair_address, dex_id, dex_name, pool_type, base_token, quote_token,
          base_symbol, quote_symbol, dex, pool_address,
          base_token_decimals, quote_token_decimals,
          base_token_info, quote_token_info, pool_name,
          market_cap_usd, market_cap, created_at, indexed_at
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,NOW())
        ON CONFLICT (id) DO UPDATE SET
          network = EXCLUDED.network,
          pair_address = EXCLUDED.pair_address,
          dex_id = EXCLUDED.dex_id,
          dex_name = EXCLUDED.dex_name,
          pool_type = EXCLUDED.pool_type,
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
        p.id, p.network, p.pair_address, p.dex_id, p.dex_name, p.pool_type,
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
      id, network, pair_address, dex_id, dex_name, pool_type, dex, base_token, quote_token,
      base_symbol, quote_symbol, pool_address,
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
      const trendingData = await getTrendingPools(network);
      if (!trendingData.data || trendingData.data.length === 0) {
        console.log(`No trending pools found for ${network}`);
        continue;
      }

      const pools = trendingData.data.slice(0, 20);

      for (const [index, pool] of pools.entries()) {
        const attrs = pool.attributes || {};
        const relationships = pool.relationships || {};
        const normalizedPairAddress = normalizeAddress(attrs.address || '', network);
        if (!normalizedPairAddress) {
          console.warn(`Skipping ${network} trending pool with missing address`);
          continue;
        }

        const pairId = `${network}_${normalizedPairAddress}`;
        const existingPair = pairs.get(pairId) || {};
        const [rawBaseSymbol, rawQuoteSymbol] = (attrs.name || existingPair.pool_name || 'UNKNOWN/UNKNOWN').split('/').map(s => s.trim());
        const baseSymbol = rawBaseSymbol || existingPair.base_symbol || 'UNKNOWN';
        const quoteSymbol = rawQuoteSymbol || existingPair.quote_symbol || 'UNKNOWN';
        const dexId = relationships?.dex?.data?.id || attrs.dex_id || existingPair.dex_id || '';
        const dexName = dexId ? dexId.split('_')[0] : existingPair.dex_name || '';
        const poolType = dexId ? mapDexIdToPoolType(dexId) : existingPair.pool_type || '';
        const dex = dexId ? dexId.split('_')[0] : existingPair.dex || '';

        // Extract full token metadata from GeckoTerminal's included array
        const included = trendingData.included || [];
        const baseTokenId = relationships?.base_token?.data?.id;
        const quoteTokenId = relationships?.quote_token?.data?.id;
        
        const baseTokenData = findTokenInIncluded(included, baseTokenId);
        const quoteTokenData = findTokenInIncluded(included, quoteTokenId);
        
        const baseTokenMeta = extractTokenMetadata(baseTokenData, network);
        const quoteTokenMeta = extractTokenMetadata(quoteTokenData, network);
        
        // Build base_token with real address and decimals, fallback to existing or defaults
        const baseToken = baseTokenMeta ? {
          address: baseTokenMeta.address || existingPair.base_token?.address || '',
          name: baseTokenMeta.name || baseSymbol,
          symbol: baseTokenMeta.symbol || baseSymbol,
          logo: baseTokenMeta.image_thumb || baseTokenMeta.image_url || existingPair.base_token?.logo || '',
          decimals: baseTokenMeta.decimals || existingPair.base_token?.decimals || 18
        } : (existingPair.base_token || { address: '', name: baseSymbol, symbol: baseSymbol, logo: '', decimals: 18 });
        
        // Build quote_token with real address and decimals
        const quoteToken = quoteTokenMeta ? {
          address: quoteTokenMeta.address || existingPair.quote_token?.address || '',
          name: quoteTokenMeta.name || quoteSymbol,
          symbol: quoteTokenMeta.symbol || quoteSymbol,
          logo: quoteTokenMeta.image_thumb || quoteTokenMeta.image_url || existingPair.quote_token?.logo || '',
          decimals: quoteTokenMeta.decimals || existingPair.quote_token?.decimals || 18
        } : (existingPair.quote_token || { address: '', name: quoteSymbol, symbol: quoteSymbol, logo: '', decimals: 18 });

        pairs.set(pairId, {
          id: pairId,
          network,
          pair_address: normalizedPairAddress,
          dex_id: dexId,
          dex_name: dexName,
          pool_type: poolType,
          base_token: baseToken,
          quote_token: quoteToken,
          base_symbol: baseToken.symbol,
          quote_symbol: quoteToken.symbol,
          dex,
          pool_address: normalizedPairAddress,
          base_token_decimals: baseToken.decimals,
          quote_token_decimals: quoteToken.decimals,
          base_token_info: baseTokenMeta || existingPair.base_token_info || null,
          quote_token_info: quoteTokenMeta || existingPair.quote_token_info || null,
          pool_name: attrs.name || existingPair.pool_name || '',
          market_cap_usd: existingPair.market_cap_usd || 0,
          market_cap: existingPair.market_cap || 0,
          created_at: attrs.pool_created_at || existingPair.created_at || null,
          indexed_at: new Date().toISOString()
        });
        totalSynced++;
        
        const baseAddr = baseToken.address ? baseToken.address.substring(0, 8) : 'no-addr';
        const quoteAddr = quoteToken.address ? quoteToken.address.substring(0, 8) : 'no-addr';
        console.log(`✓ ${pairId}: ${baseToken.symbol}/${quoteToken.symbol} (base:${baseAddr}.../${baseToken.decimals}d quote:${quoteAddr}.../${quoteToken.decimals}d) dex_id=${dexId || 'unknown'}`);
      }

      console.log(`Synced ${pools.length} pools from ${network}`);

      if (network !== SUPPORTED_CHAINS[SUPPORTED_CHAINS.length - 1]) {
        await sleep(RATE_LIMIT_CONFIG.CHAIN_FETCH_DELAY_MS);
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
    // Already have pairs — wait 60s before hitting GeckoTerminal on boot
    // to avoid immediate rate-limit on fly.io shared egress IPs.
    console.log('Waiting 60s before first GeckoTerminal sync to avoid rate limits...');
    await sleep(60000);
  } else {
    console.log('No cached pairs in DB, fetching from GeckoTerminal...');
    // No pairs yet — small delay before first fetch
    await sleep(10000);
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
