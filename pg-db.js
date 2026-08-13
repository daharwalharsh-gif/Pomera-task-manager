// ══════════════════════════════════════════════════════════════════
// pg-db.js — PostgreSQL backed in-memory database adapter
// ──────────────────────────────────────────────────────────────────
// • Drop-in replacement for sheets-db.js AND mysql2/promise pool:
//   db.query / db.execute / db.getConnection() — server.js me koi
//   change nahi chahiye (bas require ./pg-db).
// • Internally alasql (in-memory SQL engine) use karta hai — saari
//   reads MEMORY se aati hain (microseconds). server.js ki saari
//   MySQL-flavored SQL waise hi chalti hai jaise Sheets version me.
// • Writes pehle memory me (instant response), phir background me ek
//   debounced flush PostgreSQL pe (1.5 sec baad) — poori table ka
//   snapshot DELETE + bulk INSERT ek transaction me.
// • Connection `.env` se: PG_HOST, PG_PORT, PG_USER, PG_PASSWORD,
//   PG_DATABASE (ya ek single DATABASE_URL). Pehli baar tables auto
//   create hoti hain + default admin seed (admin@admin.com / admin).
// ══════════════════════════════════════════════════════════════════

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const alasql = require('alasql');
const { Pool } = require('pg');

// ── Config ─────────────────────────────────────────────────────────
const FLUSH_DEBOUNCE_MS = 1500;
const MAX_CELL_CHARS = 45000;       // keep parity with sheets blob behaviour
const BLOB_DIR = path.join(__dirname, 'data', 'blobs');

// ── LOCAL mode (DB_BACKEND=local) ──────────────────────────────────
// Postgres ke BINA chalane ke liye. Query engine wahi alasql rehta hai —
// bas snapshot Postgres ki jagah ek local JSON file me save hota hai.
// Dev / demo / "pehle dekh lo" ke liye. Production me PG_* bhar ke
// DB_BACKEND=pg (ya hybrid) use karo — code path bilkul same hai.
const LOCAL_MODE = String(process.env.DB_BACKEND || '').toLowerCase() === 'local';

// ── MYSQL mode (DB_BACKEND=mysql) ──────────────────────────────────
// Hostinger / cPanel jaisi hosting MySQL deti hai, Postgres nahi.
// Query engine wahi alasql rehta hai — sirf boot par LOAD aur write par
// FLUSH MySQL se hota hai. Server.js me kuch badalna nahi padta.
const MYSQL_MODE = String(process.env.DB_BACKEND || '').toLowerCase() === 'mysql';
const LOCAL_FILE = process.env.LOCAL_DB_FILE || path.join(__dirname, 'data', 'local-db.json');

// ── Schema (same as sheets-db.js — authoritative column order) ──────
// `cols` = column order ki authoritative list
// `autoFill` = INSERT pe agar column miss hai to ye default fill hoga
const SCHEMA = {
  users: {
    cols: ['id','name','title','email','notification_email','password','role','phone','profile_image','department','week_off','extra_off'],
    autoFill: {}
  },
  delegation_tasks: {
    cols: ['id','description','assigned_to','assigned_by','due_date','status','priority','approval','waiting_approval','remarks','created_at','last_reminder_date','completed_at'],
    autoFill: { created_at: 'NOW' }
  },
  checklist_tasks: {
    cols: ['id','description','assigned_to','assigned_by','due_date','status','priority','remarks','frequency','created_at','completed_at'],
    autoFill: { created_at: 'NOW' }
  },
  task_approvals: {
    cols: ['id','task_id','task_type','requested_by','requested_to','action_type','status','note','created_at'],
    autoFill: { created_at: 'NOW' }
  },
  task_transfers: {
    cols: ['id','task_id','task_type','from_user','to_user','requested_by','status','note','created_at'],
    autoFill: { created_at: 'NOW' }
  },
  task_comments: {
    cols: ['id','task_id','task_type','user_id','comment','created_at'],
    autoFill: { created_at: 'NOW' }
  },
  week_plans: {
    cols: ['id','employee_id','hod_id','start_date','target_count','improvement_pct','created_at','updated_at'],
    autoFill: { created_at: 'NOW', updated_at: 'NOW' }
  },
  fms_sheets: {
    cols: ['id','fms_name','sheet_name','sheet_id','header_row','total_steps','created_by','created_at'],
    autoFill: { created_at: 'NOW' }
  },
  fms_steps: {
    cols: ['id','fms_id','step_order','step_name','plan_col','actual_col','extra_input','extra_col','show_cols','delay_reason_col','doer_name_col','doer_filter_col','doer_filter_map'],
    autoFill: {}
  },
  fms_step_doers: {
    cols: ['id','step_id','user_id'],
    autoFill: {}
  },
  fms_extra_rows: {
    cols: ['id','step_id','row_label','col_letter','field_type','dropdown_options'],
    autoFill: {}
  },
  // Leave Tracker — user leave/WFH/extra-working applications. Admin approves.
  leave_tracker: {
    cols: ['id','user_id','type','reason','start_date','end_date','hours','status','applied_at','decided_by','decided_at','decision_note'],
    autoFill: { applied_at: 'NOW' }
  },
  // Open Challenges — party ki challenge/complaint, responsible person + resolution tracking.
  // files = JSON array [{name, link}] (upload /api/challenges/upload-file se)
  challenges: {
    cols: ['id','party_name','received_date','known_date','description','crm','responsible_to','priority','proposed_resolution','status','files','done_remarks','done_files','done_at','done_by','created_by','created_at','updated_at'],
    autoFill: { created_at: 'NOW', updated_at: 'NOW' }
  },
  // Attendance — Excel/Google Sheet se import kiya hua daily punch data
  attendance: {
    cols: ['id','emp_id','emp_name','user_id','attn_date','in_time','out_time','total_hrs','os_hrs','status','remarks','created_at','updated_at'],
    autoFill: { created_at: 'NOW', updated_at: 'NOW' }
  },
  // Catalogues — naam + PDF (PDF fms_files me, link yahan)
  catalogues: {
    cols: ['id','name','file_name','file_link','sort_order','created_by','created_at'],
    autoFill: { created_at: 'NOW' }
  }
};

const TABLE_NAMES = Object.keys(SCHEMA);

// Which tables THIS adapter owns (loads from + flushes to Postgres). Default
// = all. In hybrid mode (hybrid-db.js) this is narrowed to e.g.
// ['users','checklist_tasks'] so the rest can live on Google Sheets. The
// alasql in-memory engine is a shared singleton, so cross-backend JOINs still
// work — only load/flush is scoped to these tables.
let _managed = TABLE_NAMES.slice();
function setManagedTables(list) {
  if (Array.isArray(list) && list.length) _managed = list.filter(t => SCHEMA[t]);
}

// Integer columns — DB se text/numeric aa sakti hain, alasql me daalne se
// pehle parse karte hain taaki SQL me arithmetic/IN comparisons sahi chalein.
const INT_COLS = new Set([
  'id','assigned_to','assigned_by','user_id','task_id','requested_by','requested_to',
  'employee_id','hod_id','target_count','improvement_pct','fms_id','step_id','step_order',
  'total_steps','header_row','from_user','to_user','waiting_approval','created_by','decided_by','sort_order'
]);

// ══════════════════════════════════════════════════════════════════
// ALASQL CUSTOM FUNCTIONS (MySQL compatibility) — identical to sheets-db
// ══════════════════════════════════════════════════════════════════
function isoDate() { return new Date().toISOString().slice(0,10); }
function isoDateTime() { return new Date().toISOString().slice(0,19).replace('T',' '); }

alasql.fn.DATE_FORMAT = function (d, fmt) {
  if (d == null || d === '') return null;
  let s = String(d);
  if (fmt === '%Y-%m-%d') return s.length >= 10 ? s.slice(0,10) : s;
  const dt = new Date(s);
  if (isNaN(dt.getTime())) return s.slice(0,10);
  const y = dt.getFullYear();
  const m = String(dt.getMonth()+1).padStart(2,'0');
  const day = String(dt.getDate()).padStart(2,'0');
  return String(fmt).replace('%Y',y).replace('%m',m).replace('%d',day);
};
alasql.fn.CURDATE = isoDate;
alasql.fn.NOW = isoDateTime;
alasql.fn.CURRENT_TIMESTAMP = isoDateTime;
alasql.fn.YEAR = (d) => {
  if (!d) return null;
  const s = String(d);
  return parseInt(s.slice(0,4), 10) || null;
};

// ══════════════════════════════════════════════════════════════════
// STATE
// ══════════════════════════════════════════════════════════════════
let _pool = null;
let _initialized = false;
let _initPromise = null;

const _dirtyTables = new Set();
let _flushTimer = null;
let _flushInProgress = false;
let _pendingFlushResolvers = [];

const _nextId = {};

// ══════════════════════════════════════════════════════════════════
// BLOB STORAGE (for large cells like profile images) — parity w/ sheets
// ══════════════════════════════════════════════════════════════════
function ensureBlobDir() {
  if (!fs.existsSync(BLOB_DIR)) fs.mkdirSync(BLOB_DIR, { recursive: true });
}
function blobStore(value) {
  ensureBlobDir();
  const hash = crypto.createHash('md5').update(value).digest('hex');
  const file = path.join(BLOB_DIR, `${hash}.txt`);
  if (!fs.existsSync(file)) fs.writeFileSync(file, value, 'utf8');
  return `blob:${hash}`;
}
function blobLoad(ref) {
  const hash = String(ref).slice(5);
  const file = path.join(BLOB_DIR, `${hash}.txt`);
  try { return fs.readFileSync(file, 'utf8'); } catch (_) { return ''; }
}
function serializeForDb(v) {
  if (v === null || v === undefined) return null;
  const s = String(v);
  if (s.length > MAX_CELL_CHARS) return blobStore(s);
  return s;
}
function deserializeFromDb(v) {
  if (typeof v === 'string' && v.startsWith('blob:')) return blobLoad(v);
  return v;
}
function parseCellValue(col, raw) {
  let v = deserializeFromDb(raw);
  if (v === undefined || v === null || v === '') {
    return INT_COLS.has(col) ? null : '';
  }
  if (INT_COLS.has(col)) {
    const n = parseInt(v, 10);
    return Number.isNaN(n) ? null : n;
  }
  return String(v);
}

// ══════════════════════════════════════════════════════════════════
// POSTGRES CLIENT
// ══════════════════════════════════════════════════════════════════
function buildPoolConfig() {
  // Prefer a single connection string if provided (DATABASE_URL / PG_URL).
  const url = (process.env.DATABASE_URL || process.env.PG_URL || '').trim();
  if (url) {
    return { connectionString: url, ssl: pgSsl() };
  }
  // Discrete config — robust for usernames/db-names with special chars
  // (e.g. "My-DB" / "App -DB") that are painful to URL-encode.
  // Serverless (Vercel): pool max=8 taaki per-request reload (8 tables Promise.all)
  // ek hi wave me chale — 3 rakhne se queries queue hoti thi aur load par kuch
  // instances ka reload adhoora/slow hota tha (data blink karta tha). 8 tables ke
  // liye 8 connections ideal. idle 10s me release taaki shared PG ke 100 slots free
  // rahein (leftover local test servers band karna is footgun ka asli ilaaj hai).
  const isServerless = !!(process.env.VERCEL || process.env.AWS_LAMBDA_FUNCTION_NAME);
  return {
    host: (process.env.PG_HOST || '').trim(),
    port: parseInt(process.env.PG_PORT || '5432', 10),
    user: process.env.PG_USER,
    password: process.env.PG_PASSWORD,
    database: process.env.PG_DATABASE,
    ssl: pgSsl(),
    max: parseInt(process.env.PG_POOL_MAX || (isServerless ? '8' : '10'), 10),
    idleTimeoutMillis: isServerless ? 10000 : 30000,
    connectionTimeoutMillis: 15000
  };
}
function pgSsl() {
  const mode = (process.env.PG_SSL || '').toLowerCase();
  if (mode === 'require' || mode === 'true' || mode === '1') {
    return { rejectUnauthorized: false };
  }
  return false;
}
// MySQL connection. Variable ke naam ka koi ek convention zaroori nahi —
// MYSQL_* pehle, phir DB_* (jaise DB_HOST/DB_USER/DB_NAME/DB_PASSWORD),
// phir PG_*. Jo bhi hosting panel me daala ho, wahi uth jayega.
// Agar exact naam se login fail ho jaaye to lowercase wala try karte hain
// (neeche mysqlEnsureConnectable dekho). Ye us retry ka result rakhta hai.
let _mysqlCaseOverride = null;

function buildMysqlConfig() {
  const url = (process.env.MYSQL_URL || '').trim();
  const base = {
    waitForConnections: true,
    connectionLimit: parseInt(process.env.MYSQL_POOL_MAX || '8', 10),
    charset: 'utf8mb4',
    connectTimeout: 20000,
    enableKeepAlive: true,
    // Shared hosting par bade INSERT chalte hain — packet limit se bachne ke liye
    // hum khud chunk karte hain (writeTablesToPg dekho).
  };
  const ssl = /^(1|true|yes|require)$/i.test(process.env.MYSQL_SSL || '')
    ? { rejectUnauthorized: false } : undefined;
  if (url) return Object.assign({ uri: url }, base, ssl ? { ssl } : {});
  return Object.assign({
    host: (process.env.MYSQL_HOST || process.env.DB_HOST || process.env.PG_HOST || 'localhost').trim(),
    port: parseInt(process.env.MYSQL_PORT || process.env.DB_PORT || '3306', 10),
    user: _mysqlCaseOverride ? _mysqlCaseOverride.user
        : String(process.env.MYSQL_USER || process.env.DB_USER || process.env.PG_USER || '').trim(),
    password: String(process.env.MYSQL_PASSWORD || process.env.DB_PASSWORD || process.env.PG_PASSWORD || '').trim(),
    database: _mysqlCaseOverride ? _mysqlCaseOverride.database
        : String(process.env.MYSQL_DATABASE || process.env.DB_NAME || process.env.DB_DATABASE || process.env.PG_DATABASE || '').trim(),
  }, base, ssl ? { ssl } : {});
}

function getPool() {
  if (_pool) return _pool;
  if (MYSQL_MODE) {
    const mysql = require('mysql2/promise');
    const cfg = buildMysqlConfig();
    // Password kabhi print nahi — sirf yeh ki set hai ya nahi.
    console.log('  🔌 MySQL: ' + (cfg.uri ? '(MYSQL_URL se)' :
      cfg.user + '@' + cfg.host + ':' + cfg.port + ' db=' + cfg.database +
      ' password=' + (cfg.password ? cfg.password.length + ' chars' : 'KHAALI')));
    _pool = mysql.createPool(cfg);
    _pool.on('error', (err) => console.error('  ❌ MySQL pool error:', err.message));
    return _pool;
  }
  _pool = new Pool(buildPoolConfig());
  _pool.on('error', (err) => console.error('  ❌ PG pool error:', err.message));
  return _pool;
}

// pg: pool.query() -> { rows }   |   mysql2: pool.query() -> [rows, fields]
async function dbRows(pool, sql, params) {
  const res = await pool.query(sql, params);
  return MYSQL_MODE ? (res[0] || []) : (res.rows || []);
}
function qIdent(name) {
  return MYSQL_MODE
    ? '`' + String(name).replace(/`/g, '``') + '`'
    : '"' + String(name).replace(/"/g, '""') + '"';
}

// ══════════════════════════════════════════════════════════════════
// LOAD — read every table from PG and (re)populate alasql.
// ══════════════════════════════════════════════════════════════════
// LOCAL mode: saari tables ek JSON file se padho (wahi shape jo PG deta hai).
function loadAllTablesLocal() {
  let store = {};
  try { store = JSON.parse(fs.readFileSync(LOCAL_FILE, 'utf8')) || {}; }
  catch (_) { store = {}; }   // file abhi bani hi nahi — fresh start
  let totalRows = 0;
  for (const table of _managed) {
    const cols = SCHEMA[table].cols;
    const raws = Array.isArray(store[table]) ? store[table] : [];
    const inserts = [];
    let maxId = 0;
    for (const raw of raws) {
      const obj = {};
      for (const col of cols) obj[col] = parseCellValue(col, raw[col]);
      if (obj.id && typeof obj.id === 'number' && obj.id > maxId) maxId = obj.id;
      inserts.push(obj);
    }
    if (alasql.tables[table]) alasql.tables[table].data = inserts;
    _nextId[table] = maxId + 1;
    totalRows += inserts.length;
  }
  _lastReloadTs = Date.now();
  return totalRows;
}

async function loadAllTables(pool) {
  if (LOCAL_MODE) return loadAllTablesLocal();
  // PARALLEL load — saari managed tables ka SELECT ek saath. Serverless pe
  // har request se pehle reload hota hai, isliye 8 sequential round-trips ki
  // jagah ~1 round-trip = har request bahut fast.
  const results = await Promise.all(_managed.map(async (table) => {
    const cols = SCHEMA[table].cols;
    const colList = cols.map(qIdent).join(', ');
    let rows = [];
    try {
      const res = await pool.query(`SELECT ${colList} FROM ${qIdent(table)}`);
      rows = MYSQL_MODE ? (res[0] || []) : (res.rows || []);
    } catch (err) {
      // Table might not exist yet on a fresh DB — treat as empty.
      rows = [];
    }
    const inserts = [];
    let maxId = 0;
    for (const raw of rows) {
      const obj = {};
      for (const col of cols) obj[col] = parseCellValue(col, raw[col]);
      if (obj.id && typeof obj.id === 'number' && obj.id > maxId) maxId = obj.id;
      inserts.push(obj);
    }
    return { table, inserts, maxId };
  }));
  let totalRows = 0;
  for (const { table, inserts, maxId } of results) {
    if (alasql.tables[table]) alasql.tables[table].data = inserts;
    _nextId[table] = maxId + 1;
    totalRows += inserts.length;
  }
  _lastReloadTs = Date.now();
  return totalRows;
}

// Per-request reload ko throttle karne ke liye — warm instance pe 3 sec ke
// andar aayi requests dobara DB load nahi karti (PG_RELOAD_TTL_MS se tunable).
let _lastReloadTs = 0;

// Force a fresh reload from PG. Skips while a flush is mid-flight or there
// are unsaved writes, so we don't clobber pending changes.
// force=true  → TTL ignore (mutations se pehle ZAROORI: flush full-table-rewrite
//   hai, isliye stale memory se overwrite na ho — warna dusre instance ka data
//   mit jaata hai). force=false (reads) → TTL throttle se fast.
async function reload(force) {
  if (!_initialized) return init();
  if (_testMode) return;
  if (LOCAL_MODE) return;   // single process — memory hi authoritative hai
  if (_flushInProgress || _dirtyTables.size > 0) return;
  if (!force) {
    const ttl = parseInt(process.env.PG_RELOAD_TTL_MS || '3000', 10);
    if (ttl > 0 && (Date.now() - _lastReloadTs) < ttl) return;
  }
  const pool = getPool();
  await loadAllTables(pool);
}

// ══════════════════════════════════════════════════════════════════
// INIT — ensure tables exist in PG, then load into alasql
// ══════════════════════════════════════════════════════════════════
// MySQL me "ADD COLUMN IF NOT EXISTS" nahi hota, isliye pehle information_schema
// se maujood columns padh lete hain aur sirf missing hi add karte hain.
// Sab kuch 2 queries me — cold start slow nahi hota.
// MySQL username/database case-sensitive hote hain. Hosting panel me galti se
// CAPITAL type ho jaana bahut common hai. Isliye: pehle jaisa diya hai waisa
// hi try karo; sirf "Access denied" aane par ek baar lowercase se try karo.
// Sahi config kabhi nahi badalti — retry tabhi hota hai jab pehla fail ho chuka ho.
async function mysqlEnsureConnectable() {
  try {
    await getPool().query('SELECT 1');
    return;
  } catch (err) {
    const denied = err && (err.code === 'ER_ACCESS_DENIED_ERROR' || err.code === 'ER_DBACCESS_DENIED_ERROR');
    const rawUser = String(process.env.MYSQL_USER || process.env.DB_USER || process.env.PG_USER || '').trim();
    const rawDb   = String(process.env.MYSQL_DATABASE || process.env.DB_NAME || process.env.DB_DATABASE || process.env.PG_DATABASE || '').trim();
    const canRetry = denied && !_mysqlCaseOverride &&
                     (rawUser !== rawUser.toLowerCase() || rawDb !== rawDb.toLowerCase());
    if (!canRetry) throw err;

    console.warn('  ⚠️  "' + rawUser + '" se access denied — lowercase se try kar rahe hain…');
    try { await _pool.end(); } catch (_) {}
    _pool = null;
    _mysqlCaseOverride = { user: rawUser.toLowerCase(), database: rawDb.toLowerCase() };
    await getPool().query('SELECT 1');
    console.warn('  ✅ lowercase se connect ho gaya. Hosting ke environment variables me');
    console.warn('     MYSQL_USER aur MYSQL_DATABASE lowercase kar dena behtar hai.');
  }
}

async function ensureSchemaMysql(pool, wantMigrate) {
  const [tRows] = await pool.query(
    `SELECT table_name AS t FROM information_schema.tables
     WHERE table_schema = DATABASE() AND table_name IN (?)`, [_managed]);
  const have = new Set(tRows.map(r => r.t));
  const missing = _managed.filter(t => !have.has(t));

  // MEDIUMTEXT (16MB) — TEXT sirf 64KB hai aur profile image jaisi badi cell
  // usme kat jaati (blob cap 45,000 chars hai, UTF-8 me 64KB se upar ja sakta hai).
  const colType = c => (c === 'id' ? 'INT NOT NULL PRIMARY KEY' : 'MEDIUMTEXT');
  for (const table of missing) {
    const defs = SCHEMA[table].cols.map(c => qIdent(c) + ' ' + colType(c)).join(', ');
    await pool.query(`CREATE TABLE IF NOT EXISTS ${qIdent(table)} (${defs})
                      ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
  }

  // Jo tables pehle se thi unme naye columns (jaise attendance ke) add karo
  const existing = _managed.filter(t => have.has(t));
  if (!existing.length) return;
  const [cRows] = await pool.query(
    `SELECT table_name AS t, column_name AS c FROM information_schema.columns
     WHERE table_schema = DATABASE() AND table_name IN (?)`, [existing]);
  const byTable = {};
  cRows.forEach(r => { (byTable[r.t] = byTable[r.t] || new Set()).add(r.c); });
  for (const table of existing) {
    const has = byTable[table] || new Set();
    for (const c of SCHEMA[table].cols) {
      if (has.has(c)) continue;
      await pool.query(`ALTER TABLE ${qIdent(table)} ADD COLUMN ${qIdent(c)} ${colType(c)}`);
      console.log(`  🔧 MySQL: ${table}.${c} column add kiya`);
    }
  }
}

async function ensureSchema(pool) {
  // FAST PATH: ek hi query se check karo kaunsi tables pehle se hain. Sab maujood
  // ho (aur PG_MIGRATE nahi) to koi DDL nahi — cold start pe ~80 ALTER round-trips
  // bach jaate hain. DDL sirf tab jab table missing ho ya PG_MIGRATE=1 diya ho.
  const wantMigrate = /^(1|true|yes)$/i.test(process.env.PG_MIGRATE || '');

  if (MYSQL_MODE) return ensureSchemaMysql(pool, wantMigrate);

  const { rows } = await pool.query(
    `SELECT table_name FROM information_schema.tables
     WHERE table_schema='public' AND table_name = ANY($1)`, [_managed]);
  const have = new Set(rows.map(r => r.table_name));
  const missing = _managed.filter(t => !have.has(t));
  if (!missing.length && !wantMigrate) return; // sab maujood — turant nikal jao

  for (const table of (wantMigrate ? _managed : missing)) {
    const cols = SCHEMA[table].cols;
    const colDefs = cols.map(c => {
      if (c === 'id') return `${qIdent(c)} INTEGER PRIMARY KEY`;
      return `${qIdent(c)} TEXT`;
    }).join(', ');
    await pool.query(`CREATE TABLE IF NOT EXISTS ${qIdent(table)} (${colDefs})`);
    if (wantMigrate) {
      // Missing columns add karo — sirf explicit migrate pe (schema evolve hua ho).
      for (const c of cols) {
        if (c === 'id') continue;
        await pool.query(`ALTER TABLE ${qIdent(table)} ADD COLUMN IF NOT EXISTS ${qIdent(c)} TEXT`);
      }
    }
  }
}

async function init() {
  if (_initialized) return;
  if (_initPromise) return _initPromise;
  _initPromise = (async () => {
    try {
      const pool = LOCAL_MODE ? null : getPool();

      // 1. Create alasql in-memory tables (id INT, rest STRING) — same as
      //    sheets-db: bulk load bypasses the PK index so `id` stays plain INT
      //    and uniqueness is managed via the _nextId counter.
      for (const t of _managed) {
        const colsSql = SCHEMA[t].cols
          .map(c => `\`${c}\` ${c==='id' ? 'INT' : 'STRING'}`)
          .join(', ');
        alasql(`CREATE TABLE IF NOT EXISTS ${t} (${colsSql})`);
      }

      // 2. Ensure tables + columns exist (local mode me koi DDL nahi)
      if (MYSQL_MODE) await mysqlEnsureConnectable();
      if (!LOCAL_MODE) await ensureSchema(MYSQL_MODE ? getPool() : pool);

      // 3. Load managed tables into alasql
      const totalRows = await loadAllTables(pool);
      console.log(LOCAL_MODE
        ? `  ✅ Local DB loaded: ${totalRows} rows across ${_managed.length} tables → ${LOCAL_FILE}`
        : MYSQL_MODE
        ? `  ✅ MySQL DB loaded: ${totalRows} rows across ${_managed.length} tables`
        : `  ✅ PostgreSQL DB loaded: ${totalRows} rows across ${_managed.length} tables (${_managed.join(', ')})`);

      // 4. Seed default admin if users table is empty (PLAIN TEXT password)
      //    Only when THIS adapter owns the users table.
      const userCount = _managed.includes('users') ? alasql('SELECT COUNT(*) AS c FROM users')[0].c : 1;
      if (userCount === 0) {
        alasql(
          'INSERT INTO users (id,name,email,notification_email,password,role,phone,profile_image,department,week_off,extra_off) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
          [1, 'Admin', 'admin@admin.com', '', 'admin', 'admin', '', '', '', '', '']
        );
        _nextId.users = 2;
        markDirty('users');
        console.log('  🌱 Seeded default admin: admin@admin.com / admin');
      }

      _initialized = true;

      // 5. Flush-on-exit — best-effort save before process exits
      const flushAndExit = async () => {
        try { await flushNow(); } catch(_) {}
        process.exit(0);
      };
      process.on('SIGINT', flushAndExit);
      process.on('SIGTERM', flushAndExit);

    } catch (err) {
      _initPromise = null;
      throw err;
    }
  })();
  return _initPromise;
}

// ══════════════════════════════════════════════════════════════════
// SQL PREPROCESSING — identical to sheets-db.js (alasql runs the query)
// ══════════════════════════════════════════════════════════════════
function escapeAliases(sql) {
  return sql.replace(/'(?:[^'\\]|\\.)*'|\bAS\s+(\w+)\b/gi, (match, alias) => {
    if (!alias) return match;
    return `AS \`${alias}\``;
  });
}

function detectMutationTable(sql) {
  const s = sql.replace(/^\s+/, '');
  let m;
  if (/^INSERT/i.test(s)) {
    m = s.match(/INSERT\s+(?:IGNORE\s+)?INTO\s+`?(\w+)`?/i);
  } else if (/^UPDATE/i.test(s)) {
    m = s.match(/UPDATE\s+`?(\w+)`?/i);
  } else if (/^DELETE/i.test(s)) {
    m = s.match(/DELETE\s+FROM\s+`?(\w+)`?/i);
  } else {
    return null;
  }
  return m ? m[1] : null;
}

function expandBulkInsert(sql, params) {
  const m = sql.match(/^\s*INSERT\s+INTO\s+`?(\w+)`?\s*\(([^)]+)\)\s*VALUES\s*\?\s*$/i);
  if (!m) return null;
  if (!Array.isArray(params) || !Array.isArray(params[0]) || !Array.isArray(params[0][0])) return null;
  const [, table, colsStr] = m;
  const cols = colsStr.split(',').map(c => c.trim().replace(/^`|`$/g, ''));
  const rows = params[0];
  const placeholders = cols.map(() => '?').join(',');
  const valuesClause = rows.map(() => `(${placeholders})`).join(',');
  const flatParams = [];
  for (const r of rows) flatParams.push(...r);
  return {
    sql: `INSERT INTO ${table} (${cols.join(',')}) VALUES ${valuesClause}`,
    params: flatParams,
    table,
    cols,
    rowCount: rows.length
  };
}

function expandUpsert(sql, params) {
  const m = sql.match(/^\s*INSERT\s+INTO\s+`?(\w+)`?\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)\s*ON\s+DUPLICATE\s+KEY\s+UPDATE\s+(.+)$/is);
  if (!m) return null;
  const [, table, colsStr, valsStr, updateClause] = m;
  const cols = colsStr.split(',').map(c => c.trim().replace(/^`|`$/g, ''));
  const valTokens = valsStr.split(',').map(v => v.trim());
  return { table, cols, valTokens, updateClause: updateClause.trim(), params };
}

function applyInsertDefaults(table, sql, params) {
  const defaults = SCHEMA[table] && SCHEMA[table].autoFill;
  if (!defaults || !Object.keys(defaults).length) return { sql, params };
  const m = sql.match(/^\s*INSERT\s+INTO\s+`?\w+`?\s*\(([^)]+)\)\s*VALUES\s*(\(.+\))\s*$/is);
  if (!m) return { sql, params };
  const cols = m[1].split(',').map(c => c.trim().replace(/^`|`$/g, ''));
  const valuesPart = m[2];
  const isSingleTuple = /^\([^)]*\)\s*$/.test(valuesPart);
  const newCols = [...cols];
  let extraValsSql = '';
  const extraParams = [];
  for (const [col, kind] of Object.entries(defaults)) {
    if (newCols.includes(col)) continue;
    newCols.push(col);
    const v = kind === 'NOW' ? isoDateTime() : null;
    extraValsSql += ',?';
    extraParams.push(v);
  }
  if (!extraValsSql) return { sql, params };
  let newValuesPart;
  if (isSingleTuple) {
    newValuesPart = valuesPart.replace(/\)\s*$/, extraValsSql + ')');
  } else {
    newValuesPart = valuesPart.replace(/\)(?=\s*(?:,|$))/g, extraValsSql + ')');
    const tuples = valuesPart.split(/\),\s*\(/).length;
    return {
      sql: sql.replace(valuesPart, newValuesPart).replace(/\(([^)]+)\)\s*VALUES/, `(${newCols.join(',')}) VALUES`),
      params: insertExtrasIntoMultiTupleParams(params, cols.length, extraParams, tuples)
    };
  }
  return {
    sql: sql.replace(valuesPart, newValuesPart).replace(/\(([^)]+)\)\s*VALUES/, `(${newCols.join(',')}) VALUES`),
    params: [...params, ...extraParams]
  };
}

function insertExtrasIntoMultiTupleParams(params, colsPerTuple, extraParams, tuples) {
  const out = [];
  for (let t = 0; t < tuples; t++) {
    const start = t * colsPerTuple;
    out.push(...params.slice(start, start + colsPerTuple));
    out.push(...extraParams);
  }
  return out;
}

// ══════════════════════════════════════════════════════════════════
// QUERY API — identical translation layer to sheets-db.js
// ══════════════════════════════════════════════════════════════════
const INT_STR_RE = /^(?:0|-?[1-9]\d*)$/;
function coerceParams(params) {
  if (!Array.isArray(params)) return params;
  return params.map(p => {
    if (typeof p === 'string' && p.length > 0 && p.length < 16 && INT_STR_RE.test(p)) {
      return parseInt(p, 10);
    }
    return p;
  });
}

async function query(sql, params = []) {
  if (!_initialized) await init();
  if (params == null) params = [];
  if (!Array.isArray(params)) params = [params];
  params = coerceParams(params);

  const sqlTrim = sql.trim();
  if (/^\s*(ALTER|CREATE\s+TABLE|DROP|CREATE\s+INDEX)/i.test(sqlTrim)) {
    return [[], []];
  }
  if (/^\s*SELECT\s+1\s*$/i.test(sqlTrim)) {
    return [[{ '1': 1 }], []];
  }

  const bulk = expandBulkInsert(sqlTrim, params);
  if (bulk) {
    const withDefaults = applyInsertDefaults(bulk.table, bulk.sql, bulk.params);
    return executeMutation(withDefaults.sql, withDefaults.params, bulk.table);
  }

  const upsert = expandUpsert(sqlTrim, params);
  if (upsert) {
    return executeUpsert(upsert);
  }

  const mutationTable = detectMutationTable(sqlTrim);
  if (mutationTable) {
    let processedSql = sqlTrim;
    let processedParams = params;
    if (/^INSERT/i.test(sqlTrim)) {
      const withDefaults = applyInsertDefaults(mutationTable, processedSql, processedParams);
      processedSql = withDefaults.sql;
      processedParams = withDefaults.params;
    }
    return executeMutation(processedSql, processedParams, mutationTable);
  }

  try {
    const safeSql = escapeAliases(sqlTrim);
    const rows = alasql(safeSql, params);
    return [rows, []];
  } catch (err) {
    err.sql = sqlTrim;
    throw err;
  }
}

function executeMutation(sqlIn, params, table) {
  let sql = sqlIn;
  let injectedId = null;
  if (/^\s*INSERT/i.test(sql)) {
    injectedId = injectAutoId(table, sql, params);
    if (injectedId) {
      sql = injectedId.sql;
      params = injectedId.params;
    }
  }
  let affected;
  try {
    affected = alasql(sql, params);
  } catch (err) {
    err.sql = sql;
    throw err;
  }
  if (/^\s*(INSERT|UPDATE)/i.test(sql) && alasql.tables[table]) {
    const data = alasql.tables[table].data;
    if (data && data.length) {
      const lastN = /^\s*INSERT/i.test(sql) ? (injectedId ? injectedId.insertedCount : 1) : data.length;
      const startIdx = Math.max(0, data.length - lastN);
      for (let i = startIdx; i < data.length; i++) {
        const row = data[i];
        for (const c of Object.keys(row)) {
          if (INT_COLS.has(c) && typeof row[c] === 'string' && row[c] !== '') {
            const n = parseInt(row[c], 10);
            if (!Number.isNaN(n)) row[c] = n;
          }
        }
      }
    }
  }
  if (table) markDirty(table);
  const result = {
    affectedRows: typeof affected === 'number' ? affected : 0,
    insertId: injectedId ? injectedId.insertId : null
  };
  return [result, []];
}

function injectAutoId(table, sql, params) {
  if (!SCHEMA[table]) return null;
  const m = sql.match(/^(\s*INSERT\s+INTO\s+`?\w+`?\s*\()([^)]+)(\)\s*VALUES\s*)(.+)$/is);
  if (!m) return null;
  const colsList = m[2].split(',').map(c => c.trim().replace(/^`|`$/g, ''));
  if (colsList.includes('id')) {
    return null;
  }
  const valuesPart = m[4].trim().replace(/;$/, '');
  const tupleStarts = [];
  let depth = 0;
  for (let i = 0; i < valuesPart.length; i++) {
    const ch = valuesPart[i];
    if (ch === '(') { if (depth === 0) tupleStarts.push(i); depth++; }
    else if (ch === ')') depth--;
  }
  const tuples = tupleStarts.length || 1;
  let actualMax = 0;
  const _rows = alasql.tables[table] && alasql.tables[table].data;
  if (_rows && _rows.length) {
    for (const r of _rows) { const v = parseInt(r.id, 10); if (v > actualMax) actualMax = v; }
  }
  const startId = Math.max(_nextId[table] || 1, actualMax + 1);
  const newColsList = ['id', ...colsList];

  let newValues = valuesPart;
  let idAdded = 0;
  newValues = newValues.replace(/\(/g, () => {
    const thisId = startId + idAdded;
    idAdded++;
    return `(${thisId},`;
  });

  _nextId[table] = startId + tuples;
  const newSql = `${m[1].replace(/\(\s*$/, '(')}${newColsList.join(',')}${m[3]}${newValues}`;
  return {
    sql: newSql,
    params,
    insertId: startId,
    insertedCount: tuples
  };
}

const UNIQUE_KEYS = {
  week_plans: ['employee_id', 'start_date']
};
function executeUpsert({ table, cols, valTokens, updateClause, params }) {
  const keys = UNIQUE_KEYS[table];
  if (!keys || !keys.length) {
    const insertSql = `INSERT INTO ${table} (${cols.join(',')}) VALUES (${valTokens.join(',')})`;
    return executeMutation(insertSql, params, table);
  }
  const colValMap = {};
  let pIdx = 0;
  for (let i = 0; i < cols.length; i++) {
    if (valTokens[i] === '?') {
      colValMap[cols[i]] = params[pIdx++];
    } else {
      colValMap[cols[i]] = unquoteSqlLiteral(valTokens[i]);
    }
  }
  const whereSql = keys.map(k => `${k} = ?`).join(' AND ');
  const whereVals = keys.map(k => colValMap[k]);
  const existing = alasql(`SELECT id FROM ${table} WHERE ${whereSql}`, whereVals);

  if (existing.length === 0) {
    const insertSql = `INSERT INTO ${table} (${cols.join(',')}) VALUES (${valTokens.join(',')})`;
    const [res] = executeMutation(insertSql, params, table);
    return [{ affectedRows: 1, insertId: res.insertId }, []];
  }
  const id = existing[0].id;
  const setParts = updateClause.split(',').map(s => s.trim());
  const setSql = [];
  const setParams = [];
  for (const part of setParts) {
    const mm = part.match(/^`?(\w+)`?\s*=\s*VALUES\s*\(\s*`?(\w+)`?\s*\)$/i);
    if (mm) {
      const target = mm[1];
      const source = mm[2];
      setSql.push(`${target} = ?`);
      setParams.push(colValMap[source]);
    } else {
      const mm2 = part.match(/^`?(\w+)`?\s*=\s*(.+)$/i);
      if (mm2) {
        setSql.push(`${mm2[1]} = ${mm2[2]}`);
      }
    }
  }
  if (SCHEMA[table].cols.includes('updated_at')) {
    setSql.push(`updated_at = ?`);
    setParams.push(isoDateTime());
  }
  alasql(`UPDATE ${table} SET ${setSql.join(', ')} WHERE id = ?`, [...setParams, id]);
  markDirty(table);
  return [{ affectedRows: 2, insertId: id }, []];
}

function unquoteSqlLiteral(token) {
  const t = token.trim();
  if ((t.startsWith("'") && t.endsWith("'")) || (t.startsWith('"') && t.endsWith('"'))) {
    return t.slice(1, -1);
  }
  if (/^-?\d+(\.\d+)?$/.test(t)) return Number(t);
  if (t.toUpperCase() === 'NULL') return null;
  return t;
}

// ══════════════════════════════════════════════════════════════════
// CONNECTION (transaction mock — alasql is in-memory; commit/rollback
// are best-effort no-ops, same as sheets-db.js)
// ══════════════════════════════════════════════════════════════════
function getConnection() {
  return {
    query: (sql, params) => query(sql, params),
    execute: (sql, params) => query(sql, params),
    beginTransaction: async () => {},
    commit: async () => {},
    rollback: async () => {},
    release: () => {}
  };
}

// ══════════════════════════════════════════════════════════════════
// FLUSH — debounced snapshot write to PostgreSQL
// ══════════════════════════════════════════════════════════════════
function markDirty(table) {
  _dirtyTables.add(table);
  scheduleFlush();
}

function scheduleFlush() {
  if (_flushTimer) return;
  _flushTimer = setTimeout(() => {
    _flushTimer = null;
    flushNow().catch(err => console.error('  ❌ PG flush error:', err.message));
  }, FLUSH_DEBOUNCE_MS);
}

let _testMode = false;
async function flushNow() {
  if (!_initialized) return;
  if (_testMode) { _dirtyTables.clear(); return; }
  if (_flushInProgress) {
    return new Promise(resolve => _pendingFlushResolvers.push(resolve));
  }
  _flushInProgress = true;
  try {
    while (_dirtyTables.size > 0) {
      const snapshot = Array.from(_dirtyTables);
      // Write first; only clear dirty on SUCCESS. If a write fails
      // (network/auth) tables stay dirty so the next flush retries and no
      // data is lost.
      await writeTablesToPg(snapshot);
      snapshot.forEach(t => _dirtyTables.delete(t));
    }
  } finally {
    _flushInProgress = false;
    const resolvers = _pendingFlushResolvers.splice(0);
    for (const r of resolvers) r();
  }
}

// Full snapshot per dirty table: DELETE all + bulk INSERT current rows,
// inside a transaction so a crash never leaves a half-written table.
// LOCAL mode: poora snapshot ek JSON file me. Atomic — pehle .tmp likhte
// hain phir rename, taaki crash par aadhi-likhi file na reh jaaye.
function writeTablesToLocalFile() {
  const store = {};
  for (const table of _managed) {
    const cols = SCHEMA[table].cols;
    const rows = alasql.tables[table] ? alasql.tables[table].data : [];
    store[table] = rows.map(r => {
      const o = {};
      for (const c of cols) o[c] = serializeForDb(r[c]);
      return o;
    });
  }
  const dir = path.dirname(LOCAL_FILE);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  const tmp = LOCAL_FILE + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(store), 'utf8');
  fs.renameSync(tmp, LOCAL_FILE);
}

// MySQL: har dirty table ka poora snapshot — DELETE + bulk INSERT, ek transaction me.
// mysql2 ka "VALUES ?" nested-array form use karte hain (placeholder limit se bachne
// ke liye), aur 400 rows ke chunk me bhejte hain taaki max_allowed_packet na phate.
async function writeTablesToMysql(tables) {
  const pool = getPool();
  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();
    for (const table of tables) {
      const cols = SCHEMA[table].cols;
      const rows = alasql.tables[table] ? alasql.tables[table].data : [];
      await conn.query(`DELETE FROM ${qIdent(table)}`);
      if (!rows.length) continue;
      const colSql = cols.map(qIdent).join(', ');
      const CHUNK = 400;
      for (let i = 0; i < rows.length; i += CHUNK) {
        const values = rows.slice(i, i + CHUNK).map(r => cols.map(c => serializeForDb(r[c])));
        await conn.query(`INSERT INTO ${qIdent(table)} (${colSql}) VALUES ?`, [values]);
      }
    }
    await conn.commit();
  } catch (err) {
    try { await conn.rollback(); } catch (_) {}
    throw err;
  } finally {
    conn.release();
  }
}

async function writeTablesToPg(tables) {
  if (!tables.length) return;
  if (LOCAL_MODE) return writeTablesToLocalFile();
  if (MYSQL_MODE) return writeTablesToMysql(tables);
  const pool = getPool();
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    for (const table of tables) {
      const cols = SCHEMA[table].cols;
      const rows = alasql.tables[table] ? alasql.tables[table].data : [];
      await client.query(`DELETE FROM ${qIdent(table)}`);
      if (rows.length) {
        const colSql = cols.map(qIdent).join(', ');
        // Chunk inserts to stay well under the 65535 bind-param limit.
        const maxParams = 60000;
        const rowsPerChunk = Math.max(1, Math.floor(maxParams / cols.length));
        for (let start = 0; start < rows.length; start += rowsPerChunk) {
          const chunk = rows.slice(start, start + rowsPerChunk);
          const valuesSql = [];
          const flat = [];
          let p = 1;
          for (const r of chunk) {
            const ph = cols.map(() => `$${p++}`);
            valuesSql.push(`(${ph.join(', ')})`);
            for (const c of cols) flat.push(serializeForDb(r[c]));
          }
          await client.query(
            `INSERT INTO ${qIdent(table)} (${colSql}) VALUES ${valuesSql.join(', ')}`,
            flat
          );
        }
      }
    }
    await client.query('COMMIT');
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    throw err;
  } finally {
    client.release();
  }
}

// ══════════════════════════════════════════════════════════════════
// WRITE REPORT TAB — parity shim for the MIS "export to sheet" feature.
// PG has no "tabs"; we persist the 2D report into a standalone table
// `report_<slug>` (dropped + recreated each call) so the data is still
// stored and queryable. This table is NOT part of TABLE_NAMES, so
// init()/reload()/flush() never touch it.
// ══════════════════════════════════════════════════════════════════
async function writeReportTab(title, rows) {
  if (!_initialized) await init();
  if (LOCAL_MODE) {
    // Local mode me alag report table nahi banti (report UI/CSV se mil jaati hai).
    return { rows: (rows || []).length, table: null, skipped: 'local-mode' };
  }
  const pool = getPool();
  const safe = (rows || []).map(row => (Array.isArray(row) ? row : [row]).map(cell => {
    if (cell === null || cell === undefined) return '';
    let s = String(cell);
    if (s.length > MAX_CELL_CHARS) s = s.slice(0, MAX_CELL_CHARS);
    return s;
  }));
  const slug = 'report_' + String(title).toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '');
  const width = safe.reduce((w, r) => Math.max(w, r.length), 1);
  const colNames = Array.from({ length: width }, (_, i) => `c${i + 1}`);

  if (MYSQL_MODE) {
    const conn = await pool.getConnection();
    try {
      await conn.beginTransaction();
      await conn.query(`DROP TABLE IF EXISTS ${qIdent(slug)}`);
      await conn.query(`CREATE TABLE ${qIdent(slug)} (row_no INT, ` +
        colNames.map(c => qIdent(c) + ' MEDIUMTEXT').join(', ') +
        `) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
      if (safe.length) {
        const values = safe.map((r, i) => [i, ...colNames.map((_, j) => (j < r.length ? r[j] : ''))]);
        for (let i = 0; i < values.length; i += 400) {
          await conn.query(`INSERT INTO ${qIdent(slug)} (row_no, ${colNames.map(qIdent).join(', ')}) VALUES ?`,
            [values.slice(i, i + 400)]);
        }
      }
      await conn.commit();
    } catch (err) {
      try { await conn.rollback(); } catch (_) {}
      throw err;
    } finally { conn.release(); }
    return { rows: safe.length, table: slug };
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`DROP TABLE IF EXISTS ${qIdent(slug)}`);
    await client.query(
      `CREATE TABLE ${qIdent(slug)} (row_no INTEGER, ${colNames.map(c => `${qIdent(c)} TEXT`).join(', ')})`
    );
    for (let i = 0; i < safe.length; i++) {
      const r = safe[i];
      const vals = colNames.map((_, j) => (j < r.length ? r[j] : ''));
      const ph = vals.map((_, j) => `$${j + 2}`).join(', ');
      await client.query(
        `INSERT INTO ${qIdent(slug)} (row_no, ${colNames.map(qIdent).join(', ')}) VALUES ($1, ${ph})`,
        [i, ...vals]
      );
    }
    await client.query('COMMIT');
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    throw err;
  } finally {
    client.release();
  }
  return { rows: safe.length, table: slug };
}

// ══════════════════════════════════════════════════════════════════
// EXPORT
// ══════════════════════════════════════════════════════════════════
async function _testInit() {
  for (const t of TABLE_NAMES) {
    const colsSql = SCHEMA[t].cols
      .map(c => `\`${c}\` ${c==='id' ? 'INT' : 'STRING'}`)
      .join(', ');
    alasql(`CREATE TABLE IF NOT EXISTS ${t} (${colsSql})`);
    _nextId[t] = 1;
  }
  _testMode = true;
  _initialized = true;
}

// Close the pool (used by migration scripts / graceful shutdown)
async function end() {
  if (_pool) { await _pool.end(); _pool = null; }
}

module.exports = {
  init,
  reload,
  query,
  execute: query,
  getConnection,
  flushNow,
  writeReportTab,
  end,
  setManagedTables,
  detectMutationTable,
  // Test / debug helpers
  _alasql: alasql,
  _schema: SCHEMA,
  _testInit,
  _getPool: getPool,
  _loadAllTables: loadAllTables
};
