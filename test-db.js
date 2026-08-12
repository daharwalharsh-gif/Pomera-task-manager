#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════
// test-db.js — database connection check karta hai
// Chalao:  npm run test-db
// ═══════════════════════════════════════════════════════════════════
// App start karne se pehle ye chala lo. Ye .env padhta hai aur bataata
// hai ki connection ban raha hai ya nahi — aur na bane to KYUN nahi.
require('dotenv').config();

const BACKEND = (process.env.DB_BACKEND || 'hybrid').toLowerCase();
const ok   = m => console.log('  \x1b[32m✓\x1b[0m ' + m);
const fail = m => console.log('  \x1b[31m✗\x1b[0m ' + m);
const info = m => console.log('    ' + m);

(async () => {
  console.log('\n  DB_BACKEND = ' + BACKEND + '\n');

  if (BACKEND === 'local') {
    ok('Local file mode — kisi database ki zaroorat nahi.');
    info('Data: data/local-db.json');
    info('Hostinger MySQL use karna ho to .env me DB_BACKEND=mysql kar do.');
    return;
  }

  if (BACKEND === 'mysql') {
    const host = process.env.MYSQL_HOST || process.env.PG_HOST || 'localhost';
    const user = String(process.env.MYSQL_USER || process.env.PG_USER || '').trim();
    const dbNm = String(process.env.MYSQL_DATABASE || process.env.PG_DATABASE || '').trim();
    const pass = process.env.MYSQL_PASSWORD || process.env.PG_PASSWORD;

    console.log('  Host     : ' + host + ':' + (process.env.MYSQL_PORT || 3306));
    console.log('  User     : ' + (user || '(khaali)'));
    console.log('  Database : ' + (dbNm || '(khaali)'));
    console.log('  Password : ' + (pass ? '(set hai, ' + String(pass).length + ' chars)' : '\x1b[31m(KHAALI)\x1b[0m'));
    console.log('');

    if (!user || !dbNm) { fail('MYSQL_USER / MYSQL_DATABASE .env me bharo.'); process.exit(1); }
    if (!pass) { fail('MYSQL_PASSWORD khaali hai — .env me password daalo.'); process.exit(1); }

    let mysql;
    try { mysql = require('mysql2/promise'); }
    catch (_) { fail('mysql2 install nahi hai. Chalao:  npm install mysql2'); process.exit(1); }

    let conn;
    try {
      conn = await mysql.createConnection({
        host, port: parseInt(process.env.MYSQL_PORT || '3306', 10),
        user, password: pass, database: dbNm,
        connectTimeout: 15000,
        ssl: /^(1|true|yes|require)$/i.test(process.env.MYSQL_SSL || '')
          ? { rejectUnauthorized: false } : undefined,
      });
    } catch (e) {
      fail('Connect nahi hua: ' + e.code + ' — ' + e.message);
      console.log('');
      if (e.code === 'ER_ACCESS_DENIED_ERROR')
        info('Username ya password galat hai.\n' +
             '    DHYAN DO: MySQL me chhote-bade akshar alag maane jaate hain — hPanel me\n' +
             '    jo naam dikhta hai BILKUL wahi copy karo (Pomera1 aur pomera1 alag hain).\n' +
             '    Password me @ # $ jaise characters ho to .env me quotes me likho:\n' +
             '    MYSQL_PASSWORD="#Pomera@2003"');
      else if (e.code === 'ETIMEDOUT' || e.code === 'ECONNREFUSED')
        info('Server tak pahuncha hi nahi. Agar LOCAL machine se chala rahe ho to hPanel →\n' +
             '    Databases → Remote MySQL me apna IP whitelist karo, aur MYSQL_HOST me\n' +
             '    "localhost" ki jagah Hostinger ka MySQL host daalo.');
      else if (e.code === 'ER_BAD_DB_ERROR')
        info('Database naam galat hai. hPanel me poora naam dekho (u563441929_... wala).');
      else if (e.code === 'ENOTFOUND')
        info('MYSQL_HOST galat hai — hostname resolve hi nahi hua.');
      process.exit(1);
    }

    ok('Connection ban gaya!');
    const [[v]] = await conn.query('SELECT VERSION() AS v');
    info('MySQL version: ' + v.v);

    const [tables] = await conn.query(
      'SELECT table_name AS t, table_rows AS r FROM information_schema.tables WHERE table_schema = DATABASE() ORDER BY t');
    if (!tables.length) {
      info('Database abhi khaali hai — app pehli baar chalte hi saari tables khud bana legi.');
    } else {
      info(tables.length + ' table(s) mili:');
      tables.forEach(t => info('   • ' + t.t + (t.r != null ? '  (~' + t.r + ' rows)' : '')));
    }

    // Likhne ki permission hai ya nahi
    try {
      await conn.query('CREATE TABLE IF NOT EXISTS _pomera_write_test (id INT PRIMARY KEY)');
      await conn.query('DROP TABLE _pomera_write_test');
      ok('Write permission bhi hai (table bana ke delete kar diya).');
    } catch (e) {
      fail('Write permission nahi hai: ' + e.message);
    }

    await conn.end();
    console.log('\n  \x1b[32mSab theek hai — npm start kar sakte ho.\x1b[0m\n');
    return;
  }

  if (BACKEND === 'pg' || BACKEND === 'hybrid') {
    const { Pool } = require('pg');
    const pool = new Pool({
      host: process.env.PG_HOST, port: parseInt(process.env.PG_PORT || '5432', 10),
      user: process.env.PG_USER, password: process.env.PG_PASSWORD,
      database: process.env.PG_DATABASE, connectionTimeoutMillis: 15000,
      ssl: /^(1|true|yes|require)$/i.test(process.env.PG_SSL || '') ? { rejectUnauthorized: false } : false,
    });
    try {
      const { rows } = await pool.query('SELECT version()');
      ok('PostgreSQL connect ho gaya.');
      info(rows[0].version.split(',')[0]);
    } catch (e) {
      fail('Connect nahi hua: ' + e.message);
      process.exit(1);
    } finally { await pool.end(); }
    return;
  }

  console.log('  DB_BACKEND=' + BACKEND + ' ke liye is script me check nahi hai.');
})().catch(e => { fail('Unexpected: ' + e.message); process.exit(1); });
