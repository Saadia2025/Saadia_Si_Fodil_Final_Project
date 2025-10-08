/***********************************************************
* "Don't forget to add csv-parse library with NPM"
* npm install csv-parse
************************************************************
// build-gdp-sqlite.cjs
const { DatabaseSync } = require('node:sqlite'); // your requested API
const { parse } = require('csv-parse/sync');

const CSV_URL = 'https://datahub.io/core/gdp/r/gdp.csv';
const DB_NAME = 'Banks.db';
const TABLE_NAME = 'Largest_banks';

(async () => {
  // Use global fetch if available (Node 18+); otherwise lazy-load node-fetch
  const fetchImpl = globalThis.fetch || (await import('node-fetch')).default;

  // 1) Fetch CSV
  const res = await fetchImpl(CSV_URL);
  if (!res.ok) {
    throw new Error(`Failed to download CSV (${res.status} ${res.statusText})`);
  }
  const csvText = await res.text();

  // 2) Parse CSV
  const rows = parse(csvText, { columns: true, skip_empty_lines: true });

  // 3) Keep latest year per country
  const byCountry = new Map();
  for (const row of rows) {
    const country = (row['Country Name'] || '').trim();
    const year = Number(row['Year']);
    const rawVal = row['Value'];
    const value = rawVal === '' || rawVal == null ? NaN : Number(rawVal);
    if (!country || Number.isNaN(year) || Number.isNaN(value)) continue;

    const prev = byCountry.get(country);
    if (!prev || year > prev.year) {
      byCountry.set(country, { year, value });
    }
  }

  // 4) Convert GDP to billions USD (rounded to 2 decimals)
  const data = Array.from(byCountry, ([Country, { value }]) => ({
    Country,
    GDP_USD_billions: Math.round((value / 1_000_000_000) * 100) / 100
  })).sort((a, b) => a.Country.localeCompare(b.Country));

  // 5) Save to SQLite (replace table)
  const db = new DatabaseSync(DB_NAME);

  try {
    db.exec(`DROP TABLE IF EXISTS ${TABLE_NAME};`);
    db.exec(`
      CREATE TABLE ${TABLE_NAME} (
        Country TEXT NOT NULL,
        GDP_USD_billions REAL NOT NULL
      );
    `);

    db.exec('BEGIN');
    // Assuming DatabaseSync has a prepare().run() API
    const stmt = db.prepare(
      `INSERT INTO ${TABLE_NAME} (Country, GDP_USD_billions) VALUES (?, ?)`
    );

    for (const r of data) {
      stmt.run(r.Country, r.GDP_USD_billions);
    }

    db.exec('COMMIT');

    console.log('✅ Database created successfully:', DB_NAME);
    console.table(data.slice(0, 5));
  } catch (err) {
    try { db.exec('ROLLBACK'); } catch {}
    throw err;
  } finally {
    // Some implementations expose close(); use optional chaining to be safe
    db.close?.();
  }
})().catch((err) => {
  console.error('❌ Error:', err);
  process.exit(1);
});

