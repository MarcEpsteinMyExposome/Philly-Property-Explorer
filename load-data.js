const { Client } = require('pg');
const https = require('https');
const http = require('http');

const DATABASE_URL = process.env.DATABASE_URL;
const PG_CONFIG = DATABASE_URL
  ? { connectionString: DATABASE_URL, ssl: { rejectUnauthorized: false } }
  : { host: 'localhost', port: 5432, user: 'brightmeld', password: 'brightmeld' };
const DB_NAME = 'philly_explorer';
const CARTO_BASE = 'https://phl.carto.com/api/v2/sql';
const PAGE_SIZE = 50000;

// ── helpers ──

function fetch(url) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;
    mod.get(url, res => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return fetch(res.headers.location).then(resolve, reject);
      }
      let body = '';
      res.on('data', c => (body += c));
      res.on('end', () => {
        if (res.statusCode >= 400) return reject(new Error(`HTTP ${res.statusCode}: ${body.slice(0, 200)}`));
        resolve(body);
      });
      res.on('error', reject);
    }).on('error', reject);
  });
}

async function cartoQuery(sql) {
  const url = `${CARTO_BASE}?q=${encodeURIComponent(sql)}`;
  const body = await fetch(url);
  const json = JSON.parse(body);
  if (json.error) throw new Error(json.error);
  return json.rows;
}

async function fetchAll(table, extraCols = '') {
  const rows = [];
  let offset = 0;
  while (true) {
    const sql = `SELECT *${extraCols} FROM ${table} ORDER BY cartodb_id LIMIT ${PAGE_SIZE} OFFSET ${offset}`;
    console.log(`  Fetching ${table} offset=${offset}...`);
    const batch = await cartoQuery(sql);
    console.log(`  Got ${batch.length} rows`);
    rows.push(...batch);
    if (batch.length < PAGE_SIZE) break;
    offset += PAGE_SIZE;
  }
  return rows;
}

// ── database setup ──

async function ensureDatabase() {
  const admin = new Client({ ...PG_CONFIG, database: 'postgres' });
  await admin.connect();
  const res = await admin.query(`SELECT 1 FROM pg_database WHERE datname = $1`, [DB_NAME]);
  if (res.rows.length === 0) {
    console.log(`Creating database "${DB_NAME}"...`);
    await admin.query(`CREATE DATABASE ${DB_NAME}`);
  }
  await admin.end();
}

async function createTables(client) {
  await client.query(`
    DROP TABLE IF EXISTS violations;
    DROP TABLE IF EXISTS tax_delinquencies;
    DROP TABLE IF EXISTS properties;

    CREATE TABLE properties (
      parcel_number TEXT PRIMARY KEY,
      location TEXT,
      owner_1 TEXT,
      owner_2 TEXT,
      market_value NUMERIC,
      sale_price NUMERIC,
      sale_date DATE,
      year_built TEXT,
      exterior_condition TEXT,
      interior_condition TEXT,
      zoning TEXT,
      zip_code TEXT,
      category_code_description TEXT,
      building_code_description TEXT,
      total_livable_area NUMERIC,
      number_of_bedrooms NUMERIC,
      number_of_bathrooms NUMERIC,
      number_stories NUMERIC,
      garage_spaces NUMERIC,
      total_area NUMERIC,
      lat DOUBLE PRECISION,
      lng DOUBLE PRECISION
    );

    CREATE TABLE tax_delinquencies (
      id SERIAL PRIMARY KEY,
      opa_number TEXT,
      street_address TEXT,
      owner TEXT,
      total_due NUMERIC,
      principal_due NUMERIC,
      penalty_due NUMERIC,
      interest_due NUMERIC,
      other_charges_due NUMERIC,
      num_years_owed INTEGER,
      most_recent_year_owed INTEGER,
      oldest_year_owed INTEGER,
      is_actionable TEXT,
      payment_agreement TEXT,
      total_assessment NUMERIC,
      building_category TEXT
    );

    CREATE TABLE violations (
      id SERIAL PRIMARY KEY,
      opa_account_num TEXT,
      address TEXT,
      casenumber TEXT,
      violationdate DATE,
      violationtype TEXT,
      violationdescription TEXT,
      status TEXT,
      casestatus TEXT,
      casepriority TEXT,
      prioritydesc TEXT,
      caseresolutiondate DATE
    );

    CREATE INDEX idx_properties_location ON properties USING gin (location gin_trgm_ops);
    CREATE INDEX idx_properties_zip ON properties (zip_code);
    CREATE INDEX idx_tax_opa ON tax_delinquencies (opa_number);
    CREATE INDEX idx_violations_opa ON violations (opa_account_num);
  `);
}

// ── loaders ──

async function loadProperties(client) {
  console.log('\n=== Loading properties ===');
  const allRows = await fetchAll('opa_properties_public', ', ST_Y(the_geom) AS lat, ST_X(the_geom) AS lng');
  // Filter out rows without a parcel_number (PK)
  const rows = allRows.filter(r => r.parcel_number);
  console.log(`Inserting ${rows.length} properties (${allRows.length - rows.length} skipped, no parcel_number)...`);

  // 22 columns × 2000 = 44000 params (under 65535 Postgres limit)
  const batchSize = 2000;
  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize);
    const values = [];
    const params = [];
    let p = 1;
    for (const r of batch) {
      values.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++})`);
      params.push(
        r.parcel_number, r.location, r.owner_1, r.owner_2,
        r.market_value, r.sale_price, r.sale_date, r.year_built,
        r.exterior_condition, r.interior_condition, r.zoning, r.zip_code,
        r.category_code_description, r.building_code_description,
        r.total_livable_area, r.number_of_bedrooms, r.number_of_bathrooms,
        r.number_stories, r.garage_spaces, r.total_area,
        r.lat, r.lng
      );
    }
    await client.query(
      `INSERT INTO properties VALUES ${values.join(',')} ON CONFLICT (parcel_number) DO NOTHING`,
      params
    );
    console.log(`  Inserted ${Math.min(i + batchSize, rows.length)} / ${rows.length}`);
  }
}

async function loadTaxDelinquencies(client) {
  console.log('\n=== Loading tax delinquencies ===');
  const rows = await fetchAll('real_estate_tax_delinquencies');
  console.log(`Inserting ${rows.length} delinquencies...`);

  // 15 columns × 4000 = 60000 params (under 65535 limit)
  const batchSize = 4000;
  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize);
    const values = [];
    const params = [];
    let p = 1;
    for (const r of batch) {
      values.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++})`);
      params.push(
        String(r.opa_number), r.street_address, r.owner,
        r.total_due, r.principal_due, r.penalty_due, r.interest_due, r.other_charges_due,
        r.num_years_owed, r.most_recent_year_owed, r.oldest_year_owed,
        r.is_actionable, r.payment_agreement, r.total_assessment, r.building_category
      );
    }
    await client.query(
      `INSERT INTO tax_delinquencies (opa_number, street_address, owner, total_due, principal_due, penalty_due, interest_due, other_charges_due, num_years_owed, most_recent_year_owed, oldest_year_owed, is_actionable, payment_agreement, total_assessment, building_category) VALUES ${values.join(',')}`,
      params
    );
    console.log(`  Inserted ${Math.min(i + batchSize, rows.length)} / ${rows.length}`);
  }
}

async function loadViolations(client) {
  console.log('\n=== Loading violations ===');
  const rows = await fetchAll('li_violations');
  console.log(`Inserting ${rows.length} violations...`);

  // 11 columns × 5000 = 55000 params (under 65535 limit)
  const batchSize = 5000;
  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize);
    const values = [];
    const params = [];
    let p = 1;
    for (const r of batch) {
      values.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++})`);
      params.push(
        r.opa_account_num, r.address, r.casenumber,
        r.violationdate, r.violationtype, r.violationdescription,
        r.status, r.casestatus, r.casepriority, r.prioritydesc,
        r.caseresolutiondate
      );
    }
    await client.query(
      `INSERT INTO violations (opa_account_num, address, casenumber, violationdate, violationtype, violationdescription, status, casestatus, casepriority, prioritydesc, caseresolutiondate) VALUES ${values.join(',')}`,
      params
    );
    console.log(`  Inserted ${Math.min(i + batchSize, rows.length)} / ${rows.length}`);
  }
}

// ── main ──

async function main() {
  console.log('Starting Philly Property Explorer data load...\n');
  const start = Date.now();

  if (!DATABASE_URL) await ensureDatabase();

  const client = new Client(DATABASE_URL ? PG_CONFIG : { ...PG_CONFIG, database: DB_NAME });
  await client.connect();

  // Enable trigram extension for fuzzy search
  await client.query('CREATE EXTENSION IF NOT EXISTS pg_trgm');

  await createTables(client);
  await loadProperties(client);
  await loadTaxDelinquencies(client);
  await loadViolations(client);

  const counts = await Promise.all([
    client.query('SELECT count(*) FROM properties'),
    client.query('SELECT count(*) FROM tax_delinquencies'),
    client.query('SELECT count(*) FROM violations'),
  ]);
  console.log(`\n=== Done in ${((Date.now() - start) / 1000).toFixed(1)}s ===`);
  console.log(`Properties:        ${counts[0].rows[0].count}`);
  console.log(`Tax delinquencies: ${counts[1].rows[0].count}`);
  console.log(`Violations:        ${counts[2].rows[0].count}`);

  await client.end();
}

main().catch(err => { console.error(err); process.exit(1); });
