const express = require('express');
const { Pool } = require('pg');
const path = require('path');

const pool = process.env.DATABASE_URL
  ? new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } })
  : new Pool({ host: 'localhost', port: 5432, user: 'brightmeld', password: 'brightmeld', database: 'philly_explorer' });

const app = express();
app.use(express.static(path.join(__dirname, 'public')));

// Search properties with filters
app.get('/api/search', async (req, res) => {
  try {
    const { address, zip, minValue, maxValue, minYear, maxYear, delinquent } = req.query;
    const conditions = [];
    const params = [];
    let p = 1;

    if (address) {
      conditions.push(`p.location ILIKE $${p++}`);
      params.push(`%${address}%`);
    }
    if (zip) {
      if (!/^\d{5}$/.test(zip)) {
        return res.status(400).json({ error: 'Zip code must be 5 digits' });
      }
      conditions.push(`p.zip_code = $${p++}`);
      params.push(zip);
    }
    if (minValue) {
      const n = Number(minValue);
      if (!Number.isFinite(n)) return res.status(400).json({ error: 'Invalid minValue' });
      conditions.push(`p.market_value >= $${p++}`);
      params.push(n);
    }
    if (maxValue) {
      const n = Number(maxValue);
      if (!Number.isFinite(n)) return res.status(400).json({ error: 'Invalid maxValue' });
      conditions.push(`p.market_value <= $${p++}`);
      params.push(n);
    }
    if (minYear) {
      const n = Number(minYear);
      if (!Number.isFinite(n)) return res.status(400).json({ error: 'Invalid minYear' });
      conditions.push(`p.year_built >= $${p++}`);
      params.push(String(n));
    }
    if (maxYear) {
      const n = Number(maxYear);
      if (!Number.isFinite(n)) return res.status(400).json({ error: 'Invalid maxYear' });
      conditions.push(`p.year_built <= $${p++}`);
      params.push(String(n));
    }
    if (delinquent === 'true') {
      conditions.push(`td.opa_number IS NOT NULL`);
    }

    const where = conditions.length ? 'WHERE ' + conditions.join(' AND ') : '';

    const hasDelinquentFilter = delinquent === 'true';
    const taxJoin = hasDelinquentFilter
      ? 'INNER JOIN tax_delinquencies td ON td.opa_number = p.parcel_number'
      : 'LEFT JOIN tax_delinquencies td ON td.opa_number = p.parcel_number';

    // Count query (only join tax table when filtering by delinquent)
    const countSql = `
      SELECT COUNT(DISTINCT p.parcel_number) AS total
      FROM properties p
      ${hasDelinquentFilter ? taxJoin : ''}
      ${where}
    `;

    // Results query
    const dataSql = `
      SELECT DISTINCT ON (p.parcel_number)
        p.parcel_number, p.location, p.market_value, p.sale_price, p.year_built,
        p.exterior_condition, p.zoning, p.zip_code, p.lat, p.lng,
        CASE WHEN td.opa_number IS NOT NULL THEN true ELSE false END AS tax_delinquent,
        CASE WHEN v.opa_account_num IS NOT NULL THEN true ELSE false END AS has_violations
      FROM properties p
      ${taxJoin}
      LEFT JOIN violations v ON v.opa_account_num = p.parcel_number
      ${where}
      ORDER BY p.parcel_number
      LIMIT 500
    `;

    const [countResult, dataResult] = await Promise.all([
      pool.query(countSql, params),
      pool.query(dataSql, params),
    ]);

    res.json({
      total: parseInt(countResult.rows[0].total),
      rows: dataResult.rows,
    });
  } catch (err) {
    console.error('Search error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Property detail
app.get('/api/property/:parcel', async (req, res) => {
  try {
    const { parcel } = req.params;
    const [propResult, taxResult, violResult] = await Promise.all([
      pool.query('SELECT * FROM properties WHERE parcel_number = $1', [parcel]),
      pool.query('SELECT * FROM tax_delinquencies WHERE opa_number = $1 ORDER BY most_recent_year_owed DESC', [parcel]),
      pool.query('SELECT * FROM violations WHERE opa_account_num = $1 ORDER BY violationdate DESC', [parcel]),
    ]);

    if (propResult.rows.length === 0) {
      return res.status(404).json({ error: 'Property not found' });
    }

    res.json({
      property: propResult.rows[0],
      delinquencies: taxResult.rows,
      violations: violResult.rows,
    });
  } catch (err) {
    console.error('Detail error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Philly Property Explorer running at http://localhost:${PORT}`));
