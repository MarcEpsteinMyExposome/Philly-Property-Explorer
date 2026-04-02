# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
npm run load    # Fetch all data from Philly Carto API and load into Postgres (takes several minutes)
npm start       # Start Express server on http://localhost:3000
```

There are no tests or linting configured.

## Architecture

Three-file app: data loader → Postgres → Express API → single-page frontend.

**load-data.js** — Pulls three datasets from Philadelphia's free Carto SQL API (`phl.carto.com/api/v2/sql`), paginated at 50k rows, and batch-inserts into local Postgres. Creates the database and tables if they don't exist. Enables `pg_trgm` for fuzzy address search.

**server.js** — Express server with two API endpoints:
- `GET /api/search` — Dynamic WHERE clause built from query params (address ILIKE, zip, value range, year range, delinquent flag). Returns `{total, rows[]}` with results capped at 500. Uses LEFT JOINs to flag delinquent/violation status per property; swaps to INNER JOIN when the delinquent filter is active.
- `GET /api/property/:parcel` — Three parallel queries (property + delinquencies + violations) joined on `parcel_number`.

**public/index.html** — All frontend code in one file (HTML/CSS/JS). Leaflet map with dark CARTO tiles, sidebar with filters and results list, property detail modal with Street View embed.

## Database

PostgreSQL on localhost:5432, user/pass `brightmeld`, database `philly_explorer`.

Three tables linked by OPA account number (stored as TEXT everywhere, normalized from mixed types in source data):
- `properties` (PK: `parcel_number`) — ~580k rows, has GIN trigram index on `location`
- `tax_delinquencies` (FK: `opa_number` → `parcel_number`)
- `violations` (FK: `opa_account_num` → `parcel_number`)

No foreign key constraints are enforced — not every tax/violation record has a matching property.

## Key Implementation Details

- Coordinates come from `ST_Y(the_geom)` / `ST_X(the_geom)` in the Carto queries, not explicit lat/lng columns.
- Batch insert sizes are tuned to stay under Postgres's 65,535 parameter limit (2k/4k/5k rows depending on column count).
- Status dot colors: red = tax delinquent, orange = has violations, green = clean.
- The delinquent checkbox is a first-class search filter (INNER JOIN), not just a display flag.
