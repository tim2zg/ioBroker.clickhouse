# ClickHouse Storage Layout for the ioBroker ClickHouse Adapter

This document captures the database objects the adapter creates automatically. Hand it off to service owners or LLMs that need to integrate with the same schema (for example, a Go application querying ClickHouse directly).

## High-Level Architecture

| Purpose | Object(s) | Notes |
| --- | --- | --- |
| Raw telemetry per ioBroker state | `iobroker.history_<sanitized-id>` | One table per enabled datapoint, keeps dense measurements for 90 days. |
| Registry / metadata | `iobroker.history_registry` | Maps ioBroker IDs to their ClickHouse table name and value type. |
| Continuous daily aggregates (storage) | `iobroker.history_daily_state` | `AggregatingMergeTree` with aggregate function states (min/max/avg/etc.). |
| Continuous daily aggregates (queryable view) | `iobroker.history_daily` | Finalizes the aggregate states into regular columns. |
| Continuous ingestion pipeline | `iobroker.mv_history_<table>` | Materialized views (one per numeric raw table) that feed the aggregate-state table. |

The adapter manages creation, updates, and TTL policies for all of the above. You should *read* from the raw tables and the `history_daily` view; all other objects exist to keep that data current.

## Raw Per-State Tables

- **Name pattern:** `history_<sanitized-id>` (sanitization strips non-alphanumeric characters, collapses separators, and prefixes leading digits).
- **Schema:**
  ```sql
  CREATE TABLE iobroker.history_<sanitized-id> (
      ts    DateTime64(3, 'UTC'),
      value Nullable(Float64 | String | UInt8 | String)  -- depending on detected type
  )
  ENGINE = MergeTree()
  ORDER BY ts
  TTL ts + INTERVAL 90 DAY DELETE;
  ```
- **Retention:** The adapter enforces `TTL ts + INTERVAL 90 DAY DELETE` on every table. You always have ~90 days of full-resolution data.
- **Usage:** Query these tables for recent, minute-level (or better) detail. Example (Go pseudo-SQL):
  ```sql
  SELECT ts, value
  FROM iobroker.`history_fronius_0_site_P_PV`
  WHERE ts BETWEEN fromUnixTimestamp64Milli(?) AND fromUnixTimestamp64Milli(?)
  ORDER BY ts;
  ```

## Registry Table (`history_registry`)

- **Columns:** `id String`, `table String`, `type String`, `updated DateTime64(3, 'UTC')`.
- **Purpose:** Look up which raw table belongs to an ioBroker state ID and what type is stored (`number`, `string`, `boolean`, or `json`).
- **Usage:** Query once to bootstrap your application:
  ```sql
  SELECT id, table, type
  FROM iobroker.history_registry
  ORDER BY id;
  ```

## Daily Aggregates

The adapter keeps rolling daily metrics for every numeric datapoint.

### Storage Table (`history_daily_state`)

- **Schema (simplified):**
  ```sql
  CREATE TABLE iobroker.history_daily_state (
      id             String,
      day            Date,
      min_state      AggregateFunction(min, Float64),
      max_state      AggregateFunction(max, Float64),
      avg_state      AggregateFunction(avg, Float64),
      last_state     AggregateFunction(argMax, Float64, DateTime64(3, 'UTC')),
      count_state    AggregateFunction(count),
      sum_state      AggregateFunction(sum, Float64),
      integral_state AggregateFunction(sum, Float64),
      updated        DateTime DEFAULT now()
  )
  ENGINE = AggregatingMergeTree()
  ORDER BY (id, day);
  ```
- **Internals:** Rows contain aggregate function *states* (binary blobs). You normally do not query this table directly unless you understand ClickHouse aggregate-finalization semantics.

### Query View (`history_daily`)

- **Definition (conceptual):**
  ```sql
  CREATE OR REPLACE VIEW iobroker.history_daily AS
  SELECT
      id,
      day,
      minMerge(min_state)        AS min,
      maxMerge(max_state)        AS max,
      avgMerge(avg_state)        AS avg,
      argMaxMerge(last_state)    AS last,
      countMerge(count_state)    AS samples,
      sumMerge(sum_state)        AS sum,
      sumMerge(integral_state)   AS integral_kwh,
      max(updated)               AS updated
  FROM iobroker.history_daily_state
  GROUP BY id, day;
  ```
- **Usage:** Query this view for long-term trends once raw data ages out.
  ```sql
  SELECT day, min, max, avg, last, samples, sum, integral_kwh
  FROM iobroker.history_daily
  WHERE id = 'fronius.0.site.P_PV'
    AND day BETWEEN '2025-08-01' AND '2025-11-01'
  ORDER BY day;
  ```

### Materialized Views (`mv_history_<table>`)

- **Purpose:** Transform new measurements from each numeric raw table into aggregate states stored in `history_daily_state`.
- **Note:** They are maintained by the adapter. You do *not* query them directly; they contain the SQL pipeline feeding the aggregate-state table.

## Query Guidance for External Apps

| Need | Query | Notes |
| --- | --- | --- |
| Short-term, high-resolution data | `SELECT ... FROM iobroker.
      `history_<table>` ...` | Use table names from `history_registry`. Expect data for ~90 days. |
| Daily rollups (min/avg/max/etc.) | `SELECT ... FROM iobroker.history_daily ...` | Works indefinitely; ideal for dashboards beyond the raw retention window. |
| Discover available datapoints | `SELECT id, table, type FROM iobroker.history_registry` | Cache locally and refresh periodically. |
| Backfill historical aggregates | INSERT using the template in `docs/downsampling.md` | Only needed if you import legacy data predating the new adapter version. |

## Operational Notes

- **Null handling:** Raw tables may store `NULL` when no numeric value is available. Materialized views ignore those rows (`WHERE value IS NOT NULL`) and cast with `assumeNotNull` so aggregate functions operate on plain `Float64`.
- **Renames:** If an ioBroker state ID changes, the adapter creates a new table for the new ID; the old table remains until you drop it manually.
- **Permissions:** The adapter expects full access (CREATE/INSERT/ALTER) to the configured ClickHouse database. External readers typically need only `SELECT` on the relevant objects.
- **Retention adjustments:** To change the 90-day raw retention, issue `ALTER TABLE ... MODIFY TTL` statements for each per-state table. The adapter does not currently overwrite custom TTLs.

With these structures in mind, a Go client can base its CRUD logic on `history_registry`, read detailed data from the per-state tables, and fall back to `history_daily` for long-range analytics without touching the internal materialized views or aggregate-state blobs directly.
