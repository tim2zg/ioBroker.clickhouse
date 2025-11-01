# ClickHouse Retention & Downsampling for ioBroker States

The adapter keeps one ClickHouse table **per ioBroker state** (`ts`, `value`) and now maintains continuous **daily aggregates** in the background. Raw tables retain 90 days of measurements; the aggregate pipeline preserves long-term metrics (min, max, avg, last, sample count, sum, and an energy-style integral per day).

All examples below assume the default database `iobroker` and table prefix `history`. Adjust if you changed the adapter settings.

## 1. What the adapter creates automatically

Running the adapter once will provision:

- `history_registry` – mapping between state IDs, per-state tables, and stored value types.
- `history_<…>` tables – one per state, each with `TTL ts + INTERVAL 90 DAY DELETE` so raw data expires after 3 months.
- `history_daily_state` – `AggregatingMergeTree` table that stores aggregate function states.
- `history_daily` – a view that finalizes the aggregate states for easy querying.
- `mv_history_<…>` materialized views (numbers only) – one per numeric state table; they stream new samples into `history_daily_state` as data arrives.

### Verify the layout

```sql
SELECT id, table, type
FROM iobroker.history_registry
ORDER BY id
LIMIT 20;
```

```sql
SHOW TABLES FROM iobroker LIKE 'history%';
```

Pick a numeric state to inspect the materialized view and aggregate table:

```sql
DESCRIBE TABLE iobroker.`history_fronius_0_site_P_PV`;
DESCRIBE TABLE iobroker.history_daily_state;
SELECT name FROM system.tables WHERE database = 'iobroker' AND engine = 'MaterializedView' LIMIT 10;
```

## 2. Adjusting the retention window (optional)

The adapter enforces the 90-day TTL automatically every time it starts. To use a different window, rerun:

```sql
ALTER TABLE iobroker.`history_my_state`
MODIFY TTL ts + INTERVAL 180 DAY DELETE;
```

Repeat (or generate a batch using `system.tables`) for all `history_*` tables when you change the interval.

## 3. Initial backfill for aggregates

Materialized views keep `history_daily` up to date for new measurements. To populate historical days that existed **before** the upgrade, run the helper script once:

```
npm run downsample -- --host=127.0.0.1 --database=iobroker --prefix=history
```

Add `--start=<unix_ms>` / `--end=<unix_ms>` to limit the backfill to a specific time range. Leave the script in place for occasional re-runs if you import legacy data later.

## 4. Query daily aggregates

Ask ClickHouse for long-term trends straight from the view:

```sql
SELECT *
FROM iobroker.history_daily
WHERE id = 'fronius.0.site.P_PV'
ORDER BY day DESC
LIMIT 14;
```

Need raw data? Query the per-state table (`history_<…>`) directly; you will always have the last 90 days in full resolution.

## 5. Optional housekeeping

- Set a TTL on `history_daily_state` / `history_daily` if you ever want to prune daily aggregates (e.g., keep 5 years).
- Monitor `system.mutations` to ensure materialized views stay healthy; failures will show up in the adapter log as well.
- If you rename states, the adapter registers a new table; you can drop the old table manually once you are sure the data is no longer needed.

With the continuous pipeline in place you keep detailed telemetry for the recent past and summarized aggregates for the long term—no manual jobs required.
