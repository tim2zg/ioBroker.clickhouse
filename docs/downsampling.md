# ClickHouse Retention & Downsampling for ioBroker States

This guide configures ClickHouse so you keep full-resolution telemetry for the most recent **3 months** and automatically maintain lightweight **daily aggregates** for everything older. The SQL snippets are safe to run multiple times; ClickHouse simply keeps existing objects.

> **Scope**
> - Applies to **every state** written by the ClickHouse ioBroker adapter (table `iobroker.history`).
> - Daily aggregates are stored in `iobroker.history_daily` once you create the objects below.
> - Modify database/table names if you changed them in the adapter config.

## 1. Retain Raw History for 3 Months

Add a TTL to the raw history table so rows older than 90 days are deleted automatically.

```sql
ALTER TABLE iobroker.history
MODIFY TTL ts + INTERVAL 90 DAY DELETE;
```

- Runs instantly; existing data older than 90 days is removed by ClickHouse’s background task.
- Adjust `INTERVAL 90 DAY` if you need a longer window.

## 2. Create a Daily Aggregate Table

This table holds one row per datapoint and calendar day. It stores statistics (min/avg/max/last, count, sum) plus an energy-style integral useful for power sensors.

```sql
CREATE TABLE IF NOT EXISTS iobroker.history_daily
(
    id String,
    day Date,
    min Float64,
    max Float64,
    avg Float64,
    last Float64,
    samples UInt64,
    sum Float64,
    integral_kwh Float64
)
ENGINE = SummingMergeTree
ORDER BY (id, day);
```

- `SummingMergeTree` allows the materialized view to append partial aggregates and ClickHouse merges them reliably.
- You can add more columns if needed (e.g., `first`, percentiles, etc.).

## 3. Materialized View to Populate Daily Aggregates

This view watches the raw history table, groups by day, and writes the aggregates into `history_daily`.

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS iobroker.mv_history_daily
TO iobroker.history_daily
AS
WITH
    toDate(ts) AS day_bucket,
    lead(ts) OVER (PARTITION BY id ORDER BY ts) AS next_ts,
    greatest(0, dateDiff('second', ts, coalesce(next_ts, addDays(day_bucket, 1)))) AS seconds_active,
    toFloat64OrZero(val_float) AS value_w
SELECT
    id,
    day_bucket AS day,
    minState(value_w) AS min,
    maxState(value_w) AS max,
    avgState(value_w) AS avg,
    anyLastState(value_w, ts) AS last,
    countState() AS samples,
    sumState(value_w) AS sum,
    sumState(value_w * seconds_active / 3.6e6) AS integral_kwh
FROM iobroker.history
GROUP BY id, day_bucket;
```

**What it does**
- Calculates time-weighted energy (`integral_kwh`) by multiplying watts by the seconds until the next sample, then converting Wh → kWh (`3.6e6 = 1000 * 3600`).
- Keeps aggregate metrics for every datapoint (`id`) and day.
- Re-creates fine-grained data for historical analytics once raw measurements expire.

## 4. Backfill Historical Days (Optional)

If you already have older raw data and want daily aggregates immediately, run:

```sql
INSERT INTO iobroker.history_daily
SELECT
    id,
    toDate(ts) AS day,
    min(value_w),
    max(value_w),
    avg(value_w),
    anyLast(value_w),
    count(),
    sum(value_w),
    sum(value_w * seconds_active / 3.6e6)
FROM
(
    SELECT
        id,
        ts,
        toFloat64OrZero(val_float) AS value_w,
        greatest(0, dateDiff('second', ts, coalesce(next_ts, toDateTime(addDays(toDate(ts), 1))))) AS seconds_active,
        lead(ts) OVER (PARTITION BY id ORDER BY ts) AS next_ts
    FROM iobroker.history
)
GROUP BY id, toDate(ts);
```

- Execute once; afterwards the materialized view keeps `history_daily` up to date.
- For very large datasets consider batching or using `INSERT SELECT ... WHERE ts >= ... AND ts < ...` per month.

## 5. (Optional) Retention for Aggregates

Daily aggregates are typically small. If you want to prune them after several years, set another TTL:

```sql
ALTER TABLE iobroker.history_daily
MODIFY TTL day + INTERVAL 5 YEAR DELETE;
```

## 6. Verify the Setup

- Check raw retention: `SELECT min(ts), max(ts) FROM iobroker.history;`
- Inspect aggregates: `SELECT * FROM iobroker.history_daily WHERE id = 'state.id' ORDER BY day DESC LIMIT 7;`
- Confirm the materialized view is attached: `SHOW TABLES FROM iobroker;` should list `history_daily` and `mv_history_daily`.

## Usage Tips

- Grafana or your custom apps can point long-range graphs directly at `history_daily` for fast dashboards.
- Use raw `iobroker.history` for short-term detail (last 3 months) and troubleshooting.
- If you add new columns to the raw table (e.g., tags), extend the materialized view accordingly.
- To adjust the retention window later, rerun the TTL `ALTER TABLE` with a different interval.

You’re now keeping high-resolution data for the recent past and compact daily summaries for the long term—without manual cleanup jobs.
