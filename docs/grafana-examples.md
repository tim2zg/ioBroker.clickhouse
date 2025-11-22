# Grafana / ClickHouse SQL Examples

Here are some example queries to visualize your data in Grafana using the ClickHouse datasource.

## 1. Daily Energy Usage (Bar Chart)

This query displays the calculated daily integral (Energy in kWh) for a specific state.

**Visualization:** Bar Chart
**Query:**
```sql
SELECT
    toDateTime(day) as time,
    integral_kwh as value
FROM iobroker.history_daily
WHERE
    id = '0_userdata.0.solar.power' -- REPLACE WITH YOUR STATE ID
    AND day >= toDate($__from/1000)
    AND day <= toDate($__to/1000)
ORDER BY time
```

## 2. Daily Min/Max/Avg (Time Series)

This query shows the daily minimum, maximum, and average values.

**Visualization:** Time Series
**Query:**
```sql
SELECT
    toDateTime(day) as time,
    max,
    min,
    avg
FROM iobroker.history_daily
WHERE
    id = '0_userdata.0.temperature' -- REPLACE WITH YOUR STATE ID
    AND day >= toDate($__from/1000)
    AND day <= toDate($__to/1000)
ORDER BY time
```

## 3. Raw Data (High Resolution)

This query shows the raw, high-resolution data from the underlying table. Note that you need to know the table name (check `iobroker.history_registry`) or use a subquery/join if your Grafana plugin supports it (ClickHouse plugin usually does).

**Visualization:** Time Series
**Query:**
```sql
-- Option A: If you know the table name (more efficient)
SELECT
    ts as time,
    value
FROM iobroker.history_s_0_userdata_0_solar_power -- REPLACE WITH YOUR TABLE NAME
WHERE
    ts >= fromUnixTimestamp64Milli($__from)
    AND ts <= fromUnixTimestamp64Milli($__to)
ORDER BY time

-- Option B: Lookup by ID (convenient but slightly slower)
SELECT
    ts as time,
    value
FROM iobroker.history_s_0_userdata_0_solar_power -- You still need the table name here usually, 
                                                 -- but some setups allow dynamic table selection.
                                                 -- The adapter creates separate tables, so you can't 
                                                 -- easily query "ALL" raw data in one view without a UNION.
```

## 4. Combined: Recent Raw Data + Historical Daily Aggregates

This is an advanced query to show high-res data for the last 24h and daily aggregates for older data.

**Visualization:** Time Series
**Query:**
```sql
SELECT
    ts as time,
    value
FROM iobroker.history_s_0_userdata_0_solar_power
WHERE
    ts >= fromUnixTimestamp64Milli($__from)
    AND ts >= now() - INTERVAL 1 DAY

UNION ALL

SELECT
    toDateTime(day) as time,
    avg as value -- or integral_kwh, depending on what you want to see
FROM iobroker.history_daily
WHERE
    id = '0_userdata.0.solar.power'
    AND day >= toDate($__from/1000)
    AND day < toDate(now() - INTERVAL 1 DAY)
ORDER BY time
```

## 5. Battery Daily Min/Max Percentage

This query visualizes the daily minimum and maximum charge level (SoC) of a battery.

**Visualization:** Time Series (or Stat)
**Query:**
```sql
SELECT
    toDateTime(day) as time,
    max as max_soc,
    min as min_soc
FROM iobroker.history_daily
WHERE
    id = '0_userdata.0.battery.soc' -- REPLACE WITH YOUR BATTERY SOC STATE ID
    AND day >= toDate($__from/1000)
    AND day <= toDate($__to/1000)
ORDER BY time
```
