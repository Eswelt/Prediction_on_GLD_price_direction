/* ---------------------------------------------------------------------------
Query 1: Basic structure + row count + date ranges
What: Validate table availability, size, and date ranges.
How: UNION two metadata checks.
Why: Confirms that both market and news sources loaded correctly before analysis.
Expected output: two rows (market_daily, news_raw) with non-null min/max dates.
--------------------------------------------------------------------------- */
SELECT 'market_daily' AS table_name,
       COUNT(*) AS row_count,
       MIN(date) AS min_date,
       MAX(date) AS max_date
FROM market_daily
UNION ALL
SELECT 'news_raw' AS table_name,
       COUNT(*) AS row_count,
       MIN(display_date) AS min_date,
       MAX(display_date) AS max_date
FROM news_raw;

/* ---------------------------------------------------------------------------
Query 2: Build weekly market view and verify aggregation
What: Convert daily market prices to week-ending Friday values.
How: Map each daily row to week_end_date with date(...,'weekday 5'), then keep last daily close in each week.
Why: Weekly frequency is the project modeling unit.
Expected output: one row per week with weekly close for GLD/UUP/VIX.
--------------------------------------------------------------------------- */
DROP VIEW IF EXISTS market_weekly_close;
CREATE VIEW market_weekly_close AS
WITH tagged AS (
  SELECT date,
         date(date, 'weekday 5') AS week_end_date,
         gld_close, uup_close, vix_close,
         ROW_NUMBER() OVER (PARTITION BY date(date, 'weekday 5') ORDER BY date DESC) AS rn
  FROM market_daily
)
SELECT week_end_date, gld_close, uup_close, vix_close
FROM tagged
WHERE rn = 1;

SELECT COUNT(*) AS n_weeks,
       MIN(week_end_date) AS min_week,
       MAX(week_end_date) AS max_week
FROM market_weekly_close;

/* ---------------------------------------------------------------------------
Query 3: Window functions + CTE
What: Construct weekly returns and target_next_gld_ret.
How: LAG for current-week returns, LEAD for next-week target
Why: Defines supervised learning label directly in SQL.
Expected output: weekly rows with gld_ret, uup_ret, vix_ret, target_next_gld_ret.
--------------------------------------------------------------------------- */
DROP VIEW IF EXISTS market_weekly_returns;
CREATE VIEW market_weekly_returns AS
WITH base AS (
  SELECT week_end_date,
         gld_close,
         uup_close,
         vix_close,
         LAG(gld_close) OVER (ORDER BY week_end_date) AS gld_prev,
         LAG(uup_close) OVER (ORDER BY week_end_date) AS uup_prev,
         LAG(vix_close) OVER (ORDER BY week_end_date) AS vix_prev
  FROM market_weekly_close
), rets AS (
  SELECT week_end_date,
         (gld_close / gld_prev) - 1.0 AS gld_ret,
         (uup_close / uup_prev) - 1.0 AS uup_ret,
         (vix_close / vix_prev) - 1.0 AS vix_ret
  FROM base
  WHERE gld_prev IS NOT NULL AND uup_prev IS NOT NULL AND vix_prev IS NOT NULL
)
SELECT week_end_date,
       gld_ret,
       uup_ret,
       vix_ret,
       LEAD(gld_ret) OVER (ORDER BY week_end_date) AS target_next_gld_ret
FROM rets;

SELECT COUNT(*) AS n_rows,
       SUM(CASE WHEN target_next_gld_ret IS NULL THEN 1 ELSE 0 END) AS null_targets
FROM market_weekly_returns;

/* ---------------------------------------------------------------------------
Query 4: GROUP BY
What: Aggregate weekly GLD returns by year
How: GROUP BY strftime('%Y', week_end_date)
Why: Provides macro-level annual behavior baseline
Expected output: one row per year with avg/vol/min/max
--------------------------------------------------------------------------- */
SELECT strftime('%Y', week_end_date) AS year,
       COUNT(*) AS n_weeks,
       AVG(gld_ret) AS avg_gld_ret,
       sqrt(AVG(gld_ret * gld_ret) - AVG(gld_ret) * AVG(gld_ret)) AS std_gld_ret,
       MIN(gld_ret) AS min_gld_ret,
       MAX(gld_ret) AS max_gld_ret
FROM market_weekly_returns
GROUP BY strftime('%Y', week_end_date)
ORDER BY year;

/* ---------------------------------------------------------------------------
Query 5: Build weekly news view
What: Aggregate headlines into weekly text package and count
How: Map display_date to week_end_date and group-concat headlines
Why: Creates text features at same weekly frequency as market data
Expected output: one row per week with headline_count and weekly_text
--------------------------------------------------------------------------- */
DROP VIEW IF EXISTS news_weekly;
CREATE VIEW news_weekly AS
SELECT date(display_date, 'weekday 5') AS week_end_date,
       COUNT(*) AS headline_count,
       GROUP_CONCAT(headline, ' || ') AS weekly_text
FROM news_raw
GROUP BY date(display_date, 'weekday 5');

SELECT COUNT(*) AS n_news_weeks,
       MIN(week_end_date) AS min_week,
       MAX(week_end_date) AS max_week
FROM news_weekly;

/* ---------------------------------------------------------------------------
Query 6: Three-table JOIN and news join
What: Build modeling base from GLD/UUP/VIX weekly sub-tables plus news aggregates.
How: Split market_weekly_returns into g/u/v CTEs, then JOIN g-u-v on week_end_date and LEFT JOIN news_weekly.
Why: This is the core aligned dataset for model A/B/C comparison
Expected output: weekly modeling table including target and both feature families.
--------------------------------------------------------------------------- */
WITH g AS (
  SELECT week_end_date, gld_ret, target_next_gld_ret
  FROM market_weekly_returns
), u AS (
  SELECT week_end_date, uup_ret
  FROM market_weekly_returns
), v AS (
  SELECT week_end_date, vix_ret
  FROM market_weekly_returns
)
SELECT g.week_end_date,
       g.gld_ret,
       u.uup_ret,
       v.vix_ret,
       (u.uup_ret * v.vix_ret) AS uup_vix_interaction,
       g.target_next_gld_ret,
       COALESCE(n.headline_count, 0) AS headline_count,
       COALESCE(n.weekly_text, '') AS weekly_text
FROM g
JOIN u
  ON g.week_end_date = u.week_end_date
JOIN v
  ON g.week_end_date = v.week_end_date
LEFT JOIN news_weekly n
  ON g.week_end_date = n.week_end_date
WHERE g.target_next_gld_ret IS NOT NULL
ORDER BY g.week_end_date;

/* ---------------------------------------------------------------------------
Query 7: JOIN query for regime comparison
What: Compare next-week target by VIX regime after joining market+news.
How: CASE WHEN to bucket vix_ret into low/med/high, then group.
Why: Tests whether market stress state changes expected GLD outcome.
Expected output: 3 regime rows with avg target return and avg headline count.
--------------------------------------------------------------------------- */
WITH joined AS (
  SELECT m.week_end_date,
         m.vix_ret,
         m.target_next_gld_ret,
         COALESCE(n.headline_count, 0) AS headline_count
  FROM market_weekly_returns m
  LEFT JOIN news_weekly n
    ON m.week_end_date = n.week_end_date
  WHERE m.target_next_gld_ret IS NOT NULL
)
SELECT CASE
         WHEN vix_ret < -0.03 THEN 'low_vix_change'
         WHEN vix_ret >  0.03 THEN 'high_vix_change'
         ELSE 'mid_vix_change'
       END AS vix_regime,
       COUNT(*) AS n_weeks,
       AVG(target_next_gld_ret) AS avg_next_gld_ret,
       AVG(headline_count) AS avg_headline_count
FROM joined
GROUP BY vix_regime
ORDER BY vix_regime;

/* ---------------------------------------------------------------------------
Query 8: JOIN query for news-coverage segmentation
What: Compare target outcomes for no-news / low-news / high-news weeks
How: JOIN + CASE WHEN on headline_count
Why: Evaluates whether text coverage intensity links to predictability
Expected output: 3 bins with avg target and positive-rate
--------------------------------------------------------------------------- */
WITH joined AS (
  SELECT m.week_end_date,
         m.target_next_gld_ret,
         COALESCE(n.headline_count, 0) AS headline_count
  FROM market_weekly_returns m
  LEFT JOIN news_weekly n
    ON m.week_end_date = n.week_end_date
  WHERE m.target_next_gld_ret IS NOT NULL
)
SELECT CASE
         WHEN headline_count = 0 THEN 'no_news'
         WHEN headline_count <= 5 THEN 'low_news'
         ELSE 'high_news'
       END AS news_bucket,
       COUNT(*) AS n_weeks,
       AVG(target_next_gld_ret) AS avg_next_gld_ret,
       AVG(CASE WHEN target_next_gld_ret > 0 THEN 1.0 ELSE 0.0 END) AS positive_rate
FROM joined
GROUP BY news_bucket
ORDER BY news_bucket;

/* ---------------------------------------------------------------------------
Query 9: Window function - rolling 12-week mean and vol of GLD return
What: Compute rolling return moments
How: AVG(...) OVER rows between 11 preceding and current row
Why: Shows time-varying behavior and supports regime narrative
Expected output: weekly series with roll12_mean and roll12_vol
--------------------------------------------------------------------------- */
SELECT week_end_date,
       gld_ret,
       AVG(gld_ret) OVER (
         ORDER BY week_end_date
         ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
       ) AS roll12_mean,
       sqrt(
         AVG(gld_ret * gld_ret) OVER (
           ORDER BY week_end_date
           ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
         )
         -
         (AVG(gld_ret) OVER (
           ORDER BY week_end_date
           ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
         )) *
         (AVG(gld_ret) OVER (
           ORDER BY week_end_date
           ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
         ))
       ) AS roll12_vol
FROM market_weekly_returns
ORDER BY week_end_date;

/* ---------------------------------------------------------------------------
Query 10: Window function for NTILE rank of weekly news volume
What: Rank weeks into quartiles by headline_count
How: NTILE(4) OVER (ORDER BY headline_count)
Why: Enables quantile-based analysis of news intensity
Expected output: each week tagged with news_count_quartile
--------------------------------------------------------------------------- */
SELECT week_end_date,
       headline_count,
       NTILE(4) OVER (ORDER BY headline_count) AS news_count_quartile
FROM news_weekly
ORDER BY week_end_date;

/* ---------------------------------------------------------------------------
Query 11: Subquery for top 10% VIX shock weeks and next-week GLD return
What: Focus on extreme VIX weeks and summarize target performance
How: Subquery computes 90th percentile threshold using ordered row logic
Why: Tests behavior in stress tails
Expected output: count and average next-week GLD return under top VIX shocks
--------------------------------------------------------------------------- */
WITH ranked AS (
  SELECT vix_ret,
         target_next_gld_ret,
         ROW_NUMBER() OVER (ORDER BY vix_ret) AS rn,
         COUNT(*) OVER () AS n
  FROM market_weekly_returns
  WHERE target_next_gld_ret IS NOT NULL
), thresh AS (
  SELECT MIN(vix_ret) AS p90_vix_ret
  FROM ranked
  WHERE rn >= CAST(0.9 * n AS INT)
)
SELECT COUNT(*) AS n_top_vix_weeks,
       AVG(target_next_gld_ret) AS avg_next_gld_ret_top_vix
FROM market_weekly_returns
WHERE vix_ret >= (SELECT p90_vix_ret FROM thresh)
  AND target_next_gld_ret IS NOT NULL;

/* ---------------------------------------------------------------------------
Query 12: Subquery for unusually high news weeks
What: Identify high-news anomaly weeks and summarize next-week return
How: Subquery computes threshold from news_weekly distribution
Why: Checks whether rare high-information weeks differ in outcomes
Expected output: count and average next-week return for high-news anomaly weeks
--------------------------------------------------------------------------- */
WITH stats AS (
  SELECT AVG(headline_count) AS mu,
         sqrt(AVG(headline_count * headline_count) - AVG(headline_count) * AVG(headline_count)) AS sigma
  FROM news_weekly
), joined AS (
  SELECT m.week_end_date,
         m.target_next_gld_ret,
         COALESCE(n.headline_count, 0) AS headline_count
  FROM market_weekly_returns m
  LEFT JOIN news_weekly n
    ON m.week_end_date = n.week_end_date
  WHERE m.target_next_gld_ret IS NOT NULL
)
SELECT COUNT(*) AS n_high_news_weeks,
       AVG(target_next_gld_ret) AS avg_next_gld_ret_high_news
FROM joined
WHERE headline_count > (
  SELECT mu + sigma FROM stats
);
