WITH candlesticks AS (
    SELECT
        time_bucket('1 hour', time),
        symbol,
        candlestick_agg(time, price, day_volume) AS agg
    FROM
        crypto_ticks
    WHERE
        symbol = 'BTC/USD'
    GROUP BY 1,2
),
sma AS (
    SELECT
        time_bucket,
        symbol,
        avg((agg).close) OVER (PARTITION BY symbol ORDER BY time_bucket ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS sma
    FROM
        candlesticks
),
std_dev AS (
    SELECT
        sma.time_bucket,
        sma.symbol,
        sqrt(avg(((agg).close - sma) * ((agg).close - sma)) OVER (PARTITION BY sma.symbol ORDER BY sma.time_bucket ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)) AS stddev
    FROM
        candlesticks,
        sma
    WHERE
        candlesticks.time_bucket = sma.time_bucket
        AND candlesticks.symbol = sma.symbol
)
SELECT
    std_dev.time_bucket,
    std_dev.symbol,
    sma,
    sma + 2 * stddev AS upper_band,
    sma - 2 * stddev AS lower_band
FROM
    sma,
    std_dev
WHERE
    sma.time_bucket = std_dev.time_bucket
    AND sma.symbol = std_dev.symbol
ORDER BY
    time_bucket ASC;
