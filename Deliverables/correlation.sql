SELECT
    cast(date as date) as date,
    daily_rainfall_mm,
    total_tons_mined_daily
FROM
    daily_production_metrics
WHERE 1=1
    [[and cast(date as date)>={{start_date}}]]
    [[and cast(date as date)<={{end_date}}]]