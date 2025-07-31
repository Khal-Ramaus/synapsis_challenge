SELECT
    date as "Date",
    total_tons_mined_daily as "Total Tons Mined"
FROM
    daily_production_metrics
WHERE 1=1
    [[and cast(date as date)>={{start_date}}]]
    [[and cast(date as date)<={{end_date}}]]
ORDER BY
    date ASC;