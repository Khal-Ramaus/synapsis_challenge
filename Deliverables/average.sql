SELECT
    m.mine_name as "Mine Name",
    AVG(pl.quality_grade) AS "Average Quality Grade"
FROM
    production_logs pl
LEFT JOIN
    mines m ON pl.mine_id = m.mine_id
WHERE 1=1
    [[and cast(date as date)>={{start_date}}]]
    [[and cast(date as date)<={{end_date}}]]
GROUP BY
    pl.mine_id
ORDER BY
     AVG(pl.quality_grade) DESC;