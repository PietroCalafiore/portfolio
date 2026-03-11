    SELECT
    create_date AS date,
    'Inbound' AS lead_source,
    country AS market,
    'Individuals' AS segment_funnel,
    target AS target,
    spec_split_test AS specialization,
    CASE WHEN db_channel_short IN ('Organic/Direct', 'PMS database') THEN 'Organic'
        WHEN db_channel_short  = 'Paid_direct' THEN 'Paid'
        WHEN db_channel_short  = 'Events_direct' THEN 'Events'
        ELSE db_channel_short END
        AS acquisition_channel,
        CASE WHEN verified THEN 'Verified' ELSE 'Unverified' END AS verified,
         0 AS marketing_database,
    SUM(rolling_total) AS eom_marketing_database,
    0 AS lost_marketing_database
    FROM tableau_extract.marketing_budget_doc_db_eom
WHERE create_date BETWEEN '2024-01-01' AND '2025-06-01' AND
country IN ('Colombia', 'Spain', 'Mexico', 'Brazil', 'Italy', 'Poland','Germany','Chile','Turkiye')
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
ORDER BY 1, 2, 3, 4, 5, 6, 7, 8
