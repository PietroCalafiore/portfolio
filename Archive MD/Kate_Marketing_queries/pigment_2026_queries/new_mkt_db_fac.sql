    SELECT
    create_date AS date,
    'Inbound' AS lead_source,
    country AS market,
    'Clinics' AS segment_funnel,
    'Facilities' AS target,
    NULL AS specialization,
    CASE WHEN db_channel_short IN ('Organic/Direct', 'PMS database') THEN 'Organic'
        WHEN db_channel_short  = 'Paid_direct' THEN 'Paid'
        WHEN db_channel_short  = 'Events_direct' THEN 'Events'
        ELSE db_channel_short END
        AS acquisition_channel,
        NULL AS verified,
           0 AS marketing_database,
    SUM(total_new_marketing_db) AS new_marketing_database,
    0 AS lost_marketing_database
    FROM tableau_extract.marketing_budget_fac_new_db
WHERE create_date BETWEEN '2024-01-01' AND '2025-06-01' AND
country IN ('Colombia', 'Spain', 'Mexico', 'Brazil', 'Italy', 'Poland','Germany','Chile','Turkiye')
AND subbrand = 'Docplanner'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
ORDER BY 1, 2, 3, 4, 5, 6, 7, 8
