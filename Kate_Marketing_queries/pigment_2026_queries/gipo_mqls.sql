    SELECT
    month AS date,
    'Inbound' AS lead_source,
    country AS market,
    CASE WHEN mql_product = 'Clinic Agenda' THEN 'Clinics' ELSE 'PMS' END AS segment_funnel,
    CASE WHEN mql_product = 'Clinic Agenda' THEN 'Facilities' ELSE 'PMS' END AS target,
    NULL AS specialization,
    CASE WHEN db_channel_short IN ('Organic/Direct', 'PMS database') THEN 'Organic'
        WHEN db_channel_short  = 'Paid_direct' THEN 'Paid'
        WHEN db_channel_short  = 'Events_direct' THEN 'Events'
        ELSE db_channel_short END
        AS acquisition_channel,
        verified,
    direct_indirect_flag,
    active_passive_final AS active_passive,
    0 AS signups,
    COUNT(distinct contact_id) AS MQLs,
    0 AS PQLs
    FROM test.cj_mqls_monthly_clinics_1
WHERE lifecycle_stage != 'only_won' AND month BETWEEN '2024-06-01' AND '2025-06-01'
AND mql_product IS NOT NULL AND mql_product IN ('Gipo') AND country IN ('Colombia', 'Spain', 'Mexico', 'Brazil', 'Italy', 'Poland')
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
ORDER BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
