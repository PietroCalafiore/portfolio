  SELECT
    month AS date,
    'Inbound' AS lead_source,
    country AS market,
    'Individuals' AS segment_funnel,
    CASE WHEN country = 'Italy' THEN target ELSE 'DOCTOR' END AS target,
    CASE WHEN country IN ('Brazil', 'Poland', 'Chile', 'Mexico') THEN new_spec ELSE 'Undefined' END AS specialization,
    CASE WHEN max_channel = 'Organic/Direct' THEN 'Organic'
        WHEN max_channel = 'Paid_direct' THEN 'Paid'
        WHEN max_channel = 'Events_direct' THEN 'Events'
        ELSE max_channel END
        AS acquisition_channel,
    verified,
    direct_indirect_flag,
    CASE WHEN max_channel IN ('Paid_direct', 'Events_direct', 'Referral') THEN 'Active'
     WHEN month < '2025-01-01' AND active_passive = 'active_source' THEN 'Active'
         WHEN month < '2025-01-01' AND active_passive = 'passive_source' THEN 'Passive'
        ELSE active_passive_lead END AS active_passive,
      0 AS signups,
    COUNT(distinct contact_id) AS MQLS FROM test.cj_mqls_monthly
WHERE lifecycle_stage != 'only_won' AND month >= '2024-01-01' AND month < '2025-07-01'
AND country != 'Argentina'
GROUP BY  1, 2, 3, 4, 5, 6, 7, 8, 9, 10
ORDER BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
