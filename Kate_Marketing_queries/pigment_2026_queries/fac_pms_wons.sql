--FAC WONs
WITH preselection AS (
  SELECT
    deal_id,
    deal_month AS date,
    'Inbound' AS lead_source,
    cj.country AS market,
    CASE WHEN product_won = 'Clinic Agenda' THEN 'Clinics' ELSE 'PMS' END AS segment_funnel,
    CASE WHEN product_won = 'Clinic Agenda' THEN 'Facilities' ELSE 'PMS' END AS target,
     'Undefined' AS specialization,
    CASE WHEN db_channel_short IN ('Organic/Direct', 'PMS database') THEN 'Organic'
        WHEN db_channel_short = 'Paid_direct' THEN 'Paid'
        WHEN db_channel_short = 'Events_direct' THEN 'Events'
        ELSE db_channel_short END
        AS acquisition_channel,
    direct_indirect_flag,
    currency_code,
    CASE WHEN db_channel_short IN ('Paid_direct', 'Events_direct', 'Referral') THEN 'Active'
     WHEN month < '2025-01-01' AND active_passive = 'active_source' THEN 'Active'
         WHEN month < '2025-01-01' AND active_passive = 'passive_source' THEN 'Passive'
        ELSE active_passive_lead END AS active_passive,
      'Undefined' AS main_product, --what should I put for Clinics & PMS?
      'Undefined' AS package,
    COALESCE(opp.number_profiles, opp.number_profiles_pms, 1) AS new_profiles, --na for PMS, what should I put=
    mrr_euro_final,
    opp.mrr_original_currency AS new_mrr_lc
  FROM mart_cust_journey.cj_mqls_monthly_clinics cj
  LEFT JOIN mart_cust_journey.cj_opportunity_facts opp ON cj.deal_id = opp.opportunity_id
WHERE lifecycle_stage = 'only_won' AND deal_month >= '2024-01-01' AND deal_month < '2025-07-01' AND product_won IN ('Clinic Agenda', 'Gipo', 'Clinic Cloud')
AND cj.country != 'Argentina' -- AND number_profiles IS NULL AND number_profiles_pms IS NULL
GROUP BY  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
ORDER BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13)
SELECT
    date,
    lead_source,
    market,
    segment_funnel,
    target,
    specialization,
    acquisition_channel,
    direct_indirect_flag AS type,
    active_passive AS source,
    main_product,
    package,
    currency_code AS local_currency,
    COUNT(DISTINCT deal_id) AS won_deals,
    SUM(new_profiles) AS new_profiles,
    SUM(new_mrr_lc) AS new_mrr_lc,
    ROUND(SUM(mrr_euro_final),2) AS new_mrr_eur
FROM preselection
    GROUP BY  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
    ORDER BY  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
