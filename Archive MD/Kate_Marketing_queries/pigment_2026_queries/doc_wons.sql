--WONS DOC
WITH preselection AS (
  SELECT
      deal_id,
    deal_month AS date,
    'Inbound' AS lead_source,
    cj.country AS market,
    'Individuals' AS segment_funnel,
    CASE WHEN cj.country = 'Italy' AND target = 'GP' THEN 'GPs' ELSE 'Doctors' END AS target,
    CASE WHEN cj.country IN ('Brazil', 'Poland', 'Chile', 'Mexico') THEN new_spec ELSE 'Undefined' END AS specialization,
    CASE WHEN max_channel = 'Organic/Direct' THEN 'Organic'
        WHEN max_channel = 'Paid_direct' THEN 'Paid'
        WHEN max_channel = 'Events_direct' THEN 'Events'
        ELSE max_channel END
        AS acquisition_channel,
    direct_indirect_flag,
    CASE WHEN max_channel IN ('Paid_direct', 'Events_direct', 'Referral') THEN 'Active'
     WHEN month < '2025-01-01' AND active_passive = 'active_source' THEN 'Active'
         WHEN month < '2025-01-01' AND active_passive = 'passive_source' THEN 'Passive'
        ELSE active_passive_lead END AS active_passive,
      'Premium' AS main_product,
      COALESCE(opp.tier_pricing, 'Undefined') AS package,
    opp.number_profiles AS new_profiles,
    opp.mrr_original_currency AS new_mrr_lc,
    currency_code,
    cj.mrr_euro_final
  FROM test.cj_mqls_monthly cj
  LEFT JOIN mart_cust_journey.cj_opportunity_facts opp ON cj.deal_id = opp.opportunity_id
WHERE lifecycle_stage = 'only_won' AND deal_month >= '2024-01-01' AND deal_month < '2025-07-01' AND (ecommerce_deal_flag IS FALSE OR ecommerce_deal_flag IS NULL)
AND cj.country != 'Argentina'
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
