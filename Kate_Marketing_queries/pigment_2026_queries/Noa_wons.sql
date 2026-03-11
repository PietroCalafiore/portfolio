--Noa WONs
WITH preselection AS (
  SELECT
    cj.deal_id,
    cj.deal_month AS date,
    'Inbound' AS lead_source,
    cj.country AS market,
    CASE WHEN cj.customer_type_all = 'Individual' THEN 'Individuals'
         WHEN cj.customer_type_all = 'PMS' THEN 'PMS'
        ELSE 'Clinics' END AS segment_funnel,
    'Noa Notes' AS target,
    'Undefined' AS specialization,
    CASE WHEN noa_deal_last_source = 'Customer Reference Noa' THEN 'Referral'
        WHEN paid.hubspot_id THEN 'Paid'
        WHEN noa_deal_last_source IN ('Event Noa') THEN 'Event'
    WHEN hsh.sub_brand_wf_test = 'Noa' AND hsh.hs_analytics_source = 'PAID_SOCIAL'--add subbrand condition to non-direct MQLs to avoid including Agenda-related actions in this breakdowns
                    AND LOWER(hsh.hs_analytics_source_data_1) IN ('facebook', 'instagram', 'linkedin' ) THEN 'Paid'
                WHEN  hsh.sub_brand_wf_test = 'Noa' AND hsh.hs_analytics_source = 'PAID_SEARCH'
                    AND (LOWER(hsh.hs_analytics_source_data_1) IN ('yahoo', 'bing', 'google')
                        OR LOWER(hsh.hs_analytics_source_data_2) LIKE '%_%'
                        OR LOWER(hsh.hs_analytics_source_data_1) LIKE '%_%'
                        OR LOWER(hsh.hs_analytics_source_data_2) = 'google') THEN 'Paid'
    WHEN hsh.sub_brand_wf_test = 'Noa' AND hsh.doctor_facility___marketing_events_tag IS NOT NULL THEN 'Event'
    WHEN hsh.sub_brand_wf_test = 'Noa' AND (hsh.affiliate_source LIKE '%callcenter%' OR hsh.marketing_action_tag2 LIKE '%callcenter%') THEN 'Call Center'
    ELSE 'Organic' END AS acquisition_channel,
    CASE WHEN hsh.verified THEN 'Verified' ELSE 'Unverified' END AS verified,
    CASE WHEN paid.hubspot_id OR noa_deal_last_source IN ('Event Noa', 'Customer Reference Noa') THEN 'Direct'
    ELSE 'Indirect' END AS type,
    CASE WHEN noa_deal_last_source IN ('Upgrade Noa', 'Event Noa', 'Contact form Noa',
    'Noa Trial Germany', 'Sales Demo Ordered Noa', 'Buy Form Noa', 'Customer Reference Noa') OR paid.hubspot_id
        THEN 'Active' ELSE 'Passive' END AS source, --ok Martyna
      'Undefined' AS main_product, --what should I put for Clinics & PMS?
      'Undefined' AS package,
    COALESCE(opp.number_profiles_noa, 1) AS new_profiles, --na for PMS, what should I put=
    cj.mrr_euro,
    opp.currency_code,
    opp.mrr_original_currency AS new_mrr_lc,
    ROW_NUMBER() OVER (PARTITION BY cj.deal_id ORDER BY hsh.start_date ASC) AS row_per_lcs--get the first row once the contact become MQL/PQL
    FROM mart_cust_journey.noa_inbound_deals cj
    LEFT JOIN mart_cust_journey.cj_opportunity_facts opp ON cj.deal_id = opp.opportunity_id
    LEFT JOIN dw.hs_contact_live_history hsh ON hsh.hubspot_id = cj.hubspot_id AND DATE_TRUNC('month', hsh.noa_lead_at) = cj.deal_month
    LEFT JOIN mart_cust_journey.msv_paid_media_campaign_hubspot_2 paid ON cj.hubspot_id = paid.hubspot_id
    AND cj.deal_opened_at BETWEEN paid.web_date AND paid.web_date::date + interval '30 day'
                AND (cj.deal_month >= date_trunc('month',paid.date))
  WHERE lg_cm_flag = 'LG' AND cj.deal_month >= '2024-01-01' AND cj.deal_month < '2025-07-01'
    AND cj.country != 'Argentina' -- AND number_profiles IS NULL AND number_profiles_pms IS NULL
      GROUP BY  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, hsh.start_date
  QUALIFY row_per_lcs = 1
)
SELECT
    date::DATE,
    lead_source,
    market,
    segment_funnel,
    target,
    specialization,
    acquisition_channel,
    type,
    source,
    main_product,
    package,
    currency_code AS local_currency,
    COUNT(DISTINCT deal_id) AS won_deals,
    SUM(new_profiles) AS new_profiles,
    SUM(new_mrr_lc) AS new_mrr_lc,
    ROUND(SUM(mrr_euro),2) AS new_mrr_eur
FROM preselection
    GROUP BY  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
    ORDER BY  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12

