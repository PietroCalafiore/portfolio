
--Noa MQLs query
WITH pre_calculation AS (
SELECT
    COALESCE(DATE_TRUNC('month', mql_deal_at), DATE_TRUNC('month', pql_deal_at)) AS month,
    'Inbound' AS lead_source,
    noa.country AS market,
    CASE WHEN noa.segment = 'Individual' THEN 'Individuals'
    ELSE noa.segment END AS segment_funnel,
    'Noa Notes' AS target,
    'Undefined' AS specialization,
    CASE WHEN hs_noa_last_source = 'Customer Reference Noa' THEN 'Referral'
        WHEN paid.hubspot_id THEN 'Paid'
        WHEN hs_noa_last_source IN ('Event Noa') THEN 'Event'
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
    CASE WHEN paid.hubspot_id OR hs_noa_last_source IN ('Event Noa', 'Customer Reference Noa') THEN 'Direct'
    ELSE 'Indirect' END AS type,
    CASE WHEN hs_noa_last_source IN ('Upgrade Noa', 'Event Noa', 'Contact form Noa',
    'Noa Trial Germany', 'Sales Demo Ordered Noa', 'Buy Form Noa', 'Customer Reference Noa') OR paid.hubspot_id
        THEN 'Active' ELSE 'Passive' END AS source, --ok Martyna
    CASE WHEN mql_pql_flag = 'MQL' THEN open_deal_id END AS MQL_id,
    CASE WHEN mql_pql_flag = 'PQL' THEN open_deal_id END AS PQL_id,
    ROW_NUMBER() OVER (PARTITION BY noa.hubspot_id, COALESCE(DATE_TRUNC('month', mql_deal_at), DATE_TRUNC('month', pql_deal_at)) ORDER BY hsh.start_date ASC) AS row_per_lcs--get the first row once the contact become MQL/PQL
FROM mart_cust_journey.noa_marketing_kpis_cm_lg_combined noa
LEFT JOIN dw.hs_contact_live_history hsh ON hsh.hubspot_id = noa.hubspot_id AND hsh.noa_lead_at = COALESCE(noa.noa_mql_at, noa.noa_pql_at) --temp workaround to catch merged MQLs and get their correct last source as otherwise we lose them
LEFT JOIN mart_cust_journey.msv_paid_media_campaign_hubspot_2 paid ON noa.hubspot_id = paid.hubspot_id
            AND campaign like '%mql%' AND campaign_product = 'Noa' AND paid.target IN ('DOCTOR', 'GP', 'FACILITY') AND (paid.br_specialisation != 'Bad Paramedical' OR paid.br_specialisation IS NULL)
            AND (DATE_TRUNC('month', mql_deal_at) = DATE_TRUNC('month', paid.date) OR DATE_TRUNC('month', pql_deal_at) = DATE_TRUNC('month', paid.date))
where lg_cm_flag_combined = 'LG' AND (mql_excluded IS FALSE OR mql_excluded IS NULL) --AND noa.hubspot_id = 65960951
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, noa.hubspot_id, hsh.start_date
QUALIFY row_per_lcs = 1 AND month IS NOT NULL)

SELECT
     month::DATE,
     lead_source,
     market,
     segment_funnel,
     target,
     specialization,
     acquisition_channel,
     verified,
     type,
     source,
     0 AS signups,
     COUNT(DISTINCT MQL_id) AS MQLs,
    COUNT(DISTINCT PQL_id) AS PQLs
     FROM pre_calculation
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9 , 10
    ORDER BY 1, 2, 3, 4, 5, 6, 7, 8, 9 , 10
