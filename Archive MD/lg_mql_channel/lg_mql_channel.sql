WITH mqls AS (SELECT
    cmm.contact_id AS contact,
    cmm.true_deal_flag,
    CAST(LAG(cmm.contact_id)
        OVER (PARTITION BY cmm.contact_id, cmm.deal_month ORDER BY cmm.deal_month) AS VARCHAR) AS won_id_for_deduping,
    cmm.country,
    cmm.deal_allocation,
    cmm.sub_brand,
    cmm.lifecycle_stage,
    cmm.target AS segment,
    cmm.new_spec,
    cmm.verified,
    MAX(cmm.source_so)
    OVER (PARTITION BY cmm.contact_id, DATE(cmm.deal_month)) AS source,
    DATE(cmm.month) AS mql_month,
    MIN(DATE(cmm.lifecycle_stage_start)) AS mql_day,
    DATE(cmm.deal_month) AS deal_month,
    MAX(DATE(cmm.deal_stage_start)) AS deal_at,
    all_mqls.mql_product AS mql_product,
    cmm.mrr_euro_final AS mrr,
    COALESCE(cmm.mql_last_touch_channel, 'Unknown') AS channel,
    COALESCE(cmm.mql_conversion_place, 'Unknown') AS conversion_place,
    cmm.marketplace_mql_flag
    FROM mart_cust_journey.cj_mqls_monthly AS cmm
    LEFT JOIN mart_cust_journey.inbound_all_mqls AS all_mqls
        ON cmm.contact_id = all_mqls.contact_id
    WHERE (DATE(cmm.month) >= '2024-01-01' OR cmm.deal_month >= '2024-01-01')
    GROUP BY cmm.country,
        cmm.sub_brand,
        cmm.lifecycle_stage,
        cmm.target,
        cmm.new_spec,
        cmm.verified,
        cmm.source_so,
        DATE(cmm.month),
        all_mqls.mql_product,
        cmm.mql_last_touch_channel,
        cmm.mql_conversion_place,
        cmm.deal_allocation,
        cmm.contact_id,
        cmm.true_deal_flag,
        cmm.deal_month,
        cmm.mrr_euro_final,
        cmm.marketplace_mql_flag

    UNION ALL

    SELECT
        cmm.contact_id AS contact,
        cmm.true_deal_flag,
        CAST(LAG(cmm.contact_id)
            OVER (PARTITION BY cmm.contact_id, cmm.deal_month ORDER BY cmm.deal_month) AS VARCHAR) AS won_id_for_deduping,
        cmm.country,
        'Inbound' AS deal_allocation,
        hcl.sub_brand_wf_test AS sub_brand,
        cmm.lifecycle_stage,
        cmm.target AS segment,
        'FACILITY' AS new_spec,
        cmm.verified,
        MAX(cmm.source_so)
        OVER (PARTITION BY cmm.contact_id, DATE(cmm.deal_month)) AS source,
        DATE(cmm.month) AS mql_month,
        MIN(DATE(cmm.lifecycle_stage_start)) AS mql_day,
        DATE(cmm.deal_month) AS deal_month,
        MAX(DATE(hcl.recent_deal_close_date)) AS deal_at,
        all_mqls.mql_product AS mql_product,
        cmm.mrr_euro_final AS mrr,
        COALESCE(cmm.mql_last_touch_channel, 'Unknown') AS channel,
        COALESCE(cmm.mql_conversion_place, 'Unknown') AS conversion_place,
        hcl.marketplace_mql_flag
    FROM mart_cust_journey.cj_mqls_monthly_clinics AS cmm
    LEFT JOIN dw.hs_contact_live AS hcl
        ON cmm.contact_id = hcl.hubspot_id
    LEFT JOIN mart_cust_journey.inbound_all_mqls AS all_mqls
        ON cmm.contact_id = all_mqls.contact_id
    WHERE (DATE(cmm.month) >= '2024-01-01' OR cmm.deal_month >= '2024-01-01')
    GROUP BY cmm.country,
        hcl.sub_brand_wf_test,
        cmm.lifecycle_stage,
        cmm.target,
        cmm.verified,
        cmm.source_so,
        DATE(cmm.month),
        all_mqls.mql_product,
        cmm.mql_last_touch_channel,
        cmm.mql_conversion_place,
        cmm.contact_id,
        cmm.true_deal_flag,
        cmm.deal_month,
        cmm.mrr_euro_final,
        hcl.marketplace_mql_flag

    UNION ALL

    SELECT
        CAST(noa.hubspot_id AS BIGINT) AS contact,
        COALESCE(noa.deal_id IS NOT NULL, FALSE) AS true_deal_flag,
        CAST(LAG(noa.hubspot_id)
            OVER (PARTITION BY noa.hubspot_id, noa.deal_month ORDER BY noa.deal_month) AS VARCHAR) AS won_id_for_deduping,
        noa.country,
        'Inbound' AS deal_allocation,
        hcl.sub_brand_wf_test AS sub_brand,
        hcl.lifecycle_stage,
        noa.target AS segment,
        hcl.spec_split_test AS new_spec,
        CASE
            WHEN COALESCE(hcl.verified, hcl.facility_verified) IN ('true', 't', '1', TRUE) THEN 'true'
            ELSE 'false'
        END AS verified,
        MAX(noa.hs_noa_last_source)
        OVER (PARTITION BY noa.hubspot_id, DATE(noa.deal_month)) AS source,
        CAST(DATE_TRUNC('month', noa.noa_mql_at) AS DATE) AS mql_month,
        MIN(DATE(COALESCE(noa.noa_mql_at, noa.noa_pql_at))) AS mql_day,
        CASE
            WHEN noa.deal_id IS NOT NULL THEN CAST(DATE_TRUNC('month', noa.mql_deal_at) AS DATE)
        END AS deal_month,
        CASE
            WHEN noa.deal_id IS NOT NULL THEN MAX(DATE(noa.mql_deal_at))
        END AS deal_at,
        'Noa' AS mql_product,
        noa.mrr_euro AS mrr,
        COALESCE(noa.mql_last_touch_channel, 'Unknown') AS channel,
        COALESCE(noa.mql_last_conversion_place, 'Unknown') AS conversion_place,
        NULL AS marketplace_mql_flag
    FROM mart_cust_journey.noa_marketing_kpis_cm_lg_combined AS noa
    LEFT JOIN dw.hs_contact_live AS hcl
        ON noa.hubspot_id = hcl.hubspot_id
    GROUP BY noa.country,
        hcl.sub_brand_wf_test,
        hcl.lifecycle_stage,
        noa.target,
        hcl.spec_split_test,
        hcl.verified,
        hcl.facility_verified,
        noa.hs_noa_last_source,
        noa.noa_mql_at,
        noa.noa_pql_at,
        noa.deal_id,
        noa.mql_deal_at,
        noa.deal_month,
        noa.hubspot_id,
        noa.mrr_euro,
        noa.mql_last_touch_channel,
        noa.mql_last_conversion_place
)

SELECT
    COUNT(DISTINCT contact) AS contacts,
    COUNT(DISTINCT CASE WHEN lifecycle_stage != 'only_won' THEN contact END) AS mql_count,
    COUNT(DISTINCT CASE
        WHEN true_deal_flag
            AND deal_allocation = 'Inbound'
            AND mqls.won_id_for_deduping IS NULL
            THEN contact
        END) AS deal_count,
    deal_month,
    country,
    sub_brand,
    segment,
    new_spec AS spec_split,
    verified,
    source,
    mql_month AS month,
    mql_day AS day,
    deal_at,
    CASE WHEN LOWER(mql_product) LIKE '%noa%' THEN 'Noa' ELSE mql_product END AS mql_product,
    mrr,
    channel,
    conversion_place,
    marketplace_mql_flag
FROM mqls
GROUP BY country,
         sub_brand,
         deal_month,
         segment,
         spec_split,
         verified,
         source,
         mql_month,
         mql_day,
         channel,
         conversion_place,
         deal_at,
         mql_product,
         mrr,
         marketplace_mql_flag
