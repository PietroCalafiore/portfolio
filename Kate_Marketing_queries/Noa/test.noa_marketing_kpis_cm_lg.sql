--combining LG and CM in one Tableau query test
--noa_marketing_kpis_lg --STEP 3
DROP TABLE IF EXISTS test.noa_marketing_kpis_cm_lg;

CREATE TABLE test.noa_marketing_kpis_cm_lg AS
WITH won_noa_deals_ls AS ( --selecting Closed WON Noa deals belonging to Inbound, excluding trials
    SELECT * FROM mart_cust_journey.noa_inbound_deals
    --WHERE lg_cm_flag = 'LG'
),

all_sales_trials AS ( --first step to calculate trials that belong to sales. we use deal_hs table as we need maximum granularity to get negotiation type update date
    SELECT
        hubspot_id AS deal_id,
        noa_notes_trial_yes_no,
        updated_at AS trial_yes_no_set_date, --date negotiation type property was first set
        LAG(noa_notes_trial_yes_no) OVER (PARTITION BY hubspot_id ORDER BY updated_at) AS lag
    FROM dv_raw.sat_deal_hs_1h_log
    WHERE noa_notes_trial_yes_no = 'Yes' AND createdate >= '2025-03-15'
    GROUP BY hubspot_id, noa_notes_trial_yes_no, updated_at
    QUALIFY lag is NULL
),

trials_to_exclude AS ( --This identifies if a trial belongs to sales based on trial set dates compared to activation dates. Goal is to match MOT Sales/MKT split workflow + static list numbers
    SELECT
        deal.deal_id AS deal_id,
        deal.hs_contact_id,
        deal.closedate,
        deal.createdate,
        deal.country,
        deal.deal_stage,
        hs1.email,
        hs.product_so_cs,
        hs1.noa_trial_source_test,
        CASE WHEN hs1.noa_notes_stda_last_activation_at_batch <= hs1.noa_notes_saas_last_activation_at_batch THEN
            ROW_NUMBER() OVER (PARTITION BY hs.hubspot_id ORDER BY COALESCE(hs1.noa_notes_stda_last_activation_at_batch, hs1.noa_notes_saas_last_activation_at_batch))
            ELSE ROW_NUMBER() OVER (PARTITION BY hs.hubspot_id ORDER BY COALESCE(hs1.noa_notes_saas_last_activation_at_batch, hs1.noa_notes_stda_last_activation_at_batch))
        END AS row_per_deal,
        hs1.noa_notes_stda_last_activation_at_batch,
        hs1.noa_notes_saas_last_activation_at_batch,
        tsd.noa_notes_trial_yes_no,
        tsd.trial_yes_no_set_date,
        deal.dealname
    FROM all_sales_trials tsd
    LEFT JOIN mart_cust_journey.cj_deal_month deal ON deal.deal_id = tsd.deal_id
    LEFT JOIN dw.hs_deal_live hs ON deal.deal_id = hs.hubspot_id
    LEFT JOIN dw.hs_contact_live_history hs1 ON deal.hs_contact_id = hs1.hubspot_id
    WHERE deal.createdate > '2025-03-01' --we also check this from March because thats when the trial property was added to dv_raw.sat_deal_hs_1h_log
        AND (tsd.trial_yes_no_set_date <= hs1.noa_notes_stda_last_activation_at_batch OR tsd.trial_yes_no_set_date <= hs1.noa_notes_saas_last_activation_at_batch)
        AND deal.pipeline = 'Noa' AND deal.deal_stage IN ('Contract Signed', 'Decision Maker Reached', 'Contacting', 'Closed Won', 'Demo / Sales Meeting Done')
    QUALIFY row_per_deal = 1
),

all_lg_saas_trials AS (--calculating valid LG saas Noa trials, excluding CS/Sales affiliate source and customers at the time
    SELECT
        hs.noa_notes_saas_last_activation_at_batch AS trial_at,
        hs.hubspot_id,
        hs.noa_trial_source_test AS noa_trial_source,
        hs.contact_type_segment_test,
        hs.member_customers_list AS was_customer_at_noa_interaction,
        ROW_NUMBER() OVER (PARTITION BY hs.hubspot_id, DATE_TRUNC('month', hs.noa_notes_saas_last_activation_at_batch) ORDER BY hs.start_date ASC) AS row_per_trial
    FROM dw.hs_contact_live_history hs
    WHERE hs.noa_notes_saas_last_activation_at_batch IS NOT NULL
        AND (hs.affiliate_source NOT IN ('noa_cs', 'noa_sales', 'noa_clinics_cs', 'noa_clinics_sales', 'noa_pms_cs', 'noa_pms_sales', 'noa_tuotempo_sales', 'noa_tuotempo_cs') OR hs.affiliate_source IS NULL)
    QUALIFY row_per_trial = 1 AND
        (hs.email NOT LIKE '%docplanner.com%' AND hs.email NOT LIKE '%shahar%' AND hs.email NOT LIKE '%test%' AND hs.email NOT LIKE '%miodottore%'
    AND hs.email NOT LIKE '%staszek.marta%' OR hs.email IS NULL) AND (hs.contact_type_segment_test NOT IN ('PATIENT', 'STUDENTS') OR hs.contact_type_segment_test IS NULL)
),

all_lg_stda_trials AS (--calculating valid LG stda Noa trials, excluding CS/Sales affiliate source and customers at the time
    SELECT
        hs.noa_notes_stda_last_activation_at_batch AS trial_at,
        hs.hubspot_id,
        hs.noa_trial_source_test AS noa_trial_source,
        hs.contact_type_segment_test,
        hs.member_customers_list AS was_customer_at_noa_interaction,
        ROW_NUMBER() OVER (PARTITION BY hs.hubspot_id, DATE_TRUNC('month', hs.noa_notes_stda_last_activation_at_batch) ORDER BY hs.start_date ASC) AS row_per_trial
    FROM dw.hs_contact_live_history hs
    WHERE hs.noa_notes_stda_last_activation_at_batch IS NOT NULL
        AND (hs.affiliate_source NOT IN ('noa_cs', 'noa_sales', 'noa_clinics_cs', 'noa_clinics_sales', 'noa_pms_cs', 'noa_pms_sales', 'noa_tuotempo_sales', 'noa_tuotempo_cs') OR hs.affiliate_source IS NULL)
    QUALIFY row_per_trial = 1
    AND (hs.email NOT LIKE '%docplanner.com%' AND hs.email NOT LIKE '%shahar%' AND hs.email NOT LIKE '%test%' AND hs.email NOT LIKE '%miodottore%'
    AND hs.email NOT LIKE '%staszek.marta%' OR hs.email IS NULL) AND (hs.contact_type_segment_test NOT IN ('PATIENT', 'STUDENTS') OR hs.contact_type_segment_test IS NULL)
),

all_lg_trials AS ( --unifying saas and stda trials
    SELECT
        *,
        CASE WHEN (was_customer_at_noa_interaction = 'No' OR  was_customer_at_noa_interaction IS NULL) THEN 'LG' ELSE 'CM' END AS lg_cm_trial_flag,
        'saas_trial' AS trial_flag
    FROM all_lg_saas_trials
    UNION DISTINCT
    SELECT
        *,
        CASE WHEN (was_customer_at_noa_interaction = 'No' OR  was_customer_at_noa_interaction IS NULL) THEN 'LG' ELSE 'CM' END AS lg_cm_trial_flag,
        'stda_trial' AS trial_flag
    FROM all_lg_stda_trials
),

all_trials_deduped AS ( --counting only one trial per month per contact  (either saas or stda, whichever happened first)
    SELECT
        t.hubspot_id,
        t.trial_at,
        t.trial_flag,
        t.noa_trial_source,
        t.lg_cm_trial_flag,
        ROW_NUMBER() OVER (PARTITION BY t.hubspot_id, DATE_TRUNC('month', t.trial_at) ORDER BY t.trial_at ASC) AS row_per_trial_per_month,
        ROW_NUMBER() OVER (PARTITION BY t.hubspot_id ORDER BY t.trial_at ASC) AS number_of_trials_per_contact
    FROM all_lg_trials t
    LEFT JOIN trials_to_exclude tte ON t.hubspot_id = tte.hs_contact_id --excluding previously calculated trials belonging to Sales
    WHERE 1 = 1
    QUALIFY row_per_trial_per_month = 1 AND tte.hs_contact_id IS NULL
),

noa_open_deals AS (
    SELECT
        country,
        deal_id::VARCHAR,
        hubspot_id::VARCHAR,
        email,
        budget_category,
        mql_pql_flag,
        COALESCE(pql_category,mql_category, 'stda') AS pql_mql_category,--if edge case, assume STDA
        mql_at,
        pql_at,
        create_date,
        DATE_TRUNC('month', create_date) AS create_month,
        noa_last_source,
        cm_lg_flag AS lg_cm_flag_mql,
        segment,
        segment AS contact_type,
        mql_last_touch_channel,
        mql_last_conversion_place,
        mql_excluded,
        days_between_mql_and_create_deal_date
    FROM test.noa_marketing_open_deals
    WHERE create_date >= '2025-06-01' --data before June not reliable as not all MQLs/PQLs had open deals
    UNION ALL
    SELECT
        country,
        hubspot_id::VARCHAR AS deal_id,
        hubspot_id::VARCHAR,
        email,
        'Individual pre-June MQL' AS budget_category,
        mql_pql_flag,
        COALESCE(pql_category,mql_category, 'stda') AS pql_mql_category,--if edge case, assume STDA
        mql_at,
        pql_at,
        mql_at AS create_date,
        DATE_TRUNC('month', mql_at) AS create_month,
        hs_noa_last_source AS noa_last_source,
        lg_cm_flag AS lg_cm_flag_mql,
        customer_type_all AS segment,
        segment AS contact_type,
        mql_last_touch_channel,
        mql_last_conversion_place,
        mql_excluded,
        0 AS days_between_mql_and_create_deal_date
    FROM test.noa_marketing_mqls_pqls
    WHERE mql_month < '2025-06-01'
    )

SELECT --joining together all KPIs (Trials, MQLS, PQLs, WONs and fields for Noa Inbound dashboard)
    hs.hubspot_id,
    COALESCE(hs.country, hs1.country) AS country, --workaround as in some Noa cases in Italy country property is updated late
    COALESCE(hs.country, hs1.country) AS country_for_filter,
    COALESCE(wnd.lg_cm_flag, nod.lg_cm_flag_mql, atd.lg_cm_trial_flag) AS lg_cm_flag_combined,
    hs.email,
    hs.affiliate_source,
    hs.sub_brand_wf_test,
    wnd.noa_deal_last_source AS frozen_last_source,
    wnd.paid_products_before_noa AS paid_products_frozen,
    nod.mql_last_touch_channel,
    nod.mql_last_conversion_place,
    hs.contact_type_segment_test,
    CASE WHEN COALESCE(hs.contact_type_segment_test, 'UNKNOWN') IN ('GENERAL PRACTITIONER', 'GENERAL PRACTITIONER & DOCTOR')
        THEN 'GP' ELSE 'DOCTOR' END AS target,
    --trial KPIs
    atd.trial_at AS free_signup_at,
    atd.trial_flag AS trial_flag,
    atd.noa_trial_source,
    atd.lg_cm_trial_flag,
    --mql/pql kpis
    CASE WHEN nod.mql_pql_flag = 'MQL' THEN nod.create_date END AS mql_deal_at,
    CASE WHEN nod.mql_pql_flag = 'PQL' THEN nod.create_date  END AS pql_deal_at,
    CASE WHEN nod.mql_pql_flag = 'MQL' THEN nod.mql_at END AS noa_mql_at,
    CASE WHEN nod.mql_pql_flag = 'PQL' THEN nod.pql_at END AS noa_pql_at,
    nod.lg_cm_flag_mql,
    nod.mql_pql_flag,
    nod.segment,
    nod.mql_excluded,
    nod.deal_id AS open_deal_id,
    nod.contact_type,
    --CASE WHEN (hs.product_qualified_noa_at IS NULL OR DATE_TRUNC('month', hs.noa_lead_at) != DATE_TRUNC('month', hs.product_qualified_noa_at)) THEN hs.noa_lead_at END AS noa_mql_at,
    --hs.product_qualified_noa_at AS noa_pql_at,
    --WON deal KPIs
    hs.noa_active_products_batch,
    wnd.deal_name,
    wnd.lg_cm_flag,
    wnd.deal_id,
    wnd.deal_month AS deal_month,
    wnd.mrr_euro AS mrr_euro,
    wnd.mrr_original_currency AS mrr_original_currency,
    wnd.deal_closed_at AS deal_close_date,
    wnd.deal_opened_at AS deal_create_date,
    COALESCE(nod.budget_category, wnd.budget_category) AS budget_category,
    wnd.bundle_flag,
    wnd.demo_watched_at,
    wnd.noa_deal_last_source,
    wnd.deal_product,
    COALESCE(wnd.deal_category,nod.pql_mql_category) AS deal_category,
    hs1.member_customers_list AS is_current_customer,
    nod.noa_last_source AS hs_noa_last_source,
    hs.member_customers_list AS was_noa_customer,
    hs.merged_vids AS was_merged,
    wnd.paid_products_before_noa,
    wnd.info_tag,
    wnd.customer_type_all,
    nod.days_between_mql_and_create_deal_date,
    DATE_DIFF('day', nod.pql_at::DATE, wnd.deal_closed_at::DATE) AS mql_to_won,
    DATE_DIFF('day', nod.mql_at::DATE, wnd.deal_closed_at::DATE) AS pql_to_won,
    CASE WHEN hs.noa_lead_at IS NOT NULL OR hs.noa_notes_stda_last_activation_at_batch IS NOT NULL OR hs.product_qualified_noa_at IS NOT NULL OR wnd.deal_month IS NOT NULL
        THEN ROW_NUMBER() OVER (PARTITION BY hs.hubspot_id, DATE_TRUNC('month', nod.mql_at), DATE_TRUNC('month', hs.noa_notes_stda_last_activation_at_batch), DATE_TRUNC('month', nod.pql_at), wnd.deal_closed_at ORDER BY hs.start_date ASC)
    END AS row_per_lcs
FROM dw.hs_contact_live_history hs
LEFT JOIN dw.hs_contact_live hs1 ON hs.hubspot_id = hs1.hubspot_id
LEFT JOIN all_trials_deduped atd ON atd.hubspot_id = hs.hubspot_id
LEFT JOIN noa_open_deals nod ON nod.hubspot_id = hs.hubspot_id
LEFT JOIN won_noa_deals_ls wnd ON wnd.deal_id = nod.deal_id
WHERE (hs.noa_lead_at IS NOT NULL OR hs.noa_notes_stda_last_activation_at_batch IS NOT NULL OR hs.product_qualified_noa_at IS NOT NULL OR hs.noa_notes_saas_last_activation_at_batch IS NOT NULL OR wnd.deal_month IS NOT NULL)
    AND hs.start_date >= '2024-08-01' AND hs1.is_deleted IS FALSE
GROUP BY
    hs.hubspot_id,
    hs.country,
    hs.email,
    hs.affiliate_source,
    hs.sub_brand_wf_test,
    wnd.deal_closed_at,
    hs.mql_last_touch_channel_wf,
    hs.mql_last_conversion_place_wf,
    hs.contact_type_segment_test,
    wnd.paid_products_before_noa,
    atd.trial_at,
    atd.lg_cm_trial_flag,
    wnd.bundle_flag,
    wnd.lg_cm_flag,
    atd.trial_flag,
    noa_mql_at,
    noa_pql_at,
    hs.noa_active_products_batch,
    wnd.deal_name,
    wnd.deal_id,
    wnd.deal_month,
    wnd.mrr_euro,
    wnd.mrr_original_currency,
    wnd.deal_opened_at,
    wnd.demo_watched_at,
    wnd.noa_deal_last_source,
    wnd.deal_product,
    wnd.deal_category,
    hs1.member_customers_list,
    nod.noa_last_source,
    wnd.customer_type_all,
    hs.member_customers_list,
    hs.merged_vids,
    hs.noa_lead_at,
    hs.noa_notes_stda_last_activation_at_batch,
    wnd.deal_month,
    hs.start_date,
    wnd.budget_category,
    wnd.info_tag,
    wnd.hubspot_id,
    wnd.was_merged,
    hs1.country,
    atd.noa_trial_source,
    nod.lg_cm_flag_mql,
    nod.segment,
    nod.deal_id,
    nod.mql_last_touch_channel,
    nod.mql_last_conversion_place,
    nod.mql_pql_flag,
    nod.segment,
    nod.mql_excluded,
    nod.create_date,
    nod.pql_at,
    nod.mql_at,
    hs.product_qualified_noa_at,
    nod.hubspot_id,
    nod.contact_type,
    nod.mql_pql_flag,
    nod.budget_category,
    nod.pql_mql_category,
    nod.days_between_mql_and_create_deal_date
QUALIFY row_per_lcs = 1
AND country_for_filter IN ('Brazil', 'Mexico', 'Spain', 'Italy', 'Germany', 'Colombia', 'Chile', 'Turkiye', 'Peru', 'Poland')

SELECT count(distinct open_deal_id),
       country,
       mql_pql_flag,
       lg_cm_flag_combined,
       lg_cm_flag_mql
       FROM test.noa_marketing_kpis_cm_lg WHERE (DATE_TRUNC('month', mql_deal_at) = '2025-06-01' OR DATE_TRUNC('month', pql_deal_at) = '2025-06-01')
    AND mql_excluded IS FALSE --AND lg_cm_flag_combined != lg_cm_flag_mql
                                          group by 2,3,4,5
--SELECT * FROM mart_cust_journey.cj_opportunity_facts cj WHERE opportunity_id = 39375278881
SELECT count(distinct open_deal_id),
       country,
       mql_pql_flag
        FROM test.noa_marketing_kpis_lg WHERE DATE_TRUNC('month', mql_deal_at) = '2025-06-01' OR DATE_TRUNC('month', pql_deal_at) = '2025-06-01'
group by 2,3


SELECT *,lg_cm_flag_mql,lg_cm_flag,lg_cm_trial_flag,lg_cm_flag_combined
       FROM test.noa_marketing_kpis_cm_lg WHERE (DATE_TRUNC('month', mql_deal_at) = '2025-06-01' OR DATE_TRUNC('month', pql_deal_at) = '2025-06-01')
    AND mql_excluded IS FALSE AND lg_cm_flag_combined != lg_cm_flag_mql
SELECT * from test.noa_marketing_mqls_pqls where hubspot_id = 791382283
SELECT * FROM test.noa_marketing_open_deals where hubspot_id = 791382283

SELECT * FROM  test.noa_marketing_kpis_cm_lg  where hubspot_id = 791382283
--SELECT * FROM test.noa_marketing_kpis_lg limit 10 WHERE mql_pql_flag = 'MQL' noa_marketing_kpis_lg.noa_mql_at IS NOT NULL limit 100 WHERE hubspot_id = 57784292
