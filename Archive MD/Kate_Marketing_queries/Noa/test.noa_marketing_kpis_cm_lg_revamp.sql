DROP TABLE IF EXISTS test.noa_marketing_kpis_cm_lg_revamp;

CREATE TABLE test.noa_marketing_kpis_cm_lg_revamp AS
WITH won_noa_deals_ls AS ( --selecting Closed WON Noa deals belonging to Inbound, excluding trials
    SELECT * FROM mart_cust_journey.noa_inbound_deals
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
        hs.country,
        hs.email,
        hs.noa_trial_source_test AS noa_trial_source,
        hs.contact_type_segment_test AS contact_type,
        hs.sub_brand_wf_test,
        hs.affiliate_source,
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
        hs.country,
        hs.email,
        hs.noa_trial_source_test AS noa_trial_source,
        hs.contact_type_segment_test AS contact_type,
        hs.sub_brand_wf_test,
        hs.affiliate_source,
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
        t.hubspot_id::VARCHAR,
        t.country,
        t.email,
        t.trial_at,
        t.trial_flag,
        t.noa_trial_source,
        t.lg_cm_trial_flag,
        t.contact_type,
        t.sub_brand_wf_test,
        t.affiliate_source,
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
    ),

mqls_trials_join AS (
    SELECT
        COALESCE(nod.country, atd.country) AS country,
        nod.deal_id AS open_deal_id,
        COALESCE(nod.hubspot_id, atd.hubspot_id) AS hubspot_id,
        COALESCE(nod.email, atd.email) AS email,
        atd.trial_at,
        atd.trial_flag,
        atd.noa_trial_source,
        atd.sub_brand_wf_test,
        atd.lg_cm_trial_flag,
        atd.affiliate_source,
        nod.budget_category,
        nod.mql_pql_flag,
        nod.pql_mql_category,--if edge case, assume STDA
        nod.mql_at,
        nod.pql_at,
        nod.create_date,
        nod.create_month,
        nod.noa_last_source,
        nod.lg_cm_flag_mql,
        COALESCE(nod.segment, 'Individual') AS segment,
        COALESCE(nod.contact_type, atd.contact_type) AS contact_type,
        nod.mql_last_touch_channel,
        nod.mql_last_conversion_place,
        nod.mql_excluded,
        0 AS days_between_mql_and_create_deal_date
    FROM all_trials_deduped atd
    FULL OUTER JOIN noa_open_deals nod ON nod.hubspot_id = atd.hubspot_id
    WHERE 1 = 1
)

SELECT --joining together all KPIs (Trials, MQLS, PQLs, WONs and fields for Noa Inbound dashboard)
    hs.hubspot_id,
    COALESCE(hs.country, hs1.country) AS country, --workaround as in some Noa cases in Italy country property is updated late
    COALESCE(hs.country, hs1.country) AS country_for_filter,
    COALESCE(wnd.lg_cm_flag, hs.lg_cm_flag_mql, hs.lg_cm_trial_flag) AS lg_cm_flag_combined,
    hs.email,
    hs.affiliate_source,
    hs.sub_brand_wf_test,
    wnd.noa_deal_last_source AS frozen_last_source,
    wnd.paid_products_before_noa AS paid_products_frozen,
    hs.mql_last_touch_channel,
    hs.mql_last_conversion_place,
    hs.contact_type,
    CASE WHEN COALESCE(hs.contact_type, 'UNKNOWN') IN ('GENERAL PRACTITIONER', 'GENERAL PRACTITIONER & DOCTOR')
        THEN 'GP' ELSE 'DOCTOR' END AS target,
    --trial KPIs
    hs.trial_at AS free_signup_at,
    hs.trial_flag AS trial_flag,
    hs.noa_trial_source,
    hs.lg_cm_trial_flag,
    --mql/pql kpis
    CASE WHEN hs.mql_pql_flag = 'MQL' THEN hs.create_date END AS mql_deal_at,
    CASE WHEN hs.mql_pql_flag = 'PQL' THEN hs.create_date  END AS pql_deal_at,
    CASE WHEN hs.mql_pql_flag = 'MQL' THEN hs.mql_at END AS noa_mql_at,
    CASE WHEN hs.mql_pql_flag = 'PQL' THEN hs.pql_at END AS noa_pql_at,
    hs.lg_cm_flag_mql,
    hs.mql_pql_flag,
    hs.segment,
    hs.mql_excluded,
    hs.open_deal_id AS open_deal_id,
    --WON deal KPIs
    wnd.deal_name,
    wnd.lg_cm_flag,
    wnd.deal_id,
    wnd.deal_month AS deal_month,
    wnd.mrr_euro AS mrr_euro,
    wnd.mrr_original_currency AS mrr_original_currency,
    wnd.deal_closed_at AS deal_close_date,
    wnd.deal_opened_at AS deal_create_date,
    COALESCE(hs.budget_category, wnd.budget_category) AS budget_category,
    wnd.bundle_flag,
    wnd.demo_watched_at,
    wnd.noa_deal_last_source,
    wnd.deal_product,
    COALESCE(wnd.deal_category, hs.pql_mql_category) AS deal_category,
    hs1.member_customers_list AS is_current_customer,
    hs.noa_last_source AS hs_noa_last_source,
    wnd.paid_products_before_noa,
    wnd.info_tag,
    wnd.customer_type_all,
    hs.days_between_mql_and_create_deal_date,
    DATE_DIFF('day', hs.pql_at::DATE, wnd.deal_closed_at::DATE) AS mql_to_won,
    DATE_DIFF('day', hs.mql_at::DATE, wnd.deal_closed_at::DATE) AS pql_to_won
FROM mqls_trials_join hs
LEFT JOIN dw.hs_contact_live hs1 ON hs.hubspot_id = hs1.hubspot_id
LEFT JOIN won_noa_deals_ls wnd ON wnd.deal_id = hs.open_deal_id
WHERE hs1.is_deleted IS FALSE
GROUP BY
    hs.hubspot_id,
    hs.country,
    hs.email,
    hs.affiliate_source,
    hs.sub_brand_wf_test,
    wnd.deal_closed_at,
    hs.mql_last_touch_channel,
    hs.mql_last_conversion_place,
    wnd.paid_products_before_noa,
    hs.trial_at,
    hs.lg_cm_trial_flag,
    wnd.bundle_flag,
    wnd.lg_cm_flag,
    hs.trial_flag,
    noa_mql_at,
    noa_pql_at,
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
    hs.noa_last_source,
    wnd.customer_type_all,
    wnd.deal_month,
    wnd.budget_category,
    wnd.info_tag,
    wnd.hubspot_id,
    wnd.was_merged,
    hs1.country,
    hs.noa_trial_source,
    hs.lg_cm_flag_mql,
    hs.segment,
    hs.open_deal_id,
    hs.mql_last_touch_channel,
    hs.mql_last_conversion_place,
    hs.mql_pql_flag,
    hs.segment,
    hs.mql_excluded,
    hs.create_date,
    hs.pql_at,
    hs.mql_at,
    hs.hubspot_id,
    hs.contact_type,
    hs.mql_pql_flag,
    hs.budget_category,
    hs.pql_mql_category,
    hs.days_between_mql_and_create_deal_date
QUALIFY country_for_filter IN ('Brazil', 'Mexico', 'Spain', 'Italy', 'Germany', 'Colombia', 'Chile', 'Turkiye', 'Peru', 'Poland')
