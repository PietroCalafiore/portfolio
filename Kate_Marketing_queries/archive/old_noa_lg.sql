--noa_marketing_kpis_lg
DROP TABLE IF EXISTS mart_cust_journey.noa_marketing_kpis_lg;

CREATE TABLE mart_cust_journey.noa_marketing_kpis_lg AS
WITH won_noa_deals_ls AS ( --selecting Closed WON Noa deals belonging to Inbound, excluding trials
    SELECT * FROM mart_cust_journey.noa_inbound_deals
    WHERE lg_cm_flag = 'LG'
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
        hs.member_customers_list AS was_customer_at_noa_interaction,
        ROW_NUMBER() OVER (PARTITION BY hs.hubspot_id, DATE_TRUNC('month', hs.noa_notes_saas_last_activation_at_batch) ORDER BY hs.start_date ASC) AS row_per_trial
    FROM dw.hs_contact_live_history hs
    WHERE hs.noa_notes_saas_last_activation_at_batch IS NOT NULL
        AND (hs.affiliate_source NOT IN ('noa_cs', 'noa_sales', 'noa_clinics_cs', 'noa_clinics_sales', 'noa_pms_cs', 'noa_pms_sales', 'noa_tuotempo_sales', 'noa_tuotempo_cs') OR hs.affiliate_source IS NULL)
    QUALIFY row_per_trial = 1 AND was_customer_at_noa_interaction = 'No' OR  was_customer_at_noa_interaction IS NULL
    --AND NOT (hs.merged_vids IS NOT NULL AND was_customer_at_noa_interaction = 'Yes' AND paid_products_then IN ('Self (AIA Standalone)','Self (Noa Standalone)') AND wnd.dealname LIKE '%[LG]%') AND NOT (hs.country = 'Italy' AND log.last_source_so LIKE '%Noa%' AND NOT (was_merged IS NOT NULL AND was_customer_at_noa_interaction = 'Yes' AND paid_products_then NOT LIKE '%Noa%'))
        AND hs.email NOT LIKE '%docplanner.com%' AND hs.email NOT LIKE '%shahar%' AND hs.email NOT LIKE '%test%' AND hs.email NOT LIKE '%doctoralia.com%'
        AND hs.email NOT LIKE '%staszek.marta%'
),

all_lg_stda_trials AS (--calculating valid LG  stda Noa trials, excluding CS/Sales affiliate source and customers at the time
    SELECT
        hs.noa_notes_stda_last_activation_at_batch AS trial_at,
        hs.hubspot_id,
        hs.member_customers_list AS was_customer_at_noa_interaction,
        ROW_NUMBER() OVER (PARTITION BY hs.hubspot_id, DATE_TRUNC('month', hs.noa_notes_stda_last_activation_at_batch) ORDER BY hs.start_date ASC) AS row_per_trial
    FROM dw.hs_contact_live_history hs
    WHERE hs.noa_notes_stda_last_activation_at_batch IS NOT NULL
        AND (hs.affiliate_source NOT IN ('noa_cs', 'noa_sales', 'noa_clinics_cs', 'noa_clinics_sales', 'noa_pms_cs', 'noa_pms_sales', 'noa_tuotempo_sales', 'noa_tuotempo_cs') OR hs.affiliate_source IS NULL)
    QUALIFY row_per_trial = 1 AND was_customer_at_noa_interaction = 'No' OR  was_customer_at_noa_interaction IS NULL
    --AND NOT (hs.merged_vids IS NOT NULL AND was_customer_at_noa_interaction = 'Yes' AND paid_products_then IN ('Self (AIA Standalone)','Self (Noa Standalone)') AND wnd.dealname LIKE '%[LG]%') AND NOT (hs.country = 'Italy' AND log.last_source_so LIKE '%Noa%' AND NOT (hs.merged_vids IS NOT NULL AND was_customer_at_noa_interaction = 'Yes' AND paid_products_then NOT LIKE '%Noa%'))
        AND hs.email NOT LIKE '%docplanner.com%' AND hs.email NOT LIKE '%shahar%' AND hs.email NOT LIKE '%test%' AND hs.email NOT LIKE '%doctoralia.com%'
        AND hs.email NOT LIKE '%staszek.marta%'
),

all_lg_trials AS ( --unifying saas and stda trials
    SELECT
        *,
        'saas_trial' AS trial_flag
    FROM all_lg_saas_trials
    UNION DISTINCT
    SELECT
        *,
        'stda_trial' AS trial_flag
    FROM all_lg_stda_trials
),

all_trials_deduped AS ( --counting only one trial per month per contact  (either saas or stda, whichever happened first)
    SELECT
        t.hubspot_id,
        t.trial_at,
        t.trial_flag,
        ROW_NUMBER() OVER (PARTITION BY t.hubspot_id, DATE_TRUNC('month', t.trial_at) ORDER BY t.trial_at ASC) AS row_per_trial_per_month,
        ROW_NUMBER() OVER (PARTITION BY t.hubspot_id ORDER BY t.trial_at ASC) AS number_of_trials_per_contact
    FROM all_lg_trials t
    LEFT JOIN trials_to_exclude tte ON t.hubspot_id = tte.hs_contact_id --excluding previously calculated trials belonging to Sales
    WHERE 1 = 1
    QUALIFY row_per_trial_per_month = 1 AND tte.hs_contact_id IS NULL
)

SELECT --joining together all KPIs (Trials, MQLS, PQLs, WONs and fields for Noa Inbound dashboard)
    hs.hubspot_id,
    COALESCE(hs.country, hs1.country) AS country, --workaround as in some Noa cases in Italy country property is updated late
    COALESCE(hs.country, hs1.country) AS country_for_filter,
    hs.email,
    hs.affiliate_source,
    hs.sub_brand_wf_test,
    wnd.noa_deal_last_source AS frozen_last_source,
    wnd.paid_products_before_noa AS paid_products_frozen,
    hs.mql_last_touch_channel_wf,
    hs.mql_last_conversion_place_wf,
    hs.contact_type_segment_test,
    CASE WHEN COALESCE(hs.contact_type_segment_test, 'UNKNOWN') IN ('GENERAL PRACTITIONER', 'GENERAL PRACTITIONER & DOCTOR')
        THEN 'GP' ELSE 'DOCTOR' END AS target,
    --trial KPIs
    atd.trial_at AS free_signup_at,
    atd.trial_flag AS trial_flag,
    hs.noa_trial_source_test,
    --mql/pql kpis
    CASE WHEN (hs.product_qualified_noa_at IS NULL OR DATE_TRUNC('month', hs.noa_lead_at) != DATE_TRUNC('month', hs.product_qualified_noa_at)) THEN hs.noa_lead_at END AS noa_mql_at,
    hs.product_qualified_noa_at AS noa_pql_at,
    --WON deal KPIs
    hs.noa_active_products_batch,
    wnd.deal_name,
    wnd.deal_id,
    wnd.deal_month AS deal_month,
    wnd.mrr_euro AS mrr_euro,
    wnd.mrr_original_currency AS mrr_original_currency,
    wnd.deal_closed_at AS deal_close_date,
    wnd.deal_opened_at AS deal_create_date,
    wnd.budget_category,
    wnd.bundle_flag,
    wnd.demo_watched_at,
    wnd.noa_deal_last_source,
    wnd.deal_product,
    wnd.deal_category,
    hs1.facility_products_paid_batch AS paid_products_now,
    hs.facility_products_paid_batch AS paid_products_then,
    hs1.member_customers_list AS is_current_customer,
    log.last_source_so AS log_last_source,
    hs.noa_last_source AS hs_noa_last_source,
    hs.member_customers_list AS was_noa_customer,
    hs.merged_vids AS was_merged,
    wnd.paid_products_before_noa,
    wnd.info_tag,
    wnd.customer_type_all,
    DATE_DIFF('day', hs.noa_lead_at::DATE, wnd.deal_closed_at::DATE) AS mql_to_won,
    DATE_DIFF('day', hs.product_qualified_noa_at::DATE, wnd.deal_closed_at::DATE) AS pql_to_won,
    CASE WHEN hs.noa_lead_at IS NOT NULL OR hs.noa_notes_stda_last_activation_at_batch IS NOT NULL OR hs.product_qualified_noa_at IS NOT NULL OR wnd.deal_month IS NOT NULL
        THEN ROW_NUMBER() OVER (PARTITION BY hs.hubspot_id, DATE_TRUNC('month', hs.noa_lead_at), DATE_TRUNC('month', hs.noa_notes_stda_last_activation_at_batch), DATE_TRUNC('month', hs.product_qualified_noa_at), wnd.deal_closed_at ORDER BY hs.start_date ASC)
    END AS row_per_lcs
FROM dw.hs_contact_live_history hs
LEFT JOIN dw.hs_contact_live hs1 ON hs.hubspot_id = hs1.hubspot_id
LEFT JOIN dv_raw.sat_contact_hs_1h_log log ON hs.hubspot_id = log.hubspot_id AND DATE_TRUNC('month', hs.noa_lead_at) = DATE_TRUNC('month', log.updated_at) AND LOWER(log.last_source_so) LIKE '%noa%'
LEFT JOIN won_noa_deals_ls wnd ON wnd.hubspot_id = hs.hubspot_id
LEFT JOIN all_trials_deduped atd ON atd.hubspot_id = hs.hubspot_id
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
    wnd.bundle_flag,
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
    hs1.facility_products_paid_batch,
    hs.facility_products_paid_batch,
    hs1.member_customers_list,
    log.last_source_so,
    hs.noa_last_source,
    wnd.customer_type_all,
    hs.member_customers_list,
    hs.product_qualified_noa_at,
    hs.merged_vids,
    hs.noa_lead_at,
    hs.noa_notes_stda_last_activation_at_batch,
    wnd.deal_month,
    hs.start_date,
    hs.noa_trial_source_test,
    wnd.budget_category,
    wnd.info_tag,
    wnd.hubspot_id,
    wnd.was_merged,
    hs1.country
QUALIFY row_per_lcs = 1
AND (was_noa_customer IS NULL OR was_noa_customer = 'No' OR wnd.hubspot_id IS NOT NULL --excluding all CM contacts, WONs are already divided into LG/CM in noa_inbound_deals table
    --OR (was_customer = 'Yes' AND info_tag LIKE '%Bundle Noa%')
    --OR (was_merged IS NOT NULL AND was_customer = 'Yes' AND paid_products_then IN ('Self (AIA Standalone)','Self (Noa Standalone)', 'Self (Noa Saas)')  AND wnd.deal_name LIKE '%[LG]%') -- condition covers issues caused by merges where a contact appears as CM but is actually not
OR (hs.country = 'Italy' AND log.last_source_so LIKE '%Noa%' AND NOT (was_merged IS NOT NULL AND was_noa_customer = 'Yes' AND paid_products_then NOT LIKE '%Noa%'))) --condition to include Italy Bundle leads while keeping excluded fake LG leads (that were actually CM merges)
 AND hs.email NOT LIKE '%docplanner.com%' AND hs.email NOT LIKE '%shahar%' AND hs.email NOT LIKE '%test%' AND hs.email NOT LIKE '%miodottore%'
    AND hs.email NOT LIKE '%staszek.marta%' AND country_for_filter IN ('Brazil', 'Mexico', 'Spain', 'Italy', 'Germany', 'Colombia', 'Chile', 'Turkiye', 'Peru', 'Poland')
