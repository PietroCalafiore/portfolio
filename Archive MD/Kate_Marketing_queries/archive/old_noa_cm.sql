--noa_marketing_kpis_cm
DROP TABLE IF EXISTS mart_cust_journey.noa_marketing_kpis_cm;

CREATE TABLE mart_cust_journey.noa_marketing_kpis_cm AS
WITH
all_sales_trials AS ( --we dont have a trial set date property so we need to calculate manually the date the type of negotiation was set to trial
    SELECT --we didnt have detailed log data for Noa trial property before March 15th.Using HS deal live for trials before then
        hubspot_id AS deal_id,
        noa_notes_trial_yes_no,
        dw_updated_at AS trial_yes_no_set_date,
        LAG(noa_notes_trial_yes_no) OVER (PARTITION BY hubspot_id ORDER BY dw_updated_at) AS lag
    FROM dw.hs_deal_live_history WHERE noa_notes_trial_yes_no = 'Yes' AND createdate >= '2024-12-01' AND createdate < '2025-03-12'
    GROUP BY hubspot_id, noa_notes_trial_yes_no, dw_updated_at
    QUALIFY lag IS NULL
    UNION DISTINCT
    SELECT
        hubspot_id AS deal_id,
        noa_notes_trial_yes_no,
        updated_at AS trial_yes_no_set_date,
        LAG(noa_notes_trial_yes_no) OVER (PARTITION BY hubspot_id ORDER BY updated_at) AS lag
    FROM dv_raw.sat_deal_hs_1h_log WHERE noa_notes_trial_yes_no = 'Yes' AND createdate >= '2025-03-12'
    GROUP BY hubspot_id, noa_notes_trial_yes_no, updated_at
    QUALIFY lag IS NULL
),

noa_open_deals AS ( --This identifies if a trial belongs to sales based on specific conditions, including trial dates compared to activation dates
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
        hs1.noa_notes_saas_last_activation_at_batch, --pulling from live table as waiting to be added to historical one
        tsd.noa_notes_trial_yes_no,
        tsd.trial_yes_no_set_date
    FROM all_sales_trials tsd
    LEFT JOIN mart_cust_journey.cj_deal_month deal ON deal.deal_id = tsd.deal_id
    LEFT JOIN dw.hs_deal_live hs ON deal.deal_id = hs.hubspot_id
    LEFT JOIN dw.hs_contact_live_history hs1 ON deal.hs_contact_id = hs1.hubspot_id AND (hs1.noa_notes_stda_last_activation_at_batch IS NOT NULL OR hs1.noa_notes_saas_last_activation_at_batch IS NOT NULL)
    WHERE deal.createdate >= '2024-12-01' --AND deal.deal_id = 31931970675
        AND (DATE_TRUNC('day', tsd.trial_yes_no_set_date) <= hs1.noa_notes_stda_last_activation_at_batch OR DATE_TRUNC('day', tsd.trial_yes_no_set_date) <= hs1.noa_notes_saas_last_activation_at_batch)
        AND deal.pipeline IN ('Noa', 'Noa Notes') AND deal.deal_stage IN ('Contract Signed', 'Decision Maker Reached', 'Contacting', 'Closed Won', 'Demo / Sales Meeting Done')
    QUALIFY row_per_deal = 1
),

closed_deal_product_check AS ( --workaround to check product when Products paid value is null --this is to identify Individuals CM customers from Clinics
    SELECT
        hs_contact_id,
        deal_id,
        pipeline_type,
        month,
        country,
        deal_length,
        closedate::DATE AS closedate
    FROM mart_cust_journey.cj_deal_month
    WHERE pipeline_type IN ('Individual New Sales', 'Clinics New Sales')
        AND deal_stage = 'Closed Won' AND is_current_stage AND stage_is_month_new AND month >= '2023-06-01' AND deal_stage_end IS NULL
    GROUP BY
        hs_contact_id,
        deal_id,
        pipeline_type,
        month,
        country,
        deal_length,
        closedate::DATE
),

won_deals_categorized AS ( --selecting all CM Noa deals
    SELECT * FROM mart_cust_journey.noa_inbound_deals
    WHERE lg_cm_flag = 'CM'
),

all_cm_saas_trials AS ( -- CM saas trials
    SELECT
        hs.noa_notes_saas_last_activation_at_batch AS trial_at,
        hs.hubspot_id,
        hs.member_customers_list AS was_customer_at_noa_interaction,
        hs.facility_products_paid_batch AS paid_products_then,
        ROW_NUMBER() OVER (PARTITION BY hs.hubspot_id, DATE_TRUNC('month', hs.noa_notes_saas_last_activation_at_batch) ORDER BY hs.dw_updated_at ASC) AS row_per_trial
    FROM dw.hs_contact_live_history hs
    WHERE hs.noa_notes_saas_last_activation_at_batch IS NOT NULL
        AND (affiliate_source NOT IN ('noa_cs', 'noa_sales', 'noa_clinics_cs', 'noa_clinics_sales', 'noa_pms_cs', 'noa_pms_sales', 'noa_tuotempo_sales', 'noa_tuotempo_cs') OR affiliate_source IS NULL)
    QUALIFY row_per_trial = 1 AND was_customer_at_noa_interaction = 'Yes' AND hs.commercial_from_last_date_at < trial_at
    AND hs.email NOT LIKE '%docplanner.com%' AND hs.email NOT LIKE '%shahar%' AND hs.email NOT LIKE '%test%' AND hs.email NOT LIKE '%doctoralia.com%'
        AND hs.email NOT LIKE '%staszek.marta%'
),

all_cm_stda_trials AS ( -- CM stda trials
    SELECT
        hs.noa_notes_stda_last_activation_at_batch AS trial_at,
        hs.hubspot_id,
        hs.member_customers_list AS was_customer_at_noa_interaction,
        hs.facility_products_paid_batch AS paid_products_then,
        ROW_NUMBER() OVER (PARTITION BY hs.hubspot_id, DATE_TRUNC('month', hs.noa_notes_stda_last_activation_at_batch) ORDER BY hs.dw_updated_at ASC) AS row_per_trial
    FROM dw.hs_contact_live_history hs
    WHERE hs.noa_notes_stda_last_activation_at_batch IS NOT NULL --AND hs.hubspot_id = 2120091
        AND (hs.affiliate_source NOT IN ('noa_cs', 'noa_sales', 'noa_clinics_cs', 'noa_clinics_sales', 'noa_pms_cs', 'noa_pms_sales', 'noa_tuotempo_sales', 'noa_tuotempo_cs') OR hs.affiliate_source IS NULL)
    QUALIFY row_per_trial = 1 AND was_customer_at_noa_interaction = 'Yes'
    AND hs.commercial_from_last_date_at < trial_at
    AND hs.email NOT LIKE '%docplanner.com%' AND hs.email NOT LIKE '%shahar%' AND hs.email NOT LIKE '%test%' AND hs.email NOT LIKE '%doctoralia.com%'
        AND hs.email NOT LIKE '%staszek.marta%'
),

all_cm_trials AS ( -- combining saas and stda trials
    SELECT
        *,
        'saas_trial' AS trial_flag
    FROM all_cm_saas_trials
    UNION DISTINCT
    SELECT
        *,
        'stda_trial' AS trial_flag
    FROM all_cm_stda_trials
),

all_trials_deduped AS ( -- removes duplicates from previous queries (both types of trials), only counting one trial per month per contact
    SELECT
        act.hubspot_id,
        act.trial_at,
        act.trial_flag,
        act.paid_products_then AS paid_products_at_trial,
        nod.hs_contact_id,
        ROW_NUMBER() OVER (PARTITION BY act.hubspot_id, DATE_TRUNC('month', act.trial_at) ORDER BY act.trial_at ASC) AS row_per_trial_per_month,
        ROW_NUMBER() OVER (PARTITION BY act.hubspot_id ORDER BY act.trial_at ASC) AS number_of_trials_per_contact
    FROM all_cm_trials act
    LEFT JOIN noa_open_deals nod ON act.hubspot_id = nod.hs_contact_id
    WHERE 1 = 1
    QUALIFY row_per_trial_per_month = 1 AND (nod.hs_contact_id IS NULL)

),

joined_data AS ( -- joining all metrics
    SELECT
        hs.hubspot_id,
        hs.country,
        hs.email,
        hs.affiliate_source,
        hs.sub_brand_wf_test,
        COALESCE(hs.noa_last_source, wnd.noa_deal_last_source, log.last_source_so) AS noa_contact_last_source,
        CASE WHEN COALESCE(hs.contact_type_segment_test, 'UNKNOWN') IN ('GENERAL PRACTITIONER', 'GENERAL PRACTITIONER & DOCTOR') THEN 'GP' ELSE 'DOCTOR' END AS target,
        hs1.facility_products_paid_batch AS paid_products_now,
        hs.facility_products_paid_batch AS paid_products_then,
        CASE WHEN hs.facility_products_paid_batch ILIKE '%Paid by another doctor (Premium jameda platin)%'
            OR hs.facility_products_paid_batch ILIKE '%Self (Premium plus)%'
            OR hs.facility_products_paid_batch ILIKE '%Paid by another doctor (Premium jameda gold)%'
            OR hs.facility_products_paid_batch ILIKE '%Self (Premium jameda platin)%'
            OR hs.facility_products_paid_batch ILIKE '%Self (Premium promo)%'
            OR hs.facility_products_paid_batch ILIKE '%Self (Premium jameda gold)%'
            OR hs.facility_products_paid_batch ILIKE '%Paid by another doctor (Premium starter)%'
            OR hs.facility_products_paid_batch ILIKE '%Paid by another doctor (Premium promo)%'
            OR hs.facility_products_paid_batch ILIKE '%Paid by another doctor (Premium plus)%'
            OR hs.facility_products_paid_batch ILIKE '%Self (Premium vip)%'
            OR hs.facility_products_paid_batch ILIKE '%Paid by another doctor (Premium jameda gold-pro)%'
            OR hs.facility_products_paid_batch ILIKE '%Paid by another doctor (Premium vip)%'
            OR hs.facility_products_paid_batch ILIKE '%Self (Premium jameda gold-pro)%'
            OR hs.facility_products_paid_batch ILIKE '%Paid by another doctor (Premium ind)%'
            OR hs.facility_products_paid_batch ILIKE '%Self (Premium starter)%'
            OR hs.facility_products_paid_batch ILIKE '%Self (Premium ind)%'
            OR (hs.facility_products_paid_batch IS NULL AND (cdpc.pipeline_type = 'Individual New Sales' AND wnd.customer_type_all = 'Individual')) --workaround to catch cases where Paid products property is buggy due to Hays profiles overlap
            OR hs.country = 'Germany' THEN 'Individual' --ok Marta, everything in Germany is DOC despite what budget category says
            WHEN wnd.budget_category LIKE '%PMS%' OR hs.facility_products_paid_batch LIKE '%MyDr%' OR hs.facility_products_paid_batch LIKE '%Feegow%' OR hs.facility_products_paid_batch LIKE '%Clinic Cloud%' THEN 'PMS'
            WHEN wnd.budget_category LIKE '%Clinic%' OR LOWER(hs.facility_products_paid_batch) LIKE '%facility%' THEN 'Clinics'
            WHEN wnd.budget_category IN ('Noa Individual', 'Noa Ind Expansion') THEN 'Individual'
            ELSE 'Clinics'
        END AS customer_type_all, --logic to indentify only doctors with a previous Individual doctor
        cdpc.pipeline_type,
        cdpc.deal_id AS non_noa_deal_id,
        log.last_source_so AS log_last_source,
        hs.member_customers_list AS was_customer_at_noa_interaction,
        hs1.member_customers_list AS is_customer_now,
        hs.merged_vids AS was_merged,
        COALESCE(hs.commercial_from_last_date_at, hs1.commercial_from_last_date_at) AS commercial_from_last_date_at,
        --trial metrics
        atd.trial_at AS trial_at,
        atd.trial_flag AS trial_flag,
        atd.paid_products_at_trial,
        --mql_pql_metrics
        CASE WHEN (hs.product_qualified_noa_at IS NULL OR DATE_TRUNC('month', hs.noa_lead_at) != DATE_TRUNC('month', hs.product_qualified_noa_at)) AND COALESCE(hs.commercial_from_last_date_at, hs1.commercial_from_last_date_at) < hs.noa_lead_at THEN hs.noa_lead_at END AS noa_mql_at,
        CASE WHEN COALESCE(hs.commercial_from_last_date_at, hs1.commercial_from_last_date_at) < COALESCE(DATE_TRUNC('month', hs.product_qualified_noa_at), DATE_TRUNC('month', hs1.product_qualified_noa_at)) THEN COALESCE(hs.product_qualified_noa_at, hs1.product_qualified_noa_at) END AS noa_pql_at,
        CASE WHEN (hs1.noa_notes_stda_last_activation_at_batch IS NOT NULL) AND COALESCE(hs.commercial_from_last_date_at, hs1.commercial_from_last_date_at) < COALESCE(DATE_TRUNC('month', hs.product_qualified_noa_at), DATE_TRUNC('month', hs1.product_qualified_noa_at)) THEN 'stda' ELSE 'saas' END AS pql_category,
        --won_metrics
        wnd.deal_month AS deal_month,
        wnd.deal_id,
        wnd.deal_category,
        wnd.deal_product,
        wnd.bundle_flag,
        wnd.info_tag,
        wnd.noa_deal_last_source,
        wnd.budget_category,
        wnd.customer_type_all AS customer_type_deal,
        hs.contact_type_segment_test AS segment,
        CASE WHEN wnd.deal_month IS NOT NULL THEN wnd.mrr_euro END AS mrr_euro,
        CASE WHEN hs.noa_lead_at IS NOT NULL OR hs.noa_notes_stda_last_activation_at_batch IS NOT NULL OR hs.product_qualified_noa_at IS NOT NULL OR hs.noa_notes_saas_last_activation_at_batch IS NOT NULL OR atd.trial_at IS NOT NULL OR wnd.deal_month IS NOT NULL
            THEN ROW_NUMBER() OVER (PARTITION BY hs.hubspot_id, DATE_TRUNC('month', hs.noa_lead_at), DATE_TRUNC('month', atd.trial_at), DATE_TRUNC('month', hs.product_qualified_noa_at), DATE_TRUNC('month', wnd.deal_month) ORDER BY hs.dw_updated_at ASC)
        END AS row_per_lcs
    FROM dw.hs_contact_live_history hs
    LEFT JOIN dw.hs_contact_live hs1 ON hs.hubspot_id = hs1.hubspot_id
    LEFT JOIN all_trials_deduped atd ON atd.hubspot_id = hs.hubspot_id
    LEFT JOIN dv_raw.sat_contact_hs_1h_log log ON hs.hubspot_id = log.hubspot_id AND DATE_TRUNC('month', hs.noa_lead_at) = DATE_TRUNC('month', log.updated_at) AND LOWER(log.last_source_so) LIKE '%noa%'
    LEFT JOIN won_deals_categorized wnd ON wnd.hubspot_id = hs.hubspot_id
    LEFT JOIN closed_deal_product_check cdpc ON cdpc.hs_contact_id = hs.hubspot_id AND (DATE_TRUNC('month', hs1.noa_notes_stda_last_activation_at_batch) >= cdpc.month OR DATE_TRUNC('month', hs1.noa_notes_saas_last_activation_at_batch) >= cdpc.month OR DATE_TRUNC('month', hs.noa_lead_at) >= cdpc.month)
    WHERE (hs.noa_lead_at IS NOT NULL OR hs.noa_notes_stda_last_activation_at_batch IS NOT NULL OR hs.noa_notes_saas_last_activation_at_batch IS NOT NULL OR hs.product_qualified_noa_at IS NOT NULL OR wnd.deal_month IS NOT NULL OR (hs1.noa_notes_saas_last_activation_at_batch IS NOT NULL AND hs.dw_updated_at >= hs1.noa_notes_saas_last_activation_at_batch))
        AND hs.dw_updated_at >= '2024-08-01' AND hs1.is_deleted IS FALSE-- AND hs.hubspot_id = 2119781
    GROUP BY
        hs.hubspot_id,
        hs.country,
        hs.email,
        hs.affiliate_source,
        hs.sub_brand_wf_test,
        noa_contact_last_source,
        target,
        paid_products_now,
        paid_products_then,
        cdpc.pipeline_type,
        wnd.country,
        wnd.budget_category,
        non_noa_deal_id,
        log_last_source,
        was_customer_at_noa_interaction,
        is_customer_now,
        was_merged,
        COALESCE(hs.commercial_from_last_date_at, hs1.commercial_from_last_date_at),
        trial_at,
        trial_flag,
        atd.paid_products_at_trial,
        noa_mql_at,
        noa_pql_at,
        pql_category,
        deal_month,
        wnd.deal_id,
        wnd.deal_category,
        wnd.deal_product,
        wnd.bundle_flag,
        wnd.info_tag,
        wnd.noa_deal_last_source,
        wnd.customer_type_all,
        wnd.budget_category,
        segment,
        mrr_euro,
        hs.noa_lead_at,
        hs.merged_vids,
        hs.noa_notes_stda_last_activation_at_batch,
        hs.product_qualified_noa_at,
        hs1.noa_notes_saas_last_activation_at_batch,
        hs.noa_notes_saas_last_activation_at_batch,
        atd.trial_at,
        hs.dw_updated_at,
        wnd.deal_name,
        customer_type_all
    QUALIFY row_per_lcs = 1
    AND (wnd.deal_month IS NOT NULL OR (was_customer_at_noa_interaction = 'Yes' AND NOT
    (was_merged IS NOT NULL AND was_customer_at_noa_interaction = 'Yes' AND paid_products_then IN ('Self (AIA Standalone)', 'Self (Noa Standalone)') AND wnd.deal_name LIKE '%[LG]%')))
    --AND NOT (hs.country = 'Italy' AND log.last_source_so LIKE '%Noa%' AND NOT (was_merged IS NOT NULL AND was_customer_at_noa_interaction = 'Yes' AND NOT paid_products_then LIKE '%Noa%'))
    --AND NOT (was_merged IS NOT NULL AND hs.country = 'Italy' AND log.last_source_so LIKE '%Noa%' AND NOT (was_customer_at_noa_interaction = 'Yes' AND NOT paid_products_then LIKE '%Noa%'))
     AND hs.email NOT LIKE '%docplanner.com%' AND hs.email NOT LIKE '%shahar%' AND hs.email NOT LIKE '%test%' AND hs.email NOT LIKE '%doctoralia.com%' AND hs.email NOT LIKE '%miodottore%'
            AND hs.email NOT LIKE '%staszek.marta%' AND hs.country IN ('Brazil', 'Mexico', 'Spain', 'Italy', 'Germany', 'Colombia', 'Chile', 'Turkiye', 'Peru', 'Poland')
)

-- final select statement
SELECT
    hubspot_id,
    country,
    email,
    affiliate_source,
    sub_brand_wf_test,
    noa_contact_last_source,
    target,
    paid_products_now,
    paid_products_then,
    COALESCE(paid_products_then IN ('Self (Noa Saas)', 'Self (Noa Standalone)', 'Paid by another doctor (Noa Standalone)', 'Self (AIA Saas)', 'Self (AIA Standalone)'), FALSE) AS was_pure_noa,
    pipeline_type,
    non_noa_deal_id,
    log_last_source,
    was_customer_at_noa_interaction,
    is_customer_now,
    was_merged,
    commercial_from_last_date_at,
    trial_at,
    trial_flag,
    paid_products_at_trial,
    noa_mql_at,
    noa_pql_at,
    pql_category,
    deal_month,
    deal_id,
    deal_category,
    deal_product,
    bundle_flag,
    info_tag,
    noa_deal_last_source,
    segment,
    mrr_euro,
    budget_category,
    customer_type_deal,
    customer_type_all
FROM joined_data
GROUP BY
    hubspot_id,
    country,
    email,
    affiliate_source,
    sub_brand_wf_test,
    noa_contact_last_source,
    target,
    paid_products_now,
    paid_products_then,
    pipeline_type,
    non_noa_deal_id,
    log_last_source,
    was_customer_at_noa_interaction,
    is_customer_now,
    was_merged,
    commercial_from_last_date_at,
    trial_at,
    trial_flag,
    paid_products_at_trial,
    noa_mql_at,
    noa_pql_at,
    pql_category,
    deal_month,
    deal_id,
    deal_category,
    deal_product,
    bundle_flag,
    info_tag,
    noa_deal_last_source,
    segment,
    mrr_euro,
    budget_category,
    customer_type_deal,
    customer_type_all
