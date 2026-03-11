WITH won_noa_deals AS (
    SELECT
        cj.hs_contact_id,
        cj.deal_id,
        cj.country,
        cj.month AS deal_month,
        hsd.noa_notes_trial_yes_no,
        cj.mrr_euro,
        cj.mrr_original_currency,
        cj.dealname,
        hsd.last_source_so AS last_source_of_demo,
        cj.closedate
    FROM mart_cust_journey.cj_deal_month cj
    LEFT JOIN dw.hs_deal_live hsd
        ON cj.deal_id = hsd.hubspot_id
    WHERE cj.pipeline = 'Noa' AND cj.pipeline_type = 'Noa'
        AND cj.is_current_stage AND cj.stage_is_month_new AND cj.deal_stage = 'Closed Won'
        AND (hsd.noa_notes_trial_yes_no != 'Yes' OR hsd.noa_notes_trial_yes_no IS NULL)
        AND cj.month >= '2025-01-01' AND NOT cj.dealname LIKE '%[CM]%'
),

won_noa_deals_ls AS ( --last sources is taken at a later date from historical values due to manual updates
    SELECT
        deal.*,
        hsh.last_source_so AS frozen_last_source,
        ROW_NUMBER() OVER (PARTITION BY deal.deal_id ORDER BY hsh.dw_updated_at DESC) AS row
    FROM won_noa_deals deal
    LEFT JOIN dw.hs_contact_live_history hsh ON hsh.hubspot_id = deal.hs_contact_id AND hsh.dw_updated_at BETWEEN deal.deal_month AND DATEADD(DAY, 50, DATE_TRUNC('month', deal.deal_month::DATE))
    QUALIFY row = 1
),

all_sales_trials AS (
    SELECT
        hubspot_id AS deal_id,
        noa_notes_trial_yes_no,
        updated_at AS trial_yes_no_set_date,
        LAG(noa_notes_trial_yes_no) OVER (PARTITION BY hubspot_id ORDER BY updated_at) AS lag
    FROM dv_raw.sat_deal_hs_1h_log WHERE noa_notes_trial_yes_no = 'Yes' AND createdate >= '2025-03-15'
    GROUP BY 1, 2, 3
    QUALIFY lag IS NULL
),

trials_to_exclude AS ( --This identifies if a trial belongs to sales based on specific conditions, including trial dates compared to activation dates
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
        tsd.trial_yes_no_set_date,
        deal.dealname
    FROM  all_sales_trials tsd
    LEFT JOIN mart_cust_journey.cj_deal_month deal ON deal.deal_id = tsd.deal_id
    LEFT JOIN dw.hs_deal_live hs ON deal.deal_id = hs.hubspot_id
    LEFT JOIN dw.hs_contact_live_history hs1 ON deal.hs_contact_id = hs1.hubspot_id
    WHERE deal.createdate > '2025-03-01'
        AND (tsd.trial_yes_no_set_date <= hs1.noa_notes_stda_last_activation_at_batch OR tsd.trial_yes_no_set_date <= hs1.noa_notes_saas_last_activation_at_batch)
        AND deal.pipeline = 'Noa' AND deal.deal_stage IN ('Contract Signed', 'Decision Maker Reached', 'Contacting', 'Closed Won', 'Demo / Sales Meeting Done')
    QUALIFY row_per_deal = 1
),

noa_leads AS (
    SELECT
        hs.hubspot_id,
        hs.country,
        CASE WHEN (hs.affiliate_source NOT IN ('noa_cs', 'noa_sales', 'noa_clinics_cs', 'noa_clinics_sales', 'noa_pms_cs', 'noa_pms_sales', 'noa_tuotempo_sales', 'noa_tuotempo_cs') OR hs.affiliate_source IS NULL) AND nod.hs_contact_id IS NULL THEN hs.noa_notes_stda_last_activation_at_batch END AS free_signup_at,--need to add to history
        CASE WHEN (hs.product_qualified_noa_at IS NULL OR DATE_TRUNC('month', hs.noa_lead_at) != DATE_TRUNC('month', hs.product_qualified_noa_at)) THEN hs.noa_lead_at END AS noa_mql_at,
        hs.product_qualified_noa_at AS noa_pql_at,
        wnd.deal_id AS was_won,
        wnd.mrr_euro,
        wnd.mrr_original_currency,
        CASE WHEN COALESCE(hs.contact_type_segment_test, 'UNKNOWN') IN ('GENERAL PRACTITIONER', 'GENERAL PRACTITIONER & DOCTOR')
                THEN 'GP' ELSE 'DOCTOR' END AS target,
        CASE WHEN hs.noa_lead_at IS NOT NULL OR hs.noa_notes_stda_last_activation_at_batch IS NOT NULL OR hs.product_qualified_noa_at IS NOT NULL
            THEN ROW_NUMBER() OVER (PARTITION BY hs.hubspot_id, DATE_TRUNC('month', hs.noa_lead_at), DATE_TRUNC('month', hs.noa_notes_stda_last_activation_at_batch), DATE_TRUNC('month', hs.product_qualified_noa_at), DATE_TRUNC('month', wnd.deal_month) ORDER BY hs.dw_updated_at ASC)
        END AS row_per_lcs,
        CASE WHEN wnd.last_source_of_demo LIKE '%Noa%' OR wnd.frozen_last_source LIKE '%Noa%'
            OR (DATE_DIFF('day', hs.noa_lead_at::DATE, wnd.closedate::DATE) BETWEEN -5 AND 60)
            OR (DATE_DIFF('day', hs.product_qualified_noa_at::DATE, wnd.closedate::DATE) BETWEEN -5 AND 60) THEN wnd.deal_month END AS deal_month,
        hs.facility_products_paid_batch AS paid_products_then,
        hs.merged_vids AS was_merged,
        hs.member_customers_list AS was_customer,
        hs.contact_type_segment_test AS segment,
        SUM(CASE WHEN (DATE_DIFF('day', hs.product_qualified_noa_at::DATE, wnd.closedate::DATE) BETWEEN -5 AND 60) THEN 1 ELSE 0 END) OVER (PARTITION BY wnd.deal_id) AS pql_check_test --assign whether to consider a deal from PQL or not per deal_id
    FROM dw.hs_contact_live_history hs
    LEFT JOIN dw.hs_contact_live hs1 ON hs.hubspot_id = hs1.hubspot_id
    LEFT JOIN dv_raw.sat_contact_hs_1h_log log ON hs.hubspot_id = log.hubspot_id AND DATE_TRUNC('month', hs.noa_lead_at) = DATE_TRUNC('month', log.updated_at) AND LOWER(log.last_source_so) LIKE '%noa%'
    LEFT JOIN won_noa_deals_ls wnd ON wnd.hs_contact_id = hs.hubspot_id
    LEFT JOIN trials_to_exclude nod ON nod.hs_contact_id = hs.hubspot_id AND (hs.noa_notes_stda_last_activation_at_batch = nod.noa_notes_stda_last_activation_at_batch OR hs.noa_notes_saas_last_activation_at_batch = nod.noa_notes_stda_last_activation_at_batch)
    WHERE (hs.noa_lead_at IS NOT NULL OR hs.noa_notes_stda_last_activation_at_batch IS NOT NULL OR hs.product_qualified_noa_at IS NOT NULL)
        AND hs.dw_updated_at >= '2024-08-01' AND hs1.is_deleted IS FALSE AND hs.hubspot_id NOT IN (1118703974, 1118716394, 96869003182) --Feb error Germany
    QUALIFY row_per_lcs = 1 AND (hs.member_customers_list IS NULL OR hs.member_customers_list = 'No' OR (was_merged IS NOT NULL AND was_customer = 'Yes' AND paid_products_then IN ('Self (AIA Standalone)','Self (Noa Standalone)') AND wnd.dealname LIKE '%[LG]%') OR (hs.country = 'Italy' AND log.last_source_so LIKE '%Noa%' AND NOT (was_merged IS NOT NULL AND was_customer = 'Yes' AND paid_products_then NOT LIKE '%Noa%'))) --last condition covers issues caused by merges where a contact appears as CM but is actually not
        AND hs.email NOT LIKE '%docplanner.com%' AND hs.email NOT LIKE '%shahar%' AND hs.email NOT LIKE '%test%'
        AND hs.email NOT LIKE '%staszek.marta%'-- and hs.country = 'Germany' AND deal_month IS NOT NULL
),

dates AS (
    SELECT DISTINCT DATE_TRUNC('month', createdate) AS month FROM dw.hs_contact_live WHERE createdate >= '2025-01-01'
)

SELECT
    d.month::TEXT AS month,
    nk.country AS country,
    nk.target AS target,
    COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', nk.free_signup_at) = d.month AND (nk.segment NOT IN ('PATIENT', 'STUDENT') OR nk.segment IS NULL) THEN nk.hubspot_id END) AS noa_free_signups,
    0 AS cc,
    0 AS event,
    0 AS paid,
    COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', nk.noa_mql_at) = d.month AND (nk.segment NOT IN ('PATIENT', 'STUDENT') OR nk.segment IS NULL) THEN nk.hubspot_id END) AS noa_mqls,
    COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', nk.noa_pql_at) = d.month THEN nk.hubspot_id END) AS noa_pqls,
    COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', nk.deal_month) = d.month AND nk.pql_check_test = 0 THEN nk.hubspot_id END) AS noa_mql_wons,
    COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', nk.deal_month) = d.month AND nk.pql_check_test > 0 THEN nk.hubspot_id END) AS noa_pql_wons,
    SUM(CASE WHEN DATE_TRUNC('month', nk.deal_month) = d.month THEN nk.mrr_euro::INT END) AS mrr_euro,
    SUM(CASE WHEN DATE_TRUNC('month', nk.deal_month) = d.month THEN nk.mrr_original_currency::INT END) AS mrr_original_currency
FROM dates d LEFT JOIN noa_leads nk ON d.month = DATE_TRUNC('month', nk.free_signup_at) OR d.month = DATE_TRUNC('month', nk.noa_mql_at) OR d.month = DATE_TRUNC('month', nk.noa_pql_at) OR d.month = DATE_TRUNC('month', nk.deal_month) OR d.month = DATE_TRUNC('month', nk.deal_month)
WHERE d.month >= '2025-01-01' AND nk.country IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
