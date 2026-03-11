WITH all_months AS (
    SELECT DISTINCT DATE_TRUNC('month', createdate) AS create_date
    FROM dw.hs_contact_live
    WHERE
        country IN ('Colombia', 'Spain', 'Mexico', 'Brazil', 'Italy', 'Poland')
        AND is_deleted IS FALSE
        AND DATE_TRUNC('month', createdate) >= '2010-01-01'
),

all_categories AS (
    SELECT DISTINCT
        CASE WHEN hs_analytics_source = 'SOCIAL_MEDIA' AND LOWER(hs_analytics_source_data_1) IN ('facebook', 'instagram', 'linkedin') THEN 'Organic/Direct'
            WHEN hs_analytics_source = 'PAID_SOCIAL' AND LOWER(hs_analytics_source_data_1) IN ('facebook', 'instagram', 'linkedin') THEN 'Paid'
            WHEN hs_analytics_source = 'PAID_SEARCH' AND (LOWER(hs_analytics_source_data_1) IN ('yahoo', 'bing', 'google')
                OR LOWER(hs_analytics_source_data_2) LIKE '%_%'
                OR LOWER(hs_analytics_source_data_1) LIKE '%_%'
                OR LOWER(hs_analytics_source_data_2) = 'google') THEN 'Paid'
            WHEN doctor_facility___marketing_events_tag IS NOT NULL THEN 'Events'
            WHEN hs_analytics_source = 'ORGANIC_SEARCH'
                AND (LOWER(hs_analytics_source_data_1) IN ('google', 'yahoo', 'bing')
                    OR LOWER(hs_analytics_source_data_2) IN ('google', 'bing', 'yahoo')) THEN 'Organic/Direct'
            WHEN affiliate_source LIKE '%callcenter%' OR marketing_action_tag2 LIKE '%callcenter%' THEN 'Call Center'
            ELSE 'Organic/Direct' END AS db_channel_short,
        COALESCE(CASE WHEN facility_size LIKE ('%Individual%') OR facility_size LIKE ('%Small%') THEN 'Small'
            WHEN facility_size LIKE ('%Large%') OR facility_size LIKE ('%Mid%') THEN 'Medium' ELSE 'Unknown' END, 'Unknown') AS facility_size,
        country,
        CASE WHEN sub_brand_wf_test LIKE '%Doctoralia%' OR sub_brand_wf_test LIKE '%MioDottore%' OR sub_brand_wf_test LIKE '%MioDottore%' OR sub_brand_wf_test LIKE '%jameda%'
            OR sub_brand_wf_test LIKE '%DoktorTakvimi%' OR sub_brand_wf_test LIKE '%ZnanyLekarz%' OR sub_brand_wf_test IS NULL THEN 'Docplanner'
            WHEN sub_brand_wf_test LIKE '%Clinic Cloud%' THEN 'Clinic Cloud'
            WHEN sub_brand_wf_test LIKE '%Feegow%' THEN 'Feegow'
            WHEN sub_brand_wf_test LIKE '%Gipo%' THEN 'Gipo'
            WHEN sub_brand_wf_test LIKE '%MyDr%' THEN 'MyDr'
            ELSE sub_brand_wf_test END AS subbrand
    FROM
        dw.hs_contact_live
    WHERE
        country IN ('Colombia', 'Spain', 'Mexico', 'Brazil', 'Italy', 'Poland') AND is_deleted IS FALSE
),

email_swap1 AS (
    SELECT
        hcl.hubspot_id,
        hclh.email_swap_counter,
        hclh.dw_updated_at,
        hclh.start_date,
        hclh.end_date,
        LAG(hclh.email_swap_counter) OVER (PARTITION BY hclh.hubspot_id ORDER BY hclh.dw_updated_at) AS prev_email_counter--we use this to select the first row after a campaign name has been updated
    FROM dw.hs_contact_live hcl
    INNER JOIN dw.hs_contact_live_history hclh ON hcl.hubspot_id = hclh.hubspot_id
    WHERE hcl.country IN ('Colombia', 'Spain', 'Mexico', 'Brazil', 'Italy', 'Poland')
        AND hcl.is_deleted IS FALSE
        QUALIFY hclh.email_swap_counter <> prev_email_counter OR prev_email_counter IS NULL
),

final_email_swap AS (
    SELECT
        start_date,
        MAX(start_date) OVER (PARTITION BY hubspot_id) AS max_date,
        hubspot_id,
        email_swap_counter
    FROM email_swap1 WHERE TRUE
    GROUP BY 1, 3, 4
    HAVING email_swap_counter > 4 AND start_date >= CURRENT_DATE - 4
    QUALIFY start_date = max_date
),

subquery AS (
    SELECT
        hcl.hubspot_id,
        hcl.country,
        DATE_TRUNC('month', hcl.createdate) AS create_date,
        DATE_TRUNC('month', hcl.entered_marketing_database_at_test) AS entered_marketing_database_at,
        CASE WHEN hcl.sub_brand_wf_test LIKE '%Doctoralia%' OR hcl.sub_brand_wf_test LIKE '%MioDottore%' OR hcl.sub_brand_wf_test LIKE '%MioDottore%' OR hcl.sub_brand_wf_test LIKE '%jameda%'
            OR hcl.sub_brand_wf_test LIKE '%DoktorTakvimi%' OR hcl.sub_brand_wf_test LIKE '%ZnanyLekarz%' OR hcl.sub_brand_wf_test IS NULL THEN 'Docplanner'
            WHEN hcl.sub_brand_wf_test LIKE '%Clinic Cloud%' THEN 'Clinic Cloud'
            WHEN hcl.sub_brand_wf_test LIKE '%Feegow%' THEN 'Feegow'
            WHEN hcl.sub_brand_wf_test LIKE '%Gipo%' THEN 'Gipo'
            WHEN hcl.sub_brand_wf_test LIKE '%MyDr%' THEN 'MyDr'
            ELSE hcl.sub_brand_wf_test END AS subbrand,
        COALESCE(hcl.contact_type_segment_test, 'UNKNOWN') AS segment,
        CASE WHEN hcl.hs_analytics_source = 'SOCIAL_MEDIA'
            AND LOWER(hcl.hs_analytics_source_data_1) IN ('facebook', 'instagram', 'linkedin') THEN 'Organic/Direct' WHEN hcl.hs_analytics_source = 'PAID_SOCIAL'
            AND LOWER(hcl.hs_analytics_source_data_1) IN ('facebook', 'instagram', 'linkedin') THEN 'Paid' WHEN hcl.hs_analytics_source = 'PAID_SEARCH'
            AND (LOWER(hcl.hs_analytics_source_data_1) IN ('yahoo', 'bing', 'google')
                OR LOWER(hcl.hs_analytics_source_data_2) LIKE '%_%'
                OR LOWER(hcl.hs_analytics_source_data_1) LIKE '%_%'
                OR LOWER(hcl.hs_analytics_source_data_2) = 'google') THEN 'Paid' WHEN hcl.doctor_facility___marketing_events_tag IS NOT NULL THEN 'Events' WHEN hcl.hs_analytics_source = 'ORGANIC_SEARCH'
            AND (LOWER(hcl.hs_analytics_source_data_1) IN ('google', 'yahoo', 'bing')
                OR LOWER(hcl.hs_analytics_source_data_2) IN ('google', 'bing', 'yahoo')) THEN 'Organic/Direct' WHEN hcl.affiliate_source LIKE '%callcenter%' OR LOWER(hcl.marketing_action_tag2) LIKE '%callcenter%' THEN 'Call Center' ELSE 'Organic/Direct' END AS db_channel_short,
        CASE WHEN hcl.email LIKE '%gdpr%' THEN 'gdpr'
            WHEN hcl.email LIKE '%deleted%' THEN 'deleted'
            WHEN hcl.email LIKE '%docplanner%' THEN 'invalid'
            WHEN hcl.email LIKE '%znanylekarz%' THEN 'invalid'
            WHEN hcl.email LIKE '%doctoralia%' THEN 'invalid'
            WHEN hcl.email LIKE '%miodottore%' THEN 'invalid'
            WHEN hcl.email LIKE '%tuotempo.com%' THEN 'invalid'
            WHEN hcl.email LIKE '%jameda%' THEN 'invalid'
            WHEN hcl.email IS NULL THEN 'invalid'
            WHEN hcl.hs_email_optout IS TRUE THEN 'unsub'
            WHEN hcl.country IN ('Germany') AND hcl.hs_email_optout IS TRUE THEN 'unsub'
            WHEN hcl.country != 'Germany' AND hcl.unsubscribed_from_all_emails_at IS NOT NULL THEN 'unsub'
            --WHEN unsubscribed_from_all_emails_at IS NOT NULL THEN 'unsub'
            WHEN hcl.doctor_facility___hard_bounced__wf IS NOT NULL THEN 'hardbounce'
            ELSE 'valid'
        END AS email_status,
        COALESCE(CASE WHEN hcl.facility_size LIKE ('%Individual%') OR hcl.facility_size LIKE ('%Small%') THEN 'Small'
            WHEN hcl.facility_size LIKE ('%Large%') OR hcl.facility_size LIKE ('%Mid%') THEN 'Medium'
            ELSE 'Unknown' END, 'Unknown') AS facility_size,
        CASE WHEN (hcl.email IS NULL AND hcl.hubspot_id IS NOT NULL) -- email null while the hs id is not null (contact exists)
                OR (hcl.saas_user_type_batch IN ('Clinic Secretary Accountant', 'Clinic Secretary Marketing', 'Doctor Secretary', 'Clinic Secretary Receptionist', 'Clinic Secretary', 'Clinic Secretary Head of reception')
                    AND hcl.source_doctor_id IS NULL AND hcl.source_facility_id IS NULL AND hcl.secretary_managed_facility_id_dwh_batch IS NOT NULL AND hcl.hubspot_id IS NOT NULL) -- contact exists and is a secretary
                OR (hcl.secretaries_country_promoter_ninja_saas IS NOT NULL AND hcl.source_doctor_id IS NULL AND hcl.source_facility_id IS NULL AND hcl.hubspot_id IS NOT NULL) -- contact exists and is a secretary
                OR hcl.facility_is_commercial OR hcl.is_commercial --commercial flag properties, dont cover all commercials but some
                OR (hcl.secretaries_country_promoter_ninja_saas IS NOT NULL AND hcl.last_saas_batch_update_batch >= CURRENT_DATE - 30) --group 1 in Commercials list
                OR (hcl.hs_lead_status = 'UNQUALIFIED' AND hcl.country = 'Italy') --Italy excludes unqualified leads
                OR hcl.customer_status = 'active' --exclusion from Commercial list
                OR cl.facility_products_paid_batch IS NOT NULL
                OR cl.is_active_customer_batch IS TRUE
                OR hcl."tag" LIKE '%Bundle Feegow%'
                OR fe.hubspot_id IS NOT NULL
                OR hcl.member_customers_list = 'Yes'
                OR team.name LIKE '%BR_CS_ENT%' OR team.name LIKE '%DE_CS_ENT%' OR team.name LIKE '%DE CS%' OR team.name LIKE '%ES_CS_ENT%' OR team.name LIKE '%IT_CS_ENT%' OR team.name LIKE '%PL_CS_ENT%' OR team.name LIKE '%MX_CS_ENT%'
                THEN TRUE END AS marketing_base_excluded
    FROM
        dw.hs_contact_live hcl
    LEFT JOIN mart_cust_journey.cj_deal_month deal ON deal.hs_contact_id = hcl.hubspot_id
        AND deal.deal_stage = 'Closed Won'
        AND deal.is_current_stage
        AND deal.stage_is_month_new
    LEFT JOIN dw.hs_team team ON hcl.hubspot_team_id = team.hubspot_team_id
    LEFT JOIN dw.hs_association_live al ON hcl.hubspot_id = al.to_obj AND al.type = 'company_to_contact' AND al.is_deleted IS FALSE
    LEFT JOIN dw.hs_company_live cl ON cl.hubspot_id = al.from_obj AND cl.is_deleted IS FALSE
    LEFT JOIN final_email_swap fe ON fe.hubspot_id = hcl.hubspot_id
    WHERE
        hcl.is_deleted IS FALSE
        AND hcl.country IN ('Colombia', 'Spain', 'Mexico', 'Brazil', 'Italy', 'Poland')
),

subquery2 AS (
    SELECT * FROM subquery
    WHERE (email_status = 'valid' AND subbrand = 'Docplanner' AND marketing_base_excluded IS NULL AND segment IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1'))
        OR (email_status = 'valid' AND subbrand != 'Docplanner' AND marketing_base_excluded IS NULL AND (segment NOT IN ('PATIENT', 'STUDENT') OR segment IS NULL))
),

subquery3 AS (
    SELECT * FROM subquery
    WHERE (email_status = 'valid' AND subbrand = 'Docplanner' AND segment IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1'))
        OR (email_status = 'valid' AND subbrand != 'Docplanner' AND (segment NOT IN ('PATIENT', 'STUDENT') OR segment IS NULL))
)

SELECT
    all_months.create_date,
    all_categories.country,
    all_categories.db_channel_short,
    all_categories.facility_size,
    all_categories.subbrand,
    COUNT(DISTINCT subquery2.hubspot_id) AS total,
    COUNT(DISTINCT em.hubspot_id) AS total_new_marketing_db,
    COALESCE(SUM(COUNT(DISTINCT subquery2.hubspot_id)) OVER (PARTITION BY all_categories.country, all_categories.db_channel_short, all_categories.facility_size, all_categories.subbrand
        ORDER BY all_months.create_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0) AS rolling_total
FROM all_months CROSS JOIN all_categories
LEFT JOIN subquery2 ON all_months.create_date = subquery2.create_date
    AND all_categories.db_channel_short = subquery2.db_channel_short
    AND all_categories.facility_size = subquery2.facility_size
    AND all_categories.country = subquery2.country
    AND all_categories.subbrand = subquery2.subbrand
LEFT JOIN subquery3 em ON all_months.create_date = em.entered_marketing_database_at
    AND all_categories.db_channel_short = em.db_channel_short
    AND all_categories.facility_size = em.facility_size
    AND all_categories.country = em.country
    AND all_categories.subbrand = em.subbrand
GROUP BY
    1, 2, 3, 4, 5
ORDER BY
    1, 2, 3, 4, 5
