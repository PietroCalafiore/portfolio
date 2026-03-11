
DROP TABLE IF EXISTS cj_data_layer.mkt_db_eom_temp;
CREATE TABLE cj_data_layer.mkt_db_eom_temp AS
WITH all_months AS (
    SELECT DISTINCT DATE_TRUNC('month', createdate::DATE) AS create_date
    FROM dw.hs_contact_live
    WHERE DATE_TRUNC('month', createdate::DATE) >= '2007-01-01' AND country IN ('Colombia', 'Spain', 'Mexico', 'Brazil', 'Italy', 'Poland')
    UNION DISTINCT
    SELECT DISTINCT DATE_TRUNC('month', entered_marketing_database_at_test) AS create_date
    FROM dw.hs_contact_live
    WHERE DATE_TRUNC('month', createdate) >= '2007-01-01' AND country IN ('Colombia', 'Spain', 'Mexico', 'Brazil', 'Italy', 'Poland')
),

all_categories AS (
    SELECT DISTINCT
        CASE WHEN hs_analytics_source = 'PAID_SOCIAL' AND LOWER(hs_analytics_source_data_1) IN ('facebook', 'instagram', 'linkedin') THEN 'Paid'
            WHEN hs_analytics_source = 'PAID_SEARCH' AND (LOWER(hs_analytics_source_data_1) IN ('yahoo', 'bing', 'google')
                OR LOWER(hs_analytics_source_data_2) LIKE '%_%'
                OR LOWER(hs_analytics_source_data_1) LIKE '%_%'
                OR LOWER(hs_analytics_source_data_2) = 'google') THEN 'Paid'
            WHEN doctor_facility___marketing_events_tag IS NOT NULL THEN 'Events'
            WHEN affiliate_source LIKE '%callcenter%' OR LOWER(marketing_action_tag2) LIKE '%callcenter%' THEN 'Call Center'
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
            WHEN sub_brand_wf_test LIKE '%Noa%' THEN 'Noa'
            WHEN sub_brand_wf_test LIKE '%Tuotempo%' THEN 'Tuotempo'
            ELSE sub_brand_wf_test END AS subbrand
    FROM
        dw.hs_contact_live
    WHERE
        country IN ('Colombia', 'Spain', 'Mexico', 'Brazil', 'Italy', 'Poland') AND is_deleted IS FALSE
),

subquery AS (
    SELECT
        hcl.hubspot_id,
        hcl.country,
        DATE_TRUNC('month', hcl.createdate)::DATE AS create_date,
        DATE_TRUNC('month', hcl.entered_marketing_database_at_test)::DATE AS entered_marketing_database_at,
        CASE WHEN (hcl.sub_brand_wf_test LIKE '%Doctoralia%' OR hcl.sub_brand_wf_test LIKE '%MioDottore%' OR hcl.sub_brand_wf_test LIKE '%MioDottore%' OR hcl.sub_brand_wf_test LIKE '%jameda%'
            OR hcl.sub_brand_wf_test LIKE '%DoktorTakvimi%' OR hcl.sub_brand_wf_test LIKE '%ZnanyLekarz%' OR hcl.sub_brand_wf_test IS NULL)
            AND COALESCE(hcl.contact_type_segment_test, 'UNKNOWN') IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'Docplanner'
            WHEN hcl.sub_brand_wf_test LIKE '%Clinic Cloud%' THEN 'Clinic Cloud'
            WHEN hcl.sub_brand_wf_test LIKE '%Feegow%' THEN 'Feegow'
            WHEN hcl.sub_brand_wf_test LIKE '%Gipo%' THEN 'Gipo'
            WHEN hcl.sub_brand_wf_test LIKE '%MyDr%' THEN 'MyDr'
            WHEN hcl.sub_brand_wf_test LIKE '%Tuotempo%' THEN 'MyDr'
            WHEN hcl.sub_brand_wf_test LIKE '%Noa%' AND COALESCE(hcl.contact_type_segment_test, 'UNKNOWN') IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'Noa'
            ELSE 'None' END AS subbrand,
        COALESCE(hcl.contact_type_segment_test, 'UNKNOWN') AS segment,
        CASE WHEN hcl.hs_analytics_source = 'PAID_SOCIAL'
                AND LOWER(hcl.hs_analytics_source_data_1) IN ('facebook', 'instagram', 'linkedin') THEN 'Paid'
            WHEN hcl.hs_analytics_source = 'PAID_SEARCH'
                AND (LOWER(hcl.hs_analytics_source_data_1) IN ('yahoo', 'bing', 'google')
                    OR LOWER(hcl.hs_analytics_source_data_2) LIKE '%_%'
                    OR LOWER(hcl.hs_analytics_source_data_1) LIKE '%_%'
                    OR LOWER(hcl.hs_analytics_source_data_2) = 'google') THEN 'Paid'
            WHEN hcl.doctor_facility___marketing_events_tag IS NOT NULL THEN 'Events'
            WHEN hcl.affiliate_source LIKE '%callcenter%' OR LOWER(hcl.marketing_action_tag2) LIKE '%callcenter%' THEN 'Call Center' ELSE 'Organic/Direct' END AS db_channel_short,
         CASE WHEN hcl.email LIKE '%gdpr%' THEN 'gdpr'
            WHEN hcl.email LIKE '%deleted%' THEN 'deleted'
            WHEN hcl.email LIKE '%docplanner%' THEN 'invalid'
            WHEN hcl.email LIKE '%znanylekarz%' THEN 'invalid'
            WHEN hcl.email LIKE '%doctoralia%' THEN 'invalid'
            WHEN hcl.email LIKE '%miodottore%' THEN 'invalid'
            WHEN hcl.email LIKE '%tuotempo.com%' THEN 'invalid'
            WHEN hcl.email LIKE '%jameda%' THEN 'invalid'
            WHEN hcl.email IS NULL THEN 'invalid'
            WHEN hcl.country IN ('Germany') AND (hcl.hs_email_optout IS TRUE OR hcl.jameda_do_not_contact = 'True') THEN 'unsub'
            WHEN hcl.email_swap_counter > 4 AND hcl.swaped_email_at >= CURRENT_DATE - 4 THEN 'unsub'
            WHEN hcl.country != 'Germany' AND hcl.unsubscribed_from_all_emails_at IS NOT NULL THEN 'unsub'
            WHEN hcl.doctor_facility___hard_bounced__wf IS NOT NULL THEN 'hardbounce'
            ELSE 'valid' END AS email_status,
        COALESCE(CASE WHEN hcl.facility_size LIKE ('%Individual%') OR hcl.facility_size LIKE ('%Small%') THEN 'Small'
            WHEN hcl.facility_size LIKE ('%Large%') OR hcl.facility_size LIKE ('%Mid%') THEN 'Medium'
            ELSE 'Unknown' END, 'Unknown') AS facility_size_aux,
        CASE WHEN (hcl.email IS NULL AND hcl.hubspot_id IS NOT NULL) -- email null while the hs id is not null (contact exists)
            OR (hcl.saas_user_type_batch IN ('Clinic Secretary Accountant', 'Clinic Secretary Marketing', 'Doctor Secretary', 'Clinic Secretary Receptionist', 'Clinic Secretary', 'Clinic Secretary Head of reception')
                AND hcl.source_doctor_id IS NULL AND hcl.source_facility_id IS NULL AND hcl.secretary_managed_facility_id_dwh_batch IS NOT NULL AND hcl.hubspot_id IS NOT NULL) -- contact exists and is a secretary
            OR (hcl.secretaries_country_promoter_ninja_saas IS NOT NULL AND hcl.source_doctor_id IS NULL AND hcl.source_facility_id IS NULL AND hcl.hubspot_id IS NOT NULL) -- contact exists and is a secretary
            OR (hcl.hs_lead_status = 'UNQUALIFIED' AND hcl.country = 'Italy') --Italy excludes unqualified leads
            OR (hcl.country = 'Brazil' AND spec_split_test = 'Bad Paramedical')
            THEN TRUE END AS marketing_base_excluded,
        hcl.member_customers_list AS is_customer_now,
        hcl.all_comms_excl_list_member AS communication_excluded
    FROM
        dw.hs_contact_live hcl
    WHERE hcl.is_deleted IS FALSE
        AND hcl.country IN ('Colombia', 'Spain', 'Mexico', 'Brazil', 'Italy', 'Poland') AND hcl.entered_marketing_database_at_test IS NOT NULL
    QUALIFY email_status = 'valid' AND (hcl.contact_type_segment_test NOT IN ('PATIENT', 'STUDENTS') OR hcl.contact_type_segment_test IS NULL)
        AND marketing_base_excluded IS NULL
),

subquery2 AS (
    SELECT *,
    CASE WHEN subbrand IN ('Docplanner', 'Noa', 'Feegow', 'Tuotempo', 'Gipo') THEN facility_size_aux
    WHEN subbrand = 'Clinic Cloud' AND segment = 'DOCTOR' THEN 'Small'
    WHEN subbrand = 'Clinic Cloud' THEN 'Medium'
    WHEN subbrand = 'MyDr' AND segment IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'Medium'
    WHEN subbrand = 'MyDr' THEN 'Small'
    END AS facility_size
           FROM subquery
    WHERE (is_customer_now IS NULL OR is_customer_now = 'No') AND NOT (subbrand = 'Docplanner' AND communication_excluded = 'Yes')
    ),

subquery3 AS (--useless-ignore
    SELECT *,
           CASE WHEN subbrand IN ('Docplanner', 'Noa', 'Feegow', 'Tuotempo', 'Gipo') THEN facility_size_aux
    WHEN subbrand = 'Clinic Cloud' AND segment = 'DOCTOR' THEN 'Small'
    WHEN subbrand = 'Clinic Cloud' THEN 'Medium'
    WHEN subbrand = 'MyDr' AND segment IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'Medium'
    WHEN subbrand = 'MyDr' THEN 'Small'
    END AS facility_size
           FROM subquery
    WHERE (email_status = 'valid' AND subbrand = 'Docplanner' AND segment IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1'))
        OR (email_status = 'valid' AND subbrand != 'Docplanner' AND (segment NOT IN ('PATIENT', 'STUDENT') OR segment IS NULL))
)

SELECT
    all_months.create_date::DATE,
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
    1, 2, 3, 4, 5, all_months.create_date
ORDER BY
    1, 2, 3, 4, 5
