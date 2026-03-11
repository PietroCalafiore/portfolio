   WITH all_new_db AS (
    SELECT
        hs.hubspot_id,
        hs.country,
        DATE_TRUNC('month', hcl.entered_marketing_database_at_test) AS entered_marketing_database_at,
        hs.sub_brand_wf_test AS subbrand,
        CASE WHEN hs.email LIKE '%gdpr%' THEN 'gdpr'
                            WHEN hs.email LIKE '%deleted%' THEN 'deleted'
                            WHEN hs.email LIKE '%docplanner%' THEN 'invalid'
                            WHEN hs.email LIKE '%znanylekarz%' THEN 'invalid'
                            WHEN hs.email LIKE '%doctoralia%' THEN 'invalid'
                            WHEN hs.email LIKE '%miodottore%' THEN 'invalid'
                            WHEN hs.email LIKE '%tuotempo.com%' THEN 'invalid'
                            WHEN hs.email LIKE '%jameda%' THEN 'invalid'
                            WHEN hs.email IS NULL THEN 'invalid'
                            WHEN hs.country IN ('Germany') AND hs.hs_email_optout IS TRUE THEN 'unsub'
                            WHEN hs.country != 'Germany' AND hs.unsubscribed_from_all_emails_at IS NOT NULL
                                THEN 'unsub'
                            WHEN hs.doctor_facility___hard_bounced__wf IS NOT NULL THEN 'hardbounce'
                            ELSE 'valid'
                            END AS email_status,
        CASE WHEN hs.country IN ('Colombia', 'Mexico', 'Italy', 'Brazil', 'Spain')
            AND COALESCE(hs.contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY') THEN 'DOCTOR'
            WHEN hs.country IN ('Italy') AND COALESCE(hs.contact_type_segment_test, 'UNKNOWN') IN ('GENERAL PRACTITIONER', 'GENERAL PRACTITIONER & DOCTOR') THEN 'GP'
            WHEN hs.country = 'Poland' AND COALESCE(hs.contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY', 'NURSES') THEN 'DOCTOR'
            WHEN hs.country IN ('Turkey', 'Argentina', 'Chile', 'Germany', 'Turkiye')
                AND COALESCE(hs.contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY', 'DOCTOR&FACILITY - 2IN1', 'FACILITY', 'MARKETING') THEN 'DOCTOR' END AS target,
        COALESCE(COALESCE(hs.verified, hs.facility_verified), FALSE)  AS verified,
        CASE WHEN (hs.email IS NULL AND hs.hubspot_id IS NOT NULL) -- email null while the hs id is not null (contact exists)
            OR (hs.saas_user_type_batch IN ('Clinic Secretary Accountant', 'Clinic Secretary Marketing',
            'Doctor Secretary', 'Clinic Secretary Receptionist', 'Clinic Secretary', 'Clinic Secretary Head of reception')
            AND hs.source_doctor_id IS NULL AND hs.source_facility_id IS NULL AND hs.secretary_managed_facility_id_dwh_batch IS NOT NULL AND hs.hubspot_id IS NOT NULL) -- contact exists and is a secretary
            OR (hs.secretaries_country_promoter_ninja_saas IS NOT NULL AND hs.source_doctor_id IS NULL AND hs.source_facility_id IS NULL AND hs.hubspot_id IS NOT NULL) -- contact exists and is a secretary
            OR (hs.hs_lead_status = 'UNQUALIFIED' AND hs.country = 'Italy') --Italy excludes unqualified leads
            OR (hs.country = 'Brazil' AND hs.spec_split_test = 'Bad Paramedical')
            THEN TRUE END AS marketing_base_excluded,
            ROW_NUMBER() OVER (PARTITION BY hcl.hubspot_id ORDER BY hs.dw_updated_at DESC) AS row_per_lcs,
            DATE_TRUNC('month',hs.commercial_from) as last_commercial_from,
            CASE WHEN DATE_TRUNC('month',hcl.churn_date_cs) = DATE_TRUNC('month',hcl.entered_marketing_database_at_test)
                AND hs.commercial_from <= DATE_TRUNC('month',hcl.entered_marketing_database_at_test)
            THEN 'churned_from_old_commercial' --if last churn date is the same month as entered marketing database and the commercial date is old, dont count them
            ELSE 'early_churn or not commercial' END AS churn_flag,
            CASE WHEN hcl.entered_marketing_database_at_test IS NULL THEN 'still_null'
                 WHEN hcl.entered_marketing_database_at_test < '2025-01-01'  THEN 'old'
                 ELSE 'same_month or later' END AS marketing_db_date,
            CASE WHEN hs.member_customers_list = 'Yes' AND (hs.commercial_from < '2025-01-01') THEN 'old_commercial' --if at the time of entering db, he was a customer, check if commercial status is old or this month
                WHEN hs.member_customers_list = 'Yes' THEN  'new_commercial' --if doesnt fall into previous group, then newly commercial and is OK to be counted
                WHEN hs.commercial_from_last_date_at IS NULL THEN 'never_commercial'
                ELSE  'not_commercial_now' END AS commercial_flag,
                hs.member_customers_list
    FROM dw.hs_contact_live hcl
    LEFT JOIN dw.hs_contact_live_history hs on hs.hubspot_id = hcl.hubspot_id AND hs.dw_updated_at BETWEEN hcl.entered_marketing_database_at_test AND dateadd('day', 30, hcl.entered_marketing_database_at_test)
    WHERE hcl.is_deleted IS FALSE AND hs.country IS NOT NULL --IN ('Spain', 'Chile', 'Colombia', 'Germany', 'Italy', 'Mexico', 'Poland', 'Turkiye', 'Brazil')
        AND hcl.entered_marketing_database_at_test >= '2025-01-01'
    QUALIFY email_status = 'valid' AND target IN ('DOCTOR') AND (subbrand LIKE '%Doctoralia%' OR subbrand LIKE '%MioDottore%' OR subbrand LIKE '%MioDottore%' OR subbrand LIKE '%jameda%' OR subbrand LIKE '%DoktorTakvimi%' OR subbrand LIKE '%ZnanyLekarz%' OR subbrand IS NULL)
        AND row_per_lcs = 1)

    SELECT
        country,
        entered_marketing_database_at,
        verified,
        COUNT(DISTINCT hubspot_id) FROM all_new_db
    WHERE (member_customers_list = 'No' OR member_customers_list IS NULL OR commercial_flag = 'new_commercial') AND churn_flag != 'churned_from_old_commercial' AND marketing_base_excluded IS NULL
    GROUP BY 1, 2, 3
    ORDER by 1, 2, 3
