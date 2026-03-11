WITH all_verifications AS (
    SELECT
        hs.hubspot_id,
        hs.country,
        hs.sub_brand_wf_test AS subbrand_aux,
        hs.affiliate_source,
        hs.spec_split_test,
        DATE_TRUNC('month', hs.new_verification_at) AS verification_month,
        CASE WHEN hs.email LIKE '%gdpr%' THEN 'gdpr'
            WHEN hs.email LIKE '%deleted%' THEN 'deleted'
            WHEN hs.email LIKE '%docplanner%' THEN 'invalid'
            WHEN hs.email LIKE '%znanylekarz%' THEN 'invalid'
            WHEN hs.email LIKE '%doctoralia%' THEN 'invalid'
            WHEN hs.email LIKE '%miodottore%' THEN 'invalid'
            WHEN hs.email LIKE '%tuotempo.com%' THEN 'invalid'
            WHEN hs.email LIKE '%jameda%' THEN 'invalid'
            WHEN hs.email IS NULL THEN 'invalid'
            WHEN hs.country = 'Germany' AND hs.hs_email_optout IS TRUE THEN 'unsub'
            WHEN hs.country != 'Germany' AND hs.unsubscribed_from_all_emails_at IS NOT NULL THEN 'unsub'
            WHEN hs.doctor_facility___hard_bounced__wf IS NOT NULL THEN 'hardbounce' ELSE 'valid'
        END AS email_status,
        CASE WHEN hs.country IN ('Colombia', 'Mexico', 'Italy', 'Brazil', 'Spain') AND COALESCE(hs.contact_type_segment_test, 'UNKNOWN') IN
            ('UNKNOWN', 'DOCTOR', 'SECRETARY') THEN 'DOCTOR'
            WHEN hs.country IN ('Italy') AND COALESCE(hs.contact_type_segment_test, 'UNKNOWN') IN ('GENERAL PRACTITIONER', 'GENERAL PRACTITIONER & DOCTOR') THEN 'GP'
            WHEN hs.country = 'Poland' AND COALESCE(hs.contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY', 'NURSES') THEN 'DOCTOR'
            WHEN hs.country IN ('Turkey', 'Argentina', 'Chile', 'Germany', 'Turkiye')
                AND COALESCE(hs.contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY', 'DOCTOR&FACILITY - 2IN1', 'FACILITY', 'MARKETING') THEN 'DOCTOR' END AS target_aux_1,
        CASE WHEN (hs.email IS NULL AND hs.hubspot_id IS NOT NULL) -- email null while the hs id is not null (contact exists)
            OR (hs.saas_user_type_batch IN ('Clinic Secretary Accountant', 'Clinic Secretary Marketing', 'Doctor Secretary', 'Clinic Secretary Receptionist', 'Clinic Secretary', 'Clinic Secretary Head of reception')
                AND hs.source_doctor_id IS NULL AND hs.source_facility_id IS NULL AND hs.secretary_managed_facility_id_dwh_batch IS NOT NULL AND hs.hubspot_id IS NOT NULL) -- contact exists and is a secretary
            OR (hs.secretaries_country_promoter_ninja_saas IS NOT NULL AND hs.source_doctor_id IS NULL AND hs.source_facility_id IS NULL AND hs.hubspot_id IS NOT NULL) -- contact exists and is a secretary
            OR (hs.hs_lead_status = 'UNQUALIFIED' AND hs.country = 'Italy') --Italy excludes unqualified leads
            OR (hs.country = 'Brazil' AND hs.spec_split_test = 'Bad Paramedical') THEN TRUE END AS marketing_base_excluded,
        ROW_NUMBER() OVER (PARTITION BY hs.hubspot_id, DATE_TRUNC('month', hs.new_verification_at) ORDER BY hs.dw_updated_at DESC) AS row_per_lcs_aux,
        CASE WHEN DATE_TRUNC('month', hs.new_verification_at) = DATE_TRUNC('month', hs.entered_marketing_database_at_test) OR hs.entered_marketing_database_at_test BETWEEN hs.new_verification_at AND DATEADD('day', 5, hs.new_verification_at) THEN 'new' ELSE 'existing' END AS new_vs_existing_flag,
        CASE WHEN cj.deal_id IS NOT NULL AND cj.deal_month >= DATE_TRUNC('month', hs.new_verification_at) THEN TRUE ELSE FALSE END AS was_won
    FROM dw.hs_contact_live hcl
    LEFT JOIN dw.hs_contact_live_history hs ON hs.hubspot_id = hcl.hubspot_id --cant put a delay here due to some verifications appearing later in the log table(merges) 95520014861 example
    LEFT JOIN mart_cust_journey.cj_mqls_monthly cj ON hcl.hubspot_id = cj.contact_id AND cj.lifecycle_stage = 'only_won'
    WHERE hcl.is_deleted IS FALSE AND hs.country IN ('Mexico')
        AND hcl.entered_marketing_database_at_test IS NOT NULL --only field we use from non-historical data (it got backlogged late)
        AND hs.new_verification_at >= '2025-01-01'
    QUALIFY email_status = 'valid' AND target_aux_1 IN ('DOCTOR', 'GP') AND (subbrand_aux LIKE '%Doctoralia%' OR subbrand_aux LIKE '%MioDottore%' OR subbrand_aux LIKE '%MioDottore%' OR subbrand_aux LIKE '%jameda%' OR subbrand_aux LIKE '%DoktorTakvimi%' OR subbrand_aux LIKE '%ZnanyLekarz%' OR subbrand_aux IS NULL)
    AND row_per_lcs_aux = 1
)

SELECT
    country,
    target_aux_1,
    CAST(verification_month AS TEXT) AS verification_month,
    COUNT(DISTINCT hubspot_id) AS all_verifications,
    COUNT(DISTINCT CASE WHEN was_won IS TRUE THEN hubspot_id END) AS all_verifications_with_wons
FROM all_verifications
WHERE marketing_base_excluded IS NULL
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
