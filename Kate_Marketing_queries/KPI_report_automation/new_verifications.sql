WITH new_verifications AS (
SELECT
    country,
    DATE_TRUNC('month',hsh.new_verification_at) as new_verification_at,
    hsh.hubspot_id,
    hsh.affiliate_source,
    hsh.spec_split_test,
    hsh.createdate,
    CASE WHEN hsh.new_verification_at > hsh.createdate THEN TRUE END AS existing_flag,
        CASE WHEN hsh.email LIKE '%gdpr%' THEN 'gdpr'
            WHEN hsh.email LIKE '%deleted%' THEN 'deleted'
            WHEN hsh.email LIKE '%docplanner%' THEN 'invalid'
            WHEN hsh.email LIKE '%znanylekarz%' THEN 'invalid'
            WHEN hsh.email LIKE '%doctoralia%' THEN 'invalid'
            WHEN hsh.email LIKE '%miodottore%' THEN 'invalid'
            WHEN hsh.email LIKE '%tuotempo.com%' THEN 'invalid'
            WHEN hsh.email LIKE '%jameda%' THEN 'invalid'
            WHEN hsh.email IS NULL THEN 'invalid'
            WHEN hsh.country IN ('Germany') AND hsh.hs_email_optout IS TRUE THEN 'unsub'
            WHEN hsh.country != 'Germany' AND hsh.unsubscribed_from_all_emails_at IS NOT NULL THEN 'unsub'
            WHEN hsh.doctor_facility___hard_bounced__wf IS NOT NULL THEN 'hardbounce'
            ELSE 'valid'
        END AS email_status,
    CASE WHEN (hsh.email IS NULL AND hsh.hubspot_id IS NOT NULL) -- email null while the hs id is not null (contact exists)
            OR (hsh.saas_user_type_batch IN ('Clinic Secretary Accountant', 'Clinic Secretary Marketing', 'Doctor Secretary', 'Clinic Secretary Receptionist', 'Clinic Secretary', 'Clinic Secretary Head of reception')
                AND hsh.source_doctor_id IS NULL AND hsh.source_facility_id IS NULL AND hsh.secretary_managed_facility_id_dwh_batch IS NOT NULL AND hsh.hubspot_id IS NOT NULL) -- contact exists and is a secretary
            OR (hsh.secretaries_country_promoter_ninja_saas IS NOT NULL AND hsh.source_doctor_id IS NULL AND hsh.source_facility_id IS NULL AND hsh.hubspot_id IS NOT NULL) -- contact exists and is a secretary
            OR (hsh.hs_lead_status = 'UNQUALIFIED' AND hsh.country = 'Italy') THEN TRUE --Italy excludes unqualified leads
        END AS marketing_base_excluded,
         ROW_NUMBER() OVER (PARTITION BY hsh.hubspot_id, DATE_TRUNC('month',hsh.new_verification_at) ORDER BY hsh.dw_updated_at DESC) AS row_per_lcs
       FROM  dw.hs_contact_live_history hsh
WHERE hsh.new_verification_at >= '2025-01-01'
AND (hsh.sub_brand_wf_test LIKE '%Doctoralia%' OR hsh.sub_brand_wf_test LIKE '%MioDottore%' OR hsh.sub_brand_wf_test LIKE '%MioDottore%' OR hsh.sub_brand_wf_test LIKE '%jameda%' OR hsh.sub_brand_wf_test LIKE '%DoktorTakvimi%' OR hsh.sub_brand_wf_test LIKE '%ZnanyLekarz%' OR hsh.sub_brand_wf_test IS NULL)
AND hsh.dw_updated_at BETWEEN hsh.new_verification_at AND DATEADD('day',3,hsh.new_verification_at)
AND contact_type_segment_test IN ('DOCTOR', 'SECRETARY', 'GENERAL PRACTITIONER', 'GENERAL PRACTITIONER & DOCTOR')
AND country IN ('Argentina','Brazil','Chile', 'Colombia', 'Germany', 'Italy', 'Mexico', 'Poland', 'Spain', 'Turkiye')
QUALIFY row_per_lcs = 1 AND email_status = 'valid' AND marketing_base_excluded IS NULL)
SELECT
    country,
    new_verification_at,
    COUNT(DISTINCT hubspot_id) AS all_new_verifications,
    COUNT(DISTINCT CASE WHEN spec_split_test = 'Medical' THEN hubspot_id END) AS all_new_crm_verifications,
    COUNT(DISTINCT CASE WHEN LOWER(affiliate_source) LIKE '%callcenter%' THEN hubspot_id END) AS callcenter_verifications,
    COUNT(DISTINCT CASE WHEN LOWER(affiliate_source) LIKE '%callcenter%' AND existing_flag THEN hubspot_id END) AS existing_callcenter_verifications,
    COUNT(DISTINCT CASE WHEN LOWER(affiliate_source) LIKE '%callcenter%' AND spec_split_test = 'Medical' THEN hubspot_id END) AS crm_callcenter_verifications,
    COUNT(DISTINCT CASE WHEN LOWER(affiliate_source) LIKE '%callcenter%' AND spec_split_test = 'Medical' AND existing_flag THEN hubspot_id END) AS existing_crm_callcenter_verifications,
    COUNT(DISTINCT CASE WHEN (LOWER(affiliate_source) LIKE '%paid_search%' OR LOWER(affiliate_source) LIKE '%paid_social%') THEN hubspot_id END) AS paid_verifications,
    COUNT(DISTINCT CASE WHEN (LOWER(affiliate_source) LIKE '%paid_search%' OR LOWER(affiliate_source) LIKE '%paid_social%') AND existing_flag THEN hubspot_id END) AS existing_paid_verifications,
    COUNT(DISTINCT CASE WHEN (LOWER(affiliate_source) LIKE '%paid_search%' OR LOWER(affiliate_source) LIKE '%paid_social%') AND spec_split_test = 'Medical' THEN hubspot_id END) AS crm_paid_verifications,
    COUNT(DISTINCT CASE WHEN (LOWER(affiliate_source) LIKE '%paid_search%' OR LOWER(affiliate_source) LIKE '%paid_social%') AND spec_split_test = 'Medical' AND existing_flag THEN hubspot_id END) AS existing_crm_paid_verifications,
    COUNT(DISTINCT CASE WHEN existing_flag THEN hubspot_id END) AS existing_db_verifications,
    COUNT(DISTINCT CASE WHEN LOWER(affiliate_source) LIKE '%email%' THEN hubspot_id END) AS email_verifications,
    COUNT(DISTINCT CASE WHEN LOWER(affiliate_source) LIKE '%email%' AND existing_flag THEN hubspot_id END) AS existing_email_verifications,
    COUNT(DISTINCT CASE WHEN LOWER(affiliate_source) LIKE '%email%' AND spec_split_test = 'Medical' THEN hubspot_id END) AS crm_email_verifications,
    COUNT(DISTINCT CASE WHEN LOWER(affiliate_source) LIKE '%email%' AND spec_split_test = 'Medical' AND existing_flag THEN hubspot_id END) AS existing_crm_email_verifications
FROM new_verifications GROUP BY 1, 2 ORDER BY 1,2
