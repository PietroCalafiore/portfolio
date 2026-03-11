WITH subquery AS (
    SELECT
        hcl.hubspot_id,
        hcl.country,
        DATE_TRUNC('month', hcl.createdate) AS create_date,
        DATE_TRUNC('month', hcl.entered_marketing_database_at_test) AS entered_marketing_database_at,
        hcl.last_login_to_docplanner AS login_date,
        hcl.sub_brand_wf_test AS subbrand,
        CASE WHEN hcl.email LIKE '%gdpr%' THEN 'gdpr'
            WHEN hcl.email LIKE '%deleted%' THEN 'deleted'
            WHEN hcl.email LIKE '%docplanner%' THEN 'invalid'
            WHEN hcl.email LIKE '%znanylekarz%' THEN 'invalid'
            WHEN hcl.email LIKE '%doctoralia%' THEN 'invalid'
            WHEN hcl.email LIKE '%miodottore%' THEN 'invalid'
            WHEN hcl.email LIKE '%tuotempo.com%' THEN 'invalid'
            WHEN hcl.email LIKE '%jameda%' THEN 'invalid'
            WHEN hcl.email IS NULL THEN 'invalid'
            WHEN hcl.country IN ('Germany') AND hcl.hs_email_optout IS TRUE THEN 'unsub'
            WHEN hcl.country != 'Germany' AND hcl.unsubscribed_from_all_emails_at IS NOT NULL THEN 'unsub'
            WHEN hcl.doctor_facility___hard_bounced__wf IS NOT NULL THEN 'hardbounce'
            ELSE 'valid'
        END AS email_status,
CASE WHEN hcl.country IN ('Colombia', 'Mexico', 'Italy', 'Brazil', 'Spain')
            AND COALESCE(hcl.contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY') THEN 'DOCTOR'
            WHEN hcl.country IN ('Italy') AND COALESCE(hcl.contact_type_segment_test, 'UNKNOWN') IN ('GENERAL PRACTITIONER', 'GENERAL PRACTITIONER & DOCTOR') THEN 'GP'
            WHEN hcl.country = 'Poland' AND COALESCE(hcl.contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY', 'NURSES') THEN 'DOCTOR'
            WHEN hcl.country IN ('Turkey', 'Argentina', 'Chile', 'Germany', 'Turkiye')
                AND COALESCE(hcl.contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY', 'DOCTOR&FACILITY - 2IN1', 'FACILITY', 'MARKETING') THEN 'DOCTOR' END AS target,
        COALESCE(COALESCE(hcl.verified, hcl.facility_verified), FALSE) AS verified,
        CASE WHEN (hcl.email IS NULL AND hcl.hubspot_id IS NOT NULL) -- email null while the hs id is not null (contact exists)
            OR (hcl.saas_user_type_batch IN ('Clinic Secretary Accountant', 'Clinic Secretary Marketing', 'Doctor Secretary', 'Clinic Secretary Receptionist', 'Clinic Secretary', 'Clinic Secretary Head of reception')
                AND hcl.source_doctor_id IS NULL AND hcl.source_facility_id IS NULL AND hcl.secretary_managed_facility_id_dwh_batch IS NOT NULL AND hcl.hubspot_id IS NOT NULL) -- contact exists and is a secretary
            OR (hcl.secretaries_country_promoter_ninja_saas IS NOT NULL AND hcl.source_doctor_id IS NULL AND hcl.source_facility_id IS NULL AND hcl.hubspot_id IS NOT NULL) -- contact exists and is a secretary
            OR (hcl.secretaries_country_promoter_ninja_saas IS NOT NULL AND hcl.last_saas_batch_update_batch >= CURRENT_DATE - 30) --group 1 in Commercials list
            OR (hcl.hs_lead_status = 'UNQUALIFIED' AND hcl.country = 'Italy') --Italy excludes unqualified leads
            OR hclh.customer_status = 'active'       --exclusion from Commercial list
            OR ((hcl.country = 'Brazil') AND (hcl.spec_split_test = 'Bad Paramedical'))
            OR hcl.package_type__de___temporary_test LIKE '%gold%' OR hcl.package_type__de___temporary_test LIKE '%Platin%' --DE specific commercial condition
            OR team.name LIKE '%BR_CS_ENT%' OR team.name LIKE '%DE_CS_ENT%' OR team.name LIKE '%DE CS%'
            OR team.name LIKE '%ES_CS_ENT%' OR team.name LIKE '%IT_CS_ENT%' OR team.name LIKE '%PL_CS_ENT%'
            OR team.name LIKE '%MX_CS_ENT%'
            OR hclh.member_customers_list = 'Yes' THEN TRUE
        END AS marketing_base_excluded
    FROM dw.hs_contact_live hcl
    LEFT JOIN dw.hs_contact_live_history hclh
        ON hcl.hubspot_id = hclh.hubspot_id AND DATE_TRUNC('day', hclh.dw_updated_at) = DATE_TRUNC('day', hcl.entered_marketing_database_at_test)
    LEFT JOIN dw.hs_team team
        ON hclh.hubspot_team_id = team.hubspot_team_id
    WHERE hcl.is_deleted IS FALSE AND (hcl.spec_split_test != 'Bad Paramedical' OR hcl.spec_split_test IS NULL) and hcl.country= 'Turkiye'
        AND hcl.country IN ('Chile', 'Colombia', 'Spain', 'Mexico', 'Brazil', 'Germany', 'Italy', 'Poland', 'Turkey', 'Turkiye', 'Argentina')
        AND hcl.last_login_to_docplanner >= '2024-01-01'
    QUALIFY email_status = 'valid' AND marketing_base_excluded IS NULL AND target IN ('DOCTOR', 'GP') --this subquery for rolling total of the marketing DB has an additional exclusion of invalid contacts, based on the list logic
        AND (subbrand LIKE '%Doctoralia%' OR subbrand LIKE '%MioDottore%' OR subbrand LIKE '%MioDottore%' OR subbrand LIKE '%jameda%'
         OR subbrand LIKE '%DoktorTakvimi%' OR subbrand LIKE '%ZnanyLekarz%' OR subbrand IS NULL --exclusion based on ALL SUBBRANDS list,to exclude all sub-brand contacts
)
    )
    SELECT
      sb.country,
    DATE_TRUNC('month',last_login_to_docplanner) as login_month,
 COUNT(DISTINCT sb.hubspot_id) AS total,
   COUNT (DISTINCT CASE WHEN cj.lifecycle_stage_start BETWEEN  hclh.last_login_to_docplanner_saas_batch  AND hclh.last_login_to_docplanner_saas_batch + INTERVAL '30 day' THEN cj.contact_id END) AS mqls
   FROM dw.hs_contact_live_history hclh
     INNER JOIN subquery sb
        ON sb.hubspot_id = hclh.hubspot_id
         LEFT JOIN mart_cust_journey.cj_mqls_monthly cj ON HCLH.hubspot_id = cj.contact_id
   where hclh.last_login_to_docplanner >= '2024-01-01'
AND (hclh.member_customers_list IS NULL AND NOT hclh.is_commercial)
group by 1,2
order by 1,2
