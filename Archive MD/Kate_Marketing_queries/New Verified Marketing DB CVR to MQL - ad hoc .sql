WITH
    subquery as ( --actual query pulling data
    SELECT
      hcl.hubspot_id,
      hcl.country,
      hcl.new_verification_at AS verification_date,
      DATE_TRUNC('month', hcl.createdate) AS create_date,
      DATE_TRUNC('month', hcl.entered_marketing_database_at_test) as entered_marketing_database_at,
      hcl.sub_brand_wf_test as subbrand,
    hcl.spec_split_test AS spec_split_test,
      CASE -- HS latest source known
      WHEN hs_analytics_source = 'SOCIAL_MEDIA'
      AND LOWER(hs_analytics_source_data_1) IN (
        'facebook', 'instagram', 'linkedin'
      ) THEN 'Organic/Direct' WHEN hs_analytics_source = 'PAID_SOCIAL'
      AND LOWER(hs_analytics_source_data_1) IN (
        'facebook', 'instagram', 'linkedin'
      ) THEN 'Paid' WHEN hs_analytics_source = 'PAID_SEARCH'
      AND (
        LOWER(hs_analytics_source_data_1) IN ('yahoo', 'bing', 'google')
        OR LOWER(hs_analytics_source_data_2) LIKE '%_%'
        OR LOWER(hs_analytics_source_data_1) LIKE '%_%'
        OR LOWER(hs_analytics_source_data_2) = 'google'
      ) THEN 'Paid' WHEN hcl.doctor_facility___marketing_events_tag IS NOT NULL THEN 'Events' WHEN hcl.hs_analytics_source = 'ORGANIC_SEARCH'
      AND (
        LOWER(hcl.hs_analytics_source_data_1) IN ('google', 'yahoo', 'bing')
        OR LOWER(hcl.hs_analytics_source_data_2) IN ('google', 'bing', 'yahoo')
      ) THEN 'Organic/Direct' WHEN hcl.affiliate_source LIKE '%callcenter%' THEN 'Call Center' ELSE 'Organic/Direct' END AS db_channel_short,
         CASE
            WHEN hcl.email LIKE '%gdpr%' THEN 'gdpr'
            WHEN hcl.email LIKE '%deleted%' THEN 'deleted'
            WHEN hcl.email LIKE '%docplanner%' THEN 'invalid'
            WHEN hcl.email LIKE '%znanylekarz%' THEN 'invalid'
            WHEN hcl.email LIKE '%doctoralia%' THEN 'invalid'
            WHEN hcl.email LIKE '%miodottore%' THEN 'invalid'
            WHEN hcl.email LIKE '%tuotempo.com%' THEN 'invalid'
            WHEN hcl.email LIKE '%jameda%' THEN 'invalid'
            WHEN hcl.email IS NULL THEN 'invalid'
            WHEN hcl.country IN ('Germany') and hcl.hs_email_optout IS TRUE THEN 'unsub'
            WHEN hcl.country != 'Germany' and hcl.unsubscribed_from_all_emails_at IS NOT NULL THEN 'unsub'
            --WHEN unsubscribed_from_all_emails_at IS NOT NULL THEN 'unsub'
            WHEN hcl.doctor_facility___hard_bounced__wf IS NOT NULL THEN 'hardbounce'
            ELSE 'valid'
        END AS email_status,
      CASE WHEN hcl.country IN (
        'Colombia', 'Mexico', 'Italy', 'Brazil',
        'Spain'
      )
      AND COALESCE(
        hcl.contact_type_segment_test, 'UNKNOWN'
      ) IN ('UNKNOWN', 'DOCTOR', 'SECRETARY') THEN 'DOCTOR' WHEN hcl.country IN ('Italy')
      AND COALESCE(
        hcl.contact_type_segment_test, 'UNKNOWN'
      ) IN (
        'GENERAL PRACTITIONER', 'GENERAL PRACTITIONER & DOCTOR'
      ) THEN 'GP' WHEN hcl.country = 'Poland'
      AND COALESCE(
        hcl.contact_type_segment_test, 'UNKNOWN'
      ) IN (
        'UNKNOWN', 'DOCTOR', 'SECRETARY',
        'NURSES'
      ) THEN 'DOCTOR' WHEN hcl.country IN (
        'Turkey', 'Argentina', 'Chile', 'Germany',
        'Turkiye'
      )
      AND COALESCE(
        hcl.contact_type_segment_test, 'UNKNOWN'
      ) IN (
        'UNKNOWN', 'DOCTOR', 'SECRETARY',
        'DOCTOR&FACILITY - 2IN1', 'FACILITY',
        'MARKETING'
      ) THEN 'DOCTOR' ELSE 'OTHER' END AS target,
      COALESCE(
        hcl.verified, hcl.facility_verified
      ) AS verified,
          CASE WHEN (hcl.email IS NULL AND hcl.hubspot_id IS NOT NULL) -- email null while the hs id is not null (contact exists)
                  OR (hcl.saas_user_type_batch IN ('Clinic Secretary Accountant', 'Clinic Secretary Marketing', 'Doctor Secretary', 'Clinic Secretary Receptionist', 'Clinic Secretary','Clinic Secretary Head of reception')
                          AND hcl.source_doctor_id IS NULL AND hcl.source_facility_id IS NULL AND hcl.secretary_managed_facility_id_dwh_batch IS NOT NULL AND hcl.hubspot_id IS NOT NULL) -- contact exists and is a secretary
                  OR (hcl.secretaries_country_promoter_ninja_saas IS NOT NULL AND hcl.source_doctor_id IS NULL AND hcl.source_facility_id IS NULL AND hcl.hubspot_id IS NOT NULL) -- contact exists and is a secretary
OR (hclh.secretaries_country_promoter_ninja_saas IS NOT NULL and hcl.last_saas_batch_update_batch >= hcl.new_verification_at-30) --group 1 in Commercials list
OR (hcl.hs_lead_status = 'UNQUALIFIED' and hcl.country = 'Italy') --Italy excludes unqualified leads
OR hcl.package_type__de___temporary_test LIKE '%gold%' OR hcl.package_type__de___temporary_test LIKE '%Platin%'  --DE specific commercial condition
OR team.name LIKE '%BR_CS_ENT%' OR team.name LIKE '%DE_CS_ENT%' OR team.name LIKE '%DE CS%'  OR team.name LIKE '%ES_CS_ENT%'  OR team.name LIKE '%IT_CS_ENT%' OR team.name LIKE '%PL_CS_ENT%' OR team.name LIKE '%MX_CS_ENT%'
OR hclh.member_customers_list = 'Yes'
   OR hclh.facility_is_commercial OR hclh.is_commercial
                                   OR hclh.customer_status = 'active'
OR     (hcl.spec_split_test = 'Bad Paramedical')
THEN TRUE END AS marketing_base_excluded
    FROM
      dw.hs_contact_live hcl
    LEFT JOIN dw.hs_contact_live_history hclh
    ON hcl.hubspot_id = hclh.hubspot_id and DATE_TRUNC('day',hclh.dw_updated_at) = DATE_TRUNC('day',hcl.new_verification_at)
      LEFT JOIN dw.hs_team team ON hclh.hubspot_team_id = team.hubspot_team_id
    WHERE
      hcl.is_deleted IS FALSE
         and hcl.new_verification_at >= '2024-05-01'
      and hcl.country IN ('Chile', 'Colombia', 'Spain', 'Mexico', 'Brazil', 'Germany', 'Italy', 'Poland', 'Turkey', 'Turkiye', 'Argentina')
  )

SELECT sub.country, DATE_TRUNC('month',verification_date) as verification_month,

    COUNT(distinct hubspot_id) as all_verified_contacts,
    COUNT (DISTINCT CASE WHEN cj.lifecycle_stage_start BETWEEN  sub.verification_date  AND sub.verification_date + INTERVAL '30 day' THEN cj.contact_id END) AS mqls
    FROM subquery sub
    LEFT JOIN mart_cust_journey.cj_mqls_monthly cj ON sub.hubspot_id = cj.contact_id
    WHERE email_status = 'valid'
    AND sub.db_channel_short != 'Call Center'
      AND    sub.marketing_base_excluded IS NULL
    AND    sub.target IN ('DOCTOR','GP') --this subquery for rolling total of the marketing DB has an additional exclusion of invalid contacts, based on the list logic
   AND    (sub.subbrand LIKE '%Doctoralia%'
         OR     sub.subbrand LIKE '%MioDottore%'
           OR     sub.subbrand LIKE '%MioDottore%'
           OR     sub.subbrand LIKE '%jameda%'
           OR     sub.subbrand LIKE '%DoktorTakvimi%'
       OR     sub.subbrand LIKE '%ZnanyLekarz%'
          OR     sub.subbrand IS NULL)
    GROUP BY 1,2
