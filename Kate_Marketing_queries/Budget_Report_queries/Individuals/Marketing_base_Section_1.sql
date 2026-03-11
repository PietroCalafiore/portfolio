WITH --here we select all months that historical information is available for. we just need a column with all possible dates
  all_months AS (
    SELECT DISTINCT
      DATE_TRUNC('month', createdate) AS create_date
    FROM
      dw.hs_contact_live
    WHERE
      country IN ('Chile', 'Colombia', 'Spain', 'Mexico', 'Brazil', 'Germany', 'Italy', 'Poland', 'Turkey', 'Turkiye', 'Argentina')
      AND is_deleted IS FALSE
      AND DATE_TRUNC('month', createdate) >= '2010-01-01'
  ),
  all_categories AS ( --here we select all possible combinations for source/specialisation/target per country
    SELECT DISTINCT
      CASE -- HS latest source known
        WHEN hs_analytics_source = 'SOCIAL_MEDIA'
        AND LOWER(hs_analytics_source_data_1) IN (
          'facebook', 'instagram', 'linkedin'
        ) THEN 'Organic/Direct'
        WHEN hs_analytics_source = 'PAID_SOCIAL'
        AND LOWER(hs_analytics_source_data_1) IN (
          'facebook', 'instagram', 'linkedin'
        ) THEN 'Paid'
        WHEN hs_analytics_source = 'PAID_SEARCH'
        AND (
          LOWER(hs_analytics_source_data_1) IN ('yahoo', 'bing', 'google')
          OR LOWER(hs_analytics_source_data_2) LIKE '%_%'
          OR LOWER(hs_analytics_source_data_1) LIKE '%_%'
          OR LOWER(hs_analytics_source_data_2) = 'google'
        ) THEN 'Paid'
        WHEN doctor_facility___marketing_events_tag IS NOT NULL THEN 'Events'
        WHEN hs_analytics_source = 'ORGANIC_SEARCH'
        AND (
          LOWER(hs_analytics_source_data_1) IN ('google', 'yahoo', 'bing')
          OR LOWER(hs_analytics_source_data_2) IN ('google', 'bing', 'yahoo')
        ) THEN 'Organic/Direct'
        WHEN affiliate_source LIKE '%callcenter%'  OR marketing_action_tag2 LIKE '%callcenter%' THEN 'Call Center' 
        ELSE 'Organic/Direct'
      END AS db_channel_short,
      CASE WHEN country IN (
        'Colombia', 'Mexico', 'Italy', 'Brazil',
        'Spain'
      )
      AND COALESCE(
        contact_type_segment_test, 'UNKNOWN'
      ) IN ('UNKNOWN', 'DOCTOR', 'SECRETARY') THEN 'DOCTOR'
      WHEN country IN ('Italy')
      AND COALESCE(
        contact_type_segment_test, 'UNKNOWN'
      ) IN (
        'GENERAL PRACTITIONER', 'GENERAL PRACTITIONER & DOCTOR'
      ) THEN 'GP'
      WHEN country = 'Poland'
      AND COALESCE(
        contact_type_segment_test, 'UNKNOWN'
      ) IN (
        'UNKNOWN', 'DOCTOR', 'SECRETARY',
        'NURSES'
      ) THEN 'DOCTOR'
      WHEN country IN (
        'Turkey', 'Argentina', 'Chile', 'Germany',
        'Turkiye'
      )
      AND COALESCE(
        contact_type_segment_test, 'UNKNOWN'
      ) IN (
        'UNKNOWN', 'DOCTOR', 'SECRETARY',
        'DOCTOR&FACILITY - 2IN1', 'FACILITY',
        'MARKETING'
      ) THEN 'DOCTOR'
      END AS target,
      COALESCE(
        verified, facility_verified
      ) AS verified,
        country,
CASE WHEN (country = 'Brazil') and spec_split_test = 'Medical' then 'Medical'
WHEN (country = 'Brazil') and (spec_split_test = 'Paramedical' or spec_split_test IS NULL) then 'Paramedical'  ELSE 'None'  END AS spec_split_test
    FROM
      dw.hs_contact_live
    WHERE
     country IN ('Chile', 'Colombia', 'Spain', 'Mexico', 'Brazil', 'Germany', 'Italy', 'Poland', 'Turkey', 'Turkiye', 'Argentina')
      AND is_deleted IS FALSE
UNION ALL 
SELECT -- additional category combo for Chile as we had no such leads in the past but targets for this combo exist
'Events' as db_channel_short,
'DOCTOR' as target,
FALSE as verified,
'Chile' as country,
'None' as spec_split_test
  ),
    subquery as (
    SELECT
      hcl.hubspot_id,
      hcl.country,
      DATE_TRUNC('month', hcl.createdate) AS create_date,
      DATE_TRUNC('month', hcl.entered_marketing_database_at_test) as entered_marketing_database_at,
      sub_brand_wf_test as subbrand,
CASE WHEN (hcl.country = 'Brazil') and hcl.spec_split_test = 'Medical' then 'Medical'
WHEN (hcl.country = 'Brazil') and (hcl.spec_split_test = 'Paramedical' or hcl.spec_split_test IS NULL) then 'Paramedical'  ELSE 'None'  END AS spec_split_test,
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
      ) THEN 'Paid' WHEN doctor_facility___marketing_events_tag IS NOT NULL THEN 'Events' WHEN hs_analytics_source = 'ORGANIC_SEARCH'
      AND (
        LOWER(hs_analytics_source_data_1) IN ('google', 'yahoo', 'bing')
        OR LOWER(hs_analytics_source_data_2) IN ('google', 'bing', 'yahoo')
      ) THEN 'Organic/Direct' WHEN hcl.affiliate_source LIKE '%callcenter%'  OR hcl.marketing_action_tag2 LIKE '%callcenter%' THEN 'Call Center'
        ELSE 'Organic/Direct' END AS db_channel_short,
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
            WHEN hcl.country != 'Germany' and unsubscribed_from_all_emails_at IS NOT NULL THEN 'unsub'
            --WHEN unsubscribed_from_all_emails_at IS NOT NULL THEN 'unsub'
            WHEN doctor_facility___hard_bounced__wf IS NOT NULL THEN 'hardbounce'
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
OR facility_is_commercial OR is_commercial --commercial flag properties, dont cover all commercials but some
OR lifecycle_stage in ('WAITING','CLOSED-WON','ONBOARDING', 'FARMING') --excluding commercial LCS
--OR deal.deal_id is not NULL --has an active deal in CJ tables
OR (hcl.secretaries_country_promoter_ninja_saas IS NOT NULL and hcl.last_saas_batch_update_batch >= current_date-30) --group 1 in Commercials list
OR (hs_lead_status = 'UNQUALIFIED' and hcl.country = 'Italy') --Italy excludes unqualified leads
OR customer_status = 'active' --exclusion from Commercial list
OR (hcl.country = 'Brazil') and (hcl.spec_split_test = 'Bad Paramedical' )
OR hcl.package_type__de___temporary_test LIKE '%gold%' OR hcl.package_type__de___temporary_test LIKE '%Platin%'  --DE specific commercial condition
OR team.name LIKE '%BR_CS_ENT%' OR team.name LIKE '%DE_CS_ENT%' OR team.name LIKE '%DE CS%'  OR team.name LIKE '%ES_CS_ENT%'  OR team.name LIKE '%IT_CS_ENT%' OR team.name LIKE '%PL_CS_ENT%' OR team.name LIKE '%MX_CS_ENT%'
OR member_customers_list = 'Yes'
THEN TRUE END AS marketing_base_excluded
    FROM
      dw.hs_contact_live hcl
      LEFT JOIN mart_cust_journey.cj_deal_month deal ON deal.hs_contact_id = hcl.hubspot_id
      AND deal_stage = 'Closed Won'
      AND is_current_stage
      AND stage_is_month_new
      LEFT JOIN dw.hs_team team ON hcl.hubspot_team_id = team.hubspot_team_id
    WHERE
      hcl.is_deleted IS FALSE AND (hcl.spec_split_test != 'Bad Paramedical' OR hcl.spec_split_test IS NULL)
      and hcl.country IN ('Chile', 'Colombia', 'Spain', 'Mexico', 'Brazil', 'Germany', 'Italy', 'Poland', 'Turkey', 'Turkiye', 'Argentina')
  ),
    rolling_total_sub as(
        select * from subquery
    where email_status = 'valid' and marketing_base_excluded is NULL and target IN ('DOCTOR','GP') --this subquery for rolling total of the marketing DB has an additional exclusion of invalid contacts, based on the list logic
   AND (subbrand LIKE '%Doctoralia%' OR subbrand LIKE '%MioDottore%' OR subbrand LIKE '%MioDottore%' OR subbrand LIKE '%jameda%'
OR subbrand LIKE '%DoktorTakvimi%' OR subbrand LIKE '%ZnanyLekarz%' OR subbrand IS NULL) --exclusion based on ALL SUBBRANDS list,to exclude all sub-brand contacts
    ),
 new_db as(
        select * from subquery
    where email_status = 'valid' and target IN ('DOCTOR','GP')
   AND (subbrand LIKE '%Doctoralia%' OR subbrand LIKE '%MioDottore%' OR subbrand LIKE '%MioDottore%' OR subbrand LIKE '%jameda%'
OR subbrand LIKE '%DoktorTakvimi%' OR subbrand LIKE '%ZnanyLekarz%' OR subbrand IS NULL) --exclusion based on ALL SUBBRANDS list,to exclude all sub-brand contacts
    )

SELECT
  all_months.create_date,
  all_categories.country,
  all_categories.db_channel_short,
  all_categories.target,
  all_categories.verified,
  all_categories.spec_split_test,
  COUNT(DISTINCT rt.hubspot_id) AS total,
  COUNT(DISTINCT em.hubspot_id) AS total_new_marketing_db,
 coalesce(SUM(COUNT(DISTINCT rt.hubspot_id)) OVER (PARTITION BY
     all_categories.country,
    all_categories.verified,
    all_categories.db_channel_short,
    all_categories.target,
    all_categories.spec_split_test
    ORDER BY
      all_months.create_date ROWS BETWEEN UNBOUNDED PRECEDING
      AND CURRENT ROW
  ),0) AS rolling_total
FROM all_months cross join all_categories --we use cross join to create every possible combination of months/category combo
left join rolling_total_sub rt ON all_months.create_date = rt.create_date --we join the query twice using create date and entered marketing date to get the total number of contacts in the database in each month(rolling total using created date) and entered marketing database date to identify new marketing contacts each monht
  AND all_categories.db_channel_short = rt.db_channel_short
  AND all_categories.target = rt.target
  AND all_categories.verified = rt.verified
      AND all_categories.country= rt.country
    AND all_categories.spec_split_test = rt.spec_split_test
left join new_db EM ON all_months.create_date = EM.entered_marketing_database_at
  AND all_categories.db_channel_short = EM.db_channel_short
  AND all_categories.target = EM.target
  AND all_categories.verified = EM.verified
      AND all_categories.country= EM.country
    AND all_categories.spec_split_test = EM.spec_split_test
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6
ORDER BY
  1,
  2,
  3,
  4,
  5,
  6
