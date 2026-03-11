WITH base AS (
  SELECT
    hcl.hubspot_id AS hubspot_id,
    hcl.country AS country,
    hcl.createdate AS create_date,
    CASE WHEN hcl.country IN (
      'Colombia', 'Mexico', 'Italy', 'Brazil', 'Spain')
    AND COALESCE(
      hcl.contact_type_segment_test, 'UNKNOWN'
    ) IN ('UNKNOWN', 'DOCTOR', 'SECRETARY') THEN 'DOCTOR' WHEN hcl.country = 'Italy'
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
    ) THEN 'DOCTOR' ELSE 'OTHER' END AS target, --this field breakdowns which segments each country considers as Individual
    hcl.spec_split_test AS specialisation,
    CASE -- HS latest source known
    WHEN hs_analytics_source = 'SOCIAL_MEDIA'
    AND Lower(hs_analytics_source_data_1) IN(
      'facebook', 'instagram', 'linkedin'
    ) THEN 'Organic/Direct' WHEN hs_analytics_source = 'PAID_SOCIAL'
    AND Lower(hs_analytics_source_data_1) IN(
      'facebook', 'instagram', 'linkedin'
    ) THEN 'Paid' WHEN hs_analytics_source = 'PAID_SEARCH'
    AND (
      Lower(hs_analytics_source_data_1) IN ('yahoo', 'bing', 'google')
      OR Lower(hs_analytics_source_data_2) LIKE '%_%'
      OR Lower(hs_analytics_source_data_1) LIKE '%_%'
      OR Lower(hs_analytics_source_data_2) = 'google'
    ) THEN 'Paid' --WHEN date(mktg_events_tag_at) between DATEADD(day, -1, createdate) AND DATEADD(day, 1, createdate)
    WHEN doctor_facility___marketing_events_tag IS NOT NULL THEN 'Events' WHEN hs_analytics_source = 'ORGANIC_SEARCH'
    AND (
      Lower(hs_analytics_source_data_1) IN ('google', 'yahoo', 'bing')
      OR Lower(hs_analytics_source_data_2) IN ('google', 'bing', 'yahoo')
    ) THEN 'Organic/Direct' WHEN hcl.affiliate_source LIKE '%callcenter%' THEN 'Call Center' WHEN hs_analytics_source = 'DIRECT_TRAFFIC' THEN 'Organic/Direct' ELSE 'Organic/Direct' END AS db_channel_short, --this source breakdown is used in the Budget report, if needed elsewhere
    COALESCE(
      hcl.contact_type_segment_test, 'UNKNOWN'
    ) AS segment,
    hcl.sub_brand_wf_test AS subbrand,
    CASE WHEN email LIKE '%gdpr%' THEN 'gdpr' WHEN email LIKE '%deleted%' THEN 'deleted' WHEN email LIKE '%docplanner%' THEN 'invalid' WHEN email LIKE '%znanylekarz%' THEN 'invalid' WHEN email LIKE '%doctoralia%' THEN 'invalid' WHEN email LIKE '%miodottore%' THEN 'invalid' WHEN email LIKE '%deleted%' THEN 'deleted' WHEN email IS NULL THEN 'invalid' WHEN unsubscribed_from_all_emails_at IS NOT NULL
    AND zombies_engagement_level_workflows IS NOT NULL THEN 'unsub' WHEN doctor_facility___hard_bounced__wf IS NOT NULL
    AND unsubscribed_from_all_emails_at IS NULL
    AND zombies_engagement_level_workflows IS NOT NULL THEN 'hardbounce' ELSE 'valid' END AS email_status, -- this is based on ALL GENERAL EXCLUSIONS FROM COMMUNICATIONS list
    CASE WHEN (
      hcl.email IS NULL
      AND hcl.hubspot_id IS NOT NULL
    ) -- email null while the hs id is not null (contact exists)
    OR (
      hcl.saas_user_type_batch IN (
        'Clinic Secretary Accountant', 'Clinic Secretary Marketing',
        'Doctor Secretary', 'Clinic Secretary Receptionist',
        'Clinic Secretary', 'Clinic Secretary Head of reception'
      )
      AND hcl.source_doctor_id IS NULL
      AND hcl.source_facility_id IS NULL
      AND hcl.secretary_managed_facility_id_dwh_batch IS NOT NULL
      AND hcl.hubspot_id IS NOT NULL
    ) -- contact exists and is a secretary
    OR (
      hcl.secretaries_country_promoter_ninja_saas IS NOT NULL
      AND hcl.source_doctor_id IS NULL
      AND hcl.source_facility_id IS NULL
      AND hcl.hubspot_id IS NOT NULL
    ) -- contact exists and is a secretary
    OR facility_is_commercial
    OR is_commercial --commercial flag properties, dont cover all commercials but some
    OR lifecycle_stage IN (
      'WAITING', 'CLOSED-WON', 'ONBOARDING',
      'FARMING'
    ) --excluding commercial LCS
    --OR deal.deal_id IS NOT NULL --has an active deal in CJ tables - temporarily not considered as there are issues with CJ deals coming from HS
    OR (
      hcl.secretaries_country_promoter_ninja_saas IS NOT NULL
      AND hcl.last_saas_batch_update_batch >= CURRENT_DATE - 30
    ) --group 1 in Commercials list
    OR (
      hs_lead_status = 'UNQUALIFIED'
      AND hcl.country = 'Italy'
    ) --Italy excludes unqualified leads
    OR customer_status = 'active' --exclusion from Commercial List
    OR team.NAME LIKE '%BR_CS_ENT%' --exclusion from Commercil List
    OR team.NAME LIKE '%DE_CS_ENT%'
    OR team.NAME LIKE '%DE CS%'
    OR team.NAME LIKE '%ES_CS_ENT%'
    OR team.NAME LIKE '%IT_CS_ENT%'
    OR team.NAME LIKE '%PL_CS_ENT%'
    OR team.NAME LIKE '%MX_CS_ENT%' THEN true END AS marketing_base_excluded --compilation of different exclusions based on the Commercials and General Exclusion lists
  FROM
    dw.hs_contact_live hcl
    LEFT JOIN mart_cust_journey.cj_deal_month deal ON deal.hs_contact_id = hcl.hubspot_id
    AND deal_stage = 'Closed Won'
    AND is_current_stage
    AND stage_is_month_new--only selecting contact with active deals to exclude as commercials
    LEFT JOIN dw.hs_team team ON hcl.hubspot_team_id = team.hubspot_team_id
  WHERE
    hcl.is_deleted IS false
    AND hcl.country IN (
      'Argentina', 'Chile', 'Colombia',
      'Spain', 'Mexico', 'Brazil', 'Germany',
      'Italy', 'Poland', 'Turkey', 'Turkiye'
    )
)
SELECT
  country,
  target,
  count(DISTINCT hubspot_id)
FROM
  base
WHERE
  email_status = 'valid'
  AND marketing_base_excluded IS NOT TRUE and target IN ('DOCTOR','GP')
AND (subbrand LIKE '%Doctoralia%' OR subbrand LIKE '%MioDottore%' OR subbrand LIKE '%MioDottore%' OR subbrand LIKE '%jameda%'
OR subbrand LIKE '%DoktorTakvimi%' OR subbrand LIKE '%ZnanyLekarz%' OR subbrand IS NULL) --exclusion based on ALL SUBBRANDS list,to exclude all sub-brand contacts
GROUP BY
  country,target
