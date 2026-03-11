        --EOM marketing db  for PIgment with fac size for IT & Gipo 20.09
DROP TABLE IF EXISTS test.eom_mkt_db_pigment_tableau
CREATE TABLE test.eom_mkt_db_pigment_tableau AS
WITH all_months AS ( --here we select all months that historical information is available for (entered marketing database gives earlies values than create_date which we need)
    SELECT DISTINCT DATE_TRUNC('month', createdate::DATE) AS create_date
    FROM dw.hs_contact_live
    WHERE DATE_TRUNC('month', createdate::DATE) >= '2007-01-01'
    UNION DISTINCT
    SELECT DISTINCT DATE_TRUNC('month', entered_marketing_database_at_test) AS create_date
    FROM dw.hs_contact_live
    WHERE DATE_TRUNC('month', createdate) >= '2007-01-01')
    ,

all_categories AS ( --here we select all possible combinations for source/specialisation/target per country
    SELECT DISTINCT
        CASE WHEN hs_analytics_source = 'PAID_SOCIAL' AND LOWER(hs_analytics_source_data_1) IN ('facebook', 'instagram', 'linkedin') THEN 'Paid'
            WHEN hs_analytics_source = 'PAID_SEARCH' AND (LOWER(hs_analytics_source_data_1) IN ('yahoo', 'bing', 'google')
                OR LOWER(hs_analytics_source_data_2) LIKE '%_%' OR LOWER(hs_analytics_source_data_1) LIKE '%_%' OR LOWER(hs_analytics_source_data_2) = 'google') THEN 'Paid'
            WHEN doctor_facility___marketing_events_tag IS NOT NULL THEN 'Events'
            WHEN affiliate_source LIKE '%callcenter%' OR marketing_action_tag2 LIKE '%callcenter%' THEN 'Call Center'
            ELSE 'Organic' END AS db_channel_short,
        CASE WHEN COALESCE(hs.verified, hs.facility_verified) THEN 'Verified' ELSE 'Unverified' END AS verified,
        country,
         CASE WHEN
        --DP subbrand, doctors Co,mx,it,br,es
        (sub_brand_wf_test LIKE '%Doctoralia%' OR sub_brand_wf_test LIKE '%MioDottore%'
        OR sub_brand_wf_test LIKE '%MioDottore%' OR sub_brand_wf_test LIKE '%jameda%'
        OR sub_brand_wf_test LIKE '%DoktorTakvimi%'
        OR sub_brand_wf_test LIKE '%ZnanyLekarz%'
        OR sub_brand_wf_test LIKE '%ZnamyLekar%' OR sub_brand_wf_test IS NULL)
        AND COALESCE(contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY')
        AND country IN ('Colombia', 'Mexico', 'Italy', 'Brazil', 'Spain') THEN 'Individuals Core'
        --GPs
        WHEN (sub_brand_wf_test LIKE '%Doctoralia%' OR sub_brand_wf_test LIKE '%MioDottore%'
        OR sub_brand_wf_test LIKE '%MioDottore%' OR sub_brand_wf_test LIKE '%jameda%'
        OR sub_brand_wf_test LIKE '%DoktorTakvimi%'
        OR sub_brand_wf_test LIKE '%ZnanyLekarz%'
        OR sub_brand_wf_test LIKE '%ZnamyLekar%' OR sub_brand_wf_test IS NULL)
        AND country IN ('Italy') AND COALESCE(contact_type_segment_test, 'UNKNOWN') IN ('GENERAL PRACTITIONER', 'GENERAL PRACTITIONER & DOCTOR') THEN 'Individuals GPs'
        --DP subbrand, doctors Poland
        WHEN (sub_brand_wf_test LIKE '%Doctoralia%' OR sub_brand_wf_test LIKE '%MioDottore%'
        OR sub_brand_wf_test LIKE '%MioDottore%' OR sub_brand_wf_test LIKE '%jameda%'
        OR sub_brand_wf_test LIKE '%DoktorTakvimi%'
        OR sub_brand_wf_test LIKE '%ZnanyLekarz%'
        OR sub_brand_wf_test LIKE '%ZnamyLekar%' OR sub_brand_wf_test IS NULL)
        AND country = 'Poland' AND COALESCE(contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY', 'NURSES') THEN 'Individuals Core'
        --dp subbrand, only Ind segment countries
        WHEN (sub_brand_wf_test LIKE '%Doctoralia%' OR sub_brand_wf_test LIKE '%MioDottore%'
        OR sub_brand_wf_test LIKE '%MioDottore%' OR sub_brand_wf_test LIKE '%jameda%'
        OR sub_brand_wf_test LIKE '%DoktorTakvimi%'
        OR sub_brand_wf_test LIKE '%ZnanyLekarz%'
        OR sub_brand_wf_test LIKE '%ZnamyLekar%' OR sub_brand_wf_test IS NULL OR sub_brand_wf_test = 'Noa')
        AND country IN ('Turkey', 'Argentina', 'Chile', 'Germany', 'Turkiye')
        AND COALESCE(contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY', 'DOCTOR&FACILITY - 2IN1', 'FACILITY', 'MARKETING') THEN 'Individuals Core'
        --DP subbrand, facilities
        WHEN (sub_brand_wf_test LIKE '%Doctoralia%' OR sub_brand_wf_test LIKE '%MioDottore%'
        OR sub_brand_wf_test LIKE '%MioDottore%' OR sub_brand_wf_test LIKE '%jameda%'
        OR sub_brand_wf_test LIKE '%DoktorTakvimi%'
        OR sub_brand_wf_test LIKE '%ZnanyLekarz%'
        OR sub_brand_wf_test LIKE '%ZnamyLekar%' OR sub_brand_wf_test IS NULL OR sub_brand_wf_test = 'Noa' OR sub_brand_wf_test LIKE '%Noa%')
        AND COALESCE(contact_type_segment_test, 'UNKNOWN') IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'Clinics PRS'
        WHEN sub_brand_wf_test = 'Noa' THEN 'Individuals Core'
        WHEN country IN ('Brazil', 'Poland', 'Italy', 'Spain') AND
            (sub_brand_wf_test IN ('MyDr', 'Feegow', 'Clinic Cloud', 'Gipo', 'Tuotempo')
            OR sub_brand_wf_test LIKE '%Feegow%'
            OR sub_brand_wf_test LIKE '%Gipo%'
            OR sub_brand_wf_test LIKE '%MyDr%'
            OR sub_brand_wf_test LIKE '%Clinic Cloud%'
            OR sub_brand_wf_test LIKE '%Tuotempo%') THEN 'Clinics PMS' --counted as CLinics DB, all contact types except Patient and student
        WHEN sub_brand_wf_test LIKE '%Noa%' THEN 'Individuals Core' --edge cases of Noa + PMS in non-pms countries
        ELSE 'None' END AS segment_funnel,
            CASE WHEN
        --DP subbrand, Paramedics spec countries
         (sub_brand_wf_test LIKE '%Doctoralia%' OR sub_brand_wf_test LIKE '%MioDottore%'
        OR sub_brand_wf_test LIKE '%MioDottore%' OR sub_brand_wf_test LIKE '%jameda%'
        OR sub_brand_wf_test LIKE '%DoktorTakvimi%'
        OR sub_brand_wf_test LIKE '%ZnanyLekarz%'
        OR sub_brand_wf_test LIKE '%ZnamyLekar%' OR sub_brand_wf_test IS NULL)
        AND (spec_split_test = 'Paramedical' OR spec_split_test IS NULL)
        AND ((country IN  ('Brazil', 'Mexico') AND COALESCE(contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY', 'GENERAL PRACTITIONER', 'GENERAL PRACTITIONER & DOCTOR'))
        OR (country = 'Poland' AND COALESCE(contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY', 'NURSES'))
        OR (country = 'Chile' AND COALESCE(contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY', 'DOCTOR&FACILITY - 2IN1', 'FACILITY', 'MARKETING'))) THEN 'Paramedics'
         WHEN--Italy using CS logic for Paramedics
         (sub_brand_wf_test LIKE '%Doctoralia%' OR sub_brand_wf_test LIKE '%MioDottore%'
        OR sub_brand_wf_test LIKE '%MioDottore%' OR sub_brand_wf_test LIKE '%jameda%'
        OR sub_brand_wf_test LIKE '%DoktorTakvimi%'
        OR sub_brand_wf_test LIKE '%ZnanyLekarz%'
        OR sub_brand_wf_test LIKE '%ZnamyLekar%' OR sub_brand_wf_test IS NULL)
        AND country = 'Italy' AND COALESCE(contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY', 'GENERAL PRACTITIONER', 'GENERAL PRACTITIONER & DOCTOR')
        AND (s.type != 'medical' OR s.type IS NULL) THEN 'Paramedics'
        WHEN (sub_brand_wf_test LIKE '%Doctoralia%' OR sub_brand_wf_test LIKE '%MioDottore%'
        OR sub_brand_wf_test LIKE '%MioDottore%' OR sub_brand_wf_test LIKE '%jameda%'
        OR sub_brand_wf_test LIKE '%DoktorTakvimi%'
        OR sub_brand_wf_test LIKE '%ZnanyLekarz%'
        OR sub_brand_wf_test LIKE '%ZnamyLekar%' OR sub_brand_wf_test IS NULL)
        AND COALESCE(contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY', 'GENERAL PRACTITIONER', 'GENERAL PRACTITIONER & DOCTOR')
        AND country IN ('Colombia', 'Italy', 'Brazil', 'Spain', 'Mexico') THEN 'Doctors'
       --DP subbrand, doctors Poland
        WHEN (sub_brand_wf_test LIKE '%Doctoralia%' OR sub_brand_wf_test LIKE '%MioDottore%'
        OR sub_brand_wf_test LIKE '%MioDottore%' OR sub_brand_wf_test LIKE '%jameda%'
        OR sub_brand_wf_test LIKE '%DoktorTakvimi%'
        OR sub_brand_wf_test LIKE '%ZnanyLekarz%'
        OR sub_brand_wf_test LIKE '%ZnamyLekar%' OR sub_brand_wf_test IS NULL)
        AND country = 'Poland' AND COALESCE(contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY', 'NURSES') THEN 'Doctors'
        --dp subbrand, only Ind segment countries
        WHEN (sub_brand_wf_test LIKE '%Doctoralia%' OR sub_brand_wf_test LIKE '%MioDottore%'
        OR sub_brand_wf_test LIKE '%MioDottore%' OR sub_brand_wf_test LIKE '%jameda%'
        OR sub_brand_wf_test LIKE '%DoktorTakvimi%'
        OR sub_brand_wf_test LIKE '%ZnanyLekarz%'
        OR sub_brand_wf_test LIKE '%ZnamyLekar%' OR sub_brand_wf_test IS NULL OR sub_brand_wf_test IS NULL OR sub_brand_wf_test = 'Noa' OR sub_brand_wf_test LIKE '%Noa%')
        AND country IN ('Turkey', 'Argentina', 'Chile', 'Germany', 'Turkiye')
        AND COALESCE(contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY', 'DOCTOR&FACILITY - 2IN1', 'FACILITY', 'MARKETING') THEN 'Doctors'
         WHEN (sub_brand_wf_test = 'Noa'
        OR sub_brand_wf_test LIKE '%Noa%')
        AND country IN ('Turkey', 'Argentina', 'Chile', 'Germany', 'Turkiye')
        AND COALESCE(contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY', 'DOCTOR&FACILITY - 2IN1', 'FACILITY', 'MARKETING') THEN 'Noa Notes'
        --DP subbrand, facilities
        WHEN (sub_brand_wf_test LIKE '%Doctoralia%' OR sub_brand_wf_test LIKE '%MioDottore%'
        OR sub_brand_wf_test LIKE '%MioDottore%' OR sub_brand_wf_test LIKE '%jameda%'
        OR sub_brand_wf_test LIKE '%DoktorTakvimi%'
        OR sub_brand_wf_test LIKE '%ZnanyLekarz%'
        OR sub_brand_wf_test LIKE '%ZnamyLekar%' OR sub_brand_wf_test IS NULL)
        AND COALESCE(contact_type_segment_test, 'UNKNOWN') IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN
            CASE WHEN country = 'Italy' AND LOWER(doctor_specialisation_es_manual_forms) LIKE '%dentist%' THEN 'Small' --applies both for CA and Gipo--keeping this in the query for now but not setting live, waiting for HS saturday backfill
            WHEN country = 'Italy' AND (facility_number_of_doctors IN (1, 2, 3, 4, 5) OR facility_number_of_doctors = '2-3') THEN 'Small' --for IT CA
            WHEN country = 'Italy' AND (facility_number_of_doctors = 0 OR facility_number_of_doctors IS NULL) THEN 'Unknown'
            WHEN country = 'Italy' THEN 'Medium' --for IT CA, this catches weird ranges in the faciliyu number property
            WHEN facility_size LIKE ('%Individual%') OR facility_size LIKE ('%Small%') THEN 'Small'
            WHEN facility_size LIKE ('%Large%') OR facility_size LIKE ('%Mid%') THEN 'Medium'
            ELSE 'Unknown' END
        WHEN sub_brand_wf_test = 'Noa' THEN 'Noa Notes'
        WHEN country IN ('Brazil', 'Poland', 'Italy', 'Spain') AND
            (sub_brand_wf_test IN ('MyDr', 'Feegow', 'Clinic Cloud', 'Gipo', 'Tuotempo')
            OR sub_brand_wf_test LIKE '%Feegow%'
            OR sub_brand_wf_test LIKE '%Gipo%'
            OR sub_brand_wf_test LIKE '%MyDr%'
            OR sub_brand_wf_test LIKE '%Clinic Cloud%'
            OR sub_brand_wf_test LIKE '%Tuotempo%') THEN
            CASE WHEN (sub_brand_wf_test = 'Gipo' OR sub_brand_wf_test LIKE '%Gipo%') AND (LOWER(doctor_specialisation_es_manual_forms) LIKE '%dentist%' OR facility_number_of_doctors IN (1, 2, 3, 4, 5) OR facility_number_of_doctors = '2-3') THEN 'Small'
                WHEN (sub_brand_wf_test = 'Gipo' OR sub_brand_wf_test LIKE '%Gipo%') AND (facility_number_of_doctors = 0 OR facility_number_of_doctors IS NULL) THEN 'Unknown'
                WHEN (sub_brand_wf_test = 'Gipo' OR sub_brand_wf_test LIKE '%Gipo%') THEN 'Medium'
                WHEN (contact_type_segment_test = 'DOCTOR' AND country = 'Spain')
                OR ((facility_size LIKE ('%Individual%') OR facility_size LIKE ('%Small%')) AND country IN ('Italy', 'Brazil'))
                OR (country = 'Poland' AND (contact_type_segment_test IS NULL OR contact_type_segment_test NOT IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1')))
                THEN 'Small' --for CC, based on segment, logic ok Clement
            WHEN country IN ('Spain', 'Poland') OR ((facility_size LIKE ('%Large%') OR facility_size LIKE ('%Mid%')) AND country IN ('Italy', 'Brazil')) THEN 'Medium'
            ELSE 'Unknown' END --counted as CLinics DB, all contact types except Patient and student
         WHEN sub_brand_wf_test LIKE '%Noa%' THEN 'Noa Notes'
        ELSE 'None' END AS target
    FROM dw.hs_contact_live hs
     LEFT JOIN dw.country c ON c.name = hs.country
    LEFT JOIN dw.doctor d ON d.source_doctor_id = hs.source_doctor_id
    AND d.country_code = c.country_code
    AND d.is_deleted IS FALSE AND d.is_test IS FALSE
    AND d.origin = 'DP'
    LEFT JOIN dw.doctor_specialization ds
        ON ds.doctor_id = d.doctor_id
            AND ds.country_code = d.country_code
            AND ds.is_deleted IS FALSE AND ds.priority = 1
    LEFT JOIN dw.specialization s
        ON ds.specialization_id = s.specialization_id
            AND d.country_code = s.country_code
    WHERE country IN ('Chile', 'Colombia', 'Spain', 'Mexico', 'Brazil', 'Germany', 'Italy', 'Poland', 'Turkey', 'Turkiye', 'Argentina')
 UNION ALL
    SELECT -- additional category combo for Chile as we had no such leads in the past but targets for this combo exist
        'Call Center' AS db_channel_short,
        'Verified' AS verified,
         'Chile' AS country,
          'Individuals Core' AS segment_funnel,
        'Paramedics' AS target
         UNION ALL
    SELECT -- additional category combo for Chile as we had no such leads in the past but targets for this combo exist
        'Call Center' AS db_channel_short,
        'Unverified' AS verified,
         'Chile' AS country,
          'Individuals Core' AS segment_funnel,
        'Doctors' AS target
    UNION ALL
    SELECT -- additional category combo for Chile as we had no such leads in the past but targets for this combo exist
        'Events' AS db_channel_short,
        'Unverified' AS verified,
         'Germany' AS country,
          'Individuals Core' AS segment_funnel,
        'Noa Notes' AS target

    ),

subquery AS (--in some cases we pull live values (like optout status for DE and entered marketing DB value, due to log info being too recent and not having enough/correct historical data). Overall, historical data gets priority where not NULL
    SELECT
        hcl.hubspot_id AS hubspot_id,
        hcl.country AS country,
        DATE_TRUNC('month', hcl.entered_marketing_database_at_test) AS create_date,
        DATE_TRUNC('month', hcl.entered_marketing_database_at_test) AS entered_marketing_database_at,
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
        CASE WHEN hcl.hs_analytics_source = 'PAID_SOCIAL' AND LOWER(hcl.hs_analytics_source_data_1) IN ('facebook', 'instagram', 'linkedin') THEN 'Paid'
            WHEN hcl.hs_analytics_source = 'PAID_SEARCH' AND (LOWER(hcl.hs_analytics_source_data_1) IN ('yahoo', 'bing', 'google')
                OR LOWER(hcl.hs_analytics_source_data_2) LIKE '%_%' OR LOWER(hcl.hs_analytics_source_data_1) LIKE '%_%' OR LOWER(hcl.hs_analytics_source_data_2) = 'google') THEN 'Paid'
            WHEN hcl.doctor_facility___marketing_events_tag IS NOT NULL THEN 'Events'
            WHEN LOWER(hcl.affiliate_source) LIKE '%callcenter%' OR LOWER(hcl.marketing_action_tag2) LIKE '%callcenter%' THEN 'Call Center'
            ELSE 'Organic' END AS db_channel_short,
          CASE WHEN (hcl.sub_brand_wf_test LIKE '%Doctoralia%' OR hcl.sub_brand_wf_test LIKE '%MioDottore%' OR hcl.sub_brand_wf_test LIKE '%MioDottore%' OR hcl.sub_brand_wf_test LIKE '%jameda%' OR hcl.sub_brand_wf_test LIKE '%DoktorTakvimi%' OR hcl.sub_brand_wf_test LIKE '%ZnanyLekarz%' OR sub_brand_wf_test LIKE '%ZnamyLekar%' OR hcl.sub_brand_wf_test IS NULL) THEN  'Docplanner' --DP database, later split into Clinics & Individuals via contact type
            WHEN hcl.sub_brand_wf_test = 'Noa' THEN 'Noa' --Noa database, later split into Clinics & Individuals via contact type
            WHEN hcl.sub_brand_wf_test IN ('MyDr', 'Feegow', 'Clinic Cloud', 'Gipo', 'Tuotempo') OR
            hcl.sub_brand_wf_test LIKE '%Feegow%' OR hcl.sub_brand_wf_test LIKE '%Gipo%' OR hcl.sub_brand_wf_test LIKE '%MyDr%' OR hcl.sub_brand_wf_test LIKE '%Clinic Cloud%' OR hcl.sub_brand_wf_test LIKE '%Tuotempo%' THEN 'PMS' --counted as CLinics DB, all contact types except Patient and student
        ELSE hcl.sub_brand_wf_test END AS subbrand,
        COALESCE(LOWER(hcl.affiliate_source) LIKE '%callcenter%' OR LOWER(hcl.marketing_action_tag2) LIKE '%callcenter%', FALSE) AS call_center_flag,
       CASE WHEN
        --DP subbrand, doctors Co,mx,it,br,es
        (hcl.sub_brand_wf_test LIKE '%Doctoralia%' OR hcl.sub_brand_wf_test LIKE '%MioDottore%'
        OR hcl.sub_brand_wf_test LIKE '%MioDottore%' OR hcl.sub_brand_wf_test LIKE '%jameda%'
        OR hcl.sub_brand_wf_test LIKE '%DoktorTakvimi%'
        OR hcl.sub_brand_wf_test LIKE '%ZnanyLekarz%'
        OR hcl.sub_brand_wf_test LIKE '%ZnamyLekar%' OR hcl.sub_brand_wf_test IS NULL)
        AND COALESCE(hcl.contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY')
        AND hcl.country IN ('Colombia', 'Mexico', 'Italy', 'Brazil', 'Spain') THEN 'Individuals Core'
        --GPs
        WHEN (hcl.sub_brand_wf_test LIKE '%Doctoralia%' OR hcl.sub_brand_wf_test LIKE '%MioDottore%'
        OR hcl.sub_brand_wf_test LIKE '%MioDottore%' OR hcl.sub_brand_wf_test LIKE '%jameda%'
        OR hcl.sub_brand_wf_test LIKE '%DoktorTakvimi%'
        OR hcl.sub_brand_wf_test LIKE '%ZnanyLekarz%'
        OR hcl.sub_brand_wf_test LIKE '%ZnamyLekar%' OR hcl.sub_brand_wf_test IS NULL)
        AND hcl.country IN ('Italy') AND COALESCE(hcl.contact_type_segment_test, 'UNKNOWN') IN ('GENERAL PRACTITIONER', 'GENERAL PRACTITIONER & DOCTOR') THEN 'Individuals GPs'
        --DP subbrand, doctors Poland
        WHEN (hcl.sub_brand_wf_test LIKE '%Doctoralia%' OR hcl.sub_brand_wf_test LIKE '%MioDottore%'
        OR hcl.sub_brand_wf_test LIKE '%MioDottore%' OR hcl.sub_brand_wf_test LIKE '%jameda%'
        OR hcl.sub_brand_wf_test LIKE '%DoktorTakvimi%'
        OR hcl.sub_brand_wf_test LIKE '%ZnanyLekarz%'
        OR hcl.sub_brand_wf_test LIKE '%ZnamyLekar%' OR hcl.sub_brand_wf_test IS NULL)
        AND hcl.country = 'Poland' AND COALESCE(hcl.contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY', 'NURSES') THEN 'Individuals Core'
        --dp subbrand, only Ind segment countries
        WHEN (hcl.sub_brand_wf_test LIKE '%Doctoralia%' OR hcl.sub_brand_wf_test LIKE '%MioDottore%'
        OR hcl.sub_brand_wf_test LIKE '%MioDottore%' OR hcl.sub_brand_wf_test LIKE '%jameda%'
        OR hcl.sub_brand_wf_test LIKE '%DoktorTakvimi%'
        OR hcl.sub_brand_wf_test LIKE '%ZnanyLekarz%'
        OR hcl.sub_brand_wf_test LIKE '%ZnamyLekar%' OR hcl.sub_brand_wf_test IS NULL OR sub_brand_wf_test IS NULL OR sub_brand_wf_test = 'Noa' OR sub_brand_wf_test LIKE '%Noa%')
        AND hcl.country IN ('Turkey', 'Argentina', 'Chile', 'Germany', 'Turkiye')
        AND COALESCE(hcl.contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY', 'DOCTOR&FACILITY - 2IN1', 'FACILITY', 'MARKETING') THEN 'Individuals Core'
        --DP subbrand, facilities
        WHEN (hcl.sub_brand_wf_test LIKE '%Doctoralia%' OR hcl.sub_brand_wf_test LIKE '%MioDottore%'
        OR hcl.sub_brand_wf_test LIKE '%MioDottore%' OR hcl.sub_brand_wf_test LIKE '%jameda%'
        OR hcl.sub_brand_wf_test LIKE '%DoktorTakvimi%'
        OR hcl.sub_brand_wf_test LIKE '%ZnanyLekarz%'
        OR hcl.sub_brand_wf_test LIKE '%ZnamyLekar%' OR hcl.sub_brand_wf_test IS NULL OR hcl.sub_brand_wf_test = 'Noa')
        AND COALESCE(hcl.contact_type_segment_test, 'UNKNOWN') IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'Clinics PRS'
        WHEN hcl.sub_brand_wf_test = 'Noa' THEN 'Individuals Core'
        WHEN hcl.country IN ('Brazil', 'Poland', 'Italy', 'Spain') AND
            (hcl.sub_brand_wf_test IN ('MyDr', 'Feegow', 'Clinic Cloud', 'Gipo', 'Tuotempo')
            OR hcl.sub_brand_wf_test LIKE '%Feegow%'
            OR hcl.sub_brand_wf_test LIKE '%Gipo%'
            OR hcl.sub_brand_wf_test LIKE '%MyDr%'
            OR hcl.sub_brand_wf_test LIKE '%Clinic Cloud%'
            OR hcl.sub_brand_wf_test LIKE '%Tuotempo%') THEN 'Clinics PMS' --counted as CLinics DB, all contact types except Patient and student
        WHEN hcl.sub_brand_wf_test LIKE '%Noa%' THEN 'Individuals Core' --edge cases of Noa + PMS in non-pms countries
        ELSE 'None' END AS segment_funnel,
            CASE WHEN
        --DP subbrand, Paramedics spec countries
         (hcl.sub_brand_wf_test LIKE '%Doctoralia%' OR hcl.sub_brand_wf_test LIKE '%MioDottore%'
        OR hcl.sub_brand_wf_test LIKE '%MioDottore%' OR hcl.sub_brand_wf_test LIKE '%jameda%'
        OR hcl.sub_brand_wf_test LIKE '%DoktorTakvimi%'
        OR hcl.sub_brand_wf_test LIKE '%ZnanyLekarz%'
        OR hcl.sub_brand_wf_test LIKE '%ZnamyLekar%' OR hcl.sub_brand_wf_test IS NULL)
        AND (hcl.spec_split_test = 'Paramedical' OR hcl.spec_split_test IS NULL)
        AND ((hcl.country IN  ('Brazil', 'Mexico') AND COALESCE(hcl.contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY', 'GENERAL PRACTITIONER', 'GENERAL PRACTITIONER & DOCTOR'))
        OR (hcl.country = 'Poland' AND COALESCE(hcl.contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY', 'NURSES'))
        OR (hcl.country = 'Chile' AND COALESCE(hcl.contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY', 'DOCTOR&FACILITY - 2IN1', 'FACILITY', 'MARKETING'))) THEN 'Paramedics'
        WHEN--Italy using CS logic for Paramedics
         (hcl.sub_brand_wf_test LIKE '%Doctoralia%' OR hcl.sub_brand_wf_test LIKE '%MioDottore%'
        OR hcl.sub_brand_wf_test LIKE '%MioDottore%' OR hcl.sub_brand_wf_test LIKE '%jameda%'
        OR hcl.sub_brand_wf_test LIKE '%DoktorTakvimi%'
        OR hcl.sub_brand_wf_test LIKE '%ZnanyLekarz%'
        OR hcl.sub_brand_wf_test LIKE '%ZnamyLekar%' OR hcl.sub_brand_wf_test IS NULL)
        AND hcl.country = 'Italy' AND COALESCE(hcl.contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY', 'GENERAL PRACTITIONER', 'GENERAL PRACTITIONER & DOCTOR')
        AND (s.type != 'medical' OR s.type IS NULL) THEN 'Paramedics'
        WHEN (hcl.sub_brand_wf_test LIKE '%Doctoralia%' OR hcl.sub_brand_wf_test LIKE '%MioDottore%'
        OR hcl.sub_brand_wf_test LIKE '%MioDottore%' OR hcl.sub_brand_wf_test LIKE '%jameda%'
        OR hcl.sub_brand_wf_test LIKE '%DoktorTakvimi%'
        OR hcl.sub_brand_wf_test LIKE '%ZnanyLekarz%'
        OR hcl.sub_brand_wf_test LIKE '%ZnamyLekar%' OR hcl.sub_brand_wf_test IS NULL)
        AND COALESCE(hcl.contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY', 'GENERAL PRACTITIONER', 'GENERAL PRACTITIONER & DOCTOR')
        AND hcl.country IN ('Colombia', 'Italy', 'Brazil', 'Spain', 'Mexico') THEN 'Doctors'
       --DP subbrand, doctors Poland
        WHEN (hcl.sub_brand_wf_test LIKE '%Doctoralia%' OR hcl.sub_brand_wf_test LIKE '%MioDottore%'
        OR hcl.sub_brand_wf_test LIKE '%MioDottore%' OR hcl.sub_brand_wf_test LIKE '%jameda%'
        OR hcl.sub_brand_wf_test LIKE '%DoktorTakvimi%'
        OR hcl.sub_brand_wf_test LIKE '%ZnanyLekarz%'
        OR hcl.sub_brand_wf_test LIKE '%ZnamyLekar%' OR hcl.sub_brand_wf_test IS NULL)
        AND hcl.country = 'Poland' AND COALESCE(hcl.contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY', 'NURSES') THEN 'Doctors'
        --dp subbrand, only Ind segment countries
        WHEN (hcl.sub_brand_wf_test LIKE '%Doctoralia%' OR hcl.sub_brand_wf_test LIKE '%MioDottore%'
        OR hcl.sub_brand_wf_test LIKE '%MioDottore%' OR hcl.sub_brand_wf_test LIKE '%jameda%'
        OR hcl.sub_brand_wf_test LIKE '%DoktorTakvimi%'
        OR hcl.sub_brand_wf_test LIKE '%ZnanyLekarz%'
        OR hcl.sub_brand_wf_test LIKE '%ZnamyLekar%' OR hcl.sub_brand_wf_test IS NULL OR sub_brand_wf_test IS NULL)
        AND hcl.country IN ('Turkey', 'Argentina', 'Chile', 'Germany', 'Turkiye')
        AND COALESCE(hcl.contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY', 'DOCTOR&FACILITY - 2IN1', 'FACILITY', 'MARKETING') THEN 'Doctors'
         WHEN (hcl.sub_brand_wf_test = 'Noa'
        OR hcl.sub_brand_wf_test LIKE '%Noa%')
        AND hcl.country IN ('Turkey', 'Argentina', 'Chile', 'Germany', 'Turkiye')
        AND COALESCE(hcl.contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY', 'DOCTOR&FACILITY - 2IN1', 'FACILITY', 'MARKETING') THEN 'Noa Notes'
        --DP subbrand, facilities
        WHEN (hcl.sub_brand_wf_test LIKE '%Doctoralia%' OR hcl.sub_brand_wf_test LIKE '%MioDottore%'
        OR hcl.sub_brand_wf_test LIKE '%MioDottore%' OR hcl.sub_brand_wf_test LIKE '%jameda%'
        OR hcl.sub_brand_wf_test LIKE '%DoktorTakvimi%'
        OR hcl.sub_brand_wf_test LIKE '%ZnanyLekarz%'
        OR hcl.sub_brand_wf_test LIKE '%ZnamyLekar%' OR hcl.sub_brand_wf_test IS NULL)
        AND COALESCE(hcl.contact_type_segment_test, 'UNKNOWN') IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN
            CASE WHEN hcl.country = 'Italy' AND LOWER(hcl.doctor_specialisation_es_manual_forms) LIKE '%dentist%' THEN 'Small' --applies both for CA and Gipo--keeping this in the query for now but not setting live, waiting for HS saturday backfill
            WHEN hcl.country = 'Italy' AND (hcl.facility_number_of_doctors IN (1, 2, 3, 4, 5) OR hcl.facility_number_of_doctors = '2-3') THEN 'Small' --for IT CA
            WHEN hcl.country = 'Italy' AND (hcl.facility_number_of_doctors = 0 OR hcl.facility_number_of_doctors IS NULL) THEN 'Unknown'
            WHEN hcl.country = 'Italy' THEN 'Medium' --for IT CA, this catches weird ranges in the faciliyu number property
            WHEN hcl.facility_size LIKE ('%Individual%') OR hcl.facility_size LIKE ('%Small%') THEN 'Small'
            WHEN hcl.facility_size LIKE ('%Large%') OR hcl.facility_size LIKE ('%Mid%') THEN 'Medium'
            ELSE 'Unknown' END
        WHEN hcl.sub_brand_wf_test = 'Noa' THEN 'Noa Notes'
        WHEN hcl.country IN ('Brazil', 'Poland', 'Italy', 'Spain') AND
            (hcl.sub_brand_wf_test IN ('MyDr', 'Feegow', 'Clinic Cloud', 'Gipo', 'Tuotempo')
            OR hcl.sub_brand_wf_test LIKE '%Feegow%'
            OR hcl.sub_brand_wf_test LIKE '%Gipo%'
            OR hcl.sub_brand_wf_test LIKE '%MyDr%'
            OR hcl.sub_brand_wf_test LIKE '%Clinic Cloud%'
            OR hcl.sub_brand_wf_test LIKE '%Tuotempo%') THEN
            CASE WHEN (hcl.sub_brand_wf_test = 'Gipo' OR hcl.sub_brand_wf_test LIKE '%Gipo%') AND (LOWER(hcl.doctor_specialisation_es_manual_forms) LIKE '%dentist%' OR hcl.facility_number_of_doctors IN (1, 2, 3, 4, 5) OR hcl.facility_number_of_doctors = '2-3') THEN 'Small'
                WHEN (hcl.sub_brand_wf_test = 'Gipo' OR hcl.sub_brand_wf_test LIKE '%Gipo%') AND (hcl.facility_number_of_doctors = 0 OR hcl.facility_number_of_doctors IS NULL) THEN 'Unknown'
                WHEN (hcl.sub_brand_wf_test = 'Gipo' OR hcl.sub_brand_wf_test LIKE '%Gipo%') THEN 'Medium'
                WHEN (hcl.contact_type_segment_test = 'DOCTOR' AND hcl.country = 'Spain')
                OR ((hcl.facility_size LIKE ('%Individual%') OR hcl.facility_size LIKE ('%Small%')) AND hcl.country IN ('Italy', 'Brazil'))
                OR (hcl.country = 'Poland' AND (hcl.contact_type_segment_test IS NULL OR hcl.contact_type_segment_test NOT IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1')))
                THEN 'Small' --for CC, based on segment, logic ok Clement
            WHEN hcl.country IN ('Spain', 'Poland') OR ((hcl.facility_size LIKE ('%Large%') OR hcl.facility_size LIKE ('%Mid%')) AND hcl.country IN ('Italy', 'Brazil')) THEN 'Medium'
            ELSE 'Unknown' END --counted as CLinics DB, all contact types except Patient and student
         WHEN hcl.sub_brand_wf_test LIKE '%Noa%' AND entered_marketing_database_at_test >= '2024-10-01' THEN 'Noa Notes'
        ELSE 'None' END AS target,
        COALESCE(hcl.contact_type_segment_test, 'UNKNOWN') AS segment,
        CASE WHEN COALESCE(hcl.verified, hcl.facility_verified) THEN 'Verified' ELSE 'Unverified' END AS verified,
        CASE WHEN (hcl.email IS NULL AND hcl.hubspot_id IS NOT NULL) -- email null while the hs id is not null (contact exists)
            OR (hcl.saas_user_type_batch IN ('Clinic Secretary Accountant', 'Clinic Secretary Marketing', 'Doctor Secretary', 'Clinic Secretary Receptionist', 'Clinic Secretary', 'Clinic Secretary Head of reception')
                AND hcl.source_doctor_id IS NULL AND hcl.source_facility_id IS NULL AND hcl.secretary_managed_facility_id_dwh_batch IS NOT NULL AND hcl.hubspot_id IS NOT NULL) -- contact exists and is a secretary
            OR (hcl.secretaries_country_promoter_ninja_saas IS NOT NULL AND hcl.source_doctor_id IS NULL AND hcl.source_facility_id IS NULL AND hcl.hubspot_id IS NOT NULL) -- contact exists and is a secretary
            OR (hcl.hs_lead_status = 'UNQUALIFIED' AND hcl.country = 'Italy') --Italy excludes unqualified leads
            OR (hcl.country = 'Brazil' AND spec_split_test = 'Bad Paramedical')
            THEN TRUE END AS marketing_base_excluded,
        ROW_NUMBER() OVER (PARTITION BY hcl.hubspot_id ORDER BY hcl.dw_updated_at DESC) AS row_per_lcs,
        hcl.member_customers_list AS is_customer_now,
        hcl.all_comms_excl_list_member AS communication_excluded
    FROM dw.hs_contact_live hcl
     LEFT JOIN dw.country c ON c.name = hcl.country
    LEFT JOIN dw.doctor d ON d.source_doctor_id = hcl.source_doctor_id
    AND d.country_code = c.country_code
    AND d.is_deleted IS FALSE AND d.is_test IS FALSE
    AND d.origin = 'DP'
    LEFT JOIN dw.doctor_specialization ds
        ON ds.doctor_id = d.doctor_id
            AND ds.country_code = d.country_code
            AND ds.is_deleted IS FALSE AND ds.priority = 1
    LEFT JOIN dw.specialization s
        ON ds.specialization_id = s.specialization_id
            AND d.country_code = s.country_code
    WHERE hcl.is_deleted IS FALSE AND hcl.country IN ('Spain', 'Chile', 'Colombia', 'Germany', 'Italy', 'Mexico', 'Poland', 'Turkiye', 'Brazil')
        AND hcl.entered_marketing_database_at_test IS NOT NULL
    QUALIFY email_status = 'valid' AND (hcl.contact_type_segment_test NOT IN ('PATIENT', 'STUDENTS') OR hcl.contact_type_segment_test IS NULL)
        AND marketing_base_excluded IS NULL
        AND row_per_lcs = 1
),

rolling_total_sub AS (
    SELECT *,
        CASE WHEN segment_funnel IN ('Individuals Core', 'Individuals GPs', 'Clinics PRS') AND target IN ('Doctors', 'Paramedics', 'Medium', 'Small', 'Unknown') AND communication_excluded = 'Yes' THEN TRUE
    ELSE FALSE END AS comm_exclusion,
         CASE WHEN segment_funnel IN ('Clinics PRS', 'Clinics PMS') AND target IN ('Medium', 'Small', 'Unknown') THEN 'Unverified' ELSE verified END AS verified_aux,
         CASE WHEN segment_funnel = 'Individuals GPs' THEN 'Doctors' ELSE target END AS target_aux --ok Juli, Juan & Gaela
FROM subquery
    WHERE (is_customer_now IS NULL OR is_customer_now = 'No')
QUALIFY comm_exclusion IS FALSE --exclusion list property excludes pure subbrand so we cant apply it to pure PMS & Noa
)


SELECT
    am.create_date AS date,
    'Inbound' AS lead_source,
    ac.country AS market,
    ac.segment_funnel AS segment_funnel,
    ac.target AS target,
    ac.db_channel_short AS acquisition_channel,
    ac.verified AS verified,
    COALESCE(SUM(COUNT(DISTINCT rt.hubspot_id)) OVER (PARTITION BY ac.country, ac.segment_funnel, ac.target, ac.db_channel_short, ac.verified ORDER BY am.create_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0) AS rolling_total
FROM all_months am
CROSS JOIN all_categories ac
LEFT JOIN rolling_total_sub rt
        ON am.create_date = rt.entered_marketing_database_at
        AND ac.segment_funnel = rt.segment_funnel
        AND ac.db_channel_short = rt.db_channel_short
        AND ac.target = rt.target_aux
        AND ac.verified = rt.verified_aux
        AND ac.country = rt.country
WHERE ac.segment_funnel != 'None'
GROUP BY 1, 2, 3, 4, 5, 6, 7
QUALIFY ac.target IS NOT NULL --AND NOT (rolling_total = 0)
AND am.create_date >= '2023-12-01'
ORDER BY 1, 2, 3, 4, 5, 6, 7
