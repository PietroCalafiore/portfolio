   --New marketing DB - new segment split with fac size 2108
DROP TABLE IF EXISTS cj_data_layer.new_mkt_db_2024_jun_2025;
CREATE TABLE cj_data_layer.new_mkt_db_2024_jun_2025 AS
   WITH subquery AS(
    SELECT
        hs.hubspot_id AS hubspot_id,
        hs.country AS country,
        DATE_TRUNC('month', COALESCE(hs.entered_marketing_database_at_test, hcl.entered_marketing_database_at_test)) AS create_date,
            CASE WHEN
        --DP subbrand, doctors Co,mx,it,br,es
        (hs.sub_brand_wf_test LIKE '%Doctoralia%' OR hs.sub_brand_wf_test LIKE '%MioDottore%'
        OR hs.sub_brand_wf_test LIKE '%MioDottore%' OR hs.sub_brand_wf_test LIKE '%jameda%'
        OR hs.sub_brand_wf_test LIKE '%DoktorTakvimi%'
        OR hs.sub_brand_wf_test LIKE '%ZnanyLekarz%'
        OR hs.sub_brand_wf_test LIKE '%ZnamyLekar%' OR hs.sub_brand_wf_test IS NULL)
        AND COALESCE(hs.contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY')
        AND hs.country IN ('Colombia', 'Mexico', 'Italy', 'Brazil', 'Spain') THEN 'Individuals Core'
        --GPs
        WHEN (hs.sub_brand_wf_test LIKE '%Doctoralia%' OR hs.sub_brand_wf_test LIKE '%MioDottore%'
        OR hs.sub_brand_wf_test LIKE '%MioDottore%' OR hs.sub_brand_wf_test LIKE '%jameda%'
        OR hs.sub_brand_wf_test LIKE '%DoktorTakvimi%'
        OR hs.sub_brand_wf_test LIKE '%ZnanyLekarz%'
        OR hs.sub_brand_wf_test LIKE '%ZnamyLekar%' OR hs.sub_brand_wf_test IS NULL)
        AND hs.country IN ('Italy') AND COALESCE(hs.contact_type_segment_test, 'UNKNOWN') IN ('GENERAL PRACTITIONER', 'GENERAL PRACTITIONER & DOCTOR') THEN 'Individuals GPs'
        --DP subbrand, doctors Poland
        WHEN (hs.sub_brand_wf_test LIKE '%Doctoralia%' OR hs.sub_brand_wf_test LIKE '%MioDottore%'
        OR hs.sub_brand_wf_test LIKE '%MioDottore%' OR hs.sub_brand_wf_test LIKE '%jameda%'
        OR hs.sub_brand_wf_test LIKE '%DoktorTakvimi%'
        OR hs.sub_brand_wf_test LIKE '%ZnanyLekarz%'
        OR hs.sub_brand_wf_test LIKE '%ZnamyLekar%' OR hs.sub_brand_wf_test IS NULL)
        AND hs.country = 'Poland' AND COALESCE(hs.contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY', 'NURSES') THEN 'Individuals Core'
        --dp subbrand, only Ind segment countries
        WHEN (hs.sub_brand_wf_test LIKE '%Doctoralia%' OR hs.sub_brand_wf_test LIKE '%MioDottore%'
        OR hs.sub_brand_wf_test LIKE '%MioDottore%' OR hs.sub_brand_wf_test LIKE '%jameda%'
        OR hs.sub_brand_wf_test LIKE '%DoktorTakvimi%'
        OR hs.sub_brand_wf_test LIKE '%ZnanyLekarz%'
        OR hs.sub_brand_wf_test LIKE '%ZnamyLekar%' OR hs.sub_brand_wf_test IS NULL OR hs.sub_brand_wf_test IS NULL OR hs.sub_brand_wf_test = 'Noa' OR hs.sub_brand_wf_test LIKE '%Noa%')
        AND hs.country IN ('Turkey', 'Argentina', 'Chile', 'Germany', 'Turkiye')
        AND COALESCE(hs.contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY', 'DOCTOR&FACILITY - 2IN1', 'FACILITY', 'MARKETING') THEN 'Individuals Core'
        --DP subbrand, facilities
        WHEN (hs.sub_brand_wf_test LIKE '%Doctoralia%' OR hs.sub_brand_wf_test LIKE '%MioDottore%'
        OR hs.sub_brand_wf_test LIKE '%MioDottore%' OR hs.sub_brand_wf_test LIKE '%jameda%'
        OR hs.sub_brand_wf_test LIKE '%DoktorTakvimi%'
        OR hs.sub_brand_wf_test LIKE '%ZnanyLekarz%'
        OR hs.sub_brand_wf_test LIKE '%ZnamyLekar%' OR hs.sub_brand_wf_test IS NULL OR hs.sub_brand_wf_test = 'Noa')
        AND COALESCE(hs.contact_type_segment_test, 'UNKNOWN') IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'Clinics PRS'
        WHEN hs.sub_brand_wf_test = 'Noa' THEN 'Individuals Core'
        WHEN hs.country IN ('Brazil', 'Poland', 'Italy', 'Spain') AND
            (hs.sub_brand_wf_test IN ('MyDr', 'Feegow', 'Clinic Cloud', 'Gipo', 'Tuotempo')
            OR hs.sub_brand_wf_test LIKE '%Feegow%'
            OR hs.sub_brand_wf_test LIKE '%Gipo%'
            OR hs.sub_brand_wf_test LIKE '%MyDr%'
            OR hs.sub_brand_wf_test LIKE '%Clinic Cloud%'
            OR hs.sub_brand_wf_test LIKE '%Tuotempo%') THEN 'Clinics PMS' --counted as CLinics DB, all contact types except Patient and student
        WHEN hs.sub_brand_wf_test LIKE '%Noa%' THEN 'Individuals Core' --edge cases of Noa + PMS in non-pms countries
        ELSE 'None' END AS segment_funnel,
            CASE WHEN
        --DP subbrand, Paramedics spec countries
         (hs.sub_brand_wf_test LIKE '%Doctoralia%' OR hs.sub_brand_wf_test LIKE '%MioDottore%'
        OR hs.sub_brand_wf_test LIKE '%MioDottore%' OR hs.sub_brand_wf_test LIKE '%jameda%'
        OR hs.sub_brand_wf_test LIKE '%DoktorTakvimi%'
        OR hs.sub_brand_wf_test LIKE '%ZnanyLekarz%'
        OR hs.sub_brand_wf_test LIKE '%ZnamyLekar%' OR hs.sub_brand_wf_test IS NULL)
        AND (COALESCE(hs.spec_split_test, hcl.spec_split_test) = 'Paramedical' OR COALESCE(hcl.spec_split_test, hs.spec_split_test) IS NULL) --inverting the coalece for NULLs to ensure we have medical spec for first month of 2024
        AND ((hs.country IN  ('Brazil', 'Mexico') AND COALESCE(hs.contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY', 'GENERAL PRACTITIONER', 'GENERAL PRACTITIONER & DOCTOR'))
        OR (hs.country = 'Poland' AND COALESCE(hs.contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY', 'NURSES'))
        OR (hs.country = 'Chile' AND COALESCE(hs.contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY', 'DOCTOR&FACILITY - 2IN1', 'FACILITY', 'MARKETING'))) THEN 'Paramedics'
        WHEN (hs.sub_brand_wf_test LIKE '%Doctoralia%' OR hs.sub_brand_wf_test LIKE '%MioDottore%'
        OR hs.sub_brand_wf_test LIKE '%MioDottore%' OR hs.sub_brand_wf_test LIKE '%jameda%'
        OR hs.sub_brand_wf_test LIKE '%DoktorTakvimi%'
        OR hs.sub_brand_wf_test LIKE '%ZnanyLekarz%'
        OR hs.sub_brand_wf_test LIKE '%ZnamyLekar%' OR hs.sub_brand_wf_test IS NULL)
        AND COALESCE(hs.contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY', 'GENERAL PRACTITIONER', 'GENERAL PRACTITIONER & DOCTOR')
        AND hs.country IN ('Colombia', 'Italy', 'Brazil', 'Spain', 'Mexico') THEN 'Doctors'
       --DP subbrand, doctors Poland
        WHEN (hs.sub_brand_wf_test LIKE '%Doctoralia%' OR hs.sub_brand_wf_test LIKE '%MioDottore%'
        OR hs.sub_brand_wf_test LIKE '%MioDottore%' OR hs.sub_brand_wf_test LIKE '%jameda%'
        OR hs.sub_brand_wf_test LIKE '%DoktorTakvimi%'
        OR hs.sub_brand_wf_test LIKE '%ZnanyLekarz%'
        OR hs.sub_brand_wf_test LIKE '%ZnamyLekar%' OR hs.sub_brand_wf_test IS NULL)
        AND hs.country = 'Poland' AND COALESCE(hs.contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY', 'NURSES') THEN 'Doctors'
        --dp subbrand, only Ind segment countries
        WHEN (hs.sub_brand_wf_test LIKE '%Doctoralia%' OR hs.sub_brand_wf_test LIKE '%MioDottore%'
        OR hs.sub_brand_wf_test LIKE '%MioDottore%' OR hs.sub_brand_wf_test LIKE '%jameda%'
        OR hs.sub_brand_wf_test LIKE '%DoktorTakvimi%'
        OR hs.sub_brand_wf_test LIKE '%ZnanyLekarz%'
        OR hs.sub_brand_wf_test LIKE '%ZnamyLekar%' OR hs.sub_brand_wf_test IS NULL OR hs.sub_brand_wf_test IS NULL)
        AND hs.country IN ('Turkey', 'Argentina', 'Chile', 'Germany', 'Turkiye')
        AND COALESCE(hs.contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY', 'DOCTOR&FACILITY - 2IN1', 'FACILITY', 'MARKETING') THEN 'Doctors'
        WHEN (hs.sub_brand_wf_test = 'Noa'
        OR hs.sub_brand_wf_test LIKE '%Noa%')
        AND hs.country IN ('Turkey', 'Argentina', 'Chile', 'Germany', 'Turkiye')
        AND COALESCE(hs.contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY', 'DOCTOR&FACILITY - 2IN1', 'FACILITY', 'MARKETING') THEN 'Noa Notes'
        --DP subbrand, facilities
        WHEN (hs.sub_brand_wf_test LIKE '%Doctoralia%' OR hs.sub_brand_wf_test LIKE '%MioDottore%'
        OR hs.sub_brand_wf_test LIKE '%MioDottore%' OR hs.sub_brand_wf_test LIKE '%jameda%'
        OR hs.sub_brand_wf_test LIKE '%DoktorTakvimi%'
        OR hs.sub_brand_wf_test LIKE '%ZnanyLekarz%'
        OR hs.sub_brand_wf_test LIKE '%ZnamyLekar%' OR hs.sub_brand_wf_test IS NULL)
        AND COALESCE(hs.contact_type_segment_test, 'UNKNOWN') IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN
            CASE WHEN hs.facility_size LIKE ('%Individual%') OR hs.facility_size LIKE ('%Small%') THEN 'Small'
            WHEN hs.facility_size LIKE ('%Large%') OR hs.facility_size LIKE ('%Mid%') THEN 'Medium'
            ELSE 'Unknown' END
        WHEN hs.sub_brand_wf_test = 'Noa' THEN 'Noa Notes'
        WHEN hs.country IN ('Brazil', 'Poland', 'Italy', 'Spain') AND
            (hs.sub_brand_wf_test IN ('MyDr', 'Feegow', 'Clinic Cloud', 'Gipo', 'Tuotempo')
            OR hs.sub_brand_wf_test LIKE '%Feegow%'
            OR hs.sub_brand_wf_test LIKE '%Gipo%'
            OR hs.sub_brand_wf_test LIKE '%MyDr%'
            OR hs.sub_brand_wf_test LIKE '%Clinic Cloud%'
            OR hs.sub_brand_wf_test LIKE '%Tuotempo%') THEN
            CASE WHEN (hs.contact_type_segment_test = 'DOCTOR' AND hs.country = 'Spain')
                OR ((hs.facility_size LIKE ('%Individual%') OR hs.facility_size LIKE ('%Small%')) AND hs.country IN ('Italy', 'Brazil'))
                OR (hs.country = 'Poland' AND (hs.contact_type_segment_test IS NULL OR hs.contact_type_segment_test NOT IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1')))
                THEN 'Small' --for CC, based on segment, logic ok Clement
            WHEN hs.country IN ('Spain', 'Poland') OR ((hs.facility_size LIKE ('%Large%') OR hs.facility_size LIKE ('%Mid%')) AND hs.country IN ('Italy', 'Brazil')) THEN 'Medium'
            ELSE 'Unknown' END
                --counted as CLinics DB, all contact types except Patient and student
         WHEN hs.sub_brand_wf_test LIKE '%Noa%' THEN 'Noa Notes'
        ELSE 'None' END AS target,
        CASE WHEN hs.email LIKE '%gdpr%' THEN 'gdpr'
            WHEN hs.email LIKE '%deleted%' THEN 'deleted'
            WHEN hs.email LIKE '%docplanner%' THEN 'invalid'
            WHEN hs.email LIKE '%znanylekarz%' THEN 'invalid'
            WHEN hs.email LIKE '%doctoralia%' THEN 'invalid'
            WHEN hs.email LIKE '%miodottore%' THEN 'invalid'
            WHEN hs.email LIKE '%tuotempo.com%' THEN 'invalid'
            WHEN hs.email LIKE '%jameda%' THEN 'invalid'
            WHEN hs.email IS NULL THEN 'invalid'
            WHEN hs.country IN ('Germany') AND (hs.hs_email_optout IS TRUE OR hcl.jameda_do_not_contact = 'True') THEN 'unsub'
            WHEN hs.email_swap_counter > 4 AND hs.swaped_email_at >= hs.entered_marketing_database_at_test - 4 THEN 'unsub'
            WHEN hs.country != 'Germany' AND hs.unsubscribed_from_all_emails_at IS NOT NULL THEN 'unsub'
            WHEN hs.doctor_facility___hard_bounced__wf IS NOT NULL THEN 'hardbounce'
            ELSE 'valid' END AS email_status,
        CASE WHEN hcl.hs_analytics_source = 'PAID_SOCIAL' AND LOWER(hcl.hs_analytics_source_data_1) IN ('facebook', 'instagram', 'linkedin') THEN 'Paid'
            WHEN hcl.hs_analytics_source = 'PAID_SEARCH' AND (LOWER(hcl.hs_analytics_source_data_1) IN ('yahoo', 'bing', 'google')
                OR LOWER(hcl.hs_analytics_source_data_2) LIKE '%_%' OR LOWER(hcl.hs_analytics_source_data_1) LIKE '%_%' OR LOWER(hcl.hs_analytics_source_data_2) = 'google') THEN 'Paid'
            WHEN hs.doctor_facility___marketing_events_tag IS NOT NULL THEN 'Events'
            WHEN LOWER(hs.affiliate_source) LIKE '%callcenter%' OR LOWER(hs.marketing_action_tag2) LIKE '%callcenter%' THEN 'Call Center'
            ELSE 'Organic' END AS db_channel_short,
        CASE WHEN COALESCE(hs.verified, hs.facility_verified) THEN 'Verified' ELSE 'Unverified' END AS verified,
        CASE WHEN (hs.email IS NULL AND hs.hubspot_id IS NOT NULL) -- email null while the hs id is not null (contact exists)
            OR (hs.saas_user_type_batch IN ('Clinic Secretary Accountant', 'Clinic Secretary Marketing', 'Doctor Secretary', 'Clinic Secretary Receptionist', 'Clinic Secretary', 'Clinic Secretary Head of reception')
                AND hs.source_doctor_id IS NULL AND hs.source_facility_id IS NULL AND hs.secretary_managed_facility_id_dwh_batch IS NOT NULL AND hs.hubspot_id IS NOT NULL) -- contact exists and is a secretary
            OR (hs.secretaries_country_promoter_ninja_saas IS NOT NULL AND hs.source_doctor_id IS NULL AND hs.source_facility_id IS NULL AND hs.hubspot_id IS NOT NULL) -- contact exists and is a secretary
            OR (hs.hs_lead_status = 'UNQUALIFIED' AND hs.country = 'Italy') --Italy excludes unqualified leads
            OR (hs.country = 'Brazil' AND hs.spec_split_test = 'Bad Paramedical')
            OR hs.is_deleted IS TRUE
            OR hs.hs_analytics_source_data_2 IN (56674734, 59119349, 58055064, 58234863) --excluding Clinic Cloud January & March imports + Feegow February and March Imports
            THEN TRUE END AS marketing_base_excluded,
        ROW_NUMBER() OVER (PARTITION BY hs.hubspot_id ORDER BY hs.start_date DESC) AS row_per_lcs, --using starte_date as updated_at has duplicated in specific cases
        DATE_TRUNC('month', hs.commercial_from) AS last_commercial_from,
        CASE WHEN COALESCE(DATE_TRUNC('month', hcl.last_churn_date_so_da), DATE_TRUNC('month', hcl.churn_date_cs)) = DATE_TRUNC('month', hcl.entered_marketing_database_at_test)
            AND hs.commercial_from <= DATE_TRUNC('month', hcl.entered_marketing_database_at_test)
            THEN 'churned_from_old_commercial' --if last churn date is the same month as entered marketing database and the commercial date is old, dont count them
            WHEN COALESCE(DATE_TRUNC('month', hcl.last_churn_date_so_da), DATE_TRUNC('month', hcl.churn_date_cs)) = DATE_TRUNC('month', hcl.entered_marketing_database_at_test)
                AND hs.commercial_from IS NULL THEN 'churned_from_old_commercial' --workaround temp for churned but no commercial date
            ELSE 'early_churn or not commercial' END AS churn_flag,
        CASE WHEN hs.member_customers_list = 'Yes' AND (DATE_TRUNC('month', hs.commercial_from) < DATE_TRUNC('month', hcl.entered_marketing_database_at_test)) THEN 'old_commercial' --if at the time of entering db, he was a customer, check if commercial status is old or this month
            WHEN hs.member_customers_list = 'Yes' AND hs.commercial_from IS NULL AND DATE_TRUNC('month', hcl.createdate::DATE) != DATE_TRUNC('month', hcl.entered_marketing_database_at_test) THEN 'likely_old_commercial' --if we dont have commercial date info, consider it an old commercial (happens when primary company is paying so no contact properties). Use live create date value due to merges
            WHEN hs.member_customers_list = 'Yes' THEN 'new_commercial' --if doesnt fall into previous group, then newly commercial and is OK to be counted
            WHEN hs.commercial_from IS NULL THEN 'never_commercial'
            ELSE 'not_commercial_now' END AS commercial_flag,
        hs.member_customers_list,
        CASE WHEN hs.sub_brand_wf_test LIKE '%Doctoralia%' OR hs.sub_brand_wf_test LIKE '%MioDottore%'
        OR hs.sub_brand_wf_test LIKE '%MioDottore%' OR hs.sub_brand_wf_test LIKE '%jameda%'
        OR hs.sub_brand_wf_test LIKE '%DoktorTakvimi%'
        OR hs.sub_brand_wf_test LIKE '%ZnanyLekarz%'
        OR hs.sub_brand_wf_test LIKE '%ZnamyLekar%' OR hs.sub_brand_wf_test IS NULL THEN 'Docplanner'
        WHEN hs.sub_brand_wf_test IN ('MyDr', 'Feegow', 'Clinic Cloud', 'Gipo', 'Tuotempo', 'Noa') THEN hs.sub_brand_wf_test
        WHEN hs.sub_brand_wf_test LIKE '%Feegow%' THEN 'Feegow'
    WHEN hs.sub_brand_wf_test LIKE '%Gipo%' THEN 'Gipo'
    WHEN hs.sub_brand_wf_test LIKE '%Clinic Cloud%' THEN 'Clinic Cloud'
    WHEN hs.sub_brand_wf_test LIKE '%MyDr%' THEN 'MyDr'
    WHEN hs.sub_brand_wf_test LIKE '%Tuotempo%' THEN 'Tuotempo'
    WHEN hs.sub_brand_wf_test LIKE '%Noa%' THEN 'Noa'
    ELSE hs.sub_brand_wf_test END AS subbrand
    FROM dw.hs_contact_live_history hs
    LEFT JOIN dw.hs_contact_live hcl ON hs.hubspot_id = hcl.hubspot_id
    WHERE hs.country IN ('Spain', 'Chile', 'Colombia', 'Germany', 'Italy', 'Mexico', 'Poland', 'Turkiye', 'Brazil')
        AND COALESCE(hs.entered_marketing_database_at_test, hcl.entered_marketing_database_at_test) IS NOT NULL
        AND hs.start_date BETWEEN COALESCE(hs.entered_marketing_database_at_test, hcl.entered_marketing_database_at_test) AND DATEADD('day', 30, COALESCE(hs.entered_marketing_database_at_test, hcl.entered_marketing_database_at_test))
    AND (hs.contact_type_segment_test NOT IN ('PATIENT', 'STUDENTS') OR hs.contact_type_segment_test IS NULL)
        AND DATE_TRUNC('month', COALESCE(hs.entered_marketing_database_at_test, hcl.entered_marketing_database_at_test)) BETWEEN '2023-12-01' AND '2025-07-01'
        QUALIFY row_per_lcs = 1
    ),

new_db AS (
    SELECT *
    FROM subquery
    WHERE (member_customers_list = 'No' OR member_customers_list IS NULL OR commercial_flag = 'new_commercial') AND churn_flag != 'churned_from_old_commercial'
        AND email_status = 'valid'
        AND marketing_base_excluded IS NULL
    )
    SELECT
        create_date,
        country,
        segment_funnel AS segment_funnel,
        target,
        subbrand,
        db_channel_short,
        verified,
        COUNT(DISTINCT hubspot_id) AS new_marketing_database
    FROM new_db
    WHERE segment_funnel IS NOT NULL AND segment_funnel != 'None'
    GROUP BY 1, 2, 3, 4, 5, 6, 7
   ORDER BY 1, 2, 3, 4, 5, 6, 7
