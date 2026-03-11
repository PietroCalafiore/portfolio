--Pigment DOC open deal query
WITH valid_contacts AS ( --select contacts with open deals in May that are considered Inbound based on last source combos
    SELECT
        od.country,
        hclh.last_source_so,
        od.hs_contact_id,
        hclh.email AS od_email,
        od.deal_id,
        DATE_TRUNC('month', od.createdate) AS open_deal_month,
        CASE WHEN
            od.country = 'Mexico' AND od.createdate >= '2025-07-13' AND
            (sf.last_source__c  NOT IN ('Basic reference', 'Target tool [DA]', 'Sales Contact', 'Sales Contact [Outbound]', 'Target Tool [Outbound]') OR ld.last_source__c  NOT IN ('Basic reference', 'Target tool [DA]', 'Sales Contact', 'Sales Contact [Outbound]', 'Target Tool [Outbound]'))
            THEN 'Inbound'
            WHEN od.country IN ('Colombia', 'Mexico', 'Spain', 'Brazil', 'Poland', 'Argentina', 'Chile', 'Peru') --Inbound/Outbound split based on last source
                AND ((hclh.source_so NOT IN ('basic reference', 'Target tool [DA]', 'Sales Contact'))
                    OR (hclh.last_source_so NOT IN ('basic reference', 'Target tool [DA]', 'Sales Contact', 'visiting pricing')))
                THEN 'Inbound'
            WHEN od.country IN ('Turkiye', 'Germany')
                AND (hclh.last_source_so NOT IN ('Target tool [DA]', 'Sales Contact') OR hclh.source_so NOT IN ('Target tool [DA]', 'Sales Contact'))
                THEN 'Inbound'
            WHEN od.country = 'Italy' AND (COALESCE(od.segment, 'UNKNOWN') IN ('GENERAL PRACTITIONER', 'GENERAL PRACTITIONER & DOCTOR'))
                AND (hclh.source_so NOT IN (
                        'Target tool [DA]',
                        'Sales Contact',
                        'basic reference',
                        'new facility verification',
                        'Massive assignment',
                        'other')
                    OR hclh.last_source_so NOT IN (
                        'Target tool [DA]',
                        'Sales Contact',
                        'basic reference',
                        'Massive assignment',
                        'new facility verification',
                        'other'))
                THEN 'Inbound'
            WHEN od.country = 'Italy'
                AND (hclh.source_so NOT IN (
                        'Target tool [DA]',
                        'Sales Contact',
                        'basic reference',
                        'other',
                        'new facility verification',
                        'Massive assignment',
                        'New verification',
                        'other')
                    OR hclh.last_source_so NOT IN (
                        'Target tool [DA]',
                        'Sales Contact',
                        'basic reference',
                        'Massive assignment',
                        'new facility verification',
                        'New verification',
                        'visiting pricing',
                        'e-commerce abandoned cart',
                        'other'))
                THEN 'Inbound'
            WHEN od.country = 'Italy'
                AND hclh.source_so = 'New verification'
                AND hclh.last_source_so NOT IN (
                    'Target tool [DA]',
                    'Sales Contact',
                    'basic reference',
                    'Massive assignment',
                    'New verification',
                    'other')
                THEN 'Inbound'
            WHEN hdlh."tag" LIKE '%inbound%' OR hdlh."tag" LIKE '%mixed%' THEN 'Inbound'
            ELSE 'Outbound'
        END AS od_deal_allocation,
        CASE --select only DOCTOR segment contacts
            WHEN od.country IN ('Colombia', 'Mexico', 'Italy', 'Brazil', 'Peru')
                AND COALESCE(hclh.contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY')
                THEN 'DOCTOR'
            WHEN od.country IN ( 'Italy')
                AND (COALESCE(hclh.contact_type_segment_test, 'UNKNOWN') IN ('GENERAL PRACTITIONER', 'GENERAL PRACTITIONER & DOCTOR') OR hdlh.offer_type = 'GP [IT]' )
                THEN 'GP'
            WHEN od.country = 'Poland'
                AND COALESCE(hclh.contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'NURSES')
                THEN 'DOCTOR'
            WHEN od.country = 'Spain'
                AND COALESCE(hclh.contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'NURSES', 'SECRETARY')
                THEN 'DOCTOR'
            WHEN od.country IN ('Turkey', 'Turkiye', 'Argentina')
                AND COALESCE(hclh.contact_type_segment_test, 'UNKNOWN') NOT IN ('PATIENT', 'STUDENT', 'NURSES')
                THEN 'DOCTOR'
            WHEN od.country IN ('Chile', 'Germany')
                AND COALESCE(hclh.contact_type_segment_test, 'UNKNOWN') NOT IN ('PATIENT', 'STUDENT')
                THEN 'DOCTOR'
            WHEN od.country IN ('Colombia', 'Mexico', 'Italy', 'Brazil', 'Poland', 'Spain', 'Peru')
                    AND COALESCE(hclh.contact_type_segment_test,'UNKNOWN') IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1')
                    AND (hclh.lead_transferred_to_so LIKE '%Marketplace Team%') AND (DATE_TRUNC('month', od.createdate) = DATE_TRUNC('month', hclh.facility_lead_transferred_to_so_wf) OR hclh.facility_lead_transferred_to_so_wf IS NULL) THEN 'DOCTOR'
               WHEN ((od.country IN ('Colombia', 'Mexico', 'Italy', 'Brazil', 'Poland', 'Spain', 'Peru')
                    AND COALESCE(hclh.contact_type_segment_test,'UNKNOWN') IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1')) OR (od.country = 'Spain' AND COALESCE(hclh.contact_type_segment_test,'UNKNOWN') ='SOFTWARE COMPANY')) AND ls.hubspot_team_id IS NOT NULL THEN 'DOCTOR'
            ELSE 'OTHER'
        END AS od_target,
        CASE WHEN od.country IN ('Brazil', 'Mexico', 'Chile', 'Poland') THEN COALESCE(hclh.spec_split_test, 'Paramedical') ELSE 'Undefined' END AS od_specialisation,
        CASE WHEN cj.max_channel IS NOT NULL THEN cj.max_channel
            WHEN paid.hubspot_id IS NOT NULL THEN 'Paid_direct'
        WHEN hclh.mql_last_touch_channel_wf = 'Offline reference' OR (DATE_TRUNC('month', od.createdate) = DATE_TRUNC('month', hclh.customer_reference_at) OR DATE_DIFF('day', od.createdate::DATE, hclh.customer_reference_at) BETWEEN -10 AND 10) THEN 'Referral'
                WHEN hclh.mql_last_touch_channel_wf = 'Event' THEN 'Events_direct'
                WHEN hclh.hs_analytics_source = 'PAID_SOCIAL'
                    AND LOWER(hclh.hs_analytics_source_data_1) IN ('facebook', 'instagram', 'linkedin' ) THEN 'Paid'
                WHEN hclh.hs_analytics_source = 'PAID_SEARCH'
                    AND (LOWER(hclh.hs_analytics_source_data_1) IN ('yahoo', 'bing', 'google')
                        OR LOWER(hclh.hs_analytics_source_data_2) LIKE '%_%'
                        OR LOWER(hclh.hs_analytics_source_data_1) LIKE '%_%'
                        OR LOWER(hclh.hs_analytics_source_data_2) = 'google') THEN 'Paid'
                WHEN hclh.doctor_facility___marketing_events_tag IS NOT NULL THEN 'Events'
                WHEN hclh.affiliate_source LIKE '%callcenter%' OR hclh.marketing_action_tag2 LIKE '%callcenter%' THEN 'Call Center'
                ELSE 'Organic'
            END AS acquisition_channel,
        hclh.verified,
        hclh.active_passive_lead,
        CASE WHEN cj.active_passive_final IS NOT NULL THEN cj.active_passive_final ELSE hclh.active_passive_lead END AS active_passive,
        od.crm_source,
        CASE WHEN LOWER(hclh.affiliate_source) LIKE '%callcenter%' OR LOWER(hclh.marketing_action_tag2) LIKE '%callcenter%' THEN TRUE END AS callcenter_flag,
        CASE WHEN od.country = 'Mexico' THEN ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('month', od.createdate), od.hs_contact_id ORDER BY hclh.start_date DESC) ELSE 1 END AS row_num_mx, --mx check, trying to avoid duplicates by counting one deal per HS ID per month
        ROW_NUMBER() OVER (PARTITION BY od.deal_id, od.hs_contact_id ORDER BY hclh.start_date DESC) AS row_num --should be sorted by the date from the table which has the log rows
    FROM mart_cust_journey.cj_deal_month od
    LEFT JOIN dw.hs_deal_live hdlh ON hdlh.hubspot_id = od.deal_id
    LEFT JOIN dp_salesforce.opportunity sf ON sf.id = od.deal_id AND stagename IN ('Closed Won', 'Active/Won', 'Closed Lost', 'Back to Sales')
    LEFT JOIN dp_salesforce.lead ld ON ld.convertedopportunityid = sf.id
    LEFT JOIN mart_cust_journey.cj_mqls_monthly cj ON cj.contact_id = od.hs_contact_id AND DATE_TRUNC('month', od.createdate) = cj.month
    LEFT JOIN dw.hs_contact_live_history hclh
        ON hclh.hubspot_id = od.hs_contact_id
            AND hclh.start_date BETWEEN od.createdate
            AND DATEADD(DAY, 31, DATE_TRUNC('month', od.createdate::DATE))
    LEFT JOIN test.individual_sales_teams ls ON hclh.last_sales_team = ls.hubspot_team_id
    LEFT JOIN mart_cust_journey.msv_paid_media_campaign_hubspot_2 paid ON od.hs_contact_id = paid.hubspot_id
            AND campaign like '%mql%' AND paid.campaign_product = 'Agenda' AND paid.target IN ('DOCTOR', 'GP') AND (paid.br_specialisation != 'Bad Paramedical' OR paid.br_specialisation IS NULL)
            AND (DATE_DIFF('day', od.createdate::DATE, paid.date::DATE) BETWEEN -50 AND 50)
    WHERE DATE_TRUNC('month', od.createdate) >= '2024-06-01' AND (od.crm_source != 'hays' OR od.crm_source IS NULL)
        AND od.stage_is_month_new IS TRUE --AND od.hs_contact_id = 210018051
        AND (
            -- Germany
            (od.country = 'Germany' AND od.pipeline_type IN ('Clinics New Sales', 'Individual New Sales'))
            --Mexico
            OR (od.country = 'Mexico' AND od.pipeline_type IN ('Individual New Sales') AND od.createdate < '2025-07-13' AND crm_source = 'hubspot')
            OR (od.country = 'Mexico' AND od.pipeline_type IN ('Individual New Sales') AND od.createdate >= '2025-07-13' AND crm_source = 'salesforce')
            -- All other countries
            OR (od.country NOT IN ('Germany', 'Mexico') AND od.pipeline_type = 'Individual New Sales')
        )
    QUALIFY row_num = 1
    AND od_deal_allocation = 'Inbound'-- AND row_num_mx = 1 only post migration
    AND od_target IN  ('DOCTOR', 'GP')
    AND (od_email NOT LIKE '%deleted%' OR od_email IS NULL) AND (od_specialisation != 'Bad Paramedical')
    AND (sub_brand_wf_test LIKE '%Doctoralia%' OR sub_brand_wf_test LIKE '%MioDottore%' OR sub_brand_wf_test LIKE '%MioDottore%' OR sub_brand_wf_test  LIKE '%jameda%'
        OR sub_brand_wf_test  LIKE '%DoktorTakvimi%' OR sub_brand_wf_test  LIKE '%ZnanyLekarz%' OR sub_brand_wf_test IS NULL)
    )

SELECT
    open_deal_month::TEXT AS month,
    'Inbound' AS lead_source,
    country AS market,
    'Individuals' AS segment_funnel, --missing GPs
    od_target AS target,
    od_specialisation AS specialization,
    CASE WHEN acquisition_channel = 'Paid_direct' THEN 'Paid'
        WHEN acquisition_channel = 'Events_direct' THEN 'Events'
        ELSE acquisition_channel END AS acquisition_channel,
    CASE WHEN acquisition_channel IN ('Referral', 'Paid_direct', 'Events_direct') THEN 'Direct' ELSE 'Indirect' END AS type,
    CASE WHEN acquisition_channel IN ('Referral', 'Paid_direct', 'Events_direct')
        OR active_passive = 'Active' THEN 'Active'
        ELSE 'Passive' END AS source,
    COUNT(DISTINCT deal_id) AS total_open_deals
FROM valid_contacts
WHERE country != 'Argentina' AND open_deal_month >= '2024-06-01' AND open_deal_month < '2025-07-01'
GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8, 9
ORDER BY  1, 2, 3, 4, 5, 6, 7, 8, 9
