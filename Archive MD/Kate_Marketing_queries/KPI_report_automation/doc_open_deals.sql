WITH valid_contacts AS ( --select contacts with open deals in May that are considered Inbound based on last source combos
    SELECT
        od.country,
        hclh.last_source_so,
        od.hs_contact_id,
        hclh.email,
        od.deal_id,
        DATE_TRUNC('month', od.createdate) AS open_deal_month,
        CASE WHEN od.country IN ('Colombia', 'Mexico', 'Spain', 'Brazil', 'Poland', 'Argentina', 'Chile', 'Peru') --Inbound/Outbound split based on last source
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
        END AS deal_allocation,
        CASE --select only DOCTOR segment contacts
            WHEN od.country IN ('Colombia', 'Mexico', 'Italy', 'Brazil', 'Peru')
                AND COALESCE(hclh.contact_type_segment_test, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY')
                THEN 'DOCTOR'
            WHEN od.country IN ('Colombia', 'Mexico', 'Italy', 'Brazil')
                AND COALESCE(hclh.contact_type_segment_test, 'UNKNOWN') IN ('GENERAL PRACTITIONER', 'GENERAL PRACTITIONER & DOCTOR')
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
                AND COALESCE(hclh.contact_type_segment_test, 'UNKNOWN') IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1')
                AND (hclh.lead_transferred_to_so LIKE '%Marketplace Team%') THEN 'DOCTOR'
            ELSE 'OTHER'
        END AS target,
        COALESCE(hclh.spec_split_test, 'Paramedical') AS specialisation,
        hclh.sub_brand_wf_test,
        hclh.contact_type_segment_test,
        CASE WHEN LOWER(hclh.affiliate_source) LIKE '%callcenter%' OR LOWER(hclh.marketing_action_tag2) LIKE '%callcenter%' THEN TRUE END AS callcenter_flag,
        ROW_NUMBER() OVER (PARTITION BY od.deal_id, od.hs_contact_id ORDER BY hclh.start_date DESC) AS row_num --should be sorted by the date from the table which has the log rows
    FROM mart_cust_journey.cj_deal_month od
    LEFT JOIN dw.hs_deal_live hdlh ON hdlh.hubspot_id = od.deal_id
    LEFT JOIN dw.hs_contact_live_history hclh
        ON hclh.hubspot_id = od.hs_contact_id
            AND hclh.start_date BETWEEN od.createdate
            AND DATEADD(DAY, 31, DATE_TRUNC('month', od.createdate::DATE))
    WHERE DATE_TRUNC('month', od.createdate) >= '2025-01-01'
        AND od.stage_is_month_new IS TRUE
        AND od.deal_stage NOT IN ('Closed Won', 'Closed Lost')
        AND (
            -- Germany
            (od.country = 'Germany' AND od.pipeline_type IN ('Clinics New Sales', 'Individual New Sales'))
            -- Italy
            OR (
                od.country = 'Italy'
                AND od.pipeline_type = 'Individual New Sales'
                AND (NOT hdlh.offer_type = 'GP [IT]' OR hdlh.offer_type IS NULL))
            -- All other countries
            OR (od.country NOT IN ('Germany', 'Italy') AND od.pipeline_type = 'Individual New Sales')
        )
    QUALIFY row_num = 1 AND deal_allocation = 'Inbound'
    AND target = 'DOCTOR'
    AND (email NOT LIKE '%deleted%' OR email IS NULL) AND (specialisation IS NULL OR specialisation != 'Bad Paramedical')
    AND (sub_brand_wf_test LIKE '%Doctoralia%' OR sub_brand_wf_test LIKE '%MioDottore%' OR sub_brand_wf_test LIKE '%MioDottore%' OR sub_brand_wf_test  LIKE '%jameda%'
        OR sub_brand_wf_test  LIKE '%DoktorTakvimi%' OR sub_brand_wf_test  LIKE '%ZnanyLekarz%' OR sub_brand_wf_test  IS NULL)
)

SELECT
    country,
    open_deal_month::TEXT AS month,
    COUNT(DISTINCT deal_id) AS total_open_deals,
    COUNT(DISTINCT CASE WHEN specialisation = 'Medical' AND country IN ('Brazil', 'Mexico', 'Chile', 'Poland') THEN deal_id END) AS crm_open_deals,
    COUNT(DISTINCT CASE WHEN specialisation = 'Paramedical' AND country IN ('Brazil', 'Mexico', 'Chile', 'Poland') THEN deal_id END) AS prm_open_deals,
    COUNT(DISTINCT CASE WHEN callcenter_flag THEN deal_id END) AS callcenter_open_deals,
    COUNT(DISTINCT CASE WHEN specialisation = 'Medical' AND callcenter_flag THEN deal_id END) AS crm_callcenter_open_deals
FROM valid_contacts
GROUP BY
    country,
    open_deal_month::TEXT
