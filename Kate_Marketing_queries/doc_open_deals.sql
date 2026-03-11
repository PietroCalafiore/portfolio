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



    WITH WON_deals AS ( --pulling Inbound Won deals from Hubspot (Poland, Clinic Cloud) and Salesforce (Clinic Agenda, Gipo)
          SELECT
                deal.opportunity_id AS deal_id,
                deal.lead_id,
                DATE_TRUNC('month', NVL(deal.active_won_date,deal.closedate))::DATE AS month,
                deal.opportunity_name,
                deal.country,
                COALESCE(CAST(deal.hs_contact_id AS bigint), 0) AS hs_contact_id,
                DATE_TRUNC('day', NVL(deal.active_won_date,deal.closedate))::DATE AS deal_stage_start,
                deal.owner_name,
                sf.last_source__c AS sf_source,
                sf.businessline__c AS sf_product,
                MAX(mrr_euro) OVER(PARTITION BY deal.opportunity_id) AS mrr_euro_final,
                MAX(mrr_original_currency) OVER(PARTITION BY deal.opportunity_id) AS mrr_original_currency,
                live."tag",
                CASE WHEN deal.country = 'Brazil' --currently have to check both Opportunity and Lead last source due to SF issues
                    AND ((sf.last_source__c IS NULL
                    OR LOWER(sf.last_source__c) LIKE '%massive%'
                    OR LOWER(sf.last_source__c) LIKE '%outbound%'
                    OR sf.last_source__c IN ('Target tool [DA]', 'Sales Contact'))
                    AND
                    (ld.last_source__c IS NULL
                    OR LOWER(ld.last_source__c) LIKE '%massive%'
                    OR LOWER(ld.last_source__c) LIKE '%outbound%'
                    OR ld.last_source__c IN ('Target tool [DA]', 'Sales Contact')))
                    THEN 'Outbound'
                WHEN deal.country = 'Italy'
                    AND ((sf.last_source__c IN ('Basic reference', 'other', 'Target tool [DA]', 'Sales Contact', 'visiting pricing', 'new facility verification', 'New verification')
                    OR sf.last_source__c IS NULL OR LOWER(sf.last_source__c) LIKE '%internal reference%'
                    OR LOWER(sf.last_source__c) LIKE '%outbound%' OR LOWER(sf.last_source__c) LIKE '%massive%')
                    AND (ld.last_source__c IN ('Basic reference', 'other', 'Target tool [DA]', 'Sales Contact', 'visiting pricing', 'new facility verification', 'New verification')
                    OR ld.last_source__c IS NULL OR LOWER(ld.last_source__c) LIKE '%internal reference%'
                    OR LOWER(ld.last_source__c) LIKE '%outbound%' OR LOWER(ld.last_source__c) LIKE '%massive%'))
                    THEN 'Outbound'
                WHEN deal.country IN ('Mexico', 'Spain', 'Colombia') AND sf.businessline__c IN ('Clinic Agenda', 'Bundle')
                    AND ((sf.last_source__c IN ('other', 'Target tool [DA]', 'Sales Contact')
                    OR sf.last_source__c IS NULL OR LOWER(sf.last_source__c) LIKE '%internal reference%'
                    OR LOWER(sf.last_source__c) LIKE '%outbound%' OR LOWER(sf.last_source__c) LIKE '%massive%')
                    AND
                    (ld.last_source__c IN ('other', 'Target tool [DA]', 'Sales Contact')
                    OR ld.last_source__c IS NULL OR LOWER(ld.last_source__c) LIKE '%internal reference%'
                    OR LOWER(ld.last_source__c) LIKE '%outbound%' OR LOWER(ld.last_source__c) LIKE '%massive%'))
                    THEN 'Outbound'
                WHEN LOWER(sf.last_source__c) LIKE '%deleted%' THEN 'Outbound'
                ELSE 'Inbound'
                END AS deal_allocation,
                CASE WHEN deal.country = 'Brazil'
                    AND deal.pipeline = 'PMS' THEN 'Feegow'
                WHEN deal.country = 'Italy'
                    AND deal.pipeline LIKE '%PMS%' THEN 'Gipo'
                WHEN deal.country = 'Spain'
                    AND (deal.pipeline LIKE '%Enterprise SaaS4Clinics / Marketplace%' OR deal.pipeline = 'PMS')
                    AND (deal.opportunity_name LIKE '%CC%'
                    OR deal.opportunity_name LIKE '%Clinic Cloud%' OR live.hubspot_team_id IN (1453567, 271598, 3115413, 2903828)) THEN 'Clinic Cloud'
                WHEN deal.country = 'Poland' AND deal.pipeline = 'PMS' THEN 'MyDr'
                WHEN deal.country = 'Spain' AND deal.pipeline = 'Clinics New Sales' THEN 'Clinic Agenda'
                ELSE 'Clinic Agenda'
                END AS product_won,
            sf.last_source__c,
            ld.last_source__c
            FROM mart_cust_journey.cj_opportunity_facts deal
            LEFT JOIN dw.hs_deal_live live ON live.hubspot_id = deal.opportunity_id
            LEFT JOIN dp_salesforce.opportunity sf ON sf.id = deal.opportunity_id AND stagename IN ('Closed Won', 'Active/Won', 'Closed Lost', 'Back to Sales')
            LEFT JOIN dp_salesforce.lead ld ON ld.convertedopportunityid = sf.id
            WHERE (deal.current_stage = 'Closed Won' OR deal.was_active_won = 1)
                AND pipeline_type IN ('Clinics New Sales')
                AND UPPER(deal.opportunity_name) NOT LIKE '%FAKE%'
                AND deal.owner_name NOT LIKE '%Agnieszka Ligwinska%'
                AND (deal.country IN ('Colombia', 'Mexico', 'Brazil', 'Italy')
                OR (deal.country = 'Spain' AND deal.opportunity_name NOT LIKE '%Saas360%'
                AND deal.opportunity_name NOT LIKE '%PMS deal%' AND deal.opportunity_name NOT LIKE '%PMS SALES DEAL%')
                OR (deal.country = 'Poland' AND deal.pipeline != 'PMS' AND live.hubspot_team_id = 3210 AND NOT (LOWER(live."tag") LIKE '%sell%'
                    OR LOWER(live."tag") LIKE '%upgrade%'
                    OR LOWER(live."tag") LIKE '%migration%'
                    OR LOWER(live."tag") LIKE '%error%'
                    OR LOWER(live."tag") LIKE '%references%'
                    OR LOWER(live."tag") LIKE '%other%'
                    OR LOWER(live."tag") IS NULL))
                    OR deal.country = 'Poland' AND deal.pipeline = 'PMS'
                    )
                AND deal_allocation = 'Inbound' AND deal.opportunity_id IN (36830602125,38290289575)
                AND DATE_TRUNC('month', NVL(deal.active_won_date, deal.closedate))::DATE >= '2024-01-01'
                QUALIFY current_stage = 'Closed Won' AND deal.country IN ('Poland', 'Italy', 'Spain', 'Mexico', 'Colombia', 'Brazil')  OR (deal.country IN ('Italy', 'Spain', 'Mexico', 'Colombia', 'Brazil', 'Spain') AND product_won IN ('Clinic Agenda', 'Gipo') AND (deal.was_active_won = 1 OR deal.current_stage = 'Closed Won'))


            SELECT last_source__c,id, country__c, iswon,closedate, stagename, active_won_date__c FROM dp_salesforce.opportunity WHERE-- id = '006P400000QWYSdIAP'
    last_source__c  = 'Internal reference S4C/PMS'
