--Pigment FAC open deal query
WITH clinics_od AS(
    SELECT ld.id AS deal_id,
        ld.country__c AS country,
        ld.last_source__c, --opportunity last source
        mql.last_source__c,
        mql.hs_contact_id__c,
        hclh.email,
        DATE_TRUNC('month', ld.datedemoscheduled__c::DATE) AS open_deal_month,
        CASE WHEN ld.businessline__c IN ('Clinic Agenda', 'Bundle') THEN 'Clinic Agenda'
        WHEN ld.businessline__c = 'S4C/PMS' AND ld.country__c = 'Italy' AND ld.sub_business_line__c = 'GIPO' THEN 'Gipo'
        END AS product,
        CASE WHEN ld.country__c = 'Italy' AND ld.businessline__c IN ('Clinic Agenda', 'Bundle')
                    AND (ld.last_source__c IN ('Basic reference', 'Target tool [DA]', 'Sales Contact', 'visiting pricing', 'new facility verification', 'New verification', 'Internal reference', 'other') --ask Matheus why Internal reference outbound for WONs but inbund for deals
                    OR ld.last_source__c IS NULL  --checking opportunity last source
                    OR LOWER(ld.last_source__c) LIKE '%outbound%' OR LOWER(ld.last_source__c) LIKE '%massive%')
                    AND
                    (mql.last_source__c IN ('Basic reference', 'Target tool [DA]', 'Sales Contact', 'visiting pricing', 'new facility verification', 'New verification', 'Internal reference', 'other') --ask Matheus why Internal reference outbound for WONs but inbund for deals
                    OR mql.last_source__c IS NULL--checking lead last source
                    OR LOWER(mql.last_source__c) LIKE '%outbound%' OR LOWER(mql.last_source__c) LIKE '%massive%')
                    THEN 'Outbound'
             WHEN ld.country__c = 'Italy' AND ld.businessline__c IN ('S4C/PMS') AND ld.sub_business_line__c = 'GIPO'
                    AND (ld.last_source__c IN ('Basic reference', 'Target tool [DA]', 'Sales Contact', 'visiting pricing', 'new facility verification', 'New verification', 'Internal reference', 'other') --ask Matheus why Internal reference outbound for WONs but inbund for deals
                    OR ld.last_source__c IS NULL OR LOWER(ld.last_source__c) LIKE '%internal%' OR LOWER(ld.last_source__c) LIKE '%verification%'
                    OR LOWER(ld.last_source__c) LIKE '%outbound%' OR LOWER(ld.last_source__c) LIKE '%massive%')
                    AND
                    (mql.last_source__c IN ('Basic reference', 'Target tool [DA]', 'Sales Contact', 'visiting pricing', 'new facility verification', 'New verification', 'Internal reference', 'other') --ask Matheus why Internal reference outbound for WONs but inbund for deals
                    OR mql.last_source__c IS NULL OR LOWER(mql.last_source__c) LIKE '%internal%' OR LOWER(mql.last_source__c) LIKE '%verification%'
                    OR LOWER(mql.last_source__c) LIKE '%outbound%' OR LOWER(mql.last_source__c) LIKE '%massive%')
                    THEN 'Outbound'
                WHEN ld.country__c IN ('Mexico', 'Spain', 'Colombia', 'Brazil') AND ld.businessline__c IN ('Clinic Agenda', 'Bundle')
                 AND
                    (ld.last_source__c IN ('other', 'Target tool [DA]', 'Sales Contact', 'Basic reference', 'Internal reference', 'other')
                    OR ld.last_source__c IS NULL
                    OR LOWER(ld.last_source__c) LIKE '%outbound%' OR LOWER(ld.last_source__c) LIKE '%massive%')
                    AND
                      (mql.last_source__c IN ('other', 'Target tool [DA]', 'Sales Contact', 'Basic reference', 'Internal reference', 'other')
                    OR mql.last_source__c IS NULL
                    OR LOWER(mql.last_source__c) LIKE '%outbound%' OR LOWER(mql.last_source__c) LIKE '%massive%')
                    THEN 'Outbound'
                ELSE 'Inbound'
                END AS od_deal_allocation,
    CASE WHEN mart.contact_id IS NOT NULL THEN mart.db_channel_short
    WHEN paid.hubspot_id IS NOT NULL THEN 'Paid_direct' --Direct MQLs from paid Media dashboard definition
        WHEN hclh.mql_last_touch_channel_wf = 'Offline reference' OR (DATE_TRUNC('month', ld.datedemoscheduled__c) = DATE_TRUNC('month', hclh.customer_reference_at) OR DATE_DIFF('day', ld.datedemoscheduled__c::DATE, hclh.customer_reference_at) BETWEEN -10 AND 10) THEN 'Referral'
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
                ELSE 'Organic' END
        AS db_channel_per_deal,
    ROW_NUMBER() OVER (PARTITION BY ld.id ORDER BY hclh.start_date DESC) AS row_num, --should be sorted by the date from the table which has the log rows
    CASE WHEN mart.active_passive_final IS NOT NULL THEN mart.active_passive_final --first check the WON deal if there was one, if not, historical data
    WHEN hclh.active_passive_lead = 'Active' THEN 'Active' ELSE 'Passive' END AS active_passive_final,
    CASE WHEN mart.verified IN ('Verified', 'Unverified') THEN mart.verified
        WHEN mart.verified = 'Not Verified' THEN 'Unverified'
        WHEN hclh.verified THEN 'Verified' ELSE 'Unverified' END AS verified
    FROM dp_salesforce.opportunity ld
    LEFT JOIN dp_salesforce.lead mql ON ld.id = mql.convertedopportunityid
    LEFT JOIN test.cj_mqls_monthly_clinics mart ON mql.hs_contact_id__c = mart.contact_id AND ld.datedemoscheduled__c::DATE BETWEEN dateadd('day',-90, month) AND dateadd('day',40,month) --if there was a recent closed won deal, take categorization from
    LEFT JOIN dw.hs_contact_live_history hclh --get historical data for categorizarion
        ON hclh.hubspot_id = mql.hs_contact_id__c
            AND hclh.start_date BETWEEN ld.datedemoscheduled__c
            AND DATEADD(DAY, 31,DATE_TRUNC('month', ld.datedemoscheduled__c::DATE))
       LEFT JOIN mart_cust_journey.msv_paid_media_campaign_hubspot_2 paid ON  mql.hs_contact_id__c = paid.hubspot_id --check if lead was a direct MQL
            AND campaign like '%mql%' AND paid.campaign_product IN ('Agenda', 'Gipo') AND paid.target IN ('FACILITY')
            AND (DATE_DIFF('day', ld.datedemoscheduled__c::DATE, paid.date::DATE) BETWEEN -50 AND 50)
    WHERE ld.businessline__c IN ('Clinic Agenda', 'Bundle', 'S4C/PMS') --and mql.hs_contact_id__c = 136690467
    AND ld.datedemoscheduled__c >= '2024-06-01'  AND ld.datedemoscheduled__c < '2025-07-01'  AND ld.type = 'New Business'
    QUALIFY od_deal_allocation = 'Inbound' AND product IS NOT NULL AND ld.country__c IS NOT NULL AND row_num = 1
  --AND ld.country__c = 'Italy' AND DATE_TRUNC('month', ld.datedemoscheduled__c::DATE) = '2025-01-01' AND  ld.businessline__c IN ('S4C/PMS')
    ),

pl_open_deals AS (
        SELECT
            live.hubspot_id AS deal_id,
            live.country,
            DATE_TRUNC('month', live.createdate::DATE) AS open_deal_month,
            'Clinic Agenda'::TEXT AS product,
            live.last_source_so,
            CASE WHEN mart.contact_id IS NOT NULL THEN mart.db_channel_short
               WHEN paid.hubspot_id IS NOT NULL THEN 'Paid_direct'
        WHEN hclh.mql_last_touch_channel_wf = 'Offline reference' OR (DATE_TRUNC('month',  live.createdate::DATE) = DATE_TRUNC('month', hclh.customer_reference_at) OR DATE_DIFF('day',  live.createdate::DATE, hclh.customer_reference_at) BETWEEN -10 AND 10) THEN 'Referral'
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
                ELSE 'Organic' END
        AS db_channel_per_deal,
    ROW_NUMBER() OVER (PARTITION BY live.hubspot_id ORDER BY hclh.start_date DESC) AS row_num, --should be sorted by the date from the table which has the log rows
    CASE WHEN mart.active_passive_final IS NOT NULL THEN mart.active_passive_final
    WHEN hclh.active_passive_lead = 'Active' THEN 'Active' ELSE 'Passive' END AS active_passive_final
    FROM dw.hs_deal_live live
 LEFT JOIN dw.hs_deal_pipeline_dict
        ON live.pipeline = dw.hs_deal_pipeline_dict.internal_value
LEFT JOIN dw.hs_contact_live_history hclh
        ON hclh.hubspot_id = live.hubspot_contact_id
            AND hclh.start_date BETWEEN live.createdate::DATE
            AND DATEADD(DAY, 31,DATE_TRUNC('month', live.createdate::DATE))
       LEFT JOIN mart_cust_journey.msv_paid_media_campaign_hubspot_2 paid ON  live.hubspot_contact_id = paid.hubspot_id
            AND campaign like '%mql%' AND paid.campaign_product = 'Agenda' AND paid.target IN ('FACILITY')
            AND (DATE_DIFF('day', live.createdate::DATE, paid.date::DATE) BETWEEN -50 AND 50)
 LEFT JOIN test.cj_mqls_monthly_clinics mart ON (live.hubspot_contact_id = mart.contact_id OR mart.deal_id = live.hubspot_id) AND live.createdate::DATE BETWEEN dateadd('day',-90, month) AND dateadd('day', 40, month)
    WHERE live.country = 'Poland' AND live.hubspot_owner_id != 11078353 AND live.is_deleted IS FALSE
    AND live.hubspot_team_id = 3210 AND NOT (LOWER(live."tag") LIKE '%sell%'
                    OR LOWER(live."tag") LIKE '%upgrade%'
                    OR LOWER(live."tag") LIKE '%migration%'
                    OR LOWER(live."tag") LIKE '%error%'
                    OR LOWER(live."tag") LIKE '%references%'
                    OR LOWER(live."tag") IS NULL) AND hubspot_contact_id IS NOT NULL --(condition for cases like deal 35345218114)
AND hs_deal_pipeline_dict.label IN ('Enterprise SaaS4Clinics / Marketplace', 'Clinic Online Agenda')
AND DATE_TRUNC('month', live.createdate::DATE) >= '2024-06-01' AND DATE_TRUNC('month', live.createdate::DATE)< '2025-07-01'
    QUALIFY row_num = 1
    )
SELECT
     cd.open_deal_month::DATE AS month,
    'Inbound' AS lead_source,
    cd.country::TEXT AS market,
    CASE WHEN cd.product = 'Gipo' THEN 'PMS' ELSE 'Clinics' END AS segment_funnel,
    CASE WHEN cd.product = 'Gipo' THEN 'PMS' ELSE 'Facilities' END AS target,
    'Undefined' AS specialization,
    CASE WHEN db_channel_per_deal IN ('Organic/Direct', 'PMS database') THEN 'Organic'
    WHEN db_channel_per_deal = 'Paid_direct' THEN 'Paid'
    WHEN db_channel_per_deal ='Events_direct' THEN 'Event' ELSE  db_channel_per_deal END
        AS acquisition_channel,
    CASE WHEN db_channel_per_deal IN ('Referral', 'Paid_direct', 'Events_direct') THEN 'Direct' ELSE 'Indirect' END AS type,
    active_passive_final,
    COUNT(DISTINCT cd.deal_id) AS total_open_deals
    FROM clinics_od cd
GROUP BY
    1,2,3,4,5,6,7,8,9
UNION ALL
SELECT
     pod.open_deal_month::DATE AS month,
    'Inbound' AS lead_source,
    pod.country::TEXT AS market,
    'Clinics' AS segment_funnel,
    'Facilities' AS target,
    'Undefined' AS specialization,
    CASE WHEN db_channel_per_deal IN ('Organic/Direct', 'PMS database') THEN 'Organic'
    WHEN db_channel_per_deal = 'Paid_direct' THEN 'Paid'
    WHEN db_channel_per_deal ='Events_direct' THEN 'Event' ELSE  db_channel_per_deal END
        AS acquisition_channel,
    CASE WHEN db_channel_per_deal IN ('Referral', 'Paid_direct', 'Events_direct') THEN 'Direct' ELSE 'Indirect' END AS type,
    active_passive_final,
    COUNT(DISTINCT pod.deal_id) AS total_open_deals
    FROM pl_open_deals pod
GROUP BY
       1,2,3,4,5,6,7,8,9
