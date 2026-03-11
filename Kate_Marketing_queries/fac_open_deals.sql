WITH clinics_od AS(
    SELECT ld.id AS deal_id,
        ld.country__c AS country,
        ld.last_source__c,
        mql.hs_contact_id__c,
        DATE_TRUNC('month', ld.datedemoscheduled__c::DATE) AS open_deal_month,
        CASE WHEN ld.businessline__c IN ('Clinic Agenda', 'Bundle') THEN 'Clinic Agenda'
        WHEN ld.businessline__c = 'S4C/PMS' AND ld.country__c = 'Italy' AND ld.sub_business_line__c = 'GIPO' THEN 'Gipo'
        END AS product,
         CASE WHEN ld.country__c = 'Italy' AND ld.businessline__c IN ('Clinic Agenda', 'Bundle')
                    AND (ld.last_source__c IN ('Basic reference', 'Target tool [DA]', 'Sales Contact', 'visiting pricing', 'new facility verification', 'New verification') --ask Matheus why Internal reference outbound for WONs but inbund for deals
                    OR ld.last_source__c IS NULL
                    OR LOWER(ld.last_source__c) LIKE '%outbound%' OR LOWER(ld.last_source__c) LIKE '%massive%')
                    AND
                    (mql.last_source__c IN ('Basic reference', 'Target tool [DA]', 'Sales Contact', 'visiting pricing', 'new facility verification', 'New verification') --ask Matheus why Internal reference outbound for WONs but inbund for deals
                    OR mql.last_source__c IS NULL
                    OR LOWER(mql.last_source__c) LIKE '%outbound%' OR LOWER(mql.last_source__c) LIKE '%massive%')
                    THEN 'Outbound'
             WHEN ld.country__c = 'Italy' AND ld.businessline__c IN ('S4C/PMS') AND ld.sub_business_line__c = 'GIPO'
                    AND (ld.last_source__c IN ('Basic reference', 'Target tool [DA]', 'Sales Contact', 'visiting pricing', 'new facility verification', 'New verification') --ask Matheus why Internal reference outbound for WONs but inbund for deals
                    OR ld.last_source__c IS NULL OR LOWER(ld.last_source__c) LIKE '%internal%' OR LOWER(ld.last_source__c) LIKE '%verification%'
                    OR LOWER(ld.last_source__c) LIKE '%outbound%' OR LOWER(ld.last_source__c) LIKE '%massive%')
                    AND
                    (mql.last_source__c IN ('Basic reference', 'Target tool [DA]', 'Sales Contact', 'visiting pricing', 'new facility verification', 'New verification') --ask Matheus why Internal reference outbound for WONs but inbund for deals
                    OR mql.last_source__c IS NULL OR LOWER(mql.last_source__c) LIKE '%internal%' OR LOWER(mql.last_source__c) LIKE '%verification%'
                    OR LOWER(mql.last_source__c) LIKE '%outbound%' OR LOWER(mql.last_source__c) LIKE '%massive%')
                    THEN 'Outbound'
                WHEN ld.country__c IN ('Mexico', 'Spain', 'Colombia', 'Brazil') AND ld.businessline__c IN ('Clinic Agenda', 'Bundle')
                 AND
                    (ld.last_source__c IN ('other', 'Target tool [DA]', 'Sales Contact', 'Basic reference')
                    OR ld.last_source__c IS NULL
                    OR LOWER(ld.last_source__c) LIKE '%outbound%' OR LOWER(ld.last_source__c) LIKE '%massive%')
                    AND
                      (mql.last_source__c IN ('other', 'Target tool [DA]', 'Sales Contact', 'Basic reference')
                    OR mql.last_source__c IS NULL
                    OR LOWER(mql.last_source__c) LIKE '%outbound%' OR LOWER(mql.last_source__c) LIKE '%massive%')
                    THEN 'Outbound'
                ELSE 'Inbound'
                END AS deal_allocation,
    MAX(mart.db_channel_short) OVER (PARTITION BY ld.id ORDER BY mart.month DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS db_channel_per_deal,
    CASE WHEN mart.facility_size IS NOT NULL THEN mart.facility_size
    WHEN mql.facilitysize__c LIKE ('%Individual%') OR mql.facilitysize__c LIKE ('%Small%') THEN 'Small'
    WHEN mql.facilitysize__c LIKE ('%Large%') OR mql.facilitysize__c LIKE ('%Mid%') THEN 'Medium'
    ELSE 'Unknown' END AS facility_size
    FROM dp_salesforce.opportunity ld
    LEFT JOIN dp_salesforce.lead mql ON ld.id = mql.convertedopportunityid
    LEFT JOIN mart_cust_journey.cj_mqls_monthly_clinics mart ON (mql.hs_contact_id__c = mart.contact_id OR mart.deal_id = ld.id) AND ld.datedemoscheduled__c::DATE BETWEEN dateadd('day',-90, month) AND dateadd('day',40,month)
    WHERE ld.businessline__c IN ('Clinic Agenda', 'Bundle', 'S4C/PMS') --and mql.hs_contact_id__c = 136690467
    AND ld.datedemoscheduled__c BETWEEN '2024-01-01' AND CURRENT_DATE AND ld.type = 'New Business' --AND ld.country__c = 'Colombia' AND DATE_TRUNC('month', ld.datedemoscheduled__c::DATE) = '2025-04-01 00:00:00.000000'
    QUALIFY deal_allocation = 'Inbound' AND product IS NOT NULL AND ld.country__c IS NOT NULL),

pl_open_deals AS (
        SELECT
            live.hubspot_id AS deal_id,
            live.country,
            DATE_TRUNC('month', live.createdate::DATE) AS open_deal_month,
            'Clinic Agenda'::TEXT AS product,
            live.last_source_so,
            MAX(mart.db_channel_short) OVER (PARTITION BY live.hubspot_id ORDER BY mart.month DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS db_channel_per_deal,
    CASE WHEN mart.facility_size IS NOT NULL THEN mart.facility_size
    WHEN hs.facility_size LIKE ('%Individual%') OR hs.facility_size LIKE ('%Small%') THEN 'Small'
    WHEN hs.facility_size LIKE ('%Large%') OR hs.facility_size LIKE ('%Mid%') THEN 'Medium'
    ELSE 'Unknown' END AS facility_size
    FROM dw.hs_deal_live live
 LEFT JOIN dw.hs_deal_pipeline_dict
        ON live.pipeline = dw.hs_deal_pipeline_dict.internal_value
LEFT JOIN dw.hs_contact_live hs ON live.hubspot_contact_id = hs.hubspot_id --only for facility size
 LEFT JOIN mart_cust_journey.cj_mqls_monthly_clinics mart ON (live.hubspot_contact_id = mart.contact_id OR mart.deal_id = live.hubspot_id) AND live.createdate::DATE BETWEEN dateadd('day',-90, month) AND dateadd('day',40,month)
    WHERE live.country = 'Poland' AND live.hubspot_owner_id != 11078353 AND live.is_deleted IS FALSE
    AND live.hubspot_team_id = 3210 AND NOT (LOWER(live."tag") LIKE '%sell%'
                    OR LOWER(live."tag") LIKE '%upgrade%'
                    OR LOWER(live."tag") LIKE '%migration%'
                    OR LOWER(live."tag") LIKE '%error%'
                    OR LOWER(live."tag") LIKE '%references%'
                    OR LOWER(live."tag") IS NULL) AND hubspot_contact_id IS NOT NULL --(condition for cases like deal 35345218114)
AND hs_deal_pipeline_dict.label IN ('Enterprise SaaS4Clinics / Marketplace', 'Clinic Online Agenda')
AND live.createdate BETWEEN '2024-01-01'AND CURRENT_DATE --DATE_TRUNC('month', createdate::DATE) = '2025-04-01'
    )
SELECT
    cd.deal_id,
    cd.country::TEXT,
    cd.open_deal_month::DATE,
    cd.product::TEXT,
    COALESCE(cd.db_channel_per_deal,'Organic/Direct') AS db_channel_short,
    cd.facility_size,
    COUNT(DISTINCT cd.deal_id) AS total_open_deals
    FROM clinics_od cd
GROUP BY
    cd.country,
    cd.product,
    cd.open_deal_month,
    COALESCE(cd.db_channel_per_deal,'Organic/Direct'),
    cd.facility_size,
    cd.deal_id
UNION ALL
SELECT
    pod.deal_id::VARCHAR,
    pod.country::TEXT,
    pod.open_deal_month::DATE,
    pod.product::TEXT,
    COALESCE(pod.db_channel_per_deal,'Organic/Direct') AS db_channel_short,
    pod.facility_size,
    COUNT(DISTINCT pod.deal_id) AS total_open_deals
    FROM pl_open_deals pod
GROUP BY
    pod.deal_id,
    pod.country,
    pod.product,
    pod.open_deal_month,
    COALESCE(pod.db_channel_per_deal,'Organic/Direct'), 
    pod.facility_size
