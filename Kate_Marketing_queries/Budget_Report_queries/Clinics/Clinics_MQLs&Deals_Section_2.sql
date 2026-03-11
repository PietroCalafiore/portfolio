WITH WON_deals AS (
    SELECT
        deal.deal_id,
        deal.lead_id,
        deal.month::DATE as month,
        deal.deal_stage,
        deal.dealname,
        deal.country,
        COALESCE(CAST(deal.hs_contact_id AS bigint), 0) AS hs_contact_id,
        deal.deal_stage_start,
        deal.hubspot_owner_name,
        sf.last_source__c AS sf_source,
        sf.businessline__c AS sf_product,
        MAX(mrr_euro) OVER(PARTITION BY deal.deal_id) AS mrr_euro_final,
        MAX(mrr_original_currency) OVER(PARTITION BY deal.deal_id) AS mrr_original_currency,
        live."tag",
        CASE WHEN deal.country = 'Brazil'
            AND (sf.last_source__c IS NULL
            OR LOWER(sf.last_source__c) LIKE '%massive%'
            OR LOWER(sf.last_source__c) LIKE '%outbound%'
            OR sf.last_source__c IN ('Target tool [DA]', 'Sales Contact')) THEN 'Outbound'
        WHEN deal.country = 'Italy'
            AND (sf.last_source__c IN ('Basic reference', 'other', 'Target tool [DA]', 'Sales Contact', 'visiting pricing', 'new facility verification', 'New verification')
            OR sf.last_source__c IS NULL OR LOWER(sf.last_source__c) LIKE '%internal reference%'
            OR LOWER(sf.last_source__c) LIKE '%outbound%' OR LOWER(sf.last_source__c) LIKE '%massive%') THEN 'Outbound'
        WHEN deal.country IN ('Spain')
            AND sf.businessline__c IN ('Clinic Agenda', 'Bundle')
            AND (sf.last_source__c IN ('other', 'Target tool [DA]',  'Sales Contact')
            OR sf.last_source__c IS NULL OR LOWER(sf.last_source__c) LIKE '%internal reference%'
            OR LOWER(sf.last_source__c) LIKE '%outbound%') THEN 'Outbound'
        WHEN deal.country IN ('Mexico') AND sf.businessline__c IN ('Clinic Agenda', 'Bundle')
            AND (sf.last_source__c IN ('other', 'Target tool [DA]', 'Sales Contact')
            OR sf.last_source__c IS NULL OR LOWER(sf.last_source__c) LIKE '%internal reference%'
            OR LOWER(sf.last_source__c) LIKE '%outbound%' OR LOWER(sf.last_source__c) LIKE '%massive%') THEN 'Outbound'
        ELSE 'Inbound'
        END AS deal_allocation,
        CASE WHEN deal.country = 'Brazil'
            AND deal.pipeline like '%PMS%' THEN 'Feegow'
        WHEN deal.country = 'Italy'
            AND deal.pipeline like '%PMS%' THEN 'Gipo'
        WHEN deal.country = 'Spain'
            AND deal.pipeline like '%Enterprise SaaS4Clinics / Marketplace%'
            AND (deal.dealname LIKE '%CC deal%'
            OR deal.dealname LIKE '%Clinic Cloud%') THEN 'Clinic Cloud'
        WHEN deal.country = 'Poland'
            AND deal.pipeline LIKE '%PMS%' THEN 'MyDr'
        ELSE 'Clinic Agenda'
        END AS product_won
    FROM mart_cust_journey.cj_deal_month deal
    LEFT JOIN dw.hs_deal_live live ON live.hubspot_id = deal.deal_id
    LEFT JOIN dp_salesforce.opportunity sf ON sf.id = deal.deal_id
    AND stagename ='Closed Won'
    WHERE deal.deal_stage IN ('Closed Won')
        AND pipeline_type IN ('Clinics New Sales')
        AND is_current_stage
        AND stage_is_month_new
        AND UPPER(deal.dealname) NOT LIKE '%FAKE%'
        AND deal.hubspot_owner_name NOT LIKE '%Agnieszka Ligwinska%'
        AND (deal.country IN ('Colombia', 'Mexico', 'Brazil', 'Italy')
        OR (deal.country = 'Spain' AND deal.dealname NOT LIKE '%Saas360%'
        AND deal.dealname NOT LIKE '%PMS deal%')
        OR (deal.country = 'Poland' AND NOT (LOWER(live."tag") LIKE '%sell%'
            OR LOWER(live."tag") LIKE '%upgrade%'
            OR LOWER(live."tag") LIKE '%migration%'
            OR LOWER(live."tag") LIKE '%error%'
            OR LOWER(live."tag") LIKE '%references%'
            OR LOWER(live."tag") LIKE '%other%'
            OR LOWER(live."tag") IS NULL)))
        AND deal_allocation = 'Inbound'
        AND deal.month::DATE >= '2024-01-01'
),

all_mqls AS (
    SELECT 
        lcs.contact_id, --1
        lcs.lead_id, --2
        deal.deal_id,--3
        hs.email AS hs_email,--4
        contact_facts.country,--5
        CASE WHEN lcs.lifecycle_stage = 'MQL' THEN 'MQL'
            ELSE 'influenced'
        END AS lifecycle_stage,--6
        CASE WHEN deal.deal_id IS NOT NULL THEN 'FACILITY'
        WHEN COALESCE(contact_facts.segment, 'UNKNOWN') NOT IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') AND lead_transferred_to_so IS NULL
            AND hs.facility___product_recommended__wf = 'Agenda Premium' THEN 'DOCTOR'
        WHEN contact_facts.country = 'Brazil' AND (contact_facts.sub_brand LIKE '%Feegow%' and feegow_lead_at_test IS NOT NULL) THEN 'FACILITY'
        WHEN COALESCE(contact_facts.segment, 'UNKNOWN') IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1')
            AND hs.facility___product_recommended__wf = 'Agenda Premium' THEN 'DOCTOR'
        WHEN COALESCE(contact_facts.segment, 'UNKNOWN') IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
        WHEN COALESCE(contact_facts.segment, 'UNKNOWN') NOT IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1')
            AND contact_facts.sub_brand IN ('Gipo', 'Clinic Cloud') THEN 'FACILITY'
        WHEN COALESCE(contact_facts.segment, 'UNKNOWN') NOT IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1')
            AND hs.facility___product_recommended__wf IN ('GIPO', 'Clinic Cloud', 'Clinic Agenda', 'S4C/PMS') THEN 'FACILITY'
        ELSE 'DOCTOR'
        END AS target,--7
        deal.mrr_euro_final,--8
        deal.product_won,--9
        CASE WHEN contact_facts.country = 'Brazil'
            AND (hs.feegow_lead_at_test IS NOT NULL) THEN 'check_Feegow'
        WHEN contact_facts.country = 'Spain' /*(lead_transferred_to_so LIKE '%Clinic Cloud%' or hs.sales_pitch_s  LIKE '%ClinicCloud%'  AND (hs.facility___product_recommended__wf LIKE '%Clinic Cloud%'*/
          AND last_sales_team IN (1453567, 271598, 3115413, 2903828) THEN 'Clinic Cloud'
        WHEN contact_facts.country = 'Italy' AND hs.source_so IN ('Contact form GIPO', 'GIPO Contact Form',
        'Product interested S4C/PMS', 'Sales demo ordered GIPO', 'GIPO Demo', 'CallPage GIPO')
            OR lead_business_line_sf = 'GIPO' THEN 'Gipo'
        WHEN contact_facts.country = 'Poland' AND hs.facility___product_recommended__wf LIKE '%MyDr%' THEN 'MyDr'
        WHEN (hs.facility___product_recommended__wf LIKE '%DP Phone%' AND (NOT lead_business_line_sf IN ('Clinic Agenda') OR lead_business_line_sf IS NULL))
            OR (contact_facts.country IN('Spain', 'Brazil', 'Italy')
            AND lead_business_line_sf = 'DP Phone') THEN 'DP Phone'
        WHEN contact_facts.country = 'Poland' AND ((hs.facility_size NOT LIKE ('%Small%') AND hs.facility_size NOT LIKE ('%Individual%')) OR hs.facility_size IS NULL) THEN 'Clinic Agenda'
        WHEN contact_facts.country = 'Brazil' AND ld.status IS NOT NULL
            AND (hs.source_so = 'Marketing e-mail campaigns' OR hs.marketing_action_tag2 LIKE '%BRCLPE_Pricing_Test%' OR hs.marketing_action_tag2 LIKE '%Scoring Mix%') THEN 'br_temp_exclude'
        WHEN contact_type_segment_test IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1')
            AND (NOT lead_business_line_sf IN ('DP Phone', 'GIPO') OR lead_business_line_sf IS NULL)
        AND (hs.lead_status_sf IS NOT NULL OR ld.status IS NOT NULL)
        AND (NOT LOWER(lcs.contact_result_so) LIKE '%transfer to pms%'
            OR LOWER(lcs.contact_result_so) IS NULL) THEN 'Clinic Agenda'
        WHEN contact_type_segment_test IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1')
            AND (NOT lead_business_line_sf IN ('DP Phone', 'GIPO')
            OR lead_business_line_sf IS NULL)
        AND (lcs.hs_rep_name = 'salesforce_owner'
            OR ld.ownerid = '0057Q000008KjPOQA0' OR hs.hubspot_owner_id = 441140868) THEN 'check_lead_transfer'
        WHEN contact_type_segment_test IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1')
            AND (NOT lead_business_line_sf IN ('DP Phone', 'GIPO') OR lead_business_line_sf IS NULL)
        AND hs.lead_status_sf = 'Converted' AND hs.clinic_agenda_opportunity_status_sf = 'Closed Lost' THEN 'check_ca_opp_date'
        WHEN contact_type_segment_test IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1')
            AND (NOT lead_business_line_sf IN ('DP Phone', 'GIPO') OR lead_business_line_sf IS NULL)
            AND hs.bundle_opportunity_status_sf = 'Closed Lost' THEN 'check_bundle_opp_date'
        WHEN contact_type_segment_test IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1')
            AND (NOT lead_business_line_sf IN ('DP Phone', 'GIPO') OR lead_business_line_sf IS NULL)
        AND last_sales_team LIKE '%IT_INT_Smallclinics%' THEN 'Clinic Agenda'
        END AS mql_product, --10
        hs.facility___product_recommended__wf, --11
        COALESCE(CASE WHEN hs.facility_size LIKE ('%Individual%')
                    OR hs.facility_size LIKE ('%Small%') THEN 'Small'
                WHEN hs.facility_size LIKE ('%Large%')
                    OR hs.facility_size LIKE ('%Mid%') THEN 'Medium'
                ELSE 'Unknown'
                END, 'Unknown') AS facility_size,--12
        hs.source_so,--13
        hs.last_source_so,--14
        CASE WHEN contact_facts.mql_channel = 'Offline reference' THEN 'active_source'
        WHEN paid.hubspot_id IS NOT NULL THEN 'active_source'
        WHEN contact_facts.mql_channel = 'Event' THEN 'active_source'
        WHEN LOWER(hs.source_so) IN ('free audit',
                                  'free audit facility',
                                  'conference',
                                  'helpline',
                                  'livechat',
                                  'direct mail',
                                  'feegow interested',
                                  'buy form',
                                  'offer request facility',
                                  'offer request',
                                  'profile premium upgrade - improve bookings',
                                  'offer request cta',
                                  'contact form doctor zone',
                                  'callpage',
                                  'callcenter meetings',
                                  'bundle (ca & feegow) offer request',
                                  'bundle (ca & feegow) interested [whatsapp]',
                                  'integrations',
                                  'facebook ad',
                                  'whatsApp widget',
                                  'telemedicine interested',
                                  'website interested',
                                  'partnership',
                                  'plan 360 offer request',
                                  'profile premium upgrade - phone on profile',
                                  'sales demo ordered feegow',
                                  'demo request saas facility',
                                  'free trial feegow',
                                  'cliniccloud call',
                                  'profile premium upgrade - improve visibility',
                                  'gipo callpage',
                                  'gipo contact form',
                                  'cliniccloud direct email',
                                  'discount offer facility',
                                  'discount offer',
                                  'clinic cloud offer request',
                                  'dp phone free audit',
                                  'dp phone request',
                                  'tuotempo offer request facility',
                                  'fc offer request') THEN 'active_source'
        ELSE 'passive_source'
        END AS active_passive,--15
        CASE WHEN deal.deal_id IS NOT NULL THEN TRUE END AS deal_flag,--16
        deal.month AS deal_month,--17
        CASE WHEN contact_facts.mql_channel = 'Offline reference' THEN 'Referral'
        WHEN paid.hubspot_id IS NOT NULL THEN 'Paid_direct'
        WHEN contact_facts.mql_channel = 'Event' THEN 'Events_direct' -- HS latest source known
        WHEN hs_analytics_source = 'SOCIAL_MEDIA'
            AND LOWER(hs_analytics_source_data_1) IN('facebook', 'instagram', 'linkedin') THEN 'Organic/Direct'
        WHEN hs_analytics_source = 'PAID_SOCIAL'
            AND LOWER(hs_analytics_source_data_1) IN('facebook', 'instagram', 'linkedin') THEN 'Paid'
        WHEN hs_analytics_source = 'PAID_SEARCH'
            AND (LOWER(hs_analytics_source_data_1) IN ('yahoo', 'bing', 'google')
            OR LOWER(hs_analytics_source_data_2) LIKE '%_%'
            OR LOWER(hs_analytics_source_data_1) LIKE '%_%'
            OR LOWER(hs_analytics_source_data_2) = 'google') THEN 'Paid'
        WHEN doctor_facility___marketing_events_tag IS NOT NULL THEN 'Events'
        WHEN hs_analytics_source = 'ORGANIC_SEARCH'
            AND (LOWER(hs_analytics_source_data_1) IN ('google', 'yahoo', 'bing')
            OR LOWER(hs_analytics_source_data_2) IN ('google', 'bing', 'yahoo')) THEN 'Organic/Direct'
        WHEN hs. affiliate_source LIKE '%callcenter%'
            OR LOWER(hs.marketing_action_tag2) LIKE '%callcenter%' THEN 'Call Center'
        WHEN hs_analytics_source = 'DIRECT_TRAFFIC' THEN 'Organic/Direct'
        ELSE 'Organic/Direct'
        END AS db_channel_short,--18
        CASE WHEN lcs.lifecycle_stage = 'MQL' AND lcs_is_month_new THEN lcs.lifecycle_stage_start
        WHEN contact_facts.last_marketing_influenced_at IS NOT NULL THEN contact_facts.last_marketing_influenced_at
        ELSE lcs.last_marketing_influenced_at END AS lifecycle_stage_start, --19
        CASE WHEN lcs.lifecycle_stage = 'MQL'
            AND lcs_is_month_new THEN lcs.month
        WHEN lcs.last_marketing_influenced_at IS NOT NULL THEN  DATE_TRUNC('month', lcs.last_marketing_influenced_at)
        ELSE DATE_TRUNC('month', contact_facts.last_marketing_influenced_at)
        END AS MONTH,--20
        lead_transferred_to_so,--21
        deal.deal_stage_start,--22
        contact_result_so_at,--23
        hs.feegow_lead_at_test,--24
        DATE_TRUNC('month', DATE(ld.leadassignedat__c)) AS leadassignedat__c,--25
        hs.facility_lead_transferred_to_so_wf --26
    FROM mart_cust_journey.cj_lcs_month lcs
    INNER JOIN mart_cust_journey.cj_contact_facts contact_facts ON contact_facts.contact_id = lcs.contact_id
    AND contact_facts.lead_id = lcs.lead_id
    LEFT JOIN dw.hs_contact_live hs ON hs.hubspot_id = lcs.contact_id
    LEFT JOIN WON_deals deal ON lcs.contact_id = deal.hs_contact_id
    LEFT JOIN dp_salesforce.lead ld ON ld.hs_contact_id__c =lcs.contact_id
    LEFT JOIN mart_cust_journey.msv_paid_media_campaign_hubspot_2 paid ON lcs.contact_id =paid.hubspot_id
        AND lcs.lifecycle_stage = 'MQL' AND lcs_is_month_new
        AND lcs.lifecycle_stage_start BETWEEN paid.web_date AND paid.web_date::DATE + INTERVAL '30 day'
        AND campaign like '%mql%'
        AND (lcs.month = DATE_TRUNC('month', paid.date)
            OR DATE_TRUNC('month', contact_facts.last_marketing_influenced_at)= DATE_TRUNC('month', paid.date))
   WHERE ((lcs.lifecycle_stage IN ('MQL')
           AND contact_facts.country IN ('Colombia', 'Spain', 'Mexico', 'Brazil', 'Italy', 'Poland')
           AND lcs_is_month_new)
 OR (lcs.last_marketing_influenced_at IS NOT NULL  AND contact_facts.country IN ('Colombia', 'Spain', 'Mexico', 'Brazil', 'Italy', 'Poland'))
          OR (contact_facts.last_marketing_influenced_at IS NOT NULL
              AND contact_facts.country IN ('Colombia', 'Spain', 'Mexico', 'Brazil', 'Italy', 'Poland')))
),

    influenced_wo_pre_history AS (
    SELECT
        lcs.contact_id,--1
        lcs.lead_id,--2
        deal.deal_id,--3
        hs.email AS hs_email,--4
        contact_facts.country,--5
        'influenced'::varchar AS lifecycle_stage,--6
        CASE WHEN deal.deal_id IS NOT NULL THEN 'FACILITY'
        WHEN COALESCE(contact_facts.segment, 'UNKNOWN') NOT IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') AND lead_transferred_to_so IS NULL
            AND hs.facility___product_recommended__wf = 'Agenda Premium' THEN 'DOCTOR'
        WHEN contact_facts.country = 'Brazil'
            AND (contact_facts.sub_brand LIKE '%Feegow%' and feegow_lead_at_test IS NOT NULL) THEN 'FACILITY'
        WHEN COALESCE(contact_facts.segment, 'UNKNOWN') IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1')
            AND hs.facility___product_recommended__wf = 'Agenda Premium' THEN 'DOCTOR'
        WHEN COALESCE(contact_facts.segment, 'UNKNOWN') IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
        WHEN COALESCE(contact_facts.segment, 'UNKNOWN') NOT IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1')
            AND contact_facts.sub_brand IN ('Gipo', 'Clinic Cloud') THEN 'FACILITY'
        WHEN COALESCE(contact_facts.segment, 'UNKNOWN') NOT IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1')
        AND hs.facility___product_recommended__wf IN ('GIPO', 'Clinic Cloud', 'Clinic Agenda', 'S4C/PMS') THEN 'FACILITY'
        ELSE 'DOCTOR'
        END AS target,--7
        deal.mrr_euro_final,--8
        deal.product_won,--9
        CASE WHEN contact_facts.country = 'Brazil'
            AND (hs.feegow_lead_at_test IS NOT NULL) THEN 'check_Feegow'
        WHEN contact_facts.country = 'Spain' /*(lead_transferred_to_so LIKE '%Clinic Cloud%' or hs.sales_pitch_s  LIKE '%ClinicCloud%'  AND (hs.facility___product_recommended__wf LIKE '%Clinic Cloud%'*/
            AND last_sales_team IN (1453567, 271598, 3115413, 2903828) THEN 'Clinic Cloud'
        WHEN contact_facts.country = 'Italy'
            AND hs.source_so IN ('Contact form GIPO', 'GIPO Contact Form', 'Product interested S4C/PMS', 'Sales demo ordered GIPO', 'GIPO Demo', 'CallPage GIPO')
            OR lead_business_line_sf = 'GIPO' THEN 'Gipo'
        WHEN contact_facts.country = 'Poland' AND hs.facility___product_recommended__wf LIKE '%MyDr%' THEN 'MyDr'
        WHEN (hs.facility___product_recommended__wf LIKE '%DP Phone%' AND (NOT lead_business_line_sf IN ('Clinic Agenda')
            OR lead_business_line_sf IS NULL))
            OR (contact_facts.country IN ('Spain', 'Brazil', 'Italy')
            AND lead_business_line_sf = 'DP Phone') THEN 'DP Phone'
        WHEN contact_facts.country = 'Poland' AND ((hs.facility_size NOT LIKE ('%Small%') AND hs.facility_size NOT LIKE ('%Individual%')) OR hs.facility_size IS NULL) THEN 'Clinic Agenda'
        WHEN contact_facts.country = 'Brazil' AND ld.status IS NOT NULL
            AND (hs.source_so = 'Marketing e-mail campaigns'
                OR hs.marketing_action_tag2 LIKE '%BRCLPE_Pricing_Test%'
                OR hs.marketing_action_tag2 LIKE '%Scoring Mix%') THEN 'br_temp_exclude'
        WHEN contact_type_segment_test IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1')
        AND (NOT lead_business_line_sf IN ('DP Phone', 'GIPO') OR lead_business_line_sf IS NULL)
        AND (hs.lead_status_sf IS NOT NULL OR ld.status IS NOT NULL)
        AND (NOT LOWER(lcs.contact_result_so) LIKE '%transfer to pms%'
            OR LOWER(lcs.contact_result_so) IS NULL) THEN 'Clinic Agenda'
        WHEN contact_type_segment_test IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1')
            AND (NOT lead_business_line_sf IN ('DP Phone', 'GIPO') OR lead_business_line_sf IS NULL)
            AND (lcs.hs_rep_name = 'salesforce_owner' OR ld.ownerid = '0057Q000008KjPOQA0'
               OR hs.hubspot_owner_id = 441140868) THEN 'check_lead_transfer'
        WHEN contact_type_segment_test IN ('FACILITY',  'DOCTOR&FACILITY - 2IN1')
            AND (NOT lead_business_line_sf IN ('DP Phone', 'GIPO')
                OR lead_business_line_sf IS NULL)
            AND hs.lead_status_sf = 'Converted'
            AND hs.clinic_agenda_opportunity_status_sf = 'Closed Lost' THEN 'check_ca_opp_date'
        WHEN contact_type_segment_test IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1')
            AND (NOT lead_business_line_sf IN ('DP Phone', 'GIPO')
                OR lead_business_line_sf IS NULL)
          AND hs.bundle_opportunity_status_sf = 'Closed Lost' THEN 'check_bundle_opp_date'
        WHEN contact_type_segment_test IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1')
            AND (NOT lead_business_line_sf IN ('DP Phone', 'GIPO')
                OR lead_business_line_sf IS NULL)
        AND last_sales_team LIKE '%IT_INT_Smallclinics%' THEN 'Clinic Agenda'
        END AS mql_product, --10
        hs.facility___product_recommended__wf, --11
        COALESCE(CASE WHEN hs.facility_size LIKE ('%Individual%') OR hs.facility_size LIKE ('%Small%') THEN 'Small'
        WHEN hs.facility_size LIKE ('%Large%')
            OR hs.facility_size LIKE ('%Mid%') THEN 'Medium'
        ELSE 'Unknown'
        END, 'Unknown') AS facility_size,--12
        hs.source_so,--13
        hs.last_source_so,--14
        CASE WHEN contact_facts.mql_channel = 'Offline reference' THEN 'active_source'
        WHEN paid.hubspot_id IS NOT NULL THEN 'active_source'
        WHEN contact_facts.mql_channel = 'Event' THEN 'active_source'
        WHEN LOWER(hs.source_so) IN ('free audit',
                                  'free audit facility',
                                  'conference',
                                  'helpline',
                                  'livechat',
                                  'direct mail',
                                  'feegow interested',
                                  'buy form',
                                  'offer request facility',
                                  'offer request',
                                  'profile premium upgrade - improve bookings',
                                  'offer request cta',
                                  'contact form doctor zone',
                                  'callpage',
                                  'customer reference',
                                  'callcenter meetings',
                                  'bundle (ca & feegow) offer request',
                                  'bundle (ca & feegow) interested [whatsapp]',
                                  'integrations',
                                  'facebook ad',
                                  'whatsApp widget',
                                  'telemedicine interested',
                                  'website interested',
                                  'partnership',
                                  'plan 360 offer request',
                                  'profile premium upgrade - phone on profile',
                                  'sales demo ordered feegow',
                                  'demo request saas facility',
                                  'free trial feegow',
                                  'cliniccloud call',
                                  'profile premium upgrade - improve visibility',
                                  'gipo callpage',
                                  'gipo contact form',
                                  'cliniccloud direct email',
                                  'discount offer facility',
                                  'discount offer',
                                  'clinic cloud offer request',
                                  'dp phone free audit',
                                  'dp phone request',
                                  'tuotempo offer request facility',
                                  'fc offer request') THEN 'active_source'
        ELSE 'passive_source'
        END AS active_passive,--15
        CASE WHEN deal.deal_id IS NOT NULL THEN TRUE END AS deal_flag,--16
        deal.month AS deal_month,--17
        CASE WHEN contact_facts.mql_channel = 'Offline reference' THEN 'Referral'
        WHEN paid.hubspot_id IS NOT NULL THEN 'Paid_direct'
        WHEN contact_facts.mql_channel = 'Event' THEN 'Events_direct'
        WHEN hs_analytics_source = 'SOCIAL_MEDIA'
            AND LOWER(hs_analytics_source_data_1) IN('facebook', 'instagram', 'linkedin') THEN 'Organic/Direct'
        WHEN hs_analytics_source = 'PAID_SOCIAL' AND LOWER(hs_analytics_source_data_1) IN('facebook', 'instagram', 'linkedin') THEN 'Paid'
        WHEN hs_analytics_source = 'PAID_SEARCH' AND (LOWER(hs_analytics_source_data_1) IN ('yahoo', 'bing', 'google')
            OR LOWER(hs_analytics_source_data_2) like '%_%'
            OR LOWER(hs_analytics_source_data_1) like '%_%'
            OR LOWER(hs_analytics_source_data_2) = 'google') THEN 'Paid'
        WHEN doctor_facility___marketing_events_tag IS NOT NULL THEN 'Events'
        WHEN hs_analytics_source = 'ORGANIC_SEARCH'
            AND (LOWER(hs_analytics_source_data_1) IN ('google', 'yahoo', 'bing')
                OR LOWER(hs_analytics_source_data_2) IN ('google', 'bing', 'yahoo')) THEN 'Organic/Direct'
        WHEN hs. affiliate_source like '%callcenter%' OR hs.marketing_action_tag2 LIKE '%callcenter%' THEN 'Call Center'
        WHEN hs_analytics_source = 'DIRECT_TRAFFIC' THEN 'Organic/Direct'
        ELSE 'Organic/Direct'
        END AS db_channel_short,--18
        marketing_influenced_at AS lifecycle_stage_start,--19
        DATE_TRUNC('month', marketing_influenced_at) AS month,--20
        lead_transferred_to_so,--21
        deal.deal_stage_start,--22
        contact_result_so_at,--23
        hs.feegow_lead_at_test,--24
        DATE_TRUNC('month', DATE(ld.leadassignedat__c)) AS leadassignedat__c,--25
        hs.facility_lead_transferred_to_so_wf --26
        FROM mart_cust_journey.cj_lcs_month lcs
        INNER JOIN mart_cust_journey.cj_contact_facts contact_facts ON contact_facts.contact_id = lcs.contact_id
            AND contact_facts.lead_id = lcs.lead_id
        LEFT JOIN dw.hs_contact_live hs ON lcs.contact_id = hs.hubspot_id
        LEFT JOIN WON_deals deal ON lcs.contact_id = deal.hs_contact_id
        LEFT JOIN dp_salesforce.lead ld ON ld.hs_contact_id__c =lcs.contact_id
        LEFT JOIN mart_cust_journey.msv_paid_media_campaign_hubspot_2 paid ON lcs.contact_id =paid.hubspot_id
            AND lcs.lifecycle_stage = 'MQL' AND lcs_is_month_new
            AND lcs.lifecycle_stage_start BETWEEN paid.web_date AND paid.web_date::DATE + INTERVAL '30 day'
            AND paid.campaign like '%mql%'
            AND (lcs.month = DATE_TRUNC('month', paid.date)
                OR DATE_TRUNC('month', contact_facts.last_marketing_influenced_at)= DATE_TRUNC('month', paid.date))
        WHERE (marketing_influenced_at IS NOT NULL
        AND contact_facts.country IN ('Colombia', 'Spain', 'Mexico', 'Brazil', 'Italy', 'Poland'))
),
    
only_wons AS ( 
    SELECT
        COALESCE(CAST(deal.hs_contact_id AS bigint), 0) AS hs_contact_id,--1
        deal.lead_id,--2
        deal.deal_id,--3
        hs.email AS hs_email,--4
        deal.country,--5
        'won'::varchar AS lifecycle_stage,--6
        'FACILITY'::varchar AS target,--7
        deal.mrr_euro_final,--8
        deal.product_won,--9
        'unknown'::varchar AS mql_product,--10
        hs.facility___product_recommended__wf,--11
        COALESCE(CASE WHEN hs.facility_size LIKE ('%Individual%')
                    OR hs.facility_size LIKE ('%Small%') THEN 'Small'
                WHEN hs.facility_size LIKE ('%Large%')
                   OR hs.facility_size LIKE ('%Mid%') THEN 'Medium'
                ELSE 'Unknown'
                END, 'Unknown') AS facility_size,--12
        hs.source_so,--13
        hs.last_source_so,--14
        CASE WHEN contact_facts.mql_channel = 'Offline reference' THEN 'active_source'
        WHEN paid.hubspot_id IS NOT NULL THEN 'active_source'
        WHEN contact_facts.mql_channel = 'Event' THEN 'active_source'
        WHEN LOWER(hs.source_so) IN ('free audit',
                                  'free audit facility',
                                  'conference',
                                  'helpline',
                                  'livechat',
                                  'direct mail',
                                  'feegow interested',
                                  'buy form',
                                  'offer request facility',
                                  'offer request',
                                  'profile premium upgrade - improve bookings',
                                  'offer request cta',
                                  'contact form doctor zone',
                                  'callpage',
                                  'customer reference',
                                  'callcenter meetings',
                                  'bundle (ca & feegow) offer request',
                                  'bundle (ca & feegow) interested [whatsapp]',
                                  'integrations',
                                  'facebook ad',
                                  'whatsApp widget',
                                  'telemedicine interested',
                                  'website interested',
                                  'partnership',
                                  'plan 360 offer request',
                                  'profile premium upgrade - phone on profile',
                                  'sales demo ordered feegow',
                                  'demo request saas facility',
                                  'free trial feegow',
                                  'cliniccloud call',
                                  'profile premium upgrade - improve visibility',
                                  'gipo callpage',
                                  'gipo contact form',
                                  'cliniccloud direct email',
                                  'discount offer facility',
                                  'discount offer',
                                  'clinic cloud offer request',
                                  'dp phone free audit',
                                  'dp phone request',
                                  'tuotempo offer request facility',
                                  'fc offer request') THEN 'active_source'
        ELSE 'passive_source'
        END AS active_passive,--15
        CASE WHEN deal.deal_id IS NOT NULL THEN TRUE END AS deal_flag, --16
        deal.month AS deal_month, --17
        CASE WHEN contact_facts.mql_channel = 'Offline reference' THEN 'Referral'
        WHEN paid.hubspot_id IS NOT NULL THEN 'Paid_direct'
        WHEN contact_facts.mql_channel = 'Event' THEN 'Events_direct'
        WHEN hs_analytics_source = 'SOCIAL_MEDIA'
            AND LOWER(hs_analytics_source_data_1) IN ('facebook', 'instagram', 'linkedin') THEN 'Organic/Direct'
        WHEN hs_analytics_source = 'PAID_SOCIAL'
            AND LOWER(hs_analytics_source_data_1) IN ('facebook', 'instagram', 'linkedin') THEN 'Paid'
        WHEN hs_analytics_source = 'PAID_SEARCH' AND (LOWER(hs_analytics_source_data_1) IN ('yahoo', 'bing', 'google')
            OR LOWER(hs_analytics_source_data_2) LIKE '%_%'
            OR LOWER(hs_analytics_source_data_1) LIKE '%_%'
            OR LOWER(hs_analytics_source_data_2) = 'google') THEN 'Paid'
        WHEN doctor_facility___marketing_events_tag IS NOT NULL THEN 'Events'
        WHEN hs_analytics_source = 'ORGANIC_SEARCH'
            AND (LOWER(hs_analytics_source_data_1) IN ('google', 'yahoo', 'bing')
                OR LOWER(hs_analytics_source_data_2) IN ('google', 'bing', 'yahoo')) THEN 'Organic/Direct'
        WHEN hs.affiliate_source like '%callcenter%' OR LOWER(hs.marketing_action_tag2) LIKE '%callcenter%' THEN 'Call Center'
        WHEN hs_analytics_source = 'DIRECT_TRAFFIC' THEN 'Organic/Direct'
        ELSE 'Organic/Direct'
        END AS db_channel_short,--18
        deal.deal_stage_start AS lifecycle_stage_start,--19
        DATE_TRUNC('month', deal.month) AS MONTH,--20
        lead_transferred_to_so,--21
        deal.deal_stage_start,--22
        DATE_TRUNC('month', deal.month) AS contact_result_so_at,--23
        hs.feegow_lead_at_test,--24
        DATE_TRUNC('month', deal.month) AS leadassignedat__c,--25
        hs.facility_lead_transferred_to_so_wf --26
    FROM WON_deals deal
    LEFT JOIN mart_cust_journey.cj_contact_facts contact_facts ON contact_facts.contact_id = deal.hs_contact_id
        AND contact_facts.lead_id = deal.lead_id
    LEFT JOIN dw.hs_contact_live hs ON deal.hs_contact_id = hs.hubspot_id
    LEFT JOIN dp_salesforce.opportunity sf ON sf.id = deal.deal_id
    LEFT JOIN mart_cust_journey.msv_paid_media_campaign_hubspot_2 paid ON deal.hs_contact_id =paid.hubspot_id
        AND deal.deal_stage_start BETWEEN paid.web_date AND paid.web_date::DATE + INTERVAL '30 day'
        AND campaign like '%mql%'
        AND (deal.month >= DATE_TRUNC('month', paid.date))
),
    
total_mqls AS (
    SELECT * FROM all_mqls
    GROUP BY 1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            16,
            17,
            18,
            19,
            20,
            21,
            22,
            23,
            24,
            25,
            26
    UNION 
    SELECT *
    FROM influenced_wo_pre_history
    GROUP BY 1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            16,
            17,
            18,
            19,
            20,
            21,
            22,
            23,
            24,
            25,
            26
    UNION
    SELECT *
    FROM only_wons
    GROUP BY 1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            16,
            17,
            18,
            19,
            20,
            21,
            22,
            23,
            24,
            25,
            26)
SELECT 
    mql.contact_id,
    mql.lead_id,
    mql.deal_id,
    mql.hs_email,
    mql.country,
    mql.lead_transferred_to_so,
    mql.lifecycle_stage,
    mql.target,
    mql.product_won,
    mql.mql_product,
    mql.facility___product_recommended__wf,
    mql.facility_size,
    mql.source_so,
    mql.last_source_so,
    mql.active_passive,
    mql.deal_flag,
    mql.deal_month,
    mql.db_channel_short,
    mql.month,
    mql.mrr_euro_final,
    mql.facility_lead_transferred_to_so_wf AS lead_transferred_at,
    CASE WHEN mql_product = 'check_Feegow'
        AND (DATE_TRUNC('month', mql.feegow_lead_at_test) = MONTH) THEN 'Feegow'
    WHEN mql_product IN ('Clinic Cloud', 'MyDr', 'Gipo', 'DP Phone') THEN mql_product
    WHEN mql_product IN ('Clinic Agenda', 'check_Feegow')
    AND (NOT(DATE_TRUNC('month', mql.facility_lead_transferred_to_so_wf) = MONTH
    AND mql.lead_transferred_to_so = 'Marketplace Team')
        OR mql.facility_lead_transferred_to_so_wf IS NULL) THEN 'Clinic Agenda'
    WHEN mql_product = 'check_ca_opp_date'
        AND MONTH = DATE_TRUNC('month', hs.clinic_agenda_opportunity_close_date_sf) THEN 'Clinic Agenda'
    WHEN mql_product = 'check_lead_transfer'
        AND (NOT(DATE_TRUNC('month', mql.facility_lead_transferred_to_so_wf) = MONTH
        AND mql.lead_transferred_to_so = 'Marketplace Team')
        OR mql.facility_lead_transferred_to_so_wf IS NULL) THEN 'Clinic Agenda'
    WHEN mql_product = 'check_bundle_opp_date'
        AND MONTH = DATE_TRUNC('month', hs.bundle_opportunity_close_date_sf) 
    THEN 'Clinic Agenda'
    END AS mql_product_test, --additional calculations that involve dates
    MAX(db_channel_short) OVER (PARTITION BY contact_id, MONTH) AS max_channel,
    CASE WHEN deal_flag AND deal_stage_start >=lifecycle_stage_start THEN TRUE ELSE FALSE
    END AS true_deal_flag
FROM total_mqls mql
LEFT JOIN dw.hs_contact_live hs ON mql.contact_id = hs.hubspot_id
WHERE target = 'FACILITY' --and won_source = 'Inbound' --and mql_product IS NOT NULL
AND NOT (hs_email LIKE '%deleted%' AND deal_flag IS FALSE)
