-- cj_mqls_monthly
DROP TABLE IF EXISTS test.cj_mqls_monthly_gross_1;

CREATE TABLE test.cj_mqls_monthly_gross_1
AS
WITH
won_deals AS ( --first we select all the valid deals that arrived to Won stage and bought an Individual product(inclusive of e-commerce) - for CVR calculations
    SELECT
        deal.opportunity_id AS deal_id,
        deal.pipeline,
        deal.pipeline_type,
        CASE WHEN deal.country = 'Mexico' AND NVL(deal.contract_signed_date, deal.active_won_date) >= '2025-07-13' THEN --on 06.08 logic changed to consider contract signed as date of WON source of truth for Agenda Premium in SF (atm only MX)
            DATE_TRUNC('month', NVL(deal.contract_signed_date, deal.active_won_date, deal.closedate))::DATE ELSE
            DATE_TRUNC('month', NVL(deal.active_won_date, deal.closedate))::DATE END AS deal_month,
        deal.hs_contact_id,
        CASE WHEN deal.country = 'Mexico' AND NVL(deal.contract_signed_date, deal.active_won_date, deal.closedate) >= '2025-07-13' THEN NVL(deal.contract_signed_date, deal.active_won_date, deal.closedate)
        ELSE NVL(deal.active_won_date, deal.closedate) END AS deal_stage_start,
        deal.country,
        deal.hs_lead_id AS lead_id,
        live."tag" AS deal_tag,
        deal.closedate::DATE,
        live.last_source_so AS demo_ls,
        MAX(deal.mrr_euro) OVER (PARTITION BY deal.opportunity_id) AS mrr_euro_final, --this is to avoid duplicating MRR
        MAX(deal.mrr_original_currency) OVER (PARTITION BY deal.opportunity_id) AS mrr_original_currency,
        CASE WHEN e.hubspot_id IS NOT NUll THEN TRUE ELSE FALSE END AS ecommerce_deal_flag,
        crm_source,
        active_won_date,
        sf.last_source__c AS sf_opportunity_last_source,
        ld.last_source__c AS sf_lead_last_source
    FROM cj_data_layer.cj_aux_opportunity_facts deal
    LEFT JOIN dw.hs_deal_live live ON live.hubspot_id = deal.opportunity_id
    LEFT JOIN dp_salesforce.opportunity sf ON sf.id = deal.opportunity_id AND stagename IN ('Closed Won', 'Active/Won', 'Closed Lost', 'Back to Sales', 'Contract Signed')
    LEFT JOIN dp_salesforce.lead ld ON ld.convertedopportunityid = sf.id
    LEFT JOIN mart.ecommerce e ON deal.hs_contact_id = e.hubspot_id AND DATE_TRUNC('month', NVL(deal.active_won_date,deal.closedate))::DATE = DATE_TRUNC('month',deal_created_at)
    WHERE deal.country = 'Turkiye' AND deal.pipeline_type = 'Individual New Sales' AND deal.is_gross_won AND deal.current_stage IN ('Closed Won','Active/Won', 'Closed Lost', 'Back to Sales')
    AND DATE_TRUNC('month', NVL(deal.active_won_date, deal.closedate))::DATE >= '2024-01-01'
AND crm_source IN ('hubspot'))

     /*   (((deal.country = 'Mexico' AND NVL(deal.contract_signed_date, deal.active_won_date, deal.closedate)::DATE >= '2025-07-13' AND deal.pipeline = 'Agenda Premium' AND deal.pipeline_type = 'Individual New Sales' AND (deal.was_contract_signed = 1 OR deal.was_active_won = 1 OR deal.current_stage = 'Closed Won') AND e.hubspot_id IS NULL)
        OR (((deal.country = 'Mexico' AND NVL(deal.active_won_date, deal.closedate)::DATE < '2025-07-13') OR deal.country != 'Mexico') AND
        deal.current_stage IN ('Closed Won') AND deal.pipeline_type = 'Individual New Sales' AND crm_source IN ('hubspot', 'hays')
      AND DATE_TRUNC('month', NVL(deal.active_won_date, deal.closedate))::DATE >= '2024-01-01') --selecting HS source/logic for all countries except Mexico from 13.07(post-migration)
    OR (deal.country = 'Mexico' AND NVL(deal.active_won_date, deal.closedate)::DATE >= '2025-07-13' AND deal.pipeline_type = 'Individual New Sales' AND e.hubspot_id IS NOT NULL AND deal.current_stage IN ('Closed Won') AND deal.crm_source = 'hays'))  --including ecommerce for MX post migration
     OR (deal.country = 'Turkiye' AND deal.is_gross_won  AND deal.pipeline_type = 'Individual New Sales' AND crm_source IN ('hubspot')))
), --for the purposes of the budget report we only care about 2024 and later*/

--won_deals_ls AS ( --last source for HS deals is taken at a later date from historical values due to manual updates
    SELECT
        deal.*,
        hsh.email AS hs_email,
        CASE WHEN (deal.country IN ('Brazil','Mexico','Poland', 'Chile'))
                    AND hsh.spec_split_test = 'Medical' THEN 'Medical'
                WHEN (deal.country IN ('Brazil','Mexico','Poland', 'Chile'))
                    AND (hsh.spec_split_test = 'Paramedical'
                    OR hsh.spec_split_test IS NULL) THEN 'Paramedical'
                 WHEN deal.country = 'Italy' AND COALESCE(hsh.spec_split_test, hcl.spec_split_test) = 'Paramedical' THEN 'Paramedical' --pulling live values for IT as property was updated on 17.09
                ELSE 'None'
            END AS new_spec,
        hsh.contact_type_segment_test AS segment,
        COALESCE(DATE_TRUNC('month', hsh.feegow_lead_at_test), '2015-01-01') AS feegow_month,
        CASE WHEN hsh.verified THEN 'Verified' ELSE 'Not Verified' END AS verified,
        CASE WHEN hsh.mql_last_touch_channel_wf  = 'Offline reference' THEN 'active_source'
                WHEN hsh.mql_last_touch_channel_wf = 'Event' THEN 'active_source'
                WHEN LOWER(hsh.source_so) IN (
                    'free audit',
                    'free audit facility'
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
                    'whatsapp widget',
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
            END AS active_passive_aux,
        hsh.active_passive_lead,
        hsh.marketplace_mql_flag,
        hsh.mql_last_conversion_place_wf AS mql_conversion_place,
        hsh.mql_last_touch_channel_wf AS mql_last_touch_channel,
        hsh.last_source_so AS frozen_last_source,
        hsh.source_so AS frozen_source,
        CASE WHEN hsh.mql_last_touch_channel_wf = 'Offline reference' THEN 'Referral'
                WHEN hsh.mql_last_touch_channel_wf = 'Event' THEN 'Events_direct'
                WHEN hsh.hs_analytics_source = 'PAID_SOCIAL'
                    AND LOWER(hsh.hs_analytics_source_data_1) IN ('facebook','instagram','linkedin' ) THEN 'Paid'
                WHEN hsh.hs_analytics_source = 'PAID_SEARCH'
                    AND (LOWER(hsh.hs_analytics_source_data_1) IN ( 'yahoo','bing','google')
                        OR LOWER(hsh.hs_analytics_source_data_2) LIKE '%_%'
                        OR LOWER(hsh.hs_analytics_source_data_1) LIKE '%_%'
                        OR LOWER(hsh.hs_analytics_source_data_2) = 'google' ) THEN 'Paid'
                WHEN hsh.doctor_facility___marketing_events_tag IS NOT NULL THEN 'Events'
                WHEN hsh.affiliate_source LIKE '%callcenter%' OR hsh.marketing_action_tag2 LIKE '%callcenter%' THEN 'Call Center'
                ELSE 'Organic/Direct'
            END AS db_channel_short_aux,
        CASE WHEN
            deal.country = 'Mexico' AND deal.deal_stage_start >= '2025-07-13' AND
            (deal.sf_lead_last_source NOT IN ('Basic reference', 'Target tool [DA]', 'Sales Contact', 'Sales Contact [Outbound]', 'Target Tool [Outbound]') OR deal.sf_opportunity_last_source NOT IN ('Basic reference', 'Target tool [DA]', 'Sales Contact', 'Sales Contact [Outbound]', 'Target Tool [Outbound]'))
            THEN 'Inbound'
            WHEN (deal.country IN ('Colombia', 'Spain', 'Brazil', 'Poland', 'Argentina', 'Chile', 'Peru') OR (deal.country = 'Mexico' AND deal.deal_stage_start < '2025-07-13'))
                AND ((hsh.source_so NOT IN ('basic reference', 'Target tool [DA]', 'Sales Contact'))
                    OR (hsh.last_source_so NOT IN ('basic reference', 'Target tool [DA]', 'Sales Contact', 'visiting pricing')))
                THEN 'Inbound'
                WHEN deal.country IN ('Turkiye','Germany')
                    AND (hsh.last_source_so NOT IN ('Target tool [DA]', 'Sales Contact')
                        OR hsh.source_so NOT IN ('Target tool [DA]', 'Sales Contact'))
                THEN 'Inbound'
                WHEN deal.country = 'Italy' AND (COALESCE(hsh.contact_type_segment_test, 'UNKNOWN') IN ('GENERAL PRACTITIONER', 'GENERAL PRACTITIONER & DOCTOR'))
                    AND (hsh.source_so NOT IN (
                        'Target tool [DA]',
                        'Sales Contact',
                        'basic reference',
                        'new facility verification',
                        'Massive assignment',
                        'other')
                        OR hsh.last_source_so NOT IN (
                        'Target tool [DA]',
                        'Sales Contact',
                        'basic reference',
                        'Massive assignment',
                        'new facility verification',
                        'other'))
                THEN 'Inbound'
                WHEN deal.country = 'Italy'
                    AND (hsh.source_so NOT IN (
                        'Target tool [DA]',
                        'Sales Contact',
                        'basic reference',
                        'other',
                        'new facility verification',
                        'Massive assignment',
                        'New verification',
                        'other')
                        OR hsh.last_source_so NOT IN (
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
                WHEN deal_tag LIKE '%inbound%' OR deal.deal_tag LIKE '%mixed%' THEN 'Inbound'
                ELSE 'Outbound'
            END AS deal_allocation,
        hsh.sub_brand_wf_test as sub_brand,
        hsh.contact_result_so_at,
        hsh.contact_result_so,
        hsh.customer_reference_at,
        ROW_NUMBER() OVER (PARTITION BY deal.deal_id ORDER BY hsh.start_date DESC) as row
    FROM won_deals deal
    LEFT JOIN dw.hs_contact_live_history hsh ON hsh.hubspot_id = deal.hs_contact_id AND (hsh.start_date BETWEEN deal.deal_month AND DATEADD(day, 50, DATE_TRUNC('month',deal.deal_month::DATE))
        OR hsh.last_source_so_at BETWEEN DATEADD(day, -30, DATE_TRUNC('month',deal.deal_month::DATE)) AND DATEADD(day, 50, DATE_TRUNC('month',deal.deal_month::DATE))) --temp workaround to catch merged MQLs and get their correct last source as otherwise we lose them
    LEFT JOIN dw.hs_contact_live hcl ON hcl.hubspot_id = hsh.hubspot_id
    WHERE 1=1
    QUALIFY row = 1 AND (ecommerce_deal_flag OR deal_allocation = 'Inbound')
    AND (frozen_last_source IS NOT NULL OR ecommerce_deal_flag OR crm_source = 'salesforce') --trying to catch all Inbound deals even if they dont MQL for any reason
    AND (hsh.spec_split_test IS NULL OR hsh.spec_split_test != 'Bad Paramedical')
