
DROP TABLE IF EXISTS test.msv_paid_media_campaign_hubspot_mals;
--work on excluding PMS sources from MQL query or they will mess up Paid
CREATE TABLE test.msv_paid_media_campaign_hubspot_mals AS
WITH all_valid_mals_mqls AS ( --we select all contacts with an MAL or MQL stage who arent patients
    SELECT
        contact.hubspot_id::VARCHAR AS hubspot_id,
        contact.hubspot_id::VARCHAR AS lead_id,
        contact.verified,
        'MAL'::VARCHAR AS lifecycle_stage,
        contact.mkt_db_day AS lcs_date,
        contact.mkt_db_month AS lcs_month,
        contact.country,
        contact.segment_funnel,
        --contact.segment AS contact_type,ignore for now
        CASE WHEN contact.target IN ('Small', 'Medium', 'Unknown') THEN contact.target ELSE 'Undefined' END AS facility_size,
        CASE WHEN contact.segment_funnel IN ('Individuals Core') AND contact.country IN ('Brazil', 'Mexico', 'Poland', 'Chile', 'Italy') AND contact.target = 'Paramedics' THEN 'Paramedical'
        WHEN contact.segment_funnel IN ('Individuals Core') AND contact.country IN ('Brazil', 'Mexico', 'Poland', 'Chile', 'Italy') AND contact.target = 'Doctors'THEN 'Medical'
        ELSE 'Undefined' END AS specialization,
        CASE WHEN contact.target = 'Noa Notes' THEN 'Noa Notes'
            WHEN contact.segment_funnel = 'Individuals Core' THEN 'Agenda Premium'
            WHEN contact.segment_funnel = 'Individuals GPs' THEN 'Agenda Premium for GPs'
            WHEN contact.segment_funnel = 'Clinics PRS' THEN 'Clinic Agenda'
            WHEN contact.segment_funnel = 'Clinics PMS' AND contact.country = 'Brazil' THEN 'Feegow'
            WHEN contact.segment_funnel = 'Clinics PMS' AND contact.country = 'Italy' THEN 'Gipo'
            WHEN contact.segment_funnel = 'Clinics PMS' AND contact.country = 'Spain' THEN 'Clinic Cloud'
            WHEN contact.segment_funnel = 'Clinics PMS' AND contact.country = 'Poland' THEN 'MyDr'
            END AS product
    FROM mart_cust_journey.new_marketing_database_contacts contact
    --LEFT JOIN dw.hs_contact_live_history hclh
      --  ON hclh.hubspot_id = contact.hubspot_id
        --    AND hclh.start_date BETWEEN contact.mkt_db_day AND DATEADD(day, 31, contact.mkt_db_day)
    WHERE 1 = 1
    UNION ALL
        SELECT
        contact.contact_id::VARCHAR AS hubspot_id,
        contact.mql_id::VARCHAR AS lead_id, --needed for PMS based on tickets
        contact.verified,
        'MQL'::VARCHAR AS lifecycle_stage,
        CASE WHEN mql_product = 'Feegow' THEN DATEADD(day, 1, contact.mql_day::DATE) ELSE contact.mql_day END AS lcs_date, --ticket create date gets adjusted to BR time by WF but other dates dont, this messes up attribution. the ticket create date is adjusted
        CASE WHEN mql_product = 'Feegow' THEN DATE_TRUNC('month', DATEADD(day, 1, contact.mql_day::DATE)) ELSE contact.mql_month END AS lcs_month,
        contact.country,
        contact.segment_funnel,
       -- hclh.contact_type_segment_test AS contact_type, --add field to MQL table and replace ignore for now
        contact.facility_size,
        contact.specialization,
        contact.mql_product AS product
        --ROW_NUMBER() OVER (PARTITION BY contact.hubspot_id, contact.mkt_db_day ORDER BY hclh.start_date DESC) AS row
    FROM test.inbound_all_mqls contact
    WHERE 1 = 1
    UNION ALL
        SELECT
        contact.hubspot_id::VARCHAR,
        contact.hubspot_id::VARCHAR AS lead_id,
        'Unverified'::VARCHAR AS verified, --temporary
        'MQL'::VARCHAR AS lifecycle_stage,
        CASE WHEN country IN ('Brazil', 'Mexico', 'Colombia', 'Chile', 'Peru') THEN  DATEADD(day, 1, COALESCE(DATE_TRUNC('day', contact.mql_deal_at), DATE_TRUNC('day', contact.pql_deal_at))::DATE) ELSE COALESCE(DATE_TRUNC('day', contact.mql_deal_at), DATE_TRUNC('day', contact.pql_deal_at)) END AS lcs_date, --seems like the deal create date in the tables is adjusted for timezone which causes issues with attribution. adjusting it back approx
        COALESCE(DATE_TRUNC('month', contact.mql_deal_at), DATE_TRUNC('month', contact.pql_deal_at)) AS lcs_month,
        contact.country,
        CASE
            WHEN contact.segment = 'Individual' THEN 'Individuals Core'
            WHEN contact.segment = 'Clinics' THEN 'Clinics PRS'
            WHEN contact.segment = 'PMS' THEN 'Clinics PMS'
        END AS segment_funnel,
        --contact.segment AS contact_type,
        'Undefined'::VARCHAR AS facility_size,
        'Undefined'::VARCHAR AS specialization,
        'Noa Notes' AS product
    FROM mart_cust_journey.noa_marketing_kpis_cm_lg_combined contact
    WHERE contact.lg_cm_flag_combined = 'LG'
      AND (contact.mql_excluded IS FALSE OR contact.mql_excluded IS NULL)
      AND COALESCE(DATE_TRUNC('day', contact.mql_deal_at), DATE_TRUNC('day', contact.pql_deal_at)) > '2025-01-01'
    AND contact.hubspot_id = 135749524399
    ),

all_valid_mals_mqls_list AS (
    SELECT
        hubspot_id --list with one row per contact
    FROM all_valid_mals_mqls
    GROUP BY hubspot_id
),

campaign_interaction_scope AS ( -- we select all Paid Media campaign interactions with a valid campaign name structure
    SELECT
        utm_log.hubspot_id,
        full_scope.lifecycle_stage,
        full_scope.verified,
        full_scope.product,
        full_scope.lead_id,
        utm_log.country,
        utm_log.utm_campaign AS campaign,
        LAG(utm_log.utm_campaign) OVER (PARTITION BY utm_log.hubspot_id ORDER BY utm_log.updated_at) AS prev_campaign_name,--we use this to select the first row after a campaign name has been updated
        DATE_TRUNC('day', utm_log.updated_at) AS interaction_day,
        utm_log.updated_at AS interaction,
        ROW_NUMBER() OVER (PARTITION BY utm_log.hubspot_id, utm_log.utm_campaign ORDER BY utm_log.updated_at DESC) AS rn,
        COALESCE(utm_log.utm_source, 'No info') AS source,
        COALESCE(utm_log.utm_medium, 'No info') AS medium,
        COALESCE(utm_log.utm_term, LEAD(utm_log.utm_term) OVER (PARTITION BY utm_log.hubspot_id ORDER BY utm_log.updated_at), 'No info') AS keyword,
        COALESCE(utm_log.utm_content, 'None') AS content,
        CASE
            WHEN utm_log.utm_source = 'facebook' OR utm_log.utm_source = 'Social_Ads' OR utm_log.utm_source = 'fb'
                OR utm_log.utm_source = 'ig'
                THEN 'facebook'
            WHEN utm_log.utm_source = 'spotify' THEN 'spotify'
            WHEN utm_log.utm_source = 'linkedin' THEN 'linkedin'
            WHEN utm_log.utm_source = 'softdoit' THEN 'softdoit'
            WHEN utm_log.utm_source = 'criteo' THEN 'criteo'
            WHEN utm_log.utm_source = 'bing' THEN 'bing'
            WHEN utm_log.utm_source = 'taboola' THEN 'taboola'
            WHEN utm_log.utm_source = 'tiktok' THEN 'tiktok'
            WHEN utm_log.utm_source = 'capterra' THEN 'capterra'
            WHEN utm_log.utm_source = 'cronomia' THEN 'cronomia'
            WHEN utm_log.utm_source = 'techdogs' THEN 'techdogs'
            WHEN utm_log.utm_source = 'google' OR utm_log.utm_source = 'adwords' THEN 'google'
        END AS hsa_net
    FROM all_valid_mals_mqls full_scope
    INNER JOIN cj_data_layer.cj_sat_contact_hs_1h_log_merged utm_log
        ON utm_log.hubspot_id = full_scope.hubspot_id
    WHERE utm_log.utm_campaign IS NOT NULL AND NOT (utm_log.country = 'Brazil' AND (LOWER(utm_log.utm_campaign) LIKE '%\\_wa\\_%' OR LOWER(utm_log.utm_campaign) LIKE '%\\_wa\\-%'))
        AND utm_log.utm_source IN ('facebook', 'fb', 'ig', 'Social_Ads', 'spotify', 'linkedin', 'criteo', 'google', 'adwords', 'bing', 'taboola', 'tiktok', 'softdoit', 'cronomia', 'capterra', 'techdogs')
        AND (utm_log.utm_campaign LIKE '%\\_%\\_%\\_%' OR utm_log.utm_campaign IN ('it_gipo_mal', 'it_fac_mal'))
        AND utm_log.country IN ('Spain', 'Brazil', 'Mexico', 'Turkey', 'Turkiye', 'Poland', 'Italy', 'Germany', 'Colombia', 'Chile', 'Peru')
    --AND full_scope.hubspot_id = 157487950500
    GROUP BY
        utm_log.hubspot_id,
        full_scope.hubspot_id,
        full_scope.product,
        full_scope.lead_id,
        utm_log.country,
        full_scope.lifecycle_stage,
        utm_log.utm_campaign,
        full_scope.lifecycle_stage,
        utm_log.utm_source,
        utm_log.utm_medium,
        utm_log.utm_term,
        utm_log.utm_content,
        utm_log.updated_at,
        full_scope.verified
),

pre_utm_log_lcs_join AS ( -- join MAL/MQL data to Paid Media interactions
    SELECT
        lcs.hubspot_id,
        lcs.verified,
        lcs.country,
        lcs.lead_id,
        lcs.product,
        utm.campaign,
        utm.source,
        utm.medium,
        utm.keyword,
        utm.content,
        utm.hsa_net,
        lcs.lifecycle_stage,
        lcs.lcs_date,
        lcs.lcs_month,
        utm.prev_campaign_name,
        utm.interaction,
        utm.interaction_day,
        lcs.specialization,
        lcs.segment_funnel,
        COALESCE(LAST_VALUE(NULLIF(true, utm.prev_campaign_name = utm.campaign) IGNORE NULLS) OVER (
                               PARTITION BY lcs.hubspot_id, interaction_day
                               ORDER BY utm.interaction, utm.prev_campaign_name
                               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                               ), false) AS campaign_changed,--Marking if a campaign name has been changed for each hubspot_id on a given day
        CASE
         WHEN hsa_net = 'facebook' AND (utm.keyword LIKE '%Bad.name%' OR utm.keyword LIKE '%stff%') THEN 0
         WHEN LEFT(utm.campaign, 1) ~ '^[A-Z]' THEN 0
         ELSE 1
        END AS structure_flag, --check with Agata what this is
    utm.interaction_day - INTERVAL '1 hour' AS from_date,
    utm.interaction::DATE + INTERVAL '31 day' AS to_date
    FROM campaign_interaction_scope utm
    INNER JOIN all_valid_mals_mqls lcs ON utm.hubspot_id = lcs.hubspot_id
    WHERE lcs_date BETWEEN utm.interaction_day - INTERVAL '1 hour' AND utm.interaction::DATE + INTERVAL '31 day' --fix to make sure any delays in the campaign log dont affect the numbers
    --AND utm.hubspot_id = 157487950500
    ),

utm_log_lcs_join AS (
    SELECT
      *
    FROM pre_utm_log_lcs_join
    WHERE campaign_changed
    AND structure_flag = 1
    QUALIFY ROW_NUMBER() OVER (PARTITION BY hubspot_id, interaction_day, campaign, campaign_changed, lifecycle_stage
    ORDER BY structure_flag DESC, interaction DESC) = 1 -- Taking the last row within the day that campaign name has been changed
),

lcs_utm_order_control AS ( --Ordering needed to ensure we only assign an MAL/MQL to a campaign if was the last touch. Only 1 campaign per MAL and 1 per MQL
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY hubspot_id, country, lifecycle_stage, lcs_date
            ORDER BY lcs_date DESC, interaction DESC) AS lcs_order,
        ROW_NUMBER() OVER (PARTITION BY hubspot_id, country, lifecycle_stage, interaction
            ORDER BY lcs_date ASC) AS interaction_order
    FROM utm_log_lcs_join
    WHERE TRUE
        QUALIFY lcs_order = 1 and interaction_order = 1
),

cj_deals_full_scope AS (
    SELECT
        deal.country,
        CASE WHEN deal.pipeline_type ='Individual New Sales' AND deal.country = 'Mexico' AND NVL(deal.contract_signed_date, deal.active_won_date) >= '2025-07-13' THEN --on 06.08 logic changed to consider contract signed as date of WON source of truth for Agenda Premium in SF (atm only MX)
            DATE_TRUNC('month', NVL(deal.contract_signed_date, deal.active_won_date, deal.closedate))::DATE ELSE
            DATE_TRUNC('month', NVL(deal.active_won_date, deal.closedate))::DATE END AS deal_month,
        CASE WHEN deal.pipeline_type ='Individual New Sales' AND deal.country = 'Mexico' AND NVL(deal.contract_signed_date, deal.active_won_date, deal.closedate) >= '2025-07-13' THEN NVL(deal.contract_signed_date, deal.active_won_date, deal.closedate)
        ELSE NVL(deal.active_won_date, deal.closedate) END AS deal_stage_start,
        deal.hs_contact_id,
        deal.hs_lead_id AS lead_id,
        deal.opportunity_id AS deal_id,
        deal.pipeline_type,
        NVL(deal.deal_length, 12) AS deal_length,
        MAX(deal.mrr_euro) OVER(PARTITION BY  deal.opportunity_id) AS mrr_euro,
        MAX(deal.mrr_original_currency) OVER(PARTITION BY deal.opportunity_id) AS mrr_original_currency,
        deal.current_stage,
        sf.last_source__c as sf_source,
        sf.businessline__c as sf_product,
        live.hubspot_owner_id,
        deal.tag_so as tag_deal,
        CASE WHEN deal.country = 'Brazil' AND deal.pipeline = 'PMS' THEN 'Feegow'
            WHEN deal.country = 'Italy' AND deal.pipeline = 'PMS' THEN 'Gipo'
            WHEN deal.country = 'Spain' AND deal.pipeline = 'PMS' THEN 'Clinic Cloud'
            WHEN deal.country = 'Poland' AND deal.pipeline = 'PMS' THEN 'MyDr'
            WHEN deal.pipeline_type = 'Individual New Sales' THEN 'Agenda Premium'
            WHEN  deal.pipeline_type = 'Clinics New Sales' THEN 'Clinic Agenda'
            WHEN deal.pipeline IN ('Noa', 'Noa Notes') AND (deal.is_noa_trial != 'Yes' OR deal.is_noa_trial IS NULL) THEN 'Noa'
            ELSE NULL END AS product_won,
        CASE WHEN deal.pipeline IN ('Noa', 'Noa Notes') AND (deal.is_noa_trial != 'Yes' OR deal.is_noa_trial IS NULL) AND (LOWER(deal.tag_so) LIKE '%inb%' OR (sf.last_source__c LIKE '%Noa%' AND NOT sf.last_source__c LIKE '%Outbound%')) THEN 'Inbound'
            WHEN deal.country = 'Spain' AND deal.pipeline = 'PMS' THEN 'Inbound'
            WHEN deal.country = 'Brazil' AND deal.pipeline = 'PMS' AND (LOWER(deal.tag_so) LIKE '%inb%') THEN 'Inbound'
            WHEN deal.pipeline_type IN ('Individual New Sales', 'Clinics New Sales') AND doc.deal_allocation IS NOT NULL THEN doc.deal_allocation
            WHEN deal.pipeline_type IN ('Individual New Sales', 'Clinics New Sales') AND fac.deal_id IS NOT NULL THEN 'Inbound'
            WHEN deal.opportunity_id IS NOT NULL THEN 'Outbound' ELSE NULL END AS deal_allocation_aux
    FROM mart_cust_journey.cj_opportunity_facts deal
    INNER JOIN all_valid_mals_mqls_list scope -- we select just the ones that arrive to won stage after entering the correct way to the funnel, in this case MAL and MQL
               ON deal.hs_contact_id = scope.hubspot_id
    LEFT JOIN mart_cust_journey.cj_mqls_monthly doc ON doc.deal_id = deal.opportunity_id
    LEFT JOIN mart_cust_journey.cj_mqls_monthly_clinics fac on fac.deal_id = deal.opportunity_id
    LEFT JOIN dp_salesforce.opportunity sf ON sf.id = deal.opportunity_id
    LEFT JOIN mart.ecommerce e ON deal.hs_contact_id = e.hubspot_id AND DATE_TRUNC('month', NVL(deal.active_won_date,deal.closedate))::DATE = DATE_TRUNC('month',deal_created_at)
    LEFT JOIN dw.hs_deal_live live ON deal.opportunity_id = live.hubspot_id
    WHERE ((deal.country = 'Mexico' AND NVL(deal.contract_signed_date, deal.active_won_date, deal.closedate)::DATE >= '2025-07-13' AND deal.pipeline = 'Agenda Premium' AND deal.pipeline_type = 'Individual New Sales' AND (deal.was_contract_signed = 1 OR deal.was_active_won = 1 OR deal.current_stage = 'Closed Won') AND e.hubspot_id IS NULL)
        OR (((deal.country = 'Mexico' AND NVL(deal.active_won_date, deal.closedate)::DATE < '2025-07-13') OR deal.country != 'Mexico') AND
        deal.current_stage IN ('Closed Won') AND deal.pipeline_type = 'Individual New Sales' AND crm_source IN ('hubspot', 'hays')
      AND DATE_TRUNC('month', NVL(deal.active_won_date, deal.closedate))::DATE >= '2024-01-01') --selecting HS source/logic for all countries except Mexico from 13.07(post-migration)
    OR (deal.country = 'Mexico' AND NVL(deal.active_won_date, deal.closedate)::DATE >= '2025-07-13' AND deal.pipeline_type = 'Individual New Sales' AND e.hubspot_id IS NOT NULL AND deal.current_stage IN ('Closed Won') AND deal.crm_source = 'hays')  --including ecommerce for MX post migration
    ) --ind
    OR
        (deal.pipeline_type IN ('Clinics New Sales') AND (deal.pipeline != 'Noa' OR deal.pipeline IS NULL) AND UPPER(deal.opportunity_name) NOT LIKE '%FAKE%' AND deal.owner_name NOT LIKE '%Agnieszka Ligwinska%' --special exclusion for PL Clinic Agenda
        AND ((deal.current_stage = 'Closed Won' OR (deal.crm_source = 'salesforce' AND deal.was_active_won = 1 AND deal.current_stage IN ('Closed Won', 'Active/Won', 'Closed Lost', 'Back to Sales')))
      OR (deal.country = 'Poland' AND deal.pipeline = 'PMS' AND deal.current_stage = 'Contract Signed')) --as per Iga contract signed counted as WON for Mydr
    AND (deal.country IN ('Colombia', 'Mexico', 'Brazil', 'Italy') OR (deal.country = 'Spain' AND deal.opportunity_name NOT LIKE '%Saas360%'
            AND deal.opportunity_name NOT LIKE '%PMS deal%' AND deal.opportunity_name NOT LIKE '%PMS SALES DEAL%')
                OR (deal.country = 'Poland' AND deal.pipeline != 'PMS' AND live.hubspot_team_id = 3210 AND NOT (LOWER(live."tag") LIKE '%sell%'
                    OR LOWER(live."tag") LIKE '%upgrade%'
                    OR LOWER(live."tag") LIKE '%migration%'
                    OR LOWER(live."tag") LIKE '%error%'
                    OR LOWER(live."tag") LIKE '%references%'
                    OR LOWER(live."tag") LIKE '%other%'
                    OR LOWER(live."tag") IS NULL))
                    OR (deal.country = 'Poland' AND deal.pipeline = 'PMS')
                    )) --clinics/PMS
    OR (deal.pipeline IN ('Noa', 'Noa Notes')
            AND (deal.current_stage = 'Closed Won' OR deal.was_active_won = 1) AND (live.noa_notes_budget_category__wf NOT IN ('DOC - Price Upgrade', 'FAC - Price Upgrade', 'FAC - Profiles num upgrade', 'DOC - Doc Agenda churn Promo') OR live.noa_notes_budget_category__wf IS NULL)
         AND ((deal.crm_source = 'hubspot' AND (live.noa_notes_trial_yes_no != 'Yes' OR live.noa_notes_trial_yes_no IS NULL)) OR (deal.crm_source = 'salesforce' AND (sf.negotiation_type__c != 'Trial' OR sf.negotiation_type__c IS NULL))))
    GROUP BY deal.contract_signed_date, deal.active_won_date, deal.closedate, deal.opportunity_id, deal.current_stage, deal.pipeline_type, deal.is_noa_trial, deal.hs_contact_id, deal.hs_lead_id,  deal_length, deal.country, deal.mrr_euro, deal.mrr_original_currency, deal.pipeline, deal.pipeline_type, deal_allocation_aux, deal.current_stage, sf.last_source__c, sf.businessline__c,  live.hubspot_owner_id, deal.tag_so,
    scope.hubspot_id
),

cj_deals_deduped AS (
    SELECT
        country,
        deal_month,
        hs_contact_id,
        lead_id,
        deal_length,
        deal_stage_start,
        mrr_euro,
        product_won,
        NVL(ROUND(mrr_euro * deal_length, 2), 0) AS total_revenue_eur,
        NVL(ROUND(mrr_original_currency * deal_length, 2), 0) AS total_revenue_origina_curr
    FROM cj_deals_full_scope
    GROUP BY country,
        deal_month,
        hs_contact_id,
        lead_id,
        deal_length,
        deal_stage_start,
        mrr_euro,
        product_won,
        total_revenue_eur,
        total_revenue_origina_curr
),

next AS (
    SELECT
        DATE_TRUNC('day', main.lcs_date) AS date,
        main.hubspot_id,
        main.lead_id,
        main.country,
        main.verified,
        main.specialization,
        main.product,
        main.campaign,
        main.hsa_net,
        main.content,
        main.keyword,
        deal.deal_month,
        deal.product_won,
        main.segment_funnel,
        COUNT(CASE WHEN main.lifecycle_stage = 'MAL' THEN main.hubspot_id END) AS mal,
        COUNT(CASE WHEN main.lifecycle_stage = 'MQL' THEN main.hubspot_id END) AS mql
    FROM lcs_utm_order_control main
    LEFT JOIN cj_deals_deduped deal
        ON deal.hs_contact_id = main.hubspot_id
        AND deal.deal_stage_start >= DATE_TRUNC('day',main.lcs_date) --noqa
    GROUP BY date,
        main.hubspot_id,
        main.country,
        main.verified,
        main.specialization,
        main.campaign,
        main.hsa_net,
        main.content,
        main.keyword,
        deal.deal_month,
        deal.product_won,
        main.segment_funnel,
        main.lifecycle_stage,
        main.product,
        main.lead_id
)

SELECT
    date::DATE,
    country,
    hubspot_id,
    lead_id,
    verified,
    campaign,
    hsa_net,
    content,
    keyword,
    deal_month::DATE,
    product_won,
    product,
    CASE WHEN UPPER(LEFT(campaign, 2)) IS NULL OR NOT UPPER(LEFT(campaign, 2)) IN ('PL', 'ES', 'TR', 'MX', 'CO', 'CL', 'BR', 'IT', 'DE', 'PE')
        THEN
        CASE WHEN country = 'Poland' THEN 'PL'
            WHEN country = 'Spain' THEN 'ES'
            WHEN country = 'Turkiye' THEN 'TR'
            WHEN country = 'Mexico' THEN 'MX'
            WHEN country = 'Colombia' THEN 'CO'
            WHEN country = 'Chile' THEN 'CL'
            WHEN country = 'Brazil' THEN 'BR'
            WHEN country = 'Italy' THEN 'IT'
            WHEN country = 'Germany' THEN 'DE'
            WHEN country = 'Peru' THEN 'PE'
        END
        ELSE UPPER(LEFT(campaign, 2))
    END AS country_by_campaign,
    CASE WHEN SPLIT_PART(campaign, '_', 3) = 'mal-mql'
        OR SPLIT_PART(campaign, '_', 3) = 'mql-mal' THEN
        CASE WHEN REGEXP_SUBSTR(content, '^[a-zA-Z]{2}_.*') LIKE '%_mal_%' THEN 'mal'
            WHEN REGEXP_SUBSTR(content, '^[a-zA-Z]{2}_.*') LIKE '%_mql_%' THEN 'mql'
            ELSE SPLIT_PART(campaign, '_', 3)
        END
        WHEN SPLIT_PART(campaign, '_', 3) = 'pure' THEN 'mql'
        ELSE SPLIT_PART(campaign, '_', 3)
    END AS campaign_goal,
    CASE WHEN SPLIT_PART(campaign, '_', 2) = 'doc-fac'
        OR SPLIT_PART(campaign, '_', 2) = 'fac-doc' THEN
        CASE WHEN REGEXP_SUBSTR(content, '^[a-zA-Z]{2}_.*') LIKE '%_doc_%' THEN 'doc'
            WHEN REGEXP_SUBSTR(content, '^[a-zA-Z]{2}_.*') LIKE '%_fac_%' THEN 'fac'
            WHEN REGEXP_SUBSTR(content, '^[a-zA-Z]{2}_.*') LIKE '%_ent_%' THEN 'ent'
            ELSE SPLIT_PART(campaign, '_', 2)
        END
        ELSE SPLIT_PART(campaign, '_', 2)
    END AS campaign_target,
    CASE WHEN (LOWER(campaign) LIKE '%feegow%' OR LOWER(campaign) LIKE '%-fg%') AND country = 'Brazil' THEN 'Feegow'
        WHEN (LOWER(campaign) LIKE '%clinic-cloud%' OR LOWER(campaign) LIKE '%-cc%') AND country = 'Spain' THEN 'Clinic Cloud'
        WHEN LOWER(campaign) LIKE '%gipo%' AND country = 'Italy' THEN 'GIPO'
        WHEN LOWER(campaign) LIKE '%mydr%' AND country = 'Poland' THEN 'MyDr'
        WHEN LOWER(campaign) LIKE '%_noa%' OR LOWER(campaign) LIKE '%-noa%' THEN 'Noa'
        WHEN LOWER(campaign) LIKE '%tuotempo%' THEN 'TuoTempo'
        ELSE 'Agenda'
    END AS campaign_product,
    CASE WHEN LOWER(keyword) LIKE '%website%' OR LOWER(content) LIKE '%website%' THEN 'Website'
        WHEN LOWER(keyword) LIKE '%instant-form%' OR LOWER(content) LIKE '%instant-form%' THEN 'Instant Form'
        WHEN LOWER(keyword) LIKE '%messenger%' OR LOWER(content) LIKE '%messenger%' THEN 'Messenger'
        WHEN LOWER(keyword) LIKE '%whatsapp%' OR LOWER(content) LIKE '%whatsapp%' THEN 'WhatsApp'
        WHEN LOWER(keyword) LIKE '%instagram%' OR LOWER(content) LIKE '%instagram%' THEN 'Instagram'
        WHEN LOWER(keyword) LIKE '%calls%' OR LOWER(content) LIKE '%calls%' THEN 'Calls'
        WHEN LOWER(keyword) LIKE '%app%' OR LOWER(content) LIKE '%app%' THEN 'App'
        WHEN LOWER(keyword) LIKE '%engagement%' OR LOWER(content) LIKE '%engagement%' THEN 'Engagement'
        ELSE 'Other'
    END AS conversion_location,
    CASE WHEN LOWER(keyword) LIKE '%lal%' OR LOWER(content) LIKE '%lal%' THEN 'LAL'
        WHEN LOWER(keyword) LIKE '%website-visitors%' OR LOWER(content) LIKE '%website-visitors%' THEN 'Website Visitors'
        WHEN LOWER(keyword) LIKE '%hubspot-list%' OR LOWER(content) LIKE '%hubspot-list%' THEN 'Hubspot List'
        WHEN LOWER(keyword) LIKE '%broad%' OR LOWER(content) LIKE '%broad%' THEN 'Broad'
        WHEN LOWER(keyword) LIKE '%interest%' OR LOWER(content) LIKE '%interest%' THEN 'Interest'
        WHEN LOWER(keyword) LIKE '%other-audience%' OR LOWER(content) LIKE '%other-audience%' THEN 'Other Audience'
        ELSE 'Other'
    END AS targeting_method,
    CASE WHEN LOWER(keyword) LIKE '%ebook%' OR LOWER(content) LIKE '%ebook%' THEN 'Ebook'
        WHEN LOWER(keyword) LIKE '%webinar%' OR LOWER(content) LIKE '%webinar%' THEN 'Webinar'
        WHEN LOWER(keyword) LIKE '%offer-request%' OR LOWER(content) LIKE '%offer-request%' THEN 'Offer Request'
        WHEN LOWER(keyword) LIKE '%partnership%' OR LOWER(content) LIKE '%partnership%' THEN 'Partnership'
        WHEN LOWER(keyword) LIKE '%other-magnet%' OR LOWER(content) LIKE '%other-magnet%' THEN 'Other Magnet'
        WHEN LOWER(keyword) LIKE '%free-trial%' OR LOWER(content) LIKE '%free-trial%'
         OR LOWER(keyword) LIKE '%free-trail%' OR LOWER(content) LIKE '%free-trail%' THEN 'Free Trial'
        WHEN LOWER(keyword) LIKE '%event%' OR LOWER(content) LIKE '%event%' THEN 'Event'
        WHEN LOWER(keyword) LIKE '%free-profile%' OR LOWER(content) LIKE '%free-profile%' THEN 'Free Profile'
        WHEN LOWER(keyword) LIKE '%infographic%' OR LOWER(content) LIKE '%infographic%' THEN 'Infographic'
        WHEN LOWER(keyword) LIKE '%free-tools%' OR LOWER(content) LIKE '%free-tools%' THEN 'Free Tools'
        ELSE 'Other'
    END AS lead_magnet,
    CASE WHEN LOWER(keyword) LIKE '%video%' OR LOWER(content) LIKE '%video%' THEN 'Video'
        WHEN LOWER(keyword) LIKE '%static%' OR LOWER(content) LIKE '%static%' THEN 'Static'
        WHEN LOWER(keyword) LIKE '%animated%' OR LOWER(content) LIKE '%animated%' THEN 'Animated'
        WHEN LOWER(keyword) LIKE '%carousel%' OR LOWER(content) LIKE '%carousel%' THEN 'Carousel'
        WHEN LOWER(keyword) LIKE '%other-creative%' OR LOWER(content) LIKE '%other-creative%' THEN 'Other Creative'
        ELSE 'Other'
    END AS creative_type,
    specialization AS br_mx_cl_specialisation,
    segment_funnel,
    SUM(mal) AS mals,
    SUM(mql) AS mqls
FROM next
GROUP BY
    date,
    country,
    hubspot_id,
    verified,
    campaign,
    segment_funnel,
    hsa_net,
    product,
    content,
    keyword,
    deal_month,
    product_won,
    country_by_campaign,
    campaign_goal,
    campaign_target,
    campaign_product,
    conversion_location,
    targeting_method,
    lead_magnet,
    creative_type,
    br_mx_cl_specialisation,
    lead_id
