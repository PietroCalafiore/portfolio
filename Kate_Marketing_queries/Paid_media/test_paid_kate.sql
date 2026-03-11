DROP TABLE IF EXISTS test.msv_paid_media_campaign_hubspot_2;

CREATE TABLE test.msv_paid_media_campaign_hubspot_2 AS
WITH all_valid_mals_mqls AS ( --we select all contacts with an MAL or MQL stage who arent patients
    SELECT
        contact.contact_id AS hubspot_id,
        contact.lead_id,
        contact_facts.verified,
        contact.lifecycle_stage,
        contact.updated_at AS lcs_date,
        contact.is_mkt_push_lead,
        contact_facts.country,
        contact_facts.segment AS contact_type_live,
        contact_facts.product_recommended,
        contact_facts.spec_split,
        contact_facts.facility_size,
        COALESCE(hclh.contact_type_segment_test, contact_facts.segment) AS contact_type, --we freeze the contact type 30 days after the lead happens to avoid numbers fluctuating due to segment updates
        CASE WHEN contact.updated_at BETWEEN hclh.createdate AND DATEADD(day, 3, hclh.createdate) THEN TRUE ELSE FALSE END AS new_mal_flag,
        ROW_NUMBER() OVER (PARTITION BY contact.contact_id, contact.lifecycle_stage, contact.updated_at ORDER BY hclh.start_date DESC) AS row
    FROM cj_data_layer.cj_contact_lead_main contact
    INNER JOIN mart_cust_journey.cj_contact_facts contact_facts
        ON contact_facts.contact_id = contact.contact_id
            AND contact_facts.lead_id = contact.lead_id
            AND contact_facts.country IN ('Spain', 'Brazil', 'Mexico', 'Turkey', 'Turkiye', 'Poland', 'Italy', 'Germany', 'Colombia', 'Chile', 'Peru')
    LEFT JOIN dw.hs_contact_live_history hclh
        ON hclh.hubspot_id = contact.contact_id
            AND hclh.start_date BETWEEN contact.updated_at AND DATEADD(day, 31, contact.updated_at)
    WHERE contact.lifecycle_stage IN ('MAL', 'MQL') AND (contact_facts.segment != 'PATIENT' OR contact_facts.segment IS NULL)
        AND (contact.is_mkt_push_lead IS NULL
            OR (contact.is_mkt_push_lead IN (0, 1))) --AND contact.contact_id = 1821635888
        QUALIFY row=1 ),

all_valid_mals_mqls_list AS (
    SELECT
        hubspot_id,
        lead_id-- we need a list with just one row per contact
    FROM all_valid_mals_mqls
    GROUP BY hubspot_id,
        lead_id
),

campaign_interaction_scope AS ( -- we select all Paid Media campaign interactions with a valid campaign name structure
    SELECT
        utm_log.hubspot_id,
        full_scope.lead_id,
        full_scope.is_mkt_push_lead,
        full_scope.contact_type_live,
        full_scope.contact_type,
        full_scope.verified,
        full_scope.product_recommended,
        full_scope.spec_split,
        full_scope.facility_size,
        utm_log.country,
        utm_log.utm_campaign AS campaign,
        LAG(utm_log.utm_campaign) OVER (PARTITION BY utm_log.hubspot_id ORDER BY utm_log.updated_at) AS prev_campaign_name,--we use this to select the first row after a campaign name has been updated
        MIN(utm_log.updated_at) OVER (PARTITION BY utm_log.hubspot_id,utm_log.utm_campaign) AS interaction,
        --utm_log.updated_at AS interaction,
        ROW_NUMBER() OVER (PARTITION BY utm_log.hubspot_id, utm_log.utm_campaign ORDER BY utm_log.updated_at DESC) AS rn,
        COALESCE(utm_log.utm_source, 'No info') AS source,
        COALESCE(utm_log.utm_medium, 'No info') AS medium,
        COALESCE(utm_log.utm_term, LEAD(utm_log.utm_term) OVER (PARTITION BY utm_log.hubspot_id ORDER BY utm_log.updated_at), 'No info') AS keyword,
        COALESCE(utm_log.utm_content, 'None') AS content,
        MAX(CASE WHEN utm_log.utm_content IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY utm_log.hubspot_id, utm_campaign, DATE_TRUNC('day', utm_log.updated_at)) AS adset_available,
        MAX(CASE WHEN utm_log.utm_term IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY utm_log.hubspot_id, utm_campaign, DATE_TRUNC('day', utm_log.updated_at)) AS ad_keyword_available,
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
        END AS hsa_net,
    full_scope.new_mal_flag
    FROM all_valid_mals_mqls full_scope
    INNER JOIN cj_data_layer.cj_sat_contact_hs_1h_log_merged utm_log
        ON utm_log.hubspot_id = full_scope.hubspot_id
    WHERE utm_log.utm_campaign IS NOT NULL AND NOT (utm_log.country = 'Brazil' AND (LOWER(utm_log.utm_campaign) LIKE '%\\_wa\\_%' OR LOWER(utm_log.utm_campaign) LIKE '%\\_wa\\-%'))
        AND utm_log.utm_source IN ('facebook', 'fb', 'ig', 'Social_Ads', 'spotify', 'linkedin', 'criteo', 'google', 'adwords', 'bing', 'taboola', 'tiktok', 'softdoit', 'cronomia', 'capterra', 'techdogs')
        AND (utm_log.utm_campaign LIKE '%\\_%\\_%\\_%' OR utm_log.utm_campaign IN ('it_gipo_mal', 'it_fac_mal'))
        AND utm_log.country IN ('Spain', 'Brazil', 'Mexico', 'Turkey', 'Turkiye', 'Poland', 'Italy', 'Germany', 'Colombia', 'Chile', 'Peru')
    --AND utm_log.hubspot_id = 1821635888
      GROUP BY
        utm_log.hubspot_id,
        full_scope.hubspot_id,
        full_scope.lead_id,
        utm_log.country,
        full_scope.contact_type_live,
        full_scope.contact_type,
        utm_log.utm_campaign,
        full_scope.lifecycle_stage,
        full_scope.is_mkt_push_lead,
        utm_log.utm_source,
        utm_log.utm_medium,
        utm_log.utm_term,
        utm_log.utm_content,
        utm_log.updated_at,
        full_scope.verified,
        full_scope.product_recommended,
        full_scope.spec_split,
        full_scope.facility_size,
        full_scope.new_mal_flag
    QUALIFY 1 = 1 AND (utm_content IS NOT NULL OR adset_available = 0) AND (utm_term IS NOT NULL OR ad_keyword_available = 0)
      AND rn = 1
    -- Only allow null utm_term and utm_content if it was never available that day
),

utm_log_lcs_join AS ( -- join MAL/MQL data to Paid Media interactions
    SELECT
        lcs.hubspot_id,
        lcs.lead_id,
        lcs.is_mkt_push_lead,
        lcs.contact_type_live,
        lcs.contact_type,
        lcs.verified,
        lcs.product_recommended,
        lcs.spec_split,
        lcs.facility_size,
        lcs.country,
        utm.campaign,
        utm.source,
        utm.medium,
        utm.keyword,
        utm.content,
        utm.hsa_net,
        lcs.new_mal_flag,
        lcs.lifecycle_stage,
        CASE WHEN DATEPART(hour, lcs.lcs_date) = 0 AND DATEPART(minute, lcs.lcs_date) = 0 AND DATEPART(second, lcs.lcs_date) = 0  -- noqa
                THEN DATEADD(second, -1, DATEADD(day, 0, lcs.lcs_date)) ELSE lcs.lcs_date END AS new_lcs_date,  -- noqa
        lcs.lcs_date,
        utm.prev_campaign_name,
        utm.interaction,
        CASE WHEN new_lcs_date BETWEEN utm.interaction - INTERVAL '1 hour' AND utm.interaction::DATE + INTERVAL '31 day' THEN TRUE ELSE FALSE END AS test
    FROM campaign_interaction_scope utm
    INNER JOIN all_valid_mals_mqls lcs ON utm.hubspot_id = lcs.hubspot_id
    WHERE new_lcs_date BETWEEN utm.interaction - INTERVAL '1 hour' AND utm.interaction::DATE + INTERVAL '31 day' --fix to make sure any delays in the campaign log dont affect the numbers
       -- AND ((utm.prev_campaign_name != utm.campaign) OR utm.prev_campaign_name IS NULL)
),

lcs_utm_order_control AS ( --Ordering needed to ensure we only assign an MAL/MQL to a campaign if was the last touch. Only 1 campaign per MAL and 1 per MQL
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY hubspot_id, country, lifecycle_stage, new_lcs_date
            ORDER BY new_lcs_date DESC, interaction DESC) AS lcs_order,
        ROW_NUMBER() OVER (PARTITION BY hubspot_id, country, lifecycle_stage, interaction
            ORDER BY new_lcs_date ASC) AS interaction_order
    FROM utm_log_lcs_join
    WHERE TRUE
        QUALIFY lcs_order = 1 and interaction_order = 1
),

hubspot_web_props_aux_1_filtered_end AS (
    SELECT
        *,
        LAG(lead_id, 1) OVER (PARTITION BY hubspot_id ORDER BY new_lcs_date) AS prev_lead_id,--we use this to select the first row after a campaign name has been updated
        LAG(campaign, 1) OVER (PARTITION BY hubspot_id ORDER BY new_lcs_date) AS previous_campaign_name,--we use this to select the first row after a campaign name has been updated
        LAG(lifecycle_stage, 1) OVER (PARTITION BY hubspot_id ORDER BY new_lcs_date) AS previous_lcs
    FROM lcs_utm_order_control
    WHERE TRUE
        QUALIFY NOT (previous_campaign_name = campaign and lead_id <> prev_lead_id) or prev_lead_id is nULL
),

cj_deals_full_scope AS (--we select all Open Deals and Closed WONs
    SELECT
        deal.country,
        deal.month AS deal_month,
        deal.deal_stage_start,
        deal.hs_contact_id,
        deal.hs_lead_id AS lead_id,
        deal.deal_id,
        deal.pipeline_type,
        NVL(deal.deal_length, 12) AS deal_length,
        MAX(deal.mrr_euro) OVER(PARTITION BY deal.deal_id) AS mrr_euro,
        MAX(deal.mrr_original_currency) OVER(PARTITION BY deal.deal_id) AS mrr_original_currency,
        deal.deal_stage,
        sf.last_source__c as sf_source,
        sf.businessline__c as sf_product,
        deal.dealname,
        deal.hubspot_owner_id,
        deal.tag_so as tag_deal,
        CASE WHEN deal.deal_stage != 'Closed Won' AND deal.deal_stage != 'Active/Won' THEN ROW_NUMBER() OVER (PARTITION BY deal.deal_id, scope.hubspot_id ORDER BY scope.lead_id DESC) --adding to only select one row per open deal
            ELSE 1 END as row,
        CASE WHEN deal.deal_stage IN ('Closed Won', 'Active/Won') AND deal.country = 'Brazil' AND deal.pipeline = 'PMS' THEN 'Feegow'
            WHEN deal.deal_stage IN ('Closed Won', 'Active/Won') AND  deal.country = 'Italy' AND deal.pipeline = 'PMS' THEN 'Gipo'
            WHEN deal.deal_stage IN ('Closed Won', 'Active/Won') AND deal.country = 'Spain' AND deal.pipeline = 'PMS' THEN 'Clinic Cloud'
            WHEN deal.deal_stage IN ('Closed Won', 'Active/Won') AND deal.country = 'Poland' AND deal.pipeline = 'PMS' THEN 'MyDr'
            WHEN deal.deal_stage IN ('Closed Won') AND deal.pipeline_type = 'Individual New Sales' THEN 'Agenda Premium'
            WHEN deal.deal_stage IN ('Closed Won', 'Active/Won') AND deal.pipeline_type IN ('Clinics New Sales') THEN 'Clinic Agenda'
            WHEN deal.deal_stage IN ('Closed Won') AND deal.pipeline_type IN ('Noa') AND (deal.noa_notes_trial_yes_no != 'Yes' OR deal.noa_notes_trial_yes_no IS NULL) THEN 'Noa'
            ELSE NULL END AS product_won,
        CASE WHEN deal.pipeline_type IN ('Noa') AND (deal.noa_notes_trial_yes_no != 'Yes' OR deal.noa_notes_trial_yes_no IS NULL) AND (deal.tag_so LIKE '%Inbound%' OR deal.tag_so LIKE '%Mixed%') THEN 'Inbound'
            WHEN deal.deal_stage IN ('Closed Won', 'Active/Won') AND deal.pipeline_type IN ('Individual New Sales', 'Clinics New Sales') AND deal.country = 'Spain' AND deal.pipeline = 'PMS' THEN 'Inbound'
            WHEN deal.pipeline_type IN ('Individual New Sales', 'Clinics New Sales') AND doc.deal_allocation IS NOT NULL THEN doc.deal_allocation
            WHEN deal.pipeline_type IN ('Individual New Sales', 'Clinics New Sales') AND fac.deal_id IS NOT NULL THEN 'Inbound'
            WHEN deal.deal_id IS NOT NULL THEN 'Outbound' ELSE NULL END AS deal_allocation_aux
    FROM mart_cust_journey.cj_deal_month deal
    INNER JOIN all_valid_mals_mqls_list scope -- we select just the ones that arrive to won stage after entering the correct way to the funnel, in this case MAL and MQL
               ON deal.hs_contact_id = scope.hubspot_id
    LEFT JOIN mart_cust_journey.cj_mqls_monthly doc ON doc.contact_id = deal.hs_contact_id AND doc.deal_month = deal.month
    LEFT JOIN mart_cust_journey.cj_mqls_monthly_clinics fac on fac.deal_id = deal.deal_id
    LEFT JOIN dp_salesforce.opportunity sf ON sf.id = deal.deal_id
    WHERE (deal.pipeline_type IN ('Individual New Sales', 'Clinics New Sales', 'Noa')
        AND deal.stage_is_month_new  AND deal.deal_stage NOT IN ('Closed Lost', 'Closed Won', 'Active/Won')) OR (deal.pipeline_type IN ('Individual New Sales', 'Clinics New Sales')
        AND deal.stage_is_month_new  AND deal.deal_stage IN ('Closed Won', 'Active/Won') AND is_current_stage)
    OR (deal.pipeline_type IN ('Individual New Sales', 'Clinics New Sales')
        AND deal.stage_is_month_new  AND deal.deal_stage IN ('Active/Won') AND NOT is_current_stage) --catching deals that may have gone back to Sales/closed lost
    OR (deal.pipeline_type = 'Noa' AND deal.stage_is_month_new  AND deal.deal_stage IN ('Closed Won', 'Active/Won') AND is_current_stage AND (deal.noa_notes_trial_yes_no != 'Yes' OR deal.noa_notes_trial_yes_no IS NULL))
    GROUP BY deal.month, deal.pipeline_type, deal.noa_notes_trial_yes_no, deal.hs_contact_id, deal.hs_lead_id, scope.hubspot_id, scope.lead_id, deal.deal_id, deal_length, deal.country, deal.mrr_euro, deal.mrr_original_currency, deal.deal_stage_start, deal.pipeline, deal.pipeline_type, deal_allocation_aux, deal.deal_stage, sf.last_source__c, sf.businessline__c, deal.dealname, deal.hubspot_owner_id, deal.tag_so
    QUALIFY row = 1
),

cj_deals_deduped AS ( --deduplication of deal data to have 1 row per Open deal and 1 row per Closed Won
    SELECT
        deal.country,
        deal.deal_month,
        deal.hs_contact_id,
        deal.lead_id,
        deal.deal_id,
        deal.deal_length,
        deal.deal_stage_start,
        deal.deal_stage,
        deal.pipeline_type,
        CASE WHEN deal_stage IN ('Closed Won', 'Active/Won') THEN deal.mrr_euro ELSE 0 END AS mrr_euro,
        deal.product_won,
        deal.sf_source,
        deal.sf_product,
        deal.dealname,
        deal.hubspot_owner_id,
        deal.tag_deal,
        deal.deal_allocation_aux,
        CASE WHEN deal_stage IN ('Closed Won', 'Active/Won') THEN NVL(ROUND(deal.mrr_euro * deal.deal_length, 2), 0)  ELSE 0 END AS total_revenue_eur,
        CASE WHEN deal_stage IN ('Closed Won', 'Active/Won') THEN NVL(ROUND(deal.mrr_original_currency * deal.deal_length, 2), 0) ELSE 0 END AS  total_revenue_origina_curr
    FROM cj_deals_full_scope deal
    GROUP BY deal.country,
        deal.deal_month,
        deal.hs_contact_id,
        deal.lead_id,
        deal.deal_id,
        deal.deal_length,
        deal.deal_stage_start,
        deal.deal_stage,
        deal.pipeline_type,
        deal.mrr_euro,
        deal.product_won,
        deal.mrr_original_currency,
        deal.deal_stage,
        deal.deal_allocation_aux,
        deal.sf_source,
        deal.sf_product,
        deal.dealname,
        deal.hubspot_owner_id,
        deal.tag_deal
),

last_source_list AS (--selecting one last source per deal needed for Inbound/Outbound assignment
    SELECT
        country,
        contact_id,
        b.lead_id,
        lifecycle_stage_start,
        month,
        last_source_so,
        source_so, lifecycle_stage,
        DATE_TRUNC('month',lifecycle_stage_start) AS LCS_start_month,
        lead_ini,
        lead_end
            FROM ( SELECT
                    *,
                    RANK() OVER (PARTITION BY a.lead_id ORDER BY lifecycle_stage_start DESC) AS lifecycle_rank
            FROM ( SELECT
                    contact_facts.country,
                    contact_lifecycle.contact_id,
                    contact_lifecycle.lead_id,
                    DATE_TRUNC('month',contact_facts.lead_ini) AS lead_ini,
                    COALESCE(contact_facts.lead_end, CURRENT_DATE) AS lead_end,
                    contact_lifecycle.lifecycle_stage_start,
                    contact_lifecycle.month,
                    contact_lifecycle.last_source_so,
                    contact_lifecycle.source_so,
                    contact_lifecycle.lifecycle_stage,
                    RANK() OVER (PARTITION BY contact_lifecycle.lead_id ORDER BY contact_lifecycle.month DESC) AS month_rank
                    FROM cj_deals_deduped pw
                    INNER JOIN mart_cust_journey.cj_lcs_month contact_lifecycle ON pw.hs_contact_id = contact_lifecycle.contact_id AND contact_lifecycle.lead_id= pw.lead_id
                    INNER JOIN mart_cust_journey.cj_contact_facts contact_facts ON contact_lifecycle.contact_id = contact_facts.contact_id AND contact_lifecycle.lead_id = contact_facts.lead_id
           ) a
        WHERE month_rank= 1) b
        WHERE lifecycle_rank = 1
        GROUP BY 1, 2 ,3, 4, 5, 6, 7, 8, 9, 10, 11
    ),

cj_deals_final AS (--deal assignment for Inbound/Outbound based on last source
    SELECT
        pw.*,
        CASE WHEN pw.deal_stage IN ('Closed Won', 'Active/Won') THEN pw.deal_allocation_aux
        WHEN pw.country IN ('Colombia', 'Mexico', 'Spain', 'Brazil', 'Poland', 'Argentina', 'Chile', 'Peru') AND pw.pipeline_type = 'Individual New Sales'
        AND ((source.source_so IS NOT NULL AND source.source_so NOT IN ('basic reference', 'Target tool [DA]', 'Sales Contact'))
            OR (source.last_source_so NOT IN ('basic reference','visiting pricing', 'Target tool [DA]', 'Sales Contact'))) THEN 'Inbound'
        WHEN pw.country = 'Turkiye' AND pw.pipeline_type = 'Individual New Sales' AND (source.last_source_so NOT IN ('Target tool [DA]', 'Sales Contact') OR source.source_so NOT IN ('Target tool [DA]', 'Sales Contact')) THEN 'Inbound'
        WHEN pw.country = 'Germany' AND pw.pipeline_type = 'Individual New Sales' AND (source.source_so NOT IN ('Target tool [DA]', 'Sales Contact') OR source.last_source_so NOT IN ('Target tool [DA]', 'Sales Contact')) THEN 'Inbound'
        WHEN pw.country = 'Italy' AND pw.pipeline_type = 'Individual New Sales' AND source.source_so IS NOT NULL
            AND (source.source_so NOT IN ('Target tool [DA]', 'Sales Contact', 'basic reference','other', 'new facility verification', 'Massive assignment', 'New verification','other')
                OR source.last_source_so NOT IN ('Target tool [DA]', 'Sales Contact', 'basic reference', 'Massive assignment', 'visiting pricing', 'e-commerce abandoned cart', 'new facility verification', 'New verification', 'other')) THEN 'Inbound'
        WHEN pw.country = 'Brazil' AND pw.pipeline_type = 'Clinics New Sales' AND (pw.sf_source IS NOT NULL AND LOWER(pw.sf_source) NOT LIKE '%outbound%' AND pw.sf_source NOT IN ('Target tool [DA]','Sales Contact')) THEN 'Inbound'
        WHEN  pw.country = 'Italy' AND pw.pipeline_type = 'Clinics New Sales' AND (NOT pw.sf_source IN ('Basic reference','Target tool [DA]','Sales Contact','visiting pricing','new facility verification','New verification') AND LOWER(pw.sf_source) NOT LIKE '%outbound%') THEN 'Inbound'
        WHEN pw.country IN ('Spain','Mexico') AND pw.pipeline_type = 'Clinics New Sales'
            AND (pw.sf_source  NOT IN ('other','Target tool [DA]','Sales Contact') AND pw.sf_source IS NOT NULL AND LOWER(pw.sf_source) NOT LIKE '%internal reference%' AND LOWER(pw.sf_source) NOT LIKE '%outbound%') THEN 'Inbound'
        WHEN pw.country = ('Spain') AND pw.product_won = 'Clinic Cloud' and pw.dealname NOT LIKE '%PMS deal%' THEN 'Inbound'
        WHEN  pw.country = 'Poland' AND pw.pipeline_type = 'Clinics New Sales' AND (pw.tag_deal NOT LIKE '%Cross sell%' AND hubspot_owner_id != 11078353) THEN 'Inbound'
        ELSE 'Outbound' END AS deal_allocation from cj_deals_deduped pw
LEFT JOIN last_source_list source
ON source.contact_id = pw.hs_contact_id
),

final_aux AS ( --setting up MALs/MQL rows
    SELECT
        DATE(main.new_lcs_date) AS lcs_date,
        DATE(main.interaction) AS web_date,
        main.country,
        COALESCE(main.contact_type_live, 'UNKNOWN') AS contact_type_live,
        COALESCE(main.contact_type, 'UNKNOWN') AS contact_type,
        main.hubspot_id,
        main.lead_id,
        main.verified,
        main.product_recommended,
        main.spec_split,
        main.facility_size,
        main.campaign,
        main.source,
        main.medium,
        main.keyword,
        main.content,
        main.hsa_net,
        main.new_mal_flag,
        COUNT(
            CASE
                WHEN main.lifecycle_stage = 'MAL'
                    THEN main.lead_id
            END) AS mal,
        COUNT(
            CASE
                WHEN main.lifecycle_stage = 'MQL'
                    THEN main.lead_id
            END) AS mql
    FROM hubspot_web_props_aux_1_filtered_end main
    GROUP BY
        DATE(main.new_lcs_date),
        DATE(main.interaction),
        main.country,
        main.contact_type_live,
        main.contact_type,
        main.hubspot_id,
        main.lead_id,
        main.verified,
        main.campaign,
        main.source,
        main.medium,
        main.keyword,
        main.content,
        main.hsa_net,
        main.product_recommended,
        main.spec_split,
        main.facility_size,
        main.new_mal_flag
)

SELECT --precalculating fields for Tableau and pulling post MAL/MQL stages for leads to show cohort view/CJ
    main.hubspot_id AS hubspot_id,
    main.country,
    deal.deal_id,
    main.lead_id,
    deal.deal_month,
    deal.product_won,
    CASE WHEN UPPER(LEFT(main.campaign, 2)) IS NULL OR NOT UPPER(LEFT(main.campaign, 2)) IN ('PL', 'ES', 'TR', 'MX', 'CO', 'CL', 'BR', 'IT', 'DE', 'PE')
        THEN
        CASE WHEN main.country = 'Poland' THEN 'PL'
            WHEN main.country = 'Spain' THEN 'ES'
            WHEN main.country = 'Turkiye' THEN 'TR'
            WHEN main.country = 'Mexico' THEN 'MX'
            WHEN main.country = 'Colombia' THEN 'CO'
            WHEN main.country = 'Chile' THEN 'CL'
            WHEN main.country = 'Brazil' THEN 'BR'
            WHEN main.country = 'Italy' THEN 'IT'
            WHEN main.country = 'Germany' THEN 'DE'
            WHEN main.country = 'Peru' THEN 'PE'
        END
        ELSE UPPER(LEFT(main.campaign, 2))
    END AS test_country_final,
    main.contact_type_live,
    main.contact_type,
    main.lcs_date AS date,
    main.web_date,
    main.facility_size,
    deal.deal_allocation,
    deal.deal_allocation_aux,
    UPPER(LEFT(main.campaign, 2)) AS country_by_campaign,
    main.campaign,
    main.source,
    CASE WHEN SPLIT_PART(main.campaign, '_', 3) = 'mal-mql' OR SPLIT_PART(main.campaign, '_', 3) = 'mql-mal'
        THEN
        CASE WHEN REGEXP_SUBSTR(main.content, '^[a-zA-Z]{2}_.*') LIKE '%_mal_%' THEN 'mal'
                                WHEN REGEXP_SUBSTR(main.content, '^[a-zA-Z]{2}_.*') LIKE '%_mql_%' THEN 'mql'
            ELSE SPLIT_PART(main.campaign, '_', 3)
        END
        ELSE SPLIT_PART(main.campaign, '_', 3)
    END
    AS campaign_goal,
    CASE WHEN SPLIT_PART(main.campaign, '_', 2) = 'doc-fac' OR SPLIT_PART(main.campaign, '_', 2) = 'fac-doc' OR SPLIT_PART(main.campaign, '_', 2) = 'docfac'
            THEN
            CASE WHEN REGEXP_SUBSTR(main.content, '^[a-zA-Z]{2}_.*') LIKE '%_doc_%' THEN 'doc'
                WHEN REGEXP_SUBSTR(main.content, '^[a-zA-Z]{2}_.*') LIKE '%_fac_%' THEN 'fac'
                WHEN REGEXP_SUBSTR(main.content, '^[a-zA-Z]{2}_.*') LIKE '%_ent_%' THEN 'ent'
                ELSE SPLIT_PART(main.campaign, '_', 2)
            END
        ELSE SPLIT_PART(main.campaign, '_', 2)
    END
    AS campaign_target,
    CASE WHEN main.medium = 'cpc' THEN 'ppc'
        WHEN main.medium = 'Social_paid' THEN 'paid-social'
        WHEN main.medium IS NULL OR main.medium = '' OR main.medium = '\{\{ placement \}\}' THEN 'No info'
        ELSE main.medium END AS medium,
    main.keyword,
    CASE WHEN REGEXP_SUBSTR(main.content, '^[a-zA-Z]{2}_.*') IS NULL OR REGEXP_SUBSTR(main.content, '^[a-zA-Z]{2}_.*') = '' OR main.source = 'google' THEN 'None' ELSE REGEXP_SUBSTR(main.content, '^[a-zA-Z]{2}_.*') END AS adset_name,
    CASE WHEN (LOWER(main.campaign) LIKE '%feegow%' OR LOWER(main.campaign) LIKE '%-fg-%') AND main.country = 'Brazil' THEN 'Feegow'
        WHEN (LOWER(main.campaign) LIKE '%clinic-cloud%' OR LOWER(main.campaign) LIKE '%-cc-%') AND main.country = 'Spain' THEN 'Clinic Cloud'
        WHEN LOWER(main.campaign) LIKE '%gipo%' AND main.country = 'Italy' THEN 'GIPO'
        WHEN LOWER(main.campaign) LIKE '%mydr%' AND main.country = 'Poland' THEN 'MyDr'
        WHEN LOWER(main.campaign) LIKE '%_noa%' OR LOWER(main.campaign) LIKE '%-noa%' THEN 'Noa'
        WHEN LOWER(main.campaign) LIKE '%tuotempo%' THEN 'TuoTempo'
        ELSE 'Agenda' END AS campaign_product,
    CASE
        WHEN LOWER(main.keyword) LIKE '%website%' OR LOWER(main.content) LIKE '%website%' THEN 'Website'
        WHEN LOWER(main.keyword) LIKE '%instant-form%' OR LOWER(main.content) LIKE '%instant-form%' THEN 'Instant Form'
        WHEN LOWER(main.keyword) LIKE '%messenger%' OR LOWER(main.content) LIKE '%messenger%' THEN 'Messenger'
        WHEN LOWER(main.keyword) LIKE '%whatsapp%' OR LOWER(main.content) LIKE '%whatsapp%' THEN 'WhatsApp'
        WHEN LOWER(main.keyword) LIKE '%instagram%' OR LOWER(main.content) LIKE '%instagram%' THEN 'Instagram'
        WHEN LOWER(main.keyword) LIKE '%calls%' OR LOWER(main.content) LIKE '%calls%' THEN 'Calls'
        WHEN LOWER(main.keyword) LIKE '%app%' OR LOWER(main.content) LIKE '%app%' THEN 'App'
        WHEN LOWER(main.keyword) LIKE '%engagement%' OR LOWER(main.content) LIKE '%engagement%' THEN 'Engagement'
        ELSE 'Other'
    END AS conversion_location,
    CASE
        WHEN LOWER(main.keyword) LIKE '%lal%' OR LOWER(main.content) LIKE '%lal%' THEN 'LAL'
        WHEN LOWER(main.keyword) LIKE '%website-visitors%' OR LOWER(main.content) LIKE '%website-visitors%' THEN 'Website Visitors'
        WHEN LOWER(main.keyword) LIKE '%hubspot-list%' OR LOWER(main.content) LIKE '%hubspot-list%' THEN 'Hubspot List'
        WHEN LOWER(main.keyword) LIKE '%broad%' OR LOWER(main.content) LIKE '%broad%' THEN 'Broad'
        WHEN LOWER(main.keyword) LIKE '%interest%' OR LOWER(main.content) LIKE '%interest%' THEN 'Interest'
        WHEN LOWER(main.keyword) LIKE '%other-audience%' OR LOWER(main.content) LIKE '%other-audience%' THEN 'Other Audience'
        ELSE 'Other'
    END AS targeting_method,
    CASE
        WHEN LOWER(main.keyword) LIKE '%ebook%' OR LOWER(main.content) LIKE '%ebook%' THEN 'Ebook'
        WHEN LOWER(main.keyword) LIKE '%webinar%' OR LOWER(main.content) LIKE '%webinar%' THEN 'Webinar'
        WHEN LOWER(main.keyword) LIKE '%offer-request%' OR LOWER(main.content) LIKE '%offer-request%' THEN 'Offer Request'
        WHEN LOWER(main.keyword) LIKE '%partnership%' OR LOWER(main.content) LIKE '%partnership%' THEN 'Partnership'
        WHEN LOWER(main.keyword) LIKE '%other-magnet%' OR LOWER(main.content) LIKE '%other-magnet%' THEN 'Other Magnet'
        WHEN LOWER(main.keyword) LIKE '%free-trial%' OR LOWER(main.content) LIKE '%free-trial%'
         OR LOWER(main.keyword) LIKE '%free-trail%' OR LOWER(main.content) LIKE '%free-trail%' THEN 'Free Trial'
        WHEN LOWER(main.keyword) LIKE '%event%' OR LOWER(main.content) LIKE '%event%' THEN 'Event'
        WHEN LOWER(main.keyword) LIKE '%free-profile%' OR LOWER(main.content) LIKE '%free-profile%' THEN 'Free Profile'
        WHEN LOWER(main.keyword) LIKE '%infographic%' OR LOWER(main.content) LIKE '%infographic%' THEN 'Infographic'
        WHEN LOWER(main.keyword) LIKE '%free-tools%' OR LOWER(main.content) LIKE '%free-tools%' THEN 'Free Tools'
        ELSE 'Other'
    END AS lead_magnet,
    CASE
        WHEN LOWER(main.keyword) LIKE '%video%' OR LOWER(main.content) LIKE '%video%' THEN 'Video'
        WHEN LOWER(main.keyword) LIKE '%static%' OR LOWER(main.content) LIKE '%static%' THEN 'Static'
        WHEN LOWER(main.keyword) LIKE '%animated%' OR LOWER(main.content) LIKE '%animated%' THEN 'Animated'
        WHEN LOWER(main.keyword) LIKE '%carousel%' OR LOWER(main.content) LIKE '%carousel%' THEN 'Carousel'
        WHEN LOWER(main.keyword) LIKE '%other-creative%' OR LOWER(main.content) LIKE '%other-creative%' THEN 'Other Creative'
        ELSE 'Other'
    END AS creative_type,
    main.hsa_net,
    main.verified,
    main.product_recommended,
    CASE WHEN main.country IN ('Brazil', 'Mexico', 'Chile') AND main.spec_split IS NULL THEN 'Paramedical'
        WHEN main.country IN ('Brazil', 'Mexico', 'Chile') AND main.spec_split IS NOT NULL THEN main.spec_split
    END AS br_specialisation,
    main.mal,
    main.mql,
    COUNT(DISTINCT -- added distinct here to avoid duplicities
        CASE
            WHEN contact.lifecycle_stage = 'SAL'
                THEN contact.lead_id
        END) AS sal,
    COUNT(DISTINCT  -- added distinct here to avoid duplicities
        CASE
            WHEN contact.lifecycle_stage = 'SQL'
                THEN contact.lead_id
        END) AS sql,
    COUNT(DISTINCT
        CASE WHEN deal.deal_id IS NOT NULL
                THEN contact.lead_id
        END) AS won,
    COUNT(DISTINCT
        CASE
            WHEN contact.lifecycle_stage = 'CLOSED-LOST'
                THEN contact.lead_id
        END) AS lost,
    COUNT(DISTINCT
        CASE
            WHEN contact.lifecycle_stage = 'UNQ'
                THEN contact.lead_id
        END) AS unq,
    NVL(deal.total_revenue_origina_curr, 0) AS amount,
    ROUND(NVL(deal.total_revenue_eur, 0), 2) AS amount_eur,
    CASE WHEN contact1.unq_date IS NOT NULL THEN contact1.unq_date
        WHEN contact1.recycle_lost_date IS NOT NULL THEN contact1.recycle_lost_date
        WHEN contact1.lost_date IS NOT NULL THEN contact1.lost_date
        WHEN contact1.recycle_lost_date IS NOT NULL THEN contact1.recycle_lost_date
        WHEN contact1.stage_lost_date IS NOT NULL THEN contact1.stage_lost_date END AS lost_journey,
    CASE WHEN (main.campaign LIKE '%noa%') AND main.contact_type IN ('DOCTOR', 'DOCTOR&FACILITY - 2IN1', 'SECRETARY', 'UNKNOWN') THEN 'DOCTOR'
        --WHEN main.lcs_date >= '2024-01-01' AND main.contact_type = 'MARKETING'
            --AND NOT (main.campaign LIKE '%mydr%') THEN 'OTHER'
        --as per Jonathan´s request all Marketing segment contacts not to be counted for any campaigns except PMS ones from 2024 --only valid for Poland based on feedback in May 24
        WHEN main.lcs_date <= '2024-06-01' AND main.contact_type = 'DIAGNOSTIC' AND main.country = 'Italy' THEN 'OTHER' --before June 2024 Diagnostics were not counted as in-target facility. changed 19.07 as per Jonathan/Camilla
        WHEN main.country = 'Poland' AND main.campaign LIKE '%mydr%' THEN
            CASE WHEN main.contact_type IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1', 'MARKETING') THEN 'FACILITY' ELSE 'OTHER' END
        WHEN main.country = 'Poland' THEN
            CASE WHEN main.contact_type IN ('DOCTOR', 'HEALTH - GENERAL') THEN 'DOCTOR'
                WHEN main.contact_type IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                ELSE 'OTHER' END
        WHEN main.country = 'Peru' THEN
            CASE WHEN main.contact_type IN ('SECRETARY', 'DOCTOR') THEN 'DOCTOR'
                ELSE 'OTHER' END
        WHEN main.country = 'Brazil' AND (main.campaign LIKE '%feegow%' OR LOWER(main.campaign) LIKE '%-fg-%') THEN
            CASE WHEN main.contact_type IN ('PATIENT', 'UNKNOWN', 'STUDENT') THEN 'OTHER' ELSE 'FACILITY' END
        WHEN main.country = 'Brazil' THEN
            CASE WHEN main.contact_type IN ('SECRETARY', 'DOCTOR') THEN 'DOCTOR'
                WHEN main.contact_type IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                ELSE 'OTHER' END
        WHEN main.country = 'Italy' AND main.contact_type IN ('GENERAL PRACTITIONER', 'GENERAL PRACTITIONER & DOCTOR') THEN 'GP'
        WHEN main.country = 'Italy' AND main.campaign LIKE '%gipo%' THEN
             CASE WHEN main.contact_type IN ('PATIENT', 'UNKNOWN', 'STUDENT') THEN 'OTHER' ELSE 'FACILITY' END
        WHEN main.country = 'Italy' THEN
            CASE WHEN main.contact_type IN ('SECRETARY', 'DOCTOR') THEN 'DOCTOR'
                WHEN main.contact_type IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1', 'DIAGNOSTIC') THEN 'FACILITY'
                ELSE 'OTHER' END
        WHEN main.country = 'Spain' AND (main.campaign LIKE '%clinic-cloud%' OR LOWER(main.campaign) LIKE '%-cc-%') THEN
            CASE WHEN main.contact_type IN ('PATIENT', 'UNKNOWN', 'STUDENT') THEN 'OTHER' ELSE 'FACILITY' END
        WHEN main.country = 'Spain' THEN
            CASE WHEN main.contact_type IN ('SECRETARY', 'DOCTOR') THEN 'DOCTOR'
                WHEN main.contact_type IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                ELSE 'OTHER' END
        WHEN main.country IN ('Mexico', 'Turkiye', 'Colombia') THEN
            CASE WHEN main.contact_type IN ('SECRETARY', 'DOCTOR') THEN 'DOCTOR'
                WHEN main.contact_type IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                ELSE 'OTHER' END
        WHEN main.country = 'Germany' THEN
            CASE WHEN main.contact_type IN ('PATIENT', 'UNKNOWN', 'MARKETING') THEN 'OTHER'
                ELSE 'DOCTOR' END
        WHEN main.country = 'Chile' THEN
            CASE WHEN main.contact_type IN ('DOCTOR', 'SECRETARY', 'DOCTOR&FACILITY - 2IN1') THEN 'DOCTOR'
                ELSE 'OTHER' END
    END AS target,
    main.new_mal_flag
FROM final_aux main
LEFT JOIN cj_deals_final deal ON deal.hs_contact_id = main.hubspot_id
    AND deal.deal_stage_start >= main.lcs_date --noqa
LEFT JOIN mart_cust_journey.cj_contact_facts contact1 ON main.hubspot_id = contact1.contact_id
        AND main.lead_id = contact1.lead_id
LEFT JOIN cj_data_layer.cj_contact_lead_main contact
    ON main.hubspot_id = contact.contact_id
        AND main.lead_id = contact.lead_id
        AND main.lcs_date <= contact.updated_at  -- noqa
GROUP BY
    main.lcs_date,
    main.web_date,
    main.country,
    main.contact_type_live,
    main.contact_type,
    main.campaign,
    main.source,
    main.medium,
    main.keyword,
    main.content,
    main.hsa_net,
    main.mal,
    main.mql,
    deal.total_revenue_origina_curr,
    deal.total_revenue_eur,
    main.verified,
    main.product_recommended,
    main.spec_split,
    main.facility_size,
    deal.deal_month,
    main.hubspot_id,
    deal.product_won,
    lost_journey,
    main.content,
    deal.deal_id,
    main.lead_id,
    deal.deal_allocation,
    deal.deal_allocation_aux,
    main.new_mal_flag
