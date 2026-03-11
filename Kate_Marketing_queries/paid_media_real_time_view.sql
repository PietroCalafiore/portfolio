WITH all_valid_MALs_MQLs AS (
        SELECT
            contact.contact_id AS hubspot_id,
            contact.lead_id,
            contact_facts.verified,
            contact.lifecycle_stage,
            contact.updated_at as lcs_date,
            contact.is_mkt_push_lead,
            contact_facts.country,
            contact_facts.segment AS contact_type,
            contact_facts.product_recommended,
            contact_facts.spec_split
        FROM cj_data_layer.cj_contact_lead_main contact
        INNER JOIN mart_cust_journey.cj_contact_facts contact_facts
            ON contact_facts.contact_id = contact.contact_id
                AND contact_facts.lead_id = contact.lead_id
                AND contact_facts.country IN ('Spain', 'Brazil', 'Mexico', 'Turkey', 'Turkiye', 'Poland', 'Italy', 'Germany', 'Colombia', 'Chile','Peru')
        WHERE contact.lifecycle_stage IN ('MAL', 'MQL') AND (contact_facts.segment != 'PATIENT' OR contact_facts.segment IS NULL)
            AND (contact.is_mkt_push_lead IS NULL
                OR (contact.is_mkt_push_lead IN (0, 1)))
    ),

    all_valid_MALs_MQLs_list AS (
        SELECT
            hubspot_id,
            lead_id-- we need a list with just one row per contact
        FROM all_valid_MALs_MQLs
        GROUP BY hubspot_id,
            lead_id
    ),

    campaign_interaction_scope AS (
        SELECT
            utm_log.hubspot_id,
            full_scope.lead_id,
            full_scope.is_mkt_push_lead,
            full_scope.contact_type,
            full_scope.verified,
            full_scope.product_recommended,
            full_scope.spec_split,
            utm_log.country,
            utm_log.utm_campaign AS campaign,
            LAG(utm_log.utm_campaign) OVER (PARTITION BY utm_log.hubspot_id ORDER BY utm_log.updated_at) AS prev_campaign_name,--we use this to select the first row after a campaign name has been updated
            -- MIN(utm_log.updated_at) OVER (PARTITION BY utm_log.hubspot_id, utm_log.utm_campaign) AS first_campaign_interaction,
            utm_log.updated_at as interaction,
            ROW_NUMBER() OVER (PARTITION BY utm_log.hubspot_id, utm_log.utm_campaign order by utm_log.updated_at DESC ) AS rn,
            COALESCE(utm_log.utm_source, 'No info') AS source,
            COALESCE(utm_log.utm_medium, 'No info') AS medium,
            COALESCE(utm_log.utm_term, 'No info') AS keyword,
            COALESCE(utm_log.utm_content, 'No info') AS content,
            CASE
                WHEN utm_log.utm_source = 'facebook' OR utm_log.utm_source = 'Social_Ads' OR utm_log.utm_source = 'fb'
                    OR utm_log.utm_source = 'ig'
                    THEN 'facebook'
                WHEN utm_log.utm_source = 'linkedin' THEN 'linkedin'
                WHEN utm_log.utm_source = 'softdoit' THEN 'softdoit'
                WHEN utm_log.utm_source = 'criteo' THEN 'criteo'
                WHEN utm_log.utm_source = 'bing' THEN 'bing'
                WHEN utm_log.utm_source = 'taboola' THEN 'taboola'
                WHEN utm_log.utm_source = 'tiktok' THEN 'tiktok'
                WHEN utm_log.utm_source = 'google' OR utm_log.utm_source = 'adwords' THEN 'google'
            END AS hsa_net
        FROM all_valid_MALs_MQLs full_scope
        INNER JOIN cj_data_layer.cj_sat_contact_hs_1h_log_merged utm_log
            ON utm_log.hubspot_id = full_scope.hubspot_id
        WHERE utm_log.utm_campaign IS NOT NULL AND NOT (utm_log.country = 'Brazil' AND (LOWER(utm_log.utm_campaign) LIKE '%\\_wa\\_%' OR LOWER(utm_log.utm_campaign) LIKE '%\\_wa\\-%'))
            AND utm_log.utm_source IN ('facebook', 'fb', 'ig', 'Social_Ads', 'linkedin', 'criteo', 'google', 'adwords', 'bing', 'taboola', 'tiktok', 'softdoit')
            AND (utm_log.utm_campaign LIKE '%\\_%\\_%\\_%' OR utm_log.utm_campaign IN ('it_gipo_mal', 'it_fac_mal'))
            AND utm_log.country IN ('Spain', 'Brazil', 'Mexico', 'Turkey', 'Turkiye', 'Poland', 'Italy', 'Germany', 'Colombia', 'Chile','Peru')
        GROUP BY
            utm_log.hubspot_id,
            full_scope.hubspot_id,
            full_scope.lead_id,
            utm_log.country,
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
            full_scope.spec_split
    ),
        utm_log_LCS_join AS (
            SELECT lcs.hubspot_id,
                   lcs.lead_id,
                   lcs.is_mkt_push_lead,
                   lcs.contact_type,
                   lcs.verified,
                   lcs.product_recommended,
                   lcs.spec_split,
                   lcs.country,
                   utm.campaign,
                   utm.source,
                   utm.medium,
                   utm.keyword,
                   utm.content,
                   utm.hsa_net,
                   lcs.lifecycle_stage,
                    CASE WHEN  DATEPART(HOUR, lcs.lcs_date) = 0 AND DATEPART(MINUTE,lcs.lcs_date) = 0 AND DATEPART(SECOND, lcs.lcs_date) = 0
                    THEN DATEADD(SECOND, -1, DATEADD(DAY, 0, lcs.lcs_date)) ELSE lcs.lcs_date END AS new_lcs_date,
                   lcs.lcs_date,
                   prev_campaign_name,
                   interaction
                   From campaign_interaction_scope utm
                INNER JOIN all_valid_MALs_MQLs lcs on utm.hubspot_id = lcs.hubspot_id
                   where  new_lcs_date  BETWEEN interaction - INTERVAL '1 hour'  AND interaction::DATE + INTERVAL '31 day' and
    ((prev_campaign_name <> campaign)or prev_campaign_name is null)
           ),
       lcs_utm_order_control as(
        SELECT * ,
          ROW_NUMBER() OVER (PARTITION BY hubspot_id, country, lifecycle_stage,new_lcs_date
                ORDER BY new_lcs_date desc, interaction DESC) AS lcs_order,
            ROW_NUMBER() OVER (PARTITION BY hubspot_id, country, lifecycle_stage,interaction
                ORDER BY  new_lcs_date asc) AS interaction_order
            from utm_log_LCS_join
            WHERE TRUE
            QUALIFY lcs_order = 1 and interaction_order = 1
        ),
    hubspot_web_props_aux_1_filtered_end as (
            SELECT *,
        LAG(lead_id,1) OVER (PARTITION BY hubspot_id ORDER BY new_lcs_date) AS prev_lead_id,--we use this to select the first row after a campaign name has been updated
        LAG(campaign,1) OVER (PARTITION BY hubspot_id  ORDER BY new_lcs_date) AS previous_campaign_name,--we use this to select the first row after a campaign name has been updated
        LAG(lifecycle_stage,1) OVER (PARTITION BY hubspot_id  ORDER BY new_lcs_date) AS previous_lcs
            FROM   lcs_utm_order_control
               WHERE TRUE
        QUALIFY NOT (previous_campaign_name = campaign and lead_id <> prev_lead_id) or prev_lead_id is nULL
        ),

    cj_deals_full_scope AS (
        select  deal.country,deal.month as deal_month,deal_stage_start,deal.hs_contact_id, deal.lead_id, deal.deal_id, NVL(deal_length,12) as deal_length,
               MAX(mrr_euro) OVER( partition by deal.deal_id) as mrr_euro,
               MAX(mrr_original_currency) OVER( partition by deal.deal_id) as mrr_original_currency
               from mart_cust_journey.cj_deal_month deal
                INNER JOIN all_valid_MALs_MQLs_list scope -- we select just the ones that arrive to won stage after entering the correct way to the funnel, in this case MAL and MQL
               ON deal.hs_contact_id = scope.hubspot_id
               where deal.deal_stage IN ('Closed Won') and pipeline_type IN ('Individual New Sales','Clinics New Sales')
                and is_current_stage and stage_is_month_new
                GROUP BY deal_month,deal.hs_contact_id, deal.lead_id, deal.deal_id, deal_length, deal.country,mrr_euro,mrr_original_currency,deal_stage_start
        ),
        cj_deals_deduped AS(
        select  deal.country,
                deal_month,
                deal.hs_contact_id,
                deal.lead_id, deal.deal_id,
                deal_length,
                deal_stage_start,
                mrr_euro,
                NVL(ROUND(mrr_euro*deal_length,2),0) as total_revenue_eur,
                NVL(ROUND(mrr_original_currency * deal_length,2),0) AS total_revenue_origina_curr
        from  cj_deals_full_scope deal
        group by 1,2,3,4,5,6,7,8,9,10
        ),
    next as(
    select
        DATE_TRUNC('day',main.new_lcs_date) as date,
        main.country,
        lead_id,
        main.verified,
        main.spec_split,
        main.campaign,
        main.hsa_net,
        main.content,
        coalesce(main.contact_type,'UNKNOWN') as contact_type,
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
    group by 1,2,3,4,5,6,7,8,9,main.lifecycle_stage,main.lead_id)
    select
        date,
        country,
        main.verified,
        main.spec_split,
        main.campaign,
        main.hsa_net,
        main.content,
         UPPER(LEFT(main.campaign,2)) AS country_by_campaign,
             CASE WHEN SPLIT_PART(main.campaign,'_',3) = 'mal-mql' OR SPLIT_PART(main.campaign,'_',3) = 'mql-mal'
        THEN
        CASE WHEN REGEXP_SUBSTR(main.content,'^[a-zA-Z]{2}_.*') LIKE '%_mal_%' THEN 'mal'
         WHEN REGEXP_SUBSTR(main.content,'^[a-zA-Z]{2}_.*') LIKE '%_mql_%' THEN  'mql'
            ELSE SPLIT_PART(main.campaign,'_',3)
        END
        WHEN SPLIT_PART(main.campaign,'_',3) = 'pure' THEN  'mql'
        ELSE SPLIT_PART(main.campaign,'_',3)
        END
        as campaign_goal,
           CASE WHEN SPLIT_PART(main.campaign,'_',2) = 'doc-fac' OR SPLIT_PART(main.campaign,'_',2) = 'fac-doc'
        THEN
        CASE WHEN REGEXP_SUBSTR(main.content,'^[a-zA-Z]{2}_.*') LIKE '%_doc_%' THEN 'doc'
         WHEN REGEXP_SUBSTR(main.content,'^[a-zA-Z]{2}_.*') LIKE '%_fac_%' THEN  'fac'
            ELSE SPLIT_PART(main.campaign,'_',2)
        END
        ELSE SPLIT_PART(main.campaign,'_',2)
        END
     as campaign_target,
            CASE WHEN LOWER(main.campaign) LIKE '%feegow%' or LOWER(main.campaign)  LIKE '%-fg-%'  THEN 'Feegow'
    WHEN LOWER(main.campaign) LIKE '%clinic-cloud%' or LOWER(main.campaign)  LIKE '%-cc-%'  THEN 'Clinic Cloud'
    WHEN LOWER(main.campaign) LIKE '%gipo%' THEN 'GIPO'
    WHEN LOWER(main.campaign) LIKE '%mydr%' THEN 'MyDr'
    ELSE 'Agenda' END as Campaign_Product,
    CASE WHEN main.date >= '2024-01-01' AND main.contact_type = 'MARKETING'
        AND NOT (main.campaign LIKE '%mydr%') THEN 'OTHER'
        --as per Jonathan´s request all Marketing segment contacts not to be counted for any campaigns except PMS ones from 2024 --only valid for Poland based on feedback in May 24
        WHEN main.date <= '2024-06-01' AND main.contact_type = 'DIAGNOSTIC'  AND main.country = 'Italy' THEN 'OTHER' --before June 2024 Diagnostics were not counted as in-target facility. changed 19.07 as per Jonathan/Camilla
        WHEN main.country = 'Poland' AND main.campaign LIKE '%mydr%' THEN
            CASE WHEN main.contact_type IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY' ELSE 'OTHER' END
        WHEN main.country = 'Poland' THEN
            CASE WHEN main.contact_type IN ('DOCTOR', 'HEALTH - GENERAL') THEN 'DOCTOR'
                WHEN main.contact_type IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                ELSE 'OTHER' END
        WHEN main.country IN ('Colombia','Peru') THEN
            CASE WHEN main.contact_type IN ('SECRETARY', 'DOCTOR') THEN 'DOCTOR'
                WHEN main.contact_type IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                ELSE 'OTHER' END
        WHEN main.country = 'Brazil' AND (main.campaign LIKE '%feegow%' or LOWER(main.campaign)  LIKE '%-fg-%')  THEN
            CASE WHEN main.contact_type IN ('PATIENT', 'UNKNOWN', 'STUDENT') THEN 'OTHER' ELSE 'FACILITY' END
        WHEN main.country = 'Brazil' THEN
            CASE WHEN main.contact_type IN ('SECRETARY', 'DOCTOR') THEN 'DOCTOR'
                WHEN main.contact_type IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                ELSE 'OTHER' END
        WHEN main.country = 'Italy' AND main.campaign LIKE '%gipo%' THEN
       CASE WHEN main.contact_type IN ('PATIENT', 'UNKNOWN', 'STUDENT') THEN 'OTHER' ELSE 'FACILITY' END
        WHEN main.country = 'Italy' THEN
            CASE WHEN main.contact_type IN ('SECRETARY', 'DOCTOR') THEN 'DOCTOR'
                WHEN main.contact_type IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1','DIAGNOSTIC') THEN 'FACILITY'
                WHEN main.contact_type IN ('GENERAL PRACTITIONER', 'GENERAL PRACTITIONER & DOCTOR') THEN 'GP'
                ELSE 'OTHER' END
        WHEN main.country = 'Spain' AND (main.campaign LIKE '%clinic-cloud%' OR LOWER(main.campaign)  LIKE '%-cc-%') THEN
            CASE WHEN main.contact_type IN ('PATIENT', 'UNKNOWN', 'STUDENT') THEN 'OTHER' ELSE 'FACILITY' END
        WHEN main.country = 'Spain' THEN
            CASE WHEN main.contact_type IN ('SECRETARY', 'DOCTOR') THEN 'DOCTOR'
                WHEN main.contact_type IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                ELSE 'OTHER' END
        WHEN main.country = 'Mexico' THEN
            CASE WHEN main.contact_type IN ('SECRETARY', 'DOCTOR') THEN 'DOCTOR'
                WHEN main.contact_type IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                ELSE 'OTHER' END
        WHEN main.country = 'Turkiye' THEN
            CASE WHEN main.contact_type IN ('SECRETARY', 'DOCTOR') THEN 'DOCTOR'
                WHEN main.contact_type IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                ELSE 'OTHER' END
        WHEN main.country = 'Germany' THEN
            CASE WHEN main.contact_type IN ('PATIENT', 'UNKNOWN', 'MARKETING') THEN 'OTHER'
                ELSE 'DOCTOR' END
        WHEN main.country = 'Chile' THEN
            CASE WHEN main.contact_type IN ('DOCTOR', 'SECRETARY', 'DOCTOR&FACILITY - 2IN1') THEN 'DOCTOR'
                ELSE 'OTHER' END
    END AS Target,
            CASE WHEN main.country IN ('Brazil','Mexico','Chile') and main.spec_split IS NULL THEN 'Paramedical'
    WHEN main.country IN ('Brazil','Mexico','Chile') and main.spec_split IS NOT NULL THEN main.spec_split
        ELSE NULL END as br_mx_cl_specialisation,
           contact_type,
           sum(mal) as MALs,
           sum(mql) as MQLs from next main
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
