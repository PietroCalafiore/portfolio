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
        contact_facts.spec_split,
        contact_facts.facility_size
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
        full_scope.facility_size,
        utm_log.country,
        utm_log.utm_campaign AS campaign,
        LAG(utm_log.utm_campaign) OVER (PARTITION BY utm_log.hubspot_id ORDER BY utm_log.updated_at) AS prev_campaign_name,--we use this to select the first row after a campaign name has been updated
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
    WHERE utm_log.utm_campaign IS NOT NULL
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
        full_scope.spec_split,
        full_scope.facility_size
),
    utm_log_LCS_join AS (
        SELECT lcs.hubspot_id,
               lcs.lead_id,
               lcs.is_mkt_push_lead,
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
               lcs.lifecycle_stage,
                CASE WHEN  DATEPART(HOUR, lcs.lcs_date) = 0 AND DATEPART(MINUTE,lcs.lcs_date) = 0 AND DATEPART(SECOND, lcs.lcs_date) = 0
                THEN DATEADD(SECOND, -1, DATEADD(DAY, 0, lcs.lcs_date)) ELSE lcs.lcs_date END AS new_lcs_date,
               lcs.lcs_date,
               prev_campaign_name,
               interaction
               From campaign_interaction_scope utm
            INNER JOIN all_valid_MALs_MQLs lcs on utm.hubspot_id = lcs.hubspot_id
               where  new_lcs_date  BETWEEN interaction - INTERVAL '1 hour'  AND interaction::DATE + INTERVAL '31 day' and
((prev_campaign_name <> campaign) or prev_campaign_name is null)
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
hubspot_web_props_aux_1_filtered_end  as (
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
           MAX(mrr_original_currency) OVER( partition by deal.deal_id) as mrr_original_currency,
     CASE WHEN  deal.country = 'Brazil' and deal.pipeline like '%PMS%' then 'Feegow'
      WHEN  deal.country = 'Italy' and deal.pipeline like '%PMS%' then 'Gipo'
    WHEN  deal.country = 'Spain' and deal.pipeline like '%Enterprise SaaS4Clinics / Marketplace%' then 'Clinic Cloud'
       WHEN  deal.country = 'Poland' and deal.pipeline like '%PMS%' then 'MyDr'
 WHEN deal.pipeline_type = 'Individual New Sales' then 'Agenda Premium'
        ELSE 'Clinic Agenda' end as product_won
           from mart_cust_journey.cj_deal_month deal
            INNER JOIN all_valid_MALs_MQLs_list scope -- we select just the ones that arrive to won stage after entering the correct way to the funnel, in this case MAL and MQL
           ON deal.hs_contact_id = scope.hubspot_id
           where deal.deal_stage IN ('Closed Won') and pipeline_type IN ('Individual New Sales','Clinics New Sales')
           and is_current_stage and stage_is_month_new
            GROUP BY deal_month,deal.hs_contact_id, deal.lead_id, deal.deal_id, deal_length, deal.country,mrr_euro,mrr_original_currency,deal_stage_start,deal.pipeline,deal.pipeline_type
    ),
    cj_deals_deduped AS(
    select  deal.country,
            deal_month,
            deal.hs_contact_id,
            deal.lead_id, deal.deal_id,
            deal_length,
            deal_stage_start,
            mrr_euro,
            product_won,
            NVL(ROUND(mrr_euro*deal_length,2),0) as total_revenue_eur,
            NVL(ROUND(mrr_original_currency * deal_length,2),0) AS total_revenue_origina_curr
    from  cj_deals_full_scope deal
    group by 1,2,3,4,5,6,7,8,9,10,11
    ),


final_aux AS (
    SELECT
        DATE(main.new_lcs_date) AS lcs_date,
        DATE(main.interaction) AS web_date,
        main.country,
        coalesce(main.contact_type,'UNKNOWN') as contact_type,
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
        main.facility_size
)

SELECT
    main.lcs_date AS date,
    main.lead_id,
    main.web_date,
    main.country,
    main.facility_size,
    UPPER(LEFT(main.campaign,2)) AS country_by_campaign,
    CASE WHEN UPPER(LEFT(main.campaign,2))  IS NULL OR NOT UPPER(LEFT(main.campaign,2)) IN ('PL','ES','TR','MX','CO','CL','BR','IT','DE','PE')
    THEN
    CASE WHEN  main.country = 'Poland' THEN 'PL'
    WHEN  main.country = 'Spain' THEN 'ES'
    WHEN  main.country = 'Turkiye' THEN 'TR'
    WHEN  main.country = 'Mexico' THEN 'MX'
    WHEN  main.country = 'Colombia' THEN 'CO'
    WHEN  main.country = 'Chile' THEN 'CL'
    WHEN  main.country = 'Brazil' THEN 'BR'
    WHEN  main.country = 'Italy' THEN 'IT'
    WHEN  main.country = 'Germany' THEN 'DE'
    WHEN  main.country = 'Peru' THEN 'PE'
    END
    ELSE UPPER(LEFT(main.campaign,2)) END AS test_country_final,
    main.contact_type,
    main.campaign,
    main.source,
    CASE WHEN SPLIT_PART(main.campaign,'_',3) = 'mal-mql' OR SPLIT_PART(main.campaign,'_',3) = 'mql-mal'
        THEN
        CASE WHEN REGEXP_SUBSTR(main.content,'^[a-zA-Z]{2}_.*') LIKE '%_mal_%' THEN 'mal'
         WHEN REGEXP_SUBSTR(main.content,'^[a-zA-Z]{2}_.*') LIKE '%_mql_%' THEN  'mql'
            ELSE SPLIT_PART(main.campaign,'_',3)
        END
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
    CASE WHEN main.medium = 'cpc' THEN 'ppc'
    WHEN main.medium = 'Social_paid' THEN 'paid-social'
    WHEN main.medium IS NULL OR main.medium = '' OR main.medium = '{{placement}}' THEN 'No info'
    ELSE main.medium END as medium,
    main.keyword,
    REGEXP_SUBSTR(main.content,'^[a-zA-Z]{2}_.*') as adset_name,
    CASE WHEN LOWER(main.campaign) LIKE '%feegow%' or LOWER(main.campaign)  LIKE '%-fg-%'  THEN 'Feegow'
    WHEN LOWER(main.campaign) LIKE '%clinic-cloud%' or LOWER(main.campaign)  LIKE '%-cc-%'  THEN 'Clinic Cloud'
    WHEN LOWER(main.campaign) LIKE '%gipo%' THEN 'GIPO'
    WHEN LOWER(main.campaign) LIKE '%mydr%' THEN 'MyDr'
    ELSE 'Agenda' END as Campaign_Product,
    main.hsa_net,
    main.verified,
    main.product_recommended,
   CASE WHEN main.country IN ('Brazil','Mexico','Chile') and main.spec_split IS NULL THEN 'Paramedical'
    WHEN main.country IN ('Brazil','Mexico','Chile') and main.spec_split IS NOT NULL THEN main.spec_split
        ELSE NULL END as br_specialisation,
    main.hubspot_id AS hubspot_id,
    main.mal,
    main.mql,
    deal.deal_month,
    deal.product_won,
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
    ROUND(NVL(deal.total_revenue_eur,0), 2) AS amount_eur,
    CASE WHEN contact1.unq_date IS NOT NULL THEN contact1.unq_date
    WHEN contact1.recycle_lost_date  IS NOT NULL THEN contact1.recycle_lost_date
        WHEN contact1.lost_date IS NOT NULL THEN contact1.lost_date
            WHEN contact1.recycle_lost_date IS NOT NULL THEN contact1.recycle_lost_date
   ELSE NULL END AS lost_journey,
 CASE WHEN main.lcs_date >= '2024-01-01' AND main.contact_type = 'MARKETING'
        AND NOT (main.campaign LIKE '%mydr%') THEN 'OTHER'
        --as per Jonathan´s request all Marketing segment contacts not to be counted for any campaigns except PMS ones from 2024 --only valid for Poland based on feedback in May 24
        WHEN main.country = 'Poland' AND main.campaign LIKE '%mydr%' THEN
            CASE WHEN main.contact_type IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY' ELSE 'OTHER' END
        WHEN main.country = 'Poland' THEN
            CASE WHEN main.contact_type IN ('DOCTOR', 'HEALTH - GENERAL') THEN 'DOCTOR'
                WHEN main.contact_type IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                ELSE 'OTHER' END
        WHEN main.country IN ('Colombia') THEN
            CASE WHEN main.contact_type IN ('SECRETARY', 'DOCTOR') THEN 'DOCTOR'
                WHEN main.contact_type IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                ELSE 'OTHER' END
         WHEN main.country IN ('Peru') THEN
            CASE WHEN main.contact_type IN ('SECRETARY', 'DOCTOR') THEN 'DOCTOR'
                ELSE 'OTHER' END
        WHEN main.country = 'Brazil' AND main.campaign LIKE '%feegow%'  THEN
            CASE WHEN main.contact_type IN ('PATIENT', 'UNKNOWN', 'STUDENT') THEN 'OTHER' ELSE 'FACILITY' END
        WHEN main.country = 'Brazil' THEN
            CASE WHEN main.contact_type IN ('SECRETARY', 'DOCTOR') THEN 'DOCTOR'
                WHEN main.contact_type IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                ELSE 'OTHER' END
        WHEN main.country = 'Italy' AND main.campaign LIKE '%gipo%' THEN
       CASE WHEN main.contact_type IN ('PATIENT', 'UNKNOWN', 'STUDENT') THEN 'OTHER' ELSE 'FACILITY' END
        WHEN main.country = 'Italy' THEN
            CASE WHEN main.contact_type IN ('SECRETARY', 'DOCTOR') THEN 'DOCTOR'
                WHEN main.contact_type IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                WHEN main.contact_type IN ('GENERAL PRACTITIONER', 'GENERAL PRACTITIONER & DOCTOR') THEN 'GP'
                ELSE 'OTHER' END
        WHEN main.country = 'Spain' AND main.campaign LIKE '%clinic-cloud%' THEN
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
    END AS Target
FROM final_aux main
    left join cj_deals_deduped deal on deal.hs_contact_id = main.hubspot_id
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
    deal.deal_month,
    main.lead_id,
    main.hubspot_id,
    deal.product_won,
    lost_journey,
    main.content,
    main.facility_size
