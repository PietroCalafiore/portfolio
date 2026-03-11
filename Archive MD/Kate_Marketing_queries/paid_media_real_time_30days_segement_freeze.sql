-- noqa: disable=L027
DROP TABLE IF EXISTS test.paid_media_frozen_segment
CREATE TABLE test.paid_media_frozen_segment AS
WITH all_valid_mals_mqls AS (
    SELECT
        contact.contact_id AS hubspot_id,
        contact.lead_id,
        contact_facts.verified,
        contact.lifecycle_stage,
        contact.updated_at AS lcs_date,
        contact.is_mkt_push_lead,
        contact_facts.country,
        contact_facts.segment AS contact_type,
        contact_facts.product_recommended,
        contact_facts.spec_split,
        COALESCE(hclh.contact_type_segment_test, contact_facts.segment) AS frozen_contact_type,
        ROW_NUMBER() over (PARTITION BY contact.contact_id,contact.lifecycle_stage, contact.updated_at ORDER BY dw_updated_at DESC) AS row
    FROM cj_data_layer.cj_contact_lead_main contact
    INNER JOIN mart_cust_journey.cj_contact_facts contact_facts
        ON contact_facts.contact_id = contact.contact_id
            AND contact_facts.lead_id = contact.lead_id
            AND contact_facts.country IN ('Spain', 'Brazil', 'Mexico', 'Turkey', 'Turkiye', 'Poland', 'Italy', 'Germany', 'Colombia', 'Chile', 'Peru')
    LEFT JOIN dw.hs_contact_live_history hclh
        ON hclh.hubspot_id=contact.contact_id
        AND hclh.dw_updated_at BETWEEN contact.updated_at AND DATEADD(day, 31, contact.updated_at)
    WHERE contact.lifecycle_stage IN ('MAL', 'MQL') --and contact.contact_id = 74261991008
        AND (contact_facts.segment != 'PATIENT' OR contact_facts.segment IS NULL)
        AND (contact.is_mkt_push_lead IS NULL OR (contact.is_mkt_push_lead IN (0, 1)))
        QUALIFY row=1
),

all_valid_mals_mqls_list AS ( -- we need a list with just one row per contact
    SELECT
        hubspot_id,
        lead_id
    FROM all_valid_mals_mqls
    GROUP BY hubspot_id, lead_id
),

campaign_interaction_scope AS (
    SELECT
        utm_log.hubspot_id,
        full_scope.lead_id,
        full_scope.is_mkt_push_lead,
        full_scope.contact_type,
        full_scope.frozen_contact_type,
        full_scope.verified,
        full_scope.product_recommended,
        full_scope.spec_split,
        utm_log.country,
        utm_log.utm_campaign AS campaign,
        LAG(utm_log.utm_campaign) OVER (PARTITION BY utm_log.hubspot_id ORDER BY utm_log.updated_at) AS prev_campaign_name,--we use this to select the first row after a campaign name has been updated
        utm_log.updated_at AS interaction,
        ROW_NUMBER() OVER (PARTITION BY utm_log.hubspot_id, utm_log.utm_campaign ORDER BY utm_log.updated_at DESC) AS rn,
        COALESCE(utm_log.utm_source, 'No info') AS source,
        COALESCE(utm_log.utm_medium, 'No info') AS medium,
        COALESCE(utm_log.utm_term, 'No info') AS keyword,
        COALESCE(utm_log.utm_content, 'No info') AS content,
        CASE WHEN utm_log.utm_source = 'facebook'
                OR utm_log.utm_source = 'Social_Ads'
                OR utm_log.utm_source = 'fb'
                OR utm_log.utm_source = 'ig' THEN 'facebook'
            WHEN utm_log.utm_source = 'linkedin' THEN 'linkedin'
            WHEN utm_log.utm_source = 'softdoit' THEN 'softdoit'
            WHEN utm_log.utm_source = 'criteo' THEN 'criteo'
            WHEN utm_log.utm_source = 'bing' THEN 'bing'
            WHEN utm_log.utm_source = 'taboola' THEN 'taboola'
            WHEN utm_log.utm_source = 'tiktok' THEN 'tiktok'
            WHEN utm_log.utm_source = 'capterra' THEN 'capterra'
            WHEN utm_log.utm_source = 'cronomia' THEN 'cronomia'
            WHEN utm_log.utm_source = 'google'
                OR utm_log.utm_source = 'adwords' THEN 'google'
        END AS hsa_net
    FROM all_valid_mals_mqls full_scope
    INNER JOIN cj_data_layer.cj_sat_contact_hs_1h_log_merged utm_log
        ON utm_log.hubspot_id = full_scope.hubspot_id
    WHERE utm_log.utm_campaign IS NOT NULL
        AND NOT (utm_log.country = 'Brazil' AND (LOWER(utm_log.utm_campaign) LIKE '%\\_wa\\_%'
            OR LOWER(utm_log.utm_campaign) LIKE '%\\_wa\\-%'))
        AND utm_log.utm_source IN ('facebook', 'fb', 'ig', 'Social_Ads', 'linkedin', 'criteo', 'google', 'adwords', 'bing', 'taboola', 'tiktok', 'softdoit', 'capterra', 'cronomia')
        AND (utm_log.utm_campaign LIKE '%\\_%\\_%\\_%' OR utm_log.utm_campaign IN ('it_gipo_mal', 'it_fac_mal'))
        AND utm_log.country IN ('Spain', 'Brazil', 'Mexico', 'Turkey', 'Turkiye', 'Poland', 'Italy', 'Germany', 'Colombia', 'Chile', 'Peru')
    GROUP BY utm_log.hubspot_id,
        full_scope.hubspot_id,
        full_scope.lead_id,
        utm_log.country,
        full_scope.contact_type,
        full_scope.frozen_contact_type,
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

utm_log_lcs_join AS (
    SELECT
        lcs.hubspot_id,
        lcs.lead_id,
        lcs.is_mkt_push_lead,
        lcs.contact_type,
        lcs.frozen_contact_type,
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
        CASE WHEN DATEPART(hour, lcs.lcs_date) = 0
            AND DATEPART(minute, lcs.lcs_date) = 0
            AND DATEPART(second, lcs.lcs_date) = 0 THEN DATEADD(second, -1, DATEADD(day, 0, lcs.lcs_date))
            ELSE lcs.lcs_date
        END AS new_lcs_date,
        lcs.lcs_date,
        utm.prev_campaign_name,
        utm.interaction
    FROM campaign_interaction_scope utm
    INNER JOIN all_valid_mals_mqls lcs
        ON utm.hubspot_id = lcs.hubspot_id
    WHERE new_lcs_date BETWEEN utm.interaction - interval '1 hour' AND utm.interaction::date + interval '31 day'
        AND ((utm.prev_campaign_name != utm.campaign) OR utm.prev_campaign_name IS NULL)
),

lcs_utm_order_control AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY hubspot_id, country, lifecycle_stage, new_lcs_date ORDER BY new_lcs_date DESC, interaction DESC) AS lcs_order,
        ROW_NUMBER() OVER (PARTITION BY hubspot_id, country, lifecycle_stage, interaction ORDER BY new_lcs_date ASC) AS interaction_order
    FROM utm_log_lcs_join
    WHERE TRUE
    QUALIFY lcs_order = 1 AND interaction_order = 1
),

hubspot_web_props_aux_1_filtered_end AS (
    SELECT
        *,
        LAG(lead_id, 1) OVER (PARTITION BY hubspot_id ORDER BY new_lcs_date) AS prev_lead_id,          --we use this to select the first row after a campaign name has been updated
        LAG(campaign, 1) OVER (PARTITION BY hubspot_id ORDER BY new_lcs_date) AS previous_campaign_name,--we use this to select the first row after a campaign name has been updated
        LAG(lifecycle_stage, 1) OVER (PARTITION BY hubspot_id ORDER BY new_lcs_date) AS previous_lcs
    FROM lcs_utm_order_control
    WHERE TRUE
    QUALIFY NOT (previous_campaign_name = campaign AND lead_id <> prev_lead_id) OR prev_lead_id IS NULL
),

cj_deals_full_scope AS (
    SELECT
        deal.country,
        deal.month AS deal_month,
        deal.deal_stage_start,
        deal.hs_contact_id,
        deal.lead_id,
        deal.deal_id,
        NVL(deal.deal_length, 12) AS deal_length,
        MAX(deal.mrr_euro) OVER(PARTITION BY deal.deal_id) AS mrr_euro,
        MAX(deal.mrr_original_currency) OVER(PARTITION BY deal.deal_id) AS mrr_original_currency,
        CASE WHEN deal.country = 'Brazil' AND deal.pipeline LIKE '%PMS%' THEN 'Feegow'
            WHEN deal.country = 'Italy' AND deal.pipeline LIKE '%PMS%' THEN 'Gipo'
            WHEN deal.country = 'Spain' AND deal.pipeline LIKE '%Enterprise SaaS4Clinics / Marketplace%' THEN 'Clinic Cloud'
            WHEN deal.country = 'Poland' AND deal.pipeline LIKE '%PMS%' THEN 'MyDr'
            WHEN deal.pipeline_type = 'Individual New Sales' THEN 'Agenda Premium'
            ELSE 'Clinic Agenda'
        END AS product_won
    FROM mart_cust_journey.cj_deal_month deal
    INNER JOIN all_valid_mals_mqls_list scope -- we select just the ones that arrive to won stage after entering the correct way to the funnel, in this case MAL and MQL
        ON deal.hs_contact_id = scope.hubspot_id
    WHERE deal.deal_stage IN ('Closed Won') AND deal.pipeline_type IN ('Individual New Sales', 'Clinics New Sales')
        AND deal.is_current_stage
        AND deal.stage_is_month_new
    GROUP BY deal.month,
        deal.hs_contact_id,
        deal.lead_id,
        deal.deal_id,
        deal.deal_length,
        deal.country,
        deal.mrr_euro,
        deal.mrr_original_currency,
        deal.deal_stage_start,
        deal.pipeline,
        deal.pipeline_type
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
        DATE_TRUNC('day', main.new_lcs_date) AS date,
        main.hubspot_id,
        main.country,
        main.verified,
        main.spec_split,
        main.campaign,
        main.hsa_net,
        main.content,
        deal.deal_month,
        deal.product_won,
        COALESCE(main.contact_type, 'UNKNOWN') AS contact_type,
        COALESCE(main.frozen_contact_type, 'UNKNOWN') AS frozen_contact_type,
        COUNT(CASE WHEN main.lifecycle_stage = 'MAL' THEN main.lead_id END) AS mal,
        COUNT(CASE WHEN main.lifecycle_stage = 'MQL' THEN main.lead_id END) AS mql
    FROM hubspot_web_props_aux_1_filtered_end main
    LEFT JOIN cj_deals_deduped deal
        ON deal.hs_contact_id = main.hubspot_id
        AND deal.deal_stage_start >= DATE_TRUNC('day',main.new_lcs_date) --noqa
    GROUP BY date,
        main.hubspot_id,
        main.country,
        main.verified,
        main.spec_split,
        main.campaign,
        main.hsa_net,
        main.content,
        deal.deal_month,
        deal.product_won,
        main.contact_type,
        main.frozen_contact_type,
        main.lifecycle_stage
)

SELECT
    date::date,
    country,
    hubspot_id,
    verified,
    campaign,
    hsa_net,
    content,
    deal_month::date,
    product_won,
    UPPER(LEFT(campaign, 2)) AS country_by_campaign,
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
            ELSE SPLIT_PART(campaign, '_', 2)
        END
        ELSE SPLIT_PART(campaign, '_', 2)
    END AS campaign_target,
    CASE WHEN LOWER(campaign) LIKE '%feegow%'
            OR LOWER(campaign) LIKE '%-fg-%' THEN 'Feegow'
        WHEN LOWER(campaign) LIKE '%clinic-cloud%'
            OR LOWER(campaign) LIKE '%-cc-%' THEN 'Clinic Cloud'
        WHEN LOWER(campaign) LIKE '%gipo%' THEN 'GIPO'
        WHEN LOWER(campaign) LIKE '%mydr%' THEN 'MyDr'
        ELSE 'Agenda'
    END AS campaign_product,
    CASE WHEN date >= '2024-01-01' AND contact_type = 'MARKETING'
            AND NOT (campaign LIKE '%mydr%') THEN 'OTHER' --as per Jonathan´s request all Marketing segment contacts not to be counted for any campaigns except PMS ones from 2024 --only valid for Poland based on feedback in May 24
        WHEN country = 'Poland' AND campaign LIKE '%mydr%' THEN
            CASE WHEN contact_type IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                ELSE 'OTHER'
            END
        WHEN country = 'Poland' THEN
            CASE WHEN contact_type IN ('DOCTOR', 'HEALTH - GENERAL') THEN 'DOCTOR'
                WHEN contact_type IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                ELSE 'OTHER'
            END
        WHEN country IN ('Colombia', 'Peru') THEN
            CASE WHEN contact_type IN ('SECRETARY', 'DOCTOR') THEN 'DOCTOR'
                WHEN contact_type IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                ELSE 'OTHER'
            END
        WHEN country = 'Brazil' AND (campaign LIKE '%feegow%' OR LOWER(campaign) LIKE '%-fg-%') THEN
            CASE WHEN contact_type IN ('PATIENT', 'UNKNOWN', 'STUDENT') THEN 'OTHER'
                ELSE 'FACILITY'
            END
        WHEN country = 'Brazil' THEN
            CASE WHEN contact_type IN ('SECRETARY', 'DOCTOR') THEN 'DOCTOR'
                WHEN contact_type IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                ELSE 'OTHER'
            END
        WHEN country = 'Italy' AND campaign LIKE '%gipo%' THEN
            CASE WHEN contact_type IN ('PATIENT', 'UNKNOWN', 'STUDENT') THEN 'OTHER'
                ELSE 'FACILITY'
            END
        WHEN country = 'Italy' THEN
            CASE WHEN contact_type IN ('SECRETARY', 'DOCTOR') THEN 'DOCTOR'
                WHEN contact_type IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                WHEN contact_type IN ('GENERAL PRACTITIONER', 'GENERAL PRACTITIONER & DOCTOR') THEN 'GP'
                ELSE 'OTHER'
            END
        WHEN country = 'Spain' AND (LOWER(campaign) LIKE '%clinic-cloud%' OR LOWER(campaign) LIKE '%-cc-%') THEN
            CASE WHEN contact_type IN ('PATIENT', 'UNKNOWN', 'STUDENT') THEN 'OTHER'
                ELSE 'FACILITY'
            END
        WHEN country = 'Spain' THEN
            CASE WHEN contact_type IN ('SECRETARY', 'DOCTOR') THEN 'DOCTOR'
                WHEN contact_type IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                ELSE 'OTHER'
            END
        WHEN country = 'Mexico' THEN
            CASE WHEN contact_type IN ('SECRETARY', 'DOCTOR') THEN 'DOCTOR'
                WHEN contact_type IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                ELSE 'OTHER'
            END
        WHEN country = 'Turkiye' THEN
            CASE WHEN contact_type IN ('SECRETARY', 'DOCTOR') THEN 'DOCTOR'
                WHEN contact_type IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                ELSE 'OTHER'
            END
        WHEN country = 'Germany' THEN
            CASE WHEN contact_type IN ('PATIENT', 'UNKNOWN', 'MARKETING') THEN 'OTHER'
                ELSE 'DOCTOR' END
        WHEN country = 'Chile' THEN
            CASE WHEN contact_type IN ('DOCTOR', 'SECRETARY', 'DOCTOR&FACILITY - 2IN1') THEN 'DOCTOR'
                ELSE 'OTHER'
            END
    END AS target,
    CASE WHEN date >= '2024-01-01' AND frozen_contact_type = 'MARKETING'
            AND NOT (campaign LIKE '%mydr%') THEN 'OTHER' --as per Jonathan´s request all Marketing segment contacts not to be counted for any campaigns except PMS ones from 2024 --only valid for Poland based on feedback in May 24
        WHEN country = 'Poland' AND campaign LIKE '%mydr%' THEN
            CASE WHEN frozen_contact_type IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                ELSE 'OTHER'
            END
        WHEN country = 'Poland' THEN
            CASE WHEN frozen_contact_type IN ('DOCTOR', 'HEALTH - GENERAL') THEN 'DOCTOR'
                WHEN frozen_contact_type IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                ELSE 'OTHER'
            END
        WHEN country IN ('Colombia', 'Peru') THEN
            CASE WHEN frozen_contact_type IN ('SECRETARY', 'DOCTOR') THEN 'DOCTOR'
                WHEN frozen_contact_type IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                ELSE 'OTHER'
            END
        WHEN country = 'Brazil' AND (campaign LIKE '%feegow%' OR LOWER(campaign) LIKE '%-fg-%') THEN
            CASE WHEN frozen_contact_type IN ('PATIENT', 'UNKNOWN', 'STUDENT') THEN 'OTHER'
                ELSE 'FACILITY'
            END
        WHEN country = 'Brazil' THEN
            CASE WHEN frozen_contact_type IN ('SECRETARY', 'DOCTOR') THEN 'DOCTOR'
                WHEN frozen_contact_type IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                ELSE 'OTHER'
            END
        WHEN country = 'Italy' AND campaign LIKE '%gipo%' THEN
            CASE WHEN frozen_contact_type IN ('PATIENT', 'UNKNOWN', 'STUDENT') THEN 'OTHER'
                ELSE 'FACILITY'
            END
        WHEN country = 'Italy' THEN
            CASE WHEN frozen_contact_type IN ('SECRETARY', 'DOCTOR') THEN 'DOCTOR'
                WHEN frozen_contact_type IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                WHEN frozen_contact_type IN ('GENERAL PRACTITIONER', 'GENERAL PRACTITIONER & DOCTOR') THEN 'GP'
                ELSE 'OTHER'
            END
        WHEN country = 'Spain' AND (LOWER(campaign) LIKE '%clinic-cloud%' OR LOWER(campaign) LIKE '%-cc-%') THEN
            CASE WHEN frozen_contact_type IN ('PATIENT', 'UNKNOWN', 'STUDENT') THEN 'OTHER'
                ELSE 'FACILITY'
            END
        WHEN country = 'Spain' THEN
            CASE WHEN frozen_contact_type IN ('SECRETARY', 'DOCTOR') THEN 'DOCTOR'
                WHEN frozen_contact_type IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                ELSE 'OTHER'
            END
        WHEN country = 'Mexico' THEN
            CASE WHEN frozen_contact_type IN ('SECRETARY', 'DOCTOR') THEN 'DOCTOR'
                WHEN frozen_contact_type IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                ELSE 'OTHER'
            END
        WHEN country = 'Turkiye' THEN
            CASE WHEN frozen_contact_type IN ('SECRETARY', 'DOCTOR') THEN 'DOCTOR'
                WHEN frozen_contact_type IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                ELSE 'OTHER'
            END
        WHEN country = 'Germany' THEN
            CASE WHEN frozen_contact_type IN ('PATIENT', 'UNKNOWN', 'MARKETING') THEN 'OTHER'
                ELSE 'DOCTOR' END
        WHEN country = 'Chile' THEN
            CASE WHEN frozen_contact_type IN ('DOCTOR', 'SECRETARY', 'DOCTOR&FACILITY - 2IN1') THEN 'DOCTOR'
                ELSE 'OTHER'
            END
    END AS target_test,
    CASE WHEN country IN ('Brazil', 'Mexico', 'Chile') AND spec_split IS NULL THEN 'Paramedical'
        WHEN country IN ('Brazil', 'Mexico', 'Chile') AND spec_split IS NOT NULL THEN spec_split
    END AS br_mx_cl_specialisation,
    contact_type,
    frozen_contact_type,
    SUM(mal) AS mals,
    SUM(mql) AS mqls
FROM next
GROUP BY date,
    country,
    hubspot_id,
    verified,
    campaign,
    hsa_net,
    content,
    deal_month,
    product_won,
    country_by_campaign,
    campaign_goal,
    campaign_target,
    campaign_product,
    target,
    br_mx_cl_specialisation,
    contact_type,
    frozen_contact_type
