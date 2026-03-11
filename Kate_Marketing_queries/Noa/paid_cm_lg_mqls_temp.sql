WITH all_valid_noa_mqls AS (
    SELECT
        hs.hubspot_id,
        hs.noa_lead_at AS lcs_date,
        hs.verified,
        hs.lifecycle_stage,
        hs.dw_updated_at AS updated_at,
        hs.country,
        hs.contact_type_segment_test AS contact_type,
        hs.spec_split_test as specialisation,
        member_customers_list,
        ROW_NUMBER() OVER (PARTITION BY hs.hubspot_id, hs.noa_lead_at ORDER BY dw_updated_at ASC) AS row --pick earliest row with noa lead stamp
    FROM dw.hs_contact_live_history hs
    WHERE hs.country IN ('Spain', 'Brazil', 'Mexico', 'Turkey', 'Turkiye', 'Poland', 'Italy', 'Germany', 'Colombia', 'Chile', 'Peru') AND (hs.contact_type_segment_test != 'PATIENT' OR hs.contact_type_segment_test  IS NULL)
        --AND (member_customers_list != 'Yes' OR member_customers_list IS NULL) 
        AND noa_lead_at >= '2025-01-01'
        QUALIFY row=1),


campaign_interaction_scope AS (
    SELECT
        utm_log.hubspot_id,
        full_scope.contact_type,
        full_scope.verified,
        full_scope.specialisation,
        utm_log.country,
        full_scope.member_customers_list,
        utm_log.utm_campaign AS campaign,
        LAG(utm_log.utm_campaign) OVER (PARTITION BY utm_log.hubspot_id ORDER BY utm_log.updated_at) AS prev_campaign_name,--we use this to select the first row after a campaign name has been updated
        -- MIN(utm_log.updated_at) OVER (PARTITION BY utm_log.hubspot_id, utm_log.utm_campaign) AS first_campaign_interaction,
        utm_log.updated_at AS interaction,
        ROW_NUMBER() OVER (PARTITION BY utm_log.hubspot_id, utm_log.utm_campaign ORDER BY utm_log.updated_at DESC) AS rn,
        COALESCE(utm_log.utm_source, 'No info') AS source,
        COALESCE(utm_log.utm_medium, 'No info') AS medium,
        COALESCE(utm_log.utm_term, 'No info') AS keyword,
        COALESCE(utm_log.utm_content, 'No info') AS content,
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
            WHEN utm_log.utm_source = 'google' OR utm_log.utm_source = 'adwords' THEN 'google'
        END AS hsa_net
    FROM all_valid_noa_mqls full_scope
    INNER JOIN cj_data_layer.cj_sat_contact_hs_1h_log_merged utm_log
        ON utm_log.hubspot_id = full_scope.hubspot_id
    WHERE utm_log.utm_campaign IS NOT NULL AND NOT (utm_log.country = 'Brazil' AND (LOWER(utm_log.utm_campaign) LIKE '%\\_wa\\_%' OR LOWER(utm_log.utm_campaign) LIKE '%\\_wa\\-%'))
        AND utm_log.utm_source IN ('facebook', 'fb', 'ig', 'Social_Ads', 'spotify', 'linkedin', 'criteo', 'google', 'adwords', 'bing', 'taboola', 'tiktok', 'softdoit', 'cronomia', 'capterra')
        AND (utm_log.utm_campaign LIKE '%\\_%\\_%\\_%' OR utm_log.utm_campaign IN ('it_gipo_mal', 'it_fac_mal'))
        AND utm_log.country IN ('Spain', 'Brazil', 'Mexico', 'Turkey', 'Turkiye', 'Poland', 'Italy', 'Germany', 'Colombia', 'Chile', 'Peru')
    GROUP BY
        utm_log.hubspot_id,
        full_scope.hubspot_id,
        utm_log.country,
        full_scope.contact_type,
        utm_log.utm_campaign,
        full_scope.lifecycle_stage,
        utm_log.utm_source,
        utm_log.utm_medium,
        utm_log.utm_term,
        utm_log.utm_content,
        utm_log.updated_at,
        full_scope.verified,
        full_scope.specialisation,
        full_scope.member_customers_list
),
    utm_log_lcs_join AS (
    SELECT
        lcs.hubspot_id,
        lcs.contact_type,
        lcs.verified,
        lcs.country,
        utm.campaign,
        utm.source,
        utm.medium,
        utm.keyword,
        utm.content,
        utm.hsa_net,
        lcs.lifecycle_stage,
        lcs.lcs_date,
        lcs.member_customers_list,
        utm.prev_campaign_name,
        utm.interaction
    FROM campaign_interaction_scope utm
    INNER JOIN all_valid_noa_mqls lcs ON utm.hubspot_id = lcs.hubspot_id
    WHERE (lcs_date BETWEEN utm.interaction - INTERVAL '1 hour' AND utm.interaction::DATE + INTERVAL '2 day') OR DATE_TRUNC('day', lcs_date) =  DATE_TRUNC('day', interaction)  --fix to make sure any delays in the campaign log dont affect the numbers
        AND ((utm.prev_campaign_name != utm.campaign) OR utm.prev_campaign_name IS NULL)
)


    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY hubspot_id, country, lcs_date
            ORDER BY lcs_date DESC, interaction DESC) AS lcs_order,
        ROW_NUMBER() OVER (PARTITION BY hubspot_id, country, interaction
            ORDER BY lcs_date ASC) AS interaction_order
    FROM utm_log_lcs_join
    WHERE TRUE AND COALESCE(contact_type, 'UNKNOWN') IN ('DOCTOR', 'SECRETARY', 'DOCTOR&FACILITY - 2IN1', 'GENERAL PRACTITIONER & DOCTOR', 'GENERAL PRACTITIONER', 'UNKNOWN')
        QUALIFY lcs_order = 1 and interaction_order = 1
