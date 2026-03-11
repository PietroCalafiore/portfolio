-- noqa: disable=L031
-- never erase previous comment (up)
-- PAID MEDIA PROJECT
DROP TABLE IF EXISTS mart_cust_journey.msv_paid_media_campaign_hubspot_2;

CREATE TABLE mart_cust_journey.msv_paid_media_campaign_hubspot_2 AS
WITH scope_aux_1 AS (
    SELECT
        contact.contact_id AS hubspot_id,
        contact.contact_hk,
        contact.lead_id,
        contact_facts.lead_ini,
        contact_facts.lead_end,
        contact_facts.verified,
        contact.lifecycle_stage,
        contact.updated_at,
        contact.is_mkt_push_lead,
        contact_facts.country,
        contact_facts.segment AS contact_type,
        contact_facts.product_recommended,
        contact_facts.spec_split
    FROM cj_data_layer.cj_contact_lead_main contact
    INNER JOIN mart_cust_journey.cj_contact_facts contact_facts
        ON contact_facts.contact_id = contact.contact_id
            AND contact_facts.lead_id = contact.lead_id
            AND contact_facts.country IN ('Spain', 'Brazil', 'Mexico', 'Turkey', 'Turkiye', 'Poland', 'Italy', 'Germany', 'Colombia', 'Chile')
    WHERE contact.lifecycle_stage IN ('MAL', 'MQL')
        AND (contact.is_mkt_push_lead IS NULL
            OR (contact.is_mkt_push_lead IN (0, 1)))
),

scope_aux_2 AS (
    SELECT
        hubspot_id,
        lead_id-- we need a list with just one row per contact
    FROM scope_aux_1
    GROUP BY hubspot_id,
        lead_id
),

campaign_interaction_scope AS (
    SELECT
        contact.hubspot_id,
        full_scope.lead_id,
        full_scope.lead_ini,
        full_scope.lead_end,
        full_scope.is_mkt_push_lead,
        full_scope.contact_type,
        full_scope.verified,
        full_scope.product_recommended,
        full_scope.spec_split,
        contact.country,
        contact.utm_campaign,
        COALESCE(contact.utm_campaign, 'No info') AS campaign,
        COALESCE(contact.utm_source, 'No info') AS source,
        COALESCE(contact.utm_medium, 'No info') AS medium,
        COALESCE(contact.utm_term, 'No info') AS keyword,
        COALESCE(contact.utm_content, 'No info') AS content,
        CASE
            WHEN contact.utm_source = 'facebook' OR contact.utm_source = 'Social_Ads' OR contact.utm_source = 'fb'
                OR contact.utm_source = 'ig'
                THEN 'facebook'
            WHEN contact.utm_source = 'linkedin' THEN 'linkedin'
            WHEN contact.utm_source = 'softdoit' THEN 'softdoit'
            WHEN contact.utm_source = 'criteo' THEN 'criteo'
            WHEN contact.utm_source = 'bing' THEN 'bing'
            WHEN contact.utm_source = 'taboola' THEN 'taboola'
            WHEN contact.utm_source = 'tiktok' THEN 'tiktok'
            WHEN contact.utm_source = 'google' OR contact.utm_source = 'adwords' THEN 'adwords'
        END AS hsa_net,
        MIN(contact.updated_at) OVER (PARTITION BY contact.hubspot_id, contact.utm_campaign) AS first_campaign_interaction,
        full_scope.lifecycle_stage,
        full_scope.updated_at AS lcs_date,
        CASE
            WHEN contact.country = 'Brazil' THEN 'BRL'::TEXT
            WHEN contact.country = 'Mexico' THEN 'MXN'::TEXT
            WHEN contact.country = 'Colombia' THEN 'COP'::TEXT
            WHEN contact.country = 'Chile' THEN 'CLP'::TEXT
            WHEN contact.country = 'Poland' THEN 'PLN'::TEXT
            WHEN contact.country IN ('Turkey', 'Turkiye') THEN 'TRY'::TEXT
            ELSE 'EUR'
        END AS src_currency_code,
        'EUR'::TEXT AS dst_currency_code
    FROM scope_aux_1 full_scope
    INNER JOIN cj_data_layer.cj_sat_contact_hs_1h_log_merged contact
        ON contact.hubspot_id = full_scope.hubspot_id
            -- it is compulsory for a utm to be counted that this occurs BEFORE the contact changing LCS. Plus we allow for 30 day attribution window
            AND full_scope.updated_at BETWEEN contact.updated_at AND contact.updated_at::DATE + INTERVAL '30 day'
    WHERE contact.utm_campaign IS NOT NULL
        AND contact.utm_source IN ('facebook', 'fb', 'ig', 'Social_Ads', 'linkedin', 'criteo', 'google', 'adwords', 'bing', 'taboola', 'tiktok', 'softdoit')
        AND (contact.utm_campaign LIKE '%\\_%\\_%\\_%' OR contact.utm_campaign IN ('it_gipo_mal', 'it_fac_mal'))
        AND contact.country IN ('Spain', 'Brazil', 'Mexico', 'Turkey', 'Turkiye', 'Poland', 'Italy', 'Germany', 'Colombia', 'Chile')
    GROUP BY
        contact.hubspot_id,
        full_scope.hubspot_id,
        full_scope.lead_id,
        full_scope.lead_ini,
        full_scope.lead_end,
        full_scope.updated_at,
        contact.country,
        full_scope.contact_type,
        contact.utm_campaign,
        full_scope.lifecycle_stage,
        full_scope.is_mkt_push_lead,
        contact.utm_source,
        contact.utm_medium,
        contact.utm_term,
        contact.utm_content,
        contact.updated_at,
        full_scope.verified,
        full_scope.product_recommended,
        full_scope.spec_split
),

hubspot_web_props_aux_2 AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY hubspot_id, country, lead_id, lifecycle_stage, is_mkt_push_lead
            ORDER BY first_campaign_interaction DESC) AS order_click,
        ROW_NUMBER() OVER (PARTITION BY hubspot_id, country, lead_id
            ORDER BY lcs_date ASC) AS lcs_order
    FROM campaign_interaction_scope
    WHERE
        lcs_date BETWEEN first_campaign_interaction AND first_campaign_interaction::DATE + INTERVAL '30 day'
),

hubspot_web_props_aux_1_filtered_end AS (
    SELECT *
    FROM hubspot_web_props_aux_2
    WHERE order_click = 1
),

hubspot_deal_amount_scope_aux_1 AS (
    SELECT
        scope.hubspot_id,
        dw_contact.country,
        dw_contact.source_doctor_id,
        dw_contact.source_facility_id,
        MIN(main.updated_at) AS date
    FROM dw.hs_contact_live dw_contact
    INNER JOIN scope_aux_2 scope-- we select just the ones that arrive to won stage after entering the correct way to the funnel, in this case MAL and MQL
               ON dw_contact.hubspot_id = scope.hubspot_id -- DP said hubspot id is PK but i'm not sure..maybe would be better to use country...
    INNER JOIN cj_data_layer.cj_contact_lead_main main
        ON dw_contact.hubspot_id = main.contact_id
            AND scope.lead_id = main.lead_id
            AND main.lifecycle_stage IN ('CLOSED-WON', 'ONBOARDING', 'WAITING', 'FARMING') -- won stage definition
    GROUP BY scope.hubspot_id,
        dw_contact.country,
        dw_contact.source_doctor_id,
        dw_contact.source_facility_id
),

hubspot_deal_amount_scope_aux_2 AS (
    SELECT
        customer.doctor_id,
        customer.facility_id,
        customer.country_code,
        country.name AS country,
        MIN(deal.activated_at) AS activated_at,
        ROUND(SUM(NVL(deal.installation_fee, 0)
            + ((pricing.flat_price / pri_snap.invoicing_period_length) * NVL(deal.deal_length, 12))),
            2) AS amount
    FROM stage_marketplace.crm_deal deal
    INNER JOIN dw.country country
        ON deal.country_code = country.country_code
    INNER JOIN stage_marketplace.crm_customer customer
        ON customer.id = deal.crm_customer_id
            AND customer.country_code = deal.country_code
    INNER JOIN stage_marketplace.crm_deal_assignment assig --table form product premium (it is a filter itself)
        ON deal.id = assig.crm_deal_id
            AND deal.country_code = assig.country_code
            AND assig.status = 'active' --- si no filtrem implica molts duplicats tot i que amb snapshots posiblement una mica diferents
    INNER JOIN stage_marketplace.crm_deal_pricing_snapshot pri_snap
        ON assig.crm_deal_pricing_snapshot_id = pri_snap.id
            AND assig.country_code = pri_snap.country_code
    INNER JOIN stage_marketplace.crm_deal_pricing pricing
        ON pri_snap.uuid = pricing.uuid
            AND pri_snap.country_code = pricing.country_code
            AND pricing.type != 'test'    -- !!:
    WHERE activated_at >= '2022-11-08' -- so far we only have utm properties since this day
    GROUP BY customer.doctor_id,
             customer.facility_id,
             customer.country_code,
             country.name
),

hubspot_deal_amount_scope_aux_3 AS (
    -- we used hubspot ID in scope_aux1 and official IDs in scope_aux2 so we need to match them...
    SELECT
        aux_2.doctor_id,
        aux_2.facility_id,
        aux_2.country,
        aux_2.activated_at,
        aux_2.amount,
        MAX(aux_1.hubspot_id) AS hubspot_id -- to avoid duplicates from aux_1
    FROM hubspot_deal_amount_scope_aux_2 aux_2
    LEFT JOIN dw.doctor doctor
        ON aux_2.doctor_id = doctor.source_doctor_id
            AND aux_2.country_code = doctor.country_code
            AND doctor.is_deleted IS FALSE
    LEFT JOIN dw.facility facility
        ON aux_2.facility_id = facility.source_mkpl_id
            AND aux_2.country_code = facility.country_code
            AND facility.is_deleted IS FALSE
    INNER JOIN hubspot_deal_amount_scope_aux_1 aux_1
        ON (aux_1.source_doctor_id = doctor.source_doctor_id
            OR aux_1.source_facility_id = facility.source_mkpl_id)
            AND aux_1.country = aux_2.country
    GROUP BY aux_2.doctor_id,
        aux_2.facility_id,
        aux_2.country,
        aux_2.activated_at,
        aux_2.amount
),

exchange_data AS (
    SELECT DISTINCT
        date,
        src_currency_code,
        dst_currency_code,
        rate,
        dw_md5
    FROM dw.exchange_rate
    WHERE
        DATE_TRUNC('month', date) = DATE_TRUNC('month', CURRENT_DATE)
),

final_aux AS (
    SELECT
        DATE(main.lcs_date) AS lcs_date,
        DATE(main.first_campaign_interaction) AS web_date,
        main.country,
        main.contact_type,
        main.hubspot_id,
        main.lead_id,
        main.lead_ini,
        main.lead_end,
        main.verified,
        main.product_recommended,
        main.spec_split,
        main.campaign,
        main.source,
        main.medium,
        main.keyword,
        main.content,
        main.hsa_net,
        main.dst_currency_code,
        main.src_currency_code,
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
        DATE(main.lcs_date),
        DATE(main.first_campaign_interaction),
        main.country,
        main.contact_type,
        main.hubspot_id,
        main.lead_id,
        main.lead_ini,
        main.lead_end,
        main.verified,
        main.campaign,
        main.source,
        main.medium,
        main.keyword,
        main.content,
        main.hsa_net,
        main.dst_currency_code,
        main.src_currency_code,
        main.product_recommended,
        main.spec_split
)

SELECT
    main.lcs_date AS date,
    main.web_date,
    main.country,
    main.contact_type,
    main.campaign,
    main.source,
    main.medium,
    main.keyword,
    main.content,
    main.hsa_net,
    main.verified,
    main.product_recommended,
    main.spec_split,
    contact.contact_id AS hubspot_id,
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
        CASE WHEN contact.lifecycle_stage = 'CLOSED-WON' OR contact.lifecycle_stage = 'ONBOARDING'
                OR contact.lifecycle_stage = 'WAITING' OR contact.lifecycle_stage = 'FARMING'
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
    main.src_currency_code,
    main.dst_currency_code,
    exchange_data.rate,
    NVL(deal.amount, 0) AS amount,
    ROUND(NVL(deal.amount * exchange_data.rate, 0), 2) AS amount_eur
FROM final_aux main
LEFT JOIN cj_data_layer.cj_contact_lead_main contact
    ON main.hubspot_id = contact.contact_id
        AND main.lead_id = contact.lead_id
        AND main.lcs_date <= contact.updated_at  -- noqa
--and web.country = a.country
LEFT JOIN hubspot_deal_amount_scope_aux_3 deal
    ON main.hubspot_id = deal.hubspot_id
        AND main.country = deal.country
INNER JOIN exchange_data exchange_data
    ON exchange_data.src_currency_code = main.src_currency_code
        AND exchange_data.dst_currency_code = main.dst_currency_code
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
    main.src_currency_code,
    main.dst_currency_code,
    exchange_data.rate,
    contact.contact_id,
    main.mal,
    main.mql,
    deal.amount,
    main.verified,
    main.product_recommended,
    main.spec_split
