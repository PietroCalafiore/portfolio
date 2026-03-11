WITH no_meet_contacts AS (
    SELECT
        live.hubspot_id,
        DATE(live.entered_marketing_database_at_test) AS enter_db,
        live.country,
        live.contact_type_segment_test AS segment,
        live.specialisation_type_medical_paramedical_batch AS spec_type
    FROM dw.hs_contact_live live
    LEFT JOIN dw.hs_engagement_live eng
        ON live.hubspot_id = eng.hs_contact_id
            AND eng.type = 'MEETING'
            AND eng.created_at >= '2025-01-01'
            AND eng.activity_type = 'Docplanner Meet'
            AND eng.status IS NOT NULL
            AND eng.is_deleted != 'Yes'
    WHERE live.is_deleted != 'Yes'
        AND live.country IN
        ('Brazil', 'Spain', 'Italy', 'Poland', 'Turkiye', 'Chile', 'Colombia', 'Mexico', 'Germany',
         'Peru')
        AND (live.contact_type_segment_test IN ('DOCTOR', 'GENERAL PRACTITIONER', 'GENERAL PRACTITIONER & DOCTOR')
            OR live.contact_type_segment_test IS NULL)
        AND eng.hs_contact_id IS NULL

),

     -- MQL

mqls AS (SELECT DISTINCT
    mcm.contact_id::VARCHAR,
    DATE(mcm.month) AS mql_date,
    'Agenda Premium' AS mql_product,
    mcm.last_source_so AS last_source,
    mcm.mql_last_touch_channel AS channel,
    mcm.active_passive
    FROM no_meet_contacts no_m
    LEFT JOIN mart_cust_journey.cj_mqls_monthly mcm
        ON no_m.hubspot_id = mcm.contact_id
    WHERE mcm.lifecycle_stage != 'only_won'

    UNION ALL

    SELECT DISTINCT
        noa.hubspot_id::VARCHAR AS contact_id,
        DATE(DATE_TRUNC('month', COALESCE(noa.noa_mql_at, noa.noa_pql_at))) AS mql_date,
        'NOA' AS mq_product,
        noa.hs_noa_last_source AS last_source,
        noa.mql_last_touch_channel AS channel,

        'Active' AS active_passive
    FROM no_meet_contacts no_m
    LEFT JOIN mart_cust_journey.noa_marketing_kpis_cm_lg_combined noa
        ON no_m.hubspot_id = noa.hubspot_id
),

     -- Deals Opened


deals_opened AS (SELECT DISTINCT
    no_m.hubspot_id AS contact_id,
    open_d.deal_id AS open_deal_id,
    DATE(open_d.open_deal_month) AS open_deal_month

    FROM no_meet_contacts no_m
    LEFT JOIN mqls
        ON no_m.hubspot_id = mqls.contact_id
    LEFT JOIN mart_cust_journey.inbound_all_open_deals open_d
        ON no_m.hubspot_id = open_d.contact_id
    WHERE open_deal_month BETWEEN DATE_TRUNC('month', mqls.mql_date)
        AND DATEADD(MONTH, 1, DATE_TRUNC('month', mqls.mql_date))


    UNION ALL

    SELECT DISTINCT
        no_m.hubspot_id AS contact_id,
        noa.open_deal_id,
        DATE(COALESCE(noa.mql_deal_at, noa.pql_deal_at)) AS open_deal_month
    FROM no_meet_contacts no_m
    LEFT JOIN mqls
              ON no_m.hubspot_id = mqls.contact_id
    LEFT JOIN mart_cust_journey.noa_marketing_kpis_cm_lg_combined noa
              ON no_m.hubspot_id = noa.hubspot_id
    WHERE open_deal_month BETWEEN DATE_TRUNC('month', mqls.mql_date)
        AND DATEADD(MONTH, 1, DATE_TRUNC('month', mqls.mql_date))
),


     -- Deals Won

deals_won AS (SELECT DISTINCT
    no_m.hubspot_id AS contact_id,
    facts.owner_name AS sales,
    facts.team_name AS sales_team,
    facts.opportunity_id AS deal_won_id,
    DATE(facts.month_won) AS deal_won_date,
    facts.pipeline,
    facts.product,
    facts.mrr_euro AS mrr
    FROM no_meet_contacts no_m
    LEFT JOIN mqls
        ON no_m.hubspot_id = mqls.contact_id
    LEFT JOIN mart_cust_journey.cj_opportunity_facts facts
        ON no_m.hubspot_id = facts.hs_contact_id
    WHERE facts.current_stage = 'Closed Won'
        AND facts.channel_source = 'Inbound'
        AND deal_won_date BETWEEN DATE_TRUNC('month', mqls.mql_date)
        AND DATEADD(MONTH, 1, DATE_TRUNC('month', mqls.mql_date))

    UNION ALL

    SELECT DISTINCT
        no_m.hubspot_id AS contact_id,
        own.first_name || ' ' || own.last_name AS sales,
        team.name AS sales_team,
        noa.deal_id AS deal_won_id,
        DATE(noa.deal_month) AS deal_won_date,
        'NOA' AS pipeline,
        noa.product,
        noa.mrr_euro AS mrr

    FROM no_meet_contacts no_m
    LEFT JOIN mqls
        ON no_m.hubspot_id = mqls.contact_id
    LEFT JOIN mart_cust_journey.noa_marketing_kpis_cm_lg_combined noa
        ON no_m.hubspot_id = noa.hubspot_id
    LEFT JOIN dw.hs_deal_live deal
        ON noa.open_deal_id = deal.hubspot_id
    LEFT JOIN dw.hs_owner own
        ON deal.hubspot_owner_id = own.hubspot_owner_id
    INNER JOIN dw.hs_team team
        ON deal.hubspot_team_id = team.hubspot_team_id
    WHERE deal_won_date BETWEEN DATE_TRUNC('month', mqls.mql_date)
        AND DATEADD(MONTH, 1, DATE_TRUNC('month', mqls.mql_date))
)


SELECT
    COUNT(DISTINCT no_m.hubspot_id) AS contacts,
    no_m.country,
    no_m.enter_db,
    no_m.segment,
    no_m.spec_type,
    mqls.mql_date,
    mqls.channel,
    CASE WHEN LOWER(mqls.mql_product) LIKE '%noa%' THEN 'NOA' ELSE mqls.mql_product END AS mql_product,
    mqls.last_source,
    CASE WHEN LOWER(mqls.active_passive) LIKE '%active%' THEN 'active_source' ELSE mqls.active_passive END AS active_passive,
    deals_o.open_deal_id,
    deals_w.sales,
    deals_w.sales_team,
    deals_w.deal_won_id,
    deals_w.deal_won_date,
    deals_w.pipeline,
    deals_w.product,
    deals_w.mrr
FROM no_meet_contacts no_m
LEFT JOIN mqls
          ON no_m.hubspot_id = mqls.contact_id
LEFT JOIN deals_opened deals_o
          ON no_m.hubspot_id = deals_o.contact_id
LEFT JOIN deals_won deals_w
          ON no_m.hubspot_id = deals_w.contact_id
WHERE mqls.mql_date >= '2025-06-01'
GROUP BY 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18
