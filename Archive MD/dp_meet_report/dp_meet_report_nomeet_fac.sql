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
        ('Brazil', 'Spain', 'Italy', 'Poland', 'Turkiye', 'Chile', 'Colombia', 'Mexico',
         'Germany', 'Peru')
        AND live.contact_type_segment_test IN
        ('FACILITY', 'DOCTOR&FACILITY - 2IN1', 'HEALTH - GENERAL')
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
    LEFT JOIN mart_cust_journey.cj_mqls_monthly_clinics mcm
        ON no_m.hubspot_id = mcm.contact_id


    UNION ALL

    SELECT DISTINCT
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

        'active_source' AS active_passive
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

     -- Demo done

agenda_demo_done_deals AS (SELECT DISTINCT
    no_m.hubspot_id,
    facts.opportunity_id AS demo_deal_id,
    DATE(facts.demo_done_date) AS demo_date

    FROM no_meet_contacts no_m
    LEFT JOIN mqls
              ON no_m.hubspot_id = mqls.contact_id
    LEFT JOIN mart_cust_journey.cj_opportunity_facts facts
              ON no_m.hubspot_id = facts.hs_contact_id
    WHERE LOWER(facts.tag_so) LIKE '%inbound%'
                AND facts.demo_done_date IS NOT NULL
                AND facts.createdate BETWEEN DATE_TRUNC('month', mqls.mql_date)
        AND DATEADD(MONTH, 1, DATE_TRUNC('month', mqls.mql_date))
),

noa_demo_done_deals_raw AS (SELECT
    no_m.hubspot_id,
    cj.deal_id AS demo_deal_id,
    hsd.noa_notes_budget_category__wf AS budget_category,
    MAX(1) AS is_created,
    MAX(CASE
        WHEN cj.deal_stage = 'Demo / Sales Meeting Done'
            OR hsd.demo_watched_at IS NOT NULL OR sf.date_demo_done__c IS NOT NULL
            THEN 1
        ELSE 0 END) AS is_demo_done,
    MAX(CASE
        WHEN cj.deal_stage = 'Demo / Sales Meeting Done'
            THEN cj.deal_stage_start
        ELSE COALESCE(hsd.demo_watched_at, sf.date_demo_done__c) END) AS is_demo_done_date
    FROM no_meet_contacts no_m
    LEFT JOIN mart_cust_journey.cj_deal_month cj
        ON no_m.hubspot_id = cj.hs_contact_id
    LEFT JOIN dw.hs_deal_live hsd
        ON cj.deal_id = hsd.hubspot_id
            AND cj.crm_source = 'hubspot'
    LEFT JOIN dp_salesforce.opportunity sf
        ON cj.deal_id = sf.id
            AND cj.crm_source = 'salesforce'
    WHERE cj.pipeline IN ('Noa', 'Noa Notes')
        AND (cj.closed_lost_reason NOT IN ('Invalid') OR cj.closed_lost_reason IS NULL)
        AND (LOWER(hsd."tag") LIKE '%inb%' OR (sf.last_source__c LIKE '%Noa%'
                AND NOT sf.last_source__c LIKE '%Outbound%')) --only inbound deals
        AND (hsd.noa_notes_trial_yes_no != 'Yes' OR hsd.noa_notes_trial_yes_no IS NULL
            OR (cj.crm_source = 'salesforce'
                AND (sf.negotiation_type__c = 'Trial' OR sf.negotiation_type__c IS NULL)))     --excluding trial deals
        AND (budget_category NOT IN
            ('DOC - Price Upgrade', 'FAC - Price Upgrade', 'FAC - Profiles num upgrade',
                'DOC - Doc Agenda churn Promo')
            OR budget_category IS NULL)                                                    --budget categories excluded as not new sales (ok Walter)
    GROUP BY 1, 2, 3
),

noa_demo_done_deals AS (SELECT
    no_m.hubspot_id,
    noa_raw.demo_deal_id,
    DATE_TRUNC('month', noa_raw.is_demo_done_date)::DATE AS demo_date
    FROM no_meet_contacts no_m
    LEFT JOIN mqls
        ON no_m.hubspot_id = mqls.contact_id
    LEFT JOIN noa_demo_done_deals_raw noa_raw
        ON no_m.hubspot_id = noa_raw.hubspot_id
    WHERE noa_raw.is_demo_done = 1
        AND noa_raw.is_demo_done_date BETWEEN DATE_TRUNC('month', mqls.mql_date)
        AND DATEADD(MONTH, 1, DATE_TRUNC('month', mqls.mql_date))
),

demo_done_deals AS (SELECT
    hubspot_id,
    demo_deal_id,
    demo_date
    FROM agenda_demo_done_deals

    UNION ALL

    SELECT

        hubspot_id::BIGINT,
        demo_deal_id,
        demo_date
    FROM noa_demo_done_deals
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
    CASE WHEN LOWER(mqls.mql_product) LIKE '%noa%' THEN 'NOA' ELSE mqls.mql_product END AS mql_product_final,
    mqls.last_source,
    CASE WHEN LOWER(mqls.active_passive) LIKE '%active%' THEN 'active_source' ELSE mqls.active_passive END AS active_passive_final,
    deals_o.open_deal_id,
    ddd.demo_deal_id,
    ddd.demo_date,
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
LEFT JOIN demo_done_deals ddd
    ON no_m.hubspot_id = ddd.hubspot_id
LEFT JOIN deals_won deals_w
    ON no_m.hubspot_id = deals_w.contact_id
GROUP BY 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20
