WITH core_meet_data_raw AS (
    SELECT DISTINCT
        eng.hubspot_id AS meeting_id,
        eng.hs_contact_id,
        DATE(live.entered_marketing_database_at_test) AS enter_db,
        live.country,
        live.contact_type_segment_test AS segment,
        live.specialisation_type_medical_paramedical_batch AS spec_type,
        eng.status AS meeting_status,
        DATE(eng.created_at) AS scheduling_date,
        DATE(eng.meeting_end_time) AS meeting_date,
        DATE(eng.occurred_at) AS scheduled_date,
        eng.hubspot_owner_id,
        eng.hubspot_team_id
    FROM dw.hs_engagement_live eng
    INNER JOIN dw.hs_contact_live live
        ON eng.hs_contact_id = live.hubspot_id
    WHERE eng.type = 'MEETING'
        AND eng.created_at >= '2025-01-01'
        AND eng.activity_type = 'Docplanner Meet'
        AND eng.status IS NOT NULL
        AND eng.is_deleted != 'Yes'
        AND live.is_deleted != 'Yes'
        AND live.country IN ('Brazil', 'Spain', 'Italy', 'Poland', 'Turkiye', 'Chile', 'Colombia', 'Mexico', 'Germany', 'Peru')
        AND (
            live.contact_type_segment_test IN
            ('DOCTOR', 'GENERAL PRACTITIONER', 'GENERAL PRACTITIONER & DOCTOR')
            OR live.contact_type_segment_test IS NULL
        )
),

core_meet_data AS (SELECT *
    FROM (SELECT
        *,
        LAG(scheduling_date)
        OVER (PARTITION BY core.hs_contact_id ORDER BY scheduling_date) AS prev_date
        FROM core_meet_data_raw core) filtered
    WHERE prev_date IS NULL
        OR DATEDIFF(DAY, prev_date, scheduling_date) >= 7
),


called_no_show AS (SELECT
    core.hs_contact_id,
    eng.hubspot_id AS succ_call_id,
    DATE(eng.occurred_at) AS call_date
    FROM core_meet_data_raw core
    LEFT JOIN dw.hs_engagement_live eng
        ON core.hs_contact_id = eng.hs_contact_id
    WHERE core.meeting_status = 'NO_SHOW'
        AND eng.type = 'CALL'
        AND eng.status = 'COMPLETED'
        AND eng.call_outcome = 'Connected'
        AND eng.occurred_at BETWEEN core.scheduling_date AND core.scheduled_date
),

final_db AS (SELECT DISTINCT
    core.hs_contact_id,
    core.enter_db,
    core.country,
    core.segment,
    core.spec_type,
    core.meeting_id,
    core.meeting_status,
    core.scheduling_date,
    core.meeting_date,
    core.scheduled_date,
    core.hubspot_owner_id AS sales_rep_id,
    core.hubspot_team_id AS sales_team_id,
    call.succ_call_id AS call_id,
    call.call_date,
    CASE
        WHEN core.meeting_status = 'NO_SHOW' AND call.succ_call_id IS NOT NULL
            THEN 'false_no_show' END AS raw_final_status
    FROM core_meet_data core
    LEFT JOIN called_no_show call
        ON core.hs_contact_id = call.hs_contact_id
),

commercial_date AS (SELECT
    db.hs_contact_id,
    MIN(DATE(h.dw_updated_at)) AS commercial_date
    FROM final_db db
    INNER JOIN dw.hs_contact_live_history h
        ON db.hs_contact_id = h.hubspot_id
    WHERE h.member_customers_list = 'Yes'
    GROUP BY db.hs_contact_id
),

meet_lg_db AS (SELECT db.*
    FROM final_db db
    LEFT JOIN commercial_date comm_at
              ON db.hs_contact_id = comm_at.hs_contact_id
        AND (db.scheduling_date < DATEADD('day', -10, comm_at.commercial_date)
            OR comm_at.commercial_date IS NULL)
),

sales_data AS (SELECT
    eng.hubspot_owner_id,
    eng.hubspot_team_id,
    own.first_name || ' ' || own.last_name AS sales,
    team.name AS sales_team
    FROM core_meet_data_raw eng
    INNER JOIN dw.hs_owner own
        ON eng.hubspot_owner_id = own.hubspot_owner_id
    INNER JOIN dw.hs_team team
        ON eng.hubspot_team_id = team.hubspot_team_id
),

mql_data AS (SELECT DISTINCT
    lg_db.hs_contact_id::VARCHAR AS mql_id,
    DATE(mcm.lifecycle_stage_start) AS mql_date,
    CASE
        WHEN DATE(mcm.lifecycle_stage_start) BETWEEN DATEADD(DAY, -7, lg_db.scheduling_date) AND DATEADD(DAY, -1, lg_db.scheduling_date)
            THEN 'influenced_mql'
        ELSE 'mql' END AS mql_type,
    'Agenda Premium' AS mql_product,
    mcm.last_source_so AS last_source,
    mcm.mql_last_touch_channel AS channel,
    mcm.mql_conversion_place AS conversion_place,
    mcm.active_passive
    FROM meet_lg_db lg_db
    LEFT JOIN mart_cust_journey.cj_mqls_monthly mcm
        ON lg_db.hs_contact_id = mcm.contact_id
    WHERE mcm.lifecycle_stage != 'only_won'
        AND (mql_date IS NOT NULL)
        AND DATE(mcm.lifecycle_stage_start) BETWEEN DATEADD(DAY, -7, lg_db.scheduling_date) AND DATEADD(DAY, 2, lg_db.scheduling_date)


    UNION ALL

    SELECT DISTINCT
        noa.hubspot_id::VARCHAR AS mql_id,
        DATE(DATE_TRUNC('month', COALESCE(noa.mql_deal_at, noa.pql_deal_at))) AS mql_date,
        CASE
            WHEN DATE(DATE_TRUNC('month', COALESCE(noa.mql_deal_at, noa.pql_deal_at))) BETWEEN DATEADD(DAY, -7, lg_db.scheduling_date) AND DATEADD(DAY, -1, lg_db.scheduling_date)
                THEN 'influenced_mql'
            ELSE 'mql' END AS mql_type,
        'NOA' AS mql_product,
        noa.hs_noa_last_source AS last_source,
        noa.mql_last_touch_channel AS channel,
        noa.mql_last_conversion_place AS conversion_place,
        'active_source' AS active_passive

    FROM meet_lg_db lg_db
    LEFT JOIN mart_cust_journey.noa_marketing_kpis_cm_lg_combined noa
        ON lg_db.hs_contact_id = noa.hubspot_id
    WHERE DATE(DATE_TRUNC('month', COALESCE(noa.mql_deal_at, noa.pql_deal_at))) BETWEEN DATEADD(DAY, -7, lg_db.scheduling_date) AND DATEADD(DAY, 2, lg_db.scheduling_date)
),


deals_opened AS (SELECT DISTINCT
    lg_db.hs_contact_id AS contact_id,
    open_d.deal_id AS open_deal_id,
    DATE(open_d.open_deal_month) AS open_deal_month

    FROM meet_lg_db lg_db
    LEFT JOIN mart_cust_journey.inbound_all_open_deals open_d
        ON lg_db.hs_contact_id = open_d.contact_id
    WHERE open_deal_month BETWEEN DATE_TRUNC('month', lg_db.scheduling_date)
        AND DATEADD(MONTH, 1, DATE_TRUNC('month', lg_db.scheduling_date))


    UNION ALL

    SELECT DISTINCT
        lg_db.hs_contact_id AS contact_id,
        noa.open_deal_id,
        DATE(COALESCE(noa.mql_deal_at, noa.pql_deal_at)) AS open_deal_month


    FROM meet_lg_db lg_db
    LEFT JOIN mart_cust_journey.noa_marketing_kpis_cm_lg_combined noa
        ON lg_db.hs_contact_id = noa.hubspot_id
    WHERE open_deal_month BETWEEN DATE_TRUNC('month', lg_db.scheduling_date)
        AND DATEADD(MONTH, 1, DATE_TRUNC('month', lg_db.scheduling_date))
),


deal_data AS (SELECT DISTINCT
    lg_db.hs_contact_id,
    facts.opportunity_id AS deal_id,
    DATE(facts.closed_won_date) AS won_date,
    facts.pipeline,
    facts.product,
    facts.mrr_euro AS mrr

    FROM meet_lg_db lg_db
    LEFT JOIN mart_cust_journey.cj_opportunity_facts facts
        ON lg_db.hs_contact_id = facts.hs_contact_id
    WHERE LOWER(facts.tag_so) LIKE '%inbound%' AND facts.current_stage = 'Closed Won'
        AND facts.createdate BETWEEN DATEADD(DAY, -1, lg_db.scheduling_date) AND DATEADD('day', 60, lg_db.scheduling_date)


    UNION ALL


    SELECT DISTINCT
        lg_db.hs_contact_id AS contact_id,
        noa.deal_id AS deal_id,
        DATE(noa.deal_month) AS won_date,
        'NOA' AS pipeline,
        noa.product,
        noa.mrr_euro AS mrr

    FROM meet_lg_db lg_db
    LEFT JOIN mart_cust_journey.noa_marketing_kpis_cm_lg_combined noa
        ON lg_db.hs_contact_id = noa.hubspot_id
    WHERE won_date BETWEEN DATE_TRUNC('month', lg_db.scheduling_date)
        AND DATEADD(MONTH, 1, DATE_TRUNC('month', lg_db.scheduling_date))
)

SELECT DISTINCT
    meet_lg.hs_contact_id,
    meet_lg.meeting_id,
    meet_lg.enter_db,
    meet_lg.country,
    meet_lg.segment,
    meet_lg.spec_type,
    meet_lg.meeting_status,
    meet_lg.scheduling_date,
    meet_lg.meeting_date,
    meet_lg.scheduled_date,
    meet_lg.call_id,
    meet_lg.call_date,
    sales.sales,
    sales.sales_team,
    mql.mql_id,
    mql.mql_type,
    mql.mql_date,
    CASE WHEN LOWER(mql.mql_product) LIKE '%noa%' THEN 'NOA' ELSE mql.mql_product END AS mql_product,
    mql.channel,
    mql.conversion_place,
    mql.last_source,
    CASE WHEN LOWER(mql.active_passive) LIKE '%active%' THEN 'active_source' ELSE mql.active_passive END AS active_passive,
    deal_op.open_deal_id,
    deal_op.open_deal_month,
    deal.deal_id,
    deal.won_date,
    deal.pipeline,
    deal.product,
    deal.mrr,
    CASE
        WHEN meet_lg.raw_final_status = 'false_no_show' THEN 'False No Show'
        WHEN meet_lg.meeting_status = 'NO_SHOW' AND (deal.deal_id IS NOT NULL
            OR deal_op.open_deal_id IS NOT NULL) THEN 'False No Show'
        WHEN meet_lg.meeting_status = 'COMPLETED' THEN 'Completed'
        WHEN meet_lg.meeting_status = 'CANCELED' THEN 'Canceled'
        WHEN meet_lg.meeting_status = 'SCHEDULED' THEN 'Scheduled'
        WHEN meet_lg.meeting_status = 'REASSIGNED' THEN 'Reassigned'
        WHEN meet_lg.meeting_status = 'NO_SHOW' THEN 'No Show'
        ELSE 'n/a' END AS final_status
FROM meet_lg_db meet_lg
LEFT JOIN sales_data sales
          ON meet_lg.sales_rep_id = sales.hubspot_owner_id
LEFT JOIN mql_data mql
          ON meet_lg.hs_contact_id = mql.mql_id
LEFT JOIN deals_opened deal_op
          ON meet_lg.hs_contact_id = deal_op.contact_id
LEFT JOIN deal_data deal
          ON meet_lg.hs_contact_id = deal.hs_contact_id
