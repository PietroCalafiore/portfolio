DROP TABLE IF EXISTS test_bi.dp_meet_export_data;
CREATE TABLE test_bi.dp_meet_export_data AS

WITH doc_core_meet_data_raw AS (SELECT DISTINCT eng.hubspot_id                                     AS meeting_id,
                                                live.city,
                                                eng.meeting_internal_notes,
                                                eng.hs_contact_id,
                                                DATE(live.entered_marketing_database_at_test)      AS enter_db,
                                                live.country,
                                                live.contact_type_segment_test                     AS segment,
                                                live.specialisation_type_medical_paramedical_batch AS spec_type,
                                                eng.status                                         AS meeting_status,
                                                DATE(eng.created_at)                               AS scheduling_date,
                                                DATE(eng.meeting_end_time)                         AS meeting_date,
                                                DATE(eng.occurred_at)                              AS scheduled_date,
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
                              AND live.country IN
                                  ('Brazil', 'Spain', 'Italy', 'Poland', 'Turkiye', 'Chile', 'Colombia', 'Mexico',
                                  'Germany', 'Peru')
                                  AND (
                                    live.contact_type_segment_test IN
                                    ('DOCTOR', 'GENERAL PRACTITIONER', 'GENERAL PRACTITIONER & DOCTOR')
                                        OR live.contact_type_segment_test IS NULL
                                    )),

     doc_core_meet_data AS (SELECT *
                            FROM (SELECT *,
                                         LAG(scheduling_date)
                                         OVER (PARTITION BY core.hs_contact_id ORDER BY scheduling_date) AS prev_date
                                  FROM doc_core_meet_data_raw core) filtered
                            WHERE prev_date IS NULL
                               OR DATEDIFF(DAY, prev_date, scheduling_date) >= 7),


     doc_call_counts AS (SELECT core.hs_contact_id,
                                core.meeting_id,
                                COUNT(DISTINCT
                                      CASE WHEN eng.call_outcome = 'Connected' THEN eng.hubspot_id END) AS succ_call,
                                COUNT(DISTINCT CASE
                                                   WHEN eng.call_outcome <> 'Connected' OR eng.call_outcome IS NULL
                                                       THEN eng.hubspot_id END)                         AS failed_call
                         FROM doc_core_meet_data core
                                  LEFT JOIN dw.hs_engagement_live eng
                                            ON core.hs_contact_id = eng.hs_contact_id
                                                AND eng.type = 'CALL'
                                                AND eng.status = 'COMPLETED'
                                                AND DATE(eng.occurred_at) BETWEEN core.scheduling_date
                                                   AND COALESCE(core.meeting_date, DATEADD(day, 7, core.scheduling_date))
                         group by 1, 2),

     doc_final_db AS (SELECT DISTINCT core.hs_contact_id,
                                      core.city,
                                      core.enter_db,
                                      core.country,
                                      core.segment,
                                      core.spec_type,
                                      core.meeting_id,
                                      core.meeting_internal_notes,
                                      core.meeting_status,
                                      core.scheduling_date,
                                      core.meeting_date,
                                      core.scheduled_date,
                                      core.hubspot_owner_id AS sales_rep_id,
                                      core.hubspot_team_id  AS sales_team_id,
                                      call.succ_call,
                                      call.failed_call
                      FROM doc_core_meet_data core
                               LEFT JOIN doc_call_counts call
                                         ON core.meeting_id = call.meeting_id),

     doc_sales_data AS (SELECT eng.hubspot_owner_id,
                               eng.hubspot_team_id,
                               own.first_name || ' ' || own.last_name AS sales,
                               team.name                              AS sales_team
                        FROM doc_core_meet_data_raw eng
                                 INNER JOIN dw.hs_owner own
                                            ON eng.hubspot_owner_id = own.hubspot_owner_id
                                 INNER JOIN dw.hs_team team
                                            ON eng.hubspot_team_id = team.hubspot_team_id),

     doc_mql_data AS (SELECT DISTINCT lg_db.hs_contact_id::VARCHAR    AS mql_id,
                                      DATE(mcm.lifecycle_stage_start) AS mql_date,
                                      CASE
                                          WHEN DATE(mcm.lifecycle_stage_start) BETWEEN DATEADD(DAY, -2, lg_db.scheduling_date) AND DATEADD(DAY, -1, lg_db.scheduling_date)
                                              THEN 'influenced_mql'
                                          ELSE 'mql' END              AS mql_type,
                                      'Agenda Premium'                AS mql_product,
                                      mcm.last_source_so              AS last_source,
                                      mcm.mql_last_touch_channel      AS channel,
                                      mcm.mql_conversion_place        AS conversion_place,
                                      mcm.active_passive
                      FROM doc_final_db lg_db
                               LEFT JOIN mart_cust_journey.cj_mqls_monthly mcm
                                         ON lg_db.hs_contact_id = mcm.contact_id
                      WHERE mcm.lifecycle_stage != 'only_won'
                        AND (mql_date IS NOT NULL)
                        AND DATE(mcm.lifecycle_stage_start) BETWEEN DATEADD(DAY, -2, lg_db.scheduling_date) AND DATEADD(DAY, 1, lg_db.scheduling_date)


                      UNION ALL

                      SELECT DISTINCT noa.hubspot_id::VARCHAR                                               AS mql_id,
                                      DATE(DATE_TRUNC('month', COALESCE(noa.mql_deal_at, noa.pql_deal_at))) AS mql_date,
                                      CASE
                                          WHEN DATE(DATE_TRUNC('month', COALESCE(noa.mql_deal_at, noa.pql_deal_at))) BETWEEN DATEADD(DAY, -2, lg_db.scheduling_date) AND DATEADD(DAY, -1, lg_db.scheduling_date)
                                              THEN 'influenced_mql'
                                          ELSE 'mql' END                                                    AS mql_type,
                                      'NOA'                                                                 AS mql_product,
                                      noa.hs_noa_last_source                                                AS last_source,
                                      noa.mql_last_touch_channel                                            AS channel,
                                      noa.mql_last_conversion_place                                         AS conversion_place,
                                      'active_source'                                                       AS active_passive

                      FROM doc_final_db lg_db
                               LEFT JOIN mart_cust_journey.noa_marketing_kpis_cm_lg_combined noa
                                         ON lg_db.hs_contact_id = noa.hubspot_id
                      WHERE DATE(DATE_TRUNC('month', COALESCE(noa.mql_deal_at, noa.pql_deal_at))) BETWEEN DATEADD(DAY, -2, lg_db.scheduling_date) AND DATEADD(DAY, 1, lg_db.scheduling_date)),


     deals_opened AS (SELECT DISTINCT lg_db.hs_contact_id          AS contact_id,
                                      open_d.deal_id               AS open_deal_id,
                                      DATE(open_d.open_deal_month) AS open_deal_month

                      FROM doc_final_db lg_db
                               LEFT JOIN mart_cust_journey.inbound_all_open_deals open_d
                                         ON lg_db.hs_contact_id = open_d.contact_id
                      WHERE open_deal_month BETWEEN DATE_TRUNC('month', lg_db.scheduling_date)
                                AND DATEADD(MONTH, 1, DATE_TRUNC('month', lg_db.scheduling_date))


                      UNION ALL

                      SELECT DISTINCT lg_db.hs_contact_id                                                   AS contact_id,
                                      case when dealstage = '949822729' then null else noa.open_deal_id end as open_deal_id,
                                      DATE(COALESCE(noa.mql_deal_at, noa.pql_deal_at))                      AS open_deal_month

                      FROM doc_final_db lg_db
                               LEFT JOIN mart_cust_journey.noa_marketing_kpis_cm_lg_combined noa
                                         ON lg_db.hs_contact_id = noa.hubspot_id
                               left join dw.hs_deal_live deal
                                         on noa.open_deal_id = deal.hubspot_id
                      WHERE open_deal_month BETWEEN DATE_TRUNC('month', lg_db.scheduling_date)
                                AND DATEADD(MONTH, 1, DATE_TRUNC('month', lg_db.scheduling_date))),


     doc_deal_data AS (SELECT DISTINCT lg_db.hs_contact_id,
                                       facts.opportunity_id        AS deal_id,
                                       DATE(facts.closed_won_date) AS won_date,
                                       facts.pipeline,
                                       facts.product,
                                       facts.mrr_euro              AS mrr

                       FROM doc_final_db lg_db
                                LEFT JOIN mart_cust_journey.cj_opportunity_facts facts
                                          ON lg_db.hs_contact_id = facts.hs_contact_id
                       WHERE LOWER(facts.tag_so) LIKE '%inbound%'
                         AND facts.current_stage = 'Closed Won'
                         AND facts.createdate BETWEEN DATEADD(DAY, -1, lg_db.scheduling_date) AND DATEADD('day', 60, lg_db.scheduling_date)


                       UNION ALL


                       SELECT DISTINCT lg_db.hs_contact_id  AS contact_id,
                                       noa.deal_id          AS deal_id,
                                       DATE(noa.deal_month) AS won_date,
                                       'NOA'                AS pipeline,
                                       noa.product,
                                       noa.mrr_euro         AS mrr

                       FROM doc_final_db lg_db
                                LEFT JOIN mart_cust_journey.noa_marketing_kpis_cm_lg_combined noa
                                          ON lg_db.hs_contact_id = noa.hubspot_id
                       WHERE won_date BETWEEN DATE_TRUNC('month', lg_db.scheduling_date)
                                 AND DATEADD(MONTH, 1, DATE_TRUNC('month', lg_db.scheduling_date))),


     fac_core_meet_data_raw as (select distinct eng.hubspot_id                                as meeting_id,
                                                live.city,
                                                eng.meeting_internal_notes,
                                                eng.hs_contact_id,
                                                date(live.entered_marketing_database_at_test) as enter_db,
                                                live.country,
                                                live.contact_type_segment_test                as segment,
                                                live.facility_size,
                                                eng.status                                    as meeting_status,
                                                date(eng.created_at)                          as scheduling_date,
                                                date(eng.meeting_end_time)                    as meeting_date,
                                                date(eng.occurred_at)                         as scheduled_date,
                                                eng.hubspot_owner_id,
                                                eng.hubspot_team_id
                                from dw.hs_engagement_live eng
                                         join dw.hs_contact_live live
                                              on eng.hs_contact_id = live.hubspot_id
                                where eng.type = 'MEETING'
                                  and eng.created_at >= '2025-01-01'
                                  and eng.activity_type = 'Docplanner Meet'
                                  and eng.status is not null
                                  and eng.is_deleted <> 'Yes'
                                  and live.is_deleted <> 'Yes'
                                  and live.country in
                                      ('Brazil', 'Spain', 'Italy', 'Poland', 'Turkiye', 'Chile', 'Colombia', 'Mexico',
                                       'Germany', 'Peru')
                                  and live.contact_type_segment_test in
                                      ('FACILITY', 'DOCTOR&FACILITY - 2IN1', 'HEALTH - GENERAL')),

     fac_core_meet_data as (select *
                            from (select *,
                                         lag(scheduling_date)
                                         over (partition by core.hs_contact_id order by scheduling_date) as prev_date
                                  from fac_core_meet_data_raw core) filtered
                            where prev_date is null
                               or datediff(day, prev_date, scheduling_date) >= 7),


     fac_call_counts AS (SELECT core.hs_contact_id,
                                core.meeting_id,
                                COUNT(DISTINCT CASE
                                                   WHEN eng.call_outcome = 'Connected' THEN eng.hubspot_id
                                    END) AS succ_call,
                                COUNT(DISTINCT CASE
                                                   WHEN eng.call_outcome <> 'Connected'
                                                       OR eng.call_outcome IS NULL
                                                       THEN eng.hubspot_id
                                    END) AS failed_call
                         FROM fac_core_meet_data core
                                  LEFT JOIN dw.hs_engagement_live eng
                                            ON core.hs_contact_id = eng.hs_contact_id
                                                AND eng.type = 'CALL'
                                                AND eng.status = 'COMPLETED'
                                                AND DATE(eng.occurred_at) BETWEEN core.scheduling_date
                                                   AND COALESCE(core.meeting_date, DATEADD(day, 7, core.scheduling_date))
                         GROUP BY 1, 2),


     fac_final_db as (select distinct core.hs_contact_id,
                                      core.city,
                                      core.enter_db,
                                      core.country,
                                      core.segment,
                                      core.facility_size,
                                      core.meeting_id,
                                      core.meeting_internal_notes,
                                      core.meeting_status,
                                      core.scheduling_date,
                                      core.meeting_date,
                                      core.scheduled_date,
                                      core.hubspot_owner_id as sales_rep_id,
                                      core.hubspot_team_id  as sales_team_id,
                                      call.succ_call,
                                      call.failed_call
                      from fac_core_meet_data core
                               left join fac_call_counts call
                                         on core.meeting_id = call.meeting_id),

     fac_sales_data as (select eng.hubspot_owner_id,
                               eng.hubspot_team_id,
                               own.first_name || ' ' || own.last_name as sales,
                               team.name                              as sales_team
                        from fac_core_meet_data_raw eng
                                 join dw.hs_owner own
                                      on eng.hubspot_owner_id = own.hubspot_owner_id
                                 join dw.hs_team team
                                      on eng.hubspot_team_id = team.hubspot_team_id),

     fac_mql_data as (select distinct lg_db.hs_contact_id::varchar    as mql_id,
                                      date(mcm.lifecycle_stage_start) as mql_date,
                                      case
                                          when date(mcm.lifecycle_stage_start) between dateadd(day, -2, scheduling_date) and dateadd(day, -1, scheduling_date)
                                              then 'influenced_mql'
                                          else 'mql' end              as mql_type,
                                      'Agenda Premium'                as mql_product,
                                      mcm.last_source_so              as last_source,
                                      mcm.mql_last_touch_channel      as channel,
                                      mcm.mql_conversion_place        as conversion_place,
                                      mcm.active_passive


                      from fac_final_db lg_db
                               left join mart_cust_journey.cj_mqls_monthly mcm
                                         on lg_db.hs_contact_id = mcm.contact_id
                      where lifecycle_stage != 'only_won'
                        and (mql_date is not null)
                        and date(lifecycle_stage_start) between dateadd(day, -2, scheduling_date) and dateadd(day, 1, scheduling_date)


                      union all

                      select distinct lg_db.hs_contact_id::varchar    as mql_id,
                                      date(mcm.lifecycle_stage_start) as mql_date,
                                      case
                                          when date(mcm.lifecycle_stage_start) between dateadd(day, -2, scheduling_date) and dateadd(day, -1, scheduling_date)
                                              then 'influenced_mql'
                                          else 'mql' end              as mql_type,
                                      'Agenda Premium'                as mql_product,
                                      mcm.last_source_so              as last_source,
                                      mcm.mql_last_touch_channel      as channel,
                                      mcm.mql_conversion_place        as conversion_place,
                                      mcm.active_passive


                      from fac_final_db lg_db
                               left join mart_cust_journey.cj_mqls_monthly_clinics mcm
                                         on lg_db.hs_contact_id = mcm.contact_id
                      where (mql_date is not null)
                        and date(mcm.lifecycle_stage_start) between dateadd(day, -2, scheduling_date) and dateadd(day, 1, scheduling_date)


                      union all

                      select distinct noa.hubspot_id::varchar                                               as mql_id,
                                      date(date_trunc('month', COALESCE(noa.mql_deal_at, noa.pql_deal_at))) as mql_date,
                                      case
                                          when date(date_trunc('month', COALESCE(noa.mql_deal_at, noa.pql_deal_at))) between dateadd(day, -2, scheduling_date) and dateadd(day, -1, scheduling_date)
                                              then 'influenced_mql'
                                          else 'mql' end                                                    as mql_type,
                                      'NOA'                                                                 as mql_product,
                                      noa.hs_noa_last_source                                                as last_source,
                                      noa.mql_last_touch_channel                                            as channel,
                                      noa.mql_last_conversion_place                                         as conversion_place,
                                      'active_source'                                                       as active_passive

                      from fac_final_db lg_db
                               left join mart_cust_journey.noa_marketing_kpis_cm_lg_combined noa
                                         on lg_db.hs_contact_id = noa.hubspot_id
                      where date(date_trunc('month', COALESCE(noa.mql_deal_at, noa.pql_deal_at))) between dateadd(day, -2, scheduling_date) and dateadd(day, 1, scheduling_date)),


     fac_deals_opened as (select distinct lg_db.hs_contact_id          as contact_id,
                                          open_d.deal_id               as open_deal_id,
                                          date(open_d.open_deal_month) as open_deal_month

                          from fac_final_db lg_db
                                   left join mart_cust_journey.inbound_all_open_deals open_d
                                             on lg_db.hs_contact_id = open_d.contact_id
                          where open_deal_month BETWEEN date_trunc('month', scheduling_date)
                                    AND dateadd(month, 1, date_trunc('month', scheduling_date))


                          union all

                          select distinct lg_db.hs_contact_id                                                   as contact_id,
                                          case when dealstage = '949822729' then null else noa.open_deal_id end as open_deal_id,
                                          date(coalesce(noa.mql_deal_at, noa.pql_deal_at))                      as open_deal_month


                          from fac_final_db lg_db
                                   left join mart_cust_journey.noa_marketing_kpis_cm_lg_combined noa
                                             on lg_db.hs_contact_id = noa.hubspot_id
                                   left join dw.hs_deal_live deal
                                             on noa.open_deal_id = deal.hubspot_id
                          WHERE open_deal_month BETWEEN date_trunc('month', scheduling_date)
                                    AND dateadd(month, 1, date_trunc('month', scheduling_date))),

     fac_agenda_demo_done_deals as (select distinct lg_db.hs_contact_id,
                                                    facts.opportunity_id       as demo_deal_id,
                                                    date(facts.demo_done_date) as demo_date

                                    from fac_final_db lg_db
                                             left join mart_cust_journey.cj_opportunity_facts facts
                                                       on lg_db.hs_contact_id = facts.hs_contact_id
                                    where lower(facts.tag_so) like '%inbound%'
                                      and facts.demo_done_date is not null
                                      and facts.createdate between dateadd(day, -1, scheduling_date) and dateadd('day', 60, scheduling_date)),

     fac_noa_demo_done_deals_raw as (SELECT lg_db.hs_contact_id,
                                            cj.deal_id                                                            as demo_deal_id,
                                            hsd.noa_notes_budget_category__wf                                     AS budget_category,
                                            MAX(1)                                                                AS is_created,
                                            MAX(CASE
                                                    WHEN cj.deal_stage = 'Demo / Sales Meeting Done' OR
                                                         hsd.demo_watched_at IS NOT NULL OR
                                                         sf.date_demo_done__c IS NOT NULL
                                                        THEN 1
                                                    ELSE 0 END)                                                   AS is_demo_done,
                                            MAX(CASE
                                                    WHEN cj.deal_stage = 'Demo / Sales Meeting Done'
                                                        THEN cj.deal_stage_start
                                                    ELSE COALESCE(hsd.demo_watched_at, sf.date_demo_done__c) END) AS is_demo_done_date
                                     FROM fac_final_db lg_db
                                              left join mart_cust_journey.cj_deal_month cj
                                                        on lg_db.hs_contact_id = cj.hs_contact_id
                                              LEFT JOIN dw.hs_deal_live hsd
                                                        ON cj.deal_id = hsd.hubspot_id
                                                            AND cj.crm_source = 'hubspot'
                                              LEFT JOIN dp_salesforce.opportunity sf
                                                        ON cj.deal_id = sf.id
                                                            AND cj.crm_source = 'salesforce'
                                     WHERE cj.pipeline IN ('Noa', 'Noa Notes')
                                       AND (cj.closed_lost_reason NOT IN ('Invalid') OR cj.closed_lost_reason IS NULL)
                                       AND (LOWER(hsd."tag") LIKE '%inb%' OR (sf.last_source__c LIKE '%Noa%' AND
                                                                              NOT sf.last_source__c LIKE '%Outbound%')) --only inbound deals
                                       AND (hsd.noa_notes_trial_yes_no != 'Yes' OR hsd.noa_notes_trial_yes_no IS NULL OR
                                            (cj.crm_source = 'salesforce' AND
                                             (sf.negotiation_type__c = 'Trial' OR sf.negotiation_type__c IS NULL)))     --excluding trial deals
                                       AND (budget_category NOT IN
                                            ('DOC - Price Upgrade', 'FAC - Price Upgrade', 'FAC - Profiles num upgrade',
                                             'DOC - Doc Agenda churn Promo') OR
                                            budget_category IS NULL)                                                    --budget categories excluded as not new sales (ok Walter)
                                     GROUP BY 1, 2, 3),
     fac_noa_demo_done_deals as (select lg_db.hs_contact_id,
                                        demo_deal_id,
                                        DATE_TRUNC('month', is_demo_done_date)::DATE AS demo_date
                                 from fac_final_db lg_db
                                          left join fac_noa_demo_done_deals_raw noa_raw
                                                    on lg_db.hs_contact_id = noa_raw.hs_contact_id
                                 where is_demo_done = 1
                                   and is_demo_done_date between dateadd(day, -1, scheduling_date) and dateadd('day', 60, scheduling_date)),

     fac_demo_done_deals as (select hs_contact_id,
                                    demo_deal_id,
                                    demo_date
                             from fac_agenda_demo_done_deals

                             union all

                             select hs_contact_id::bigint,
                                    demo_deal_id,
                                    demo_date
                             from fac_noa_demo_done_deals),


     fac_deal_data as (select distinct lg_db.hs_contact_id,
                                       facts.opportunity_id        as deal_id,
                                       date(facts.closed_won_date) as won_date,
                                       facts.pipeline,
                                       facts.product,
                                       facts.mrr_euro              as mrr

                       from fac_final_db lg_db
                                left join mart_cust_journey.cj_opportunity_facts facts
                                          on lg_db.hs_contact_id = facts.hs_contact_id
                       where lower(facts.tag_so) like '%inbound%'
                         and facts.current_stage = 'Closed Won'
                         and facts.createdate between dateadd(day, -1, scheduling_date) and dateadd('day', 60, scheduling_date)


                       union all


                       select distinct lg_db.hs_contact_id  as contact_id,
                                       noa.deal_id          as deal_id,
                                       date(noa.deal_month) as won_date,
                                       'NOA'                as pipeline,
                                       noa.product,
                                       noa.mrr_euro         as mrr

                       from fac_final_db lg_db
                                left join mart_cust_journey.noa_marketing_kpis_cm_lg_combined noa
                                          on lg_db.hs_contact_id = noa.hubspot_id
                       where won_date BETWEEN date_trunc('month', scheduling_date)
                                 AND dateadd(month, 1, date_trunc('month', scheduling_date)))

SELECT DISTINCT meet_lg.hs_contact_id,
                meet_lg.meeting_id,
                meet_lg.meeting_internal_notes,
                meet_lg.enter_db,
                meet_lg.country,
                meet_lg.city,
                meet_lg.segment,
                'Individual'                    as facility_size,
                meet_lg.spec_type,
                meet_lg.meeting_status,
                meet_lg.scheduling_date,
                meet_lg.meeting_date,
                meet_lg.scheduled_date,
                meet_lg.succ_call,
                meet_lg.failed_call,
                sales.sales,
                sales.sales_team,
                mql.mql_id,
                mql.mql_type,
                mql.mql_date,
                CASE
                    WHEN LOWER(mql.mql_product) LIKE '%noa%' THEN 'NOA'
                    ELSE mql.mql_product END    AS mql_product,
                mql.channel,
                mql.conversion_place,
                mql.last_source,
                CASE
                    WHEN LOWER(mql.active_passive) LIKE '%active%' THEN 'active_source'
                    ELSE mql.active_passive END AS active_passive,
                deal_op.open_deal_id,
                deal_op.open_deal_month,
                CAST(NULL AS VARCHAR(50))       AS demo_id,
                CAST(NULL AS DATE)              AS demo_date,
                deal.deal_id,
                deal.won_date,
                deal.pipeline,
                deal.product,
                deal.mrr,
                CASE
                    WHEN meet_lg.meeting_status = 'NO_SHOW' AND (deal.deal_id IS NOT NULL
                        OR deal_op.open_deal_id IS NOT NULL) THEN 'False No Show'
                    WHEN meet_lg.meeting_status = 'COMPLETED' THEN 'Completed'
                    WHEN meet_lg.meeting_status = 'CANCELED' THEN 'Canceled'
                    WHEN meet_lg.meeting_status = 'SCHEDULED' THEN 'Scheduled'
                    WHEN meet_lg.meeting_status = 'REASSIGNED' THEN 'Reassigned'
                    WHEN meet_lg.meeting_status = 'NO_SHOW' THEN 'No Show'
                    ELSE 'n/a' END              AS final_status
FROM doc_final_db meet_lg
         LEFT JOIN doc_sales_data sales
                   ON meet_lg.sales_rep_id = sales.hubspot_owner_id
         LEFT JOIN doc_mql_data mql
                   ON meet_lg.hs_contact_id = mql.mql_id
         LEFT JOIN deals_opened deal_op
                   ON meet_lg.hs_contact_id = deal_op.contact_id
         LEFT JOIN doc_deal_data deal
                   ON meet_lg.hs_contact_id = deal.hs_contact_id


union all


select distinct meet_lg.hs_contact_id,
                meeting_id,
                meeting_internal_notes,
                enter_db,
                meet_lg.country,
                meet_lg.city,
                segment,
                meet_lg.facility_size,
                'Facility'                                                                as spec_type,
                meeting_status,
                scheduling_date,
                meeting_date,
                scheduled_date,
                succ_call,
                failed_call,
                sales,
                sales_team,
                mql_id,
                mql_type,
                mql_date,
                case when lower(mql_product) like '%noa%' then 'NOA' else mql_product end as mql_product,
                channel,
                conversion_place,
                last_source,
                case
                    when lower(mql.active_passive) like '%active%' then 'active_source'
                    else mql.active_passive end                                           as active_passive,
                open_deal_id,
                open_deal_month,
                demo_deal_id,
                demo_date,
                deal_id,
                won_date,
                deal.pipeline,
                deal.product,
                mrr,
                case
                    when meeting_status = 'NO_SHOW' and (deal_id is not null
                        or open_deal_id is not null) then 'False No Show'
                    when meeting_status = 'COMPLETED' then 'Completed'
                    when meeting_status = 'CANCELED' then 'Canceled'
                    when meeting_status = 'SCHEDULED' then 'Scheduled'
                    when meeting_status = 'REASSIGNED' then 'Reassigned'
                    when meeting_status = 'NO_SHOW' then 'No Show'
                    else 'n/a' end                                                        as final_status
from fac_final_db meet_lg
         left join fac_sales_data sales
                   on meet_lg.sales_rep_id = sales.hubspot_owner_id
         left join fac_mql_data mql
                   on meet_lg.hs_contact_id = mql.mql_id
         left join fac_deals_opened deal_op
                   on meet_lg.hs_contact_id = deal_op.contact_id
         left join fac_demo_done_deals ddd
                   on meet_lg.hs_contact_id = ddd.hs_contact_id
         left join fac_deal_data deal
                   on meet_lg.hs_contact_id = deal.hs_contact_id