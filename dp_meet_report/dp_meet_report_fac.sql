    with core_meet_data_raw as (select distinct eng.hubspot_id                           as meeting_id,
                                            eng.hs_contact_id,
                                            date(live.entered_marketing_database_at_test) as enter_db,
                                            live.country,
                                            live.contact_type_segment_test                as segment,
                                            live.facility_size,
                                            eng.status                               as meeting_status,
                                            date(eng.created_at)                         as scheduling_date,
                                            date(eng.meeting_end_time)                   as meeting_date,
                                            date(eng.occurred_at)                        as scheduled_date,
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

     core_meet_data as (select *
                        from (select *,
                                     lag(scheduling_date)
                                     over (partition by core.hs_contact_id order by scheduling_date) as prev_date
                              from core_meet_data_raw core) filtered
                        where prev_date is null
                           or datediff(day, prev_date, scheduling_date) >= 7),


     called_no_show as (select core.hs_contact_id,
                               eng.hubspot_id       succ_call_id,
                               date(eng.occurred_at) as call_date
                        from core_meet_data_raw core
                                 left join dw.hs_engagement_live eng
                                           on core.hs_contact_id = eng.hs_contact_id
                        where core.meeting_status = 'NO_SHOW'
                          and eng.type = 'CALL'
                          and eng.status = 'COMPLETED'
                          and eng.call_outcome = 'Connected'
                          and eng.occurred_at between core.scheduling_date and core.scheduled_date),

     final_db as (select distinct core.hs_contact_id,
                                  core.enter_db,
                                  core.country,
                                  core.segment,
                                  core.facility_size,
                                  core.meeting_id,
                                  core.meeting_status,
                                  core.scheduling_date,
                                  core.meeting_date,
                                  core.scheduled_date,
                                  core.hubspot_owner_id                 as sales_rep_id,
                                  core.hubspot_team_id                  as sales_team_id,
                                  call.succ_call_id                     as call_id,
                                  call.call_date,
                                  case
                                      when meeting_status = 'NO_SHOW' and succ_call_id is not null
                                          then 'false_no_show' end as raw_final_status
                  from core_meet_data core
                           left join called_no_show call
                                     on core.hs_contact_id = call.hs_contact_id),

     commercial_date as (select db.hs_contact_id,
                                min(date(h.dw_updated_at)) as commercial_date
                         from final_db db
                                  join dw.hs_contact_live_history h
                                       on db.hs_contact_id = h.hubspot_id
                         where h.member_customers_list = 'Yes'
                         group by db.hs_contact_id),

     meet_lg_db as (select db.*
                    from final_db db
                             left join commercial_date comm_at
                                       on db.hs_contact_id = comm_at.hs_contact_id
                                           and (db.scheduling_date < dateadd('day', -10, comm_at.commercial_date)
                                               or comm_at.commercial_date is null)),

     sales_data as (select eng.hubspot_owner_id,
                           eng.hubspot_team_id,
                           own.first_name || ' ' || own.last_name as sales,
                           team.name                           as sales_team
                    from core_meet_data_raw eng
                             join dw.hs_owner own
                                  on eng.hubspot_owner_id = own.hubspot_owner_id
                             join dw.hs_team team
                                  on eng.hubspot_team_id = team.hubspot_team_id),

     mql_data as (select distinct lg_db.hs_contact_id::varchar             as mql_id,
                                  date(mcm.lifecycle_stage_start)        as mql_date,
                                  case
                                      when date(mcm.lifecycle_stage_start) between dateadd(day, -7, scheduling_date) and dateadd(day, -1, scheduling_date)
                                          then 'influenced_mql'
                                      else 'mql' end                 as mql_type,
                                  'Agenda Premium' as mql_product,
                                  mcm.last_source_so                     as last_source,
                                  mcm.mql_last_touch_channel             as channel,
                                  mcm.mql_conversion_place               as conversion_place,
                                  mcm.active_passive


                  from meet_lg_db lg_db
                           left join mart_cust_journey.cj_mqls_monthly mcm
                                     on lg_db.hs_contact_id = mcm.contact_id
                  where lifecycle_stage != 'only_won'
                    and (mql_date is not null)
                    and date(lifecycle_stage_start) between dateadd(day, -7, scheduling_date) and dateadd(day, 2, scheduling_date)


                  union all

                  select distinct lg_db.hs_contact_id::varchar             as mql_id,
                                  date(mcm.lifecycle_stage_start)        as mql_date,
                                  case
                                      when date(mcm.lifecycle_stage_start) between dateadd(day, -7, scheduling_date) and dateadd(day, -1, scheduling_date)
                                          then 'influenced_mql'
                                      else 'mql' end                 as mql_type,
                                  'Agenda Premium' as mql_product,
                                  mcm.last_source_so                     as last_source,
                                  mcm.mql_last_touch_channel             as channel,
                                  mcm.mql_conversion_place               as conversion_place,
                                  mcm.active_passive


                  from meet_lg_db lg_db
                           left join mart_cust_journey.cj_mqls_monthly_clinics mcm
                                     on lg_db.hs_contact_id = mcm.contact_id
                  where (mql_date is not null)
                    and date(mcm.lifecycle_stage_start) between dateadd(day, -7, scheduling_date) and dateadd(day, 2, scheduling_date)


                  union all

                  select distinct noa.hubspot_id::varchar                                       as mql_id,
                                  date(date_trunc('month', COALESCE(noa.mql_deal_at, noa.pql_deal_at))) as mql_date,
                                  case
                                      when date(date_trunc('month', COALESCE(noa.mql_deal_at, noa.pql_deal_at))) between dateadd(day, -7, scheduling_date) and dateadd(day, -1, scheduling_date)
                                          then 'influenced_mql'
                                      else 'mql' end                                            as mql_type,
                                  'NOA'                                                         as mql_product,
                                  noa.hs_noa_last_source                                            as last_source,
                                  noa.mql_last_touch_channel                                        as channel,
                                  noa.mql_last_conversion_place                                     as conversion_place,
                                  'active_source'                                               as active_passive

                  from meet_lg_db lg_db
                           left join mart_cust_journey.noa_marketing_kpis_cm_lg_combined noa
                                     on lg_db.hs_contact_id = noa.hubspot_id
                  where date(date_trunc('month', COALESCE(noa.mql_deal_at, noa.pql_deal_at))) between dateadd(day, -7, scheduling_date) and dateadd(day, 2, scheduling_date)),


     deals_opened as (select distinct lg_db.hs_contact_id as contact_id,
                                      open_d.deal_id       as open_deal_id,
                                      date(open_d.open_deal_month)   as open_deal_month

                      from meet_lg_db lg_db
                               left join mart_cust_journey.inbound_all_open_deals open_d
                                         on lg_db.hs_contact_id = open_d.contact_id
                      where open_deal_month BETWEEN date_trunc('month', scheduling_date)
                                AND dateadd(month, 1, date_trunc('month', scheduling_date))


                      union all

                      select distinct lg_db.hs_contact_id                      as contact_id,
                                      noa.open_deal_id,
                                      date(coalesce(noa.mql_deal_at, noa.pql_deal_at)) as open_deal_month


                      from meet_lg_db lg_db
                               left join mart_cust_journey.noa_marketing_kpis_cm_lg_combined noa
                                         on lg_db.hs_contact_id = noa.hubspot_id
                      WHERE open_deal_month BETWEEN date_trunc('month', scheduling_date)
                                AND dateadd(month, 1, date_trunc('month', scheduling_date))),

     agenda_demo_done_deals as (select distinct lg_db.hs_contact_id,
                                                facts.opportunity_id        as demo_deal_id,
                                                date(facts.demo_done_date) as demo_date

                                from meet_lg_db lg_db
                                         left join mart_cust_journey.cj_opportunity_facts facts
                                                   on lg_db.hs_contact_id = facts.hs_contact_id
                                where lower(facts.tag_so) like '%inbound%'
                                  and facts.demo_done_date is not null
                                  and facts.createdate between dateadd(day, -1, scheduling_date) and dateadd('day', 60, scheduling_date)),

     noa_demo_done_deals_raw as (SELECT lg_db.hs_contact_id,
                                        cj.deal_id as demo_deal_id,
                                        hsd.noa_notes_budget_category__wf                                     AS budget_category,
                                        MAX(1)                                                                AS is_created,
                                        MAX(CASE
                                                WHEN cj.deal_stage = 'Demo / Sales Meeting Done' OR
                                                     hsd.demo_watched_at IS NOT NULL OR sf.date_demo_done__c IS NOT NULL
                                                    THEN 1
                                                ELSE 0 END)                                                   AS is_demo_done,
                                        MAX(CASE
                                                WHEN cj.deal_stage = 'Demo / Sales Meeting Done'
                                                    THEN cj.deal_stage_start
                                                ELSE COALESCE(hsd.demo_watched_at, sf.date_demo_done__c) END) AS is_demo_done_date
                                 FROM meet_lg_db lg_db
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
     noa_demo_done_deals as (select lg_db.hs_contact_id,
                                    demo_deal_id,
                                    DATE_TRUNC('month', is_demo_done_date)::DATE AS demo_date
                             from meet_lg_db lg_db
                                 left join noa_demo_done_deals_raw noa_raw
                             on lg_db.hs_contact_id = noa_raw.hs_contact_id
                             where is_demo_done = 1
                             and is_demo_done_date between dateadd(day, -1, scheduling_date) and dateadd('day', 60, scheduling_date)
                             ),

     demo_done_deals as (select
                             hs_contact_id,
                             demo_deal_id,
                             demo_date
                         from agenda_demo_done_deals

                         union all

                         select

                             hs_contact_id::bigint,
                             demo_deal_id,
                             demo_date
                         from noa_demo_done_deals),


     deal_data as (select distinct lg_db.hs_contact_id,
                                   facts.opportunity_id        as deal_id,
                                   date(facts.closed_won_date) as won_date,
                                   facts.pipeline,
                                   facts.product,
                                   facts.mrr_euro              as mrr

                   from meet_lg_db lg_db
                            left join mart_cust_journey.cj_opportunity_facts facts
                                      on lg_db.hs_contact_id = facts.hs_contact_id
                   where lower(facts.tag_so) like '%inbound%'
                     and facts.current_stage = 'Closed Won'
                     and facts.createdate between dateadd(day, -1, scheduling_date) and dateadd('day', 60, scheduling_date)


                   union all


                   select distinct lg_db.hs_contact_id as contact_id,
                                   noa.deal_id             as deal_id,
                                   date(noa.deal_month)    as won_date,
                                   'NOA'               as pipeline,
                                   noa.product,
                                   noa.mrr_euro            as mrr

                   from meet_lg_db lg_db
                            left join mart_cust_journey.noa_marketing_kpis_cm_lg_combined noa
                                      on lg_db.hs_contact_id = noa.hubspot_id
                   where won_date BETWEEN date_trunc('month', scheduling_date)
                             AND dateadd(month, 1, date_trunc('month', scheduling_date)))

select distinct meet_lg.hs_contact_id,
                meeting_id,
                enter_db,
                meet_lg.country,
                segment,
                meet_lg.facility_size,
                meeting_status,
                scheduling_date,
                meeting_date,
                scheduled_date,
                call_id,
                call_date,
                sales,
                sales_team,
                mql_id,
                mql_type,
                mql_date,
         case when lower(mql_product) like '%noa%' then 'NOA' else mql_product end as mql_product,
                channel,
                conversion_place,
                last_source,
         case when lower(mql.active_passive) like '%active%' then 'active_source' else mql.active_passive end as active_passive,
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
                    when raw_final_status = 'false_no_show' then 'False No Show'
                    when meeting_status = 'NO_SHOW' and (deal_id is not null
                        or open_deal_id is not null) then 'False No Show'
                    when meeting_status = 'COMPLETED' then 'Completed'
                    when meeting_status = 'CANCELED' then 'Canceled'
                    when meeting_status = 'SCHEDULED' then 'Scheduled'
                    when meeting_status = 'REASSIGNED' then 'Reassigned'
                    when meeting_status = 'NO_SHOW' then 'No Show'
                    else 'n/a' end as final_status
from meet_lg_db meet_lg
         left join sales_data sales
                   on meet_lg.sales_rep_id = sales.hubspot_owner_id
         left join mql_data mql
                   on meet_lg.hs_contact_id = mql.mql_id
         left join deals_opened deal_op
                   on meet_lg.hs_contact_id = deal_op.contact_id
         left join demo_done_deals ddd
                   on meet_lg.hs_contact_id = ddd.hs_contact_id
         left join deal_data deal
                   on meet_lg.hs_contact_id = deal.hs_contact_id
