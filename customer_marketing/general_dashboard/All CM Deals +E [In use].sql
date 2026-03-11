with expansion_raw_deals as (select facts.opportunity_id        as deal_id,
                                    owner_id,
                                    case
                                        when facts.pipeline like 'Expansion metrics%'
                                            and facts.current_stage = 'Closed Won'
                                            and lower(facts.product) like '%media%'
                                            and facts.tag_so like '%Upsell%'
                                            and lower(deals.last_source_cm) similar to '%media%|%m360%|%google%'
                                            then 'media360'
                                        when
                                            facts.pipeline like 'Expansion metrics%'
                                                and facts.current_stage = 'Closed Won'
                                                and integration_type = 'PMS - MKTPL Freemium'
                                            then 'patient_portal'
                                        when facts.pipeline like 'Expansion metrics%'
                                            and facts.current_stage = 'Closed Won'
                                            and facts.product in ('First Class', 'first_class')
                                            and facts.tag_so like '%Upsell%'
                                            and lower(deals.last_source_cm) like '%first%'
                                            and lower(last_source_cm) like '%first%'
                                            and hcl.first_class_lead_at >= '2024-11-01'
                                            then 'first_class'

                                        else null end           as deal_category,
                                    facts.country,
                                    facts.hs_contact_id,
                                    deals.last_source_cm        as last_source,
                                    facts.pipeline,
                                    facts.pipeline_type,
                                    facts.product,
                                    date(facts.createdate)      as create_date,
                                    date(facts.closed_won_date) as closed_won_date,
                                    date(facts.month_won)       as closed_won_month,
                                    facts.mrr_euro,
                                    facts.mrr_original_currency

                             from mart_cust_journey.cj_opportunity_facts facts
                                      left join dw.hs_deal_live deals
                                                on facts.opportunity_id = deals.hubspot_id
                                      left join dw.hs_contact_live hcl
                                                on facts.hs_contact_id = hcl.hubspot_id
                             where facts.pipeline like 'Expansion metrics%'
                               and facts.current_stage = 'Closed Won'
                               and closed_won_month >= '2025-10-01'


                             union all


                             select distinct deal_id                    as deal_id,
                                             hubspot_owner_id::VARCHAR  as owner_id,
                                             'noa'                      as deal_product,
                                             noa.country,
                                             noa.hubspot_id             as hs_contact_id,
                                             frozen_last_source         as last_source,
                                             'Noa'                      as pipeline,
                                             'Noa'                      as pipeline_type,
                                             'Noa'                      as product,
                                             date(noa.deal_create_date) as create_date,
                                             date(noa.deal_close_date)  as closed_won_date,
                                             date(noa.deal_month)       as closed_won_month,
                                             noa.mrr_euro,
                                             noa.mrr_original_currency
                             from mart_cust_journey.noa_marketing_kpis_cm_lg_combined noa
                                      left join dw.hs_deal_live deal
                                                on noa.deal_id = deal.hubspot_id
                             where lg_cm_flag_combined = 'CM'),

     expansion_deals as (select hs_contact_id,
                                country,
                                deal_id,
                                case when owner_id = '514996346' then 1 else 0 end as ecommerce_flag,
                                pipeline,
                                pipeline_type,
                                product,
                                deal_category,
                                create_date,
                                closed_won_date,
                                closed_won_month,
                                mrr_euro,
                                mrr_original_currency
                         from expansion_raw_deals exp
                         where closed_won_date >= '2025-11-01'),

     fc_raw_deals as (select subscription_id as deal_id,
                             mkpl_entity_id,
                             CASE
                                 WHEN country_code = 'ar' THEN 'Argentina'
                                 WHEN country_code = 'cl' THEN 'Chile'
                                 WHEN country_code = 'mx' THEN 'Mexico'
                                 WHEN country_code = 'br' THEN 'Brazil'
                                 WHEN country_code = 'pl' THEN 'Poland'
                                 WHEN country_code = 'es' THEN 'Spain'
                                 WHEN country_code = 'it' THEN 'Italy'
                                 WHEN country_code = 'co' THEN 'Colombia'
                                 WHEN country_code = 'de' THEN 'Germany'
                                 END         AS country,
                             activation_source,
                             date            as deal_at,
                             local_currency_price,
                             euro_currency_price
                      from bld_media360.expansion_fc_activations_detailed fc

                      where activation_source_by_creator = 'ecommerce'
                        and date(month) >= '2025-11-01'),

     fc_ecomm_deals as (select hcl.hubspot_id                                    as hs_contact_id,
                               fc.country,
                               deal_id,
                               '1'                                               as ecommerce_flag,
                               case
                                   when contact_type_segment_test in
                                        ('FACILITY', 'DOCTOR&FACILITY - 2IN1', 'HEALTH - GENERAL')
                                       then 'Expansion metrics (Clinics)'
                                   else 'Expansion metrics (Agenda Doctors)' end as pipeline,
                               case
                                   when contact_type_segment_test in
                                        ('FACILITY', 'DOCTOR&FACILITY - 2IN1', 'HEALTH - GENERAL')
                                       then 'Clinics Expansion'
                                   else 'Individual Expansion' end               as pipeline_type,
                               case
                                   when contact_type_segment_test in
                                        ('FACILITY', 'DOCTOR&FACILITY - 2IN1', 'HEALTH - GENERAL')
                                       then 'First Class for Clinic'
                                   else 'First Class' end                        as product,
                               'first_class'                                     as deal_category,
                               deal_at                                           as create_date,
                               deal_at                                           as closed_won_date,
                               DATE_TRUNC('month', deal_at)                      as closed_won_month,
                               euro_currency_price                               as mrr_euro,
                               local_currency_price                              as mrr_original_currency
                        from fc_raw_deals fc
                                 left join dw.hs_contact_live hcl
                                           on fc.country = hcl.country
                                               and (fc.mkpl_entity_id = hcl.source_doctor_id
                                                   or fc.mkpl_entity_id = hcl.source_facility_id))

select hs_contact_id,
       country,
       deal_id,
       ecommerce_flag,
       pipeline,
       pipeline_type,
       product,
       deal_category,
       create_date,
       closed_won_date,
       closed_won_month,
       mrr_euro,
       mrr_original_currency
from expansion_deals

union all

select hs_contact_id::varchar,
       country,
       deal_id,
       ecommerce_flag,
       pipeline,
       pipeline_type,
       product,
       deal_category,
       create_date,
       closed_won_date,
       closed_won_month,
       mrr_euro,
       mrr_original_currency
from fc_ecomm_deals


