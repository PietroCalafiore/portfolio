/*
 This is an example of how to extract past values from log tables and order them
 in order to get any pattern
 */


with nit_contacts as (select hubspot_id
                       from dw.hs_contact_live_history hclh
                       where extract(year from createdate) in ('2021','2022','2023')
                        and country in ('Italy', 'Spain','Turkey','Brazil','Poland','Germany','Argentina','Colombia',
                            'Chile', 'Mexico')
                        and contact_type_segment_test in ('MARKETING','HEALTH - GENERAL','SECRETARY')
                       and is_deleted <> 'true'
                        AND (email NOT SIMILAR TO '%(docplanner|miodottore|doctoralia|znanylekarz|jameda|deleted|gdpr)%' OR email IS NULL)
                        AND (hs_lead_status NOT IN ('UNQUALIFIED') OR hs_lead_status IS NULL)),
    -- To get contacts with specific segments
cnt_segments as (select nit.hubspot_id, count(distinct hclh.contact_type_segment_test) as cnt
                 from nit_contacts nit
                          join dw.hs_contact_live_history hclh
                               on nit.hubspot_id = hclh.hubspot_id
                 group by nit.hubspot_id),
    -- To have those segments counted, since we're interested only in contacts with multiple segments
multiple_segments as (select cs.hubspot_id
                      from cnt_segments cs
                      where cs.cnt > 1),
    -- Here's the count
segment_table as (select hclh.hubspot_id,
                              contact_type_segment_test,
                              LAG(contact_type_segment_test) OVER (PARTITION BY hclh.hubspot_id ORDER BY start_date) AS previous_segment,
                              date(start_date) as date
                       from dw.hs_contact_live_history hclh
                       join multiple_segments ms
                        on hclh.hubspot_id = ms.hubspot_id
),
ordered_segments as (select hubspot_id,
                            contact_type_segment_test,
                            date,
                            ROW_NUMBER() OVER (PARTITION BY hubspot_id ORDER BY date DESC) AS rn
                     from segment_table st
                     where (contact_type_segment_test <> previous_segment or
                            (contact_type_segment_test is not null and previous_segment is null))),
final_table as (select distinct os.hubspot_id                                               as contacts,
                                country,
                                extract(year from createdate)                               as year,
                                MAX(CASE WHEN rn = 1 THEN os.contact_type_segment_test END) AS segment_1_current,
                                MAX(CASE WHEN rn = 2 THEN os.contact_type_segment_test END) AS segment_2,
                                MAX(CASE WHEN rn = 3 THEN os.contact_type_segment_test END) AS segment_3,
                                case when max(date) > lcs_mql_at_test then 1 else 0 end     as segment_after_mql,
                                -- Creating a table with different and ordered segments
                                case
                                    when extract(year from lcs_mql_at_test) in ('2021', '2022', '2023') then '1'
                                    else '0' end                                            as mql,
                                case
                                    when lifecycle_stage in ('CLOSED-WON', 'WAITING', 'ONBOARDING', 'FARMING') then '1'
                                    else '0' end                                            as customer
                from ordered_segments os
                         join dw.hs_contact_live hcl
                              on os.hubspot_id = hcl.hubspot_id
                group by os.hubspot_id, country, year, lcs_mql_at_test, mql, customer)
select count(contacts) as contacts,
       country,
       year,
       segment_1_current,
       segment_2,
       segment_3,
       segment_after_mql,
       mql,
       customer,
       'current_segment' as tabella
from final_table ft
group by country, year, segment_1_current, segment_2, segment_3, segment_after_mql, mql, customer, tabella
union all
select count(contacts) as contacts,
       country,
       year,
       segment_1_current,
       segment_2,
       segment_3,
       segment_after_mql,
       mql,
       customer,
       'previous_segment' as tabella
from final_table ft
group by country, year, segment_1_current, segment_2, segment_3, segment_after_mql, mql, customer, tabella