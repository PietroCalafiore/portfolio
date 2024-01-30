/*
 This is an example of how to work with ordered timestamps for the same column
 and how to compare them to a specific action
 */


with all_mals as (select hubspot_id, recent_conversion_date
                  from dw.hs_contact_live_history hclh
                  where country in ('Italy', 'Spain','Turkey','Brazil','Poland','Germany','Argentina','Colombia',
                        'Chile', 'Mexico')
                    and is_deleted <> 'Yes'
                    and extract(year from lcs_mal_at_test) = '2023'
                    and (recent_conversion_date < recent_deal_close_date or recent_deal_close_date is null)),
conv_cnt as (select distinct hubspot_id, count(distinct recent_conversion_date) as conv_cnt
             from all_mals allm
             group by hubspot_id),
mal_more_once as (select hubspot_id
             from conv_cnt cc
             where conv_cnt > 1), -- Counting more than 1 distinct timestamp
conv_date_table as (select mmo.hubspot_id,
                               date(recent_conversion_date) as conv_date,
                               LAG(date(recent_conversion_date))
                               OVER (PARTITION BY hclh.hubspot_id ORDER BY start_date) AS prev_date,
                               date(dw_updated_at)                                     as date
                    -- Creating a table with timestamps
                        from mal_more_once mmo
                            join dw.hs_contact_live_history hclh
                                on mmo.hubspot_id = hclh.hubspot_id
),
ordered_conv_date as (select hubspot_id,
                             conv_date,
                             date,
                             ROW_NUMBER() OVER (PARTITION BY hubspot_id ORDER BY date DESC) AS rn
                      -- Ordering them
                        from conv_date_table cdtab
                        where (conv_date <> prev_date or prev_date is null)
                        ),
date_diff as (select distinct hubspot_id,
                              MAX(CASE WHEN rn = 1 THEN ordate.conv_date END) AS conv_date_1,
                              MAX(CASE WHEN rn = 2 THEN ordate.conv_date END) AS conv_date_2,
                              datediff(day, conv_date_2, conv_date_1) as conv_date_diff
              from ordered_conv_date ordate
              group by hubspot_id),
deals as (select distinct hclh.hubspot_id
              from dw.hs_contact_live_history hclh
                left join dw.hs_deal_live deal
                    on hclh.hubspot_id = deal.hubspot_contact_id
                left join dw.hs_deal_pipeline_dict pdict
                    on deal.pipeline = pdict.internal_value
                where hclh.lifecycle_stage in ('CLOSED-WON','WAITING','ONBOARDING','FARMING')
                    and pipeline in ('default','43a325e9-6d74-4fce-9539-1f8792945936',
                                    '10148608','16264973'))
-- Joing by deals table
select count(ddf.hubspot_id) as mals, hcl.country,
       case when hcl.utm_medium in ('paid-social','ppc','display','cpc') then 1 else 0 end as paid,
       case when hcl.lifecycle_stage in ('CLOSED-WON','WAITING','ONBOARDING','FARMING')
           and recent_deal_close_date between recent_conversion_date
               and DATEADD(day, 60, recent_conversion_date) then 1 else 0 end as customer
from date_diff ddf
    left join deals d
        on ddf.hubspot_id = d.hubspot_id
    join dw.hs_contact_live hcl
        on ddf.hubspot_id = hcl.hubspot_id
where conv_date_diff > 180
group by country, paid, customer




