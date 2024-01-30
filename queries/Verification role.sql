/*
 This is an example of how to work with dates and compare them
 */

with all_database as (select country,
                       hubspot_id,
                       coalesce(date(became_verified_facility_at), date(new_verification_at)) as verification_at,
                       date(lcs_mql_at_test)                                                  as mql_at
                from dw.hs_contact_live hcl
                where country in ('Italy', 'Spain', 'Turkey', 'Brazil', 'Poland', 'Germany', 'Argentina', 'Colombia',
                                  'Chile', 'Mexico')
                    and extract(year from createdate) = 2023
                    and (contact_type_segment_test NOT IN ('PATIENT', 'STUDENTS') OR contact_type_segment_test IS NULL)
                    and is_deleted = FALSE
                    and (email NOT SIMILAR TO '%(docplanner|miodottore|doctoralia|znanylekarz|jameda|deleted|gdpr)%' OR
                         email IS NULL)
                    and (hs_lead_status NOT IN ('UNQUALIFIED') OR hs_lead_status IS NULL)),
won_deals as (select ad.hubspot_id,
                     date(hdl.createdate) as won_at
              from all_database ad
                       left join dw.hs_deal_live hdl
                            on ad.hubspot_id = hdl.hubspot_contact_id
                       join dw.hs_deal_stage_dict ddict
                            on hdl.dealstage = ddict.internal_value
              where lower(label) like '%won%'
                and extract(year from hdl.createdate) = 2023
                and hdl.is_deleted <> 'Yes'),
verification_first as (select country,
                           ad.hubspot_id,
                           verification_at,
                           mql_at,
                           won_at
                    from all_database ad
                         left join won_deals wd
                            on ad.hubspot_id = wd.hubspot_id
                    where verification_at is not null
                      and (verification_at < dateadd(day, -5, mql_at) or mql_at is null)
                      and (mql_at is not null and won_at is not null or won_at is null))
select country,
    sum(case when verification_at is not null then 1 else 0 end) as verified,
    sum(case when mql_at is not null then 1 else 0 end) as mqls,
    sum(case when verification_at < dateadd(day, -10, won_at) and won_at is not null then 1 else 0 end) as wons
from verification_first
group by country