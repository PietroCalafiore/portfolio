with customer_base as (
    select
        hcl.hubspot_id,
        hcl.country,
        contact_type_segment_test as segment,
        spec_split_test as spec_type,
        date(commercial_from) as commercial_from,
        hcl.member_customers_list
    from dw.hs_contact_live hcl
),

mql_data as (
    select distinct
        hist.hubspot_id::varchar,
        date(hist.media360_ga_lead_at)                     as media360_lead_at,
        date(hist.first_class_lead_at)                     as fc_lead_at,
        date(hist.media360_sm_lead_at)                     as patientportal_lead_at,
        date(hist.doctor_facility___p_p_vip___lead_at__wf) as payment_lead_at
    from dw.hs_contact_live_history hist
    where
          date(hist.media360_ga_lead_at) >= '2025-01-01'
       or date(hist.first_class_lead_at) >= '2025-01-01'
       or date(hist.media360_sm_lead_at) >= '2025-01-01'
       or date(hist.doctor_facility___p_p_vip___lead_at__wf) >= '2025-01-01'
),

noa_mql_data as (
    select distinct
        noa.hubspot_id,
        date(mql_deal_at) as noa_mql_at
    from mart_cust_journey.noa_marketing_kpis_cm_lg_combined noa
    where lg_cm_flag_combined = 'CM'
      and date(mql_deal_at) >= '2025-01-01'
),

mql_contacts as (
    select hubspot_id from mql_data
    union
    select hubspot_id from noa_mql_data
),

customer_contacts as (
    select hubspot_id::varchar
    from customer_base
    where member_customers_list = 'Yes'
),

population as (
    select hubspot_id from mql_contacts
    union
    select hubspot_id from customer_contacts
),

all_mql_data as (
    select
        distinct cb.hubspot_id,
        cb.country,
        cb.segment,
        cb.spec_type,
        cb.commercial_from,
        mql.media360_lead_at,
        mql.fc_lead_at,
        mql.patientportal_lead_at,
        mql.payment_lead_at,
        noa.noa_mql_at,
        cb.member_customers_list
    from population p
    join customer_base cb
        on p.hubspot_id = cb.hubspot_id
    left join mql_data mql
        on p.hubspot_id = mql.hubspot_id
    left join noa_mql_data noa
        on p.hubspot_id = noa.hubspot_id
)

select *
from all_mql_data
