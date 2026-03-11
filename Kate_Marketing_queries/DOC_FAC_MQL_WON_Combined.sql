WITH mqls AS (SELECT contact_id as contact,
                     true_deal_flag,
                     LAG(contact) over (Partition by contact,deal_month order by deal_month) as won_id_for_deduping,
                     deal_allocation,
                     cmm.country,
                     cmm.sub_brand,
                     cmm.lifecycle_stage,
                     cmm.target                                                                AS segment,
                     cmm.new_spec,
                     cmm.verified,
                     MAX(cmm.source_so) OVER (PARTITION BY contact_id,DATE(deal_month))       AS source,
                     date(month)                                                               AS mql_month,
                     min(DATE(cmm.lifecycle_stage_start))                                      AS mql_day,
                     DATE(deal_month) AS deal_month,
                     max(date(recent_deal_close_date))                                         AS deal_at,
                     COALESCE(hcl.mql_last_touch_channel_wf, 'Unknown')                        AS channel,
                     COALESCE(hcl.mql_last_conversion_place_wf, 'Unknown')                     AS conversion_place
              FROM mart_cust_journey.cj_mqls_monthly cmm
                       LEFT JOIN dw.hs_contact_live hcl ON cmm.contact_id = hcl.hubspot_id
              WHERE (DATE(cmm.month) >= '2024-01-01' OR cmm.deal_month >= '2024-01-01')
              group by cmm.country, cmm.sub_brand, cmm.lifecycle_stage, cmm.target, cmm.new_spec,
                       cmm.verified, cmm.source_so, DATE(cmm.month),
                       hcl.mql_last_touch_channel_wf, hcl.mql_last_conversion_place_wf,
                       contact,true_deal_flag,
                     deal_month, deal_allocation
              UNION ALL
              SELECT cmm.contact_id                       AS contact,
                     true_deal_flag,
                     LAG(contact) over (Partition by contact,deal_month order by deal_month) as won_id_for_deduping,
                     'Inbound' AS deal_allocation,
                     cmm.country,
                     hcl.sub_brand_wf_test                AS sub_brand,
                     cmm.lifecycle_stage,
                     cmm.target                           AS segment,
                     'None'                               AS new_spec,
                     CASE
                         WHEN hcl.facility_verified = true THEN 'Verified'
                         ELSE 'Not Verified'
                         END                              AS verified,
                     MAX(cmm.source_so) OVER (PARTITION BY contact_id,DATE(deal_month))       AS source,
                     DATE(cmm.month)                      AS mql_month,
                     MIN(DATE(cmm.lifecycle_stage_start)) AS mql_day,
                     DATE(deal_month) AS deal_month,
                     MAX(DATE(recent_deal_close_date))                                         AS deal_at,
                     hcl.mql_last_touch_channel_wf        AS channel,
                     hcl.mql_last_conversion_place_wf     AS conversion_place
              FROM mart_cust_journey.cj_mqls_monthly_clinics cmm
                       LEFT JOIN dw.hs_contact_live hcl ON cmm.contact_id = hcl.hubspot_id
              WHERE cmm.mql_product IS NOT NULL
                AND (DATE(cmm.month) >= '2024-01-01' OR cmm.deal_month >= '2024-01-01')
              GROUP BY cmm.contact_id, cmm.country, hcl.sub_brand_wf_test, cmm.lifecycle_stage, cmm.target,
                       hcl.facility_verified, cmm.source_so, DATE(cmm.month),
                       hcl.mql_last_touch_channel_wf, hcl.mql_last_conversion_place_wf,true_deal_flag,
                     deal_month,deal_allocation)
select count(distinct contact) as contacts,
       count(DISTINCT CASE WHEN lifecycle_stage != 'only_won' THEN contact END) as mql,
       count(DISTINCT CASE WHEN true_deal_flag and deal_allocation = 'Inbound' AND mqls.won_id_for_deduping IS NULL THEN contact END) as won,
       deal_month,
       country,
       sub_brand,
       segment,
       new_spec            as spec_split,
       verified,
       source,
       mql_month           as mql_month,
       mql_day             as mql_at,
       deal_at,
       channel,
       conversion_place
from mqls
group by country,
         sub_brand,
         deal_month,
         segment,
         spec_split,
         verified,
         source,
         mql_month,
         mql_day,
         channel,
         conversion_place,
         deal_at
