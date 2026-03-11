WITH paid_open_deals AS
(
SELECT
main.country,
main.hubspot_id,
main.web_date,
main.campaign as Campaign_Name,
main.contact_type,
main.product_won,
main.deal_month,
main.lead_id,
main.br_specialisation,
cf.lead_ini,
cf.lead_end,
cf.lead_id,
cf.lead_id as lead_id_with_deal,
sf.last_source__c as sf_source,
sf.businessline__c as sf_product,
live.dealname,
live.hubspot_owner_id,
live."tag" as tag_deal,
main.lost_journey,
od.month as open_deal_month,
od.pipeline_type,
max(Campaign_Name) over(partition by main.hubspot_id order by main.date desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as last_campaign,
row_number() over (partition by od.deal_id,main.hubspot_id order by cf.lead_id desc) as row
FROM mart_cust_journey.msv_paid_media_campaign_hubspot_2 main
LEFT JOIN mart_cust_journey.cj_deal_month od on od.hs_contact_id = main.hubspot_id
LEFT JOIN  mart_cust_journey.cj_contact_facts cf on cf.contact_id = main.hubspot_id AND od.month between DATE_TRUNC('month',cf.lead_ini) AND COALESCE(cf.lead_end,current_date)
left join dw.hs_deal_live live on live.hubspot_id = od.deal_id
left join dp_salesforce.opportunity sf on sf.id = od.deal_id
where (main.br_specialisation != 'Bad Paramedical' OR main.br_specialisation IS NULL)  and od.month >= '2024-07-01'
--and main.country = 'Spain'
 and od.stage_is_month_new IS TRUE AND od.deal_stage NOT IN ('Closed Won', 'Closed Lost') and od.pipeline_type IN ('Individual New Sales', 'Clinics New Sales')
QUALIFY row = 1
),
    last_source_list as
    (
        select country, contact_id, b.lead_id,lifecycle_stage_start, month, last_source_so, source_so, lifecycle_stage, DATE_TRUNC('month',lifecycle_stage_start) as LCS_start_month,lead_ini,lead_end
            from (select *, rank() over(partition by a.lead_id order by lifecycle_stage_start desc) as lifecycle_rank
            from
                (
                select contact_facts.country, contact_lifecycle.contact_id, contact_lifecycle.lead_id,DATE_TRUNC('month',contact_facts.lead_ini) as lead_ini,COALESCE(contact_facts.lead_end,current_date) aS lead_end,
            contact_lifecycle.lifecycle_stage_start, contact_lifecycle.month, contact_lifecycle.last_source_so, contact_lifecycle.source_so, contact_lifecycle.lifecycle_stage,
            rank() over(partition by contact_lifecycle.lead_id
            order by contact_lifecycle.month desc) as month_rank
             from paid_open_deals pw INNER JOIN
  mart_cust_journey.cj_lcs_month contact_lifecycle ON pw.hubspot_id = contact_lifecycle.contact_id and contact_lifecycle.lead_id= pw.lead_id_with_deal
inner join mart_cust_journey.cj_contact_facts contact_facts
            on contact_lifecycle.contact_id = contact_facts.contact_id and contact_lifecycle.lead_id = contact_facts.lead_id
           ) a
        where month_rank= 1) b
        where lifecycle_rank = 1
        group by 1, 2 ,3, 4, 5, 6, 7, 8,9,10,11
    )
select pw.country,
       pw.hubspot_id,
       pw.lost_journey,
       pw.web_date,
       pw.dealname,
       pw.open_deal_month,
       pw.last_campaign,
       pw.pipeline_type,
       pw.tag_deal,
       pw.sf_product,
       pw.product_won,
       pw.deal_month,
        pw.sf_source,
source.last_source_so,
source.source_so,
     CASE
                 WHEN pw.country IN ('Colombia',
                                'Mexico',
                                'Spain',
                                'Brazil',
                                'Poland',
                                'Argentina',
                                'Chile',
'Peru') AND  pw.pipeline_type = 'Individual New Sales'
                 AND    ((source.source_so IS NOT NULL AND source.source_so NOT IN ('basic reference',
                                 'Target tool [DA]',
                                 'Sales Contact')) OR (source.last_source_so NOT IN ('basic reference','visiting pricing',
                                   'Target tool [DA]',
                                   'Sales Contact'))) THEN 'Inbound'
                 WHEN pw.country = 'Turkiye'
                 AND   (source.last_source_so NOT IN ('Target tool [DA]',
                                   'Sales Contact') OR source.source_so NOT IN ('Target tool [DA]',
                                 'Sales Contact')) THEN 'Inbound'
                 WHEN pw.country IN (
                                'Germany')
                 AND   (source.source_so NOT IN ('Target tool [DA]',
                                 'Sales Contact') OR source.last_source_so NOT IN ('Target tool [DA]',
                                   'Sales Contact')) THEN 'Inbound'

                 WHEN pw.country = 'Italy' and pw.pipeline_type = 'Individual New Sales'
                 AND    source.source_so IS NOT NULL
                 AND    (source.source_so NOT IN ('Target tool [DA]',
                                 'Sales Contact',
                                 'basic reference','other',
'new facility verification',
 'Massive assignment',
                                 'New verification','other') OR         source.last_source_so NOT IN ('Target tool [DA]',
                                   'Sales Contact',
                                   'basic reference',
                                   'Massive assignment',
                                    'visiting pricing',
                                    'e-commerce abandoned cart',
                                   'new facility verification',
                                   'New verification', 'other')) THEN 'Inbound'
         WHEN pw.country = 'Brazil' AND (pw.sf_source IS NOT NULL AND LOWER(pw.sf_source) NOT LIKE '%outbound%' AND pw.sf_source NOT IN ('Target tool [DA]','Sales Contact')) THEN 'Inbound'
WHEN  pw.country = 'Italy' and  (NOT pw.sf_source IN ('Basic reference','Target tool [DA]','Sales Contact','visiting pricing','new facility verification','New verification') AND LOWER(pw.sf_source) NOT LIKE '%outbound%') THEN 'Inbound'
    WHEN pw.country IN ('Spain','Mexico') AND pw.pipeline_type = 'Clinics New Sales'
    AND (pw.sf_source  NOT IN ('other','Target tool [DA]','Sales Contact') AND pw.sf_source IS NOT NULL AND LOWER(pw.sf_source) NOT LIKE '%internal reference%' AND LOWER(pw.sf_source) NOT LIKE '%outbound%') THEN 'Inbound'
WHEN pw.country IN ('Spain') AND pw.pipeline_type = 'Clinics New Sales' and pw.dealname NOT LIKE '%PMS deal%' THEN 'Inbound'
    WHEN  pw.country = 'Poland'  and ( pw.tag_deal NOT LIKE '%Cross sell%' AND hubspot_owner_id != 11078353) THEN 'Inbound'
 ELSE 'Outbound'
           END AS deal_allocation from paid_open_deals pw
LEFT JOIN last_source_list source
ON source.contact_id = pw.hubspot_id
