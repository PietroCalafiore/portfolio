WITH paid_wons AS
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
live."tag",
max(Campaign_Name) over(partition by main.hubspot_id order by main.date desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as last_campaign,
row_number() over (partition by main.deal_month,main.hubspot_id order by cf.lead_id desc) as row
FROM mart_cust_journey.msv_paid_media_campaign_hubspot_2 main
LEFT JOIN  mart_cust_journey.cj_contact_facts cf on cf.contact_id = main.hubspot_id AND deal_month between DATE_TRUNC('month',cf.lead_ini) AND COALESCE(cf.lead_end,current_date)
left join dw.hs_deal_live live on live.hubspot_id = main.deal_id
left join dp_salesforce.opportunity sf on sf.id = main.deal_id and stagename ='Closed Won'
where product_won is not null
and (main.br_specialisation != 'Bad Paramedical' OR main.br_specialisation IS NULL)  and deal_month >= '2024-01-01'
QUALIFY row = 1
),
    last_source_list as
    (
        select country, contact_id, lead_id,lifecycle_stage_start, month, last_source_so, source_so, lifecycle_stage, DATE_TRUNC('month',lifecycle_stage_start) as LCS_start_month,lead_ini,lead_end
            from (select *, rank() over(partition by lead_id order by lifecycle_stage_start desc) as lifecycle_rank
            from
                (
                select contact_facts.country, contact_lifecycle.contact_id, contact_lifecycle.lead_id,DATE_TRUNC('month',contact_facts.lead_ini) as lead_ini,COALESCE(contact_facts.lead_end,current_date) aS lead_end,
            contact_lifecycle.lifecycle_stage_start, contact_lifecycle.month, contact_lifecycle.last_source_so, contact_lifecycle.source_so, contact_lifecycle.lifecycle_stage,
            rank() over(partition by contact_lifecycle.lead_id
            order by contact_lifecycle.month desc) as month_rank
             from paid_wons pw INNER JOIN
  mart_cust_journey.cj_lcs_month contact_lifecycle ON pw.hubspot_id = contact_lifecycle.contact_id and contact_lifecycle.lead_id= pw.lead_id_with_deal
inner join mart_cust_journey.cj_contact_facts contact_facts
            on contact_lifecycle.contact_id = contact_facts.contact_id and contact_lifecycle.lead_id = contact_facts.lead_id
           ) a
        where month_rank= 1) b
        where lifecycle_rank = 1
        group by 1, 2 ,3, 4, 5, 6, 7, 8,9,10,11
    )

SELECT   --ate as lcs_date,
    LCS_start_month,
         main.country,
         main.hubspot_id,
         --cast(web_date as date) as Date_of_Paid_interaction,
         main.deal_month,
         main.campaign_name as Campaign_Name,
         main.last_campaign,
         main.contact_type,
         source.last_source_so,
         source.source_so,
         source.lead_ini,
         source.lead_end,
         main.product_won,
         main.sf_source,
     CASE
                 WHEN main.country IN ('Colombia',
                                'Mexico',
                                'Spain',
                                'Brazil',
                                'Poland',
                                'Argentina',
                                'Chile',
'Peru') AND  main.product_won = 'Agenda Premium'
                 AND    ((source.source_so IS NOT NULL AND source.source_so NOT IN ('basic reference',
                                 'Target tool [DA]',
                                 'Sales Contact')) OR (source.last_source_so NOT IN ('basic reference','visiting pricing',
                                   'Target tool [DA]',
                                   'Sales Contact'))) THEN 'Inbound'
                 WHEN main.country = 'Turkiye'
                 AND   (source.last_source_so NOT IN ('Target tool [DA]',
                                   'Sales Contact') OR source.source_so NOT IN ('Target tool [DA]',
                                 'Sales Contact')) THEN 'Inbound'
                 WHEN main.country IN (
                                'Germany')
                 AND   (source.source_so NOT IN ('Target tool [DA]',
                                 'Sales Contact') OR source.last_source_so NOT IN ('Target tool [DA]',
                                   'Sales Contact')) THEN 'Inbound'

                 WHEN main.country = 'Italy' and main.product_won = 'Agenda Premium'
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
         WHEN  main.country = 'Brazil' AND (main.sf_source IS NOT NULL AND LOWER(main.sf_source) NOT LIKE '%outbound%' AND main.sf_source NOT IN ('Target tool [DA]','Sales Contact')) THEN 'Inbound'
WHEN  main.country = 'Italy' and  (NOT main.sf_source IN ('Basic reference','Target tool [DA]','Sales Contact','visiting pricing','new facility verification','New verification') AND LOWER(sf_source) NOT LIKE '%outbound%') THEN 'Inbound'
    WHEN main.country IN ('Spain','Mexico') AND product_won = 'Clinic Agenda'
    AND (main.sf_source  NOT IN ('other','Target tool [DA]','Sales Contact') AND main.sf_source IS NOT NULL AND LOWER(sf_source) NOT LIKE '%internal reference%' AND LOWER(sf_source) NOT LIKE '%outbound%') THEN 'Inbound'
WHEN main.country IN ('Spain') AND product_won = 'Clinic Cloud' and main.dealname NOT LIKE '%PMS deal%' THEN 'Inbound'
    WHEN  main.country = 'Poland'  and ( main."tag" LIKE '%Cross sell%' OR hubspot_owner_id = 11078353) THEN NULL
WHEN  main.country = 'Poland' THEN 'Inbound'
 ELSE 'Outbound'
           END AS deal_allocation, --272,
main.br_specialisation
FROM  paid_wons main 
LEFT JOIN last_source_list source
ON source.contact_id = main.hubspot_id WHERE deal_allocation IS NOT NULL
GROUP BY --date,
         main.country,
         main.hubspot_id,
         main.campaign_name,
         main.deal_month,
LCS_start_month,
         main.product_won,
         main.contact_type,
         source.last_source_so,
         source.source_so,
source.lead_ini,
source.lead_end,
deal_allocation,
main.sf_source,
main.last_campaign,
main.br_specialisation
order by deal_month
