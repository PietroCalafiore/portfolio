WITH all_valid_mals_mqls AS
(
           SELECT     contact.contact_id AS hubspot_id,
                      contact.lead_id,
                      contact_facts.verified,
                      contact.lifecycle_stage,
                      contact.updated_at AS lcs_date,
                      contact.is_mkt_push_lead,
                      contact_facts.country,
                      contact_facts.segment AS contact_type,
                      contact_facts.product_recommended,
                      contact_facts.spec_split
           FROM       cj_data_layer.cj_contact_lead_main contact
           INNER JOIN mart_cust_journey.cj_contact_facts contact_facts
           ON         contact_facts.contact_id = contact.contact_id
           AND        contact_facts.lead_id = contact.lead_id
           AND        contact_facts.country IN ('Spain',
                                                'Brazil',
                                                'Mexico',
                                                'Turkey',
                                                'Turkiye',
                                                'Poland',
                                                'Italy',
                                                'Germany',
                                                'Colombia',
                                                'Chile',
                                                'Peru')
           WHERE      contact.lifecycle_stage IN ('MAL',
                                                  'MQL')
           AND        (
                                 contact_facts.segment != 'PATIENT'
                      OR         contact_facts.segment IS NULL)
           AND        (
                                 contact.is_mkt_push_lead IS NULL
                      OR         (
                                            contact.is_mkt_push_lead IN (0,
                                                                         1))
                      AND        contact.updated_at >= '2024-01-01' ) ), all_valid_mals_mqls_list AS
(
         SELECT   hubspot_id,
                  lead_id-- we need a list with just one row per contact
         FROM     all_valid_mals_mqls
         GROUP BY hubspot_id,
                  lead_id ), campaign_interaction_scope AS
(
           SELECT     utm_log.hubspot_id,
                      full_scope.lead_id,
                      full_scope.is_mkt_push_lead,
                      full_scope.contact_type,
                      full_scope.verified,
                      full_scope.product_recommended,
                      full_scope.spec_split,
                      utm_log.country,
                      utm_log.utm_campaign                                                                         AS campaign,
                      Lag(utm_log.utm_campaign) OVER (partition BY utm_log.hubspot_id ORDER BY utm_log.updated_at) AS prev_campaign_name,--we use this to select the first row after a campaign name has been updated
                      -- MIN(utm_log.updated_at) OVER (PARTITION BY utm_log.hubspot_id, utm_log.utm_campaign) AS first_campaign_interaction,
                      utm_log.updated_at                                                                                          AS interaction,
                      Row_number() OVER (partition BY utm_log.hubspot_id, utm_log.utm_campaign ORDER BY utm_log.updated_at DESC ) AS rn,
                      COALESCE(utm_log.utm_source, 'No info')                                                                     AS source,
                      COALESCE(utm_log.utm_medium, 'No info')                                                                     AS medium,
                      COALESCE(utm_log.utm_term, 'No info')                                                                       AS keyword,
                      COALESCE(utm_log.utm_content, 'No info')                                                                    AS content,
                      CASE
                                 WHEN utm_log.utm_source = 'facebook'
                                 OR         utm_log.utm_source = 'Social_Ads'
                                 OR         utm_log.utm_source = 'fb'
                                 OR         utm_log.utm_source = 'ig' THEN 'facebook'
                                 WHEN utm_log.utm_source = 'linkedin' THEN 'linkedin'
                                 WHEN utm_log.utm_source = 'softdoit' THEN 'softdoit'
                                 WHEN utm_log.utm_source = 'criteo' THEN 'criteo'
                                 WHEN utm_log.utm_source = 'bing' THEN 'bing'
                                 WHEN utm_log.utm_source = 'taboola' THEN 'taboola'
                                 WHEN utm_log.utm_source = 'tiktok' THEN 'tiktok'
                                 WHEN utm_log.utm_source = 'google'
                                 OR         utm_log.utm_source = 'adwords' THEN 'google'
                      END AS hsa_net
           FROM       all_valid_mals_mqls full_scope
           INNER JOIN cj_data_layer.cj_sat_contact_hs_1h_log_merged utm_log
           ON         utm_log.hubspot_id = full_scope.hubspot_id
           WHERE      utm_log.utm_campaign IS NOT NULL
           AND        utm_log.utm_source IN ('facebook',
                                             'fb',
                                             'ig',
                                             'Social_Ads',
                                             'linkedin',
                                             'criteo',
                                             'google',
                                             'adwords',
                                             'bing',
                                             'taboola',
                                             'tiktok',
                                             'softdoit')
           AND        (
                                 utm_log.utm_campaign LIKE '%\\_%\\_%\\_%'
                      OR         utm_log.utm_campaign IN ('it_gipo_mal',
                                                          'it_fac_mal'))
           AND        utm_log.country IN ('Spain',
                                          'Brazil',
                                          'Mexico',
                                          'Turkey',
                                          'Turkiye',
                                          'Poland',
                                          'Italy',
                                          'Germany',
                                          'Colombia',
                                          'Chile',
                                          'Peru')
           AND        utm_log.updated_at >= '2023-12-01'
           GROUP BY   utm_log.hubspot_id,
                      full_scope.hubspot_id,
                      full_scope.lead_id,
                      utm_log.country,
                      full_scope.contact_type,
                      utm_log.utm_campaign,
                      full_scope.lifecycle_stage,
                      full_scope.is_mkt_push_lead,
                      utm_log.utm_source,
                      utm_log.utm_medium,
                      utm_log.utm_term,
                      utm_log.utm_content,
                      utm_log.updated_at,
                      full_scope.verified,
                      full_scope.product_recommended,
                      full_scope.spec_split ), utm_log_lcs_join AS
(
           SELECT     lcs.hubspot_id,
                      lcs.lead_id,
                      lcs.is_mkt_push_lead,
                      lcs.contact_type,
                      lcs.verified,
                      lcs.product_recommended,
                      lcs.spec_split,
                      lcs.country,
                      utm.campaign,
                      utm.source,
                      utm.medium,
                      utm.keyword,
                      utm.content,
                      utm.hsa_net,
                      lcs.lifecycle_stage,
                      CASE
                                 WHEN Datepart(hour, lcs.lcs_date) = 0
                                 AND        Datepart(minute,lcs.lcs_date) = 0
                                 AND        Datepart(second, lcs.lcs_date) = 0 THEN Dateadd(second, -1, Dateadd(day, 0, lcs.lcs_date))
                                 ELSE lcs.lcs_date
                      END AS new_lcs_date,
                      lcs.lcs_date,
                      prev_campaign_name,
                      interaction
           FROM       campaign_interaction_scope utm
           INNER JOIN all_valid_mals_mqls lcs
           ON         utm.hubspot_id = lcs.hubspot_id
           WHERE      new_lcs_date BETWEEN interaction - interval '1 hour' AND        interaction::date + interval '31 day'
           AND        ((
                                            prev_campaign_name <> campaign)
                      OR         prev_campaign_name IS NULL) ), lcs_utm_order_control AS
(
         SELECT   * ,
                  row_number() OVER (partition BY hubspot_id, country, lifecycle_stage,new_lcs_date ORDER BY new_lcs_date DESC, interaction DESC) AS lcs_order,
                  row_number() OVER (partition BY hubspot_id, country, lifecycle_stage,interaction ORDER BY new_lcs_date ASC)                     AS interaction_order
         FROM     utm_log_lcs_join
         WHERE    true qualify lcs_order = 1
         AND      interaction_order = 1 ), hubspot_web_props_aux_1_filtered_end AS
(
         SELECT   *,
                  lag(lead_id,1) OVER (partition BY hubspot_id ORDER BY new_lcs_date)         AS prev_lead_id,          --we use this to select the first row after a campaign name has been updated
                  lag(campaign,1) OVER (partition BY hubspot_id ORDER BY new_lcs_date)        AS previous_campaign_name,--we use this to select the first row after a campaign name has been updated
                  lag(lifecycle_stage,1) OVER (partition BY hubspot_id ORDER BY new_lcs_date) AS previous_lcs
         FROM     lcs_utm_order_control
         WHERE    true qualify NOT (
                           previous_campaign_name = campaign
                  AND      lead_id <> prev_lead_id)
         OR       prev_lead_id IS NULL ), cj_deals_full_scope AS
(
           SELECT     deal.country,
                      deal.month AS deal_month,
                      deal_stage_start,
                      deal.hs_contact_id,
                      deal.lead_id,
                      deal.deal_id,
                      nvl(deal_length,12)                                         AS deal_length,
                      max(mrr_euro) OVER( partition BY deal.deal_id)              AS mrr_euro,
                      max(mrr_original_currency) OVER( partition BY deal.deal_id) AS mrr_original_currency,
                      CASE
                                 WHEN deal.country = 'Brazil'
                                 AND        deal.pipeline LIKE '%PMS%' THEN 'Feegow'
                                 WHEN deal.country = 'Italy'
                                 AND        deal.pipeline LIKE '%PMS%' THEN 'Gipo'
                                 WHEN deal.country = 'Spain'
                                 AND        deal.pipeline LIKE '%Enterprise SaaS4Clinics / Marketplace%' THEN 'Clinic Cloud'
                                 WHEN deal.country = 'Poland'
                                 AND        deal.pipeline LIKE '%PMS%' THEN 'MyDr'
                                 WHEN deal.pipeline_type = 'Individual New Sales' THEN 'Agenda Premium'
                                 ELSE 'Clinic Agenda'
                      END AS product_won
           FROM       mart_cust_journey.cj_deal_month deal
           INNER JOIN all_valid_mals_mqls_list scope -- we select just the ones that arrive to won stage after entering the correct way to the funnel, in this case MAL and MQL
           ON         deal.hs_contact_id = scope.hubspot_id
           WHERE      deal.deal_stage IN ('Closed Won')
           AND        pipeline_type   IN ('Individual New Sales',
                                          'Clinics New Sales')
           AND        is_current_stage
           AND        stage_is_month_new
           GROUP BY   deal_month,
                      deal.hs_contact_id,
                      deal.lead_id,
                      deal.deal_id,
                      deal_length,
                      deal.country,
                      mrr_euro,
                      mrr_original_currency,
                      deal_stage_start,
                      deal.pipeline,
                      deal.pipeline_type ), cj_deals_deduped AS
(
         SELECT   deal.country,
                  deal_month,
                  deal.hs_contact_id,
                  deal.lead_id,
                  deal_length,
                  deal_stage_start,
                  mrr_euro,
                  product_won,
                  nvl(round(mrr_euro              *deal_length,2),0)  AS total_revenue_eur,
                  nvl(round(mrr_original_currency * deal_length,2),0) AS total_revenue_origina_curr
         FROM     cj_deals_full_scope deal
         GROUP BY deal.country,
                  deal_month,
                  deal.hs_contact_id,
                  deal.lead_id,
                  deal_length,
                  deal_stage_start,
                  mrr_euro,
                  product_won,
                  total_revenue_eur,
                  total_revenue_origina_curr ), 
next AS
(
          SELECT    DATE_TRUNC('day',main.new_lcs_date) AS date,
                    main.hubspot_id,
                    main.country,
                    main.verified,
                    main.spec_split,
                    main.campaign,
                    main.hsa_net,
                    main.content,
                    deal.deal_month,
                    deal.product_won,
                    COALESCE(main.contact_type,'UNKNOWN') AS contact_type,
                    COUNT(
                    CASE
                              WHEN main.lifecycle_stage = 'MAL' THEN main.lead_id
                    END) AS mal,
                    COUNT(
                    CASE
                              WHEN main.lifecycle_stage = 'MQL' THEN main.lead_id
                    END) AS mql
          FROM      hubspot_web_props_aux_1_filtered_end main
          LEFT JOIN cj_deals_deduped deal
          ON        deal.hs_contact_id = main.hubspot_id
          AND       deal.deal_stage_start >= date_trunc('day',main.new_lcs_date) --noqa
          GROUP BY  date,
                    main.hubspot_id,
                    main.country,
                    main.verified,
                    main.spec_split,
                    main.campaign,
                    main.hsa_net,
                    main.content,
                    deal.deal_month,
                    deal.product_won,
                    contact_type,
                    main.lifecycle_stage)
SELECT   date,
         country,
         main.hubspot_id,
         main.verified,
         main.campaign,
         main.hsa_net,
         main.content,
         main.deal_month,
         main.product_won,
         upper(LEFT(main.campaign,2)) AS country_by_campaign,
         CASE
                  WHEN split_part(main.campaign,'_',3) = 'mal-mql'
                  OR       split_part(main.campaign,'_',3) = 'mql-mal' THEN
                           CASE
                                    WHEN regexp_substr(main.content,'^[a-zA-Z]{2}_.*') LIKE '%_mal_%' THEN 'mal'
                                    WHEN regexp_substr(main.content,'^[a-zA-Z]{2}_.*') LIKE '%_mql_%' THEN 'mql'
                                    ELSE split_part(main.campaign,'_',3)
                           END
                  WHEN split_part(main.campaign,'_',3) = 'pure' THEN 'mql'
                  ELSE split_part(main.campaign,'_',3)
         END AS campaign_goal,
         CASE
                  WHEN split_part(main.campaign,'_',2) = 'doc-fac'
                  OR       split_part(main.campaign,'_',2) = 'fac-doc' THEN
                           CASE
                                    WHEN regexp_substr(main.content,'^[a-zA-Z]{2}_.*') LIKE '%_doc_%' THEN 'doc'
                                    WHEN regexp_substr(main.content,'^[a-zA-Z]{2}_.*') LIKE '%_fac_%' THEN 'fac'
                                    ELSE split_part(main.campaign,'_',2)
                           END
                  ELSE split_part(main.campaign,'_',2)
         END AS campaign_target,
         CASE
                  WHEN lower(main.campaign) LIKE '%feegow%'
                  OR       lower(main.campaign) LIKE '%-fg-%' THEN 'Feegow'
                  WHEN lower(main.campaign) LIKE '%clinic-cloud%'
                  OR       lower(main.campaign) LIKE '%-cc-%' THEN 'Clinic Cloud'
                  WHEN lower(main.campaign) LIKE '%gipo%' THEN 'GIPO'
                  WHEN lower(main.campaign) LIKE '%mydr%' THEN 'MyDr'
                  ELSE 'Agenda'
         END AS campaign_product,
         CASE
                  WHEN main.date >= '2024-01-01'
                  AND      main.contact_type = 'MARKETING'
                  AND      NOT (
                                    main.campaign LIKE '%mydr%') THEN 'OTHER'
                           --as per Jonathan´s request all Marketing segment contacts not to be counted for any campaigns except PMS ones from 2024 --only valid for Poland based on feedback in May 24
                  WHEN main.country = 'Poland'
                  AND      main.campaign LIKE '%mydr%' THEN
                           CASE
                                    WHEN main.contact_type IN ('FACILITY',
                                                               'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                                    ELSE 'OTHER'
                           END
                  WHEN main.country = 'Poland' THEN
                           CASE
                                    WHEN main.contact_type IN ('DOCTOR',
                                                               'HEALTH - GENERAL') THEN 'DOCTOR'
                                    WHEN main.contact_type IN ('FACILITY',
                                                               'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                                    ELSE 'OTHER'
                           END
                  WHEN main.country IN ('Colombia',
                                        'Peru') THEN
                           CASE
                                    WHEN main.contact_type IN ('SECRETARY',
                                                               'DOCTOR') THEN 'DOCTOR'
                                    WHEN main.contact_type IN ('FACILITY',
                                                               'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                                    ELSE 'OTHER'
                           END
                  WHEN main.country = 'Brazil'
                  AND      (
                                    main.campaign LIKE '%feegow%'
                           OR       lower(main.campaign) LIKE '%-fg-%') THEN
                           CASE
                                    WHEN main.contact_type IN ('PATIENT',
                                                               'UNKNOWN',
                                                               'STUDENT') THEN 'OTHER'
                                    ELSE 'FACILITY'
                           END
                  WHEN main.country = 'Brazil' THEN
                           CASE
                                    WHEN main.contact_type IN ('SECRETARY',
                                                               'DOCTOR') THEN 'DOCTOR'
                                    WHEN main.contact_type IN ('FACILITY',
                                                               'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                                    ELSE 'OTHER'
                           END
                  WHEN main.country = 'Italy'
                  AND      main.campaign LIKE '%gipo%' THEN
                           CASE
                                    WHEN main.contact_type IN ('PATIENT',
                                                               'UNKNOWN',
                                                               'STUDENT') THEN 'OTHER'
                                    ELSE 'FACILITY'
                           END
                  WHEN main.country = 'Italy' THEN
                           CASE
                                    WHEN main.contact_type IN ('SECRETARY',
                                                               'DOCTOR') THEN 'DOCTOR'
                                    WHEN main.contact_type IN ('FACILITY',
                                                               'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                                    WHEN main.contact_type IN ('GENERAL PRACTITIONER',
                                                               'GENERAL PRACTITIONER & DOCTOR') THEN 'GP'
                                    ELSE 'OTHER'
                           END
                  WHEN main.country = 'Spain'
                  AND      (
                                    main.campaign LIKE '%clinic-cloud%'
                           OR       lower(main.campaign) LIKE '%-cc-%') THEN
                           CASE
                                    WHEN main.contact_type IN ('PATIENT',
                                                               'UNKNOWN',
                                                               'STUDENT') THEN 'OTHER'
                                    ELSE 'FACILITY'
                           END
                  WHEN main.country = 'Spain' THEN
                           CASE
                                    WHEN main.contact_type IN ('SECRETARY',
                                                               'DOCTOR') THEN 'DOCTOR'
                                    WHEN main.contact_type IN ('FACILITY',
                                                               'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                                    ELSE 'OTHER'
                           END
                  WHEN main.country = 'Mexico' THEN
                           CASE
                                    WHEN main.contact_type IN ('SECRETARY',
                                                               'DOCTOR') THEN 'DOCTOR'
                                    WHEN main.contact_type IN ('FACILITY',
                                                               'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                                    ELSE 'OTHER'
                           END
                  WHEN main.country = 'Turkiye' THEN
                           CASE
                                    WHEN main.contact_type IN ('SECRETARY',
                                                               'DOCTOR') THEN 'DOCTOR'
                                    WHEN main.contact_type IN ('FACILITY',
                                                               'DOCTOR&FACILITY - 2IN1') THEN 'FACILITY'
                                    ELSE 'OTHER'
                           END
                  WHEN main.country = 'Germany' THEN
                           CASE
                                    WHEN main.contact_type IN ('PATIENT',
                                                               'UNKNOWN',
                                                               'MARKETING') THEN 'OTHER'
                                    ELSE 'DOCTOR'
                           END
                  WHEN main.country = 'Chile' THEN
                           CASE
                                    WHEN main.contact_type IN ('DOCTOR',
                                                               'SECRETARY',
                                                               'DOCTOR&FACILITY - 2IN1') THEN 'DOCTOR'
                                    ELSE 'OTHER'
                           END
         END AS target,
         CASE
                  WHEN main.country IN ('Brazil',
                                        'Mexico',
                                        'Chile')
                  AND      main.spec_split IS NULL THEN 'Paramedical'
                  WHEN main.country IN ('Brazil',
                                        'Mexico',
                                        'Chile')
                  AND      main.spec_split IS NOT NULL THEN main.spec_split
                  ELSE NULL
         END AS br_mx_cl_specialisation,
         contact_type,
         sum(mal) AS mals,
         sum(mql) AS mqls
FROM     next main
GROUP BY date,
         country,
         main.hubspot_id,
         main.verified,
         main.campaign,
         main.hsa_net,
         main.content,
         main.deal_month,
         main.product_won,
         country_by_campaign,
         campaign_goal,
         campaign_target,
         campaign_product,
         target,
         br_mx_cl_specialisation,
         contact_type
