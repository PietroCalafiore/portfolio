WITH won_deals AS
(
       --first we select all the valid deals that arrived to Won stage and bought an Individual product(inclusive of e-commerce) - for CVR calculations
       SELECT *
       FROM   mart_cust_journey.cj_deal_month deal
       WHERE  deal.deal_stage IN ('Closed Won')
       AND    pipeline_type   IN ('Individual New Sales')
       AND    is_current_stage
       AND    stage_is_month_new
       AND    month >= '2024-01-01' ), --for the purposes of the budget report we only care about 2024
 all_mqls AS(  --here we select All Pure MQLs (lcs=MQL) + influenced leads. in the future only this CTE should be needed
           SELECT     lcs.contact_id, --1
                      hs.email AS hs_email,--2
                      contact_facts.country,--3
                        hs.source_so,--4
                     hs.last_source_so, --5
                     hs.last_source_so_at,--6
                    COALESCE(DATE_TRUNC('month',hs.feegow_lead_at_test),'2015-01-01') as feegow_month, --8
                    CASE
                                 WHEN lcs.lifecycle_stage = 'MQL' THEN 'MQL'
                                 ELSE 'influenced'
                      END AS lifecycle_stage,--9
                      CASE -- we calculate what an Individual Doctor MQL means using list rules. except for when there is a deal for DOC or GP product
                                 WHEN deal.deal_stage IN ('Closed Won')
                                 AND        pipeline_type = 'Individual New Sales'
                                 AND        is_current_stage
                                 AND        stage_is_month_new
                                 AND        COALESCE(contact_facts.segment,'UNKNOWN') IN ('GENERAL PRACTITIONER',
                                                                                          'GENERAL PRACTITIONER & DOCTOR') THEN 'GP'
                                 WHEN deal.deal_stage IN ('Closed Won')
                                 AND        pipeline_type = 'Individual New Sales'
                                 AND        is_current_stage
                                 AND        stage_is_month_new THEN'DOCTOR'
                                 WHEN contact_facts.country IN ('Colombia',
                                                                'Mexico',
                                                                'Italy',
                                                                'Brazil')
                                 AND        COALESCE(contact_facts.segment,'UNKNOWN') IN ('UNKNOWN',
                                                                                          'DOCTOR',
                                                                                          'SECRETARY') THEN 'DOCTOR'
                                 WHEN contact_facts.country IN ('Colombia',
                                                                'Mexico',
                                                                'Italy',
                                                                'Brazil')
                                 AND        COALESCE(contact_facts.segment,'UNKNOWN') IN ('GENERAL PRACTITIONER',
                                                                                          'GENERAL PRACTITIONER & DOCTOR') THEN 'GP'
                                 WHEN contact_facts.country = 'Poland'
                                 AND        COALESCE(contact_facts.segment,'UNKNOWN') IN ('UNKNOWN',
                                                                                          'DOCTOR',
                                                                                          'NURSES') THEN 'DOCTOR'
                                 WHEN contact_facts.country = 'Spain'
                                 AND        COALESCE(contact_facts.segment,'UNKNOWN') IN ('UNKNOWN',
                                                                                          'DOCTOR',
                                                                                          'NURSES',
                                                                                          'SECRETARY') THEN 'DOCTOR'
                                 WHEN contact_facts.country IN ('Turkey',
                                                                'Turkiye',
                                                                'Argentina')
                                 AND        COALESCE(contact_facts.segment,'UNKNOWN') NOT IN ('PATIENT',
                                                                                              'STUDENT',
                                                                                              'NURSES') THEN 'DOCTOR'
                                 WHEN contact_facts.country IN ('Chile',
                                                                'Germany')
                                 AND        COALESCE(contact_facts.segment,'UNKNOWN') NOT IN ('PATIENT',
                                                                                              'STUDENT') THEN 'DOCTOR'
                                 WHEN contact_facts.country IN ('Colombia',
                                                                'Mexico',
                                                                'Italy',
                                                                'Brazil',
                                                                'Poland',
                                                                'Spain')
                                 AND        COALESCE(contact_facts.segment,'UNKNOWN') IN('FACILITY',
                                                                                         'DOCTOR&FACILITY - 2IN1')
                                 AND        hs.facility___product_recommended__wf LIKE '%Agenda Premium%' THEN 'DOCTOR'
                                 ELSE 'OTHER'
                      END                                       AS target,--10
               CASE WHEN (hs_lead_status = 'UNQUALIFIED' and contact_facts.country = 'Italy') THEN TRUE  --Italy excludes unqualified leads
WHEN (hs.spec_split_test = 'Bad Paramedical' and contact_facts.country = 'Brazil') THEN TRUE --Brazil considers certain paramedics to be "Bad" leads, we exclude them from MQLs and Deals
WHEN NOT (sub_brand LIKE '%Doctoralia%' --we only count leads that dont belong exclusively to our sub-brand products. Based on list logic
                                 OR         sub_brand LIKE '%MioDottore%'
                                 OR         sub_brand LIKE '%jameda%'
                                 OR         sub_brand LIKE '%DoktorTakvimi%'
                                 OR         sub_brand LIKE '%ZnanyLekarz%'
                                 OR         sub_brand IS NULL )    THEN TRUE
WHEN hs.email  LIKE '%docplanner.com%'
       OR     hs.email IS NULL THEN TRUE ELSE FALSE
END AS mql_excluded,--11
                        CASE WHEN deal.deal_id is NOT NULL THEN true
                                 ELSE false
                      END AS deal_flag_aux,--15
                      CASE WHEN deal.deal_id is NOT NULL THEN deal.month
                                 ELSE NULL
                      END AS deal_month,--16
    CASE
                                 WHEN contact_facts.country IN ('Colombia',
                                                                'Mexico',
                                                                'Spain',
                                                                'Brazil',
                                                                'Poland',
                                                                'Argentina',
                                                                'Chile')
                                 AND        hs.source_so IS NULL THEN 'Outbound'
                                 WHEN contact_facts.country IN ('Colombia',
                                                                'Mexico',
                                                                'Spain',
                                                                'Brazil',
                                                                'Poland',
                                                                'Argentina')
                                 AND        hs.source_so IS NOT NULL
                                 AND        hs.source_so NOT IN ('basic reference',
                                                                 'Target tool [DA]',
                                                                 'Sales Contact') THEN 'Inbound'
                                 WHEN contact_facts.country IN ('Colombia',
                                                                'Mexico',
                                                                'Spain',
                                                                'Brazil',
                                                                'Poland',
                                                                'Argentina',
                                                                'Chile')
                                 AND        hs.source_so NOT IN ('basic reference',
                                                                 'Target tool [DA]',
                                                                 'Sales Contact')
                                 AND        hs.last_source_so NOT IN ('basic reference',
                                                                      'Target tool [DA]',
                                                                      'Sales Contact') THEN 'Inbound'
                                 WHEN contact_facts.country IN ('Colombia',
                                                                'Mexico',
                                                                'Spain',
                                                                'Brazil',
                                                                'Poland',
                                                                'Argentina',
                                                                'Chile')
                                 AND        hs.source_so IN ('basic reference',
                                                             'Target tool [DA]',
                                                             'Sales Contact')
                                 AND        hs.last_source_so NOT IN ('basic reference',
                                                                      'visiting pricing',
                                                                      'Target tool [DA]',
                                                                      'Sales Contact') THEN 'Inbound'
                                 WHEN contact_facts.country = 'Turkiye'
                                 AND        hs.last_source_so IS NOT NULL
                                 AND        hs.last_source_so NOT IN ('Target tool [DA]',
                                                                      'Sales Contact') THEN 'Inbound'
                                 WHEN contact_facts.country IN ('Turkiye',
                                                                'Germany')
                                 AND        hs.source_so IS NOT NULL
                                 AND        hs.source_so NOT IN ('Target tool [DA]',
                                                                 'Sales Contact') THEN 'Inbound'
                                 WHEN contact_facts.country = 'Germany'
                                 AND        hs.last_source_so NOT IN ('Target tool [DA]',
                                                                      'Sales Contact')
                                 AND        (
                                                       hs.source_so IN ('Target tool [DA]',
                                                                        'Sales Contact')
                                            OR         hs.source_so IS NULL) THEN 'Inbound'
                                 WHEN contact_facts.country = 'Italy'
                                 AND        hs.source_so IS NOT NULL
                                 AND        hs.source_so NOT IN ('Target tool [DA]',
                                                                 'Sales Contact',
                                                                 'basic reference',
                                                                 'Massive assignment',
                                                                 'New verification','other') THEN 'Inbound'
                                 WHEN contact_facts.country = 'Italy'
                                 AND        hs.source_so IN ('Target tool [DA]',
                                                             'Sales Contact',
                                                             'basic reference',
                                                             'Massive assignment',
                                                             'New verification','other')
                                 AND        hs.last_source_so NOT IN ('Target tool [DA]',
                                                                      'Sales Contact',
                                                                      'basic reference',
                                                                      'Massive assignment',
                                                                      'New verification', 'other') THEN 'Inbound'
                      END AS deal_allocation, --27  --we exclude deals of certain sources based on list. We use the last source available (hs_contact_live data) to replicate list functionality
                      CASE
                                 WHEN lcs.lifecycle_stage = 'MQL'
                                 AND        lcs_is_month_new THEN lcs.lifecycle_stage_start
                                 ELSE contact_facts.last_marketing_influenced_at
                      END AS lifecycle_stage_start,--19
                      CASE
                                 WHEN lcs.lifecycle_stage = 'MQL'
                                 AND        lcs_is_month_new THEN lcs.month
                                 ELSE Date_trunc('month', contact_facts.last_marketing_influenced_at)
                      END AS month, --20
                            deal.deal_stage_start
           FROM       mart_cust_journey.cj_lcs_month lcs
           INNER JOIN mart_cust_journey.cj_contact_facts contact_facts
           ON         contact_facts.contact_id = lcs.contact_id
           AND        contact_facts.lead_id = lcs.lead_id
           LEFT JOIN  dw.hs_contact_live hs
           ON         hs.hubspot_id = lcs.contact_id
           LEFT JOIN  won_deals deal
           ON         lcs.contact_id = deal.hs_contact_id
           WHERE      ((
                                            lcs.lifecycle_stage   IN ('MQL')
                                 AND        contact_facts.country IN ('Argentina',
                                                                      'Chile',
                                                                      'Colombia',
                                                                      'Spain',
                                                                      'Mexico',
                                                                      'Brazil',
                                                                      'Germany',
                                                                      'Italy',
                                                                      'Poland',
                                                                      'Turkey',
                                                                      'Turkiye')
                                 AND        lcs_is_month_new)
                      OR         (
                                            contact_facts.last_marketing_influenced_at IS NOT NULL
                                 AND        contact_facts.country IN ('Argentina',
                                                                      'Chile',
                                                                      'Colombia',
                                                                      'Spain',
                                                                      'Mexico',
                                                                      'Brazil',
                                                                      'Germany',
                                                                      'Italy',
                                                                      'Poland',
                                                                      'Turkey',
                                                                      'Turkiye')))
          
           ),
    influenced_wo_pre_history AS (
    SELECT lcs.contact_id, --1
                                         hs.email, --2
                                         contact_facts.country,--3
                                          hs.source_so,--4
                                         hs.last_source_so, --5
                                         hs.last_source_so_at, --6
                                         COALESCE(DATE_TRUNC('month',hs.feegow_lead_at_test),'2015-01-01') as feegow_month, --8
                    'influenced'::varchar AS lifecycle_stage,--9
                      CASE -- we calculate what an Individual Doctor MQL means using list rules. except for when there is a deal for DOC or GP product
                                 WHEN deal.deal_stage IN ('Closed Won')
                                 AND        pipeline_type = 'Individual New Sales'
                                 AND        is_current_stage
                                 AND        stage_is_month_new
                                 AND        COALESCE(contact_facts.segment,'UNKNOWN') IN ('GENERAL PRACTITIONER',
                                                                                          'GENERAL PRACTITIONER & DOCTOR') THEN 'GP'
                                 WHEN deal.deal_stage IN ('Closed Won')
                                 AND        pipeline_type = 'Individual New Sales'
                                 AND        is_current_stage
                                 AND        stage_is_month_new THEN'DOCTOR'
                                 WHEN contact_facts.country IN ('Colombia',
                                                                'Mexico',
                                                                'Italy',
                                                                'Brazil')
                                 AND        COALESCE(contact_facts.segment,'UNKNOWN') IN ('UNKNOWN',
                                                                                          'DOCTOR',
                                                                                          'SECRETARY') THEN 'DOCTOR'
                                 WHEN contact_facts.country IN ('Colombia',
                                                                'Mexico',
                                                                'Italy',
                                                                'Brazil')
                                 AND        COALESCE(contact_facts.segment,'UNKNOWN') IN ('GENERAL PRACTITIONER',
                                                                                          'GENERAL PRACTITIONER & DOCTOR') THEN 'GP'
                                 WHEN contact_facts.country = 'Poland'
                                 AND        COALESCE(contact_facts.segment,'UNKNOWN') IN ('UNKNOWN',
                                                                                          'DOCTOR',
                                                                                          'NURSES') THEN 'DOCTOR'
                                 WHEN contact_facts.country = 'Spain'
                                 AND        COALESCE(contact_facts.segment,'UNKNOWN') IN ('UNKNOWN',
                                                                                          'DOCTOR',
                                                                                          'NURSES',
                                                                                          'SECRETARY') THEN 'DOCTOR'
                                 WHEN contact_facts.country IN ('Turkey',
                                                                'Turkiye',
                                                                'Argentina')
                                 AND        COALESCE(contact_facts.segment,'UNKNOWN') NOT IN ('PATIENT',
                                                                                              'STUDENT',
                                                                                              'NURSES') THEN 'DOCTOR'
                                 WHEN contact_facts.country IN ('Chile',
                                                                'Germany')
                                 AND        COALESCE(contact_facts.segment,'UNKNOWN') NOT IN ('PATIENT',
                                                                                              'STUDENT') THEN 'DOCTOR'
                                 WHEN contact_facts.country IN ('Colombia',
                                                                'Mexico',
                                                                'Italy',
                                                                'Brazil',
                                                                'Poland',
                                                                'Spain')
                                 AND        COALESCE(contact_facts.segment,'UNKNOWN') IN('FACILITY',
                                                                                         'DOCTOR&FACILITY - 2IN1')
                                 AND        hs.facility___product_recommended__wf LIKE '%Agenda Premium%' THEN 'DOCTOR'
                                 ELSE 'OTHER'
                      END                                       AS target,--10
             CASE
                                             WHEN (hs_lead_status = 'UNQUALIFIED' and contact_facts.country = 'Italy')
                                                 THEN TRUE--Italy excludes unqualified leads
                 WHEN (hs.spec_split_test = 'Bad Paramedical' and contact_facts.country = 'Brazil') THEN TRUE
                                             WHEN NOT (sub_brand LIKE '%Doctoralia%'
                                                 OR sub_brand LIKE '%MioDottore%'
                                                 OR sub_brand LIKE '%jameda%'
                                                 OR sub_brand LIKE '%DoktorTakvimi%'
                                                 OR sub_brand LIKE '%ZnanyLekarz%'
                                                 OR sub_brand IS NULL) THEN TRUE
                                             WHEN hs.email LIKE '%docplanner.com%'
                                                 OR hs.email IS NULL THEN TRUE
                                             END                                      AS mql_excluded, --12
    CASE
                                             WHEN deal.deal_id is NOT NULL THEN true
                                             ELSE false
                                             END                                      AS deal_flag_aux, --15
                                         CASE
                                             WHEN deal.deal_id is NOT NULL THEN deal.month
                                             ELSE NULL
                                             END                                      AS deal_month,--16
                                         CASE
                                             WHEN contact_facts.country IN ('Colombia',
                                                                            'Mexico',
                                                                            'Spain',
                                                                            'Brazil',
                                                                            'Poland',
                                                                            'Argentina',
                                                                            'Chile')
                                                 AND hs.source_so IS NULL THEN 'Outbound'
                                             WHEN contact_facts.country IN ('Colombia',
                                                                            'Mexico',
                                                                            'Spain',
                                                                            'Brazil',
                                                                            'Poland',
                                                                            'Argentina')
                                                 AND hs.source_so IS NOT NULL
                                                 AND hs.source_so NOT IN ('basic reference',
                                                                          'Target tool [DA]',
                                                                          'Sales Contact') THEN 'Inbound'
                                             WHEN contact_facts.country IN ('Colombia',
                                                                            'Mexico',
                                                                            'Spain',
                                                                            'Brazil',
                                                                            'Poland',
                                                                            'Argentina',
                                                                            'Chile')
                                                 AND hs.source_so NOT IN ('basic reference',
                                                                          'Target tool [DA]',
                                                                          'Sales Contact')
                                                 AND hs.last_source_so NOT IN ('basic reference',
                                                                               'Target tool [DA]',
                                                                               'Sales Contact') THEN 'Inbound'
                                             WHEN contact_facts.country IN ('Colombia',
                                                                            'Mexico',
                                                                            'Spain',
                                                                            'Brazil',
                                                                            'Poland',
                                                                            'Argentina',
                                                                            'Chile')
                                                 AND hs.source_so IN ('basic reference',
                                                                      'Target tool [DA]',
                                                                      'Sales Contact')
                                                 AND hs.last_source_so NOT IN ('basic reference',
                                                                               'visiting pricing',
                                                                               'Target tool [DA]',
                                                                               'Sales Contact') THEN 'Inbound'
                                             WHEN contact_facts.country = 'Turkiye'
                                                 AND hs.last_source_so IS NOT NULL
                                                 AND hs.last_source_so NOT IN ('Target tool [DA]',
                                                                               'Sales Contact') THEN 'Inbound'
                                             WHEN contact_facts.country IN ('Turkiye',
                                                                            'Germany')
                                                 AND hs.source_so IS NOT NULL
                                                 AND hs.source_so NOT IN ('Target tool [DA]',
                                                                          'Sales Contact') THEN 'Inbound'
                                             WHEN contact_facts.country = 'Germany'
                                                 AND hs.last_source_so NOT IN ('Target tool [DA]',
                                                                               'Sales Contact')
                                                 AND (
                                                      hs.source_so IN ('Target tool [DA]',
                                                                       'Sales Contact')
                                                          OR hs.source_so IS NULL) THEN 'Inbound'
                                             WHEN contact_facts.country = 'Italy'
                                                 AND hs.source_so IS NOT NULL
                                                 AND hs.source_so NOT IN ('Target tool [DA]',
                                                                          'Sales Contact',
                                                                          'basic reference',
                                                                          'Massive assignment',
                                                                          'New verification','other') THEN 'Inbound'
                                             WHEN contact_facts.country = 'Italy'
                                                 AND hs.source_so IN ('Target tool [DA]',
                                                                      'Sales Contact',
                                                                      'basic reference',
                                                                      'Massive assignment',
                                                                      'New verification','other')
                                                 AND hs.last_source_so NOT IN ('Target tool [DA]',
                                                                               'Sales Contact',
                                                                               'basic reference',
                                                                               'Massive assignment',
                                                                               'New verification','other') THEN 'Inbound'
                                             END                                      AS deal_allocation, --17

                                         marketing_influenced_at                      AS lifecycle_stage_start, --19
                                         date_trunc('month', marketing_influenced_at) AS month,--20 
                        deal.deal_stage_start
                                  FROM mart_cust_journey.cj_lcs_month lcs
                                           INNER JOIN mart_cust_journey.cj_contact_facts contact_facts
                                                      ON contact_facts.contact_id = lcs.contact_id
                                                          AND contact_facts.lead_id = lcs.lead_id
                                           LEFT JOIN dw.hs_contact_live hs
                                                     ON lcs.contact_id = hs.hubspot_id
                                           LEFT JOIN won_deals deal
                                                     ON lcs.contact_id = deal.hs_contact_id
                                  WHERE marketing_influenced_at IS NOT NULL
                                    AND contact_facts.country IN ('Argentina',
                                                                  'Chile',
                                                                  'Colombia',
                                                                  'Spain',
                                                                  'Mexico',
                                                                  'Brazil',
                                                                  'Germany',
                                                                  'Italy',
                                                                  'Poland',
                                                                  'Turkey',
                                                                  'Turkiye')
    )
                                                                  ,

    only_wons AS(
           SELECT    cast(deal.hs_contact_id AS bigint), --1
                     contact_facts.email AS hs_email,--2
                    deal.country,--3
                      hs.source_so,--4
                     hs.last_source_so, --5
                     hs.last_source_so_at,--6
            COALESCE(DATE_TRUNC('month',hs.feegow_lead_at_test),'2015-01-01') as feegow_month, --8
           'only_won'::varchar AS lifecycle_stage,--9
                     CASE
                               WHEN deal.deal_stage IN ('Closed Won')
                               AND       pipeline_type = 'Individual New Sales'
                               AND       is_current_stage
                               AND       stage_is_month_new
                               AND       COALESCE(contact_facts.segment,'UNKNOWN') IN ('GENERAL PRACTITIONER',
                                                                                       'GENERAL PRACTITIONER & DOCTOR') THEN 'GP'
                               WHEN deal.deal_stage IN ('Closed Won')
                               AND       pipeline_type = 'Individual New Sales'
                               AND       is_current_stage
                               AND       stage_is_month_new THEN'DOCTOR'
                               WHEN deal.country IN ('Colombia',
                                                     'Mexico',
                                                     'Italy',
                                                     'Brazil')
                               AND       COALESCE(deal.segment,'UNKNOWN') IN ('UNKNOWN',
                                                                              'DOCTOR',
                                                                              'SECRETARY') THEN 'DOCTOR'
                               WHEN deal.country IN ('Colombia',
                                                     'Mexico',
                                                     'Italy',
                                                     'Brazil')
                               AND       COALESCE(deal.segment,'UNKNOWN') IN ('GENERAL PRACTITIONER',
                                                                              'GENERAL PRACTITIONER & DOCTOR') THEN 'GP'
                               WHEN deal.country = 'Poland'
                               AND       COALESCE(deal.segment,'UNKNOWN') IN ('UNKNOWN',
                                                                              'DOCTOR',
                                                                              'NURSES') THEN 'DOCTOR'
                               WHEN deal.country = 'Spain'
                               AND       COALESCE(deal.segment,'UNKNOWN') IN ('UNKNOWN',
                                                                              'DOCTOR',
                                                                              'NURSES',
                                                                              'SECRETARY') THEN 'DOCTOR'
                               WHEN deal.country IN ('Turkey',
                                                     'Turkiye',
                                                     'Argentina')
                               AND       COALESCE(deal.segment,'UNKNOWN') NOT IN ('PATIENT',
                                                                                  'STUDENT',
                                                                                  'NURSES') THEN 'DOCTOR'
                               WHEN deal.country IN ('Chile',
                                                     'Germany')
                               AND       COALESCE(deal.segment,'UNKNOWN') NOT IN ('PATIENT',
                                                                                  'STUDENT') THEN 'DOCTOR'
                               ELSE 'DOCTOR'
                     END                              AS target,--10
             CASE WHEN (hs.spec_split_test = 'Bad Paramedical' and contact_facts.country = 'Brazil') THEN TRUE ELSE FALSE END as mql_excluded,--11
           TRUE AS deal_flag_aux, --15
                      deal.month
                             AS deal_month, --16
           CASE
                               WHEN contact_facts.country IN ('Colombia',
                                                              'Mexico',
                                                              'Spain',
                                                              'Brazil',
                                                              'Poland',
                                                              'Argentina',
                                                              'Chile')
                               AND       hs.source_so IS NULL THEN 'Outbound'
                               WHEN contact_facts.country IN ('Colombia',
                                                              'Mexico',
                                                              'Spain',
                                                              'Brazil',
                                                              'Poland',
                                                              'Argentina')
                               AND       hs.source_so IS NOT NULL
                               AND       hs.source_so NOT IN ('basic reference',
                                                              'Target tool [DA]',
                                                              'Sales Contact') THEN 'Inbound'
                               WHEN contact_facts.country IN ('Colombia',
                                                              'Mexico',
                                                              'Spain',
                                                              'Brazil',
                                                              'Poland',
                                                              'Argentina',
                                                              'Chile')
                               AND       hs.source_so NOT IN ('basic reference',
                                                              'Target tool [DA]',
                                                              'Sales Contact')
                               AND       hs.last_source_so NOT IN ('basic reference',
                                                                   'Target tool [DA]',
                                                                   'Sales Contact') THEN 'Inbound'
                               WHEN contact_facts.country IN ('Colombia',
                                                              'Mexico',
                                                              'Spain',
                                                              'Brazil',
                                                              'Poland',
                                                              'Argentina',
                                                              'Chile')
                               AND       hs.source_so IN ('basic reference',
                                                          'Target tool [DA]',
                                                          'Sales Contact')
                               AND       hs.last_source_so NOT IN ('basic reference',
                                                                   'visiting pricing',
                                                                   'Target tool [DA]',
                                                                   'Sales Contact') THEN 'Inbound'
                               WHEN contact_facts.country = 'Turkiye'
                               AND       hs.last_source_so IS NOT NULL
                               AND       hs.last_source_so NOT IN ('Target tool [DA]',
                                                                   'Sales Contact') THEN 'Inbound'
                               WHEN contact_facts.country IN ('Turkiye',
                                                              'Germany')
                               AND       hs.source_so IS NOT NULL
                               AND       hs.source_so NOT IN ('Target tool [DA]',
                                                              'Sales Contact') THEN 'Inbound'
                               WHEN contact_facts.country = 'Germany'
                               AND       hs.last_source_so NOT IN ('Target tool [DA]',
                                                                   'Sales Contact')
                               AND       (
                                                   hs.source_so IN ('Target tool [DA]',
                                                                    'Sales Contact')
                                         OR        hs.source_so IS NULL) THEN 'Inbound'
                               WHEN contact_facts.country = 'Italy'
                               AND       hs.source_so IS NOT NULL
                               AND       hs.source_so NOT IN ('Target tool [DA]',
                                                              'Sales Contact',
                                                              'basic reference',
                                                              'Massive assignment',
                                                              'New verification','other') THEN 'Inbound'
                               WHEN contact_facts.country = 'Italy'
                               AND       hs.source_so IN ('Target tool [DA]',
                                                          'Sales Contact',
                                                          'basic reference',
                                                          'Massive assignment',
                                                          'New verification','other')
                               AND       hs.last_source_so NOT IN ('Target tool [DA]',
                                                                   'Sales Contact',
                                                                   'basic reference',
                                                                   'Massive assignment',
                                                                   'New verification','other') THEN 'Inbound'
                     END AS deal_allocation,--17
                     deal.deal_stage_start AS lifecycle_stage_start,--19
                     deal.month            AS month, --20
                deal.deal_stage_start
           FROM      won_deals deal
           LEFT JOIN mart_cust_journey.cj_contact_facts contact_facts
           ON        contact_facts.contact_id = deal.hs_contact_id
           AND       contact_facts.lead_id = deal.lead_id
           LEFT JOIN dw.hs_contact_live hs
           ON        deal.hs_contact_id = hs.hubspot_id
           WHERE     (
                               deal.deal_stage       IN ('Closed Won')
                     AND       contact_facts.country IN ('Argentina',
                                                         'Chile',
                                                         'Colombia',
                                                         'Spain',
                                                         'Mexico',
                                                         'Brazil',
                                                         'Germany',
                                                         'Italy',
                                                         'Poland',
                                                         'Turkey',
                                                         'Turkiye')
                     AND       is_current_stage
                     AND       stage_is_month_new )
           AND       pipeline_type = 'Individual New Sales'
           AND       channel_sales IN('Inbound',
                                      'Mixed')
    ),
    total_mqls AS(
        select * from all_mqls group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
        UNION
        select * from influenced_wo_pre_history group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
        UNION
        select * from only_wons group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16)

SELECT *,CASE
              WHEN deal_allocation = 'Outbound' THEN false
                                WHEN  deal_stage_start <lifecycle_stage_start THEN FALSE
              ELSE deal_flag_aux
       END AS true_deal_flag
FROM   total_mqls
WHERE  (mql_excluded IS NOT TRUE OR mql_excluded IS NULL) --covering various special conditions
AND    (
              hs_email NOT LIKE '%docplanner.com%'
       OR     hs_email IS NULL)
AND    NOT (
              hs_email LIKE '%deleted%'
       AND    deal_flag_aux IS false) --MQLs with "deleted" in the email are not counted but if they reach a deal, should still be counted.
AND NOT (feegow_month = month AND deal_flag_aux IS false) --if a contact has been a feegow lead in that month, its excluded - if he bought an Agenda premium deal later we count it
and target IN ('DOCTOR','GP')
