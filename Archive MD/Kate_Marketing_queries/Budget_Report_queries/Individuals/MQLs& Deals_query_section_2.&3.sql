WITH
won_deals AS ( --first we select all the valid deals that arrived to Won stage and bought an Individual product(inclusive of e-commerce) - for CVR calculations
    SELECT
        deal.deal_stage,
        deal.deal_id,
        deal.pipeline,
        deal.pipeline_type,
        deal.is_current_stage,
        deal.stage_is_month_new,
        deal.month::DATE AS deal_month,
        deal.hs_contact_id,
        deal.deal_stage_start,
        deal.country,
        deal.segment,
        deal.lead_id,
        live.offer_type,
        MAX(deal.mrr_euro) OVER (PARTITION BY deal.deal_id) AS mrr_euro_final, --this is to avoid duplicating MRR
        MAX(deal.mrr_original_currency) OVER (PARTITION BY deal.deal_id) AS mrr_original_currency
    FROM mart_cust_journey.cj_deal_month deal
    LEFT JOIN dw.hs_deal_live live ON live.hubspot_id = deal.deal_id
    WHERE deal.deal_stage IN ('Closed Won')
      AND deal.pipeline_type IN ('Individual New Sales')
      AND deal.is_current_stage
      AND deal.stage_is_month_new
      AND deal.month >= '2024-01-01'
), --for the purposes of the budget report we only care about 2024

won_deals_ls AS (
    SELECT
        deal.*,
        last_source_so AS frozen_last_source,source_so AS frozen_source,
        ROW_NUMBER() over (PARTITION BY deal.deal_id ORDER BY dw_updated_at DESC) as row
    FROM won_deals deal
    INNER JOIN dw.hs_contact_live_history hsh ON hsh.hubspot_id = deal.hs_contact_id AND hsh.dw_updated_at BETWEEN deal.deal_month AND DATEADD(day, 50, DATE_TRUNC('month',deal.deal_month::DATE))
    QUALIFY row = 1
),

all_mqls AS (
        SELECT
            lcs.contact_id, --1
            hs.email AS hs_email,--2
            contact_facts.country,--3
            hs.source_so,--4
            hs.last_source_so, --5
            hs.last_source_so_at,--6
            CASE WHEN (contact_facts.country = 'Brazil')
                    AND hs.spec_split_test = 'Medical' THEN 'Medical'
                WHEN (contact_facts.country = 'Brazil')
                    AND ( hs.spec_split_test = 'Paramedical'
                    OR hs.spec_split_test IS NULL) THEN 'Paramedical'
                ELSE 'None'
            END AS new_spec, --7
            COALESCE(DATE_TRUNC('month',hs.feegow_lead_at_test),'2015-01-01') as feegow_month, --8
            CASE WHEN lcs.lifecycle_stage = 'MQL' THEN 'MQL' ELSE 'influenced' END AS lifecycle_stage,--9
         -- we calculate what an Individual Doctor MQL means using list rules. except for when there is a deal for DOC or GP product
            CASE WHEN deal.deal_id IS NOT NULL AND deal.offer_type = 'GP [IT]'
                THEN 'GP'
                WHEN deal.deal_id IS NOT NULL
                THEN 'DOCTOR'
                WHEN contact_facts.country IN ('Colombia', 'Mexico', 'Italy', 'Brazil', 'Peru')
                    AND COALESCE(contact_facts.segment,'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY')
                THEN 'DOCTOR'
                WHEN contact_facts.country IN ('Colombia', 'Mexico', 'Italy', 'Brazil')
                    AND COALESCE(contact_facts.segment,'UNKNOWN') IN ('GENERAL PRACTITIONER', 'GENERAL PRACTITIONER & DOCTOR')
                THEN 'GP'
                WHEN contact_facts.country = 'Poland'
                    AND COALESCE(contact_facts.segment,'UNKNOWN') IN ('UNKNOWN','DOCTOR', 'NURSES')
                THEN 'DOCTOR'
                WHEN contact_facts.country = 'Spain'
                    AND COALESCE(contact_facts.segment,'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'NURSES', 'SECRETARY')
                THEN 'DOCTOR'
                WHEN contact_facts.country IN ('Turkey','Turkiye','Argentina')
                    AND COALESCE(contact_facts.segment,'UNKNOWN') NOT IN ('PATIENT', 'STUDENT', 'NURSES')
                THEN 'DOCTOR'
                WHEN contact_facts.country IN ('Chile','Germany')
                    AND COALESCE(contact_facts.segment,'UNKNOWN') NOT IN ('PATIENT','STUDENT')
                THEN 'DOCTOR'
                WHEN contact_facts.country IN ('Colombia', 'Mexico', 'Italy', 'Brazil', 'Poland', 'Spain', 'Peru')
                    AND COALESCE(contact_facts.segment,'UNKNOWN') IN('FACILITY', 'DOCTOR&FACILITY - 2IN1')
                    AND hs.facility___product_recommended__wf LIKE '%Agenda Premium%'  THEN 'DOCTOR'
                WHEN contact_facts.country IN ('Colombia', 'Mexico', 'Italy', 'Brazil', 'Poland', 'Spain', 'Peru')
                    AND COALESCE(contact_facts.segment,'UNKNOWN') IN('FACILITY', 'DOCTOR&FACILITY - 2IN1')
                    AND hs.lead_transferred_to_so LIKE '%Agenda Premium%'
                THEN 'DOCTOR'
                ELSE 'OTHER'
            END AS target,--10
            CASE WHEN (hs_lead_status = 'UNQUALIFIED' AND contact_facts.country = 'Italy') THEN TRUE  --Italy excludes unqualified leads
                WHEN (hs.spec_split_test = 'Bad Paramedical' AND contact_facts.country = 'Brazil') THEN TRUE
                WHEN hs.email  LIKE '%docplanner.com%' OR hs.email IS NULL THEN TRUE ELSE FALSE
            END AS mql_excluded,--11
            hs.facility___product_recommended__wf,--12
            CASE WHEN contact_facts.mql_channel = 'Offline reference' THEN 'active_source'
                WHEN paid.hubspot_id IS NOT NULL THEN 'active_source'
                WHEN contact_facts.mql_channel = 'Event' THEN 'active_source'
                WHEN LOWER(hs.source_so) IN (
                    'free audit',
                    'free audit facility'
                    'conference',
                    'helpline',
                    'livechat',
                    'direct mail',
                    'feegow interested',
                    'buy form',
                    'offer request facility',
                    'offer request',
                    'profile premium upgrade - improve bookings',
                    'offer request cta',
                    'contact form doctor zone',
                    'callpage',
                    'customer reference',
                    'callcenter meetings',
                    'bundle (ca & feegow) offer request',
                    'bundle (ca & feegow) interested [whatsapp]',
                    'integrations',
                    'facebook ad',
                    'whatsApp widget',
                    'telemedicine interested',
                    'website interested',
                    'partnership',
                    'plan 360 offer request',
                    'profile premium upgrade - phone on profile',
                    'sales demo ordered feegow',
                    'demo request saas facility',
                    'free trial feegow',
                    'cliniccloud call',
                    'profile premium upgrade - improve visibility',
                    'gipo callpage',
                    'gipo contact form',
                    'cliniccloud direct email',
                    'discount offer facility',
                    'discount offer',
                    'clinic cloud offer request',
                    'dp phone free audit',
                    'dp phone request',
                    'tuotempo offer request facility',
                    'fc offer request') THEN 'active_source'
                ELSE 'passive_source'
            END AS active_passive, --13
            contact_facts.verified AS verified,--14
            CASE WHEN deal.deal_id is NOT NULL AND pipeline != 'e-commerce' THEN TRUE ELSE FALSE END AS deal_flag_aux,--15
            CASE WHEN  pipeline = 'e-commerce' THEN TRUE ELSE FALSE END AS ecommerce_deal_flag,
            CASE WHEN deal.deal_id IS NOT NULL THEN deal.deal_month ELSE NULL END AS deal_month,--16
            CASE WHEN contact_facts.country IN ('Colombia', 'Mexico', 'Spain', 'Brazil', 'Poland', 'Argentina', 'Chile', 'Peru')
                AND ((deal.frozen_source NOT IN ('basic reference', 'Target tool [DA]', 'Sales Contact'))
                    OR (deal.frozen_last_source NOT IN ('basic reference', 'Target tool [DA]', 'Sales Contact', 'visiting pricing')))
                THEN 'Inbound'
                WHEN contact_facts.country IN ('Turkiye','Germany')
                    AND (deal.frozen_last_source NOT IN ('Target tool [DA]', 'Sales Contact')
                        OR deal.frozen_source NOT IN ('Target tool [DA]', 'Sales Contact'))
                THEN 'Inbound'
                WHEN contact_facts.country = 'Italy'
                    AND (deal.frozen_source NOT IN (
                        'Target tool [DA]',
                        'Sales Contact',
                        'basic reference',
                        'other',
                        'new facility verification',
                        'Massive assignment',
                        'New verification',
                        'other')
                        OR deal.frozen_last_source NOT IN (
                        'Target tool [DA]',
                        'Sales Contact',
                        'basic reference',
                        'Massive assignment',
                        'new facility verification',
                        'New verification',
                        'visiting pricing',
                        'e-commerce abandoned cart',
                        'other'))
                THEN 'Inbound'
                ELSE 'Outbound'
            END AS deal_allocation,--17
            CASE WHEN contact_facts.mql_channel = 'Offline reference' THEN 'Referral'
                WHEN paid.hubspot_id IS NOT NULL THEN 'Paid_direct'
                WHEN contact_facts.mql_channel = 'Event' THEN 'Events_direct'
                                        -- HS latest source known
                WHEN hs_analytics_source = 'SOCIAL_MEDIA'
                    AND LOWER(hs_analytics_source_data_1) IN( 'facebook','instagram','linkedin' )
                THEN 'Organic/Direct'
                WHEN hs_analytics_source = 'PAID_SOCIAL'
                    AND LOWER(hs_analytics_source_data_1) IN( 'facebook','instagram','linkedin' ) THEN 'Paid'
                WHEN hs_analytics_source = 'PAID_SEARCH'
                    AND (LOWER(hs_analytics_source_data_1) IN ( 'yahoo','bing','google')
                        OR LOWER(hs_analytics_source_data_2) LIKE '%_%'
                        OR LOWER(hs_analytics_source_data_1) LIKE '%_%'
                        OR LOWER(hs_analytics_source_data_2) = 'google' ) THEN 'Paid'
                WHEN doctor_facility___marketing_events_tag IS NOT NULL THEN 'Events'
                WHEN hs_analytics_source = 'ORGANIC_SEARCH'
                    AND (LOWER(hs_analytics_source_data_1) IN ('google','yahoo','bing')
                        OR LOWER(hs_analytics_source_data_2) IN ('google','bing','yahoo')) THEN 'Organic/Direct'
                WHEN hs. affiliate_source LIKE '%callcenter%' OR hs.marketing_action_tag2 LIKE '%callcenter%' THEN 'Call Center'
                WHEN hs_analytics_source = 'DIRECT_TRAFFIC' THEN 'Organic/Direct'
                ELSE 'Organic/Direct'
            END AS db_channel_short, --18
            CASE WHEN lcs.lifecycle_stage = 'MQL' AND lcs_is_month_new
                THEN lcs.lifecycle_stage_start
                WHEN lcs.last_marketing_influenced_at IS NOT NULL THEN lcs.last_marketing_influenced_at
                ELSE contact_facts.last_marketing_influenced_at
            END AS lifecycle_stage_start,--19
            CASE WHEN lcs.lifecycle_stage = 'MQL' AND lcs_is_month_new
                THEN lcs.month
                WHEN lcs.last_marketing_influenced_at IS NOT NULL THEN  Date_trunc('month', lcs.last_marketing_influenced_at)
                ELSE Date_trunc('month', contact_facts.last_marketing_influenced_at)
            END AS month, --20
            deal.deal_stage_start,
            sub_brand,
            deal.mrr_euro_final,
            deal.frozen_last_source,
            deal.frozen_source
       FROM mart_cust_journey.cj_lcs_month lcs
       INNER JOIN mart_cust_journey.cj_contact_facts contact_facts
            ON contact_facts.contact_id = lcs.contact_id
                AND contact_facts.lead_id = lcs.lead_id
       LEFT JOIN  dw.hs_contact_live hs
            ON hs.hubspot_id = lcs.contact_id
       LEFT JOIN  won_deals_ls deal
            ON lcs.contact_id = deal.hs_contact_id
       LEFT JOIN  mart_cust_journey.msv_paid_media_campaign_hubspot_2 paid
            ON lcs.contact_id =paid.hubspot_id
                AND lcs.lifecycle_stage = 'MQL'
                AND lcs_is_month_new
                AND lcs.lifecycle_stage_start BETWEEN paid.web_date
                AND paid.web_date::date + interval '30 day'
                AND campaign LIKE '%mql%'
                AND (lcs.month = date_trunc('month',paid.date)
                    OR date_trunc('month', contact_facts.last_marketing_influenced_at) = date_trunc('month',paid.date) )
       WHERE ((lcs.lifecycle_stage   IN ('MQL')
                AND contact_facts.country IN ('Argentina', 'Chile', 'Colombia', 'Spain', 'Mexico', 'Brazil', 'Germany', 'Italy', 'Poland', 'Turkey', 'Turkiye', 'Peru')
                AND lcs_is_month_new)
                  OR (lcs.last_marketing_influenced_at IS NOT NULL  AND contact_facts.country IN ('Argentina', 'Chile', 'Colombia', 'Spain', 'Mexico', 'Brazil', 'Germany', 'Italy', 'Poland', 'Turkey', 'Turkiye', 'Peru'))
            OR (contact_facts.last_marketing_influenced_at IS NOT NULL))),

only_wons AS (
    SELECT
        CAST(deal.hs_contact_id AS bigint) AS contact_id, --1
        contact_facts.email AS hs_email,--2
        deal.country,--3
        hs.source_so,--4
        hs.last_source_so, --5
        hs.last_source_so_at,--6
        CASE WHEN (contact_facts.country = 'Brazil') AND hs.spec_split_test = 'Medical' THEN 'Medical'
            WHEN (contact_facts.country = 'Brazil') AND (hs.spec_split_test = 'Paramedical' OR hs.spec_split_test IS NULL) THEN 'Paramedical'
            ELSE 'None'
        END AS new_spec, --7
        COALESCE(DATE_TRUNC('month',hs.feegow_lead_at_test),'2015-01-01') as feegow_month, --8
        'only_won'::varchar AS lifecycle_stage,--9
        CASE WHEN deal.deal_id IS NOT NULL AND deal.offer_type = 'GP [IT]' THEN 'GP'
            WHEN deal.deal_id IS NOT NULL THEN 'DOCTOR'
            WHEN deal.country IN ('Colombia', 'Mexico', 'Italy', 'Brazil', 'Peru')
                AND COALESCE(deal.segment, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'SECRETARY')
            THEN 'DOCTOR'
            WHEN deal.country IN ('Colombia', 'Mexico', 'Italy', 'Brazil')
                AND COALESCE(deal.segment, 'UNKNOWN') IN ('GENERAL PRACTITIONER', 'GENERAL PRACTITIONER & DOCTOR')
            THEN 'GP'
            WHEN deal.country = 'Poland'
                AND COALESCE(deal.segment, 'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'NURSES')
            THEN 'DOCTOR'
            WHEN deal.country = 'Spain'
                AND COALESCE(deal.segment,'UNKNOWN') IN ('UNKNOWN', 'DOCTOR', 'NURSES', 'SECRETARY')
            THEN 'DOCTOR'
            WHEN deal.country IN ('Turkey', 'Turkiye', 'Argentina')
                AND COALESCE(deal.segment,'UNKNOWN') NOT IN ('PATIENT', 'STUDENT', 'NURSES')
            THEN 'DOCTOR'
            WHEN deal.country IN ('Chile', 'Germany')
                AND COALESCE(deal.segment,'UNKNOWN') NOT IN ('PATIENT', 'STUDENT')
            THEN 'DOCTOR'
            ELSE 'DOCTOR'
        END AS target,--10
        CASE WHEN (hs.spec_split_test = 'Bad Paramedical' and contact_facts.country = 'Brazil') THEN TRUE ELSE FALSE END as mql_excluded,--11
        'Agenda Premium'::varchar AS facility___product_recommended__wf, --12
        CASE  WHEN contact_facts.mql_channel = 'Offline reference' THEN 'active_source'
            WHEN paid.hubspot_id IS NOT NULL THEN 'active_source'
            WHEN contact_facts.mql_channel = 'Event' THEN 'active_source'
            ELSE 'passive_source'
        END AS active_passive, --13
        contact_facts.verified AS verified, --14
        CASE WHEN  pipeline != 'e-commerce' THEN TRUE ELSE FALSE END AS deal_flag_aux, --15
        CASE WHEN  pipeline = 'e-commerce' THEN TRUE ELSE FALSE END AS ecommerce_deal_flag,
        deal.deal_month AS deal_month, --16
        CASE WHEN deal.country IN ('Colombia', 'Mexico', 'Spain', 'Brazil', 'Poland', 'Argentina', 'Chile', 'Peru')
                AND ((deal.frozen_source NOT IN ('basic reference', 'Target tool [DA]', 'Sales Contact'))
                    OR (deal.frozen_last_source NOT IN ('basic reference', 'Target tool [DA]', 'Sales Contact', 'visiting pricing')))
                THEN 'Inbound'
                WHEN deal.country IN ('Turkiye','Germany')
                    AND (deal.frozen_last_source NOT IN ('Target tool [DA]', 'Sales Contact')
                        OR deal.frozen_source NOT IN ('Target tool [DA]', 'Sales Contact'))
                THEN 'Inbound'
                WHEN deal.country = 'Italy'
                    AND (deal.frozen_source NOT IN (
                        'Target tool [DA]',
                        'Sales Contact',
                        'basic reference',
                        'other',
                        'new facility verification',
                        'Massive assignment',
                        'New verification',
                        'other')
                        OR deal.frozen_last_source NOT IN (
                        'Target tool [DA]',
                        'Sales Contact',
                        'basic reference',
                        'Massive assignment',
                        'new facility verification',
                        'New verification',
                        'visiting pricing',
                        'e-commerce abandoned cart',
                        'other'))
                THEN 'Inbound'
                ELSE 'Outbound'
            END AS deal_allocation,--17
        CASE WHEN contact_facts.mql_channel = 'Offline reference' THEN 'Referral'
                WHEN paid.hubspot_id IS NOT NULL THEN 'Paid_direct'
                WHEN contact_facts.mql_channel = 'Event' THEN 'Events_direct'
                                        -- HS latest source known
                WHEN hs_analytics_source = 'SOCIAL_MEDIA'
                    AND LOWER(hs_analytics_source_data_1) IN( 'facebook','instagram','linkedin' )
                THEN 'Organic/Direct'
                WHEN hs_analytics_source = 'PAID_SOCIAL'
                    AND LOWER(hs_analytics_source_data_1) IN( 'facebook','instagram','linkedin' ) THEN 'Paid'
                WHEN hs_analytics_source = 'PAID_SEARCH'
                    AND (LOWER(hs_analytics_source_data_1) IN ( 'yahoo','bing','google')
                        OR LOWER(hs_analytics_source_data_2) LIKE '%_%'
                        OR LOWER(hs_analytics_source_data_1) LIKE '%_%'
                        OR LOWER(hs_analytics_source_data_2) = 'google' ) THEN 'Paid'
                WHEN doctor_facility___marketing_events_tag IS NOT NULL THEN 'Events'
                WHEN hs_analytics_source = 'ORGANIC_SEARCH'
                    AND (LOWER(hs_analytics_source_data_1) IN ('google','yahoo','bing')
                        OR LOWER(hs_analytics_source_data_2) IN ('google','bing','yahoo')) THEN 'Organic/Direct'
                WHEN hs. affiliate_source LIKE '%callcenter%' OR hs.marketing_action_tag2 LIKE '%callcenter%' THEN 'Call Center'
                WHEN hs_analytics_source = 'DIRECT_TRAFFIC' THEN 'Organic/Direct'
                ELSE 'Organic/Direct'
        END AS db_channel_short, --18
        deal.deal_stage_start AS lifecycle_stage_start,--19
        deal.deal_month AS month, --20
        deal.deal_stage_start,
        sub_brand,
        deal.mrr_euro_final,
        deal.frozen_last_source,
        deal.frozen_source
        FROM won_deals_ls deal
        LEFT JOIN mart_cust_journey.cj_contact_facts contact_facts
            ON contact_facts.contact_id = deal.hs_contact_id
                AND contact_facts.lead_id = deal.lead_id
        LEFT JOIN dw.hs_contact_live hs
            ON deal.hs_contact_id = hs.hubspot_id
        LEFT JOIN mart_cust_journey.msv_paid_media_campaign_hubspot_2 paid
            ON deal.hs_contact_id =paid.hubspot_id
                AND deal.deal_stage_start BETWEEN paid.web_date AND paid.web_date::date + interval '30 day'
                AND campaign LIKE '%mql%'
                AND (deal.deal_month >= date_trunc('month',paid.date))
        WHERE (deal.deal_stage IN ('Closed Won')
            AND contact_facts.country IN ('Argentina', 'Chile', 'Colombia', 'Spain', 'Mexico', 'Brazil', 'Germany', 'Italy', 'Poland', 'Turkey', 'Turkiye', 'Peru')
            AND is_current_stage
            AND stage_is_month_new)
            AND pipeline_type = 'Individual New Sales'
            AND channel_sales IN ('Inbound', 'Mixed')
),

total_mqls AS (
    SELECT * FROM all_mqls GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26
    UNION
    SELECT * FROM only_wons GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26)

SELECT *,
    max(db_channel_short) OVER( partition BY contact_id, month) AS max_channel,
    max(active_passive) OVER( partition BY contact_id, month) AS max_ap,
    CASE WHEN deal_allocation = 'Outbound' THEN FALSE
         WHEN  country = 'Turkey' AND deal_stage_start < lifecycle_stage_start::date - interval '1 day'  THEN FALSE
         ELSE deal_flag_aux
       END AS true_deal_flag
FROM total_mqls
WHERE (mql_excluded IS NOT TRUE OR mql_excluded IS NULL) --covering various special conditions
    AND (hs_email NOT LIKE '%docplanner.com%' OR hs_email IS NULL)
    AND NOT (hs_email LIKE '%deleted%' AND (deal_flag_aux IS FALSE AND ecommerce_deal_flag IS FALSE)) --MQLs with "deleted" in the email are not counted but if they reach a deal, should still be counted.
    AND NOT (feegow_month = month AND deal_flag_aux IS FALSE AND ecommerce_deal_flag IS FALSE) --if a contact has been a feegow lead in that month, its excluded - if he bought an Agenda premium deal later we count it
    AND target IN ('DOCTOR','GP')
    AND (((sub_brand LIKE '%Doctoralia%' OR sub_brand LIKE '%MioDottore%' OR sub_brand LIKE '%MioDottore%' OR sub_brand LIKE '%jameda%'
        OR sub_brand LIKE '%DoktorTakvimi%' OR sub_brand LIKE '%ZnanyLekarz%' OR sub_brand IS NULL)  AND  NOT deal_flag_aux) OR deal_flag_aux OR ecommerce_deal_flag)
