WITH base_events AS (
    SELECT
        h.hubspot_id,
        REGEXP_REPLACE(h.doctor_facility___marketing_events_tag, '_MAL|_MQL$', '') AS base_event_tag,
        MAX(
            CASE
                WHEN h.doctor_facility___marketing_events_tag LIKE '%_MAL%'
                    THEN h.doctor_facility___marketing_events_tag
            END
        ) AS mal_tag,
        MAX(
            CASE
                WHEN h.doctor_facility___marketing_events_tag LIKE '%_MQL%'
                    THEN h.doctor_facility___marketing_events_tag
            END
        ) AS mql_tag,
        MIN(DATE(h.mktg_events_tag_at)) AS event_tag_date,
        l.contact_type_segment_test AS segment,
        l.country
    FROM dw.hs_contact_live_history AS h
    INNER JOIN dw.hs_contact_live AS l
        ON h.hubspot_id = l.hubspot_id
    WHERE
        REGEXP_REPLACE(h.doctor_facility___marketing_events_tag, '_MAL|_MQL$', '') IS NOT NULL
        AND l.is_deleted != 'Yes'
    GROUP BY
        h.hubspot_id,
        base_event_tag,
        l.contact_type_segment_test,
        l.country
),

ranked AS (
    SELECT
        b.*,
        ROW_NUMBER() OVER (
            PARTITION BY b.hubspot_id
            ORDER BY b.event_tag_date DESC
        ) AS rn
    FROM base_events AS b
),

clean_event_contact_table AS (
    SELECT
        hubspot_id,
        base_event_tag,
        mal_tag,
        mql_tag,
        event_tag_date,
        segment,
        country
    FROM ranked
    WHERE rn = 1
),

commercial_date AS (
    SELECT
        h.hubspot_id,
        MIN(DATE(h.dw_updated_at)) AS commercial_date
    FROM clean_event_contact_table AS cet
    LEFT JOIN dw.hs_contact_live_history AS h
        ON cet.hubspot_id = h.hubspot_id
    WHERE h.member_customers_list = 'Yes'
    GROUP BY h.hubspot_id
),

attendees_table AS (
    SELECT
        cet.hubspot_id,
        cet.segment,
        cet.country,
        cet.base_event_tag,
        cet.event_tag_date,
        cet.mal_tag,
        cet.mql_tag,
        CASE
            WHEN cd.commercial_date IS NOT NULL
                 AND cd.commercial_date < cet.event_tag_date THEN 'existing_customer'
            ELSE 'mktg_database'
        END AS contact_type
    FROM clean_event_contact_table AS cet
    LEFT JOIN commercial_date AS cd
        ON cet.hubspot_id = cd.hubspot_id
),

event_mqls AS (
    SELECT DISTINCT
        cmm.contact_id AS hubspot_id,
        DATE(cmm.mql_month) AS month,
        cmm.mql_product
    FROM clean_event_contact_table AS cet
    LEFT JOIN mart_cust_journey.inbound_all_mqls AS cmm
        ON cet.hubspot_id = cmm.contact_id
            AND (
                DATE_TRUNC('month', cmm.mql_month) = DATE_TRUNC('month', cet.event_tag_date)
                OR DATE_TRUNC('month', cmm.mql_month) = DATE_TRUNC('month', ADD_MONTHS(cet.event_tag_date, 1))
            )
    WHERE
        cmm.contact_id IS NOT NULL
        AND cmm.mql_last_touch_channel = 'Events'
),

event_ind_lg_deal_id AS (
    SELECT DISTINCT
        cmm.contact_id AS hubspot_id,
        cmm.deal_id,
        cet.event_tag_date
    FROM clean_event_contact_table AS cet
    LEFT JOIN mart_cust_journey.cj_mqls_monthly AS cmm
        ON cet.hubspot_id = cmm.contact_id
            AND DATE_TRUNC('month', cmm.deal_month)
            BETWEEN DATE_TRUNC('month', ADD_MONTHS(cet.event_tag_date, -1))
            AND DATE_TRUNC('month', ADD_MONTHS(cet.event_tag_date, 1))
    WHERE
        cmm.contact_id IS NOT NULL
        AND cmm.lifecycle_stage = 'only_won'
        AND cmm.db_channel_short IN ('Events', 'Events_direct')
),

event_fac_lg_deal_id AS (
    SELECT DISTINCT
        cmm.contact_id AS hubspot_id,
        cmm.deal_id,
        cet.event_tag_date
    FROM clean_event_contact_table AS cet
    LEFT JOIN mart_cust_journey.cj_mqls_monthly_clinics AS cmm
        ON cet.hubspot_id = cmm.contact_id
            AND DATE_TRUNC('month', cmm.deal_month)
            BETWEEN DATE_TRUNC('month', ADD_MONTHS(cet.event_tag_date, -1))
            AND DATE_TRUNC('month', ADD_MONTHS(cet.event_tag_date, 1))
    WHERE
        cmm.contact_id IS NOT NULL
        AND cmm.lifecycle_stage = 'only_won'
        AND cmm.db_channel_short IN ('Events', 'Events_direct')
),

event_noa_lg_deal_id AS (
    SELECT DISTINCT
        noa.hubspot_id::BIGINT AS hubspot_id,
        noa.deal_id,
        cet.event_tag_date
    FROM clean_event_contact_table AS cet
    LEFT JOIN mart_cust_journey.noa_marketing_kpis_cm_lg_combined AS noa
        ON cet.hubspot_id = noa.hubspot_id
    WHERE
        noa.deal_create_date BETWEEN DATEADD(DAY, -30, cet.event_tag_date)
        AND DATEADD(DAY, 30, cet.event_tag_date)
),

event_docfac_customer_deal_id AS (
    SELECT
        at.hubspot_id,
        facts.opportunity_id AS deal_id,
        at.event_tag_date
    FROM attendees_table AS at
    LEFT JOIN mart_cust_journey.cj_opportunity_facts AS facts
        ON at.hubspot_id = facts.hs_contact_id
    WHERE
        at.contact_type = 'existing_customer'
        AND LOWER(facts.current_stage) LIKE '%won%'
        AND facts.createdate BETWEEN DATE_TRUNC('month', ADD_MONTHS(at.event_tag_date, -1))
        AND DATE_TRUNC('month', ADD_MONTHS(at.event_tag_date, 1))
),

event_deals_id AS (
    SELECT * FROM event_ind_lg_deal_id
    UNION ALL
    SELECT * FROM event_fac_lg_deal_id
    UNION ALL
    SELECT * FROM event_noa_lg_deal_id
    UNION ALL
    SELECT * FROM event_docfac_customer_deal_id
),

deals_info AS (
    SELECT
        deal.hubspot_id,
        deal.deal_id,
        deal.event_tag_date,
        CASE
            WHEN facts.createdate BETWEEN DATEADD(DAY, -45, deal.event_tag_date)
                AND DATEADD(DAY, -1, deal.event_tag_date)
                THEN 'influenced_deal'
            ELSE 'event_deal'
        END AS deal_type,
        facts.pipeline,
        facts.product,
        facts.mrr_euro
    FROM event_deals_id AS deal
    LEFT JOIN mart_cust_journey.cj_opportunity_facts AS facts
        ON deal.deal_id = facts.opportunity_id
    WHERE
        facts.createdate BETWEEN DATEADD(DAY, -45, deal.event_tag_date)
        AND DATEADD(DAY, 30, deal.event_tag_date)
),

final_table AS (
    SELECT DISTINCT
        at.hubspot_id,
        at.base_event_tag,
        at.mal_tag,
        at.mql_tag,
        CASE WHEN mqls.hubspot_id IS NOT NULL THEN 1 ELSE 0 END AS mql,
        mqls.month,
        mqls.mql_product,
        at.event_tag_date,
        at.segment,
        at.country,
        at.contact_type,
        di.deal_id,
        di.deal_type,
        di.pipeline,
        di.product,
        di.mrr_euro
    FROM attendees_table AS at
    LEFT JOIN event_mqls AS mqls
        ON at.hubspot_id = mqls.hubspot_id
            AND (
                DATE_TRUNC('month', at.event_tag_date) = mqls.month
                OR DATE_TRUNC('month', ADD_MONTHS(at.event_tag_date, 1)) = mqls.month
            )
    LEFT JOIN deals_info AS di
        ON at.hubspot_id = di.hubspot_id
            AND at.event_tag_date = di.event_tag_date
)

SELECT
    hubspot_id,
    base_event_tag,
    mal_tag,
    mql_tag,
    mql,
    month,
    mql_product,
    event_tag_date,
    segment,
    country,
    contact_type,
    deal_id,
    deal_type,
    pipeline,
    product,
    mrr_euro
FROM final_table
