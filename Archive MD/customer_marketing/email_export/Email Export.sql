WITH email_data AS (
    SELECT
        from_address,
        subject,
        campaign,
        subscription_type,
        state,
        sent,
        delivered,
        opened,
        clicked,
        replied,
        hard_bounced,
        spam_reports,
        unsubscribed,
        name                        AS email_name,
        CAST(hubspot_id AS VARCHAR) AS internal_hubspot_id,
        hard_bounced                AS not_sent,
        created_at                  AS created_date,
        updated_at                  AS send_date,
        updated_at                  AS updated_date,
        updated_at                  AS last_event,
        'no_wf_associated'          AS associated_workflows,
        dw_created_at               AS inserted_at
    FROM dw.hs_marketing_email
    WHERE EXTRACT(YEAR FROM dw_created_at) >= 2026
      AND "state" LIKE '%PUBLISHED%'

    UNION ALL

    SELECT
        from_address,
        subject,
        campaign,
        subscription_type,
        state,
        sent,
        delivered,
        opened,
        clicked,
        replied,
        hard_bounced,
        spam_reports,
        unsubscribed,
        email_name,
        CAST(internal_hubspot_id AS VARCHAR) AS internal_hubspot_id,
        not_sent,
        created_date,
        send_date,
        updated_date,
        last_event,
        associated_workflows,
        inserted_at
    FROM hubspot_manual_import.marketing_email
    WHERE EXTRACT(YEAR FROM last_event) >= 2026
      AND "state" LIKE '%AUTOMATED%'
),
final AS (
    SELECT
        email_name,
        internal_hubspot_id,
CASE
        WHEN email_name LIKE '% LG %' THEN 'LG'
        ELSE 'CM'
    END AS team,
        CASE
            WHEN email_name LIKE 'ES%' THEN 'Spain'
            WHEN email_name LIKE 'IT%' THEN 'Italy'
            WHEN email_name LIKE 'PL%' THEN 'Poland'
            WHEN email_name LIKE 'MX%' THEN 'Mexico'
            WHEN email_name LIKE 'BR%' THEN 'Brazil'
            WHEN email_name LIKE 'CO%' THEN 'Colombia'
            WHEN email_name LIKE 'CL%' THEN 'Chile'
            WHEN email_name LIKE 'TR%' THEN 'Turkey'
            ELSE NULL
        END AS country,
CASE
        WHEN email_name LIKE '%DOC%' AND email_name NOT LIKE '%FAC%' THEN 'DOC'
        WHEN email_name LIKE '%FAC%' THEN 'FAC'
        WHEN email_name LIKE '%DOC_FAC%' THEN 'DOC_FAC'
        WHEN email_name LIKE '%GP%' THEN 'GP'
        WHEN email_name LIKE '%MMG%' THEN 'GP'
        WHEN email_name LIKE '%DPP%' THEN 'DPP'
        ELSE 'N/A'
    END AS audience,
        CASE
        WHEN email_name LIKE '%ENGAGEMENT%' THEN 'ENGAGEMENT'
        WHEN email_name LIKE '%MQL_GENERATION%' THEN 'MQL_GENERATION'
        WHEN email_name LIKE '%NURTURING%'
          OR email_name LIKE '%PRODUCT_KNOWLEDGE_ADVANCE%'
          OR email_name LIKE '%SCORING_INCREASE%' THEN 'NURTURING'
        WHEN email_name LIKE '% VERIFICATION %' THEN 'VERIFICATION'
        WHEN email_name LIKE '% NEWSLETTER %' THEN 'NEWSLETTER'
        WHEN email_name LIKE '% MARKETPLACE_TRAFFIC %' THEN 'MARKETPLACE_TRAFFIC'
        WHEN email_name LIKE '%CROSS-SELLING_PMS%' THEN 'CROSS-SELLING_PMS'
        WHEN email_name LIKE '%CROSS-SELLING_CA%' THEN 'CROSS-SELLING_CA'
        WHEN email_name LIKE '%CROSS-SELLING_AP%' THEN 'CROSS-SELLING_AP'
        WHEN email_name LIKE '%CROSS-SELLING_NOA%' THEN 'CROSS-SELLING_NOA'
        ELSE 'OTHER'
    END AS campaign_goal,
        send_date,
        sent,
        delivered,
        opened AS opens,
        clicked AS clicks,
        hard_bounced,
        unsubscribed
    FROM email_data
    where email_name similar to '%CM%|%CX%'
    and send_date >= '2026-01-01'
)
SELECT
    *
FROM final
where team = 'CM'
