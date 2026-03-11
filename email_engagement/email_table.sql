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
    name AS email_name,
    CAST(hubspot_id AS VARCHAR) AS internal_hubspot_id,
    hard_bounced AS not_sent,
    created_at AS created_date,
    updated_at AS send_date,
    updated_at AS updated_date,
    updated_at AS last_event,
    'no_wf_associated' AS associated_workflows,
    dw_created_at AS inserted_at
FROM
    dw.hs_marketing_email
WHERE
    EXTRACT(YEAR FROM dw_created_at) >= '2024'
    AND state LIKE '%PUBLISHED%'

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
FROM
    hubspot_manual_import.marketing_email
WHERE
    EXTRACT(YEAR FROM last_event) >= '2024'
    AND state LIKE '%AUTOMATED%'
