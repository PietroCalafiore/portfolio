WITH monthly_snapshots AS (
    SELECT
        flow_id,
        name,
        DATE_TRUNC('month', dw_updated_at) AS month,
        enrolled_contacts,
        completed_contacts,
        succeeded_contacts,
        dw_updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY name, DATE_TRUNC('month', dw_updated_at)
            ORDER BY dw_updated_at DESC
        ) AS rn
    FROM dw.hs_workflow_history WHERE dw_updated_at > '2025-01-01'
    ORDER BY flow_id ASC, name ASC, month DESC
),

latest_monthly AS (
    SELECT
        flow_id,
        name,
        DATE(month) AS month,
        enrolled_contacts,
        completed_contacts,
        succeeded_contacts
    FROM monthly_snapshots
    WHERE rn = 1
)

SELECT
    flow_id,
    name,
    month,
    enrolled_contacts,
    succeeded_contacts,
    enrolled_contacts - LAG(enrolled_contacts) OVER (
        PARTITION BY flow_id, name
        ORDER BY month
    ) AS new_enrolled,
    succeeded_contacts - LAG(succeeded_contacts) OVER (
        PARTITION BY flow_id, name
        ORDER BY month
    ) AS new_succeeded
FROM latest_monthly
ORDER BY flow_id ASC, name ASC, month DESC;
