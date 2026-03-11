
WITH pre_calculation AS (
SELECT
    COALESCE(DATE_TRUNC('month', free_signup_at)) AS month,
    'Inbound' AS lead_source,
    noa.country AS market,
      'Individuals' AS segment_funnel, --dont have the final definition for trial segment yet, also not much data for Clinics/PMS anyway
    'Noa Notes' AS target,
       noa.hubspot_id
FROM mart_cust_journey.noa_marketing_kpis_cm_lg_combined noa
where lg_cm_flag_combined = 'LG'
GROUP BY 1, 2, 3, 4, 5, 6
QUALIFY month BETWEEN '2025-01-01' AND '2025-06-01')

 SELECT
     month::DATE,
     lead_source,
     market,
     segment_funnel,
     target,
     COUNT(DISTINCT hubspot_id) AS signups,
     0 AS mqls
     FROM pre_calculation
        GROUP BY 1, 2, 3, 4, 5
