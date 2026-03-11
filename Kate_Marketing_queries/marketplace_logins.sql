with sum AS (
SELECT hubspot_id,
       last_login_to_docplanner,
       country,
       start_date,
       lag(DATE_TRUNC('month', last_login_to_docplanner)) OVER (PARTITION BY hubspot_id order by start_date asc) As lag, --previous logic value
       CASE WHEN lag(DATE_TRUNC('month', last_login_to_docplanner))  OVER (PARTITION BY hubspot_id order by start_date asc) IS NULL THEN TRUE ELSE FALSE END AS first_time_flag,
       row_number() over (PARTITION BY hubspot_id,DATE_TRUNC('month', last_login_to_docplanner)  ORDER BY start_date ASC) AS row
FROM dw.hs_contact_live_history
WHERE last_login_to_docplanner >= '2023-12-01' AND source_doctor_id IS NOT NULL --AND hubspot_id = 438175848
AND (member_customers_list = 'No' OR member_customers_list IS NULL) AND (is_commercial IS FALSE OR is_commercial IS NULL)
QUALIFY row = 1
)
 SELECT country, DATE_TRUNC('month', last_login_to_docplanner) AS login_month, first_time_flag, COUNT(DISTINCT hubspot_id)  FROM sum
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
