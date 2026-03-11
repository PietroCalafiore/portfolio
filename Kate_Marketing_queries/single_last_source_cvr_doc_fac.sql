--DOC single last source CVR
WITH contacts_with_one_ls AS (
SELECT COUNT(DISTINCT last_source_so), hubspot_id FROM dw.hs_contact_live_history
GROUP BY 2
HAVING COUNT(DISTINCT last_source_so) = 1)

SELECT  last_source_so, COUNT(DISTINCT contact_id),
        COUNT( DISTINCT CASE WHEN cj_mqls_monthly.true_deal_flag AND deal_month >= month THEN contact_id END) AS had_won_deal
        FROM mart_cust_journey.cj_mqls_monthly
WHERE lifecycle_stage != 'only_won' AND contact_id IN (SELECT hubspot_id FROM contacts_with_one_ls)
AND month >= '2025-01-01'
GROUP BY 1

-- DOC normal data
SELECT  last_source_so, COUNT(DISTINCT contact_id),
        COUNT( DISTINCT CASE WHEN cj_mqls_monthly.true_deal_flag AND deal_month >= month THEN contact_id END) AS had_won_deal
        FROM mart_cust_journey.cj_mqls_monthly
WHERE lifecycle_stage != 'only_won' AND month >= '2025-01-01'
GROUP BY 1

--FAC single last source CVR
WITH contacts_with_one_ls AS (
    SELECT COUNT(DISTINCT last_source_so), hubspot_id FROM dw.hs_contact_live_history
    GROUP BY 2
    HAVING COUNT(DISTINCT last_source_so) = 1)
    
    SELECT  last_source_so, COUNT(DISTINCT contact_id),
            COUNT( DISTINCT CASE WHEN true_deal_flag AND deal_month >= month AND product_won = 'Clinic Agenda' THEN contact_id END) AS had_won_deal
            FROM mart_cust_journey.cj_mqls_monthly_clinics
    WHERE lifecycle_stage != 'only_won' AND cj_mqls_monthly_clinics.mql_product = 'Clinic Agenda' AND contact_id IN (SELECT hubspot_id FROM contacts_with_one_ls)
    AND month >= '2025-01-01'
    GROUP BY 1
    
  -- FAC normal data  
    SELECT  last_source_so, COUNT(DISTINCT contact_id),
            COUNT( DISTINCT CASE WHEN true_deal_flag AND deal_month >= month AND product_won = 'Clinic Agenda'  THEN contact_id END) AS had_won_deal
            FROM mart_cust_journey.cj_mqls_monthly_clinics
    WHERE lifecycle_stage != 'only_won' AND month >= '2025-01-01'  AND cj_mqls_monthly_clinics.mql_product = 'Clinic Agenda'
    GROUP BY 1
