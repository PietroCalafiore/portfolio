WITH paid_leads AS(
SELECT
    rt.country,
    DATE_TRUNC('month', rt.date)::DATE AS date,
    CASE WHEN rt.country IN ('Brazil', 'Chile') AND br_mx_cl_specialisation IN ('Medical', 'Paramedical') THEN br_mx_cl_specialisation ELSE 'None' END AS specialisation,
    rt.verified,
    SUM(CASE WHEN rt.campaign_goal = 'mal' AND rt.target = 'DOCTOR' THEN mals ELSE 0 END) AS doc_mals,
    SUM(CASE WHEN rt.campaign_goal = 'mql' AND rt.target = 'DOCTOR' THEN mqls ELSE 0 END) AS doc_mqls,
    SUM(CASE WHEN rt.campaign_goal = 'mal' AND rt.target = 'DOCTOR' THEN mqls ELSE 0 END) AS doc_mqls_from_mals,
    SUM(CASE WHEN rt.campaign_goal = 'mal' AND rt.target = 'FACILITY' AND rt.campaign_product = 'Agenda' THEN mals ELSE 0 END) AS fac_mals,
    SUM(CASE WHEN rt.campaign_goal = 'mql' AND rt.target = 'FACILITY' AND rt.campaign_product = 'Agenda' THEN mqls ELSE 0 END) AS fac_mqls,
    SUM(CASE WHEN rt.campaign_goal = 'mal' AND rt.target = 'FACILITY' AND rt.campaign_product = 'Agenda' THEN mqls ELSE 0 END) AS fac_mqls_from_mals,
    SUM(CASE WHEN rt.campaign_goal = 'mal' AND rt.target = 'FACILITY' AND rt.campaign_product != 'Agenda' THEN mals ELSE 0 END) AS pms_mals,
    SUM(CASE WHEN rt.campaign_goal = 'mql' AND rt.target = 'FACILITY' AND rt.campaign_product != 'Agenda' THEN mqls ELSE 0 END) AS pms_mqls,
    SUM(CASE WHEN rt.campaign_goal = 'mal' AND rt.target = 'FACILITY' AND rt.campaign_product != 'Agenda' THEN mqls ELSE 0 END) AS pms_mqls_from_mals,
    SUM(CASE WHEN rt.campaign_goal = 'mal' AND rt.target = 'GP' THEN mals ELSE 0 END) AS gp_mals,
    SUM(CASE WHEN rt.campaign_goal = 'mql' AND rt.target = 'GP' THEN mqls ELSE 0 END) AS gp_mqls,
    SUM(CASE WHEN rt.campaign_goal = 'mal' AND rt.target = 'GP' THEN mqls ELSE 0 END) AS gp_mqls_from_mals
FROM tableau_extract.paid_media_real_time rt
WHERE rt.date >= '2024-01-01' AND (rt.br_mx_cl_specialisation != 'Bad Paramedical' OR rt.br_mx_cl_specialisation IS NULL)
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4),
    paid_wons AS (
        SELECT
    hs.country,
    deal_month AS deal_month,
    CASE WHEN hs.country IN ('Brazil', 'Chile') AND hs.br_specialisation IN ('Medical', 'Paramedical')  THEN hs.br_specialisation ELSE 'None'  END AS specialisation,
    hs.verified,
     COUNT(DISTINCT CASE WHEN hs.product_won = ('Agenda Premium') AND hs.target !='GP' THEN hs.hubspot_id ELSE NULL END) as doc_wons,
    COUNT(DISTINCT CASE WHEN hs.product_won = ('Clinic Agenda') THEN hs.hubspot_id ELSE NULL END) as fac_wons,
    COUNT(DISTINCT CASE WHEN hs.product_won = ('Agenda Premium') AND hs.target ='GP' THEN hs.hubspot_id ELSE NULL END) as gp_wons,
    COUNT(DISTINCT CASE WHEN hs.product_won NOT IN ('Agenda Premium', 'Clinic Agenda') THEN hs.hubspot_id ELSE NULL END) as pms_wons
   FROM mart_cust_journey.msv_paid_media_campaign_hubspot_2 hs
   WHERE  (hs.br_specialisation != 'Bad Paramedical' OR hs.br_specialisation IS NULL)
    AND hs.deal_allocation = 'Inbound' AND hs.deal_month IS NOT NULL
   GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4
    )
SELECT rt.country,
    rt.date::TEXT,
    rt.specialisation,
    rt.verified,
    doc_mals,
    doc_mqls,
    doc_mqls_from_mals,
    fac_mals,
    fac_mqls,
    fac_mqls_from_mals,
    pms_mals,
    pms_mqls,
    pms_mqls_from_mals,
    gp_mals,
    gp_mqls,
    gp_mqls_from_mals,
    hs.doc_wons,
    fac_wons,
    pms_wons,
    gp_wons
FROM paid_leads rt
LEFT JOIN paid_wons hs
    ON rt.country = hs.country AND rt.date = hs.deal_month
           AND rt.specialisation = hs.specialisation
           AND rt.verified = hs.verified
ORDER BY 1,2,3,4
