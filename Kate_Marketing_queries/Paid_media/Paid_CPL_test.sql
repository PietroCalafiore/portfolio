WITH in_target_leads AS
(SELECT
    country_by_campaign AS country,
    hsa_net AS platform,
    campaign AS campaign_name,
    content AS adset_name,
    keyword AS ad_keyword,
    COALESCE(SUM(CASE WHEN campaign_goal = 'mal' AND campaign_product = 'Agenda' AND target IN ('DOCTOR','FACILITY') THEN mals END), 0)
     AS Total_Agenda_MALs,
    COALESCE(SUM(CASE WHEN campaign_goal = 'mal' AND campaign_product = 'Agenda' AND target = 'DOCTOR' THEN mals END), 0)
     AS DOC_Agenda_MALs,
    COALESCE(SUM(CASE WHEN campaign_goal = 'mal' AND campaign_product = 'Agenda' AND target = 'FACILITY' THEN mals END), 0)
     AS FAC_Agenda_MALs,
    COALESCE(SUM(CASE WHEN campaign_goal = 'mql' AND campaign_product = 'Agenda' AND target = 'DOCTOR' THEN mqls END), 0)
      AS DOC_Agenda_MQLs,
    COALESCE(SUM(CASE WHEN campaign_goal = 'mql' AND campaign_product = 'Agenda' AND target = 'FACILITY' THEN mqls END), 0)
      AS FAC_Agenda_MQLs
       FROM tableau_extract.paid_media_real_time
       WHERE country_by_campaign = 'ES' AND date >= '2025-05-01' AND hsa_net = 'facebook'
       AND campaign = 'es_doc-fac_mal_agenda_meta_awon'
       GROUP BY 1, 2, 3, 4, 5),

DOC_FAC_leads_share_calc AS (
SELECT
    country,
    platform,
    campaign_name,
    adset_name,
    ad_keyword,
    Total_Agenda_MALs,
    DOC_Agenda_MALs,
    ROUND(DOC_Agenda_MALs *100.0/NULLIF(Total_Agenda_MALs, 0) /100.0, 2) AS DOC_MALs_perc,
    FAC_Agenda_MALs,
    ROUND(FAC_Agenda_MALs *100.0/NULLIF(Total_Agenda_MALs, 0) /100.0, 2) AS FAC_MALs_perc
    FROM in_target_leads),

total_spend AS (
SELECT
    country_by_campaign AS country,
    datasource AS platform,
    campaign AS campaign_name,
    adset_name_final AS adset_name,
    ad_keyword,
    ROUND(SUM(spend_eur) OVER (PARTITION BY campaign), 2) AS total_campaign_spend,
    ROUND(SUM(spend_eur) OVER (PARTITION BY adset_name_final), 2) AS total_adset_spend,
    ROUND(SUM(spend_eur) OVER (PARTITION BY ad_keyword), 2) AS total_ad_keyword_spend,
    ROUND(SUM(spend_eur), 2) AS Spend
FROM mart_cust_journey.msv_paid_media_windsor_2
WHERE campaign = 'es_doc-fac_mal_agenda_meta_awon' AND date >= '2025-05-01'
GROUP BY 1, 2, 3, 4, 5, spend_eur
)

SELECT
    ts.country,
    ts.platform,
    ts.campaign_name,
    ts.adset_name,
    ts.ad_keyword,
    ROUND(spend, 2) AS spend,
    ROUND(total_campaign_spend, 1) AS total_campaign_spend,
    ROUND(total_adset_spend, 1) AS total_adset_spend,
    ROUND(total_ad_keyword_spend, 1) AS total_ad_keyword_spend,
    ROUND(spend * DOC_MALs_perc, 1) AS doc_spend,
    DOC_Agenda_MALs,
    DOC_MALs_perc,
    ROUND((spend * DOC_MALs_perc)/NULLIF(DOC_Agenda_MALs, 0), 1) AS doc_agenda_CPL,
    ROUND(spend * FAC_MALs_perc, 1) AS fac_spend,
    FAC_Agenda_MALs,
    FAC_MALs_perc,
    ROUND((spend * FAC_MALs_perc)/NULLIF(FAC_Agenda_MALs, 0), 1) AS fac_agenda_CPL
    FROM total_spend ts
    LEFT JOIN DOC_FAC_leads_share_calc ls ON ts.campaign_name = ls.campaign_name AND ls.adset_name = ts.adset_name AND ts.ad_keyword = ls.ad_keyword

