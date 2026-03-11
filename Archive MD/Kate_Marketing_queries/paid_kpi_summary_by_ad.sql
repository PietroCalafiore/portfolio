WITH windsor AS (
    SELECT
        wd.country,
        wd.ad_keyword,
        wd.campaign,
        SUM(wd.spend_eur) AS Spend_EUR,
        SUM(wd.impressions) AS impressions
    FROM mart_cust_journey.msv_paid_media_windsor_2 wd
    WHERE wd.date BETWEEN '2024-12-01' AND '2025-02-28'
    GROUP BY 1, 2, 3
),
hubspot AS (
    SELECT
        hs.country,
        hs.keyword,
        hs.campaign,
        SUM(hs.mal) AS MALs,
        SUM(hs.mql) AS MQLs,
        SUM(hs.sal) AS SALs,
        SUM(hs.sql) AS SQLs,
        SUM(CASE WHEN hs.product_won IS NULL and deal_allocation = 'Inbound' THEN hs.won END) AS open_deals,
        SUM(CASE WHEN hs.product_won IS NOT NULL and deal_allocation = 'Inbound' THEN hs.won END) AS WONs,
        SUM(CASE WHEN hs.product_won IS NOT NULL and deal_allocation = 'Inbound' THEN hs.amount_eur END) AS Revenue_EUR,
        SUM(CASE WHEN hs.mal > 0 THEN hs.mql ELSE 0 END) AS MQLs_with_MAL
    FROM mart_cust_journey.msv_paid_media_campaign_hubspot_2 hs
    where DATE>= '2024-12-01'
    GROUP BY 1, 2, 3
)

SELECT
    wd.ad_keyword,
    wd.campaign,
    wd.Spend_EUR,
    wd.impressions,
    hs.MALs,
    hs.MQLs,
    CASE
        WHEN hs.MALs > 0 THEN ROUND(hs.MQLs_with_MAL * 1.0 / hs.MALs, 3)
        ELSE NULL
    END AS MAL_MQL_CVR,
    hs.SALs,
    hs.SQLs,
    hs.WONs,
    hs.open_deals,
    hs.Revenue_EUR
FROM windsor wd
LEFT JOIN hubspot hs
    ON wd.ad_keyword = hs.keyword
    AND wd.campaign = hs.campaign;
