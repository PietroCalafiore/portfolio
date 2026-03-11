—current Tableau source for MKT DB

DROP TABLE IF EXISTS test.final_mkt_db_tableau_source;
CREATE TABLE test.final_mkt_db_tableau_source AS
WITH new_mals AS(
SELECT
        mkt_db_month AS date,
        'Inbound' AS lead_source,
        country AS market,
        segment_funnel AS segment_funnel,
        CASE WHEN segment_funnel = 'Individuals GPs' THEN 'Doctors' ELSE target END AS target, --ok Juli, Juan & Gaela
        db_channel_short AS acquisition_channel,
        CASE WHEN segment_funnel IN ('Clinics PRS', 'Clinics PMS') AND target IN ('Medium', 'Small', 'Unknown') THEN 'Unverified' ELSE verified END AS verified,
        COUNT(DISTINCT hubspot_id) AS new_marketing_database
    FROM mart_cust_journey.new_marketing_database_contacts
    WHERE segment_funnel IS NOT NULL AND segment_funnel != 'None' AND mkt_db_month >= '2025-11-01'
    GROUP BY 1, 2, 3, 4, 5, 6, 7
   ORDER BY 1, 2, 3, 4, 5, 6, 7),

prep AS(
SELECT
    eom_db.date AS date,
    'Inbound' AS lead_source,
    eom_db.market,
    eom_db.segment_funnel ,
    eom_db.target,
    eom_db.acquisition_channel,
    eom_db.verified,
    LAG(rolling_total) OVER (PARTITION BY eom_db.segment_funnel,eom_db.market, eom_db.target, eom_db.acquisition_channel, eom_db.verified ORDER BY eom_db.date) AS beginning,
    eom_db.rolling_total,
    COALESCE(new_db.new_marketing_database,0) AS new_mals,
    COALESCE( eom_db.rolling_total-new_db.new_marketing_database-LAG(rolling_total) OVER (PARTITION BY eom_db.segment_funnel,eom_db.market, eom_db.target, eom_db.acquisition_channel, eom_db.verified ORDER BY eom_db.date),0)
    AS lost_mals
FROM mart_cust_journey.eom_marketing_database_agg eom_db
FULL OUTER JOIN new_mals new_db ON
    eom_db.date = new_db.date
    AND eom_db.market = new_db.market
    AND eom_db.segment_funnel = new_db.segment_funnel
    AND eom_db.target = new_db.target
    AND eom_db.acquisition_channel = new_db.acquisition_channel
    AND eom_db.verified = new_db.verified
WHERE eom_db.date >= '2025-10-01'
QUALIFY eom_db.date >= '2025-11-01'
ORDER BY 1, 2, 3, 4, 5, 6)
SELECT *,
CASE WHEN market IN ('Brazil', 'Mexico', 'Poland', 'Chile', 'Italy')
AND target = 'Doctors' THEN 'Medical'
 WHEN market IN ('Brazil', 'Mexico', 'Poland', 'Chile', 'Italy')
AND target = 'Paramedics' THEN 'Paramedical'
WHEN target = 'Noa Notes' THEN 'Pure Noa'
ELSE 'None' END AS spec_split
 FROM prep
WHERE segment_funnel IN ('Individuals GPs', 'Individuals Core')
UNION ALL
select *,
CASE WHEN market IN ('Brazil', 'Mexico', 'Poland', 'Chile', 'Italy')
AND target = 'Doctors' THEN 'Medical'
 WHEN market IN ('Brazil', 'Mexico', 'Poland', 'Chile', 'Italy')
AND target = 'Paramedics' THEN 'Paramedical'
WHEN target = 'Noa Notes' THEN 'Pure Noa'
ELSE 'None' END AS spec_split
FROM cj_data_layer.marketing_db_pigment_tableau ex
WHERE segment_funnel IN ('Individuals GPs', 'Individuals Core')
AND date >= '2024-01-01'




