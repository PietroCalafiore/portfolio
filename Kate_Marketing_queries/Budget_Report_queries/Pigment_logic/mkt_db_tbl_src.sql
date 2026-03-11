CREATE TABLE cj_data_layer.marketing_db_pigment_tableau AS
SELECT
    eom_db.date,
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
FROM test.eom_mkt_db_pigment_tableau eom_db
FULL OUTER JOIN test.new_mkt_db_pigment_tableau new_db ON
    eom_db.date = new_db.date
    AND eom_db.market = new_db.market
    AND eom_db.segment_funnel = new_db.segment_funnel
    AND eom_db.target = new_db.target
    AND eom_db.acquisition_channel = new_db.acquisition_channel
    AND eom_db.verified = new_db.verified
  ORDER BY 1, 2, 3, 4, 5, 6
