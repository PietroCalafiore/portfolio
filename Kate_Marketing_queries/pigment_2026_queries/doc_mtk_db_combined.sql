 SELECT * FROM test.eom_mkt_db_pigment eom_db
  FULL OUTER JOIN test.new_mkt_db_pigment new_db ON
    eom_db.date = new_db.date
    AND eom_db.lead_source = new_db.lead_source
    AND eom_db.market = new_db.market
    AND eom_db.segment_funnel = new_db.segment_funnel
    AND eom_db.target = new_db.target
    AND eom_db.specialization = new_db.specialization
    AND eom_db.acquisition_channel = new_db.acquisition_channel
    AND eom_db.verified = new_db.verified
  ORDER BY 1, 2, 3, 4, 5, 6, 7, 8
