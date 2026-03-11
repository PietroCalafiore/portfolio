DROP TABLE IF EXISTS test.kate_paid_telemedicine;
CREATE TABLE test.kate_paid_telemedicine AS
SELECT cj.*,
    CASE WHEN cj.keyword = 'br_doc_mql_agenda_meta_instant-form_hubspot-list_offer-request_static_telemedicine-ad04-light' THEN NULL ELSE hs.monthly_budget_range_test END AS monthly_budget_range_test,
    CASE WHEN cj.keyword = 'br_doc_mql_agenda_meta_instant-form_hubspot-list_offer-request_static_telemedicine-ad04-light' THEN NULL ELSE hs.intended_start_timeline_test END AS intended_start_timeline_test,
    hs.contact_result_so,
    hs.contact_result_so_at,
    CASE WHEN cj.keyword = 'br_doc_mql_agenda_meta_instant-form_hubspot-list_offer-request_static_telemedicine-ad04-light-test' THEN TRUE END AS test_ad_flag,
    CASE WHEN deal_month IS NOT NULL AND product_won IS NOT NULL AND (deal_month = date_trunc('month',date) OR deal_month > date) THEN TRUE ELSE FALSE END AS recent_won
FROM mart_cust_journey.msv_paid_media_campaign_hubspot_2 cj
LEFT JOIN dw.hs_contact_live hs ON cj.hubspot_id = hs.hubspot_id
WHERE cj.date >='2025-01-15'
AND cj.keyword IN ('br_doc_mql_agenda_meta_instant-form_hubspot-list_offer-request_static_telemedicine-ad04-light-test',
                   'br_doc_mql_agenda_meta_instant-form_hubspot-list_offer-request_static_telemedicine-ad04-light')

