--All Noa Deals with a CM/LG field:
DROP TABLE IF EXISTS test.kate_noa_inbound_deals;
CREATE TABLE test.kate_noa_inbound_deals AS
WITH won_noa_deals AS ( --selecting Closed WON Noa deals belonging to Inbound, excluding trials
    SELECT
        cj.country,
        cj.hs_contact_id,
        cj.deal_id,
        cj.month AS deal_month,
        cj.dealname,
        cj.createdate,
        hsd.demo_watched_at,
        hsd.doctor_facility___first_contact_at__wf AS first_contact_at,
        cj.closedate,
        hsd.noa_notes_budget_category__wf AS budget_category,
        hsd.customer_type,
        cj.mrr_euro,
        cj.mrr_original_currency,
        hsd."tag" AS deal_tag,
        hsd.offer_type,
        hsd.tag_so_new AS info_tag,
        hsd.noa_last_source AS noa_deal_last_source,
        hsd.product_so_cs,
        CASE WHEN hsd.tag_so_new LIKE '%Post trial%' THEN 'Post trial' ELSE 'Direct Sale' END AS sale_type,
        CASE WHEN hsd.cs_tag__wf_manual = 'Cross-sell' THEN 'Cross-sell'
            WHEN hsd.tag_so_new LIKE '%Bundle%' THEN 'Bundle'
            ELSE 'Noa only' END AS type_customer
    FROM mart_cust_journey.cj_deal_month cj
    LEFT JOIN dw.hs_deal_live hsd ON cj.deal_id = hsd.hubspot_id
    WHERE cj.pipeline = 'Noa' AND cj.pipeline_type = 'Noa'
        AND cj.is_current_stage AND cj.stage_is_month_new AND cj.deal_stage = 'Closed Won'
        AND (hsd.noa_notes_trial_yes_no != 'Yes' OR hsd.noa_notes_trial_yes_no IS NULL) --filtering out Trial deals, we only want Paid
        AND cj.month >= '2025-01-01'
        AND LOWER(hsd."tag") LIKE '%inb%'--logic aligned with RevOps to select Inbound deals only
),

won_noa_deals_ls AS ( --classifying deals based on MQL/PQL stage, & type of Noa product. last sources is taken at a later date from historical values due to manual updates
    SELECT
        deal.*,
        CASE WHEN hsh.facility_products_paid_batch ILIKE '%Paid by another doctor (Premium jameda platin)%'
            OR hsh.facility_products_paid_batch ILIKE '%Self (Premium plus)%'
            OR hsh.facility_products_paid_batch ILIKE '%Paid by another doctor (Premium jameda gold)%'
            OR hsh.facility_products_paid_batch ILIKE '%Self (Premium jameda platin)%'
            OR hsh.facility_products_paid_batch ILIKE '%Self (Premium promo)%'
            OR hsh.facility_products_paid_batch ILIKE '%Self (Premium jameda gold)%'
            OR hsh.facility_products_paid_batch ILIKE '%Paid by another doctor (Premium starter)%'
            OR hsh.facility_products_paid_batch ILIKE '%Paid by another doctor (Premium promo)%'
            OR hsh.facility_products_paid_batch ILIKE '%Paid by another doctor (Premium plus)%'
            OR hsh.facility_products_paid_batch ILIKE '%Self (Premium vip)%'
            OR hsh.facility_products_paid_batch ILIKE '%Paid by another doctor (Premium jameda gold-pro)%'
            OR hsh.facility_products_paid_batch ILIKE '%Paid by another doctor (Premium vip)%'
            OR hsh.facility_products_paid_batch ILIKE '%Self (Premium jameda gold-pro)%'
            OR hsh.facility_products_paid_batch ILIKE '%Paid by another doctor (Premium ind)%'
            OR hsh.facility_products_paid_batch ILIKE '%Self (Premium starter)%'
            OR hsh.facility_products_paid_batch ILIKE '%Self (Premium ind)%'
            OR (hsh.facility_products_paid_batch IS NULL AND deal.customer_type = 'Individual') THEN 'Individual' --workaround to catch cases where Paid products property is buggy due to Hays profiles overlap
            WHEN deal.country = 'Germany' THEN 'Individual' --ok Marta, everything in Germany is DOC despite what budget category says
            WHEN deal.budget_category LIKE '%PMS%' OR hsh.facility_products_paid_batch LIKE '%Saas MyDr%' THEN 'PMS'
            WHEN deal.budget_category LIKE '%Clinic%' OR hsh.facility_products_paid_batch LIKE '%Paid by facility%' THEN 'Clinics'
            WHEN deal.budget_category IN ('Noa Individual', 'Noa Ind Expansion') THEN 'Individual'
            ELSE 'check' --keeping a condition to catch and correct all edge cases
        END AS customer_type_all,
        hsh.commercial_from_last_date_at AS commercial_date_before_noa,
        hsh.last_source_so AS frozen_last_source,
        hsh.facility_products_paid_batch AS paid_products_before_noa,
        hsh.member_customers_list AS was_customer,
        CASE WHEN hsh.product_qualified_noa_at >= deal.closedate - INTERVAL '60 day' THEN 'PQL_deal'
            ELSE 'MQL_deal' END AS deal_category,
        COALESCE(hsh.merged_vids, NULL) AS was_merged,
        MAX(hsh.facility_products_paid_batch) OVER (PARTITION BY hsh.hubspot_id) AS products_paid_with_noa, --products paid value that should have Noa product
        MAX(hsh.noa_active_products_batch) OVER (PARTITION BY hsh.hubspot_id) AS active_noa_product,
        ROW_NUMBER() OVER (PARTITION BY deal.deal_id ORDER BY hsh.start_date ASC) AS row_at_deal --getting the row with the earliest info to classify CMs
    FROM won_noa_deals deal
    LEFT JOIN dw.hs_contact_live_history hsh ON hsh.hubspot_id = deal.hs_contact_id AND hsh.start_date BETWEEN deal.deal_month AND DATEADD(DAY, 50, DATE_TRUNC('month', deal.deal_month::DATE))
    QUALIFY row_at_deal = 1
)

SELECT
    country,
    deal_month,
    hs_contact_id AS hubspot_id,
    deal_id,
    dealname AS deal_name,
    noa_deal_last_source,
    createdate AS deal_opened_at,
    demo_watched_at,
    closedate AS deal_closed_at,
    info_tag,
    CASE WHEN LOWER(products_paid_with_noa) LIKE '%standalone%' OR LOWER(active_noa_product) LIKE '%stda%' THEN 'stda_deal'
        WHEN LOWER(products_paid_with_noa) LIKE '%saas%' OR LOWER(active_noa_product) LIKE '%saas%' THEN 'saas_deal'
        WHEN LOWER(offer_type) LIKE '%stda%' THEN 'stda_deal'
        WHEN LOWER(offer_type) LIKE '%saas%' THEN 'saas_deal'
        ELSE 'stda_deal' END AS deal_product, --classifying STDA vs Saas Noa versions, paid products most reliable but if unavailable checking offer type
    CASE WHEN info_tag LIKE '%Bundle%' --if customer was commercial before the month Noa bundle was bought, its a CM customer (since another product was older)
        AND DATE_TRUNC('month', commercial_date_before_noa) < deal_month AND was_customer = 'Yes' THEN 'CM'
        WHEN was_customer = 'No' OR was_customer IS NULL --LG are those that at the time of the deal were not in Customers list or recently entered because of buying a Bundle in the same month
            OR (info_tag LIKE '%Bundle%' AND was_customer = 'Yes') OR DATE_TRUNC('month', commercial_date_before_noa) = deal_month THEN 'LG'
        WHEN customer_type_all = 'PMS' THEN 'CM'
        WHEN products_paid_with_noa IN ('Self (Noa Standalone)', 'Self (Noa Standalone);Paid by another doctor (Noa Standalone)', 'Paid by another doctor (Noa Standalone)', 'Self (AIA Standalone)', 'Self (AIA Standalone);Self (Noa Standalone)', 'Self (Noa Saas)', 'Paid by facility (Noa Standalone)', 'Paid by another doctor (Noa Saas)') THEN 'LG'
        ELSE 'CM'
    END AS lg_cm_flag, --catching edge cases due to merges. if after purchase only Noa products are in Products Paid, its definitely LG
    budget_category,
    deal_category,
    customer_type_all,--main field to divide PMS/Clinics/individuals
    type_customer AS bundle_flag,
    mrr_euro,
    mrr_original_currency,
    paid_products_before_noa,
    products_paid_with_noa,
    was_customer,
    was_merged
FROM won_noa_deals_ls
