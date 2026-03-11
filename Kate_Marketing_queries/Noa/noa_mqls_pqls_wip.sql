-- mart_cust_journey.noa_marketing_mqls_pqls STEP 1
DROP TABLE IF EXISTS test.noa_marketing_mqls_pqls;

CREATE TABLE test.noa_marketing_mqls_pqls
DISTKEY(hubspot_id)
AS

WITH all_noa_mqls AS ( --this subquery select all Noa MQLs, selecting one per month per contact
    SELECT
        hs.hubspot_id,
        COALESCE(hs.country, hs1.country) AS country_final, --pulling live country value for edge cases where country is NULL at first
        hs.email,
        DATE_TRUNC('month', hs.noa_lead_at) AS mql_month,
        DATE_TRUNC('month', hs.product_qualified_noa_at) AS pql_month,
        hs.noa_lead_at AS mql_at,
        hs.product_qualified_noa_at AS pql_at,
        hs.member_customers_list AS was_customer,
        hs.facility_products_paid_batch AS paid_products, --contact level
        hcl.facility_products_paid_batch AS company_paid_products, --company level
        MAX(hs.facility_products_paid_batch) OVER (PARTITION BY hs.hubspot_id, hs.noa_lead_at) AS max_paid_products, --getting the max paid_products value to se if there ever was one (for CM edge case assignation)
        hs.commercial_from_last_date_at,
        hs.merged_vids AS was_merged,
        hs.noa_last_source AS hs_noa_last_source,
        hs.contact_type_segment_test,
        hs.noa_opportunity_status_sf,
        hs.mql_last_touch_channel_wf AS mql_last_touch_channel,
        hs.mql_last_conversion_place_wf AS mql_last_conversion_place,
        hs.feegow_commercial_import_test,
        MAX(hs.noa_notes_stda_last_activation_at_batch) OVER (PARTITION BY hs.hubspot_id) AS stda_activated_at, --replicating list logic, if STDA activation date is known (doesnt matter the date itself)
        CASE WHEN (MIN(hs.start_date) OVER (PARTITION BY hs.hubspot_id) > DATEADD('day', 5, hs.noa_lead_at)) AND hs.merged_vids IS NOT NULL THEN 'delayed_info' ELSE 'ok' END AS merge_delay, --126529398896 example of when older MQL data is lost due to merge, so workaround to flag delays is needed both on MQL and PQL dates
        ROW_NUMBER() OVER (PARTITION BY hs.hubspot_id, DATE_TRUNC('month', hs.noa_lead_at) ORDER BY hs.start_date ASC) AS row_per_mql --partitioning only by noa_lead_at because there shouldnt be MQL stamps with no PQL date
    FROM dw.hs_contact_live_history hs
    LEFT JOIN dw.hs_contact_live hs1 ON hs.hubspot_id = hs1.hubspot_id
    LEFT JOIN dw.hs_association_live hal
        ON hs1.hubspot_id = hal.to_obj
            AND hal.is_deleted IS FALSE
            AND hal.type = 'company_to_contact'
    LEFT JOIN dw.hs_company_live hcl
        ON hal.from_obj = hcl.hubspot_id
            AND hcl.is_deleted IS FALSE
    WHERE hs.noa_lead_at IS NOT NULL --because of the way hs_contact_history handles merges I cant limit the rows only to when the MQL/PQL happened
        AND hs.start_date >= '2024-08-01' AND hs1.is_deleted IS FALSE --AND hs.hubspot_id = 8977640764
    QUALIFY row_per_mql = 1 AND country_final IN ('Brazil', 'Mexico', 'Spain', 'Italy', 'Germany', 'Colombia', 'Chile', 'Turkiye', 'Peru', 'Poland')
    AND (hs.email NOT LIKE '%docplanner.com%' AND hs.email NOT LIKE '%shahar%' AND hs.email NOT LIKE '%test%' AND hs.email NOT LIKE '%miodottore%'
    AND hs.email NOT LIKE '%staszek.marta%' OR hs.email IS NULL)
),

all_noa_pqls AS ( --this subquery select all Noa PQLs, selecting one per month per contact
    SELECT
        hs.hubspot_id,
        COALESCE(hs.country, hs1.country) AS country_final,
        hs.email,
        DATE_TRUNC('month', hs.product_qualified_noa_at) AS mql_month,
        DATE_TRUNC('month', hs.product_qualified_noa_at) AS pql_month,
        hs.product_qualified_noa_at AS mql_at,
        COALESCE(hs.product_qualified_noa_at, MAX(hs.product_qualified_noa_at) OVER (PARTITION BY hs.hubspot_id, DATE_TRUNC('month', hs.noa_lead_at))) AS pql_at,
        hs.member_customers_list AS was_customer,
        hs.facility_products_paid_batch AS paid_products, --contact level
        hcl.facility_products_paid_batch AS company_paid_products, --company level
        MAX(hs.facility_products_paid_batch) OVER (PARTITION BY hs.hubspot_id, hs.noa_lead_at) AS max_paid_products, --getting the max paid_products value to see if there ever was one (for CM edge case assignation)
        hs.commercial_from_last_date_at,
        hs.merged_vids AS was_merged,
        hs.noa_last_source AS hs_noa_last_source,
        hs.contact_type_segment_test,
        hs.noa_opportunity_status_sf,
        hs.mql_last_touch_channel_wf AS mql_last_touch_channel,
        hs.mql_last_conversion_place_wf AS mql_last_conversion_place,
        hs.feegow_commercial_import_test,
        MAX(hs.noa_notes_stda_last_activation_at_batch) OVER (PARTITION BY hs.hubspot_id) AS stda_activated_at,
        CASE WHEN (MIN(hs.start_date) OVER (PARTITION BY hs.hubspot_id) > DATEADD('day', 5, hs.product_qualified_noa_at)) AND hs.merged_vids IS NOT NULL THEN 'delayed_info' ELSE 'ok' END AS merge_delay, --126529398896 example of when older MQL data is lost due to merge, so workaround to flag delays is needed both on MQL and PQL dates
        ROW_NUMBER() OVER (PARTITION BY hs.hubspot_id, DATE_TRUNC('month', hs.product_qualified_noa_at) ORDER BY hs.start_date ASC) AS row_per_pql
    FROM dw.hs_contact_live_history hs
    LEFT JOIN dw.hs_contact_live hs1 ON hs.hubspot_id = hs1.hubspot_id
    LEFT JOIN dw.hs_association_live hal
        ON hs1.hubspot_id = hal.to_obj
            AND hal.is_deleted IS FALSE
            AND hal.type = 'company_to_contact'
    LEFT JOIN dw.hs_company_live hcl
        ON hal.from_obj = hcl.hubspot_id
            AND hcl.is_deleted IS FALSE
    WHERE hs.product_qualified_noa_at IS NOT NULL --because of the way hs_contact_history handles merges I cant limit the rows only to when the MQL/PQL happened
        AND hs.start_date >= '2024-08-01' AND hs1.is_deleted IS FALSE --AND hs.hubspot_id = 672947120
    QUALIFY row_per_pql = 1 AND country_final IN ('Brazil', 'Mexico', 'Spain', 'Italy', 'Germany', 'Colombia', 'Chile', 'Turkiye', 'Peru', 'Poland')
    AND (hs.email NOT LIKE '%docplanner.com%' AND hs.email NOT LIKE '%shahar%' AND hs.email NOT LIKE '%test%' AND hs.email NOT LIKE '%miodottore%'
    AND hs.email NOT LIKE '%staszek.marta%' OR hs.email IS NULL)
),

all_noa_mqls_pqls AS ( --union of both MQL and PQL metrics
    SELECT
        *,
        'MQL' AS mql_pql_flag
    FROM all_noa_mqls
    UNION ALL
    SELECT
        *,
        'PQL' AS mql_pql_flag
    FROM all_noa_pqls
),

deduped_final AS (--selecting only 1 type of lead per month per contact, with preference given to PQL. If there was a PQL in a given month, we count only the PQL, if PQL was null or an old one, we count MQL that month
    SELECT
        hubspot_id,
        email,
        mql_month,
        pql_month,
        mql_at,
        pql_at,
        country_final AS country,
        was_customer,
        mql_last_touch_channel,
        mql_last_conversion_place,
        stda_activated_at,
        contact_type_segment_test AS segment,
        COALESCE(paid_products, max_paid_products, company_paid_products) AS paid_products,
        max_paid_products,
        company_paid_products, --we still pull those values because in a COALESCE we might lose information if first value isnt NULL
        commercial_from_last_date_at,
        feegow_commercial_import_test,
        hs_noa_last_source,
        merge_delay,
        mql_pql_flag,--flag to identify MQL vs PQL rows
        ROW_NUMBER() OVER (PARTITION BY hubspot_id, DATE_TRUNC('month', mql_at) ORDER BY mql_pql_flag DESC) AS row_per_lead -- ORDER by DESC so PQL always takes priority for same month (mql_month field in PQl subquery has pql month)
    FROM all_noa_mqls_pqls
    WHERE 1 = 1
    QUALIFY row_per_lead = 1
)

SELECT
    hubspot_id,
    email,
    country,
    mql_pql_flag,
    pql_at,
    mql_at,
    mql_month,
    pql_month,
    was_customer,
    paid_products AS paid_products,
    segment,
    commercial_from_last_date_at,
    hs_noa_last_source,
    mql_last_touch_channel,
    mql_last_conversion_place,
    merge_delay,
    CASE WHEN mql_pql_flag = 'PQL' AND country != 'Italy' AND stda_activated_at IS NOT NULL THEN 'stda' ELSE 'saas' END AS pql_category, --matching CM lists rules, if there was ever a STDA trial activation, then STDA PQL, otherwise Saas
    CASE WHEN mql_pql_flag = 'MQL' AND country != 'Italy' AND (LOWER(paid_products) LIKE '%starter%' OR LOWER(paid_products) LIKE '%starter%') THEN 'stda' ELSE 'saas' END AS mql_category, --all PQLs & MQLs in Italy to be saas as per @Alessandra Coletta
    CASE WHEN was_customer = 'Yes' AND (COALESCE(paid_products, company_paid_products, max_paid_products)
        IN ('Self (Noa Standalone)', 'Self (AIA Standalone)', 'Paid by facility (Noa Standalone)', 'Self (Noa SaaS)', 'Paid by another doctor (Noa Saas)', 'Paid by another doctor (Noa Standalone)', 'Paid by facility (Noa Saas)', 'Self (AIA Standalone)', 'Self (AIA Standalone);Self (Noa Standalone)', 'Self (AIA SaaS);Self (Noa Saas)', 'Self (AIA SaaS)') --if at the time of PQL, the only paid product for the contact was Noa, its usually a merge and not CM
        OR DATE_TRUNC('month', commercial_from_last_date_at) = DATE_TRUNC('month', mql_at)) THEN 'LG' --PQL/MQL month is the same as last commercial date at the time of interaction, its usually a Bundle so LG
        WHEN was_customer = 'Yes' THEN 'CM' ELSE 'LG' END AS lg_cm_flag,
      CASE WHEN (was_customer = 'No' OR was_customer IS NULL) OR (was_customer = 'Yes' AND (DATE_TRUNC('month', commercial_from_last_date_at) = DATE_TRUNC('month', mql_at)
        OR paid_products IN ('Self (Noa Standalone)', 'Self (AIA Standalone)', 'Paid by facility (Noa Standalone)', 'Self (Noa Saas)', 'Paid by another doctor (Noa Saas)', 'Paid by another doctor (Noa Standalone)', 'Paid by facility (Noa Saas)', 'Self (AIA SaaS);Self (Noa Saas)', 'Self (AIA Standalone);Self (Noa Standalone)', 'Self (AIA Standalone)', 'Self (AIA SaaS)')))
        THEN 'Individual' --at the moment LG MQLs arent split into DOC/FAC, need to agree on rules with stakeholders. here we apply same logic as above to flag LG and assign all as Individual
        WHEN paid_products ILIKE '%Paid by another doctor (Premium jameda platin)%' --we use Paid products to flag individual customers, logic from HS lists
            OR paid_products ILIKE '%Self (Premium plus)%'
            OR paid_products ILIKE '%Paid by another doctor (Premium jameda gold)%'
            OR paid_products ILIKE '%Self (Premium jameda platin)%'
            OR paid_products ILIKE '%Self (Premium promo)%'
            OR paid_products ILIKE '%Self (Premium jameda gold)%'
            OR paid_products ILIKE '%Paid by another doctor (Premium starter)%'
            OR paid_products ILIKE '%Paid by another doctor (Premium promo)%'
            OR paid_products ILIKE '%Paid by another doctor (Premium plus)%'
            OR paid_products ILIKE '%Self (Premium vip)%'
            OR paid_products ILIKE '%Paid by another doctor (Premium jameda gold-pro)%'
            OR paid_products ILIKE '%Paid by another doctor (Premium vip)%'
            OR paid_products ILIKE '%Self (Premium jameda gold-pro)%'
            OR paid_products ILIKE '%Paid by another doctor (Premium ind)%'
            OR paid_products ILIKE '%Self (Premium starter)%' OR paid_products ILIKE '%Self (MP only)%'
            OR paid_products ILIKE '%Self (Premium ind)%'--workaround to catch cases where Paid products property is buggy due to Hays profiles overlap
            OR (max_paid_products ILIKE '%Self (Premium%' AND NOT max_paid_products ILIKE '%Self (Premium Online agenda%')
            OR country IN ('Germany', 'Chile', 'Turkiye') THEN 'Individual' --ok Marta, everything in Germany is DOC despite what budget category says
        WHEN LOWER(paid_products) ILIKE '%pms%' OR LOWER(paid_products) ILIKE '%gipo%' OR paid_products ILIKE '%MyDr%'
            OR paid_products ILIKE '%Clinic Cloud%' OR paid_products ILIKE '%Feegow%' OR LOWER(company_paid_products) ILIKE '%pms%' OR LOWER(company_paid_products) ILIKE '%gipo%' OR company_paid_products ILIKE '%MyDr%'
            OR company_paid_products ILIKE '%Clinic Cloud%' OR company_paid_products ILIKE '%Feegow%' OR company_paid_products ILIKE '%ClinicCloud%'
            OR max_paid_products ILIKE '%Clinic Cloud%'
            THEN 'PMS' --if Customer not individual, we check all related paid products values to identify if they are PMS or CLinics. This logic is WIP and needs to be improved/agreed with stakeholders
        WHEN LOWER(paid_products) ILIKE '%facility%' OR LOWER(paid_products) ILIKE '%saas only%' OR LOWER(paid_products) ILIKE '%tuotempo%'
            OR paid_products ILIKE '%DP Phone%' OR company_paid_products ILIKE '%DP Phone%' OR LOWER(company_paid_products) ILIKE '%facility%' OR LOWER(company_paid_products) ILIKE '%saas only%' OR LOWER(company_paid_products) ILIKE '%tuotempo%'
            OR max_paid_products ILIKE '%facility%'
            THEN 'Clinics'
        WHEN feegow_commercial_import_test = 1 THEN 'PMS' --workaround trying to spot Feegow customers
        WHEN country = 'Poland' THEN 'PMS' --this is temporary while we get a more reliable way to spot MyDr customers, generally if anyone is not assigned at this stage, its myDR
        WHEN paid_products IN ('Self (Noa Standalone)', 'Self (AIA Standalone)') THEN 'Individual'
        ELSE 'Clinics' --technically wrong but only 20 MQLs we cant properly identify 18.06
    END AS customer_type_all
FROM deduped_final
