DROP TABLE IF EXISTS test.noa_marketing_open_deals; --STEP 2
CREATE TABLE test.noa_marketing_open_deals AS
SELECT
        cj.country,
        cj.createdate AS create_date,
        cj.opportunity_id AS deal_id,
        cj.hs_contact_id AS hubspot_id,
        mp.email,
        mp.mql_at,
        mp.pql_at,
        COALESCE(mp.mql_pql_flag, 'MQL') AS mql_pql_flag, --if edge case, assume MQL
        CASE WHEN hsd.noa_notes_budget_category__wf IN ('Noa Ind Expansion', 'Noa Clinic - Expansion', 'Noa Clinics - Expansion', 'Noa Paid by TuoTempo', 'Noa Paid by PMS') THEN 'CM'
        WHEN hsd.noa_notes_budget_category__wf IN ('Noa New Clinic - Bundle', 'Noa Individual', 'Noa Clinics - new sales', 'Noa PMS - new sales', 'PMS - New customer') OR sf.type = 'New Business' THEN 'LG'
            WHEN sf.type IS NOT NULL THEN 'CM'
        END AS cm_lg_flag,
        CASE WHEN hsd.noa_notes_budget_category__wf IN ('Noa Ind Expansion', 'Noa Individual') THEN 'Individual'
            WHEN hsd.noa_notes_budget_category__wf IN ('Noa Clinic - Expansion', 'Noa New Clinic - Bundle', 'Noa Clinics - expansion', 'Noa Clinics - new sales', 'Noa Paid by TuoTempo') OR sf.type IS NOT NULL THEN 'Clinics' --temporary assuming all SF deaks are clinics, before MX move
            WHEN hsd.noa_notes_budget_category__wf IN ('Noa Paid by PMS', 'PMS - New customer') THEN 'PMS'
            ELSE hsd.noa_notes_budget_category__wf
        END AS segment,
        COALESCE(mp.pql_category, 'stda') AS pql_category, --if edge case, assume STDA
        COALESCE(mp.mql_category, 'stda') AS mql_category,
        hsd.noa_notes_budget_category__wf AS budget_category,
        COALESCE(hsd.noa_last_source, sf.last_source__c) AS noa_last_source,
        mp.segment AS contact_type,
        mp.mql_last_touch_channel,
        mp.mql_last_conversion_place,
        DATEDIFF(day, cj.createdate, mp.mql_at) AS days_between_mql_and_create_deal_date,
        ROW_NUMBER() over (PARTITION BY cj.opportunity_id ORDER BY mp.mql_at ASC) AS row_per_opp
    FROM mart_cust_journey.cj_opportunity_facts cj
    LEFT JOIN dw.hs_deal_live hsd
        ON cj.opportunity_id = hsd.hubspot_id
            AND cj.crm_source = 'hubspot'
    LEFT JOIN dp_salesforce.opportunity sf
        ON cj.opportunity_id = sf.id
            AND cj.crm_source = 'salesforce'
    LEFT JOIN test.noa_marketing_mqls_pqls mp ON mp.hubspot_id = cj.hs_contact_id AND DATEDIFF(day, cj.createdate, mp.mql_at) BETWEEN -50 AND 60
    WHERE cj.pipeline = 'Noa' AND (cj.closed_lost_reason NOT IN ('Invalid') OR cj.closed_lost_reason IS NULL)
        AND (LOWER(hsd."tag") LIKE '%inb%' OR (sf.last_source__c LIKE '%Noa%' AND NOT sf.last_source__c LIKE '%Outbound%'))   --only inbound deals
        AND (hsd.noa_notes_trial_yes_no != 'Yes' OR hsd.noa_notes_trial_yes_no IS NULL OR (cj.crm_source = 'salesforce' AND (sf.negotiation_type__c = 'Trial' OR sf.negotiation_type__c IS NULL)))     --excluding trial deals
        AND (budget_category NOT IN ('DOC - Price Upgrade', 'FAC - Price Upgrade', 'FAC - Profiles num upgrade', 'DOC - Doc Agenda churn Promo') OR budget_category IS NULL) --budget categories excluded as not new sales (ok Walter)
    QUALIFY row_per_opp = 1


