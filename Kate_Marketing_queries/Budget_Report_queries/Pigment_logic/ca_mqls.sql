-- cj_mqls_monthly
DROP TABLE IF EXISTS test.cj_mqls_monthly_clinics;

CREATE TABLE test.cj_mqls_monthly_clinics
DISTKEY(contact_id)
AS
WITH WON_deals AS ( --pulling Inbound Won deals from Hubspot (Poland, Clinic Cloud) and Salesforce (Clinic Agenda, Gipo). also pulling historical values from HS table for WON categorization
          SELECT
                deal.opportunity_id AS deal_id,
                deal.hs_lead_id AS lead_id,
                hsh.email,
                DATE_TRUNC('month', NVL(deal.active_won_date,deal.closedate))::DATE AS month,
                deal.opportunity_name,
                deal.country,
                COALESCE(CAST(deal.hs_contact_id AS bigint), 0) AS hs_contact_id,
                DATE_TRUNC('day', NVL(deal.active_won_date,deal.closedate))::DATE AS deal_stage_start,
                deal.owner_name,
                sf.last_source__c AS sf_source,
                sf.businessline__c AS sf_product,
                MAX(mrr_euro) OVER (PARTITION BY deal.opportunity_id) AS mrr_euro_final,
                MAX(mrr_original_currency) OVER(PARTITION BY deal.opportunity_id) AS mrr_original_currency,
                live."tag",
                CASE WHEN deal.country = 'Italy'
                    AND ((sf.last_source__c IN ('Basic reference', 'other', 'Target tool [DA]', 'Sales Contact', 'visiting pricing', 'new facility verification', 'New verification', 'Internal reference')
                    OR sf.last_source__c IS NULL
                    OR LOWER(sf.last_source__c) LIKE '%outbound%' OR LOWER(sf.last_source__c) LIKE '%massive%')
                    AND (ld.last_source__c IN ('Basic reference', 'other', 'Target tool [DA]', 'Sales Contact', 'visiting pricing', 'new facility verification', 'New verification', 'Internal reference')
                    OR ld.last_source__c IS NULL
                    OR LOWER(ld.last_source__c) LIKE '%outbound%' OR LOWER(ld.last_source__c) LIKE '%massive%'))
                    THEN 'Outbound'
                WHEN deal.country IN ('Mexico', 'Spain', 'Colombia', 'Brazil') AND sf.businessline__c IN ('Clinic Agenda', 'Bundle', 'Marketplace')
                    AND ((sf.last_source__c IN ('other', 'Target tool [DA]', 'Sales Contact')
                    OR sf.last_source__c IS NULL OR LOWER(sf.last_source__c) LIKE '%internal reference%'
                    OR LOWER(sf.last_source__c) LIKE '%outbound%' OR LOWER(sf.last_source__c) LIKE '%massive%')
                    AND
                    (ld.last_source__c IN ('other', 'Target tool [DA]', 'Sales Contact')
                    OR ld.last_source__c IS NULL OR LOWER(ld.last_source__c) LIKE '%internal reference%'
                    OR LOWER(ld.last_source__c) LIKE '%outbound%' OR LOWER(ld.last_source__c) LIKE '%massive%'))
                    THEN 'Outbound'
                WHEN LOWER(sf.last_source__c) LIKE '%deleted%' THEN 'Outbound'
                ELSE 'Inbound'
                END AS deal_allocation, --for SF opps we have to check both Lead and Opp last source as they can be different, issue in SF
                CASE WHEN deal.country = 'Brazil'
                    AND deal.pipeline = 'PMS' THEN 'Feegow'
                WHEN deal.country = 'Italy'
                    AND deal.pipeline LIKE '%PMS%' OR deal.pipeline = 'GIPO' THEN 'Gipo'
                WHEN deal.country = 'Spain'
                    AND (deal.pipeline LIKE '%Enterprise SaaS4Clinics / Marketplace%' OR deal.pipeline = 'PMS')
                    AND (deal.opportunity_name LIKE '%CC%'
                    OR deal.opportunity_name LIKE '%Clinic Cloud%' OR live.hubspot_team_id IN (1453567, 271598, 3115413, 2903828)) THEN 'Clinic Cloud'
                WHEN deal.country = 'Poland' AND deal.pipeline = 'PMS' THEN 'MyDr'
                WHEN deal.country = 'Spain' AND deal.pipeline = 'Clinics New Sales' THEN 'Clinic Agenda'
                WHEN sf.businessline__c = 'Noa Notes' THEN NULL
                ELSE 'Clinic Agenda'
                END AS product_won,
            sf.last_source__c,
            ld.last_source__c,
            hsh.hs_analytics_source,
            hsh.customer_reference_at,
            COALESCE(CASE WHEN hsh.facility_size LIKE ('%Individual%')
                OR hsh.facility_size LIKE ('%Small%') THEN 'Small'
                    WHEN hsh.facility_size LIKE ('%Large%')
                       OR hsh.facility_size LIKE ('%Mid%') THEN 'Medium'
                    ELSE 'Unknown'
                    END, 'Unknown') AS facility_size,--12
            hsh.source_so,--13
            hsh.last_source_so,--14
            hsh.affiliate_source,
            CASE WHEN hsh.verified THEN 'Verified' ELSE 'Not Verified' END AS verified,
            hsh.mql_last_touch_channel_wf AS mql_channel,
            hsh.doctor_facility___marketing_events_tag,
            hsh.marketing_action_tag2,
            hsh.hs_analytics_source_data_2,
            hsh.hs_analytics_source_data_1,
            hsh.lead_transferred_to_so,--21
            hsh.feegow_lead_at_test AS feegow_lead_at,--24
            hsh.facility_lead_transferred_to_so_wf, --26
            hsh.lead_status_sf,
            hsh.sub_brand_wf_test AS subbrand,
            hsh.lead_business_line_sf,
            CASE WHEN hsh.active_passive_lead = 'Outbound' THEN 'Passive' ELSE COALESCE(hsh.active_passive_lead, 'Passive') END AS active_passive_lead,
            ROW_NUMBER() OVER (PARTITION BY deal.opportunity_id ORDER BY hsh.start_date DESC) as row --getting only
        FROM mart_cust_journey.cj_opportunity_facts deal
        LEFT JOIN dw.hs_deal_live live ON live.hubspot_id = deal.opportunity_id
        LEFT JOIN dp_salesforce.opportunity sf ON sf.id = deal.opportunity_id AND stagename IN ('Closed Won', 'Active/Won', 'Closed Lost', 'Back to Sales')
        LEFT JOIN dp_salesforce.lead ld ON ld.convertedopportunityid = sf.id
        LEFT JOIN dw.hs_contact_live_history hsh ON hsh.hubspot_id = deal.hs_contact_id AND (hsh.start_date BETWEEN DATE_TRUNC('month', NVL(deal.active_won_date,deal.closedate))::DATE AND DATEADD(day, 50, DATE_TRUNC('month', DATE_TRUNC('month', NVL(deal.active_won_date, deal.closedate))::DATE))
            OR hsh.last_source_so_at BETWEEN DATEADD('day', -30, DATE_TRUNC('month',DATE_TRUNC('month', NVL(deal.active_won_date,deal.closedate))::DATE)) AND DATEADD('day', 50, DATE_TRUNC('month', DATE_TRUNC('month', NVL(deal.active_won_date, deal.closedate))::DATE))) --temp workaround to catch merged MQLs and get their correct last source as otherwise we lose them
        WHERE (deal.current_stage = 'Closed Won' OR (deal.was_active_won = 1 AND deal.current_stage IN ('Closed Won', 'Active/Won', 'Closed Lost', 'Back to Sales')))
            AND pipeline_type IN ('Clinics New Sales')
            AND UPPER(deal.opportunity_name) NOT LIKE '%FAKE%'
            AND deal.owner_name NOT LIKE '%Agnieszka Ligwinska%'
            AND (deal.country IN ('Colombia', 'Mexico', 'Brazil', 'Italy')
                OR (deal.country = 'Spain' AND deal.opportunity_name NOT LIKE '%Saas360%'
            AND deal.opportunity_name NOT LIKE '%PMS deal%' AND deal.opportunity_name NOT LIKE '%PMS SALES DEAL%')
                OR (deal.country = 'Poland' AND deal.pipeline != 'PMS' AND live.hubspot_team_id = 3210 AND NOT (LOWER(live."tag") LIKE '%sell%'
                    OR LOWER(live."tag") LIKE '%upgrade%'
                    OR LOWER(live."tag") LIKE '%migration%'
                    OR LOWER(live."tag") LIKE '%error%'
                    OR LOWER(live."tag") LIKE '%references%'
                    OR LOWER(live."tag") LIKE '%other%'
                    OR LOWER(live."tag") IS NULL))
                    OR deal.country = 'Poland' AND deal.pipeline = 'PMS'
                    )
            AND deal_allocation = 'Inbound'
            AND DATE_TRUNC('month', NVL(deal.active_won_date, deal.closedate))::DATE >= '2024-01-01'
            QUALIFY row = 1 AND
            ((current_stage = 'Closed Won' AND deal.country IN ('Poland', 'Italy', 'Spain', 'Mexico', 'Colombia', 'Brazil'))  OR (deal.country IN ('Italy', 'Spain', 'Mexico', 'Colombia', 'Brazil', 'Spain') AND product_won IN ('Clinic Agenda', 'Gipo') AND (deal.was_active_won = 1 OR deal.current_stage = 'Closed Won'))) --workaround to consider Active Won stage for SF products but not for HS to match marketing reporting
),

gipo_leads_to_exclude AS (
SELECT DISTINCT hubspot_id FROM dv_raw.sat_contact_hs_1h_log hs WHERE last_source_so_at = '2025-01-29' AND  last_source_so = 'Product interested S4C/PMS' --workaround due to Gipo lead rain in January
),

hs_mqls AS (--getting all Pure MQLs based on LCS
    SELECT
        hs.hubspot_id AS contact_id,
        hs.country,
        DATE_TRUNC('month', hs.lcs_mql_at_test) AS month,
        hs.lcs_mql_at_test AS lifecycle_stage_start,
        'MQL' AS lcs,
        hs.is_deleted AS latest_deleted_value,
        MIN(hs.start_date) OVER (PARTITION BY hs.hubspot_id) AS earliest_record_hs,
        CASE WHEN MIN(DATE_TRUNC('day', hs.start_date)) OVER (PARTITION BY hs.hubspot_id) > DATE_TRUNC('day', hs.lcs_mql_at_test) THEN TRUE
            ELSE FALSE END AS merged_issue, --condition to be able to correctly get historical data later in the query. If there was a merge post-MQL, we get the oldest row avaialble in the HS log table
        ROW_NUMBER() OVER (PARTITION BY hs.hubspot_id, DATE_TRUNC('month', hs.lcs_mql_at_test) ORDER BY hs.start_date DESC) AS row_per_mql
    FROM dw.hs_contact_live_history hs
    WHERE (hs.lcs_mql_at_test >= '2024-01-01')-- AND hs.hubspot_id = 593094216-- IN (127389108547,129997314928,77524684827,128562590268)
    AND hs.country IN ('Colombia', 'Spain', 'Mexico', 'Brazil', 'Italy', 'Poland') --if was deleted at MQL moment, delete, but if currently deleted, check if it was merge
    GROUP BY 1, 2, 3, 4, 5, 6, hs.start_date
    QUALIFY row_per_mql = 1 AND latest_deleted_value IS FALSE),

hs_influenced AS (--getting all Influenced MQLs based on last marketing influenced at property
     SELECT
        hs.hubspot_id AS contact_id,
        hs.country,
        DATE_TRUNC('month', hs.last_marketing_influenced_at_test) AS month,
        hs.last_marketing_influenced_at_test AS lifecycle_stage_start,
        'influenced' AS lcs,
        hs.is_deleted AS latest_deleted_value,
        MIN(hs.start_date) OVER (PARTITION BY hs.hubspot_id) AS earliest_record_hs,
        CASE WHEN MIN(DATE_TRUNC('day', hs.start_date)) OVER (PARTITION BY hs.hubspot_id) > DATE_TRUNC('day', hs.last_marketing_influenced_at_test) THEN TRUE
        ELSE FALSE END AS merged_issue,
        ROW_NUMBER() OVER (PARTITION BY hs.hubspot_id, DATE_TRUNC('month', hs.last_marketing_influenced_at_test) ORDER BY hs.start_date DESC) AS row_per_influenced
    FROM dw.hs_contact_live_history hs
    WHERE (hs.last_marketing_influenced_at_test >= '2024-01-01') --AND hs.hubspot_id = 76527995805
    AND hs.country IN ('Colombia', 'Spain', 'Mexico', 'Brazil', 'Italy', 'Poland')
    GROUP BY 1, 2, 3, 4, 5, 6, hs.start_date
    QUALIFY row_per_influenced = 1 AND latest_deleted_value IS FALSE
),

all_leads AS (
    SELECT * FROM hs_mqls
    UNION
    SELECT * FROM hs_influenced
),

leads_deduped AS( --deduplicating to one MQL per contact per month
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY contact_id, month ORDER BY month) AS row_per_lcs
    FROM all_leads
    WHERE 1=1
    QUALIFY row_per_lcs = 1
),

all_mqls AS ( --subquery that selects all MQLs for CA & PMS products, and categorized them. Switching from CJ data source to HS due to the complexity of handling merges otherwise
        SELECT
            mql.contact_id AS contact_id, --1
            deal.deal_id,--2
            hs.email AS hs_email,--3
            hs.country,--4
            mql.lifecycle_stage_start AS lifecycle_stage_start, --5
            mql.month AS MONTH,--6
            mql.lcs AS lifecycle_stage,--7
            CASE WHEN deal.deal_id IS NOT NULL THEN 'FACILITY'
            WHEN hs.contact_type_segment_test IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') OR (hs.country = 'Italy' AND hs.contact_type_segment_test IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1', 'DIAGNOSTIC'))
            THEN 'FACILITY'
            ELSE 'check_pms'
            END AS target_aux,--7 --for CA, we select only 2 main contact types (3 for Italy). For PMS, currently no such filter in HS reports--8 --for CA, we select only 2 main contact types (3 for Italy). For PMS, currently no such filter in HS reports
            deal.mrr_euro_final,--9
            deal.product_won,--10
            CASE
            WHEN hs.lead_transferred_to_so = 'Clinic Online Agenda' AND DATE_TRUNC('month', hs.facility_lead_transferred_to_so_wf) = mql.month AND (hs.lead_business_line_sf IN ('Clinic Agenda', 'Agenda Premium') OR hs.lead_business_line_sf IS NULL) THEN 'Clinic Agenda' --catching trnasferred leads, but if PMS, still assign to PMS not agenda lower in the logic
            WHEN mql.country = 'Brazil' AND ((DATE_TRUNC('month', COALESCE(hs.feegow_lead_at_test, hcl.feegow_lead_at_test)) = mql.month AND (hs.lead_business_line_sf != 'Clinic Agenda' OR hs.lead_business_line_sf IS NULL))
            OR (mql.month <= '2024-02-01' AND hs.last_source_so LIKE '%Feegow%')) --feeogow lead at was added to historical tables mid-february,trying to not overexclude leads with lead business line condition
            THEN 'Feegow' --selecting Feegow leads based on Feegow lead at. Pending final definitions from Feegow team once fully moved to HS.
            WHEN (DATE_TRUNC('month', hs.noa_lead_at) = mql.month) AND hs.last_source_so LIKE '%Noa%' THEN 'Noa' --Noa leads to be excluded
            WHEN mql.country = 'Spain'
              AND (hs.last_sales_team IN (1453567, 271598, 3115413, 2903828)) THEN 'Clinic Cloud' -- temporary CLinic Cloud selection, pending final definitions
             WHEN mql.country = 'Italy' AND mql.month <= '2024-04-01' AND hs.source_so IN ('Contact form GIPO', 'GIPO Contact Form', 'ClinicCloud direct email', 'ClinicCloud call', 'ClinicCloud offer request', 'Clinic Cloud offer request',
            'Product interested S4C/PMS', 'Sales demo ordered GIPO', 'GIPO Demo', 'CallPage GIPO', 'GIPO Callpage','Free trial Feegow', 'Demo watched Feegow', 'Offer Request GipoDental', 'Visiting pricing Feegow') THEN 'Gipo' --SF data not available fully before April
            WHEN mql.country = 'Italy' AND
                 (hs.hubspot_owner_id = 441140868 OR --sf owner
                    (hs.lead_status_sf IS NOT NULL AND
                        (DATE_TRUNC('month', hs.contact_result_so_at) BETWEEN mql.month AND DATE_ADD('month', 1, mql.month))))
            AND ((hs.source_so IN ('Contact form GIPO', 'GIPO Contact Form', 'ClinicCloud direct email', 'ClinicCloud call', 'ClinicCloud offer request', 'Clinic Cloud offer request',
            'Product interested S4C/PMS', 'Sales demo ordered GIPO', 'GIPO Demo', 'CallPage GIPO', 'GIPO Callpage','Free trial Feegow', 'Demo watched Feegow', 'Offer Request GipoDental', 'Visiting pricing Feegow'))
                     OR hs.lead_business_line_sf IN ('GIPO', 'S4C/PMS')) THEN 'Gipo' --selecting Gipo leads, checking HS list structure or Salesforce business line
            WHEN mql.country = 'Poland' AND (hs.last_source_so IN
                ('Contact form GIPO', 'GIPO Contact Form', 'ClinicCloud direct email', 'ClinicCloud call', 'ClinicCloud offer request', 'Clinic Cloud offer request',
            'Product interested S4C/PMS', 'Sales demo ordered GIPO', 'GIPO Demo', 'CallPage GIPO', 'GIPO Callpage','Free trial Feegow', 'Demo watched Feegow', 'Offer Request GipoDental', 'Visiting pricing Feegow') OR hs.last_source_so LIKE '%S4C/PMS%')
                AND hs.sub_brand_wf_test LIKE '%MyDr%' THEN 'MyDr' --Mydr leads, no official definition yet
            WHEN (hs.facility___product_recommended__wf LIKE '%DP Phone%' AND (NOT hs.lead_business_line_sf IN ('Clinic Agenda', 'Gipo') OR hs.lead_business_line_sf IS NULL))
                OR (mql.country IN ('Spain', 'Brazil', 'Italy')
                AND hs.lead_business_line_sf = 'DP Phone') THEN 'DP Phone' -- DP Phone leads, low volume
            WHEN mql.country = 'Poland' AND ((hs.facility_size NOT IN ('Small', 'Individual (1)', 'Individual (2)')) OR hs.facility_size IS NULL) THEN 'Clinic Agenda' --PL Clinic Agenda leads, HS source of logic
            WHEN mql.country = 'Brazil' AND ld.status IS NOT NULL AND (mql.month <='2025-01-01')
                AND (hs.source_so = 'Marketing e-mail campaigns' OR (hs.marketing_action_tag2 LIKE '%BRCLPE_Pricing_Test%' AND hs.source_so = 'visiting pricing') OR hs.marketing_action_tag2 LIKE '%Scoring Mix%') THEN NULL --old test, no longer needed confirmed by martyna
            WHEN (hs.contact_type_segment_test IN ('FACILITY', 'DOCTOR&FACILITY - 2IN1') OR (mql.country = 'Italy' AND hs.contact_type_segment_test IN ('DIAGNOSTIC'))) -- Clinic Agenda leads, HS report logic replicated
            AND (NOT hs.lead_business_line_sf IN ('DP Phone', 'GIPO', 'TuoTempo') OR hs.lead_business_line_sf IS NULL)
            AND (
                 (--lcs.hs_rep_name = 'salesforce_owner'
                 ld.ownerid = '0057Q000008KjPOQA0' OR hs.hubspot_owner_id = 441140868 OR ld.ownerid IS NOT NULL) --salesforce owner, ---lead status & business line added to historical tables in june 2024
                OR
                (hs.lead_status_sf IN ('Working - Scheduled', 'Assigned', 'Qualifying', 'New', 'New - Not Contacted', 'Working - Contacted')) ---lead status & business line added to historical tables in june 2024
                OR
             ((DATE_TRUNC('month', hs.contact_result_so_at) BETWEEN mql.month AND DATE_ADD('month', 1, mql.month)--if the contact result is next month, dont lose the leads
                    AND ((NOT hs.contact_result_so IN ('wrong lead - transfer to Clinic Cloud', 'wrong lead - transfer to DP Phone', 'wrong lead - transfer to Individuals', 'wrong lead - transfer to PMS'))
                OR hs.contact_result_so IS NULL) AND hs.hubspot_owner_id IN (10420456) AND hs.lead_status_sf IN ('Disqualified (Closed)', 'Closed - Not Scheduled'))) --data quality but considering next month contact result
                 OR --33660674
                  ((DATE_TRUNC('month', hs.contact_result_so_at) BETWEEN mql.month AND DATE_ADD('month', 1, mql.month))
                    AND ((NOT hs.contact_result_so IN ('wrong lead - transfer to Clinic Cloud', 'wrong lead - transfer to DP Phone', 'wrong lead - transfer to Individuals', 'wrong lead - transfer to PMS'))
                OR hs.contact_result_so IS NULL) AND hs.hubspot_owner_id IN (10420456)) --owner DQ
                     OR
                 (hs.lead_status_sf = 'Converted' AND hs.clinic_agenda_opportunity_status_sf = 'Closed Lost'
            AND (mql.month BETWEEN DATEADD('month', -1, DATE_TRUNC('month', hs.clinic_agenda_opportunity_close_date_sf)) AND DATE_TRUNC('month', hs.clinic_agenda_opportunity_close_date_sf))
                ) --CA lost same month or next month (to account for end of month leads being closed next month)
                OR
                 (hs.lead_status_sf = 'Converted' AND hs.bundle_opportunity_status_sf = 'Closed Lost' AND
                (mql.month BETWEEN DATEADD('month', -1, DATE_TRUNC('month', hs.bundle_opportunity_close_date_sf)) AND DATE_TRUNC('month', hs.bundle_opportunity_close_date_sf))
                ) --bundles lost same month  or next month (to account for end of month leads being closed next month)
                 ) THEN 'Clinic Agenda'
            --WHEN hs.lead_transferred_to_so = 'Clinic Online Agenda' AND DATE_TRUNC('month', hs.facility_lead_transferred_to_so_wf) = mql.month THEN 'Clinic Agenda'
            END AS mql_product, --11¡
            hs.facility___product_recommended__wf, --12
            COALESCE(CASE WHEN COALESCE(hs.facility_size, hcl.facility_size) LIKE ('%Individual%')
                        OR COALESCE(hs.facility_size, hcl.facility_size) LIKE ('%Small%') THEN 'Small'
                    WHEN COALESCE(hs.facility_size, hcl.facility_size) LIKE ('%Large%')
                        OR COALESCE(hs.facility_size, hcl.facility_size) LIKE ('%Mid%') THEN 'Medium'
                    ELSE 'Unknown'
                    END, 'Unknown') AS facility_size,--13
            hs.source_so,--14
            hs.last_source_so,--15
            CASE WHEN hs.mql_last_touch_channel_wf = 'Offline reference' THEN 'active_source'
            WHEN paid.hubspot_id IS NOT NULL THEN 'active_source'
            WHEN hs.mql_last_touch_channel_wf = 'Event' THEN 'active_source'
            WHEN LOWER(hs.source_so) IN ('free audit',
                                      'free audit facility', 'conference',
                                      'helpline', 'livechat',
                                      'direct mail',
                                      'feegow interested',
                                      'buy form',
                                      'offer request facility',
                                      'offer request',
                                      'profile premium upgrade - improve bookings',
                                      'offer request cta', 'contact form doctor zone',
                                      'callpage', 'callcenter meetings',
                                      'bundle (ca & feegow) offer request',
                                      'bundle (ca & feegow) interested [whatsapp]',
                                      'integrations', 'facebook ad',
                                      'whatsapp widget', 'telemedicine interested',
                                      'website interested', 'partnership',
                                      'website interested', 'partnership',
                                      'plan 360 offer request', 'profile premium upgrade - phone on profile',
                                      'sales demo ordered feegow', 'demo request saas facility',
                                      'free trial feegow', 'cliniccloud call',
                                      'profile premium upgrade - improve visibility',
                                      'gipo callpage',
                                      'gipo contact form',
                                      'cliniccloud direct email',
                                      'discount offer facility',
                                      'discount offer',
                                      'clinic cloud offer request',
                                      'dp phone free audit',
                                      'dp phone request',
                                      'tuotempo offer request facility',
                                      'fc offer request') THEN 'active_source'
            ELSE 'passive_source'
            END AS active_passive,--16
            CASE WHEN deal.deal_id IS NOT NULL THEN TRUE END AS deal_flag,--17
            deal.month AS deal_month,--18
            CASE WHEN hs.mql_last_touch_channel_wf = 'Offline reference' THEN 'Referral'
            WHEN paid.hubspot_id IS NOT NULL THEN 'Paid_direct'
            WHEN hs.mql_last_touch_channel_wf = 'Event' THEN 'Events_direct' -- HS latest source known
            WHEN hcl.hs_analytics_source = 'PAID_SOCIAL'
                AND LOWER(hcl.hs_analytics_source_data_1) IN('facebook', 'instagram', 'linkedin') THEN 'Paid'
            WHEN hcl.hs_analytics_source = 'PAID_SEARCH'
                AND (LOWER(hcl.hs_analytics_source_data_1) IN ('yahoo', 'bing', 'google')
                OR LOWER(hcl.hs_analytics_source_data_2) LIKE '%_%'
                OR LOWER(hcl.hs_analytics_source_data_1) LIKE '%_%'
                OR LOWER(hcl.hs_analytics_source_data_2) = 'google') THEN 'Paid'
            WHEN hs.doctor_facility___marketing_events_tag IS NOT NULL THEN 'Events'
            WHEN hs.affiliate_source LIKE '%callcenter%'
                OR LOWER(hs.marketing_action_tag2) LIKE '%callcenter%' THEN 'Call Center'
            ELSE 'Organic/Direct'
            END AS db_channel_short,--19
            CASE WHEN hs.verified THEN 'Verified' ELSE 'Not Verified' END AS verified,
            hs.lead_transferred_to_so,--20
            deal.deal_stage_start,--21
            hs.contact_result_so_at,--22
            COALESCE(hs.feegow_lead_at_test, hcl.feegow_lead_at_test) AS feegow_lead_at,--23
            hs.facility_lead_transferred_to_so_wf, --25
            CASE WHEN hs.country = 'Brazil' AND (hs.last_source_so IS NULL OR LOWER(hs.last_source_so) LIKE '%target tool%' OR LOWER(hs.last_source_so)  LIKE '%sales contact%') AND (date_trunc('month',hs.feegow_lead_at_test) = mql.month) THEN TRUE --excluding leads that were overstamped as MQLs due to wrong local workflows
            WHEN (DATE_TRUNC('month', hs.facility_lead_transferred_to_so_wf) = mql.month) AND hs.lead_transferred_to_so = 'Marketplace Team' THEN TRUE --excluding leads transferred to Individuals in the same month
            WHEN hs.country = 'Poland' AND (mql.month = DATE_TRUNC('month', hs.last_source_so_at)) AND hs.last_source_so IN  ('Contact form GIPO', 'GIPO Contact Form', 'ClinicCloud direct email', 'ClinicCloud call', 'ClinicCloud offer request',
            'Product interested S4C/PMS', 'Sales demo ordered GIPO', 'GIPO Demo', 'CallPage GIPO', 'GIPO Callpage','Free trial Feegow', 'Demo watched Feegow', 'Offer Request GipoDental', 'Visiting pricing Feegow')
            AND hs.sub_brand_wf_test LIKE '%MyDr%' THEN TRUE
            WHEN hs.last_source_so LIKE '%Noa%' AND (hs.source_so LIKE '%Noa%' OR hs.source_so IN ('Target tool [DA]', 'Sales Contact', 'Massive assignment') OR (DATE_TRUNC('month', hs.recent_source) < mql.month)) THEN TRUE --excluding Noa MQLs unless they were also CA MQLs same month
            --WHEN hs.merged_vids IS NOT NULL AND hm.hubspot_id IS NULL AND lcs.lifecycle_stage = 'MQL' AND lcs_is_month_new AND lcs.month != DATE_TRUNC('month', hs.lcs_mql_at_test) AND lcs.month != DATE_TRUNC('month', hs.last_marketing_influenced_at_test) THEN TRUE --workaround when merges in HS cause an old MQL to be counted in CJ. checking influenced stamp additionally because sometimes a new MQL stamp is overriden with an old one
            WHEN hs.country = 'Italy' AND (DATE_TRUNC('DAY', mql.lifecycle_stage_start) = '2025-01-29') THEN TRUE --remains of Gipo rain lead issue on 29.01
            WHEN hs.member_customers_list = 'Yes' AND (DATE_TRUNC('month', hs.noa_lead_at) = mql.month) THEN TRUE --Noa CM MQLs exclusion
            WHEN mql.country = 'Poland' AND hs.facility_size IN ('Small', 'Individual (1)', 'Individual (2)') THEN TRUE
            WHEN mql.country = 'Brazil' AND mql.lifecycle_stage_start IN ('2024-10-22 00:00:00.000000', '2024-04-15 00:00:00.000000', '2025-01-17 00:00:00.000000', '2025-07-16 00:00:00.000000') AND (hs.last_source_so = 'Target tool [DA]' OR hs.last_source_so IS NULL) THEN TRUE --ssome sort of WF errors in HS that was corrected manually by BR team or MOT
            WHEN mql.country = 'Brazil' AND hs.last_source_so = 'Online consultation form'  AND mql.month IN ('2024-12-01', '2025-01-01') THEN TRUE --for some reason this LS was excluded in dec count, for Jan MOT did manual updated in hs
           -- WHEN mql.country = 'Spain' AND mql.lifecycle_stage_start IN ('2025-01-21 00:00:00.000000') THEN TRUE --Spain manually excluded 40 leads from KPI sheet, excluding 1 day for Pigment as I dont know which leads
            WHEN excl.hubspot_id IS NOT NULL THEN TRUE -- lead transfer issue in Jan Feb
            WHEN mql.country = 'Brazil' AND hs.source_so = 'visiting pricing' AND hs.marketing_action_tag2 LIKE '%BRCLPE_Pricing_Test%' THEN TRUE
            WHEN mql.country IN ('Brazil', 'Italy') AND mql.month >= '2024-06-01' AND hs.lead_business_line_sf IS NULL THEN TRUE --from July 24 we should always have business line info. done for pigment number matching
            WHEN hs.lead_business_line_sf IN ('DP Phone') THEN TRUE
            ELSE FALSE END AS mql_excluded,--26
            hs.lead_business_line_sf,--27
            hs.lead_status_sf,--28
            deal.sf_source,--29
            hs.sub_brand_wf_test AS subbrand,--30
            CASE WHEN hs.active_passive_lead = 'Outbound' THEN 'Passive' ELSE COALESCE(hs.active_passive_lead, 'Passive') END AS active_passive_lead,
            hs.customer_reference_at,
            hs.facility_lead_tagging_system_monthly_update_workflow AS fac_lds,
            ROW_NUMBER() OVER (PARTITION BY mql.contact_id, mql.month ORDER BY hs.start_date DESC) AS row_per_hist
        FROM leads_deduped mql
        LEFT JOIN dw.hs_contact_live_history hs ON hs.hubspot_id = mql.contact_id AND
         CASE WHEN mql.merged_issue THEN hs.start_date = mql.earliest_record_hs
            ELSE hs.start_date BETWEEN DATE_TRUNC('day', mql.lifecycle_stage_start) AND DATEADD('day', 15, mql.lifecycle_stage_start)
             END
        LEFT JOIN dw.hs_contact_live hcl ON hcl.hubspot_id = mql.contact_id
        LEFT JOIN test.lead_transfer_issue_mqls_jan_2025 excl ON excl.hubspot_id = mql.contact_id AND DATE_TRUNC('month', excl.create_date) = mql.month
        LEFT JOIN WON_deals deal ON mql.contact_id = deal.hs_contact_id
        LEFT JOIN dp_salesforce.lead ld ON ld.hs_contact_id__c = mql.contact_id AND ld.businessline__c IN ('Clinic Agenda')  AND DATE_TRUNC('month', createddate::date) = mql.month
        LEFT JOIN mart_cust_journey.msv_paid_media_campaign_hubspot_2 paid ON mql.contact_id = paid.hubspot_id
            AND paid.campaign like '%mql%' AND paid.campaign_product != 'Noa' AND paid.target = 'FACILITY'
            AND (mql.month = DATE_TRUNC('month', paid.date))
       WHERE 1=1 --AND mql.contact_id = 761267863
    QUALIFY row_per_hist = 1
    ),

    only_wons AS ( --subquery to consider all Inbound WONs (even if there was no corresponding MQLs in the prior subquery (edge case))
        SELECT
            COALESCE(CAST(deal.hs_contact_id AS bigint), 0) AS hs_contact_id,--1
            deal.deal_id,--2
            deal.email AS hs_email,--3
            deal.country,--4
            deal.deal_stage_start AS lifecycle_stage_start,--5
            DATE_TRUNC('month', deal.month) AS MONTH,--6
            'only_won'::varchar AS lifecycle_stage,--7
            'FACILITY'::varchar AS target_aux,--8
            deal.mrr_euro_final,--9
            deal.product_won,--10
            deal.product_won AS mql_product,--10
            'Clinic Agenda'::varchar AS facility___product_recommended__wf,--11
            deal.facility_size,--12
            deal.source_so,--13
            deal.last_source_so,--14
            CASE WHEN deal.mql_channel = 'Offline reference' THEN 'active_source'
            WHEN paid.hubspot_id IS NOT NULL THEN 'active_source'
            WHEN deal.mql_channel = 'Event' THEN 'active_source'
            WHEN LOWER(deal.source_so) IN ('free audit',
                                      'free audit facility',
                                      'conference',
                                      'helpline', 'livechat', 'direct mail',
                                      'feegow interested',
                                      'buy form', 'offer request facility',
                                      'offer request', 'profile premium upgrade - improve bookings',
                                      'offer request cta', 'contact form doctor zone',
                                      'callpage', 'customer reference',
                                      'callcenter meetings',
                                      'bundle (ca & feegow) offer request',
                                      'bundle (ca & feegow) interested [whatsapp]',
                                      'integrations', 'facebook ad',
                                      'whatsapp widget', 'telemedicine interested',
                                      'website interested', 'partnership',
                                      'plan 360 offer request',
                                      'profile premium upgrade - phone on profile',
                                      'sales demo ordered feegow',
                                      'demo request saas facility', 'free trial feegow',
                                      'cliniccloud call',
                                      'profile premium upgrade - improve visibility',
                                      'gipo callpage', 'gipo contact form',
                                      'cliniccloud direct email', 'discount offer facility', 'discount offer',
                                      'clinic cloud offer request',
                                      'dp phone free audit', 'dp phone request',
                                      'tuotempo offer request facility',
                                      'fc offer request') THEN 'active_source'
            ELSE 'passive_source'
            END AS active_passive,--15
            CASE WHEN deal.deal_id IS NOT NULL THEN TRUE END AS deal_flag, --16
            deal.month AS deal_month, --17
            CASE WHEN deal.mql_channel = 'Offline reference' THEN 'Referral'
            WHEN paid.hubspot_id IS NOT NULL THEN 'Paid_direct'
            WHEN deal.mql_channel = 'Event' THEN 'Events_direct'
            WHEN deal.hs_analytics_source = 'PAID_SOCIAL'
                AND LOWER(deal.hs_analytics_source_data_1) IN ('facebook', 'instagram', 'linkedin') THEN 'Paid'
            WHEN deal.hs_analytics_source = 'PAID_SEARCH' AND (LOWER(deal.hs_analytics_source_data_1) IN ('yahoo', 'bing', 'google')
                OR LOWER(deal.hs_analytics_source_data_2) LIKE '%_%'
                OR LOWER(deal.hs_analytics_source_data_1) LIKE '%_%'
                OR LOWER(deal.hs_analytics_source_data_2) = 'google') THEN 'Paid'
            WHEN deal.doctor_facility___marketing_events_tag IS NOT NULL THEN 'Events'
            WHEN deal.affiliate_source like '%callcenter%' OR LOWER(deal.marketing_action_tag2) LIKE '%callcenter%' THEN 'Call Center'
            ELSE 'Organic/Direct'
            END AS db_channel_short,--18
            deal.verified,
            deal.lead_transferred_to_so,--21
            deal.deal_stage_start,--22
            DATE_TRUNC('month', deal.month) AS contact_result_so_at,--23
            deal.feegow_lead_at,--24
            deal.facility_lead_transferred_to_so_wf, --26
            FALSE AS mql_excluded,
            deal.lead_business_line_sf,
            deal.lead_status_sf,
            deal.sf_source,
            deal.subbrand,
            deal.active_passive_lead,
            deal.customer_reference_at,
           'FACILITY'::varchar AS fac_lds,
            1 AS row_per_hist
        FROM WON_deals deal
        LEFT JOIN mart_cust_journey.cj_contact_facts contact_facts ON contact_facts.contact_id = deal.hs_contact_id
        LEFT JOIN dp_salesforce.opportunity sf ON sf.id = deal.deal_id
        LEFT JOIN mart_cust_journey.msv_paid_media_campaign_hubspot_2 paid ON deal.hs_contact_id = paid.hubspot_id
            AND deal.deal_stage_start BETWEEN paid.web_date AND paid.web_date::DATE + INTERVAL '30 day'
            AND campaign like '%mql%'
            AND (deal.month >= DATE_TRUNC('month', paid.date))
        --WHERE DEAL.hs_contact_id = 105030095184
    ),

    total_mqls AS (
        SELECT * FROM all_mqls
        WHERE 0=0
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,  11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34
        QUALIFY ROW_NUMBER() OVER (PARTITION BY contact_id, month ORDER BY lifecycle_stage_start DESC) = 1
        UNION
        SELECT *
        FROM only_wons
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,  11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34
)

    SELECT
        mql.contact_id,
        mql.deal_id,
        mql.hs_email,
        mql.country,
        mql.deal_month,
        mql.month,
        mql.mql_product,
        mql.fac_lds,
        mql.lifecycle_stage,
        mql.lifecycle_stage_start,
        CASE WHEN mql.target_aux = 'check_pms' AND mql_product IN ('Clinic Cloud', 'Feegow', 'MyDr', 'Gipo') THEN 'FACILITY'
        WHEN mql.target_aux = 'FACILITY' THEN 'FACILITY'
        ELSE NULL END AS target,
        mql.product_won,
        mql.facility___product_recommended__wf,
        mql.facility_size,
        mql.source_so,
        mql.last_source_so,
        mql.active_passive,
        mql.deal_flag,
        CASE WHEN db_channel_short = 'Paid_direct' THEN db_channel_short
        WHEN (month = DATE_TRUNC('month', customer_reference_at) OR DATE_DIFF('day', lifecycle_stage_start, customer_reference_at) BETWEEN -10 AND 10) THEN 'Referral'
        WHEN mql.db_channel_short = 'Organic/Direct' AND mql_product = 'Clinic Agenda' AND
        mql.country IN ('Spain', 'Italy', 'Brazil') AND (mql.subbrand LIKE '%Gipo%' OR mql.subbrand LIKE '%Feegow%' OR mql.subbrand LIKE '%Clinic Cloud%') THEN 'PMS database'
        ELSE mql.db_channel_short
        END AS db_channel_short, --adding new source for Clinic Agenda leads, to spot if they come from PMS database
        mql.mrr_euro_final,
        mql.facility_lead_transferred_to_so_wf AS lead_transferred_at,
        mql.lead_transferred_to_so,
        mql.lead_business_line_sf,
        mql.lead_status_sf,
        mql.sf_source,
        mql.verified,
        mql.active_passive_lead,
        CASE WHEN mql.db_channel_short IN ('Paid_direct', 'Referral', 'Events_direct') OR (month = DATE_TRUNC('month', customer_reference_at) OR DATE_DIFF('day', lifecycle_stage_start, customer_reference_at) BETWEEN -10 AND 10) THEN 'Active' --direct channels are always active mqls
        WHEN mql.month >= '2025-01-01' THEN mql.active_passive_lead
        WHEN mql.month < '2025-01-01' AND mql.active_passive = 'active_source' THEN 'Active'
            ELSE 'Passive' END AS active_passive_final,
        CASE WHEN deal_flag AND deal_stage_start >= lifecycle_stage_start THEN TRUE ELSE FALSE
        END AS true_deal_flag,
        CASE WHEN (month = DATE_TRUNC('month', customer_reference_at) OR DATE_DIFF('day', lifecycle_stage_start, customer_reference_at) BETWEEN -10 AND 10) THEN 'Direct'
           WHEN mql.db_channel_short IN ('Paid_direct', 'Referral', 'Events_direct') THEN 'Direct'
        ELSE 'Indirect' END AS direct_indirect_flag --flag to split Direct vs Indirect MQLs in Tableau,
    FROM total_mqls mql
    LEFT JOIN gipo_leads_to_exclude gp ON gp.hubspot_id = mql.contact_id
    WHERE NOT (hs_email LIKE '%deleted%' AND (deal_flag IS FALSE OR deal_flag IS NULL))
    AND NOT (mql.month = '2025-01-01' AND gp.hubspot_id IS NOT NULL)
    AND (mql_excluded IS FALSE OR mql_excluded IS NULL)
    QUALIFY target = 'FACILITY' AND mql_product IS NOT NULL;

