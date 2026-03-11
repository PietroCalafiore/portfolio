ITH noa_deal_stages AS ( --selecting main Open deal stages per deal (
    SELECT
        cj.deal_id,
            MAX(CASE WHEN cj.deal_stage = 'Initial Contact' OR hsd.doctor_facility___first_contact_at__wf IS NOT NULL OR opportunity.reached_decision_maker_date__c IS NOT NULL OR opportunity.reached_stakeholder_date__c IS NOT NULL THEN 1 ELSE 0 END) AS is_contacted,
            MAX(CASE WHEN cj.deal_stage = 'Demo / Sales Meeting Done' OR hsd.demo_watched_at IS NOT NULL OR opportunity.date_demo_done__c IS NOT NULL THEN 1 ELSE 0 END) AS is_demo_done,
            MAX(CASE WHEN cj.deal_stage = 'Contract Signed' OR hsd.launch_date_forecast_s IS NOT NULL OR opportunity.contract_signed_date__c IS NOT NULL THEN 1 ELSE 0 END) AS is_proposals_approved
    FROM mart_cust_journey.cj_deal_month cj
    LEFT JOIN dw.hs_deal_live hsd
            ON cj.deal_id = hsd.hubspot_id
                AND cj.crm_source = 'hubspot'
        LEFT JOIN dp_salesforce.opportunity
            ON cj.deal_id = opportunity.id
                AND cj.crm_source = 'salesforce'
    WHERE cj.pipeline = 'Noa'
    AND (hsd.noa_notes_trial_yes_no != 'Yes' OR hsd.noa_notes_trial_yes_no IS NULL) AND
        (opportunity.negotiation_type__c != 'Trial' OR opportunity.negotiation_type__c IS NULL)
        --filtering out Trial deals, we only want Paid
    AND cj.month >= '2025-01-01'
    AND LOWER(hsd."tag") LIKE '%inb%'--logic aligned with RevOps to select Inbound deals only
    GROUP BY 1
