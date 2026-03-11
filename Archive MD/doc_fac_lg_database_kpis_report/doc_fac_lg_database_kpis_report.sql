WITH db AS (SELECT
    hubspot_id,
    createdate,
    country,
    CASE
        WHEN contact_type_segment_test IN (
            'DOCTOR',
            'GENERAL PRACTITIONER',
            'GENERAL PRACTITIONER & DOCTOR'
        ) THEN 'Doctor'
        WHEN contact_type_segment_test IN (
            'FACILITY',
            'DOCTOR&FACILITY - 2IN1',
            'HEALTH - GENERAL'
        ) THEN 'Facility'
        ELSE 'other'
    END AS segment,
    CASE
        WHEN NULLIF(TRIM(sub_brand_wf_test), '') IS NULL THEN 'MAIN_BRAND'
        WHEN sub_brand_wf_test SIMILAR TO
            '%MioDottore%|%Doctoralia%|%DoktorTakvimi%|%jameda%|%ZnanyLekarz%'
            THEN 'MAIN_BRAND'
        WHEN LOWER(sub_brand_wf_test) LIKE '%noa%' THEN 'NOA'
        ELSE 'PMS'
    END AS brand_bucket,
    doctor_facility___hard_bounced__wf,
    unsubscribed_from_all_emails_at,
    zombies_engagement_level_workflows AS engagement,
    become_engaged_at,
    become_sleeper_at
    FROM dw.hs_contact_live
    WHERE country IN (
        'Italy',
        'Spain',
        'Turkiye',
        'Brazil',
        'Poland',
        'Germany',
        'Argentina',
        'Colombia',
        'Chile',
        'Mexico',
        'Czechia',
        'Peru'
    )
    AND (
        contact_type_segment_test NOT IN ('PATIENT', 'STUDENTS')
        OR contact_type_segment_test IS NULL
    )
    AND is_deleted = FALSE
    AND (
        email NOT SIMILAR TO '%(docplanner|miodottore|doctoralia|znanylekarz|jameda|deleted|gdpr)%'
        OR email IS NULL
    )
    AND (
        hs_lead_status NOT IN ('UNQUALIFIED')
        OR hs_lead_status IS NULL
    )
    AND (
        saas_user_type_batch NOT LIKE '%Secretary%'
        OR saas_user_type_batch IS NULL
    )
    AND (
        member_customers_list != 'Yes'
        OR member_customers_list IS NULL
    )
),

monthly_contacts AS (SELECT
    country,
    segment,
    brand_bucket,
    COUNT(DISTINCT hubspot_id) AS db,
    EXTRACT(MONTH FROM createdate) AS month,
    EXTRACT(YEAR FROM createdate) AS year
    FROM db
    GROUP BY country,
        segment,
        brand_bucket,
        month,
        year
),

cumulative_contacts AS (SELECT
    country,
    segment,
    brand_bucket,
    year,
    month,
    db,
    SUM(db) OVER (
        PARTITION BY
            country,
            segment,
            brand_bucket
        ORDER BY
            year,
            month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_db
    FROM monthly_contacts
),

hardbounce_data AS (SELECT
    db.country,
    db.segment,
    db.brand_bucket,
    db.hubspot_id,
    ROW_NUMBER() OVER (
        PARTITION BY db.hubspot_id
        ORDER BY history.dw_updated_at
    ) AS row_num,
    DATE(history.dw_updated_at) AS date
    FROM db
    INNER JOIN dw.hs_contact_live_history AS history
        ON db.hubspot_id = history.hubspot_id
    WHERE db.doctor_facility___hard_bounced__wf IS NOT NULL
),

hb_count AS (SELECT
    country,
    segment,
    brand_bucket,
    COUNT(hubspot_id) AS hb,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(YEAR FROM date) AS year
    FROM hardbounce_data
    WHERE row_num = 1
    GROUP BY country,
        segment,
        brand_bucket,
        month,
        year
),

unsubscribe_data AS (SELECT
    country,
    segment,
    brand_bucket,
    COUNT(hubspot_id) AS unsub,
    EXTRACT(MONTH FROM unsubscribed_from_all_emails_at) AS month,
    EXTRACT(YEAR FROM unsubscribed_from_all_emails_at) AS year
    FROM db
    WHERE unsubscribed_from_all_emails_at IS NOT NULL
    GROUP BY country,
        segment,
        brand_bucket,
        month,
        year
),

disengaged_to_engaged_data AS (SELECT
    country,
    segment,
    brand_bucket,
    COUNT(hubspot_id) AS disengaged_to_engaged,
    EXTRACT(MONTH FROM become_engaged_at) AS month,
    EXTRACT(YEAR FROM become_engaged_at) AS year
    FROM db
    WHERE become_engaged_at IS NOT NULL
        AND become_sleeper_at IS NOT NULL
        AND DATE(become_sleeper_at) <= DATE(become_engaged_at)
    GROUP BY country,
        segment,
        brand_bucket,
        month,
        year
),

disengaged_data AS (SELECT
    country,
    segment,
    brand_bucket,
    COUNT(hubspot_id) AS disengaged,
    EXTRACT(MONTH FROM become_sleeper_at) AS month,
    EXTRACT(YEAR FROM become_sleeper_at) AS year
    FROM db
    WHERE become_sleeper_at IS NOT NULL
    GROUP BY country,
        segment,
        brand_bucket,
        month,
        year
),

daily_engagement AS (SELECT
    date,
    country,
    CASE
        WHEN contact_type_segment_test IN (
            'DOCTOR',
            'GENERAL PRACTITIONER',
            'GENERAL PRACTITIONER & DOCTOR'
        ) THEN 'Doctor'
        WHEN contact_type_segment_test IN (
            'FACILITY',
            'DOCTOR&FACILITY - 2IN1',
            'HEALTH - GENERAL'
        ) THEN 'Facility'
        ELSE 'other'
    END AS segment,
    CASE
        WHEN NULLIF(TRIM(sub_brand_wf_test), '') IS NULL THEN 'MAIN_BRAND'
        WHEN sub_brand_wf_test SIMILAR TO
            '%MioDottore%|%Doctoralia%|%DoktorTakvimi%|%jameda%|%ZnanyLekarz%'
            THEN 'MAIN_BRAND'
        WHEN LOWER(sub_brand_wf_test) LIKE '%noa%' THEN 'NOA'
        ELSE 'PMS'
    END AS brand_bucket,
    SUM(engaged) OVER (
        PARTITION BY
            country,
            CASE
                WHEN contact_type_segment_test IN (
                    'DOCTOR',
                    'GENERAL PRACTITIONER',
                    'GENERAL PRACTITIONER & DOCTOR'
                ) THEN 'Doctor'
                WHEN contact_type_segment_test IN (
                    'FACILITY',
                    'DOCTOR&FACILITY - 2IN1',
                    'HEALTH - GENERAL'
                ) THEN 'Facility'
                ELSE 'other'
            END,
            CASE
                WHEN NULLIF(TRIM(sub_brand_wf_test), '') IS NULL THEN 'MAIN_BRAND'
                WHEN sub_brand_wf_test SIMILAR TO
                    '%MioDottore%|%Doctoralia%|%DoktorTakvimi%|%jameda%|%ZnanyLekarz%'
                    THEN 'MAIN_BRAND'
                WHEN LOWER(sub_brand_wf_test) LIKE '%noa%' THEN 'NOA'
                ELSE 'PMS'
            END,
            date
    ) AS daily_engaged,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    ROW_NUMBER() OVER (
        PARTITION BY
            country,
            CASE
                WHEN contact_type_segment_test IN (
                    'DOCTOR',
                    'GENERAL PRACTITIONER',
                    'GENERAL PRACTITIONER & DOCTOR'
                ) THEN 'Doctor'
                WHEN contact_type_segment_test IN (
                    'FACILITY',
                    'DOCTOR&FACILITY - 2IN1',
                    'HEALTH - GENERAL'
                ) THEN 'Facility'
                ELSE 'other'
            END,
            CASE
                WHEN NULLIF(TRIM(sub_brand_wf_test), '') IS NULL THEN 'MAIN_BRAND'
                WHEN sub_brand_wf_test SIMILAR TO
                    '%MioDottore%|%Doctoralia%|%DoktorTakvimi%|%jameda%|%ZnanyLekarz%'
                    THEN 'MAIN_BRAND'
                WHEN LOWER(sub_brand_wf_test) LIKE '%noa%' THEN 'NOA'
                ELSE 'PMS'
            END,
            EXTRACT(YEAR FROM date),
            EXTRACT(MONTH FROM date)
        ORDER BY date DESC
    ) AS row_num
    FROM gds_extract.user_engagement_daily
)

SELECT
    cc.country,
    cc.brand_bucket,
    cc.segment,
    TO_CHAR(TO_DATE(cc.year || '-' || cc.month || '-01', 'YYYY-MM-DD'), 'YYYY-MM-DD') AS date,
    cc.db AS new_db,
    (COALESCE(hc.hb, 0) + COALESCE(ud.unsub, 0)) AS db_loss,
    cc.db - (COALESCE(hc.hb, 0) + COALESCE(ud.unsub, 0)) AS db_balance,
    ROUND(
        (cc.db - (COALESCE(hc.hb, 0) + COALESCE(ud.unsub, 0)))::FLOAT
        / NULLIF(cc.db, 0),
        2
    ) AS db_balance_perc,

    COALESCE(de.daily_engaged, 0) AS tot_engaged,
    COALESCE(dte.disengaged_to_engaged, 0) AS re_engaged,
    COALESCE(dd.disengaged, 0) AS disengaged,
    COALESCE(dte.disengaged_to_engaged, 0) - COALESCE(dd.disengaged, 0)
    AS re_engaged_db_balance,
    ROUND(
        (COALESCE(dte.disengaged_to_engaged, 0) - COALESCE(dd.disengaged, 0))::FLOAT
        / NULLIF(COALESCE(dte.disengaged_to_engaged, 0), 0),
        2
    ) AS re_engaged_balance_perc


FROM cumulative_contacts AS cc
LEFT JOIN hb_count AS hc
    ON cc.country = hc.country
        AND cc.segment = hc.segment
        AND cc.brand_bucket = hc.brand_bucket
        AND cc.year = hc.year
        AND cc.month = hc.month
LEFT JOIN unsubscribe_data AS ud
    ON cc.country = ud.country
        AND cc.segment = ud.segment
        AND cc.brand_bucket = ud.brand_bucket
        AND cc.year = ud.year
        AND cc.month = ud.month
LEFT JOIN disengaged_to_engaged_data AS dte
    ON cc.country = dte.country
        AND cc.segment = dte.segment
        AND cc.brand_bucket = dte.brand_bucket
        AND cc.year = dte.year
        AND cc.month = dte.month
LEFT JOIN disengaged_data AS dd
    ON cc.country = dd.country
        AND cc.segment = dd.segment
        AND cc.brand_bucket = dd.brand_bucket
        AND cc.year = dd.year
        AND cc.month = dd.month
LEFT JOIN daily_engagement AS de
    ON cc.country = de.country
        AND cc.segment = de.segment
        AND cc.brand_bucket = de.brand_bucket
        AND cc.year = de.year
        AND cc.month = de.month
        AND de.row_num = 1
WHERE TO_DATE(cc.year || '-' || cc.month || '-01', 'YYYY-MM-DD') >= DATE '2025-01-01'
