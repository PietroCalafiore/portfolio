--first we select the number of questions that were asked to doctors, by country and month
With
    Questions
    as (
SELECT
      country_code,
    date_trunc('month', created_at) as month,
       count(DISTINCT id || country_code)  as question_count
FROM stage_marketplace.doctor_question
WHERE deleted_at IS NULL
  AND is_accepted = 1
  AND is_moderated = 1
GROUP BY 1,2
),

-- next step is done as Germany is the only country missing from the questions table
-- so we add it manually for all months so we can use this CTE as base for future joins
      month_year as (
        select country_code, month as month,
               row_number() over(partition by month) as row from questions

        ),
questions_final as (
select * from Questions
UNION
select 'de' as country_code,
 DATE(my.month) as month,
0 as question_count
from
month_year my
UNION
select 'tr' as country_code,
date_trunc('month', current_date) as month,
0 as question_count
from
month_year my

),


answers as (
---Number of answers provided (cumulative)
SELECT
       country_code,
    date_trunc('month', dqa.created_at) as month,
       count(DISTINCT id ||country_code) as answer_count
FROM stage_marketplace.doctor_question_answer dqa
WHERE deleted_at IS NULL
  AND answered_at IS NOT NULL
 GROUP BY 1,2
 ORDER BY 1),

--next calculations are done to calculate Unique doctors providing answers.
--We calculate the month/year the doctor answered a question for the first time
 first_answer as(
SELECT doctor_id,country_code,
                           min(date_trunc('month', dqa.created_at)) as first_entry_date
                    FROM  stage_marketplace.doctor_question_answer dqa
                    WHERE deleted_at IS NULL
  AND answered_at IS NOT NULL
                    GROUP by 1,2),
 --then we count cumulative count of unique doctors per month per country
    ranked as(
    select count(*) over (partition by country_code order by first_entry_date rows unbounded preceding) as counts, first_entry_date, country_code
from first_answer)
,
 --as final step we select the max value for each month, which shows the unique doctors providing answers for that month and country
    docs_answer as(
SELECT country_code,first_entry_date as month,
       max(counts) as docs_answering
FROM ranked
GROUP BY 1,2  ),
--monthly bookings from Marketplace & Saas cpme from the dw.booking table
     monthly_bookings as (
         SELECT b.country_code,
        date_trunc('month', booked_at) :: date AS month,
       COUNT(DISTINCT booking_id)             AS bookings
FROM dw.booking b
         LEFT JOIN dw.doctor d
                   ON b.doctor_id = d.doctor_id
                       AND d.is_deleted IS FALSE
WHERE b.recurrency = 0
  AND (b.is_moved = 0 OR b.is_moved IS NULL)
  AND b.is_deleted IS FALSE
  AND d.is_test IS FALSE
  --AND b.booked_at <= '2020-07-01'
  AND b.booked_at <= b.visited_at
GROUP BY 1,2
union
--Germany´s pre-merger bookings are added manually as they are not in the database
select
'de' as country_code,
'2002-01-01' as month,
4541000 as bookings from
stage.kpi_mart_tt


/*
union all
 SELECT b.country_code,
        --date_trunc('month', booked_at) :: date AS month,
       COUNT(DISTINCT booking_id)             AS bookings
FROM dw.booking b
         LEFT JOIN dw.doctor d
                   ON b.doctor_id = d.doctor_id
                       AND d.is_deleted IS FALSE
WHERE is_booking
  AND b.is_deleted IS FALSE
  AND b.booked_at > '2020-07-01'
GROUP BY 1*/
         )
     ,
    -- we calculate how many employees were active for each month and country by looking at their employment start and end date
    employees as (
        select extract(year from month) as yr,
      extract(month from month) as mt,
     s.country as country_code,
      count(1) as employees
from dw.teamshare_employee s
left join questions_final my on my.country_code = country
    where (month between coalesce(s.employment_start_date, '1900-01-01')
                and coalesce(s.termination_date,'2099-01-01'))
and is_deleted is false
and (employment_type <> 'Non employee (payroll provider)'
or employment_type is null) --exclude fake employees
and termination_date is null
group by extract(year from month)
       , extract(month from month),
         s.country
order by 1,2,3
    )
    ,
    reviews as (
         select o.country_code,
      DATE_TRUNC('month', created_at) AS month,
       count(o.id) AS opinions
from stage_marketplace.opinion o
where
 o.is_accepted = 1 and country_code != 'de' or
 ( o.is_accepted = 1 and country_code = 'de' and o.created_at between '2020-11-01' and current_date) --Jameda has a lot of reviews pre-merger but we only consider ones starting in NOV 20
group by 1,2
    ),
    doctor_profiles as (
        SELECT country_code,
date::date as month,
doctor_profiles
FROM (SELECT country_code,
date,
sum(t1.value)
OVER (PARTITION BY country_code ORDER BY date ROWS UNBOUNDED PRECEDING) AS doctor_profiles
FROM (SELECT d.country_code,
date_trunc('month', d.dw_created_at - 1) AS date,
count(DISTINCT root_id)                  AS value
FROM dw.doctor d
JOIN dw.country c
ON c.country_code = d.country_code
WHERE (d.status_id NOT IN (1, 4) OR d.status_id IS NULL)
AND d.is_deleted IS NOT TRUE
AND origin = 'DP'
AND d.country_code IN  ('ar', 'br', 'cl', 'co', 'cz', 'es', 'it', 'mx', 'pe', 'pl', 'pt', 'tr','de')
GROUP BY d.country_code, date) t1) t2
--WHERE date >= '2021-01-01'
    ),
 user_sessions AS (
 SELECT v.country_code,
                   to_date(yearmonth, 'YYYYMM') AS month,
                   sum(users)                   AS unique_users,
                   sum(sessions)             AS sessions
            FROM google_analytics.investors_kpi ik
                     JOIN google_analytics.view v
                          ON v.view_id = ik.view_id
            WHERE v.country_code IN ('ar', 'br', 'cl', 'co', 'cz', 'es', 'it', 'mx', 'pe', 'pl', 'pt', 'tr')
              AND v.ga_account = 'v3'
              AND v.property_scope = 'Marketplace'
              AND v.view_scope = 'General'
            and yearmonth < '2023-01-01'
            GROUP BY 1, 2
            union all
            SELECT ik.country_code,
                   DATE_TRUNC('month', month) AS month,
                   sum(users)                   AS unique_users,
                   sum(sessions)             AS sessions
            FROM google_analytics_bq.investors_kpi ik
            WHERE ik.country_code IN ('de')
            GROUP BY 1, 2
            union all
            SELECT ik.country_code,
                   DATE_TRUNC('month', month) AS month,
                   sum(users)                   AS unique_users,
                   sum(sessions)             AS sessions
            FROM google_analytics_bq.investors_kpi ik
            WHERE ik.country_code IN ('ar', 'br', 'cl', 'co', 'cz', 'es', 'it', 'mx', 'pe', 'pl', 'pt', 'tr')
            and DATE_TRUNC('month', month) >= '2023-01-01'
            GROUP BY 1, 2
)

select coalesce(mb.country_code,q.country_code,da.country_code,a.country_code,r.country_code,dp.country_code,us.country_code,em.country_code) as country_code,
       coalesce(mb.month,q.month,a.month,da.month,mb.month,r.month,dp.month,us.month) as month,
       question_count,
       answer_count,
       docs_answering,
       bookings as bookings,
       opinions as reviews,
       doctor_profiles as doctor_profiles,
       unique_users as unique_users,
       sessions AS sessions,
       employees
from
 questions_final q full outer join monthly_bookings mb
on q.country_code = mb.country_code and mb.month=q.month
full outer join answers a on q.country_code=a.country_code and q.month=a.month
full outer join  docs_answer da on da.country_code = q.country_code and da.month = q.month
full outer join  reviews r on r.country_code =q.country_code and q.month=r.month
full outer join  doctor_profiles dp on dp.country_code =q.country_code and q.month=dp.month
full outer join  user_sessions us on us.country_code=q.country_code and q.month=us.month
full outer join  employees em on em.yr = extract(year from q.month) and em.mt=extract(month from q.month) and em.country_code=q.country_code
ORDER BY 1
