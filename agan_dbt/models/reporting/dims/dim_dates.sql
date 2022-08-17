WITH dates AS (

  SELECT *
  FROM {{ ref("date_series")}} as t
  WHERE date >= '2013-01-01'::date and date <= getdate()::timestamptz + INTERVAL '30 DAYS'
--  (CURRENT_DATE AT TIME ZONE 'Europe/Berlin') + INTERVAL '30 DAYS'
--    GENERATE_SERIES (
--        '2013-01-01'
--      , (CURRENT_DATE AT TIME ZONE 'Europe/Berlin') + INTERVAL '30 DAYS'
--      , INTERVAL '1 DAY'
--    ) AS date

), combined AS (

  select distinct
  ROW_NUMBER() OVER (ORDER BY date ASC) AS date_id, * from
  (select

      DATE(date) AS date
    , DATE(DATE_TRUNC('year', date)) AS year_start
    , EXTRACT(year FROM date) AS year
    , EXTRACT(year FROM date) AS isoyear
    , DATE(DATE_TRUNC('quarter', date)) AS quarter_start
    , EXTRACT(quarter FROM date) AS quarter
    , DATE(DATE_TRUNC('month', date)) AS month_start
    , EXTRACT(month FROM date) AS month
    , DATE(DATE_TRUNC('week', date)) AS week_start
    , EXTRACT(week FROM date) AS week
    , TO_CHAR(date, 'IYYY-IW') AS iso_week
    , TO_CHAR(date, 'IYYY-ID') AS iso_weekday
--    , EXTRACT(isodow FROM date) AS iso_weekday
    , EXTRACT(day FROM date) AS day
    , TO_CHAR(date, 'dy') AS day_name
    , EXTRACT(doy FROM date) AS day_num
    , EXTRACT(dow FROM date) AS weekday
    , EXTRACT(year FROM date) || TO_CHAR(date, '"-W"IW') AS week_of_year
    , TO_CHAR(date, 'YYYY-MM') AS month_of_year
    , EXTRACT(year FROM date) || TO_CHAR(date, '"-Q"Q') AS quarter_of_year

  FROM dates
  ) as dt

), final AS(

  SELECT
    *
    , DENSE_RANK() OVER(ORDER BY month ASC) AS month_num
    , DENSE_RANK() OVER(ORDER BY week ASC) AS week_num

  FROM combined

)

SELECT * FROM final
