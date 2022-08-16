SELECT 
	row_number() over( order by date) as dates_id,
    t.*
    ,count(CASE WHEN week_day_num BETWEEN 1 AND 5 THEN 1 END) OVER (partition by year_month) as week_days_in_month
FROM {{ ref('date_series') }} as t
WHERE date >= '2021-01-01'::date and date <= getdate()::date
--    (SELECT
--        EXTRACT(YEAR FROM d) as year
--        ,EXTRACT(month FROM d) as month
--        ,EXTRACT(YEAR FROM d)||'-'||to_char(d,'MM') as year_month
--        ,date(d) as date
--        ,to_char(date(d), 'DD/MM/YYYY') as date_format
--        ,DATE_PART('days', DATE_TRUNC('month', d) + '1 MONTH'::INTERVAL - '1 DAY'::INTERVAL)::INT as days_in_month
--        ,to_char(d, 'Month') as month_name
--        ,to_char(d, 'Day') as week_day
--        ,extract(dow from d) as week_day_num
--        ,concat(to_char(d,'MM'),'-',to_char(d,'DD')) as month_date
--    FROM generate_series('2021-01-01'::date, (date_trunc('month', current_date::date) + interval '1 month' - interval '1 day')::date, '1 day') as gs(d)
--     ) as t

--Use this in the far future
--with series as (
--
--select (date '2021-01-01' + d) as d
--from  generate_series(1, ('5222-01-01'::date - date '2021-01-01')) d
--
--),
--results as (
--
--SELECT
--        EXTRACT(YEAR FROM d) as year
--        ,EXTRACT(month FROM d) as month
--        ,EXTRACT(YEAR FROM d)||'-'||to_char(d,'MM') as year_month
--        ,date(d) as date
--        ,to_char(date(d), 'DD/MM/YYYY') as date_format
--        ,DATE_PART('days', DATE_TRUNC('month', d) + '1 MONTH'::INTERVAL - '1 DAY'::INTERVAL)::INT as days_in_month
--        ,to_char(d, 'Month') as month_name
--        ,to_char(d, 'Day') as week_day
--        ,extract(dow from d) as week_day_num
--        ,to_char(d,'MM') || '-' || to_char(d,'DD') as month_date
--    FROM
--   series
--)
--select * from results
