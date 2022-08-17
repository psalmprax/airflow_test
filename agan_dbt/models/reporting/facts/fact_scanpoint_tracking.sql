--SELECT
--
--  id
--  , employee_id
--  , employee_back_office_name
--  , employee_real_name
--  , scanpoint_id
--  , scanpoint_name
--  , department_id
--  , department_name
--  , offer_id
--  , device_id
--  , condition_id
--  , condition_title
--  , manufactor_id
--  , manufactor_name
--  , deleted
--  , category_id
--  , category_name AS category
--  , timestamp_in
--  , timestamp_out
--  , started_on
--  , completed_on
--  , working_time
--
--FROM {{ ref('obj_scanpoint_tracking') }}
with ost as (
	SELECT
	  *
	from {{ ref('obj_scanpoint_tracking') }}
),
rs as (
	select cast(offer_id as text) as offer_id_new
	  , business_channel_type
  	  , business_channel
  	from {{ ref('report_stock') }}
),
results as (
	select * from ost
	left join rs
	on ost.offer_id = rs.offer_id_new
)

select
    distinct
    id
  , employee_id
  , employee_back_office_name
  , employee_real_name
  , scanpoint_id
  , scanpoint_name
  , department_id
  , department_name
  , offer_id
  , device_id
  , condition_id
  , condition_title
  , manufactor_id
  , manufactor_name
  , deleted
  , category_id
  , category_name AS category
  , timestamp_in
  , timestamp_out
  , started_on
  , completed_on
  , working_time
  , business_channel_type
  , business_channel
from results
where offer_id !~ '[a-zA-Z]{1}'
and device_id is not null
order by offer_id asc