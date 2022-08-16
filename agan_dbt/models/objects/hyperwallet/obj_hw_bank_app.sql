WITH bank_app AS (
 SELECT 
  id AS application_id,
  "name",
  CASE
    WHEN "name" like '%huawei%' THEN 0
    WHEN "name" like '%samsung%' then 0
    ELSE 1
  END AS app_bank_match
-- FROM raw_wkfs.application
 FROM {{ ref("obj_wkfs.application_cleaned") }}
)
,filter_bank_app AS (

   SELECT
     application_id
   FROM bank_app
   WHERE app_bank_match = 1

)
SELECT * FROM filter_bank_app order by convert(numeric, application_id) asc
