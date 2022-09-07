WITH raws AS (
  SELECT
    currency_id
    , 	COALESCE(convert(decimal(15,2), case value when '' then null else value end),0.00) as value
--    , value
    , ROW_NUMBER() OVER (PARTITION BY currency_id ORDER BY id DESC) AS rn
  FROM {{ ref('obj_wkfs.currency_rate_cleaned') }}
)
SELECT currency_id, value AS fx_rate FROM raws WHERE rn = 1