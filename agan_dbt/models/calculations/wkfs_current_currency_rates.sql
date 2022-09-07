WITH raws AS (
  SELECT
    currency_id
    , value::convert(decimal(15,2) as value
    , ROW_NUMBER() OVER (PARTITION BY currency_id ORDER BY id DESC) AS rn
  FROM {{ ref('obj_wkfs.currency_rate_cleaned') }}
)
SELECT currency_id, value AS fx_rate FROM raws WHERE rn = 1