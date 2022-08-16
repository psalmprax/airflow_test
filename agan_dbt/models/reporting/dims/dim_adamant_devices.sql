WITH devices AS (

  SELECT * FROM {{ ref('obj_adamant_devices') }}

)

SELECT * FROM devices

