--https://github.com/asgoodasnu/analytics-transformations/blob/develop/models/objects/wkfs/obj_wkfs_promo_code.sql
--obj_wkfs_promo_code

SELECT
    pc.id
    , pc.name
    , pc.code
    , pc.is_unique
    , pc.min_amount_is_active
    , pc.value
    , pc.min_amount_value
    , pc.created_at
    , pc.valid_until
FROM {{ ref('obj_wkfs.promo_code_cleaned') }} pc