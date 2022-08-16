SELECT
    row_number() over (order by states) as status_code,
    states,
    classification_process,
    classification_stock,
    responsibility,
    department

FROM {{ ref('mapping_offer_states') }}