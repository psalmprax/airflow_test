{{ config(
    materialized='incremental'
  , unique_key='_id'
  , on_schema_change="sync_all_columns"

) }}

WITH offer_state AS (

  SELECT
      date
    -- PK for incremental load
    , offer_state || '_' || (date::TEXT) || '_' || category_id || '_' || condition_id || '_' || deleted AS _id
    , offer_state
    , classification_process
    , classification_stock
    , department
    , responsibility
    , category_id
    , category
    , deleted
    , condition_id
    , COUNT(offer_id) AS number_of_offers
    -- These aggregations are relevant for the avg calculations below.
    , MIN(state_valid_from::DATE) AS oldest_created_at_offer
    , SUM(date - state_valid_from::DATE) AS total_age_of_offers
    , business_channel_type
    , business_channel

FROM {{ ref("fact_offer_states") }} -- analytics_reporting.fact_offer_states

    WHERE date >= (SELECT MAX(date) - INTERVAL '1 day' FROM {{ this }}

 GROUP BY 1,2,3,4,5,6,7,8,9,10,11,15,16

), calculations_state AS (

  SELECT
      _id
    , date
    , offer_state
    , category_id
    , category
    , deleted
    , condition_id
    , classification_process
    , classification_stock
    , department
    , responsibility
    , number_of_offers
    , business_channel_type
    , business_channel
    , date - oldest_created_at_offer AS duration_of_oldest_offer_in_days
    , total_age_of_offers/number_of_offers
      AS avg_age_of_offers_per_status_in_days

  FROM offer_state

), calculations_department AS (

  SELECT
      date
    , department
    , ROUND(SUM(total_age_of_offers)/SUM(number_of_offers),2)
      AS avg_age_of_offers_per_department_in_days

  FROM offer_state
  GROUP BY 1,2

), final AS (

  SELECT
      cs.*
    -- This value should be review as distinct by date and department.
    , cd.avg_age_of_offers_per_department_in_days

  FROM calculations_state AS cs
    LEFT JOIN calculations_department AS cd
      ON cd.date = cs.date
      AND cd.department = cs.department
)

SELECT * FROM final
