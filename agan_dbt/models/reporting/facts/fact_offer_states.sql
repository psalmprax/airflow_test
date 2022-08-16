WITH last_stock AS (

  SELECT * FROM (SELECT
      offer_id
    , stock_out
    , stock_in
    , business_channel_type
    , business_channel
    , ROW_NUMBER() OVER (
        PARTITION BY offer_id
        ORDER BY stock_in DESC
      ) AS last_stock_in

  FROM {{ ref('report_stock') }} ) st
  WHERE last_stock_in = 1

)
, offer_last_state_of_day AS (

  SELECT 
  * 
  FROM 
  (SELECT
      *
    -- In case of multiple state changes per day, we selected the final change
    -- of the day.
    , ROW_NUMBER() OVER(
        PARTITION BY offer_id, created_at::DATE
        ORDER BY created_at DESC
      ) AS last_state_per_day

  FROM {{ ref('obj_wkfs_offer_offer_state_set') }}
  ) tmp
  WHERE last_state_per_day = 1

)
, mapping AS (

   SELECT * FROM {{ ref('dim_status') }}

), new_state AS (

  SELECT 
	  s.*
	  , owo.category_id , owo.category, owo.condition_id , owo.deleted 
	  , LEAD(s.created_at) OVER (PARTITION BY s.offer_id ORDER BY s.created_at ASC) AS new_state_created_at
	  , LEAD(s.offer_state) OVER (PARTITION BY s.offer_id ORDER BY s.created_at ASC) AS new_state
	  , ROW_NUMBER() OVER (PARTITION BY s.offer_id ORDER BY s.created_at DESC) AS last_offer_state
  FROM 
  (
  SELECT
    ols.*
    , ls.business_channel_type
    , ls.business_channel
    , ls.stock_in AS latest_stock_in
    , ls.stock_out AS latest_stock_out

  FROM offer_last_state_of_day AS ols
    LEFT JOIN last_stock AS ls
      ON ls.offer_id = ols.offer_id 
   ) s
  LEFT JOIN  analytics_objects.obj_wkfs_offer owo
     ON s.offer_id = owo.id

), offer_stock_out AS (

  SELECT
      *
--    /*
--     * This condition is a combination of two main observations in the data.
--     *
--     * First,
--     * offers with final states '15 stock out' or '39 sold' should be considered
--     * as finalized on the date one of the above states is set.
--     * This date is prefered to stock_out date, as in some cases the stock_out
--     * is populated at a later stage and thus, using it, might lead to
--     * misleading consideration of offers per day and status.
--     *
--     * Second,
--     * there are offers with only one state record (not any of the above two),
--     * but having a stock_out date.
--     * For these offers, we consider the stock_out date as the point of the
--     * final state.
--     *
--     */
    , CASE
        WHEN last_offer_state = 1 AND offer_state IN ('15 stock out', '39 sold')
        THEN created_at + INTERVAL '1 day'
        ELSE COALESCE(new_state_created_at, latest_stock_out + INTERVAL '1 day')
      END AS state_valid_until

  FROM new_state

), series AS (

  SELECT
      gs.gs::DATE AS date
    , offer_id
    , offer_state
    , category_id
    , category
    , deleted
    , condition_id
    , created_at AS state_valid_from
    , new_state
    , new_state_created_at
    , state_valid_until
    , business_channel_type
    , business_channel
    , latest_stock_in
    , latest_stock_out

  FROM offer_stock_out,
  GENERATE_SERIES(created_at::DATE,
    COALESCE((state_valid_until::DATE - INTERVAL '1 day')::DATE
      , CURRENT_DATE), INTERVAL '1 day') AS gs

), states_mapping AS (

  SELECT
      s.*
    , m.classification_process
    , m.classification_stock
    , m.department
    , m.responsibility

  FROM series AS s
    LEFT JOIN mapping AS m
      ON m.states = s.offer_state
)
SELECT distinct * FROM states_mapping
