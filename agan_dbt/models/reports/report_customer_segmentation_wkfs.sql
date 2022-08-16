SELECT

  offer_id AS id
  , user_id
  , email
  , firstname
  , lastname
  , street
  , number_trades
  , first_trade
  , created_at
  , year_created
  , total_accepted_amount
  , total_converted_amount
  , avg_accepted_amount
  , ta
  , device 
  , category
  , model
  , promo_value
  , number_retoure
  , retoure_reason_type
  , retoure_description
  , count_back_to_seller
  , count_clarification_questionnaire
  , count_clarification_technical
  , count_clarification_optical
  , count_clarification_general_data
       
FROM {{ ref('wkfs_segmentation_calculations') }}
