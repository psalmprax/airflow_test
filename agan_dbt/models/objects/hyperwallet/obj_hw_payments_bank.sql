
WITH bank_payment AS (
  SELECT 
    id AS payment_id
    , payment_type_id
    , payment_type
    , paypal_id
    , swift_account_owner
    , split_to_array(trim(swift_account_owner),' ') AS rsa
    , NULL AS swift_iban
    , enc_init_swift
    , enc_bic_swift
    , enc_iban_swift
    , _enc_key
  FROM {{ ref('obj_wkfs_payment') }} owp
  WHERE payment_type_id = 13
)
, card_rsa AS (
SELECT
  payment_id
  , payment_type_id
  , payment_type
  , paypal_id
  , swift_account_owner
  , swift_iban
  , rsa
  , enc_init_swift
	, enc_bic_swift
	, enc_iban_swift
  , _enc_key
  , CASE
      WHEN get_array_length(rsa) = 1
      THEN 1
      WHEN get_array_length(rsa) = 2
      THEN 2
      WHEN get_array_length(rsa) = 3
      THEN 3
    END AS cardin
FROM bank_payment
)
, swift_split AS (
 SELECT
    *
    , rsa[0] AS first_name
    , CASE
        WHEN cardin = 3
        THEN rsa[1]
      END middle_name
    , CASE
        WHEN cardin = 2
        THEN rsa[1]
        WHEN cardin = 3
        THEN rsa[2]
      END AS last_name
  FROM card_rsa
)

SELECT
  payment_id
  , payment_type_id
  , payment_type
  , paypal_id
  , swift_account_owner
  , first_name::varchar
  , middle_name::varchar
  , last_name::varchar
  , swift_iban
  , enc_init_swift
  , enc_bic_swift
  , enc_iban_swift
  , _enc_key
FROM swift_split
