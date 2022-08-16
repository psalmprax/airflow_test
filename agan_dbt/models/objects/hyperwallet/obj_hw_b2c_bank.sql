WITH bank_app_trade AS (
SELECT 
    obt.trade_id
    , obt.user_id
    , obt.payment_id
    , obt.application_id
    , obt.landingpage_id
    , obt.company_name

FROM {{ ref('obj_hw_bank_app') }} ohba
JOIN {{ ref('obj_hw_b2c_trades') }} obt
  ON obt.application_id = ohba.application_id
)
,bank_app_trade_payments AS (
  SELECT 
    bat.*
    , bp.payment_type
    , bp.paypal_id
    , bp.first_name
    , bp.middle_name
    , bp.last_name
    , bp.swift_iban
    , bp.enc_init_swift
    , bp.enc_bic_swift
    , bp.enc_iban_swift
    , bp._enc_key
    
  FROM bank_app_trade bat
  JOIN {{ ref('obj_hw_payments_bank')}} bp 
    ON bat.payment_id = bp.payment_id
)

,b2c_users AS (
   
  SELECT 
    batp.*
    , owu.firstname
    , owu.lastname 
    , owu.email
    , owu.street
    , owu.birthdate
    , owu.zip
    , owu.city
    , owu.country 
  FROM bank_app_trade_payments AS batp
  LEFT JOIN {{ ref('obj_wkfs_user') }} owu
    ON batp.user_id = owu.id 
)

SELECT 
    bu.trade_id
   , owo.id AS offer_id
   , owo.calculated_payment_date
   , owo.is_offer_paid_mail_sent
   , bu.user_id
   , bu.paypal_id
   , bu.payment_id
   , bu.application_id
   , bu.landingpage_id
   , bu.company_name
   , bu.first_name AS bank_fn
   , bu.middle_name AS bank_mn
   , bu.last_name AS bank_ls
   , bu.swift_iban
   , bu.enc_init_swift
   , bu.enc_bic_swift
   , bu.enc_iban_swift
   , bu._enc_key
   , bu.firstname
   , bu.lastname 
   , bu.birthdate
   , bu.email
   , bu.street
   , bu.city
   , bu.country
   , bu.zip
   , owo.device_id
   , owo.paid_amount 
FROM b2c_users AS bu 
LEFT JOIN {{ ref('obj_hw_latest_payment_not_sent') }} owo 
  ON bu.trade_id = owo.trade_id 
WHERE owo.paid_amount IS NOT NULL
