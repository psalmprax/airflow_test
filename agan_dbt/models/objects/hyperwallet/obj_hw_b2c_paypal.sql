WITH b2c_payments AS (
   SELECT 
      bt.trade_id,
      bt.user_id,
      bt.payment_id,
      bt.application_id,
      bt.landingpage_id,
      bp.payment_type,
      bp.paypal_id 
   FROM {{ ref('obj_hw_b2c_trades') }} AS bt 
   JOIN {{ ref('obj_hw_payments_paypal')}} AS bp
      ON bt.payment_id = bp.payment_id 
),
b2c_users AS (
   
   SELECT 
       bp.*,
       owu.firstname,
       owu.lastname ,
       owu.email,
       owu.street ,
       owu.birthdate,
       owu.zip,
       owu.city,
       owu.country 
   FROM b2c_payments AS bp
   LEFT JOIN {{ ref('obj_wkfs_user') }} owu 
      ON bp.user_id = owu.id 
)

SELECT 
    bu.trade_id,
    owo.id AS offer_id,
    owo.calculated_payment_date,
    owo.is_offer_paid_mail_sent,
    bu.user_id,
    bu.payment_id,
    bu.paypal_id,
    bu.application_id,
    bu.landingpage_id,
    bu.firstname,
    bu.lastname ,
    bu.email,
    bu.street,
    bu.birthdate,
    bu.city,
    bu.country,
    bu.zip,
    owo.device_id,
    owo.paid_amount 
FROM b2c_users AS bu 
LEFT JOIN {{ ref('obj_hw_latest_payment_not_sent') }} owo 
  ON bu.trade_id = owo.trade_id 
WHERE owo.paid_amount IS NOT NULL 

   

