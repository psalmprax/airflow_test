WITH b2b_payments AS (

   SELECT 
     bt.trade_id,
	   bt.user_id,
	   bt.payment_id,
	   bt.application_id,
	   bt.landingpage_id,
     bt.company_name,
     bp.payment_type,
     bp.paypal_id 

   FROM {{ ref('obj_hw_b2b_trades') }} AS bt 
   INNER JOIN {{ ref('obj_hw_payments_paypal')}} AS bp
      ON bt.payment_id = bp.payment_id 

)
,b2b_users AS (
   
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
   FROM b2b_payments AS bp
   LEFT JOIN {{ ref('obj_wkfs_user') }} owu 
      ON bp.user_id = owu.id 
)

SELECT 
   bu.trade_id,
   owo.id AS offer_id,
   owo.calculated_payment_date,
   owo.is_offer_paid_mail_sent,
   bu.user_id,
   bu.paypal_id,
   bu.payment_id,
   bu.application_id,
   bu.landingpage_id,
   bu.company_name,
   bu.firstname,
   bu.lastname ,
   bu.birthdate,
   bu.email,
   bu.street,
   bu.city,
   bu.country,
   bu.zip,
   owo.device_id,
   owo.paid_amount 
FROM b2b_users AS bu 
LEFT JOIN {{ ref('obj_hw_latest_payment_not_sent') }} owo 
  ON bu.trade_id = owo.trade_id 
WHERE owo.paid_amount IS NOT NULL

   
   
