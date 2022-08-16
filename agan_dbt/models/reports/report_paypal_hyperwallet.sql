
WITH hw_b2c AS
(
  SELECT
      offer_id AS id
	  , calculated_payment_date
	  , is_offer_paid_mail_sent
	  , 649476011683 AS "progId.programId"
	  , md5(email) AS "custId.clientCustomerId"
	  , 'INDIVIDUAL' AS "profile.entityType"
	  , NULL AS "profile.accountType"
	  , firstname AS "profile.firstName" 
	  , lastname AS "profile.lastName"
	  , NULL AS "profile.businessContactAddress.street"
	  , NULL AS "profile.businessContactAddress.city"
	  , NULL AS "profile.businessContactAddress.country"
	  , NULL AS "profile.businessContactAddress.postCode"
	  , birthdate AS "profile.dateOfBirth"
	  , NULL AS "profile.businessName"
	  , street AS "profile.street"
	  , city AS "profile.city"
	  , country AS "profile.country"
	  , zip AS "profile.postCode"
	  , email AS "prefs.emailAddress"
	  , paid_amount AS "payment.amount"
	  , 'EUR' AS "payment.currencyCode"
--	  , concat(cast(offer_id AS text),'-',cast(trade_id AS text)) AS "payment.clientReferenceNumber"
      , cast(offer_id AS text)||'-'||cast(trade_id AS text) AS "payment.clientReferenceNumber"
--	  , concat(cast(trade_id AS text),'-',cast(offer_id AS text)) AS "payment.description"
	  , cast(trade_id AS text)||'-'||cast(offer_id AS text) AS "payment.description"
	  , 'OTHER' AS "payment.purposeOfPayment"
	  , paypal_id AS "payment.memo"
	  , 'PAYPAL_ACCOUNT' AS "ea.externalAccountType"
	  , 'EUR' AS "ea.currencyCode"
	  , NULL AS "ea.bankCode"
	  , paypal_id AS "ea.accountNumber"
	  , country AS "ea.destinationCountryCode"
	  , NULL AS "ea.firstName"
      , NULL AS "ea.middleName"
      , NULL AS "ea.lastName"
	  , 649474601683 AS "payment.fundingProgramId"
	  , NULL::text enc_init_swift
      , NULL::text enc_bic_swift
      , NULL::text enc_iban_swift
      , NULL::varchar _enc_key
  FROM {{ ref('obj_hw_b2c_paypal') }}

),hw_b2b AS (
	SELECT
        offer_id AS id
	    , calculated_payment_date
		, is_offer_paid_mail_sent
		, 649476011683 AS "progId.programId"
		, md5(email) AS "custId.clientCustomerId"
		, 'COMPANY' AS "profile.entityType"
		, 'PrivateCompany' AS "profile.accountType"
		, firstname AS "profile.firstName"
		, lastname AS "profile.lastName"
		, street AS "profile.businessContactAddress.street"
		, city AS "profile.businessContactAddress.city"
		, country AS "profile.businessContactAddress.country"
		, zip AS "profile.businessContactAddress.postCode"
		, birthdate AS "profile.dateOfBirth"
		, company_name AS "profile.businessName"
		, street AS "profile.street"
		, city AS "profile.city"
		, country AS "profile.country"
		, zip AS "profile.postCode"
		, email AS "prefs.emailAddress"
		, paid_amount AS "payment.amount"
		, 'EUR' AS "payment.currencyCode"
--	  , concat(cast(offer_id AS text),'-',cast(trade_id AS text)) AS "payment.clientReferenceNumber"
      , cast(offer_id AS text)||'-'||cast(trade_id AS text) AS "payment.clientReferenceNumber"
--	  , concat(cast(trade_id AS text),'-',cast(offer_id AS text)) AS "payment.description"
	  , cast(trade_id AS text)||'-'||cast(offer_id AS text) AS "payment.description"
		, 'OTHER' AS "payment.purposeOfPayment"
		, paypal_id AS "payment.memo"
	    , 'PAYPAL_ACCOUNT' AS "ea.externalAccountType"
	    , 'EUR' AS "ea.currencyCode"
		, NULL AS "ea.bankCode"
	    , paypal_id AS "ea.accountNumber"
	    , country AS "ea.destinationCountryCode"
		, NULL AS "ea.firstName"
        , NULL AS "ea.middleName"
        , NULL AS "ea.lastName"
	    , 649474601683 AS "payment.fundingProgramId"
		, NULL::text enc_init_swift
        , NULL::text enc_bic_swift
        , NULL::text enc_iban_swift
        , NULL::varchar _enc_key

	FROM {{ ref('obj_hw_b2b_paypal') }}
)
, paypal_union AS (
SELECT
    * 
FROM hw_b2b
UNION
SELECT * FROM hw_b2c
)
, hw_b2c_bank AS (
 SELECT
      offer_id AS id
	  , calculated_payment_date
	  , is_offer_paid_mail_sent
	  , 649476011683 AS "progId.programId"
	  , md5(email) AS "custId.clientCustomerId"
	  , 'INDIVIDUAL' AS "profile.entityType"
	  , NULL AS "profile.accountType"
	  , firstname AS "profile.firstName" 
	  , lastname AS "profile.lastName"
	  , NULL AS "profile.businessContactAddress.street"
	  , NULL AS "profile.businessContactAddress.city"
	  , NULL AS "profile.businessContactAddress.country"
	  , NULL AS "profile.businessContactAddress.postCode"
	  , birthdate AS "profile.dateOfBirth"
	  , NULL AS "profile.businessName"
	  , street AS "profile.street"
	  , city AS "profile.city"
	  , country AS "profile.country"
	  , zip AS "profile.postCode"
	  , email AS "prefs.emailAddress"
	  , paid_amount AS "payment.amount"
	  , 'EUR' AS "payment.currencyCode"
--	  , concat(cast(offer_id AS text),'-',cast(trade_id AS text)) AS "payment.clientReferenceNumber"
      , cast(offer_id AS text)||'-'||cast(trade_id AS text) AS "payment.clientReferenceNumber"
--	  , concat(cast(trade_id AS text),'-',cast(offer_id AS text)) AS "payment.description"
	  , cast(trade_id AS text)||'-'||cast(offer_id AS text) AS "payment.description"
	  , 'OTHER' AS "payment.purposeOfPayment"
	  , paypal_id AS "payment.memo"
	  , 'BANK_ACCOUNT_EUROPE' AS "ea.externalAccountType"
	  , 'EUR' AS "ea.currencyCode"
	  , NULL AS "ea.bankCode"
	  , NULL AS "ea.accountNumber"
	  , country AS "ea.destinationCountryCode"
	  , bank_fn as "ea.firstName"
      , bank_mn as "ea.middleName"
      , bank_ls as "ea.lastName"
	  , 649474601683 AS "payment.fundingProgramId"
	  , enc_init_swift
      , enc_bic_swift
      , enc_iban_swift
      , _enc_key
  FROM {{ ref('obj_hw_b2c_bank') }}
),
hw_b2b_bank AS (
	SELECT
        offer_id AS id
	    , calculated_payment_date
		, is_offer_paid_mail_sent
		, 649476011683 AS "progId.programId"
		, md5(email) AS "custId.clientCustomerId"
		, 'COMPANY' AS "profile.entityType"
		, 'PrivateCompany' AS "profile.accountType"
		, firstname AS "profile.firstName"
		, lastname AS "profile.lastName"
		, street AS "profile.businessContactAddress.street"
		, city AS "profile.businessContactAddress.city"
		, country AS "profile.businessContactAddress.country"
		, zip AS "profile.businessContactAddress.postCode"
		, birthdate AS "profile.dateOfBirth"
		, company_name AS "profile.businessName"
		, street AS "profile.street"
		, city AS "profile.city"
		, country AS "profile.country"
		, zip AS "profile.postCode"
		, email AS "prefs.emailAddress"
		, paid_amount AS "payment.amount"
		, 'EUR' AS "payment.currencyCode"
--	  , concat(cast(offer_id AS text),'-',cast(trade_id AS text)) AS "payment.clientReferenceNumber"
      , cast(offer_id AS varchar)||'-'::varchar||cast(trade_id AS varchar) AS "payment.clientReferenceNumber"
--	  , concat(cast(trade_id AS text),'-',cast(offer_id AS text)) AS "payment.description"
	  , cast(trade_id AS varchar)||'-'::varchar||cast(offer_id AS varchar) AS "payment.description"
		, 'OTHER' AS "payment.purposeOfPayment"
		, paypal_id AS "payment.memo"
	    , 'BANK_ACCOUNT_EUROPE' AS "ea.externalAccountType"
	    , 'EUR' AS "ea.currencyCode"
		, NULL AS "ea.bankCode"
	    , NULL AS "ea.accountNumber"
	    , country AS "ea.destinationCountryCode"
		, bank_fn as "ea.firstName"
        , bank_mn as "ea.middleName"
        , bank_ls as "ea.lastName"
	    , 649474601683 AS "payment.fundingProgramId"
		, enc_init_swift
        , enc_bic_swift
        , enc_iban_swift
        , _enc_key

	FROM {{ ref('obj_hw_b2b_bank') }}
)
, bank_union AS (
SELECT
    * 
FROM hw_b2b_bank
UNION
SELECT * FROM hw_b2c_bank
)

SELECT 
  *
FROM paypal_union
UNION
SELECT * FROM bank_union
