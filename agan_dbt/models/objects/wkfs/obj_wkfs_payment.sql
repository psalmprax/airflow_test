SELECT
  p.id
  , p.payment_type_id
  , pt.label AS payment_type
  , p_dd.account_owner AS account_owner_direct_debit
  , p_dda.account_owner AS account_owner_direct_debit_austria
  , p_ddsp.account_owner AS account_owner_direct_debit_spain
  , p_pp.paypal_id
  , p_pp.paypal_id_type
  , p_sc.simphone AS simyophone
  , p_bc.blauphone
  , p_oc.cntrackid AS otto_credit_cn_track_id
  , p_ac.allmaxxid
  , p_dds.swift_account_owner
  , p_dds.iban AS swift_iban
  , p_dds.bic AS swift_bic
  , p_cc.cancom_customer_number
  -- add new encrypted bank columns
  , p_dds.initialization_vector as enc_init_swift
  , p_dds.bank_identifier_code as enc_bic_swift
  , p_dds.international_bank_account_number as enc_iban_swift
  , 'wvnJQHB3gyv6qUCfg615Vjam'::varchar(max) as _enc_key
  
FROM {{ ref('obj_wkfs.payment_cleaned') }} AS p
  LEFT JOIN {{ ref('obj_wkfs.payment_type_cleaned') }} AS pt
    ON pt.id = p.payment_type_id
  LEFT JOIN {{ ref('obj_wkfs.payment_direct_debit_cleaned') }} AS p_dd
    ON p_dd.payment_id = p.id
  LEFT JOIN {{ ref('obj_wkfs.payment_paypal_cleaned') }} AS p_pp
    ON p_pp.payment_id = p.id
  LEFT JOIN {{ ref('obj_wkfs.payment_simyo_credits_cleaned') }} AS p_sc
    ON p_sc.payment_id = p.id
  LEFT JOIN {{ ref('obj_wkfs.payment_blau_credits_cleaned') }} AS p_bc
    ON p_bc.payment_id = p.id
  LEFT JOIN {{ ref('obj_wkfs.payment_otto_credits_cleaned') }} AS p_oc
    ON p_oc.payment_id = p.id
  LEFT JOIN {{ ref('obj_wkfs.payment_allmaxx_credits_cleaned') }} AS p_ac
    ON p_ac.payment_id = p.id
  LEFT JOIN {{ ref('obj_wkfs.payment_direct_debit_swift_cleaned') }} AS p_dds
    ON p_dds.payment_id = p.id
  LEFT JOIN {{ ref('obj_wkfs.payment_direct_debit_austria_cleaned') }} AS p_dda
    ON p_dda.payment_id = p.id
  LEFT JOIN {{ ref('obj_wkfs.payment_direct_debit_spain_cleaned') }} AS p_ddsp
    ON p_ddsp.payment_id = p.id
  LEFT JOIN {{ ref('obj_wkfs.payment_cancom_cleaned') }} AS p_cc
    ON p_cc.payment_id = p.id