SELECT
-- TBU: select relevant columns
--    /* id / primary & foreign key fields */
    "off".id

    -- table: device, manufactor
    , "off".model -- 1740
    , "off".device_id -- 598
    , "off".device -- 13
    , "off".usage -- 65
    , "off".manufactor_name -- 1690
    , "off".manufactor_id
    , "off".series
    , "off".edition
    , "off".capacity -- 1750
    , "off".hw_branding
    , "off".sw_branding
    , "off".simlock

--     table: user
    , usr.email -- 3
    , usr.firstname -- 4
    , usr.lastname -- 5
    , usr.full_name -- 304
    , usr.street -- 6
    , usr.zip -- 7
--    , usr.plzip -- 637
    , usr.city -- 8
    , usr.country -- 9
    , usr.is_optin -- 63
    , usr.salutation -- 81
--
--     table: b2b_trade_detail
    , tr.b2b_order_id -- 1127

--     table: offer
    , "off".offer -- 1
    , "off".id AS offer_id -- 2
    , "off".trade_id -- 10
    , "off".is_paid_string

    , "off".amount -- 14
    , "off".amount_original -- 14 original
    , "off".accepted_amount -- 15
    , "off".accepted_amount_original -- 15 original
    , "off".converted_amount -- 16, no original
    , "off".converted_amount_cent -- 17, no original
    , "off".paid_amount -- 18
    , "off".paid_amount_original -- 18 original
    , "off".paid_amount_2 -- 19 same as paid_amount ?!
    , "off".paid_amount_2_original -- 19 original
    , "off".commission_amount -- 42
    , "off".net_amount -- 70
    , "off".net_amount_original -- 70 original
    , "off".condition_id
    , "off".condition_title
    , "off".condition_code -- 75
    , "off".condition_user -- 82 and 1852
    , "off".original_amount -- 84
    , "off".original_amount_original -- 84 original
--
    , "off".cost_refurbishments -- 481
    , "off".refurbishments -- 481
    , "off".retoure_description -- 541
    , "off".retoure_reason_detail -- 1710
    , "off".retoure_reason_type
    , "off".retoure_is_warranty -- 1122

    , "off".rsp_trade_created -- 556
    , "off".rsp_trade_set_paid -- 559
    , "off".all_tracking_campaign -- 625
    , "off".tracking_campaign_group -- 622
    , "off".condition_original -- 631
    , "off".product_check_device_condition
    , "off".simlock_net_customer_answer -- 1114
    , "off".battery_condition_customer_answer -- 1708
    , "off".latest_offer_state_change_at -- 1116
    , "off".latest_offer_state_change_at_rts
--
    , "off".product_check_field_other -- 565
    , "off".product_check_field_technical_state -- 1102
    , "off".offer_state -- 838
    , "off".article_number -- 1694
    , "off".ios_battery_capacity -- 1686
    , "off".imei -- 1775
    , "off".first_stock_id
    , "off".first_stock_in
    , "off".last_stock_in -- 1734
    , "off".first_stock_out
    , "off".last_stock_out
    , "off".first_stock_out_channel
    , "off".first_clarification_start
    , "off".first_clarification_end
    , "off".first_clarification_accepted
    , "off".hdd_size -- 1713
    , "off".tax -- 71
--
    , "off".donation_amount
    , "off".donation_amount_original
    , "off".promo_amount

--     table: application
    , tr.app_name -- app.name AS application_name -- 11

    /* filter field */
    , NOT "off".date_paid IS NULL AS is_offer_paid
    , NOT "off".date_paid_mail_sent IS NULL AS is_offer_paid_mail_sent
    , NOT tr.deleted = 0 AS is_trade_deleted
    , NOT "off".deleted = 0 AS is_offer_deleted

    /* date filter fields */
    , tr.created_at AS trade_created_at
    , tr.updated_at AS trade_updated_at
    , "off".date_paid AS offer_paid_at -- 29
    , "off".date_paid_mail_sent AS offer_paid_mail_sent_at -- 1125
    , "off".calculated_payment_date AS offer_calculated_payment_date -- 1126
--
--     table: trade
    , tr.created_at AS trade_date_time
    , tr.created_at::DATE AS trade_date
    , tr.created_at::TIMESTAMPTZ -- WITH TIME ZONE AS trade_time
    , tr.login_email -- 391
    , tr.login_name -- 394
    , tr.last_login_at -- 487
    , tr.eno_contact_number -- 490
    , tr.ubm_partner
    , tr.dhl_identcode -- 77
    , tr.is_fastlane -- 80
    , tr.userfields -- 69
    , tr.company_name -- 1776
    , tr.huawei_store_email --
    , tr.commission_value --
    , tr.trade_sf_guard_user
    , tr.app_currency_id

--     table: donation_project
    , "off".donation_project
--
--     table: promo_code
    , "off".promo_code -- 35
    , "off".promo_name -- 628
    , "off".promo_value -- 36
    , "off".promo_value_type -- 37
--
--     table: landingpage
    , tr.landingpage_id
    , tr.landingpage -- 62
--
--     payment columns
    , pay.payment_type -- 41
    , pay.account_owner_direct_debit -- 31
    , pay.account_owner_direct_debit_austria -- 86
    , pay.account_owner_direct_debit_spain -- 424
    , pay.paypal_id -- 61
    , pay.paypal_id_type
    , pay.simyophone -- 49
    , pay.blauphone -- 39
    , pay.otto_credit_cn_track_id -- 44
    , pay.allmaxxid -- 38
    , pay.swift_account_owner -- 72
    , pay.swift_bic -- 74
    , pay.swift_iban -- 73
    , pay.cancom_customer_number -- 574
--
--     other
    , tr.creator_e_plus_sapid
    , tr.creator_kostenstelle
    , usr.province
    , "off".business_channel_type
    , "off".business_channel
    , tr.location
    , "off".category
    , "off".category_id
--
--     stock aggregate information
    , "off".number_of_stocks
    , "off".number_of_stock_outs
    , "off".latest_stock_in
    , "off".latest_stock_out
    , "off".is_stock_out
    , "off".id_of_active_stock_for_offer
FROM {{ ref('obj_wkfs_offer') }} AS "off"
    LEFT JOIN {{ ref('obj_wkfs_trade') }} AS tr
        ON tr.transaction_id = "off".trade_id
    LEFT JOIN {{ ref('obj_wkfs_payment') }} AS pay
        ON pay.id = tr.payment_id
    LEFT JOIN {{ ref('obj_wkfs_user') }} AS usr
        ON usr.id = tr.user_id
WHERE NOT tr.transaction_id IS NULL
