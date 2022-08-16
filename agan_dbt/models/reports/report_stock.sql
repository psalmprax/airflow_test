SELECT
    -- data from stock object table
    st.id
    , st.offer_id
    , st.stock_in 
    , st.stock_out 
    , st.location_id -- tbu
    , st.created_at
    , st.updated_at 
    , st.status

     -- device information
    , "off".device
    , "off".device_id
    , "off".article_number
    , "off".category
    , "off".manufactor_name
    , st.color
    , st.serial
    , st.ovp
    , "off".imei
    , "off".series
    , "off".hw_branding
    , "off".sw_branding
    , "off".simlock
    , "off".offer_state
    , "off".model
    , "off".edition
    , "off".capacity
    , "off".business_channel_type
    , "off".business_channel

    -- device condition
    , "off".condition_id
    , "off".condition_code
    , "off".condition_title
    , "off".condition_original

    -- repair table
    , st.boxnumber
    , st.calculated_amount
    , st.repair_contractor_name
    , st.repair_costs
    , st.repair_out_at
    , st.repair_start_at
    , st.repair_stop_at
    , st.repair_description
    , st.repair_new_optical_state
    , st.repair_is_battery_changed
    , st.is_repaired

    -- clarification table
    , st.clarification_start_at
    , st.clarification_end_at

    -- stock out table
    , st.stock_out_info
    , st.stock_out_price
    , st.stock_out_price_original
    , st.stock_out_shipping_costs
    , st.stock_out_shipping_costs_original
    , st.stock_out_channel
    , st.storage_time

    -- customer information
    , "off".trade_id
    , "off".app_name
    , "off".firstname
    , "off".lastname
    , "off".full_name -- 304
    , "off".street -- 6
    , "off".zip -- 7
--    , "off".plzip -- 637
    , "off".city -- 8
    , "off".country -- 9
    , "off".is_optin -- 63
    , "off".salutation -- 81

    -- offer information
    , "off".paid_amount
    , "off".paid_amount_original
    , "off".donation_amount
    , "off".donation_amount_original
    , "off".last_stock_in
    , "off".last_stock_out
    , "off".offer_paid_at
    , "off".latest_offer_state_change_at
    , "off".latest_offer_state_change_at_rts
    , "off".tax
    , "off".promo_value
    , "off".location
    , "off".b2b_order_id
    , "off".landingpage
    , "off".rsp_trade_set_paid
    , "off".is_paid_string

    -- mixed source calculations
    , st.stock_out_price - "off".paid_amount + st.stock_out_shipping_costs AS margin
--    , TO_CHAR(GETDATE() - "off".latest_offer_state_change_at, 'dd')::BIGINT AS latest_offer_state_changed_in_days
    , DATEDIFF("day", "off".latest_offer_state_change_at, GETDATE())::BIGINT AS latest_offer_state_changed_in_days

--    , TO_CHAR(GETDATE() - "off".latest_offer_state_change_at_rts, 'dd')::BIGINT AS latest_offer_state_changed_in_days_rts
    , DATEDIFF("day", "off".latest_offer_state_change_at_rts, GETDATE())::BIGINT AS latest_offer_state_changed_in_days_rts

--    , TO_CHAR(GETDATE() - "off".last_stock_in, 'dd')::BIGINT AS last_stock_in_range
    , DATEDIFF("day","off".last_stock_in, GETDATE())::BIGINT AS last_stock_in_range


    , CASE WHEN DATEDIFF("day", "off".last_stock_in, GETDATE())::BIGINT < 30
         THEN '<30'
         ELSE
            CASE WHEN DATEDIFF("day", "off".last_stock_in, GETDATE())::BIGINT BETWEEN 30 AND 60
                 THEN '30-60'
                 ELSE
                    CASE WHEN DATEDIFF("day", "off".last_stock_in, GETDATE())::BIGINT BETWEEN 60 AND 90
                         THEN '60-90'
                         ELSE '+90'
                    END
            END
    END AS age_structure

    -- dimension on base category_id + manufactor_id + device_id + condition_id for ticket AS-3891
    -- product_check_device_condition is always a device in state 5
    -- NO CLUE WHAT TO DO FROM HERE
    ,
    ("off".category_id::varchar ||
    "off".manufactor_id::varchar ||
    "off".device_id::varchar ||
    CASE WHEN "off".product_check_device_condition IS NULL THEN 5 ELSE "off".product_check_device_condition END::varchar)
    ::BIGINT AS stocklist_id

    , woslclsd.offers_created_last_seven_days
    , woslclfd.offers_created_last_fourteen_days
    , woslslsd.offers_sold_last_seven_days
    , woslslfd.offers_sold_last_fourteen_days
    , woslsisd.offers_stock_in_seven_days
    , woslsifd.offers_stock_in_fourteen_days
    , wostlst.offers_stock_time
    , wostlst.stock AS offers_stock_rts_online


FROM {{ ref('obj_wkfs_stock') }} AS st
    LEFT JOIN {{ ref('report_offer') }} AS "off"
        ON "off".id::BIGINT = st.offer_id::BIGINT
    LEFT JOIN {{ ref('wkfs_offer_stock_list_created_last_seven_days') }} AS woslclsd
        ON ("off".category_id ||"off".manufactor_id ||"off".device_id || CASE WHEN "off".product_check_device_condition IS NULL THEN 5 ELSE "off".product_check_device_condition END)::BIGINT = woslclsd.id
    LEFT JOIN {{ ref('wkfs_offer_stock_list_created_last_fourteen_days') }} AS woslclfd
        ON ("off".category_id || "off".manufactor_id || "off".device_id || CASE WHEN "off".product_check_device_condition IS NULL THEN 5 ELSE "off".product_check_device_condition END)::BIGINT = woslclfd.id
    LEFT JOIN {{ ref('wkfs_offer_stock_list_sales_last_seven_days') }} AS woslslsd
        ON ("off".category_id || "off".manufactor_id||"off".device_id || CASE WHEN "off".product_check_device_condition IS NULL THEN 5 ELSE "off".product_check_device_condition END)::BIGINT = woslslsd.id
    LEFT JOIN {{ ref('wkfs_offer_stock_list_sales_last_fourteen_days') }} AS woslslfd
        on ("off".category_id || "off".manufactor_id || "off".device_id || CASE WHEN "off".product_check_device_condition IS NULL THEN 5 ELSE "off".product_check_device_condition END)::BIGINT = woslslfd.id
    LEFT JOIN {{ ref('wkfs_offer_stock_list_stock_in_seven_days') }} AS woslsisd
        ON ("off".category_id || "off".manufactor_id || "off".device_id || CASE WHEN "off".product_check_device_condition IS NULL THEN 5 ELSE "off".product_check_device_condition END)::BIGINT = woslsisd.id
    LEFT JOIN {{ ref('wkfs_offer_stock_list_stock_in_fourteen_days') }} AS woslsifd
        ON ("off".category_id || "off".manufactor_id || "off".device_id || CASE WHEN "off".product_check_device_condition IS NULL THEN 5 ELSE "off".product_check_device_condition END)::BIGINT = woslsifd.id
    LEFT JOIN {{ ref('wkfs_offer_stock_list_stock_time') }} AS wostlst
        ON ("off".category_id || "off".manufactor_id || "off".device_id || CASE WHEN "off".product_check_device_condition IS NULL THEN 5 ELSE "off".product_check_device_condition END)::BIGINT = wostlst.id