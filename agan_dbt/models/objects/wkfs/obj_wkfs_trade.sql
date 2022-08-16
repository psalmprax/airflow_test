WITH ubm_partners AS (
    SELECT
      aufv.transaction_id
--      , STRING_AGG(convert(varchar(max),au.name) || ':' || convert(varchar(max),aufv.value), '\n') AS ubm_partner
      , LISTAGG(convert(varchar(max),au.name) || ':' || convert(varchar(max),aufv.value), '\n') AS ubm_partner

    FROM {{ ref('obj_wkfs.add_userfield_value_cleaned') }} AS aufv
      LEFT JOIN {{ ref('obj_wkfs.add_userfield_cleaned') }} AS au
        ON au.id = aufv.add_userfield_id
    GROUP BY 1
)
, adduserfields AS (
  SELECT
    auv.transaction_id
--    , STRING_AGG(
--      COALESCE(convert(varchar(max),au.name) || ': ' || COALESCE(convert(varchar(max),auv.value), ''), 'n.a.')
--      , E'\n'
--    ) AS userfields
    , LISTAGG(
      COALESCE(convert(varchar(max),au.name) || ': ' || COALESCE(convert(varchar(max),auv.value), ''), 'n.a.')
      , '\n'
    ) AS userfields
  FROM {{ ref('obj_wkfs.add_userfield_value_cleaned') }} AS auv
    LEFT JOIN {{ ref('obj_wkfs.add_userfield_cleaned') }} AS au
      ON au.id = auv.add_userfield_id
  GROUP BY 1
)
, company_name AS (
  SELECT
    auv.transaction_id
    , auv.value AS company_name
  FROM {{ ref('obj_wkfs.add_userfield_cleaned') }} AS au
    LEFT JOIN {{ ref('obj_wkfs.add_userfield_value_cleaned') }} AS auv
      ON auv.add_userfield_id = au.id
  WHERE au.usage = 'firmname'
)

SELECT
  {{ select_all_except(
    'wkfs',
    'trade',
    [
      'fastlane'
      ,"dag"
      ,"data_interval_start"
      ,"data_interval_end"
      ,"execution_date"
      ,"next_execution_date"
      ,"prev_data_interval_start_success"
      ,"prev_data_interval_end_success"
      ,"prev_execution_date"
      ,"prev_start_date_success"
      ,"prev_execution_date_success"

    ],
    True,
    'tr')
  }}
  , tr.transaction_id AS id
  , up.ubm_partner
  , COALESCE(tr.fastlane = 1, FALSE) AS is_fastlane
  , auf.userfields
  , cn.company_name
  , tse.email AS huawei_store_email
  , lp.id AS landingpage_id
  , lp.name as landingpage -- 62
  , COALESCE(lp.tax / 100, 0) AS landingpage_tax -- 71
  , CASE
      WHEN lp.overwrite_application_payment = 1
        THEN lppt.provision / 100
      ELSE apt.provision / 100
    END AS commission_value
  , app.currency_id AS app_currency_id
  , app.name AS app_name
  , (app.name = 'b2b') AND (b2btd.with_taxes = 1) AS is_b2b_tax
  , usr.lastname AS user_lastname

  -- currency
  , cc.name as original_currency
  , cc.abbreviation AS original_currency_unit
  , cr.value AS currency_exchange_rate

--  /* 1-n relationship!
  -- dhl related
  , dhl_pl.created_at AS dhl_pl_created_at
  , dhl_pl.shipment_date AS dhl_pl_shipment_date
  , dhl_pl.shipment_notification_number AS dhl_pl_shipment_notification_number
  , dhl_pl.package_count AS dhl_pl_package_count
--  , CONCAT_WS(
--      '-'
--      , dhl_pl.shipment_start_hour
--      , dhl_pl.shipment_end_hour
--    ) AS dhl_pl_pickup_time
--  , dhl_pl.shipment_start_hour::INTERVAL
--  , dhl_pl.shipment_end_hour::INTERVAL
  , ('-'||dhl_pl.shipment_start_hour||dhl_pl.shipment_end_hour) AS dhl_pl_pickup_time
  , dhl_pl.shipment_start_hour::TIME
  , dhl_pl.shipment_end_hour::TIME
  , dhl_de.shipment_confirmation_number AS dhl_de_shipment_confirmation_number
--  , CONCAT_WS(' ', dhl_de.first_name, dhl_de.name) AS dhl_de_full_name
  , (' '||dhl_de.first_name|| dhl_de.name) AS dhl_de_full_name

  , dhl_de.street AS dhl_de_street
  , dhl_de.house_number AS dhl_de_housenumber
  , dhl_de.postal_code AS dhl_de_postal_code
  , dhl_de.city AS dhl_de_city
  , dhl_de.note AS dhl_de_note
  , dhl_de.shipment_date AS dhl_de_shipment_date
  , dhl_de.package_count AS dhl_de_package_count
--  */

  -- sf guard user information
  , sgu.username AS trade_sf_guard_user
  , COALESCE(sgu.email, 'Unknown User') AS login_email
  , COALESCE(sgu.username, 'Unknown User') AS login_name
  , COALESCE(sgu.eno_contact_number, 'Unknown User') AS eno_contact_number
  , sgu.last_login_at
  , sgu.phone
  , sgu_c.e_plus_sapid AS creator_e_plus_sapid
  , sgu_c.kostenstelle AS creator_kostenstelle

  -- other information
  , lo.name AS location
  , b2btd.order_number AS b2b_order_id
FROM {{ ref('obj_wkfs.trade_cleaned') }} AS tr

  -- CTEs
  LEFT JOIN ubm_partners AS up
    ON up.transaction_id = tr.transaction_id
  LEFT JOIN adduserfields AS auf
    ON auf.transaction_id = tr.transaction_id
  LEFT JOIN company_name AS cn
    ON cn.transaction_id = tr.transaction_id

  -- payment related information -- only for payment_type_id below
  LEFT JOIN {{ ref('obj_wkfs_payment') }} AS pay
    ON pay.id = tr.payment_id

  -- sf guard user information
  LEFT JOIN {{ ref('obj_wkfs_sf_guard_user') }} AS sgu
    ON sgu.id = tr.sf_guard_user_id

  -- sf guard user information - trade creator
  LEFT JOIN {{ ref('obj_wkfs_sf_guard_user') }} AS sgu_c
    ON sgu_c.id = tr.created_by_id

  -- user related informations
  LEFT JOIN {{ ref('obj_wkfs.user_cleaned') }} AS usr
    ON usr.id = tr.user_id
    LEFT JOIN {{ ref('obj_wkfs.trade_store_email_cleaned') }} AS tse
      ON tse.email = usr.email

  -- landingpage related information
  LEFT JOIN {{ ref('obj_wkfs.landingpage_trade_cleaned') }} AS lp_t
    ON lp_t.trade_id = tr.transaction_id
    LEFT JOIN {{ ref('obj_wkfs.landingpage_cleaned') }} AS lp
      ON lp.id = lp_t.landingpage_id
    LEFT JOIN {{ ref('obj_wkfs.landingpage_payment_type_cleaned') }} AS lppt
      ON lppt.landingpage_id = lp_t.landingpage_id
      AND lppt.payment_type_id = pay.payment_type_id

  LEFT JOIN {{ ref('obj_wkfs.application_payment_type_cleaned') }} AS apt
    ON apt.application_id = tr.application_id
    AND apt.payment_type_id = pay.payment_type_id

  -- currency related information
  LEFT JOIN {{ ref('obj_wkfs.currency_rate_cleaned') }} AS cr
    ON cr.id = tr.currency_rate_id
    LEFT JOIN {{ ref('obj_wkfs.currency_cleaned') }} AS cc
      ON cc.id = cr.currency_id

--  /* dhl_pl and dhl_de are a 1-n relationship with trade!
  -- dhl related information
  LEFT JOIN {{ ref('obj_wkfs.dhl_pickup_pl_cleaned') }} dhl_pl
    ON dhl_pl.trade_id = tr.transaction_id
  LEFT JOIN {{ ref('obj_wkfs.dhl_pickup_de_cleaned') }} dhl_de
    ON dhl_de.trade_id = tr.transaction_id
--  */
  
  -- various other joins
  LEFT JOIN {{ ref('obj_wkfs.application_cleaned') }} AS app
    ON app.id = tr.application_id
  LEFT JOIN {{ ref('obj_wkfs.b2b_trade_detail_cleaned') }} AS b2btd
    ON b2btd.trade_id = tr.transaction_id
  LEFT JOIN {{ ref('obj_wkfs.location_cleaned') }} AS lo
    ON lo.id = tr.location_id