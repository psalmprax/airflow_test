SELECT
  usr.id
  , usr.firstname
  , usr.lastname
  , usr.street || COALESCE(' ' || usr.number, '') AS street
  , usr.zip
--  , LEFT(usr.zip, 2) || '-' || RIGHT(usr.zip, LENGTH(usr.zip) - 2) AS plzip
  , usr.city
  , usr.country
  , usr.email
  , usr.phone
  , usr.birthdate
  , usr.optin = 1 AS is_optin
  , CASE
      WHEN usr.gender = 0 THEN 'Herr'
      WHEN usr.gender = 1 then 'Frau'
      ELSE '-'
    END AS salutation
  , usr.email = 'debug@wirkaufens.de' AS is_debug_user
  , usr.firstname || ' ' || usr.lastname AS full_name
  , prov.name AS province
FROM {{ ref('obj_wkfs.user_cleaned') }} AS usr
  LEFT JOIN {{ ref('obj_wkfs.province_cleaned') }} AS prov
    ON prov.id = usr.province_id