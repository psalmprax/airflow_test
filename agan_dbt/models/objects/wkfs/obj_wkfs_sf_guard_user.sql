SELECT
  sgu.id
  , sgu.username
  , sgu.created_at
  , sgu.last_login AS last_login_at
  , sgu.is_active = 1 AS is_active
  , sgu.is_super_admin = 1 AS is_super_admin
  , sgu.application_id
  , sgu_p.firstname
  , sgu_p.lastname
  , COALESCE(sgu_p.email, 'no-mail') AS email
  , TRIM(sgu_p.street || COALESCE(sgu_p.number, '')) AS street
  , sgu_p.zip
  , sgu_p.city
  , sgu_p.country
  , sgu_p.phone
  , sgu_p.company
  , sgu_p.sapid
  , COALESCE(sgu_eno.contact_number, 'User is not in ENO Group')
    AS eno_contact_number
  , sgu_ep.sapid as e_plus_sapid
  , sgu_ep.kostenstelle
  , sgu_ep.user_region
FROM {{ ref('obj_wkfs.sf_guard_user_cleaned') }} AS sgu
  LEFT JOIN {{ ref('obj_wkfs.sf_guard_user_profile_cleaned') }} AS sgu_p
    ON sgu_p.user_id = sgu.id
  LEFT JOIN {{ ref('obj_wkfs.sf_guard_user_profile_eno_cleaned') }} AS sgu_eno
    ON sgu_eno.user_id = sgu.id
  LEFT JOIN {{ ref('obj_wkfs.sf_guard_user_profile_e_plus_cleaned') }} AS sgu_ep
    ON sgu_ep.user_id = sgu.id