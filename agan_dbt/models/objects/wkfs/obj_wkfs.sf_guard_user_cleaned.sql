select
	convert(numeric, case id when '' then null else id end) as id,
	case
		username when '' then null
		else username
	end as username,
	case
		algorithm when '' then null
		else algorithm
	end as algorithm,
	case
		salt when '' then null
		else salt
	end as salt,
	case
		"password" when '' then null
		else "password"
	end as "password",
	convert(timestamp, case created_at when '' then null else created_at end) as created_at,
	case
		last_login when '' then null
		else last_login
	end as last_login,
	convert(numeric, case is_active when '' then null else is_active end) as is_active,
	convert(numeric, case is_super_admin when '' then null else is_super_admin end) as is_super_admin,
	convert(numeric, case application_id when '' then null else application_id end) as application_id
from
	raw_wkfs.sf_guard_user