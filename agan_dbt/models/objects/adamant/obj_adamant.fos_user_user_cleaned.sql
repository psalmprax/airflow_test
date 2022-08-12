select
	convert(numeric,case id when '' then null else id end) as id,
	case username when '' then null else username end as username,
	case username_canonical when '' then null else username_canonical end as username_canonical,
	case email when '' then null else email end as email,
	case email_canonical when '' then null else email_canonical end as email_canonical,
	convert(numeric,case enabled when '' then null else enabled end) as enabled,
	case "password" when '' then null else "password" end as password,
	last_login, 
	case roles when '' then null else roles end as roles,
	convert(timestamp, case created_at when '' then null else created_at end) as created_at, 
	convert(timestamp, case updated_at when '' then null else updated_at end) as updated_at, 
	case gender when '' then null else gender end as gender,
	case facebook_data when '' then null else facebook_data end as facebook_data,
	case twitter_data when '' then null else twitter_data end as twitter_data,
	case gplus_data when '' then null else gplus_data end as gplus_data,
	case column_settings when '' then null else column_settings end as column_settings,
	case firstname when '' then null else firstname end as firstname,
	case lastname when '' then null else lastname end as lastname,
	case salt when '' then null else salt end as salt
from 
	raw_adamant.fos_user_user