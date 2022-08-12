select
	convert(numeric, case user_id when '' then null else user_id end) as user_id,
	case
		sapid when '' then null
		else sapid
	end as sapid,
	case
		kostenstelle when '' then null
		else kostenstelle
	end as kostenstelle,
	convert(numeric, case user_region when '' then null else user_region end) as user_region
from
	raw_wkfs.sf_guard_user_profile_e_plus