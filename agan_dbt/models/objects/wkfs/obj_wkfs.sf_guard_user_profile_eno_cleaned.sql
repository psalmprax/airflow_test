select
	convert(numeric, case user_id when '' then null else user_id end) as user_id,
	case
		contact_number when '' then null
		else contact_number
	end as contact_number
from
	raw_wkfs.sf_guard_user_profile_eno