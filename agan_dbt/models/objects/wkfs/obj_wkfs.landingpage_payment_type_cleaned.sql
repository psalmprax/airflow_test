select
	convert(numeric, case landingpage_id when '' then null else landingpage_id end) as landingpage_id,
	convert(numeric, case payment_type_id when '' then null else payment_type_id end) as payment_type_id,
	case
		calc_button_title when '' then null
		else calc_button_title
	end as calc_button_title,
	convert(numeric, case showcalc when '' then null else showcalc end) as showcalc,
	convert(numeric, case showcart when '' then null else showcart end) as showcart,
	convert(numeric, case preselect when '' then null else preselect end) as preselect,
	convert(numeric, case default_payment_type when '' then null else default_payment_type end) as default_payment_type,
	convert(numeric, case "order" when '' then null else "order" end) as "order",
	convert(numeric, case bonus when '' then null else bonus end) as bonus,
	convert(numeric, case provision when '' then null else provision end) as provision
from
	raw_wkfs.landingpage_payment_type