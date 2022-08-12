select
	convert(numeric, case id when '' then null else id end) as id,
	case
		calculation_code when '' then null
		else calculation_code
	end as calculation_code,
	case
		question when '' then null
		else question
	end as question,
	case
		description when '' then null
		else description
	end as description,
	case
		deprication when '' then null
		else deprication
	end as deprication,
	convert(numeric, case deprication_answer when '' then null else deprication_answer end) as deprication_answer,
	convert(numeric, case accessory when '' then null else accessory end) as accessory,
	case
		accessory_text when '' then null
		else accessory_text
	end as accessory_text,
	convert(numeric, case device_question when '' then null else device_question end) as device_question,
	convert(numeric, case visible_in_frontend when '' then null else visible_in_frontend end) as visible_in_frontend,
	convert(numeric, case "type" when '' then null else "type" end) as "type",
	convert(numeric, case disable_new_condition when '' then null else disable_new_condition end) as disable_new_condition
from
	raw_wkfs.question