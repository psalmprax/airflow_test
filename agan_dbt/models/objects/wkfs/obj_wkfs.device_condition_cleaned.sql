select
	convert(numeric, case id when '' then null else id end) as id,
	case
		code when '' then null
		else code
	end as code,
	case
		title when '' then null
		else title
	end as title,
	case
		"text" when '' then null
		else "text"
	end as "text",
	case
		video_url when '' then null
		else video_url
	end as video_url,
	convert(numeric, case conditionprice when '' then null else conditionprice end) as conditionprice,
	convert(numeric, case conditionsalesprice when '' then null else conditionsalesprice end) as conditionsalesprice,
	convert(numeric, case hide_frontend when '' then null else hide_frontend end) as hide_frontend,
	case
		ebay_template_title when '' then null
		else ebay_template_title
	end as ebay_template_title,
	case
		ebay_template_condition_text when '' then null
		else ebay_template_condition_text
	end as ebay_template_condition_text
from
	raw_wkfs.device_condition