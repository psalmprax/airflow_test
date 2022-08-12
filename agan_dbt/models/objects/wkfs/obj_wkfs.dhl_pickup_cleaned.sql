select
	convert(numeric, case id when '' then null else id end) as id,
	convert(numeric, case user_id when '' then null else user_id end) as user_id,
	convert(timestamp, case pickup_date when '' then null else pickup_date end) as pickup_date,
	convert(time, substring(case ready_by_time when '' then null else ready_by_time end,
		position('s' in  case ready_by_time when '' then null else ready_by_time end)+1,
		position('s' in  case ready_by_time when '' then null else ready_by_time end)+9)) as ready_by_time,

	convert(time, substring(case closing_time when '' then null else closing_time end,
		position('s' in  case closing_time when '' then null else closing_time end)+1,
		position('s' in  case closing_time when '' then null else closing_time end)+9)) as closing_time,
	convert(numeric, case status when '' then null else status end) as status,
	case
		status_message when '' then null
		else status_message
	end as status_message,
	case
		confirmation_number when '' then null
		else confirmation_number
	end as confirmation_number
from
	raw_wkfs.dhl_pickup