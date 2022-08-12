select
	convert(numeric,case id when '' then null else id end) as id,
	convert(timestamp, case started_at when '' then null else started_at end) as started_at, 
	convert(timestamp, case finished_at when '' then null else finished_at end) as finished_at, 
	case duration when '' then null else duration end as duration,
	convert(timestamp, case created_at when '' then null else created_at end) as created_at, 
	convert(timestamp, case updated_at when '' then null else updated_at end) as updated_at, 	
	convert(numeric,case hide_notification_email when '' then null else hide_notification_email end) as hide_notification_email
from
	raw_adamant.warranty