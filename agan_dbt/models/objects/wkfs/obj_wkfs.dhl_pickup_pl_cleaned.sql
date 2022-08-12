with results as (
select
	convert(numeric, case id when '' then null else id end) as id,
	convert(numeric, case trade_id when '' then null else trade_id end) as trade_id,
	convert(timestamp, case shipment_date when '' then null else shipment_date end) as shipment_date,
	convert(time, substring(case shipment_start_hour when '' then null else shipment_start_hour end,
		position('s' in  case shipment_start_hour when '' then null else shipment_start_hour end)+1,
		position('s' in  case shipment_start_hour when '' then null else shipment_start_hour end)+9)) as shipment_start_hour,
	convert(time, substring(case shipment_end_hour when '' then null else shipment_end_hour end,
		position('s' in  case shipment_end_hour when '' then null else shipment_end_hour end)+1,
		position('s' in  case shipment_end_hour when '' then null else shipment_end_hour end)+9)) as shipment_end_hour,
	case
		"name" when '' then null
		else "name"
	end as "name",
	case
		postal_code when '' then null
		else postal_code
	end as postal_code,
	case
		city when '' then null
		else city
	end as city,
	case
		street when '' then null
		else street
	end as street,
	case
		house_number when '' then null
		else house_number
	end as house_number,
	case
		apartment_number when '' then null
		else apartment_number
	end as apartment_number,
	case
		email when '' then null
		else email
	end as email,
	case
		phone when '' then null
		else phone
	end as phone,
	convert(numeric, case package_count when '' then null else package_count end) as package_count,
	case
		shipment_notification_number when '' then null
		else shipment_notification_number
	end as shipment_notification_number,
	case
		dispatch_notification_number when '' then null
		else dispatch_notification_number
	end as dispatch_notification_number,
	convert(timestamp, case created_at when '' then null else created_at end) as created_at,
	case
		label_type when '' then null
		else label_type
	end as label_type,
	case
		label_format when '' then null
		else label_format
	end as label_format
from
	{{ source("wkfs","dhl_pickup_pl") }}
)

select * from results