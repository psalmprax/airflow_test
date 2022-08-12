select
	convert(numeric,case id when '' then null else id end) as id,
	convert(numeric,case business_channel_id when '' then null else business_channel_id end) as business_channel_id,
	convert(numeric,case reference when '' then null else reference end) as reference,
	case article_number when '' then null else article_number end as article_number,
	case order_number when '' then null else order_number end as order_number,
	convert(numeric,case tax when '' then null else tax end) as tax,
	case "name" when '' then null else "name" end as name,
	case status when '' then null else status end as status,
	convert(timestamp, case status_updated_at when '' then null else status_updated_at end) as status_updated_at,
	case imei when '' then null else imei end as imei, 
	case serial_number when '' then null else serial_number end as serial_number,
	convert(numeric,case warranty_permitted when '' then null else warranty_permitted end) as warranty_permitted,
	convert(timestamp, case created_at when '' then null else created_at end) as created_at, 
	convert(timestamp, case updated_at when '' then null else updated_at end) as updated_at, 
	convert(numeric,case stock_id when '' then null else stock_id end) as stock_id
from 
	raw_adamant.device
