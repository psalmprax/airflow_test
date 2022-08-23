with results as (
select
	convert(numeric, case id when '' then null else id end) as id,
	case
		article_number when '' then null
		else article_number
	end as article_number,
	convert(timestamp, case created_at when '' then null else created_at end) as created_at,
	convert(timestamp, case updated_at when '' then null else updated_at end) as updated_at,
	convert(numeric, case sf_guard_user_id when '' then null else sf_guard_user_id end) as sf_guard_user_id,
	convert(numeric, case device_id when '' then null else device_id end) as device_id,
	convert(numeric, case trade_id when '' then null else trade_id end) as trade_id,
	convert(numeric, case "condition" when '' then null else "condition" end) as "condition",
	convert(decimal(15,2), case amount when '' then null else amount end) as amount,
	convert(decimal(15,2), case accepted_amount when '' then null else accepted_amount end) as accepted_amount,
	convert(decimal(15,2), case original_amount when '' then null else original_amount end) as original_amount,
	case
		state when '' then null
		else state
	end as state,
	convert(numeric, case deleted when '' then null else deleted end) as deleted,
	convert(numeric, case force_listing when '' then null else force_listing end) as force_listing,
	case
		title when '' then null
		else title
	end as title,
	convert(numeric, case latest_stock_id when '' then null else latest_stock_id end) as latest_stock_id,
	convert(timestamp, case date_paid when '' then null else date_paid end) as date_paid,
	convert(timestamp, case date_paid_mail_sent when '' then null else date_paid_mail_sent end) as date_paid_mail_sent,
	convert(timestamp, case calculated_payment_date when '' then null else calculated_payment_date end) as calculated_payment_date,
	convert(numeric, case product_check_device_condition when '' then null else product_check_device_condition end) as product_check_device_condition,
	case
		"comment" when '' then null
		else "comment"
	end as "comment",
	case
		serial when '' then null
		else serial
	end as serial,
	case
		additional_accessories when '' then null
		else additional_accessories
	end as additional_accessories,
	convert(numeric, case device_requests_id when '' then null else device_requests_id end) as device_requests_id,
	convert(numeric, case product_check_device_id when '' then null else product_check_device_id end) as product_check_device_id,
	convert(decimal(15,2), case converted_amount when '' then null else converted_amount end) as converted_amount,
	convert(decimal(15,2), case commission_amount when '' then null else commission_amount end) as commission_amount,
	convert(timestamp, case deleted_at when '' then null else deleted_at end) as deleted_at,
	case
		actindo_data_hash when '' then null
		else actindo_data_hash
	end as actindo_data_hash,
	convert(decimal(15,2), case recommended_sales_price when '' then null else recommended_sales_price end) as recommended_sales_price,
	case
		imei when '' then null
		else imei
	end as imei
from
	{{ source("wkfs","offer") }}
)

select * from results