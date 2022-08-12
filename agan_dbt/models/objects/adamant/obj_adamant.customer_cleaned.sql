select
	convert(numeric,case id when '' then null else id end) as id,
	case firstname when '' then null else firstname end as firstname,
	case lastname when '' then null else lastname end as lastname,
	case email when '' then null else email end as email,
	case phone when '' then null else phone end as phone,
	convert(numeric,case orders_count when '' then null else orders_count end) as orders_count,
	convert(numeric,case address_id when '' then null else address_id end) as address_id
from 
	raw_adamant.customer
