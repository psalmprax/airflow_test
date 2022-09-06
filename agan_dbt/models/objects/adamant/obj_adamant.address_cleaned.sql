-- raw_adamant.address definition

select
	convert(numeric,case id when '' then null else id end) as id,
	case street when '' then null else street end as street,
	case additional when '' then null else additional end as additional,
	case postal_code when '' then null else postal_code end as postal_code,
	case city when '' then null else city end as city,
	case country_code when '' then null else country_code end as country_code,
	case firstname when '' then null else firstname end as firstname,
	case lastname when '' then null else lastname end as lastname
from
    {{ source("adamant","address")}}
--	raw_adamant.address