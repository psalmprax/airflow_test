select
	distinct brand,
	case
		when brand in 
			('Apple',
			'Canon',
			'Google',
			'Huawei',
			'Microsoft',
			'Nikon',
			'OnePlus',
			'Samsung',
			'Sony',
			'Xiaomi')
			then brand
		else 'other'
	end as target_brand_2022
from
    {{ ref("dim_brand") }} as db
--	analytics_reporting.dim_brand db
