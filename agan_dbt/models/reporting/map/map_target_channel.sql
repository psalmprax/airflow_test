select
	outlet_name as channel,
	case
		when outlet_name like '%AGAN_RENT%' then outlet_name
		when outlet_name in 
			('Amazon',
			'B2B Verkauf',
			'Back Market',
			'Cdiscount',
			'Eprice (IT)',
			'Fnac',
			'Handydortmund',
			'Idealo DE',
			'Mitarbeiter_Verkauf',
			'Notebook12',
			'RDC (FR)',
			'Rakuten FR',
			'Rakuten GmbH',
			'Webshop',
			'Webshop_ES',
			'Webshop_FR',
			'Webshop_IT',
			'Wish',
			'eBay',
			'eBay_ES',
			'eBay_FR',
			'eBay_IT',
			'eBay_Teildefekte',
			'refurbed')
			then outlet_name
		when outlet_name in ('ePrice', 'eprice') then 'Eprice (IT)'
		when outlet_name = 'Rueducommerce' then 'RDC (FR)'
		when outlet_name = 'ebay_FR' then 'eBay_FR'
		else 'Other Marketplaces: Amazon IT, Amazon FR, Amazon ES, LDLC, Lengow 1, Lengow 2, Lengow 3, Lengow 4'
	end as target_channel_2022
from
    {{ ref("obj_wkfs.business_channel_cleaned") }} owbcc
--	analytics_objects."obj_wkfs.business_channel_cleaned"
