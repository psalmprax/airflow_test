WITH blacklist as (
    select
    	row_number() over(order by channels) as id,
        *
    FROM {{ ref("blacklist") }}
--    (Values
--            ('back2customer')
--            ,('back2seller')
--            ,('Ersatzteile intern')
--            ,('interne Verwendung MA')
--            ,('interne Verwendung Platz')
--            ,('Inventur Bereinigung (nur für GF)')
--            ,('oldRE')
--            ,('other')
--            ,('RE_Kanal')
--            ,('replacement')
--            ,('')
--         ) as t (channels)
)

select * from blacklist