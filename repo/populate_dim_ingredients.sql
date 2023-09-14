truncate table core.dim_ingredients restart identity cascade;

insert into core.dim_ingredients (
	ingredient_type,
	ingredient_name
	)
select 'yeast' as ingredient_type
	,b.ingredients::json->>'yeast' as ingredient_name
from raw_data.beers b
where b.ingredients::json->>'yeast' is not null
group by 1,2
union
select 'hops' as ingredient_type
	,hops::json->>'name' as ingredient_name
from raw_data.beers b
	,json_array_elements(b.ingredients::json->'hops') as hops
where b.ingredients::json->'hops' is not null
group by 1,2
union
select 'malt' as ingredient_type
	,malt::json->>'name' as ingredient_name
from raw_data.beers b
	,json_array_elements(b.ingredients::json->'malt') as malt
where b.ingredients::json->'malt' is not null
group by 1,2
order by 1,2