create table core.link_method_beer(
	id serial primary key,
	dim_beer_id int,
	stage varchar,
	temp decimal,
	temp_unit varchar,
	duration decimal,
	foreign key (dim_beer_id) references core.dim_beer (id)
)
;

insert into core.link_method_beer(
	dim_beer_id,
	stage,
	temp,
	temp_unit,
	duration
)
select 
	db.id as dim_beer_id
	,'mashing' as stage
	,cast((mash_temp::json->>'temp')::json->>'value' as decimal) as temp
	,(mash_temp::json->>'temp')::json->>'unit' as temp_unit
	,cast(mash_temp::json->>'duration' as decimal) as duration
from raw_data.beers b
join json_array_elements(b.method::json->'mash_temp') as mash_temp on true
join core.dim_beer db on db.beer_id = b.id 
union
select 
	db.id as dim_beer_id
	,'fermentation' as stage
	,cast(((b.method::json->>'fermentation')::json->>'temp')::json->>'value' as decimal) as temp
	,((b.method::json->>'fermentation')::json->>'temp')::json->>'unit' as temp_unit
	,null as duration
from raw_data.beers b
join core.dim_beer db on db.beer_id = b.id
order by 1,2 desc