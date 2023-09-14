--drop table core.dim_beer;

create table if not exists core.dim_beer (
id serial primary key,
beer_id int,
beer_name varchar,
beer_tagline varchar,
first_brewed_year int,
description varchar,
image_url varchar,
abv decimal,
ibu decimal,
target_fg decimal,
ebc decimal,
srm decimal,
attenuation_level decimal,
volume_litres decimal,
boil_volume_litres decimal,
brewers_tips varchar
);


insert into core.dim_beer (
		beer_id,
		beer_name,
		beer_tagline,
		first_brewed_year,
		description,
		image_url,
		abv,
		ibu,
		target_fg,
		ebc,
		srm,
		attenuation_level,
		volume_litres,
		boil_volume_litres,
		brewers_tips 
		)
select
	b.id,
	b.name as beer_name,
	b.tagline as beer_tagline,
	cast(right(b.first_brewed,4)as int) as first_brewed_year,
	b.description,
	b.image_url,
	b.abv,
	b.ibu,
	b.target_fg,
	b.ebc,
	b.srm,
	b.attenuation_level,
	cast(b.volume::json->>'value' as decimal) as volume_litres,
	cast(b.boil_volume::json->>'value' as decimal) as boil_volume_litres,
	b.brewers_tips	
from raw_data.beers b;