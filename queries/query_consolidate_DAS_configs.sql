CREATE OR REPLACE TABLE `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string`
    OPTIONS (
        expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 365 DAY))
    AS

select date, 'default' country_code, 'default' domain, 'default' device_category, 'default' rtt_category, config_level, bidders
from `{project_id}.DAS_increment.DAS_config_{tablename_ext}_string`

union all

select date, country_code, 'default', 'default', 'default', config_level, bidders
from `{project_id}.DAS_increment.DAS_config_geo_cou_{tablename_ext}_string`;


insert into `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string`
with geo as (
select t1.*, geo_country country_code 
from `{project_id}.DAS_increment.DAS_config_geo_{tablename_ext}_string` t1
join `sublime-elixir-273810.ideal_ad_stack.continent_country_mapping` t2
using (geo_continent)
)
select date, country_code, 'default', 'default', 'default', t1.config_level, t1.bidders
from geo t1
left join `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string` t2
using (date, country_code)
where t2.config_level is null;
  

insert into `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string`
with t1 as (
  select c.*, coalesce(l2.bidders, l0.bidders) as bidders_existing
  from `{project_id}.DAS_increment.DAS_config_geo_cou_dev_{tablename_ext}_string` c
  left join (select * from `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string` where config_level in (1, 2)) l2
  using (date, country_code)
  left join (select * from `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string` where config_level=0) l0
  using (date)
)
select date, country_code, 'default', device_category, 'default', config_level, bidders
from t1
where bidders != bidders_existing;


insert into `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string`
with t1 as (
  select c.*, coalesce(l3.bidders, l2.bidders, l0.bidders) as bidders_existing
  from `{project_id}.DAS_increment.DAS_config_geo_cou_dev_rtt_{tablename_ext}_string` c
  left join (select * from `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string` where config_level=3) l3
  using (date, country_code, device_category)
  left join (select * from `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string` where config_level in (1, 2)) l2
  using (date, country_code)
  left join (select * from `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string` where config_level=0) l0
  using (date)
)
select date, country_code, 'default', device_category, rtt_category, config_level, bidders
from t1
where bidders != bidders_existing;


insert into `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string`
select date, country_code, domain, 'default', 'default', config_level, bidders
from `{project_id}.DAS_increment.DAS_config_geo_cou_dom_{tablename_ext}_string`;


insert into `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string`
with t1 as (
  select c.*, l5.bidders as bidders_existing
  from `{project_id}.DAS_increment.DAS_config_geo_cou_dom_dev_{tablename_ext}_string` c
  left join (select * from `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string` where config_level=5) l5
  using (date, country_code, domain)
)
select date, country_code, domain, device_category, 'default', config_level, bidders
from t1
where bidders != bidders_existing;


insert into `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string`
with t1 as (
  select c.*, coalesce(l6.bidders, l5.bidders) as bidders_existing
  from `{project_id}.DAS_increment.DAS_config_geo_cou_dom_dev_rtt_{tablename_ext}_string` c
  left join (select * from `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string` where config_level=6) l6
  using (date, country_code, domain, device_category)
  left join (select * from `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string` where config_level=5) l5
  using (date, country_code, domain)
  )
select date, country_code, domain, device_category, rtt_category, config_level, bidders
from t1
where bidders != bidders_existing;


CREATE OR REPLACE TABLE `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}`
    OPTIONS (
        expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 365 DAY))
    AS

select * except (bidders), split(bidders, ',') bidders
from `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string`


