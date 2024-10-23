
CREATE OR REPLACE TABLE `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string`
    OPTIONS (
        expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 365 DAY))
    AS

-- do country first, then add in any country from continent that isn't already there, then add default country

select date, 'default' geo_continent, 'default' country_code, 'default' domain, 'default' device_category, 'default' rtt_category, config_level, bidders
from `{project_id}.DAS_increment.DAS_config_{tablename_ext}_string`;


insert into `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string`
with t1 as (
  select c.*, l0.bidders as bidders_existing
  from `{project_id}.DAS_increment.DAS_config_geo_{tablename_ext}_string` c
  left join `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string` l0
  using (date)
)
select date, geo_continent, 'default', 'default', 'default', 'default', config_level, bidders
from t1
where bidders != bidders_existing;


insert into `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string`
with t1 as (
  select c.*, coalesce(l1.bidders, l0.bidders) as bidders_existing
  from `{project_id}.DAS_increment.DAS_config_geo_cou_{tablename_ext}_string` c
  left join (select * from `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string` where config_level=1) l1
  using (date, geo_continent)
  left join (select * from `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string` where config_level=0) l0
  using (date)
)
select date, geo_continent, country_code, 'default', 'default', 'default', config_level, bidders
from t1
where bidders != bidders_existing;


insert into `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string`
with t1 as (
  select c.*, coalesce(l2.bidders, l1.bidders, l0.bidders) as bidders_existing
  from `{project_id}.DAS_increment.DAS_config_geo_cou_dev_{tablename_ext}_string` c
  left join (select * from `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string` where config_level=2) l2
  using (date, geo_continent, country_code)
  left join (select * from `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string` where config_level=1) l1
  using (date, geo_continent)
  left join (select * from `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string` where config_level=0) l0
  using (date)
)
select date, geo_continent, country_code, 'default', device_category, 'default', config_level, bidders
from t1
where bidders != bidders_existing;


insert into `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string`
with t1 as (
  select c.*, coalesce(l3.bidders, l2.bidders, l1.bidders, l0.bidders) as bidders_existing
  from `{project_id}.DAS_increment.DAS_config_geo_cou_dev_rtt_{tablename_ext}_string` c
  left join (select * from `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string` where config_level=3) l3
  using (date, geo_continent, country_code, device_category)
  left join (select * from `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string` where config_level=2) l2
  using (date, geo_continent, country_code)
  left join (select * from `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string` where config_level=1) l1
  using (date, geo_continent)
  left join (select * from `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string` where config_level=0) l0
  using (date)
)
select date, geo_continent, country_code, 'default', device_category, rtt_category, config_level, bidders
from t1
where bidders != bidders_existing;


insert into `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string`
select date, geo_continent, country_code, domain, 'default', 'default', config_level, bidders
from `{project_id}.DAS_increment.DAS_config_geo_cou_dom_{tablename_ext}_string`;


insert into `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string`
with t1 as (
  select c.*, l5.bidders as bidders_existing
  from `{project_id}.DAS_increment.DAS_config_geo_cou_dom_dev_{tablename_ext}_string` c
  left join (select * from `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string` where config_level=5) l5
  using (date, geo_continent, country_code, domain)
)
select date, geo_continent, country_code, domain, device_category, 'default', config_level, bidders
from t1
where bidders != bidders_existing;


insert into `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string`
with t1 as (
  select c.*, coalesce(l6.bidders, l5.bidders) as bidders_existing
  from `{project_id}.DAS_increment.DAS_config_geo_cou_dom_dev_rtt_{tablename_ext}_string` c
  left join (select * from `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string` where config_level=6) l6
  using (date, geo_continent, country_code, domain, device_category)
  left join (select * from `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string` where config_level=5) l5
  using (date, geo_continent, country_code, domain)
  )
select date, geo_continent, country_code, domain, device_category, rtt_category, config_level, bidders
from t1
where bidders != bidders_existing;


CREATE OR REPLACE TABLE `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}`
    OPTIONS (
        expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 365 DAY))
    AS

select * except (bidders), split(bidders, ',') bidders
from `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext}_string`


