
CREATE OR REPLACE TABLE `streamamp-qa-239417.DAS_increment.DAS_config_consolidated_2024-10-10_20_mab100000_mib1000_ds7_bc3_string`
    OPTIONS (
        expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 365 DAY))
    AS

select date, 'default' geo_continent, 'default' country_code, 'default' domain, 'default' device_category, 'default' rtt_category, config_level, bidders
from `streamamp-qa-239417.DAS_increment.DAS_config_2024-10-10_20_mab100000_mib1000_ds7_bc3_string`;


insert into `streamamp-qa-239417.DAS_increment.DAS_config_consolidated_2024-10-10_20_mab100000_mib1000_ds7_bc3_string`
with t1 as (
  select c.*, l0.bidders as bidders_existing
  from `streamamp-qa-239417.DAS_increment.DAS_config_geo_2024-10-10_20_mab100000_mib1000_ds7_bc3_string` c
  left join `streamamp-qa-239417.DAS_increment.DAS_config_consolidated_2024-10-10_20_mab100000_mib1000_ds7_bc3_string` l0
  using (date)
)
select date, geo_continent, 'default', 'default', 'default', 'default', config_level, bidders
from t1
where bidders != bidders_existing;


insert into `streamamp-qa-239417.DAS_increment.DAS_config_consolidated_2024-10-10_20_mab100000_mib1000_ds7_bc3_string`
with t1 as (
  select c.*, coalesce(l1.bidders, l0.bidders) as bidders_existing
  from `streamamp-qa-239417.DAS_increment.DAS_config_geo_cou_2024-10-10_20_mab100000_mib1000_ds7_bc3_string` c
  left join (select * from `streamamp-qa-239417.DAS_increment.DAS_config_consolidated_2024-10-10_20_mab100000_mib1000_ds7_bc3_string` where config_level=1) l1
  using (date, geo_continent)
  left join (select * from `streamamp-qa-239417.DAS_increment.DAS_config_consolidated_2024-10-10_20_mab100000_mib1000_ds7_bc3_string` where config_level=0) l0
  using (date)
)
select date, geo_continent, country_code, 'default', 'default', 'default', config_level, bidders
from t1
where bidders != bidders_existing;


insert into `streamamp-qa-239417.DAS_increment.DAS_config_consolidated_2024-10-10_20_mab100000_mib1000_ds7_bc3_string`
with t1 as (
  select c.*, coalesce(l2.bidders, l1.bidders, l0.bidders) as bidders_existing
  from `streamamp-qa-239417.DAS_increment.DAS_config_geo_cou_dev_2024-10-10_20_mab100000_mib1000_ds7_bc3_string` c
  left join (select * from `streamamp-qa-239417.DAS_increment.DAS_config_consolidated_2024-10-10_20_mab100000_mib1000_ds7_bc3_string` where config_level=2) l2
  using (date, geo_continent, country_code)
  left join (select * from `streamamp-qa-239417.DAS_increment.DAS_config_consolidated_2024-10-10_20_mab100000_mib1000_ds7_bc3_string` where config_level=1) l1
  using (date, geo_continent)
  left join (select * from `streamamp-qa-239417.DAS_increment.DAS_config_consolidated_2024-10-10_20_mab100000_mib1000_ds7_bc3_string` where config_level=0) l0
  using (date)
)
select date, geo_continent, country_code, 'default', device_category, 'default', config_level, bidders
from t1
where bidders != bidders_existing;

insert into `streamamp-qa-239417.DAS_increment.DAS_config_consolidated_2024-10-10_20_mab100000_mib1000_ds7_bc3_string`
with t1 as (
  select c.*, coalesce(l3.bidders, l2.bidders, l1.bidders, l0.bidders) as bidders_existing
  from `streamamp-qa-239417.DAS_increment.DAS_config_geo_cou_dev_rtt_2024-10-10_20_mab100000_mib1000_ds7_bc3_string` c
  left join (select * from `streamamp-qa-239417.DAS_increment.DAS_config_consolidated_2024-10-10_20_mab100000_mib1000_ds7_bc3_string` where config_level=3) l3
  using (date, geo_continent, country_code, device_category)
  left join (select * from `streamamp-qa-239417.DAS_increment.DAS_config_consolidated_2024-10-10_20_mab100000_mib1000_ds7_bc3_string` where config_level=2) l2
  using (date, geo_continent, country_code)
  left join (select * from `streamamp-qa-239417.DAS_increment.DAS_config_consolidated_2024-10-10_20_mab100000_mib1000_ds7_bc3_string` where config_level=1) l1
  using (date, geo_continent)
  left join (select * from `streamamp-qa-239417.DAS_increment.DAS_config_consolidated_2024-10-10_20_mab100000_mib1000_ds7_bc3_string` where config_level=0) l0
  using (date)
)
select date, geo_continent, country_code, 'default', device_category, rtt_category, config_level, bidders
from t1
where bidders != bidders_existing;

insert into `streamamp-qa-239417.DAS_increment.DAS_config_consolidated_2024-10-10_20_mab100000_mib1000_ds7_bc3_string`
with t1 as (
  select c.*, coalesce(l2.bidders, l1.bidders, l0.bidders) as bidders_existing
  from `streamamp-qa-239417.DAS_increment.DAS_config_geo_cou_dom_2024-10-10_20_mab100000_mib1000_ds7_bc3_string` c
  left join (select * from `streamamp-qa-239417.DAS_increment.DAS_config_consolidated_2024-10-10_20_mab100000_mib1000_ds7_bc3_string` where config_level=2) l2
  using (date, geo_continent, country_code)
  left join (select * from `streamamp-qa-239417.DAS_increment.DAS_config_consolidated_2024-10-10_20_mab100000_mib1000_ds7_bc3_string` where config_level=1) l1
  using (date, geo_continent)
  left join (select * from `streamamp-qa-239417.DAS_increment.DAS_config_consolidated_2024-10-10_20_mab100000_mib1000_ds7_bc3_string` where config_level=0) l0
  using (date)
)
select date, geo_continent, country_code, domain, 'default', 'default', config_level, bidders
from t1
where bidders != bidders_existing;

insert into `streamamp-qa-239417.DAS_increment.DAS_config_consolidated_2024-10-10_20_mab100000_mib1000_ds7_bc3_string`
with t1 as (
  select c.*, coalesce(l5.bidders, l2.bidders, l1.bidders, l0.bidders) as bidders_existing
  from `streamamp-qa-239417.DAS_increment.DAS_config_geo_cou_dom_dev_2024-10-10_20_mab100000_mib1000_ds7_bc3_string` c
  left join (select * from `streamamp-qa-239417.DAS_increment.DAS_config_consolidated_2024-10-10_20_mab100000_mib1000_ds7_bc3_string` where config_level=5) l5
  using (date, geo_continent, country_code, domain)
  left join (select * from `streamamp-qa-239417.DAS_increment.DAS_config_consolidated_2024-10-10_20_mab100000_mib1000_ds7_bc3_string` where config_level=2) l2
  using (date, geo_continent, country_code)
  left join (select * from `streamamp-qa-239417.DAS_increment.DAS_config_consolidated_2024-10-10_20_mab100000_mib1000_ds7_bc3_string` where config_level=1) l1
  using (date, geo_continent)
  left join (select * from `streamamp-qa-239417.DAS_increment.DAS_config_consolidated_2024-10-10_20_mab100000_mib1000_ds7_bc3_string` where config_level=0) l0
  using (date)
)
select date, geo_continent, country_code, domain, device_category, 'default', config_level, bidders
from t1
where bidders != bidders_existing;

insert into `streamamp-qa-239417.DAS_increment.DAS_config_consolidated_2024-10-10_20_mab100000_mib1000_ds7_bc3_string`
with t1 as (
  select c.*, coalesce(l6.bidders, l5.bidders, l2.bidders, l1.bidders, l0.bidders) as bidders_existing
  from `streamamp-qa-239417.DAS_increment.DAS_config_geo_cou_dom_dev_rtt_2024-10-10_20_mab100000_mib1000_ds7_bc3_string` c
  left join (select * from `streamamp-qa-239417.DAS_increment.DAS_config_consolidated_2024-10-10_20_mab100000_mib1000_ds7_bc3_string` where config_level=6) l6
  using (date, geo_continent, country_code, domain, rtt_category)
  left join (select * from `streamamp-qa-239417.DAS_increment.DAS_config_consolidated_2024-10-10_20_mab100000_mib1000_ds7_bc3_string` where config_level=5) l5
  using (date, geo_continent, country_code, domain)
  left join (select * from `streamamp-qa-239417.DAS_increment.DAS_config_consolidated_2024-10-10_20_mab100000_mib1000_ds7_bc3_string` where config_level=2) l2
  using (date, geo_continent, country_code)
  left join (select * from `streamamp-qa-239417.DAS_increment.DAS_config_consolidated_2024-10-10_20_mab100000_mib1000_ds7_bc3_string` where config_level=1) l1
  using (date, geo_continent)
  left join (select * from `streamamp-qa-239417.DAS_increment.DAS_config_consolidated_2024-10-10_20_mab100000_mib1000_ds7_bc3_string` where config_level=0) l0
  using (date)
)
select date, geo_continent, country_code, domain, device_category, rtt_category, config_level, bidders
from t1
where bidders != bidders_existing;



-- select *
-- from `streamamp-qa-239417.DAS_increment.DAS_config_consolidated_2024-10-10_20_mab100000_mib1000_ds7_bc3_string`
-- where date ='2024-10-01' and domain !='default'
