with rollup67 as (

select * except (rps)
from `streamamp-qa-239417.DAS_increment.DAS_config_combined_uncompressed_2024-10-10_2_1_1`
where config_level in (6, 7)
qualify min(config_level) over(partition by date, geo_continent, device_category, country_code, domain, bidders) >= config_level or config_level != 7

), rollup567 as (

select * from (
  select * from rollup67

  union all

  select * except (rps)
  from `streamamp-qa-239417.DAS_increment.DAS_config_combined_uncompressed_2024-10-10_2_1_1`
  where config_level = 5
)
qualify min(config_level) over(partition by date, geo_continent, country_code, domain, bidders) >= config_level or config_level != 6

), rollup4567 as (

select *
from (
  select * from rollup567

  union all

  select * except (rps)
  from `streamamp-qa-239417.DAS_increment.DAS_config_combined_uncompressed_2024-10-10_2_1_1`
  where config_level = 4
)
qualify min(config_level) over(partition by date, geo_continent, device_category, rtt_category, country_code, bidders) >= config_level or config_level != 5

), rollup34567 as (

select *
from (
  select * from rollup4567

  union all

  select * except (rps)
  from `streamamp-qa-239417.DAS_increment.DAS_config_combined_uncompressed_2024-10-10_2_1_1`
  where config_level = 3
)
qualify min(config_level) over(partition by date, geo_continent, device_category, country_code, bidders) >= config_level or config_level != 4

), rollup234567 as (

select *
from (
  select * from rollup34567

  union all

  select * except (rps)
  from `streamamp-qa-239417.DAS_increment.DAS_config_combined_uncompressed_2024-10-10_2_1_1`
  where config_level = 2
)
qualify min(config_level) over(partition by date, geo_continent, country_code, bidders) >= config_level or config_level != 3

), rollup1234567 as (

select *
from (
  select * from rollup234567

  union all

  select * except (rps)
  from `streamamp-qa-239417.DAS_increment.DAS_config_combined_uncompressed_2024-10-10_2_1_1`
  where config_level = 1
)
qualify min(config_level) over(partition by date, geo_continent, bidders) >= config_level or config_level != 2
), rollup01234567 as (

select *
from (
  select * from rollup1234567

  union all

  select * except (rps)
  from `streamamp-qa-239417.DAS_increment.DAS_config_combined_uncompressed_2024-10-10_2_1_1`
  where config_level = 0
)
qualify min(config_level) over(partition by date, bidders) >= config_level or config_level != 1
)

select *
from rollup01234567

