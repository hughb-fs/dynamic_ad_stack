CREATE OR REPLACE TABLE `{project_id}.DAS_increment.{tablename_to_config}`
    OPTIONS (
        expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 365 DAY))
    AS

with raw_expt as (
    select DATE_TRUNC(date, {date_granularity}) date {dims}
    from `streamamp-qa-239417.DAS_increment.{tablename_expt_from}`
    left join `sublime-elixir-273810.ideal_ad_stack.continent_country_mapping` on country_code = geo_country
    where bidder not in ('amazon', 'preGAMAuction', 'seedtag', 'justpremium', 'sonobi')
        and status = 'client'
        and country_code is not null and country_code != ''
    group by date {dims}
    having sum(session_count) > 100
),

raw_session as (
    select DATE_TRUNC(date, {date_granularity}) date {dims}, sum(session_count) session_count
    from `{project_id}.DAS_increment.{tablename_opt_from}`
    left join `sublime-elixir-273810.ideal_ad_stack.continent_country_mapping` on country_code = geo_country
    where country_code is not null and country_code != ''
    group by date {dims}
),

cohorts_to_include as (
  select date {dims}
  from raw_session
  join raw_expt using (date {dims})
  qualify row_number() over (partition by date order by session_count desc) <= {max_cohort_count}
),

raw as (
    select *
    from `{project_id}.DAS_increment.{tablename_expt_from}`
    left join `sublime-elixir-273810.ideal_ad_stack.continent_country_mapping` on country_code = geo_country
    where bidder not in ('amazon', 'preGAMAuction', 'seedtag', 'justpremium', 'sonobi')
        and status = 'client'
        and country_code is not null and country_code != ''
),

agg as (
    select DATE_TRUNC(date, {date_granularity}) date, bidder {dims},
        ifnull(safe_divide(sum(revenue), sum(session_count)), 0) rps
    from raw
    group by date {dims}, bidder
),

bidder_rps as (
    select *, row_number() over(partition by date {dims} order by rps desc) rn
    from agg
    join cohorts_to_include using (date {dims})
)

select date {dims}, {config_level} config_level, array_agg(bidder) bidders
from bidder_rps
where rn <= {bidder_count}
group by date {dims};
