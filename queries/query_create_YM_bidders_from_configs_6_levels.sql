
CREATE OR REPLACE TABLE `{project_id}.DAS_increment.{tablename_to_YM_bidders}`
    OPTIONS (
        expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 365 DAY))
    AS

with cohorts as (
    select * except (date), DATE_SUB(DATE_TRUNC(date, {date_granularity}), INTERVAL 1 {date_granularity}) as date, date as date_session_stats
    from `streamamp-qa-239417.DAS_increment.daily_session_stats_{tablename_ext_session_stats}`
    join `sublime-elixir-273810.ideal_ad_stack.continent_country_mapping` on country_code = geo_country
),

cohort_bidders as (
    select c.*,
        coalesce(l5.config_level, l4.config_level, l3.config_level, l2.config_level, l1.config_level, l0.config_level) config_level,
        coalesce(l5.bidders, l4.bidders, l3.bidders, l2.bidders, l1.bidders, l0.bidders) bidders

    from cohorts c
    left join `streamamp-qa-239417.DAS_increment.{YM_strategy_name}_geo_cou_dom_{tablename_ext_YM_config}` l5
        using (date, geo_continent, country_code, domain)
    left join `streamamp-qa-239417.DAS_increment.{YM_strategy_name}_dom_{tablename_ext_YM_config}` l4
        using (date, domain)
    left join `streamamp-qa-239417.DAS_increment.{YM_strategy_name}_geo_cou_dev_{tablename_ext_YM_config}` l3
        using (date, geo_continent, country_code, device_category)
    left join `streamamp-qa-239417.DAS_increment.{YM_strategy_name}_geo_cou_{tablename_ext_YM_config}` l2
        using (date, geo_continent, country_code)
    left join `streamamp-qa-239417.DAS_increment.{YM_strategy_name}_geo_{tablename_ext_YM_config}` l1
        using (date, geo_continent)
    left join `streamamp-qa-239417.DAS_increment.{YM_strategy_name}_{tablename_ext_YM_config}` l0
        using (date)
)

select date_session_stats as date, date as date_config, * except (date, date_session_stats)
from cohort_bidders

