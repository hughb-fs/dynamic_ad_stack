
CREATE OR REPLACE TABLE `{project_id}.DAS_increment.{tablename_to}`
    OPTIONS (
        expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 365 DAY))
    AS

with cohorts as (
    select * except (date), DATE_SUB(date, INTERVAL {days_match} day) as date, date as date_session_stats
    from `{project_id}.DAS_increment.daily_session_stats_{tablename_ext_session_stats}`
    join `sublime-elixir-273810.ideal_ad_stack.continent_country_mapping` on country_code = geo_country
), 

cohort_bidders as (
    select c.*,
        coalesce(l1.config_level, l0.config_level) config_level,
        coalesce(l1.bidders, l0.bidders) bidders

    from cohorts c
    left join `{project_id}.DAS_increment.DAS_config_geo_{tablename_ext_DAS_config}` l1
        using (date, geo_continent)
    left join `{project_id}.DAS_increment.DAS_config_{tablename_ext_DAS_config}` l0
        using (date)
)

select date_session_stats as date, date as date_config, * except (date, date_session_stats)
from cohort_bidders