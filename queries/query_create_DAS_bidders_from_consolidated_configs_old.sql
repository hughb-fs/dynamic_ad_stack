
CREATE OR REPLACE TABLE `{project_id}.DAS_increment.{tablename_to}_consolidated`
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
        coalesce(l7.config_level, l6.config_level, l5.config_level, l4.config_level, l3.config_level, l2.config_level, l1.config_level, l0.config_level) config_level,
        coalesce(l7.bidders, l6.bidders, l5.bidders, l4.bidders, l3.bidders, l2.bidders, l1.bidders, l0.bidders) bidders

    from cohorts c
    left join (select * from `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext_consolidated}` where config_level=7) l7
        using (date, geo_continent, country_code, domain, device_category, rtt_category)
    left join (select * except (rtt_category) from `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext_consolidated}` where config_level=6) l6
        using (date, geo_continent, country_code, domain, device_category)
    left join (select * except (device_category, rtt_category) from `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext_consolidated}` where config_level=5) l5
        using (date, geo_continent, country_code, domain)
    left join (select * from `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext_consolidated}` where config_level=4) l4
        using (date, geo_continent, country_code, device_category, rtt_category)
    left join (select * from `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext_consolidated}` where config_level=3) l3
        using (date, geo_continent, country_code, device_category)
    left join (select * from `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext_consolidated}` where config_level=2) l2
        using (date, geo_continent, country_code)
    left join (select * from `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext_consolidated}` where config_level=1) l1
        using (date, geo_continent)
    left join (select * from `{project_id}.DAS_increment.DAS_config_consolidated_{tablename_ext_consolidated}` where config_level=0) l0
        using (date)
)

select date_session_stats as date, date as date_config, * except (date, date_session_stats)
from cohort_bidders


-- TESTING CODE
--select *
--from
--(
--    select date, geo_continent, country_code, domain, device_category, rtt_category, string_agg(b, ',' order by b) bidders
--    from (
--      select * except (domain, bidders), ifnull(domain, '_null_') domain
--        from `streamamp-qa-239417.DAS_increment.DAS_bidders_2024-10-10_20_mab100000_mib1000_ds1_bm0_bc5_consolidated` a1
--        cross join unnest(a1.bidders) as b
--    )
--    group by 1, 2, 3, 4, 5, 6
--) t1
--join
--(
--    select date, geo_continent, country_code, domain, device_category, rtt_category, string_agg(b, ',' order by b) bidders
--    from (
--      select * except (domain, bidders), ifnull(domain, '_null_') domain
--        from `streamamp-qa-239417.DAS_increment.DAS_bidders_2024-10-10_20_mab100000_mib1000_ds1_bm0_bc5` a2
--        cross join unnest(a2.bidders) as b
--    )
--    group by 1, 2, 3, 4, 5, 6
--) t2
--using (date, geo_continent, country_code, domain, device_category, rtt_category)
--where t1.bidders != t2.bidders
