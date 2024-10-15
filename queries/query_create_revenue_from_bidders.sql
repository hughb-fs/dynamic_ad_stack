CREATE OR REPLACE TABLE `streamamp-qa-239417.DAS_increment.{tablename_to}`
    OPTIONS (
        expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 365 DAY))
    AS

with bidders as(
    select t1.* except(bidders), bidder
    from `streamamp-qa-239417.DAS_increment.{tablename_bidders}` t1, t1.bidders as bidder
),

t3 as (
    select bidders.*,
        coalesce(l7.config_level, l6.config_level, l5.config_level, l4.config_level, l3.config_level, l2.config_level, l1.config_level, l0.config_level) config_level,
        coalesce(l7.bidder, l6.bidder, l5.bidder, l4.bidder, l3.bidder, l2.bidder, l1.bidder, l0.bidder) bidder,
        coalesce(l7.rn, l6.rn, l5.rn, l4.rn, l3.rn, l2.rn, l1.rn, l0.rn) rn,
        coalesce(l7.rps, l6.rps, l5.rps, l4.rps, l3.rps, l2.rps, l1.rps, l0.rps) rps

    from bidders
    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_continent_country_code_domain_device_category_rtt_category_{tablename_ext_bidder_rps}_1` l7
        using (date, bidder, geo_continent, country_code, domain, device_category, rtt_category)
    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_continent_country_code_domain_device_category_{tablename_ext_bidder_rps}_1` l6
        using (date, bidder, geo_continent, country_code, domain, device_category)
    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_continent_country_code_domain_{tablename_ext_bidder_rps}_1` l5
        using (date, bidder, geo_continent, country_code, domain)
    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_continent_country_code_device_category_rtt_category_{tablename_ext_bidder_rps}_1` l4
        using (date, bidder, geo_continent, country_code, device_category, rtt_category)
    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_continent_country_code_device_category_{tablename_ext_bidder_rps}_1` l3
        using (date, bidder, geo_continent, country_code, device_category)
    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_continent_country_code_{tablename_ext_bidder_rps}_1` l2
        using (date, bidder, geo_continent, country_code)
    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_continent_{tablename_ext_bidder_rps}_1` l1
        using (date, bidder, geo_continent)
    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_{tablename_ext_bidder_rps}_1` l0
        using (date, bidder)
)

select date, geo_continent, country_code, domain, device_category, rtt_category,
  avg(rn) rn_avg,
  sum(rps) rps_sum,
  sum(rps * session_count) / 1000 revenue,
  min(session_count) session_count_min,
  max(session_count) session_count_max
from t3
group by 1, 2, 3, 4, 5, 6;

select date, sum(revenue) revenue
from `streamamp-qa-239417.DAS_increment.{tablename_to}`
group by 1
order by 1





with t1 as (

    select c.*,
        coalesce(l7.config_level, l6.config_level, l5.config_level, l4.config_level, l3.config_level, l2.config_level, l1.config_level, l0.config_level) config_level,
        coalesce(l7.bidder_rps, l6.bidder_rps, l5.bidder_rps, l4.bidder_rps, l3.bidder_rps, l2.bidder_rps, l1.bidder_rps, l0.bidder_rps) bidder_rps

    from `streamamp-qa-239417.DAS_increment.DAS_bidders_2024-10-10_20_1_ds1_bm0_bc10` c

    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_continent_country_code_domain_device_category_rtt_category_2024-10-10_20_1_ds1` l7
        using (date, geo_continent, country_code, domain, device_category, rtt_category)
    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_continent_country_code_domain_device_category_2024-10-10_20_1_ds1` l6
        using (date, geo_continent, country_code, domain, device_category)
    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_continent_country_code_domain_2024-10-10_20_1_ds1` l5
        using (date, geo_continent, country_code, domain)
    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_continent_country_code_device_category_rtt_category_2024-10-10_20_1_ds1` l4
        using (date, geo_continent, country_code, device_category, rtt_category)
    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_continent_country_code_device_category_2024-10-10_20_1_ds1` l3
        using (date, geo_continent, country_code, device_category)
    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_continent_country_code_2024-10-10_20_1_ds1` l2
        using (date, geo_continent, country_code)
    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_continent_2024-10-10_20_1_ds1` l1
        using (date, geo_continent)
    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_2024-10-10_20_1_ds1` l0
        using (date)

    where date='2024-09-23' and geo_continent='SA' and country_code='AR' and domain='bleepingcomputer.com' and device_category='mobile' and rtt_category='superfast'

)

select *
from t1 --`streamamp-qa-239417.DAS_increment.DAS_bidder_rps_2024-10-10_20_1_ds1` l0
limit 100





with t1 as (

    select c.*,
        coalesce(l7.config_level, l6.config_level, l5.config_level, l4.config_level, l3.config_level, l2.config_level, l1.config_level, l0.config_level) config_level,
        coalesce(l7.bidder_rps, l6.bidder_rps, l5.bidder_rps, l4.bidder_rps, l3.bidder_rps, l2.bidder_rps, l1.bidder_rps, l0.bidder_rps) bidder_rps

    from `streamamp-qa-239417.DAS_increment.DAS_bidders_2024-10-10_20_1_ds1_bm0_bc10` c

    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_continent_country_code_domain_device_category_rtt_category_2024-10-10_20_1_ds1` l7
        using (date, geo_continent, country_code, domain, device_category, rtt_category)
    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_continent_country_code_domain_device_category_2024-10-10_20_1_ds1` l6
        using (date, geo_continent, country_code, domain, device_category)
    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_continent_country_code_domain_2024-10-10_20_1_ds1` l5
        using (date, geo_continent, country_code, domain)
    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_continent_country_code_device_category_rtt_category_2024-10-10_20_1_ds1` l4
        using (date, geo_continent, country_code, device_category, rtt_category)
    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_continent_country_code_device_category_2024-10-10_20_1_ds1` l3
        using (date, geo_continent, country_code, device_category)
    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_continent_country_code_2024-10-10_20_1_ds1` l2
        using (date, geo_continent, country_code)
    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_continent_2024-10-10_20_1_ds1` l1
        using (date, geo_continent)
    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_2024-10-10_20_1_ds1` l0
        using (date)

    where date='2024-09-23' and geo_continent='SA' and country_code='AR' and domain='bleepingcomputer.com' and device_category='mobile' and rtt_category='superfast'

)

select date, geo_continent, country_code, domain, device_category, rtt_category, bidders_strategy, bidder_rps.bidder, bidder_rps.rps
from t1
cross join unnest(t1.bidders) as bidders_strategy
cross join unnest(t1.bidder_rps) as bidder_rps
limit 100


-- SELECT
--   race,
--   participant
-- FROM Races AS r
-- CROSS JOIN UNNEST(r.participants) AS participant;





with t1 as (

    select c.*,
        coalesce(l7.config_level, l6.config_level, l5.config_level, l4.config_level, l3.config_level, l2.config_level, l1.config_level, l0.config_level) config_level,
        coalesce(l7.bidder_rps, l6.bidder_rps, l5.bidder_rps, l4.bidder_rps, l3.bidder_rps, l2.bidder_rps, l1.bidder_rps, l0.bidder_rps) bidder_rps

    from `streamamp-qa-239417.DAS_increment.YM_daily_bidders_2024-10-10_20_1_ds1_bm0_bc10` c

    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_continent_country_code_domain_device_category_rtt_category_2024-10-10_20_1_ds1` l7
        using (date, geo_continent, country_code, domain, device_category, rtt_category)
    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_continent_country_code_domain_device_category_2024-10-10_20_1_ds1` l6
        using (date, geo_continent, country_code, domain, device_category)
    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_continent_country_code_domain_2024-10-10_20_1_ds1` l5
        using (date, geo_continent, country_code, domain)
    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_continent_country_code_device_category_rtt_category_2024-10-10_20_1_ds1` l4
        using (date, geo_continent, country_code, device_category, rtt_category)
    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_continent_country_code_device_category_2024-10-10_20_1_ds1` l3
        using (date, geo_continent, country_code, device_category)
    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_continent_country_code_2024-10-10_20_1_ds1` l2
        using (date, geo_continent, country_code)
    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_continent_2024-10-10_20_1_ds1` l1
        using (date, geo_continent)
    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_2024-10-10_20_1_ds1` l0
        using (date)

    where date='2024-09-23' and geo_continent='SA' and country_code='AR' and domain='bleepingcomputer.com' and device_category='mobile' and rtt_category='superfast'

)

select date, geo_continent, country_code, domain, device_category, rtt_category, bidders_strategy, bidder_rps.bidder, bidder_rps.rps
from t1
cross join unnest(t1.bidders) as bidders_strategy
join unnest(t1.bidder_rps) as bidder_rps on bidders_strategy=bidder_rps.bidder
limit 100




with t1 as (

    select c.*,
        coalesce(l7.config_level, l6.config_level, l5.config_level, l4.config_level, l3.config_level, l2.config_level, l1.config_level, l0.config_level) config_level,
        coalesce(l7.bidder_rps, l6.bidder_rps, l5.bidder_rps, l4.bidder_rps, l3.bidder_rps, l2.bidder_rps, l1.bidder_rps, l0.bidder_rps) bidder_rps

    from `streamamp-qa-239417.DAS_increment.YM_daily_bidders_2024-10-10_20_1_ds1_bm0_bc10` c

    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_continent_country_code_domain_device_category_rtt_category_2024-10-10_20_1_ds1` l7
        using (date, geo_continent, country_code, domain, device_category, rtt_category)
    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_continent_country_code_domain_device_category_2024-10-10_20_1_ds1` l6
        using (date, geo_continent, country_code, domain, device_category)
    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_continent_country_code_domain_2024-10-10_20_1_ds1` l5
        using (date, geo_continent, country_code, domain)
    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_continent_country_code_device_category_rtt_category_2024-10-10_20_1_ds1` l4
        using (date, geo_continent, country_code, device_category, rtt_category)
    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_continent_country_code_device_category_2024-10-10_20_1_ds1` l3
        using (date, geo_continent, country_code, device_category)
    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_continent_country_code_2024-10-10_20_1_ds1` l2
        using (date, geo_continent, country_code)
    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_continent_2024-10-10_20_1_ds1` l1
        using (date, geo_continent)
    left join `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_2024-10-10_20_1_ds1` l0
        using (date)

    where date='2024-09-23' and geo_continent='SA' and country_code='AR' and domain='bleepingcomputer.com' and device_category='mobile'

)

select date, geo_continent, country_code, domain, device_category, rtt_category, --bidders_strategy, bidder_rps.bidder,
sum(bidder_rps.rps) rps_sum,
    count(*) bidder_count,
--  avg(bidder_rps.rn) rn_avg,
  sum(bidder_rps.rps * session_count) / 1000 revenue,
  min(session_count) session_count_min,
  max(session_count) session_count_max

from t1
cross join unnest(t1.bidders) as bidders_strategy
join unnest(t1.bidder_rps) as bidder_rps on bidders_strategy=bidder_rps.bidder
group by 1, 2, 3, 4, 5, 6



-- SELECT
--   race,
--   participant
-- FROM Races AS r
-- CROSS JOIN UNNEST(r.participants) AS participant;


