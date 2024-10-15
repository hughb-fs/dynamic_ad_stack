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

