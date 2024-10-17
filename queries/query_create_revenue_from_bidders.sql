CREATE OR REPLACE TABLE `{project_id}.DAS_increment.{tablename_to}`
    OPTIONS (
        expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 365 DAY))
    AS

with t1 as (

    select c.*,
        coalesce(l7.config_level, l6.config_level, l5.config_level, l4.config_level, l3.config_level, l2.config_level, l1.config_level, l0.config_level) config_level,
        coalesce(l7.bidder_rps, l6.bidder_rps, l5.bidder_rps, l4.bidder_rps, l3.bidder_rps, l2.bidder_rps, l1.bidder_rps, l0.bidder_rps) bidder_rps

    from `{project_id}.DAS_increment.{tablename_bidders}` c

    left join `{project_id}.DAS_increment.DAS_bidder_rps_geo_cou_dom_dev_rtt_{tablename_ext_bidder_rps}` l7
        using (date, geo_continent, country_code, domain, device_category, rtt_category)
    left join `{project_id}.DAS_increment.DAS_bidder_rps_geo_cou_dom_dev_{tablename_ext_bidder_rps}` l6
        using (date, geo_continent, country_code, domain, device_category)
    left join `{project_id}.DAS_increment.DAS_bidder_rps_geo_cou_dom_{tablename_ext_bidder_rps}` l5
        using (date, geo_continent, country_code, domain)
    left join `{project_id}.DAS_increment.DAS_bidder_rps_geo_cou_dev_rtt_{tablename_ext_bidder_rps}` l4
        using (date, geo_continent, country_code, device_category, rtt_category)
    left join `{project_id}.DAS_increment.DAS_bidder_rps_geo_cou_dev_{tablename_ext_bidder_rps}` l3
        using (date, geo_continent, country_code, device_category)
    left join `{project_id}.DAS_increment.DAS_bidder_rps_geo_cou_{tablename_ext_bidder_rps}` l2
        using (date, geo_continent, country_code)
    left join `{project_id}.DAS_increment.DAS_bidder_rps_geo_{tablename_ext_bidder_rps}` l1
        using (date, geo_continent)
    left join `{project_id}.DAS_increment.DAS_bidder_rps_{tablename_ext_bidder_rps}` l0
        using (date)
)

select date, geo_continent, country_code, domain, device_category, rtt_category, --bidders_strategy, bidder_rps.bidder,
sum(bidder_rps.rps) rps_sum,
    count(*) bidder_count,
    avg(bidder_rps.rn) rn_avg,
    sum(bidder_rps.rps * session_count) / 1000 revenue,
    min(session_count) session_count_min,
    max(session_count) session_count_max

from t1
cross join unnest(t1.bidders) as bidders_strategy
join unnest(t1.bidder_rps) as bidder_rps on bidders_strategy=bidder_rps.bidder
group by 1, 2, 3, 4, 5, 6

