

CREATE OR REPLACE TABLE `{project_id}.DAS_increment.{tablename_to_bidder_rps}`
    OPTIONS (
        expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 365 DAY))
    AS

with raw as (

    select *
    from `{project_id}.DAS_increment.{tablename_from}`
    left join `sublime-elixir-273810.ideal_ad_stack.continent_country_mapping` on country_code = geo_country
    where bidder not in ('amazon', 'preGAMAuction', 'seedtag', 'justpremium', 'sonobi')
        and status = 'client'
        and date <= date_sub('{processing_date}', interval 1 day)
        and country_code is not null and country_code != ''

), agg_1_day as (
    select date, bidder {dims},
        sum(session_count) session_count,
        sum(revenue) revenue,
        sum(revenue_sq) revenue_sq
    from raw
    group by date, bidder {dims}


), agg_N_days as (

    select date, bidder {dims},
        sum(session_count) over(partition by bidder {dims} order by date rows between {N_days_preceding} preceding and current row) session_count,
        sum(revenue) over(partition by bidder {dims} order by date rows between {N_days_preceding} preceding and current row) revenue,
        sum(revenue_sq) over(partition by bidder {dims} order by date rows between {N_days_preceding} preceding and current row) revenue_sq
    from agg_1_day

), qual_1 as (

    select *
    from agg_N_days
    where session_count > {min_individual_bidder_session_count}
        and date >= date_add((select min(date) from `{project_id}.DAS_increment.{tablename_from}`), interval {N_days_preceding} day)

), qual_2 as (
    select *
    from qual_1
    qualify sum(session_count) over(partition by date {dims}) > {min_all_bidder_session_count}

), pre_stats as (

    select date, bidder {dims},
        session_count,
        safe_divide(revenue, session_count) mean_revenue,
        safe_divide(revenue_sq, session_count) mean_revenue_sq
    from qual_2

), stats as (

    select date, bidder {dims},
        session_count,
        mean_revenue * 1000 rps,
        if(mean_revenue_sq < pow(mean_revenue, 2), 0, sqrt((mean_revenue_sq - pow(mean_revenue, 2)) / session_count)) * 1000 rps_std
    from pre_stats

), rank as (

    select *, ifnull(safe_divide(rps, rps_std), 0) rps_z_score,
        row_number() over(partition by date {dims} order by rps desc) rn
    from stats

)

select cast('{processing_date}' as date) as processing_date, *
from rank;

CREATE OR REPLACE TABLE `{project_id}.DAS_increment.{tablename_to_config}`
    OPTIONS (
        expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 365 DAY))
    AS

select date {dims},
    string_agg(bidder, ',' order by bidder) bidders, sum(rps) rps
from `{project_id}.DAS_increment.{tablename_to_bidder_rps}`
where rn <= {bidder_count}
group by date {dims}




