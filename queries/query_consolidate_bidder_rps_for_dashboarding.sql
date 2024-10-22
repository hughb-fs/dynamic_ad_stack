
CREATE OR REPLACE TABLE `{project_id}.DAS_increment.DAS_bidder_rps_for_{tablename_ext}_dashboarding`
    OPTIONS (
        expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 365 DAY))
    AS

select date, 'default' geo_continent, 'default' country_code, 'default' domain, 'default' device_category, 'default' rtt_category, config_level, bidder, rps
from `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_{tablename_ext}_unnest`

union all

select date, geo_continent, 'default', 'default', 'default', 'default', config_level, bidder, rps
from `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_{tablename_ext}_unnest`

union all

select date, geo_continent, country_code, 'default', 'default', 'default', config_level, bidder, rps
from `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_cou_{tablename_ext}_unnest`

union all

select date, geo_continent, country_code, 'default', device_category, 'default', config_level, bidder, rps
from `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_cou_dev_{tablename_ext}_unnest`

union all

select date, geo_continent, country_code, 'default', device_category, rtt_category, config_level, bidder, rps
from `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_cou_dev_rtt_{tablename_ext}_unnest`

union all

select date, geo_continent, country_code, domain, 'default', 'default', config_level, bidder, rps
from `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_cou_dom_{tablename_ext}_unnest`

union all

select date, geo_continent, country_code, domain, device_category, 'default', config_level, bidder, rps
from `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_cou_dom_dev_{tablename_ext}_unnest`

union all

select date, geo_continent, country_code, domain, device_category, rtt_category, config_level, bidder, rps
from `streamamp-qa-239417.DAS_increment.DAS_bidder_rps_geo_cou_dom_dev_rtt_{tablename_ext}_unnest`
