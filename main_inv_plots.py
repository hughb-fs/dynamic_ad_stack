

import dateutil.utils
import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery
import configparser
from google.cloud import bigquery_storage
import os, sys
import numpy as np
import datetime as dt
from naming_utils import *
from matplotlib.backends.backend_pdf import PdfPages


pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

# config_path = '../config.ini'
# config = configparser.ConfigParser()
# config.read(config_path)

project_id = "streamamp-qa-239417"
client = bigquery.Client(project=project_id)
bqstorageclient = bigquery_storage.BigQueryReadClient()

def get_bq_data(query, replacement_dict={}):
    for k, v in replacement_dict.items():
        query = query.replace("{" + k + "}", str(v))
    return client.query(query).result().to_dataframe(bqstorage_client=bqstorageclient, progress_bar_type='tqdm')

config_hierarchy = [[],
                    ['geo_continent'],
                    ['geo_continent', 'country_code'],
                    ['geo_continent', 'country_code', 'device_category'],
                    ['geo_continent', 'country_code', 'device_category', 'rtt_category'],
                    ['geo_continent', 'country_code', 'domain'],
                    ['geo_continent', 'country_code', 'domain', 'device_category'],
                    ['geo_continent', 'country_code', 'domain', 'device_category', 'rtt_category']]


def main_z_score_plot():
    last_date = dt.date(2024, 10, 10)
    days = 20
    bidder_count = 10

    repl_dict = {'project_id': project_id}

    cohort_count_list = []
    with PdfPages(f'plots/z_scores.pdf') as pdf:
        for (min_all_bidder_session_count, min_individual_bidder_session_count) in [(10000, 200), (100000, 1000)]:
            for days_smoothing in [1, 7]:

                for config_level, dims_list in enumerate(config_hierarchy):

                    if config_level <= 1:
                        continue

                    dims, name, not_null_str = get_dims_and_name(dims_list, last_date, days, days_smoothing, min_all_bidder_session_count,
                                                   min_individual_bidder_session_count)

                    print(f'loading and plotting: {project_id}.DAS_increment.DAS_bidder_rps{name}')
                    cohort_count = get_bq_data(f'select avg(count) from (select date, count(*) count '
                                               f'from `{project_id}.DAS_increment.DAS_bidder_rps{name}` group by 1)').values[0][0]

                    tablename = f'DAS_config_consolidated_{get_tablename_ext(last_date, days, min_all_bidder_session_count,
                                  min_individual_bidder_session_count, days_smoothing)}_bc{bidder_count}'

                    cohort_count_consolidated = get_bq_data(f'select avg(count) from (select date, count(*) count '
                                               f'from `{project_id}.DAS_increment.{tablename}` where config_level={config_level} group by 1)').values[0][0]

                    cohort_count_list.append(
                        {'min_all_bidder_session_count': min_all_bidder_session_count, 'days_smoothing': days_smoothing,
                         'dims': dims, 'cohort_count': cohort_count, 'cohort_count_consolidated': cohort_count_consolidated})

                    repl_dict['dims'] = dims
                    repl_dict['tablename'] = f'DAS_bidder_rps{name}_unnest'
                    query = open(os.path.join(sys.path[0], 'queries/query_inv_z_score.sql'), "r").read()
                    df = get_bq_data(query, repl_dict)

                    max_z_plot = 2
                    fig_1, ax_1 = plt.subplots(figsize=(16, 12), nrows=2, ncols=2)
                    fig_1.suptitle(f'min_all_bidder_session_count: {min_all_bidder_session_count}, days_smoothing: {days_smoothing}, dims: {dims}, cohort_count: {cohort_count:0.0f}')
                    ax_1 = ax_1.flatten()
                    for rn_1_2 in range(4):
                        df_rn_1_2 = df[df['rn_1_2'] == rn_1_2 + 1]

                        df_hist_cdf_list = []
                        fig, ax = plt.subplots(figsize=(12, 9))
                        for rn_1 in df_rn_1_2['rn_1'].unique():

                            z = df_rn_1_2[df_rn_1_2['rn_1'] == rn_1]['z_score'].values
                            z[z > max_z_plot + 0.5] = max_z_plot + 0.5
                            y_cdf, x_cdf, _ = plt.hist(z, bins=100, cumulative=True, density=True)
                            df_hist_cdf_list.append(pd.DataFrame(y_cdf.transpose(), index=pd.Index(x_cdf[:-1]), columns=[rn_1]))

                        df_hist_cdf = pd.concat(df_hist_cdf_list).sort_index().ffill().bfill()
                        col_order = df_hist_cdf[df_hist_cdf.index < max_z_plot].iloc[-1, :].sort_values(ascending=False).index
                        df_hist_cdf = df_hist_cdf[col_order]

                        df_hist_cdf.plot(ax=ax_1[rn_1_2], ylabel='cdf', xlim=[0, max_z_plot], title=f'diff: {rn_1_2+1}')

                    pdf.savefig(fig_1)
                    fig_1.savefig(f'plots/z_score_pngs/z_score_{repl_dict['tablename']}.png')

    cohort_count_all = pd.DataFrame(cohort_count_list).pivot(index='dims', values=['cohort_count', 'cohort_count_consolidated'],
                                                             columns=['min_all_bidder_session_count', 'days_smoothing'])
    cohort_count_all.to_csv('plots/cohort_counts.csv')
    f = 9

if __name__ == "__main__":
    main_z_score_plot()