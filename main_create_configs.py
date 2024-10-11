import dateutil.utils
import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery
import configparser
from google.cloud import bigquery_storage
import os, sys
import numpy as np
import datetime as dt
import pickle
import plotly.express as px
import kaleido
from scipy.stats import linregress
from matplotlib.backends.backend_pdf import PdfPages

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

config_path = '../config.ini'
config = configparser.ConfigParser()
config.read(config_path)

project_id = "streamamp-qa-239417"
client = bigquery.Client(project=project_id)
bqstorageclient = bigquery_storage.BigQueryReadClient()


config_hierarchy = [[],
                    ['geo_continent'],
                    ['country_code'],
                    ['country_code', 'device_category'],
                    ['country_code', 'device_category', 'rtt_category'],
                    ['country_code', 'domain'],
                    ['country_code', 'device_category', 'domain'],
                    ['country_code', 'device_category', 'rtt_category', 'domain']]

def get_bq_data(query, replacement_dict={}):
    for k, v in replacement_dict.items():
        query = query.replace("{" + k + "}", str(v))
    return client.query(query).result().to_dataframe(bqstorage_client=bqstorageclient, progress_bar_type='tqdm')

def main_create_bidder_session_stats(last_date, days):

    repl_dict = {'project_id': project_id,
                 'processing_date': last_date,
                 'days_back_start': days,
                 'days_back_end': 1,
                # 'aer_to_bwr_join_type': 'left join'
                 'aer_to_bwr_join_type': 'join'}

    print(f'creating: {project_id}.DAS_increment.daily_bidder_domain_expt_session_stats_unexpanded_'
          f'{repl_dict["aer_to_bwr_join_type"]}_{repl_dict["processing_date"]}_'
          f'{repl_dict["days_back_start"]}_{repl_dict["days_back_end"]}')

    query = open(os.path.join(sys.path[0], 'queries/query_daily_bidder_domain_expt_session_stats.sql'), "r").read()
    get_bq_data(query, repl_dict)

    # query = open(os.path.join(sys.path[0], 'queries/query_daily_opt_session_stats.sql'), "r").read()
    # get_bq_data(query, repl_dict)


def get_dims_and_name(dims_list, last_date, days, days_smoothing):
    dims = ''.join([', ' + d for d in dims_list])
    name = f"{dims.replace(", ", "_")}_{last_date.strftime("%Y-%m-%d")}_{days}_1_{days_smoothing}"
    return dims, name

def main_create_daily_configs(last_date, days):

    processing_date = last_date - dt.timedelta(days=1)
    repl_dict = {'project_id': project_id,
                 'tablename_from': f'daily_bidder_domain_expt_session_stats_join_{last_date.strftime("%Y-%m-%d")}_{days}_1',
                 'processing_date': processing_date.strftime("%Y-%m-%d"),
                 'days_back_end': 1,
                 'min_all_bidder_session_count': 100000,
                 'min_individual_bidder_session_count': 1000}

    for days_smoothing in [1, 7]:
        for dims_list in config_hierarchy:
            dims, name = get_dims_and_name(dims_list, last_date, days, days_smoothing)
            repl_dict['dims'] = dims
            repl_dict['N_days_preceding'] = days_smoothing - 1
            repl_dict['tablename_to'] = f'DAS_config{name}'

            print(f'creating: {repl_dict['tablename_to']}')

            query = open(os.path.join(sys.path[0], 'queries/query_create_daily_country_config.sql'), "r").read()
            get_bq_data(query, repl_dict)


def main_plot_daily_config(last_date, days):

    for days_smoothing in [1, 7]:
        for dims_list in config_hierarchy:
            dims, name = get_dims_and_name(dims_list, last_date, days, days_smoothing)
            tablename_to = f'DAS_config{name}'
            query = (f'select bidder || "_" || status bidder_status, rn, count(*) count, sum(session_count) session_count, avg(rps) rps '
                         f'from `{project_id}.DAS_increment.{tablename_to}` '
                         f'group by 1, 2')

            df = get_bq_data(query)
            df['revenue'] = df['session_count'] * df['rps']

            for col in ['count', 'session_count', 'revenue']:
                df_p = df.pivot(index='rn', columns='bidder_status', values=col).fillna(0)
                df_p_cum_sum = df_p.cumsum()
                df_totals = df_p.sum()
                df_r = df_p_cum_sum / df_totals
                col_order = df_r.mean().sort_values(ascending=False).index
                df_r = df_r[col_order]

                fig, ax = plt.subplots(figsize=(12, 9))
                df_r.plot(ax=ax, xlabel='bidder status rank', ylabel=f'cumulative proportion weighted by {col}', title='Bidder status performance summary')
                fig.savefig(f'plots/bidder_status_perf{name}_{col}.png')

            where_and = ''
            if 'country_code' in dims:
                where_and += f' and country_code = "US"'
            if 'device_category' in dims:
                where_and += f' and device_category = "desktop"'

            query = (f'select date, bidder, avg(rn) rn, count(*) count, sum(session_count) session_count, avg(rps) rps '
                     f'from `{project_id}.DAS_increment.{tablename_to}` '
                     f'where status = "client" {where_and}'
                     f'group by 1, 2')

            df = get_bq_data(query)
            for col in ['rn', 'rps']:
                df_t = df.pivot(index='date', columns='bidder', values=col)
                col_order = df_t.iloc[-1].sort_values(ascending=False).index
                df_t = df_t[col_order]
                fig, ax = plt.subplots(figsize=(12, 9))
                df_t.plot(ax=ax, ylabel=col, title=f'Bidder {col} for date{where_and} with {days_smoothing} days smoothing')
                fig.savefig(f'plots/bidder_status_over_time{name}_{col}.png')

            if 'continent' in dims:
                query = (
                    f'select date, bidder, geo_continent, avg(rn) rn, count(*) count, sum(session_count) session_count, avg(rps) rps '
                    f'from `{project_id}.DAS_increment.{tablename_to}` '
                    f'where status = "client"'
                    f'group by 1, 2, 3')
                df = get_bq_data(query)

                with PdfPages(f'plots/configs{name}.pdf') as pdf:
                    for continent in df['geo_continent'].unique():
                        df_c = df[df['geo_continent'] == continent]

                        fig, ax = plt.subplots(figsize=(12, 9), nrows=2)
                        fig.suptitle(continent)
                        for i, col in enumerate(['rn', 'rps']):
                            df_c_v = df_c.pivot(index='date', columns='bidder', values=col)
                            col_order = df_c_v.iloc[-1].sort_values(ascending=False).index
                            df_c_v = df_c_v[col_order]
                            df_c_v.plot(ax=ax[i], ylabel=col)
                        pdf.savefig()



if __name__ == "__main__":
    last_date = dt.date(2024, 10, 10)
    days = 20
    main_create_bidder_session_stats(last_date, days)
    main_create_daily_configs(last_date, days)
    main_plot_daily_config(last_date, days)

    # last_date = dt.date(2024, 10, 8)
    # days = 20
    # main_create_daily_configs(last_date, days)
    # main_plot_daily_config(last_date, days)
