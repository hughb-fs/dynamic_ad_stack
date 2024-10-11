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
                    ['geo_continent', 'country_code'],
                    ['geo_continent', 'country_code', 'device_category'],
                    ['geo_continent', 'country_code', 'device_category', 'rtt_category'],
                    ['geo_continent', 'country_code', 'domain'],
                    ['geo_continent', 'country_code', 'domain', 'device_category'],
                    ['geo_continent', 'country_code', 'domain', 'device_category', 'rtt_category']]

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

    all_dims = list(set(sum(config_hierarchy, [])))

    for days_smoothing in [1, 7]:
        if days < days_smoothing:
            continue

        select_for_union_list = []
        for config_level, dims_list in enumerate(config_hierarchy):
            dims, name = get_dims_and_name(dims_list, last_date, days, days_smoothing)
            repl_dict['dims'] = dims
            repl_dict['N_days_preceding'] = days_smoothing - 1
            repl_dict['tablename_to_bidder_rps'] = f'DAS_bidder_rps{name}'
            repl_dict['tablename_to_config'] = f'DAS_config{name}'
            repl_dict['bidder_count'] = 3

            print(f'creating: {repl_dict['tablename_to_bidder_rps']} and {repl_dict['tablename_to_config']}')

            query = open(os.path.join(sys.path[0], 'queries/query_create_config.sql'), "r").read()
            get_bq_data(query, repl_dict)

            select_dims = ", ".join([d if d in dims_list else f"'default' {d}" for d in all_dims])
            select_for_union = (f'(select date, {select_dims}, bidders, rps, {config_level} config_level '
                                f'from `{project_id}.DAS_increment.{repl_dict['tablename_to_config']}`)')
            select_for_union_list.append(select_for_union)

        query = (f'CREATE OR REPLACE TABLE `{project_id}.DAS_increment.DAS_config_combined_uncompressed_{last_date.strftime("%Y-%m-%d")}_{days}_1_{days_smoothing}` '
                 f'OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)) '
                 f'AS {" union all ".join(select_for_union_list)}')
        get_bq_data(query)


def main_plot_daily_config(last_date, days):

    for days_smoothing in [1, 7]:
        if days < days_smoothing:
            continue

        for dims_list in config_hierarchy:
            dims, name = get_dims_and_name(dims_list, last_date, days, days_smoothing)
            tablename = f'DAS_bidder_rps{name}'
            query = (f'select bidder, rn, count(*) count, sum(session_count) session_count, avg(rps) rps '
                         f'from `{project_id}.DAS_increment.{tablename}` '
                         f'group by 1, 2')

            df = get_bq_data(query)
            df['revenue'] = df['session_count'] * df['rps']

            for col in ['count', 'session_count', 'revenue']:
                df_p = df.pivot(index='rn', columns='bidder', values=col).fillna(0)
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
                     f'from `{project_id}.DAS_increment.{tablename}` '
                     f'where 1=1 {where_and}'
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
                    f'from `{project_id}.DAS_increment.{tablename}` '
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
    days = 2
    # main_create_bidder_session_stats(last_date, days)
    main_create_daily_configs(last_date, days)
    main_plot_daily_config(last_date, days)

    # last_date = dt.date(2024, 10, 8)
    # days = 20
    # main_create_daily_configs(last_date, days)
    # main_plot_daily_config(last_date, days)
