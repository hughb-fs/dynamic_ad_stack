def get_tablename_ext(last_date, days, min_all_bidder_session_count, min_individual_bidder_session_count,
                      days_smoothing):
    return f"{last_date.strftime("%Y-%m-%d")}_{days}_mab{min_all_bidder_session_count}_mib{min_individual_bidder_session_count}_ds{days_smoothing}"

def get_dims(dims_list):
    return ''.join([', ' + d for d in dims_list])

def get_dims_name(dims_list):
    return ''.join(['_' + d[:3] for d in dims_list])

def get_dims_and_name(dims_list, last_date, days, days_smoothing, min_all_bidder_session_count,
                      min_individual_bidder_session_count):
    dims = get_dims(dims_list)

    tablename_ext = get_tablename_ext(last_date, days, min_all_bidder_session_count,
                                      min_individual_bidder_session_count, days_smoothing)
    name = f"{get_dims_name(dims_list)}_{tablename_ext}"
    return dims, name
def get_daily_data_tablename(expt_or_opt, expanded, last_date, days, aer_to_bwr_join_type='join', partial=False):

    if expt_or_opt == "expt":
        base = f'daily_bidder_expt_session_stats_{aer_to_bwr_join_type}_{last_date}_{days}{'_partial' if partial else ''}'
        if expanded:
            return base + "_expanded"
        else:
            return base + "_unexpanded"
    elif expt_or_opt == "opt":
        return f'daily_session_stats_{last_date}_{days}{'_partial' if partial else ''}'
    else:
        assert False