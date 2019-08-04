import pandas as pd
import datetime


def get_relative_spans(start_year, end_year, df):
    relative_span_dict = {}

    for year in range(start_year, end_year + 1):
        total_weeks = weeks_for_year(year)
        for week in range(1, total_weeks + 1):
            start, end = get_start_end_dates(year, week)
            filtered_df = df.loc[start:end]
            if not (filtered_df.empty):
                key = str(year) + "-W" + str(week)
                max_close = float(filtered_df["close_usd"].max())
                min_close = float(filtered_df["close_usd"].min())
                relative_span_dict[key] = (max_close - min_close) / min_close

    return relative_span_dict


def get_start_end_dates(year, week):
    d = datetime.date(year, 1, 1)
    if d.weekday() <= 3:
        d = d - datetime.timedelta(d.weekday())
    else:
        d = d + datetime.timedelta(7 - d.weekday())
    dlt = datetime.timedelta(days=(week - 1) * 7)
    return str(d + dlt), str(d + dlt + datetime.timedelta(days=6))


def weeks_for_year(year):
    last_week = datetime.date(int(year), 12, 28)
    return last_week.isocalendar()[1]


def get_week_with_greatest_relative_span(cal_dict):
    v = list(cal_dict.values())
    k = list(cal_dict.keys())
    key = k[v.index(max(v))]
    return key
