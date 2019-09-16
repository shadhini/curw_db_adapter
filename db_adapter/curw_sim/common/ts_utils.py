import pandas as pd
import numpy as np


def fill_ts_missing_entries(start, end, timeseries, interpolation_method, timestep):
    """

    :param start: "YYYY-MM-DD HH:MM:SS" the starting timestamp of the timeseries index
    :param end: "YYYY-MM-DD HH:MM:SS" the last timestamp of the timeseries index
    :param timeseries: list of [time, value] lists
    :param interpolation_method:
    :param timestep: int: the timestep value in minutes
    :return:
    """
    # create empty dataframe with all time steps
    empty_df_index = pd.date_range(start=start, end=end, freq='{}min'.format(timestep))
    empty_df = pd.DataFrame(index=empty_df_index)

    # covert input timeseries into a dataframe
    original_data = np.array(timeseries)
    index = original_data[:, 0]
    data = original_data[:, 1:]

    input_df = pd.DataFrame.from_records(data=data, index=index)

    # fill missing entries
    df = empty_df.join(input_df)

    # fill missing values using specified interpolation method
    processed_df = pd.to_numeric(df[df.columns[0]], errors='coerce')
    final_df = processed_df.interpolate(method=interpolation_method, limit_direction='both')
    final_df.index = final_df.index.map(str)
    return final_df.reset_index().values.tolist()


# TS2 = [["2019-08-22 01:00:00", 1.8], ["2019-08-22 02:30:00", 1.5], ["2019-08-22 03:30:00", 1.4],["2019-08-22 07:30:00", 2.4],
#        ["2019-08-22 08:30:00", 2.5], ["2019-08-23 07:30:00", 2.5], ["2019-08-23 08:30:00", 2.5]]
#
# print(fill_ts_missing_entries(start="2019-08-22 00:00:00", end="2019-08-23 12:00:00", timeseries=TS2,
#                               interpolation_method="linear", timestep=30))




