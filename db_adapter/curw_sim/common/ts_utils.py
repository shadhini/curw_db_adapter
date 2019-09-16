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
    return processed_df.interpolate(method=interpolation_method, limit_direction='both')




