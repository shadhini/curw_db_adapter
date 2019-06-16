from datetime import datetime, timedelta


def process_5_min_ts(newly_extracted_timeseries, expected_start):

    processed_ts = []

    current_timestamp = expected_start
    extracted_ts_index = 0

    while extracted_ts_index < len(newly_extracted_timeseries):
        if current_timestamp == newly_extracted_timeseries[extracted_ts_index][0]:
            processed_ts.append(newly_extracted_timeseries[extracted_ts_index])
            extracted_ts_index +=1
            current_timestamp = current_timestamp + timedelta(minutes=5)
        elif current_timestamp < newly_extracted_timeseries[extracted_ts_index][0]:
            processed_ts.append([current_timestamp, -99999])
            current_timestamp = current_timestamp + timedelta(minutes=5)
        else:
            extracted_ts_index +=1

    return processed_ts


def fill_missing_values_5_min_ts(newly_extracted_timeseries, OBS_TS):

    obs_timeseries = OBS_TS

    obs_index = 0
    new_ts_index = 0

    while new_ts_index < len(newly_extracted_timeseries) and obs_index < len(obs_timeseries):
        if obs_timeseries[obs_index][0] == newly_extracted_timeseries[new_ts_index][0]:
            if obs_timeseries[obs_index][1] == -99999:
                obs_timeseries[obs_index][1] == newly_extracted_timeseries[new_ts_index][1]
            obs_index += 1
            new_ts_index += 1
        elif obs_timeseries[obs_index][0] < newly_extracted_timeseries[new_ts_index][0]:
            obs_index += 1
        else:
            new_ts_index += 1

    return obs_timeseries


def convert_15_min_ts_to_5_mins_ts(newly_extracted_timeseries, expected_start=None):

    processed_ts = []

    current_timestamp = newly_extracted_timeseries[0][0]
    if expected_start is not None:
        current_timestamp = expected_start

    extracted_ts_index = 0

    while extracted_ts_index < len(newly_extracted_timeseries):
        if current_timestamp > newly_extracted_timeseries[extracted_ts_index][0]:
            extracted_ts_index += 1
        elif (newly_extracted_timeseries[extracted_ts_index][0] - current_timestamp) < timedelta(minutes=15):
            processed_ts.append([current_timestamp, newly_extracted_timeseries[extracted_ts_index][1]/3])
            current_timestamp = current_timestamp + timedelta(minutes=5)
        else:
            processed_ts.append([current_timestamp, -99999])
            current_timestamp = current_timestamp + timedelta(minutes=5)

    return processed_ts


def append_value_for_timestamp(existing_ts, new_ts):

    """
    Appending timeseries assuming start and end of both timeseries are same
    :param existing_ts: list of [timestamp, value1, value2, .., valuen] lists (note: this might include several values)
    :param new_ts: list of [timestamp, VALUE] list (note: this include single value)
    :return: list of [timestamp, value1, value2, .., valuen, VALUE]
    """

    appended_ts =[]

    for i in range(len(existing_ts)):
        appended_ts.append(existing_ts[i])
        appended_ts[i].append(new_ts[i][1])

    return appended_ts


def average_timeseries(timeseries):
    """
    Give the timeseries with avg value for given timeseries containing several values per one timestamp
    :param timeseries:
    :return:
    """
    avg_timeseries = []

    if len(timeseries[0]) <= 2:
        return timeseries
    else:
        for i in range(len(timeseries)):
            count = len(timeseries[i])-1
            print((sum(timeseries[i][1:])/count))
            avg_timeseries.append([timeseries[i][0], "%.3f" % (sum(timeseries[i][1:])/count)])

    return avg_timeseries


# timeseries1 = [["2019-05-06 00:00:00", 0.025, 0.369, 0.12, 2.36], ["2019-05-06 00:05:00", 0.025, 0.369, 0.12, 2.36],
#               ["2019-05-06 00:10:00", 0.025, 0.025, 0.025, 0.025]]
#
# timeseries = [["2019-05-06 00:00:00", 0], ["2019-05-06 00:05:00", 0.025],
#               ["2019-05-06 00:10:00", 0.025]]
#
# appended_Ts = append_value_for_timestamp(existing_ts=timeseries1, new_ts=timeseries)
#
# print(appended_Ts)
#
# print(average_timeseries(appended_Ts))
