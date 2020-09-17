import traceback
from datetime import datetime, timedelta
import math

from db_adapter.logger import logger


def round_up_datetime_to_nearest_x_minutes(datetime_value, mins):

    base_time = datetime_value.replace(minute=0, second=0, microsecond=0)

    multiplier = math.ceil(datetime_value.minute / mins)

    return base_time + timedelta(minutes=mins*multiplier)


def process_continuous_ts(original_ts, expected_start, filling_value, timestep):
    """

    :param original_ts: original timeseries to be processed : list of [time, value] pairs
    :param expected_start: expected start of the output timeseries
    :param filling_value: value that should be assigned for missing data
    :param timestep: expected time step in minutes of the output timeseries
    :return: timeseries as list of [time, value] pairs
    """

    processed_ts = []

    current_timestamp = expected_start
    original_ts_index = 0

    while original_ts_index < len(original_ts):
        if current_timestamp == original_ts[original_ts_index][0]:
            processed_ts.append(original_ts[original_ts_index])
            original_ts_index +=1
            current_timestamp = current_timestamp + timedelta(minutes=timestep)
        elif current_timestamp < original_ts[original_ts_index][0]:
            processed_ts.append([current_timestamp, filling_value])
            current_timestamp = current_timestamp + timedelta(minutes=timestep)
        else:
            original_ts_index +=1

    return processed_ts


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


def process_15_min_ts(newly_extracted_timeseries, expected_start):

    processed_ts = []

    current_timestamp = expected_start
    extracted_ts_index = 0

    while extracted_ts_index < len(newly_extracted_timeseries):
        if current_timestamp == newly_extracted_timeseries[extracted_ts_index][0]:
            processed_ts.append(newly_extracted_timeseries[extracted_ts_index])
            extracted_ts_index +=1
            current_timestamp = current_timestamp + timedelta(minutes=15)
        elif current_timestamp < newly_extracted_timeseries[extracted_ts_index][0]:
            processed_ts.append([current_timestamp, -99999])
            current_timestamp = current_timestamp + timedelta(minutes=15)
        else:
            extracted_ts_index +=1

    return processed_ts


def fill_missing_values(newly_extracted_timeseries, OBS_TS):

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


def join_ts(TS1, TS2):
    """
    Joing time, value pairs of 2 timeseries, if both timeseries include same timestamp
    :return:
    """

    if TS1[-1][0] > TS2[-1][0]:
        ts1 = TS1
        ts2 = TS2
    else:
        ts1 = TS2
        ts2 = TS1

    output_ts=[]

    ts1_index = 0
    ts2_index = 0

    # ts1 last entry has the largest timestamp
    while ts2_index < len(ts2):
        if ts1[ts1_index][0] == ts2[ts2_index][0]:
            output_ts.append(ts1[ts1_index])
            output_ts[-1].append(ts2[ts2_index][1])
            ts1_index +=1
            ts2_index +=1
        elif ts1[ts1_index][0] < ts2[ts2_index][0]:
            output_ts.append(ts1[ts1_index])
            ts1_index += 1
        elif ts1[ts1_index][0] > ts2[ts2_index][0]:
            output_ts.append(ts2[ts2_index])
            ts2_index +=1

    output_ts.extend(ts1[ts1_index:])

    return output_ts


def append_ts(original_ts, new_ts):
    """
    append new ts to original ts
    :param original_ts:
    :param new_ts:
    :return:
    """

    appended_ts = []

    original_ts_index = 0
    new_ts_index = 0

    while original_ts_index < len(original_ts):

        if new_ts_index < len(new_ts):

            if original_ts[original_ts_index][0] == new_ts[new_ts_index][0]:
                appended_ts.append(original_ts[original_ts_index])
                appended_ts[original_ts_index].append(new_ts[new_ts_index][1])
                original_ts_index += 1
                new_ts_index += 1
            elif original_ts[original_ts_index][0] < new_ts[new_ts_index][0]:
                appended_ts.append(original_ts[original_ts_index])
                original_ts_index += 1
            elif original_ts[original_ts_index][0] > new_ts[new_ts_index][0]:
                appended_ts.append(new_ts[new_ts_index])
                new_ts_index += 1
        else:
            if original_ts_index < len(original_ts):
                appended_ts.extend(original_ts[original_ts_index:])
            break

    if new_ts_index < len(new_ts):
        appended_ts.extend(new_ts[new_ts_index:])

    return appended_ts


def append_value_for_timestamp(existing_ts, new_ts):

    """
    Appending timeseries assuming start and end of both timeseries are same
    :param existing_ts: list of [timestamp, value1, value2, .., valuen] lists (note: this might include several values)
    :param new_ts: list of [timestamp, VALUE] list (note: this include single value)
    :return: list of [timestamp, value1, value2, .., valuen, VALUE]
    """

    appended_ts =[]

    if len(existing_ts) == len(new_ts) and existing_ts[0][0] == new_ts[0][0]:
        for i in range(len(existing_ts)):
            appended_ts.append(existing_ts[i])
            appended_ts[i].append(new_ts[i][1])
    else:
        return existing_ts

    return appended_ts


def average_timeseries(timeseries):
    """
    Give the timeseries with avg value for given timeseries containing several values per one timestamp
    :param timeseries:
    :return:
    """
    avg_timeseries = []

    for i in range(len(timeseries)):
        if len(timeseries[i])>1:
            count = len(timeseries[i])-1
            avg_timeseries.append([timeseries[i][0], '%.3f' % (sum(timeseries[i][1:])/count)])

    return avg_timeseries


def summed_timeseries(timeseries):
    """
    Give sum of value series against timestamps for given timeseries containing several values per one timestamp
    :param timeseries:
    :return:
    """
    sum_timeseries = []

    for i in range(len(timeseries)):
        if len(timeseries[i])>1:
            sum_timeseries.append([timeseries[i][0], '%.3f' % (sum(timeseries[i][1:]))])

    return sum_timeseries


##########################
# Extract obs timeseries #
##########################
def extract_obs_rain_5_min_ts(connection, id, start_time, end_time=None):
    """
    Extract obs station timeseries (15 min intervals)
    :param connection: connection to curw database
    :param stations_dict: dictionary with station_id as keys and run_ids as values
    :param start_time: start of timeseries
    :return:
    """

    timeseries = []

    try:
        # Extract per 5 min observed timeseries
        with connection.cursor() as cursor1:
            if end_time is None:
                sql_statement = "select `time`, `value`  from data where `id`=%s and `time` >= %s ;"
                rows = cursor1.execute(sql_statement, (id, start_time))
            else:
                sql_statement = "select `time`, `value`  from data where `id`=%s and `time` >= %s and `time` <= %s;"
                rows = cursor1.execute(sql_statement, (id, start_time, end_time))

            if rows > 0:
                results = cursor1.fetchall()
                for result in results:
                    timeseries.append([result.get('time'), result.get('value')])

        return timeseries

    except Exception as ex:
        traceback.print_exc()
        logger.error("Exception occurred while retrieving observed rainfall 5 min timeseries from database")


def extract_obs_rain_15_min_ts(connection, id, start_time, end_time=None):
    """
    Extract obs station timeseries (15 min intervals)
    :param connection: connection to curw database
    :param stations_dict: dictionary with station_id as keys and run_ids as values
    :param start_time: start of timeseries
    :return:
    """

    timeseries = []

    try:
        # Extract per 15 min observed timeseries
        with connection.cursor() as cursor1:
            # sql_statement = "select max(`time`) as time, sum(`value`) as value from `data` where `id`=%s and `time` >= %s " \
            #                 "group by floor((HOUR(TIMEDIFF(time, %s))*60+MINUTE(TIMEDIFF(time, %s))-1)/15);"

            if end_time is None:
                sql_statement = "select max(`time`) as time, sum(`value`) as value from `data` where `id`=%s and `time` >= %s " \
                                "group by floor(((to_seconds(time)/60)-1)/15);"
                rows = cursor1.execute(sql_statement, (id, start_time))
            else:
                sql_statement = "select max(`time`) as time, sum(`value`) as value from `data` where `id`=%s and `time` >= %s and `time` <= %s " \
                                "group by floor(((to_seconds(time)/60)-1)/15);"
                rows = cursor1.execute(sql_statement, (id, start_time, end_time))

            if rows > 0:
                results = cursor1.fetchall()
                for result in results:
                    timeseries.append([round_up_datetime_to_nearest_x_minutes(result.get('time'), 15),
                                       result.get('value')])

        return timeseries

    except Exception as ex:
        traceback.print_exc()
        logger.error("Exception occurred while retrieving observed rainfall 15 min timeseries from database")


def extract_obs_rain_custom_min_intervals(connection, id, time_step, start_time, end_time=None):
    """
    Extract obs station timeseries (custom min intervals)
    :param connection: connection to curw database
    :param start_time: start of timeseries
    :param id: hash id of the timeseries
    :param time_step: frequency of the timeseries
    :param end_time: end of the timeseries
    :return:
    """

    timeseries = []

    try:
        # Extract per 15 min observed timeseries
        with connection.cursor() as cursor1:
            # sql_statement = "select max(`time`) as time, sum(`value`) as value from `data` where `id`=%s and `time` >= %s " \
            #                 "group by floor((HOUR(TIMEDIFF(time, %s))*60+MINUTE(TIMEDIFF(time, %s))-1)/15);"

            if end_time is None:
                sql_statement = "select max(`time`) as time, sum(`value`) as value from `data` where `id`=%s and `time` >= %s " \
                                "group by floor(((to_seconds(time)/60)-1)/" + time_step + ");"
                rows = cursor1.execute(sql_statement, (id, start_time))
            else:
                sql_statement = "select max(`time`) as time, sum(`value`) as value from `data` where `id`=%s and `time` >= %s and `time` <= %s " \
                                "group by floor(((to_seconds(time)/60)-1)/" + time_step + ");"
                rows = cursor1.execute(sql_statement, (id, start_time, end_time))

            if rows > 0:
                results = cursor1.fetchall()
                for result in results:
                    timeseries.append([round_up_datetime_to_nearest_x_minutes(result.get('time'), 60),
                                       result.get('value')])

        return timeseries

    except Exception as ex:
        traceback.print_exc()
        logger.error("Exception occurred while retrieving observed rainfall 15 min timeseries from database")


########
# test #
########
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
