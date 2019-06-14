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
