import traceback

from db_adapter.logger import logger


def get_curw_fcst_hash_ids(pool, sim_tag=None, source_id=None, variable_id=None, unit_id=None, station_id=None,
                           start=None, end=None):

    pre_sql_statement = "SELECT `id` FROM `run` WHERE "

    condition_list = []
    variable_list = []

    score = 0

    if sim_tag is not None:
        condition_list.append("`sim_tag`=%s")
        variable_list.append(sim_tag)
        score +=1
    if source_id is not None:
        condition_list.append("`source`=%s")
        variable_list.append(source_id)
        score +=1
    if variable_id is not None:
        condition_list.append("`variable`=%s")
        variable_list.append(variable_id)
        score +=1
    if unit_id is not None:
        condition_list.append("`unit`=%s")
        variable_list.append(unit_id)
        score +=1
    if station_id is not None:
        condition_list.append("`station`=%s")
        variable_list.append(station_id)
        score +=1
    if start is not None:
        condition_list.append("`start_date`=%s")
        variable_list.append(start)
        score +=1
    if end is not None:
        condition_list.append("`end_date`=%s")
        variable_list.append(end)
        score +=1

    if score == 0:
        return None

    conditions = " AND ".join(condition_list)

    sql_statement = pre_sql_statement + conditions + ";"

    print(sql_statement)

    ids = []
    connection = pool.connection()
    try:
        with connection.cursor() as cursor:
            row_count = cursor.execute(sql_statement, tuple(variable_list))
            if row_count > 0:
                results = cursor.fetchall()
                for result in results:
                    ids.append(result.get('id'))
        return ids
    except Exception as exception:
        error_message = "Exception occurred while retrieving hash ids. ::: ".format(' '.join(condition_list))
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()


def get_distinct_fgts_for_given_id(pool, id_):

    sql_statement = "select distinct `fgt` from data where id=%s ;"

    fgts = []
    connection = pool.connection()
    try:
        with connection.cursor() as cursor:
            row_count = cursor.execute(sql_statement, id_)
            if row_count > 0:
                results = cursor.fetchall()
                for result in results:
                    fgts.append(result.get('fgt'))
        return fgts
    except Exception as exception:
        error_message = "Exception occurred while retrieving fgts for hash id {}. ".format(id_)
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()
