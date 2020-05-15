import traceback
from datetime import datetime, timedelta


class DelTimeseries:
    def __init__(self, pool, data_table, run_table):
        self.pool = pool
        self.data_table = data_table
        self.run_table = run_table

    def delete_timeseries(self, id_, start=None, end=None):
        """
        Delete specific timeseries identified by hash id
        :param id_: hash id
        :param start: start time inclusive
        :param end: end time inclusive
        :return:
        """

        connection = self.pool.connection()
        data_table = self.data_table

        pre_sql_statement = "DELETE FROM `curw_sim`.`" + data_table + "` WHERE "

        condition_list = []
        variable_list = []

        condition_list.append("`id`= %s")
        variable_list.append(id_)

        if start is not None:
            condition_list.append("`time`>=%s")
            variable_list.append(start)
        if end is not None:
            condition_list.append("`time`<=%s")
            variable_list.append(end)

        conditions = " AND ".join(condition_list)

        sql_statement = pre_sql_statement + conditions + ";"

        print(sql_statement)

        try:
            with connection.cursor() as cursor:
                row_count = cursor.execute(sql_statement, tuple(variable_list))

            connection.commit()
            return row_count
        except Exception as exception:
            connection.rollback()
            error_message = "Deletion of timeseries with hash id {} failed".format(id_)
            print(error_message)
            traceback.print_exc()
        finally:
            if connection is not None:
                connection.close()

    def delete_all_by_hash_id(self, id_):
        """
        Delete all timeseries with same hash id (same meta data)
        :param id_: hash id
        :return:
        """

        connection = self.pool.connection()
        run_table = self.run_table

        try:

            with connection.cursor() as cursor:
                sql_statement = "DELETE FROM `curw_sim`.`" + run_table + "` WHERE `id`= %s ;"
                row_count = cursor.execute(sql_statement, id_)

            connection.commit()
            return row_count
        except Exception as exception:
            connection.rollback()
            error_message = "Deletion of timeseries with hash id {} failed".format(id_)
            print(error_message)
            traceback.print_exc()
        finally:
            if connection is not None:
                connection.close()

    def bulk_delete_timeseries(self, ids, start=None, end=None):
        """
        Delete specific timeseries identified by hash id
        :param id_: hash id list
        :param start: start time inclusive
        :param end: end time inclusive
        :return:
        """

        connection = self.pool.connection()
        data_table = self.data_table

        pre_sql_statement = "DELETE FROM `curw_sim`.`" + data_table + "` WHERE "

        condition_list = []
        variable_list = []

        condition_list.append("`id`= %s")

        if start is not None:
            condition_list.append("`time`>=%s")
            variable_list.append(start)
        if end is not None:
            condition_list.append("`time`<=%s")
            variable_list.append(end)

        conditions = " AND ".join(condition_list)

        sql_statement = pre_sql_statement + conditions + ";"

        print(sql_statement)

        processed_variable_list = [[id] for id in ids]

        for i in range(len(ids)):
            processed_variable_list[i].extend(variable_list)

        ilength = (len(ids) // 100) * 100
        count = 0
        while count < ilength:
            try:
                with connection.cursor() as cursor:
                    row_count = cursor.executemany(sql_statement, processed_variable_list[count: count+100])
                connection.commit()
                print(count + len(processed_variable_list[count: count+100]))
                count += 100
            except Exception as exception:
                connection.rollback()
                error_message = "Deletion of timeseries failed"
                print(error_message)
                traceback.print_exc()

        try:
            with connection.cursor() as cursor:
                row_count = cursor.executemany(sql_statement, processed_variable_list[count: len(ids)])
            connection.commit()
            print(count + len(processed_variable_list[count: len(ids)]))
        except Exception as exception:
            connection.rollback()
            error_message = "Deletion of timeseries failed"
            print(error_message)
            traceback.print_exc()
        finally:
            if connection is not None:
                connection.close()

    def bulk_delete_all_by_hash_id(self, ids):
        """
        Delete all timeseries with same hash id (same meta data)
        :param id_: hash id list
        :return:
        """

        connection = self.pool.connection()
        run_table = self.run_table

        ilength = (len(ids) // 100) * 100
        count = 0
        while count < ilength:
            try:
                with connection.cursor() as cursor:
                    sql_statement = "DELETE FROM `curw_sim`.`" + run_table + "` WHERE `id`= %s ;"
                    row_count = cursor.executemany(sql_statement, ids[count: count+100])
                connection.commit()
                print(count + len(ids[count: count+100]))
                count += 100
            except Exception as exception:
                connection.rollback()
                error_message = "Deletion of run entries along with all relating timeseries failed"
                print(error_message)
                traceback.print_exc()

        try:
            with connection.cursor() as cursor:
                sql_statement = "DELETE FROM `curw_sim`.`" + run_table + "` WHERE `id`= %s ;"
                row_count = cursor.executemany(sql_statement, ids[count: len(ids)])
            connection.commit()
            print(count + len(ids[count: len(ids)]))
        except Exception as exception:
            connection.rollback()
            error_message = "Deletion of run entries along with all relating timeseries failed"
            print(error_message)
            traceback.print_exc()
        finally:
            if connection is not None:
                connection.close()


def get_curw_sim_hash_ids(pool, run_table, model=None, method=None, obs_end_start=None, obs_end_end=None, grid_id=None):

    """
    Retrieve specific set of hash ids from curw_sim run tables
    :param pool: database connection pool
    :param model: target model
    :param method: interpolation method
    :param obs_end_start: start of the considering obs_end range, inclusive
    :param obs_end_end: end of the considering obs_end range, inclusive
    :param grid_id: grid id pattern, escape character $
    :return:
    """

    pre_sql_statement = "SELECT `id` FROM `" + run_table + "` WHERE "

    condition_list = []
    variable_list = []

    score = 0

    if model is not None:
        condition_list.append("`model`=%s")
        variable_list.append(model)
        score +=1
    if method is not None:
        condition_list.append("`method`=%s")
        variable_list.append(method)
        score +=1
    if obs_end_start is not None:
        condition_list.append("`obs_end`>=%s")
        variable_list.append(obs_end_start)
        score +=1
    if obs_end_end is not None:
        condition_list.append("`obs_end`<=%s")
        variable_list.append(obs_end_end)
        score +=1
    if grid_id is not None:
        condition_list.append("`grid_id` like %s ESCAPE '$'")
        variable_list.append(grid_id)
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
    except Exception:
        traceback.print_exc()
    finally:
        if connection is not None:
            connection.close()