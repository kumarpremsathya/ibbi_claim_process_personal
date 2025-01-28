from config import ibbi_config
from functions import get_data_count_database
import sys
import json
from datetime import datetime


def insert_log_into_table(log_list):

    print("insert_log_into_table function is called")
    removal_date = datetime.now().strftime('%Y-%m-%d')
    connection = ibbi_config.db_connection()
    cursor = connection.cursor()

    try:
        query = """
            INSERT INTO ibbi_log (source_name, script_status, data_available, data_scraped, total_record_count, failure_reason, comments, source_status, newly_added_count, deleted_source, deleted_source_count, removal_date)
            VALUES (%(source_name)s, %(script_status)s, %(data_available)s, %(data_scraped)s, %(total_record_count)s, %(failure_reason)s, %(comments)s, %(source_status)s, %(newly_added_count)s, %(deleted_source)s, %(deleted_source_count)s, %(removal_date)s)
        """
        values = {
            'source_name': ibbi_config.source_name,
            'script_status': log_list[1] if log_list[1] else None,
            'data_available': ibbi_config.no_data_avaliable if ibbi_config.no_data_avaliable else None,
            'data_scraped': ibbi_config.no_data_scraped if ibbi_config.no_data_scraped else None,
            'total_record_count': get_data_count_database.get_data_count_database(),
            'failure_reason': log_list[2] if log_list[2] else None,
            'comments': log_list[3] if log_list[3] else None,
            'source_status': ibbi_config.source_status,
            'newly_added_count': ibbi_config.newly_added_count,
            # 'updated_source_count': ibbi_config.updated_count,
            'deleted_source':json.dumps(ibbi_config.deleted_source) if ibbi_config.deleted_source else None,
            'deleted_source_count': ibbi_config.deleted_source_count,
            'removal_date':removal_date if ibbi_config.deleted_source else None,


        }

        cursor.execute(query, values)
        print("log list", values)
        connection.commit()
        connection.close()

    except Exception as e:
        print("Error in insert_log_into_table function:", e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f"Error occurred at line {exc_tb.tb_lineno}:")
        print(f"Exception Type: {exc_type}")
        print(f"Exception Object: {exc_obj}")
        print(f"Traceback: {exc_tb}")
           