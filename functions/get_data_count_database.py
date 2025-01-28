import sys
from config import ibbi_config

connection = ibbi_config.db_connection()
cursor = connection.cursor()


def get_data_count_database():
    
    try:
        print("get_data_count_database function is called")
        cursor.execute("SELECT COUNT(*) FROM ibbi_claims_process;")
        result = cursor.fetchone()
        print("Result from database query:", result)
        if result:
            return result[0]
        else:
            raise ValueError("Query did not return any results")
    except Exception as e:
        print("Error in get_data_count_database :", e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f"Error occurred at line {exc_tb.tb_lineno}:")
        print(f"Exception Type: {exc_type}")
        print(f"Exception Object: {exc_obj}")
        print(f"Traceback: {exc_tb}")