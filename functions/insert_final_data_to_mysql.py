import sys
import traceback
import pandas as pd
import mysql.connector
from config import ibbi_config
from functions import  send_mail, log



def insert_final_data_to_mysql(data_rows):

    print("insert_excel_data_to_mysql function is called")

    connection = ibbi_config.db_connection()
    cursor = connection.cursor()

    df = pd.DataFrame(data_rows)
    
    count = 0
    try:
        df = pd.DataFrame(data_rows) if isinstance(data_rows, list) else pd.DataFrame([data_rows])
        for index, data_row in df.iterrows():
            query = """
            INSERT INTO ibbi_claims_process (
                source_name, corporate_debtor, name_of_irp_rp_liquidator, 
                under_process, latest_claim_as_on_date, view_details, 
                header_information, claims_details, pdf_links, 
                pdf_names, pdf_relative_paths
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """

            values = (
                'ibbi_claims_process',  # source_name
                data_row['corporate_debtor'],
                data_row['name_of_irp_rp_liquidator'],
                data_row['under_process'],
                data_row['latest_claim_as_on_date'],
                data_row['view_details'],
                data_row.get('Header_Information', '{}'),
                data_row.get('Claims_Details', '{}'),
                data_row.get('PDF_Links', '[]'),
                data_row.get('PDF_Names', '[]'),
                data_row.get('Relative_Paths', '[]')
            )
            
            cursor.execute(query, values)
            count += 1
            print(f"Successfully inserted data for {data_row['corporate_debtor']}")
            print(f"Current count of inserted rows: {count}")
                
        connection.commit()  # Commit after all insertions
        print(f"Total rows inserted: {count}")

        ibbi_config.log_list[1] = "Success"
        ibbi_config.no_data_scraped = count
        ibbi_config.newly_added_count = count
        ibbi_config.log_list[3] = f"{ibbi_config.newly_added_count} new data"
        
        print("log table====", ibbi_config.log_list)
        log.insert_log_into_table(ibbi_config.log_list)
        ibbi_config.log_list = [None] * 4
        print("Data has been successfully inserted into the MySQL database.")
        sys.exit()
        
    except Exception as e:
        connection.rollback()
        ibbi_config.log_list[1] = "Failure"
        ibbi_config.log_list[2] = "error in insert part"
        print("log table====", ibbi_config.log_list)
        log.insert_log_into_table(ibbi_config.log_list)
        
        ibbi_config.log_list = [None] * 4
        traceback.print_exc()
        send_mail.send_email("ibbi section Data inserted into the database error", e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f"Error occurred at line {exc_tb.tb_lineno}:")
        print(f"Exception Type: {exc_type}")
        print(f"Exception Object: {exc_obj}")
        print(f"Traceback: {exc_tb}")
         
        sys.exit("script error")

       
    finally:
        if cursor:
            cursor.close()








