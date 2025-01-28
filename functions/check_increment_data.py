
import sys
import traceback
import pandas as pd
from datetime import datetime
from config import ibbi_config
from functions import scrape_claim_details_and_download_pdf, log, send_mail, removal_date


def get_deleted_data_as_objects(deleted_data_df):
    """Get only those deleted records that don't have a removal_date yet"""
    

    connection = ibbi_config.db_connection()
    cursor = connection.cursor()

    deleted_records = []
    
    for _, row in deleted_data_df.iterrows():
        # Check if record already has removal_date
        check_query = """
            SELECT removal_date 
            FROM ibbi_claims_process 
            WHERE corporate_debtor = %s 
            AND name_of_irp_rp_liquidator = %s 
            AND under_process = %s 
            AND latest_claim_as_on_date = %s 
            AND view_details = %s
        """
        check_values = (
            row['corporate_debtor'],
            row['name_of_irp_rp_liquidator'],
            row['under_process'],
            row['latest_claim_as_on_date'],
            row['view_details']
        )
        
        cursor.execute(check_query, check_values)
        result = cursor.fetchone()
        
        # Only add to deleted_records if removal_date is not set
        if result and result[0] is None:
            record = {
                'corporate_debtor': row['corporate_debtor'],
                'name_of_irp_rp_liquidator': row['name_of_irp_rp_liquidator'],
                'under_process': row['under_process'],
                'latest_claim_as_on_date': row['latest_claim_as_on_date'],
                'view_details': row['view_details']
            }
            deleted_records.append(record)
    
    cursor.close()
    return deleted_records


def check_increment_data(excel_path):
    print("check_increment_data function is called")

    try:
       
        with ibbi_config.db_connection() as connection:
            query = "SELECT * FROM ibbi_claims_process"
            db_df = pd.read_sql(query, con=connection)
 
        xl_df = pd.read_excel(excel_path)
        excel_df = xl_df.drop_duplicates() # remove duplicates

        database_df = db_df.drop(columns=['sr_no', 'source_name', 'header_information', 'claims_details', 'pdf_links','pdf_names', 'pdf_relative_paths', 'updated_date','removal_date', 'date_scraped'])
        print("database columns", db_df.columns)
        print("excel columns", excel_df.columns)

        # Add diagnostic prints
        print("\nafter dropping columns:")
        print("Database DataFrame shape:", database_df.shape)
        print("Excel DataFrame shape:", xl_df.shape)
        print("\nSample from database:")
        print(database_df.head(1).to_string())
        print("\nSample from Excel:")
        print(xl_df.head(1).to_string())
        
        
        # Check data types of columns
        print("\nDatabase column types:")
        print(database_df.dtypes)
        print("\nExcel column types:")
        print(excel_df.dtypes)
        
        # Check for whitespace or special characters
        print("\nSample values comparison:")
        for col in database_df.columns:
            print(f"\nColumn: {col}")
            print("Database first value:", repr(database_df[col].iloc[0]))
            print("Excel first value:", repr(excel_df[col].iloc[0]))

        database_df.columns = database_df.columns.str.strip().str.lower()
        excel_df.columns = excel_df.columns.str.strip().str.lower()

        new_data = excel_df.merge(database_df, how="left", indicator=True).query('_merge == "left_only"').drop(columns=['_merge'])
        deleted_data = database_df.merge(excel_df, how="left", indicator=True).query('_merge == "left_only"').drop(columns=['_merge'])
        new_data.to_excel("ibbi_new_data.xlsx", index=False)
        deleted_data.to_excel("ibbi_deleted.xlsx", index=False)


        # Print the missing rows in database and Excel
        print("Rows in Excel but not in database (New Data):")
        print(new_data)

        print("\nRows in database but not in Excel (Deleted Data):")
        print(deleted_data)

        
        # Get deleted records and update removal_date
        if not deleted_data.empty:

            # Get only records that don't have removal_date
            deleted_records = get_deleted_data_as_objects(deleted_data)
            print(f"Total deleted records found: {len(deleted_data)}")
            print(f"New deleted records (without removal date): {len(deleted_records)}")

            update_new_deleted_count, skipped_count = removal_date.update_removal_date(deleted_records)

        ibbi_config.no_data_avaliable = len(new_data)
        ibbi_config.no_data_scraped = len(new_data)
        ibbi_config.deleted_source_count = len(deleted_data)

        ibbi_config.deleted_source_count = update_new_deleted_count
        ibbi_config.deleted_source = deleted_records

        print( "missing rows in database", len(new_data))
        print("missing rows in Excel", len(deleted_data))
       
        if update_new_deleted_count > 0 and len(new_data) == 0:
            ibbi_config.log_list[1] = "Success"
            
            ibbi_config.log_list[3] = "Some data are deleted in the website"
            log.insert_log_into_table(ibbi_config.log_list)
            # print("log table====", ibbi_config.log_list)
            ibbi_config.log_list = [None] * 4
            sys.exit()

        elif len(new_data) == 0:
            ibbi_config.log_list[1] = "Success"
           
            ibbi_config.log_list[3] = "no new data"
            log.insert_log_into_table(ibbi_config.log_list)
            print("log table====", ibbi_config.log_list)
            ibbi_config.log_list = [None] * 4
            sys.exit()

        current_date = datetime.now().strftime("%Y-%m-%d")
        increment_file_name = f"incremental_excel_sheet_{current_date}.xlsx"
        increment_data_excel_path = fr"C:\Users\Premkumar.8265\Desktop\ibbi_claims_process\data\incremental_excel_sheet\{increment_file_name}"
         
        # missing_rows_in_db.to_excel(increment_data_excel_path, index=False)
        pd.DataFrame(new_data).to_excel(increment_data_excel_path, index=False)
       
        # scrape_claim_details_and_download_pdf.scrape_claim_details_and_download_pdf(increment_data_excel_path)
 
    except Exception as e:
        traceback.print_exc()
        ibbi_config.log_list[1] = "Failure"
 
        ibbi_config.log_list[2] = "error in checking in incremental part"
        log.insert_log_into_table(ibbi_config.log_list)
        print("checking incremental part error:", ibbi_config.log_list)
        send_mail.send_email("ibbi section checking incremental part error", e)
        ibbi_config.log_list = [None] * 4
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f"Error occurred at line {exc_tb.tb_lineno}:")
        print(f"Exception Type: {exc_type}")
        print(f"Exception Object: {exc_obj}")
        print(f"Traceback: {exc_tb}")
        sys.exit()
