import pandas as pd
import sys
import traceback
import pandas as pd
from datetime import datetime
from config import ibbi_config
from functions import scrape_claim_details_and_download_pdf, log, send_mail, removal_date



def standardize_dataframes(df1, df2):
    """
    Standardize data types in both dataframes for comparison
    """
    # Create copies to avoid modifying original dataframes
    df1_clean = df1.copy()
    df2_clean = df2.copy()
    
    # Convert date columns to datetime with consistent format
    date_columns = ['latest_claim_as_on_date']
    for col in date_columns:
        df1_clean[col] = pd.to_datetime(df1_clean[col], errors='coerce')
        df2_clean[col] = pd.to_datetime(df2_clean[col], errors='coerce')
    
    # Convert string columns to consistent format
    string_columns = [
            'corporate_debtor',
            'name_of_irp_rp_liquidator',
            'under_process',
            'view_details'
    ]
    
    for col in string_columns:
        df1_clean[col] = df1_clean[col].astype(str).str.strip()
        df2_clean[col] = df2_clean[col].astype(str).str.strip()
    
    return df1_clean, df2_clean



def check_increment_data(excel_path):
    print("check_increment_data function is called")

    try:
       
        with ibbi_config.db_connection() as connection:
            query = "SELECT * FROM ibbi_claims_process"
            database_df = pd.read_sql(query, con=connection)
        
        database_df.to_excel('database_records.xlsx', index=False)

 
        xl_df = pd.read_excel(excel_path)
        excel_df = xl_df.drop_duplicates() # remove duplicates
        
        
       # Standardize both dataframes
        df_sample_clean, df_demo_clean = standardize_dataframes(excel_df, database_df)
        
        # Perform merge on all columns
        merge_columns = [
            'corporate_debtor',
            'name_of_irp_rp_liquidator',
            'under_process',
            'latest_claim_as_on_date',
            'view_details'
        ]
        
        # Perform the merge
        comparison = df_sample_clean.merge(
            df_demo_clean,
            on=merge_columns,
            how='left',
            indicator=True
        )
        
        # Filter incremental rows (only in sample.xlsx)
        incremental_rows = comparison[comparison['_merge'] == 'left_only']
        
        # Drop the merge indicator column
        incremental_rows = incremental_rows.drop(columns=['_merge'])
        
        # Format dates back to readable format for output
        incremental_rows['latest_claim_as_on_date'] = incremental_rows['latest_claim_as_on_date'].dt.strftime('%d-%m-%Y')
        
        # Save results
        incremental_rows.to_excel('incremental_rows_new.xlsx', index=False)
        
        # Print summary
        print(f"Total rows in sample.xlsx: {len(excel_df)}")
        print(f"Total rows in ibbi_claims_process.xlsx: {len(database_df)}")
        print(f"Number of incremental rows found: {len(incremental_rows)}")




        # Print the missing rows in database and Excel
        print("Rows in Excel but not in database (New Data):")
        # print(new_data)

        print("\nRows in database but not in Excel (Deleted Data):")
        # print(deleted_data)

        
        # # Get deleted records and update removal_date
        # if not deleted_data:

        #     # Get only records that don't have removal_date
        #     # deleted_records = get_deleted_data_as_objects(deleted_data)
        #     print(f"Total deleted records found: {len(deleted_data)}")
        #     # print(f"New deleted records (without removal date): {len(deleted_records)}")

        #     # updated_count, skipped_count = removal_date.update_removal_date(deleted_records)

        # ibbi_config.no_data_avaliable = len(new_data)
        # ibbi_config.no_data_scraped = len(new_data)
        # ibbi_config.deleted_source_count = len(deleted_data)

        # # ibbi_config.deleted_source_count = updated_count
        # # ibbi_config.deleted_source = deleted_records

        # print( "missing rows in database", len(new_data))
        # print("missing rows in Excel", len(deleted_data))
       
        # if len(deleted_data) > 0 and len(new_data) == 0:
        #     ibbi_config.log_list[1] = "Success"
            
        #     ibbi_config.log_list[3] = "Some data are deleted in the website"
        #     log.insert_log_into_table(ibbi_config.log_list)
        #     # print("log table====", ibbi_config.log_list)
        #     ibbi_config.log_list = [None] * 4
        #     sys.exit()

        # elif len(new_data) == 0:
        #     ibbi_config.log_list[1] = "Success"
           
        #     ibbi_config.log_list[3] = "no new data"
        #     log.insert_log_into_table(ibbi_config.log_list)
        #     print("log table====", ibbi_config.log_list)
        #     ibbi_config.log_list = [None] * 4
        #     sys.exit()

        # current_date = datetime.now().strftime("%Y-%m-%d")
        # increment_file_name = f"incremental_excel_sheet_{current_date}.xlsx"
        # increment_data_excel_path = fr"C:\Users\Premkumar.8265\Desktop\ibbi_claims_process\data\incremental_excel_sheet\{increment_file_name}"
         
        # # missing_rows_in_db.to_excel(increment_data_excel_path, index=False)
        # pd.DataFrame(new_data).to_excel(increment_data_excel_path, index=False)
       
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


# first_excel_sheet_name =f"first_excel_sheet_{ibbi_config.current_date}.xlsx"
# first_exceL_sheet_path = fr"C:\Users\Premkumar.8265\Desktop\ibbi_claims_process\data\first_excel_sheet\{first_excel_sheet_name}"

# check_increment_data(first_exceL_sheet_path)