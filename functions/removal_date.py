import sys
import traceback
import pandas as pd
from datetime import datetime
from config import ibbi_config


def update_removal_date(deleted_data_df):
    print("update removal dates function is called")
   
    connection = ibbi_config.db_connection()
    cursor = connection.cursor()
    try:
        current_date = datetime.now().strftime("%Y-%m-%d")
        # current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
         
        update_new_deleted_count = 0
        
        for _, row in deleted_data_df.iterrows():
            print(f"\nProcessing deleted record: {row['corporate_debtor']}")
            
            # Direct update without checking - if record exists and removal_date is null, it will be updated
            update_query = """
                UPDATE ibbi_claims_process 
                SET removal_date = %s 
                WHERE corporate_debtor = %s 
                AND name_of_irp_rp_liquidator = %s 
                AND under_process = %s 
                AND latest_claim_as_on_date = %s 
                AND view_details = %s
                AND removal_date IS NULL
            """
            
            values = (
                current_date,
                row['corporate_debtor'],
                row['name_of_irp_rp_liquidator'],
                row['under_process'],
                row['latest_claim_as_on_date'],
                row['view_details']
            )
            
            print("Executing update with values:", values)
            cursor.execute(update_query, values)
            rows_affected = cursor.rowcount
            print(f"Rows affected by update: {rows_affected}")
            
            update_new_deleted_count += rows_affected
            
        connection.commit()
        cursor.close()
        
        print(f"Total records updated with removal date: {update_new_deleted_count}")
        return update_new_deleted_count
        
    except Exception as e:
        print("Error in update_removal_date:")
        traceback.print_exc()
        if connection:
            connection.rollback()
        return 0
    finally:
        if connection:
            connection.close()