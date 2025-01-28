import sys
import traceback
import pandas as pd
from datetime import datetime
from config import ibbi_config


def update_removal_date(deleted_records):

    print("update removal dates function is called")
    
    connection = ibbi_config.db_connection()
    cursor = connection.cursor()
    try:
        current_date = datetime.now().strftime("%Y-%m-%d")
         
        update_new_deleted_count = 0
        skipped_count = 0
        
        for record in deleted_records:
            # First check if removal_date is already set
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
                record['corporate_debtor'],
                record['name_of_irp_rp_liquidator'],
                record['under_process'],
                record['latest_claim_as_on_date'],
                record['view_details']
            )
            
            cursor.execute(check_query, check_values)
            result = cursor.fetchone()
            
            # If record exists and removal_date is not set, update it
            if result and result[0] is None:
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
                values = (current_date,) + check_values
                cursor.execute(update_query, values)
                update_new_deleted_count += cursor.rowcount
            else:
                skipped_count += 1
        
        connection.commit()
        cursor.close()
        
        print(f"Records updated with removal date: {update_new_deleted_count}")
        
        return update_new_deleted_count, skipped_count
    except:
        pass