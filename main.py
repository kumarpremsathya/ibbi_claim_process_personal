import sys
import traceback
from config import ibbi_config
from functions import extract_all_data_in_website, log, check_increment_data


def main():
    print("main function is called")
    if ibbi_config.source_status == "Active":
        # extract_all_data_in_website.extract_all_data_in_website()
        
        first_excel_sheet_name =f"first_excel_sheet_{ibbi_config.current_date}.xlsx"
        first_exceL_sheet_path = fr"C:\Users\Premkumar.8265\Desktop\ibbi_claims_process\ibbi_claim_process_personal\data\first_excel_sheet\{first_excel_sheet_name}"
        
        check_increment_data.check_increment_data(first_exceL_sheet_path)
        print("finsihed")

    elif ibbi_config.source_status == "Hibernated":
        ibbi_config.log_list[1] = "not run"
        print(ibbi_config.log_list)
        log.insert_log_into_table(ibbi_config.log_list)

        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f"Error occurred at line {exc_tb.tb_lineno}:")
        print(f"Exception Type: {exc_type}")
        print(f"Exception Object: {exc_obj}")
        print(f"Traceback: {exc_tb}")
            
    elif ibbi_config.source_status == "Inactive":
        ibbi_config.log_list[1] = "not run"
       
        traceback.print_exc()
        print(ibbi_config.log_list)
        log.insert_log_into_table(ibbi_config.log_list)
        
        print(ibbi_config.log_list)
        ibbi_config.log_list = [None] * 4
        traceback.print_exc()
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f"Error occurred at line {exc_tb.tb_lineno}:")
        print(f"Exception Type: {exc_type}")
        print(f"Exception Object: {exc_obj}")
        print(f"Traceback: {exc_tb}")
        sys.exit("script error")


if __name__ == "__main__":
    main()
