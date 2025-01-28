
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
import sys
from datetime import datetime
from config import ibbi_config
from functions import log,  check_increment_data, send_mail

import traceback


def extract_all_data_in_website():
    # Initialize the Chrome driver
    browser = ibbi_config.browser
    
    try:
        try:
            # Base URL to scrape
            base_url = ibbi_config.url
            
            # Initialize lists to store the data
            all_data = []
            
            # Start with the first page
            page = 1

            page_numbers = list(range(4, 122)) 
        except (TimeoutException, WebDriverException, NoSuchElementException) as e:
            raise Exception("Website not opened correctly") from e
            
        while True:
            try:
                try:
                    # if page in page_numbers:
                    #     print(f"Skipping page {page}")
                    #     page += 1
                    #     continue

                    url = f"{base_url}{page}"
                    print(f"Scraping page {page}: {url}")
                    browser.get(url)
                    browser.maximize_window()
                    
                    # Wait for the table to be present
                    wait = WebDriverWait(browser, 20)
                    table = wait.until(EC.presence_of_element_located((By.ID, "examples")))
                    
                    # Find all rows in the table body (excluding the header row)
                    rows = table.find_elements(By.TAG_NAME, "tr")[1:]
                    
                except (TimeoutException, WebDriverException, NoSuchElementException) as e:
                    raise Exception("Website not opened correctly") from e
                
                for row in rows:
                    try:
                        # Extract cells from each row
                        cells = row.find_elements(By.TAG_NAME, "td")
                        if len(cells) >= 5:  # Ensure row has expected number of columns
                            row_data = {
                                'corporate_debtor': cells[0].text.strip(),
                                'name_of_irp_rp_liquidator': cells[1].text.strip(),
                                'under_process': cells[2].text.strip(),
                                'latest_claim_as_on_date': cells[3].text.strip(),
                                'view_details': cells[4].find_element(By.TAG_NAME, "a").get_attribute("href") if cells[4].find_elements(By.TAG_NAME, "a") else ""
                            }
                            all_data.append(row_data)
                    except Exception as e:
                        print(f"Error processing row: {e}")
                        continue
                
                # Move to the next page
                page += 1
            except Exception as e:
                print(f"Error on page {page}: {e}")
                break 

        # Convert to DataFrame and save only if we have data
        if all_data:
            df = pd.DataFrame(all_data)
           
            first_excel_sheet_name = f"first_excel_sheet_{ibbi_config.current_date}.xlsx"
        
            first_exceL_sheet_path = rf"C:\Users\Premkumar.8265\Desktop\ibbi_claims_process\data\first_excel_sheet\{first_excel_sheet_name}"

            # Remove duplicates from the original DataFrame
            # df_unique = df.drop_duplicates()

            df.to_excel(first_exceL_sheet_path, index=False)

            print(f"Successfully scraped {len(all_data)} records. Data has been saved to {first_excel_sheet_name}")

            # print("df========\n\n", df.to_string( ))
            # check_increment_data.check_increment_data(first_exceL_sheet_path)
            
        else:
            print("No data was collected during the scraping process")
            return None
    
    except Exception as e:
        ibbi_config.log_list[1] = "Failure"
        if str(e) == "Website not opened correctly":
            ibbi_config.log_list[3] = "Website is not opened"
        else:
            ibbi_config.log_list[3] = "Error in data extraction part" 
        print("error in data extraction part======", ibbi_config.log_list)
        log.insert_log_into_table(ibbi_config.log_list)
        ibbi_config.log_list = [None] * 4

        traceback.print_exc()
        send_mail.send_email("ibbi section extract data in website error", e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f"Error occurred at line {exc_tb.tb_lineno}:")
        print(f"Exception Type: {exc_type}")
        print(f"Exception Object: {exc_obj}")
        print(f"Traceback: {exc_tb}")
        sys.exit("script error")

    finally:
        browser.quit()
