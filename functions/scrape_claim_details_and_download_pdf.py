import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
import os
import requests
import json
import sys
from datetime import datetime
from functions import insert_final_data_to_mysql, log
from config import ibbi_config
import traceback


def download_pdfs(pdf_links, claim_date):
    print("download pdf function is called")
    
    downloaded_files = []
    try:
        # Parse the date string
        day, month, year = claim_date.split('-')
        
        # Map month number to month name
        month_names = {
            '01': 'jan', '02': 'feb', '03': 'mar', '04': 'apr',
            '05': 'may', '06': 'jun', '07': 'jul', '08': 'aug',
            '09': 'sep', '10': 'oct', '11': 'nov', '12': 'dec'
        }
        

        # Use the new base directory path
        base_dir = fr"C:\Users\Premkumar.8265\Desktop\ibbi_claims_process\pdf_downloads"
        claim_process_dir = os.path.join(base_dir, "claim_process")
        year_dir = os.path.join(claim_process_dir, year)
        month_dir = os.path.join(year_dir, month_names[month])
        
        # Create directories if they don't exist
        os.makedirs(month_dir, exist_ok=True)
        
        for link in pdf_links:
            try:
                # Extract the filename from the URL
                raw_filename = link.split('/')[-1]
                sanitized_filename = raw_filename.split('-')[-1]
                
                # Create full filepath
                filepath = os.path.join(month_dir, sanitized_filename)
                
                # Create relative path
                relative_path = os.path.join("pdf_downloads", "claim_process", year, month_names[month], sanitized_filename)
                relative_path = relative_path.replace("\\", "/")
                
                # Download the PDF
                response = requests.get(link)
                response.raise_for_status()
                
                # Save the PDF
                with open(filepath, 'wb') as pdf_file:
                    pdf_file.write(response.content)
                
                # Store the filename and relative path
                downloaded_files.append((sanitized_filename, relative_path))
                
                print(f"Downloaded: {sanitized_filename} to {month_dir}")
            
            except requests.exceptions.RequestException as e:
                print(f"Failed to download {link}: {e}")
    
    except Exception as e:
        print(f"Error processing date or creating folders: {e}")
    
    return downloaded_files


def scrape_claim_details(browser, url, claim_date, max_retries=3, retry_delay=5):
  
    for attempt in range(max_retries):
        try:
            try:
                browser.get(url)
                browser.maximize_window()
                
                # Wait for table to be present
                wait = WebDriverWait(browser, 20)
                table = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/section/div/div/div[3]/table")))

            except (TimeoutException, WebDriverException, NoSuchElementException) as e:
                raise Exception("Website not opened correctly") from e   
            
            # Get header information
            header_data = {
                'Corporate_Debtor': browser.find_element(By.NAME, "corporate_debtor").get_attribute("value"),
                'CIN_Number': browser.find_element(By.NAME, "cin_no").get_attribute("value"),
                'Date_of_Commencement': browser.find_element(By.NAME, "date_of_commencement").get_attribute("value"),
                'List_of_Claim_date': browser.find_element(By.NAME, "list_of_creditors").get_attribute("value"),
            }
            
            # Initialize list to store all claim rows
            claims_data = []
            pdf_links = []
            
            # Look specifically for the percentage share header in the thead section
            thead = table.find_element(By.TAG_NAME, "thead")
            headers = thead.find_elements(By.TAG_NAME, "th")
            
            # Check both the th text content and potential comments
            has_percentage_column = any(
                "% Share in Total Amount of Claims Admitted" in header.text 
                for header in headers
            )


            print("has_percentage_column", has_percentage_column)

            # Extract claim details from each row
            rows = table.find_elements(By.TAG_NAME, "tr")[3:]  # Skip header rows
            
            if has_percentage_column :

                for row in rows:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) > 1:  # Skip empty rows
                        # Check if this is the total row
                        if 'total' in row.get_attribute("class").lower():

                            try:
                                total_per_elem = row.find_element(By.ID, "total_per")
                                total_percentage = total_per_elem.text.strip()
                            except:
                                total_percentage = ""
                            total_row = {

                                'Category': 'Total',
                                'Summary_of_Claims_Received': {
                                    'No_of_Claims': get_input_value_by_name(cells, 'total_crno'),
                                    'Amount': get_input_value_by_name(cells, 'total_crm')
                                },
                                'Summary_of_Claims_Admitted': {
                                    'No_of_Claims': get_input_value_by_name(cells, 'clain_adm_no'),
                                    'Amount_of_claims_admitted': get_input_value_by_name(cells, 'clain_adm_amt'),
                                    "% Share in Total Amount of Claims Admitted": total_percentage
                                },
                                'Amount_of_Contingent_Claims': get_input_value_by_name(cells, 'total_acc'),
                                'Amount_of_Claim_Not_Admitted': get_input_value_by_name(cells, 'total_claim_not_admitted'),
                                'Amount_of_Claims_Under_Verification': get_input_value_by_name(cells, 'total_amuv'),
                                'Details_in_Annexure': {
                                    'link': '',
                                    'filename': ''
                                },
                                'Remarks': ''
                            }
                            claims_data.append(total_row)
                            continue

                        # Handle regular rows
                        annexure_link = ""
                        annexure_filename = ""
                        try:
                            # First try to find anchor directly in td
                            try:
                                annexure_elem = cells[10].find_element(By.TAG_NAME, "a")
                                
                            except:
                                # If not found, look for div containing anchor
                                try:
                                    div = cells[10].find_element(By.TAG_NAME, "div")
                                    annexure_elem = div.find_element(By.TAG_NAME, "a")

                                except:
                                    annexure_elem = None
                        
                            
                            if annexure_elem:
                                annexure_link = annexure_elem.get_attribute("href")
                                annexure_filename = annexure_elem.get_attribute("title")
                                if annexure_link and annexure_link.lower().endswith('.pdf'):
                                    pdf_links.append(annexure_link)
                        except:
                            pass
                        
                        claim_row = {
                            'Sl_No': cells[0].text.strip(),
                            'Category_of_Creditor': cells[1].text.strip(),
                            'Summary_of_Claims_Received': {
                                'No_of_Claims': get_input_value(cells[2]),
                                'Amount': get_input_value(cells[3])
                            },
                            'Summary_of_Claims_Admitted': {
                                'No_of_Claims': get_input_value(cells[4]),
                                'Amount_of_claims_admitted': get_input_value(cells[5]),
                                "% Share in Total Amount of Claims Admitted":get_input_value(cells[6])
                            },
                            'Amount_of_Contingent_Claims': get_input_value(cells[7]),
                            'Amount_of_Claim_Not_Admitted': get_input_value(cells[8]),
                            'Amount_of_Claims_Under_Verification': get_input_value(cells[9]),
                            'Details_in_Annexure': {
                                'link': annexure_link,
                                'filename': annexure_filename
                            },
                            'Remarks': get_textarea_value(cells[-1])
                        }
                        claims_data.append(claim_row)
            
            else:
                for row in rows:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) > 1:  # Skip empty rows
                        # Check if this is the total row
                        if 'total' in row.get_attribute("class").lower():
                            total_row = {
                                'Category': 'Total',
                                'Summary_of_Claims_Received': {
                                    'No_of_Claims': get_input_value_by_name(cells, 'total_crno'),
                                    'Amount': get_input_value_by_name(cells, 'total_crm')
                                },
                                'Summary_of_Claims_Admitted': {
                                    'No_of_Claims': get_input_value_by_name(cells, 'clain_adm_no'),
                                    'Amount_of_claims_admitted': get_input_value_by_name(cells, 'clain_adm_amt')
                                },
                                'Amount_of_Contingent_Claims': get_input_value_by_name(cells, 'conti_claim_amt'),
                                'Amount_of_Claims_Rejected': get_input_value_by_name(cells, 'rej_clain_amt'),
                                'Amount_of_Claims_Under_Verification': get_input_value_by_name(cells, 'claim_veri_amt'),
                                'Details_in_Annexure': {
                                    'link': '',
                                    'filename': ''
                                },
                                'Remarks': ''
                            }
                            claims_data.append(total_row)
                            continue

                        # Handle regular rows as before
                        annexure_link = ""
                        annexure_filename = ""
                        try:
                            annexure_elem = cells[9].find_element(By.TAG_NAME, "a")
                            annexure_link = annexure_elem.get_attribute("href")
                            annexure_filename = annexure_elem.get_attribute("title")
                            if annexure_link and annexure_link.lower().endswith('.pdf'):
                                pdf_links.append(annexure_link)
                        except:
                            pass
                        
                        claim_row = {
                            'Sl_No': cells[0].text.strip(),
                            'Category_of_Stakeholders': cells[1].text.strip(),
                            'Summary_of_Claims_Received': {
                                'No_of_Claims': get_input_value(cells[2]),
                                'Amount': get_input_value(cells[3])
                            },
                            'Summary_of_Claims_Admitted': {
                                'No_of_Claims': get_input_value(cells[4]),
                                'Amount_of_claims_admitted': get_input_value(cells[5])
                            },
                            'Amount_of_Contingent_Claims': get_input_value(cells[6]),
                            'Amount_of_Claims_Rejected': get_input_value(cells[7]),
                            'Amount_of_Claims_Under_Verification': get_input_value(cells[8]),
                            'Details_in_Annexure': {
                                'link': annexure_link,
                                'filename': annexure_filename
                            },
                            'Remarks': get_textarea_value(cells[-1])
                        }
                        claims_data.append(claim_row)
            
            # Download PDFs and get their information
            downloaded_files = download_pdfs(pdf_links, claim_date)

            # Create DataFrame
            df = pd.DataFrame({
                'view_details': url,
                'Header_Information': [json.dumps(header_data)],
                'Claims_Details': [json.dumps(claims_data, indent=4)],
                'PDF_Links': [json.dumps(pdf_links)],
                'PDF_Names': [json.dumps([f[0] for f in downloaded_files])],
                'Relative_Paths': [json.dumps([f[1] for f in downloaded_files])]
            })
            
            print("df=========", df)
            return df
            
        except Exception as e:
            print(f"An error occurred: {e}")
            # retry_scrape_claim_details(browser, url, claim_date)
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
    

      # Return DataFrame with None values if all retries fail
    return pd.DataFrame({
        'view_details': [url],
        'Header_Information': [None],
        'Claims_Details': [None],
        'PDF_Links': [None],
        'PDF_Names': [None],
        'Relative_Paths': [None]
    })


def get_input_value(cell):
    """Helper function to get input value from cell"""
    try:
        input_elem = cell.find_element(By.TAG_NAME, "input")
        return input_elem.get_attribute("value")
    except:
        return ""

def get_input_value_by_name(cells, name):
    """Helper function to get input value by name attribute"""
    try:
        for cell in cells:
            try:
                input_elem = cell.find_element(By.NAME, name)
                return input_elem.get_attribute("value")
            except:
                continue
        return ""
    except:
        return ""

def get_textarea_value(cell):
    """Helper function to get textarea value from cell"""
    try:
        textarea_elem = cell.find_element(By.TAG_NAME, "textarea")
        return textarea_elem.get_attribute("value")
    except:
        return ""


def scrape_claim_details_and_download_pdf(increment_data_excel_path):
    
    print("scrape_claim_details_and_download_pdf function is called")
    try:
        # Initialize browser once
        browser = ibbi_config.browser

        # Read the input Excel file
        df = pd.read_excel(increment_data_excel_path)

        # # # Use iloc to select specific rows (as in your example)
        df = df.iloc[:]
 
        combined_rows = [] 
        for index, row in df.iterrows():
            if pd.notna(row['view_details']):

                print(f"Processing record {index + 1} of {len(df)}: {row['corporate_debtor']}")

                # Get detailed data for each row
                detailed_data = scrape_claim_details(
                    browser=browser,
                    url=row['view_details'],
                    claim_date=row['latest_claim_as_on_date']
                )
                
                if detailed_data is not None and not detailed_data.empty:
                    # Combine the original row data with detailed data
                    combined_row = row.to_dict()
                    detailed_row = detailed_data.iloc[0].to_dict()
                    combined_row.update(detailed_row)
                    combined_rows.append(combined_row)  # Add to batch
                    
                     # Create DataFrame correctly by wrapping values in lists
                    df_final = pd.DataFrame({
                        key: [value] for key, value in combined_row.items()
                    })
                    
                    print("df_final", df_final)
                   
                    final_excel_sheet_name = f"final_sheet_{ibbi_config.current_date}.xlsx"
                    final_excel_sheet_path = fr"C:\Users\Premkumar.8265\Desktop\ibbi_claims_process\data\final_excel_sheet\{final_excel_sheet_name}"

                    df_final.to_excel(final_excel_sheet_path, index=False)
                
        # Batch insert all rows at once
        if combined_rows:
            insert_final_data_to_mysql.insert_final_data_to_mysql(combined_rows)
                    
        # print(f"Processed and stored data for {combined_row['corporate_debtor']}")
                
        # Add a small delay between requests to avoid overwhelming the server
        time.sleep(2)

    except Exception as e:
        print(f"An error occurred in scrap claim details and pdf part: {e}")
        ibbi_config.log_list[1] = "Failure"
       
        ibbi_config.log_list[3] = "Error in scrap claim details and pdf part" 
        print("error in data extraction in scrap claim details and pdf part======", ibbi_config.log_list)
        log.insert_log_into_table(ibbi_config.log_list)
        ibbi_config.log_list = [None] * 4

        traceback.print_exc()
        # send_mail.send_email("ibbi section extract data in website error", e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f"Error occurred at line {exc_tb.tb_lineno}:")
        print(f"Exception Type: {exc_type}")
        print(f"Exception Object: {exc_obj}")
        print(f"Traceback: {exc_tb}")
        sys.exit("script error")
    
    finally:
        browser.quit()  # Close browser only once at the en
        print("Database connection closed.")
