import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import re 
import requests
import json
import sys
import mysql.connector
from mysql.connector import Error
from datetime import datetime


def scrape_ibbi_claims():
    # Initialize the Chrome driver
    browser = webdriver.Chrome()
    
    try:
        # Base URL to scrape
        base_url = "https://ibbi.gov.in/claims/claim-process?page="
        
        # Initialize lists to store the data
        all_data = []
        
        # Start with the first page
        page = 1

        page_numbers = list(range(4, 122)) 

        while True:
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
            output_file = 'ibbi_claims_data.xlsx'

            # Remove duplicates from the original DataFrame
            df_unique = df.drop_duplicates()
            df_unique.to_excel(output_file, index=False)
            print(f"Successfully scraped {len(all_data)} records. Data saved to '{output_file}'")
            return df
        else:
            print("No data was collected during the scraping process")
            return None
    
    
    finally:
        browser.quit()


def download_pdfs(pdf_links, claim_date):
    
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
        
        # Create folder structure with claim_process
        base_dir = "pdf_downloads"
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
            browser.get(url)
            browser.maximize_window()
            
            # Wait for table to be present
            wait = WebDriverWait(browser, 20)
            table = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/section/div/div/div[3]/table")))
            
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
                'View_Details': url,
                'Header_Information': [json.dumps(header_data)],
                'Claims_Details': [json.dumps(claims_data, indent=4)],
                'PDF_Links': [json.dumps(pdf_links)],
                'PDF_Names': [json.dumps([f[0] for f in downloaded_files])],
                'Relative_Paths': [json.dumps([f[1] for f in downloaded_files])]
            })
            
            return df
            
        except Exception as e:
            print(f"Error connecting  (attempt {attempt + 1}): {e}")
            # retry_scrape_claim_details(browser, url, claim_date)
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)


# def retry_scrape_claim_details(browser, url, claim_date, retries=3):
#     for attempt in range(retries):
#         try:
#             return scrape_claim_details(browser, url, claim_date)
#         except Exception as e:
#             print(f"Retry attempt {attempt + 1} failed: {e}")
#             if attempt < retries - 1:
#                 time.sleep(5)
#     print("All retry attempts failed.")
#     return None


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


def create_db_connection_with_retry(max_retries=3, retry_delay=5):
    for attempt in range(max_retries):
        try:
            connection = mysql.connector.connect(
               host='4.213.77.165',
                user='root1',
                password='Mysql1234$',
                database='ibbi'
            )
            if connection.is_connected():
                print(f"Successfully connected to MySQL on attempt {attempt + 1}")
                return connection
        except Error as e:
            print(f"Error connecting to MySQL (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
    print("Failed to connect to MySQL after multiple attempts")
    return None


def insert_into_database(connection, data_row):
    try:
        cursor = connection.cursor()
        
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
        
        # Prepare the values for insertion
        values = (
            'ibbi_claim_process',  # source_name
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
        connection.commit()
        print(f"Successfully inserted data for {data_row['corporate_debtor']}")
        
    except Error as e:
        print(f"Error inserting data: {e}")
        connection.rollback()
    finally:
        if cursor:
            cursor.close()


def get_processed_records(connection):
    """Get list of already processed View_Details URLs from database"""
    try:
        cursor = connection.cursor()
        query = "SELECT view_details FROM ibbi_claims_process"
        cursor.execute(query)
        processed_urls = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return processed_urls
    except Error as e:
        print(f"Error fetching processed records: {e}")
        return []


def main():

    # Initialize browser once
    browser = webdriver.Chrome()

    # Connect to MySQL database
    connection = create_db_connection_with_retry()
    if not connection:
        sys.exit(1)

    try:
        # df_main = scrape_ibbi_claims()
          
        # Get list of already processed URLs
        processed_urls = get_processed_records(connection)
        print(f"Found {len(processed_urls)} already processed records")

        # Read the input Excel file
        df = pd.read_excel('incremental_rows_new.xlsx')

        # # # Use iloc to select specific rows (as in your example)
        df = df.iloc[:]
        
        # # Filter out already processed records
        # df_remaining = df[~df['View_Details'].isin(processed_urls)]
        # print(f"Processing {len(df_remaining)} remaining records out of {len(df)} total records")

        # if df_remaining.empty:
        #     print("No new records to process")
        #     return


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
                    
                    # Insert the combined data into the database
                    insert_into_database(connection, combined_row)
                    
                    print(f"Processed and stored data for {combined_row['corporate_debtor']}")
                
                # Add a small delay between requests to avoid overwhelming the server
                time.sleep(2)

    except Exception as e:
        print(f"An error occurred in main: {e}")
    
    finally:
        browser.quit()  # Close browser only once at the end
        if connection and connection.is_connected():
            connection.close()
            print("Database connection closed.")


# def main():

#     # Connect to MySQL database
#     connection = create_db_connection_with_retry()
#     if not connection:
#         sys.exit(1)

#     df_main = scrape_ibbi_claims()
     

#     df = pd.read_excel('ibbi_claims_data.xlsx')
#     # Use iloc to select the first 10 rows or some specific rows
#     df = df.iloc[151:153]

#     all_combined_data = []
#     for index, row in df.iterrows():
#         if pd.notna(row['View_Details']):
#             detailed_data = scrape_claim_details(
#                 url=row['View_Details'],
#                 claim_date=row['Latest_Claim_As_On_Date']
#             )
#             if detailed_data is not None and not detailed_data.empty:
#                 all_combined_data.append(detailed_data)


#      # Step 6: Combine all scraped detailed data into a single DataFrame
#     combined_df = pd.concat(all_combined_data, ignore_index=True)
    
#     # Save to a single Excel file
#     # combined_df.to_excel('combined_ibbi_claims_data.xlsx', index=False)


#     # Step 7: Merge the original DataFrame with the combined detailed DataFrame
#     final_combined_df = pd.merge(df, combined_df, on='View_Details', how='inner')

    
#     # Convert JSON strings to lists for better Excel viewing
#     final_combined_df['PDF_Names'] = final_combined_df['PDF_Names'].apply(json.loads)
#     final_combined_df['Relative_Paths'] = final_combined_df['Relative_Paths'].apply(json.loads)
    
#     # Step 8: Save the final combined DataFrame to a new Excel file
#     final_combined_df.to_excel('final_combined_ibbi_claims_data.xlsx', index=False)
#     print("All data has been saved to 'final_combined_ibbi_claims_data.xlsx'")



if __name__ == "__main__":
    main()