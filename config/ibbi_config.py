from datetime import datetime
import mysql.connector
from selenium import webdriver

source_status = "Active"
source_name = "ibbi_claims_process"


log_list = [None] * 4
no_data_avaliable = 0
no_data_scraped = 0
newly_added_count = 0
deleted_source = []
deleted_source_count = 0
update_new_deleted_count = 0

url =  "https://ibbi.gov.in/claims/claim-process?page="

browser = webdriver.Chrome()

current_date = datetime.now().strftime("%Y-%m-%d")


# host = "localhost"
# user = "root"
# password = "root"
# database = "ibbi"
# auth_plugin = "mysql_native_password"


host = "4.213.77.165"
user = "root1"
password = "Mysql1234$"
database = "ibbi"
auth_plugin = "mysql_native_password"


def db_connection():
    connection = mysql.connector.connect(
        host = host,
        user = user,
        password = password,
        database = database,
        auth_plugin = auth_plugin

    )
    return connection
