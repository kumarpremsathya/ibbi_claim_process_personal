from django.shortcuts import render
import pandas as pd
# Create your views here.
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework import status
from .models import *
from datetime import datetime
from django.http import Http404
from django.http import HttpResponse
import zipfile
from .models import ibbi_claims_process
from django.core.exceptions import ValidationError
import os
import re
from django.db.models import Min
from django.conf import settings
import calendar
import tempfile
from django.utils.dateparse import parse_date
import ast
import json


def validate_date(date_string):
    """
    Validates if the given date string is in the format YYYY-MM-DD.
    Returns True if valid, otherwise False.
    """
    try:
        datetime.strptime(date_string, '%Y-%m-%d')
        return True
    except:
       return False

class Custom404View(APIView):
    """
    Custom view to handle 404 errors and return a JSON response.
    """
    def get(self, request, *args, **kwargs):
        return Response({"result": "Resource not found"}, status=status.HTTP_404_NOT_FOUND)



# Define your API view
class GetOrderDateViewClaimProcess(APIView):
    """
    API view to get order details based on the provided date and source name.
    """
    def get(self, request, *args, **kwargs):
        try:
            limit = int(request.GET.get('limit', 50))
            offset = int(request.GET.get('offset', 0))
        except ValueError:
            return Response({"result": "Invalid limit or offset -, must be an integer"}, status = status.HTTP_422_UNPROCESSABLE_ENTITY)

        date_str = str(request.GET.get('date', None))
        source_name = kwargs.get('source_name')  # Extract 'type_of_order' parameter from URL

        if date_str:
            if len(date_str) != 10 or not validate_date(date_str):
                return Response({"result": "Incorrect date format, should be YYYY-MM-DD"}, status = status.HTTP_422_UNPROCESSABLE_ENTITY)

            try:
                date = datetime.strptime(date_str, '%Y-%m-%d').date()  # Convert string to datetime.date object
            except ValueError:
                return Response({"result": "Incorrect date format, should be YYYY-MM-DD"}, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

            valid_parameters = {'limit', 'offset', 'date'}
            provided_parameters = set(request.GET.keys())

            if not valid_parameters.issuperset(provided_parameters):
                return Response({"result": "Invalid query parameters, check spelling for given parameters"}, status = status.HTTP_400_BAD_REQUEST)

            earliest_date = ibbi_claims_process.objects.filter(source_name=source_name).aggregate(Min('date_scraped'))['date_scraped__min']

            if earliest_date:
                earliest_date = earliest_date.date()  # Convert earliest_date to datetime.date object
                
                if date < earliest_date:
                    return Response({"result": f"Data is available from initial scraping date {earliest_date}"}, status=status.HTTP_400_BAD_REQUEST)
            
            try:
                # Filter queryset based on 'type_of_order' parameter
                # date_obj = parse_date(date)
                if source_name == 'ibbi_claims_process':
                    order_details = ibbi_claims_process.objects.filter(date_scraped__startswith=date, source_name='ibbi_claims_process').values( 'sr_no','source_name', 'corporate_debtor', 'name_of_irp_rp_liquidator',  'under_process',  'latest_claim_as_on_date', 
                                                                                                                                                'view_details','header_information','claims_details','pdf_links', 'pdf_names', 'pdf_relative_paths', 'updated_date','removal_date', 'date_scraped')[offset:limit]
                    total_count = ibbi_claims_process.objects.filter(date_scraped__startswith=date, source_name='ibbi_claims_process').count()

                else:
                    return Response({"result": "Invalid 'source_name' parameter"}, status=status.HTTP_400_BAD_REQUEST)


                def clean_json_data(data):
                    """
                    Recursively cleans JSON data by parsing JSON strings to objects.
                    """
                    if isinstance(data, dict):
                        for key, value in data.items():
                            if isinstance(value, str):
                                try:
                                    # Parse JSON strings to objects
                                    data[key] = json.loads(value)
                                except json.JSONDecodeError:
                                    # Ignore non-JSON strings
                                    pass
                            elif isinstance(value, (dict, list)):
                                # Recursively clean nested structures
                                clean_json_data(value)
                    elif isinstance(data, list):
                        for i, value in enumerate(data):
                            if isinstance(value, str):
                                try:
                                    data[i] = json.loads(value)
                                except json.JSONDecodeError:
                                    pass
                            elif isinstance(value, (dict, list)):
                                clean_json_data(value)

                # Clean the JSON data
                cleaned_order_details = []
                for item in order_details:
                    cleaned_item = item.copy()
                    clean_json_data(cleaned_item)
                    cleaned_order_details.append(cleaned_item)
                
                # print(" order_details :", order_details )
                if len(cleaned_order_details) > 0:
                        
                    # Create download link for the zip file
                    total_pdf_download_link = request.build_absolute_uri('/api/v1/{}/download_pdfs/?date={}'.format(source_name, date))+ f'&limit={limit}&offset={offset}' 

                    
                    # Return JSON response with results including the total PDF download link
                    return Response({"result": cleaned_order_details, 'total_count': total_count, 'total_pdf_download_link': total_pdf_download_link}, status=status.HTTP_200_OK)
                else:
                    return Response({"result": "No Data Provided in your specific date!!!."}, status = status.HTTP_404_NOT_FOUND)
            except TimeoutError:
                return Response({"result": "timeout error"}, status = status.HTTP_502_BAD_GATEWAY)
            except Exception as err:
                return Response({"result": f"An internal server error occurred: {err}"}, status = status.HTTP_500_INTERNAL_SERVER_ERROR)
        else:
           raise Http404("Page not found")



class DownloadPDFsViewClaimProcess(APIView):
    """
    API view to download PDFs based on the provided date and source name.
    """
    def get(self, request, *args, **kwargs):
        try:
            limit = int(request.GET.get('limit', 50))
            offset = int(request.GET.get('offset', 0))
            
            # Check if the limit is above 500 and raise an exception if so
            if limit > 500:
                return Response({"result": "Limit should not exceed 500"}, status=status.HTTP_400_BAD_REQUEST)
        except ValueError as ve:
            return Response({"result": str(ve)}, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

        date = request.GET.get('date')
        source_name = kwargs.get('source_name')  # Extract 'type_of_order ' parameter from URL
        
        
        # BASE_DIR = Path(__file__).resolve().parent.parent
        # print("BASE_DIR: ", BASE_DIR)
        
        BASE_DIR2 = 'C:\\Users\\Premkumar.8265\\Desktop\\ibbi_claims_process\\' # Update with your base directory
        print("BASE_DIR2: ", BASE_DIR2)
        
        try:
            if date:
                try:
                    validate_date(date)
                except ValidationError:
                    return Response({"result": "Incorrect date format, should be YYYY-MM-DD"}, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

                valid_parameters = {'limit', 'offset', 'date'}
                provided_parameters = set(request.GET.keys())

                if not valid_parameters.issuperset(provided_parameters):
                    return Response({"result": "Invalid query parameters, check spelling for given parameters"}, status=status.HTTP_400_BAD_REQUEST)

                root_directory = os.path.join(settings.MEDIA_ROOT, source_name)
                print("Root directory:", root_directory)  # Check the root directory

                year, month, day = date.split('-')
                month_names = {
                    1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
                    7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'
                }
                month_name = month_names[int(month)]
                print("Year:", year)  # Check the year
                print("Month:", month_name)  # Check the month name
                
                # Retrieve all orders for the specified date
                order_details = ibbi_claims_process.objects.filter(date_scraped__startswith=date, source_name=source_name)[offset:limit]

                # print("order_details:", order_details.values())
                
                # # Convert the queryset into a Pandas DataFrame for printing object values in table format in terminal. 
                # order_details_df = pd.DataFrame(list(order_details.values()))

                # # Print the DataFrame
                # print("Order Details DataFrame:")
                # print(order_details_df)
                
                pdf_paths_set = set()
                
                for order in order_details:
                    try:
                        paths_list = ast.literal_eval(order.pdf_relative_paths)
                        for path in paths_list:
                            full_path = os.path.join(BASE_DIR2, path.lstrip('/'))
                            pdf_paths_set.add(full_path)
                    except (ValueError, SyntaxError):
                        print("Error: Invalid format in pdf_relative_paths for order ID:", order.sr_no)
                        return Response({"result": "Invalid PDF paths format"}, status=status.HTTP_422_UNPROCESSABLE_ENTITY)


                print("PDF paths:", pdf_paths_set)  # Check the PDF paths
                
                if pdf_paths_set:
                    temp_file = tempfile.NamedTemporaryFile(delete=False)
                    with zipfile.ZipFile(temp_file, 'w') as zip_file:
                        for pdf_path in pdf_paths_set:
                            if os.path.exists(pdf_path):
                                zip_file.write(pdf_path, os.path.relpath(pdf_path, BASE_DIR2))
                            else:
                                # If the PDF file is missing, log an error or return a message
                                print("Error: The file does not exist:", pdf_path)
                                return Response({"result": f"PDF file {pdf_path} is missing"}, status=status.HTTP_404_NOT_FOUND)

                    temp_file.close()
                    temp_file = open(temp_file.name, 'rb')
                    data = temp_file.read()
                    temp_file.close()
                    os.unlink(temp_file.name)
                    
                    response = HttpResponse(data, content_type='application/zip')
                    response['Content-Disposition'] = 'attachment; filename="ibbi_claim_process.zip"'
                    
                    return response
                else:
                    return HttpResponse("No PDF files found for the specified date.", status=status.HTTP_404_NOT_FOUND)
            else:
                return HttpResponse("Date parameter is required.", status=status.HTTP_400_BAD_REQUEST)
        except ValueError:
            return HttpResponse("Invalid limit or offset value, must be an integer", status=status.HTTP_422_UNPROCESSABLE_ENTITY)
        except ValidationError:
            return HttpResponse("Incorrect date format, should be YYYY-MM-DD", status=status.HTTP_422_UNPROCESSABLE_ENTITY)
        except Exception as e:
            print("Error:", e)  # Debugging statement
            return HttpResponse("An error occurred.", status=status.HTTP_500_INTERNAL_SERVER_ERROR)



