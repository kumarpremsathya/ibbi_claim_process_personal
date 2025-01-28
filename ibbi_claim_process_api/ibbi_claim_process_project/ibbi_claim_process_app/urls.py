from django.urls import path
from  .views import *

urlpatterns = [

    path('<str:source_name>/get/', GetOrderDateViewClaimProcess.as_view(), name='GetOrderDateViewClaimProcess'),
    path('<str:source_name>/download_pdfs/', DownloadPDFsViewClaimProcess.as_view(), name='download_pdfs'), 
    path('<path:invalid_path>', Custom404View.as_view(), name='invalid_path'),

]
