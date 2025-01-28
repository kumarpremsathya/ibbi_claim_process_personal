from django.db import models

# Create your models here.

from django.db import models

class ibbi_claims_process(models.Model):
    sr_no = models.AutoField(primary_key=True)
    source_name = models.CharField(max_length=255, null=True, blank=True)
    corporate_debtor = models.CharField(max_length=255, null=True, blank=True)
    name_of_irp_rp_liquidator = models.CharField(max_length=255, null=True, blank=True)
    under_process = models.CharField(max_length=100, null=True, blank=True)
    latest_claim_as_on_date = models.CharField(max_length=100, null=True, blank=True)
    view_details = models.TextField(null=True, blank=True)
    header_information = models.TextField(null=True, blank=True)
    claims_details = models.TextField(null=True, blank=True)
    pdf_links = models.TextField(null=True, blank=True)
    pdf_names = models.TextField(null=True, blank=True)
    pdf_relative_paths = models.TextField(null=True, blank=True)
    updated_date = models.CharField(max_length=250, null=True, blank=True)
    removal_date = models.CharField(max_length=250, null=True, blank=True)
    date_scraped = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'ibbi_claims_process'
       
