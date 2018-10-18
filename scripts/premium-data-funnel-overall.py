
# coding: utf-8

# In[1]:


import argparse
import datetime as DT
import csv
import time

from apiclient.discovery import build
import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools

import re
import itertools


# In[2]:


SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
DISCOVERY_URI = ('https://analyticsreporting.googleapis.com/$discovery/rest')
CLIENT_SECRETS_PATH = 'client_secrets.json' # Path to client_secrets.json file.
VIEW_ID = '157208265'


# In[3]:


def initialize_analyticsreporting():
    # Set up a Flow object to be used if we need to authenticate.
    flow = client.flow_from_clientsecrets(
                    CLIENT_SECRETS_PATH, scope=SCOPES,
                    message=tools.message_if_missing(CLIENT_SECRETS_PATH))

    # Dummy command line args
    parser = argparse.ArgumentParser(
                    formatter_class=argparse.RawDescriptionHelpFormatter,
                    parents=[tools.argparser])
    flags = parser.parse_args([])

    # Prepare credentials, and authorize HTTP object with them.
    # If the credentials don't exist or are invalid run through the native client
    # flow. The Storage object will ensure that if successful the good
    # credentials will get written back to a file.
    storage = file.Storage('analyticsreporting.dat')
    credentials = storage.get()
    if credentials is None or credentials.invalid:
        credentials = tools.run_flow(flow, storage, flags)
    http = credentials.authorize(http=httplib2.Http())

    # Build the service object.
    analytics = build('analytics', 'v4', http=http, discoveryServiceUrl=DISCOVERY_URI)

    return analytics


# In[85]:


def get_report(analytics, dimensions=[], metrics=[], filters=[], segments=[]):
    # Use the Analytics Service Object to query the Analytics Reporting API V4.
    return analytics.reports().batchGet(
        body={
        'reportRequests': [
            {
                'viewId': VIEW_ID,
                'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
                'dimensions': dimensions,
                'metrics': metrics,
                'dimensionFilterClauses': filters,
                'segments': segments
            }]
        }
    ).execute()


# In[86]:


def print_response(response, funnel):
  """Parses and prints the Analytics Reporting API V4 response"""

  for report in response.get('reports', []):
    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
    rows = report.get('data', {}).get('rows', [])

    for row in rows:
      dimensions = row.get('dimensions', [])
      dateRangeValues = row.get('metrics', [])

      for header, dimension in zip(dimensionHeaders, dimensions):
        print (header + ': ' + dimension)

      for i, values in enumerate(dateRangeValues):
        for metricHeader, value in zip(metricHeaders, values.get('values')):
          print (funnel + ': ' + value)
          return value


analytics = initialize_analyticsreporting()

today = DT.date.today()
initial = today - DT.timedelta(days=1)
start_date = initial.strftime('%Y-%m-%d')
end_date = start_date

def all_users():
    metrics = [{'expression': 'ga:users'}]
    response = get_report(analytics, [], metrics)
    value = print_response(response, 'all users')
    return int(value or 0)


def users_by_segment(segment_id, name):
    dimensions = [
        {"name": "ga:segment"}
    ]
    metrics = [{'expression': 'ga:users'}]
    filters = []
    segments = [
        {
          "segmentId": segment_id
        }
    ]
    response = get_report(analytics, dimensions, metrics, filters, segments)
    value = print_response(response, name)
    return int(value or 0)


def events_by_expression(label_expression, name, category='outbound', action='click'):
    metrics = [{'expression': 'ga:uniqueEvents'}]
    filters = [
        {'filters': [{'dimensionName': 'ga:eventCategory', 'expressions': category}]},
        {'filters': [{'dimensionName': 'ga:eventAction', 'expressions': action}]},
        {'filters': [{'dimensionName': 'ga:eventLabel', 'expressions': label_expression}]}
    ]
    response = get_report(analytics, [], metrics, filters)
    value = print_response(response, name)
    return int(value or 0)


users_total = all_users()
users_on_core_datasets = users_by_segment('gaid::7ttldF7oQiCWlohm8oJ9WA', 'users on core datasets')
users_on_sports_data = users_by_segment('gaid::oZKd1ze1RHmN5J8PlDukLw', 'users on sports data')
users_on_machine_learning = users_by_segment('gaid::gZ7K7R7HRWuhNiPbmwgByQ', 'users on machine learning datasets')
users_on_cryptocurrency_datasets = users_by_segment('gaid::iqN7sg3TR0iPVi2Ty7FG0w', 'users on cryptocurrency datasets')
users_on_s_and_p_500_datasets = users_by_segment('gaid::0GZPLmVFSpSxUMs_Gqnn4Q', 'users on s&p500 datasets')
users_on_world_bank_datasets = users_by_segment('gaid::t7xV4c6aRaiHtzAOLDkHMg', 'users on world bank datasets')
users_on_unece_datasets = users_by_segment('gaid::5jjFNk42RVyxr6PJraLzQQ', 'users on unece datasets')
users_on_search_page = users_by_segment('gaid::PmciwK4hSriZ9eEdwl2Gsw', 'users on search page')
users_on_collections_pages = users_by_segment('gaid::jpG6-A21Sl-fwcPkHoEYiQ', 'users on collections pages')
users_on_dashboard_page = users_by_segment('gaid::8xhvjoPMQZWRAkQ0oO9qeg', 'users on dashboard page')
users_on_pricing_page = users_by_segment('gaid::nPPZfz5gT_WWl7b64TYcUQ', 'users on pricing page')
users_on_showcase_pages_with_premium_data_offer = users_by_segment('gaid::N-mAkahiSH-kWx8-z_2dyw', 'users on showcase pages with premium offer')
users_on_pages_with_premium_data_offer = users_by_segment('gaid::9LO8-oi-RrGyNOL9GituBw', 'users on pages with premium offer')
time.sleep(50)
premium_offer_clicks_on_core_data = events_by_expression('/core/.+#premium-data', 'clicks on premium offer on core data')
premium_offer_clicks_on_sports_data = events_by_expression('/sports-data/.+#premium-data', 'clicks on premium offer on sports-data')
premium_offer_clicks_on_machine_learning = events_by_expression('/machine-learning/.+#premium-data', 'clicks on premium offer on machine-learning')
premium_offer_clicks_on_cryptocurrency_data = events_by_expression('/cryptocurrency/.+#premium-data', 'clicks on premium offer on cryptocurrency data')
premium_offer_clicks_on_s_and_p_500_data = events_by_expression('/s-and-p-500-financial-data/.+#premium-data', 'clicks on premium offer on s-and-p-500-financial-data')

premium_offer_clicks_on_world_bank_data = events_by_expression('/world-bank/.+#premium-data', 'clicks on premium offer on world-bank data')
premium_offer_clicks_on_unece_data = events_by_expression('/unece/.+#premium-data', 'clicks on premium offer on unece data')
premium_offer_clicks_on_search_page = events_by_expression('', 'clicks on premium offer on search page', 'Premium data offer clicks', 'search')
premium_offer_clicks_on_collections_pages = events_by_expression('datahub.io/collections.*#premium-data', 'clicks on premium offer on collections pages')
premium_offer_clicks_on_dashboard_page = events_by_expression('', 'clicks on premium offer on dashboard page', 'Premium data offer clicks', 'dashboard')
premium_offer_clicks_on_pricing_page = events_by_expression('Contact us', 'clicks on premium offer on pricing page', 'pricingPage', 'click')
clicks_on_showcase_pages = premium_offer_clicks_on_core_data \
    + premium_offer_clicks_on_sports_data \
    + premium_offer_clicks_on_machine_learning \
    + premium_offer_clicks_on_cryptocurrency_data \
    + premium_offer_clicks_on_s_and_p_500_data \
    + premium_offer_clicks_on_world_bank_data \
    + premium_offer_clicks_on_unece_data
clicks_total = clicks_on_showcase_pages \
    + premium_offer_clicks_on_search_page \
    + premium_offer_clicks_on_collections_pages \
    + premium_offer_clicks_on_dashboard_page \
    + premium_offer_clicks_on_pricing_page
time.sleep(50)
form_submissions_core = events_by_expression('core.*', 'Premium offer form submissions from core data', 'Premium data form submissions', 'success')
form_submissions_sports_data = events_by_expression('sports-data.*', 'Premium offer form submissions from sports data', 'Premium data form submissions', 'success')
form_submissions_machine_learning = events_by_expression('machine-learning.*', 'Premium offer form submissions from machine-learning data', 'Premium data form submissions', 'success')
form_submissions_cryptocurrency = events_by_expression('cryptocurrency.*', 'Premium offer form submissions from cryptocurrency data', 'Premium data form submissions', 'success')
form_submissions_s_and_p_500_data = events_by_expression('s-and-p-500-financial-data.*', 'Premium offer form submissions from s-and-p-500-financial data', 'Premium data form submissions', 'success')
form_submissions_world_bank = events_by_expression('world-bank.*', 'Premium offer form submissions from world-bank data', 'Premium data form submissions', 'success')
form_submissions_unece = events_by_expression('unece.*', 'Premium offer form submissions from unece data', 'Premium data form submissions', 'success')
form_submissions_search_page = events_by_expression('^search', 'Premium offer form submissions from search page', 'Premium data form submissions', 'success')
form_submissions_collections_pages = events_by_expression('^collections', 'Premium offer form submissions from collections pages', 'Premium data form submissions', 'success')
form_submissions_dashboard_page = events_by_expression('^dashboard', 'Premium offer form submissions from dashboard page', 'Premium data form submissions', 'success')
form_submissions_pricing_page = events_by_expression('^pricing', 'Premium offer form submissions from pricing page', 'Premium data form submissions', 'success')
submissions_on_showcase_pages = form_submissions_core \
    + form_submissions_sports_data \
    + form_submissions_machine_learning \
    + form_submissions_cryptocurrency \
    + form_submissions_s_and_p_500_data \
    + form_submissions_world_bank \
    + form_submissions_unece
form_submissions_total = events_by_expression('', 'Premium offer form submissions', 'Premium data form submissions', 'success')

# Write to CSV (daily data):
with open('data/premium-data-funnel-overall-daily.csv', 'a', newline='') as csvfile:
    writer = csv.writer(csvfile, delimiter=',')
    writer.writerow([
        start_date,
        users_total,
        users_on_pages_with_premium_data_offer,
        users_on_showcase_pages_with_premium_data_offer,
        users_on_core_datasets,
        users_on_sports_data,
        users_on_machine_learning,
        users_on_cryptocurrency_datasets,
        users_on_s_and_p_500_datasets,
        users_on_world_bank_datasets,
        users_on_unece_datasets,
        users_on_search_page,
        users_on_collections_pages,
        users_on_dashboard_page,
        users_on_pricing_page,
        clicks_total,
        clicks_on_showcase_pages,
        premium_offer_clicks_on_core_data,
        premium_offer_clicks_on_sports_data,
        premium_offer_clicks_on_machine_learning,
        premium_offer_clicks_on_cryptocurrency_data,
        premium_offer_clicks_on_s_and_p_500_data,
        premium_offer_clicks_on_world_bank_data,
        premium_offer_clicks_on_unece_data,
        premium_offer_clicks_on_search_page,
        premium_offer_clicks_on_collections_pages,
        premium_offer_clicks_on_dashboard_page,
        premium_offer_clicks_on_pricing_page,
        form_submissions_total,
        submissions_on_showcase_pages,
        form_submissions_core,
        form_submissions_sports_data,
        form_submissions_machine_learning,
        form_submissions_cryptocurrency,
        form_submissions_s_and_p_500_data,
        form_submissions_world_bank,
        form_submissions_unece,
        form_submissions_search_page,
        form_submissions_collections_pages,
        form_submissions_dashboard_page,
        form_submissions_pricing_page
    ])

# Script for converting daily to weekly (last week):
with open('data/premium-data-funnel-overall-daily.csv', 'r') as daily:
    reader=csv.reader(daily)
    rows=[r for r in reader]
    with open('data/premium-data-funnel-overall-weekly.csv', 'a', newline='') as weekly:
        writer = csv.writer(weekly, delimiter=',')
        weekly_row = [0] * 41
        for i in range(7):
            day = rows[len(rows)-1-i]
            if i == 0:
                weekly_row[0] = day[0]
            for k in range(1,41):
                if day[k]:
                    weekly_row[k] += int(day[k])
        writer.writerow(weekly_row)


# with open('data/premium-data-funnel-overall-daily.csv', 'r') as daily:
#     reader=csv.reader(daily)
#     rows=[r for r in reader]
#     with open('data/premium-data-funnel-overall-weekly.csv', 'a', newline='') as weekly:
#         writer = csv.writer(weekly, delimiter=',')
#         for i in range(1,2):
#             weekly_row = [0] * 41
#             for j in range(1,8):
#                 day = rows[i+j]
#                 if j == 7:
#                     weekly_row[0] = day[0]
#                 for k in range(1,41):
#                     if day[k]:
#                         weekly_row[k] += int(day[k])
#             writer.writerow(weekly_row)
