
# coding: utf-8

# In[1]:


import argparse
import datetime as DT
import csv

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


def get_report(analytics, dimensions=[], metrics=[], filters=[]):
    # Use the Analytics Service Object to query the Analytics Reporting API V4.
    return analytics.reports().batchGet(
        body={
        'reportRequests': [
            {
                'viewId': VIEW_ID,
                'dateRanges': [{'startDate': startDate, 'endDate': endDate}],
                'dimensions': dimensions,
                'metrics': metrics,
                'dimensionFilterClauses': filters
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


# In[91]:


today = DT.date.today()
initial = today - DT.timedelta(days=1)
startDate = initial.strftime('%Y-%m-%d')
endDate = startDate
analytics = initialize_analyticsreporting()

row = [0] * 7
row[0] = startDate

def incoming_user():
    dimensions = []
    metrics = [{'expression': 'ga:uniquePageviews'}]
    filters = [
        {'filters': [{'dimensionName': 'ga:pagePath', 'expressions': '/core/.+'}]}
    ]
    response = get_report(analytics, dimensions, metrics, filters)
    value = print_response(response, 'incoming user')
    row[1] = value or 0

incoming_user()

def interacting_user():
    dimensions = []
    metrics = [{'expression': 'ga:uniqueEvents'}]
    filters = [
        {'filters': [{'dimensionName': 'ga:eventCategory', 'expressions': 'scroll depth'}]},
        {'filters': [{'dimensionName': 'ga:eventLabel', 'expressions': '25'}]}
    ]
    response = get_report(analytics, dimensions, metrics, filters)
    value = print_response(response, 'interacting user')
    row[2] = value or 0

    filters = [
        {'filters': [{'dimensionName': 'ga:eventLabel', 'expressions': '/core/.+'}]},
        {'filters': [{'dimensionName': 'ga:pagePath', 'expressions': '/core/.+'}]}
    ]
    response = get_report(analytics, dimensions, metrics, filters)
    value = print_response(response, 'heavily interacting user')
    row[3] = value or 0

interacting_user()

def interested_user():
    dimensions = []
    metrics = [{'expression': 'ga:uniqueEvents'}]
    filters = [
        {'filters': [{'dimensionName': 'ga:eventCategory', 'expressions': 'showcase'}]},
        {'filters': [{'dimensionName': 'ga:eventAction', 'expressions': 'click'}]},
        {'filters': [{'dimensionName': 'ga:eventLabel', 'expressions': 'premium-data-offer'}]}
    ]
    response = get_report(analytics, dimensions, metrics, filters)
    value = print_response(response, 'interested user')
    row[4] = value or 0

interested_user()

with open('data/premium-data-funnel-daily.csv', 'a', newline='\n') as csvfile:
    writer = csv.writer(csvfile, delimiter=',')
    writer.writerow(row)

# Script for converting daily to weekly (last week):
with open('data/premium-data-funnel-daily.csv', 'r') as daily:
    reader = csv.reader(daily)
    rows = [r for r in reader]
    with open('data/premium-data-funnel-weekly.csv', 'a', newline='\n') as weekly:
        writer = csv.writer(weekly, delimiter=',')
        weekly_row = [0] * 7
        for i in range(7):
            day = rows[len(rows)-1-i]
            if i == 0:
                weekly_row[0] = day[0]
            for k in range(1,7):
                if day[k]:
                    weekly_row[k] += int(day[k])
        writer.writerow(weekly_row)
