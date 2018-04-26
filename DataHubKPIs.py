
# coding: utf-8

# In[29]:


import argparse

from apiclient.discovery import build
import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools

import re
import itertools


# In[30]:


SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
DISCOVERY_URI = ('https://analyticsreporting.googleapis.com/$discovery/rest')
CLIENT_SECRETS_PATH = 'client_secrets.json' # Path to client_secrets.json file.
VIEW_ID = '157208265'


# In[31]:


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


# In[32]:


startDate = 'yesterday'


# In[33]:


def get_all_new_users(analytics):
    # Use the Analytics Service Object to query the Analytics Reporting API V4.
    return analytics.reports().batchGet(
        body={
        'reportRequests': [
            {
                'viewId': VIEW_ID,
                'dateRanges': [{'startDate': startDate, 'endDate': 'today'}],
                'dimensions': [],
                'metrics': [{'expression': 'ga:newUsers'}],
            }]
        }
    ).execute()


# In[34]:


def get_new_users_event_report(analytics):
    # Use the Analytics Service Object to query the Analytics Reporting API V4.
    return analytics.reports().batchGet(
        body={
        'reportRequests': [
            {
                'viewId': VIEW_ID,
                'dateRanges': [{'startDate': startDate, 'endDate': 'today'}],
                'dimensions': [{'name': 'ga:eventCategory'}, {'name': 'ga:eventAction'}, {'name': 'ga:eventLabel'}],
                'metrics': [{'expression': 'ga:newUsers'}],
            }]
        }
    ).execute()


# In[35]:


def get_all_pages(analytics):
    # Use the Analytics Service Object to query the Analytics Reporting API V4.
    return analytics.reports().batchGet(
        body={
        'reportRequests': [
            {
                'viewId': VIEW_ID,
                'dateRanges': [{'startDate': startDate, 'endDate': 'today'}],
                'dimensions': [{'name': 'ga:pagePath'}],
                'metrics': [],
            }]
        }
    ).execute()


# In[36]:


def parse_response(response):
    """Parses and prints the Analytics Reporting API V4 response"""

    for report in response.get('reports', []):
        columnHeader = report.get('columnHeader', {})
        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
        rows = report.get('data', {}).get('rows', [])

        for row in rows:
            dimensions = row.get('dimensions', [])
            dateRangeValues = row.get('metrics', [])

            desc = {}
            ret = [desc]
            
            for header, dimension in zip(dimensionHeaders, dimensions):
                desc[header] = dimension

            values = dateRangeValues[0].get('values')
            ret.append(dict(
                (metricHeader.get('name'), int(value))
                for metricHeader, value in zip(metricHeaders, values)
            ))
            
            yield ret


# In[37]:


def event(cat, act, lbl):
    cat, act, lbl = re.compile(cat), re.compile(act), re.compile(lbl)
    def func(dims):
        return ('ga:eventCategory' in dims and
                cat.match(dims['ga:eventCategory']) and
                'ga:eventAction' in dims and
                act.match(dims['ga:eventAction']) and
                'ga:eventLabel' in dims and
                lbl.match(dims['ga:eventLabel']))
    return func


# In[38]:


def path(path):
    path = re.compile(path)
    def func(dims):
        return ('ga:pagePath' in dims and
                path.match(dims['ga:pagePath']))
    return func


# In[39]:


def nodims():
    def func(dims):
        return not dims
    return func


# In[75]:


import json
def get_npm_stats():
    h = httplib2.Http('.cache')
    resp, content = h.request('https://api.npmjs.org/downloads/point/last-week/data-cli', 'GET')
    dict_content = json.loads(content)
    return ('99 - NPM installs', dict_content['downloads'])


# In[76]:


FUNNEL = [
    ('Total new users in the site',
         nodims(),
         'ga:newUsers'),
    ('Clicks on "download" from anywhere', 
         event('outbound', 'click', 'https://datahub.io/download'), 
         'ga:newUsers'),
    ('CLI downloads (from web)', 
         event('outbound', 'click', 'https://github.com/datahq/data-cli/releases/download/.+'),
         'ga:newUsers'),
    ('First run of the CLI', 
         event('cli', 'first-run', '.*'),
         'ga:newUsers'),
    ('Total pushes from the CLI', 
         event('cli', 'push', '.*'), 
         'ga:newUsers'),
    ('Successful pushes from the CLI',
         event('cli', 'push-success', '.*'),
         'ga:newUsers'),
    ('First pushes from the CLI',
         event('cli', 'push-first', '.*'),
         'ga:newUsers'),
    ('Visit the showcase after push - method 1', 
         path('/[^/]+/[^/]+/v/[0-9]+'), 
         'count'),
    ('Visit the showcase after push - method 2', 
         event('showcase', 'visit', 'cli'), 
         'count'),
]


# In[77]:


def extract_funnel(rows):
    funnel = [
        ('%02d - %s' % (i, title),
         func,
         met)
        for i, (title, func, met) in enumerate(FUNNEL)
    ]
    ret = {}
    for dims, mets in rows:
        for title, func, met in funnel:
            if func(dims):
                if met in mets:
                    ret.setdefault(title, 0)
                    ret[title] += mets.get(met)
                elif met == 'count':
                    ret.setdefault(title, 0)
                    ret[title] += 1
            
    return sorted(list(ret.items()))


# In[78]:


analytics = initialize_analyticsreporting()
response = itertools.chain(*(
    parse_response(r(analytics))
    for r in [
        get_new_users_event_report,
        get_all_pages,
        get_all_new_users,
    ]
))
funnel = extract_funnel(response)
npm_stats = get_npm_stats()
funnel.append(npm_stats)
for t, a in funnel:
    print('%10d - %s' % (a, t))

