import pandas as pd
from datetime import datetime,timedelta
import json
import base64
import requests

df=pd.read_csv('connector_uptime.csv')
print(df)

# def getHeightTicketCreationLink(row):
#     return f'=HYPERLINK("https://fivetran.height.app/?newTask=saha-connector-uptime-exclusions-STwOMcjPhK3o)","Create exclusion ticket")'

def get_URL(connector_string):

    end = int(datetime.utcnow().timestamp() * 1000)
    yesterday = datetime.utcnow() - timedelta(days=1)
    begin = int(yesterday.timestamp() * 1000)
    
    NR_ACCOUNT_FIVERAN_PRODUCTION = "2880887"

    query = f"labels.app.kubernetes.io/name:'donkey' OR labels.app:donkey OR origin:donkey level:SEVERE labels.connector-detail:'{connector_string}'"
    attrs = ['timestamp', 'level', 'event_summary', 'message', 'exception', 'time']

    data = {
        'query': query,
        'begin': begin,
        'end': end,
        'isEntitled': True,
        'attrs': attrs
    }

    json_string = json.dumps(data)
    base64_encoded = base64.b64encode(json_string.encode()).decode()

    launcher = base64_encoded

    url = (f"https://one.newrelic.com/launcher/logger.log-launcher?"
        f"launcher={launcher}&platform[accountId]={NR_ACCOUNT_FIVERAN_PRODUCTION}"
        f"&platform[timeRange][begin_time]={begin}&platform[timeRange][end_time]={end}")
    
    return url

def getConnectorString(row):
    connector_string=f"{row['account_name']}/{row['group_name']}/{row['service']}/{row['schema']}"
    return connector_string

def getErrorMessage(row):
    url = "https://api.newrelic.com/graphql"
    query=f"SELECT `exception` FROM Log WHERE `labels.app.kubernetes.io/name` = 'donkey' OR `labels.app` = 'donkey' AND `annotations.connector-detail` = '{row['Connector String']}' AND `level` = 'SEVERE' SINCE 24 hours ago limit 1"
    payload = "{\"query\":\"query {\\n  actor {\\n    account(id: 2880887) {\\n      name\\n      nrql(query: \\\"%s\\\") {\\n        results\\n      }\\n    }\\n  }\\n}\",\"variables\":{}}" %(query)
    headers = {
    'Content-Type': 'application/json',
    'API-Key': 'NRAK-UPR04FJ6GGYHB9XC1ZQS3USWA5I'
    }
    try:
        request = requests.post(url, headers=headers, data=payload)
        response=request.json()
        print(response)
        return response['data']['actor']['account']['nrql']['results'][0]['exception']
    except TypeError:
        return "error"

def getIntegrationId(row):
    return row['connector_id'].split(' - ')[0].strip('[]')

df = df.drop(columns=df.columns[0])
# df['Connector String']=df.apply(getConnectorString, axis=1)
# df['New Relic Log Link'] = df.apply(getConnectorString, axis=1)
# df['New Relic Log Link'] = df['New Relic Log Link'].apply(lambda x: f'=HYPERLINK("{get_URL(x)}", "{x}")')
df['Error Message']=df.apply(getErrorMessage,axis=1)

# df=df.drop(['account_name'], axis=1)
# df=df.drop(['group_name'], axis=1)
# df=df.drop(['service'], axis=1)
# df=df.drop(['schema'], axis=1)

# df['Create Exclusion Height Ticket'] = df.apply(getHeightTicketCreationLink, axis=1)
# df = df.reindex(columns=['Connector String', 'percent_uptime', 'New Relic Log Link', 'Create Exclusion Height Ticket'])

print(df.to_csv('testing.csv',index=False))