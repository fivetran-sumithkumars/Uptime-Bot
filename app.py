import os
import datetime
import logging
import pandas as pd
from datetime import datetime,timedelta
import json
import base64
import requests
from slack_bolt import App
from dotenv import load_dotenv
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk.errors import SlackApiError
from google.cloud import bigquery

load_dotenv()
SLACK_BOT_TOKEN =  os.getenv('SLACK_BOT_TOKEN')
SLACK_SIGNING_SECRET = os.getenv('SLACK_SIGNING_SECRET')

app = App(token=SLACK_BOT_TOKEN)
logger = logging.getLogger(__name__)
bq_client = bigquery.Client()

input_channel_ids = "channel_ids"
input_query = "query"
channel = "channel"


def get_connector_ids_in_query(connector_ids):
    condition_list = []
    for service in connector_ids:
        condition = f"UPPER((connections_timeline.service_detail)) = UPPER('{service}')"
        condition_list.append(condition)
    result_string = ' OR '.join(condition_list)
    return result_string

def construct_query(connector_ids):
    start_of_query='''WITH accounts_timeline AS (select * from `digital-arbor-400`.transforms_bi.sf_account_timeline),  
    connections_timeline AS (select * from `digital-arbor-400`.transforms_bi.connections_timeline),
    connections AS (select * from `digital-arbor-400`.transforms_bi.connections)
    SELECT
        connections.pg_account_name  AS account_name,
        connections.group_name  AS group_name,
        connections.service_detail  AS service,
        connections.integration_schema_name  AS schema,
        1 - safe_divide(sum(connections_timeline.fivetran_caused_downtime_minutes), sum(connections_timeline.total_time_minutes))
        AS percent_uptime
    FROM `digital-arbor-400`.transforms_bi.accounts  AS accounts
    LEFT JOIN accounts_timeline ON accounts_timeline.sf_account_id = accounts.id
    LEFT JOIN connections_timeline ON accounts_timeline.date = connections_timeline.date
        and accounts_timeline.sf_account_id = connections_timeline.sf_account_id
    LEFT JOIN connections ON connections_timeline.connector_id= connections.connector_id
    WHERE ((accounts.pbf_account_c OR (NOT COALESCE(accounts.pbf_account_c , FALSE)))) 
    AND ((('''
    
    end_of_query=''')) AND (NOT (connections._integration_deleted    ) OR (connections._integration_deleted    ) IS NULL)) AND ((connections.integration_updated ) < ((TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Los_Angeles'), 'America/Los_Angeles'), INTERVAL 0 DAY), 'America/Los_Angeles'))) AND (NOT (connections.paused ) OR (connections.paused ) IS NULL) AND (((UPPER(( connections.integration_status  )) = UPPER('failed'))) AND ((UPPER(( connections.pg_account_status )) = UPPER('Customer'))))) 
    GROUP BY 1,2,3,4 having percent_uptime < 1 order by 3 '''

    QUERY = start_of_query+get_connector_ids_in_query(connector_ids)+end_of_query
    
    print(QUERY)

    return QUERY

def getHeightTicketCreationLink(row):
    return f'=HYPERLINK("https://fivetran.height.app/?newTask=saha-connector-uptime-exclusions-STwOMcjPhK3o","Create exclusion ticket")'

def get_URL(connector_string):

    end = int(datetime.utcnow().timestamp() * 1000)
    yesterday = datetime.utcnow() - timedelta(days=1)
    begin = int(yesterday.timestamp() * 1000)
    
    NR_ACCOUNT_FIVERAN_PRODUCTION = "2880887"

    query = "labels.app.kubernetes.io/name:\"donkey\" OR labels.app:donkey labels.connector-detail:'%s' level:SEVERE" % (connector_string)
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
    print(f"Getting error from new relic started for {row['Connector String']}.......")
    url = "https://api.newrelic.com/graphql"
    query=f"SELECT `exception` FROM Log WHERE `labels.app.kubernetes.io/name` = 'donkey' OR `labels.app` = 'donkey' AND `annotations.connector-detail` = '{row['Connector String']}' AND `level` = 'SEVERE' SINCE 24 hours ago limit 1"
    payload = "{\"query\":\"query {\\n  actor {\\n    account(id: 2880887) {\\n      name\\n      nrql(query: \\\"%s\\\") {\\n        results\\n      }\\n    }\\n  }\\n}\",\"variables\":{}}" %(query)
    headers = {
    'Content-Type': 'application/json',
    'API-Key': os.getenv('NEW_RELIC_KEY_FIVETRAN')
    }
    try:
        request = requests.post(url, headers=headers, data=payload)
        response=request.json()
        results=response['data']['actor']['account']['nrql']['results']
        if results:
            first_result = results[0]
            exception = first_result.get("exception", None)

        if exception:
            # Log the exception for debugging
            print(f"NRQL exception found: {exception}")
            return exception

        else:
            return "Could not fetch error message, Please check using new relic log link"
    
    except Exception as e:
        return "Could not fetch error message, Please check using new relic log link"
    
def processDataFrame(df):
    print("Processing the data frame started.......")
    df['Connector String']=df.apply(getConnectorString, axis=1)
    df['New Relic Log Link'] = df.apply(getConnectorString, axis=1)
    df['New Relic Log Link'] = df['New Relic Log Link'].apply(lambda x: f'=HYPERLINK("{get_URL(x)}", "{x}")')

    df['Error Message']=df.apply(getErrorMessage,axis=1)
    df=df.drop(['account_name'], axis=1)
    df=df.drop(['group_name'], axis=1)
    # df=df.drop(['service'], axis=1)
    df=df.drop(['schema'], axis=1)

    df['Create Exclusion Height Ticket'] = df.apply(getHeightTicketCreationLink, axis=1)
    df = df.reindex(columns=['service','Connector String', 'percent_uptime','Error Message','New Relic Log Link','Create Exclusion Height Ticket'])
    print("Processing the data frame completed.......")
    return df

def run_query_and_get_results(QUERY):
    query_job = bq_client.query(QUERY)  
    print("Query executed successfully......")
    df = query_job.result().to_dataframe()
    df=processDataFrame(df)
    print(df.to_csv('connector_uptime.csv',index=False))
    
def execute(connector_ids):
    QUERY = construct_query(connector_ids)
    run_query_and_get_results(QUERY)
    
def schedule_message(channel_id,connector_ids):
    # Create a timestamp for tomorrow at 9AM
    tomorrow = datetime.date.today() + datetime.timedelta(days=1)
    scheduled_time = datetime.time(hour=9, minute=30)
    schedule_timestamp = datetime.datetime.combine(tomorrow, scheduled_time).strftime('%s')

    try:
        # Call the chat.scheduleMessage method using the WebClient
        execute(connector_ids)
        app.client.files_upload_v2(
            file = 'connector_uptime.csv',
            channel = channel,
            initial_comment="Hey Team, here is the list of failing connectors for today. Please investigate and create exclusion tickets if necessary!!!")
    except SlackApiError as e:
        logger.error("Error scheduling message: {}".format(e))

@app.error
def custom_error_handler(error, body, logger):
    print(f"*************Error: {error}******************")

    
@app.shortcut("uptime-bot")
def open_modal(ack, shortcut, client):
    ack()
    blocks = []     
    channels_block = {
        "type": "input",
        "block_id": input_channel_ids,
        "label": {"type": "plain_text", "text": "Channel to post"},
        "element": {
            "type": "channels_select",
            "placeholder": {
                "type": "plain_text",
                "text": "Select your channel to post the connector uptime",
            },
            "action_id": "_",
        },
    }
    query_block = {
        "type": "input",
        "block_id": input_query,
        "optional": True,
        "element": {
            "type": "plain_text_input",
            "action_id": "_",
            "placeholder": {
                "type": "plain_text",
                "text": "Eg: stripe,shopify,sage_intacct,workday_hcm",
            },
        },
        "label": {
            "type": "plain_text",
            "text": "Connector service ids(Separate multiple terms by a comma)",
        },
    }

    blocks.append(channels_block)
    blocks.append(query_block)

    client.views_open(
        trigger_id=shortcut["trigger_id"],
        view={
            "type": "modal",
            "callback_id": "uptime_view",
            "title": {
                "type": "plain_text",
                "text": "Connector Uptime Bot"
	        },
            "submit": {
                "type": "plain_text",
                "text": "Submit"
            },
            "blocks": blocks
        }
    )
    
@app.view("uptime_view")
def handle_view_submission_events(ack, body,client,view,logger):
    user = body["user"]["username"]
    channel=view["state"]["values"]["channel_ids"]["_"]["selected_channel"]
    connector_ids_list=view["state"]["values"]["query"]["_"]["value"]
    ack()
    msg = ""
    try:
        msg = f"Hey {user}, Your submission was successful. Uptime for {connector_ids_list} will be tracked every day at 9 AM IST."
        # schedule_message(channel,connector_ids_list.split(','))
        pass
    except Exception as e:
        msg = "There was an error with your submission, Please check and try again"

    
    try:
        # client.chat_postMessage(channel=channel, text=msg)
        execute(connector_ids_list.split(','))
        print(f"Uploading file to channel {channel}......")
        app.client.files_upload_v2(
            file = 'connector_uptime.csv',
            channel = channel,
            initial_comment="Hey Team, here is the list of failing connectors for today. Please investigate and create exclusion tickets if necessary!!!\n Dowload and view the file for better experience...")
    
        # app.client.files_upload(
        #     file = 'connector_uptime.csv',
        #     channel = channel,
        #     title = 'Connector Uptime',
        #     initial_comment = 'Here is the list of failing connectors data from BigQuery')
        print("Uploading file completed ......")
    except Exception as e:
        logger.exception(f"Failed to post a message {e}")

if __name__ == "__main__":
    SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"]).start()

