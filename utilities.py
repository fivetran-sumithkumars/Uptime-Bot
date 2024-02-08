@app.message("connector uptime")
def message_hello(message, say):
    logger.info(message)
    say(f"Hey there <@{message['user']}>!, we have developed new connector uptime bot that checks the connector uptime daily for you and reports any affected connectors")


@app.command("/start")
def start_bot(respond,ack,body):
    logger.info(body)
    ack()
    respond(
        blocks = [
            {
			"type": "input",
			"element": {
				"type": "plain_text_input",
				"action_id": "plain_text_input-action"
			},
			"label": {
				"type": "plain_text",
				"text": "Enter the integrtion service id one below the other to monitor the uptime",
				"emoji": True
			}
		},
		{
			"type": "actions",
			"elements": [
				{
					"type": "button",
					"text": {
						"type": "plain_text",
						"text": "Submit",
						"emoji": True
					},
					"value": "submit",
					"action_id": "actionId-submit"
				}
			]
		}
        ]
    )
    
    
@app.action("actionId-submit")
def update_message(say,ack, body, respond):
    ack()
    connector_ids=body["state"]["values"]["iR8yt"]["plain_text_input-action"]["value"].split(',')
    channel_id=body['channel']['id']
    print(connector_ids)
    print(channel_id)
    # schedule_message(channel_id,connector_ids)
    respond("Uptime will be tracked for ===>  "+' '.join(map(str, connector_ids))+"\n"+construct_message(connector_ids))

