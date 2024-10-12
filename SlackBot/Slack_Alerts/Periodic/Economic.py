import investpy
import pandas as pd
from slack_sdk.errors import SlackApiError
from datetime import datetime
from dotenv import load_dotenv
from SlackBot.Slack_Alerts.Periodic.Base import Base_Periodic
import logging

logger = logging.getLogger(__name__)

class Economic(Base_Periodic):
    def __init__(self, files):
        super().__init__(files)

    def send_alert(self):
        logger.debug(f" ECON | send_alert | Note: Running")
        today = datetime.now()
        today_str = today.strftime('%m/%d/%Y')
        logger.debug(f" ECON | send_alert | Note: Fetching economic data for {today_str}...")

        try:
            calendar = investpy.news.economic_calendar(
                time_zone=None,
                time_filter='time_only',
                countries=['united states'],
                importances=['high', 'medium'],
                categories=None,
                from_date=None,  
                to_date=None
            )
        except Exception as e:
            logger.debug(f" ECON | send_alert | Note: Error fetching economic calendar {e}")
            return

        logger.debug(f" ECON | send_alert | Note: Manipulating Dataframe")
        
        calendar_df = pd.DataFrame(calendar)
        columns_to_keep = ['time', 'event', 'importance']
        calendar_df = calendar_df[columns_to_keep]
        
        formatted_events = [self.format_event(row) for _, row in calendar_df.iterrows()]
        if not formatted_events:
            formatted_events = ["No significant events today."]
        
        slack_channel = "#econ_outlook"  # Ensure the channel name is correct and includes the '#'

        # Construct the blocks
        block = []

        # Header block
        block.append({
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"Economic Outlook for {today_str}",
                "emoji": True
            }
        })

        # Divider
        block.append({"type": "divider"})

        # Event blocks
        for event in formatted_events:
            block.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": event
                }
            })

        # Footer block
        block.append({"type": "divider"})
        block.append({
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "`Control Risk and Trade Well!`"
                }
            ]
        })

        # Send the message
        try:
            response = self.client.chat_postMessage(
                channel=slack_channel,
                blocks=block,
                text=f"Economic Outlook for {today_str}"  # Fallback text
            )
            logger.info(f" ECON | send_alert | Channel: {slack_channel} | Note: Message Sent")
        except SlackApiError as e:
            logger.error(f"Error sending message: {e.response['error']}")
            
    def format_event(self, row):
        logger.debug(f" ECON | format_event | Note: Running")
        
        event_time = row.get('time', 'N/A')
        event_name = row.get('event', 'N/A')
        importance = row.get('importance', 'N/A').capitalize()

        importance_emojis = {
            'High': ':red_circle:',
            'Medium': ':large_orange_diamond:',
            'Low': ':white_circle:'
        }
        
        importance_emoji = importance_emojis.get(importance, ':grey_question:')
        formatted_event = f">{importance_emoji}  *{event_time}* - *{event_name}*"
        
        return formatted_event