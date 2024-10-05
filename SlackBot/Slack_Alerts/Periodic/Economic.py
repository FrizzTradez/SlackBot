import investpy
import pandas as pd
import slack
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
        formatted_message = "\n".join(formatted_events) if formatted_events else "No significant events today."

        slack_channel = "econ_outlook"  
        header_message = (
            f":black_large_square: *Economic Outlook for {today_str}* :black_large_square:\n"
            "───────────────────────────\n"
        )
        footer_message = "\n───────────────────────────\n`Control Risk and Trade Well!`"

        full_message = f"{header_message}{formatted_message}{footer_message}"

        self.send_slack_message(slack_channel, full_message)
        logger.info(f" ECON | send_alert | Channel: econ_outlook | Note: Message Sent")

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