import investpy
import pandas as pd
import slack
from datetime import datetime
from dotenv import load_dotenv
from SlackBot.Slack_Alerts.Periodic.Base import Base_Periodic

class Economic(Base_Periodic):
    def __init__(self, files):
        super().__init__(files)

    def send_alert(self):
        # Get Today's Data Only
        today = datetime.now()
        today_str = today.strftime('%m/%d/%Y')
        print(f"Fetching economic data for {today_str}...")

        # Get the economic calendar for the day
        try:
            calendar = investpy.news.economic_calendar(
                time_zone=None,
                time_filter='time_only',
                countries=['united states'],
                importances=['high', 'medium'],
                categories=None,
                from_date=None,  # Defaults to today's data
                to_date=None
            )
        except Exception as e:
            print(f"Error fetching economic calendar: {e}")
            return

        # Convert to DataFrame and manipulate data
        print("Manipulating Dataframe...")
        calendar_df = pd.DataFrame(calendar)
        columns_to_keep = ['time', 'event', 'importance']
        calendar_df = calendar_df[columns_to_keep]

        # Store the formatted events in a list
        formatted_events = [self.format_event(row) for _, row in calendar_df.iterrows()]

        # Join the events into a single message with line breaks
        formatted_message = "\n".join(formatted_events) if formatted_events else "No significant events today."

        print("Sending to Slack...")
        # Send the formatted message
        slack_channel = "econ_outlook"  # Replace with your actual Slack channel
        header_message = (
            f":black_large_square: *Economic Outlook for {today_str}* :black_large_square:\n"
            "───────────────────────────\n"
        )
        footer_message = "\n───────────────────────────\n`Control Risk and Trade Well!`"

        full_message = f"{header_message}{formatted_message}{footer_message}"

        self.send_slack_message(slack_channel, full_message)
        print("Completed...")

    def format_event(self, row):
        
        # Extract event details
        event_time = row.get('time', 'N/A')
        event_name = row.get('event', 'N/A')
        importance = row.get('importance', 'N/A').capitalize()

        # Map importance levels to emojis
        importance_emojis = {
            'High': ':red_circle:',
            'Medium': ':large_orange_diamond:',
            'Low': ':white_circle:'
        }
        importance_emoji = importance_emojis.get(importance, ':grey_question:')

        # Construct a formatted string for each event
        formatted_event = f">{importance_emoji}  *{event_time}* - *{event_name}*"
        return formatted_event