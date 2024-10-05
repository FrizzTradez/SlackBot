import os
import slack
import logging
from datetime import datetime, time
from zoneinfo import ZoneInfo
from logs.Logging_Config import setup_logging
from dotenv import load_dotenv

load_dotenv()
setup_logging()
logger = logging.getLogger(__name__)

class Base_Conditional:
    slack_channels = {
        'ES': 'alert_es',
        'NQ': 'alert_nq',
        'RTY': 'alert_rty',
        'CL': 'alert_cl'
    }
    product_color = {
        'ES': 'blue',
        'NQ': 'green',
        'RTY': 'orange',
        'CL': 'purple'
    }
    
    def __init__(self, product_name, variables):
        self.product_name = product_name
        self.variables = variables
        slack_token = os.getenv("SLACK_TOKEN") 
        self.slack_client = slack.WebClient(token=slack_token)
        
        # TimeZone Setup
        self.est = ZoneInfo('America/New_York')
        self.current_datetime = datetime.now(self.est)
        self.current_time = self.current_datetime.time()
        
        # Time Regulations for Equity Products
        self.equity_open = time(9, 30) 
        self.equity_ib = time(10, 30)
        self.equity_lunch_start = time(12, 00)
        self.equity_lunch_end = time(14, 00)
        self.equity_close = time(16, 00)

        # Time Regulations for Crude 
        self.crude_open = time(9, 00)
        self.crude_ib = time(10, 00)
        self.crude_close = time(14, 30) 
        
        # Custom Time Regulations for Playbook
        self.crude_pvat_start = time(9, 2)
        self.equity_pvat_start = time(9, 32)
        
    def send_slack_message(self, channel, message):
        if channel:
            try:
                response = self.slack_client.chat_postMessage(channel=channel, text=message) 
                logger.info(f"| Slack Response: {response['ts']}|")
                
            except Exception as e:
                logger.error(f"Failed to send message to {channel}: {e}")
        else:
            logger.warning(f"No Slack channel configured for the product.")