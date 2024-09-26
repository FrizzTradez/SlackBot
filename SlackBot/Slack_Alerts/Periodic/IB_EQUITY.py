import logging
import math
from datetime import datetime
from SlackBot.External import External_Config
from logs.Logging_Config import setup_logging
from SlackBot.Slack_Alerts.Periodic.Base import Base_Periodic

setup_logging()
logger = logging.getLogger(__name__)

class IB_Equity_Alert(Base_Periodic):
    def __init__(self, files):
        super().__init__(files)
        
    def ib_check_in_equity(self): # need to do logging
        for product_name in ['ES', 'NQ', 'RTY']:
            variables = self.fetch_latest_variables(product_name) 
            if not variables:
                print(f"No data available for {product_name}")
                continue
            
            product = product_name
            color = self.product_color.get(product_name) 
            ib_type = print("do calculations")
            ib_range = print("do calculations") 
            ib_vatr = print("do calculations") 
            exhausted = print("Do Calculations") 
            range_used = print("do Calculations")
            range_up = print("do calculations")
            range_down = print("do Calculations")
            gap = print("do calculations")
            gap_tier = print("do calculations") 
            rvol = variables.get(f'{product_name}_CUMULATIVE_RVOL')
            posture = print("do calculations") 

            message = (
                f">:large_{color}_square: *{product} - Alert - IB Check-In* :large_{color}_square:\n"
                "──────────────────────\n"
                f"*{ib_type}*: {ib_range}p = {ib_vatr} of Avg\n"
                f"*Expected Range*: _{exhausted}_ {range_used} Used\n"
                "────────────────\n"
                f"*Rng Up*: _{range_up}_\n"
                f"*Rng Down*: _{range_down}_\n"
                "────────────────\n"
                f"*{gap}* = _{gap_tier}_\n"
                f"*RVOL*: {rvol}\n"
                f"*Current Posture*: {posture}\n"
                "──────────────────────\n"
                f">*Alert Time*: _{self.current_time}_\n"
            )
            
            channel = self.slack_channels.get(product_name)
            if channel:
                self.slack_client.chat_postMessage(channel=channel, text=message) 
                print(f"Message send to {channel} for {product_name}")
            else:
                print(f"No Slack channel configured for {product_name}")
                