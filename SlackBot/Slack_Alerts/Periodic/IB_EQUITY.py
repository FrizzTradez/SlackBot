import logging
import math
from datetime import datetime
from SlackBot.External import External_Config
from SlackBot.Slack_Alerts.Periodic.Base import Base_Periodic

logger = logging.getLogger(__name__)

class IB_Equity_Alert(Base_Periodic):
    def __init__(self, files):
        super().__init__(files)
        
# ---------------------- Specific Calculations ------------------------- #
    def ib_info(self):
        
        print("Return IB Type, IB Range , IB Vatr")
        
    def exp_range_info(self):
        print("Return Exhausted , Range used, range up, range down, ")

    def gap_info(self):
        print("Return Gap Tier, Gap Amount, IF Gap(Tier Closed?)")

    def posture(self):
        print("Postures")

    def open_type(self):
        print("Open Type of the session")
# ---------------------- Alert Preparation ------------------------- #
    def send_alert(self):
        for product_name in ['ES', 'NQ', 'RTY']:
            self.variables = self.fetch_latest_variables(product_name) 
            if not self.variables:
                print(f"No data available for {product_name}")
                continue
            
            # Raw Variables
            self.product_name = product_name
            # Message Variables
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
            rvol = self.variables.get(f'{self.product_name}_CUMULATIVE_RVOL')
            posture = print("do calculations") 
            open_type = print()
            
            # Message Template
            message = (
                f">:large_{color}_square: *{self.product_name} - Alert - IB Check-In* :large_{color}_square:\n"
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
            
            # Send Slack Alert
            channel = self.slack_channels.get(self.product_name)
            if channel:
                self.slack_client.chat_postMessage(channel=channel, text=message) 
                print(f"Message send to {channel} for {self.product_name}")
            else:
                print(f"No Slack channel configured for {self.product_name}")
                