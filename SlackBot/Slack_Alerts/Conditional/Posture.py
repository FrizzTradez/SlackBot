import logging
import math
import threading
from datetime import datetime
from SlackBot.External import External_Config
from SlackBot.Slack_Alerts.Conditional.Base import Base_Conditional
from slack_sdk.models.blocks import SectionBlock, DividerBlock, ContextBlock, MarkdownTextObject

logger = logging.getLogger(__name__)

last_alerts = {}
last_alerts_lock = threading.Lock()

class POSTURE(Base_Conditional):
    def __init__(self, product_name, variables):    
        super().__init__(product_name, variables)
        
        # Variables (Round All Variables)
        self.cpl = round(self.variables.get(f'{self.product_name}_CPL'), 2)
        self.prior_close = round(self.variables.get(f'{self.product_name}_PRIOR_CLOSE'), 2)
        self.fd_vpoc = round(self.variables.get(f'{self.product_name}_5D_VPOC'), 2)
        self.td_vpoc = round(self.variables.get(f'{self.product_name}_20D_VPOC'), 2)
        
        self.es_impvol = External_Config.es_impvol
        self.nq_impvol = External_Config.nq_impvol
        self.rty_impvol = External_Config.rty_impvol
        self.cl_impvol = External_Config.cl_impvol 
        
        self.exp_rng = self.exp_range() 

# ---------------------------------- Specific Calculations ------------------------------------ #   
    def exp_range(self):
        logger.debug(f" POSTURE | exp_range | Product: {self.product_name} | Note: Running")

        # Calculation (product specific or Not)
        if not self.prior_close:
            logger.error(f" POSTURE | exp_range | Product: {self.product_name} | Note: No Close Found")
            raise ValueError(f" POSTURE | exp_range | Product: {self.product_name} | Note: Need Close For Calculation!")
        
        if self.product_name == 'ES':
            exp_range = round(((self.prior_close * (self.es_impvol/100)) * math.sqrt(1/252)) , 2)
            
            logger.debug(f" POSTURE | exp_range | Product: {self.product_name} | EXP_RNG: {exp_range}")
            return exp_range
        
        elif self.product_name == 'NQ':
            exp_range = round(((self.prior_close * (self.nq_impvol/100)) * math.sqrt(1/252)) , 2)
            
            logger.debug(f" POSTURE | exp_range | Product: {self.product_name} | EXP_RNG: {exp_range}")
            return exp_range
        
        elif self.product_name == 'RTY':
            exp_range = round(((self.prior_close * (self.rty_impvol/100)) * math.sqrt(1/252)) , 2)
        
            logger.debug(f" POSTURE | exp_range | Product: {self.product_name} | EXP_RNG: {exp_range}")
            return exp_range
        
        elif self.product_name == 'CL':
            exp_range = round(((self.prior_close * (self.cl_impvol/100)) * math.sqrt(1/252)) , 2)
            
            logger.debug(f" POSTURE | exp_range | Product: {self.product_name} | EXP_RNG: {exp_range}")
            return exp_range
        
        else:
            raise ValueError(f" POSTURE | exp_range | Product: {self.product_name} | Note: Unknown Product")
        
        
# ---------------------------------- Driving Input Logic ------------------------------------ #      
    def input(self):
        logger.debug(f" POSTURE | input | Note: Running")
        
        threshold = round((self.exp_rng * 0.68), 2)

        if (abs(self.cpl - self.fd_vpoc) <= threshold) and (abs(self.fd_vpoc - self.td_vpoc) <= threshold):
            posture = "PRICE=5D=20D"
        elif (self.cpl > self.fd_vpoc + threshold) and (self.fd_vpoc > self.td_vpoc + threshold):
            posture = "PRICE^5D^20D"
        elif (self.cpl < self.fd_vpoc - threshold) and (self.fd_vpoc < self.td_vpoc - threshold):
            posture = "PRICEv5Dv20D"
        elif (abs(self.cpl - self.fd_vpoc) <= threshold) and (self.fd_vpoc > self.td_vpoc + threshold):
            posture = "PRICE=5D^20D"
        elif (self.cpl > self.fd_vpoc + threshold) and (abs(self.fd_vpoc - self.td_vpoc) <= threshold):
            posture = "PRICE^5D=20D"
        elif (self.cpl < self.fd_vpoc - threshold) and (abs(self.fd_vpoc - self.td_vpoc) <= threshold):
            posture = "PRICEv5D=20D"
        elif (abs(self.cpl - self.fd_vpoc) <= threshold) and (self.fd_vpoc < self.td_vpoc - threshold):
            posture = "PRICE=5Dv20D"
        elif (self.cpl > self.fd_vpoc + threshold) and (self.fd_vpoc < self.td_vpoc - threshold):
            posture = "PRICE^5Dv20D"
        elif (self.cpl < self.fd_vpoc - threshold) and (self.fd_vpoc > self.td_vpoc + threshold):
            posture = "PRICEv5D^20D"
        else:
            posture = "Other"

        logger.debug(f" POSTURE | input | Current_Posture: {posture} | Price: {self.cpl} | 5DVPOC: {self.fd_vpoc} | 20DVPOC: {self.td_vpoc}") 

        return posture
# ---------------------------------- Opportunity Window ------------------------------------ #   
    def time_window(self):
        logger.debug(f" POSTURE | time_window | Product: {self.product_name} | Note: Running")
        
        # Update current time
        self.current_datetime = datetime.now(self.est)
        self.current_time = self.current_datetime.time()
        
        # Define time windows based on product type
        if self.product_name == 'CL':
            start_time = self.crude_open
            end_time = self.crude_close
            logger.debug(f" POSTURE | time_window | Product: {self.product_name} | Time Window: {start_time} - {end_time}")
        elif self.product_name in ['ES', 'RTY', 'NQ']:
            start_time = self.equity_open
            end_time = self.equity_close
            logger.debug(f" POSTURE | time_window | Product: {self.product_name} | Time Window: {start_time} - {end_time}")
        else:
            logger.warning(f" POSTURE | time_window | Product: {self.product_name} | No time window defined.")
            return False  
        
        # Check if current time is within the window
        if start_time <= self.current_time <= end_time:
            logger.debug(f" POSTURE | time_window | Product: {self.product_name} | Within Window: {self.current_time}.")
            return True
        else:
            logger.debug(f" POSTURE | time_window | Product: {self.product_name} | Outside Window {self.current_time}.")
            return False
        
    def check(self):
        logger.debug(f" POSTURE | check | Product: {self.product_name} | Note: Running")
        self.current_posture = self.input()
        logic = False

        with last_alerts_lock:
            self.last_posture = last_alerts.get(self.product_name)
            self.last_alert_time = last_alerts.get(f"{self.product_name}_alert_time")
            current_time = datetime.now()

            if self.last_posture is None:
                # First time, initialize posture and alert time
                last_alerts[self.product_name] = self.current_posture
                last_alerts[f"{self.product_name}_alert_time"] = current_time
                logger.debug(f" POSTURE | check | Product: {self.product_name} | Note: Initial posture set to {self.current_posture}")
                # Do not send an alert on initialization
            elif self.current_posture != self.last_posture:
                time_since_last_alert = (current_time - self.last_alert_time).total_seconds() if self.last_alert_time else None
                if self.last_alert_time is None or time_since_last_alert >= 1800:
                    # Posture has changed and at least 30 minutes have passed since last alert
                    logic = True
                    self.alert_reason = 'Posture Change after 30 minutes'
                    last_alerts[self.product_name] = self.current_posture
                    last_alerts[f"{self.product_name}_alert_time"] = current_time
                    logger.info(f" POSTURE | check | Product: {self.product_name} | Current_Posture: {self.current_posture} | Last_Posture: {self.last_posture} | Time Since Last Alert: {time_since_last_alert} seconds | Note: Posture Change Detected")
                else:
                    # Posture changed but not enough time has passed
                    logger.info(f" POSTURE | check | Product: {self.product_name} | Time Since Last Alert: {time_since_last_alert} seconds | Note: Posture changed but alert sent less than 30 minutes ago")
            else:
                # No posture change
                logger.info(f" POSTURE | check | Product: {self.product_name} | Note: No posture change")

        if logic and self.time_window():
            try:
                self.execute()
            except Exception as e:
                logger.error(f" POSTURE | check | Product: {self.product_name} | Note: Failed to send Slack alert: {e}")
        else:
            logger.info(f" POSTURE | check | Product: {self.product_name} | Current_Posture: {self.current_posture} | Note: No Alert Sent")
# ---------------------------------- Alert Preparation------------------------------------ # 
    def slack_message(self):
        logger.debug(f" POSTURE | slack_message | Product: {self.product_name} | Note: Running")
        
        pro_color = self.product_color.get(self.product_name)
        alert_time_formatted = self.current_datetime.strftime('%H:%M:%S') 
        
        blocks = []

        # Title Block
        title_text = f":large_{pro_color}_square:  *{self.product_name} - Context Alert - Pos*  :large_{pro_color}_square:"
        title_block = SectionBlock(text=title_text)
        blocks.append(title_block)

        # Divider
        blocks.append(DividerBlock())

        # Change Warning Block
        change_text = f"> :warning:   *CHANGE*    :warning:"
        change_block = SectionBlock(text=change_text)
        blocks.append(change_block)

        # Posture Changes Block
        posture_text = (
            f"- Prev Posture: *_{self.last_posture}_*!\n"
            f"- New Posture: *_{self.current_posture}_*!\n"
        )
        posture_block = SectionBlock(text=posture_text)
        blocks.append(posture_block)

        # Divider
        blocks.append(DividerBlock())

        # Alert Time / Price Context Block
        alert_time_text = f"*Alert Time / Price*: _{alert_time_formatted} EST | {self.cpl}_"
        alert_time_block = ContextBlock(elements=[
            MarkdownTextObject(text=alert_time_text)
        ])
        blocks.append(alert_time_block)

        # Convert blocks to dicts
        blocks = [block.to_dict() for block in blocks]

        return blocks  

    def execute(self):
        logger.debug(f" POSTURE | execute | Product: {self.product_name} | Note: Running")
        
        blocks = self.slack_message()
        channel = self.slack_channels_alert.get(self.product_name)
        
        if channel:
            self.slack_client.chat_postMessage(
                channel=channel,
                blocks=blocks,
                text=f"Context Alert - Pos for {self.product_name}"
            )
            logger.info(f" POSTURE | execute | Product: {self.product_name} | Note: Alert Sent To {channel}")
        else:
            logger.debug(f" POSTURE | execute | Product: {self.product_name} | Note: No Slack Channel Configured")