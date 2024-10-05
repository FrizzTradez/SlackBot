import logging
import math
from datetime import datetime
from SlackBot.External import External_Config
from SlackBot.Slack_Alerts.Conditional.Base import Base_Conditional
import threading
import re
logger = logging.getLogger(__name__)

last_alerts = {}
last_alerts_lock = threading.Lock()

class PRE_IB_BIAS(Base_Conditional):
    def __init__(self, product_name, variables):    
        super().__init__(product_name, variables)
        
        # Variables
        self.es_bias = External_Config.es_bias
        self.nq_bias = External_Config.nq_bias
        self.rty_bias = External_Config.rty_bias
        self.cl_bias = External_Config.cl_bias
        self.cpl = round(self.variables.get(f'{self.product_name}_CPL'), 2)
        
# ---------------------------------- Driving Input Logic ------------------------------------ #         
    def input(self):
        logger.debug(f" PRE_IB | input | Product: {self.product_name} | Note: Running")
        
        self.bias_string = ''
        if self.product_name == 'ES':
            self.bias_string = self.es_bias
        elif self.product_name == 'NQ':
            self.bias_string = self.nq_bias
        elif self.product_name == 'RTY':
            self.bias_string = self.rty_bias
        elif self.product_name == 'CL':
            self.bias_string = self.cl_bias
        
        self.price = None
        self.bias_char = ''
        if self.bias_string:
            self.bias_string = self.bias_string.strip()
            match = re.match(r'^([0-9]*\.?[0-9]+)([^\d\.]*)$', self.bias_string)
            if match:
                self.price_str = match.group(1)
                self.bias_char = match.group(2).strip()
                try:
                    self.price = float(self.price_str)
                except ValueError:
                    self.price = None
            else:
                try:
                    self.price = float(self.bias_string)
                    self.bias_char = ''
                except ValueError:
                    self.price = None
                    self.bias_char = ''
        else:
            self.price = None
            self.bias_char = ''
            
        self.bias_condition = False 
        if self.price is not None and self.bias_char:
            if self.bias_char.lower() == 'v':
                self.bias_condition = self.cpl > self.price
                self.direction = "above"
            elif self.bias_char == '^':
                self.bias_condition = self.cpl < self.price
                self.direction = "below"
            else:
                self.bias_condition = False
                self.direction = None
        else:
            pass

        logic = (
            self.bias_condition
        )    

        logger.debug(
            f" PRE_IB | input | Product: {self.product_name} | Bias_Symbol: {self.bias_char} | Bias_Price: {self.price} | Last_Price: {self.cpl} | LOGIC: {logic}"
        )
        
        return logic
# ---------------------------------- Opportunity Window ------------------------------------ #   
    def time_window(self):
        logger.debug(f" PRE_IB | opp_window | Product: {self.product_name} | Note: Running")
        
        # Update current time
        self.current_datetime = datetime.now(self.est)
        self.current_time = self.current_datetime.time()
        
        # Define time windows based on product type
        if self.product_name == 'CL':
            start_time = self.crude_open
            end_time = self.crude_close
            logger.debug(f" PRE_IB | opp_window | Product: {self.product_name} | Time Window: {start_time} - {end_time}")
        elif self.product_name in ['ES', 'RTY', 'NQ']:
            start_time = self.equity_open
            end_time = self.equity_close
            logger.debug(f" PRE_IB | opp_window | Product: {self.product_name} | Time Window: {start_time} - {end_time}")
        else:
            logger.warning(f" PRE_IB | opp_window | Product: {self.product_name} | No time window defined.")
            return False  
        
        # Check if current time is within the window
        if start_time <= self.current_time <= end_time:
            logger.debug(f" PRE_IB | opp_window | Product: {self.product_name} | Within Window: {self.current_time}.")
            return True
        else:
            logger.debug(f" PRE_IB | opp_window | Product: {self.product_name} | Outside Window {self.current_time}.")
            return False   
# ---------------------------------- Main Function ------------------------------------ #                  
    def check(self):
        logger.debug(f" PRE_IB | check | Product: {self.product_name} | Note: Running")
        
        # Driving Input
        if self.input() and self.time_window():
            
            with last_alerts_lock:
                last_alert = last_alerts.get(self.product_name)   
                current_date = datetime.now().date()
                logger.debug(f" PRE_IB | check | Product: {self.product_name} | Current Alert: {self.direction} | Last Alert: {last_alert}")
                
                if last_alert != current_date: 
                    logger.info(f" PRE_IB | check | Product: {self.product_name} | Note: Condition Met")
                    try:
                        last_alerts[self.product_name] = current_date
                        self.execute()
                    except Exception as e:
                        logger.error(f" PRE_IB | check | Product: {self.product_name} | Note: Failed to send Slack alert: {e}")
                else:
                    logger.debug(f" PRE_IB | check | Product: {self.product_name} | Note: Alert Already Sent Today")
        else:
            logger.info(f" PRE_IB | check | Product: {self.product_name} | Note: Condition Not Met Or No Bias")
# ---------------------------------- Alert Preparation------------------------------------ #  
    def slack_message(self):
        logger.debug(f" PRE_IB | slack_message | Product: {self.product_name} | Note: Running")
        
        pro_color = self.product_color.get(self.product_name)
        alert_time_formatted = self.current_datetime.strftime('%H:%M:%S') 
        
        direction_settings = {
            "above": {
                "text": "Above"
            },
            "below": {
                "text": "Below"
            }
        }
    
        settings = direction_settings.get(self.direction)
        if not settings:
            raise ValueError(f" PVAT | slack_message | Note: Invalid direction '{self.direction}'")
    
        message_template = (
            f">:large_{pro_color}_square:  *{self.product_name} - Context Alert - Bias*  :large_{pro_color}_square:\n"
            "────────────────────\n"
            f">         :warning:   *VIOLATION*    :warning:\n"      
            f"- Price Trading {settings['text']} *_{self.price}_*!\n"
            "────────────────────\n"            
            f">*Alert Time*: _{alert_time_formatted}_ EST\n"
        )
        
        return message_template  
    
    def execute(self):
        logger.debug(f" PRE_IB | execute | Product: {self.product_name} | Note: Running")
        
        message = self.slack_message()
        channel = self.slack_channels.get(self.product_name)
        
        if channel:
            self.send_slack_message(channel, message)
            logger.info(f" PRE_IB | execute | Product: {self.product_name} | Note: ALert Sent To {channel}")
        else:
            logger.debug(f" PRE_IB | execute | Product: {self.product_name} | Note: No Slack Channel Configured")
 