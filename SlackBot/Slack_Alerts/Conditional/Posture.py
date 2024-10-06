import logging
import math
import threading
from datetime import datetime
from SlackBot.External import External_Config
from SlackBot.Slack_Alerts.Conditional.Base import Base_Conditional

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
            
        logger.debug(f" POSTURE | posture | Current_Posture: {posture}") 
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
# ---------------------------------- Main Function ------------------------------------ #      
    def check(self):
        logger.debug(f" POSTURE | check | Product: {self.product_name} | Note: Running")
        self.current_posture = self.input()
        logic = False
            
        with last_alerts_lock:
            self.last_posture = last_alerts.get(self.product_name)
            if self.last_posture is None:
                last_alerts[self.product_name] = self.current_posture
                logger.debug(f" POSTURE | check | Product: {self.product_name} | Note: Initial posture set to {self.current_posture}")                
            elif self.current_posture != self.last_posture: 
                logic = True
                last_alerts[self.product_name] = self.current_posture
                logger.info(f" POSTURE | check | Product: {self.product_name} | Current_Posture: {self.current_posture} | Last_Posture: {self.last_posture} | Note: Posture Change Detected")
                               
        if logic and self.time_window():      
            try: 
                self.execute()
            except Exception as e:
                logger.error(f" POSTURE | check | Product: {self.product_name} | Note: Failed to send Slack alert: {e}")
        else:
            logger.info(f" POSTURE | check | Product: {self.product_name} | Current_Posture: {self.current_posture} | Last_Posture: {self.last_posture} | Note: No Posture Change")
# ---------------------------------- Alert Preparation------------------------------------ #  
    def slack_message(self):
        logger.debug(f" POSTURE | slack_message | Product: {self.product_name} | Note: Running")
        
        pro_color = self.product_color.get(self.product_name)
        alert_time_formatted = self.current_datetime.strftime('%H:%M:%S') 
        
        message_template = (
            f">:large_{pro_color}_square:  *{self.product_name} - Context Alert - Posture*  :large_{pro_color}_square:\n"
            "────────────────────\n"
            f">         :warning:   *CHANGE*    :warning:\n"      
            f"- Prev Posture: *_{self.last_posture}_*!\n"
            f"- New Posture: *_{self.current_posture}_*!\n"
            "────────────────────\n"            
            f">*Alert Time*: _{alert_time_formatted}_ EST\n"
        )
        return message_template  
    
    def execute(self):
        logger.debug(f" POSTURE | execute | Product: {self.product_name} | Note: Running")
        
        message = self.slack_message()
        channel = self.slack_channels.get(self.product_name)
        
        if channel:
            self.send_slack_message(channel, message)
            logger.info(f" POSTURE | execute | Product: {self.product_name} | Note: ALert Sent To {channel}")
        else:
            logger.debug(f" POSTURE | execute | Product: {self.product_name} | Note: No Slack Channel Configured")