import logging
import math
import threading
from datetime import datetime
from SlackBot.External import External_Config
from logs.Logging_Config import setup_logging
from SlackBot.Slack_Alerts.Conditional.Base import Base_Conditional

setup_logging()
logger = logging.getLogger(__name__)

last_alerts = {}
last_alerts_lock = threading.Lock()

class PVAT(Base_Conditional):
    def __init__(self, product_name, variables):    
        super().__init__(product_name, variables)
        
        # Variables
        self.p_vpoc = self.variables.get(f'{product_name}_PVPOC')
        self.open = self.variables.get(f'{product_name}_D_OPEN')
        self.p_high = self.variables.get(f'{product_name}_PRIOR_HIGH')
        self.p_low = self.variables.get(f'{product_name}_PRIOR_LOW')
        self.ib_atr = self.variables.get(f'{product_name}_IB_ATR')
        self.euro_ibh = self.variables.get(f'{product_name}_EURO_IBH')
        self.euro_ibl = self.variables.get(f'{product_name}_EURO_IBL')
        self.orh = self.variables.get(f'{product_name}_ORH')
        self.orl = self.variables.get(f'{product_name}_ORL')
        self.eth_vwap = self.variables.get(f'{product_name}_ETH_VWAP')
        self.cpl = self.variables.get(f'{product_name}_CPL')
        self.total_ovn_delta = self.variables.get(f'{self.product_name}_TOTAL_OVN_DELTA')
        self.total_rth_delta = self.variables.get(f'{self.product_name}_TOTAL_RTH_DELTA')
        self.prior_close = self.variables.get(f'{self.product_name}_PRIOR_CLOSE')
        self.es_impvol = External_Config.es_impvol
        self.nq_impvol = External_Config.nq_impvol
        self.rty_impvol = External_Config.rty_impvol
        self.cl_impvol = External_Config.cl_impvol 
        self.delta = self.total_delta()
        self.exp_rng, self.exp_hi, self.exp_lo = self.exp_range() 

# ---------------------------------- Specific Calculations ------------------------------------ #   
    def exp_range(self):
        logger.info(f"Running exp_range Calculation For {self.product_name}")

        # Calculation (product specific or Not)
        if not self.prior_close:
            logger.error(f"Prior close not found for product: {self.product_name}")
            raise ValueError(f"Prior close is required for {self.product_name}")
        
        if self.product_name == 'ES':
            exp_range = round(((self.prior_close * (self.es_impvol/100)) * math.sqrt(1/252)) , 2)
            exp_hi = round((self.prior_close + exp_range) , 2)
            exp_lo = round((self.prior_close - exp_range) , 2)
            
            logger.info(f"|exp_range Success| Product : {self.product_name} | EXP_RNG : {exp_range} | EXP_HI : {exp_hi} | EXP_LO : {exp_lo} |")
            return exp_range, exp_hi, exp_lo
        
        elif self.product_name == 'NQ':
            exp_range = round(((self.prior_close * (self.nq_impvol/100)) * math.sqrt(1/252)) , 2)
            exp_hi = round((self.prior_close + exp_range) , 2)
            exp_lo = round((self.prior_close - exp_range) , 2)
            
            logger.info(f"|exp_range Success| Product : {self.product_name} | EXP_RNG : {exp_range} | EXP_HI : {exp_hi} | EXP_LO : {exp_lo} |")
            return exp_range, exp_hi, exp_lo
        
        elif self.product_name == 'RTY':
            exp_range = round(((self.prior_close * (self.rty_impvol/100)) * math.sqrt(1/252)) , 2)
            exp_hi = round((self.prior_close + exp_range) , 2)
            exp_lo = round((self.prior_close - exp_range) , 2)
        
            logger.info(f"|exp_range Success| Product : {self.product_name} | EXP_RNG : {exp_range} | EXP_HI : {exp_hi} | EXP_LO : {exp_lo} |")
            return exp_range, exp_hi, exp_lo
        
        elif self.product_name == 'CL':
            exp_range = round(((self.prior_close * (self.cl_impvol/100)) * math.sqrt(1/252)) , 2)
            exp_hi = round((self.prior_close + exp_range) , 2)
            exp_lo = round((self.prior_close - exp_range) , 2)
            
            logger.info(f"|exp_range Success| Product : {self.product_name} | EXP_RNG : {exp_range} | EXP_HI : {exp_hi} | EXP_LO : {exp_lo} |")
            return exp_range, exp_hi, exp_lo
        
        else:
            raise ValueError(f"Unknown product: {self.product_name}")
        
    def total_delta(self):
        logger.info(f"Running total_delta Calculation For {self.product_name}")

        # Calculation (Product Specific or Not)        
        total_delta = self.total_ovn_delta + self.total_rth_delta

        return total_delta   
    
# ---------------------------------- Driving Input Logic ------------------------------------ #   
    def input(self):
        
        # Driving Input
        logic = (
            self.p_low - (self.exp_rng * 0.1) <= self.cpl <= self.p_high + (self.exp_rng * 0.1)
            and
            abs(self.cpl - self.p_vpoc) > self.exp_rng * 0.1 
            and
            abs(self.cpl - self.p_vpoc) <= self.ib_atr
            )    
        
        logger.info(f"|pvat_input {logic}| Product : {self.product_name} |")
        return logic
    
# ---------------------------------- Opportunity Window ------------------------------------ #   
    def opp_window(self):
        
        # Update current time
        self.current_datetime = datetime.now(self.est)
        self.current_time = self.current_datetime.time()
        
        logger.info(f"Current EST Time: {self.current_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Define time windows based on product type
        if self.product_name == 'CL':
            start_time = self.crude_open
            end_time = self.crude_ib
            logger.info(f"Product '{self.product_name}' detected. Time Window: {start_time} - {end_time}")
        elif self.product_name in ['ES', 'RTY', 'NQ']:
            start_time = self.equity_open
            end_time = self.equity_ib
            logger.info(f"Product '{self.product_name}' detected. Time Window: {start_time} - {end_time}")
        else:
            logger.warning(f"Unknown product '{self.product_name}'. No time window defined.")
            return False  
        
        # Check if current time is within the window
        if start_time <= self.current_time <= end_time:
            logger.info(f"Current time {self.current_time} is within the opportunity window.")
            return True
        else:
            logger.info(f"Current time {self.current_time} is outside the opportunity window.")
            return False
# ---------------------------------- Calculate Criteria ------------------------------------ #      
    def check(self):
        if self.input() and self.opp_window():
            
            # Logic For Direction 
            self.direction = "short" if self.cpl > self.p_vpoc else "long"
            self.color = "red" if self.direction == "short" else "green"
            
            with last_alerts_lock:
                last_alert = last_alerts.get(self.product_name)   
                logger.info(f"Current direction: {self.direction}, Last alert: {last_alert} for {self.product_name}")
                
                if self.direction != last_alert: 
                    logger.info("Condition met. Preparing to send Slack alert.")
                    
                    # Logic For c_within_atr 
                    if abs(self.cpl - self.p_vpoc) <= self.ib_atr:
                        self.c_within_atr = "x" 
                    else:
                        self.c_within_atr = "  "
                    # Logic For c_orderflow
                    self.c_orderflow = "  "
                    if self.direction == "short" and self.delta < 0:
                        self.c_orderflow = "x"
                    elif self.direction == "long" and self.delta > 0:
                        self.c_orderflow = "x"
                    # Logic for c_euro IB
                    self.c_euro_ib = "  "
                    if self.direction == "short" and self.cpl < self.euro_ibl:
                        self.c_euro_ib = "x"
                    elif self.direction == "long" and self.cpl > self.euro_ibh:
                        self.c_euro_ib = "x"
                    # Logic for c_or
                    self.c_or = "  "
                    if self.direction == "short" and self.cpl < self.orl:
                        self.c_or = "x"
                    elif self.direction == "long" and self.cpl > self.orh:
                        self.c_or = "x"
                    # Logic for c_between
                    self.c_between = "  "
                    if self.direction == "short" and self.p_vpoc < self.cpl < self.eth_vwap:
                        self.c_between = "x"
                    elif self.direction == "long" and self.eth_vwap < self.cpl < self.p_vpoc:
                        self.c_between = "x"
                    # Logic for c_align
                    if abs(self.eth_vwap - self.p_vpoc) <= (self.exp_rng * 0.05):
                        self.c_align = "x"
                    else: 
                        self.c_align = "  "
                    # Logic for Score 
                    self.score = sum(1 for condition in [self.c_within_atr, self.c_orderflow, self.c_euro_ib, self.c_or, self.c_between, self.c_align] if condition == "x")   

                    try:
                        logger.info(f"Current direction: {self.direction}, Last alert: {last_alert} for {self.product_name}")
                        last_alerts[self.product_name] = self.direction
                        self.execute()
                    except Exception as e:
                        logger.error(f"Failed to send Slack alert: {e}")
                else:
                    logger.info(f"Direction '{self.direction}' is the same as the last alerted direction. No alert sent to avoid spamming.")
        else:
            logger.info("Condition not met. No action taken.")
# ---------------------------------- Alert Preparation------------------------------------ #  
    def slack_message(self):
        pro_color = self.product_color.get(self.product_name)
        alert_time_formatted = self.current_datetime.strftime('%H:%M:%S') 
        direction_settings = {
            "long": {
                "pv_indicator": "^",
                "c_euro_ib_text": "Above Euro IBH",
                "c_or_text": "Above 30 Sec Opening Range High"
            },
            "short": {
                "pv_indicator": "v",
                "c_euro_ib_text": "Below Euro IBL",
                "c_or_text": "Below 30 Sec Opening Range Low"
            }
        }
    
        settings = direction_settings.get(self.direction)
        if not settings:
            raise ValueError(f"Invalid direction: {self.direction}")
    
        message_template = (
            f">:large_{pro_color}_square: *{self.product_name} - Playbook Alert - PVAT {settings['pv_indicator']}* :large_{self.color}_circle:\n"
            "──────────────────────\n"
            f"*Destination*: _{self.p_vpoc} (Prior Session Vpoc)_\n" 
            f"*Risk*: _Wrong if auction fails to complete PVPOC test before IB, or accepts away form value_\n" 
            f"*Driving Input*: _Auction opening in range or slightly outside range, divergent from prior session Vpoc_\n" 
            "────────────────\n"
            "                *Criteria*\n"
            f"*[{self.c_within_atr}]* Target Within ATR Of IB\n" 
            f"*[{self.c_orderflow}]* Orderflow In Direction Of Target (*_{self.delta}_*)\n" 
            f"*[{self.c_euro_ib}]* {settings['c_euro_ib_text']}\n" 
            f"*[{self.c_or}]* {settings['c_or_text']}\n" 
            f"\n*[{self.c_between}]* Between DVWAP and PVPOC\n" 
            "Or\n"
            f"*[{self.c_align}]* DVWAP and PVPOC aligned\n" 
            "────────────────\n"
            f">*Playbook Score*: _{self.score} / 5_\n"    
            "──────────────────────\n"
            f">*Alert Time*: _{alert_time_formatted} EST_\n"
        )
        return message_template  
    
    def execute(self):

        message = self.slack_message()
        channel = self.slack_channels.get(self.product_name)
        
        if channel:
            self.send_slack_message(channel, message)
            print(f"#PVAT ALert sent to {channel} for {self.product_name}")
        else:
            print(f"No Slack channel configured for {self.product_name}")
            