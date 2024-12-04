import logging
import math
import threading
from datetime import datetime
from SlackBot.External import External_Config
from slack_sdk.models.blocks import SectionBlock, DividerBlock, ContextBlock, MarkdownTextObject
from SlackBot.Slack_Alerts.Conditional.Base import Base_Conditional
# For Directional Open Go With there are multiple types 
# of directional opens. THis needs to be able to detect those different directional
# open types in real time as they are in progress, So if this is the case there needs to be a threshold that determines when there is a high probability of a certain open type.
# Logic Sequence: Directional Open In Play, 
# Should we trigger if there is no destination, how would I even programmatically assess this? Code the Edge, What is the statistical edge?!?!


logger = logging.getLogger(__name__)

last_alerts = {}
last_alerts_lock = threading.Lock()

class DOGW(Base_Conditional):
    def __init__(self, product_name, variables):    
        super().__init__(product_name, variables)
        
        # Variables (Round All Variables)
        self.p_vpoc = round(self.variables.get(f'{self.product_name}_PRIOR_VPOC'), 2)
        self.day_open = round(self.variables.get(f'{self.product_name}_DAY_OPEN'), 2)
        self.prior_high = round(self.variables.get(f'{self.product_name}_PRIOR_HIGH'), 2)
        self.prior_low = round(self.variables.get(f'{self.product_name}_PRIOR_LOW'), 2)
        self.ib_atr = round(self.variables.get(f'{self.product_name}_IB_ATR'), 2)
        self.euro_ibh = round(self.variables.get(f'{self.product_name}_EURO_IBH'), 2)
        self.euro_ibl = round(self.variables.get(f'{self.product_name}_EURO_IBL'), 2)
        self.orh = round(self.variables.get(f'{self.product_name}_ORH'), 2)
        self.orl = round(self.variables.get(f'{self.product_name}_ORL'), 2)
        self.a_high = round(variables.get(f'{product_name}_A_HIGH'), 2)
        self.a_low = round(variables.get(f'{product_name}_A_LOW'), 2)
        self.b_high = round(variables.get(f'{product_name}_B_HIGH'), 2)
        self.b_low = round(variables.get(f'{product_name}_B_LOW'), 2)
        self.eth_vwap = round(self.variables.get(f'{self.product_name}_ETH_VWAP'), 2)
        self.cpl = round(self.variables.get(f'{self.product_name}_CPL'), 2)
        self.total_ovn_delta = round(self.variables.get(f'{self.product_name}_TOTAL_OVN_DELTA'), 2)
        self.total_rth_delta = round(self.variables.get(f'{self.product_name}_TOTAL_RTH_DELTA'), 2)
        self.prior_close = round(self.variables.get(f'{self.product_name}_PRIOR_CLOSE'), 2)
        self.ib_high = round(self.variables.get(f'{product_name}_IB_HIGH'), 2)
        self.ib_low = round(self.variables.get(f'{product_name}_IB_LOW'), 2)
        self.rvol = round(self.variables.get(f'{product_name}_RVOL'), 2)
        
        self.es_impvol = External_Config.es_impvol
        self.nq_impvol = External_Config.nq_impvol
        self.rty_impvol = External_Config.rty_impvol
        self.cl_impvol = External_Config.cl_impvol 
        
        self.delta = self.total_delta()
        self.exp_rng, self.exp_hi, self.exp_lo = self.exp_range() 
        self.opentype = self.open_type_algorithm()

# ---------------------------------- Specific Calculations ------------------------------------ #   
    def open_type_algorithm(self):
        # This algorithm will need to detect open types in real time predictably.
        # There are lots of fluctuations across the Mid point or the OPENING RANGE.
        # The question is how do we get the prediction time down to 10 minutes after open?
        # How can we reliably predict the open type 10 minutes after the open?
        
        logger.debug(f" DOGW | open_type_algorithm | Note: Running")

        a_period_mid = round(((self.a_high + self.a_low) / 2), 2)
        a_period_range = (self.a_high - self.a_low)
        overlap = max(0, min(max(self.a_high, self.b_high), self.prior_high) - max(min(self.a_low, self.b_low), self.prior_low))
        total_range = max(self.a_high, self.b_high) - min(self.a_low, self.b_low)

        if (abs(self.day_open - self.a_high) <= 0.025 * a_period_range) and (self.b_high < a_period_mid):
            open_type = "OD v"
        elif (abs(self.day_open - self.a_low) <= 0.025 * a_period_range) and (self.b_low > a_period_mid):
            open_type = "OD ^"
        elif (self.day_open > a_period_mid) and (self.b_high < a_period_mid):
            open_type = "OTD v"
        elif (self.day_open < a_period_mid) and (self.b_low > a_period_mid):
            open_type = "OTD ^"
        elif (self.day_open > a_period_mid) and (self.b_low > a_period_mid) and (self.b_high > self.orh):
            open_type = "ORR ^"
        elif (self.day_open < a_period_mid) and (self.b_high < a_period_mid) and (self.b_low < self.orl):
            open_type = "ORR v"
        elif overlap >= 0.5 * total_range:
            open_type = "OAIR"
        elif (overlap < 0.5 * total_range) and (self.day_open >= self.prior_high):
            open_type = "OAOR ^"
        elif (overlap < 0.5 * total_range) and (self.day_open <= self.prior_low):
            open_type = "OAOR v"
        else:
            open_type = "Other"

        return open_type    
    
    def exp_range(self):
        logger.debug(f" DOGW | exp_range | Product: {self.product_name} | Note: Running")

        # Calculation (product specific or Not)
        if not self.prior_close:
            logger.error(f" DOGW | exp_range | Product: {self.product_name} | Note: No Close Found")
            raise ValueError(f" DOGW | exp_range | Product: {self.product_name} | Note: Need Close For Calculation!")
        
        if self.product_name == 'ES':
            exp_range = round(((self.prior_close * (self.es_impvol/100)) * math.sqrt(1/252)) , 2)
            exp_hi = (self.prior_close + exp_range)
            exp_lo = (self.prior_close - exp_range)
            
            logger.debug(f" DOGW | exp_range | Product: {self.product_name} | EXP_RNG: {exp_range} | EXP_HI: {exp_hi} | EXP_LO: {exp_lo}")
            return exp_range, exp_hi, exp_lo
        
        elif self.product_name == 'NQ':
            exp_range = round(((self.prior_close * (self.nq_impvol/100)) * math.sqrt(1/252)) , 2)
            exp_hi = (self.prior_close + exp_range)
            exp_lo = (self.prior_close - exp_range)
            
            logger.debug(f" DOGW | exp_range | Product: {self.product_name} | EXP_RNG: {exp_range} | EXP_HI: {exp_hi} | EXP_LO: {exp_lo}")
            return exp_range, exp_hi, exp_lo
        
        elif self.product_name == 'RTY':
            exp_range = round(((self.prior_close * (self.rty_impvol/100)) * math.sqrt(1/252)) , 2)
            exp_hi = (self.prior_close + exp_range)
            exp_lo = (self.prior_close - exp_range)
        
            logger.debug(f" DOGW | exp_range | Product: {self.product_name} | EXP_RNG: {exp_range} | EXP_HI: {exp_hi} | EXP_LO: {exp_lo}")
            return exp_range, exp_hi, exp_lo
        
        elif self.product_name == 'CL':
            exp_range = round(((self.prior_close * (self.cl_impvol/100)) * math.sqrt(1/252)) , 2)
            exp_hi = (self.prior_close + exp_range)
            exp_lo = (self.prior_close - exp_range)
            
            logger.debug(f" DOGW | exp_range | Product: {self.product_name} | EXP_RNG: {exp_range} | EXP_HI: {exp_hi} | EXP_LO: {exp_lo}")
            return exp_range, exp_hi, exp_lo
        
        else:
            raise ValueError(f" DOGW | exp_range | Product: {self.product_name} | Note: Unknown Product")
        
    def total_delta(self):
        logger.debug(f" DOGW | total_delta | Product: {self.product_name} | Note: Running")

        # Calculation (Product Specific or Not)        
        total_delta = self.total_ovn_delta + self.total_rth_delta
        
        logger.debug(f" DOGW | total_delta | TOTAL_DELTA: {total_delta}")
        return total_delta   
    
# ---------------------------------- Driving Input Logic ------------------------------------ #   
    def input(self):
        logger.debug(f" DOGW | input | Product: {self.product_name} | Note: Running")
        
        self.used_atr = self.ib_high - self.ib_low
        self.remaining_atr = max((self.ib_atr - self.used_atr), 0)
        self.target = 
        
        # Direction Based Logic
        if self.direction == "short":
            self.atr_condition = abs(self.ib_low - self.p_vpoc) <= self.remaining_atr
            self.or_condition = self.cpl < self.orl
        elif self.direction == "long":
            self.atr_condition = abs(self.ib_high - self.p_vpoc) <= self.remaining_atr
            self.or_condition = self.cpl > self.orh
                        
        # Driving Input
        logic = (
            self.opentype
            in 
            ["OD v", "OD ^", "OTD v", "OTD ^", "ORR ^", "ORR v", "OAOR ^", "OAOR v"]
            and 
            self.or_condition
            and
            self.atr_condition
            ) 
        
        logger.debug(f" DOGW | input | Product: {self.product_name} | LOGIC: {logic}")
        
        return logic
    
# ---------------------------------- Opportunity Window ------------------------------------ #   
    def time_window(self):
        logger.debug(f" DOGW | time_window | Product: {self.product_name} | Note: Running")
        
        # Update current time
        self.current_datetime = datetime.now(self.est)
        self.current_time = self.current_datetime.time()
        
        # Define time windows based on product type
        if self.product_name == 'CL':
            start_time = self.crude_pvat_start
            end_time = self.crude_ib
            logger.debug(f" DOGW | time_window | Product: {self.product_name} | Time Window: {start_time} - {end_time}")
            
        elif self.product_name in ['ES', 'RTY', 'NQ']:
            start_time = self.equity_pvat_start
            end_time = self.equity_ib
            logger.debug(f" DOGW | time_window | Product: {self.product_name} | Time Window: {start_time} - {end_time}")
        else:
            logger.warning(f" DOGW | time_window | Product: {self.product_name} | No time window defined.")
            return False  
        
        # Check if current time is within the window
        if start_time <= self.current_time <= end_time:
            logger.debug(f" DOGW | time_window | Product: {self.product_name} | Within Window: {self.current_time}.")
            return True
        else:
            logger.debug(f" DOGW | time_window | Product: {self.product_name} | Outside Window {self.current_time}.")
            return False
# ---------------------------------- Calculate Criteria ------------------------------------ #      
    def check(self):
        logger.debug(f" DOGW | check | Product: {self.product_name} | Note: Running")
        
        # Define Direction
        self.direction = "short" if self.opentype in ["OD v", "OTD v", "ORR v", "OAOR v"] else "long"
        self.color = "red" if self.direction == "short" else "green"
    
        # Driving Input
        if self.input() and self.time_window():
            
            with last_alerts_lock:
                last_alert = last_alerts.get(self.product_name)   
                logger.debug(f" DOGW | check | Product: {self.product_name} | Current Alert: {self.direction} | Last Alert: {last_alert}")
                
                if self.direction != last_alert: 
                    logger.info(f" DOGW | check | Product: {self.product_name} | Note: Condition Met")
                    
                    # Logic For c_within_atr 
                    if self.atr_condition: 
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
                        last_alerts[self.product_name] = self.direction
                        self.execute()
                    except Exception as e:
                        logger.error(f" DOGW | check | Product: {self.product_name} | Note: Failed to send Slack alert: {e}")
                else:
                    logger.debug(f" DOGW | check | Product: {self.product_name} | Note: Alert: {self.direction} Is Same")
        else:
            logger.info(f" DOGW | check | Product: {self.product_name} | Note: Condition Not Met")
# ---------------------------------- Alert Preparation------------------------------------ #  
    def slack_message(self):
        logger.debug(f" DOGW | slack_message | Product: {self.product_name} | Note: Running")
        
        pro_color = self.product_color.get(self.product_name)
        alert_time_formatted = self.current_datetime.strftime('%H:%M:%S') 
        
        direction_settings = {
            "long": {
                "pv_indicator": "^",
                "c_euro_ib_text": "Above Euro IBH",
                "c_or_text": "Above 30 Sec Opening Range High",
                "large": "large_"
            },
            "short": {
                "pv_indicator": "v",
                "c_euro_ib_text": "Below Euro IBL",
                "c_or_text": "Below 30 Sec Opening Range Low",
                "large": ""
            }
        }

        settings = direction_settings.get(self.direction)
        if not settings:
            raise ValueError(f" DOGW | slack_message | Note: Invalid direction '{self.direction}'")

        blocks = []

        # Title Block
        title_text = f":large_{pro_color}_square: *{self.product_name} - Playbook Alert -* :{settings['large']}{self.color}_circle: *PVAT {settings['pv_indicator']}*"
        title_block = SectionBlock(text=title_text)
        blocks.append(title_block)

        # Divider
        blocks.append(DividerBlock())

        # Description Block
        description_text = (
            f"*Destination*: _{self.p_vpoc} (Prior Session Vpoc)_\n"
            f"*Risk*: _Wrong if auction fails to complete PVPOC test before IB, or accepts away from value_\n"
            f"*Driving Input*: _Auction opening in range or slightly outside range, divergent from prior session Vpoc_\n"
        )
        description_block = SectionBlock(text=description_text)
        blocks.append(description_block)

        # Divider
        blocks.append(DividerBlock())

        # Criteria Header
        criteria_header = SectionBlock(text=">*Criteria*")
        blocks.append(criteria_header)

        # Criteria Details
        criteria_text = (
            f"*[{self.c_within_atr}]* Target Within ATR Of IB\n"
            f"*[{self.c_orderflow}]* Orderflow In Direction Of Target (*_{self.delta}_*)\n"
            f"*[{self.c_euro_ib}]* {settings['c_euro_ib_text']}\n"
            f"*[{self.c_or}]* {settings['c_or_text']}\n"
            f"\n*[{self.c_between}]* Between DVWAP and PVPOC\n"
            "Or\n"
            f"*[{self.c_align}]* DVWAP and PVPOC aligned\n"
        )
        criteria_block = SectionBlock(text=criteria_text)
        blocks.append(criteria_block)

        # Divider
        blocks.append(DividerBlock())

        # Playbook Score Block
        score_text = f">*Playbook Score*: _{self.score} / 5_\n"
        score_block = SectionBlock(text=score_text)
        blocks.append(score_block)
        
        # Alert Time and Price Context Block
        alert_time_text = f"*Alert Time / Price*: _{alert_time_formatted} EST | {self.cpl}_"
        alert_time_block = ContextBlock(elements=[
            MarkdownTextObject(text=alert_time_text)
        ])
        blocks.append(alert_time_block)

        # Divider
        blocks.append(DividerBlock())

        # Convert blocks to dicts
        blocks = [block.to_dict() for block in blocks]

        return blocks  
    
    def execute(self):
        logger.debug(f" DOGW | execute | Product: {self.product_name} | Note: Running")
        
        blocks = self.slack_message()
        channel = self.slack_channels_playbook.get(self.product_name)
        
        if channel:
            self.slack_client.chat_postMessage(
                channel=channel,
                blocks=blocks,
                text=f"Playbook Alert - DOGW for {self.product_name}"
            )
            logger.info(f" DOGW | execute | Product: {self.product_name} | Note: Alert Sent To {channel}")
        else:
            logger.debug(f" DOGW | execute | Product: {self.product_name} | Note: No Slack Channel Configured")
            