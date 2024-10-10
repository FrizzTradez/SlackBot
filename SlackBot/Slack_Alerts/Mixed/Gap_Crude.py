import logging
import math
from SlackBot.External import External_Config
from SlackBot.Slack_Alerts.Periodic.Base import Base_Periodic
import threading
from datetime import datetime
import time

logger = logging.getLogger(__name__)

class Gap_Check_Crude(Base_Periodic):
    def __init__(self, files):
        super().__init__(files)
        
    # ---------------------- Specific Calculations ------------------------- #
    def exp_range(self, prior_close, impvol):
        logger.debug(f" GAP_CRUDE | exp_range | Note: Running")
        
        exp_range = round(((prior_close * (impvol / 100)) * math.sqrt(1 / 252)), 2)
        
        return exp_range

    def gap_info(self, day_open, prior_high, prior_low, exp_range):
        logger.debug(f" GAP_CRUDE | gap_info | Note: Running")
        
        gap = ""
        gap_tier = ""
        
        if day_open > prior_high:
            gap_size = round((day_open - prior_high), 2)
            gap = "Gap Up"
            
            if exp_range == 0:
                gap_tier = "Undefined"  
            else:
                gap_ratio = round((gap_size / exp_range) , 2)
                if gap_ratio <= 0.5:
                    gap_tier = "Tier 1"
                elif gap_ratio <= 0.75:
                    gap_tier = "Tier 2"
                else:
                    gap_tier = "Tier 3"
        
        elif day_open < prior_low:
            gap_size = round((prior_low - day_open), 2)
            gap = "Gap Down"
            
            if exp_range == 0:
                gap_tier = "Undefined" 
            else:
                gap_ratio = round((gap_size / exp_range) , 2)
                if gap_ratio <= 0.5:
                    gap_tier = "Tier 1"
                elif gap_ratio <= 0.75:
                    gap_tier = "Tier 2"
                else:
                    gap_tier = "Tier 3"
        
        else:
            gap = "No Gap"
            gap_tier = "Tier 0"
        
        return gap, gap_tier, gap_size
    # ---------------------- Driving Input Logic ------------------------- #
    def send_alert(self):
        threads = []
        for product_name in ['CL']:
            thread = threading.Thread(target=self.process_product, args=(product_name,))
            thread.start()
            threads.append(thread)
            time.sleep(1)

        # Optionally wait for all threads to complete
        for thread in threads:
            thread.join()
    # ---------------------- Alert Preparation ------------------------- #
    def process_product(self, product_name):
        try:
            variables = self.fetch_latest_variables(product_name)
            if not variables:
                logger.error(f" GAP_CRUDE | process_product | Product: {product_name} |  Note: No data available ")
                return
            
            # Variables specific to the product
            prior_close = round(variables.get(f'{product_name}_PRIOR_CLOSE'), 2)
            day_open = round(variables.get(f'{product_name}_DAY_OPEN'), 2)
            prior_high = round(variables.get(f'{product_name}_PRIOR_HIGH'), 2)
            prior_low = round(variables.get(f'{product_name}_PRIOR_LOW'), 2)

            impvol = External_Config.cl_impvol

            color = self.product_color.get(product_name)
            current_time = datetime.now(self.est).strftime('%H:%M:%S')
            
            # Calculations
            exp_range = self.exp_range(
                prior_close, impvol
                )
            gap, gap_tier, gap_size = self.gap_info(
                day_open, prior_high, prior_low, exp_range
                )
            
            if gap in ["Gap Up", "Gap Down"]:
                # Direction Symbols
                direction_emojis = {
                    'Gap Up': ':arrow_up:',
                    'Gap Down': ':arrow_down:',
                }
                
                # Message Template
                message = (
                    f">:large_{color}_square:  *{product_name} - Context Alert - Gap*  :large_{color}_square:\n"
                    "────────────────────\n"
                    f">      :warning:   *Opening In Gap*    :warning:\n"      
                    f"- *_{gap_tier}_* Gap {direction_emojis.get(gap)} : {gap_size}p\n"
                    "────────────────────\n"            
                    f">*Alert Time*: _{current_time}_ EST\n"
                )
                
                # Send Slack Alert
                channel = self.slack_channels.get(product_name)
                if channel:
                    self.slack_client.chat_postMessage(channel=channel, text=message) 
                    logger.info(f" GAP_CRUDE | process_product | Note: Message sent to {channel} for {product_name}")
                else:
                    logger.error(f" GAP_CRUDE | process_product | Note: No Slack channel configured for {product_name}")
            else:
                logger.info(f" GAP_CRUDE | process_product | Product: {product_name} | Note: No Gap detected, message not sent.")
        except Exception as e:
            logger.error(f" GAP_CRUDE | process_product | Product: {product_name} | Error processing: {e}")