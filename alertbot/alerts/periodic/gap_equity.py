import logging
import math
from SlackBot.External import External_Config
from SlackBot.Slack_Alerts.Periodic.Base import Base_Periodic
from slack_sdk.models.blocks import SectionBlock, DividerBlock, ContextBlock, MarkdownTextObject
import threading
from datetime import datetime
import time

logger = logging.getLogger(__name__)

class Gap_Check_Equity(Base_Periodic):
    def __init__(self, files):
        super().__init__(files)
        
    # ---------------------- Specific Calculations ------------------------- #
    def exp_range(self, prior_close, impvol):
        logger.debug(f" GAP_EQUITY | exp_range | Note: Running")
        
        exp_range = round(((prior_close * (impvol / 100)) * math.sqrt(1 / 252)), 2)
        
        return exp_range

    def gap_info(self, day_open, prior_high, prior_low, exp_range):
        logger.debug(f" GAP_EQUITY | gap_info | Note: Running")
        
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
       
    # ---------------------- Alert Preparation ------------------------- #
    def send_alert(self):
        threads = []
        for product_name in ['ES', 'NQ', 'RTY']:
            thread = threading.Thread(target=self.process_product, args=(product_name,))
            thread.start()
            threads.append(thread)
            time.sleep(1)

        # Optionally wait for all threads to complete
        for thread in threads:
            thread.join()

    def process_product(self, product_name):
        try:
            variables = self.fetch_latest_variables(product_name)
            if not variables:
                logger.error(f" GAP_EQUITY | process_product | Product: {product_name} |  Note: No data available ")
                return
            
            # Variables (Round All Variables) 
            prior_close = round(variables.get(f'{product_name}_PRIOR_CLOSE'), 2)
            day_open = round(variables.get(f'{product_name}_DAY_OPEN'), 2)
            prior_high = round(variables.get(f'{product_name}_PRIOR_HIGH'), 2)
            prior_low = round(variables.get(f'{product_name}_PRIOR_LOW'), 2)
            
            # Implied volatility specific to the product
            if product_name == 'ES':
                impvol = External_Config.es_impvol
            elif product_name == 'NQ':
                impvol = External_Config.nq_impvol
            elif product_name == 'RTY':
                impvol = External_Config.rty_impvol
            else:
                raise ValueError(f" GAP_EQUITY | process_product | Note: {product_name}")
            
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
                
                # Build the blocks
                blocks = []

                # Title Block
                title_block = SectionBlock(
                    text=f":large_{color}_square:  *{product_name} - Context Alert - Gap*  :large_{color}_square:"
                )
                blocks.append(title_block)

                # Divider
                blocks.append(DividerBlock())

                # Gap Alert Header
                gap_alert_header = SectionBlock(
                    text=">:warning:   *GAP*    :warning:"
                )
                blocks.append(gap_alert_header)

                # Gap Details
                gap_details_text = f"- *_{gap_tier}_* Gap {direction_emojis.get(gap)} : {gap_size}p"
                gap_details_block = SectionBlock(text=gap_details_text)
                blocks.append(gap_details_block)

                # Divider
                blocks.append(DividerBlock())

                # Alert Time Context Block
                alert_time_context = ContextBlock(elements=[
                    MarkdownTextObject(text=f"*Alert Time*: _{current_time}_ EST")
                ])
                blocks.append(alert_time_context)

                # Convert blocks to dicts
                blocks = [block.to_dict() for block in blocks]

                # Send Slack Alert
                channel = self.slack_channels_alert.get(product_name)
                if channel:
                    self.slack_client.chat_postMessage(
                        channel=channel,
                        blocks=blocks,
                        text=f"Context Alert - Gap for {product_name}"
                    ) 
                    logger.info(f" GAP_CRUDE | process_product | Note: Message sent to {channel} for {product_name}")
                else:
                    logger.error(f" GAP_CRUDE | process_product | Note: No Slack channel configured for {product_name}")
            else:
                logger.info(f" GAP_CRUDE | process_product | Product: {product_name} | Note: No Gap detected, message not sent.")
        except Exception as e:
            logger.error(f" GAP_CRUDE | process_product | Product: {product_name} | Error processing: {e}")