import logging
import math
from SlackBot.External import External_Config
from SlackBot.Slack_Alerts.Periodic.Base import Base_Periodic
from slack_sdk.models.blocks import SectionBlock, DividerBlock, ContextBlock, MarkdownTextObject
import threading
from datetime import datetime
import time

logger = logging.getLogger(__name__)

class Datr_Crude(Base_Periodic):
    def __init__(self, files):
        super().__init__(files)
        
    # ---------------------- Specific Calculations ------------------------- #
    def exp_range(self, prior_close, impvol):
        logger.debug(f" GAP_CRUDE | exp_range | Note: Running")
        
        exp_range = round(((prior_close * (impvol / 100)) * math.sqrt(1 / 252)), 2)
        
        return exp_range
    
    def prior_day(self, prior_high, prior_low, prior_close, ibh, ibl):
        
        if prior_high <= ibh and prior_low >= ibl:
            day_type = "Non-Trend"
        elif (prior_low < ibl and prior_high > ibh and 
            prior_close >= ibh + 0.5 * (ibh - ibl)):
            day_type = "Neutral Extreme ^"
        elif (prior_low < ibl and prior_high > ibh and 
            prior_close <= ibl - 0.5 * (ibh - ibl)):
            day_type = "Neutral Extreme v"
        elif (prior_high > ibh and prior_low < ibl and
            prior_close >= (ibl - 0.5 * (ibh - ibl)) and
            prior_close <= (ibh + 0.5 * (ibh - ibl))):
            day_type = "Neutral Center"
        elif (prior_high > ibh and prior_low >= ibl and 
            prior_high <= ibh + 0.5 * (ibh - ibl)):
            day_type = "Normal Day ^"
        elif (prior_low < ibl and prior_high <= ibh and 
            prior_low >= ibl - 0.5 * (ibh - ibl)):
            day_type = "Normal Day v"
        elif (prior_high > ibh and prior_low >= ibl and
            prior_high >= ibh + (ibh - ibl) and
            prior_close >= ibh + (ibh - ibl)):
            day_type = "Trend ^"
        elif (prior_high > ibh and prior_low >= ibl and
            prior_close <= ibh + (ibh - ibl) and
            prior_high >= ibh + 1.25 * (ibh - ibl)):
            day_type = "Trend ^"
        elif (prior_low < ibl and prior_high <= ibh and
            prior_low <= ibl - (ibh - ibl) and
            prior_close <= ibl - (ibh - ibl)):
            day_type = "Trend v"
        elif (prior_low < ibl and prior_high <= ibh and
            prior_close >= ibl - (ibh - ibl) and
            prior_low <= ibl - 1.25 * (ibh - ibl)):
            day_type = "Trend v"
        elif (prior_high > ibh and prior_low >= ibl and
            prior_high >= ibh + 0.5 * (ibh - ibl) and
            prior_high <= ibh + (ibh - ibl)):
            day_type = "Normal Var ^"
        elif (prior_high > ibh and prior_low >= ibl and
            prior_high >= ibh + (ibh - ibl) and
            prior_close <= ibh + (ibh - ibl)):
            day_type = "Normal Var ^"
        elif (prior_low < ibl and prior_high <= ibh and
            prior_low <= ibl - 0.5 * (ibh - ibl) and
            prior_low >= ibl - (ibh - ibl)):
            day_type = "Normal Var v"
        elif (prior_low < ibl and prior_high <= ibh and
            prior_low <= ibl - (ibh - ibl) and
            prior_close >= ibl - (ibh - ibl)):
            day_type = "Normal Var v"
        else:
            day_type = "Other"
        
        return day_type
    # ---------------------- Driving Input Logic ------------------------- #
    def input(self, day_type, day_open):
        if 
        return input
        
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
                logger.error(f" DATR_CRUDE | process_product | Product: {product_name} |  Note: No data available ")
                return
            
            # Variables specific to the product
            prior_close = round(variables.get(f'{product_name}_PRIOR_CLOSE'), 2)
            day_open = round(variables.get(f'{product_name}_DAY_OPEN'), 2)
            prior_high = round(variables.get(f'{product_name}_PRIOR_HIGH'), 2)
            prior_low = round(variables.get(f'{product_name}_PRIOR_LOW'), 2)
            ibh = round(variables.get(f'{product_name}_IB_HIGH'), 2)
            ibl = round(variables.get(f'{product_name}_IB_LOW'), 2)
            impvol = External_Config.cl_impvol
            color = self.product_color.get(product_name)
            current_time = datetime.now(self.est).strftime('%H:%M:%S')
            
            # Calculations
            day_type = self.prior_day(
                prior_high, prior_low, prior_close, ibh, ibl
                )
            input = self.input(
                day_type, day_open
                )
            
            if day_type == 'Trend ^':
               self.direction = 'Higher'
            elif day_type == 'Trend v':
               self.direction = 'Lower'
               
            direction_emojis = {
                'Higher': ':arrow_up:',
                'Lower': ':arrow_down:',
            }
            
            if input:
                
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
                    logger.info(f" DATR_CRUDE | process_product | Note: Message sent to {channel} for {product_name}")
                else:
                    logger.error(f" DATR_CRUDE | process_product | Note: No Slack channel configured for {product_name}")
            else:
                logger.info(f" DATR_CRUDE | process_product | Product: {product_name} | Note: DATR Not in play.")
        except Exception as e:
            logger.error(f" DATR_CRUDE | process_product | Product: {product_name} | Error processing: {e}")