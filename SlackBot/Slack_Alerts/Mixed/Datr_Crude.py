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
        logger.debug(f" DATR_CRUDE | exp_range | Note: Running")
        
        exp_range = round(((prior_close * (impvol / 100)) * math.sqrt(1 / 252)), 2)
        
        return exp_range
    
    def total_delta(self, total_ovn_delta, total_rth_delta):

        # Calculation (Product Specific or Not)        
        total_delta = total_ovn_delta + total_rth_delta
        
        logger.debug(f" DATR_CRUDE | total_delta | TOTAL_DELTA: {total_delta}")
        return total_delta  
     
    def prior_day(self, prior_high, prior_low, prior_close, prior_ibh, prior_ibl):
        
        if prior_high <= prior_ibh and prior_low >= prior_ibl:
            day_type = "Non-Trend"
        elif (prior_low < prior_ibl and prior_high > prior_ibh and 
            prior_close >= prior_ibh + 0.5 * (prior_ibh - prior_ibl)):
            day_type = "Neutral Extreme ^"
        elif (prior_low < prior_ibl and prior_high > prior_ibh and 
            prior_close <= prior_ibl - 0.5 * (prior_ibh - prior_ibl)):
            day_type = "Neutral Extreme v"
        elif (prior_high > prior_ibh and prior_low < prior_ibl and
            prior_close >= (prior_ibl - 0.5 * (prior_ibh - prior_ibl)) and
            prior_close <= (prior_ibh + 0.5 * (prior_ibh - prior_ibl))):
            day_type = "Neutral Center"
        elif (prior_high > prior_ibh and prior_low >= prior_ibl and 
            prior_high <= prior_ibh + 0.5 * (prior_ibh - prior_ibl)):
            day_type = "Normal Day ^"
        elif (prior_low < prior_ibl and prior_high <= prior_ibh and 
            prior_low >= prior_ibl - 0.5 * (prior_ibh - prior_ibl)):
            day_type = "Normal Day v"
        elif (prior_high > prior_ibh and prior_low >= prior_ibl and
            prior_high >= prior_ibh + (prior_ibh - prior_ibl) and
            prior_close >= prior_ibh + (prior_ibh - prior_ibl)):
            day_type = "Trend ^"
        elif (prior_high > prior_ibh and prior_low >= prior_ibl and
            prior_close <= prior_ibh + (prior_ibh - prior_ibl) and
            prior_high >= prior_ibh + 1.25 * (prior_ibh - prior_ibl)):
            day_type = "Trend ^"
        elif (prior_low < prior_ibl and prior_high <= prior_ibh and
            prior_low <= prior_ibl - (prior_ibh - prior_ibl) and
            prior_close <= prior_ibl - (prior_ibh - prior_ibl)):
            day_type = "Trend v"
        elif (prior_low < prior_ibl and prior_high <= prior_ibh and
            prior_close >= prior_ibl - (prior_ibh - prior_ibl) and
            prior_low <= prior_ibl - 1.25 * (prior_ibh - prior_ibl)):
            day_type = "Trend v"
        elif (prior_high > prior_ibh and prior_low >= prior_ibl and
            prior_high >= prior_ibh + 0.5 * (prior_ibh - prior_ibl) and
            prior_high <= prior_ibh + (prior_ibh - prior_ibl)):
            day_type = "Normal Var ^"
        elif (prior_high > prior_ibh and prior_low >= prior_ibl and
            prior_high >= prior_ibh + (prior_ibh - prior_ibl) and
            prior_close <= prior_ibh + (prior_ibh - prior_ibl)):
            day_type = "Normal Var ^"
        elif (prior_low < prior_ibl and prior_high <= prior_ibh and
            prior_low <= prior_ibl - 0.5 * (prior_ibh - prior_ibl) and
            prior_low >= prior_ibl - (prior_ibh - prior_ibl)):
            day_type = "Normal Var v"
        elif (prior_low < prior_ibl and prior_high <= prior_ibh and
            prior_low <= prior_ibl - (prior_ibh - prior_ibl) and
            prior_close >= prior_ibl - (prior_ibh - prior_ibl)):
            day_type = "Normal Var v"
        else:
            day_type = "Other"
        
        return day_type
    # ---------------------- Driving Input Logic ------------------------- #
    def input(self, day_open, prior_high, prior_low, prior_vpoc, exp_rng):
        tolerance = (exp_rng * 0.15)
        prior_mid = (prior_high + prior_low) / 2
        
        if (prior_high - tolerance) > day_open > (prior_low + tolerance):
            if self.direction == 'Higher':
                input = (
                    day_open > prior_mid 
                    and
                    prior_vpoc > ((prior_high + prior_mid) / 2) 
                )
            elif self.direction == 'Lower':
                input = (
                    day_open < prior_mid
                    and 
                    prior_vpoc < ((prior_low + prior_mid) / 2)
                )
        else: 
            input = False
        return input
        
    def send_alert(self):
        threads = []
        for product_name in ['CL']:
            thread = threading.Thread(target=self.check, args=(product_name,))
            thread.start()
            threads.append(thread)
            time.sleep(1)

        # Optionally wait for all threads to complete
        for thread in threads:
            thread.join()
    # ---------------------- Alert Preparation ------------------------- #
    def check(self, product_name):
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
            prior_ibh = round(variables.get(f'{product_name}_PRIOR_IB_HIGH'), 2)
            prior_ibl = round(variables.get(f'{product_name}_PRIOR_IB_LOW'), 2)
            total_ovn_delta = round(variables.get(f'{product_name}_TOTAL_OVN_DELTA'), 2)
            total_rth_delta = round(variables.get(f'{product_name}_TOTAL_RTH_DELTA'), 2)
            prior_vpoc = round(variables.get(f'{product_name}_PRIOR_VPOC'), 2)  
            eth_vwap = round(variables.get(f'{product_name}_ETH_VWAP'), 2)       
            impvol = External_Config.cl_impvol
            color = self.product_color.get(product_name)
            current_time = datetime.now(self.est).strftime('%H:%M:%S')
            
            # Calculations
            prior_day_type = self.prior_day(
                prior_high, prior_low, prior_close, prior_ibh, prior_ibl
                )
            delta = self.total_delta(
                total_ovn_delta, total_rth_delta
                )
            exp_rng = self.exp_range(
                prior_close, impvol
                )
            prior_mid = (prior_high + prior_low) / 2
            
            if prior_day_type == 'Trend ^':
               self.direction = 'Higher'
            elif prior_day_type == 'Trend v':
               self.direction = 'Lower'
               
            direction_emojis = {
                'Higher': ':arrow_up:',
                'Lower': ':arrow_down:',
            }
            
            if self.input(day_open, prior_high, prior_low, prior_vpoc, exp_rng):
                try:    
                    # Logic For c_orderflow
                    c_orderflow = "  "
                    if self.direction == "Lower" and delta < 0:
                        c_orderflow = "x"
                    elif self.direction == "Higher" and delta > 0:
                        c_orderflow = "x"
                        
                    # Logic for c_open
                    if prior_low < day_open < prior_high:
                        c_open = "x"
                    else:
                        c_open = "  "
                    
                    # Logic for c_trend
                    c_trend = "x"
                    
                    # Logic for c_open_mid
                    c_open_mid = "  "
                    if self.direction == "Lower" and day_open < prior_mid:
                        c_between = "x"
                    elif self.direction == "Higher" and day_open > prior_mid:
                        c_between = "x"
                        
                    # Logic for c_vwap
                    c_vwap = "  "
                    if self.direction == "Lower" and:
                        c_vwap = "x"
                    elif self.direction == "Higher" and:
                        c_vwap = "x"
                        

                        
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
                except Exception as e:
                    logger.error(f" DATR_CRUDE | process_product | Product: {product_name} | Error Calculating Criteria: {e}")
            else:
                logger.info(f" DATR_CRUDE | process_product | Product: {product_name} | Note: DATR Not in play.")
        except Exception as e:
            logger.error(f" DATR_CRUDE | process_product | Product: {product_name} | Error processing: {e}")