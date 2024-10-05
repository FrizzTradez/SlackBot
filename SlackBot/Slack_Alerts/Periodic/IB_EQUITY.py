import logging
import math
from SlackBot.External import External_Config
from SlackBot.Slack_Alerts.Periodic.Base import Base_Periodic
import threading
from datetime import datetime
import time

logger = logging.getLogger(__name__)

class IB_Equity_Alert(Base_Periodic):
    def __init__(self, files):
        super().__init__(files)
        
    # ---------------------- Specific Calculations ------------------------- #
    def ib_info(self, ib_high, ib_low, ib_atr):
        logger.debug(f" IB_EQUITY | ib_info | Note: Running")
        
        ib_range = round((ib_high - ib_low), 2)
        ib_vatr = round((ib_range / ib_atr), 2)
       
        if ib_vatr > 1.1:
            ib_type = "Wide IB"
        elif ib_vatr < 0.85:
            ib_type = "Narrow IB"
        elif 0.85 <= ib_vatr <= 1.1:
            ib_type = "Average IB"
        
        return ib_range, ib_type, ib_vatr*100
        
    def exp_range_info(self, prior_close, cpl, ovn_to_ibh, ovn_to_ibl, impvol):
        logger.debug(f" IB_EQUITY | exp_range_info | Note: Running")
        
        exp_range = round(((prior_close * (impvol / 100)) * math.sqrt(1 / 252)), 2)
        exp_hi = (prior_close + exp_range)
        exp_lo = (prior_close - exp_range)
        range_used = round(((ovn_to_ibh - ovn_to_ibl) / exp_range), 2)

        if abs(range_used) >= 1:
            exhausted = "Exhausted"
        elif abs(range_used) <= 0.55:
            exhausted = "Below Avg"
        else:
            exhausted = "Nominal"

        if cpl > exp_hi:
            range_up = "Exhausted"
        else:
            range_up = round(abs((exp_hi - cpl) / (exp_range * 2) * 100), 2)

        if cpl < exp_lo:
            range_down = "Exhausted"
        else:
            range_down = round(abs((exp_lo - cpl) / (exp_range * 2) * 100), 2)
        
        return exhausted, range_used*100, range_up, range_down, exp_range

    def gap_info(self, day_open, prior_high, prior_low, exp_range):
        logger.debug(f" IB_EQUITY | gap_info | Note: Running")
        
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

    def posture(self, cpl, fd_vpoc, td_vpoc, exp_range):
        logger.debug(f" IB_EQUITY | posture | Note: Running")
        
        threshold = round((exp_range * 0.68), 2)

        if (abs(cpl - fd_vpoc) <= threshold) and (abs(fd_vpoc - td_vpoc) <= threshold):
            posture = "PRICE=5D=20D"
        elif (cpl > fd_vpoc + threshold) and (fd_vpoc > td_vpoc + threshold):
            posture = "PRICE^5D^20D"
        elif (cpl < fd_vpoc - threshold) and (fd_vpoc < td_vpoc - threshold):
            posture = "PRICEv5Dv20D"
        elif (abs(cpl - fd_vpoc) <= threshold) and (fd_vpoc > td_vpoc + threshold):
            posture = "PRICE=5D^20D"
        elif (cpl > fd_vpoc + threshold) and (abs(fd_vpoc - td_vpoc) <= threshold):
            posture = "PRICE^5D=20D"
        elif (cpl < fd_vpoc - threshold) and (abs(fd_vpoc - td_vpoc) <= threshold):
            posture = "PRICEv5D=20D"
        elif (abs(cpl - fd_vpoc) <= threshold) and (fd_vpoc < td_vpoc - threshold):
            posture = "PRICE=5Dv20D"
        elif (cpl > fd_vpoc + threshold) and (fd_vpoc < td_vpoc - threshold):
            posture = "PRICE^5Dv20D"
        elif (cpl < fd_vpoc - threshold) and (fd_vpoc > td_vpoc + threshold):
            posture = "PRICEv5D^20D"
        else:
            posture = "Other"

        return posture
        
    def open_type(self, a_high, a_low, b_high, b_low, day_open, orh, orl, prior_high, prior_low):
        logger.debug(f" IB_EQUITY | open_type | Note: Running")
        
        a_period_mid = round(((a_high + a_low) / 2), 2)
        a_period_range = (a_high - a_low)
        overlap = max(0, min(max(a_high, b_high), prior_high) - max(min(a_low, b_low), prior_low))
        total_range = max(a_high, b_high) - min(a_low, b_low)

        if (abs(day_open - a_high) <= 0.025 * a_period_range) and (b_high < a_period_mid):
            open_type = "OD v"
        elif (abs(day_open - a_low) <= 0.025 * a_period_range) and (b_low > a_period_mid):
            open_type = "OD ^"
        elif (day_open > a_period_mid) and (b_high < a_period_mid):
            open_type = "OTD v"
        elif (day_open < a_period_mid) and (b_low > a_period_mid):
            open_type = "OTD ^"
        elif (day_open > a_period_mid) and (b_low > a_period_mid) and (b_high > orh):
            open_type = "ORR ^"
        elif (day_open < a_period_mid) and (b_high < a_period_mid) and (b_low < orl):
            open_type = "ORR v"
        elif overlap >= 0.5 * total_range:
            open_type = "OAIR"
        elif (overlap < 0.5 * total_range) and (day_open >= prior_high):
            open_type = "OAOR ^"
        elif (overlap < 0.5 * total_range) and (day_open <= prior_low):
            open_type = "OAOR v"
        else:
            open_type = "Other"

        return open_type
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
                logger.error(f" IB_EQUITY | process_product | Product: {product_name} |  Note: No data available ")
                return
            
            # Variables (Round All Variables) 
            ib_atr = round(variables.get(f'{product_name}_IB_ATR'), 2)
            ib_high = round(variables.get(f'{product_name}_IB_HIGH'), 2)
            ib_low = round(variables.get(f'{product_name}_IB_LOW'), 2)
            prior_close = round(variables.get(f'{product_name}_PRIOR_CLOSE'), 2)
            day_open = round(variables.get(f'{product_name}_DAY_OPEN'), 2)
            prior_high = round(variables.get(f'{product_name}_PRIOR_HIGH'), 2)
            prior_low = round(variables.get(f'{product_name}_PRIOR_LOW'), 2)
            cpl = round(variables.get(f'{product_name}_CPL'), 2)
            fd_vpoc = round(variables.get(f'{product_name}_5D_VPOC'), 2)
            td_vpoc = round(variables.get(f'{product_name}_20D_VPOC'), 2)
            ovn_to_ibh = round(variables.get(f'{product_name}_OVNTOIB_HI'), 2)
            ovn_to_ibl = round(variables.get(f'{product_name}_OVNTOIB_LO'), 2)
            a_high = round(variables.get(f'{product_name}_A_HIGH'), 2)
            a_low = round(variables.get(f'{product_name}_A_LOW'), 2)
            b_high = round(variables.get(f'{product_name}_B_HIGH'), 2)
            b_low = round(variables.get(f'{product_name}_B_LOW'), 2)
            orh = round(variables.get(f'{product_name}_ORH'), 2)
            orl = round(variables.get(f'{product_name}_ORL'), 2)
            rvol = round(variables.get(f'{product_name}_CUMULATIVE_RVOL'), 2)
            
            # Implied volatility specific to the product
            if product_name == 'ES':
                impvol = External_Config.es_impvol
            elif product_name == 'NQ':
                impvol = External_Config.nq_impvol
            elif product_name == 'RTY':
                impvol = External_Config.rty_impvol
            else:
                raise ValueError(f" IB_EQUITY | process_product | Note: {product_name}")
            
            color = self.product_color.get(product_name)
            current_time = datetime.now(self.est).strftime('%H:%M:%S')
            
            # Calculations
            ib_range, ib_type, ib_vatr = self.ib_info(
                ib_high, ib_low, ib_atr
                )
            exhausted, range_used, range_up, range_down, exp_range = self.exp_range_info(
                prior_close, cpl, ovn_to_ibh, ovn_to_ibl, impvol
                )
            gap, gap_tier, gap_size = self.gap_info(
                day_open, prior_high, prior_low, exp_range
                )
            posture = self.posture(
                cpl, fd_vpoc, td_vpoc, exp_range
                )
            open_type = self.open_type(
                a_high, a_low, b_high, b_low, day_open, orh, orl, prior_high, prior_low
                )
            
            # Message Template
            message = (
                f">:large_{color}_square:  *{product_name} - Alert - IB Check-In*  :large_{color}_square:\n"
                "────────────────────\n"
                f">                *Session Stats*\n"             
                f"*Open Type*: _{open_type}_\n"
                f"*{ib_type}*: _{ib_range}p_ = _{ib_vatr}%_ of Avg\n"
                f"{f'*{gap}*: _{gap_size}_ = _{gap_tier}_\n' if gap != 'No Gap' else ''}"
                f"*Rvol*: _{rvol}%_\n"
                f"*Current Posture*: _{posture}_\n"
                "────────────────────\n"
                f">             *Expected Range*\n"
                f"*Rng Used*: _{exhausted}_ | _{range_used}%_ Used\n"
                f"*Range Left Up*: _{range_up}{'' if range_up == 'Exhausted' else '%'}_\n"
                f"*Range Left Down*: _{range_down}{'' if range_down == 'Exhausted' else '%'}_\n"
                "────────────────────\n"                
                f">*Alert Time*: _{current_time} EST_\n"
            )
                
            # Send Slack Alert
            channel = self.slack_channels.get(product_name)
            if channel:
                self.slack_client.chat_postMessage(channel=channel, text=message) 
                logger.info(f" IB_EQUITY | process_product | Note: Message sent to {channel} for {product_name}")
            else:
                logger.error(f" IB_EQUITY | process_product | Note: No Slack channel configured for {product_name}")
        except Exception as e:
            logger.error(f" IB_EQUITY | process_product | Product: {product_name} | Error processing: {e}")
                