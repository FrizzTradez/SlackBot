import logging
import math
from SlackBot.External import External_Config
from SlackBot.Slack_Alerts.Periodic.Base import Base_Periodic
import threading
from datetime import datetime

logger = logging.getLogger(__name__)

class IB_Equity_Alert(Base_Periodic):
    def __init__(self, files):
        super().__init__(files)
        
    # ---------------------- Specific Calculations ------------------------- #
    def ib_info(self, ib_high, ib_low, ib_atr):
        ib_range = ib_high - ib_low
        ib_vatr = ib_range / ib_atr
       
        if ib_vatr > 1.1:
            ib_type = "Wide IB"
        elif ib_vatr < 0.85:
            ib_type = "Narrow IB"
        elif 0.85 <= ib_vatr <= 1.1:
            ib_type = "Average IB"

        return ib_range, ib_type, ib_vatr
        
    def exp_range_info(self, prior_close, cpl, ovn_to_ibh, ovn_to_ibl, impvol):
        exp_range = round(((prior_close * (impvol / 100)) * math.sqrt(1 / 252)), 2)
        exp_hi = round(prior_close + exp_range, 2)
        exp_lo = round(prior_close - exp_range, 2)
        range_used = ((ovn_to_ibh - ovn_to_ibl) / exp_range)

        if abs(range_used) > 1:
            exhausted = "Exhausted"
        else:
            exhausted = "Nominal"

        if cpl > exp_hi:
            range_up = "Exhausted"
        else:
            range_up_value = abs((exp_hi - cpl) / (exp_range * 2))
            range_up = f"{range_up_value:.2f}"

        if cpl < exp_lo:
            range_down = "Exhausted"
        else:
            range_down_value = abs((exp_lo - cpl) / (exp_range * 2))
            range_down = f"{range_down_value:.2f}"
        
        return exhausted, range_used, range_up, range_down, exp_range

    def gap_info(self, day_open, prior_high, prior_low, exp_range):
        gap = ""
        gap_tier = ""
        
        if day_open > prior_high:
            gap_size = round(day_open - prior_high, 2)
            gap = f"Gap Up: {gap_size}"
            
            if exp_range == 0:
                gap_tier = "Undefined"  
            else:
                gap_ratio = gap_size / exp_range
                if gap_ratio <= 0.5:
                    gap_tier = "Tier 1"
                elif gap_ratio <= 0.75:
                    gap_tier = "Tier 2"
                else:
                    gap_tier = "Tier 3"
        
        elif day_open < prior_low:
            gap_size = round(prior_low - day_open, 2)
            gap = f"Gap Down: {gap_size}"
            
            if exp_range == 0:
                gap_tier = "Undefined" 
            else:
                gap_ratio = gap_size / exp_range
                if gap_ratio <= 0.5:
                    gap_tier = "Tier 1"
                elif gap_ratio <= 0.75:
                    gap_tier = "Tier 2"
                else:
                    gap_tier = "Tier 3"
        
        else:
            gap = "No Gap"
            gap_tier = ""
        
        return gap, gap_tier

    def posture(self, cpl, fd_vpoc, td_vpoc, exp_range):
        threshold = exp_range * 0.68

        if (abs(cpl - fd_vpoc) <= threshold) and (abs(fd_vpoc - td_vpoc) <= threshold):
            posture = "Price=5D=20D"
        elif (cpl > fd_vpoc + threshold) and (fd_vpoc > td_vpoc + threshold):
            posture = "Price^5D^20D"
        elif (cpl < fd_vpoc - threshold) and (fd_vpoc < td_vpoc - threshold):
            posture = "Pricev5Dv20D"
        elif (abs(cpl - fd_vpoc) <= threshold) and (fd_vpoc > td_vpoc + threshold):
            posture = "Price=5D^20D"
        elif (cpl > fd_vpoc + threshold) and (abs(fd_vpoc - td_vpoc) <= threshold):
            posture = "Price^5D=20D"
        elif (cpl < fd_vpoc - threshold) and (abs(fd_vpoc - td_vpoc) <= threshold):
            posture = "Pricev5D=20D"
        elif (abs(cpl - fd_vpoc) <= threshold) and (fd_vpoc < td_vpoc - threshold):
            posture = "Price=5Dv20D"
        elif (cpl > fd_vpoc + threshold) and (fd_vpoc < td_vpoc - threshold):
            posture = "Price^5Dv20D"
        elif (cpl < fd_vpoc - threshold) and (fd_vpoc > td_vpoc + threshold):
            posture = "Pricev5D^20D"
        else:
            posture = "Other"

        return posture
        
    def open_type(self, a_high, a_low, b_high, b_low, day_open, orh, orl, prior_high, prior_low):
        a_period_mid = (a_high + a_low) / 2
        a_period_range = a_high - a_low
        overlap = max(0, min(max(a_high, b_high), prior_high) - max(min(a_low, b_low), prior_low))
        total_range = max(a_high, b_high) - min(a_low, b_low)

        if (day_open > a_period_mid) and (b_high < a_period_mid):
            open_type = "OTD v"
        elif (day_open < a_period_mid) and (b_low > a_period_mid):
            open_type = "OTD ^"
        elif (abs(day_open - a_high) <= 0.05 * a_period_range) and (b_high < a_period_mid):
            open_type = "OD v"
        elif (abs(day_open - a_low) <= 0.05 * a_period_range) and (b_low > a_period_mid):
            open_type = "OD ^"
        elif (day_open > a_period_mid) and (b_low > a_period_mid) and (b_high > orh):
            open_type = "ORR ^"
        elif (day_open < a_period_mid) and (b_high < a_period_mid) and (b_low < orl):
            open_type = "ORR v"
        elif overlap >= 0.5 * total_range:
            open_type = "OAIR"
        elif (overlap < 0.5 * total_range) and (day_open > prior_high):
            open_type = "OAOR ^"
        elif (overlap < 0.5 * total_range) and (day_open < prior_low):
            open_type = "OAOR v"
        else:
            open_type = "Other"

        return open_type
    # ---------------------- Alert Preparation ------------------------- #
    def send_alert(self):
        threads = []
        for product_name in ['CL']:
            thread = threading.Thread(target=self.process_product, args=(product_name,))
            thread.start()
            threads.append(thread)

        # Optionally wait for all threads to complete
        for thread in threads:
            thread.join()

    def process_product(self, product_name):
        try:
            variables = self.fetch_latest_variables(product_name)
            if not variables:
                print(f"No data available for {product_name}")
                return
            
            # Variables specific to the product
            ib_atr = variables.get(f'{product_name}_IB_ATR')
            ib_high = variables.get(f'{product_name}_IB_HIGH')
            ib_low = variables.get(f'{product_name}_IB_LOW')
            prior_close = variables.get(f'{product_name}_PRIOR_CLOSE')
            day_open = variables.get(f'{product_name}_DAY_OPEN')
            prior_high = variables.get(f'{product_name}_PRIOR_HIGH')
            prior_low = variables.get(f'{product_name}_PRIOR_LOW')
            cpl = variables.get(f'{product_name}_CPL')
            fd_vpoc = variables.get(f'{product_name}_5D_VPOC')
            td_vpoc = variables.get(f'{product_name}_20D_VPOC')
            ovn_to_ibh = variables.get(f'{product_name}_OVNTOIB_HI')
            ovn_to_ibl = variables.get(f'{product_name}_OVNTOIB_LO')
            a_high = variables.get(f'{product_name}_A_HIGH')
            a_low = variables.get(f'{product_name}_A_LOW')
            b_high = variables.get(f'{product_name}_B_HIGH')
            b_low = variables.get(f'{product_name}_B_LOW')
            orh = variables.get(f'{product_name}_ORH')
            orl = variables.get(f'{product_name}_ORL')
            rvol = variables.get(f'{product_name}_CUMULATIVE_RVOL') 
            
            impvol = External_Config.cl_impvol

            color = self.product_color.get(product_name)
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Calculations
            ib_range, ib_type, ib_vatr = self.ib_info(
                ib_high, ib_low, ib_atr
                )
            exhausted, range_used, range_up, range_down, exp_range = self.exp_range_info(
                prior_close, cpl, ovn_to_ibh, ovn_to_ibl, impvol
                )
            gap, gap_tier = self.gap_info(
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
                f">:large_{color}_square: *{product_name} - Alert - IB Check-In* :large_{color}_square:\n"
                "──────────────────────\n"
                f"*{ib_type}*: {ib_range}p = {ib_vatr} of Avg\n"
                f"*Expected Range*: _{exhausted}_ {range_used} Used\n"
                "────────────────\n"
                f"*Rng Up*: _{range_up}_\n"
                f"*Rng Down*: _{range_down}_\n"
                "────────────────\n"
                f"*{gap}* = _{gap_tier}_\n"
                f"*RVOL*: {rvol}\n"
                f"*Current Posture*: {posture}\n"
                "──────────────────────\n"
                f">*Alert Time*: _{current_time}_\n"
            )
            
            # Send Slack Alert
            channel = self.slack_channels.get(product_name)
            if channel:
                self.slack_client.chat_postMessage(channel=channel, text=message) 
                print(f"Message sent to {channel} for {product_name}")
            else:
                print(f"No Slack channel configured for {product_name}")
        except Exception as e:
            logger.error(f"Error processing {product_name}: {e}")