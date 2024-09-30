import logging
import math
from datetime import datetime
from SlackBot.External import External_Config
from SlackBot.Slack_Alerts.Periodic.Base import Base_Periodic

logger = logging.getLogger(__name__)

class IB_Equity_Alert(Base_Periodic):
    def __init__(self, files):
        super().__init__(files)
        
# ---------------------- Specific Calculations ------------------------- #
    def ib_info(self):
        
        # Calculations
        ib_range = self.ib_high - self.ib_low
        ib_vatr = ib_range / self.ib_atr
       
        if ib_vatr > 1.1:
            ib_type = "Wide IB"
        elif ib_vatr < 0.85:
            ib_type = "Narrow IB"
        elif 0.85 <= ib_vatr <= 1.1:
            ib_type = "Average IB"

        return ib_range, ib_type, ib_vatr
        
    def exp_range_info(self):
        
        # Calculations
        if self.product_name == 'ES':
            exp_range = round(((self.prior_close * (self.es_impvol / 100)) * math.sqrt(1 / 252)), 2)
        elif self.product_name == 'NQ':
            exp_range = round(((self.prior_close * (self.nq_impvol / 100)) * math.sqrt(1 / 252)), 2)
        elif self.product_name == 'RTY':
            exp_range = round(((self.prior_close * (self.rty_impvol / 100)) * math.sqrt(1 / 252)), 2)
        elif self.product_name == 'CL':
            exp_range = round(((self.prior_close * (self.cl_impvol / 100)) * math.sqrt(1 / 252)), 2)
        else:
            raise ValueError(f"Unknown product: {self.product_name}")

        exp_hi = round(self.prior_close + exp_range, 2)
        exp_lo = round(self.prior_close - exp_range, 2)
        range_used = ((self.ovn_to_ibh - self.ovn_to_ibl) / exp_range)

        if abs(range_used) > 1:
            exhausted = "Exhausted"
        else:
            exhausted = "Nominal"

        if self.cpl > exp_hi:
            range_up = "Exhausted"
        else:
            range_up_value = abs((exp_hi - self.cpl) / (exp_range * 2))
            range_up = f"{range_up_value:.2f}"

        # Calculate range_down
        if self.cpl < exp_lo:
            range_down = "Exhausted"
        else:
            range_down_value = abs((exp_lo - self.cpl) / (exp_range * 2))
            range_down = f"{range_down_value:.2f}"
        
        return exhausted, range_used, range_up, range_down, exp_range

    def gap_info(self):
        # Get values from exp_range_info, including exp_range
        exhausted, range_used, range_up, range_down, exp_range = self.exp_range_info()

        # Initialize gap and gap_tier
        gap = ""
        gap_tier = ""
        
        # Check for Gap Up
        if self.day_open > self.prior_high:
            gap_size = round(self.day_open - self.prior_high, 2)
            gap = f"Gap Up: {gap_size}"
            
            # Calculate gap_tier
            if exp_range == 0:
                gap_tier = "Undefined"  # Avoid division by zero
            else:
                gap_ratio = gap_size / exp_range
                if gap_ratio <= 0.5:
                    gap_tier = "Tier 1"
                elif gap_ratio <= 0.75:
                    gap_tier = "Tier 2"
                else:
                    gap_tier = "Tier 3"
        
        # Check for Gap Down
        elif self.day_open < self.prior_low:
            gap_size = round(self.prior_low - self.day_open, 2)
            gap = f"Gap Down: {gap_size}"
            
            # Calculate gap_tier
            if exp_range == 0:
                gap_tier = "Undefined"  # Avoid division by zero
            else:
                gap_ratio = gap_size / exp_range
                if gap_ratio <= 0.5:
                    gap_tier = "Tier 1"
                elif gap_ratio <= 0.75:
                    gap_tier = "Tier 2"
                else:
                    gap_tier = "Tier 3"
        
        # No Gap
        else:
            gap = "No Gap"
            gap_tier = ""
        
        return gap, gap_tier

    def posture(self):
        exhausted, range_used, range_up, range_down, exp_range = self.exp_range_info()
        
        threshold = exp_range * 0.68

        if (abs(self.cpl - self.fd_vpoc) <= threshold) and (abs(self.fd_vpoc - self.td_vpoc) <= threshold):
            posture = "Price=5D=20D"
        elif (self.cpl > self.fd_vpoc + threshold) and (self.fd_vpoc > self.td_vpoc + threshold):
            posture = "Price^5D^20D"
        elif (self.cpl < self.fd_vpoc - threshold) and (self.fd_vpoc < self.td_vpoc - threshold):
            posture = "Pricev5Dv20D"
        elif (abs(self.cpl - self.fd_vpoc) <= threshold) and (self.fd_vpoc > self.td_vpoc + threshold):
            posture = "Price=5D^20D"
        elif (self.cpl > self.fd_vpoc + threshold) and (abs(self.fd_vpoc - self.td_vpoc) <= threshold):
            posture = "Price^5D=20D"
        elif (self.cpl < self.fd_vpoc - threshold) and (abs(self.fd_vpoc - self.td_vpoc) <= threshold):
            posture = "Pricev5D=20D"
        elif (abs(self.cpl - self.fd_vpoc) <= threshold) and (self.fd_vpoc < self.td_vpoc - threshold):
            posture = "Price=5Dv20D"
        elif (self.cpl > self.fd_vpoc + threshold) and (self.fd_vpoc < self.td_vpoc - threshold):
            posture = "Price^5Dv20D"
        elif (self.cpl < self.fd_vpoc - threshold) and (self.fd_vpoc > self.td_vpoc + threshold):
            posture = "Pricev5D^20D"
        else:
            posture = "Other"

        return posture
        
    def open_type(self):
        A_period_mid = (self.a_high + self.a_low) / 2
        A_period_range = self.a_high - self.a_low
        overlap = max(0, min(max(self.a_high, self.b_high), self.prior_high) - max(min(self.a_low, self.b_low), self.prior_low))
        total_range = max(self.a_high, self.b_high) - min(self.a_low, self.b_low)

        if (self.day_open > A_period_mid) and (self.b_high < A_period_mid):
            open_type = "OTD v"
        elif (self.day_open < A_period_mid) and (self.b_low > A_period_mid):
            open_type = "OTD ^"
        elif (abs(self.day_open - self.a_high) <= 0.05 * A_period_range) and (self.b_high < A_period_mid):
            open_type = "OD v"
        elif (abs(self.day_open - self.a_low) <= 0.05 * A_period_range) and (self.b_low > A_period_mid):
            open_type = "OD ^"
        elif (self.day_open > A_period_mid) and (self.b_low > A_period_mid) and (self.b_high > self.orh):
            open_type = "ORR ^"
        elif (self.day_open < A_period_mid) and (self.b_high < A_period_mid) and (self.b_low < self.orl):
            open_type = "ORR v"
        elif overlap >= 0.5 * total_range:
            open_type = "OAIR"
        elif (overlap < 0.5 * total_range) and (self.day_open > self.prior_high):
            open_type = "OAOR ^"
        elif (overlap < 0.5 * total_range) and (self.day_open < self.prior_low):
            open_type = "OAOR v"
        else:
            open_type = "Other"

        return open_type
# ---------------------- Alert Preparation ------------------------- #
    def send_alert(self):
        for self.product_name in ['ES', 'NQ', 'RTY']:
            self.variables = self.fetch_latest_variables(self.product_name) 
            if not self.variables:
                print(f"No data available for {self.product_name}")
                continue
            
            # Variables
            self.ib_atr = self.variables.get(f'{self.product_name}_IB_ATR')
            self.ib_high = self.variables.get(f'{self.product_name}_IB_HIGH')
            self.ib_low = self.variables.get(f'{self.product_name}_IB_LOW')
            self.prior_close = self.variables.get(f'{self.product_name}_PRIOR_CLOSE')
            self.day_open = self.variables.get(f'{self.product_name}_DAY_OPEN')
            self.prior_high= self.variables.get(f'{self.product_name}_PRIOR_HIGH')
            self.prior_low = self.variables.get(f'{self.product_name}_PRIOR_LOW')
            self.cpl = self.variables.get(f'{self.product_name}_CPL')
            self.fd_vpoc = self.variables.get(f'{self.product_name}_5D_VPOC')
            self.td_vpoc = self.variables.get(f'{self.product_name}_20D_VPOC')
            self.ovn_to_ibh = self.variables.get(f'{self.product_name}_OVNTOIB_HI')
            self.ovn_to_ibl = self.variables.get(f'{self.product_name}_OVNTOIB_LO')
            self.a_high = self.variables.get(f'{self.product_name}_A_HIGH')
            self.a_low = self.variables.get(f'{self.product_name}_A_LOW')
            self.b_high = self.variables.get(f'{self.product_name}_B_HIGH')
            self.b_low = self.variables.get(f'{self.product_name}_B_LOW')
            self.orh = self.variables.get(f'{self.product_name}_ORH')
            self.orl = self.variables.get(f'{self.product_name}_ORL')
            rvol = self.variables.get(f'{self.product_name}_CUMULATIVE_RVOL') 
             
            self.es_impvol = External_Config.es_impvol
            self.nq_impvol = External_Config.nq_impvol
            self.rty_impvol = External_Config.rty_impvol
            self.cl_impvol = External_Config.cl_impvol 
            
            # Message Variables
            color = self.product_color.get(self.product_name)
            rvol = self.variables.get(f'{self.product_name}_CUMULATIVE_RVOL')
            ib_range, ib_type, ib_vatr = self.ib_info()
            exhausted, range_used, range_up, range_down, exp_range = self.exp_range_info()
            gap, gap_tier = self.gap_info()
            posture = self.posture()
            open_type = self.open_type()
            
            # Message Template
            message = (
                f">:large_{color}_square: *{self.product_name} - Alert - IB Check-In* :large_{color}_square:\n"
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
                f">*Alert Time*: _{self.current_time}_\n"
            )
            
            # Send Slack Alert
            channel = self.slack_channels.get(self.product_name)
            if channel:
                self.slack_client.chat_postMessage(channel=channel, text=message) 
                print(f"Message send to {channel} for {self.product_name}")
            else:
                print(f"No Slack channel configured for {self.product_name}")
                