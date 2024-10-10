import logging
import threading
from datetime import datetime
from SlackBot.Slack_Alerts.Conditional.Base import Base_Conditional

logger = logging.getLogger(__name__)

last_alerts = {}
last_alerts_lock = threading.Lock()

class NEUTRAL(Base_Conditional):
    def __init__(self, product_name, variables):    
        super().__init__(product_name, variables)
        
        # Variables (Round All Variables)
        self.cpl = round(self.variables.get(f'{self.product_name}_CPL'), 2)
        self.ib_high = round(self.variables.get(f'{product_name}_IB_HIGH'), 2)
        self.ib_low = round(self.variables.get(f'{product_name}_IB_LOW'), 2)
        self.day_high = round(self.variables.get(f'{product_name}_DAY_HIGH'), 2)
        self.day_low = round(self.variables.get(f'{product_name}_DAY_LOW'), 2)
        
# ---------------------------------- Driving Input Logic ------------------------------------ #      
    def input(self, last_state):
        logger.debug(f" NEUTRAL | input | Product: {self.product_name} | Note: Running")

        last_extension = last_state['last_extension']
        neutral_alert_sent = last_state['neutral_alert_sent']
        has_gone_neutral = last_state.get('has_gone_neutral', False)

        logic = False
        self.neutral_type = None

        # Update last_extension based on current day_high and day_low
        extension_occurred = False

        if self.day_high > self.ib_high and last_extension != 'IBH':
            last_state['last_extension'] = 'IBH'
            last_state['neutral_alert_sent'] = False  # Reset alert flag
            extension_occurred = True
            logger.debug(f" NEUTRAL | input | Product: {self.product_name} | Note: Extended IB High")
        elif self.day_low < self.ib_low and last_extension != 'IBL':
            last_state['last_extension'] = 'IBL'
            last_state['neutral_alert_sent'] = False  # Reset alert flag
            extension_occurred = True
            logger.debug(f" NEUTRAL | input | Product: {self.product_name} | Note: Extended IB Low")

        if not has_gone_neutral:
            # Check for first neutral condition
            if last_state['last_extension'] == 'IBH' and self.day_low < self.ib_low and not neutral_alert_sent:
                logic = True
                self.neutral_type = 'Neutral_Lower'
                last_state['has_gone_neutral'] = True
                logger.debug(f" NEUTRAL | input | Product: {self.product_name} | Note: Neutral Lower detected (First Neutral)")
            elif last_state['last_extension'] == 'IBL' and self.day_high > self.ib_high and not neutral_alert_sent:
                logic = True
                self.neutral_type = 'Neutral_Higher'
                last_state['has_gone_neutral'] = True
                logger.debug(f" NEUTRAL | input | Product: {self.product_name} | Note: Neutral Higher detected (First Neutral)")
        else:
            # After first neutral, any new extension is considered neutral
            if extension_occurred and not neutral_alert_sent:
                logic = True
                if last_state['last_extension'] == 'IBH':
                    self.neutral_type = 'Neutral_Higher'
                elif last_state['last_extension'] == 'IBL':
                    self.neutral_type = 'Neutral_Lower'
                logger.debug(f" NEUTRAL | input | Product: {self.product_name} | Note: {self.neutral_type} detected (Subsequent Neutral)")

        return logic
# ---------------------------------- Opportunity Window ------------------------------------ #   
    def time_window(self):
        logger.debug(f" NEUTRAL | time_window | Product: {self.product_name} | Note: Running")
        
        # Update current time
        self.current_datetime = datetime.now(self.est)
        self.current_time = self.current_datetime.time()
        
        # Define time windows based on product type
        if self.product_name == 'CL':
            start_time = self.crude_ib
            end_time = self.crude_close
            logger.debug(f" NEUTRAL | time_window | Product: {self.product_name} | Time Window: {start_time} - {end_time}")
        elif self.product_name in ['ES', 'RTY', 'NQ']:
            start_time = self.equity_ib
            end_time = self.equity_close
            logger.debug(f" NEUTRAL | time_window | Product: {self.product_name} | Time Window: {start_time} - {end_time}")
        else:
            logger.warning(f" NEUTRAL | time_window | Product: {self.product_name} | No time window defined.")
            return False  
        
        # Check if current time is within the window
        if start_time <= self.current_time <= end_time:
            logger.debug(f" NEUTRAL | time_window | Product: {self.product_name} | Within Window: {self.current_time}.")
            return True
        else:
            logger.debug(f" NEUTRAL | time_window | Product: {self.product_name} | Outside Window {self.current_time}.")
            return False
# ---------------------------------- Main Function ------------------------------------ #      
    def check(self):
        logger.debug(f" NEUTRAL | check | Product: {self.product_name} | Note: Running")
        logic = False
            
        with last_alerts_lock:
            # Retrieve or initialize the last state
            last_state = last_alerts.get(self.product_name)
            if last_state is None:
                last_state = {'last_extension': None, 'neutral_alert_sent': False, 'has_gone_neutral': False}
                last_alerts[self.product_name] = last_state
                logger.debug(f" NEUTRAL | check | Product: {self.product_name} | Note: Initialized last_state")

            # Evaluate the input
            logic = self.input(last_state)

            # Update the last state
            last_alerts[self.product_name] = last_state
                            
        if logic and self.time_window():      
            try: 
                self.execute()
                # Set the alert flag to prevent duplicate alerts for the same extension
                with last_alerts_lock:
                    last_state['neutral_alert_sent'] = True
            except Exception as e:
                logger.error(f" NEUTRAL | check | Product: {self.product_name} | Note: Failed to send Slack alert: {e}")
        else:
            logger.debug(f" NEUTRAL | check | Product: {self.product_name} | Note: No alert sent")

    def slack_message(self):
        logger.debug(f" NEUTRAL | slack_message | Product: {self.product_name} | Note: Running")
        
        pro_color = self.product_color.get(self.product_name)
        alert_time_formatted = self.current_datetime.strftime('%H:%M:%S') 

        direction_emojis = {
            'Neutral_Higher': ':arrow_up:',
            'Neutral_Lower': ':arrow_down:',
            }

        message_template = (
            f">:large_{pro_color}_square:  *{self.product_name} - Context Alert - IB*  :large_{pro_color}_square:\n"
            "────────────────────\n"
            f">       :warning:   *NEUTRAL*    :warning:\n"      
            f"- Neutral: Ib Extension {direction_emojis.get(self.neutral_type)}!\n"
            "────────────────────\n"            
            f">*Alert Time*: _{alert_time_formatted}_ EST\n"
        )
        return message_template  
    
    def execute(self):
        logger.debug(f" NEUTRAL | execute | Product: {self.product_name} | Note: Running")
        
        message = self.slack_message()
        channel = self.slack_channels.get(self.product_name)
        
        if channel:
            self.send_slack_message(channel, message)
            logger.info(f" NEUTRAL | execute | Product: {self.product_name} | Note: ALert Sent To {channel}")
        else:
            logger.debug(f" NEUTRAL | execute | Product: {self.product_name} | Note: No Slack Channel Configured")