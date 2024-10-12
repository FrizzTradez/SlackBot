import logging
import threading
from datetime import datetime
from SlackBot.Slack_Alerts.Conditional.Base import Base_Conditional
from slack_sdk.models.blocks import SectionBlock, DividerBlock, ContextBlock, MarkdownTextObject

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

        # Initialize variables to keep track of alerts
        has_alerted_neutral_lower = last_state.get('has_alerted_neutral_lower', False)
        has_alerted_neutral_higher = last_state.get('has_alerted_neutral_higher', False)

        self.neutral_type = None

        # Check if both IBH and IBL have been extended
        if self.day_high > self.ib_high and self.day_low < self.ib_low:
            # Both sides have been extended, do not send an alert
            logger.debug(f" NEUTRAL | input | Product: {self.product_name} | Note: Both IBH and IBL have been extended, no alert will be sent")
            return False

        logic = False

        # Check for Neutral Lower scenario
        if self.day_high > self.ib_high and not has_alerted_neutral_lower:
            if self.cpl < self.ib_low:
                logic = True
                self.neutral_type = 'Lower'
                last_state['has_alerted_neutral_lower'] = True
                logger.debug(f" NEUTRAL | input | Product: {self.product_name} | Note: Neutral Lower detected")

        # Check for Neutral Higher scenario
        elif self.day_low < self.ib_low and not has_alerted_neutral_higher:
            if self.cpl > self.ib_high:
                logic = True
                self.neutral_type = 'Higher'
                last_state['has_alerted_neutral_higher'] = True
                logger.debug(f" NEUTRAL | input | Product: {self.product_name} | Note: Neutral Higher detected")

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
                last_state = {
                    'has_alerted_neutral_lower': False,
                    'has_alerted_neutral_higher': False
                }
                last_alerts[self.product_name] = last_state
                logger.debug(f" NEUTRAL | check | Product: {self.product_name} | Note: Initialized last_state")

            # Evaluate the input
            logic = self.input(last_state)

            # Update the last state
            last_alerts[self.product_name] = last_state

        if logic and self.time_window():
            try:
                self.execute()
            except Exception as e:
                logger.error(f" NEUTRAL | check | Product: {self.product_name} | Note: Failed to send Slack alert: {e}")
        else:
            logger.debug(f" NEUTRAL | check | Product: {self.product_name} | Note: No alert sent")
# ---------------------------------- Alert Preparation------------------------------------ # 
    def slack_message(self):
        logger.debug(f" NEUTRAL | slack_message | Product: {self.product_name} | Note: Running")
        
        pro_color = self.product_color.get(self.product_name)
        alert_time_formatted = self.current_datetime.strftime('%H:%M:%S') 

        direction_emojis = {
            'Higher': ':arrow_up:',
            'Lower': ':arrow_down:',
        }

        arrow = direction_emojis.get(self.neutral_type)

        blocks = []

        # Title Block
        title_text = f":large_{pro_color}_square:  *{self.product_name} - Context Alert - IB*  :large_{pro_color}_square:"
        title_block = SectionBlock(text=title_text)
        blocks.append(title_block)

        # Divider
        blocks.append(DividerBlock())

        # Neutral Block
        neutral_text = f"> {arrow}   *NEUTRAL*    {arrow}"
        neutral_block = SectionBlock(text=neutral_text)
        blocks.append(neutral_block)

        # Neutral Activity Block
        activity_text = f"- Neutral Activity: IB Extension {arrow}!"
        activity_block = SectionBlock(text=activity_text)
        blocks.append(activity_block)

        # Divider
        blocks.append(DividerBlock())

        # Alert Time Context Block
        alert_time_text = f"*Alert Time*: _{alert_time_formatted}_ EST"
        alert_time_block = ContextBlock(elements=[
            MarkdownTextObject(text=alert_time_text)
        ])
        blocks.append(alert_time_block)

        # Convert blocks to dicts
        blocks = [block.to_dict() for block in blocks]

        return blocks  

    def execute(self):
        logger.debug(f" NEUTRAL | execute | Product: {self.product_name} | Note: Running")
        
        blocks = self.slack_message()
        channel = self.slack_channels.get(self.product_name)
        
        if channel:
            self.slack_client.chat_postMessage(
                channel=channel,
                blocks=blocks,
                text=f"Context Alert - IB for {self.product_name}"
            )
            logger.info(f" NEUTRAL | execute | Product: {self.product_name} | Note: Alert Sent To {channel}")
        else:
            logger.debug(f" NEUTRAL | execute | Product: {self.product_name} | Note: No Slack Channel Configured")