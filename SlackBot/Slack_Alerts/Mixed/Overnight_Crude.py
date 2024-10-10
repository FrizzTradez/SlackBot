import logging
from SlackBot.Slack_Alerts.Periodic.Base import Base_Periodic
import threading
from datetime import datetime
import time

logger = logging.getLogger(__name__)

class Overnight_Equity(Base_Periodic):
    def __init__(self, files):
        super().__init__(files)
        
    # ---------------------- Alert Preparation ------------------------- #
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

    def process_product(self, product_name):
        try:
            variables = self.fetch_latest_variables(product_name)
            if not variables:
                logger.error(f" OVERNIGHT_CRUDE | process_product | Product: {product_name} |  Note: No data available ")
                return
            
            # Variables (Round All Variables) 
            overnight_high = round(variables.get(f'{product_name}_OVNH'), 2)
            overnight_low = round(variables.get(f'{product_name}_OVNL'), 2)
            day_high = round(variables.get(f'{product_name}_DAY_HIGH'), 2)
            day_low = round(variables.get(f'{product_name}_DAY_LOW'), 2)
            
            color = self.product_color.get(product_name)
            current_time = datetime.now(self.est).strftime('%H:%M:%S')
            
            if day_high < overnight_high and day_low > overnight_low:
                
                # Message Template
                message = (
                    f">:large_{color}_square:  *{product_name} - Context Alert - Stat*  :large_{color}_square:\n"
                    "────────────────────\n"
                    f">      :warning:   *OVERNIGHT*    :warning:\n"      
                    f"- Overnight Stat In Play! \n"
                    "────────────────────\n"            
                    f">*Alert Time*: _{current_time}_ EST\n"
                )
                
                # Send Slack Alert
                channel = self.slack_channels.get(product_name)
                if channel:
                    self.slack_client.chat_postMessage(channel=channel, text=message) 
                    logger.info(f" OVERNIGHT_CRUDE | process_product | Note: Message sent to {channel} for {product_name}")
                else:
                    logger.error(f" OVERNIGHT_CRUDE | process_product | Note: No Slack channel configured for {product_name}")
            else:
                logger.info(f" OVERNIGHT_CRUDE | process_product | Product: {product_name} | Note: Overnight Stat Complete")
        except Exception as e:
            logger.error(f" OVERNIGHT_CRUDE | process_product | Product: {product_name} | Error processing: {e}")