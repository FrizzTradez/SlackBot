import logging
import math
from datetime import datetime
from SlackBot.External import External_Config
from SlackBot.Slack_Alerts.Conditional.Base import Base_Conditional
from slack_sdk.models.blocks import SectionBlock, DividerBlock, ContextBlock, MarkdownTextObject

logger = logging.getLogger(__name__)

# This is A Time and Conditional Based Setup (Both are Required)

class DATR(Base_Conditional): 
    
    def __init__(self, product_name, variables):    
        super().__init__(product_name, variables)
        
    def datr_input(self):
        print("driving input for ibgw return score and related calculations return long/short")
        