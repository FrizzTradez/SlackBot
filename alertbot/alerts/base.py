import os
import discord
import logging
from datetime import datetime, time
from zoneinfo import ZoneInfo
from logs.Logging_Config import setup_logging
from dotenv import load_dotenv

load_dotenv()
setup_logging()
logger = logging.getLogger(__name__)

class Base:
    discord_webhooks_playbook = {
        'ES': os.getenv("DISCORD_PLAYBOOK_ES_WEBHOOK"),
        'NQ': os.getenv("DISCORD_PLAYBOOK_NQ_WEBHOOK"),
        'RTY': os.getenv("DISCORD_PLAYBOOK_RTY_WEBHOOK"),
        'CL': os.getenv("DISCORD_PLAYBOOK_CL_WEBHOOK")
    }
    discord_webhooks_alert = {
        'ES': os.getenv("DISCORD_CONTEXT_ES_WEBHOOK"),
        'NQ': os.getenv("DISCORD_CONTEXT_NQ_WEBHOOK"),
        'RTY': os.getenv("DISCORD_CONTEXT_RTY_WEBHOOK"),
        'CL': os.getenv("DISCORD_CONTEXT_CL_WEBHOOK")
    }
    product_color = {
        'ES': 'blue',
        'NQ': 'green',
        'RTY': 'orange',
        'CL': 'purple'
    }
    
    def __init__(self, product_name, variables):
        self.product_name = product_name
        self.variables = variables
        
        # TimeZone Setup
        self.est = ZoneInfo('America/New_York')
        self.current_datetime = datetime.now(self.est)
        self.current_time = self.current_datetime.time()
        
        # Time Regulations for Equity Products
        self.equity_open = time(9, 30) 
        self.equity_ib = time(10, 30)
        self.equity_lunch_start = time(12, 00)
        self.equity_lunch_end = time(14, 00)
        self.equity_close = time(16, 00)

        # Time Regulations for Crude 
        self.crude_open = time(9, 00)
        self.crude_ib = time(10, 00)
        self.crude_close = time(14, 30) 
        
        # Custom Time Regulations for Playbook
        self.crude_pvat_start = time(9, 2)
        self.equity_pvat_start = time(9, 32)
        self.crude_dogw_start = time(9, 10)
        self.equity_dogw_start = time(9, 40)
        
    def send_discord_message(self, webhook_url, message, username="Notifier", avatar_url=None):
        """
        Sends a message to a Discord channel via a webhook.

        :param webhook_url: The Discord webhook URL.
        :param message: The message to send.
        :param username: The display name of the webhook.
        :param avatar_url: The avatar image URL of the webhook.
        """
        if webhook_url:
            try:
                webhook = discord.Webhook.from_url(webhook_url, adapter=discord.RequestsWebhookAdapter())
                embed = discord.Embed(description=message, color=self.get_color())
                webhook.send(embed=embed, username=username, avatar_url=avatar_url)
                logger.info(f"Message sent to Discord webhook: {webhook_url}")
            except Exception as e:
                logger.error(f"Failed to send message to Discord webhook: {e}")
        else:
            logger.warning(f"No Discord webhook URL configured for the product '{self.product_name}'.")
        
    def send_playbook_message(self, message):
        webhook_url = self.discord_webhooks_playbook.get(self.product_name)
        self.send_discord_message(webhook_url, message)

    def send_context_message(self, message):
        webhook_url = self.discord_webhooks_alert.get(self.product_name)
        self.send_discord_message(webhook_url, message)
    def send_economic_message(self, message):
        webhook_url = os.getenv("DISCORD_ECON_WEBHOOK")
        self.send_discord_message(webhook_url, message)