from watchdog.observers import Observer
from SlackBot.Source.FileChange import *
from SlackBot.External import External_Config
from SlackBot.Source.Startup import *
from SlackBot.Slack_Alerts.Periodic.IB_CRUDE import IB_Crude_Alert
from SlackBot.Slack_Alerts.Periodic.IB_EQUITY import IB_Equity_Alert
from logs.Logging_Config import setup_logging
from datetime import datetime
from zoneinfo import ZoneInfo
import time

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

import os

def main():
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("Fetching External Data...\n")
    # ------------------------- Startup Processes ------------------------------ #
    es_impvol, nq_impvol, rty_impvol, cl_impvol = Initialization.grab_impvol(external_data)
    External_Config.set_impvol(es_impvol, nq_impvol, rty_impvol, cl_impvol)
    
    ib_equity_alert = IB_Equity_Alert(files)
    ib_crude_alert = IB_Crude_Alert(files)
    
    est = ZoneInfo('America/New_York')  # Define EST timezone
    
    # ---------------------- Initialize APScheduler ----------------------------- #
    scheduler = BackgroundScheduler(timezone=est)
    
    # Schedule IB Equity Alert at 10:30 AM EST every day
    scheduler.add_job(
        ib_equity_alert.ib_check_in_equity,
        trigger=CronTrigger(hour=10, minute=30),
        name='IB Equity Alert'
    )
    
    # Schedule IB Crude Alert at 10:00 AM EST every day
    scheduler.add_job(
        ib_crude_alert.ib_check_in_crude,
        trigger=CronTrigger(hour=10, minute=0),
        name='IB Crude Alert'
    )
    
    scheduler.start()
    logger.info("APScheduler started with EST timezone.")
    
    # ---------------------- Start Monitoring Files ----------------------------- #
    logger.info("Press Enter To Start Monitoring...")
    input("")
    
    event_handler = FileChangeHandler(files, conditions, debounce_interval=1.0)
    observer = Observer()

    directories_to_watch = set(os.path.dirname(os.path.abspath(task["filepath"])) for task in files)
    for directory in directories_to_watch:
        observer.schedule(event_handler, path=directory, recursive=False)
    
    observer.start()
    
    logger.info("Monitoring started. Press Ctrl+C to stop.")
    
    try:
        while True:
            time.sleep(1)  # Keep the main thread alive
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Shutting down...")
        observer.stop()
        scheduler.shutdown()
    observer.join()

if __name__ == '__main__':
    main()