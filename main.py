from watchdog.observers import Observer
from SlackBot.Source.FileChange import *
from SlackBot.External import External_Config
from SlackBot.Source.Startup import *
from SlackBot.Slack_Alerts.Periodic.IB_CRUDE import IB_Crude_Alert
from SlackBot.Slack_Alerts.Periodic.IB_EQUITY import IB_Equity_Alert
from logs.Logging_Config import setup_logging
from zoneinfo import ZoneInfo
import time
from datetime import timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import os

#                Necessary Improvements for 10/04/24
# ------------------------------------------------------------ #
# Econ Alert If we are before 8:45 AM EST wait, if After send immediately (Addition)
# Conditional Alerts for Gap Tier and Gap CLose (Use These to Modify IB Check-in) (Addition)
# Pre IB Bias Violation Alert (Fetch from Google Sheets) (Addition)
# Implement Changes That Were made to Day and Open Types Within Google Sheets (Check)
# Implement Posture Change Alerts (Addition)
# Do Something With Overnight Stat (Addition)
# ------------------------------------------------------------ #

def main():
    start_time = time.time()
    
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.debug(" Main | Note: Fetching External Data\n")
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
        ib_equity_alert.send_alert,
        trigger=CronTrigger(hour=10, minute=30, timezone=est),
        name='IB Equity Alert'
    )
    
    # Schedule IB Crude Alert at 10:00 AM EST every day
    scheduler.add_job(
        ib_crude_alert.send_alert,
        trigger=CronTrigger(hour=10, minute=00, timezone=est),
        name='IB Crude Alert'
    )
    
    scheduler.start()
    logger.info("APScheduler started.")
    
    # ---------------------- Start Monitoring Files ----------------------------- #
    logger.info(" Main | Note: Press Enter To Start Monitoring...\n")
    input("")
    
    event_handler = FileChangeHandler(files, conditions, debounce_interval=1.0)
    observer = Observer()

    directories_to_watch = set(os.path.dirname(os.path.abspath(task["filepath"])) for task in files)
    for directory in directories_to_watch:
        observer.schedule(event_handler, path=directory, recursive=False)
    
    observer.start()
    
    logger.info(" Main | Note: Monitoring started. Press 'Ctrl+C' to stop.")
    
    try:
        while True:
            time.sleep(1)  # Keep the main thread alive
    except KeyboardInterrupt:
        logger.info(" Main | Note: Shutting down...")
        observer.stop()
        scheduler.shutdown()
        
    observer.join()
    
    end_time = time.time()
    elapsed_time = timedelta(seconds=end_time - start_time)
    logger.info(f"\n Main | Note: Script ran for {elapsed_time}")

if __name__ == '__main__': 
    main()