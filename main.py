from watchdog.observers import Observer
from SlackBot.Source.FileChange import *
from SlackBot.External import External_Config
from SlackBot.Source.Startup import *
from SlackBot.Slack_Alerts.Periodic.Ib_Crude import IB_Crude_Alert
from SlackBot.Slack_Alerts.Periodic.Ib_Equity import IB_Equity_Alert
from SlackBot.Slack_Alerts.Periodic.Economic import Economic
from SlackBot.Slack_Alerts.Mixed.Gap_Equity import Gap_Check_Equity
from SlackBot.Slack_Alerts.Mixed.Gap_Crude import Gap_Check_Crude
from logs.Logging_Config import setup_logging
from zoneinfo import ZoneInfo
import time
from datetime import timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import os

#                Necessary Improvements for 10/04/24
# ------------------------------------------------------------ #
# IB Neutral Alert (Addition)
# Swing Bias Alert Similar to Pre IB Bias (Addition)
# Do Something With Overnight Stat (Addition)
# Start to Work On More Playbook Setups! 
# Make Sure Alert Messages Are Consistent Looking
# ------------------------------------------------------------ #

def main():
    start_time = time.time()
    
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.debug(" Main | Note: Fetching External Data\n")
    # ------------------------- Startup Processes ------------------------------ #
    es_impvol, nq_impvol, rty_impvol, cl_impvol = Initialization.grab_impvol(external_impvol)
    External_Config.set_impvol(es_impvol, nq_impvol, rty_impvol, cl_impvol)
    
    es_bias, nq_bias, rty_bias, cl_bias = Initialization.grab_bias(external_bias)
    External_Config.set_bias(es_bias, nq_bias, rty_bias, cl_bias)
    
    ib_equity_alert = IB_Equity_Alert(files)
    ib_crude_alert = IB_Crude_Alert(files)
    economic_alert = Economic(files)
    gap_check_equity_alert = Gap_Check_Equity(files)
    gap_check_crude_alert = Gap_Check_Crude(files)
    est = ZoneInfo('America/New_York')
    
    # ---------------------- Initialize APScheduler ----------------------------- #
    scheduler = BackgroundScheduler(timezone=est)
    
    # Schedule Econ Alert at 8:45 AM EST every day
    scheduler.add_job(
        economic_alert.send_alert,
        trigger=CronTrigger(hour=22, minute=45, timezone=est),
        name='Economic Alert'
    )
    # Schedule Gap Check Equity 9:30 AM EST every day
    scheduler.add_job(
        gap_check_equity_alert.send_alert,
        trigger=CronTrigger(hour=9, minute=30, timezone=est),
        name='Gap Check Equity'
    )      
    # Schedule Gap Check Crude at 9:00 AM EST every day
    scheduler.add_job(
        gap_check_crude_alert.send_alert,
        trigger=CronTrigger(hour=9, minute=0, timezone=est),
        name='Gap Check Crude'
    )    
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
    logger.info(" Main | Note: Press Enter To Start Monitoring...")
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
            time.sleep(1) 
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