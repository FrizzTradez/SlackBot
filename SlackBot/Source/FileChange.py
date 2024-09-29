import threading
from queue import Queue
import time
import logging
import os
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from SlackBot.Source.Startup import Initialization
from SlackBot.Slack_Alerts.Conditional.PVAT import PVAT

logger = logging.getLogger(__name__)

conditions = [
    {
        "name": "PVAT_ES",
        "required_files": ["ES_1", "ES_2", "ES_3","ES_4","ES_6","ES_7"]
    },
    {
        "name": "PVAT_NQ",
        "required_files": ["NQ_1","NQ_2","NQ_3","NQ_4","NQ_6","NQ_7"]
    },
    {
        "name": "PVAT_RTY",
        "required_files": ["RTY_1","RTY_2","RTY_3","RTY_4","RTY_6","RTY_7"]
    },
    {
        "name": "PVAT_CL",
        "required_files": ["CL_1","CL_2","CL_3","CL_4","CL_6","CL_7"]
    },
]

class FileChangeHandler(FileSystemEventHandler):
    def __init__(self, files, conditions, debounce_interval=1.0):

        self.files = files
        self.conditions = conditions
        self.file_paths = [os.path.abspath(task["filepath"]) for task in self.files]
        
        self.file_to_task = {task["name"]: task for task in self.files}

        self.conditions_dict = {condition["name"]: set(condition["required_files"]) for condition in self.conditions}

        self.updated_conditions = {condition["name"]: set() for condition in self.conditions}

        self.processing_queue = Queue()

        self.processing_thread = threading.Thread(target=self.process_queue, daemon=True)
        self.processing_thread.start()

        self.lock = threading.Lock()

        self.debounce_interval = debounce_interval
        self.last_processed = {}

        self.conditions_in_queue = set()

    def on_modified(self, event):

        if event.is_directory:
            return  

        filepath = os.path.abspath(event.src_path)

        if filepath in self.file_paths:
            current_time = time.time()
            last_time = self.last_processed.get(filepath, 0)
            if current_time - last_time < self.debounce_interval:

                return
            else:

                self.last_processed[filepath] = current_time

            logger.info(f"{filepath} modified")

            try:

                task = next((t for t in self.files if os.path.abspath(t["filepath"]) == filepath), None)
                
                if task:
                    file_name = task["name"]
                    product_name, file_id = self.extract_product_and_id(file_name)
                    
                    if not product_name or not file_id:
                        logger.warning(f"Invalid task name format: {file_name}")
                        return

                    with self.lock:

                        for condition in self.conditions:
                            if file_name in condition["required_files"]:
                                self.updated_conditions[condition["name"]].add(file_name)
                                logger.info(f"Updated files for {condition['name']}: {self.updated_conditions[condition['name']]}")

                                if self.updated_conditions[condition["name"]] == set(condition["required_files"]):

                                    if condition["name"] not in self.conditions_in_queue:

                                        self.processing_queue.put(condition)
                                        self.conditions_in_queue.add(condition["name"])
                                        logger.info(f"All required files for {condition['name']} have been updated. Enqueued for processing.")

                                        self.updated_conditions[condition["name"]] = set()
                                    else:
                                        logger.info(f"Condition '{condition['name']}' is already in the queue or being processed.")
                else:
                    logger.warning(f"No task found for file {event.src_path}")
                    
            except Exception as e:
                logger.error(f"Error processing file {event.src_path}: {e}")

    def extract_product_and_id(self, task_name):

        parts = task_name.split('_')
        if len(parts) < 2:
            return None, None
        return parts[0], parts[1]
    
    # The Funnel and Queueing system
    def process_queue(self):

        while True:
            condition = self.processing_queue.get()
            try:
                condition_name = condition["name"]
                required_files = condition["required_files"]

                logger.info(f"Processing {condition_name} with files: {required_files}")

                tasks = [self.file_to_task[file_name] for file_name in required_files]

                all_variables = Initialization.prep_data(tasks)
                
                product_name = required_files[0].split('_')[0]
                
                variables = all_variables.get(product_name, {})

                if not variables:
                    logger.warning(f"No variables found for product '{product_name}'. Skipping processing.")
                    continue
                
                # IMPORTANT 
                pvat = PVAT(product_name, variables)
                pvat.check()

                logger.info(f"Processing completed for {condition_name}.")
            except Exception as e:
                logger.error(f"Error processing condition '{condition['name']}': {e}")
            finally:
                with self.lock:
                    self.conditions_in_queue.discard(condition["name"])
                self.processing_queue.task_done()
