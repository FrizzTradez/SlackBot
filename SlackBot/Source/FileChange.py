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
        "required_files": ["ES_1","ES_2","ES_3","ES_4","ES_6","ES_7"]
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

            logger.debug(f" FileChange | Note: {filepath} modified")

            try:

                task = next((t for t in self.files if os.path.abspath(t["filepath"]) == filepath), None)
                
                if task:
                    file_name = task["name"]
                    product_name, file_id = self.extract_product_and_id(file_name)
                    
                    if not product_name or not file_id:
                        logger.warning(f" FileChange | FileName: {file_name} | Note: Invalid task name")
                        return

                    with self.lock:

                        for condition in self.conditions:
                            if file_name in condition["required_files"]:
                                self.updated_conditions[condition["name"]].add(file_name)
                                logger.debug(f" FileChange | Condition: {condition['name']} | CurrentQueue: {self.updated_conditions[condition['name']]}")

                                if self.updated_conditions[condition["name"]] == set(condition["required_files"]):

                                    if condition["name"] not in self.conditions_in_queue:

                                        self.processing_queue.put(condition)
                                        self.conditions_in_queue.add(condition["name"])
                                        logger.debug(f" FileChange | Condition: {condition['name']} | Note: Enqueue For Processing")

                                        self.updated_conditions[condition["name"]] = set()
                                    else:
                                        logger.debug(f" FileChange | Condition: {condition['name']} | Note: Already In Queue")
                else:
                    logger.warning(f" FileChange | FilePath: {event.src_path} | Note: No task found for file")
                    
            except Exception as e:
                logger.error(f" FileChange | FilePath: {event.src_path} | Note: Error Processing File: {e}")

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

                logger.debug(f" FileChange | Condition: {condition_name} | Processing: {required_files}")

                tasks = [self.file_to_task[file_name] for file_name in required_files]

                all_variables = Initialization.prep_data(tasks)
                
                product_name = required_files[0].split('_')[0]
                
                variables = all_variables.get(product_name, {})

                if not variables:
                    logger.warning(f" FileChange | Product: {product_name} | Note: No variables found for product")
                    continue
                
                # IMPORTANT 
                pvat = PVAT(product_name, variables)
                pvat.check()

                logger.debug(f" FileChange | Condition: {condition_name} | Note: Complete")
            except Exception as e:
                logger.error(f" FileChange | Condition: {condition['name']} | Note: Error Processing Condition: {e}")
            finally:
                with self.lock:
                    self.conditions_in_queue.discard(condition["name"])
                self.processing_queue.task_done()
