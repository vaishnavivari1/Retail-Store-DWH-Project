
"""
    Purpose of the Script:
          The script is used to create a Centralized Log folder where the logs of each script is kept.
          It is a best practie to keep all the logs at a single place / folder. It checks if the logs
          folder exist or not, if not creates the logs folder. Then we create a logger object that is 
          the log file and for that file we set a handler to decide where the information should be
          kept. Formatter is used to tell how the information is stored.

    This function is called via the concept of module
      to call: 
              from logger_setup import get_logger
              log_variable = get_logger("Filename")
          
"""

# ---- logger_setup.py ----
import os
import logging as log

def get_logger(log_filename="default.log", log_folder="logs"):
    """
        Creates and returns a logger that writes to a centralized log folder.
        Each script can pass its own log filename.
    """
    os.makedirs(log_folder, exist_ok=True)
    log_path = os.path.join(log_folder, log_filename)
    logger = log.getLogger(log_filename)

    if not logger.hasHandlers():
        handler = log.FileHandler(log_path, mode="a")
        formatter = log.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(log.INFO)

    return logger