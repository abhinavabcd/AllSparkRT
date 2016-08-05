'''
Created on Feb 2, 2016

@author: abhinav
'''
import logging
import config 
from logging.handlers import TimedRotatingFileHandler
# create logger

logger = logging.getLogger(config.APPLICATION_NAME)
logger.setLevel(logging.ERROR)

# create console handler and set level to debug
log_handler = logging.StreamHandler()
log_handler.setLevel(logging.ERROR)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log_handler.setFormatter(formatter)
#default
logger.addHandler(log_handler)



def init_timed_rotating_log(path , level=logging.ERROR):
    """"""
    global logger
    global log_handler
    
    logger.removeHandler(log_handler) # remove current log handler
    log_handler = TimedRotatingFileHandler(path,
                                       when="h",
                                       interval=1,
                                       backupCount=5)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # add formatter to ch
    log_handler.setFormatter(formatter)

    logger.addHandler(log_handler)
    log_handler.setLevel(level)
 