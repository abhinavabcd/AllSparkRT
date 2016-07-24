'''
Created on Feb 2, 2016

@author: abhinav
'''
import logging
import config 
# create logger
logger = logging.getLogger(config.APPLICATION_NAME)
logger.setLevel(logging.ERROR)

# create console handler and set level to debug
console_log_handler = logging.StreamHandler()
console_log_handler.setLevel(logging.ERROR)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to ch
console_log_handler.setFormatter(formatter)

# add ch to logger
logger.addHandler(console_log_handler)

