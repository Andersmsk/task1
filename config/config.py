import logging
import sys

OUTPUT_PATH = 'path'


# set logging output into console  (logging is more powerful and wide functioned than standard Error e:)
"""
level=logging.INFO: Sets the logging level to INFO, only messages with a level of INFO or higher will be logged --
format="%(asctime)s - %(levelname)s - %(message)s - the record will include the event time information(%(asctime)s),
the logging level (%(levelname)s), and the message itself (%(message)s)
"""
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s", stream=sys.stdout)

logger = logging.getLogger('task1')
logger.setLevel(logging.DEBUG)
