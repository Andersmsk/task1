import psycopg2
from psycopg2 import Error
import logging


logger = logging.getLogger('database')
logger.setLevel(logging.DEBUG)

class Database:
    """Making class for connection and basic DB operations"""

    def __init__(self, config: dict) -> None:
        self.config = config
        self.connection = None
        self.cursor = None

    def connect(self) -> None:
        """Making connection to Database using .env file and dotenv lib"""
        try:
            self.connection = psycopg2.connect(user=self.config["DB_USERNAME"],
                                               password=self.config["DB_PASSWORD"],
                                               host=self.config["DB_HOST"],
                                               port=self.config["DB_PORT"],
                                               database=self.config["DB_DATABASE"])
            self.cursor = self.connection.cursor()  # initializing cursor
            logging.info("-- Connected to database...")
        except Error as e:
            logging.error(f"-- Failed to connect to database: {e}")

    def execute_query(self, query: str) -> None:
        """Sending and executing query to database"""
        self.cursor.execute(query)

    def commit(self) -> None:
        """Saving result in DB after query"""
        self.connection.commit()

    def close(self) -> None:
        """Close cursor and after close connection for saving resources"""
        self.cursor.close()
        self.connection.close()
        logging.info("-- DB connection closed")