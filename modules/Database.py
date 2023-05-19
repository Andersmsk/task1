import psycopg2
from psycopg2 import Error
import logging

logger = logging.getLogger('database')


class Database:
    """Making class for connection and basic DB operations"""

    def __init__(self, config: dict) -> None:
        """Initialize the Database object.

        :param config: Configuration for the database connection
        :type config: dict
        :rtype: None
        """

        self.config = config
        self.connection = None
        self.cursor = None

    def connect(self) -> None:
        """Make a connection to the database.

        :rtype: None
        """
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
        """Execute a query on the database.

        :param query: SQL query to be executed
        :type query: str
        :rtype: None
        """
        self.cursor.execute(query)

    def commit(self) -> None:
        """Commit the changes made to the database.

        :rtype: None
        """
        self.connection.commit()

    def close(self) -> None:
        """Close the cursor and the database connection.

        :rtype: None
        """
        self.cursor.close()
        self.connection.close()
        logging.info("-- DB connection closed")
