import json
import logging
from typing import Any


logger = logging.getLogger('JSONFile')


class JSONFile:
    """ Making class for reading JSON files, r-attribute means reading only"""
    @staticmethod
    def read_file(file_path: str) -> Any:
            """reading JSON file
            @rtype: object
            """
            try:
                with open(file_path, "r") as f:
                    return json.load(f)
            except FileNotFoundError as e:
                logging.error(f"-- Error reading JSON file {e}")  # throw error if file not found
