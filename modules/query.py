import json
import logging
from typing import Any
import xml.etree.ElementTree as ET
from psycopg2 import Error

from modules import Database, XMLFile


logger = logging.getLogger('query')


def execute_query(connection: Database, query: str) -> Any:
    """Executing query and returning result"""
    connection.execute_query(query)
    return connection.cursor.fetchall()


def save_query_result_to_json(result: Any, filename: str) -> None:
    """Saving query result to JSON file"""
    data = {"columns": [desc[0] for desc in result.description], "rows": [dict(row) for row in result]}
    with open(filename, "w") as f:
        json.dump(data, f)
    logging.info(f"Query result saved into {filename}")


def save_query_result_to_xml(result: Any, filename: str) -> None:
    """Saving query result to XML file"""
    data = {"columns": [desc[0] for desc in result.description], "rows": [dict(row) for row in result]}
    XMLFile.save_file(data, filename)


def execute_and_save_query_result(connection: Database, query: str, filename: str, file_format: str) -> None:
    """Executing query and saving result in specified format"""
    result = execute_query(connection, query)

    if file_format == "json":
        save_query_result_to_json(result, filename)
    elif file_format == "xml":
        save_query_result_to_xml(result, filename)
    else:
        logging.error("-- Invalid file format. Only JSON and XML formats are supported.")


def execute_query_and_save_xml(database: Any, result: Any, filename: str) -> None:
    """Executing and saving XML file, the same description as in XML parent class
    @param database: database connection
    @param result:
    @param filename: filename of output json file
    """
    try:
        # collecting column names
        column_names = [desc[0] for desc in database.cursor.description]
        # creating dictionary for each row
        rows = [dict(zip(column_names, row)) for row in result]

        # creating root xml element
        root = ET.Element("data")

        # creating elements for columns
        columns_element = ET.SubElement(root, "columns")

        # creating elements for each column in column_names
        for column in column_names:
            # creating column element inside columns element
            column_element = ET.SubElement(columns_element, "column")
            # setting column element text = column name
            column_element.text = column

        # creating elements for rows
        rows_element = ET.SubElement(root, "rows")
        # creating element for each row in rows list
        for row in rows:
            # creating element row in element rows
            row_element = ET.SubElement(rows_element, "row")
            # for each pare column-value in current row
            for column, value in row.items():
                # creating element with the name = column
                column_element = ET.SubElement(row_element, column)
                # setting element text = current
                column_element.text = str(value)

        # creating object ElementTree with root element
        tree = ET.ElementTree(root)
        # writing tree data to file
        tree.write(filename)

        logging.info(f"Query result saved into {filename}")
    except Error as e:
        logging.error(f"-- Error while creating XML file: {e}")


def execute_and_save_query_json(database: Any, result: Any, filename: str) -> None:
    """ Executing and saving query to JSON file """
    try:
        # Collecting column names with cursor.description
        column_names = [desc[0] for desc in database.cursor.description]
        # Make a dictionary, zip - connecting column name + row
        rows = [dict(zip(column_names, row)) for row in result]

        data = {
            "columns": column_names,
            "rows": rows
        }
        # Creating file (w-option new file) and put there our data
        with open(filename, "w") as f:
            json.dump(data, f)
        logging.info(f"-- Query result saved into file {filename}")

    except Error as e:
        logging.error(f"json execute and save func. error: {e}")


def create_indexes(database: Any) -> None:
    """Creating indexes in database
    @param database: desc
    """
    try:
        create_index_query = 'CREATE INDEX IF NOT EXISTS idx_room_name ON public.room("name")'
        database.execute_query(create_index_query)  # using cursor.execute(query)
        logging.info("-- Index idx_room_name created")

        create_index_query = "CREATE INDEX IF NOT EXISTS idx_student_birthday ON public.student(birthday)"
        database.execute_query(create_index_query)
        logging.info("-- Index idx_student_birthday created")

        create_index_query = "CREATE INDEX IF NOT EXISTS idx_student_room ON public.student(room)"
        database.execute_query(create_index_query)
        logging.info("-- Index idx_student_room created")

        database.commit()  # saving changes to DB
        logging.info("Indexes created successfully")

    except Error as e:
        logging.error(f"-- Error while creating indexes {e}")
