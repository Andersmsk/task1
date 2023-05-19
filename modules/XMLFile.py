import logging
import xml.etree.ElementTree as ET
from psycopg2 import Error


logger = logging.getLogger('XMLFile')


class XMLFile:
    """ Class for saving query results from DB to XML file """

    @staticmethod
    def save_file(data: dict, filename: str) -> None:
        """Save incoming data to file (filename).

        :param data: Data to be saved in XML format
        :type data: dict
        :param filename: Name of the output XML file
        :type filename: str
        :rtype: None
        """
        try:
            # creating root XML element
            root = ET.Element("data")

            # creating elements for columns
            columns_element = ET.SubElement(root, "columns")
            # creating elements for each column in column_names
            for column in data["columns"]:
                # creating column element inside columns element
                column_element = ET.SubElement(columns_element, "column")
                # setting column element text = column name
                column_element.text = column

            # creating elements for rows
            rows_element = ET.SubElement(root, "rows")
            # creating element for each row in rows list
            for row in data["rows"]:
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