# importing library json to work with JSON format
# importing library psycopg2 to work with POSTGRESQL DBMS
# importing library tqdm for statistic
# importing library os for working with operating system
# importing xml.etree.ElementTree to work with XML files
# importing error exception from psycopg2
# importing dotenv to move out credentials
# importing logging for logs

import json
import tqdm
import argparse
import os
import logging


from psycopg2 import Error
from tqdm import tqdm
from dotenv import dotenv_values
from typing import Any

from modules.JSONFile import JSONFile
from modules.Database import Database
from modules.query import execute_and_save_query_json, execute_query_and_save_xml


logger = logging.getLogger('main')


def main(students_file_path: str, rooms_file_path: str, output_format: str) -> Any:
    """Main program logic"""
    # Load the environment variables from the .env file
    config = dotenv_values(".env")

    # Create an instance of the DatabaseConnection class
    database = Database(config)
    database.connect()

    # Read JSON files
    rooms = JSONFile.read_file(rooms_file_path)
    students = JSONFile.read_file(students_file_path)

    # Insert data into the table room (In DBMS)
    try:
        for room in tqdm(rooms, desc=f"-- Inserting values into table room DB {database.config['DB_DATABASE']}"):
            insert_query = f"INSERT INTO room(id, name) VALUES ({room['id']}, '{room['name']}')"
            database.execute_query(insert_query)

        for student in tqdm(students,
                            desc=f"-- Inserting values into table student DB {database.config['DB_DATABASE']}"):
            insert_query = f"INSERT INTO student(id, birthday, name, room, sex) " \
                           f"VALUES ({student['id']}, '{student['birthday']}', '{student['name']}', " \
                           f"{student['room']}, '{student['sex']}')"
            database.execute_query(insert_query)

        database.commit()  # saving transaction result
        logging.info("-- Data inserted")
    except Error as e:
        logging.error(f"-- Error inserting data {e}")

    # Creating folder for result files IF NOT EXISTS
    results_folder = "results"
    if not os.path.exists(results_folder):
        os.makedirs(results_folder)
        logging.info("-- Folder 'results' created")

    # Executing and saving queries
    try:
        query1 = """
            SELECT public.room.id AS room_id,
            public.room."name" AS room_name,
            COUNT(public.student.id) AS students_quantity

            FROM public.room
                INNER JOIN public.student
                ON public.room.id = public.student.room 
            GROUP BY public.room.id
            ORDER BY public.room.id; 
            """
        database.execute_query(query1)  # executing query
        result = database.cursor.fetchall()  # collecting information from query

        filename1 = os.path.join(results_folder, "query1_result")  # join folder name + file name = results/query1_r.js
        if output_format == "json":  # selecting by user before launch .py
            filename1 += ".json"  # concatenate filename + .json
            execute_and_save_query_json(database, result, filename1)  # executing function for json
        elif output_format == "xml":  # selecting by user before launch
            filename1 += ".xml"  # concatenate filename + .xml
            execute_query_and_save_xml(database, result, filename1)  # executing function for xml

        query2 = """
            SELECT public.room.id AS room_id,
            public.room."name" AS room_name,
            COUNT(public.student.id) AS students_quantity,
            AVG(EXTRACT(YEAR FROM age(now(), public.student.birthday))) ::INTEGER AS average_age

            FROM public.room
                INNER JOIN public.student
                ON public.room.id = public.student.room
            GROUP BY public.room.id 
            ORDER BY average_age ASC
            LIMIT 5;
            """

        database.execute_query(query2)
        result = database.cursor.fetchall()

        filename2 = os.path.join(results_folder, "query2_result")
        if output_format == "json":
            filename2 += ".json"
            execute_and_save_query_json(database, result, filename2)
        elif output_format == "xml":
            filename2 += ".xml"
            execute_query_and_save_xml(database, result, filename2)

        query3 = """
            SELECT public.room.id AS room_id,
            public.room."name" AS room_name,
            COUNT(public.student.id) AS students_quantity,
            MAX(EXTRACT(YEAR FROM age(now(), public.student.birthday))) ::INTEGER -
            MIN(EXTRACT(YEAR FROM age(now(), public.student.birthday))) ::INTEGER AS stud_age_diff

            FROM public.room
                INNER JOIN public.student
                ON public.room.id = public.student.room
            GROUP BY public.room.id
            ORDER BY stud_age_diff DESC, students_quantity ASC
            LIMIT 5;
            """

        database.execute_query(query3)
        result = database.cursor.fetchall()

        filename3 = os.path.join(results_folder, "query3_result")
        if output_format == "json":
            filename3 += ".json"
            execute_and_save_query_json(database, result, filename3)
        elif output_format == "xml":
            filename3 += ".xml"
            execute_query_and_save_xml(database, result, filename3)

        query4 = """
            SELECT public.room.id AS room_id,
            public.room."name" AS room_name,
            STRING_AGG(public.student.sex, ', ' ORDER BY public.student.sex) AS genders_in_room

            FROM public.room
                INNER JOIN public.student
                ON public.room.id = public.student.room
            WHERE public.student.sex IN (UPPER('M'), UPPER('F'))
            GROUP BY public.room.id
            HAVING COUNT(DISTINCT public.student.sex) = 2
            ORDER BY public.room.id;
            """

        database.execute_query(query4)
        result = database.cursor.fetchall()

        filename4 = os.path.join(results_folder, "query4_result")
        if output_format == "json":
            filename4 += ".json"
            execute_and_save_query_json(database, result, filename4)
        elif output_format == "xml":
            filename4 += ".xml"
            execute_query_and_save_xml(database, result, filename4)

    except Error as e:
        logging.error(f"Error while executing queries {e}")

    finally:
        database.close()  # closing cursor and connection


if __name__ == "__main__":
    logger = logging.getLogger('task1')
    logger.setLevel(logging.DEBUG)
    logging.info('starting job')
    parser = argparse.ArgumentParser(description="Database Query and Export \
                                                 --example Python file.py source/students.json source/rooms.json json")
    parser.add_argument("students", type=str, help="Path to the students file")
    parser.add_argument("rooms", type=str, help="Path to the rooms file")
    parser.add_argument("format", choices=["json", "xml"], help="Output format (json or xml)")

    args = parser.parse_args()

    main(args.students, args.rooms, args.format)

# config.py
# OUTPUT_PATH = 'path'
# создать конфиг файл для логгера + переменные (папки)dir
# разделить код на модули
# добавить описание параметров в док стринги
